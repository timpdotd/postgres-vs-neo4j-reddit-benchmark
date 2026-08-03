#!/usr/bin/env python3
"""
load_data.py — High-performance ETL pipeline for PostgreSQL and Neo4j.

Enforces 3NF Normalization (Relational) and Multi-Node/Multi-Relationship
Property Graph Modeling (NoSQL) as required by FAQ Q12.

Pipeline architecture:
  1. In-memory deduplication & entity resolution across both SNAP TSV files.
     Splits data into unique Subreddits, Posts, and Hyperlinks.
  2. PostgreSQL loading:
     - Uses high-speed bulk `COPY FROM STDIN` via StringIO buffers.
     - Runs `VACUUM ANALYZE` post-load to ensure accurate query planner statistics.
  3. Neo4j loading:
     - Enforces constraints and property indexes first.
     - Phase 1: Bulk UNWIND creation of all (:Subreddit) nodes.
     - Phase 2: Bulk UNWIND MERGE of (:Post) nodes and (:Subreddit)-[:POSTED]->(:Post)-[:REFERENCES]->(:Subreddit) edges.
"""

import io
import json
import csv
import logging
import os
import argparse
import sys
import time
from pathlib import Path
from typing import Any

import psycopg
from neo4j import GraphDatabase

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent.parent / "data"
TSV_FILES = [
    ("body",  DATA_DIR / "soc-redditHyperlinks-body.tsv"),
    ("title", DATA_DIR / "soc-redditHyperlinks-title.tsv"),
]

PG_CONFIG = {
    "host":     os.getenv("PG_HOST",  "localhost"),
    "port":     int(os.getenv("PG_PORT",  "5432")),
    "dbname":   os.getenv("PG_DB",    "reddit_benchmark"),
    "user":     os.getenv("PG_USER",  "reddit_user"),
    "password": os.getenv("PG_PASS",  "reddit_password"),
}

NEO4J_URI  = os.getenv("NEO4J_URI",  "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "reddit_password")

BATCH_SIZE_NEO = 2500

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "load_data.log", encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Phase 1: In-memory parsing and Entity Resolution (3NF / Property Graph)
# ---------------------------------------------------------------------------

def parse_dataset() -> tuple[dict[str, int], list[tuple], set[tuple]]:
    """
    Parses both TSVs and resolves entities into normalized collections:
      - subreddits: dict[name, int_id]
      - posts: list of (post_int_id, post_id, source_sub_int_id, timestamp, source_type, post_label, props_pg_array, props_list)
      - hyperlinks: set of (post_int_id, target_sub_int_id)
    """
    log.info("Starting entity resolution across TSV files...")
    t0 = time.perf_counter()

    subreddits: dict[str, int] = {}
    posts_map: dict[str, tuple] = {}
    hyperlinks: set[tuple[int, int]] = set()

    def get_sub_id(name: str) -> int:
        if name not in subreddits:
            subreddits[name] = len(subreddits) + 1
        return subreddits[name]

    total_rows = 0
    for src_type, file_path in TSV_FILES:
        if not file_path.exists():
            log.warning("File not found: %s (skipping)", file_path)
            continue

        log.info("Reading %s...", file_path.name)
        with open(file_path, "r", encoding="utf-8") as f:
            header = f.readline()  # skip header
            for line in f:
                total_rows += 1
                parts = line.rstrip("\r\n").split("\t")
                if len(parts) < 6:
                    continue

                src_name, tgt_name, post_id, ts_str, label_str, props_str = parts[:6]

                src_id = get_sub_id(src_name)
                tgt_id = get_sub_id(tgt_name)

                # Entity resolve Post (deduplicate by global post_id)
                if post_id not in posts_map:
                    post_int_id = len(posts_map) + 1
                    label = int(label_str)
                    
                    # Parse vector
                    try:
                        props_list = [float(x) for x in props_str.split(",") if x]
                    except ValueError:
                        props_list = []
                    
                    # PostgreSQL array literal string e.g. "{0.1, 0.2}"
                    props_pg = "{" + ",".join(str(x) for x in props_list) + "}" if props_list else "{}"

                    posts_map[post_id] = (
                        post_int_id,
                        post_id,
                        src_id,
                        ts_str,
                        src_type,
                        label,
                        props_pg,
                        props_list,
                        src_name,
                    )
                else:
                    post_int_id = posts_map[post_id][0]

                # Link edge
                hyperlinks.add((post_int_id, tgt_id))

    el = time.perf_counter() - t0
    log.info("Entity resolution complete in %.2f s (processed %d rows):", el, total_rows)
    log.info("  -> Subreddit entities : %d", len(subreddits))
    log.info("  -> Post entities      : %d", len(posts_map))
    log.info("  -> Hyperlink edges    : %d", len(hyperlinks))

    return subreddits, list(posts_map.values()), hyperlinks


# ---------------------------------------------------------------------------
# Phase 2: Export CSVs for Native Loading
# ---------------------------------------------------------------------------

def export_csvs(subreddits: dict[str, int], posts: list[tuple], hyperlinks: set[tuple]) -> None:
    log.info("=== Exporting native CSV files for bulk loading ===")
    t0 = time.perf_counter()
    
    # 1. Write subreddits.csv
    with open(DATA_DIR / "subreddits.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name"])
        for name, sub_id in subreddits.items():
            writer.writerow([sub_id, name])
            
    # 2. Write posts.csv (for Neo4j) and posts_pg.csv (for PostgreSQL)
    with open(DATA_DIR / "posts.csv", "w", encoding="utf-8", newline="") as f_neo, \
         open(DATA_DIR / "posts_pg.csv", "w", encoding="utf-8", newline="") as f_pg:
        writer_neo = csv.writer(f_neo)
        writer_pg = csv.writer(f_pg)
        writer_neo.writerow(["int_id", "post_id", "src_id", "ts", "type", "label", "props", "src_name"])
        writer_pg.writerow(["int_id", "post_id", "src_id", "ts", "type", "label", "props"])
        for p in posts:
            # p: (post_int_id, post_id, src_id, ts, type, label, props_pg, props_list, src_name)
            ts_neo = p[3].replace(" ", "T")
            writer_neo.writerow([p[0], p[1], p[2], ts_neo, p[4], p[5], p[6], p[8]])
            writer_pg.writerow([p[0], p[1], p[2], ts_neo, p[4], p[5], p[6]])
            
    # 3. Write links.csv (for Neo4j) and links_pg.csv (for PostgreSQL)
    with open(DATA_DIR / "links.csv", "w", encoding="utf-8", newline="") as f_neo, \
         open(DATA_DIR / "links_pg.csv", "w", encoding="utf-8", newline="") as f_pg:
        writer_neo = csv.writer(f_neo)
        writer_pg = csv.writer(f_pg)
        writer_neo.writerow(["post_int_id", "post_id", "tgt_id", "tgt_name"])
        writer_pg.writerow(["post_int_id", "tgt_id"])
        
        sub_id_to_name = {v: k for k, v in subreddits.items()}
        post_id_map = {p[0]: p[1] for p in posts}
        
        for pid_int, tgt_int in hyperlinks:
            writer_neo.writerow([pid_int, post_id_map[pid_int], tgt_int, sub_id_to_name[tgt_int]])
            writer_pg.writerow([pid_int, tgt_int])
            
    el = time.perf_counter() - t0
    log.info("CSV export finished in %.2f s.", el)


# ---------------------------------------------------------------------------
# Phase 3: PostgreSQL High-Speed Bulk Loading
# ---------------------------------------------------------------------------

def load_postgresql() -> None:
    log.info("=== PostgreSQL: Connecting and applying DDL ===")
    conn = psycopg.connect(**PG_CONFIG)
    conn.autocommit = False

    schema_sql = Path(__file__).parent.parent / "sql" / "schema.sql"
    with conn.cursor() as cur:
        with open(schema_sql, "r", encoding="utf-8") as f:
            cur.execute(f.read())
    conn.commit()
    log.info("PostgreSQL schema reset.")

    t0 = time.perf_counter()
    with conn.cursor() as cur:
        log.info("Native COPYing subreddits into PostgreSQL...")
        with open(DATA_DIR / "subreddits.csv", "r", encoding="utf-8") as f:
            with cur.copy("COPY subreddits (id, name) FROM STDIN WITH (FORMAT csv, HEADER true)") as copy:
                while data := f.read(8192):
                    copy.write(data)
        
        log.info("Native COPYing posts into PostgreSQL...")
        with open(DATA_DIR / "posts_pg.csv", "r", encoding="utf-8") as f:
            with cur.copy("COPY posts (id, post_id, source_subreddit_id, timestamp, source_type, post_label, post_properties) FROM STDIN WITH (FORMAT csv, HEADER true)") as copy:
                while data := f.read(8192):
                    copy.write(data)
        
        log.info("Native COPYing hyperlinks into PostgreSQL...")
        with open(DATA_DIR / "links_pg.csv", "r", encoding="utf-8") as f:
            with cur.copy("COPY hyperlinks (post_id, target_subreddit_id) FROM STDIN WITH (FORMAT csv, HEADER true)") as copy:
                while data := f.read(8192):
                    copy.write(data)

    conn.commit()
    el = time.perf_counter() - t0
    log.info("PostgreSQL native bulk data load finished in %.2f s.", el)

    # VACUUM ANALYZE for query planner statistics
    log.info("Running VACUUM ANALYZE to compute table statistics...")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("VACUUM ANALYZE subreddits, posts, hyperlinks;")
    conn.close()
    log.info("PostgreSQL database ready and optimized.")


# ---------------------------------------------------------------------------
# Phase 4: Neo4j High-Speed Native CSV Bulk Loading
# ---------------------------------------------------------------------------

def load_neo4j() -> None:
    log.info("=== Neo4j: Connecting and applying constraints ===")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

    # Reset graph
    with driver.session() as session:
        log.info("Clearing existing Neo4j graph...")
        session.run("MATCH (n) DETACH DELETE n")
        
        # Apply schema constraints and indexes
        schema_cypher = Path(__file__).parent.parent / "cypher" / "schema.cypher"
        with open(schema_cypher, "r", encoding="utf-8") as f:
            statements = [s.strip() for s in f.read().split(";") if s.strip()]
            for stmt in statements:
                cleaned_lines = [line for line in stmt.split('\n') if not line.strip().startswith('//')]
                cleaned_stmt = '\n'.join(cleaned_lines).strip()
                if cleaned_stmt:
                    session.run(cleaned_stmt)
    log.info("Neo4j schema and constraints applied.")

    t0 = time.perf_counter()

    with driver.session() as session:
        log.info("Loading Subreddit nodes natively via LOAD CSV...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///subreddits.csv' AS row
            CALL (row) {
                CREATE (:Subreddit {name: row.name})
            } IN TRANSACTIONS OF 10000 ROWS
        """)
        
        log.info("Loading Post nodes natively via LOAD CSV...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///posts.csv' AS row
            CALL (row) {
                MATCH (src:Subreddit {name: row.src_name})
                CREATE (p:Post {
                    post_id:         row.post_id,
                    timestamp:       datetime(row.ts),
                    source_type:     row.type,
                    post_label:      toInteger(row.label),
                    post_properties: CASE WHEN row.props = '{}' THEN [] ELSE [x IN split(substring(row.props, 1, size(row.props)-2), ',') | toFloat(x)] END
                })
                CREATE (src)-[:POSTED]->(p)
            } IN TRANSACTIONS OF 10000 ROWS
        """)
        
        log.info("Loading Hyperlink edges natively via LOAD CSV...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'file:///links.csv' AS row
            CALL (row) {
                MATCH (p:Post {post_id: row.post_id})
                MATCH (src:Subreddit)-[:POSTED]->(p)
                MATCH (tgt:Subreddit {name: row.tgt_name})
                CREATE (p)-[:REFERENCES]->(tgt)
                CREATE (src)-[:LINKED_TO {
                    post_id:         p.post_id,
                    timestamp:       p.timestamp,
                    source_type:     p.source_type,
                    post_label:      p.post_label
                }]->(tgt)
            } IN TRANSACTIONS OF 10000 ROWS
        """)

    driver.close()
    el = time.perf_counter() - t0
    log.info("Neo4j native bulk data load finished in %.2f s.", el)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="ETL Loader for PostgreSQL and Neo4j")
    parser.add_argument("--skip-postgres", action="store_true", help="Skip loading PostgreSQL")
    parser.add_argument("--skip-neo4j", action="store_true", help="Skip loading Neo4j")
    args = parser.parse_args()

    if args.skip_postgres and args.skip_neo4j:
        log.info("Both databases skipped. Nothing to do.")
        return

    subreddits, posts, hyperlinks = parse_dataset()
    if not subreddits:
        log.error("No data found! Verify TSV files exist in data/ directory.")
        sys.exit(1)

    export_csvs(subreddits, posts, hyperlinks)

    if not args.skip_postgres:
        load_postgresql()
    else:
        log.info("Skipping PostgreSQL load as requested.")

    if not args.skip_neo4j:
        load_neo4j()
    else:
        log.info("Skipping Neo4j load as requested.")
        
    log.info("=== ETL Pipeline Complete! Ready for benchmarking ===")


if __name__ == "__main__":
    main()
