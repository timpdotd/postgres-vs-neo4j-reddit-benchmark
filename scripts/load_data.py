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
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import psycopg2
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
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
# Phase 2: PostgreSQL High-Speed Bulk Loading
# ---------------------------------------------------------------------------

def load_postgresql(subreddits: dict[str, int], posts: list[tuple], hyperlinks: set[tuple]) -> None:
    log.info("=== PostgreSQL: Connecting and applying DDL ===")
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False

    schema_sql = Path(__file__).parent.parent / "sql" / "schema.sql"
    with conn.cursor() as cur:
        with open(schema_sql, "r", encoding="utf-8") as f:
            cur.execute(f.read())
    conn.commit()
    log.info("PostgreSQL schema reset.")

    t0 = time.perf_counter()
    with conn.cursor() as cur:
        # 1. COPY subreddits
        log.info("COPYing %d subreddits into PostgreSQL...", len(subreddits))
        buf_subs = io.StringIO()
        for name, sub_id in subreddits.items():
            # escape backslashes and tabs if any
            clean_name = name.replace("\\", "\\\\").replace("\t", " ")
            buf_subs.write(f"{sub_id}\t{clean_name}\n")
        buf_subs.seek(0)
        cur.copy_from(buf_subs, "subreddits", columns=("id", "name"))

        # 2. COPY posts
        log.info("COPYing %d posts into PostgreSQL...", len(posts))
        buf_posts = io.StringIO()
        for p in posts:
            # p: (post_int_id, post_id, src_id, ts, type, label, props_pg, props_list, src_name)
            pid_clean = p[1].replace("\\", "\\\\").replace("\t", " ")
            buf_posts.write(f"{p[0]}\t{pid_clean}\t{p[2]}\t{p[3]}\t{p[4]}\t{p[5]}\t{p[6]}\n")
        buf_posts.seek(0)
        cur.copy_from(buf_posts, "posts", columns=("id", "post_id", "source_subreddit_id", "timestamp", "source_type", "post_label", "post_properties"))

        # 3. COPY hyperlinks
        log.info("COPYing %d hyperlinks into PostgreSQL...", len(hyperlinks))
        buf_links = io.StringIO()
        for post_int_id, tgt_id in hyperlinks:
            buf_links.write(f"{post_int_id}\t{tgt_id}\n")
        buf_links.seek(0)
        cur.copy_from(buf_links, "hyperlinks", columns=("post_id", "target_subreddit_id"))

    conn.commit()
    el = time.perf_counter() - t0
    log.info("PostgreSQL bulk data load finished in %.2f s.", el)

    # VACUUM ANALYZE for query planner statistics
    log.info("Running VACUUM ANALYZE to compute table statistics...")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("VACUUM ANALYZE subreddits, posts, hyperlinks;")
    conn.close()
    log.info("PostgreSQL database ready and optimized.")


# ---------------------------------------------------------------------------
# Phase 3: Neo4j High-Speed UNWIND Bulk Loading
# ---------------------------------------------------------------------------

def load_neo4j(subreddits: dict[str, int], posts: list[tuple], hyperlinks: set[tuple]) -> None:
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
                if not stmt.startswith("//"):
                    session.run(stmt)
    log.info("Neo4j schema and constraints applied.")

    t0 = time.perf_counter()

    # 1. Load Subreddit Nodes
    log.info("Loading %d Subreddit nodes into Neo4j...", len(subreddits))
    sub_names = [{"name": name} for name in subreddits.keys()]
    with driver.session() as session:
        for i in range(0, len(sub_names), BATCH_SIZE_NEO):
            batch = sub_names[i : i + BATCH_SIZE_NEO]
            session.run("""
                UNWIND $batch AS item
                CREATE (:Subreddit {name: item.name})
            """, batch=batch)

    # Build memory lookup for hyperlinks: post_int_id -> list of target subreddit integer IDs
    links_by_post: dict[int, list[int]] = {}
    for pid_int, tgt_int in hyperlinks:
        links_by_post.setdefault(pid_int, []).append(tgt_int)

    # Invert subreddits lookup: id -> name
    sub_id_to_name = {v: k for k, v in subreddits.items()}

    # 2. Load Posts and Relationships in batches
    log.info("Loading %d Post nodes and relationships into Neo4j...", len(posts))
    post_batches = []
    current_batch = []

    for p in posts:
        # p: (post_int_id, post_id, src_id, ts, type, label, props_pg, props_list, src_name)
        post_int_id = p[0]
        tgt_ids = links_by_post.get(post_int_id, [])
        tgt_names = [sub_id_to_name[tid] for tid in tgt_ids]

        # Format timestamp for Neo4j datetime()
        ts_neo = p[3].replace(" ", "T")

        current_batch.append({
            "pid":   p[1],
            "src":   p[8],
            "tgts":  tgt_names,
            "ts":    ts_neo,
            "type":  p[4],
            "label": p[5],
            "props": p[7],
        })

        if len(current_batch) >= BATCH_SIZE_NEO:
            post_batches.append(current_batch)
            current_batch = []
    if current_batch:
        post_batches.append(current_batch)

    with driver.session() as session:
        for idx, batch in enumerate(post_batches, 1):
            if idx % 50 == 0 or idx == len(post_batches):
                log.info("  -> Processing Post batch %d/%d...", idx, len(post_batches))
            session.run("""
                UNWIND $batch AS row
                MATCH (src:Subreddit {name: row.src})
                CREATE (p:Post {
                    post_id:         row.pid,
                    timestamp:       datetime(row.ts),
                    source_type:     row.type,
                    post_label:      row.label,
                    post_properties: row.props
                })
                CREATE (src)-[:POSTED]->(p)
                WITH p, row
                UNWIND row.tgts AS tgt_name
                MATCH (tgt:Subreddit {name: tgt_name})
                CREATE (p)-[:REFERENCES]->(tgt)
            """, batch=batch)

    driver.close()
    el = time.perf_counter() - t0
    log.info("Neo4j bulk data load finished in %.2f s.", el)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    subreddits, posts, hyperlinks = parse_dataset()
    if not subreddits:
        log.error("No data found! Verify TSV files exist in data/ directory.")
        sys.exit(1)

    load_postgresql(subreddits, posts, hyperlinks)
    load_neo4j(subreddits, posts, hyperlinks)
    log.info("=== ETL Pipeline Complete! Ready for benchmarking ===")


if __name__ == "__main__":
    main()
