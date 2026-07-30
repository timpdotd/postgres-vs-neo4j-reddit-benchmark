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
    "host":     os.getenv("PG_HOST",  "127.0.0.1"),
    "port":     int(os.getenv("PG_PORT",  "5432")),
    "dbname":   os.getenv("PG_DB",    "reddit_benchmark"),
    "user":     os.getenv("PG_USER",  "reddit_user"),
    "password": os.getenv("PG_PASS",  "reddit_password"),
}

NEO4J_URI  = os.getenv("NEO4J_URI",  "bolt://127.0.0.1:7687")
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

import argparse

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

    if not args.skip_postgres:
        load_postgresql(subreddits, posts, hyperlinks)
    else:
        log.info("Skipping PostgreSQL load as requested.")

    if not args.skip_neo4j:
        load_neo4j(subreddits, posts, hyperlinks)
    else:
        log.info("Skipping Neo4j load as requested.")
        
    log.info("=== ETL Pipeline Complete! Ready for benchmarking ===")


if __name__ == "__main__":
    main()
