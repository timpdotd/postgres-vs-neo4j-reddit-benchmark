#!/usr/bin/env python3
"""
load_data.py — ETL pipeline for the Reddit Hyperlink Network benchmark.

Reads both SNAP TSV files from ../data/, deduplicates subreddit names,
and bulk-loads data into PostgreSQL (via COPY) and Neo4j (via two-phase
UNWIND batching: nodes first, then relationships).

Usage:
    python scripts/load_data.py [--pg-only] [--neo4j-only] [--limit N]

Environment overrides (all have sensible defaults):
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS
    NEO4J_URI, NEO4J_USER, NEO4J_PASS
"""

import argparse
import csv
import io
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import psycopg2
import psycopg2.extras
from neo4j import GraphDatabase

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent.parent / "data"
SQL_DIR  = Path(__file__).parent.parent / "sql"

TSV_FILES = {
    "body":  DATA_DIR / "soc-redditHyperlinks-body.tsv",
    "title": DATA_DIR / "soc-redditHyperlinks-title.tsv",
}

PG_CONFIG = {
    "host":     os.getenv("PG_HOST",  "localhost"),
    "port":     int(os.getenv("PG_PORT",  "5432")),
    "dbname":   os.getenv("PG_DB",   "reddit_benchmark"),
    "user":     os.getenv("PG_USER", "reddit_user"),
    "password": os.getenv("PG_PASS", "reddit_password"),
}

NEO4J_URI  = os.getenv("NEO4J_URI",  "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "reddit_password")

# Neo4j batch sizes — nodes are small (name only), rels carry the full payload
NEO4J_NODE_BATCH = 5_000
NEO4J_REL_BATCH  = 1_000

# PostgreSQL bulk-copy chunk size
PG_COPY_CHUNK = 50_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# TSV parsing
# ---------------------------------------------------------------------------

# Official SNAP column header names:
# SOURCE_SUBREDDIT  TARGET_SUBREDDIT  POST_ID  TIMESTAMP  POST_LABEL  POST_PROPERTIES
_REQUIRED_COLS = {"SOURCE_SUBREDDIT", "TARGET_SUBREDDIT", "POST_ID", "TIMESTAMP", "POST_LABEL"}


def _parse_post_properties(raw: str) -> list[float] | None:
    """Parse the 86-value comma-separated POST_PROPERTIES field."""
    raw = raw.strip()
    if not raw:
        return None
    try:
        vals = [float(v) for v in raw.split(",")]
        return vals if len(vals) == 86 else None   # reject malformed vectors
    except ValueError:
        return None


def iter_rows(source_type: str, limit: int | None = None) -> Generator[dict, None, None]:
    """
    Yield parsed row dicts from one SNAP TSV file.
    source_type: 'body' or 'title'
    """
    path = TSV_FILES[source_type]
    if not path.exists():
        log.error("Missing data file: %s", path)
        sys.exit(1)

    log.info("Parsing %s (%.0f MB)...", path.name, path.stat().st_size / 1e6)

    count = skipped = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if not _REQUIRED_COLS.issubset(row.keys()):
                skipped += 1
                continue

            src  = row["SOURCE_SUBREDDIT"].strip().lower()
            tgt  = row["TARGET_SUBREDDIT"].strip().lower()
            pid  = row["POST_ID"].strip()
            ts   = row["TIMESTAMP"].strip()
            lbl  = row["POST_LABEL"].strip()
            prop = row.get("POST_PROPERTIES", "").strip()

            if not (src and tgt and pid and ts and lbl):
                skipped += 1
                continue

            try:
                post_label = int(lbl)
                timestamp  = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                skipped += 1
                continue

            if post_label not in (-1, 1):
                skipped += 1
                continue

            yield {
                "source":          src,
                "target":          tgt,
                "post_id":         pid,
                "timestamp":       timestamp,
                "source_type":     source_type,
                "post_label":      post_label,
                "post_properties": _parse_post_properties(prop),
            }

            count += 1
            if limit and count >= limit:
                break

    log.info("  → %d rows parsed, %d skipped from %s", count, skipped, path.name)


# ---------------------------------------------------------------------------
# PostgreSQL loader
# ---------------------------------------------------------------------------

def _pg_apply_schema(conn) -> None:
    schema_sql = (SQL_DIR / "schema.sql").read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(schema_sql)
    conn.commit()
    log.info("PostgreSQL schema applied.")


def _pg_upsert_subreddits(conn, names: set[str]) -> dict[str, int]:
    """Insert all unique subreddit names, return name→id map."""
    log.info("Upserting %d subreddit names into PostgreSQL...", len(names))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO subreddits (name) VALUES %s ON CONFLICT (name) DO NOTHING",
            [(n,) for n in sorted(names)],
            page_size=5_000,
        )
        conn.commit()
        cur.execute("SELECT name, id FROM subreddits")
        return {row[0]: row[1] for row in cur.fetchall()}


def _pg_copy_hyperlinks(conn, rows: list[dict], sub_ids: dict[str, int]) -> int:
    """Bulk-insert hyperlinks using PostgreSQL COPY protocol (fastest method)."""
    buf = io.StringIO()
    inserted = 0
    for row in rows:
        src_id = sub_ids.get(row["source"])
        tgt_id = sub_ids.get(row["target"])
        if src_id is None or tgt_id is None:
            continue

        props_str = (
            "{" + ",".join(str(v) for v in row["post_properties"]) + "}"
            if row["post_properties"]
            else "\\N"
        )
        buf.write(
            f"{src_id}\t{tgt_id}\t{row['post_id']}\t"
            f"{row['timestamp'].isoformat()}\t"
            f"{row['source_type']}\t{row['post_label']}\t{props_str}\n"
        )
        inserted += 1

    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            "COPY hyperlinks "
            "(source_subreddit_id, target_subreddit_id, post_id, timestamp, "
            " source_type, post_label, post_properties) "
            "FROM STDIN WITH (FORMAT TEXT, NULL '\\N')",
            buf,
        )
    conn.commit()
    return inserted


def _pg_vacuum_analyze(conn) -> None:
    """
    Run VACUUM ANALYZE so the query planner has accurate statistics.
    Must run outside a transaction block (autocommit=True).
    Without this, EXPLAIN plans after a fresh load can be wildly wrong.
    """
    log.info("Running VACUUM ANALYZE (updating planner statistics)...")
    old_isolation = conn.isolation_level
    conn.set_isolation_level(0)  # AUTOCOMMIT
    with conn.cursor() as cur:
        cur.execute("VACUUM ANALYZE subreddits")
        cur.execute("VACUUM ANALYZE hyperlinks")
    conn.set_isolation_level(old_isolation)
    log.info("VACUUM ANALYZE complete.")


def load_postgres(all_rows: list[dict]) -> None:
    log.info("=== PostgreSQL Load ===")
    t0 = time.perf_counter()

    conn = psycopg2.connect(**PG_CONFIG)
    _pg_apply_schema(conn)

    # Collect all unique subreddit names
    names: set[str] = {row["source"] for row in all_rows} | {row["target"] for row in all_rows}
    sub_ids = _pg_upsert_subreddits(conn, names)

    # Bulk-insert hyperlinks in chunks via COPY
    total_inserted = 0
    for i in range(0, len(all_rows), PG_COPY_CHUNK):
        chunk = all_rows[i : i + PG_COPY_CHUNK]
        total_inserted += _pg_copy_hyperlinks(conn, chunk, sub_ids)
        log.info("  PG: %d / %d hyperlinks inserted...", total_inserted, len(all_rows))

    # Critical: update planner statistics before benchmarking
    _pg_vacuum_analyze(conn)

    conn.close()
    log.info(
        "PostgreSQL load complete: %d subreddits, %d hyperlinks in %.1fs",
        len(sub_ids), total_inserted, time.perf_counter() - t0,
    )


# ---------------------------------------------------------------------------
# Neo4j loader — two-phase approach
# ---------------------------------------------------------------------------
# Phase 1: Create all Subreddit nodes (MERGE on name, batched)
#          → Each name hits the uniqueness index exactly once.
# Phase 2: MATCH both endpoint nodes (they exist), CREATE relationship
#          → No redundant MERGE lookups per relationship.
#
# This is ~3–5× faster than the naive single-pass MERGE+CREATE approach
# because MERGE inside a relationship batch repeatedly re-checks uniqueness.
# ---------------------------------------------------------------------------

def _neo4j_apply_schema(driver) -> None:
    schema = (Path(__file__).parent.parent / "cypher" / "schema.cypher").read_text("utf-8")
    statements = [
        s.strip() for s in schema.split(";")
        if s.strip() and not s.strip().startswith("//")
    ]
    with driver.session() as session:
        for stmt in statements:
            try:
                session.run(stmt)
            except Exception as exc:
                log.warning("Schema stmt (may be harmless): %s", exc)
    log.info("Neo4j schema applied (%d statements).", len(statements))


def _neo4j_phase1_nodes(driver, names: set[str]) -> None:
    """Phase 1: MERGE all unique Subreddit nodes in batches."""
    log.info("Neo4j phase 1 — upserting %d Subreddit nodes...", len(names))
    name_list = sorted(names)
    for i in range(0, len(name_list), NEO4J_NODE_BATCH):
        batch = name_list[i : i + NEO4J_NODE_BATCH]
        with driver.session() as session:
            session.run(
                "UNWIND $names AS name MERGE (:Subreddit {name: name})",
                names=batch,
            )
    log.info("  Neo4j nodes done.")


def _neo4j_phase2_rels(driver, rows: list[dict]) -> None:
    """Phase 2: MATCH existing nodes, CREATE relationships (no MERGE on rels)."""
    log.info("Neo4j phase 2 — creating %d HYPERLINKS_TO relationships...", len(rows))
    total = 0
    for i in range(0, len(rows), NEO4J_REL_BATCH):
        batch = rows[i : i + NEO4J_REL_BATCH]
        payload = [
            {
                "src":             r["source"],
                "tgt":             r["target"],
                "post_id":         r["post_id"],
                "timestamp":       r["timestamp"].isoformat(),
                "source_type":     r["source_type"],
                "post_label":      r["post_label"],
                "post_properties": r["post_properties"],  # None → null
            }
            for r in batch
        ]
        with driver.session() as session:
            session.run(
                """
                UNWIND $rels AS rel
                MATCH (src:Subreddit {name: rel.src})
                MATCH (tgt:Subreddit {name: rel.tgt})
                CREATE (src)-[:HYPERLINKS_TO {
                    post_id:         rel.post_id,
                    timestamp:       datetime(rel.timestamp),
                    source_type:     rel.source_type,
                    post_label:      rel.post_label,
                    post_properties: rel.post_properties
                }]->(tgt)
                """,
                rels=payload,
            )
        total += len(batch)
        if total % 50_000 == 0 or total == len(rows):
            log.info("  Neo4j: %d / %d relationships created...", total, len(rows))


def load_neo4j(all_rows: list[dict]) -> None:
    log.info("=== Neo4j Load (two-phase) ===")
    t0 = time.perf_counter()

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    _neo4j_apply_schema(driver)

    names = {r["source"] for r in all_rows} | {r["target"] for r in all_rows}
    _neo4j_phase1_nodes(driver, names)
    _neo4j_phase2_rels(driver, all_rows)

    driver.close()
    log.info(
        "Neo4j load complete: %d nodes, %d relationships in %.1fs",
        len(names), len(all_rows), time.perf_counter() - t0,
    )


# ---------------------------------------------------------------------------
# Row-count verification
# ---------------------------------------------------------------------------

def verify_counts(pg_only: bool, neo4j_only: bool) -> None:
    log.info("=== Row-Count Verification ===")
    pg_sub = pg_hl = neo_sub = neo_hl = None

    if not neo4j_only:
        conn = psycopg2.connect(**PG_CONFIG)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM subreddits")
            pg_sub = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM hyperlinks")
            pg_hl = cur.fetchone()[0]
        conn.close()
        log.info("  PostgreSQL — subreddits: %d, hyperlinks: %d", pg_sub, pg_hl)

    if not pg_only:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        with driver.session() as session:
            neo_sub = session.run("MATCH (s:Subreddit) RETURN count(s) AS n").single()["n"]
            neo_hl  = session.run("MATCH ()-[r:HYPERLINKS_TO]->() RETURN count(r) AS n").single()["n"]
        driver.close()
        log.info("  Neo4j — Subreddit nodes: %d, HYPERLINKS_TO: %d", neo_sub, neo_hl)

    if pg_sub is not None and neo_sub is not None:
        match = (pg_sub == neo_sub) and (pg_hl == neo_hl)
        if match:
            log.info("  ✓ Counts match between PostgreSQL and Neo4j.")
        else:
            log.warning(
                "COUNT MISMATCH! PG(%d subs, %d hl) vs Neo4j(%d nodes, %d rels)",
                pg_sub, pg_hl, neo_sub, neo_hl,
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="ETL loader for the Reddit benchmark.")
    parser.add_argument("--pg-only",    action="store_true", help="Load PostgreSQL only")
    parser.add_argument("--neo4j-only", action="store_true", help="Load Neo4j only")
    parser.add_argument(
        "--limit", type=int, default=None, metavar="N",
        help="Cap rows per TSV file (useful for quick smoke tests)",
    )
    args = parser.parse_args()

    t_global = time.perf_counter()

    log.info("Reading SNAP dataset files...")
    all_rows: list[dict] = []
    for source_type in ("body", "title"):
        for row in iter_rows(source_type, limit=args.limit):
            all_rows.append(row)
    log.info("Total rows to load: %d", len(all_rows))

    if not args.neo4j_only:
        load_postgres(all_rows)

    if not args.pg_only:
        load_neo4j(all_rows)

    verify_counts(args.pg_only, args.neo4j_only)
    log.info("All done in %.1fs.", time.perf_counter() - t_global)


if __name__ == "__main__":
    main()
