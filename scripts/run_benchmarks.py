#!/usr/bin/env python3
"""
run_benchmarks.py — PostgreSQL vs Neo4j benchmark runner.

Executes 10 analytical queries of increasing complexity on both databases.

Timing methodology:
  PostgreSQL  — server-side EXPLAIN (ANALYZE, FORMAT JSON) "Execution Time" (ms).
                Excludes Python driver overhead and network transfer.
  Neo4j       — driver-reported result.consume().result_consumed_after (ms).
                Measured from when the query was sent to when all results were
                consumed; includes network transfer from Docker to localhost.
                Both metrics exclude Python-side computation but are not
                directly comparable. Differences are documented in the output.

Cold vs warm cache:
  Run 0       — "cold" run: plain execution, no EXPLAIN, wall-clock timed.
                Represents first-access performance (partially warm OS disk cache,
                partially cold DB buffer cache).
  Runs 1–N    — "warm" timed runs via server-side timing (EXPLAIN ANALYZE / driver).
  The median of runs 1–N is the headline benchmark figure.

Usage:
    python scripts/run_benchmarks.py [--runs N] [--output PATH] [--pg-only] [--neo4j-only]

Environment overrides:
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS
    NEO4J_URI, NEO4J_USER, NEO4J_PASS
"""

import argparse
import json
import logging
import os
import time
from pathlib import Path
from statistics import median, mean, stdev
from typing import Any

import psycopg2
import psycopg2.extras
from neo4j import GraphDatabase

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent.parent / "data"

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

DEFAULT_RUNS   = 5
DEFAULT_OUTPUT = DATA_DIR / "benchmark_results.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Query registry
# ---------------------------------------------------------------------------
# Each entry contains:
#   id          — short identifier (T1-B … T5-B)
#   name        — human-readable description
#   tier        — 1–5 difficulty tier
#   pg_sql      — parameterized SQL (psycopg2 %(name)s style)
#   pg_params   — lambda(seeds) → dict of SQL parameters (or None)
#   neo_cypher  — Cypher query string ($name style)
#   neo_params  — lambda(seeds) → dict of Cypher parameters (or None)
# ---------------------------------------------------------------------------

QUERIES: list[dict[str, Any]] = [
    # ── Tier 1 ──────────────────────────────────────────────────────────────
    {
        "id":   "T1-B",
        "name": "All outgoing links from seed subreddit",
        "tier": 1,
        "pg_sql": """
            SELECT s_tgt.name, h.post_id, h.timestamp, h.source_type, h.post_label
            FROM hyperlinks h
            JOIN subreddits s_src ON s_src.id = h.source_subreddit_id
            JOIN subreddits s_tgt ON s_tgt.id = h.target_subreddit_id
            WHERE s_src.name = %(seed)s
            ORDER BY h.timestamp DESC
        """,
        "pg_params":  lambda s: {"seed": s["seed"]},
        "neo_cypher": """
            MATCH (src:Subreddit {name: $seed})-[h:HYPERLINKS_TO]->(tgt:Subreddit)
            RETURN tgt.name AS target_subreddit,
                   h.post_id, h.timestamp, h.source_type, h.post_label
            ORDER BY h.timestamp DESC
        """,
        "neo_params": lambda s: {"seed": s["seed"]},
    },
    {
        "id":   "T1-C",
        "name": "Hostile links only from seed subreddit",
        "tier": 1,
        "pg_sql": """
            SELECT s_tgt.name, h.post_id, h.timestamp, h.source_type
            FROM hyperlinks h
            JOIN subreddits s_src ON s_src.id = h.source_subreddit_id
            JOIN subreddits s_tgt ON s_tgt.id = h.target_subreddit_id
            WHERE s_src.name = %(seed)s AND h.post_label = -1
            ORDER BY h.timestamp DESC
        """,
        "pg_params":  lambda s: {"seed": s["seed"]},
        "neo_cypher": """
            MATCH (src:Subreddit {name: $seed})-[h:HYPERLINKS_TO {post_label: -1}]->(tgt:Subreddit)
            RETURN tgt.name AS target_subreddit,
                   h.post_id, h.timestamp, h.source_type
            ORDER BY h.timestamp DESC
        """,
        "neo_params": lambda s: {"seed": s["seed"]},
    },
    # ── Tier 2 ──────────────────────────────────────────────────────────────
    {
        "id":   "T2-A",
        "name": "Global in-degree ranking (top-20 most-linked-to subreddits)",
        "tier": 2,
        "pg_sql": """
            SELECT s.name AS subreddit, COUNT(*) AS in_degree
            FROM hyperlinks h
            JOIN subreddits s ON s.id = h.target_subreddit_id
            GROUP BY s.name
            ORDER BY in_degree DESC
            LIMIT 20
        """,
        "pg_params":  lambda s: None,
        "neo_cypher": """
            MATCH ()-[h:HYPERLINKS_TO]->(tgt:Subreddit)
            RETURN tgt.name AS subreddit, count(h) AS in_degree
            ORDER BY in_degree DESC LIMIT 20
        """,
        "neo_params": lambda s: None,
    },
    {
        "id":   "T2-B",
        "name": "Top-20 subreddits by hostile outgoing link count",
        "tier": 2,
        "pg_sql": """
            SELECT s.name AS subreddit, COUNT(*) AS hostile_count
            FROM hyperlinks h
            JOIN subreddits s ON s.id = h.source_subreddit_id
            WHERE h.post_label = -1
            GROUP BY s.name
            ORDER BY hostile_count DESC
            LIMIT 20
        """,
        "pg_params":  lambda s: None,
        "neo_cypher": """
            MATCH (src:Subreddit)-[:HYPERLINKS_TO {post_label: -1}]->()
            RETURN src.name AS subreddit, count(*) AS hostile_count
            ORDER BY hostile_count DESC LIMIT 20
        """,
        "neo_params": lambda s: None,
    },
    # ── Tier 3 ──────────────────────────────────────────────────────────────
    {
        "id":   "T3-A",
        "name": "2-hop common targets of seed_a and seed_b",
        "tier": 3,
        "pg_sql": """
            WITH targets_a AS (
                SELECT DISTINCT h.target_subreddit_id
                FROM hyperlinks h JOIN subreddits s ON s.id = h.source_subreddit_id
                WHERE s.name = %(seed_a)s
            ),
            targets_b AS (
                SELECT DISTINCT h.target_subreddit_id
                FROM hyperlinks h JOIN subreddits s ON s.id = h.source_subreddit_id
                WHERE s.name = %(seed_b)s
            )
            SELECT s.name AS shared_target
            FROM targets_a a
            JOIN targets_b b USING (target_subreddit_id)
            JOIN subreddits s ON s.id = a.target_subreddit_id
            ORDER BY s.name LIMIT 25
        """,
        "pg_params":  lambda s: {"seed_a": s["seed_a"], "seed_b": s["seed_b"]},
        "neo_cypher": """
            MATCH (a:Subreddit {name: $seed_a})-[:HYPERLINKS_TO]->(shared:Subreddit)
            WITH shared
            MATCH (b:Subreddit {name: $seed_b})-[:HYPERLINKS_TO]->(shared)
            RETURN shared.name AS shared_target
            ORDER BY shared_target LIMIT 25
        """,
        "neo_params": lambda s: {"seed_a": s["seed_a"], "seed_b": s["seed_b"]},
    },
    {
        "id":   "T3-C",
        "name": "Common hostile attackers of both seed_a and seed_b",
        "tier": 3,
        "pg_sql": """
            WITH attackers_a AS (
                SELECT DISTINCT h.source_subreddit_id
                FROM hyperlinks h JOIN subreddits s ON s.id = h.target_subreddit_id
                WHERE s.name = %(seed_a)s AND h.post_label = -1
            ),
            attackers_b AS (
                SELECT DISTINCT h.source_subreddit_id
                FROM hyperlinks h JOIN subreddits s ON s.id = h.target_subreddit_id
                WHERE s.name = %(seed_b)s AND h.post_label = -1
            )
            SELECT s.name AS common_attacker
            FROM attackers_a a
            JOIN attackers_b b USING (source_subreddit_id)
            JOIN subreddits s ON s.id = a.source_subreddit_id
            ORDER BY s.name LIMIT 25
        """,
        "pg_params":  lambda s: {"seed_a": s["seed_a"], "seed_b": s["seed_b"]},
        "neo_cypher": """
            MATCH (atk:Subreddit)-[:HYPERLINKS_TO {post_label: -1}]->(a:Subreddit {name: $seed_a})
            WITH atk
            MATCH (atk)-[:HYPERLINKS_TO {post_label: -1}]->(b:Subreddit {name: $seed_b})
            RETURN atk.name AS common_attacker
            ORDER BY atk.name LIMIT 25
        """,
        "neo_params": lambda s: {"seed_a": s["seed_a"], "seed_b": s["seed_b"]},
    },
    # ── Tier 4 ──────────────────────────────────────────────────────────────
    {
        "id":   "T4-A",
        "name": "Mutual hostile pairs (global)",
        "tier": 4,
        "pg_sql": """
            SELECT s_a.name AS sub_a, s_b.name AS sub_b, COUNT(*) AS mutual_hostile_links
            FROM hyperlinks ab
            JOIN hyperlinks ba
                ON  ba.source_subreddit_id = ab.target_subreddit_id
                AND ba.target_subreddit_id = ab.source_subreddit_id
                AND ba.post_label = -1
            JOIN subreddits s_a ON s_a.id = ab.source_subreddit_id
            JOIN subreddits s_b ON s_b.id = ab.target_subreddit_id
            WHERE ab.post_label = -1
              AND ab.source_subreddit_id < ab.target_subreddit_id
            GROUP BY s_a.name, s_b.name
            ORDER BY mutual_hostile_links DESC
            LIMIT 20
        """,
        "pg_params":  lambda s: None,
        "neo_cypher": """
            MATCH (a:Subreddit)-[ab:HYPERLINKS_TO {post_label: -1}]->(b:Subreddit)
                  -[:HYPERLINKS_TO {post_label: -1}]->(a)
            WHERE id(a) < id(b)
            RETURN a.name AS sub_a, b.name AS sub_b, count(ab) AS mutual_hostile_links
            ORDER BY mutual_hostile_links DESC LIMIT 20
        """,
        "neo_params": lambda s: None,
    },
    {
        "id":   "T4-B",
        "name": "Bounded BFS depth 3 from seed_bfs",
        "tier": 4,
        "pg_sql": """
            WITH RECURSIVE bfs(node_id, depth, path) AS (
                SELECT s.id, 0, ARRAY[s.id]
                FROM subreddits s WHERE s.name = %(seed_bfs)s
                UNION ALL
                SELECT h.target_subreddit_id, b.depth + 1, b.path || h.target_subreddit_id
                FROM bfs b
                JOIN hyperlinks h ON h.source_subreddit_id = b.node_id
                WHERE b.depth < 3
                  AND NOT (h.target_subreddit_id = ANY(b.path))
            )
            SELECT s.name AS reachable_subreddit, MIN(bfs.depth) AS min_hops
            FROM bfs JOIN subreddits s ON s.id = bfs.node_id
            WHERE bfs.depth > 0
            GROUP BY s.name ORDER BY min_hops, s.name LIMIT 500
        """,
        "pg_params":  lambda s: {"seed_bfs": s["seed_bfs"]},
        "neo_cypher": """
            MATCH p = (src:Subreddit {name: $seed_bfs})-[:HYPERLINKS_TO*1..3]->(tgt:Subreddit)
            WHERE src <> tgt
            WITH tgt, min(length(p)) AS min_hops
            RETURN tgt.name AS reachable_subreddit, min_hops
            ORDER BY min_hops, tgt.name LIMIT 500
        """,
        "neo_params": lambda s: {"seed_bfs": s["seed_bfs"]},
    },
    # ── Tier 5 ──────────────────────────────────────────────────────────────
    {
        "id":   "T5-A",
        "name": "3-node hostile-sentiment cycles seeded from seed",
        "tier": 5,
        "pg_sql": """
            SELECT s_a.name AS node_a, s_b.name AS node_b, s_c.name AS node_c
            FROM hyperlinks ab
            JOIN hyperlinks bc
                ON  bc.source_subreddit_id = ab.target_subreddit_id
                AND bc.post_label = -1
            JOIN hyperlinks ca
                ON  ca.source_subreddit_id = bc.target_subreddit_id
                AND ca.target_subreddit_id = ab.source_subreddit_id
                AND ca.post_label = -1
            JOIN subreddits s_a ON s_a.id = ab.source_subreddit_id
            JOIN subreddits s_b ON s_b.id = ab.target_subreddit_id
            JOIN subreddits s_c ON s_c.id = bc.target_subreddit_id
            WHERE ab.post_label = -1
              AND s_a.name = %(seed)s
              AND ab.target_subreddit_id < bc.target_subreddit_id
            LIMIT 50
        """,
        "pg_params":  lambda s: {"seed": s["seed"]},
        "neo_cypher": """
            MATCH (a:Subreddit {name: $seed})
                  -[:HYPERLINKS_TO {post_label: -1}]->(b:Subreddit)
                  -[:HYPERLINKS_TO {post_label: -1}]->(c:Subreddit)
                  -[:HYPERLINKS_TO {post_label: -1}]->(a)
            WHERE id(b) < id(c)
            RETURN a.name AS node_a, b.name AS node_b, c.name AS node_c
            LIMIT 50
        """,
        "neo_params": lambda s: {"seed": s["seed"]},
    },
    {
        "id":   "T5-B",
        "name": "Bounded BFS depth 4 from seed_bfs",
        "tier": 5,
        "pg_sql": """
            WITH RECURSIVE bfs(node_id, depth, path) AS (
                SELECT s.id, 0, ARRAY[s.id]
                FROM subreddits s WHERE s.name = %(seed_bfs)s
                UNION ALL
                SELECT h.target_subreddit_id, b.depth + 1, b.path || h.target_subreddit_id
                FROM bfs b
                JOIN hyperlinks h ON h.source_subreddit_id = b.node_id
                WHERE b.depth < 4
                  AND NOT (h.target_subreddit_id = ANY(b.path))
            )
            SELECT s.name AS reachable_subreddit, MIN(bfs.depth) AS min_hops
            FROM bfs JOIN subreddits s ON s.id = bfs.node_id
            WHERE bfs.depth > 0
            GROUP BY s.name ORDER BY min_hops, s.name LIMIT 500
        """,
        "pg_params":  lambda s: {"seed_bfs": s["seed_bfs"]},
        "neo_cypher": """
            MATCH p = (src:Subreddit {name: $seed_bfs})-[:HYPERLINKS_TO*1..4]->(tgt:Subreddit)
            WHERE src <> tgt
            WITH tgt, min(length(p)) AS min_hops
            RETURN tgt.name AS reachable_subreddit, min_hops
            ORDER BY min_hops, tgt.name LIMIT 500
        """,
        "neo_params": lambda s: {"seed_bfs": s["seed_bfs"]},
    },
]


# ---------------------------------------------------------------------------
# Seed selection
# ---------------------------------------------------------------------------

def pick_seeds(conn) -> dict[str, str]:
    """
    Auto-select benchmark seeds from the loaded dataset.

    seed      (T1-B, T1-C, T5-A) — top hostile sender: a community with rich
              hostile outgoing links, interesting for cycle detection.
    seed_a/b  (T3-A, T3-C)       — top-2 by total out-degree: popular enough
              to share many common targets and attackers.
    seed_bfs  (T4-B, T5-B)       — ~rank 200 by out-degree: medium-low degree
              to keep BFS expansion tractable at depth 3 and 4.
    """
    with conn.cursor() as cur:
        # Top subreddits by total out-degree
        cur.execute("""
            SELECT s.name, COUNT(*) AS c
            FROM hyperlinks h
            JOIN subreddits s ON s.id = h.source_subreddit_id
            GROUP BY s.name ORDER BY c DESC LIMIT 210
        """)
        by_outdeg = cur.fetchall()  # list of (name, count)

        # Top subreddit by hostile out-degree (for T1-C and T5-A)
        cur.execute("""
            SELECT s.name
            FROM hyperlinks h
            JOIN subreddits s ON s.id = h.source_subreddit_id
            WHERE h.post_label = -1
            GROUP BY s.name ORDER BY COUNT(*) DESC LIMIT 1
        """)
        top_hostile = cur.fetchone()[0]

    seeds = {
        "seed":     top_hostile,
        "seed_a":   by_outdeg[0][0],
        "seed_b":   by_outdeg[1][0],
        "seed_bfs": by_outdeg[199][0],   # rank-200 (0-indexed: 199)
    }

    log.info("Seeds selected:")
    log.info("  seed (top hostile sender)  : %s", seeds["seed"])
    log.info("  seed_a (rank-1 out-degree) : %s (out-deg %d)", seeds["seed_a"], by_outdeg[0][1])
    log.info("  seed_b (rank-2 out-degree) : %s (out-deg %d)", seeds["seed_b"], by_outdeg[1][1])
    log.info("  seed_bfs (rank-200)        : %s (out-deg %d)", seeds["seed_bfs"], by_outdeg[199][1])
    return seeds


# ---------------------------------------------------------------------------
# PostgreSQL timing helpers
# ---------------------------------------------------------------------------

def _pg_cold_run(conn, sql: str, params: dict | None) -> tuple[float, int]:
    """
    Execute the query once without EXPLAIN ANALYZE.
    Returns (wall_clock_ms, row_count).
    Used for the cold-cache run — measures raw client-observed latency.
    """
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute(sql, params)
        rows = cur.fetchall()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return elapsed_ms, len(rows)


def _pg_timed_run(conn, sql: str, params: dict | None) -> tuple[float, int]:
    """
    Execute the query via EXPLAIN (ANALYZE, FORMAT JSON) and extract the
    server-reported Execution Time. Returns (exec_ms, actual_rows_approx).
    EXPLAIN ANALYZE runs the query internally — results are not returned to
    the client, but all rows are produced on the server side.
    """
    explain_sql = "EXPLAIN (ANALYZE, FORMAT JSON) " + sql
    with conn.cursor() as cur:
        cur.execute(explain_sql, params)
        raw = cur.fetchone()[0]
        # psycopg2 returns JSON as a Python list; older versions return a string
        plan = raw if isinstance(raw, list) else __import__("json").loads(raw)
        exec_ms = plan[0]["Execution Time"]
        actual_rows = plan[0]["Plan"].get("Actual Rows", -1)
    return exec_ms, actual_rows


def run_pg_query(conn, query: dict, seeds: dict, n_runs: int) -> dict:
    """Run one query on PostgreSQL: 1 cold + n_runs warm server-timed runs."""
    q_id   = query["id"]
    sql    = query["pg_sql"]
    params = query["pg_params"](seeds)

    log.info("  [PG] %s — cold run...", q_id)
    try:
        cold_ms, cold_rows = _pg_cold_run(conn, sql, params)
        log.info("    cold: %.1f ms  (%d rows)", cold_ms, cold_rows)
    except Exception as exc:
        log.error("    cold run FAILED: %s", exc)
        conn.rollback()
        return _error_result(q_id, query["name"], query["tier"], "postgresql", n_runs, str(exc))

    warm_times: list[float] = []
    for i in range(1, n_runs + 1):
        try:
            ms, rows = _pg_timed_run(conn, sql, params)
            warm_times.append(ms)
            log.info("    warm %d/%d: %.1f ms", i, n_runs, ms)
        except Exception as exc:
            log.error("    warm run %d FAILED: %s", i, exc)
            conn.rollback()
            warm_times.append(float("nan"))

    valid = [t for t in warm_times if not __import__("math").isnan(t)]
    return {
        "query_id":     q_id,
        "query_name":   query["name"],
        "tier":         query["tier"],
        "db":           "postgresql",
        "timing_method":"EXPLAIN ANALYZE Execution Time (server-side ms)",
        "seeds_used":   params or {},
        "cold_ms":      cold_ms,
        "cold_rows":    cold_rows,
        "warm_runs":    n_runs,
        "warm_times_ms": warm_times,
        "median_ms":    median(valid) if valid else None,
        "mean_ms":      mean(valid) if valid else None,
        "stdev_ms":     stdev(valid) if len(valid) > 1 else 0.0,
        "min_ms":       min(valid) if valid else None,
        "max_ms":       max(valid) if valid else None,
    }


# ---------------------------------------------------------------------------
# Neo4j timing helpers
# ---------------------------------------------------------------------------

def _neo4j_cold_run(driver, cypher: str, params: dict | None) -> tuple[float, int]:
    """
    Execute once and return (wall_clock_ms, row_count).
    Used for cold-cache run — wall-clock timed on the Python side.
    """
    with driver.session() as session:
        t0 = time.perf_counter()
        result = session.run(cypher, **(params or {}))
        rows = result.data()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return elapsed_ms, len(rows)


def _neo4j_timed_run(driver, cypher: str, params: dict | None) -> tuple[float, int]:
    """
    Execute and return (result_consumed_after_ms, row_count).
    result_consumed_after is reported by the Neo4j driver in ms — measured
    from when the query was dispatched to when all results were consumed.
    Includes server execution + network transfer to localhost.
    """
    with driver.session() as session:
        result = session.run(cypher, **(params or {}))
        rows = result.data()
        summary = result.consume()
        server_ms = float(summary.result_consumed_after)
    return server_ms, len(rows)


def run_neo4j_query(driver, query: dict, seeds: dict, n_runs: int) -> dict:
    """Run one query on Neo4j: 1 cold + n_runs warm driver-timed runs."""
    q_id   = query["id"]
    cypher = query["neo_cypher"]
    params = query["neo_params"](seeds)

    log.info("  [Neo4j] %s — cold run...", q_id)
    try:
        cold_ms, cold_rows = _neo4j_cold_run(driver, cypher, params)
        log.info("    cold: %.1f ms  (%d rows)", cold_ms, cold_rows)
    except Exception as exc:
        log.error("    cold run FAILED: %s", exc)
        return _error_result(q_id, query["name"], query["tier"], "neo4j", n_runs, str(exc))

    warm_times: list[float] = []
    for i in range(1, n_runs + 1):
        try:
            ms, rows = _neo4j_timed_run(driver, cypher, params)
            warm_times.append(ms)
            log.info("    warm %d/%d: %.1f ms", i, n_runs, ms)
        except Exception as exc:
            log.error("    warm run %d FAILED: %s", i, exc)
            warm_times.append(float("nan"))

    valid = [t for t in warm_times if not __import__("math").isnan(t)]
    return {
        "query_id":     q_id,
        "query_name":   query["name"],
        "tier":         query["tier"],
        "db":           "neo4j",
        "timing_method":"result_consumed_after (driver-reported ms, includes transfer)",
        "seeds_used":   params or {},
        "cold_ms":      cold_ms,
        "cold_rows":    cold_rows,
        "warm_runs":    n_runs,
        "warm_times_ms": warm_times,
        "median_ms":    median(valid) if valid else None,
        "mean_ms":      mean(valid) if valid else None,
        "stdev_ms":     stdev(valid) if len(valid) > 1 else 0.0,
        "min_ms":       min(valid) if valid else None,
        "max_ms":       max(valid) if valid else None,
    }


# ---------------------------------------------------------------------------
# Error placeholder
# ---------------------------------------------------------------------------

def _error_result(qid: str, name: str, tier: int, db: str,
                  n_runs: int, error: str) -> dict:
    return {
        "query_id": qid, "query_name": name, "tier": tier, "db": db,
        "error": error, "warm_runs": n_runs,
        "warm_times_ms": [], "median_ms": None, "mean_ms": None,
        "cold_ms": None, "cold_rows": 0,
    }


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def save_results(results: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    log.info("Results saved → %s", path)


def print_table(results: list[dict]) -> None:
    """Print a formatted comparison table (median warm times)."""
    by_query: dict[str, dict] = {}
    for r in results:
        by_query.setdefault(r["query_id"], {})[r["db"]] = r

    W = 100
    print("\n" + "=" * W)
    print(f"{'ID':<8} {'Tier':<6} {'Description':<42} "
          f"{'PG median':>10} {'Neo4j med':>10} {'Speedup':>9}")
    print("=" * W)

    for qid in sorted(by_query):
        pg  = by_query[qid].get("postgresql")
        neo = by_query[qid].get("neo4j")
        rec = pg or neo
        tier = rec.get("tier", "?")
        name = rec["query_name"][:41]
        pg_t  = f"{pg['median_ms']:.1f} ms"  if (pg  and pg.get("median_ms")  is not None) else "err/N/A"
        neo_t = f"{neo['median_ms']:.1f} ms" if (neo and neo.get("median_ms") is not None) else "err/N/A"
        if pg and neo and pg.get("median_ms") and neo.get("median_ms"):
            ratio = pg["median_ms"] / neo["median_ms"]
            winner = f"Neo4j {ratio:.2f}×" if ratio > 1 else f"PG    {1/ratio:.2f}×"
        else:
            winner = "—"
        print(f"{qid:<8} {tier:<6} {name:<42} {pg_t:>10} {neo_t:>10} {winner:>9}")

    print("=" * W)
    print("\nNote: PG times = server-side EXPLAIN ANALYZE 'Execution Time'.")
    print("      Neo4j times = driver result_consumed_after (server+network).")
    print("      Speedup > 1.0× = Neo4j is faster for that query.\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark runner: PostgreSQL vs Neo4j")
    parser.add_argument("--runs",       type=int,  default=DEFAULT_RUNS,   help=f"Warm runs per query (default: {DEFAULT_RUNS})")
    parser.add_argument("--output",     type=Path, default=DEFAULT_OUTPUT, help="JSON output path")
    parser.add_argument("--pg-only",    action="store_true", help="PostgreSQL only")
    parser.add_argument("--neo4j-only", action="store_true", help="Neo4j only")
    args = parser.parse_args()

    # Always read seeds from PostgreSQL (they're the same subreddit names in both DBs)
    conn = psycopg2.connect(**PG_CONFIG)
    seeds = pick_seeds(conn)

    all_results: list[dict] = []

    if not args.neo4j_only:
        log.info("=== PostgreSQL Benchmarks (%d queries × %d warm runs) ===", len(QUERIES), args.runs)
        conn.set_session(autocommit=True)   # prevents idle-in-transaction locking
        for query in QUERIES:
            log.info("Running %s: %s", query["id"], query["name"])
            result = run_pg_query(conn, query, seeds, args.runs)
            all_results.append(result)
        conn.close()

    if not args.pg_only:
        log.info("=== Neo4j Benchmarks (%d queries × %d warm runs) ===", len(QUERIES), args.runs)
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        for query in QUERIES:
            log.info("Running %s: %s", query["id"], query["name"])
            result = run_neo4j_query(driver, query, seeds, args.runs)
            all_results.append(result)
        driver.close()

    save_results(all_results, args.output)
    print_table(all_results)
    log.info("Done. Open notebooks/results_analysis.ipynb for charts.")


if __name__ == "__main__":
    main()
