#!/usr/bin/env python3
"""
run_benchmarks.py — PostgreSQL vs Neo4j multi-metric benchmark runner.

Adapted for the 3NF Normalized Relational Schema and Multi-Node Property Graph Schema
to comply with FAQ Q12 non-trivial modeling requirements.

Metrics captured per query per database:
  PostgreSQL (via EXPLAIN ANALYZE BUFFERS FORMAT JSON):
    - planning_ms       : query planner time (server-side)
    - execution_ms      : query executor time (server-side, excludes planning AND network)
    - buffer_hits       : pages served from shared_buffers (cache hit)
    - buffer_reads      : pages read from disk (cache miss)
    - buffer_hit_ratio  : hits / (hits + reads)  → cache effectiveness
    - temp_blocks       : blocks spilled to temp disk (memory pressure indicator)
    - actual_rows       : rows produced by the top plan node

  Neo4j (via Python driver result summary):
    - consumed_ms  : time from query submission until ALL rows are fetched and consumed.
                     = server_compute + Bolt_serialization + network_transfer.
                     For small result sets (<~100 rows), network term is negligible,
                     so consumed_ms ≈ server_compute.  PRIMARY metric for aggregations.
    - available_ms : time from query submission until the CURSOR becomes readable.
                     Due to Neo4j’s lazy evaluation, this is effectively the planning/RTT
                     time, NOT the full server computation. It is consistently ~1ms regardless
                     of query complexity and is therefore NOT a reliable server-side proxy.
                     SECONDARY metric, shown for transparency.
    - db_hits      : total record-store accesses (via one-off PROFILE run)

  Both:
    - cold_ms      : first-access wall-clock time (cold-ish cache)
    - result_count : number of rows returned
    - timing variability: stdev and coefficient of variation over warm runs

FAIR COMPARISON METHODOLOGY:
  For PG we use EXPLAIN ANALYZE execution_ms (server-only).
  For Neo4j we use consumed_ms as the primary metric, with the following note:
    • For queries returning ≤50 rows: consumed_ms ≈ server_compute (network overhead <1ms)
    • For queries returning thousands of rows (T1-B: 24k rows): consumed_ms includes
      significant Bolt transfer overhead (estimated ~53μs/row). The notebook decomposes
      this per query using the result_count field.
  available_ms is NOT used as primary because it reports ~1ms for ALL queries
  (including heavy aggregations like T4-A taking 1800ms) due to lazy cursor initialization.
"""

import argparse
import json
import logging
import math
import os
import time
import threading
import subprocess
from pathlib import Path
from statistics import median, mean, stdev
from typing import Any

import psycopg
import psutil
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

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "run_benchmarks.log", encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Background Memory Profiler (Docker Stats)
# ---------------------------------------------------------------------------

class MemoryProfiler:
    def __init__(self):
        self.running = False
        self.thread = None
        self.peak_pg_mb = 0.0
        self.peak_neo_mb = 0.0

    def start(self):
        self.running = True
        self.peak_pg_mb = 0.0
        self.peak_neo_mb = 0.0
        self.thread = threading.Thread(target=self._poll, daemon=True)
        self.thread.start()

    def stop(self) -> dict:
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
        return {"postgres_peak_mb": self.peak_pg_mb, "neo4j_peak_mb": self.peak_neo_mb}

    def _parse_mb(self, usage_str: str) -> float:
        usage = usage_str.split(" / ")[0].strip()
        val_str = "".join(c for c in usage if c.isdigit() or c == '.')
        if not val_str: return 0.0
        try:
            val = float(val_str)
            if "GiB" in usage or "GB" in usage: return val * 1024.0
            if "MiB" in usage or "MB" in usage: return val
            if "KiB" in usage or "KB" in usage: return val / 1024.0
            return val / (1024.0*1024.0)
        except ValueError:
            return 0.0

    def _poll(self):
        while self.running:
            try:
                res = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format", "{{.Name}},{{.MemUsage}}"],
                    capture_output=True, text=True, timeout=1.0
                )
                # Fix: split on actual newline character, not literal '\n'
                for line in res.stdout.strip().split('\n'):
                    if not line: continue
                    parts = line.split(',')
                    if len(parts) >= 2:
                        name, usage = parts[0], parts[1]
                        mb = self._parse_mb(usage)
                        if "postgres" in name:
                            self.peak_pg_mb = max(self.peak_pg_mb, mb)
                        elif "neo4j" in name:
                            self.peak_neo_mb = max(self.peak_neo_mb, mb)
            except Exception:
                pass
            time.sleep(0.5)

memory_profiler = MemoryProfiler()


# ---------------------------------------------------------------------------
# Query registry (Adapted for 3NF & Multi-Node Property Graph Schema)
# ---------------------------------------------------------------------------

QUERIES: list[dict[str, Any]] = [
    {
        "id": "T1-B", "name": "All outgoing links from seed", "tier": 1,
        "pg_sql": """
            SELECT s_tgt.name AS target_subreddit, p.post_id, p.timestamp, p.source_type, p.post_label
            FROM posts p
            JOIN subreddits s_src ON s_src.id = p.source_subreddit_id
            JOIN hyperlinks h     ON h.post_id = p.id
            JOIN subreddits s_tgt ON s_tgt.id = h.target_subreddit_id
            WHERE s_src.name = %(seed)s
            ORDER BY p.timestamp DESC
        """,
        "pg_params":  lambda s: {"seed": s["seed"]},
        "neo_cypher": """
            MATCH (src:Subreddit {name: $seed})-[:POSTED]->(p:Post)-[:REFERENCES]->(tgt:Subreddit)
            RETURN tgt.name AS target_subreddit, p.post_id AS post_id, p.timestamp AS timestamp,
                   p.source_type AS source_type, p.post_label AS post_label
            ORDER BY p.timestamp DESC
        """,
        "neo_params": lambda s: {"seed": s["seed"]},
    },
    {
        "id": "T1-C", "name": "Hostile links only from seed", "tier": 1,
        "pg_sql": """
            SELECT s_tgt.name AS target_subreddit, p.post_id, p.timestamp, p.source_type
            FROM posts p
            JOIN subreddits s_src ON s_src.id = p.source_subreddit_id
            JOIN hyperlinks h     ON h.post_id = p.id
            JOIN subreddits s_tgt ON s_tgt.id = h.target_subreddit_id
            WHERE s_src.name = %(seed)s AND p.post_label = -1
            ORDER BY p.timestamp DESC
        """,
        "pg_params":  lambda s: {"seed": s["seed"]},
        "neo_cypher": """
            MATCH (src:Subreddit {name: $seed})-[:POSTED]->(p:Post {post_label: -1})-[:REFERENCES]->(tgt:Subreddit)
            RETURN tgt.name AS target_subreddit, p.post_id AS post_id, p.timestamp AS timestamp, p.source_type AS source_type
            ORDER BY p.timestamp DESC
        """,
        "neo_params": lambda s: {"seed": s["seed"]},
    },
    {
        "id": "T2-A", "name": "Global in-degree ranking (top 20)", "tier": 2,
        "pg_sql": """
            SELECT s.name AS subreddit, COUNT(*) AS in_degree
            FROM hyperlinks h
            JOIN subreddits s ON s.id = h.target_subreddit_id
            GROUP BY s.name
            ORDER BY in_degree DESC LIMIT 20
        """,
        "pg_params":  lambda s: None,
        "neo_cypher": """
            MATCH ()-[:LINKED_TO]->(tgt:Subreddit)
            RETURN tgt.name AS subreddit, count(*) AS in_degree
            ORDER BY in_degree DESC LIMIT 20
        """,
        "neo_params": lambda s: None,
    },
    {
        "id": "T2-B", "name": "Top-20 subreddits by hostile link count", "tier": 2,
        "pg_sql": """
            SELECT s.name AS subreddit, COUNT(*) AS hostile_count
            FROM posts p
            JOIN subreddits s ON s.id = p.source_subreddit_id
            WHERE p.post_label = -1
            GROUP BY s.name
            ORDER BY hostile_count DESC LIMIT 20
        """,
        "pg_params":  lambda s: None,
        "neo_cypher": """
            MATCH (src:Subreddit)-[:POSTED]->(p:Post {post_label: -1})
            RETURN src.name AS subreddit, count(p) AS hostile_count
            ORDER BY hostile_count DESC LIMIT 20
        """,
        "neo_params": lambda s: None,
    },
    {
        "id": "T3-A", "name": "2-hop common targets of seed_a and seed_b", "tier": 3,
        "pg_sql": """
            WITH targets_a AS (
                SELECT DISTINCT h.target_subreddit_id
                FROM posts p
                JOIN subreddits s ON s.id = p.source_subreddit_id
                JOIN hyperlinks h ON h.post_id = p.id
                WHERE s.name = %(seed_a)s
            ),
            targets_b AS (
                SELECT DISTINCT h.target_subreddit_id
                FROM posts p
                JOIN subreddits s ON s.id = p.source_subreddit_id
                JOIN hyperlinks h ON h.post_id = p.id
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
            MATCH (a:Subreddit {name: $seed_a})-[:LINKED_TO]->(shared:Subreddit)
            WITH shared
            MATCH (b:Subreddit {name: $seed_b})-[:LINKED_TO]->(shared)
            RETURN shared.name AS shared_target ORDER BY shared_target LIMIT 25
        """,
        "neo_params": lambda s: {"seed_a": s["seed_a"], "seed_b": s["seed_b"]},
    },
    {
        "id": "T3-C", "name": "Common hostile attackers of seed_a and seed_b", "tier": 3,
        "pg_sql": """
            WITH attackers_a AS (
                SELECT DISTINCT p.source_subreddit_id
                FROM posts p
                JOIN hyperlinks h ON h.post_id = p.id
                JOIN subreddits s ON s.id = h.target_subreddit_id
                WHERE s.name = %(seed_a)s AND p.post_label = -1
            ),
            attackers_b AS (
                SELECT DISTINCT p.source_subreddit_id
                FROM posts p
                JOIN hyperlinks h ON h.post_id = p.id
                JOIN subreddits s ON s.id = h.target_subreddit_id
                WHERE s.name = %(seed_b)s AND p.post_label = -1
            )
            SELECT s.name AS common_attacker
            FROM attackers_a a
            JOIN attackers_b b USING (source_subreddit_id)
            JOIN subreddits s ON s.id = a.source_subreddit_id
            ORDER BY s.name LIMIT 25
        """,
        "pg_params":  lambda s: {"seed_a": s["seed_a"], "seed_b": s["seed_b"]},
        "neo_cypher": """
            MATCH (atk:Subreddit)-[r1:LINKED_TO]->(a:Subreddit {name: $seed_a})
            WHERE r1.post_label = -1
            WITH atk
            MATCH (atk)-[r2:LINKED_TO]->(b:Subreddit {name: $seed_b})
            WHERE r2.post_label = -1
            RETURN atk.name AS common_attacker ORDER BY atk.name LIMIT 25
        """,
        "neo_params": lambda s: {"seed_a": s["seed_a"], "seed_b": s["seed_b"]},
    },
    {
        "id": "T4-A", "name": "Global mutual hostile pairs", "tier": 4,
        "pg_sql": """
            SELECT s_a.name AS sub_a, s_b.name AS sub_b, COUNT(*) AS mutual_hostile_links
            FROM posts p_ab
            JOIN hyperlinks h_ab ON h_ab.post_id = p_ab.id
            JOIN posts p_ba      ON p_ba.source_subreddit_id = h_ab.target_subreddit_id
            JOIN hyperlinks h_ba ON h_ba.post_id = p_ba.id AND h_ba.target_subreddit_id = p_ab.source_subreddit_id
            JOIN subreddits s_a  ON s_a.id = p_ab.source_subreddit_id
            JOIN subreddits s_b  ON s_b.id = h_ab.target_subreddit_id
            WHERE p_ab.post_label = -1 AND p_ba.post_label = -1
              AND p_ab.source_subreddit_id < h_ab.target_subreddit_id
            GROUP BY s_a.name, s_b.name ORDER BY mutual_hostile_links DESC LIMIT 20
        """,
        "pg_params":  lambda s: None,
        "neo_cypher": """
            MATCH (a:Subreddit)-[r1:LINKED_TO]->(b:Subreddit)-[r2:LINKED_TO]->(a)
            WHERE r1.post_label = -1 AND r2.post_label = -1
              AND elementId(a) < elementId(b)
            RETURN a.name AS sub_a, b.name AS sub_b, count(*) AS mutual_hostile_links
            ORDER BY mutual_hostile_links DESC LIMIT 20
        """,
        "neo_params": lambda s: None,
    },
    {
        "id": "T4-B", "name": "Bounded BFS depth 2 from seed_bfs", "tier": 4,
        "pg_sql": """
            WITH RECURSIVE bfs(node_id, depth, path) AS (
                SELECT s.id, 0, ARRAY[s.id] FROM subreddits s WHERE s.name = %(seed_bfs)s
                UNION ALL
                SELECT h.target_subreddit_id, b.depth + 1, b.path || h.target_subreddit_id
                FROM bfs b
                JOIN posts p      ON p.source_subreddit_id = b.node_id
                JOIN hyperlinks h ON h.post_id = p.id
                WHERE b.depth < 2 AND NOT (h.target_subreddit_id = ANY(b.path))
            )
            SELECT s.name AS reachable, MIN(bfs.depth) AS min_hops
            FROM bfs JOIN subreddits s ON s.id = bfs.node_id
            WHERE bfs.depth > 0
            GROUP BY s.name ORDER BY min_hops, s.name LIMIT 500
        """,
        "pg_params":  lambda s: {"seed_bfs": s["seed_bfs"]},
        "neo_cypher": """
            MATCH p = (src:Subreddit {name: $seed_bfs})-[:LINKED_TO*1..2]->(tgt:Subreddit)
            WHERE src <> tgt
            WITH tgt, min(length(p)) AS min_hops
            RETURN tgt.name AS reachable, min_hops ORDER BY min_hops, tgt.name LIMIT 500
        """,
        "neo_params": lambda s: {"seed_bfs": s["seed_bfs"]},
    },
    {
        "id": "T5-A", "name": "3-node hostile cycles seeded", "tier": 5,
        "pg_sql": """
            SELECT s_a.name AS node_a, s_b.name AS node_b, s_c.name AS node_c
            FROM posts p_ab
            JOIN hyperlinks h_ab ON h_ab.post_id = p_ab.id
            JOIN posts p_bc      ON p_bc.source_subreddit_id = h_ab.target_subreddit_id AND p_bc.post_label = -1
            JOIN hyperlinks h_bc ON h_bc.post_id = p_bc.id
            JOIN posts p_ca      ON p_ca.source_subreddit_id = h_bc.target_subreddit_id AND p_ca.post_label = -1
            JOIN hyperlinks h_ca ON h_ca.post_id = p_ca.id AND h_ca.target_subreddit_id = p_ab.source_subreddit_id
            JOIN subreddits s_a  ON s_a.id = p_ab.source_subreddit_id
            JOIN subreddits s_b  ON s_b.id = h_ab.target_subreddit_id
            JOIN subreddits s_c  ON s_c.id = h_bc.target_subreddit_id
            WHERE p_ab.post_label = -1 AND s_a.name = %(seed)s
              AND h_ab.target_subreddit_id < h_bc.target_subreddit_id
            LIMIT 50
        """,
        "pg_params":  lambda s: {"seed": s["seed"]},
        "neo_cypher": """
            MATCH (a:Subreddit {name: $seed})-[r1:LINKED_TO]->(b:Subreddit)-[r2:LINKED_TO]->(c:Subreddit)-[r3:LINKED_TO]->(a)
            WHERE r1.post_label = -1 AND r2.post_label = -1 AND r3.post_label = -1
              AND elementId(b) < elementId(c)
            RETURN a.name AS node_a, b.name AS node_b, c.name AS node_c LIMIT 50
        """,
        "neo_params": lambda s: {"seed": s["seed"]},
    },
    {
        "id": "T5-B", "name": "Bounded BFS depth 3 from seed_bfs", "tier": 5,
        "pg_sql": """
            WITH RECURSIVE bfs(node_id, depth, path) AS (
                SELECT s.id, 0, ARRAY[s.id] FROM subreddits s WHERE s.name = %(seed_bfs)s
                UNION ALL
                SELECT h.target_subreddit_id, b.depth + 1, b.path || h.target_subreddit_id
                FROM bfs b
                JOIN posts p      ON p.source_subreddit_id = b.node_id
                JOIN hyperlinks h ON h.post_id = p.id
                WHERE b.depth < 3 AND NOT (h.target_subreddit_id = ANY(b.path))
            )
            SELECT s.name AS reachable, MIN(bfs.depth) AS min_hops
            FROM bfs JOIN subreddits s ON s.id = bfs.node_id
            WHERE bfs.depth > 0
            GROUP BY s.name ORDER BY min_hops, s.name LIMIT 500
        """,
        "pg_params":  lambda s: {"seed_bfs": s["seed_bfs"]},
        "neo_cypher": """
            MATCH p = (src:Subreddit {name: $seed_bfs})-[:LINKED_TO*1..3]->(tgt:Subreddit)
            WHERE src <> tgt
            WITH tgt, min(length(p)) AS min_hops
            RETURN tgt.name AS reachable, min_hops ORDER BY min_hops, tgt.name LIMIT 500
        """,
        "neo_params": lambda s: {"seed_bfs": s["seed_bfs"]},
    },
]


# ---------------------------------------------------------------------------
# Seed selection
# ---------------------------------------------------------------------------

def pick_seeds(conn) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.name, COUNT(*) AS c
            FROM posts p
            JOIN subreddits s ON s.id = p.source_subreddit_id
            JOIN hyperlinks h ON h.post_id = p.id
            GROUP BY s.name ORDER BY c DESC LIMIT 210
        """)
        by_outdeg = cur.fetchall()
        cur.execute("""
            SELECT s.name FROM posts p
            JOIN subreddits s ON s.id = p.source_subreddit_id
            WHERE p.post_label = -1
            GROUP BY s.name ORDER BY COUNT(*) DESC LIMIT 1
        """)
        top_hostile = cur.fetchone()[0]

    seeds = {
        "seed":     top_hostile,
        "seed_a":   by_outdeg[0][0],
        "seed_b":   by_outdeg[1][0],
        "seed_bfs": by_outdeg[199][0],
    }
    log.info("Seeds:  seed=%s  seed_a=%s (deg %d)  seed_b=%s (deg %d)  seed_bfs=%s (deg %d)",
             seeds["seed"],
             seeds["seed_a"], by_outdeg[0][1],
             seeds["seed_b"], by_outdeg[1][1],
             seeds["seed_bfs"], by_outdeg[199][1])
    return seeds


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def _sum_buffer_nodes(node: dict) -> tuple[int, int, int]:
    """Recursively sum (shared_hits, shared_reads, temp_blocks) across plan tree."""
    hit  = node.get("Shared Hit Blocks",     0)
    read = node.get("Shared Read Blocks",    0)
    tmp  = node.get("Temp Written Blocks",   0) + node.get("Temp Read Blocks", 0)
    for child in node.get("Plans", []):
        ch, cr, ct = _sum_buffer_nodes(child)
        hit += ch; read += cr; tmp += ct
    return hit, read, tmp


def _pg_cold_run(conn, sql: str, params) -> tuple[float, int]:
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute(sql, params)
        rows = cur.fetchall()
    return (time.perf_counter() - t0) * 1000.0, len(rows)


def _pg_timed_run(conn, sql: str, params) -> dict:
    """EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) — returns full metric dict."""
    with conn.cursor() as cur:
        cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, params)
        raw = cur.fetchone()[0]
        plan = raw if isinstance(raw, list) else json.loads(raw)
    top  = plan[0]
    hits, reads, tmp = _sum_buffer_nodes(top["Plan"])
    return {
        "execution_ms":  top["Execution Time"],
        "planning_ms":   top["Planning Time"],
        "buffer_hits":   hits,
        "buffer_reads":  reads,
        "temp_blocks":   tmp,
        "actual_rows":   top["Plan"].get("Actual Rows", -1),
    }


def run_pg_query(conn, query: dict, seeds: dict, n_runs: int) -> dict:
    sql    = query["pg_sql"].strip()
    params = query["pg_params"](seeds)
    q_id   = query["id"]

    log.info("  [PG] %s cold run...", q_id)
    try:
        cold_ms, cold_rows = _pg_cold_run(conn, sql, params)
    except Exception as exc:
        log.error("  [PG] %s cold FAILED: %s", q_id, exc); conn.rollback()
        return _err(query, "postgresql", n_runs, str(exc))
    log.info("    cold=%.1f ms  rows=%d", cold_ms, cold_rows)

    exec_ms_list, plan_ms_list, hits_list, reads_list, tmp_list = [], [], [], [], []
    result_count = cold_rows

    psutil.cpu_percent(interval=None)  # Initialize CPU utilization tracker
    memory_profiler.start()
    for i in range(1, n_runs + 1):
        try:
            m = _pg_timed_run(conn, sql, params)
            exec_ms_list.append(m["execution_ms"])
            plan_ms_list.append(m["planning_ms"])
            hits_list.append(m["buffer_hits"])
            reads_list.append(m["buffer_reads"])
            tmp_list.append(m["temp_blocks"])
            if i == 1 and m["actual_rows"] >= 0:
                result_count = m["actual_rows"]
            log.info("    warm %d/%d: exec=%.1f ms  plan=%.2f ms  hits=%d  reads=%d",
                     i, n_runs, m["execution_ms"], m["planning_ms"],
                     m["buffer_hits"], m["buffer_reads"])
        except Exception as exc:
            log.error("    warm %d FAILED: %s", i, exc); conn.rollback()
            exec_ms_list.append(float("nan"))

    mem_peaks = memory_profiler.stop()

    valid = [t for t in exec_ms_list if not math.isnan(t)]
    total_hits  = sum(hits_list)
    total_reads = sum(reads_list)
    hit_ratio   = total_hits / (total_hits + total_reads) if (total_hits + total_reads) > 0 else 1.0
    med_exec    = median(valid) if valid else None
    cv          = (stdev(valid) / mean(valid) * 100) if len(valid) > 1 and mean(valid) > 0 else 0.0

    client_cpu = psutil.cpu_percent(interval=None)
    mem_stat = psutil.virtual_memory()

    return {
        "query_id":            q_id,
        "query_name":          query["name"],
        "tier":                query["tier"],
        "db":                  "postgresql",
        "timing_method":       "EXPLAIN ANALYZE Execution Time (server-side ms, excludes network)",
        "seeds_used":          params or {},
        # Cold cache
        "cold_ms":             cold_ms,
        "cold_rows":           cold_rows,
        # Result info
        "result_count":        result_count,
        # Server-side memory profiling
        "server_peak_ram_mb":  round(mem_peaks["postgres_peak_mb"], 2),
        # Client-side system resource telemetry (via psutil)
        "client_cpu_pct":      round(client_cpu, 2),
        "client_ram_pct":      round(mem_stat.percent, 2),
        "client_ram_used_mb":  round(mem_stat.used / (1024 * 1024), 2),
        # Warm timing (per-run lists for box plots)
        "warm_runs":           n_runs,
        "warm_execution_ms":   exec_ms_list,
        "warm_planning_ms":    plan_ms_list,
        # Warm timing aggregates
        "median_execution_ms": med_exec,
        "mean_execution_ms":   mean(valid) if valid else None,
        "stdev_execution_ms":  stdev(valid) if len(valid) > 1 else 0.0,
        "min_execution_ms":    min(valid) if valid else None,
        "max_execution_ms":    max(valid) if valid else None,
        "cv_pct":              cv,
        "cold_warm_delta_ms":  cold_ms - med_exec if med_exec else None,
        # Planning
        "median_planning_ms":  median(plan_ms_list) if plan_ms_list else None,
        "warm_buffer_hits":    hits_list,
        "warm_buffer_reads":   reads_list,
        "warm_temp_blocks":    tmp_list,
        # Buffer aggregates
        "total_buffer_hits":   total_hits,
        "total_buffer_reads":  total_reads,
        "buffer_hit_ratio":    round(hit_ratio, 4),
        "avg_temp_blocks":     mean(tmp_list) if tmp_list else 0,
    }


# ---------------------------------------------------------------------------
# Neo4j helpers
# ---------------------------------------------------------------------------

def _neo4j_cold_run(driver, cypher: str, params) -> tuple[float, int]:
    with driver.session() as session:
        t0 = time.perf_counter()
        result = session.run(cypher, **(params or {}))
        rows = result.data()
    return (time.perf_counter() - t0) * 1000.0, len(rows)


def _neo4j_timed_run(driver, cypher: str, params) -> dict:
    with driver.session() as session:
        result = session.run(cypher, **(params or {}))
        rows   = result.data()
        summ   = result.consume()
    return {
        "consumed_ms":  float(summ.result_consumed_after),
        "available_ms": float(summ.result_available_after),
        "row_count":    len(rows),
    }


def _neo4j_profile_once(driver, cypher: str, params) -> int:
    """One-off PROFILE run to collect db_hits. NOT used for timing.

    In Neo4j driver 5.x, profile plan nodes are plain dicts with camelCase keys
    (e.g. 'dbHits', 'children'). Older versions exposed them as object attributes.
    We handle both cases for robustness.
    """
    def _sum_hits(node) -> int:
        # Neo4j 5.x driver returns dicts; fallback to attribute access for older drivers
        if isinstance(node, dict):
            total = node.get("dbHits", 0) or 0
            children = node.get("children", [])
        else:
            total = getattr(node, "db_hits", 0) or 0
            children = getattr(node, "children", [])
        for child in children:
            total += _sum_hits(child)
        return total
    try:
        with driver.session() as session:
            result = session.run("PROFILE " + cypher, **(params or {}))
            result.data()
            summ = result.consume()
            if summ.profile is None:
                log.warning("PROFILE returned no plan (PROFILE clause may not be supported).")
                return -1
            hits = _sum_hits(summ.profile)
            log.debug("PROFILE db_hits raw sum: %d", hits)
            return hits
    except Exception as exc:
        log.warning("PROFILE run failed (non-fatal): %s", exc)
        return -1


def run_neo4j_query(driver, query: dict, seeds: dict, n_runs: int) -> dict:
    cypher = query["neo_cypher"].strip()
    params = query["neo_params"](seeds)
    q_id   = query["id"]

    log.info("  [Neo4j] %s cold run...", q_id)
    try:
        cold_ms, cold_rows = _neo4j_cold_run(driver, cypher, params)
    except Exception as exc:
        log.error("  [Neo4j] %s cold FAILED: %s", q_id, exc)
        return _err(query, "neo4j", n_runs, str(exc))
    log.info("    cold=%.1f ms  rows=%d", cold_ms, cold_rows)

    log.info("    profiling for db_hits (not timed)...")
    db_hits = _neo4j_profile_once(driver, cypher, params)
    log.info("    db_hits=%d", db_hits)

    consumed_list, available_list = [], []
    result_count = cold_rows

    psutil.cpu_percent(interval=None)  # Initialize CPU utilization tracker
    memory_profiler.start()
    for i in range(1, n_runs + 1):
        try:
            m = _neo4j_timed_run(driver, cypher, params)
            consumed_list.append(m["consumed_ms"])
            available_list.append(m["available_ms"])
            if i == 1:
                result_count = m["row_count"]
            log.info("    warm %d/%d: consumed=%.1f ms  available=%.1f ms",
                     i, n_runs, m["consumed_ms"], m["available_ms"])
        except Exception as exc:
            log.error("    warm %d FAILED: %s", i, exc)
            consumed_list.append(float("nan"))
            available_list.append(float("nan"))

    mem_peaks = memory_profiler.stop()

    valid_c = [t for t in consumed_list  if not math.isnan(t)]
    valid_a = [t for t in available_list if not math.isnan(t)]
    med_c   = median(valid_c) if valid_c else None
    cv      = (stdev(valid_c) / mean(valid_c) * 100) if len(valid_c) > 1 and mean(valid_c) > 0 else 0.0

    client_cpu = psutil.cpu_percent(interval=None)
    mem_stat = psutil.virtual_memory()

    # PRIMARY comparison metric: consumed_ms (total query time incl. fetch).
    # For small result sets (<50 rows), network overhead is negligible: consumed ≈ server_compute.
    # For large result sets (T1-B: 24k rows), consumed includes Bolt transfer overhead;
    # the notebook uses result_count to estimate and decompose the transfer component.
    #
    # NOTE: available_ms is NOT used as primary because it returns ~1ms for ALL queries
    # (including heavy aggregations like T4-A 1800ms) due to Neo4j lazy cursor init.
    # available_ms only represents planning+RTT time, not server computation.
    med_c   = median(valid_c) if valid_c else None
    cv      = (stdev(valid_c) / mean(valid_c) * 100) if len(valid_c) > 1 and mean(valid_c) > 0 else 0.0
    med_a   = median(valid_a) if valid_a else None

    return {
        "query_id":             q_id,
        "query_name":           query["name"],
        "tier":                 query["tier"],
        "db":                   "neo4j",
        # METHODOLOGY NOTE:
        # PRIMARY timing = consumed_ms (server+fetch). For small result sets it approximates
        # server-side compute. For large result sets, notebook decomposes transfer overhead.
        # available_ms is unreliable as server proxy (lazy cursor init returns ~1ms always).
        "timing_method":        "result_consumed_after (server+Bolt fetch ms). NOTE: available_after is unreliable as server proxy due to lazy evaluation - see docstring.",
        "seeds_used":           params or {},
        # Cold cache
        "cold_ms":              cold_ms,
        "cold_rows":            cold_rows,
        # Result info
        "result_count":         result_count,
        "db_hits":              db_hits,
        # Server-side memory profiling
        "server_peak_ram_mb":   round(mem_peaks["neo4j_peak_mb"], 2),
        # Client-side system resource telemetry (via psutil)
        "client_cpu_pct":       round(client_cpu, 2),
        "client_ram_pct":       round(mem_stat.percent, 2),
        "client_ram_used_mb":   round(mem_stat.used / (1024 * 1024), 2),
        # Warm timing per-run lists
        "warm_runs":            n_runs,
        "warm_consumed_ms":     consumed_list,
        "warm_available_ms":    available_list,
        # PRIMARY: consumed_ms (total query time)
        # warm_execution_ms = consumed_list so that boxplots (Chart 3) are self-consistent
        "warm_execution_ms":    consumed_list,
        "median_execution_ms":  med_c,
        "median_consumed_ms":   med_c,
        "mean_execution_ms":    mean(valid_c) if valid_c else None,
        "stdev_execution_ms":   stdev(valid_c) if len(valid_c) > 1 else 0.0,
        "min_execution_ms":     min(valid_c) if valid_c else None,
        "max_execution_ms":     max(valid_c) if valid_c else None,
        "cv_pct":               cv,
        "cold_warm_delta_ms":   cold_ms - med_c if med_c else None,
        # SECONDARY: available_ms shown for transparency (NOT server compute for aggregations)
        "median_available_ms":  med_a,
        "stdev_consumed_ms":    stdev(valid_c) if len(valid_c) > 1 else 0.0,
        "cv_consumed_pct":      cv,
        # Bolt transfer overhead (unreliable for aggregations: available_ms is ~1ms always)
        "transfer_overhead_ms": (med_c - med_a)
                                if (med_c is not None and med_a is not None) else None,
    }


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _err(query: dict, db: str, n_runs: int, msg: str) -> dict:
    return {
        "query_id": query["id"], "query_name": query["name"],
        "tier": query["tier"], "db": db, "error": msg,
        "warm_runs": n_runs, "warm_execution_ms": [],
        "median_execution_ms": None, "cold_ms": None, "cold_rows": 0,
    }


def save_results(results: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    log.info("Results saved → %s", path)


def print_table(results: list[dict]) -> None:
    by_q: dict[str, dict] = {}
    for r in results:
        by_q.setdefault(r["query_id"], {})[r["db"]] = r

    W = 124
    print("\n" + "=" * W)
    hdr = (f"{'ID':<8} {'Tier':<5} {'Name':<38} "
           f"{'PG exec':<10} {'PG plan':<9} {'PG buf%':<9} "
           f"{'Neo4j e2e':<11} {'Neo4j avail':<12} {'Notes':<15}")
    print(hdr)
    print("=" * W)

    for qid in sorted(by_q):
        pg  = by_q[qid].get("postgresql", {})
        neo = by_q[qid].get("neo4j", {})
        rec = pg or neo
        t   = f"T{rec.get('tier','?')}"
        name = rec.get("query_name","")[:37]

        pg_exec = f"{pg['median_execution_ms']:.1f}" if pg.get("median_execution_ms") else "err"
        pg_plan = f"{pg['median_planning_ms']:.2f}"  if pg.get("median_planning_ms")  else "—"
        pg_buf  = f"{pg['buffer_hit_ratio']*100:.1f}%" if pg.get("buffer_hit_ratio")  else "—"

        # PRIMARY: consumed_ms (total time including fetch)
        neo_e2e = f"{neo['median_consumed_ms']:.1f}" if neo.get("median_consumed_ms") else "err"
        neo_avl = f"{neo['median_available_ms']:.1f}" if neo.get("median_available_ms") else "—"
        rows    = neo.get("result_count", 0) or pg.get("result_count", 0)

        # Speedup note: for small results consumed ≈ server_compute
        if pg.get("median_execution_ms") and neo.get("median_consumed_ms"):
            ratio_e2e = pg["median_execution_ms"] / neo["median_consumed_ms"]
            if rows <= 100:
                note = f"Neo {ratio_e2e:.1f}x (svr≈)"
            else:
                note = f"Neo {ratio_e2e:.1f}x (incl.tx)"
        else:
            note = "—"

        print(f"{qid:<8} {t:<5} {name:<38} "
              f"{pg_exec:<10} {pg_plan:<9} {pg_buf:<9} "
              f"{neo_e2e:<11} {neo_avl:<12} {note:<15}")

    print("=" * W)
    print("\nPG exec      = EXPLAIN ANALYZE Execution Time (server-only, excludes network)")
    print("Neo4j e2e    = result_consumed_after (server+Bolt fetch) ← PRIMARY")
    print("               For rows<=100: e2e ≈ server_compute (network overhead <1ms)")
    print("               For rows>>100: e2e includes Bolt transfer (T1-B: ~53μs/row)")
    print("Neo4j avail  = result_available_after ← UNRELIABLE: ~1ms for ALL queries (lazy cursor init)\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-metric benchmark: PostgreSQL vs Neo4j")
    parser.add_argument("--runs",        type=int,  default=DEFAULT_RUNS)
    parser.add_argument("--output",      type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--pg-only",     action="store_true")
    parser.add_argument("--neo4j-only",  action="store_true")
    args = parser.parse_args()

    conn   = psycopg.connect(**PG_CONFIG)
    seeds  = pick_seeds(conn)
    conn.close()

    all_results: list[dict] = []

    if not args.neo4j_only:
        log.info("=== PostgreSQL: %d queries × %d warm runs ===", len(QUERIES), args.runs)
        conn = psycopg.connect(**PG_CONFIG, autocommit=True)
        for q in QUERIES:
            log.info("Running %s: %s", q["id"], q["name"])
            all_results.append(run_pg_query(conn, q, seeds, args.runs))
        conn.close()

    if not args.pg_only:
        log.info("=== Neo4j: %d queries × %d warm runs + 1 PROFILE ===", len(QUERIES), args.runs)
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        for q in QUERIES:
            log.info("Running %s: %s", q["id"], q["name"])
            all_results.append(run_neo4j_query(driver, q, seeds, args.runs))
        driver.close()

    save_results(all_results, args.output)
    print_table(all_results)
    log.info("Open notebooks/results_analysis.ipynb to generate charts.")


if __name__ == "__main__":
    main()
