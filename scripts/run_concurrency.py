#!/usr/bin/env python3
"""
run_concurrency.py — Measures Queries Per Second (QPS) and latency under load.
Simulates concurrent clients hitting both PostgreSQL and Neo4j simultaneously.
"""

import json
import time
import statistics
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg
from neo4j import GraphDatabase

from run_benchmarks import QUERIES, pick_seeds, PG_CONFIG, NEO4J_URI, NEO4J_USER, NEO4J_PASS

DATA_DIR = Path(__file__).parent.parent / "data"

# Workload: A mix of Lookup, Aggregation, and Pathfinding
WORKLOAD_IDS = ["T1-B", "T2-A", "T4-B"]
CONCURRENCY_LEVELS = [1, 10, 50]
TOTAL_REQUESTS = 150  # Requests to execute per concurrency level

def run_pg_worker(query: dict, seeds: dict) -> float:
    t0 = time.perf_counter()
    try:
        with psycopg.connect(**PG_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(query["pg_sql"], query["pg_params"](seeds))
                cur.fetchall()
    except Exception as e:
        print(f"PG worker error: {e}")
    return (time.perf_counter() - t0) * 1000.0

def run_neo4j_worker(query: dict, seeds: dict) -> float:
    t0 = time.perf_counter()
    try:
        # We must create a new driver instance or use a shared one.
        # Shared driver is better for connection pooling, simulating a real app.
        pass # implemented below
    except Exception as e:
        print(f"Neo4j worker error: {e}")
    return (time.perf_counter() - t0) * 1000.0

def execute_neo4j_with_driver(driver, query: dict, seeds: dict) -> float:
    t0 = time.perf_counter()
    try:
        with driver.session() as session:
            session.run(query["neo_cypher"], **(query["neo_params"](seeds) or {})).consume()
    except Exception as e:
        print(f"Neo4j driver error: {e}")
    return (time.perf_counter() - t0) * 1000.0

def main():
    print("=== Concurrency & Throughput Benchmark ===")
    
    # Setup
    with psycopg.connect(**PG_CONFIG) as conn:
        seeds = pick_seeds(conn)
    
    workload = [q for q in QUERIES if q["id"] in WORKLOAD_IDS]
    
    results = []

    # Initialize Neo4j Driver (Thread-safe connection pool)
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS), max_connection_pool_size=100)

    for db_name in ["PostgreSQL", "Neo4j"]:
        print(f"\n[*] Testing {db_name}...")
        
        for c in CONCURRENCY_LEVELS:
            print(f"    Concurrency: {c} workers, {TOTAL_REQUESTS} total requests")
            
            latencies = []
            start_time = time.perf_counter()
            
            with ThreadPoolExecutor(max_workers=c) as executor:
                futures = []
                for i in range(TOTAL_REQUESTS):
                    # Rotate queries in workload
                    q = workload[i % len(workload)]
                    
                    if db_name == "PostgreSQL":
                        futures.append(executor.submit(run_pg_worker, q, seeds))
                    else:
                        futures.append(executor.submit(execute_neo4j_with_driver, driver, q, seeds))
                        
                for f in as_completed(futures):
                    latencies.append(f.result())
            
            total_time = time.perf_counter() - start_time
            qps = TOTAL_REQUESTS / total_time
            p95 = statistics.quantiles(latencies, n=100)[94] if len(latencies) >= 100 else max(latencies)
            
            print(f"      -> QPS: {qps:.2f}, p95 Latency: {p95:.1f} ms")
            
            results.append({
                "db": db_name.lower(),
                "concurrency": c,
                "qps": qps,
                "p95_latency_ms": p95,
                "median_latency_ms": statistics.median(latencies)
            })
            
            time.sleep(2) # Cooldown
            
    driver.close()

    out_file = DATA_DIR / "concurrency_results.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4)
    print(f"\n[+] Concurrency results saved to {out_file.name}")

if __name__ == "__main__":
    main()
