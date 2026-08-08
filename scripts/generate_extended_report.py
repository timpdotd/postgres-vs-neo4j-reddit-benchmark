#!/usr/bin/env python3
"""
generate_extended_report.py — Compiles all JSON metrics into a Markdown report.
Reads ETL, Storage, Memory, Concurrency, Scalability and standard Benchmark data.
"""

import json
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
REPORT_PATH = Path(__file__).parent.parent / "extended_analysis.md"

def load_json_safe(filename):
    p = DATA_DIR / filename
    if p.exists():
        with open(p, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def main():
    print("[*] Generating Extended Analysis Report...")
    
    etl = load_json_safe("etl_metrics.json")
    storage = load_json_safe("storage_metrics.json")
    bench = load_json_safe("benchmark_results.json")
    conc = load_json_safe("concurrency_results.json")
    scale = load_json_safe("scalability_results.json")
    
    with open(REPORT_PATH, 'w', encoding='utf-8') as md:
        md.write("# PostgreSQL vs Neo4j: Advanced Metrics Report\n\n")
        
        # 1. ETL & Storage
        md.write("## 1. Storage & Ingestion (ETL)\n\n")
        md.write("| Metric | PostgreSQL | Neo4j | Winner |\n")
        md.write("|--------|------------|-------|--------|\n")
        
        if etl:
            p_time = etl.get("postgres_load_seconds", 0)
            n_time = etl.get("neo4j_load_seconds", 0)
            win = "PostgreSQL" if p_time < n_time else "Neo4j"
            md.write(f"| Ingestion Time | {p_time:.2f} s | {n_time:.2f} s | **{win}** |\n")
            
        if storage:
            p_mb = storage.get("postgres_bytes", 0) / (1024*1024)
            n_mb = storage.get("neo4j_bytes", 0) / (1024*1024)
            win = "PostgreSQL" if p_mb < n_mb else "Neo4j"
            md.write(f"| Storage Size | {p_mb:.2f} MB | {n_mb:.2f} MB | **{win}** |\n")
            
        md.write("\n*PostgreSQL often wins in storage density due to 3NF normalization, while Neo4j trades disk space for index-free adjacency pointers.* \n\n")
        
        # 2. Concurrency (QPS)
        if conc:
            md.write("## 2. Concurrency & Throughput (QPS)\n\n")
            md.write("| Concurrency Level | Postgres QPS | Neo4j QPS | Postgres p95 (ms) | Neo4j p95 (ms) |\n")
            md.write("|-------------------|--------------|-----------|-------------------|----------------|\n")
            
            # Group by concurrency
            conc_levels = sorted(list(set(c["concurrency"] for c in conc)))
            for c_level in conc_levels:
                p_qps, n_qps, p_p95, n_p95 = 0, 0, 0, 0
                for c in conc:
                    if c["concurrency"] == c_level:
                        if c["db"] == "postgresql":
                            p_qps, p_p95 = c["qps"], c["p95_latency_ms"]
                        else:
                            n_qps, n_p95 = c["qps"], c["p95_latency_ms"]
                md.write(f"| {c_level} workers | {p_qps:.1f} | {n_qps:.1f} | {p_p95:.1f} | {n_p95:.1f} |\n")
            md.write("\n")
            
        # 3. Scalability O(1) vs O(N)
        if scale:
            md.write("## 3. Scalability (T5-B Pathfinding)\n\n")
            md.write("| Dataset Size | PostgreSQL Time (ms) | Neo4j Time (ms) |\n")
            md.write("|--------------|----------------------|-----------------|\n")
            
            pcts = sorted(list(set(s["dataset_pct"] for s in scale)))
            for pct in pcts:
                p_ms, n_ms = "-", "-"
                for s in scale:
                    if s["dataset_pct"] == pct:
                        if s["db"] == "postgresql": p_ms = f"{s['t5b_execution_ms']:.1f}"
                        if s["db"] == "neo4j": n_ms = f"{s['t5b_execution_ms']:.1f}"
                md.write(f"| {pct}% | {p_ms} | {n_ms} |\n")
            md.write("\n*Notice how Neo4j execution time remains flat (O(1) relative to total DB size), whereas PostgreSQL query time degrades as B-Tree indices and Join hash tables grow (O(N) or O(log N)).*\n\n")
            
        # 4. Memory Profiling (RSS)
        if bench:
            md.write("## 4. Peak Memory Consumption (RSS)\n\n")
            md.write("| Query Tier | Postgres Peak RAM | Neo4j Peak RAM |\n")
            md.write("|------------|-------------------|----------------|\n")
            
            tiers = sorted(list(set(b["tier"] for b in bench)))
            for t in tiers:
                p_ram, n_ram = 0, 0
                for b in bench:
                    if b["tier"] == t:
                        ram = b.get("server_peak_ram_mb", 0)
                        if b["db"] == "postgresql": p_ram = max(p_ram, ram)
                        if b["db"] == "neo4j": n_ram = max(n_ram, ram)
                md.write(f"| Tier {t} | {p_ram:.1f} MB | {n_ram:.1f} MB |\n")
            md.write("\n")
            
    print(f"[+] Report generated at {REPORT_PATH.name}")

if __name__ == "__main__":
    main()
