#!/usr/bin/env python3
"""
run_scalability_test.py — Evaluates algorithm complexity scaling (O(1) vs O(N)).
Automatically samples the dataset at 20%, 50%, and 100%, reloads the databases,
and measures execution time for deep pathfinding (T5-B).

WARNING: This script drops databases and reloads them multiple times.
It will take a while to run.
"""

import sys
import json
import time
import shutil
import subprocess
from pathlib import Path

import psycopg
import os

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"

PG_CONFIG = {
    "host":     os.getenv("PG_HOST",  "localhost"),
    "port":     int(os.getenv("PG_PORT",  "5432")),
    "dbname":   os.getenv("PG_DB",   "reddit_benchmark"),
    "user":     os.getenv("PG_USER", "reddit_user"),
    "password": os.getenv("PG_PASS", "reddit_password"),
}

TSV_FILES = [
    "soc-redditHyperlinks-body.tsv",
    "soc-redditHyperlinks-title.tsv"
]

FRACTIONS = [0.2, 0.5, 1.0]

def count_lines(filepath: Path) -> int:
    with open(filepath, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)

def subsample_tsv(src: Path, dest: Path, fraction: float, total_lines: int):
    target_lines = int(total_lines * fraction)
    print(f"    Subsampling {src.name} to {fraction*100:.0f}% ({target_lines} lines)...")
    with open(src, 'r', encoding='utf-8') as fin, open(dest, 'w', encoding='utf-8') as fout:
        for i, line in enumerate(fin):
            if i > target_lines:
                break
            fout.write(line)

def run_script(script_name: str, args: list = None):
    cmd = [sys.executable, str(PROJECT_ROOT / "scripts" / script_name)] + (args or [])
    subprocess.run(cmd, check=True, cwd=PROJECT_ROOT)

def main():
    print("=== O(1) vs O(N) Scalability Testing ===")
    
    # 1. Backup original TSVs
    print("[*] Backing up original dataset...")
    for tsv in TSV_FILES:
        src = DATA_DIR / tsv
        backup = DATA_DIR / f"{tsv}.backup"
        if src.exists() and not backup.exists():
            shutil.copy2(src, backup)
            
    # Calculate total lines
    total_lines_map = {}
    for tsv in TSV_FILES:
        backup = DATA_DIR / f"{tsv}.backup"
        if not backup.exists():
            print(f"Missing dataset {tsv}!")
            return
        total_lines_map[tsv] = count_lines(backup)
        
    results = []
    
    try:
        for fraction in FRACTIONS:
            print(f"\n{'='*50}")
            print(f"[*] Testing Dataset Size: {fraction*100:.0f}%")
            print(f"{'='*50}")
            
            # Subsample
            for tsv in TSV_FILES:
                src = DATA_DIR / f"{tsv}.backup"
                dest = DATA_DIR / tsv
                subsample_tsv(src, dest, fraction, total_lines_map[tsv])
                
            # Run ETL
            print("\n[*] Running ETL pipeline for this dataset size...")
            run_script("load_data.py")

            # After ETL, run VACUUM ANALYZE so PostgreSQL planner statistics reflect
            # the CURRENT dataset size (not stale stats from a previous 100% load).
            # Without this, PG may use query plans optimized for 100% when running 20%.
            print("\n[*] Running VACUUM ANALYZE to update PG planner statistics...")
            try:
                with psycopg.connect(**PG_CONFIG, autocommit=True) as conn:
                    with conn.cursor() as cur:
                        cur.execute("VACUUM ANALYZE subreddits, posts, hyperlinks;")
                print("    VACUUM ANALYZE complete.")
            except Exception as e:
                print(f"    [!] VACUUM ANALYZE failed (non-fatal): {e}")
            
            # Run Benchmark (we only really care about T4-B / T5-B for scaling)
            print("\n[*] Running Benchmark (recording times)...")
            run_script("run_benchmarks.py", ["--runs", "3"])
            
            # Read benchmark_results.json and extract T5-B times
            bench_res = DATA_DIR / "benchmark_results.json"
            if bench_res.exists():
                with open(bench_res, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for row in data:
                        if row["query_id"] == "T5-B":
                            db = row["db"]
                            # Use median execution time
                            ms = row.get("median_execution_ms") or row.get("median_consumed_ms")
                            if ms is not None:
                                results.append({
                                    "dataset_fraction": fraction,
                                    "dataset_pct": int(fraction * 100),
                                    "db": db,
                                    "t5b_execution_ms": ms
                                })
                                print(f"      -> {db.upper()} T5-B Time: {ms:.1f} ms")
                                
    except Exception as e:
        print(f"\n[!] Error during scalability test: {e}")
        
    finally:
        # Restore original TSVs
        print("\n[*] Restoring original dataset backups...")
        for tsv in TSV_FILES:
            src = DATA_DIR / f"{tsv}.backup"
            dest = DATA_DIR / tsv
            if src.exists():
                shutil.move(src, dest)
                
        # Save results
        out_file = DATA_DIR / "scalability_results.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4)
        print(f"\n[+] Scalability results saved to {out_file.name}")
        
        # Recommendation
        print("[!] Note: You must run the master pipeline (run.bat) again to restore the 100% databases.")

if __name__ == "__main__":
    main()
