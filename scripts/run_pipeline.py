#!/usr/bin/env python3
"""
run_pipeline.py — Master End-to-End Orchestration Script.

Course: Data Management (2025/2026) | Author: Davide Timperi (1950722)

This script executes the complete benchmark pipeline from start to finish:
  1. Validates that the Python environment has all required dependencies installed.
  2. Verifies that dataset files exist in the data/ directory.
  3. Executes scripts/load_data.py to bulk-load PostgreSQL and Neo4j.
  4. Executes scripts/run_benchmarks.py to benchmark all 10 queries across 5 tiers.
  5. Summarizes output locations and directs the user to the Jupyter analysis notebook.

Usage:
    python scripts/run_pipeline.py
"""

import os
import sys
import time
import subprocess
from pathlib import Path

try:
    import psycopg
    from neo4j import GraphDatabase
except ImportError:
    pass  # Handled by verify_dependencies()

# ---------------------------------------------------------------------------
# Configuration & Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOAD_SCRIPT = PROJECT_ROOT / "scripts" / "load_data.py"
BENCHMARK_SCRIPT = PROJECT_ROOT / "scripts" / "run_benchmarks.py"
MEASURE_SCRIPT = PROJECT_ROOT / "scripts" / "measure_storage.py"
CONCURRENCY_SCRIPT = PROJECT_ROOT / "scripts" / "run_concurrency.py"
REPORT_SCRIPT = PROJECT_ROOT / "scripts" / "generate_extended_report.py"
NOTEBOOK_PATH = PROJECT_ROOT / "notebooks" / "results_analysis.ipynb"

TSV_FILES = [
    DATA_DIR / "soc-redditHyperlinks-body.tsv",
    DATA_DIR / "soc-redditHyperlinks-title.tsv",
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


def print_header(step: int, total: int, title: str) -> None:
    print("\n" + "#" * 75)
    print(f"#  STEP {step}/{total}: {title}")
    print("#" * 75 + "\n")


def verify_dependencies() -> None:
    """Check that core libraries (psycopg, neo4j, psutil, pandas) are importable."""
    print("[*] Verifying active Python environment dependencies...")
    missing = []
    for pkg, import_name in [
        ("psycopg", "psycopg"),
        ("neo4j", "neo4j"),
        ("psutil", "psutil"),
        ("pandas", "pandas"),
    ]:
        try:
            __import__(import_name)
        except ImportError:
            missing.append(pkg)
            
    if missing:
        print(f"[!] ERROR: Missing required Python packages: {', '.join(missing)}")
        print("    Please activate your virtual environment or run the setup script first:")
        print("        python scripts/setup_env.py")
        sys.exit(1)
    print("    [+] All required Python dependencies are available.")


def verify_dataset() -> None:
    """Verify that SNAP TSV files exist in data/."""
    print(f"[*] Verifying dataset files in {DATA_DIR}...")
    missing = [f.name for f in TSV_FILES if not f.exists()]
    if missing:
        print(f"[!] ERROR: Missing dataset files in data/: {', '.join(missing)}")
        print("    Please download the Reddit Hyperlink Network dataset from SNAP:")
        print("        https://snap.stanford.edu/data/soc-RedditHyperlinks.html")
        print(f"    and place the uncompressed .tsv files into: {DATA_DIR}")
        sys.exit(1)
    for f in TSV_FILES:
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"    [+] Found {f.name} ({size_mb:.1f} MB)")


def check_databases_populated() -> bool:
    """Check if PostgreSQL and Neo4j already contain data."""
    print("[*] Checking if databases are already populated...")
    
    # Check Postgres
    pg_populated = False
    try:
        with psycopg.connect(**PG_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM subreddits;")
                count = cur.fetchone()[0]
                if count > 0:
                    pg_populated = True
    except Exception:
        pass

    # Check Neo4j
    neo4j_populated = False
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        with driver.session() as session:
            result = session.run("MATCH (n:Post) RETURN count(n) AS c")
            count = result.single()["c"]
            if count > 0:
                neo4j_populated = True
        driver.close()
    except Exception:
        pass

    if pg_populated and neo4j_populated:
        print("    [+] Both databases already contain data. Skipping ETL.")
        return True
    
    print("    [-] Databases are empty or partially populated. ETL is required.")
    return False


def run_script(script_path: Path, step_name: str) -> float:
    """Execute a Python script as a subprocess and return execution time in seconds."""
    t0 = time.perf_counter()
    try:
        subprocess.run(
            [sys.executable, str(script_path)],
            check=True,
            cwd=PROJECT_ROOT,
        )
    except subprocess.CalledProcessError as exc:
        print(f"\n[!] ERROR: {step_name} failed with exit code {exc.returncode}.")
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n[!] WARNING: {step_name} interrupted by user.")
        sys.exit(1)
    return time.perf_counter() - t0


def main() -> None:
    total_steps = 6
    print("\n" + "=" * 75)
    print("  POSTGRESQL vs. NEO4J — MASTER BENCHMARK PIPELINE ORCHESTRATION")
    print("=" * 75)

    # Step 1: Validate Environment
    print_header(1, total_steps, "Validating Environment & Dataset")
    verify_dependencies()
    verify_dataset()

    # Step 2: ETL Loader
    print_header(2, total_steps, "Executing ETL Pipeline (scripts/load_data.py)")
    
    if check_databases_populated():
        print("\n[+] Skipping ETL Pipeline (Data already loaded).")
        load_time = 0.0
    else:
        load_time = run_script(LOAD_SCRIPT, "ETL Loader")
        print(f"\n[+] ETL Pipeline completed in {load_time:.2f} seconds.")

    print("\n[*] Running Storage Profiler (scripts/measure_storage.py)...")
    run_script(MEASURE_SCRIPT, "Storage Profiler")

    # Step 3: Benchmark Suite
    print_header(3, total_steps, "Executing Comparative Benchmark Suite (scripts/run_benchmarks.py)")
    bench_time = run_script(BENCHMARK_SCRIPT, "Benchmark Runner")
    print(f"\n[+] Benchmark Suite completed in {bench_time:.2f} seconds.")

    # Step 4: Concurrency & Throughput
    print_header(4, total_steps, "Executing Concurrency Tests (scripts/run_concurrency.py)")
    conc_time = run_script(CONCURRENCY_SCRIPT, "Concurrency Runner")
    print(f"\n[+] Concurrency Tests completed in {conc_time:.2f} seconds.")

    # Step 5: Generate Report
    print_header(5, total_steps, "Generating Extended Markdown Report (scripts/generate_extended_report.py)")
    rep_time = run_script(REPORT_SCRIPT, "Report Generator")
    print(f"\n[+] Report Generation completed in {rep_time:.2f} seconds.")

    # Step 6: Summary & Next Steps
    print_header(6, total_steps, "Pipeline Execution Complete!")
    print(f"[*] Total Pipeline Execution Time: {(load_time + bench_time + conc_time + rep_time):.2f} seconds.")
    print("\n[Chart] Results & Analysis:")
    print(f"    1. Raw benchmark JSON saved to: {DATA_DIR / 'benchmark_results.json'}")
    print(f"    2. Extended Markdown Report: {PROJECT_ROOT / 'extended_analysis.md'}")
    print(f"    3. To view charts, box plots, and Q12 ergonomic analysis, launch Jupyter:")
    print(f"           jupyter notebook {NOTEBOOK_PATH}\n")
    print("=" * 75 + "\n")


if __name__ == "__main__":
    main()
