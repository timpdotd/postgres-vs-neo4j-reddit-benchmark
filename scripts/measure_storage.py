#!/usr/bin/env python3
"""
measure_storage.py — Calculates the physical on-disk storage footprint of
PostgreSQL and Neo4j after data has been loaded.
"""

import os
import sys
import json
import subprocess
from pathlib import Path

try:
    import psycopg
except ImportError:
    print("psycopg not installed.")
    sys.exit(1)

DATA_DIR = Path(__file__).parent.parent / "data"

PG_CONFIG = {
    "host":     os.getenv("PG_HOST",  "localhost"),
    "port":     int(os.getenv("PG_PORT",  "5432")),
    "dbname":   os.getenv("PG_DB",    "reddit_benchmark"),
    "user":     os.getenv("PG_USER",  "reddit_user"),
    "password": os.getenv("PG_PASS",  "reddit_password"),
}

def get_postgres_size_bytes() -> int:
    """Returns the size of the PostgreSQL database in bytes."""
    try:
        with psycopg.connect(**PG_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_database_size(%s);", (PG_CONFIG["dbname"],))
                result = cur.fetchone()
                return result[0] if result else 0
    except Exception as e:
        print(f"Error querying Postgres size: {e}")
        return 0

def get_neo4j_size_bytes() -> int:
    """Returns the size of the Neo4j databases directory in bytes via docker exec."""
    try:
        # Run du inside the Neo4j container to get the size of /data/databases
        result = subprocess.run(
            ["docker", "exec", "reddit-neo4j", "du", "-sb", "/data/databases"],
            capture_output=True,
            text=True,
            check=True
        )
        # Output looks like: "4815162342\t/data/databases\n"
        size_str = result.stdout.split()[0]
        return int(size_str)
    except Exception as e:
        print(f"Error querying Neo4j size: {e}")
        return 0

def main():
    print("[*] Measuring physical storage footprints...")
    pg_size = get_postgres_size_bytes()
    neo4j_size = get_neo4j_size_bytes()

    print(f"    -> PostgreSQL size : {pg_size / (1024*1024):.2f} MB")
    print(f"    -> Neo4j size      : {neo4j_size / (1024*1024):.2f} MB")

    metrics = {
        "postgres_bytes": pg_size,
        "neo4j_bytes": neo4j_size
    }

    out_file = DATA_DIR / "storage_metrics.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=4)
    print(f"[+] Storage metrics saved to {out_file.name}")

if __name__ == "__main__":
    main()
