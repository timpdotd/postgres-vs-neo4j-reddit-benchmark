# PostgreSQL vs. Neo4j: Reddit Hyperlink Network Benchmark

**Course:** Data Management (2025/2026)  
**Author / Group Member:** Davide Timperi (Matricola: `1950722`)  
**Project Type:** [NoSQL] Relational vs. Graph Database Comparison  

---

## 🎯 Project Overview & Objectives

The primary focus of this project is to evaluate, benchmark, and compare the analytical capabilities and efficiency of a traditional **Relational DBMS (PostgreSQL)** against a native **Graph Database (Neo4j)**. 

By implementing an end-to-end analytical pipeline on a real-world social network dataset, this project highlights the architectural advantages, trade-offs, and scalability limits of both relational and graph-based approaches when dealing with interconnected data.

Key evaluation pillars include:
1. **Structural Efficiency & Modeling:** Designing a normalized multi-entity relational schema (with foreign keys and join tables) alongside an equivalent multi-label graph schema in Neo4j representing communities, posts, and distinct interaction types.
2. **Quantitative Performance Benchmarking:** Measuring query execution times and system resource consumption across analytical queries of increasing complexity:
   - **Point Lookups:** Simple direct edge retrievals and filtering.
   - **2-Hop Collaborative Filtering:** Intermediate neighborhood joins and recommendation patterns.
   - **Complex Recursive Path-Finding:** Deep graph traversals, such as cycle detection to identify closed *"echo chambers"* or mutual toxic harassment loops between communities.
3. **Qualitative & Ergonomic Analysis:** Contrasting SQL recursive Common Table Expressions (CTEs) against native Cypher traversal syntax to assess expressive power, code readability, and developer ease-of-use.

---

## 📊 Dataset Description

This project utilizes the **Reddit Hyperlink Network** dataset provided by the [Stanford Network Analysis Project (SNAP)](https://snap.stanford.edu/data/soc-RedditHyperlinks.html).

* **Timespan:** 2.5 years (January 2014 – April 2017)
* **Scale:** 
  * **Nodes:** `55,863` unique subreddits (communities)
  * **Edges:** `858,490` post-to-post hyperlinks across communities
  * **Embeddings:** `51,278` subreddit feature vectors
* **Network Classification:** Directed, Signed, Temporal, and Attributed.

### Key Network Characteristics
* **Directed Interactions:** Every hyperlink traces a distinct connection originating from a source subreddit post and linking to a target subreddit post.
* **Temporal Tracking:** Each interaction carries a precise chronological timestamp.
* **Attributed Properties:** Includes a multi-dimensional text property vector capturing the linguistic features of the source post.
* **Signed Edge Weights:** Every interaction is annotated with a discrete edge weight derived from a high-accuracy crowdsourced classifier:
  * `-1`: Explicitly hostile / negative sentiment (e.g., brigade or harassment links)
  * `+1`: Neutral / positive sentiment (e.g., informative or supportive cross-references)

---

## 🛠️ Repository Structure

```text
📦 postgres-vs-neo4j-reddit-benchmark
 ┣ 📂 cypher/                # Neo4j implementation
 ┃ ┣ 📜 benchmarks.cypher    # Analytical Cypher path-finding traversals (PROFILE)
 ┃ ┗ 📜 schema.cypher        # Uniqueness constraints, node/relationship property indexes
 ┣ 📂 data/                  # Downloaded SNAP Reddit TSV/CSV dataset files (git-ignored)
 ┣ 📂 docker/                # Containerization configs
 ┃ ┗ 📜 docker-compose.yml   # Local deployment for PostgreSQL & Neo4j instances
 ┣ 📂 docs/                  # Academic documentation
 ┃ ┗ 📜 conceptual_model.md  # 3NF E/R + Property Graph design justifications (FAQ Q12/Q13)
 ┣ 📂 graphs/                # Generated charts and visualizations
 ┣ 📂 logs/                  # Execution logs (git-ignored)
 ┣ 📂 notebooks/             # Data visualization & comparative analysis
 ┃ ┗ 📜 results_analysis.ipynb # Jupyter Notebook for interactive charts and analysis
 ┣ 📂 scripts/               # Python automation pipeline
 ┃ ┣ 📜 export_html.py       # Exports markdown reports to HTML format
 ┃ ┣ 📜 generate_extended_report.py # Compiles JSON metrics → extended_analysis.md
 ┃ ┣ 📜 generate_graphs.py   # Matplotlib chart generator → outputs graphs/*.png
 ┃ ┣ 📜 load_data.py         # ETL: parses SNAP TSVs, 3NF-normalizes, bulk-loads both DBs
 ┃ ┣ 📜 measure_storage.py   # Measures on-disk storage for both databases via Docker stats
 ┃ ┣ 📜 run_benchmarks.py    # Benchmark runner: EXPLAIN ANALYZE (PG) + consumed_after (Neo4j)
 ┃ ┣ 📜 run_concurrency.py   # Concurrent workload test (1/10/50 workers — QPS & p95 latency)
 ┃ ┣ 📜 run_pipeline.py      # End-to-end orchestrator (runs all steps in sequence)
 ┃ ┣ 📜 run_scalability_test.py # Scalability test: T5-B at 20/50/100% dataset sizes
 ┃ ┗ 📜 setup_env.py         # Validates environment and Python dependencies
 ┣ 📂 sql/                   # PostgreSQL implementation
 ┃ ┣ 📜 benchmarks.sql       # Analytical SQL queries and recursive CTEs (EXPLAIN ANALYZE)
 ┃ ┣ 📜 schema.sql           # Normalized tables (deferred constraints for bulk load performance)
 ┃ ┗ 📜 schema_indexes.sql   # Foreign keys, unique constraints, and covering indexes (applied post-COPY)
 ┣ 📜 [DM 25_26][1950722] Project Proposal.pdf # Project proposal document
 ┣ 📜 cleanup.bat            # Windows batch script to clean up generated files and containers
 ┣ 📜 extended_analysis.md   # Extended metrics report (generated)
 ┣ 📜 pyproject.toml         # Python project configuration
 ┣ 📜 requirements.txt       # Python dependencies
 ┣ 📜 run.bat                # Windows batch script to execute the master benchmark pipeline
 ┗ 📜 setup.bat              # Windows batch script for environment setup
```

---

## 🚀 Setup & Execution Instructions

### 1. Prerequisites
* **Docker & Docker Compose** (for running PostgreSQL and Neo4j locally)
* **Python 3.10+** (for data loading and benchmark automation)

### 2. Windows Quick Start (Batch Scripts)
For Windows users, turnkey batch scripts are provided to automate the workflow:
- **`setup.bat`**: Validates the environment and installs Python dependencies.
- **`run.bat`**: Executes the master pipeline end-to-end (ETL, benchmarks, metrics, and reports).
- **`cleanup.bat`**: Cleans up generated files, logs, and optionally wipes the Docker database volumes.

### 3. Environment Setup (Manual)
Clone the repository and spin up the database containers:
```bash
# Start PostgreSQL and Neo4j containers in the background
cd docker
docker-compose up -d
# Install Python dependencies
pip install -e .
```

### 4. Data Ingestion
Download both TSV files from [SNAP Reddit Hyperlinks](https://snap.stanford.edu/data/soc-RedditHyperlinks.html) into the `data/` directory, then execute the ETL loader:
```bash
python scripts/load_data.py
```

### 5. Running Benchmarks
Execute the comparative benchmark suite (10 queries × 5 warm runs each):
```bash
python scripts/run_benchmarks.py
```

### 6. Generate Charts & Extended Analysis
```bash
# Generate all 9 comparison charts → graphs/*.png
cd scripts && python generate_graphs.py && cd ..

# Compile ETL, storage, concurrency and scalability metrics → extended_analysis.md
python scripts/generate_extended_report.py
```

### 7. Optional: Advanced Metric Tests
```bash
# Concurrency/throughput test (1, 10, 50 parallel workers)
python scripts/run_concurrency.py

# Scalability test — reloads DB at 20/50/100% to measure O(1) vs O(N) growth
python scripts/run_scalability_test.py
```

### 8. Open Jupyter Notebook (Interactive Analysis)
```bash
jupyter notebook notebooks/results_analysis.ipynb
```

> **Note on Neo4j timing:** `result_consumed_after` (the primary metric) includes server computation **plus** Bolt network transfer. For queries returning ≤50 rows this overhead is negligible (<1ms). For queries returning thousands of rows (T1-B: 24k rows), the notebook includes a per-query decomposition of the transfer component. `result_available_after` is shown for reference only — it consistently reports ~1ms for all queries regardless of complexity due to Neo4j's lazy cursor initialization.