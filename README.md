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
 ┣ 📂 data/                  # Downloaded SNAP Reddit TSV/CSV dataset files (git-ignored)
 ┣ 📂 docker/                # Containerization configs
 ┃ ┗ 📜 docker-compose.yml   # Local deployment for PostgreSQL & Neo4j instances
 ┣ 📂 sql/                   # PostgreSQL implementation
 ┃ ┣ 📜 schema.sql           # Normalized tables, primary/foreign keys, and indexes
 ┃ ┗ 📜 benchmarks.sql       # Analytical SQL queries and recursive CTEs
 ┣ 📂 cypher/                # Neo4j implementation
 ┃ ┣ 📜 schema.cypher        # Node labels, relationship definitions, and edge indexes
 ┃ ┗ 📜 benchmarks.cypher    # Analytical Cypher path-finding traversals
 ┣ 📂 scripts/               # Python automation pipeline
 ┃ ┣ 📜 load_data.py         # ETL script to parse SNAP files and populate both databases
 ┃ ┗ 📜 run_benchmarks.py    # Automated test runner for timing query execution & resource usage
 ┗ 📂 notebooks/             # Data visualization & comparative analysis
   ┗ 📜 results_analysis.ipynb # Jupyter Notebook generating charts and final evaluation reports
```

---

## 🚀 Setup & Execution Instructions

### 1. Prerequisites
* **Docker & Docker Compose** (for running PostgreSQL and Neo4j locally)
* **Python 3.10+** (for data loading and benchmark automation)

### 2. Environment Setup
Clone the repository and spin up the database containers:
```bash
# Start PostgreSQL and Neo4j containers in the background
cd docker
docker-compose up -d
```

### 3. Data Ingestion
Download the dataset from [SNAP Reddit Hyperlinks](https://snap.stanford.edu/data/soc-RedditHyperlinks.html) into the `data/` directory, then execute the ETL loader:
```bash
python scripts/load_data.py
```

### 4. Running Benchmarks & Analysis
Execute the comparative benchmark suite across both database engines:
```bash
python scripts/run_benchmarks.py
```
Once the benchmarks complete, open `notebooks/results_analysis.ipynb` in Jupyter or VS Code to view the visual performance comparisons, execution time graphs, and ergonomic analysis.