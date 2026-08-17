# PostgreSQL vs Neo4j: Advanced Metrics Report

## 1. Storage & Ingestion (ETL)

| Metric | PostgreSQL | Neo4j | Winner |
|--------|------------|-------|--------|
| Ingestion Time | 48.44 s | 465.04 s | **PostgreSQL** |
| Storage Size | 418.92 MB | 1613.25 MB | **PostgreSQL** |

*PostgreSQL often wins in storage density due to 3NF normalization, while Neo4j trades disk space for index-free adjacency pointers.* 

## 2. Concurrency & Throughput (QPS)

| Concurrency Level | Postgres QPS | Neo4j QPS | Postgres p95 (ms) | Neo4j p95 (ms) |
|-------------------|--------------|-----------|-------------------|----------------|
| 1 workers | 2.7 | 2.8 | 678.3 | 1145.7 |
| 10 workers | 9.5 | 9.7 | 1972.2 | 1695.6 |
| 50 workers | 7.8 | 4.1 | 12964.4 | 20103.8 |

## 3. Scalability (T5-B Pathfinding)

| Dataset Size | PostgreSQL Time (ms) | Neo4j Time (ms) |
|--------------|----------------------|-----------------|
| 20% | 15832.4 | 2.0 |
| 50% | 257604.7 | 2.0 |
| 100% | 298923.9 | 1.0 |

*Notice how Neo4j execution time remains flat (O(1) relative to total DB size), whereas PostgreSQL query time degrades as B-Tree indices and Join hash tables grow (O(N) or O(log N)).*

## 4. Peak Memory Consumption (RSS)

| Query Tier | Postgres Peak RAM | Neo4j Peak RAM |
|------------|-------------------|----------------|
| Tier 1 | 0.0 MB | 0.0 MB |
| Tier 2 | 0.0 MB | 0.0 MB |
| Tier 3 | 0.0 MB | 0.0 MB |
| Tier 4 | 0.0 MB | 0.0 MB |
| Tier 5 | 0.0 MB | 0.0 MB |

