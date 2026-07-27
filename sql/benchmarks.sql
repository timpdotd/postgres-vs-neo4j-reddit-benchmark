-- =============================================================================
-- SQL Benchmark Queries — Reddit Hyperlink Network
-- 10 queries across 5 difficulty tiers for PostgreSQL vs Neo4j comparison.
--
-- Standalone usage (psql):
--   \set seed         'leagueoflegends'
--   \set seed_a       'askreddit'
--   \set seed_b       'worldnews'
--   \set seed_bfs     'dataisbeautiful'
--
-- The Python runner (run_benchmarks.py) auto-selects seeds from the data
-- and substitutes them using psycopg2 named parameters %(name)s.
--
-- Each query here is wrapped with EXPLAIN (ANALYZE, FORMAT JSON) so the file
-- can be used for manual inspection. The runner strips the EXPLAIN prefix and
-- adds it back programmatically to capture server-side Execution Time.
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- T1-B │ TIER 1 │ All outgoing links from a given subreddit
-- ─────────────────────────────────────────────────────────────────────────────
-- Measures: single composite index scan (idx_hl_source).
-- Expected: < 5 ms (returns all links for one subreddit, ordered by time).
-- ─────────────────────────────────────────────────────────────────────────────
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT
    s_tgt.name  AS target_subreddit,
    h.post_id,
    h.timestamp,
    h.source_type,
    h.post_label
FROM       hyperlinks  h
JOIN subreddits s_src ON s_src.id = h.source_subreddit_id
JOIN subreddits s_tgt ON s_tgt.id = h.target_subreddit_id
WHERE s_src.name = :'seed'
ORDER BY h.timestamp DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- T1-C │ TIER 1 │ Hostile links only from a given subreddit
-- ─────────────────────────────────────────────────────────────────────────────
-- Measures: composite index scan (idx_hl_source_label) with post_label = -1.
-- Expected: < 5 ms (subset of T1-B; composite index makes this equally fast).
-- ─────────────────────────────────────────────────────────────────────────────
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT
    s_tgt.name  AS target_subreddit,
    h.post_id,
    h.timestamp,
    h.source_type
FROM       hyperlinks  h
JOIN subreddits s_src ON s_src.id = h.source_subreddit_id
JOIN subreddits s_tgt ON s_tgt.id = h.target_subreddit_id
WHERE s_src.name = :'seed'
  AND h.post_label = -1
ORDER BY h.timestamp DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- T2-A │ TIER 2 │ Global in-degree ranking (top-20 most-linked-to subreddits)
-- ─────────────────────────────────────────────────────────────────────────────
-- Measures: full scan of hyperlinks + GROUP BY + sort — no parameter needed.
-- PostgreSQL may use parallel workers (shared_buffers tuning in docker-compose).
-- Expected: 200–800 ms (858k rows aggregated).
-- ─────────────────────────────────────────────────────────────────────────────
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT
    s.name      AS subreddit,
    COUNT(*)    AS in_degree
FROM       hyperlinks  h
JOIN subreddits s ON s.id = h.target_subreddit_id
GROUP BY s.name
ORDER BY in_degree DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- T2-B │ TIER 2 │ Top-20 subreddits by hostile outgoing link count
-- ─────────────────────────────────────────────────────────────────────────────
-- Measures: index scan on post_label + GROUP BY.
-- The partial-scan via idx_hl_source_label makes this faster than T2-A.
-- Expected: 100–500 ms.
-- ─────────────────────────────────────────────────────────────────────────────
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT
    s.name      AS subreddit,
    COUNT(*)    AS hostile_count
FROM       hyperlinks  h
JOIN subreddits s ON s.id = h.source_subreddit_id
WHERE h.post_label = -1
GROUP BY s.name
ORDER BY hostile_count DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- T3-A │ TIER 3 │ 2-hop common targets (subreddits both r/A and r/B link to)
-- ─────────────────────────────────────────────────────────────────────────────
-- Measures: two index scans + hash join (intersection) + name join.
-- This is the "collaborative filtering" pattern — natural for graph, two CTEs for SQL.
-- Expected: 50–300 ms.
-- ─────────────────────────────────────────────────────────────────────────────
EXPLAIN (ANALYZE, FORMAT JSON)
WITH targets_a AS (
    SELECT DISTINCT h.target_subreddit_id
    FROM   hyperlinks h
    JOIN   subreddits s ON s.id = h.source_subreddit_id
    WHERE  s.name = :'seed_a'
),
targets_b AS (
    SELECT DISTINCT h.target_subreddit_id
    FROM   hyperlinks h
    JOIN   subreddits s ON s.id = h.source_subreddit_id
    WHERE  s.name = :'seed_b'
)
SELECT s.name AS shared_target
FROM   targets_a a
JOIN   targets_b b USING (target_subreddit_id)
JOIN   subreddits s ON s.id = a.target_subreddit_id
ORDER  BY s.name
LIMIT  25;


-- ─────────────────────────────────────────────────────────────────────────────
-- T3-C │ TIER 3 │ Common hostile attackers of both r/A and r/B
-- ─────────────────────────────────────────────────────────────────────────────
-- Measures: two index scans filtered by post_label=-1 + hash join.
-- Answers: "which communities attacked both of these targets?"
-- Expected: 50–300 ms.
-- ─────────────────────────────────────────────────────────────────────────────
EXPLAIN (ANALYZE, FORMAT JSON)
WITH attackers_a AS (
    SELECT DISTINCT h.source_subreddit_id
    FROM   hyperlinks h
    JOIN   subreddits s ON s.id = h.target_subreddit_id
    WHERE  s.name = :'seed_a'
      AND  h.post_label = -1
),
attackers_b AS (
    SELECT DISTINCT h.source_subreddit_id
    FROM   hyperlinks h
    JOIN   subreddits s ON s.id = h.target_subreddit_id
    WHERE  s.name = :'seed_b'
      AND  h.post_label = -1
)
SELECT s.name AS common_attacker
FROM   attackers_a a
JOIN   attackers_b b USING (source_subreddit_id)
JOIN   subreddits s ON s.id = a.source_subreddit_id
ORDER  BY s.name
LIMIT  25;


-- ─────────────────────────────────────────────────────────────────────────────
-- T4-A │ TIER 4 │ Mutual hostile pairs (A→(-1)→B and B→(-1)→A)
-- ─────────────────────────────────────────────────────────────────────────────
-- Measures: self-join on hyperlinks twice, deduplicated by src_id < tgt_id.
-- Each pair (A,B) is reported once with total mutual hostile link count.
-- The composite index idx_hl_src_tgt_label is critical for performance.
-- Expected: 1–10 s (self-join on 170k+ hostile edges).
-- ─────────────────────────────────────────────────────────────────────────────
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT
    s_a.name    AS sub_a,
    s_b.name    AS sub_b,
    COUNT(*)    AS mutual_hostile_links
FROM       hyperlinks  ab
JOIN hyperlinks ba
    ON  ba.source_subreddit_id = ab.target_subreddit_id
    AND ba.target_subreddit_id = ab.source_subreddit_id
    AND ba.post_label          = -1
JOIN subreddits s_a ON s_a.id = ab.source_subreddit_id
JOIN subreddits s_b ON s_b.id = ab.target_subreddit_id
WHERE ab.post_label                 = -1
  AND ab.source_subreddit_id < ab.target_subreddit_id   -- deduplicate pairs
GROUP BY s_a.name, s_b.name
ORDER BY mutual_hostile_links DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- T4-B │ TIER 4 │ Bounded BFS — nodes reachable within 3 hops from seed_bfs
-- ─────────────────────────────────────────────────────────────────────────────
-- Measures: PostgreSQL recursive CTE vs Cypher *1..3 variable-length traversal.
-- Cycle guard: path-array prevents re-visiting nodes already on the current path.
-- seed_bfs is chosen at ~rank 200 by out-degree (medium-low degree node).
-- Expected: 2–20 s in PostgreSQL, < 3 s in Neo4j.
-- ─────────────────────────────────────────────────────────────────────────────
EXPLAIN (ANALYZE, FORMAT JSON)
WITH RECURSIVE bfs(node_id, depth, path) AS (
    -- Base: start at seed node, depth 0
    SELECT s.id, 0, ARRAY[s.id]
    FROM   subreddits s
    WHERE  s.name = :'seed_bfs'

    UNION ALL

    -- Step: follow one outgoing edge, prevent revisiting nodes on current path
    SELECT h.target_subreddit_id,
           b.depth + 1,
           b.path || h.target_subreddit_id
    FROM   bfs b
    JOIN   hyperlinks h ON h.source_subreddit_id = b.node_id
    WHERE  b.depth < 3
      AND  NOT (h.target_subreddit_id = ANY(b.path))
)
SELECT s.name              AS reachable_subreddit,
       MIN(bfs.depth)      AS min_hops
FROM   bfs
JOIN   subreddits s ON s.id = bfs.node_id
WHERE  bfs.depth > 0          -- exclude the seed itself
GROUP  BY s.name
ORDER  BY min_hops, s.name
LIMIT  500;


-- ─────────────────────────────────────────────────────────────────────────────
-- T5-A │ TIER 5 │ 3-node hostile-sentiment cycles (echo chambers), seeded
-- ─────────────────────────────────────────────────────────────────────────────
-- Finds triangles:  seed →(-1)→ B →(-1)→ C →(-1)→ seed
-- Seeded at 'seed' (top hostile sender) to bound the search.
-- Deduplication: report each triangle once (tgt_B_id < tgt_C_id).
-- Expected: 5–60 s in PostgreSQL, 2–15 s in Neo4j.
-- ─────────────────────────────────────────────────────────────────────────────
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT
    s_a.name    AS node_a,
    s_b.name    AS node_b,
    s_c.name    AS node_c
FROM       hyperlinks  ab
JOIN hyperlinks bc
    ON  bc.source_subreddit_id = ab.target_subreddit_id
    AND bc.post_label          = -1
JOIN hyperlinks ca
    ON  ca.source_subreddit_id = bc.target_subreddit_id
    AND ca.target_subreddit_id = ab.source_subreddit_id
    AND ca.post_label          = -1
JOIN subreddits s_a ON s_a.id = ab.source_subreddit_id
JOIN subreddits s_b ON s_b.id = ab.target_subreddit_id
JOIN subreddits s_c ON s_c.id = bc.target_subreddit_id
WHERE ab.post_label = -1
  AND s_a.name = :'seed'
  AND ab.target_subreddit_id < bc.target_subreddit_id   -- deduplicate B,C pair
LIMIT 50;


-- ─────────────────────────────────────────────────────────────────────────────
-- T5-B │ TIER 5 │ Bounded BFS — nodes reachable within 4 hops from seed_bfs
-- ─────────────────────────────────────────────────────────────────────────────
-- Same seed as T4-B, depth extended to 4 — directly shows how one extra hop
-- affects execution time (key chart for the comparison report).
-- Expected: 10 s – 3 min in PostgreSQL, 5–60 s in Neo4j.
-- ─────────────────────────────────────────────────────────────────────────────
EXPLAIN (ANALYZE, FORMAT JSON)
WITH RECURSIVE bfs(node_id, depth, path) AS (
    SELECT s.id, 0, ARRAY[s.id]
    FROM   subreddits s
    WHERE  s.name = :'seed_bfs'

    UNION ALL

    SELECT h.target_subreddit_id,
           b.depth + 1,
           b.path || h.target_subreddit_id
    FROM   bfs b
    JOIN   hyperlinks h ON h.source_subreddit_id = b.node_id
    WHERE  b.depth < 4
      AND  NOT (h.target_subreddit_id = ANY(b.path))
)
SELECT s.name              AS reachable_subreddit,
       MIN(bfs.depth)      AS min_hops
FROM   bfs
JOIN   subreddits s ON s.id = bfs.node_id
WHERE  bfs.depth > 0
GROUP  BY s.name
ORDER  BY min_hops, s.name
LIMIT  500;
