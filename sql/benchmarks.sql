-- =============================================================================
-- Benchmark Query Suite: PostgreSQL (3NF Normalized Schema)
-- 10 Queries across 5 Complexity Tiers
-- All queries wrapped in EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) for server timing
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- TIER 1: Point Lookups & Filtering
-- ─────────────────────────────────────────────────────────────────────────────

-- T1-B: All outgoing links from seed
-- Access Pattern: 3-table join (subreddits -> posts -> hyperlinks -> subreddits).
-- Uses index on subreddits(name) and posts(source_subreddit_id).
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT s_tgt.name AS target_subreddit, p.post_id, p.timestamp, p.source_type, p.post_label
FROM posts p
JOIN subreddits s_src ON s_src.id = p.source_subreddit_id
JOIN hyperlinks h     ON h.post_id = p.id
JOIN subreddits s_tgt ON s_tgt.id = h.target_subreddit_id
WHERE s_src.name = 'leagueoflegends'
ORDER BY p.timestamp DESC;

-- T1-C: Hostile links only from seed
-- Access Pattern: Same as T1-B but utilizes compound index idx_posts_source_label.
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT s_tgt.name AS target_subreddit, p.post_id, p.timestamp, p.source_type
FROM posts p
JOIN subreddits s_src ON s_src.id = p.source_subreddit_id
JOIN hyperlinks h     ON h.post_id = p.id
JOIN subreddits s_tgt ON s_tgt.id = h.target_subreddit_id
WHERE s_src.name = 'leagueoflegends' AND p.post_label = -1
ORDER BY p.timestamp DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- TIER 2: Global Aggregations
-- ─────────────────────────────────────────────────────────────────────────────

-- T2-A: Global in-degree ranking (top 20)
-- Access Pattern: Full table scan on hyperlinks aggregated by target_subreddit_id.
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT s.name AS subreddit, COUNT(*) AS in_degree
FROM hyperlinks h
JOIN subreddits s ON s.id = h.target_subreddit_id
GROUP BY s.name
ORDER BY in_degree DESC
LIMIT 20;

-- T2-B: Top-20 subreddits by hostile link count
-- Access Pattern: Full table scan or index scan on idx_posts_label (= -1) grouped by source.
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT s.name AS subreddit, COUNT(*) AS hostile_count
FROM posts p
JOIN subreddits s ON s.id = p.source_subreddit_id
WHERE p.post_label = -1
GROUP BY s.name
ORDER BY hostile_count DESC
LIMIT 20;


-- ─────────────────────────────────────────────────────────────────────────────
-- TIER 3: Multi-hop Pattern Matching & Joins
-- ─────────────────────────────────────────────────────────────────────────────

-- T3-A: 2-hop common targets of seed_a and seed_b
-- Access Pattern: Intersection of target subreddits via two CTEs joining posts & hyperlinks.
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH targets_a AS (
    SELECT DISTINCT h.target_subreddit_id
    FROM posts p
    JOIN subreddits s ON s.id = p.source_subreddit_id
    JOIN hyperlinks h ON h.post_id = p.id
    WHERE s.name = 'askreddit'
),
targets_b AS (
    SELECT DISTINCT h.target_subreddit_id
    FROM posts p
    JOIN subreddits s ON s.id = p.source_subreddit_id
    JOIN hyperlinks h ON h.post_id = p.id
    WHERE s.name = 'iama'
)
SELECT s.name AS shared_target
FROM targets_a a
JOIN targets_b b USING (target_subreddit_id)
JOIN subreddits s ON s.id = a.target_subreddit_id
ORDER BY s.name
LIMIT 25;

-- T3-C: Common hostile attackers of seed_a and seed_b
-- Access Pattern: Reverse 2-hop intersection filtering on post_label = -1.
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH attackers_a AS (
    SELECT DISTINCT p.source_subreddit_id
    FROM posts p
    JOIN hyperlinks h ON h.post_id = p.id
    JOIN subreddits s ON s.id = h.target_subreddit_id
    WHERE s.name = 'askreddit' AND p.post_label = -1
),
attackers_b AS (
    SELECT DISTINCT p.source_subreddit_id
    FROM posts p
    JOIN hyperlinks h ON h.post_id = p.id
    JOIN subreddits s ON s.id = h.target_subreddit_id
    WHERE s.name = 'iama' AND p.post_label = -1
)
SELECT s.name AS common_attacker
FROM attackers_a a
JOIN attackers_b b USING (source_subreddit_id)
JOIN subreddits s ON s.id = a.source_subreddit_id
ORDER BY s.name
LIMIT 25;


-- ─────────────────────────────────────────────────────────────────────────────
-- TIER 4: Recursive & Complex Self-Joins
-- ─────────────────────────────────────────────────────────────────────────────

-- T4-A: Global mutual hostile pairs (A -> B and B -> A both hostile)
-- Access Pattern: Heavy 6-table self-join across posts and hyperlinks with symmetry breaking.
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
SELECT s_a.name AS sub_a, s_b.name AS sub_b, COUNT(*) AS mutual_hostile_links
FROM posts p_ab
JOIN hyperlinks h_ab ON h_ab.post_id = p_ab.id
JOIN posts p_ba      ON p_ba.source_subreddit_id = h_ab.target_subreddit_id
JOIN hyperlinks h_ba ON h_ba.post_id = p_ba.id AND h_ba.target_subreddit_id = p_ab.source_subreddit_id
JOIN subreddits s_a  ON s_a.id = p_ab.source_subreddit_id
JOIN subreddits s_b  ON s_b.id = h_ab.target_subreddit_id
WHERE p_ab.post_label = -1 AND p_ba.post_label = -1
  AND p_ab.source_subreddit_id < h_ab.target_subreddit_id
GROUP BY s_a.name, s_b.name
ORDER BY mutual_hostile_links DESC
LIMIT 20;

-- T4-B: Bounded BFS shortest path up to depth 3 from seed
-- Access Pattern: Recursive CTE with array cycle detection joining posts and hyperlinks at each hop.
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH RECURSIVE bfs(node_id, depth, path) AS (
    SELECT s.id, 0, ARRAY[s.id]
    FROM subreddits s
    WHERE s.name = 'bestof'
    UNION ALL
    SELECT h.target_subreddit_id, b.depth + 1, b.path || h.target_subreddit_id
    FROM bfs b
    JOIN posts p      ON p.source_subreddit_id = b.node_id
    JOIN hyperlinks h ON h.post_id = p.id
    WHERE b.depth < 3 AND NOT (h.target_subreddit_id = ANY(b.path))
)
SELECT s.name AS reachable, MIN(bfs.depth) AS min_hops
FROM bfs
JOIN subreddits s ON s.id = bfs.node_id
WHERE bfs.depth > 0
GROUP BY s.name
ORDER BY min_hops, s.name
LIMIT 500;


-- ─────────────────────────────────────────────────────────────────────────────
-- TIER 5: Graph Traversal & Cycle Detection (Hostile Workload for Relational)
-- ─────────────────────────────────────────────────────────────────────────────

-- T5-A: 3-node hostile cycles seeded at seed (A -> B -> C -> A all hostile)
-- Access Pattern: 9-table join (3x posts, 3x hyperlinks, 3x subreddits).
-- Highlights relational join complexity vs graph pattern matching.
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
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
WHERE p_ab.post_label = -1 AND s_a.name = 'leagueoflegends'
  AND h_ab.target_subreddit_id < h_bc.target_subreddit_id
LIMIT 50;

-- T5-B: Bounded BFS shortest path up to depth 4 from seed
-- Access Pattern: 4-hop recursive CTE (working set expansion challenges buffer cache).
EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
WITH RECURSIVE bfs(node_id, depth, path) AS (
    SELECT s.id, 0, ARRAY[s.id]
    FROM subreddits s
    WHERE s.name = 'bestof'
    UNION ALL
    SELECT h.target_subreddit_id, b.depth + 1, b.path || h.target_subreddit_id
    FROM bfs b
    JOIN posts p      ON p.source_subreddit_id = b.node_id
    JOIN hyperlinks h ON h.post_id = p.id
    WHERE b.depth < 4 AND NOT (h.target_subreddit_id = ANY(b.path))
)
SELECT s.name AS reachable, MIN(bfs.depth) AS min_hops
FROM bfs
JOIN subreddits s ON s.id = bfs.node_id
WHERE bfs.depth > 0
GROUP BY s.name
ORDER BY min_hops, s.name
LIMIT 500;
