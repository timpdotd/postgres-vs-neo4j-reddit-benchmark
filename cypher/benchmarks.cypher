// =============================================================================
// Cypher Benchmark Queries — Reddit Hyperlink Network
// 10 queries mirroring sql/benchmarks.sql for the PostgreSQL vs Neo4j comparison.
//
// Usage in Neo4j Browser:
//   :param seed       => 'leagueoflegends'
//   :param seed_a     => 'askreddit'
//   :param seed_b     => 'worldnews'
//   :param seed_bfs   => 'dataisbeautiful'
//
// Prefix each query with PROFILE (executes + shows db-hits) or
// EXPLAIN (plan only, does not execute) for manual inspection.
// The Python runner (run_benchmarks.py) calls these without PROFILE and
// extracts server-side timing from result.consume().result_consumed_after.
// =============================================================================


// ─────────────────────────────────────────────────────────────────────────────
// T1-B │ TIER 1 │ All outgoing links from a given subreddit
// ─────────────────────────────────────────────────────────────────────────────
// Single traversal from indexed Subreddit node; relationship properties returned.
// No WHERE clause beyond the node lookup — all relationships included.
// ─────────────────────────────────────────────────────────────────────────────
PROFILE
MATCH (src:Subreddit {name: $seed})-[h:HYPERLINKS_TO]->(tgt:Subreddit)
RETURN
    tgt.name      AS target_subreddit,
    h.post_id,
    h.timestamp,
    h.source_type,
    h.post_label
ORDER BY h.timestamp DESC;


// ─────────────────────────────────────────────────────────────────────────────
// T1-C │ TIER 1 │ Hostile links only from a given subreddit
// ─────────────────────────────────────────────────────────────────────────────
// Same traversal pattern as T1-B with post_label filter on the relationship.
// Uses rel_idx_post_label; compare db-hits vs T1-B for index effectiveness.
// ─────────────────────────────────────────────────────────────────────────────
PROFILE
MATCH (src:Subreddit {name: $seed})-[h:HYPERLINKS_TO {post_label: -1}]->(tgt:Subreddit)
RETURN
    tgt.name      AS target_subreddit,
    h.post_id,
    h.timestamp,
    h.source_type
ORDER BY h.timestamp DESC;


// ─────────────────────────────────────────────────────────────────────────────
// T2-A │ TIER 2 │ Global in-degree ranking (top-20 most-linked-to subreddits)
// ─────────────────────────────────────────────────────────────────────────────
// Traverses all HYPERLINKS_TO relationships and aggregates by target node.
// No seed parameter — full graph scan equivalent to T2-A in SQL.
// ─────────────────────────────────────────────────────────────────────────────
PROFILE
MATCH ()-[h:HYPERLINKS_TO]->(tgt:Subreddit)
RETURN
    tgt.name      AS subreddit,
    count(h)      AS in_degree
ORDER BY in_degree DESC
LIMIT 20;


// ─────────────────────────────────────────────────────────────────────────────
// T2-B │ TIER 2 │ Top-20 subreddits by hostile outgoing link count
// ─────────────────────────────────────────────────────────────────────────────
// Uses rel_idx_post_label index to scan only hostile (-1) edges.
// Compares to T2-A to show effect of relationship index on global aggregation.
// ─────────────────────────────────────────────────────────────────────────────
PROFILE
MATCH (src:Subreddit)-[:HYPERLINKS_TO {post_label: -1}]->()
RETURN
    src.name      AS subreddit,
    count(*)      AS hostile_count
ORDER BY hostile_count DESC
LIMIT 20;


// ─────────────────────────────────────────────────────────────────────────────
// T3-A │ TIER 3 │ 2-hop common targets (subreddits both r/A and r/B link to)
// ─────────────────────────────────────────────────────────────────────────────
// Natural graph pattern: two nodes pointing to a shared intermediate.
// The WITH clause intersects the results of two independent traversals —
// far more readable than the two-CTE SQL approach.
// ─────────────────────────────────────────────────────────────────────────────
PROFILE
MATCH (a:Subreddit {name: $seed_a})-[:HYPERLINKS_TO]->(shared:Subreddit)
WITH shared
MATCH (b:Subreddit {name: $seed_b})-[:HYPERLINKS_TO]->(shared)
RETURN
    shared.name   AS shared_target
ORDER BY shared_target
LIMIT 25;


// ─────────────────────────────────────────────────────────────────────────────
// T3-C │ TIER 3 │ Common hostile attackers of both r/A and r/B
// ─────────────────────────────────────────────────────────────────────────────
// Two-pass pattern: find hostile attackers of r/A, then check which of them
// also attacked r/B. The WITH clause passes the attacker set to the second MATCH.
// ─────────────────────────────────────────────────────────────────────────────
PROFILE
MATCH (atk:Subreddit)-[:HYPERLINKS_TO {post_label: -1}]->(a:Subreddit {name: $seed_a})
WITH atk
MATCH (atk)-[:HYPERLINKS_TO {post_label: -1}]->(b:Subreddit {name: $seed_b})
RETURN
    atk.name      AS common_attacker
ORDER BY atk.name
LIMIT 25;


// ─────────────────────────────────────────────────────────────────────────────
// T4-A │ TIER 4 │ Mutual hostile pairs (A→(-1)→B and B→(-1)→A)
// ─────────────────────────────────────────────────────────────────────────────
// Bidirectional hostile pattern — declared in a single MATCH clause.
// Compare to T4-A SQL which requires a self-join and explicit deduplication.
// id(a) < id(b) deduplicates: each pair reported once.
// ─────────────────────────────────────────────────────────────────────────────
PROFILE
MATCH (a:Subreddit)-[ab:HYPERLINKS_TO {post_label: -1}]->(b:Subreddit)
      -[:HYPERLINKS_TO {post_label: -1}]->(a)
WHERE id(a) < id(b)
RETURN
    a.name        AS sub_a,
    b.name        AS sub_b,
    count(ab)     AS mutual_hostile_links
ORDER BY mutual_hostile_links DESC
LIMIT 20;


// ─────────────────────────────────────────────────────────────────────────────
// T4-B │ TIER 4 │ Bounded BFS — nodes reachable within 3 hops from seed_bfs
// ─────────────────────────────────────────────────────────────────────────────
// *1..3 variable-length path — replaces the entire recursive CTE in T4-B SQL.
// min(length(p)) selects shortest path to each reachable node.
// seed_bfs is a medium-low-degree subreddit to bound the expansion.
// ─────────────────────────────────────────────────────────────────────────────
PROFILE
MATCH p = (src:Subreddit {name: $seed_bfs})-[:HYPERLINKS_TO*1..3]->(tgt:Subreddit)
WHERE src <> tgt
WITH tgt, min(length(p)) AS min_hops
RETURN
    tgt.name      AS reachable_subreddit,
    min_hops
ORDER BY min_hops, tgt.name
LIMIT 500;


// ─────────────────────────────────────────────────────────────────────────────
// T5-A │ TIER 5 │ 3-node hostile-sentiment cycles (echo chambers), seeded
// ─────────────────────────────────────────────────────────────────────────────
// Closed triangle pattern: seed →(-1)→ b →(-1)→ c →(-1)→ seed
// The cycle is expressed as a single MATCH — compare to the triple self-join
// and explicit deduplication needed in T5-A SQL.
// id(b) < id(c) deduplicates the {b,c} pair (seed/a is fixed).
// ─────────────────────────────────────────────────────────────────────────────
PROFILE
MATCH (a:Subreddit {name: $seed})
      -[:HYPERLINKS_TO {post_label: -1}]->(b:Subreddit)
      -[:HYPERLINKS_TO {post_label: -1}]->(c:Subreddit)
      -[:HYPERLINKS_TO {post_label: -1}]->(a)
WHERE id(b) < id(c)
RETURN
    a.name        AS node_a,
    b.name        AS node_b,
    c.name        AS node_c
LIMIT 50;


// ─────────────────────────────────────────────────────────────────────────────
// T5-B │ TIER 5 │ Bounded BFS — nodes reachable within 4 hops from seed_bfs
// ─────────────────────────────────────────────────────────────────────────────
// Same seed and structure as T4-B with *1..4 — one extra hop.
// The depth-3 vs depth-4 comparison shows how BFS scales in both engines.
// ─────────────────────────────────────────────────────────────────────────────
PROFILE
MATCH p = (src:Subreddit {name: $seed_bfs})-[:HYPERLINKS_TO*1..4]->(tgt:Subreddit)
WHERE src <> tgt
WITH tgt, min(length(p)) AS min_hops
RETURN
    tgt.name      AS reachable_subreddit,
    min_hops
ORDER BY min_hops, tgt.name
LIMIT 500;
