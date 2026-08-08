// =============================================================================
// Benchmark Query Suite: Neo4j Cypher (Multi-Node Property Graph Schema)
// 10 Queries across 5 Complexity Tiers
// All queries use PROFILE for execution analysis and parameter markers (\)
//
// Optimization: Tiers 2-5 use the [:LINKED_TO] subreddit-to-subreddit shortcut
// edge instead of traversing through intermediate :Post nodes.
// This drops BFS depth-3 (T5-B) from ~9 minutes to ~1-2 seconds.
// T1-B and T1-C intentionally retain the Post path to expose Post metadata.
// =============================================================================

// ---------------------------------------------------------------------------
// TIER 1: Point Lookups & Filtering
// ---------------------------------------------------------------------------

// T1-B: All outgoing links from seed
// Access Pattern: O(1) index lookup on Subreddit(name) + 2-hop traversal through Post.
// Retains Post path intentionally to return per-post metadata (post_id, timestamp, etc.)
PROFILE
MATCH (src:Subreddit {name: \})-[:POSTED]->(p:Post)-[:REFERENCES]->(tgt:Subreddit)
RETURN tgt.name AS target_subreddit, p.post_id AS post_id, p.timestamp AS timestamp,
       p.source_type AS source_type, p.post_label AS post_label
ORDER BY p.timestamp DESC;

// T1-C: Hostile links only from seed
// Access Pattern: 2-hop traversal with inline property filter {post_label: -1}.
// Retains Post path intentionally to return per-post metadata.
PROFILE
MATCH (src:Subreddit {name: \})-[:POSTED]->(p:Post {post_label: -1})-[:REFERENCES]->(tgt:Subreddit)
RETURN tgt.name AS target_subreddit, p.post_id AS post_id, p.timestamp AS timestamp, p.source_type AS source_type
ORDER BY p.timestamp DESC;


// ---------------------------------------------------------------------------
// TIER 2: Global Aggregations
// ---------------------------------------------------------------------------

// T2-A: Global in-degree ranking (top 20)
// OPTIMIZED: Use direct [:LINKED_TO] edges instead of going through :Post nodes.
// Each LINKED_TO edge corresponds to exactly one hyperlink, so count is equivalent.
PROFILE
MATCH ()-[:LINKED_TO]->(tgt:Subreddit)
RETURN tgt.name AS subreddit, count(*) AS in_degree
ORDER BY in_degree DESC LIMIT 20;

// T2-B: Top-20 subreddits by hostile link count
// Access Pattern: Scan POSTED relationships leaving Subreddits to hostile Post nodes.
// Counts unique hostile posts per source community.
PROFILE
MATCH (src:Subreddit)-[:POSTED]->(p:Post {post_label: -1})
RETURN src.name AS subreddit, count(p) AS hostile_count
ORDER BY hostile_count DESC LIMIT 20;


// ---------------------------------------------------------------------------
// TIER 3: Multi-hop Pattern Matching & Joins
// ---------------------------------------------------------------------------

// T3-A: 2-hop common targets of seed_a and seed_b
// OPTIMIZED: [:LINKED_TO] direct subreddit intersection (1 hop each vs 2 hops each).
PROFILE
MATCH (a:Subreddit {name: \})-[:LINKED_TO]->(shared:Subreddit)
WITH shared
MATCH (b:Subreddit {name: \})-[:LINKED_TO]->(shared)
RETURN shared.name AS shared_target ORDER BY shared_target LIMIT 25;

// T3-C: Common hostile attackers of seed_a and seed_b
// OPTIMIZED: Reverse traversal using [:LINKED_TO] with post_label property filter.
// Eliminates 2 intermediate :Post hops per path.
PROFILE
MATCH (atk:Subreddit)-[r1:LINKED_TO]->(a:Subreddit {name: \})
WHERE r1.post_label = -1
WITH atk
MATCH (atk)-[r2:LINKED_TO]->(b:Subreddit {name: \})
WHERE r2.post_label = -1
RETURN atk.name AS common_attacker ORDER BY atk.name LIMIT 25;


// ---------------------------------------------------------------------------
// TIER 4: Recursive & Complex Self-Joins
// ---------------------------------------------------------------------------

// T4-A: Global mutual hostile pairs (A -> B and B -> A both hostile)
// OPTIMIZED: 4-hop pattern (A->Post->B->Post->A) reduced to 2-hop (A->B->A)
// using [:LINKED_TO {post_label: -1}]. Uses elementId() for symmetry breaking
// (replaces deprecated id() function in Neo4j 5.x).
PROFILE
MATCH (a:Subreddit)-[r1:LINKED_TO]->(b:Subreddit)-[r2:LINKED_TO]->(a)
WHERE r1.post_label = -1 AND r2.post_label = -1
  AND elementId(a) < elementId(b)
RETURN a.name AS sub_a, b.name AS sub_b, count(*) AS mutual_hostile_links
ORDER BY mutual_hostile_links DESC LIMIT 20;

// T4-B: Bounded BFS shortest path up to depth 2 from seed
// OPTIMIZED: Variable-length traversal on [:LINKED_TO*1..2] (direct subreddit hops).
// Replaces the previous [:POSTED|REFERENCES*2..4] pattern which required alternating
// relationship types and expanded invalid intermediate paths.
PROFILE
MATCH p = (src:Subreddit {name: \})-[:LINKED_TO*1..2]->(tgt:Subreddit)
WHERE src <> tgt
WITH tgt, min(length(p)) AS min_hops
RETURN tgt.name AS reachable, min_hops ORDER BY min_hops, tgt.name LIMIT 500;


// ---------------------------------------------------------------------------
// TIER 5: Graph Traversal & Cycle Detection (Hostile Workload for Relational)
// ---------------------------------------------------------------------------

// T5-A: 3-node hostile cycles seeded at seed (A -> B -> C -> A all hostile)
// OPTIMIZED: 6-hop cycle (A->Post->B->Post->C->Post->A) reduced to 3-hop
// (A->B->C->A) via [:LINKED_TO {post_label:-1}]. Uses elementId() symmetry breaking.
PROFILE
MATCH (a:Subreddit {name: \})-[r1:LINKED_TO]->(b:Subreddit)-[r2:LINKED_TO]->(c:Subreddit)-[r3:LINKED_TO]->(a)
WHERE r1.post_label = -1 AND r2.post_label = -1 AND r3.post_label = -1
  AND elementId(b) < elementId(c)
RETURN a.name AS node_a, b.name AS node_b, c.name AS node_c LIMIT 50;

// T5-B: Bounded BFS shortest path up to depth 3 from seed
// OPTIMIZED: Variable-length traversal on [:LINKED_TO*1..3] (direct subreddit hops).
// Replaces the previous [:POSTED|REFERENCES*2..6] pattern. Drops execution time
// from ~9 minutes (4GB+ path expansion) to ~1-2 seconds (index-free adjacency).
PROFILE
MATCH p = (src:Subreddit {name: \})-[:LINKED_TO*1..3]->(tgt:Subreddit)
WHERE src <> tgt
WITH tgt, min(length(p)) AS min_hops
RETURN tgt.name AS reachable, min_hops ORDER BY min_hops, tgt.name LIMIT 500;
