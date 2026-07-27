// =============================================================================
// Benchmark Query Suite: Neo4j Cypher (Multi-Node Property Graph Schema)
// 10 Queries across 5 Complexity Tiers
// All queries use PROFILE for execution analysis and parameter markers ($param)
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// TIER 1: Point Lookups & Filtering
// ─────────────────────────────────────────────────────────────────────────────

// T1-B: All outgoing links from seed
// Access Pattern: O(1) index lookup on Subreddit(name) followed by 2-hop traversal.
PROFILE
MATCH (src:Subreddit {name: $seed})-[:POSTED]->(p:Post)-[:REFERENCES]->(tgt:Subreddit)
RETURN tgt.name AS target_subreddit, p.post_id AS post_id, p.timestamp AS timestamp,
       p.source_type AS source_type, p.post_label AS post_label
ORDER BY p.timestamp DESC;

// T1-C: Hostile links only from seed
// Access Pattern: 2-hop traversal with inline property filter {post_label: -1}.
PROFILE
MATCH (src:Subreddit {name: $seed})-[:POSTED]->(p:Post {post_label: -1})-[:REFERENCES]->(tgt:Subreddit)
RETURN tgt.name AS target_subreddit, p.post_id AS post_id, p.timestamp AS timestamp, p.source_type AS source_type
ORDER BY p.timestamp DESC;


// ─────────────────────────────────────────────────────────────────────────────
// TIER 2: Global Aggregations
// ─────────────────────────────────────────────────────────────────────────────

// T2-A: Global in-degree ranking (top 20)
// Access Pattern: Scan all REFERENCES relationships entering Subreddit nodes.
PROFILE
MATCH ()-[:REFERENCES]->(tgt:Subreddit)
RETURN tgt.name AS subreddit, count(*) AS in_degree
ORDER BY in_degree DESC LIMIT 20;

// T2-B: Top-20 subreddits by hostile link count
// Access Pattern: Scan POSTED relationships leaving Subreddits to hostile Post nodes.
PROFILE
MATCH (src:Subreddit)-[:POSTED]->(p:Post {post_label: -1})
RETURN src.name AS subreddit, count(p) AS hostile_count
ORDER BY hostile_count DESC LIMIT 20;


// ─────────────────────────────────────────────────────────────────────────────
// TIER 3: Multi-hop Pattern Matching & Joins
// ─────────────────────────────────────────────────────────────────────────────

// T3-A: 2-hop common targets of seed_a and seed_b
// Access Pattern: Intersection of 2-hop paths using Cypher WITH clause.
PROFILE
MATCH (a:Subreddit {name: $seed_a})-[:POSTED]->(:Post)-[:REFERENCES]->(shared:Subreddit)
WITH shared
MATCH (b:Subreddit {name: $seed_b})-[:POSTED]->(:Post)-[:REFERENCES]->(shared)
RETURN shared.name AS shared_target ORDER BY shared_target LIMIT 25;

// T3-C: Common hostile attackers of seed_a and seed_b
// Access Pattern: Reverse 2-hop intersection with hostile sentiment filter.
PROFILE
MATCH (atk:Subreddit)-[:POSTED]->(:Post {post_label: -1})-[:REFERENCES]->(a:Subreddit {name: $seed_a})
WITH atk
MATCH (atk)-[:POSTED]->(:Post {post_label: -1})-[:REFERENCES]->(b:Subreddit {name: $seed_b})
RETURN atk.name AS common_attacker ORDER BY atk.name LIMIT 25;


// ─────────────────────────────────────────────────────────────────────────────
// TIER 4: Recursive & Complex Self-Joins
// ─────────────────────────────────────────────────────────────────────────────

// T4-A: Global mutual hostile pairs (A -> B and B -> A both hostile)
// Access Pattern: 4-hop cyclical pattern matching with ID symmetry breaking.
PROFILE
MATCH (a:Subreddit)-[:POSTED]->(:Post {post_label: -1})-[:REFERENCES]->(b:Subreddit)
      -[:POSTED]->(:Post {post_label: -1})-[:REFERENCES]->(a)
WHERE id(a) < id(b)
RETURN a.name AS sub_a, b.name AS sub_b, count(*) AS mutual_hostile_links
ORDER BY mutual_hostile_links DESC LIMIT 20;

// T4-B: Bounded BFS shortest path up to depth 3 from seed
// Access Pattern: Variable-length traversal (*2..6 graph edges = 1..3 subreddit hops).
PROFILE
MATCH p = (src:Subreddit {name: $seed_bfs})-[:POSTED|REFERENCES*2..6]->(tgt:Subreddit)
WHERE src <> tgt
WITH tgt, min(length(p) / 2) AS min_hops
RETURN tgt.name AS reachable, min_hops ORDER BY min_hops, tgt.name LIMIT 500;


// ─────────────────────────────────────────────────────────────────────────────
// TIER 5: Graph Traversal & Cycle Detection (Hostile Workload for Relational)
// ─────────────────────────────────────────────────────────────────────────────

// T5-A: 3-node hostile cycles seeded at seed (A -> B -> C -> A all hostile)
// Access Pattern: 6-hop directed cycle pattern match. Highly concise compared to 9 SQL joins.
PROFILE
MATCH (a:Subreddit {name: $seed})
      -[:POSTED]->(:Post {post_label: -1})-[:REFERENCES]->(b:Subreddit)
      -[:POSTED]->(:Post {post_label: -1})-[:REFERENCES]->(c:Subreddit)
      -[:POSTED]->(:Post {post_label: -1})-[:REFERENCES]->(a)
WHERE id(b) < id(c)
RETURN a.name AS node_a, b.name AS node_b, c.name AS node_c LIMIT 50;

// T5-B: Bounded BFS shortest path up to depth 4 from seed
// Access Pattern: Variable-length traversal (*2..8 graph edges = 1..4 subreddit hops).
PROFILE
MATCH p = (src:Subreddit {name: $seed_bfs})-[:POSTED|REFERENCES*2..8]->(tgt:Subreddit)
WHERE src <> tgt
WITH tgt, min(length(p) / 2) AS min_hops
RETURN tgt.name AS reachable, min_hops ORDER BY min_hops, tgt.name LIMIT 500;
