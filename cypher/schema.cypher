// =============================================================================
// Neo4j Property Graph Schema (Multi-Node / Multi-Relationship Model)
// Dataset: Reddit Hyperlink Network (SNAP)
// Source:  https://snap.stanford.edu/data/soc-RedditHyperlinks.html
//
// Conceptual Modeling Justification (FAQ Q12 Compliance):
// To avoid trivial modeling (e.g., a single node type and relationship type),
// we model the domain using an expressive Property Graph schema that mirrors
// the true semantic structure of Reddit interactions:
//
// Nodes:
//   (:Subreddit {name: STRING [UNIQUE]})
//   (:Post {post_id: STRING [UNIQUE], timestamp: DATETIME, source_type: STRING,
//           post_label: INTEGER, post_properties: LIST<FLOAT>})
//
// Relationships:
//   (:Subreddit)-[:POSTED]->(:Post)
//   (:Post)-[:REFERENCES]->(:Subreddit)
//
// A hyperlink traversal from source community A to target community B is thus
// modeled as a semantic 2-hop path:
//   (a:Subreddit)-[:POSTED]->(p:Post)-[:REFERENCES]->(b:Subreddit)
// This attaches linguistic properties and sentiment directly to the content
// entity (:Post), enabling rich graph pattern matching and analytics.
// =============================================================================

// ─────────────────────────────────────────────────────────────────────────────
// CONSTRAINTS & UNIQUENESS
// ─────────────────────────────────────────────────────────────────────────────

// Enforce subreddit name uniqueness (creates backing B-tree index for fast MATCH)
CREATE CONSTRAINT subreddit_name_unique IF NOT EXISTS
    FOR (s:Subreddit)
    REQUIRE s.name IS UNIQUE;

// Enforce post_id uniqueness across the network
CREATE CONSTRAINT post_id_unique IF NOT EXISTS
    FOR (p:Post)
    REQUIRE p.post_id IS UNIQUE;


// ─────────────────────────────────────────────────────────────────────────────
// NODE PROPERTY INDEXES FOR BENCHMARK QUERIES
// ─────────────────────────────────────────────────────────────────────────────

// Index on post sentiment label (-1 hostile vs +1 neutral/positive)
CREATE INDEX post_idx_label IF NOT EXISTS
    FOR (p:Post)
    ON (p.post_label);

// Compound index on sentiment label and source file type (body vs title)
CREATE INDEX post_idx_label_type IF NOT EXISTS
    FOR (p:Post)
    ON (p.post_label, p.source_type);

// Index on post timestamp for temporal slicing and ordering
CREATE INDEX post_idx_timestamp IF NOT EXISTS
    FOR (p:Post)
    ON (p.timestamp);


// =============================================================================
// 86-DIMENSION POST_PROPERTIES LEGEND (0-indexed in Neo4j lists)
// =============================================================================
// [0]  num_chars              [29] liwc_ipron             [58] liwc_certain
// [1]  num_chars_no_ws        [30] liwc_article           [59] liwc_inhib
// [2]  frac_alpha             [31] liwc_verbs             [60] liwc_incl
// [3]  frac_digits            [32] liwc_auxvb             [61] liwc_excl
// [4]  frac_upper             [33] liwc_past              [62] liwc_percept
// [5]  frac_whitespace        [34] liwc_present           [63] liwc_see
// [6]  frac_special           [35] liwc_future            [64] liwc_hear
// [7]  num_words              [36] liwc_adverbs           [65] liwc_feel
// [8]  num_unique_words       [37] liwc_prep              [66] liwc_bio
// [9]  num_long_words         [38] liwc_conj              [67] liwc_body
// [10] avg_word_length        [39] liwc_negate            [68] liwc_health
// [11] num_unique_stopwords   [40] liwc_quant             [69] liwc_sexual
// [12] frac_stopwords         [41] liwc_numbers           [70] liwc_ingest
// [13] num_sentences          [42] liwc_swear             [71] liwc_relativ
// [14] num_long_sentences     [43] liwc_social            [72] liwc_motion
// [15] avg_chars_per_sentence [44] liwc_family            [73] liwc_space
// [16] avg_words_per_sentence [45] liwc_friends           [74] liwc_time
// [17] readability_ari        [46] liwc_humans            [75] liwc_work
// [18] vader_positive         [47] liwc_affect            [76] liwc_achiev
// [19] vader_negative         [48] liwc_posemo            [77] liwc_leisure
// [20] vader_compound         [49] liwc_negemo            [78] liwc_home
// [21] liwc_funct             [50] liwc_anx               [79] liwc_money
// [22] liwc_pronoun           [51] liwc_anger             [80] liwc_relig
// [23] liwc_ppron             [52] liwc_sad               [81] liwc_death
// [24] liwc_i                 [53] liwc_cogmech           [82] liwc_assent
// [25] liwc_we                [54] liwc_insight           [83] liwc_dissent
// [26] liwc_you               [55] liwc_cause             [84] liwc_nonflu
// [27] liwc_shehe             [56] liwc_discrep           [85] liwc_filler
// [28] liwc_they              [57] liwc_tentat
// =============================================================================
