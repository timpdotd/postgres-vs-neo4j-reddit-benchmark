// =============================================================================
// Neo4j Schema
// Dataset: Reddit Hyperlink Network (SNAP)
// Source:  https://snap.stanford.edu/data/soc-RedditHyperlinks.html
// Paper:   Kumar et al., "Community Interaction and Conflict on the Web", WWW 2018
//
// Graph model:
//   (:Subreddit)-[:HYPERLINKS_TO {props}]->(:Subreddit)
//
// One (:Subreddit) node per unique community (≈55,863 nodes).
// One [:HYPERLINKS_TO] relationship per TSV row (≈858,490 edges total across
// both files). The relationship carries all edge attributes from the dataset.
//
// TSV columns mapped to relationship properties:
//   POST_ID         → post_id        : STRING
//   TIMESTAMP       → timestamp      : DATETIME  (UTC)
//   source_type     → source_type    : STRING    ('body' | 'title')
//   POST_LABEL      → post_label     : INTEGER   (-1 hostile | +1 neutral/positive)
//   POST_PROPERTIES → post_properties: LIST<FLOAT>  (86 dimensions, see legend below)
// =============================================================================


// =============================================================================
// CONSTRAINTS
// =============================================================================

// Enforce uniqueness on subreddit name — also creates the backing B-tree index
// used for all MATCH lookups by name.
CREATE CONSTRAINT subreddit_name_unique IF NOT EXISTS
    FOR (s:Subreddit)
    REQUIRE s.name IS UNIQUE;


// =============================================================================
// RELATIONSHIP PROPERTY INDEXES
// Chosen to cover the five benchmark query patterns (Q1–Q5).
// Note: relationship indexes require Neo4j 5.x (community edition supported).
// =============================================================================

// Q1 — Filter edges by post_label (hostile: -1 / neutral: +1)
CREATE INDEX rel_idx_post_label IF NOT EXISTS
    FOR ()-[r:HYPERLINKS_TO]-()
    ON (r.post_label);

// Q1 / Q3 — Compound filter: label + source file type
CREATE INDEX rel_idx_label_source_type IF NOT EXISTS
    FOR ()-[r:HYPERLINKS_TO]-()
    ON (r.post_label, r.source_type);

// Temporal range queries and chronological ordering
CREATE INDEX rel_idx_timestamp IF NOT EXISTS
    FOR ()-[r:HYPERLINKS_TO]-()
    ON (r.timestamp);


// =============================================================================
// GRAPH MODEL REFERENCE
// (Schema is implicit in Neo4j — this section is documentation only)
// =============================================================================
//
// ── NODE ─────────────────────────────────────────────────────────────────────
//
//   (:Subreddit)
//     name : STRING  — subreddit name, lowercase (UNIQUE, indexed)
//
// ── RELATIONSHIP ─────────────────────────────────────────────────────────────
//
//   [:HYPERLINKS_TO]  directed: (source_subreddit)-[:HYPERLINKS_TO]->(target_subreddit)
//
//     post_id         : STRING        — Reddit post ID in the source subreddit
//     timestamp       : DATETIME      — UTC datetime of the source post
//     source_type     : STRING        — 'body' or 'title' (which TSV file)
//     post_label      : INTEGER       — edge sign: +1 neutral/positive, -1 hostile
//     post_properties : LIST<FLOAT>   — 86-dim feature vector (may be null)
//
//   86-dimension POST_PROPERTIES legend (0-indexed in Neo4j lists):
//     [0]  num_chars              Number of characters
//     [1]  num_chars_no_ws        Number of characters (no whitespace)
//     [2]  frac_alpha             Fraction of alphabetical characters
//     [3]  frac_digits            Fraction of digits
//     [4]  frac_upper             Fraction of uppercase characters
//     [5]  frac_whitespace        Fraction of whitespace characters
//     [6]  frac_special           Fraction of special characters (,!? etc.)
//     [7]  num_words              Number of words
//     [8]  num_unique_words       Number of unique words
//     [9]  num_long_words         Number of long words (≥6 chars)
//     [10] avg_word_length        Average word length
//     [11] num_unique_stopwords   Number of unique stopwords
//     [12] frac_stopwords         Fraction of stopwords
//     [13] num_sentences          Number of sentences
//     [14] num_long_sentences     Number of long sentences (≥10 words)
//     [15] avg_chars_per_sentence Average characters per sentence
//     [16] avg_words_per_sentence Average words per sentence
//     [17] readability_ari        Automated Readability Index
//     [18] vader_positive         Positive sentiment (VADER)
//     [19] vader_negative         Negative sentiment (VADER)
//     [20] vader_compound         Compound sentiment (VADER)
//     [21] liwc_funct             LIWC: Function words
//     [22] liwc_pronoun           LIWC: Pronouns
//     [23] liwc_ppron             LIWC: Personal pronouns
//     [24] liwc_i                 LIWC: First person singular
//     [25] liwc_we                LIWC: First person plural
//     [26] liwc_you               LIWC: Second person
//     [27] liwc_shehe             LIWC: Third person singular
//     [28] liwc_they              LIWC: Third person plural
//     [29] liwc_ipron             LIWC: Impersonal pronouns
//     [30] liwc_article           LIWC: Articles
//     [31] liwc_verbs             LIWC: Verbs
//     [32] liwc_auxvb             LIWC: Auxiliary verbs
//     [33] liwc_past              LIWC: Past tense
//     [34] liwc_present           LIWC: Present tense
//     [35] liwc_future            LIWC: Future tense
//     [36] liwc_adverbs           LIWC: Adverbs
//     [37] liwc_prep              LIWC: Prepositions
//     [38] liwc_conj              LIWC: Conjunctions
//     [39] liwc_negate            LIWC: Negations
//     [40] liwc_quant             LIWC: Quantifiers
//     [41] liwc_numbers           LIWC: Numbers
//     [42] liwc_swear             LIWC: Swear words
//     [43] liwc_social            LIWC: Social processes
//     [44] liwc_family            LIWC: Family
//     [45] liwc_friends           LIWC: Friends
//     [46] liwc_humans            LIWC: Humans
//     [47] liwc_affect            LIWC: Affective processes
//     [48] liwc_posemo            LIWC: Positive emotions
//     [49] liwc_negemo            LIWC: Negative emotions
//     [50] liwc_anx               LIWC: Anxiety
//     [51] liwc_anger             LIWC: Anger
//     [52] liwc_sad               LIWC: Sadness
//     [53] liwc_cogmech           LIWC: Cognitive mechanisms
//     [54] liwc_insight           LIWC: Insight
//     [55] liwc_cause             LIWC: Causation
//     [56] liwc_discrep           LIWC: Discrepancy
//     [57] liwc_tentat            LIWC: Tentative
//     [58] liwc_certain           LIWC: Certainty
//     [59] liwc_inhib             LIWC: Inhibition
//     [60] liwc_incl              LIWC: Inclusion
//     [61] liwc_excl              LIWC: Exclusion
//     [62] liwc_percept           LIWC: Perceptual processes
//     [63] liwc_see               LIWC: Seeing
//     [64] liwc_hear              LIWC: Hearing
//     [65] liwc_feel              LIWC: Feeling
//     [66] liwc_bio               LIWC: Biological processes
//     [67] liwc_body              LIWC: Body
//     [68] liwc_health            LIWC: Health
//     [69] liwc_sexual            LIWC: Sexuality
//     [70] liwc_ingest            LIWC: Ingestion
//     [71] liwc_relativ           LIWC: Relativity
//     [72] liwc_motion            LIWC: Motion
//     [73] liwc_space             LIWC: Space
//     [74] liwc_time              LIWC: Time
//     [75] liwc_work              LIWC: Work
//     [76] liwc_achiev            LIWC: Achievement
//     [77] liwc_leisure           LIWC: Leisure
//     [78] liwc_home              LIWC: Home
//     [79] liwc_money             LIWC: Money
//     [80] liwc_relig             LIWC: Religion
//     [81] liwc_death             LIWC: Death
//     [82] liwc_assent            LIWC: Assent
//     [83] liwc_dissent           LIWC: Dissent
//     [84] liwc_nonflu            LIWC: Nonfluencies
//     [85] liwc_filler            LIWC: Filler words
//
// ── EXAMPLE PATH ─────────────────────────────────────────────────────────────
//
//   (:Subreddit {name: 'theredlion'})
//     -[:HYPERLINKS_TO {post_label: -1, source_type: 'body', timestamp: datetime('2013-12-31T18:18:37Z')}]->
//   (:Subreddit {name: 'soccer'})
