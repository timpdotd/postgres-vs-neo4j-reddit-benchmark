-- =============================================================================
-- PostgreSQL Schema
-- Dataset: Reddit Hyperlink Network (SNAP)
-- Source:  https://snap.stanford.edu/data/soc-RedditHyperlinks.html
-- Paper:   Kumar et al., "Community Interaction and Conflict on the Web", WWW 2018
--
-- TSV columns (both body + title files share the same format):
--   SOURCE_SUBREDDIT | TARGET_SUBREDDIT | POST_ID | TIMESTAMP | POST_LABEL | POST_PROPERTIES
--
-- Design decisions:
--   • Subreddits are deduplicated into their own lookup table (55,863 unique nodes).
--   • Every row in the TSV becomes one row in `hyperlinks` — no aggregation.
--   • source_type  distinguishes which file the row came from ('body' / 'title').
--   • post_label   stores the signed edge weight exactly as in the dataset (-1 / +1).
--   • post_properties stores the 86-dimensional LIWC/readability vector as REAL[].
--     The individual dimensions are documented in the array below.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- TABLE: subreddits
-- One row per unique community name (node table).
-- Names are stored lowercase to match the TSV data.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS subreddits (
    id   SERIAL PRIMARY KEY,
    name TEXT   NOT NULL,
    CONSTRAINT uq_subreddit_name UNIQUE (name)
);


-- -----------------------------------------------------------------------------
-- TABLE: hyperlinks
-- One row per inter-subreddit hyperlink found in the SNAP TSV files.
--
-- Column notes:
--   post_id         — the Reddit post ID in the SOURCE subreddit that contains
--                     the link (e.g. "1u4nrps"). Not globally unique across both
--                     files; the combination (post_id, source_type) is unique.
--   timestamp       — UTC timestamp of the source post.
--   source_type     — which SNAP file this row came from:
--                       'body'  → soc-redditHyperlinks-body.tsv
--                       'title' → soc-redditHyperlinks-title.tsv
--   post_label      — signed edge weight assigned by crowd-sourcing + classifier:
--                       +1  neutral or positive sentiment toward target
--                       -1  explicitly hostile / negative toward target
--   post_properties — 86-element LIWC / readability feature vector of the
--                     source post text. See dimension legend below.
--                     NULL when the TSV row has no PROPERTIES field.
--
-- 86-dimension POST_PROPERTIES legend (1-indexed):
--   [1]  num_chars              Number of characters
--   [2]  num_chars_no_ws        Number of characters (no whitespace)
--   [3]  frac_alpha             Fraction of alphabetical characters
--   [4]  frac_digits            Fraction of digits
--   [5]  frac_upper             Fraction of uppercase characters
--   [6]  frac_whitespace        Fraction of whitespace characters
--   [7]  frac_special           Fraction of special characters (,!? etc.)
--   [8]  num_words              Number of words
--   [9]  num_unique_words       Number of unique words
--   [10] num_long_words         Number of long words (≥6 chars)
--   [11] avg_word_length        Average word length
--   [12] num_unique_stopwords   Number of unique stopwords
--   [13] frac_stopwords         Fraction of stopwords
--   [14] num_sentences          Number of sentences
--   [15] num_long_sentences     Number of long sentences (≥10 words)
--   [16] avg_chars_per_sentence Average characters per sentence
--   [17] avg_words_per_sentence Average words per sentence
--   [18] readability_ari        Automated Readability Index
--   [19] vader_positive         Positive sentiment (VADER)
--   [20] vader_negative         Negative sentiment (VADER)
--   [21] vader_compound         Compound sentiment (VADER)
--   [22] liwc_funct             LIWC: Function words
--   [23] liwc_pronoun           LIWC: Pronouns
--   [24] liwc_ppron             LIWC: Personal pronouns
--   [25] liwc_i                 LIWC: First person singular
--   [26] liwc_we                LIWC: First person plural
--   [27] liwc_you               LIWC: Second person
--   [28] liwc_shehe             LIWC: Third person singular
--   [29] liwc_they              LIWC: Third person plural
--   [30] liwc_ipron             LIWC: Impersonal pronouns
--   [31] liwc_article           LIWC: Articles
--   [32] liwc_verbs             LIWC: Verbs
--   [33] liwc_auxvb             LIWC: Auxiliary verbs
--   [34] liwc_past              LIWC: Past tense
--   [35] liwc_present           LIWC: Present tense
--   [36] liwc_future            LIWC: Future tense
--   [37] liwc_adverbs           LIWC: Adverbs
--   [38] liwc_prep              LIWC: Prepositions
--   [39] liwc_conj              LIWC: Conjunctions
--   [40] liwc_negate            LIWC: Negations
--   [41] liwc_quant             LIWC: Quantifiers
--   [42] liwc_numbers           LIWC: Numbers
--   [43] liwc_swear             LIWC: Swear words
--   [44] liwc_social            LIWC: Social processes
--   [45] liwc_family            LIWC: Family
--   [46] liwc_friends           LIWC: Friends
--   [47] liwc_humans            LIWC: Humans
--   [48] liwc_affect            LIWC: Affective processes
--   [49] liwc_posemo            LIWC: Positive emotions
--   [50] liwc_negemo            LIWC: Negative emotions
--   [51] liwc_anx               LIWC: Anxiety
--   [52] liwc_anger             LIWC: Anger
--   [53] liwc_sad               LIWC: Sadness
--   [54] liwc_cogmech           LIWC: Cognitive mechanisms
--   [55] liwc_insight           LIWC: Insight
--   [56] liwc_cause             LIWC: Causation
--   [57] liwc_discrep           LIWC: Discrepancy
--   [58] liwc_tentat            LIWC: Tentative
--   [59] liwc_certain           LIWC: Certainty
--   [60] liwc_inhib             LIWC: Inhibition
--   [61] liwc_incl              LIWC: Inclusion
--   [62] liwc_excl              LIWC: Exclusion
--   [63] liwc_percept           LIWC: Perceptual processes
--   [64] liwc_see               LIWC: Seeing
--   [65] liwc_hear              LIWC: Hearing
--   [66] liwc_feel              LIWC: Feeling
--   [67] liwc_bio               LIWC: Biological processes
--   [68] liwc_body              LIWC: Body
--   [69] liwc_health            LIWC: Health
--   [70] liwc_sexual            LIWC: Sexuality
--   [71] liwc_ingest            LIWC: Ingestion
--   [72] liwc_relativ           LIWC: Relativity
--   [73] liwc_motion            LIWC: Motion
--   [74] liwc_space             LIWC: Space
--   [75] liwc_time              LIWC: Time
--   [76] liwc_work              LIWC: Work
--   [77] liwc_achiev            LIWC: Achievement
--   [78] liwc_leisure           LIWC: Leisure
--   [79] liwc_home              LIWC: Home
--   [80] liwc_money             LIWC: Money
--   [81] liwc_relig             LIWC: Religion
--   [82] liwc_death             LIWC: Death
--   [83] liwc_assent            LIWC: Assent
--   [84] liwc_dissent           LIWC: Dissent
--   [85] liwc_nonflu            LIWC: Nonfluencies
--   [86] liwc_filler            LIWC: Filler words
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hyperlinks (
    id                  BIGSERIAL    PRIMARY KEY,
    source_subreddit_id INT          NOT NULL REFERENCES subreddits(id),
    target_subreddit_id INT          NOT NULL REFERENCES subreddits(id),
    post_id             TEXT         NOT NULL,
    timestamp           TIMESTAMPTZ  NOT NULL,
    source_type         TEXT         NOT NULL CHECK (source_type IN ('body', 'title')),
    post_label          SMALLINT     NOT NULL CHECK (post_label IN (-1, 1)),
    post_properties     REAL[]       -- 86-dim vector; NULL when absent in TSV
);


-- =============================================================================
-- INDEXES
-- Chosen to cover the five benchmark query patterns (Q1–Q5).
-- =============================================================================

-- Q1 — Point lookup: all outgoing edges from a given source subreddit
CREATE INDEX IF NOT EXISTS idx_hl_source
    ON hyperlinks (source_subreddit_id);

-- Q1 — Same lookup filtered by post_label (-1 hostile links)
CREATE INDEX IF NOT EXISTS idx_hl_source_label
    ON hyperlinks (source_subreddit_id, post_label);

-- Q2 — Collaborative filtering: lookups by target subreddit
CREATE INDEX IF NOT EXISTS idx_hl_target
    ON hyperlinks (target_subreddit_id);

-- Q3 / Q5 — Mutual / cycle detection: composite for fast bidirectional joins
CREATE INDEX IF NOT EXISTS idx_hl_src_tgt_label
    ON hyperlinks (source_subreddit_id, target_subreddit_id, post_label);

-- Temporal range queries and ordering
CREATE INDEX IF NOT EXISTS idx_hl_timestamp
    ON hyperlinks (timestamp);

-- Optional: distinguish body vs title edges
CREATE INDEX IF NOT EXISTS idx_hl_source_type
    ON hyperlinks (source_type);
