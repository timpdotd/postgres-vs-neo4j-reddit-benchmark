-- =============================================================================
-- PostgreSQL Schema (3NF Normalized)
-- Dataset: Reddit Hyperlink Network (SNAP)
-- Source:  https://snap.stanford.edu/data/soc-RedditHyperlinks.html
--
-- Conceptual Modeling Justification (FAQ Q12 & Q13 Compliance):
-- In accordance with relational database design principles and 3NF normalization,
-- we decompose the raw semi-structured TSV dataset into three distinct entities:
--
-- 1. subreddits: Represents unique community entities (nodes in social graph).
-- 2. posts: Represents user-generated content originating within a source subreddit.
--    Holds all post-intrinsic attributes: timestamp, sentiment label, body/title
--    type, and the 86-dimensional linguistic feature vector (POST_PROPERTIES).
-- 3. hyperlinks: Represents the directed reference from a specific post to a
--    target subreddit. Normalizing this separates the existence and properties of
--    a post from the linkage graph, avoiding redundancy if posts contain multiple links.
--
-- IMPORTANT: Indexes, UNIQUE constraints, and FOREIGN KEY constraints are intentionally
-- deferred to sql/schema_indexes.sql and applied AFTER bulk COPY loading.
-- This avoids incremental B-Tree maintenance overhead during data ingestion (2-3x speedup).
-- =============================================================================

DROP TABLE IF EXISTS hyperlinks CASCADE;
DROP TABLE IF EXISTS posts CASCADE;
DROP TABLE IF EXISTS subreddits CASCADE;

-- ---------------------------------------------------------------------------
-- Table 1: subreddits (Community entities)
-- ---------------------------------------------------------------------------
CREATE TABLE subreddits (
    id   SERIAL PRIMARY KEY,
    name TEXT   NOT NULL
);

-- ---------------------------------------------------------------------------
-- Table 2: posts (Content entities with linguistic & sentiment attributes)
-- FK and UNIQUE constraints are deferred to sql/schema_indexes.sql.
-- ---------------------------------------------------------------------------
CREATE TABLE posts (
    id                  SERIAL       PRIMARY KEY,
    post_id             TEXT         NOT NULL,
    source_subreddit_id INT          NOT NULL,
    timestamp           TIMESTAMPTZ  NOT NULL,
    source_type         TEXT         NOT NULL CHECK (source_type IN ('body', 'title')),
    post_label          SMALLINT     NOT NULL CHECK (post_label IN (-1, 1)),
    post_properties     REAL[]       -- 86-dimensional feature vector; NULL if absent
);

-- ---------------------------------------------------------------------------
-- Table 3: hyperlinks (Directed relational edges from Post to Target Subreddit)
-- FK and UNIQUE constraints are deferred to sql/schema_indexes.sql.
-- ---------------------------------------------------------------------------
CREATE TABLE hyperlinks (
    id                  SERIAL PRIMARY KEY,
    post_id             INT    NOT NULL,
    target_subreddit_id INT    NOT NULL
);

-- All indexes, UNIQUE constraints, and FOREIGN KEY constraints are in sql/schema_indexes.sql.
-- The ETL script (scripts/load_data.py) applies them automatically after COPY completes.
