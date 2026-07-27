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
-- =============================================================================

DROP TABLE IF EXISTS hyperlinks CASCADE;
DROP TABLE IF EXISTS posts CASCADE;
DROP TABLE IF EXISTS subreddits CASCADE;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table 1: subreddits (Community entities)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE subreddits (
    id   SERIAL PRIMARY KEY,
    name TEXT   NOT NULL UNIQUE
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Table 2: posts (Content entities with linguistic & sentiment attributes)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE posts (
    id                  SERIAL       PRIMARY KEY,
    post_id             TEXT         NOT NULL UNIQUE,
    source_subreddit_id INT          NOT NULL REFERENCES subreddits(id) ON DELETE CASCADE,
    timestamp           TIMESTAMPTZ  NOT NULL,
    source_type         TEXT         NOT NULL CHECK (source_type IN ('body', 'title')),
    post_label          SMALLINT     NOT NULL CHECK (post_label IN (-1, 1)),
    post_properties     REAL[]       -- 86-dimensional feature vector; NULL if absent
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Table 3: hyperlinks (Directed relational edges from Post to Target Subreddit)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE hyperlinks (
    id                  SERIAL PRIMARY KEY,
    post_id             INT    NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    target_subreddit_id INT    NOT NULL REFERENCES subreddits(id) ON DELETE CASCADE,
    UNIQUE (post_id, target_subreddit_id)
);


-- =============================================================================
-- COVERING INDEXES FOR PERFORMANCE BENCHMARKING
-- =============================================================================

-- Subreddit lookup indexes (B-tree on name is created automatically via UNIQUE constraint)

-- Posts indexes: covering foreign key joins and sentiment/time filtering
CREATE INDEX idx_posts_source_id       ON posts (source_subreddit_id);
CREATE INDEX idx_posts_source_label    ON posts (source_subreddit_id, post_label);
CREATE INDEX idx_posts_label           ON posts (post_label);
CREATE INDEX idx_posts_timestamp       ON posts (timestamp);

-- Hyperlinks indexes: covering foreign key joins and collaborative filtering
CREATE INDEX idx_hyperlinks_post_id    ON hyperlinks (post_id);
CREATE INDEX idx_hyperlinks_target_id  ON hyperlinks (target_subreddit_id);
CREATE INDEX idx_hyperlinks_composite  ON hyperlinks (target_subreddit_id, post_id);
