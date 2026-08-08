-- =============================================================================
-- PostgreSQL Deferred Index & Constraint Creation
-- Dataset: Reddit Hyperlink Network (SNAP)
--
-- Run AFTER bulk COPY FROM STDIN to avoid incremental B-Tree maintenance
-- during row insertion (which degrades COPY throughput by 2-3x).
--
-- Load order:
--   1. schema.sql   — bare tables, no indexes, no FK constraints
--   2. COPY ...     — fast bulk load on heap-only tables
--   3. schema_indexes.sql (this file) — batch index build + FK validation
-- =============================================================================

-- Foreign Key Constraints (batch-validate after load)
ALTER TABLE posts
    ADD CONSTRAINT posts_source_subreddit_fk
    FOREIGN KEY (source_subreddit_id) REFERENCES subreddits(id) ON DELETE CASCADE;

ALTER TABLE hyperlinks
    ADD CONSTRAINT hyperlinks_post_fk
    FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE;

ALTER TABLE hyperlinks
    ADD CONSTRAINT hyperlinks_target_subreddit_fk
    FOREIGN KEY (target_subreddit_id) REFERENCES subreddits(id) ON DELETE CASCADE;

-- Unique Constraints (creates backing B-tree index automatically)
ALTER TABLE subreddits ADD CONSTRAINT subreddits_name_unique UNIQUE (name);
ALTER TABLE posts       ADD CONSTRAINT posts_post_id_unique   UNIQUE (post_id);
ALTER TABLE hyperlinks  ADD CONSTRAINT hyperlinks_unique      UNIQUE (post_id, target_subreddit_id);

-- Posts Covering Indexes
CREATE INDEX idx_posts_source_id       ON posts (source_subreddit_id);
CREATE INDEX idx_posts_source_label    ON posts (source_subreddit_id, post_label);
CREATE INDEX idx_posts_label           ON posts (post_label);
CREATE INDEX idx_posts_timestamp       ON posts (timestamp);

-- Hyperlinks Covering Indexes
CREATE INDEX idx_hyperlinks_post_id    ON hyperlinks (post_id);
CREATE INDEX idx_hyperlinks_composite  ON hyperlinks (target_subreddit_id, post_id);
