-- Normalized social events, produced by Module 2 (collectors) and stored by Module 4 (db-writer).
-- This file is used twice, so the two paths cannot drift apart:
--   1. docker-entrypoint-initdb.d on the first start of the PostgreSQL volume (docker-compose.shared.yml)
--   2. db-writer on startup, which makes an older volume pick up new statements
--
-- Run as the database owner (kol_admin) or as kol_app itself; SET ROLE makes the tables belong to
-- kol_app either way, so kol_readonly gets SELECT through the default privileges in 01-roles.sh.
SET ROLE kol_app;

CREATE TABLE IF NOT EXISTS kol.social_events (
    event_id          uuid        PRIMARY KEY,
    platform          text        NOT NULL,
    event_type        text        NOT NULL,
    content_id        text        NOT NULL,
    parent_id         text,
    kol_id            text        NOT NULL,
    author_id         text        NOT NULL,
    author_handle     text        NOT NULL DEFAULT '',
    author_name       text        NOT NULL DEFAULT '',
    author_followers  integer,
    text              text        NOT NULL DEFAULT '',
    hashtags          text[]      NOT NULL DEFAULT '{}',
    mentions          text[]      NOT NULL DEFAULT '{}',
    tags              text[]      NOT NULL DEFAULT '{}',
    metrics           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    lang              text        NOT NULL DEFAULT '',
    default_language  text        NOT NULL DEFAULT '',
    category_id       text        NOT NULL DEFAULT '',
    root_id           text,
    cid               text        NOT NULL DEFAULT '',
    rev               text        NOT NULL DEFAULT '',
    links             text[]      NOT NULL DEFAULT '{}',
    facets            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    media             jsonb       NOT NULL DEFAULT '[]'::jsonb,
    topics            text[]      NOT NULL DEFAULT '{}',
    content_details   jsonb       NOT NULL DEFAULT '{}'::jsonb,
    live_viewers      integer,
    author_videos     integer,
    author_total_views bigint,
    created_at        timestamptz NOT NULL,
    ingested_at       timestamptz NOT NULL,
    collector         text        NOT NULL DEFAULT '',
    collector_version text        NOT NULL DEFAULT '',
    stored_at         timestamptz NOT NULL DEFAULT now()
);

-- CREATE TABLE IF NOT EXISTS does not add columns to a volume that already exists, so every newer
-- column is also declared here. Re-running the writer on an old database is therefore safe.
ALTER TABLE kol.social_events
    ADD COLUMN IF NOT EXISTS root_id            text,
    ADD COLUMN IF NOT EXISTS cid                text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS rev                text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS links              text[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS facets             jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS media              jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS topics             text[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS content_details    jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS live_viewers       integer,
    ADD COLUMN IF NOT EXISTS author_videos      integer,
    ADD COLUMN IF NOT EXISTS author_total_views bigint;

-- Per-KOL timeline (dashboard, score computation).
CREATE INDEX IF NOT EXISTS social_events_kol_created_idx
    ON kol.social_events (kol_id, created_at DESC);

-- Per-platform recency scans.
CREATE INDEX IF NOT EXISTS social_events_platform_created_idx
    ON kol.social_events (platform, created_at DESC);

-- Resolve likes/reposts back to the post they refer to.
CREATE INDEX IF NOT EXISTS social_events_content_idx
    ON kol.social_events (platform, content_id);

-- Hashtag / tag trends.
CREATE INDEX IF NOT EXISTS social_events_hashtags_idx
    ON kol.social_events USING GIN (hashtags);
CREATE INDEX IF NOT EXISTS social_events_tags_idx
    ON kol.social_events USING GIN (tags);
CREATE INDEX IF NOT EXISTS social_events_topics_idx
    ON kol.social_events USING GIN (topics);

-- Rebuild a Bluesky thread from its root.
CREATE INDEX IF NOT EXISTS social_events_root_idx
    ON kol.social_events (root_id) WHERE root_id IS NOT NULL;

-- ---------------------------------------------------------------------------------------------
-- Results of Module 3 (stream-processor), stored by metrics_writer.py from kol.metrics.*.
-- kol_scores is also where Module 3 restores a KOL's cumulative totals after a restart.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kol.kol_scores (
    kol_id            text             PRIMARY KEY,
    platform          text             NOT NULL,
    handle            text,
    followers         integer,
    total_events      integer          NOT NULL DEFAULT 0,
    window_events     integer          NOT NULL DEFAULT 0,
    window_minutes    integer          NOT NULL DEFAULT 15,
    event_breakdown   jsonb            NOT NULL DEFAULT '{}'::jsonb,
    metrics_total     jsonb            NOT NULL DEFAULT '{}'::jsonb,
    metrics_window    jsonb            NOT NULL DEFAULT '{}'::jsonb,
    engagement_score  double precision NOT NULL DEFAULT 0,
    window_score      double precision NOT NULL DEFAULT 0,
    velocity_per_min  double precision NOT NULL DEFAULT 0,
    top_hashtags      text[]           NOT NULL DEFAULT '{}',
    last_event_at     timestamptz,
    updated_at        timestamptz      NOT NULL DEFAULT now()
);

-- Leaderboards: all-time and trending.
CREATE INDEX IF NOT EXISTS kol_scores_engagement_idx ON kol.kol_scores (engagement_score DESC);
CREATE INDEX IF NOT EXISTS kol_scores_window_idx ON kol.kol_scores (window_score DESC);

CREATE TABLE IF NOT EXISTS kol.keyword_metrics (
    keyword           text             PRIMARY KEY,
    is_hashtag        boolean          NOT NULL DEFAULT false,
    count             integer          NOT NULL DEFAULT 0,
    total_count       integer          NOT NULL DEFAULT 0,
    window_minutes    integer          NOT NULL DEFAULT 15,
    velocity_per_min  double precision NOT NULL DEFAULT 0,
    platforms         jsonb            NOT NULL DEFAULT '{}'::jsonb,
    unique_kols       integer          NOT NULL DEFAULT 0,
    updated_at        timestamptz      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS keyword_metrics_count_idx ON kol.keyword_metrics (count DESC);

-- One row per KOL per minute, for score-over-time charts. Pruned by metrics_writer.py.
CREATE TABLE IF NOT EXISTS kol.kol_score_history (
    kol_id            text             NOT NULL,
    bucket            timestamptz      NOT NULL,
    engagement_score  double precision NOT NULL,
    window_score      double precision NOT NULL,
    velocity_per_min  double precision NOT NULL,
    total_events      integer          NOT NULL,
    PRIMARY KEY (kol_id, bucket)
);

CREATE INDEX IF NOT EXISTS kol_score_history_bucket_idx ON kol.kol_score_history (bucket);

RESET ROLE;
