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

RESET ROLE;
