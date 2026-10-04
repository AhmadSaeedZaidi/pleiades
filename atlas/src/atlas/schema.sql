CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS channels (
    id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    country VARCHAR(10),
    custom_url VARCHAR(100),
    created_at TIMESTAMPTZ,
    is_verified BOOLEAN DEFAULT FALSE,
    last_scraped_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS channel_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id VARCHAR(50) REFERENCES channels(id) ON DELETE CASCADE,
    changed_at TIMESTAMPTZ DEFAULT NOW(),
    old_title VARCHAR(255),
    new_title VARCHAR(255),
    event_type VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS channel_stats_log (
    channel_id VARCHAR(50) REFERENCES channels(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    view_count BIGINT,
    subscriber_count BIGINT,
    video_count INTEGER,
    PRIMARY KEY (channel_id, timestamp)
);

CREATE TABLE IF NOT EXISTS videos (
    id VARCHAR(20) PRIMARY KEY,
    channel_id VARCHAR(50) REFERENCES channels(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    published_at TIMESTAMPTZ,
    duration INTEGER,
    tags TEXT[],
    category_id VARCHAR(10),
    default_language VARCHAR(10),
    wiki_topics TEXT[],
    discovered_at TIMESTAMPTZ DEFAULT NOW(),
    last_updated_at TIMESTAMPTZ,
    archived_at TIMESTAMPTZ,
    status VARCHAR(20) DEFAULT 'PENDING',
    has_transcript BOOLEAN DEFAULT FALSE,
    has_visuals BOOLEAN DEFAULT FALSE,
    -- Extracted audio has been stored to the vault by the Singer consumer at
    -- `audio/{id}.opus`. The Scribe uses this flag only for local audio-STT
    -- fallback; caption extraction remains independent.
    has_audio BOOLEAN DEFAULT FALSE,
    -- The YouTube source media has been fetched by the streamer (network pull)
    -- and stored to the vault as a raw artifact at `raw_uri`. The singer
    -- consumer later extracts the speech track locally (no YouTube rate limit)
    -- and flips `has_audio`. Decoupling the network fetch from the local
    -- extraction lets the egress-IP-flagged VPS avoid repeated YouTube pulls.
    fetched BOOLEAN DEFAULT FALSE,
    -- Vault path of the raw fetched artifact (e.g. `raw/{prefix}/{id}.<ext>`).
    raw_uri VARCHAR(255),
    -- Timestamp the raw artifact was stored via mark_fetched. Drives the raw
    -- TTL reclamation window so the muralist (clip consumer) has a bounded
    -- chance to derive from it before it is reclaimed.
    raw_stored_at TIMESTAMPTZ,
    -- The full source video has been archived to the vault by the muralist
    -- consumer at `videos/{id}.mp4`. Marked once the full clip is stored.
    has_video BOOLEAN DEFAULT FALSE,
    -- Staging for the janitor-owned transcript vault write: the Scribe stages
    -- transcript content in `transcripts`; the janitor flushes it in batched
    -- commits. `vault_write_pending` is the work queue. `audio_pending` is
    -- retained only as a legacy migration column and is no longer populated.
    vault_write_pending BOOLEAN DEFAULT FALSE,
    audio_pending BYTEA
);

-- Idempotent migration for existing deployments: the `has_audio` and
-- `has_video` columns were added after the `videos` table first shipped.
-- `ADD COLUMN IF NOT EXISTS` is a no-op on a fresh database where the
-- CREATE TABLE above already has them.
ALTER TABLE videos ADD COLUMN IF NOT EXISTS has_audio BOOLEAN DEFAULT FALSE;
ALTER TABLE videos ADD COLUMN IF NOT EXISTS has_video BOOLEAN DEFAULT FALSE;
-- `fetched` / `raw_uri` added when the streamer/singer pipeline was split into
-- a network-fetch (streamer) + local-extract (singer) pair.
ALTER TABLE videos ADD COLUMN IF NOT EXISTS fetched BOOLEAN DEFAULT FALSE;
ALTER TABLE videos ADD COLUMN IF NOT EXISTS raw_uri VARCHAR(255);
-- `raw_stored_at` added to bound raw-artifact retention (muralist TTL window).
ALTER TABLE videos ADD COLUMN IF NOT EXISTS raw_stored_at TIMESTAMPTZ;
-- `has_captions` / `captions_uri` were added when the streamer fetched captions
-- alongside the raw media. Caption ownership has since moved entirely to the
-- Scribe (single `timedtext` throttle surface, with an audio-STT fallback), so
-- the streamer no longer stores captions and these columns are dead. Dropped.
ALTER TABLE videos DROP COLUMN IF EXISTS has_captions;
ALTER TABLE videos DROP COLUMN IF EXISTS captions_uri;

-- P2 migration: separate the tracker's cooldown timestamp from last_updated_at,
-- which was overloaded (pipeline agents + tracker both wrote to it). The tracker
-- now writes to last_tracked_at, while pipeline agents continue writing to
-- last_updated_at.
ALTER TABLE videos ADD COLUMN IF NOT EXISTS last_tracked_at TIMESTAMPTZ;

-- Cheap lookup of videos awaiting a vault flush (janitor work queue).
CREATE INDEX IF NOT EXISTS idx_videos_vault_pending
    ON videos (id) WHERE has_transcript AND vault_write_pending;
CREATE INDEX IF NOT EXISTS idx_videos_vault_flush
    ON videos (discovered_at, id) WHERE has_transcript AND vault_write_pending;

COMMENT ON COLUMN videos.status IS
'Lifecycle state machine: PENDING → PROCESSING → PROCESSED → ARCHIVED | FAILED';

CREATE TABLE IF NOT EXISTS video_stats_log (
    video_id VARCHAR(20) REFERENCES videos(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    views BIGINT,
    likes BIGINT,
    comment_count BIGINT,
    PRIMARY KEY (video_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_stats_retention
    ON video_stats_log (timestamp, video_id);

CREATE TABLE IF NOT EXISTS system_events (
    id UUID DEFAULT gen_random_uuid(),
    event_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(50),
    payload JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, created_at)
);

CREATE TABLE IF NOT EXISTS search_queue (
    id SERIAL PRIMARY KEY,
    query_term TEXT UNIQUE NOT NULL,
    priority INTEGER DEFAULT 0,
    mention_count INTEGER DEFAULT 0,
    next_page_token TEXT,
    last_searched_at TIMESTAMPTZ,
    result_count_total INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active',
    -- When the term entered the queue; drives time-decay scoring (Phase 2).
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transcripts (
    video_id VARCHAR(20) PRIMARY KEY REFERENCES videos(id) ON DELETE CASCADE,
    language VARCHAR(10) DEFAULT 'en',
    -- vault_uri is NULL while the transcript is staged locally (Option A);
    -- the janitor fills it in once it flushes the content to the vault.
    vault_uri TEXT,
    -- Staged transcript content (Option A): the scribe writes the segments here
    -- so the janitor can flush them to the vault; NULLed after a successful
    -- vault write. Keeps the persistence path decoupled from extraction.
    content JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transcripts_unvaulted
    ON transcripts (video_id) WHERE vault_uri IS NULL AND content IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_transcripts_retained_payload
    ON transcripts (video_id) WHERE vault_uri IS NOT NULL AND content IS NOT NULL;

CREATE TABLE IF NOT EXISTS watchlist (
    video_id VARCHAR(20) PRIMARY KEY,
    tracking_tier VARCHAR(20) NOT NULL DEFAULT 'HOURLY'
        CHECK (tracking_tier IN ('HOURLY', 'DAILY', 'WEEKLY', 'DORMANT')),
    last_tracked_at TIMESTAMPTZ,
    next_track_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_views BIGINT CHECK (last_views IS NULL OR last_views >= 0),
    last_likes BIGINT CHECK (last_likes IS NULL OR last_likes >= 0),
    last_comment_count BIGINT CHECK (
        last_comment_count IS NULL OR last_comment_count >= 0
    ),
    unavailable_count INTEGER NOT NULL DEFAULT 0 CHECK (unavailable_count >= 0),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE watchlist IS
'Adaptive Scheduling: durable last-sample state and persistent cadence.
Available videos remain eligible indefinitely; DORMANT videos are rechecked monthly.';

SELECT create_hypertable('channel_stats_log', 'timestamp',
    if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('video_stats_log', 'timestamp',
    if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('system_events', 'created_at',
    if_not_exists => TRUE, migrate_data => TRUE);

CREATE INDEX IF NOT EXISTS idx_channel_scrape ON channels(last_scraped_at ASC);
CREATE INDEX IF NOT EXISTS idx_channel_history_channel ON channel_history(channel_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_video_publish ON videos(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_video_tags ON videos USING GIN(tags);
CREATE INDEX IF NOT EXISTS idx_video_category ON videos(category_id);
CREATE INDEX IF NOT EXISTS idx_video_tracker_tracked_at ON videos(last_tracked_at ASC NULLS FIRST);
CREATE INDEX IF NOT EXISTS idx_video_status ON videos(status, discovered_at);
CREATE INDEX IF NOT EXISTS idx_video_channel ON videos(channel_id);
CREATE INDEX IF NOT EXISTS idx_search_queue_fetch ON search_queue(priority DESC, mention_count DESC);
CREATE INDEX IF NOT EXISTS idx_watchlist_next_track ON watchlist(next_track_at ASC);
CREATE INDEX IF NOT EXISTS idx_watchlist_tier ON watchlist(tracking_tier, next_track_at ASC);
CREATE INDEX IF NOT EXISTS idx_events_type ON system_events(event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_entity ON system_events(entity_id, created_at DESC);

-- Partial indexes for the hottest claim/sweep paths. These cover only the
-- rows the agent fleet actually scans, keeping the index small and the
-- planner's selectivity high (PostgreSQL Engineering: partial indexes).
-- This file provisions fresh databases. On an existing production database,
-- build the v2 Streamer/Singer indexes with the standalone concurrent script
-- at deploy/sql/20260902_claim_indexes_v2.sql; do not re-run this file for rollout.
CREATE INDEX IF NOT EXISTS idx_video_scribe_claim ON videos(discovered_at ASC)
    WHERE status IN ('PENDING', 'PROCESSING') AND has_transcript = FALSE;
CREATE INDEX IF NOT EXISTS idx_video_painter_claim ON videos(discovered_at ASC)
    WHERE status IN ('PENDING', 'PROCESSING') AND has_visuals = FALSE;
CREATE INDEX IF NOT EXISTS idx_video_streamer_claim_v2 ON videos(discovered_at DESC)
    WHERE status IN ('PENDING', 'PROCESSING') AND fetched = FALSE;
CREATE INDEX IF NOT EXISTS idx_video_singer_claim_v2 ON videos(discovered_at ASC)
    WHERE status IN ('PENDING', 'PROCESSING', 'PROCESSED') AND fetched = TRUE AND has_audio = FALSE;
CREATE INDEX IF NOT EXISTS idx_video_muralist_claim ON videos(discovered_at ASC)
    WHERE status IN ('PENDING', 'PROCESSING') AND has_video = FALSE;
CREATE INDEX IF NOT EXISTS idx_video_sweep ON videos(last_updated_at ASC)
    WHERE status = 'PROCESSED';

-- ===========================================================================
-- Storage Limits & Retention Policies
-- ===========================================================================
-- This VPS has ~41 GB free. Each row in the stats logs is ~60 bytes.
-- Without retention, the pipeline fills the disk in weeks. The settings
-- below cap growth at ~4 GB total for the time-series tables.

-- Per-table row caps (enforced by Janitor sweep):
--   channel_stats_log: 500 000 rows  ≈ 30 MB
--   video_stats_log:   2 000 000 rows ≈ 120 MB
--   system_events:     100 000 rows   ≈ 50 MB
--   transcripts:       metadata only (payload in HF vault)

COMMENT ON TABLE channel_stats_log IS
'Time-series channel metrics. Retention: 500K rows (~30 MB). Janitor sweeps oldest first.';
COMMENT ON TABLE video_stats_log IS
'Time-series video metrics. Retention: 2M rows (~120 MB). Janitor sweeps oldest first.';
COMMENT ON TABLE system_events IS
'System event log. Retention: 100K rows (~50 MB). Janitor sweeps oldest first.';
COMMENT ON TABLE search_queue IS
'Search term queue. Janitor removes inactive/resolved terms.';
COMMENT ON TABLE transcripts IS
'Transcript metadata vault pointers. Payload lives in HF dataset, not Postgres.';

-- When TimescaleDB is available, uncomment:
-- SELECT add_retention_policy('channel_stats_log', INTERVAL '90 days');
-- SELECT add_retention_policy('video_stats_log', INTERVAL '90 days');
-- SELECT add_retention_policy('system_events', INTERVAL '30 days');


-- ===========================================================================
-- P1b migration: explicit per-step state (fan-out / fan-in join barrier)
-- Each artifact in the `raw -> {singer, painter, muralist}` fan-out and the
-- downstream `scribe` gets its OWN phase column instead of a conjunction of
-- booleans. `pipeline_phase` is a *derived* frontier (a video's progress
-- readable as a state, not inferred from booleans) for ops/monitoring only —
-- it does NOT drive claim selection, so the parallel topology is preserved.
-- The legacy booleans (fetched/has_audio/...) stay as a transitional seam kept
-- in sync by `sync_step_phases`; they are removed in P3
-- (docs/agent-consolidation-proposal.md). All statements are idempotent so
-- `provision_schema` can re-apply them on every agent startup.
-- ===========================================================================

DO $$ BEGIN
    CREATE TYPE step_phase AS ENUM ('PENDING', 'PROCESSING', 'DONE', 'FAILED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

ALTER TABLE videos ADD COLUMN IF NOT EXISTS raw_phase step_phase DEFAULT 'PENDING';
ALTER TABLE videos ADD COLUMN IF NOT EXISTS audio_phase step_phase DEFAULT 'PENDING';
ALTER TABLE videos ADD COLUMN IF NOT EXISTS visuals_phase step_phase DEFAULT 'PENDING';
ALTER TABLE videos ADD COLUMN IF NOT EXISTS transcript_phase step_phase DEFAULT 'PENDING';
ALTER TABLE videos ADD COLUMN IF NOT EXISTS clip_phase step_phase DEFAULT 'PENDING';

-- Frontier = the earliest step still not DONE (pipeline order). IMMUTABLE so it
-- can back a generated column.
CREATE OR REPLACE FUNCTION pipeline_frontier(
    raw step_phase, audio step_phase, visuals step_phase,
    transcript step_phase, clip step_phase
) RETURNS VARCHAR LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN raw <> 'DONE' THEN 'RAW'
        WHEN audio <> 'DONE' THEN 'AUDIO'
        WHEN visuals <> 'DONE' THEN 'VISUALS'
        WHEN transcript <> 'DONE' THEN 'TRANSCRIPT'
        WHEN clip <> 'DONE' THEN 'CLIP'
        ELSE 'DONE'
    END;
$$;

ALTER TABLE videos ADD COLUMN IF NOT EXISTS pipeline_phase VARCHAR GENERATED ALWAYS AS (
    pipeline_frontier(raw_phase, audio_phase, visuals_phase, transcript_phase, clip_phase)
) STORED;

-- Backfill phase columns from the legacy booleans — only rows where a boolean
-- is TRUE but its phase is not yet DONE (i.e. pre-migration data). Genuinely
-- pending rows (boolean FALSE, phase PENDING) are intentionally left alone so
-- this is a no-op after the first run.
UPDATE videos SET
    raw_phase        = CASE WHEN fetched        THEN 'DONE' ELSE raw_phase END,
    audio_phase      = CASE WHEN has_audio       THEN 'DONE' ELSE audio_phase END,
    visuals_phase    = CASE WHEN has_visuals     THEN 'DONE' ELSE visuals_phase END,
    transcript_phase = CASE WHEN has_transcript  THEN 'DONE' ELSE transcript_phase END,
    clip_phase       = CASE WHEN has_video       THEN 'DONE' ELSE clip_phase END
WHERE (fetched AND raw_phase <> 'DONE')
   OR (has_audio AND audio_phase <> 'DONE')
   OR (has_visuals AND visuals_phase <> 'DONE')
   OR (has_transcript AND transcript_phase <> 'DONE')
   OR (has_video AND clip_phase <> 'DONE');

-- Bidirectional sync: keep booleans and phase columns consistent regardless of
-- which code path writes which. Old agents that only touch booleans still keep
-- phases correct; new code that drives phases keeps booleans correct.
--
-- Per stage the precedence is:
--   1. boolean TRUE wins   -> the artifact exists, so the phase is DONE
--   2. an explicitly changed phase wins -> derive the boolean from it
--   3. otherwise the boolean was cleared -> reset the phase to PENDING, but
--      preserve a terminal FAILED so a dead stage is not silently retried.
--
-- Step 2 must be reached on INSERT. The previous form tested
-- `NEW.fetched IS DISTINCT FROM OLD.fetched`, but OLD is NULL on INSERT, so
-- that was always true for a false/DEFAULT boolean and the boolean branch
-- always won. The net effect was that
-- `INSERT INTO videos (..., raw_phase) VALUES (..., 'PROCESSING')` was silently
-- rewritten to 'PENDING': a video could never be enqueued mid-phase. Testing
-- `NEW.fetched` (true) instead of "changed" fixes that and also makes an
-- explicit `has_x = FALSE, x_phase = 'PROCESSING'` update behave as written.
CREATE OR REPLACE FUNCTION sync_step_phases() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.fetched THEN
        NEW.raw_phase := 'DONE'::step_phase;
    ELSIF NEW.raw_phase IS DISTINCT FROM OLD.raw_phase THEN
        NEW.fetched := (NEW.raw_phase = 'DONE');
    ELSE
        NEW.raw_phase := CASE WHEN NEW.raw_phase = 'FAILED' THEN 'FAILED'::step_phase
                              ELSE 'PENDING'::step_phase END;
    END IF;
    IF NEW.has_audio THEN
        NEW.audio_phase := 'DONE'::step_phase;
    ELSIF NEW.audio_phase IS DISTINCT FROM OLD.audio_phase THEN
        NEW.has_audio := (NEW.audio_phase = 'DONE');
    ELSE
        NEW.audio_phase := CASE WHEN NEW.audio_phase = 'FAILED' THEN 'FAILED'::step_phase
                                ELSE 'PENDING'::step_phase END;
    END IF;
    IF NEW.has_visuals THEN
        NEW.visuals_phase := 'DONE'::step_phase;
    ELSIF NEW.visuals_phase IS DISTINCT FROM OLD.visuals_phase THEN
        NEW.has_visuals := (NEW.visuals_phase = 'DONE');
    ELSE
        NEW.visuals_phase := CASE WHEN NEW.visuals_phase = 'FAILED' THEN 'FAILED'::step_phase
                                  ELSE 'PENDING'::step_phase END;
    END IF;
    IF NEW.has_transcript THEN
        NEW.transcript_phase := 'DONE'::step_phase;
    ELSIF NEW.transcript_phase IS DISTINCT FROM OLD.transcript_phase THEN
        NEW.has_transcript := (NEW.transcript_phase = 'DONE');
    ELSE
        NEW.transcript_phase := CASE WHEN NEW.transcript_phase = 'FAILED' THEN 'FAILED'::step_phase
                                     ELSE 'PENDING'::step_phase END;
    END IF;
    IF NEW.has_video THEN
        NEW.clip_phase := 'DONE'::step_phase;
    ELSIF NEW.clip_phase IS DISTINCT FROM OLD.clip_phase THEN
        NEW.has_video := (NEW.clip_phase = 'DONE');
    ELSE
        NEW.clip_phase := CASE WHEN NEW.clip_phase = 'FAILED' THEN 'FAILED'::step_phase
                               ELSE 'PENDING'::step_phase END;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS sync_step_phases_trigger ON videos;
CREATE TRIGGER sync_step_phases_trigger
    BEFORE INSERT OR UPDATE ON videos
    FOR EACH ROW EXECUTE FUNCTION sync_step_phases();

-- YouTube topic knowledge graph
CREATE TABLE IF NOT EXISTS knowledge_topics (
    url TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    language TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS video_topics (
    video_id VARCHAR(20) NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    topic_url TEXT NOT NULL REFERENCES knowledge_topics(url),
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(video_id, topic_url)
);
CREATE TABLE IF NOT EXISTS channel_topics (
    channel_id VARCHAR(50) NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    topic_url TEXT NOT NULL REFERENCES knowledge_topics(url),
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(channel_id, topic_url)
);
CREATE TABLE IF NOT EXISTS youtube_topic_sync (
    kind TEXT NOT NULL CHECK (kind IN ('video', 'channel')),
    resource_id TEXT NOT NULL,
    observed_at TIMESTAMPTZ,
    outcome TEXT CHECK (outcome IN ('topics', 'empty', 'unavailable')),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_token UUID,
    leased_until TIMESTAMPTZ,
    PRIMARY KEY(kind, resource_id)
);
CREATE INDEX IF NOT EXISTS idx_video_topics_topic ON video_topics(topic_url, video_id);
CREATE INDEX IF NOT EXISTS idx_channel_topics_topic ON channel_topics(topic_url, channel_id);
CREATE INDEX IF NOT EXISTS idx_topic_sync_due ON youtube_topic_sync(kind, next_attempt_at, resource_id);

COMMENT ON TABLE knowledge_topics IS 'Canonical Wikipedia URLs reported by YouTube topicDetails.topicCategories.';
COMMENT ON TABLE youtube_topic_sync IS 'Observed topic coverage and expiring owned leases; absence, empty topics and unavailable resources are distinct.';
