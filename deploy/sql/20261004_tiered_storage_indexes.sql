-- Run outside a transaction. These indexes also work without TimescaleDB.
-- No retention policy or data deletion is introduced by this migration.
SET lock_timeout = '2s';
SET statement_timeout = '5min';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stats_retention
    ON video_stats_log (timestamp, video_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_videos_vault_flush
    ON videos (discovered_at, id) WHERE has_transcript AND vault_write_pending;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transcripts_unvaulted
    ON transcripts (video_id) WHERE vault_uri IS NULL AND content IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transcripts_retained_payload
    ON transcripts (video_id) WHERE vault_uri IS NOT NULL AND content IS NOT NULL;
