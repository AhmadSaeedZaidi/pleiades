-- Durable Tracker state and safe watchlist-gap enrollment template.
--
-- Apply this file with psql.  This migration does not execute the watchlist-gap
-- enrollment at the end: review the candidate count
-- and the staggered schedule first, then run that INSERT as a separately
-- approved data operation.

\set ON_ERROR_STOP on

BEGIN;

-- Lock guard: fail fast rather than queue behind a live writer.
--
-- The Tracker is the hottest writer in the fleet (a 60-second cycle) and the
-- Painter/Streamer claim every 120 seconds. Without a lock_timeout, DDL in this
-- file waits indefinitely for those transactions, holding the queue behind it and
-- stalling the pipeline for as long as the wait lasts. 3s is chosen so the
-- migration aborts with a clear error and can be retried in a quiet window,
-- rather than silently degrading production throughput.
SET lock_timeout = '3s';
SET statement_timeout = '120s';

-- Counters are nullable because a watchlist row may not have been observed by
-- Tracker yet.  The counter row is the durable previous sample after the hot
-- video_stats_log row is archived.
ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS last_views BIGINT;
ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS last_likes BIGINT;
ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS last_comment_count BIGINT;
ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS unavailable_count INTEGER;

-- Existing deployments get the same invariant as a fresh schema.  The UPDATE
-- only touches newly-added NULLs; it is safe to rerun and makes the subsequent
-- NOT NULL constraint deterministic.
ALTER TABLE watchlist ALTER COLUMN unavailable_count SET DEFAULT 0;
UPDATE watchlist
SET unavailable_count = 0
WHERE unavailable_count IS NULL;
ALTER TABLE watchlist ALTER COLUMN unavailable_count SET NOT NULL;
UPDATE watchlist SET tracking_tier = 'HOURLY' WHERE tracking_tier IS NULL;
ALTER TABLE watchlist ALTER COLUMN tracking_tier SET NOT NULL;

-- The inline check in atlas/schema.sql has the generated PostgreSQL name below.
-- Drop that old version before adding the additive DORMANT-aware constraint.
ALTER TABLE watchlist DROP CONSTRAINT IF EXISTS watchlist_tracking_tier_check;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'watchlist'::regclass
          AND conname = 'watchlist_tracking_tier_check_v2'
    ) THEN
        ALTER TABLE watchlist
            ADD CONSTRAINT watchlist_tracking_tier_check_v2
            CHECK (tracking_tier IN ('HOURLY', 'DAILY', 'WEEKLY', 'DORMANT'))
            NOT VALID;
    END IF;
END
$$;
ALTER TABLE watchlist VALIDATE CONSTRAINT watchlist_tracking_tier_check_v2;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'watchlist'::regclass
          AND conname = 'watchlist_tracker_counters_nonnegative'
    ) THEN
        ALTER TABLE watchlist
            ADD CONSTRAINT watchlist_tracker_counters_nonnegative
            CHECK (
                (last_views IS NULL OR last_views >= 0)
                AND (last_likes IS NULL OR last_likes >= 0)
                AND (last_comment_count IS NULL OR last_comment_count >= 0)
                AND unavailable_count >= 0
            )
            NOT VALID;
    END IF;
END
$$;
ALTER TABLE watchlist
    VALIDATE CONSTRAINT watchlist_tracker_counters_nonnegative;

-- Existing rows intentionally start with a NULL durable sample. Bulk-seeding all
-- 81k rows from the Timescale hypertable produced an unacceptable update plan
-- on the 2-core production VPS and was rolled back. Tracker establishes each
-- baseline on its next normal observation; no historical sample is fabricated.

COMMENT ON COLUMN watchlist.last_views IS
'Most recent observed YouTube view counter; durable previous sample for Tracker velocity.';
COMMENT ON COLUMN watchlist.last_likes IS
'Most recent observed YouTube like counter; durable previous sample for Tracker reporting.';
COMMENT ON COLUMN watchlist.last_comment_count IS
'Most recent observed YouTube comment counter; durable previous sample for Tracker reporting.';
COMMENT ON COLUMN watchlist.unavailable_count IS
'Consecutive Tracker samples absent from the API; three omissions move the row to DORMANT.';
COMMENT ON CONSTRAINT watchlist_tracking_tier_check_v2 ON watchlist IS
'Tracker tiers include DORMANT for unavailable videos awaiting a monthly recheck.';
COMMENT ON CONSTRAINT watchlist_tracker_counters_nonnegative ON watchlist IS
'YouTube counters and consecutive omission count cannot be negative.';

COMMIT;

ANALYZE watchlist;

/*
 * REVIEW-ONLY, DATA-INDEPENDENT BACKFILL TEMPLATE
 *
 * This enrolls every video currently absent from watchlist.  It assigns the
 * normal age tier using published_at (falling back to discovered_at):
 *   < 24 hours = HOURLY, < 7 days = DAILY, otherwise = WEEKLY.
 * A stable hash slot is applied within each tier interval (3,600 / 86,400 /
 * 604,800 seconds), so candidates are spread rather than due at once.  The
 * template is intentionally commented out: first inspect the count, candidate
 * IDs, and projected API pace in a staging/maintenance session.
 *
 * Candidate count preflight (all videos absent from watchlist):
 *
 *   SELECT count(*)
 *   FROM videos v
 *   LEFT JOIN watchlist w ON w.video_id = v.id
 *   WHERE w.video_id IS NULL;
 *
 * Staggered enrollment (run only after review):
 *
 *   WITH params AS (
 *       SELECT NOW() AS anchor
 *   ),
 *   candidates AS (
 *       SELECT v.id,
 *              COALESCE(v.published_at, v.discovered_at, p.anchor) AS observed_at,
 *              p.anchor
 *       FROM videos v
 *       CROSS JOIN params p
 *       LEFT JOIN watchlist w ON w.video_id = v.id
 *       WHERE w.video_id IS NULL
 *   ),
 *   tiered AS (
 *       SELECT c.*,
 *              CASE
 *                  WHEN c.observed_at >= c.anchor - INTERVAL '1 day' THEN 'HOURLY'
 *                  WHEN c.observed_at >= c.anchor - INTERVAL '7 days' THEN 'DAILY'
 *                  ELSE 'WEEKLY'
 *              END AS tier
 *       FROM candidates c
 *   ),
 *   slotted AS (
 *       SELECT t.*,
 *              CASE t.tier
 *                  WHEN 'HOURLY' THEN 3600
 *                  WHEN 'DAILY' THEN 86400
 *                  ELSE 604800
 *              END AS interval_seconds
 *       FROM tiered t
 *   )
 *   INSERT INTO watchlist (
 *       video_id, tracking_tier, last_tracked_at, next_track_at,
 *       last_views, last_likes, last_comment_count, unavailable_count,
 *       created_at
 *   )
 *   SELECT s.id,
 *          s.tier,
 *          prior.timestamp,
 *          s.anchor + (
 *              ((hashtextextended(s.id, 0) % s.interval_seconds
 *                + s.interval_seconds) % s.interval_seconds)
 *              * INTERVAL '1 second'
 *          ),
 *          prior.views,
 *          prior.likes,
 *          prior.comment_count,
 *          0,
 *          s.anchor
 *   FROM slotted s
 *   LEFT JOIN LATERAL (
 *       SELECT l.timestamp, l.views, l.likes, l.comment_count
 *       FROM video_stats_log l
 *       WHERE l.video_id = s.id
 *       ORDER BY l.timestamp DESC
 *       LIMIT 1
 *   ) prior ON TRUE
 *   ON CONFLICT (video_id) DO NOTHING;
 */
