-- Safe rollback for 20260902_tracker_durable_state.sql.
--
-- When safe, this restores the original three-tier check.  Durable counter
-- columns are deliberately kept by default: dropping them would destroy
-- observations retained after the migration and is not a routine rollback.

\set ON_ERROR_STOP on

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

-- Refuse the downgrade if DORMANT rows exist.  They require the v2 constraint
-- and monthly-recheck policy; silently coercing or deleting them would be data loss.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM watchlist WHERE tracking_tier = 'DORMANT') THEN
        RAISE EXCEPTION
            'Rollback refused: watchlist contains DORMANT rows; retain v2 Tracker policy';
    END IF;
END
$$;

BEGIN;
LOCK TABLE watchlist IN SHARE ROW EXCLUSIVE MODE;

-- Repeat the guard under the table lock to avoid restoring the old constraint
-- if a DORMANT row appeared after the initial preflight.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM watchlist WHERE tracking_tier = 'DORMANT') THEN
        RAISE EXCEPTION
            'Rollback refused: watchlist contains DORMANT rows; retain v2 Tracker policy';
    END IF;

    ALTER TABLE watchlist DROP CONSTRAINT IF EXISTS watchlist_tracking_tier_check_v2;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'watchlist'::regclass
          AND conname = 'watchlist_tracking_tier_check'
    ) THEN
        ALTER TABLE watchlist
            ADD CONSTRAINT watchlist_tracking_tier_check
            CHECK (tracking_tier IN ('HOURLY', 'DAILY', 'WEEKLY'));
    END IF;
END
$$;

COMMIT;

-- Intentionally retained additions:
--   watchlist.last_views
--   watchlist.last_likes
--   watchlist.last_comment_count
--   watchlist.unavailable_count
--   watchlist_tracker_counters_nonnegative
--
-- Destructive cleanup requires a separately reviewed data-loss decision and is
-- not part of this rollback.  If explicitly approved after exporting the
-- values, run these statements in a maintenance window:
--
--   ALTER TABLE watchlist
--       DROP CONSTRAINT IF EXISTS watchlist_tracker_counters_nonnegative;
--   ALTER TABLE watchlist
--       DROP COLUMN IF EXISTS last_views,
--       DROP COLUMN IF EXISTS last_likes,
--       DROP COLUMN IF EXISTS last_comment_count,
--       DROP COLUMN IF EXISTS unavailable_count;
