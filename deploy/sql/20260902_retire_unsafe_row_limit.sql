-- Retire an unused generic helper that is unsafe for composite primary keys.
--
-- The function chose only the first primary-key column, so invoking it against
-- video_stats_log or channel_stats_log could delete all history for selected
-- subjects rather than only the requested oldest rows. No application caller
-- exists. Reintroducing a generic retention API requires explicit ordering and
-- row-identity semantics, so this code-only removal has no rollback script.

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

DROP FUNCTION IF EXISTS enforce_table_row_limit(REGCLASS, BIGINT);

COMMIT;
