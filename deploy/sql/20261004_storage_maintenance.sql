-- Per-table maintenance for high-churn hot storage, not global server tuning.
-- These changes preserve all rows. VACUUM is a separate operator command.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
ALTER TABLE transcripts SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.02,
    toast.autovacuum_vacuum_scale_factor = 0.02
);
ALTER TABLE video_stats_log SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.02
);
COMMIT;
-- To restore defaults on an installation whose original reloptions were NULL:
-- ALTER TABLE transcripts RESET (autovacuum_vacuum_scale_factor,
--     autovacuum_analyze_scale_factor, toast.autovacuum_vacuum_scale_factor);
-- ALTER TABLE video_stats_log RESET (autovacuum_vacuum_scale_factor,
--     autovacuum_analyze_scale_factor);
