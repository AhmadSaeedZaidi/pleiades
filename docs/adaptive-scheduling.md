# Adaptive tracking

Every discovered video joins `watchlist` atomically with ingestion. Tracker takes
due targets in bounded batches and writes metrics plus the next schedule in one
transaction. Tracking survives hot-media archival.

Age establishes a baseline: hourly for the first day, daily through the first
week, and weekly afterwards. Measured view velocity can promote hot videos or
accelerate decay. Unavailable videos enter `DORMANT` and are revisited monthly.

The durable last sample (`last_views`, likes/comments, `last_tracked_at`) belongs
to the watchlist, so retention of `video_stats_log` does not erase the baseline.
Velocity uses view delta divided by elapsed hours; absent/invalid samples are
handled explicitly. See `WatchlistRepository.calculate_next_track_time` and the
Tracker operation for the authoritative thresholds and missing-video behavior.

The dashboard shows the watchlist count, due count, last-sample views and tier.
It never advances the schedule or requests fresh YouTube statistics.

[Architecture](architecture.md) · [Tests](testing.md)
