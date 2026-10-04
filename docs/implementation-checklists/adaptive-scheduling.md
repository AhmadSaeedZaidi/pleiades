> Historical record. Use the current documentation index and October audit for active guidance.

# Adaptive Scheduling — Implementation Checklist (age + velocity)

**Status: IN PROGRESS**

Persistent work log for shifting the Tracker from a uniform `videos.last_tracked_at`
FIFO to the **designed adaptive-scheduling architecture** (per `docs/adaptive-scheduling.md`
and `docs/architecture.md` §2 Tracker), adding a **view-velocity override** so hot videos
stay tracked hourly past the age cutoff and dead videos decay faster.

Consumed by: `maia/src/maia/tracker/flow.py`, `atlas/src/atlas/repositories/watchlist.py`,
`atlas/src/atlas/repositories/video/tracking.py`, `atlas/src/atlas/schema.sql`, hunter flow.

---

## Goal

Tracker provides relevance / view-growth statistics with **exponential decay**:

- Hot videos: tracked **every hour** (even past 24h, if still growing fast)
- Sustained: after ~1 day **every day**; after ~7 days **every week**
- Dead/decaying videos: recede to weekly faster

Directory of live control switches: `atlas/src/atlas/config.py`
`TRACKER_*` settings.

## Designed architecture (must respect)

- `watchlist` table (`schema.sql:151-161`): `video_id PK`, `tracking_tier`
  `CHECK IN ('HOURLY','DAILY','WEEKLY')`, `last_tracked_at`, `next_track_at`,
  `created_at`. NO FK to `videos` → survives janitor hard-delete by design.
- `WatchlistRepository` (`atlas/src/atlas/repositories/watchlist.py`):
  `add`, `fetch_batch` (by `next_track_at <= NOW()` + `FOR UPDATE SKIP LOCKED`),
  `update_schedule`, `calculate_next_track_time`.
- Metrics stay in `video_stats_log` (hottest tier); schedule lives in `watchlist`.
- Janitor deletes `videos` rows only; `watchlist` untouched (separate table).

## Status

- [x] Confirm intended architecture (watchlist + adaptive-scheduling doc exists, unwired)
- [x] Diagnose current behavior gap: tracker used `videos` FIFO, watchlist empty (0 rows)
- [x] Fix prior opportunistic bugs (double-logging; dead-video queue wedge via `mark_tracked`)
- [x] Add `TRACKER_*` velocity + decay settings to `config.py`
- [x] `WatchlistRepository`:
  - [x] `fetch_batch` returns `published_at` (LEFT JOIN `videos`) so age-tier can be computed
  - [x] `calculate_next_track_time(published_at, views_per_hour=None)` → (tier, next_track_at)
  - [x] velocity override: keep/watch a growing video at its more-frequent tier
- [x] Find previous stats per video (`video_stats_log`) to compute velocity
- [x] Hunter: `watchlist.add()` after ingest
- [x] Tracker flow: fetch from `watchlist`, update schedule w/ velocity, handle dead
- [x] Backfill watchlist from existing `videos` (staggered `next_track_at`)
- [x] Tests updated + added; maia + atlas suites green; ruff clean
- [x] Deploy (restart orchestrator), backfill, verify tiers liveness + velocity live
- [x] Record **observed** performance in §Performance below

---

## Performance considerations — velocity override

### Theoretical (design time)

1. **Velocity query = +1 indexed request per cycle.** Computing views/hour for a
   50-video batch requires one extra `video_stats_log` read (previous sample per id).
   PK is `(video_id, timestamp)` → `DISTINCT ON (video_id) ... ORDER BY timestamp DESC`
   serves each id in one pass; cost scales with **batch size (50)**, not corpus size.
   Expect a few ms, negligible vs the YouTube API round-trip (hundreds of ms).
   Risk: LOW.

2. **Hot-video retention does NOT add API cost.** Velocity only *raises* a video's
   tier (hourlier). It never increases per-cycle `batch_size` or adds extra API
   calls — the same ≤50 id `videos.list` call serves whatever the batch is. A
   video promoted HOURLY consumes 1 slot more often, but the corpus cycles evenly
   over the same quota. Risk: LOW.

3. **`next_track_at` index stays selective.** Watchlist lookup still uses
   `idx_watchlist_next_track`; velocity only changes the value written back, not
   how the next batch is discovered. `FOR UPDATE SKIP LOCKED` prevents dup work.
   Risk: LOW.

4. **Backfill burst risk (MEDIUM).** Seeding `watchlist` for ~57k existing videos
   must **stagger `next_track_at`** (e.g. spread hours/days) or the first tracker
   cycle floods with overdue rows. The 1h cooldown keeps `fetch_batch` naturally
   paced; verify no retention storm on first deploy.

### Observed (post-implementation)

Deployed + verified live on 2026-08-03 (local `pleiades` DB, `pleiades-ingestion` service).

- **Backfill**: 56,877 videos inserted with tier-by-age + staggered `next_track_at`
  (HOURLY within ~15 min, DAILY within ~1 day, WEEKLY within ~1 week) to avoid a
  cold-start overdue burst.
  - Tier distribution post-backfill: HOURLY 227, DAILY 1,939, WEEKLY 54,711.
- **Cold-start fix**: first live tracker cycle hit `psycopg FeatureNotSupported:
  FOR UPDATE cannot be applied to the nullable side of an outer join` in
  `fetch_batch` (LEFT JOIN `videos`). Fixed by scoping the row lock to the
  nullable-preserved side only: `FOR UPDATE OF w SKIP LOCKED`. Resolved and
  re-deployed at ~17:37 UTC.
- **Per-cycle behavior (live)**: tracker fetches 50 due videos from `watchlist`,
  stats fetch + hot-tier log succeeds, dead/deleted videos (not on YouTube) get
  their schedule advanced toward less-frequent tracking (no longer wedged by
  `mark_tracked`). Earlier double-logging bug stays fixed (rows == unique
  videos).
- **Velocity override (live verify)**: one processed video at 66.4 views/hr (> 50
  `HOT`) → kept at/boosted to HOURLY; dead (0.0) + unknown (`None`) videos fell
  to the age floor (WEEKLY). Velocity query is a single windowed scan over the
  batch's `(video_id, timestamp)` PK — cheap (see theoretical note).
- **Quota**: archeologist (search) key pool exhausted at 17:34 UTC — pre-existing,
  unrelated to watchlist (separate key pool from tracker's `videos.list`).
  Tracker continued to fall via adaptive decay once keys available.

To-fill (running): precise per-cycle velocity-query latency ms; steady-state overdue
backlog drain; units/day vs `tracker_demand_units()` model over a full day.