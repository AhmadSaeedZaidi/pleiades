> Historical record. Use the current documentation index and October audit for active guidance.

# Pleiades Next Steps

**Date:** 2026-09-02
**Status:** durable Tracker schema/backfill active under
`pleiades-ingestion.service`; the required YouTube SOCKS tunnel was restored and
live media acquisition resumed on 2026-09-02

## Current direction

Pleiades is a PostgreSQL-owned, restart-safe, at-least-once multimodal YouTube
ingestion service. The main systemd process should own live cadence and call
eight scheduled plain async operations; Archeologist remains manual. PostgreSQL owns durable claims, work state, hot
metadata, and tracking; the Hugging Face vault owns large or historical
artifacts.

Prefect is temporarily an optional compatibility layer. Its decorated adapters
remain for rollback and telemetry, but its state is not pipeline recovery and
its API is not required for application operations. A future lightweight
YouTube core should contain normalized models, API/media clients, captions,
typed network errors, and egress policy - not PostgreSQL, Prefect, schedules, or
retention rules. A thin Search MCP should consume that core.

## Completed work

- Reconstructed and audited the current architecture.
- Added plain operation seams and one in-process scheduler cadence.
- Retained unscheduled thin Prefect adapters and removed Prefect from the
  application execution path.
- Established `make check` as the canonical hermetic gate and isolated guarded
  integration/live tiers in Alkyone. The gate passes 325 tests plus Ruff and
  mypy.
- Aligned the Prefect runtime/lockfile on 3.7.8, removed the Atlas orchestration
  dependency, and removed the unused conflicting PoToken package.
- Added typed YouTube API failure classification and safe Scribe temporary
  workspace cleanup.
- Sharded future raw and metadata vault paths by video-ID prefix, with legacy
  read/reclaim compatibility, after live logs confirmed the flat raw directory
  exceeded the Hugging Face entry limit.
- Passed the guarded read-only live preflight against PostgreSQL, Hugging Face,
  and local configuration without consuming YouTube quota.
- Made Discord delivery observable and testable: notifier calls return an
  acceptance result, tracker warns on nondelivery, and heartbeat includes the
  result in its summary. An authorized live OPS heartbeat was accepted.
- Activated the plain-operation scheduler under the legacy local unit name and
  observed substantially lower memory/task use. Added forced INFO logging and a
  live-verified audio format fallback after activation exposed those gaps.
- Installed and activated the versioned `pleiades-ingestion.service`; the
  legacy executor and Prefect worker are inactive/disabled. Verified the micro
  Prefect API, micro-backed SOCKS route, tracker/heartbeat Discord delivery,
  media downloads, frame/audio extraction, janitor work, and the guarded
  Alkyone live preflight after handoff.
- Made Singer memory-bounded: raw media is materialized directly to disk and
  successful audio is committed every four videos instead of after ten.
- Replaced the ambiguous retry transition with explicit raw/audio/visuals/
  transcript/clip releases and stage-aware failures. Both are conditional on
  the current `PROCESSING` phase so stale workers cannot undo completed work.
- Made transcript cold-write finalization atomic and reclaim hot JSON for future
  flushes, with a non-empty URI guard that prevents data loss.
- Added versioned concurrent v2 Streamer/Singer claim-index migration and
  rollback scripts under `deploy/sql/`; they have not been run in production.
- Completed and validated the final code changes locally. After the old process
  reached its 4 GiB cgroup limit, systemd restarted the executor and loaded the
  final worktree. Post-restart live cycles verified bounded Singer commits,
  media stages, tracker updates, Discord Watch/OPS delivery, and the guarded
  PostgreSQL/Hugging Face/configuration preflight. The production v2 indexes and
  data backfills remain intentionally unexecuted.
- Applied the additive durable Tracker migration after cancelling and rolling
  back an unacceptably expensive corpus-wide baseline query. Existing rows now
  establish baselines on normal observations.
- Inserted all 1,320 missing watchlist rows without overwriting existing state,
  started the executor, and verified atomic 50-ID Tracker cycles plus accepted
  Discord Watch summaries. The live watchlist now covers all 82,869 videos.
- Confirmed the current Prefect workspace contains zero deployments. The small
  VPS remains useful for its independent SOCKS exit; Prefect is not a live
  scheduler.
- Made Hunter acknowledgement replay-safe: all accepted results settle, any
  database failure prevents pagination advancement, and page-ack failures are
  propagated. Added partial-failure and acknowledgement-failure tests.
- Replaced the MCP reserve pool's partial object construction with a supported
  `KeyRing.from_keys` boundary and normal strategy injection; session rotation,
  dead-key handling, and exhaustion are covered by tests.
- Removed the unused composite-key-unsafe row-limit helper from the source
  schema and added a versioned production retirement migration. The migration
  is intentionally not applied yet.
- Reconstructed, installed, and enabled a versioned `youtube-proxy.service`
  after the live unit file was found deleted. The restored tunnel passed a
  micro-VPS route check and the next normal Streamer cycle fetched media.

## Observed throughput baseline

Before this activation, a representative one-hour window sustained about
50-55 raw fetches/hour, 50 audio completions/hour (66 audio files), and 40 frame
completions/hour. At roughly 6,991 audio-pending and 7,113 visuals-pending rows,
the backlog was about 140 and 178 hours respectively at those rates. Discovery
added no new work because the single Search API project was returning its
distinct daily-quota HTTP 429; media derivation and tracking continued.

At the 18:43 UTC read-only check, the application process was active with
zero restarts and Tracker/Discord remained healthy, but port 1090 was not
listening. Streamer therefore released every claimed item after a local proxy
connection refusal. This is an infrastructure outage, not API quota exhaustion;
the versioned proxy unit was installed and enabled at 19:15 UTC. Port 1090 and a
micro-VPS request through SOCKS succeeded, and the next scheduled Streamer cycle
fetched 37.7 MB for its first item instead of failing at connection setup.

## Next five highest-value tasks

1. **Add durable work leases.** Add owner/expiry leases and phase predicates for
   expensive stage work, then watchlist/search/transcript queues. Hunter's
   failure acknowledgement is now replay-safe, but overlapping or dead workers
   can still duplicate claims until this schema and repository migration lands.

2. **Finish the MCP's real boundary before adding features.** The reserve
   `KeyRing` now uses a supported, fully tested factory/API. Decide how public
   artifact downloads are authenticated, then extract the production-independent
   YouTube core without database, vault, scheduling, or orchestration policy.

3. **Make cold archival idempotent and queryable.** Give each metrics batch a
   deterministic identity/manifest, verify it before hot deletion, report actual
   deletes, and add a reader. Distinguish vault not-found from transient/auth
   failures before Singer marks work permanently failed.

4. **Enforce a project-level YouTube/network budget.** Put a hard daily ledger
   around expensive Data API methods, require an explicit bounded date/category
   budget for Archeologist, replace broad media-error heuristics with typed
   unavailable/rate-limit/egress failures, and make all intended proxy variables
   agree. Preserve the deliberately slow Hunter cadence.

5. **Reconcile live schema, packaging, and documentation drift.** Review and
   execute `deploy/sql/20260902_claim_indexes_v2.sql`, verify planner use, then
   batch verified transcript-content and stale-raw-pointer repairs. Remove
   duplicate Poetry metadata, repair malformed dotenv lines through the secret
   management path, and archive superseded worker-era docs.

After those reliability fixes, extract the lightweight YouTube core and make MCP
thin. Move normalized YouTube identifiers/models, search/video/channel/topic
clients, typed key and network errors, captions, media acquisition, and
frame/audio primitives behind a production-independent package boundary. Keep
database, vault policy, Prefect, schedules, and retention above it.

## Production-sensitive manual work

These require maintainer review, backups, staged rollout, and rollback:

- retiring the disabled legacy unit after the post-handoff rollback window;
- concurrent replacement of the two drifted claim indexes;
- execution of `deploy/sql/20260902_retire_unsafe_row_limit.sql` after checking
  the live function definition and taking the normal schema-change precautions;
- verified transcript-content and stale-raw-pointer backfills;
- canonical state/lease schema changes and backfills;
- artifact movement, retention, deletion, or bulk vault migration;
- remote Prefect deployment/control-plane retirement;
- API keys, cookies, PoToken, SOCKS proxy, and egress configuration.

The authorized additive Tracker schema and watchlist backfill were applied. No
Prefect state, existing vault object, credential value, proxy, or remote service
was modified. The local executor service was renamed and restarted;
bounded live checks exercised database reads, YouTube media acquisition, the
existing micro-backed proxy, and both surveillance and OPS Discord webhooks.
Normal application processing continued against durable PostgreSQL state.

## Genuine remaining questions

- What retention and re-fetch guarantees apply to each raw and derived asset?
- What lease/retry policy should replace opportunistic `PROCESSING` reclaim?
- What observation window is sufficient to retire Prefect adapters?
- Which MCP features must work without production credentials?
- Which topic traversals would justify explicit PostgreSQL edges and, much
  later, a graph database?
---

## Tracker implementation handoff (2026-09-02)

The approved Tracker schema work is implemented and active in production:

- `deploy/sql/20260902_tracker_durable_state.sql` adds durable previous
  counters, a consecutive omission count, DORMANT support, and reuse of
  the existing due-queue index;
- existing watchlist rows establish their durable baseline on the next successful
  Tracker check; the migration leaves all-gap enrollment as a separately reviewed INSERT, stably
  staggered within each row's assigned tier interval;
- available videos older than seven days remain WEEKLY forever; three
  consecutive API omissions move a row to DORMANT for monthly recheck;
- `deploy/sql/20260902_tracker_durable_state_rollback.sql` refuses to
  downgrade while DORMANT rows exist and retains populated counter columns
  by default.

The initial full-history baseline query was cancelled and rolled back because its
plan was too expensive for the executor. The corrected migration committed in
about ten seconds, and all 1,320 enrollment gaps were inserted with weekly
staggering. Keep Hunter to one search every 20 minutes and scheduled
Archeologist discovery disabled; Tracker's 50-ID `videos.list` batches are
independent. Historical samples missed during the broken-tracker interval cannot
be reconstructed, so normal observations establish new baselines.

## Tracker activation status (2026-09-02)

The durable Tracker implementation and migration are complete locally. Hermetic
validation passed 325 tests across Atlas, Maia, MCP, and Alkyone; all package
linters and type checks passed. The guarded read-only Alkyone live preflight
also passed database, vault, and configuration checks without a YouTube call.

Production activation is complete. The live database has 82,869 videos and
82,869 watchlist rows. The service runs eight scheduled operations, Tracker at
50 IDs/minute, Hunter at one search/20 minutes, and no scheduled Archeologist.
The first new Tracker cycle committed 48 available and two unavailable
observations and delivered the revised Discord summary; subsequent 50/50 cycles
also delivered. Hunter remains isolated on the known daily-quota HTTP 429. Do
not run a separate discovery smoke call or increase discovery cadence.

