> Historical record. Use the current documentation index and October audit for active guidance.

# Pleiades Architecture Audit

**Audit date:** 2026-08-20
**Repository:** `wip/adaptive-scheduling-and-mcp` at `ab0295f`
**Audit status:** complete; bounded refactor implemented and reviewed
**Implementation follow-up:** 2026-09-02; current worktree active and live-verified

This document describes what the repository and live local runtime actually do,
not what older README files or earlier design proposals intended. Claims are
anchored to source, configuration, history, tests, and safe local runtime
observations. The architecture and runtime sections explicitly label the
pre-refactor audit baseline where the bounded implementation campaign changed
execution seams. Credentials, cookie contents, and key values were not
inspected.

## 1. Executive diagnosis

Pleiades has a coherent product thesis: continuously discover useful YouTube
videos, retain operational metadata in PostgreSQL, and fan out expensive or
large media work into cold storage. The useful part of the current system is a
real producer/consumer pipeline backed by PostgreSQL row claiming and a vault.
It is not a prototype that needs a rewrite.

At the audit baseline, the repository was difficult to reason about because
several valid designs had been layered together without fully retiring their
predecessors:

- Prefect deployment schedules and queues coexist with an application-owned
  in-process scheduler.
- PostgreSQL owns durable video work, while Prefect still looks like a second
  orchestration authority.
- Video progress is represented by status, five booleans, five step phases, and
  a generated frontier.
- Some agents use `BaseBatchAgent`; others retain bespoke wrappers and legacy
  flow entry points.
- Repositories are documented as SQL boundaries but some repository mixins write
  to the vault and perform archival transactions.
- MCP is a useful on-demand product, but imports production Maia internals and
  requires the entire production package stack.
- Current and historical operational documentation disagrees about the active
  systemd service and the Streamer/Singer/Scribe responsibilities.

The recommended direction is a moderate simplification, not a rewrite:

1. Make plain Python application operations and one systemd-owned scheduler the
   source of execution truth. Keep Prefect as an optional adapter temporarily,
   for telemetry and rollback, but stop depending on it for recovery.
2. Converge eventually on one canonical stage-state model, with an explicit,
   production-safe migration. Do not touch live state columns or reclaim logic
   in this campaign.
3. Extract a lightweight, production-independent YouTube core and make MCP a
   thin consumer of it.

The first campaign has now implemented the execution-boundary simplification
without destabilizing the vault migration, watchlist, raw-reclamation barrier,
or live database. The state-model and shared-core simplifications remain
deliberately deferred.

## 2. Intended product

The design intent is a high-throughput, resource-efficient, multimodal YouTube
data ingestion system for a small Oracle VPS (approximately two CPUs and 12 GB
RAM). It should collect:

- representative frames/images;
- normalized audio;
- captions/transcripts and, where needed, STT-derived transcripts;
- video and channel metadata;
- search/discovery metadata;
- topic identifiers suitable for later relationships.

The intended storage model is:

```text
HOT: PostgreSQL on the main VPS
  operational state, queues, active metadata, recent statistics,
  scheduling/tracking/indexing, transcript staging

COLD: HuggingFace-backed vault/object storage
  raw audio, normalized audio, frames, transcripts, full videos,
  metadata snapshots, archived statistics and history
```

A second micro VPS provides the Prefect control plane and a SOCKS5 route for
selected YouTube traffic. The main VPS runs ingestion. These are separate
failure domains: API quota, invalid or revoked keys/authentication, temporary
throttling, policy or IP blocking, yt-dlp anti-bot failures, proxy
reachability, and cookies or PoToken behavior must not be collapsed into one
generic retry. HTTP status codes are symptoms that require response/context
classification, not failure domains by themselves.

The secondary product is a small interactive YouTube Search/Inspection MCP. It
should reuse YouTube search, metadata, captions, media, frame, and normalized
model functionality without requiring production PostgreSQL, Prefect, queues,
retention policy, or production vault credentials.

## 3. Actual architecture at audit baseline

The following runtime diagram and fact register are the evidence-backed
pre-refactor baseline reconstructed during the audit. The first campaign has
since changed only the execution seam: the live scheduler now calls plain
operations, while PostgreSQL claims, artifact paths, stage markers, and media
behavior remain as described here. See **Implementation Follow-up** for the
post-refactor runtime truth.

### Runtime diagram (pre-refactor baseline)

```text
                         remote micro VPS
                 ┌──────────────────────────────┐
                 │ Prefect API/server            │
                 │ health: reachable locally    │
                 │ SOCKS5 endpoint for egress    │
                 └──────────────┬───────────────┘
                                │ API telemetry/run state
                                │
main VPS                        │
┌───────────────────────────────▼─────────────────────────────┐
│ systemd: prefect-orchestrator (historical local name)                               │
│ maia.orchestrator: nine asyncio CycleSpec loops              │
│                                                              │
│ Hunter ────────┐                                             │
│ Archeologist ──┼── YouTube Data API (direct aiohttp)         │
│ Tracker ───────┘                                             │
│                                                              │
│ Streamer ──── yt-dlp ── SOCKS5 ── YouTube                   │
│ Singer ────── ffmpeg                                        │
│ Painter ───── ffmpeg/range requests                         │
│ Scribe ────── captions/STT                                  │
│ Janitor ───── archive/reclaim/Prefect hygiene                │
│ Heartbeat ─── health probes                                  │
│ Muralist ──── manual optional full-video archival             │
└──────────────┬───────────────────────────┬───────────────────┘
               │ PostgreSQL claims/state   │ vault/object writes
               ▼                           ▼
        ┌───────────────┐          ┌──────────────────────────┐
        │ PostgreSQL    │          │ HF vault / GCS strategy   │
        │ videos        │          │ raw/audio/frames/etc.     │
        │ channels      │          │                           │
        │ search_queue  │          └──────────────────────────┘
        │ watchlist     │
        │ stats/logs    │
        │ transcript    │
        └───────────────┘

MCP (separate process) ── currently imports Maia media/scribe/strategies
                         └── direct on-demand YouTube/network work
```

### Main execution facts (pre-refactor baseline)

- The active systemd unit was `prefect-orchestrator`, executing
  `python -m maia.orchestrator` in one long-lived process.
- The baseline `maia/src/maia/orchestrator.py:42-150` defined nine cycle
  specifications with
  intervals from approximately one to fifteen minutes.
- Each baseline cycle called a decorated Prefect flow in-process. It did not
  wait for a
  Prefect worker to claim a deployment run.
- The old `prefect-worker` unit exists as an inactive/disabled rollback path.
- Remote Prefect deployments exist and are not paused, but each deployment's
  schedule currently has `schedule.active=false`. Recent live runs are direct
  runs without deployment, queue, or pool ownership.
- PostgreSQL row claiming and stage markers, not Prefect state, determine which
  video work is durable and pending.

### Evidence anchors

- `maia/src/maia/orchestrator.py:42-150`: cycle definitions and in-process loop.
- `prefect.yaml`: nine historical deployment declarations and schedules.
- `atlas/src/atlas/repositories/video/state_machine.py:12-220`: SQL claims and
  stage markers.
- `atlas/src/atlas/schema.sql:280-375`: phases, booleans, trigger, generated
  frontier.
- `mcp/src/pleiades_mcp/media.py:17-182`: MCP's direct imports into Maia.
- `ab0295f`, `056f4af`, `3c88df3`, `5122ad9`, `b36adcf`: architecture-generation
  history.

### Reality confidence register

| Subsystem | Purpose | Status | Confidence | Basis |
| --- | --- | --- | --- | --- |
| Atlas DB/config/repositories | hot state, claims, SQL adapters | CORE | High | source/schema/tests |
| Video fan-out stages | raw, audio, frames, captions/STT | CORE | High | current flows and vault calls |
| Search/Hunter/Archeologist | discovery and metadata ingestion | CORE | High | current flows and SQL repositories |
| Tracker/watchlist | adaptive recurring statistics | CORE | High | current tracker and watchlist SQL |
| Vault strategies | HF/GCS cold artifacts/history | CORE | High | `atlas/vault.py`, active migration process |
| Prefect deployment layer | historical remote scheduling/queues | TRANSITIONAL | High for code and current schedule flags; medium for historical activation | source plus verified deployment/run state |
| In-process scheduler | current execution cadence | CORE | High | active systemd and `maia.orchestrator` |
| Network/egress | direct API plus proxied yt-dlp | SUPPORTING | High | code, service environment, live SOCKS process |
| MCP | interactive search/inspection | EXPERIMENTAL | High | server/media code and tests |
| Topic/graph model | future semantic relationships | EXPERIMENTAL/DEAD PATH | High | schema/model have no active topic writer |
| Training/HF dashboard surfaces | older ML products | LEGACY/OUT OF SCOPE | Medium | tree/history; not part of current ingestion flow |
| Production vault contents and migration completion | actual retained cold data | UNKNOWN | Low | credentials/data were intentionally not inspected |
| Exact historical Prefect activation/remote run history | remote control-plane history | UNKNOWN | Medium | current deployment existence and `schedule.active=false` are verified; older history was not reconstructed |

The UNKNOWN entries are deliberately not inferred from documentation. They are
manual-operational questions for a later production-safe review.

## 4. One-video lifecycle

1. Hunter or Archeologist claims a term from `search_queue` and queries the
   YouTube Data API. Hunter applies quality filters and writes video/channel
   metadata to PostgreSQL. Hunter may also write a dated metadata snapshot to
   the vault and add a row to `watchlist`.
2. Streamer claims a video whose raw source is not complete. It invokes
   `StealthVideoStreamer.download_unified`, then commits native raw audio and
   `info.json` to the vault and marks the raw/fetched stage.
3. Singer claims raw-ready work, reads raw audio from the vault, invokes ffmpeg,
   and writes `audio/{id}.opus` or chunks. It then marks audio complete.
4. Painter claims raw metadata, selects a stream, extracts representative
   frames, writes one visual batch, and marks visuals complete.
5. Scribe first attempts captions using a client cascade. If captions are
   unavailable and duration/STT limits allow, it obtains audio and calls the
   configured STT providers. It stages transcript content in PostgreSQL and
   marks transcript complete.
6. Janitor flushes staged transcripts to the vault, archives old statistics,
   archives eligible video metadata, and eventually purges hot rows.
7. Tracker independently claims due `watchlist` rows, obtains current stats,
   writes `video_stats_log`, and advances adaptive tracking schedule fields.
8. Muralist can optionally archive a full video. It is manually registered and
   intentionally not part of the nine scheduled deployment declarations.

This is a useful fan-out/fan-in pipeline. At-least-once behavior is expected:
claims accept unfinished `PROCESSING` rows opportunistically, so a crash can
cause retry or duplicate artifact work. Artifact writes and completion markers
are not one transaction. This is acceptable only if every stage remains
idempotent and marker updates are carefully ordered.

## 5. Current module boundaries

| Boundary | Current responsibility | Assessment |
| --- | --- | --- |
| `atlas` | settings, DB pool, schema, SQL adapters/repos, vault strategies, API key rings | Valuable infrastructure, but too broad and globally configured |
| `maia` | YouTube discovery, media pipelines, STT, agents, Prefect flows, scheduler | Actual application layer, with production and reusable concerns mixed |
| `mcp` | interactive tools, local artifacts, summary/STT integration | Useful product, but dependency direction is inverted |
| `alkyone` | integration/safety tests | Valuable guardrail; package-local invocation needed |
| `training`, `hf-spaces` | ML/dashboard surfaces | Legacy or separate products; not required by current ingestion thesis |

`VideoRepository` is a broad façade composed from ingestion, tracking, state,
Janitor, quality, and transcript mixins. This is more than a normal Repository
boundary and makes it difficult to tell which code owns persistence versus
workflow policy.

`BaseBatchAgent` is a useful local template for common claim/process/store
behavior, but forcing every agent into it would be cargo-culting. Hunter,
Tracker, Archeologist, Janitor, and Heartbeat have genuinely different shapes.

The strongest missing boundary is a plain YouTube core. Current media and API
capabilities are reusable in practice but live under Maia and import Atlas
settings/state. Production orchestration and persistence should sit above them.

## 6. Hot/cold storage audit

### Hot PostgreSQL

Current operational tables include:

- `videos`: active metadata, stage markers, status, raw URI, timestamps;
- `channels`: current channel metadata;
- `search_queue`: durable discovery terms and pagination;
- `watchlist`: durable adaptive tracking queue;
- `video_stats_log` and `channel_stats_log`: recent time-series data;
- `transcripts`: transcript staging and vault pointers;
- `system_events`: operational events.

This is the correct general role for PostgreSQL. The watchlist should remain
independent of video-row retention because tracking may outlive active video
processing.

### Cold vault

`atlas/src/atlas/vault.py` defines HF and GCS strategies and these broad paths:

```text
raw/{prefix}/{id}.<native audio container>
meta/{prefix}/{id}.info.json
audio/{id}.opus or audio/{id}/{chunk}.opus
frames/{id}/{index}.webp
transcripts/{shard}/{id}.json
videos/{id}.mp4
metadata/{date}/{id}.json
metrics/date=.../hour=.../*.parquet
```

Large media, historical snapshots, transcripts after flush, and old metrics are
appropriate cold-tier contents. Vault commits are batched for some operations,
and Janitor has a safety gate before deleting hot records.

Live inspection found that the intended transcript boundary had not actually
been enforced: all 58,964 transcript rows had both `vault_uri` and non-NULL
`content`, duplicating about 1.21 GB of payload in PostgreSQL after the cold
write. The `transcripts` table occupied about 1.29 GB of a 1.55 GB database.
The repository finalization path now records the URI, clears staged JSON, and
clears `vault_write_pending` atomically for future flushes. Existing rows need a
separately verified, batched production backfill; this audit did not mutate them.

### Boundary weaknesses

- Only some artifacts have explicit database pointers (`raw_uri`, transcript
  `vault_uri`); several locations are convention-based.
- Streamer, Singer, and Painter write directly to cold storage, while Scribe
  stages in PostgreSQL and Janitor writes later.
- Repository archive methods perform both database and vault work, so the
  persistence boundary is not uniform.
- A local Docker vault mount remains alongside remote HF/GCS strategies; this
  appears to be development or historical infrastructure, not the active
  production source of truth.
- A vault migration was active during inspection. Migration completion and
  retention contents were not established.

### Judgment

Hot PostgreSQL plus cold vault is sound and should be retained. The immediate
need is not a storage rewrite. Enforce the cold-write boundary, repair the
existing transcript duplication after verifying every referenced object, and
then establish explicit artifact ownership/pointers and observable reclaim outcomes.
Do not change the live schema, vault layout, or raw-reclamation join barrier in
the first refactor campaign.

## 7. Durable state and queue audit

PostgreSQL is the real durable work system:

- `search_queue` claims discovery terms with row locking and score decay;
- `watchlist` claims adaptive tracking targets;
- `videos` claims per-stage ingestion work;
- status/phase/boolean markers communicate completion to consumers.

Claims use `FOR UPDATE SKIP LOCKED`, which is appropriate for two CPU cores and
multiple possible workers. `PROCESSING` rows with unfinished flags are
opportunistically reclaimable because claim predicates include unfinished
processing work. This is not a lease: there is no robust claim-owner heartbeat
or expiry identity. A crash can produce duplicate work, particularly after a
vault write and before the completion marker.

`FAILED` rows are not automatically equivalent to retryable pending work and
currently require explicit/manual reset behavior. This should be clarified in a
future state migration.

The current state model is overcomplete. The trigger keeps legacy booleans and
step phases synchronized, but both representations remain in queries and
indexes. `pipeline_phase` is generated and diagnostic only. A production-safe
future state model should retain independent stage completion (because stages
fan out) while eliminating duplicate representations.

## 8. Prefect trial and verdict

### What Prefect provided at the audit baseline

Prefect provides flow/task run records, logs/telemetry, API-visible states, and
the possibility of remote deployment scheduling. Current direct runs may emit
both flow and task telemetry. The live application, however,
uses one systemd process to schedule and execute flows in-process. It does not
use the remote worker/queue system for current ingestion cycles.

The remote API was therefore on the critical path for direct flow startup:
safe local Prefect 3.7.8 testing and installed-source inspection show that a
direct flow invocation creates its flow run through the configured API before
entering the user flow body. With the remote API unavailable, the body failed
to start. This contradicted the desired control-plane-independent ingestion
requirement.

Official semantics:

- Deployments are server-side representations for remote scheduling and
  execution: <https://docs.prefect.io/v3/concepts/deployments>
- Flow states distinguish failed code from crashed infrastructure and depend on
  workers for deployment runs: <https://docs.prefect.io/v3/concepts/states>
- Local flow-run retries execute in the current process when no deployment is
  involved: <https://docs.prefect.io/v3/how-to-guides/workflows/retry-flow-runs>
- Work pools and queues provide deployment concurrency/priority controls:
  <https://docs.prefect.io/v3/concepts/work-pools>

### Operational failure behavior

- PostgreSQL video/search/watchlist state survives worker or orchestrator
  restart.
- A process death after claim can leave `PROCESSING`; later claims can retry
  unfinished work, but this is opportunistic at-least-once behavior rather than
  lease-based recovery.
- The legacy Prefect zombie reaper cleans telemetry/run state only. Its old
  queue-slot rationale does not govern live ingestion and its fixed threshold
  can be unsafe for long media/STT work; it is now opt-in only for the Prefect
  rollback adapter and must be validated/tuned before rollback use.
- At the baseline, a remote control-plane outage prevented a decorated flow
  body from starting, even though durable database work remained intact.

### Verdict: REPLACE GRADUALLY

The first campaign decoupled application operations from Prefect and runs them
from one systemd-owned scheduler. Flow entry points remain as thin adapters for
rollback and optional telemetry. The plain path does not invoke Prefect flow or
task objects and may run when the remote API is unavailable. An adapter may
retain flow-level telemetry; task telemetry is not assumed after task
decorators were removed. Prefect states, deployment queues, and zombie cleanup
are not video-pipeline recovery. The deployment schedule source was removed
from `prefect.yaml`; remote deployment retirement and Prefect package removal
remain deferred.

This is not an immediate unsafe Prefect deletion. It is a controlled reduction
of its role.

## 9. Network and rate-limit architecture

### Current paths

- YouTube Data API/search/statistics use direct aiohttp/API requests.
- yt-dlp media acquisition uses `YOUTUBE_PROXY`, currently a SOCKS5 route via
  `127.0.0.1:1090` in the active service environment.
- `atlas/egress.py` supports a list of egresses and a `direct` sentinel.
- Cookies, Deno/EJS, PoToken, and impersonation support are configured in the
  media layer.
- API keys are divided among hunting, tracking, and archeology rings.

### Failure domains that must remain distinct

| Failure | Correct ownership/response |
| --- | --- |
| API quota exhaustion | Rotate/rate-limit key ring; cooldown workload |
| Invalid/revoked API key | Quarantine key; alert separately |
| HTTP 429 | Treat as a temporary-throttling or quota signal; classify using response context |
| HTTP 401 | Auth/key failure, not quota by default |
| HTTP 403 | Inspect body/reason; do not equate every 403 with quota |
| yt-dlp anti-bot/PoToken | Media acquisition strategy and cookies/runtime |
| Datacenter-IP blocking | Egress/proxy strategy |
| Proxy tunnel unavailable | Egress health and fallback policy |

Current `ResiliencyExecutor` broadly treats 403 as quota-like, while Atlas
documentation claims bare 403 is excluded. This needs a typed error taxonomy
and tests, but production credentials and proxy behavior are explicitly out of
scope for the first campaign.

## 10. Capability and scope inventory

### Core capabilities worth preserving

- PostgreSQL `SKIP LOCKED` claiming;
- separate adaptive watchlist and video retention;
- batched vault commits;
- captions-first transcript acquisition;
- typed media failures and bounded retries;
- bounded concurrency tuned for the small VPS;
- raw reclaim join barrier;
- quality filtering and API key workload rings;
- manual full-video archival as an optional capability;
- local unit-test seams around batch agents/storage.

### Supporting capabilities

- Heartbeat and Discord notifications;
- systemd resource caps;
- Janitor stats/transcript/archive routines;
- local artifact cache for MCP;
- integration safety guard (`alkyone`).

### Experimental, transitional, or out-of-scope surfaces

- MCP production-stack dependency shape;
- `channel_history` and historical `wiki_topics` graph proposals; the current
  video schema preserves only `category_id`;
- Prefect deployment schedules and queues as a live execution source (the
  declarations remain only as rollback adapters);
- training/HF dashboard/legacy ML surfaces for ingestion;
- multiple compatibility wrappers for old agent entry points.

The product does not require nine Prefect deployments, nine stylistically
uniform agent classes, a Repository for every operation, a graph database, or a
universal multi-platform abstraction.

## 11. Architecture history

The history shows incremental engineering rather than one failed design:

- `2bed5ec`: moved database infrastructure into Atlas.
- `26b74de`: generic/Timescale adapter generation.
- `76ce300f`: Repository pattern.
- `9e928f6`: fan-out/per-step state migration.
- `a4762b7`: raw reclamation work.
- `3c88df3`: Atlas repositories and Maia producer/consumer checkpoint.
- `056f4af`: partial `BaseBatchAgent` and module decomposition.
- `ecf2ea9`: dead-code removal.
- `b36adcf`: MCP introduced as a separate product.
- `5122ad9`: adaptive watchlist, key-pool, and vault migration work.
- `ab0295f`: current WIP; adaptive scheduling, Prefect refactor, MCP polish,
  and deletion of legacy ML workflows/tests.

The main accidental complexity came from keeping compatibility seams alive while
adding the next generation. The code is not uniformly bad; it is unfinished
convergence.

## 12. Technical debt grouped by cause

The following debt inventory describes the audit baseline. The first campaign
removed the live scheduler's Prefect availability dependency and the duplicate
cadence source; the remaining items are intentionally deferred.

### Multiple sources of truth at baseline

- Prefect schedules plus baseline `CycleSpec` intervals plus systemd process
  lifetime.
- Prefect flow state plus PostgreSQL stage state.
- Status, booleans, step phases, and generated frontier.
- Former active `prefect-orchestrator` versus stale `prefect-worker` operational names.

### Incomplete boundary migrations

- Repository mixins cross SQL and vault responsibilities.
- Scribe stages transcripts while other stages write vault directly.
- `BaseBatchAgent` covers only some agents.
- MCP reuses Maia internals rather than a deliberate shared core.

### Historical leftovers

- likely-unused `channel_history` and `audio_pending` paths; historical docs
  mention `wiki_topics`, but it is not in the current source schema.
- Legacy ML/dashboard surfaces unrelated to current ingestion.
- Stale docs, rollback scripts, tests, and deployment setup.

### Operational coupling at baseline

- Direct Prefect invocation required the remote API before user code started;
  the plain scheduler path now avoids this dependency.
- Global settings/database/vault singletons complicate standalone tests and MCP.
- HF vault retries use blocking sleeps in synchronous provider methods.
- Local JSON quota state is not evidently atomic.

### Error-domain compression

- Bare API 403 and quota exhaustion are conflated.
- yt-dlp failures, proxy failures, and API failures are not represented by one
  stable public error taxonomy.

## 13. Documentation audit

Documentation is valuable historical evidence. The primary deployment and
orchestrator documents were corrected during the first campaign, but older
runbooks and package READMEs remain historical evidence rather than an
automatic statement of runtime truth.

### Current or useful documents

- `README.md`, `docs/deploy.md`, `docs/micro-prefect-orchestration.md`, and
  `docs/implementation-checklists/orchestrator-contract.md`: current scheduler
  direction and rollback posture after the first campaign.
- `docs/implementation-checklists/orchestrator-contract.md`: records recent
  in-process execution decisions.
- `docs/implementation-checklists/adaptive-scheduling.md`: explains the
  watchlist/tracker design.
- `docs/implementation-checklists/worklog-item7.md`: explicitly identifies
  service-name drift.
- `docs/tiered-storage.md` and vault migration documents: useful historical
  storage/migration evidence.

### Stale or contradictory documents

- `docs/vault-migration.md` and older operational notes retain historical
  `prefect-worker` language or migration assumptions; they need a separate
  documentation pass.
- `maia/README.md` assigns transcription to Singer and normalized audio to
  Streamer, contrary to current flow code.
- `atlas/README.md` refers to older executor terminology and overstates clean
  repository/vault separation.
- `tools/setup_orchestration.py` and some historical deployment snippets still
  need review for service-name drift.

The remaining stale documents should be corrected as part of the deployment
and package-metadata work, after the production scheduler has been observed
and its rollback posture is known.

## 14. What is worth preserving

Preserve the product/data-engineering ideas that buy real behavior:

- PostgreSQL as durable work truth;
- row-locking and bounded claims;
- separate watchlist retention from video archival;
- independent fan-out stages;
- idempotent vault paths and batched commits;
- raw reclaim join barrier;
- captions-first transcription;
- key-ring isolation and workload cooldowns;
- typed media failures;
- systemd CPU/memory caps;
- optional Muralist rather than forcing full-video archival into every run;
- package-local tests and Alkyone production safety checks.

## 15. What should probably die

Subject to caller and production verification, the following concepts have no
current product requirement:

- inactive Prefect worker/queue scheduling as a source of live execution truth;
- duplicate deployment schedules after the in-process scheduler is proven;
- Prefect zombie queue-slot cleanup as pipeline recovery once rollback use is
  retired;
- schema-only-looking `channel_history` and `audio_pending` remnants, after
  live schema/history verification;
- broad generic Repository-per-operation expansion;
- old ML/dashboard paths in the ingestion architecture;
- MCP's requirement on the complete production stack;
- stale worker service names in runbooks and deployment automation;
- legacy wrapper layers after callers are migrated.

Deletion must follow caller proof and, for schema objects, a separate migration
with backup/rollback.

## 16. Reusable YouTube core

The target shared core should be a small package/module with explicit, plain
interfaces:

```text
youtube_core/
  models.py          normalized video/channel/search/topic fields
  api.py             search, videos, channels, topicDetails clients
  errors.py          quota/auth/transient/blocked classifications
  keys.py            key-ring rotation and cooldown policy
  media.py           yt-dlp acquisition and metadata
  captions.py        captions client cascade
  frames.py          stream/frame primitives
  audio.py           ffmpeg normalization/chunking primitives
  egress.py          explicit proxy policy
```

It must not import PostgreSQL, Vault, Prefect, production queues, retention
policy, or deployment configuration.

The ingestion application can adapt these operations to PostgreSQL claims and
vault writes. MCP can use them directly with a local artifact sink. Shared
Models should preserve YouTube IDs, channel IDs, category IDs, and the raw
topic fields returned by the API, including `topicCategories` Wikipedia URLs
when available. The deprecated `topicDetails.topicIds` and
`relevantTopicIds` fields should not be treated as a durable identity contract.
Do this without prematurely designing a universal multi-platform framework.

## 17. Search MCP architecture

The intended target is:

```text
pleiades-mcp
  ├── MCP transport/tool definitions
  ├── local artifact cache
  ├── optional summary/STT dependencies
  └── youtube-core dependency

production ingestion
  ├── PostgreSQL work/state adapter
  ├── cold-vault adapter
  ├── scheduler/orchestration adapter
  └── youtube-core dependency
```

MCP should not require `DATABASE_URL`, production HF credentials, Prefect, or
Maia flow modules. Search and inspection are interactive and should fail or
back off per request without modifying production queue state.

The extraction should be incremental: first move plain API/media functions and
normalized models behind a neutral module, then change MCP imports, then adapt
ingestion callers. Do not build a general plugin framework for other video
platforms yet.

## 18. Knowledge-graph evolution

Current topic support is not an active graph: the live model/schema preserves
only `category_id`; it has no `topicDetails`, Wikipedia-concept field, or explicit
search-discovery edge. Search terms and snippet tags represent discovery behavior,
not semantic edges.
The current YouTube API documentation marks `topicDetails.topicIds` and
`relevantTopicIds` deprecated; `topicDetails.topicCategories` (Wikipedia URLs)
is the reliable topic list to preserve when returned:
<https://developers.google.com/youtube/v3/docs/videos>.

Recommended sequence:

1. Add/fetch YouTube `topicDetails` only when the product has a real consumer.
2. Preserve the raw topic fields returned by the API and
   `topicCategories`/Wikipedia URLs in normalized models or an explicit
   artifact/metadata payload; do not build key integrity around deprecated
   topic ID fields.
3. Add ordinary PostgreSQL tables when relationships become useful:

   ```text
   topics(topic_key, source, canonical_url, label, ...)
   video_topics(video_id, topic_key, observed_at, source)
   search_discoveries(search_id/term, video_id, discovered_at, ...)
   ```

4. Add channel/topic/concept edges only after query patterns are known.
5. Consider a graph database only if traversal workload or scale materially
   exceeds ordinary indexed PostgreSQL edges.

No graph database should be introduced now.

## 19. Architecture alternatives

| Option | Throughput/resource fit | Recovery/state | Operations/testing | Prefect | Judgment |
| --- | --- | --- | --- | --- | --- |
| Conservative cleanup: keep decorated flows, deployment schedules, and current repositories | Similar throughput; still pays remote-control-plane and framework overhead | PostgreSQL remains durable but dual truth persists | Low migration risk; continued schedule/service drift | Remains critical and confusing | Reject as target |
| Moderate simplification: plain operations, one systemd scheduler, optional Prefect adapter, retained hot/cold | Best fit for 2 CPU/12 GB; preserves current fan-out and bounded concurrency | DB remains authoritative; restart behavior is explicit; at-least-once semantics can be tested | Moderate migration; plain functions are easier to test | Gradually reduced to telemetry/rollback | Recommended |
| Strong immediate removal: remove Prefect, normalize schema, split shared core, rewrite packaging now | Simplest eventual shape | Potentially strongest end state | High simultaneous schema, package, and production risk | Removed immediately | Reject for this campaign |

The recommendation synthesizes the moderate option. The likely endpoint is no
Prefect dependency in the critical ingestion path, but that endpoint should be
earned through a staged migration rather than assumed.

## 20. Recommended target architecture

```text
systemd: pleiades-scheduler
        │ one cadence source; restartable; no remote control-plane requirement
        ▼
plain application cycles
  discover()       → PostgreSQL search_queue/videos/watchlist
  fetch_raw()      → cold vault + PostgreSQL marker
  normalize_audio()→ cold vault + PostgreSQL marker
  extract_frames() → cold vault + PostgreSQL marker
  transcribe()     → cold vault/DB pointer + PostgreSQL marker
  track_stats()    → PostgreSQL hot stats
  janitor()        → explicit cold-write/archive boundary
        │
  ├── optional Prefect adapter for flow-level telemetry/temporary rollback
        └── thin MCP → shared YouTube core → local artifact sink

PostgreSQL: durable work claims, canonical stage state, hot metadata/queues
Vault: large artifacts and historical data, explicit artifact manifest
```

Target principles:

- domain operations are ordinary async Python functions;
- orchestration adapters do not contain business logic;
- PostgreSQL is the only durable work-state authority;
- stage operations are idempotent and at-least-once safe;
- all network and CPU concurrency is explicit and tuned to the host;
- cold writes have one clear ownership contract;
- MCP has no production-state dependency;
- topic identifiers are preserved without a graph platform.

## 21. Keep / Simplify / Remove

| Keep | Simplify | Remove/defer |
| --- | --- | --- |
| PostgreSQL claims and watchlist | Prefect to optional adapter | Live Prefect deployment schedules after migration |
| Hot/cold storage principle | Stage state to one canonical model | Worker/queue/zombie concepts no longer used |
| Fan-out media stages | `VideoRepository` responsibility boundaries | Schema-only-looking topic/history/audio remnants after live verification |
| Key rings and typed media errors | Shared YouTube/media core | MCP dependency on Atlas[all]+Maia |
| Vault batching and raw-reclaim barrier | Agent/flow wrappers | Graph database and universal platform abstractions |
| Muralist as optional capability | Documentation/service naming | Unrelated legacy ML ingestion surfaces |

## 22. Safe migration boundaries

### Safe first boundaries

- Extract plain application operations without changing SQL/schema.
- Add a systemd scheduler path that can run those operations without Prefect.
- Keep current decorated flows as thin adapters until validation is complete.
- Add focused tests for operation contracts, idempotence, and scheduler failure.
- Extract YouTube models/errors/API/media functions without moving artifact data.
- Make MCP depend on the extracted core and local sinks.
- Keep runbooks/CD service names aligned with the confirmed active unit.

### Production-sensitive boundaries requiring manual planning

- Any removal/rename of state columns or schema tables.
- Converting claims to leases or changing `PROCESSING` recovery semantics.
- Changing raw reclaim conditions or TTL.
- Vault repository migration, retention, or bulk deletion.
- API key/cookie/proxy credentials and egress policy.
- Prefect deployment deletion or remote database migration.

### Do not touch yet

- live state columns;
- vault layout and migration data;
- raw reclaim join barrier;
- watchlist/tracker separation;
- production credentials or proxy routing;
- graph database;
- bulk legacy deletion without caller proof.

## 23. Final verdict

### What should Pleiades be?

A PostgreSQL-owned, restart-safe, at-least-once multimodal YouTube ingestion
service with bounded async workers, a clear hot/cold vault boundary, and a
small shared YouTube core. A thin Search MCP should consume that core without
carrying production infrastructure.

### Is the product thesis coherent?

Yes. Continuous discovery plus adaptive tracking, parallel media derivation,
and cold artifact retention is a coherent product for the stated hardware. The
current complexity is primarily unfinished convergence, not a fundamentally
wrong thesis.

### Where did most accidental complexity come from?

From retaining previous orchestration, state, repository, and packaging
generations while introducing new ones: Prefect worker deployments followed by
an in-process scheduler; booleans followed by phases; broad repositories;
partial agent-template migration; and MCP reuse through production modules.

### Should Prefect stay?

Replace gradually. Keep it temporarily as an optional telemetry/rollback
adapter. It should not own durable video recovery or remain a prerequisite for
the application operation to begin.

### What belongs in the shared ingestion/MCP core?

Normalized YouTube identifiers/models, search/video/channel/topic API clients,
typed error and key-rotation policy, yt-dlp/media acquisition, captions,
audio/frame primitives, and explicit egress policy. No database, vault,
Prefect, production queue, schedule, or retention policy.

### Is hot PostgreSQL plus cold vault sound?

Yes. Keep it. Clarify artifact ownership and pointers, then improve the cold
write boundary incrementally. Do not rewrite the layout during the current
vault migration.

### How should graph functionality evolve?

Preserve topic identifiers and Wikipedia URLs first. Use normal PostgreSQL
topic/video/search-discovery edges when there is a real query. Adopt a graph
database only after demonstrated traversal/scale needs.

### Cleanup or rewrite?

Cleanup and staged boundary extraction. A rewrite would discard working claims,
reclaim, key resilience, and storage behavior while creating unacceptable
operational risk.

### Three highest-leverage simplifications

1. Plain application operations plus one systemd scheduler; Prefect optional.
2. One canonical stage-state model after a production-safe migration.
3. A production-independent YouTube core and thin MCP.

### What should not be touched yet?

Live schema/state columns, vault migration/layout, raw reclaim barrier,
watchlist/tracker separation, credentials/proxy configuration, graph database,
and bulk legacy deletion.

## Implementation Follow-up

The bounded implementation campaign is complete. It implemented the
highest-value execution and validation simplifications without changing the
live PostgreSQL schema or durable work-state semantics. A production-observed
Hugging Face directory limit required one additional, backward-compatible
vault-path correction.

## What changed

- Added plain `*_operation` seams for eight scheduled capabilities plus the manual
  Archeologist operation, and made
  the single asyncio scheduler call them directly.
- Retained one thin, unscheduled Prefect flow adapter per capability for
  telemetry or rollback. Prefect no longer owns live cadence or durable
  recovery.
- Removed Prefect task/run-logger coupling from the application operations.
  Janitor and Heartbeat now treat Prefect as optional.
- Replaced ambiguous test entry points with three explicit tiers: hermetic
  `make check`, guarded isolated `make test-int`, and guarded read-only
  `make test-live`. Live checks now live only under Alkyone.
- Aligned installed and locked dependencies on Prefect 3.7.8, removed Prefect
  from Atlas, and removed the conflicting unused `yt-dlp-get-pot` package.
- Classified YouTube API errors so daily search quota, invalid keys, auth
  failures, throttling, and media/egress failures do not share one recovery
  action.
- Sharded new raw and metadata paths by video-ID prefix
  (`raw/<prefix>/...`, `meta/<prefix>/...`) after live logs proved the flat
  `/raw/` directory exceeded the Hugging Face 10,000-entry limit. Readers and
  reclaim retain legacy flat-path fallback; existing objects were not moved or
  deleted.
- Made Scribe temporary workspaces self-cleaning on success and failure.
- Aligned README, deployment, orchestration, testing, vault, and CD
  documentation with these boundaries.

## Changed boundaries

```text
systemd pleiades-ingestion --> maia.orchestrator --> plain operations
optional unscheduled Prefect adapters -------------> plain operations
                                                       |-- PostgreSQL claims/state
                                                       |-- vault/media stages
                                                       +-- media/network work
```

PostgreSQL remains the sole durable work-state owner. Prefect remains installed
temporarily because compatibility adapters and rollback tooling still exist.
The scheduler-seam refactor did not change durable state. The later follow-up
does tighten per-stage release/failure writes, transcript finalization, and
source-schema claim indexes; its live schema migration remains deferred. Proxy
routing and media outputs are unchanged, and legacy vault reads remain supported.

## Validation

- Canonical `make check`: 311 passed (Atlas 105, Maia 184, MCP 11, Alkyone
  guard 11), with Ruff check/format and mypy clean across all four packages.
- Focused storage tests cover new sharded writes, legacy metadata reads, and
  both new and legacy raw reclaim paths.
- Discord notification delivery now returns an explicit result, logs accepted
  deliveries at INFO, and is covered for HTTP 204, HTTP 429, and missing-hook
  behavior. Tracker warns on nondelivery and heartbeat reports `notified`.
- CLI parsing validates the consolidated batch-agent contract, including
  `maia streamer --batch-size`.
- `git diff --check` passed and no `.orig` or `.rej` artifacts remain.
- Guarded Alkyone read-only live preflight: 3 passed (PostgreSQL connectivity,
  Hugging Face repository access, and configuration/key presence). It consumes
  no YouTube API quota.
- Read-only production inspection confirmed PostgreSQL remains the durable
  work-state owner and the old service continued productive tracker/scribe
  cycles. It also confirmed raw uploads failed at the Hugging Face flat-folder
  limit before activation of the sharding change.
- All three Poetry manifests currently fail `poetry check` only because they
  duplicate PEP 621 and legacy `[tool.poetry]` metadata. Runtime dependency
  resolution and the canonical gate pass; metadata cleanup is deferred.
- An operator restart activated the plain-operation scheduler under the legacy
  local unit name `prefect-orchestrator`. The old process exceeded its 90-second
  stop timeout and systemd killed it; the new process stayed near 240-324 MB and
  9-13 tasks instead of the prior roughly 1.4 GB and 22 tasks.
- Live logs confirmed independent Hunter/Archeologist daily search-quota 429s are
  isolated and do not stop the executor. No Hugging Face flat-folder rejection
  recurred during the observation window.
- Live Scribe fallback exposed one remaining hard `bestaudio` selector. The
  corrected `bestaudio/best` command produced a 3,173,596-byte Opus artifact
  for an exact previously failing video through the configured proxy.
- An explicitly authorized live Discord OPS heartbeat was accepted. It
  reported the executor healthy, no down/degraded units, current tracker and
  pipeline counts, and the expected rate-limited status.
- The executor handoff to the versioned `pleiades-ingestion.service` completed
  on 2026-09-01. The replacement is active/enabled; legacy
  `prefect-orchestrator` and `prefect-worker` are inactive/disabled. The unit
  explicitly orders the local micro-backed `youtube-proxy` tunnel and PoToken
  provider, applies 1.8-CPU/4-GiB cgroup backstops, and loads the final logging
  and media fallback changes.
- Post-handoff live cycles confirmed tracker updates and accepted Discord
  surveillance deliveries, an accepted OPS heartbeat, frame extraction,
  janitor archival, audio chunk extraction, and proxied media downloads.
  Streamer then committed five raw objects plus five metadata objects in one HF
  transaction; PostgreSQL recorded all five as `raw_phase=DONE` with new
  `raw/<prefix>/<video>.mp4` pointers. The micro Prefect health endpoint
  returned healthy and a quota-free YouTube probe through the SOCKS tunnel
  returned HTTP 204. Search-only Hunter and Archeologist cycles remained
  isolated on their known daily-quota 429s.
- The guarded Alkyone live preflight was repeated after the handoff: all three
  read-only checks passed.
- The real `.env` has malformed lines 15 and 85, producing repeated
  python-dotenv warnings. Credential-bearing configuration was intentionally
  not rewritten.
- Singer now materializes raw media directly to a temporary path through a
  backend-specific vault API: Hugging Face copies its cached file and GCS
  downloads to a filename, avoiding a full in-memory raw object. It processes
  sequentially and commits at four-video boundaries, making earlier work durable
  while remaining within the account-wide 128-commit/hour ceiling.
- Retry transitions are now stage-specific (`raw`, `audio`, `visuals`,
  `transcript`, `clip`). Downstream retries no longer reset `raw_phase` and
  silently flip `fetched` false. Release and permanent-failure writes also
  require that their own phase is still `PROCESSING`, so a stale worker cannot
  overwrite a newer completion. Live inspection found 1,584 pre-existing rows
  with `fetched=false` and a non-NULL `raw_uri`; repairing them remains a
  production-sensitive data migration.
- Transcript finalization is one atomic PostgreSQL statement and clears hot JSON
  only when a non-empty cold URI exists. This fixes future duplication; the
  existing 58,964 duplicated payloads remain untouched pending verification.
- Live Streamer/Singer claim-index predicates differ from `schema.sql`; both had
  zero scans and Singer used a sequential scan at roughly 82k videos. The
  repository now provides `deploy/sql/20260902_claim_indexes_v2.sql` and its
  rollback script. It creates corrected v2 indexes concurrently while retaining
  the old names for rollback; it was intentionally not executed against the
  production database in this campaign.
- A read-only production check verified all 9,103 non-NULL `raw_uri` values use
  the repository-relative `raw/...` invariant; none are provider-qualified or
  anomalous.
- A pre-activation read-only health window found the old service active with zero
  restarts: 9 Streamer, 3 Singer, 8 Painter, and 58 Tracker cycles in one hour
  (about 45 raw, 30 audio, and 40 frame completions). Recent Tracker, OPS, and
  quota alerts were accepted by Discord.
- At 04:52:30 UTC the old process reached the unit's 4 GiB memory limit and was
  OOM-killed; systemd restarted it automatically at 04:52:46 and thereby loaded
  the final worktree. The replacement completed 8 Streamer, 2 Singer, 5 Painter,
  and 57 Tracker cycles during the observation window, with 57 Watch and 5 OPS
  Discord deliveries. Singer logged sequential processing, a four-video commit
  (24 chunk files), a final one-video commit (5 files), and clean completion.
  Its process high-water mark was about 2.71 GiB and later RSS about 614 MiB;
  cgroup usage was about 2.56 GiB with one active FFmpeg child. The guarded
  PostgreSQL/Hugging Face/configuration live preflight again passed 3/3.

## Intentionally deferred

- Live schema/state and lease migration.
- Remote Prefect deployment/control-plane retirement.
- Existing vault-object migration or deletion; audio-path sharding and an
  artifact manifest remain follow-up work.
- Live claim-index replacement, transcript-content backfill, and stale
  raw-pointer reconciliation.
- Shared YouTube-core package extraction.
- Production credential, proxy, cookie, and PoToken changes.
---

## Tracker migration addendum (2026-09-02)

The preceding audit describes the pre-migration live schema. The approved
follow-up is `deploy/sql/20260902_tracker_durable_state.sql`, with a paired
safe rollback. It keeps `watchlist` independent of `videos` and adds durable
previous counters (`last_views`, `last_likes`, `last_comment_count`) plus a
non-negative consecutive omission count.

The intended state policy is:

- available videos older than seven days remain `WEEKLY` forever, with fresh
  velocity allowed to promote them;
- three consecutive API omissions move a row to `DORMANT`, retained for a
  monthly recheck; a successful response resets the count;
- recent `video_stats_log` samples remain hot until Janitor verifies their
  cold archival;
- Tracker uses `videos.list` batches of at most 50 IDs; discovery is capped
  at one Hunter search every 20 minutes and scheduled Archeologist runs stay
  disabled.

A one-time live inspection observed 1,319 database videos absent from the
watchlist. That is an audit observation only. The migration's commented backfill is generic, covers every missing watchlist
row, and is staggered within each row's assigned cadence interval.
Counters missed during the broken-tracker interval cannot be reconstructed
from YouTube; enrollment establishes a new baseline.

The first production attempt tried to derive a baseline for every existing row
from the full stats history. That query saturated one CPU for more than twelve
minutes, was cancelled, and rolled back cleanly. The migration was corrected to
make existing rows establish a baseline at their next normal observation. The
corrected additive migration then committed in about ten seconds.

### Tracker implementation validation and activation state

The durable Tracker code, tests, source schema, migration, and rollback are
implemented locally. Validation passed 325 hermetic tests plus Atlas/Maia strict
type checks and all package lint gates. A guarded read-only Alkyone production
preflight passed without consuming YouTube quota.

Production activation completed on 2026-09-02. The migration added the durable
counters and omission count with validated constraints; all 1,320 missing videos
were inserted without overwriting existing watchlist state and staggered across
their weekly cadence. `pleiades-ingestion.service` then started with eight
scheduled operations, Hunter at one search per 20 minutes, Tracker at 50 IDs per
minute, and no scheduled Archeologist.

The first Tracker cycle fetched 50 rows, persisted 48 available observations and
two unavailable observations atomically, and delivered the revised Discord Watch
summary. Subsequent 50/50 cycles and Discord deliveries also succeeded. Hunter's
first scheduled search received the already-known project daily-quota HTTP 429;
that failure remained isolated from media processing and tracking. No additional
discovery smoke request was made.

## 24. Full post-activation audit (2026-09-02)

This section supersedes stale current-state statements in the historical
baseline above. It combines targeted source/history review, structural searches,
325 hermetic tests, a guarded three-check live preflight, read-only production
queries, service logs, and direct checks of the current Prefect workspace.

### Current live system

```text
small Oracle VPS                         executor Oracle VPS (2 CPU / ~12 GiB)
┌──────────────────────────┐            ┌──────────────────────────────────┐
│ optional Prefect API     │◀ telemetry │ pleiades-ingestion.service       │
│ SOCKS exit endpoint      │◀───────────│  eight asyncio operation loops   │
└──────────────────────────┘            │  Hunter → PostgreSQL videos      │
                                        │  Tracker → watchlist + stats     │
                                        │  media stages → HF vault         │
                                        │  Janitor → verified cold reclaim │
                                        │  Heartbeat/Tracker → Discord     │
                                        └───────────┬──────────────┬───────┘
                                                    │              │
                                              PostgreSQL      HF cold vault
                                              durable/hot     large/history
```

The executor, local proxy tunnel, and PoToken provider are active. The separate
small-server Prefect API was queried read-only and contained zero deployments;
there is therefore no current duplicate Prefect schedule. Official Prefect 3
documentation confirms that deployments and workers provide remote scheduling
and infrastructure execution, while deployment schedules must be explicitly
paused when they exist. Those capabilities are not on Pleiades' live critical
path today ([deployments](https://docs.prefect.io/v3/concepts/deployments),
[schedule management](https://docs.prefect.io/v3/how-to-guides/deployments/manage-schedules)).

### Live data and throughput facts

| Observation | Verified result |
|---|---|
| Video/watchlist enrollment | 82,869 / 82,869; no enrollment gap after backfill |
| Tracker contract | durable counters, omission count, DORMANT constraint, due index present |
| Hot stats | 245,623 rows spanning approximately seven days; 77,376 videos |
| Transcript pointers | 59,203 of 59,208 rows have a cold URI |
| Duplicate hot transcript payload | 58,976 rows still retain content after receiving a cold URI |
| Artifacts | 8,099 fetched, 2,402 audio, 995 visuals, zero optional full-video artifacts |
| Observed media rate | roughly 45–55 raw, 30–50 audio, and 40 frame completions/hour in measured windows |
| Discovery | intentionally slow; currently blocked by a distinct project daily-quota 429 |
| Prefect workspace | zero deployments; systemd is the sole live scheduler |

The application is working: tracking, raw/media processing, cold-stat archival,
and Discord reporting continue. It is not fully healthy: discovery is quota
limited, media backlogs are measured in days, and the reliability issues below
remain.

### Requirements decision

| Class | Requirement |
|---|---|
| MUST HAVE | PostgreSQL-durable work and tracker state; restart recovery; bounded CPU/RAM; explicit API/quota/egress errors; idempotent hot-to-cold movement; observable Discord/service health |
| SHOULD HAVE | Expiring leases for parallel workers; project-level quota ledger; production-independent YouTube core; authenticated MCP artifact delivery; queryable cold manifests |
| NICE TO HAVE | Optional Prefect telemetry, richer historical analytics, PostgreSQL topic/concept edges, manually budgeted historical discovery |
| NOT ACTUALLY REQUIRED | Prefect scheduling, nine deployed agent processes, permanent full-video storage, a graph database, or a universal multi-platform abstraction |

### Verified risk register

| Severity | Finding | Classification and impact |
|---|---|---|
| P0 latent | `enforce_table_row_limit` deletes by only the first primary-key column | MITIGATED IN SOURCE. No caller exists, the helper is removed from the canonical schema, and a versioned drop migration is prepared. The latent live-schema risk remains until that migration is reviewed and applied. |
| P1 | Stage, watchlist, search, and transcript claims are not durable leases | VERIFIED. Database locks end when repository calls return. Stage queries can reclaim `PROCESSING` rows without owner/expiry/phase guards, duplicating expensive work if runners overlap. The sole current scheduler reduces but does not remove the risk. |
| P1 | Hunter advances search pagination after individual ingestion failures | RESOLVED IN WORKTREE. All accepted items now settle; cancellation propagates, database failures prevent acknowledgement, and acknowledgement failures propagate. Partial-failure and ack-failure tests prove replay behavior. Activation awaits a future service restart. |
| P1 | MCP reserve key-pool construction omits required session dictionaries | RESOLVED AND TESTED. `KeyRing.from_keys` performs complete initialization and the strategy accepts normal dependency injection; tests cover sessions, rotation, dead keys, and exhaustion. |
| P1 | Manual Archeologist has an unbounded-by-budget historical default | VERIFIED. The 2005–2024 × five-category default can attempt about 1,200 expensive `search.list` calls. It is unscheduled but still callable. |
| P2 | Cold stats upload/delete is not crash-idempotent and has no application reader | VERIFIED. A crash after unique-object upload and before hot deletion can duplicate history; requested rather than actual delete count is reported. |
| P2 | Vault fetch errors collapse outage/auth/not-found into `None`/`False` | VERIFIED. Singer can permanently fail audio when the vault is temporarily unavailable. |
| P2 | Media error heuristics label generic unavailable/extraction errors as rate limits | VERIFIED. Private, deleted, geo-blocked, and malformed inputs can retry indefinitely. |
| P2 | Orchestrator and `BaseBatchAgent` under-report cycle failures | VERIFIED. Loops catch failures and sleep at fixed cadence; ordinary item exceptions are excluded from stored results but the claimed count is reported as processed. |
| P2 | Proxy/quota policy is incomplete | VERIFIED. Media uses the egress pool, but Data API and Shorts probes can use direct traffic; Docker documents `MAIA_PROXY_URL` although runtime reads `YOUTUBE_PROXY`; compliance mode is warning-only and there is no project-level daily hard budget. |
| P2 | MCP public artifact sidecar has no application authentication | VERIFIED. The supplied launcher binds MCP and static cached artifacts to all interfaces. Safe only behind an authenticated network/reverse proxy. |
| P3 | Lifecycle and rollback documents contain historical claims presented as current | VERIFIED. Adaptive/tiered-storage deletion claims and some worker-era runbooks need explicit historical banners or correction. |

Primary evidence anchors: `atlas/src/atlas/schema.sql` and
`deploy/sql/20260902_retire_unsafe_row_limit.sql` (unsafe generic retention);
`atlas/src/atlas/adapters/__init__.py:63-92` plus
`repositories/video/state_machine.py:12-122`, `repositories/watchlist.py:35-51`,
and `repositories/search_queue.py:18-35` (claim lifetime); Hunter
`flow.py:198-247` (acknowledgement); `mcp/media.py:89-99` and
`atlas/utils.py:113-168` (reserve ring); Archeologist `flow.py:108-140`;
Janitor `janitor.py:243-307`; vault fetches `vault.py:336-371,533-563`;
`maia/base.py:79-117`; `maia/orchestrator.py:68-104`; and
`mcp/run_server_full.sh:18-30`. Each reported Python issue was screened against
intentional patterns and verified in its caller context; the optional
`review-verification-protocol` skill referenced by the Python-review skill was
not installed.

The real environment and cookie files were tightened from mode 664 to 600
without changing values. Two malformed dotenv lines remain a configuration
hygiene issue; credential history and external vault duplication are UNKNOWN.

### Updated verdict

Keep the product thesis and the present target architecture. Prefer cleanup over
rewrite. PostgreSQL plus a cold object vault remains the correct storage split,
but cold writes need a deterministic manifest/read contract before more hot data
is reclaimed. Prefect should continue leaving the live path and may be removed
after the thin adapters and rollback window are no longer useful; do not remove
the second VPS's independent proxy role with it.

The next three leverage points are now: (1) add durable owner/expiry leases;
(2) make cold archival idempotent and queryable; and (3) extract a small YouTube
core after resolving the MCP public-artifact threat model. Do not yet bulk-move
vault objects, collapse stage columns,
repair production rows without a batched rollback plan, add a graph database, or
increase discovery rate.

## 25. No-quota reliability follow-up (2026-09-02)

This bounded campaign fixed two verified failure paths without consuming
YouTube quota or mutating production:

- `KeyRing.from_keys` is now the supported explicit-key construction path. The
  MCP builds its reserve ring through that API and injects it into a normally
  initialized `YouTubeSearchStrategy`; partial `__new__` construction is gone.
- Hunter waits for every accepted database write, preserves successful
  idempotent writes, and refuses to acknowledge the search page if any write
  fails. Cancellation and acknowledgement failures also propagate.
- The unused, composite-key-unsafe `enforce_table_row_limit` helper was removed
  from the canonical schema. `deploy/sql/20260902_retire_unsafe_row_limit.sql`
  retires an existing live function, but has not been applied.

Validation passed the canonical `make check` gate: 115 Atlas, 187 Maia, 12 MCP,
and 11 Alkyone tests (325 total), plus Ruff lint/format and strict package type
checks. During that code-validation campaign, no live YouTube/API smoke cycle,
database migration, service restart, or external-state mutation was performed.
Therefore Hunter's new semantics become live only on the next controlled
application restart, and the unsafe helper may remain in the live database until
its migration is applied. The separately authorized proxy recovery is recorded
below.

A final read-only health snapshot found `pleiades-ingestion.service` active with
zero restarts and about 730 MiB resident memory. Tracker commits and Discord
Watch delivery were succeeding, but the expected `127.0.0.1:1090` SOCKS listener
was absent. Every sampled Streamer item was consequently released after a local
connection refusal. The cached `youtube-proxy.service` had been stopped at
14:33 UTC and its `/etc/systemd/system` file was missing; the micro VPS remained
reachable by no-op SSH. After explicit authorization, the validated replacement at
`deploy/systemd/youtube-proxy.service` was installed and enabled at 19:15 UTC.
The listener, systemd state, and a micro-VPS request through SOCKS succeeded.
The next normal Streamer cycle fetched 37.7 MB for its first item through the
restored route, while Tracker and Discord delivery continued. No separate
YouTube discovery or quota smoke request was made.

