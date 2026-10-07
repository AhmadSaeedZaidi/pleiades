# October audit and cleanup

2026-10-04 · branch `september-audit` · audited the existing dirty working tree.
Prior uncommitted pipeline changes were retained. The initial cleanup phase did
not deploy pipeline changes; the separately authorized storage follow-up is recorded below. During that initial phase, the existing ingestion service was not restarted or stopped,
and no live database/vault mutations, extraction jobs, notifications, or paid
provider calls were performed by this audit.

## Runtime observations

The active process is `maia.orchestrator` under `pleiades-ingestion.service`.
The initial systemd observation showed an active/running service and four
historical restarts. A bounded read-only PostgreSQL observation found roughly
94,000 collected videos, a populated durable watchlist, and recent Janitor events.
The existing MCP listener uses port 8000; the new dashboard uses loopback 8080.
Counts change as normal ingestion continues.

Initial live database probes used PostgreSQL read-only sessions and statement
timeouts. No Alkyone integration or live-mutation test was run during that phase.

## Changes

- Removed the discontinued `training/` and `hf-spaces/` prediction/dashboard stack,
  its seven ML test modules, and four training workflows. Runtime import searches
  found no consumers in Atlas, Maia, MCP, Alkyone, or the active scheduler. The
  removed data loaders referenced absent legacy tables and the workflows pulled
  images from a different repository.
- Removed obsolete scratch documents, item worklogs, and a committed migration
  run log. Current guides now cover the actual plain-operation scheduler.
  Historical audits/challenges/migration records are explicitly labeled.
- Replaced two stale repository skills with focused current instructions and
  made them versionable. Removed repository copies of generic deployment,
  ML-pipeline, and PostgreSQL skills; global skills remain untouched.
- Corrected root test discovery and centralized database isolation. The missing
  event mock in the Atlas archival safety test caused about 30 seconds of database
  connection attempts despite passing; it now verifies mocked failure reporting.
  MCP unit configuration overrides inherited credentials with dummy values.
- Removed three unused generic state methods and their implementation-mirror
  tests after checking callers. Retained `mark_done` and compatibility flows
  because real integration/Prefect consumers still use them.
- Added five regressions reproducing lifecycle demotion on stage retry, then
  consolidated the release methods. Retry now updates only the current stage,
  retains its PROCESSING guard, preserves lifecycle status, and excludes archived
  rows. This source fix requires a reviewed ingestion deployment to become live.
- Added the independent read-only web interface with overview, discovery chart,
  video search/filter/pagination, inspector and staged transcript preview,
  discovery queue, activity, service observation, token access and demo mode.
- Added repository/API tests for bounds, parameterization, access control,
  redaction, caching, read-only pool configuration, and database isolation.
- Added the dashboard and current-doc link checks to the quality gate, validated
  JavaScript syntax, removed duplicate pre-push type-check hooks, and added a CI
  production-image build. Documentation changes now run CI too.
- Deployment checks out the CI SHA for smoke, refuses a dirty target checkout,
  and runs `make check` before restart. It no longer provisions unnecessary
  Prefect deployments. Runtime images default to the single scheduler and use
  Hugging Face extras instead of installing unused GCS dependencies.
- Removed secret prefixes from the manual API diagnostic output, compacted
  tracked credential documentation, and ignored downloaded media/transcripts.
- Made Maia's convenience exports lazy, preserving callers while metadata
  imports avoid initializing Atlas/Prefect. Removed image credential placeholders.
- Replaced configuration validation that renamed `.env` and spawned a subprocess
  with isolated Settings construction; removed the unused environment fixture.
  Empty unit-test package markers were removed to make root discovery work.

Deleted/replaced pre-existing files are recoverable under
`/tmp/pleiades-cleanup-20261004`; the initial tracked diff is saved there.
No unrelated code changes were reset or committed.

## Verification

Baseline: 379 unit tests passed; all component lint/type gates passed. The
baseline Atlas suite took about 32 seconds, dominated by the unmocked DB attempt.
After test isolation it completes in roughly two seconds on this machine.

Dashboard live HTTP checks returned success for overview, videos, events, and
queries. Chromium exercised all four views, empty search, failed-stage filtering,
and video inspection. Desktop and 390-pixel mobile layouts were inspected; no
browser JavaScript errors or document overflow were found. Previews are local
artifacts, not additional repository assets.

Final validation passed:

| Check | Result |
| --- | --- |
| `make check` | Passed: lint, formatting, types, 406 unit tests, JS syntax, doc links |
| Root `pytest` | 406 passed |
| Current documentation links | 22 guides checked |
| Repository skills | Both validated with skill-creator's validator |
| Production Docker build | Built `pleiades-pipeline:october-audit` locally; no push |
| Container smoke | Metadata import passed with network disabled and bounded resources |
| Dashboard/MCP wheels | Built; dashboard wheel contains all frontend assets |
| Browser | Four views, search, stage filter, inspector, responsive layout passed |

The installed FastAPI/Starlette test client emits one upstream deprecation warning;
tests pass without upgrading the running service's environment. No GitHub workflow
was dispatched. Ingestion retained PID 3464677 and its existing four restarts at
the final observation. The dashboard is a separate local process on port 8080.

## Remaining findings requiring coordinated work

1. **Durable ownership:** stage claims have no owner/expiry lease that survives
   transaction completion. Concurrent executors can duplicate work. Do not start
   Compose/Prefect workers alongside the systemd scheduler on the same database.
2. **Source/runtime drift:** live schema and already-imported Python code can
   differ from the working tree. Production schema/index changes need reviewed
   migrations, backups, and a separate deployment; cleanup does not apply them.
3. **Phase normalization:** redundant flags/phases and lifecycle/archive state
   still require coordinated schema/repository changes. Dashboard coverage is hot
   stage state, not a lifetime inventory of vaulted artifacts.
4. **MCP boundary:** optional MCP imports still depend on pipeline configuration.
   Extracting a lightweight YouTube core will remove unnecessary database/vault
   configuration and Prefect dependencies. Existing public MCP/artifact access
   requires an authenticated deployment; the new UI does not change that service.
5. **Legacy operational utilities:** bulk recovery/repaint/migration scripts
   remain maintenance tools, not routine health checks. Review their scopes,
   deletion ordering, credential handling and explicit opt-ins before use.
6. **Project quota and error taxonomy:** project-level API budgets and typed
   media/egress failures remain separate reliability work. Key rotation is not
   proof of independent quota.

Reference implementations use the official
[FastAPI lifespan documentation](https://fastapi.tiangolo.com/advanced/events/)
and [Psycopg connection documentation](https://www.psycopg.org/psycopg3/docs/api/connections.html).


## Authorized storage follow-up

The user requested hot/cold efficiency improvements and a reusable abstraction,
and explicitly allowed interrupting the hobby pipeline. The resulting package
is `pleiades-tiered-storage`, with no runtime dependencies or application imports.
It provides version-aware verified batch promotion, SQLite/filesystem adapters,
transparent reads, retention policy, immutable object paths, existing-copy reuse,
byte/record/concurrency bounds, cancellation handling, and concise failure results.
Atlas and Maia use the same engine for transcript and metadata handoffs.

The database observation found 57,564 transcript rows with both a pointer and
retained JSON. The transcript relation occupied 1,525,645,312 bytes including
TOAST/indexes. Three bounded read-only cold checks matched staged JSON exactly
under canonical JSON comparison; these checks cleared no hot data. Reconciliation
now prioritizes new work and then verifies/reuses retained cold copies before
clearing matching hot versions. Legacy object bodies stay intact; new index
paths and immutable bodies avoid overwriting finalized receipts.

The four additive indexes in `deploy/sql/20261004_tiered_storage_indexes.sql`
were applied concurrently to the live database and verified valid. The metric
retention query changed from sequential scan + sort to `idx_stats_retention`;
estimated top-level planner cost fell from 37,574.04 to 170.31. This is a planner
estimate, not a measured wall-time speedup. Indexes added about 37 MiB total.
No schema columns, retention policies, table rewrites, or manual data purges ran.

A local 5,000-row synthetic metric sample produced 82,828 bytes with Snappy and
38,005 bytes with Zstandard, a 54.1% reduction. Encoding timings were not compared
because the first call included library startup. New nullable-integer regressions
also verify values above 2**53 survive Parquet round trips. Completed manifest
retries skip reserialization, and metric deletion checks the selected row version.
Janitor processes at most eight metric batches per cycle and submits metadata
pages once instead of dividing each page into fixed 25-record commits. Dry runs
now skip mutation phases/events. Vault retry policy has one owner rather than a
second caller loop, and all vault operations share the isolated bounded executor.

Validation: canonical `make check` passed 441 unit tests; seven PostgreSQL
transaction/concurrency tests on a temporary separate UTF-8 PostgreSQL instance,
local Docker build, and package wheels. Existing unit DB protection remains
active; the separate storage integration suite uses the existing Alkyone guards
and no YouTube, remote-vault, or Prefect calls. The updated Poetry locks match
all three component manifests. The final standalone wheel was installed and its
SQLite/filesystem example executed in a clean environment without Atlas/Maia
or any third-party runtime packages.

Startup verification exposed a legacy metric compatibility issue: nullable
counters in older Parquet files were encoded as doubles, despite their original
integer batch identity. The failed handoff kept hot rows. Canonicalization now
recovers integral counters before checking the original digest; regressions
confirm rounded large integers still fail verification. A read-only check then
verified the existing 5,000-row cold batch without uploading it again.

Audio uploads also hit the flat directory limit. A bounded live tree inspection
found exactly 10,000 files and 1,922 subdirectories there. New audio and chunks
use `media/audio/{prefix}/{id}...`, leaving that full legacy directory untouched.
Reads and purge/migration helpers support both layouts. The restarted Singer
successfully committed two four-file audio batches.

Activation: the four indexes are live, and the reusable package was installed
without upgrading existing runtime dependencies. After systemd denied a normal
restart, the user explicitly authorized sudo. The final service restart completed
at 17:01:05 UTC on October 4, with PID 1399083, active/running and zero automatic
restarts. Its first cycle flushed 50 transcripts with no failures and verified,
archived, and purged 40,000 old metric rows in exactly eight bounded batches.
The preceding activation also flushed 50 transcripts. Reconciliation continues
gradually; this does not claim all retained payloads or physical table bloat
have been removed. The read-only dashboard still answers HTTP 200 on port 8080.


## Authorized hosting and topic graph follow-up

The UI is now hosted at `https://opencodeserver.duckdns.org:8443/pleiades/`, using
persistent user services for the loopback dashboard and Caddy proxy. Username
is `operator`; only a private bcrypt password hash is stored outside Git. The
existing Nginx site redirects the route and forwards HTTP-01 certificate
challenges. Port 443's existing service stays in place. The user approved opening
8443 and allowing the Docker subnet to reach the solver on 8090. The same 8443
opening was applied to OCI ingress with an ETag guard, preserving all existing
ingress and egress rules. Let's Encrypt issued the certificate. A public browser
verified trusted TLS, password protection for HTML/assets/API, wrong-password
rejection, real graph data, video details, JSON export, and mobile layout.

The reviewed additive topic migration is live. Video/channel metadata now
includes YouTube Wikipedia topic classifications; a ninth operation backfills
existing rows in owned, expiring batches. PostgreSQL stores canonical topics,
observed relationships, and outcomes for empty/unavailable resources. The graph
survives ordinary archival. No Wikipedia crawl or transcript facts are inferred.
The initial cycle checked 100 videos and 100 channels, yielding 33 topics and 371
relationships; later cycles continue gradual enrichment. SQL regressions ran on
the separate PostgreSQL instance, covering stale owners, replacement snapshots,
empty/unavailable responses, purged parents, channel atomicity, and bounded reads.

GitHub automation is consolidated into CI, guarded manual Alkyone integration,
and executor CD. CI installs locked packages directly, runs the canonical gate
plus isolated PostgreSQL regressions, and builds the production image. The
obsolete registry bootstrap and its duplicate Dockerfile are removed, along with
retired training workflows. There were no open issues or PRs to triage at inspection.
Historical branches have not been deleted.

PR #9 is open. Its first CI run exposed unit tests that still used the shared
agent-state file and unmocked vault reads. Unit fixtures now refuse network
connections and DNS lookups, isolate state per test, and explicitly fake the
affected vault/notification/quota collaborators. The canonical gate passes
with those guards in place; integration tiers retain their separate opt-ins.

## Disk pressure follow-up

The user requested a better SQL/vault tradeoff after activation. Measurement
found the 45 GiB server filesystem at 99%, a roughly 1.8 GiB local PostgreSQL
database, and 1.17 GiB of retained transcript bodies with existing vault pointers.
The main retained transcripts were still PROCESSING, so verified transcript
handoff must run independently of final video archival. The global pipeline Hub
cache was another 11 GiB and included broken cached links; Docker build cache
held several GiB of reclaimable artifacts.

Source changes separate two-day hot metric retention from seven-day video
retention, raise transcript reconciliation from fifty to up to one thousand
records per cycle under count/byte/time budgets, and scope Hub downloads to
short-lived local directories. Cold verification and row-version checks remain
mandatory. Cache deletion and physical database maintenance are recorded
separately from those logical retention changes below after execution.


Execution: after explicit user approvals, unused Docker build cache reclaimed
4.506 GB. The current pipeline dataset's 11 GiB local Hub cache was removed only
after remote repository access succeeded; other caches, running containers,
tagged images, SQL rows, and remote vault contents were preserved. Filesystem
usage fell from 100% to 68%, with about 15 GiB available. Ingestion was restarted
and is active/running with zero automatic restarts. Its first new transcript
batch verified and released 100 bodies with no failures. Reconciliation continues
gradually; this is not a claim that all 56,000 retained bodies are already cold.

Ordinary `VACUUM (ANALYZE)` completed for transcripts and video metrics under
bounded statement/lock timeouts. Planner counts are now refreshed and dead
space reusable. The reviewed maintenance migration lowers per-table vacuum and
analyze scale factors to 2%, including transcript TOAST. It is live and preserves
rows. No blocking full-table rewrite or filesystem shrink was performed.


## Failure cohorts and Grapher (October 4 follow-up)

The heartbeat initially exposed 554 videos with at least one failed stage:
276 raw, 62 audio and 216 visual failures. It also exposed 2,595 legacy
whole-video `FAILED` rows. These sets overlap by 493 rows, so they represent
2,656 distinct affected videos rather than 3,149. The phase failure markers
do not retain individual error causes. Many affected videos already have
transcripts; failure counts are not data-loss counts or new incidents per cycle.

Retained logs match 57 audio-failed videos to local ENOSPC errors during media
download. The Hugging Face materialization helper caught all errors and returned
false; Singer interpreted false as missing remote media and made the stage
terminal. The helper now distinguishes confirmed remote absence from local
cache, disk, auth and transport failures. Retryable materialization errors
release only audio to pending. Tests cover disk-full, interruption, confirmed
absence and the local-cache exception hierarchy. A bounded repository recovery
method selects explicit IDs and refuses legacy whole-video failures, changed or
completed audio, and rows without fetched raw input; it does not delete artifacts.

Current raw fetch logs also show YouTube/yt-dlp's “page needs to be reloaded”
errors, while retained visual logs include download failures. Historical flags
cannot establish the exact cause of every older row. They should not be cleared
or bulk-replayed simply to produce a green report.

Graph enrichment is now the registered [Grapher Maia agent](../knowledge-graph.md),
with an exclusive fixed key reserve before main-pool allocation. The server's
three unique keys become one discovery, one tracking and one Grapher key.
The reserve comes from manual Archeologist capacity; that agent needs an
additional key for manual operation. Grapher never borrows hunting keys.
