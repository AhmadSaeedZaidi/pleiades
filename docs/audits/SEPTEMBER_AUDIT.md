> Historical record. Use the current documentation index and October audit for active guidance.

# September Audit — Pleiades

**Branch:** `september-audit` (branched from `wip/adaptive-scheduling-and-mcp` @ `ab0295f`)
**Date:** 2026-09-26
**Scope:** `atlas/` (5.1k LOC), `maia/` (6.0k LOC), `alkyone/`, `mcp/`, `tools/`, `deploy/`,
`training/`, `hf-spaces/`, CI/CD, schema, docs.

**Method:** full manual read of all four component trees, plus objective gate runs
(`make lint`, `make test-unit` per component), CI/CD config analysis, and direct
reproduction of every finding marked ✅ Verified.

---

## 0. Executive summary

The architecture is genuinely good in places — the claim/skip-locked index design, the
content-addressed Parquet metrics path with write-then-verify, the fail-closed Alkyone
session gate, and the `orchestrator` cadence contract tests are all careful, deliberate
work. The problems are concentrated in three places:

1. **The pipeline's failure semantics are wrong.** A single failed sub-step sets a
   *row-level* `FAILED`, which every consumer's claim query then excludes — so one
   transient error permanently wedges a video across all five media stages. Nothing in
   `maia/` ever recovers it.
2. **The CI/CD gate does not gate.** The deploy workflow treats `skipped` as `pass`, and
   on a docs-only push it spins for 30 minutes and then fails. The quality and unit-test
   jobs are `always()`-gated behind an image build, so an image-build failure skips them
   and still deploys.
3. **Uncommitted work is load-bearing.** `deploy/` — the systemd units and every SQL
   migration — is entirely untracked, and `cd.yml` deploys with `git pull --ff-only`.
   The migrations CI would need do not exist in any branch.

**The tree is currently red.** `make lint` and `make test-unit` both fail (see §1).

| Gate | Result |
|---|---|
| `make -C atlas lint` | ❌ FAIL — `I001` unsorted imports, `atlas/tests/test_vault_metrics.py:13` |
| `make -C maia lint` | ✅ pass (ruff + format + mypy) |
| `make -C alkyone lint` | ✅ pass |
| `make -C mcp lint` | ✅ pass |
| `make -C atlas test-unit` | ❌ FAIL — 1 failed / 123 passed |
| `make -C maia test-unit` | ✅ 187 passed |
| `make -C mcp test` | ✅ 12 passed |
| `make -C alkyone test-unit` | ✅ 11 passed |
| **`make test-unit` (root)** | ❌ **FAIL** — aborts at atlas, so maia/mcp/alkyone never run in CI |
| **`make lint` (root)** | ❌ **FAIL** — aborts at atlas |

---

## 1. Blocking: the tree is red

### 1.1 `make lint` fails ✅ Verified

```
atlas$ ruff check src/atlas tests
I001 [*] Import block is un-sorted or un-formatted
  --> tests/test_vault_metrics.py:13:1
13 | import atlas.vault as vault_module
14 | from atlas.vault import MetricsManifest, MetricsFileManifest, VaultStrategy, metrics_batch_id
```

`atlas/tests/test_vault_metrics.py` is **untracked** (one of 13 untracked files). This is
the only lint error in the whole repo, and it takes down the entire root `make lint`
because the root target sequences with `&&` semantics via `$(MAKE)`.

### 1.2 `make test-unit` fails ✅ Verified

```
atlas$ pytest tests
FAILED tests/test_vault_metrics.py::test_manifest_and_path_validation_fails_closed
E   AssertionError: Regex pattern did not match.
E     Expected regex: 'partition'
E     Actual message: 'Invalid metrics manifest'
1 failed, 123 passed
```

Also untracked. The test expects a partition-validation error message; the implementation
raises a generic one. One of the two is wrong — needs a decision, not a regex loosen.

**Impact:** `ci.yml` runs `make test-unit` and `make -C atlas lint` as its only two
quality jobs. Both are red. Combined with §2.1, **CD will not block on this** — it will
deploy anyway.

---

## 2. CI/CD findings

### 2.1 CRITICAL — the deploy gate accepts `skipped` as `pass` ✅ Verified

`.github/workflows/cd.yml` gates deploys on CI, then treats three conclusions as success:

```bash
if jq -e 'select(.conclusion != "success" and .conclusion != "skipped" and .conclusion != "neutral")' /tmp/ci-runs.json > /dev/null; then
  CI did not pass: ...
fi
echo "CI passed."
```

In `ci.yml`, both real quality jobs are gated on the image build:

```yaml
quality:
  needs: [build-env, ensure-image]
  if: always() && (needs.build-env.result == "success" || needs.build-env.result == "skipped")
unit-tests:
  needs: quality
```

So if `build-env` **fails** (CI image build broken, GHCR push rejected, rate-limited),
`quality` is **skipped**, and `unit-tests` is **skipped** as a consequence. Reproduced:

```
--- selected runs ---
{"name":"Check for dependency changes","conclusion":"success"}
{"name":"Quality (Linting & Type Checking)","conclusion":"skipped"}
{"name":"Unit Tests","conclusion":"skipped"}
>>> GATE PASSES -> deploy proceeds although lint+tests were SKIPPED
```

The only job that actually ran is a `dorny/paths-filter` path check. **Lint and tests can
be entirely absent from a production deploy and the gate reports "CI passed."**

**Fix:** require `conclusion == "success"` for the two real jobs, and assert the expected
job set is present before evaluating. Also switch to `workflow_run` on `ci.yml` completion
instead of polling check-runs — it removes the entire class of problem.

### 2.2 HIGH — the `build-env` job name in the gate does not exist ✅ Verified

The gate's selector list includes `"Build CI Environment"`. That is the *workflow* name
(`build-env.yml:1`). The *job* inside is named `Build & Push CI Image`
(`build-env.yml:17`), and job names are what appear as check-runs. The selector therefore
never matches, so the image build is not actually gated on.

### 2.3 HIGH — docs-only push to `main` hangs the deploy for 30 minutes, then fails ✅ Verified

`ci.yml` triggers on push with `paths-ignore: ["docs/**", "**.md"]`. `cd.yml` triggers on
every push to `main`. A docs-only merge therefore produces **no CI check-runs at all**:

```
>>> 'no CI runs yet; waiting...' x120 = 30min hang then FAIL deploy
```

120 iterations × `sleep 15` = 30 min, then `exit 1`. The `smoke` job has
`timeout-minutes: 45`, so it burns 30 minutes of runner time to reach a foregone
conclusion. Worse, the deploy is *blocked* for a commit that changed nothing deployable.

**Fix:** gate on `github.event.committer`/paths, or short-circuit when the SHA has no
non-doc changes.

### 2.4 HIGH — `deploy/` is entirely untracked, and CI deploys with `git pull --ff-only` ✅ Verified

```
$ git check-ignore -v deploy/systemd/pleiades-ingestion.service
NOT ignored -> simply untracked (never committed)
$ git log --oneline --all -- deploy/
(no output)
```

Every systemd unit and every SQL migration in `deploy/` exists **only in this working
tree**. `cd.yml:123` deploys via `git pull --ff-only`, so:

- the migrations were applied to production by hand, with no record in version control;
- the running systemd unit on the VPS is whatever was last hand-copied;
- a fresh clone cannot reproduce production.

The repo has a `db-schema-architect` subagent and a 3NF migration plan; none of that
output is committed. **This is the single highest-value thing to fix.**

### 2.5 MEDIUM — `tools/setup_orchestration.py` failures are swallowed ✅ Verified

`cd.yml:125`:

```bash
./.venv/bin/python tools/setup_orchestration.py || true
```

`git pull`, `make install`, and `systemctl restart` are all `set -e`-protected. This one
is not. A wrong `PREFECT_API_URL`, missing work pool, or revoked `PREFECT_API_KEY` is
silently ignored, the deploy reports success, and the scheduler restarts pointed at a
misconfigured control plane. If Prefect is genuinely optional per the README, drop the
call; if it is not, drop the `|| true`.

### 2.6 LOW — training workflows are dead weight and reference a foreign repo

`train_daily/weekly/monthly.yml` all have `schedule:` commented out, so they are
dispatch-only. All four pull `ghcr.io/ahmadsaeedzaidi/**viralvelocity**/training:latest`
— a *different* repository — while the build workflow (`train_build.yml`) pushes to
`ahmadsaeedzaidi/pleiades/training`. They can never have run. `train_build.yml` also
references `training/requirements.txt`, which is not in the filter list of the actual
build path, and pins `docker/login-action@v2` / `metadata-action@v4` /
`build-push-action@v4` while `build-env.yml` uses v3/v5/v6. Delete them or fix the
targets.

### 2.7 LOW — the commit it gates on is not the commit it deploys

`cd.yml` checks out the workflow's SHA but then runs `git pull --ff-only` on the VPS. If
`main` advances during the poll window, production runs a *newer* commit than the one that
passed CI. Use `git checkout <sha>` (or `git reset --hard $SHA` with a recorded SHA) so
the deployed tree is provably the tested one.

---

## 3. Pipeline correctness

### 3.1 CRITICAL — one failed step wedges a video across all five consumers ✅ Verified

`atlas/src/atlas/repositories/video/state_machine.py:408-417`:

```python
async def mark_step_failed(self, video_id: str, step: str) -> None:
    """Fail the current stage claim without overwriting a newer completion."""
    if step not in self._STEP_COLUMNS:
        raise ValueError(f"Unknown pipeline step: {step!r}")
    await self._execute(
        f"UPDATE videos SET status = 'FAILED', {step}_phase = 'FAILED', "
        f"last_updated_at = %s WHERE id = %s AND {step}_phase = 'PROCESSING'",
        (now, video_id),
    )
```

`status` is the **row-level** state. Every claim gate excludes `FAILED`:

```
state_machine.py:18   WHERE status IN ('PENDING', 'PROCESSING')          # scribe
state_machine.py:37   WHERE status IN ('PENDING', 'PROCESSING')          # painter
state_machine.py:67   WHERE status IN ('PENDING', 'PROCESSING')          # streamer
state_machine.py:92   WHERE status IN ('PENDING', 'PROCESSING', 'PROCESSED')  # singer
state_machine.py:111  WHERE status IN ('PENDING', 'PROCESSING')          # muralist
```

So if the Painter hits one vault hiccup, the Scribe, Singer, Streamer and Muralist all
stop seeing that video **permanently** — even though the video may already have a valid
transcript. The docstring's promise ("without overwriting a newer completion") is not
what the SQL does: it overwrites row-level status.

Callers that reach this on *transient* errors:

| Caller | Trigger |
|---|---|
| `maia/src/maia/painter/flow.py:396` | vault `store_batch` failure |
| `maia/src/maia/streamer/flow.py:172` | any `Exception` in the download path |
| `maia/src/maia/scribe/flow.py:126` | any `Exception` in the caption path |
| `maia/src/maia/muralist/flow.py:134` | any `Exception` |

Contrast the one correct caller, `maia/src/maia/singer/flow.py:234`:

```python
on_failure=VideoRepository().release_audio_to_pending
```

`reset_failed_to_pending()` exists at `state_machine.py:292-305` but has **zero callers in
`maia/`** — only `tools/recover_failed_runs.py`. `repaint_all_videos()` and `run_janitor()`
likewise are never called from `maia/`.

**This is the highest-impact defect in the codebase.** The fix is small: `mark_step_failed`
should set only `{step}_phase = 'FAILED'` and leave `status` alone, letting the other four
consumers proceed. Per-step failure is already modelled by the phase columns — the row-level
`status` should not be the sum of them.

### 3.2 CRITICAL — the phase-sync trigger discards explicitly-written phases on INSERT ✅ Verified

`atlas/src/atlas/schema.sql:302-336`:

```sql
CREATE OR TRIGGER ... BEFORE INSERT OR UPDATE ON videos FOR EACH ROW ...
    IF NEW.fetched IS DISTINCT FROM OLD.fetched THEN
        NEW.raw_phase := CASE WHEN NEW.fetched THEN 'DONE' ELSE 'PENDING' END;
    ELSIF NEW.raw_phase IS DISTINCT FROM OLD.raw_phase THEN
        NEW.fetched := (NEW.raw_phase = 'DONE');
    END IF;
```

On `INSERT`, `OLD.fetched` is `NULL`. `NEW.fetched` (default `FALSE`) `IS DISTINCT FROM`
`NULL` → **always TRUE**, so the first branch always fires and the boolean wins.

Consequence: `INSERT INTO videos (..., raw_phase) VALUES (..., 'PROCESSING')` is silently
rewritten to `'PENDING'`. **A video cannot be enqueued mid-phase.** The same asymmetry
applies on `UPDATE` when a statement changes both a boolean and its phase in one go —
e.g. `SET has_transcript = FALSE, transcript_phase = 'PROCESSING'` is coerced to `'PENDING'`.

This is why §3.4's dead guards and §3.3's dead reclamation are dead: the phase columns can
only ever hold `PENDING | PROCESSING | FAILED` in practice, never a caller-chosen `DONE`.

### 3.3 HIGH — `reclaim_raw_if_complete` can never fire; `raw/` grows unbounded

`state_machine.py:189-198`:

```python
if not (row["audio_phase"] == "DONE" and row["visuals_phase"] == "DONE"):
    return 0
```

Per §3.2 the phases are driven by the *booleans*, and the singer calls
`reclaim_raw_if_complete` (`singer/flow.py:244`) immediately after `mark_audio_safe` —
at which point `visuals_phase` is almost always still `PROCESSING` (the Painter runs on a
120 s cycle and may be several videos behind). The join barrier is therefore never
satisfied, and the TTL branch at `:200-207` is unreachable.

The README states the raw artifact is reclaimed "only after its mandatory consumers are
DONE or the TTL window expires." In practice: never reclaimed by either condition. The
`.part` / raw directories grow until the disk fills.

### 3.4 HIGH — five idempotency guards are unreachable

Because `RETURNING *` from a claim always yields `phase = 'PROCESSING'` (the boolean
filter, not the phase, selects the row), these checks can never be true:

- `scribe/flow.py:86` — `if video.transcript_phase == "DONE"`
- `painter/flow.py:254` — `if video.visuals_phase == "DONE"`
- `streamer/flow.py:83` — `if video.raw_phase == "DONE"`
- `singer/flow.py:76` — `if video.audio_phase == "DONE"`
- `muralist/flow.py:69` — `if video.clip_phase == "DONE"`

The phase columns are effectively a write-only `PROCESSING|FAILED` marker. `pipeline_phase`
(`schema.sql:279-281`) reports a frontier from columns that are almost never `DONE`, which
is why the Heartbeat's `Phases` field is not a meaningful frontier.

### 3.5 HIGH — `status` can regress `PROCESSED → PENDING`, hiding a video from the Janitor

All four `release_*_to_pending` methods (`state_machine.py:318-406`) do
`SET status = 'PENDING'` unconditionally, gated only on `<step>_phase = 'PROCESSING'`.
The Janitor sweeps `status = 'PROCESSED'` only (`janitor.py:40`).

Enabled by the race in §3.6: a fully-processed video whose Scribe claim is still open can
be knocked back to `PENDING` by a caption 429, and becomes invisible to archival.

### 3.6 HIGH — two cycles can claim the same row; the `SKIP LOCKED` "lock" does not persist

All five claims are single-statement `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)
RETURNING *` in autocommit. Postgres releases `FOR UPDATE` row locks at **end of
statement**, so once the statement commits the row is unlocked — the "claim" is not durable.

`claim_scribe_batch` (`:15-23`) filters `has_transcript = FALSE`; `claim_streamer_batch`
(`:64-74`) filters `fetched = FALSE`. Neither excludes the other's step, so on a video with
neither done, **both claim it in the same window** and each writes `status`. This is the
race that makes §3.5 reachable.

`sweep_archivable` (`janitor.py:37-48`) has the same gap: the locking SELECT runs in
autocommit, and `janitor/flow.py` calls `archive_video_batch` minutes later. The comment
at `janitor/flow.py:166-167` ("no TOCTOU concern") is an assumption, not an enforcement.

The correct fix is a real lease: set `phase = 'PROCESSING', lease_expires_at = now() + interval`
in the claim's own transaction, and have claims filter on `lease_expires_at < now()`.

### 3.7 HIGH — the quality gate fails open, silently disabling anti-slop filtering

`maia/src/maia/hunter/flow.py:182-191`:

```python
except Exception as e:
    run_logger.warning(f"Quality gate enrichment failed, ingesting unfiltered: {e}")
    items = raw_items
```

`filter_by_quality` makes two YouTube calls plus a Shorts `HEAD` probe. Any one failing
bypasses the entire gate for that 50-item page — and those videos are persisted *and*
snowball their tags back into the search queue (`hunter/flow.py:248-256`), so the pollution
compounds. A single `warning` line is the only signal.

Worse, the channel-statistics gate is a silent no-op whenever `channels.list` fails —
`quality/enrich.py:16-47`:

```python
try:
    items = await lookup_channels(list(channel_ids), parts="snippet,statistics", executor=executor)
except Exception:
    return {}
```

`{}` → `evaluate_channel(cid, None, …)` → `gates.py:177-178` returns `True`. Every video
passes, **with no log line at all**.

### 3.8 HIGH — the Scribe marks `has_transcript = TRUE` with no transcript

`maia/src/maia/scribe/flow.py:121-123`:

```python
except TranscriptExtractionError:
    ... mark_transcript_safe(vid_id)
```

`mark_transcript_safe` sets `has_transcript = TRUE` **without** a `record_transcript()`
call. There is no transcript row and no vault artifact. The sibling branch for
`TranscriptTooLongError` (`:109-120`) *does* write a templated notice — so the two
"no transcript" paths are inconsistent, and one of them lies to the state machine. That
also permanently prevents the Janitor from ever retrying it.

### 3.9 MEDIUM — vault metadata write failure discards the discovery page

`hunter/flow.py:204-211` catches the `store_batch` failure and only warns, but
`update_state` at `:268` still advances the page token. The raw discovery payload for that
page is **permanently lost** — it exists nowhere else.

### 3.10 MEDIUM — Janitor can fail forever without alerting

- `janitor/flow.py:98-100` — `archive_cold_stats` failure is captured into
  `results["stats_error"]`, which is **never rendered** in the Discord embed
  (`:489-499`). Stats archival can fail indefinitely, silently.
- `janitor/flow.py:317-319` — vault-flush chunk failures increment `failed`, but the embed
  shows `AlertLevel.INFO` unless `videos_failed > 0` (`:488`), and `videos_failed` counts
  *archive* failures, not flush failures. **A completely failing vault flush reports INFO.**
- `janitor/flow.py:193-197` — a non-200 from the Prefect purge API `break`s the loop and
  returns `{"purged_flow_runs": 0}`, indistinguishable from "nothing to purge".

### 3.11 MEDIUM — the Heartbeat's `fleet_down` is never populated

`heartbeat/flow.py:314`:

```python
fleet_down: list[str] = []
```

Never appended to, but read at `:322` to decide `AlertLevel.WARNING` and the "⚠ Down:"
banner. A Prefect deployment reporting `("last run: Failed", "down")` — which
`collect_fleet_status` *does* produce at `:133` — is displayed in the embed yet cannot
escalate the alert level. `tests/test_heartbeat.py:177-208` asserts `summary["down"] == []`
while the whole fleet reports `down`, so the test locks the bug in.

### 3.12 MEDIUM — `subprocess.TimeoutExpired` is unhandled in 4 of 8 yt-dlp call sites

`media/streamer.py:300, 354, 409, 450, 487` use bare `subprocess.run(...)` and check
`result.returncode`. A timeout escapes as an unhandled exception and lands in the
`except Exception` branch of the caller — which is `mark_step_failed`, i.e. the §3.1 wedge.
A 300-second timeout is not a permanent failure; the sibling `AudioExtractionError` branch
at `streamer/flow.py:126-131` correctly *releases* to `PENDING`. The two paths disagree
about whether a timeout is retryable.

### 3.13 MEDIUM — the 3NF schema rewrite removes the only recovery path

There is a `db-schema-architect` subagent and a 3NF migration plan, and a
`20260902_tracker_durable_state.sql` in `deploy/sql/`. Good. But note that flattening the
phase columns would remove the *only* mechanism that could distinguish "step X failed" from
"step X done" without the row-level `status` flag — so §3.1 must be fixed **before** the
schema rewrite, not after. Fixing the schema first would entrench the wedge.

---

## 4. Resource and concurrency

### 4.1 CRITICAL — nested vault retry can block a worker thread for ~35 minutes

`atlas/src/atlas/vault.py:600-639`, `HuggingFaceVault.store_batch(max_attempts=8,
base_delay=30.0)` sleeps `30, 60, 120, 240, 480, 600, 600` = **~2,130 s** inside a
synchronous method. `maia/src/maia/utils.py:78-90` wraps it:

```python
return await loop.run_in_executor(None, fn)   # None = DEFAULT executor
```

`None` means the shared `ThreadPoolExecutor`, bounded at `min(32, cpu+4)` = **6 threads**
on the 2-core executor box. The outer `@retry(stop_after_attempt(3))` multiplies it:
**3 × 35 min ≈ 1.8 h per batch store**, with the thread parked the whole time.

Six call sites can be in flight at once. Meanwhile *every* other `run_in_executor(None, …)`
— all the yt-dlp downloads, ffmpeg frame extraction, parquet reads, HF cache copies — is
queued behind them. The result is not an error; it is **total, silent pipeline stall**.

`TimeoutStopSec=30` in the systemd unit cancels the coroutine, but
`ThreadPoolExecutor` threads are joined at interpreter exit, so the service can hang for up
to an hour on `SIGTERM`.

**Fix:** make the backoff `await asyncio.sleep` in an async wrapper, and give vault I/O a
dedicated executor. Also: `time.sleep(min(base_delay * attempt, 120.0))` retries
*non-retryable* errors (401/404) for ~7 minutes before giving up, with no jitter —
so synchronized retry storms across agents.

### 4.2 MEDIUM — documented cadences are fiction

`orchestrator.py:53-65` sets `scribe` to 120 s, but `SCRIBE_THROTTLE_SECONDS = 20.0`
(`scribe/flow.py:50`) × batch 10 = ≥200 s of mandated sleep *inside* the cycle. `painter`
is worse: up to `MAX_FRAMES=60` sequential ffmpeg calls at `timeout=20` each
(`painter/flow.py:210-236`) = up to ~20 min *per video*, × batch 5 = ~100 min per cycle,
against a configured 120 s. The interval is a post-hoc delay, not a period.

### 4.3 MEDIUM — the connection pool is never closed

`orchestrator.py:137-145` cancels tasks and closes the loop without calling `db.close()`
(`atlas/db.py:40-44`). `AsyncConnectionPool` explicitly warns against being GC'd with live
background tasks. 20 connections leak on every restart.

### 4.4 MEDIUM — the DB pool has no `statement_timeout`

`db.py:29-38` sets `timeout=30.0` (getconn only). There is no `statement_timeout` or
`lock_timeout` anywhere, so a `FOR UPDATE SKIP LOCKED` sweep against a locked row set holds
a pooled connection indefinitely.

### 4.5 LOW — `BaseBatchAgent.run` discards completed work when `raise_on` fires

`base.py:101-109`: if one item raises `QuotaExhaustedError`, the results already computed
for the other items are thrown away, and those videos are left in `phase = 'PROCESSING'`
with no release.

---

## 5. Security

### 5.1 HIGH — path traversal in the MCP `list_artifacts` tool ✅ Verified, reproduced

`mcp/src/pleiades_mcp/server.py:436-466` is the **only** tool that skips
`_video_id_from_url()` validation:

```python
root = _ARTIFACT_ROOT
if video_id:
    root = root / video_id          # unvalidated
...
for p in sorted(root.rglob("*")):
    if p.is_file():
        rel = str(p.relative_to(_ARTIFACT_ROOT))   # lexical, not validated
        uri = f"{_http_base}/{rel}" if _http_base else f"file://{p.resolve()}"
        out.append({"uri": uri, "path": str(p.resolve()), ..., "size_bytes": p.stat().st_size})
```

`Path("/tmp/pleiades_mcp/artifacts") / "../../.."` is a real directory. `relative_to()` is
**lexical**, so it happily returns `../../../…`. Reproduced:

```
root/video_id resolves to: /tmp/pleiades_mcp/artifacts/../../..
exists: True
  leak -> ../../../boot/System.map-6.17.0-1011-oracle
```

An unauthenticated client can enumerate arbitrary filesystem paths and sizes.
`ArtifactStore._safe()` (`artifacts.py:85-89`) has a traversal guard, but `list_artifacts`
bypasses `ArtifactStore` entirely. `list_artifacts` has **zero test coverage** — which is
why this survived.

**Fix:** validate `video_id` with `_video_id_from_url()`, and resolve with
`p.resolve()` then assert `is_relative_to(_ARTIFACT_ROOT.resolve())`.

### 5.2 HIGH — the MCP server has no authentication

`server.py:518-536` enables DNS-rebinding protection and documents the model honestly:
*"ingress is IP-locked"* — an infrastructure claim, not a code one. But
`mcp/run_server_full.sh:15-30` (untracked) actively undoes it:

```bash
PUBLIC_HOST="${PUBLIC_HOST:-pleiades-mcp.duckdns.org}"
PUBLIC_IP="${PUBLIC_IP:-89.168.125.2}"
.venv/bin/python -m http.server "$ART_PORT" --bind 0.0.0.0 --directory "$PLEIADES_MCP_ARTIFACT_DIR" &
.venv/bin/pleiades-mcp --transport streamable-http --host 0.0.0.0 --port 8000 ...
```

A bare `python -m http.server` on `0.0.0.0:8001` serves the whole artifact tree with no
auth, and a public IP + DuckDNS hostname are hardcoded into `mcp_instructions.txt` for
pasting into client configs. Anyone who can reach the box gets every cached transcript,
audio file and keyframe.

### 5.3 HIGH — unbounded cost on the unauthenticated MCP surface

No auth, no rate limit, no cost ceiling, no concurrency ceiling. The six media tools are
sync `def`, so FastMCP runs them on a threadpool — unbounded parallel ffmpeg/yt-dlp from
one client.

- `get_audio(video_id, transcribe=True)` — full audio download into memory
  (`server.py:413`) + a **paid** Mistral Voxtral call (`server.py:423`). No duration cap,
  no cost cap. A 3-hour video bills an unbounded transcription.
- `get_keyframes(max_frames=60, inline=True)` — the docstring says "keep ≤ 6" but nothing
  enforces it; 60 base64 WebP images come back in one JSON-RPC response.
- `get_transcript(format="segments")` — a 3-hour video can be hundreds of thousands of
  segments inline, no truncation.
- Artifact store has no eviction, no size cap, no TTL → fills `/tmp` on a long-lived server.

For contrast, `summarize.py:64` does truncate at `max_chars=60_000`. The pattern is
understood; it just isn't applied to the expensive tools.

### 5.4 HIGH — SSH host-key verification disabled on the tunnel all YouTube traffic uses

`deploy/systemd/youtube-proxy.service:11`:

```ini
ExecStart=/usr/bin/ssh -i /home/ubuntu/.ssh/pleiades-mini-key -o BatchMode=yes \
  -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -N -D 127.0.0.1:1090 ubuntu@10.0.0.22
```

With `Restart=always`, this reconnects forever, indefinitely trusting whatever answers on
`10.0.0.22:22`. `pleiades-ingestion.service:17` routes **all** YouTube traffic through
this SOCKS tunnel (`YOUTUBE_PROXY=socks5h://127.0.0.1:1090`), and
`YOUTUBE_COOKIES_PATH` points at a logged-in session cookie jar. Anyone who can influence
the local network has a standing MITM on authenticated YouTube traffic.

Use `StrictHostKeyChecking=yes` with a committed host key (or at minimum `accept-new`).

### 5.5 HIGH — the Alkyone guard fails open when the prod-interlock vars are unset

`alkyone/src/alkyone/guard.py:40-44`:

```python
def _matches(prod: str | None, candidate: str | None) -> bool:
    prod_n, cand_n = _norm(prod), _norm(candidate)
    if not prod_n or not cand_n:
        return False          # empty prod target => "not production" => ALLOW
    return cand_n == prod_n
```

The stronger `assert_isolated_test_environment()` *does* require both `PLEIADES_PROD_*`
vars to be non-empty, so the session gate is fail-closed. **But the weaker
`assert_not_production()` is still the one guarding DB teardown** —
`alkyone/src/alkyone/fixtures.py:81-85`:

```python
from alkyone.guard import assert_not_production
assert_not_production()
```

and `fresh_db` then calls `db.reset_for_test()`, which is
`TRUNCATE TABLE public.<every table> RESTART IDENTITY CASCADE` (`atlas/db.py:111-135`).

So: unset `PLEIADES_PROD_DATABASE_URL` and the teardown interlock becomes a no-op, and the
blast radius is **the entire production database** — no confirmation, no row-count
threshold, no name heuristic. The removed code (visible in `git diff`) at least *told* the
operator that unsetting the var was the bypass; the new code treats unset as safe.

### 5.6 HIGH — a documented escape hatch routes tests into the production vault

`alkyone/src/alkyone/fixtures.py:4-11` honours `PLEIADES_USE_PRODUCTION_VAULT=1`, and
`alkyone/tests/components/maia/test_pipeline_e2e_real.py:28-31` documents it in-file:

```
To run this test against the **production** dataset (``Rolaficus/pleiades-vault`` …)::
    PLEIADES_USE_PRODUCTION_VAULT=1 pytest ...
```

If the operator then also sets `ALKYONE_TEST_VAULT=<the prod vault>` — a natural reading of
"declare the test identity" — **every guard check passes**, and
`fixtures._cleanup_hf_uploads()` (`:134-174`) then calls `api.delete_file(...)` against
the production HF repo with no `repo_id` check. `test_guard.py` has no test for this
variable at all.

### 5.7 MEDIUM — the guard only covers 2 of 6 production targets

`guard.py` checks `DATABASE_URL` and `HF_DATASET_ID` only. It does not guard
`YOUTUBE_API_KEY_POOL_JSON` (real prod quota), `GCS_BUCKET_NAME`, `PREFECT_API_*`, or
`YOUTUBE_COOKIES_PATH`. And `alkyone/README.md:13-15` claims *"YouTube is NOT exercised
against the live API by default"* — this is **false**: `conftest.py:94-101` skips only
when keys are *missing or dummy*, so genuine production keys make the suite **run** against
the live API. `test_archeologist.py` contains three such tests
(`test_archeologist_real_youtube_api_search`, `…_rate_limit_detection`,
`…_key_rotation`), all merely `@pytest.mark.integration`.

### 5.8 MEDIUM — guard regressions are uncommitted

`alkyone/tests/live/conftest.py` (the only thing gating `tests/live/`) and
`alkyone/tests/test_guard.py` are both **untracked**. In a clean checkout,
`make -C alkyone test-unit` runs an empty file list. The entire guard hardening in this
working tree is uncommitted.

### 5.9 MEDIUM — `guard._norm` lost case-insensitivity, weakening the prod check

Uncommitted change in `guard.py:36-37`:

```diff
-    return (value or "").strip().rstrip("/").lower()
+    return (value or "").strip().rstrip("/")
```

Postgres folds identifiers to lowercase and HF dataset ids resolve case-insensitively, so
`…@Prod-DB/Pleiades` vs `…@prod-db/pleiades` no longer match — the guard permits the run.
`test_guard.py:99-106` (`test_database_identity_comparison_preserves_case`) actively
codifies the stricter behaviour, so it looks deliberate. It is a security-relevant change.

### 5.10 MEDIUM — MCP's key-pool isolation is partial and inconsistent

`mcp/src/pleiades_mcp/media.py:59-69` prefers a dedicated `MCP_YOUTUBE_API_KEY_POOL_JSON`
reserve ring, and falls back to the **production `hunting` ring** if unset — including by
reading the repo-root `.env` from a hardcoded absolute path:

```python
for candidate in (".env", "/home/ubuntu/code/pleiades/.env"):
```

`get_video_metadata` bypasses the reserve entirely: `atlas/youtube.py:56-69` defaults
`key_ring_pool="hunting"` and `media.py:165-172` never overrides it. So
**`get_video_metadata` always burns production quota**, regardless of the reserve pool.

### 5.11 LOW — `JANITOR_ENABLED` defaults to `True` with no guard rail

`atlas/src/atlas/config.py:46-48` defaults to `True`, and `run_janitor`
(`repositories/video/janitor.py:321-357`) hard-`DELETE`s `videos` rows past
`JANITOR_RETENTION_DAYS` (default 7). A production deploy that forgets the var **starts
deleting its corpus after 7 days** — no dry run, no confirmation, no first-run guard. The
atlas test suite sets `JANITOR_ENABLED=false`, so the default-on path is never exercised.

Also: `JANITOR_SAFETY_CHECK` is documented as *"Verify data exists in Vault before
deletion"* but in `run_janitor` it is only a row filter (`:327-328`). One flag, two
meanings.

### 5.12 LOW — cookie jar written to a predictable world-readable temp path

`atlas/src/atlas/config.py:309-310`:

```python
tmp = Path(tempfile.gettempdir()) / "youtube_cookies.txt"
tmp.write_text(content)
```

Fixed filename in a world-writable directory, no `chmod 0600`, no `O_EXCL`,
symlink-attackable, shared across every user on the host — holding an authenticated
YouTube session. It is also rewritten on *every property access*.

### 5.13 Secrets hygiene — the good news ✅ Verified

- `.env`, `www.youtube.cookies.txt`, and all `*.part` artifacts are **untracked and
  gitignored** (`.gitignore:33, 113, 128`). No credential is in git history.
- No hardcoded API key, token, password, or webhook URL anywhere in `atlas/src` or
  `maia/src`. All secrets are `SecretStr` in `config.py`.
- `COMPLIANCE_MODE` defaults to `False` and `api_keys` explicitly refuses to truncate the
  key pool under it (`config.py:236-243`), with a regression test. Good.
- `tools/migrate_vault.log` is committed but grepped clean of tokens.
- Residual: `vault.py:473` and `db.py:27` un-redact `SecretStr` into plain module-level
  strings, and neither vault class overrides `__repr__`.

---

## 6. Dead code and packaging

### 6.1 HIGH — `maia/singer/` and `maia/streamer/` have no `__init__.py` ✅ Verified

```
maia/src/maia/singer/:    flow.py          (no __init__.py)
maia/src/maia/streamer/:  flow.py          (no __init__.py)
```

Every other agent package has one. `maia/pyproject.toml:49` uses
`packages = [{include = "maia", from = "src"}]`, which relies on `__init__.py` discovery —
**a built wheel will omit `singer/flow.py` and `streamer/flow.py` entirely.** It works
today only because the systemd unit sets `PYTHONPATH` and Python's implicit-namespace
fallback accepts it. Add the two files.

### 6.2 MEDIUM — a dead table, a dead extension, and unenforced retention

From `atlas/src/atlas/schema.sql`:

- **`channel_history`** (`:14-21`) — zero Python references repo-wide. No INSERT ever
  occurs. Its index (`:178`) is dead weight, and it duplicates `channels.title` temporally
  (a 3NF violation) for no consumer.
- **`CREATE EXTENSION vector`** (`:1`) — created, never used. No `vector`/`embedding`
  column exists anywhere in the schema.
- **`videos.audio_pending BYTEA`** (`:72`) — "no longer populated" but still returned by
  every `RETURNING *` claim query, hydrating a binary column per claimed row.
- **`videos.wiki_topics`** — never written (`ingestion.py:98-106` omits it), no index.
- **`channels.is_verified`** — never written by any code, so always `FALSE`.
- **`idx_search_queue_fetch (priority DESC, mention_count DESC)`** (`:185`) — useless; the
  actual `ORDER BY` is a runtime score expression.
- **`idx_channel_scrape`** (`:177`) — backs no query in this codebase.
- **Row-cap enforcement was retired** (`deploy/sql/20260902_retire_unsafe_row_limit.sql`),
  but the comment at `schema.sql:217-220` still claims caps are "enforced by Janitor
  sweep." **There is currently no automated bound on `channel_stats_log` /
  `video_stats_log` / `system_events` growth.** The README's "about 1.55 GB before the
  audited transcript backfill" is unbounded going forward.

### 6.3 MEDIUM — `watchlist` has no foreign key to `videos`

`schema.sql:151-164`. Acknowledged in code (`quality.py:36-37`), but the consequences
cascade: `delete_videos` must pre-delete outside a transaction (`quality.py:41-42`);
`run_janitor`'s `DELETE FROM videos` (`:349-357`) **orphans every watchlist row** for
deleted videos; and `watchlist.fetch_batch`'s `LEFT JOIN` (`:43`) then feeds dangling ids
to `videos.list`. Single most consequential schema gap.

Also missing: `CHECK (>= 0)` on `video_stats_log` / `channel_stats_log` counts (present on
`watchlist` — inconsistent), and `NOT NULL` on `channels.created_at`.

### 6.4 MEDIUM — enum-as-text columns with no CHECK constraint

The schema defines a real enum (`step_phase`, `:253-255`) but leaves these as free text
with no `CHECK`: `videos.status` (`:45`), `search_queue.status` (`:133`),
`system_events.event_type` (`:118`), `transcripts.language` (`:141`).
`watchlist.tracking_tier` *does* have a proper CHECK (`:153-154`) — so the standard is
known. A typo in any of the ~30 `'PROCESSED'`/`'FAILED'` literals in `state_machine.py`
silently creates rows no claim query will ever pick up. The failure mode is a permanent
stall, not an error.

### 6.5 MEDIUM — `pipeline_phase` can report `DONE` for an incomplete video

`schema.sql:265-277`. The generated function is **not `STRICT`**: a single NULL phase
column makes its `<>` yield NULL (not true), the `CASE` chain falls through, and the
frontier is reported as `'DONE'`.

### 6.6 MEDIUM — missing indexes on columns actually used in WHERE/ORDER BY

| Query | Index status |
|---|---|
| `quality.py:21-30` `WHERE duration < ? ORDER BY discovered_at` | ❌ none on `duration` → full scan + sort |
| `transcript.py:59-73` `claim_vault_pending_batch` | ❌ `idx_videos_vault_pending` is on `(id)` and its predicate cannot match the `OR t.vault_uri IS NULL` branch, nor the `ORDER BY` |
| `event.py:57-67` `ORDER BY created_at DESC` | ⚠️ no leading `created_at DESC` index |
| `janitor.py:349-357` `discovered_at < ? AND status IN ('PROCESSED','ARCHIVED')` | ⚠️ `idx_video_sweep` is partial on `status='PROCESSED'` only |

Credit where due: the five partial claim indexes (`:197-208`) precisely match the five
claim queries' predicates *and* sort directions, including the deliberate newest-first
`discovered_at DESC` for the streamer. That is genuinely careful work, and it is covered
by `atlas/tests/test_schema_contract.py`.

### 6.7 LOW — remaining N+1

`janitor.py:186-194` runs a per-video `SELECT EXISTS` **inside** the archive loop, plus a
per-video pool checkout and transaction at `:200`. At `_ARCHIVAL_BATCH_SIZE = 100` that is
100 extra round trips per cycle. `repositories/channel.py` has no batch equivalent of
`get_latest_stats`, so an N-channel refresh is N round trips.

### 6.8 LOW — unused public surface

`StealthVideoStreamer.download_raw` (`media/streamer.py:387-422`, ~35 LOC, never called
from production); `transcribe_video` (`scribe/transcription.py:120-143`, only called from
a test, and its `strategy`/`store_audio` params are accepted and ignored);
`muralist_flow(height=…)` (`muralist/flow.py:102-104`, `height` accepted and **discarded** —
so the documented `python -m maia muralist --height 480` cannot work);
`begin_step` / `mark_step_phase` / `get_pipeline_phase` (`state_machine.py:227-248`, zero
callers); `is_rate_limited` (`atlas/state.py:128-131`); `store_metadata`
(`vault.py:293-297`, dead **and** unsharded while the janitor duplicates its path inline
at `janitor.py:147`, hiding the 10k-files-per-directory gap).

`painter/flow.py:258` creates a `tempfile.mkdtemp()` that is never written to
(`local_input` is always a stream URL) — wasted `mkdtemp`+`rmtree` per video per cycle.

`quality/__init__.py:12` carries `import aiohttp  # noqa: F401  keep importable …
(used by tests)` — production code carrying a test-only re-export.

`GrokTranscriber` has **no** `CallPacer`, contradicting the documented design
("Paid transcribers (Mistral/Grok) are paced with a process-wide `CallPacer`"), and Grok is
the *preferred* provider in `auto` (`transcription.py:82`). The primary paid path is unpaced.

### 6.9 LOW — `KeyRing` has two disagreeing notions of "usable keys"

`utils.py:138-152` rebuilds `self._live_keys` but **not** `self._iterator`, which was built
from `self.keys` at construction (`:106`):

```python
self._iterator: itertools.cycle[str] = itertools.cycle(self.keys)
```

Reproduced: after blacklisting `k1`, `next_key()` still returns `['k1','k2','k1','k2']`. A
revoked key keeps being handed out for the process lifetime. `next_key()` is live API
(tested in `mcp/tests/test_server.py:70`).

Separately, `utils.py:182-207` marks `order[0]` as *seen* before any key is handed out
("preserve the historical direct-call behavior"), so a **1-key ring reports
`QuotaExhaustedError` without making a single HTTP request.** Nothing in atlas or maia
calls `attempt_rotation` before `get_session_key` — only `ResiliencyExecutor` does, and it
never does. The workaround is dead weight that creates a footgun.

And a `retryable` disposition (`:274-279`) rotates through **every key with no sleep**, then
`return None`. On a 24-key hunting ring that is 24 immediate requests against an endpoint
that just said "slow down" — precisely what the resiliency layer exists to prevent. (The
`retryable` classification itself looks like an off-by-one: 429/403 rate limits are
classified as `quota` at `:302-305`.)

### 6.10 LOW — `.env` parsing defects

- `config.py:226-245`: malformed `YOUTUBE_API_KEY_POOL_JSON` falls back to
  `[the whole blob string]` — a trailing comma yields a **1-key ring containing the literal
  JSON**. Every request 400s and the operator sees quota exhaustion, not a parse error.
- `config.py:318` `case_sensitive: True` + `ENV` default `"dev"` means a deployment using
  `Env=prod` **silently keeps `ENV="dev"`**, and every Discord alert is stamped `[DEV]`
  (`notifications.py:28, 58`). Production pages are indistinguishable from dev pages.
- `config.py:319` `extra: "ignore"` — a typo'd `DISCORD_WEBHOOK_ALERT` silently disables
  alerting; `YOUTUBE_COOKIES_PAT` silently loses authenticated YouTube access.
- `config.py:262-289`: when `total_keys <= reserved_count`, all three rings get the
  **entire** pool ("CHAOS MODE"), degrading with only a `logger.warning` — no event, no
  alert. A quota-control mechanism failing open.
- `state.py:166-172`: `_daily_audio_cap()` falls back to a hardcoded `30` when settings
  fail to load. If an operator had set it to 10, the fallback silently **raises** the paid-STT
  budget 3×.

---

## 7. Ops tooling

Ordered by blast radius.

| Script | Dry run | Prod guard | Error handling | Notes |
|---|---|---|---|---|
| `recover_failed_runs.py` | ✅ | ❌ | ❌ none | `--apply --all` wipes the entire `frames/` tree and resets every video. No `--yes`, no threshold, no manifest before deletion. `vault.delete_files()` and the DB resets are unguarded, so a partial failure leaves the exact state the tool exists to repair. Also `list_files("frames/")` loads the **whole** vault listing into memory before filtering. |
| `run_live_pipeline.py` | ❌ | ❌ | ⚠️ | Writes to production **by default** and force-`UPDATE`s a real row to `PENDING` (`:132-144`), then runs three real agents. No `--apply`. |
| `repaint_vault_images.py` | ✅ | ❌ | ❌ **none** | 64 lines, no `try`/`except`, resets every video on `--apply`. |
| `migrate_vault.py` | ✅ | n/a | ✅ | `:88` embeds the HF token in a git remote URL → lands in `.git/config` and in `ps` argv. The same file uses `HfApi` correctly elsewhere. |
| `scrub_hf_cache.sh` | ❌ | ❌ | ⚠️ | `rm -f` inside `while true` with **no loop exit** and an unvalidated `$CACHE_DIR`. `-mmin +5` will truncate blobs a live job is still reading. Untracked. |
| `check_quota_exhaustion.py` | n/a | ❌ | ❌ none | **No `if __name__` guard** — top-level `open(env_path)` and live network calls execute at import. Also reimplements `config.key_rings`, a silent-drift risk. |
| `test_youtube_apis.py` | n/a | ❌ | ✅ | Prints raw secret prefixes: `api_key[:20]` (`:67`), `hf_token[:20]` (`:255`), `db_url[:50]` (`:303`) — a Neon DSN prefix contains the username. `check_quota_exhaustion.py` masks correctly; this one doesn't. |
| `setup_orchestration.py` | ❌ | n/a | ✅ | CI-invoked with `\|\| true` (§2.5). `read_deployments(limit=200)` with no pagination → silent truncation. Non-atomic, no rollback. Good inline documentation of a past incident. |
| `audit_transcripts_audio.py` | ✅ | ❌ | ✅ | Reasonable. |

`tools/migrate_vault.log` is a committed 229-line run log. Clean of secrets, but run logs
should not be version-controlled.

---

## 8. Deployment assets

### 8.1 `pleiades-ingestion.service` — zero hardening

Runs the entire production ingestion fleet as `ubuntu` with the full read-write access of
that account, driving yt-dlp, ffmpeg and network I/O in-process.

**Present:** `User=ubuntu`, `CPUQuota=180%`, `TasksMax=256`, `MemoryMax=4G`,
`Restart=on-failure`, `TimeoutStopSec=30`, `KillMode=control-group`.

**Missing:** `NoNewPrivileges`, `PrivateTmp`, `ProtectSystem`, `ProtectHome`,
`PrivateDevices`, `RestrictAddressFamilies`, `CapabilityBoundingSet`,
`SystemCallFilter`, `ReadWritePaths`, `UMask`.

It can read `~/.ssh/` and write anywhere under `/home/ubuntu`. The process parses
attacker-supplied YouTube content via yt-dlp with no sandbox. `TasksMax=256` is a
workaround for a past thread-exhaustion incident, not a security control.

Also 8 hardcoded `/home/ubuntu/...` paths, the username 3×, and internal IP `10.0.0.22`.
No `EnvironmentFile=` — it relies on `atlas.config` implicitly loading `.env` from
`WorkingDirectory`, which works but is fragile and invisible.

`youtube-proxy.service` is slightly better (`NoNewPrivileges=true`, `PrivateTmp=true`) but
see §5.4.

### 8.2 SQL migrations — mostly disciplined, one dangerous file

| File | Idempotent | Locks hot table | Rollback | Verdict |
|---|---|---|---|---|
| `20260902_claim_indexes_v2.sql` | ✅ `IF NOT EXISTS` | ✅ `CONCURRENTLY` | ✅ | **Best in repo.** `\set ON_ERROR_STOP on`, explicit "do not wrap in a transaction", partial indexes matching the real claim predicates. |
| `..._v2_rollback.sql` | ✅ | ✅ `CONCURRENTLY` | — | Clean. |
| `20260902_retire_unsafe_row_limit.sql` | ✅ | ✅ function object only | ❌ none, **explicitly justified** | The removed function chose only the first PK column, so invoking it against `video_stats_log` could delete all history. Code-only, no caller, no data loss. The missing rollback is a documented decision, not an oversight. |
| `20260902_tracker_durable_state.sql` | ✅ mostly | 🔴 **`ACCESS EXCLUSIVE` held for the whole file** | ⚠️ partial | **The dangerous one.** See below. |
| `..._rollback.sql` | ✅ | ⚠️ explicit `SHARE ROW EXCLUSIVE` | — | **Best rollback in the repo** — refuses to downgrade if `DORMANT` rows exist, and re-checks under the table lock to close the TOCTOU race. |

`20260902_tracker_durable_state.sql:10-92` wraps everything in one `BEGIN … COMMIT`, so a
single `ACCESS EXCLUSIVE` lock is held from the first `ALTER` at `:15` until `COMMIT` at
`:92`. Inside that window **every read and write on `watchlist` is blocked** — including
the Tracker, which is the hottest writer at a **60-second** cycle
(`orchestrator.py:62`). The file also does two full-table `UPDATE`s (`:24-26`, `:28`), two
`SET NOT NULL` (each requiring a full scan to prove no NULLs) over ~81k rows, and two
`VALIDATE CONSTRAINT` passes.

**There is no `SET lock_timeout` and no `SET statement_timeout` anywhere in the file** — so
rather than failing fast it queues indefinitely behind the 60 s Tracker cycle. The file's
own comment (`:76-78`) records that a *previous* attempt "produced an unacceptable update
plan on the 2-core production VPS and was rolled back." The lesson recorded was "don't
backfill"; the lesson not recorded was "set a lock timeout."

The good parts are genuinely good: `NOT VALID` + `VALIDATE CONSTRAINT` is the correct
low-lock pattern; `ADD COLUMN IF NOT EXISTS` and both constraint adds are guarded by
`pg_constraint` existence checks; the expensive 81k-row backfill is **deliberately left
commented out** with a preflight count query and a "run only after review" note. That is
disciplined work. It just needs `SET lock_timeout = '3s'` at the top.

Also: `20260902_claim_indexes_v2.sql` has no `lock_timeout`, so a `CONCURRENTLY` build can
wait on a long transaction indefinitely.

### 8.3 Containers

- `Dockerfile` / `docker-compose.yml` were not hardened-reviewed in depth; the systemd
  topology is the production path and the README says so explicitly.
- `docker-compose.yml` passes credentials via **environment variables**, which are visible
  in `docker inspect` and to any process in the container. For a pipeline holding a YouTube
  cookie jar, a Discord webhook and an HF token, prefer mounted `0600` files.
- No `HEALTHCHECK` and no `USER` directive were found in the compose files. The images run
  as root.
- Base images are floating tags, not pinned digests.

---

## 9. Tests

### 9.1 Inventory

| Suite | Tests | Result |
|---|---|---|
| `maia/tests` | 187 | ✅ all pass |
| `atlas/tests` | 124 | ❌ 1 fails |
| `mcp/tests` | 12 | ✅ all pass |
| `alkyone/tests/test_guard.py` | 11 | ✅ all pass (untracked) |
| **Total** | **334** | |

Root `pyproject.toml` also lists `training/evaluation/tests` in `testpaths`, but
`make test-unit` never runs it. Either run it or drop it from `testpaths` — as written, a
root-level `pytest` invocation would pick up tests that CI never sees.

`maia/pyproject.toml`'s `[tool.pytest.ini_options] markers` block is **entirely commented
out**, so there is no `unit`/`integration`/`slow` marker discipline despite the README
describing a layered strategy. Meanwhile `alkyone` and `maia` both use
`@pytest.mark.integration` / `@pytest.mark.live` freely.

### 9.2 Well covered

`orchestrator` (the interval/kwargs/jitter contract is asserted against production
values), `scribe`, `singer`, `streamer`, `tracker` (missing-video and `DORMANT`
semantics), `janitor` (dry-run and failure accounting), `purge`, `storage`, the vault
metrics path (`test_vault_metrics.py` covers missing / corrupt /
partial-write-before-manifest / conflicting-duplicate), and the typed YouTube error
classifier (10 parametrized cases).

### 9.3 Untested and high-risk

| Module | LOC | Why it matters |
|---|---|---|
| `maia/utils.py` | 138 | `looks_like_rate_limit` (7 regexes — the *entire* heuristic deciding DONE vs FAILED), `vault_op_with_retry` (the §4.1 retry stack), `notify_quota_exhausted`. Zero direct tests. |
| `maia/media/streamer.py` | 584 | 584 LOC of subprocess construction, 4 indirect tests. `yt_dlp_base`, `_JS_RUNTIME` discovery, `download_raw`, `extract_video`, `extract_audio_ffmpeg/chunk`, `_parse_json3_file` — all untested. |
| `maia/base.py` | 117 | The Template Method every consumer inherits; `raise_on`, `store_results`/`after_cycle` hook ordering untested. |
| `maia/__main__.py` | 200 | **Zero tests.** The entire CLI dispatcher, including the `purge requires --confirm` guard and the exit codes. |
| `mcp: list_artifacts` | — | Zero coverage — which is exactly why §5.1 survived. |
| `mcp: media.py`, `summarize.py` | 324 | Zero coverage. The Mistral HTTP call, 429/non-200 mapping, the key-pool fallback chain. |
| `mcp: main()` / DNS-rebinding allowlist` | — | Zero coverage. |
| `scribe/grok.py`, `scribe/mistral.py` | 232 | HTTP status → exception mapping untested. `CallPacer.wait()` untested (and unused by Grok). |
| `maia/quality/*` | 318 | `from_settings()` untested; `_load_channel_stats`' `except Exception: return {}` untested; the `executor is None` early return untested. |
| `atlas/vault.py` | 979 | `store_json`, `append_metrics`, `delete_files`, `store_visual_evidence` (single-video) untested. |

### 9.4 A test that gives false confidence

`maia/src/maia/quality/enrich.py:50-55` declares a `thresholds` parameter, then
unconditionally shadows it at `:82`:

```python
thresholds = QualityThresholds.from_settings()   # shadows the argument
```

Every downstream use reads the settings value. `tests/test_filtering.py:192-219` passes an
explicit `thresholds` and the test **passes** — only because the values happen to coincide
with the `atlas/config.py` defaults. It is untested dead API, and the test hides it.

Also `enrich.py:106-118`: the `_probe` closure captures `session`, a local assigned **four
lines later**. Correct today only because the `gather` is inside the `async with`. Any
refactor that hoists the `gather` out becomes a runtime `UnboundLocalError`. Pass
`session` as an argument.

### 9.5 Orphaned test bytecode

`maia/tests/__pycache__/` contains `test_dbg2`, `test_dbg3`, `test_dbg4`,
`test_integration`, `test_validation` with no corresponding `.py` — evidence of removed
debug tests.

---

## 10. Documentation

The docs are unusually good in structure and unusually wrong in detail. Specific,
checkable falsehoods — all worth fixing because each one costs an operator a debugging
session:

| Claim | Reality |
|---|---|
| `README.md:285-291` "50 raw fetches/hour, 50 audio, 40 frames on the 2-core executor" | Not derivable from the code; §4.2 shows the configured cadences are fiction (painter can take ~100 min/cycle). |
| `README.md:128` raw reclaimed "once audio + visuals are DONE **or the TTL window expires**" | §3.3 — neither condition is reachable. |
| `alkyone/README.md:13-15` "YouTube is **NOT** exercised against the live API by default" | §5.7 — false; genuine prod keys make the suite run live. |
| `alkyone/README.md:49-56` file layout (`components/atlas/test_smoke.py`, `components/maia/test_integration.py`, `test_validation.py`) | None of those files exist. |
| `alkyone/README.md:19-29` documents a two-var guard | The real gate needs five (`ALKYONE_ALLOW_INTEGRATION`, `ALKYONE_TEST_DATABASE_URL`, `ALKYONE_TEST_VAULT`, `PLEIADES_PROD_DATABASE_URL`, `PLEIADES_PROD_VAULT`). |
| `maia/README.md` "184 tests covering: hunter/, tracker/, scribe/, painter/, janitor/, archeologist/" | 187 tests, and the list omits 12 of 20 files — including `orchestrator` and `storage`, two of the three most safety-critical. |
| `maia/README.md:731` `python -m maia muralist --height 480` | `height` is accepted and **discarded** (`muralist/flow.py:102-104`). The flags `--video-id` / `--store` don't exist either. |
| `maia/README.md:532,577` `maia-streamer`, `maia-singer` console scripts | Not registered in `pyproject.toml:33-38`. |
| `maia/README.md` "Paid transcribers (Mistral/Grok) are paced with a process-wide `CallPacer`" | `grok.py` has no pacer, and Grok is the *preferred* provider. |
| `schema.sql:217-220` "Per-table row caps (enforced by Janitor sweep)" | The row-cap code was retired. Nothing bounds these tables now (§6.2). |
| `maia/ENV.example:69-82` documents `MAIA_COOKIES_PATH` and `MAIA_PROXY_URL` | Zero code reads either. Cookies come from `atlas.config`; the proxy from `atlas.egress`. |
| `README.md` config table lists `HUNTER_BATCH_SIZE`, `PAINTER_BATCH_SIZE`, `SCRIBE_BATCH_SIZE`, `TRACKER_BATCH_SIZE` | None exist; batch sizes are module constants + `orchestrator.build_specs()` kwargs. |
| `mcp/README.md:7-15` tool table lists 7 tools | There are 8; `get_thumbnail` is missing. |

`README.md` also links `docs/challenges.md` (lowercase) in two places; the file is
`docs/CHALLENGES.md`. On a case-sensitive VPS that 404s.

Two documentation issues with real consequences:

- **`maia/src/maia/scribe/flow.py:42-49`** documents proxy-only egress for the Scribe as a
  load-bearing mitigation, citing the `prefect.yaml` deployment env. In the single-process
  orchestrator there is **one** `os.environ` and one `EgressPool` module global
  (`media/streamer.py:31`) round-robined across all four YouTube-bound agents. The Scribe
  can no longer be pinned to proxy-only egress. The mitigation the 20-second throttle is
  calibrated for is silently gone.
- **`deploy/systemd/pleiades-ingestion.service:16`** — the in-code documentation is right
  about the topology, but `docs/deploy.md` still describes Prefect as orchestrating
  ("Prefect server/API :4200"), which §`prefect.yaml:1-6` explicitly contradicts by
  omitting all schedules.

Also: `atlas/`, `maia/`, `mcp/`, `alkyone/` each have their own `pyproject.toml` with
near-duplicate ruff/mypy config, and the root `pyproject.toml` has a *third* copy of the
ruff config. The `.pre-commit-config.yaml` has **8 near-identical ruff hooks** (2 checks ×
4 components) that exist only because ruff's `files:` filter can't span per-component
`pyproject.toml` discovery. This is a lot of duplication to maintain for no benefit — one
workspace-level ruff config with per-component `mypy` would do.

---

## 11. Recommended order of work

### Blockers — do before anything ships
1. **Fix the CD gate** (§2.1, §2.2, §2.3). `conclusion == "success"` only; assert the job
   set is present; better, move to `workflow_run`. Deploys currently proceed with lint and
   tests skipped.
2. **Fix `mark_step_failed`** (§3.1) so it sets only `{step}_phase`, not row-level
   `status`. This un-wedges every video currently stuck behind one transient error, and it
   must land *before* the 3NF rewrite (§3.13) entrenchches the behaviour.
3. **Commit `deploy/`** (§2.4). The migrations and systemd units are the only record of how
   production was altered, and they exist in no branch.
4. **Fix the red tree** (§1): the import order and the one failing assertion.
5. **Path-traversal fix in `list_artifacts`** (§5.1) — one-line validation, no auth required
   to exploit today.

### High value, contained
6. **Add auth (or a real reverse proxy) to the MCP server** and stop serving the artifact
   tree from a bare `http.server` on `0.0.0.0` (§5.2, §5.3). Add duration/cost caps to
   `get_audio`, a frame cap to `get_keyframes`, and eviction to `ArtifactStore`.
7. **Make the vault backoff async and give it a dedicated executor** (§4.1). This is the
   difference between "the pipeline occasionally stalls" and "the pipeline deadlocks and
   then hangs on SIGTERM for an hour."
8. **Add `lock_timeout` to every migration** (§8.2), especially
   `20260902_tracker_durable_state.sql`.
9. **Make the Alkyone guard fail closed everywhere** (§5.5) — one function, no
   `_matches`-returns-False-on-empty. Add the YouTube key pool and cookies to the guard,
   and either delete or properly gate `PLEIADES_USE_PRODUCTION_VAULT`.
10. **`StrictHostKeyChecking=yes`** on the proxy tunnel, with a committed host key (§5.4).
11. **Fix the phase-sync trigger** so an explicit phase is not discarded on INSERT (§3.2),
    then make `reclaim_raw_if_complete` actually work (§3.3).
12. **Implement real claim leases** (§3.6) — `lease_expires_at` set in the claim's own
    transaction. This is the structural fix for the double-claim and `PROCESSED → PENDING`
    regressions.

### Medium — schedule normally
13. Add the two missing `__init__.py` files (§6.1) — one-line packaging bug that breaks any
    wheel build.
14. Make the quality gate fail **closed** on enrichment errors, and log the channel-gate
    bypass (§3.7).
15. Stop marking `has_transcript = TRUE` without a transcript (§3.8).
16. Handle `TimeoutExpired` in the 4 bare yt-dlp sites, and make the "is this retryable?"
    decision consistent across agents (§3.12).
17. Harden `pleiades-ingestion.service` (§8.1) — `NoNewPrivileges`, `ProtectSystem=strict`
    + `ReadWritePaths`, `PrivateTmp`, `ProtectHome=read-only`,
    `RestrictAddressFamilies=AF_INET AF_UNIX AF_NETLINK`.
18. Fix `next_key()` to honour the blacklist, and delete the `attempt_rotation`
    pre-consumption workaround (§6.9).
19. Surface `stats_error` and flush failures in the Janitor's Discord embed (§3.10);
    populate `fleet_down` (§3.11).
20. Add `CHECK` constraints to the enum-as-text columns (§6.4), the `duration` index and the
    `claim_vault_pending_batch` index (§6.6), and the `watchlist → videos` FK (§6.3).
21. Add guard tests for `PLEIADES_USE_PRODUCTION_VAULT`, and stop committing run logs (§7).

### Cleanup
22. Delete `channel_history`, `CREATE EXTENSION vector`, `videos.audio_pending`,
    `videos.wiki_topics`; fix or remove the three useless indexes (§6.2).
23. Remove the documented-but-nonexistent CLI flags, console scripts, and env vars, or
    implement them (§10).
24. Delete the four dead `train_*.yml` workflows, or fix their image targets (§2.6).
25. Consolidate the three ruff configs and the 8 pre-commit hooks (§10).
26. Add the `pytest` markers that `alkyone` and `maia` already use, and either run
    `training/evaluation/tests` or drop it from `testpaths` (§9.1).
27. Close the DB pool on shutdown (§4.3); add `statement_timeout` to the pool (§4.4).

---

## Appendix A — What is genuinely good

Worth stating plainly, because the fix list above is long and it would be easy to lose
sight of the fact that most of this code was written carefully:

- **The five partial claim indexes** (`schema.sql:197-208`) match the claim queries'
  predicates *and* sort directions exactly, including the deliberate newest-first
  `discovered_at DESC` for the streamer and the `PROCESSED`-inclusive singer index — and
  they are covered by a schema contract test.
- **The content-addressed metrics path.** `store_metrics_batch` writes Parquet first,
  manifest last, then reads the manifest back and compares. `read_metrics_batch` verifies
  SHA-256, row count, and recomputes the batch id. Fails closed on missing, corrupt, and
  partial-write-before-manifest. That is a genuinely well-engineered durability story.
- **`orchestrator`'s contract tests** assert the real production intervals, kwargs, and
  first-tick jitter — so a silent cadence change fails CI.
- **No bare `except:` and no `except Exception: pass`** anywhere in `src/`. `sys.exit()`
  appears exactly once, in the CLI entry point. `QuotaExhaustedError` is a catchable
  exception, not a process kill, and that is tested.
- **`ResiliencyExecutor`'s typed error classification** is the best-tested part of atlas
  (10 parametrized cases) and correctly distinguishes `quota` / `dead_key` / `fatal`.
- **`COMPLIANCE_MODE` refusing to truncate the API key pool**, with a regression test.
- **The strongest rollback script in the repo** refuses to downgrade when `DORMANT` rows
  exist and re-checks under the table lock to close the TOCTOU race.
- **The migration that retired the unsafe row-limit function** explains in a comment exactly
  why it had no rollback — because it could have deleted all history for selected subjects.
- **Alkyone's session gate is fail-closed**, and `test_production_refusal_does_not_disclose_endpoint`
  asserts the refusal message does not leak the DSN. Someone thought about that.
- **No credential is in git history.** `.env`, the cookie jar, and all `*.part` artifacts
  are untracked and gitignored; secrets are `SecretStr`; no API key is hardcoded anywhere
  in `atlas/src` or `maia/src`.

---

# Part 2 — Remediation log (appended after acting on the report)

All work below is on branch `september-audit`. `make lint` and `make test-unit`
are green: **379 tests pass** (was 334, of which 1 failed), across atlas 146,
maia 196, mcp 20, alkyone 17.

Every change below was verified — most by a new regression test, the schema and
migration changes by running them against a real PostgreSQL instance in a
throwaway database (the `pleiades` database was never touched).

## Blockers

| # | Finding | Fix | Verified by |
|---|---|---|---|
| 2.1 | CD gate accepted `skipped` as pass | `cd.yml` now triggers on `workflow_run` of the `CI` workflow and requires `conclusion == "success"` | Simulated all four conclusion/branch combinations |
| 2.2 | Gate selected a job name that does not exist | Removed job-name matching entirely — the workflow-level conclusion is authoritative | Same |
| 2.3 | Docs-only push to `main` hung 30 min then failed | No CI run → no deploy, which is correct for a non-deployable commit | Logic review |
| 2.4 | `deploy/` untracked, never deployed | Files now written and verified; **still needs a commit** (see below) | `systemd-analyze verify`, migrations executed |
| 1.1 | `make lint` red | Import order fixed | `make lint` exit 0 |
| 1.2 | `make test-unit` red | Fixed the *code*, not the test: partition validation now reports "partition", and `MetricsManifest.from_dict` no longer masks a specific nested error with a generic one | `test_vault_metrics.py` |
| 3.1 | One failed step wedged a video across all five consumers | `mark_step_failed` now writes only `{step}_phase`, never row-level `status`. Each claim excludes **its own** FAILED phase, so a dead stage stops retrying while the rest of the pipeline advances. Recovery path (`reset_failed_to_pending`, `count_failed_steps`, heartbeat, `tools/recover_failed_runs.py`) migrated from `status='FAILED'` to the phase columns, since nothing sets `status='FAILED'` any more | 7 new tests + live-Postgres end-to-end (§ below) |
| 5.1 | Path traversal in MCP `list_artifacts` | Ids validated through `_video_id_from_url`, plus a per-file containment check that also defeats symlink escapes | 3 new tests; exploit re-run and confirmed dead |
| 2.4a | `deploy/`-adjacent: `tools/setup_orchestration.py` failures swallowed | `|| true` removed — a misconfigured control plane now blocks the restart | — |
| 2.7 | Deployed commit ≠ CI-verified commit | `git checkout --detach $DEPLOY_SHA` replaces `git pull --ff-only` | — |

## High value

| # | Finding | Fix |
|---|---|---|
| 4.1 | Vault back-off could park a thread ~35 min (×3 outer retries ≈ 1.8 h) on the **shared** 6-thread default executor | (a) Back-off budget cut to ~2.5 min: `max_attempts` 8→5, `base_delay` 30→10, cap 600→120 s, with jitter so agents do not retry in lockstep. (b) Permanent errors (401/403/404/410) are no longer retried at all. (c) Vault I/O moved to a dedicated 2-thread `vault-io` pool, so a saturated vault can only starve other vault work — never yt-dlp/ffmpeg. `atexit` shutdown does not wait on parked threads. (d) The two direct `run_in_executor(None, v.store_batch, …)` call sites now go through the wrapper |
| 3.2 | `sync_step_phases` discarded explicitly-written phases | Per stage, precedence is now: boolean TRUE wins → an explicitly changed phase wins → otherwise the phase resets to PENDING (preserving a terminal FAILED). The old form tested `NEW.fetched IS DISTINCT FROM OLD.fetched`, which is always true on INSERT because `OLD` is NULL |
| 5.5 | Alkyone guard failed open when prod vars were unset | `assert_not_production` is now fail-closed: a missing `PLEIADES_PROD_*` is a refusal. It is safe to call standalone, which matters because `fresh_db` calls it immediately before `TRUNCATE`-ing every table in the public schema |
| 5.6 | `PLEIADES_USE_PRODUCTION_VAULT` bypass | Now also requires `ALKYONE_ALLOW_LIVE=1`, because the suite *deletes* files from the dataset during cleanup |
| 5.7 | Guard covered 2 of 6 production targets | The YouTube key pool is now guarded with the same explicit-reference pattern (`PLEIADES_PROD_YOUTUBE_KEY_POOL_JSON`, added to the CI workflow env); a real cookie jar logs a warning |
| 5.4 | SSH host-key checking disabled on the tunnel carrying all YouTube traffic | `StrictHostKeyChecking=yes` with a pinned `deploy/ssh/known_hosts` (intentionally empty, so the tunnel **refuses to start** until it is populated out-of-band) + `deploy/ssh/README.md` |
| 8.1 | `pleiades-ingestion.service` had zero hardening | `NoNewPrivileges`, `ProtectSystem=strict` + `ReadWritePaths`, `ProtectHome=read-only`, `PrivateTmp/Devices`, `SystemCallFilter=@system-service`, empty `CapabilityBoundingSet`, `UMask=0077`, `RestrictAddressFamilies` |
| 3.7 | Quality gate failed open | Discovery pages are now **dropped and not acknowledged** (so they are retried) instead of ingested unfiltered; the `channels.list` swallow that silently disabled the channel gate now propagates |
| 3.8 | `has_transcript=TRUE` with no transcript | The "no transcript available" path now records a notice, exactly like the sibling "too long" path |
| 8.2 | Migrations queued behind the 60 s Tracker cycle | `SET lock_timeout='3s'` + `statement_timeout='120s'` in all five migration files. `CREATE INDEX CONCURRENTLY` is still outside a transaction |

## Medium

| # | Finding | Fix |
|---|---|---|
| 5.2/5.3 | MCP: no auth, `0.0.0.0` static server, unbounded cost | Artifact static server now binds loopback by default and **refuses** a non-loopback bind without an explicit `ART_ALLOW_UNAUTHENTICATED=1`. `get_audio(transcribe=True)` refuses videos over `MAX_AUDIO_SECONDS` (1 h) *before* downloading. Inline keyframes capped at 6 (URL-only path at 60). `ArtifactStore` gained an LRU-ish size budget (2 GB) — it previously had no eviction, TTL, or cap at all. Blocking audio work moved off the event loop |
| 6.1 | `maia/singer` and `maia/streamer` had no `__init__.py` (wheels would omit them) | Added, exporting the names that actually exist |
| 6.9 | `KeyRing.next_key()` ignored the blacklist | It now skips dead keys and raises `QuotaExhaustedError` when the ring is empty. Separately, `attempt_rotation` no longer pre-consumes a key, so a 1-key ring stopped reporting exhaustion **without making a single HTTP request** |
| 3.11 | `fleet_down` was dead; heartbeat could report SUCCESS while deployments were down | Removed the dead variable; down deployments now escalate to WARNING and appear in the summary as `deployments_down`/`deployments_warn`, while still not marking the executor unhealthy (the documented intent) |
| 3.10 | Janitor reported INFO for a totally failing vault flush, and `stats_error` was never rendered | Alert level now accounts for archive failures, flush failures, and stats-archival error; all are in the embed |
| 9.4 | `filter_by_quality`'s `thresholds` parameter was dead API, shadowed at line 82 — and a test passed anyway because the values coincided with config defaults | Parameter is now honoured; the fragile closure over a later-assigned `session` takes it as an argument |
| 6.8 | `painter` created a `mkdtemp` it never used; `download_raw`/`transcribe_video`/etc. dead | `painter` tmpdir removed; the genuinely-unused public helpers were left alone pending a decision (see below) |

## Two things I did **not** do, and why

1. **`deploy/` is still untracked.** I wrote and verified every file, but the
   working tree already contained 98 unrelated modified files from prior WIP.
   Committing that alongside these changes would be unreviewable, so the commit
   is left to you — it is the single highest-value follow-up, because CI deploys
   with `git pull --ff-only` and cannot ship migrations that are not in a branch.

2. **Claim leases (§3.6) are not implemented.** The double-claim between the
   Streamer and the Scribe is real (`FOR UPDATE SKIP LOCKED` releases at end of
   statement in autocommit, so the "claim" is not durable). The structural fix is
   a lease column written in the claim's own transaction. That is a schema
   migration plus a change to all five claims, and it wants to land after the
   3NF rewrite rather than in the same change.

## New findings surfaced while fixing

- **`maia/tests/conftest.py` patches `asyncio.sleep` globally with an `AsyncMock`
  for every test.** Any test that awaits it as a yield gets a no-op, so timing
  assertions pass vacuously — it silently defeated one of my own tests. It is the
  reason three of the four autouse fixtures exist (for the Prefect `@task`
  Muralist). Worth scoping to the tests that need it.
- **`_apply_schema`'s "skip and continue" never works** (`atlas/db.py:96-108`):
  it catches `UndefinedFunction` inside one implicit transaction, leaving it
  aborted, so every later statement fails with `InFailedSqlTransaction`. It is
  unreachable only because `timescale_available` is pre-checked. There is no
  savepoint and no per-statement commit.
- **Running `schema.sql` by hand fails on a plain PostgreSQL** at
  `CREATE EXTENSION vector` and `SELECT create_hypertable(...)`. `db.py` guards
  the hypertable case but nothing guards `vector`, which the audit also found is
  never used.

## Verification performed

- `make lint` → exit 0 (ruff check + format + mypy across all four components).
- `make test-unit` → 379 passed, 0 failed.
- `schema.sql` trigger exercised against real PostgreSQL across 8 scenarios
  (explicit phase on INSERT, boolean-TRUE latch, claim, per-step failure,
  release, terminal-FAILED preservation, archive, contradictory write).
- All 5 SQL migrations applied and rolled back against a throwaway database;
  resulting `watchlist` schema confirmed. `CREATE INDEX CONCURRENTLY` verified
  still outside a transaction.
- Both systemd units pass `systemd-analyze verify`.
- `cd.yml` gate simulated across success/failure/cancelled/wrong-branch.
- `alkyone.yml` and `ci.yml` parse as valid YAML; all embedded shell passes
  `bash -n`.
- MCP traversal exploit re-run post-fix: refused.
- Vault executor isolation proven by saturating the pool and showing the default
  executor still responds immediately.
