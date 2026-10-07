> Historical record. Use the current documentation index and October audit for active guidance.

# Challenges Faced — Engineering Log

A running record of the hard problems encountered while building and operating
Pleiades, and how each was resolved (or is still open). Written for the next
operator so the same trap isn't fallen into twice.

---

## 1. YouTube 2026 anti-bot stack (PoToken + Deno + cookies + impersonation)

**Problem.** In 2026 YouTube requires a per-video **Proof-of-Origin (Po) token**
for authenticated GVS / subtitle requests, and runs a BotGuard JS challenge.
Plain `yt-dlp` gets `LOGIN_REQUIRED` / hard rate-limits from the VPS egress IP.

**Resolution.**
- `bgutil-ytdlp-pot-provider` runs as a separate Node server (`bgutil-provider`
  systemd unit, `127.0.0.1:4416`); yt-dlp's built-in plugin calls it for PoTokens.
- `deno` ≥ 2.3.0 on `PATH` solves the BotGuard JS challenge (`--js-runtimes deno:`).
- TLS impersonation (`--impersonate Chrome-*`) + a Netscape cookies file
  (`YOUTUBE_COOKIES_PATH`) for authenticated sessions.
- The stale `yt-dlp-get-pot` package conflicts with the built-in framework —
  kept uninstalled.

**Still sharp.** Cookies must be exported from an incognito session and closed
without logging out, or YouTube rotates and invalidates them. A malformed cookies
file makes the streamer release videos non-fatally (never fetched).

---

## 2. Streamer PoToken bug — `ANDROID_VR` silently kills every fetch

**Problem.** `streamer.py` used `player_client="default,tv"`. `default` expands to
include `ANDROID_VR`, which the bgutil provider **does not support**. yt-dlp only
fetches a PoToken **once** (for the first client = `ANDROID_VR`); because that
client is rejected, **no PoToken is ever produced**, and every subsequent client
(including the supported `tv`) is sent without one → `LOGIN_REQUIRED` for all
videos. The whole raw-fetch stage was dead.

**Resolution.** Changed the default to `player_client="web,tv"` — both are
supported by bgutil. Verified: a manual streamer run completed and `raw_phase
DONE` advanced for the first time.

---

## 3. Concurrency death-spiral (load 26–27 on 2 cores)

**Problem.** The `default` work pool had **no global `concurrency_limit`** (unlimited).
With 9 deployments each claiming `max_active_runs: 1`, up to 9 flow-runs started at
once, each spawning ffmpeg / yt-dlp → dozens of CPU-bound processes → load 26–27 →
the box became unresponsive.

**Resolution.**
- Global work-pool cap (`concurrency_limit = 9` after tuning; see §4).
- Per-deployment **work queues** (`concurrency_limit = 1` each) — the *only*
  mechanism that actually enforces per-agent isolation in Prefect 3.7.7.
- cgroup backstop on the worker: `CPUQuota=180%`, `TasksMax=120` — the box can
  never be fully saturated even if Prefect is misconfigured.

---

## 4. Priority starvation — the streamer never got a slot

**Problem.** An earlier config set the pool `concurrency_limit = 5` *with*
queue priorities (heartbeat=1 … streamer=9). The five cheap-but-frequent agents
(heartbeat/janitor/archeologist/tracker/hunter) consumed all five slots, and the
**streamer** (the pipeline *input*) was starved. Singer/painter were also starved.
Result: 23,741 PENDING raw videos, no new fetches, downstream agents idle.

**Resolution.** Raise the pool limit to **9** (≥ number of queues needing a slot)
so every queue always has a fair chance. Priorities are now cosmetic.

> **Gotcha.** Per-deployment `max_active_runs` and `Deployment.concurrency_limit`
> are **silently ignored** in Prefect 3.7.7 — only the work-queue `concurrency_limit`
> works. Deployments must set `work_queue_name` or they fall back to the unlimited
> `default` queue and isolation breaks.

---

## 5. The `FAILED` dead-end

**Problem.** Every claim gate selects `status IN ('PENDING','PROCESSING',…)` and
**excludes `FAILED`**, so a failed video is never reclaimed. Three failure shapes
piled up:
- **audio_stuck** — singer's `extract_audio` had a hard 180s timeout and transcoded
  the whole track; long videos timed out → `mark_step_failed`.
- **transcript_stuck** — live / no-caption videos; scribe's loader raised a
  non-`TranscriptExtractionError` → hit a generic `except` → `mark_step_failed` (dead-end)
  instead of releasing.
- **visuals_stuck** — painter frame grabs stranded purely by the dead-end.

**Resolution.** Made transient failures non-fatal with stage-specific retry
transitions (for example, `release_audio_to_pending()`), so unrelated raw state
is preserved and retryable rows are re-queued. Permanent failures now mark the
specific stage `FAILED` without overwriting a newer completion. Singer chunks long audio so each
ffmpeg call stays under the timeout. Scribe treats any caption-loader exception as
"no captions" (re-raising `QuotaExhaustedError` so backoff still works). A one-off
`UPDATE videos SET status='PENDING' WHERE status='FAILED'` drained the backlog
(145 → 0) without the blunt frame-purge tool.

---

## 6. Micro SQLite lock storm

**Problem.** The control plane is a 1/8-OCPU box on SQLite. Under write contention
(e.g. right after a restart) the server 503s with `database is locked`, which
blocked `prefect deploy` and run scheduling.

**Resolution.** Server connection URL patched to `?timeout=30` (busy_timeout 5s →
30s) so writes retry. Durable fix would be migrating the orchestration DB to
Postgres.

---

## 7. Orphaned flow-run slots hold the pool "full"

**Problem.** After a control-plane outage, orphaned flow runs from the pre-outage
worker hold work-pool slots forever (their worker is gone, so Late-detection can't
finalize them). The pool reads as full and the new worker starts nothing.

**Resolution.** Delete the orphans via API (`PENDING`/`RUNNING`/`CRASHED` runs with
no live worker) — `active_slots` drops to 0 and scheduling resumes.

---

## 8. Heartbeat false "everything is down"

**Problem.** `collect_fleet_status()` called `read_flow_runs(limit=500)`. The micro
API **caps `read_flow_runs` at `limit=200`** and returns `422` above that. The
`except Exception` caught the 422 and **masqueraded it as "API unreachable" for every
deployment** — a false total outage.

**Resolution.** Replaced the bulk query with a **per-deployment** query
(`read_flow_runs(flow_run_filter=deployment_id, limit=1)`), which sidesteps the 200
cap. Lesson: never assume a large `limit` is accepted by the micro API.

---

## 9. Muralist raw-reclaim race (open)

**Problem.** `reclaim_raw_if_complete` deletes `raw_uri` once the mandatory consumers
(audio + visuals) are DONE. Muralist (manual-only) needs that same `raw` to build
the clip, but its claim gate requires `raw_uri IS NOT NULL`. If singer+painter finish
before a manual muralist run, `raw` is reclaimed (or aged out past `RAW_TTL_HOURS`)
and muralist can never claim the videos. ~12 "all-done" videos (audio/transcript/
visuals DONE, `has_video=FALSE`, `clip_phase=PENDING`) are stranded in `PENDING`.

**Status.** Open. Options: make the clip stage non-blocking for `PROCESSED`, or run
muralist manually (within the TTL window) against them. Not blocking — they're
otherwise complete.

---

## 10. HF vault HTTP 500 saturating slots (observed)

**Problem.** The HuggingFace vault occasionally throws HTTP 500 while retrying ~89
LFS files; the blocking vault push holds a worker concurrency slot, which can make
a manual trigger go `Late`.

**Status.** Observed, not yet fixed. Mitigations considered: make vault pushes less
blocking / more resilient, or move cold-tier writes off the hot path.

---

## 11. Cookies file format (resolved)

**Problem.** `www.youtube.cookies.txt does not look like a Netscape format cookies
file` → the streamer released videos (non-fatal, but they never got fetched).

**Resolution.** A correctly-formatted Netscape cookies file was supplied at
`pleiades/www.youtube.cookies.txt` and wired via `YOUTUBE_COOKIES_PATH` in
`prefect.yaml` for the streamer/singer/painter/scribe deployments.

---

## 12. Code-audit findings (this session)

A dedicated audit removed dead code and fixed latent bugs:
- **Dead code removed:** `AdaptiveConcurrency` + `_notify_rate_limit_downgrade` +
  `run_in_executor` (maia utils); `ChannelHistory` / `Transcript` models + `VideoStatus`
  alias (atlas); two unreferenced janitor `@task` wrappers; an unused watchlist logger.
- **Bugs fixed:**
  - `quality.py` — `final` was undefined when a batch was fully rejected → `NameError`
    that silently bypassed the quality gate.
  - `heartbeat/flow.py` — a hand-set `multipart/form-data` header (no boundary) made
    the audio-API health probe always report down; the Mistral branch now uses
    `x-api-key` to match `scribe/mistral.py`.
  - `tracking.py` — YouTube `viewCount`/`likeCount`/`commentCount` (strings) were
    inserted into `BIGINT` columns without coercion.
  - `transcript.py` — `clear_vault_pending` latched `PROCESSED` on `has_visuals` alone;
    now requires transcript+audio+visuals like the rest of the state machine.
  - `tools/repaint_vault_images.py` — referenced an undefined `env_path` (NameError at
    import); fixed.
  - `scribe/flow.py` — `tempfile.mktemp` (race-prone) → `NamedTemporaryFile`.

---

## 13. Plans still not fully integrated

The `docs/agent-consolidation-proposal.md` is **partially** implemented:
- **Done:** P1a (claim gates + TTL reclamation), P1b (per-step phases + join barrier),
  P4 (alkyone prod-safety guard).
- **Pending:** P2 (`BaseBatchAgent` to kill ×9 duplication), P3 (decompose the five
  oversized flow files), P4 doc refresh (`docs/architecture.md` / `docs/testing.md`
  still describe the old CI-runner world).
- **Open hardening:** migrate the orchestration DB from SQLite to Postgres.

---

## 14. HF 10k-files-per-directory limit killed the vault migration (resolved)

**Symptom:** the vault migration (`tools/migrate_vault.py --apply`) crashed with
`RuntimeError: Batch commit failed after 8 attempts` and log lines:
`Your push was rejected because it contains too many files per directory. Each directory
in your git repo can only contain up to 10000 files. Offending directories: /transcripts/`.

**Root causes (two, distinct):**
1. **Unreliable diff source.** The script listed files via the HF HTTP `siblings`
   endpoint, which truncates above ~1k files. It reported 27,724 "missing" (== every
   source keep-prefix file) and mass re-uploaded files that already existed in the
   target, silently inflating the transcripts dir toward the limit.
2. **HF hard limit.** The clean vault's `transcripts/` already held 9,990 flat files
   (written by the pipeline during the earlier vault mixup), and HF rejects any commit
   that would put >10k entries in a directory.

**Fix:**
- **Authoritative listing:** switch to git trees (`git clone --no-checkout --depth 1
  --filter=blob:none` + `git ls-tree -r --name-only HEAD`). Correct missing set is
  **10,021** (28 raw + 9,993 transcripts); audio/metadata already complete.
- **Transcript sharding:** new transcripts are written as
  `transcripts/{video_id[:2]}/{video_id}.json` via `atlas.vault.transcript_path`.
  `fetch_transcript` tries the sharded path first, then the legacy flat path.
  Sharded writes also flow through `janitor/flow.py`, `purge.py`, `run_live_pipeline.py`,
  and the e2e test.
- **Verified empirically:** a probe commit confirmed subdirectories do **not** count
  toward a parent directory's 10k-entry limit, so the 9,990 legacy flat transcripts can
  stay in place.

**Also fixed on the way:** the earlier `.env` vault-mixup investigation (518 FAILED
videos) is documented in `docs/vault-migration.md`. The migration is diff-based and
resumable; post-migration the cookies file must be restored and the worker restarted.
