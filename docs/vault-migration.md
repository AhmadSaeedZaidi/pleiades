> Historical record. Use the current documentation index and October audit for active guidance.

# Vault Migration: `pleiades-vault` → `pleiades-vault-clean`

## Context

| Vault | Repo | Type | Storage Used | Status |
|-------|------|------|-------------|--------|
| **Old** (`pleiades-vault`) | `Rolaficus/pleiades-vault` | Private | 99.8 GB | **FULL** — writes return 403 |
| **New** (`pleiades-vault-clean`) | `Rolaficus/pleiades-vault-clean` | Public | 165 GB | Unlimited storage, writable |

The `.env` file at `pleiades/.env:88` was setting `HF_DATASET_ID=Rolaficus/pleiades-vault`
(the wrong repo). **Already fixed** to `Rolaficus/pleiades-vault-clean` — but the
FAILED videos and every artifact that lives only in the old vault still need their
data migrated.

> **Vault mixup**: during a previous window the pipeline was actually *writing* to the
> clean vault while *reading* from the old vault. That is why the clean vault already
> contains many `raw/`, `audio/`, `transcripts/`, `frames/` artifacts (for *different*
> video IDs than the old vault), and why stored artifacts 404'd on read (→ 518 FAILED).
> The migration copies only files that exist in the source and are **missing** in the
> target.

## Artifact Lifecycle

Each video goes through this pipeline, and each step reads/writes specific vault paths:

| Agent | Reads | Writes | Path pattern |
|-------|-------|--------|-------------|
| **Streamer** | — | raw audio stream (bestaudio, no re-encode) | `raw/{prefix}/{id}.webm` / `raw/{prefix}/{id}.m4a` |
| **Singer** | `raw/{prefix}/{id}.*` | extracted speech track | `media/audio/{prefix}/{id}.opus` |
| **Painter** | `raw/{prefix}/{id}.*` + `metadata` | keyframe images | `frames/{id}/{idx}.webp` |
| **Scribe** | sharded audio with flat-file fallback (STT) | transcript JSON | `transcripts/{prefix}/{id}/index.json` |
| **Janitor** | raw/audio/transcripts | archived video JSON | `videos/{id}.json` |

> **`raw/` contains audio, not video.** `download_raw` runs yt-dlp with `-f bestaudio/best`
> and keeps the native container (`.webm`/`.m4a`); the singer extracts the speech track
> later. Video *frames* live in `frames/`; painter uses stream URLs from metadata.

> **Transcript sharding.** To stay under HF's 10,000-entries-per-directory limit,
> transcripts are written as `transcripts/{video_id[:2]}/{video_id}.json`
> (see `atlas.vault.transcript_path`). Legacy flat `transcripts/{id}.json` files remain
> readable via a fallback in `fetch_transcript`. Subdirectories do **not** count toward
> a parent directory's 10k entry limit (verified empirically with a probe commit).

## File Inventory (authoritative — from git trees, Aug 2026)

### Old vault (`pleiades-vault`) — source

| Directory | File count | Notes |
|-----------|-----------|-------|
| `raw/` | 2,989 | Audio streams (webm/m4a). |
| `audio/` | 4,218 | Extracted speech tracks. |
| `transcripts/` | 9,996 | Caption JSON. |
| `metadata/` | 10,521 | yt-dlp info JSON. |
| `frames/` | — | **Skip** (painter regenerates). |
| `metrics/` | — | **Skip** (not pipeline-critical). |

### Clean vault (`pleiades-vault-clean`) — target

| Directory | File count | Notes |
|-----------|-----------|-------|
| `raw/` | 7,712 | Contains pipeline's own writes (mixup era). Only 28 source files missing. |
| `audio/` | 9,972 | Nothing missing. |
| `transcripts/` | 9,990 flat | Pipeline's own flat transcripts. 9,993 source transcripts missing. |
| `metadata/` | 57,826 | Nothing missing. |

**Must-migrate (10,021 total):** `raw/` (28), `transcripts/` (9,993).
**Already complete:** `audio/`, `metadata/`. **Skip:** `frames/`, `metrics/`.

## Migration Script

Located at `tools/migrate_vault.py`.

### Strategy

1. **List files via git trees** (shallow clone, `--no-checkout`, `--filter=blob:none`).
   The HF HTTP `siblings` listing **truncates** above ~1k files and undercounted the
   target — the previous run wrongly computed 27,724 "missing" and mass re-uploaded.
2. **Read from old vault** using `hf_hub_download` (streaming to temp).
3. **Write to clean vault** using `HfApi.create_commit` with `CommitOperationAdd` (batched).
4. **Transcripts are sharded on the target** (`target_path()` maps
   `transcripts/{id}.json` → `transcripts/{id[:2]}/{id}.json`).
5. **Batch size**: 50 files per commit — stays under HF's 128-commits/hour limit and keeps
   each commit small enough to avoid LFS timeouts.
6. **Rate-limit backoff**: exponential backoff (30s → 600s) on HTTP 429.

### Key code patterns

```python
from atlas.vault import transcript_path

def target_path(src_path: str) -> str:
    if src_path.startswith("transcripts/"):
        video_id = src_path[len("transcripts/"):].removesuffix(".json")
        return transcript_path(video_id)
    return src_path
```

### Verification

```bash
python tools/migrate_vault.py               # dry run — git-tree diff counts
python tools/migrate_vault.py --verify      # per-prefix counts
python tools/migrate_vault.py --list-missing  # files still pending
```

Expected post-migration: `Total files to migrate: 0`.

### Post-migration steps

1. Change `.env` setting: **already done** (`HF_DATASET_ID=Rolaficus/pleiades-vault-clean`).
2. **Restore the cookies file** (`www.youtube.cookies.txt` — currently 0 bytes, broke
   scribe STT fallback + painter). Re-export from a browser to `.env`'s `YOUTUBE_COOKIES_PATH`.
3. Restart the Prefect worker: `sudo systemctl restart prefect-worker`.
4. Reset the 518 FAILED videos back to PENDING:
   ```sql
   UPDATE videos SET status='PENDING', fetched=FALSE, raw_uri=NULL, error=NULL
   WHERE status='FAILED';
   ```
5. Monitor streamer log: `tail -f /var/log/pleiades/prefect-worker.log | grep -i "streamer\|fetch_source"`.

## Edge Cases & Quirks

### 0. HF 10k-files-per-directory hard limit
HF rejects any commit that would put **>10,000 entries in a single directory**
(`Bad request … contains too many files per directory`). This is what killed the
previous migration run on `/transcripts/` (target already had 9,990 flat files).
Two consequences:
- **Transcripts are now sharded** into `transcripts/{prefix}/` subdirectories on write.
- **Subdirectories do not count** toward the parent's entry count (probe-verified), so
  the 9,990 legacy flat transcripts can stay put.

### 1. HF API rate limits (128 commits/hour)
The 128-commits/hour cap is per-repo. We measured ~38 commits/hr in production (the cap
was **not** the bottleneck). Still, keep batches at 50 files and rely on the 429 backoff.

### 2. LFS file size limits
HF LFS has a soft 5 GB per file limit. Our files are small (raw ~4 MB avg). No issue.

### 3. HF token permissions
The token in `HF_TOKEN` (in `.env`, gitignored) must have **write access** to both repos. Do not commit a real token here.

### 4. Partial migration and agent cutover
If the migration is interrupted and agents start writing to the clean vault mid-way,
new agent writes go to the correct repo. The migration diff skips anything already in
the target — never overwrites.

### 5. Frame regeneration
Frames are **not migrated** because the painter regenerates them on re-process.
No data loss.

### 6. Orphaned URIs in the database
Videos with `fetched=TRUE` store URIs like `hf://datasets/Rolaficus/pleiades-vault/raw/{prefix}/{id}.opus`.
After cutover, agents read via `fetch_binary` which resolves against the _current_
`HF_DATASET_ID`. Since the clean vault will contain the migrated data, the same
path resolves correctly under the new repo ID. **No DB updates needed** — but the
cutover must happen *after* migration completes.

### 7. Metrics data (not migrated)
The `metrics/` parquet files are historical tracking data (HF Spaces dashboard).
Skip migration; they remain in the old vault.

### 8. Concurrent migration
The script must be the **only writer** to the clean vault during migration to avoid
commit conflicts. Worker is stopped until migration completes.

## Execution Order

1. Stop worker: `sudo systemctl stop prefect-worker` ✅ (worker intentionally stopped)
2. Run `python tools/migrate_vault.py --apply` (in tmux `vault_migrate`) ✅ running
3. Verify file counts match: `python tools/migrate_vault.py --verify`
4. Restore cookies file (`www.youtube.cookies.txt`)
5. Change `.env` to clean vault (already done)
6. Restart worker: `sudo systemctl restart prefect-worker`
7. Reset 518 FAILED videos → PENDING
8. Monitor pipeline

## Relevant Files

| File | Purpose |
|------|---------|
| `pleiades/.env` | `HF_DATASET_ID` setting (line 88), `YOUTUBE_COOKIES_PATH` (line 80) |
| `pleiades/atlas/src/atlas/vault.py` | `HuggingFaceVault`, `transcript_path()` shard helper, `fetch_transcript` (sharded + flat fallback) |
| `pleiades/atlas/src/atlas/config.py` | Settings model, reads `HF_DATASET_ID` from env |
| `pleiades/maia/src/maia/media/streamer.py` | yt-dlp download (`download_raw`, `-f bestaudio/best`) |
| `pleiades/maia/src/maia/streamer/flow.py` | `commit_artifacts` call, `raw/` URI construction |
| `pleiades/maia/src/maia/storage.py` | `commit_artifacts` — vault batch-write logic |
| `pleiades/maia/src/maia/singer/flow.py` | Reads `raw/` → writes `audio/` |
| `pleiades/maia/src/maia/painter/flow.py` | Reads `raw/` → writes `frames/` |
| `pleiades/maia/src/maia/scribe/flow.py` | Reads `audio/` (STT fallback) → stages transcripts |
| `pleiades/maia/src/maia/janitor/flow.py` | Flushes transcripts via `transcript_path(vid)` (sharded) |
| `pleiades/maia/src/maia/purge.py` | Deletes sharded + legacy flat transcript paths |
| `pleiades/tools/migrate_vault.py` | Migration script (git-tree diff, sharded targets) |
| `pleiades/tools/scrub_hf_cache.sh` | Periodic HF blob cache scrub (tmux `hf_scrub`) |
| `pleiades/tools/migrate_vault.log` | Migration run log |
| `pleiades/docs/micro-prefect-orchestration.md` | Operational runbook, control-plane details |
| `pleiades/docs/CHALLENGES.md` | Engineering log (section 7: streamer/YouTube access) |
