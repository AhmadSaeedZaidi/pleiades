#!/usr/bin/env python3
"""Migrate must-migrate artifacts from pleiades-vault to pleiades-vault-clean.

The old vault (private, 99.8 GB / 100 GB) is FULL — writes return 403
"Private repository storage limit reached". The target vault is public and has
no storage limit. This script copies every must-migrate artifact (raw/, audio/,
transcripts/, metadata/) that exists in the source but is missing in the target.

Transcripts are written to a *sharded* target layout (``transcripts/ab/…`` via
:func:`atlas.vault.transcript_path`) so no single directory exceeds HF's
10k-entries-per-directory limit. File listings come from git trees (the
HTTP ``siblings`` listing truncates above ~1000 files and undercounts).

frames/ and metrics/ are intentionally skipped (painter regenerates frames;
metrics are historical and not pipeline-critical). See docs/vault-migration.md.

Usage:
    python tools/migrate_vault.py               # dry run (report diff counts)
    python tools/migrate_vault.py --apply       # execute migration
    python tools/migrate_vault.py --verify      # verify post-migration counts
    python tools/migrate_vault.py --list-missing  # list files still pending
"""

from __future__ import annotations

import argparse
import io
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "atlas" / "src"))

from atlas.vault import audio_path, transcript_path  # noqa: E402

_env_path = Path(__file__).resolve().parent.parent / ".env"
for _line in _env_path.read_text().splitlines() if _env_path.exists() else []:
    _line = _line.strip()
    if _line and not _line.startswith("#") and "=" in _line:
        _k, _, _v = _line.partition("=")
        _k = _k.strip()
        _v = _v.strip()
        if " #" in _v:
            _v = _v.split(" #", 1)[0].strip()
        _v = _v.strip('"').strip("'")
        if _k and _k not in os.environ:
            os.environ[_k] = _v

from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_download  # noqa: E402

SRC_REPO = "Rolaficus/pleiades-vault"
TGT_REPO = "Rolaficus/pleiades-vault-clean"
KEEP_PREFIXES = ("raw/", "audio/", "media/audio/", "transcripts/", "metadata/")
BATCH_SIZE = 50
MAX_ATTEMPTS = 8
BASE_DELAY = 30.0

HF_TOKEN = os.environ.get("HF_TOKEN")
if not HF_TOKEN:
    print("ERROR: HF_TOKEN not found in .env or environment", file=sys.stderr)
    sys.exit(1)


def _is_rate_limited(exc: Exception) -> bool:
    resp = getattr(exc, "response", None)
    status = getattr(resp, "status_code", None)
    if status == 429:
        return True
    return "429" in str(exc)


def target_path(src_path: str) -> str:
    """Map a source repo path to its target repo path.

    Flat transcripts/audio are sharded; existing nested paths stay intact.
    """
    if src_path.startswith("transcripts/") and src_path.count("/") == 1:
        video_id = src_path[len("transcripts/") :].removesuffix(".json")
        return transcript_path(video_id)
    if src_path.startswith("audio/") and src_path.count("/") == 1:
        return audio_path(Path(src_path).stem)
    return src_path


def _clone_or_fetch(repo_id: str, repo_dir: Path) -> None:
    """Shallow-clone (or fetch) a dataset repo with blobs skipped (fast trees)."""
    url = f"https://user:{HF_TOKEN}@huggingface.co/datasets/{repo_id}"
    env = dict(
        os.environ,
        GIT_LFS_SKIP_SMUDGE="1",
        GIT_TERMINAL_PROMPT="0",
        GIT_ASKPASS="echo",
    )
    if not (repo_dir / ".git").exists():
        subprocess.run(
            [
                "git",
                "clone",
                "--no-checkout",
                "--depth",
                "1",
                "--filter=blob:none",
                url,
                str(repo_dir),
            ],
            check=True,
            capture_output=True,
            text=True,
            env=env,
            timeout=600,
        )
    else:
        subprocess.run(
            ["git", "-C", str(repo_dir), "fetch", "--depth", "1", "origin", "main"],
            check=True,
            capture_output=True,
            text=True,
            env=env,
            timeout=600,
        )


def list_files_git(repo_id: str, workdir: Path) -> list[str]:
    """List every file in *repo_id*'s default branch via git tree (authoritative)."""
    repo_dir = workdir / repo_id.replace("/", "_")
    _clone_or_fetch(repo_id, repo_dir)
    out = subprocess.run(
        ["git", "-C", str(repo_dir), "ls-tree", "-r", "--name-only", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        timeout=300,
    )
    return sorted(out.stdout.splitlines())


def compute_diff(src: list[str], tgt: list[str]) -> list[str]:
    tgt_set = frozenset(tgt)
    return sorted(
        f
        for f in src
        if f not in tgt_set
        and any(f.startswith(p) for p in KEEP_PREFIXES)
        and target_path(f) not in tgt_set
    )


def download_file(path: str) -> bytes:
    local = hf_hub_download(
        repo_id=SRC_REPO,
        filename=path,
        repo_type="dataset",
        token=HF_TOKEN,
    )
    with open(local, "rb") as fh:
        return fh.read()


def commit_batch(
    api: HfApi,
    batch: list[tuple[str, bytes]],
    batch_idx: int,
    total_batches: int,
) -> None:
    ops = [
        CommitOperationAdd(path_in_repo=target_path(path), path_or_fileobj=io.BytesIO(data))
        for path, data in batch
    ]
    delay = BASE_DELAY
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            api.create_commit(
                repo_id=TGT_REPO,
                repo_type="dataset",
                operations=ops,
                commit_message=f"Migrate batch {batch_idx}/{total_batches}: {len(batch)} files",
            )
            return
        except Exception as exc:
            if _is_rate_limited(exc):
                print(
                    f"    429 - backing off {delay:.0f}s (attempt {attempt}/{MAX_ATTEMPTS})",
                    flush=True,
                )
                time.sleep(delay)
                delay = min(delay * 2, 600.0)
            else:
                print(f"    FAIL: {exc}", flush=True)
                time.sleep(min(BASE_DELAY * attempt, 120.0))
    raise RuntimeError(f"Batch commit failed after {MAX_ATTEMPTS} attempts")


def verify_counts() -> None:
    print("Listing source/dest via git trees...", flush=True)
    t0 = time.time()
    with tempfile.TemporaryDirectory(prefix="migrate_") as td:
        workdir = Path(td)
        src = list_files_git(SRC_REPO, workdir)
        tgt = list_files_git(TGT_REPO, workdir)
    print(f"  Listed in {time.time() - t0:.1f}s", flush=True)

    missing = compute_diff(src, tgt)
    missing_set = frozenset(missing)

    print(f"\n{'Prefix':<20} {'Source':>10} {'Target':>10} {'Missing':>10}")
    print("-" * 52)
    total_missing = 0
    for prefix in KEEP_PREFIXES:
        sc = sum(1 for f in src if f.startswith(prefix))
        tc = sum(1 for f in tgt if f.startswith(prefix))
        diff = sum(1 for f in missing_set if f.startswith(prefix))
        total_missing += diff
        print(f"{prefix:<20} {sc:>10} {tc:>10} {diff:>10}")

    print(f"\nTotal files to migrate: {total_missing}")


def list_missing() -> None:
    with tempfile.TemporaryDirectory(prefix="migrate_") as td:
        workdir = Path(td)
        src = list_files_git(SRC_REPO, workdir)
        tgt = list_files_git(TGT_REPO, workdir)
    missing = compute_diff(src, tgt)
    if not missing:
        print("All files migrated.", flush=True)
    else:
        for f in missing:
            print(target_path(f))
        print(f"\n{len(missing)} files still pending", flush=True)


def run_migration() -> None:
    api = HfApi(token=HF_TOKEN)

    print("Listing source/dest via git trees...", flush=True)
    t0 = time.time()
    with tempfile.TemporaryDirectory(prefix="migrate_") as td:
        workdir = Path(td)
        src_files = list_files_git(SRC_REPO, workdir)
        tgt_files = list_files_git(TGT_REPO, workdir)
    print(f"  Source: {len(src_files)} files", flush=True)
    print(f"  Target: {len(tgt_files)} files", flush=True)
    print(f"  Listed in {time.time() - t0:.1f}s", flush=True)

    missing = compute_diff(src_files, tgt_files)
    if not missing:
        print("Nothing to migrate - clean vault already has everything.", flush=True)
        return

    print(f"\nFiles to migrate: {len(missing)}", flush=True)
    for prefix in KEEP_PREFIXES:
        count = sum(1 for f in missing if f.startswith(prefix))
        print(f"  {prefix}: {count}", flush=True)
    batches = [missing[i : i + BATCH_SIZE] for i in range(0, len(missing), BATCH_SIZE)]
    total_batches = len(batches)
    print(f"\nWill commit {total_batches} batches ({BATCH_SIZE}/batch).", flush=True)
    est_min = total_batches / 120 * 60
    print(f"Est. ~{est_min:.0f} min at 120 commits/h cap", flush=True)

    migrated = 0
    bytes_total = 0
    start = time.time()

    for batch_idx, batch in enumerate(batches, 1):
        batch_data: list[tuple[str, bytes]] = []
        for path in batch:
            try:
                data = download_file(path)
                batch_data.append((path, data))
                bytes_total += len(data)
            except Exception as exc:
                print(f"  SKIP {path}: {exc}", flush=True)
                continue

        if not batch_data:
            continue

        commit_batch(api, batch_data, batch_idx, total_batches)
        migrated += len(batch_data)
        elapsed = time.time() - start
        rate = migrated / elapsed * 60 if elapsed > 0 else 0
        pct = migrated / len(missing) * 100
        print(
            f"  [{batch_idx}/{total_batches}] {migrated}/{len(missing)} ({pct:.1f}%) "
            f"({bytes_total / 1024 / 1024:.0f} MB) - {rate:.1f} files/min",
            flush=True,
        )

    elapsed = time.time() - start
    print(
        f"\nDone. {migrated}/{len(missing)} files in {elapsed / 60:.1f} min "
        f"({bytes_total / 1024 / 1024 / 1024:.2f} GB)",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate vault artifacts to clean repo")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--apply", action="store_true", help="Execute migration")
    group.add_argument("--verify", action="store_true", help="Verify post-migration counts")
    group.add_argument("--list-missing", action="store_true", help="List files still pending")
    args = parser.parse_args()

    if args.verify:
        verify_counts()
    elif args.list_missing:
        list_missing()
    elif args.apply:
        run_migration()
    else:
        verify_counts()


if __name__ == "__main__":
    main()
