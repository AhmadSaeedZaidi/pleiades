#!/usr/bin/env python3
"""Recover FAILED pipeline runs and purge their corrupted vault frames.

Two coupled problems remain after the frame-corruption / quota-exhaustion bugs:

  1. A stage failed terminally and the heartbeat reports the per-stage
     breakdown (e.g. 260 failed visuals runs) because a cycle raised. Failures
     are recorded in the per-step phase columns — ``raw_phase``,
     ``audio_phase``, ``visuals_phase``, ``transcript_phase``, ``clip_phase`` —
     not as a row-level ``status='FAILED'``.
  2. Their (corrupt) frame files still live in the vault under ``frames/<id>/``.

This tool:
  * clears every terminally-failed stage back to ``PENDING`` (clearing
    ``has_visuals`` so the Painter reclaims and re-collects them), and
  * permanently deletes the stale ``frames/<id>/`` files from the vault.

The vault purge is performed BEFORE the DB reset so a running Painter cycle
cannot race to recollect frames we are about to delete.

Usage:
    python tools/recover_failed_runs.py                 # dry run (reports counts)
    python tools/recover_failed_runs.py --apply         # reset FAILED + purge their frames
    python tools/recover_failed_runs.py --apply --all   # also reset EVERY video + purge all frames
    python tools/recover_failed_runs.py --apply --video-id ABCD123
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Make ``atlas`` importable without an editable install.
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT / "atlas" / "src"))

# Load .env manually (mirror of tools/repaint_vault_images.py).
_env_path = _ROOT / ".env"
for _line in _env_path.read_text().splitlines() if _env_path.exists() else []:
    _line = _line.strip()
    if _line and not _line.startswith("#") and "=" in _line:
        _k, _, _v = _line.partition("=")
        _k = _k.strip()
        _v = _v.strip()
        if " #" in _v:  # strip inline comment
            _v = _v.split(" #", 1)[0].strip()
        _v = _v.strip('"').strip("'")
        if _k and _k not in os.environ:
            os.environ[_k] = _v


async def _failed_ids(repo) -> list[str]:
    # Failures are recorded per stage (raw_phase/audio_phase/visuals_phase/
    # transcript_phase/clip_phase), not as a row-level status='FAILED'.
    rows = await repo._fetch_all(  # type: ignore[attr-defined]
        "SELECT id FROM videos "
        "WHERE raw_phase = 'FAILED' OR audio_phase = 'FAILED' "
        "   OR visuals_phase = 'FAILED' OR transcript_phase = 'FAILED' "
        "   OR clip_phase = 'FAILED' "
        "ORDER BY id"
    )
    return [r["id"] for r in rows]


async def _reset_one(repo, video_id: str) -> None:
    await repo._execute(  # type: ignore[attr-defined]
        """
        UPDATE videos
        SET status = 'PENDING', has_visuals = FALSE, last_updated_at = now()
        WHERE id = %s
        """,
        (video_id,),
    )


async def main(apply: bool, all_videos: bool, video_id: str | None) -> None:
    from atlas.repositories import VideoRepository
    from atlas.vault import get_vault

    repo = VideoRepository()
    vault = get_vault()

    # Collect the set of video ids whose frames we will purge.
    if video_id:
        target_ids = [video_id]
        failed_ids = target_ids
    elif all_videos:
        target_ids = []  # purge the entire frames/ tree
        failed_ids = await _failed_ids(repo)
    else:
        target_ids = failed_ids = await _failed_ids(repo)

    # Discover frame files in the vault (one listing of the whole tree).
    all_frames = vault.list_files("frames/")
    if target_ids:
        prefixes = tuple(f"frames/{vid}/" for vid in target_ids)
        frames_to_purge = [p for p in all_frames if p.startswith(prefixes)]
    else:
        frames_to_purge = list(all_frames)

    if not apply:
        scope = (
            f"video {video_id}"
            if video_id
            else ("ALL videos" if all_videos else f"{len(failed_ids)} FAILED video(s)")
        )
        print(f"[dry-run] Scope: {scope}")
        print(f"[dry-run] {len(failed_ids)} video(s) would be reset to PENDING.")
        print(f"[dry-run] {len(frames_to_purge)} frame file(s) would be purged from the vault.")
        print("Run with --apply to perform the recovery.")
        return

    # 1) Purge the stale vault frames first.
    deleted = vault.delete_files(frames_to_purge)
    print(f"Purged {deleted} frame file(s) from the vault.")

    # 2) Reset the DB so the Painter re-collects them.
    if video_id:
        await _reset_one(repo, video_id)
        print(f"Reset video {video_id} to PENDING (has_visuals=FALSE).")
    elif all_videos:
        n = await repo.repaint_all_videos()
        print(f"Reset {n} video(s) to PENDING (has_visuals=FALSE) via repaint_all_videos.")
    else:
        n = await repo.reset_failed_to_pending()
        print(f"Reset {n} FAILED video(s) to PENDING (has_visuals=FALSE).")

    print("Recovery complete — the Painter will re-collect the frames on its next cycle.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually perform the recovery")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Reset EVERY video (not just FAILED) and purge the entire frames/ tree",
    )
    parser.add_argument("--video-id", default=None, help="Target a single video id")
    args = parser.parse_args()
    asyncio.run(main(args.apply, args.all, args.video_id))
