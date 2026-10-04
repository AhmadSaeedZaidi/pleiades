#!/usr/bin/env python3
"""Audit vault transcripts + audio for quality control, and uncheck corrupted ones.

We are only ~12h into a fresh collection and cannot afford silent corruption
(the yt-dlp ``--js-runtimes`` regression broke scribe audio extraction, and the
frame-corruption bug showed how one bad window poisons the vault). For every
video that is ``has_transcript=TRUE`` AND fully flushed to the vault
(``vault_write_pending=FALSE``), this tool:

  * validates the transcript JSON (present, parsed, has >= 1 non-empty segment), and
  * if an audio file exists in the vault, validates it (size >= MIN, OggS magic).

A video is treated as **corrupted** if its transcript is invalid OR its
(present) audio file is invalid. Missing audio is deliberately NOT treated as
corruption — captions-only videos have no audio artifact, so flagging them would
be a false positive.

Corrupted videos are *unchecked* (``has_transcript=FALSE``, ``status=PENDING``)
so the Scribe re-claims and re-extracts them. This reprocessing loss is
acceptable for quality control.

Usage:
    python tools/audit_transcripts_audio.py            # dry run (report only)
    python tools/audit_transcripts_audio.py --apply    # uncheck corrupted videos
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

# Load .env manually (strip inline comments so pydantic bool fields parse).
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

MIN_AUDIO_BYTES = 1024  # anything smaller is a truncated/empty artifact
CONCURRENCY = 12


def _valid_transcript(t: object) -> bool:
    """A transcript is valid if it is a list with >= 1 segment carrying text."""
    if not isinstance(t, list):
        return False
    if not t:
        return False
    for seg in t:
        if isinstance(seg, dict) and isinstance(seg.get("text"), str) and seg["text"].strip():
            return True
    return False


async def _check_transcript(vault, vid: str, sem) -> tuple[bool, str]:
    async with sem:
        try:
            t = await asyncio.to_thread(vault.fetch_transcript, vid)
        except Exception as e:  # noqa: BLE001
            return True, f"fetch error: {e}"
    ok = _valid_transcript(t)
    return (not ok), ("invalid/empty" if not ok else "ok")


async def _check_audio(vault, vid: str, sem) -> tuple[bool, str]:
    async with sem:
        try:
            data = await asyncio.to_thread(vault.fetch_audio, vid)
        except Exception:  # noqa: BLE001 - missing/corrupt read => treat as missing
            return False, "missing"
    if data is None:
        return False, "missing"
    with data:
        raw = data.getvalue()
    if len(raw) < MIN_AUDIO_BYTES:
        return True, f"too small ({len(raw)} bytes)"
    if raw[:4] != b"OggS":
        return True, "not Ogg/Opus container"
    return False, f"ok ({len(raw)} bytes)"


async def main(apply: bool) -> None:
    from atlas.repositories import VideoRepository
    from atlas.vault import get_vault

    repo = VideoRepository()
    vault = get_vault()

    rows = await repo._fetch_all(  # type: ignore[attr-defined]
        """
        SELECT id FROM videos
        WHERE has_transcript = TRUE AND vault_write_pending = FALSE
        ORDER BY id
        """
    )
    ids = [r["id"] for r in rows]
    print(f"Auditing {len(ids)} flushed, transcribed video(s)...")

    sem = asyncio.Semaphore(CONCURRENCY)
    transcript_tasks = [_check_transcript(vault, vid, sem) for vid in ids]
    audio_tasks = [_check_audio(vault, vid, sem) for vid in ids]
    transcript_res = await asyncio.gather(*transcript_tasks)
    audio_res = await asyncio.gather(*audio_tasks)

    corrupt: list[str] = []
    transcript_bad = 0
    audio_bad = 0
    audio_missing = 0
    for vid, (t_bad, t_msg), (a_bad, a_msg) in zip(ids, transcript_res, audio_res, strict=True):
        if t_bad:
            transcript_bad += 1
            print(f"  [transcript] {vid}: {t_msg}")
        if a_bad:
            audio_bad += 1
            print(f"  [audio]      {vid}: {a_msg}")
        elif a_msg == "missing":
            audio_missing += 1
        if t_bad or a_bad:
            corrupt.append(vid)

    print("-" * 60)
    print(f"Audited           : {len(ids)}")
    print(f"Transcript invalid: {transcript_bad}")
    print(f"Audio invalid     : {audio_bad}")
    print(f"Audio missing     : {audio_missing} (captions-only; not corruption)")
    print(f"Corrupted (uncheck): {len(corrupt)}")

    if not apply:
        print("\n[dry-run] No changes made. Re-run with --apply to uncheck corrupted videos.")
        return

    for vid in corrupt:
        await repo.unmark_transcript(vid)  # type: ignore[attr-defined]
    print(f"\nUnchecked {len(corrupt)} corrupted video(s) — the Scribe will re-extract them.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually uncheck corrupted videos")
    args = parser.parse_args()
    asyncio.run(main(args.apply))
