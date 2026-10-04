#!/usr/bin/env python3
"""Reset every video so its visual evidence is recollected by the Painter.

After fixing a corruption bug in frame extraction, already-stored frames in the
vault are invalid. This marks all videos ``PENDING`` with ``has_visuals=FALSE``
so the Painter re-claims and re-archives them on its next cycle.

Usage:
    python tools/repaint_vault_images.py          # dry run (reports count)
    python tools/repaint_vault_images.py --apply  # actually reset

Requires ``DATABASE_URL`` (from ``.env`` or the environment).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Make ``atlas`` importable without an editable install.
sys.path.insert(0, str(Path(__file__).parent.parent / "atlas" / "src"))

# Load .env manually (mirror of tools/check_quota_exhaustion.py).
_env_path = Path(__file__).parent.parent / ".env"
for line in _env_path.read_text().splitlines() if _env_path.exists() else []:
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip()
        if " #" in val:
            val = val.split(" #", 1)[0].strip()
        val = val.strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


async def main(apply: bool) -> None:
    from atlas.repositories import VideoRepository

    repo = VideoRepository()
    if not apply:
        # Report how many rows would be touched without mutating anything.
        rows = await repo._fetch_all(  # type: ignore[attr-defined]
            "SELECT COUNT(*) AS n FROM videos"
        )
        count = rows[0]["n"] if rows else 0
        print(f"[dry-run] {count} video(s) would be reset to PENDING/has_visuals=FALSE.")
        print("Run with --apply to perform the reset.")
        return

    n = await repo.repaint_all_videos()
    print(f"Reset {n} video(s) to PENDING with has_visuals=FALSE. Painter will re-collect.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually perform the reset")
    args = parser.parse_args()
    asyncio.run(main(args.apply))
