"""Run with an optional data directory; no external services or credentials."""

import argparse
import asyncio
import tempfile
from pathlib import Path

from tiered_storage import FilesystemColdStore, SQLiteHotStore, TieredStorage


async def run(root: Path) -> None:
    hot = SQLiteHotStore(root / "hot.db")
    storage = TieredStorage(hot, FilesystemColdStore(root / "cold"))
    await hot.stage("example", b"A reusable hot/cold handoff")
    print(f"Before: {await storage.read('example')!r}")
    result = await storage.flush()
    print(f"Promoted: {result.promoted}")
    print(f"After: {await storage.read('example')!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", nargs="?", type=Path)
    args = parser.parse_args()
    if args.directory:
        asyncio.run(run(args.directory))
    else:
        with tempfile.TemporaryDirectory(prefix="tiered-storage-") as directory:
            asyncio.run(run(Path(directory)))


if __name__ == "__main__":
    main()
