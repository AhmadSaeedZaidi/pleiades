"""Useful standalone adapters; SQLite and filesystem work runs off the loop."""

import asyncio
import hashlib
import os
import sqlite3
import tempfile
import time
import uuid
from collections.abc import Sequence
from contextlib import closing
from pathlib import Path
from urllib.parse import unquote, urlparse

from .core import HotObject, StagedItem, StoredItem


class SQLiteHotStore:
    def __init__(self, path: str | Path, *, retention_seconds: float = 0) -> None:
        if retention_seconds < 0:
            raise ValueError("Retention must be non-negative")
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.retention_seconds = retention_seconds
        with closing(sqlite3.connect(self.path)) as conn, conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tiered_objects (
                    key TEXT PRIMARY KEY, version TEXT NOT NULL,
                    payload BLOB, uri TEXT, sha256 TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS tiered_pending
                ON tiered_objects(updated_at) WHERE payload IS NOT NULL
            """)

    async def stage(self, key: str, payload: bytes) -> str:
        item = StagedItem(key, uuid.uuid4().hex, payload)

        def write() -> None:
            with closing(sqlite3.connect(self.path)) as conn, conn:
                conn.execute(
                    """
                    INSERT INTO tiered_objects(key,version,payload,sha256,updated_at)
                    VALUES (?,?,?,?,?)
                    ON CONFLICT(key) DO UPDATE SET
                        version=excluded.version,payload=excluded.payload,
                        uri=NULL,sha256=excluded.sha256,updated_at=excluded.updated_at
                """,
                    (key, item.version, payload, item.sha256, time.time()),
                )

        await asyncio.to_thread(write)
        return item.version

    async def pending(self, limit: int) -> list[StagedItem]:
        if limit < 1:
            raise ValueError("Selection limit must be positive")

        def select() -> list[StagedItem]:
            with closing(sqlite3.connect(self.path)) as conn, conn:
                rows = conn.execute(
                    """
                    SELECT key,version,payload FROM tiered_objects
                    WHERE payload IS NOT NULL AND updated_at <= ?
                    ORDER BY updated_at,key LIMIT ?
                """,
                    (time.time() - self.retention_seconds, limit),
                ).fetchall()
            return [StagedItem(key, version, payload) for key, version, payload in rows]

        return await asyncio.to_thread(select)

    async def finalize(self, items: Sequence[StoredItem]) -> set[str]:
        def commit() -> set[str]:
            completed: set[str] = set()
            with closing(sqlite3.connect(self.path)) as conn, conn:
                conn.execute("BEGIN IMMEDIATE")
                for stored in items:
                    changed = conn.execute(
                        """
                        UPDATE tiered_objects SET uri=?,payload=NULL
                        WHERE key=? AND version=? AND sha256=? AND payload IS NOT NULL
                    """,
                        (stored.uri, stored.item.key, stored.item.version, stored.item.sha256),
                    )
                    if changed.rowcount:
                        completed.add(stored.item.key)
            return completed

        return await asyncio.to_thread(commit)

    async def get(self, key: str) -> HotObject | None:
        def select() -> HotObject | None:
            with closing(sqlite3.connect(self.path)) as conn, conn:
                row = conn.execute(
                    "SELECT payload,uri,sha256 FROM tiered_objects WHERE key=?", (key,)
                ).fetchone()
            return HotObject(*row) if row else None

        return await asyncio.to_thread(select)


class FilesystemColdStore:
    """Content-addressed files with atomic replacement and fsync before receipt."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        created: list[Path] = []
        directory = self.root
        while not directory.exists():
            created.append(directory)
            directory = directory.parent
        self.root.mkdir(parents=True, exist_ok=True)
        for directory in reversed(created):
            fd = os.open(directory.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def _contained(self, path: Path) -> Path:
        resolved = path.resolve()
        if not resolved.is_relative_to(self.root):
            raise ValueError("Cold object path is outside the configured root")
        return resolved

    async def write_batch(self, items: Sequence[StagedItem]) -> dict[str, str]:
        def write() -> dict[str, str]:
            receipts: dict[str, str] = {}
            for item in items:
                destination = self._contained(self.root / item.path)
                destination.parent.mkdir(parents=True, exist_ok=True)
                # A retry reuses an already durable object without rewriting it.
                if (
                    destination.is_file()
                    and hashlib.sha256(destination.read_bytes()).hexdigest() == item.sha256
                ):
                    receipts[item.key] = destination.as_uri()
                    continue
                fd, temporary = tempfile.mkstemp(dir=destination.parent, prefix=".tiered-")
                try:
                    with os.fdopen(fd, "wb") as output:
                        output.write(item.payload)
                        output.flush()
                        os.fsync(output.fileno())
                    os.replace(temporary, destination)
                    # Persist new directory entries as well as the file itself.
                    parent = destination.parent
                    while True:
                        directory_fd = os.open(parent, os.O_RDONLY)
                        try:
                            os.fsync(directory_fd)
                        finally:
                            os.close(directory_fd)
                        if parent == self.root:
                            break
                        parent = parent.parent
                finally:
                    Path(temporary).unlink(missing_ok=True)
                receipts[item.key] = destination.as_uri()
            return receipts

        return await asyncio.to_thread(write)

    async def read(self, uri: str) -> bytes | None:
        parsed = urlparse(uri)
        if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
            raise ValueError("Filesystem cold storage requires a local file URI")
        path = self._contained(Path(unquote(parsed.path)))

        def read() -> bytes | None:
            try:
                return path.read_bytes()
            except FileNotFoundError:
                return None

        return await asyncio.to_thread(read)
