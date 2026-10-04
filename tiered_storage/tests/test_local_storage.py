"""Real SQLite/filesystem round trips, with no external services."""

from pathlib import Path
from unittest.mock import patch

import pytest

from tiered_storage import FilesystemColdStore, SQLiteHotStore, TieredStorage


async def test_stage_promote_read_and_reopen(tmp_path: Path) -> None:
    hot = SQLiteHotStore(tmp_path / "hot.db")
    cold = FilesystemColdStore(tmp_path / "cold")
    storage = TieredStorage(hot, cold)
    await hot.stage("document", b"hello")
    assert await storage.read("document") == b"hello"
    result = await storage.flush()
    assert result.promoted == ("document",)
    record = await hot.get("document")
    assert record is not None and record.payload is None and record.uri
    assert await storage.read("missing") is None
    reopened = TieredStorage(SQLiteHotStore(tmp_path / "hot.db"), cold)
    assert await reopened.read("document") == b"hello"
    assert not (await reopened.flush()).selected


async def test_staging_new_version_during_upload_preserves_new_payload(tmp_path: Path) -> None:
    hot = SQLiteHotStore(tmp_path / "hot.db")
    cold = FilesystemColdStore(tmp_path / "cold")
    storage = TieredStorage(hot, cold)
    await hot.stage("document", b"old")
    original_write = cold.write_batch

    async def concurrent_write(items):
        receipts = await original_write(items)
        await hot.stage("document", b"new")
        return receipts

    with patch.object(cold, "write_batch", side_effect=concurrent_write):
        result = await storage.flush()
    assert result.deferred == ("document",)
    assert await storage.read("document") == b"new"
    assert (await storage.flush()).promoted == ("document",)
    assert await storage.read("document") == b"new"


async def test_retention_delays_handoff(tmp_path: Path) -> None:
    hot = SQLiteHotStore(tmp_path / "hot.db", retention_seconds=3600)
    await hot.stage("recent", b"data")
    assert await hot.pending(50) == []
    assert (await hot.get("recent")).payload == b"data"


async def test_cold_corruption_is_detected_on_read(tmp_path: Path) -> None:
    hot = SQLiteHotStore(tmp_path / "hot.db")
    cold = FilesystemColdStore(tmp_path / "cold")
    storage = TieredStorage(hot, cold)
    await hot.stage("document", b"hello")
    await storage.flush()
    for obj in (tmp_path / "cold").rglob("*.bin"):
        obj.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="digest"):
        await storage.read("document")


async def test_filesystem_refuses_uri_outside_its_root(tmp_path: Path) -> None:
    cold = FilesystemColdStore(tmp_path / "cold")
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"private")
    with pytest.raises(ValueError, match="root"):
        await cold.read(outside.as_uri())
