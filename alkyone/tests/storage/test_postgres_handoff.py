"""Actual MVCC and transaction regressions against an isolated PostgreSQL DB."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

from atlas.repositories import TranscriptRepository, VideoRepository

from tiered_storage import FilesystemColdStore, StagedItem, StoredItem, promote


async def test_new_transcript_during_handoff_stays_hot(storage_pool, tmp_path):
    repo = TranscriptRepository(storage_pool)
    await repo.record_transcript("V1", None, content_json=[{"text": "old"}])
    old = await repo.pending(50)
    assert len(old) == 1
    await repo.record_transcript("V1", None, content_json=[{"text": "new"}])
    cold = FilesystemColdStore(tmp_path)
    assert (await promote(old, cold, repo.finalize)).deferred == ("V1",)
    row = await repo._fetch_one("SELECT content,vault_uri FROM transcripts WHERE video_id='V1'")
    assert row == {"content": [{"text": "new"}], "vault_uri": None}
    current = await repo.pending(50)
    assert (await promote(current, cold, repo.finalize)).promoted == ("V1",)
    row = await repo._fetch_one("SELECT content,vault_uri FROM transcripts WHERE video_id='V1'")
    assert row["content"] is None and row["vault_uri"].endswith(current[0].path)
    assert await repo.pending(50) == []
    assert (await promote(old, cold, repo.finalize)).deferred == ("V1",)


async def test_archive_refuses_a_transcript_staged_after_snapshot(storage_pool):
    videos = VideoRepository(storage_pool)
    transcript = TranscriptRepository(storage_pool)
    row = await videos._fetch_one("SELECT xmin::text AS version FROM videos WHERE id='V1'")
    item = StoredItem(StagedItem("V1", row["version"], b"{}"), "memory://verified")
    await transcript.record_transcript("V1", None, content_json=[{"text": "new"}])
    assert await videos._finalize_archives([item]) == set()
    assert await transcript._fetch_one("SELECT content FROM transcripts WHERE video_id='V1'")
    assert (await videos.get_by_id("V1")).status != "ARCHIVED"


async def test_archive_rechecks_transcript_safety_under_parent_lock(storage_pool):
    videos = VideoRepository(storage_pool)
    await videos._execute("INSERT INTO transcripts(video_id,content) VALUES ('V1','[]')")
    row = await videos._fetch_one("SELECT xmin::text AS version FROM videos WHERE id='V1'")
    item = StoredItem(StagedItem("V1", row["version"], b"{}"), "memory://verified")
    assert await videos._finalize_archives([item]) == set()
    assert (await videos.get_by_id("V1")).has_transcript


async def test_archive_retains_recent_metrics_and_refuses_late_scribe(storage_pool):
    videos = VideoRepository(storage_pool)
    await videos._execute(
        "INSERT INTO transcripts(video_id,vault_uri) VALUES ('V1','memory://cold')"
    )
    await videos._execute(
        "INSERT INTO video_stats_log(video_id,timestamp,views) VALUES ('V1',now(),1)"
    )
    row = await videos._fetch_one("SELECT xmin::text AS version FROM videos WHERE id='V1'")
    item = StoredItem(StagedItem("V1", row["version"], b"{}"), "memory://verified")
    assert await videos._finalize_archives([item]) == {"V1"}
    assert await videos._fetch_one("SELECT * FROM video_stats_log WHERE video_id='V1'")
    assert await videos._fetch_one("SELECT * FROM transcripts WHERE video_id='V1'") is None
    import pytest

    with pytest.raises(ValueError, match="archived"):
        await TranscriptRepository(storage_pool).record_transcript("V1", None, content_json=[])


async def test_changed_metric_row_is_not_deleted(storage_pool):
    videos = VideoRepository(storage_pool)
    timestamp = datetime.now(UTC) - timedelta(days=30)
    await videos._execute(
        "INSERT INTO video_stats_log(video_id,timestamp,views) VALUES ('V1',%s,1)", (timestamp,)
    )
    row = await videos._fetch_one("SELECT xmin::text AS version FROM video_stats_log")
    await videos._execute("UPDATE video_stats_log SET views=2 WHERE video_id='V1'")
    assert await videos._delete_cold_stats(["V1"], [timestamp], [row["version"]]) == 0
    row = await videos._fetch_one("SELECT views,xmin::text AS version FROM video_stats_log")
    assert row["views"] == 2
    assert await videos._delete_cold_stats(["V1"], [timestamp], [row["version"]]) == 1


async def test_metadata_write_and_verification_failure_leave_hot_video(storage_pool):
    videos = VideoRepository(storage_pool)
    cold = AsyncMock()
    cold.write_batch.side_effect = OSError("offline")
    video = await videos.get_by_id("V1")
    with (
        patch("atlas.storage.VaultColdStore", return_value=cold),
        patch("atlas.vault.get_vault"),
        patch("atlas.events.events.emit", new_callable=AsyncMock),
    ):
        result = await videos.archive_video_batch([video])
    assert result["failed"] == 1
    assert (await videos.get_by_id("V1")).status == "PROCESSED"


async def test_metadata_archives_using_shared_handoff_engine(storage_pool, tmp_path):
    videos = VideoRepository(storage_pool)
    cold = FilesystemColdStore(tmp_path)
    video = await videos.get_by_id("V1")
    with patch("atlas.storage.VaultColdStore", return_value=cold), patch("atlas.vault.get_vault"):
        result = await videos.archive_video_batch([video])
    assert result["archived"] == 1
    assert (await videos.get_by_id("V1")).status == "ARCHIVED"
    assert list(tmp_path.rglob("*.json"))
