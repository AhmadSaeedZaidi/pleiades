"""Operator storage snapshots against an isolated PostgreSQL database."""

from atlas.repositories import VideoRepository
from atlas.repositories.heartbeat import HeartbeatRepository


async def test_storage_snapshot_distinguishes_staged_retained_and_cold(storage_pool):
    repo = HeartbeatRepository(storage_pool)
    empty = await repo.storage_snapshot()
    assert empty["hot_bodies"] == empty["payload_bytes"] == 0
    await repo._execute("""
        INSERT INTO videos(id,channel_id,title) VALUES ('V2','channel','b'), ('V3','channel','c');
        INSERT INTO transcripts(video_id,content,vault_uri) VALUES
            ('V1','[{"text":"staged"}]',NULL),
            ('V2','[{"text":"retained"}]','memory://existing'),
            ('V3',NULL,'memory://cold');
        INSERT INTO watchlist(video_id,next_track_at,last_tracked_at) VALUES
            ('V1',now()-interval '1 hour',now()),
            ('V2',now()+interval '1 hour',NULL);
    """)
    row = await repo.storage_snapshot()
    assert row["hot_bodies"] == 2 and row["staged"] == row["retained"] == 1
    assert row["payload_bytes"] > 0 and row["database_bytes"] > 0
    assert row["tracking_due"] == 1 and row["last_tracked_at"] is not None
    # Read-only collection leaves bodies and receipts intact.
    assert row == await repo.storage_snapshot()


async def test_failure_aggregate_counts_videos_once_and_exposes_legacy_overlap(storage_pool):
    repo = VideoRepository(storage_pool)
    await repo._execute("""
        UPDATE videos SET status='FAILED',raw_phase='FAILED',visuals_phase='FAILED' WHERE id='V1';
        INSERT INTO videos(id,channel_id,title,status,audio_phase)
            VALUES ('V2','channel','b','PROCESSING','FAILED');
    """)
    snapshot = await repo.pipeline_snapshot()
    assert snapshot["failed_steps"] == 2
    assert sum(snapshot["failed_step_counts"].values()) == 3
    assert snapshot["status_counts"]["FAILED"] == snapshot["failed_overlap"] == 1


async def test_audio_recovery_preserves_legacy_failures_completed_stages_and_payloads(storage_pool):
    repo = VideoRepository(storage_pool)
    await repo._execute("""
        UPDATE videos SET status='PROCESSING',raw_phase='DONE',audio_phase='FAILED',
            has_audio=FALSE,fetched=TRUE,raw_uri='memory://raw' WHERE id='V1';
        INSERT INTO transcripts(video_id,content) VALUES ('V1','[{"text":"keep"}]');
        INSERT INTO videos(id,channel_id,title,status,raw_phase,audio_phase,has_audio,raw_uri)
        VALUES ('V2','channel','legacy','FAILED','DONE','FAILED',FALSE,'memory://raw'),
               ('V3','channel','done','PROCESSING','DONE','DONE',TRUE,'memory://raw');
    """)
    assert await repo.retry_failed_audio(["V1", "V2", "V3"]) == ["V1"]
    assert await repo.retry_failed_audio(["V1", "V2", "V3"]) == []
    assert (await repo._fetch_one("SELECT audio_phase FROM videos WHERE id='V2'"))[
        "audio_phase"
    ] == "FAILED"
    assert (await repo._fetch_one("SELECT has_audio FROM videos WHERE id='V3'"))[
        "has_audio"
    ] is True
    assert (await repo._fetch_one("SELECT content FROM transcripts WHERE video_id='V1'"))[
        "content"
    ] == [{"text": "keep"}]
