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


async def test_artifact_inventory_uses_sql_flags_and_counts_cold_transcript_pointers(storage_pool):
    repo = VideoRepository(storage_pool)
    await repo._execute("""
        UPDATE videos SET fetched=TRUE,has_video=TRUE WHERE id='V1';
        INSERT INTO transcripts(video_id,content) VALUES ('V1','[{"text":"staged"}]');
        INSERT INTO videos(id,channel_id,title,status,has_visuals,has_audio)
            VALUES ('cold','channel','archived','ARCHIVED',FALSE,FALSE),
                   ('audio','channel','audio','PROCESSING',FALSE,TRUE);
        INSERT INTO transcripts(video_id,content,vault_uri)
            VALUES ('cold',NULL,'memory://verified-transcript');
    """)
    snapshot = await repo.pipeline_snapshot()
    assert snapshot["transcripts"] == 2
    assert snapshot["with_visuals"] == snapshot["raw_sources"] == snapshot["full_clips"] == 1
    assert snapshot["audios"] == 2


async def test_ready_counts_exclude_archived_retired_and_legacy_failed_inputs(storage_pool):
    repo = VideoRepository(storage_pool)
    await repo._execute("""
        INSERT INTO videos(id,channel_id,title,status,fetched,has_audio,has_visuals,has_transcript)
        VALUES ('ready','channel','ready','PROCESSING',TRUE,FALSE,FALSE,FALSE),
               ('archived','channel','old','ARCHIVED',TRUE,FALSE,FALSE,FALSE),
               ('legacy','channel','failed','FAILED',TRUE,FALSE,FALSE,FALSE);
        INSERT INTO videos(id,channel_id,title,status,fetched,has_audio,retired_at)
            VALUES ('retired','channel','parked','PROCESSING',TRUE,FALSE,now());
        INSERT INTO videos(id,channel_id,title,status,fetched,last_updated_at)
            VALUES ('raw','channel','source','PENDING',FALSE,now()-interval '1 hour');
    """)
    row = await repo.pipeline_snapshot()
    assert (
        row["ready_raw"]
        == row["ready_audio"]
        == row["ready_visuals"]
        == row["ready_transcripts"]
        == 1
    )


async def test_visual_recovery_only_requeues_intact_diagnosed_sources(storage_pool):
    repo = VideoRepository(storage_pool)
    await repo._execute("""
        UPDATE videos SET status='PROCESSING',raw_phase='DONE',audio_phase='DONE',
            transcript_phase='DONE',visuals_phase='FAILED',has_visuals=FALSE,
            has_audio=TRUE,fetched=TRUE,raw_uri='memory://raw' WHERE id='V1';
        INSERT INTO transcripts(video_id,content) VALUES ('V1','[{"text":"keep"}]');
        INSERT INTO videos(id,channel_id,title,status,raw_phase,audio_phase,
            transcript_phase,visuals_phase,has_visuals,fetched,raw_uri,retired_at)
        SELECT id,'channel',id,status,'DONE','DONE','DONE','FAILED',FALSE,TRUE,
            'memory://raw',retired_at
        FROM (VALUES ('legacy','FAILED',NULL::timestamptz),
                     ('retired','PROCESSING',now()),
                     ('changed','PROCESSING',NULL::timestamptz)) v(id,status,retired_at);
        UPDATE videos SET audio_phase='PENDING' WHERE id='changed';
    """)
    ids = ["V1", "legacy", "retired", "changed"]
    assert await repo.retry_failed_visuals(ids) == ["V1"]
    assert await repo.retry_failed_visuals(ids) == []
    assert await repo.retry_failed_visuals([]) == []
    rows = await repo._fetch_all("SELECT id,visuals_phase FROM videos WHERE id <> 'V1'")
    assert all(row["visuals_phase"] == "FAILED" for row in rows)
    row = await repo._fetch_one("SELECT raw_uri,has_audio FROM videos WHERE id='V1'")
    assert row == {"raw_uri": "memory://raw", "has_audio": True}
    assert (await repo._fetch_one("SELECT content FROM transcripts WHERE video_id='V1'"))[
        "content"
    ] == [{"text": "keep"}]
