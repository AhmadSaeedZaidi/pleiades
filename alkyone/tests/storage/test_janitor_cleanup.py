"""Reversible dead-video cleanup, only on the guarded disposable database."""

import pytest
from atlas.repositories import VideoRepository


async def seed(repo):
    await repo._execute("""
        ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS tracking_tier TEXT DEFAULT 'HOURLY';
        ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS unavailable_count INTEGER DEFAULT 0;
        ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();
        UPDATE videos SET status='FAILED',raw_phase='FAILED',has_audio=FALSE WHERE id='V1';
        INSERT INTO transcripts(video_id,content) VALUES ('V1','[{"text":"preserve"}]');
        INSERT INTO watchlist(video_id,tracking_tier,unavailable_count,created_at)
            VALUES ('V1','DORMANT',3,now()-interval '8 days');
        INSERT INTO videos(id,channel_id,title,status,raw_phase) VALUES
            ('recent','channel','recent','FAILED','FAILED'),
            ('one_miss','channel','one','FAILED','FAILED'),
            ('running','channel','busy','PROCESSING','PROCESSING'),
            ('available','channel','available','FAILED','FAILED'),
            ('old','channel','old','PENDING','PENDING');
        INSERT INTO videos(id,channel_id,title,status,fetched,raw_uri,raw_phase)
            VALUES ('cached','channel','cached','PROCESSING',TRUE,'memory://raw','DONE');
        INSERT INTO watchlist(video_id,tracking_tier,unavailable_count,created_at)
            VALUES ('cached','DORMANT',3,now()-interval '8 days');
        UPDATE videos SET last_updated_at=now()-interval '1 hour' WHERE id='old';
        INSERT INTO watchlist(video_id,tracking_tier,unavailable_count,created_at,last_tracked_at)
        VALUES ('recent','DORMANT',3,now()-interval '8 days',now()-interval '1 day'),
               ('one_miss','DORMANT',1,now()-interval '8 days',NULL),
               ('running','DORMANT',3,now()-interval '8 days',NULL),
               ('available','HOURLY',0,now()-interval '8 days',now()),
               ('old','DORMANT',4,now()-interval '8 days',NULL);
    """)


async def test_cleanup_dry_run_preserves_data_and_only_parks_confirmed_quiescent_videos(
    storage_pool,
):
    repo = VideoRepository(storage_pool)
    await seed(repo)
    preview = await repo.cleanup_unavailable_videos(dry_run=True)
    assert set(preview["retired_ids"]) == {"V1", "old"}
    assert await repo._fetch_scalar("SELECT count(*) FROM videos WHERE retired_at IS NOT NULL") == 0
    result = await repo.cleanup_unavailable_videos()
    assert set(result["retired_ids"]) == {"V1", "old"}
    assert await repo.cleanup_unavailable_videos() == {"retired_ids": [], "restored_ids": []}
    row = await repo._fetch_one(
        "SELECT status,raw_phase,retirement_reason FROM videos WHERE id='V1'"
    )
    assert row == {
        "status": "FAILED",
        "raw_phase": "FAILED",
        "retirement_reason": "repeated_api_unavailability",
    }
    assert await repo._fetch_scalar("SELECT content FROM transcripts WHERE video_id='V1'") == [
        {"text": "preserve"}
    ]
    assert "old" not in {v.id for v in await repo.claim_streamer_batch(10)}
    snapshot = await repo.pipeline_snapshot()
    assert snapshot["retired_videos"] == 2
    assert snapshot["legacy_failed_active"] == 3
    assert snapshot["failed_steps"] == 3


async def test_successful_tracker_observation_restores_without_resetting_failed_stages(
    storage_pool,
):
    repo = VideoRepository(storage_pool)
    await seed(repo)
    await repo.cleanup_unavailable_videos()
    await repo._execute(
        """UPDATE watchlist SET unavailable_count=0,tracking_tier='WEEKLY',last_tracked_at=now()
           WHERE video_id IN ('V1','old')"""
    )
    preview = await repo.cleanup_unavailable_videos(dry_run=True)
    assert set(preview["restored_ids"]) == {"V1", "old"}
    assert await repo._fetch_scalar("SELECT count(*) FROM videos WHERE retired_at IS NOT NULL") == 2
    restored = await repo.cleanup_unavailable_videos()
    assert set(restored["restored_ids"]) == {"V1", "old"}
    assert "old" in {v.id for v in await repo.claim_streamer_batch(10)}
    assert await repo._fetch_scalar("SELECT raw_phase FROM videos WHERE id='V1'") == "FAILED"


async def test_cleanup_skips_locked_rows_and_obeys_page_limit(storage_pool):
    repo = VideoRepository(storage_pool)
    await seed(repo)
    async with storage_pool.get_connection() as conn, conn.transaction():
        await conn.execute("SELECT id FROM videos WHERE id='V1' FOR UPDATE")
        result = await repo.cleanup_unavailable_videos(batch_size=1)
        assert result["retired_ids"] == ["old"]
    assert (await repo.cleanup_unavailable_videos(batch_size=1))["retired_ids"] == ["V1"]


@pytest.mark.parametrize("size", [0, 101])
async def test_cleanup_rejects_unbounded_pages(storage_pool, size):
    with pytest.raises(ValueError):
        await VideoRepository(storage_pool).cleanup_unavailable_videos(batch_size=size)
