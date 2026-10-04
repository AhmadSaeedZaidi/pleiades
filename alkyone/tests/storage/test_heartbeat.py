"""Operator storage snapshots against an isolated PostgreSQL database."""

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
