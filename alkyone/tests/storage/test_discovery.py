"""Discovery metadata, replay and queue-pressure behavior on isolated PostgreSQL."""

import asyncio

import pytest_asyncio
from atlas.repositories import VideoRepository
from atlas.repositories.discovery import DiscoveryRepository
from atlas.repositories.search_queue import SearchQueueRepository


@pytest_asyncio.fixture
async def discovery_pool(storage_pool):
    repo = VideoRepository(storage_pool)
    await repo._execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id TEXT PRIMARY KEY,title TEXT,country TEXT,created_at TIMESTAMPTZ);
        CREATE TABLE IF NOT EXISTS search_queue (
            id SERIAL PRIMARY KEY,query_term TEXT UNIQUE NOT NULL,priority INTEGER DEFAULT 0,
            mention_count INTEGER DEFAULT 0,next_page_token TEXT,last_searched_at TIMESTAMPTZ,
            result_count_total INTEGER DEFAULT 0,status TEXT DEFAULT 'active',
            created_at TIMESTAMPTZ DEFAULT now());
        TRUNCATE search_queue,channels;
        ALTER TABLE videos ADD COLUMN IF NOT EXISTS duration INTEGER;
        ALTER TABLE videos ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;
        ALTER TABLE videos ADD COLUMN IF NOT EXISTS tags TEXT[];
        ALTER TABLE videos ADD COLUMN IF NOT EXISTS category_id TEXT;
        ALTER TABLE videos ADD COLUMN IF NOT EXISTS default_language TEXT;
        ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS tracking_tier TEXT DEFAULT 'HOURLY';
        ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();
        INSERT INTO channels(id,title,country) VALUES ('channel','Existing','IN');
    """)
    return storage_pool


async def test_country_snapshot_uses_declared_metadata_and_separates_recent(discovery_pool):
    repo = VideoRepository(discovery_pool)
    await repo._execute("""
        INSERT INTO channels(id,title,country) VALUES ('ng','Nigeria','NG');
        INSERT INTO videos(id,channel_id,title,discovered_at) VALUES
            ('older','channel','old',now()-interval '8 days'),
            ('unknown','missing','unknown',now()),('new','ng','new',now());
    """)
    snapshot = await DiscoveryRepository(discovery_pool).geography_snapshot()
    assert snapshot["lifetime"] == {"IN": 2, "NG": 1, "unknown": 1}
    assert snapshot["recent"] == {"IN": 1, "NG": 1, "unknown": 1}
    assert await DiscoveryRepository(discovery_pool).existing_video_ids(["V1", "missing"]) == {"V1"}


async def test_insert_marker_is_true_once_even_for_concurrent_discovery(discovery_pool):
    repo = VideoRepository(discovery_pool)
    video = {
        "id": "concurrent",
        "snippet": {"channelId": "channel", "title": "Actual new video"},
        "contentDetails": {"duration": "PT5M"},
    }
    inserted = await asyncio.gather(
        repo.ingest_video_metadata(video), repo.ingest_video_metadata(video)
    )
    assert sorted(inserted) == [False, True]
    assert await repo.ingest_video_metadata(video) is False
    assert (
        await repo._fetch_scalar("SELECT count(*) FROM watchlist WHERE video_id='concurrent'") == 1
    )
    assert await repo._fetch_scalar("SELECT duration FROM videos WHERE id='concurrent'") == 300


async def test_queue_saturates_mentions_and_cools_recently_searched_terms(discovery_pool):
    repo = SearchQueueRepository(discovery_pool)
    await repo._execute("""
        INSERT INTO search_queue(query_term,mention_count,last_searched_at) VALUES
            ('ten',10,NULL),('million',1000000,NULL),('recent',1000000,now());
    """)
    assert [row.query_term for row in await repo.fetch_batch(3)] == ["ten", "million"]
    await repo.ensure_seed_terms(["physics lecture"])
    assert (await repo.fetch_batch(1))[0].query_term == "physics lecture"


async def test_population_penalties_persist_and_can_recover_without_deleting_terms(discovery_pool):
    repo = SearchQueueRepository(discovery_pool)
    await repo.add_terms(["science"], penalty=-60)
    await repo.add_terms(["science"], penalty=0)
    row = await repo._fetch_one(
        "SELECT id,priority,mention_count FROM search_queue WHERE query_term='science'"
    )
    assert row["priority"] == -60 and row["mention_count"] == 2
    await repo.set_geography_penalty(row["id"], 0)
    assert (
        await repo._fetch_scalar("SELECT priority FROM search_queue WHERE query_term='science'")
        == 0
    )
    await repo.set_geography_penalty(row["id"], -90)
    assert (
        await repo._fetch_scalar("SELECT priority FROM search_queue WHERE query_term='science'")
        == -90
    )


async def test_curated_seed_pressure_cannot_erase_cooldown_or_create_a_cull_retry_loop(
    discovery_pool,
):
    repo = SearchQueueRepository(discovery_pool)
    await repo.ensure_seed_terms(["physics lecture"])
    key = await repo._fetch_scalar("SELECT id FROM search_queue WHERE query_term='physics lecture'")
    await repo.set_geography_penalty(key, -1000)
    assert await repo._fetch_scalar("SELECT priority FROM search_queue WHERE id=%s", (key,)) == 20
    await repo._execute(
        "UPDATE search_queue SET priority=-1000,last_searched_at=now() WHERE id=%s", (key,)
    )
    await repo.cull_stale(below=0)
    await repo.ensure_seed_terms(["physics lecture"])
    assert await repo._fetch_scalar("SELECT count(*) FROM search_queue WHERE id=%s", (key,)) == 1
    assert await repo.fetch_batch(1) == []
