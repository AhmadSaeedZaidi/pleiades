"""Real PostgreSQL coverage for leases, facts, atomic ingestion and graph reads."""

import os
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import pytest_asyncio
from atlas.repositories.channel import ChannelRepository
from atlas.repositories.knowledge_graph import KnowledgeGraphReader
from atlas.repositories.topics import TopicRepository, record_topic_snapshot
from psycopg import AsyncConnection
from psycopg.rows import dict_row

URL = "https://en.wikipedia.org/wiki/Science"
OTHER = "https://en.wikipedia.org/wiki/Technology"


class GraphPool:
    @asynccontextmanager
    async def get_connection(self):
        async with await AsyncConnection.connect(
            os.environ["DATABASE_URL"], options="-c search_path=topic_graph_test"
        ) as conn:
            yield conn

    async def rows(self, sql, params):
        async with self.get_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(sql, params)
                return await cursor.fetchall()


@pytest_asyncio.fixture
async def graph_pool():
    # conftest's fail-closed Alkyone guard already validated the isolated DSN.
    pool = GraphPool()
    async with pool.get_connection() as conn:
        await conn.execute("""DROP SCHEMA IF EXISTS topic_graph_test CASCADE;
            CREATE SCHEMA topic_graph_test;
            CREATE TABLE channels(id VARCHAR(50) PRIMARY KEY, title TEXT NOT NULL,
                country TEXT, custom_url TEXT, created_at TIMESTAMPTZ, last_scraped_at TIMESTAMPTZ);
            CREATE TABLE videos(id VARCHAR(20) PRIMARY KEY, title TEXT NOT NULL,
                channel_id TEXT, wiki_topics TEXT[], status TEXT DEFAULT 'ARCHIVED',
                discovered_at TIMESTAMPTZ DEFAULT now());
            CREATE TABLE channel_stats_log(channel_id VARCHAR(50) REFERENCES channels(id),
                timestamp TIMESTAMPTZ, view_count BIGINT,
                subscriber_count BIGINT, video_count BIGINT,
                PRIMARY KEY(channel_id,timestamp));
            INSERT INTO channels(id,title) VALUES ('c1','Example publisher');
            INSERT INTO videos(id,title,channel_id) VALUES ('v1','Example video','c1');""")
    migration = Path(__file__).resolve().parents[3] / "deploy/sql/20261004_knowledge_graph.sql"
    async with pool.get_connection() as conn:
        await conn.execute(migration.read_text())
    yield pool
    async with pool.get_connection() as conn:
        await conn.execute("DROP SCHEMA topic_graph_test CASCADE")


async def snapshot(pool, urls, kind="video", key="v1"):
    async with pool.get_connection() as conn, conn.transaction():
        await record_topic_snapshot(
            conn, kind, {"id": key, "topicDetails": {"topicCategories": urls}}
        )


async def due(pool):
    async with pool.get_connection() as conn:
        await conn.execute(
            "UPDATE youtube_topic_sync SET next_attempt_at=now()-interval '1 second'"
        )


@pytest.mark.asyncio
async def test_claim_ownership_empty_unavailable_and_archived_graph(graph_pool):
    repo = TopicRepository(graph_pool)
    token, ids = await repo.claim("video")
    assert ids == ["v1"]
    _, overlapping = await repo.claim("video")
    assert overlapping == []
    assert (
        await repo.complete(
            "video",
            token,
            ids,
            [{"id": "v1", "topicDetails": {"topicCategories": [URL, URL, OTHER]}}],
        )
    )["observed"] == 1
    assert len(await graph_pool.rows("SELECT * FROM video_topics", ())) == 2
    # Missing API resource keeps last known facts, with an explicit unavailable outcome.
    await due(graph_pool)
    token, ids = await repo.claim("video")
    assert (await repo.complete("video", token, ids, []))["unavailable"] == 1
    assert len(await graph_pool.rows("SELECT * FROM video_topics", ())) == 2
    result = await KnowledgeGraphReader(graph_pool.rows).graph(topic=URL, limit=10)
    assert result["summary"]["unavailable"] == 1
    assert any(n["kind"] == "video" and n["status"] == "ARCHIVED" for n in result["nodes"])
    assert {e["relation"] for e in result["edges"]} == {"HAS_TOPIC", "PUBLISHED_BY"}
    await due(graph_pool)
    token, ids = await repo.claim("video")
    await repo.complete("video", token, ids, [{"id": "v1"}])
    assert await graph_pool.rows("SELECT * FROM video_topics", ()) == []
    assert (await graph_pool.rows("SELECT wiki_topics FROM videos", ()))[0]["wiki_topics"] == []


@pytest.mark.asyncio
async def test_fresh_snapshot_supersedes_inflight_backfill_and_partial_preserves(graph_pool):
    repo = TopicRepository(graph_pool)
    token, ids = await repo.claim("video")
    await snapshot(graph_pool, [OTHER])
    result = await repo.complete(
        "video", token, ids, [{"id": "v1", "topicDetails": {"topicCategories": [URL]}}]
    )
    assert result["deferred"] == 1
    async with graph_pool.get_connection() as conn:
        await record_topic_snapshot(conn, "video", {"id": "v1", "statistics": {}})
    facts = await graph_pool.rows("SELECT topic_url FROM video_topics", ())
    assert facts == [{"topic_url": OTHER}]


@pytest.mark.asyncio
async def test_expired_lease_and_parent_deletion_do_not_accept_late_response(graph_pool):
    repo = TopicRepository(graph_pool)
    token, ids = await repo.claim("video")
    async with graph_pool.get_connection() as conn:
        await conn.execute("UPDATE youtube_topic_sync SET leased_until=now()-interval '1 second'")
    replacement, ids = await repo.claim("video")
    assert replacement != token
    assert (await repo.complete("video", token, ids, [{"id": "v1"}]))["deferred"] == 1
    async with graph_pool.get_connection() as conn:
        await conn.execute("DELETE FROM videos WHERE id='v1'")
    result = await repo.complete(
        "video", replacement, ids, [{"id": "v1", "topicDetails": {"topicCategories": [URL]}}]
    )
    assert result["deferred"] == 1
    assert await graph_pool.rows("SELECT * FROM video_topics", ()) == []
    assert (await repo.claim("video"))[1] == []
    assert await graph_pool.rows("SELECT * FROM youtube_topic_sync", ()) == []


@pytest.mark.asyncio
async def test_channel_snapshot_and_graph_facts_commit_atomically(graph_pool):
    repo = ChannelRepository(graph_pool)
    await repo.ingest_channel_snapshot(
        {
            "id": "c1",
            "snippet": {"title": "New title"},
            "statistics": {"viewCount": "123"},
            "topicDetails": {"topicCategories": [URL]},
        }
    )
    result = await KnowledgeGraphReader(graph_pool.rows).graph(search="Science")
    assert result["summary"]["channel_checked"] == 1
    assert result["edges"][0]["source"] == "channel:c1"
    with pytest.raises(ValueError):
        await repo.ingest_channel_snapshot(
            {
                "id": "c1",
                "snippet": {"title": "Bad title"},
                "topicDetails": {"topicCategories": ["bad"]},
            }
        )
    assert (await graph_pool.rows("SELECT title FROM channels WHERE id=%s", ("c1",)))[0][
        "title"
    ] == "New title"
    assert len(await graph_pool.rows("SELECT * FROM channel_stats_log", ())) == 1


@pytest.mark.asyncio
async def test_graph_query_bounds_and_literal_search(graph_pool):
    async with graph_pool.get_connection() as conn:
        await conn.execute(
            "INSERT INTO videos(id,title,channel_id) "
            "SELECT 'v'||i,'Video '||i,'c1' FROM generate_series(2,65) i"
        )
        for i in range(1, 66):
            await record_topic_snapshot(
                conn, "video", {"id": f"v{i}", "topicDetails": {"topicCategories": [URL, OTHER]}}
            )
    reader = KnowledgeGraphReader(graph_pool.rows)
    result = await reader.graph(topic=URL, limit=50)
    assert len([n for n in result["nodes"] if n["kind"] == "video"]) == 50
    assert all(
        e["source"] in {n["id"] for n in result["nodes"]}
        and e["target"] in {n["id"] for n in result["nodes"]}
        for e in result["edges"]
    )
    assert (await reader.graph(search="%"))["topics"] == []
    assert (await reader.graph(search="' OR 1=1 --"))["nodes"] == []
