"""DB-only integration suite; no YouTube, Prefect or remote vault credentials."""

import os
from contextlib import asynccontextmanager

import pytest_asyncio
from alkyone.guard import assert_isolated_test_environment
from psycopg import AsyncConnection


def pytest_configure(config):
    assert_isolated_test_environment()


class TestPool:
    def __init__(self, dsn):
        self.dsn = dsn

    @asynccontextmanager
    async def get_connection(self):
        async with await AsyncConnection.connect(self.dsn) as conn:
            yield conn


@pytest_asyncio.fixture
async def storage_pool():
    """Use only a database that the existing Alkyone guard positively identified."""
    assert_isolated_test_environment()
    pool = TestPool(os.environ["DATABASE_URL"])
    async with pool.get_connection() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                id VARCHAR(20) PRIMARY KEY, channel_id TEXT NOT NULL, title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PROCESSED', discovered_at TIMESTAMPTZ DEFAULT now(),
                last_updated_at TIMESTAMPTZ DEFAULT now(), archived_at TIMESTAMPTZ,
                has_transcript BOOLEAN DEFAULT TRUE, has_audio BOOLEAN DEFAULT TRUE,
                has_visuals BOOLEAN DEFAULT TRUE, vault_write_pending BOOLEAN DEFAULT FALSE
            );
            CREATE TABLE IF NOT EXISTS transcripts (
                video_id VARCHAR(20) PRIMARY KEY REFERENCES videos(id), language TEXT,
                vault_uri TEXT, content JSONB
            );
            CREATE TABLE IF NOT EXISTS video_stats_log (
                video_id VARCHAR(20) REFERENCES videos(id), views BIGINT, likes BIGINT,
                comment_count BIGINT, timestamp TIMESTAMPTZ,
                PRIMARY KEY(video_id, timestamp)
            );
            TRUNCATE transcripts, video_stats_log, videos;
            INSERT INTO videos(id,channel_id,title) VALUES ('V1','channel','example');
        """)
    yield pool
