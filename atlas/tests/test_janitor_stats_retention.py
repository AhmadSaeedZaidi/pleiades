"""Regression coverage for retaining recent Tracker history during archival."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from atlas.models import Video
from atlas.repositories.video.janitor import VideoJanitorMixin

from tiered_storage import StagedItem, StoredItem


class RecordingTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_args: object) -> None:
        return None


class RecordingConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def transaction(self) -> RecordingTransaction:
        return RecordingTransaction()

    async def execute(self, query: str, _params: tuple[object, ...]):
        self.queries.append(query)
        cur = MagicMock()
        cur.fetchall = AsyncMock(return_value=[("V1",)])
        return cur


class SuccessfulJanitor(VideoJanitorMixin):
    def __init__(self) -> None:
        self.connection = RecordingConnection()
        self._fetch_one = AsyncMock(return_value={"unvaulted": False})
        self.get_latest_stats_batch = AsyncMock(return_value={})

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[RecordingConnection]:
        yield self.connection


def _video() -> Video:
    return Video(
        id="V1",
        channel_id="ch",
        title="tracked",
        status="PROCESSED",
        last_updated_at=datetime.now(UTC) - timedelta(days=30),
    )


@pytest.mark.asyncio
async def test_archive_keeps_recent_stats_for_cold_archiver() -> None:
    janitor = SuccessfulJanitor()

    completed = await janitor._finalize_archives(
        [StoredItem(StagedItem("V1", "123", b"{}"), "memory://metadata")]
    )
    assert completed == {"V1"}
    statements = "\n".join(janitor.connection.queries)
    assert "UPDATE videos" in statements
    assert "DELETE FROM transcripts" in statements
    assert "DELETE FROM video_stats_log" not in statements
