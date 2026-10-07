"""Contracts for universal Tracker enrollment during video ingestion."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from atlas.repositories.video.ingestion import VideoIngestionMixin
from atlas.repositories.video.tracking import VideoTrackingMixin


class FakeConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.commits = 0

    async def execute(self, query: str, params: tuple[Any, ...]) -> None:
        self.calls.append((query, params))

    async def commit(self) -> None:
        self.commits += 1


class FakeIngestionRepository(VideoIngestionMixin):
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[FakeConnection]:
        yield self.connection


@pytest.mark.asyncio
async def test_ingestion_enrolls_video_in_watchlist_in_same_commit() -> None:
    connection = FakeConnection()
    repository = FakeIngestionRepository(connection)

    await repository.ingest_video_metadata(
        {
            "id": "video-1",
            "snippet": {
                "title": "Tracked from every producer",
                "publishedAt": "2026-09-02T00:00:00Z",
            },
        }
    )

    assert connection.commits == 1
    assert len(connection.calls) == 2
    video_sql, _ = connection.calls[0]
    watchlist_sql, watchlist_params = connection.calls[1]


class FakeTrackingConnection:
    def __init__(self) -> None:
        self.commits = 0

    async def commit(self) -> None:
        self.commits += 1


class FakeTrackingCursor:
    def __init__(self) -> None:
        self.connection = FakeTrackingConnection()
        self.calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    async def executemany(self, query: str, params: list[tuple[Any, ...]]) -> None:
        self.calls.append((query, params))


class FakeTrackingRepository(VideoTrackingMixin):
    def __init__(self, cursor: FakeTrackingCursor) -> None:
        self.cursor = cursor

    @asynccontextmanager
    async def _cursor(self) -> AsyncIterator[FakeTrackingCursor]:
        yield self.cursor


@pytest.mark.asyncio
async def test_tracker_stats_and_all_schedules_commit_atomically() -> None:
    from datetime import UTC, datetime, timedelta

    cursor = FakeTrackingCursor()
    repository = FakeTrackingRepository(cursor)
    observed_at = datetime.now(UTC)
    previous_at = observed_at - timedelta(days=7)
    stats = [{"id": "available", "statistics": {"viewCount": "10"}}]
    schedules = [
        {
            "video_id": "available",
            "tracking_tier": "DAILY",
            "last_tracked_at": observed_at,
            "last_views": 10,
            "last_likes": None,
            "last_comment_count": None,
            "unavailable_count": 0,
            "next_track_at": observed_at + timedelta(days=1),
        },
        {
            "video_id": "missing",
            "tracking_tier": "WEEKLY",
            "last_tracked_at": previous_at,
            "last_views": 8,
            "last_likes": None,
            "last_comment_count": None,
            "unavailable_count": 1,
            "next_track_at": observed_at + timedelta(days=7),
        },
    ]

    await repository.update_stats_batch(stats, schedules, tracked_at=observed_at)

    assert cursor.connection.commits == 1
    assert len(cursor.calls) == 3
    timestamp_params = cursor.calls[0][1]
    stats_params = cursor.calls[1][1]
    schedule_params = cursor.calls[2][1]
    assert timestamp_params == [(observed_at, "available")]
    assert stats_params[0][0] == "available"
    assert {params[-1] for params in schedule_params} == {"available", "missing"}
    missing_params = next(params for params in schedule_params if params[-1] == "missing")
    assert missing_params[1] == previous_at
