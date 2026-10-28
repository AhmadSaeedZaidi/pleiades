"""Unit tests for adaptive-scheduling decay tier logic (no DB)."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from atlas.repositories import WatchlistRepository
from atlas.repositories.watchlist import WatchlistRepository as WatchlistRepo


@pytest.fixture
def repo() -> WatchlistRepository:
    """WatchlistRepository with the default config thresholds."""
    return WatchlistRepository(db_pool=None)  # no DB access in these tests


class FakeDriver:
    """Fake DB driver capturing the SQL executed and returning canned rows.

    ``_fetch_all`` returns ``velocity_rows`` in the exact order the mock wants
    (simulating the windowed query already ordered by timestamp DESC), and
    records the params of the last query for FIFO/cap assertions.
    """

    def __init__(self, fetch_all_rows: list[dict[str, Any]] | None = None) -> None:
        self.fetch_all_rows = fetch_all_rows or []
        self.last_params: tuple[Any, ...] | None = None
        self.execute_many_calls: list[tuple[Any, ...]] = []

    async def _fetch_all(
        self, query: str, params: tuple[Any, ...] | None = None
    ) -> list[dict[str, Any]]:
        self.last_params = params
        return self.fetch_all_rows

    async def _execute_many(self, query: str, params_list: list[tuple[Any, ...]]) -> None:
        self.execute_many_calls = params_list


def _repo_with(fake: FakeDriver) -> WatchlistRepo:
    """Build a WatchlistRepository bound to the fake driver."""
    repo = WatchlistRepo(db_pool=None)
    repo._fetch_all = fake._fetch_all  # type: ignore[method-assign]
    repo._execute_many = fake._execute_many  # type: ignore[method-assign]
    return repo


def _vt(views: int | None, ts: datetime) -> dict[str, Any]:
    return {"video_id": "V1", "views": views, "timestamp": ts}


@pytest.mark.parametrize(
    "age, expected_tier",
    [
        (timedelta(hours=2), "HOURLY"),
        (timedelta(hours=23), "HOURLY"),
        (timedelta(hours=24), "DAILY"),
        (timedelta(days=3), "DAILY"),
        (timedelta(days=6, hours=23), "DAILY"),
        (timedelta(days=7), "WEEKLY"),
        (timedelta(days=30), "WEEKLY"),
    ],
)
def test_age_floor(repo, age, expected_tier):
    published = datetime.now(UTC) - age
    tier, _next = repo.calculate_next_track_time(published_at=published)
    assert tier == expected_tier


def test_hot_velocity_promotes_old_video(repo):
    """A viral old video (velocity >= HOT) stays HOURLY past the age cutoffs."""
    published = datetime.now(UTC) - timedelta(days=30)
    tier, next_at = repo.calculate_next_track_time(published_at=published, views_per_hour=500.0)
    assert tier == "HOURLY"
    assert next_at <= datetime.now(UTC) + timedelta(hours=1, minutes=1)


def test_dead_velocity_drops_weekly_floor(repo):
    """A video with essentially no views/hour drops one tier, flooring at WEEKLY."""
    published = datetime.now(UTC) - timedelta(days=2)  # DAILY by age
    tier, next_at = repo.calculate_next_track_time(published_at=published, views_per_hour=0.0)
    assert tier == "WEEKLY"
    assert next_at <= datetime.now(UTC) + timedelta(days=7, hours=1)


def test_dead_hourly_drops_to_daily(repo):
    """A <24h video with zero velocity drops to DAILY (never slower than weekly)."""
    published = datetime.now(UTC) - timedelta(hours=10)  # HOURLY by age
    tier, next_at = repo.calculate_next_track_time(published_at=published, views_per_hour=0.0)
    assert tier == "DAILY"


def test_unknown_velocity_keeps_age_floor(repo):
    """Unknown velocity (None) falls back to the age floor, no boost or drop."""
    published = datetime.now(UTC) - timedelta(days=3)  # DAILY
    tier, _ = repo.calculate_next_track_time(published_at=published, views_per_hour=None)
    assert tier == "DAILY"


def test_no_published_at_uses_current_tier(repo):
    """When legacy publication time is NULL: keep current tier."""
    tier, next_at = repo.calculate_next_track_time(
        published_at=None, views_per_hour=None, tier="WEEKLY"
    )
    assert tier == "WEEKLY"
    assert next_at > datetime.now(UTC)


# --- Durable velocity and missing-item retry policy ---


def test_velocity_views_per_hour_math_reports_units(repo):
    now = datetime.now(UTC)
    result = repo.velocity_views_per_hour(100, now - timedelta(hours=2), 150, now)
    assert result == 25.0


def test_velocity_counter_reset_clamps_to_zero(repo):
    now = datetime.now(UTC)
    result = repo.velocity_views_per_hour(200, now - timedelta(hours=1), 150, now)
    assert result == 0.0


@pytest.mark.parametrize(
    "previous_views,previous_at,current_views,offset",
    [
        (None, datetime.now(UTC), 10, timedelta(hours=1)),
        (10, None, 20, timedelta(hours=1)),
        (10, datetime.now(UTC), None, timedelta(hours=1)),
        (10, datetime.now(UTC), 20, timedelta(0)),
        (10, datetime.now(UTC), 20, timedelta(hours=-1)),
    ],
)
def test_velocity_unknown_for_incomplete_or_nonpositive_window(
    repo, previous_views, previous_at, current_views, offset
):
    current_at = (previous_at or datetime.now(UTC)) + offset
    assert (
        repo.velocity_views_per_hour(previous_views, previous_at, current_views, current_at) is None
    )


def test_velocity_accepts_naive_legacy_timestamp(repo):
    previous_at = datetime(2026, 1, 1, 0, 0)
    current_at = datetime(2026, 1, 1, 2, 0, tzinfo=UTC)
    assert repo.velocity_views_per_hour(10, previous_at, 30, current_at) == 10.0


def test_missing_item_retries_at_existing_tier(repo):
    now = datetime.now(UTC)
    tier, next_at = repo.retry_next_track_time("DAILY", now)
    assert tier == "DAILY"
    assert next_at == now + timedelta(days=1)


def test_unknown_retry_tier_falls_back_to_weekly(repo):
    now = datetime.now(UTC)
    tier, next_at = repo.retry_next_track_time("UNKNOWN", now)
    assert tier == "WEEKLY"
    assert next_at == now + timedelta(days=7)


def test_dormant_recheck_is_monthly(repo):
    now = datetime.now(UTC)
    assert repo.dormant_next_track_time(now) == now + timedelta(days=30)


# --- fetch_batch FIFO + batch cap ---


def _wl_row(video_id: str, next_at: datetime) -> dict[str, Any]:
    return {
        "video_id": video_id,
        "tracking_tier": "HOURLY",
        "last_tracked_at": None,
        "next_track_at": next_at,
        "created_at": None,
        "published_at": None,
    }


@pytest.mark.asyncio
async def test_fetch_batch_respects_fifo_order():
    """Rows come back in next_track_at ASC order and are parsed in that order."""
    now = datetime.now(UTC)
    fake = FakeDriver(
        fetch_all_rows=[
            _wl_row("V1", now + timedelta(minutes=5)),
            _wl_row("V2", now + timedelta(minutes=1)),
            _wl_row("V3", now + timedelta(minutes=3)),
        ]
    )
    repo = _repo_with(fake)
    items = await repo.fetch_batch(batch_size=50)
    assert [i.video_id for i in items] == ["V1", "V2", "V3"]


@pytest.mark.asyncio
async def test_fetch_batch_caps_to_limit():
    """batch_size is forwarded as the LIMIT param and bounds the returned batch."""
    fake = FakeDriver(fetch_all_rows=[_wl_row(f"V{i}", datetime.now(UTC)) for i in range(10)])
    repo = _repo_with(fake)
    items = await repo.fetch_batch(batch_size=10)
    assert fake.last_params == (10,)
    assert len(items) == 10


@pytest.mark.asyncio
async def test_fetch_batch_empty_is_empty():
    fake = FakeDriver(fetch_all_rows=[])
    repo = _repo_with(fake)
    assert await repo.fetch_batch(batch_size=10) == []


# --- update_schedule ---


@pytest.mark.asyncio
async def test_update_schedule_writes_params_in_column_order():
    """Each update becomes a row of (tier, last_tracked_at, next_track_at, video_id)."""
    fake = FakeDriver()
    repo = _repo_with(fake)
    now = datetime.now(UTC)
    updates = [
        {
            "video_id": "V1",
            "tracking_tier": "DAILY",
            "last_tracked_at": now,
            "next_track_at": now + timedelta(days=1),
        }
    ]
    await repo.update_schedule(updates)
    params = fake.execute_many_calls[0]
    assert params == ("DAILY", now, None, None, None, 0, now + timedelta(days=1), "V1")


@pytest.mark.asyncio
async def test_update_schedule_empty_short_circuits():
    """No updates -> _execute_many is never called."""
    fake = FakeDriver()
    repo = _repo_with(fake)
    await repo.update_schedule([])
    assert fake.execute_many_calls == []
