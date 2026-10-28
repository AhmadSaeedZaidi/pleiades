"""
Tests for Maia Tracker module (adaptive-scheduling watchlist).
"""

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from atlas.utils import QuotaExhaustedError
from maia.tracker.flow import (
    fetch_targets_task,
    tracker_operation,
    update_stats_task,
)


@pytest.fixture
def mock_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.fetch_videos = AsyncMock()
    return strategy


@pytest.mark.asyncio
async def test_fetch_targets_empty():
    """Test fetch_targets when no videos need updates."""
    with patch("maia.tracker.flow.WatchlistRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.fetch_batch = AsyncMock(return_value=[])

        result = await fetch_targets_task(batch_size=50)

        assert result == []
        mock_repo.fetch_batch.assert_called_once_with(50)


@pytest.mark.asyncio
async def test_fetch_targets_with_videos(mock_watchlist_item: dict[str, Any]):
    """Test fetch_targets returns watchlist items needing updates."""
    from atlas.models import WatchlistItem

    with patch("maia.tracker.flow.WatchlistRepository") as MockRepo:
        mock_repo = MockRepo.return_value
        mock_repo.fetch_batch = AsyncMock(return_value=[WatchlistItem(**mock_watchlist_item)])

        result = await fetch_targets_task(batch_size=50)

        assert len(result) == 1
        assert result[0]["video_id"] == "dQw4w9WgXcQ"


@pytest.mark.asyncio
async def test_update_stats_empty_list(mock_strategy: MagicMock):
    """Test update_stats with empty video list."""
    result = await update_stats_task([], mock_strategy)
    assert result == 0


@pytest.mark.asyncio
async def test_update_stats_success(
    mock_strategy: MagicMock, mock_youtube_stats_response: dict[str, Any]
):
    """Test update_stats fetches stats and advances the decay schedule."""
    mock_strategy.fetch_videos.return_value = mock_youtube_stats_response

    mock_videos = [{"video_id": "dQw4w9WgXcQ", "tracking_tier": "HOURLY"}]

    with (
        patch("maia.tracker.flow.VideoRepository") as MockVideoRepo,
        patch("maia.tracker.flow.WatchlistRepository") as MockWatchRepo,
    ):
        mock_video_repo = MockVideoRepo.return_value
        mock_video_repo.update_stats_batch = AsyncMock()
        mock_watch = MockWatchRepo.return_value
        mock_watch.velocity_views_per_hour = MagicMock(return_value=None)
        mock_watch.calculate_next_track_time = MagicMock(return_value=("DAILY", 1))
        mock_watch.retry_next_track_time = MagicMock(return_value=("WEEKLY", 2))

        result = await update_stats_task(mock_videos, mock_strategy)

        assert result == 1
        mock_video_repo.update_stats_batch.assert_awaited_once()
        stats_items, schedule = mock_video_repo.update_stats_batch.call_args.args[:2]
        assert len(stats_items) == 1
        assert schedule[0]["video_id"] == "dQw4w9WgXcQ"
        assert schedule[0]["unavailable_count"] == 0


@pytest.mark.asyncio
async def test_update_stats_schedules_missing_videos(
    mock_strategy: MagicMock, mock_youtube_stats_response: dict[str, Any]
):
    """Test update_stats advances the schedule for videos the API no longer
    returns (deleted/private/geo-blocked)."""
    returned_id = mock_youtube_stats_response["items"][0]["id"]
    dead_videos = [{"video_id": "dead_video_1"}, {"video_id": "dead_video_2"}]
    mock_strategy.fetch_videos.return_value = mock_youtube_stats_response

    with (
        patch("maia.tracker.flow.VideoRepository") as MockVideoRepo,
        patch("maia.tracker.flow.WatchlistRepository") as MockWatchRepo,
    ):
        mock_video_repo = MockVideoRepo.return_value
        mock_video_repo.update_stats_batch = AsyncMock()
        mock_watch = MockWatchRepo.return_value
        mock_watch.velocity_views_per_hour = MagicMock(return_value=None)
        mock_watch.calculate_next_track_time = MagicMock(return_value=("WEEKLY", 2))
        mock_watch.retry_next_track_time = MagicMock(return_value=("WEEKLY", 2))

        result = await update_stats_task(dead_videos + [{"video_id": returned_id}], mock_strategy)

        assert result == 1
        # Both unavailable and returned videos persist in the same transaction.
        updates = mock_video_repo.update_stats_batch.call_args.args[1]
        assert {u["video_id"] for u in updates} == {
            "dead_video_1",
            "dead_video_2",
            returned_id,
        }
        mock_video_repo.update_stats_batch.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_stats_empty_items_does_not_log(
    mock_strategy: MagicMock, mock_tracker_target: dict[str, Any]
):
    """Test update_stats returns 0 and skips stats logging when the API returns
    no items (all videos gone from YouTube), but still advances schedules."""
    mock_strategy.fetch_videos.return_value = {"items": []}

    with (
        patch("maia.tracker.flow.VideoRepository") as MockVideoRepo,
        patch("maia.tracker.flow.WatchlistRepository") as MockWatchRepo,
    ):
        mock_video_repo = MockVideoRepo.return_value
        mock_video_repo.update_stats_batch = AsyncMock()
        mock_watch = MockWatchRepo.return_value
        mock_watch.velocity_views_per_hour = MagicMock(return_value=None)
        mock_watch.calculate_next_track_time = MagicMock(return_value=("WEEKLY", 0))
        mock_watch.retry_next_track_time = MagicMock(return_value=("WEEKLY", 2))

        result = await update_stats_task([mock_tracker_target], mock_strategy)

        assert result == 0
        mock_video_repo.update_stats_batch.assert_awaited_once()
        assert mock_video_repo.update_stats_batch.call_args.args[0] == []
        schedule = mock_video_repo.update_stats_batch.call_args.args[1]
        assert schedule[0]["unavailable_count"] == 1
        assert schedule[0]["last_tracked_at"] == mock_tracker_target.get("last_tracked_at")


@pytest.mark.asyncio
async def test_update_stats_handles_api_errors(
    mock_strategy: MagicMock, mock_tracker_target: dict[str, Any]
):
    """API failures propagate and leave the due row unchanged."""
    mock_strategy.fetch_videos.side_effect = Exception("API Error")

    with (
        patch("maia.tracker.flow.VideoRepository"),
        patch("maia.tracker.flow.WatchlistRepository"),
        pytest.raises(Exception, match="API Error"),
    ):
        await update_stats_task([mock_tracker_target], mock_strategy)


@pytest.mark.asyncio
async def test_update_stats_propagates_rate_limit(
    mock_strategy: MagicMock, mock_tracker_target: dict[str, Any]
):
    """Test update_stats propagates QuotaExhaustedError."""

    mock_strategy.fetch_videos.side_effect = QuotaExhaustedError("All keys exhausted")

    with (
        patch("maia.tracker.flow.notify_quota_exhausted", new_callable=AsyncMock) as notify,
        pytest.raises(QuotaExhaustedError),
    ):
        await update_stats_task([mock_tracker_target], mock_strategy)
    notify.assert_awaited_once_with("tracker")


@patch("maia.tracker.flow.notify_quota_exhausted", new_callable=AsyncMock)
async def test_update_stats_notifies_on_quota_exhausted(
    mock_notify, mock_strategy, mock_tracker_target
):
    """QuotaExhaustedError triggers the notify path and is not swallowed."""
    mock_strategy.fetch_videos.side_effect = QuotaExhaustedError("All keys exhausted")

    with pytest.raises(QuotaExhaustedError):
        await update_stats_task([mock_tracker_target], mock_strategy)

    mock_notify.assert_awaited_once_with("tracker")


@patch("maia.tracker.flow.clear_quota_exhausted")
@patch("maia.tracker.flow.fetch_targets_task", new_callable=AsyncMock)
async def test_tracker_flow_idle_empty_watchlist(mock_fetch, mock_clear, mock_strategy):
    """Empty watchlist -> idle stats returned and quota marker cleared, no notify."""
    mock_fetch.return_value = []

    stats = await tracker_operation(batch_size=50, strategy=mock_strategy)

    assert stats == {"videos_fetched": 0, "videos_updated": 0, "videos_unavailable": 0}
    mock_fetch.assert_awaited_once_with(batch_size=50)
    mock_clear.assert_called_once_with("tracker")


@patch("maia.tracker.flow.clear_quota_exhausted")
@patch("maia.tracker.flow.update_stats_task", new_callable=AsyncMock)
@patch("maia.tracker.flow.fetch_targets_task", new_callable=AsyncMock)
@patch("maia.tracker.flow.notifier", new_callable=AsyncMock)
async def test_tracker_flow_caps_batch_size_at_50(
    mock_notifier, mock_fetch, mock_update, mock_clear, mock_strategy
):
    """batch_size > 50 is capped to the YouTube API limit before fetching."""
    mock_fetch.return_value = [{"video_id": "V1"}]
    mock_update.return_value = 1

    with patch("maia.tracker.flow.VideoRepository") as MockVideoRepo:
        MockVideoRepo.return_value.pipeline_snapshot = AsyncMock(
            return_value={"status_counts": {}, "total": 0}
        )
        stats = await tracker_operation(batch_size=500, strategy=mock_strategy)

    mock_fetch.assert_awaited_once_with(batch_size=50)
    assert stats["videos_fetched"] == 1
    assert stats["videos_updated"] == 1


@patch("maia.tracker.flow.clear_quota_exhausted")
@patch("maia.tracker.flow.VideoRepository")
@patch("maia.tracker.flow.notifier", new_callable=AsyncMock)
@patch("maia.tracker.flow.update_stats_task", new_callable=AsyncMock)
@patch("maia.tracker.flow.fetch_targets_task", new_callable=AsyncMock)
async def test_tracker_flow_notify_surveillance_success(
    mock_fetch, mock_update, mock_notifier, mock_video_repo, mock_clear, mock_strategy
):
    """A successful cycle posts a surveillance summary via the notifier."""
    mock_fetch.return_value = [{"video_id": "V1"}, {"video_id": "V2"}]
    mock_update.return_value = 2
    mock_video_repo.return_value.pipeline_snapshot = AsyncMock(
        return_value={"status_counts": {"PROCESSED": 5}, "total": 10}
    )

    stats = await tracker_operation(batch_size=50, strategy=mock_strategy)

    assert stats == {"videos_fetched": 2, "videos_updated": 2, "videos_unavailable": 0}
    mock_notifier.send.assert_awaited_once()
    assert mock_notifier.send.call_args.kwargs["fields"]["Unavailable This Check"] == "0"
    assert "Unavailable this check: 0" in mock_notifier.send.call_args.kwargs["description"]
    assert mock_notifier.send.call_args.kwargs["level"] is not None
    mock_clear.assert_called_once_with("tracker")


@pytest.mark.asyncio
async def test_third_missing_response_enters_dormant_without_fabricating_sample(
    mock_strategy: MagicMock,
):
    previous_at = datetime.now(UTC) - timedelta(days=7)
    next_at = datetime.now(UTC) + timedelta(days=30)
    target = {
        "video_id": "gone-video",
        "tracking_tier": "WEEKLY",
        "last_tracked_at": previous_at,
        "last_views": 123,
        "last_likes": 4,
        "last_comment_count": 5,
        "unavailable_count": 2,
    }
    mock_strategy.fetch_videos.return_value = {"items": []}

    with (
        patch("maia.tracker.flow.VideoRepository") as MockVideoRepo,
        patch("maia.tracker.flow.WatchlistRepository") as MockWatchRepo,
    ):
        mock_video_repo = MockVideoRepo.return_value
        mock_video_repo.update_stats_batch = AsyncMock()
        MockWatchRepo.return_value.dormant_next_track_time.return_value = next_at

        result = await update_stats_task([target], mock_strategy)

    assert result == 0
    stats_items, schedule = mock_video_repo.update_stats_batch.call_args.args[:2]
    assert stats_items == []
    update = schedule[0]
    assert update["tracking_tier"] == "DORMANT"
    assert update["unavailable_count"] == 3
    assert update["next_track_at"] == next_at
    assert update["last_tracked_at"] == previous_at
    assert update["last_views"] == 123
