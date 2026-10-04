"""Maia Tracker: Video metrics monitoring agent.

Consumer in the Producer-Consumer pipeline. Pulls due videos from the
adaptive-scheduling ``watchlist``, fetches fresh statistics from the YouTube
Data API, persists them to ``video_stats_log``, and advances each video's
decay tier (age + view velocity).
"""

import argparse
import logging
from datetime import UTC, datetime
from typing import Any

from atlas.notifications import AlertChannel, AlertLevel, notifier
from atlas.repositories import VideoRepository, WatchlistRepository
from atlas.repositories.watchlist import UNAVAILABLE_MISSES_BEFORE_DORMANT
from atlas.state import clear_quota_exhausted
from atlas.utils import QuotaExhaustedError
from prefect import flow

from maia.strategies import YouTubeSearchStrategy
from maia.utils import cli_bootstrap, notify_quota_exhausted, run_agent_main

logger = logging.getLogger(__name__)


def _video_id(v: dict[str, Any]) -> str:
    """Extract the id from a watchlist item dict (``video_id`` or legacy ``id``)."""
    return str(v.get("video_id") or v["id"])


def _as_datetime(value: Any) -> datetime | None:
    """Convert a database/API timestamp to an aware datetime when possible."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _as_int(value: Any) -> int | None:
    """Return an integer counter, or ``None`` for an absent counter."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def fetch_targets_task(batch_size: int) -> list[dict[str, Any]]:
    """Fetch videos that need statistics updates (due per adaptive schedule)."""
    watchlist_repo = WatchlistRepository()
    run_logger = logger

    targets = await watchlist_repo.fetch_batch(min(batch_size, 50))
    run_logger.info(f"Fetched {len(targets)} videos from watchlist (batch_size={batch_size}).")
    return [t.model_dump() for t in targets]


async def update_stats_task(videos: list[dict[str, Any]], strategy: YouTubeSearchStrategy) -> int:
    """Fetch one API batch and atomically persist its stats and schedules.

    Missing IDs are expected for deleted/private/geo-blocked videos and are
    represented in the watchlist state. API and database failures propagate so
    they cannot be mistaken for an unavailable video.

    Args:
        videos: Due watchlist rows, including their previous durable sample.
        strategy: YouTube Data API strategy.

    Returns:
        Number of requested IDs present in the successful API response.
    """
    if not videos:
        return 0
    if len(videos) > 50:
        logger.warning("Tracker received %d videos; capping API batch at 50", len(videos))
        videos = videos[:50]

    video_repo = VideoRepository()
    watchlist_repo = WatchlistRepository()
    video_ids = [_video_id(v) for v in videos]
    logger.info("Fetching stats for %d videos...", len(video_ids))

    try:
        response_json = await strategy.fetch_videos(video_ids)
    except QuotaExhaustedError:
        await notify_quota_exhausted("tracker")
        raise

    if not isinstance(response_json, dict):
        raise RuntimeError("YouTube tracker API returned no response")
    items_value = response_json.get("items")
    if not isinstance(items_value, list):
        raise RuntimeError("YouTube tracker API response omitted its items list")

    requested_ids = set(video_ids)
    items = [
        item
        for item in items_value
        if isinstance(item, dict) and str(item.get("id")) in requested_ids
    ]
    returned_by_id = {str(item["id"]): item for item in items}
    missing_ids = [video_id for video_id in video_ids if video_id not in returned_by_id]
    observed_at = datetime.now(UTC)
    schedule_updates: list[dict[str, Any]] = []

    for video in videos:
        video_id = _video_id(video)
        item = returned_by_id.get(video_id)
        if item is None:
            previous_misses = _as_int(video.get("unavailable_count")) or 0
            unavailable_count = max(0, previous_misses) + 1
            if unavailable_count >= UNAVAILABLE_MISSES_BEFORE_DORMANT:
                tier = "DORMANT"
                next_track_at = watchlist_repo.dormant_next_track_time(observed_at)
            else:
                tier, next_track_at = watchlist_repo.retry_next_track_time(
                    str(video.get("tracking_tier") or "WEEKLY"), observed_at
                )
            last_tracked_at = _as_datetime(video.get("last_tracked_at"))
            last_views = _as_int(video.get("last_views"))
            last_likes = _as_int(video.get("last_likes"))
            last_comment_count = _as_int(video.get("last_comment_count"))
        else:
            statistics = item.get("statistics") or {}
            current_views = _as_int(statistics.get("viewCount"))
            current_likes = _as_int(statistics.get("likeCount"))
            current_comment_count = _as_int(statistics.get("commentCount"))
            previous_at = _as_datetime(video.get("last_tracked_at"))
            velocity = watchlist_repo.velocity_views_per_hour(
                _as_int(video.get("last_views")),
                previous_at,
                current_views,
                observed_at,
            )
            tier, next_track_at = watchlist_repo.calculate_next_track_time(
                published_at=_as_datetime(video.get("published_at")),
                views_per_hour=velocity,
                tier=video.get("tracking_tier"),
            )
            last_tracked_at = observed_at
            unavailable_count = 0
            last_views = current_views
            last_likes = current_likes
            last_comment_count = current_comment_count

        schedule_updates.append(
            {
                "video_id": video_id,
                "tracking_tier": tier,
                "last_tracked_at": last_tracked_at,
                "last_views": last_views,
                "last_likes": last_likes,
                "last_comment_count": last_comment_count,
                "unavailable_count": unavailable_count,
                "next_track_at": next_track_at,
            }
        )
    await video_repo.update_stats_batch(items, schedule_updates, tracked_at=observed_at)
    if missing_ids:
        logger.info("Advanced %d unavailable video rechecks", len(missing_ids))
    if items:
        logger.info("Logged %d stats to hot tier", len(items))
    return len(items)


async def tracker_operation(
    batch_size: int = 50, strategy: YouTubeSearchStrategy | None = None
) -> dict[str, Any]:
    """Execute a complete Tracker cycle: fetch stale videos, update stats.

    Args:
        batch_size: Number of videos to process (max 50 for YouTube API).
        strategy: YouTubeSearchStrategy for API access.

    Returns a dict with cycle statistics.
    """
    run_logger = logger
    strategy = strategy or YouTubeSearchStrategy("tracking", agent_name="tracker")
    run_logger.info("=== Starting Tracker Cycle ===")

    stats: dict[str, Any] = {
        "videos_fetched": 0,
        "videos_updated": 0,
        "videos_unavailable": 0,
    }

    try:
        if batch_size > 50:
            run_logger.warning(f"Batch size {batch_size} exceeds YouTube API limit. Capping at 50.")
            batch_size = 50

        targets = await fetch_targets_task(batch_size=batch_size)
        stats["videos_fetched"] = len(targets)

        if not targets:
            run_logger.info("No videos need tracking updates. Tracker cycle complete (idle).")
            clear_quota_exhausted("tracker")
            return stats

        updated_count = await update_stats_task(targets, strategy)
        stats["videos_updated"] = updated_count
        stats["videos_unavailable"] = len(targets) - updated_count

        run_logger.info(
            f"=== Tracker Cycle Complete === "
            f"Fetched: {stats['videos_fetched']}, "
            f"Updated: {stats['videos_updated']}, "
            f"Unavailable: {stats['videos_unavailable']}"
        )

        snapshot = await VideoRepository().pipeline_snapshot()
        status_str = ", ".join(
            f"{k}: {v}" for k, v in sorted(snapshot.get("status_counts", {}).items())
        )
        notified = await notifier.send(
            title="Tracker Cycle Summary",
            description=(
                f"Updated: {stats['videos_updated']}/{stats['videos_fetched']} | "
                f"Unavailable this check: {stats['videos_unavailable']}"
            ),
            channel=AlertChannel.SURVEILLANCE,
            level=(AlertLevel.SUCCESS if stats["videos_unavailable"] == 0 else AlertLevel.WARNING),
            fields={
                "Videos Updated": str(stats["videos_updated"]),
                "Unavailable This Check": str(stats["videos_unavailable"]),
                "Video Stats": status_str,
                "Total Corpus": str(snapshot.get("total", 0)),
                "Transcripts": str(snapshot.get("transcripts", 0)),
                "Audios": str(snapshot.get("audios", 0)),
                "Visuals": str(snapshot.get("with_visuals", 0)),
                "Ingested (1h)": str(snapshot.get("ingested_1h", 0)),
            },
        )
        if not notified:
            run_logger.warning("Tracker cycle summary was not delivered to Discord")

    except QuotaExhaustedError:
        run_logger.critical("Tracker Cycle terminated — all API keys exhausted")
        raise
    except Exception as e:
        run_logger.exception(f"Tracker cycle failed with unexpected error: {e}")
        raise

    clear_quota_exhausted("tracker")
    return stats


class TrackerAgent:
    """Tracker Agent: video metrics monitoring and statistics tracking."""

    name = "tracker"

    def __init__(self) -> None:
        """Initialize the Tracker agent with its YouTube search strategy."""
        self.logger = logging.getLogger(self.name)
        self.strategy = YouTubeSearchStrategy("tracking", agent_name="tracker")

    @staticmethod
    def add_cli_args(parser: argparse.ArgumentParser) -> None:
        """Register command-line arguments for the Tracker agent."""
        parser.add_argument(
            "--batch-size",
            type=int,
            default=50,
            help="Number of videos to track per cycle (max 50, default: 50)",
        )

    async def run(self, batch_size: int = 50, **kwargs: Any) -> dict[str, Any]:
        """Execute a complete Tracker cycle and return its statistics dict."""
        result: dict[str, Any] = await tracker_operation(
            batch_size=batch_size, strategy=self.strategy
        )
        return result


async def tracker_flow(
    batch_size: int = 50,
    strategy: YouTubeSearchStrategy | None = None,
    executor: Any | None = None,
) -> dict[str, Any]:
    """Plain compatibility helper for callers that inject a strategy."""
    if strategy is None:
        strategy = YouTubeSearchStrategy("tracking", agent_name="tracker")
    if executor is not None:
        # Alkyone's historical integration injects the already-configured
        # resiliency executor rather than a complete strategy object.
        strategy.executor = executor
    return await tracker_operation(batch_size=batch_size, strategy=strategy)


@flow(name="run_tracker_cycle")
async def run_tracker_cycle(batch_size: int = 50) -> dict[str, Any]:
    """
    Legacy function wrapper for backward compatibility.

    Prefer using TrackerAgent directly for new code.
    """
    return await tracker_operation(batch_size=batch_size)


def main() -> None:
    run_agent_main(lambda: tracker_operation(), "tracker")


if __name__ == "__main__":
    cli_bootstrap()
    main()
