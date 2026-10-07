"""Maia Archeologist: Historical video discovery agent.

Producer in the Producer-Consumer pipeline. Sole responsibility is to
discover historical video IDs from past years and push them to the
video table (the work queue for downstream consumers).
"""

import argparse
import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from atlas.repositories import VideoRepository
from atlas.state import clear_quota_exhausted
from atlas.utils import QuotaExhaustedError
from prefect import flow

from maia.hunter.flow import enrich_channels_task
from maia.quality import filter_by_quality
from maia.strategies import YouTubeSearchStrategy
from maia.utils import (
    channel_ids_of,
    cli_bootstrap,
    notify_quota_exhausted,
    run_agent_main,
)

logger = logging.getLogger(__name__)

TARGET_CATEGORIES = ["10", "20", "24", "28", "27"]


async def hunt_history_task(year: int, month: int, strategy: YouTubeSearchStrategy) -> None:
    """Search for top videos in target categories for a specific month in history."""
    run_logger = logger
    video_repo = VideoRepository()

    start_date = datetime(year, month, 1, tzinfo=UTC)
    if month == 12:
        end_date = datetime(year + 1, 1, 1, tzinfo=UTC)
    else:
        end_date = datetime(year, month + 1, 1, tzinfo=UTC)

    start_str = start_date.isoformat().replace("+00:00", "Z")
    end_str = end_date.isoformat().replace("+00:00", "Z")

    run_logger.info(f"Archeologist digging in: {start_str} to {end_str}")

    for category in TARGET_CATEGORIES:
        params: dict[str, Any] = {
            "part": "snippet",
            "type": "video",
            "order": "viewCount",
            "publishedAfter": start_str,
            "publishedBefore": end_str,
            "videoCategoryId": category,
            "maxResults": 50,
        }

        try:
            data = await strategy.search(params)
            if not data:
                run_logger.warning(f"No data returned for {year}-{month} (Cat: {category})")
                continue

            raw_items = data.get("items", [])

            # QUALITY GATE — enrich + filter before persisting / enriching.
            items = await filter_by_quality(raw_items, strategy.executor, logger=run_logger)

            ingest_tasks = [
                video_repo.ingest_video_metadata(item, priority_override=100) for item in items
            ]
            await asyncio.gather(*ingest_tasks)

            channel_ids = channel_ids_of(items)
            if channel_ids:
                try:
                    n = await enrich_channels_task(channel_ids, strategy)
                    run_logger.info(
                        f"Enriched {n} channel(s) for recovered relics (Cat: {category})"
                    )
                except QuotaExhaustedError:
                    await notify_quota_exhausted("archeologist")
                    raise
                except Exception as e:
                    run_logger.warning(
                        f"Channel enrichment after archeology ingest (non-fatal): {e}"
                    )

            run_logger.info(
                f"Recovered {len(items)}/{len(raw_items)} relics (passed quality gate) "
                f"from {year}-{month} (Cat: {category})"
            )
            # A category completed without exhausting quota → clear the marker so
            # the heartbeat stops reporting "rate limited" for this agent.
            clear_quota_exhausted("archeologist")

        except QuotaExhaustedError:
            await notify_quota_exhausted("archeologist")
            raise
        except Exception as e:
            run_logger.exception(f"Archeologist error on {year}-{month} (Cat: {category}): {e}")
            continue


async def archeology_operation(
    start_year: int = 2005,
    end_year: int = 2024,
    strategy: YouTubeSearchStrategy | None = None,
) -> dict[str, Any]:
    """Execute an archeology campaign to discover historical videos.

    WARNING: consumes massive YouTube quota — run sparingly.

    Args:
        start_year: Start year for the historical campaign.
        end_year: End year for the historical campaign.
        strategy: YouTubeSearchStrategy for API access.

    Returns a dict with campaign statistics.
    """
    run_logger = logger
    strategy = strategy or YouTubeSearchStrategy("archeology", agent_name="archeologist")
    run_logger.info("Starting Archeology Campaign...")

    stats = {
        "years_processed": 0,
        "months_processed": 0,
        "videos_discovered": 0,
    }

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            await hunt_history_task(year, month, strategy)
            stats["months_processed"] += 1
        stats["years_processed"] += 1

    run_logger.info(f"Archeology Campaign Complete: {stats}")
    return stats


class ArcheologistAgent:
    """Archeologist Agent: historical video discovery from past years."""

    name = "archeologist"

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.name)
        self.strategy = YouTubeSearchStrategy("archeology", agent_name="archeologist")

    @staticmethod
    def add_cli_args(parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--start-year",
            type=int,
            default=2005,
            help="Start year for historical campaign (default: 2005)",
        )
        parser.add_argument(
            "--end-year",
            type=int,
            default=2024,
            help="End year for historical campaign (default: 2024)",
        )

    async def run(
        self, start_year: int = 2005, end_year: int = 2024, **kwargs: Any
    ) -> dict[str, Any]:
        result: dict[str, Any] = await archeology_operation(
            start_year=start_year, end_year=end_year, strategy=self.strategy
        )
        return result


async def archeology_flow(
    start_year: int = 2005,
    end_year: int = 2024,
    strategy: YouTubeSearchStrategy | None = None,
) -> dict[str, Any]:
    """Plain compatibility helper for callers that inject a strategy."""
    return await archeology_operation(start_year=start_year, end_year=end_year, strategy=strategy)


@flow(name="run_archeology_campaign")
async def run_archeology_campaign(start_year: int = 2005, end_year: int = 2024) -> None:
    await archeology_operation(start_year=start_year, end_year=end_year)


async def hunt_history(year: int, month: int) -> None:
    strategy = YouTubeSearchStrategy("archeology", agent_name="archeologist")
    await hunt_history_task(year, month, strategy)


def main() -> None:
    run_agent_main(lambda: archeology_operation(start_year=2010, end_year=2010), "archeologist")


if __name__ == "__main__":
    cli_bootstrap()
    main()
