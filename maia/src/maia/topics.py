"""Bounded topic backfill and refresh, with no browser-triggered API calls."""

import argparse
import asyncio
import logging

from atlas.config import get_settings
from atlas.repositories.topics import TopicRepository
from atlas.youtube import lookup_channels, lookup_videos

logger = logging.getLogger(__name__)


async def topics_operation(batch_size: int = 50, max_batches: int = 4) -> dict[str, int]:
    if not 1 <= batch_size <= 50 or not 1 <= max_batches <= 20:
        raise ValueError("Topic enrichment requires bounded batches")
    totals = {"observed": 0, "unavailable": 0, "deferred": 0, "batches": 0}
    if not get_settings().TOPIC_SYNC_ENABLED:
        return totals
    repo = TopicRepository()
    # Fairly service both resource kinds; unused budget serves the other queue.
    for index in range(max_batches):
        kind = "video" if index % 2 == 0 else "channel"
        token, ids = await repo.claim(kind, batch_size)
        if not ids:
            kind = "channel" if kind == "video" else "video"
            token, ids = await repo.claim(kind, batch_size)
        if not ids:
            break
        lookup = lookup_videos if kind == "video" else lookup_channels
        try:
            items = await lookup(ids, parts="topicDetails", key_ring_pool="hunting")
            result = await repo.complete(kind, token, ids, items)
        except asyncio.CancelledError:
            # The durable lease expires if cancellation interrupts release.
            await repo.release(kind, token)
            raise
        except Exception as error:
            await repo.release(kind, token)
            logger.warning("Topic batch retained for retry (%s)", type(error).__name__)
            raise
        totals["batches"] += 1
        for name, value in result.items():
            totals[name] += value
    logger.info("Topic enrichment: %s", totals)
    return totals


def main() -> None:
    from maia.utils import cli_bootstrap

    cli_bootstrap()
    parser = argparse.ArgumentParser(description="Run one bounded YouTube topic enrichment cycle")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--max-batches", type=int, default=4)
    args = parser.parse_args()
    asyncio.run(topics_operation(args.batch_size, args.max_batches))


if __name__ == "__main__":
    main()
