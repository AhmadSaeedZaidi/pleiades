"""Bounded topic backfill and refresh, with no browser-triggered API calls."""

import argparse
import asyncio
import logging
from typing import Any

from atlas.config import get_settings
from atlas.repositories.topics import TopicRepository
from atlas.utils import KeyRing, ResiliencyExecutor
from atlas.youtube import lookup_channels, lookup_videos
from prefect import flow

logger = logging.getLogger(__name__)


async def grapher_operation(batch_size: int = 50, max_batches: int = 4) -> dict[str, int]:
    if not 1 <= batch_size <= 50 or not 1 <= max_batches <= 20:
        raise ValueError("Topic enrichment requires bounded batches")
    totals = {"observed": 0, "unavailable": 0, "deferred": 0, "batches": 0}
    if not get_settings().TOPIC_SYNC_ENABLED:
        return totals
    executor = ResiliencyExecutor(KeyRing("grapher"), agent_name="grapher")
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
            items = await lookup(ids, parts="topicDetails", executor=executor)
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
    logger.info("Grapher enrichment: %s", totals)
    return totals


@flow(name="grapher_cycle")
async def grapher_flow(batch_size: int = 50, max_batches: int = 4) -> dict[str, int]:
    return await grapher_operation(batch_size, max_batches)


class GrapherAgent:
    """Enrich the topic graph using a dedicated YouTube key ring."""

    name = "grapher"

    @staticmethod
    def add_cli_args(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--batch-size", type=int, default=50)
        parser.add_argument("--max-batches", type=int, default=4)

    async def run(self, **kwargs: Any) -> dict[str, Any]:
        return await grapher_operation(
            batch_size=kwargs.get("batch_size", 50), max_batches=kwargs.get("max_batches", 4)
        )


def main() -> None:
    from maia.utils import cli_bootstrap

    cli_bootstrap()
    parser = argparse.ArgumentParser(description="Run one bounded YouTube topic enrichment cycle")
    GrapherAgent.add_cli_args(parser)
    args = parser.parse_args()
    asyncio.run(grapher_operation(args.batch_size, args.max_batches))


if __name__ == "__main__":
    main()
