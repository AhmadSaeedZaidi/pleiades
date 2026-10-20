"""Shared batch-agent scaffolding (Template Method) for the video consumers.

Every "claim a batch of work items -> process each under a bounded semaphore ->
store the results -> report stats" agent repeats the *same* control loop:

* fetch the next batch (``claim_batch``)
* bound concurrency with an :class:`asyncio.Semaphore` + ``asyncio.gather``
* pace with a throttle sleep
* re-raise any fatal error (e.g. quota exhaustion) caught by ``return_exceptions``
* persist the successful results in one batch (``store_results``)
* run a post-cycle hook (``after_cycle``)

``BaseBatchAgent`` implements that loop exactly once; concrete agents supply the
three hooks (``claim_batch`` / ``process_one`` / ``store_results``) plus policy
constants. This is a pure refactor of the former per-agent ``*_flow`` functions —
the observable behaviour (logging, vault commits, DB markers, error handling) is
unchanged.
"""

from __future__ import annotations

import abc
import argparse
import asyncio
import logging
from typing import Any

from atlas.models import Video
from atlas.state import clear_quota_exhausted

logger = logging.getLogger(__name__)


class BaseBatchAgent(abc.ABC):
    """Template base for the claim -> process -> store video consumers."""

    # ── subclass contract ──────────────────────────────────────────────────
    name: str
    default_batch_size: int = 5
    # Bounded concurrency for the per-item semaphore.
    max_concurrent: int = 1
    # Pacing delay (seconds) applied after each item, to stay under rate limits.
    throttle_seconds: float = 0.0
    # Exception types that, if any item raises, are re-raised after the gather
    # (terminating the cycle). E.g. quota exhaustion.
    raise_on: tuple[type[Exception], ...] = ()
    # When True, clear the stale quota-exhausted marker for this agent at the
    # end of a successful (non-raise_on) cycle. Set False for agents that do
    # not enforce quota gating.
    clear_quota_after_cycle: bool = True

    @staticmethod
    def add_cli_args(parser: argparse.ArgumentParser) -> None:
        """Register the common bounded-batch CLI option."""
        parser.add_argument("--batch-size", type=int, default=None)

    @abc.abstractmethod
    async def claim_batch(self, n: int) -> list[Video]:
        """Return the next batch of work items to process."""

    @abc.abstractmethod
    async def process_one(self, item: Video) -> Any:
        """Process a single work item. Runs under the concurrency semaphore.
        Its return value is collected and passed to :meth:`store_results`."""

    # ── optional hooks ─────────────────────────────────────────────────────
    async def store_results(self, results: list[Any]) -> None:
        """Persist/commit a batch of successfully-processed results.

        ``results`` excludes any item whose processing raised.
        """
        return None

    async def after_cycle(self) -> None:
        """Hook run after results are stored (only when no ``raise_on`` error)."""
        return None

    # ── shared loop ────────────────────────────────────────────────────────
    async def run(self, batch_size: int | None = None, **kwargs: Any) -> dict[str, Any]:
        """Execute one cycle and return its statistics dict."""
        n = batch_size or self.default_batch_size
        run_logger = logger.getChild(self.name)
        run_logger.info(f"=== Starting {self.name.capitalize()} Cycle ===")

        targets = await self.claim_batch(n)
        if not targets:
            run_logger.info(f"No work for {self.name}. Cycle complete (idle).")
            return {"videos_processed": 0}

        run_logger.info(f"Processing {len(targets)} items concurrently...")

        sem = asyncio.Semaphore(self.max_concurrent)

        async def _bounded(item: Video) -> Any:
            async with sem:
                result = await self.process_one(item)
                if self.throttle_seconds:
                    await asyncio.sleep(self.throttle_seconds)
                return result

        gathered = await asyncio.gather(*[_bounded(t) for t in targets], return_exceptions=True)

        if self.raise_on:
            for r in gathered:
                if isinstance(r, self.raise_on):
                    raise r

        ok = [r for r in gathered if not isinstance(r, Exception)]
        await self.store_results(ok)
        if self.clear_quota_after_cycle:
            clear_quota_exhausted(self.name)
        await self.after_cycle()

        run_logger.info(
            f"=== {self.name.capitalize()} Cycle Complete === Processed {len(targets)} items"
        )
        return {"videos_processed": len(targets)}
