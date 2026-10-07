"""In-process Prefect orchestrator for the Pleiades fleet.

Replaces the scheduler + ``prefect worker start`` ``--type process`` model,
where every flow cycle spawned a full ``python -m prefect.engine`` subprocess
(~17 threads, 170-400 MB RSS each). With many flows that churned past the
systemd ``TasksMax`` ceiling, causing ``RuntimeError: can't start new thread``.

This service invokes plain application operations on a single event loop. The
Prefect ``@flow`` entrypoints remain compatibility adapters for rollback and
optional telemetry, but the live scheduler does not require the remote
Prefect API to start or execute ingestion.

Prefect deployment schedules are intentionally omitted from ``prefect.yaml``;
the systemd service below is the single cadence source. The deployments remain
available as rollback adapters while this migration settles.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any

# Plain operations own live cadence; Prefect flows remain optional adapters.
from maia.grapher.flow import grapher_operation
from maia.heartbeat.flow import heartbeat_operation
from maia.hunter.flow import hunter_operation
from maia.janitor.flow import janitor_operation
from maia.painter.flow import painter_operation
from maia.scribe.flow import scribe_operation
from maia.singer.flow import singer_operation
from maia.streamer.flow import streamer_operation
from maia.telemetry import cycle_monitor
from maia.tracker.flow import tracker_operation

logger = logging.getLogger("maia.orchestrator")

CoroFactory = Callable[..., Coroutine[Any, Any, Any]]


@dataclass
class CycleSpec:
    """Declarative definition of one agent's periodic in-process cycle."""

    name: str
    operation: CoroFactory
    interval: float
    kwargs: dict[str, Any] = field(default_factory=dict)
    initial_jitter: float = 0.0


def build_specs() -> list[CycleSpec]:
    """Build the single scheduler cadence and its plain operations."""
    return [
        # (name, operation, interval_seconds, kwargs, initial_jitter)
        CycleSpec("streamer", streamer_operation, 120, {"batch_size": 5}, 0.0),
        CycleSpec("singer", singer_operation, 300, {"batch_size": 10}, 0.6),
        CycleSpec("painter", painter_operation, 120, {"batch_size": 5}, 1.2),
        CycleSpec("scribe", scribe_operation, 120, {"batch_size": 10}, 1.8),
        CycleSpec("hunter", hunter_operation, 1200, {"batch_size": 1}, 2.4),
        CycleSpec("tracker", tracker_operation, 60, {"batch_size": 50}, 3.0),
        CycleSpec("heartbeat", heartbeat_operation, 900, {}, 3.6),
        CycleSpec("janitor", janitor_operation, 900, {"dry_run": False}, 4.2),
        CycleSpec("grapher", grapher_operation, 600, {"batch_size": 50, "max_batches": 4}, 4.8),
    ]


async def run_cycle(
    name: str,
    operation: CoroFactory,
    *,
    kwargs: dict[str, Any] | None = None,
    jitter: float = 0.0,
) -> None:
    """Run one plain operation, isolating failures from the other cycles."""
    if jitter:
        await asyncio.sleep(jitter)
    cycle_monitor.start(name)
    try:
        result = await operation(**(kwargs or {}))
        cycle_monitor.finish(name, result)
        logger.info("orchestrator cycle %s complete: %s", name, result)
    except asyncio.CancelledError:
        cycle_monitor.cancel(name)
        raise
    except Exception as error:  # noqa: BLE001 - a failing cycle must not kill the loop
        cycle_monitor.finish(name, error=error)
        logger.exception("orchestrator cycle %s FAILED", name)


async def agent_loop(spec: CycleSpec) -> None:
    """Run one agent forever with ``spec.interval`` between cycle attempts."""
    cycle_monitor.register(spec.name, spec.interval)
    logger.info(
        "starting orchestrator cycle %s (interval=%ss, kwargs=%s)",
        spec.name,
        spec.interval,
        spec.kwargs,
    )
    first_tick = True
    while True:
        await run_cycle(
            spec.name,
            spec.operation,
            kwargs=spec.kwargs,
            jitter=spec.initial_jitter if first_tick else 0.0,
        )
        first_tick = False
        await asyncio.sleep(spec.interval)


async def run(specs: list[CycleSpec] | None = None) -> None:
    """Launch one asyncio task per agent cycle loop, all in the same process."""
    spec_list = specs or build_specs()
    logger.info("starting orchestrator with %d agents", len(spec_list))
    tasks = [asyncio.create_task(agent_loop(spec), name=f"cycle-{spec.name}") for spec in spec_list]
    await asyncio.gather(*tasks)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s - %(message)s",
        force=True,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stop = asyncio.Event()

    def _request_stop(signum: int, _frame: Any) -> None:
        logger.warning("received signal %s; draining", signum)
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop, sig, None)
        except NotImplementedError:  # non-POSIX
            signal.signal(sig, lambda s, f: stop.set())

    try:
        loop.run_until_complete(_run_until_stop(stop))
    finally:
        # Cancel in-flight plain operations before closing the event loop.
        pending = asyncio.all_tasks(loop)
        for t in pending:
            t.cancel()
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()


async def _run_until_stop(stop: asyncio.Event) -> None:
    runner = asyncio.create_task(run())
    try:
        await stop.wait()
    finally:
        runner.cancel()
        try:
            await runner
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    main()
