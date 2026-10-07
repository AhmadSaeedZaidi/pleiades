"""
Tests for Maia Orchestrator (in-process fleet scheduler).

Contract: docs/implementation-checklists/orchestrator-contract.md
Covers: build_specs surface, run_cycle failure isolation / cancellation
pass-through / jitter, agent_loop cadence + first-tick jitter, run dispatch,
signal drain, and _run_until_stop shutdown.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from maia.orchestrator import (
    CycleSpec,
    _run_until_stop,
    agent_loop,
    build_specs,
    main,
    run,
    run_cycle,
)

# Real asyncio.sleep captured at import time (before the autouse mock_sleep
# fixture patches asyncio.sleep) so tests can yield to the event loop.
_REAL_SLEEP = asyncio.sleep


@pytest.fixture(autouse=True)
def cycle_observations(monkeypatch):
    from maia.telemetry import CycleMonitor

    monitor = CycleMonitor()
    monkeypatch.setattr("maia.orchestrator.cycle_monitor", monitor)
    return monitor


_SCHEDULED_AGENTS = {
    "streamer",
    "singer",
    "painter",
    "scribe",
    "hunter",
    "tracker",
    "heartbeat",
    "janitor",
    "grapher",
}


# --- build_specs: public surface ---


@pytest.mark.asyncio
async def test_scheduler_records_failure_then_recovery(cycle_observations):
    cycle_observations.register("tracker", 60)
    await run_cycle("tracker", AsyncMock(side_effect=RuntimeError("private details")))
    assert cycle_observations.snapshot()["cycles"][0]["state"] == "failed"
    await run_cycle("tracker", AsyncMock(return_value={"videos_updated": 5}))
    row = cycle_observations.snapshot()["cycles"][0]
    assert row["state"] == "idle" and row["failures"] == 0
    assert row["progress_1h"] == {"videos_updated": 5}


def test_build_specs_returns_scheduled_agents():
    specs = build_specs()
    assert len(specs) == 9
    assert {s.name for s in specs} == _SCHEDULED_AGENTS


def test_build_specs_uses_plain_operations():
    specs = build_specs()
    assert all(s.operation.__name__.endswith("_operation") for s in specs)
    assert all(not hasattr(s.operation, "fn") for s in specs)
    assert [s.initial_jitter for s in specs] == pytest.approx([i * 0.6 for i in range(9)])


@pytest.mark.asyncio
async def test_plain_cycle_runs_without_prefect_api():
    """The application scheduler invokes a plain callable, not Prefect."""
    operation = AsyncMock(return_value={"videos_processed": 0})
    with patch.dict("os.environ", {"PREFECT_API_URL": "http://127.0.0.1:1/api"}):
        await run_cycle("streamer", operation)
    operation.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_real_plain_operation_runs_without_prefect_api():
    """A real application operation does not consult the Prefect API."""
    from maia.streamer import flow as streamer_module

    with (
        patch.dict("os.environ", {"PREFECT_API_URL": "http://127.0.0.1:1/api"}),
        patch.object(streamer_module, "get_rate_limit_cooldown_until", return_value=None),
        patch.object(
            streamer_module.BaseBatchAgent,
            "run",
            new_callable=AsyncMock,
            return_value={"videos_processed": 0},
        ) as base_run,
        patch.object(streamer_module, "clear_rate_limit_cooldown"),
    ):
        await run_cycle(
            "streamer",
            streamer_module.streamer_operation,
            kwargs={"batch_size": 1},
        )

    base_run.assert_awaited_once()


@pytest.mark.asyncio
async def test_prefect_flow_adapter_delegates_to_plain_operation():
    """Compatibility flow entrypoints are only telemetry/rollback adapters."""
    from maia.streamer import flow as streamer_module

    operation = AsyncMock(return_value={"videos_processed": 0})
    with patch.object(streamer_module, "streamer_operation", operation):
        result = await streamer_module.streamer_flow.fn(batch_size=5)

    assert result == {"videos_processed": 0}
    operation.assert_awaited_once_with(batch_size=5)


def test_build_specs_intervals_match_production_cadence():
    specs = {s.name: s for s in build_specs()}
    assert specs["tracker"].interval == 60
    assert specs["streamer"].interval == 120
    assert specs["painter"].interval == 120
    assert specs["scribe"].interval == 120
    assert specs["singer"].interval == 300
    assert specs["hunter"].interval == 1200
    assert specs["heartbeat"].interval == 900
    assert specs["janitor"].interval == 900


def test_build_specs_kwargs_match_scheduler_defaults():
    specs = {s.name: s for s in build_specs()}
    assert specs["tracker"].kwargs == {"batch_size": 50}
    assert specs["streamer"].kwargs == {"batch_size": 5}
    assert specs["painter"].kwargs == {"batch_size": 5}
    assert specs["scribe"].kwargs == {"batch_size": 10}
    assert specs["singer"].kwargs == {"batch_size": 10}
    assert specs["hunter"].kwargs == {"batch_size": 1}
    assert specs["heartbeat"].kwargs == {}
    assert specs["janitor"].kwargs == {"dry_run": False}


# --- run_cycle: failure isolation, cancellation, jitter ---


@pytest.mark.asyncio
async def test_run_cycle_logs_completion_on_success():
    coro = AsyncMock(return_value="ok")
    with patch("maia.orchestrator.logger") as mock_logger:
        await run_cycle("tracker", lambda: coro())
    coro.assert_awaited_once()
    mock_logger.info.assert_called_once()


@pytest.mark.asyncio
async def test_run_cycle_swallows_failure_and_logs():
    async def boom():
        raise RuntimeError("boom")

    with patch("maia.orchestrator.logger") as mock_logger:
        # A failing cycle must never propagate out of run_cycle.
        await run_cycle("tracker", boom)
    mock_logger.exception.assert_called_once()


@pytest.mark.asyncio
async def test_run_cycle_re_raises_cancelled_error():
    async def cancel():
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await run_cycle("tracker", cancel)


@pytest.mark.asyncio
async def test_run_cycle_jitter_sleeps_before_await(mock_sleep):
    coro = AsyncMock(return_value=None)
    await run_cycle("tracker", lambda: coro(), jitter=1.5)
    mock_sleep.assert_awaited_once_with(1.5)
    coro.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_cycle_no_jitter_does_not_sleep(mock_sleep):
    coro = AsyncMock(return_value=None)
    await run_cycle("tracker", lambda: coro())
    mock_sleep.assert_not_awaited()


# --- agent_loop: cadence + first-tick jitter ---


async def _yielding_flow(**kwargs):
    """Flow stub that yields to the event loop so the infinite loop can be
    driven and cancelled from the test."""
    await _REAL_SLEEP(0)
    return None


@pytest.mark.asyncio
async def test_agent_loop_calls_plain_operation_with_kwargs_and_sleeps_interval(mock_sleep):
    operation = MagicMock(side_effect=_yielding_flow)
    spec = CycleSpec(
        "tracker", operation, interval=60, kwargs={"batch_size": 50}, initial_jitter=0.6
    )
    task = asyncio.create_task(agent_loop(spec))
    for _ in range(3):
        await _REAL_SLEEP(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    operation.assert_called_with(batch_size=50)
    # First sleep is the stagger jitter; subsequent sleeps are the interval.
    sleeps = [c.args[0] for c in mock_sleep.await_args_list]
    assert sleeps[0] == 0.6
    assert 60 in sleeps


@pytest.mark.asyncio
async def test_agent_loop_jitter_only_on_first_tick(mock_sleep):
    operation = MagicMock(side_effect=_yielding_flow)
    spec = CycleSpec(
        "painter", operation, interval=120, kwargs={"batch_size": 5}, initial_jitter=1.2
    )
    task = asyncio.create_task(agent_loop(spec))
    for _ in range(4):
        await _REAL_SLEEP(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    sleeps = [c.args[0] for c in mock_sleep.await_args_list]
    assert sleeps[0] == 1.2
    # Every sleep after the first must be the plain interval (no jitter).
    assert all(s == 120 for s in sleeps[1:])


# --- run: worker dispatch ---


@pytest.mark.asyncio
async def test_run_dispatches_one_task_per_spec():
    specs = [
        CycleSpec("a", MagicMock(return_value=AsyncMock(return_value=None)), 1),
        CycleSpec("b", MagicMock(return_value=AsyncMock(return_value=None)), 1),
    ]
    with (
        patch("maia.orchestrator.agent_loop", return_value=AsyncMock()),
        patch("maia.orchestrator.asyncio.create_task") as mock_ct,
        patch("maia.orchestrator.asyncio.gather") as mock_gather,
    ):

        def close_coro(coro, **_kwargs):
            coro.close()
            return MagicMock()

        mock_ct.side_effect = close_coro
        mock_gather.return_value = _REAL_SLEEP(0)
        await run(specs)

    assert mock_ct.call_count == 2
    names = [c.kwargs["name"] for c in mock_ct.call_args_list]
    assert names == ["cycle-a", "cycle-b"]


@pytest.mark.asyncio
async def test_run_defaults_to_build_specs():
    with (
        patch("maia.orchestrator.build_specs") as mock_build,
        patch("maia.orchestrator.agent_loop", return_value=AsyncMock()),
        patch("maia.orchestrator.asyncio.create_task") as mock_ct,
        patch("maia.orchestrator.asyncio.gather") as mock_gather,
    ):

        def close_coro(coro, **_kwargs):
            coro.close()
            return MagicMock()

        mock_ct.side_effect = close_coro
        mock_build.return_value = [CycleSpec("x", MagicMock(), 1)]
        mock_gather.return_value = _REAL_SLEEP(0)
        await run()

    mock_build.assert_called_once()
    assert mock_ct.call_count == 1


# --- shutdown: signal drain ---


@pytest.mark.asyncio
async def test_run_until_stop_cancels_runner_when_stop_set():
    stop = asyncio.Event()
    cancelled = asyncio.Event()

    async def blocking_run():
        stop.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with patch("maia.orchestrator.run", new=blocking_run):
        await _run_until_stop(stop)
    assert cancelled.is_set()


def test_main_installs_signal_handlers_and_drains_pending_tasks():
    loop = asyncio.new_event_loop()

    async def wait_forever() -> None:
        await asyncio.Event().wait()

    pending = [loop.create_task(wait_forever()), loop.create_task(wait_forever())]
    signal_handler = MagicMock()
    loop.add_signal_handler = signal_handler  # type: ignore[method-assign]

    async def stop_immediately(_stop: asyncio.Event) -> None:
        return None

    with (
        patch("maia.orchestrator.asyncio.new_event_loop", return_value=loop),
        patch("maia.orchestrator._run_until_stop", new=stop_immediately),
        patch("maia.orchestrator.logging.basicConfig") as configure_logging,
    ):
        main()

    assert configure_logging.call_args.kwargs["force"] is True
    # SIGINT + SIGTERM handlers registered on the loop.
    assert signal_handler.call_count == 2
    # In-flight cycles are cancelled (drained) before the loop closes.
    for t in pending:
        assert t.cancelled()
    assert loop.is_closed()


def test_every_agent_package_is_a_real_package():
    """Regression: singer/ and streamer/ had no __init__.py.

    maia's pyproject uses `packages = [{include = "maia", from = "src"}]`, which
    relies on __init__.py discovery, so a built wheel silently omitted
    singer/flow.py and streamer/flow.py. It only worked in production because the
    systemd unit sets PYTHONPATH and Python's implicit-namespace fallback accepts
    the directories — a packaging bug that would surface at the first wheel build.
    """
    import maia

    root = Path(maia.__file__).parent
    for agent in (
        "hunter",
        "archeologist",
        "streamer",
        "muralist",
        "tracker",
        "janitor",
        "painter",
        "scribe",
        "singer",
        "heartbeat",
        "grapher",
    ):
        init = root / agent / "__init__.py"
        assert init.is_file(), f"{agent} is missing __init__.py; wheel builds will omit it"
        assert init.stat().st_size > 0, f"{agent}/__init__.py is empty"


def test_singer_and_streamer_are_importable_as_packages():
    from maia.singer import SingerAgent
    from maia.streamer import StreamerAgent

    assert SingerAgent.name == "singer"
    assert StreamerAgent.name == "streamer"
