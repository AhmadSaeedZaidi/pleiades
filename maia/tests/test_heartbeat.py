"""
Tests for Maia Heartbeat fleet-unit enumeration.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from atlas.notifications import AlertLevel
from maia.heartbeat.flow import (
    _RUN_STATE_HEALTH,
    FLEET_DEPLOYMENTS,
    FLEET_UNITS,
    _knowledge_graph_field,
    _unit_state,
    collect_fleet_status,
    heartbeat_flow,
    heartbeat_operation,
)

_GRAPH_METRICS = {
    "topic_count": 3,
    "video_edges": 120,
    "channel_edges": 30,
    "video_total": 100,
    "video_checked": 75,
    "channel_total": 20,
    "channel_checked": 10,
    "video_checked_1h": 9,
    "channel_checked_1h": 3,
    "empty": 4,
    "unavailable": 2,
    "latest_observed": datetime(2026, 10, 4, tzinfo=UTC),
}


@pytest.fixture(autouse=True)
def graph_metrics():
    with patch(
        "maia.heartbeat.flow.collect_knowledge_graph_metrics",
        new_callable=AsyncMock,
        return_value=_GRAPH_METRICS,
    ) as collector:
        yield collector


def _deployment(name):
    d = MagicMock()
    d.id = uuid4()
    d.name = name
    return d


def test_fleet_unit_is_pleiades_ingestion():
    # The application scheduler is the active executor; Prefect deployment
    # health is optional telemetry only.
    assert FLEET_UNITS == ["pleiades-ingestion"]


def test_fleet_deployments_are_the_nine_agents():
    # The heartbeat now reports all nine automated Prefect deployments.
    assert set(FLEET_DEPLOYMENTS) == {
        "streamer",
        "singer",
        "painter",
        "scribe",
        "hunter",
        "tracker",
        "archeologist",
        "heartbeat",
        "janitor",
    }


def test_fleet_excludes_manual_only_muralist():
    # muralist is a manual-only capability with no deployment to probe.
    assert "muralist" not in FLEET_DEPLOYMENTS


# --- Run-state health / staleness thresholds ---


@pytest.mark.parametrize(
    "state, expected",
    [
        ("Completed", "healthy"),
        ("Running", "healthy"),
        ("Pending", "warn"),
        ("Scheduled", "warn"),
        ("Paused", "warn"),
        ("Cancelled", "warn"),
        ("Failed", "down"),
        ("Crashed", "down"),
    ],
)
def test_run_state_health_mapping(state, expected):
    """Every Prefect run-state maps to a governed fleet-health label."""
    assert _RUN_STATE_HEALTH[state] == expected


def test_unknown_run_state_falls_back_to_warn():
    """An unrecognised (e.g. future) run-state is treated as a staleness warn,
    not down — a new state must never flip a deployment to down."""
    assert _RUN_STATE_HEALTH.get("some_new_state", "warn") == "warn"


@pytest.mark.asyncio
async def test_collect_fleet_status_stale_never_run_as_warn():
    """A deployment that has never run is reported as 'never run' / warn."""
    deploy = _deployment("tracker")

    client = MagicMock()
    client.read_deployments = AsyncMock(return_value=[deploy])
    client.read_flow_runs = AsyncMock(return_value=[])

    with patch("maia.heartbeat.flow.get_client", return_value=_AsyncCtx(client)):
        status = await collect_fleet_status()

    assert status["tracker"] == ("never run", "warn")


@pytest.mark.asyncio
async def test_collect_fleet_status_marks_unregistered_as_down():
    """A known deployment absent from the API response is 'not registered' / down."""
    deploy = _deployment("hunter")
    client = MagicMock()
    client.read_deployments = AsyncMock(return_value=[deploy])
    client.read_flow_runs = AsyncMock(return_value=[])  # not reached for the missing one

    with patch("maia.heartbeat.flow.get_client", return_value=_AsyncCtx(client)):
        status = await collect_fleet_status()

    # hunter is present; every other FLEET_DEPLOYMENT is missing -> down.
    assert status["hunter"][1] == "warn"
    assert status["scribe"] == ("not registered", "down")


@pytest.mark.asyncio
async def test_collect_fleet_status_last_run_state_drives_health():
    """The most recent flow-run's state name drives the deployment's health."""
    deploy = _deployment("janitor")
    run = MagicMock()
    run.state = MagicMock()
    run.state.name = "Failed"
    client = MagicMock()
    client.read_deployments = AsyncMock(return_value=[deploy])
    client.read_flow_runs = AsyncMock(return_value=[run])

    with patch("maia.heartbeat.flow.get_client", return_value=_AsyncCtx(client)):
        status = await collect_fleet_status()

    assert status["janitor"] == ("last run: Failed", "down")


class _AsyncCtx:
    """Async-context-manager shim for ``async with get_client()``."""

    def __init__(self, client):
        self._client = client

    async def __aenter__(self):
        return self._client

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_collect_fleet_status_api_unreachable_marks_all_down():
    """If the Prefect API is unreachable, every deployment is 'API unreachable'"""
    with patch("maia.heartbeat.flow.get_client", side_effect=RuntimeError("no control plane")):
        status = await collect_fleet_status()
    assert status == {nm: ("API unreachable", "down") for nm in FLEET_DEPLOYMENTS}


@pytest.mark.parametrize(
    "props, expected_health",
    [
        ("ActiveState=active", "healthy"),
        ("ActiveState=activating\nSubState=auto-restart\nExecMainStatus=0", "healthy"),
        ("ActiveState=activating\nSubState=auto-restart\nExecMainStatus=1", "warn"),
        ("ActiveState=activating\nSubState=starting", "warn"),
        ("ActiveState=failed\nExecMainStatus=127", "down"),
    ],
)
def test_unit_state_health(monkeypatch, props, expected_health):
    """systemctl probe output maps to fleet-health labels."""
    with patch(
        "maia.heartbeat.flow.subprocess.run",
        return_value=MagicMock(stdout=props, returncode=0),
    ):
        _label, health = _unit_state("pleiades-ingestion")
    assert health == expected_health


@patch("maia.heartbeat.flow.subprocess.run", side_effect=OSError("no systemctl"))
def test_unit_state_probe_error_is_down(mock_run):
    """A failed probe must never crash the cycle; reports down."""
    _label, health = _unit_state("pleiades-ingestion")
    assert health == "down"


_METRICS = {
    "status_counts": {},
    "total": 0,
    "transcripts": 0,
    "with_visuals": 0,
    "audios": 0,
    "ingested_1h": 0,
}
_STORAGE = {
    "hot_bodies": 10,
    "staged": 4,
    "retained": 6,
    "payload_bytes": 1024,
    "database_bytes": 2**30,
    "tracking_due": 3,
    "last_tracked_at": None,
}


@pytest.fixture
def reporting(monkeypatch):
    import maia.heartbeat.flow as heartbeat
    from maia.telemetry import CycleMonitor

    monitor = CycleMonitor(lambda: 100.0)
    monitor.register("tracker", 60)
    monitor.start("tracker")
    monitor.finish("tracker", {"videos_updated": 10})
    pipeline = AsyncMock(return_value=dict(_METRICS))
    storage = AsyncMock(return_value=_STORAGE)
    sender = AsyncMock(return_value=True)
    monkeypatch.setattr(heartbeat, "cycle_monitor", monitor)
    monkeypatch.setattr(
        heartbeat, "collect_service_status", lambda: {"pleiades-ingestion": ("active", "healthy")}
    )
    monkeypatch.setattr(heartbeat, "collect_pipeline_metrics", pipeline)
    monkeypatch.setattr(heartbeat, "collect_storage_metrics", storage)
    monkeypatch.setattr(
        heartbeat, "collect_disk_metrics", lambda: {"used": 50, "free": 50, "total": 100}
    )
    monkeypatch.setattr(heartbeat, "quota_exhausted_agents", lambda: [])
    monkeypatch.setattr(
        heartbeat,
        "get_settings",
        lambda: MagicMock(TOPIC_SYNC_ENABLED=True, GROK_API_KEY=None, MISTRAL_API_KEY=None),
    )
    monkeypatch.setattr(heartbeat.notifier, "send", sender)
    return pipeline, storage, sender, monitor


@pytest.mark.asyncio
async def test_plain_heartbeat_skips_prefect_and_reports_verified_progress(reporting):
    with patch("maia.heartbeat.flow.get_client") as client:
        summary = await heartbeat_operation()
    client.assert_not_called()
    assert summary["healthy"] and summary["executor_online"]
    fields = reporting[2].await_args.kwargs["fields"]
    assert "Staged: 4" in fields["Hot / cold storage"]
    assert "tracker: idle" in fields["Workers"]
    assert "configuration only; no API probe" in fields["API configuration"]
    assert reporting[2].await_args.kwargs["level"] == AlertLevel.SUCCESS


@pytest.mark.asyncio
async def test_optional_prefect_is_visible_without_reclassifying_executor(reporting):
    with patch(
        "maia.heartbeat.flow.collect_fleet_status",
        return_value={name: ("API unreachable", "down") for name in FLEET_DEPLOYMENTS},
    ):
        summary = await heartbeat_flow.fn()
    assert summary["executor_online"] and summary["healthy"]
    assert "API unreachable" in reporting[2].await_args.kwargs["fields"]["Optional Prefect"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_collector", ["graph", "pipeline", "storage"])
async def test_metrics_failure_preserves_other_reporting(
    reporting, graph_metrics, failed_collector
):
    collector = {"graph": graph_metrics, "pipeline": reporting[0], "storage": reporting[1]}[
        failed_collector
    ]
    collector.side_effect = RuntimeError("private database details")
    summary = await heartbeat_operation()
    assert summary["notified"] and summary["executor_online"]
    assert not summary["healthy"] and summary["status"] == "Attention"
    fields = reporting[2].await_args.kwargs["fields"]
    assert "private database details" not in str(fields)
    if failed_collector != "graph":
        assert "Topic links: **150**" in fields["Knowledge graph"]
    if failed_collector != "pipeline":
        assert "Videos: **0**" in fields["Extraction"]


@pytest.mark.asyncio
async def test_stage_failures_are_not_a_false_all_clear(reporting):
    reporting[0].return_value = dict(
        _METRICS, failed_steps=5, failed_step_counts={"audio": 5}, status_counts={"FAILED": 2}
    )
    summary = await heartbeat_operation()
    assert not summary["healthy"] and summary["executor_online"]
    assert "5 stage failures; 2 legacy failed records" in summary["issues"]
    assert reporting[2].await_args.kwargs["level"] == AlertLevel.WARNING


@pytest.mark.asyncio
async def test_actual_failed_worker_and_disk_pressure_are_visible(reporting, monkeypatch):
    reporting[3].start("tracker")
    reporting[3].finish("tracker", error=RuntimeError("private endpoint"))
    monkeypatch.setattr(
        "maia.heartbeat.flow.collect_disk_metrics", lambda: {"used": 96, "free": 4, "total": 100}
    )
    summary = await heartbeat_operation()
    assert summary["status"] == "Degraded" and "tracker failed" in summary["issues"]
    fields = reporting[2].await_args.kwargs["fields"]
    assert "RuntimeError, 1 consecutive" in fields["Workers"]
    assert "private endpoint" not in str(fields)
    assert reporting[2].await_args.kwargs["level"] == AlertLevel.CRITICAL


@pytest.mark.asyncio
async def test_disk_warning_accounts_for_reserved_space(reporting, monkeypatch):
    monkeypatch.setattr(
        "maia.heartbeat.flow.collect_disk_metrics",
        lambda: {"used": 80, "free": 10, "total": 100},
    )
    summary = await heartbeat_operation()
    assert summary["status"] == "Attention"
    assert "disk 88.9% used" in summary["issues"]


@pytest.mark.asyncio
async def test_standalone_snapshot_does_not_invent_worker_health(reporting, monkeypatch):
    monkeypatch.setattr("maia.heartbeat.flow.cycle_monitor.snapshot", lambda: None)
    summary = await heartbeat_operation()
    assert not summary["healthy"]
    assert "scheduler observations unavailable" in summary["issues"]


@pytest.mark.asyncio
async def test_collectors_are_parallel_and_cancelled_at_deadline(reporting, monkeypatch):
    import asyncio

    import maia.heartbeat.flow as heartbeat

    arrivals = []
    ready = asyncio.Event()

    async def collector(value):
        arrivals.append(1)
        if len(arrivals) == 3:
            ready.set()
        await ready.wait()
        return value

    monkeypatch.setattr(heartbeat, "collect_pipeline_metrics", lambda: collector(_METRICS))
    monkeypatch.setattr(
        heartbeat, "collect_knowledge_graph_metrics", lambda: collector(_GRAPH_METRICS)
    )
    monkeypatch.setattr(heartbeat, "collect_storage_metrics", lambda: collector(_STORAGE))
    summary = await asyncio.wait_for(heartbeat_operation(), timeout=1)
    assert summary["notified"] and len(arrivals) == 3
    real_timeout = asyncio.timeout
    monkeypatch.setattr(heartbeat.asyncio, "timeout", lambda _: real_timeout(0.01))

    async def blocked():
        await asyncio.Event().wait()
        return {}

    assert await heartbeat._bounded_metrics("blocked", blocked) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [True, False])
async def test_graph_coverage_and_pause_are_reported(reporting, monkeypatch, enabled):
    monkeypatch.setattr(
        "maia.heartbeat.flow.get_settings",
        lambda: MagicMock(TOPIC_SYNC_ENABLED=enabled, GROK_API_KEY=None, MISTRAL_API_KEY=None),
    )
    summary = await heartbeat_operation()
    assert summary["topic_sync_enabled"] is enabled
    graph = reporting[2].await_args.kwargs["fields"]["Knowledge graph"]
    assert f"Backfill: **{'enabled' if enabled else 'paused'}**" in graph
    assert "Videos checked: **75 / 100 (75.0%)**" in graph
    assert "Channels checked: **10 / 20 (50.0%)**" in graph
    assert "Checked (1h): **9 videos** · **3 channels**" in graph


def test_empty_graph_reports_first_check_and_paused_backfill():
    metrics = {key: 0 for key in _GRAPH_METRICS}
    metrics["latest_observed"] = None
    assert "awaiting first check" in _knowledge_graph_field(metrics, enabled=False)


@pytest.mark.asyncio
async def test_report_fits_discord_limits_even_with_large_optional_telemetry(reporting):
    with patch(
        "maia.heartbeat.flow.collect_fleet_status",
        return_value={name: ("x" * 10000, "down") for name in FLEET_DEPLOYMENTS},
    ):
        await heartbeat_operation(include_prefect=True)
    payload = reporting[2].await_args.kwargs
    fields = payload["fields"]
    assert len(fields) <= 25 and all(0 < len(value) <= 1024 for value in fields.values())
    size = sum(len(key) + len(value) for key, value in fields.items())
    assert size + len(payload["title"]) + len(payload["description"]) + 100 < 6000
