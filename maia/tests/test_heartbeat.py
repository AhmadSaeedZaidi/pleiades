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


@pytest.mark.asyncio
async def test_prefect_telemetry_does_not_mark_executor_unhealthy():
    """The Prefect adapter exposes control-plane outage as optional telemetry."""
    with (
        patch(
            "maia.heartbeat.flow.collect_service_status",
            return_value={"pleiades-ingestion": ("active", "healthy")},
        ),
        patch(
            "maia.heartbeat.flow.collect_fleet_status",
            return_value={nm: ("API unreachable", "down") for nm in FLEET_DEPLOYMENTS},
        ) as fleet_status,
        patch(
            "maia.heartbeat.flow.collect_pipeline_metrics",
            return_value={
                "status_counts": {},
                "total": 0,
                "transcripts": 0,
                "with_visuals": 0,
                "audios": 0,
                "ingested_1h": 0,
            },
        ),
        patch("maia.heartbeat.flow.check_audio_api_health", return_value=("healthy", "ok")),
        patch("maia.heartbeat.flow.notifier") as mock_notifier,
    ):
        mock_notifier.send = AsyncMock(return_value=True)
        summary = await heartbeat_flow.fn()

    fleet_status.assert_awaited_once()
    assert summary["notified"] is True
    assert summary["healthy"] is True
    assert summary["down"] == []


@pytest.mark.asyncio
async def test_plain_heartbeat_skips_prefect_with_unavailable_api_url():
    """Plain scheduler heartbeat never initializes or contacts Prefect."""
    with (
        patch.dict("os.environ", {"PREFECT_API_URL": "http://127.0.0.1:1/api"}),
        patch(
            "maia.heartbeat.flow.collect_service_status",
            return_value={"pleiades-ingestion": ("active", "healthy")},
        ),
        patch("maia.heartbeat.flow.collect_fleet_status", new_callable=AsyncMock) as fleet_status,
        patch("maia.heartbeat.flow.get_client") as get_client,
        patch(
            "maia.heartbeat.flow.collect_pipeline_metrics",
            return_value={
                "status_counts": {},
                "total": 0,
                "transcripts": 0,
                "with_visuals": 0,
                "audios": 0,
                "ingested_1h": 0,
            },
        ),
        patch("maia.heartbeat.flow.check_audio_api_health", return_value=("healthy", "ok")),
        patch("maia.heartbeat.flow.notifier") as mock_notifier,
    ):
        mock_notifier.send = AsyncMock()
        summary = await heartbeat_operation()

    fleet_status.assert_not_awaited()
    get_client.assert_not_called()
    assert summary["healthy"] is True


_METRICS = {
    "status_counts": {},
    "total": 0,
    "transcripts": 0,
    "with_visuals": 0,
    "audios": 0,
    "ingested_1h": 0,
}


@pytest.mark.asyncio
async def test_down_deployments_are_surfaced_not_silently_healthy():
    """Regression: `fleet_down` was initialised to [] and never populated.

    Every branch read it, so a Prefect deployment reporting "not registered" or
    "last run: Failed" could never escalate the banner and the summary reported
    healthy with an empty `down` list — a false all-clear.
    """
    fleet = {nm: ("not registered", "down") for nm in FLEET_DEPLOYMENTS}
    with (
        patch(
            "maia.heartbeat.flow.collect_service_status",
            return_value={"pleiades-ingestion": ("active", "healthy")},
        ),
        patch("maia.heartbeat.flow.collect_fleet_status", return_value=fleet),
        patch("maia.heartbeat.flow.collect_pipeline_metrics", return_value=_METRICS),
        patch("maia.heartbeat.flow.check_audio_api_health", return_value=("healthy", "ok")),
        patch("maia.heartbeat.flow.notifier") as mock_notifier,
    ):
        mock_notifier.send = AsyncMock(return_value=True)
        summary = await heartbeat_flow.fn()

    # Prefect is optional telemetry: it must not make the executor look unhealthy.
    assert summary["healthy"] is True
    assert summary["down"] == []
    # ...but it must not be reported as a clean SUCCESS either.
    assert summary["deployments_down"] == FLEET_DEPLOYMENTS
    assert mock_notifier.send.await_args.kwargs["level"] == AlertLevel.WARNING


@pytest.mark.asyncio
@pytest.mark.parametrize("topic_sync_enabled", [True, False])
async def test_healthy_deployments_report_success(topic_sync_enabled):
    fleet = {nm: ("last run: Completed", "healthy") for nm in FLEET_DEPLOYMENTS}
    with (
        patch(
            "maia.heartbeat.flow.collect_service_status",
            return_value={"pleiades-ingestion": ("active", "healthy")},
        ),
        patch("maia.heartbeat.flow.collect_fleet_status", return_value=fleet),
        patch("maia.heartbeat.flow.collect_pipeline_metrics", return_value=_METRICS),
        patch("maia.heartbeat.flow.check_audio_api_health", return_value=("healthy", "ok")),
        patch("maia.heartbeat.flow.quota_exhausted_agents", return_value=[]),
        patch(
            "maia.heartbeat.flow.get_settings",
            return_value=MagicMock(TOPIC_SYNC_ENABLED=topic_sync_enabled),
        ),
        patch("maia.heartbeat.flow.notifier") as mock_notifier,
    ):
        mock_notifier.send = AsyncMock(return_value=True)
        summary = await heartbeat_flow.fn()

    assert summary["healthy"] is True
    assert summary["deployments_down"] == []
    assert mock_notifier.send.await_args.kwargs["level"] == AlertLevel.SUCCESS
    assert summary["knowledge_graph"] == _GRAPH_METRICS
    assert summary["topic_sync_enabled"] is topic_sync_enabled
    graph = mock_notifier.send.await_args.kwargs["fields"]["Knowledge graph"]
    assert f"Backfill: **{'enabled' if topic_sync_enabled else 'paused'}**" in graph
    assert "Wikipedia topics: **3** · Topic links: **150**" in graph
    assert "Videos checked: **75 / 100 (75.0%)**" in graph
    assert "Channels checked: **10 / 20 (50.0%)**" in graph
    assert "Checked (1h): **9 videos** · **3 channels**" in graph
    assert "No topics: **4** · Unavailable: **2**" in graph
    assert f"<t:{int(_GRAPH_METRICS['latest_observed'].timestamp())}:R>" in graph
    assert len(graph) <= 1024


def test_empty_graph_reports_first_check_and_paused_backfill():
    metrics = {key: 0 for key in _GRAPH_METRICS}
    metrics["latest_observed"] = None
    graph = _knowledge_graph_field(metrics, enabled=False)
    assert "Backfill: **paused**" in graph
    assert "Videos checked: **0 / 0 (0.0%)**" in graph
    assert "Channels checked: **0 / 0 (0.0%)**" in graph
    assert "awaiting first check" in graph


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_collector", ["graph", "pipeline"])
async def test_metrics_failure_preserves_other_reporting(graph_metrics, failed_collector):
    error = RuntimeError("private database details")
    pipeline = AsyncMock(return_value=_METRICS)
    if failed_collector == "graph":
        graph_metrics.side_effect = error
    else:
        pipeline.side_effect = error
    with (
        patch(
            "maia.heartbeat.flow.collect_service_status",
            return_value={"pleiades-ingestion": ("active", "healthy")},
        ),
        patch("maia.heartbeat.flow.collect_pipeline_metrics", pipeline),
        patch("maia.heartbeat.flow.check_audio_api_health", return_value=("healthy", "ok")),
        patch("maia.heartbeat.flow.quota_exhausted_agents", return_value=[]),
        patch("maia.heartbeat.flow.notifier") as notifier,
    ):
        notifier.send = AsyncMock(return_value=True)
        summary = await heartbeat_operation()

    assert summary["notified"] is True
    assert summary["healthy"] is True
    fields = notifier.send.await_args.kwargs["fields"]
    assert "private database details" not in str(fields)
    if failed_collector == "graph":
        assert summary["knowledge_graph"] is None
        assert "Topic metrics unavailable" in fields["Knowledge graph"]
        assert "Videos: **0**" in fields["Content"]
        assert notifier.send.await_args.kwargs["level"] == AlertLevel.INFO
    else:
        assert summary["knowledge_graph"] == _GRAPH_METRICS
        assert "Videos: **?**" in fields["Content"]
        assert "Topic links: **150**" in fields["Knowledge graph"]
