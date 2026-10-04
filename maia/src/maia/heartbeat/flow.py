"""Bounded, passive pipeline heartbeat invoked by the application scheduler."""

import argparse
import asyncio
import logging
import shutil
import subprocess
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from atlas.config import get_settings
from atlas.notifications import AlertChannel, notifier
from atlas.repositories import VideoRepository
from atlas.repositories.heartbeat import HeartbeatRepository
from atlas.repositories.topics import TopicRepository
from atlas.state import quota_exhausted_agents
from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterDeploymentId

from maia.heartbeat.report import _knowledge_graph_field as _knowledge_graph_field
from maia.heartbeat.report import build_report
from maia.telemetry import cycle_monitor
from maia.utils import cli_bootstrap, run_agent_main

logger = logging.getLogger(__name__)

# The executor is the single long-running application scheduler. Prefect
# deployment state is retained as optional telemetry only; it must never decide
# whether ingestion is healthy.
INGESTION_SERVICE = "pleiades-ingestion"
FLEET_UNITS = [INGESTION_SERVICE]

# Legacy Prefect compatibility deployments, separate from the live scheduler.
FLEET_DEPLOYMENTS = [
    "streamer",
    "singer",
    "painter",
    "scribe",
    "hunter",
    "tracker",
    "archeologist",
    "heartbeat",
    "janitor",
]

# Map a Prefect flow-run state name to fleet health.
_RUN_STATE_HEALTH = {
    "Completed": "healthy",
    "Running": "healthy",
    "Pending": "warn",
    "Scheduled": "warn",
    "Paused": "warn",
    "Cancelled": "warn",
    "Failed": "down",
    "Crashed": "down",
}


def _unit_state(unit: str) -> tuple[str, str]:
    """Return ``(label, health)`` for *unit* (best-effort, no sudo).

    ``health`` is ``healthy`` / ``warn`` / ``down``. The polling agents are
    oneshot loops: they run a short cycle, exit 0, then sit in
    ``activating (auto-restart)`` until the next ``RestartSec`` tick — a *healthy*
    steady state. Only a non-zero last exit or a ``failed`` unit is a real problem.
    """
    try:
        result = subprocess.run(
            ["systemctl", "show", unit, "-p", "ActiveState,SubState,ExecMainStatus"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        props = dict(
            line.split("=", 1) for line in result.stdout.strip().splitlines() if "=" in line
        )
        active = props.get("ActiveState", "unknown")
        sub = props.get("SubState", "")
        exit_code = props.get("ExecMainStatus", "0")

        if active == "active":
            return ("active", "healthy")
        if active == "activating" and sub == "auto-restart":
            # Steady state for oneshot-loop agents between RestartSec ticks.
            if exit_code == "0":
                return ("cycling", "healthy")
            return (f"restarting (exit {exit_code})", "warn")
        if active == "activating":
            return ("starting", "warn")
        if active == "failed" or exit_code != "0":
            return (f"failed (exit {exit_code})", "down")
        return (active or "unknown", "down")
    except Exception as e:  # noqa: BLE001 - status probe must never crash the cycle
        logger.warning(f"Could not probe {unit}: {e}")
        return ("unknown", "down")


def collect_service_status() -> dict[str, tuple[str, str]]:
    """Probe every fleet unit and return ``{unit: (label, health)}``."""
    return {unit: _unit_state(unit) for unit in FLEET_UNITS}


async def collect_fleet_status() -> dict[str, tuple[str, str]]:
    """Return ``{deployment: (label, health)}`` from the Prefect API.

    Health is derived from each deployment's most recent flow run. Best-effort:
    if the API is unreachable, every deployment is reported as unreachable so
    the operator can see the control plane is down (rather than the agents).

    Note: the micro's Prefect server caps ``read_flow_runs`` at ``limit=200``
    (it returns ``422 Unprocessable Entity`` above that). Querying each
    deployment's latest run individually (``limit=1``) sidesteps the cap
    entirely and is always correct regardless of run volume.
    """
    out: dict[str, tuple[str, str]] = {}
    try:
        async with get_client() as client:
            deployments = await client.read_deployments(limit=100)
            for d in deployments:
                deployment_name = d.name
                if deployment_name is None:
                    continue
                try:
                    runs = await client.read_flow_runs(
                        flow_run_filter=FlowRunFilter(
                            deployment_id=FlowRunFilterDeploymentId(any_=[d.id])
                        ),
                        limit=1,
                    )
                except Exception:  # noqa: BLE001 - one bad deployment must not sink the rest
                    runs = []
                if runs and runs[0].state:
                    st = runs[0].state.name or "UNKNOWN"
                    out[deployment_name] = (f"last run: {st}", _RUN_STATE_HEALTH.get(st, "warn"))
                else:
                    out[deployment_name] = ("never run", "warn")
            # Surface any known deployment missing from the API response.
            for nm in FLEET_DEPLOYMENTS:
                if nm not in out:
                    out[nm] = ("not registered", "down")
    except Exception as e:  # noqa: BLE001 - fleet probe must never crash the cycle
        logger.warning(f"Could not query Prefect fleet status: {e}")
        out = {nm: ("API unreachable", "down") for nm in FLEET_DEPLOYMENTS}
    return out


async def collect_pipeline_metrics() -> dict[str, Any]:
    """Gather pipeline counts from the database for the status report."""
    return dict[str, Any](await VideoRepository().pipeline_snapshot())


async def collect_knowledge_graph_metrics() -> dict[str, Any]:
    """Read topic coverage without calling YouTube or running enrichment."""
    return await TopicRepository().heartbeat_snapshot()


async def collect_storage_metrics() -> dict[str, Any]:
    return await HeartbeatRepository().storage_snapshot()


def collect_disk_metrics() -> dict[str, int]:
    usage = shutil.disk_usage(Path.cwd())
    return {"total": usage.total, "used": usage.used, "free": usage.free}


async def _bounded_metrics(
    name: str, collector: Callable[[], Awaitable[dict[str, Any]]]
) -> dict[str, Any] | None:
    try:
        async with asyncio.timeout(8):
            return await collector()
    except Exception as error:  # noqa: BLE001 - preserve independent reporting
        logger.warning("Heartbeat %s metrics unavailable (%s)", name, type(error).__name__)
        return None


async def heartbeat_operation(*, include_prefect: bool = False) -> dict[str, Any]:
    """Observe local work and bounded SQL snapshots; send one OPS report."""
    started = time.monotonic()
    services = await asyncio.to_thread(collect_service_status)
    metrics, graph_metrics, storage_metrics = await asyncio.gather(
        _bounded_metrics("extraction", collect_pipeline_metrics),
        _bounded_metrics("graph", collect_knowledge_graph_metrics),
        _bounded_metrics("storage", collect_storage_metrics),
    )
    disk = None
    try:
        disk = await asyncio.to_thread(collect_disk_metrics)
    except OSError as error:
        logger.warning("Disk metrics unavailable (%s)", type(error).__name__)
    fleet = None
    if include_prefect:
        try:
            async with asyncio.timeout(8):
                fleet = await collect_fleet_status()
        except Exception as error:  # noqa: BLE001 - optional compatibility telemetry
            logger.warning("Prefect telemetry unavailable (%s)", type(error).__name__)
            fleet = {name: ("API unreachable", "down") for name in FLEET_DEPLOYMENTS}
    settings = get_settings()
    audio_configuration = (
        "Groq key configured"
        if settings.GROK_API_KEY
        else "Mistral key configured"
        if settings.MISTRAL_API_KEY
        else "No transcription API key configured"
    ) + " (configuration only; no API probe)"
    workers = cycle_monitor.snapshot()
    elapsed = time.monotonic() - started
    report = build_report(
        services=services,
        pipeline=metrics,
        graph=graph_metrics,
        storage=storage_metrics,
        workers=workers,
        disk=disk,
        topic_sync_enabled=settings.TOPIC_SYNC_ENABLED,
        rate_limited=quota_exhausted_agents(),
        audio_configuration=audio_configuration,
        collection_seconds=elapsed,
        fleet=fleet,
    )
    notified = await notifier.send(
        title=f"🛰 Pleiades: {report['status']}",
        description=report["description"],
        channel=AlertChannel.OPS,
        level=report["level"],
        fields=report["fields"],
    )
    summary = {
        "healthy": report["healthy"],
        "executor_online": report["executor_online"],
        "status": report["status"],
        "issues": report["issues"],
        "notified": notified,
        "metrics": metrics,
        "knowledge_graph": graph_metrics,
        "storage": storage_metrics,
        "workers": workers,
        "topic_sync_enabled": settings.TOPIC_SYNC_ENABLED,
        "collection_seconds": elapsed,
    }
    logger.info(
        "Heartbeat sent: %s (notified=%s, collection=%.2fs, issues=%s)",
        report["status"],
        notified,
        elapsed,
        report["issues"],
    )
    return summary


@flow(name="heartbeat_cycle")
async def heartbeat_flow() -> dict[str, Any]:
    """Prefect compatibility adapter for :func:`heartbeat_operation`."""
    return await heartbeat_operation(include_prefect=True)


class HeartbeatAgent:
    name = "heartbeat"

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.name)

    @staticmethod
    def add_cli_args(parser: argparse.ArgumentParser) -> None:
        return None

    async def run(self, **kwargs: Any) -> dict[str, Any]:
        return await heartbeat_operation()


def main() -> None:
    run_agent_main(heartbeat_operation, "heartbeat")


if __name__ == "__main__":
    cli_bootstrap()
    main()
