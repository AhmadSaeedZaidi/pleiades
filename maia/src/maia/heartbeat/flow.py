"""Maia Heartbeat: periodic fleet online-status reporter.

Collects service liveness + pipeline metrics once per cycle, posts a status
embed to Discord, and exits (systemd restarts it on its timer).
"""

import argparse
import asyncio
import logging
import subprocess
from typing import Any

import httpx
from atlas.config import get_settings
from atlas.notifications import AlertChannel, AlertLevel, notifier
from atlas.repositories import VideoRepository
from atlas.repositories.topics import TopicRepository
from atlas.state import quota_exhausted_agents
from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterDeploymentId

from maia.utils import cli_bootstrap, run_agent_main

logger = logging.getLogger(__name__)

# The executor is the single long-running application scheduler. Prefect
# deployment state is retained as optional telemetry only; it must never decide
# whether ingestion is healthy.
INGESTION_SERVICE = "pleiades-ingestion"
FLEET_UNITS = [INGESTION_SERVICE]

# All nine automated deployments (muralist is intentionally excluded — it is a
# manual-only capability with no deployment).
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


async def check_audio_api_health() -> tuple[str, str]:
    """Probe the configured speech-to-text endpoint for liveness.

    Returns ``(status, detail)`` with status ``healthy`` / ``degraded`` / ``down``.
    Issues a tiny probe using the configured key; a 401/403 means reachable but
    bad key, a 2xx/4xx (non-timeout) means reachable, a connection error means
    down. Best-effort: never raises.
    """
    from atlas.config import get_settings

    settings = get_settings()
    if settings.GROK_API_KEY:
        url = "https://api.groq.com/openai/v1/audio/transcriptions"
        headers = {"Authorization": f"Bearer {settings.GROK_API_KEY.get_secret_value()}"}
        provider = "Groq Whisper"
    elif settings.MISTRAL_API_KEY:
        url = "https://api.mistral.ai/v1/audio/transcriptions"
        headers = {"x-api-key": settings.MISTRAL_API_KEY.get_secret_value()}
        provider = "Mistral Voxtral"
    else:
        return ("down", "No transcription API key configured")

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # No audio body — we only want to confirm the endpoint is reachable
            # and the credential is accepted (a 4xx/422 is fine; a 401/403 is a
            # bad key; a connection error is a down endpoint). httpx sets the
            # multipart Content-Type (with boundary) from the files kwarg.
            resp = await client.post(
                url,
                headers=headers,
                files={"file": ("healthcheck.ogg", b"", "audio/ogg")},
            )
        if resp.status_code in (200, 201):
            return ("healthy", f"{provider} reachable")
        # Body may disambiguate a bad credential from a benign validation error.
        body = resp.text or ""
        if resp.status_code in (401, 403) or "incorrect api key" in body.lower():
            return ("degraded", f"{provider} reachable but key rejected (HTTP {resp.status_code})")
        if resp.status_code in (400, 413, 415, 422):
            # Wrong content-type / empty file etc. — endpoint is up.
            return ("healthy", f"{provider} reachable")
        return ("degraded", f"{provider} returned HTTP {resp.status_code}")
    except httpx.TimeoutException:
        return ("degraded", f"{provider} probe timed out")
    except httpx.HTTPError as e:
        return ("down", f"{provider} unreachable: {e}")
    except Exception as e:  # noqa: BLE001 - probe must never crash the cycle
        logger.warning(f"Audio API health probe failed: {e}")
        return ("down", f"Audio API probe error: {e}")


def _build_fields(services: dict[str, tuple[str, str]], metrics: dict[str, Any]) -> dict[str, str]:
    """Build the Discord embed fields from service states and pipeline metrics."""
    icons = {"healthy": "🟢", "warn": "🟡", "down": "🔴"}

    # Split the merged services dict into the executor (systemd unit) and the
    # Prefect deployments so each gets its own embed field.
    executor_line = "\n".join(
        f"{icons.get(health, '🔴')} `{unit}` — {label}"
        for unit, (label, health) in services.items()
        if unit in FLEET_UNITS
    )
    deployments_line = "\n".join(
        f"{icons.get(health, '🔴')} `{unit}` — {label}"
        for unit, (label, health) in services.items()
        if unit not in FLEET_UNITS
    )

    sc = metrics["status_counts"]
    # Terminal stage failures live in the per-step phase columns, not in a
    # row-level status. Report the per-step breakdown so an operator can see
    # which agent is wedging, and flag degraded when any stage is failing.
    fsc = metrics.get("failed_step_counts") or {}
    failed_total = int(metrics.get("failed_steps") or 0)
    failed_breakdown = ", ".join(f"{k}={v}" for k, v in fsc.items() if v) or "none"
    pipeline_line = (
        f"PENDING: **{sc.get('PENDING', 0)}**\n"
        f"PROCESSING: **{sc.get('PROCESSING', 0)}**\n"
        f"PROCESSED: **{sc.get('PROCESSED', 0)}**\n"
        f"ARCHIVED: **{sc.get('ARCHIVED', 0)}**\n"
        f"Stage failures: **{failed_total}** ({failed_breakdown})"
    )

    content_line = (
        f"Videos: **{metrics['total']}**\n"
        f"Transcripts: **{metrics['transcripts']}**\n"
        f"With visuals: **{metrics['with_visuals']}**\n"
        f"Audios extracted: **{metrics['audios']}**\n"
        f"Ingested (1h): **{metrics['ingested_1h']}**"
    )

    phases = metrics.get("phase_counts", {})
    phase_line = (
        f"RAW: **{phases.get('RAW', 0)}** | "
        f"AUDIO: **{phases.get('AUDIO', 0)}** | "
        f"VISUALS: **{phases.get('VISUALS', 0)}** | "
        f"TRANSCRIPT: **{phases.get('TRANSCRIPT', 0)}** | "
        f"CLIP: **{phases.get('CLIP', 0)}** | "
        f"NONE: **{phases.get('NONE', 0)}**"
    )

    tracker_line = (
        f"Ever tracked: **{metrics.get('tracked_ever', 0)}**\n"
        f"Tracked (1h): **{metrics.get('tracked_1h', 0)}**\n"
        f"Tracked (24h): **{metrics.get('tracked_24h', 0)}**\n"
        f"Stats log rows: **{metrics.get('stats_log_size', 0)}**"
    )

    return {
        "Executor": executor_line,
        "Deployments": deployments_line,
        "Pipeline": pipeline_line,
        "Content": content_line,
        "Phases": phase_line,
        "Tracker": tracker_line,
    }


def _knowledge_graph_field(metrics: dict[str, Any] | None, *, enabled: bool) -> str:
    backfill = f"Backfill: **{'enabled' if enabled else 'paused'}**"
    if metrics is None:
        return f"{backfill}\n⚠ Topic metrics unavailable"

    def coverage(kind: str) -> str:
        checked, total = metrics[f"{kind}_checked"], metrics[f"{kind}_total"]
        percent = checked / total * 100 if total else 0.0
        return f"**{checked:,} / {total:,} ({percent:.1f}%)**"

    latest = metrics["latest_observed"]
    last_check = f"<t:{int(latest.timestamp())}:R>" if latest else "awaiting first check"
    links = metrics["video_edges"] + metrics["channel_edges"]
    return (
        f"{backfill}\n"
        f"Wikipedia topics: **{metrics['topic_count']:,}** · Topic links: **{links:,}**\n"
        f"Videos checked: {coverage('video')}\n"
        f"Channels checked: {coverage('channel')}\n"
        f"Checked (1h): **{metrics['video_checked_1h']:,} videos** · "
        f"**{metrics['channel_checked_1h']:,} channels**\n"
        f"No topics: **{metrics['empty']:,}** · Unavailable: **{metrics['unavailable']:,}**\n"
        f"Last check: {last_check}"
    )


async def heartbeat_operation(*, include_prefect: bool = False) -> dict[str, Any]:
    """Collect status, post to Discord, and return a summary dict.

    The live application scheduler must not depend on the Prefect control
    plane. The decorated compatibility adapter opts into deployment-health
    telemetry explicitly; plain callers only probe the executor and pipeline.
    """
    services = await asyncio.to_thread(collect_service_status)
    if include_prefect:
        try:
            fleet = await collect_fleet_status()
        except Exception as e:  # noqa: BLE001 - never let the fleet probe crash the cycle
            logger.warning(f"Fleet status collection failed: {e}")
            fleet = {nm: ("API unreachable", "down") for nm in FLEET_DEPLOYMENTS}
        # Merge executor liveness (systemd) with deployment health (Prefect API).
        services = {**services, **fleet}

    try:
        metrics = await collect_pipeline_metrics()
    except Exception as e:  # noqa: BLE001 - still report service status if DB is down
        logger.exception(f"Failed to collect pipeline metrics: {e}")
        metrics = {
            "total": "?",
            "status_counts": {},
            "transcripts": "?",
            "with_visuals": "?",
            "audios": "?",
            "ingested_1h": "?",
        }

    graph_metrics = None
    try:
        # Bound pool acquisition as well as the repository's SQL execution.
        async with asyncio.timeout(8):
            graph_metrics = await collect_knowledge_graph_metrics()
    except Exception as error:  # noqa: BLE001 - keep the rest of the heartbeat available
        logger.warning("Could not collect topic metrics (%s)", type(error).__name__)
    topic_sync_enabled = get_settings().TOPIC_SYNC_ENABLED

    # Audio transcription API liveness (Grok STT / Mistral Voxtral).
    try:
        audio_status, audio_detail = await check_audio_api_health()
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Audio API health check errored: {e}")
        audio_status, audio_detail = "down", f"error: {e}"

    # Agents currently paused by quota exhaustion (surfaced, not alerted).
    rate_limited = quota_exhausted_agents()

    fields = _build_fields(services, metrics)
    fields["Knowledge graph"] = _knowledge_graph_field(graph_metrics, enabled=topic_sync_enabled)
    audio_icons = {"healthy": "🟢", "degraded": "🟡", "down": "🔴"}
    fields["Audio API"] = f"{audio_icons.get(audio_status, '🔴')} {audio_detail}"
    if rate_limited:
        fields["Quota"] = "⏳ Rate limited / quota exhausted: " + ", ".join(rate_limited)

    # Distinguish the executor worker (systemd) from the Prefect deployments.
    executor_down = [u for u in FLEET_UNITS if services.get(u, ("", "healthy"))[1] == "down"]
    # Deployment health is optional control-plane telemetry. A Prefect outage
    # must not mark the PostgreSQL-backed ingestion scheduler unhealthy, so these
    # never escalate to WARNING on their own — but they must still be surfaced,
    # because silently reporting SUCCESS while every deployment shows "Failed" or
    # "not registered" is a false all-clear.
    deployments_down = [
        d for d in FLEET_DEPLOYMENTS if services.get(d, ("", "healthy"))[1] == "down"
    ]
    deployments_warn = [
        d for d in FLEET_DEPLOYMENTS if services.get(d, ("", "healthy"))[1] == "warn"
    ]
    degraded = [u for u, (_, health) in services.items() if u in FLEET_UNITS and health == "warn"]

    # A degraded audio API is surfaced but does not by itself flip the banner to
    # "Degraded" — only a truly down executor or deployment does. Quota-exhausted
    # agents are shown in the "Quota" field (and their alert is rate-limited).
    if executor_down:
        level = AlertLevel.WARNING
        status_word = "Degraded"
        description = f"⚠ Down: {', '.join(executor_down)}"
    elif deployments_down:
        level = AlertLevel.WARNING
        status_word = "Degraded"
        description = (
            f"⚠ Executor online, but {len(deployments_down)} deployment(s) report down: "
            f"{', '.join(deployments_down)}"
        )
    elif rate_limited:
        level = AlertLevel.INFO
        status_word = "Rate Limited"
        description = (
            "Online — quota exhausted for: "
            + ", ".join(rate_limited)
            + " (transcription/API paused; alerts rate-limited)"
        )
    elif degraded or audio_status != "healthy" or deployments_warn or graph_metrics is None:
        level = AlertLevel.INFO
        status_word = "Online"
        extra = []
        if degraded:
            extra.append(f"{len(degraded)} service(s) degraded: {', '.join(degraded)}")
        if deployments_warn:
            extra.append(
                f"{len(deployments_warn)} deployment(s) not healthy: {', '.join(deployments_warn)}"
            )
        if audio_status != "healthy":
            extra.append(f"audio API {audio_status}")
        if graph_metrics is None:
            extra.append("knowledge graph metrics unavailable")
        description = "Online — " + "; ".join(extra)
    else:
        level = AlertLevel.SUCCESS
        status_word = "Online"
        description = "All fleet services are online. ✅"

    notified = await notifier.send(
        title=f"🛰 Pleiades Fleet Status: {status_word}",
        description=description,
        channel=AlertChannel.OPS,
        level=level,
        fields=fields,
    )

    summary = {
        # "healthy" is about the ingestion fleet (executor + its services), which
        # is what actually runs the pipeline. Deployment telemetry is reported
        # separately so a Prefect outage is visible without being conflated with
        # the scheduler's health.
        "healthy": not executor_down and not degraded,
        "down": executor_down,
        "deployments_down": deployments_down,
        "deployments_warn": deployments_warn,
        "degraded": degraded,
        "notified": notified,
        "metrics": metrics,
        "knowledge_graph": graph_metrics,
        "topic_sync_enabled": topic_sync_enabled,
    }
    logger.info(
        f"Heartbeat sent: {status_word} "
        f"(notified={notified}, executor_down={executor_down}, "
        f"deployments_down={deployments_down}, degraded={degraded})"
    )
    return summary


@flow(name="heartbeat_cycle")
async def heartbeat_flow() -> dict[str, Any]:
    """Prefect compatibility adapter for :func:`heartbeat_operation`."""
    return await heartbeat_operation(include_prefect=True)


class HeartbeatAgent:
    """Heartbeat Agent: posts periodic fleet online-status to Discord."""

    name = "heartbeat"

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.name)

    @staticmethod
    def add_cli_args(parser: argparse.ArgumentParser) -> None:
        # No arguments — a single status snapshot per invocation.
        return None

    async def run(self, **kwargs: Any) -> dict[str, Any]:
        return await heartbeat_operation()


def main() -> None:
    run_agent_main(heartbeat_operation, "heartbeat")


if __name__ == "__main__":
    cli_bootstrap()
    main()
