"""Pure, bounded formatting of observed pipeline health."""

from typing import Any

from atlas.notifications import AlertLevel


def _bytes(value: int) -> str:
    return f"{value / 2**30:.2f} GiB" if value >= 2**30 else f"{value / 2**20:.1f} MiB"


def _age(seconds: float) -> str:
    return f"{seconds / 60:.0f}m" if seconds >= 60 else f"{seconds:.0f}s"


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


def build_report(
    *,
    services: dict[str, tuple[str, str]],
    pipeline: dict[str, Any] | None,
    graph: dict[str, Any] | None,
    storage: dict[str, Any] | None,
    workers: dict[str, Any] | None,
    disk: dict[str, int] | None,
    topic_sync_enabled: bool,
    rate_limited: list[str],
    audio_configuration: str,
    collection_seconds: float,
    fleet: dict[str, tuple[str, str]] | None = None,
) -> dict[str, Any]:
    issues: list[str] = []
    executor_online = bool(services) and all(health == "healthy" for _, health in services.values())
    executor_down = not services or any(health == "down" for _, health in services.values())
    if not executor_online:
        issues.append("executor unavailable or restarting")
    fields = {
        "Executor": "\n".join(f"{name}: **{label}**" for name, (label, _) in services.items())
        or "Unavailable"
    }
    progress: dict[str, int] = {}
    if workers is None:
        fields["Workers"] = "Scheduler observations unavailable in this process"
        issues.append("scheduler observations unavailable")
    else:
        lines = []
        for row in workers["cycles"]:
            state = row["state"]
            icon = "⚠" if row["failures"] or state in {"late", "slow", "interrupted"} else "·"
            detail = f" ({row['error']}, {row['failures']} consecutive)" if row["error"] else ""
            lines.append(f"{icon} {row['name']}: {state} {_age(row['elapsed_seconds'])}{detail}")
            if icon == "⚠":
                issues.append(f"{row['name']} {state}")
            for key, count in row["progress_1h"].items():
                progress[key] = progress.get(key, 0) + count
        fields["Workers"] = "\n".join(lines)
        period = (
            "1h"
            if workers["uptime_seconds"] >= 3600
            else f"since start ({_age(workers['uptime_seconds'])})"
        )
        fields["Executor"] += f"\nObserved progress: {period}"
    if pipeline is None:
        fields["Extraction"] = "Metrics unavailable"
        issues.append("extraction metrics unavailable")
    else:
        sc = pipeline["status_counts"]
        failed = int(pipeline.get("failed_steps", 0))
        legacy = int(sc.get("FAILED", 0))
        breakdown = (
            ", ".join(
                f"{key}={value:,}"
                for key, value in pipeline.get("failed_step_counts", {}).items()
                if value
            )
            or "none"
        )
        fields["Extraction"] = (
            f"Videos: **{pipeline['total']:,}** · New (1h): **{pipeline['ingested_1h']:,}**\n"
            f"Pending: {sc.get('PENDING', 0):,} · Processing: {sc.get('PROCESSING', 0):,}\n"
            f"Processed: {sc.get('PROCESSED', 0):,} · Archived: {sc.get('ARCHIVED', 0):,}\n"
            f"Stage failures: **{failed:,}** ({breakdown})\nLegacy failed records: **{legacy:,}**"
        )
        if failed or legacy:
            issues.append(f"{failed:,} stage failures; {legacy:,} legacy failed records")
        fields["Tracking"] = (
            f"Updated (1h): **{pipeline.get('tracked_1h', 0):,}** · "
            f"(24h): **{pipeline.get('tracked_24h', 0):,}**\n"
            f"Stats in SQL: **{pipeline.get('stats_log_size', 0):,}**"
        )
    if storage is None:
        fields["Hot / cold storage"] = "Metrics unavailable"
        issues.append("storage metrics unavailable")
    else:
        fields["Hot / cold storage"] = (
            f"SQL: **{_bytes(storage['database_bytes'])}** · "
            f"Hot bodies: **{storage['hot_bodies']:,}**\n"
            f"Staged: {storage['staged']:,} · Retained with cold pointer: {storage['retained']:,}\n"
            f"Hot payload footprint: {_bytes(storage['payload_bytes'])} "
            "(excludes table/index overhead)"
        )
        fields.setdefault("Tracking", "Metrics unavailable")
        fields["Tracking"] += f"\nDue now: **{storage['tracking_due']:,}**"
    if workers is not None:
        fields["Hot / cold storage"] += (
            f"\nObserved: **{progress.get('vault_flushed', 0):,}** verified handoffs · "
            f"**{progress.get('stats_archived', 0):,}** stats archived"
        )
    if disk is None:
        fields["Host disk"] = "Metrics unavailable"
        issues.append("disk metrics unavailable")
        disk_percent = 0.0
    else:
        # Match df's available-space denominator, excluding reserved blocks.
        disk_percent = disk["used"] / (disk["used"] + disk["free"]) * 100
        fields["Host disk"] = f"**{disk_percent:.1f}% used** · **{_bytes(disk['free'])} free**"
        if disk_percent >= 85:
            issues.append(f"disk {disk_percent:.1f}% used")
    fields["Knowledge graph"] = _knowledge_graph_field(graph, enabled=topic_sync_enabled)
    if graph is None:
        issues.append("graph metrics unavailable")
    fields["API configuration"] = (
        audio_configuration
        + "\n"
        + (
            "Quota paused: " + ", ".join(rate_limited)
            if rate_limited
            else "No recorded quota pauses"
        )
    )
    if rate_limited:
        issues.append("quota pauses: " + ", ".join(rate_limited))
    if fleet is not None:
        fields["Optional Prefect"] = "\n".join(
            f"{name}: {label}" for name, (label, _) in fleet.items()
        )
    fields["Host disk"] += f"\nCollection: {collection_seconds:.2f}s · Cadence: 15m"
    critical = executor_down or disk_percent >= 95
    status = "Degraded" if critical else "Attention" if issues else "Healthy"
    return {
        "status": status,
        "healthy": not issues,
        "executor_online": executor_online,
        "issues": issues,
        "level": AlertLevel.CRITICAL
        if critical
        else AlertLevel.WARNING
        if issues
        else AlertLevel.SUCCESS,
        "description": (
            "; ".join(issues) or "Executor online; observed cycles and metrics healthy"
        )[:700],
        # Nine fields at <=500 chars leave room for title, description and footer
        # under Discord's aggregate 6,000-character limit.
        "fields": {
            key: value[:499] + "…" if len(value) > 500 else value for key, value in fields.items()
        },
    }
