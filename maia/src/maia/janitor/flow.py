"""Maia Janitor: tiered storage state-machine cleanup agent.

Moves data from the hot index (Neon PostgreSQL) to the cold tier (Vault) via a
strict transactional state machine: PENDING → PROCESSING → PROCESSED → ARCHIVED
(and FAILED). On vault failure it logs to EventRepository and leaves the hot DB
untouched.
"""

import argparse
import asyncio
import logging
import os
import time
from typing import Any

from atlas.config import get_settings
from atlas.events import events
from atlas.notifications import AlertChannel, AlertLevel, notifier
from atlas.repositories import TranscriptRepository, VideoRepository
from atlas.storage import VaultColdStore
from atlas.vault import get_vault, transcript_index_path
from prefect import flow

from maia.utils import cli_bootstrap, run_agent_main, vault_op_with_retry
from tiered_storage import TransferPolicy, promote

logger = logging.getLogger(__name__)


DEFAULT_BATCH_SIZE = 50


async def sweep_phase_task(batch_size: int) -> list[dict[str, Any]]:
    """Phase 1: Sweep — find PROCESSED videos eligible for archival."""
    video_repo = VideoRepository()
    run_logger = logger

    videos = await video_repo.sweep_archivable(batch_size=batch_size)
    run_logger.info("Sweep: selected %d PROCESSED videos for archival", len(videos))
    return [v.model_dump() for v in videos]


async def handoff_phase_task(videos_data: list[dict[str, Any]], dry_run: bool) -> dict[str, Any]:
    """Phase 3: Hand-off — serialize each video, vault it, verify, then purge."""
    from atlas.models import Video

    video_repo = VideoRepository()
    run_logger = logger

    videos = [Video(**v) for v in videos_data]

    run_logger.info(f"Hand-off: archiving {len(videos)} videos (dry_run={dry_run})")

    result = await video_repo.archive_video_batch(videos, dry_run=dry_run)

    if not dry_run:
        await events.emit(
            "janitor.batch_complete",
            "janitor",
            {
                "archived": result.get("archived", 0),
                "failed": result.get("failed", 0),
                "failed_ids": result.get("failed_ids", []),
                "dry_run": result.get("dry_run", False),
                "would_archive": result.get("would_archive", 0),
            },
        )

    run_logger.info(
        f"Hand-off: {result.get('archived', 0)} archived, {result.get('failed', 0)} failed"
    )
    return result  # type: ignore[no-any-return]


async def archive_cold_stats_task(retention_days: int = 7, max_batches: int = 8) -> dict[str, int]:
    """Archive stats_log rows older than retention_days from hot tier to vault."""
    if retention_days < 0 or max_batches < 1:
        raise ValueError("Retention must be non-negative and max_batches positive")
    video_repo = VideoRepository()
    run_logger = logger

    run_logger.info(f"Stats archival starting (retention: {retention_days} days)...")

    total_archived = 0
    batch_count = 0

    for _ in range(max_batches):
        try:
            archived = await video_repo.archive_cold_stats(
                retention_days=retention_days, batch_size=5000
            )
            if archived == 0:
                break

            total_archived += archived
            batch_count += 1
            run_logger.info(f"Stats batch {batch_count}: {archived} rows (total: {total_archived})")
            if batch_count < max_batches:
                await asyncio.sleep(1)
        except Exception as e:
            run_logger.exception(f"Stats archival batch failed: {e}")
            raise

    run_logger.info(f"Stats archival complete: {total_archived} rows in {batch_count} batches")
    return {"archived": total_archived, "batches": batch_count}


async def refresh_key_pools_task() -> dict[str, Any]:
    """Recompute the dynamic key-pool allocation from the corpus size.

    Gated to a weekly cadence: ``refresh_allocation`` only rewrites the cache
    when older than ``REFRESH_INTERVAL_DAYS``. Hunter/tracking ring sizes then
    scale with the number of videos in the database.
    """
    from atlas.config import get_settings
    from atlas.key_pool import refresh_allocation

    run_logger = logger
    settings = get_settings()
    repo = VideoRepository()

    video_count = await repo.count_videos()
    total_keys = len(settings.api_keys)

    sizes = await asyncio.to_thread(
        refresh_allocation,
        total_keys,
        video_count,
        settings.KEY_POOL_ARCHEOLOGY_SIZE,
    )

    if sizes is None:
        run_logger.info("Key-pool allocation still fresh — no change")
        return {"refreshed": False, "video_count": video_count}

    run_logger.info(
        f"Key-pool allocation updated: tracking={sizes.tracking}, "
        f"archeology={sizes.archeology}, video_count={video_count}"
    )
    return {
        "refreshed": True,
        "video_count": video_count,
        "tracking": sizes.tracking,
        "archeology": sizes.archeology,
    }


async def cull_search_queue_task() -> dict[str, Any]:
    """Delete search terms whose time-decayed score has dropped below the cull
    threshold (Phase 2). In-progress paginations are protected."""
    from atlas.repositories import SearchQueueRepository

    run_logger = logger
    deleted = await SearchQueueRepository().cull_stale()
    if deleted:
        run_logger.info(f"Search queue: culled {deleted} stale term(s)")
    return {"culled": deleted}


async def purge_prefect_runs_task(
    age_days: int = 7,
    batch_limit: int = 50,
    max_batches: int = 20,
) -> dict[str, Any]:
    """Delete flow runs (and cascaded task runs + logs) older than *age_days*.

    Calls Prefect's ``/flow_runs/bulk_delete`` in batches (max 50 per call).
    Single-ownership assumption: the Janitor is the *only* agent that purges
    orchestration history, so there is no TOCTOU concern.

    Returns the total number of flow runs deleted.
    """
    from datetime import UTC, datetime, timedelta

    import httpx

    run_logger = logger
    cutoff = (datetime.now(UTC) - timedelta(days=age_days)).isoformat()
    total_deleted = 0
    async with httpx.AsyncClient(
        base_url=os.environ.get("PREFECT_API_URL", "http://10.0.0.22:4200/api"),
        headers={"Content-Type": "application/json"},
        timeout=httpx.Timeout(30.0),
    ) as client:
        for batch_idx in range(max_batches):
            resp = await client.post(
                "/flow_runs/bulk_delete",
                json={
                    "flow_runs": {
                        "end_time": {"before_": cutoff},
                    },
                    "limit": batch_limit,
                },
            )
            if resp.status_code != 200:
                run_logger.warning(
                    f"Purge batch {batch_idx}: HTTP {resp.status_code} — {resp.text[:200]}"
                )
                break
            body = resp.json()
            deleted = body.get("deleted", [])
            if not deleted:
                break
            total_deleted += len(deleted)

    if total_deleted:
        run_logger.info(f"Purged {total_deleted} completed flow runs older than {age_days}d")
    return {"purged_flow_runs": total_deleted}


async def reap_zombie_runs_task(max_age_minutes: int = 15) -> dict[str, Any]:
    """Reap stale Prefect runs as legacy rollback hygiene.

    This is not part of the live PostgreSQL-backed scheduler. The plain
    ``janitor_operation`` leaves it disabled; only the Prefect janitor
    compatibility adapter opts in. It addresses orphaned Prefect runs after a
    worker or control-plane failure, but forcing a run to CRASHED changes
    orchestration state. The default 15-minute threshold is therefore a
    rollback heuristic, not a universal safety guarantee: validate and tune
    ``max_age_minutes`` against the actual rollback workload before enabling
    this operation. Runs older than the selected threshold are force-set to
    CRASHED to release their legacy Prefect queue slot.
    """
    from datetime import UTC, datetime

    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import (
        FlowRunFilter,
        FlowRunFilterState,
        FlowRunFilterStateType,
    )
    from prefect.client.schemas.objects import StateType
    from prefect.states import Crashed

    run_logger = logger
    reaped = 0
    async with get_client() as client:
        running = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(type=FlowRunFilterStateType(any_=[StateType.RUNNING]))
            ),
            limit=100,
        )
        now = datetime.now(UTC)
        for r in running:
            started = r.start_time or now
            age_min = (now - started).total_seconds() / 60
            if age_min > max_age_minutes:
                await client.set_flow_run_state(
                    r.id,
                    Crashed(
                        message=(
                            f"Reaped by janitor: RUNNING for {age_min:.0f}min "
                            f"(> {max_age_minutes}min) — process presumed dead, "
                            f"freeing the '{r.work_queue_name}' queue slot."
                        )
                    ),
                    force=True,
                )
                run_logger.warning(
                    f"Reaped zombie run '{r.name}' (queue={r.work_queue_name}, "
                    f"age={age_min:.0f}min)"
                )
                reaped += 1
    if reaped:
        run_logger.warning(f"Janitor reaped {reaped} zombie run(s), freeing queue slots")
    return {"reaped": reaped}


async def vault_flush_task(batch_size: int = 50) -> dict[str, Any]:
    """Write/verify a bounded batch, then conditionally release staged payloads."""
    policy = TransferPolicy(batch_size=batch_size)
    transcript_repo = TranscriptRepository()
    pending = await transcript_repo.pending(batch_size)
    if not pending:
        return {"flushed": 0, "failed": 0}
    cold = VaultColdStore(
        get_vault(),
        heads={item.key: transcript_index_path(item.key) for item in pending},
        legacy_json_uris={item.existing_uri for item in pending if item.existing_uri},
        run=vault_op_with_retry,
    )
    result = await promote(pending, cold, transcript_repo.finalize, policy)
    for failure in result.failed:
        logger.warning(
            "Vault flush %s failed at %s (%s)", failure.key, failure.stage, failure.error
        )
    logger.info(
        "Vault flush: %d flushed, %d failed, %d changed during handoff",
        len(result.promoted),
        len(result.failed),
        len(result.deferred),
    )
    # Keep the existing operation result shape; version changes are retryable.
    return {"flushed": len(result.promoted), "failed": len(result.failed) + len(result.deferred)}


async def flush_transcripts_task() -> dict[str, int]:
    """Drain several small verified batches with a count and elapsed-time budget."""
    settings = get_settings()
    deadline = time.monotonic() + settings.JANITOR_TRANSCRIPT_BUDGET_SECONDS
    totals = {"flushed": 0, "failed": 0, "batches": 0}
    for _ in range(settings.JANITOR_TRANSCRIPT_MAX_BATCHES):
        if time.monotonic() >= deadline:
            break
        result = await vault_flush_task(settings.JANITOR_TRANSCRIPT_BATCH_SIZE)
        totals["batches"] += 1
        totals["flushed"] += result.get("flushed", 0)
        totals["failed"] += result.get("failed", 0)
        # Avoid repeatedly selecting a failing record within the same cycle.
        if result.get("failed") or not result.get("flushed"):
            break
    return totals


async def log_summary_task(results: dict[str, Any]) -> None:
    """Emit final summary event for the janitor cycle."""
    run_logger = logger
    run_logger.info("=" * 60)
    run_logger.info("JANITOR CYCLE SUMMARY")
    run_logger.info(f"  Stats archived:       {results.get('stats_archived', 0)}")
    run_logger.info(f"  Videos archived:      {results.get('videos_archived', 0)}")
    run_logger.info(f"  Videos failed:        {results.get('videos_failed', 0)}")
    run_logger.info(f"  Prefect runs purged:  {results.get('purged', 0)}")
    run_logger.info(f"  Dry run:              {results.get('dry_run', False)}")
    run_logger.info("=" * 60)

    if not results.get("dry_run"):
        await events.emit(
            "janitor.cycle_complete",
            "janitor",
            {
                "stats_archived": results.get("stats_archived", 0),
                "videos_archived": results.get("videos_archived", 0),
                "videos_failed": results.get("videos_failed", 0),
                "dry_run": results.get("dry_run", False),
            },
        )


async def janitor_operation(
    dry_run: bool = False,
    archive_stats: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    prefect_hygiene: bool = False,
) -> dict[str, Any]:
    """Execute the Janitor cleanup cycle — a strict transactional state machine.

    Phases: stats archival (optional), sweep for PROCESSED videos, hand-off
    (serialize → vault → verify → mark ARCHIVED → purge), and a summary event.

    Args:
        dry_run: Log what would happen without making changes.
        archive_stats: Whether to run stats archival phase.
        batch_size: Number of videos to process per hand-off batch.

    Returns a dict with stats_archived, videos_archived, videos_failed, dry_run.
    """
    run_logger = logger
    run_logger.info("=" * 60)
    run_logger.info("JANITOR STATE MACHINE CYCLE STARTING")
    run_logger.info(f"  dry_run={dry_run}, archive_stats={archive_stats}")
    run_logger.info("=" * 60)

    results: dict[str, Any] = {
        "stats_archived": 0,
        "videos_archived": 0,
        "videos_failed": 0,
        "dry_run": dry_run,
    }

    prefect_hygiene = prefect_hygiene and not dry_run
    if prefect_hygiene:
        try:
            reap_result = await reap_zombie_runs_task()
            results["zombie_runs_reaped"] = reap_result.get("reaped", 0)
        except Exception as e:
            run_logger.exception(f"Phase 0 (zombie-run reap) failed: {e}")
            results["zombie_reap_error"] = str(e)
    else:
        results["zombie_runs_reaped"] = 0
        run_logger.info("Phase 0: Prefect zombie cleanup skipped (optional telemetry only)")

    if dry_run:
        results.update(vault_flushed=0, vault_failed=0)
        run_logger.info("Dry run: key refresh, queue cull and vault flush skipped")
    else:
        try:
            pool_result = await refresh_key_pools_task()
            results["key_pool"] = pool_result
        except Exception as e:
            run_logger.exception(f"Phase 0 (key-pool refresh) failed: {e}")
            results["key_pool_error"] = str(e)

        try:
            cull_result = await cull_search_queue_task()
            results["search_queue_culled"] = cull_result.get("culled", 0)
        except Exception as e:
            run_logger.exception(f"Phase 0b (search-queue cull) failed: {e}")
            results["search_queue_cull_error"] = str(e)

        try:
            flush_result = await flush_transcripts_task()
            results["vault_flushed"] = flush_result.get("flushed", 0)
            results["vault_failed"] = flush_result.get("failed", 0)
        except Exception as e:
            run_logger.exception(f"Phase 0c (vault flush) failed: {e}")
            results["vault_flush_error"] = str(e)

    if prefect_hygiene:
        try:
            purge_result = await purge_prefect_runs_task(age_days=7)
            results["purged"] = purge_result.get("purged_flow_runs", 0)
        except Exception as e:
            run_logger.exception(f"Phase 0d (prefect purge) failed: {e}")
            results["purge_error"] = str(e)
    else:
        results["purged"] = 0
        run_logger.info("Phase 0d: Prefect run purge skipped (optional telemetry only)")

    if archive_stats and not dry_run:
        run_logger.info("Phase 1/3: Archiving cold stats...")
        try:
            stats_result = await archive_cold_stats_task(
                retention_days=get_settings().JANITOR_METRICS_RETENTION_DAYS
            )
            results["stats_archived"] = stats_result["archived"]
        except Exception as e:
            run_logger.exception(f"Phase 1 (stats) failed: {e}")
            results["stats_error"] = str(e)
    else:
        run_logger.info(f"Phase 1/3: Skipped (archive_stats={archive_stats}, dry_run={dry_run})")

    run_logger.info("Phase 2/3: Sweeping for PROCESSED videos...")
    # Sweep a single page per cycle. sweep_archivable() has no cursor/offset and
    # only marks rows ARCHIVED in the hand-off phase below, so re-calling it in a
    # loop returns the *same* rows forever (infinite loop once the backlog is
    # >= batch_size). The janitor runs periodically, so any remaining backlog is
    # drained by subsequent cycles as rows transition PROCESSED -> ARCHIVED.
    all_archivable: list[dict[str, Any]] = await sweep_phase_task(batch_size)

    run_logger.info(f"Phase 2/3: Found {len(all_archivable)} videos to archive")

    if not all_archivable:
        run_logger.info("Phase 3/3: No videos to archive — cycle complete")
        await log_summary_task(results)
        return results

    run_logger.info(f"Phase 3/3: Archiving {len(all_archivable)} videos...")

    # The shared engine owns byte/record chunking; submit the selected page once.
    handoff_result = await handoff_phase_task(all_archivable, dry_run)
    results["videos_archived"] = handoff_result.get("would_archive" if dry_run else "archived", 0)
    results["videos_failed"] = handoff_result.get("failed", 0)

    await log_summary_task(results)

    if dry_run:
        return results

    # Alert level must reflect every failure mode, not just archive failures.
    # vault_failed counts transcript flush failures and stats_error means cold
    # stats archival failed outright; both used to be invisible here, so a
    # janitor that could not write to the vault at all still reported INFO.
    degraded_reasons: list[str] = []
    if results.get("videos_failed", 0):
        degraded_reasons.append(f"{results['videos_failed']} video(s) failed to archive")
    if results.get("vault_failed", 0):
        degraded_reasons.append(f"{results['vault_failed']} transcript flush(es) failed")
    if results.get("stats_error"):
        degraded_reasons.append(f"cold stats archival failed: {results['stats_error']}")

    await notifier.send(
        title="🧹 Pleiades Janitor Cycle Complete",
        description=(
            f"Archived: {results.get('videos_archived', 0)} videos | "
            f"Stats: {results.get('stats_archived', 0)} rows | "
            f"Vault flushed: {results.get('vault_flushed', 0)} | "
            f"Vault failed: {results.get('vault_failed', 0)} | "
            f"Prefect runs purged: {results.get('purged', 0)}"
            + (
                f" | ⚠ {len(degraded_reasons)} issue(s): {'; '.join(degraded_reasons)}"
                if degraded_reasons
                else ""
            )
        ),
        channel=AlertChannel.ALERTS,
        level=AlertLevel.WARNING if degraded_reasons else AlertLevel.INFO,
        fields={
            "Videos Archived": str(results.get("videos_archived", 0)),
            "Videos Failed": str(results.get("videos_failed", 0)),
            "Stats Archived (rows)": str(results.get("stats_archived", 0)),
            "Stats Archival Error": str(results.get("stats_error") or "none"),
            "Vault Flushed": str(results.get("vault_flushed", 0)),
            "Vault Failed": str(results.get("vault_failed", 0)),
            "Search Queue Culled": str(results.get("search_queue_culled", 0)),
            "Zombie Runs Reaped": str(results.get("zombie_runs_reaped", 0)),
            "Prefect Runs Purged": str(results.get("purged", 0)),
            "Dry Run": str(results.get("dry_run", False)),
        },
    )

    return results


class JanitorAgent:
    """Janitor Agent: tiered storage state machine."""

    name = "janitor"

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.name)

    @staticmethod
    def add_cli_args(parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=True,
            help="Run in dry-run mode (default: true)",
        )
        parser.add_argument(
            "--no-dry-run",
            dest="dry_run",
            action="store_false",
            help="Disable dry-run mode (perform actual archival)",
        )
        parser.add_argument(
            "--archive-stats",
            action="store_true",
            default=True,
            help="Archive old stats to cold tier (default: true)",
        )
        parser.add_argument(
            "--no-archive-stats",
            dest="archive_stats",
            action="store_false",
            help="Skip stats archival",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=DEFAULT_BATCH_SIZE,
            help=f"Videos per archival batch (default: {DEFAULT_BATCH_SIZE})",
        )

    async def run(
        self,
        dry_run: bool = False,
        archive_stats: bool = True,
        batch_size: int = DEFAULT_BATCH_SIZE,
        **kwargs: Any,
    ) -> dict[str, Any]:
        result: dict[str, Any] = await janitor_operation(
            dry_run=dry_run, archive_stats=archive_stats, batch_size=batch_size
        )
        return result


@flow(name="janitor_cycle")
async def janitor_flow(
    dry_run: bool = False,
    archive_stats: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, Any]:
    """Prefect compatibility adapter for :func:`janitor_operation`."""
    return await janitor_operation(
        dry_run=dry_run,
        archive_stats=archive_stats,
        batch_size=batch_size,
        prefect_hygiene=True,
    )


async def janitor_cycle(
    dry_run: bool = False, archive_stats: bool = True, batch_size: int = DEFAULT_BATCH_SIZE
) -> dict[str, Any]:
    """Legacy plain compatibility entrypoint; prefer :func:`janitor_operation`."""
    return await janitor_operation(
        dry_run=dry_run,
        archive_stats=archive_stats,
        batch_size=batch_size,
    )


def main() -> None:
    run_agent_main(lambda: janitor_operation(dry_run=True), "janitor")


if __name__ == "__main__":
    cli_bootstrap()
    main()
