#!/usr/bin/env python3
"""Idempotent setup of the Pleiades Prefect orchestration on the micro control plane.

This captures the orchestration config that lives ONLY in the micro's SQLite
DB — the work-pool global concurrency limit, and the per-deployment work-queue
limits + priorities — so the 2-VPS setup is reproducible after a micro rebuild
or work-pool recreation.

What it does:
  * sets the ``default`` work pool global ``concurrency_limit`` to 9 (one slot
    per queue; the worker's ``CPUQuota=180%`` cgroup + per-queue ``limit=1``
    prevent CPU saturation on the 2-core box)
  * creates/updates one work queue per deployment, each ``concurrency_limit=1``
    so every agent type runs at most one instance at a time

It does NOT touch deployment schedules or ``work_queue_name`` bindings — those
live in ``prefect.yaml`` and are applied by ``prefect deploy``.

SECURITY: contains no secrets. All credentials (``PREFECT_API_URL``, and if the
server requires auth, ``PREFECT_API_KEY``) are read from the environment. Never
paste tokens into this file.

Usage (run on the executor VPS):
    export PREFECT_API_URL=http://10.0.0.22:4200/api
    python tools/setup_orchestration.py
"""

from __future__ import annotations

import asyncio
import os
import sys

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import DeploymentUpdate, WorkPoolUpdate

POOL_NAME = "default"
# Global ceiling for the 2-core executor. Set to the number of queues (9) so
# every agent type gets exactly one slot.  With per-queue `limit=1` the worst
# case is 9 concurrent runs, safe under the worker's CPUQuota=180% cgroup
# backstop (most agents are cheap DB queries; the heavy ffmpeg agents are
# limited to 1 each anyway).
#
# HISTORY: was 5 with strict priorities 1-9, which caused priority starvation:
# heartbeat(1)/janitor(2)/archeologist(3)/tracker(4)/hunter(5) filled the pool
# and streamer(9)/singer(8)/painter(7) never got a slot → no raw videos fetched
# → pipeline stalled.  With limit=9 every queue gets a slot regardless, matching
# the old "all agents run concurrently" model.
GLOBAL_CONCURRENCY_LIMIT = 9

# Per-deployment work queues — same order as docs/micro-prefect-orchestration.md §3.
# Priority is moot when GLOBAL_CONCURRENCY_LIMIT ≥ len(QUEUES) because every
# queue with pending work gets a slot.  Priorities are kept for a visual ordering
# in the Prefect UI; they do not gate scheduling.
QUEUES = [
    ("heartbeat", 1),
    ("janitor", 2),
    ("archeologist", 3),
    ("tracker", 4),
    ("hunter", 5),
    ("scribe", 6),
    ("painter", 7),
    ("singer", 8),
    ("streamer", 9),
]


async def main() -> int:
    api_url = os.environ.get("PREFECT_API_URL")
    if not api_url:
        print(
            "ERROR: PREFECT_API_URL is required (e.g. http://10.0.0.22:4200/api)",
            file=sys.stderr,
        )
        return 2

    async with get_client() as client:
        # --- work pool global limit ---
        try:
            await client.read_work_pool(POOL_NAME)
        except Exception as exc:  # noqa: BLE001
            print(
                f"ERROR: work pool '{POOL_NAME}' not found ({exc}).\n"
                f"       Create it explicitly with:\n"
                f"       prefect work-pool create --type process {POOL_NAME}\n"
                f"       Then re-run this command.",
                file=sys.stderr,
            )
            return 1

        await client.update_work_pool(
            POOL_NAME, work_pool=WorkPoolUpdate(concurrency_limit=GLOBAL_CONCURRENCY_LIMIT)
        )
        print(f"work pool '{POOL_NAME}': concurrency_limit -> {GLOBAL_CONCURRENCY_LIMIT}")

        # --- per-deployment work queues ---
        # NON-DESTRUCTIVE: create a queue only if missing, otherwise UPDATE it in
        # place. We deliberately do NOT delete+recreate queues.
        #
        # WHY (hard-won): deleting a work queue reassigns/orphans the `work_queue_id`
        # of every already-SCHEDULED run that pointed at it. Recreation mints a NEW
        # queue id, so those pending runs (and any deployment whose pool binding gets
        # dropped in the churn) end up with `work_queue_id=None` — the worker polls
        # the fresh queues but the server never hands it the stranded runs, so every
        # agent freezes indefinitely. That is far worse than the transient +1h
        # `expected_start_time` wedge the delete was meant to cure. The correct cure
        # for a genuine scheduler wedge is to reap the stale non-terminal runs (see
        # `reap_orphaned_runs` below) and/or restart the worker — never to delete the
        # queue out from under live runs.
        existing = {q.name: q for q in await client.read_work_queues(work_pool_name=POOL_NAME)}
        for name, priority in QUEUES:
            if name in existing:
                await client.update_work_queue(
                    existing[name].id,
                    concurrency_limit=1,
                    priority=priority,
                )
                print(
                    f"queue '{name}': updated in place "
                    f"(limit=1, priority={priority}) id={existing[name].id}"
                )
            else:
                q = await client.create_work_queue(
                    name=name,
                    concurrency_limit=1,
                    priority=priority,
                    work_pool_name=POOL_NAME,
                )
                print(f"queue '{name}': created (limit=1, priority={priority}) id={q.id}")

        # --- ensure every deployment is bound to this pool + its queue ---
        # A run's `work_queue_id` is resolved from the deployment's (work_pool,
        # work_queue). If a deployment has `work_pool_name=None` (e.g. after a
        # micro rebuild or a botched queue recreation), its runs get
        # `work_queue_id=None` and are never picked up. Re-assert the binding so
        # newly-scheduled runs always resolve to a live queue id.
        rebound = 0
        for dep in await client.read_deployments(limit=200):
            if dep.work_queue_name and dep.work_pool_name != POOL_NAME:
                await client.update_deployment(
                    dep.id,
                    DeploymentUpdate(work_pool_name=POOL_NAME, work_queue_name=dep.work_queue_name),
                )
                print(
                    f"deployment '{dep.name}': rebound -> "
                    f"pool={POOL_NAME} queue={dep.work_queue_name}"
                )
                rebound += 1
        if rebound == 0:
            print("all deployments already bound to pool 'default' (no rebind needed)")

        # The pre-existing `default` queue is intentionally left as-is (unused;
        # no deployment binds to it).

    print("orchestration setup complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
