"""Maia Muralist: full-video archival consumer ("super painter").

For every video that still lacks its source clip, it downloads the video at a
compact native resolution (YouTube's own pre-encoded stream — no CPU re-encode)
via the shared :class:`~maia.media.streamer.StealthVideoStreamer` and writes it
to the vault at ``videos/{video_id}.mp4``, then marks ``has_video``.

Status: manual-only — there is no systemd unit, so it is not part of the polling
loop. Archiving full clips is storage-hungry, and the muralist is kept as a
runnable, claim-based capability to switch on once storage allows.
"""

import asyncio
import logging
import shutil
import tempfile
from typing import Any

from atlas.models import Video
from atlas.repositories import VideoRepository
from atlas.utils import QuotaExhaustedError
from atlas.vault import get_vault, video_path
from prefect import flow, get_run_logger, task

from maia.base import BaseBatchAgent
from maia.media.streamer import StealthVideoStreamer, VideoExtractionError
from maia.storage import commit_artifacts
from maia.utils import cli_bootstrap, notify_quota_exhausted, run_agent_main, vault_op_with_retry

logger = logging.getLogger(__name__)

# Default archival resolution: 720p keeps good fidelity using YouTube's
# pre-encoded stream (no local transcode). Drop to 360/480 to hoard more per TB.
DEFAULT_HEIGHT = 720

# Archival is bandwidth/heavy; keep concurrency low to avoid YouTube HTTP 429.
MAX_CONCURRENT_VIDEOS = 2

# Pacing delay (seconds) between video downloads.
MURALIST_THROTTLE_SECONDS = 2.0


@task(name="fetch_muralist_targets")
async def fetch_muralist_targets_task(batch_size: int) -> list[Video]:
    """Fetch videos that still need their source clip archived."""
    video_repo = VideoRepository()
    targets = await video_repo.claim_muralist_batch(batch_size)
    if targets:
        get_run_logger().info(f"Fetched {len(targets)} videos needing video archival.")
    return targets  # type: ignore[no-any-return]


@task(name="process_video")
async def process_video_task(video: Video) -> tuple[str, bytes, str] | None:
    """Download *video*'s source clip and return ``(video_id, bytes, ext)``.

    Storage is deferred to the flow level so an entire batch of clips can be
    written to the vault in ONE commit, keeping us under HuggingFace's
    128-commits/hour cap. Returns ``None`` on failure.

    ``QuotaExhaustedError`` is re-raised so the flow can apply backoff / retry.
    """
    video_repo = VideoRepository()
    run_logger = get_run_logger()
    vid_id = video.id

    # Idempotent: a DONE clip is never re-downloaded; this guards manual reruns
    # from double-writing the full source clip (the claim gate excludes it).
    if video.clip_phase == "DONE":
        run_logger.info(f"Clip already archived for {vid_id} — skipping")
        return None

    tmpdir = tempfile.mkdtemp(prefix="muralist-")
    try:
        streamer = StealthVideoStreamer()
        video_path_local = await asyncio.to_thread(
            streamer.extract_video, vid_id, tmpdir, DEFAULT_HEIGHT
        )
        video_bytes = await asyncio.to_thread(video_path_local.read_bytes)
        run_logger.info(
            f"Downloaded {vid_id} → {len(video_bytes) / 1048576:.1f} MB ({video_path_local.suffix})"
        )
        return (vid_id, video_bytes, video_path_local.suffix.lstrip("."))
    except QuotaExhaustedError:
        await notify_quota_exhausted("muralist")
        raise
    except VideoExtractionError as e:
        # Likely transient (throttle / network) — release to PENDING for retry
        # rather than marking FAILED, so a bad hour doesn't strand the video.
        run_logger.warning(f"Video extraction failed for {vid_id}, releasing: {e}")
        await video_repo.release_clip_to_pending(vid_id)
        return None
    except Exception as e:
        run_logger.exception(f"Muralist failed on {vid_id}: {e}")
        await video_repo.mark_step_failed(vid_id, "clip")
        return None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@flow(name="run_muralist_cycle")
async def muralist_flow(batch_size: int, height: int = DEFAULT_HEIGHT) -> dict[str, Any]:
    """Execute a Muralist cycle: archive source clips to the vault."""
    return await MuralistAgent().run(batch_size=batch_size)


class MuralistAgent(BaseBatchAgent):
    """Muralist Agent: full-video archival ("super painter"). Manual-only."""

    name = "muralist"
    default_batch_size = 5
    max_concurrent = MAX_CONCURRENT_VIDEOS
    throttle_seconds = MURALIST_THROTTLE_SECONDS
    raise_on = (QuotaExhaustedError,)

    async def claim_batch(self, n: int) -> list[Video]:
        return await fetch_muralist_targets_task(n)

    async def process_one(self, video: Video) -> tuple[str, bytes, str] | None:
        return await process_video_task(video)

    async def store_results(self, results: list[Any]) -> None:
        extracted: list[tuple[str, bytes, str]] = [
            r for r in results if isinstance(r, tuple) and len(r) == 3
        ]
        if not extracted:
            return
        items = [(video_path(vid, ext), data) for vid, data, ext in extracted]
        vids = [vid for vid, _data, _ext in extracted]
        await commit_artifacts(
            items=items,
            video_ids=vids,
            mark_safe=VideoRepository().mark_video_safe,
            on_failure=lambda vid: VideoRepository().mark_step_failed(vid, "clip"),
            label="source clips",
            store=vault_op_with_retry,
            vault=get_vault(),
        )


def main() -> None:
    run_agent_main(lambda: muralist_flow(batch_size=MuralistAgent.default_batch_size), "muralist")


if __name__ == "__main__":
    cli_bootstrap()
    main()
