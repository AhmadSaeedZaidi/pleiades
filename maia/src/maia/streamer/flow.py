"""Maia Streamer: YouTube source fetcher (network pull only).

The streamer does the network-heavy, rate-limit-prone YouTube download of a
video's best audio stream and stores it to the vault as a raw artifact, then
flags the video ``fetched``. The singer later extracts speech locally, keeping
the YouTube rate-limit surface confined to this single agent.
"""

import asyncio
import json
import logging
import shutil
import tempfile
import time
from typing import Any

from atlas.models import Video
from atlas.repositories import VideoRepository
from atlas.state import (
    bump_rate_limit_cooldown,
    clear_rate_limit_cooldown,
    get_rate_limit_cooldown_until,
)
from atlas.utils import QuotaExhaustedError
from atlas.vault import get_vault, meta_path, raw_path
from prefect import flow

from maia.base import BaseBatchAgent
from maia.media.streamer import (
    AudioExtractionError,
    StealthVideoStreamer,
    StreamRateLimitError,
)
from maia.storage import commit_artifacts
from maia.utils import cli_bootstrap, notify_quota_exhausted, run_agent_main, vault_op_with_retry

logger = logging.getLogger(__name__)

# Set when a cycle observes a YouTube rate-limit (HTTP 403 / bot-check). The
# streamer's run() reads this after the cycle to decide whether to back off.
_rate_limit_cycle_hit = False


def _note_rate_limit() -> None:
    """Record that the current cycle hit a YouTube rate limit."""
    global _rate_limit_cycle_hit
    _rate_limit_cycle_hit = True


# The YouTube fetch is bandwidth/heavy and rate-limit prone; keep concurrency low
# to avoid HTTP 429 on the (flagged) VPS egress IP.
MAX_CONCURRENT_VIDEOS = 1

# Pacing delay (seconds) between fetches.
STREAMER_THROTTLE_SECONDS = 1.5


async def fetch_streamer_targets_task(batch_size: int) -> list[Video]:
    """Fetch videos whose YouTube source has not yet been fetched."""
    video_repo = VideoRepository()
    targets = await video_repo.claim_streamer_batch(batch_size)
    if targets:
        logger.info(f"Fetched {len(targets)} videos needing a source fetch.")
    return targets  # type: ignore[no-any-return]


async def fetch_source_task(
    video: Video,
) -> tuple[str, str, bytes, bytes | None] | None:
    """Unified YouTube fetch for *video*; return ``(id, raw_uri, raw_bytes, meta_bytes)``.

    Calls the shared streamer's single ingress (``download_unified``) which pulls
    the audio + metadata in ONE YouTube session. Captions are NOT fetched here
    (owned by the Scribe); storage is deferred to the flow level so a batch is
    written in ONE commit. Returns ``None`` on failure.
    """

    video_repo = VideoRepository()
    run_logger = logger
    vid_id = video.id

    # Idempotent: a DONE video is never re-pulled; this guards manual reruns
    # from a redundant YouTube fetch (the claim gate already excludes it).
    if video.raw_phase == "DONE":
        run_logger.info(f"Raw already fetched for {vid_id} — skipping")
        return None

    tmpdir = tempfile.mkdtemp(prefix="streamer-raw-")
    try:
        streamer = StealthVideoStreamer()
        audio_path, info_file = await asyncio.to_thread(streamer.download_unified, vid_id, tmpdir)
        raw_bytes = await asyncio.to_thread(audio_path.read_bytes)
        raw_uri = raw_path(vid_id, audio_path.name)
        run_logger.info(f"Fetched {len(raw_bytes)} bytes of audio for {vid_id}")

        meta_bytes = None
        if info_file is not None:
            meta_bytes = await asyncio.to_thread(info_file.read_bytes)
        else:
            # Self-healing: the info.json was not written during the main download
            # (e.g. YouTube rate-limited the metadata endpoint mid-fetch). Fetch it
            # separately so the painter has stream URLs for frame extraction.
            try:
                info = await asyncio.to_thread(streamer.extract_info, vid_id)
                meta_bytes = json.dumps(info, ensure_ascii=False).encode()
                run_logger.info(f"Fetched metadata separately for {vid_id}")
            except Exception:
                run_logger.warning(
                    f"Could not fetch metadata separately for {vid_id} "
                    f"— painter will fall back to its own fetch",
                    exc_info=True,
                )

        return (vid_id, raw_uri, raw_bytes, meta_bytes)
    except QuotaExhaustedError:
        # Release to PENDING for a later cycle instead of re-raising (which would
        # crash the whole service and crash-loop on the auto-restart).
        await notify_quota_exhausted("streamer")
        await video_repo.release_raw_to_pending(vid_id)
        return None
    except StreamRateLimitError as e:
        # Transient — release back to PENDING so it retries on a later cycle.
        run_logger.warning(f"Rate-limited on {vid_id}, releasing: {e}")
        await video_repo.release_raw_to_pending(vid_id)
        _note_rate_limit()
        return None
    except AudioExtractionError as e:
        # Likely transient (throttle / network) — release to PENDING for retry
        # rather than marking FAILED, so a bad hour doesn't strand the video.
        run_logger.warning(f"Raw fetch failed for {vid_id}, releasing: {e}")
        await video_repo.release_raw_to_pending(vid_id)
        return None
    except Exception as e:
        run_logger.exception(f"Streamer failed on {vid_id}: {e}")
        await video_repo.mark_step_failed(vid_id, "raw")
        return None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


class StreamerAgent(BaseBatchAgent):
    """Streamer Agent: fetch YouTube sources for the singer to extract."""

    name = "streamer"
    default_batch_size = 5
    max_concurrent = MAX_CONCURRENT_VIDEOS
    throttle_seconds = STREAMER_THROTTLE_SECONDS

    async def claim_batch(self, n: int) -> list[Video]:
        return await fetch_streamer_targets_task(n)

    async def process_one(self, video: Video) -> tuple[str, str, bytes, bytes | None] | None:
        return await fetch_source_task(video)

    async def store_results(self, results: list[Any]) -> None:
        fetched = [r for r in results if isinstance(r, tuple) and len(r) == 4]
        if not fetched:
            return
        items: list[tuple[str, bytes]] = []
        vids: list[str] = []
        id_uri: dict[str, str] = {}
        for _id, raw_uri, raw_bytes, meta in fetched:
            items.append((raw_uri, raw_bytes))
            if meta:
                items.append((meta_path(_id), meta))
            vids.append(_id)
            id_uri[_id] = raw_uri

        await commit_artifacts(
            items=items,
            video_ids=vids,
            mark_safe=lambda vid: VideoRepository().mark_fetched(vid, id_uri[vid]),
            on_failure=lambda vid: VideoRepository().mark_step_failed(vid, "raw"),
            label="videos' raw+meta",
            store=vault_op_with_retry,
            vault=get_vault(),
        )

    async def run(self, batch_size: int | None = None, **kwargs: Any) -> dict[str, Any]:
        """Run the plain Streamer operation (without Prefect context)."""
        return await streamer_operation(batch_size=batch_size, **kwargs)


async def streamer_operation(batch_size: int | None = None, **kwargs: Any) -> dict[str, Any]:
    """Run one cycle with rate-limit back-off.

    If a previous cycle hit YouTube rate limits (HTTP 403 / bot-check), we sit
    out this cycle instead of re-claiming and hammering the flagged IP every
    schedule tick. The cooldown grows exponentially (see ``atlas.state``) and
    resets after any clean cycle.
    """
    global _rate_limit_cycle_hit
    _rate_limit_cycle_hit = False

    cooldown_until = get_rate_limit_cooldown_until(StreamerAgent.name)
    if cooldown_until is not None and time.time() < cooldown_until:
        logger.info(
            f"streamer in rate-limit cooldown until {time.ctime(cooldown_until)} "
            f"— backing off to avoid hammering the flagged IP; skipping cycle"
        )
        return {
            "videos_processed": 0,
            "skipped": True,
            "cooldown_until": cooldown_until,
        }

    # Cooldown is operation-level policy, so call the inherited loop directly;
    # going through StreamerAgent.run would re-enter this operation.
    agent = StreamerAgent()
    result = await BaseBatchAgent.run(agent, batch_size=batch_size, **kwargs)

    if _rate_limit_cycle_hit:
        new_cd = bump_rate_limit_cooldown(StreamerAgent.name)
        logger.info(f"Rate limit hit this cycle; backing off until {time.ctime(new_cd)}")
    else:
        clear_rate_limit_cooldown(StreamerAgent.name)

    return result


@flow(name="run_streamer_cycle")
async def streamer_flow(batch_size: int) -> dict[str, Any]:
    """Prefect compatibility adapter for :func:`streamer_operation`."""
    return await streamer_operation(batch_size=batch_size)


def main() -> None:
    run_agent_main(
        lambda: streamer_operation(batch_size=StreamerAgent.default_batch_size),
        "streamer",
    )


if __name__ == "__main__":
    cli_bootstrap()
    main()
