"""Maia Scribe: Transcript extraction agent.

Consumer in the Producer-Consumer pipeline. Pulls videos needing
transcripts from the video table, fetches them via yt-dlp native
subtitle extraction, and persists results to Atlas Vault.
"""

import argparse
import asyncio
import logging
import tempfile
from pathlib import Path
from typing import Any

from atlas.models import Video
from atlas.repositories import TranscriptRepository, VideoRepository
from atlas.state import audio_cap_reached, clear_quota_exhausted, record_audio_usage
from atlas.utils import QuotaExhaustedError
from atlas.vault import get_vault
from prefect import flow

from maia.utils import notify_quota_exhausted, vault_op_with_retry

from .loader import (
    TranscriptExtractionError,
    TranscriptLoader,
    TranscriptRateLimitError,
)
from .transcription import (
    transcribe_audio_download,
    transcribe_audio_path,
)

logger = logging.getLogger(__name__)

# Kept low because the VPS egress IP is flagged by YouTube; high concurrency triggers HTTP 429.
MAX_CONCURRENT_TRANSCRIPTS = 1

# Pacing delay (seconds) between transcript fetches to stay under YouTube's per-IP
# rate limits on the `timedtext` (caption) endpoint.
#
# TUNING (2026-07-17): captions are throttled far more aggressively per-IP than
# the audio/video surfaces. At 1.5s the Scribe re-hit the same 1-2 egress IPs
# every couple seconds, keeping them permanently HTTP-429 flagged → 0 real
# transcripts ingested over hours. A controlled probe showed an IP RECOVERS once
# it gets a rest, so we pace much more slowly to let each IP's per-IP allowance
# replenish between requests. Combined with proxy-only egress for the Scribe
# (see the `scribe` deployment env: YOUTUBE_PROXY without the flagged `direct`
# executor IP), this trades raw request rate for a far higher success rate.
SCRIBE_THROTTLE_SECONDS = 20.0

# Scribe will not spend paid STT hours on videos longer than this; instead it
# writes a templated "unavailable" transcript. YouTube captions (free) are still
# used when present, regardless of length.
SCRIBE_MAX_DURATION_SECONDS = 1800

SCRIBE_LONG_VIDEO_MESSAGE = "error: sorry, video too long and auto transcript not available"
SCRIBE_NO_TRANSCRIPT_MESSAGE = "error: no transcript available for this video"


class TranscriptTooLongError(Exception):
    """Raised when a video is too long for the paid STT fallback (no auto transcript)."""


async def fetch_scribe_targets_task(batch_size: int) -> list[Video]:
    """Fetch videos that need transcripts."""
    video_repo = VideoRepository()
    targets = await video_repo.claim_scribe_batch(batch_size)
    if targets:
        logger.info(f"Fetched {len(targets)} videos needing transcripts.")
    return targets  # type: ignore[no-any-return]


async def process_transcript_task(video: Video) -> None:
    """Transcribe a video and stage it locally for the janitor to persist.

    Idempotent on DONE. Releases to PENDING on rate-limit (for retry) and marks
    the video safe when no transcript is available.
    """
    video_repo = VideoRepository()
    transcript_repo = TranscriptRepository()
    run_logger = logger
    vid_id = video.id

    # Idempotent: a DONE video is never re-transcribed, guarding manual reruns
    # from redundant STT/quota use (the claim gate already excludes it).
    if video.transcript_phase == "DONE":
        run_logger.info(f"Transcript already done for {vid_id} — skipping")
        return

    try:
        segments = await _transcribe(video)
        await transcript_repo.record_transcript(
            vid_id,
            vault_uri=None,
            language="en",
            content_json=segments,
        )
        await video_repo.mark_transcript_safe(vid_id)
        run_logger.info(f"Scribed transcript for {vid_id}")

    except QuotaExhaustedError:
        await notify_quota_exhausted("scribe")
        raise
    except TranscriptRateLimitError as e:
        # Transient — release back to PENDING so it retries on a later cycle
        # instead of being permanently marked as having no transcript.
        run_logger.warning(f"Rate-limited on {vid_id}, releasing for retry: {e}")
        await video_repo.release_transcript_to_pending(vid_id)
    except TranscriptTooLongError:
        # Long video with no auto transcript: store a templated notice so the
        # corpus (and the janitor's vault flush) has a record, without spending
        # paid STT hours or raising concerning errors.
        run_logger.info(f"Too long for auto transcript; templated notice for {vid_id}")
        await transcript_repo.record_transcript(
            vid_id,
            vault_uri=None,
            language="en",
            content_json=[{"text": SCRIBE_LONG_VIDEO_MESSAGE}],
        )
        await video_repo.mark_transcript_safe(vid_id)
    except TranscriptExtractionError as e:
        # No transcript is obtainable for this video, but that is a *fact about
        # the video*, not a pipeline failure. Record it the same way the
        # too-long branch does so the corpus has a record and the janitor's vault
        # flush is not left with a has_transcript row that has no content.
        #
        # Previously this called mark_transcript_safe() with no
        # record_transcript(), which set has_transcript=TRUE while no transcript
        # row and no vault artifact existed — a lie to the state machine that also
        # permanently prevented the janitor from ever retrying it.
        run_logger.info(f"No transcript available for {vid_id}: {e}")
        await transcript_repo.record_transcript(
            vid_id,
            vault_uri=None,
            language="en",
            content_json=[{"text": SCRIBE_NO_TRANSCRIPT_MESSAGE}],
        )
        await video_repo.mark_transcript_safe(vid_id)
    except Exception as e:
        run_logger.exception(f"Failed to scribe {vid_id} after retries: {e}")
        await video_repo.mark_step_failed(vid_id, "transcript")


async def _transcribe(video: Video) -> list[dict[str, Any]]:
    """Return transcript segments, preferring YouTube captions over paid STT."""
    vid_id = video.id
    duration = getattr(video, "duration", None)
    too_long = bool(duration and duration > SCRIBE_MAX_DURATION_SECONDS)

    # This is the ONLY place that hits the `timedtext` endpoint, so the throttle
    # surface is centralized here. A rate-limit (TranscriptRateLimitError) is
    # transient and MUST propagate so the flow releases the video back to PENDING
    # for a later retry — it must NOT fall through to paid Grok/Mistral STT. Only
    # a genuine "no captions" failure (TranscriptExtractionError) justifies the
    # paid fallback. Quota exhaustion is a real signal and must propagate too.
    try:
        return TranscriptLoader().fetch(vid_id)
    except (QuotaExhaustedError, TranscriptRateLimitError):
        raise
    except Exception as e:  # noqa: BLE001 - any other failure means no captions
        logger.info(f"No YouTube captions for {vid_id}: {e}")

    # No auto transcript available. Long videos skip the paid STT fallback so we
    # never burn STT budget (or spam errors) on multi-hour broadcasts. The caller
    # writes a templated "unavailable" transcript for these.
    if too_long:
        raise TranscriptTooLongError(
            f"No auto transcript and video too long ({duration}s) for {vid_id}"
        )

    # Audio STT (paid Grok/Mistral fallback), gated by our own daily cap so we
    # never blow the budget (captions above are free and preferred).
    if audio_cap_reached():
        raise TranscriptExtractionError(
            f"Daily audio-transcription cap reached; skipping paid STT for {vid_id}"
        )
    try:
        audio_buf = await vault_op_with_retry(lambda: get_vault().fetch_audio(vid_id))
        if audio_buf is not None:
            # Keep the downloaded vault audio in a private directory whose
            # lifetime covers transcription and cleanup on every exit path.
            with tempfile.TemporaryDirectory(prefix="scribe-stt-") as tmpdir:
                tmp_path = Path(tmpdir) / f"{vid_id}.opus"
                with audio_buf:
                    await asyncio.to_thread(tmp_path.write_bytes, audio_buf.getvalue())
                segs = (await asyncio.to_thread(transcribe_audio_path, tmp_path)).segments
        else:
            segs = (await asyncio.to_thread(transcribe_audio_download, vid_id)).segments
        record_audio_usage(1)
        return segs
    except (TranscriptExtractionError, QuotaExhaustedError):
        raise
    except Exception as e:
        raise TranscriptExtractionError(f"Audio STT unavailable for {vid_id}: {e}") from e


async def scribe_operation(batch_size: int = 10) -> dict[str, Any]:
    """Execute one Scribe cycle as a plain application operation."""
    run_logger = logger
    run_logger.info("=== Starting Scribe Cycle ===")

    targets = await fetch_scribe_targets_task(batch_size)

    if not targets:
        run_logger.info("No videos need transcripts. Scribe cycle complete (idle).")
        return {"videos_processed": 0}

    run_logger.info(f"Processing {len(targets)} videos concurrently...")

    sem = asyncio.Semaphore(MAX_CONCURRENT_TRANSCRIPTS)

    async def _bounded(video: Video) -> None:
        async with sem:
            await process_transcript_task(video)
            await asyncio.sleep(SCRIBE_THROTTLE_SECONDS)

    results = await asyncio.gather(*[_bounded(v) for v in targets], return_exceptions=True)
    # Propagate any QuotaExhaustedError that was caught by return_exceptions
    quota_errors = [r for r in results if isinstance(r, QuotaExhaustedError)]
    if quota_errors:
        raise quota_errors[0]

    # Cycle completed without quota exhaustion — clear any stale marker.
    clear_quota_exhausted("scribe")

    run_logger.info(f"=== Scribe Cycle Complete === Processed {len(targets)} videos")
    return {"videos_processed": len(targets)}


@flow(name="run_scribe_cycle")
async def scribe_flow(batch_size: int) -> dict[str, Any]:
    """Prefect compatibility adapter for :func:`scribe_operation`."""
    return await scribe_operation(batch_size=batch_size)


class ScribeAgent:
    """Scribe Agent: transcript extraction and storage."""

    name = "scribe"

    def __init__(self) -> None:
        """Initialize the Scribe agent."""
        self.logger = logging.getLogger(self.name)

    @staticmethod
    def add_cli_args(parser: argparse.ArgumentParser) -> None:
        """Register command-line arguments for the Scribe agent."""
        parser.add_argument(
            "--batch-size",
            type=int,
            default=10,
            help="Number of videos to process per cycle (default: 10)",
        )

    async def run(self, batch_size: int = 10, **kwargs: Any) -> dict[str, Any]:
        """Execute a complete Scribe cycle and return its statistics dict."""
        result: dict[str, Any] = await scribe_operation(batch_size=batch_size)
        return result


async def run_scribe_cycle(batch_size: int = 10) -> None:
    """
    Legacy plain compatibility entrypoint for backward compatibility.

    Prefer using ScribeAgent directly for new code.
    """
    await scribe_operation(batch_size=batch_size)


def main() -> None:
    """Entry point for running the Scribe as a standalone service."""
    try:
        asyncio.run(scribe_operation())
    except KeyboardInterrupt:
        logger.info("Scribe stopped by user (SIGINT)")
    except Exception as e:
        logger.exception(f"Scribe failed with error: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
