"""Maia Painter: Video keyframe extraction agent.

Consumer in the Producer-Consumer pipeline. Pulls videos needing
visual processing from the video table, extracts keyframes via FFmpeg,
and persists them to Atlas Vault.
"""

import asyncio
import contextlib
import json
import logging
import os
import random
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
from atlas.models import Video
from atlas.repositories import VideoRepository
from atlas.utils import QuotaExhaustedError
from atlas.vault import get_vault, legacy_meta_path, meta_path
from prefect import flow

from maia.base import BaseBatchAgent
from maia.painter.streamer import StealthVideoStreamer, StreamRateLimitError
from maia.utils import cli_bootstrap, notify_quota_exhausted, run_agent_main, vault_op_with_retry

logger = logging.getLogger(__name__)

# The VPS egress IP is flagged by YouTube, so keep concurrency low to avoid HTTP 429.
MAX_CONCURRENT_VIDEOS = 1

# ── Frame-extraction policy ───────────────────────────────────────────────────
FRAME_INTERVAL_SECONDS = 15.0
MIN_FRAMES = 4
MAX_FRAMES = 60
HEATMAP_PEAKS = 8
FRAME_HEIGHT = 720
# WebP is ~50% smaller than JPEG at comparable quality and is universally
# supported by ML/vision tooling.
FRAME_FORMAT = "webp"
FRAME_WEBP_QUALITY = 80  # libwebp -quality (0–100)
FRAME_JPEG_QUALITY = 3  # ffmpeg -q:v fallback when FRAME_FORMAT="jpg" (≈ q90)
# Prefer a source stream at/just above the target height and an efficient codec
# (AV1/VP9) to minimise download; keyframes are analysed at native resolution.
STREAM_TARGET_HEIGHT = 720


def plan_timestamps(
    duration: float,
    chapter_starts: list[float] | None = None,
    heatmap_peaks: list[float] | None = None,
) -> list[float]:
    """Plan the set of timestamps (seconds) to sample for a video.

    Combines a uniform grid with chapter starts and heatmap peaks, de-duplicates,
    then clamps the count to ``[MIN_FRAMES, MAX_FRAMES]`` (evenly downsampling
    when over, back-filling uniformly when under).
    """
    if duration <= 0:
        return []

    last = max(0.0, duration - 1.0)
    timestamps: set[float] = set()

    # Uniform grid every FRAME_INTERVAL_SECONDS.
    n_grid = int(duration // FRAME_INTERVAL_SECONDS)
    for i in range(n_grid + 1):
        timestamps.add(min(i * FRAME_INTERVAL_SECONDS, last))

    # Salient points: chapters + heatmap peaks (rounded to reduce near-dupes).
    for pts in (chapter_starts or [], heatmap_peaks or []):
        for t in pts:
            if 0.0 <= t <= last:
                timestamps.add(round(float(t), 1))

    ordered = sorted(timestamps)

    # Back-fill to the minimum with an even spread.
    if len(ordered) < MIN_FRAMES:
        ordered = sorted(set(np.linspace(0.0, last, MIN_FRAMES).tolist()))

    # Downsample evenly to the maximum.
    if len(ordered) > MAX_FRAMES:
        idx = sorted(set(np.linspace(0, len(ordered) - 1, MAX_FRAMES).round().astype(int)))
        ordered = [ordered[i] for i in idx]

    return ordered


def select_stream_url(
    info: dict[str, Any], target_height: int = STREAM_TARGET_HEIGHT
) -> str | None:
    """Pick a video stream URL for frame extraction.

    Frames are keyframe stills, not full video, so we keep the source light:
    prefer **efficient codecs (AV1/VP9)** at **≤ *target_height***. Falls back to
    the tallest available video-only format, then to the default url.
    """
    video_formats = [
        f
        for f in info.get("formats", [])
        if f.get("url") and f.get("height") and f.get("vcodec", "none") != "none"
    ]
    if not video_formats:
        default_url: str | None = info.get("url")
        return default_url

    # Prefer low-res (<= target) to keep the frame source bandwidth-light; among
    # those, prefer efficient codecs, then the tallest (best quality) within cap.
    le = [f for f in video_formats if f["height"] <= target_height]
    pool = le or video_formats

    def _score(f: dict[str, Any]) -> tuple[int, int]:
        codecs = (f.get("vcodec") or "").lower()
        efficient = 0 if ("av1" in codecs or "vp9" in codecs) else 1
        return (efficient, -f["height"])

    return str(min(pool, key=_score)["url"])


async def fetch_painter_targets_task(batch_size: int) -> list[Video]:
    """Fetch videos that need visual processing."""
    video_repo = VideoRepository()
    return list[Video](await video_repo.claim_painter_batch(batch_size))


def _is_valid_image(data: bytes, ext: str) -> bool:
    """Reject truncated/garbage frames before they reach the vault.

    WebP is a RIFF container (``RIFF<size>WEBP``); JPEG starts with the SOI
    marker ``FF D8 FF``. A frame that fails this check would otherwise be
    stored as a corrupt file in the vault.
    """
    if not data or len(data) < 32:
        return False
    if ext == "webp":
        return data[:4] == b"RIFF" and data[8:12] == b"WEBP"
    return data[:3] == b"\xff\xd8\xff"


def _ffmpeg_extract_frame(stream_url: str, timestamp: float) -> bytes | None:
    """Grab a single frame via FFmpeg (faster than OpenCV for remote streams).

    Encodes to a temp file, never piped to stdout: the WebP muxer needs a
    seekable output, so piping WebP to ``-`` corrupts the RIFF container
    (which was producing invalid vault images); JPEG pipes unreliably too.
    Returns encoded image bytes or None on failure.
    """
    fd, out_path = tempfile.mkstemp(suffix=f".{FRAME_FORMAT}")
    os.close(fd)
    try:
        # Downscale to the target height (never upscale), preserving aspect.
        scale = f"scale=-2:'min(ih,{FRAME_HEIGHT})'"
        if FRAME_FORMAT == "webp":
            codec_args = ["-c:v", "libwebp", "-quality", str(FRAME_WEBP_QUALITY), "-f", "webp"]
        else:
            codec_args = ["-c:v", "mjpeg", "-q:v", str(FRAME_JPEG_QUALITY), "-f", "image2"]

        cmd = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            str(timestamp),
            "-i",
            stream_url,
            "-frames:v",
            "1",
            "-vf",
            scale,
            *codec_args,
            out_path,
        ]

        process = subprocess.run(cmd, capture_output=True, timeout=20)

        if process.returncode != 0:
            if process.stderr:
                logger.debug(
                    f"FFmpeg stderr at {timestamp}s: {process.stderr.decode(errors='replace')}"
                )
            return None

        with Path(out_path).open("rb") as fh:
            data = fh.read()

        if not _is_valid_image(data, FRAME_FORMAT):
            logger.warning(f"FFmpeg produced an invalid {FRAME_FORMAT} frame at {timestamp}s")
            return None

        return data

    except subprocess.TimeoutExpired:
        logger.warning(f"FFmpeg timeout at {timestamp}s")
        return None
    except Exception as e:
        logger.warning(f"FFmpeg failed at {timestamp}s: {e}")
        return None
    finally:
        with contextlib.suppress(OSError):
            Path(out_path).unlink()


def _extract_frames_surgical(
    stream_url: str, target_timestamps: list[float], duration: float, run_logger: Any
) -> list[tuple[int, bytes]]:
    """
    Worker function using FFmpeg for surgical frame extraction.

    Executed in a thread pool to avoid blocking the asyncio event loop.
    Each frame is extracted independently via HTTP Range requests.
    """
    frames_to_vault: list[tuple[int, bytes]] = []

    fps = 30.0

    for ts in target_timestamps:
        if ts > duration:
            continue

        frame_idx = int(ts * fps)

        image_bytes = _ffmpeg_extract_frame(stream_url, ts)

        if image_bytes:
            frames_to_vault.append((frame_idx, image_bytes))
        else:
            run_logger.warning(f"FFmpeg failed to grab frame at {ts}s")

    return frames_to_vault


async def process_frames_task(video: Video) -> tuple[str, list[tuple[int, bytes]]] | None:
    """Extract keyframes for a single video (FFmpeg surgical extraction).

    Preferred path: read the streamer's stashed ``meta/{id}.info.json`` from the
    vault and pull frames via HTTP range requests from YouTube's CDN (one YouTube
    session). Legacy rows with no vault metadata fall back to a fresh YouTube
    ``extract_info``. Returns ``(video_id, frames_to_vault)`` or ``None`` on hard
    failure. Storage is deferred to the flow level.
    """
    video_repo = VideoRepository()
    run_logger = logger
    vid_id = video.id

    # Idempotent: a DONE video is never re-processed; this guards manual reruns
    # (the claim gate already excludes it, and `repaint` resets to PENDING).
    if video.visuals_phase == "DONE":
        run_logger.info(f"Visuals already extracted for {vid_id} — skipping")
        return None

    tmpdir = tempfile.mkdtemp(prefix="painter-raw-")
    try:
        v = get_vault()
        local_input: str | None = None
        duration = 0.0
        chapters: list[float] = []
        heatmap: list[float] = []

        # Preferred path: the unified streamer stashed the video's stream URLs
        # and metadata in the vault as meta/{id}.info.json, so we pull frames via
        # HTTP range requests from YouTube's CDN (no second YouTube session).
        meta_buf = await asyncio.to_thread(v.fetch_binary, meta_path(vid_id))
        if meta_buf is None:
            meta_buf = await asyncio.to_thread(v.fetch_binary, legacy_meta_path(vid_id))
        info: dict[str, Any] | None = None
        if meta_buf is not None:
            try:
                info = json.loads(meta_buf.getvalue())
            except Exception:
                info = None

        if info is not None:
            stream_url = select_stream_url(info)
            if stream_url:
                local_input = stream_url
                duration = info.get("duration", 0) or 0
                chapters = [c.get("start_time", 0.0) for c in info.get("chapters", []) or []]
                heatmap = StealthVideoStreamer().extract_heatmap_peaks(
                    info.get("heatmap", []) or [], top_n=HEATMAP_PEAKS
                )

        # Fallback (legacy rows missing vault metadata): fresh YouTube metadata.
        if local_input is None:
            run_logger.info(f"No vault metadata for {vid_id}; falling back to YouTube frames")
            streamer = StealthVideoStreamer()
            info = await asyncio.to_thread(streamer.extract_info, vid_id)
            duration = info.get("duration", 0) or 0
            stream_url = select_stream_url(info)
            if not stream_url:
                run_logger.error(f"No stream URL found for {vid_id}")
                await video_repo.mark_step_failed(vid_id, "visuals")
                return None
            chapters = [c.get("start_time", 0.0) for c in info.get("chapters", []) or []]
            heatmap = streamer.extract_heatmap_peaks(
                info.get("heatmap", []) or [], top_n=HEATMAP_PEAKS
            )
            local_input = stream_url

        sorted_timestamps = plan_timestamps(duration, chapters, heatmap)
        run_logger.info(
            f"Targeting {len(sorted_timestamps)} frames for {vid_id} "
            f"(duration={duration}s, chapters={len(chapters)}, peaks={len(heatmap)})"
        )

        frames_to_vault = await asyncio.to_thread(
            _extract_frames_surgical, local_input, sorted_timestamps, duration, run_logger
        )

        if not frames_to_vault:
            # The stashed CDN URL is signed and short-TTL; it may have expired,
            # so retry once with a fresh YouTube metadata pull before giving up.
            if meta_buf is not None:
                run_logger.warning(
                    f"Frame pull failed for {vid_id} via vault URL (likely expired); refreshing"
                )
                streamer = StealthVideoStreamer()
                info = await asyncio.to_thread(streamer.extract_info, vid_id)
                stream_url = select_stream_url(info)
                if stream_url:
                    frames_to_vault = await asyncio.to_thread(
                        _extract_frames_surgical,
                        stream_url,
                        sorted_timestamps,
                        duration,
                        run_logger,
                    )
            if not frames_to_vault:
                run_logger.warning(f"No frames extracted for {vid_id}")
                await video_repo.mark_step_failed(vid_id, "visuals")
                return None

        run_logger.info(f"Extracted {len(frames_to_vault)} keyframes for {vid_id}")
        return (vid_id, frames_to_vault)

    except QuotaExhaustedError:
        await notify_quota_exhausted("painter")
        raise
    except StreamRateLimitError as e:
        # Transient — release back to PENDING so it retries on a later cycle
        # instead of being permanently marked FAILED.
        run_logger.warning(f"Rate-limited on {vid_id}, releasing for retry: {e}")
        await video_repo.release_visuals_to_pending(vid_id)
        return None
    except Exception as e:
        run_logger.exception(f"Painter failed on {vid_id}: {e}")
        await video_repo.mark_step_failed(vid_id, "visuals")
        return None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


class PainterAgent(BaseBatchAgent):
    """Painter Agent: video keyframe extraction."""

    name = "painter"
    default_batch_size = 5
    max_concurrent = MAX_CONCURRENT_VIDEOS
    raise_on = (QuotaExhaustedError,)

    async def claim_batch(self, n: int) -> list[Video]:
        return await fetch_painter_targets_task(n)

    async def process_one(self, video: Video) -> tuple[str, list[tuple[int, bytes]]] | None:
        # Surgical frame extraction paces itself with a small random jitter to
        # avoid stampeding YouTube's CDN with perfectly-aligned requests.
        await asyncio.sleep(random.uniform(0.5, 2.0))
        return await process_frames_task(video)

    async def store_results(self, results: list[Any]) -> None:
        # Write the whole batch to the vault in ONE commit to stay under
        # HuggingFace's 128-commits/hour account cap; failed/RELEASED videos are
        # absent here. (Painter's vault API is bespoke — store_visual_evidence_batch
        # — so it is not covered by the generic maia.storage helper.)
        extracted: list[tuple[str, list[tuple[int, bytes]]]] = [
            r for r in results if isinstance(r, tuple)
        ]
        if not extracted:
            return
        video_repo = VideoRepository()
        v = get_vault()
        entries = [(vid, frames, FRAME_FORMAT) for vid, frames in extracted]
        try:
            await vault_op_with_retry(lambda: v.store_visual_evidence_batch(entries))
            for vid, _ in extracted:
                await video_repo.mark_visuals_safe(vid)
            logger.info(f"Batched {len(extracted)} videos' frames into ONE vault commit")
        except Exception as e:
            logger.exception(f"Batched frame store failed ({len(extracted)} vids): {e}")
            for vid, _ in extracted:
                await video_repo.mark_step_failed(vid, "visuals")


async def painter_operation(batch_size: int | None = None, **kwargs: Any) -> dict[str, Any]:
    """Execute one Painter cycle as a plain application operation."""
    return await PainterAgent().run(batch_size=batch_size, **kwargs)


@flow(name="run_painter_cycle")
async def painter_flow(batch_size: int) -> dict[str, Any]:
    """Prefect compatibility adapter for :func:`painter_operation`."""
    return await painter_operation(batch_size=batch_size)


async def run_painter_cycle(batch_size: int = 5) -> None:
    """Legacy plain compatibility entrypoint; prefer :func:`painter_operation`."""
    await painter_operation(batch_size=batch_size)


def main() -> None:
    run_agent_main(lambda: painter_operation(batch_size=PainterAgent.default_batch_size), "painter")


if __name__ == "__main__":
    cli_bootstrap()
    main()
