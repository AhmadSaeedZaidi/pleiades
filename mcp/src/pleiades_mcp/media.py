"""Thin wrappers around Pleiades' real media capabilities, adapted for on-demand
single-video use by the MCP server (vs. the batch agent fleet).

Each function is self-contained: it spins up the YouTube streamer / ffmpeg / the
Mistral transcriber as needed, returns the produced data, and lets the caller
persist artifacts. Network-heavy and rate-limit-prone calls raise typed errors
the server maps to friendly MCP error messages.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from maia.media.streamer import (
    AudioExtractionError,
    StealthVideoStreamer,
    TranscriptExtractionError,
    TranscriptRateLimitError,
)
from maia.painter.flow import plan_timestamps, select_stream_url
from maia.scribe.loader import TranscriptLoader
from maia.scribe.transcription import transcribe_audio_download

logger = logging.getLogger("pleiades_mcp.media")

# Mirror the painter's frame policy so produced keyframes look like the fleet's.
FRAME_INTERVAL_SECONDS = 15.0
MIN_FRAMES = 4
MAX_FRAMES = 60
FRAME_HEIGHT = 720
FRAME_FORMAT = "webp"
FRAME_WEBP_QUALITY = 80


class MediaUnavailableError(Exception):
    """A required external resource (YouTube, ffmpeg, Mistral) was unavailable."""


def build_search_strategy(agent_name: str = "mcp-search") -> Any:
    """Return a YouTubeSearchStrategy for MCP search.

    If ``MCP_YOUTUBE_API_KEY_POOL_JSON`` is set, the strategy uses that dedicated
    reserve pool (a private KeyRing) so MCP searches never consume the shared
    Hunter/Tracker/Archeologist quota. Otherwise it falls back to the shared
    ``hunting`` ring, preserving the original behaviour.
    """
    import json
    import os

    from maia.strategies import YouTubeSearchStrategy

    raw = os.environ.get("MCP_YOUTUBE_API_KEY_POOL_JSON", "").strip()
    if not raw:
        # atlas.config loads .env only into its own declared fields, so a custom
        # var like MCP_YOUTUBE_API_KEY_POOL_JSON never reaches os.environ. Read
        # it directly from the project .env as a fallback.
        try:
            from dotenv import dotenv_values

            for candidate in (".env", "/home/ubuntu/code/pleiades/.env"):
                if os.path.exists(candidate):
                    val = dotenv_values(candidate).get("MCP_YOUTUBE_API_KEY_POOL_JSON")
                    if val and val.strip():
                        raw = val.strip()
                        break
        except Exception:  # noqa: BLE001
            raw = ""
    if not raw:
        return YouTubeSearchStrategy("hunting", agent_name=agent_name)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = [raw]
    keys = [parsed] if isinstance(parsed, str) else list(parsed)
    keys = [k for k in keys if k]
    if not keys:
        logger.warning(
            "MCP_YOUTUBE_API_KEY_POOL_JSON is set but empty; "
            "falling back to the shared 'hunting' pool."
        )
        return YouTubeSearchStrategy("hunting", agent_name=agent_name)

    from atlas.utils import KeyRing

    ring = KeyRing.from_keys("mcp-reserve", keys)
    strategy = YouTubeSearchStrategy("mcp-reserve", agent_name=agent_name, key_ring=ring)
    logger.info(
        "MCP search using dedicated reserve pool (%d key%s).",
        len(keys),
        "" if len(keys) == 1 else "s",
    )
    return strategy


def fetch_transcript_segments(video_id: str) -> list[dict[str, Any]]:
    """Return caption/transcript segments for *video_id* (captions-only cascade)."""
    try:
        return TranscriptLoader().fetch(video_id)
    except TranscriptRateLimitError as e:
        raise MediaUnavailableError(
            f"YouTube caption endpoint is rate-limiting this IP: {e}. Retry later."
        ) from e
    except TranscriptExtractionError as e:
        raise MediaUnavailableError(f"No transcript available for {video_id}: {e}") from e


def fetch_audio_segments(video_id: str) -> list[dict[str, Any]]:
    """Download *video_id* and transcribe its audio via the Mistral STT fallback.

    Returns Voxtral segments. Raises MediaUnavailableError on failure.
    """
    try:
        result = transcribe_audio_download(video_id, strategy="mistral")
    except Exception as e:  # noqa: BLE001 - surface a friendly message
        raise MediaUnavailableError(f"Audio transcription failed for {video_id}: {e}") from e
    return result.segments


def extract_audio_file(video_id: str, dest_dir: Path) -> Path:
    """Download *video_id*'s speech-optimized opus track to *dest_dir*."""
    streamer = StealthVideoStreamer()
    try:
        audio_path, _info = streamer.download_unified(video_id, str(dest_dir))
    except (AudioExtractionError, Exception) as e:  # noqa: BLE001
        raise MediaUnavailableError(f"Audio download failed for {video_id}: {e}") from e
    return audio_path


def extract_keyframes(video_id: str, dest_dir: Path) -> list[tuple[int, bytes]]:
    """Extract keyframe images (webp bytes) for *video_id* and return them."""
    streamer = StealthVideoStreamer()
    try:
        info = streamer.extract_info(video_id)
    except Exception as e:  # noqa: BLE001
        raise MediaUnavailableError(f"Could not fetch YouTube metadata for {video_id}: {e}") from e

    duration = float(info.get("duration") or 0)
    stream_url = select_stream_url(info)
    if not stream_url:
        raise MediaUnavailableError(f"No playable stream URL for {video_id}")

    chapters = [c.get("start_time", 0.0) for c in info.get("chapters", []) or []]
    heatmap = streamer.extract_heatmap_peaks(info.get("heatmap", []) or [], top_n=8)
    timestamps = plan_timestamps(duration, chapters, heatmap)

    from maia.painter.flow import _ffmpeg_extract_frame

    out: list[tuple[int, bytes]] = []
    for ts in timestamps:
        if ts > duration:
            continue
        data = _ffmpeg_extract_frame(stream_url, ts)
        if data:
            out.append((int(ts * 30), data))

    frames = out
    if not frames:
        raise MediaUnavailableError(f"No keyframes extracted for {video_id}")
    return frames


async def fetch_metadata(video_id: str) -> dict[str, Any]:
    """Resolve structured metadata for *video_id* via the YouTube Data API."""
    from atlas.youtube import lookup_videos

    items = await lookup_videos([video_id])
    if not items:
        raise MediaUnavailableError(f"No metadata found for {video_id}")
    return items[0]


# YouTube thumbnail file names, best resolution first.
_THUMBNAIL_NAMES = ("maxresdefault", "sddefault", "hqdefault", "mqdefault", "default")


def fetch_thumbnail(video_id: str) -> tuple[bytes, str]:
    """Fetch the highest-resolution available thumbnail for *video_id*.

    Returns ``(jpeg_bytes, source_url)``. Raises MediaUnavailableError if no
    thumbnail could be retrieved. Only talks to i.ytimg.com (no arbitrary URLs).
    """
    import httpx

    last_err: Exception | None = None
    with httpx.Client(timeout=10.0, follow_redirects=True) as client:
        for name in _THUMBNAIL_NAMES:
            url = f"https://i.ytimg.com/vi/{video_id}/{name}.jpg"
            try:
                resp = client.get(url)
            except Exception as e:  # noqa: BLE001
                last_err = e
                continue
            # YouTube returns a 120x90 placeholder (a small body) for missing
            # sizes rather than a 404, so also guard on a plausible size.
            if resp.status_code == 200 and len(resp.content) > 1024:
                return resp.content, url
    raise MediaUnavailableError(
        f"No thumbnail available for {video_id}" + (f": {last_err}" if last_err else "")
    )
