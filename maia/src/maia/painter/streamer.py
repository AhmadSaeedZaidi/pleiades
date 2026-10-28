"""Maia Painter: stealth YouTube streamer.

The actual implementation now lives in :mod:`maia.media.streamer` so the Painter
and Scribe share one yt-dlp + Deno PoToken invocation path. This module exists
only to keep existing imports (and tests) working.
"""

from maia.media.streamer import (
    AudioExtractionError,
    StealthVideoStreamer,
    StreamRateLimitError,
    VideoExtractionError,
)

__all__ = [
    "StealthVideoStreamer",
    "StreamRateLimitError",
    "AudioExtractionError",
    "VideoExtractionError",
]
