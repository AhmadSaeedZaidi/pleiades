"""Heuristic quality gate for discovered videos (pre-ingestion).

Rejects Shorts / low-traction / low-engagement / AI-slop videos before they
hit the database or seed the snowball. Pure and side-effect free for testing.

This package was extracted from the former single ``maia/quality.py`` module so
each consumer (Hunter / Archeologist) imports only the piece it needs; the public
names below are re-exported here to preserve the ``from maia.quality import ...``
API.
"""

import aiohttp  # noqa: F401  keep importable as maia.quality.aiohttp (used by tests)

from maia.quality.duration import parse_iso8601_duration
from maia.quality.enrich import filter_by_quality
from maia.quality.gates import (
    _matches_ai,
    _matches_reupload,
    evaluate_channel,
    evaluate_video,
)
from maia.quality.shorts import is_youtube_short
from maia.quality.thresholds import QualityThresholds
from maia.quality.types import QualityResult

__all__ = [
    "QualityThresholds",
    "QualityResult",
    "evaluate_video",
    "evaluate_channel",
    "filter_by_quality",
    "parse_iso8601_duration",
    "is_youtube_short",
    "_matches_ai",
    "_matches_reupload",
]
