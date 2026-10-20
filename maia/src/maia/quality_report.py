"""Ingestion-quality monitor.

Surfaces corpus-level statistics so operators can watch what the Hunter is
actually pulling in (Shorts proportion, duration mix, artifact coverage) and
decide when to tighten the quality gate or run a purge.
"""

import logging
from typing import Any

from atlas.repositories import VideoRepository

logger = logging.getLogger(__name__)


async def report_ingestion_quality() -> dict[str, Any]:
    """Return aggregate ingestion-quality statistics for the videos table."""
    repo = VideoRepository()
    return dict[str, Any](await repo.quality_report())
