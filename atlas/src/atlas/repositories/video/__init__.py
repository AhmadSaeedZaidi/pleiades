import logging

from atlas.adapters import DatabaseAdapter
from atlas.repositories.video.ingestion import VideoIngestionMixin
from atlas.repositories.video.janitor import VideoJanitorMixin
from atlas.repositories.video.quality import VideoQualityMixin
from atlas.repositories.video.state_machine import VideoStateMixin
from atlas.repositories.video.tracking import VideoTrackingMixin
from atlas.repositories.video.transcript import TranscriptRepository

logger = logging.getLogger("atlas.repositories.video")


class VideoRepository(
    VideoIngestionMixin,
    VideoTrackingMixin,
    VideoStateMixin,
    VideoJanitorMixin,
    VideoQualityMixin,
    TranscriptRepository,
    DatabaseAdapter,
):
    """Repository for the ``videos`` table and its associated stats log."""


__all__ = ["VideoRepository", "TranscriptRepository"]
