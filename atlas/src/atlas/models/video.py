from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict

# Per-step phase for the fan-out/fan-in pipeline (streamer/singer/painter/
# scribe/muralist); legacy booleans are a transitional seam.
StepPhase = Literal["PENDING", "PROCESSING", "DONE", "FAILED"]


class Video(BaseModel):  # type: ignore[misc]
    id: str
    channel_id: str | None = None
    title: str
    published_at: datetime | None = None
    duration: int | None = None
    tags: list[str] | None = None
    category_id: str | None = None
    default_language: str | None = None
    wiki_topics: list[str] | None = None
    discovered_at: datetime | None = None
    last_updated_at: datetime | None = None
    last_tracked_at: datetime | None = None
    archived_at: datetime | None = None
    status: str | None = "PENDING"
    has_transcript: bool = False
    has_visuals: bool = False
    has_audio: bool = False
    has_video: bool = False
    fetched: bool = False
    raw_uri: str | None = None
    # ── P1b per-step phases ──
    raw_phase: StepPhase = "PENDING"
    audio_phase: StepPhase = "PENDING"
    visuals_phase: StepPhase = "PENDING"
    transcript_phase: StepPhase = "PENDING"
    clip_phase: StepPhase = "PENDING"
    # Derived frontier for ops/monitoring only; never drives claim selection.
    pipeline_phase: str | None = None

    model_config = ConfigDict(from_attributes=True)


class VideoStats(BaseModel):  # type: ignore[misc]
    video_id: str
    timestamp: datetime
    views: int | None = None
    likes: int | None = None
    comment_count: int | None = None

    model_config = ConfigDict(from_attributes=True)
