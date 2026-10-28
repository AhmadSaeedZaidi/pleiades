from datetime import datetime

from pydantic import BaseModel, ConfigDict


class WatchlistItem(BaseModel):  # type: ignore[misc]
    video_id: str
    tracking_tier: str = "HOURLY"
    last_tracked_at: datetime | None = None
    last_views: int | None = None
    last_likes: int | None = None
    last_comment_count: int | None = None
    unavailable_count: int = 0
    next_track_at: datetime
    created_at: datetime | None = None
    # Joined from videos for decay scheduling. It can be NULL for legacy rows
    # or source metadata that did not include a publication timestamp.
    published_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)
