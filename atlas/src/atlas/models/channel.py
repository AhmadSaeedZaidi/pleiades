from datetime import datetime

from pydantic import BaseModel, ConfigDict


class Channel(BaseModel):  # type: ignore[misc]
    id: str
    title: str
    country: str | None = None
    custom_url: str | None = None
    created_at: datetime | None = None
    is_verified: bool = False
    last_scraped_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class ChannelStats(BaseModel):  # type: ignore[misc]
    channel_id: str
    timestamp: datetime
    view_count: int | None = None
    subscriber_count: int | None = None
    video_count: int | None = None

    model_config = ConfigDict(from_attributes=True)
