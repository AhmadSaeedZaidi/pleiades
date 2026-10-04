from atlas.adapters import DatabaseAdapter
from atlas.config import settings
from atlas.db import db
from atlas.events import events
from atlas.models import (
    Channel,
    ChannelStats,
    SearchQueueItem,
    SystemEvent,
    Video,
    VideoStats,
    WatchlistItem,
)
from atlas.notifications import AlertChannel, AlertLevel, notifier
from atlas.repositories import (
    ChannelRepository,
    EventRepository,
    SearchQueueRepository,
    VideoRepository,
    WatchlistRepository,
)
from atlas.vault import get_vault

__all__ = [
    # Configuration
    "settings",
    "db",
    # Storage
    "get_vault",
    # Events & notifications
    "events",
    "notifier",
    "AlertChannel",
    "AlertLevel",
    # Base adapter
    "DatabaseAdapter",
    # Domain models
    "Channel",
    "ChannelStats",
    "SearchQueueItem",
    "SystemEvent",
    "Video",
    "VideoStats",
    "WatchlistItem",
    # Repositories
    "ChannelRepository",
    "EventRepository",
    "SearchQueueRepository",
    "VideoRepository",
    "WatchlistRepository",
]

__version__ = "0.2.1"
