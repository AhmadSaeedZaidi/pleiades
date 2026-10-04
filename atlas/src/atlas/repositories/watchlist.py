import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from atlas.adapters import DatabaseAdapter
from atlas.config import settings
from atlas.models.watchlist import WatchlistItem

logger = logging.getLogger("atlas.repositories.watchlist")

_TIER_INTERVAL = {
    "DORMANT": timedelta(days=30),
    "HOURLY": timedelta(hours=1),
    "DAILY": timedelta(days=1),
    "WEEKLY": timedelta(days=7),
}


UNAVAILABLE_MISSES_BEFORE_DORMANT = 3
# More-frequent-first order used by the drop-one-tier logic.
_TIER_ORDER = ("HOURLY", "DAILY", "WEEKLY")


class WatchlistRepository(DatabaseAdapter):
    async def add(self, video_id: str, tier: str = "HOURLY") -> None:
        await self._execute(
            """
            INSERT INTO watchlist (video_id, tracking_tier, next_track_at, created_at)
            VALUES (%s, %s, NOW(), NOW())
            ON CONFLICT (video_id) DO NOTHING
            """,
            (video_id, tier),
        )

    async def fetch_batch(self, batch_size: int = 50) -> list[WatchlistItem]:
        rows = await self._fetch_all(
            """
            SELECT w.video_id, w.tracking_tier, w.last_tracked_at,
                   w.last_views, w.last_likes,
                   w.last_comment_count, w.unavailable_count,
                   w.next_track_at, w.created_at, v.published_at
            FROM watchlist w
            LEFT JOIN videos v ON v.id = w.video_id
            WHERE w.next_track_at <= NOW()
            ORDER BY w.next_track_at ASC
            LIMIT %s
            FOR UPDATE OF w SKIP LOCKED
            """,
            (batch_size,),
        )
        return [WatchlistItem.model_validate(r) for r in rows]

    async def update_schedule(self, updates: list[dict[str, Any]]) -> None:
        if not updates:
            return

        query = """
            UPDATE watchlist
            SET tracking_tier = %s,
                last_tracked_at = %s,
                last_views = %s,
                last_likes = %s,
                last_comment_count = %s,
                unavailable_count = %s,
                next_track_at = %s
            WHERE video_id = %s
        """
        params_list = [
            (
                u["tracking_tier"],
                u["last_tracked_at"],
                u.get("last_views"),
                u.get("last_likes"),
                u.get("last_comment_count"),
                u.get("unavailable_count", 0),
                u["next_track_at"],
                u["video_id"],
            )
            for u in updates
        ]
        await self._execute_many(query, params_list)

    @staticmethod
    def velocity_views_per_hour(
        previous_views: int | None,
        previous_at: datetime | None,
        current_views: int | None,
        current_at: datetime,
    ) -> float | None:
        """Calculate velocity from the durable watchlist sample.

        The previous counters and timestamp are read from ``watchlist`` by
        :meth:`fetch_batch`; this deliberately does not depend on retention of
        rows in ``video_stats_log``. Missing counters, clock skew, or a view
        counter reset make velocity unknown.
        """
        if previous_views is None or current_views is None or previous_at is None:
            return None
        if previous_at.tzinfo is None:
            previous_at = previous_at.replace(tzinfo=UTC)
        if current_at.tzinfo is None:
            current_at = current_at.replace(tzinfo=UTC)
        delta_h = (current_at - previous_at).total_seconds() / 3600.0
        if delta_h <= 0:
            return None

        return max(0.0, (current_views - previous_views) / delta_h)

    def _tier_from_age(self, published_at: datetime) -> str:
        now = datetime.now(UTC)
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=UTC)
        age = now - published_at
        if age < timedelta(hours=settings.TRACKER_AGE_HOURLY_HOURS):
            return "HOURLY"
        if age < timedelta(days=settings.TRACKER_AGE_DAILY_DAYS):
            return "DAILY"
        return "WEEKLY"

    def _boost_tier(self, age_tier: str, views_per_hour: float | None) -> str:
        """Velocity override on top of the age floor.

        * HOT velocity  → HOURLY (a growing video stays hourly past the age cutoffs)
        * DEAD velocity → one tier less frequent than the age floor (recedes sooner)
        * otherwise / unknown → the age floor unchanged
        """
        if views_per_hour is None:
            return age_tier
        if views_per_hour >= settings.TRACKER_HOT_VIEWS_PER_HOUR:
            return "HOURLY"
        if views_per_hour <= settings.TRACKER_DEAD_VIEWS_PER_HOUR:
            # Drop one tier, flooring at WEEKLY (never slower than weekly).
            idx = _TIER_ORDER.index(age_tier)
            return _TIER_ORDER[min(idx + 1, len(_TIER_ORDER) - 1)]
        return age_tier

    def calculate_next_track_time(
        self,
        published_at: datetime | None,
        views_per_hour: float | None = None,
        tier: str | None = None,
    ) -> tuple[str, datetime]:
        now = datetime.now(UTC)

        # Missing publication time falls back to the current tier. A successful
        # observation revives DORMANT rows to the normal weekly floor.
        if published_at is not None:
            age_tier = self._tier_from_age(published_at)
        else:
            age_tier = "WEEKLY" if tier == "DORMANT" else (tier or "WEEKLY")

        effective_tier = self._boost_tier(age_tier, views_per_hour)
        interval = _TIER_INTERVAL[effective_tier]

        return (effective_tier, now + interval)

    @staticmethod
    def retry_next_track_time(tier: str, now: datetime | None = None) -> tuple[str, datetime]:
        """Retry a missing item at its existing cadence before dormancy."""
        retry_tier = tier if tier in _TIER_ORDER else "WEEKLY"
        anchor = now or datetime.now(UTC)
        return retry_tier, anchor + _TIER_INTERVAL[retry_tier]

    @staticmethod
    def dormant_next_track_time(now: datetime | None = None) -> datetime:
        """Return the monthly recheck time for an unavailable video."""
        return (now or datetime.now(UTC)) + _TIER_INTERVAL["DORMANT"]
