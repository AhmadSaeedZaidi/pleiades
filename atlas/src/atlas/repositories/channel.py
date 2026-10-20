import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from atlas.adapters import DatabaseAdapter
from atlas.models.channel import Channel, ChannelStats
from atlas.repositories.topics import record_topic_snapshot

logger = logging.getLogger("atlas.repositories.channel")


class ChannelRepository(DatabaseAdapter):
    async def get_by_id(self, channel_id: str) -> Channel | None:
        row = await self._fetch_one("SELECT * FROM channels WHERE id = %s", (channel_id,))
        return Channel.model_validate(row) if row else None

    async def get_latest_stats(self, channel_id: str) -> ChannelStats | None:
        row = await self._fetch_one(
            """
            SELECT channel_id, subscriber_count, view_count, video_count, timestamp
            FROM channel_stats_log
            WHERE channel_id = %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (channel_id,),
        )
        return ChannelStats.model_validate(row) if row else None

    async def needs_refresh(self, channel_id: str, max_age_hours: int = 24) -> bool:
        cutoff = datetime.now(UTC) - timedelta(hours=max_age_hours)
        row = await self._fetch_one(
            """
            SELECT 1 FROM channel_stats_log
            WHERE channel_id = %s AND timestamp >= %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (channel_id, cutoff),
        )
        return row is None

    async def ingest_channel_snapshot(self, channel_data: dict[str, Any]) -> None:
        ch_id = channel_data.get("id")
        if not ch_id:
            logger.warning("ingest_channel_snapshot called without channel id")
            return

        snippet = channel_data.get("snippet", {}) or {}
        stats = channel_data.get("statistics", {}) or {}

        title = snippet.get("title") or ch_id
        country = snippet.get("country")
        custom_url = snippet.get("customUrl")
        created_at = snippet.get("publishedAt")

        now = datetime.now(UTC)

        upsert_query = """
            INSERT INTO channels (id, title, country, custom_url, created_at, last_scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                country = COALESCE(EXCLUDED.country, channels.country),
                custom_url = COALESCE(EXCLUDED.custom_url, channels.custom_url),
                created_at = COALESCE(channels.created_at, EXCLUDED.created_at),
                last_scraped_at = EXCLUDED.last_scraped_at
        """

        def _to_int(v: Any) -> int | None:
            if v is None:
                return None
            try:
                return int(v)
            except (TypeError, ValueError):
                return None

        sub_count = _to_int(stats.get("subscriberCount"))
        view_count = _to_int(stats.get("viewCount"))
        video_count = _to_int(stats.get("videoCount"))

        log_query = """INSERT INTO channel_stats_log
            (channel_id, timestamp, view_count, subscriber_count, video_count)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (channel_id, timestamp) DO NOTHING
        """
        async with self._connection() as conn, conn.transaction():
            await conn.execute(upsert_query, (ch_id, title, country, custom_url, created_at, now))
            await record_topic_snapshot(conn, "channel", channel_data)
            if any(value is not None for value in (sub_count, view_count, video_count)):
                await conn.execute(log_query, (ch_id, now, view_count, sub_count, video_count))
        logger.info(
            f"Channel snapshot {ch_id}: subs={sub_count}, views={view_count}, videos={video_count}"
        )
