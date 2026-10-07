import logging
import re
from typing import Any

from atlas.adapters import DatabaseAdapter
from atlas.models.video import Video, VideoStats
from atlas.repositories.topics import record_topic_snapshot

logger = logging.getLogger("atlas.repositories.video.ingestion")

_ARCHIVAL_BATCH_SIZE = 100

_ISO_DURATION_RE = re.compile(r"^P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$")


def _parse_iso_duration(value: Any) -> int | None:
    """Parse an ISO-8601 duration (``PT1M5S``) to seconds; None if absent/invalid."""
    if not isinstance(value, str):
        return None
    m = _ISO_DURATION_RE.match(value)
    if not m:
        return None
    d, h, mn, s = (int(x) if x else 0 for x in m.groups())
    return d * 86400 + h * 3600 + mn * 60 + s


class VideoIngestionMixin(DatabaseAdapter):
    async def get_by_id(self, video_id: str) -> Video | None:
        row = await self._fetch_one("SELECT * FROM videos WHERE id = %s", (video_id,))
        return Video.model_validate(row) if row else None

    async def get_latest_stats(self, video_id: str) -> VideoStats | None:
        row = await self._fetch_one(
            """
            SELECT video_id, views, likes, comment_count, timestamp
            FROM video_stats_log
            WHERE video_id = %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (video_id,),
        )
        return VideoStats.model_validate(row) if row else None

    async def get_latest_stats_batch(self, video_ids: list[str]) -> dict[str, VideoStats]:
        """Return the latest stats row per video id in ONE query (avoids N+1).

        ``DISTINCT ON`` picks the newest ``video_stats_log`` row per video in a
        single pass; videos with no stats are simply absent from the result.
        """
        if not video_ids:
            return {}
        rows = await self._fetch_all(
            """
            SELECT DISTINCT ON (video_id) video_id, views, likes, comment_count, timestamp
            FROM video_stats_log
            WHERE video_id = ANY(%s)
            ORDER BY video_id, timestamp DESC
            """,
            (video_ids,),
        )
        return {r["video_id"]: VideoStats.model_validate(r) for r in rows}

    async def ingest_video_metadata(
        self, video_data: dict[str, Any], priority_override: int | None = None
    ) -> None:
        from datetime import UTC, datetime

        snippet = video_data.get("snippet", {})
        channel_id = snippet.get("channelId")
        channel_title = (
            snippet.get("channelTitle")
            or snippet.get("channel")
            or (_CHANNEL_TITLE_PENDING if channel_id else None)
        )

        async with self._connection() as conn:
            if channel_id and channel_title:
                channel_upsert = """
                    INSERT INTO channels (id, title, created_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        title = CASE
                            WHEN BTRIM(EXCLUDED.title) <> ''
                                AND EXCLUDED.title <> %s
                            THEN EXCLUDED.title
                            WHEN BTRIM(channels.title) <> ''
                                AND channels.title <> %s
                            THEN channels.title
                            ELSE EXCLUDED.title
                        END
                """
                pend = _CHANNEL_TITLE_PENDING
                await conn.execute(
                    channel_upsert,
                    (channel_id, channel_title, datetime.now(UTC), pend, pend),
                )

            video_query = """
                INSERT INTO videos (
                    id, channel_id, title, published_at, duration,
                    tags, category_id, default_language, discovered_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    duration = COALESCE(EXCLUDED.duration, videos.duration)
            """
            vid_id = video_data.get("id")
            if isinstance(vid_id, dict):
                vid_id = vid_id.get("videoId")

            if not vid_id:
                await conn.commit()
                return

            # Duration only comes from contentDetails, present when enriched via
            # videos.list (search snippets omit it).
            duration = _parse_iso_duration(video_data.get("contentDetails", {}).get("duration"))

            await conn.execute(
                video_query,
                (
                    vid_id,
                    channel_id,
                    snippet.get("title"),
                    snippet.get("publishedAt"),
                    duration,
                    snippet.get("tags", []),
                    snippet.get("categoryId"),
                    snippet.get("defaultLanguage"),
                    datetime.now(UTC),
                ),
            )
            # Enrollment belongs to the ingestion transaction, not to an
            # individual producer. Hunter, Archeologist, MCP/live tools, and
            # future callers therefore cannot create an untracked video row.
            await conn.execute(
                """
                INSERT INTO watchlist (video_id, tracking_tier, next_track_at, created_at)
                VALUES (%s, 'HOURLY', NOW(), NOW())
                ON CONFLICT (video_id) DO NOTHING
                """,
                (vid_id,),
            )
            await record_topic_snapshot(conn, "video", {**video_data, "id": vid_id})
            await conn.commit()


_CHANNEL_TITLE_PENDING = "Pending channel index"
