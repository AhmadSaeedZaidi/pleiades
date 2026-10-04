"""Quality / retention queries for the ``videos`` table.

Supports the ingestion-quality monitor and the short-video purge command.
"""

import logging
from typing import Any

from atlas.adapters import DatabaseAdapter

logger = logging.getLogger("atlas.repositories.video.quality")


class VideoQualityMixin(DatabaseAdapter):
    async def find_short_video_ids(self, max_duration: int, limit: int | None = None) -> list[str]:
        """Return IDs of videos shorter than ``max_duration`` seconds.

        Includes videos that already have artifacts (transcripts, frames, audio,
        video) — the purge command removes those too.
        """
        query = (
            "SELECT id FROM videos "
            "WHERE duration IS NOT NULL AND duration < %s "
            "ORDER BY discovered_at ASC"
        )
        params: tuple[Any, ...] = (max_duration,)
        if limit is not None:
            query += " LIMIT %s"
            params = (max_duration, limit)
        rows = await self._fetch_all(query, params)
        return [row["id"] for row in rows]

    async def delete_videos(self, ids: list[str]) -> int:
        """Permanently delete the given video rows (and their artifacts' rows).

        ``transcripts`` and ``video_stats_log`` cascade via FK; ``watchlist`` has
        no FK so it is deleted explicitly. Returns the number of video rows removed.
        """
        if not ids:
            return 0
        await self._execute("DELETE FROM watchlist WHERE video_id = ANY(%s)", (ids,))
        await self._execute("DELETE FROM videos WHERE id = ANY(%s)", (ids,))
        # Row count from the last statement (videos) is what we report.
        return len(ids)

    async def quality_report(self) -> dict[str, Any]:
        """Aggregate ingestion-quality statistics for monitoring."""
        # Single aggregate pass over `videos` replaces the previous 4 scalar
        # COUNT queries; status/duration-bucket groupings stay separate because
        # they are different GROUP BY shapes.
        agg = await self._fetch_one(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE duration IS NOT NULL) AS total_with_duration,
                COUNT(*) FILTER (WHERE duration IS NOT NULL AND duration < 180) AS shorts_under_3m,
                COUNT(*) FILTER (WHERE has_transcript) AS transcripts,
                COUNT(*) FILTER (WHERE has_visuals)     AS visuals,
                COUNT(*) FILTER (WHERE has_audio)       AS audio,
                COUNT(*) FILTER (WHERE has_video)       AS video
            FROM videos
            """
        )
        agg = agg or {}
        by_status = await self._fetch_all(
            "SELECT status, COUNT(*) AS n FROM videos GROUP BY status ORDER BY n DESC"
        )
        buckets = await self._fetch_all(
            """
            SELECT
                CASE
                    WHEN duration < 60  THEN 'under_1m'
                    WHEN duration < 180 THEN '1_3m'
                    WHEN duration < 600 THEN '3_10m'
                    WHEN duration < 1200 THEN '10_20m'
                    ELSE 'over_20m'
                END AS bucket,
                COUNT(*) AS n
            FROM videos
            WHERE duration IS NOT NULL
            GROUP BY bucket
            ORDER BY bucket
            """
        )
        return {
            "total_videos": agg.get("total") or 0,
            "total_with_duration": agg.get("total_with_duration") or 0,
            "shorts_under_3m": agg.get("shorts_under_3m") or 0,
            "by_status": {row["status"]: row["n"] for row in by_status},
            "duration_buckets": {row["bucket"]: row["n"] for row in buckets},
            "artifact_coverage": {
                "transcripts": agg.get("transcripts") or 0,
                "visuals": agg.get("visuals") or 0,
                "audio": agg.get("audio") or 0,
                "video": agg.get("video") or 0,
            },
        }
