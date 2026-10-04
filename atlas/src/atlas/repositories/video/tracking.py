import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from atlas.adapters import DatabaseAdapter
from atlas.models.video import VideoStats

logger = logging.getLogger("atlas.repositories.video.tracking")


def _to_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


class VideoTrackingMixin(DatabaseAdapter):
    async def log_stats_batch(self, stats_list: list[VideoStats]) -> None:
        if not stats_list:
            return

        query = """
            INSERT INTO video_stats_log (video_id, views, likes, comment_count, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (video_id, timestamp) DO UPDATE SET
                views = EXCLUDED.views,
                likes = EXCLUDED.likes,
                comment_count = EXCLUDED.comment_count
        """
        params_list = [
            (s.video_id, s.views, s.likes, s.comment_count, s.timestamp) for s in stats_list
        ]
        await self._execute_many(query, params_list)
        logger.info(f"Logged {len(stats_list)} stats to hot tier")

    async def update_stats_batch(
        self,
        updates: list[dict[str, Any]],
        watchlist_updates: list[dict[str, Any]] | None = None,
        tracked_at: datetime | None = None,
    ) -> None:
        """Persist a Tracker sample and its schedule in one transaction.

        ``updates`` contains the API-returned video items. ``watchlist_updates``
        contains every requested ID, including IDs missing from the response,
        so a missing video can advance its recheck state without fabricating a
        stats row. The single cursor/commit makes the video timestamp, stats
        sample, and durable watchlist state succeed or fail together.
        """
        if not updates and not watchlist_updates:
            return

        watchlist_updates = watchlist_updates or []
        now = tracked_at or datetime.now(UTC)
        video_ids = list(dict.fromkeys([str(u["id"]) for u in updates]))

        timestamp_statement = "UPDATE videos SET last_tracked_at = %s WHERE id = %s"
        log_statement = """
            INSERT INTO video_stats_log (video_id, views, likes, comment_count, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (video_id, timestamp) DO UPDATE SET
                views = EXCLUDED.views,
                likes = EXCLUDED.likes,
                comment_count = EXCLUDED.comment_count
        """
        watchlist_statement = """
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

        ts_params = [(now, video_id) for video_id in video_ids]
        log_params = [
            (
                str(u["id"]),
                _to_int(u.get("statistics", {}).get("viewCount")),
                _to_int(u.get("statistics", {}).get("likeCount")),
                _to_int(u.get("statistics", {}).get("commentCount")),
                now,
            )
            for u in updates
        ]
        watchlist_params = [
            (
                u["tracking_tier"],
                u.get("last_tracked_at"),
                u.get("last_views"),
                u.get("last_likes"),
                u.get("last_comment_count"),
                u.get("unavailable_count", 0),
                u["next_track_at"],
                str(u["video_id"]),
            )
            for u in watchlist_updates
        ]

        async with self._cursor() as cur:
            if updates:
                await cur.executemany(timestamp_statement, ts_params)
                await cur.executemany(log_statement, log_params)
            if watchlist_updates:
                await cur.executemany(watchlist_statement, watchlist_params)
            await cur.connection.commit()

    async def pipeline_snapshot(self) -> dict[str, Any]:
        """Return a point-in-time snapshot of pipeline health for reporting.

        Includes lifecycle status counts, total videos, transcript / visual
        coverage, tracker metrics, and pipeline velocity.
        """
        now = datetime.now(UTC)
        cutoff_1h = now - timedelta(hours=1)
        cutoff_24h = now - timedelta(hours=24)

        # Single aggregate pass over `videos` for all scalar counts (was 9
        # separate COUNT queries). Status/phase groupings stay separate because
        # they are different GROUP BY shapes; transcripts/video_stats_log are
        # different tables.
        agg = await self._fetch_one(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE has_visuals) AS with_visuals,
                COUNT(*) FILTER (WHERE has_audio) AS audios,
                COUNT(*) FILTER (WHERE discovered_at > NOW() - INTERVAL '1 hour') AS ingested_1h,
                COUNT(*) FILTER (WHERE last_tracked_at IS NOT NULL) AS tracked_ever,
                COUNT(*) FILTER (WHERE last_tracked_at > %s) AS tracked_1h,
                COUNT(*) FILTER (WHERE last_tracked_at > %s) AS tracked_24h
            FROM videos
            """,
            (cutoff_1h, cutoff_24h),
        )
        agg = agg or {}

        status_rows = await self._fetch_all(
            "SELECT status, COUNT(*) AS c FROM videos GROUP BY status"
        )
        status_counts = {r["status"]: r["c"] for r in status_rows}

        # Per-step terminal failures. These used to be reported through a
        # row-level status='FAILED', but mark_step_failed now records the failure
        # only in the phase column so one dead stage cannot strand the other four
        # consumers. Surface them separately, keyed as step names, and also total
        # them under "failed_steps" for dashboards that just want a count.
        failed_step_rows = await self._fetch_all(
            """
            SELECT
                COUNT(*) FILTER (WHERE raw_phase = 'FAILED') AS raw,
                COUNT(*) FILTER (WHERE audio_phase = 'FAILED') AS audio,
                COUNT(*) FILTER (WHERE visuals_phase = 'FAILED') AS visuals,
                COUNT(*) FILTER (WHERE transcript_phase = 'FAILED') AS transcript,
                COUNT(*) FILTER (WHERE clip_phase = 'FAILED') AS clip
            FROM videos
            WHERE raw_phase = 'FAILED'
               OR audio_phase = 'FAILED'
               OR visuals_phase = 'FAILED'
               OR transcript_phase = 'FAILED'
               OR clip_phase = 'FAILED'
            """
        )
        failed_row = failed_step_rows[0] if failed_step_rows else {}
        failed_step_counts = {
            step: int(failed_row.get(step, 0) or 0)
            for step in ("raw", "audio", "visuals", "transcript", "clip")
        }
        # A video can fail more than one stage; count distinct videos, not stages.
        failed_videos = (
            await self._fetch_scalar(
                """
            SELECT COUNT(*) FROM videos
            WHERE raw_phase = 'FAILED'
               OR audio_phase = 'FAILED'
               OR visuals_phase = 'FAILED'
               OR transcript_phase = 'FAILED'
               OR clip_phase = 'FAILED'
            """
            )
            or 0
        )

        transcripts = await self._fetch_scalar("SELECT COUNT(*) FROM transcripts") or 0
        stats_log_size = await self._fetch_scalar("SELECT COUNT(*) FROM video_stats_log") or 0

        pipeline_phase_rows = await self._fetch_all(
            "SELECT COALESCE(pipeline_phase, 'NONE') AS phase, COUNT(*) AS c "
            "FROM videos GROUP BY pipeline_phase"
        )
        phase_counts = {r["phase"]: r["c"] for r in pipeline_phase_rows}

        return {
            "total": agg.get("total") or 0,
            "status_counts": status_counts,
            "failed_step_counts": failed_step_counts,
            "failed_steps": failed_videos,
            "transcripts": transcripts,
            "with_visuals": agg.get("with_visuals") or 0,
            "audios": agg.get("audios") or 0,
            "ingested_1h": agg.get("ingested_1h") or 0,
            "tracked_ever": agg.get("tracked_ever") or 0,
            "tracked_1h": agg.get("tracked_1h") or 0,
            "tracked_24h": agg.get("tracked_24h") or 0,
            "stats_log_size": stats_log_size,
            "phase_counts": phase_counts,
        }
