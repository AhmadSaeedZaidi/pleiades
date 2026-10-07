"""Bounded, read-only queries for the operator dashboard.

The dashboard supplies its own read-only pool. This repository never uses the
pipeline's write-capable DatabaseManager or calls agent operations.
"""

from typing import Any

from psycopg_pool import AsyncConnectionPool

from atlas.repositories.knowledge_graph import KnowledgeGraphReader

STAGES = ("raw", "audio", "visuals", "transcript", "clip")
STATUSES = ("PENDING", "PROCESSING", "PROCESSED", "ARCHIVED", "FAILED")


class DashboardRepository:
    def __init__(self, pool: AsyncConnectionPool[Any]) -> None:
        self.pool = pool

    async def graph(self, search: str = "", topic: str = "", limit: int = 25) -> dict[str, Any]:
        return await KnowledgeGraphReader(self.rows).graph(search, topic, limit)

    async def rows(self, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(query, params)
            return list(await cursor.fetchall())

    async def overview(self) -> dict[str, Any]:
        counts = (
            await self.rows("""
            SELECT COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status = 'ARCHIVED') AS archived,
                COUNT(*) FILTER (WHERE status = 'PROCESSED') AS processed,
                COUNT(*) FILTER (WHERE discovered_at >= NOW() - INTERVAL '24 hours') AS discovered,
                COUNT(*) FILTER (WHERE vault_write_pending) AS vault_pending,
                COUNT(*) FILTER (WHERE raw_phase = 'DONE') AS raw_done,
                COUNT(*) FILTER (WHERE audio_phase = 'DONE') AS audio_done,
                COUNT(*) FILTER (WHERE visuals_phase = 'DONE') AS visuals_done,
                COUNT(*) FILTER (WHERE transcript_phase = 'DONE') AS transcript_done,
                COUNT(*) FILTER (WHERE clip_phase = 'DONE') AS clip_done,
                COUNT(*) FILTER (WHERE raw_phase = 'FAILED') AS raw_failed,
                COUNT(*) FILTER (WHERE audio_phase = 'FAILED') AS audio_failed,
                COUNT(*) FILTER (WHERE visuals_phase = 'FAILED') AS visuals_failed,
                COUNT(*) FILTER (WHERE transcript_phase = 'FAILED') AS transcript_failed,
                COUNT(*) FILTER (WHERE clip_phase = 'FAILED') AS clip_failed,
                COUNT(*) FILTER (WHERE status = 'FAILED' OR raw_phase = 'FAILED'
                    OR audio_phase = 'FAILED' OR visuals_phase = 'FAILED'
                    OR transcript_phase = 'FAILED' OR clip_phase = 'FAILED') AS failed
            FROM videos
        """)
        )[0]
        tracking = (
            await self.rows("""
            SELECT COUNT(*) AS total,
                COUNT(*) FILTER (WHERE next_track_at <= NOW()) AS due,
                MAX(last_tracked_at) AS last_tracked_at
            FROM watchlist
        """)
        )[0]
        timeline = await self.rows("""
            WITH discovered AS (
                SELECT date_trunc('hour', discovered_at) AS hour, COUNT(*) AS count
                FROM videos
                WHERE discovered_at >= date_trunc('hour', NOW()) - INTERVAL '23 hours'
                GROUP BY 1
            )
            SELECT hours.hour, COALESCE(d.count, 0) AS count
            FROM generate_series(date_trunc('hour', NOW()) - INTERVAL '23 hours',
                date_trunc('hour', NOW()), INTERVAL '1 hour') AS hours(hour)
            LEFT JOIN discovered d ON d.hour = hours.hour
            ORDER BY hours.hour
        """)
        return {
            "counts": counts,
            "tracking": tracking,
            "timeline": timeline,
            "stages": [
                {"name": s, "done": counts[f"{s}_done"], "failed": counts[f"{s}_failed"]}
                for s in STAGES
            ],
        }

    async def videos(
        self,
        search: str = "",
        status: str = "",
        stage: str = "",
        page: int = 1,
        page_size: int = 25,
    ) -> dict[str, Any]:
        if status and status not in STATUSES:
            raise ValueError("Unknown lifecycle status")
        if stage and stage not in STAGES:
            raise ValueError("Unknown extraction stage")
        if not 1 <= page <= 1000 or not 1 <= page_size <= 50 or len(search) > 120:
            raise ValueError("Invalid pagination or search")
        conditions: list[str] = []
        params: list[Any] = []
        if search:
            # Treat SQL pattern characters as literal search input.
            term = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            conditions.append("(v.title ILIKE %s OR v.id = %s OR c.title ILIKE %s)")
            params.extend((f"%{term}%", search, f"%{term}%"))
        if status:
            conditions.append("v.status = %s")
            params.append(status)
        if stage:
            conditions.append(f"v.{stage}_phase = 'FAILED'")
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        # Only the validated stage name and fixed condition strings enter SQL.
        base = f"FROM videos v LEFT JOIN channels c ON c.id = v.channel_id {where}"
        total = (await self.rows(f"SELECT COUNT(*) AS total {base}", tuple(params)))[0]["total"]
        rows = await self.rows(
            f"""
            SELECT v.id, v.title, c.title AS channel, v.duration, v.status,
                v.retired_at, v.retirement_reason,
                v.discovered_at, v.published_at, v.raw_phase, v.audio_phase,
                v.visuals_phase, v.transcript_phase, v.clip_phase,
                w.last_views AS views, w.tracking_tier
            FROM videos v LEFT JOIN channels c ON c.id = v.channel_id
                LEFT JOIN watchlist w ON w.video_id = v.id
            {where}
            ORDER BY v.discovered_at DESC, v.id DESC LIMIT %s OFFSET %s
        """,
            (*params, page_size, (page - 1) * page_size),
        )
        return {"items": rows, "total": total, "page": page, "page_size": page_size}

    async def video(self, video_id: str) -> dict[str, Any] | None:
        rows = await self.rows(
            """
            SELECT v.id, v.title, c.title AS channel, v.duration, v.status,
                v.retired_at, v.retirement_reason,
                v.published_at, v.discovered_at, v.last_updated_at, v.tags,
                v.raw_phase, v.audio_phase, v.visuals_phase, v.transcript_phase, v.clip_phase,
                w.last_views AS views, w.last_likes AS likes,
                w.last_comment_count AS comments, w.tracking_tier, w.next_track_at,
                t.language, (t.vault_uri IS NOT NULL) AS transcript_vaulted,
                LEFT(t.content::text, 24000) AS transcript_preview,
                (length(t.content::text) > 24000) AS transcript_truncated
            FROM videos v LEFT JOIN channels c ON c.id = v.channel_id
                LEFT JOIN watchlist w ON w.video_id = v.id
                LEFT JOIN transcripts t ON t.video_id = v.id
            WHERE v.id = %s
        """,
            (video_id,),
        )
        if not rows:
            return None
        stats = await self.rows(
            """
            SELECT timestamp, views, likes, comment_count FROM video_stats_log
            WHERE video_id = %s ORDER BY timestamp DESC LIMIT 30
        """,
            (video_id,),
        )
        return {**rows[0], "stats": list(reversed(stats))}

    async def events(self) -> list[dict[str, Any]]:
        # Event payloads can include exception messages or tokens. Never expose them.
        return await self.rows("""
            SELECT event_type, entity_id, created_at FROM system_events
            ORDER BY created_at DESC, id DESC LIMIT 80
        """)

    async def queries(self) -> list[dict[str, Any]]:
        return await self.rows("""
            SELECT query_term, priority, mention_count, status, last_searched_at,
                result_count_total FROM search_queue
            ORDER BY priority DESC, mention_count DESC, id LIMIT 100
        """)
