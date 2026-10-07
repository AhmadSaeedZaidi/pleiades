import json
import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from atlas.adapters import DatabaseAdapter
from atlas.config import settings
from atlas.models.video import Video
from atlas.vault_io import run_vault_io
from tiered_storage import StagedItem, StoredItem, promote

if TYPE_CHECKING:
    from atlas.repositories.video.protocols import VideoRepositoryProtocol

logger = logging.getLogger("atlas.repositories.video.janitor")

_ARCHIVAL_BATCH_SIZE = 100

# A video is only safe to archive (drop from the hot tier) once its transcript
# content is confirmed stored in the vault. Otherwise the hand-off phase would
# DELETE the transcript row and zero has_* flags with the content never having
# left the hot DB — silently destroying it (and making the heartbeat report
# fabricated data loss). This clause excludes any video that:
#   - still has a vault flush pending (scheduler's vault_flush_task hasn't run), or
#   - has a transcript row whose content was never written to the vault.
_VAULT_SAFE_CLAUSE = """
  AND (vault_write_pending IS NOT TRUE)
  AND NOT EXISTS (
      SELECT 1 FROM transcripts t
      WHERE t.video_id = videos.id
        AND (t.vault_uri IS NULL OR t.content IS NOT NULL)
  )
"""


class VideoJanitorMixin(DatabaseAdapter):
    async def cleanup_unavailable_videos(
        self, batch_size: int = 50, dry_run: bool = False
    ) -> dict[str, list[str]]:
        """Park repeatedly unavailable sources; preserve every artifact and phase.

        Tracker owns availability observations and monthly rechecks. This method
        makes no outbound requests. Both parent and watchlist rows stay locked
        from selection through mutation, excluding concurrent claims/tracking.
        A successful observation restores eligibility, never resets failures.
        """
        if not 1 <= batch_size <= 100:
            raise ValueError("Cleanup batch_size must be between 1 and 100")
        async with self._connection() as conn, conn.transaction():
            if dry_run:
                await conn.execute("SET TRANSACTION READ ONLY")
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            await conn.execute("SET LOCAL lock_timeout = '1s'")
            lock = "" if dry_run else "FOR UPDATE OF v, w SKIP LOCKED"
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""SELECT v.id FROM videos v JOIN watchlist w ON w.video_id=v.id
                        WHERE v.retired_at IS NOT NULL AND w.unavailable_count=0
                          AND w.last_tracked_at > v.retired_at
                        ORDER BY v.retired_at, v.id LIMIT %s {lock}""",
                    (batch_size,),
                )
                restored = [row[0] for row in await cur.fetchall()]
                await cur.execute(
                    f"""SELECT v.id FROM videos v JOIN watchlist w ON w.video_id=v.id
                        WHERE v.retired_at IS NULL
                          AND v.status IN ('PENDING','PROCESSING','FAILED')
                          AND v.fetched IS NOT TRUE AND v.raw_uri IS NULL
                          AND w.tracking_tier='DORMANT' AND w.unavailable_count>=3
                          AND COALESCE(w.last_tracked_at,w.created_at)
                              < NOW() - INTERVAL '7 days'
                          AND NOT ('PROCESSING'=ANY(ARRAY[v.raw_phase::text,
                              v.audio_phase::text,v.visuals_phase::text,
                              v.transcript_phase::text,v.clip_phase::text]))
                        ORDER BY (v.status='FAILED') DESC, v.discovered_at, v.id
                        LIMIT %s {lock}""",
                    (batch_size,),
                )
                retired = [row[0] for row in await cur.fetchall()]
            if not dry_run:
                if restored:
                    await conn.execute(
                        """UPDATE videos SET retired_at=NULL, retirement_reason=NULL
                           WHERE id=ANY(%s)""",
                        (restored,),
                    )
                if retired:
                    await conn.execute(
                        """UPDATE videos SET retired_at=NOW(),
                           retirement_reason='repeated_api_unavailability'
                           WHERE id=ANY(%s)""",
                        (retired,),
                    )
        return {"retired_ids": retired, "restored_ids": restored}

    async def sweep_archivable(self, batch_size: int = _ARCHIVAL_BATCH_SIZE) -> list[Video]:
        cutoff = datetime.now(UTC) - timedelta(days=settings.JANITOR_RETENTION_DAYS)
        rows = await self._fetch_all(
            f"""
            SELECT * FROM videos
            WHERE status = 'PROCESSED'
              AND last_updated_at < %s
              {_VAULT_SAFE_CLAUSE}
            ORDER BY last_updated_at ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (cutoff, batch_size),
        )
        return [Video.model_validate(r) for r in rows]

    async def count_archivable(self) -> int:
        cutoff = datetime.now(UTC) - timedelta(days=settings.JANITOR_RETENTION_DAYS)
        row = await self._fetch_one(
            f"""
            SELECT COUNT(*) as total FROM videos
            WHERE status = 'PROCESSED'
              AND last_updated_at < %s
              {_VAULT_SAFE_CLAUSE}
            """,
            (cutoff,),
        )
        return int(row["total"]) if row else 0

    async def count_videos(self) -> int:
        """Return the total number of videos in the corpus (fast estimate).

        Uses the planner's ``reltuples`` statistic so the query stays O(1) even
        as the corpus grows to millions of rows; falls back to an exact count
        when statistics are unavailable (e.g. a freshly-loaded table).
        """
        row = await self._fetch_one(
            "SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = 'videos'"
        )
        estimate = int(row["estimate"]) if row and row["estimate"] is not None else 0
        if estimate > 0:
            return estimate
        exact = await self._fetch_one("SELECT COUNT(*) AS total FROM videos")
        return int(exact["total"]) if exact else 0

    # ── Janitor: Hand-off Phase (serialize + vault + verify + purge) ──────

    async def archive_video_batch(
        self: "VideoRepositoryProtocol", videos: list[Video], dry_run: bool = False
    ) -> dict[str, Any]:
        """Vault fresh metadata snapshots, verify, then conditionally archive.

        The common handoff engine bounds commits and readback concurrency. Hot
        finalization uses one transaction per batch, rechecking row versions
        and transcript safety under the same parent locks used by Scribe.
        """
        from atlas.events import events
        from atlas.storage import VaultColdStore
        from atlas.vault import get_vault

        if not videos:
            return {"archived": 0, "failed": 0}
        if dry_run:
            return {
                "archived": 0,
                "dry_run": True,
                "would_archive": len(videos),
                "video_ids": [video.id for video in videos],
            }
        ids = list(dict.fromkeys(video.id for video in videos))
        snapshots = await self._fetch_all(
            """SELECT v.*, v.xmin::text AS storage_version, t.vault_uri AS transcript_uri
               FROM videos v LEFT JOIN transcripts t ON t.video_id = v.id
               WHERE v.id = ANY(%s) AND v.status = 'PROCESSED'""",
            (ids,),
        )
        latest = await self.get_latest_stats_batch(ids)
        date_key = datetime.now(UTC).strftime("%Y-%m-%d")
        items: list[StagedItem] = []
        for snapshot in snapshots:
            video = Video.model_validate(snapshot)
            metadata: dict[str, Any] = {
                "id": video.id,
                "channel_id": video.channel_id,
                "title": video.title,
                "published_at": video.published_at.isoformat() if video.published_at else None,
                "duration": video.duration,
                "tags": video.tags,
                "category_id": video.category_id,
                "discovered_at": video.discovered_at.isoformat() if video.discovered_at else None,
                "last_updated_at": video.last_updated_at.isoformat()
                if video.last_updated_at
                else None,
                "has_transcript": video.has_transcript,
                "has_visuals": video.has_visuals,
                "wiki_topics": video.wiki_topics,
                "transcript_uri": snapshot["transcript_uri"],
            }
            if stats := latest.get(video.id):
                metadata["stats"] = {
                    "views": stats.views,
                    "likes": stats.likes,
                    "comment_count": stats.comment_count,
                    "last_tracked_at": stats.timestamp.isoformat() if stats.timestamp else None,
                }
            items.append(
                StagedItem(
                    key=video.id,
                    version=snapshot["storage_version"],
                    payload=json.dumps(
                        metadata,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                        allow_nan=False,
                    ).encode("utf-8"),
                    namespace=f"metadata/{date_key}/{video.id}",
                    suffix=".json",
                )
            )
        cold = VaultColdStore(
            get_vault(),
            heads={item.key: f"metadata/{date_key}/{item.key}/index.json" for item in items},
        )
        result = await promote(items, cold, self._finalize_archives)
        failed_ids = [failure.key for failure in result.failed] + list(result.deferred)
        failed_ids += [key for key in ids if key not in {item.key for item in items}]
        if failed_ids:
            logger.warning(
                "Archival retained %d hot records after failure or concurrent change",
                len(failed_ids),
            )
            await events.emit(
                "janitor.archive_failed",
                "janitor",
                {
                    "failed_ids": failed_ids,
                    "retention_days": settings.JANITOR_RETENTION_DAYS,
                    "failures": [
                        {"id": f.key, "stage": f.stage, "error": f.error} for f in result.failed
                    ],
                },
            )
        return {
            "archived": len(result.promoted),
            "failed": len(failed_ids),
            "failed_ids": failed_ids,
        }

    async def _finalize_archives(self, items: Sequence[StoredItem]) -> set[str]:
        if not items:
            return set()
        ids = [stored.item.key for stored in items]
        versions = [stored.item.version for stored in items]
        async with self._connection() as conn, conn.transaction():
            await conn.execute(
                "SELECT id FROM videos WHERE id = ANY(%s) ORDER BY id FOR UPDATE",
                (ids,),
            )
            cur = await conn.execute(
                """
                UPDATE videos v SET status = 'ARCHIVED', archived_at = %s,
                    has_transcript = FALSE, has_audio = FALSE, has_visuals = FALSE
                FROM unnest(%s::text[], %s::text[]) AS staged(id, version)
                WHERE v.id = staged.id AND v.xmin::text = staged.version
                  AND v.status = 'PROCESSED' AND v.vault_write_pending IS NOT TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM transcripts t WHERE t.video_id = v.id
                      AND (t.vault_uri IS NULL OR t.content IS NOT NULL)
                  )
                RETURNING v.id
                """,
                (datetime.now(UTC), ids, versions),
            )
            archived = {row[0] for row in await cur.fetchall()}
            if archived:
                await conn.execute(
                    "DELETE FROM transcripts WHERE video_id = ANY(%s)", (sorted(archived),)
                )
            # Recent stats and durable tracking state remain hot.
            return archived

    async def archive_cold_stats(self, retention_days: int = 7, batch_size: int = 5000) -> int:
        """Move old ``video_stats_log`` rows to the vault and purge them from hot."""
        from atlas.vault import get_vault, metrics_batch_id, metrics_partition

        if retention_days < 0 or batch_size < 1:
            raise ValueError("Retention must be non-negative and batch size positive")

        cutoff_date = datetime.now(UTC) - timedelta(days=retention_days)

        async with self._connection() as conn, conn.transaction():
            cur = await conn.execute(
                """
                    SELECT video_id, views, likes, comment_count, timestamp, xmin::text AS version
                    FROM video_stats_log
                    WHERE timestamp < %s
                    ORDER BY timestamp ASC, video_id ASC
                    LIMIT %s
                    """,
                (cutoff_date, batch_size),
            )
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = await cur.fetchall()

        if not rows:
            return 0

        stats = [dict(zip(columns, row, strict=True)) for row in rows]

        cold_rows: list[dict[str, Any]] = []
        versions: list[str] = []
        for stat in stats:
            ts = stat["timestamp"]
            iso_ts = ts.isoformat() if isinstance(ts, datetime) else ts
            cold_rows.append(
                {
                    "video_id": stat["video_id"],
                    "views": stat["views"],
                    "likes": stat["likes"],
                    "comment_count": stat["comment_count"],
                    "timestamp": iso_ts,
                }
            )
            versions.append(stat["version"])

        # Step 2: upload to the vault off the event loop (no DB lock held).
        v = get_vault()
        manifest = await run_vault_io(lambda: self._append_cold_metrics(v, cold_rows))
        expected_id = metrics_batch_id(cold_rows)
        expected_partitions = {metrics_partition(row["timestamp"]) for row in cold_rows}
        actual_partitions = {(file.date, file.hour) for file in manifest.files}
        if (
            manifest.batch_id != expected_id
            or manifest.row_count != len(cold_rows)
            or actual_partitions != expected_partitions
            or sum(file.row_count for file in manifest.files) != len(cold_rows)
        ):
            raise RuntimeError(f"Metrics manifest verification failed for batch {expected_id}")

        video_ids = [row["video_id"] for row in cold_rows]
        timestamps = [row["timestamp"] for row in cold_rows]
        deleted = await self._delete_cold_stats(video_ids, timestamps, versions)

        logger.info(f"Archived and purged {deleted} stats from hot tier")
        return deleted

    @staticmethod
    def _append_cold_metrics(vault: Any, rows: list[dict[str, Any]]) -> Any:
        return vault.store_metrics_batch(rows)

    async def _delete_cold_stats(
        self, video_ids: list[str], timestamps: list[Any], versions: list[str]
    ) -> int:
        # Pairwise unnest is a guaranteed zip on all PG versions.
        async with self._connection() as conn, conn.transaction():
            cur = await conn.execute(
                """
                DELETE FROM video_stats_log s
                USING unnest(%s::text[], %s::timestamptz[], %s::text[])
                    AS archived(video_id, timestamp, version)
                WHERE s.video_id = archived.video_id AND s.timestamp = archived.timestamp
                  AND s.xmin::text = archived.version
                RETURNING s.video_id, s.timestamp
                """,
                (video_ids, timestamps, versions),
            )
            deleted_rows = await cur.fetchall()
        return len(deleted_rows)

    # ── Hard delete of fully-archived videos past retention ───────────────

    async def run_janitor(self, dry_run: bool = False) -> dict[str, Any]:
        if not settings.JANITOR_ENABLED:
            return {"deleted": 0, "reason": "disabled"}

        cutoff_date = datetime.now(UTC) - timedelta(days=settings.JANITOR_RETENTION_DAYS)
        safety_clause = ""
        if settings.JANITOR_SAFETY_CHECK:
            safety_clause = "AND (has_transcript = TRUE OR has_visuals = TRUE)"

        # Only PROCESSED and ARCHIVED rows are eligible for hard deletion.
        count_result = await self._fetch_one(
            f"""
            SELECT COUNT(*) as total
            FROM videos
            WHERE discovered_at < %s
              AND status IN ('PROCESSED', 'ARCHIVED')
              {safety_clause}
            """,
            (cutoff_date,),
        )
        total_to_delete = int(count_result["total"]) if count_result else 0

        if total_to_delete == 0:
            return {"deleted": 0, "reason": "none_eligible"}

        if dry_run:
            return {"deleted": 0, "dry_run": True, "would_delete": total_to_delete}

        await self._execute(
            f"""
            DELETE FROM videos
            WHERE discovered_at < %s
              AND status IN ('PROCESSED', 'ARCHIVED')
              {safety_clause}
            """,
            (cutoff_date,),
        )
        return {
            "deleted": total_to_delete,
            "cutoff_date": cutoff_date.isoformat(),
            "retention_days": settings.JANITOR_RETENTION_DAYS,
            "safety_check_enabled": settings.JANITOR_SAFETY_CHECK,
        }
