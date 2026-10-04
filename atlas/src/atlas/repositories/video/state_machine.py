import asyncio
import logging
from datetime import UTC, datetime

from atlas.adapters import DatabaseAdapter
from atlas.models.video import Video

logger = logging.getLogger("atlas.repositories.video.state_machine")


class VideoStateMixin(DatabaseAdapter):
    async def claim_scribe_batch(self, batch_size: int = 10) -> list[Video]:
        rows = await self._fetch_all(
            """
            UPDATE videos SET status = 'PROCESSING', transcript_phase = 'PROCESSING'
            WHERE id IN (
                SELECT id FROM videos
                WHERE status IN ('PENDING', 'PROCESSING')
                  AND has_transcript = FALSE
                  AND transcript_phase <> 'FAILED'
                ORDER BY discovered_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            """,
            (batch_size,),
        )
        return [Video.model_validate(r) for r in rows]

    async def claim_painter_batch(self, batch_size: int = 5) -> list[Video]:
        # Painter consumes the vault-stored raw, so it only processes already-fetched videos.
        rows = await self._fetch_all(
            """
            UPDATE videos SET status = 'PROCESSING', visuals_phase = 'PROCESSING'
            WHERE id IN (
                SELECT id FROM videos
                WHERE status IN ('PENDING', 'PROCESSING')
                  AND fetched = TRUE
                  AND has_visuals = FALSE
                  AND visuals_phase <> 'FAILED'
                ORDER BY discovered_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            """,
            (batch_size,),
        )
        return [Video.model_validate(r) for r in rows]

    async def claim_streamer_batch(self, batch_size: int = 5) -> list[Video]:
        """Claim videos whose YouTube source has not yet been fetched.

        The streamer does the network pull; the singer later extracts the audio
        from the stored raw artifact.

        Orders by ``discovered_at DESC`` (newest-first) so fresh videos — far
        more likely to be available — are fetched before old/buried content.
        Also skips videos whose ``last_updated_at`` is within the last 15
        minutes (a retry cooldown), preventing a tight infinite loop on
        permanently-unavailable videos (removed/private/geo-blocked).
        """
        rows = await self._fetch_all(
            """
            UPDATE videos SET status = 'PROCESSING', raw_phase = 'PROCESSING'
            WHERE id IN (
                SELECT id FROM videos
                WHERE status IN ('PENDING', 'PROCESSING')
                  AND fetched = FALSE
                  AND raw_phase <> 'FAILED'
                  AND (last_updated_at IS NULL
                       OR last_updated_at < NOW() - INTERVAL '15 minutes')
                ORDER BY discovered_at DESC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            """,
            (batch_size,),
        )
        return [Video.model_validate(r) for r in rows]

    async def claim_singer_batch(self, batch_size: int = 5) -> list[Video]:
        """Claim fetched videos whose audio is not stored yet.

        The singer extracts the speech track locally (no YouTube rate limit),
        stores ``media/audio/{prefix}/{id}.opus``, and flips ``has_audio``.
        """
        rows = await self._fetch_all(
            """
            UPDATE videos SET status = 'PROCESSING', audio_phase = 'PROCESSING'
            WHERE id IN (
                SELECT id FROM videos
                WHERE status IN ('PENDING', 'PROCESSING', 'PROCESSED')
                  AND fetched = TRUE
                  AND has_audio = FALSE
                  AND audio_phase <> 'FAILED'
                ORDER BY discovered_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            """,
            (batch_size,),
        )
        return [Video.model_validate(r) for r in rows]

    async def claim_muralist_batch(self, batch_size: int = 5) -> list[Video]:
        rows = await self._fetch_all(
            """
            UPDATE videos SET status = 'PROCESSING', clip_phase = 'PROCESSING'
            WHERE id IN (
                SELECT id FROM videos
                WHERE status IN ('PENDING', 'PROCESSING')
                  AND has_video = FALSE
                  AND raw_uri IS NOT NULL
                  AND clip_phase <> 'FAILED'
                ORDER BY discovered_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *
            """,
            (batch_size,),
        )
        return [Video.model_validate(r) for r in rows]

    async def mark_transcript_safe(self, video_id: str) -> None:
        now = datetime.now(UTC)
        await self._execute(
            """
            UPDATE videos
            SET has_transcript = TRUE,
                last_updated_at = %s,
                -- PROCESSED requires the full audio+visuals+transcript set; do not
                -- latch it until audio is also present (scribe is caption-first and
                -- runs in parallel with the singer, so audio may finish after here).
                status = CASE WHEN has_visuals AND has_audio THEN 'PROCESSED' ELSE status END
            WHERE id = %s AND transcript_phase <> 'DONE'
            """,
            (now, video_id),
        )

    async def mark_visuals_safe(self, video_id: str) -> None:
        now = datetime.now(UTC)
        await self._execute(
            """
            UPDATE videos
            SET has_visuals = TRUE,
                last_updated_at = %s,
                -- PROCESSED requires the full audio+visuals+transcript set; do not
                -- latch it until audio is also present (the singer may finish after
                -- the painter, so audio is latched separately in mark_audio_safe).
                status = CASE WHEN has_transcript AND has_audio THEN 'PROCESSED' ELSE status END
            WHERE id = %s AND visuals_phase <> 'DONE'
            """,
            (now, video_id),
        )

    async def mark_fetched(
        self,
        video_id: str,
        raw_uri: str,
    ) -> None:
        """Record that the YouTube source was fetched and stored at *raw_uri*."""
        now = datetime.now(UTC)
        await self._execute(
            """
            UPDATE videos
            SET fetched = TRUE,
                raw_uri = %s,
                raw_stored_at = %s,
                last_updated_at = %s
            WHERE id = %s AND raw_phase <> 'DONE'
            """,
            (raw_uri, now, now, video_id),
        )

    async def reclaim_raw_if_complete(self, video_id: str) -> int:
        """Delete the raw artifact once every raw-consuming step has joined.

        Returns files reclaimed (0/1).
        """
        from atlas.config import settings

        row = await self._fetch_one(
            """
            SELECT raw_uri, audio_phase, visuals_phase, clip_phase, raw_stored_at
            FROM videos WHERE id = %s
            """,
            (video_id,),
        )
        # Join barrier: reclaim only once both mandatory consumers (audio+visuals)
        # are DONE, so we never pull the input out from under a pending consumer.
        if not row or not row["raw_uri"]:
            return 0
        if not (row["audio_phase"] == "DONE" and row["visuals_phase"] == "DONE"):
            return 0

        # Reclaim when the clip is DONE, or once the raw has aged past RAW_TTL_HOURS
        # (a NULL raw_stored_at is only reclaimed via clip DONE).
        clip_done = row["clip_phase"] == "DONE"
        raw_stored_at = row["raw_stored_at"]
        raw_age_hours = (
            (datetime.now(UTC) - raw_stored_at).total_seconds() / 3600.0
            if raw_stored_at is not None
            else None
        )
        ttl_hours = settings.RAW_TTL_HOURS
        if not clip_done and not (raw_age_hours is not None and raw_age_hours > ttl_hours):
            return 0

        from atlas.vault import get_vault, legacy_meta_path, meta_path

        raw_uri = row["raw_uri"]
        # Existing rows use raw/<file> with flat metadata; new rows are sharded.
        is_sharded = len(str(raw_uri).split("/")) > 2
        metadata_uri = meta_path(video_id) if is_sharded else legacy_meta_path(video_id)
        paths = [raw_uri, metadata_uri]
        try:
            deleted = await asyncio.to_thread(get_vault().delete_files, paths)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"raw reclamation failed for {video_id}: {e}")
            return 0
        await self._execute("UPDATE videos SET raw_uri = NULL WHERE id = %s", (video_id,))
        logger.info(f"Reclaimed {deleted} raw artifacts for {video_id}")
        return deleted

    _STEP_COLUMNS = frozenset({"raw", "audio", "visuals", "transcript", "clip"})

    async def mark_audio_safe(self, video_id: str) -> None:
        now = datetime.now(UTC)
        await self._execute(
            """
            UPDATE videos
            SET has_audio = TRUE,
                last_updated_at = %s,
                -- If audio was the last missing artifact, latch PROCESSED now. This
                -- closes the fan-out race: scribe/painter may have already set
                -- has_transcript/has_visuals but could not latch PROCESSED without audio.
                status = CASE WHEN has_visuals AND has_transcript THEN 'PROCESSED' ELSE status END
            WHERE id = %s AND audio_phase <> 'DONE'
            """,
            (now, video_id),
        )

    async def mark_video_safe(self, video_id: str) -> None:
        now = datetime.now(UTC)
        await self._execute(
            """
            UPDATE videos
            SET has_video = TRUE,
                last_updated_at = %s
            WHERE id = %s AND clip_phase <> 'DONE'
            """,
            (now, video_id),
        )

    async def repaint_all_videos(self) -> int:
        """Reset every video to PENDING and clear has_visuals so the Painter
        re-collects frames (used after a frame-corruption fix)."""
        now = datetime.now(UTC)
        rows = await self._fetch_all(
            """
            UPDATE videos
            SET status = 'PENDING', has_visuals = FALSE, last_updated_at = %s
            RETURNING id
            """,
            (now,),
        )
        return len(rows)

    async def reset_failed_to_pending(self) -> int:
        """Clear every terminally-failed stage back to PENDING so it is retried.

        Failures live in the per-step phase columns, not in the row-level
        ``status`` (see ``mark_step_failed``), so recovery has to look at the
        phases. Any video with at least one 'FAILED' phase is released, and its
        row-level status is recomputed from the artifacts that actually exist so
        a fully-processed video is not needlessly demoted to PENDING (which
        would hide it from the Janitor sweep).
        """
        now = datetime.now(UTC)
        rows = await self._fetch_all(
            """
            UPDATE videos
            SET raw_phase = CASE
                    WHEN raw_phase = 'FAILED' THEN 'PENDING'::step_phase
                    ELSE raw_phase END,
                audio_phase = CASE
                    WHEN audio_phase = 'FAILED' THEN 'PENDING'::step_phase
                    ELSE audio_phase END,
                visuals_phase = CASE
                    WHEN visuals_phase = 'FAILED' THEN 'PENDING'::step_phase
                    ELSE visuals_phase END,
                transcript_phase = CASE
                    WHEN transcript_phase = 'FAILED' THEN 'PENDING'::step_phase
                    ELSE transcript_phase END,
                clip_phase = CASE
                    WHEN clip_phase = 'FAILED' THEN 'PENDING'::step_phase
                    ELSE clip_phase END,
                has_visuals = CASE WHEN visuals_phase = 'FAILED' THEN FALSE ELSE has_visuals END,
                status = CASE
                    WHEN has_audio AND has_visuals AND has_transcript THEN 'PROCESSED'
                    WHEN archived_at IS NOT NULL THEN 'ARCHIVED'
                    ELSE 'PENDING'
                END,
                last_updated_at = %s
            WHERE raw_phase = 'FAILED'
               OR audio_phase = 'FAILED'
               OR visuals_phase = 'FAILED'
               OR transcript_phase = 'FAILED'
               OR clip_phase = 'FAILED'
            RETURNING id
            """,
            (now,),
        )
        return len(rows)

    async def count_failed_steps(self) -> dict[str, int]:
        """Per-step count of terminally failed videos, for operator reporting.

        The row-level ``status`` no longer carries a 'FAILED' value, so any
        dashboard or alert that used to read ``status = 'FAILED'`` has to count
        the phase columns instead.
        """
        rows = await self._fetch_all(
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
        row = rows[0] if rows else {}
        return {step: int(row.get(step, 0) or 0) for step in self._STEP_COLUMNS}

    async def mark_done(self, video_id: str) -> None:
        now = datetime.now(UTC)
        await self._execute(
            """
            UPDATE videos
            SET status = 'PROCESSED', last_updated_at = %s
            WHERE id = %s
            """,
            (now, video_id),
        )

    async def _release_step_to_pending(self, video_id: str, step: str) -> None:
        """Release only the current stage; preserve lifecycle and sibling artifacts."""
        if step not in self._STEP_COLUMNS:
            raise ValueError(f"Unknown pipeline step: {step!r}")
        await self._execute(
            f"UPDATE videos SET {step}_phase = 'PENDING', last_updated_at = %s "
            f"WHERE id = %s AND {step}_phase = 'PROCESSING' AND status <> 'ARCHIVED'",
            (datetime.now(UTC), video_id),
        )

    async def release_raw_to_pending(self, video_id: str) -> None:
        """Release a Streamer claim for retry without demoting lifecycle status."""
        await self._release_step_to_pending(video_id, "raw")

    async def release_audio_to_pending(self, video_id: str) -> None:
        """Release a Singer claim while preserving the fetched source."""
        await self._release_step_to_pending(video_id, "audio")

    async def release_visuals_to_pending(self, video_id: str) -> None:
        """Release a Painter claim while preserving the fetched source."""
        await self._release_step_to_pending(video_id, "visuals")

    async def release_clip_to_pending(self, video_id: str) -> None:
        """Release a Muralist claim while preserving other artifact stages."""
        await self._release_step_to_pending(video_id, "clip")

    async def release_transcript_to_pending(self, video_id: str) -> None:
        """Release a Scribe claim without requeueing a source-media download."""
        await self._release_step_to_pending(video_id, "transcript")

    async def mark_step_failed(self, video_id: str, step: str) -> None:
        """Fail one pipeline stage without touching the other four.

        This deliberately records the failure ONLY in ``{step}_phase`` and leaves
        the row-level ``status`` alone. ``status`` used to be set to 'FAILED'
        here, which was a whole-row terminal state: every claim query filters on
        ``status IN ('PENDING', 'PROCESSING'[, 'PROCESSED'])``, so one transient
        error in one agent (a vault hiccup in the Painter, a yt-dlp timeout in
        the Streamer) silently and permanently removed the video from the other
        four consumers. Nothing in maia ever recovered it.

        Per-step isolation is what the phase columns are for. The matching claim
        query excludes rows whose own phase is 'FAILED', so a genuinely dead
        stage stops being retried while the rest of the pipeline still advances.
        """
        if step not in self._STEP_COLUMNS:
            raise ValueError(f"Unknown pipeline step: {step!r}")
        now = datetime.now(UTC)
        await self._execute(
            f"UPDATE videos SET {step}_phase = 'FAILED', last_updated_at = %s "
            f"WHERE id = %s AND {step}_phase = 'PROCESSING'",
            (now, video_id),
        )

    async def retry_failed_audio(self, video_ids: list[str]) -> list[str]:
        """Retry a bounded, explicitly diagnosed audio cohort without touching artifacts.

        Callers must establish that the selected failures are retryable. Legacy
        whole-video failures and completed/changed audio stages remain untouched.
        """
        ids = list(dict.fromkeys(video_ids))
        if len(ids) > 100:
            raise ValueError("Audio recovery requires at most 100 explicit IDs")
        if not ids:
            return []
        rows = await self._fetch_all(
            """UPDATE videos SET audio_phase='PENDING', last_updated_at=now()
               WHERE id=ANY(%s) AND status IN ('PENDING','PROCESSING')
                 AND audio_phase='FAILED' AND has_audio=FALSE
                 AND fetched=TRUE AND raw_phase='DONE' AND raw_uri IS NOT NULL
               RETURNING id""",
            (ids,),
        )
        return [row["id"] for row in rows]

    async def unmark_transcript(self, video_id: str) -> None:
        """Revert a video to needing a transcript so the Scribe re-extracts it
        (has_visuals left untouched)."""
        now = datetime.now(UTC)
        await self._execute(
            """
            UPDATE videos
            SET has_transcript = FALSE, status = 'PENDING', last_updated_at = %s
            WHERE id = %s
            """,
            (now, video_id),
        )

    async def find_transcript_video_ids(self, scope: str = "without_visuals") -> list[str]:
        """Return IDs of videos whose transcript should be reset, by *scope*.

        Scopes: ``all`` (every transcript), ``without_visuals``, ``without_audio``,
        ``pending`` (transcript present but still PENDING/unvetted).
        """
        if scope == "all":
            where = "has_transcript = TRUE"
        elif scope == "without_visuals":
            where = "has_transcript = TRUE AND has_visuals = FALSE"
        elif scope == "without_audio":
            where = "has_transcript = TRUE AND has_audio = FALSE"
        elif scope == "pending":
            where = "has_transcript = TRUE AND status = 'PENDING'"
        else:
            raise ValueError(f"Unknown transcript purge scope: {scope!r}")
        rows = await self._fetch_all(f"SELECT id FROM videos WHERE {where}")
        return [r["id"] for r in rows]

    async def unmark_transcripts_batch(self, video_ids: list[str]) -> int:
        """Batch-uncheck transcripts (return to PENDING, clear ``has_transcript``).

        The Scribe re-derives them via upsert (not delete-then-insert), and the
        stale ``vault_write_pending`` flag is cleared so the janitor doesn't flush old content.
        """
        if not video_ids:
            return 0
        now = datetime.now(UTC)
        await self._execute(
            """
            UPDATE videos
            SET has_transcript = FALSE,
                status = 'PENDING',
                vault_write_pending = FALSE,
                last_updated_at = %s
            WHERE id = ANY(%s)
            """,
            (now, video_ids),
        )
        return len(video_ids)

    async def mark_archived(self, video_id: str) -> None:
        now = datetime.now(UTC)
        await self._execute(
            """
            UPDATE videos
            SET status = 'ARCHIVED', archived_at = %s
            WHERE id = %s
            """,
            (now, video_id),
        )
