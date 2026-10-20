"""Stages transcripts locally and tracks which still need flushing to the vault."""

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from atlas.adapters import DatabaseAdapter
from tiered_storage import StagedItem, StoredItem


class TranscriptRepository(DatabaseAdapter):
    """DAO for transcript staging and vault-write pending tracking."""

    async def record_transcript(
        self,
        video_id: str,
        vault_uri: str | None,
        language: str = "en",
        content_json: Any | None = None,
    ) -> None:
        """Stage a transcript locally and queue the vault write.

        Inserts the transcript row and marks ``vault_write_pending`` so the
        janitor flushes it in batched commits; ``vault_uri`` stays NULL until
        that flush succeeds. Pass ``vault_uri`` only for the janitor's
        post-flush update.

        Audio is NOT staged here — the singer writes ``media/audio/{prefix}/{id}.opus``
        straight to the vault, so the legacy ``audio_pending`` staging column
        is unused.
        """
        content_param: Any = json.dumps(content_json) if content_json is not None else None
        async with self._cursor() as cur:
            # All transcript writers lock the parent first. Archival uses the
            # same order, so it cannot delete a transcript staged concurrently.
            await cur.execute(
                "SELECT id FROM videos WHERE id = %s AND status <> 'ARCHIVED' FOR UPDATE",
                (video_id,),
            )
            if await cur.fetchone() is None:
                raise ValueError("Cannot stage a transcript for a missing or archived video")
            await cur.execute(
                """
                INSERT INTO transcripts (video_id, language, vault_uri, content)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (video_id) DO UPDATE
                    SET vault_uri = EXCLUDED.vault_uri,
                        language = EXCLUDED.language,
                        content = EXCLUDED.content
                """,
                (video_id, language, vault_uri, content_param),
            )
            if vault_uri is None:
                await cur.execute(
                    "UPDATE videos SET vault_write_pending = TRUE WHERE id = %s",
                    (video_id,),
                )
            await cur.connection.commit()

    async def pending(self, limit: int = 50) -> list[StagedItem]:
        """Select bounded work using the pending and missing-pointer indexes.

        Selection is not a lease. Immutable cold paths and version-conditional
        finalization make an overlapping handoff safe without keeping DB locks
        open across a remote upload.
        """
        if limit < 1:
            raise ValueError("Selection limit must be positive")
        rows = await self._fetch_all(
            """
            WITH candidates AS (
                (SELECT v.id, v.discovered_at, 0 AS priority FROM videos v
                 JOIN transcripts t ON t.video_id = v.id
                 WHERE v.has_transcript AND v.vault_write_pending
                   AND v.status <> 'ARCHIVED' AND t.content IS NOT NULL
                 ORDER BY v.discovered_at, v.id LIMIT %s)
                UNION
                (SELECT v.id, v.discovered_at, 0 AS priority
                 FROM transcripts t JOIN videos v ON v.id = t.video_id
                 WHERE t.vault_uri IS NULL AND t.content IS NOT NULL
                   AND v.has_transcript AND v.vault_write_pending IS NOT TRUE
                   AND v.status <> 'ARCHIVED'
                 ORDER BY v.discovered_at, v.id LIMIT %s)
                UNION
                (SELECT v.id, v.discovered_at, 1 AS priority FROM transcripts t
                 JOIN videos v ON v.id = t.video_id
                 WHERE t.vault_uri IS NOT NULL AND t.content IS NOT NULL
                   AND v.vault_write_pending IS NOT TRUE AND v.status <> 'ARCHIVED'
                 ORDER BY t.video_id LIMIT %s)
            )
            SELECT v.id, t.content AS transcript, t.xmin::text AS version,
                   t.vault_uri AS existing_uri
            FROM candidates c JOIN videos v ON v.id = c.id
            JOIN transcripts t ON t.video_id = v.id
            WHERE t.content IS NOT NULL AND v.status <> 'ARCHIVED'
            ORDER BY c.priority, c.discovered_at, c.id LIMIT %s
            """,
            (limit, limit, limit, limit),
        )
        return [
            StagedItem(
                key=row["id"],
                version=row["version"],
                payload=json.dumps(
                    row["transcript"],
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8"),
                namespace=f"transcripts/{row['id'][:2]}/{row['id']}",
                suffix=".json",
                existing_uri=row["existing_uri"],
            )
            for row in rows
        ]

    async def finalize(self, items: Sequence[StoredItem]) -> set[str]:
        """Finalize a verified batch in one transaction, preserving newer content."""
        if not items:
            return set()
        if any(not stored.uri or not stored.uri.strip() for stored in items):
            raise ValueError("vault_uri is required before staged transcript cleanup")
        keys = [stored.item.key for stored in items]
        versions = [stored.item.version for stored in items]
        uris = [stored.uri for stored in items]
        async with self._connection() as conn, conn.transaction():
            # Stable ordering avoids deadlocks between overlapping batches and
            # coordinates with record_transcript and video archival.
            await conn.execute(
                """SELECT id FROM videos WHERE id = ANY(%s) AND status <> 'ARCHIVED'
                   ORDER BY id FOR UPDATE""",
                (keys,),
            )
            cur = await conn.execute(
                """
                WITH staged AS (
                    SELECT * FROM unnest(%s::text[], %s::text[], %s::text[])
                    AS s(video_id, version, uri)
                ), transcript_written AS (
                    UPDATE transcripts t SET vault_uri = s.uri, content = NULL
                    FROM staged s, videos v
                    WHERE t.video_id = s.video_id AND v.id = t.video_id
                      AND v.status <> 'ARCHIVED' AND t.xmin::text = s.version
                      AND t.content IS NOT NULL
                    RETURNING t.video_id
                )
                UPDATE videos AS v SET vault_write_pending = FALSE,
                    last_updated_at = %s,
                    status = CASE
                        WHEN has_transcript AND has_audio AND has_visuals THEN 'PROCESSED'
                        ELSE status END
                WHERE v.id IN (SELECT video_id FROM transcript_written)
                RETURNING v.id
                """,
                (keys, versions, uris, datetime.now(UTC)),
            )
            return {row[0] for row in await cur.fetchall()}
