"""Transactional topic facts and owned, expiring enrichment leases."""

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.rows import dict_row

from atlas.adapters import DatabaseAdapter
from atlas.topics import Topic, resource_topics

# These SQL identifiers come only from the internal allowlist.
_TABLES = {
    "video": ("videos", "video_topics", "video_id"),
    "channel": ("channels", "channel_topics", "channel_id"),
}


async def _write_edges(
    conn: AsyncConnection[Any], kind: str, snapshots: dict[str, list[Topic]], observed_at: datetime
) -> None:
    table, edges, id_column = _TABLES[kind]
    ids = sorted(snapshots)
    unique = {t.url: t for values in snapshots.values() for t in values}
    topics = sorted(unique.values(), key=lambda t: t.url)
    if topics:
        await conn.execute(
            """INSERT INTO knowledge_topics(url,label,language)
               SELECT * FROM unnest(%s::text[],%s::text[],%s::text[])
               ON CONFLICT(url) DO NOTHING""",
            ([t.url for t in topics], [t.label for t in topics], [t.language for t in topics]),
        )
    await conn.execute(f"DELETE FROM {edges} WHERE {id_column} = ANY(%s)", (ids,))
    pairs = [(key, topic.url) for key in ids for topic in snapshots[key]]
    if pairs:
        await conn.execute(
            f"""INSERT INTO {edges}({id_column},topic_url,observed_at)
                SELECT p.id,p.url,%s FROM unnest(%s::text[],%s::text[]) AS p(id,url)
                ON CONFLICT({id_column},topic_url) DO UPDATE
                    SET observed_at=EXCLUDED.observed_at""",
            (observed_at, [p[0] for p in pairs], [p[1] for p in pairs]),
        )
    if kind == "video":
        # Retain the existing Video model's convenient topic projection.
        import json

        await conn.execute(
            """UPDATE videos v SET wiki_topics=ARRAY(SELECT jsonb_array_elements_text(s.urls))
               FROM jsonb_each(%s::jsonb) AS s(id,urls) WHERE v.id=s.id""",
            (json.dumps({key: [t.url for t in snapshots[key]] for key in ids}),),
        )


async def record_topic_snapshot(
    conn: AsyncConnection[Any], kind: str, resource: dict[str, Any]
) -> None:
    """Called inside the ingestion transaction, after its parent row is written."""
    topics = resource_topics(resource)
    if topics is None:
        return
    if kind not in _TABLES:
        raise ValueError("Unknown resource kind")
    resource_id = resource.get("id")
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("Topic snapshot requires a resource id")
    now = datetime.now(UTC)
    await conn.execute(
        """INSERT INTO youtube_topic_sync(kind,resource_id,observed_at,outcome,next_attempt_at)
           VALUES(%s,%s,%s,%s,%s)
           ON CONFLICT(kind,resource_id) DO UPDATE SET observed_at=EXCLUDED.observed_at,
               outcome=EXCLUDED.outcome,next_attempt_at=EXCLUDED.next_attempt_at,
               lease_token=NULL,leased_until=NULL""",
        (kind, resource_id, now, "topics" if topics else "empty", now + timedelta(days=30)),
    )
    await _write_edges(conn, kind, {resource_id: topics}, now)


class TopicRepository(DatabaseAdapter):
    async def heartbeat_snapshot(self) -> dict[str, Any]:
        """Report current graph coverage in one bounded, read-only transaction."""
        async with self._connection() as conn, conn.transaction():
            await conn.execute("SET TRANSACTION READ ONLY")
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute("""WITH coverage AS (
                    SELECT 'video' AS kind,s.observed_at,s.outcome
                    FROM videos v LEFT JOIN youtube_topic_sync s
                        ON s.kind='video' AND s.resource_id=v.id
                    UNION ALL
                    SELECT 'channel' AS kind,s.observed_at,s.outcome
                    FROM channels c LEFT JOIN youtube_topic_sync s
                        ON s.kind='channel' AND s.resource_id=c.id
                ) SELECT
                    count(*) FILTER (WHERE kind='video') AS video_total,
                    count(*) FILTER (WHERE kind='channel') AS channel_total,
                    count(*) FILTER (WHERE kind='video' AND observed_at IS NOT NULL)
                        AS video_checked,
                    count(*) FILTER (WHERE kind='channel' AND observed_at IS NOT NULL)
                        AS channel_checked,
                    count(*) FILTER (WHERE kind='video'
                        AND observed_at > now()-INTERVAL '1 hour') AS video_checked_1h,
                    count(*) FILTER (WHERE kind='channel'
                        AND observed_at > now()-INTERVAL '1 hour') AS channel_checked_1h,
                    count(*) FILTER (WHERE outcome='empty') AS empty,
                    count(*) FILTER (WHERE outcome='unavailable') AS unavailable,
                    max(observed_at) AS latest_observed,
                    (SELECT count(*) FROM knowledge_topics) AS topic_count,
                    (SELECT count(*) FROM video_topics) AS video_edges,
                    (SELECT count(*) FROM channel_topics) AS channel_edges
                    FROM coverage""")
                snapshot = await cursor.fetchone()
        if snapshot is None:
            raise RuntimeError("Graph coverage query returned no snapshot")
        return snapshot

    async def claim(self, kind: str, limit: int = 50) -> tuple[str, list[str]]:
        if kind not in _TABLES or not 1 <= limit <= 50:
            raise ValueError("Invalid topic claim")
        table = _TABLES[kind][0]
        token = str(uuid4())
        async with self._connection() as conn, conn.transaction():
            # Purged resources should not leave an ever-growing queue behind.
            await conn.execute(
                f"""WITH missing AS (
                    SELECT s.kind,s.resource_id FROM youtube_topic_sync s
                    WHERE s.kind=%s AND NOT EXISTS (
                        SELECT 1 FROM {table} r WHERE r.id=s.resource_id)
                    ORDER BY s.resource_id LIMIT %s FOR UPDATE SKIP LOCKED
                ) DELETE FROM youtube_topic_sync s USING missing m
                    WHERE s.kind=m.kind AND s.resource_id=m.resource_id""",
                (kind, limit),
            )
            # Seed only one bounded page; backfill does not insert the whole corpus.
            await conn.execute(
                f"""INSERT INTO youtube_topic_sync(kind,resource_id)
                    SELECT %s,r.id FROM {table} r
                    LEFT JOIN youtube_topic_sync s ON s.kind=%s AND s.resource_id=r.id
                    WHERE s.resource_id IS NULL ORDER BY r.id LIMIT %s
                    ON CONFLICT(kind,resource_id) DO NOTHING""",
                (kind, kind, limit),
            )
            cursor = await conn.execute(
                f"""WITH due AS (
                    SELECT s.kind,s.resource_id FROM youtube_topic_sync s
                    JOIN {table} r ON r.id=s.resource_id
                    WHERE s.kind=%s AND s.next_attempt_at<=now()
                      AND (s.leased_until IS NULL OR s.leased_until<=now())
                    ORDER BY s.next_attempt_at,s.resource_id LIMIT %s
                    FOR UPDATE OF s SKIP LOCKED
                ) UPDATE youtube_topic_sync s SET lease_token=%s::uuid,
                    leased_until=now()+INTERVAL '10 minutes'
                  FROM due WHERE s.kind=due.kind AND s.resource_id=due.resource_id
                  RETURNING s.resource_id""",
                (kind, limit, token),
            )
            ids = sorted(row[0] for row in await cursor.fetchall())
        return token, ids

    async def complete(
        self, kind: str, token: str, ids: Sequence[str], items: list[dict[str, Any]]
    ) -> dict[str, int]:
        if kind not in _TABLES or len(ids) > 50 or len(set(ids)) != len(ids):
            raise ValueError("Invalid topic completion")
        requested = set(ids)
        returned: dict[str, list[Topic]] = {}
        for item in items:
            key = item.get("id")
            if key not in requested or key in returned:
                raise ValueError("Unexpected YouTube topic resource")
            topics = resource_topics(item)
            # This request explicitly asked for topicDetails; an absent section
            # in an existing resource means no topics, not a missing observation.
            returned[key] = topics or []
        now = datetime.now(UTC)
        table = _TABLES[kind][0]
        async with self._connection() as conn, conn.transaction():
            # Match ingestion's parent -> topic-state ordering to avoid deadlocks.
            parents = await conn.execute(
                f"SELECT id FROM {table} WHERE id=ANY(%s) ORDER BY id FOR UPDATE", (list(ids),)
            )
            existing = [row[0] for row in await parents.fetchall()]
            cursor = await conn.execute(
                """UPDATE youtube_topic_sync SET observed_at=%s,lease_token=NULL,leased_until=NULL,
                       outcome=CASE WHEN resource_id=ANY(%s) THEN 'empty' ELSE 'unavailable' END,
                       next_attempt_at=%s
                   WHERE kind=%s AND resource_id=ANY(%s) AND lease_token=%s::uuid
                     AND leased_until>now() RETURNING resource_id""",
                (now, list(returned), now + timedelta(days=7), kind, existing, token),
            )
            accepted = {row[0] for row in await cursor.fetchall()}
            snapshots = {key: returned[key] for key in sorted(accepted & returned.keys())}
            if snapshots:
                await _write_edges(conn, kind, snapshots, now)
                cursor = await conn.execute(
                    """UPDATE youtube_topic_sync SET next_attempt_at=%s,
                        outcome=CASE WHEN resource_id=ANY(%s) THEN 'topics' ELSE 'empty' END
                        WHERE kind=%s AND resource_id=ANY(%s)""",
                    (
                        now + timedelta(days=30),
                        [key for key, topics in snapshots.items() if topics],
                        kind,
                        list(snapshots),
                    ),
                )
        # Unavailable API resources retain their previous facts, explicitly stale.
        return {
            "observed": len(snapshots),
            "unavailable": len(accepted - returned.keys()),
            "deferred": len(requested - accepted),
        }

    async def release(self, kind: str, token: str) -> None:
        await self._execute(
            """UPDATE youtube_topic_sync SET lease_token=NULL,leased_until=NULL,
               next_attempt_at=now()+INTERVAL '1 hour' WHERE kind=%s AND lease_token=%s::uuid""",
            (kind, token),
        )
