"""Read-only hot storage and durable tracking counts for operator reporting."""

from typing import Any

from psycopg.rows import dict_row

from atlas.adapters import DatabaseAdapter


class HeartbeatRepository(DatabaseAdapter):
    async def storage_snapshot(self) -> dict[str, Any]:
        async with self._connection() as conn, conn.transaction():
            await conn.execute("SET TRANSACTION READ ONLY")
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            async with conn.cursor(row_factory=dict_row) as cursor:
                await cursor.execute("""SELECT
                    count(*) FILTER (WHERE content IS NOT NULL) AS hot_bodies,
                    count(*) FILTER (WHERE content IS NOT NULL AND vault_uri IS NULL) AS staged,
                    count(*) FILTER (WHERE content IS NOT NULL AND vault_uri IS NOT NULL)
                        AS retained,
                    coalesce(sum(pg_column_size(content)) FILTER (WHERE content IS NOT NULL),0)
                        AS payload_bytes,
                    pg_database_size(current_database()) AS database_bytes,
                    (SELECT count(*) FROM watchlist WHERE next_track_at<=now()) AS tracking_due,
                    (SELECT max(last_tracked_at) FROM watchlist) AS last_tracked_at
                    FROM transcripts""")
                result = await cursor.fetchone()
        if result is None:
            raise RuntimeError("Storage query returned no snapshot")
        return result
