"""Read-only geography/novelty observations for bounded discovery decisions."""

from psycopg.rows import dict_row

from atlas.adapters import DatabaseAdapter


class DiscoveryRepository(DatabaseAdapter):
    async def geography_snapshot(self) -> dict[str, dict[str, int]]:
        async with self._connection() as conn, conn.transaction():
            await conn.execute("SET TRANSACTION READ ONLY")
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""SELECT CASE WHEN upper(trim(c.country)) ~ '^[A-Z]{2}$'
                        THEN upper(trim(c.country)) ELSE 'unknown' END AS country,
                        count(*) AS lifetime,
                        count(*) FILTER (WHERE v.discovered_at>now()-interval '7 days') AS recent
                    FROM videos v LEFT JOIN channels c ON c.id=v.channel_id GROUP BY 1""")
                rows = await cur.fetchall()
        return {
            cohort: {row["country"]: int(row[cohort]) for row in rows}
            for cohort in ("lifetime", "recent")
        }

    async def existing_video_ids(self, video_ids: list[str]) -> set[str]:
        if len(video_ids) > 50:
            raise ValueError("Discovery pages are bounded to 50 videos")
        if not video_ids:
            return set()
        rows = await self._fetch_all("SELECT id FROM videos WHERE id=ANY(%s)", (video_ids,))
        return {row["id"] for row in rows}
