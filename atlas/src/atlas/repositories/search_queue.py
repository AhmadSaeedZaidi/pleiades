import logging
from datetime import UTC, datetime

from atlas.adapters import DatabaseAdapter
from atlas.config import get_settings
from atlas.models.search_queue import SearchQueueItem

logger = logging.getLogger("atlas.repositories.search_queue")

# Dynamic relevance score evaluated at read time; depends on NOW() so it is not
# indexable — the cull keeps the table small enough that the sort is negligible.
_SCORE_EXPR = (
    "(mention_count * %s - EXTRACT(EPOCH FROM (NOW() - created_at)) / 3600.0 * %s + priority)"
)


class SearchQueueRepository(DatabaseAdapter):
    async def fetch_batch(self, batch_size: int = 10) -> list[SearchQueueItem]:
        s = get_settings()
        balanced = s.DISCOVERY_BALANCE_ENABLED
        score = (
            _SCORE_EXPR.replace("mention_count * %s", "LEAST(mention_count, %s) * %s")
            if balanced
            else _SCORE_EXPR
        )
        cooldown = (
            "AND (last_searched_at IS NULL OR last_searched_at < NOW() - %s * INTERVAL '1 hour')"
            if balanced
            else ""
        )
        params: tuple[object, ...] = (
            (s.DISCOVERY_QUERY_COOLDOWN_HOURS, s.DISCOVERY_MENTION_CAP) if balanced else ()
        )
        rows = await self._fetch_all(
            f"""
            SELECT *
            FROM search_queue
            WHERE status = 'active' {cooldown}
            ORDER BY {score} DESC, last_searched_at ASC NULLS FIRST, id ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (
                *params,
                s.SEARCH_QUEUE_MENTION_WEIGHT,
                s.SEARCH_QUEUE_DECAY_PER_HOUR,
                batch_size,
            ),
        )
        return [SearchQueueItem.model_validate(r) for r in rows]

    async def cull_stale(self, below: float | None = None) -> int:
        """Delete low-scoring, time-decayed terms.

        Never culls terms mid-pagination (``next_page_token IS NOT NULL``).
        Opt-in: only runs when a threshold is set (``SEARCH_QUEUE_CULL_BELOW``);
        ``None`` disables it so the queue stays a long-lived accumulator and the
        hunter is never starved of seed terms. Returns rows deleted (0 if disabled).
        """
        s = get_settings()
        threshold = s.SEARCH_QUEUE_CULL_BELOW if below is None else below
        if threshold is None:
            logger.info("search_queue cull disabled (SEARCH_QUEUE_CULL_BELOW is None)")
            return 0
        async with self._cursor() as cur:
            await cur.execute(
                f"""
                DELETE FROM search_queue
                WHERE next_page_token IS NULL
                  AND {_SCORE_EXPR} < %s
                  AND query_term <> ALL(%s::text[])
                """,
                (
                    s.SEARCH_QUEUE_MENTION_WEIGHT,
                    s.SEARCH_QUEUE_DECAY_PER_HOUR,
                    threshold,
                    s.DISCOVERY_SEED_TERMS if s.DISCOVERY_BALANCE_ENABLED else [],
                ),
            )
            deleted = cur.rowcount
            await cur.connection.commit()
        return int(deleted)

    async def update_state(
        self,
        topic_id: int,
        next_token: str | None,
        result_count: int,
        status: str = "active",
    ) -> None:
        now = datetime.now(UTC)
        await self._execute(
            """
            UPDATE search_queue
            SET next_page_token = %s,
                last_searched_at = %s,
                result_count_total = COALESCE(result_count_total, 0) + %s,
                status = %s
            WHERE id = %s
            """,
            (next_token, now, result_count, status, topic_id),
        )

    async def ensure_seed_terms(self, terms: list[str]) -> None:
        if len(terms) > 100 or any(not term.strip() or len(term) > 100 for term in terms):
            raise ValueError("Seed terms must be bounded non-empty strings")
        await self._execute_many(
            """INSERT INTO search_queue(query_term,priority,mention_count)
               VALUES(%s,20,0) ON CONFLICT(query_term) DO UPDATE SET
                   status='active',next_page_token=NULL
               WHERE search_queue.status='exhausted'
                 AND search_queue.last_searched_at < NOW() - %s * INTERVAL '1 hour'""",
            [
                (term, get_settings().DISCOVERY_QUERY_COOLDOWN_HOURS)
                for term in dict.fromkeys(terms)
            ],
        )

    async def set_geography_penalty(self, topic_id: int, penalty: int) -> None:
        if not -1000 <= penalty <= 0:
            raise ValueError("Geography penalties must be between -1000 and zero")
        await self._execute(
            """UPDATE search_queue SET priority=CASE
                   WHEN %s < 0 THEN %s WHEN priority<0 THEN 0 ELSE priority END
               WHERE id=%s AND query_term <> ALL(%s::text[])""",
            (
                penalty,
                penalty,
                topic_id,
                get_settings().DISCOVERY_SEED_TERMS
                if get_settings().DISCOVERY_BALANCE_ENABLED
                else [],
            ),
        )

    async def add_terms(self, terms: list[str], penalty: int = 0) -> int:
        if not -1000 <= penalty <= 0:
            raise ValueError("Geography penalties must be non-positive and bounded")
        if not terms:
            return 0

        unique_terms = list(set(terms))
        query = """
            INSERT INTO search_queue (query_term, priority, mention_count)
            VALUES (%s, %s, 1)
            ON CONFLICT (query_term)
            DO UPDATE SET mention_count = search_queue.mention_count + 1,
                priority = LEAST(search_queue.priority, EXCLUDED.priority)
        """
        params_list = [(term, penalty) for term in unique_terms]
        await self._execute_many(query, params_list)
        return len(unique_terms)
