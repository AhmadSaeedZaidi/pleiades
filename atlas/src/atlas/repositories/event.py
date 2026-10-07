import logging
import uuid
from datetime import UTC, datetime
from typing import Any

import orjson

from atlas.adapters import DatabaseAdapter
from atlas.models.event import SystemEvent

logger = logging.getLogger("atlas.repositories.event")


class EventRepository(DatabaseAdapter):
    async def emit(self, event_type: str, entity_id: str, payload: dict[str, Any]) -> None:
        event_id = str(uuid.uuid4())
        created_at = datetime.now(UTC)
        payload_json = orjson.dumps(payload).decode("utf-8")

        try:
            await self._execute(
                """
                INSERT INTO system_events (id, event_type, entity_id, payload, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (event_id, event_type, entity_id, payload_json, created_at),
            )
        except Exception as e:
            logger.exception(f"Event bus failure: {e}")

    async def get_by_entity(self, entity_id: str, limit: int = 50) -> list[SystemEvent]:
        rows = await self._fetch_all(
            """
            SELECT id, event_type, entity_id, payload, created_at
            FROM system_events
            WHERE entity_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (entity_id, limit),
        )
        return [SystemEvent.model_validate(r) for r in rows]

    async def get_by_type(self, event_type: str, limit: int = 50) -> list[SystemEvent]:
        rows = await self._fetch_all(
            """
            SELECT id, event_type, entity_id, payload, created_at
            FROM system_events
            WHERE event_type = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (event_type, limit),
        )
        return [SystemEvent.model_validate(r) for r in rows]

    async def get_recent(self, limit: int = 100) -> list[SystemEvent]:
        rows = await self._fetch_all(
            """
            SELECT id, event_type, entity_id, payload, created_at
            FROM system_events
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [SystemEvent.model_validate(r) for r in rows]
