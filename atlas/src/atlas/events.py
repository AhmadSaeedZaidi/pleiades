import logging
from typing import Any

from atlas.repositories.event import EventRepository

logger = logging.getLogger("atlas.events")


class EventBus:
    def __init__(self) -> None:
        self._repo = EventRepository()

    async def emit(self, event_type: str, entity_id: str, payload: dict[str, Any]) -> None:
        await self._repo.emit(event_type, entity_id, payload)


events = EventBus()
