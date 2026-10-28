from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class SystemEvent(BaseModel):  # type: ignore[misc]
    id: str
    event_type: str
    entity_id: str | None = None
    payload: dict[str, Any] | None = None
    created_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)
