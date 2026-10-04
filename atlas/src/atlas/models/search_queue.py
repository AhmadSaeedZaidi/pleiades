from datetime import datetime

from pydantic import BaseModel, ConfigDict


class SearchQueueItem(BaseModel):  # type: ignore[misc]
    id: int
    query_term: str
    priority: int = 0
    mention_count: int = 0
    next_page_token: str | None = None
    last_searched_at: datetime | None = None
    result_count_total: int | None = 0
    status: str = "active"
    created_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)
