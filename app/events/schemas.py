import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel


class FeedEvent(BaseModel):
    seq: int
    type: str
    order_id: uuid.UUID
    occurred_at: datetime
    data: dict[str, Any]


class EventPage(BaseModel):
    events: list[FeedEvent]
    next_after: int
