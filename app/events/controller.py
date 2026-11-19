from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.events.schemas import EventPage, FeedEvent
from app.orders.models import OutboxEvent


async def list_events(session: AsyncSession, after: int, limit: int) -> EventPage:
    rows = await session.scalars(
        select(OutboxEvent).where(OutboxEvent.seq > after).order_by(OutboxEvent.seq).limit(limit)
    )
    events = [
        FeedEvent(
            seq=row.seq,
            type=row.event_type,
            order_id=row.aggregate_id,
            occurred_at=row.created_at,
            data=row.payload,
        )
        for row in rows
    ]
    return EventPage(events=events, next_after=events[-1].seq if events else after)
