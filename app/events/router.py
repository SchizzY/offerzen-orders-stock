from typing import Annotated

from fastapi import APIRouter, Query

from app.db import SessionDep
from app.events import controller
from app.events.schemas import EventPage

router = APIRouter(prefix="/events", tags=["events"])

BIGINT_MAX = 2**63 - 1


@router.get("")
async def list_events(
    session: SessionDep,
    after: Annotated[int, Query(ge=0, le=BIGINT_MAX)] = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> EventPage:
    """Accepted orders in commit order. Resume with `after=<next_after>`; dedupe by `seq`."""
    return await controller.list_events(session, after, limit)
