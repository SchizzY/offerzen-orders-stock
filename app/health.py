import asyncio
from collections.abc import Awaitable

from fastapi import APIRouter, Response, status
from sqlalchemy import text

from app.db import SessionDep
from app.valkey import ValkeyDep

PROBE_TIMEOUT_SECONDS = 2

router = APIRouter(tags=["health"])


async def _probe(check: Awaitable[object]) -> str:
    try:
        async with asyncio.timeout(PROBE_TIMEOUT_SECONDS):
            await check
    except Exception:
        return "unavailable"
    return "ok"


@router.get("/health")
async def health(session: SessionDep, valkey: ValkeyDep, response: Response) -> dict[str, str]:
    postgres_status, valkey_status = await asyncio.gather(
        _probe(session.execute(text("SELECT 1"))),
        _probe(valkey.ping()),
    )
    checks = {"postgres": postgres_status, "valkey": valkey_status}
    if any(result != "ok" for result in checks.values()):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return checks
