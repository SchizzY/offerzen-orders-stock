import asyncio
from collections.abc import Awaitable

from fastapi import APIRouter, Response, status
from loguru import logger
from sqlalchemy import text

from app.db import SessionDep
from app.valkey import ValkeyDep

PROBE_TIMEOUT_SECONDS = 2

router = APIRouter(tags=["health"])


async def _probe(dependency: str, check: Awaitable[object]) -> str:
    try:
        async with asyncio.timeout(PROBE_TIMEOUT_SECONDS):
            await check
    except Exception as exc:
        logger.warning(
            "health probe failed: dependency={} error={}", dependency, type(exc).__name__
        )
        return "unavailable"
    return "ok"


@router.get("/health")
async def health(session: SessionDep, valkey: ValkeyDep, response: Response) -> dict[str, str]:
    postgres_status, valkey_status = await asyncio.gather(
        _probe("postgres", session.execute(text("SELECT 1"))),
        _probe("valkey", valkey.ping()),
    )
    checks = {"postgres": postgres_status, "valkey": valkey_status}
    if any(result != "ok" for result in checks.values()):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return checks
