import logging
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.db import get_session
from app.main import create_app
from app.valkey import get_valkey


async def test_health_reports_both_dependencies_ok(client: AsyncClient) -> None:
    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"postgres": "ok", "valkey": "ok"}


async def test_health_returns_503_naming_the_unreachable_dependency(
    caplog: pytest.LogCaptureFixture,
) -> None:
    unreachable = Redis.from_url("redis://localhost:1/0")
    app = create_app()
    app.dependency_overrides[get_valkey] = lambda: unreachable
    transport = ASGITransport(app=app)
    with caplog.at_level(logging.WARNING, logger="app.health"):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/health")
    await unreachable.aclose()

    assert response.status_code == 503
    assert response.json() == {"postgres": "ok", "valkey": "unavailable"}
    assert caplog.messages == ["health probe failed: dependency=valkey error=ConnectionError"]


async def test_health_returns_503_when_postgres_is_unreachable() -> None:
    unreachable = create_async_engine("postgresql+asyncpg://orders:orders@localhost:1/orders")

    async def unreachable_session() -> AsyncGenerator[AsyncSession, None]:
        async with async_sessionmaker(unreachable)() as session:
            yield session

    app = create_app()
    app.dependency_overrides[get_session] = unreachable_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
    await unreachable.dispose()

    assert response.status_code == 503
    assert response.json() == {"postgres": "unavailable", "valkey": "ok"}
