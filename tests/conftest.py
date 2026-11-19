from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from app.db import engine
from app.main import app


@pytest.fixture(autouse=True)
async def clean_tables() -> None:
    if not (engine.url.database or "").endswith("_test"):
        pytest.exit(f"refusing to truncate {engine.url.database!r}; run tests via scripts/test.sh")
    async with engine.begin() as connection:
        await connection.execute(
            text("TRUNCATE products, orders, order_items, outbox_events RESTART IDENTITY CASCADE")
        )


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client
