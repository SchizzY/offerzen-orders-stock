import asyncio
import gc
import time
from collections.abc import AsyncIterator

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis
from sqlalchemy import func, select

from app.config import settings
from app.db import SessionLocal
from app.main import RELAY_STOP_GRACE_SECONDS, app
from app.orders.relay import SEQUENCE_LOCK_KEY
from tests.test_relay import (
    BANANAS,
    eventually,
    place_order,
    sessions_waiting_on_locks,
    stream_ids,
)


@pytest.fixture(autouse=True)
def relay_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "relay_enabled", True)
    monkeypatch.setattr(settings, "poll_interval_seconds", 0.05)


@pytest.fixture(autouse=True)
async def nothing_left_running() -> AsyncIterator[None]:
    """Fail a test that leaves a task pending or makes the loop report an error, such as
    "Task was destroyed but it is pending"."""
    loop = asyncio.get_running_loop()
    before = asyncio.all_tasks()
    errors: list[str] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: errors.append(context["message"]))
    yield
    gc.collect()
    await asyncio.sleep(0)
    loop.set_exception_handler(previous_handler)
    assert asyncio.all_tasks() - before - {asyncio.current_task()} == set()
    assert errors == []


async def test_app_relay_publishes_new_orders_and_stops_promptly(
    client: AsyncClient, valkey: Redis
) -> None:
    async def published() -> bool:
        return await stream_ids(valkey) == ["1-0"]

    async with app.router.lifespan_context(app):
        (await client.post("/products", json=BANANAS)).raise_for_status()
        await place_order(client, "web-1")
        await eventually(published)
        shutdown_started = time.monotonic()

    assert time.monotonic() - shutdown_started < 0.5


async def test_shutdown_cancels_a_relay_stuck_on_a_hung_valkey(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reached = asyncio.Event()

    async def never_reply(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        reached.set()
        await reader.read()
        writer.close()

    server = await asyncio.start_server(never_reply, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    hung = Redis.from_url(f"redis://127.0.0.1:{port}/1")
    monkeypatch.setattr("app.valkey.valkey", hung)
    try:
        async with asyncio.timeout(5), app.router.lifespan_context(app):
            await reached.wait()
            shutdown_started = time.monotonic()
        shutdown = time.monotonic() - shutdown_started
    finally:
        await hung.aclose()
        server.close()
        await server.wait_closed()

    assert shutdown < RELAY_STOP_GRACE_SECONDS + 0.5


async def test_shutdown_cancels_a_relay_stuck_on_a_postgres_lock() -> None:
    async def sequencer_waiting() -> bool:
        return await sessions_waiting_on_locks() == 1

    async with SessionLocal() as holder, holder.begin():
        await holder.execute(select(func.pg_advisory_xact_lock(SEQUENCE_LOCK_KEY)))
        async with asyncio.timeout(5), app.router.lifespan_context(app):
            await eventually(sequencer_waiting)
            shutdown_started = time.monotonic()
        shutdown = time.monotonic() - shutdown_started

    assert shutdown < RELAY_STOP_GRACE_SECONDS + 0.5
