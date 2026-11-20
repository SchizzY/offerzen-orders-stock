import asyncio
import json
import random
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from loguru import logger
from redis.asyncio import Redis
from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.catalog.models import Product
from app.config import settings
from app.db import SessionLocal, engine
from app.orders.models import Order, OrderItem, OutboxEvent
from app.stock import controller
from app.stock.consumer import consume_once, ensure_group, run_worker
from app.stock.router import router as stock_router
from tests.logs import captured_logs

STREAM = settings.orders_stream
GROUP = settings.stock_group
CUSTOMER = "cust-stock-secret-7"


@pytest.fixture
async def stock_client() -> AsyncGenerator[AsyncClient, None]:
    app = FastAPI()
    app.include_router(stock_router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


async def add_product(client: AsyncClient, sku: str, stock: int) -> None:
    product = {"sku": sku, "name": sku, "price_cents": 100, "stock": stock}
    (await client.post("/products", json=product)).raise_for_status()


async def place_order(client: AsyncClient, order_ref: str, lines: dict[str, int]) -> None:
    order = {
        "order_ref": order_ref,
        "customer_id": CUSTOMER,
        "items": [{"sku": sku, "qty": qty} for sku, qty in lines.items()],
    }
    response = await client.post("/orders", json=order)
    assert response.status_code == 201, response.text


async def publish(valkey: Redis, order_ref: str, seq: int) -> None:
    """XADD the order's outbox event as the relay does."""
    async with SessionLocal() as session:
        event = (
            await session.scalars(
                select(OutboxEvent)
                .join(Order, Order.id == OutboxEvent.aggregate_id)
                .where(Order.order_ref == order_ref)
            )
        ).one()
    fields = {
        "seq": seq,
        "event_type": event.event_type,
        "order_id": str(event.aggregate_id),
        "order_ref": order_ref,
        "payload": json.dumps(event.payload),
    }
    await valkey.xadd(STREAM, fields, id=f"{seq}-0")


async def drain(valkey: Redis, consumer: str = "stock-test") -> int:
    handled = 0
    async with asyncio.timeout(30):
        while batch := await consume_once(valkey, consumer, block_ms=50):
            handled += batch
    return handled


async def eventually(check: Callable[[], Awaitable[bool]]) -> None:
    async with asyncio.timeout(20):
        while True:
            if await check():
                return
            await asyncio.sleep(0.05)


async def on_hand(client: AsyncClient, sku: str) -> int:
    return (await client.get(f"/products/{sku}")).json()["stock"]


async def stock_status(client: AsyncClient, order_ref: str) -> str:
    return (await client.get(f"/orders/{order_ref}")).json()["stock_status"]


async def unacked(valkey: Redis) -> int:
    return (await valkey.xpending(STREAM, GROUP))["pending"]


async def test_reserving_decrements_every_line_and_acks(client: AsyncClient, valkey: Redis) -> None:
    await add_product(client, "APL-1", 10)
    await add_product(client, "BAN-1", 5)
    await place_order(client, "web-1", {"APL-1": 3, "BAN-1": 5})
    await publish(valkey, "web-1", 1)

    assert await consume_once(valkey, "stock-test", block_ms=50) == 1

    assert await on_hand(client, "APL-1") == 7
    assert await on_hand(client, "BAN-1") == 0
    assert await stock_status(client, "web-1") == "reserved"
    assert await unacked(valkey) == 0


async def test_one_short_line_reserves_nothing_and_the_next_order_still_reserves(
    client: AsyncClient, valkey: Redis
) -> None:
    await add_product(client, "APL-1", 10)
    await add_product(client, "BAN-1", 5)
    await place_order(client, "web-1", {"APL-1": 3, "BAN-1": 6})
    await place_order(client, "web-2", {"BAN-1": 5})
    await publish(valkey, "web-1", 1)
    await publish(valkey, "web-2", 2)

    assert await consume_once(valkey, "stock-test", block_ms=50) == 2

    assert await stock_status(client, "web-1") == "insufficient"
    assert await stock_status(client, "web-2") == "reserved"
    assert await on_hand(client, "APL-1") == 10
    assert await on_hand(client, "BAN-1") == 0
    assert await unacked(valkey) == 0


async def test_two_events_for_one_order_reserve_it_once(client: AsyncClient, valkey: Redis) -> None:
    await add_product(client, "APL-1", 10)
    await place_order(client, "web-1", {"APL-1": 3})
    await publish(valkey, "web-1", 1)
    await publish(valkey, "web-1", 2)

    assert await drain(valkey) == 2

    assert await on_hand(client, "APL-1") == 7
    assert await stock_status(client, "web-1") == "reserved"
    assert await unacked(valkey) == 0


async def test_redelivering_the_whole_stream_changes_nothing(
    client: AsyncClient, valkey: Redis
) -> None:
    await add_product(client, "APL-1", 4)
    await place_order(client, "web-1", {"APL-1": 3})
    await place_order(client, "web-2", {"APL-1": 3})
    await publish(valkey, "web-1", 1)
    await publish(valkey, "web-2", 2)
    await drain(valkey)

    await valkey.xgroup_setid(STREAM, GROUP, "0")

    assert await drain(valkey) == 2
    assert await on_hand(client, "APL-1") == 1
    assert await stock_status(client, "web-1") == "reserved"
    assert await stock_status(client, "web-2") == "insufficient"
    assert await unacked(valkey) == 0


async def crash_after_reading(valkey: Redis, consumer: str) -> None:
    await valkey.xreadgroup(GROUP, consumer, {STREAM: ">"}, count=10)


async def crash_after_committing(valkey: Redis, consumer: str) -> None:
    [[_, entries]] = await valkey.xreadgroup(GROUP, consumer, {STREAM: ">"}, count=10)
    for _, fields in entries:
        async with SessionLocal() as session:
            order_id = uuid.UUID(fields[b"order_id"].decode())
            await controller.apply_order_accepted(session, order_id)


@pytest.mark.parametrize(
    "crash",
    [crash_after_reading, crash_after_committing],
    ids=["before-commit", "after-commit-before-ack"],
)
async def test_an_unacked_entry_is_applied_once_when_its_worker_resumes(
    client: AsyncClient, valkey: Redis, crash: Callable[[Redis, str], Awaitable[None]]
) -> None:
    await add_product(client, "APL-1", 10)
    await place_order(client, "web-1", {"APL-1": 3})
    await ensure_group(valkey)
    await publish(valkey, "web-1", 1)
    await crash(valkey, "stock-a")
    assert await unacked(valkey) == 1

    assert await consume_once(valkey, "stock-a", block_ms=50) == 1
    assert await consume_once(valkey, "stock-a", block_ms=50) == 0

    assert await on_hand(client, "APL-1") == 7
    assert await stock_status(client, "web-1") == "reserved"
    assert await unacked(valkey) == 0


async def test_entries_a_dead_worker_left_unacked_are_applied_by_its_replacement(
    client: AsyncClient, valkey: Redis
) -> None:
    await add_product(client, "APL-1", 10)
    await place_order(client, "web-1", {"APL-1": 3})
    await ensure_group(valkey)
    await publish(valkey, "web-1", 1)
    await crash_after_reading(valkey, "host-111")
    await asyncio.sleep(0.05)

    assert await consume_once(valkey, "host-222", block_ms=50, claim_idle_ms=10) == 1

    assert await on_hand(client, "APL-1") == 7
    assert await stock_status(client, "web-1") == "reserved"
    assert await unacked(valkey) == 0


async def test_two_workers_racing_over_shared_skus_apply_every_order_once(
    client: AsyncClient, valkey: Redis
) -> None:
    skus = [f"SKU-{index}" for index in range(5)]
    initial = 40
    for sku in skus:
        await add_product(client, sku, initial)
    rng = random.Random(7)
    orders = {
        f"race-{index}": {sku: rng.randint(1, 4) for sku in rng.sample(skus, rng.randint(1, 3))}
        for index in range(100)
    }
    for order_ref, lines in orders.items():
        await place_order(client, order_ref, lines)
    refs = list(orders)
    # Each order twice, the second pass reversed, so both workers meet on the same orders.
    for seq, order_ref in enumerate(refs + refs[::-1], start=1):
        await publish(valkey, order_ref, seq)

    async with asyncio.timeout(60):
        handled = await asyncio.gather(drain(valkey, "stock-a"), drain(valkey, "stock-b"))

    assert sum(handled) == 200
    assert await unacked(valkey) == 0
    statuses = {order_ref: await stock_status(client, order_ref) for order_ref in refs}
    assert set(statuses.values()) == {"reserved", "insufficient"}
    reserved = [orders[ref] for ref in refs if statuses[ref] == "reserved"]
    final = {sku: await on_hand(client, sku) for sku in skus}
    assert final == {sku: initial - sum(lines.get(sku, 0) for lines in reserved) for sku in skus}
    # Stock only falls, so a line that was short when its order was applied is still short.
    for order_ref in (ref for ref in refs if statuses[ref] == "insufficient"):
        assert any(qty > final[sku] for sku, qty in orders[order_ref].items())


async def test_reserving_does_not_deadlock_with_an_order_being_placed_on_the_same_skus(
    client: AsyncClient, valkey: Redis
) -> None:
    await add_product(client, "APL-1", 10)
    await add_product(client, "BAN-1", 10)
    await place_order(client, "web-1", {"APL-1": 1, "BAN-1": 1})
    await publish(valkey, "web-1", 1)

    # Placing an order inserts its items in request order; each item's FK check holds a
    # key-share lock on its product until the placement commits.
    async with engine.connect() as placing, placing.begin():
        order_id = (
            await placing.execute(
                insert(Order)
                .values(order_ref="web-2", customer_id=CUSTOMER, total_cents=200, request_hash="")
                .returning(Order.id)
            )
        ).scalar_one()
        item = {"order_id": order_id, "qty": 1, "unit_price_cents": 100}
        await placing.execute(insert(OrderItem).values(sku="BAN-1", **item))
        reserving = asyncio.create_task(consume_once(valkey, "stock-test", block_ms=50))
        await asyncio.sleep(0.3)
        await placing.execute(insert(OrderItem).values(sku="APL-1", **item))

    assert await reserving == 1
    assert await on_hand(client, "APL-1") == 9
    assert await on_hand(client, "BAN-1") == 9
    assert await stock_status(client, "web-1") == "reserved"


@asynccontextmanager
async def sessions_planned(**planner: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    planned = create_async_engine(settings.database_url, connect_args={"server_settings": planner})
    try:
        yield async_sessionmaker(planned, expire_on_commit=False)
    finally:
        await planned.dispose()


async def order_id(order_ref: str) -> uuid.UUID:
    async with SessionLocal() as session:
        return (await session.scalars(select(Order.id).where(Order.order_ref == order_ref))).one()


async def test_reservations_whose_plans_read_skus_in_different_orders_do_not_deadlock(
    client: AsyncClient,
) -> None:
    # Inserted out of sku order, so a sequential scan meets BAN-1 first and an index scan APL-1.
    await add_product(client, "BAN-1", 10)
    await add_product(client, "APL-1", 10)
    await place_order(client, "web-1", {"APL-1": 1, "BAN-1": 1})
    await place_order(client, "web-2", {"APL-1": 1, "BAN-1": 1})

    async def both_waiting() -> bool:
        async with engine.connect() as connection:
            waiting = await connection.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity"
                    " WHERE datname = current_database() AND wait_event_type = 'Lock'"
                )
            )
        return waiting == 2

    async with (
        sessions_planned(enable_seqscan="off", enable_bitmapscan="off") as by_index,
        sessions_planned(enable_indexscan="off", enable_bitmapscan="off") as by_heap,
    ):
        # Holding APL-1 lines both reservations up on it before either finishes.
        async with engine.connect() as holding, holding.begin():
            await holding.execute(
                select(Product).where(Product.sku == "APL-1").with_for_update(key_share=True)
            )
            reserving = [
                asyncio.create_task(controller.apply_order_accepted(sessions(), order))
                for sessions, order in [
                    (by_index, await order_id("web-1")),
                    (by_heap, await order_id("web-2")),
                ]
            ]
            await eventually(both_waiting)

        assert await asyncio.gather(*reserving) == ["reserved", "reserved"]

    assert await on_hand(client, "APL-1") == 8
    assert await on_hand(client, "BAN-1") == 8


async def delete_stream(valkey: Redis) -> None:
    await valkey.delete(STREAM)


async def destroy_group(valkey: Redis) -> None:
    await valkey.xgroup_destroy(STREAM, GROUP)


@pytest.mark.parametrize(
    ("lose", "republish"),
    [(delete_stream, [("web-1", 1), ("web-2", 2)]), (destroy_group, [("web-2", 2)])],
    ids=["stream-deleted", "group-destroyed"],
)
async def test_worker_recreates_a_group_lost_mid_read_and_keeps_going(
    client: AsyncClient,
    valkey: Redis,
    lose: Callable[[Redis], Awaitable[None]],
    republish: list[tuple[str, int]],
) -> None:
    await add_product(client, "APL-1", 10)
    await place_order(client, "web-1", {"APL-1": 3})
    await place_order(client, "web-2", {"APL-1": 2})
    await publish(valkey, "web-1", 1)
    assert await drain(valkey) == 1

    reading = asyncio.create_task(consume_once(valkey, "stock-test", block_ms=2000))
    await asyncio.sleep(0.2)
    await lose(valkey)
    await asyncio.sleep(0.2)
    for order_ref, seq in republish:
        await publish(valkey, order_ref, seq)
    await reading
    await drain(valkey)

    assert await on_hand(client, "APL-1") == 5
    assert await stock_status(client, "web-1") == "reserved"
    assert await stock_status(client, "web-2") == "reserved"
    assert await unacked(valkey) == 0


ENTRY: dict[str, Any] = {"seq": 1, "event_type": "order.accepted", "order_ref": "web-x"}


@pytest.mark.parametrize(
    "fields",
    [
        ENTRY,
        {**ENTRY, "order_id": "not-a-uuid"},
        {**ENTRY, "seq": "one", "order_id": str(uuid.uuid4())},
        {**ENTRY, "event_type": "order.cancelled", "order_id": str(uuid.uuid4())},
        {**ENTRY, "order_id": str(uuid.uuid4())},
    ],
    ids=["missing-order-id", "bad-order-id", "bad-seq", "other-event-type", "unknown-order"],
)
async def test_an_entry_that_cannot_be_applied_is_acked_without_blocking_the_queue(
    client: AsyncClient, valkey: Redis, fields: dict[str, Any]
) -> None:
    await add_product(client, "APL-1", 10)
    await place_order(client, "web-1", {"APL-1": 3})
    await valkey.xadd(STREAM, fields, id="1-0")
    await publish(valkey, "web-1", 2)

    assert await drain(valkey) == 2

    assert await on_hand(client, "APL-1") == 7
    assert await stock_status(client, "web-1") == "reserved"
    assert await unacked(valkey) == 0


async def test_each_event_logs_seq_order_ref_and_result_but_not_the_customer(
    client: AsyncClient, valkey: Redis
) -> None:
    await add_product(client, "APL-1", 1)
    await place_order(client, "web-1", {"APL-1": 1})
    await place_order(client, "web-2", {"APL-1": 1})
    await publish(valkey, "web-1", 1)
    await publish(valkey, "web-2", 2)
    await publish(valkey, "web-1", 3)

    with captured_logs("app.stock") as logs:
        await drain(valkey)

    events = [line for line in logs if "order_ref=" in line]
    assert len(events) == 3
    assert (
        "seq=1" in events[0] and "order_ref=web-1" in events[0] and "result=reserved" in events[0]
    )
    assert "seq=2" in events[1] and "order_ref=web-2" in events[1]
    assert "result=insufficient" in events[1]
    assert "seq=3" in events[2] and "order_ref=web-1" in events[2]
    assert "result=duplicate" in events[2]
    assert CUSTOMER not in "\n".join(logs)


@pytest.fixture
async def orders_reject_stock_changes() -> AsyncGenerator[None, None]:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "ALTER TABLE orders ADD CONSTRAINT ck_orders_test_frozen"
                " CHECK (stock_status = 'pending')"
            )
        )
    yield
    async with engine.begin() as connection:
        await connection.execute(
            text("ALTER TABLE orders DROP CONSTRAINT IF EXISTS ck_orders_test_frozen")
        )


async def test_worker_retries_an_event_whose_transaction_failed_without_logging_row_values(
    client: AsyncClient, valkey: Redis, orders_reject_stock_changes: None
) -> None:
    await add_product(client, "APL-1", 10)
    await place_order(client, "web-1", {"APL-1": 3})
    await publish(valkey, "web-1", 1)
    stop = asyncio.Event()

    async def failure_logged() -> bool:
        return any("IntegrityError" in line for line in logs)

    async def reserved() -> bool:
        return await stock_status(client, "web-1") == "reserved"

    with captured_logs() as logs:
        worker = asyncio.create_task(run_worker(stop, "stock-test"))
        await eventually(failure_logged)
        assert await on_hand(client, "APL-1") == 10
        assert await stock_status(client, "web-1") == "pending"

        async with engine.begin() as connection:
            await connection.execute(
                text("ALTER TABLE orders DROP CONSTRAINT ck_orders_test_frozen")
            )
        await eventually(reserved)
        stop.set()
        await asyncio.wait_for(worker, 5)

    assert await on_hand(client, "APL-1") == 7
    assert await unacked(valkey) == 0
    output = "\n".join(logs)
    assert CUSTOMER not in output
    assert "Failing row" not in output


@contextmanager
def captured_levels() -> Iterator[list[tuple[str, str]]]:
    records: list[tuple[str, str]] = []
    handler_id = logger.add(
        lambda message: records.append((message.record["level"].name, message.record["message"])),
        level="TRACE",
        filter="app.stock",
    )
    try:
        yield records
    finally:
        logger.remove(handler_id)


async def test_worker_creates_its_group_at_info_and_warns_only_when_it_is_lost_mid_run(
    client: AsyncClient, valkey: Redis
) -> None:
    await add_product(client, "APL-1", 10)
    await place_order(client, "web-1", {"APL-1": 3})
    stop = asyncio.Event()

    async def reserved() -> bool:
        return await stock_status(client, "web-1") == "reserved"

    async def group_logged() -> bool:
        return any("group created" in m or "group missing" in m for _, m in logs)

    with captured_levels() as logs:
        worker = asyncio.create_task(run_worker(stop, "stock-test"))
        await eventually(group_logged)
        at_start = list(logs)
        await destroy_group(valkey)
        await publish(valkey, "web-1", 1)
        await eventually(reserved)
        stop.set()
        await asyncio.wait_for(worker, 5)

    assert [message for level, message in at_start if "group" in message and level != "INFO"] == []
    assert any(level == "INFO" and "group created" in message for level, message in at_start)
    lost = [message for level, message in logs[len(at_start) :] if level == "WARNING"]
    assert len(lost) == 1
    assert "group missing" in lost[0]


async def consumers(valkey: Redis) -> dict[str, int]:
    return {
        consumer["name"].decode(): consumer["pending"]
        for consumer in await valkey.xinfo_consumers(STREAM, GROUP)
    }


async def test_a_worker_that_stops_with_nothing_pending_leaves_the_group(
    client: AsyncClient, valkey: Redis
) -> None:
    await add_product(client, "APL-1", 10)
    await place_order(client, "web-1", {"APL-1": 3})
    await place_order(client, "web-2", {"APL-1": 2})
    await ensure_group(valkey)
    await publish(valkey, "web-1", 1)
    await crash_after_reading(valkey, "stock-other")
    await publish(valkey, "web-2", 2)
    stop = asyncio.Event()

    async def reserved() -> bool:
        return await stock_status(client, "web-2") == "reserved"

    worker = asyncio.create_task(run_worker(stop, "stock-test"))
    await eventually(reserved)
    stop.set()
    await asyncio.wait_for(worker, 5)

    assert await consumers(valkey) == {"stock-other": 1}


async def test_a_worker_that_stops_with_entries_pending_stays_in_the_group(
    client: AsyncClient, valkey: Redis, orders_reject_stock_changes: None
) -> None:
    await add_product(client, "APL-1", 10)
    await place_order(client, "web-1", {"APL-1": 3})
    await publish(valkey, "web-1", 1)
    stop = asyncio.Event()

    async def failure_logged() -> bool:
        return any("stock worker error" in message for _, message in logs)

    with captured_levels() as logs:
        worker = asyncio.create_task(run_worker(stop, "stock-test"))
        await eventually(failure_logged)
        stop.set()
        await asyncio.wait_for(worker, 5)

    assert await consumers(valkey) == {"stock-test": 1}


async def test_stock_shows_on_hand_and_the_qty_still_waiting_for_the_worker(
    client: AsyncClient, stock_client: AsyncClient, valkey: Redis
) -> None:
    await add_product(client, "APL-1", 10)
    await add_product(client, "BAN-1", 4)
    assert (await stock_client.get("/stock/APL-1")).json() == {
        "sku": "APL-1",
        "on_hand": 10,
        "pending_qty": 0,
    }

    await place_order(client, "web-1", {"APL-1": 3})
    await place_order(client, "web-2", {"APL-1": 2, "BAN-1": 1})
    await place_order(client, "web-3", {"APL-1": 1, "BAN-1": 5})

    apples = await stock_client.get("/stock/APL-1")
    assert apples.status_code == 200
    assert apples.json() == {"sku": "APL-1", "on_hand": 10, "pending_qty": 6}
    assert (await stock_client.get("/stock/BAN-1")).json() == {
        "sku": "BAN-1",
        "on_hand": 4,
        "pending_qty": 6,
    }

    for seq, order_ref in enumerate(["web-1", "web-2", "web-3"], start=1):
        await publish(valkey, order_ref, seq)
    await drain(valkey)

    assert (await stock_client.get("/stock/APL-1")).json() == {
        "sku": "APL-1",
        "on_hand": 5,
        "pending_qty": 0,
    }
    assert (await stock_client.get("/stock/BAN-1")).json() == {
        "sku": "BAN-1",
        "on_hand": 3,
        "pending_qty": 0,
    }


async def test_stock_for_an_unknown_sku_is_404(stock_client: AsyncClient) -> None:
    response = await stock_client.get("/stock/NOPE-1")

    assert response.status_code == 404
    assert response.json() == {"detail": "product 'NOPE-1' not found"}


@pytest.mark.parametrize("sku", ["bad sku!", "-leading-dash", "A" * 65])
async def test_stock_for_a_malformed_sku_is_422(stock_client: AsyncClient, sku: str) -> None:
    response = await stock_client.get(f"/stock/{sku}")

    assert response.status_code == 422
