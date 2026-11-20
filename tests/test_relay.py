import asyncio
import json
import threading
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from httpx import AsyncClient
from redis import Redis as SyncRedis
from redis.asyncio import Redis
from sqlalchemy import func, select, text

from app.config import settings
from app.db import SessionLocal
from app.orders.models import Order, OutboxEvent
from app.orders.relay import SEQUENCE_LOCK_KEY, publish_pending, run_relay, sequence_pending
from tests.logs import captured_logs

BANANAS = {"sku": "BAN-001", "name": "Bananas 1kg", "price_cents": 199, "stock": 50}


@pytest.fixture(autouse=True)
async def bananas(client: AsyncClient) -> None:
    (await client.post("/products", json=BANANAS)).raise_for_status()


async def place_order(client: AsyncClient, order_ref: str) -> dict[str, Any]:
    response = await client.post(
        "/orders",
        json={
            "order_ref": order_ref,
            "customer_id": "cust-42",
            "items": [{"sku": "BAN-001", "qty": 1}],
        },
    )
    assert response.status_code == 201
    return response.json()


def outbox_event(order_ref: str, **columns: Any) -> OutboxEvent:
    return OutboxEvent(
        event_type="order.accepted",
        aggregate_id=uuid.uuid4(),
        payload={"order_ref": order_ref},
        **columns,
    )


async def add_events(count: int) -> None:
    async with SessionLocal() as session, session.begin():
        session.add_all(outbox_event(f"bulk-{n}") for n in range(count))


async def events() -> list[OutboxEvent]:
    async with SessionLocal() as session:
        return list(await session.scalars(select(OutboxEvent).order_by(OutboxEvent.id)))


async def seq_by_ref() -> dict[str, int | None]:
    return {event.payload["order_ref"]: event.seq for event in await events()}


async def sequence() -> int:
    async with SessionLocal() as session:
        return await sequence_pending(session)


async def publish(valkey: Redis) -> int:
    async with SessionLocal() as session:
        return await publish_pending(session, valkey)


async def sequence_all() -> None:
    while await sequence():
        pass


async def stream(valkey: Redis) -> list[tuple[str, dict[str, str]]]:
    return [
        (entry_id.decode(), {key.decode(): value.decode() for key, value in fields.items()})
        for entry_id, fields in await valkey.xrange(settings.orders_stream)
    ]


async def stream_ids(valkey: Redis) -> list[str]:
    return [entry_id for entry_id, _ in await stream(valkey)]


async def sessions_waiting_on_locks() -> int:
    async with SessionLocal() as session:
        return await session.scalar(
            text(
                "SELECT count(*) FROM pg_stat_activity"
                " WHERE datname = current_database() AND wait_event_type = 'Lock'"
            )
        )


async def seconds_to_publish(count: int, valkey: Redis) -> float:
    stop = asyncio.Event()
    started = time.monotonic()
    relay = asyncio.create_task(run_relay(stop))
    try:
        async with asyncio.timeout(20):
            while await valkey.xlen(settings.orders_stream) < count:  # noqa: ASYNC110
                await asyncio.sleep(0.01)
    finally:
        stop.set()
        await asyncio.wait_for(relay, timeout=5)
    return time.monotonic() - started


async def eventually(check: Callable[[], Awaitable[bool]]) -> None:
    async with asyncio.timeout(5):
        # Polls Postgres, Valkey and captured logs, none of which can set an Event.
        while not await check():  # noqa: ASYNC110
            await asyncio.sleep(0.01)


async def test_orders_are_numbered_from_one_in_commit_order(client: AsyncClient) -> None:
    for order_ref in ("web-1", "web-2", "web-3"):
        await place_order(client, order_ref)

    assert await sequence() == 3
    assert await seq_by_ref() == {"web-1": 1, "web-2": 2, "web-3": 3}
    assert await sequence() == 0


async def test_uncommitted_event_waits_and_takes_the_next_seq_once_committed(
    client: AsyncClient,
) -> None:
    await place_order(client, "web-1")
    assert await sequence() == 1

    async with SessionLocal() as held:
        async with held.begin():
            held.add(outbox_event("late"))
            await held.flush()
            await place_order(client, "early")

            assert await sequence() == 1
            assert await seq_by_ref() == {"web-1": 1, "early": 2}

        assert await sequence() == 1

    assert await seq_by_ref() == {"web-1": 1, "late": 3, "early": 2}
    by_id = [event.payload["order_ref"] for event in await events()]
    assert by_id == ["web-1", "late", "early"]


async def test_concurrent_sequencers_number_each_event_exactly_once() -> None:
    await add_events(200)

    async with SessionLocal() as blocker, blocker.begin():
        await blocker.execute(select(OutboxEvent).where(OutboxEvent.id == 1).with_for_update())
        sequencers = asyncio.gather(sequence(), sequence())

        async def both_waiting() -> bool:
            return await sessions_waiting_on_locks() == 2

        await eventually(both_waiting)
    counts = await sequencers

    assert sum(counts) == 200
    assert [event.seq for event in await events()] == list(range(1, 201))


async def test_publish_adds_one_entry_per_event_keyed_by_seq(
    client: AsyncClient, valkey: Redis
) -> None:
    first = await place_order(client, "web-1")
    await place_order(client, "web-2")
    await sequence_all()

    assert await publish(valkey) == 2

    entries = await stream(valkey)
    assert [entry_id for entry_id, _ in entries] == ["1-0", "2-0"]
    async with SessionLocal() as session:
        order_id = await session.scalar(select(Order.id).where(Order.order_ref == "web-1"))
    fields = entries[0][1]
    assert set(fields) == {"seq", "event_type", "order_id", "order_ref", "payload"}
    assert fields["seq"] == "1"
    assert fields["event_type"] == "order.accepted"
    assert fields["order_id"] == str(order_id)
    assert fields["order_ref"] == "web-1"
    assert json.loads(fields["payload"]) == {
        key: value for key, value in first.items() if key != "stock_status"
    }


async def test_publishing_again_adds_nothing(client: AsyncClient, valkey: Redis) -> None:
    await place_order(client, "web-1")
    await place_order(client, "web-2")
    await sequence_all()
    await publish(valkey)

    assert await publish(valkey) == 0
    assert await stream_ids(valkey) == ["1-0", "2-0"]


async def test_publish_continues_from_the_stream_after_new_orders(
    client: AsyncClient, valkey: Redis
) -> None:
    await place_order(client, "web-1")
    await sequence_all()
    await publish(valkey)
    await place_order(client, "web-2")
    await sequence_all()

    assert await publish(valkey) == 1
    assert await stream_ids(valkey) == ["1-0", "2-0"]


async def test_lost_stream_is_republished_from_the_first_seq(
    client: AsyncClient, valkey: Redis
) -> None:
    for order_ref in ("web-1", "web-2", "web-3"):
        await place_order(client, order_ref)
    await sequence_all()
    await publish(valkey)

    await valkey.delete(settings.orders_stream)

    assert await publish(valkey) == 3
    assert await stream_ids(valkey) == ["1-0", "2-0", "3-0"]


async def test_stream_lost_between_reading_its_top_and_writing_is_republished_whole(
    client: AsyncClient, valkey: Redis
) -> None:
    for order_ref in ("web-1", "web-2", "web-3"):
        await place_order(client, order_ref)
    await sequence_all()
    await publish(valkey)
    for order_ref in ("web-4", "web-5"):
        await place_order(client, order_ref)
    await sequence_all()

    async def publisher_waiting() -> bool:
        return await sessions_waiting_on_locks() == 1

    async with SessionLocal() as blocker:
        async with blocker.begin():
            await blocker.execute(text("LOCK TABLE outbox_events IN ACCESS EXCLUSIVE MODE"))
            publishing = asyncio.create_task(publish(valkey))
            await eventually(publisher_waiting)
            await valkey.delete(settings.orders_stream)

        assert await publishing == 5
    assert await stream_ids(valkey) == [f"{seq}-0" for seq in range(1, 6)]


async def test_concurrent_publishers_add_each_seq_exactly_once(valkey: Redis) -> None:
    await add_events(250)
    await sequence_all()

    counts = list(await asyncio.gather(publish(valkey), publish(valkey), publish(valkey)))
    while added := await publish(valkey):
        counts.append(added)

    assert sum(counts) == 250
    assert await stream_ids(valkey) == [f"{seq}-0" for seq in range(1, 251)]


async def test_relay_publishes_as_fast_while_a_consumer_group_reads_and_acks(
    valkey: Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "poll_interval_seconds", 60)
    await add_events(5000)
    await sequence_all()
    alone = await seconds_to_publish(5000, valkey)

    await valkey.delete(settings.orders_stream)
    await valkey.xgroup_create(settings.orders_stream, "readers", id="0", mkstream=True)
    acked: list[int] = []
    done_reading = threading.Event()

    # A thread with its own connection, so reads and acks land mid-round as a worker
    # process's would.
    def read_and_ack() -> None:
        with SyncRedis.from_url(settings.valkey_url) as reader:
            while not done_reading.is_set():
                batches = reader.xreadgroup(
                    "readers", "reader-1", {settings.orders_stream: ">"}, count=1, block=10
                )
                for _, entries in batches or []:
                    ids = [entry_id for entry_id, _ in entries]
                    acked.append(reader.xack(settings.orders_stream, "readers", *ids))

    reading = asyncio.create_task(asyncio.to_thread(read_and_ack))
    try:
        contended = await seconds_to_publish(5000, valkey)
    finally:
        done_reading.set()
        await reading

    assert sum(acked) > 0
    assert await stream_ids(valkey) == [f"{seq}-0" for seq in range(1, 5001)]
    assert contended < 3 * alone + 1


async def test_publish_stamps_only_the_events_it_published(
    client: AsyncClient, valkey: Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "relay_batch_size", 2)
    for order_ref in ("web-1", "web-2", "web-3"):
        await place_order(client, order_ref)
    await sequence_all()

    assert await publish(valkey) == 2

    published = {event.seq: event.published_at is not None for event in await events()}
    assert published == {1: True, 2: True, 3: False}


async def test_event_marked_published_before_it_had_a_seq_is_still_published(
    valkey: Redis,
) -> None:
    async with SessionLocal() as session, session.begin():
        session.add(outbox_event("legacy", published_at=func.now()))

    assert await sequence() == 1
    assert await publish(valkey) == 1
    assert [fields["order_ref"] for _, fields in await stream(valkey)] == ["legacy"]


async def test_relay_logs_counts_but_never_the_customer(client: AsyncClient, valkey: Redis) -> None:
    await place_order(client, "web-1")

    with captured_logs("app") as logs:
        await sequence_all()
        await publish(valkey)

    assert logs
    assert not [line for line in logs if "cust-42" in line]


async def test_relay_survives_valkey_outage_and_publishes_once_it_returns(
    client: AsyncClient, valkey: Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "poll_interval_seconds", 0.01)
    dead = Redis.from_url("redis://127.0.0.1:1/1")
    monkeypatch.setattr("app.valkey.valkey", dead)
    await place_order(client, "web-1")
    stop = asyncio.Event()

    async def failed_twice() -> bool:
        return sum("relay publish failed" in line for line in logs) >= 2

    async def published() -> bool:
        return await stream_ids(valkey) == ["1-0"]

    with captured_logs("app.orders.relay") as logs:
        relay = asyncio.create_task(run_relay(stop))
        try:
            await eventually(failed_twice)
            assert not relay.done()
            assert await seq_by_ref() == {"web-1": 1}
            assert await stream_ids(valkey) == []

            monkeypatch.setattr("app.valkey.valkey", valkey)
            await eventually(published)
        finally:
            stop.set()
            await asyncio.wait_for(relay, timeout=5)
            await dead.aclose()

    failures = [line for line in logs if "relay publish failed" in line]
    assert set(failures) == {"relay publish failed: error=ConnectionError"}


async def test_relay_keeps_sequencing_new_orders_while_valkey_is_down(
    client: AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "poll_interval_seconds", 60)
    monkeypatch.setattr(settings, "relay_batch_size", 10)
    dead = Redis.from_url("redis://127.0.0.1:1/1")
    monkeypatch.setattr("app.valkey.valkey", dead)
    await add_events(100)
    stop = asyncio.Event()

    async def all_in_the_feed() -> bool:
        feed = (await client.get("/events", params={"limit": 500})).json()
        return [event["seq"] for event in feed["events"]] == list(range(1, 101))

    with captured_logs("app.orders.relay") as logs:
        relay = asyncio.create_task(run_relay(stop))
        try:
            await eventually(all_in_the_feed)
        finally:
            stop.set()
            await asyncio.wait_for(relay, timeout=5)
            await dead.aclose()

    assert "relay publish failed: error=ConnectionError" in logs


async def test_relay_drains_a_backlog_larger_than_a_batch_without_waiting_to_poll(
    valkey: Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "poll_interval_seconds", 60)
    monkeypatch.setattr(settings, "relay_batch_size", 2)
    await add_events(5)
    stop = asyncio.Event()
    relay = asyncio.create_task(run_relay(stop))

    async def drained() -> bool:
        return await stream_ids(valkey) == [f"{seq}-0" for seq in range(1, 6)]

    try:
        await eventually(drained)
    finally:
        stop.set()
        await asyncio.wait_for(relay, timeout=1)


async def test_relay_publishes_what_it_sequences_while_the_publisher_waits_to_poll(
    valkey: Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "poll_interval_seconds", 60)
    await add_events(3)
    stop = asyncio.Event()

    async def sequencer_waiting() -> bool:
        return await sessions_waiting_on_locks() == 1

    async def published() -> bool:
        return await stream_ids(valkey) == ["1-0", "2-0", "3-0"]

    async with SessionLocal() as blocker:
        async with blocker.begin():
            await blocker.execute(select(func.pg_advisory_xact_lock(SEQUENCE_LOCK_KEY)))
            relay = asyncio.create_task(run_relay(stop))
            await eventually(sequencer_waiting)
            # Time for the publisher's first round to find nothing and start waiting.
            await asyncio.sleep(0.2)
            assert await stream_ids(valkey) == []
        try:
            await eventually(published)
        finally:
            stop.set()
            await asyncio.wait_for(relay, timeout=1)


async def test_relay_backs_off_when_valkey_fails_mid_drain(
    valkey: Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "poll_interval_seconds", 60)
    monkeypatch.setattr(settings, "relay_batch_size", 1)
    await add_events(1000)
    dead = Redis.from_url("redis://127.0.0.1:1/1")
    stop = asyncio.Event()

    async def draining() -> bool:
        return await valkey.xlen(settings.orders_stream) > 0

    with captured_logs("app.orders.relay") as logs:
        relay = asyncio.create_task(run_relay(stop))
        try:
            await eventually(draining)
            monkeypatch.setattr("app.valkey.valkey", dead)
            await asyncio.sleep(0.5)
        finally:
            stop.set()
            await asyncio.wait_for(relay, timeout=5)
            await dead.aclose()

    assert sum("relay publish failed" in line for line in logs) == 1


async def test_relay_stops_promptly_while_waiting_to_poll(
    client: AsyncClient, valkey: Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "poll_interval_seconds", 60)
    await place_order(client, "web-1")
    stop = asyncio.Event()
    relay = asyncio.create_task(run_relay(stop))

    async def published() -> bool:
        return await stream_ids(valkey) == ["1-0"]

    await eventually(published)
    stop.set()

    await asyncio.wait_for(relay, timeout=1)
