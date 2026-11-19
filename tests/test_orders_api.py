import asyncio
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

from app.db import Base, SessionLocal
from app.orders.models import Order, OrderItem, OutboxEvent

BANANAS = {"sku": "BAN-001", "name": "Bananas 1kg", "price_cents": 199, "stock": 50}
APPLES = {"sku": "APL-003", "name": "Apples 1kg", "price_cents": 349, "stock": 20}

ORDER: dict[str, Any] = {
    "order_ref": "web-100045",
    "customer_id": "cust-42",
    "items": [{"sku": "BAN-001", "qty": 2}, {"sku": "APL-003", "qty": 1}],
}


@pytest.fixture(autouse=True)
async def products(client: AsyncClient) -> None:
    for product in (BANANAS, APPLES):
        (await client.post("/products", json=product)).raise_for_status()


async def count(model: type[Base]) -> int:
    async with SessionLocal() as session:
        return (await session.execute(select(func.count()).select_from(model))).scalar_one()


async def test_create_prices_each_line_from_the_catalog(client: AsyncClient) -> None:
    response = await client.post("/orders", json=ORDER)

    assert response.status_code == 201
    assert response.headers["Idempotent-Replay"] == "false"
    body = response.json()
    assert body["order_ref"] == "web-100045"
    assert body["customer_id"] == "cust-42"
    assert body["status"] == "accepted"
    assert body["total_cents"] == 2 * 199 + 349
    assert body["items"] == [
        {"sku": "APL-003", "qty": 1, "unit_price_cents": 349, "line_total_cents": 349},
        {"sku": "BAN-001", "qty": 2, "unit_price_cents": 199, "line_total_cents": 398},
    ]
    assert (await client.get("/orders/web-100045")).json() == body


async def test_order_keeps_the_prices_from_the_time_it_was_placed(client: AsyncClient) -> None:
    created = (await client.post("/orders", json=ORDER)).json()
    (await client.post("/products", json={**BANANAS, "price_cents": 999})).raise_for_status()

    fetched = (await client.get("/orders/web-100045")).json()

    assert fetched == created
    assert fetched["total_cents"] == 2 * 199 + 349


async def test_accepted_order_writes_one_outbox_event_with_the_order(client: AsyncClient) -> None:
    body = (await client.post("/orders", json=ORDER)).json()

    async with SessionLocal() as session:
        order_id = (await session.execute(select(Order.id))).scalar_one()
        event = (await session.execute(select(OutboxEvent))).scalar_one()

    assert event.event_type == "order.accepted"
    assert event.aggregate_id == order_id
    assert event.payload == body
    assert event.processed_at is None


@pytest.mark.parametrize(
    "duplicate",
    [ORDER, {**ORDER, "items": list(reversed(ORDER["items"]))}],
    ids=["identical", "items-reordered"],
)
async def test_duplicate_submission_replays_the_original_order(
    client: AsyncClient, duplicate: dict[str, Any]
) -> None:
    first = await client.post("/orders", json=ORDER)
    second = await client.post("/orders", json=duplicate)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.headers["Idempotent-Replay"] == "true"
    assert second.json() == first.json()
    assert await count(Order) == 1
    assert await count(OrderItem) == 2
    assert await count(OutboxEvent) == 1


async def test_reusing_an_order_ref_with_a_different_payload_conflicts(
    client: AsyncClient,
) -> None:
    original = (await client.post("/orders", json=ORDER)).json()

    changed = {**ORDER, "items": [{"sku": "BAN-001", "qty": 3}]}
    response = await client.post("/orders", json=changed)

    assert response.status_code == 409
    assert response.json()["detail"]["order_ref"] == "web-100045"
    assert (await client.get("/orders/web-100045")).json() == original
    assert await count(Order) == 1
    assert await count(OutboxEvent) == 1


async def test_unknown_sku_is_rejected_and_leaves_nothing_behind(client: AsyncClient) -> None:
    rejected = await client.post(
        "/orders",
        json={**ORDER, "items": [{"sku": "BAN-001", "qty": 1}, {"sku": "NOPE-404", "qty": 1}]},
    )

    assert rejected.status_code == 422
    assert [error["input"] for error in rejected.json()["detail"]] == ["NOPE-404"]
    assert await count(Order) == 0
    assert await count(OutboxEvent) == 0
    assert (await client.post("/orders", json=ORDER)).status_code == 201


@pytest.mark.parametrize(
    "change",
    [
        {"order_ref": ""},
        {"order_ref": "   "},
        {"customer_id": ""},
        {"items": []},
        {"items": [{"sku": "", "qty": 1}]},
        {"items": [{"sku": "BAN-001", "qty": 0}]},
        {"items": [{"sku": "BAN-001", "qty": 1}, {"sku": "BAN-001", "qty": 2}]},
    ],
    ids=[
        "empty-order-ref",
        "blank-order-ref",
        "empty-customer",
        "no-items",
        "empty-sku",
        "zero-qty",
        "duplicate-sku",
    ],
)
async def test_invalid_order_is_rejected(client: AsyncClient, change: dict[str, Any]) -> None:
    response = await client.post("/orders", json={**ORDER, **change})

    assert response.status_code == 422
    assert await count(Order) == 0


async def test_get_unknown_order_returns_404(client: AsyncClient) -> None:
    response = await client.get("/orders/web-missing")

    assert response.status_code == 404


async def test_concurrent_duplicates_count_once(client: AsyncClient) -> None:
    responses = await asyncio.gather(*(client.post("/orders", json=ORDER) for _ in range(10)))

    assert sorted(r.status_code for r in responses) == [200] * 9 + [201]
    assert sorted(r.headers["Idempotent-Replay"] for r in responses) == ["false"] + ["true"] * 9
    assert all(r.json() == responses[0].json() for r in responses)
    assert await count(Order) == 1
    assert await count(OrderItem) == 2
    assert await count(OutboxEvent) == 1
