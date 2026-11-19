import asyncio
import json
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


INT32_MAX = 2_147_483_647


def item(qty: Any = 1, sku: str = "BAN-001") -> dict[str, Any]:
    return {"items": [{"sku": sku, "qty": qty}]}


@pytest.mark.parametrize(
    ("change", "loc"),
    [
        ({"order_ref": ""}, ["body", "order_ref"]),
        ({"order_ref": "   "}, ["body", "order_ref"]),
        ({"order_ref": "web\x00100045"}, ["body", "order_ref"]),
        ({"order_ref": "web-\ud800"}, ["body", "order_ref"]),
        ({"order_ref": "web/2026/001"}, ["body", "order_ref"]),
        ({"order_ref": "r" * 65}, ["body", "order_ref"]),
        ({"order_ref": "r" * 4000}, ["body", "order_ref"]),
        ({"customer_id": ""}, ["body", "customer_id"]),
        ({"customer_id": "cust\x0042"}, ["body", "customer_id"]),
        ({"items": []}, ["body", "items"]),
        ({"items": [{"sku": f"SKU-{n}", "qty": 1} for n in range(101)]}, ["body", "items"]),
        (item(sku=""), ["body", "items", 0, "sku"]),
        (item(sku="BAN\x00001"), ["body", "items", 0, "sku"]),
        (item(qty=0), ["body", "items", 0, "qty"]),
        (item(qty=10_001), ["body", "items", 0, "qty"]),
        (item(qty=INT32_MAX + 1), ["body", "items", 0, "qty"]),
        (item(qty=True), ["body", "items", 0, "qty"]),
        (item(qty=1.0), ["body", "items", 0, "qty"]),
        (
            {"items": [{"sku": "BAN-001", "qty": 1}, {"sku": "BAN-001", "qty": 2}]},
            ["body", "items"],
        ),
    ],
    ids=[
        "empty-order-ref",
        "blank-order-ref",
        "nul-in-order-ref",
        "lone-surrogate-in-order-ref",
        "slash-in-order-ref",
        "order-ref-65-chars",
        "order-ref-4000-chars",
        "empty-customer",
        "nul-in-customer",
        "no-items",
        "101-items",
        "empty-sku",
        "nul-in-sku",
        "zero-qty",
        "qty-over-10000",
        "qty-over-int32",
        "qty-bool",
        "qty-float",
        "duplicate-sku",
    ],
)
async def test_invalid_order_is_rejected(
    client: AsyncClient, change: dict[str, Any], loc: list[str | int]
) -> None:
    # ensure_ascii keeps a lone surrogate as a JSON escape, as a real client would send it.
    response = await client.post(
        "/orders",
        content=json.dumps({**ORDER, **change}),
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 422
    assert loc in [error["loc"] for error in response.json()["detail"]]
    assert await count(Order) == 0


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity", "1e999"])
async def test_rejection_of_non_finite_number_is_standard_json(
    client: AsyncClient, literal: str
) -> None:
    body = json.dumps({**ORDER, "items": [{"sku": "BAN-001", "qty": 0}]}).replace(
        '"qty": 0', f'"qty": {literal}'
    )
    response = await client.post(
        "/orders", content=body, headers={"Content-Type": "application/json"}
    )

    def reject(constant: str) -> None:
        raise ValueError(f"non-standard JSON constant {constant}")

    assert response.status_code == 422
    detail = json.loads(response.text, parse_constant=reject)["detail"]
    assert ["body", "items", 0, "qty"] in [error["loc"] for error in detail]


async def test_order_accepts_the_largest_quantity(client: AsyncClient) -> None:
    response = await client.post("/orders", json={**ORDER, **item(qty=10_000)})

    assert response.status_code == 201
    assert response.json()["total_cents"] == 10_000 * 199


@pytest.mark.parametrize(
    "order_ref", ["%00", "web%20100045", ".web"], ids=["nul", "space", "leading-dot"]
)
async def test_get_with_a_malformed_order_ref_returns_422(
    client: AsyncClient, order_ref: str
) -> None:
    response = await client.get(f"/orders/{order_ref}")

    assert response.status_code == 422
    assert [error["loc"] for error in response.json()["detail"]] == [["path", "order_ref"]]


async def test_replay_after_a_price_change_returns_the_original_order(
    client: AsyncClient,
) -> None:
    first = await client.post("/orders", json=ORDER)
    (await client.post("/products", json={**BANANAS, "price_cents": 999})).raise_for_status()

    replay = await client.post("/orders", json=ORDER)

    assert first.status_code == 201
    assert replay.status_code == 200
    assert replay.headers["Idempotent-Replay"] == "true"
    assert replay.json() == first.json()
    assert replay.json()["total_cents"] == 2 * 199 + 349
    assert {line["sku"]: line["unit_price_cents"] for line in replay.json()["items"]} == {
        "APL-003": 349,
        "BAN-001": 199,
    }


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
