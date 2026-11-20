from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from app.db import engine
from app.events.router import router as events_router
from app.main import create_app

BANANAS = {"sku": "BAN-001", "name": "Bananas 1kg", "price_cents": 199, "stock": 50}
APPLES = {"sku": "APL-003", "name": "Apples 1kg", "price_cents": 349, "stock": 20}


def feed_app() -> FastAPI:
    app = create_app()
    if not any(getattr(route, "path", None) == "/events" for route in app.routes):
        app.include_router(events_router)
    return app


@pytest.fixture
async def feed() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(transport=ASGITransport(app=feed_app()), base_url="http://test") as c:
        for product in (BANANAS, APPLES):
            (await c.post("/products", json=product)).raise_for_status()
        yield c


async def place(client: AsyncClient, order_ref: str, items: dict[str, int]) -> dict[str, Any]:
    response = await client.post(
        "/orders",
        json={
            "order_ref": order_ref,
            "customer_id": "cust-42",
            "items": [{"sku": sku, "qty": qty} for sku, qty in items.items()],
        },
    )
    assert response.status_code == 201
    return response.json()


async def sequence(*order_refs: str) -> None:
    """Stand in for the relay: give these orders' events seq 1, 2, ... in the order given."""
    async with engine.begin() as connection:
        for seq, order_ref in enumerate(order_refs, start=1):
            await connection.execute(
                text("UPDATE outbox_events SET seq = :seq WHERE payload->>'order_ref' = :ref"),
                {"seq": seq, "ref": order_ref},
            )


async def page(client: AsyncClient, **params: int) -> dict[str, Any]:
    response = await client.get("/events", params=params)
    assert response.status_code == 200
    return response.json()


def refs(body: dict[str, Any]) -> list[str]:
    return [event["data"]["order_ref"] for event in body["events"]]


async def test_events_are_ordered_by_seq_not_insert_order(feed: AsyncClient) -> None:
    for ref in ("ord-a", "ord-b", "ord-c"):
        await place(feed, ref, {"BAN-001": 1})
    await sequence("ord-c", "ord-a", "ord-b")

    body = await page(feed)

    assert refs(body) == ["ord-c", "ord-a", "ord-b"]
    assert [event["seq"] for event in body["events"]] == [1, 2, 3]
    assert body["next_after"] == 3


async def test_unsequenced_events_are_not_served(feed: AsyncClient) -> None:
    for ref in ("ord-a", "ord-b", "ord-c"):
        await place(feed, ref, {"BAN-001": 1})
    await sequence("ord-b")

    body = await page(feed)

    assert refs(body) == ["ord-b"]
    assert body["next_after"] == 1


async def test_next_after_pages_through_the_feed(feed: AsyncClient) -> None:
    for ref in ("ord-a", "ord-b", "ord-c", "ord-d", "ord-e"):
        await place(feed, ref, {"BAN-001": 1})
    await sequence("ord-a", "ord-b", "ord-c", "ord-d", "ord-e")

    seen: list[str] = []
    after = 0
    for expected in (["ord-a", "ord-b"], ["ord-c", "ord-d"], ["ord-e"]):
        body = await page(feed, after=after, limit=2)
        assert refs(body) == expected
        seen += refs(body)
        after = body["next_after"]

    assert seen == ["ord-a", "ord-b", "ord-c", "ord-d", "ord-e"]
    assert after == 5


async def test_an_empty_page_keeps_the_cursor(feed: AsyncClient) -> None:
    await place(feed, "ord-a", {"BAN-001": 1})
    await sequence("ord-a")

    assert await page(feed, after=1) == {"events": [], "next_after": 1}
    assert await page(feed, after=40) == {"events": [], "next_after": 40}


async def test_an_empty_feed_starts_at_zero(feed: AsyncClient) -> None:
    assert await page(feed) == {"events": [], "next_after": 0}


async def test_event_carries_the_accepted_order(feed: AsyncClient) -> None:
    order = await place(feed, "web-100045", {"BAN-001": 2, "APL-003": 1})
    await sequence("web-100045")

    [event] = (await page(feed))["events"]

    async with engine.connect() as connection:
        order_id, occurred_at = (
            await connection.execute(text("SELECT aggregate_id, created_at FROM outbox_events"))
        ).one()
    assert set(event) == {"seq", "type", "order_id", "occurred_at", "data"}
    assert event["seq"] == 1
    assert event["type"] == "order.accepted"
    assert event["order_id"] == str(order_id)
    assert datetime.fromisoformat(event["occurred_at"]) == occurred_at
    assert datetime.fromisoformat(event["occurred_at"]).utcoffset().total_seconds() == 0
    assert event["data"] == {
        "order_ref": "web-100045",
        "customer_id": "cust-42",
        "status": "accepted",
        "total_cents": 2 * 199 + 349,
        "created_at": order["created_at"],
        "items": [
            {"sku": "APL-003", "qty": 1, "unit_price_cents": 349, "line_total_cents": 349},
            {"sku": "BAN-001", "qty": 2, "unit_price_cents": 199, "line_total_cents": 398},
        ],
    }


@pytest.mark.parametrize(
    ("params", "loc"),
    [
        ({"after": -1}, ["query", "after"]),
        ({"after": 2**63}, ["query", "after"]),
        ({"after": "x"}, ["query", "after"]),
        ({"limit": 0}, ["query", "limit"]),
        ({"limit": 501}, ["query", "limit"]),
        ({"limit": "x"}, ["query", "limit"]),
    ],
)
async def test_out_of_range_params_are_rejected(
    feed: AsyncClient, params: dict[str, Any], loc: list[str]
) -> None:
    response = await feed.get("/events", params=params)

    assert response.status_code == 422
    [error] = response.json()["detail"]
    assert set(error) == {"type", "loc", "msg"}
    assert error["loc"] == loc


async def test_limit_bounds_are_inclusive(feed: AsyncClient) -> None:
    await place(feed, "ord-a", {"BAN-001": 1})
    await place(feed, "ord-b", {"BAN-001": 1})
    await sequence("ord-a", "ord-b")

    assert refs(await page(feed, limit=1)) == ["ord-a"]
    assert refs(await page(feed, limit=500)) == ["ord-a", "ord-b"]
    assert await page(feed, after=2**63 - 1) == {"events": [], "next_after": 2**63 - 1}
