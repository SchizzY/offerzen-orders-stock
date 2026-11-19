import json
from typing import Any

import pytest
from httpx import AsyncClient

BANANAS = {"sku": "BAN-001", "name": "Bananas 1kg", "price_cents": 199, "stock": 50}
APPLES = {"sku": "APL-003", "name": "Apples 1kg", "price_cents": 349, "stock": 20}


async def test_upsert_creates_a_product_that_can_be_fetched(client: AsyncClient) -> None:
    created = await client.post("/products", json=BANANAS)
    fetched = await client.get("/products/BAN-001")

    assert created.status_code == 200
    assert created.json() == BANANAS
    assert fetched.status_code == 200
    assert fetched.json() == BANANAS


async def test_upsert_with_an_existing_sku_replaces_the_product(client: AsyncClient) -> None:
    await client.post("/products", json=BANANAS)
    changed = {**BANANAS, "name": "Bananas 2kg", "price_cents": 379, "stock": 12}

    updated = await client.post("/products", json=changed)
    listed = await client.get("/products")

    assert updated.status_code == 200
    assert updated.json() == changed
    assert listed.json() == [changed]


async def test_get_unknown_sku_returns_404(client: AsyncClient) -> None:
    response = await client.get("/products/NOPE-404")

    assert response.status_code == 404


async def test_list_returns_every_product_ordered_by_sku(client: AsyncClient) -> None:
    await client.post("/products", json=BANANAS)
    await client.post("/products", json=APPLES)

    response = await client.get("/products")

    assert response.status_code == 200
    assert response.json() == [APPLES, BANANAS]


INT32_MAX = 2_147_483_647


@pytest.mark.parametrize(
    ("change", "field"),
    [
        ({"price_cents": INT32_MAX + 1}, "price_cents"),
        ({"stock": INT32_MAX + 1}, "stock"),
        ({"price_cents": -1}, "price_cents"),
        ({"stock": True}, "stock"),
        ({"sku": "BAN\x00001"}, "sku"),
        ({"sku": "A/B"}, "sku"),
        ({"sku": "   "}, "sku"),
        ({"sku": "B" * 65}, "sku"),
        ({"name": "Bananas\x00"}, "name"),
        ({"name": "Bananas \ud800"}, "name"),
        ({"name": "   "}, "name"),
        ({"name": "x" * 201}, "name"),
    ],
    ids=[
        "price-over-int32",
        "stock-over-int32",
        "negative-price",
        "stock-bool",
        "nul-in-sku",
        "slash-in-sku",
        "blank-sku",
        "long-sku",
        "nul-in-name",
        "lone-surrogate-in-name",
        "blank-name",
        "long-name",
    ],
)
async def test_invalid_product_is_rejected_and_not_stored(
    client: AsyncClient, change: dict[str, Any], field: str
) -> None:
    response = await client.post(
        "/products",
        content=json.dumps({**BANANAS, **change}),
        headers={"Content-Type": "application/json"},
    )

    assert response.status_code == 422
    assert ["body", field] in [error["loc"] for error in response.json()["detail"]]
    assert (await client.get("/products")).json() == []


async def test_upsert_accepts_the_int32_maximum(client: AsyncClient) -> None:
    biggest = {**BANANAS, "price_cents": INT32_MAX, "stock": INT32_MAX}

    response = await client.post("/products", json=biggest)

    assert response.status_code == 200
    assert (await client.get("/products/BAN-001")).json() == biggest


@pytest.mark.parametrize("sku", ["%00", "BAN%20001", "-BAN"], ids=["nul", "space", "leading-dash"])
async def test_get_with_a_malformed_sku_returns_422(client: AsyncClient, sku: str) -> None:
    response = await client.get(f"/products/{sku}")

    assert response.status_code == 422
    assert [error["loc"] for error in response.json()["detail"]] == [["path", "sku"]]
