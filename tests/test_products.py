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
