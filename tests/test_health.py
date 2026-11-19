from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis

from app.main import create_app
from app.valkey import get_valkey


async def test_health_reports_both_dependencies_ok(client: AsyncClient) -> None:
    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"postgres": "ok", "valkey": "ok"}


async def test_health_returns_503_naming_the_unreachable_dependency() -> None:
    unreachable = Redis.from_url("redis://localhost:1/0")
    app = create_app()
    app.dependency_overrides[get_valkey] = lambda: unreachable
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")
    await unreachable.aclose()

    assert response.status_code == 503
    assert response.json() == {"postgres": "ok", "valkey": "unavailable"}
