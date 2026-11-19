from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import health
from app.catalog.router import router as catalog_router
from app.db import engine
from app.valkey import valkey


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    yield
    await valkey.aclose()
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="Orders & Stock", lifespan=lifespan)
    app.include_router(health.router)
    app.include_router(catalog_router)
    return app


app = create_app()
