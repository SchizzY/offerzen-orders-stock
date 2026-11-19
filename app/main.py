import json
import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError

from app import health
from app.catalog.router import router as catalog_router
from app.db import engine
from app.orders.router import router as orders_router
from app.valkey import valkey


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    yield
    await valkey.aclose()
    await engine.dispose()


def _standard_json(value: object) -> object:
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {key: _standard_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_standard_json(item) for item in value]
    return value


async def validation_error(_: Request, exc: RequestValidationError) -> Response:
    # Errors echo the rejected input; ASCII escaping keeps a lone surrogate encodable,
    # and non-finite floats become strings because NaN/Infinity are not JSON.
    detail = _standard_json(jsonable_encoder(exc.errors()))
    return Response(
        json.dumps({"detail": detail}, separators=(",", ":"), allow_nan=False),
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        media_type="application/json",
    )


def create_app() -> FastAPI:
    app = FastAPI(title="Orders & Stock", lifespan=lifespan)
    app.add_exception_handler(RequestValidationError, validation_error)
    app.include_router(health.router)
    app.include_router(catalog_router)
    app.include_router(orders_router)
    return app


app = create_app()
