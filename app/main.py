import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, status
from fastapi.exceptions import RequestValidationError

from app import health
from app.catalog.router import router as catalog_router
from app.db import engine
from app.orders.router import router as orders_router
from app.valkey import valkey


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
    yield
    await valkey.aclose()
    await engine.dispose()


async def validation_error(_: Request, exc: RequestValidationError) -> Response:
    # The rejected input is not echoed: it can be bytes, NaN, a lone surrogate or deeply
    # nested, none of which encode safely, and the client already has it.
    detail = [{key: error[key] for key in ("type", "loc", "msg")} for error in exc.errors()]
    return Response(
        json.dumps({"detail": detail}, separators=(",", ":")),
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
