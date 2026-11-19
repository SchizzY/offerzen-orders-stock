import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, status
from fastapi.exceptions import RequestValidationError
from loguru import logger
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app import health
from app.catalog.router import router as catalog_router
from app.db import engine
from app.logging import describe_error, setup_logging
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


class CatchUnhandledErrors:
    """Answer any unhandled request error with a JSON 500 and one data-free log line.

    Starlette's own catch-all handler re-raises to the server, which logs the traceback.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        response_started = False

        async def tracking_send(message: Message) -> None:
            nonlocal response_started
            response_started = response_started or message["type"] == "http.response.start"
            await send(message)

        try:
            await self.app(scope, receive, tracking_send)
        except Exception as error:
            if response_started:
                raise
            logger.error(
                "unhandled error in {} {}: {}",
                scope["method"],
                scope["path"],
                describe_error(error),
            )
            response = Response(
                '{"detail":"internal server error"}',
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                media_type="application/json",
            )
            await response(scope, receive, send)


def create_app() -> FastAPI:
    setup_logging()
    app = FastAPI(title="Orders & Stock", lifespan=lifespan)
    app.add_exception_handler(RequestValidationError, validation_error)
    app.add_middleware(CatchUnhandledErrors)
    app.include_router(health.router)
    app.include_router(catalog_router)
    app.include_router(orders_router)
    return app


app = create_app()
