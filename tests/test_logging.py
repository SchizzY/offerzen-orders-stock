import asyncio
import gc
import io
import json
import logging
import logging.config
import math
import subprocess
import sys
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import contextmanager, redirect_stderr

import pytest
import uvicorn.config
from httpx import AsyncClient
from loguru import logger
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.config import Settings, settings
from app.db import engine, get_session
from app.logging import setup_logging
from app.main import app
from tests.logs import captured_logs

SECRET_CUSTOMER = "cust-detail-leak-9"
LEAKY_CUSTOMER = "cust-LEAK-7"
LEAKY_INSERT = text(
    "INSERT INTO orders (order_ref, customer_id, total_cents, request_hash)"
    f" VALUES ('leak-ref', '{LEAKY_CUSTOMER}', 1, 'h')"
)


@contextmanager
def stderr_buffer() -> Iterator[io.StringIO]:
    """Swap stderr for a buffer; on exit, point loguru back at the real stderr.

    Used inside test bodies: pytest reinstalls its own sys.stderr at the start of each phase.
    """
    try:
        with redirect_stderr(io.StringIO()) as buffer:
            yield buffer
    finally:
        setup_logging()


@pytest.fixture
async def orders_reject_secret_customer() -> AsyncIterator[None]:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "ALTER TABLE orders ADD CONSTRAINT ck_orders_test_reject"
                f" CHECK (customer_id <> '{SECRET_CUSTOMER}')"
            )
        )
    yield
    async with engine.begin() as connection:
        await connection.execute(text("ALTER TABLE orders DROP CONSTRAINT ck_orders_test_reject"))


async def test_rejected_write_returns_500_without_logging_row_values(
    client: AsyncClient, orders_reject_secret_customer: None
) -> None:
    product = {"sku": "LOG-001", "name": "Log test", "price_cents": 100, "stock": 5}
    (await client.post("/products", json=product)).raise_for_status()
    order = {
        "order_ref": "log-leak-1",
        "customer_id": SECRET_CUSTOMER,
        "items": [{"sku": "LOG-001", "qty": 1}],
    }

    with captured_logs() as logs:
        response = await client.post("/orders", json=order)

    assert response.status_code == 500
    assert response.json() == {"detail": "internal server error"}
    output = "\n".join(logs)
    assert "23514" in output  # check_violation: the rejection was logged
    assert SECRET_CUSTOMER not in output
    assert "DETAIL" not in output
    assert "Failing row" not in output


def assert_scrubbed(output: str) -> None:
    assert LEAKY_CUSTOMER not in output
    assert "leak-ref" not in output
    assert "DETAIL" not in output
    assert "[SQL" not in output
    assert "Traceback" not in output
    assert "error=IntegrityError sqlstate=23505" in output


async def insert_duplicate_order() -> None:
    async with engine.begin() as connection:
        await connection.execute(LEAKY_INSERT)
    async with engine.begin() as connection:
        await connection.execute(LEAKY_INSERT)


async def test_unawaited_task_db_error_is_logged_without_row_values() -> None:
    with captured_logs() as logs:
        task = asyncio.create_task(insert_duplicate_order())
        await asyncio.wait([task])
        del task
        gc.collect()

    output = "\n".join(logs)
    assert output.startswith("Task exception was never retrieved error=")
    assert_scrubbed(output)


def log_like_uvicorn(_: IntegrityError) -> None:
    logging.getLogger("uvicorn.error").exception("Exception in ASGI application")


def log_with_loguru_exception(_: IntegrityError) -> None:
    logger.exception("loguru exception")


def log_with_loguru_opt(error: IntegrityError) -> None:
    logger.opt(exception=error).error("loguru opt")


def log_wrapped_error(error: IntegrityError) -> None:
    try:
        raise RuntimeError("wrapped") from error
    except RuntimeError as wrapped:
        logger.opt(exception=wrapped).error("wrapped error")


@pytest.mark.parametrize(
    "emit", [log_like_uvicorn, log_with_loguru_exception, log_with_loguru_opt, log_wrapped_error]
)
async def test_logged_db_errors_are_reduced_to_class_and_sqlstate(
    emit: Callable[[IntegrityError], None],
) -> None:
    with captured_logs() as logs:
        try:
            await insert_duplicate_order()
        except IntegrityError as error:
            emit(error)

    (line,) = logs
    assert "\n" not in line
    assert_scrubbed(line)


async def test_stderr_sink_gets_the_scrubbed_db_error() -> None:
    with stderr_buffer() as stderr:
        setup_logging()
        try:
            await insert_duplicate_order()
        except IntegrityError:
            logging.getLogger("uvicorn.error").exception("Exception in ASGI application")

    assert_scrubbed(stderr.getvalue())


@pytest.mark.parametrize("error", [TimeoutError(), ConnectionRefusedError(61, "refused")])
async def test_unhandled_request_error_returns_json_500_and_one_log_line(
    client: AsyncClient, error: Exception
) -> None:
    async def broken_session() -> None:
        raise error

    app.dependency_overrides[get_session] = broken_session
    try:
        with captured_logs("app") as logs:
            response = await client.get("/orders/any-ref")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 500
    assert response.json() == {"detail": "internal server error"}
    (line,) = logs
    assert f"error={type(error).__name__} sqlstate=-" in line
    assert "\n" not in line


def test_unformattable_stdlib_record_does_not_raise_or_log_its_args() -> None:
    with stderr_buffer() as stderr:
        setup_logging()
        logging.getLogger("x").warning("bad %d", "cust-args-leak")

    output = stderr.getvalue()
    assert "unformattable log record" in output
    assert "cust-args-leak" not in output


def test_setup_logging_survives_its_sink_being_removed_elsewhere() -> None:
    with stderr_buffer() as stderr:
        setup_logging()
        logger.remove()
        setup_logging()
        logger.warning("after re-setup")

    assert stderr.getvalue().count("after re-setup") == 1


def test_first_setup_keeps_sinks_registered_before_it() -> None:
    script = (
        "import sys\n"
        "from loguru import logger\n"
        "logger.add(sys.stdout, format='{message}')\n"
        "from app.logging import setup_logging\n"
        "setup_logging()\n"
        "logger.warning('early sink probe')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )

    assert result.stdout.count("early sink probe") == 1
    assert result.stderr.count("early sink probe") == 1


@pytest.mark.parametrize(
    ("value", "expected"),
    [("warn", "WARNING"), ("WARN", "WARNING"), ("debug", "DEBUG"), ("Error", "ERROR")],
)
def test_settings_normalise_log_level(value: str, expected: str) -> None:
    assert Settings(log_level=value).log_level == expected


def test_settings_reject_unknown_log_level() -> None:
    with pytest.raises(ValidationError, match="log_level"):
        Settings(log_level="loud")


@pytest.mark.parametrize(
    "name", ["uvicorn", "uvicorn.error", "uvicorn.access", "asyncpg", "sqlalchemy.engine.Engine"]
)
def test_stdlib_records_reach_loguru_and_stderr_exactly_once(name: str) -> None:
    expected = f"probe from {name}"
    with stderr_buffer() as stderr:
        logging.config.dictConfig(uvicorn.config.LOGGING_CONFIG)
        setup_logging()
        setup_logging()

        with captured_logs() as logs:
            logging.getLogger(name).warning("probe from %s", name)

    assert logs.count(expected) == 1
    assert stderr.getvalue().count(expected) == 1


def test_json_mode_writes_one_json_object_per_line(monkeypatch: pytest.MonkeyPatch) -> None:
    with stderr_buffer() as stderr, monkeypatch.context() as patch:
        patch.setattr(settings, "log_json", True)
        setup_logging()
        logging.getLogger("uvicorn.error").warning("json probe")

    (line,) = stderr.getvalue().splitlines()
    record = json.loads(line)["record"]
    assert record["message"] == "json probe"
    assert record["level"]["name"] == "WARNING"


@pytest.mark.parametrize("field", ["db_connect_timeout_seconds", "db_command_timeout_seconds"])
@pytest.mark.parametrize("value", [0, -1, math.inf, math.nan])
def test_settings_reject_unusable_db_timeouts(field: str, value: float) -> None:
    with pytest.raises(ValidationError):
        Settings(**{field: value})


@pytest.mark.parametrize("field", ["db_connect_timeout_seconds", "db_command_timeout_seconds"])
def test_settings_accept_a_positive_db_timeout(field: str) -> None:
    assert getattr(Settings(**{field: 0.25}), field) == 0.25


@pytest.mark.parametrize("value", [0, 1001])
def test_settings_reject_relay_batch_size_out_of_range(value: int) -> None:
    with pytest.raises(ValidationError):
        Settings(relay_batch_size=value)


@pytest.mark.parametrize("value", [1, 1000])
def test_settings_accept_relay_batch_size_bounds(value: int) -> None:
    assert Settings(relay_batch_size=value).relay_batch_size == value


@pytest.mark.parametrize("value", [0, -1, math.inf, math.nan])
def test_settings_reject_unusable_poll_interval(value: float) -> None:
    with pytest.raises(ValidationError):
        Settings(poll_interval_seconds=value)


def test_settings_relay_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RELAY_BATCH_SIZE", raising=False)
    monkeypatch.delenv("POLL_INTERVAL_SECONDS", raising=False)
    defaults = Settings(_env_file=None)

    assert defaults.relay_batch_size == 100
    assert defaults.poll_interval_seconds == 0.5
