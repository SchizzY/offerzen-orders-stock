import io
import json
import logging
import logging.config
import math
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager, redirect_stderr

import pytest
import uvicorn.config
from httpx import AsyncClient
from pydantic import ValidationError
from sqlalchemy import text

from app.config import Settings, settings
from app.db import engine
from app.logging import setup_logging
from tests.logs import captured_logs

SECRET_CUSTOMER = "cust-detail-leak-9"


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


@pytest.mark.parametrize("name", ["uvicorn", "uvicorn.error", "uvicorn.access", "asyncpg"])
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
