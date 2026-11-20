import asyncio
import re
from collections.abc import AsyncGenerator, Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.db import engine

MIGRATIONS = Path(__file__).resolve().parents[1] / "migrations"
PUBLISHED = datetime(2026, 11, 19, 12, tzinfo=UTC)

PHASE_1_ROWS = [
    "INSERT INTO products (sku, name, price_cents, stock) VALUES ('BAN-001', 'Bananas', 199, 50)",
    """INSERT INTO orders (id, order_ref, customer_id, total_cents, request_hash) VALUES
        ('00000000-0000-0000-0000-000000000001', 'web-1', 'cust-1', 398, 'h1'),
        ('00000000-0000-0000-0000-000000000002', 'web-2', 'cust-2', 199, 'h2')""",
    """INSERT INTO order_items (order_id, sku, qty, unit_price_cents) VALUES
        ('00000000-0000-0000-0000-000000000001', 'BAN-001', 2, 199),
        ('00000000-0000-0000-0000-000000000002', 'BAN-001', 1, 199)""",
    """INSERT INTO outbox_events (event_type, aggregate_id, payload, processed_at) VALUES
        ('order.accepted', '00000000-0000-0000-0000-000000000001', '{"order_ref": "web-1"}', NULL),
        ('order.accepted', '00000000-0000-0000-0000-000000000002', '{"order_ref": "web-2"}',
         '2026-11-19T12:00:00Z')""",
]

PHASE_1_COLUMNS = {
    "orders": {
        "id",
        "order_ref",
        "customer_id",
        "total_cents",
        "status",
        "request_hash",
        "created_at",
    },
    "outbox_events": {"id", "event_type", "aggregate_id", "payload", "created_at", "processed_at"},
}


class Scratch:
    def __init__(self, database: str) -> None:
        url = engine.url.set(database=database)
        self.engine = create_async_engine(url, poolclass=NullPool)
        self.config = Config()
        self.config.set_main_option("script_location", str(MIGRATIONS))
        self.config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False))

    async def alembic(self, step: Callable[..., None], *args: str) -> None:
        # env.py drives its own event loop, so it runs off this test's loop.
        await asyncio.to_thread(step, self.config, *args)

    async def rows(self, sql: str) -> list[tuple[object, ...]]:
        async with self.engine.connect() as connection:
            return [tuple(row) for row in await connection.execute(text(sql))]

    async def columns(self, table: str) -> set[str]:
        rows = await self.rows(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = 'public' AND table_name = '{table}'"
        )
        return {name for (name,) in rows}


@pytest.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    database = re.sub(r"_test$", "_migrations_test", engine.url.database or "")
    admin = engine.execution_options(isolation_level="AUTOCOMMIT")
    async with admin.connect() as connection:
        await connection.execute(text(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'))
        await connection.execute(text(f'CREATE DATABASE "{database}"'))
    scratch = Scratch(database)
    yield scratch
    await scratch.engine.dispose()
    async with admin.connect() as connection:
        await connection.execute(text(f'DROP DATABASE "{database}" WITH (FORCE)'))


async def test_0002_keeps_phase_1_rows_through_upgrade_downgrade_and_upgrade(
    scratch: Scratch,
) -> None:
    await scratch.alembic(command.upgrade, "0001")
    async with scratch.engine.begin() as connection:
        for statement in PHASE_1_ROWS:
            await connection.execute(text(statement))

    await scratch.alembic(command.upgrade, "head")

    assert await scratch.rows("SELECT order_ref, stock_status FROM orders ORDER BY 1") == [
        ("web-1", "pending"),
        ("web-2", "pending"),
    ]
    assert await scratch.rows("SELECT id, seq, published_at FROM outbox_events ORDER BY 1") == [
        (1, None, None),
        (2, None, PUBLISHED),
    ]
    assert await scratch.rows("SELECT count(*) FROM order_items") == [(2,)]
    for status in ("reserved", "insufficient", "pending"):
        async with scratch.engine.begin() as connection:
            await connection.execute(
                text("UPDATE orders SET stock_status = :status"), {"status": status}
            )
    with pytest.raises(IntegrityError):
        async with scratch.engine.begin() as connection:
            await connection.execute(text("UPDATE orders SET stock_status = 'shipped'"))
    assert await scratch.rows(
        "SELECT pg_get_indexdef('ix_outbox_events_unsequenced'::regclass),"
        " pg_get_indexdef('ix_orders_stock_pending'::regclass)"
    ) == [
        (
            "CREATE INDEX ix_outbox_events_unsequenced ON public.outbox_events"
            " USING btree (id) WHERE (seq IS NULL)",
            "CREATE INDEX ix_orders_stock_pending ON public.orders"
            " USING btree (id) WHERE (stock_status = 'pending'::text)",
        )
    ]
    await scratch.alembic(command.check)

    await scratch.alembic(command.downgrade, "0001")

    assert {table: await scratch.columns(table) for table in PHASE_1_COLUMNS} == PHASE_1_COLUMNS
    assert await scratch.rows("SELECT id, processed_at FROM outbox_events ORDER BY 1") == [
        (1, None),
        (2, PUBLISHED),
    ]
    assert await scratch.rows("SELECT order_ref FROM orders ORDER BY 1") == [("web-1",), ("web-2",)]

    await scratch.alembic(command.upgrade, "head")

    assert await scratch.rows("SELECT order_ref, stock_status FROM orders ORDER BY 1") == [
        ("web-1", "pending"),
        ("web-2", "pending"),
    ]
    assert await scratch.rows("SELECT id, seq, published_at FROM outbox_events ORDER BY 1") == [
        (1, None, None),
        (2, None, PUBLISHED),
    ]
