import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from app.db import engine


async def test_database_errors_do_not_echo_bound_parameters() -> None:
    async with engine.connect() as connection:
        with pytest.raises(DBAPIError) as raised:
            await connection.execute(
                text("SELECT 1 / 0, CAST(:value AS text)"), {"value": "cust-42"}
            )

    assert "cust-42" not in str(raised.value)
