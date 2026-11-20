import asyncio
import os
import signal
import sys
from pathlib import Path

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis

from tests.test_stock import CUSTOMER, add_product, eventually, on_hand, place_order, publish

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("stop_signal", [signal.SIGTERM, signal.SIGINT], ids=["SIGTERM", "SIGINT"])
async def test_worker_process_applies_events_and_exits_cleanly_on_signal(
    client: AsyncClient, valkey: Redis, stop_signal: signal.Signals
) -> None:
    await add_product(client, "APL-1", 10)
    await place_order(client, "web-1", {"APL-1": 3})

    async def reserved() -> bool:
        return await on_hand(client, "APL-1") == 7

    worker = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "app.stock.worker",
        cwd=ROOT,
        env={**os.environ, "LOG_LEVEL": "INFO", "LOG_JSON": "false"},
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        await publish(valkey, "web-1", 1)
        await eventually(reserved)
        worker.send_signal(stop_signal)
        _, stderr = await asyncio.wait_for(worker.communicate(), 10)
    finally:
        if worker.returncode is None:
            worker.kill()
            await worker.wait()

    assert worker.returncode == 0
    output = stderr.decode()
    assert "order_ref=web-1" in output
    assert "result=reserved" in output
    assert CUSTOMER not in output
