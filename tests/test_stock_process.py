import asyncio
import contextlib
import os
import signal
import sys
from pathlib import Path

import pytest
from httpx import AsyncClient
from redis.asyncio import Redis

from tests.test_stock import (
    CUSTOMER,
    GROUP,
    STREAM,
    add_product,
    eventually,
    on_hand,
    place_order,
    publish,
)

ROOT = Path(__file__).resolve().parents[1]
SIGNALS = pytest.mark.parametrize(
    "stop_signal", [signal.SIGTERM, signal.SIGINT], ids=["SIGTERM", "SIGINT"]
)


async def start_worker() -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "app.stock.worker",
        cwd=ROOT,
        env={**os.environ, "LOG_LEVEL": "INFO", "LOG_JSON": "false"},
        stderr=asyncio.subprocess.PIPE,
    )


async def stop_worker(worker: asyncio.subprocess.Process, stop_signal: signal.Signals) -> str:
    try:
        if worker.returncode is None:
            worker.send_signal(stop_signal)
        _, stderr = await asyncio.wait_for(worker.communicate(), 10)
    finally:
        if worker.returncode is None:
            worker.kill()
            await worker.wait()
    return stderr.decode()


@SIGNALS
async def test_worker_process_applies_events_and_exits_cleanly_on_signal(
    client: AsyncClient, valkey: Redis, stop_signal: signal.Signals
) -> None:
    await add_product(client, "APL-1", 10)
    await place_order(client, "web-1", {"APL-1": 3})

    async def reserved() -> bool:
        return await on_hand(client, "APL-1") == 7

    worker = await start_worker()
    try:
        await publish(valkey, "web-1", 1)
        await eventually(reserved)
    finally:
        output = await stop_worker(worker, stop_signal)

    assert worker.returncode == 0
    assert "order_ref=web-1" in output
    assert "result=reserved" in output
    assert CUSTOMER not in output


@SIGNALS
@pytest.mark.parametrize("delay", [0.05, 0.3])
async def test_worker_process_exits_cleanly_on_a_signal_while_it_is_still_starting(
    stop_signal: signal.Signals, delay: float
) -> None:
    worker = await start_worker()
    await asyncio.sleep(delay)
    output = await stop_worker(worker, stop_signal)

    assert worker.returncode == 0
    assert "Traceback" not in output


@SIGNALS
async def test_worker_process_exits_cleanly_when_signalled_again_while_stopping(
    valkey: Redis, stop_signal: signal.Signals
) -> None:
    async def registered() -> bool:
        return await valkey.exists(STREAM) and bool(await valkey.xinfo_consumers(STREAM, GROUP))

    worker = await start_worker()
    try:
        await eventually(registered)
        async with asyncio.timeout(10):
            while worker.returncode is None:
                worker.send_signal(stop_signal)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(worker.wait(), 0.005)
    finally:
        output = await stop_worker(worker, stop_signal)

    assert worker.returncode == 0
    assert "Traceback" not in output
    assert "stock worker stopped" in output
