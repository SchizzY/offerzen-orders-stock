import asyncio
import contextlib
import os
import signal
import socket
import uuid
from dataclasses import dataclass

from loguru import logger
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.config import settings
from app.db import SessionLocal, engine
from app.logging import setup_logging
from app.orders.controller import ORDER_ACCEPTED
from app.stock import controller
from app.valkey import valkey as app_valkey

BATCH_SIZE = 50
CLAIM_IDLE_MS = 30_000
BACKOFF_INITIAL_SECONDS = 0.5
BACKOFF_MAX_SECONDS = 5.0
LEAVE_TIMEOUT_SECONDS = 2.0
STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM)

type Entry = tuple[bytes, dict[bytes, bytes]]


@dataclass(frozen=True, slots=True)
class OrderAccepted:
    seq: int
    order_id: uuid.UUID
    order_ref: str


def parse_entry(fields: dict[bytes, bytes]) -> OrderAccepted:
    if fields[b"event_type"].decode() != ORDER_ACCEPTED:
        raise ValueError("not an order.accepted event")
    return OrderAccepted(
        seq=int(fields[b"seq"]),
        order_id=uuid.UUID(fields[b"order_id"].decode()),
        order_ref=fields[b"order_ref"].decode(),
    )


async def ensure_group(valkey: Redis) -> bool:
    """Create the group, reading from the stream's start. Return whether it was missing."""
    try:
        await valkey.xgroup_create(
            settings.orders_stream, settings.stock_group, id="0", mkstream=True
        )
    except ResponseError as exc:
        if not str(exc).startswith("BUSYGROUP"):
            raise
        return False
    return True


async def leave_group(valkey: Redis, consumer: str) -> None:
    """Drop this consumer from the group, unless entries still wait on it to be claimed."""
    pending = await valkey.xpending_range(
        settings.orders_stream,
        settings.stock_group,
        min="-",
        max="+",
        count=1,
        consumername=consumer,
    )
    if not pending:
        await valkey.xgroup_delconsumer(settings.orders_stream, settings.stock_group, consumer)


async def consume_once(
    valkey: Redis, consumer: str, block_ms: int = 1000, claim_idle_ms: int = CLAIM_IDLE_MS
) -> int:
    try:
        entries = await _read(valkey, consumer, block_ms, claim_idle_ms)
    except ResponseError as exc:
        if not str(exc).startswith("NOGROUP"):
            raise
        logger.warning(
            "stock group missing, creating it: stream={} group={}",
            settings.orders_stream,
            settings.stock_group,
        )
        await ensure_group(valkey)
        entries = await _read(valkey, consumer, block_ms, claim_idle_ms)
    for entry_id, fields in entries:
        await _apply(entry_id, fields)
        await valkey.xack(settings.orders_stream, settings.stock_group, entry_id)
    return len(entries)


async def _read(valkey: Redis, consumer: str, block_ms: int, claim_idle_ms: int) -> list[Entry]:
    # Consumer names die with their process, so adopt what a dead worker left unacked.
    await valkey.xautoclaim(
        settings.orders_stream,
        settings.stock_group,
        consumer,
        claim_idle_ms,
        count=BATCH_SIZE,
        justid=True,
    )
    own = await _read_group(valkey, consumer, "0", block=None)
    new = await _read_group(valkey, consumer, ">", block=None if own else block_ms)
    return own + new


async def _read_group(
    valkey: Redis, consumer: str, entry_id: str, block: int | None
) -> list[Entry]:
    response = await valkey.xreadgroup(
        settings.stock_group,
        consumer,
        {settings.orders_stream: entry_id},
        count=BATCH_SIZE,
        block=block,
    )
    return [entry for _, entries in response for entry in entries]


async def _apply(entry_id: bytes, fields: dict[bytes, bytes]) -> None:
    try:
        event = parse_entry(fields)
    except (KeyError, ValueError) as exc:
        logger.warning(
            "stock skipped malformed entry: entry_id={} error={}",
            entry_id.decode(),
            type(exc).__name__,
        )
        return
    async with SessionLocal() as session:
        result = await controller.apply_order_accepted(session, event.order_id)
    logger.info(
        "stock event applied: seq={} order_ref={} result={}", event.seq, event.order_ref, result
    )


async def run_worker(stop: asyncio.Event, consumer: str) -> None:
    logger.info(
        "stock worker started: consumer={} stream={} group={}",
        consumer,
        settings.orders_stream,
        settings.stock_group,
    )
    delay = BACKOFF_INITIAL_SECONDS
    group_checked = False
    while not stop.is_set():
        try:
            if not group_checked:
                if await ensure_group(app_valkey):
                    logger.info(
                        "stock group created: stream={} group={}",
                        settings.orders_stream,
                        settings.stock_group,
                    )
                group_checked = True
            await consume_once(app_valkey, consumer)
        except Exception as exc:
            logger.warning(
                "stock worker error: error={} retry_in_seconds={}", type(exc).__name__, delay
            )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), delay)
            delay = min(delay * 2, BACKOFF_MAX_SECONDS)
        else:
            delay = BACKOFF_INITIAL_SECONDS
    try:
        async with asyncio.timeout(LEAVE_TIMEOUT_SECONDS):
            await leave_group(app_valkey, consumer)
    except Exception as exc:
        logger.warning("stock worker kept its consumer: error={}", type(exc).__name__)
    logger.info("stock worker stopped: consumer={}", consumer)


async def _serve() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in STOP_SIGNALS:
        loop.add_signal_handler(signum, stop.set)
    try:
        await run_worker(stop, f"{socket.gethostname()}-{os.getpid()}")
    finally:
        await app_valkey.aclose()
        await engine.dispose()
        # Closing the loop, then the interpreter, resets these to defaults that kill the process
        # or raise KeyboardInterrupt. Nothing is left to stop, so hold them until exit.
        signal.pthread_sigmask(signal.SIG_BLOCK, STOP_SIGNALS)


def main() -> None:
    setup_logging()
    asyncio.run(_serve())
