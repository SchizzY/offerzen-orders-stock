import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable
from itertools import chain
from typing import Any

from loguru import logger
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy import Row, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import SessionLocal
from app.orders.models import OutboxEvent
from app.valkey import get_valkey

SEQUENCE_LOCK_KEY = int.from_bytes(b"outbox")
MAX_BACKOFF_SECONDS = 5.0

# Appends the batch only if the stream still ends at the top the batch was read after
# (ARGV[1]); otherwise another relay got there first, or the stream was lost and
# recreated, and a write would leave a hole the stock worker never sees. One atomic
# script rather than WATCH, which every consumer-group read and ack invalidates.
# ARGV[2] is the width of each entry that follows: its id, then its field/value pairs.
_APPEND_AT_TOP = """
local top = '0'
if redis.call('EXISTS', KEYS[1]) == 1 then
    local info = redis.call('XINFO', 'STREAM', KEYS[1])
    for i = 1, #info, 2 do
        if info[i] == 'last-generated-id' then
            top = string.match(info[i + 1], '^%d+')
        end
    end
end
if top ~= ARGV[1] then
    return false
end
local width = tonumber(ARGV[2])
local added = 0
for i = 3, #ARGV, width do
    redis.call('XADD', KEYS[1], unpack(ARGV, i, i + width - 1))
    added = added + 1
end
return added
"""

# Runs under the sequence lock in its own statement, so it sees every earlier
# sequencer's commit and only committed events: seq follows commit order with no gaps.
_ASSIGN_SEQ = text("""
    WITH numbered AS (
        SELECT id,
               (SELECT coalesce(max(seq), 0) FROM outbox_events)
                   + row_number() OVER (ORDER BY id) AS seq
        FROM outbox_events
        WHERE seq IS NULL
        ORDER BY id
        LIMIT :batch
    )
    UPDATE outbox_events
    SET seq = numbered.seq
    FROM numbered
    WHERE outbox_events.id = numbered.id
    RETURNING outbox_events.seq
""")


async def sequence_pending(session: AsyncSession) -> int:
    async with session.begin():
        await session.execute(select(func.pg_advisory_xact_lock(SEQUENCE_LOCK_KEY)))
        seqs = (await session.scalars(_ASSIGN_SEQ, {"batch": settings.relay_batch_size})).all()
    if seqs:
        logger.info("sequenced events: count={} seq={}..{}", len(seqs), min(seqs), max(seqs))
    return len(seqs)


async def publish_pending(session: AsyncSession, valkey: Redis) -> int:
    while (published := await _publish_batch(session, valkey)) is None:
        logger.debug("stream moved before the batch went in, retrying")
    return published


async def run_relay(stop: asyncio.Event) -> None:
    # Separate loops, so a Valkey outage or stall never holds up sequencing, which feeds
    # GET /events. Newly sequenced events, and stopping, wake an idle publisher.
    publish_now = asyncio.Event()

    async def sequence_round() -> int:
        async with SessionLocal() as session:
            sequenced = await sequence_pending(session)
        if sequenced:
            publish_now.set()
        return sequenced

    async def publish_round() -> int:
        publish_now.clear()
        async with SessionLocal() as session:
            return await publish_pending(session, get_valkey())

    async with asyncio.TaskGroup() as loops:
        loops.create_task(_repeat("sequence", sequence_round, stop, wake=stop))
        loops.create_task(_repeat("publish", publish_round, stop, wake=publish_now))
        await stop.wait()
        publish_now.set()


async def _repeat(
    name: str, step: Callable[[], Awaitable[int]], stop: asyncio.Event, wake: asyncio.Event
) -> None:
    backoff = settings.poll_interval_seconds
    while not stop.is_set():
        try:
            handled = await step()
        except Exception as exc:
            logger.warning("relay {} failed: error={}", name, type(exc).__name__)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            await _wait(stop, backoff)
        else:
            backoff = settings.poll_interval_seconds
            if handled < settings.relay_batch_size:
                await _wait(wake, settings.poll_interval_seconds)


async def _wait(event: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(event.wait(), seconds)


async def _publish_batch(session: AsyncSession, valkey: Redis) -> int | None:
    """Publish the next batch after the stream's top, or None if the stream moved first."""
    top = await _stream_top(valkey)
    async with session.begin():
        events = (
            await session.execute(
                select(
                    OutboxEvent.seq,
                    OutboxEvent.event_type,
                    OutboxEvent.aggregate_id,
                    OutboxEvent.payload,
                )
                .where(OutboxEvent.seq > top)
                .order_by(OutboxEvent.seq)
                .limit(settings.relay_batch_size)
            )
        ).all()
    if not events:
        return 0

    entries = [[f"{event.seq}-0", *chain.from_iterable(_entry(event).items())] for event in events]
    added = await valkey.register_script(_APPEND_AT_TOP)(
        keys=[settings.orders_stream],
        args=[top, len(entries[0]), *chain.from_iterable(entries)],
    )
    if added is None:
        return None

    first, last = events[0].seq, events[-1].seq
    async with session.begin():
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.seq.between(first, last), OutboxEvent.published_at.is_(None))
            .values(published_at=func.now())
        )
    logger.info("published events: count={} seq={}..{}", added, first, last)
    return added


async def _stream_top(valkey: Redis) -> int:
    try:
        info = await valkey.xinfo_stream(settings.orders_stream)
    except ResponseError as exc:
        if str(exc) != "no such key":
            raise
        return 0
    return int(info["last-generated-id"].split(b"-")[0])


def _entry(event: Row[Any]) -> dict[str, str | int]:
    return {
        "seq": event.seq,
        "event_type": event.event_type,
        "order_id": str(event.aggregate_id),
        "order_ref": event.payload["order_ref"],
        "payload": json.dumps(event.payload),
    }
