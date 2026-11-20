import asyncio
import contextlib
import json
from typing import Any

from loguru import logger
from redis.asyncio import Redis
from redis.exceptions import ResponseError, WatchError
from sqlalchemy import Row, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import SessionLocal
from app.orders.models import OutboxEvent
from app.valkey import get_valkey

SEQUENCE_LOCK_KEY = int.from_bytes(b"outbox")
MAX_BACKOFF_SECONDS = 5.0

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

    # The batch goes in only if the stream still ends at `top`. Another relay that got
    # there first, or a stream lost and recreated meanwhile, ends the round instead of
    # leaving a hole the stock worker would never see.
    async with valkey.pipeline() as pipe:
        await pipe.watch(settings.orders_stream)
        if await _stream_top(pipe) != top:
            return 0
        pipe.multi()
        for event in events:
            pipe.xadd(settings.orders_stream, _entry(event), id=f"{event.seq}-0")
        try:
            await pipe.execute()
        except WatchError:
            return 0

    first, last = events[0].seq, events[-1].seq
    async with session.begin():
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.seq.between(first, last), OutboxEvent.published_at.is_(None))
            .values(published_at=func.now())
        )
    logger.info("published events: count={} seq={}..{}", len(events), first, last)
    return len(events)


async def run_relay(stop: asyncio.Event) -> None:
    backoff = settings.poll_interval_seconds
    while not stop.is_set():
        try:
            async with SessionLocal() as session:
                sequenced = await sequence_pending(session)
            async with SessionLocal() as session:
                published = await publish_pending(session, get_valkey())
        except Exception as exc:
            logger.warning("relay round failed: error={}", type(exc).__name__)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            delay = backoff
        else:
            backoff = settings.poll_interval_seconds
            full_batch = settings.relay_batch_size in (sequenced, published)
            delay = 0 if full_batch else settings.poll_interval_seconds
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), delay)


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
