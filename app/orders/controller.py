from collections.abc import Iterable
from dataclasses import dataclass

from sqlalchemy import Select, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import raiseload, selectinload

from app.catalog.models import Product
from app.orders.domain import OrderLine, PricedLine, order_total, request_fingerprint
from app.orders.models import Order, OrderItem, OutboxEvent
from app.orders.schemas import CreateOrderRequest, OrderDetail, OrderItemOut

ORDER_ACCEPTED = "order.accepted"


@dataclass(frozen=True, slots=True)
class Created:
    order: OrderDetail


@dataclass(frozen=True, slots=True)
class Replayed:
    order: OrderDetail


@dataclass(frozen=True, slots=True)
class Conflict:
    order_ref: str


@dataclass(frozen=True, slots=True)
class UnknownSkus:
    skus: list[str]


type CreateOrderResult = Created | Replayed | Conflict | UnknownSkus


async def create_order(session: AsyncSession, request: CreateOrderRequest) -> CreateOrderResult:
    lines = [OrderLine(item.sku, item.qty) for item in request.items]
    fingerprint = request_fingerprint(request.customer_id, lines)

    async with session.begin():
        prices = await _prices(session, [line.sku for line in lines])
        unknown = sorted(line.sku for line in lines if line.sku not in prices)
        if unknown:
            return UnknownSkus(unknown)
        priced = [PricedLine(line.sku, line.qty, prices[line.sku]) for line in lines]

        order = await session.scalar(
            insert(Order)
            .values(
                order_ref=request.order_ref,
                customer_id=request.customer_id,
                total_cents=order_total(priced),
                request_hash=fingerprint,
            )
            .on_conflict_do_nothing(index_elements=[Order.order_ref])
            .returning(Order)
            .options(raiseload(Order.items))
        )
        if order is None:
            existing = (await session.scalars(_by_ref(request.order_ref))).one()
            if existing.request_hash != fingerprint:
                return Conflict(request.order_ref)
            return Replayed(_stored_detail(existing))

        session.add_all(
            OrderItem(
                order_id=order.id,
                sku=line.sku,
                qty=line.qty,
                unit_price_cents=line.unit_price_cents,
            )
            for line in priced
        )
        detail = _detail(order, priced)
        session.add(
            OutboxEvent(
                event_type=ORDER_ACCEPTED,
                aggregate_id=order.id,
                payload=detail.model_dump(mode="json", exclude={"stock_status"}),
            )
        )
    return Created(detail)


async def get_order(session: AsyncSession, order_ref: str) -> OrderDetail | None:
    order = await session.scalar(_by_ref(order_ref))
    return None if order is None else _stored_detail(order)


async def _prices(session: AsyncSession, skus: list[str]) -> dict[str, int]:
    rows = await session.execute(
        select(Product.sku, Product.price_cents).where(Product.sku.in_(skus))
    )
    return {sku: price_cents for sku, price_cents in rows}


def _by_ref(order_ref: str) -> Select[Order]:
    return select(Order).where(Order.order_ref == order_ref).options(selectinload(Order.items))


def _stored_detail(order: Order) -> OrderDetail:
    return _detail(
        order,
        (PricedLine(item.sku, item.qty, item.unit_price_cents) for item in order.items),
    )


def _detail(order: Order, lines: Iterable[PricedLine]) -> OrderDetail:
    return OrderDetail(
        order_ref=order.order_ref,
        customer_id=order.customer_id,
        status=order.status,
        stock_status=order.stock_status,
        total_cents=order.total_cents,
        created_at=order.created_at,
        items=[
            OrderItemOut(
                sku=line.sku,
                qty=line.qty,
                unit_price_cents=line.unit_price_cents,
                line_total_cents=line.line_total_cents,
            )
            for line in sorted(lines, key=lambda line: line.sku)
        ],
    )
