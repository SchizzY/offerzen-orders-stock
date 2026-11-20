import uuid
from typing import Literal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.catalog.models import Product
from app.orders.models import Order, OrderItem
from app.stock.schemas import StockLevel

type ApplyResult = Literal["reserved", "insufficient", "duplicate", "unknown"]

# FOR NO KEY UPDATE, the lock an UPDATE takes anyway. FOR UPDATE would also conflict with the
# key-share locks that placing an order holds on its products, and deadlock against it.
ROW_LOCK = {"key_share": True}


async def apply_order_accepted(session: AsyncSession, order_id: uuid.UUID) -> ApplyResult:
    async with session.begin():
        order = await session.get(Order, order_id, with_for_update=ROW_LOCK)
        if order is None:
            return "unknown"
        if order.stock_status != "pending":
            return "duplicate"
        products = {
            product.sku: product
            for product in await session.scalars(
                select(Product)
                .where(Product.sku.in_([item.sku for item in order.items]))
                .order_by(Product.sku)
                .with_for_update(**ROW_LOCK)
            )
        }
        outcome: Literal["reserved", "insufficient"] = (
            "reserved"
            if all(products[item.sku].stock >= item.qty for item in order.items)
            else "insufficient"
        )
        if outcome == "reserved":
            for item in order.items:
                products[item.sku].stock -= item.qty
        order.stock_status = outcome
    return outcome


async def get_stock(session: AsyncSession, sku: str) -> StockLevel | None:
    pending_qty = (
        select(func.coalesce(func.sum(OrderItem.qty), 0))
        .join(Order, Order.id == OrderItem.order_id)
        .where(OrderItem.sku == Product.sku, Order.stock_status == "pending")
        .scalar_subquery()
    )
    row = (
        await session.execute(
            select(Product.sku, Product.stock, pending_qty).where(Product.sku == sku)
        )
    ).one_or_none()
    if row is None:
        return None
    found_sku, stock, pending = row
    return StockLevel(sku=found_sku, on_hand=stock, pending_qty=pending)
