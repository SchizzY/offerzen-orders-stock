import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Index, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base
from app.orders.domain import StockStatus


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint("status IN ('accepted')", name="status_known"),
        CheckConstraint(
            "stock_status IN ('pending', 'reserved', 'insufficient')", name="stock_status_known"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=text("gen_random_uuid()")
    )
    order_ref: Mapped[str] = mapped_column(Text, unique=True)
    customer_id: Mapped[str] = mapped_column(Text)
    total_cents: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(Text, server_default="accepted")
    stock_status: Mapped[StockStatus] = mapped_column(Text, server_default="pending")
    request_hash: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    items: Mapped[list["OrderItem"]] = relationship(lazy="selectin", order_by="OrderItem.sku")


class OrderItem(Base):
    __tablename__ = "order_items"
    __table_args__ = (CheckConstraint("qty > 0", name="qty_positive"),)

    order_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("orders.id", ondelete="CASCADE"), primary_key=True
    )
    sku: Mapped[str] = mapped_column(Text, ForeignKey("products.sku"), primary_key=True)
    qty: Mapped[int]
    unit_price_cents: Mapped[int]


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        Index("ix_outbox_events_unsequenced", "id", postgresql_where=text("seq IS NULL")),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_type: Mapped[str] = mapped_column(Text)
    aggregate_id: Mapped[uuid.UUID]
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    seq: Mapped[int | None] = mapped_column(BigInteger, unique=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
