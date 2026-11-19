from datetime import datetime

from pydantic import BaseModel, Field, field_validator

from app.orders.domain import StockStatus
from app.types import Code, Quantity


class OrderItemIn(BaseModel):
    sku: Code
    qty: Quantity


class CreateOrderRequest(BaseModel):
    order_ref: Code
    customer_id: Code
    items: list[OrderItemIn] = Field(min_length=1, max_length=100)

    @field_validator("items")
    @classmethod
    def skus_are_unique(cls, items: list[OrderItemIn]) -> list[OrderItemIn]:
        skus = [item.sku for item in items]
        duplicates = sorted({sku for sku in skus if skus.count(sku) > 1})
        if duplicates:
            raise ValueError(f"duplicate sku in items: {', '.join(duplicates)}")
        return items


class OrderItemOut(BaseModel):
    sku: str
    qty: int
    unit_price_cents: int
    line_total_cents: int


class OrderDetail(BaseModel):
    order_ref: str
    customer_id: str
    status: str
    stock_status: StockStatus
    total_cents: int
    items: list[OrderItemOut]
    created_at: datetime
