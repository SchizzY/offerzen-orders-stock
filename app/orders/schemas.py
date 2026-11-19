from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints, field_validator

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class OrderItemIn(BaseModel):
    sku: NonEmptyStr
    qty: int = Field(ge=1)


class CreateOrderRequest(BaseModel):
    order_ref: NonEmptyStr
    customer_id: NonEmptyStr
    items: list[OrderItemIn] = Field(min_length=1)

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
    total_cents: int
    items: list[OrderItemOut]
    created_at: datetime
