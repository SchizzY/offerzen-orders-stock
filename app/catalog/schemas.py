from pydantic import BaseModel, ConfigDict

from app.types import Code, Label, NonNegativeInt32


class Product(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sku: Code
    name: Label
    price_cents: NonNegativeInt32
    stock: NonNegativeInt32
