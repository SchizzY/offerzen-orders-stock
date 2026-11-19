from pydantic import BaseModel, ConfigDict, Field


class Product(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sku: str = Field(min_length=1)
    name: str = Field(min_length=1)
    price_cents: int = Field(ge=0)
    stock: int = Field(ge=0)
