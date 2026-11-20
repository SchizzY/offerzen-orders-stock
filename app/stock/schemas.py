from pydantic import BaseModel


class StockLevel(BaseModel):
    sku: str
    on_hand: int
    pending_qty: int
