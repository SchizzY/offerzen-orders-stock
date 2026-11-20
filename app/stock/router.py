from fastapi import APIRouter, HTTPException, status

from app.db import SessionDep
from app.stock import controller
from app.stock.schemas import StockLevel
from app.types import Code

router = APIRouter(prefix="/stock", tags=["stock"])


@router.get("/{sku}")
async def get_stock(sku: Code, session: SessionDep) -> StockLevel:
    stock = await controller.get_stock(session, sku)
    if stock is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"product {sku!r} not found")
    return stock
