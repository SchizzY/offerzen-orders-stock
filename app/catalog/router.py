from collections.abc import Sequence

from fastapi import APIRouter, HTTPException, status

from app.catalog import controller, schemas
from app.catalog.models import Product
from app.db import SessionDep
from app.types import Code

router = APIRouter(prefix="/products", tags=["catalog"])


@router.post("", response_model=schemas.Product)
async def upsert_product(data: schemas.Product, session: SessionDep) -> Product:
    return await controller.upsert_product(session, data)


@router.get("", response_model=list[schemas.Product])
async def list_products(session: SessionDep) -> Sequence[Product]:
    return await controller.list_products(session)


@router.get("/{sku}", response_model=schemas.Product)
async def get_product(sku: Code, session: SessionDep) -> Product:
    product = await controller.get_product(session, sku)
    if product is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"product {sku!r} not found")
    return product
