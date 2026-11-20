from collections.abc import Sequence

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.catalog import schemas
from app.catalog.models import Product


async def upsert_product(session: AsyncSession, data: schemas.Product) -> Product:
    stmt = insert(Product).values(**data.model_dump())
    stmt = stmt.on_conflict_do_update(
        index_elements=[Product.sku],
        set_={
            "name": stmt.excluded.name,
            "price_cents": stmt.excluded.price_cents,
            "updated_at": func.now(),
        },
    ).returning(Product)
    product = (await session.scalars(stmt)).one()
    await session.commit()
    return product


async def list_products(session: AsyncSession) -> Sequence[Product]:
    return (await session.scalars(select(Product).order_by(Product.sku))).all()


async def get_product(session: AsyncSession, sku: str) -> Product | None:
    return await session.get(Product, sku)
