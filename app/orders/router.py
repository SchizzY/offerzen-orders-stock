from typing import assert_never

from fastapi import APIRouter, HTTPException, Response, status

from app.db import SessionDep
from app.orders import controller
from app.orders.controller import Conflict, Created, Replayed, UnknownSkus
from app.orders.schemas import CreateOrderRequest, OrderDetail
from app.types import Code

router = APIRouter(prefix="/orders", tags=["orders"])

REPLAY_HEADER = "Idempotent-Replay"


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_200_OK: {
            "model": OrderDetail,
            "description": "Replay: this order_ref was already accepted with the same payload",
        },
        status.HTTP_409_CONFLICT: {
            "description": "This order_ref was already accepted with a different payload",
        },
    },
)
async def create_order(
    body: CreateOrderRequest, response: Response, session: SessionDep
) -> OrderDetail:
    result = await controller.create_order(session, body)
    match result:
        case Created(order):
            response.headers[REPLAY_HEADER] = "false"
            return order
        case Replayed(order):
            response.status_code = status.HTTP_200_OK
            response.headers[REPLAY_HEADER] = "true"
            return order
        case Conflict(order_ref):
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                {
                    "message": "order_ref was already used for a different order",
                    "order_ref": order_ref,
                },
            )
        case UnknownSkus(skus):
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                [
                    {
                        "type": "unknown_sku",
                        "loc": ["body", "items", index, "sku"],
                        "msg": "Unknown sku",
                        "input": item.sku,
                    }
                    for index, item in enumerate(body.items)
                    if item.sku in skus
                ],
            )
        case _:
            assert_never(result)


@router.get("/{order_ref}")
async def get_order(order_ref: Code, session: SessionDep) -> OrderDetail:
    order = await controller.get_order(session, order_ref)
    if order is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"order {order_ref!r} not found")
    return order
