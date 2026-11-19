import hashlib

import pytest

from app.orders.domain import OrderLine, PricedLine, order_total, request_fingerprint


def test_fingerprint_is_sha256_of_canonical_json_with_items_sorted_by_sku() -> None:
    canonical = (
        b'{"customer_id":"cust-42","items":[{"qty":1,"sku":"APL-003"},{"qty":2,"sku":"BAN-001"}]}'
    )

    fingerprint = request_fingerprint("cust-42", [OrderLine("BAN-001", 2), OrderLine("APL-003", 1)])

    assert fingerprint == hashlib.sha256(canonical).hexdigest()


@pytest.mark.parametrize(
    ("customer_id", "lines"),
    [
        ("cust-7", [OrderLine("BAN-001", 2)]),
        ("cust-42", [OrderLine("BAN-001", 3)]),
        ("cust-42", [OrderLine("APL-003", 2)]),
        ("cust-42", [OrderLine("BAN-001", 2), OrderLine("APL-003", 1)]),
    ],
    ids=["customer", "qty", "sku", "extra-item"],
)
def test_fingerprint_changes_when_the_payload_changes(
    customer_id: str, lines: list[OrderLine]
) -> None:
    original = request_fingerprint("cust-42", [OrderLine("BAN-001", 2)])

    assert request_fingerprint(customer_id, lines) != original


def test_line_total_is_qty_times_unit_price() -> None:
    assert PricedLine("BAN-001", 3, 199).line_total_cents == 597


def test_order_total_sums_every_line() -> None:
    lines = [PricedLine("BAN-001", 2, 199), PricedLine("APL-003", 1, 349)]

    assert order_total(lines) == 747
