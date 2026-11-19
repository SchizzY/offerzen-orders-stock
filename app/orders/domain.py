import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class OrderLine:
    sku: str
    qty: int


@dataclass(frozen=True, slots=True)
class PricedLine:
    sku: str
    qty: int
    unit_price_cents: int

    @property
    def line_total_cents(self) -> int:
        return self.qty * self.unit_price_cents


def request_fingerprint(customer_id: str, lines: Iterable[OrderLine]) -> str:
    canonical = json.dumps(
        {
            "customer_id": customer_id,
            "items": [
                {"sku": line.sku, "qty": line.qty}
                for line in sorted(lines, key=lambda line: line.sku)
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def order_total(lines: Iterable[PricedLine]) -> int:
    return sum(line.line_total_cents for line in lines)
