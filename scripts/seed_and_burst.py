"""Seed products, then fire a concurrent burst of orders with duplicate order_refs.

Exits non-zero if any order_ref was created more than once or a response breaks
the idempotency contract, so it doubles as a smoke check against a running API.

    python scripts/seed_and_burst.py [--base-url URL] [--fixed-refs]
"""

import argparse
import asyncio
import os
import secrets
import sys
import time
from collections import Counter
from dataclasses import dataclass

import httpx

PRODUCTS = [
    {"sku": "BAN-001", "name": "Bananas 1kg", "price_cents": 199, "stock": 50},
    {"sku": "MLK-002", "name": "Milk 2L", "price_cents": 349, "stock": 40},
    {"sku": "APL-003", "name": "Apples 1kg", "price_cents": 279, "stock": 30},
    {"sku": "BRD-004", "name": "Bread loaf", "price_cents": 189, "stock": 25},
]


@dataclass(frozen=True)
class Submission:
    order_ref: str
    attempt: int
    body: dict


@dataclass(frozen=True)
class Outcome:
    submission: Submission
    status: int
    replay: str | None


def order_body(order_ref: str, customer_id: str, items: dict[str, int]) -> dict:
    return {
        "order_ref": order_ref,
        "customer_id": customer_id,
        "items": [{"sku": sku, "qty": qty} for sku, qty in items.items()],
    }


def build_burst(prefix: str) -> list[Submission]:
    """Six distinct orders: two sent several times, one ref reused with a different body."""
    plan = [
        ("ORD-001", "cust-1", {"BAN-001": 2, "MLK-002": 1}, 4),
        ("ORD-002", "cust-2", {"APL-003": 3}, 3),
        ("ORD-003", "cust-3", {"BRD-004": 1, "BAN-001": 1}, 1),
        ("ORD-004", "cust-1", {"MLK-002": 2}, 1),
        ("ORD-005", "cust-4", {"APL-003": 1, "BRD-004": 2}, 1),
        ("ORD-006", "cust-5", {"BAN-001": 5}, 1),
    ]
    burst = []
    for suffix, customer_id, items, copies in plan:
        ref = f"{prefix}-{suffix}"
        burst += [
            Submission(ref, n, order_body(ref, customer_id, items)) for n in range(1, copies + 1)
        ]
    conflict_ref = f"{prefix}-ORD-001"
    burst.append(Submission(conflict_ref, 5, order_body(conflict_ref, "cust-1", {"BAN-001": 9})))
    return burst


def fingerprint(order: dict) -> tuple:
    return order["customer_id"], sorted((item["sku"], item["qty"]) for item in order["items"])


async def submit(client: httpx.AsyncClient, submission: Submission) -> Outcome:
    response = await client.post("/orders", json=submission.body)
    return Outcome(submission, response.status_code, response.headers.get("Idempotent-Replay"))


async def run(base_url: str, prefix: str, fixed_refs: bool) -> bool:
    async with httpx.AsyncClient(base_url=base_url, timeout=10) as client:
        for product in PRODUCTS:
            (await client.post("/products", json=product)).raise_for_status()
        print(f"seeded {len(PRODUCTS)} products at {base_url}\n")

        burst = build_burst(prefix)
        outcomes = await asyncio.gather(*(submit(client, s) for s in burst))

        print(f"{'order_ref':<34} {'try':>3} {'status':>6}  replay")
        for o in outcomes:
            print(
                f"{o.submission.order_ref:<34} {o.submission.attempt:>3} "
                f"{o.status:>6}  {o.replay or '-'}"
            )

        refs = list(dict.fromkeys(s.order_ref for s in burst))
        stored = {}
        for ref in refs:
            response = await client.get(f"/orders/{ref}")
            stored[ref] = response.json() if response.status_code == 200 else response.status_code

    ok = True
    print(
        f"\n{'order_ref':<34} {'sent':>4} {'201':>3} {'200':>3} {'409':>3} "
        f"{'total_cents':>11}  verdict"
    )
    for ref in refs:
        mine = [o for o in outcomes if o.submission.order_ref == ref]
        counts = Counter(o.status for o in mine)
        problems = check(mine, stored[ref], fixed_refs)
        ok = ok and not problems
        total = stored[ref]["total_cents"] if isinstance(stored[ref], dict) else "-"
        verdict = "ok" if not problems else "FAIL: " + "; ".join(problems)
        print(
            f"{ref:<34} {len(mine):>4} {counts[201]:>3} {counts[200]:>3} {counts[409]:>3} "
            f"{total:>11}  {verdict}"
        )
    return ok


def check(outcomes: list[Outcome], stored: dict | int, fixed_refs: bool) -> list[str]:
    """The stored order picks the winning body; it must be created once and replayed, others 409."""
    if isinstance(stored, int):
        return [f"GET returned {stored}"]
    problems = []
    created = sum(o.status == 201 for o in outcomes)
    if created > 1:
        problems.append(f"{created} creates")
    if created == 0 and not fixed_refs:
        problems.append("never created")
    for o in outcomes:
        same_body = fingerprint(o.submission.body) == fingerprint(stored)
        expected = {(201, "false"), (200, "true")} if same_body else {(409, None)}
        got = (o.status, o.replay) if o.status != 409 else (409, None)
        if got not in expected:
            problems.append(f"try {o.submission.attempt} got {o.status}/{o.replay}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-url", default=os.environ.get("BASE_URL", "http://localhost:8000"))
    parser.add_argument(
        "--fixed-refs",
        action="store_true",
        help="reuse stable order_refs to show replays across runs",
    )
    args = parser.parse_args()
    if args.fixed_refs:
        prefix = "DEMO"
    else:
        prefix = f"{time.strftime('RUN%Y%m%d-%H%M%S')}-{secrets.token_hex(3)}"
    try:
        ok = asyncio.run(run(args.base_url, prefix, args.fixed_refs))
    except httpx.HTTPError as exc:
        print(f"request failed: {exc!r}", file=sys.stderr)
        return 2
    print("\nPASS" if ok else "\nFAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
