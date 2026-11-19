"""Stop the stock worker mid-run: orders keep landing, stock catches up after recovery.

Needs the API running with its relay. The script owns the stock worker: it starts
`python -m app.stock.worker`, SIGTERMs it for the outage, restarts it, and always
stops it on the way out. Exit 0 pass, 1 an expectation failed, 2 cannot run.

    python scripts/outage_demo.py [--base-url URL] [--timeout SECONDS] [--force]
"""

import argparse
import os
import secrets
import shlex
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import TextIO

import httpx
import redis

from app.config import settings

REPO_ROOT = Path(__file__).resolve().parent.parent
SEED_STOCK = 100
HEALTHY_QTY, HEALTHY_ORDERS = 2, 3
OUTAGE_QTY, OUTAGE_ORDERS = 3, 5
HOLD_SECONDS = 3
LIVE_IDLE_MS = 10_000
WORKER_LOG = Path(tempfile.gettempdir()) / "outage_demo_worker.log"


class Failed(Exception):
    pass


class CannotRun(Exception):
    pass


def live_consumers(valkey: redis.Redis, max_idle_ms: float) -> list[str]:
    try:
        consumers = valkey.xinfo_consumers(settings.orders_stream, settings.stock_group)
    except redis.ResponseError:  # stream or group not created yet
        return []
    return [c["name"] for c in consumers if c["idle"] < max_idle_ms]


class Worker:
    def __init__(self, cmd: list[str], valkey: redis.Redis, log: TextIO):
        self.cmd = cmd
        self.valkey = valkey
        self.log = log
        self.proc: subprocess.Popen | None = None

    def start(self, timeout: float) -> None:
        """Spawn the worker and wait until it has talked to the consumer group since spawning."""
        spawned = time.monotonic()
        self.proc = subprocess.Popen(self.cmd, cwd=REPO_ROOT, stdout=self.log, stderr=self.log)
        while not live_consumers(self.valkey, (time.monotonic() - spawned) * 1000):
            if self.proc.poll() is not None:
                raise Failed(f"worker exited with {self.proc.returncode} before registering")
            if time.monotonic() - spawned > timeout:
                raise Failed(f"worker did not register in group {settings.stock_group!r}")
            time.sleep(0.1)

    def stop(self) -> int | None:
        if self.proc is None or self.proc.poll() is not None:
            return None if self.proc is None else self.proc.returncode
        self.proc.send_signal(signal.SIGTERM)
        try:
            return self.proc.wait(10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            return self.proc.wait()


class Demo:
    def __init__(self, client: httpx.Client, worker: Worker, timeout: float):
        self.client = client
        self.worker = worker
        self.timeout = timeout
        self.sku = f"OUT-{secrets.token_hex(3).upper()}"
        self.refs: list[str] = []
        self.started = time.monotonic()

    def stock(self) -> dict:
        return self.client.get(f"/stock/{self.sku}").raise_for_status().json()

    def row(self, phase: str, note: str) -> dict:
        stock = self.stock()
        t = time.monotonic() - self.started
        print(f"{phase:<9} {t:>6.1f} {stock['on_hand']:>7} {stock['pending_qty']:>7}  {note}")
        return stock

    def expect_stock(self, stock: dict, on_hand: int, pending_qty: int) -> None:
        got = (stock["on_hand"], stock["pending_qty"])
        if got != (on_hand, pending_qty):
            raise Failed(f"expected on_hand/pending {on_hand}/{pending_qty}, got {got[0]}/{got[1]}")

    def expect_orders(self, refs: list[str], stock_status: str) -> str:
        statuses = [
            self.client.get(f"/orders/{ref}").raise_for_status().json()["stock_status"]
            for ref in refs
        ]
        if any(s != stock_status for s in statuses):
            raise Failed(f"expected {stock_status} for {refs}, got {statuses}")
        return f"{len(refs)} orders {stock_status}"

    def place(self, tag: str, count: int, qty: int) -> list[str]:
        refs = [f"{self.sku}-{tag}{n}" for n in range(1, count + 1)]
        for ref in refs:
            body = {
                "order_ref": ref,
                "customer_id": "outage-demo",
                "items": [{"sku": self.sku, "qty": qty}],
            }
            response = self.client.post("/orders", json=body)
            if response.status_code != 201:
                raise Failed(f"POST /orders {ref} returned {response.status_code}")
        self.refs += refs
        return refs

    def drain(self) -> None:
        deadline = time.monotonic() + self.timeout
        while (stock := self.stock())["pending_qty"]:
            if time.monotonic() > deadline:
                raise Failed(f"pending_qty still {stock['pending_qty']} after {self.timeout:g}s")
            time.sleep(0.2)

    def run(self) -> None:
        print(f"{'phase':<9} {'t+s':>6} {'on_hand':>7} {'pending':>7}  note")
        product = {"sku": self.sku, "name": "Outage demo", "price_cents": 100, "stock": SEED_STOCK}
        self.client.post("/products", json=product).raise_for_status()
        self.row("seed", f"seeded {self.sku}")

        self.worker.start(self.timeout)
        self.row("healthy", "worker started")
        refs = self.place("H", HEALTHY_ORDERS, HEALTHY_QTY)
        self.row("healthy", f"placed {len(refs)} x qty {HEALTHY_QTY}")
        self.drain()
        healthy_on_hand = SEED_STOCK - HEALTHY_ORDERS * HEALTHY_QTY
        self.expect_stock(self.row("healthy", "drained"), healthy_on_hand, 0)
        self.row("healthy", self.expect_orders(refs, "reserved"))

        code = self.worker.stop()
        if code != 0:
            raise Failed(f"worker exited with {code} on SIGTERM")
        self.row("outage", "worker stopped (SIGTERM, exit 0)")
        refs = self.place("O", OUTAGE_ORDERS, OUTAGE_QTY)
        backlog = OUTAGE_ORDERS * OUTAGE_QTY
        stock = self.row("outage", f"placed {len(refs)} x qty {OUTAGE_QTY}, all 201")
        self.expect_stock(stock, healthy_on_hand, backlog)
        self.row("outage", self.expect_orders(refs, "pending"))
        time.sleep(HOLD_SECONDS)
        self.expect_stock(
            self.row("outage", f"after a {HOLD_SECONDS}s hold"), healthy_on_hand, backlog
        )

        recovering = time.monotonic()
        self.worker.start(self.timeout)
        self.row("recovery", "worker restarted")
        self.drain()
        caught_up = time.monotonic() - recovering
        stock = self.row("recovery", f"caught up {caught_up:.1f}s after restart")
        self.expect_stock(stock, healthy_on_hand - backlog, 0)
        self.row("recovery", self.expect_orders(self.refs, "reserved"))


def preflight(client: httpx.Client, valkey: redis.Redis, force: bool) -> None:
    try:
        health = client.get("/health")
    except httpx.HTTPError as exc:
        raise CannotRun(f"API unreachable at {client.base_url}: {exc!r}") from exc
    if health.status_code != 200:
        raise CannotRun(f"GET /health returned {health.status_code}")
    # Idle alone cannot tell a live worker from one stopped seconds ago, so give recent
    # consumers time to age out: a live worker keeps re-reading and stays under the threshold.
    deadline = time.monotonic() + LIVE_IDLE_MS / 1000 + 1
    try:
        others = live_consumers(valkey, LIVE_IDLE_MS)
        if others and not force:
            print(f"waiting for recently active consumers to go idle: {', '.join(others)}")
        while others and not force and time.monotonic() < deadline:
            time.sleep(0.5)
            others = live_consumers(valkey, LIVE_IDLE_MS)
    except redis.RedisError as exc:
        raise CannotRun(f"Valkey unreachable: {exc!r}") from exc
    if others and not force:
        raise CannotRun(
            f"a stock worker is already running ({', '.join(others)}); it would hide the "
            "outage. Stop it, or pass --force."
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-url", default=os.environ.get("BASE_URL", "http://localhost:8000"))
    parser.add_argument("--timeout", type=float, default=30, help="seconds to wait for catch-up")
    parser.add_argument("--force", action="store_true", help="run even if a worker is active")
    parser.add_argument("--worker-cmd", help=argparse.SUPPRESS)
    args = parser.parse_args()

    worker_cmd = shlex.split(args.worker_cmd or f"{sys.executable} -m app.stock.worker")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    valkey = redis.Redis.from_url(settings.valkey_url, decode_responses=True)
    with (
        httpx.Client(base_url=args.base_url, timeout=10) as client,
        WORKER_LOG.open("a") as log,
    ):
        worker = Worker(worker_cmd, valkey, log)
        try:
            preflight(client, valkey, args.force)
            print(f"worker log: {WORKER_LOG}\n")
            Demo(client, worker, args.timeout).run()
        except (CannotRun, httpx.TransportError, redis.ConnectionError) as exc:
            print(f"\ncannot run: {exc}", file=sys.stderr)
            return 2
        except (Failed, httpx.HTTPStatusError) as exc:
            print(f"\nFAIL: {exc}")
            return 1
        except KeyboardInterrupt:
            print("\ninterrupted")
            return 130
        finally:
            worker.stop()
    print("\nPASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
