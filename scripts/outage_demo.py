"""Stop the stock worker mid-run: orders keep landing, stock catches up after recovery.

Needs the API running with its relay. The script owns the stock worker: it starts
`python -m app.stock.worker`, SIGTERMs it for the outage, restarts it, and always
stops it on the way out, including on SIGINT, SIGTERM or SIGHUP. Exit 0 pass,
1 an expectation failed, 2 cannot run, 128 + N interrupted by signal N.

    python scripts/outage_demo.py [--base-url URL] [--timeout SECONDS] [--force]
"""

import argparse
import contextlib
import math
import os
import secrets
import shlex
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
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
STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


class Failed(Exception):
    pass


class CannotRun(Exception):
    pass


class Interrupted(BaseException):
    """A stop signal reached the script. A BaseException, so no `except Exception` eats it."""

    def __init__(self, signum: int):
        super().__init__(signal.Signals(signum).name)
        self.signum = signum


_held_signals: list[int] | None = None


def _on_stop_signal(signum: int, _frame: object) -> None:
    if _held_signals is None:
        raise Interrupted(signum)
    _held_signals.append(signum)


def install_signal_handlers() -> None:
    for signum in STOP_SIGNALS:
        signal.signal(signum, _on_stop_signal)


@contextlib.contextmanager
def signals_held() -> Iterator[None]:
    """Hold stop signals until the block ends, then raise the first one that arrived."""
    global _held_signals
    _held_signals = []
    try:
        yield
    finally:
        held, _held_signals = _held_signals, None
        if held:
            raise Interrupted(held[0])


def live_consumers(valkey: redis.Redis, max_idle_ms: float) -> list[str]:
    try:
        consumers = valkey.xinfo_consumers(settings.orders_stream, settings.stock_group)
    except redis.ResponseError:  # stream or group not created yet
        return []
    return [c["name"] for c in consumers if c["idle"] < max_idle_ms]


def stock_group(valkey: redis.Redis) -> dict:
    try:
        groups = valkey.xinfo_groups(settings.orders_stream)
    except redis.ResponseError:  # stream not created yet
        return {}
    return next((g for g in groups if g["name"] == settings.stock_group), {})


class Worker:
    def __init__(self, cmd: list[str], valkey: redis.Redis, log: TextIO):
        self.cmd = cmd
        self.valkey = valkey
        self.log = log
        self.proc: subprocess.Popen | None = None

    def start(self, timeout: float) -> None:
        """Spawn the worker and wait until its own consumer has talked to the group."""
        spawned = time.monotonic()
        # A signal raised inside Popen, after the fork, would lose the only handle on the child.
        with signals_held():
            self.proc = subprocess.Popen(self.cmd, cwd=REPO_ROOT, stdout=self.log, stderr=self.log)
        # The worker names its consumer <host>-<pid> once its signal handlers are installed.
        own = f"-{self.proc.pid}"
        while not any(
            name.endswith(own)
            for name in live_consumers(self.valkey, (time.monotonic() - spawned) * 1000)
        ):
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
    def __init__(
        self,
        client: httpx.Client,
        valkey: redis.Redis,
        worker: Worker,
        timeout: float,
        sku: str,
    ):
        self.client = client
        self.valkey = valkey
        self.worker = worker
        self.timeout = timeout
        self.sku = sku
        self.phase = "seed"
        self.refs: list[str] = []
        self.started = 0.0

    def stock(self) -> dict:
        return self.client.get(f"/stock/{self.sku}").raise_for_status().json()

    def row(self, note: str) -> dict:
        stock = self.stock()
        t = time.monotonic() - self.started
        print(f"{self.phase:<9} {t:>6.1f} {stock['on_hand']:>7} {stock['pending_qty']:>7}  {note}")
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
        """Wait for pending_qty 0, failing only after `timeout` seconds with no progress.

        Progress is this SKU's pending_qty or the stock group's read and ack position moving,
        so a worker that is busy on other events' backlog still counts as progressing.
        """
        progress, moved = None, time.monotonic()
        while (stock := self.stock())["pending_qty"]:
            group = stock_group(self.valkey)
            now = (stock["pending_qty"], group.get("last-delivered-id"), group.get("pending"))
            if now != progress:
                progress, moved = now, time.monotonic()
            elif time.monotonic() - moved > self.timeout:
                raise Failed(self.stalled(stock["pending_qty"], group))
            time.sleep(0.2)

    def stalled(self, pending_qty: int, group: dict) -> str:
        message = f"pending_qty still {pending_qty} after {self.timeout:g}s without progress"
        backlog = group.get("pending", 0) + (group.get("lag") or 0)
        if backlog:
            message += (
                f"; {backlog} events still wait in group {settings.stock_group!r}, so the "
                "worker was likely draining a backlog: re-run with a larger --timeout"
            )
        return message

    def run(self) -> None:
        self.started = time.monotonic()
        print(f"{'phase':<9} {'t+s':>6} {'on_hand':>7} {'pending':>7}  note")
        product = {"sku": self.sku, "name": "Outage demo", "price_cents": 100, "stock": SEED_STOCK}
        self.client.post("/products", json=product).raise_for_status()
        self.row(f"seeded {self.sku}")

        self.phase = "healthy"
        self.worker.start(self.timeout)
        self.row("worker started")
        refs = self.place("H", HEALTHY_ORDERS, HEALTHY_QTY)
        self.row(f"placed {len(refs)} x qty {HEALTHY_QTY}")
        self.drain()
        healthy_on_hand = SEED_STOCK - HEALTHY_ORDERS * HEALTHY_QTY
        self.expect_stock(self.row("drained"), healthy_on_hand, 0)
        self.row(self.expect_orders(refs, "reserved"))

        self.phase = "outage"
        code = self.worker.stop()
        if code != 0:
            raise Failed(f"worker exited with {code} on SIGTERM")
        self.row("worker stopped (SIGTERM, exit 0)")
        refs = self.place("O", OUTAGE_ORDERS, OUTAGE_QTY)
        backlog = OUTAGE_ORDERS * OUTAGE_QTY
        stock = self.row(f"placed {len(refs)} x qty {OUTAGE_QTY}, all 201")
        self.expect_stock(stock, healthy_on_hand, backlog)
        self.row(self.expect_orders(refs, "pending"))
        time.sleep(HOLD_SECONDS)
        self.expect_stock(self.row(f"after a {HOLD_SECONDS}s hold"), healthy_on_hand, backlog)

        self.phase = "recovery"
        recovering = time.monotonic()
        self.worker.start(self.timeout)
        self.row("worker restarted")
        self.drain()
        caught_up = time.monotonic() - recovering
        stock = self.row(f"caught up {caught_up:.1f}s after restart")
        self.expect_stock(stock, healthy_on_hand - backlog, 0)
        self.row(self.expect_orders(self.refs, "reserved"))


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
            print(
                f"waiting for recently active consumers to go idle: {', '.join(others)}",
                file=sys.stderr,
            )
        while others and not force and time.monotonic() < deadline:
            time.sleep(0.5)
            others = live_consumers(valkey, LIVE_IDLE_MS)
    except redis.RedisError as exc:
        raise CannotRun(f"Valkey unreachable: {exc!r}") from exc
    if others and not force:
        raise CannotRun(
            f"a stock worker was active in the last {LIVE_IDLE_MS / 1000:g} s "
            f"({', '.join(others)}); stop it and re-run, or pass --force"
        )


def positive_seconds(value: str) -> float:
    seconds = float(value)
    if not (math.isfinite(seconds) and seconds > 0):
        raise argparse.ArgumentTypeError(f"must be a positive, finite number, got {value!r}")
    return seconds


def run(args: argparse.Namespace) -> int:
    worker_cmd = shlex.split(args.worker_cmd or f"{sys.executable} -m app.stock.worker")
    sku = f"OUT-{secrets.token_hex(3).upper()}"
    valkey = redis.Redis.from_url(settings.valkey_url, decode_responses=True)
    with (
        httpx.Client(base_url=args.base_url, timeout=10) as client,
        tempfile.NamedTemporaryFile(
            "w", prefix=f"outage-demo-{sku}-", suffix=".log", delete=False
        ) as log,
    ):
        worker = Worker(worker_cmd, valkey, log)
        demo = Demo(client, valkey, worker, args.timeout, sku)
        try:
            preflight(client, valkey, args.force)
            print(f"worker log: {log.name}\n")
            demo.run()
        except CannotRun as exc:
            print(f"\ncannot run: {exc}", file=sys.stderr)
            return 2
        except httpx.TransportError as exc:
            print(
                f"\ncannot run: API unreachable at {client.base_url} during {demo.phase}: {exc!r}",
                file=sys.stderr,
            )
            return 2
        except redis.ConnectionError as exc:
            print(f"\ncannot run: Valkey unreachable during {demo.phase}: {exc!r}", file=sys.stderr)
            return 2
        except (Failed, httpx.HTTPStatusError) as exc:
            print(f"\nFAIL: {exc}")
            return 1
        finally:
            with signals_held():
                worker.stop()
    print("\nPASS")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-url", default=os.environ.get("BASE_URL", "http://localhost:8000"))
    parser.add_argument(
        "--timeout",
        type=positive_seconds,
        default=30,
        help="seconds to wait for the worker to register, and for catch-up without progress",
    )
    parser.add_argument("--force", action="store_true", help="run even if a worker is active")
    parser.add_argument("--worker-cmd", help=argparse.SUPPRESS)
    args = parser.parse_args()

    install_signal_handlers()
    try:
        return run(args)
    except Interrupted as exc:
        print(f"\ninterrupted by {exc}", file=sys.stderr)
        return 128 + exc.signum


if __name__ == "__main__":
    sys.exit(main())
