"""Follow the "order accepted" feed and print one line per event.

The cursor (the last seq handled) is saved to a file after each printed batch, so a
restart resumes where it left off. Delivery is at-least-once: a consumer killed between
printing and saving prints that batch again, and a real consumer dedupes by seq.

    python scripts/feed_consumer.py [--base-url URL] [--cursor-file PATH] [--from-start]
                                    [--once] [--interval SECONDS]
"""

import argparse
import asyncio
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, TextIO

import httpx

MAX_BACKOFF_SECONDS = 30.0


class CursorError(Exception):
    pass


def load_cursor(path: Path) -> int:
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return 0
    digits = raw.strip()
    if not (digits.isascii() and digits.isdigit()):
        raise CursorError(f"cursor file {path} does not hold a non-negative integer: {raw!r}")
    return int(digits)


def save_cursor(path: Path, seq: int) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(f"{seq}\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def format_event(event: dict[str, Any]) -> str:
    data = event["data"]
    items = ",".join(f"{item['sku']}x{item['qty']}" for item in data["items"])
    return (
        f"seq={event['seq']} order={data['order_ref']} "
        f"total_cents={data['total_cents']} items={items}"
    )


async def poll_once(
    client: httpx.AsyncClient, after: int, cursor_file: Path, out: TextIO = sys.stdout
) -> int:
    """Fetch one page after `after`, print it, then save and return the new cursor."""
    response = await client.get("/events", params={"after": after})
    response.raise_for_status()
    page = response.json()
    for event in page["events"]:
        print(format_event(event), file=out, flush=True)
    next_after: int = page["next_after"]
    if next_after != after:
        save_cursor(cursor_file, next_after)
    return next_after


async def follow(client: httpx.AsyncClient, after: int, cursor_file: Path, interval: float) -> None:
    """Poll forever: drain without pausing, idle `interval` when caught up, back off on errors."""
    delay = interval
    while True:
        try:
            next_after = await poll_once(client, after, cursor_file)
        except httpx.HTTPError as exc:
            print(
                f"feed unavailable: {type(exc).__name__}; retrying in {delay:g}s",
                file=sys.stderr,
                flush=True,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_BACKOFF_SECONDS)
            continue
        delay = interval
        if next_after == after:
            await asyncio.sleep(interval)
        after = next_after


async def run(args: argparse.Namespace) -> None:
    after = 0 if args.from_start else load_cursor(args.cursor_file)
    async with httpx.AsyncClient(base_url=args.base_url, timeout=10) as client:
        if args.once:
            await poll_once(client, after, args.cursor_file)
        else:
            await follow(client, after, args.cursor_file, args.interval)


def positive_float(raw: str) -> float:
    value = float(raw)
    if not value > 0 or value == float("inf"):
        raise argparse.ArgumentTypeError(f"must be a positive number, got {raw}")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-url", default=os.environ.get("BASE_URL", "http://localhost:8000"))
    parser.add_argument("--cursor-file", type=Path, default=Path(".feed_cursor"))
    parser.add_argument("--from-start", action="store_true", help="ignore the saved cursor")
    parser.add_argument("--once", action="store_true", help="poll once, then exit")
    parser.add_argument("--interval", type=positive_float, default=1.0, help="idle poll seconds")
    args = parser.parse_args(argv)
    try:
        asyncio.run(run(args))
    except CursorError as exc:
        print(f"{exc}; fix or delete it, or pass --from-start", file=sys.stderr)
        return 2
    except httpx.HTTPError as exc:
        print(f"feed unavailable: {type(exc).__name__}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
