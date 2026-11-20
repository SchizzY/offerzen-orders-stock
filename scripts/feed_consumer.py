"""Follow the "order accepted" feed and print one line per event.

The cursor (the last seq handled) is saved to a file after each printed page, so a
restart resumes where it left off. Delivery is at-least-once: a consumer killed between
printing and saving prints that page again, and a real consumer dedupes by seq.

Outages (network errors, 5xx, 429, a response that is not the feed) are retried with
backoff. A request the feed rejects (any other 4xx) stops the consumer, since retrying
cannot fix it.

    python scripts/feed_consumer.py [--base-url URL] [--cursor-file PATH] [--from-start]
                                    [--once] [--interval SECONDS]
"""

import argparse
import asyncio
import os
import signal
import sys
import tempfile
from pathlib import Path
from typing import Any, TextIO

import httpx

MAX_BACKOFF_SECONDS = 30.0
EXIT_BROKEN_PIPE = 128 + 13  # what a shell reports for a process killed by SIGPIPE


class CursorError(Exception):
    pass


class FeedUnavailable(Exception):
    """The feed could not be read right now; retrying later may work."""


class FeedRejected(Exception):
    """The feed refused the request; retrying the same request cannot work."""


def load_cursor(path: Path) -> int:
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return 0
    except (OSError, UnicodeDecodeError) as exc:
        raise CursorError(
            f"cursor file {path} cannot be read ({type(exc).__name__}); "
            "fix or delete it, or pass --from-start"
        ) from None
    digits = raw.strip()
    if not (digits.isascii() and digits.isdigit()):
        raise CursorError(
            f"cursor file {path} does not hold a non-negative integer: {raw!r}; "
            "fix or delete it, or pass --from-start"
        )
    return int(digits)


def check_cursor_writable(path: Path) -> None:
    """Fail before the first poll, not after printing a page that can then never be saved."""
    if path.is_dir():
        raise CursorError(f"cursor file {path} is a directory")
    try:
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    except OSError as exc:
        raise CursorError(f"cannot write cursor file {path}: {exc.strerror}") from None
    os.close(fd)
    os.unlink(tmp)


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


def parse_page(response: httpx.Response, after: int) -> tuple[list[str], int]:
    """Turn a feed response into its printed lines and next cursor, or raise why it can't."""
    status = response.status_code
    if status >= 500 or status == 429:
        raise FeedUnavailable(f"HTTP {status}")
    if status == 422:
        raise FeedRejected(
            f"feed rejected cursor {after} (HTTP 422); "
            "fix or delete the cursor file, or pass --from-start"
        )
    if status != 200:
        raise FeedRejected(f"feed request failed: HTTP {status} from {response.url}")
    try:
        page = response.json()
        lines = [format_event(event) for event in page["events"]]
        next_after = page["next_after"]
    except (ValueError, KeyError, TypeError):
        raise FeedUnavailable(f"response from {response.url} is not the event feed") from None
    if type(next_after) is not int:
        raise FeedUnavailable(f"response from {response.url} is not the event feed")
    return lines, next_after


async def poll_once(client: httpx.AsyncClient, after: int, cursor_file: Path, out: TextIO) -> int:
    """Fetch one page after `after`, print it, then save and return the new cursor."""
    try:
        response = await client.get("/events", params={"after": after})
    except httpx.TransportError as exc:
        raise FeedUnavailable(type(exc).__name__) from None
    lines, next_after = parse_page(response, after)
    for line in lines:
        print(line, file=out, flush=True)
    if next_after != after:
        save_cursor(cursor_file, next_after)
    return next_after


async def drain(client: httpx.AsyncClient, after: int, cursor_file: Path, out: TextIO) -> int:
    """Read page after page until one brings nothing new, and return the final cursor."""
    while (next_after := await poll_once(client, after, cursor_file, out)) != after:
        after = next_after
    return after


async def follow(
    client: httpx.AsyncClient, after: int, cursor_file: Path, interval: float, out: TextIO
) -> None:
    """Poll forever: drain without pausing, idle `interval` when caught up, back off on outages."""
    delay = interval
    while True:
        try:
            next_after = await poll_once(client, after, cursor_file, out)
        except FeedUnavailable as exc:
            print(f"feed unavailable: {exc}; retrying in {delay:g}s", file=sys.stderr, flush=True)
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_BACKOFF_SECONDS)
            continue
        delay = interval
        if next_after == after:
            await asyncio.sleep(interval)
        after = next_after


async def run(args: argparse.Namespace) -> None:
    check_cursor_writable(args.cursor_file)
    after = 0 if args.from_start else load_cursor(args.cursor_file)
    async with httpx.AsyncClient(base_url=args.base_url, timeout=10) as client:
        if args.once:
            await drain(client, after, args.cursor_file, sys.stdout)
        else:
            await follow(client, after, args.cursor_file, args.interval, sys.stdout)


def positive_float(raw: str) -> float:
    value = float(raw)
    if not value > 0 or value == float("inf"):
        raise argparse.ArgumentTypeError(f"must be a positive number, got {raw}")
    return value


def _stop(signum: int, frame: object) -> None:
    raise KeyboardInterrupt


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-url", default=os.environ.get("BASE_URL", "http://localhost:8000"))
    parser.add_argument("--cursor-file", type=Path, default=Path(".feed_cursor"))
    parser.add_argument("--from-start", action="store_true", help="ignore the saved cursor")
    parser.add_argument(
        "--once",
        action="store_true",
        help="print every event after the cursor, page by page, then exit",
    )
    parser.add_argument("--interval", type=positive_float, default=1.0, help="idle poll seconds")
    args = parser.parse_args(argv)
    signal.signal(signal.SIGTERM, _stop)
    try:
        asyncio.run(run(args))
    except CursorError as exc:
        print(exc, file=sys.stderr)
        return 2
    except FeedUnavailable as exc:
        print(f"feed unavailable: {exc}", file=sys.stderr)
        return 1
    except FeedRejected as exc:
        print(exc, file=sys.stderr)
        return 1
    except BrokenPipeError:
        # The reader went away (`| head`). Point stdout at /dev/null so the interpreter's
        # final flush of the unwritten line does not print a second error on exit.
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return EXIT_BROKEN_PIPE
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
