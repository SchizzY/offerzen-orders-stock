import importlib.util
import io
import os
import select
import signal
import socket
import subprocess
import sys
import time
from collections.abc import AsyncGenerator, Iterator
from pathlib import Path
from types import ModuleType

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from app.db import engine
from tests.test_events_api import APPLES, BANANAS, feed_app, place

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "feed_consumer.py"
PORT = 8230


def load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("feed_consumer", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


consumer = load_script()


@pytest.fixture
async def feed() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(transport=ASGITransport(app=feed_app()), base_url="http://test") as c:
        for product in (BANANAS, APPLES):
            (await c.post("/products", json=product)).raise_for_status()
        yield c


async def relay() -> None:
    """Stand in for the relay: sequence every unsequenced event after the current max, by id."""
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "UPDATE outbox_events o SET seq = s.n FROM ("
                " SELECT id, (SELECT coalesce(max(seq), 0) FROM outbox_events)"
                "   + row_number() OVER (ORDER BY id) AS n"
                " FROM outbox_events WHERE seq IS NULL) s"
                " WHERE o.id = s.id"
            )
        )


class Killed(Exception):
    pass


class DiesAfter(io.StringIO):
    """stdout for a process that is killed right after printing `lines` lines."""

    def __init__(self, lines: int) -> None:
        super().__init__()
        self.left = lines

    def write(self, s: str) -> int:
        written = super().write(s)
        self.left -= s.count("\n")
        if self.left <= 0:
            raise Killed
        return written


async def test_prints_each_event_and_saves_the_cursor(feed: AsyncClient, tmp_path: Path) -> None:
    await place(feed, "web-1", {"BAN-001": 2, "APL-003": 1})
    await place(feed, "web-2", {"APL-003": 3})
    await relay()
    cursor_file = tmp_path / "cursor"
    out = io.StringIO()

    after = await consumer.poll_once(feed, 0, cursor_file, out)

    assert out.getvalue().splitlines() == [
        "seq=1 order=web-1 total_cents=747 items=APL-003x1,BAN-001x2",
        "seq=2 order=web-2 total_cents=1047 items=APL-003x3",
    ]
    assert "cust-42" not in out.getvalue()
    assert after == 2
    assert cursor_file.read_text() == "2\n"
    assert os.listdir(tmp_path) == ["cursor"]


async def test_restart_resumes_after_the_saved_cursor(feed: AsyncClient, tmp_path: Path) -> None:
    cursor_file = tmp_path / "cursor"
    await place(feed, "web-1", {"BAN-001": 1})
    await relay()
    await consumer.poll_once(feed, consumer.load_cursor(cursor_file), cursor_file, io.StringIO())
    await place(feed, "web-2", {"BAN-001": 1})
    await place(feed, "web-3", {"BAN-001": 1})
    await relay()

    out = io.StringIO()
    await consumer.poll_once(feed, consumer.load_cursor(cursor_file), cursor_file, out)

    assert [line.split()[1] for line in out.getvalue().splitlines()] == [
        "order=web-2",
        "order=web-3",
    ]
    assert cursor_file.read_text() == "3\n"


async def test_an_empty_poll_leaves_the_cursor_alone(feed: AsyncClient, tmp_path: Path) -> None:
    cursor_file = tmp_path / "cursor"
    cursor_file.write_text("7\n")
    out = io.StringIO()

    assert await consumer.poll_once(feed, 7, cursor_file, out) == 7

    assert out.getvalue() == ""
    assert cursor_file.read_text() == "7\n"


async def test_a_crash_before_saving_reprints_the_batch(feed: AsyncClient, tmp_path: Path) -> None:
    cursor_file = tmp_path / "cursor"
    cursor_file.write_text("0\n")
    for ref in ("web-1", "web-2", "web-3"):
        await place(feed, ref, {"BAN-001": 1})
    await relay()

    with pytest.raises(Killed):
        await consumer.poll_once(feed, 0, cursor_file, DiesAfter(3))
    assert cursor_file.read_text() == "0\n"

    out = io.StringIO()
    await consumer.poll_once(feed, consumer.load_cursor(cursor_file), cursor_file, out)

    assert [line.split()[0] for line in out.getvalue().splitlines()] == ["seq=1", "seq=2", "seq=3"]
    assert cursor_file.read_text() == "3\n"


def test_a_missing_cursor_file_starts_from_zero(tmp_path: Path) -> None:
    assert consumer.load_cursor(tmp_path / "absent") == 0


def run_cli(*args: str, timeout: float = 20) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=ROOT,
    )


@pytest.mark.parametrize("content", ["abc\n", "-5\n", "", "1.5\n"])
def test_a_corrupt_cursor_file_stops_with_an_error(tmp_path: Path, content: str) -> None:
    cursor_file = tmp_path / "cursor"
    cursor_file.write_text(content)

    result = run_cli(
        "--once", "--cursor-file", str(cursor_file), "--base-url", "http://127.0.0.1:9"
    )

    assert result.returncode == 2
    assert result.stdout == ""
    assert str(cursor_file) in result.stderr
    assert "Traceback" not in result.stderr
    assert cursor_file.read_text() == content


def wait_for_port(port: int, deadline: float) -> None:
    while time.monotonic() < deadline:
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise TimeoutError(f"nothing listening on {port}")


@pytest.fixture
def server() -> Iterator[str]:
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "--factory",
            "tests.test_events_api:feed_app",
            "--port",
            str(PORT),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env={**os.environ, "LOG_LEVEL": "WARNING"},
    )
    try:
        wait_for_port(PORT, time.monotonic() + 20)
        yield f"http://127.0.0.1:{PORT}"
    finally:
        process.terminate()
        process.wait(timeout=10)


async def test_cli_resumes_and_replays_from_start(
    feed: AsyncClient, server: str, tmp_path: Path
) -> None:
    await place(feed, "web-1", {"BAN-001": 2})
    await place(feed, "web-2", {"APL-003": 1})
    await relay()
    cursor = ["--base-url", server, "--cursor-file", str(tmp_path / "cursor")]

    first = run_cli("--once", *cursor)
    again = run_cli("--once", *cursor)
    replay = run_cli("--once", "--from-start", *cursor)

    lines = [
        "seq=1 order=web-1 total_cents=398 items=BAN-001x2",
        "seq=2 order=web-2 total_cents=349 items=APL-003x1",
    ]
    assert (first.returncode, first.stdout.splitlines()) == (0, lines)
    assert (again.returncode, again.stdout) == (0, "")
    assert (replay.returncode, replay.stdout.splitlines()) == (0, lines)
    assert (tmp_path / "cursor").read_text() == "2\n"


def test_keeps_retrying_through_network_errors_and_exits_cleanly_on_sigint(
    tmp_path: Path,
) -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        dead_port = sock.getsockname()[1]
    process = subprocess.Popen(
        [
            sys.executable,
            str(SCRIPT),
            "--base-url",
            f"http://127.0.0.1:{dead_port}",
            "--cursor-file",
            str(tmp_path / "cursor"),
            "--interval",
            "0.05",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=ROOT,
    )
    try:
        assert process.stderr is not None
        errors = []
        for _ in range(2):
            ready, _, _ = select.select([process.stderr], [], [], 15)
            assert ready, "no error line on stderr"
            errors.append(process.stderr.readline())
        assert process.poll() is None
        process.send_signal(signal.SIGINT)
        stdout, stderr = process.communicate(timeout=10)
    finally:
        process.kill()

    assert process.returncode == 0
    assert stdout == ""
    assert all("ConnectError" in line for line in errors)
    assert "Traceback" not in stderr
    assert not (tmp_path / "cursor").exists()


async def test_a_server_error_does_not_advance_the_cursor(tmp_path: Path) -> None:
    def broken(_: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    async with AsyncClient(transport=httpx.MockTransport(broken), base_url="http://x") as client:
        with pytest.raises(httpx.HTTPError):
            await consumer.poll_once(client, 0, tmp_path / "cursor", io.StringIO())

    assert not (tmp_path / "cursor").exists()
