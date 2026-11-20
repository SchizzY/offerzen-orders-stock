import asyncio
import importlib.util
import io
import os
import select
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType

import httpx
import pytest
from httpx import AsyncClient
from sqlalchemy import text

from app.db import engine
from tests.test_events_api import place, seed_products

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "feed_consumer.py"


def load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("feed_consumer", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


consumer = load_script()


@pytest.fixture
async def feed(client: AsyncClient) -> AsyncClient:
    return await seed_products(client)


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


def feed_event(seq: int) -> dict[str, object]:
    return {
        "seq": seq,
        "data": {"order_ref": f"web-{seq}", "total_cents": 199, "items": [{"sku": "B", "qty": 1}]},
    }


def paged_feed(head: int, page_size: int) -> Callable[[httpx.Request], httpx.Response]:
    """A feed holding seq 1..head that serves at most `page_size` events per page."""

    def handler(request: httpx.Request) -> httpx.Response:
        after = int(request.url.params.get("after", "0"))
        seqs = range(after + 1, min(after + page_size, head) + 1)
        events = [feed_event(seq) for seq in seqs]
        return httpx.Response(
            200, json={"events": events, "next_after": seqs[-1] if seqs else after}
        )

    return handler


def scripted(*steps: httpx.Response | Exception) -> Callable[[httpx.Request], httpx.Response]:
    remaining = list(steps)

    def handler(request: httpx.Request) -> httpx.Response:
        step = remaining.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    return handler


def mock_client(handler: Callable[[httpx.Request], httpx.Response]) -> AsyncClient:
    return AsyncClient(transport=httpx.MockTransport(handler), base_url="http://x")


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


async def test_drain_reads_every_page_and_saves_after_each(tmp_path: Path) -> None:
    cursor_file = tmp_path / "cursor"
    async with mock_client(paged_feed(head=5, page_size=2)) as client:
        with pytest.raises(Killed):
            await consumer.drain(client, 0, cursor_file, DiesAfter(3))
        assert cursor_file.read_text() == "2\n"

        out = io.StringIO()
        assert await consumer.drain(client, 2, cursor_file, out) == 5

    assert [line.split()[0] for line in out.getvalue().splitlines()] == ["seq=3", "seq=4", "seq=5"]
    assert cursor_file.read_text() == "5\n"


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="<html><body>It works!</body></html>"),
        httpx.Response(200, json={"events": []}),
        httpx.Response(200, json={"events": [{"seq": 1}], "next_after": 1}),
        httpx.Response(200, json={"events": [feed_event(1)], "next_after": "1"}),
        httpx.Response(200, json=[]),
    ],
    ids=["html", "no-next-after", "event-without-data", "string-next-after", "list"],
)
async def test_a_response_that_is_not_the_feed_is_unavailable(
    tmp_path: Path, response: httpx.Response
) -> None:
    out = io.StringIO()
    async with mock_client(lambda _: response) as client:
        with pytest.raises(consumer.FeedUnavailable, match="not the event feed"):
            await consumer.poll_once(client, 0, tmp_path / "cursor", out)

    assert out.getvalue() == ""
    assert not (tmp_path / "cursor").exists()


async def test_a_server_error_does_not_advance_the_cursor(tmp_path: Path) -> None:
    async with mock_client(lambda _: httpx.Response(503)) as client:
        with pytest.raises(consumer.FeedUnavailable, match="HTTP 503"):
            await consumer.poll_once(client, 0, tmp_path / "cursor", io.StringIO())

    assert not (tmp_path / "cursor").exists()


async def test_follow_retries_outages_and_stops_on_a_rejected_request(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cursor_file = tmp_path / "cursor"
    handler = scripted(
        httpx.Response(503),
        httpx.Response(429),
        httpx.ConnectError("refused"),
        httpx.Response(200, text="<html></html>"),
        httpx.Response(200, json={"events": [feed_event(1)], "next_after": 1}),
        httpx.Response(404),
    )
    out = io.StringIO()

    async with mock_client(handler) as client:
        with pytest.raises(consumer.FeedRejected, match="HTTP 404"):
            await consumer.follow(client, 0, cursor_file, 0.001, out)

    assert out.getvalue().splitlines() == ["seq=1 order=web-1 total_cents=199 items=Bx1"]
    assert cursor_file.read_text() == "1\n"
    retries = capsys.readouterr().err.splitlines()
    reasons = ["HTTP 503", "HTTP 429", "ConnectError", "not the event feed"]
    for line, reason in zip(retries, reasons, strict=True):
        assert line.startswith("feed unavailable: ") and reason in line


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


def assert_one_error_line(result: subprocess.CompletedProcess[str], code: int) -> str:
    assert result.returncode == code
    assert result.stdout == ""
    assert "Traceback" not in result.stderr
    [line] = result.stderr.splitlines()
    return line


@pytest.mark.parametrize("content", ["abc\n", "-5\n", "", "1.5\n"])
def test_a_corrupt_cursor_file_stops_with_an_error(tmp_path: Path, content: str) -> None:
    cursor_file = tmp_path / "cursor"
    cursor_file.write_text(content)

    result = run_cli(
        "--once", "--cursor-file", str(cursor_file), "--base-url", "http://127.0.0.1:9"
    )

    assert str(cursor_file) in assert_one_error_line(result, 2)
    assert cursor_file.read_text() == content


@pytest.mark.parametrize("unreadable", ["not-utf8", "no-permission"])
def test_an_unreadable_cursor_file_stops_with_an_error(tmp_path: Path, unreadable: str) -> None:
    cursor_file = tmp_path / "cursor"
    cursor_file.write_bytes(b"\xff\xfe\x00")
    if unreadable == "no-permission":
        if os.geteuid() == 0:
            pytest.skip("root can read a mode-000 file")
        cursor_file.write_text("7\n")
        cursor_file.chmod(0)
    try:
        result = run_cli(
            "--once", "--cursor-file", str(cursor_file), "--base-url", "http://127.0.0.1:9"
        )
    finally:
        cursor_file.chmod(0o600)

    assert str(cursor_file) in assert_one_error_line(result, 2)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
    return port


def wait_for_port(port: int, deadline: float) -> None:
    while time.monotonic() < deadline:
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise TimeoutError(f"nothing listening on {port}")


def serve(args: list[str], port: int, cwd: Path = ROOT) -> Iterator[str]:
    process = subprocess.Popen(
        [sys.executable, "-m", *args],
        cwd=cwd,
        env={**os.environ, "LOG_LEVEL": "WARNING"},
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_port(port, time.monotonic() + 20)
        yield f"http://127.0.0.1:{port}"
    finally:
        process.terminate()
        process.wait(timeout=10)


@pytest.fixture
def server() -> Iterator[str]:
    port = free_port()
    yield from serve(["uvicorn", "app.main:app", "--port", str(port)], port)


@pytest.fixture
def html_server(tmp_path: Path) -> Iterator[str]:
    """A web server that answers /events with an HTML page, as a wrong --base-url might."""
    site = tmp_path / "site"
    site.mkdir()
    (site / "events").write_text("<html><body>It works!</body></html>\n")
    port = free_port()
    yield from serve(["http.server", "--bind", "127.0.0.1", str(port)], port, cwd=site)


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


async def test_once_drains_past_the_first_page(
    feed: AsyncClient, server: str, tmp_path: Path
) -> None:
    await place(feed, "web-1", {"BAN-001": 1})
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO outbox_events (event_type, aggregate_id, payload)"
                " SELECT event_type, aggregate_id,"
                "   jsonb_set(payload, '{order_ref}', to_jsonb('web-' || n))"
                " FROM outbox_events, generate_series(2, 250) n"
            )
        )
    await relay()

    result = run_cli("--once", "--base-url", server, "--cursor-file", str(tmp_path / "cursor"))

    assert result.returncode == 0
    orders = [line.split()[1] for line in result.stdout.splitlines()]
    assert orders == [f"order=web-{n}" for n in range(1, 251)]
    assert (tmp_path / "cursor").read_text() == "250\n"


@pytest.mark.parametrize("mode", [["--once"], ["--interval", "0.05"]], ids=["once", "follow"])
@pytest.mark.parametrize("where", ["missing-dir", "is-a-dir"])
async def test_an_unwritable_cursor_path_stops_before_polling(
    feed: AsyncClient, server: str, tmp_path: Path, mode: list[str], where: str
) -> None:
    await place(feed, "web-1", {"BAN-001": 1})
    await relay()
    cursor_file = tmp_path / "missing" / "cursor" if where == "missing-dir" else tmp_path

    result = run_cli(*mode, "--base-url", server, "--cursor-file", str(cursor_file))

    assert str(cursor_file) in assert_one_error_line(result, 2)


@pytest.mark.parametrize("mode", [["--once"], ["--interval", "0.05"]], ids=["once", "follow"])
@pytest.mark.parametrize(
    ("cursor", "path", "status"),
    [(str(2**63), "", "HTTP 422"), ("0", "/nope", "HTTP 404")],
    ids=["cursor-out-of-range", "wrong-base-url"],
)
def test_a_rejected_request_stops_instead_of_retrying(
    server: str, tmp_path: Path, mode: list[str], cursor: str, path: str, status: str
) -> None:
    cursor_file = tmp_path / "cursor"
    cursor_file.write_text(f"{cursor}\n")

    result = run_cli(*mode, "--base-url", server + path, "--cursor-file", str(cursor_file))

    line = assert_one_error_line(result, 1)
    assert status in line
    if status == "HTTP 422":
        assert "cursor" in line
    assert cursor_file.read_text() == f"{cursor}\n"


def test_once_reports_a_response_that_is_not_the_feed(html_server: str, tmp_path: Path) -> None:
    result = run_cli("--once", "--base-url", html_server, "--cursor-file", str(tmp_path / "c"))

    assert "feed unavailable" in assert_one_error_line(result, 1)
    assert not (tmp_path / "c").exists()


@pytest.fixture
def dead_url() -> str:
    return f"http://127.0.0.1:{free_port()}"


@pytest.mark.parametrize(
    ("target", "reason"),
    [("dead_url", "ConnectError"), ("html_server", "not the event feed")],
    ids=["connection-refused", "not-the-feed"],
)
@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM], ids=["sigint", "sigterm"])
def test_follow_keeps_retrying_and_exits_cleanly_on_a_stop_signal(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    target: str,
    reason: str,
    signum: signal.Signals,
) -> None:
    base_url = request.getfixturevalue(target)
    process = subprocess.Popen(
        [
            sys.executable,
            str(SCRIPT),
            "--base-url",
            base_url,
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
        process.send_signal(signum)
        stdout, stderr = process.communicate(timeout=10)
    finally:
        process.kill()

    assert process.returncode == 0
    assert stdout == ""
    assert all(line.startswith("feed unavailable: ") and reason in line for line in errors)
    assert "Traceback" not in stderr
    assert not (tmp_path / "cursor").exists()


async def test_a_closed_stdout_exits_quietly_without_saving(
    feed: AsyncClient, server: str, tmp_path: Path
) -> None:
    """`feed_consumer.py | head` once head has exited: the next print hits a closed pipe."""
    read_end, write_end = os.pipe()
    os.close(read_end)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(SCRIPT),
        "--base-url",
        server,
        "--cursor-file",
        str(tmp_path / "cursor"),
        "--interval",
        "0.05",
        stdout=write_end,
        stderr=subprocess.PIPE,
        cwd=ROOT,
    )
    os.close(write_end)
    try:
        await place(feed, "web-1", {"BAN-001": 1})
        await relay()
        _, stderr = await asyncio.wait_for(process.communicate(), timeout=15)
    finally:
        if process.returncode is None:
            process.kill()

    assert process.returncode == 128 + signal.SIGPIPE
    assert stderr == b""
    assert not (tmp_path / "cursor").exists()


def test_help_says_once_drains_the_feed() -> None:
    result = run_cli("--help")

    assert "every event after the cursor" in " ".join(result.stdout.split())
