import contextlib
import importlib.util
import os
import re
import select
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

import httpx
import pytest
import redis

from app.config import settings

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "outage_demo.py"
STREAM = settings.orders_stream
GROUP = settings.stock_group
STOP_SIGNALS = [signal.SIGINT, signal.SIGTERM, signal.SIGHUP]
SLEEPER = [sys.executable, "-c", "import time; time.sleep(60)"]


@pytest.fixture(scope="module")
def demo() -> ModuleType:
    spec = importlib.util.spec_from_file_location("outage_demo", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def sync_valkey() -> Iterator[redis.Redis]:
    client = redis.Redis.from_url(settings.valkey_url, decode_responses=True)
    yield client
    client.close()


@pytest.fixture
def restore_signals() -> Iterator[None]:
    saved = {signum: signal.getsignal(signum) for signum in STOP_SIGNALS}
    yield
    for signum, handler in saved.items():
        signal.signal(signum, handler)


@contextlib.contextmanager
def consumer_kept_active(name: str) -> Iterator[None]:
    """Another worker: a consumer in the stock group that keeps polling."""
    client = redis.Redis.from_url(settings.valkey_url)
    with contextlib.suppress(redis.ResponseError):
        client.xgroup_create(STREAM, GROUP, id="$", mkstream=True)
    stop = threading.Event()

    def poll() -> None:
        while not stop.wait(0.05):
            client.xreadgroup(GROUP, name, {STREAM: "0"})

    client.xreadgroup(GROUP, name, {STREAM: "0"})

    thread = threading.Thread(target=poll)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join()
        client.close()


def add_backlog(valkey: redis.Redis, events: int) -> None:
    for _ in range(events):
        valkey.xadd(STREAM, {"event_type": "other.event"})
    valkey.xgroup_create(STREAM, GROUP, id="0")


def stock_api(sku: str, pending_qty: Callable[[], int]) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/stock/{sku}"
        return httpx.Response(200, json={"sku": sku, "on_hand": 10, "pending_qty": pending_qty()})

    return httpx.Client(transport=httpx.MockTransport(handler), base_url="http://api")


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf"])
def test_timeout_must_be_positive_and_finite(value: str) -> None:
    result = subprocess.run(
        [sys.executable, SCRIPT, "--base-url", "http://127.0.0.1:9", f"--timeout={value}"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 2
    assert "argument --timeout" in result.stderr


@pytest.mark.parametrize("signum", STOP_SIGNALS, ids=lambda s: s.name)
def test_stop_signal_landing_inside_spawn_leaves_no_worker(
    demo: ModuleType,
    sync_valkey: redis.Redis,
    restore_signals: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    signum: signal.Signals,
) -> None:
    real_popen = subprocess.Popen
    spawned: list[subprocess.Popen] = []

    def popen_then_signal(*args, **kwargs) -> subprocess.Popen:
        # The child exists, but Popen has not returned it to the caller yet.
        spawned.append(real_popen(*args, **kwargs))
        os.kill(os.getpid(), signum)
        time.sleep(0.5)
        return spawned[-1]

    monkeypatch.setattr(subprocess, "Popen", popen_then_signal)
    demo.install_signal_handlers()
    with (tmp_path / "worker.log").open("w") as log:
        worker = demo.Worker(SLEEPER, sync_valkey, log)
        try:
            with pytest.raises(demo.Interrupted):
                worker.start(timeout=5)
            worker.stop()
            assert spawned[0].poll() is not None
        finally:
            for proc in spawned:
                proc.kill()
                proc.wait()


def test_start_waits_for_its_own_consumer_not_any_live_one(
    demo: ModuleType, sync_valkey: redis.Redis, restore_signals: None, tmp_path: Path
) -> None:
    demo.install_signal_handlers()
    with (
        consumer_kept_active("other-host-1"),
        (tmp_path / "worker.log").open("w") as log,
    ):
        worker = demo.Worker([sys.executable, "-m", "app.stock.worker"], sync_valkey, log)
        try:
            worker.start(timeout=20)
            names = [c["name"] for c in sync_valkey.xinfo_consumers(STREAM, GROUP)]
            assert any(name.endswith(f"-{worker.proc.pid}") for name in names), names
        finally:
            worker.stop()


def test_drain_keeps_waiting_while_the_worker_works_through_a_backlog(
    demo: ModuleType, sync_valkey: redis.Redis
) -> None:
    add_backlog(sync_valkey, 30)
    drained = threading.Event()

    def slow_worker() -> None:
        client = redis.Redis.from_url(settings.valkey_url)
        while response := client.xreadgroup(GROUP, "slow-1", {STREAM: ">"}, count=1):
            client.xack(STREAM, GROUP, response[0][1][0][0])
            time.sleep(0.05)
        drained.set()
        client.close()

    thread = threading.Thread(target=slow_worker)
    thread.start()
    try:
        with stock_api("OUT-1", lambda: 0 if drained.is_set() else 5) as client:
            demo.Demo(client, sync_valkey, None, 0.5, "OUT-1").drain()
    finally:
        thread.join()


def test_drain_that_stalls_on_a_backlog_suggests_a_longer_timeout(
    demo: ModuleType, sync_valkey: redis.Redis
) -> None:
    add_backlog(sync_valkey, 10)

    with stock_api("OUT-1", lambda: 5) as client, pytest.raises(demo.Failed) as failed:
        demo.Demo(client, sync_valkey, None, 0.5, "OUT-1").drain()

    message = str(failed.value)
    assert "pending_qty still 5" in message
    assert "10 events" in message
    assert "backlog" in message
    assert "--timeout" in message


def test_preflight_names_a_recently_active_worker_on_stderr(
    demo: ModuleType,
    sync_valkey: redis.Redis,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(demo, "LIVE_IDLE_MS", 1000)
    healthy = httpx.MockTransport(lambda _: httpx.Response(200))

    with (
        consumer_kept_active("busy-host-9"),
        httpx.Client(transport=healthy, base_url="http://api") as client,
        pytest.raises(demo.CannotRun) as cannot_run,
    ):
        demo.preflight(client, sync_valkey, force=False)

    assert str(cannot_run.value) == (
        "a stock worker was active in the last 1 s (busy-host-9); "
        "stop it and re-run, or pass --force"
    )
    out, err = capsys.readouterr()
    assert out == ""
    assert "waiting for recently active consumers to go idle: busy-host-9" in err


# End to end: the real API with its relay, the real worker, the script as a process.


@dataclass
class Api:
    proc: subprocess.Popen
    url: str

    def stop(self) -> None:
        self.proc.terminate()
        self.proc.wait(10)


@pytest.fixture
def api() -> Iterator[Api]:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", f"--port={port}"],
        cwd=ROOT,
        env={**os.environ, "RELAY_ENABLED": "true"},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    api = Api(proc, f"http://127.0.0.1:{port}")
    deadline = time.monotonic() + 20
    while True:
        with contextlib.suppress(httpx.TransportError):
            if httpx.get(f"{api.url}/health").status_code == 200:
                break
        assert proc.poll() is None and time.monotonic() < deadline, "API did not start"
        time.sleep(0.1)
    yield api
    api.stop()


def run_demo(api: Api) -> subprocess.Popen:
    # Unbuffered bytes, so select() sees every line the script has written.
    return subprocess.Popen(
        [sys.executable, SCRIPT, "--base-url", api.url],
        cwd=ROOT,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )


def finish(proc: subprocess.Popen) -> tuple[str, str]:
    stdout, stderr = proc.communicate(timeout=90)
    return stdout.decode(), stderr.decode()


def read_until(proc: subprocess.Popen, pattern: str, timeout: float = 30) -> list[str]:
    lines: list[str] = []
    deadline = time.monotonic() + timeout
    while not lines or not re.search(pattern, lines[-1]):
        ready, _, _ = select.select([proc.stdout], [], [], max(0, deadline - time.monotonic()))
        line = proc.stdout.readline().decode() if ready else ""
        assert line, f"no line matching {pattern!r}; output so far: {lines}"
        lines.append(line)
    return lines


def assert_no_worker_survives(stdout: str) -> None:
    log = Path(re.search(r"^worker log: (.+)$", stdout, re.MULTILINE).group(1))
    pids = [int(pid) for pid in re.findall(r"consumer=\S+-(\d+)", log.read_text())]
    assert pids, "the worker never started"
    alive = []
    for pid in pids:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)
            alive.append(pid)
    assert not alive, f"orphaned workers {alive}"


def test_demo_passes_end_to_end(api: Api) -> None:
    proc = run_demo(api)
    stdout, stderr = finish(proc)

    assert proc.returncode == 0, stdout + stderr
    assert stdout.rstrip().endswith("PASS")
    log = re.search(r"^worker log: (.+)$", stdout, re.MULTILINE).group(1)
    assert "outage-demo-OUT-" in log
    assert_no_worker_survives(stdout)


@pytest.mark.parametrize("signum", STOP_SIGNALS, ids=lambda s: s.name)
def test_stop_signal_mid_run_stops_the_worker(api: Api, signum: signal.Signals) -> None:
    proc = run_demo(api)
    seen = read_until(proc, r"^healthy .* worker started")
    proc.send_signal(signum)
    stdout, stderr = finish(proc)

    assert_no_worker_survives("".join(seen) + stdout)
    assert proc.returncode == 128 + signum, stderr
    assert f"interrupted by {signum.name}" in stderr


def test_api_dying_mid_run_names_the_service_and_phase(api: Api) -> None:
    proc = run_demo(api)
    seen = read_until(proc, r"^outage .* worker stopped")
    api.stop()
    stdout, stderr = finish(proc)

    assert_no_worker_survives("".join(seen) + stdout)
    assert proc.returncode == 2, stdout + stderr
    assert f"API unreachable at {api.url} during outage" in stderr
