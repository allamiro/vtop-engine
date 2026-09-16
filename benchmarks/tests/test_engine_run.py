"""A long-lived engine for the measured window, its /metrics scraped (#510).

The acceptance criteria of #510, one group each:

* the default is unchanged — a scenario naming no engine mode writes the files
  and headers it always has, asserted over a generated results directory;
* a long-lived run is observed — engine_metrics.csv carries more than one
  timestamped sample of the engine's own counters over the configured window;
* per-second buckets exist — counters carry a delta and a per-second rate;
* a silent scrape failure fails the run — mid-window, and never answering;
* the engine is not left running — on a failed run and on an interrupt, in
  both runner modes.

The host-mode supervision tests start a REAL process (a stand-in engine that
serves /metrics on VTOP_METRICS_ADDR) and assert its pid is gone afterwards,
because "the engine was terminated" is a property of the operating system, not
of a mock. Container mode cannot start docker in CI, so its tests assert the
stop is issued INSIDE the container — the property that matters there, since
killing the `docker exec` client leaves the exec'd process running.
"""
from __future__ import annotations

import csv
import glob
import json
import os
import signal
import socket
import stat
import subprocess
import sys
import textwrap
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import run_benchmark  # noqa: E402
from lib import engine, engine_run, prom_text  # noqa: E402
from lib.metrics import CSV_HEADERS  # noqa: E402
from lib.scenario import DEFAULTS, Scenario  # noqa: E402

# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def _scenario(**overrides) -> Scenario:
    return Scenario({**DEFAULTS, **overrides})


def _body(commits: int, bytes_out: int, width: int = 8) -> str:
    return textwrap.dedent(f"""\
        # HELP vtop_commits_total Source progress commits
        # TYPE vtop_commits_total counter
        vtop_commits_total{{format="jsonl",source_type="file",tenant="default"}} {commits}
        # TYPE vtop_bytes_out_total counter
        vtop_bytes_out_total{{format="jsonl",source_type="file",tenant="default"}} {bytes_out}
        # TYPE vtop_upload_width gauge
        vtop_upload_width {width}
        """)


class StubEndpoint:
    """A /metrics endpoint in this process standing in for the engine's: it
    answers only once `arm()` says the engine started (before that, nothing
    here is an engine — the pre-launch leak check must see no answer), then
    `answers` times with an increasing counter, then 503."""

    def __init__(self, answers: int | None = None, body=None) -> None:
        self.answers = answers
        self.body = body or (lambda served: _body(served * 10, served * 1000))
        self.armed = False
        self.served = 0
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - http.server's name
                if not stub.armed or (stub.answers is not None and stub.served >= stub.answers):
                    self.send_response(503)
                    self.end_headers()
                    return
                stub.served += 1
                body = stub.body(stub.served).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def arm(self) -> None:
        self.armed = True

    @property
    def endpoint(self) -> engine_run.MetricsEndpoint:
        return engine_run.MetricsEndpoint(bind=f"127.0.0.1:{self.port}", host="127.0.0.1", port=self.port)

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def stub_factory():
    made = []

    def make(**kwargs) -> StubEndpoint:
        stub = StubEndpoint(**kwargs)
        made.append(stub)
        return stub

    yield make
    for stub in made:
        stub.close()


class FakeProcess:
    """An engine-process double that records how it was stopped."""

    def __init__(self, on_start=None) -> None:
        self.started = False
        self.stopped = False
        self.rc = None
        self.on_start = on_start

    def start(self, log) -> None:
        self.started = True
        if self.on_start is not None:
            self.on_start()

    def exit_code(self):
        return self.rc

    def stop(self, grace: float) -> str:
        self.stopped = True
        return "sigint"


def _closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# A stand-in `vtopctl run`: serves /metrics on VTOP_METRICS_ADDR, writes its
# pid, and exits 0 on SIGINT the way the engine's ctrl-c stop does. The mode
# variable picks a misbehaviour.
FAKE_ENGINE = r'''
import os, signal, sys, threading, time
from http.server import BaseHTTPRequestHandler, HTTPServer

if os.environ.get("FAKE_ENGINE_PROBE"):
    sys.exit(0)
mode = os.environ.get("FAKE_ENGINE_MODE", "ok")
with open(os.environ["FAKE_ENGINE_PIDFILE"], "w") as fh:
    fh.write(str(os.getpid()))
if mode == "ignore-sigint":
    signal.signal(signal.SIGINT, signal.SIG_IGN)
else:
    signal.signal(signal.SIGINT, lambda *a: os._exit(0))
start = time.monotonic()
served = [0]

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if mode.startswith("stop-answering-after:") and served[0] >= int(mode.split(":")[1]):
            self.send_response(503); self.end_headers(); return
        served[0] += 1
        n = int((time.monotonic() - start) * 100)
        if mode.startswith("finished:"):
            # Everything this engine will ever do was done before its endpoint
            # first answered: the counters hold still from the first scrape.
            n = int(mode.split(":")[1])
        body = ("# TYPE vtop_commits_total counter\n"
                'vtop_commits_total{format="jsonl",source_type="file",tenant="default"} %d\n'
                "# TYPE vtop_bytes_out_total counter\n"
                'vtop_bytes_out_total{format="jsonl",source_type="file",tenant="default"} %d\n'
                "# TYPE vtop_upload_width gauge\nvtop_upload_width 8\n" % (n, n * 1000)).encode()
        self.send_response(200); self.send_header("Content-Length", str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def log_message(self, *a):
        pass

if mode.startswith("exit-after:"):
    threading.Timer(float(mode.split(":")[1]), lambda: os._exit(3)).start()
host, port = os.environ["VTOP_METRICS_ADDR"].rsplit(":", 1)
HTTPServer((host, int(port)), H).serve_forever()
'''


@pytest.fixture
def fake_engine(tmp_path):
    path = tmp_path / "vtopctl"
    path.write_text(f"#!{sys.executable}\n{FAKE_ENGINE}", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    _require_exec(str(path))
    return str(path)


def _require_exec(path: str) -> None:
    """These tests supervise REAL processes started from the pytest temp dir.
    A noexec temp mount would otherwise fail them with a bare PermissionError
    that reads like a supervision bug."""
    try:
        subprocess.run([path, "--version"], capture_output=True, timeout=10,
                       env={**os.environ, "FAKE_ENGINE_PROBE": "1"})
    except PermissionError:
        pytest.fail(f"cannot execute files under {os.path.dirname(path)} (a noexec temp mount?); "
                    "run pytest with --basetemp on an exec-able filesystem")


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    # A zombie still answers kill(0); it is not running.
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as fh:
            return fh.read().split(")")[-1].split()[0] != "Z"
    except OSError:
        return True


def _read_pid(pidfile: str) -> int:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            with open(pidfile, encoding="utf-8") as fh:
                text = fh.read().strip()
            if text:
                return int(text)
        except OSError:
            pass
        time.sleep(0.02)
    raise AssertionError("the stand-in engine never wrote its pid")


def _host_engine(fake_engine, tmp_path, monkeypatch, mode="ok"):
    pidfile = str(tmp_path / "engine.pid")
    monkeypatch.setenv("FAKE_ENGINE_PIDFILE", pidfile)
    monkeypatch.setenv("FAKE_ENGINE_MODE", mode)
    endpoint = engine_run.endpoint_for("host")
    sc = _scenario(engine_mode="run", duration_seconds=1, backend="mock")
    proc = engine_run.launcher(fake_engine, str(tmp_path / "_engine.yaml"), sc, endpoint)
    assert isinstance(proc, engine_run.HostEngine)
    return sc, endpoint, proc, pidfile


# --------------------------------------------------------------------------
# the scenario keys
# --------------------------------------------------------------------------


def test_a_scenario_naming_no_engine_mode_runs_process_once():
    assert engine_run.engine_mode(_scenario()) == engine_run.PROCESS_ONCE
    assert engine_run.engine_mode(Scenario({"name": "bare"})) == engine_run.PROCESS_ONCE, (
        "a scenario without the key at all is today's per-cycle run")


@pytest.mark.parametrize("value", ["long-lived", "Run", True, 1])
def test_an_unknown_engine_mode_is_refused_not_defaulted(value):
    with pytest.raises(ValueError, match="engine_mode"):
        engine_run.engine_mode(_scenario(engine_mode=value, duration_seconds=5))


def test_a_long_lived_run_without_a_window_is_refused():
    with pytest.raises(ValueError, match="duration_seconds"):
        engine_run.engine_mode(_scenario(engine_mode="run", duration_seconds=0))


@pytest.mark.parametrize("interval", [0, -1, 1.5, 5, True, "fast"])
def test_a_scrape_interval_that_cannot_give_per_second_buckets_is_refused(interval):
    with pytest.raises(ValueError, match="engine_scrape_interval_seconds"):
        engine_run.engine_mode(_scenario(engine_mode="run", duration_seconds=5,
                                         engine_scrape_interval_seconds=interval))


def test_a_scrape_interval_on_a_per_cycle_run_is_refused_as_applied_by_nothing():
    with pytest.raises(ValueError, match="applies only to engine_mode: run"):
        engine_run.engine_mode(_scenario(engine_scrape_interval_seconds=0.5))


def test_container_mode_forwards_the_metrics_address_by_name_only():
    sc = _scenario(runner_mode="container", backend="mock")
    argv, env = engine.invocation("/bin/vtopctl", ["run", "--config", "/tmp/c.yaml"], sc,
                                  extra_env={"VTOP_METRICS_ADDR": "0.0.0.0:9464"})
    at = argv.index("VTOP_METRICS_ADDR")
    assert argv[at - 1] == "-e" and at < argv.index(engine.CONTAINER_SERVICE), (
        "the address must cross the exec boundary as a named variable before the service")
    assert "0.0.0.0:9464" not in argv and env["VTOP_METRICS_ADDR"] == "0.0.0.0:9464"


def test_the_compose_engine_publishes_the_metrics_port_on_loopback_only():
    compose = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "docker-compose.benchmark.yml")
    with open(compose, encoding="utf-8") as fh:
        text = fh.read()
    service = text.split("\n  vtop-engine:\n", 1)[1].split("\n  netem:\n", 1)[0]
    expected = (f'"${{VTOP_BIND_ADDR:-127.0.0.1}}:${{{engine_run.METRICS_PORT_ENV}:-'
                f'{engine_run.CONTAINER_METRICS_PORT}}}:{engine_run.CONTAINER_METRICS_PORT}"')
    assert expected in service, (
        "the host-side runner scrapes the containerized engine through this published port; "
        "it must match the port the runner tells the engine to bind, and stay on loopback")


# --------------------------------------------------------------------------
# the default is unchanged
# --------------------------------------------------------------------------


def _stub_run_benchmark(monkeypatch, process_once):
    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path", lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config", lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint", lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.engine, "process_once", process_once)
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 4, "bytes": 4096})


def _committed_once():
    outcome = [{
        "batch_id": "b1", "committed": True, "final_state": "source_committed",
        "object_uri": "s3://telemetry-jsonl/b1",
        "metrics": {"compressed_bytes": 10, "uncompressed_bytes": 20,
                    "total_ms": 5, "object_upload_ms": 2},
    }]
    calls = {"n": 0}

    def _once(*a, **k):
        calls["n"] += 1
        return (0, outcome, "") if calls["n"] == 1 else (0, [], "")

    return _once, calls


@pytest.mark.parametrize("mode_line", ["", "engine_mode: process-once\n"])
def test_a_scenario_naming_no_engine_mode_writes_todays_results_byte_for_byte(
        tmp_path, monkeypatch, mode_line):
    once, calls = _committed_once()
    _stub_run_benchmark(monkeypatch, once)
    scenario = tmp_path / "default.yaml"
    scenario.write_text("name: default-mode\nbackend: mock\nvolume: 4\n"
                        "sys_sample_interval: 0.1\n" + mode_line, encoding="utf-8")
    results = tmp_path / "results"
    monkeypatch.setattr(sys, "argv", ["run_benchmark.py", str(scenario), "--results-dir", str(results)])
    assert run_benchmark.main() == 0
    assert calls["n"] >= 1, "the default must still drive one process-once per cycle"
    (run_dir,) = glob.glob(str(results / "*"))
    assert sorted(os.listdir(run_dir)) == sorted([*CSV_HEADERS, "summary.json", "summary.md"]), (
        "a per-cycle run must write exactly the files it always has — no engine_metrics.csv, "
        "no engine-run.log")
    for name, header in CSV_HEADERS.items():
        with open(os.path.join(run_dir, name), newline="", encoding="utf-8") as fh:
            first = fh.readline()
        assert first == ",".join(header) + "\r\n", (
            f"{name}'s header must be byte-identical to CSV_HEADERS; a column added to the "
            "default run changes every recorded scenario's result shape")
    with open(os.path.join(run_dir, "summary.json"), encoding="utf-8") as fh:
        summary = json.load(fh)
    assert not [k for k in summary if k.startswith("engine_")], (
        "the long-lived columns must not appear on a per-cycle run")
    assert summary["p95_latency_ms"] == 5, "per-cycle latency columns keep their measured values"
    with open(os.path.join(run_dir, "summary.md"), encoding="utf-8") as fh:
        assert "Engine mode" not in fh.read()


# --------------------------------------------------------------------------
# a long-lived run is observed
# --------------------------------------------------------------------------


def test_a_long_lived_run_records_timestamped_engine_samples_over_the_configured_window(
        tmp_path, monkeypatch, fake_engine):
    pidfile = str(tmp_path / "engine.pid")
    monkeypatch.setenv("FAKE_ENGINE_PIDFILE", pidfile)
    monkeypatch.setenv("FAKE_ENGINE_MODE", "ok")
    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path", lambda *a, **k: fake_engine)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint", lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 4, "bytes": 4096})

    def _no_cycles(*a, **k):
        raise AssertionError("a long-lived run must not fall back to per-cycle process-once")

    monkeypatch.setattr(run_benchmark.engine, "process_once", _no_cycles)
    scenario = tmp_path / "long.yaml"
    scenario.write_text("name: long-lived\nbackend: mock\nvolume: 4\nduration_seconds: 1\n"
                        "engine_mode: run\nengine_scrape_interval_seconds: 0.2\n"
                        "sys_sample_interval: 0.1\n", encoding="utf-8")
    results = tmp_path / "results"
    monkeypatch.setattr(sys, "argv", ["run_benchmark.py", str(scenario), "--results-dir", str(results)])
    assert run_benchmark.main() == 0
    (run_dir,) = glob.glob(str(results / "*"))

    with open(os.path.join(run_dir, engine_run.ENGINE_METRICS_CSV), newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    stamps = sorted({r["timestamp"] for r in rows})
    assert len(stamps) > 1, "a long-lived run must produce more than one sample"
    assert all(r["timestamp"] for r in rows), "every row carries its sample's timestamp"
    commits = [int(r["value"]) for r in rows if r["metric"] == "vtop_commits_total"]
    assert len(commits) == len(stamps) and commits == sorted(commits) and commits[-1] > commits[0], (
        "each sample must carry the engine's own counter value, read from its endpoint")

    with open(os.path.join(run_dir, "summary.json"), encoding="utf-8") as fh:
        summary = json.load(fh)
    # The window runs from launch (its counters' zero); the scheduled part
    # after the opening row is the configured duration in whole intervals.
    start_span = float(rows[0]["interval_seconds"])
    scheduled = summary["engine_window_seconds"] - start_span
    assert 1.0 - 0.01 <= scheduled <= 1.0 + 0.2 + 0.25, (
        "the window after the engine first answered must match the configured duration, rounded "
        f"up to whole intervals — got {scheduled} of {summary['engine_window_seconds']}")
    assert summary["engine_samples"] == len(stamps)
    assert summary["engine_stopped_by"] == "sigint", "a cooperative engine gets the graceful stop"
    assert summary["successful_batches"] == int(summary["engine_commits_total_window"]) > 0, (
        "the run's batch count comes from the engine's counter inside the window")
    assert summary["p95_latency_ms"] == "", (
        "a long-lived engine prints no per-batch outcomes, so a percentile is unknown, not 0.0")
    with open(os.path.join(run_dir, "metrics.csv"), newline="", encoding="utf-8") as fh:
        header = next(csv.reader(fh))
    assert header == CSV_HEADERS["metrics.csv"] + engine_run.summary_column_names(), (
        "the engine columns are appended to metrics.csv, never interleaved")
    assert not _alive(_read_pid(pidfile)), "the engine must be stopped when the window closes"


def test_work_the_engine_finished_before_its_first_answer_is_counted_in_the_window(
        tmp_path, stub_factory):
    # The engine committed four batches between launch and the first scrape
    # that got an answer, and nothing after: every sample reads the same four.
    stub = stub_factory(body=lambda served: _body(commits=4, bytes_out=4000))
    proc = FakeProcess(on_start=stub.arm)
    window = engine_run.measure_window(
        "vtopctl", "/c.yaml", _scenario(engine_mode="run"), str(tmp_path),
        duration=0.3, interval=0.1, endpoint=stub.endpoint, engine_process=proc)
    assert window.run_totals()["success"] == 4, (
        "a fresh engine's counters start at zero, so batches it committed before its endpoint "
        "first answered are this run's work; baselining on the first answer discards them, and a "
        "small run that finished early is refused as having measured nothing")
    assert window.summary_columns()["engine_bytes_out_total_window"] == "4000"
    with open(window.csv_path, newline="", encoding="utf-8") as fh:
        opening = [r for r in csv.DictReader(fh) if r["metric"] == "vtop_commits_total"][0]
    assert opening["delta"] == "4", "the opening row carries the increase since launch"
    assert opening["delta_per_second"] == "", (
        "the launch-to-first-answer span is not a scheduled bucket; a rate over it would be a "
        "per-second number over whatever length the start happened to take")
    assert window.rollups["vtop_commits_total"].max_rate == 0.0, (
        "the per-interval extreme reads only whole scheduled buckets, none of which saw a commit")


def test_the_window_is_measured_from_launch_because_that_is_where_its_counters_start(
        tmp_path, stub_factory):
    def slow_start():
        time.sleep(0.3)
        stub.arm()

    stub = stub_factory()
    proc = FakeProcess(on_start=slow_start)
    window = engine_run.measure_window(
        "vtopctl", "/c.yaml", _scenario(engine_mode="run"), str(tmp_path),
        duration=0.2, interval=0.1, endpoint=stub.endpoint, engine_process=proc)
    assert window.window_seconds >= 0.3 + 0.2, (
        "engine_window_seconds is the span the _window counters cover, and they cover everything "
        f"since launch — a window that omits the start overstates every rate: {window.window_seconds}")
    with open(window.csv_path, newline="", encoding="utf-8") as fh:
        opening = next(csv.DictReader(fh))
    assert float(opening["interval_seconds"]) >= 0.3 and float(opening["elapsed_seconds"]) >= 0.3, (
        "the opening row states how long the engine ran before it answered")


def test_a_small_run_that_finishes_before_the_first_scrape_is_not_refused_as_empty(
        tmp_path, monkeypatch, fake_engine):
    monkeypatch.setenv("FAKE_ENGINE_PIDFILE", str(tmp_path / "engine.pid"))
    monkeypatch.setenv("FAKE_ENGINE_MODE", "finished:3")
    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path", lambda *a, **k: fake_engine)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint", lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 4, "bytes": 4096})
    monkeypatch.setattr(run_benchmark.tempfile, "tempdir", str(tmp_path))
    scenario = tmp_path / "early.yaml"
    scenario.write_text("name: finished-early\nbackend: mock\nvolume: 4\nduration_seconds: 0.4\n"
                        "engine_mode: run\nengine_scrape_interval_seconds: 0.2\n"
                        "sys_sample_interval: 0.1\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["run_benchmark.py", str(scenario),
                                      "--results-dir", str(tmp_path / "results")])
    assert run_benchmark.main() == 0, (
        "the seed lands before launch, so a small mock or localfs run can commit everything before "
        "the first scrape; that run measured three batches, not nothing")
    (run_dir,) = glob.glob(str(tmp_path / "results" / "*"))
    with open(os.path.join(run_dir, "summary.json"), encoding="utf-8") as fh:
        assert json.load(fh)["successful_batches"] == 3


# --------------------------------------------------------------------------
# per-second buckets
# --------------------------------------------------------------------------


def test_counters_carry_per_interval_deltas_and_per_second_rates_and_gauges_do_not(tmp_path):
    path = str(tmp_path / "series.csv")
    rec = engine_run.SeriesRecorder(path)
    rec.add("t0", 10.0, prom_text.parse(_body(commits=100, bytes_out=5000, width=8)))
    rec.add("t1", 10.5, prom_text.parse(_body(commits=150, bytes_out=5000, width=4)))
    # A labelled counter first exposed mid-window started at zero in that
    # interval — the engine creates its labelled counters on first increment.
    late = _body(commits=150, bytes_out=7000, width=6) + (
        "# TYPE vtop_upload_throttled_total counter\n"
        'vtop_upload_throttled_total{stage="object_upload"} 3\n')
    rec.add("t2", 11.5, prom_text.parse(late))
    rec.close()
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    def row(stamp, metric):
        (r,) = [r for r in rows if r["timestamp"] == stamp and r["metric"] == metric]
        return r

    assert row("t0", "vtop_commits_total")["delta"] == "", "the opening sample has no interval"
    assert row("t1", "vtop_commits_total")["delta"] == "50"
    assert float(row("t1", "vtop_commits_total")["delta_per_second"]) == 100.0, (
        "50 commits in half a second is a rate of 100/s: the bucket must be per second, not per sample")
    assert float(row("t2", "vtop_bytes_out_total")["delta_per_second"]) == 2000.0
    assert row("t2", "vtop_upload_throttled_total")["delta"] == "3"
    assert row("t1", "vtop_upload_width")["delta"] == "" and row("t1", "vtop_upload_width")["type"] == "gauge"

    roll = rec.rollups
    assert roll["vtop_bytes_out_total"].window == 2000 and roll["vtop_bytes_out_total"].max_rate == 2000.0
    assert roll["vtop_commits_total"].max_rate == 100.0, (
        "the max one-second rate is what answers 'was the cap ever exceeded in a second'")
    assert (roll["vtop_upload_width"].low, roll["vtop_upload_width"].high) == (4, 8)


def test_a_counter_that_goes_backwards_refuses_the_series(tmp_path):
    rec = engine_run.SeriesRecorder(str(tmp_path / "s.csv"))
    rec.add("t0", 0.0, prom_text.parse(_body(commits=100, bytes_out=1)))
    with pytest.raises(engine_run.SeriesInconsistent, match="went backwards"):
        rec.add("t1", 1.0, prom_text.parse(_body(commits=2, bytes_out=1)))
    rec.close()


def test_a_summary_series_of_the_wrong_kind_refuses_the_series(tmp_path):
    rec = engine_run.SeriesRecorder(str(tmp_path / "s.csv"))
    wrong = "# TYPE vtop_upload_width counter\nvtop_upload_width 8\n"
    with pytest.raises(engine_run.SeriesInconsistent, match="vtop_upload_width"):
        rec.add("t0", 0.0, prom_text.parse(wrong))
    rec.close()


def test_every_metrics_column_is_filled_and_a_never_exposed_counter_is_blank_not_zero(tmp_path):
    rec = engine_run.SeriesRecorder(str(tmp_path / "s.csv"))
    rec.add("t0", 0.0, prom_text.parse(_body(commits=0, bytes_out=0)))
    rec.add("t1", 1.0, prom_text.parse(_body(commits=5, bytes_out=10)))
    rec.close()
    window = engine_run.EngineWindow(
        endpoint=engine_run.MetricsEndpoint("127.0.0.1:1", "127.0.0.1", 1), interval=1.0,
        opened_at="t0", closed_at="t1", window_seconds=1.0, samples=2,
        csv_path="", log_path="", rollups=rec.rollups)
    cols = window.summary_columns()
    assert set(engine_run.summary_column_names()) <= set(cols), (
        "metrics.csv's header derives from SUMMARY_SERIES, so the summary must fill every one")
    assert cols["engine_commits_total_window"] == "5"
    assert cols["engine_upload_throttled_total_window"] == "", (
        "a family the endpoint never exposed is 'never incremented or not in this binary' — unknown")


# --------------------------------------------------------------------------
# a silent scrape failure fails the run
# --------------------------------------------------------------------------


def test_an_endpoint_that_stops_answering_mid_window_refuses_rather_than_reporting_the_samples(
        tmp_path, stub_factory):
    stub = stub_factory(answers=3)
    proc = FakeProcess(on_start=stub.arm)
    with pytest.raises(engine_run.ScrapeStopped, match=f"127.0.0.1:{stub.port}"):
        engine_run.measure_window(
            "vtopctl", "/c.yaml", _scenario(engine_mode="run"), str(tmp_path),
            duration=2.0, interval=0.1, endpoint=stub.endpoint, engine_process=proc)
    assert stub.served == 3
    assert not os.path.exists(tmp_path / engine_run.ENGINE_METRICS_CSV), (
        "the three samples it did get must not be reported as a measurement")
    assert os.path.exists(tmp_path / engine_run.ENGINE_METRICS_PARTIAL_CSV), (
        "the partial series is kept as diagnosis, under a name no reader mistakes for a result")
    assert proc.stopped, "a refused window still stops its engine"


def test_an_endpoint_that_never_answers_refuses_naming_the_address_it_tried(tmp_path):
    port = _closed_port()
    endpoint = engine_run.MetricsEndpoint(bind=f"127.0.0.1:{port}", host="127.0.0.1", port=port)
    proc = FakeProcess()
    with pytest.raises(engine_run.EndpointNeverAnswered) as err:
        engine_run.measure_window(
            "vtopctl", "/c.yaml", _scenario(engine_mode="run"), str(tmp_path),
            duration=1.0, interval=0.1, endpoint=endpoint, engine_process=proc, ready_timeout=0.3)
    assert f"http://127.0.0.1:{port}/metrics" in str(err.value), (
        "the refusal must name the address it tried, or nobody can tell a wrong port from a dead engine")
    assert proc.stopped


def test_something_already_answering_before_launch_refuses_without_starting_an_engine(
        tmp_path, stub_factory):
    stub = stub_factory()
    stub.arm()  # answering before this run launched anything
    proc = FakeProcess()
    with pytest.raises(engine_run.EndpointAlreadyAnswering, match="leaked engine"):
        engine_run.measure_window(
            "vtopctl", "/c.yaml", _scenario(engine_mode="run"), str(tmp_path),
            duration=1.0, interval=0.1, endpoint=stub.endpoint, engine_process=proc)
    assert not proc.started, (
        "a leaked engine at the address would be scraped in this run's place; refuse before launching")


def test_an_engine_that_exits_before_the_window_closes_refuses_the_run(
        tmp_path, monkeypatch, fake_engine):
    sc, endpoint, proc, pidfile = _host_engine(fake_engine, tmp_path, monkeypatch, mode="exit-after:0.5")
    with pytest.raises(engine_run.EngineExitedEarly, match="status 3"):
        engine_run.measure_window(fake_engine, "/c.yaml", sc, str(tmp_path), duration=3.0,
                                  interval=0.1, endpoint=endpoint, engine_process=proc)
    assert not os.path.exists(tmp_path / engine_run.ENGINE_METRICS_CSV)


def test_a_refused_long_lived_run_exits_nonzero_through_the_runner(tmp_path, monkeypatch):
    def _refuse(*a, **k):
        raise engine_run.ScrapeStopped("scraping stopped")

    _stub_run_benchmark(monkeypatch, lambda *a, **k: (0, [], ""))
    monkeypatch.setattr(run_benchmark.engine_run, "measure_window", _refuse)
    # Keep this run's mkdtemp directories inside the test's own tmp_path, in
    # case a regression leaks them past the refusal's cleanup.
    monkeypatch.setattr(run_benchmark.tempfile, "tempdir", str(tmp_path))
    scenario = tmp_path / "long.yaml"
    scenario.write_text("name: refused\nbackend: mock\nvolume: 4\nduration_seconds: 1\n"
                        "engine_mode: run\nseed_concurrently: true\nseed_interval_seconds: 0.05\n"
                        "sys_sample_interval: 0.1\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["run_benchmark.py", str(scenario),
                                      "--results-dir", str(tmp_path / "results")])
    with pytest.raises(engine_run.EngineWindowError):
        run_benchmark.main()
    (run_dir,) = glob.glob(str(tmp_path / "results" / "*"))
    assert not os.path.exists(os.path.join(run_dir, "summary.json")), (
        "a refused window must not leave a summary a matrix would read")
    assert not [t for t in threading.enumerate() if t.name == "seeder"], (
        "the concurrent seeder must be stopped on the refusal path too")


# --------------------------------------------------------------------------
# the engine is not left running — host mode, a real process
# --------------------------------------------------------------------------


def test_the_host_engine_is_terminated_when_the_window_fails(tmp_path, monkeypatch, fake_engine):
    sc, endpoint, proc, pidfile = _host_engine(fake_engine, tmp_path, monkeypatch,
                                               mode="stop-answering-after:3")
    with pytest.raises(engine_run.ScrapeStopped):
        engine_run.measure_window(fake_engine, "/c.yaml", sc, str(tmp_path), duration=5.0,
                                  interval=0.1, endpoint=endpoint, engine_process=proc)
    assert not _alive(_read_pid(pidfile)), (
        "a leaked engine holds the shaped link and corrupts the next scenario in a matrix")


def test_the_host_engine_is_terminated_on_a_keyboard_interrupt(tmp_path, monkeypatch, fake_engine):
    sc, endpoint, proc, pidfile = _host_engine(fake_engine, tmp_path, monkeypatch)
    window_ticks = {"n": 0}

    def interrupting_sleep(seconds):
        # Interrupt INSIDE the window — after the engine answered and a few
        # samples were taken — which is where a ctrl-c during a soak lands.
        if os.path.exists(tmp_path / engine_run.ENGINE_METRICS_PARTIAL_CSV) and seconds > \
                engine_run.READY_POLL_SECONDS:
            window_ticks["n"] += 1
            if window_ticks["n"] >= 3:
                raise KeyboardInterrupt
        time.sleep(seconds)

    with pytest.raises(KeyboardInterrupt):
        engine_run.measure_window(fake_engine, "/c.yaml", sc, str(tmp_path), duration=5.0,
                                  interval=0.1, endpoint=endpoint, engine_process=proc,
                                  sleep=interrupting_sleep)
    assert not _alive(_read_pid(pidfile)), "ctrl-c must stop the engine, not orphan it"


def test_a_host_engine_that_ignores_the_graceful_stop_is_killed(tmp_path, monkeypatch, fake_engine):
    sc, endpoint, proc, pidfile = _host_engine(fake_engine, tmp_path, monkeypatch, mode="ignore-sigint")
    window = engine_run.measure_window(fake_engine, "/c.yaml", sc, str(tmp_path), duration=0.3,
                                       interval=0.1, endpoint=endpoint, engine_process=proc,
                                       stop_grace=0.3)
    assert window.samples >= 2
    assert not _alive(_read_pid(pidfile)), "SIGINT ignored must escalate to SIGKILL"
    assert window.stopped_by == "sigkill", (
        "a forced stop leaves in-flight batches for the recovery pass; the run must record it")


# --------------------------------------------------------------------------
# the engine is not left running — container mode
# --------------------------------------------------------------------------


class FakeClient:
    """The `docker compose exec` client: alive until terminated or killed."""

    def __init__(self, argv, **kwargs) -> None:
        self.argv = argv
        self.returncode = None
        self.terminated = False

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = -signal.SIGTERM

    def kill(self):
        self.returncode = -signal.SIGKILL

    def wait(self, timeout=None):
        return self.returncode


class FakeDocker:
    def __init__(self, stop_rc: int = 0, on_exec=None) -> None:
        self.clients: list[FakeClient] = []
        self.runs: list[list[str]] = []
        self.stop_rc = stop_rc
        self.on_exec = on_exec

    def popen(self, argv, **kwargs):
        client = FakeClient(argv, **kwargs)
        self.clients.append(client)
        if self.on_exec is not None:
            self.on_exec()
        return client

    def run(self, argv, **kwargs):
        self.runs.append(argv)
        return subprocess.CompletedProcess(
            argv, self.stop_rc, stdout="still running: 42" if self.stop_rc else "stopped-by:INT\n", stderr="")

    def stops_inside(self, token: str) -> list[list[str]]:
        return [argv for argv in self.runs
                if engine.CONTAINER_SERVICE in argv and "sh" in argv and token in argv
                and argv[argv.index("sh") + 2] == engine_run._CONTAINER_STOP_SCRIPT]


def _container_engine(docker: FakeDocker, token: str):
    sc = _scenario(runner_mode="container", engine_mode="run", duration_seconds=1, backend="mock")
    argv, env = engine.invocation("vtopctl", ["run", "--config", token], sc,
                                  extra_env={"VTOP_METRICS_ADDR": "0.0.0.0:9464"})
    return sc, engine_run.ContainerEngine(argv, env, token=token, popen=docker.popen, run=docker.run)


def test_the_launcher_picks_the_container_engine_and_names_this_runs_config_as_its_token():
    sc = _scenario(runner_mode="container", engine_mode="run", duration_seconds=1, backend="mock")
    endpoint = engine_run.endpoint_for("container")
    proc = engine_run.launcher("/host/vtopctl", "/run/root/_engine.yaml", sc, endpoint)
    assert isinstance(proc, engine_run.ContainerEngine)
    assert proc.token == "/run/root/_engine.yaml"
    assert endpoint.bind == f"0.0.0.0:{engine_run.CONTAINER_METRICS_PORT}", (
        "inside the container the engine must listen on its interface, not its own loopback")


def test_the_container_engine_is_stopped_inside_the_container_when_the_window_fails(
        tmp_path, stub_factory):
    stub = stub_factory(answers=2)
    docker = FakeDocker(on_exec=stub.arm)
    token = str(tmp_path / "_engine.yaml")
    sc, proc = _container_engine(docker, token)
    with pytest.raises(engine_run.ScrapeStopped):
        engine_run.measure_window("vtopctl", token, sc, str(tmp_path), duration=2.0, interval=0.1,
                                  endpoint=stub.endpoint, engine_process=proc)
    assert docker.stops_inside(token), (
        "killing the docker exec client does not stop the exec'd engine; the stop must run "
        "inside the container, matched on this run's config")
    assert docker.clients[0].terminated, "and the client itself is reaped"


def test_the_container_engine_is_stopped_inside_the_container_on_a_keyboard_interrupt(
        tmp_path, stub_factory):
    stub = stub_factory()
    docker = FakeDocker(on_exec=stub.arm)
    token = str(tmp_path / "_engine.yaml")
    sc, proc = _container_engine(docker, token)
    ticks = {"n": 0}

    def interrupting_sleep(seconds):
        ticks["n"] += 1
        if ticks["n"] >= 3:
            raise KeyboardInterrupt
        time.sleep(seconds)

    with pytest.raises(KeyboardInterrupt):
        engine_run.measure_window("vtopctl", token, sc, str(tmp_path), duration=2.0, interval=0.1,
                                  endpoint=stub.endpoint, engine_process=proc,
                                  sleep=interrupting_sleep)
    assert docker.stops_inside(token), "ctrl-c must stop the containerized engine inside the container"
    assert docker.clients[0].terminated


def test_a_container_engine_that_survives_the_stop_is_reported_not_assumed_gone(tmp_path):
    docker = FakeDocker(stop_rc=1)
    token = str(tmp_path / "_engine.yaml")
    _, proc = _container_engine(docker, token)
    with open(tmp_path / "log", "w") as log:
        proc.start(log)
    with pytest.raises(engine_run.EngineNotStopped, match="still running: 42"):
        proc.stop(grace=1)
    assert len(docker.stops_inside(token)) == 2, "a failed stop gets one immediate hard kill"
    assert docker.clients[0].terminated


def _stand_in_vtopctl(tmp_path) -> str:
    """A shell binary COPIED under the name vtopctl, so its comm and argv[0]
    are the engine's — the two things the stop script matches before the
    config token."""
    shell = os.path.realpath("/bin/sh")
    path = tmp_path / "bin" / "vtopctl"
    path.parent.mkdir()
    path.write_bytes(open(shell, "rb").read())
    path.chmod(0o755)
    try:
        subprocess.run([str(path), "-c", ":"], check=True, timeout=10)
    except PermissionError:
        pytest.fail(f"cannot execute files under {path.parent} (a noexec temp mount?); "
                    "run pytest with --basetemp on an exec-able filesystem")
    return str(path)


def _start_stand_in(binary: str, config: str, graceful: bool = True) -> subprocess.Popen:
    # `wait` on a background sleep, so a trapped SIGINT is acted on at once —
    # or, for the stubborn one, SIGINT ignored outright. The argv after the
    # script mirrors `vtopctl run --config <path>`.
    if graceful:
        script = "sleep 30 & child=$!; trap 'kill $child; exit 0' INT; wait; kill $child"
    else:
        script = "trap '' INT; sleep 30 & wait"
    # Its own group, so cleanup can take the background sleep with it.
    return subprocess.Popen(["vtopctl", "-c", script, "run", "--config", config], executable=binary,
                            start_new_session=True)


def _reap_group(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


def _stop_script(token: str, grace: int) -> subprocess.CompletedProcess:
    return subprocess.run(["/bin/sh", "-c", engine_run._CONTAINER_STOP_SCRIPT, "vtop-bench-stop", token,
                           str(grace)], capture_output=True, text=True, timeout=60)


def test_the_in_container_stop_script_stops_this_runs_engine_and_no_other(tmp_path):
    """The stop script's matching, run against real processes on this host.

    Only the stand-in naming THIS run's config may be stopped: a neighbouring
    run's engine is not this run's to kill. /proc is Linux; so is the lab.
    """
    if not os.path.isdir("/proc/self"):
        pytest.skip("the stop script reads /proc")
    binary = _stand_in_vtopctl(tmp_path)
    token = str(tmp_path / "ours" / "_engine.yaml")
    ours = _start_stand_in(binary, token)
    theirs = _start_stand_in(binary, str(tmp_path / "theirs" / "_engine.yaml"))
    try:
        time.sleep(0.3)  # let both install their traps
        result = _stop_script(token, grace=5)
        assert result.returncode == 0, result.stdout + result.stderr
        assert result.stdout.strip() == "stopped-by:INT", "a graceful stop must be reported as one"
        assert ours.wait(timeout=10) == 0, "this run's engine must get the graceful SIGINT stop"
        assert theirs.poll() is None, "another run's engine must be left alone"
    finally:
        for p in (ours, theirs):
            _reap_group(p)


def test_the_in_container_stop_script_escalates_to_sigkill_when_sigint_is_ignored(tmp_path):
    if not os.path.isdir("/proc/self"):
        pytest.skip("the stop script reads /proc")
    binary = _stand_in_vtopctl(tmp_path)
    token = str(tmp_path / "ours" / "_engine.yaml")
    stubborn = _start_stand_in(binary, token, graceful=False)
    try:
        time.sleep(0.3)
        started = time.monotonic()
        result = _stop_script(token, grace=1)
        assert result.returncode == 0, result.stdout + result.stderr
        assert result.stdout.strip() == "stopped-by:KILL", (
            "a forced stop leaves in-flight batches for the recovery pass; it must say so")
        assert stubborn.wait(timeout=10) == -signal.SIGKILL, (
            "an engine that ignores the graceful stop must still be stopped")
        assert time.monotonic() - started < 10, "the grace is seconds of wall clock, not scan iterations"
    finally:
        _reap_group(stubborn)


# --------------------------------------------------------------------------
# the comparison matrix names the engine mode, and never mixes two
# --------------------------------------------------------------------------


def _write_matrix_scenarios(tmp_path, **bodies) -> dict:
    files = {}
    for name, body in bodies.items():
        path = tmp_path / f"{name}.yaml"
        path.write_text(body, encoding="utf-8")
        files[name] = str(path)
    return files


def _long_lived_summary(name: str) -> dict:
    return {"scenario_name": name, "engine_mode": "run", "engine_window_seconds": 10.0,
            "engine_metrics_address": "http://127.0.0.1:41234/metrics",
            "engine_commits_total_window": "12", "engine_upload_width_max": "8",
            "scenario": {"engine_mode": "run", "engine_scrape_interval_seconds": 1.0}}


def test_a_row_without_an_engine_mode_reads_as_the_per_cycle_run_it_was():
    import run_matrix

    before_510 = run_matrix.matrix_row({"scenario_name": "01", "scenario": {"name": "01"}})
    assert before_510["engine_mode"] == engine_run.PROCESS_ONCE, (
        "a summary written before #510 has no engine_mode, and every such run was one "
        "process-once per cycle")
    # A per-cycle summary never carries the key either (its results are
    # byte-identical to before #510) even though today's loader puts the
    # default in its scenario; and what the run DID beats what it asked for.
    per_cycle = run_matrix.matrix_row({"scenario_name": "02", "scenario": {
        "engine_mode": "run", "engine_scrape_interval_seconds": 1.0}})
    assert per_cycle["engine_mode"] == engine_run.PROCESS_ONCE, (
        "a summary with no engine columns was produced by the per-cycle loop, whatever its "
        "scenario requested")
    assert per_cycle["engine_scrape_interval_seconds"] == "", (
        "a scenario knob no engine applied must not show in the table as if it had been")
    run_matrix.refuse_incomparable([before_510, per_cycle])
    assert run_matrix.matrix_row(_long_lived_summary("03"))["engine_mode"] == engine_run.LONG_LIVED


def test_a_matrix_whose_rows_mix_engine_modes_is_refused_before_the_rest_run(tmp_path):
    import run_matrix

    files = _write_matrix_scenarios(
        tmp_path, once1="name: once1\n", once2="name: once2\n",
        long="name: long\nengine_mode: run\nduration_seconds: 10\n", once3="name: once3\n")
    rows = {"once1": run_matrix.matrix_row({"scenario_name": "once1", "scenario": {}}),
            "once2": run_matrix.matrix_row({"scenario_name": "once2", "scenario": {}}),
            "once3": run_matrix.matrix_row({"scenario_name": "once3", "scenario": {}}),
            "long": run_matrix.matrix_row(_long_lived_summary("long"))}
    calls = []

    def runs(path):
        name = os.path.splitext(os.path.basename(path))[0]
        calls.append(name)
        return rows[name]

    with pytest.raises(run_matrix.IncomparableRuns) as exc:
        run_matrix.collect_rows([files[n] for n in ("once1", "once2", "long", "once3")], runs)
    assert calls == ["once1", "long"], (
        f"ran {calls}: a fresh process per cycle and one long-lived engine are different "
        "execution models, and the conflict must cost one row of each, not the matrix")
    message = str(exc.value)
    assert "process-once, run" in message and "2 were not run" in message, (
        "the refusal names both engine modes and how much of the matrix it spared")


def test_a_long_lived_matrix_carries_the_engine_columns_and_a_per_cycle_one_only_its_mode():
    import run_matrix

    assert "engine_mode" in run_matrix.COMPARE_COLS, (
        "a number read without knowing which engine produced it is read against the wrong model")
    once = run_matrix.matrix_row({"scenario_name": "once", "scenario": {}})
    assert run_matrix.matrix_columns([once]) == run_matrix.COMPARE_COLS, (
        "a per-cycle matrix keeps its table: engine columns it could only ever leave blank are "
        "not added to it")
    long_cols = run_matrix.matrix_columns([run_matrix.matrix_row(_long_lived_summary("long"))])
    expected = [c for c in engine_run.summary_column_names()
                if c not in ("engine_mode", "engine_metrics_address")]
    assert long_cols == run_matrix.COMPARE_COLS + expected, (
        "a long-lived matrix carries the flat engine columns derived from SUMMARY_SERIES, so a "
        "series added there reaches the matrix too")
    assert "engine_metrics_address" not in long_cols, (
        "a fresh loopback port per run is not a condition two rows are compared on")


def test_the_matrix_writes_the_engine_columns_and_no_table_when_modes_conflict(
        tmp_path, monkeypatch):
    import run_matrix

    files = _write_matrix_scenarios(tmp_path, long1="name: long1\n", long2="name: long2\n",
                                    once="name: once\n")
    summaries = {"long1": _long_lived_summary("long1"), "long2": _long_lived_summary("long2"),
                 "once": {"scenario_name": "once", "scenario": {}}}
    ran = []

    def run_one(path, results_dir):
        name = os.path.splitext(os.path.basename(path))[0]
        ran.append(name)
        run_dir = os.path.join(results_dir, f"run-{name}")
        os.makedirs(run_dir)
        with open(os.path.join(run_dir, "summary.json"), "w", encoding="utf-8") as fh:
            json.dump(summaries[name], fh)
        return run_dir

    monkeypatch.setattr(run_matrix, "run_one", run_one)
    agreeing = tmp_path / "agreeing"
    monkeypatch.setattr(sys, "argv", ["run_matrix.py", files["long1"], files["long2"],
                                      "--results-dir", str(agreeing)])
    assert run_matrix.main() == 0
    (table,) = glob.glob(str(agreeing / "matrix-*" / "matrix.csv"))
    with open(table, newline="", encoding="utf-8") as fh:
        written = list(csv.DictReader(fh))
    assert [(r["engine_mode"], r["engine_commits_total_window"]) for r in written] == [
        ("run", "12"), ("run", "12")], "the matrix carries each row's engine mode and counters"

    conflicting = tmp_path / "conflicting"
    monkeypatch.setattr(sys, "argv", ["run_matrix.py", files["long1"], files["once"],
                                      "--results-dir", str(conflicting)])
    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.main()
    assert not glob.glob(str(conflicting / "matrix-*" / "matrix.*")), (
        "a table that exists is a table somebody reads; a mixed-mode one is never written")
