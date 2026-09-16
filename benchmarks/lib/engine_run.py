"""A long-lived engine for the measured window, with its own /metrics scraped (#510).

Every other number the harness reports comes from one `vtopctl process-once`
per cycle — a fresh process each time — or from the host through psutil. Two
things are invisible from there. A rate cap's token bucket and the adaptive
width controller both live in the engine process and reset with it, so a
series of fresh processes never shows either settling; and the engine's own
counters (bytes on the wire, throttles, width) are process-lifetime values
that nothing was reading. `psutil.net_io_counters()` cannot stand in: it is
host-wide, counts loopback and the store's own traffic, and cannot attribute a
byte to VTOP with anything else on the box.

`engine_mode: run` replaces the per-cycle loop with ONE `vtopctl run` for the
window, started with VTOP_METRICS_ADDR set, and samples its Prometheus endpoint
on a fixed interval of at most a second. The samples are written per series to
`engine_metrics.csv`, and counters carry their per-interval delta and
per-second rate — a cap that holds on average and is exceeded in one second is
a cap that does not hold, and only the buckets can say which.

The run is REFUSED, never reported partially, when:

* the endpoint never answers before the window would open (the address is
  named — a run whose first samples are connection refusals measured nothing);
* something already answers at that address before the engine starts (a leaked
  engine from an earlier run would be scraped in place of this one);
* a scrape fails, or falls behind by more than an interval, inside the window;
* a counter goes backwards or a series disappears (a different process is
  answering, or the engine restarted and its counters reset);
* the engine exits before the window closes.

And the engine is stopped on EVERY way out — a clean close, any refusal above,
a failure anywhere in between and a KeyboardInterrupt — in both runner modes. A
leaked engine keeps uploading through the shaped link and corrupts whichever
scenario runs next in a matrix.
"""
from __future__ import annotations

import csv
import http.client
import math
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from . import engine, prom_text

# --- the scenario keys --------------------------------------------------------

PROCESS_ONCE = "process-once"
LONG_LIVED = "run"
# Named after the subcommands they run, so a scenario reads as what executes.
ENGINE_MODES = (PROCESS_ONCE, LONG_LIVED)

DEFAULT_SCRAPE_INTERVAL_SECONDS = 1.0
# The ceiling is the promise the artifact makes: per-SECOND buckets. A coarser
# interval would average away exactly the one-second excursion a rate-cap
# criterion is written to catch, so it is refused rather than accepted.
MAX_SCRAPE_INTERVAL_SECONDS = 1.0

# How long the endpoint gets to answer after launch. The engine starts its
# metrics server before it opens the ledger (crates/vtop-cli/src/commands.rs),
# so a healthy engine answers in milliseconds; the budget is for a container
# exec on a loaded host, not for the engine.
READY_TIMEOUT_SECONDS = 30.0
# How often readiness is polled. Tight so the opening row lands close to
# launch, though no work is lost to a slow answer any more: the counters are
# read against zero at launch (see `measure_window`). A refused connection
# costs microseconds.
READY_POLL_SECONDS = 0.02

# How long the graceful stop gets before SIGKILL. `vtopctl run` treats ctrl-c
# as a graceful stop — it force-flushes its buffered batches and exits — but
# only a signal that arrives while its loop is parked in `select!` is seen: the
# ctrl_c future is created inside each iteration, so a SIGINT delivered during
# a busy cycle is lost. Measured against the v0.6.0 release binary under a
# concurrent seeder: no exit 15 s after one SIGINT, nor after 75 SIGINTs 0.2 s
# apart, nor 24 s after the load stopped. So SIGINT is re-sent through the
# grace (an engine that goes idle catches one), the grace is short, and HOW the
# engine stopped is recorded (`engine_stopped_by`): a SIGKILLed engine leaves
# in-flight batches that the recovery pass afterwards will find. The stop
# happens after the window closed either way, so it is never in the series.
STOP_GRACE_SECONDS = 10.0
STOP_RESEND_SECONDS = 0.5

# The container's side of the endpoint. The compose file publishes this port
# on the host's loopback (VTOP_BENCH_METRICS_PORT picks the host side), which
# is how the host-side runner reaches it — see `endpoint_for`.
CONTAINER_METRICS_PORT = 9464
# The engine's own opt-in (crates/vtop-observe, ADDR_ENV): nothing listens
# unless it is set, which is why the per-cycle mode never opened a port.
METRICS_ADDR_ENV = "VTOP_METRICS_ADDR"
METRICS_PORT_ENV = "VTOP_BENCH_METRICS_PORT"

ENGINE_METRICS_CSV = "engine_metrics.csv"
# Where samples stream while the window is open. Renamed to ENGINE_METRICS_CSV
# only when the window closed cleanly and the engine is stopped, so a refused
# run keeps its raw evidence for diagnosis under a name no reader mistakes for
# a measurement.
ENGINE_METRICS_PARTIAL_CSV = "engine_metrics.partial.csv"
ENGINE_LOG = "engine-run.log"

ENGINE_METRICS_COLUMNS = (
    "timestamp", "elapsed_seconds", "interval_seconds", "metric", "labels",
    "type", "value", "delta", "delta_per_second",
)

# THE SERIES SURFACED AS FLAT SUMMARY COLUMNS, and what each one is.
#
# Adding a column for a new engine series is one line here: the header of
# metrics.csv, the summary.json keys and the README's column list all derive
# from this tuple. A counter gets `engine_<name>_window` (its increase inside
# the window) and `engine_<name>_max_per_second` (the largest one-interval
# rate); a gauge gets `engine_<name>_min` and `engine_<name>_max`. Every
# series is summed across its label sets. The declared kind is checked against
# the endpoint's own `# TYPE` line, and a mismatch refuses the run: a gauge
# differenced as a counter is a rate of nothing.
#
# `vtop_upload_egress_bytes_total` (#481's rate-cap counter) does not exist yet;
# when it lands, it is one more ("vtop_upload_egress_bytes_total", "counter").
SUMMARY_SERIES: tuple[tuple[str, str], ...] = (
    ("vtop_commits_total", "counter"),
    ("vtop_failed_total", "counter"),
    ("vtop_records_total", "counter"),
    ("vtop_bytes_in_total", "counter"),
    ("vtop_bytes_out_total", "counter"),
    ("vtop_upload_throttled_total", "counter"),
    ("vtop_inflight_batches", "gauge"),
    ("vtop_upload_width", "gauge"),
)

# Summary columns computed only from per-cycle `process-once` outcomes. A
# long-lived engine prints no outcomes, so in that mode these are unknown —
# blank, not the 0.0 an empty percentile list would otherwise produce.
PER_CYCLE_ONLY_COLUMNS = (
    "avg_latency_ms", "avg_batch_duration_ms", "p50_latency_ms",
    "p95_latency_ms", "p99_latency_ms", "upload_p50_ms", "upload_p95_ms",
    "compression_ratio_avg",
)


class EngineWindowError(RuntimeError):
    """A long-lived window that did not produce a whole measurement."""


class EndpointNeverAnswered(EngineWindowError):
    pass


class EndpointAlreadyAnswering(EngineWindowError):
    pass


class ScrapeStopped(EngineWindowError):
    pass


class EngineExitedEarly(EngineWindowError):
    pass


class SeriesInconsistent(EngineWindowError):
    pass


class EngineNotStopped(EngineWindowError):
    pass


def engine_mode(scenario) -> str:
    """The scenario's engine mode, validated before anything is launched.

    Refused rather than defaulted, exactly like `runner_mode`: a typo that fell
    back to `process-once` would measure the thing the scenario was written to
    get away from and file it under the scenario's name.
    """
    raw = scenario.get("engine_mode", PROCESS_ONCE)
    if raw is None or raw == "":
        raw = PROCESS_ONCE
    if not isinstance(raw, str) or raw not in ENGINE_MODES:
        raise ValueError(f"engine_mode must be one of {ENGINE_MODES}, got {raw!r}")
    interval = scrape_interval(scenario)
    if raw == PROCESS_ONCE:
        # A knob that is recorded and applied by nothing is the failure this
        # harness spends most of its refusals preventing.
        if interval != DEFAULT_SCRAPE_INTERVAL_SECONDS:
            raise ValueError(
                "engine_scrape_interval_seconds applies only to engine_mode: run; "
                f"this scenario runs {PROCESS_ONCE}, which scrapes nothing")
        return raw
    try:
        duration = float(scenario.get("duration_seconds", 0) or 0)
    except (TypeError, ValueError):
        duration = 0.0
    if duration <= 0:
        raise ValueError(
            "engine_mode: run needs duration_seconds > 0: the window IS the "
            "measurement, and a long-lived engine has no drain to stop at")
    return raw


def scrape_interval(scenario) -> float:
    raw = scenario.get("engine_scrape_interval_seconds", DEFAULT_SCRAPE_INTERVAL_SECONDS)
    if raw is None or raw == "":
        return DEFAULT_SCRAPE_INTERVAL_SECONDS
    # A YAML bool is an int to Python; `true` is not an interval.
    if isinstance(raw, bool):
        raise ValueError(f"engine_scrape_interval_seconds must be a number of seconds, got {raw!r}")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ValueError(
            f"engine_scrape_interval_seconds must be a number of seconds, got {raw!r}") from None
    if not (0 < value <= MAX_SCRAPE_INTERVAL_SECONDS):
        raise ValueError(
            f"engine_scrape_interval_seconds must be in (0, {MAX_SCRAPE_INTERVAL_SECONDS}], got "
            f"{value}: the series promises per-second buckets, and a coarser interval "
            "averages away the one-second excursion it exists to show")
    return value


def _short(name: str) -> str:
    return name[len("vtop_"):] if name.startswith("vtop_") else name


def summary_column_names() -> list[str]:
    """Every flat column a long-lived run adds to metrics.csv and summary.json,
    derived from SUMMARY_SERIES so the two can never disagree."""
    cols = ["engine_mode", "engine_metrics_address", "engine_window_seconds",
            "engine_scrape_interval_seconds", "engine_samples", "engine_stopped_by"]
    for name, kind in SUMMARY_SERIES:
        if kind == "counter":
            cols += [f"engine_{_short(name)}_window", f"engine_{_short(name)}_max_per_second"]
        else:
            cols += [f"engine_{_short(name)}_min", f"engine_{_short(name)}_max"]
    return cols


def metrics_columns(mode: str) -> dict[str, list[str]]:
    """Extra columns for ResultsWriter. EMPTY for process-once, so a scenario
    that names no engine mode writes exactly the files and headers it always
    has."""
    if mode != LONG_LIVED:
        return {}
    return {"metrics.csv": summary_column_names()}


# --- where the endpoint is ----------------------------------------------------


@dataclass(frozen=True)
class MetricsEndpoint:
    bind: str   # the VTOP_METRICS_ADDR the engine is given, in its own namespace
    host: str   # where THIS process scrapes it
    port: int

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/metrics"


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _lab_setting(name: str, default: str) -> str:
    """A compose variable as the benchmark stack resolves it: a PRESENT shell
    variable wins even when blank (and blank means the default), else the value
    filed in benchmarks/.env, else the default — the channels and the order
    lib/engine.py already follows for the stack's credentials."""
    if name in os.environ:
        return os.environ[name] or default
    return engine._dotenv_overrides().get(name) or default


def endpoint_for(mode: str) -> MetricsEndpoint:
    """Where the engine listens and where the runner reads it, per runner mode.

    HOST: a fresh loopback port per run, so two runs on one machine — or a
    leaked engine from an earlier one — never share an address.

    CONTAINER: the engine listens on all of its own interfaces at a fixed port,
    and the compose file publishes that port on the host's loopback. Chosen
    over the two alternatives, both tried on paper against this lab:
    scraping the container's bridge address directly works only where the host
    can route to a bridge (not Docker Desktop, not rootless docker), and a
    `docker exec` per sample costs 100–200 ms of client startup against a
    one-second bucket, on the same CPU the engine is being measured on. The
    published port is loopback-only by default, like every other port in the
    file, and the scrape never crosses the middlebox — netem shapes the path
    between the engine and the store, not the engine's own interface.
    """
    if mode == "container":
        port = int(_lab_setting(METRICS_PORT_ENV, str(CONTAINER_METRICS_PORT)))
        bind_addr = _lab_setting("VTOP_BIND_ADDR", "127.0.0.1")
        # A wildcard bind is reachable on loopback; a specific address is
        # where the published port actually is.
        host = "127.0.0.1" if bind_addr in ("0.0.0.0", "::", "[::]") else bind_addr
        return MetricsEndpoint(bind=f"0.0.0.0:{CONTAINER_METRICS_PORT}", host=host, port=port)
    port = _free_loopback_port()
    return MetricsEndpoint(bind=f"127.0.0.1:{port}", host="127.0.0.1", port=port)


class ScrapeFailed(Exception):
    pass


def scrape(endpoint: MetricsEndpoint, timeout: float) -> prom_text.Exposition:
    """One GET of /metrics, parsed.

    http.client rather than urllib on purpose: urllib honours HTTP_PROXY, and a
    lab host with a proxy configured would send a loopback scrape through it.
    Raises ScrapeFailed for every way this is not a reading of THE ENGINE —
    including a 200 from something that exposes no `vtop_` family at all.
    """
    conn = http.client.HTTPConnection(endpoint.host, endpoint.port, timeout=timeout)
    try:
        conn.request("GET", "/metrics")
        resp = conn.getresponse()
        body = resp.read()
        if resp.status != 200:
            raise ScrapeFailed(f"HTTP {resp.status}: {body[:200]!r}")
    except (OSError, http.client.HTTPException) as err:
        raise ScrapeFailed(f"{type(err).__name__}: {err}") from err
    finally:
        conn.close()
    try:
        expo = prom_text.parse(body.decode("utf-8"))
    except (UnicodeDecodeError, prom_text.PromParseError) as err:
        raise ScrapeFailed(f"unreadable exposition: {err}") from err
    if not any(family.startswith("vtop_") for family in expo.types):
        raise ScrapeFailed("the endpoint answered but exposes no vtop_ metric family; it is not the engine")
    return expo


# --- the engine process, per runner mode ---------------------------------------


class HostEngine:
    """`vtopctl run` as a host process, in its own process group.

    Its own SESSION, so a terminal ctrl-c reaches the runner and not the
    engine: the runner's KeyboardInterrupt is then the one path that stops the
    engine, gracefully and on the runner's schedule, instead of racing it.
    """

    def __init__(self, argv: list[str], env: dict[str, str], popen=subprocess.Popen) -> None:
        self.argv = argv
        self.env = env
        self._popen = popen
        self.proc = None

    @property
    def pid(self) -> int | None:
        return self.proc.pid if self.proc is not None else None

    def start(self, log) -> None:
        self.proc = self._popen(self.argv, env=self.env, stdin=subprocess.DEVNULL,
                                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)

    def exit_code(self) -> int | None:
        return self.proc.poll() if self.proc is not None else None

    def _signal_group(self, sig: int) -> None:
        # Only while the leader is unreaped: after wait() its pid — and so the
        # group id — may belong to somebody else.
        if self.proc is None or self.proc.poll() is not None:
            return
        try:
            os.killpg(self.proc.pid, sig)
        except ProcessLookupError:
            pass

    def stop(self, grace: float) -> str:
        """Stop the engine; returns how: `sigint`, `sigkill`, or `exited` when
        it was already gone."""
        if self.proc is None or self.proc.poll() is not None:
            return "exited"
        how = "sigkill"
        try:
            deadline = time.monotonic() + grace
            while True:
                self._signal_group(signal.SIGINT)
                try:
                    self.proc.wait(timeout=max(0.0, min(STOP_RESEND_SECONDS, deadline - time.monotonic())))
                    how = "sigint"
                    break
                except subprocess.TimeoutExpired:
                    if time.monotonic() >= deadline:
                        break
        finally:
            # Runs on an expired grace AND on a second ctrl-c during the
            # graceful wait: an interrupted stop must still stop.
            if self.proc.poll() is None:
                self._signal_group(signal.SIGKILL)
                self.proc.wait()
                how = "sigkill"
        return how


# The in-container stop. Killing the `docker exec` CLIENT does not stop the
# process it started — verified against this lab's hardened ubuntu image with
# both SIGTERM and SIGKILL to the client: the exec'd process keeps running. So
# the engine is found and signalled INSIDE the container, by the one thing that
# is unique to this run: its config path as an exact argv element, on a process
# that IS vtopctl (its comm, and its argv[0]). That matches no other run's
# engine, and not this script. No pid is carried from launch, so there is no
# window in which an interrupt between launch and a pid read leaves the engine
# untracked. Only the shell, coreutils and grep are needed — the image has no
# procps — and the comm prefilter is a `read` builtin, so a scan forks only for
# processes already named vtopctl.
#
# SIGINT (the engine's graceful stop), re-sent through `grace` seconds of wall
# clock for the reason STOP_GRACE_SECONDS gives, then SIGKILL. The last line
# says how it stopped; exit 1 naming the pids when anything survives both.
_CONTAINER_STOP_SCRIPT = r"""
tok=$1
grace=$2
nl='
'
ours() {
  found=""
  for d in /proc/[0-9]*; do
    read -r comm 2>/dev/null < "$d/comm" || continue
    [ "$comm" = vtopctl ] || continue
    args=$(tr '\000' '\n' 2>/dev/null < "$d/cmdline") || continue
    first=${args%%"$nl"*}
    case "$first" in
      vtopctl|*/vtopctl) ;;
      *) continue ;;
    esac
    if printf '%s\n' "$args" | grep -qxF -e "$tok"; then
      found="$found ${d#/proc/}"
    fi
  done
  printf '%s' "$found"
}
pids=$(ours)
if [ -z "$pids" ]; then echo stopped-by:none; exit 0; fi
end=$(( $(date +%s) + grace ))
while [ "$(date +%s)" -lt "$end" ]; do
  kill -s INT $pids 2>/dev/null
  sleep 0.5
  pids=$(ours)
  if [ -z "$pids" ]; then echo stopped-by:INT; exit 0; fi
done
kill -s KILL $pids 2>/dev/null
sleep 0.5
left=$(ours)
if [ -z "$left" ]; then echo stopped-by:KILL; exit 0; fi
echo "still running:$left"
exit 1
"""


def container_exec_argv(args: list[str]) -> list[str]:
    return ["docker", "compose", "-f", engine.COMPOSE_FILE, "exec", "-T",
            engine.CONTAINER_SERVICE] + args


class ContainerEngine:
    """`vtopctl run` exec'd inside the lab's engine container."""

    def __init__(self, argv: list[str], env: dict[str, str], token: str,
                 popen=subprocess.Popen, run=subprocess.run) -> None:
        self.argv = argv
        self.env = env
        self.token = token
        self._popen = popen
        self._run = run
        self.proc = None

    def start(self, log) -> None:
        self.proc = self._popen(self.argv, env=self.env, stdin=subprocess.DEVNULL,
                                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)

    def exit_code(self) -> int | None:
        # The client exits when the exec'd process does, with its status.
        return self.proc.poll() if self.proc is not None else None

    def _stop_inside(self, grace: float) -> str:
        argv = container_exec_argv(
            ["sh", "-c", _CONTAINER_STOP_SCRIPT, "vtop-bench-stop", self.token,
             str(int(math.ceil(max(0.0, grace))))])
        try:
            result = self._run(argv, capture_output=True, text=True, timeout=grace + 60)
        except (OSError, subprocess.TimeoutExpired) as err:
            raise EngineNotStopped(
                f"could not stop the engine inside the container ({err}); it may still be "
                f"running — `docker exec vtop-bench-engine` and kill the vtopctl whose argv "
                f"names {self.token}") from err
        if result.returncode != 0:
            raise EngineNotStopped(
                "the engine inside the container survived SIGINT and SIGKILL "
                f"({(result.stdout or '').strip() or (result.stderr or '').strip() or 'no output'}); "
                "a leaked engine holds the shaped link for the next scenario")
        last = ((result.stdout or "").strip().splitlines() or [""])[-1]
        return {"stopped-by:INT": "sigint", "stopped-by:KILL": "sigkill",
                "stopped-by:none": "exited"}.get(last, "unknown")

    def _reap_client(self) -> None:
        if self.proc is None or self.proc.poll() is not None:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()

    def stop(self, grace: float) -> str:
        """Stop the engine inside the container; returns how, as HostEngine."""
        if self.proc is None:
            return "exited"
        try:
            return self._stop_inside(grace)
        except BaseException:
            # Interrupted or failed mid-stop: one immediate hard kill before
            # the original reason propagates, so a second ctrl-c does not
            # become a leaked engine.
            try:
                self._stop_inside(0)
            except BaseException as again:  # noqa: BLE001 - reported, the first reason wins
                print(f"[bench] WARNING: the containerized engine may still be running: {again}",
                      file=sys.stderr)
            raise
        finally:
            self._reap_client()


def launcher(binary: str, config_path: str, scenario, endpoint: MetricsEndpoint):
    extra = {METRICS_ADDR_ENV: endpoint.bind}
    argv, env = engine.invocation(binary, ["run", "--config", config_path], scenario, extra_env=extra)
    if engine.runner_mode(scenario) == "container":
        return ContainerEngine(argv, env, token=config_path)
    # An operator's metrics TLS settings would turn the endpoint into HTTPS
    # under a plaintext scraper, which then reads as "never answered". The
    # container is immune — only named variables cross that boundary.
    for key in ("VTOP_METRICS_TLS_CERT", "VTOP_METRICS_TLS_KEY", "VTOP_METRICS_TLS_CLIENT_CA"):
        env.pop(key, None)
    return HostEngine(argv, env)


# --- the series ---------------------------------------------------------------


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fmt(value: float) -> str:
    if math.isfinite(value) and value == int(value):
        return str(int(value))
    return repr(value)


@dataclass
class _Rollup:
    seen: bool = False
    window: float = 0.0
    max_rate: float | None = None
    low: float | None = None
    high: float | None = None


@dataclass
class SeriesRecorder:
    """Streams samples to CSV and rolls up the SUMMARY_SERIES as they arrive.

    Holds only the previous exposition, not the whole series: an hour at one
    sample a second is thousands of expositions, and the summary needs running
    sums and extremes, not history.
    """

    path: str
    rows_written: int = 0
    samples: int = 0
    first_mono: float | None = None
    last_mono: float | None = None
    first_iso: str = ""
    last_iso: str = ""
    rollups: dict[str, _Rollup] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._fh = open(self.path, "w", newline="", encoding="utf-8")
        self._csv = csv.writer(self._fh)
        self._csv.writerow(ENGINE_METRICS_COLUMNS)
        self._prev: prom_text.Exposition | None = None
        self._opening_span = False
        self.rollups = {name: _Rollup() for name, _ in SUMMARY_SERIES}

    def close(self) -> None:
        self._fh.close()

    def origin(self, iso: str, mono: float) -> None:
        """Open the series at the engine's LAUNCH, with every series at zero.

        True of a process this run just started, and only of one: the engine's
        registry is built in-process (vtop_core::telemetry), its counters are
        created at zero or on their first increment, and nothing else answers
        at the address — the pre-launch check refuses the run if something
        does. Without an origin the first answer is the baseline, and whatever
        the engine committed between launch and that answer is in no delta: a
        small run seeded before launch can finish there and be filed as having
        measured nothing.

        The span from launch to the first answer is written as the opening
        row's interval, with its increase as the delta, but it is NOT a
        scheduled bucket — its length is whatever the start took, a few
        milliseconds or a container exec — so it gets no per-second rate and
        does not enter `max_rate`.
        """
        if self.samples:
            raise ValueError("the origin of a series must be set before its first sample")
        self._prev = prom_text.Exposition()
        self._opening_span = True
        self.first_mono, self.first_iso = mono, iso
        self.last_mono, self.last_iso = mono, iso

    def add(self, iso: str, mono: float, expo: prom_text.Exposition) -> None:
        prev = self._prev
        dt = (mono - self.last_mono) if self.last_mono is not None else None
        elapsed = (mono - self.first_mono) if self.first_mono is not None else 0.0
        if prev is not None:
            gone = sorted(set(prev.series) - set(expo.series))
            if gone:
                raise SeriesInconsistent(
                    f"series {', '.join(gone[:3])} disappeared between two scrapes: the engine's "
                    "registry never drops a series, so a different process is answering or the "
                    "engine restarted")
        for name, declared in SUMMARY_SERIES:
            present = any(s.name == name for s in expo.series.values())
            if present and expo.kind(name) != declared:
                raise SeriesInconsistent(
                    f"{name} is declared a {declared} in SUMMARY_SERIES but the endpoint types it "
                    f"{expo.kind(name)}; its summary columns would be arithmetic on the wrong kind")
        interval_deltas: dict[str, float] = {}
        for key in sorted(expo.series):
            s = expo.series[key]
            kind = expo.kind(s.name)
            delta = per_second = ""
            if prev is not None and kind == "counter":
                before = prev.series.get(key)
                # A counter first exposed mid-window started at zero within
                # this interval: the engine's labelled counters are created by
                # their first increment, so its first value IS the increment.
                d = s.value - (before.value if before is not None else 0.0)
                if d < 0:
                    raise SeriesInconsistent(
                        f"counter {key} went backwards ({_fmt(before.value)} -> {_fmt(s.value)}): "
                        "the engine restarted, or a different process is answering")
                delta = _fmt(d)
                per_second = repr(round(d / dt, 6)) if dt and not self._opening_span else ""
                interval_deltas[s.name] = interval_deltas.get(s.name, 0.0) + d
            self._csv.writerow([
                iso, f"{elapsed:.3f}", f"{dt:.3f}" if dt is not None else "",
                s.name, prom_text.render_labels(s.labels)[1:-1] if s.labels else "",
                kind, _fmt(s.value), delta, per_second,
            ])
            self.rows_written += 1
        self._fh.flush()
        for name, declared in SUMMARY_SERIES:
            roll = self.rollups[name]
            total = expo.total(name)
            if total is None:
                continue
            roll.seen = True
            if declared == "counter":
                if prev is not None:
                    roll.window += interval_deltas.get(name, 0.0)
                if prev is not None and dt and not self._opening_span:
                    rate = interval_deltas.get(name, 0.0) / dt
                    roll.max_rate = rate if roll.max_rate is None else max(roll.max_rate, rate)
            else:
                roll.low = total if roll.low is None else min(roll.low, total)
                roll.high = total if roll.high is None else max(roll.high, total)
        if self.first_mono is None:
            self.first_mono, self.first_iso = mono, iso
        self.last_mono, self.last_iso = mono, iso
        self._prev = expo
        self._opening_span = False
        self.samples += 1


@dataclass
class EngineWindow:
    """A closed window: where the series is, and its flat rollup."""

    endpoint: MetricsEndpoint
    interval: float
    opened_at: str
    closed_at: str
    window_seconds: float
    samples: int
    csv_path: str
    log_path: str
    rollups: dict[str, _Rollup]
    # How the engine was stopped after the window closed: `sigint` (graceful,
    # buffers flushed), `sigkill` (forced; in-flight batches are left for the
    # recovery pass to find), or `exited`.
    stopped_by: str = "unknown"

    def _counter(self, name: str) -> float:
        roll = self.rollups.get(name)
        return roll.window if roll is not None and roll.seen else 0.0

    def run_totals(self) -> dict[str, int]:
        """The run-level numbers the per-cycle loop would have produced, from
        the counters' increase INSIDE the window: batches committed and failed,
        and uncompressed/compressed bytes of the committed ones — the engine
        increments bytes_in/bytes_out at the commit, the same population the
        per-cycle loop sums.

        Zero rather than blank for a family never exposed: these columns have
        always been numbers, the endpoint answered throughout, and a commit or
        a failure creates its series.
        """
        return {
            "success": int(self._counter("vtop_commits_total")),
            "failed": int(self._counter("vtop_failed_total")),
            "in_bytes": int(self._counter("vtop_bytes_in_total")),
            "out_bytes": int(self._counter("vtop_bytes_out_total")),
        }

    def summary_columns(self) -> dict[str, object]:
        cols: dict[str, object] = {
            "engine_mode": LONG_LIVED,
            "engine_metrics_address": self.endpoint.url,
            "engine_window_seconds": round(self.window_seconds, 3),
            "engine_scrape_interval_seconds": self.interval,
            "engine_samples": self.samples,
            "engine_stopped_by": self.stopped_by,
        }
        for name, kind in SUMMARY_SERIES:
            roll = self.rollups[name]
            # BLANK when the family was never exposed: that is "never
            # incremented" OR "this binary does not have it", and a zero would
            # claim to know which.
            if kind == "counter":
                cols[f"engine_{_short(name)}_window"] = _fmt(roll.window) if roll.seen else ""
                cols[f"engine_{_short(name)}_max_per_second"] = (
                    round(roll.max_rate, 3) if roll.seen and roll.max_rate is not None else "")
            else:
                cols[f"engine_{_short(name)}_min"] = _fmt(roll.low) if roll.low is not None else ""
                cols[f"engine_{_short(name)}_max"] = _fmt(roll.high) if roll.high is not None else ""
        for col in PER_CYCLE_ONLY_COLUMNS:
            cols[col] = ""
        return cols

    def log_tail(self, limit: int = 8000) -> str:
        try:
            with open(self.log_path, encoding="utf-8", errors="replace") as fh:
                text = fh.read()
        except OSError:
            return ""
        return text[-limit:]


def _tail(path: str, limit: int = 2000) -> str:
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            return fh.read()[-limit:].strip()
    except OSError:
        return ""


def measure_window(binary: str, config_path: str, scenario, results_dir: str, *,
                   duration: float | None = None, interval: float | None = None,
                   endpoint: MetricsEndpoint | None = None, engine_process=None,
                   ready_timeout: float = READY_TIMEOUT_SECONDS,
                   stop_grace: float = STOP_GRACE_SECONDS,
                   sleep=time.sleep, clock=time.monotonic) -> EngineWindow:
    """Run one long-lived engine for the window and return its closed series.

    THE WINDOW STARTS AT LAUNCH. The runner seeds before it calls this, so the
    engine has work from its first cycle, and a small run can commit all of
    it before the endpoint first answers. The counters are therefore read
    against zero at launch — true of the fresh process this starts (see
    `SeriesRecorder.origin`) — rather than against the first answer, and
    `window_seconds` is measured from launch too, so the `_window` columns and
    the span they cover describe the same interval. Seeding after the first
    answer instead was rejected: it would write the initial files while the
    engine reads them, which is exactly the half-written-file hazard the
    runner refuses concurrent whole-file seeding for. The recovery pass
    `vtopctl run` makes at start is inside the window by this definition; the
    runner gives every run a fresh ledger, so there it recovers nothing.

    Raises an EngineWindowError subclass for every refusal in the module
    docstring. The engine is stopped before this returns or raises, whatever
    happened — including a KeyboardInterrupt, which propagates after the stop.
    """
    mode = engine.runner_mode(scenario)
    if duration is None:
        duration = float(scenario.get("duration_seconds", 0) or 0)
    if interval is None:
        interval = scrape_interval(scenario)
    if endpoint is None:
        endpoint = endpoint_for(mode)

    # BEFORE LAUNCH, nothing may answer there. A leaked engine from an earlier
    # run holding the address would be scraped in this one's place, and the new
    # engine's own bind failure is only a log line (the endpoint is opt-in and
    # never fatal to the data path).
    try:
        scrape(endpoint, timeout=1.0)
    except ScrapeFailed:
        pass
    else:
        raise EndpointAlreadyAnswering(
            f"an engine already answers at {endpoint.url} before this run started one — a "
            "leaked engine from an earlier run? Stop it first; scraping it would file its "
            "counters under this run")

    proc = engine_process if engine_process is not None else launcher(
        binary, config_path, scenario, endpoint)
    log_path = os.path.join(results_dir, ENGINE_LOG)
    partial = os.path.join(results_dir, ENGINE_METRICS_PARTIAL_CSV)
    recorder = None
    stopped_by = ""
    with open(log_path, "w", encoding="utf-8") as log:
        try:
            recorder = SeriesRecorder(partial)
            recorder.origin(_iso_now(), clock())
            proc.start(log)
            _observe(proc, endpoint, recorder, duration, interval, ready_timeout,
                     log_path, sleep, clock)
        finally:
            try:
                if recorder is not None:
                    recorder.close()
            finally:
                stopped_by = proc.stop(stop_grace)
    final = os.path.join(results_dir, ENGINE_METRICS_CSV)
    os.replace(partial, final)
    return EngineWindow(
        endpoint=endpoint, interval=interval, opened_at=recorder.first_iso,
        closed_at=recorder.last_iso, window_seconds=recorder.last_mono - recorder.first_mono,
        samples=recorder.samples, csv_path=final, log_path=log_path, rollups=recorder.rollups,
        stopped_by=stopped_by or "unknown")


def _observe(proc, endpoint: MetricsEndpoint, recorder: SeriesRecorder, duration: float,
             interval: float, ready_timeout: float, log_path: str, sleep, clock) -> None:
    def exited(rc: int, when: str) -> EngineExitedEarly:
        tail = _tail(log_path)
        return EngineExitedEarly(
            f"the engine exited with status {rc} {when}; a window it did not live through "
            f"measured nothing. Its output ({log_path}):\n{tail or '(empty)'}")

    # READINESS. The first answer is the first SAMPLE, and the scheduled ticks
    # count from it — a run whose first samples are connection refusals has
    # measured nothing. The counters' baseline is the launch, already set as
    # the recorder's origin.
    deadline = clock() + ready_timeout
    last_error = "no attempt was made"
    while True:
        rc = proc.exit_code()
        if rc is not None:
            raise exited(rc, f"before its metrics endpoint at {endpoint.url} answered")
        mono, iso = clock(), _iso_now()
        try:
            expo = scrape(endpoint, timeout=max(0.1, min(2.0, ready_timeout)))
            break
        except ScrapeFailed as err:
            last_error = str(err)
        if clock() >= deadline:
            hint = ""
            if isinstance(proc, ContainerEngine):
                hint = (f" The compose file publishes the container's :{CONTAINER_METRICS_PORT} on the "
                        f"host at {endpoint.host}:{endpoint.port}; a vtop-engine container created "
                        "before that port existed does not have it — recreate the containerized "
                        "profile.")
            raise EndpointNeverAnswered(
                f"the engine's metrics endpoint at {endpoint.url} never answered within "
                f"{ready_timeout:g}s of launch (last error: {last_error}).{hint}")
        sleep(READY_POLL_SECONDS)
    recorder.add(iso, mono, expo)

    # THE WINDOW. Ticks are scheduled from the opening sample, not from the
    # previous one, so scrape latency never accumulates into drift; the window
    # is rounded UP to a whole number of intervals so its last bucket is whole.
    opened = mono
    ticks = max(1, math.ceil(duration / interval - 1e-9))
    for k in range(1, ticks + 1):
        target = opened + k * interval
        now = clock()
        if now < target:
            sleep(target - now)
        rc = proc.exit_code()
        if rc is not None:
            raise exited(rc, f"{clock() - opened:.1f}s into a {duration:g}s window")
        mono, iso = clock(), _iso_now()
        gap = mono - recorder.last_mono
        if gap > 2 * interval:
            raise ScrapeStopped(
                f"the scraper fell behind: {gap:.2f}s between samples against a {interval:g}s "
                f"interval, {mono - opened:.1f}s into the window at {endpoint.url}. A bucket that "
                "wide averages the excursions it exists to show; the series is refused rather "
                "than reported coarse")
        try:
            expo = scrape(endpoint, timeout=interval)
        except ScrapeFailed as err:
            raise ScrapeStopped(
                f"scraping {endpoint.url} stopped {mono - opened:.1f}s into a {duration:g}s window "
                f"after {recorder.samples} sample(s): {err}. A partial series is not a "
                "measurement") from err
        recorder.add(iso, mono, expo)
