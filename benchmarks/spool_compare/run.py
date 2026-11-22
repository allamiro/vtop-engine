#!/usr/bin/env python3
"""Run the spool comparison (#748): files and a syslog spool to object storage, VTOP against Vector and Fluent Bit.

    python3 benchmarks/spool_compare/run.py --plan smoke                 # minutes, any host
    python3 benchmarks/spool_compare/run.py --plan full --dry-run        # the cells and projected hours
    python3 benchmarks/spool_compare/run.py --plan full --budget-minutes 120 --cooldown-seconds 60
    python3 benchmarks/spool_compare/run.py --resume spool-<stamp> --budget-minutes 120
    python3 benchmarks/spool_compare/run.py --check benchmarks/spool_compare/results/spool-<stamp>

Each system run is its own Compose project (`compose.yml`) on fresh volumes: the
lab MinIO, the generator (the only writer of the spool, and the owner of every
rotation), the tool in its limit class, and in crash cells the S3 tap under
the store's name. The checker reads a snapshot of the tool's prefix and the
generator's expected sequence numbers; it never reads a tool's state.

Refused before any Docker work: a plan the kind cannot run, a projection over
`--max-hours`, a tool configuration that differs from HEAD (the CSV records
the hash of what ran, rule 13), a product the registry does not let run, and
a Fluent Bit configuration that does not state `storage.type` and `db.sync`
(the settings that decide its crash result, #748 test 8).

Results: `results/<run-id>/spool.csv` (schema `compare_common.schema`),
`kills.csv`, `environment.json`, and `logs/` (each run's tool log, tap trace,
generator state). `results/` runs are committed by hand in a data PR; nothing
here commits.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import tempfile
import time
import urllib.request
from collections.abc import Callable
from typing import Any

HERE = os.path.dirname(os.path.realpath(__file__))
BENCH = os.path.dirname(HERE)
REPO = os.path.dirname(BENCH)
if BENCH not in sys.path:
    sys.path.insert(0, BENCH)

from compare_common import cgroup, checker, docker_api, environment, kills, lab, registry, runner, schema, store  # noqa: E402
from compare_common import plan as plans  # noqa: E402
from kafka_compare import stats  # noqa: E402
from spool_compare.kind import KIND  # noqa: E402

COMPOSE = os.path.join(HERE, "compose.yml")
RESULTS = os.path.join(HERE, "results")
BUCKET = "bench"
TOOLS = "benchmarks/spool_compare/tools"
CONFIGS = {
    "fluent-bit": {"durable": f"{TOOLS}/fluent-bit/durable.conf"},
    "vtop": {"strong": f"{TOOLS}/vtop/strong.yaml"},
    "vector": {"acks": f"{TOOLS}/vector/acks.yaml", "disk-buffer": f"{TOOLS}/vector/disk-buffer.yaml"},
    "otel-collector": {"file-storage": f"{TOOLS}/otel-collector/file-storage.yaml"},
}
PREFIX = {"fluent-bit": "fluent-bit", "vtop": "vtop", "vector": "vector", "otel-collector": "otel"}
TELEMETRY = {
    "vtop": "no usage reporting; OpenTelemetry export off (OTEL_SDK_DISABLED=true, no endpoint set)",
    "vector": "no usage reporting; API server off (api.enabled: false)",
    "fluent-bit": "no usage reporting; HTTP monitoring server off (http_server off)",
    "otel-collector": "no usage reporting; own metrics off (service.telemetry.metrics.level: none)",
}
DEVIATIONS = [
    "VTOP stores each line as read (newline-framed syslog, gzip); Vector and Fluent Bit wrap each line in a JSON "
    "object (their documented JSON output). The checker reads the id token in either form; object sizes and "
    "MiB/s are of SOURCE bytes, so the wrapping does not change them (rule 15).",
    "Fluent Bit's S3 output has no byte-and-age pair identical to the others': `total_file_size 32M` and "
    "`upload_timeout 60s` are the closest documented settings.",
    "Every tool reads the active file and its rotated names (`app.log*`): a backlog is written across rotated "
    "files before the tool starts. Rotated names are `app.log.<k>`, k never reused, never deleted.",
    "Time to store-visible uses the generator's write time and MinIO's LastModified (milliseconds), both on the "
    "Docker VM's clock.",
]
QUIET_SECONDS = 75  # one 60 s batch age and margin: no new object for this long is quiescence


def log(message: str) -> None:
    lab.log("spool-compare", message)


# ------------------------------------------------------------------ configs


def durability_settings(system: str, text: str) -> dict[str, str]:
    """The settings that decide a tool's crash result, as its configuration file states them (#748 test 8)."""
    def find(pattern: str) -> str:
        match = re.search(pattern, text, re.M)
        return match.group(1) if match else ""

    if system == "fluent-bit":
        return {"storage.type": find(r"^\s*storage\.type\s+(\S+)"), "db.sync": find(r"^\s*db\.sync\s+(\S+)"),
                "storage.sync": find(r"^\s*storage\.sync\s+(\S+)"), "db": find(r"^\s*db\s+(\S+)")}
    if system == "vector":
        return {"buffer.type": find(r"^\s{6}type:\s*(memory|disk)\s*$"),
                "acknowledgements.enabled": find(r"acknowledgements:\s*\n\s*enabled:\s*(\S+)")}
    if system == "vtop":
        return {"require_strong_verification": find(r"^\s*require_strong_verification:\s*(\S+)")}
    if system == "otel-collector":
        return {"file_storage.fsync": find(r"^\s*fsync:\s*(\S+)"), "sending_queue.storage": find(r"storage:\s*(\S+)")}
    return {}


def record_durability(configs: dict[str, dict[str, dict[str, str]]]) -> dict[str, dict[str, dict[str, str]]]:
    """{system: {profile: settings}}; refuses a Fluent Bit config that does not state both deciding settings."""
    out: dict[str, dict[str, dict[str, str]]] = {}
    for system, profiles in configs.items():
        for profile, recorded in profiles.items():
            settings = durability_settings(system, recorded["text"])
            if system == "fluent-bit" and not (settings.get("storage.type") and settings.get("db.sync")):
                raise SystemExit(f"{recorded['path']} does not state storage.type and db.sync; they decide Fluent "
                                 "Bit's crash result and must be visible in the environment block (#748 test 8)")
            out.setdefault(system, {})[profile] = settings
    return out


def require_config(system: str, profile: str) -> str:
    """The profile's committed configuration file; a missing one is a refused row, never a weaker stand-in."""
    rel = CONFIGS.get(system, {}).get(profile)
    if not rel or not os.path.isfile(os.path.join(REPO, rel)):
        raise runner.CellRefused(f"{system} profile {profile}: its documented-best configuration "
                                 f"({rel or 'none registered'}) cannot be written, so it is not run; no weaker "
                                 "configuration is substituted (#748 test 9)")
    return os.path.join(REPO, rel)


# ---------------------------------------------------------------- the lab


class Lab:
    """One Compose project: the store, the generator, one tool, and in crash cells the tap."""

    def __init__(self, project: str, system: str, config: str, *, vtop_image: str, tap: bool, logs: str,
                 cpus: str, memory: str):
        self.project, self.system, self.tap = project, system, tap
        self.env = {"VTOP_IMAGE": vtop_image, "TOOL_CONFIG": config, "BENCH_DIR": BENCH, "RUN_LOGS": logs,
                    "TOOL_CPUS": cpus, "TOOL_MEMORY": memory, "STORE_ALIAS": "minio-real" if tap else "minio",
                    "DOCKER_SOCKET": docker_api.SOCKET, "HARNESS_IMAGE": lab.HARNESS_IMAGE}
        self.profiles = ["--profile", system] + (["--profile", "tap"] if tap else [])
        self.ports: dict[str, int] = {}

    def compose(self, *args: str, timeout: float = 600, check: bool = True):
        return lab.compose(self.project, [COMPOSE], *self.profiles, *args, env=self.env, timeout=timeout,
                           check=check)

    def port(self, service: str, inner: int) -> int:
        key = f"{service}:{inner}"
        if key not in self.ports:
            out = self.compose("port", service, str(inner)).stdout.strip()
            self.ports[key] = int(out.rsplit(":", 1)[1])
        return self.ports[key]

    def up_base(self) -> None:
        self.compose("up", "-d", "--wait", "minio")
        self.compose("run", "--rm", "volume-init")
        services = ["generator"] + (["tap"] if self.tap else [])
        self.compose("up", "-d", *services)
        self.mc(f"mc mb --ignore-existing lab/{BUCKET}")
        _wait(lambda: self._http("generator", 9400, "/state", None) is not None, 60, "the generator")

    def mc(self, script: str) -> str:
        return self.compose("exec", "-T", "minio", "sh", "-c", script).stdout

    def _http(self, service: str, inner: int, path: str, body: dict | None, timeout: float = 600) -> Any:
        url = f"http://127.0.0.1:{self.port(service, inner)}{path}"
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(url, data=data, method="GET" if body is None else "POST",
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read()
        except OSError:
            return None
        text = raw.decode()
        return json.loads(text) if text.startswith(("{", "[")) else text

    def gen(self, path: str, body: dict | None = None) -> Any:
        answer = self._http("generator", 9400, path, body)
        if answer is None:
            raise RuntimeError(f"generator {path} did not answer")
        return answer

    def tap_ctl(self, path: str, body: dict | None = None) -> Any:
        answer = self._http("tap", 9300, path, body, timeout=30)
        if answer is None:
            raise RuntimeError(f"tap {path} did not answer")
        return answer

    def container(self) -> str:
        return self.compose("ps", "-a", "-q", self.system).stdout.strip()

    def start_tool(self) -> float:
        """Start (or restart) the tool; a tool that exits at start is a refused row with its own error."""
        started = time.time()
        self.compose("up", "-d", "--no-recreate", self.system)
        time.sleep(3)
        state = lab.container_exit(self.container())
        if not state["running"]:
            raise runner.CellRefused(f"{self.system} exited {state['exit_code']} at start: "
                                     f"{lab.logs_tail(self.container(), 15)}")
        return started

    def running(self) -> bool:
        name = self.container()
        return bool(name) and bool(lab.container_exit(name)["running"])

    def kill(self) -> float:
        docker_api.kill(self.container())
        return time.time()

    def stop_tool(self) -> None:
        self.compose("stop", "-t", "30", self.system)

    def restart_tool(self) -> float:
        self.compose("start", self.system)
        return time.time()

    def listing(self, prefix: str) -> list[dict[str, Any]]:
        out = self.mc(f"mc ls --json --recursive lab/{BUCKET}/{prefix}/ || true")
        return [entry for entry in (json.loads(line) for line in out.splitlines() if line.strip())
                if entry.get("type") == "file"]

    def snapshot(self, prefix: str, out_dir: str) -> checker.Snapshot:
        inner = "/tmp/snap"
        lines = store.snapshot_commands("", BUCKET, prefix, inner)
        self.mc(f"rm -rf {inner} && " + " && ".join(lines))
        self.compose("cp", f"minio:{inner}/.", out_dir)
        listing = checker.read_json_lines(os.path.join(out_dir, "listing.jsonl"))
        incomplete = [entry.get("key", "") for entry in checker.read_json_lines(
            os.path.join(out_dir, "incomplete.jsonl")) if entry.get("key")]
        return checker.snapshot_from_dir(os.path.join(out_dir, "objects"), listing, incomplete)

    def request_counts(self) -> dict[str, float]:
        url = f"http://127.0.0.1:{self.port('minio', 9000)}/minio/v2/metrics/cluster"
        with urllib.request.urlopen(url, timeout=30) as response:
            return store.request_counts(response.read().decode())

    def save_logs(self, out_dir: str) -> None:
        os.makedirs(out_dir, exist_ok=True)
        name = self.container()
        if name:
            text = lab.sh(["docker", "logs", name], check=False)
            with open(os.path.join(out_dir, f"{self.system}.log"), "w", encoding="utf-8") as handle:
                handle.write(text.stdout + text.stderr)
        state = self._http("generator", 9400, "/state", None, timeout=10)
        with open(os.path.join(out_dir, "generator-state.json"), "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=1)

    def down(self) -> None:
        self.compose("down", "-v", "--remove-orphans", timeout=300, check=False)


def _wait(predicate: Callable[[], bool], seconds: float, what: str, interval: float = 1.0) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise RuntimeError(f"{what} was not ready within {seconds:.0f} s")


def quiesce(listing: Callable[[], list[dict[str, Any]]], running: Callable[[], bool], *, quiet: float,
            timeout: float, clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep,
            poll: float = 5.0) -> list[dict[str, Any]]:
    """Wait until the prefix has at least one object and no new object for `quiet` seconds.

    A tool that exits meanwhile is a failed row, with no retry (rule 18)."""
    deadline = clock() + timeout
    last_shape, changed = None, clock()
    while True:
        entries = listing()
        shape = (len(entries), sum(int(e.get("size", 0)) for e in entries))
        if shape != last_shape:
            last_shape, changed = shape, clock()
        if entries and clock() - changed >= quiet:
            return entries
        if not running():
            raise runner.ToolFailed("the tool exited before the run reached quiescence")
        if clock() > deadline:
            raise runner.ToolFailed(f"no quiescence within {timeout / 60:.0f} minutes ({shape[0]} objects)")
        sleep(poll)


# ------------------------------------------------------------ cell drivers


def run_k4(point: kills.KillPoint, started_at: float, rotate: Callable[[], Any], kill: Callable[[], float],
           clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep
           ) -> tuple[float, float]:
    """K4: rotate the active file `at_seconds` after the tool's (re)start, then kill `window_ms` after the
    rotation returned. Returns (rotated_at, killed_at); the kill never precedes the rotation."""
    wait = started_at + float(point.at_seconds) - clock()
    if wait > 0:
        sleep(wait)
    rotate()
    rotated_at = clock()
    sleep(int(point.window_ms) / 1000.0)
    return rotated_at, kill()


def down_while_rotating(running: Callable[[], bool], stop: Callable[[], None], rotate: Callable[[], Any],
                        write: Callable[[], Any], start: Callable[[], Any], rotations: int = 2) -> int:
    """Stop the tool, rotate `rotations` times with lines written after each, start it again.

    Refuses the run (CellRefused) if the tool is running at any rotation: the cell
    measures rotations that happened while the tool was down, nothing else (#748 test 7)."""
    stop()
    for index in range(rotations):
        if running():
            raise runner.CellRefused(f"the tool was running at rotation {index + 1} of a down-while-rotating run; "
                                     "the run does not measure what the cell names")
        rotate()
        write()
    if running():
        raise runner.CellRefused("the tool was running before its restart in a down-while-rotating run")
    start()
    return rotations


def first_object_after(listing: list[dict[str, Any]], times: list[float]) -> str:
    """The median seconds from each restart to the first object whose LastModified follows it."""
    stamps = sorted(checker.parse_time(e["lastModified"]) for e in listing if e.get("lastModified"))
    gaps = []
    for at in times:
        later = [s for s in stamps if s >= at]
        if later:
            gaps.append(later[0] - at)
    return f"{stats.median(gaps):.1f}" if gaps else ""


class Runner:
    def __init__(self, args: argparse.Namespace, env: dict[str, Any], run_dir: str, vtop_image: str):
        self.args, self.env, self.run_dir, self.vtop_image = args, env, run_dir, vtop_image
        self.tag = lab.run_tag(env["run_id"])
        self.index = 0
        self.kills_path = os.path.join(run_dir, "kills.csv")

    def describe(self, item: plans.Planned) -> dict[str, str]:
        reg = registry.load()
        entry = registry.product(reg, item.system)
        image = self.vtop_image if item.system == "vtop" else entry.get("image", "")
        recorded = self.env["configs"].get(item.system, {}).get(item.profile, {})
        version = self.env["comparable"]["vtop_commit"][:12] if item.system == "vtop" else entry["version"]
        return {"system_version": version,
                "image_digest": self.env["comparable"]["images"].get(item.system, image),
                "config_sha256": recorded.get("sha256", "")}

    def record_kill(self, item: plans.Planned, point: kills.KillPoint, trigger: str, killed_at: float,
                    delay_ms: Any, restarted_at: float, last_line: str) -> None:
        new = not os.path.exists(self.kills_path)
        with open(self.kills_path, "a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            if new:
                writer.writerow(kills.KILLS_COLUMNS)
            writer.writerow([self.env["run_id"], item.system, item.profile, item.cell, item.repetition,
                             point.index, point.kind, trigger, point.n or "", point.at_seconds or "",
                             point.window_ms if point.window_ms is not None else "", f"{killed_at:.3f}",
                             delay_ms, f"{restarted_at:.3f}", "", last_line])

    def execute(self, item: plans.Planned) -> dict[str, Any]:
        config = require_config(item.system, item.profile)
        self.index += 1
        key = "-".join(item.key(KIND.variant_columns))
        logs = os.path.join(self.run_dir, "logs", key)
        os.makedirs(logs, exist_ok=True)
        tap = item.cell in ("crash-K1", "crash-K2")
        project = f"vtop748-{self.tag}-{self.index}"
        tool = Lab(project, item.system, config, vtop_image=self.vtop_image, tap=tap, logs=logs,
                   cpus=self.args.tool_cpus, memory=self.args.tool_memory)
        p = item.params
        tool.down()
        try:
            tool.up_base()
            tool.gen("/setup", {"source": item.variant["source"], "rotation": item.variant["rotation"],
                                "seed": int(p.get("seed", 748)), "tag": self.tag,
                                "line_bytes": int(p.get("line_bytes", 200)),
                                "rotate_bytes": int(p.get("rotate_bytes", 64 * 2**20))})
            return self._cell(item, tool, logs)
        finally:
            try:
                tool.save_logs(logs)
            finally:
                tool.down()

    def _cell(self, item: plans.Planned, tool: Lab, logs: str) -> dict[str, Any]:
        p = item.params
        records = int(p.get("records", 0))
        timeout = float(p.get("timeout_minutes", 30)) * 60
        prefix = PREFIX[item.system]
        out: dict[str, Any] = {}
        restarts: list[float] = []
        rotations_by_harness = 0
        if item.cell == "live":
            before = None
            started = tool.start_tool()
            rate = live_rate(self.run_dir, item, float(p.get("live_fraction", 0.5)), p.get("rate"))
            p = dict(p, rate=rate)
            seconds = float(p.get("warmup_seconds", 120)) + float(p.get("measure_seconds", 600))
            tool.gen("/live", {"rate": rate, "seconds": seconds})
            _wait(lambda: not tool.gen("/state").get("live_running"), seconds + 120, "the live writer", 5)
        else:
            preload = records if item.cell != "down-while-rotating" else records // 3
            tool.gen("/backlog", {"count": preload})
            before = tool.request_counts()
            started = tool.start_tool()
        sampler = cgroup.Sampler(tool.container(), docker_api.stats)
        with sampler:
            if item.cell in ("crash-K1", "crash-K2", "crash-K3", "crash-K4-rotation"):
                kind = item.cell.split("-")[1]
                points = kills.schedule(int(p.get("kill_seed", 748)), kind, item.repetition,
                                        int(p.get("kills_per_run", 3)),
                                        n_range=tuple(p.get("n_range", (2, 12))),
                                        seconds_range=tuple(p.get("seconds_range", (5.0, 40.0))))
                life = started
                fired = 0
                for point in points:
                    if kind in ("K1", "K2"):
                        tool.tap_ctl("/arm", {"kind": kind, "n": point.n, "container": tool.container()})
                        done = _until(lambda count=fired: len(tool.tap_ctl("/state")["fired"]) > count, timeout)
                        if not done:
                            break  # the tool finished before its Nth PUT: fewer kills, recorded as such
                        event = tool.tap_ctl("/state")["fired"][-1]
                        killed_at, delay = event["killed_at"], event["kill_delay_ms"]
                        tool.tap_ctl("/reset", {})
                    elif kind == "K3":
                        wait = life + float(point.at_seconds) - time.time()
                        if wait > 0:
                            time.sleep(wait)
                        killed_at, delay = tool.kill(), ""
                    else:
                        _rotated, killed_at = run_k4(point, life, lambda: tool.gen("/rotate", {}), tool.kill)
                        rotations_by_harness += 1
                        delay = ""
                        tool.gen("/backlog", {"count": max(records // 10, 1)})
                    fired += 1
                    last = lab.last_log_line(tool.container())
                    _until(lambda: not tool.running(), 30)
                    life = tool.restart_tool()
                    restarts.append(life)
                    self.record_kill(item, point, kind, killed_at, delay, life, last)
                if fired == 0:
                    raise runner.ToolFailed(f"no {kind} kill fired: the tool finished before the first trigger")
                out["kills"] = fired
            elif item.cell == "down-while-rotating":
                quiesce(lambda: tool.listing(prefix), tool.running, quiet=QUIET_SECONDS, timeout=timeout)
                third = records // 3
                rotations_by_harness = down_while_rotating(
                    tool.running, tool.stop_tool, lambda: tool.gen("/rotate", {}),
                    lambda: tool.gen("/backlog", {"count": third}), tool.restart_tool)
            entries = quiesce(lambda: tool.listing(prefix), tool.running, quiet=QUIET_SECONDS, timeout=timeout)
            ended = time.time()
        after = tool.request_counts() if before is not None else None
        state = tool.gen("/state")
        expected = range(0, int(state["next_seq"]))
        snap = tool.snapshot(prefix, tempfile.mkdtemp(prefix="snap-", dir=logs))
        result = checker.check(snap, expected, self.tag,
                               is_metadata=lambda key: key.endswith((".manifest.json", ".receipt.json")))
        out.update({"records_expected": result.expected, "records_present": result.present_unique,
                    "gaps": result.gaps, "duplicates": result.duplicates, "torn_objects": result.torn_objects,
                    "orphan_objects": result.orphan_objects, "unexpected": result.unexpected})
        if item.cell in ("crash-K4-rotation", "down-while-rotating"):
            out["rotations"] = len(state.get("rotations", []))
        if restarts:
            out["seconds_to_first_object_after_restart"] = first_object_after(entries, restarts)
        if item.cell == "smoke" and (result.gaps or result.duplicates or result.torn_objects or result.unexpected):
            raise runner.ToolFailed(f"smoke: {result.gaps} gaps, {result.duplicates} duplicates, "
                                    f"{result.torn_objects} torn, {result.unexpected} unexpected; the tool is not "
                                    "timed (sample gaps: " + ",".join(map(str, result.gap_sample[:5])) + ")")
        line_bytes = int(p.get("line_bytes", 200)) + 1
        if item.cell == "drain":
            last = max(checker.parse_time(e["lastModified"]) for e in entries)
            window = max(last - started, 1e-3)
            source_bytes = result.present_unique * line_bytes
            usage = cgroup.window(sampler.samples, started, ended)
            gib = source_bytes / 2**30
            per = store.per_gib(store.requests_by_method(before, after), source_bytes)
            sizes = result.object_sizes or [0]
            out.update({"window_seconds": f"{window:.1f}", "lines_per_sec": f"{result.present_unique / window:.1f}",
                        "mib_per_sec": f"{source_bytes / 2**20 / window:.2f}", "source_bytes": source_bytes,
                        "cpu_seconds": f"{usage.cpu_seconds:.2f}", "cpu_seconds_per_gib":
                        f"{usage.cpu_seconds / gib:.2f}", "peak_rss_sampled_bytes": usage.peak_rss_sampled_bytes,
                        "put_per_gib": f"{per['PUT']:.2f}", "get_per_gib": f"{per['GET']:.2f}",
                        "head_per_gib": f"{per['HEAD']:.2f}", "list_per_gib": f"{per['LIST']:.2f}",
                        "other_per_gib": f"{per['POST'] + per['DELETE'] + per['OTHER']:.2f}",
                        "objects": result.data_objects, "object_size_p50_bytes": int(stats.percentile(sizes, 50)),
                        "object_size_p99_bytes": int(stats.percentile(sizes, 99))})
        if item.cell == "live":
            written = [line.split(",") for line in str(tool.gen("/written")).splitlines() if line]
            warm = started + float(p.get("warmup_seconds", 120))
            lags = [(result.first_visible[int(seq)] - float(at)) * 1000 for seq, at in written
                    if float(at) >= warm and int(seq) in result.first_visible]
            measured = [1 for _seq, at in written if float(at) >= warm]
            achieved = len(measured) / float(p.get("measure_seconds", 600))
            out.update({"offered_rate": p["rate"], "achieved_rate": f"{achieved:.1f}",
                        "sustained": "true" if stats.sustained(float(p["rate"]), achieved) else "false",
                        "visible_p50_ms": f"{stats.percentile(lags, 50):.0f}" if lags else "",
                        "visible_p99_ms": f"{stats.percentile(lags, 99):.0f}" if lags else ""})
        if sampler.errors:
            out["note"] = f"{len(sampler.errors)} cgroup samples failed: {sampler.errors[0][:120]}"
        return out


def live_rate(run_dir: str, item: plans.Planned, fraction: float, fixed: Any = None) -> float:
    """The live cell's offered rate: `fraction` of the SLOWEST system's median drain rate for the same variant
    in this run (#748: an offer above a tool's capacity measures its queue, not its latency). A plan may fix
    `rate` instead (the smoke and hosted plans, which have no drain cell)."""
    if fixed is not None:
        return float(fixed)
    path = os.path.join(run_dir, f"{KIND.name}.csv")
    rows = schema.read_rows(path, KIND) if os.path.exists(path) else []
    by_system: dict[str, list[float]] = {}
    for row in rows:
        if (row["cell"] == "drain" and row["status"] == "ok" and row["source"] == item.variant["source"]
                and row["rotation"] == item.variant["rotation"] and row["lines_per_sec"]):
            by_system.setdefault(row["system"], []).append(float(row["lines_per_sec"]))
    if not by_system:
        raise runner.CellRefused("no drain result for this variant in the run, so the live rate cannot be derived; "
                                 "run the drain cell first, or give the plan a fixed `rate`")
    return round(min(stats.median(v) for v in by_system.values()) * fraction, 1)


def _until(predicate: Callable[[], bool], seconds: float, interval: float = 0.5) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


# --------------------------------------------------------------------- main


def comparable_block(args: argparse.Namespace, plan: dict[str, Any], vtop_image: str, configs: dict) -> dict:
    reg = registry.load()
    images = {s: registry.product(reg, s).get("image", "") for s in plan["systems"] if s != "vtop"}
    images["vtop"] = lab.image_id(vtop_image) if vtop_image else ""
    facts = environment.host_facts(probe_disk=not args.no_disk_probe)
    facts.update({
        "host_class": plan["host_class"],
        "vtop_commit": environment.git("rev-parse", "HEAD"),
        "vtop_engine_tree": lab.engine_tree(),
        "tool_limits": f"{args.tool_cpus} CPUs, {args.tool_memory}, no swap",
        "images": images,
        "minio_image": lab.MINIO_IMAGE,
        "harness_image": lab.HARNESS_IMAGE,
        "config_sha256": {s: {p: c["sha256"] for p, c in v.items()} for s, v in configs.items()},
    })
    return facts


def check(run_dir: str) -> int:
    try:
        env = schema.read_environment(run_dir)
        rows = schema.read_rows(os.path.join(run_dir, "spool.csv"), KIND)
        problems = schema.check_rows(rows, KIND, env)
        extra, missing = schema.check_completeness(rows, KIND, env["plan"])
    except schema.RunRefused as exc:
        print(f"check refused: {exc}", file=sys.stderr)
        return 2
    expected = len(plans.expected_keys(env["plan"], KIND))
    print(f"{expected - len(missing)} of {expected} expected rows present")
    for problem in problems + extra:
        print(f"problem: {problem}")
    for key in missing:
        print(f"missing: {key}")
    return 0 if not (problems or extra or missing) else 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--plan", default="smoke", help="a plan in plans.json: smoke, hosted, full")
    parser.add_argument("--smoke", action="store_true", help="the same as --plan smoke")
    parser.add_argument("--tools", default=None, help="comma-separated subset of the plan's systems")
    parser.add_argument("--host-class", choices=plans.HOST_CLASSES, default=None,
                        help="override the plan's host class (laptop for a smoke on a laptop)")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--resume", default=None, metavar="RUN_ID")
    parser.add_argument("--budget-minutes", type=float, default=None)
    parser.add_argument("--cooldown-seconds", type=float, default=0.0)
    parser.add_argument("--max-hours", type=float, default=30.0)
    parser.add_argument("--tool-cpus", default="2")
    parser.add_argument("--tool-memory", default="2g")
    parser.add_argument("--private", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--check", default=None, metavar="RUN_DIR")
    parser.add_argument("--results", default=RESULTS)
    parser.add_argument("--no-disk-probe", action="store_true", help="hosted runners: record the disk as not probed")
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args(argv)
    if args.check:
        return check(args.check)
    try:
        plan = plans.select(plans.load_plans(os.path.join(HERE, "plans.json")), "smoke" if args.smoke else args.plan)
        if args.tools:
            wanted = [t.strip() for t in args.tools.split(",") if t.strip()]
            plan["systems"] = [s for s in plan["systems"] if s in wanted]
        if args.host_class:
            plan["host_class"] = args.host_class
        plans.check_plan(plan, KIND)
        print(plans.describe(plan, KIND, args.cooldown_seconds))
        plans.check_budget(plan, KIND, args.max_hours, args.cooldown_seconds)
    except plans.PlanRefused as exc:
        print(f"plan refused: {exc}", file=sys.stderr)
        return 2
    if args.dry_run:
        return 0
    reg = registry.load()
    registry.check(reg)
    for system in plan["systems"]:
        registry.require_runnable(reg, system, private=args.private, use_case="b")
    paths = [CONFIGS[s][p] for s in plan["systems"] for p in KIND.profiles[s] if p in CONFIGS.get(s, {})]
    drift = environment.config_drift(paths + ["benchmarks/spool_compare/compose.yml"])
    if drift:
        raise SystemExit(f"tool configuration differs from HEAD ({', '.join(drift)}): commit it, so the hash the "
                         "CSV records names a file a reader can find (rule 13)")
    configs = environment.record_configs({s: dict(CONFIGS[s]) for s in plan["systems"]})
    durability = record_durability(configs)
    vtop_image = lab.ensure_vtop_image(args.rebuild) if "vtop" in plan["systems"] else ""
    lab.ensure_minio_image()
    lab.ensure_image(lab.HARNESS_IMAGE)
    for system in plan["systems"]:
        if system != "vtop":
            lab.ensure_image(registry.product(reg, system)["image"])
    comparable = comparable_block(args, plan, vtop_image, configs)
    if args.resume:
        run_dir = os.path.join(args.results, lab.check_run_id(args.resume))
        env = schema.read_environment(run_dir)
        runner.check_resume(env, comparable)
    else:
        run_id = lab.check_run_id(args.run_id or lab.new_run_id("spool"))
        run_dir = os.path.join(args.results, run_id)
        os.makedirs(run_dir)
        env = environment.new_environment(KIND.name, run_id, plan, comparable, configs, telemetry={
            s: TELEMETRY[s] for s in plan["systems"]}, private=args.private,
            extra={"durability": durability, "deviations": DEVIATIONS})
        environment.save(run_dir, env)
    driver = Runner(args, env, run_dir, vtop_image)
    outcome = runner.run_plan(run_dir, KIND, env, driver.describe, driver.execute,
                              budget_minutes=args.budget_minutes, cooldown_seconds=args.cooldown_seconds, log=log)
    if not outcome.stopped_by_budget:
        env["run_state"] = "complete"
        env["finished_at"] = environment.utc_now()
        environment.save(run_dir, env)
    log(f"{outcome.done} runs done, {outcome.skipped} skipped, {outcome.remaining} remaining; {run_dir}")
    return check(run_dir)


if __name__ == "__main__":
    sys.exit(main())
