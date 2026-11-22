#!/usr/bin/env python3
"""Run the archive comparison (#746): a Kafka topic to object storage, VTOP against Vector and Kafka Connect S3 (Lenses).

    python3 benchmarks/archive_compare/run.py --plan smoke                 # minutes, any host
    python3 benchmarks/archive_compare/run.py --plan full --dry-run        # the cells and projected hours
    python3 benchmarks/archive_compare/run.py --plan full --budget-minutes 120 --cooldown-seconds 60
    python3 benchmarks/archive_compare/run.py --resume archive-<stamp> --budget-minutes 120
    python3 benchmarks/archive_compare/run.py --check benchmarks/archive_compare/results/archive-<stamp>

Each system run is its own Compose project (`compose.yml`) on fresh volumes: one Apache Kafka node (the source, not
under test), the lab MinIO, the offset sampler, the tool in its limit class, and in crash and fault cells the S3 tap
under the store's name. The seeded producer (`producer.py`) writes the records; the checker (`check_bucket.py`)
reads a snapshot of the bucket and the producer's expected sequence numbers, and never reads a tool's state. The
offset sampler (`commits.py`) turns the broker's own record of every offset commit into the consumer group's lag
and into time to source-committed.

Durations are the plan's (`plans.json`): `smoke` is minutes (CI's nightly and any laptop), `hosted` is the
correctness cells on a hosted runner, `full` is every cell at the sizes #746 names, on the dedicated host. A live
cell's warm-up and measured seconds are `warmup_seconds` and `measure_seconds` in its plan entry.

Refused before any Docker work: a plan the kind cannot run, a projection over `--max-hours`, a performance cell on a
hosted host class, a tool configuration that differs from HEAD (the CSV records the hash of what ran, rule 13), and a
product the registry does not let run.

Results: `results/<run-id>/archive.csv` (schema `compare_common.schema`), `kills.csv`, `environment.json`, and
`logs/` (each run's tool log, tap trace, the offset commits, the producer's log, the bucket snapshot). Runs are
committed by hand in a data PR; nothing here commits.
"""
from __future__ import annotations

import argparse
import copy
import csv
import json
import os
import subprocess
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

from archive_compare import check_bucket, commits, faults, producer  # noqa: E402
from archive_compare.kind import KIND, TOOLS  # noqa: E402
from compare_common import cgroup, checker, docker_api, environment, kills, lab, registry, runner, schema, store  # noqa: E402
from compare_common import plan as plans  # noqa: E402
from compare_common import tap as tap_module  # noqa: E402
from kafka_compare import stats  # noqa: E402

COMPOSE = os.path.join(HERE, "compose.yml")
RESULTS = os.path.join(HERE, "results")
BUCKET = "bench"
TOPIC = "bench"
CONFIGS = {system: dict(spec["profiles"]) for system, spec in TOOLS.items()}
EXTRA_CONFIGS = {system: dict(spec.get("extra_configs") or {}) for system, spec in TOOLS.items()}
KAFKA_IMAGE = "apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7"
DEVIATIONS = [
    "The record is one JSON object per Kafka value. VTOP and Vector store the value as it came, newline-framed; "
    "Connect (Lenses) parses it with its JSON converter and writes it again, so its bytes may differ and its "
    "content may not. The smoke counts byte-exact and JSON-equal records separately; the checker reads the id token "
    "in either form, and MiB/s is of source bytes (the value as produced), so the framing does not change it (rule 15).",
    "Object size is set by each tool's documented threshold nearest to 'about 32 MiB uncompressed or 60 s': VTOP "
    "`batching.max_bytes` and Vector `batch.max_bytes` count uncompressed payload; Connect (Lenses) `flush.size` "
    "counts the staged file. The measured object sizes (p50 and p99) are reported.",
    "Connect's `.temp-upload/` objects (the staging step of its exactly-once commit) and `.indexes/` entries "
    "(its commit-chain bookkeeping) are counted as orphan objects and listed by name, as #746 specifies; the "
    "`.indexes/` entries are permanent by design, so Connect's orphan count has a floor.",
    "Connect's offsets are committed by its worker at `offset.flush.interval.ms` (the 60 s default), so its time "
    "to source-committed includes up to that interval; Vector commits every 5 s by default; VTOP after VERIFIED.",
    "The producer is `kafka-console-producer.sh` (acks=all, linger.ms=5) fed by the seeded generator; the produce "
    "time of a record is the generator's write time, up to 5 ms before the send. Partitions come from the key's "
    "murmur2 hash, the broker's default partitioner, checked against the partition end offsets before a tool starts.",
    "Time to store-visible (MinIO's LastModified, ms), time to source-committed (the broker's stamp on the group's "
    "offset commit) and the producer's write time share the Docker VM's clock; the window start and the kill times "
    "are the harness host's clock, which is the same clock on a Linux host.",
    "Connect runs in standalone mode (one worker, no internal topics); its sink tasks' offsets are committed to the "
    "consumer group, as in distributed mode (#746 open question 2).",
]
QUIET_SECONDS = 75  # one 60 s batch age and margin: no new object for this long is quiescence
SOURCE_CPUS, SOURCE_MEMORY = "2", "3g"


def log(message: str) -> None:
    lab.log("archive-compare", message)


# ------------------------------------------------------------------ configs


def require_config(system: str, profile: str) -> str:
    """The profile's committed configuration file; a missing one is a refused row, never a weaker stand-in."""
    rel = CONFIGS.get(system, {}).get(profile)
    if not rel or not os.path.isfile(os.path.join(REPO, rel)):
        raise runner.CellRefused(f"{system} profile {profile}: its documented-best configuration "
                                 f"({rel or 'none registered'}) cannot be written, so it is not run; no weaker "
                                 "configuration is substituted")
    return os.path.join(REPO, rel)


def config_paths(systems: list[str]) -> list[str]:
    """Every committed file a run of these systems is made of, for the drift check (rule 13)."""
    paths = ["benchmarks/archive_compare/compose.yml", "benchmarks/archive_compare/tools.json"]
    for system in systems:
        paths += list(CONFIGS[system].values()) + list(EXTRA_CONFIGS[system].values())
        if TOOLS[system].get("dockerfile"):
            paths.append(TOOLS[system]["dockerfile"])
    return paths


def extra_config_hashes(systems: list[str]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for system in systems:
        for name, rel in EXTRA_CONFIGS[system].items():
            with open(os.path.join(REPO, rel), encoding="utf-8") as handle:
                out.setdefault(system, {})[name] = schema.sha256_text(handle.read())
    return out


# ---------------------------------------------------------------- the lab


class Lab:
    """One Compose project: the source, the store, the offset sampler, one tool, and in some cells the tap."""

    def __init__(self, project: str, system: str, config: str, *, cell: str, vtop_image: str, tap: bool, logs: str,
                 cpus: str, memory: str, partitions: int, fault_args: str = ""):
        if tap:
            tap_module.refuse_in_cell(cell)  # a drain or live cell through the tap is refused (rule 17)
        self.project, self.system, self.tap, self.logs = project, system, tap, logs
        self.service = TOOLS[system]["service"]
        self.partitions = partitions
        worker = EXTRA_CONFIGS[system].get("worker")
        self.env = {"VTOP_IMAGE": vtop_image, "TOOL_CONFIG": config, "BENCH_DIR": BENCH, "RUN_LOGS": logs,
                    "WORKER_CONFIG": os.path.join(REPO, worker) if worker else "/dev/null",
                    "TOOL_CPUS": cpus, "TOOL_MEMORY": memory, "KAFKA_CPUS": SOURCE_CPUS,
                    "KAFKA_MEMORY": SOURCE_MEMORY, "KAFKA_IMAGE": KAFKA_IMAGE, "TOPIC": TOPIC,
                    "PARTITIONS": str(partitions), "STORE_ALIAS": "minio-real" if tap else "minio",
                    "DOCKER_SOCKET": "/var/run/docker.sock", "HARNESS_IMAGE": lab.HARNESS_IMAGE,
                    "FAULT_ARGS": fault_args}
        self.env.update({k: v for k, v in (("KAFKA_CPUS", os.environ.get("VTOP_SOURCE_CPUS")),
                                           ("KAFKA_MEMORY", os.environ.get("VTOP_SOURCE_MEMORY"))) if v})
        self.profiles = ["--profile", self.service] + (["--profile", "tap"] if tap else [])
        self.ports: dict[str, int] = {}
        self.polls = 0  # the harness's own listings, taken out of the store's request counts
        self.files = [COMPOSE]
        os.makedirs(logs, exist_ok=True)
        os.chmod(logs, 0o777)  # the source's tools write commits.jsonl as their own user
        subnet = os.environ.get("VTOP_LAB_SUBNET")
        if subnet:
            # A host whose Docker address pools are used up (many projects' networks) can still give
            # the lab an explicit subnet. One project runs at a time, so one subnet is enough.
            override = os.path.join(logs, "compose.subnet.yml")
            with open(override, "w", encoding="utf-8") as handle:
                handle.write(f"networks:\n  lab:\n    ipam:\n      config:\n        - subnet: \"{subnet}\"\n")
            self.files.append(override)
        self.commits_path = os.path.join(logs, "commits.jsonl")
        self.produced_path = os.path.join(logs, "produced.csv")

    def command(self, *args: str) -> list[str]:
        out = ["docker", "compose", "-p", self.project]
        for path in self.files:
            out += ["-f", path]
        return out + self.profiles + list(args)

    def compose(self, *args: str, timeout: float = 600, check: bool = True):
        return lab.sh(self.command(*args), env=self.env, timeout=timeout, check=check)

    def port(self, service: str, inner: int) -> int:
        key = f"{service}:{inner}"
        if key not in self.ports:
            out = self.compose("port", service, str(inner)).stdout.strip()
            self.ports[key] = int(out.rsplit(":", 1)[1])
        return self.ports[key]

    def up_base(self) -> None:
        self.compose("up", "-d", "--wait", "kafka", "store", timeout=900)
        self.compose("run", "--rm", "-T", "volume-init")
        self.compose("run", "--rm", "-T", "topic-init", timeout=300)
        self.compose("up", "-d", "commits")
        if self.tap:
            self.compose("up", "-d", "tap")
            _wait(lambda: self._http("tap", 9300, "/state", None, 10) is not None, 60, "the tap")
        self.mc(f"mc mb --ignore-existing lab/{BUCKET}")

    def mc(self, script: str) -> str:
        return self.compose("exec", "-T", "store", "sh", "-c", script).stdout

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

    def tap_ctl(self, path: str, body: dict | None = None) -> Any:
        answer = self._http("tap", 9300, path, body, timeout=30)
        if answer is None:
            raise RuntimeError(f"tap {path} did not answer")
        return answer

    # ---- the source and the producer

    def console_producer(self) -> list[str]:
        return self.command("exec", "-T", "-e", "KAFKA_HEAP_OPTS=-Xmx256m", "kafka",
                            "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "kafka:9092",
                            "--topic", TOPIC, "--reader-property", "parse.key=true",
                            "--reader-property", f"key.separator={producer.KEY_SEPARATOR}",
                            "--command-property", "acks=all", "--command-property", "linger.ms=5",
                            "--batch-size", "131072")

    def produce(self, producer_args: list[str], timeout: float) -> None:
        """Pipe the seeded producer (in the lab) into the console producer (in the source's container)."""
        gen_err = os.path.join(self.logs, "producer.err")
        with open(gen_err, "ab") as err:
            gen = subprocess.Popen(self.command("run", "--rm", "-T", "--no-deps", "producer", "python", "-m",
                                                "archive_compare.producer", *producer_args),
                                   stdout=subprocess.PIPE, stderr=err, env={**os.environ, **self.env})
            sink = subprocess.Popen(self.console_producer(), stdin=gen.stdout, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, env={**os.environ, **self.env})
            gen.stdout.close()
            try:
                out, _ = sink.communicate(timeout=timeout)
                gen.wait(timeout=60)
            except subprocess.TimeoutExpired:
                gen.kill()
                sink.kill()
                raise RuntimeError("the producer did not finish in time") from None
        if gen.returncode != 0 or sink.returncode != 0:
            raise RuntimeError(f"the producer failed (generator {gen.returncode}, console producer "
                               f"{sink.returncode}): {out.decode(errors='replace')[-400:]}")

    def backlog(self, seed: int, tag: str, start: int, count: int, size: int) -> None:
        self.produce(["backlog", "--seed", str(seed), "--tag", tag, "--start", str(start), "--count", str(count),
                      "--size", str(size), "--partitions", str(self.partitions)], timeout=7200)

    def end_offsets(self) -> list[int]:
        out = self.compose("exec", "-T", "-e", "KAFKA_HEAP_OPTS=-Xmx256m", "kafka",
                           "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "kafka:9092", "--topic",
                           TOPIC, timeout=120).stdout
        ends = {}
        for row in out.splitlines():
            parts = row.strip().split(":")
            if len(parts) == 3 and parts[0] == TOPIC:
                ends[int(parts[1])] = int(parts[2])
        return [ends.get(p, 0) for p in range(self.partitions)]

    # ---- the tool

    def container(self) -> str:
        return self.compose("ps", "-a", "-q", self.service).stdout.strip()

    def create_tool(self) -> str:
        """Create the tool's container without starting it, so a kill can be armed and the cgroup sampled
        before the tool can issue a single request or spend a cycle."""
        self.compose("up", "--no-start", self.service)
        return self.container()

    def start_tool(self) -> float:
        """Start the created tool; a tool that exits at start is a refused row with its own error.

        Exit 137 is SIGKILL: a K1 or K2 kill the tap fired during the first seconds, not a failed start."""
        started = time.time()
        self.compose("start", self.service)
        time.sleep(3)
        state = lab.container_exit(self.container())
        if not state["running"] and state["exit_code"] != 137:
            raise runner.CellRefused(f"{self.system} exited {state['exit_code']} at start: "
                                     f"{lab.logs_tail(self.container(), 15)}")
        return started

    def running(self) -> bool:
        name = self.container()
        return bool(name) and bool(lab.container_exit(name)["running"])

    def kill(self) -> float:
        docker_api.kill(self.container())
        return time.time()

    def restart_tool(self) -> float:
        self.compose("start", self.service)
        return time.time()

    # ---- the store

    def listing(self, prefix: str = "") -> list[dict[str, Any]]:
        self.polls += 1
        out = self.mc(f"mc ls --json --recursive lab/{BUCKET}/{prefix} || true")
        return [entry for entry in (json.loads(line) for line in out.splitlines() if line.strip())
                if entry.get("type") == "file"]

    def snapshot(self, out_dir: str) -> checker.Snapshot:
        """The whole bucket: a run has one tool and a fresh bucket (check_bucket.py)."""
        inner = "/tmp/snap"
        lines = store.snapshot_commands("", BUCKET, "", inner)
        self.mc(f"rm -rf {inner} && " + " && ".join(lines))
        self.compose("cp", f"store:{inner}/.", out_dir)
        listing = checker.read_json_lines(os.path.join(out_dir, "listing.jsonl"))
        incomplete = [entry.get("key", "") for entry in checker.read_json_lines(
            os.path.join(out_dir, "incomplete.jsonl")) if entry.get("key")]
        return checker.snapshot_from_dir(os.path.join(out_dir, "objects"), listing, incomplete)

    def request_counts(self) -> dict[str, float]:
        url = f"http://127.0.0.1:{self.port('store', 9000)}/minio/v2/metrics/cluster"
        with urllib.request.urlopen(url, timeout=30) as response:
            return store.request_counts(response.read().decode())

    def timeline(self) -> commits.Timeline:
        return commits.Timeline(commits.read(self.commits_path), TOOLS[self.system]["group"], TOPIC)

    def save_logs(self, out_dir: str) -> None:
        os.makedirs(out_dir, exist_ok=True)
        name = self.container()
        if name:
            text = lab.sh(["docker", "logs", name], check=False)
            with open(os.path.join(out_dir, f"{self.system}.log"), "w", encoding="utf-8") as handle:
                handle.write(text.stdout + text.stderr)

    def down(self) -> None:
        self.compose("down", "-v", "--remove-orphans", timeout=300, check=False)


def _wait(predicate: Callable[[], bool], seconds: float, what: str, interval: float = 1.0) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise RuntimeError(f"{what} was not ready within {seconds:.0f} s")


def _until(predicate: Callable[[], bool], seconds: float, interval: float = 0.5) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def quiesce(listing: Callable[[], list[dict[str, Any]]], running: Callable[[], bool], lag: Callable[[], int], *,
            quiet: float, timeout: float, clock: Callable[[], float] = time.time,
            sleep: Callable[[float], None] = time.sleep, poll: float = 5.0) -> list[dict[str, Any]]:
    """Wait until the group's lag is 0 and the bucket has at least one object and none new for `quiet` seconds.

    A tool that exits meanwhile is a failed row, with no retry (rule 18)."""
    deadline = clock() + timeout
    last_shape, changed = None, clock()
    while True:
        entries = listing()
        shape = (len(entries), sum(int(e.get("size", 0)) for e in entries))
        if shape != last_shape:
            last_shape, changed = shape, clock()
        remaining = lag()
        if entries and remaining == 0 and clock() - changed >= quiet:
            return entries
        if not running():
            raise runner.ToolFailed(f"the tool exited before the run reached quiescence (lag {remaining} records)")
        if clock() > deadline:
            raise runner.ToolFailed(f"no quiescence within {timeout / 60:.0f} minutes ({shape[0]} objects, "
                                    f"the group's lag {remaining} records)")
        sleep(poll)


def first_object_after(listing: list[dict[str, Any]], times: list[float]) -> str:
    """The median seconds from each restart to the first object whose LastModified follows it."""
    stamps = sorted(checker.parse_time(e["lastModified"]) for e in listing if e.get("lastModified"))
    gaps = []
    for at in times:
        later = [s for s in stamps if s >= at]
        if later:
            gaps.append(later[0] - at)
    return f"{stats.median(gaps):.1f}" if gaps else ""


def usage_end(started: float, window: float, ended: float, interval: float) -> float:
    """Where the drain cell's resource window ends: the throughput window's end, but never before two
    samples can fit in it, and never after the run."""
    return min(ended, started + max(window, 2 * interval))


def without_harness(counts: dict[str, int], harness: dict[str, int]) -> dict[str, int]:
    """The tool's request counts: the store's, less the harness's own polling, never below zero."""
    return {method: max(0, counts[method] - harness.get(method, 0)) for method in counts}


def live_sustained(offered: float, achieved: float, gaps: int) -> bool:
    """A live run sustained the offered rate only if the records were delivered too."""
    return stats.sustained(offered, achieved) and not gaps


def drain_window(started: float, last_object: float, lag_zero_at: float | None) -> float:
    """Seconds from the tool's start to the later of its last data object and the group's lag reaching 0."""
    end = max(last_object, lag_zero_at or 0.0)
    return max(end - started, 1e-3)


def live_rate(run_dir: str, item: plans.Planned, fraction: float, fixed: Any = None) -> float:
    """The live cell's offered rate: `fraction` of the SLOWEST system's median drain rate for the same record size
    in this run (#746: an offer above a tool's capacity measures its queue, not its latency; the lesson of #596).
    A plan may fix `rate` instead (a plan with no drain cell)."""
    if fixed is not None:
        return float(fixed)
    path = os.path.join(run_dir, f"{KIND.name}.csv")
    rows = schema.read_rows(path, KIND) if os.path.exists(path) else []
    by_system: dict[str, list[float]] = {}
    for row in rows:
        if (row["cell"] == "drain" and row["status"] == "ok" and row["record_size"] == str(item.variant["record_size"])
                and row["records_per_sec"]):
            by_system.setdefault(row["system"], []).append(float(row["records_per_sec"]))
    if not by_system:
        raise runner.CellRefused("no drain result for this record size in the run, so the live rate cannot be "
                                 "derived; run the drain cell first, or give the plan a fixed `rate`")
    return round(min(stats.median(v) for v in by_system.values()) * fraction, 1)


def kill_points(item: plans.Planned) -> list[kills.KillPoint]:
    p = item.params
    kind = item.cell.split("-")[1] if item.cell.startswith("crash-") else ""
    if not kind:
        return []
    return kills.schedule(int(p.get("kill_seed", 746)), kind, item.repetition, int(p.get("kills_per_run", 3)),
                          n_range=tuple(p.get("n_range", (2, 12))),
                          seconds_range=tuple(p.get("seconds_range", (5.0, 40.0))))


def uses_tap(cell: str) -> bool:
    """K1 and K2 fire on the store's requests and `verify-fault` flips a read-back, so they go through the tap; every
    other cell goes straight to the store (K3 kills at a seeded time and needs none)."""
    return cell in ("crash-K1", "crash-K2", "verify-fault")


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

    def smoke_failed(self, item: plans.Planned) -> str | None:
        """Why a system's smoke did not pass in this run, if it did not: nothing is timed or crashed before it
        passes. None when the smoke passed or this run has no smoke for the system and profile."""
        if item.cell == "smoke":
            return None
        path = os.path.join(self.run_dir, f"{KIND.name}.csv")
        if not os.path.exists(path):
            return None
        bad = [row for row in schema.read_rows(path, KIND)
               if row["cell"] == "smoke" and row["system"] == item.system and row["profile"] == item.profile
               and row["status"] != "ok"]
        if bad:
            return (f"{item.system} {item.profile} did not pass its smoke ({bad[0]['status']}, "
                    f"{bad[0]['note'][:160]}); nothing is timed or crashed before it does")
        return None

    def execute(self, item: plans.Planned) -> dict[str, Any]:
        config = require_config(item.system, item.profile)
        forbidden = KIND.forbidden(item.system, item.profile, item.cell) if KIND.forbidden else None
        if forbidden:
            raise runner.CellRefused(forbidden)
        reason = self.smoke_failed(item)
        if reason:
            raise runner.CellRefused(reason)
        self.index += 1
        key = "-".join(item.key(KIND.variant_columns))
        logs = os.path.join(self.run_dir, "logs", key)
        os.makedirs(logs, exist_ok=True)
        p = item.params
        tap = uses_tap(item.cell)
        fault_args = ""
        if item.cell == "verify-fault":
            fault_args = f"--fault-seed {int(p.get('fault_seed', 746))} --fault-one-in {int(p.get('fault_one_in', 10))}"
        project = f"vtop746-{self.tag}-{self.index}"
        tool = Lab(project, item.system, config, cell=item.cell, vtop_image=self.vtop_image, tap=tap, logs=logs,
                   cpus=self.args.tool_cpus, memory=self.args.tool_memory, partitions=int(p.get("partitions", 3)),
                   fault_args=fault_args)
        tool.down()
        try:
            tool.up_base()
            return self._cell(item, tool, logs)
        finally:
            try:
                tool.save_logs(logs)
            except (OSError, RuntimeError) as exc:  # best effort: never hide the run's own error
                log(f"could not save the logs of {project}: {exc}")
            finally:
                tool.down()

    def _wait_fired(self, tool: Lab, fired: int, timeout: float) -> bool:
        """Wait for the tap's next kill; give up when the tool has gone quiet (no new object for the quiet
        period) or the cell's timeout passes: a trigger past the tool's last PUT can never fire."""
        deadline = time.time() + timeout
        shape, changed = None, time.time()
        while time.time() < deadline:
            if len(tool.tap_ctl("/state")["fired"]) > fired:
                return True
            entries = tool.listing()
            now = (len(entries), sum(int(e.get("size", 0)) for e in entries))
            if now != shape:
                shape, changed = now, time.time()
            elif time.time() - changed >= QUIET_SECONDS:
                return False
            time.sleep(2)
        return False

    def _cell(self, item: plans.Planned, tool: Lab, logs: str) -> dict[str, Any]:
        p = item.params
        size = int(item.variant["record_size"])
        partitions = int(p.get("partitions", 3))
        seed = int(p.get("seed", 746))
        records = int(p.get("records", 0))
        timeout = float(p.get("timeout_minutes", 45)) * 60
        out: dict[str, Any] = {}
        restarts: list[float] = []
        live_cell = item.cell == "live"
        if not live_cell:
            tool.backlog(seed, self.tag, 0, records, size)
            ends = producer.counts(0, records, partitions)
            loaded = tool.end_offsets()
            if loaded != ends:
                raise RuntimeError(f"the topic holds {loaded} records per partition, the seeded producer wrote "
                                   f"{ends}: the partitioner is not the one producer.py assumes")
        else:
            ends = [0] * partitions
        points = kill_points(item)
        kind = points[0].kind if points else ""
        container = tool.create_tool()
        if kind in ("K1", "K2"):
            # Armed before the container's first life can send a request.
            tool.tap_ctl("/reset", {})
            tool.tap_ctl("/arm", {"kind": kind, "n": points[0].n, "container": container})
        before = tool.request_counts() if item.cell == "drain" else None
        sampler = cgroup.Sampler(container, docker_api.stats)
        total = records
        with sampler:
            started = tool.start_tool()
            if live_cell:
                rate = live_rate(self.run_dir, item, float(p.get("live_fraction", 0.5)), p.get("rate"))
                p = dict(p, rate=rate)
                seconds = float(p.get("warmup_seconds", 120)) + float(p.get("measure_seconds", 600))
                tool.produce(["live", "--seed", str(seed), "--tag", self.tag, "--size", str(size), "--rate",
                              str(rate), "--seconds", str(seconds), "--log", "/logs/produced.csv",
                              "--partitions", str(partitions)], timeout=seconds + 600)
                produced = commits.read_produced(tool.produced_path)
                total = len(produced)
                ends = producer.counts(0, total, partitions)
            if kind:
                life = started
                fired = 0
                for index, point in enumerate(points):
                    if kind in ("K1", "K2"):
                        if not self._wait_fired(tool, fired, timeout):
                            break  # the tool went idle before its Nth PUT: fewer kills, recorded as such
                        event = tool.tap_ctl("/state")["fired"][-1]
                        killed_at, delay = event["killed_at"], event["kill_delay_ms"]
                    else:
                        wait = life + float(point.at_seconds) - time.time()
                        if wait > 0:
                            time.sleep(wait)
                        killed_at, delay = tool.kill(), ""
                    fired += 1
                    last = lab.last_log_line(container)
                    _until(lambda: not tool.running(), 30)
                    if kind in ("K1", "K2"):
                        # The restarted life counts its PUTs from zero, and its trigger is armed before it starts.
                        tool.tap_ctl("/reset", {})
                        if index + 1 < len(points):
                            tool.tap_ctl("/arm", {"kind": kind, "n": points[index + 1].n, "container": container})
                    life = tool.restart_tool()
                    restarts.append(life)
                    self.record_kill(item, point, kind, killed_at, delay, life, last)
                if fired == 0:
                    raise runner.ToolFailed(f"no {kind} kill fired: the tool went idle before the first trigger")
                out["kills"] = fired
                if fired < len(points):
                    out["note"] = (f"{fired} of {len(points)} planned kills: the tool went idle before the next "
                                   "trigger, so this run is not comparable with runs that took every kill")
            entries = quiesce(lambda: tool.listing(), tool.running, lambda: tool.timeline().lag(ends),
                              quiet=QUIET_SECONDS, timeout=timeout)
            ended = time.time()
        after = tool.request_counts() if before is not None else None
        harness_requests = None
        if before is not None:
            # The harness's own listings (quiescence polling) hit the same store whose counters are the tool's
            # request cost. One idle listing is measured now; the polls are taken out at that price, an upper
            # bound on the harness's share (the tool's LIST count can be understated, never inflated).
            idle_before = tool.request_counts()
            tool.listing()
            tool.polls -= 1
            unit = store.requests_by_method(idle_before, tool.request_counts())
            harness_requests = {method: unit[method] * tool.polls for method in unit}
        snap = tool.snapshot(tempfile.mkdtemp(prefix="snap-", dir=logs))
        result = check_bucket.check(snap, total, self.tag)
        out.update({"records_expected": result.expected, "records_present": result.present_unique,
                    "gaps": result.gaps, "duplicates": result.duplicates, "torn_objects": result.torn_objects,
                    "orphan_objects": result.orphan_objects, "unexpected": result.unexpected})
        if restarts:
            out["seconds_to_first_object_after_restart"] = first_object_after(entries, restarts)
        timeline = tool.timeline()
        if item.cell == "smoke":
            exact, same_json, wrong = check_bucket.verify_records(snap, seed, self.tag, size, partitions)
            out["note"] = f"{exact} records byte-exact, {same_json} equal as JSON only"
            if wrong or result.gaps or result.duplicates or result.torn_objects or result.unexpected:
                raise runner.ToolFailed(
                    f"smoke: {result.gaps} gaps, {result.duplicates} duplicates, {result.torn_objects} torn, "
                    f"{result.unexpected} unexpected, {len(wrong)} records that differ from the seeded one; the "
                    "tool is not timed (sample gaps: " + ",".join(map(str, result.gap_sample[:5])) + ")")
        if item.cell == "drain":
            last_object = max(result.first_visible.values()) if result.first_visible else started
            lag_zero = [timeline.committed_at(part, end - 1) for part, end in enumerate(ends) if end]
            window = drain_window(started, last_object, max((t for t in lag_zero if t), default=None))
            source_bytes = result.present_unique * size
            usage = cgroup.window(sampler.samples, started, usage_end(started, window, ended, sampler.interval))
            gib = source_bytes / 2**30
            counts = without_harness(store.requests_by_method(before, after), harness_requests)
            per = store.per_gib(counts, source_bytes)
            sizes = result.object_sizes or [0]
            out.update({"window_seconds": f"{window:.1f}", "records_per_sec": f"{result.present_unique / window:.1f}",
                        "mib_per_sec": f"{source_bytes / 2**20 / window:.2f}", "source_bytes": source_bytes,
                        "cpu_seconds": f"{usage.cpu_seconds:.2f}",
                        "cpu_seconds_per_gib": f"{usage.cpu_seconds / gib:.2f}",
                        "peak_rss_sampled_bytes": usage.peak_rss_sampled_bytes,
                        "put_per_gib": f"{per['PUT']:.2f}", "get_per_gib": f"{per['GET']:.2f}",
                        "head_per_gib": f"{per['HEAD']:.2f}", "list_per_gib": f"{per['LIST']:.2f}",
                        "other_per_gib": f"{per['POST'] + per['DELETE'] + per['OTHER']:.2f}",
                        "objects": result.data_objects, "object_size_p50_bytes": int(stats.percentile(sizes, 50)),
                        "object_size_p99_bytes": int(stats.percentile(sizes, 99)),
                        "compression_ratio": f"{result.present_unique * (size + 1) / max(result.data_bytes, 1):.2f}"})
        if live_cell:
            warm = started + float(p.get("warmup_seconds", 120))
            measured = [row for row in produced if row[3] >= warm]
            visible = [(result.first_visible[seq] - at) * 1000 for seq, _part, _off, at in measured
                       if seq in result.first_visible]
            committed = commits.source_committed_ms(timeline, produced, after=warm)
            achieved = len(measured) / float(p.get("measure_seconds", 600))
            out.update({"offered_rate": p["rate"], "achieved_rate": f"{achieved:.1f}",
                        # A tool that lost records is not sustaining the offered rate, whatever it accepted: its
                        # latency would be ranked over the surviving records only.
                        "sustained": "true" if live_sustained(float(p["rate"]), achieved, result.gaps) else "false"})
            if visible:
                out.update({"visible_p50_ms": f"{stats.percentile(visible, 50):.0f}",
                            "visible_p99_ms": f"{stats.percentile(visible, 99):.0f}"})
            if committed:
                out.update({"committed_p50_ms": f"{stats.percentile(committed, 50):.0f}",
                            "committed_p99_ms": f"{stats.percentile(committed, 99):.0f}"})
        if item.cell == "verify-fault":
            out.update(self._fault_columns(item, tool, snap, timeline, logs, partitions, total))
        if sampler.errors:
            out["note"] = "; ".join(filter(None, (out.get("note"),
                f"{len(sampler.errors)} cgroup samples failed: {sampler.errors[0][:120]}")))
        return out

    def _fault_columns(self, item: plans.Planned, tool: Lab, snap: checker.Snapshot, timeline: commits.Timeline,
                       logs: str, partitions: int, total: int) -> dict[str, int]:
        p = item.params
        state = tool.tap_ctl("/state")
        data_keys = [key for key in snap.objects if not check_bucket.is_orphan(key) and not check_bucket.is_metadata(key)]
        selected = faults.selected_keys(data_keys, BUCKET, int(p.get("fault_seed", 746)), int(p.get("fault_one_in", 10)))
        flipped = faults.flipped_keys(state.get("corrupted", []), BUCKET)
        trace_path = os.path.join(logs, "tap.csv")
        trace = list(csv.DictReader(open(trace_path, encoding="utf-8"))) if os.path.exists(trace_path) else []  # noqa: SIM115
        reacted = faults.put_after_flip(trace, BUCKET)
        # An object is committed past when the group committed beyond every record in it.
        layout = producer.layout(total, partitions)
        committed = set()
        ids: dict[str, list[int]] = {}
        for key in data_keys:
            text = checker.decode(snap.objects[key])
            ids[key] = checker.ids_in(text, self.tag)[0] if text is not None else []
            if ids[key] and all(timeline.committed_at(*layout[seq]) is not None for seq in ids[key] if seq < total):
                committed.add(key)
        holders: dict[int, set[str]] = {}
        for key, seqs in ids.items():
            for seq in seqs:
                holders.setdefault(seq, set()).add(key)
        replayed = {key for key in selected if any(len(holders.get(seq, ())) > 1 for seq in ids.get(key, ()))}
        return faults.classify(selected, flipped, reacted | replayed, committed).columns()


# --------------------------------------------------------------------- main


def comparable_block(args: argparse.Namespace, plan: dict[str, Any], vtop_image: str, configs: dict) -> dict:
    reg = registry.load()
    images = {s: registry.product(reg, s).get("image", "") for s in plan["systems"] if s != "vtop"}
    images["vtop"] = lab.image_id(vtop_image) if vtop_image else ""
    for system in plan["systems"]:
        if TOOLS[system].get("image_tag"):
            images[system] = lab.image_id(TOOLS[system]["image_tag"])
    facts = environment.host_facts(probe_disk=not args.no_disk_probe)
    facts.update({
        "host_class": plan["host_class"],
        "vtop_commit": environment.git("rev-parse", "HEAD"),
        "vtop_engine_tree": lab.engine_tree(),
        "tool_limits": f"{args.tool_cpus} CPUs, {args.tool_memory}, no swap",
        "source_limits": f"{os.environ.get('VTOP_SOURCE_CPUS', SOURCE_CPUS)} CPUs, "
                         f"{os.environ.get('VTOP_SOURCE_MEMORY', SOURCE_MEMORY)}, no swap",
        "kafka_image": KAFKA_IMAGE,
        "images": images,
        "minio_image": lab.MINIO_IMAGE,
        "harness_image": lab.HARNESS_IMAGE,
        "jvm_flags": {s: TOOLS[s]["jvm_flags"] for s in plan["systems"] if TOOLS[s].get("jvm_flags")},
        "config_sha256": {s: {p: c["sha256"] for p, c in v.items()} for s, v in configs.items()},
        "extra_config_sha256": extra_config_hashes(plan["systems"]),
    })
    return facts


def check(run_dir: str) -> int:
    try:
        env = schema.read_environment(run_dir)
        rows = schema.read_rows(os.path.join(run_dir, f"{KIND.name}.csv"), KIND)
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


def check_hosted(plan: dict[str, Any]) -> None:
    """A hosted runner is valid for correctness metrics only (#744 section 4.4): refuse the plan before any Docker
    work, by the rule the loader checks."""
    if plan.get("host_class") != "hosted":
        return
    performance = sorted(cell["cell"] for cell in plan["cells"] if KIND.cells.get(cell["cell"]) == "performance")
    if performance:
        raise plans.PlanRefused(f"plan {plan.get('name')} runs {', '.join(performance)} on a hosted host class; "
                                "a shared runner's CPU is not a measurement host, so only correctness cells run there")


def plan_for(args: argparse.Namespace, recorded: dict[str, Any] | None) -> dict[str, Any]:
    """The plan this invocation runs. A fresh run builds it from the arguments; a resume takes the plan the
    run recorded (its name, its tool selection after `--tools`, its host class), so `--resume <dir>` needs no
    repeated arguments. An argument that was repeated and disagrees with the record is refused."""
    wanted_name = "smoke" if args.smoke else args.plan
    wanted_tools = [t.strip() for t in args.tools.split(",") if t.strip()] if args.tools else None
    if recorded is not None:
        plan = copy.deepcopy(recorded.get("plan"))
        if not isinstance(plan, dict):
            raise plans.PlanRefused("cannot resume: environment.json records no plan")
        disagree = []
        if wanted_name and wanted_name != plan.get("name"):
            disagree.append(f"--plan {wanted_name} (recorded: {plan.get('name')})")
        if wanted_tools is not None and set(wanted_tools) != set(plan.get("systems") or []):
            disagree.append(f"--tools {args.tools} (recorded: {','.join(plan.get('systems') or [])})")
        if args.host_class and args.host_class != plan.get("host_class"):
            disagree.append(f"--host-class {args.host_class} (recorded: {plan.get('host_class')})")
        if disagree:
            raise plans.PlanRefused("cannot resume with " + "; ".join(disagree) + ": a resume continues the plan "
                                    "the run recorded, so leave these arguments out")
        return plan
    plan = plans.select(plans.load_plans(os.path.join(HERE, "plans.json")), wanted_name or "smoke")
    if wanted_tools is not None:
        plan["systems"] = [s for s in plan["systems"] if s in wanted_tools]
    if args.host_class:
        plan["host_class"] = args.host_class
    return plan


def ensure_tool_images(systems: list[str], reg: dict[str, Any], rebuild: bool) -> None:
    lab.ensure_image(KAFKA_IMAGE)
    for system in systems:
        spec = TOOLS[system]
        if spec.get("dockerfile"):
            tag = spec["image_tag"]
            if rebuild or lab.sh(["docker", "image", "inspect", tag], check=False).returncode != 0:
                log(f"building {tag} from {spec['dockerfile']}")
                lab.sh(["docker", "build", "-f", os.path.join(REPO, spec["dockerfile"]), "-t", tag,
                        os.path.dirname(os.path.join(REPO, spec["dockerfile"]))], timeout=3600)
        elif system != "vtop":
            lab.ensure_image(registry.product(reg, system)["image"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--plan", default=None, help="a plan in plans.json: smoke (the default), hosted, full; a "
                        "resume reads the plan the run recorded")
    parser.add_argument("--smoke", action="store_true", help="the same as --plan smoke")
    parser.add_argument("--tools", default=None, help="comma-separated subset of the plan's systems")
    parser.add_argument("--host-class", choices=plans.HOST_CLASSES, default=None,
                        help="override the plan's host class (laptop for a smoke on a laptop)")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--resume", default=None, metavar="RUN_ID",
                        help="continue a run from its directory; its plan, tool selection and host class are "
                        "the recorded ones, so none of them is repeated")
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
    recorded = None
    if args.resume:
        resume_dir = os.path.join(args.results, lab.check_run_id(args.resume))
        try:
            recorded = schema.read_environment(resume_dir)
        except schema.RunRefused as exc:
            print(f"resume refused: {exc}", file=sys.stderr)
            return 2
        if (recorded.get("comparable") or {}).get("host_disk") == "not probed":
            args.no_disk_probe = True  # a hosted run recorded no disk probe; its resume must not add one
    try:
        plan = plan_for(args, recorded)
        plans.check_plan(plan, KIND)
        check_hosted(plan)
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
        registry.require_runnable(reg, system, private=args.private, use_case="a")
    drift = environment.config_drift(config_paths(plan["systems"]))
    if drift:
        raise SystemExit(f"tool configuration differs from HEAD ({', '.join(drift)}): commit it, so the hash the "
                         "CSV records names a file a reader can find (rule 13)")
    configs = environment.record_configs({s: dict(CONFIGS[s]) for s in plan["systems"]})
    vtop_image = lab.ensure_vtop_image(args.rebuild) if "vtop" in plan["systems"] else ""
    lab.ensure_minio_image()
    lab.ensure_image(lab.HARNESS_IMAGE)
    ensure_tool_images(plan["systems"], reg, args.rebuild)
    comparable = comparable_block(args, plan, vtop_image, configs)
    if args.resume:
        run_dir = resume_dir
        env = recorded
        try:
            runner.check_resume(env, comparable)
        except schema.RunRefused as exc:
            print(f"resume refused: {exc}", file=sys.stderr)
            return 2
    else:
        run_id = lab.check_run_id(args.run_id or lab.new_run_id("archive"))
        run_dir = os.path.join(args.results, run_id)
        os.makedirs(run_dir)
        env = environment.new_environment(KIND.name, run_id, plan, comparable, configs, telemetry={
            s: TOOLS[s]["telemetry"] for s in plan["systems"]}, private=args.private,
            extra={"deviations": DEVIATIONS})
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
