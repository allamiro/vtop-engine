#!/usr/bin/env python3
"""Run the phase-1 VTOP vs Apache Kafka matrix (#595, #596) on this Docker host.

One command, both systems, one at a time:

    python3 benchmarks/kafka_compare/run.py

For every cell of the matrix, Kafka then VTOP is brought up fresh (empty named
volumes), proven correct by a byte-exact smoke read-back, warmed up, and timed
with the SAME client container - the JVM perf tools from the pinned
`apache/kafka` image - at saturation, then at fixed rates derived from Kafka's
saturation for that cell, then for consume throughput. Each measurement is one
CSV row; a cell a system could not run is a row saying why. See README.md.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import platform
import re
import subprocess
import sys
import time
from typing import Any

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from kafka_compare import cellshape, perftool, plan, schema, stats, topology  # noqa: E402

REPO = os.path.dirname(os.path.dirname(HERE))
KAFKA_IMAGE = "apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7"
TOPIC = "bench"
PROFILE = "defaults"
# The client container's limits: identical for both systems, and larger than
# a node's so the client is not the bottleneck it measures.
CLIENT_CPUS = "4"
CLIENT_MEMORY = "4g"
CLIENT_HEAP = "-Xms1g -Xmx1g"
# Explicit on the command line for both systems; every other producer setting
# is the client's default, and the client's own dump of them is recorded.
# `compression.type` is the cell's (#596), appended per run.
PRODUCER_PROPS = ("acks=all",)
# What each profile changes on Kafka (#595 rule 4, #596 D8). VTOP's node
# configs are the same under both: it acknowledges at a quorum of 2 of 3
# whose appends are fsynced, so its rows under the second profile repeat
# the first's measurement and say so (`same_config_as`), except the
# fixed-rate rows, whose rates come from that profile's Kafka saturation.
KAFKA_FLUSH_MESSAGES = {"defaults": "9223372036854775807", "fsync-parity": "1"}
# Offset commits are off for both: the gateway without a metadata plane
# refuses OffsetCommit (a listed gap), and a consumer measurement should not
# hinge on it either way.
CONSUMER_PROPS = ("enable.auto.commit=false",)

SYSTEM_SPEC = {
    "kafka": {"compose": "compose.kafka.yml", "network": "kc-kafka-bench",
              "bootstrap": "kafka-1:9092", "leader": "kc-kafka-kafka-1-1"},
    "vtop": {"compose": "compose.vtop.yml", "network": "kc-vtop-bench",
             "bootstrap": "vtop-1:9092", "leader": "kc-vtop-vtop-1-1"},
}


def log(message: str) -> None:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%H:%M:%S")
    print(f"[kafka-compare {stamp}] {message}", flush=True)


def sh(args: list[str], *, check: bool = True, timeout: float | None = None, input_text: str | None = None,
       env: dict[str, str] | None = None, cwd: str | None = None) -> subprocess.CompletedProcess:
    merged = dict(os.environ)
    if env:
        merged.update(env)
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, input=input_text,
                            env=merged, cwd=cwd)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(args[:6])} ... exited {result.returncode}: "
                           f"{(result.stderr or result.stdout).strip()[-800:]}")
    return result


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


# ----------------------------------------------------------------------- environment


def _compose_limits(path: str) -> dict[str, str]:
    """The per-node limits a compose file declares (its first cpus/mem_limit/memswap_limit)."""
    with open(path) as handle:
        text = handle.read()
    limits = {}
    for key in ("cpus", "mem_limit", "memswap_limit"):
        match = re.search(rf"^\s+{key}:\s*(\S+)\s*$", text, re.MULTILINE)
        if not match:
            raise SystemExit(f"{path} declares no {key}: refusing an unlimited node (rule 1)")
        limits[key] = match.group(1)
    return limits


def check_equal_limits() -> dict[str, str]:
    kafka = _compose_limits(os.path.join(HERE, "compose.kafka.yml"))
    vtop = _compose_limits(os.path.join(HERE, "compose.vtop.yml"))
    if kafka != vtop:
        raise SystemExit(f"per-node limits differ (rule 1): kafka {kafka} vs vtop {vtop}")
    return kafka


def _sysctl(name: str) -> str:
    try:
        return sh(["sysctl", "-n", name]).stdout.strip()
    except (RuntimeError, FileNotFoundError):
        return ""


def host_memory_bytes() -> str:
    """Physical RAM in bytes: the macOS sysctl, else POSIX sysconf (Linux), else ""."""
    value = _sysctl("hw.memsize")
    if value:
        return value
    try:
        pages, page_size = os.sysconf("SC_PHYS_PAGES"), os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, ValueError, OSError):
        return ""
    return str(pages * page_size) if pages > 0 and page_size > 0 else ""


def volume_filesystem_command() -> list[str]:
    """`df -T` on a fresh ANONYMOUS volume. `docker run --rm` removes an
    anonymous volume with its container, so the probe never names, and never
    deletes, a volume that this run did not create."""
    return ["docker", "run", "--rm", "--mount", "type=volume,destination=/probe", "--entrypoint", "df",
            KAFKA_IMAGE, "-T", "/probe"]


def git(*args: str) -> str:
    return sh(["git", "-C", REPO, *args]).stdout.strip()


# What docker/Dockerfile compiles the image from. A change here that is not
# committed would be built into an image tagged, and recorded, as HEAD.
BUILD_INPUTS = ("Cargo.toml", "Cargo.lock", ".cargo", "crates", "docker")


def dirty_build_inputs() -> list[str]:
    status = git("status", "--porcelain", "--untracked-files=all", "--", *BUILD_INPUTS)
    return [line for line in status.splitlines() if line.strip()]


def capture_environment(run_id: str, matrix: dict[str, Any], vtop_image: str, limits: dict[str, str]) -> dict:
    info = json.loads(sh(["docker", "info", "--format", "{{json .}}"]).stdout)
    version = json.loads(sh(["docker", "version", "--format", "{{json .}}"]).stdout)
    kafka_inspect = json.loads(sh(["docker", "image", "inspect", KAFKA_IMAGE]).stdout)[0]
    vtop_inspect = json.loads(sh(["docker", "image", "inspect", vtop_image]).stdout)[0]
    java = sh(["docker", "run", "--rm", "--entrypoint", "java", KAFKA_IMAGE, "-version"]).stderr.strip()
    volume_fs = sh(volume_filesystem_command(), check=False).stdout.strip().splitlines()
    others = [name for name in sh(["docker", "ps", "--format", "{{.Names}}"]).stdout.split() if name]
    host_mem = host_memory_bytes()
    comparable = {
        "host_cpu": _sysctl("machdep.cpu.brand_string") or platform.processor(),
        "host_model": _sysctl("hw.model"),
        "host_logical_cpus": _sysctl("hw.ncpu") or str(os.cpu_count()),
        "host_memory_bytes": host_mem,
        "host_os": f"macOS {platform.mac_ver()[0]}" if platform.mac_ver()[0] else
                   f"{platform.system()} {platform.release()}",
        "docker_platform": (version.get("Server") or {}).get("Platform", {}).get("Name", ""),
        "docker_server_version": info.get("ServerVersion"),
        "docker_vm_os": info.get("OperatingSystem"),
        "docker_vm_kernel": info.get("KernelVersion"),
        "docker_vm_arch": info.get("Architecture"),
        "docker_vm_cpus": info.get("NCPU"),
        "docker_vm_memory_bytes": info.get("MemTotal"),
        "docker_storage_driver": info.get("Driver"),
        # Device and type only: the usage columns change run to run and would
        # make every run's fingerprint different.
        "volume_filesystem": " ".join(volume_fs[-1].split()[:2]) if volume_fs else "unknown",
        "node_limits": limits,
        "client_limits": {"cpus": CLIENT_CPUS, "memory": CLIENT_MEMORY, "heap": CLIENT_HEAP},
        "kafka_image": KAFKA_IMAGE,
        "kafka_image_id": kafka_inspect["Id"],
        "vtop_commit": git("rev-parse", "HEAD"),
        "vtop_engine_tree": git("rev-parse", "HEAD:crates"),
        "vtop_image": vtop_image,
        "vtop_image_id": vtop_inspect["Id"],
        "client": client_name(),
        "client_java": java.splitlines()[0] if java else "",
        "profile": PROFILE,
        "profiles": list(matrix.get("profiles") or [PROFILE]),
        "kafka_flush_messages": {k: KAFKA_FLUSH_MESSAGES[k] for k in matrix.get("profiles") or [PROFILE]},
        "producer_props": list(PRODUCER_PROPS),
        "consumer_props": list(CONSUMER_PROPS),
    }
    return {
        "run_id": run_id,
        "run_state": "running",
        "started_at": utc_now(),
        "label": "single host, Docker Desktop; indicative, not a production benchmark",
        "comparable": comparable,
        "matrix": matrix,
        "worktree_dirty": bool(git("status", "--porcelain")),
        # A count, not names: the names are other people's projects and do not
        # belong in a committed result; the count says the VM was not idle.
        "other_containers_at_start_count": len(others),
        "configs": {
            "kafka_compose": _read(os.path.join(HERE, "compose.kafka.yml")),
            "vtop_compose": _read(os.path.join(HERE, "compose.vtop.yml")),
            "vtop_leader": _read(os.path.join(HERE, "vtop", "leader.yaml")),
            "vtop_follower_1": _read(os.path.join(HERE, "vtop", "follower-1.yaml")),
            "vtop_follower_2": _read(os.path.join(HERE, "vtop", "follower-2.yaml")),
        },
        "observed": {},
        "events": [],
    }


def _read(path: str) -> str:
    with open(path) as handle:
        return handle.read()


def client_name() -> str:
    tag = KAFKA_IMAGE.split("@")[0].split(":")[-1]
    return f"kafka-producer-perf-test/kafka-consumer-perf-test {tag} (JVM)"


def save_environment(run_dir: str, environment: dict) -> None:
    path = os.path.join(run_dir, "environment.json")
    tmp = path + ".tmp"
    with open(tmp, "w") as handle:
        json.dump(environment, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(tmp, path)


# --------------------------------------------------------------------------- clusters


# The cell a cluster is brought up for; `compose down` needs the same
# variables the `up` was given, so they are kept here between the two.
_compose_env: dict[str, str] = {"VTOP_CONFIG_DIR": os.path.join(HERE, "vtop"),
                                "KAFKA_FLUSH_MESSAGES": KAFKA_FLUSH_MESSAGES["defaults"]}


def compose(system: str, *args: str, vtop_image: str, timeout: float = 600) -> subprocess.CompletedProcess:
    spec = SYSTEM_SPEC[system]
    return sh(["docker", "compose", "-f", os.path.join(HERE, spec["compose"]), *args],
              env={"VTOP_IMAGE": vtop_image, "KAFKA_IMAGE": KAFKA_IMAGE, **_compose_env}, timeout=timeout,
              cwd=HERE)


def cluster_down(system: str, vtop_image: str) -> None:
    compose(system, "down", "-v", "--remove-orphans", vtop_image=vtop_image)


def vtop_config_dir(ctx: dict) -> str:
    """The cell's generated VTOP configs, written once per partition count
    into the run directory, their hashes recorded."""
    partitions = ctx["cell"]["partitions"]
    directory = os.path.join(ctx["run_dir"], "vtop-config", f"p{partitions}")
    if not os.path.isdir(directory):
        hashes = topology.write(topology.vtop_configs(partitions), directory)
        ctx["env"]["observed"].setdefault("vtop_topology", {})[str(partitions)] = {
            "leaders": topology.leader_map(partitions), "config_sha256": hashes}
    return directory


def cluster_up(ctx: dict, system: str) -> None:
    cell = ctx["cell"]
    cluster_down(system, ctx["vtop_image"])
    if system == "vtop":
        _compose_env["VTOP_CONFIG_DIR"] = vtop_config_dir(ctx)
    else:
        _compose_env["KAFKA_FLUSH_MESSAGES"] = KAFKA_FLUSH_MESSAGES[cell["profile"]]
    compose(system, "up", "-d", "--wait", "--wait-timeout", "300", vtop_image=ctx["vtop_image"], timeout=400)
    if system == "kafka":
        command = ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", SYSTEM_SPEC[system]["bootstrap"],
                   "--create", "--topic", TOPIC, "--partitions", str(cell["partitions"]),
                   "--replication-factor", "3", "--config", "min.insync.replicas=2"]
        if cell["profile"] == "fsync-parity":
            command += ["--config", "flush.messages=1"]
        client(system, command, timeout=120)


_client_seq = 0


def client(system: str, command: list[str], *, timeout: float, input_text: str | None = None,
           info_logging: bool = False, check: bool = True) -> subprocess.CompletedProcess:
    """One client invocation in its own container on the system's network."""
    global _client_seq
    _client_seq += 1
    name = f"kc-client-{os.getpid()}-{_client_seq}"
    env_args = ["-e", f"KAFKA_HEAP_OPTS={CLIENT_HEAP}"]
    if info_logging:
        env_args += ["-e", "KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=/opt/kafka/config/log4j2.yaml"]
    args = ["docker", "run", "--rm", "--name", name, "--network", SYSTEM_SPEC[system]["network"],
            "--cpus", CLIENT_CPUS, "--memory", CLIENT_MEMORY, *env_args]
    if input_text is not None:
        args.append("-i")
    args += [KAFKA_IMAGE, *command]
    try:
        return sh(args, timeout=timeout, input_text=input_text, check=check)
    except subprocess.TimeoutExpired:
        sh(["docker", "rm", "-f", name], check=False)
        raise


# ------------------------------------------------------------------------------ smoke


def smoke(ctx: dict, system: str, count: int, size: int) -> None:
    """The smoke gate in the cell's own shape (#596): `count` keyed records of
    the cell's size, compression and partition count, produced with acks=all
    and read back partition by partition through the same client. Raises
    unless every record comes back exactly once, each partition in produced
    order, with the CreateTime it was produced with (cellshape.check_smoke)."""
    cell = ctx["cell"]
    records = cellshape.smoke_records(count, size)
    bootstrap = SYSTEM_SPEC[system]["bootstrap"]
    started = int(time.time() * 1000)
    client(system, ["/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", bootstrap, "--topic", TOPIC,
                    "--compression-codec", cell["compression"],
                    "--reader-property", "parse.key=true", "--reader-property", "key.separator=\t",
                    "--command-property", "acks=all"],
                timeout=300, input_text="".join(f"{key}\t{value}\n" for key, value in records))
    finished = int(time.time() * 1000)
    reads = []
    for partition in range(cell["partitions"]):
        read = client(system, ["/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", bootstrap,
                               "--topic", TOPIC, "--partition", str(partition), "--offset", "earliest",
                               "--timeout-ms", "15000", "--property", "print.timestamp=true",
                               "--property", "print.key=true"], timeout=300, check=False)
        reads += cellshape.parse_smoke_read(partition, read.stdout)
    # A second either side: the client's clock and this process's are the
    # same host's, read a few milliseconds apart.
    cellshape.check_smoke(records, reads, cell["partitions"], (started - 1000, finished + 1000))


# ------------------------------------------------------------------------------- runs


def producer_command(system: str, records: int, size: int, rate: int, warmup: int,
                     compression: str = "none") -> list[str]:
    return ["/opt/kafka/bin/kafka-producer-perf-test.sh", "--bootstrap-server", SYSTEM_SPEC[system]["bootstrap"],
            "--topic", TOPIC, "--num-records", str(records), "--record-size", str(size), "--throughput", str(rate),
            "--warmup-records", str(warmup), "--command-property", *PRODUCER_PROPS,
            f"compression.type={compression}"]


def run_producer(ctx: dict, system: str, records: int, size: int, rate: int, timeout: float,
                 info_logging: bool = False) -> tuple[perftool.ProducerResult | None, str, float, str]:
    """(result or None, note, wall seconds, raw output). With N producers in
    the cell, N tool processes run at once in the one client container, each
    sending records/N at rate/N (#596 D11), combined by `cellshape.combine`;
    the row's `lat_basis` is then `worst-of-N` (set by the caller)."""
    producers = ctx["cell"]["producers"]
    compression = ctx["cell"]["compression"]
    if producers > 1:
        return _run_producers(ctx, system, records, size, rate, timeout)
    warmup = max(1, records // 5)
    started = time.monotonic()
    try:
        done = client(system, producer_command(system, records, size, rate, warmup, compression), timeout=timeout,
                      check=False, info_logging=info_logging)
    except subprocess.TimeoutExpired:
        return None, f"timed out after {int(timeout)} s", time.monotonic() - started, ""
    wall = time.monotonic() - started
    output = done.stdout + "\n" + done.stderr
    errors = sorted({line.strip()[:200] for line in output.splitlines()
                     if "Exception" in line and not line.strip().startswith("at ")})
    try:
        result = perftool.parse_producer(done.stdout, expect_steady_state=True)
    except perftool.PerfParseError as exc:
        detail = "; ".join(errors[:2]) or output.strip()[-300:]
        return None, f"{exc}; exit {done.returncode}; {detail}", wall, output
    if done.returncode != 0 or errors:
        return None, f"exit {done.returncode}; {'; '.join(errors[:2])}", wall, output
    return result, "", wall, output


def _run_producers(ctx: dict, system: str, records: int, size: int, rate: int,
                   timeout: float) -> tuple[perftool.ProducerResult | None, str, float, str]:
    producers = ctx["cell"]["producers"]
    each = max(1, records // producers)
    each_rate = rate if rate < 0 else max(1, rate // producers)
    commands = [producer_command(system, each, size, each_rate, max(1, each // 5), ctx["cell"]["compression"])
                for _ in range(producers)]
    started = time.monotonic()
    try:
        done = client(system, ["sh", "-c", cellshape.multi_producer_script(commands)], timeout=timeout,
                      check=False)
    except subprocess.TimeoutExpired:
        return None, f"timed out after {int(timeout)} s", time.monotonic() - started, ""
    wall = time.monotonic() - started
    output = done.stdout + "\n" + done.stderr
    try:
        parts = cellshape.split_outputs(done.stdout, producers)
        results = []
        for index, (code, text) in enumerate(parts):
            if code != 0:
                return None, f"producer {index} of {producers} exited {code}", wall, output
            results.append(perftool.parse_producer(text, expect_steady_state=True))
    except perftool.PerfParseError as exc:
        return None, f"{exc}; exit {done.returncode}", wall, output
    return cellshape.combine(results).result, "", wall, output


def base_row(ctx: dict, system: str, mode: str, size: int, rep: int) -> dict[str, Any]:
    cell = ctx["cell"]
    return {
        "run_id": ctx["run_id"], "env_fingerprint": ctx["fingerprint"],
        "vtop_commit": ctx["env"]["comparable"]["vtop_commit"], "kafka_image": KAFKA_IMAGE,
        "client": client_name(), "profile": cell["profile"], "system": system, "mode": mode,
        "record_size": size, "partitions": cell["partitions"], "producers": cell["producers"],
        "compression": cell["compression"], "acks": "all",
        "repetition": rep, "load_basis": "kafka" if mode == "fixed_rate" else "",
        "started_at": utc_now(),
    }


def emit(ctx: dict, row: dict[str, Any]) -> None:
    """Write one row, and remember it for the copies a shared configuration
    owes under the other profiles."""
    schema.append_row(ctx["csv"], row)
    ctx.setdefault("emitted", []).append(dict(row))


def producer_row(row: dict, result: perftool.ProducerResult | None, note: str, wall: float) -> dict:
    row["wall_seconds"] = round(wall, 1)
    if result is None:
        row.update(status="failed", note=note)
        return row
    row.update(status="ok", records=result.records, records_per_sec=result.records_per_sec,
               mib_per_sec=result.mib_per_sec, lat_avg_ms=result.lat_avg_ms, lat_max_ms=result.lat_max_ms,
               lat_p50_ms=result.lat_p50_ms, lat_p95_ms=result.lat_p95_ms, lat_p99_ms=result.lat_p99_ms,
               lat_p999_ms=result.lat_p999_ms,
               lat_basis="single" if row["producers"] == 1 else f"worst-of-{row['producers']}", note=note)
    return row


def refuse_rows(ctx: dict, system: str, size: int, reason: str, load_pcts: list[int]) -> None:
    """A row for every measurement of this system's cell, saying why it did not run."""
    for rep in range(1, ctx["reps"] + 1):
        modes = [("saturation", None)] + [("fixed_rate", pct) for pct in load_pcts]
        if ctx["consume"]:
            modes.append(("consume", None))
        for mode, pct in modes:
            row = base_row(ctx, system, mode, size, rep)
            row.update(status="refused", note=reason, load_pct=pct)
            emit(ctx, row)


def cell_tag(ctx: dict, system: str, size: int) -> str:
    cell = ctx["cell"]
    return (f"{system}-{cell['profile']}-p{cell['partitions']}-n{cell['producers']}-{cell['compression']}"
            f"-{size}B")


def repeat_under(ctx: dict, rows: list[dict[str, Any]], profile: str, measured_under: str) -> None:
    """Write `rows` again under `profile`, each naming the profile it was
    measured under (#596 decision 2: VTOP's configuration is the same under
    both, so its saturation and consume runs are made once)."""
    for row in rows:
        emit(ctx, {**row, "profile": profile, "same_config_as": measured_under})


def run_vtop_cell(ctx: dict, size: int, rates_by_profile: dict[str, dict[int, int] | None]) -> None:
    """VTOP in one cell, its cluster brought up once for every profile.

    Saturation and consume are measured once, under the first profile, and
    repeated under the others with `same_config_as`. The fixed-rate rows are
    measured for every profile, because their offered rates are a percentage
    of THAT profile's Kafka saturation.
    """
    profiles = list(rates_by_profile)
    first = profiles[0]
    ctx["cell"]["profile"] = first
    mark = len(ctx.get("emitted", []))
    run_cell(ctx, "vtop", size, rates_by_profile[first], extra_rates={p: rates_by_profile[p] for p in profiles[1:]})
    measured = [row for row in ctx["emitted"][mark:]
                if row["profile"] == first and row["mode"] in ("saturation", "consume")]
    for profile in profiles[1:]:
        repeat_under(ctx, measured, profile, first)
    ctx["cell"]["profile"] = first


def run_cell(ctx: dict, system: str, size: int, kafka_rates: dict[int, int] | None,
             extra_rates: dict[str, dict[int, int] | None] | None = None) -> dict[int, int] | None:
    """Every measurement of one system in one cell. Returns Kafka's derived
    rates. `extra_rates` (VTOP only) are further profiles' Kafka rates, run as
    fixed-rate rows in the same cluster after the first profile's."""
    sat_records = ctx["sat_records"][size]
    load_pcts = list(stats.LOAD_FRACTIONS)
    logs = os.path.join(ctx["run_dir"], "logs")
    os.makedirs(logs, exist_ok=True)
    tag = cell_tag(ctx, system, size)
    try:
        log(f"{tag}: bringing the cluster up fresh")
        cluster_up(ctx, system)
        log(f"{tag}: smoke - {ctx['smoke_records']} records of {size} B, produced and read back byte-exact")
        smoke(ctx, system, ctx["smoke_records"], size)
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        reason = f"cluster refused before timing: {exc}"[:900]
        log(f"{tag}: {reason}")
        ctx["env"]["events"].append({"at": utc_now(), "system": system, "record_size": size, "event": reason})
        _save_leader_log(system, logs, tag, ctx["vtop_image"])
        refuse_rows(ctx, system, size, reason, load_pcts)
        first = ctx["cell"]["profile"]
        for profile in extra_rates or {}:
            ctx["cell"]["profile"] = profile
            refuse_rows(ctx, system, size, reason, load_pcts)
        ctx["cell"]["profile"] = first
        cluster_down(system, ctx["vtop_image"])
        return None
    ctx["env"]["events"].append({"at": utc_now(), "system": system, "record_size": size,
                                 "event": f"smoke passed: {ctx['smoke_records']} records byte-exact"})
    if system == "kafka" and "kafka_topic" not in ctx["env"]["observed"]:
        _observe_kafka(ctx)

    # Unrecorded warm-up run: JIT, page cache, segment creation on both sides.
    log(f"{tag}: warm-up run (not recorded)")
    result, note, _, output = run_producer(ctx, system, max(sat_records // 4, 1000), size, -1, ctx["sat_timeout"],
                                           info_logging=True)
    try:
        ctx["env"]["observed"].setdefault("producer_config", {})[f"{system}-{size}"] = \
            perftool.parse_client_config(output)
    except perftool.PerfParseError as exc:
        ctx["env"]["events"].append({"at": utc_now(), "system": system, "event": f"producer config: {exc}"})
    with open(os.path.join(logs, f"{tag}-warmup.log"), "w") as handle:
        handle.write(output)

    saturation = []
    for rep in range(1, ctx["reps"] + 1):
        log(f"{tag}: saturation rep {rep}/{ctx['reps']} ({sat_records} records)")
        row = base_row(ctx, system, "saturation", size, rep)
        result, note, wall, output = run_producer(ctx, system, sat_records, size, -1, ctx["sat_timeout"])
        _save_output(logs, f"{tag}-saturation-{rep}.log", output)
        if result is not None:
            saturation.append(result.records_per_sec)
        emit(ctx, producer_row(row, result, note, wall))
        log(f"{tag}: -> {row.get('records_per_sec', row.get('note'))} records/s")

    if system == "kafka":
        try:
            kafka_rates = stats.derive_offered_rates(saturation)
        except ValueError as exc:
            kafka_rates = None
            ctx["env"]["events"].append({"at": utc_now(), "event": f"no offered rates for {size} B: {exc}"})
        ctx["env"]["observed"].setdefault("offered_rates", {})[tag] = kafka_rates
    first_profile = ctx["cell"]["profile"]
    for profile, rates in [(first_profile, kafka_rates), *(extra_rates or {}).items()]:
        ctx["cell"]["profile"] = profile
        run_fixed_rates(ctx, system, size, rates, load_pcts, logs, cell_tag(ctx, system, size))
    ctx["cell"]["profile"] = first_profile
    run_consume(ctx, system, size, sat_records, logs, tag)
    _save_leader_log(system, logs, tag, ctx["vtop_image"])
    cluster_down(system, ctx["vtop_image"])
    return kafka_rates


def run_fixed_rates(ctx: dict, system: str, size: int, kafka_rates: dict[int, int] | None, load_pcts: list[int],
                    logs: str, tag: str) -> None:
    for rep in range(1, ctx["reps"] + 1):
        for pct in load_pcts:
            row = base_row(ctx, system, "fixed_rate", size, rep)
            row["load_pct"] = pct
            if not kafka_rates:
                row.update(status="refused", note="no Kafka saturation for this cell to derive the offered rate from")
                emit(ctx, row)
                continue
            rate = kafka_rates[pct]
            records = stats.records_for_duration(rate, ctx["fixed_seconds"])
            row["offered_rate"] = rate
            log(f"{tag}: fixed rate {pct}% = {rate} records/s, rep {rep}/{ctx['reps']}")
            timeout = max(ctx["sat_timeout"], 6 * records / rate)
            result, note, wall, output = run_producer(ctx, system, records, size, rate, timeout)
            _save_output(logs, f"{tag}-fixed-{pct}-{rep}.log", output)
            if result is not None:
                row["sustained"] = stats.sustained(rate, result.records_per_sec)
            emit(ctx, producer_row(row, result, note, wall))
            log(f"{tag}: -> p50 {row.get('lat_p50_ms')} ms p99 {row.get('lat_p99_ms')} ms "
                f"achieved {row.get('records_per_sec')} sustained={row.get('sustained')}")


def run_consume(ctx: dict, system: str, size: int, sat_records: int, logs: str, tag: str) -> None:
    if ctx["consume"]:
        for rep in range(1, ctx["reps"] + 1):
            row = base_row(ctx, system, "consume", size, rep)
            log(f"{tag}: consume rep {rep}/{ctx['reps']} ({sat_records} records from the start of the log)")
            started = time.monotonic()
            command = ["/opt/kafka/bin/kafka-consumer-perf-test.sh", "--bootstrap-server",
                       SYSTEM_SPEC[system]["bootstrap"], "--topic", TOPIC, "--num-records", str(sat_records),
                       "--group", f"kc-{ctx['run_id']}-{tag}-{rep}", "--timeout", "60000",
                       "--command-property", *CONSUMER_PROPS]
            try:
                done = client(system, command, timeout=ctx["sat_timeout"], check=False)
                _save_output(logs, f"{tag}-consume-{rep}.log", done.stdout + done.stderr)
                # A summary printed by a tool that then failed is not a
                # measurement, as on the producer path.
                if done.returncode != 0:
                    raise perftool.PerfParseError("the consumer perf tool exited nonzero")
                result = perftool.parse_consumer(done.stdout, sat_records)
                row.update(status="ok", records=result.records, records_per_sec=result.records_per_sec,
                           mib_per_sec=result.mib_per_sec,
                           note=f"rebalance {result.rebalance_ms:.0f} ms excluded; fetch {result.fetch_ms:.0f} ms")
            except subprocess.TimeoutExpired:
                row.update(status="failed", note=f"timed out after {ctx['sat_timeout']} s")
            except perftool.PerfParseError as exc:
                row.update(status="failed", note=f"{exc}; exit {done.returncode}")
            row["wall_seconds"] = round(time.monotonic() - started, 1)
            emit(ctx, row)
            log(f"{tag}: -> {row.get('records_per_sec', row.get('note'))} records/s consumed")


def _save_output(logs: str, name: str, output: str) -> None:
    with open(os.path.join(logs, name), "w") as handle:
        handle.write(output)


def _save_leader_log(system: str, logs: str, tag: str, vtop_image: str) -> None:
    done = sh(["docker", "logs", SYSTEM_SPEC[system]["leader"]], check=False)
    text = done.stdout + done.stderr
    # Kafka's broker log is long and routine; keep its tail. VTOP's leader log
    # is where gateway refusals are named, so keep it whole.
    if system == "kafka":
        text = "\n".join(text.splitlines()[-400:])
    _save_output(logs, f"{tag}-node1.log", text)


def _observe_kafka(ctx: dict) -> None:
    bootstrap = SYSTEM_SPEC["kafka"]["bootstrap"]
    topic = client("kafka", ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", bootstrap, "--describe",
                             "--topic", TOPIC], timeout=120, check=False).stdout.strip()
    broker = client("kafka", ["/opt/kafka/bin/kafka-configs.sh", "--bootstrap-server", bootstrap, "--describe",
                              "--all", "--entity-type", "brokers", "--entity-name", "1"],
                    timeout=120, check=False).stdout
    wanted = re.compile(r"^\s+(log\.flush\.[a-z.]+|flush\.[a-z.]+|min\.insync\.replicas|default\.replication\.factor|"
                        r"replica\.lag\.time\.max\.ms|num\.(network|io|replica\.fetchers)[a-z.]*|"
                        r"log\.segment\.bytes|message\.max\.bytes|unclean\.leader\.election\.enable|"
                        r"broker\.session\.timeout\.ms|group\.initial\.rebalance\.delay\.ms|"
                        r"socket\.[a-z.]+|replica\.fetch\.[a-z.]+|log\.message\.timestamp\.type)=(\S*)")
    settings = {}
    for line in broker.splitlines():
        match = wanted.match(line)
        if match:
            settings[match.group(1)] = match.group(3)
    ctx["env"]["observed"]["kafka_topic"] = topic
    ctx["env"]["observed"]["kafka_broker_1_config"] = settings


# ------------------------------------------------------------------------------- main


def ensure_vtop_image(rebuild: bool) -> str:
    """The image of HEAD's engine, built if absent or asked for.

    Refuses when the build inputs differ from HEAD: an existing image would be
    stale, and a rebuilt one would put uncommitted code under HEAD's tag and
    in the results as HEAD's commit. Commit (or stash) first.
    """
    dirty = dirty_build_inputs()
    if dirty:
        raise SystemExit(f"the engine's build inputs differ from HEAD ({'; '.join(dirty[:5])}): "
                         "commit or stash them, so the image benchmarked is the commit recorded")
    sha = git("rev-parse", "--short=12", "HEAD")
    image = f"vtop-bench:{sha}"
    exists = sh(["docker", "image", "inspect", image], check=False).returncode == 0
    if rebuild or not exists:
        log(f"building {image} from docker/Dockerfile (both binaries)")
        sh(["docker", "build", "-f", os.path.join(REPO, "docker", "Dockerfile"), "-t", image, REPO], timeout=3600)
    return image


def parse_sat_records(text: str, sizes: list[int]) -> dict[int, int]:
    """Records per saturation and consume run, for each of `sizes`.

    A size given twice is refused (which count would run is a guess). A size
    not in `sizes` is dropped, not refused, so `--sizes 100` works with the
    default table; either way the recorded matrix holds only what runs.
    """
    table = {}
    for item in text.split(","):
        size, _, count = item.partition(":")
        try:
            size_n, count_n = int(size), int(count)
        except ValueError:
            raise SystemExit(f"--sat-records entry {item!r} is not SIZE:COUNT") from None
        if count_n < 1:
            raise SystemExit(f"--sat-records gives {count_n} records for {size_n} B: must be positive")
        if size_n in table:
            raise SystemExit(f"--sat-records gives record size {size_n} B twice")
        table[size_n] = count_n
    missing = [size for size in sizes if size not in table]
    if missing:
        raise SystemExit(f"--sat-records names no count for record sizes {missing}")
    return {size: table[size] for size in sizes}


def parse_sizes(text: str) -> list[int]:
    try:
        sizes = [int(size) for size in text.split(",")]
    except ValueError:
        raise SystemExit(f"--sizes {text!r} is not a comma-separated list of byte counts") from None
    if any(size < 1 for size in sizes) or len(set(sizes)) != len(sizes):
        raise SystemExit(f"--sizes {text!r}: record sizes must be positive and distinct")
    return sizes


# One path component: a run id names a directory under results/, never a path.
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def check_run_id(run_id: str) -> str:
    if not _RUN_ID.match(run_id) or ".." in run_id:
        raise SystemExit(f"--run-id {run_id!r} must be one path component of letters, digits, '.', '_' or '-'")
    return run_id


def check_args(args: argparse.Namespace) -> tuple[list[int], dict[int, int]]:
    """Every matrix argument, refused up front before any Docker work."""
    sizes = parse_sizes(args.sizes)
    sat_records = parse_sat_records(args.sat_records, sizes)
    if args.reps < 1:
        raise SystemExit(f"--reps {args.reps}: at least one repetition is needed")
    for name, value in (("--fixed-seconds", args.fixed_seconds), ("--sat-timeout", args.sat_timeout)):
        if not math.isfinite(value) or value <= 0:
            raise SystemExit(f"{name} {value}: must be a positive, finite number of seconds")
    if args.smoke_records < 1:
        raise SystemExit(f"--smoke-records {args.smoke_records}: the smoke must read back at least one record")
    if args.run_id is not None:
        check_run_id(args.run_id)
    return sizes, sat_records


def parse_list(text: str, flag: str, allowed: tuple[str, ...]) -> list[str]:
    values = [v.strip() for v in text.split(",")]
    if not values or len(set(values)) != len(values) or any(v not in allowed for v in values):
        raise SystemExit(f"{flag} {text!r}: distinct values from {', '.join(allowed)}")
    return values


def parse_counts(text: str, flag: str, ceiling: int) -> list[int]:
    try:
        values = [int(v) for v in text.split(",")]
    except ValueError:
        raise SystemExit(f"{flag} {text!r} is not a comma-separated list of counts") from None
    if len(set(values)) != len(values) or any(not 1 <= v <= ceiling for v in values):
        raise SystemExit(f"{flag} {text!r}: distinct counts from 1 to {ceiling}")
    return values


def build_matrix(args: argparse.Namespace, sizes: list[int], sat_records: dict[int, int]) -> dict[str, Any]:
    """The matrix this invocation runs, as environment.json records it. The
    schema-2 axes say what the phase-1 shape does: one compression, one
    profile, Kafka-relative rates."""
    load_pcts = list(stats.LOAD_FRACTIONS)
    profiles = parse_list(args.profiles, "--profiles", plan.PROFILES)
    return {"record_sizes": sizes, "partitions": parse_counts(args.partitions, "--partitions", 9),
            "producers": parse_counts(args.producers, "--producers", 16),
            "compressions": parse_list(args.compressions, "--compressions", plan.COMPRESSIONS),
            "acks": ["all"], "profile": profiles[0], "profiles": profiles,
            # VTOP's node configs are the same under every profile (#596 D8),
            # so its saturation and consume runs are made once per cell.
            "vtop_profiles_share_config": len(profiles) > 1, "repetitions": args.reps,
            "load_pcts": load_pcts, "load_bases": {"kafka": load_pcts}, "consume": not args.no_consume,
            "sat_records": {str(k): v for k, v in sat_records.items()},
            "fixed_seconds": args.fixed_seconds, "warmup_fraction": 0.2, "smoke_records": args.smoke_records}


def check_plan(matrix: dict[str, Any], max_hours: float) -> float:
    """The projected hours, or a refusal before any Docker work."""
    if not math.isfinite(max_hours) or max_hours <= 0:
        raise SystemExit(f"--max-hours {max_hours}: must be a positive, finite number of hours")
    try:
        plan.check_matrix(matrix)
        return plan.check_budget(matrix, max_hours)
    except plan.PlanRefused as exc:
        raise SystemExit(f"refused before any Docker work: {exc}") from None


def check(run_dir: str) -> int:
    """`--check`: what a run directory still lacks. 0 when every expected row
    is there once and nothing else is; 2 otherwise, listing the missing keys."""
    try:
        found = schema.check_run(run_dir)
    except schema.ReportRefused as exc:
        print(f"check refused: {exc}", file=sys.stderr)
        return 2
    if found.duplicates or found.extra:
        print(f"{len(found.duplicates)} duplicated and {len(found.extra)} unexpected row keys, "
              f"first {(found.duplicates + found.extra)[:3]}", file=sys.stderr)
        return 2
    present = len(found.expected) - len(found.missing)
    print(f"{run_dir}: {present} of {len(found.expected)} expected rows present")
    if found.missing:
        print(f"missing {len(found.missing)} (key {', '.join(schema.KEY)}):", file=sys.stderr)
        for key in found.missing:
            print("  " + " ".join(str(part) if part != "" else "-" for part in key), file=sys.stderr)
        return 2
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--sizes", default="100,1024", help="record sizes in bytes")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--sat-records", default="100:3000000,1024:1000000",
                        help="records per saturation run, per record size")
    parser.add_argument("--fixed-seconds", type=float, default=30.0, help="target length of a fixed-rate run")
    parser.add_argument("--smoke-records", type=int, default=10000)
    parser.add_argument("--sat-timeout", type=float, default=1800.0, help="seconds before a run is failed")
    parser.add_argument("--no-consume", action="store_true")
    parser.add_argument("--rebuild", action="store_true", help="rebuild the vtop image even if it exists")
    parser.add_argument("--partitions", default="1", help="partition counts, e.g. 1,6 (VTOP: one range each)")
    parser.add_argument("--producers", default="1", help="producer processes per cell, e.g. 1,4")
    parser.add_argument("--compressions", default="none", help="producer codecs, e.g. none,zstd")
    parser.add_argument("--profiles", default=PROFILE, help="durability profiles: defaults,fsync-parity")
    parser.add_argument("--max-hours", type=float, default=30.0,
                        help="refuse a matrix projected to take longer, before any Docker work")
    parser.add_argument("--check", metavar="RUN_DIR", default=None,
                        help="report which rows a run still lacks; exit 0 when complete, 2 otherwise")
    args = parser.parse_args(argv)

    if args.check is not None:
        return check(args.check)
    sizes, sat_records = check_args(args)
    matrix = build_matrix(args, sizes, sat_records)
    hours = check_plan(matrix, args.max_hours)
    log(f"{len(schema.expected_keys(matrix))} result rows, projected {hours:.1f} h (--max-hours {args.max_hours:g})")
    if args.reps < 3:
        log(f"warning: {args.reps} repetitions is below #595 rule 7's minimum of 3")
    limits = check_equal_limits()
    for system in SYSTEM_SPEC:
        running = sh(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project=kc-{system}"]).stdout
        if running.strip():
            raise SystemExit(f"kc-{system} is already running; stop it first (one system at a time, rule 1)")
    sh(["docker", "pull", "-q", KAFKA_IMAGE], timeout=1800)
    vtop_image = ensure_vtop_image(args.rebuild)

    run_id = args.run_id or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = os.path.join(HERE, "results", run_id)
    if os.path.exists(run_dir):
        raise SystemExit(f"{run_dir} exists; runs never overwrite each other")
    os.makedirs(run_dir)
    env = capture_environment(run_id, matrix, vtop_image, limits)
    ctx = {"cell": {"partitions": matrix["partitions"][0], "producers": matrix["producers"][0],
                    "compression": matrix["compressions"][0], "profile": matrix["profiles"][0]},
           "run_id": run_id, "run_dir": run_dir, "csv": os.path.join(run_dir, "results.csv"), "env": env,
           "fingerprint": schema.fingerprint(env), "vtop_image": vtop_image, "reps": args.reps,
           "sat_records": sat_records, "fixed_seconds": args.fixed_seconds,
           "smoke_records": args.smoke_records, "sat_timeout": args.sat_timeout, "consume": not args.no_consume}
    schema.write_header(ctx["csv"])
    save_environment(run_dir, env)
    log(f"run {run_id} -> {run_dir} (fingerprint {ctx['fingerprint']})")
    try:
        for cell in plan.axes(matrix).cells:
            ctx["cell"] = {"partitions": cell.partitions, "producers": cell.producers,
                           "compression": cell.compression, "profile": matrix["profiles"][0]}
            rates = {}
            for profile in matrix["profiles"]:
                ctx["cell"]["profile"] = profile
                rates[profile] = run_cell(ctx, "kafka", cell.record_size, None)
                save_environment(run_dir, env)
            run_vtop_cell(ctx, cell.record_size, rates)
            save_environment(run_dir, env)
    except BaseException as exc:
        env["run_state"] = "interrupted"
        env["events"].append({"at": utc_now(), "event": f"run interrupted: {exc!r}"[:900]})
        save_environment(run_dir, env)
        for system in SYSTEM_SPEC:
            try:
                cluster_down(system, vtop_image)
            except Exception:  # best effort on the way out
                pass
        raise
    env["run_state"] = "complete"
    env["finished_at"] = utc_now()
    save_environment(run_dir, env)
    log(f"done: {ctx['csv']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
