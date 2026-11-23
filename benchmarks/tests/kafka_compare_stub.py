"""A Docker-free stand-in for everything `kafka_compare/run.py` reaches outside Python.

`install(run, root)` replaces the functions of a loaded `run` module that call Docker, git or the host, so
`run.main([...])` runs the whole matrix in a second or two and writes a real run directory under
`<root>/results/`. Each system answers with fixed, plausible numbers: Kafka saturates at KAFKA_RPS records/s
and VTOP at VTOP_RPS, a fixed-rate leg achieves the smaller of its offered rate and its system's saturation,
and every end-to-end record arrives in 1 or 2 ms. The rest of the runner (the matrix, the rows, the
environment, the events, the error paths) is the real code.

It is used by the tests of the runner's lifecycle (resume, SIGTERM, saved events), and it is how the
schema-4 fixture `fixtures/kafka-schema4-runner` was written: by the runner as it was at 22e18046, the
harness the v0.11.1 AWS round ran, with this stub in place of Docker.

`calls` records what the runner asked for: each cluster brought up (system, partitions), each producer
invocation (system, records, rate). `hooks` lets a test act at one of those points (raise, block).
"""
from __future__ import annotations

import json
import os
import subprocess
from typing import Any

KAFKA_RPS = 300_000.0
VTOP_RPS = 6_000.0
GAUGE = "vtop_broker_group_commit_enabled {}\n"


def saturation_of(system: str) -> float:
    return KAFKA_RPS if system == "kafka" else VTOP_RPS


def producer_output(records: int, rps: float, size: int, warmup: int) -> str:
    """What kafka-producer-perf-test prints, steady-state line included."""
    steady = max(records - warmup, 1)
    mbps = rps * size / 1048576
    summary = "2.00 ms avg latency, 6.00 ms max latency, 2 ms 50th, 3 ms 95th, 4 ms 99th, 5 ms 99.9th."
    return (f"{records // 2} records sent, {rps:.1f} records/sec ({mbps:.2f} MB/sec), 2.0 ms avg latency, "
            f"6.0 ms max latency.\n"
            f"{records} records sent, {rps:.6f} records/sec ({mbps:.2f} MB/sec), {summary}\n"
            f"{steady} steady state records sent, {rps:.6f} records/sec ({mbps:.2f} MB/sec), {summary}\n")


CONSUMER = ("start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, "
            "fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec\n"
            "2026-11-23 00:00:00:000, 2026-11-23 00:00:01:000, {mib:.4f}, 1.0, {n}, {rps:.4f}, 100, 1000, 1.0, "
            "{rps:.4f}\n")


def _fake_sh(repo: str):
    def sh(args, *, check=True, timeout=None, input_text=None, env=None, cwd=None):
        text = " ".join(args)
        out, err = "", ""
        if args[:2] == ["docker", "info"]:
            out = json.dumps({"DockerRootDir": "/var/lib/docker", "ServerVersion": "29.0.0", "OperatingSystem":
                              "Ubuntu 24.04", "KernelVersion": "6.8.0", "Architecture": "x86_64", "NCPU": 16,
                              "MemTotal": 32 * 2**30, "Driver": "overlayfs"})
        elif args[:2] == ["docker", "version"]:
            out = json.dumps({"Server": {"Platform": {"Name": "Docker Engine - Community"}}})
        elif args[:3] == ["docker", "image", "inspect"]:
            out = json.dumps([{"Id": "sha256:" + ("1" if "kafka" in text else "2") * 64}])
        elif "java" in args and "-version" in args:
            err = 'openjdk version "21.0.11" 2026-04-21'
        elif "df" in args:
            out = "Filesystem     Type 1K-blocks Used Available Use% Mounted on\n/dev/nvme1n1 ext4 1 1 1 1% /probe\n"
        elif args[:1] == ["sysctl"]:
            return subprocess.CompletedProcess(args, 1, "", "")
        return subprocess.CompletedProcess(args, 0, out, err)
    return sh


def _fake_git(repo: str):
    def git(*args):
        if args[:2] == ("rev-parse", "HEAD"):
            return "a" * 40
        if args[:2] == ("rev-parse", "HEAD:crates"):
            return "b" * 40
        if args[:1] == ("rev-parse",):
            return "c" * 12
        if args[:1] == ("show",):
            path = args[1].split(":", 1)[1]
            with open(os.path.join(repo, path), encoding="utf-8") as handle:
                return handle.read()
        return ""
    return git


def _link_inputs(run, root: str) -> None:
    """`run.HERE` is pointed at `root`, so a run is written under `<root>/results/`; the files the runner reads
    from beside itself are linked in."""
    for name in ("compose.kafka.yml", "compose.vtop.yml", "compose.kafka.fsync-parity.yml", "vtop", "client"):
        source, target = os.path.join(run.HERE, name), os.path.join(root, name)
        if os.path.exists(source) and not os.path.lexists(target):
            os.symlink(source, target)


def install(run, root: str, *, set_attr=setattr, hooks: dict[str, Any] | None = None) -> dict[str, list]:
    """Replace `run`'s outside world. `set_attr` is pytest's `monkeypatch.setattr` in a test (undone after it),
    plain `setattr` in a subprocess. Returns the record of calls."""
    hooks = hooks or {}
    calls: dict[str, list] = {"up": [], "producer": [], "leader": []}
    os.makedirs(root, exist_ok=True)
    _link_inputs(run, root)
    repo = run.REPO
    set_attr(run, "HERE", root)
    set_attr(run, "sh", _fake_sh(repo))
    set_attr(run, "git", _fake_git(repo))
    set_attr(run.hostmeta, "collect", lambda: {"instance_type": "stub.large", "cpu_model": "Stub CPU",
                                               "kernel": "Linux 6.8.0"})
    set_attr(run, "preflight_clients", lambda matrix, rebuild=False: "kc-e2e-client:stub" if matrix.get("e2e")
             else None)
    set_attr(run, "ensure_vtop_image", lambda rebuild: "vtop-bench:stub")
    set_attr(run, "cluster_down", lambda *a, **k: None)
    set_attr(run, "smoke", lambda *a, **k: hooks["smoke"](*a) if "smoke" in hooks else None)
    set_attr(run, "_save_leader_log", lambda *a, **k: None)

    def cluster_up(system, image, profile="defaults", leader_config=None, partitions=1):
        calls["up"].append((system, partitions))
        calls["leader"].append(leader_config)
        if "cluster_up" in hooks:
            hooks["cluster_up"](system, partitions)

    set_attr(run, "cluster_up", cluster_up)

    def scrape(_system):
        last = calls["leader"][-1]
        with open(last, encoding="utf-8") as handle:
            return GAUGE.format(1 if "enabled: true" in handle.read() else 0)

    set_attr(run, "scrape_leader_metrics", scrape)

    def observe(ctx, profile="defaults"):
        suffix = "" if profile == "defaults" else f"_{profile}"
        ctx["env"]["observed"][f"kafka_topic{suffix}"] = "Topic: bench PartitionCount: 1"
        ctx["env"]["observed"][f"kafka_broker_1_config{suffix}"] = {"min.insync.replicas": "2"}

    set_attr(run, "_observe_kafka", observe)

    def run_producer(system, records, size, rate, timeout, info_logging=False):
        calls["producer"].append((system, records, rate))
        if "producer" in hooks:
            hooks["producer"](system, records, rate)
        saturation = saturation_of(system)
        rps = saturation if rate < 0 else min(float(rate), saturation)
        warmup = max(1, records // 5)
        output = producer_output(records, rps, size, warmup)
        result = run.perftool.parse_producer(output, expect_steady_state=True)
        return result, "", records / rps, output

    set_attr(run, "run_producer", run_producer)

    def client(system, command, *, timeout, input_text=None, info_logging=False, check=True):
        if "kafka-consumer-perf-test.sh" in " ".join(command):
            n = int(command[command.index("--num-records") + 1])
            rps = saturation_of(system)
            return subprocess.CompletedProcess(command, 0, CONSUMER.format(mib=n * 100 / 1048576, n=n, rps=rps), "")
        return subprocess.CompletedProcess(command, 0, "", "")

    set_attr(run, "client", client)

    def measure(ctx, system, action):
        if not ctx.get("resources"):
            return action(), None, ""
        window = run.resources.Window(cpu_seconds=1.5, disk_write_bytes=4096, disk_read_bytes=0, devices=("nvme1n1",))
        return action(), run.Window(1_000, 2_000, window), ""

    set_attr(run, "measure", measure)

    def run_e2e_once(system, size, rate, records, timeout, image):
        result, note, wall, output = run.run_producer(system, records, size, rate, timeout)
        half = records // 2
        out = f"READY\nconsumed {records}\n1 {half}\n2 {records - half}\n"
        return result, note, wall, output, 0, out, ""

    set_attr(run, "run_e2e_once", run_e2e_once)
    return calls


def ctx_for(run, run_dir: str, **overrides) -> dict[str, Any]:
    """A cell's context as `run.main` builds it, for driving `run_cell` alone: one repetition, 100 B, a 1 s
    fixed-rate target, no consume, no resources, no end-to-end stage."""
    os.makedirs(run_dir, exist_ok=True)
    csv_path = os.path.join(run_dir, "results.csv")
    run.schema.write_header(csv_path)
    env = {"comparable": {"vtop_commit": "a" * 40}, "observed": {}, "events": [], "run_state": "running"}
    ctx = {"run_id": "r", "run_dir": run_dir, "csv": csv_path, "env": env, "fingerprint": "f" * 16,
           "vtop_image": "vtop-bench:stub", "reps": 1, "sat_records": {100: 30_000}, "fixed_seconds": 1.0,
           "smoke_records": 10, "sat_timeout": 1800.0, "consume": False, "group_commit_configs": {},
           "matrix": {}, "resources": False, "e2e": False, "saturation": {}}
    ctx.update(overrides)
    return ctx


ARGS = ["--sizes", "100", "--reps", "1", "--sat-records", "100:30000", "--fixed-seconds", "1",
        "--smoke-records", "10", "--host-disk", "Stub NVMe, SSD, NVMe, ext4", "--group-commit", "off,on"]
