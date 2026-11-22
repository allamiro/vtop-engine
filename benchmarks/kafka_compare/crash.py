#!/usr/bin/env python3
"""Acknowledged records that survive a crash of the whole replica set, VTOP vs Apache Kafka (#599).

    python3 benchmarks/kafka_compare/crash.py --smoke                  # 12 crashes, about 25 minutes
    python3 benchmarks/kafka_compare/crash.py --setup-flakey 20480 --crashes 10   # about 1 hour, see README

For every (crash number, configuration, crash mode): a fresh three-node cluster on empty data
directories, one producer at a fixed rate with `acks=all` and the idempotent producer on, a trigger
`t_i` seconds after the producer starts, the crash, the restart, and a read of the whole partition
to check every record the client saw acknowledged. One row per crash in crash.csv.

THE SAME CLIENT FOR BOTH SYSTEMS: `kafka-verifiable-producer.sh` from the pinned apache/kafka
image, through the Kafka gateway of VTOP's range leader (compose.vtop.yml, the shape of #596) and
against the three KRaft brokers (compose.kafka.yml).

CONFIGURATIONS: `kafka-defaults` (no flush: an acknowledgement means replicated), `kafka-fsync-parity`
(`log.flush.interval.messages=1` on every broker and `flush.messages=1` on the topic) and
`vtop-shipped` (the node configuration as shipped: an append is acknowledged after a quorum of two of
three has fsynced it; the same configuration answers both Kafka profiles).

CRASH MODES, and what each does and does not simulate (the report repeats this):

- `flakey-drop`: all three nodes are SIGKILLed by one command, then every write the page cache had not
  yet sent to the device is dropped: the three data directories are on ONE ext4 filesystem on a
  dm-flakey device, and `drop_writes` is loaded without freezing or syncing the filesystem before it
  is unmounted and mounted again. This is the loss of the host's page cache for all three replicas at
  once. It is NOT a drive whose volatile cache lies about flushes, a torn block, or independent
  crashes; the kernel may flush a little more between the kill and the drop (`kill_to_drop_ms`), which
  can only make a system look better. Needs root on the Docker host (`crash/powercut.sh`).
- `sigkill`: the same SIGKILL of all three, with the filesystem left alone. A killed process leaves
  the page cache intact, so nothing the kernel holds is lost: this is the control, and the one
  that needs no root.

Every crash is self-checking with two canary files (see crash_analysis.py): a crash whose canary does
not behave is a `refused` row, kept, never ok.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
import time
from typing import Any, NamedTuple

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from kafka_compare import crash_analysis as ca  # noqa: E402
from kafka_compare import crash_schema as cs  # noqa: E402
from kafka_compare import failover_stats as fstats  # noqa: E402
from kafka_compare import run as phase1  # noqa: E402
from kafka_compare import schema  # noqa: E402

REPO = phase1.REPO
KAFKA_IMAGE = phase1.KAFKA_IMAGE
TOPIC = "bench"
POWERCUT = os.path.join(HERE, "crash", "powercut.sh")

SPEC = {
    "kafka": {"project": "kc-crash-kafka", "network": "kc-crash-kafka-bench",
              "files": ("compose.kafka.yml", os.path.join("crash", "compose.kafka.crash.yml")),
              "parity_file": os.path.join("crash", "compose.kafka.fsync-parity.yml"),
              "containers": [f"kc-crash-kafka-kafka-{n}-1" for n in (1, 2, 3)],
              "bootstrap": "kafka-1:9092,kafka-2:9092,kafka-3:9092", "data": ["kafka-1", "kafka-2", "kafka-3"]},
    "vtop": {"project": "kc-crash-vtop", "network": "kc-crash-vtop-bench",
             "files": ("compose.vtop.yml", os.path.join("crash", "compose.vtop.crash.yml")),
             "parity_file": None,
             "containers": [f"kc-crash-vtop-vtop-{n}-1" for n in (1, 2, 3)],
             "bootstrap": "vtop-1:9092", "data": ["vtop-1", "vtop-2", "vtop-3"]},
}
PRODUCER = "kc-crash-producer"
# kafka-verifiable-producer.sh sets retries=0 for itself, which also turns idempotence off (#613): both
# are put back to the client's defaults, as failover.py does.
PRODUCER_PROPS = ("retries=2147483647", "enable.idempotence=true")
CLIENT_HEAP = phase1.CLIENT_HEAP

FIXED_TRIGGER_WINDOW = (25.0, 55.0)
CANARY_LEAD_S = 3.0          # the unsynced canary is written this long before the kill
STOP_PRODUCER_AFTER_S = 1.0  # stdout of the last acknowledgements reaches the file before the client dies
SERVE_DEADLINE_S = 240.0
SENTINEL_DEADLINE_S = 120.0
READ_IDLE_MS = 30000
SUSTAIN_FLOOR = 0.85
SUSPECT_AFTER = 5
CLOCK_PROBES = 7

log = phase1.log
sh = phase1.sh
utc_now = phase1.utc_now


class Refused(Exception):
    """The crash could not run, or did not run as specified; the message goes in `note`."""


class SuspectMethod(Exception):
    """Kafka's defaults lost nothing: the method is probably not dropping the page cache."""


class CapReached(Exception):
    """The next crash would not fit in --max-hours."""


class Params(NamedTuple):
    rate: int
    seed: int
    idempotent: bool = True
    sustain_floor: float = SUSTAIN_FLOOR


# --------------------------------------------------------------------------- commands


def kill_command(containers: list[str]) -> list[str]:
    """One command that SIGKILLs every node: the whole replica set at once."""
    return ["docker", "kill", "--signal", "KILL", *containers]


def default_host_prefix(euid: int | None = None) -> list[str]:
    euid = os.geteuid() if euid is None else euid
    return [] if euid == 0 else ["sudo", "-n"]


class Host:
    """Root-level operations on the Docker host: the data filesystem, the canaries, the drop."""

    def __init__(self, root: str, prefix: list[str], runner: Any = None) -> None:
        self.root = root
        self.prefix = prefix
        self.runner = sh if runner is None else runner

    def command(self, name: str, *args: str) -> list[str]:
        return [*self.prefix, "bash", POWERCUT, name, self.root, *args]

    def call(self, name: str, *args: str, timeout: float = 300) -> str:
        done = self.runner(self.command(name, *args), timeout=timeout)
        return done.stdout


def topic_create_args(config: str) -> list[str]:
    args = ["--create", "--topic", TOPIC, "--partitions", "1", "--replication-factor", "3",
            "--config", "min.insync.replicas=2"]
    if config == "kafka-fsync-parity":
        args += ["--config", "flush.messages=1"]
    return args


_UNSET = 9223372036854775807


def verify_flush(config: str, described: str) -> None:
    """The broker's own report of `log.flush.interval.messages` must match the profile, or the
    cell is not what its name says (#595 rule 4)."""
    match = re.search(r"log\.flush\.interval\.messages=(\d+)", described)
    if not match:
        raise Refused("the broker does not report log.flush.interval.messages, so the profile cannot be verified")
    value = int(match.group(1))
    if config == "kafka-fsync-parity" and value != 1:
        raise Refused(f"kafka-fsync-parity needs log.flush.interval.messages=1 on every broker, the broker "
                      f"reports {value}")
    if config == "kafka-defaults" and value != _UNSET:
        raise Refused(f"kafka-defaults needs log.flush.interval.messages unset (Kafka's default), "
                      f"the broker reports {value}")


# ---------------------------------------------------------------------------- one crash


def run_crash(b: Any, p: Params, config: str, mode: str, crash: int, trigger_after_s: float) -> dict[str, Any]:
    """One crash on backend `b`; returns its crash.csv row (without the run-constant columns)."""
    system, profile = ca.CONFIGS[config]
    prefix = ca.value_prefix(crash)
    started_at = utc_now()
    began = time.monotonic()
    row: dict[str, Any] = {
        "system": system, "config": config, "profile": profile, "crash_mode": mode, "crash": crash,
        "seed": p.seed, "trigger_after_s": trigger_after_s, "trigger_method": ca.CRASH_MODE_LABELS[mode],
        "offered_rate": p.rate, "client": "kafka-verifiable-producer",
        "idempotent": p.idempotent, "started_at": started_at, "needed_manual_repair": False, "note": "",
    }
    b.begin(config, mode, crash)
    try:
        return _crash(b, p, row, config, mode, crash, prefix, trigger_after_s)
    except Refused as exc:
        row.update(status="refused", note=str(exc))
    except Exception as exc:  # a crash that went wrong is a row that says so, and the run goes on
        row.update(status="failed", note=f"{type(exc).__name__}: {exc}"[:600])
    finally:
        row["wall_seconds"] = round(time.monotonic() - began, 1)
        try:
            b.save_logs(config, mode, crash, {k: row.get(k) for k in ("status", "note", "acked", "acked_lost")})
        except Exception as exc:
            log(f"saving the logs of {config}/{mode}/{crash} failed: {exc}")
        b.cleanup()
    return row


def _crash(b: Any, p: Params, row: dict[str, Any], config: str, mode: str, crash: int, prefix: str,
           trigger_after_s: float) -> dict[str, Any]:
    b.reset(config)
    b.bring_up(config)
    b.sync()
    b.canary_synced()
    b.start_producer(config, prefix, int(p.rate * (trigger_after_s + 120)))
    startup_wall = b.wait_producer_started()
    trigger_at = startup_wall + trigger_after_s
    b.wait_until(trigger_at - CANARY_LEAD_S)
    offset_ms, uncertainty_ms = b.clock_probe()
    b.canary_unsynced()
    b.wait_until(trigger_at)

    # THE CRASH: every node dies at once; in power-loss mode every write not yet on the device is then
    # dropped. Nothing, and above all no sync, runs between these two calls.
    kill_ms = b.kill_all()
    row["kill_to_drop_ms"] = None
    if mode == "flakey-drop":
        b.drop()
        row["kill_to_drop_ms"] = round(b.now_ms() - kill_ms, 1)
    b.stop_producer()
    run = ca.parse_acks(b.producer_lines(), prefix)
    b.release()
    if mode == "flakey-drop":
        b.restore()
    canary = b.canary_check()
    row.update(ca.canary_flags(canary))
    verdict = ca.judge_canary(mode, canary)

    kill_vm_ms = kill_ms + offset_ms
    acks = run.acks
    row.update(send_errors=run.errors, acked=len(acks), acked_last_seq=acks[-1].seq if acks else None,
               acked_in_last_10s=ca.acked_in_window(acks, kill_vm_ms + uncertainty_ms, 10))
    if not acks:
        raise RuntimeError("the producer acknowledged nothing before the crash")
    expected = p.rate * trigger_after_s
    row["status"] = "ok"
    if len(acks) < p.sustain_floor * expected:
        # An unsustained profile leaves acknowledged records nobody wrote: the offered rate was not met.
        row.update(status="refused", note=(
            f"the cell did not sustain the offered rate: {len(acks)} of {expected:.0f} records acknowledged "
            f"in the {trigger_after_s:g} s before the crash ({len(acks) / expected:.0%}, floor "
            f"{p.sustain_floor:.0%})"))
    elif not verdict.ok:
        row.update(status="refused", note=verdict.reason)

    started = time.monotonic()
    try:
        b.restart(config)
        end_before = b.wait_served()
        first = ca.diff_acked(acks, ca.parse_consumed(b.read_all(end_before) if end_before else "", prefix,
                                                      ca.sentinel_value(crash)))
        b.produce_sentinel(ca.sentinel_value(crash))
        row["recovery_seconds"] = round(time.monotonic() - started, 1)
        end_after = b.wait_served()
        consumed = ca.parse_consumed(b.read_all(end_after), prefix, ca.sentinel_value(crash))
    except Exception as exc:
        row.update(needed_manual_repair=True, recovery_seconds=round(time.monotonic() - started, 1))
        if row["status"] == "ok":
            row["status"] = "failed"
        said = f"the cluster did not serve after the restart: {exc}"
        row["note"] = ((row["note"] + "; " if row["note"] else "") + said)[:600]
        return row
    final = ca.diff_acked(acks, consumed)
    row.update(
        read_records=sum(1 for c in consumed if c.kind != "sentinel"), read_missing_before_sentinel=first.acked_lost,
        acked_lost=final.acked_lost, lost_first_seq=final.lost_first_seq, lost_last_seq=final.lost_last_seq,
        lost_runs=final.lost_runs, lost_window_s=(None if not final.lost else round(
            ca.lost_window_seconds(acks, final.lost, kill_vm_ms) or 0.0, 2)),
        present_unacked=final.unacked_present, duplicates=final.duplicates, reordered=final.reordered,
        offset_mismatch=final.offset_mismatch, unexpected=final.unexpected,
        sentinel_needed=first.acked_lost > final.acked_lost)
    return row


# ---------------------------------------------------------------------------- the loop


def run_matrix(b: Any, p: Params, matrix: dict[str, Any], run_dir: str, base_row: dict[str, Any],
               skip: set[tuple[str, str, int]] | None = None, deadline_s: float | None = None,
               clock: Any = time.monotonic, crash_estimate_s: float = 180.0,
               suspect_after: int = SUSPECT_AFTER) -> list[dict[str, Any]]:
    """Every configuration and mode for crash 1, then for crash 2, and so on, so a run cut short
    has the same number of crashes in every cell and every configuration saw the same schedule."""
    path = os.path.join(run_dir, "crash.csv")
    schedule = ca.trigger_schedule(p.seed, int(matrix["crashes"]), *matrix["trigger_window_s"])
    skip = skip or set()
    rows: list[dict[str, Any]] = []
    t0 = clock()
    for crash_number in range(1, int(matrix["crashes"]) + 1):
        for mode in matrix["crash_modes"]:
            for config in matrix["configs"]:
                if (config, mode, crash_number) in skip:
                    continue
                if deadline_s is not None and clock() - t0 + crash_estimate_s > deadline_s:
                    raise CapReached(
                        f"stopped before crash {crash_number} of {config}/{mode}: another crash would pass "
                        f"--max-hours ({deadline_s / 3600:g} h). --check lists what is owed; --resume continues.")
                trigger = schedule[crash_number - 1]
                log(f"crash {crash_number}/{matrix['crashes']} {config} {mode} (trigger {trigger} s)")
                row = run_crash(b, p, config, mode, crash_number, trigger)
                row.update(base_row)
                cs.append_rows(path, [row])
                rows.append(row)
                log(f"  -> {row['status']}: acked {row.get('acked')}, lost {row.get('acked_lost')}"
                    + (f" ({row['note'][:120]})" if row.get("note") else ""))
                every = cs.read_rows(path, allow_empty=True)
                message = ca.suspect_method(every, suspect_after)
                if message:
                    raise SuspectMethod(message)
    return rows


# --------------------------------------------------------------------------- the backend


class DockerBackend:
    """The real cluster, producer, consumer and data filesystem."""

    def __init__(self, ctx: dict[str, Any]) -> None:
        self.ctx = ctx
        self.host: Host = ctx["host"]
        self.run_dir = ctx["run_dir"]
        self.raw = os.path.join(self.run_dir, ".raw")
        self.config = ""
        self.system = ""
        self.mode = ""
        self.crash = 0
        self.producer: subprocess.Popen | None = None
        self._seq = 0
        os.makedirs(self.raw, exist_ok=True)
        os.chmod(self.raw, 0o777)

    # --- identity and time
    def wall(self) -> float:
        return time.time()

    def now_ms(self) -> float:
        return time.time_ns() / 1e6

    def wait_until(self, t_unix_s: float) -> None:
        delay = t_unix_s - time.time()
        if delay > 0:
            time.sleep(delay)

    def begin(self, config: str, mode: str, crash: int) -> None:
        self.config, self.mode, self.crash = config, mode, crash
        self.system = ca.CONFIGS[config][0]
        self.crash_raw = os.path.join(self.raw, f"{config}-{mode}-{crash:03d}")
        os.makedirs(self.crash_raw, exist_ok=True)
        os.chmod(self.crash_raw, 0o777)
        with open(os.path.join(self.crash_raw, "producer.properties"), "w") as handle:
            handle.write("".join(f"{line}\n" for line in PRODUCER_PROPS))

    # --- compose
    def compose(self, system: str, *args: str, timeout: float = 600, config: str | None = None,
                check: bool = True) -> subprocess.CompletedProcess:
        spec = SPEC[system]
        files = list(spec["files"])
        if config == "kafka-fsync-parity":
            files.append(spec["parity_file"])
        command = ["docker", "compose", "-p", spec["project"]]
        for name in files:
            command += ["-f", os.path.join(HERE, name)]
        return sh([*command, *args], timeout=timeout, cwd=HERE, check=check, env={
            "VTOP_IMAGE": self.ctx["vtop_image"], "KAFKA_IMAGE": KAFKA_IMAGE,
            "CRASH_DATA_ROOT": self.ctx["data_root"], "CRASH_NODE_CPUS": str(self.ctx["node_cpus"])})

    def down(self, config: str | None = None, volumes: bool = False) -> None:
        sh(["docker", "rm", "-f", PRODUCER], check=False)
        for system in SPEC:
            self.compose(system, "down", *(["-v"] if volumes else []), "--remove-orphans", config=config,
                         check=False)

    def reset(self, config: str) -> None:
        self.down(volumes=True)
        root = self.ctx["data_root"]
        sh(["docker", "run", "--rm", "--user", "0", "--entrypoint", "sh", "-v", f"{root}:/d", KAFKA_IMAGE, "-c",
            "find /d -mindepth 1 -maxdepth 1 ! -name lost+found ! -name '.kc599-*' -exec rm -rf {} +"], timeout=300)
        for name in SPEC[ca.CONFIGS[config][0]]["data"]:
            path = os.path.join(root, name)
            os.makedirs(path, exist_ok=True)
            os.chmod(path, 0o777)

    def bring_up(self, config: str) -> None:
        system = ca.CONFIGS[config][0]
        try:
            self.compose(system, "up", "-d", "--wait", "--wait-timeout", "300", config=config, timeout=400)
        except RuntimeError as exc:
            raise Refused(f"the {system} cluster did not come up healthy: {exc}"[:500]) from None
        if system == "kafka":
            self.client(["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", SPEC[system]["bootstrap"],
                         *topic_create_args(config)], timeout=120)
            self.observe_kafka(config)

    def observe_kafka(self, config: str) -> None:
        """The brokers' own word for the flush settings, once per configuration (rule 8), and a
        refusal when it does not match the profile."""
        seen = self.ctx["env"]["observed"].setdefault("kafka_flush", {})
        broker = self.client(["/opt/kafka/bin/kafka-configs.sh", "--bootstrap-server", SPEC["kafka"]["bootstrap"],
                              "--entity-type", "brokers", "--entity-name", "1", "--describe", "--all"],
                             timeout=120).stdout
        topic = self.client(["/opt/kafka/bin/kafka-configs.sh", "--bootstrap-server", SPEC["kafka"]["bootstrap"],
                             "--entity-type", "topics", "--entity-name", TOPIC, "--describe", "--all"],
                            timeout=120).stdout
        verify_flush(config, broker)
        if config not in seen:
            keep = ("log.flush.interval.messages", "log.flush.interval.ms", "log.flush.scheduler.interval.ms",
                    "min.insync.replicas", "default.replication.factor", "replica.lag.time.max.ms",
                    "unclean.leader.election.enable")
            seen[config] = {"broker_1": [line.split(" sensitive=")[0] for line in broker.splitlines()
                                         if line.strip().startswith(keep)],
                            "topic": [line.split(" sensitive=")[0] for line in topic.splitlines()
                                      if line.strip().startswith(("flush.messages", "flush.ms", "min.insync"))]}
            self.ctx["save_env"]()

    # --- docker clients
    def client(self, command: list[str], *, timeout: float, input_text: str | None = None, check: bool = True,
               info_logging: bool = False) -> subprocess.CompletedProcess:
        self._seq += 1
        name = f"kc-crash-client-{os.getpid()}-{self._seq}"
        env = ["-e", f"KAFKA_HEAP_OPTS={CLIENT_HEAP}"]
        if info_logging:
            env += ["-e", "KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=/opt/kafka/config/log4j2.yaml"]
        args = ["docker", "run", "--rm", "--name", name, "--network", SPEC[self.system]["network"],
                "--cpus", str(self.ctx["client_cpus"]), "--memory", self.ctx["client_memory"], *env]
        if input_text is not None:
            args.append("-i")
        args += [KAFKA_IMAGE, *command]
        try:
            return sh(args, timeout=timeout, input_text=input_text, check=check)
        except subprocess.TimeoutExpired:
            sh(["docker", "rm", "-f", name], check=False)
            raise

    def sync(self) -> None:
        self.host.call("sync")

    def canary_synced(self) -> None:
        self.host.call("canary-synced")

    def canary_unsynced(self) -> None:
        self.host.call("canary-unsynced")

    def canary_check(self) -> ca.Canary:
        out = self.host.call("canary-check").strip()
        found = dict(part.split("=") for part in out.split())
        return ca.Canary(int(found["synced"]), ca.CANARY_BYTES, int(found["unsynced"]), ca.CANARY_BYTES)

    # --- the producer
    def start_producer(self, config: str, prefix: str, max_messages: int) -> None:
        out = open(os.path.join(self.crash_raw, "producer.out"), "w")
        err = open(os.path.join(self.crash_raw, "producer.err"), "w")
        command = ["docker", "run", "--rm", "--name", PRODUCER, "--network", SPEC[self.system]["network"],
                   "--cpus", str(self.ctx["client_cpus"]), "--memory", self.ctx["client_memory"],
                   "-e", f"KAFKA_HEAP_OPTS={CLIENT_HEAP}", "-v", f"{self.crash_raw}:/out", KAFKA_IMAGE,
                   "/opt/kafka/bin/kafka-verifiable-producer.sh", "--bootstrap-server", SPEC[self.system]["bootstrap"],
                   "--topic", TOPIC, "--acks", "-1", "--command-config", "/out/producer.properties",
                   "--throughput", str(self.ctx["params"].rate), "--max-messages", str(max_messages),
                   "--value-prefix", prefix]
        self.producer = subprocess.Popen(command, stdout=out, stderr=err)

    def wait_producer_started(self) -> float:
        deadline = time.monotonic() + 90
        path = os.path.join(self.crash_raw, "producer.out")
        while time.monotonic() < deadline:
            if self.producer and self.producer.poll() is not None:
                raise Refused("the producer exited before it started: see producer.err")
            with open(path) as handle:
                if "startup_complete" in handle.read():
                    return time.time()
            time.sleep(0.1)
        raise Refused("the producer did not start within 90 s")

    def clock_probe(self) -> tuple[float, float]:
        samples = []
        for _ in range(CLOCK_PROBES):
            before = time.time_ns()
            done = sh(["docker", "exec", PRODUCER, "date", "+%s%N"], check=False)
            after = time.time_ns()
            if done.returncode == 0 and done.stdout.strip().isdigit():
                samples.append((before, int(done.stdout.strip()), after))
        try:
            return fstats.clock_offset(samples)
        except ValueError:
            return 0.0, 50.0

    def kill_all(self) -> float:
        started = time.time_ns() / 1e6
        sh(kill_command(SPEC[self.system]["containers"]))
        return started

    def stop_producer(self) -> None:
        time.sleep(STOP_PRODUCER_AFTER_S)
        sh(["docker", "kill", "--signal", "KILL", PRODUCER], check=False)
        if self.producer:
            self.producer.wait(timeout=60)

    def producer_lines(self) -> list[str]:
        with open(os.path.join(self.crash_raw, "producer.out")) as handle:
            return handle.read().splitlines()

    # --- the crash
    def drop(self) -> None:
        self.host.call("drop")

    def release(self) -> None:
        """Remove the dead containers so nothing holds the data filesystem open."""
        self.save_node_logs("killed")
        self.compose(self.system, "down", "--remove-orphans", config=self.config, check=False)

    def restore(self) -> None:
        self.host.call("restore")

    def restart(self, config: str) -> None:
        try:
            self.compose(self.system, "up", "-d", "--wait", "--wait-timeout", "300", config=config, timeout=400)
        except RuntimeError as exc:
            raise Refused(f"compose up after the crash: {exc}"[:400]) from None

    def wait_served(self) -> int:
        """The partition's end offset, once the cluster answers ListOffsets: the first served read."""
        deadline = time.monotonic() + SERVE_DEADLINE_S
        last = ""
        while time.monotonic() < deadline:
            done = self.client(["/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server",
                                SPEC[self.system]["bootstrap"], "--topic", TOPIC, "--time", "-1"], timeout=90,
                               check=False)
            match = re.search(rf"^{TOPIC}:0:(\d+)\s*$", done.stdout, re.MULTILINE)
            if done.returncode == 0 and match:
                return int(match.group(1))
            last = (done.stderr or done.stdout).strip()[-200:]
            time.sleep(2)
        raise Refused(f"the cluster did not serve within {SERVE_DEADLINE_S:.0f} s of the restart: {last}")

    def read_all(self, end: int) -> str:
        done = self.client(["/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server",
                            SPEC[self.system]["bootstrap"], "--topic", TOPIC, "--partition", "0", "--offset",
                            "earliest", "--max-messages", str(end), "--timeout-ms", str(READ_IDLE_MS),
                            "--property", "print.offset=true"], timeout=900, check=False)
        with open(os.path.join(self.crash_raw, f"consumer-{end}.out"), "w") as handle:
            handle.write(done.stdout)
        return done.stdout

    def produce_sentinel(self, value: str) -> None:
        deadline = time.monotonic() + SENTINEL_DEADLINE_S
        last = ""
        while time.monotonic() < deadline:
            done = self.client(["/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server",
                                SPEC[self.system]["bootstrap"], "--topic", TOPIC, "--command-property", "acks=all"],
                               timeout=120, input_text=value + "\n", check=False)
            if done.returncode == 0:
                return
            last = (done.stderr or done.stdout).strip()[-200:]
            time.sleep(3)
        raise Refused(f"no write was acknowledged within {SENTINEL_DEADLINE_S:.0f} s of the restart: {last}")

    # --- evidence and cleanup
    def save_node_logs(self, tag: str) -> None:
        logs = self.crash_logs()
        for index, container in enumerate(SPEC[self.system]["containers"], start=1):
            done = sh(["docker", "logs", "--tail", "300", container], check=False)
            with open(os.path.join(logs, f"node-{index}-{tag}.log"), "w") as handle:
                handle.write((done.stdout + done.stderr)[-60000:])

    def crash_logs(self) -> str:
        path = os.path.join(self.run_dir, "logs", f"{self.config}-{self.mode}-{self.crash:03d}")
        os.makedirs(path, exist_ok=True)
        return path

    def save_logs(self, config: str, mode: str, crash: int, summary: dict[str, Any]) -> None:
        logs = self.crash_logs()
        self.save_node_logs("after")
        with open(os.path.join(logs, "summary.json"), "w") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True, default=str)
        for name in ("producer.err",):
            src = os.path.join(self.crash_raw, name)
            if os.path.isfile(src):
                with open(src) as handle, open(os.path.join(logs, name), "w") as out:
                    out.write(handle.read()[-20000:])

    def cleanup(self) -> None:
        if self.producer and self.producer.poll() is None:
            sh(["docker", "rm", "-f", PRODUCER], check=False)
            self.producer.wait()
        self.down(config=self.config, volumes=True)
        shutil.rmtree(self.crash_raw, ignore_errors=True)


# ------------------------------------------------------------------------ environment


def cpu_model() -> str:
    try:
        with open("/proc/cpuinfo") as handle:
            for line in handle:
                if line.lower().startswith(("model name", "hardware", "cpu model")):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or platform.machine()


def instance_type() -> str:
    """The cloud instance type, when the DMI says so (EC2 Nitro writes it to product_name)."""
    try:
        with open("/sys/devices/virtual/dmi/id/sys_vendor") as handle:
            vendor = handle.read().strip()
        with open("/sys/devices/virtual/dmi/id/product_name") as handle:
            product = handle.read().strip()
        return f"{vendor} {product}"
    except OSError:
        return ""


def capture_environment(run_id: str, matrix: dict[str, Any], vtop_image: str, limits: dict[str, str], disk: str,
                        args: argparse.Namespace, host: Host) -> dict[str, Any]:
    env = phase1.capture_environment(run_id, matrix, vtop_image, limits, disk)
    c = env["comparable"]
    for key in ("profile", "consumer_props"):
        c.pop(key, None)
    c["vtop_commit"] = phase1.git("log", "-1", "--format=%H", "--", "crates", "Cargo.toml", "Cargo.lock", "docker")
    c["harness_commit"] = phase1.git("rev-parse", "HEAD")
    c["host_cpu"] = cpu_model() if platform.system() == "Linux" else c["host_cpu"]
    c["host_instance"] = instance_type()
    c["node_limits"] = {**limits, "cpus": str(args.node_cpus)}
    c["client_limits"] = {"cpus": str(args.client_cpus), "memory": args.client_memory, "heap": CLIENT_HEAP}
    c["client"] = ("kafka-verifiable-producer.sh " + KAFKA_IMAGE.split("@")[0].split(":")[-1] +
                   " (JVM), the same invocation against both systems; reads by kafka-console-consumer.sh")
    c["producer_props"] = ["acks=all (--acks -1)", *PRODUCER_PROPS,
                           "every other producer setting at the client default"]
    c["profiles"] = {
        "kafka-defaults": "Kafka as shipped: no flush.messages, an acknowledgement is replication",
        "kafka-fsync-parity": "log.flush.interval.messages=1 on every broker and flush.messages=1 on the topic",
        "vtop-shipped": "node configuration as shipped: acknowledged after a quorum (2 of 3) has fsynced; "
                        "answers both Kafka profiles",
    }
    c["crash_modes"] = {mode: ca.CRASH_MODE_LABELS[mode] for mode in matrix["crash_modes"]}
    c["data_filesystem"] = dict(line.split("=", 1) for line in host.call("describe").split() if "=" in line)
    c["simulation_limits"] = (
        "page-cache loss of one host for all three replicas at once; not a drive whose volatile cache lies "
        "about flushes, not a torn block, not independent crashes; the kernel can flush a little more "
        "between the kill and the drop (kill_to_drop_ms)")
    c["topology"] = {"kafka": "3 KRaft combined broker+controller nodes, RF=3, min.insync.replicas=2",
                     "vtop": "static range leader with the Kafka gateway plus two followers, quorum 2 of 3 "
                             "(compose.vtop.yml); no metadata plane, so no election after the restart"}
    env["kind"] = "crash"
    env["label"] = "single Linux host, Docker Engine; indicative, not a production benchmark"
    env["configs"] = {
        "kafka_compose": phase1._read(os.path.join(HERE, "compose.kafka.yml")),
        "kafka_crash_override": phase1._read(os.path.join(HERE, "crash", "compose.kafka.crash.yml")),
        "kafka_fsync_parity_override": phase1._read(os.path.join(HERE, "crash", "compose.kafka.fsync-parity.yml")),
        "vtop_compose": phase1._read(os.path.join(HERE, "compose.vtop.yml")),
        "vtop_crash_override": phase1._read(os.path.join(HERE, "crash", "compose.vtop.crash.yml")),
        "vtop_leader": phase1._read(os.path.join(HERE, "vtop", "leader.yaml")),
        "vtop_follower_1": phase1._read(os.path.join(HERE, "vtop", "follower-1.yaml")),
        "vtop_follower_2": phase1._read(os.path.join(HERE, "vtop", "follower-2.yaml")),
        "powercut": phase1._read(POWERCUT),
    }
    return env


# --------------------------------------------------------------------------- arguments


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--smoke", action="store_true",
                        help="2 crashes per cell in both modes, triggers at 8 to 12 s: a few minutes per "
                             "configuration, to prove the method on this host")
    parser.add_argument("--crashes", type=int, default=None, help="crashes per (configuration, mode); default 30")
    parser.add_argument("--configs", default=",".join(ca.CONFIGS), help="comma-separated; default all three")
    parser.add_argument("--modes", default=None, help="comma-separated of sigkill, flakey-drop; default flakey-drop")
    parser.add_argument("--rate", type=int, default=1000, help="records per second offered (fixed, open loop)")
    parser.add_argument("--trigger-window", default=None,
                        help="LOW,HIGH seconds after the producer starts; the crash time is drawn from it per "
                             "crash number, identically for every configuration (default 25,55)")
    parser.add_argument("--seed", type=int, default=599)
    parser.add_argument("--data-root", default="/var/lib/kc599/data",
                        help="where every node's data directory lives; in flakey-drop mode it must be the "
                             "mounted dm-flakey filesystem (--setup-flakey makes it)")
    parser.add_argument("--setup-flakey", type=int, metavar="MIB", default=None,
                        help="create the sparse image, dm-flakey device and ext4 at --data-root if it is not "
                             "already there (needs root, loop and dm-flakey)")
    parser.add_argument("--teardown-flakey", action="store_true", help="remove it again when the run ends")
    parser.add_argument("--host-exec", default=None, metavar="PREFIX",
                        help="how to run powercut.sh as root (default: nothing as root, else 'sudo -n')")
    parser.add_argument("--host-disk", default=None, metavar="TEXT",
                        help="name the disk behind Docker's data yourself, for a host (a virtio VM) whose "
                             "probe cannot; recorded as given")
    parser.add_argument("--node-cpus", type=int, default=2, help="CPUs per node, Kafka and VTOP alike")
    parser.add_argument("--client-cpus", type=int, default=4)
    parser.add_argument("--client-memory", default="4g")
    parser.add_argument("--suspect-after", type=int, default=SUSPECT_AFTER,
                        help="stop when Kafka's defaults lose nothing in this many power-loss crashes")
    parser.add_argument("--rebuild", action="store_true", help="rebuild the vtop image even if it exists")
    parser.add_argument("--max-hours", type=float, default=30.0,
                        help="refuse a matrix projected to take longer, and stop before a crash that would pass it")
    parser.add_argument("--resume", metavar="RUN_ID", default=None,
                        help="continue this run: crashes already in its crash.csv are skipped")
    parser.add_argument("--check", metavar="RUN_DIR", default=None,
                        help="report which crashes a run still owes; exit 0 when complete, 2 otherwise")
    return parser.parse_args(argv)


_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def build_matrix(args: argparse.Namespace) -> dict[str, Any]:
    crashes = args.crashes if args.crashes is not None else (2 if args.smoke else 30)
    modes = args.modes if args.modes is not None else ("sigkill,flakey-drop" if args.smoke else "flakey-drop")
    window = args.trigger_window or ("8,12" if args.smoke else "25,55")
    try:
        low, high = (float(part) for part in window.split(","))
    except ValueError:
        raise SystemExit(f"--trigger-window {window!r}: two comma-separated seconds, LOW,HIGH") from None
    return {"configs": [c for c in args.configs.split(",") if c], "crash_modes": [m for m in modes.split(",") if m],
            "crashes": crashes, "rate": args.rate, "seed": args.seed,
            "trigger_window_s": [low, high], "smoke": bool(args.smoke)}


def check_args(args: argparse.Namespace, matrix: dict[str, Any]) -> float:
    """Every argument and the plan, refused up front before any Docker work. Returns projected hours."""
    if args.run_id is not None and (not _RUN_ID.match(args.run_id) or ".." in args.run_id):
        raise SystemExit(f"--run-id {args.run_id!r} must be one path component of letters, digits, '.', '_' or '-'")
    if not math.isfinite(args.max_hours) or args.max_hours <= 0:
        raise SystemExit(f"--max-hours {args.max_hours}: must be a positive, finite number of hours")
    for name, value in (("--node-cpus", args.node_cpus), ("--client-cpus", args.client_cpus)):
        if value < 1:
            raise SystemExit(f"{name} {value}: at least one CPU")
    if args.suspect_after < 1:
        raise SystemExit(f"--suspect-after {args.suspect_after}: at least one crash")
    try:
        cs.check_matrix(matrix)
        hours = cs.check_budget(matrix, args.max_hours)
    except (cs.PlanRefused, ValueError) as exc:
        raise SystemExit(f"refused before any Docker work: {exc}") from None
    return hours


# ------------------------------------------------------------------------------- main


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.check is not None:
        return cs.check_main(args.check)
    matrix = build_matrix(args)
    hours = check_args(args, matrix)
    cells = len(matrix["configs"]) * len(matrix["crash_modes"])
    log(f"{cells} cells x {matrix['crashes']} crashes = {cells * matrix['crashes']} crash rows, "
        f"projected {hours:.1f} h (--max-hours {args.max_hours:g})")
    if matrix["crashes"] < 20 and not matrix["smoke"]:
        log(f"warning: {matrix['crashes']} crashes per cell: with none lost the 95 % upper bound on the chance a "
            f"crash loses something is {ca.upper_bound_95(0, matrix['crashes']):.1%} (30 crashes: 9.5 %)")
    limits = phase1.check_equal_limits()
    host = Host(args.data_root, shlex.split(args.host_exec) if args.host_exec is not None else default_host_prefix())
    disk = args.host_disk or phase1.probe_host_disk()
    status = host.call("status").strip()
    if "flakey-drop" in matrix["crash_modes"] and status != "flakey":
        if args.setup_flakey is None:
            raise SystemExit(f"{args.data_root} is not a dm-flakey filesystem. Make it with --setup-flakey MIB, or "
                             f"by hand: sudo bash {POWERCUT} setup {args.data_root} 20480")
        log(f"setting up the dm-flakey filesystem at {args.data_root} ({args.setup_flakey} MiB)")
        host.call("setup", str(args.setup_flakey))
    for system in SPEC:
        running = sh(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={SPEC[system]['project']}"])
        if running.stdout.strip():
            raise SystemExit(f"{SPEC[system]['project']} is already running; stop it first (one system at a time)")
    sh(["docker", "pull", "-q", KAFKA_IMAGE], timeout=1800)
    vtop_image = ensure_image(args.rebuild)

    resumed: set[tuple[str, str, int]] = set()
    if args.resume:
        run_id = phase1.check_run_id(args.resume)
        run_dir = os.path.join(HERE, "results", run_id)
        if not os.path.isfile(os.path.join(run_dir, "crash.csv")):
            raise SystemExit(f"--resume {run_id}: no crash.csv in {run_dir}")
        resumed = {(r["config"], r["crash_mode"], int(float(r["crash"]))) for r in
                   cs.read_rows(os.path.join(run_dir, "crash.csv"), allow_empty=True)}
    else:
        run_id = args.run_id or "crash-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_dir = os.path.join(HERE, "results", run_id)
        if os.path.exists(run_dir):
            raise SystemExit(f"{run_dir} exists; runs never overwrite each other (--resume continues one)")
        os.makedirs(run_dir)
    env = capture_environment(run_id, matrix, vtop_image, limits, disk, args, host)
    fingerprint = schema.fingerprint(env)
    if args.resume:
        with open(os.path.join(run_dir, "environment.json")) as handle:
            before = json.load(handle)
        if schema.fingerprint(before) != fingerprint or before.get("matrix") != matrix:
            raise SystemExit(f"--resume {run_id}: this host, build or matrix differs from the run being resumed "
                             f"({schema.fingerprint(before)} against {fingerprint}); a run is one environment")
        env["observed"] = before.get("observed", {})
        env["events"] = before.get("events", [])
        env["started_at"] = before.get("started_at", env["started_at"])
    else:
        cs.write_header(os.path.join(run_dir, "crash.csv"))
    env["run_state"] = "running"
    phase1.save_environment(run_dir, env)
    params = Params(rate=args.rate, seed=args.seed)
    ctx = {"run_dir": run_dir, "env": env, "host": host, "vtop_image": vtop_image, "data_root": args.data_root,
           "node_cpus": args.node_cpus, "client_cpus": args.client_cpus, "client_memory": args.client_memory,
           "params": params, "save_env": lambda: phase1.save_environment(run_dir, env)}
    backend = DockerBackend(ctx)
    base_row = {"run_id": run_id, "env_fingerprint": fingerprint, "vtop_commit": env["comparable"]["vtop_commit"],
                "kafka_image": KAFKA_IMAGE}
    log(f"run {run_id} -> {run_dir} (fingerprint {fingerprint})")
    code = 0
    try:
        run_matrix(backend, params, matrix, run_dir, base_row, skip=resumed, deadline_s=args.max_hours * 3600,
                   suspect_after=args.suspect_after)
        env["run_state"] = "complete"
        env["finished_at"] = utc_now()
    except (SuspectMethod, CapReached) as exc:
        env["run_state"] = "interrupted"
        env["events"].append({"at": utc_now(), "event": str(exc)[:900]})
        print(str(exc), file=sys.stderr)
        code = 3 if isinstance(exc, SuspectMethod) else 4
    except BaseException as exc:
        env["run_state"] = "interrupted"
        env["events"].append({"at": utc_now(), "event": f"run interrupted: {exc!r}"[:900]})
        phase1.save_environment(run_dir, env)
        backend.down(volumes=True)
        raise
    finally:
        if args.teardown_flakey:
            try:
                host.call("teardown")
            except Exception as exc:
                log(f"flakey teardown failed: {exc}")
    backend.down(volumes=True)
    phase1.save_environment(run_dir, env)
    log(f"{env['run_state']}: {os.path.join(run_dir, 'crash.csv')}")
    return code


def ensure_image(rebuild: bool) -> str:
    dirty = phase1.dirty_build_inputs()
    if dirty:
        raise SystemExit(f"the engine's build inputs differ from HEAD ({'; '.join(dirty[:5])}): commit or stash them, "
                         "so the image benchmarked is the commit recorded")
    commit = phase1.git("log", "-1", "--format=%H", "--", "crates", "Cargo.toml", "Cargo.lock", "docker")
    image = f"vtop-bench:{commit[:12]}"
    exists = sh(["docker", "image", "inspect", image], check=False).returncode == 0
    if rebuild or not exists:
        log(f"building {image} from docker/Dockerfile")
        sh(["docker", "build", "-f", os.path.join(REPO, "docker", "Dockerfile"), "-t", image, REPO], timeout=3600)
    return image


if __name__ == "__main__":
    sys.exit(main())
