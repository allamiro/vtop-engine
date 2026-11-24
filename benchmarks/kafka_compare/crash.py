#!/usr/bin/env python3
"""Acknowledged records that survive a crash of the whole replica set, VTOP vs Apache Kafka (#599).

    python3 benchmarks/kafka_compare/crash.py --smoke                  # 12 crashes, about 20 minutes
    python3 benchmarks/kafka_compare/crash.py --setup-flakey 10240 --crashes 12 --modes flakey-drop  # ~1 hour

For every (crash number, configuration, crash mode): a fresh three-node cluster on empty data
directories, one producer at a fixed rate with `acks=all` and the idempotent producer on, a trigger
`t_i` seconds after the producer starts, the crash, the restart, and a read of the whole partition
to check every record the client saw acknowledged. One row per crash in crash.csv.

THE SAME CLIENT FOR BOTH SYSTEMS, through the Kafka gateway of VTOP's range leader (compose.vtop.yml,
the shape of #596) and against the three KRaft brokers (compose.kafka.yml). Records are 1 KiB (`--record-size`,
the issue's size): `kafka-verifiable-producer.sh` has no size option, so the client is `crash/client/
AckProducer.java`, the same KafkaProducer from the same pinned apache/kafka image printing the same lines with a
padded value (built once by `ensure_client_image`). `--record-size 0` uses the stock tool and a few bytes a record.

CONFIGURATIONS: `kafka-defaults` (no flush: an acknowledgement means replicated), `kafka-fsync-parity`
(`log.flush.interval.messages=1` on every broker and `flush.messages=1` on the topic) and VTOP in up to three
configurations that differ in the node's `group_commit:` section only (#752): `vtop-shipped` (the section left
out: the node as shipped, and the quorum of two of three has fsynced an append before it is acknowledged; the
same configuration answers both Kafka profiles), `vtop-group-commit-on` and `vtop-group-commit-off`. By default
a run takes the shipped one and the state that differs from it, which the node's source says (see
`shipped_group_commit`); the leader's own gauge is checked against each configuration.

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
import hashlib
import json
import math
import os
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
# The producer with a record size: its source and Dockerfile, and the JDK the pinned apache/kafka image lacks.
CLIENT_DIR = os.path.join(HERE, "crash", "client")
JDK_IMAGE = "eclipse-temurin:21.0.11_10-jdk@sha256:98155457fcdbeb0987472e8abf721657b698ede407aad52ac24ae42618c0931c"
DEFAULT_RECORD_SIZE = 1024
NODE_CONFIG_RS = os.path.join("crates", "vtop-node", "src", "config.rs")
LEADER_GAUGE = "vtop_broker_group_commit_enabled"
# kafka-verifiable-producer.sh sets retries=0 for itself, which also turns idempotence off (#613): both
# are put back to the client's defaults, as failover.py does.
PRODUCER_PROPS = ("retries=2147483647", "enable.idempotence=true")
CLIENT_HEAP = phase1.CLIENT_HEAP

FIXED_TRIGGER_WINDOW = (25.0, 55.0)
CANARY_LEAD_S = 1.0          # the unsynced canary is written this long before the kill (a journal commit or an
                             # fsync by the system under test between the two would flush it)
CLOCK_LEAD_S = 4.0           # the clock probe runs this much earlier still
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


class ShortRead(Exception):
    """The read that decides the loss ended before the sentinel: the measurement failed, nothing was lost."""


class CapReached(Exception):
    """The next crash would not fit in --max-hours."""


class Params(NamedTuple):
    rate: int
    seed: int
    idempotent: bool = True
    sustain_floor: float = SUSTAIN_FLOOR
    record_size: int = DEFAULT_RECORD_SIZE


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


def compose_files(system: str, config: str | None) -> list[str]:
    """The Compose files of a system for a configuration: the base, the crash override, and for Kafka's
    fsync-parity the flush override. `config` may be another system's (teardown runs both systems with
    the crash's configuration): the flush override belongs to Kafka only."""
    files = list(SPEC[system]["files"])
    if system == "kafka" and config == "kafka-fsync-parity":
        files.append(SPEC["kafka"]["parity_file"])
    return files


def client_label(record_size: int) -> str:
    return "AckProducer (kafka-clients, padded values)" if record_size > 0 else "kafka-verifiable-producer"


def shipped_group_commit() -> bool | None:
    """Whether the node at this checkout ships with group commit on, or None when its source does not say."""
    try:
        with open(os.path.join(REPO, NODE_CONFIG_RS), encoding="utf-8") as handle:
            return ca.shipped_group_commit_default(handle.read())
    except (OSError, ValueError):
        return None


def vtop_leader_file(config: str) -> str:
    """The leader's node configuration (relative to this directory) a configuration runs: the shipped file, or
    that file plus the `group_commit:` section (crash/leader-group-commit-{on,off}.yaml)."""
    asked = ca.VTOP_GROUP_COMMIT.get(config)
    if asked is None:
        return os.path.join("vtop", "leader.yaml")
    return os.path.join("crash", f"leader-group-commit-{'on' if asked else 'off'}.yaml")


def redundant_configs(configs: list[str], shipped: bool | None) -> list[str]:
    """One line per configuration that runs the same group-commit setting as an earlier one: a replicate, not
    a comparison, and the operator should know."""
    seen: dict[bool, str] = {}
    notes = []
    for config in configs:
        effective = ca.effective_group_commit(config, shipped)
        if effective is None:
            continue
        if effective in seen:
            notes.append(f"{config} runs the same group-commit setting as {seen[effective]} "
                         f"(group commit {'on' if effective else 'off'}; the node ships with it "
                         f"{'on' if shipped else 'off'}), so it is a second sample of one configuration")
        else:
            seen[effective] = config
    return notes


def verify_group_commit(config: str, metrics: str, shipped: bool | None) -> None:
    """The leader's own gauge must match the configuration, or the cell is not what its name says (#595 rule 4)."""
    match = re.search(rf"^{LEADER_GAUGE}\s+(\d+)\s*$", metrics, re.MULTILINE)
    if not match:
        raise Refused(f"the leader does not report {LEADER_GAUGE}, so the group-commit setting of {config} cannot "
                      "be verified")
    value = int(match.group(1))
    expected = ca.effective_group_commit(config, shipped)
    if expected is None:
        return  # the shipped default could not be read from the source: nothing to compare with
    if value != int(expected):
        shipped_says = ("" if config != "vtop-shipped" or shipped is None
                        else f" (the node's shipped default is {'on' if shipped else 'off'})")
        raise Refused(f"{config} needs {LEADER_GAUGE} {int(expected)}, the leader reports {value}{shipped_says}")


def client_image_name() -> str:
    """kc-crash-client:<12 hex>, named for the source, the Dockerfile and both pinned images it is built from."""
    digest = hashlib.sha256()
    for name in ("Dockerfile", "AckProducer.java"):
        with open(os.path.join(CLIENT_DIR, name), "rb") as handle:
            digest.update(name.encode() + b"\0" + handle.read() + b"\0")
    digest.update(KAFKA_IMAGE.encode() + b"\0" + JDK_IMAGE.encode())
    return f"kc-crash-client:{digest.hexdigest()[:12]}"


def ensure_client_image(rebuild: bool) -> str:
    """The producer image (see crash/client/Dockerfile), built when it is not there."""
    image = client_image_name()
    exists = sh(["docker", "image", "inspect", image], check=False).returncode == 0
    if rebuild or not exists:
        log(f"building {image} (the producer with a record size)")
        sh(["docker", "build", "-t", image, "--build-arg", f"KAFKA_IMAGE={KAFKA_IMAGE}",
            "--build-arg", f"JDK_IMAGE={JDK_IMAGE}", CLIENT_DIR], timeout=1800)
    return image


def anonymous_volumes(mounts_json: str, project: str) -> list[str]:
    """The names of the anonymous volumes in `docker inspect --format '{{json .Mounts}}'` output, one JSON
    array per line: a volume whose name is a 64-character hex id. Named volumes (this project's data
    volumes, `<project>_<name>`) are never in the answer."""
    found = []
    for line in mounts_json.splitlines():
        try:
            mounts = json.loads(line)
        except json.JSONDecodeError:
            continue
        for mount in mounts or []:
            name = mount.get("Name") or ""
            if re.fullmatch(r"[0-9a-f]{64}", name) and not name.startswith(project):
                found.append(name)
    return found


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
        "offered_rate": p.rate, "record_size": p.record_size, "client": client_label(p.record_size),
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
        try:
            b.cleanup()
        except Exception as exc:  # the next crash's reset removes what is left; this one's row stands
            log(f"cleaning up after {config}/{mode}/{crash} failed: {exc}")
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
    b.wait_until(trigger_at - CANARY_LEAD_S - CLOCK_LEAD_S)
    offset_ms, uncertainty_ms = b.clock_probe()
    b.wait_until(trigger_at - CANARY_LEAD_S)
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
    sentinel = ca.sentinel_value(crash)
    try:
        b.restart(config)
        end_before = b.wait_served()
        first = ca.diff_acked(acks, ca.parse_consumed(b.read_all(end_before) if end_before else "", prefix, sentinel))
        # The sentinel is produced, and then waited for until the partition's end offset has moved past what
        # it was: a leader that restarted serves what is cluster-committed, and a record acknowledged before
        # the crash may not be readable until a new commit advances that (D10). The read that decides the
        # loss is the one after it, so a read of a leader that is not yet serving is never counted as one.
        b.produce_sentinel(sentinel, end_before)
        row["recovery_seconds"] = round(time.monotonic() - started, 1)
        end_after = b.wait_served(at_least=end_before + 1)
        consumed = ca.parse_consumed(b.read_all(end_after), prefix, sentinel)
        if not any(c.kind == "sentinel" for c in consumed):
            # A consumer that failed or stopped early returns a short read; counting every acknowledged
            # record it did not reach as lost would turn a client fault into a durability result (review of
            # #946). The read that decides the loss must reach the sentinel, the partition's last record.
            raise ShortRead(f"the read-back did not reach the sentinel: {len(consumed)} record"
                            f"{'' if len(consumed) == 1 else 's'} read of the {end_after} the partition holds")
    except ShortRead as exc:
        row.update(recovery_seconds=round(time.monotonic() - started, 1))
        if row["status"] == "ok":
            row["status"] = "failed"
        row["note"] = ((row["note"] + "; " if row["note"] else "") + str(exc))[:600]
        return row
    except Exception as exc:
        row.update(needed_manual_repair=True, recovery_seconds=round(time.monotonic() - started, 1))
        if row["status"] == "ok":
            row["status"] = "failed"
        said = f"the cluster did not serve after the restart: {exc}"
        row["note"] = ((row["note"] + "; " if row["note"] else "") + said)[:600]
        return row
    final = ca.diff_acked(acks, consumed)
    row.update(
        read_records=sum(1 for c in consumed if c.kind != "sentinel"), served_end_before_sentinel=end_before,
        served_end_after_sentinel=end_after,
        read_missing_before_sentinel=first.acked_lost,
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
        command = ["docker", "compose", "-p", SPEC[system]["project"]]
        for name in compose_files(system, config):
            command += ["-f", os.path.join(HERE, name)]
        return sh([*command, *args], timeout=timeout, cwd=HERE, check=check, env={
            "VTOP_IMAGE": self.ctx["vtop_image"], "KAFKA_IMAGE": KAFKA_IMAGE,
            "CRASH_DATA_ROOT": self.ctx["data_root"], "CRASH_NODE_CPUS": str(self.ctx["node_cpus"]),
            "VTOP_LEADER_CONFIG": "./" + vtop_leader_file(config or self.config).replace(os.sep, "/")})

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
        if system == "vtop":
            self.observe_group_commit(config)
        if system == "kafka":
            self.client(["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", SPEC[system]["bootstrap"],
                         *topic_create_args(config)], timeout=120)
            self.observe_kafka(config)

    def observe_group_commit(self, config: str) -> None:
        """The leader's own word for group commit, once per configuration, and a refusal when it does not match."""
        done = sh(["docker", "exec", SPEC["vtop"]["containers"][0], "curl", "-sf", "http://127.0.0.1:9600/metrics"],
                  check=False, timeout=60)
        if done.returncode != 0:
            raise Refused(f"the leader's /metrics could not be read, so the group-commit setting of {config} "
                          f"cannot be verified: {(done.stderr or done.stdout).strip()[-200:]}")
        verify_group_commit(config, done.stdout, self.ctx.get("shipped_group_commit"))
        match = re.search(rf"^{LEADER_GAUGE}\s+(\d+)\s*$", done.stdout, re.MULTILINE)
        seen = self.ctx["env"]["observed"].setdefault("vtop_group_commit", {})
        if match and config not in seen:
            seen[config] = int(match.group(1))
            self.ctx["save_env"]()

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
        size = int(self.ctx.get("record_size", 0))
        if size > 0:
            image = self.ctx["client_image"]
            tool = ["/opt/kafka/bin/kafka-run-class.sh", "AckProducer"]
            env = ["-e", f"KAFKA_HEAP_OPTS={CLIENT_HEAP}", "-e", "CLASSPATH=/opt/ackproducer"]
            tail = ["--value-size", str(size)]
        else:
            image = KAFKA_IMAGE
            tool = ["/opt/kafka/bin/kafka-verifiable-producer.sh"]
            env = ["-e", f"KAFKA_HEAP_OPTS={CLIENT_HEAP}"]
            tail = []
        command = ["docker", "run", "--rm", "--name", PRODUCER, "--network", SPEC[self.system]["network"],
                   "--cpus", str(self.ctx["client_cpus"]), "--memory", self.ctx["client_memory"],
                   *env, "-v", f"{self.crash_raw}:/out", image,
                   *tool, "--bootstrap-server", SPEC[self.system]["bootstrap"],
                   "--topic", TOPIC, "--acks", "-1", "--command-config", "/out/producer.properties",
                   "--throughput", str(self.ctx["params"].rate), "--max-messages", str(max_messages),
                   "--value-prefix", prefix, *tail]
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
        """Remove the dead containers so nothing holds the data filesystem open, and the anonymous volumes the
        image declares for them (they would pile up, one set per crash). The data volumes are left alone, and
        removed only by the next reset, with the containers gone."""
        self.save_node_logs("killed")
        names = sh(["docker", "inspect", "--format", "{{json .Mounts}}", *SPEC[self.system]["containers"]],
                   check=False).stdout
        self.compose(self.system, "down", "--remove-orphans", config=self.config, check=False)
        for volume in anonymous_volumes(names, SPEC[self.system]["project"]):
            sh(["docker", "volume", "rm", volume], check=False)

    def restore(self) -> None:
        self.host.call("restore")

    def restart(self, config: str) -> None:
        try:
            self.compose(self.system, "up", "-d", "--wait", "--wait-timeout", "300", config=config, timeout=400)
        except RuntimeError as exc:
            raise Refused(f"compose up after the crash: {exc}"[:400]) from None

    def wait_served(self, at_least: int = 0, deadline_s: float = SERVE_DEADLINE_S) -> int:
        """The partition's end offset, once the cluster answers ListOffsets (the first served read) and the
        end offset is at least `at_least`."""
        deadline = time.monotonic() + deadline_s
        last = ""
        while time.monotonic() < deadline:
            done = self.client(["/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server",
                                SPEC[self.system]["bootstrap"], "--topic", TOPIC, "--time", "-1"], timeout=90,
                               check=False)
            match = re.search(rf"^{TOPIC}:0:(\d+)\s*$", done.stdout, re.MULTILINE)
            if done.returncode == 0 and match:
                end = int(match.group(1))
                if end >= at_least:
                    return end
                last = f"the end offset is {end}, below {at_least}"
            else:
                last = (done.stderr or done.stdout).strip()[-200:]
            time.sleep(2)
        raise Refused(f"the cluster did not serve within {deadline_s:.0f} s of the restart: {last}")

    def read_all(self, end: int) -> str:
        done = self.client(["/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server",
                            SPEC[self.system]["bootstrap"], "--topic", TOPIC, "--partition", "0", "--offset",
                            "earliest", "--max-messages", str(end), "--timeout-ms", str(READ_IDLE_MS),
                            "--property", "print.offset=true"], timeout=900, check=False)
        with open(os.path.join(self.crash_raw, f"consumer-{end}.out"), "w") as handle:
            handle.write(done.stdout)
        return done.stdout

    def produce_sentinel(self, value: str, end_before: int) -> None:
        """Produce the sentinel with `acks=all` and wait until the end offset has moved past `end_before`. The
        console producer's exit status is not a proof of an acknowledgement (it exits 0 after a send that was
        refused), and a fresh idempotent client is refused by VTOP's gateway while the old producer's state
        stands, so what is checked is the effect: the partition grew. Retried until it does."""
        deadline = time.monotonic() + SENTINEL_DEADLINE_S
        last = ""
        while time.monotonic() < deadline:
            done = self.client(["/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server",
                                SPEC[self.system]["bootstrap"], "--topic", TOPIC, "--command-property", "acks=all"],
                               timeout=180, input_text=value + "\n", check=False)
            last = (done.stderr or done.stdout).strip()[-200:]
            if done.returncode == 0:
                try:
                    self.wait_served(at_least=end_before + 1, deadline_s=20)
                    return
                except Refused as exc:
                    last = str(exc)
            time.sleep(3)
        raise Refused(f"no write became readable within {SENTINEL_DEADLINE_S:.0f} s of the restart: {last}")

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
    c["host_instance"] = instance_type()  # the DMI's word; host_metadata.instance_type is IMDSv2's
    c["node_limits"] = {**limits, "cpus": str(args.node_cpus)}
    c["client_limits"] = {"cpus": str(args.client_cpus), "memory": args.client_memory, "heap": CLIENT_HEAP}
    size = int(matrix["record_size"])
    version = KAFKA_IMAGE.split("@")[0].split(":")[-1]
    c["record_size"] = size
    c["client"] = ((f"AckProducer {version} (crash/client: kafka-clients from the pinned image, values padded to "
                    f"{size} bytes)" if size > 0 else f"kafka-verifiable-producer.sh {version} (JVM)")
                   + ", the same invocation against both systems; reads by kafka-console-consumer.sh")
    shipped = shipped_group_commit()
    c["vtop_group_commit_shipped_default"] = shipped
    c["producer_props"] = ["acks=all (--acks -1)", *PRODUCER_PROPS,
                           "every other producer setting at the client default"]
    c["profiles"] = {
        "kafka-defaults": "Kafka as shipped: no flush.messages, an acknowledgement is replication",
        "kafka-fsync-parity": "log.flush.interval.messages=1 on every broker and flush.messages=1 on the topic",
        "vtop-shipped": "node configuration as shipped (no group_commit section): acknowledged after a quorum "
                        "(2 of 3) has fsynced; answers both Kafka profiles",
        "vtop-group-commit-on": "the shipped leader plus group_commit.enabled: true (concurrent produce requests "
                                "share one fsync and one replication round)",
        "vtop-group-commit-off": "the shipped leader plus group_commit.enabled: false (each produce request has "
                                 "its own barrier)",
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
        "vtop_leader_group_commit_on": phase1._read(os.path.join(HERE, vtop_leader_file("vtop-group-commit-on"))),
        "vtop_leader_group_commit_off": phase1._read(os.path.join(HERE, vtop_leader_file("vtop-group-commit-off"))),
        "ack_producer_java": phase1._read(os.path.join(CLIENT_DIR, "AckProducer.java")),
        "ack_producer_dockerfile": phase1._read(os.path.join(CLIENT_DIR, "Dockerfile")),
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
    parser.add_argument("--configs", default=None,
                        help="comma-separated, from " + ", ".join(ca.CONFIGS) + "; default: both Kafka profiles, "
                             "vtop-shipped and the group-commit state that differs from the node's shipped default")
    parser.add_argument("--record-size", type=int, default=DEFAULT_RECORD_SIZE, metavar="BYTES",
                        help="bytes per record, the same for both systems (default 1024, the issue's); 0 uses "
                             "kafka-verifiable-producer, which sends a few bytes and has no size option")
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
    if args.configs is None:
        configs = ca.default_configs(shipped_group_commit())
        if configs is None:
            raise SystemExit(f"--configs: {NODE_CONFIG_RS} does not say whether the node ships with group commit on, "
                             "so the default pair of VTOP configurations cannot be chosen; name the configurations "
                             f"({', '.join(ca.CONFIGS)})")
        configs = list(configs)
    else:
        configs = [c for c in args.configs.split(",") if c]
    return {"configs": configs, "crash_modes": [m for m in modes.split(",") if m], "record_size": args.record_size,
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
    """The crash run, with SIGTERM handled as Ctrl-C is for its whole length (run.sigterm_raises)."""
    with phase1.sigterm_raises():
        return _main(argv)


def entry(argv: list[str] | None = None) -> int:
    """The command line: a run ended by SIGTERM exits 143 after recording it."""
    try:
        return main(argv)
    except phase1.Terminated as exc:
        return 128 + exc.signum


def _main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    phase1.checked_host_disk(args.host_disk)
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
    for note in redundant_configs(matrix["configs"], shipped_group_commit()):
        log(f"note: {note}")
    disk = phase1.resolve_host_disk(args.host_disk)
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
    client_image = ensure_client_image(args.rebuild) if matrix["record_size"] > 0 else None

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
    params = Params(rate=args.rate, seed=args.seed, record_size=matrix["record_size"])
    ctx = {"run_dir": run_dir, "env": env, "host": host, "vtop_image": vtop_image, "data_root": args.data_root,
           "record_size": matrix["record_size"], "client_image": client_image,
           "shipped_group_commit": shipped_group_commit(),
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
        env["events"].append(phase1.interruption_event(exc))
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
    sys.exit(entry())
