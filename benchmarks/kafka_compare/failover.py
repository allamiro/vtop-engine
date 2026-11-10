#!/usr/bin/env python3
"""Failover under load, VTOP vs Apache Kafka (#597), on this Docker host.

    python3 benchmarks/kafka_compare/failover.py --reps 5

For every (fault, repetition), Kafka then VTOP: a fresh three-node cluster, a
producer at a fixed rate with acks=all, the fault on the partition's leader
35 s in (5 s warm-up, then the 30 s "before" window), 60 s more of load, then
a read of the whole partition to check every acknowledged record. One row
per repetition in failover.csv, its ack-rate timeline in timeline.csv.

THE TWO CLIENT PATHS DIFFER, and every output says so. A Kafka client cannot
follow a VTOP failover: the gateway is refused on a candidate node, and only a
candidate takes a range over in place (see the report). So:

- Kafka: `kafka-verifiable-producer.sh` (the JVM client from the pinned
  apache/kafka image), bootstrapped at all three brokers, idempotent by
  default, every client timeout at its default;
- VTOP: `vtop-node produce` (the native client), driven by
  vtop/failover-driver.sh, which looks the leader up in the range lease and
  resumes from the acknowledged floor after an early exit.

Faults: kill9_leader (`docker kill -s KILL`), stop_leader (`docker stop -t
30`: SIGTERM, Kafka's controlled shutdown / VTOP's orderly shutdown),
partition_leader (`docker network disconnect`, healed after 30 s). The
faulted node is left down (kill, stop) for the rest of the repetition.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from collections import Counter
from typing import Any

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from kafka_compare import failover_schema as fschema  # noqa: E402
from kafka_compare import failover_stats as fstats  # noqa: E402
from kafka_compare import perftool, schema  # noqa: E402
from kafka_compare import run as phase1  # noqa: E402

REPO = phase1.REPO
KAFKA_IMAGE = phase1.KAFKA_IMAGE
PYTHON_IMAGE = "python:3.13-slim@sha256:bf44cdfcb76cd3b41e879bc058fc37ec5872002ccfde7fcb765e218cde0cd79c"
PROFILE = "defaults"
TOPIC = "bench"
CLIENT_CPUS = phase1.CLIENT_CPUS
CLIENT_MEMORY = phase1.CLIENT_MEMORY
CLIENT_HEAP = phase1.CLIENT_HEAP

WARMUP_S = 5
PRE_S = 30
POST_S = 60
HEAL_AFTER_S = 30
STOP_GRACE_S = 30
BIN_MS = 100
TIMELINE_FROM_MS = -10_000
TIMELINE_TO_MS = 60_000
DRAIN_DEADLINE_S = 240

TOPIC_UUID = "aaaaaaaa-0000-0000-0000-0000000000b1"
RANGE_UUID = "aaaaaaaa-0000-0000-0000-0000000000c1"
NODE_UUID = {n: f"aaaaaaaa-0000-0000-0000-0000000000a{n}" for n in (1, 2, 3)}

SPEC = {
    "kafka": {"compose": "compose.kafka.yml", "project": "kc-kafka", "network": "kc-kafka-bench",
              "container": "kc-kafka-kafka-{n}-1", "alias": "kafka-{n}",
              "bootstrap": "kafka-1:9092,kafka-2:9092,kafka-3:9092"},
    "vtop": {"compose": "compose.vtop-ha.yml", "project": "kc-vtop-ha", "network": "kc-vtop-ha-bench",
             "container": "kc-vtop-ha-vtop-{n}-1", "alias": "vtop-{n}"},
}
PRODUCER = "kc-fo-producer"
OBSERVER = "kc-fo-observer"
# kafka-verifiable-producer.sh sets retries=0 for itself, which also turns
# idempotence off: every batch in flight at a fault would fail at once, which
# is not how a Kafka producer behaves with the client's defaults. Both are put
# back to the client defaults (found in the trial run, where 65 records failed
# with "Disconnected from node 3" the instant the leader died).
KAFKA_PRODUCER_PROPS = ("retries=2147483647", "enable.idempotence=true")

log = phase1.log
sh = phase1.sh
utc_now = phase1.utc_now


class Refused(Exception):
    """The repetition could not run as specified; the message goes in `note`."""


class Incomplete(Exception):
    """The producer did not acknowledge every record it was asked to send: a failed
    repetition, whose truncated latencies and gaps are not a measurement."""


def check_complete(row: dict[str, Any], producer_finished: bool, how: str) -> None:
    """Refuse to summarise a repetition whose producer did not finish, or finished short.

    A producer stopped at the drain deadline leaves its last outage unresolved
    (no acknowledgement ends the gap) and its unsent records out of the latency
    percentiles, so the numbers would describe a better run than the one that
    happened. The read-back still ran and stays in the row.
    """
    acked, target = row.get("records_acked"), row["records_target"]
    if not producer_finished or acked != target:
        raise Incomplete(f"producer incomplete ({how}): {acked} of {target} records acknowledged")


# ----------------------------------------------------------------------- environment


def check_equal_limits() -> dict[str, str]:
    kafka = phase1._compose_limits(os.path.join(HERE, "compose.kafka.yml"))
    vtop = phase1._compose_limits(os.path.join(HERE, "compose.vtop-ha.yml"))
    if kafka != vtop:
        raise SystemExit(f"per-node limits differ (rule 1): kafka {kafka} vs vtop {vtop}")
    return kafka


def engine_commit() -> str:
    """The last commit that changed what the VTOP image is built from: the image is
    tagged by it, so documentation and harness commits do not force a rebuild."""
    return phase1.git("log", "-1", "--format=%H", "--", "crates", "Cargo.toml", "Cargo.lock", "docker")


def ensure_vtop_image(rebuild: bool) -> str:
    dirty = phase1.dirty_build_inputs()
    if dirty:
        raise SystemExit(f"the engine's build inputs differ from HEAD ({'; '.join(dirty[:5])}): "
                         "commit or stash them, so the image benchmarked is the commit recorded")
    image = f"vtop-bench:{engine_commit()[:12]}"
    exists = sh(["docker", "image", "inspect", image], check=False).returncode == 0
    if rebuild or not exists:
        log(f"building {image} from docker/Dockerfile")
        sh(["docker", "build", "-f", os.path.join(REPO, "docker", "Dockerfile"), "-t", image, REPO], timeout=3600)
    return image


def capture_environment(run_id: str, matrix: dict, vtop_image: str, limits: dict) -> dict:
    env = phase1.capture_environment(run_id, matrix, vtop_image, limits)
    c = env["comparable"]
    c["vtop_commit"] = engine_commit()
    c["harness_commit"] = phase1.git("rev-parse", "HEAD")
    c["profile"] = PROFILE
    c["client"] = "kafka-verifiable-producer (JVM) vs vtop-node produce (native); NOT the same client path"
    c["client_kafka"] = f"kafka-verifiable-producer.sh {KAFKA_IMAGE.split('@')[0].split(':')[-1]} (JVM)"
    c["client_vtop"] = "vtop-node produce (native protocol, TLS) via vtop/failover-driver.sh"
    c["observer_image"] = PYTHON_IMAGE
    # The two transports differ, and reconnect timings carry it: every VTOP
    # plane is mutual TLS (promotion is refused on a plaintext replica plane),
    # Kafka's listeners are PLAINTEXT as compose.kafka.yml ships them.
    c["transport"] = "Kafka: PLAINTEXT on every listener; VTOP: mutual TLS on every plane"
    # How `vtop-node produce --rate` paces a batch: it goes out once its LAST
    # record is due (#613 review), so no record leaves before its schedule.
    c["vtop_pacing"] = "last-record-due"
    # Where its ack log stamps an acknowledgement: on the answer, before the
    # acked-floor file's fsync (#613 review), not after it.
    c["vtop_ack_stamp"] = "on-answer"
    c["producer_props"] = ["acks=all (--acks -1)", *KAFKA_PRODUCER_PROPS,
                           "every other producer setting at the client default"]
    c.pop("consumer_props", None)
    env["kind"] = "failover"
    env["label"] = "single host, Docker Desktop; indicative, not a production benchmark"
    env["configs"] = {
        "kafka_compose": phase1._read(os.path.join(HERE, "compose.kafka.yml")),
        "vtop_compose": phase1._read(os.path.join(HERE, "compose.vtop-ha.yml")),
        **{f"vtop_node_{n}": phase1._read(os.path.join(HERE, "vtop", f"ha-node-{n}.yaml")) for n in (1, 2, 3)},
        "vtop_driver": phase1._read(os.path.join(HERE, "vtop", "failover-driver.sh")),
    }
    env["timeouts"] = {
        "kafka_client": "request.timeout.ms, delivery.timeout.ms, retry.backoff.ms, metadata.max.age.ms: see "
                        "observed.kafka_producer_config (the client's own dump)",
        "kafka_broker": "broker.session.timeout.ms, replica.lag.time.max.ms: see observed.kafka_broker_1_config",
        "vtop_lease": {"lease_duration_ms": 15000, "renew_interval_ms": 5000, "poll_interval_ms": 2000,
                       "source": "binary defaults (crates/vtop-node/src/config.rs), no override in "
                                 "vtop/ha-node-*.yaml"},
        "vtop_raft": {"election_timeout_min_ms": 300, "election_timeout_max_ms": 600, "heartbeat_interval_ms": 60,
                      "source": "binary defaults"},
        "vtop_driver": {"request_timeout_ms": 30000, "backoff_ms": 100,
                        "source": "vtop/failover-driver.sh, set to the JVM client's request.timeout.ms and "
                                  "retry.backoff.ms defaults"},
        "vtop_lease_observer": {"poll_ms": 100,
                                "source": "vtop/failover-driver.sh LEASE_POLL_SECONDS: the lease observer, which "
                                          "feeds only the time to new leader, never the producer"},
        "kafka_observer_poll_ms": 100,
    }
    return env


# --------------------------------------------------------------------------- clusters


def compose(system: str, *args: str, ctx: dict, timeout: float = 600) -> subprocess.CompletedProcess:
    return sh(["docker", "compose", "-f", os.path.join(HERE, SPEC[system]["compose"]), *args],
              env={"VTOP_IMAGE": ctx["vtop_image"], "KAFKA_IMAGE": KAFKA_IMAGE, "VTOP_TLS_DIR": ctx["certs"]},
              timeout=timeout, cwd=HERE)


def remove_helpers() -> None:
    for name in (PRODUCER, OBSERVER):
        sh(["docker", "rm", "-f", name], check=False)


def cluster_down(system: str, ctx: dict) -> None:
    remove_helpers()
    compose(system, "down", "-v", "--remove-orphans", ctx=ctx)


def container(system: str, node: int) -> str:
    return SPEC[system]["container"].format(n=node)


def vtop_tool(ctx: dict, raw: str, *args: str, entrypoint: str = "vtopctl", timeout: float = 60,
              check: bool = False) -> subprocess.CompletedProcess:
    return sh(["docker", "run", "--rm", "--network", SPEC["vtop"]["network"], "-v", f"{ctx['certs']}:/etc/vtop/tls:ro",
               "-v", f"{raw}:/out", "--entrypoint", entrypoint, ctx["vtop_image"], *args], timeout=timeout,
              check=check)


def write_admin_configs(raw: str) -> None:
    for n in (1, 2, 3):
        with open(os.path.join(raw, f"admin-{n}.yaml"), "w") as handle:
            handle.write(f'endpoint: "vtop-{n}:9200"\nserver_name: "localhost"\nca_cert: /etc/vtop/tls/ca.pem\n'
                         "client_cert: /etc/vtop/tls/admin.pem\nclient_key: /etc/vtop/tls/admin-key.pem\n")


def vtop_meta(ctx: dict, raw: str, *args: str) -> dict:
    """One `vtopctl meta` call, tried at each voter in turn; the first JSON answer."""
    last = ""
    for n in (1, 2, 3):
        done = vtop_tool(ctx, raw, "--json", "meta", *args, "--config", f"/out/admin-{n}.yaml", timeout=30)
        if done.returncode == 0:
            try:
                return json.loads(done.stdout)
            except json.JSONDecodeError:
                last = done.stdout[-300:]
                continue
        last = (done.stderr or done.stdout).strip()[-300:]
    raise RuntimeError(f"vtopctl meta {' '.join(args[:2])} answered at no voter: {last}")


def vtop_lease(ctx: dict, raw: str) -> tuple[str, int] | None:
    answer = vtop_meta(ctx, raw, "range-lease", "--topic-uuid", TOPIC_UUID, "--range-uuid", RANGE_UUID)
    lease = answer.get("lease")
    if not lease:
        return None
    return lease["holder_node_uuid"], int(lease["fencing_epoch"])


def vtop_up(ctx: dict, raw: str) -> tuple[int, int]:
    """A fresh co-located cluster, its metadata group initialised, the topic created and the
    three nodes registered; returns (leader node, its epoch) once a candidate holds the range."""
    cluster_down("vtop", ctx)
    compose("vtop", "up", "-d", ctx=ctx, timeout=300)
    write_admin_configs(raw)
    deadline = time.monotonic() + 60
    while True:
        done = vtop_tool(ctx, raw, "--json", "meta", "init", "--members", "1,2,3", "--config", "/out/admin-1.yaml")
        if done.returncode == 0:
            break
        if time.monotonic() > deadline:
            raise Refused(f"metadata init never succeeded: {(done.stderr or done.stdout).strip()[-300:]}")
        time.sleep(1)
    deadline = time.monotonic() + 60
    while True:
        try:
            if vtop_meta(ctx, raw, "status").get("current_leader"):
                break
        except RuntimeError:
            pass
        if time.monotonic() > deadline:
            raise Refused("no metadata leader within 60 s of init")
        time.sleep(0.5)
    vtop_meta(ctx, raw, "create-topic", "--name", TOPIC, "--topic-uuid", TOPIC_UUID, "--root-range-uuid", RANGE_UUID)
    for n in (1, 2, 3):
        vtop_meta(ctx, raw, "register-node", "--node-uuid", NODE_UUID[n], "--addr", f"vtop-{n}:9400")
    deadline = time.monotonic() + 90
    while True:
        try:
            lease = vtop_lease(ctx, raw)
        except RuntimeError:
            lease = None
        if lease:
            holder, epoch = lease
            return int(holder[-1]), epoch
        if time.monotonic() > deadline:
            raise Refused("no candidate acquired the range within 90 s of registration")
        time.sleep(0.5)


def kafka_up(ctx: dict) -> None:
    cluster_down("kafka", ctx)
    compose("kafka", "up", "-d", "--wait", "--wait-timeout", "300", ctx=ctx, timeout=400)
    phase1.client("kafka", ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka-1:9092", "--create",
                            "--topic", TOPIC, "--partitions", "1", "--replication-factor", "3",
                            "--config", "min.insync.replicas=2"], timeout=120)


def start_kafka_observer(raw: str) -> None:
    sh(["docker", "run", "-d", "--name", OBSERVER, "--network", SPEC["kafka"]["network"], "-v", f"{raw}:/out",
        "-v", f"{os.path.join(HERE, 'metadata_probe.py')}:/probe.py:ro", PYTHON_IMAGE, "python3", "/probe.py",
        "--brokers", SPEC["kafka"]["bootstrap"], "--topic", TOPIC, "--out", "/out/metadata.jsonl",
        "--stop-file", "/out/stop", "--interval", "0.1"])


def kafka_settled_leader(raw: str, timeout: float = 60) -> int:
    """The leader all three brokers name, with all three replicas in the ISR."""
    deadline = time.monotonic() + timeout
    path = os.path.join(raw, "metadata.jsonl")
    while time.monotonic() < deadline:
        latest = {}
        if os.path.exists(path):
            with open(path) as handle:
                for line in handle:
                    event = json.loads(line)
                    latest[event["broker"]] = event
        views = {(e.get("leader"), tuple(e.get("isr") or ())) for e in latest.values() if e.get("ok")}
        if len(latest) == 3 and all(e.get("ok") for e in latest.values()) and len(views) == 1:
            leader, isr = views.pop()
            if leader is not None and leader >= 0 and len(isr) == 3:
                return leader
        time.sleep(0.2)
    raise Refused(f"the brokers never agreed on a leader with a full ISR within {timeout:.0f} s")


def kafka_controller() -> int | None:
    done = phase1.client("kafka", ["/opt/kafka/bin/kafka-metadata-quorum.sh", "--bootstrap-server",
                                   SPEC["kafka"]["bootstrap"], "describe", "--status"], timeout=120, check=False)
    for line in done.stdout.splitlines():
        if line.startswith("LeaderId:"):
            return int(line.split(":", 1)[1])
    return None


def clock_probe(name: str) -> tuple[float, float]:
    samples = []
    for _ in range(7):
        before = time.time_ns()
        done = sh(["docker", "exec", name, "date", "+%s%N"], check=False)
        after = time.time_ns()
        if done.returncode == 0 and done.stdout.strip().isdigit():
            samples.append((before, int(done.stdout.strip()), after))
    return fstats.clock_offset(samples)


def inject(system: str, fault: str, node: int) -> float:
    """Apply the fault to `node`; returns its host-clock start in unix ms."""
    name = container(system, node)
    started = time.time_ns() / 1e6
    if fault == "kill9_leader":
        sh(["docker", "kill", "--signal", "KILL", name])
    elif fault == "stop_leader":
        sh(["docker", "stop", "-t", str(STOP_GRACE_S), name], timeout=STOP_GRACE_S + 60)
    elif fault == "partition_leader":
        sh(["docker", "network", "disconnect", SPEC[system]["network"], name])
    else:
        raise ValueError(fault)
    return started


def heal(system: str, node: int) -> None:
    sh(["docker", "network", "connect", "--alias", SPEC[system]["alias"].format(n=node), SPEC[system]["network"],
        container(system, node)])


def save_node_logs(system: str, logs: str, full: bool) -> None:
    """Each node's log: whole when `full` (a VTOP run whose read-back failed), else the lines
    that tell the failover story plus the tail."""
    keep = ("role", "lease", "promot", "fence", "boundary", "ready", "shutdown", "WARN", "ERROR", "leader",
            "Leader", "ISR", "isr", "elect", "Fenc", "Shutting down", "controlled")
    for n in (1, 2, 3):
        done = sh(["docker", "logs", "-t", container(system, n)], check=False)
        text = (done.stdout + done.stderr).splitlines()
        if not full:
            # Raft's per-heartbeat replication errors to a dead peer (two a
            # second, each several lines) would push the promotion lines out
            # of any window; they say nothing the role lines do not.
            picked = [line for line in text if any(word in line for word in keep)
                      and "openraft::replication" not in line and "replication_handler" not in line]
            text = picked[-600:] + ["--- tail ---"] + text[-40:]
        with open(os.path.join(logs, f"node-{n}.log"), "w") as handle:
            handle.write("\n".join(text) + "\n")


def save_vtop_evidence(ctx: dict, raw: str, evidence: str) -> None:
    """Everything a VTOP run with a failed read-back left: raw client data and each node's volume."""
    os.makedirs(evidence, exist_ok=True)
    for name in os.listdir(raw):
        path = os.path.join(raw, name)
        if os.path.isfile(path):
            shutil.copy2(path, evidence)
    for n in (1, 2, 3):
        volume = f"{SPEC['vtop']['project']}_vtop-{n}-data"
        sh(["docker", "run", "--rm", "-v", f"{volume}:/d:ro", "-v", f"{evidence}:/e", "--entrypoint", "tar",
            ctx["vtop_image"], "czf", f"/e/vtop-{n}-volume.tgz", "-C", "/d", "."], check=False, timeout=600)


# ------------------------------------------------------------------------ repetitions


def base_row(ctx: dict, system: str, fault: str, rep: int) -> dict[str, Any]:
    c = ctx["env"]["comparable"]
    return {"run_id": ctx["run_id"], "env_fingerprint": ctx["fingerprint"], "vtop_commit": c["vtop_commit"],
            "kafka_image": KAFKA_IMAGE, "profile": PROFILE, "system": system,
            "client_path": fschema.CLIENT_PATHS[system], "fault": fault, "repetition": rep,
            "offered_rate": ctx["rate"], "records_target": ctx["total"], "started_at": utc_now()}


def wait_until(t_unix_s: float) -> None:
    delay = t_unix_s - time.time()
    if delay > 0:
        time.sleep(delay)


def write_producer_properties(raw: str) -> None:
    with open(os.path.join(raw, "producer.properties"), "w") as handle:
        handle.write("".join(f"{line}\n" for line in KAFKA_PRODUCER_PROPS))


def verifiable_producer(raw: str, *args: str, info_logging: bool = False, name: str = PRODUCER) -> list[str]:
    env = ["-e", f"KAFKA_HEAP_OPTS={CLIENT_HEAP}"]
    if info_logging:
        env += ["-e", "KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=/opt/kafka/config/log4j2.yaml"]
    return ["docker", "run", "--rm", "--name", name, "--network", SPEC["kafka"]["network"], "--cpus", CLIENT_CPUS,
            "--memory", CLIENT_MEMORY, *env, "-v", f"{raw}:/out", KAFKA_IMAGE,
            "/opt/kafka/bin/kafka-verifiable-producer.sh", "--bootstrap-server", SPEC["kafka"]["bootstrap"],
            "--topic", TOPIC, "--acks", "-1", "--command-config", "/out/producer.properties", *args]


def run_kafka(ctx: dict, fault: str, raw: str, logs: str, row: dict) -> tuple[list, dict]:
    kafka_up(ctx)
    write_producer_properties(raw)
    if "kafka_broker_1_config" not in ctx["env"]["observed"]:
        phase1._observe_kafka({"env": ctx["env"]})
        observe_kafka_producer_config(ctx, raw)
    start_kafka_observer(raw)
    leader = kafka_settled_leader(raw)
    controller = kafka_controller()
    row.update(leader_node=leader, leader_was_controller=None if controller is None else controller == leader)
    out_path = os.path.join(raw, "producer.out")
    err_path = os.path.join(raw, "producer.err")
    command = verifiable_producer(raw, "--throughput", str(ctx["rate"]), "--max-messages", str(ctx["total"]))
    stopped = False
    with open(out_path, "w") as out, open(err_path, "w") as err:
        producer = subprocess.Popen(command, stdout=out, stderr=err)
    launched = time.time()
    fault_at = launched + 2 + WARMUP_S + PRE_S  # ~2 s of JVM start before the schedule's origin
    try:
        wait_until(fault_at - 3)
        row["clock_offset_ms"], row["clock_uncertainty_ms"] = clock_probe(OBSERVER)
        wait_until(fault_at)
        fault_ms = inject("kafka", fault, leader)
        row["fault_at"] = dt.datetime.fromtimestamp(fault_ms / 1000, dt.timezone.utc).isoformat()
        if fault == "partition_leader":
            wait_until(fault_ms / 1000 + HEAL_AFTER_S)
            heal("kafka", leader)
        try:
            producer.wait(timeout=POST_S + DRAIN_DEADLINE_S)
        except subprocess.TimeoutExpired:
            sh(["docker", "rm", "-f", PRODUCER], check=False)
            producer.wait()
            row["note"] = f"producer still running {POST_S + DRAIN_DEADLINE_S} s after the fault; stopped"
            stopped = True
    finally:
        if producer.poll() is None:
            sh(["docker", "rm", "-f", PRODUCER], check=False)
            producer.wait()
    with open(os.path.join(raw, "stop"), "w"):
        pass
    sh(["docker", "rm", "-f", OBSERVER], check=False)

    with open(out_path) as handle:
        run = fstats.parse_verifiable_producer(handle)
    with open(err_path) as handle:
        stderr = handle.read()
    consumed = phase1.client("kafka", [
        "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", SPEC["kafka"]["bootstrap"], "--topic",
        TOPIC, "--partition", "0", "--offset", "earliest", "--timeout-ms", "20000",
        "--property", "print.offset=true"], timeout=600, check=False)
    with open(os.path.join(raw, "consumer.out"), "w") as handle:
        handle.write(consumed.stdout)
    records = fstats.parse_console_consumer(consumed.stdout)
    check = fstats.kafka_loss_check([a.first_seq for a in run.acks], [value for _, value in records])
    warnings = fstats.kafka_client_warnings(stderr)
    fault_vm_ms = fault_ms + row["clock_offset_ms"]
    with open(os.path.join(raw, "metadata.jsonl")) as handle:
        leader_seen = fstats.kafka_time_to_leader(handle, fault_vm_ms, leader, row["clock_uncertainty_ms"])
    if leader_seen:
        row.update(time_to_leader_ms=round(leader_seen[0], 1), new_leader_node=leader_seen[1])
    else:
        row["note"] = (row.get("note", "") + "; no broker named a new leader during the run").lstrip("; ")
    row.update(send_errors=len(run.errors), retries=warnings["retries"], reroutes=None,
               error_classes=kafka_error_classes(warnings, run.errors),
               acked_lost=check.acked_lost, duplicates=check.duplicates, unacked_present=check.unacked_present,
               records_acked=len({a.first_seq for a in run.acks}))
    if check.acked_lost or check.duplicates or check.unexpected:
        row["verify"] = (f"FAILED: {check.acked_lost} acknowledged records missing (first {check.first_lost}), "
                         f"{check.duplicates} duplicates, {check.unexpected} unexpected values, "
                         f"{len(records)} records read")
        log(f"KAFKA READ-BACK FAILED ({row['verify']}): keeping the producer and consumer output")
        evidence = os.path.join(logs, "evidence")
        os.makedirs(evidence, exist_ok=True)
        for name in ("producer.out", "consumer.out"):
            with open(os.path.join(evidence, f"{name}.gz"), "wb") as handle:
                subprocess.run(["gzip", "-9", "-c", os.path.join(raw, name)], stdout=handle, check=False)
    else:
        row["verify"] = "passed"
    for name, keep in (("producer.err", True), ("metadata.jsonl", True)):
        if keep:
            shutil.copy2(os.path.join(raw, name), os.path.join(logs, name))
    with open(os.path.join(logs, "consumer.err"), "w") as handle:
        handle.write(consumed.stderr[-4000:])
    with open(os.path.join(logs, "summary.json"), "w") as handle:
        json.dump({"tool_data": run.tool_data, "non_json_stdout_lines": run.non_json_lines,
                   "records_read": len(records), "loss_check": check._asdict(),
                   "first_send_errors": run.errors[:5]}, handle, indent=2, sort_keys=True)
    finished = not stopped and producer.returncode == 0
    how = "stopped at the drain deadline" if stopped else f"exit {producer.returncode}"
    return run.acks, {"origin_ms": run.startup_ms, "fault_vm_ms": fault_vm_ms, "finished": finished, "how": how}


def kafka_error_classes(warnings: dict[str, int], errors: list[tuple[float, str, str]]) -> str:
    """The Kafka row's `error_classes` cell: the client's warning counts, then each
    exception class the producer reported a failed send with, all as `label=count`,
    the form `failover_report.error_classes` reads (a bare label is dropped there)."""
    sends = Counter(f"send_error:{exception}" for _, exception, _ in errors)
    classes = [f"{k}={v}" for k, v in warnings.items() if v] + [f"{k}={v}" for k, v in sorted(sends.items())]
    return "; ".join(classes)


def observe_kafka_producer_config(ctx: dict, raw: str) -> None:
    """The JVM producer's own dump of its resolved config (INFO), once per run set (rule 8).
    Zero records: anything it sent would be in the measured partition."""
    done = sh(verifiable_producer(raw, "--max-messages", "0", info_logging=True, name="kc-fo-config"),
              timeout=120, check=False)
    try:
        config = perftool.parse_client_config(done.stdout + "\n" + done.stderr)
    except perftool.PerfParseError as exc:
        config = {"error": str(exc)}
    ctx["env"]["observed"]["kafka_producer_config"] = config


def run_vtop(ctx: dict, fault: str, raw: str, logs: str, row: dict) -> tuple[list, dict]:
    leader, epoch = vtop_up(ctx, raw)
    status = vtop_meta(ctx, raw, "status")
    meta_leader = status.get("current_leader")
    row.update(leader_node=leader, leader_was_controller=None if meta_leader is None else int(meta_leader) == leader)
    anchor_us = int((time.time() + 3) * 1e6)
    os.chmod(raw, 0o777)
    driver = os.path.join(HERE, "vtop", "failover-driver.sh")
    sh(["docker", "run", "-d", "--name", PRODUCER, "--network", SPEC["vtop"]["network"], "--cpus", CLIENT_CPUS,
        "--memory", CLIENT_MEMORY, "-v", f"{ctx['certs']}:/etc/vtop/tls:ro", "-v", f"{raw}:/out",
        "-v", f"{driver}:/driver.sh:ro", "-e", f"TOTAL={ctx['total']}", "-e", f"RATE={ctx['rate']}",
        "-e", f"BATCH={ctx['vtop_batch']}", "-e", f"VALUE_BYTES={ctx['vtop_value_bytes']}",
        "-e", f"ANCHOR_US={anchor_us}", "--entrypoint", "bash", ctx["vtop_image"], "/driver.sh"])
    fault_at = anchor_us / 1e6 + WARMUP_S + PRE_S
    wait_until(fault_at - 3)
    row["clock_offset_ms"], row["clock_uncertainty_ms"] = clock_probe(PRODUCER)
    wait_until(fault_at)
    fault_ms = inject("vtop", fault, leader)
    row["fault_at"] = dt.datetime.fromtimestamp(fault_ms / 1000, dt.timezone.utc).isoformat()
    if fault == "partition_leader":
        wait_until(fault_ms / 1000 + HEAL_AFTER_S)
        heal("vtop", leader)
    try:
        sh(["docker", "wait", PRODUCER], check=False, timeout=POST_S + DRAIN_DEADLINE_S)
    except subprocess.TimeoutExpired:
        pass  # judged by the `done` marker below; the driver is removed either way
    finished = os.path.exists(os.path.join(raw, "done"))
    if not finished:
        row["note"] = f"driver did not finish within {POST_S + DRAIN_DEADLINE_S} s after the fault"
    sh(["docker", "rm", "-f", PRODUCER], check=False)

    with open(os.path.join(raw, "acks.log")) as handle:
        acks = fstats.parse_ack_log(handle.read())
    with open(os.path.join(raw, "lease.log")) as handle:
        lease_lines = handle.read().splitlines()
    with open(os.path.join(raw, "driver.log")) as handle:
        driver_log = handle.read()
    fault_vm_ms = fault_ms + row["clock_offset_ms"]
    leader_seen = fstats.vtop_time_to_leader(lease_lines, fault_vm_ms, NODE_UUID[leader], row["clock_uncertainty_ms"])
    if leader_seen:
        row.update(time_to_leader_ms=round(leader_seen[0], 1), new_leader_node=int(leader_seen[1][-1]))
    else:
        row["note"] = (row.get("note", "") + "; the lease never named a new holder during the run").lstrip("; ")
    summary = fstats.driver_summary(driver_log)
    acked = acks[-1].first_seq + acks[-1].records if acks else 0
    row.update(send_errors=summary["failed_attempts"], retries=max(0, summary["attempts"] - 1),
               reroutes=max(0, summary["leaders_used"] - 1),
               error_classes="; ".join(f"{k}={v}" for k, v in summary["reasons"].items()), records_acked=acked)

    # Read the whole range back from whoever holds it now, byte-exact (vtop-node verify).
    holder = vtop_lease(ctx, raw)
    if holder is None:
        row["verify"] = "FAILED: no lease holder to read back from"
    else:
        node, current_epoch = int(holder[0][-1]), holder[1]
        with open(os.path.join(raw, "verify-client.yaml"), "w") as handle:
            handle.write(f"cluster_id: 11111111-2222-3333-4444-555555555555\n"
                         f"principal_id: aaaaaaaa-0000-0000-0000-0000000000ce\n"
                         f"producer_id: aaaaaaaa-0000-0000-0000-0000000000ce\nproducer_epoch: 1\n"
                         f"fencing_epoch: {current_epoch}\n"
                         f'range: {{ topic: "bench", topic_epoch: 1, range_id: {RANGE_UUID}, range_generation: 0 }}\n'
                         f'server_name: "localhost"\n'
                         f"tls: {{ ca: /etc/vtop/tls/ca.pem, cert: /etc/vtop/tls/data-1.pem, "
                         f"key: /etc/vtop/tls/data-1-key.pem }}\n")
        done = vtop_tool(ctx, raw, "verify", "--client-config", "/out/verify-client.yaml", "--addr",
                         f"vtop-{node}:9400", "--expect-at-least", str(acked), "--max-offset-gaps",
                         "0", "--value-bytes", str(ctx["vtop_value_bytes"]), "--batch", "512",
                         entrypoint="vtop-node", timeout=600)
        result = fstats.parse_vtop_verify(done.stdout, done.stderr, done.returncode)
        with open(os.path.join(logs, "verify.out"), "w") as handle:
            handle.write(done.stdout[-2000:] + "\n--- stderr ---\n" + done.stderr[-2000:])
        if result.passed:
            row.update(verify="passed", acked_lost=0, duplicates=0,
                       unacked_present=max(0, (result.records or 0) - acked))
        else:
            row["verify"] = f"FAILED: {result.error or 'no verify_done line'} (exit {done.returncode})"
    for name in ("driver.log", "lease.log"):
        shutil.copy2(os.path.join(raw, name), os.path.join(logs, name))
    if row.get("verify", "").startswith("FAILED"):
        log(f"VTOP READ-BACK FAILED ({row['verify']}): keeping the run's data in full")
        save_vtop_evidence(ctx, raw, os.path.join(logs, "evidence"))
    origin_ms = anchor_us / 1000.0
    how = "driver finished" if finished else "driver stopped at the drain deadline"
    return acks, {"origin_ms": origin_ms, "fault_vm_ms": fault_vm_ms, "finished": finished, "how": how}


def run_rep(ctx: dict, system: str, fault: str, rep: int) -> None:
    tag = f"{system}-{fault}-{rep}"
    raw = os.path.join(ctx["work"], tag)
    logs = os.path.join(ctx["run_dir"], "logs", tag)
    os.makedirs(raw)
    os.makedirs(logs)
    row = base_row(ctx, system, fault, rep)
    started = time.monotonic()
    timeline: list[dict] = []
    log(f"{tag}: start")
    try:
        runner = run_kafka if system == "kafka" else run_vtop
        acks, times = runner(ctx, fault, raw, logs, row)
        check_complete(row, times["finished"], times["how"])
        fault_vm_ms = times["fault_vm_ms"]
        ack_times = [event.t_ms for event in acks]
        gap = fstats.unavailability(ack_times, fault_vm_ms, POST_S * 1000)
        if gap is None:
            raise Refused("nothing was acknowledged after the fault within the post-fault window: no recovery")
        latencies = fstats.record_latencies(acks, times["origin_ms"], ctx["rate"])
        p99_before = fstats.window_percentile(latencies, fault_vm_ms - PRE_S * 1000, fault_vm_ms, 99)
        p99_after = fstats.window_percentile(latencies, fault_vm_ms, fault_vm_ms + 30_000, 99)
        p50_after = fstats.window_percentile(latencies, fault_vm_ms, fault_vm_ms + 30_000, 50)
        row.update(status="ok", unavailability_ms=round(gap.gap_ms, 1), gap_start_ms=round(gap.start_rel_ms, 1),
                   baseline_max_gap_ms=round(fstats.max_gap(ack_times, fault_vm_ms - PRE_S * 1000, fault_vm_ms), 1),
                   p99_before_ms=None if p99_before is None else round(p99_before, 1),
                   p99_after_ms=None if p99_after is None else round(p99_after, 1),
                   p50_after_ms=None if p50_after is None else round(p50_after, 1),
                   max_latency_ms=round(max(latency for _, latency in latencies), 1))
        for t_ms, records in fstats.ack_rate_bins(acks, fault_vm_ms, TIMELINE_FROM_MS, TIMELINE_TO_MS, BIN_MS):
            timeline.append({"run_id": ctx["run_id"], "env_fingerprint": ctx["fingerprint"], "system": system,
                             "fault": fault, "repetition": rep, "t_ms": t_ms, "records": records})
        log(f"{tag}: unavailable {row['unavailability_ms']} ms, leader in {row.get('time_to_leader_ms')} ms, "
            f"lost {row.get('acked_lost')}, dups {row.get('duplicates')}, verify {row.get('verify')}")
    except Incomplete as exc:
        row.update(status="failed", note=(str(exc) + ("; " + row["note"] if row.get("note") else ""))[:900])
        log(f"{tag}: failed: {exc}")
    except Refused as exc:
        row.update(status="refused" if "acknowledged after the fault" not in str(exc) else "failed",
                   note=(str(exc) + ("; " + row["note"] if row.get("note") else ""))[:900])
        log(f"{tag}: {row['status']}: {exc}")
    except (RuntimeError, subprocess.TimeoutExpired, fstats.ParseError, ValueError, OSError) as exc:
        row.update(status="failed", note=f"{type(exc).__name__}: {exc}"[:900])
        log(f"{tag}: failed: {exc}")
    finally:
        try:
            save_node_logs(system, logs, full=row.get("verify", "").startswith("FAILED"))
        except Exception as exc:  # the row still records what was measured
            log(f"{tag}: saving node logs failed: {exc}")
        try:
            cluster_down(system, ctx)
        except Exception as exc:  # the next repetition's `up` starts with a down anyway
            log(f"{tag}: down failed: {exc}")
    if row.get("verify", "").startswith("FAILED"):
        ctx["losses"].append(tag)
    row["wall_seconds"] = round(time.monotonic() - started, 1)
    fschema.append_rows(ctx["csv"], [row])
    if timeline:
        fschema.append_rows(ctx["timeline"], timeline, fschema.TIMELINE_COLUMNS)
    if not ctx["keep_raw"]:
        shutil.rmtree(raw, ignore_errors=True)


# ------------------------------------------------------------------------------- main


def key_mode() -> int:
    """The mode a private key needs for the containers (user vtop, uid 10001) to read it.

    Docker Desktop's file sharing presents a bind-mounted file as owned by the
    user reading it, so an owner-only key is readable in the container (checked
    with a uid-10001 container on Docker Desktop 4.99). A native Linux daemon
    keeps the host owner, so there the key must be world-readable; the per-run
    work directory (mkdtemp, 0700) is what keeps other host users out.
    """
    return 0o644 if platform.system() == "Linux" else 0o600


def mint_certs(directory: str) -> None:
    sh(["bash", os.path.join(REPO, "scripts", "live-chaos", "gen-certs.sh"), directory, "1", "2", "3", "--",
        NODE_UUID[1], NODE_UUID[2], NODE_UUID[3]])
    # The CA key signs nothing after minting; removed, no container can see it.
    os.remove(os.path.join(directory, "ca-key.pem"))
    for name in os.listdir(directory):
        os.chmod(os.path.join(directory, name), key_mode() if name.endswith("-key.pem") else 0o644)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--rate", type=int, default=5000, help="offered records/s, both systems")
    parser.add_argument("--faults", default=",".join(fstats.FAULTS))
    parser.add_argument("--systems", default="kafka,vtop")
    parser.add_argument("--vtop-batch", type=int, default=100)
    parser.add_argument("--vtop-value-bytes", type=int, default=8)
    parser.add_argument("--work-dir", default=None, help="raw per-repetition data (default: a temp dir)")
    parser.add_argument("--keep-raw", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    args = parser.parse_args(argv)

    faults = [fault for fault in args.faults.split(",") if fault]
    systems = [system for system in args.systems.split(",") if system]
    unknown = [f for f in faults if f not in fstats.FAULTS] + [s for s in systems if s not in SPEC]
    if unknown:
        raise SystemExit(f"unknown faults or systems: {unknown}")
    if args.reps < 3:
        log(f"warning: {args.reps} repetitions is below the 3 this benchmark promises")
    limits = check_equal_limits()
    for system in SPEC:
        running = sh(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={SPEC[system]['project']}"])
        if running.stdout.strip():
            raise SystemExit(f"{SPEC[system]['project']} is already running; stop it first (one system at a time)")
    sh(["docker", "pull", "-q", KAFKA_IMAGE], timeout=1800)
    sh(["docker", "pull", "-q", PYTHON_IMAGE], timeout=1800)
    vtop_image = ensure_vtop_image(args.rebuild)

    run_id = args.run_id or ("failover-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    run_dir = os.path.join(HERE, "results", run_id)
    if os.path.exists(run_dir):
        raise SystemExit(f"{run_dir} exists; runs never overwrite each other")
    os.makedirs(os.path.join(run_dir, "logs"))
    work = args.work_dir or tempfile.mkdtemp(prefix="kc-failover-")
    os.makedirs(work, exist_ok=True)
    certs = os.path.join(work, "certs")
    mint_certs(certs)
    total = args.rate * (WARMUP_S + PRE_S + POST_S)
    matrix = {"systems": systems, "faults": faults, "repetitions": args.reps, "offered_rate": args.rate,
              "records_per_repetition": total, "warmup_s": WARMUP_S, "pre_s": PRE_S, "post_s": POST_S,
              "heal_after_s": HEAL_AFTER_S, "stop_grace_s": STOP_GRACE_S, "timeline_bin_ms": BIN_MS,
              "timeline_from_ms": TIMELINE_FROM_MS, "timeline_to_ms": TIMELINE_TO_MS,
              "drain_deadline_s": DRAIN_DEADLINE_S, "vtop_batch": args.vtop_batch,
              "vtop_value_bytes": args.vtop_value_bytes, "order": "per fault, per repetition: kafka then vtop"}
    env = capture_environment(run_id, matrix, vtop_image, limits)
    ctx = {"run_id": run_id, "run_dir": run_dir, "work": work, "certs": certs, "env": env,
           "fingerprint": schema.fingerprint(env), "vtop_image": vtop_image, "rate": args.rate, "total": total,
           "vtop_batch": args.vtop_batch, "vtop_value_bytes": args.vtop_value_bytes, "keep_raw": args.keep_raw,
           "csv": os.path.join(run_dir, "failover.csv"), "timeline": os.path.join(run_dir, "timeline.csv"),
           "losses": []}
    fschema.write_header(ctx["csv"])
    fschema.write_header(ctx["timeline"], fschema.TIMELINE_COLUMNS)
    phase1.save_environment(run_dir, env)
    log(f"run {run_id} -> {run_dir} (fingerprint {ctx['fingerprint']}, raw data in {work})")
    try:
        for fault in faults:
            for rep in range(1, args.reps + 1):
                for system in systems:
                    run_rep(ctx, system, fault, rep)
                    phase1.save_environment(run_dir, env)
    except BaseException as exc:
        env["run_state"] = "interrupted"
        env["events"].append({"at": utc_now(), "event": f"run interrupted: {exc!r}"[:900]})
        phase1.save_environment(run_dir, env)
        for system in SPEC:
            try:
                cluster_down(system, ctx)
            except Exception:  # best effort on the way out
                pass
        raise
    env["run_state"] = "complete"
    env["finished_at"] = utc_now()
    env["fingerprint_check"] = schema.fingerprint(env)
    phase1.save_environment(run_dir, env)
    if not args.work_dir and not args.keep_raw:
        shutil.rmtree(work, ignore_errors=True)
    if ctx["losses"]:
        log(f"FAIL: the read-back found missing or duplicated acknowledged records in {ctx['losses']}")
        return 1
    log(f"done: {ctx['csv']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
