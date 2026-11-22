#!/usr/bin/env python3
"""Record what the cluster plane's control side looked like (#728).

The data-plane history (`produce --history`, `verify --dump`) says what the
clients wrote and read. This recorder says who led: it polls, until it is
stopped, and appends one JSON line per observation with the wall-clock
instants the observation was asked for and answered (`invoke_ms`,
`complete_ms`), so `check_control.py` can order observations in real time:

  meta_status  `vtopctl --json meta status` against one metadata node: the
               answering node, its Raft term and server state, and its vote
               (term, voted-for, committed)
  lease_read   `vtopctl --json meta range-lease`, a linearizable read of the
               range's lease: the range's fencing epoch, the lease's holder,
               epoch and deadline, and the applied index it was served at.
               Rotated over the metadata nodes, so a read through every node
               is in the history
  node_lease   one data node's /metrics: whether it is the authorized
               leaseholder (`vtop_broker_lease_active`), at which epoch
               (`vtop_broker_meta_fencing_epoch`, `vtop_broker_held_fencing_epoch`)

A failed observation (a killed node, a metadata group mid-election) is
recorded with `"ok": false` and is evidence of nothing. Each target is polled
by its own thread, so a slow or dead target does not delay the others.
SIGTERM or SIGINT stops it; every line is written whole, by one write, so a
SIGKILL can cut the file only between lines.

Usage:
  record_control.py --out FILE --vtopctl PATH --topic UUID --range UUID
      [--meta ID=ADMIN_CONFIG ...] [--node NAME=METRICS_ADDR=NODE_UUID ...]
      [--status-interval-ms 1000] [--lease-interval-ms 300] [--node-interval-ms 250]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.request

STOP = threading.Event()
TIMEOUT_S = 5.0
METRIC = re.compile(r"^(vtop_broker_(?:lease_active|meta_fencing_epoch|held_fencing_epoch|candidate_leading))(?:\{[^}]*\})?\s+(\S+)\s*$")


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class Sink:
    """Append-only JSON lines, one os.write per line."""

    def __init__(self, path: str):
        self.fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        self.lock = threading.Lock()

    def write(self, event: dict) -> None:
        line = (json.dumps(event, separators=(",", ":")) + "\n").encode()
        with self.lock:
            os.write(self.fd, line)


def run_json(argv: list[str]) -> dict | None:
    try:
        done = subprocess.run(argv, capture_output=True, timeout=TIMEOUT_S, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if done.returncode != 0:
        return None
    try:
        value = json.loads(done.stdout)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def meta_status(vtopctl: str, meta_id: str, config: str) -> dict:
    invoke = now_ms()
    d = run_json([vtopctl, "--json", "meta", "status", "--config", config])
    event = {"op": "meta_status", "via": meta_id, "invoke_ms": invoke, "complete_ms": now_ms(), "ok": d is not None}
    if d is not None:
        vote = d.get("vote") or {}
        event.update(
            node_id=d.get("node_id"),
            term=d.get("current_term"),
            state=d.get("server_state"),
            leader=d.get("current_leader"),
            vote_term=vote.get("term"),
            voted_for=vote.get("voted_for"),
            vote_committed=bool(vote.get("vote_committed")),
        )
    return event


def lease_read(vtopctl: str, meta_id: str, config: str, topic: str, range_: str) -> dict:
    invoke = now_ms()
    d = run_json([vtopctl, "--json", "meta", "range-lease", "--config", config,
                  "--topic-uuid", topic, "--range-uuid", range_])
    event = {"op": "lease_read", "via": meta_id, "invoke_ms": invoke, "complete_ms": now_ms(), "ok": d is not None}
    if d is not None:
        lease = d.get("lease")
        event.update(
            found=bool(d.get("found")),
            range_epoch=d.get("fencing_epoch"),
            applied_index=d.get("read_at_applied_index"),
            holder=lease.get("holder_node_uuid") if lease else None,
            lease_epoch=lease.get("fencing_epoch") if lease else None,
            expires_at_ms=lease.get("expires_at_ms") if lease else None,
        )
    return event


def node_lease(name: str, addr: str, node_uuid: str) -> dict:
    invoke = now_ms()
    values: dict[str, int] = {}
    ok = True
    try:
        with urllib.request.urlopen(f"http://{addr}/metrics", timeout=TIMEOUT_S) as resp:
            body = resp.read().decode("utf-8", "replace")
        for line in body.splitlines():
            m = METRIC.match(line)
            if m:
                values[m.group(1)] = int(float(m.group(2)))
    except (OSError, ValueError):
        ok = False
    event = {"op": "node_lease", "node": name, "node_uuid": node_uuid,
             "invoke_ms": invoke, "complete_ms": now_ms(), "ok": ok}
    if ok:
        # A node that exports no lease gauge (a static follower, a node that
        # never had a lease) claims nothing.
        event.update(
            active=values.get("vtop_broker_lease_active", 0) == 1,
            epoch=values.get("vtop_broker_meta_fencing_epoch"),
            held=values.get("vtop_broker_held_fencing_epoch"),
            leading=values.get("vtop_broker_candidate_leading"),
        )
    return event


def poll(interval_ms: int, observe, sink: Sink) -> None:
    while not STOP.is_set():
        started = time.monotonic()
        sink.write(observe())
        rest = interval_ms / 1000 - (time.monotonic() - started)
        if rest > 0:
            STOP.wait(rest)


def pairs(values: list[str], parts: int, flag: str) -> list[list[str]]:
    out = []
    for value in values:
        split = value.split("=", parts - 1)
        if len(split) != parts or not all(split):
            raise SystemExit(f"record_control: {flag} {value!r}: expected {parts} '='-separated parts")
        out.append(split)
    return out


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", required=True)
    ap.add_argument("--vtopctl", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--range", required=True, dest="range_")
    ap.add_argument("--meta", action="append", default=[], help="ID=ADMIN_CONFIG")
    ap.add_argument("--node", action="append", default=[], help="NAME=METRICS_ADDR=NODE_UUID")
    ap.add_argument("--status-interval-ms", type=int, default=1000)
    ap.add_argument("--lease-interval-ms", type=int, default=300)
    ap.add_argument("--node-interval-ms", type=int, default=250)
    args = ap.parse_args(argv)
    metas = pairs(args.meta, 2, "--meta")
    nodes = pairs(args.node, 3, "--node")
    if not metas and not nodes:
        raise SystemExit("record_control: nothing to observe (no --meta, no --node)")

    sink = Sink(args.out)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: STOP.set())

    threads = []
    for meta_id, config in metas:
        threads.append(threading.Thread(
            target=poll, daemon=True,
            args=(args.status_interval_ms, lambda m=meta_id, c=config: meta_status(args.vtopctl, m, c), sink)))
    if metas:
        turn = {"i": 0}

        def rotating_read() -> dict:
            meta_id, config = metas[turn["i"] % len(metas)]
            turn["i"] += 1
            return lease_read(args.vtopctl, meta_id, config, args.topic, args.range_)

        threads.append(threading.Thread(target=poll, daemon=True, args=(args.lease_interval_ms, rotating_read, sink)))
    for name, addr, node_uuid in nodes:
        threads.append(threading.Thread(
            target=poll, daemon=True,
            args=(args.node_interval_ms, lambda n=name, a=addr, u=node_uuid: node_lease(n, a, u), sink)))
    for t in threads:
        t.start()
    while not STOP.is_set():
        STOP.wait(0.5)
    for t in threads:
        t.join(timeout=TIMEOUT_S + 1)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
