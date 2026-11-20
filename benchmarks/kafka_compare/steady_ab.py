#!/usr/bin/env python3
"""Steady-state A/B of two VTOP node images, no faults (#613 follow-up).

    python3 benchmarks/kafka_compare/steady_ab.py --node-images vtop-bench:A,vtop-bench:B \
        --client-image vtop-bench:B --rounds 4 --out <dir>

The failover run's VTOP steady-state gaps grew between two runs taken on
different engines AND different host load. This separates the two: the same
three-node co-located cluster failover.py builds, the same native driver at
the same rate, but no fault, and the two node images INTERLEAVED (A B A B ...)
so host contention falls on both alike. Each run records its own host noise.

The client (`vtop-node produce`, `vtopctl`) comes from ONE image for every run,
so only the nodes differ: the produce pacing and the ack-log stamp changed in
#613, and a client difference would otherwise show up as a node difference.

Per run, over the steady window (after `--warmup` seconds): the longest gap
between acknowledgements, the p50/p99/max latency measured from the fixed-rate
schedule (how far behind its schedule the producer ran), and the batch round
trip p50/p99 from the ack log. One row per run in `ab.csv`, its noise in
`host_noise-<run>.csv`, and the conditions in `environment.json`.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from kafka_compare import failover, host_noise, stats  # noqa: E402
from kafka_compare import failover_stats as fstats  # noqa: E402
from kafka_compare import run as phase1  # noqa: E402

COLUMNS = ("run", "round", "engine", "node_image", "node_image_id", "client_image", "started_at", "seconds",
           "records_acked", "acks", "max_gap_ms", "lat_p50_ms", "lat_p99_ms", "lat_max_ms", "rtt_p50_ms",
           "rtt_p99_ms", "load1_mean", "load1_max", "cpu_idle_pct_min", "docker_rust_builds_max", "note")


def steady_metrics(ack_text: str, origin_ms: float, rate: int, warmup_s: float) -> dict[str, float]:
    """The steady-window numbers of one run, from its ack log (`<unix_us> <total> <rtt_us>` lines)."""
    events = fstats.parse_ack_log(ack_text)
    start = origin_ms + warmup_s * 1000
    times = [e.t_ms for e in events if e.t_ms >= start]
    if len(times) < 2:
        raise ValueError("fewer than two acknowledgements in the steady window")
    latencies = [lat for intended, lat in fstats.record_latencies(events, origin_ms, rate) if intended >= start]
    rtts = []
    for line in ack_text.splitlines():
        parts = line.split()
        if len(parts) == 3 and int(parts[0]) / 1000 >= start:
            rtts.append(int(parts[2]) / 1000)
    return {
        "acks": len(times),
        "max_gap_ms": round(max(b - a for a, b in zip(times, times[1:])), 1),
        "lat_p50_ms": round(stats.percentile(latencies, 50), 1),
        "lat_p99_ms": round(stats.percentile(latencies, 99), 1),
        "lat_max_ms": round(max(latencies), 1),
        "rtt_p50_ms": round(stats.percentile(rtts, 50), 1),
        "rtt_p99_ms": round(stats.percentile(rtts, 99), 1),
    }


def one_run(ctx: dict, node_image: str, raw: str) -> tuple[str, float]:
    """Bring up a fresh cluster of `node_image`, drive it at the rate for ctx['seconds'], tear it
    down; returns (ack log text, schedule origin in unix ms)."""
    original = failover.compose

    def compose_with_nodes(system, *args, ctx, timeout=600):
        return original(system, *args, ctx={**ctx, "vtop_image": node_image}, timeout=timeout)

    failover.compose = compose_with_nodes
    try:
        failover.vtop_up(ctx, raw)
        anchor_us = int((time.time() + 3) * 1e6)
        os.chmod(raw, 0o777)
        driver = os.path.join(HERE, "vtop", "failover-driver.sh")
        phase1.sh(["docker", "run", "-d", "--name", failover.PRODUCER, "--network", failover.SPEC["vtop"]["network"],
                   "--cpus", failover.CLIENT_CPUS, "--memory", failover.CLIENT_MEMORY,
                   "-v", f"{ctx['certs']}:/etc/vtop/tls:ro", "-v", f"{raw}:/out", "-v", f"{driver}:/driver.sh:ro",
                   "-e", f"TOTAL={ctx['total']}", "-e", f"RATE={ctx['rate']}", "-e", f"BATCH={ctx['vtop_batch']}",
                   "-e", f"VALUE_BYTES={ctx['vtop_value_bytes']}", "-e", f"ANCHOR_US={anchor_us}",
                   "--entrypoint", "bash", ctx["vtop_image"], "/driver.sh"])
        phase1.sh(["docker", "wait", failover.PRODUCER], check=False, timeout=ctx["seconds"] + 300)
        if not os.path.exists(os.path.join(raw, "done")):
            raise RuntimeError("the driver did not finish")
        with open(os.path.join(raw, "acks.log")) as handle:
            return handle.read(), anchor_us / 1000.0
    finally:
        failover.compose = original
        failover.cluster_down("vtop", ctx)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--node-images", required=True, help="A,B")
    parser.add_argument("--client-image", required=True)
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument("--seconds", type=int, default=120)
    parser.add_argument("--warmup", type=float, default=10.0)
    parser.add_argument("--rate", type=int, default=5000)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    engines = dict(zip("AB", args.node_images.split(",")))
    if len(engines) != 2 or os.path.exists(args.out):
        raise SystemExit("--node-images must be A,B, and --out must not exist")
    os.makedirs(args.out)
    work = tempfile.mkdtemp(prefix="kc-steady-ab-")
    certs = os.path.join(work, "certs")
    failover.mint_certs(certs)
    ids = {k: json.loads(phase1.sh(["docker", "image", "inspect", v]).stdout)[0]["Id"] for k, v in engines.items()}
    env = {"what": "steady-state A/B of two VTOP node images, no faults, interleaved; see steady_ab.py",
           "engines": {k: {"image": engines[k], "image_id": ids[k]} for k in engines},
           "client_image": args.client_image, "harness_commit": phase1.git("rev-parse", "HEAD"),
           "rate": args.rate, "seconds": args.seconds, "warmup_s": args.warmup, "batch": 100, "value_bytes": 8,
           "profile": "defaults", "rounds": args.rounds, "order": "per round: A then B",
           "host_logical_cpus": phase1._sysctl("hw.ncpu") or str(os.cpu_count()), "started_at": phase1.utc_now()}
    ctx = {"certs": certs, "vtop_image": args.client_image, "rate": args.rate, "total": args.rate * args.seconds,
           "seconds": args.seconds, "vtop_batch": 100, "vtop_value_bytes": 8}
    path = os.path.join(args.out, "ab.csv")
    with open(path, "w", newline="") as handle:
        csv.writer(handle).writerow(COLUMNS)
    try:
        for rnd in range(1, args.rounds + 1):
            for engine in "AB":
                name = f"r{rnd}-{engine}"
                raw = os.path.join(work, name)
                os.makedirs(raw)
                noise_path = os.path.join(args.out, f"host_noise-{name}.csv")
                sampler = host_noise.Sampler(noise_path, 10.0)
                sampler.start()
                row = {"run": name, "round": rnd, "engine": engine, "node_image": engines[engine],
                       "node_image_id": ids[engine], "client_image": args.client_image,
                       "started_at": phase1.utc_now(), "seconds": args.seconds}
                phase1.log(f"{name}: {engines[engine]}")
                try:
                    text, origin = one_run(ctx, engines[engine], raw)
                    last = fstats.parse_ack_log(text)[-1]
                    row["records_acked"] = last.first_seq + last.records
                    row.update(steady_metrics(text, origin, args.rate, args.warmup))
                except (RuntimeError, ValueError, subprocess.SubprocessError, OSError) as exc:
                    row["note"] = f"{type(exc).__name__}: {exc}"[:300]
                finally:
                    sampler.stop()
                summary = host_noise.summarize(host_noise.read(noise_path))
                row.update({k: summary.get(k) for k in ("load1_mean", "load1_max", "cpu_idle_pct_min",
                                                         "docker_rust_builds_max")})
                with open(path, "a", newline="") as handle:
                    csv.writer(handle).writerow(["" if row.get(c) is None else row[c] for c in COLUMNS])
                phase1.log(f"{name}: max gap {row.get('max_gap_ms')} ms, p99 {row.get('lat_p99_ms')} ms, "
                           f"rtt p99 {row.get('rtt_p99_ms')} ms, load max {row.get('load1_max')} {row.get('note', '')}")
                shutil.rmtree(raw, ignore_errors=True)
    finally:
        failover.cluster_down("vtop", ctx)
        env["finished_at"] = phase1.utc_now()
        with open(os.path.join(args.out, "environment.json"), "w") as handle:
            json.dump(env, handle, indent=2, sort_keys=True)
            handle.write("\n")
        shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
