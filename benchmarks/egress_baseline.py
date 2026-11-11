#!/usr/bin/env python3
"""The tuned-TCP baseline sweep (#483): run it, collect it, report it.

Three subcommands, each a separate step so the raw data sits between them:

    run      one grid point through run_benchmark.py, on the netem lab
    collect  every run directory -> the committed raw CSVs
    report   the raw CSVs -> the per-point summary, the gate verdict, and the
             charts (SVG, byte-for-byte deterministic for a given input)

The gate itself is fixed in docs/EGRESS_TRANSPORT_RESEARCH.md ("The gate,
fixed before the sweep was read"); `report` evaluates exactly that text and
nothing else, so a change to the thresholds here without the doc is a change
the doc review sees.

Why the workload looks the way it does: every object is a whole file of
exactly OBJECT_BYTES, one record per batch, `compression: none`. So each
batch is one object, the object's size is the wire payload, and a
`parts_in_flight` value is visible — with 5 MiB parts a 20 MiB object has
four parts, and 1, 2 and 4 in flight are three different things.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))

MIB = 1024 * 1024
OBJECT_BYTES = 20 * MIB
PART_BYTES = 5 * MIB
GRID_CONCURRENCY = (1, 2, 4, 8, 16)
GRID_PARTS = (1, 2, 4)

# The gate, as committed in docs/EGRESS_TRANSPORT_RESEARCH.md before any run
# was read. Changing either number here without changing the doc is a defect.
GATE_UNUSED_FRACTION = 0.20
GATE_P95_RATIO = 2.0
TIE_POINTS = 0.02  # "ties within two percentage points"
GATED_PROFILES = ("wan100", "wan100-loss1")
LOSSY_PROFILE = "wan100-loss1"
CLEAN_PROFILE = "wan100"

# The link profiles. Keys are scenario keys; an empty map is the clean LAN —
# the middlebox stays in the path (the engine still dials netem:9200), with no
# qdisc on it.
PROFILES: dict[str, dict] = {
    "lan": {},
    "wan100": {
        "shaping_driver": "netem", "shaping_latency_ms": 100, "shaping_jitter_ms": 0,
        "shaping_bottleneck_kbps": 10000, "shaping_buffer_bdp": 1.0,
    },
    "wan100-loss1": {
        "shaping_driver": "netem", "shaping_latency_ms": 100, "shaping_jitter_ms": 0,
        "shaping_bottleneck_kbps": 10000, "shaping_buffer_bdp": 1.0,
        "shaping_loss_pct": 1.0, "shaping_loss_model": "gemodel",
    },
    "wan100-policed": {
        "shaping_driver": "netem", "shaping_latency_ms": 100, "shaping_jitter_ms": 0,
        "shaping_policer_kbps": 10000,
    },
}
# What each profile's goodput is read against when there is no calibrated C.
POLICER_KBPS = 10000


def config_label(c: int | None, pif: int | None) -> str:
    """`default` for the shipped settings, else `c{c}-p{pif}`."""
    if c is None:
        return "default"
    return f"c{c}-p{pif}"


LAN_OBJECTS = 64


def objects_for(c: int | None, profile: str = "wan100") -> int:
    """The backlog a run drains: two waves at the configured width, never fewer
    than four objects. The default's width is the shipped eight. The clean LAN
    drains a fixed 64 (1.25 GiB) instead, because two waves there finish in
    well under a second and nothing about such a run can be timed."""
    if profile == "lan":
        return LAN_OBJECTS
    width = 8 if c is None else c
    return max(4, 2 * width)


def scenario_yaml(profile: str, c: int | None, pif: int | None, name: str,
                  competitor: str = "", duration: int = 0, volume_files: int = 0) -> str:
    if profile not in PROFILES:
        raise ValueError(f"unknown profile {profile!r}; one of {sorted(PROFILES)}")
    lines = [
        f"name: {name}",
        f"description: tcp baseline sweep (#483), profile {profile}, "
        f"config {config_label(c, pif)}",
        # The objects are supplied by the sweep (a hard-linked seed dir), so
        # the runner generates none of its own.
        "volume: 0",
        "file_size: small",
        "format: jsonl",
        "whole_file: true",
        "compression: none",
        "checksum: sha256",
        "backend: s3_native",
        'bucket: "vtop-bench-soak"',
        "runner_mode: container",
        "endpoint_url: http://netem:9200",
        "batch_max_records: 1",
        f"batch_max_bytes: {OBJECT_BYTES + MIB}",
        "batch_max_age_seconds: 3600",
        f"duration_seconds: {duration}",
        "fault: none",
        "sys_sample_interval: 1.0",
    ]
    if c is not None:
        lines.append(f"max_concurrent_batches: {c}")
        lines += [
            "transports:",
            "  tcp_tls:",
            f"    max_concurrency: {c}",
            f"    parts_in_flight: {pif}",
            f"    part_size_bytes: {PART_BYTES}",
        ]
    for key, value in PROFILES[profile].items():
        lines.append(f"{key}: {value}")
    if competitor:
        lines.append(f'shaping_competitor: "{competitor}"')
    del volume_files
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------- run

def ensure_pool(pool: str, count: int) -> list[str]:
    """`count` distinct files of exactly OBJECT_BYTES of JSONL, made once."""
    sys.path.insert(0, HERE)
    from lib import seed  # noqa: PLC0415 - the harness's own line generator

    os.makedirs(pool, exist_ok=True)
    base_path = os.path.join(pool, "_base.jsonl")
    if not os.path.exists(base_path) or os.path.getsize(base_path) < OBJECT_BYTES:
        seed.random.seed(483)
        tmp = base_path + ".tmp"
        seed._write_text_file(tmp, OBJECT_BYTES, "jsonl")  # noqa: SLF001
        os.replace(tmp, base_path)
    with open(base_path, "rb") as fh:
        base = fh.read(OBJECT_BYTES)
    paths = []
    for i in range(count):
        path = os.path.join(pool, f"obj-{i:03d}.jsonl")
        if not os.path.exists(path) or os.path.getsize(path) != OBJECT_BYTES:
            head = f'{{"pool_object":{i}}}\n'.encode()
            body = head + base[: OBJECT_BYTES - len(head)]
            assert len(body) == OBJECT_BYTES
            with open(path + ".tmp", "wb") as fh:
                fh.write(body)
            os.replace(path + ".tmp", path)
        paths.append(path)
    return paths


def _minio_rm_prefix(prefix: str) -> None:
    """The bench bucket outlives runs; a sweep of hundreds of 20 MiB objects
    would fill the VM's disk, so each run's prefix is removed after it."""
    script = ('mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" '
              '"$MINIO_ROOT_PASSWORD" >/dev/null && '
              f'mc rm -r --force "local/vtop-bench-soak/{prefix}" >/dev/null')
    subprocess.run(["docker", "exec", "-e", "HOME=/tmp", "vtop-bench-minio", "sh", "-c", script],
                   check=False, capture_output=True, text=True)


# The containers whose CPU a run is charged with: the engine (the measured
# program) and the store (the other end, named when it is the bottleneck).
CPU_CONTAINERS = {"engine": "vtop-bench-engine", "store": "vtop-bench-minio"}


def cgroup_cpu_seconds(container: str) -> float | None:
    """The container's cumulative CPU time from its cgroup v2 `cpu.stat`.

    Read directly rather than sampled through `docker stats`: a sampled
    percentage misses most of a two-second LAN run, while the counter is exact
    at both ends. None where the cgroup is not where systemd's driver puts it.
    """
    proc = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", container],
                          check=False, capture_output=True, text=True)
    cid = proc.stdout.strip()
    if proc.returncode != 0 or not cid:
        return None
    path = f"/sys/fs/cgroup/system.slice/docker-{cid}.scope/cpu.stat"
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                key, _, value = line.partition(" ")
                if key == "usage_usec":
                    return int(value) / 1e6
    except OSError:
        return None
    return None


def _vm_loadavg() -> str:
    try:
        with open("/proc/loadavg", encoding="utf-8") as fh:
            return " ".join(fh.read().split()[:3])
    except OSError:
        return ""


CONTENDED_BASE = os.path.join(HERE, "scenarios", "18-competing-flow.yaml")


def contended_yaml(base_text: str, c: int | None, pif: int | None, name: str) -> str:
    """Scenario 18 with only its name and its width replaced.

    A contended row cannot use the sweep's drain: the competitor's share of the
    engine is charged only to cycles its window WHOLLY contains, and one
    drain of 20 MiB objects is one cycle longer than any window. Scenario 18's
    workload exists for exactly that constraint — short per-cycle drains of
    small batches — so the contended rows run it unchanged apart from the
    configuration being compared.
    """
    keep = [line for line in base_text.splitlines()
            if not line.startswith(("name:", "max_concurrent_batches:"))]
    lines = [f"name: {name}"] + keep
    if c is None:
        lines.append("max_concurrent_batches: 8")
    else:
        lines += [f"max_concurrent_batches: {c}", "transports:", "  tcp_tls:",
                  f"    max_concurrency: {c}", f"    parts_in_flight: {pif}",
                  f"    part_size_bytes: {PART_BYTES}"]
    return "\n".join(lines) + "\n"


def cmd_run(args) -> int:
    c = None if args.config == "default" else args.c
    pif = None if args.config == "default" else args.pif
    label = config_label(c, pif)
    name = f"tcpb-{args.profile}-{label}-r{args.rep}"
    scen = os.path.join(args.run_root, f"{name}-{os.getpid()}.yaml")
    seed_args: list[str] = []
    work = ""
    if args.phase == "contended":
        if args.profile != "wan100":
            raise SystemExit("contended rows run on scenario 18's link, the wan100 profile")
        n = 0
        with open(CONTENDED_BASE, encoding="utf-8") as fh:
            body = contended_yaml(fh.read(), c, pif, name)
    else:
        n = args.objects or objects_for(c, args.profile)
        pool = ensure_pool(args.pool, n)
        work = os.path.join(args.run_root, f"seed-{name}-{os.getpid()}")
        os.makedirs(work)
        for path in pool[:n]:
            os.link(path, os.path.join(work, os.path.basename(path)))
        seed_args = ["--seed-dir", work]
        body = scenario_yaml(args.profile, c, pif, name, competitor=args.competitor,
                             duration=args.duration)
    with open(scen, "w", encoding="utf-8") as fh:
        fh.write(body)
    os.makedirs(args.results, exist_ok=True)
    before = set(os.listdir(args.results))
    started = datetime.now(timezone.utc).isoformat()
    load_before = _vm_loadavg()
    cpu_before = {k: cgroup_cpu_seconds(v) for k, v in CPU_CONTAINERS.items()}
    t0 = time.monotonic()
    proc = subprocess.run([sys.executable, os.path.join(HERE, "run_benchmark.py"), scen,
                           "--results-dir", args.results, *seed_args],
                          check=False)
    wall = time.monotonic() - t0
    cpu_after = {k: cgroup_cpu_seconds(v) for k, v in CPU_CONTAINERS.items()}
    cpu_used = {k: ("" if cpu_before[k] is None or cpu_after[k] is None
                    else round(cpu_after[k] - cpu_before[k], 3)) for k in CPU_CONTAINERS}
    new = sorted(set(os.listdir(args.results)) - before)
    run_id = new[0] if len(new) == 1 else ""
    if run_id:
        _minio_rm_prefix(run_id)
    if work:
        shutil.rmtree(work, ignore_errors=True)
    os.remove(scen)
    index = os.path.join(args.results, "sweep_index.csv")
    fresh = not os.path.exists(index)
    with open(index, "a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if fresh:
            w.writerow(["run_id", "phase", "profile", "config", "c", "pif", "rep", "objects",
                        "competitor", "exit_code", "started_utc", "wall_seconds",
                        "vm_loadavg_before", "vm_loadavg_after", "vm",
                        "engine_cpu_seconds", "store_cpu_seconds"])
        w.writerow([run_id, args.phase, args.profile, label, "" if c is None else c,
                    "" if pif is None else pif, args.rep, n,
                    "bulk:60 (scenario 18)" if args.phase == "contended" else args.competitor,
                    proc.returncode, started, round(wall, 1), load_before, _vm_loadavg(),
                    args.vm, cpu_used["engine"], cpu_used["store"]])
    return proc.returncode


# ----------------------------------------------------------------- collect

def _read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _f(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


RUN_COLUMNS = [
    "run_id", "phase", "vm", "profile", "config", "c", "pif", "rep", "date_utc", "competitor",
    "objects_committed", "objects_failed", "committed_bytes", "busy_seconds",
    "goodput_mbps", "link_mbps", "link_source", "fraction_of_link",
    "object_upload_p50_ms", "object_upload_p95_ms", "manifest_mean_ms",
    "verify_mean_ms", "verify_p95_ms", "checksum_mean_ms", "batch_mean_ms",
    "engine_cpu_seconds", "store_cpu_seconds", "engine_cores_busy",
    "cpu_seconds_per_gib",
    "competitor_goodput_mbps_with", "competitor_goodput_mbps_without",
    "vtop_goodput_mbps_with", "competitor_jain_index",
    "vm_loadavg_before", "vm_loadavg_after", "exit_code",
]
OBJECT_COLUMNS = [
    "run_id", "batch_id", "object_bytes", "object_upload_ms", "manifest_upload_ms",
    "verify_ms", "checksum_ms", "batch_total_ms", "status",
]
CYCLE_COLUMNS = ["run_id", "cycle", "started_seconds", "cycle_seconds", "exit_code",
                 "batches", "committed_batches", "committed_bytes"]


def percentile(values: list[float], pct: float) -> float:
    """Linear interpolation between closest ranks — the harness's own rule."""
    if not values:
        return float("nan")
    s = sorted(values)
    k = (len(s) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def summarize_run(meta: dict, summary: dict, cycles: list[dict],
                  batches: list[dict]) -> tuple[dict, list[dict]]:
    """One run's row, and its per-object rows, from the files the runner wrote."""
    busy = sum(_f(c["cycle_seconds"]) for c in cycles if int(_f(c["committed_batches"])) > 0)
    committed = [b for b in batches if b.get("batch_status") == "committed"]
    failed = [b for b in batches if b.get("batch_status") == "failed"]
    nbytes = sum(int(_f(b["compressed_bytes"])) for b in committed)
    goodput = nbytes * 8 / busy / 1e6 if busy else float("nan")
    calibrated = _f(summary.get("emulator_validation_mbps"), float("nan"))
    profile = meta["profile"]
    if profile == "wan100-policed":
        link, source = POLICER_KBPS / 1000.0, "policer token rate"
    elif profile in GATED_PROFILES:
        link, source = calibrated, "iperf3 on the impairment-free twin, this run"
    else:
        link, source = float("nan"), "none (unshaped)"
    up = [_f(b["upload_duration_ms"]) for b in committed]
    ver = [_f(b["verify_duration_ms"]) for b in committed]
    # CPU from the containers' cgroup counters, read by `run` around the whole
    # harness call: the engine container runs nothing but this run's
    # `vtopctl`, so its counter over the call IS the engine's CPU (the empty
    # closing cycle and the recovery pass included, both milliseconds).
    cpu = _f(meta.get("engine_cpu_seconds"), float("nan"))
    gib = nbytes / (1024 ** 3)
    row = {
        "run_id": meta["run_id"], "phase": meta.get("phase", ""), "vm": meta.get("vm", ""),
        "profile": profile,
        "config": meta["config"], "c": meta["c"], "pif": meta["pif"], "rep": meta["rep"],
        "date_utc": summary["start_time"][:10], "competitor": meta.get("competitor", ""),
        "objects_committed": len(committed), "objects_failed": len(failed),
        "committed_bytes": nbytes, "busy_seconds": round(busy, 3),
        "goodput_mbps": round(goodput, 4), "link_mbps": "" if math.isnan(link) else link,
        "link_source": source,
        "fraction_of_link": "" if math.isnan(link) else round(goodput / link, 4),
        "object_upload_p50_ms": round(percentile(up, 50), 1),
        "object_upload_p95_ms": round(percentile(up, 95), 1),
        "manifest_mean_ms": round(statistics.fmean(
            [_f(b["manifest_upload_duration_ms"]) for b in committed]), 1) if committed else "",
        "verify_mean_ms": round(statistics.fmean(ver), 1) if ver else "",
        "verify_p95_ms": round(percentile(ver, 95), 1) if ver else "",
        "checksum_mean_ms": round(statistics.fmean(
            [_f(b["checksum_duration_ms"]) for b in committed]), 1) if committed else "",
        "batch_mean_ms": round(statistics.fmean(
            [_f(b["total_batch_duration_ms"]) for b in committed]), 1) if committed else "",
        "engine_cpu_seconds": "" if math.isnan(cpu) else round(cpu, 3),
        "store_cpu_seconds": meta.get("store_cpu_seconds", ""),
        "engine_cores_busy": "" if math.isnan(cpu) or not busy else round(cpu / busy, 3),
        "cpu_seconds_per_gib": "" if math.isnan(cpu) or not gib else round(cpu / gib, 3),
        "competitor_goodput_mbps_with": summary.get("competitor_goodput_mbps_with", ""),
        "competitor_goodput_mbps_without": summary.get("competitor_goodput_mbps_without", ""),
        "vtop_goodput_mbps_with": summary.get("vtop_goodput_mbps_with", ""),
        "competitor_jain_index": summary.get("competitor_jain_index", ""),
        "vm_loadavg_before": meta.get("vm_loadavg_before", ""),
        "vm_loadavg_after": meta.get("vm_loadavg_after", ""),
        "exit_code": meta.get("exit_code", ""),
    }
    objects = [{
        "run_id": meta["run_id"], "batch_id": b["batch_id"],
        "object_bytes": b["compressed_bytes"], "object_upload_ms": b["upload_duration_ms"],
        "manifest_upload_ms": b["manifest_upload_duration_ms"],
        "verify_ms": b["verify_duration_ms"], "checksum_ms": b["checksum_duration_ms"],
        "batch_total_ms": b["total_batch_duration_ms"], "status": b["batch_status"],
    } for b in batches]
    return row, objects


def cmd_collect(args) -> int:
    runs, objects, cycles = [], [], []
    for results in args.results:
        for meta in _read_csv(os.path.join(results, "sweep_index.csv")):
            if not meta["run_id"]:
                print(f"[collect] {results}: a run with no results dir "
                      f"({meta['profile']} {meta['config']} r{meta['rep']}, "
                      f"exit {meta['exit_code']}) - recorded as a refusal", file=sys.stderr)
                continue
            rdir = os.path.join(results, meta["run_id"])
            summary_path = os.path.join(rdir, "summary.json")
            if not os.path.exists(summary_path):
                print(f"[collect] {meta['run_id']}: no summary.json (exit "
                      f"{meta['exit_code']}) - refused by the harness, not a row",
                      file=sys.stderr)
                continue
            with open(summary_path, encoding="utf-8") as fh:
                summary = json.load(fh)
            cyc = _read_csv(os.path.join(rdir, "cycle_metrics.csv"))
            row, objs = summarize_run(meta, summary, cyc,
                                      _read_csv(os.path.join(rdir, "batch_metrics.csv")))
            runs.append(row)
            objects += objs
            cycles += [{k: c.get(k, "") for k in CYCLE_COLUMNS} for c in cyc]
    os.makedirs(args.out, exist_ok=True)
    # Sorted, so the committed files do not depend on directory listing order.
    _write_csv(os.path.join(args.out, "runs.csv"),
               sorted(runs, key=lambda r: r["run_id"]), RUN_COLUMNS)
    _write_csv(os.path.join(args.out, "objects.csv"),
               sorted(objects, key=lambda r: (r["run_id"], r["batch_id"])), OBJECT_COLUMNS)
    _write_csv(os.path.join(args.out, "cycles.csv"),
               sorted(cycles, key=lambda r: (r["run_id"], int(r["cycle"]))), CYCLE_COLUMNS)
    print(f"[collect] {len(runs)} runs, {len(objects)} objects -> {args.out}")
    return 0


# ------------------------------------------------------------------ report

def best_config(points: dict[str, list[float]], connections: dict[str, int]) -> str:
    """The grid point with the highest median fraction; ties within
    TIE_POINTS go to fewer connections, then to the label (so the choice is
    deterministic)."""
    medians = {k: statistics.median(v) for k, v in points.items() if v}
    top = max(medians.values())
    tied = [k for k, m in medians.items() if top - m <= TIE_POINTS]
    return min(tied, key=lambda k: (connections[k], k))


def within_noise(threshold: float, values: list[float]) -> bool:
    return bool(values) and min(values) <= threshold <= max(values)


def evaluate_gate(runs: list[dict], objects: list[dict]) -> dict:
    """The gate exactly as the doc fixes it. `runs` rows are runs.csv rows;
    grid runs carry `phase` grid, confirmation runs `phase` confirm."""
    out: dict = {"thresholds": {"unused_fraction": GATE_UNUSED_FRACTION,
                                "p95_ratio": GATE_P95_RATIO}, "profiles": {}}
    best: dict[str, str] = {}
    for profile in GATED_PROFILES:
        grid = [r for r in runs if r["profile"] == profile and r["phase"] == "grid"
                and r["config"] != "default"]
        points: dict[str, list[float]] = {}
        conns: dict[str, int] = {}
        for r in grid:
            points.setdefault(r["config"], []).append(_f(r["fraction_of_link"]))
            conns[r["config"]] = int(_f(r["c"])) * int(_f(r["pif"]))
        if not points:
            continue
        best[profile] = best_config(points, conns)
        conf = [_f(r["fraction_of_link"]) for r in runs if r["profile"] == profile
                and r["phase"] == "confirm" and r["config"] == best[profile]]
        unused = [1 - f for f in conf]
        med = statistics.median(unused) if unused else float("nan")
        out["profiles"][profile] = {
            "best_config": best[profile],
            "selection_median_fraction": round(statistics.median(points[best[profile]]), 4),
            "confirmation_runs": len(conf),
            "unused_median": round(med, 4) if unused else None,
            "unused_min": round(min(unused), 4) if unused else None,
            "unused_max": round(max(unused), 4) if unused else None,
            "opens": (med > GATE_UNUSED_FRACTION) if unused else None,
            "within_noise": within_noise(GATE_UNUSED_FRACTION, unused),
        }
    k = best.get(LOSSY_PROFILE)
    if k:
        by_run = {r["run_id"]: r for r in runs}

        def pooled(profile: str) -> list[float]:
            ids = {r["run_id"] for r in runs if r["profile"] == profile
                   and r["phase"] == "confirm" and r["config"] == k}
            return [_f(o["object_upload_ms"]) for o in objects
                    if o["run_id"] in ids and o["status"] == "committed"]

        lossy, clean = pooled(LOSSY_PROFILE), pooled(CLEAN_PROFILE)
        per_rep_ratios = []
        for rl in sorted(r["run_id"] for r in runs if r["profile"] == LOSSY_PROFILE
                         and r["phase"] == "confirm" and r["config"] == k):
            per_rep_ratios.append(_f(by_run[rl]["object_upload_p95_ms"]))
        ratio = (percentile(lossy, 95) / percentile(clean, 95)) if lossy and clean else None
        clean_p95s = [_f(r["object_upload_p95_ms"]) for r in runs if r["profile"] == CLEAN_PROFILE
                      and r["phase"] == "confirm" and r["config"] == k]
        rep_ratios = ([a / b for a in per_rep_ratios for b in clean_p95s if b]
                      if clean_p95s else [])
        out["p95_ratio"] = {
            "config": k, "lossy_objects": len(lossy), "clean_objects": len(clean),
            "lossy_p95_ms": round(percentile(lossy, 95), 1) if lossy else None,
            "clean_p95_ms": round(percentile(clean, 95), 1) if clean else None,
            "ratio": round(ratio, 3) if ratio is not None else None,
            "ratio_min_across_run_pairs": round(min(rep_ratios), 3) if rep_ratios else None,
            "ratio_max_across_run_pairs": round(max(rep_ratios), 3) if rep_ratios else None,
            "opens": (ratio > GATE_P95_RATIO) if ratio is not None else None,
            "within_noise": within_noise(GATE_P95_RATIO, rep_ratios),
        }
    decided = [p["opens"] for p in out["profiles"].values()] + [
        out.get("p95_ratio", {}).get("opens")]
    out["datagram_issues_open"] = (any(bool(d) for d in decided)
                                   if all(d is not None for d in decided) else None)
    return out


LINK_LIMITED_FRACTION = 0.90
# The lab VM the sweep ran in (Lima, vz): six vCPUs for the engine, the store
# and the middlebox together.
LAB_VCPUS = 6


def name_bottleneck(profile: str, fraction: float, verify_share: float,
                    engine_cores: float, store_cores: float) -> str:
    """What limited a point, by a fixed rule rather than by eye.

    - a shaped profile at >= 90% of its link reference: the link's rate;
    - below that, the verification read-back when it takes a quarter or more
      of a batch's time (a wire change cannot shrink it);
    - otherwise the link's per-flow window: too few TCP flows for the round
      trip and the loss to fill the rate;
    - the unshaped LAN: CPU when the engine or the store kept 80% of the lab
      VM's cores busy over the drain; otherwise the store's I/O path (its
      write and read-back through the middlebox's forwarding), which this
      sweep does not separate further.
    """
    if profile == "lan":
        if engine_cores >= 0.8 * LAB_VCPUS:
            return "CPU (engine)"
        if store_cores >= 0.8 * LAB_VCPUS:
            return "CPU (store)"
        return "store I/O (no CPU saturated)"
    if fraction >= LINK_LIMITED_FRACTION:
        return "link (rate)"
    if verify_share >= 0.25:
        return "verification"
    return "link (per-flow window)"


POINT_COLUMNS = [
    "profile", "config", "c", "pif", "phase", "runs", "goodput_mbps_median",
    "goodput_mbps_min", "goodput_mbps_max", "link_mbps_median", "fraction_median",
    "fraction_min", "fraction_max", "object_upload_p95_ms_pooled",
    "object_upload_p50_ms_pooled", "verify_mean_ms", "verify_p95_ms_pooled",
    "verify_share_of_batch", "manifest_mean_ms", "cpu_seconds_per_gib_median",
    "engine_cores_busy_median", "store_cores_busy_median", "objects", "objects_failed",
    "competitor_goodput_mbps_with", "competitor_goodput_mbps_without",
    "vtop_goodput_mbps_with", "competitor_jain_index", "bottleneck",
]


def _med(values: list[float]):
    vals = [v for v in values if not math.isnan(v)]
    return round(statistics.median(vals), 4) if vals else ""


def aggregate_points(runs: list[dict], objects: list[dict]) -> list[dict]:
    """One row per (profile, config, phase): the repetitions folded together."""
    by_run: dict[str, list[dict]] = {}
    for o in objects:
        by_run.setdefault(o["run_id"], []).append(o)
    groups: dict[tuple, list[dict]] = {}
    for r in runs:
        groups.setdefault((r["profile"], r["config"], r["phase"]), []).append(r)
    nan = float("nan")
    rows = []
    for (profile, config, phase), rs in sorted(groups.items()):
        objs = [o for r in rs for o in by_run.get(r["run_id"], [])
                if o["status"] == "committed"]
        up = [_f(o["object_upload_ms"]) for o in objs]
        ver = [_f(o["verify_ms"]) for o in objs]
        total = [_f(o["batch_total_ms"]) for o in objs]
        frac = [_f(r["fraction_of_link"], nan) for r in rs]
        good = [_f(r["goodput_mbps"], nan) for r in rs]
        store_cores = [(_f(r["store_cpu_seconds"], nan) / _f(r["busy_seconds"]))
                       if _f(r["busy_seconds"]) else nan for r in rs]
        verify_share = (sum(ver) / sum(total)) if total and sum(total) else 0.0
        fmed = _med(frac)
        ecores = _med([_f(r["engine_cores_busy"], nan) for r in rs])
        scores = _med(store_cores)

        def med_of(col: str, rows_=rs):
            return _med([_f(r[col], nan) for r in rows_])

        row = {
            "profile": profile, "config": config, "c": rs[0]["c"], "pif": rs[0]["pif"],
            "phase": phase, "runs": len(rs),
            "goodput_mbps_median": _med(good),
            "goodput_mbps_min": round(min(good), 4), "goodput_mbps_max": round(max(good), 4),
            "link_mbps_median": med_of("link_mbps"),
            "fraction_median": fmed,
            "fraction_min": "" if fmed == "" else round(min(frac), 4),
            "fraction_max": "" if fmed == "" else round(max(frac), 4),
            "object_upload_p95_ms_pooled": round(percentile(up, 95), 1) if up else "",
            "object_upload_p50_ms_pooled": round(percentile(up, 50), 1) if up else "",
            "verify_mean_ms": round(statistics.fmean(ver), 1) if ver else "",
            "verify_p95_ms_pooled": round(percentile(ver, 95), 1) if ver else "",
            "verify_share_of_batch": round(verify_share, 4),
            "manifest_mean_ms": med_of("manifest_mean_ms"),
            "cpu_seconds_per_gib_median": med_of("cpu_seconds_per_gib"),
            "engine_cores_busy_median": ecores, "store_cores_busy_median": scores,
            "objects": len(objs),
            "objects_failed": sum(int(_f(r["objects_failed"])) for r in rs),
            "competitor_goodput_mbps_with": med_of("competitor_goodput_mbps_with"),
            "competitor_goodput_mbps_without": med_of("competitor_goodput_mbps_without"),
            "vtop_goodput_mbps_with": med_of("vtop_goodput_mbps_with"),
            "competitor_jain_index": med_of("competitor_jain_index"),
        }
        row["bottleneck"] = name_bottleneck(
            profile, _f(fmed, 0.0), verify_share, _f(ecores, 0.0), _f(scores, 0.0))
        rows.append(row)
    return rows


# Categorical slots 1-3 of the dataviz reference palette (light / dark), in
# fixed order: parts_in_flight 1, 2, 4. Three slots validate adjacent and
# all-pairs in both modes.
SERIES = (("#2a78d6", "#3987e5"), ("#eb6834", "#d95926"), ("#1baf7a", "#199e70"))
PROFILE_TITLES = {
    "wan100": "100 ms, 10 Mbit/s, 0% loss",
    "wan100-loss1": "100 ms, 10 Mbit/s, 1% gemodel loss",
    "wan100-policed": "100 ms, 10 Mbit/s policer, no queue",
    "lan": "clean LAN",
}


def _svg_style() -> str:
    light = "".join(f".s{i}{{stroke:{lt}}}.f{i}{{fill:{lt}}}" for i, (lt, _) in enumerate(SERIES))
    dark = "".join(f".s{i}{{stroke:{dk}}}.f{i}{{fill:{dk}}}" for i, (_, dk) in enumerate(SERIES))
    return (
        "<style>"
        ".bg{fill:#fcfcfb}.t1{fill:#0b0b0b}.t2{fill:#52514e}.grid{stroke:#e4e3df}"
        ".axis{stroke:#9b9a95}.ref{stroke:#52514e}" + light
        + "@media (prefers-color-scheme: dark){.bg{fill:#1a1a19}.t1{fill:#f0efec}"
        ".t2{fill:#c3c2b7}.grid{stroke:#3a3a37}.axis{stroke:#6f6e69}.ref{stroke:#c3c2b7}"
        + dark + "}"
        "text{font-family:-apple-system,'Segoe UI',Helvetica,Arial,sans-serif}"
        "</style>"
    )


def svg_small_multiples(points: list[dict], profiles: tuple[str, ...], value: str,
                        title: str, y_label: str, y_max: float, y_ticks: list[float],
                        ref: float | None = None, ref_label: str = "",
                        fmt=lambda v: f"{v:g}") -> str:
    """Line small multiples: x = max_concurrency (log2-spaced), one line per
    parts_in_flight, one panel per profile, one shared y scale. Deterministic:
    no ids, no timestamps, fixed float formatting."""
    pw, ph, ml, mr, mt, mb, gap = 300, 220, 56, 16, 64, 58, 24
    width = ml + len(profiles) * pw + (len(profiles) - 1) * gap + mr
    height = mt + ph + mb + 34
    xs = list(GRID_CONCURRENCY)

    def xpos(i_panel, c):
        x0 = ml + i_panel * (pw + gap)
        return x0 + 14 + (pw - 28) * (math.log2(c) / math.log2(xs[-1]))

    def ypos(v):
        return mt + ph - ph * min(v, y_max) / y_max

    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
           f'viewBox="0 0 {width} {height}" role="img" aria-label="{title}">', _svg_style(),
           f'<rect class="bg" x="0" y="0" width="{width}" height="{height}"/>',
           f'<text class="t1" x="{ml}" y="24" font-size="15" font-weight="600">{title}</text>']
    lx = ml
    out.append(f'<text class="t2" x="{lx}" y="46" font-size="12">parts_in_flight:</text>')
    lx += 100
    for i, pif in enumerate(GRID_PARTS):
        out.append(f'<line class="s{i}" x1="{lx}" y1="42" x2="{lx + 18}" y2="42" '
                   f'stroke-width="2"/><circle class="f{i}" cx="{lx + 9}" cy="42" r="4"/>')
        out.append(f'<text class="t2" x="{lx + 24}" y="46" font-size="12">{pif}</text>')
        lx += 46
    if ref is not None:
        out.append(f'<line class="ref" x1="{lx + 10}" y1="42" x2="{lx + 28}" y2="42" '
                   f'stroke-width="1.5" stroke-dasharray="4 3"/>')
        out.append(f'<text class="t2" x="{lx + 34}" y="46" font-size="12">{ref_label}</text>')
    for ip, profile in enumerate(profiles):
        x0 = ml + ip * (pw + gap)
        out.append(f'<text class="t1" x="{x0}" y="{mt - 6}" font-size="12" '
                   f'font-weight="600">{PROFILE_TITLES.get(profile, profile)}</text>')
        for t in y_ticks:
            y = ypos(t)
            out.append(f'<line class="grid" x1="{x0}" y1="{y:.1f}" x2="{x0 + pw}" '
                       f'y2="{y:.1f}" stroke-width="1"/>')
            if ip == 0:
                out.append(f'<text class="t2" x="{x0 - 6}" y="{y + 4:.1f}" font-size="11" '
                           f'text-anchor="end">{fmt(t)}</text>')
        out.append(f'<line class="axis" x1="{x0}" y1="{mt + ph}" x2="{x0 + pw}" '
                   f'y2="{mt + ph}" stroke-width="1"/>')
        for c in xs:
            out.append(f'<text class="t2" x="{xpos(ip, c):.1f}" y="{mt + ph + 16}" '
                       f'font-size="11" text-anchor="middle">{c}</text>')
        if ref is not None:
            y = ypos(ref)
            out.append(f'<line class="ref" x1="{x0}" y1="{y:.1f}" x2="{x0 + pw}" y2="{y:.1f}" '
                       f'stroke-width="1.5" stroke-dasharray="4 3"/>')
        for i, pif in enumerate(GRID_PARTS):
            pts = []
            for c in xs:
                row = next((r for r in points if r["profile"] == profile and r["phase"] == "grid"
                            and r["config"] == config_label(c, pif)), None)
                if row is None or row.get(value, "") == "":
                    continue
                pts.append((xpos(ip, c), ypos(_f(row[value]))))
            if len(pts) > 1:
                d = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
                out.append(f'<polyline class="s{i}" points="{d}" fill="none" stroke-width="2" '
                           f'stroke-linejoin="round" stroke-linecap="round"/>')
            for x, y in pts:
                out.append(f'<circle class="f{i}" cx="{x:.1f}" cy="{y:.1f}" r="4"/>')
    out.append(f'<text class="t2" x="{ml + (width - ml - mr) / 2:.1f}" y="{mt + ph + 40}" '
               'font-size="12" text-anchor="middle">max_concurrency (objects in flight), '
               'median of the grid repetitions</text>')
    out.append(f'<text class="t2" x="14" y="{mt + ph / 2:.1f}" font-size="12" '
               f'text-anchor="middle" transform="rotate(-90 14 {mt + ph / 2:.1f})">'
               f'{y_label}</text>')
    out.append("</svg>")
    return "\n".join(out) + "\n"


def _write_csv(path: str, rows: list[dict], cols: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


# The conditions each profile's rows carry in the document's linted table
# (scripts/check-egress-claims.sh): RTT, loss rate, loss model, bottleneck
# rate, buffer depth.
PROFILE_CONDITIONS = {
    "lan": ("< 1 ms (no delay qdisc)", "0%", "none", "none (unshaped)", "n/a (no qdisc)"),
    "wan100": ("100 ms", "0%", "none", "10 Mbit/s (tbf)", "1 BDP (125 KB)"),
    "wan100-loss1": ("100 ms", "1%", "gemodel (bursty, mean burst 2 packets), upload only",
                     "10 Mbit/s (tbf)", "1 BDP (125 KB)"),
    "wan100-policed": ("100 ms", "0% configured", "policer-induced",
                       "10 Mbit/s policer, 5000 B bucket", "none (policer, no queue)"),
}
HARDWARE = "Apple M5 Max host; Lima vz VM, 6 vCPU, 8 GiB, Linux 6.8 (host shared)"
TRANSPORT = "tcp_tls (s3_native; plain HTTP to the lab MinIO)"
MEASUREMENT_HEADER = [
    "Hardware", "RTT", "Loss rate", "Loss model", "Bottleneck rate", "Buffer depth",
    "Competitor state", "Transport", "Bottleneck", "Date", "Configuration", "Runs",
    "Goodput, Mbit/s: median (min-max)", "Of link reference", "object_upload p95",
    "Verify mean", "CPU-s per GiB", "Competitor with / without VTOP, Mbit/s",
]


def _config_text(row: dict) -> str:
    if row["config"] == "default":
        text = "default (8 in flight, 8 MiB parts, 4 parts in flight)"
    else:
        text = (f"max_concurrency {row['c']}, parts_in_flight {row['pif']}, "
                f"5 MiB parts")
    if row["phase"] == "confirm":
        text += ", confirmation runs"
    if row["phase"] == "contended":
        text += ", scenario 18 workload"
    return text


def measurement_rows(points: list[dict], dates: dict[tuple, str]) -> list[str]:
    """The document's Measurements table rows, one per point, generated so a
    number in the doc is the number in points.csv."""
    order = {"lan": 0, "wan100": 1, "wan100-loss1": 2, "wan100-policed": 3}
    phase_order = {"grid": 0, "confirm": 1, "contended": 2}

    def key(r):
        cfg = (999, 0) if r["config"] == "default" else (int(r["c"]), int(r["pif"]))
        return (phase_order[r["phase"]], order[r["profile"]], cfg)

    lines = ["| " + " | ".join(MEASUREMENT_HEADER) + " |",
             "|" + "|".join("---" for _ in MEASUREMENT_HEADER) + "|"]
    for r in sorted(points, key=key):
        rtt, loss, model, rate, buf = PROFILE_CONDITIONS[r["profile"]]
        contended = r["phase"] == "contended"
        good = ("no object committed" if r["goodput_mbps_median"] == ""
                else f"{_f(r['goodput_mbps_median']):.2f} ({_f(r['goodput_mbps_min']):.2f}-"
                     f"{_f(r['goodput_mbps_max']):.2f})")
        frac = "" if r["fraction_median"] == "" else f"{_f(r['fraction_median']):.3f}"
        p95 = ("" if r["object_upload_p95_ms_pooled"] == ""
               else f"{_f(r['object_upload_p95_ms_pooled']) / 1000:.1f} s")
        ver = "" if r["verify_mean_ms"] == "" else f"{_f(r['verify_mean_ms']) / 1000:.2f} s"
        cpu = ("" if r["cpu_seconds_per_gib_median"] == ""
               else f"{_f(r['cpu_seconds_per_gib_median']):.1f}")
        comp = (f"{_f(r['competitor_goodput_mbps_with']):.2f} / "
                f"{_f(r['competitor_goodput_mbps_without']):.2f}" if contended else "")
        cells = [HARDWARE, rtt, loss, model, rate, buf,
                 "bulk TCP flow (iperf3), same queue" if contended else "none",
                 TRANSPORT, r["bottleneck"], dates[(r["profile"], r["config"], r["phase"])],
                 _config_text(r), r["runs"], good, frac or "n/a", p95 or "n/a", ver or "n/a",
                 cpu or "n/a", comp or "n/a"]
        if int(_f(r["objects_failed"])):
            cells[12] += f"; {r['objects_failed']} object(s) failed"
        lines.append("| " + " | ".join(str(c) for c in cells) + " |")
    return lines


def cmd_report(args) -> int:
    runs = _read_csv(os.path.join(args.data, "runs.csv"))
    objects = _read_csv(os.path.join(args.data, "objects.csv"))
    points = aggregate_points(runs, objects)
    _write_csv(os.path.join(args.data, "points.csv"), points, POINT_COLUMNS)
    dates: dict[tuple, str] = {}
    for r in runs:
        k = (r["profile"], r["config"], r["phase"])
        lo, hi = dates.get(k, (r["date_utc"], r["date_utc"]))
        dates[k] = (min(lo, r["date_utc"]), max(hi, r["date_utc"]))
    date_text = {k: lo if lo == hi else f"{lo} to {hi}" for k, (lo, hi) in dates.items()}
    with open(os.path.join(args.data, "measurements_table.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(measurement_rows(points, date_text)) + "\n")
    gate = evaluate_gate(runs, objects)
    with open(os.path.join(args.data, "gate.json"), "w", encoding="utf-8") as fh:
        json.dump(gate, fh, indent=2, sort_keys=True)
        fh.write("\n")
    os.makedirs(args.charts, exist_ok=True)
    shaped = ("wan100", "wan100-loss1", "wan100-policed")
    charts = {
        "fraction_of_link.svg": svg_small_multiples(
            points, shaped, "fraction_median",
            "VTOP goodput as a fraction of the link reference", "fraction of link",
            1.05, [0.0, 0.2, 0.4, 0.6, 0.8, 1.0], ref=1 - GATE_UNUSED_FRACTION,
            ref_label="gate: below 0.80 opens"),
        "object_upload_p95.svg": svg_small_multiples(
            points, shaped, "object_upload_p95_ms_pooled",
            "object_upload p95, 20 MiB objects", "seconds", args.p95_max_ms,
            [args.p95_max_ms * k / 4 for k in range(5)], fmt=lambda v: f"{v / 1000:g}"),
    }
    for name, body in charts.items():
        with open(os.path.join(args.charts, name), "w", encoding="utf-8") as fh:
            fh.write(body)
    print(json.dumps(gate, indent=2, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="one grid point through run_benchmark.py")
    r.add_argument("--profile", required=True, choices=sorted(PROFILES))
    r.add_argument("--config", default="grid", choices=("grid", "default"))
    r.add_argument("--c", type=int, default=1)
    r.add_argument("--pif", type=int, default=1)
    r.add_argument("--rep", type=int, required=True)
    r.add_argument("--phase", default="grid", choices=("grid", "confirm", "contended"))
    r.add_argument("--objects", type=int, default=0,
                   help="override the backlog size (default: max(4, 2 x width))")
    r.add_argument("--competitor", default="", help='e.g. "bulk:120" (contended rows)')
    r.add_argument("--duration", type=int, default=0,
                   help="duration_seconds; 0 drains the backlog once")
    r.add_argument("--results", required=True)
    r.add_argument("--pool", default="/tmp/vtop483/pool")
    r.add_argument("--run-root", default=os.environ.get("VTOP_BENCH_RUN_ROOT", "/tmp"))
    r.add_argument("--vm", default=os.uname().nodename)
    r.set_defaults(func=cmd_run)
    c = sub.add_parser("collect", help="run directories -> raw CSVs")
    c.add_argument("results", nargs="+")
    c.add_argument("--out", required=True)
    c.set_defaults(func=cmd_collect)
    rp = sub.add_parser("report", help="raw CSVs -> points.csv, gate.json, charts")
    rp.add_argument("--data", required=True)
    rp.add_argument("--charts", required=True)
    rp.add_argument("--p95-max-ms", type=float, default=400000.0)
    rp.set_defaults(func=cmd_report)
    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
