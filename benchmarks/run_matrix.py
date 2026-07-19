#!/usr/bin/env python3
"""Run several benchmark scenarios and build a comparison matrix.

Usage:
  python3 benchmarks/run_matrix.py [scenario.yaml ...] [--results-dir DIR]
  python3 benchmarks/run_matrix.py --all            # every benchmarks/scenarios/*.yaml
  python3 benchmarks/run_matrix.py --sweep [--formats cef,jsonl]
      [--compression none,gzip:6,zstd:3] [--sizes small,medium]
      [--batches 10000,100000] [--volume 50]       # generated grid (#90)

`--sweep` generates the cross-product of the requested dimensions as scenario
files under the matrix dir (so a run is fully reproducible from its artifacts)
and then runs them exactly like hand-written scenarios.

Produces, under <results-dir>/matrix-<stamp>/: matrix.csv and matrix.md
comparing all runs, plus each individual run dir (untouched, not overwritten).
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))

COMPARE_COLS = [
    "scenario_name", "run_id", "format", "file_size", "volume", "compression",
    "compression_level",
    "checksum", "backend", "duration_seconds", "total_input_files",
    "total_input_bytes", "total_output_bytes", "compression_ratio_avg",
    "throughput_files_per_sec", "throughput_mb_per_sec", "avg_latency_ms",
    "p95_latency_ms", "p99_latency_ms", "successful_batches", "failed_batches",
    "replayed_files", "cpu_max_percent", "memory_max_mb",
]


# Defaults for --sweep (#90): a moderate, representative grid. Formats span
# the compressibility range (CEF/syslog are wordy, JSONL structured, text
# free-form); compression covers the none/gzip/zstd decision at meaningful
# levels; sizes exercise many-small-files vs fewer-large-files.
SWEEP_FORMATS = ["cef", "jsonl", "syslog", "text"]
SWEEP_COMPRESSION = ["none", "gzip:1", "gzip:6", "gzip:9", "zstd:1", "zstd:3", "zstd:9"]
SWEEP_SIZES = ["small", "medium"]
SWEEP_BATCHES = [10000, 100000]


def parse_compression(spec):
    """'gzip:6' -> ('gzip', 6); 'none' -> ('none', 0)."""
    if ":" in spec:
        kind, level = spec.split(":", 1)
        return kind, int(level)
    return spec, 0


def build_sweep(formats, compressions, sizes, batches, volume):
    """The cross-product of the requested dimensions as scenario dicts.

    Pure (no I/O) so the grid itself is unit-testable: a silent generator bug
    would otherwise surface as a plausible-looking but incomplete matrix.
    """
    out = []
    for fmt in formats:
        for comp_spec in compressions:
            kind, level = parse_compression(comp_spec)
            for size in sizes:
                for batch in batches:
                    comp_tag = kind if kind == "none" else f"{kind}{level}"
                    out.append({
                        "name": f"sweep-{fmt}-{comp_tag}-{size}-b{batch // 1000}k",
                        "description": f"sweep cell: {fmt} / {comp_spec} / {size} / max_records={batch}",
                        "volume": volume,
                        "file_size": size,
                        "format": fmt,
                        "compression": kind,
                        "compression_level": level,
                        "checksum": "sha256",
                        "backend": "mock",
                        "batch_max_records": batch,
                        "batch_max_bytes": 1073741824,
                        "batch_max_age_seconds": 60,
                        "duration_seconds": 0,
                        "fault": "none",
                        "sys_sample_interval": 0.5,
                    })
    return out


def write_scenario(scenario, out_dir):
    """Write one sweep cell as a flat YAML file; returns its path."""
    path = os.path.join(out_dir, f"{scenario['name']}.yaml")
    with open(path, "w") as fh:
        for k, v in scenario.items():
            if isinstance(v, str) and any(ch in v for ch in ":#{}[]"):
                v = '"' + v.replace("\\", "\\\\").replace('"', '\\"') + '"'
            fh.write(f"{k}: {v}\n")
    return path


def run_one(scenario_path, results_dir):
    proc = subprocess.run(
        [sys.executable, os.path.join(HERE, "run_benchmark.py"), scenario_path,
         "--results-dir", results_dir],
        capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        print(f"[matrix] WARNING: scenario {scenario_path} failed "
              f"(exit {proc.returncode}); excluded from the matrix", file=sys.stderr)
        return None
    run_dir = None
    for line in proc.stdout.splitlines():
        if "results ->" in line:
            run_dir = line.split("results ->", 1)[1].strip()
    if run_dir is None:
        print(f"[matrix] WARNING: could not locate results dir for {scenario_path}",
              file=sys.stderr)
    return run_dir


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("scenarios", nargs="*")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--results-dir", default=os.path.join(HERE, "results"))
    ap.add_argument("--sweep", action="store_true",
                    help="generate and run the #90 grid instead of scenario files")
    ap.add_argument("--formats", default=",".join(SWEEP_FORMATS))
    ap.add_argument("--compression", default=",".join(SWEEP_COMPRESSION),
                    help="comma-separated kind[:level], e.g. none,gzip:6,zstd:3")
    ap.add_argument("--sizes", default=",".join(SWEEP_SIZES))
    ap.add_argument("--batches", default=",".join(str(b) for b in SWEEP_BATCHES))
    ap.add_argument("--volume", type=int, default=50,
                    help="files generated per sweep cell")
    args = ap.parse_args()

    os.makedirs(args.results_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    matrix_dir = os.path.join(args.results_dir, f"matrix-{stamp}")
    os.makedirs(matrix_dir)

    if args.sweep:
        cells = build_sweep(
            [f.strip() for f in args.formats.split(",") if f.strip()],
            [c.strip() for c in args.compression.split(",") if c.strip()],
            [s.strip() for s in args.sizes.split(",") if s.strip()],
            [int(b) for b in args.batches.split(",") if b.strip()],
            args.volume,
        )
        sweep_dir = os.path.join(matrix_dir, "scenarios")
        os.makedirs(sweep_dir)
        scenarios = [write_scenario(c, sweep_dir) for c in cells]
        print(f"[matrix] sweep: {len(scenarios)} cells -> {sweep_dir}")
    else:
        scenarios = list(args.scenarios)
        if args.all or not scenarios:
            scenarios = sorted(glob.glob(os.path.join(HERE, "scenarios", "*.yaml")))
    if not scenarios:
        print("no scenarios found", file=sys.stderr)
        return 2

    rows = []
    for sc in scenarios:
        print(f"\n=== {os.path.basename(sc)} ===")
        run_dir = run_one(sc, args.results_dir)
        if not run_dir:
            continue
        summ_path = os.path.join(run_dir, "summary.json")
        if not os.path.exists(summ_path):
            continue
        with open(summ_path) as fh:
            summ = json.load(fh)
        flat = dict(summ)
        flat.update(summ.get("scenario", {}))
        rows.append(flat)

    # matrix.csv
    with open(os.path.join(matrix_dir, "matrix.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(COMPARE_COLS)
        for r in rows:
            w.writerow([r.get(c, "") for c in COMPARE_COLS])

    # matrix.md (escape Markdown table cells: pipes and newlines)
    def md_cell(v):
        return str(v).replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ").replace("\r", " ")

    with open(os.path.join(matrix_dir, "matrix.md"), "w") as fh:
        fh.write(f"# Benchmark matrix ({stamp})\n\n")
        fh.write("| " + " | ".join(COMPARE_COLS) + " |\n")
        fh.write("|" + "|".join(["---"] * len(COMPARE_COLS)) + "|\n")
        for r in rows:
            fh.write("| " + " | ".join(md_cell(r.get(c, "")) for c in COMPARE_COLS) + " |\n")

    print(f"\n[matrix] {len(rows)} runs -> {matrix_dir}/matrix.csv, matrix.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
