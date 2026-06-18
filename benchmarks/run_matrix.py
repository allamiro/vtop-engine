#!/usr/bin/env python3
"""Run several benchmark scenarios and build a comparison matrix.

Usage:
  python3 benchmarks/run_matrix.py [scenario.yaml ...] [--results-dir DIR]
  python3 benchmarks/run_matrix.py --all            # every benchmarks/scenarios/*.yaml

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
    "checksum", "backend", "duration_seconds", "total_input_files",
    "total_input_bytes", "total_output_bytes", "compression_ratio_avg",
    "throughput_files_per_sec", "throughput_mb_per_sec", "avg_latency_ms",
    "p95_latency_ms", "p99_latency_ms", "successful_batches", "failed_batches",
    "replayed_files", "cpu_max_percent", "memory_max_mb",
]


def run_one(scenario_path, results_dir):
    proc = subprocess.run(
        [sys.executable, os.path.join(HERE, "run_benchmark.py"), scenario_path,
         "--results-dir", results_dir],
        capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    run_dir = None
    for line in proc.stdout.splitlines():
        if "results ->" in line:
            run_dir = line.split("results ->", 1)[1].strip()
    return run_dir


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("scenarios", nargs="*")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--results-dir", default=os.path.join(HERE, "results"))
    args = ap.parse_args()

    scenarios = list(args.scenarios)
    if args.all or not scenarios:
        scenarios = sorted(glob.glob(os.path.join(HERE, "scenarios", "*.yaml")))
    if not scenarios:
        print("no scenarios found", file=sys.stderr)
        return 2

    os.makedirs(args.results_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    matrix_dir = os.path.join(args.results_dir, f"matrix-{stamp}")
    os.makedirs(matrix_dir)

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

    # matrix.md
    with open(os.path.join(matrix_dir, "matrix.md"), "w") as fh:
        fh.write(f"# Benchmark matrix ({stamp})\n\n")
        fh.write("| " + " | ".join(COMPARE_COLS) + " |\n")
        fh.write("|" + "|".join(["---"] * len(COMPARE_COLS)) + "|\n")
        for r in rows:
            fh.write("| " + " | ".join(str(r.get(c, "")) for c in COMPARE_COLS) + " |\n")

    print(f"\n[matrix] {len(rows)} runs -> {matrix_dir}/matrix.csv, matrix.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
