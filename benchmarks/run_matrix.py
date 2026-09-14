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
sys.path.insert(0, HERE)

from lib.scenario import load_scenario  # noqa: E402
from lib.shaping import SHAPING_COLUMNS, selected_driver  # noqa: E402

COMPARE_COLS = [
    "scenario_name", "run_id", "format", "file_size", "volume", "compression",
    "compression_level",
    "checksum", "backend", "duration_seconds", "total_input_files",
    "total_input_bytes", "total_output_bytes", "compression_ratio_avg",
    "throughput_files_per_sec", "throughput_mb_per_sec", "avg_latency_ms",
    "p95_latency_ms", "p99_latency_ms", "successful_batches", "failed_batches",
    "replayed_files", "cpu_max_percent", "memory_max_mb",
    # The pipe a shaped run was measured through (#403, #477); empty when
    # unshaped. Spliced from lib/shaping.py so the matrix cannot fall behind a
    # driver that adds a column.
    *SHAPING_COLUMNS, "emulator_validation_mbps", "upload_p95_ms",
    # Which way the sender ran (#476); a comparison across modes is a
    # comparison of namespaces, and the matrix must say so.
    "runner_mode",
    # Which WIRE carried the bytes (#479); a comparison across transports is a
    # comparison of wires, so the matrix must carry it beside runner_mode.
    # Blank for backends that route through no EgressTransport seam.
    "transport",
    # ... and how it was TUNED (#480, review): a matrix that varies only the
    # tuning otherwise shows every row with identical visible conditions, so
    # the reader cannot see the one thing the runs differ in. Serialized as
    # sorted key=value pairs, so the same tuning always renders the same
    # string and two matrices diff cleanly.
    "transport_tuning_flat",
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


def refuse_mixed_drivers_in_scenarios(paths: list) -> None:
    """Refuse an incomparable scenario list before anything is run.

    The same rule as `refuse_mixed_drivers`, applied to what the scenario FILES
    ask for rather than to what the runs produced. A scenario that cannot be
    read is not a refusal — the runner reports that far better, per scenario,
    and a matrix that would not start because one file has a typo in an
    unrelated key would be worse than the problem.
    """
    declared = []
    for path in paths:
        try:
            scenario = load_scenario(path)
            # NORMALISED first (review): `selected_driver` strips whitespace,
            # so a scenario carrying `shaping_driver: " netem "` runs as netem
            # while a raw comparison here calls it unshaped and skips it —
            # and a netem scenario has no `shaping_api_url` to be caught by
            # instead. Deciding shaped-ness on a different reading of the key
            # from the one the runner uses is how a driver goes uncounted.
            driver = selected_driver(scenario)
            if not str(scenario.get("shaping_api_url", "") or "").strip() and \
                    driver != "netem":
                continue
            # A scenario that cannot produce a row must not contribute a driver
            # (review). `load_scenario` only parses and applies defaults, so a
            # scenario with `volume: nope` loads fine, fails in the runner, and
            # is excluded from the matrix by `run_one` — but counting its
            # driver here would abort a matrix that previously wrote the other
            # scenarios' rows perfectly well. Predicting a conflict must never
            # be worse than discovering one.
            if not _would_produce_a_row(scenario):
                continue
            declared.append({"shaping_driver": driver})
        except Exception:  # noqa: BLE001 - see the docstring
            continue
    refuse_mixed_drivers(declared)


def _would_produce_a_row(scenario) -> bool:
    """Whether this scenario looks capable of completing a run.

    A PREDICTION, and a deliberately shallow one: the authority on whether a
    scenario runs is the runner, and duplicating its judgement here would be a
    second implementation to drift. What this rules out is the case that made
    the preflight worse than the check it replaced — a scenario that declares a
    driver and then cannot run, taking the whole matrix down with it. So it
    asks only the questions that are cheap and certain: does the shape build,
    does the run go through the shape, and do the knobs that have to be numbers
    parse as numbers FOR WHOEVER READS THEM. That last qualifier is load-bearing
    (review): a knob the runner consumes is judged by what Python makes of it,
    while a knob written through verbatim into the engine's config file is
    judged by what the ENGINE makes of it, and the two disagree — Python's
    `int()` happily takes a float or a bool that serde refuses outright.

    The asymmetry is what decides what belongs here. A wrong "will not run"
    costs nothing — the post-run refusal still fires on a real conflict, on the
    rows that actually exist — while a wrong "will run" costs the whole matrix.
    So every check the runner makes DETERMINISTICALLY, before it writes
    anything, is worth repeating; nothing that depends on the run itself is.
    """
    from lib.shaping import require_endpoint_reaches_the_shape, shape_from_scenario
    try:
        shape = shape_from_scenario(scenario)
    except Exception:  # noqa: BLE001 - a shape that will not build will not run
        return False
    # runner_mode is validated by the runner before it creates a row, and it
    # is exactly as cheap and deterministic to check here (review).
    from lib.engine import effective_endpoint, runner_mode
    try:
        runner_mode(scenario)
    except Exception:  # noqa: BLE001 - an unrunnable mode will not produce a row
        return False
    if shape is not None:
        # The runner's very next question, and the one this preflight was
        # missing (review): a netem scenario left in the default
        # `runner_mode: host` builds its shape and names a valid mode, yet
        # `require_endpoint_reaches_the_shape` refuses EVERY host-mode netem
        # run — the middlebox has nothing to sit between — and it does so
        # before the results writer exists. Counting that scenario's driver
        # aborted a matrix whose toxiproxy scenarios beside it would have
        # produced perfectly good rows.
        #
        # Deterministic and free: the endpoint is resolved from the
        # environment and the scenario, exactly as the runner resolves it, and
        # nothing here dials anything.
        try:
            require_endpoint_reaches_the_shape(scenario, effective_endpoint(scenario))
        except Exception:  # noqa: BLE001 - a run that bypasses its shape is refused
            return False
    # The combinations the runner refuses outright, for the same reason as
    # runner_mode: deterministic, decided before any row exists, and cheap to
    # ask here (review). Concurrent seeding writes to the final path, so a
    # whole-file format lets the engine commit a half-written object — the
    # runner exits 2 on it, and a scenario that exits 2 contributes no driver.
    # A zero pipeline width is preserved into the engine config on purpose, so
    # the engine's own validation refuses it — deliberately, so the mistake is
    # named rather than clamped. A scenario the engine refuses produces no row
    # (review).
    width = scenario.get("max_concurrent_batches")
    if width not in (None, ""):
        # Judged as the ENGINE will read it, not as Python will coerce it
        # (review). `write_engine_config` renders this knob verbatim —
        # `f"  max_concurrent_batches: {value}"` — into a YAML file serde
        # deserializes into a `usize`, and the loader hands a scenario's
        # `1.5` over as a float and its `true` as a bool. `int()` takes both
        # (1 either way), so the old check counted a driver for a scenario the
        # engine refuses at config load with "invalid type: floating point
        # `1.5`, expected usize" — before a row exists, which is the whole
        # family this preflight is meant to exclude.
        #
        # So ask the question the written file will ask: render the value the
        # way that line renders it, and read THAT as an integer. "1.5", "2.0"
        # and "True" are not integers and the engine refuses all three; the
        # string "3" is, and lands in the file unquoted as `3`, which the
        # engine reads perfectly well.
        rendered = str(width).strip()
        try:
            if int(rendered) <= 0:
                return False
        except ValueError:
            return False
    if scenario.get("duration_seconds") and scenario.get("seed_concurrently", False):
        if scenario.get("whole_file", False) or scenario.get("format") == "binary":
            return False
        try:
            if float(scenario.get("seed_interval_seconds", 1.0)) <= 0:
                return False
        except (TypeError, ValueError):
            return False
    for key, cast in (("volume", int), ("duration_seconds", int),
                      ("backlog_multiplier", float), ("seed_interval_seconds", float)):
        value = scenario.get(key)
        if value in (None, ""):
            continue
        try:
            cast(value)
        except (TypeError, ValueError):
            return False
    return True


def matrix_row(summary: dict) -> dict:
    """One summary flattened into one comparison row.

    The scenario's own keys fill in what the summary does not carry — but they
    must NOT overwrite a column the run RESOLVED (#477). Every scenario carries
    the loader's default `shaping_driver: toxiproxy`, so a blanket update
    stamps that onto unshaped rows too, and `refuse_mixed_drivers` then fires
    on the one comparison it is written to allow: a netem run beside its own
    unshaped baseline. The summary's value is what the run DID; the scenario's
    is what it asked for, and where they differ the former wins.
    """
    row = dict(summary)
    resolved = {key: row[key] for key in SHAPING_COLUMNS if key in row}
    row.update(summary.get("scenario", {}))
    row.update(resolved)
    # An unshaped run carries no shaping columns at all, and blank is what the
    # refusal reads as "not shaped" — spell it, rather than leaving the
    # scenario's request showing.
    for key in SHAPING_COLUMNS:
        row.setdefault(key, "")
    return row


class IncomparableRuns(RuntimeError):
    """Two shaping drivers in one comparison table.

    Not a warning and not a label (#477): a per-connection bandwidth toxic
    and an L3 token bucket with a real buffer are different links, and a
    reader who sees two rows side by side in one table will compare them
    whatever the driver column says. The refusal names both drivers, because
    the fix is to run the comparison on one of them.
    """


def refuse_mixed_drivers(rows: list[dict]) -> None:
    """Refuse a row set spanning two shaping drivers.

    Unshaped rows do not count: a shaped run against an unshaped one is the
    comparison shaping exists FOR, and its two rows differ in a column that
    says so. Two SHAPED rows on different drivers differ in the emulator
    itself, which no column can make comparable.
    """
    drivers = sorted({str(r.get("shaping_driver", "") or "").strip()
                      for r in rows} - {""})
    if len(drivers) > 1:
        raise IncomparableRuns(
            f"this matrix spans {len(drivers)} shaping drivers ({', '.join(drivers)}): "
            "a per-connection proxy toxic and an L3 qdisc are different links, so their "
            "rows cannot share a comparison table. Run the comparison on one driver — "
            "split the scenario list, or point them all at the same shaping_driver")


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

    # BEFORE a single scenario runs (review). The drivers are readable from the
    # scenario files, so a combination that is deterministically incomparable
    # is knowable up front — and the bundled soaks it would waste are 20+
    # minutes of wall clock. Refusing after the last run finishes is correct
    # and useless; `--all` spans toxiproxy and netem today, so this is the
    # ordinary path, not a corner.
    refuse_mixed_drivers_in_scenarios(scenarios)

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
        rows.append(matrix_row(summ))

    # Before a single row is written (#477): a table that exists is a table
    # somebody reads, so the refusal has to land before the file does.
    refuse_mixed_drivers(rows)

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
