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
    # Whether the store was reached through the lab's h3 proxy (#484). The
    # matrix is exactly where this matters: scenario 17 against scenario 12 IS
    # the proxy hop's price, and the two rows are otherwise indistinguishable.
    "h3_proxy",
]

# The columns a RUN RESOLVES, which the scenario's own keys may never fill in
# over. Each of these has a scenario key of the same name saying what was
# ASKED for, and a summary key of the same name saying what the run actually
# did; `matrix_row` copies the scenario in to supply what the summary lacks,
# so without this list the request quietly wins.
RESOLVED_COLUMNS = (
    # #477: every scenario carries the loader's default `shaping_driver:
    # toxiproxy`, so a blanket update stamps it onto unshaped rows too.
    *SHAPING_COLUMNS,
    # #479, #480: a non-empty VTOP_S3_TRANSPORT outranks the scenario's
    # `transport`, and the summary records the wire the bytes went over —
    # taking the scenario's here would relabel an overridden run as the wire it
    # asked for, and the tuning beside it belongs to the transport that ran.
    "transport", "transport_tuning_flat",
    # #476: the summary's value is the NORMALIZED mode, so a scenario that
    # leaves it blank is filed as the `host` it ran in rather than as nothing.
    "runner_mode",
    # #484 (review): the summary says "h3-proxy" or "", the scenario says True
    # or False, and the scenario was winning — so the column added to tell
    # scenario 17 from scenario 12 carried the declaration rather than the
    # topology, in a spelling the other rows do not even use.
    "h3_proxy",
)


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


def declared_driver(path: str) -> str:
    """The shaping driver a scenario FILE asks for; "" when it asks for none or
    cannot be read.

    Used for one thing only: deciding which scenario runs NEXT (see
    `collect_rows`). It never decides whether a matrix is refused, so a wrong
    answer here can move the moment a conflict is found and can do nothing
    else — which is the property that lets it stay this simple.

    Normalised through `selected_driver`, the reading the runner itself uses: a
    scenario carrying `shaping_driver: " netem "` runs as netem, and scheduling
    it as unshaped would put it behind every other scenario in the list.
    """
    try:
        scenario = load_scenario(path)
        driver = selected_driver(scenario)
    except Exception:  # noqa: BLE001 - an unreadable file is the runner's to report
        return ""
    if not str(scenario.get("shaping_api_url", "") or "").strip() and driver != "netem":
        return ""
    return driver


def collect_rows(paths: list, load_row) -> list:
    """Run every scenario and return their rows in LIST order, refusing the
    moment the rows that exist span two shaping drivers.

    WHY NOTHING IS PREDICTED (review, seven rounds of it). The refusal used to
    fire only after every scenario had run, which made a conflict readable from
    the scenario files cost the whole matrix. The first fix judged it up front
    from what the files DECLARE — and a declared driver is not a row: a
    scenario that cannot run produces none, and counting its driver aborted
    matrices that had always succeeded. So the preflight grew a prediction of
    whether each scenario would produce a row, and every round of review found
    another rule it did not mirror: runner_mode, the endpoint check, concurrent
    whole-file seeding, the seed interval, a zero or non-integer pipeline width,
    a fractional duration read as an int, a compression name the engine cannot
    deserialize, a zero batch limit. None of those was the last one. Half of
    them are the ENGINE's rules, which Python can only copy, and no copy at all
    can foresee the other way a scenario fails to produce a row: a lab that is
    not up. A prediction of a run is a second implementation of the run.

    So the decision reads only what the runner actually produced. `load_row`
    runs one scenario and returns its row, or None when it produced none —
    exactly the judgement `run_one` has always made — and the moment the rows
    in hand span two drivers the matrix is refused, before another scenario
    starts. A scenario that cannot run contributes no driver because it
    contributes no row; nothing has to know why.

    WHAT THAT COSTS, and how it is kept small: a real conflict is found only
    once both drivers have produced a row. The declared driver buys the order
    that makes that as early as possible — while some shaped driver is declared
    but has no row yet, the next scenario run is the first one in the list
    declaring such a driver; otherwise the next in list order. A real conflict
    therefore costs one successful run per driver (plus whatever declared runs
    failed on the way), not the matrix. And because the declaration only
    ORDERS, a misread of it can make a refusal later, never wrong: it cannot
    abort a matrix that would have succeeded, and it cannot let a mixed table be
    written.
    """
    declared = [declared_driver(path) for path in paths]
    pending = list(range(len(paths)))
    produced: list = []  # (index, row)
    while pending:
        seen = {str(row.get("shaping_driver", "") or "").strip() for _i, row in produced}
        index = next((i for i in pending if declared[i] and declared[i] not in seen),
                     pending[0])
        pending.remove(index)
        row = load_row(paths[index])
        if row is None:
            continue
        produced.append((index, row))
        try:
            refuse_mixed_drivers([row for _i, row in produced])
        except IncomparableRuns as exc:
            ran = [os.path.basename(paths[i]) for i, _row in produced]
            skipped = [os.path.basename(paths[i]) for i in sorted(pending)]
            raise IncomparableRuns(
                f"{exc}. Refused as soon as the rows in hand spanned both, after "
                f"{len(ran)} scenario(s) produced rows ({', '.join(ran)}); "
                f"{len(skipped)} were not run"
                + (f" ({', '.join(skipped)})" if skipped else "")
                + ". Their run directories are kept; no matrix was written") from None
    return [row for _i, row in sorted(produced, key=lambda pair: pair[0])]


def matrix_row(summary: dict) -> dict:
    """One summary flattened into one comparison row.

    The scenario's own keys fill in what the summary does not carry — but they
    must NOT overwrite a column the run RESOLVED (#477). Every scenario carries
    the loader's default `shaping_driver: toxiproxy`, so a blanket update
    stamps that onto unshaped rows too, and `refuse_mixed_drivers` then fires
    on the one comparison it is written to allow: a netem run beside its own
    unshaped baseline. The summary's value is what the run DID; the scenario's
    is what it asked for, and where they differ the former wins.

    That rule is a list (RESOLVED_COLUMNS), not a shaping special case, and it
    grew because every column added since has needed it: a transport chosen by
    VTOP_S3_TRANSPORT, and the proxy hop, whose column read `True` from the
    scenario where every other row reads `h3-proxy` or blank.
    """
    row = dict(summary)
    resolved = {key: row[key] for key in RESOLVED_COLUMNS if key in row}
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

    # Said up front when the files declare more than one shaped driver, and
    # ONLY said: whether the matrix is refused is decided by the rows the runs
    # produce, not by what the files ask for (see collect_rows). `--all` spans
    # toxiproxy and netem today, so this is the ordinary path.
    drivers = sorted({driver for driver in map(declared_driver, scenarios) if driver})
    if len(drivers) > 1:
        print(f"[matrix] these scenarios declare {len(drivers)} shaping drivers "
              f"({', '.join(drivers)}); one scenario of each runs first, and the matrix is "
              "refused as soon as two of them produce rows", file=sys.stderr)

    def load_row(sc):
        print(f"\n=== {os.path.basename(sc)} ===")
        run_dir = run_one(sc, args.results_dir)
        if not run_dir:
            return None
        summ_path = os.path.join(run_dir, "summary.json")
        if not os.path.exists(summ_path):
            return None
        with open(summ_path) as fh:
            return matrix_row(json.load(fh))

    rows = collect_rows(scenarios, load_row)

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
