#!/usr/bin/env python3
"""Tabulate a competitor-pair job's runs and bundle what is worth committing (#610).

`.github/workflows/competitor-pair.yml` runs scenarios 22 (uncapped) and 23
(capped) a few times each and leaves, in one directory:

    runs.tsv                      one row per half: order, repetition, scenario,
                                  exit code, run id
    results/<run_id>/summary.json each half's own result (a refused half has none)
    environment/, logs/           where and how it ran

This script turns that directory into the table and the committed record, so no
number in either is typed by hand: every figure is read from a half's own
summary.json.

    competitor_pair_report.py OUT_DIR            writes OUT_DIR/pair_runs.csv and
                                                 OUT_DIR/pair_record.json, and
                                                 prints a Markdown table
    competitor_pair_report.py OUT_DIR --bundle D also copies into D the files to
                                                 commit under benchmarks/measurements/:
                                                 the two tables, runs.tsv, environment/,
                                                 logs/, and each ACCEPTED half's
                                                 summary.json and metrics.csv

A refused half is listed in the table with its exit code and no figures, and
its raw per-cycle CSVs (tens of megabytes) are left in the workflow artifact:
they describe no measured window. A pair is a repetition in which both halves
were accepted; only complete pairs enter the medians.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import statistics
import sys

UNCAPPED, CAPPED = "22", "23"

COLUMNS = [
    "order",
    "repetition",
    "scenario",
    "exit_code",
    "run_id",
    "cap_bytes_per_second",
    "emulator_validation_mbps",
    "alone_before_mbps",
    "alone_after_mbps",
    "competitor_goodput_mbps_without",
    "competitor_goodput_mbps_with",
    "share_of_solo_pct",
    "solo_disagreement_pct",
    "competitor_goodput_mbps_over_vtop_window",
    "vtop_goodput_mbps_with",
    "competitor_jain_index",
    "vtop_attributed_cycles",
    "vtop_window_seconds",
    "sync_spread_seconds",
    "final_line_delay_seconds",
    "successful_batches",
    "failed_batches",
]

TABLE_COLUMNS = [
    "order",
    "scenario",
    "exit_code",
    "cap_bytes_per_second",
    "competitor_goodput_mbps_without",
    "competitor_goodput_mbps_with",
    "share_of_solo_pct",
    "vtop_goodput_mbps_with",
    "competitor_jain_index",
    "vtop_attributed_cycles",
]

# What a pair compares, per half, read from its own summary.json.
PAIR_FIELDS = [
    "run_id",
    "competitor_goodput_mbps_with",
    "competitor_goodput_mbps_without",
    "share_of_solo_pct",
    "competitor_goodput_mbps_over_vtop_window",
    "vtop_goodput_mbps_with",
    "competitor_jain_index",
    "vtop_attributed_cycles",
    "vtop_window_seconds",
]


def read_runs(out_dir):
    path = os.path.join(out_dir, "runs.tsv")
    if not os.path.exists(path):
        return None
    with open(path, newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def read_summary(out_dir, run_id):
    if not run_id:
        return {}
    path = os.path.join(out_dir, "results", run_id, "summary.json")
    if not os.path.exists(path):
        return {}
    with open(path) as handle:
        return json.load(handle)


def row_for(run, summary):
    competitor = summary.get("competitor") or {}
    return {
        "order": run["order"],
        "repetition": run["repetition"],
        "scenario": run["scenario"],
        "exit_code": run["exit_code"],
        "run_id": run["run_id"],
        "cap_bytes_per_second": summary.get("upload_max_egress_bytes_per_second", ""),
        "emulator_validation_mbps": summary.get("emulator_validation_mbps", ""),
        "alone_before_mbps": (competitor.get("alone_before") or {}).get("goodput_mbps", ""),
        "alone_after_mbps": (competitor.get("alone_after") or {}).get("goodput_mbps", ""),
        "competitor_goodput_mbps_without": summary.get("competitor_goodput_mbps_without", ""),
        "competitor_goodput_mbps_with": summary.get("competitor_goodput_mbps_with", ""),
        "share_of_solo_pct": competitor.get("share_of_solo_pct", ""),
        "solo_disagreement_pct": competitor.get("solo_disagreement_pct", ""),
        "competitor_goodput_mbps_over_vtop_window": summary.get(
            "competitor_goodput_mbps_over_vtop_window", ""
        ),
        "vtop_goodput_mbps_with": summary.get("vtop_goodput_mbps_with", ""),
        "competitor_jain_index": summary.get("competitor_jain_index", ""),
        "vtop_attributed_cycles": competitor.get("vtop_attributed_cycles", ""),
        "vtop_window_seconds": competitor.get("vtop_window_seconds", ""),
        "sync_spread_seconds": competitor.get("competitor_window_sync_spread_seconds", ""),
        "final_line_delay_seconds": competitor.get("competitor_window_final_line_delay_seconds", ""),
        "successful_batches": summary.get("successful_batches", ""),
        "failed_batches": summary.get("failed_batches", ""),
    }


def accepted(row):
    """A half is accepted when its run exited 0 and it left a competitor figure."""
    return row["exit_code"] == "0" and row["competitor_goodput_mbps_with"] != ""


def half_of(row):
    return {name: row[name] for name in PAIR_FIELDS if name in row} | {
        "upload_max_egress_bytes_per_second": row["cap_bytes_per_second"],
    }


def build_record(rows):
    by_repetition = {}
    for row in rows:
        by_repetition.setdefault(row["repetition"], {})[row["scenario"]] = row
    pairs = []
    for repetition, halves in sorted(by_repetition.items(), key=lambda kv: int(kv[0])):
        uncapped, capped = halves.get(UNCAPPED), halves.get(CAPPED)
        entry = {
            "repetition": int(repetition),
            "uncapped_run": uncapped["run_id"] if uncapped else None,
            "capped_run": capped["run_id"] if capped else None,
            "uncapped_exit_code": uncapped["exit_code"] if uncapped else None,
            "capped_exit_code": capped["exit_code"] if capped else None,
            "complete": bool(uncapped and capped and accepted(uncapped) and accepted(capped)),
        }
        if entry["complete"]:
            entry["without_cap"] = half_of(uncapped)
            entry["with_cap"] = half_of(capped)
            entry["competitor_goodput_mbps_with_minus_without_cap"] = round(
                float(capped["competitor_goodput_mbps_with"])
                - float(uncapped["competitor_goodput_mbps_with"]),
                3,
            )
        pairs.append(entry)
    complete = [p for p in pairs if p["complete"]]
    record = {
        "halves": len(rows),
        "halves_accepted": sum(1 for r in rows if accepted(r)),
        "halves_refused_or_failed": sum(1 for r in rows if not accepted(r)),
        "repetitions": len(pairs),
        "complete_pairs": len(complete),
        "pairs": pairs,
    }
    if complete:
        def values(side, name):
            return [float(p[side][name]) for p in complete if p[side][name] != ""]

        record["median_over_complete_pairs"] = {
            side: {
                name: round(statistics.median(vals), 3)
                for name in (
                    "competitor_goodput_mbps_with",
                    "competitor_goodput_mbps_without",
                    "share_of_solo_pct",
                    "vtop_goodput_mbps_with",
                    "competitor_jain_index",
                )
                if (vals := values(side, name))
            }
            for side in ("without_cap", "with_cap")
        }
    return record


def markdown(rows):
    lines = ["### Competitor pair runs", ""]
    lines.append("| " + " | ".join(TABLE_COLUMNS) + " |")
    lines.append("|" + "---|" * len(TABLE_COLUMNS))
    for row in rows:
        lines.append("| " + " | ".join(str(row[name]) for name in TABLE_COLUMNS) + " |")
    return "\n".join(lines)


def write_tables(out_dir, rows):
    with open(os.path.join(out_dir, "pair_runs.csv"), "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    record = build_record(rows)
    with open(os.path.join(out_dir, "pair_record.json"), "w") as handle:
        json.dump(record, handle, indent=2)
        handle.write("\n")
    return record


def bundle(out_dir, rows, destination):
    os.makedirs(destination, exist_ok=True)
    for name in ("runs.tsv", "pair_runs.csv", "pair_record.json"):
        shutil.copy(os.path.join(out_dir, name), os.path.join(destination, name))
    for directory in ("environment", "logs"):
        source = os.path.join(out_dir, directory)
        if os.path.isdir(source):
            shutil.copytree(source, os.path.join(destination, directory), dirs_exist_ok=True)
    kept = []
    for row in rows:
        if not accepted(row):
            continue
        run_dir = os.path.join(out_dir, "results", row["run_id"])
        target = os.path.join(destination, "runs", row["run_id"])
        os.makedirs(target, exist_ok=True)
        for name in ("summary.json", "metrics.csv"):
            if os.path.exists(os.path.join(run_dir, name)):
                shutil.copy(os.path.join(run_dir, name), os.path.join(target, name))
        kept.append(row["run_id"])
    return kept


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("out_dir")
    parser.add_argument("--bundle", metavar="DIR")
    args = parser.parse_args(argv)
    runs = read_runs(args.out_dir)
    if runs is None:
        print("no runs.tsv: the pair never started")
        return 0
    rows = [row_for(run, read_summary(args.out_dir, run["run_id"])) for run in runs]
    record = write_tables(args.out_dir, rows)
    print(markdown(rows))
    print(
        f"\n{record['complete_pairs']} complete pair(s) of {record['repetitions']} "
        f"repetition(s); {record['halves_refused_or_failed']} half(s) refused or failed."
    )
    if args.bundle:
        kept = bundle(args.out_dir, rows, args.bundle)
        print(f"bundled {len(kept)} accepted half(s) into {args.bundle}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
