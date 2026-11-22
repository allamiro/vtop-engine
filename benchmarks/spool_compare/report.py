#!/usr/bin/env python3
"""Charts and docs/benchmarks/spool-comparison.md from a committed spool run (#748, #744 section 5.1 (b), (d)).

    python3 -m pip install --require-hashes -r benchmarks/kafka_compare/requirements-report.txt
    python3 benchmarks/spool_compare/report.py benchmarks/spool_compare/results/<run-id>

Refuses (exit 2, the reason on stderr, nothing written) a run `compare_common.schema.load_run`
refuses: unfinished, partial, mixed, private, a performance row from a hosted runner, a config
hash that is not the recorded config's. Prints every path written, one per line, for
`compare_common/regenerate.py`'s drift check.
"""
from __future__ import annotations

import argparse
import os
import sys

HERE = os.path.dirname(os.path.realpath(__file__))
BENCH = os.path.dirname(HERE)
REPO = os.path.dirname(BENCH)
if BENCH not in sys.path:
    sys.path.insert(0, BENCH)

from compare_common import report as creport  # noqa: E402
from compare_common import schema  # noqa: E402
from spool_compare.kind import KIND  # noqa: E402

M = creport.Metric
DRAIN = ("drain",)
CRASH = ("crash-K1", "crash-K2", "crash-K3", "crash-K4-rotation", "down-while-rotating")


def group_label(cell: str, variant: dict[str, str]) -> str:
    return f"{cell}, {variant['source']}, {variant['rotation']}"


REPORT = creport.Report(
    kind=KIND,
    title="Files and a syslog spool to object storage: VTOP, Vector and Fluent Bit",
    doc_name="spool-comparison",
    img_dir="spool/img",
    intro=("A generator appends RFC 5424 lines, each with a unique seeded id, to size-rotated files (rename or "
           "copytruncate; rotated files are kept); each tool reads the same directory read-only in 2 CPUs and "
           "2 GiB and writes to the same lab MinIO, one object per about 32 MiB or 60 s, gzip (#748). Every gap "
           "and duplicate is counted by an independent checker against the seeded ids after the tool has run to "
           "quiescence with no further fault. Fluent Bit's tail offsets may trail or lead delivery by its own "
           "documentation: a gap there is a measured consequence of its documented cursor, shown beside the "
           "settings that decide it (`storage.type`, `db.sync`), not a statement about speed."),
    charts=(
        creport.Chart("spool-drain-throughput", "Drain throughput",
                      "A pre-written backlog across rotated files, drained to quiescence; source bytes in the store.",
                      (M("lines", "lines/s", "lines_per_sec", "higher", DRAIN, digits=0),
                       M("mib", "MiB/s of source bytes", "mib_per_sec", "higher", DRAIN, digits=1))),
        creport.Chart("spool-drain-cost", "What draining costs",
                      "The tool container's cgroup counters over the drain window; RSS is the sampled peak.",
                      (M("cpu", "CPU-seconds per GiB", "cpu_seconds_per_gib", "lower", DRAIN),
                       M("rss", "peak RSS (sampled), bytes", "peak_rss_sampled_bytes", "lower", DRAIN, digits=0))),
        creport.Chart("spool-live-latency", "Time from a line's write to its object being visible",
                      "The generator at half the slowest tool's median drain rate, 10 minutes after a 2 minute "
                      "warm-up; per line.",
                      (M("p50", "write-to-visible p50, ms", "visible_p50_ms", "lower", ("live",), digits=0),
                       M("p99", "write-to-visible p99, ms", "visible_p99_ms", "lower", ("live",), digits=0))),
        creport.Chart("spool-crash", "What a kill -9 and a rotation cost",
                      "Summed over the runs, three kills planned per run; a run whose later triggers never fired is "
                      "left out of the totals and named. K1: at the start of the Nth PUT; K2: right after "
                      "the store answered it; K3: at a seeded time; K4: inside a seeded window after a rotation; "
                      "down-while-rotating: two rotations while the tool was stopped.",
                      (M("gaps", "lines lost (gaps)", "gaps", "lower", CRASH, reduce="sum", digits=0),
                       M("dups", "duplicate lines", "duplicates", "lower", CRASH, reduce="sum", digits=0),
                       M("torn", "torn objects", "torn_objects", "lower", CRASH, reduce="sum", digits=0))),
    ),
    group_label=group_label,
    notes=("## Reading this\n\n"
           "Not ranked: compression ratio, inotify against polling (a configuration choice, recorded in each "
           "tool's configuration file), and verification (classified as in #744 (e); no fault is injected here). "
           "Vector's `disk-buffer` profile is in the CSV and the tables, never a second bar of Vector's colour. "
           "A K1 or K2 run whose tool went idle before every planned kill fired keeps its observed counts in the "
           "CSV and is left out of the crash totals (the table names it), so no total mixes complete and "
           "incomplete kill schedules. Power-cut survival of each tool's own state is not measured here "
           "(#748: wave 3)."),
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run_dir")
    parser.add_argument("--doc", default=None)
    parser.add_argument("--img-dir", default=None)
    args = parser.parse_args(argv)
    try:
        written = creport.build(args.run_dir, REPORT, REPO, doc_path=args.doc, img_dir=args.img_dir)
    except schema.RunRefused as exc:
        print(f"report refused: {exc}", file=sys.stderr)
        return 2
    for path in written:
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
