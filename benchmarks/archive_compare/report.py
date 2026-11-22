#!/usr/bin/env python3
"""Charts and docs/benchmarks/archive-comparison.md from a committed archive run (#746, #744 section 5.1 (a), (d), (e)).

    python3 -m pip install --require-hashes -r benchmarks/kafka_compare/requirements-report.txt
    python3 benchmarks/archive_compare/report.py benchmarks/archive_compare/results/<run-id>

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

from archive_compare.kind import KIND  # noqa: E402
from compare_common import report as creport  # noqa: E402
from compare_common import schema  # noqa: E402

M = creport.Metric
DRAIN = ("drain",)
CRASH = ("crash-K1", "crash-K2", "crash-K3")


def group_label(cell: str, variant: dict[str, str]) -> str:
    size = int(variant["record_size"])
    return f"{cell}, {'1 KiB' if size == 1024 else f'{size} B'}"


REPORT = creport.Report(
    kind=KIND,
    title="Kafka topic to object storage: VTOP, Vector and Kafka Connect S3",
    doc_name="archive-comparison",
    img_dir="archive/img",
    intro=("One Kafka topic (Apache Kafka 4.3.2, one KRaft node, 3 partitions), the same lab MinIO, each tool in "
           "2 CPUs and 2 GiB, output normalised to one JSON line per record, gzip, about 32 MiB or 60 s per "
           "object (#746). Every gap and duplicate is counted by an independent checker against the seeded record "
           "ids, never from a tool's own metadata. Each tool's \"committed\" means something different, so time "
           "to store-visible and time to source-committed are two clocks, never one \"time to durable\"."),
    charts=(
        creport.Chart("archive-drain-throughput", "Drain throughput",
                      "A pre-loaded backlog drained to quiescence; source bytes that are in the store at the end.",
                      (M("records", "records/s", "records_per_sec", "higher", DRAIN, digits=0),
                       M("mib", "MiB/s of source bytes", "mib_per_sec", "higher", DRAIN, digits=1))),
        creport.Chart("archive-drain-cost", "What draining costs",
                      "The tool container's cgroup counters over the drain window; RSS is the sampled peak.",
                      (M("cpu", "CPU-seconds per GiB archived", "cpu_seconds_per_gib", "lower", DRAIN),
                       M("rss", "peak RSS (sampled), bytes", "peak_rss_sampled_bytes", "lower", DRAIN, digits=0))),
        creport.Chart("archive-drain-requests", "Store requests per GiB archived",
                      "From MinIO's own request counters, never the tap. VTOP reads objects back and writes a "
                      "manifest by design.",
                      (M("put", "PUT per GiB", "put_per_gib", "lower", DRAIN),
                       M("get", "GET per GiB", "get_per_gib", "lower", DRAIN),
                       M("head", "HEAD per GiB", "head_per_gib", "lower", DRAIN))),
        creport.Chart("archive-live-latency", "Time to store-visible and time to source-committed",
                      "A producer at half the slowest tool's median drain rate; p99 per record, two clocks.",
                      (M("visible", "store-visible p99, ms", "visible_p99_ms", "lower", ("live",), digits=0),
                       M("committed", "source-committed p99, ms", "committed_p99_ms", "lower", ("live",),
                         digits=0))),
        creport.Chart("archive-objects", "Objects written",
                      "Object count and median object size under the same size and age thresholds.",
                      (M("objects", "objects", "objects", "lower", DRAIN, digits=0),
                       M("size", "object size p50, bytes", "object_size_p50_bytes", "higher", DRAIN, digits=0))),
        creport.Chart("archive-crash", "What a kill -9 costs",
                      "Three kill -9 per run, summed over the runs. K1: at the start of the Nth PUT; K2: right "
                      "after the store answered it; K3: at a seeded time.",
                      (M("gaps", "records lost (gaps)", "gaps", "lower", CRASH, reduce="sum", digits=0),
                       M("dups", "duplicate records", "duplicates", "lower", CRASH, reduce="sum", digits=0),
                       M("torn", "torn objects", "torn_objects", "lower", CRASH, reduce="sum", digits=0),
                       M("orphans", "orphan objects", "orphan_objects", "lower", CRASH, reduce="sum",
                         digits=0))),
        creport.Chart("archive-verify-fault", "A corrupted read-back",
                      "The tap flips one bit in the body of a read-back of one object in ten. A tool that never "
                      "reads back cannot notice it: that is by design, and no speed ranking is drawn from it.",
                      (M("past", "corrupt objects committed past", "corrupted_committed_past", "lower",
                         ("verify-fault",), reduce="sum", digits=0),)),
    ),
    group_label=group_label,
    notes=("## Reading this\n\n"
           "Not ranked: compression ratio (codec levels differ), JVM warm-up (excluded and recorded), each tool's "
           "exactly-once claim (the measured duplicates are the result). VTOP's `verify-off` profile is a "
           "deliberate weakening that isolates the cost of verification; it is in the CSV and the tables, never a "
           "second orange bar."),
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
