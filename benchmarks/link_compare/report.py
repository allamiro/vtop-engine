#!/usr/bin/env python3
"""Charts and docs/benchmarks/link-comparison.md from a committed link run (#747, #744 section 5.1 (c)).

    python3 -m pip install --require-hashes -r benchmarks/kafka_compare/requirements-report.txt
    python3 benchmarks/link_compare/report.py benchmarks/link_compare/results/<run-id>

Refuses (exit 2, the reason on stderr, nothing written) a run
`compare_common.schema.load_run` refuses, and a run whose tuned grid is not the
size the tuned chart's subtitle names. Prints every path written, one per line,
for `compare_common/regenerate.py`'s drift check.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.realpath(__file__))
BENCH = os.path.dirname(HERE)
REPO = os.path.dirname(BENCH)
if BENCH not in sys.path:
    sys.path.insert(0, BENCH)

from compare_common import report as creport  # noqa: E402
from compare_common import schema  # noqa: E402
from link_compare import kind as link  # noqa: E402
from link_compare.kind import KIND  # noqa: E402

M = creport.Metric
DEFAULTS = ("defaults",)


def committed_grid_size() -> int:
    """The tuned grid size of the committed `full` plan: the size the tuned chart's subtitle names."""
    with open(os.path.join(HERE, "plans.json"), encoding="utf-8") as handle:
        plan = json.load(handle)["plans"]["full"]
    return link.check_grids(plan["grids"], plan["systems"])


GRID_SIZE = committed_grid_size()


def group_label(cell: str, variant: dict[str, str]) -> str:
    label = variant["link_profile"]
    if variant.get("connections") not in ("", "default"):
        label += f", {variant['connections']} conns"
    if cell == "tuned":
        label += ", best of grid"
    elif variant.get("grid_point"):
        label += f", {variant['grid_point']}"
    return label


REPORT = creport.Report(
    kind=KIND,
    title="Upload over a long, lossy and policed link: VTOP, aws-cli, rclone and s5cmd",
    doc_name="link-comparison",
    img_dir="link/img",
    intro=("The same seeded, incompressible files, from the same local volume, to the same lab MinIO, through the "
           "same netem middlebox (#477), by each tool in turn, in an order rotated per repetition (#747). Links: "
           "100 ms and 10 Mbit/s with one BDP of buffer (`wan100`), the same with 1% bursty loss "
           "(`wan100-loss1`), and a 10 Mbit/s token-bucket policer with no queue (`wan100-policed`). Objects "
           "committed of N is the harness's own read-back of every object by sha256, never a tool's exit code. "
           "Goodput is committed payload over the tool's whole run, against the iperf3 calibration that brackets "
           "it (the policed link: its token rate). VTOP's goodput includes its own read-back verification; the "
           "other tools do none, and VTOP's upload-only goodput is a separate CSV column "
           "(`vtop_upload_goodput_fraction`), never drawn in its place."),
    charts=(
        creport.Chart("link-defaults-goodput", "Goodput at each tool's documented defaults",
                      "Fraction of the link reference; end to end, VTOP's verification included.",
                      (M("goodput", "goodput, fraction of the link", "goodput_fraction", "higher", DEFAULTS,
                         digits=2),)),
        creport.Chart("link-equal-conns-goodput", "Goodput at equal total connections",
                      "Each tool set to 4, 8 and 16 concurrent TCP connections in total (workers x parts in "
                      "flight, computed per tool and recorded in the CSV).",
                      (M("goodput-eq", "goodput, fraction of the link", "goodput_fraction", "higher",
                         ("equal-conns",), digits=2),)),
        creport.Chart("link-committed", "Objects committed of N",
                      "Judged by the harness's read-back and sha256, at each tool's defaults. The policed link is "
                      "the one that matters.",
                      (M("committed", "objects committed", "objects_committed", "higher", DEFAULTS + ("committed",),
                         digits=0),)),
        creport.Chart("link-wire-bytes", "Bytes on the wire per payload byte",
                      "From the middlebox's interface counters: every retransmission, header and verification "
                      "request counts; no tool's own report is read.",
                      (M("wire", "wire bytes per payload byte", "wire_bytes_per_payload_byte", "lower", DEFAULTS,
                         digits=3),)),
        creport.Chart("link-cpu", "CPU-seconds per GiB committed",
                      "The tool container's cgroup counter over its run (2 CPUs, 2 GiB each).",
                      (M("cpu", "CPU-seconds per GiB", "cpu_seconds_per_gib", "lower", DEFAULTS, digits=2),)),
        creport.Chart("link-neighbour", "The neighbour's goodput beside each tool",
                      "A plain TCP iperf3 flow sharing the bottleneck (#478): alone, beside the tool, alone again. "
                      "Jain's index is in the CSV beside both per-flow goodputs.",
                      (M("neighbour", "neighbour goodput with the tool, Mbit/s", "neighbour_with_mbps", "higher",
                         ("neighbour",), digits=2),
                       M("toolflow", "tool goodput beside the neighbour, Mbit/s", "tool_with_mbps", "higher",
                         ("neighbour",), digits=2))),
        creport.Chart("link-tuned", "Defaults beside the best of each tool's grid",
                      f"Each tool's best point of a tuned grid of {GRID_SIZE} points over its own knobs, the same "
                      "grid size for every tool, re-run on all files.",
                      (M("tuned", "goodput, fraction of the link", "goodput_fraction", "higher",
                         ("defaults", "tuned"), digits=2, ranked=False),)),
    ),
    group_label=group_label,
    notes=("## Reading this\n\n"
           "Not ranked: VTOP's verification read-back (its cost is inside VTOP's end-to-end goodput and wire "
           "bytes; the other tools do not read back), and tuned results, which appear only beside the defaults "
           "with the grid size named. A run whose two calibrations disagree by more than 10% was refused, and a "
           "hosted runner's row counts only where the recorded bottleneck was the link."),
)


def check_grid(run_dir: str) -> None:
    env = schema.read_environment(run_dir)
    plan = env.get("plan") or {}
    if any(cell.get("cell") in ("tuned", "tuned-grid") for cell in plan.get("cells", [])):
        size = link.check_grids(plan.get("grids") or {}, plan["systems"])
        if size != GRID_SIZE:
            raise schema.RunRefused(f"the run's tuned grid has {size} points; the tuned chart's subtitle names "
                                    f"{GRID_SIZE} (the committed full plan's)")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run_dir")
    parser.add_argument("--doc", default=None)
    parser.add_argument("--img-dir", default=None)
    args = parser.parse_args(argv)
    try:
        check_grid(args.run_dir)
        written = creport.build(args.run_dir, REPORT, REPO, doc_path=args.doc, img_dir=args.img_dir)
    except (schema.RunRefused, link.LinkRefused) as exc:
        print(f"report refused: {exc}", file=sys.stderr)
        return 2
    for path in written:
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
