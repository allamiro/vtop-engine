#!/usr/bin/env python3
"""Judge one capped long-lived run against the #481 rate criterion.

  python3 benchmarks/egress_cap_check.py benchmarks/results/<run_id>

Reads the run's engine_metrics.csv and the cap its summary.json recorded,
and writes beside them (or under --out):

  egress_buckets.csv   every one-second bucket of vtop_upload_egress_bytes_total
  egress_verdict.json  worst bucket, the window's ratio to the cap, pass/fail

Exit 0 when both halves of the criterion hold, 1 when either fails (the files
are written either way: a failed criterion is a result), 2 when the run cannot
be judged at all.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib import egress_cap  # noqa: E402
from lib.engine_run import ENGINE_METRICS_CSV  # noqa: E402


def recorded_cap(run_dir: str) -> float:
    with open(os.path.join(run_dir, "summary.json"), encoding="utf-8") as fh:
        summary = json.load(fh)
    raw = summary.get("upload_max_egress_bytes_per_second", "")
    if raw in ("", None):
        raise ValueError(f"{run_dir}/summary.json records no upload_max_egress_bytes_per_second: "
                         "the run was uncapped, so there is no criterion to judge it by")
    return float(raw)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("run_dir")
    ap.add_argument("--out", help="directory for the two output files (default: run_dir)")
    args = ap.parse_args(argv)
    out_dir = args.out or args.run_dir
    try:
        cap = recorded_cap(args.run_dir)
        buckets = egress_cap.read_buckets(os.path.join(args.run_dir, ENGINE_METRICS_CSV))
        verdict = egress_cap.evaluate(buckets, cap)
    except (OSError, ValueError) as exc:
        print(f"egress_cap_check: {exc}", file=sys.stderr)
        return 2
    os.makedirs(out_dir, exist_ok=True)
    egress_cap.write_buckets(buckets, cap, os.path.join(out_dir, "egress_buckets.csv"))
    egress_cap.write_verdict(verdict, os.path.join(out_dir, "egress_verdict.json"))
    worst = verdict["worst_bucket"]
    print(f"cap {cap:.0f} B/s over {verdict['buckets']} buckets ({verdict['window_seconds']} s): "
          f"worst bucket {worst['rate']:.0f} B/s ({worst['ratio_to_cap']:.4f} of cap, "
          f"bucket {worst['index']}), total {verdict['ratio_to_cap']:.4f} of cap x window "
          f"(predicted {verdict['predicted_ratio']:.4f}) -> "
          f"{'PASS' if verdict['pass'] else 'FAIL'}")
    return 0 if verdict["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
