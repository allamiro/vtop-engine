"""Summarise rig runs from their engine logs (benchmarks/long_link/run.sh).

Usage: python3 analyze.py WORK_DIR RUN [RUN ...]       one line per run
       python3 analyze.py WORK_DIR --csv RUN [RUN ...]  the same as CSV

Per run: committed batches, records and uncompressed MiB; throughput end to
end (first productive read to last committed batch, which includes the final
remainder sealed by max_batch_age) and to 90% of the bytes (without that
tail); the engine's CPU over the run's wall clock; and the mean of each stage
timing from the batch_metrics lines.
"""

import csv
import os
import re
import statistics
import sys
from datetime import datetime, timezone

STAGES = ["total_ms", "object_upload_ms", "manifest_upload_ms", "verify_ms", "commit_ms", "state_write_ms",
          "compress_ms"]
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _ts(line: str) -> float:
    return datetime.strptime(line[:26], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp()


def _field(line: str, key: str) -> float:
    m = re.search(rf' {key}="?([0-9.]+)', line)
    return float(m.group(1)) if m else float("nan")


def summarise(work: str, name: str) -> dict:
    run = os.path.join(work, "runs", name)
    with open(os.path.join(run, "result.txt")) as f:
        result = dict(kv.split("=") for kv in f.read().split())
    with open(os.path.join(run, "engine.log")) as f:
        log = [ANSI.sub("", line) for line in f]
    batches = [line for line in log if "batch_metrics" in line]
    first_read = min(_ts(line) for line in log if "read_cycle_profile" in line and " records_read=0 " not in line)
    timed = sorted((_ts(line), _field(line, "uncompressed_bytes")) for line in batches)
    total = sum(b for _, b in timed)
    upto, t90 = 0.0, timed[-1][0]
    for t, b in timed:
        upto += b
        if upto >= 0.9 * total:
            t90 = t
            break
    wall = float(result["t1"]) - float(result["t0"])
    out = {
        "run": name,
        "batches": len(batches),
        "records": int(sum(_field(line, "records") for line in batches)),
        "mib": round(total / 2**20, 1),
        "mib_per_s": round(total / 2**20 / (timed[-1][0] - first_read), 2),
        "mib_per_s_to_90pct": round(upto / 2**20 / (t90 - first_read), 2),
        "cpu_cores": round(int(result["ticks"]) / int(result["hz"]) / wall, 3),
        "object_mib": round(statistics.mean(_field(line, "compressed_bytes") for line in batches) / 2**20, 2),
    }
    for stage in STAGES:
        out[stage] = round(statistics.mean(_field(line, stage) for line in batches))
    return out


def main() -> None:
    work, rest = sys.argv[1], sys.argv[2:]
    as_csv = bool(rest) and rest[0] == "--csv"
    rows = [summarise(work, name) for name in (rest[1:] if as_csv else rest)]
    if as_csv:
        writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
        return
    for row in rows:
        print(" ".join(f"{k}={v}" for k, v in row.items()))


if __name__ == "__main__":
    main()
