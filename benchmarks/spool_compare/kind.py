"""The `spool` kind (#748): its cells, profiles, columns and row rules.

Cells (#748 "Cells"):

- `smoke` (correctness): a small pre-written spool, every line present
  exactly once in the bucket; nothing is timed before it passes;
- `drain` (performance): a pre-written backlog across rotated files drained
  to quiescence; lines/s, MiB/s, CPU, RSS, requests per GiB;
- `live` (performance): the generator at a fixed fraction of the slowest
  tool's drain rate; time from a line's write to its object being visible;
- `crash-K1..K3` (correctness): three `kill -9` per run, at the start of the
  Nth PUT (K1, through the S3 tap), right after the store answered it (K2),
  or at a seeded time (K3);
- `crash-K4-rotation` (correctness): the harness rotates the active file at a
  seeded moment and the kill follows inside a seeded window;
- `down-while-rotating` (correctness): the tool is stopped, the file is
  rotated twice, the tool is started; a run where the tool was running during
  a rotation is refused.

Variants: `source` is the directory layout the generator writes (`file`, a
plain append-only log, read by VTOP's File source; `syslog-spool`, the same
lines in a spool directory, read by VTOP's Syslog-spool source; the other
tools read both with the same file input), and `rotation` is `rename` or
`copytruncate`.

Profiles: VTOP `strong` (the default commit rule); Vector `acks` (sink
acknowledgements, memory buffer, `when_full: block`) and `disk-buffer`; Fluent
Bit `durable` (its documented best: `storage.type filesystem`, `db.sync
full`); OpenTelemetry Collector `file-storage` (optional, off in the shipped
plans).
"""
from __future__ import annotations

from compare_common import schema

SOURCES = ("file", "syslog-spool")
ROTATIONS = ("rename", "copytruncate")
CRASH_CELLS = ("crash-K1", "crash-K2", "crash-K3", "crash-K4-rotation")
CELLS = {
    "smoke": "correctness",
    "drain": "performance",
    "live": "performance",
    "crash-K1": "correctness",
    "crash-K2": "correctness",
    "crash-K3": "correctness",
    "crash-K4-rotation": "correctness",
    "down-while-rotating": "correctness",
}
PROFILES = {
    "fluent-bit": ("durable",),
    "vtop": ("strong",),
    "vector": ("acks", "disk-buffer"),
    "otel-collector": ("file-storage",),
}

CHECK_COLUMNS = ("records_expected", "records_present", "gaps", "duplicates", "torn_objects", "orphan_objects",
                 "unexpected")
PERF_COLUMNS = ("window_seconds", "lines_per_sec", "mib_per_sec", "source_bytes", "cpu_seconds",
                "cpu_seconds_per_gib", "peak_rss_sampled_bytes", "put_per_gib", "get_per_gib", "head_per_gib",
                "list_per_gib", "other_per_gib", "objects", "object_size_p50_bytes", "object_size_p99_bytes")
LIVE_COLUMNS = ("offered_rate", "achieved_rate", "sustained", "visible_p50_ms", "visible_p99_ms")
CRASH_COLUMNS = ("kills_planned", "kills", "rotations", "seconds_to_first_object_after_restart")
METRIC_COLUMNS = CHECK_COLUMNS + PERF_COLUMNS + LIVE_COLUMNS + CRASH_COLUMNS

# The metric columns an `ok` row of each cell must carry.
REQUIRED = {
    "smoke": CHECK_COLUMNS,
    "drain": CHECK_COLUMNS + PERF_COLUMNS,
    "live": CHECK_COLUMNS + LIVE_COLUMNS,
    "crash-K1": CHECK_COLUMNS + ("kills_planned", "kills"),
    "crash-K2": CHECK_COLUMNS + ("kills_planned", "kills"),
    "crash-K3": CHECK_COLUMNS + ("kills_planned", "kills"),
    "crash-K4-rotation": CHECK_COLUMNS + ("kills_planned", "kills", "rotations"),
    "down-while-rotating": CHECK_COLUMNS + ("rotations",),
}


def check_row(row: dict[str, str]) -> list[str]:
    problems = []
    if row["source"] not in SOURCES:
        problems.append(f"source {row['source']!r} is not one of {SOURCES}")
    if row["rotation"] not in ROTATIONS:
        problems.append(f"rotation {row['rotation']!r} is not one of {ROTATIONS}")
    if row["status"] != "ok":
        return problems
    missing = [column for column in REQUIRED.get(row["cell"], ()) if row.get(column, "") == ""]
    if missing:
        problems.append(f"an ok {row['cell']} row lacks {', '.join(missing)}")
        return problems
    if row["cell"] == "smoke":
        bad = {c: row[c] for c in ("gaps", "duplicates", "torn_objects", "unexpected") if row[c] not in ("0", "0.0")}
        if bad:
            problems.append(f"a smoke row with {bad} is not ok: a smoke where a line is not present exactly once "
                            "refuses the tool")
    if row["cell"] == "live" and row.get("sustained") not in ("true", "false"):
        problems.append(f"sustained is {row.get('sustained')!r}, not true or false")
    if row["cell"] in CRASH_CELLS and int(float(row["kills"])) < 1:
        problems.append("a crash row with no kill")
    if row["cell"] in CRASH_CELLS and int(float(row["kills"])) > int(float(row["kills_planned"])):
        problems.append(f"a crash row with {row['kills']} kills of {row['kills_planned']} planned")
    if row["cell"] in ("crash-K4-rotation", "down-while-rotating") and int(float(row["rotations"])) < 1:
        problems.append("a rotation cell with no rotation")
    return problems


def unusable(row: dict[str, str]) -> str | None:
    """Why an ok row cannot join comparative totals: a K1/K2 run whose later triggers never fired (the tool
    went idle first) was killed fewer times than planned, and its gaps and duplicates are not comparable with a
    run that took every kill. The observed counts stay in the CSV (review of #908)."""
    if row["cell"] in CRASH_CELLS and row.get("kills") and row.get("kills_planned"):
        if int(float(row["kills"])) < int(float(row["kills_planned"])):
            return f"incomplete kill schedule: {int(float(row['kills']))} of {int(float(row['kills_planned']))} kills"
    return None


KIND = schema.Kind(
    name="spool",
    family="spool",
    use_cases=("b", "d"),
    variant_columns=("source", "rotation"),
    metric_columns=METRIC_COLUMNS,
    cells=CELLS,
    profiles=PROFILES,
    forbidden=None,
    check_row=check_row,
    unusable=unusable,
)
