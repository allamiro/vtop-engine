"""The `archive` kind (#746): its cells, profiles, columns and row rules.

Cells (#746 "Cells"):

- `smoke` (correctness): a small pre-loaded topic, every record read back by
  the checker; nothing is timed before it passes;
- `drain` (performance): a pre-loaded backlog drained to quiescence;
  records/s, MiB/s, CPU, RSS, requests per GiB;
- `live` (performance): a producer at a fixed fraction of the slowest tool's
  drain rate; time to store-visible and time to source-committed;
- `crash-K1..K3` (correctness): three `kill -9` per run through the S3 tap
  (K1, K2) or at a seeded time (K3); gaps, duplicates, torn and orphan objects;
- `verify-fault` (correctness): the tap flips a bit in one read-back in ten;
  corrupt objects committed past.

Profiles (#746 "Output normalisation"): VTOP `strong` (the default) and the
labelled `verify-off` (non-default, isolates the cost of verification; never in
a crash or fault cell); Vector `acks` (sink acknowledgements, memory buffer,
`when_full: block`) and `disk-buffer`; Connect (Lenses) `exactly-once`.
"""
from __future__ import annotations

import json
import os

from compare_common import schema

HERE = os.path.dirname(os.path.abspath(__file__))
CRASH_CELLS = ("crash-K1", "crash-K2", "crash-K3")
CELLS = {
    "smoke": "correctness",
    "drain": "performance",
    "live": "performance",
    "crash-K1": "correctness",
    "crash-K2": "correctness",
    "crash-K3": "correctness",
    "verify-fault": "correctness",
}


def load_tools(path: str = os.path.join(HERE, "tools.json")) -> dict:
    """tools.json's systems: the one place a tool or a profile is added (see its `_comment`)."""
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)["systems"]


TOOLS = load_tools()
PROFILES = {system: tuple(spec["profiles"]) for system, spec in TOOLS.items()}

CHECK_COLUMNS = ("records_expected", "records_present", "gaps", "duplicates", "torn_objects", "orphan_objects",
                 "unexpected")
PERF_COLUMNS = ("window_seconds", "records_per_sec", "mib_per_sec", "source_bytes", "cpu_seconds",
                "cpu_seconds_per_gib", "peak_rss_sampled_bytes", "put_per_gib", "get_per_gib", "head_per_gib",
                "list_per_gib", "other_per_gib", "objects", "object_size_p50_bytes", "object_size_p99_bytes",
                "compression_ratio")
LIVE_COLUMNS = ("offered_rate", "achieved_rate", "sustained", "visible_p50_ms", "visible_p99_ms",
                "committed_p50_ms", "committed_p99_ms")
CRASH_COLUMNS = ("kills", "seconds_to_first_object_after_restart")
FAULT_COLUMNS = ("corrupted_objects", "corrupted_read_back", "corrupted_noticed", "corrupted_committed_past")
METRIC_COLUMNS = CHECK_COLUMNS + PERF_COLUMNS + LIVE_COLUMNS + CRASH_COLUMNS + FAULT_COLUMNS

# The metric columns an `ok` row of each cell must carry.
REQUIRED = {
    "smoke": CHECK_COLUMNS,
    "drain": CHECK_COLUMNS + PERF_COLUMNS,
    "live": CHECK_COLUMNS + LIVE_COLUMNS,
    "crash-K1": CHECK_COLUMNS + CRASH_COLUMNS,
    "crash-K2": CHECK_COLUMNS + CRASH_COLUMNS,
    "crash-K3": CHECK_COLUMNS + CRASH_COLUMNS,
    "verify-fault": CHECK_COLUMNS + FAULT_COLUMNS,
}


def forbidden(system: str, profile: str, cell: str) -> str | None:
    if system == "vtop" and profile == "verify-off" and (cell in CRASH_CELLS or cell == "verify-fault"):
        return ("VTOP's verify-off profile is a deliberate weakening and is never run in a crash or fault cell; "
                "it exists only to isolate the cost of verification in drain and live")
    return None


def check_row(row: dict[str, str]) -> list[str]:
    if row["status"] != "ok":
        return []
    problems = []
    missing = [column for column in REQUIRED.get(row["cell"], ()) if row.get(column, "") == ""]
    if missing:
        problems.append(f"an ok {row['cell']} row lacks {', '.join(missing)}")
    if row["cell"] == "smoke" and not missing:
        bad = {c: row[c] for c in ("gaps", "duplicates", "torn_objects", "unexpected") if row[c] not in ("0", "0.0")}
        if bad:
            problems.append(f"a smoke row with {bad} is not ok: a smoke that does not read back every record "
                            "byte-exact refuses the tool")
    if row["cell"] == "live" and row.get("sustained") not in ("true", "false"):
        problems.append(f"sustained is {row.get('sustained')!r}, not true or false")
    if row["cell"] in CRASH_CELLS and not missing and int(float(row["kills"])) < 1:
        problems.append("a crash row with no kill")
    return problems


KIND = schema.Kind(
    name="archive",
    family="archive",
    use_cases=("a", "d", "e"),
    variant_columns=("record_size",),
    metric_columns=METRIC_COLUMNS,
    cells=CELLS,
    profiles=PROFILES,
    forbidden=forbidden,
    check_row=check_row,
)
