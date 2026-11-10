"""The failover CSVs (#597): columns, writing, and the refusals the report applies.

Two files per run, both under results/<run-id>/:

- `failover.csv`: one row per (system, fault, repetition), what that fault did;
- `timeline.csv`: records acknowledged per 100 ms around each fault, for the
  ack-rate chart.

The report draws from `load_failover_run` only, which returns a complete,
single-environment run or raises `schema.ReportRefused` (#595 rule 9). A
repetition that could not be measured is a ROW saying why, never a gap.
"""
from __future__ import annotations

import csv
import json
import math
import os
from typing import Any, NamedTuple

from kafka_compare import schema
from kafka_compare.failover_stats import FAULTS

SYSTEMS = ("kafka", "vtop")
STATUSES = ("ok", "failed", "refused")
CLIENT_PATHS = {"kafka": "kafka-jvm", "vtop": "vtop-native"}

COLUMNS = (
    "run_id",
    "env_fingerprint",
    "vtop_commit",
    "kafka_image",
    "profile",
    "system",
    "client_path",
    "fault",
    "repetition",
    "status",
    "offered_rate",
    "records_target",
    "records_acked",
    "leader_node",
    "leader_was_controller",
    "fault_at",
    "clock_offset_ms",
    "clock_uncertainty_ms",
    "unavailability_ms",
    "gap_start_ms",
    "baseline_max_gap_ms",
    "time_to_leader_ms",
    "new_leader_node",
    "p99_before_ms",
    "p99_after_ms",
    "p50_after_ms",
    "max_latency_ms",
    "send_errors",
    "retries",
    "reroutes",
    "error_classes",
    "acked_lost",
    "duplicates",
    "unacked_present",
    "verify",
    "started_at",
    "wall_seconds",
    "note",
)

TIMELINE_COLUMNS = ("run_id", "env_fingerprint", "system", "fault", "repetition", "t_ms", "records")

RUN_CONSTANT = ("run_id", "env_fingerprint", "vtop_commit", "kafka_image", "profile")

# An ok row must carry every one of these as a number. time_to_leader_ms may be
# empty on an ok row only when the observer saw no new leader (said in `note`).
_NUMERIC_OK = ("offered_rate", "records_target", "records_acked", "unavailability_ms", "baseline_max_gap_ms",
               "p99_before_ms", "p99_after_ms", "p50_after_ms", "max_latency_ms", "send_errors", "retries")
# The correctness columns: numbers whenever the read-back finished. A
# verification that FAILED (VTOP's verify stops at the first bad record, so it
# cannot count) leaves them empty and says so in `verify`; that row is the
# finding, and the report puts it at the top, never only in a chart.
_CORRECTNESS = ("acked_lost", "duplicates")


def write_header(path: str, columns: tuple[str, ...] = COLUMNS) -> None:
    with open(path, "w", newline="") as handle:
        csv.writer(handle).writerow(columns)


def append_rows(path: str, rows: list[dict[str, Any]], columns: tuple[str, ...] = COLUMNS) -> None:
    """Append rows, fsynced, so an interrupted run keeps what it measured."""
    for row in rows:
        unknown = set(row) - set(columns)
        if unknown:
            raise ValueError(f"unknown columns for {os.path.basename(path)}: {sorted(unknown)}")
    with open(path, "a", newline="") as handle:
        writer = csv.writer(handle)
        for row in rows:
            writer.writerow([schema.format_value(row.get(column)) for column in columns])
        handle.flush()
        os.fsync(handle.fileno())


def _read(path: str, columns: tuple[str, ...]) -> list[dict[str, str]]:
    if not os.path.isfile(path):
        raise schema.ReportRefused(f"no {os.path.basename(path)} at {path}")
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != columns:
            raise schema.ReportRefused(f"{path} has columns {reader.fieldnames}, expected {list(columns)}")
        rows = list(reader)
    if not rows:
        raise schema.ReportRefused(f"{path} has a header and no rows")
    for line, row in enumerate(rows, start=2):
        if None in row or any(value is None for value in row.values()):
            raise schema.ReportRefused(f"{os.path.basename(path)} line {line} has the wrong number of fields")
    return rows


def _number(row: dict[str, str], column: str, line: int, allow_negative: bool = False) -> float:
    text = row.get(column, "")
    try:
        value = float(text)
    except ValueError as exc:
        raise schema.ReportRefused(f"failover.csv line {line}: {column} is {text!r}, not a number") from exc
    if math.isnan(value) or math.isinf(value) or (value < 0 and not allow_negative):
        raise schema.ReportRefused(f"failover.csv line {line}: {column} is {text!r}")
    return value


def validate_rows(rows: list[dict[str, str]]) -> None:
    for line, row in enumerate(rows, start=2):
        if row["system"] not in SYSTEMS:
            raise schema.ReportRefused(f"failover.csv line {line}: unknown system {row['system']!r}")
        if row["fault"] not in FAULTS:
            raise schema.ReportRefused(f"failover.csv line {line}: unknown fault {row['fault']!r}")
        if row["status"] not in STATUSES:
            raise schema.ReportRefused(f"failover.csv line {line}: unknown status {row['status']!r}")
        if row["client_path"] != CLIENT_PATHS[row["system"]]:
            raise schema.ReportRefused(
                f"failover.csv line {line}: {row['system']} measured through {row['client_path']!r}, "
                f"expected {CLIENT_PATHS[row['system']]!r}"
            )
        _number(row, "repetition", line)
        if row["status"] == "ok":
            for column in _NUMERIC_OK:
                _number(row, column, line)
            if row["verify"] == "passed":
                for column in _CORRECTNESS:
                    _number(row, column, line)
            elif not row["verify"].startswith("FAILED"):
                raise schema.ReportRefused(
                    f"failover.csv line {line}: verify is {row['verify']!r}, expected 'passed' or 'FAILED: ...'"
                )
            if row["time_to_leader_ms"]:
                _number(row, "time_to_leader_ms", line, allow_negative=True)
            elif not row["note"].strip():
                raise schema.ReportRefused(
                    f"failover.csv line {line}: no time to leader and no note saying why"
                )
            _number(row, "gap_start_ms", line, allow_negative=True)
        elif not row["note"].strip():
            raise schema.ReportRefused(f"failover.csv line {line}: a {row['status']} row must say why in `note`")
    for column in RUN_CONSTANT:
        values = sorted({row[column] for row in rows})
        if len(values) != 1:
            raise schema.ReportRefused(
                f"failover.csv mixes {len(values)} values of {column} ({', '.join(values)}): "
                "one chart must come from one environment and one commit"
            )


def expected_keys(matrix: dict[str, Any]) -> set[tuple[str, str, int]]:
    return {(system, fault, rep) for system in matrix["systems"] for fault in matrix["faults"]
            for rep in range(1, int(matrix["repetitions"]) + 1)}


def check_complete(rows: list[dict[str, str]], matrix: dict[str, Any]) -> None:
    seen: dict[tuple, int] = {}
    for row in rows:
        key = (row["system"], row["fault"], int(float(row["repetition"])))
        seen[key] = seen.get(key, 0) + 1
    duplicates = sorted(key for key, count in seen.items() if count > 1)
    if duplicates:
        raise schema.ReportRefused(f"failover.csv has duplicate rows for {duplicates[:3]}")
    expected = expected_keys(matrix)
    missing = sorted(expected - set(seen))
    if missing:
        raise schema.ReportRefused(
            f"failover.csv is partial: {len(missing)} of {len(expected)} expected rows are missing, first {missing[:3]}"
        )
    extra = sorted(set(seen) - expected)
    if extra:
        raise schema.ReportRefused(f"failover.csv has rows outside the recorded matrix: {extra[:3]}")


def check_timeline(timeline: list[dict[str, str]], rows: list[dict[str, str]], fingerprint: str) -> None:
    """Every ok repetition has its timeline, from the same run and environment, and nothing else does."""
    have = {}
    for line, point in enumerate(timeline, start=2):
        if point["env_fingerprint"] != fingerprint or point["run_id"] != rows[0]["run_id"]:
            raise schema.ReportRefused(f"timeline.csv line {line} comes from another run or environment")
        try:
            key = (point["system"], point["fault"], int(point["repetition"]))
            float(point["t_ms"])
            records = int(point["records"])
        except ValueError as exc:
            raise schema.ReportRefused(f"timeline.csv line {line}: {exc}") from exc
        if records < 0:
            raise schema.ReportRefused(f"timeline.csv line {line}: negative record count")
        have[key] = have.get(key, 0) + 1
    ok = {(row["system"], row["fault"], int(float(row["repetition"]))) for row in rows if row["status"] == "ok"}
    missing = sorted(ok - set(have))
    if missing:
        raise schema.ReportRefused(f"timeline.csv has no points for the ok repetitions {missing[:3]}")
    extra = sorted(set(have) - ok)
    if extra:
        raise schema.ReportRefused(f"timeline.csv has points for repetitions that are not ok: {extra[:3]}")
    if len(set(have.values())) > 1:
        raise schema.ReportRefused("timeline.csv repetitions have different numbers of bins")


class FailoverRun(NamedTuple):
    rows: list[dict[str, str]]
    timeline: list[dict[str, str]]
    environment: dict[str, Any]
    run_dir: str


def load_failover_run(run_dir: str) -> FailoverRun:
    env_path = os.path.join(run_dir, "environment.json")
    if not os.path.isfile(env_path):
        raise schema.ReportRefused(f"no environment.json in {run_dir}")
    with open(env_path) as handle:
        try:
            environment = json.load(handle)
        except json.JSONDecodeError as exc:
            raise schema.ReportRefused(f"{env_path} is not JSON: {exc}") from exc
    if environment.get("kind") != "failover":
        raise schema.ReportRefused(f"{env_path} is not a failover run (kind={environment.get('kind')!r})")
    if environment.get("run_state") != "complete":
        raise schema.ReportRefused(
            f"{env_path} says run_state={environment.get('run_state')!r}: the run did not finish"
        )
    matrix = environment.get("matrix")
    if not isinstance(matrix, dict):
        raise schema.ReportRefused(f"{env_path} records no matrix to check the CSV against")
    rows = _read(os.path.join(run_dir, "failover.csv"), COLUMNS)
    validate_rows(rows)
    fingerprint = schema.fingerprint(environment)
    if rows[0]["env_fingerprint"] != fingerprint:
        raise schema.ReportRefused(
            f"failover.csv was written under environment {rows[0]['env_fingerprint']}, "
            f"but environment.json fingerprints to {fingerprint}"
        )
    check_complete(rows, matrix)
    timeline = _read(os.path.join(run_dir, "timeline.csv"), TIMELINE_COLUMNS)
    check_timeline(timeline, rows, fingerprint)
    return FailoverRun(rows, timeline, environment, run_dir)
