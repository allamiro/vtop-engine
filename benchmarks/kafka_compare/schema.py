"""The results CSV: its columns, how a row is written, and how a run is judged complete.

#595 rule 9 is enforced here: `load_run` either returns a complete, single-
environment set of rows or raises `ReportRefused` naming what is wrong. The
report never draws from anything else, so a chart cannot quietly have a hole
where a row is missing - a cell that did not run is a ROW saying so (status
`failed` or `refused`), and the chart shows that gap by name.
"""
from __future__ import annotations

import csv
import json
import math
import os
from collections.abc import Iterable
from typing import Any, NamedTuple

SYSTEMS = ("kafka", "vtop")
STATUSES = ("ok", "failed", "refused")
MODES = ("saturation", "fixed_rate", "consume")

COLUMNS = (
    "run_id",
    "env_fingerprint",
    "vtop_commit",
    "kafka_image",
    "client",
    "profile",
    "system",
    "mode",
    "record_size",
    "partitions",
    "producers",
    "acks",
    "repetition",
    "load_pct",
    "offered_rate",
    "status",
    "sustained",
    "records",
    "records_per_sec",
    "mib_per_sec",
    "lat_avg_ms",
    "lat_max_ms",
    "lat_p50_ms",
    "lat_p95_ms",
    "lat_p99_ms",
    "lat_p999_ms",
    "started_at",
    "wall_seconds",
    "note",
)

# Columns that must be the same on every row of one run: a chart that mixed
# them would compare two environments as if they were one (rule 9).
RUN_CONSTANT = ("run_id", "env_fingerprint", "vtop_commit", "kafka_image", "client", "profile")

_NUMERIC_OK = {
    "saturation": ("records", "records_per_sec", "mib_per_sec"),
    "fixed_rate": ("records", "records_per_sec", "mib_per_sec", "lat_p50_ms", "lat_p95_ms",
                   "lat_p99_ms", "lat_p999_ms"),
    "consume": ("records", "records_per_sec", "mib_per_sec"),
}


class ReportRefused(Exception):
    """The data cannot honestly be charted; the message says exactly why."""


class Cell(NamedTuple):
    record_size: int
    partitions: int
    producers: int


def fingerprint(environment: dict[str, Any]) -> str:
    """A short, stable hash of the environment block that decides comparability.

    Taken over `environment["comparable"]` only - host, Docker VM, limits,
    images, commit, client - so the run's timestamps do not make every run
    "different", and anything that does change the numbers does.
    """
    import hashlib

    comparable = environment.get("comparable")
    if not isinstance(comparable, dict) or not comparable:
        raise ReportRefused("environment.json has no 'comparable' block to fingerprint")
    canonical = json.dumps(comparable, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return repr(round(value, 6))
    return str(value)


def write_header(path: str) -> None:
    with open(path, "w", newline="") as handle:
        csv.writer(handle).writerow(COLUMNS)


def append_row(path: str, row: dict[str, Any]) -> None:
    """Append one row, flushed and fsynced, so an interrupted run keeps every
    row it measured (and the report then refuses it as partial)."""
    unknown = set(row) - set(COLUMNS)
    if unknown:
        raise ValueError(f"unknown result columns: {sorted(unknown)}")
    with open(path, "a", newline="") as handle:
        csv.writer(handle).writerow([format_value(row.get(column)) for column in COLUMNS])
        handle.flush()
        os.fsync(handle.fileno())


# ----------------------------------------------------------------------------- loading


def _number(row: dict[str, str], column: str, line: int) -> float:
    text = row.get(column, "")
    try:
        value = float(text)
    except ValueError as exc:
        raise ReportRefused(f"results.csv line {line}: {column} is {text!r}, not a number") from exc
    if math.isnan(value) or math.isinf(value) or value < 0:
        raise ReportRefused(f"results.csv line {line}: {column} is {text!r}")
    return value


def read_rows(csv_path: str) -> list[dict[str, str]]:
    if not os.path.isfile(csv_path):
        raise ReportRefused(f"no results CSV at {csv_path}")
    with open(csv_path, newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != COLUMNS:
            raise ReportRefused(
                f"{csv_path} has columns {reader.fieldnames}, expected {list(COLUMNS)}"
            )
        rows = list(reader)
    if not rows:
        raise ReportRefused(f"{csv_path} has a header and no rows")
    return rows


def validate_rows(rows: list[dict[str, str]]) -> None:
    """Every row well-formed, every run-constant column identical across rows."""
    for line, row in enumerate(rows, start=2):
        if None in row or any(value is None for value in row.values()):
            raise ReportRefused(f"results.csv line {line} has the wrong number of fields")
        if row["system"] not in SYSTEMS:
            raise ReportRefused(f"results.csv line {line}: unknown system {row['system']!r}")
        if row["mode"] not in MODES:
            raise ReportRefused(f"results.csv line {line}: unknown mode {row['mode']!r}")
        if row["status"] not in STATUSES:
            raise ReportRefused(f"results.csv line {line}: unknown status {row['status']!r}")
        for column in ("record_size", "partitions", "producers", "repetition"):
            _number(row, column, line)
        if row["mode"] == "fixed_rate":
            _number(row, "load_pct", line)
        if row["status"] == "ok":
            for column in _NUMERIC_OK[row["mode"]]:
                _number(row, column, line)
            if row["mode"] == "fixed_rate":
                _number(row, "offered_rate", line)
                if row["sustained"] not in ("true", "false"):
                    raise ReportRefused(f"results.csv line {line}: sustained is {row['sustained']!r}")
        elif not row["note"].strip():
            raise ReportRefused(
                f"results.csv line {line}: a {row['status']} row must say why in `note`"
            )
    for column in RUN_CONSTANT:
        values = sorted({row[column] for row in rows})
        if len(values) != 1:
            raise ReportRefused(
                f"results.csv mixes {len(values)} values of {column} ({', '.join(values)}): "
                "one chart must come from one environment and one commit"
            )


def expected_keys(matrix: dict[str, Any]) -> set[tuple]:
    """Every (system, mode, record_size, partitions, producers, repetition, load_pct)
    a complete run of `matrix` writes a row for."""
    keys = set()
    reps = int(matrix["repetitions"])
    for size in matrix["record_sizes"]:
        for partitions in matrix["partitions"]:
            for producers in matrix["producers"]:
                for system in SYSTEMS:
                    for rep in range(1, reps + 1):
                        keys.add((system, "saturation", size, partitions, producers, rep, ""))
                        for pct in matrix["load_pcts"]:
                            keys.add((system, "fixed_rate", size, partitions, producers, rep, str(pct)))
                        if matrix.get("consume"):
                            keys.add((system, "consume", size, partitions, producers, rep, ""))
    return keys


def row_key(row: dict[str, str]) -> tuple:
    return (
        row["system"],
        row["mode"],
        int(float(row["record_size"])),
        int(float(row["partitions"])),
        int(float(row["producers"])),
        int(float(row["repetition"])),
        str(int(float(row["load_pct"]))) if row["load_pct"] else "",
    )


def check_complete(rows: list[dict[str, str]], matrix: dict[str, Any]) -> None:
    """Refuse a partial run: every expected row present exactly once, nothing extra."""
    seen: dict[tuple, int] = {}
    for row in rows:
        key = row_key(row)
        seen[key] = seen.get(key, 0) + 1
    duplicates = sorted(key for key, count in seen.items() if count > 1)
    if duplicates:
        raise ReportRefused(f"results.csv has duplicate rows for {duplicates[:3]}")
    expected = expected_keys(matrix)
    missing = sorted(expected - set(seen))
    if missing:
        raise ReportRefused(
            f"results.csv is partial: {len(missing)} of {len(expected)} expected rows are missing, "
            f"first {missing[:3]}"
        )
    extra = sorted(set(seen) - expected)
    if extra:
        raise ReportRefused(f"results.csv has rows outside the recorded matrix: {extra[:3]}")


class Run(NamedTuple):
    rows: list[dict[str, str]]
    environment: dict[str, Any]
    run_dir: str


def load_run(run_dir: str) -> Run:
    """The rows and environment of one complete run, or ReportRefused."""
    env_path = os.path.join(run_dir, "environment.json")
    if not os.path.isfile(env_path):
        raise ReportRefused(f"no environment.json in {run_dir}")
    with open(env_path) as handle:
        try:
            environment = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ReportRefused(f"{env_path} is not JSON: {exc}") from exc
    if environment.get("run_state") != "complete":
        raise ReportRefused(
            f"{env_path} says run_state={environment.get('run_state')!r}: the run did not finish"
        )
    matrix = environment.get("matrix")
    if not isinstance(matrix, dict):
        raise ReportRefused(f"{env_path} records no matrix to check the CSV against")
    rows = read_rows(os.path.join(run_dir, "results.csv"))
    validate_rows(rows)
    expected_fp = fingerprint(environment)
    if rows[0]["env_fingerprint"] != expected_fp:
        raise ReportRefused(
            f"results.csv was written under environment {rows[0]['env_fingerprint']}, "
            f"but environment.json fingerprints to {expected_fp}"
        )
    check_complete(rows, matrix)
    return Run(rows, environment, run_dir)


def select(rows: Iterable[dict[str, str]], **criteria: Any) -> list[dict[str, str]]:
    """Rows whose columns equal the given values (compared as text)."""
    wanted = {key: str(value) for key, value in criteria.items()}
    return [row for row in rows if all(row[key] == value for key, value in wanted.items())]
