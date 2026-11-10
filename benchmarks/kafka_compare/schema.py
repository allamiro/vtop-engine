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
import re
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


# A key column is a plain decimal integer, written the way `format_value`
# writes one: "100", never "100.0" or "1e2". The completeness check and the
# report's cell selection both compare these as text, so a second spelling of
# the same number would pass one and vanish from the other.
_CANONICAL_INT = re.compile(r"^[1-9][0-9]*$")
KEY_COLUMNS = ("record_size", "partitions", "producers", "repetition")


def _key_int(row: dict[str, str], column: str, line: int) -> int:
    text = row.get(column, "")
    if not _CANONICAL_INT.match(text):
        raise ReportRefused(f"results.csv line {line}: {column} is {text!r}, not a positive whole number")
    return int(text)


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
        for column in KEY_COLUMNS:
            _key_int(row, column, line)
        if row["mode"] == "fixed_rate":
            if not 0 < _key_int(row, "load_pct", line) < 100:
                raise ReportRefused(f"results.csv line {line}: load_pct is {row['load_pct']!r}")
            # The report reads the offered rate from any row of the cell, ok or
            # not, so every row that carries one must hold a positive whole rate.
            if row["offered_rate"] or row["status"] == "ok":
                _key_int(row, "offered_rate", line)
        elif row["load_pct"]:
            raise ReportRefused(f"results.csv line {line}: a {row['mode']} row has load_pct {row['load_pct']!r}")
        if row["status"] == "ok":
            for column in _NUMERIC_OK[row["mode"]]:
                _number(row, column, line)
            if row["mode"] == "fixed_rate":
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
    """The row's matrix key. `validate_rows` has already required every key
    column to be a canonical integer, so the key is the same text the report
    selects cells by."""
    return (
        row["system"],
        row["mode"],
        int(row["record_size"]),
        int(row["partitions"]),
        int(row["producers"]),
        int(row["repetition"]),
        row["load_pct"],
    )


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _positive_ints(matrix: dict[str, Any], key: str, upper: int | None = None) -> None:
    values = matrix.get(key)
    if (not isinstance(values, list) or not values
            or any(not _is_int(v) or v < 1 or (upper is not None and v >= upper) for v in values)
            or len(set(values)) != len(values)):
        raise ReportRefused(f"environment.json: matrix.{key} is {values!r}, not a list of distinct positive integers")


def check_matrix(matrix: dict[str, Any]) -> None:
    """The recorded matrix has the shape `expected_keys` and the report rely on,
    so a malformed one is a refusal, not a traceback."""
    for key in ("record_sizes", "partitions", "producers"):
        _positive_ints(matrix, key)
    _positive_ints(matrix, "load_pcts", upper=100)
    for key in ("repetitions", "smoke_records"):
        if not _is_int(matrix.get(key)) or matrix[key] < 1:
            raise ReportRefused(f"environment.json: matrix.{key} is {matrix.get(key)!r}, not a positive integer")
    acks = matrix.get("acks")
    if not isinstance(acks, list) or not acks or not all(isinstance(a, str) and a for a in acks):
        raise ReportRefused(f"environment.json: matrix.acks is {acks!r}, not a list of acknowledgement modes")
    if not isinstance(matrix.get("consume"), bool):
        raise ReportRefused(f"environment.json: matrix.consume is {matrix.get('consume')!r}, not true or false")


def check_identity(rows: list[dict[str, str]], environment: dict[str, Any]) -> None:
    """Bind the CSV's identity columns to the run environment.json records.

    The fingerprint covers `comparable` only, while every row carries its own
    run id, commit, image, client, profile and acks. The report labels its
    charts from environment.json, so the rows must name that same run, and
    run with an acknowledgement mode the recorded matrix ran.
    """
    comparable = environment["comparable"]
    recorded = {
        "run_id": environment.get("run_id"),
        "vtop_commit": comparable.get("vtop_commit"),
        "kafka_image": comparable.get("kafka_image"),
        "client": comparable.get("client"),
        "profile": comparable.get("profile"),
    }
    for column, value in recorded.items():
        if rows[0][column] != value:
            raise ReportRefused(f"results.csv has {column}={rows[0][column]!r}, but environment.json records {value!r}")
    acks = environment["matrix"]["acks"]
    for line, row in enumerate(rows, start=2):
        if row["acks"] not in acks:
            raise ReportRefused(f"results.csv line {line}: acks={row['acks']!r}, but the recorded matrix ran {acks}")


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
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ReportRefused(f"{env_path} is not JSON: {exc}") from exc
    if not isinstance(environment, dict):
        raise ReportRefused(f"{env_path} holds a JSON {type(environment).__name__}, not an object")
    if environment.get("run_state") != "complete":
        raise ReportRefused(
            f"{env_path} says run_state={environment.get('run_state')!r}: the run did not finish"
        )
    matrix = environment.get("matrix")
    if not isinstance(matrix, dict):
        raise ReportRefused(f"{env_path} records no matrix to check the CSV against")
    check_matrix(matrix)
    rows = read_rows(os.path.join(run_dir, "results.csv"))
    validate_rows(rows)
    expected_fp = fingerprint(environment)
    if rows[0]["env_fingerprint"] != expected_fp:
        raise ReportRefused(
            f"results.csv was written under environment {rows[0]['env_fingerprint']}, "
            f"but environment.json fingerprints to {expected_fp}"
        )
    check_identity(rows, environment)
    check_complete(rows, matrix)
    return Run(rows, environment, run_dir)


def select(rows: Iterable[dict[str, str]], **criteria: Any) -> list[dict[str, str]]:
    """Rows whose columns equal the given values (compared as text)."""
    wanted = {key: str(value) for key, value in criteria.items()}
    return [row for row in rows if all(row[key] == value for key, value in wanted.items())]
