"""The results CSV: its columns, how a row is written, and how a run is judged complete.

#595 rule 9 is enforced here: `load_run` either returns a complete, single-
environment set of rows or raises `ReportRefused` naming what is wrong. The
report never draws from anything else, so a chart cannot quietly have a hole
where a row is missing - a cell that did not run is a ROW saying so (status
`failed` or `refused`), and the chart shows that gap by name.

There are two layouts (#596). Schema 2 (`COLUMNS`) adds compression, the load
basis, the latency basis, the end-to-end mode, the measurement window and
`same_config_as`, and it moves `profile` from the run-constant set into the
row key. Schema 1 (`COLUMNS_V1`, phase 1's committed run) still loads. Its rows
are read as schema 2 with compression `none`, load basis `kafka` on fixed-rate
rows and latency basis `single`, which is what that run did, so its report is
unchanged byte for byte.
"""
from __future__ import annotations

import csv
import json
import math
import os
import re
from collections.abc import Iterable
from typing import Any, NamedTuple

from kafka_compare import plan

SYSTEMS = plan.SYSTEMS
STATUSES = ("ok", "failed", "refused")
MODES = ("saturation", "fixed_rate", "consume", "e2e")
COMPRESSIONS = plan.COMPRESSIONS
ACKS = plan.ACKS
PROFILES = plan.PROFILES
LOAD_BASES = plan.LOAD_BASES

COLUMNS_V1 = (
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

COLUMNS = (
    "run_id",
    "env_fingerprint",
    "vtop_commit",
    "kafka_image",
    "client",
    "system",
    "mode",
    "record_size",
    "partitions",
    "producers",
    "compression",
    "acks",
    "profile",
    # The profile whose measurement this row repeats (#596 decision 2). VTOP's
    # node configuration is the same under both profiles, so it runs once per
    # cell and its rows are recorded under each profile; a copy names the
    # profile it was measured under. Blank on every row measured as itself.
    "same_config_as",
    "repetition",
    "load_basis",
    "load_pct",
    "offered_rate",
    "status",
    "sustained",
    "records",
    "records_produced",
    "records_per_sec",
    "mib_per_sec",
    "lat_avg_ms",
    "lat_max_ms",
    "lat_p50_ms",
    "lat_p95_ms",
    "lat_p99_ms",
    "lat_p999_ms",
    "lat_basis",
    "started_at",
    "window_start_ms",
    "window_end_ms",
    "wall_seconds",
    "note",
)

# Columns that must be the same on every row of one run: a chart that mixed
# them would compare two environments as if they were one (rule 9). Schema 1
# also held `profile` constant; schema 2 makes it a key.
RUN_CONSTANT = ("run_id", "env_fingerprint", "vtop_commit", "kafka_image", "client")
RUN_CONSTANT_V1 = RUN_CONSTANT + ("profile",)

# The row key: completeness and duplicates are judged on it.
KEY = ("system", "mode", "record_size", "partitions", "producers", "compression", "acks", "profile",
       "repetition", "load_basis", "load_pct")

_LATENCY = ("lat_p50_ms", "lat_p95_ms", "lat_p99_ms", "lat_p999_ms")
_NUMERIC_OK = {
    "saturation": ("records", "records_per_sec", "mib_per_sec"),
    "fixed_rate": ("records", "records_per_sec", "mib_per_sec") + _LATENCY,
    "consume": ("records", "records_per_sec", "mib_per_sec"),
    "e2e": ("records", "records_produced") + _LATENCY,
}
# Modes whose ok rows carry percentiles, and so say how they were combined.
_HAS_LATENCY = ("saturation", "fixed_rate", "e2e")
_LAT_BASIS = re.compile(r"^(single|worst-of-[2-9][0-9]*)$")


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


def write_header(path: str, columns: tuple[str, ...] = COLUMNS) -> None:
    with open(path, "w", newline="") as handle:
        csv.writer(handle).writerow(columns)


def append_row(path: str, row: dict[str, Any], columns: tuple[str, ...] = COLUMNS) -> None:
    """Append one row, flushed and fsynced, so an interrupted run keeps every
    row it measured (and the report then refuses it as partial)."""
    unknown = set(row) - set(columns)
    if unknown:
        raise ValueError(f"unknown result columns: {sorted(unknown)}")
    with open(path, "a", newline="") as handle:
        csv.writer(handle).writerow([format_value(row.get(column)) for column in columns])
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


def _upgrade_v1(row: dict[str, str]) -> dict[str, str]:
    """A schema-1 row read as schema 2: what phase 1 did, said in the new
    columns. A short row is left as it is, so `validate_rows` refuses it by
    line."""
    if None in row or any(value is None for value in row.values()):
        return row
    out = dict(row)
    out["compression"] = "none"
    out["same_config_as"] = ""
    out["load_basis"] = "kafka" if row["mode"] == "fixed_rate" else ""
    out["lat_basis"] = "single" if row["status"] == "ok" and row["mode"] in _HAS_LATENCY else ""
    for column in ("records_produced", "window_start_ms", "window_end_ms"):
        out[column] = ""
    return out


def read_rows(csv_path: str, *, allow_empty: bool = False) -> tuple[list[dict[str, str]], int]:
    """The rows, as schema 2, and the schema the file was written in."""
    if not os.path.isfile(csv_path):
        raise ReportRefused(f"no results CSV at {csv_path}")
    with open(csv_path, newline="") as handle:
        reader = csv.DictReader(handle)
        header = tuple(reader.fieldnames or ())
        if header == COLUMNS:
            version = 2
        elif header == COLUMNS_V1:
            version = 1
        else:
            raise ReportRefused(
                f"{csv_path} has columns {reader.fieldnames}, expected schema 2 {list(COLUMNS)} "
                f"or schema 1 {list(COLUMNS_V1)}"
            )
        rows = list(reader)
    if not rows and not allow_empty:
        raise ReportRefused(f"{csv_path} has a header and no rows")
    if version == 1:
        rows = [_upgrade_v1(row) for row in rows]
    return rows, version


def _whole_or_blank(row: dict[str, str], column: str, line: int) -> int | None:
    text = row[column]
    if text == "":
        return None
    if not (text == "0" or _CANONICAL_INT.match(text)):
        raise ReportRefused(f"results.csv line {line}: {column} is {text!r}, not a whole number")
    return int(text)


def _validate_v2_columns(row: dict[str, str], line: int) -> None:
    """The schema-2 columns of one row (a schema-1 row has them filled in)."""
    if row["compression"] not in COMPRESSIONS:
        raise ReportRefused(f"results.csv line {line}: unknown compression {row['compression']!r}")
    if row["acks"] not in ACKS:
        raise ReportRefused(f"results.csv line {line}: acks is {row['acks']!r}, not one of {list(ACKS)}")
    if row["profile"] not in PROFILES:
        raise ReportRefused(f"results.csv line {line}: unknown profile {row['profile']!r}")
    same = row["same_config_as"]
    if same:
        if same not in PROFILES or same == row["profile"]:
            raise ReportRefused(f"results.csv line {line}: same_config_as is {same!r} on a {row['profile']!r} "
                                "row; it must name the other profile the row was measured under")
        if row["system"] != "vtop":
            raise ReportRefused(f"results.csv line {line}: a {row['system']} row repeats another profile's "
                                "measurement; only VTOP, whose configuration the profiles do not change, may")
    basis = row["load_basis"]
    if row["mode"] in ("fixed_rate", "e2e") and basis not in LOAD_BASES:
        raise ReportRefused(f"results.csv line {line}: a {row['mode']} row has load_basis {basis!r}")
    if row["mode"] in ("saturation", "consume") and basis:
        raise ReportRefused(f"results.csv line {line}: a {row['mode']} row has load_basis {basis!r}")
    if row["mode"] != "e2e" and row["records_produced"]:
        raise ReportRefused(f"results.csv line {line}: records_produced is for e2e rows only")
    if row["mode"] not in _HAS_LATENCY and row["lat_basis"]:
        raise ReportRefused(f"results.csv line {line}: a {row['mode']} row has lat_basis {row['lat_basis']!r}")
    start = _whole_or_blank(row, "window_start_ms", line)
    end = _whole_or_blank(row, "window_end_ms", line)
    if (start is None) != (end is None) or (start is not None and end < start):
        raise ReportRefused(f"results.csv line {line}: the window {row['window_start_ms']!r}.."
                            f"{row['window_end_ms']!r} is not a start and an end at or after it")


def validate_rows(rows: list[dict[str, str]], version: int = 2) -> None:
    """Every row well-formed, every run-constant column identical across rows."""
    for line, row in enumerate(rows, start=2):
        if None in row or any(value is None for value in row.values()):
            raise ReportRefused(f"results.csv line {line} has the wrong number of fields")
        if row["mode"] not in MODES:
            raise ReportRefused(f"results.csv line {line}: unknown mode {row['mode']!r}")
        if row["system"] not in SYSTEMS:
            raise ReportRefused(f"results.csv line {line}: unknown system {row['system']!r}")
        _validate_v2_columns(row, line)
        if row["status"] not in STATUSES:
            raise ReportRefused(f"results.csv line {line}: unknown status {row['status']!r}")
        for column in KEY_COLUMNS:
            _key_int(row, column, line)
        if row["mode"] in ("fixed_rate", "e2e"):
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
            if row["mode"] in ("fixed_rate", "e2e"):
                if row["sustained"] not in ("true", "false"):
                    raise ReportRefused(f"results.csv line {line}: sustained is {row['sustained']!r}")
            if row["mode"] in _HAS_LATENCY and not _LAT_BASIS.match(row["lat_basis"]):
                raise ReportRefused(f"results.csv line {line}: lat_basis is {row['lat_basis']!r}, "
                                    "not single or worst-of-N")
            if row["mode"] == "e2e" and row["records"] != row["records_produced"]:
                # Fewer consumed than produced is a gap the CDF would hide: the
                # run writes such a row as failed, saying how many are missing.
                raise ReportRefused(f"results.csv line {line}: an ok e2e row consumed {row['records']} of "
                                    f"{row['records_produced']} records produced")
        elif not row["note"].strip():
            raise ReportRefused(
                f"results.csv line {line}: a {row['status']} row must say why in `note`"
            )
    for column in (RUN_CONSTANT_V1 if version == 1 else RUN_CONSTANT):
        values = sorted({row[column] for row in rows})
        if len(values) != 1:
            raise ReportRefused(
                f"results.csv mixes {len(values)} values of {column} ({', '.join(values)}): "
                "one chart must come from one environment and one commit"
            )


def expected_keys(matrix: dict[str, Any]) -> set[tuple]:
    """Every row key (see `KEY`) a complete run of `matrix` writes. There is
    one definition, in `plan`, for the runner and the report."""
    return plan.expected_keys(matrix)


def row_key(row: dict[str, str]) -> tuple:
    """The row's matrix key, in `KEY` order. `validate_rows` has already
    required every integer key column to be canonical, so the key is the same
    text the report selects cells by."""
    return (
        row["system"],
        row["mode"],
        int(row["record_size"]),
        int(row["partitions"]),
        int(row["producers"]),
        row["compression"],
        row["acks"],
        row["profile"],
        int(row["repetition"]),
        row["load_basis"],
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
    try:
        plan.check_matrix(matrix)
    except plan.PlanRefused as exc:
        raise ReportRefused(f"environment.json: {exc}") from exc


def check_identity(rows: list[dict[str, str]], environment: dict[str, Any], version: int = 2) -> None:
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
    }
    if version == 1:
        # One profile per schema-1 run, recorded in the environment.
        recorded["profile"] = comparable.get("profile")
    for column, value in recorded.items():
        if rows[0][column] != value:
            raise ReportRefused(f"results.csv has {column}={rows[0][column]!r}, but environment.json records {value!r}")
    acks = list(environment["matrix"]["acks"])
    acks += [extra["acks"] for extra in environment["matrix"].get("acks_slice") or [] if extra["acks"] not in acks]
    for line, row in enumerate(rows, start=2):
        if row["acks"] not in acks:
            raise ReportRefused(f"results.csv line {line}: acks={row['acks']!r}, but the recorded matrix ran {acks}")


class Completeness(NamedTuple):
    expected: set[tuple]
    missing: list[tuple]
    duplicates: list[tuple]
    extra: list[tuple]


def completeness(rows: list[dict[str, str]], matrix: dict[str, Any]) -> Completeness:
    """What `rows` lack, repeat or add against what `matrix` owes."""
    seen: dict[tuple, int] = {}
    for row in rows:
        key = row_key(row)
        seen[key] = seen.get(key, 0) + 1
    expected = expected_keys(matrix)
    return Completeness(expected, sorted(expected - set(seen)),
                        sorted(key for key, count in seen.items() if count > 1), sorted(set(seen) - expected))


def check_complete(rows: list[dict[str, str]], matrix: dict[str, Any]) -> None:
    """Refuse a partial run: every expected row present exactly once, nothing extra."""
    expected, missing, duplicates, extra = completeness(rows, matrix)
    if duplicates:
        raise ReportRefused(f"results.csv has duplicate rows for {duplicates[:3]}")
    if missing:
        raise ReportRefused(
            f"results.csv is partial: {len(missing)} of {len(expected)} expected rows are missing, "
            f"first {missing[:3]}"
        )
    if extra:
        raise ReportRefused(f"results.csv has rows outside the recorded matrix: {extra[:3]}")


class Run(NamedTuple):
    rows: list[dict[str, str]]
    environment: dict[str, Any]
    run_dir: str
    version: int = 2


def _load_environment(run_dir: str) -> tuple[str, dict[str, Any]]:
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
    return env_path, environment


def check_run(run_dir: str) -> Completeness:
    """What a run directory still lacks, whatever its run_state: the runner's
    `--check`. The rows present must be well formed and name this run's
    environment; the only thing tolerated is that some are missing."""
    env_path, environment = _load_environment(run_dir)
    matrix = environment.get("matrix")
    if not isinstance(matrix, dict):
        raise ReportRefused(f"{env_path} records no matrix to check the CSV against")
    check_matrix(matrix)
    rows, version = read_rows(os.path.join(run_dir, "results.csv"), allow_empty=True)
    if rows:
        validate_rows(rows, version)
        expected_fp = fingerprint(environment)
        if rows[0]["env_fingerprint"] != expected_fp:
            raise ReportRefused(f"results.csv was written under environment {rows[0]['env_fingerprint']}, "
                                f"but environment.json fingerprints to {expected_fp}")
        check_identity(rows, environment, version)
    return completeness(rows, matrix)


def load_run(run_dir: str) -> Run:
    """The rows and environment of one complete run, or ReportRefused."""
    env_path, environment = _load_environment(run_dir)
    if environment.get("run_state") != "complete":
        raise ReportRefused(
            f"{env_path} says run_state={environment.get('run_state')!r}: the run did not finish"
        )
    matrix = environment.get("matrix")
    if not isinstance(matrix, dict):
        raise ReportRefused(f"{env_path} records no matrix to check the CSV against")
    check_matrix(matrix)
    rows, version = read_rows(os.path.join(run_dir, "results.csv"))
    validate_rows(rows, version)
    expected_fp = fingerprint(environment)
    if rows[0]["env_fingerprint"] != expected_fp:
        raise ReportRefused(
            f"results.csv was written under environment {rows[0]['env_fingerprint']}, "
            f"but environment.json fingerprints to {expected_fp}"
        )
    check_identity(rows, environment, version)
    check_complete(rows, matrix)
    return Run(rows, environment, run_dir, version)


def select(rows: Iterable[dict[str, str]], **criteria: Any) -> list[dict[str, str]]:
    """Rows whose columns equal the given values (compared as text)."""
    wanted = {key: str(value) for key, value in criteria.items()}
    return [row for row in rows if all(row[key] == value for key, value in wanted.items())]
