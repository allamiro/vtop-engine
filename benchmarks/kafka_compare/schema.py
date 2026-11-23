"""The results CSV: its columns, how a row is written, and how a run is judged complete.

#595 rule 9 is enforced here: `load_run` either returns a complete, single-
environment set of rows or raises `ReportRefused` naming what is wrong. The
report never draws from anything else, so a chart cannot quietly have a hole
where a row is missing - a cell that did not run is a ROW saying so (status
`failed` or `refused`), and the chart shows that gap by name.

There are four layouts. Schema 2 (`COLUMNS_V2`, #596) adds compression, the
load basis, the latency basis, the end-to-end mode, the measurement window and
`same_config_as`, and it moves `profile` from the run-constant set into the
row key. Schema 3 (`COLUMNS_V3`, #752) adds `group_commit`, VTOP's
`group_commit.enabled` for the row (`off` or `on`; blank for Kafka, which has
no such setting, and for a run that did not vary it), and makes it part of the
row key. Schema 4 (`COLUMNS`, #600) adds the resources a measured run's window
cost (`delivered_bytes`, `cpu_seconds`, `disk_write_bytes`, `disk_read_bytes`,
summed over the system's three nodes), a sidecar `e2e_hist.csv` holding each
end-to-end row's 1 ms latency histogram, and the matrix keys `resources` and
`e2e.extra_profiles`. Schemas 1 (`COLUMNS_V1`, phase 1's committed run), 2 and
3 still load. Their rows are read as schema 4: schema 1 with compression
`none`, load basis `kafka` on fixed-rate rows and latency basis `single`, which
is what that run did, and every older row with the columns it lacks blank, so a
committed run's report is unchanged byte for byte.
"""
from __future__ import annotations

import csv
import json
import math
import os
import re
from collections.abc import Iterable
from typing import Any, NamedTuple

from kafka_compare import e2e, plan

SYSTEMS = plan.SYSTEMS
STATUSES = ("ok", "failed", "refused")
MODES = ("saturation", "fixed_rate", "consume", "e2e")
COMPRESSIONS = plan.COMPRESSIONS
ACKS = plan.ACKS
PROFILES = plan.PROFILES
LOAD_BASES = plan.LOAD_BASES
GROUP_COMMITS = plan.GROUP_COMMITS

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

COLUMNS_V2 = (
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

# Schema 3: schema 2 plus `group_commit` after `profile` (#752). VTOP's
# `group_commit.enabled` for this row, `off` or `on`; blank on every Kafka row
# and on every row of a run that did not vary it.
COLUMNS_V3 = COLUMNS_V2[:COLUMNS_V2.index("profile") + 1] + ("group_commit",) \
    + COLUMNS_V2[COLUMNS_V2.index("profile") + 1:]

# Schema 4: schema 3 plus what a measured run's window cost, after the window it covers (#600). All four are
# blank on a row that was not measured with `matrix.resources`, and on every failed or refused row.
RESOURCE_COLUMNS = ("delivered_bytes", "cpu_seconds", "disk_write_bytes", "disk_read_bytes")
COLUMNS = COLUMNS_V3[:COLUMNS_V3.index("window_end_ms") + 1] + RESOURCE_COLUMNS \
    + COLUMNS_V3[COLUMNS_V3.index("window_end_ms") + 1:]
# The modes whose ok rows carry a resource window when the matrix asks for one.
RESOURCE_MODES = ("saturation", "fixed_rate", "consume", "e2e")

# The sidecar of the end-to-end mode: one line per (ok e2e row, 1 ms latency bucket) holding records. A row's
# key columns, then the bucket's lower edge in ms (60000 means 60 s or more) and the count.
E2E_HIST = "e2e_hist.csv"
E2E_HIST_KEY = ("system", "record_size", "partitions", "producers", "compression", "acks", "profile",
                "group_commit", "repetition", "load_basis", "load_pct")
E2E_HIST_COLUMNS = E2E_HIST_KEY + ("bucket_ms", "count")

# Columns that must be the same on every row of one run: a chart that mixed
# them would compare two environments as if they were one (rule 9). Schema 1
# also held `profile` constant; schema 2 makes it a key.
RUN_CONSTANT = ("run_id", "env_fingerprint", "vtop_commit", "kafka_image", "client")
RUN_CONSTANT_V1 = RUN_CONSTANT + ("profile",)

# The row key: completeness and duplicates are judged on it.
KEY = ("system", "mode", "record_size", "partitions", "producers", "compression", "acks", "profile",
       "group_commit", "repetition", "load_basis", "load_pct")

_LATENCY = ("lat_p50_ms", "lat_p95_ms", "lat_p99_ms", "lat_p999_ms")
_NUMERIC_OK = {
    "saturation": ("records", "records_per_sec", "mib_per_sec"),
    "fixed_rate": ("records", "records_per_sec", "mib_per_sec") + _LATENCY,
    "consume": ("records", "records_per_sec", "mib_per_sec"),
    "e2e": ("records", "records_produced") + _LATENCY,
}
# Modes whose ok rows carry percentiles, and so say how they were combined.
_HAS_LATENCY = ("saturation", "fixed_rate", "e2e")
# Measurement columns a repeated row (`same_config_as`) must carry
# unchanged from the row it repeats.
_MEASURED = ("status", "sustained", "records", "records_per_sec", "mib_per_sec", "lat_avg_ms", "lat_max_ms",
             "lat_p50_ms", "lat_p95_ms", "lat_p99_ms", "lat_p999_ms", "lat_basis", "started_at",
             "window_start_ms", "window_end_ms", "wall_seconds", "note") + RESOURCE_COLUMNS


def expected_lat_basis(producers: str) -> str:
    """`single` for one producer; the worst of N producers' percentiles for N."""
    return "single" if producers == "1" else f"worst-of-{producers}"


class ReportRefused(Exception):
    """The data cannot honestly be charted; the message says exactly why."""


# environment.json's layout. A run without the key is schema 1 (both runs
# committed before #600). Schema 2 adds `comparable.host_disk`: the disk
# behind Docker's volumes, which every image's footer names (#600). Schema 3
# adds `comparable.vtop_group_commit`, the group-commit settings the run
# measured and the configuration it wrote for each (#752). Schema 4 adds the
# matrix keys `resources` and `e2e.extra_profiles`, and with them the schema-4
# results.csv and the `e2e_hist.csv` sidecar (#600).
SCHEMA_VERSION = 4


def check_schema_version(environment: dict[str, Any], env_path: str) -> None:
    """Refuse a run this reader does not know, and a schema-2 run that did not record its disk."""
    if "schema_version" not in environment:
        version = 1
    else:
        version = environment["schema_version"]
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            raise ReportRefused(f"{env_path} has schema_version {version!r}, which is not a version number "
                                "(a positive whole number): the file is malformed")
    if version > SCHEMA_VERSION:
        raise ReportRefused(f"{env_path} has schema_version {version}; this report reads 1 to {SCHEMA_VERSION}, "
                            "so the run was written by a newer harness: update the report first")
    if version >= 2:
        comparable = environment.get("comparable") or {}
        disk = comparable.get("host_disk")
        if not isinstance(disk, str) or not disk.strip():
            raise ReportRefused(f"{env_path} is schema_version {version} but records no comparable.host_disk "
                                f"({disk!r}): the footer of every image must name the disk")
    matrix = environment.get("matrix")
    if version < 4 and isinstance(matrix, dict):
        e2e_config = matrix.get("e2e")
        asked = [key for key, present in (("resources", "resources" in matrix),
                                          ("e2e.extra_profiles", isinstance(e2e_config, dict)
                                           and "extra_profiles" in e2e_config)) if present]
        if asked:
            raise ReportRefused(f"{env_path} is schema_version {version} but its matrix has {', '.join(asked)}: "
                                "those are schema 4")
    check_group_commit_record(environment, env_path, version)


def check_group_commit_record(environment: dict[str, Any], env_path: str, version: int) -> None:
    """The group-commit settings a run measured are recorded, and agree with its matrix (#752).

    `comparable.vtop_group_commit.settings` maps each setting the matrix asks for (`matrix.group_commits`)
    to what the harness wrote into VTOP's leader config for it, so the setting is part of the environment
    the run's fingerprint covers. A failover run (the HA cluster, no such setting) is exempt.
    """
    matrix = environment.get("matrix")
    asked = matrix.get("group_commits") if isinstance(matrix, dict) else None
    if asked is not None and version < 3:
        raise ReportRefused(f"{env_path} is schema_version {version} but its matrix has group_commits: "
                            "that axis is schema 3")
    if version < 3 or environment.get("kind") == "failover":
        return
    comparable = environment.get("comparable") or {}
    recorded = comparable.get("vtop_group_commit")
    settings = recorded.get("settings") if isinstance(recorded, dict) else None
    if not isinstance(settings, dict):
        raise ReportRefused(f"{env_path} is schema_version {version} but records no "
                            f"comparable.vtop_group_commit.settings ({recorded!r}): the run's group-commit "
                            "settings must be part of its environment")
    if list(settings) != list(asked or []):
        raise ReportRefused(f"{env_path} records group-commit settings {list(settings)} but its matrix's "
                            f"group_commits is {asked!r}")
    for name, written in settings.items():
        if not isinstance(written, dict) or written.get("enabled") is not (name == "on"):
            raise ReportRefused(f"{env_path}: the recorded `{name}` setting is {written!r}, which does not "
                                f"say `enabled: {'true' if name == 'on' else 'false'}`")


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


def _upgrade_v3(row: dict[str, str]) -> dict[str, str]:
    """A schema-3 row read as schema 4: it recorded no resources, so the four columns are blank."""
    if None in row or any(value is None for value in row.values()):
        return row
    return {**row, **{column: "" for column in RESOURCE_COLUMNS}}


def _upgrade_v2(row: dict[str, str]) -> dict[str, str]:
    """A schema-2 row read as schema 4: it did not vary VTOP's group commit, so the setting is blank, and it
    recorded no resources."""
    if None in row or any(value is None for value in row.values()):
        return row
    return _upgrade_v3({**row, "group_commit": ""})


def _upgrade_v1(row: dict[str, str]) -> dict[str, str]:
    """A schema-1 row read as schema 4: what phase 1 did, said in the new
    columns. A short row is left as it is, so `validate_rows` refuses it by
    line."""
    if None in row or any(value is None for value in row.values()):
        return row
    out = dict(row)
    out["compression"] = "none"
    out.update(_upgrade_v2(out))
    out["same_config_as"] = ""
    out["load_basis"] = "kafka" if row["mode"] == "fixed_rate" else ""
    out["lat_basis"] = "single" if row["status"] == "ok" and row["mode"] in _HAS_LATENCY else ""
    for column in ("records_produced", "window_start_ms", "window_end_ms"):
        out[column] = ""
    return out


def read_rows(csv_path: str, *, allow_empty: bool = False) -> tuple[list[dict[str, str]], int]:
    """The rows, as schema 4, and the schema the file was written in."""
    if not os.path.isfile(csv_path):
        raise ReportRefused(f"no results CSV at {csv_path}")
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        header = tuple(reader.fieldnames or ())
        if header == COLUMNS:
            version = 4
        elif header == COLUMNS_V3:
            version = 3
        elif header == COLUMNS_V2:
            version = 2
        elif header == COLUMNS_V1:
            version = 1
        else:
            raise ReportRefused(
                f"{csv_path} has columns {reader.fieldnames}, expected schema 4 {list(COLUMNS)}, "
                f"schema 3 {list(COLUMNS_V3)}, schema 2 {list(COLUMNS_V2)} or schema 1 {list(COLUMNS_V1)}"
            )
        rows = list(reader)
    if not rows and not allow_empty:
        raise ReportRefused(f"{csv_path} has a header and no rows")
    if version == 1:
        rows = [_upgrade_v1(row) for row in rows]
    elif version == 2:
        rows = [_upgrade_v2(row) for row in rows]
    elif version == 3:
        rows = [_upgrade_v3(row) for row in rows]
    return rows, version


def _whole_or_blank(row: dict[str, str], column: str, line: int) -> int | None:
    text = row[column]
    if text == "":
        return None
    if not (text == "0" or _CANONICAL_INT.match(text)):
        raise ReportRefused(f"results.csv line {line}: {column} is {text!r}, not a whole number")
    return int(text)


def _validate_v2_columns(row: dict[str, str], line: int) -> None:
    """The schema-2 and -3 columns of one row (an older row has them filled in)."""
    if row["compression"] not in COMPRESSIONS:
        raise ReportRefused(f"results.csv line {line}: unknown compression {row['compression']!r}")
    if row["acks"] not in ACKS:
        raise ReportRefused(f"results.csv line {line}: acks is {row['acks']!r}, not one of {list(ACKS)}")
    if row["profile"] not in PROFILES:
        raise ReportRefused(f"results.csv line {line}: unknown profile {row['profile']!r}")
    if row["group_commit"] not in ("",) + GROUP_COMMITS:
        raise ReportRefused(f"results.csv line {line}: group_commit is {row['group_commit']!r}, not blank or one "
                            f"of {list(GROUP_COMMITS)}")
    if row["group_commit"] and row["system"] != "vtop":
        raise ReportRefused(f"results.csv line {line}: a {row['system']} row has group_commit "
                            f"{row['group_commit']!r}; the setting is VTOP's, and {row['system']} has no "
                            "counterpart")
    same = row["same_config_as"]
    if same:
        if same not in PROFILES or same == row["profile"]:
            raise ReportRefused(f"results.csv line {line}: same_config_as is {same!r} on a {row['profile']!r} "
                                "row; it must name the other profile the row was measured under")
        if row["system"] != "vtop":
            raise ReportRefused(f"results.csv line {line}: a {row['system']} row repeats another profile's "
                                "measurement; only VTOP, whose configuration the profiles do not change, may")
        if row["mode"] not in ("saturation", "consume"):
            # A rated row's offered rate is a share of THAT profile's
            # saturation, so it cannot be another profile's measurement.
            raise ReportRefused(f"results.csv line {line}: a {row['mode']} row repeats another profile's "
                                "measurement; only saturation and consume rows may, rated rows are run per profile")
    basis = row["load_basis"]
    if row["mode"] in ("fixed_rate", "e2e") and basis not in LOAD_BASES:
        raise ReportRefused(f"results.csv line {line}: a {row['mode']} row has load_basis {basis!r}")
    if row["mode"] in ("saturation", "consume") and basis:
        raise ReportRefused(f"results.csv line {line}: a {row['mode']} row has load_basis {basis!r}")
    if row["mode"] != "e2e" and row["records_produced"]:
        raise ReportRefused(f"results.csv line {line}: records_produced is for e2e rows only")
    if row["mode"] not in _HAS_LATENCY and row["lat_basis"]:
        raise ReportRefused(f"results.csv line {line}: a {row['mode']} row has lat_basis {row['lat_basis']!r}")
    present = [column for column in RESOURCE_COLUMNS if row[column] != ""]
    if present:
        if len(present) != len(RESOURCE_COLUMNS):
            missing = [c for c in RESOURCE_COLUMNS if row[c] == ""]
            raise ReportRefused(f"results.csv line {line}: {', '.join(present)} given but {', '.join(missing)} "
                                "blank: a row's resources are all there or all absent")
        _key_int(row, "delivered_bytes", line)
        _number(row, "cpu_seconds", line)
        for column in ("disk_write_bytes", "disk_read_bytes"):
            _whole_or_blank(row, column, line)
    start = _whole_or_blank(row, "window_start_ms", line)
    end = _whole_or_blank(row, "window_end_ms", line)
    if (start is None) != (end is None) or (start is not None and end < start):
        raise ReportRefused(f"results.csv line {line}: the window {row['window_start_ms']!r}.."
                            f"{row['window_end_ms']!r} is not a start and an end at or after it")


def validate_rows(rows: list[dict[str, str]], version: int = 4) -> None:
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
            if row["mode"] in _HAS_LATENCY and row["lat_basis"] != expected_lat_basis(row["producers"]):
                raise ReportRefused(f"results.csv line {line}: lat_basis is {row['lat_basis']!r} with "
                                    f"{row['producers']} producer(s); it must be "
                                    f"{expected_lat_basis(row['producers'])!r}")
            if row["mode"] == "e2e" and row["records"] != row["records_produced"]:
                # Fewer consumed than produced is a gap the CDF would hide: the
                # run writes such a row as failed, saying how many are missing.
                raise ReportRefused(f"results.csv line {line}: an ok e2e row consumed {row['records']} of "
                                    f"{row['records_produced']} records produced")
        elif not row["note"].strip():
            raise ReportRefused(
                f"results.csv line {line}: a {row['status']} row must say why in `note`"
            )
    check_repeats(rows)
    for column in (RUN_CONSTANT_V1 if version == 1 else RUN_CONSTANT):
        values = sorted({row[column] for row in rows})
        if len(values) != 1:
            raise ReportRefused(
                f"results.csv mixes {len(values)} values of {column} ({', '.join(values)}): "
                "one chart must come from one environment and one commit"
            )


def check_repeats(rows: list[dict[str, str]]) -> None:
    """Every row that repeats another profile's measurement (`same_config_as`)
    has that measured row, itself measured rather than repeated, and carries
    its measurement unchanged (review)."""
    by_key = {}
    for row in rows:
        by_key.setdefault(_text_key(row), row)
    for line, row in enumerate(rows, start=2):
        if not row["same_config_as"]:
            continue
        source = by_key.get(_text_key({**row, "profile": row["same_config_as"]}))
        if source is None or source["same_config_as"]:
            raise ReportRefused(f"results.csv line {line}: it repeats a {row['same_config_as']} measurement "
                                "that is not in the run as a measured row")
        changed = [c for c in _MEASURED if row[c] != source[c]]
        if changed:
            raise ReportRefused(f"results.csv line {line}: it repeats a {row['same_config_as']} measurement but "
                                f"differs from it in {', '.join(changed)}")


def _text_key(row: dict[str, str]) -> tuple:
    return tuple(row[column] for column in KEY)


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
        row["group_commit"],
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


def check_identity(rows: list[dict[str, str]], environment: dict[str, Any], version: int = 4) -> None:
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
    version: int = 4
    # {row_key of an ok e2e row: {bucket lower edge in ms: records}} (schema 4; empty before it).
    hist: dict[tuple, dict[int, int]] = {}


def _load_environment(run_dir: str) -> tuple[str, dict[str, Any]]:
    env_path = os.path.join(run_dir, "environment.json")
    if not os.path.isfile(env_path):
        raise ReportRefused(f"no environment.json in {run_dir}")
    with open(env_path, encoding="utf-8") as handle:
        try:
            environment = json.load(handle)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ReportRefused(f"{env_path} is not JSON: {exc}") from exc
    if not isinstance(environment, dict):
        raise ReportRefused(f"{env_path} holds a JSON {type(environment).__name__}, not an object")
    check_schema_version(environment, env_path)
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


def check_versions(environment: dict[str, Any], csv_version: int) -> None:
    """environment.json and results.csv are written together: from schema 3 on the two carry the same
    number, so a schema-4 CSV under an older environment, or the reverse, is two harnesses' files in one
    directory."""
    version = environment.get("schema_version", 1)
    if (csv_version >= 3 or version >= 3) and csv_version != version:
        raise ReportRefused(f"results.csv has the schema-{csv_version} columns but environment.json has "
                            f"schema_version {version}: they were not written together")


def check_resources(rows: list[dict[str, str]], matrix: dict[str, Any]) -> None:
    """Every ok measured row carries its resources and window when the matrix asks for them, and no row does
    when it does not (a figure the run did not plan would have no method behind it)."""
    asked = bool(matrix.get("resources"))
    for line, row in enumerate(rows, start=2):
        present = [column for column in RESOURCE_COLUMNS if row[column] != ""]
        owed = asked and row["status"] == "ok" and row["mode"] in RESOURCE_MODES
        if owed:
            for column in (*RESOURCE_COLUMNS, "window_start_ms", "window_end_ms"):
                if row[column] == "":
                    what = "its window" if column.startswith("window") else column
                    raise ReportRefused(f"results.csv line {line}: an ok {row['mode']} row of a run that records "
                                        f"resources has no {what} ({column} is blank)")
        elif present:
            why = ("the run's matrix does not ask for resources" if not asked
                   else f"a {row['status']} {row['mode']} row has none to carry")
            raise ReportRefused(f"results.csv line {line}: {', '.join(present)} given, but {why}")


def _hist_int(row: dict[str, str], column: str, line: int, floor: int, ceiling: int | None = None) -> int:
    text = row[column]
    ok = text == "0" or bool(_CANONICAL_INT.match(text))
    value = int(text) if ok else -1
    if not ok or value < floor or (ceiling is not None and value > ceiling):
        bound = f" from {floor} to {ceiling}" if ceiling is not None else f" of at least {floor}"
        raise ReportRefused(f"{E2E_HIST} line {line}: {column} is {text!r}, not a whole number{bound}")
    return value


def load_hist(run_dir: str, rows: list[dict[str, str]], matrix: dict[str, Any],
              version: int) -> dict[tuple, dict[int, int]]:
    """Each ok end-to-end row's latency histogram, held to its row: the file exists when the matrix has the
    end-to-end mode, every line belongs to an ok e2e row, the counts sum to that row's `records`, and the
    row's percentiles are the histogram's (so a table and a CDF cannot disagree)."""
    path = os.path.join(run_dir, E2E_HIST)
    if version < 4:
        return {}
    ok_e2e = {row_key(row): (line, row) for line, row in enumerate(rows, start=2)
              if row["mode"] == "e2e" and row["status"] == "ok"}
    if not matrix.get("e2e"):
        if os.path.exists(path):
            raise ReportRefused(f"{path} exists, but the matrix has no end-to-end mode to hold it")
        return {}
    if not os.path.isfile(path):
        raise ReportRefused(f"no {E2E_HIST} in {run_dir}: the run's matrix has the end-to-end mode, whose "
                            "histograms it holds")
    hist: dict[tuple, dict[int, int]] = {}
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != E2E_HIST_COLUMNS:
            raise ReportRefused(f"{path} has columns {reader.fieldnames}, expected {list(E2E_HIST_COLUMNS)}")
        for line, entry in enumerate(reader, start=2):
            if None in entry or any(value is None for value in entry.values()):
                raise ReportRefused(f"{E2E_HIST} line {line} has the wrong number of fields")
            for column in ("record_size", "partitions", "producers", "repetition"):
                _key_int(entry, column, line)
            bucket = _hist_int(entry, "bucket_ms", line, 0, e2e.OVERFLOW_MS)
            count = _hist_int(entry, "count", line, 1)
            key = (entry["system"], "e2e", int(entry["record_size"]), int(entry["partitions"]),
                   int(entry["producers"]), entry["compression"], entry["acks"], entry["profile"],
                   entry["group_commit"], int(entry["repetition"]), entry["load_basis"], entry["load_pct"])
            if key not in ok_e2e:
                raise ReportRefused(f"{E2E_HIST} line {line} is a histogram for {key[:1] + key[2:]}, which is "
                                    "no ok e2e row of results.csv")
            buckets = hist.setdefault(key, {})
            if bucket in buckets:
                raise ReportRefused(f"{E2E_HIST} line {line} gives bucket {bucket} ms twice for {key[:1] + key[2:]}")
            buckets[bucket] = count
    for key, (line, row) in ok_e2e.items():
        counts = hist.get(key, {})
        if sum(counts.values()) != int(row["records"]):
            raise ReportRefused(f"the histogram of results.csv line {line} sums to {sum(counts.values())} records, "
                                f"but its row consumed {row['records']}")
        for column, (num, den) in e2e.PERCENTILES.items():
            want = e2e.percentile_ms(counts, num, den)
            if float(row[column]) != want:
                raise ReportRefused(f"results.csv line {line}: {column} is {row[column]!r}, but its histogram "
                                    f"in {E2E_HIST} says {want!r}")
    return hist


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
    check_versions(environment, version)
    validate_rows(rows, version)
    expected_fp = fingerprint(environment)
    if rows[0]["env_fingerprint"] != expected_fp:
        raise ReportRefused(
            f"results.csv was written under environment {rows[0]['env_fingerprint']}, "
            f"but environment.json fingerprints to {expected_fp}"
        )
    check_identity(rows, environment, version)
    check_complete(rows, matrix)
    check_resources(rows, matrix)
    return Run(rows, environment, run_dir, version, load_hist(run_dir, rows, matrix, version))


def select(rows: Iterable[dict[str, str]], **criteria: Any) -> list[dict[str, str]]:
    """Rows whose columns equal the given values (compared as text)."""
    wanted = {key: str(value) for key, value in criteria.items()}
    return [row for row in rows if all(row[key] == value for key, value in wanted.items())]
