"""The crash CSV (#599): columns, writing, the matrix a run owes, and the refusals the report applies.

One file per run, `results/<run-id>/crash.csv`: one row per (configuration, crash
mode, crash number), what that crash did. The report draws from `load_crash_run`
only, which returns a complete, single-environment run or raises
`schema.ReportRefused` (#595 rule 9). A crash that could not be measured is a ROW
saying why (`refused` or `failed`), never a gap.

`environment.json` is the harness's usual one (schema 2, `kind: "crash"`), with the
matrix in `matrix`; no schema bump is needed, since a crash run is a new kind with
its own file and the version's rule (`comparable.host_disk` recorded) is unchanged.
"""
from __future__ import annotations

import json
import math
import os
import sys
from typing import Any, NamedTuple

from kafka_compare import crash_analysis as ca
from kafka_compare import failover_schema as fschema
from kafka_compare import schema

CONFIGS = tuple(ca.CONFIGS)
CRASH_MODES = ca.CRASH_MODES
STATUSES = ("ok", "failed", "refused")

COLUMNS = (
    "run_id",
    "env_fingerprint",
    "vtop_commit",
    "kafka_image",
    "system",
    "config",
    "profile",
    "crash_mode",
    "crash",
    "status",
    "seed",
    "trigger_after_s",
    "trigger_method",
    "offered_rate",
    "client",
    "idempotent",
    "send_errors",
    "acked",
    "acked_last_seq",
    "acked_in_last_10s",
    "read_records",
    "read_missing_before_sentinel",
    "acked_lost",
    "lost_first_seq",
    "lost_last_seq",
    "lost_runs",
    "lost_window_s",
    "present_unacked",
    "duplicates",
    "reordered",
    "offset_mismatch",
    "unexpected",
    "canary_synced_ok",
    "canary_unsynced_lost",
    "sentinel_needed",
    "kill_to_drop_ms",
    "recovery_seconds",
    "needed_manual_repair",
    "started_at",
    "wall_seconds",
    "note",
)

RUN_CONSTANT = ("run_id", "env_fingerprint", "vtop_commit", "kafka_image")

# An ok row carries every one of these as a number (kill_to_drop_ms only in a power-loss mode).
_NUMERIC_OK = ("seed", "trigger_after_s", "offered_rate", "send_errors", "acked", "read_records",
               "read_missing_before_sentinel", "acked_lost", "lost_runs", "present_unacked", "duplicates",
               "reordered", "offset_mismatch", "unexpected", "recovery_seconds", "wall_seconds")


class PlanRefused(Exception):
    """The matrix cannot be run as asked; raised before any Docker work."""


def write_header(path: str, columns: tuple[str, ...] = COLUMNS) -> None:
    fschema.write_header(path, columns)


def append_rows(path: str, rows: list[dict[str, Any]], columns: tuple[str, ...] = COLUMNS) -> None:
    """Append rows, fsynced, so an interrupted run keeps what it measured."""
    fschema.append_rows(path, rows, columns)


# ------------------------------------------------------------------------------ matrix


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def check_matrix(matrix: dict[str, Any]) -> None:
    configs = matrix.get("configs")
    if not isinstance(configs, list) or not configs or len(set(configs)) != len(configs):
        raise PlanRefused("matrix.configs must be a non-empty list without repeats")
    for config in configs:
        if config not in CONFIGS:
            raise PlanRefused(f"unknown configuration {config!r}; the choices are {', '.join(CONFIGS)}")
    modes = matrix.get("crash_modes")
    if not isinstance(modes, list) or not modes or len(set(modes)) != len(modes):
        raise PlanRefused("matrix.crash_modes must be a non-empty list without repeats")
    for mode in modes:
        if mode not in CRASH_MODES:
            raise PlanRefused(f"unknown crash mode {mode!r}; the choices are {', '.join(CRASH_MODES)}")
    for key, floor in (("crashes", 1), ("rate", 1)):
        value = matrix.get(key)
        if not _is_int(value) or value < floor:
            raise PlanRefused(f"matrix.{key} is {value!r}: a whole number of at least {floor} is needed")
    if not _is_int(matrix.get("seed")):
        raise PlanRefused(f"matrix.seed is {matrix.get('seed')!r}: the schedule's seed must be a whole number")
    window = matrix.get("trigger_window_s")
    if (not isinstance(window, list) or len(window) != 2
            or not all(isinstance(v, (int, float)) and math.isfinite(v) for v in window)
            or not 0 < window[0] <= window[1]):
        raise PlanRefused(f"matrix.trigger_window_s is {window!r}: two ordered, positive seconds are needed")


def expected_keys(matrix: dict[str, Any]) -> set[tuple[str, str, int]]:
    return {(config, mode, crash) for config in matrix["configs"] for mode in matrix["crash_modes"]
            for crash in range(1, int(matrix["crashes"]) + 1)}


# Seconds per step of one crash, measured on the smoke run (Docker Desktop, 18 CPUs) and rounded up.
# `bring_up` is the fresh cluster (VTOP's is the shorter), `restart` the cluster back after the crash,
# `readback` two reads of the partition and the sentinel, `restore` the filesystem's drop and remount.
DEFAULT_TIMING = {
    "bring_up": {"kafka": 40.0, "vtop": 25.0},
    "warm_up": 5.0,
    "restore": {"sigkill": 8.0, "flakey-drop": 20.0},
    "restart": {"kafka": 45.0, "vtop": 30.0},
    "readback": 40.0,
    "teardown": 12.0,
}


def projected_seconds(matrix: dict[str, Any], timing: dict[str, Any] | None = None) -> float:
    """Wall time of the whole matrix: every crash brings a fresh cluster up, produces until its
    trigger (the window's mean), crashes it, restarts it and reads it back."""
    t = {**DEFAULT_TIMING, **(timing or {})}
    mean_trigger = sum(matrix["trigger_window_s"]) / 2
    total = 0.0
    for config in matrix["configs"]:
        system = ca.CONFIGS[config][0]
        for mode in matrix["crash_modes"]:
            one = (t["bring_up"][system] + t["warm_up"] + mean_trigger + t["restore"][mode] + t["restart"][system]
                   + t["readback"] + t["teardown"])
            total += one * int(matrix["crashes"])
    return total


def check_budget(matrix: dict[str, Any], max_hours: float, timing: dict[str, Any] | None = None) -> float:
    """The projected hours, or PlanRefused when they exceed `max_hours`."""
    hours = projected_seconds(matrix, timing) / 3600
    if hours > max_hours:
        raise PlanRefused(f"the matrix is projected to take {hours:.1f} h, over --max-hours {max_hours:g}; "
                          "narrow the matrix or raise the limit")
    return hours


# ------------------------------------------------------------------------------- rows


def _number(row: dict[str, str], column: str, line: int) -> float:
    text = row.get(column, "")
    try:
        value = float(text)
    except ValueError as exc:
        raise schema.ReportRefused(f"crash.csv line {line}: {column} is {text!r}, not a number") from exc
    if math.isnan(value) or math.isinf(value) or value < 0:
        raise schema.ReportRefused(f"crash.csv line {line}: {column} is {text!r}")
    return value


def _bool(row: dict[str, str], column: str, line: int) -> bool:
    text = row.get(column, "")
    if text not in ("true", "false"):
        raise schema.ReportRefused(f"crash.csv line {line}: {column} is {text!r}, expected true or false")
    return text == "true"


def validate_rows(rows: list[dict[str, str]]) -> None:
    for line, row in enumerate(rows, start=2):
        config = row["config"]
        if config not in CONFIGS:
            raise schema.ReportRefused(f"crash.csv line {line}: unknown config {config!r}")
        system, profile = ca.CONFIGS[config]
        if row["system"] != system or row["profile"] != profile:
            raise schema.ReportRefused(f"crash.csv line {line}: {config} is {system}/{profile}, "
                                       f"the row says {row['system']}/{row['profile']}")
        if row["crash_mode"] not in CRASH_MODES:
            raise schema.ReportRefused(f"crash.csv line {line}: unknown crash_mode {row['crash_mode']!r}")
        if row["status"] not in STATUSES:
            raise schema.ReportRefused(f"crash.csv line {line}: unknown status {row['status']!r}")
        _number(row, "crash", line)
        if row["status"] != "ok":
            if not row["note"].strip():
                raise schema.ReportRefused(f"crash.csv line {line}: a {row['status']} row must say why in `note`")
            continue
        for column in _NUMERIC_OK:
            _number(row, column, line)
        synced_ok = _bool(row, "canary_synced_ok", line)
        unsynced_lost = _bool(row, "canary_unsynced_lost", line)
        for column in ("idempotent", "sentinel_needed", "needed_manual_repair"):
            _bool(row, column, line)
        # The canary is the method's own check: an ok row whose canary did not behave is a row that
        # should have been `refused`, and counting it would put a number in a chart that means nothing.
        survived_whole = not unsynced_lost
        behaved = synced_ok and (survived_whole if row["crash_mode"] == "sigkill" else unsynced_lost)
        if not behaved:
            raise schema.ReportRefused(
                f"crash.csv line {line}: the row is ok but its canary did not behave as a {row['crash_mode']} "
                f"crash requires (synced ok {synced_ok}, unsynced lost {unsynced_lost}); such a crash is "
                "`refused`, never ok")
        if row["crash_mode"] == "flakey-drop":
            _number(row, "kill_to_drop_ms", line)
        lost = int(float(row["acked_lost"]))
        if lost > int(float(row["acked"])):
            raise schema.ReportRefused(f"crash.csv line {line}: more acknowledged records lost ({lost}) "
                                       f"than were acknowledged ({row['acked']})")
        located = [row["lost_first_seq"] != "", row["lost_last_seq"] != ""]
        if lost and not all(located):
            raise schema.ReportRefused(f"crash.csv line {line}: {lost} records lost but lost_first_seq or "
                                       "lost_last_seq is empty")
        if not lost and any(located):
            raise schema.ReportRefused(f"crash.csv line {line}: nothing lost but lost_first_seq or "
                                       "lost_last_seq is set")
    for column in RUN_CONSTANT:
        values = sorted({row[column] for row in rows})
        if len(values) != 1:
            raise schema.ReportRefused(
                f"crash.csv mixes {len(values)} values of {column} ({', '.join(values)}): "
                "one chart must come from one environment and one commit"
            )


class Completeness(NamedTuple):
    expected: set[tuple[str, str, int]]
    missing: list[tuple[str, str, int]]
    duplicates: list[tuple[str, str, int]]
    extra: list[tuple[str, str, int]]


def completeness(rows: list[dict[str, str]], matrix: dict[str, Any]) -> Completeness:
    seen: dict[tuple[str, str, int], int] = {}
    for row in rows:
        key = (row["config"], row["crash_mode"], int(float(row["crash"])))
        seen[key] = seen.get(key, 0) + 1
    expected = expected_keys(matrix)
    return Completeness(expected, sorted(expected - set(seen)), sorted(k for k, n in seen.items() if n > 1),
                        sorted(set(seen) - expected))


def check_complete(rows: list[dict[str, str]], matrix: dict[str, Any]) -> None:
    found = completeness(rows, matrix)
    if found.duplicates:
        raise schema.ReportRefused(f"crash.csv has duplicate rows for {found.duplicates[:3]}")
    if found.missing:
        by_cell: dict[tuple[str, str], int] = {}
        for config, mode, _ in found.missing:
            by_cell[(config, mode)] = by_cell.get((config, mode), 0) + 1
        crashes = int(matrix["crashes"])
        cells = ", ".join(f"{config}/{mode} has {crashes - n} of {crashes}" for (config, mode), n in
                          sorted(by_cell.items()))
        raise schema.ReportRefused(f"crash.csv is partial: {len(found.missing)} of {len(found.expected)} expected "
                                   f"rows are missing ({cells}); first {found.missing[:3]}")
    if found.extra:
        raise schema.ReportRefused(f"crash.csv has rows outside the recorded matrix: {found.extra[:3]}")


def check_enough_ok(rows: list[dict[str, str]], min_ok: int) -> None:
    """Refuse a configuration with fewer `ok` crashes than the reader needs, by name."""
    if min_ok <= 1:
        return
    count: dict[tuple[str, str], int] = {}
    for row in rows:
        key = (row["config"], row["crash_mode"])
        count[key] = count.get(key, 0) + (1 if row["status"] == "ok" else 0)
    for (config, mode), ok in sorted(count.items()):
        if ok < min_ok:
            raise schema.ReportRefused(
                f"{config} in mode {mode} has {ok} ok crashes, and this reader wants at least {min_ok}: "
                "a '0 lost' from fewer crashes says too little (the 95 % upper bound on the chance a crash "
                "loses something is 13.9 % at 20 crashes and 9.5 % at 30)")


# ------------------------------------------------------------------------------ loading


def read_rows(path: str, allow_empty: bool = False) -> list[dict[str, str]]:
    return fschema._read(path, COLUMNS, allow_empty=allow_empty)


def _load_environment(run_dir: str) -> tuple[str, dict[str, Any]]:
    env_path = os.path.join(run_dir, "environment.json")
    if not os.path.isfile(env_path):
        raise schema.ReportRefused(f"no environment.json in {run_dir}")
    with open(env_path, encoding="utf-8") as handle:
        try:
            environment = json.load(handle)
        except json.JSONDecodeError as exc:
            raise schema.ReportRefused(f"{env_path} is not JSON: {exc}") from exc
    if environment.get("kind") != "crash":
        raise schema.ReportRefused(f"{env_path} is not a crash run (kind={environment.get('kind')!r})")
    schema.check_schema_version(environment, env_path)
    matrix = environment.get("matrix")
    if not isinstance(matrix, dict):
        raise schema.ReportRefused(f"{env_path} records no matrix to check the CSV against")
    try:
        check_matrix(matrix)
    except PlanRefused as exc:
        raise schema.ReportRefused(f"{env_path}: {exc}") from exc
    return env_path, environment


class CrashRun(NamedTuple):
    rows: list[dict[str, str]]
    environment: dict[str, Any]
    run_dir: str


def load_crash_run(run_dir: str, min_ok: int = 1) -> CrashRun:
    env_path, environment = _load_environment(run_dir)
    if environment.get("run_state") != "complete":
        raise schema.ReportRefused(
            f"{env_path} says run_state={environment.get('run_state')!r}: the run did not finish")
    rows = read_rows(os.path.join(run_dir, "crash.csv"))
    validate_rows(rows)
    fingerprint = schema.fingerprint(environment)
    if rows[0]["env_fingerprint"] != fingerprint:
        raise schema.ReportRefused(
            f"crash.csv was written under environment {rows[0]['env_fingerprint']}, "
            f"but environment.json fingerprints to {fingerprint}")
    check_complete(rows, environment["matrix"])
    check_enough_ok(rows, min_ok)
    return CrashRun(rows, environment, run_dir)


def check_run(run_dir: str) -> Completeness:
    """What a run directory still owes, whatever its state."""
    _, environment = _load_environment(run_dir)
    rows = read_rows(os.path.join(run_dir, "crash.csv"), allow_empty=True)
    return completeness(rows, environment["matrix"])


def check_main(run_dir: str) -> int:
    """`--check`: 0 when every expected row is there once and nothing else is and the run finished;
    2 otherwise, listing the missing keys."""
    try:
        found = check_run(run_dir)
        with open(os.path.join(run_dir, "environment.json"), encoding="utf-8") as handle:
            state = json.load(handle).get("run_state")
    except schema.ReportRefused as exc:
        print(f"check refused: {exc}", file=sys.stderr)
        return 2
    if found.duplicates or found.extra:
        print(f"{len(found.duplicates)} duplicated and {len(found.extra)} unexpected row keys, "
              f"first {(found.duplicates + found.extra)[:3]}", file=sys.stderr)
        return 2
    present = len(found.expected) - len(found.missing)
    print(f"{run_dir}: {present} of {len(found.expected)} expected rows present; run_state {state}")
    if found.missing:
        print(f"missing {len(found.missing)} (key config, crash_mode, crash):", file=sys.stderr)
        for config, mode, crash in found.missing:
            print(f"  {config} {mode} {crash}", file=sys.stderr)
        return 2
    if state != "complete":
        print(f"every row is present but run_state is {state!r}; the report reads only a complete run",
              file=sys.stderr)
        return 2
    return 0
