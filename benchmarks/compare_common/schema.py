"""The results CSV of every cross-product comparison, and how a run is judged (#744 section 4.2).

#595 rule 9 and #744 rules 11, 13 and 21 are enforced here: `load_run` either
returns a complete, single-environment set of rows that every chart may draw,
or raises `RunRefused` naming what is wrong. A cell that did not run is a ROW
saying so (status `failed` or `refused`, with the reason in `note`), never a
missing row, so a chart cannot quietly have a hole.

Every kind shares the run-constant columns (`run_id`, `env_fingerprint`,
`vtop_commit`, `host_class`), the key (`system`, `profile`, `cell`,
`repetition`, then the kind's variant columns) and the bookkeeping columns
(`system_version`, `image_digest`, `config_sha256`, `metric_class`, `status`,
`note`, `started_at`, `wall_seconds`); a `Kind` adds its own metric columns.
`metric_class` is per row, not per run: a run holds correctness cells
(crashes, faults) and performance cells (drain, live) side by side, and the
cell decides which a row is.

Refused, before anything is drawn: a missing or unfinished run; a header that
is not the kind's; a row from another run, commit, host class or environment
(the CSV's fingerprint must be the hash of `environment.json`'s `comparable`
block); a system or profile the kind does not run; a status other than ok,
failed or refused, or a non-ok row with no reason; a `performance` row from a
`hosted` host (#744 section 4.4: a shared runner's CPU is not a measurement
host); a product the registry does not let a chart draw; a `config_sha256`
that is not the hash of the configuration text the run recorded; a duplicate
key, a key the plan does not owe, or a key it owes that is missing; and
anything the kind's own `check_row` refuses.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
from collections.abc import Callable, Sequence
from typing import Any, NamedTuple

from compare_common import plan as plans
from compare_common import registry as reg

SCHEMA_VERSION = 1
STATUSES = ("ok", "failed", "refused")
METRIC_CLASSES = ("correctness", "performance")
RUN_CONSTANT = ("run_id", "env_fingerprint", "vtop_commit", "host_class")
BASE_KEY = ("system", "profile", "cell", "repetition")
BOOKKEEPING = ("system_version", "image_digest", "config_sha256", "metric_class", "status", "note", "started_at",
               "wall_seconds")


class RunRefused(Exception):
    """The run cannot be drawn or vouched for; the message says why."""


class Kind(NamedTuple):
    """What one comparison kind adds to the shared schema."""

    name: str  # "archive": environment.json's `kind`, and <name>_compare/
    family: str  # the registry family that colours its charts
    use_cases: tuple[str, ...]  # #744 section 1 letters a product must be registered for
    variant_columns: tuple[str, ...]  # key columns beyond the shared key
    metric_columns: tuple[str, ...]
    cells: dict[str, str]  # cell -> metric class
    profiles: dict[str, tuple[str, ...]]  # system -> its durability profiles
    # (system, profile, cell) -> a reason that combination is refused, or None.
    forbidden: Callable[[str, str, str], str | None] | None = None
    # One row -> its problems (kind-specific rules), or an empty list.
    check_row: Callable[[dict[str, str]], list[str]] | None = None
    rotate_systems: bool = False

    @property
    def key(self) -> tuple[str, ...]:
        return BASE_KEY + self.variant_columns

    @property
    def columns(self) -> tuple[str, ...]:
        return RUN_CONSTANT + self.key + BOOKKEEPING + self.metric_columns


def fingerprint(comparable: dict[str, Any]) -> str:
    """The hash that ties every row to one environment (rule 9)."""
    text = json.dumps(comparable, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


# ------------------------------------------------------------------- writing


def ensure_header(path: str, kind: Kind) -> None:
    """Create the CSV with the kind's header, or refuse one whose header is not the kind's."""
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, newline="", encoding="utf-8") as handle:
            header = next(csv.reader(handle), [])
        if tuple(header) != kind.columns:
            raise RunRefused(f"{path}: the header is not the {kind.name} kind's; a run is resumed only into "
                             "the layout it was started with")
        return
    with open(path, "w", newline="", encoding="utf-8") as handle:
        csv.writer(handle).writerow(kind.columns)


def append_row(path: str, kind: Kind, row: dict[str, Any]) -> None:
    """Append one row and make it durable before the next run starts (resume reads it back)."""
    unknown = sorted(set(row) - set(kind.columns))
    if unknown:
        raise RunRefused(f"row has columns the {kind.name} kind does not: {unknown}")
    ensure_header(path, kind)
    with open(path, "a", newline="", encoding="utf-8") as handle:
        csv.DictWriter(handle, fieldnames=kind.columns, extrasaction="raise").writerow(
            {column: "" if row.get(column) is None else row.get(column) for column in kind.columns})
        handle.flush()
        os.fsync(handle.fileno())


# ------------------------------------------------------------------- reading


def read_rows(path: str, kind: Kind) -> list[dict[str, str]]:
    if not os.path.isfile(path):
        raise RunRefused(f"{path} is missing")
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != kind.columns:
            raise RunRefused(f"{path}: the header is not the {kind.name} kind's columns")
        rows = []
        for number, row in enumerate(reader, 2):
            if None in row or any(value is None for value in row.values()):
                raise RunRefused(f"{path}:{number}: the row has the wrong number of fields (truncated?)")
            rows.append(row)
    return rows


def read_environment(run_dir: str) -> dict[str, Any]:
    path = os.path.join(run_dir, "environment.json")
    if not os.path.isfile(path):
        raise RunRefused(f"{path} is missing")
    try:
        with open(path, encoding="utf-8") as handle:
            env = json.load(handle)
    except ValueError as exc:
        raise RunRefused(f"{path} is not JSON: {exc}") from exc
    if env.get("schema_version") != SCHEMA_VERSION:
        raise RunRefused(f"{path}: schema_version {env.get('schema_version')!r}; this loader reads {SCHEMA_VERSION}")
    return env


def row_key(row: dict[str, str], kind: Kind) -> tuple[str, ...]:
    return tuple(row[column] for column in kind.key)


def check_rows(rows: Sequence[dict[str, str]], kind: Kind, env: dict[str, Any],
               registry: dict[str, Any] | None = None) -> list[str]:
    """Every problem with the rows that are present, judged against the run's environment."""
    registry = registry if registry is not None else reg.load()
    problems = []
    comparable = env.get("comparable") or {}
    expected_constants = {
        "run_id": env.get("run_id"),
        "env_fingerprint": fingerprint(comparable),
        "vtop_commit": comparable.get("vtop_commit"),
        "host_class": comparable.get("host_class"),
    }
    configs = env.get("configs") or {}
    for number, row in enumerate(rows, 2):
        where = f"row {number} ({'/'.join(row_key(row, kind))})"
        for column, want in expected_constants.items():
            if row[column] != str(want):
                problems.append(f"{where}: {column} is {row[column]!r}, the run's environment says {want!r}")
        system, profile, cell = row["system"], row["profile"], row["cell"]
        if system not in kind.profiles:
            problems.append(f"{where}: system {system!r} is not run by the {kind.name} kind")
            continue
        if profile not in kind.profiles[system]:
            problems.append(f"{where}: {system} has no profile {profile!r}")
        if cell not in kind.cells:
            problems.append(f"{where}: unknown cell {cell!r}")
            continue
        if row["metric_class"] != kind.cells[cell]:
            problems.append(f"{where}: metric_class {row['metric_class']!r}; cell {cell} is {kind.cells[cell]}")
        if row["status"] not in STATUSES:
            problems.append(f"{where}: status {row['status']!r} is not one of {STATUSES}")
        elif row["status"] != "ok" and not row["note"].strip():
            problems.append(f"{where}: a {row['status']} row must say why in `note`")
        if row["host_class"] == "hosted" and row["metric_class"] == "performance":
            problems.append(f"{where}: a performance row from a hosted runner; only correctness metrics are valid "
                            "there (#744 section 4.4)")
        try:
            reg.require_chartable(registry, system)
        except reg.RegistryError as exc:
            problems.append(f"{where}: {exc}")
        if kind.forbidden:
            reason = kind.forbidden(system, profile, cell)
            if reason:
                problems.append(f"{where}: {reason}")
        recorded = (configs.get(system) or {}).get(profile)
        if row["status"] != "refused" or row["config_sha256"]:
            if not recorded:
                problems.append(f"{where}: environment.json records no configuration for {system} {profile}")
            elif row["config_sha256"] != recorded.get("sha256") or sha256_text(recorded.get("text", "")) != \
                    recorded.get("sha256"):
                problems.append(f"{where}: config_sha256 {row['config_sha256'][:12]}... is not the hash of the "
                                f"configuration the run recorded for {system} {profile}")
        if kind.check_row:
            problems.extend(f"{where}: {problem}" for problem in kind.check_row(row))
    return problems


def check_completeness(rows: Sequence[dict[str, str]], kind: Kind, plan: dict[str, Any]) -> tuple[list[str], list[str]]:
    """(problems, missing keys): duplicates and rows outside the plan are problems; missing keys are listed."""
    expected = plans.expected_keys(plan, kind)
    expected_set = set(expected)
    seen: dict[tuple[str, ...], int] = {}
    problems = []
    for row in rows:
        key = row_key(row, kind)
        seen[key] = seen.get(key, 0) + 1
        if key not in expected_set:
            problems.append(f"row {'/'.join(key)} is not in the plan the run recorded")
    problems.extend(f"row {'/'.join(key)} appears {count} times" for key, count in sorted(seen.items()) if count > 1)
    missing = ["/".join(key) for key in expected if key not in seen]
    return problems, missing


def load_run(run_dir: str, kind: Kind, *, require_complete: bool = True,
             registry: dict[str, Any] | None = None) -> tuple[list[dict[str, str]], dict[str, Any]]:
    """The rows and environment of a finished, complete, single-environment run, or RunRefused."""
    env = read_environment(run_dir)
    if env.get("kind") != kind.name:
        raise RunRefused(f"{run_dir}: environment.json says kind {env.get('kind')!r}, not {kind.name!r}")
    if require_complete and env.get("run_state") != "complete":
        raise RunRefused(f"{run_dir}: run_state is {env.get('run_state')!r}; the run did not finish")
    if env.get("private"):
        raise RunRefused(f"{run_dir}: a --private run is never charted")
    rows = read_rows(os.path.join(run_dir, f"{kind.name}.csv"), kind)
    problems = check_rows(rows, kind, env, registry)
    plan = env.get("plan")
    if not isinstance(plan, dict):
        raise RunRefused(f"{run_dir}: environment.json records no plan")
    try:
        plans.check_plan(plan, kind)
    except plans.PlanRefused as exc:
        problems.append(f"the recorded plan: {exc}")
    else:
        extra, missing = check_completeness(rows, kind, plan)
        problems.extend(extra)
        if missing and require_complete:
            problems.append(f"{len(missing)} expected rows are missing: {', '.join(missing[:20])}"
                            + (" ..." if len(missing) > 20 else ""))
    if problems:
        raise RunRefused(f"{run_dir}: " + "; ".join(problems[:30]) + (" ..." if len(problems) > 30 else ""))
    return rows, env
