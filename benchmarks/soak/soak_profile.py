"""Soak profiles and the fault plan they imply (#726).

A profile is a JSON file under profiles/. It holds what the run does (rate,
duration, fault cadence), what the lab is sized to (partitions, retention,
batching), and what the evaluator holds the run to (`criteria`). The driver
and the evaluator both read the same file, and both derive the fault plan
from it with `build_plan`, so what the evaluator expects to have happened is
never a separate list that can drift from what the driver was asked to do.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

PROFILE_DIR = Path(__file__).resolve().parent / "profiles"

# The faults the driver can inject, in the order a cycle visits them by
# default. A profile may use a subset or reorder them, but a fault the
# injectors do not implement is refused when the profile is loaded.
FAULT_KINDS = ("kill9", "broker_restart", "partition", "store_outage", "clock_step", "enospc")

# Trend series the evaluator gates. `fault_windows` says whether samples taken
# while a fault is active (and until the engine has recovered from it) are
# left out: a store outage legitimately fills the work directory and lengthens
# the archive lag, so those two series are judged on the quiet samples, while
# memory, open files and ledger size are judged on every sample, a leak during
# a fault being a leak.
TREND_SERIES = {
    "rss_bytes": {"label": "engine resident memory", "unit": "bytes", "fault_windows": "include"},
    "open_fds": {"label": "engine open files", "unit": "files", "fault_windows": "include"},
    "ledger_bytes": {"label": "ledger size (state database and its log)", "unit": "bytes", "fault_windows": "include"},
    "work_dir_bytes": {"label": "work directory size", "unit": "bytes", "fault_windows": "exclude"},
    "batch_seconds": {"label": "mean batch duration", "unit": "seconds", "fault_windows": "exclude"},
    "lag_seconds": {"label": "archive lag", "unit": "seconds", "fault_windows": "exclude"},
}

REQUIRED_KEYS = (
    "name",
    "description",
    "duration_s",
    "rate_records_per_s",
    "record_bytes",
    "partitions",
    "sample_interval_s",
    "lag_interval_s",
    "raw_snapshot_interval_s",
    "prune_interval_s",
    "scrub_interval_s",
    "scrub_sample_batches",
    "fault_interval_s",
    "first_fault_after_s",
    "quiet_tail_s",
    "fault_cycle",
    "fault_hold_s",
    "clock_step_seconds",
    "recovery_timeout_s",
    "drain_timeout_s",
    "check_timeout_s",
    "min_free_gib",
    "kafka_retention_hours",
    "engine",
    "criteria",
)

CRITERIA_KEYS = (
    "trend_window_fraction",
    "trend_min_pairs",
    "trend",
    "min_rate_fraction",
    "max_steady_lag_p95_s",
    "min_samples_fraction",
    "max_sample_gap_s",
    "min_free_gib_floor",
    "min_scrub_batches",
    "min_prune_fraction",
)

TREND_KEYS = ("max_growth_pct", "min_abs_growth")


class ProfileError(ValueError):
    """A profile that cannot be run, with the key at fault."""


@dataclass(frozen=True)
class PlannedFault:
    id: int
    at_s: int
    kind: str
    hold_s: int


def _positive_int(profile: dict, key: str, minimum: int = 1) -> int:
    value = profile.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ProfileError(f"profile key '{key}' must be an integer of at least {minimum}, got {value!r}")
    return value


def validate(profile: dict) -> dict:
    """Return the profile when it can be run; raise ProfileError naming the key."""
    if not isinstance(profile, dict):
        raise ProfileError("a profile is a JSON object")
    missing = [key for key in REQUIRED_KEYS if key not in profile]
    if missing:
        raise ProfileError(f"profile is missing keys: {', '.join(missing)}")
    for key in (
        "duration_s",
        "rate_records_per_s",
        "record_bytes",
        "partitions",
        "sample_interval_s",
        "lag_interval_s",
        "raw_snapshot_interval_s",
        "prune_interval_s",
        "scrub_interval_s",
        "scrub_sample_batches",
        "fault_interval_s",
        "first_fault_after_s",
        "quiet_tail_s",
        "recovery_timeout_s",
        "drain_timeout_s",
        "check_timeout_s",
        "kafka_retention_hours",
    ):
        _positive_int(profile, key)
    if not isinstance(profile["min_free_gib"], int) or profile["min_free_gib"] < 0:
        raise ProfileError("profile key 'min_free_gib' must be a non-negative integer")
    cycle = profile["fault_cycle"]
    if not isinstance(cycle, list) or not cycle:
        raise ProfileError("profile key 'fault_cycle' must be a non-empty list")
    for kind in cycle:
        if kind not in FAULT_KINDS:
            raise ProfileError(f"fault_cycle names '{kind}', which is not one of {', '.join(FAULT_KINDS)}")
    hold = profile["fault_hold_s"]
    if not isinstance(hold, dict):
        raise ProfileError("profile key 'fault_hold_s' must be an object")
    for kind in cycle:
        if not isinstance(hold.get(kind), int) or hold[kind] < 0:
            raise ProfileError(f"fault_hold_s must give a non-negative integer for '{kind}'")
    if "enospc" in cycle and hold["enospc"] < 1:
        raise ProfileError("fault_hold_s.enospc must be at least 1: the engine needs time to meet the full disk")
    steps = profile["clock_step_seconds"]
    if not isinstance(steps, list) or not all(isinstance(s, int) and s != 0 for s in steps):
        raise ProfileError("clock_step_seconds must be a list of non-zero integers")
    if "clock_step" in cycle and not steps:
        raise ProfileError("clock_step is in fault_cycle but clock_step_seconds is empty")
    if profile["quiet_tail_s"] >= profile["duration_s"]:
        raise ProfileError("quiet_tail_s must be shorter than duration_s")
    criteria = profile["criteria"]
    if not isinstance(criteria, dict):
        raise ProfileError("profile key 'criteria' must be an object")
    absent = [key for key in CRITERIA_KEYS if key not in criteria]
    if absent:
        raise ProfileError(f"criteria is missing keys: {', '.join(absent)}")
    fraction = criteria["trend_window_fraction"]
    if not isinstance(fraction, (int, float)) or not 0 < fraction <= 1:
        raise ProfileError("criteria.trend_window_fraction must be in (0, 1]")
    trend = criteria["trend"]
    if not isinstance(trend, dict):
        raise ProfileError("criteria.trend must be an object")
    for series in TREND_SERIES:
        if series not in trend:
            raise ProfileError(f"criteria.trend has no entry for '{series}'")
        for key in TREND_KEYS:
            value = trend[series].get(key) if isinstance(trend[series], dict) else None
            if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
                raise ProfileError(f"criteria.trend.{series}.{key} must be a non-negative number")
    plan = build_plan(profile)
    if not plan:
        raise ProfileError(
            "the profile schedules no fault: duration_s is too short for first_fault_after_s and quiet_tail_s"
        )
    kinds = {fault.kind for fault in plan}
    unscheduled = [kind for kind in cycle if kind not in kinds]
    if unscheduled:
        raise ProfileError(
            f"the plan never reaches {', '.join(unscheduled)}: lengthen duration_s or shorten fault_interval_s"
        )
    return profile


def build_plan(profile: dict) -> list[PlannedFault]:
    """The faults a run injects: one every `fault_interval_s` from
    `first_fault_after_s`, cycling through `fault_cycle`, none in the last
    `quiet_tail_s` (the engine drains and the final checks run undisturbed)."""
    cycle = profile["fault_cycle"]
    hold = profile["fault_hold_s"]
    last = profile["duration_s"] - profile["quiet_tail_s"]
    plan: list[PlannedFault] = []
    at = profile["first_fault_after_s"]
    while at <= last:
        kind = cycle[len(plan) % len(cycle)]
        plan.append(PlannedFault(id=len(plan) + 1, at_s=at, kind=kind, hold_s=hold[kind]))
        at += profile["fault_interval_s"]
    return plan


def clock_step_for(profile: dict, occurrence: int) -> int:
    """The clock step of the `occurrence`-th (0-based) clock_step fault."""
    steps = profile["clock_step_seconds"]
    return steps[occurrence % len(steps)]


def load(name_or_path: str) -> dict:
    """Load a profile by name (`72h`) or by path, and validate it."""
    path = Path(name_or_path)
    if not path.suffix:
        path = PROFILE_DIR / f"{name_or_path}.json"
    try:
        profile = json.loads(path.read_text())
    except FileNotFoundError:
        known = ", ".join(sorted(p.stem for p in PROFILE_DIR.glob("*.json")))
        raise ProfileError(f"no profile at {path} (known: {known})") from None
    except json.JSONDecodeError as error:
        raise ProfileError(f"{path} is not valid JSON: {error}") from None
    return validate(profile)
