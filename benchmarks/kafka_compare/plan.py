"""What a run of a matrix writes, and how long it is projected to take (#596).

One definition of "complete", shared by the runner and the report. The
runner's `--check` and the report's completeness check both read
`expected_keys`, so a run and its report cannot disagree about which rows a
matrix owes.

A matrix recorded before schema 2 (phase 1, run 20261109T232328Z) has none of
the schema-2 axes. Each one defaults to what that run did: compression
`none`, the one recorded `profile`, a `kafka` load basis over `load_pcts`, no
end-to-end rows and no `acks` slice. Its keys are therefore the rows it
wrote.

Schema 3 adds one axis, `group_commits` (#752): VTOP's `group_commit:` setting,
`off` and/or `on`. It belongs to VTOP alone (Kafka has no counterpart), so
Kafka's rows carry a blank setting and VTOP owes its rows once per setting
asked for. A matrix without the axis leaves VTOP at the node's own default and
writes a blank setting everywhere, which is what every earlier run did.

Schema 4 (#600) adds two optional keys. `resources: true` says every measured row also carries the CPU
seconds and bytes written its window cost. `e2e.extra_profiles` lists profiles that Kafka ALONE is also
measured under, in the end-to-end mode only, at the same common offered rate: the latency cost of a durability
profile, read beside the crash results. VTOP owes no row for them (its rows carry a blank group-commit
setting there too, as Kafka's always do).

    python3 benchmarks/kafka_compare/plan.py MATRIX.json [--max-hours H]

prints the cells, the expected row count and the projected hours, and exits
2 when the projection is over `--max-hours`.
"""
from __future__ import annotations

import argparse
import itertools
import json
import sys
from typing import Any, NamedTuple

SYSTEMS = ("kafka", "vtop")
COMPRESSIONS = ("none", "gzip", "zstd", "snappy", "lz4")
ACKS = ("all", "1")
PROFILES = ("defaults", "fsync-parity")
LOAD_BASES = ("kafka", "common")
# VTOP's `group_commit.enabled` (#752), in the order a run lists them. Written
# into the node config as `enabled: false` / `enabled: true`, never left to the
# binary's default, so a later change of that default cannot change what "off" means.
GROUP_COMMITS = ("off", "on")
SLICE_MODES = ("saturation", "consume")


class PlanRefused(Exception):
    """The matrix or the projected cost is not acceptable; the message says why."""


class Cell(NamedTuple):
    """One (record size, partitions, producers, compression, acks) shape, and
    the measurements each system makes in it."""

    record_size: int
    partitions: int
    producers: int
    compression: str
    acks: str
    modes: tuple[str, ...]


class Axes(NamedTuple):
    cells: list[Cell]
    profiles: list[str]
    repetitions: int
    load_bases: dict[str, list[int]]
    e2e: dict[str, Any] | None
    group_commits: list[str]


def axes(matrix: dict[str, Any]) -> Axes:
    """The matrix's axes, a pre-schema-2 matrix's defaulted to what it ran."""
    profiles = matrix.get("profiles") or [matrix.get("profile") or "defaults"]
    load_bases = matrix.get("load_bases") or {"kafka": list(matrix.get("load_pcts") or [])}
    e2e = matrix.get("e2e") or None
    main_modes = ["saturation"]
    if any(load_bases.values()):
        main_modes.append("fixed_rate")
    if matrix.get("consume"):
        main_modes.append("consume")
    if e2e:
        main_modes.append("e2e")
    cells = [
        Cell(size, partitions, producers, compression, acks, tuple(main_modes))
        for size, partitions, producers, compression, acks in itertools.product(
            matrix["record_sizes"], matrix["partitions"], matrix["producers"],
            matrix.get("compressions") or ["none"], matrix["acks"])
    ]
    for extra in matrix.get("acks_slice") or []:
        cells.append(Cell(extra["record_size"], extra["partitions"], extra["producers"], extra["compression"],
                          extra["acks"], tuple(extra["modes"])))
    return Axes(cells, list(profiles), int(matrix["repetitions"]), dict(load_bases), e2e,
                list(matrix.get("group_commits") or []))


def expected_keys(matrix: dict[str, Any]) -> set[tuple]:
    """Every row key a complete run of `matrix` writes:

    (system, mode, record_size, partitions, producers, compression, acks,
     profile, group_commit, repetition, load_basis, load_pct)

    with load_basis and load_pct as the CSV's text ("" where a mode has
    none), and group_commit "" for Kafka and for a matrix without the axis,
    else VTOP's setting: VTOP owes its rows once per setting asked for. A
    system that is not run at a rate still owes its row: a `refused` row
    saying why, never a hole.
    """
    a = axes(matrix)
    keys = set()
    for cell in a.cells:
        for profile, system, rep in itertools.product(a.profiles, SYSTEMS, range(1, a.repetitions + 1)):
            for group_commit in (a.group_commits or [""]) if system == "vtop" else [""]:
                base = (system, None, cell.record_size, cell.partitions, cell.producers, cell.compression,
                        cell.acks, profile, group_commit, rep)
                for mode in cell.modes:
                    if mode == "fixed_rate":
                        for basis, pcts in a.load_bases.items():
                            for pct in pcts:
                                keys.add(base[:1] + (mode,) + base[2:] + (basis, str(pct)))
                    elif mode == "e2e":
                        keys.add(base[:1] + (mode,) + base[2:] + (a.e2e["load_basis"], str(a.e2e["load_pct"])))
                    else:
                        keys.add(base[:1] + (mode,) + base[2:] + ("", ""))
    for extra in extra_profiles(matrix):
        for cell in a.cells:
            if "e2e" in cell.modes:
                for rep in range(1, a.repetitions + 1):
                    keys.add(("kafka", "e2e", cell.record_size, cell.partitions, cell.producers, cell.compression,
                              cell.acks, extra, "", rep, a.e2e["load_basis"], str(a.e2e["load_pct"])))
    return keys


def extra_profiles(matrix: dict[str, Any]) -> list[str]:
    """The profiles Kafka alone is also measured under in the end-to-end mode (schema 4), or none."""
    e2e = matrix.get("e2e")
    return list(e2e.get("extra_profiles") or []) if isinstance(e2e, dict) else []


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def check_matrix(matrix: dict[str, Any]) -> None:
    """The schema-2 axes, where present, have the shape `expected_keys` needs."""
    def strings(key: str, allowed: tuple[str, ...]) -> None:
        values = matrix.get(key)
        if values is None:
            return
        if (not isinstance(values, list) or not values or len(set(values)) != len(values)
                or any(v not in allowed for v in values)):
            raise PlanRefused(f"matrix.{key} is {values!r}, not a list of distinct values from {list(allowed)}")

    for key in ("record_sizes", "partitions", "producers"):
        values = matrix.get(key)
        if (not isinstance(values, list) or not values
                or any(not _is_int(v) or v < 1 for v in values) or len(set(values)) != len(values)):
            raise PlanRefused(f"matrix.{key} is {values!r}, not a list of distinct positive integers")
    if not _is_int(matrix.get("repetitions")) or matrix["repetitions"] < 1:
        raise PlanRefused(f"matrix.repetitions is {matrix.get('repetitions')!r}, not a positive integer")
    acks = matrix.get("acks")
    if not isinstance(acks, list) or not acks or len(set(acks)) != len(acks):
        raise PlanRefused(f"matrix.acks is {matrix.get('acks')!r}, not a list of distinct acknowledgement modes")
    fixed = matrix.get("fixed_seconds", 30.0)
    if isinstance(fixed, bool) or not isinstance(fixed, (int, float)) or not fixed > 0:
        raise PlanRefused(f"matrix.fixed_seconds is {fixed!r}, not a positive number of seconds")
    share = matrix.get("vtop_profiles_share_config", False)
    if not isinstance(share, bool):
        raise PlanRefused(f"matrix.vtop_profiles_share_config is {share!r}, not true or false")
    if not isinstance(matrix.get("acks_slice", []), list):
        raise PlanRefused(f"matrix.acks_slice is {matrix.get('acks_slice')!r}, not a list of cells")
    if not isinstance(matrix.get("load_bases", {}), dict):
        raise PlanRefused(f"matrix.load_bases is {matrix.get('load_bases')!r}, not a map of bases to rates")
    strings("compressions", COMPRESSIONS)
    strings("profiles", PROFILES)
    strings("group_commits", GROUP_COMMITS)
    if any(a not in ACKS for a in matrix["acks"]):
        raise PlanRefused(f"matrix.acks is {matrix['acks']!r}; acknowledgement modes are {list(ACKS)}")
    bases = matrix.get("load_bases")
    if isinstance(bases, dict) and isinstance(bases.get("kafka"), list) and "load_pcts" in matrix \
            and list(bases["kafka"]) != list(matrix["load_pcts"]):
        # The report draws the Kafka-relative rows by `load_pcts`; two lists
        # that disagree would leave measured rows out of it, or show cells
        # that were never owed (review).
        raise PlanRefused(f"matrix.load_bases.kafka {bases['kafka']!r} differs from matrix.load_pcts "
                          f"{matrix['load_pcts']!r}; they name the same rates")
    if bases is not None:
        if not isinstance(bases, dict) or not bases or any(b not in LOAD_BASES for b in bases):
            raise PlanRefused(f"matrix.load_bases is {bases!r}, not a map from {list(LOAD_BASES)} to load percentages")
        for basis, pcts in bases.items():
            if (not isinstance(pcts, list) or not pcts or len(set(pcts)) != len(pcts)
                    or any(not _is_int(p) or not 0 < p < 100 for p in pcts)):
                raise PlanRefused(f"matrix.load_bases.{basis} is {pcts!r}, not distinct percentages in 1..99")
    e2e = matrix.get("e2e")
    if e2e not in (None, False):
        if (not isinstance(e2e, dict) or e2e.get("load_basis") not in LOAD_BASES
                or not _is_int(e2e.get("load_pct")) or not 0 < e2e["load_pct"] < 100):
            raise PlanRefused(f"matrix.e2e is {e2e!r}, not {{load_basis, load_pct}}")
        if "extra_profiles" in e2e:
            extra = e2e["extra_profiles"]
            if (not isinstance(extra, list) or not extra or len(set(extra)) != len(extra)
                    or any(p not in PROFILES for p in extra)):
                raise PlanRefused(f"matrix.e2e.extra_profiles is {extra!r}, not a list of distinct profiles from "
                                  f"{list(PROFILES)}")
            main = matrix.get("profiles") or [matrix.get("profile") or "defaults"]
            already = [p for p in extra if p in main]
            if already:
                raise PlanRefused(f"matrix.e2e.extra_profiles names {already}, which the run already measures "
                                  "in every mode")
    if not isinstance(matrix.get("resources", False), bool):
        raise PlanRefused(f"matrix.resources is {matrix.get('resources')!r}, not true or false")
    for extra in matrix.get("acks_slice") or []:
        if (not isinstance(extra, dict) or extra.get("acks") not in ACKS
                or extra.get("compression") not in COMPRESSIONS
                or any(not _is_int(extra.get(k)) or extra.get(k) < 1
                       for k in ("record_size", "partitions", "producers"))
                or not extra.get("modes") or any(m not in SLICE_MODES for m in extra["modes"])):
            raise PlanRefused(f"matrix.acks_slice entry {extra!r} is not a cell with acks, compression, "
                              f"record_size, partitions, producers and modes from {list(SLICE_MODES)}")


# ----------------------------------------------------------------------------- cost

# Seconds per step: estimates from the plan on #596, for duration-targeted
# runs. An estimate to refuse an impossible matrix before any Docker work,
# not a measurement; the rehearsal's first cells calibrate it.
#
# A matrix that sizes saturation and consume runs by record count
# (`sat_records`, the runner today) is charged by the bytes they move at
# SLOWEST_BYTES_PER_SECOND instead, when that is longer: phase 1's VTOP
# moved about 2.3 MiB/s at both record sizes (run 20261109T232328Z), so 2 MiB/s
# keeps a record-count run from passing the budget on a duration it never
# had (review).
DEFAULT_TIMING = {
    "bring_up": 90.0,        # cluster up, smoke read-back, cluster down
    "warm_up": 30.0,         # the unrecorded warm-up run
    "saturation": 25.0,      # one duration-targeted saturation run
    "fixed_rate": 10.0,      # one fixed-rate run's overhead; its duration is matrix.fixed_seconds
    "consume": 20.0,
    "e2e": 40.0,
    # Schema 3: the cgroup counters are read, and the nodes synced, before and after every measured run.
    "resource_window": 6.0,
}
SLOWEST_BYTES_PER_SECOND = 2 * 1024 * 1024


def projected_seconds(matrix: dict[str, Any], timing: dict[str, float] | None = None) -> float:
    """Wall time of the whole matrix, both systems, every profile run.

    A VTOP cell whose configuration is the same under two profiles
    (`matrix.vtop_profiles_share_config`) brings its cluster up and makes its
    unrated runs once, recorded under both (`same_config_as`); its
    fixed-rate runs are made per profile.
    """
    t = {**DEFAULT_TIMING, **(timing or {})}
    # A fixed-rate run lasts the matrix's own target (review): charging a
    # constant would let a long --fixed-seconds pass the budget unseen.
    t["fixed_rate"] += float(matrix.get("fixed_seconds", 30.0))
    a = axes(matrix)
    fixed = sum(len(p) for p in a.load_bases.values())
    sat_records = {int(k): int(v) for k, v in (matrix.get("sat_records") or {}).items()}
    total = 0.0
    profiles = len(a.profiles)
    for cell in a.cells:
        sized = {mode: t[mode] for mode in ("saturation", "consume")}
        if cell.record_size in sat_records:
            moved = sat_records[cell.record_size] * cell.record_size / SLOWEST_BYTES_PER_SECOND
            sized = {mode: max(seconds, moved) for mode, seconds in sized.items()}
        shared = sum(sized.get(m, t[m]) for m in cell.modes if m != "fixed_rate")
        rated = t["fixed_rate"] * fixed if "fixed_rate" in cell.modes else 0.0
        if matrix.get("resources"):
            runs = sum(1 for m in cell.modes if m != "fixed_rate") + (fixed if "fixed_rate" in cell.modes else 0)
            shared += t["resource_window"] * runs
        one = t["bring_up"] + t["warm_up"] + a.repetitions * (shared + rated)
        # Kafka runs every profile in full. VTOP, when its configuration is
        # the same under all of them, brings its cluster up once and makes
        # its unrated runs once, but its fixed-rate runs once per profile:
        # their rates are a share of that profile's Kafka saturation.
        if matrix.get("vtop_profiles_share_config"):
            vtop = one + (profiles - 1) * a.repetitions * rated
        else:
            vtop = one * profiles
        # VTOP's cluster is brought up and measured afresh for every group-commit
        # setting asked for (the node's config is read at start); Kafka's is not.
        total += one * profiles + vtop * max(len(a.group_commits), 1)
        if "e2e" in cell.modes:
            # Kafka alone, once per extra profile: a fresh cluster, the warm-up, and the e2e runs.
            total += len(extra_profiles(matrix)) * (t["bring_up"] + t["warm_up"] + a.repetitions * t["e2e"])
    return total


def check_budget(matrix: dict[str, Any], max_hours: float, timing: dict[str, float] | None = None) -> float:
    """The projected hours, or PlanRefused when they exceed `max_hours`."""
    hours = projected_seconds(matrix, timing) / 3600
    if hours > max_hours:
        raise PlanRefused(f"the matrix is projected to take {hours:.1f} h, over --max-hours {max_hours:g}; "
                          "narrow the matrix or raise the limit")
    return hours


def summary(matrix: dict[str, Any]) -> str:
    """The plan's one-line summary: cells, rows owed and projected hours."""
    a = axes(matrix)
    hours = projected_seconds(matrix) / 3600
    settings = f" x group commit {', '.join(a.group_commits)} (VTOP)" if a.group_commits else ""
    return (f"{len(a.cells)} cells x {len(a.profiles)} profile(s) x {len(SYSTEMS)} systems{settings} x "
            f"{a.repetitions} repetitions: {len(expected_keys(matrix))} result rows, projected {hours:.1f} h")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("matrix", help="a JSON file holding a matrix, or an environment.json")
    parser.add_argument("--max-hours", type=float, default=None)
    args = parser.parse_args(argv)
    with open(args.matrix) as handle:
        data = json.load(handle)
    matrix = data.get("matrix", data)
    try:
        check_matrix(matrix)
        print(summary(matrix))
        if args.max_hours is not None:
            check_budget(matrix, args.max_hours)
    except PlanRefused as exc:
        print(f"plan refused: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
