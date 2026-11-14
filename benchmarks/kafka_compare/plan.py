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
    return Axes(cells, list(profiles), int(matrix["repetitions"]), dict(load_bases), e2e)


def expected_keys(matrix: dict[str, Any]) -> set[tuple]:
    """Every row key a complete run of `matrix` writes:

    (system, mode, record_size, partitions, producers, compression, acks,
     profile, repetition, load_basis, load_pct)

    with load_basis and load_pct as the CSV's text ("" where a mode has
    none). A system that is not run at a rate still owes its row: a
    `refused` row saying why, never a hole.
    """
    a = axes(matrix)
    keys = set()
    for cell in a.cells:
        for profile, system, rep in itertools.product(a.profiles, SYSTEMS, range(1, a.repetitions + 1)):
            base = (system, None, cell.record_size, cell.partitions, cell.producers, cell.compression,
                    cell.acks, profile, rep)
            for mode in cell.modes:
                if mode == "fixed_rate":
                    for basis, pcts in a.load_bases.items():
                        for pct in pcts:
                            keys.add(base[:1] + (mode,) + base[2:] + (basis, str(pct)))
                elif mode == "e2e":
                    keys.add(base[:1] + (mode,) + base[2:] + (a.e2e["load_basis"], str(a.e2e["load_pct"])))
                else:
                    keys.add(base[:1] + (mode,) + base[2:] + ("", ""))
    return keys


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
}
SLOWEST_BYTES_PER_SECOND = 2 * 1024 * 1024


def projected_seconds(matrix: dict[str, Any], timing: dict[str, float] | None = None) -> float:
    """Wall time of the whole matrix, both systems, every profile run.

    A VTOP cell whose configuration is the same under two profiles runs once
    and its rows are recorded under both (`same_config_as`), when
    `matrix.vtop_profiles_share_config` says so.
    """
    t = {**DEFAULT_TIMING, **(timing or {})}
    # A fixed-rate run lasts the matrix's own target (review): charging a
    # constant would let a long --fixed-seconds pass the budget unseen.
    t["fixed_rate"] += float(matrix.get("fixed_seconds", 30.0))
    a = axes(matrix)
    fixed = sum(len(p) for p in a.load_bases.values())
    sat_records = {int(k): int(v) for k, v in (matrix.get("sat_records") or {}).items()}
    total = 0.0
    for cell in a.cells:
        sized = {mode: t[mode] for mode in ("saturation", "consume")}
        if cell.record_size in sat_records:
            moved = sat_records[cell.record_size] * cell.record_size / SLOWEST_BYTES_PER_SECOND
            sized = {mode: max(seconds, moved) for mode, seconds in sized.items()}
        per_rep = 0.0
        for mode in cell.modes:
            per_rep += sized.get(mode, t[mode]) * (fixed if mode == "fixed_rate" else 1)
        one = t["bring_up"] + t["warm_up"] + a.repetitions * per_rep
        kafka_runs = len(a.profiles)
        vtop_runs = 1 if matrix.get("vtop_profiles_share_config") else len(a.profiles)
        total += one * (kafka_runs + vtop_runs)
    return total


def check_budget(matrix: dict[str, Any], max_hours: float, timing: dict[str, float] | None = None) -> float:
    """The projected hours, or PlanRefused when they exceed `max_hours`."""
    hours = projected_seconds(matrix, timing) / 3600
    if hours > max_hours:
        raise PlanRefused(f"the matrix is projected to take {hours:.1f} h, over --max-hours {max_hours:g}; "
                          "narrow the matrix or raise the limit")
    return hours


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
        a = axes(matrix)
        hours = projected_seconds(matrix) / 3600
        print(f"{len(a.cells)} cells x {len(a.profiles)} profile(s) x {len(SYSTEMS)} systems x "
              f"{a.repetitions} repetitions: {len(expected_keys(matrix))} result rows, projected {hours:.1f} h")
        if args.max_hours is not None:
            check_budget(matrix, args.max_hours)
    except PlanRefused as exc:
        print(f"plan refused: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
