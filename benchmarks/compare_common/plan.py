"""What a cross-product run owes, and how long it is projected to take (#744 section 4.1).

One definition of "complete", shared by the runner, `--check` and the report,
as `kafka_compare/plan.py` is for the Kafka comparison: a plan lists the cells,
the systems and their durability profiles per cell, the key variants of each
cell (record size, link profile, rotation style...) and the repetitions, and
`expected_keys` turns it into the rows a run must hold. A run records the plan
it ran in `environment.json`, so the report judges completeness against what
was asked, not against what the code would ask today.

Each kind commits its plans in `<kind>_compare/plans.json` under names
(`smoke`, `hosted`, `full`); `load_plans` reads them and `check_plan` refuses a
plan that names an unknown cell, system or profile, a profile a cell forbids,
or a variant missing a key column.

    python3 -m compare_common.plan archive_compare/plans.json full --max-hours 12

prints the cells, the expected row count and the projected hours, and exits 2
when the projection is over `--max-hours`.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from typing import Any, NamedTuple

HOST_CLASSES = ("laptop", "hosted", "dedicated")


class PlanRefused(Exception):
    """The plan, or the projected cost of it, is not acceptable; the message says why."""


class Planned(NamedTuple):
    """One run of one system, in one profile, cell, variant and repetition."""

    system: str
    profile: str
    cell: str
    repetition: int
    variant: dict[str, Any]
    params: dict[str, Any]
    metric_class: str

    def key(self, variant_columns: Sequence[str]) -> tuple[str, ...]:
        return key_of(self.system, self.profile, self.cell, self.repetition, self.variant, variant_columns)


def key_of(system: str, profile: str, cell: str, repetition: Any, variant: dict[str, Any],
           variant_columns: Sequence[str]) -> tuple[str, ...]:
    """The row key as strings, the form the CSV holds it in."""
    return (str(system), str(profile), str(cell), str(repetition),
            *(str(variant.get(column, "")) for column in variant_columns))


def load_plans(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict) or not isinstance(data.get("plans"), dict):
        raise PlanRefused(f"{path}: no `plans` object")
    return data


def select(plans: dict[str, Any], name: str) -> dict[str, Any]:
    try:
        plan = dict(plans["plans"][name])
    except KeyError:
        raise PlanRefused(f"no plan named {name!r}; there are {sorted(plans['plans'])}") from None
    plan["name"] = name
    plan.setdefault("kind", plans.get("kind"))
    return plan


def check_plan(plan: dict[str, Any], kind: Any) -> None:
    """Refuse a plan the kind cannot run, or whose rows could not be told apart."""
    problems = []
    if plan.get("kind") != kind.name:
        problems.append(f"plan kind is {plan.get('kind')!r}, not {kind.name!r}")
    if plan.get("host_class") not in HOST_CLASSES:
        problems.append(f"host_class {plan.get('host_class')!r} is not one of {HOST_CLASSES}")
    systems = plan.get("systems") or []
    if not systems:
        problems.append("no systems")
    for system in systems:
        if system not in kind.profiles:
            problems.append(f"system {system!r} is not run by the {kind.name} kind")
    cells = plan.get("cells") or []
    if not cells:
        problems.append("no cells")
    seen = set()
    for index, cell in enumerate(cells):
        name = cell.get("cell")
        where = f"cells[{index}] ({name})"
        if name not in kind.cells:
            problems.append(f"{where}: unknown cell; the kind has {sorted(kind.cells)}")
            continue
        if name in seen:
            problems.append(f"{where}: listed twice")
        seen.add(name)
        reps = cell.get("repetitions")
        if not isinstance(reps, int) or reps < 1:
            problems.append(f"{where}: repetitions must be a positive integer, got {reps!r}")
        minutes = cell.get("minutes_per_run")
        if not isinstance(minutes, (int, float)) or minutes <= 0:
            problems.append(f"{where}: minutes_per_run must be positive, got {minutes!r}")
        variants = cell.get("variants") or [{}]
        keys = set()
        for variant in variants:
            missing = [column for column in kind.variant_columns if column not in variant]
            if missing:
                problems.append(f"{where}: a variant lacks {missing}")
            key = tuple(str(variant.get(column, "")) for column in kind.variant_columns)
            if key in keys:
                problems.append(f"{where}: two variants with the key {key}")
            keys.add(key)
        for system in systems:
            for profile in cell_profiles(plan, cell, system):
                if profile not in kind.profiles.get(system, ()):
                    problems.append(f"{where}: {system} has no profile {profile!r}")
                reason = kind.forbidden(system, profile, name) if kind.forbidden else None
                if reason:
                    problems.append(f"{where}: {system} {profile}: {reason}")
    if problems:
        raise PlanRefused("; ".join(problems))


def cell_profiles(plan: dict[str, Any], cell: dict[str, Any], system: str) -> list[str]:
    """The profiles `system` runs in `cell`: the cell's own list, else the plan's."""
    own = (cell.get("profiles") or {}).get(system)
    if own is not None:
        return list(own)
    return list((plan.get("profiles") or {}).get(system) or [])


def schedule(plan: dict[str, Any], kind: Any) -> list[Planned]:
    """Every run the plan asks for, in the order they run.

    Cells in plan order; inside a cell, each variant, each repetition, then the
    systems. With the kind's `rotate_systems`, the systems' order is rotated by
    one per repetition (A B C D, then B C D A...), so a drift in the host over
    a long cell is spread across the systems instead of landing on whichever
    runs last (#747)."""
    out = []
    systems = list(plan["systems"])
    for cell in plan["cells"]:
        name = cell["cell"]
        metric_class = kind.cells[name]
        for variant in cell.get("variants") or [{}]:
            for repetition in range(1, cell["repetitions"] + 1):
                order = systems
                if kind.rotate_systems:
                    shift = (repetition - 1) % len(systems)
                    order = systems[shift:] + systems[:shift]
                for system in order:
                    for profile in cell_profiles(plan, cell, system):
                        params = dict(cell.get("params") or {})
                        params.update({k: v for k, v in variant.items() if k not in kind.variant_columns})
                        out.append(Planned(system, profile, name, repetition,
                                           {column: variant[column] for column in kind.variant_columns},
                                           params, metric_class))
    return out


def expected_keys(plan: dict[str, Any], kind: Any) -> list[tuple[str, ...]]:
    return [item.key(kind.variant_columns) for item in schedule(plan, kind)]


def projected_hours(plan: dict[str, Any], kind: Any, cooldown_seconds: float = 0.0) -> float:
    minutes = 0.0
    by_cell = {cell["cell"]: cell for cell in plan["cells"]}
    for item in schedule(plan, kind):
        minutes += float(by_cell[item.cell]["minutes_per_run"]) + cooldown_seconds / 60.0
    return minutes / 60.0


def describe(plan: dict[str, Any], kind: Any, cooldown_seconds: float = 0.0) -> str:
    lines = [f"plan {plan.get('name')} ({plan['kind']}), host class {plan['host_class']}, "
             f"systems {', '.join(plan['systems'])}"]
    items = schedule(plan, kind)
    for cell in plan["cells"]:
        count = sum(1 for item in items if item.cell == cell["cell"])
        lines.append(f"  {cell['cell']:<20} {kind.cells[cell['cell']]:<12} {count:>4} runs x "
                     f"{cell['minutes_per_run']} min")
    lines.append(f"{len(items)} rows expected, projected {projected_hours(plan, kind, cooldown_seconds):.1f} hours "
                 "(an estimate from per-run costs, not a measurement)")
    return "\n".join(lines)


def check_budget(plan: dict[str, Any], kind: Any, max_hours: float, cooldown_seconds: float = 0.0) -> float:
    hours = projected_hours(plan, kind, cooldown_seconds)
    if hours > max_hours:
        raise PlanRefused(f"plan {plan.get('name')} is projected at {hours:.1f} hours, over --max-hours {max_hours}; "
                          "run it in pieces with --budget-minutes and --resume, or raise the limit")
    return hours


def main(argv: list[str] | None = None) -> int:
    from compare_common import kinds

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("plans", help="a <kind>_compare/plans.json")
    parser.add_argument("name", help="the plan's name in that file (smoke, hosted, full)")
    parser.add_argument("--max-hours", type=float, default=None)
    parser.add_argument("--cooldown-seconds", type=float, default=0.0)
    args = parser.parse_args(argv)
    try:
        plans = load_plans(args.plans)
        kind = kinds.get(plans.get("kind"))
        plan = select(plans, args.name)
        check_plan(plan, kind)
        print(describe(plan, kind, args.cooldown_seconds))
        if args.max_hours is not None:
            check_budget(plan, kind, args.max_hours, args.cooldown_seconds)
    except PlanRefused as exc:
        print(f"plan refused: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
