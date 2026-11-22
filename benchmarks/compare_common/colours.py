"""One colour per system per family in the cross-product charts (#744 section 4.3).

A family's systems take the reference palette's categorical slots in the
registry's order (`products.toml` [families.*].order), never by rank: slot 1
blue, slot 2 orange (VTOP, always), slot 3 aqua, slot 4 yellow. A chart draws
the family order filtered to the systems it shows, so a filtered chart never
repaints a survivor.

The checks are the Kafka comparison's (`kafka_compare.colours`: OKLab distance
x100, Machado 2009 colour-vision-deficiency simulation, WCAG contrast), run on
every ADJACENT pair of a chart's order, because adjacent bars are the ones a
reader must tell apart. Orange beside yellow fails the normal-vision floor
(#744 section 4.3), so a chart may hold yellow only when aqua sits between it
and orange, which in practice means only the exact four-system order.
"""
from __future__ import annotations

from collections.abc import Iterable, Sequence

from kafka_compare import colours as base

# (name, light step, dark step), slots 1 to 4 of the reference palette.
SLOTS = (
    ("blue", "#2a78d6", "#3987e5"),
    ("orange", "#eb6834", "#d95926"),
    ("aqua", "#1baf7a", "#199e70"),
    ("yellow", "#eda100", "#c98500"),
)
MAX_SYSTEMS = len(SLOTS)
# Without a table beside the chart, three systems at most (#744 section 4.3).
MAX_SYSTEMS_WITHOUT_TABLE = 3


class ColourRefused(ValueError):
    """The chart's systems cannot be drawn with the validated palette; the message says why."""


def slot_of(order: Sequence[str], system: str) -> int:
    if system not in order:
        raise ColourRefused(f"system {system!r} is not in the family order {list(order)}")
    return list(order).index(system)


def adjacent_failures(hexes: Sequence[str], mode: str = "light") -> list[str]:
    """Every adjacent pair that fails the CVD target or the normal-vision floor."""
    out = []
    for one, two in zip(hexes, hexes[1:]):
        normal = base.delta_e(one, two)
        if normal < base.NORMAL_FLOOR:
            out.append(f"{one} beside {two}: normal-vision distance {normal:.1f} < {base.NORMAL_FLOOR}")
        for kind in base.MACHADO:
            seen = base.delta_e(one, two, kind)
            if seen < base.CVD_TARGET:
                out.append(f"{one} beside {two}: {kind} distance {seen:.1f} < {base.CVD_TARGET}")
    return out


def series(order: Sequence[str], systems: Iterable[str], mode: str = "light", *,
           with_table: bool = True) -> dict[str, str]:
    """{system: colour} for one chart, in the family order; refused when the palette cannot separate it."""
    if mode not in ("light", "dark"):
        raise ColourRefused(f"mode is {mode!r}, not light or dark")
    wanted = set(systems)
    unknown = sorted(wanted - set(order))
    if unknown:
        raise ColourRefused(f"systems {unknown} are not in the family order {list(order)}")
    drawn = [system for system in order if system in wanted]
    limit = MAX_SYSTEMS if with_table else MAX_SYSTEMS_WITHOUT_TABLE
    if len(drawn) > limit:
        raise ColourRefused(f"{len(drawn)} systems in one chart; at most {limit}"
                            + ("" if with_table else " without a table beside it"))
    if len(order) > MAX_SYSTEMS:
        raise ColourRefused(f"the family order has {len(order)} systems; only {MAX_SYSTEMS} slots are validated")
    column = 1 if mode == "light" else 2
    hexes = [SLOTS[slot_of(order, system)][column] for system in drawn]
    failures = adjacent_failures(hexes, mode)
    if failures:
        raise ColourRefused(f"order {drawn} cannot be drawn: " + "; ".join(failures))
    return dict(zip(drawn, hexes))
