#!/usr/bin/env python3
"""The README's evidence charts, drawn from committed measurements only.

    python3 -m pip install -r benchmarks/kafka_compare/requirements-report.txt
    python3 benchmarks/readme_charts.py
    git status --porcelain -- docs/assets/readme/      # what CI does next: empty means no drift

Writes a light and a dark SVG per chart to docs/assets/readme/, for the
README's `<picture>` elements. Every plotted value is read from these files,
and none is typed here:

- egress-utilisation: benchmarks/measurements/483-tcp-baseline/points.csv
- policed-uplink:     benchmarks/measurements/663-policed-uplink/runs.csv
- kafka-failover:     benchmarks/kafka_compare/results/failover-20261111T032549Z/failover.csv
- kafka-throughput:   benchmarks/kafka_compare/results/20261109T232328Z/results.csv

The Kafka runs go through kafka_compare's own loaders (which refuse a partial
or mixed run) and its own summaries, so a number here is the number
docs/benchmarks/ prints for the same cell.

`facts()` is the other half: the figures the README's prose quotes, computed
from the same files. tests/test_readme_charts.py fails when the README stops
quoting one of them, so the text cannot drift from the data either. It needs
no matplotlib.

Refuses, exit 2, writing nothing: a plotting stack other than the pinned one,
or a DejaVu Sans from outside matplotlib (the same checks as regenerate.py,
because a different matplotlib moves every glyph). Refuses, exit 2, after
writing: a committed file under docs/assets/readme/ that no chart writes.

Output is byte-identical for given inputs and pinned stack: a fixed SVG hash
salt, glyphs drawn as paths, and no date or software banner in the metadata.
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
from typing import Any

HERE = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, HERE)

from kafka_compare import failover_report, failover_schema, report, schema  # noqa: E402
from kafka_compare import regenerate as kc_regenerate  # noqa: E402
from kafka_compare.failover_stats import FAULT_LABELS  # noqa: E402

REPO = os.path.dirname(HERE)
OUT = "docs/assets/readme"
EGRESS = "benchmarks/measurements/483-tcp-baseline/points.csv"
POLICED = "benchmarks/measurements/663-policed-uplink/runs.csv"
THROUGHPUT_RUN = "benchmarks/kafka_compare/results/20261109T232328Z"
FAILOVER_RUN = "benchmarks/kafka_compare/results/failover-20261111T032549Z"

# Categorical slots 1 and 2 of the reference palette, light and dark steps,
# validated (CVD and contrast) against GitHub's light #ffffff and dark #0d1117
# page backgrounds. The images are transparent, so the page shows through.
THEMES: dict[str, dict[str, str]] = {
    "light": {"ink": "#0b0b0b", "ink2": "#52514e", "grid": "#e1e0d9", "axis": "#c3c2b7",
              "s1": "#2a78d6", "s2": "#eb6834", "muted": "#898781"},
    "dark": {"ink": "#f0efec", "ink2": "#c3c2b7", "grid": "#30363d", "axis": "#484f58",
             "s1": "#3987e5", "s2": "#d95926", "muted": "#898781"},
}

FAULT_ORDER = ("kill9_leader", "stop_leader", "partition_leader")


class Refused(Exception):
    """The charts cannot be vouched for; the message says why."""


# ------------------------------------------------------------------------ data


def _read_csv(rel: str, repo: str = REPO) -> list[dict[str, str]]:
    with open(os.path.join(repo, rel), newline="") as handle:
        return list(csv.DictReader(handle))


def egress_points(repo: str = REPO) -> dict[str, Any]:
    """The #483 grid on the two queue-backed 100 ms profiles, and the shipped default's confirmation runs.

    {"grid": {(profile, pif): [(c, median, min, max), ...]}, "default": {profile: (median, min, max, runs)}}
    """
    grid: dict[tuple[str, int], list[tuple[int, float, float, float]]] = {}
    default: dict[str, tuple[float, float, float, int]] = {}
    grid_runs: set[int] = set()
    for row in _read_csv(EGRESS, repo):
        if row["profile"] not in ("wan100", "wan100-loss1"):
            continue
        values = (float(row["fraction_median"]), float(row["fraction_min"]), float(row["fraction_max"]))
        if row["phase"] == "grid":
            grid.setdefault((row["profile"], int(row["pif"])), []).append((int(row["c"]), *values))
            grid_runs.add(int(row["runs"]))
        elif row["phase"] == "confirm" and row["config"] == "default":
            default[row["profile"]] = (*values, int(row["runs"]))
    for points in grid.values():
        points.sort()
    if set(default) != {"wan100", "wan100-loss1"}:
        raise Refused(f"{EGRESS}: no default confirmation row for {sorted({'wan100', 'wan100-loss1'} - set(default))}")
    if len(grid_runs) != 1:
        raise Refused(f"{EGRESS}: grid points ran {sorted(grid_runs)} times; the chart's caption names one count")
    return {"grid": grid, "default": default, "grid_runs": grid_runs.pop()}


def _link(run_id: str) -> str:
    if "policed" in run_id:
        return "policed"
    return "loss1" if "loss1" in run_id else "clean"


def policed_runs(repo: str = REPO) -> dict[tuple[str, str], dict[str, int]]:
    """Objects committed and seeded per (link, before|after) from #663's runs.csv.

    `main*` tags ran an unchanged origin/main engine (before #666); `fix*` tags ran the fix.
    """
    out: dict[tuple[str, str], dict[str, int]] = {}
    for row in _read_csv(POLICED, repo):
        tag = row["tag"]
        if tag.startswith("main"):
            when = "before"
        elif tag.startswith("fix"):
            when = "after"
        else:
            raise Refused(f"{POLICED}: tag {tag!r} is neither a main* (before) nor a fix* (after) run")
        cell = out.setdefault((_link(row["run_id"]), when), {"committed": 0, "seeded": 0, "runs": 0, "per_run": set()})
        cell["committed"] += int(row["committed"])
        cell["seeded"] += int(row["seeded"])
        cell["runs"] += 1
        cell["per_run"].add(int(row["seeded"]))
    return out


def failover(repo: str = REPO) -> dict[str, Any]:
    run = failover_schema.load_failover_run(os.path.join(repo, FAILOVER_RUN))
    rows = run.rows
    cells = {}
    per_fault = {}
    for fault in FAULT_ORDER:
        for system in ("kafka", "vtop"):
            cells[(fault, system)] = failover_report.summary(rows, system, fault, "unavailability_ms")
        mine = [row for row in rows if row["fault"] == fault]
        per_fault[fault] = {
            "repetitions": len(mine),
            "read_back": sum(row["verify"] == "passed" for row in mine),
            "lost": sum(int(float(row["acked_lost"])) for row in mine if row["acked_lost"]),
            "duplicates": sum(int(float(row["duplicates"])) for row in mine if row["duplicates"]),
        }
    per_cell = {len(failover_report.all_rows(rows, s, f)) for f in FAULT_ORDER for s in ("kafka", "vtop")}
    if len(per_cell) != 1:
        raise Refused(f"{FAILOVER_RUN}: repetitions per fault and system differ ({sorted(per_cell)})")
    return {
        "cells": cells,
        "per_fault": per_fault,
        "per_cell": per_cell.pop(),
        "repetitions": len(rows),
        "ok": sum(row["status"] == "ok" for row in rows),
        "read_back": sum(row["verify"] == "passed" for row in rows),
        "acked": sum(int(row["records_acked"]) for row in rows if row["records_acked"]),
        "lost": sum(int(float(row["acked_lost"])) for row in rows if row["acked_lost"]),
        "duplicates": sum(int(float(row["duplicates"])) for row in rows if row["duplicates"]),
    }


def throughput(repo: str = REPO) -> dict[str, Any]:
    run = schema.load_run(os.path.join(repo, THROUGHPUT_RUN))
    sizes = run.environment["matrix"]["record_sizes"]
    cells = {}
    for size in sizes:
        for system in ("kafka", "vtop"):
            cells[(size, system, "mib")] = report.cell_summary(run.rows, system, "saturation", size,
                                                               "mib_per_sec")["summary"]
            cells[(size, system, "rps")] = report.cell_summary(run.rows, system, "saturation", size,
                                                               "records_per_sec")["summary"]
    return {"sizes": sizes, "cells": cells, "cpus": run.environment["comparable"]["node_limits"]["cpus"],
            "repetitions": run.environment["matrix"]["repetitions"]}


def facts(repo: str = REPO) -> dict[str, str]:
    """Every figure the README's prose quotes, as the README spells it."""
    egress = egress_points(repo)
    clean, lossy = egress["default"]["wan100"], egress["default"]["wan100-loss1"]
    single_lossy = egress["grid"][("wan100-loss1", 1)][0]
    policed = policed_runs(repo)
    fo = failover(repo)
    tp = throughput(repo)
    ratios = [tp["cells"][(size, "kafka", "rps")].median / tp["cells"][(size, "vtop", "rps")].median
              for size in tp["sizes"]]
    out = {}
    for (fault, system), cell in fo["cells"].items():
        out[f"unavailable_{fault}_{system}"] = _seconds(cell.median)
    for size in tp["sizes"]:
        for system in ("kafka", "vtop"):
            out[f"mib_{size}_{system}"] = f"{tp['cells'][(size, system, 'mib')].median:.2f}"
    return {
        **out,
        "default_clean": f"{clean[0]:.3f}",
        "default_lossy": f"{lossy[0]:.3f}",
        "default_runs": f"{clean[3]} runs",
        "single_stream_lossy": f"{single_lossy[1]:.2f}",
        "policed_before": "{committed} of {seeded}".format(**policed[("policed", "before")]),
        "policed_after": "{committed} of {seeded}".format(**policed[("policed", "after")]),
        # "All N failovers ... read back every acknowledged record" is only true while every one passed.
        "failover_reps": (f"All {fo['repetitions']} failovers" if fo["read_back"] == fo["repetitions"]
                          else f"{fo['read_back']} of {fo['repetitions']} failovers"),
        "failover_acked": f"{fo['acked']:,}",
        "failover_lost": f"{fo['lost']} lost",
        "failover_dups": f"{fo['duplicates']} duplicated",
        "failover_per_system": f"{fo['per_cell'] * len(FAULT_ORDER)} per system",
        "throughput_ratio_low": f"{min(ratios):.1f}x",
        "throughput_ratio_high": f"{max(ratios):.1f}x",
    }


def _seconds(ms: float) -> str:
    return f"{ms / 1000:.1f} s"


# ---------------------------------------------------------------------- charts


def _setup(theme: dict[str, str]):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    matplotlib.rcdefaults()
    matplotlib.rcParams.update({
        "svg.hashsalt": "vtop-readme",
        "svg.fonttype": "path",
        "font.family": "DejaVu Sans",
        "font.size": 9,
        "axes.edgecolor": theme["axis"],
        "axes.linewidth": 0.8,
        "axes.labelcolor": theme["ink2"],
        "axes.titlecolor": theme["ink"],
        "axes.facecolor": "none",
        "figure.facecolor": "none",
        "savefig.facecolor": "none",
        "savefig.transparent": True,
        "xtick.color": theme["ink2"],
        "ytick.color": theme["ink2"],
        "grid.color": theme["grid"],
        "grid.linewidth": 0.8,
        "grid.linestyle": "-",
        "legend.frameon": False,
        "legend.labelcolor": theme["ink2"],
        "path.simplify": False,
    })
    return plt


def _axes(ax) -> None:
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.tick_params(length=0)
    ax.set_axisbelow(True)


def _header(fig, theme, height_in: float, title: str, subtitle: str) -> None:
    fig.text(0.012, 1 - 0.14 / height_in, title, fontsize=12, color=theme["ink"], ha="left", va="top",
             fontweight="bold")
    fig.text(0.012, 1 - 0.42 / height_in, subtitle, fontsize=8.2, color=theme["ink2"], ha="left", va="top",
             linespacing=1.4)


def _footer(fig, theme, text: str) -> None:
    fig.text(0.012, 0.015, text, fontsize=7, color=theme["ink2"], ha="left", va="bottom")


def _save(fig, path: str) -> str:
    fig.savefig(path, format="svg", metadata={"Date": None, "Creator": None})
    return path


def egress_chart(plt, theme, data) -> Any:
    width_in, height_in = 9.0, 4.3
    fig, axes = plt.subplots(1, 2, figsize=(width_in, height_in), sharey=True)
    fig.subplots_adjust(left=0.075, right=0.985, top=1 - 1.25 / height_in, bottom=0.82 / height_in, wspace=0.08)
    styles = {"wan100": ("clean", theme["s1"], "o"), "wan100-loss1": ("1% burst loss", theme["s2"], "s")}
    for ax, pif in zip(axes, (1, 4)):
        _axes(ax)
        ax.grid(axis="y")
        ax.axhline(1.0, color=theme["ink2"], linewidth=0.8)
        for profile, (label, color, marker) in styles.items():
            points = data["grid"][(profile, pif)]
            xs = [p[0] for p in points]
            ax.plot(xs, [p[1] for p in points], color=color, linewidth=2, marker=marker, markersize=5,
                    label=label, zorder=3)
            ax.vlines(xs, [p[2] for p in points], [p[3] for p in points], color=color, linewidth=1, zorder=2)
        ax.set_xscale("log", base=2)
        ax.set_xticks([1, 2, 4, 8, 16])
        ax.set_xticklabels(["1", "2", "4", "8", "16"])
        ax.set_xlim(0.8, 20)
        ax.set_ylim(0, 1.1)
        ax.set_yticks([0, 0.25, 0.5, 0.75, 1.0])
        ax.set_xlabel("objects uploaded at once (max_concurrency)")
        title = "1 part in flight per object" if pif == 1 else "4 parts in flight per object (the default)"
        ax.set_title(title, loc="left", fontsize=9, color=theme["ink"])
    axes[0].set_ylabel("share of the link carried")
    # Direct labels where the two lines are furthest apart.
    left = axes[0]
    for profile, (label, _color, _m) in styles.items():
        first = data["grid"][(profile, 1)][0]
        left.annotate(label, (first[0], first[1]), xytext=(8, -3 if profile == "wan100" else 0),
                      textcoords="offset points", fontsize=8.5, color=theme["ink"], va="center")
    # The shipped default's own confirmation runs, beside the grid point it shares a shape with.
    right = axes[1]
    for profile, shift in (("wan100", 0.9), ("wan100-loss1", 1.11)):
        median, low, high, _runs = data["default"][profile]
        label, color, _m = styles[profile]
        right.vlines(8 * shift, low, high, color=theme["ink"], linewidth=1, zorder=4)
        right.plot(8 * shift, median, marker="D", markersize=7, markerfacecolor=color,
                   markeredgecolor=theme["ink"], markeredgewidth=1.2, linestyle="none", zorder=5)
    clean, lossy = data["default"]["wan100"], data["default"]["wan100-loss1"]
    right.text(18, 0.70, f"shipped default, {clean[3]} runs each:\nclean {clean[0]:.3f}, "
               f"1% loss {lossy[0]:.3f}", ha="right", va="top", fontsize=8, color=theme["ink"],
               linespacing=1.4)
    right.text(19, 1.015, "link", ha="right", va="bottom", fontsize=7.5, color=theme["ink2"])
    handles = [plt.Line2D([], [], color=c, marker=m, linewidth=2, markersize=5, label=lab)
               for lab, c, m in styles.values()]
    handles.append(plt.Line2D([], [], color=theme["ink"], marker="D", markerfacecolor="none", linestyle="none",
                              markersize=6, label="shipped default"))
    fig.legend(handles=handles, loc="upper left", ncol=3, fontsize=8.2, bbox_to_anchor=(0.065, 1 - 0.80 / height_in),
               handlelength=1.6, columnspacing=1.6)
    _header(fig, theme, height_in, "Concurrent uploads fill a lossy 100 ms link",
            "Goodput as a share of a 10 Mbit/s, 100 ms emulated link (1.0 = the iperf3-measured link). "
            f"Line: median of {data['grid_runs']} runs per point; whisker: min-max.")
    _footer(fig, theme, f"Data: {EGRESS} (#483). Single host, Lima VM, netem delay and loss, tbf rate, "
            "one-BDP queue.")
    return fig


POLICED_LINKS = (
    ("policed", "10 Mbit/s policer, no queue"),
    ("clean", "10 Mbit/s, 100 ms, clean"),
    ("loss1", "10 Mbit/s, 100 ms, 1% loss"),
)


def policed_chart(plt, theme, data) -> Any:
    width_in, height_in = 9.0, 3.5
    fig, ax = plt.subplots(figsize=(width_in, height_in))
    fig.subplots_adjust(left=0.24, right=0.84, top=1 - 1.2 / height_in, bottom=0.8 / height_in)
    _axes(ax)
    ax.grid(axis="x")
    colors = {"before": theme["muted"], "after": theme["s1"]}
    labels, positions = [], []
    bar = 0.3
    for index, (link, name) in enumerate(POLICED_LINKS):
        base = -index * 1.0
        for offset, when in ((-bar / 2 - 0.02, "before"), (bar / 2 + 0.02, "after")):
            cell = data[(link, when)]
            share = cell["committed"] / cell["seeded"]
            y = base - offset
            ax.barh(y, share * 100, height=bar, color=colors[when], linewidth=0)
            runs = f"{cell['runs']} run" + ("s" if cell["runs"] != 1 else "")
            ax.text(share * 100 + 1.2, y, f"{cell['committed']} of {cell['seeded']} ({runs})",
                    va="center", ha="left", fontsize=8, color=theme["ink"])
        labels.append(name)
        positions.append(base)
    ax.set_yticks(positions)
    ax.set_yticklabels(labels, color=theme["ink"])
    ax.set_xlim(0, 100)
    ax.set_xticks([0, 25, 50, 75, 100])
    ax.set_xticklabels(["0%", "25%", "50%", "75%", "100%"])
    per_run = set().union(*(cell["per_run"] for cell in data.values()))
    if len(per_run) != 1:
        raise Refused(f"{POLICED}: runs seeded {sorted(per_run)} objects; the axis label names one count")
    ax.set_xlabel(f"objects committed, of the {per_run.pop()} seeded per run")
    handles = [plt.Rectangle((0, 0), 1, 1, color=colors["before"], label="before #666 (unchanged main)"),
               plt.Rectangle((0, 0), 1, 1, color=colors["after"],
                             label="after #666 (the deadline grows with the body)")]
    fig.legend(handles=handles, loc="upper left", ncol=2, fontsize=8.2, bbox_to_anchor=(0.005, 1 - 0.78 / height_in),
               handlelength=1.2, columnspacing=1.6)
    _header(fig, theme, height_in, "A policed uplink went from 0 committed objects to all of them",
            "Objects committed at the shipped upload defaults, before and after #666. On the two 100 ms links, "
            "every object committed both times.")
    _footer(fig, theme, f"Data: {POLICED} (#663). Single host, Lima VM; per-run goodput and the cause capture "
            "are in the same directory.")
    return fig


def failover_chart(plt, theme, data) -> Any:
    width_in, height_in = 9.0, 3.9
    fig, ax = plt.subplots(figsize=(width_in, height_in))
    fig.subplots_adjust(left=0.25, right=0.8, top=1 - 1.25 / height_in, bottom=0.8 / height_in)
    _axes(ax)
    ax.grid(axis="x")
    styles = {"kafka": ("Apache Kafka (JVM client)", theme["s1"], "o"),
              "vtop": ("VTOP (native client)", theme["s2"], "s")}
    positions, labels = [], []
    top = 0.0
    for index, fault in enumerate(FAULT_ORDER):
        base = -index * 1.0
        for offset, system in ((-0.17, "kafka"), (0.17, "vtop")):
            cell = data["cells"][(fault, system)]
            y = base - offset
            _label, color, marker = styles[system]
            ax.hlines(y, cell.low / 1000, cell.high / 1000, color=color, linewidth=1.5)
            ax.plot(cell.median / 1000, y, marker=marker, markersize=7, color=color, linestyle="none")
            ax.text(cell.high / 1000 + 0.6, y, _seconds(cell.median), va="center", fontsize=8, color=theme["ink"])
            top = max(top, cell.high / 1000)
        positions.append(base)
        labels.append(FAULT_LABELS[fault])
    ax.set_yticks(positions)
    ax.set_yticklabels(labels, color=theme["ink"])
    ax.set_xlim(0, top * 1.12)
    ax.set_xlabel("seconds without an acknowledgement after the fault (dot: median; line: min-max)")
    for index, fault in enumerate(FAULT_ORDER):
        tally = data["per_fault"][fault]
        ax.text(1.02, -index * 1.0, f"lost {tally['lost']}, duplicated {tally['duplicates']}\n"
                f"{tally['read_back']} of {tally['repetitions']} read back in full", transform=ax.get_yaxis_transform(),
                va="center", ha="left", fontsize=7.8, color=theme["ink2"], linespacing=1.35)
    handles = [plt.Line2D([], [], color=c, marker=m, linewidth=1.5, markersize=6, label=lab)
               for lab, c, m in styles.values()]
    fig.legend(handles=handles, loc="upper left", ncol=2, fontsize=8.2, bbox_to_anchor=(0.005, 1 - 0.80 / height_in),
               handlelength=1.6, columnspacing=1.6)
    _header(fig, theme, height_in, "Failover: Kafka resumes sooner; neither lost an acknowledged record",
            f"{data['repetitions']} leader failovers under load, {data['per_cell']} per fault per system, "
            f"three nodes each, {data['acked']:,} acknowledged records read back.")
    _footer(fig, theme, f"Data: {FAILOVER_RUN}/failover.csv (#597). Single host, Docker Desktop; "
            "the two clients are different programs.")
    return fig


def throughput_chart(plt, theme, data) -> Any:
    width_in, height_in = 9.0, 3.8
    fig, ax = plt.subplots(figsize=(width_in, height_in))
    fig.subplots_adjust(left=0.075, right=0.985, top=1 - 1.25 / height_in, bottom=0.8 / height_in)
    _axes(ax)
    ax.grid(axis="y")
    styles = {"kafka": ("Apache Kafka", theme["s1"]), "vtop": ("VTOP (Kafka gateway)", theme["s2"])}
    width, gap = 0.16, 0.03
    top = 0.0
    ratios = []
    for position, size in enumerate(data["sizes"]):
        for index, system in enumerate(("kafka", "vtop")):
            cell = data["cells"][(size, system, "mib")]
            x = position + (index - 0.5) * (width + gap)
            ax.bar(x, cell.median, width=width, color=styles[system][1], linewidth=0)
            ax.vlines(x, cell.low, cell.high, color=theme["ink2"], linewidth=1)
            ax.text(x, cell.high + 0.8, f"{cell.median:.2f}", ha="center", va="bottom", fontsize=8,
                    color=theme["ink"])
            top = max(top, cell.high)
        ratio = data["cells"][(size, "kafka", "rps")].median / data["cells"][(size, "vtop", "rps")].median
        ratios.append((position, ratio))
    ax.set_xticks(range(len(data["sizes"])))
    ax.set_xticklabels([f"{report.size_label(size)} records" for size in data["sizes"]], color=theme["ink"])
    for position, ratio in ratios:
        ax.text(position, top * 1.2, f"Kafka: {ratio:.1f}x VTOP's records/s", ha="center", va="bottom",
                fontsize=8.2, color=theme["ink"])
    ax.set_xlim(-0.6, len(data["sizes"]) - 0.4)
    ax.set_ylim(0, top * 1.32)
    ax.set_ylabel("MiB/s at saturation")
    handles = [plt.Rectangle((0, 0), 1, 1, color=c, label=lab) for lab, c in styles.values()]
    fig.legend(handles=handles, loc="upper left", ncol=2, fontsize=8.2, bbox_to_anchor=(0.065, 1 - 0.80 / height_in),
               handlelength=1.2, columnspacing=1.6)
    _header(fig, theme, height_in, "Produce throughput: Apache Kafka is an order of magnitude ahead",
            f"Same JVM producer, acks=all, three nodes each at {data['cpus']} CPUs. "
            "A VTOP ack waits for fsync on 2 of 3 replicas; Kafka's, as shipped, does not.")
    _footer(fig, theme, f"Data: {THROUGHPUT_RUN}/results.csv (#596). Bar: median of {data['repetitions']} "
            "repetitions; whisker: min-max. Single host, Docker Desktop.")
    return fig


CHARTS = (
    ("egress-utilisation", egress_chart, egress_points),
    ("policed-uplink", policed_chart, policed_runs),
    ("kafka-failover", failover_chart, failover),
    ("kafka-throughput", throughput_chart, throughput),
)


def render(repo: str = REPO) -> list[str]:
    out_dir = os.path.join(repo, OUT)
    os.makedirs(out_dir, exist_ok=True)
    written = []
    for name, draw, load in CHARTS:
        data = load(repo)
        for mode, theme in THEMES.items():
            plt = _setup(theme)
            fig = draw(plt, theme, data)
            written.append(_save(fig, os.path.join(out_dir, f"{name}-{mode}.svg")))
            plt.close(fig)
    return written


def orphans(written: list[str], repo: str = REPO) -> list[str]:
    """Committed files under OUT that no chart writes: stale output nobody can regenerate."""
    names = subprocess.run(["git", "ls-tree", "-r", "-z", "--name-only", "HEAD", "--", OUT], cwd=repo,
                           check=True, capture_output=True, text=True).stdout.split("\0")
    mine = {os.path.realpath(path) for path in written}
    return sorted(name for name in names if name and os.path.realpath(os.path.join(repo, name)) not in mine)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_args(argv)
    try:
        kc_regenerate.check_stack(kc_regenerate.read_pins())
        kc_regenerate.check_font()
        written = render()
        stale = orphans(written)
        if stale:
            raise Refused("committed output that no chart writes (stale; delete it or restore its chart): "
                          + ", ".join(stale))
    except (Refused, kc_regenerate.Refused, schema.ReportRefused) as exc:
        print(f"readme_charts refused: {exc}", file=sys.stderr)
        return 2
    for path in written:
        print(f"wrote {os.path.relpath(path, REPO)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
