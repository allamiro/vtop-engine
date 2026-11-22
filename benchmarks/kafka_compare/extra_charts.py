"""The end-to-end latency CDF, the resource charts and the profile latency chart (#596 metrics 3 and 4, #600).

Drawn from a schema-3 run loaded by `schema.load_run`: its `e2e` rows and their 1 ms histograms (`run.hist`),
and the `delivered_bytes`, `cpu_seconds` and `disk_write_bytes` columns of its measured rows. Nothing here reads
a file or types a number: a figure is the CSV's, or a quotient of two of the CSV's.

The module is split so the arithmetic is testable without a plotting stack: everything above the "charts"
rule imports no matplotlib, and `report.py` / `crash_report.py` hand the charts the `plt` they set up.

Configurations. The profile latency chart and the crash report speak of three: Kafka as shipped (`defaults`),
Kafka with an fsync on every message (`fsync-parity`), and VTOP as shipped (its one configuration, which
acknowledges after a quorum fsync under either profile). Both Kafka configurations take Kafka's one colour and
are told apart by marker, hatch and line style, never by a second hue.
"""
from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Any

from kafka_compare import colours, e2e, footer, schema, stats
from kafka_compare import report as base

# (system, profile), in the order every chart and table lists them.
CONFIGS = (("kafka", "defaults"), ("kafka", "fsync-parity"), ("vtop", "defaults"))
# The crash harness's names for the same three (crash_analysis.CONFIGS).
CONFIG_NAME = {("kafka", "defaults"): "kafka-defaults", ("kafka", "fsync-parity"): "kafka-fsync-parity",
               ("vtop", "defaults"): "vtop-shipped"}
CONFIG_LABEL = {"kafka-defaults": "Kafka, defaults", "kafka-fsync-parity": "Kafka, fsync on every message",
                "vtop-shipped": "VTOP, as shipped"}
_STYLE = {("kafka", "defaults"): {"marker": "o", "hatch": "", "linestyle": "-"},
          ("kafka", "fsync-parity"): {"marker": "D", "hatch": "////", "linestyle": "--"},
          ("vtop", "defaults"): {"marker": "s", "hatch": "", "linestyle": "-"}}

# The 0 ms bucket is drawn here on a log axis rather than dropped (the CDF's x is the bucket's lower edge).
LOG_FLOOR_MS = 0.5

IMAGES = {"e2e-cdf": "kafka-compare-e2e-cdf", "resources": "kafka-compare-resources",
          "profile-latency": "kafka-compare-profile-latency"}


def label_of(config: tuple[str, str]) -> str:
    return CONFIG_LABEL[CONFIG_NAME[config]]


def config_style(config: tuple[str, str]) -> dict[str, str]:
    """Colour (the registry's, by system), marker, hatch and line style of one configuration."""
    return {"color": colours.colour(config[0], "light"), **_STYLE[config]}


# ------------------------------------------------------------------------------------------ arithmetic


def cdf(hist: dict[int, int]) -> list[tuple[int, float]]:
    """(bucket lower edge in ms, share of records at or under that bucket), in bucket order."""
    total = sum(hist.values())
    if not total:
        raise ValueError("an empty histogram has no CDF")
    out, seen = [], 0
    for bucket, count in sorted(hist.items()):
        seen += count
        out.append((bucket, seen / total))
    return out


def pooled(hists: Iterable[dict[int, int]]) -> dict[int, int]:
    """Several repetitions' histograms as one: the records of all of them."""
    out: dict[int, int] = {}
    for hist in hists:
        for bucket, count in hist.items():
            out[bucket] = out.get(bucket, 0) + count
    return out


def cpu_seconds_per_gib(row: dict[str, str]) -> float:
    return float(row["cpu_seconds"]) / (int(row["delivered_bytes"]) / 2**30)


def write_amplification(row: dict[str, str]) -> float:
    """Bytes the system's three nodes wrote to disk per byte the run delivered."""
    return int(row["disk_write_bytes"]) / int(row["delivered_bytes"])


METRICS = {"cpu_per_gib": (cpu_seconds_per_gib, "CPU seconds per GiB delivered"),
           "write_amp": (write_amplification, "bytes written per byte delivered")}


def value_text(value: float) -> str:
    """A bar's value as text. A zero is the digit 0: a bar of no height must still say what it is."""
    if value == 0:
        return "0"
    if abs(value) >= 1000:
        return base.compact(value)
    if abs(value) >= 100:
        return f"{value:,.0f}"
    if abs(value) >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def range_text(summary: stats.Summary) -> str:
    return f"{value_text(summary.median)} ({value_text(summary.low)}-{value_text(summary.high)})"


def has_e2e(env: dict[str, Any]) -> bool:
    return bool(env["matrix"].get("e2e"))


def has_resources(env: dict[str, Any]) -> bool:
    return bool(env["matrix"].get("resources"))


def has_profile_cost(env: dict[str, Any]) -> bool:
    e2e_config = env["matrix"].get("e2e")
    return isinstance(e2e_config, dict) and bool(e2e_config.get("extra_profiles"))


def basis_text(env: dict[str, Any]) -> str:
    """What the end-to-end offered rate is a share of, from the matrix."""
    config = env["matrix"]["e2e"]
    who = "the slower system's" if config["load_basis"] == "common" else "Kafka's"
    return f"{config['load_pct']}% of {who} median saturation for the cell"


def configs(rows: list[dict[str, str]]) -> list[tuple[str, str]]:
    """The configurations that have end-to-end rows, in CONFIGS order."""
    present = {(row["system"], row["profile"]) for row in rows if row["mode"] == "e2e"}
    return [config for config in CONFIGS if config in present]


def e2e_cell(rows: list[dict[str, str]], system: str, profile: str, size: int, column: str) -> dict[str, Any]:
    """Median and min-max of `column` over the ok repetitions of one configuration and record size."""
    selected = schema.select(rows, system=system, mode="e2e", profile=profile, record_size=size)
    ok = [row for row in selected if row["status"] == "ok"]
    out: dict[str, Any] = {"n": len(selected), "ok": len(ok), "not_ok": [r for r in selected if r["status"] != "ok"],
                           "summary": None, "offered": None, "records": None,
                           "sustained": bool(selected) and all(r["status"] == "ok" and r["sustained"] == "true"
                                                               for r in selected)}
    if ok:
        out["summary"] = stats.summarize([float(row[column]) for row in ok])
        out["records"] = stats.median([float(row["records"]) for row in ok])
    rated = [row for row in selected if row["offered_rate"]]
    if rated:
        out["offered"] = int(float(rated[0]["offered_rate"]))
    return out


def resource_cell(rows: list[dict[str, str]], system: str, mode: str, size: int, metric: str,
                  load_pct: int | None = None, profile: str = "defaults") -> dict[str, Any]:
    """Median and min-max over the ok repetitions of a per-delivered-unit resource figure: each repetition is
    divided by its own delivered bytes before the median is taken."""
    criteria: dict[str, Any] = {"system": system, "mode": mode, "record_size": size, "profile": profile}
    if load_pct is not None:
        criteria["load_pct"] = load_pct
    selected = schema.select(rows, **criteria)
    ok = [row for row in selected if row["status"] == "ok" and row["delivered_bytes"] != ""]
    out: dict[str, Any] = {"n": len(selected), "ok": len(ok), "not_ok": [r for r in selected if r["status"] != "ok"],
                           "summary": None}
    if ok:
        function = METRICS[metric][0]
        out["summary"] = stats.summarize([function(row) for row in ok])
    return out


def _no_result(cell: dict[str, Any]) -> str:
    reason = cell["not_ok"][0]["note"] if cell["not_ok"] else "no rows"
    return f"no result ({base.md_cell(reason[:60])})"


# --------------------------------------------------------------------------------------------- page text


def _table(header: list[str], body: list[list[Any]]) -> list[str]:
    return ["| " + " | ".join(header) + " |", "|" + "---|" * len(header),
            *("| " + " | ".join(str(c) for c in line) + " |" for line in body)]


def _sizes(env: dict[str, Any]) -> list[int]:
    return env["matrix"]["record_sizes"]


def e2e_section(run: schema.Run, images: dict[str, str], hist_rel: str) -> list[str]:
    rows, env = run.rows, run.environment
    cfgs = configs(rows)
    out = ["## End-to-end latency", "", f"![End-to-end latency CDF]({images['e2e-cdf']})", ""]
    out.append("Latency from a record's `CreateTime` (the producer stamps it when `send()` is called, so batching "
               "and linger time are inside it) to the moment the consumer's `poll()` returned it, both read from "
               "the one host's clock and counted into 1 ms buckets (a `CreateTime` has millisecond resolution, "
               f"so no finer is possible). The histograms are in [`e2e_hist.csv`]({hist_rel}); every percentile "
               "below is read from them. Every configuration was offered the same rate, "
               f"{basis_text(env)}, so the latencies are read at one load. The thick line is the pooled "
               "records of every repetition, the thin ones are the repetitions.")
    out.append("")
    body = []
    for size in _sizes(env):
        for config in cfgs:
            cells = {col: e2e_cell(rows, config[0], config[1], size, col) for col in e2e.PERCENTILES}
            first = cells["lat_p50_ms"]
            sustained = ("yes" if first["sustained"] else "**no**") if first["summary"] is not None else "-"
            body.append([base.size_label(size), label_of(config),
                         base._fmt(first["offered"]) if first["offered"] else "-", sustained,
                         *(base._summary_cell(cells[col]) for col in e2e.PERCENTILES),
                         base._fmt(first["records"]) if first["records"] is not None else "-"])
    out += _table(["record size", "configuration", "offered records/s", "sustained", "p50 ms", "p95 ms", "p99 ms",
                   "p99.9 ms", "records consumed"], body)
    out.append("")
    out.append("Median over the repetitions, with the min-max in brackets; percentiles are the lower edge of "
               "the 1 ms bucket that holds the rank. A configuration marked **no** in `sustained` did not hold "
               "the offered rate in every repetition (achieved more than 5% short), and its curve is drawn "
               "dotted: it measures a queue in the client as much as the system.")
    out.append("")
    for size in _sizes(env):
        parts = []
        for config in cfgs:
            cell = e2e_cell(rows, config[0], config[1], size, "lat_p99_ms")
            if cell["summary"] is not None:
                parts.append(f"{label_of(config)} {base._fmt(cell['summary'].median)} ms "
                             f"({base._fmt(cell['summary'].low)}-{base._fmt(cell['summary'].high)})")
        if parts:
            out.append(f"- {base.size_label(size)} records, p99 end to end: " + "; ".join(parts) + ".")
    out.append("")
    return out


def resources_section(run: schema.Run, images: dict[str, str]) -> list[str]:
    rows, env = run.rows, run.environment
    out = ["## CPU and bytes written", "", f"![CPU and bytes written]({images['resources']})", ""]
    out.append("What each system's three nodes spent, from their own cgroup v2 counters (`cpu.stat` usage and "
               "`io.stat` bytes, summed over the nodes), read before and after each measured run. A node is "
               "`sync`ed before each reading and the closing reading waits 2 s for write-back, so what a node "
               "had acknowledged but not yet written (Kafka under `defaults` acknowledges before it flushes) "
               "is counted in the run that caused it. \"Delivered\" is the bytes the run produced, or for "
               "a consume run read. Bytes written per byte delivered includes replication to all three nodes "
               "and each system's log and metadata overhead. The chart draws the saturation runs and the "
               "end-to-end runs (one equal offered load for both systems); the table also lists the "
               "fixed-rate and consume runs.")
    out.append("")
    body = []
    modes = [("saturation", None), *(("fixed_rate", p) for p in env["matrix"]["load_pcts"]), ("e2e", None),
             *([("consume", None)] if env["matrix"].get("consume") else [])]
    for size in _sizes(env):
        for mode, pct in modes:
            for system in ("kafka", "vtop"):
                cells = [resource_cell(rows, system, mode, size, metric, pct) for metric in METRICS]
                label = mode.replace("_", " ") + (f" {pct}%" if pct else "")
                body.append([base.size_label(size), label, base.SYSTEM_STYLE[system]["label"],
                             *(range_text(c["summary"]) if c["summary"] is not None else _no_result(c)
                               for c in cells)])
    out += _table(["record size", "run", "system", "CPU seconds per GiB delivered",
                   "bytes written per byte delivered"], body)
    out.append("")
    out.append("Median over the repetitions, min-max in brackets.")
    out.append("")
    for size in _sizes(env):
        for metric, (_f, said) in METRICS.items():
            k = resource_cell(rows, "kafka", "saturation", size, metric)["summary"]
            v = resource_cell(rows, "vtop", "saturation", size, metric)["summary"]
            if k is not None and v is not None and k.median and v.median:
                high, low = ("Kafka", "VTOP") if k.median >= v.median else ("VTOP", "Kafka")
                factor = max(k.median, v.median) / min(k.median, v.median)
                out.append(f"- {base.size_label(size)} at saturation, {said}: {high} spends {factor:.1f}x "
                           f"{low}'s median.")
    out.append("")
    return out


def profile_section(run: schema.Run, images: dict[str, str]) -> list[str]:
    rows, env = run.rows, run.environment
    cfgs = configs(rows)
    out = ["## What each durability profile costs in latency", "",
           f"![Latency by durability profile]({images['profile-latency']})", ""]
    out.append("The same end-to-end runs as above, one bar per configuration: Kafka as shipped, Kafka with an "
               "fsync on every message (so an acknowledgement waits for the in-sync replicas to flush), and VTOP "
               "as shipped, whose quorum acknowledgement already waits for an fsync. This is what the extra "
               "durability costs in latency, to be read beside what the crash report shows each of them losing. "
               "VTOP has one configuration, so it is measured once.")
    out.append("")
    body = []
    for size in _sizes(env):
        for config in cfgs:
            p50 = e2e_cell(rows, config[0], config[1], size, "lat_p50_ms")
            p99 = e2e_cell(rows, config[0], config[1], size, "lat_p99_ms")
            body.append([base.size_label(size), label_of(config),
                         ("yes" if p50["sustained"] else "**no**") if p50["summary"] is not None else "-",
                         base._summary_cell(p50), base._summary_cell(p99)])
    out += _table(["record size", "configuration", "sustained", "p50 ms", "p99 ms"], body)
    out.append("")
    for size in _sizes(env):
        a = e2e_cell(rows, "kafka", "defaults", size, "lat_p99_ms")["summary"]
        b = e2e_cell(rows, "kafka", "fsync-parity", size, "lat_p99_ms")["summary"]
        if a is not None and b is not None and a.median and b.median:
            out.append(f"- {base.size_label(size)} records: an fsync on every message moves Kafka's median p99 from "
                       f"{base._fmt(a.median)} ms to {base._fmt(b.median)} ms, {b.median / a.median:.1f}x.")
    out.append("")
    return out


def sections(run: schema.Run, images: dict[str, str], hist_rel: str) -> list[str]:
    """The new page sections a run's data supports, in page order; empty for a run without any."""
    out: list[str] = []
    env = run.environment
    if has_e2e(env):
        out += e2e_section(run, images, hist_rel)
    if has_resources(env):
        out += resources_section(run, images)
    if has_profile_cost(env):
        out += profile_section(run, images)
    return out


# ----------------------------------------------------------------------------------------------- charts


def _step_points(hist: dict[int, int]) -> tuple[list[float], list[float]]:
    points = cdf(hist)
    xs = [max(float(ms), LOG_FLOOR_MS) for ms, _ in points]
    return [xs[0], *xs], [0.0, *(share for _, share in points)]


def e2e_cdf_chart(plt, run: schema.Run, img_dir: str) -> list[str]:
    from matplotlib.lines import Line2D

    rows, env = run.rows, run.environment
    sizes, cfgs = _sizes(env), configs(rows)
    width_in, height_in = 4.7 * len(sizes) + 0.9, 5.6
    fig, axes = plt.subplots(1, len(sizes), figsize=(width_in, height_in), squeeze=False, sharey=True)
    fig.subplots_adjust(left=0.075 if len(sizes) > 1 else 0.12, right=0.99, top=1 - 1.95 / height_in,
                        bottom=1.4 / height_in, wspace=0.07)
    held_back: set[str] = set()
    for ax, size in zip(axes[0], sizes):
        base._style_axes(ax)
        ax.set_xscale("log")
        ax.grid(axis="both", which="major")
        right = LOG_FLOOR_MS
        for config in cfgs:
            style = config_style(config)
            cell = e2e_cell(rows, config[0], config[1], size, "lat_p50_ms")
            ok = schema.select(rows, system=config[0], mode="e2e", profile=config[1], record_size=size,
                               status="ok")
            hists = [run.hist[schema.row_key(row)] for row in ok]
            line = style["linestyle"] if cell["sustained"] else ":"
            if not cell["sustained"] and cell["summary"] is not None:
                held_back.add(label_of(config))
            for hist in hists:
                xs, ys = _step_points(hist)
                ax.plot(xs, ys, color=style["color"], linewidth=0.8, alpha=0.35, linestyle=line,
                        drawstyle="steps-post", zorder=2)
                right = max(right, xs[-1])
            if hists:
                xs, ys = _step_points(pooled(hists))
                ax.plot(xs, ys, color=style["color"], linewidth=2.2, linestyle=line, drawstyle="steps-post",
                        zorder=3)
        for share, name in ((0.5, "p50"), (0.99, "p99")):
            ax.axhline(share, color=base.INK_2, linewidth=0.8, linestyle=(0, (1, 3)), zorder=1)
            ax.text(0.99, share - 0.035, name, transform=ax.get_yaxis_transform(), fontsize=7, color=base.INK_2,
                    ha="right")
        ax.set_xlim(LOG_FLOOR_MS * 0.8, right * 1.5)
        ax.set_ylim(0, 1.03)
        ax.set_yticks([0, 0.25, 0.5, 0.75, 1.0])
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p: f"{v * 100:.0f}%"))
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p: base._tick(v)))
        ax.xaxis.set_minor_formatter(plt.NullFormatter())
        offered = next((c["offered"] for c in (e2e_cell(rows, cf[0], cf[1], size, "lat_p50_ms") for cf in cfgs)
                        if c["offered"]), None)
        ax.set_title(f"{base.size_label(size)} records" + (f", {base.compact(offered)}/s offered" if offered else ""),
                     loc="left", fontsize=9)
        ax.set_xlabel("end-to-end latency, ms (log scale; 0 ms drawn at 0.5)")
    axes[0][0].set_ylabel("share of records delivered within")
    handles = []
    for config in cfgs:
        style = config_style(config)
        handles.append(Line2D([], [], color=style["color"], linewidth=2.2, linestyle=style["linestyle"],
                              label=label_of(config)))
    if held_back:
        handles.append(Line2D([], [], color=base.INK_2, linewidth=2.0, linestyle=":",
                              label="dotted: offered rate NOT held (" + ", ".join(sorted(held_back)) + ")"))
    fig.legend(handles=handles, loc="upper left", ncol=min(len(handles), 3), fontsize=8, handlelength=2.6,
               columnspacing=1.6, bbox_to_anchor=(0.06, 1 - 1.05 / height_in))
    base._header(fig, height_in, "End-to-end latency, produce to consume: the distribution, VTOP vs Apache Kafka",
                 base._wrap(f"Cumulative share of records by latency from the producer's CreateTime to the "
                            f"consumer's poll(), 1 ms buckets. Offered: {basis_text(env)}, the same rate for every "
                            "configuration. Thick: all repetitions pooled; thin: each repetition.", 150))
    text = footer.footer_text(env)
    base._footer(fig, text, 230)
    paths = base._save(fig, img_dir, IMAGES["e2e-cdf"], text)
    plt.close(fig)
    return paths


def _bars(ax, x: float, summary: stats.Summary | None, width: float, style: dict[str, str], label: str | None,
          cell: dict[str, Any], top: float) -> float:
    """One bar with its whisker and value label; a zero is drawn as a label on the baseline. Returns the
    highest value drawn."""
    if summary is None:
        status = cell["not_ok"][0]["status"] if cell["not_ok"] else "missing"
        ax.text(x, 0, f" no result ({status})", rotation=90, ha="center", va="bottom", fontsize=7,
                color=base.INK_2)
        return top
    hatch = {"hatch": style["hatch"], "edgecolor": base.SURFACE} if style["hatch"] else {}
    ax.bar(x, summary.median, width=width, facecolor=style["color"], linewidth=0, label=label, **hatch)
    ax.errorbar(x, summary.median, yerr=[[summary.median - summary.low], [summary.high - summary.median]],
                fmt="none", ecolor=base.INK_2, elinewidth=1, capsize=3, capthick=1)
    text = value_text(summary.median)
    if cell.get("ok") is not None and cell["ok"] < cell["n"]:
        text += f"\n({cell['ok']}/{cell['n']} reps)"
    ax.annotate(text, (x, summary.high), xytext=(0, 4), textcoords="offset points", ha="center", va="bottom",
                fontsize=7.5, color=base.INK)
    return max(top, summary.high)


def resources_chart(plt, run: schema.Run, img_dir: str) -> list[str]:
    rows, env = run.rows, run.environment
    sizes = _sizes(env)
    modes = (("saturation", "at saturation"), ("e2e", "at the end-to-end offered load"))
    width_in, height_in = 10.0, 8.6
    fig, axes = plt.subplots(2, 2, figsize=(width_in, height_in))
    fig.subplots_adjust(left=0.085, right=0.985, top=1 - 1.35 / height_in, bottom=1.1 / height_in, wspace=0.28,
                        hspace=0.42)
    width, gap = 0.2, 0.03
    for r, (mode, mode_label) in enumerate(modes):
        for c, (metric, (_f, said)) in enumerate(METRICS.items()):
            ax = axes[r][c]
            base._style_axes(ax)
            ax.grid(axis="y")
            top = 0.0
            for index, system in enumerate(("kafka", "vtop")):
                style = {"color": base.SYSTEM_STYLE[system]["color"], "hatch": ""}
                for position, size in enumerate(sizes):
                    cell = resource_cell(rows, system, mode, size, metric)
                    top = _bars(ax, position + (index - 0.5) * (width + gap), cell["summary"], width, style,
                                base.SYSTEM_STYLE[system]["label"] if position == 0 else None, cell, top)
            ax.set_xticks(range(len(sizes)))
            ax.set_xticklabels([f"{base.size_label(s)} records" for s in sizes])
            ax.set_xlim(-0.6, len(sizes) - 0.4)
            ax.set_ylim(0, top * 1.22 if top else 1)
            ax.set_ylabel(said)
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p: "0" if v == 0 else (
                base.compact(v) if v >= 10_000 else f"{v:g}")))
            ax.set_title(f"{said}, {mode_label}", loc="left", fontsize=8.3, color=base.INK_2)
    handles = [plt.Rectangle((0, 0), 1, 1, color=base.SYSTEM_STYLE[s]["color"], label=base.SYSTEM_STYLE[s]["label"])
               for s in ("kafka", "vtop")]
    fig.legend(handles=handles, loc="upper left", ncol=2, fontsize=8.5,
               bbox_to_anchor=(0.075, 1 - 0.88 / height_in), handlelength=1.2, columnspacing=1.6)
    base._header(fig, height_in, "CPU and bytes written per byte delivered, VTOP vs Apache Kafka",
                 "Three nodes' cgroup v2 counters summed over each run's window (sync before each reading, 2 s "
                 "settle at the close).\nBar: median of repetitions; whiskers: min-max.")
    text = footer.footer_text(env)
    base._footer(fig, text, 190)
    paths = base._save(fig, img_dir, IMAGES["resources"], text)
    plt.close(fig)
    return paths


def profile_latency_chart(plt, run: schema.Run, img_dir: str) -> list[str]:
    from matplotlib.patches import Patch

    rows, env = run.rows, run.environment
    sizes, cfgs = _sizes(env), configs(rows)
    width_in, height_in = 10.0, 5.4
    fig, axes = plt.subplots(1, 2, figsize=(width_in, height_in))
    fig.subplots_adjust(left=0.08, right=0.985, top=1 - 1.7 / height_in, bottom=1.15 / height_in, wspace=0.24)
    width, gap = 0.12, 0.02
    unheld = False
    for ax, (column, name) in zip(axes, (("lat_p50_ms", "p50"), ("lat_p99_ms", "p99"))):
        base._style_axes(ax)
        ax.grid(axis="y")
        top = 0.0
        for index, config in enumerate(cfgs):
            style = config_style(config)
            for position, size in enumerate(sizes):
                cell = e2e_cell(rows, config[0], config[1], size, column)
                x = position + (index - (len(cfgs) - 1) / 2) * (width + gap)
                top = _bars(ax, x, cell["summary"], width, style, None, cell, top)
                if cell["summary"] is not None and not cell["sustained"]:
                    unheld = True
                    ax.text(x, 0, "*", ha="center", va="bottom", fontsize=11, color=base.INK)
        ax.set_xticks(range(len(sizes)))
        ax.set_xticklabels([f"{base.size_label(s)} records" for s in sizes])
        ax.set_xlim(-0.6, len(sizes) - 0.4)
        ax.set_ylim(0, top * 1.2 if top else 1)
        ax.set_ylabel(f"{name} end-to-end latency, ms")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p: base._tick(v)))
        ax.set_title(f"{name} (bar: median of repetitions; whiskers: min-max)", loc="left", fontsize=8.5,
                     color=base.INK_2)
    handles = []
    for config in cfgs:
        style = config_style(config)
        kw = {"hatch": style["hatch"], "edgecolor": base.SURFACE} if style["hatch"] else {}
        handles.append(Patch(facecolor=style["color"], linewidth=0, label=label_of(config), **kw))
    if unheld:
        handles.append(Patch(facecolor=base.SURFACE, edgecolor=base.INK_2, label="* offered rate NOT held"))
    fig.legend(handles=handles, loc="upper left", ncol=len(handles), fontsize=8.3,
               bbox_to_anchor=(0.07, 1 - 0.95 / height_in), handlelength=1.2, columnspacing=1.5)
    base._header(fig, height_in, "What each durability profile costs in latency, Kafka vs VTOP",
                 base._wrap(f"End-to-end latency at {basis_text(env)}, the same rate for every configuration. "
                            "Kafka's two bars share its colour; the hatched one flushes to disk after every "
                            "message. VTOP's acknowledgement already waits for an fsync.", 150))
    text = footer.footer_text(env)
    base._footer(fig, text, 190)
    paths = base._save(fig, img_dir, IMAGES["profile-latency"], text)
    plt.close(fig)
    return paths


def draw(plt, run: schema.Run, img_dir: str) -> tuple[list[str], dict[str, str]]:
    """The charts a run's data supports: the paths written, and {key: image file name} for the page."""
    env = run.environment
    written: list[str] = []
    keys: list[str] = []
    if has_e2e(env):
        written += e2e_cdf_chart(plt, run, img_dir)
        keys.append("e2e-cdf")
    if has_resources(env):
        written += resources_chart(plt, run, img_dir)
        keys.append("resources")
    if has_profile_cost(env):
        written += profile_latency_chart(plt, run, img_dir)
        keys.append("profile-latency")
    return written, {key: os.path.join(img_dir, f"{IMAGES[key]}.png") for key in keys}
