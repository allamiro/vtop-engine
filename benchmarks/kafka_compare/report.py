#!/usr/bin/env python3
"""Charts and report for the VTOP vs Apache Kafka comparison (#600), from one run's CSV.

    python3 benchmarks/kafka_compare/report.py benchmarks/kafka_compare/results/<run-id>

Reads `results.csv` and `environment.json`, refuses (exit 2, the reason on
stderr) a missing, partial or mixed-environment run (#595 rule 9), and writes
PNG + SVG charts to docs/benchmarks/img/ and docs/benchmarks/kafka-comparison.md.
Every measured number in them (tables, charts, ratios) comes from the CSV; the
run's conditions (machine, limits, images, commit, matrix, dates) come from
environment.json. None is typed here.

A failover run (#597; its directory holds `failover.csv`) is handed to
failover_report.py, which writes docs/benchmarks/kafka-failover.md under the
same rules.

Output is deterministic for a given CSV, environment.json and matplotlib
version: fixed rcParams, a fixed SVG hash salt, and no timestamps or software
banners in the metadata.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Any

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from kafka_compare import schema, stats  # noqa: E402

REPO = os.path.dirname(os.path.dirname(HERE))
DEFAULT_IMG_DIR = os.path.join(REPO, "docs", "benchmarks", "img")
DEFAULT_DOC = os.path.join(REPO, "docs", "benchmarks", "kafka-comparison.md")
DEFAULT_FAILOVER_DOC = os.path.join(REPO, "docs", "benchmarks", "kafka-failover.md")

# One colour per system in every chart: categorical slots 1 and 2 of the
# reference palette, validated for CVD separation (protan dE 24.7) and
# contrast on the light surface. Blue vs orange, never red vs green. The
# images carry their own light surface, so the hue is the same wherever the
# page puts them (light or dark mode).
SYSTEM_STYLE = {
    "kafka": {"label": "Apache Kafka", "color": "#2a78d6", "marker": "o", "hatch": ""},
    "vtop": {"label": "VTOP (Kafka gateway)", "color": "#eb6834", "marker": "s", "hatch": ""},
}
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e2e1dc"
PERCENTILES = (("lat_p50_ms", "p50"), ("lat_p95_ms", "p95"), ("lat_p99_ms", "p99"), ("lat_p999_ms", "p99.9"))
# The perf tool reports percentiles in whole milliseconds; a 0 is "under 1 ms"
# and is drawn at this floor on the log axis rather than dropped.
LOG_FLOOR_MS = 0.5

GATEWAY_GAPS = (
    ("CreateTopics", "kafka-topics.sh --create",
     "UnsupportedVersionException: The node does not support CREATE_TOPICS",
     "The VTOP topic is the range in the node config; Kafka's is created by run.py with kafka-topics.sh."),
    ("DescribeConfigs", "kafka-topics.sh --describe",
     "UnsupportedVersionException: The node does not support DESCRIBE_CONFIGS",
     "Not needed by the perf tools; the topic layout is recorded from the node config instead."),
    ("OffsetCommit (no metadata plane)", "kafka-consumer-perf-test.sh with auto-commit on",
     "Offset commit failed on partition bench-0 at offset <n>: The message format version on the broker "
     "does not support the request (gateway: \"kafka OffsetCommit refused: this gateway has no durable offset "
     "store\")",
     "enable.auto.commit=false for BOTH systems; reads are measured, commits are not."),
    ("Compressed batches", "producer with compression.type=zstd",
     "UnsupportedCompressionTypeException: batch 0: record batch is compressed (codec 4), which this gateway "
     "does not decompress",
     "compression.type=none for both systems; compression is out of phase 1."),
    ("Gateway on followers", "vtop-node with a `kafka` block on a follower",
     "the node refuses a kafka block on any role but leader or standalone (the gateway is served by a leader or "
     "standalone only)",
     "The gateway runs on the range's leader only; one partition means one gateway."),
)


# ----------------------------------------------------------------------------- data


def _float(row: dict[str, str], column: str) -> float:
    return float(row[column])


def _all_sustained(selected: list[dict[str, str]]) -> bool:
    """Whether the offered rate held in EVERY repetition of the cell.

    A failed or refused repetition did not hold it either, so it counts
    against the cell rather than dropping out of the judgement.
    """
    return bool(selected) and all(row["status"] == "ok" and row.get("sustained") == "true" for row in selected)


def cell_summary(rows: list[dict[str, str]], system: str, mode: str, size: int, column: str,
                 load_pct: int | None = None) -> dict[str, Any]:
    """Median/min/max of `column` over the ok repetitions, plus what did not run."""
    criteria: dict[str, Any] = {"system": system, "mode": mode, "record_size": size}
    if load_pct is not None:
        criteria["load_pct"] = load_pct
    selected = schema.select(rows, **criteria)
    ok = [row for row in selected if row["status"] == "ok"]
    not_ok = [row for row in selected if row["status"] != "ok"]
    out: dict[str, Any] = {"n": len(selected), "ok": len(ok), "not_ok": not_ok, "summary": None,
                           "sustained": _all_sustained(selected),
                           "offered": None}
    if ok:
        out["summary"] = stats.summarize([_float(row, column) for row in ok])
        if mode == "fixed_rate":
            out["offered"] = int(float(ok[0]["offered_rate"]))
            out["achieved"] = stats.summarize([_float(row, "records_per_sec") for row in ok])
    elif selected and selected[0].get("offered_rate"):
        out["offered"] = int(float(selected[0]["offered_rate"]))
    return out


def size_label(size: int) -> str:
    return f"{size // 1024} KiB" if size % 1024 == 0 else f"{size} B"


def compact(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 10_000:
        return f"{value / 1000:.0f}K"
    if value >= 1000:
        return f"{value / 1000:.1f}K"
    if value >= 10:
        return f"{value:.0f}"
    return f"{value:.2f}"


def gib(value: Any) -> str:
    """Bytes as whole GiB, or `unknown` when the run could not read them
    (e.g. `host_memory_bytes` is empty on a host without the macOS sysctl)."""
    try:
        size = int(value)
    except (TypeError, ValueError):
        return "unknown"
    return f"{size / 2**30:.0f} GiB" if size > 0 else "unknown"


def footer_text(env: dict[str, Any], run_id: str) -> str:
    c = env["comparable"]
    limits = c["node_limits"]
    return (
        f"{c['host_cpu']} ({c['host_logical_cpus']} logical CPUs, {gib(c.get('host_memory_bytes'))}), "
        f"{c['host_os']} | "
        f"{c['docker_platform']}, VM {c['docker_vm_cpus']} CPUs / {gib(c.get('docker_vm_memory_bytes'))}, "
        f"kernel {c['docker_vm_kernel']}, "
        f"volumes {c['volume_filesystem']}\n"
        f"per node: {limits['cpus']} CPUs, {limits['mem_limit']} RAM | Kafka {c['kafka_image'].split('@')[0]} "
        f"(KRaft, 3 brokers) | VTOP {c['vtop_commit'][:12]} | client {c['client']} | "
        f"profile {c['profile']} | {env['matrix']['repetitions']} repetitions | run {run_id} "
        f"({env['started_at'][:10]}) | single host, indicative, not a production benchmark"
    )


# --------------------------------------------------------------------------- charts


def _setup_matplotlib():
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    matplotlib.rcdefaults()
    matplotlib.rcParams.update({
        "svg.hashsalt": "vtop-kafka-compare",
        "svg.fonttype": "path",
        "font.family": "DejaVu Sans",
        "font.size": 9,
        "axes.edgecolor": GRID,
        "axes.linewidth": 0.8,
        "axes.labelcolor": INK_2,
        "axes.titlecolor": INK,
        "axes.titlesize": 10,
        "axes.facecolor": SURFACE,
        "figure.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "xtick.color": INK_2,
        "ytick.color": INK_2,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "grid.linestyle": "-",
        "legend.frameon": False,
        "path.simplify": False,
    })
    return plt


def _save(fig, img_dir: str, name: str) -> list[str]:
    paths = []
    for ext, metadata in (("png", {"Software": None}), ("svg", {"Date": None, "Creator": None})):
        path = os.path.join(img_dir, f"{name}.{ext}")
        fig.savefig(path, dpi=150, metadata=metadata)
        paths.append(path)
    return paths


def _wrap(text: str, width: int) -> str:
    import textwrap

    return "\n".join(textwrap.fill(line, width) for line in text.split("\n"))


def _header(fig, height_in: float, title: str, subtitle: str) -> None:
    """Title and subtitle pinned in inches from the top, so they never move with the plot."""
    fig.text(0.012, 1 - 0.18 / height_in, title, fontsize=12.5, color=INK, ha="left", va="top")
    fig.text(0.012, 1 - 0.50 / height_in, subtitle, fontsize=8, color=INK_2, ha="left", va="top", linespacing=1.4)


def _footer(fig, text: str, width_chars: int) -> None:
    fig.text(0.012, 0.012, _wrap(text, width_chars), fontsize=6.5, color=INK_2, ha="left", va="bottom",
             linespacing=1.45)


def _style_axes(ax) -> None:
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.tick_params(length=0)
    ax.set_axisbelow(True)


def _mib(value: float) -> str:
    return f"{value:,.0f}" if value >= 10 else f"{value:.2f}"


def _tick(value: float) -> str:
    if value == 0:
        return "0"
    if value >= 10_000:
        return compact(value)
    if value >= 1:
        return f"{value:,.0f}"
    return f"{value:g}"


def throughput_chart(plt, rows, env, run_id, mode: str, title: str, subtitle: str, name: str,
                     img_dir: str) -> list[str]:
    sizes = env["matrix"]["record_sizes"]
    width_in, height_in = 10.0, 5.0
    fig, axes = plt.subplots(1, 2, figsize=(width_in, height_in))
    fig.subplots_adjust(left=0.08, right=0.985, top=1 - 1.55 / height_in, bottom=1.05 / height_in, wspace=0.26)
    metrics = (("records_per_sec", "records/s"), ("mib_per_sec", "MiB/s"))
    # Thin bars (the band's leftover is air) with a surface gap between the pair.
    width, gap = 0.13, 0.02
    for ax, (column, unit) in zip(axes, metrics):
        _style_axes(ax)
        ax.grid(axis="y")
        top = 0.0
        for index, system in enumerate(("kafka", "vtop")):
            style = SYSTEM_STYLE[system]
            for position, size in enumerate(sizes):
                cell = cell_summary(rows, system, mode, size, column)
                x = position + (index - 0.5) * (width + gap)
                summary = cell["summary"]
                if summary is None:
                    # The gap is drawn by name; the reason is in the report's table.
                    status = cell["not_ok"][0]["status"] if cell["not_ok"] else "missing"
                    ax.text(x, 0, f" no result ({status})", rotation=90, ha="center", va="bottom",
                            fontsize=7, color=INK_2)
                    continue
                ax.bar(x, summary.median, width=width, color=style["color"], linewidth=0,
                       label=style["label"] if position == 0 else None)
                ax.errorbar(x, summary.median, yerr=[[summary.median - summary.low], [summary.high - summary.median]],
                            fmt="none", ecolor=INK_2, elinewidth=1, capsize=3, capthick=1)
                label = compact(summary.median) if column == "records_per_sec" else _mib(summary.median)
                if cell["ok"] < cell["n"]:
                    label += f"\n({cell['ok']}/{cell['n']} reps)"
                ax.annotate(label, (x, summary.high), xytext=(0, 4), textcoords="offset points", ha="center",
                            va="bottom", fontsize=7.5, color=INK)
                top = max(top, summary.high)
        ax.set_xticks(range(len(sizes)))
        ax.set_xticklabels([f"{size_label(size)} records" for size in sizes])
        ax.set_xlim(-0.6, len(sizes) - 0.4)
        ax.set_ylim(0, top * 1.15 if top else 1)
        ax.set_ylabel(unit)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p: _tick(v)))
        ax.set_title(f"{unit} (bar: median of repetitions; whiskers: min-max)", loc="left", fontsize=8.5,
                     color=INK_2)
    handles = [plt.Rectangle((0, 0), 1, 1, color=SYSTEM_STYLE[s]["color"], label=SYSTEM_STYLE[s]["label"])
               for s in ("kafka", "vtop")]
    fig.legend(handles=handles, loc="upper left", ncol=2, fontsize=8.5,
               bbox_to_anchor=(0.07, 1 - 0.88 / height_in), handlelength=1.2, columnspacing=1.6)
    _header(fig, height_in, title, subtitle)
    _footer(fig, footer_text(env, run_id), 190)
    paths = _save(fig, img_dir, name)
    plt.close(fig)
    return paths


def latency_chart(plt, rows, env, run_id, img_dir: str) -> list[str]:
    from matplotlib.lines import Line2D

    sizes = env["matrix"]["record_sizes"]
    pcts = env["matrix"]["load_pcts"]
    width_in = 12.0
    height_in = 2.9 * len(sizes) + 2.3
    fig, axes = plt.subplots(len(sizes), len(PERCENTILES), figsize=(width_in, height_in), squeeze=False,
                             sharey=True)
    fig.subplots_adjust(left=0.065, right=0.99, top=1 - 1.6 / height_in, bottom=1.1 / height_in, hspace=0.55,
                        wspace=0.07)
    hollow_seen = False
    high = None
    dodge = {"kafka": -0.07, "vtop": 0.07}
    for r, size in enumerate(sizes):
        for c, (column, label) in enumerate(PERCENTILES):
            ax = axes[r][c]
            _style_axes(ax)
            ax.set_yscale("log")
            ax.grid(axis="y", which="major")
            for system in ("kafka", "vtop"):
                style = SYSTEM_STYLE[system]
                xs, ys = [], []
                for i, pct in enumerate(pcts):
                    x = i + dodge[system]
                    cell = cell_summary(rows, system, "fixed_rate", size, column, pct)
                    summary = cell["summary"]
                    if summary is None:
                        short = "Kafka" if system == "kafka" else "VTOP"
                        ax.text(x, 0.97, f"{short}:\nno\nresult", transform=ax.get_xaxis_transform(), ha="center",
                                va="top", fontsize=6.5, color=INK_2, linespacing=1.1)
                        continue
                    median = max(summary.median, LOG_FLOOR_MS)
                    lo = max(summary.low, LOG_FLOOR_MS)
                    hi = max(summary.high, LOG_FLOOR_MS)
                    high = hi if high is None else max(high, hi)
                    ax.errorbar(x, median, yerr=[[median - lo], [hi - median]], fmt="none", ecolor=INK_2,
                                elinewidth=1, capsize=2.5, capthick=1, zorder=2)
                    sustained = cell["sustained"]
                    hollow_seen = hollow_seen or not sustained
                    ax.plot(x, median, marker=style["marker"], markersize=7.5, zorder=4,
                            markerfacecolor=style["color"] if sustained else SURFACE,
                            markeredgecolor=SURFACE if sustained else style["color"],
                            markeredgewidth=1.6, linestyle="none")
                    if sustained:
                        xs.append(i)
                        ys.append(median)
                # A line joins only consecutive sustained points: a run that
                # fell behind its offer is not a latency AT that offer.
                for j in range(1, len(xs)):
                    if xs[j] == xs[j - 1] + 1:
                        ax.plot([xs[j - 1] + dodge[system], xs[j] + dodge[system]], ys[j - 1:j + 1],
                                color=style["color"], linewidth=2, solid_capstyle="round", zorder=3)
            offered = [cell_summary(rows, "kafka", "fixed_rate", size, column, pct)["offered"] for pct in pcts]
            ax.set_xticks(range(len(pcts)))
            ax.set_xticklabels([f"{pct}%\n{compact(rate)}/s" if rate else f"{pct}%" for pct, rate in
                                zip(pcts, offered)], fontsize=7.5)
            ax.set_xlim(-0.5, len(pcts) - 0.5)
            ax.set_title(f"{label}, {size_label(size)} records", loc="left", fontsize=9)
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p: _tick(v)))
            ax.yaxis.set_minor_formatter(plt.NullFormatter())
            if c == 0:
                ax.set_ylabel("produce latency, ms (log scale)")
    if high is not None:
        axes[0][0].set_ylim(LOG_FLOOR_MS * 0.8, high * 1.8)
    handles = [Line2D([], [], color=SYSTEM_STYLE[s]["color"], marker=SYSTEM_STYLE[s]["marker"], linewidth=2,
                      markersize=7, markeredgecolor=SURFACE, label=SYSTEM_STYLE[s]["label"]) for s in ("kafka", "vtop")]
    if hollow_seen:
        handles.append(Line2D([], [], color=INK_2, marker="o", linestyle="none", markersize=7,
                              markerfacecolor=SURFACE, markeredgecolor=INK_2,
                              label="hollow marker: offered rate NOT sustained (achieved >5% short); "
                                    "closed-loop latency, no curve drawn"))
    fig.legend(handles=handles, loc="upper left", ncol=3, fontsize=8, handlelength=2.2, columnspacing=1.6,
               bbox_to_anchor=(0.055, 1 - 1.02 / height_in))
    _header(fig, height_in, "Produce latency at fixed offered load (open loop), VTOP vs Apache Kafka",
            _wrap("x axis: % of KAFKA's median saturation for the cell (records/s beneath), offered to both systems. "
                  "Marker: median of repetitions; whiskers: min-max. Latency is send() to acknowledgement as the "
                  "JVM perf tool measures it, in whole ms (a 0 is drawn at 0.5).", 200))
    _footer(fig, footer_text(env, run_id), 230)
    paths = _save(fig, img_dir, "kafka-compare-produce-latency")
    plt.close(fig)
    return paths


# --------------------------------------------------------------------------- report


def _fmt(value: float, digits: int = 0) -> str:
    return f"{value:,.{digits}f}"


def md_cell(text: str) -> str:
    """Free text made safe for one Markdown table cell: line breaks flattened,
    and the characters that end a cell, open a tag or a code span escaped."""
    flat = " ".join(text.split())
    for char in ("\\", "|", "`", "*", "_", "[", "]", "<", ">"):
        flat = flat.replace(char, "\\" + char)
    return flat


def _summary_cell(cell: dict[str, Any], digits: int = 0) -> str:
    summary = cell["summary"]
    if summary is None:
        reason = cell["not_ok"][0]["note"] if cell["not_ok"] else "no rows"
        return f"no result ({md_cell(reason[:60])})"
    text = f"{_fmt(summary.median, digits)} ({_fmt(summary.low, digits)}-{_fmt(summary.high, digits)})"
    if cell["ok"] < cell["n"]:
        text += f" [{cell['ok']}/{cell['n']} reps ok]"
    return text


def _ratio(a: dict[str, Any], b: dict[str, Any]) -> float | None:
    # Either side at zero has no ratio: a/0 is undefined, and 0/b would later
    # be inverted to say which system is faster.
    if a["summary"] is None or b["summary"] is None or a["summary"].median == 0 or b["summary"].median == 0:
        return None
    return a["summary"].median / b["summary"].median


def write_markdown(rows, env, run_id, csv_rel: str, env_rel: str, images: dict[str, str], doc_path: str) -> None:
    import matplotlib

    c = env["comparable"]
    sizes = env["matrix"]["record_sizes"]
    pcts = env["matrix"]["load_pcts"]
    reps = env["matrix"]["repetitions"]
    machine = f"{c['host_cpu']} ({c['host_model']}), {c['docker_platform']}"
    lines: list[str] = []
    add = lines.append
    add("# VTOP vs Apache Kafka: measured")
    add("")
    add("<!-- Generated by benchmarks/kafka_compare/report.py from the committed CSV. Do not edit by hand: "
        "change the generator or re-run the benchmark. -->")
    add("")
    add(f"> **Single host, {machine}; indicative, not a production benchmark.** Both clusters ran as Linux "
        "containers in the same Docker Desktop VM, one at a time, with identical per-node limits. Treat the "
        "numbers as a relative reading on a laptop, not as capacity figures for either system.")
    add("")
    add(f"Phase 1 of [#595](https://github.com/allamiro/vtop-engine/issues/595) "
        f"([#596](https://github.com/allamiro/vtop-engine/issues/596) throughput and latency, "
        f"[#600](https://github.com/allamiro/vtop-engine/issues/600) charts). Raw data: [`{os.path.basename(csv_rel)}`]"
        f"({csv_rel}) and [`{os.path.basename(env_rel)}`]({env_rel}), run `{run_id}`. Every measured number on "
        "this page and in the images (tables, charts, ratios) is generated from that CSV, and the stated "
        "conditions (machine, limits, images, commit, matrix, dates) from that `environment.json`, by "
        "`benchmarks/kafka_compare/report.py`.")
    add("")
    add("## Method")
    add("")
    add("- **Same machine, same limits (rule 1).** Three Kafka brokers (KRaft, combined broker+controller) and "
        f"three VTOP nodes, each a container with {c['node_limits']['cpus']} CPUs and "
        f"{c['node_limits']['mem_limit']} of memory, no swap, data on named volumes on the Docker VM's disk "
        f"(`{c['volume_filesystem']}`). One system runs at a time; each cell starts from empty volumes.")
    add(f"- **Same client (rule 2).** `{c['client']}` from `{c['kafka_image']}`, in its own container "
        f"({c['client_limits']['cpus']} CPUs, {c['client_limits']['memory']}), pointed at each cluster in turn. "
        f"Producer properties set: `{' '.join(c['producer_props'])}`; everything else is the client's default "
        "for both (its own config dump for each system is in `environment.json` under `observed.producer_config`).")
    add("- **Same replication contract (rule 3).** Kafka: RF=3, `min.insync.replicas=2`, `acks=all`. VTOP: one "
        "range replicated to three nodes, acknowledged at a write quorum of 2 of 3 (`Durability::Quorum`), "
        "`acks=all` from the client.")
    add(f"- **Profile `{c['profile']}` (rule 4).** Each system as shipped. This is **not** durability parity: "
        "Kafka acknowledges once the in-sync replicas have the batch in their page cache (`flush.messages` "
        "unset), while a VTOP quorum acknowledgement means two replicas have **fsynced** the append. The "
        "`fsync-parity` profile is phase 2.")
    add("- **Open-loop latency (rule 6).** Latency comes from fixed offered rates (`--throughput`) at "
        f"{', '.join(f'{p}%' for p in pcts)} of **Kafka's** median saturation for the cell, offered to both "
        "systems. A run more than 5% short of its offered rate is flagged *not sustained*: the tool's throttle "
        "then becomes a closed loop and its latency understates the real one, so the chart draws such points "
        "hollow and joins no line through them.")
    add(f"- **Repetition (rule 7).** {reps} repetitions per measurement; the first "
        f"{percent(env['matrix']['warmup_fraction'])} of each run's records are "
        "warm-up (`--warmup-records`) and excluded, after an unrecorded warm-up run per cell. Tables show the "
        "median with the min-max across repetitions.")
    add("- **Correctness first.** Before any timing, each fresh cluster took "
        f"{env['matrix']['smoke_records']:,} records of the cell's size with `acks=all` and handed every one "
        "back, byte-exact and in order, through the same client; a cluster that failed would have been refused.")
    add("")
    add("## Produce throughput at saturation")
    add("")
    add(f"![Produce throughput]({images['produce-throughput']})")
    add("")
    add("| record size | system | records/s | MiB/s |")
    add("|---|---|---|---|")
    for size in sizes:
        for system in ("kafka", "vtop"):
            rps = cell_summary(rows, system, "saturation", size, "records_per_sec")
            mib = cell_summary(rows, system, "saturation", size, "mib_per_sec")
            add(f"| {size_label(size)} | {SYSTEM_STYLE[system]['label']} | {_summary_cell(rps)} | "
                f"{_summary_cell(mib, 2)} |")
    add("")
    for size in sizes:
        ratio = _ratio(cell_summary(rows, "kafka", "saturation", size, "records_per_sec"),
                       cell_summary(rows, "vtop", "saturation", size, "records_per_sec"))
        if ratio is not None:
            faster = "Kafka" if ratio >= 1 else "VTOP"
            factor = ratio if ratio >= 1 else 1 / ratio
            add(f"- {size_label(size)} records: **{faster}** saturates at {factor:.1f}x the other's median "
                "records/s.")
    add("")
    add("## Produce latency at fixed offered load")
    add("")
    add(f"![Produce latency]({images['produce-latency']})")
    add("")
    add("| record size | load | offered records/s | system | achieved records/s | sustained | p50 ms | p99 ms "
        "| p99.9 ms |")
    add("|---|---|---|---|---|---|---|---|---|")
    for size in sizes:
        for pct in pcts:
            for system in ("kafka", "vtop"):
                p50 = cell_summary(rows, system, "fixed_rate", size, "lat_p50_ms", pct)
                p99 = cell_summary(rows, system, "fixed_rate", size, "lat_p99_ms", pct)
                p999 = cell_summary(rows, system, "fixed_rate", size, "lat_p999_ms", pct)
                offered = p50["offered"]
                achieved = _fmt(p50["achieved"].median) if p50["summary"] is not None else "-"
                sustained = ("yes" if p50["sustained"] else "**no**") if p50["summary"] is not None else "-"
                add(f"| {size_label(size)} | {pct}% | {_fmt(offered) if offered else '-'} | "
                    f"{SYSTEM_STYLE[system]['label']} | {achieved} | {sustained} | {_summary_cell(p50)} | "
                    f"{_summary_cell(p99)} | {_summary_cell(p999)} |")
    add("")
    for size in sizes:
        for system in ("kafka", "vtop"):
            cells = [cell_summary(rows, system, "fixed_rate", size, "lat_p50_ms", pct) for pct in pcts]
            held = [pct for pct, cell in zip(pcts, cells) if cell["summary"] is not None and cell["sustained"]]
            add(f"- {size_label(size)} records, {SYSTEM_STYLE[system]['label']}: sustained "
                f"{len(held)} of {len(pcts)} offered rates in every repetition"
                + (f" ({', '.join(f'{p}%' for p in held)})." if held else "."))
        vtop_sat = cell_summary(rows, "vtop", "saturation", size, "records_per_sec")["summary"]
        lowest = cell_summary(rows, "kafka", "fixed_rate", size, "lat_p50_ms", pcts[0])["offered"]
        if vtop_sat is not None and lowest and vtop_sat.high < lowest:
            add(f"- {size_label(size)} records: VTOP's fastest saturation repetition ({_fmt(vtop_sat.high)} "
                f"records/s) is below the lowest offered rate ({_fmt(lowest)} records/s), so none of its "
                "fixed-rate runs could be open-loop; see *Reading the results*.")
    add("")
    if any(row["mode"] == "consume" for row in rows):
        add("## Consume throughput")
        add("")
        add(f"![Consume throughput]({images['consume-throughput']})")
        add("")
        add("`kafka-consumer-perf-test` reading the first records of the partition with a fresh group each "
            "repetition, offset commits off for both. The rates are the tool's `fetch.*` columns, which exclude "
            "the group join (Kafka's `group.initial.rebalance.delay.ms` alone is 3 s).")
        add("")
        add("| record size | system | records/s | MiB/s |")
        add("|---|---|---|---|")
        for size in sizes:
            for system in ("kafka", "vtop"):
                rps = cell_summary(rows, system, "consume", size, "records_per_sec")
                mib = cell_summary(rows, system, "consume", size, "mib_per_sec")
                add(f"| {size_label(size)} | {SYSTEM_STYLE[system]['label']} | {_summary_cell(rps)} | "
                    f"{_summary_cell(mib, 2)} |")
        add("")
    add("## Reading the results")
    add("")
    add(READING.format(lowest_pct=pcts[0]))
    add("")
    add("## Gateway gaps met (rule 10)")
    add("")
    add("| gap | met by | exact error | what was done |")
    add("|---|---|---|---|")
    for gap, met_by, error, done in GATEWAY_GAPS:
        add(f"| {gap} | {met_by} | `{error}` | {done} |")
    add("")
    add("## Known gaps in this phase")
    add("")
    add(known_gaps(env))
    add("")
    add("## Environment")
    add("")
    add("| | |")
    add("|---|---|")
    for key in ("host_cpu", "host_model", "host_logical_cpus", "host_memory_bytes", "host_os", "docker_platform",
                "docker_server_version", "docker_vm_cpus", "docker_vm_memory_bytes", "docker_vm_kernel",
                "docker_vm_arch", "docker_storage_driver", "volume_filesystem", "kafka_image", "vtop_commit",
                "vtop_image", "client", "client_java", "profile"):
        add(f"| `{key}` | `{c.get(key)}` |")
    add(f"| run | `{run_id}`, started {env['started_at']}, finished {env.get('finished_at', '?')} |")
    add(f"| charts | matplotlib {matplotlib.__version__} |")
    add("")
    others = env.get("other_containers_at_start_count")
    if others:
        add(f"{others} unrelated containers were running on the same Docker host when the run started "
            "(`environment.json`, `other_containers_at_start_count`). Both systems shared the VM with them, one "
            "system at a time; they are noise neither system chose, and part of why these are laptop numbers.")
    add("")
    with open(doc_path, "w") as handle:
        handle.write("\n".join(lines))


READING = """\
Where Kafka is faster, the reasons known from the code and the configuration are these. None of them was
isolated by a separate measurement in this phase; they are the explanation the code supports, not a proof.

1. **What an acknowledgement costs.** Under `defaults`, a Kafka `acks=all` acknowledgement waits for the
   in-sync replicas to have the batch in their logs (page cache); nothing is fsynced. A VTOP quorum
   acknowledgement waits until a majority of the three replicas, the leader counted, has made the append
   *durable* (fsync), and the leader has advanced the committed high-water mark. Every produce request pays
   for fsyncs and a replication round trip before it is answered. This is the largest single difference, and
   it is a difference in the guarantee as much as in speed; the `fsync-parity` profile (phase 2) prices the
   same guarantee on Kafka.
2. **No group commit on the gateway path.** `vtop-node data` appends each produce request as its own durability
   barrier: the broker's group-commit mode (one fsync and replication round shared by many concurrent
   requests) exists in `vtop-broker` but is not wired into the node. The client caps a request at
   `batch.size` (16 KiB by default) whatever the record size, so a ceiling set per request shows up as a
   roughly constant MiB/s across record sizes and a record rate that falls as records grow, which is the shape
   of VTOP's cells here.
3. **Translation in the gateway.** Every Kafka record batch is decoded, validated (CRC, attributes) and
   re-appended through the native broker, which frames it again for the replica plane; Kafka validates the
   batch and appends it with offsets assigned in place, without taking its records apart. The cost is CPU and
   copies, not round trips.
4. **Fetch path.** Kafka serves a fetch from stored batches with a zero-copy file transfer; the gateway reads
   records through the native broker and re-encodes them as a Kafka record batch.

A separate, unrecorded probe during development showed the VTOP nodes far below their CPU limit while
saturated, which fits (1) and (2): the produce path is bound by durability round trips, not CPU. Per-node CPU
and bytes written are not in this run's CSV; recording them is #596's resource metric (phase 2).

**On the latency chart:** where VTOP's saturation is below the lowest offered rate ({lowest_pct}% of Kafka's), no VTOP
fixed-rate run can be sustained, and the perf tool's throttle degenerates into a closed loop against a full
producer buffer. Those VTOP latencies are queueing in the client (seconds), not VTOP's service latency at that
load; they are drawn hollow and must not be read as "VTOP's p99 at {lowest_pct}% load". A fair open-loop VTOP latency
curve needs offered rates below VTOP's own saturation, which the issue's Kafka-relative rule does not produce
here; adding VTOP-relative rates is a phase-2 item.

Nothing here measures what VTOP adds over Kafka on this path (fenced epochs, the verified commit rule, the
evidence chain; see [COMPARISON.md](../COMPARISON.md)), and nothing here is a failover, archive or crash
measurement (#597, #598, #599)."""

def percent(fraction: float) -> str:
    return f"{fraction * 100:g}%"


def _counted(values: list[int], noun: str) -> str:
    joined = " and ".join(str(v) for v in values)
    return f"{joined} {noun}" + ("" if values == [1] else "s")


def known_gaps(env: dict[str, Any]) -> str:
    """The known-gaps list, with the matrix facts read from the run, not typed in."""
    m = env["matrix"]
    sizes, partitions, producers = m["record_sizes"], m["partitions"], m["producers"]
    profile = env["comparable"]["profile"]
    # #596's full matrix, and which parts of it this run did not cover.
    full = (("10 KiB", 10240 in sizes), ("6 partitions", 6 in partitions), ("4 producers", 4 in producers),
            ("zstd", False), ("`fsync-parity`", profile == "fsync-parity"))
    missing = [name for name, covered in full if not covered]
    lines = [
        f"- **Matrix size.** Record sizes {' and '.join(size_label(size) for size in sizes)} only; "
        f"{_counted(partitions, 'partition')}, {_counted(producers, 'producer')}, no compression, profile\n"
        f"  `{profile}` only." + (f" #596's full matrix ({', '.join(missing)}) is not run." if missing else ""),
    ]
    if max(partitions) == 1:
        lines.append(
            "- **One gateway, one partition.** The gateway runs on the range's leader only; a multi-partition VTOP "
            "topic\n  needs one leader per partition and is not exercised here.")
    return "\n".join(lines) + "\n" + KNOWN_GAPS_FIXED


# The gaps that do not depend on the run's matrix: how the harness is built.
KNOWN_GAPS_FIXED = """\
- **No metadata plane.** VTOP runs as a static leader with two followers (live-chaos scenario 17's shape), so
  there is no lease, no failover and no durable consumer-offset store; Kafka runs its full KRaft quorum. Neither
  matters to a steady-state produce/fetch measurement, but the two clusters are not doing identical background
  work.
- **Latency resolution.** The JVM perf tool reports percentiles in whole milliseconds, from a sample of the
  run's records; sub-millisecond differences are invisible.
- **End-to-end latency, CPU and write amplification** (#596 metrics 3 and 4) are not measured yet."""


# ----------------------------------------------------------------------------- main


def build(run_dir: str, img_dir: str, doc_path: str) -> list[str]:
    run = schema.load_run(run_dir)  # raises ReportRefused before anything is written
    plt = _setup_matplotlib()
    os.makedirs(img_dir, exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(doc_path)), exist_ok=True)
    run_id = run.environment["run_id"]
    m = run.environment["matrix"]
    shape = f"{_counted(m['partitions'], 'partition')}, {_counted(m['producers'], 'producer')}"
    written = []
    written += throughput_chart(plt, run.rows, run.environment, run_id, "saturation",
                                "Produce throughput at saturation, VTOP vs Apache Kafka",
                                f"{shape}, acks={'/'.join(m['acks'])}, RF 3 / min ISR 2 (VTOP: write quorum 2 of 3). "
                                "Same JVM client container for both systems.",
                                "kafka-compare-produce-throughput", img_dir)
    written += latency_chart(plt, run.rows, run.environment, run_id, img_dir)
    if run.environment["matrix"].get("consume"):
        written += throughput_chart(plt, run.rows, run.environment, run_id, "consume",
                                    "Consume throughput, VTOP vs Apache Kafka",
                                    f"{_counted(m['partitions'], 'partition')}, 1 consumer in a fresh group, "
                                    "offset commits off for both; the tool's "
                                    "fetch rate, which excludes the group join. Same JVM client container for both.",
                                    "kafka-compare-consume-throughput", img_dir)
    doc_dir = os.path.dirname(os.path.abspath(doc_path))
    images = {
        key: os.path.relpath(os.path.join(img_dir, f"kafka-compare-{key}.png"), doc_dir)
        for key in ("produce-throughput", "produce-latency", "consume-throughput")
    }
    csv_rel = os.path.relpath(os.path.join(run_dir, "results.csv"), doc_dir)
    env_rel = os.path.relpath(os.path.join(run_dir, "environment.json"), doc_dir)
    write_markdown(run.rows, run.environment, run_id, csv_rel, env_rel, images, doc_path)
    written.append(doc_path)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("run_dir", help="benchmarks/kafka_compare/results/<run-id>")
    parser.add_argument("--img-dir", default=DEFAULT_IMG_DIR)
    parser.add_argument("--doc", default=None, help="default: kafka-comparison.md, or kafka-failover.md for a "
                                                     "failover run")
    args = parser.parse_args(argv)
    try:
        if os.path.isfile(os.path.join(args.run_dir, "failover.csv")):
            from kafka_compare import failover_report

            written = failover_report.build(args.run_dir, args.img_dir, args.doc or DEFAULT_FAILOVER_DOC,
                                            _setup_matplotlib)
        else:
            written = build(args.run_dir, args.img_dir, args.doc or DEFAULT_DOC)
    except schema.ReportRefused as exc:
        print(f"report refused: {exc}", file=sys.stderr)
        return 2
    for path in written:
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
