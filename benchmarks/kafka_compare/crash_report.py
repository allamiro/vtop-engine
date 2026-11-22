"""Charts and report for the whole-replica-set crash benchmark (#599, #600), from one crash run's CSV.

Called by report.py when the run directory holds a crash.csv:

    python3 benchmarks/kafka_compare/report.py benchmarks/kafka_compare/results/<crash-run-id>

Writes PNG + SVG to docs/benchmarks/img/ and docs/benchmarks/kafka-crash.md (or where the run's
report.json says). Every measured number comes from crash.csv, the conditions from environment.json;
`crash_schema.load_crash_run` refuses a missing, partial, unfinished or mixed run, a row that is `ok`
while its canary did not behave, and a configuration with fewer than MIN_OK_CRASHES ok crashes, before
anything is drawn. The loss table is at the top of the page, never only a chart, and every "0 lost" is
printed with the exact binomial upper bound that says how little the crashes behind it can show.

The reader is `crash_schema`, the harness's own module (#599), so the report applies exactly the refusals
the harness wrote for it and there is no second copy of them to drift.
"""
from __future__ import annotations

import os
from typing import Any, NamedTuple

from kafka_compare import colours, footer, stats
from kafka_compare import crash_analysis as ca
from kafka_compare import crash_schema as cs
from kafka_compare import report as base

# The fewest ok crashes a configuration may have for the report to draw it. The plan is 12 per
# configuration and mode, so two crashes may be refused or failed; below ten a "0 lost" says too little
# to chart (at 10 the 95 % bound is 25.9 %). `report.py --min-ok` lowers it to preview a short run.
MIN_OK_CRASHES = 10

# Power loss is the measurement, the process kill its control: that is the order of every table and chart.
MODE_ORDER = ("flakey-drop", "sigkill")
MODE_TITLE = {"flakey-drop": "power loss of the whole replica set (dm-flakey drop_writes)",
              "sigkill": "SIGKILL of all three nodes, page cache kept (the control)"}
# One colour per system from the registry; the two Kafka configurations share it and differ by marker.
MARKER = {"kafka-defaults": "o", "kafka-fsync-parity": "D", "vtop-shipped": "s"}
SYSTEM_LABEL = {"kafka": "Apache Kafka", "vtop": "VTOP (Kafka gateway)"}
CONFIG_LABEL = {"kafka-defaults": "Kafka, defaults", "kafka-fsync-parity": "Kafka, fsync on every message",
                "vtop-shipped": "VTOP, as shipped"}


class Cell(NamedTuple):
    """One (configuration, mode): what its crashes did."""

    config: str
    mode: str
    rows: list[dict[str, str]]
    ok: list[dict[str, str]]
    refused: int
    failed: int
    lost: list[int]  # acked_lost per ok crash, in crash order
    lossy: int  # ok crashes that lost at least one acknowledged record
    acked: int
    duplicates: int
    reordered: int
    offset_mismatch: int
    unexpected: int
    sentinel_needed: int
    manual_repair: int
    drops_ms: list[float]
    bound: float | None  # 95 % upper bound on the chance a crash loses something, when none did


def _int(row: dict[str, str], column: str) -> int:
    return int(float(row[column]))


def cells(rows: list[dict[str, str]], env: dict[str, Any]) -> list[Cell]:
    matrix = env["matrix"]
    out = []
    for mode in (m for m in MODE_ORDER if m in matrix["crash_modes"]):
        for config in matrix["configs"]:
            mine = sorted((r for r in rows if r["config"] == config and r["crash_mode"] == mode),
                          key=lambda r: _int(r, "crash"))
            ok = [r for r in mine if r["status"] == "ok"]
            lost = [_int(r, "acked_lost") for r in ok]
            drops = [float(r["kill_to_drop_ms"]) for r in ok if r["kill_to_drop_ms"] != ""]
            lossy = sum(1 for n in lost if n > 0)
            out.append(Cell(
                config, mode, mine, ok, sum(1 for r in mine if r["status"] == "refused"),
                sum(1 for r in mine if r["status"] == "failed"), lost, lossy,
                sum(_int(r, "acked") for r in ok), sum(_int(r, "duplicates") for r in ok),
                sum(_int(r, "reordered") for r in ok), sum(_int(r, "offset_mismatch") for r in ok),
                sum(_int(r, "unexpected") for r in ok), sum(1 for r in ok if r["sentinel_needed"] == "true"),
                sum(1 for r in mine if r["needed_manual_repair"] == "true"), drops,
                ca.upper_bound_95(0, len(ok)) if ok and lossy == 0 else None))
    return out


def _cell(text: Any) -> str:
    """A table cell: one line, no pipe."""
    return " ".join(str(text).split()).replace("|", "\\|")


def _table(header: list[str], body: list[list[Any]]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(_cell(c) for c in line) + " |" for line in body]
    return lines


def crashes(n: int) -> str:
    return f"{n} crash" + ("" if n == 1 else "es")


def percent(fraction: float) -> str:
    return f"{fraction * 100:.1f} %"


def _count(value: float) -> str:
    return f"{value:,.0f}"


def _ms(value: float) -> str:
    return f"{value:,.0f} ms"


def _kind_of(config: str) -> str:
    return ca.CONFIGS[config][0]


# ------------------------------------------------------------------------------ footer


def footer_text(env: dict[str, Any]) -> str:
    """The footer every crash image carries: host, disk, kernel, limits, images, commit, client, crashes."""
    c = env["comparable"]
    m = env["matrix"]
    limits = c["node_limits"]
    fs = c.get("data_filesystem") or {}
    host = f"{c['host_cpu']}, {c['host_logical_cpus']} logical CPUs, {c['host_os']}"
    if c.get("host_instance"):
        host = f"{c['host_instance']}: " + host
    first = [
        ("host", host),
        ("RAM", footer.gib(c.get("host_memory_bytes"))),
        ("disk", footer.disk(c)),
        ("data fs", f"{fs.get('fs_type', 'unknown')} {fs.get('mount_options', '')} on {fs.get('mount_source', '?')}"
                    .replace("  ", " ")),
        ("Docker", f"{c['docker_platform']} {c.get('docker_server_version', '')}".strip()),
        ("per node", f"{limits['cpus']} CPUs, {limits['mem_limit']} RAM"),
    ]
    second = [
        ("Kafka", c["kafka_image"].split("@")[0] + " (KRaft, 3 brokers)"),
        ("VTOP", c["vtop_commit"][:12] + " (3 nodes, Kafka gateway)"),
        ("client", f"kafka-verifiable-producer, acks=all, {m['rate']:,} records/s"),
        ("crashes", f"{m['crashes']} per configuration and mode, seed {m['seed']}"),
        ("run", env["run_id"]),
        ("date", env["started_at"][:10]),
    ]
    lines = []
    for line in (first, second):
        for label, value in line:
            if footer.SEPARATOR.strip() in value or "\n" in value:
                raise ValueError(f"footer field {label!r} is {value!r}, which holds a separator or a line break")
        lines.append(footer.SEPARATOR.join(f"{label}: {value}" for label, value in line))
    lines[-1] += footer.SEPARATOR + footer.CAVEAT
    return "\n".join(lines)


# ------------------------------------------------------------------------------ charts


def _x_offsets(n: int, spread: float = 0.62) -> list[float]:
    return [(j - (n - 1) / 2) * (spread / max(n - 1, 1)) if n > 1 else 0.0 for j in range(n)]


def loss_chart(plt, run: cs.CrashRun, img_dir: str, text: str) -> list[str]:
    from matplotlib.lines import Line2D
    from matplotlib.ticker import MaxNLocator

    env = run.environment
    everything = cells(run.rows, env)
    modes = [m for m in MODE_ORDER if m in env["matrix"]["crash_modes"]]
    configs = env["matrix"]["configs"]
    width_in, height_in = 6.2 * len(modes) + 0.6, 5.7
    fig, axes = plt.subplots(1, len(modes), figsize=(width_in, height_in), squeeze=False)
    fig.subplots_adjust(left=0.075 if len(modes) > 1 else 0.11, right=0.99, top=1 - 1.75 / height_in,
                        bottom=1.45 / height_in, wspace=0.24)
    for ax, mode in zip(axes[0], modes):
        base._style_axes(ax)
        ax.grid(axis="y")
        mine = {c.config: c for c in everything if c.mode == mode}
        top = max([4] + [n for c in mine.values() for n in c.lost])
        for i, config in enumerate(configs):
            cell = mine[config]
            colour = colours.colour(_kind_of(config), "light")
            xs = [i + dx for dx in _x_offsets(len(cell.lost))]
            ax.scatter(xs, cell.lost, marker=MARKER[config], s=30, color=colour, edgecolors=base.SURFACE,
                       linewidths=0.8, zorder=4)
            if cell.lost:
                ax.hlines(stats.median(cell.lost), i - 0.34, i + 0.34, color=base.INK, linewidth=1.3, zorder=5)
            said = f"{cell.lossy} of {len(cell.ok)} lost records"
            if cell.bound is not None:
                said += f"\n95 % bound: {percent(cell.bound)}"
            ax.text(i, 0.975, said, transform=ax.get_xaxis_transform(), ha="center", va="top", fontsize=7.5,
                    color=base.INK)
        ax.set_xticks(range(len(configs)))
        ax.set_xticklabels([f"{CONFIG_LABEL[c]}\n{len(mine[c].ok)} of {len(mine[c].rows)} crashes ok"
                            for c in configs], fontsize=7.8)
        ax.set_xlim(-0.6, len(configs) - 0.4)
        ax.set_ylim(-0.04 * top, top * 1.4)
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))  # counts: no 0.5 of a record
        ax.set_ylabel("acknowledged records lost, per crash")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p: base._tick(v)))
        ax.set_title(MODE_TITLE[mode], loc="left", fontsize=8.8)
    handles = [Line2D([], [], color=colours.colour(_kind_of(c), "light"), marker=MARKER[c], linewidth=0,
                      markersize=6.5, markeredgecolor=base.SURFACE, label=CONFIG_LABEL[c]) for c in configs]
    handles.append(Line2D([], [], color=base.INK, linewidth=1.3, label="median over the ok crashes"))
    fig.legend(handles=handles, loc="upper left", ncol=len(handles), fontsize=8, handlelength=1.4,
               bbox_to_anchor=(0.06, 1 - 1.18 / height_in))
    base._header(fig, height_in, "Acknowledged records lost when the whole replica set crashes, Apache Kafka vs VTOP",
                 base._wrap("One dot per crash that measured; zero is on the axis. Acknowledged means the client "
                            "printed the acknowledgement (acks=all) before the kill. A crash whose canary did not "
                            "behave is refused and not drawn. The 95 % bound is the exact binomial upper bound on the "
                            "chance that a crash loses something, given that none of these did.", 190))
    base._footer(fig, text, 200)
    paths = base._save(fig, img_dir, "kafka-crash-loss", text)
    plt.close(fig)
    return paths


def drop_chart(plt, run: cs.CrashRun, img_dir: str, text: str) -> list[str]:
    from matplotlib.lines import Line2D

    env = run.environment
    configs = env["matrix"]["configs"]
    mine = {c.config: c for c in cells(run.rows, env) if c.mode == "flakey-drop"}
    width_in, height_in = 7.5, 5.0
    fig, ax = plt.subplots(figsize=(width_in, height_in))
    fig.subplots_adjust(left=0.11, right=0.985, top=1 - 1.55 / height_in, bottom=1.45 / height_in)
    base._style_axes(ax)
    ax.grid(axis="y")
    top = max([1.0] + [v for c in mine.values() for v in c.drops_ms])
    for i, config in enumerate(configs):
        drops = mine[config].drops_ms
        colour = colours.colour(_kind_of(config), "light")
        ax.scatter([i + dx for dx in _x_offsets(len(drops))], drops, marker=MARKER[config], s=30, color=colour,
                   edgecolors=base.SURFACE, linewidths=0.8, zorder=4)
        if drops:
            median = stats.median(drops)
            ax.hlines(median, i - 0.34, i + 0.34, color=base.INK, linewidth=1.3, zorder=5)
            ax.text(i, 0.975, f"median {_ms(median)}", transform=ax.get_xaxis_transform(), ha="center", va="top",
                    fontsize=7.5, color=base.INK)
        else:
            ax.text(i, 0.975, "no ok crash", transform=ax.get_xaxis_transform(), ha="center", va="top",
                    fontsize=7.5, color=base.INK_2)
    ax.set_xticks(range(len(configs)))
    ax.set_xticklabels([CONFIG_LABEL[c] for c in configs], fontsize=7.8)
    ax.set_xlim(-0.6, len(configs) - 0.4)
    ax.set_ylim(0, top * 1.35)
    ax.set_ylabel("milliseconds from the kill to the drop")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p: base._tick(v)))
    handles = [Line2D([], [], color=base.INK, linewidth=1.3, label="median over the ok crashes")]
    fig.legend(handles=handles, loc="upper left", fontsize=8, bbox_to_anchor=(0.09, 1 - 1.12 / height_in))
    base._header(fig, height_in, "The gap between the kill and the drop of unflushed writes",
                 base._wrap("One dot per power-loss crash that measured. In this gap the kernel may still flush a "
                            "little of what the page cache holds; that can only make a system look better, never "
                            "worse, and it is the same method for every configuration.", 120))
    base._footer(fig, text, 120)
    paths = base._save(fig, img_dir, "kafka-crash-drop-gap", text)
    plt.close(fig)
    return paths


# ------------------------------------------------------------------------------ the page


def _loss_row(cell: Cell) -> list[Any]:
    n = len(cell.ok)
    worst = max(cell.lost) if cell.lost else 0
    lossy_lost = [v for v in cell.lost if v > 0]
    return [CONFIG_LABEL[cell.config], cell.mode, f"{n} of {len(cell.rows)}", _count(cell.acked),
            f"**{_count(sum(cell.lost))}**", f"{cell.lossy} of {n}" if n else "-",
            _count(stats.median(lossy_lost)) if lossy_lost else "-", _count(worst) if n else "-",
            percent(cell.bound) if cell.bound is not None else "-"]


def sentences(all_cells: list[Cell]) -> list[str]:
    """What the table says, in words, from the table's own numbers (no number is typed here)."""
    out = []
    for cell in all_cells:
        n = len(cell.ok)
        who = f"`{cell.config}` under {cell.mode}"
        if n == 0:
            out.append(f"{who}: no crash measured, so nothing can be said.")
        elif cell.lossy == 0:
            out.append(f"{who}: no acknowledged record was lost in {crashes(n)}. Zero of {n} bounds the chance that "
                       f"a crash loses something only to {percent(cell.bound)} at 95 % confidence (exact binomial), "
                       "so this is evidence for the guarantee, not proof of it.")
        else:
            out.append(f"{who}: acknowledged records were lost in {cell.lossy} of {n} crashes "
                       f"({_count(sum(cell.lost))} records in all, {_count(max(cell.lost))} in the worst crash).")
    return out


DEFINITIONS = """\
- **Acknowledged:** the client printed `producer_send_success` for the record, with `acks=all`, before the cluster
  was killed. The client is stopped right after the cluster, so nothing is acknowledged after the crash.
- **Lost:** an acknowledged record that a read of the whole partition, after the cluster is back and a sentinel
  record has been acknowledged, does not find. A record that was written but never acknowledged is neither a loss
  nor a duplicate.
- **Duplicate, reordered, offset mismatch:** an extra copy of a record id in that read; a record read after one with
  a larger id; an acknowledged record that is present but not at the offset it was acknowledged at.
- **Sentinel needed:** the read before the sentinel and the read after it differ, that is, records acknowledged
  before the crash were not served until a new write was committed (a restarted leader serves what is
  cluster-committed). It says when the answer arrived, not that anything was lost.
- **Refused:** the crash's canary did not behave (the fsynced canary must be whole after the crash; the unsynced
  one must be absent or short in a power-loss crash and whole after a process kill). The method did not do what it
  claims, so the row says nothing about the system; it is kept, with its reason, and never counted as ok.
- **Failed:** the crash could not be measured to the end, for example because the cluster did not serve after the
  restart (the row then says `needed_manual_repair`). Kept, with its reason, and never counted as ok."""

LIMITS = """\
- **One host.** All three replicas of both systems share one machine, one kernel, one filesystem and one
  failure domain, on purpose: the crash takes all three at once. This is the loss of one host's page cache, not a
  rack, a zone or independent failures.
- **A simulation of power loss.** `flakey-drop` kills the processes and then drops every write the page cache had not
  flushed (dm-flakey `drop_writes`), which is a kernel panic or power loss with a disk that honours flushes. It does
  not model a drive whose volatile write cache lies about flushes, a torn write inside a block, or a hardware
  write-back cache. It is not the `echo b > /proc/sysrq-trigger` of a virtual machine that #599 preferred, which
  needs a second machine for the client.
- **Small records.** The client is `kafka-verifiable-producer.sh`, whose value prefix must be an integer and which
  has no record-size flag, so a record is a few bytes of text. The loss window of a page cache depends on time, not
  on bytes.
- **VTOP without its metadata plane.** VTOP runs as a static range leader with two followers, so there is no lease
  and no election after the restart; Kafka runs its full KRaft quorum."""


def write_markdown(run: cs.CrashRun, images: dict[str, str], csv_rel: dict[str, str], text: str,
                   doc_path: str) -> None:
    env = run.environment
    rows = run.rows
    c = env["comparable"]
    m = env["matrix"]
    all_cells = cells(rows, env)
    lines: list[str] = []
    add = lines.append
    add(f"<!-- Generated by benchmarks/kafka_compare/crash_report.py from {csv_rel['crash']}. Do not edit by hand: "
        "change the generator or re-run the benchmark. -->")
    add("")
    add("# Whole-replica-set crash: acknowledged records after power loss, VTOP vs Apache Kafka")
    add("")
    where = c.get("host_instance") or c["host_cpu"]
    add(f"> **Single host, {_cell(where)}, {_cell(c['docker_platform'])}; indicative, not a production benchmark.** "
        "Each crash brings up a fresh three-node cluster, kills all three nodes at once and reads back what the "
        "producer had been told was safe.")
    add("")
    add("[#599](https://github.com/allamiro/vtop-engine/issues/599), part of "
        "[#595](https://github.com/allamiro/vtop-engine/issues/595); charts per "
        "[#600](https://github.com/allamiro/vtop-engine/issues/600). Raw data: "
        f"[`crash.csv`]({csv_rel['crash']}) and [`environment.json`]({csv_rel['environment']}), run "
        f"`{env['run_id']}`. Every number below is generated from those files by "
        "`benchmarks/kafka_compare/crash_report.py`.")
    add("")
    suspect = ca.suspect_method(rows, 5)
    if suspect:
        add(f"> **The method is suspect.** {suspect}")
        add("")
    add("## What was measured")
    add("")
    add(f"{m['crashes']} crashes per configuration and mode, a producer at {m['rate']:,} records/s, crash times drawn "
        f"from one seed ({m['seed']}) in a window of {m['trigger_window_s'][0]:g} to {m['trigger_window_s'][1]:g} "
        "seconds after the producer starts, so every configuration sees the same schedule. Configurations:")
    add("")
    for config in m["configs"]:
        add(f"- `{config}`: {_cell((c.get('profiles') or {}).get(config, ''))}")
    add("")
    add("Crash modes:")
    add("")
    for mode in (x for x in MODE_ORDER if x in m["crash_modes"]):
        add(f"- `{mode}`: {_cell((c.get('crash_modes') or {}).get(mode, ca.CRASH_MODE_LABELS[mode]))}")
    add("")
    add("Definitions:")
    add("")
    add(DEFINITIONS)
    add("")
    add("## Loss: acknowledged records that were not there afterwards")
    add("")
    add(f"Counted over the ok crashes only. \"95 % bound\" is the exact one-sided upper bound on the chance that a "
        "crash loses something, given that none of the ok crashes did; it is printed only for a cell that lost "
        "nothing.")
    add("")
    lines += _table(["configuration", "mode", "ok crashes", "acknowledged", "lost", "crashes that lost records",
                     "median lost in those", "worst crash", "95 % bound"], [_loss_row(x) for x in all_cells])
    add("")
    for sentence in sentences(all_cells):
        add(f"- {sentence}")
    add("")
    add(f"![Acknowledged records lost per crash]({images['loss']})")
    add("")
    add("## Everything else the read-back checks")
    add("")
    lines += _table(
        ["configuration", "mode", "duplicates", "reordered", "offset mismatch", "unexpected values",
         "sentinel needed", "refused", "failed", "needed manual repair"],
        [[CONFIG_LABEL[x.config], x.mode, _count(x.duplicates), _count(x.reordered), _count(x.offset_mismatch),
          _count(x.unexpected), f"{x.sentinel_needed} of {len(x.ok)}", x.refused, x.failed, x.manual_repair]
         for x in all_cells])
    add("")
    if "flakey-drop" in m["crash_modes"]:
        add("## The gap between the kill and the drop")
        add("")
        add("`kill_to_drop_ms`: the time from the kill to the moment unflushed writes were dropped. In it the kernel "
            "may flush a little more, which can only make a system look better.")
        add("")
        drop_rows = []
        for x in (y for y in all_cells if y.mode == "flakey-drop"):
            if x.drops_ms:
                s = stats.summarize(x.drops_ms)
                drop_rows.append([CONFIG_LABEL[x.config], f"{_ms(s.median)} ({_ms(s.low)} - {_ms(s.high)})", s.n])
            else:
                drop_rows.append([CONFIG_LABEL[x.config], "no ok crash", 0])
        lines += _table(["configuration", "median (min - max)", "crashes"], drop_rows)
        add("")
        add(f"![Kill to drop gap]({images['drop']})")
        add("")
    add("## Every crash")
    add("")
    add("One row per crash, in the order the harness ran them. `missing before sentinel` is what the first read "
        "lacked; `lost` is what the second read, after the sentinel, still lacked.")
    add("")
    body = []
    for x in all_cells:
        for r in x.rows:
            if r["status"] == "ok":
                lost_window = f"{float(r['lost_window_s']):.1f}" if r["lost_window_s"] else "-"
                drop = _count(float(r["kill_to_drop_ms"])) if r["kill_to_drop_ms"] else "-"
                body.append([r["config"], r["crash_mode"], r["crash"], "ok", _count(_int(r, "acked")),
                             _count(_int(r, "read_missing_before_sentinel")), _count(_int(r, "acked_lost")),
                             lost_window, _count(_int(r, "duplicates")), _count(_int(r, "reordered")),
                             "yes" if r["sentinel_needed"] == "true" else "no", drop,
                             f"{float(r['recovery_seconds']):.1f}"])
            else:
                body.append([r["config"], r["crash_mode"], r["crash"], r["status"], "-", "-", "-", "-", "-", "-",
                             "-", "-", "-"])
    lines += _table(["configuration", "mode", "crash", "status", "acknowledged", "missing before sentinel", "lost",
                     "oldest lost, s before the kill", "duplicates", "reordered", "sentinel needed",
                     "kill to drop, ms", "recovery, s"], body)
    add("")
    not_ok = [r for r in rows if r["status"] != "ok"]
    if not_ok:
        add("## Crashes that are not ok")
        add("")
        add("Recorded, not retried, and never counted above.")
        add("")
        for r in not_ok:
            add(f"- `{r['config']}` / `{r['crash_mode']}` / crash {r['crash']}: **{r['status']}**, "
                f"{_cell(r['note'])} (the node logs are under `logs/` in the run directory)")
        add("")
    add("## Limits")
    add("")
    if c.get("simulation_limits"):
        add(f"The harness records this about its own method: {_cell(c['simulation_limits'])}.")
        add("")
    add(f"- **Few crashes.** {m['crashes']} per configuration and mode. A configuration that loses nothing in "
        f"{m['crashes']} crashes is bounded to {percent(ca.upper_bound_95(0, int(m['crashes'])))} at 95 % "
        "confidence, not proven safe; the bound is printed beside every cell that lost nothing.")
    add(LIMITS)
    add("")
    add("## Environment")
    add("")
    add("```")
    add(text)
    add("```")
    add("")
    with open(doc_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def build(run_dir: str, img_dir: str, doc_path: str, setup_matplotlib, min_ok: int = MIN_OK_CRASHES) -> list[str]:
    run = cs.load_crash_run(run_dir, min_ok=min_ok)  # raises ReportRefused before anything is written
    text = footer_text(run.environment)
    plt = setup_matplotlib()
    os.makedirs(img_dir, exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(doc_path)), exist_ok=True)
    written = loss_chart(plt, run, img_dir, text)
    if "flakey-drop" in run.environment["matrix"]["crash_modes"]:
        written += drop_chart(plt, run, img_dir, text)
    doc_dir = os.path.dirname(os.path.abspath(doc_path))
    images = {key: os.path.relpath(os.path.join(img_dir, f"kafka-crash-{name}.png"), doc_dir)
              for key, name in (("loss", "loss"), ("drop", "drop-gap"))}
    rel = {"crash": os.path.relpath(os.path.join(run_dir, "crash.csv"), doc_dir),
           "environment": os.path.relpath(os.path.join(run_dir, "environment.json"), doc_dir)}
    write_markdown(run, images, rel, text, doc_path)
    written.append(doc_path)
    return written
