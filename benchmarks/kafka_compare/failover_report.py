"""Charts and report for the failover comparison (#597, #600), from one failover run's CSVs.

Called by report.py when the run directory holds a failover.csv:

    python3 benchmarks/kafka_compare/report.py benchmarks/kafka_compare/results/<failover-run-id>

Writes PNG + SVG to docs/benchmarks/img/ and docs/benchmarks/kafka-failover.md.
Every measured number comes from failover.csv / timeline.csv, the conditions
from environment.json; `failover_schema` refuses a missing, partial or mixed
run before anything is drawn. Correctness (lost and duplicated acknowledged
records) is a table at the top of the report, never only a chart.
"""
from __future__ import annotations

import os
from typing import Any

from kafka_compare import failover_schema as fschema
from kafka_compare import report as base
from kafka_compare import stats
from kafka_compare.failover_stats import FAULT_LABELS, classify_vtop_reason

SYSTEMS = ("kafka", "vtop")
STYLE = {
    "kafka": {**base.SYSTEM_STYLE["kafka"], "label": "Apache Kafka (JVM client)"},
    "vtop": {**base.SYSTEM_STYLE["vtop"], "label": "VTOP (native client, NOT the Kafka path)"},
}
SHORT = {"kafka": "Kafka", "vtop": "VTOP"}

LEASE_OBSERVER_SOURCE = ("vtop/failover-driver.sh `LEASE_POLL_SECONDS`: the lease observer's poll, which feeds "
                         "only the time to new leader, never the producer")

CANDIDATE_GATEWAY_ERROR = (
    "error: `kafka` is configured on a Candidate node: the gateway is served by a leader or standalone only "
    "— it holds one broker, and a candidate's changes with the lease (#225)"
)


# ----------------------------------------------------------------------------- data


def _f(value: str) -> float | None:
    return float(value) if value not in ("", None) else None


def ok_rows(rows, system: str, fault: str) -> list[dict[str, str]]:
    return [row for row in rows if row["system"] == system and row["fault"] == fault and row["status"] == "ok"]


def all_rows(rows, system: str, fault: str) -> list[dict[str, str]]:
    return [row for row in rows if row["system"] == system and row["fault"] == fault]


def summary(rows, system: str, fault: str, column: str) -> stats.Summary | None:
    values = [_f(row[column]) for row in ok_rows(rows, system, fault)]
    values = [value for value in values if value is not None]
    return stats.summarize(values) if values else None


def fmt_ms(value: float) -> str:
    if value >= 10_000:
        return f"{value / 1000:,.1f} s"
    if value >= 1000:
        return f"{value / 1000:,.2f} s"
    return f"{value:,.0f} ms"


def fmt_count(value: float) -> str:
    return f"{value:,.0f}"


def error_classes(text: str) -> dict[str, int]:
    """The `label=count` pairs of an error_classes cell, labels normalised for display
    (the run classified some VTOP refusals as `other: <first 60 chars>`; the same refusal
    from different nodes is one class here), counts summed."""
    out: dict[str, int] = {}
    for part in (text or "").split("; "):
        label, _, count = part.rpartition("=")
        if not label or not count.isdigit():
            continue
        out[classify_label(label)] = out.get(classify_label(label), 0) + int(count)
    return out


def classify_label(label: str) -> str:
    if label.startswith("other:"):
        return classify_vtop_reason(label[len("other:"):])
    return label


def fmt_classes(text: str) -> str:
    classes = error_classes(text)
    return "; ".join(f"{label} {count}" for label, count in sorted(classes.items())) or "-"


def readback_sentence(rows: list[dict[str, str]]) -> str:
    """What the read-back of these repetitions found, from their `verify` and correctness columns."""
    failed = [row for row in rows if row["verify"].startswith("FAILED")]
    missing = [row for row in rows if not row["verify"]]
    lost = sum(int(float(row["acked_lost"])) for row in rows if row["acked_lost"])
    dups = sum(int(float(row["duplicates"])) for row in rows if row["duplicates"])
    if failed:
        return (f"The read-back FAILED in {len(failed)} of them; see the correctness table at the top.")
    if missing:
        return f"{len(missing)} of them never reached a read-back; see the correctness table at the top."
    return (f"The read-back of {'each' if len(rows) > 1 else 'it'} passed: {lost} acknowledged records lost, "
            f"{dups} duplicated.")


def no_outage(row: dict[str, str]) -> bool:
    """Whether the longest acknowledgement gap after the fault is within twice the longest gap of
    the steady 30 s before it: then the producer saw no outage that steady-state jitter does not
    also produce, and the "window" is that jitter, not a failover."""
    gap, steady = _f(row["unavailability_ms"]), _f(row["baseline_max_gap_ms"])
    return gap is not None and steady is not None and gap <= 2 * steady


def fmt_summary(s: stats.Summary | None, n_all: int, unit=fmt_ms) -> str:
    if s is None:
        return "no result"
    text = f"{unit(s.median)} ({unit(s.low)} - {unit(s.high)})"
    if s.n < n_all:
        text += f" [{s.n}/{n_all} reps]"
    return text


def footer_text(env: dict[str, Any]) -> str:
    c = env["comparable"]
    m = env["matrix"]
    limits = c["node_limits"]
    return (
        f"{c['host_cpu']} ({c['host_logical_cpus']} logical CPUs, {base.gib(c.get('host_memory_bytes'))}), "
        f"{c['host_os']} | "
        f"{c['docker_platform']}, VM {c['docker_vm_cpus']} CPUs / {base.gib(c.get('docker_vm_memory_bytes'))}, "
        f"kernel {c['docker_vm_kernel']}, "
        f"volumes {c['volume_filesystem']}\n"
        f"per node: {limits['cpus']} CPUs, {limits['mem_limit']} RAM | Kafka {c['kafka_image'].split('@')[0]} "
        f"(KRaft, 3 combined nodes) | VTOP {c['vtop_commit'][:12]} (3 co-located candidates) | clients: "
        f"{c['client_kafka']} vs {c['client_vtop']} | profile {c['profile']} (shipped timeouts) | "
        f"{m['offered_rate']:,} records/s offered | {m['repetitions']} repetitions | run {env['run_id']} "
        f"({env['started_at'][:10]}) | single host, indicative, not a production benchmark"
    )


# --------------------------------------------------------------------------- charts


def timeline_chart(plt, run: fschema.FailoverRun, img_dir: str) -> list[str]:
    from matplotlib.lines import Line2D

    env = run.environment
    faults = env["matrix"]["faults"]
    bin_ms = int(env["matrix"]["timeline_bin_ms"])
    width_in = 12.0
    height_in = 2.55 * len(faults) + 2.2
    fig, axes = plt.subplots(len(faults), 2, figsize=(width_in, height_in), squeeze=False, sharex=True, sharey=True)
    fig.subplots_adjust(left=0.07, right=0.99, top=1 - 1.55 / height_in, bottom=1.05 / height_in, hspace=0.42,
                        wspace=0.05)
    per_second = 1000 / bin_ms
    # The axis stops at a few times the offered rate: the backlog burst after an
    # outage can be 50x the steady rate, and drawn to scale it would flatten the
    # steady state and the outage into the baseline. A clipped burst says so.
    cap = env["matrix"]["offered_rate"] * 8
    for r, fault in enumerate(faults):
        for c, system in enumerate(SYSTEMS):
            ax = axes[r][c]
            base._style_axes(ax)
            ax.grid(axis="y")
            style = STYLE[system]
            series: dict[int, list[tuple[float, float]]] = {}
            for point in run.timeline:
                if point["system"] == system and point["fault"] == fault:
                    series.setdefault(int(point["repetition"]), []).append(
                        (float(point["t_ms"]) / 1000, int(point["records"]) * per_second))
            peak = 0.0
            for rep in sorted(series):
                xs, ys = zip(*sorted(series[rep]))
                ax.plot(xs, ys, color=style["color"], linewidth=0.9, alpha=0.75, zorder=3, drawstyle="steps-post")
                peak = max(peak, max(ys))
            if peak > cap:
                ax.text(0.99, 0.96, f"bursts clipped; highest {base.compact(peak)}/s", transform=ax.transAxes,
                        ha="right", va="top", fontsize=7, color=base.INK_2)
            ax.axvline(0, color=base.INK, linewidth=1.1, zorder=4)
            ax.text(0.4, 0.96, "fault", transform=ax.get_xaxis_transform(), fontsize=7.5, color=base.INK, va="top")
            if fault == "partition_leader":
                heal = env["matrix"]["heal_after_s"]
                ax.axvline(heal, color=base.INK_2, linewidth=1, linestyle=(0, (4, 3)), zorder=4)
                ax.text(heal + 0.4, 0.96, "healed", transform=ax.get_xaxis_transform(), fontsize=7.5,
                        color=base.INK_2, va="top")
            n_ok = len(series)
            n_all = len(all_rows(run.rows, system, fault))
            ax.set_title(f"{SHORT[system]}: {FAULT_LABELS[fault]} ({n_ok}/{n_all} repetitions)", loc="left",
                         fontsize=9)
            if not series:
                ax.text(0.5, 0.5, "no ok repetition", transform=ax.transAxes, ha="center", color=base.INK_2)
            if c == 0:
                ax.set_ylabel("records acknowledged / s")
            if r == len(faults) - 1:
                ax.set_xlabel("seconds from the fault")
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p: base._tick(v)))
    axes[0][0].set_ylim(0, cap)
    axes[0][0].set_xlim(env["matrix"]["timeline_from_ms"] / 1000, env["matrix"]["timeline_to_ms"] / 1000)
    handles = [Line2D([], [], color=STYLE[s]["color"], linewidth=1.6, label=STYLE[s]["label"]) for s in SYSTEMS]
    fig.legend(handles=handles, loc="upper left", ncol=2, fontsize=8.5, handlelength=2,
               bbox_to_anchor=(0.06, 1 - 0.95 / height_in))
    base._header(fig, height_in, "Acknowledgement rate around a leader fault, Apache Kafka vs VTOP",
                 base._wrap(f"One line per repetition, {bin_ms} ms bins, offered "
                            f"{env['matrix']['offered_rate']:,} records/s. A flat zero is the outage; the spike after "
                            "it is the backlog the outage held back. The two systems are driven by DIFFERENT "
                            "clients (a Kafka client cannot follow a VTOP failover).", 210))
    base._footer(fig, footer_text(env), 230)
    paths = base._save(fig, img_dir, "kafka-failover-timeline")
    plt.close(fig)
    return paths


def box_chart(plt, run: fschema.FailoverRun, img_dir: str) -> list[str]:
    from matplotlib.lines import Line2D

    env = run.environment
    faults = env["matrix"]["faults"]
    metrics = (("unavailability_ms", "Unavailability window (longest acknowledgement gap)"),
               ("time_to_leader_ms", "Time to new leader (fault to metadata naming it)"))
    width_in, height_in = 12.0, 5.6
    fig, axes = plt.subplots(1, len(metrics), figsize=(width_in, height_in))
    fig.subplots_adjust(left=0.07, right=0.99, top=1 - 1.6 / height_in, bottom=1.35 / height_in, wspace=0.18)
    offsets = {"kafka": -0.17, "vtop": 0.17}
    for ax, (column, title) in zip(axes, metrics):
        base._style_axes(ax)
        ax.set_yscale("log")
        ax.grid(axis="y", which="major")
        low = high = None
        for i, fault in enumerate(faults):
            for system in SYSTEMS:
                style = STYLE[system]
                measured = [_f(row[column]) for row in ok_rows(run.rows, system, fault)]
                measured = [v for v in measured if v is not None]
                values = [v for v in measured if v > 0]
                x = i + offsets[system]
                if len(values) < len(measured):
                    ax.text(x, 0.01, f"{len(measured) - len(values)} at <= 0 ms\nnot drawn", ha="center",
                            va="bottom", transform=ax.get_xaxis_transform(), fontsize=6.5, color=base.INK_2)
                if not values:
                    ax.text(x, 0.03, "no\nresult", transform=ax.get_xaxis_transform(), ha="center", fontsize=6.5,
                            color=base.INK_2)
                    continue
                low = min(values) if low is None else min(low, min(values))
                high = max(values) if high is None else max(high, max(values))
                ax.boxplot([values], positions=[x], widths=0.26, whis=(0, 100), showfliers=False,
                           patch_artist=True, manage_ticks=False,
                           boxprops={"facecolor": base.SURFACE, "edgecolor": style["color"], "linewidth": 1.4},
                           medianprops={"color": style["color"], "linewidth": 2.2},
                           whiskerprops={"color": style["color"], "linewidth": 1.1},
                           capprops={"color": style["color"], "linewidth": 1.1})
                spread = [x + (j - (len(values) - 1) / 2) * 0.035 for j in range(len(values))]
                ax.scatter(spread, values, marker=style["marker"], s=22, color=style["color"],
                           edgecolors=base.SURFACE, linewidths=0.8, zorder=4)
                # The label is the median of every measured value, as in the
                # headline table, whether or not each one could be drawn.
                median = stats.median(measured)
                ax.annotate(fmt_ms(median), (x, max(values)), xytext=(0, 5), textcoords="offset points",
                            ha="center", fontsize=7.5, color=base.INK)
        ax.set_xticks(range(len(faults)))
        ax.set_xticklabels([FAULT_LABELS[f].replace(" of the leader", "\nof the leader") for f in faults],
                           fontsize=8)
        ax.set_xlim(-0.6, len(faults) - 0.4)
        ax.set_title(title, loc="left", fontsize=9)
        ax.set_ylabel("ms (log scale)")
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p: fmt_ms(v)))
        ax.yaxis.set_minor_formatter(plt.NullFormatter())
        if low is not None:
            ax.set_ylim(max(0.5, low / 3), high * 2.5)
    handles = [Line2D([], [], color=STYLE[s]["color"], marker=STYLE[s]["marker"], linewidth=0, markersize=6.5,
                      markeredgecolor=base.SURFACE, label=STYLE[s]["label"]) for s in SYSTEMS]
    fig.legend(handles=handles, loc="upper left", ncol=2, fontsize=8.5, handlelength=1.4,
               bbox_to_anchor=(0.06, 1 - 0.98 / height_in))
    base._header(fig, height_in, "Failover under load: how long the producer waited, Apache Kafka vs VTOP",
                 base._wrap("Box: quartiles; line: median; whiskers: min-max; dots: each repetition (label: median). "
                            "Both systems at their shipped timeouts (Kafka broker.session.timeout.ms 9 s, VTOP lease "
                            "15 s). DIFFERENT clients: the JVM Kafka producer vs VTOP's native producer.", 210))
    base._footer(fig, footer_text(env), 230)
    paths = base._save(fig, img_dir, "kafka-failover-unavailability")
    plt.close(fig)
    return paths


# --------------------------------------------------------------------------- report


def write_markdown(run: fschema.FailoverRun, images: dict[str, str], csv_rel: dict[str, str], doc_path: str) -> None:
    import matplotlib

    env = run.environment
    rows = run.rows
    c = env["comparable"]
    m = env["matrix"]
    faults = m["faults"]
    reps = m["repetitions"]
    lines: list[str] = []
    add = lines.append
    add("# Failover under load: VTOP vs Apache Kafka, measured")
    add("")
    add("<!-- Generated by benchmarks/kafka_compare/report.py from the committed CSVs. Do not edit by hand: "
        "change the generator or re-run the benchmark. -->")
    add("")
    add(f"> **Single host, {c['host_cpu']} ({c['host_model']}), {c['docker_platform']}; indicative, not a "
        "production benchmark.** Both clusters ran as Linux containers in the same Docker Desktop VM, one at a "
        "time, with identical per-node limits.")
    add("")
    add("[#597](https://github.com/allamiro/vtop-engine/issues/597), part of "
        "[#595](https://github.com/allamiro/vtop-engine/issues/595); charts per "
        "[#600](https://github.com/allamiro/vtop-engine/issues/600). Raw data: "
        f"[`failover.csv`]({csv_rel['failover']}), [`timeline.csv`]({csv_rel['timeline']}) and "
        f"[`environment.json`]({csv_rel['environment']}), run `{env['run_id']}`. Every number below is generated "
        "from those files by `benchmarks/kafka_compare/report.py`.")
    add("")
    add("## First finding: a Kafka client cannot follow a VTOP failover")
    add("")
    add("The only VTOP topology that fails over by itself is the one the Helm chart renders for a replicated "
        "range: every node a **candidate**, the role following the metadata lease (#284). A candidate refuses the "
        "Kafka gateway at startup, as a co-located node and as a data node alike:")
    add("")
    add("```")
    add(CANDIDATE_GATEWAY_ERROR)
    add("```")
    add("")
    add("The gateway runs only on a `leader` or `standalone` node. A leased `leader` with followers can carry it "
        "(live-chaos scenario 20), but followers run no lease agent and never take the range themselves "
        "(`crates/vtop-node/src/lease_watcher.rs`): failing over that shape means restarting a follower with a "
        "rewritten leader config, which is what scenario 09 does by hand. So there is no promoted node for a Kafka "
        "client's metadata refresh to find, and no failover through the Kafka wire protocol to measure. The "
        "phase-1 static leader could not fail over at all (no lease, plaintext replica plane).")
    add("")
    add("**What was measured instead, labelled everywhere:** VTOP's failover through its **native** client "
        "(`vtop-node produce`, driven by `vtop/failover-driver.sh`, which looks the leader up in the range lease "
        "and resumes from the acknowledged floor), against Kafka's failover through its own JVM client. The "
        "clusters are the fair part of the comparison (three nodes each, identical limits, both voters of their "
        "metadata quorum and replicas of the partition); the clients are not the same program, so the numbers "
        "compare two client+cluster paths, not two clusters behind one client.")
    add("")
    add("## Correctness: acknowledged records lost and duplicated")
    add("")
    add("After every repetition the whole partition was read back: Kafka with `kafka-console-consumer.sh` from "
        "offset 0, every acknowledged value compared as text with what was sent; VTOP with `vtop-node verify`, "
        "which checks every visible record byte-exact against the producer's sequence, in order (a duplicate or a "
        "gap fails it).")
    add("")
    add("| system | fault | repetitions read back | acknowledged records lost | duplicates | read-back failures |")
    add("|---|---|---|---|---|---|")
    failures = []
    for system in SYSTEMS:
        for fault in faults:
            done = [row for row in all_rows(rows, system, fault) if row["verify"]]
            lost = sum(int(float(row["acked_lost"])) for row in done if row["acked_lost"])
            dups = sum(int(float(row["duplicates"])) for row in done if row["duplicates"])
            failed = [row for row in done if row["verify"].startswith("FAILED")]
            failures += failed
            add(f"| {STYLE[system]['label']} | {FAULT_LABELS[fault]} | {len(done)} of {reps} | **{lost}** | "
                f"**{dups}** | {len(failed)} |")
    add("")
    if failures:
        add("**READ-BACK FAILURES (the finding of this run):**")
        add("")
        for row in failures:
            add(f"- {row['system']} / {row['fault']} / repetition {row['repetition']}: `{row['verify']}` "
                f"(evidence under `logs/{row['system']}-{row['fault']}-{row['repetition']}/`)")
        add("")
    else:
        add("Every repetition that reached its read-back found every acknowledged record, once, byte-exact, on "
            "both systems.")
        add("")
    not_ok = [row for row in rows if row["status"] != "ok"]
    if not_ok:
        add("Repetitions that did not produce a measurement (recorded, not retried):")
        add("")
        for row in not_ok:
            add(f"- {row['system']} / {row['fault']} / repetition {row['repetition']}: **{row['status']}**, "
                f"{row['note']}")
        add("")
    add("## Headline")
    add("")
    add("Median (min - max) over the repetitions that ran, each system at its shipped timeouts.")
    add("")
    add("| fault | system | unavailability window (longest ack gap after the fault) | time to new leader | "
        "p99 latency, 30 s after | p99 latency, "
        "30 s before | client retries (each client's own count, not comparable) | acked lost | duplicates |")
    add("|---|---|---|---|---|---|---|---|---|")
    for fault in faults:
        for system in SYSTEMS:
            n_all = len(all_rows(rows, system, fault))

            def cell(column, unit=fmt_ms, s=system, f=fault, n=n_all):
                return fmt_summary(summary(rows, s, f, column), n, unit)

            add(f"| {FAULT_LABELS[fault]} | {SHORT[system]} | {cell('unavailability_ms')} | "
                f"{cell('time_to_leader_ms')} | {cell('p99_after_ms')} | {cell('p99_before_ms')} | "
                f"{cell('retries', fmt_count)} | {cell('acked_lost', fmt_count)} | "
                f"{cell('duplicates', fmt_count)} |")
    add("")
    quiet = [row for row in rows if row["status"] == "ok" and no_outage(row)]
    if quiet:
        add("The unavailability window is the longest gap between acknowledgements after the fault, so a "
            "repetition with no outage still reports one: its longest ordinary gap. In "
            + ", ".join(f"{SHORT[row['system']]} {row['fault']} repetition {row['repetition']} "
                        f"({fmt_ms(float(row['unavailability_ms']))}, starting {fmt_ms(float(row['gap_start_ms']))} "
                        f"after the fault, against a {fmt_ms(float(row['baseline_max_gap_ms']))} steady-state "
                        "longest gap)" for row in quiet)
            + " the gap is within twice the steady-state one: the producer saw no outage, and that value is "
            "jitter, not a failover time. It is still counted in the median and range above.")
        add("")
    batch_note = vtop_batch_pacing_note(env, rows)
    if batch_note:
        add(batch_note)
        add("")
    add(f"![Unavailability and time to leader]({images['unavailability']})")
    add("")
    add(f"![Acknowledgement rate around the fault]({images['timeline']})")
    add("")
    add("## Per repetition")
    add("")
    add("| system | fault | rep | status | leader | leader was metadata leader | unavailable | gap starts | "
        "steady max gap | to new leader | new leader | p99 after | sent errors | retries | re-routes | "
        "error classes | verify |")
    add("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for system in SYSTEMS:
        for fault in faults:
            for row in sorted(all_rows(rows, system, fault), key=lambda r: int(r["repetition"])):
                def ms(column, r=row):
                    value = _f(r[column])
                    return "-" if value is None else fmt_ms(value) if value >= 0 else f"{value:,.0f} ms"
                add(f"| {SHORT[system]} | {fault} | {row['repetition']} | {row['status']} | "
                    f"{row['leader_node'] or '-'} | {row['leader_was_controller'] or '-'} | "
                    f"{ms('unavailability_ms')} | {ms('gap_start_ms')} | {ms('baseline_max_gap_ms')} | "
                    f"{ms('time_to_leader_ms')} | {row['new_leader_node'] or '-'} | {ms('p99_after_ms')} | "
                    f"{row['send_errors'] or '-'} | {row['retries'] or '-'} | {row['reroutes'] or '-'} | "
                    f"{fmt_classes(row['error_classes']).replace('|', '/')} | {row['verify'] or '-'} |")
    add("")
    add("## Method")
    add("")
    add(METHOD.format(rate=m["offered_rate"], total=m["records_per_repetition"], warm=m["warmup_s"], pre=m["pre_s"],
                      post=m["post_s"], heal=m["heal_after_s"], grace=m["stop_grace_s"], batch=m["vtop_batch"],
                      value=m["vtop_value_bytes"], reps=reps, cpus=c["node_limits"]["cpus"],
                      mem=c["node_limits"]["mem_limit"], fs=c["volume_filesystem"]))
    add("")
    add("## Timeouts that decide these numbers (rule 5)")
    add("")
    observed = env.get("observed", {})
    broker = observed.get("kafka_broker_1_config", {})
    producer = observed.get("kafka_producer_config", {})
    t = env["timeouts"]
    add("| system | setting | value | source |")
    add("|---|---|---|---|")
    for key in ("broker.session.timeout.ms", "replica.lag.time.max.ms", "unclean.leader.election.enable"):
        add(f"| Kafka broker | `{key}` | `{broker.get(key, '?')}` | `kafka-configs --describe --all`, broker 1 |")
    for key in ("request.timeout.ms", "delivery.timeout.ms", "retry.backoff.ms", "metadata.max.age.ms",
                "enable.idempotence", "acks", "linger.ms", "metadata.recovery.strategy"):
        add(f"| Kafka client | `{key}` | `{producer.get(key, '?')}` | the producer's own config dump |")
    for key, value in t["vtop_lease"].items():
        if key != "source":
            add(f"| VTOP lease | `{key}` | `{value}` | {t['vtop_lease']['source']} |")
    for key, value in t["vtop_raft"].items():
        if key != "source":
            add(f"| VTOP metadata Raft | `{key}` | `{value}` | {t['vtop_raft']['source']} |")
    for key, value in t["vtop_driver"].items():
        if key == "source":
            continue
        # Runs before the #613 review filed the lease observer's poll under
        # the driver's shared source, which names only the JVM client
        # defaults the request timeout and back-off copy.
        source = LEASE_OBSERVER_SOURCE if key == "lease_observer_poll_ms" else t["vtop_driver"]["source"]
        add(f"| VTOP native driver | `{key}` | `{value}` | {source} |")
    for key, value in t.get("vtop_lease_observer", {}).items():
        if key != "source":
            add(f"| VTOP lease observer | `{key}` | `{value}` | {t['vtop_lease_observer']['source']} |")
    add(f"| both observers | poll interval | `{t['kafka_observer_poll_ms']} ms` | `metadata_probe.py`, "
        "`failover-driver.sh` |")
    add("")
    add("No `matched-timeouts` profile was run: these are the shipped defaults, and the failover times follow from "
        "them more than from either mechanism.")
    add("")
    add("## Reading the results")
    add("")
    add(READING)
    add("")
    # Produce attempts the VTOP leader answered with a refusal (rather than a lost
    # connection): the run classed them "produce rejected", later runs by their reason.
    rejected_labels = ("produce rejected", "quorum not reached (Overloaded)")
    refused = [(row, sum(error_classes(row["error_classes"]).get(label, 0) for label in rejected_labels))
               for row in rows if row["system"] == "vtop"]
    refused = [(row, count) for row, count in refused if count]
    if refused:
        add("- **Produce refused by a serving leader.** In " + ", ".join(
            f"{row['fault']} repetition {row['repetition']} ({count} attempts)" for row, count in refused)
            + ", a VTOP leader answered produce attempts with a refusal rather than an acknowledgement. "
            + readback_sentence([row for row, _ in refused])
            + " The driver log of each such repetition (`logs/vtop-<fault>-<rep>/driver.log`) has the exact lines.")
    add("")
    add("## Not measured here")
    add("")
    add(NOT_MEASURED)
    add("")
    add("## Environment")
    add("")
    add("| | |")
    add("|---|---|")
    for key in ("host_cpu", "host_model", "host_logical_cpus", "host_memory_bytes", "host_os", "docker_platform",
                "docker_server_version", "docker_vm_cpus", "docker_vm_memory_bytes", "docker_vm_kernel",
                "docker_vm_arch", "docker_storage_driver", "volume_filesystem", "kafka_image", "vtop_commit",
                "harness_commit", "vtop_image", "client_kafka", "client_vtop", "client_java", "observer_image",
                "profile"):
        add(f"| `{key}` | `{c.get(key)}` |")
    add(f"| run | `{env['run_id']}`, started {env['started_at']}, finished {env.get('finished_at', '?')} |")
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


def vtop_batch_pacing_note(env: dict[str, Any], rows: list[dict[str, str]]) -> str:
    """The run's VTOP latency caveat, when its producer sent batches early.

    Until the #613 review, `vtop-node produce --rate` sent a batch when its FIRST record was due, so
    the other records of the batch were on the wire before their scheduled time, and a latency taken
    from each record's schedule came out short by up to (batch - 1) / rate. Runs whose environment
    records `vtop_pacing: "last-record-due"` were taken after the fix and carry no caveat.
    """
    if env.get("comparable", {}).get("vtop_pacing") == "last-record-due":
        return ""
    m = env["matrix"]
    early_ms = (int(m["vtop_batch"]) - 1) / int(m["offered_rate"]) * 1000
    vtop = [row for row in rows if row["system"] == "vtop" and row["status"] == "ok"]
    if not vtop:
        return ""
    before = max(float(row["p99_before_ms"]) for row in vtop)
    after = min(float(row["p99_after_ms"]) for row in vtop)
    return (f"**VTOP latency in this run is understated by up to {early_ms:.1f} ms.** Its producer "
            f"(`vtop-node produce --rate`, batches of {m['vtop_batch']}) sent each batch when the batch's FIRST "
            f"record was due, so the batch's other records left up to {early_ms:.1f} ms before their scheduled "
            "send time, and a latency measured from the schedule is short by up to that much. Against VTOP's "
            f"30 s-before p99 (at most {fmt_ms(before)} here) that is not a small error, so that column is not "
            "comparable with Kafka's; against its 30 s-after p99 (at least "
            f"{fmt_ms(after)} here) it is. The unavailability window and the time to new leader do not use the "
            "schedule. The producer now waits until a batch's last record is due; a VTOP p99 before that is "
            "comparable needs a re-run.")


METHOD = """\
- **Clusters (rule 1).** Kafka: three combined broker+controller nodes in KRaft mode
  (`compose.kafka.yml`), the topic created with one partition, RF=3, `min.insync.replicas=2`. VTOP: three
  co-located nodes, each a metadata Raft voter and a candidate replica of one range
  (`compose.vtop-ha.yml`, `vtop/ha-node-*.yaml`), quorum acknowledgement (2 of 3), TLS on every plane
  (promotion is refused on a plaintext replica plane); Kafka's listeners are PLAINTEXT, as `compose.kafka.yml`
  ships them, so the VTOP side also pays TLS handshakes on every reconnect. Every node {cpus} CPUs, {mem}, no
  swap, a named volume on `{fs}`. Each repetition starts from empty volumes, one system at a time.
- **Load.** {rate:,} records/s offered for {total:,} records, `acks=all`, one partition, one producer. The
  rate is well under both systems' saturation (phase 1: about 24K records/s for VTOP at 100 B through the
  gateway; its native client reached about 16K records/s closed-loop in batches of {batch} while this
  benchmark was built), so neither cluster recovers from a full queue. Kafka:
  `kafka-verifiable-producer.sh --throughput {rate} --acks -1`, bootstrapped at all three brokers, idempotent
  and every timeout at the client's default; its records are the decimal sequence numbers (1-6 bytes, no
  key). VTOP: `vtop-node produce --rate {rate}` in batches of {batch} records (8-byte key, {value}-byte value),
  one producer identity for the run, `--request-timeout-ms 30000` and a 100 ms back-off (the JVM client's
  `request.timeout.ms` and `retry.backoff.ms` defaults). The driver looks the leader up in the range lease
  only when a produce ends, the way the JVM client refreshes metadata on an error; both schedules are anchored
  for the whole run, so what an outage held back is sent as a backlog afterwards.
- **Timeline.** {warm} s warm-up, {pre} s "before" window, the fault, {post} s "after". Faults, on the
  partition's leader (Kafka: the broker all three name as leader, ISR full; VTOP: the lease holder):
  `kill9_leader` = `docker kill --signal KILL`; `stop_leader` = `docker stop -t {grace}` (SIGTERM: Kafka's
  controlled shutdown, VTOP's orderly shutdown that releases the lease); `partition_leader` =
  `docker network disconnect`, reconnected after {heal} s. A killed or stopped node stays down for the rest
  of the repetition.
- **Measurements.** *Unavailability window*: the longest gap between consecutive acknowledgements that ends
  after the fault (for a crash, the last ack before the fault to the first after; it also catches an outage
  that starts after the signal, as an orderly stop's does). *Steady max gap*: the longest such gap in the 30 s
  before the fault, for scale. *Time to new leader*: fault to the first observation naming another leader:
  Kafka, any broker's Metadata answer (`metadata_probe.py`, each broker every 100 ms); VTOP, the range lease
  read every 100 ms. *Latency*: acknowledgement time minus the record's intended send time on the fixed-rate
  schedule (no coordinated omission), p99 of the records due in the 30 s after the fault vs the 30 s before.
  *Retries*: Kafka, the producer's logged retried requests; VTOP, produce attempts after the first.
  Timestamps are the Docker VM's clock (producer, observers); the fault is stamped on the host and moved onto
  that clock by a per-repetition `docker exec date` probe, whose half-width is recorded
  (`clock_uncertainty_ms`).
- **Repetition (rule 7).** {reps} repetitions per (system, fault), Kafka and VTOP alternating; median and
  min-max reported, every repetition listed. A repetition that failed is a row, not a retry."""

READING = """\
- **Crash (`kill -9`).** Both systems wait out a timeout before anything else can happen. Kafka's controller
  fences the dead broker after `broker.session.timeout.ms` (9 s) and elects a leader from the ISR; the client
  finds it on its next metadata refresh. VTOP's survivors cannot take the range until the dead holder's lease
  expires (15 s), then the winner fences the replicas at a new epoch and establishes the committed boundary
  from a quorum before serving. The node logs of every VTOP crash repetition (`logs/vtop-kill9_leader-*/`)
  show three steps after the fault, in order: (1) the lease lapses and a survivor is granted it (the time-to-
  leader column; the dead holder's last renewal sets how much of the 15 s remains); (2) the first promotion
  attempt is refused with `too few replicas could confirm the committed boundary ... answered=1 required=2`,
  because the other survivor has not yet adopted the new epoch from its own lease poll (2 s), and the retry
  succeeds (`verified promotion`); (3) **`range lease held` is followed about 10 s later by
  `data_node_role_changed role=leader`**: on taking the range, a candidate waits up to 10 s for a replica
  stream to EVERY peer before it serves (`crates/vtop-node/src/data_node.rs`, the wait before
  `LocalBroker::with_replication` in the candidate's leader build), and after a crash one peer is the dead
  node, so the full 10 s always elapses although a quorum (itself plus the live follower) is already
  there. Step 3 is a third or more of every VTOP crash window here and is not a safety requirement;
  waiting for a quorum of streams instead of all of them would remove it. Kafka has no counterpart: its
  window is the 9 s session timeout plus the election and the client's metadata refresh. Part of the
  remaining difference is 9 s vs 15 s, i.e. configuration; a matched-timeouts profile would separate the two.
- **Orderly stop.** Kafka's controlled shutdown moves leadership before the broker exits. VTOP releases its
  lease on SIGTERM (#280; the node logs `range lease released for shutdown`), so a survivor acquires it
  within its next lease poll instead of waiting out the expiry, which is why its time to new leader is short.
  The same two steps as after a crash then follow (a refused first promotion attempt, then the ~10 s wait for
  a replica stream to the node that has just stopped), so the producer's window is much longer than the
  leader handover itself.
- **Partition.** The isolated leader cannot be told it is deposed. Kafka's controller fences it after the
  session timeout and elects another leader, but the JVM producer holds its in-flight request to the isolated
  broker until `request.timeout.ms` (30 s) before it refreshes metadata; VTOP's isolated holder cannot renew its
  lease, and the native driver also waits out its 30 s request timeout. In both, the client's request timeout,
  not the cluster's election, sets the window.
- **What VTOP's promotion adds** (verified boundary, fenced epochs, the transition record in metadata) costs
  little time next to the lease expiry; it is why no acknowledged record may be lost, and the read-back table
  above is the evidence for this run. Issue #611 describes a suspected path where a promotion could truncate a
  quorum-acknowledged record; this benchmark's read-backs did not hit it, which is not evidence that it cannot
  happen (#611 needs its deterministic test).
- **The clients differ.** The JVM producer buffers records while it has no leader and sends them as soon as
  metadata names one; the native driver re-reads the lease only after its produce ends (connection lost,
  refused, or a 30 s request timeout). Part of every VTOP window is that client's behaviour, not the cluster's."""

NOT_MEASURED = """\
- **The Kafka client path on VTOP** (see the first finding). No VTOP number on this page is a Kafka client's.
- **#597's other faults:** `kill -9` of the metadata leader alone (F2) and of a follower (F5). In both
  co-located clusters the partition leader is often also the metadata leader; the per-repetition table says
  when it was.
- **`matched-timeouts` profile** and the idempotent producer turned off (#597 metric 5 asks for both).
- **Ten kills per cell** (#595 rule 7 for failover): this run has the repetitions its matrix records.
- **Evidence left behind** (#597 metric 7): VTOP's transition record is not read back here.
- **Laptop, one host.** Docker Desktop's VM; network partitions are `docker network disconnect`, not a
  packet-loss link."""


def build(run_dir: str, img_dir: str, doc_path: str, setup_matplotlib) -> list[str]:
    run = fschema.load_failover_run(run_dir)  # raises ReportRefused before anything is written
    plt = setup_matplotlib()
    os.makedirs(img_dir, exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(doc_path)), exist_ok=True)
    written = []
    written += box_chart(plt, run, img_dir)
    written += timeline_chart(plt, run, img_dir)
    doc_dir = os.path.dirname(os.path.abspath(doc_path))
    images = {key: os.path.relpath(os.path.join(img_dir, f"kafka-failover-{key}.png"), doc_dir)
              for key in ("unavailability", "timeline")}
    rel = {name: os.path.relpath(os.path.join(run_dir, f"{name}.{ext}"), doc_dir)
           for name, ext in (("failover", "csv"), ("timeline", "csv"), ("environment", "json"))}
    write_markdown(run, images, rel, doc_path)
    written.append(doc_path)
    return written
