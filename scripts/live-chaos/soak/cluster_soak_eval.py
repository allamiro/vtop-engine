#!/usr/bin/env python3
"""The cluster-plane soak's pass/fail evaluator (#727, criterion A3 of #714).

It reads what `cluster-soak.sh` recorded and decides, criterion by criterion,
whether the run shows what A3 asks for: seven days of three metadata and
three data nodes under continuous produce with a fault every hour, and

  * no acknowledged record lost: the operation history (#728) holds every
    acknowledged record once, at its offset, and nothing nobody wrote;
  * one leader per epoch: no fencing epoch was ever led by two data nodes,
    and no Raft term by two metadata nodes;
  * bounded memory and disk: no growth trend in any node's memory or open
    files, nor in a data node's directory, and that directory stays under
    the ceiling retention promises;
  * `vtopctl segment verify` clean on every replica at the end.

It is built on the engine soak's evaluator (benchmarks/soak/soak_eval.py,
#726): the same criterion record, Theil-Sen trend fit per process lifetime,
coverage rule, fault-plan check and report, so the two soaks judge the same
way. A criterion with nothing to judge fails; it never passes vacuously.

Inputs, in the run directory: profile.json, events.jsonl, samples.jsonl,
leaders.jsonl, finals.json and history-check.json (check_history.py's
summary over history/). Exit status: 0 every criterion passed, 1 one or more
failed, 2 an input could not be read.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import cluster_soak  # noqa: E402  (puts benchmarks/soak on the path)
import soak_eval  # noqa: E402
from soak_eval import Criterion, InputError, _coverage, _fmt, read_jsonl, sha256_of, theil_sen  # noqa: E402

EVALUATOR_VERSION = 1


def _nodes(samples: list[dict], kind: str | None = None) -> list[str]:
    names: set[str] = set()
    for sample in samples:
        for name, node in (sample.get("nodes") or {}).items():
            if kind is None or node.get("kind") == kind:
                names.add(name)
    return sorted(names)


def check_samples(profile: dict, samples: list[dict]) -> Criterion:
    """Every node sampled throughout: a record that found the node up and
    measured it counts, one each `sample_interval_s`."""
    criteria = profile["criteria"]
    duration = float(profile["duration_s"])
    wanted = criteria["min_samples_fraction"] * duration / profile["sample_interval_s"]
    nodes = _nodes(samples)
    said, ok = [], bool(nodes)
    for name in nodes:
        times = [
            s["elapsed_s"]
            for s in samples
            if (s.get("nodes") or {}).get(name, {}).get("up") and s["nodes"][name].get("rss_bytes") is not None
        ]
        good, text = _coverage(times, wanted, criteria["max_sample_gap_s"], duration)
        ok = ok and good
        if not good:
            said.append(f"{name}: {text}")
    return Criterion(
        "samples",
        "every node was sampled throughout",
        ok and len(nodes) == 6,
        f"{len(nodes)} nodes sampled" + (f"; short: {'; '.join(said)}" if said else ""),
        f"six nodes, each at least {criteria['min_samples_fraction']:.0%} of its interval's worth "
        f"over {duration:.0f} s, no gap over {criteria['max_sample_gap_s']} s",
    )


def check_unplanned_exits(events: list[dict]) -> Criterion:
    bad = [e for e in events if e.get("kind") == "node_exit" and not e.get("planned")]
    return Criterion(
        "no_unplanned_exit",
        "no node exits except when a fault stops it",
        not bad,
        f"{len(bad)} unplanned exit(s)",
        "0",
        "; ".join(f"{e.get('node')} at {e['elapsed_s']:.0f} s" for e in bad[:5]),
    )


def check_history(history: dict | None) -> Criterion:
    expected = "the history check valid: no acknowledged record lost, duplicated or reordered, no phantom"
    title = "no acknowledged record was lost (the operation history, #728)"
    if not history:
        return Criterion("history", title, False, "no history check was recorded", expected)
    violations = history.get("violations") or {}
    observed = (
        f"{history.get('acknowledged_records', 0)} acknowledged records over {history.get('reads', 0)} reads; "
        f"batches {history.get('batches', {})}; "
        + (", ".join(f"{kind} {v['count']}" for kind, v in violations.items()) or "no violation")
    )
    first = [f"{kind}: {v['first'][0]}" for kind, v in violations.items() if v.get("first")]
    passed = bool(history.get("valid")) and history.get("acknowledged_records", 0) > 0
    return Criterion("history", title, passed, observed, expected, "; ".join(first[:5]))


def check_progress(profile: dict, events: list[dict], history: dict | None) -> Criterion:
    """The load kept going: enough records acknowledged for the time it ran."""
    stops = [e["elapsed_s"] for e in events if e.get("kind") == "producer_stopped"]
    starts = [e["elapsed_s"] for e in events if e.get("kind") == "load_start"]
    ran = (min(stops) if stops else float(profile["duration_s"])) - (min(starts) if starts else 0.0)
    acked = (history or {}).get("acknowledged_records", 0)
    wanted = profile["criteria"]["min_acked_fraction"] * profile["rate_records_per_s"] * max(ran, 0.0)
    return Criterion(
        "progress",
        "the producer kept acknowledging through the run",
        acked > 0 and acked >= wanted,
        f"{acked} records acknowledged over {ran:.0f} s",
        f"at least {wanted:.0f} ({profile['criteria']['min_acked_fraction']:.0%} of "
        f"{profile['rate_records_per_s']}/s)",
    )


def check_reads(profile: dict, events: list[dict]) -> Criterion:
    duration = float(profile["duration_s"])
    times = [e["elapsed_s"] for e in events if e.get("kind") == "read" and e.get("ok")]
    wanted = 0.5 * duration / profile["read_interval_s"]
    good, text = _coverage(times, wanted, profile["criteria"]["max_read_gap_s"], duration)
    final = [e for e in events if e.get("kind") == "final_read"]
    final_ok = bool(final) and bool(final[-1].get("ok"))
    return Criterion(
        "reads",
        "the range was read throughout, and once more after the load stopped",
        good and final_ok,
        f"successful reads {text}; final read {'ok' if final_ok else 'missing or failed'}",
        f"at least half of one every {profile['read_interval_s']} s, no gap over "
        f"{profile['criteria']['max_read_gap_s']} s, and a final read",
    )


def check_read_integrity(events: list[dict]) -> Criterion:
    """No read was stopped by what the leader served: verify gives up before
    it records such a read, so the history cannot judge it, and one bad read
    among good ones is still a broken promise."""
    bad = [e for e in events if e.get("kind") == "read" and e.get("integrity")]
    return Criterion(
        "read_integrity",
        "no read found the leader serving what the log forbids",
        not bad,
        f"{len(bad)} read(s) stopped on a served record at or above the committed high watermark, "
        "offsets going backwards or the watermark going back",
        "0",
        "; ".join(f"at {e['elapsed_s']:.0f} s from {e.get('node')}: {str(e.get('detail', ''))[-160:]}" for e in bad[:3]),
    )


def check_range_leaders(samples: list[dict], leaders: list[dict]) -> Criterion:
    """No fencing epoch led by two data nodes: from every `role=leader` the
    nodes logged, and from every sample in which a node said it leads."""
    by_epoch: dict[int, set[str]] = {}
    for row in leaders:
        by_epoch.setdefault(int(row["epoch"]), set()).add(row["node"])
    sampled = 0
    for sample in samples:
        for name, node in (sample.get("nodes") or {}).items():
            metrics = node.get("metrics") or {}
            if node.get("kind") == "data" and metrics.get("vtop_broker_candidate_leading") == 1:
                epoch = metrics.get("vtop_broker_held_fencing_epoch")
                if epoch is not None:
                    sampled += 1
                    by_epoch.setdefault(int(epoch), set()).add(name)
    shared = {epoch: sorted(nodes) for epoch, nodes in sorted(by_epoch.items()) if len(nodes) > 1}
    return Criterion(
        "one_range_leader_per_epoch",
        "one data node led each fencing epoch",
        bool(by_epoch) and not shared,
        f"{len(by_epoch)} epochs led ({len(leaders)} logged leaderships, {sampled} leading samples); "
        f"{len(shared)} epoch(s) led by more than one node",
        "every epoch led by exactly one node, and at least one leadership observed",
        "; ".join(f"epoch {e}: {', '.join(n)}" for e, n in list(shared.items())[:5]),
    )


def check_meta_leaders(samples: list[dict]) -> Criterion:
    by_term: dict[int, set[str]] = {}
    for sample in samples:
        for name, node in (sample.get("nodes") or {}).items():
            metrics = node.get("metrics") or {}
            if node.get("kind") == "meta" and metrics.get("raft_leader"):
                term = metrics.get("vtop_meta_raft_term")
                if term is not None:
                    by_term.setdefault(int(term), set()).add(name)
    shared = {term: sorted(nodes) for term, nodes in sorted(by_term.items()) if len(nodes) > 1}
    return Criterion(
        "one_meta_leader_per_term",
        "one metadata node led each Raft term",
        bool(by_term) and not shared,
        f"{len(by_term)} terms with a sampled leader; {len(shared)} with more than one",
        "every term led by at most one node, and at least one leader sampled",
        "; ".join(f"term {t}: {', '.join(n)}" for t, n in list(shared.items())[:5]),
    )


def series_points(samples: list[dict], name: str, series: str) -> list[tuple[float, float, int]]:
    """(elapsed_s, value, lifetime) for one node's series, while it was up."""
    points = []
    for sample in samples:
        node = (sample.get("nodes") or {}).get(name)
        if not node or not node.get("up"):
            continue
        value = node.get(series)
        if isinstance(value, (int, float)):
            points.append((float(sample["elapsed_s"]), float(value), int(node.get("lifetime", 0))))
    return points


def check_trends(profile: dict, samples: list[dict]) -> list[Criterion]:
    """No growth trend over the last `trend_window_fraction` of the run, per
    series and node, fitted within each process lifetime (soak_eval.theil_sen):
    growth fails when it is over both the absolute and the relative limit."""
    criteria = profile["criteria"]
    duration = float(profile["duration_s"])
    fraction = criteria["trend_window_fraction"]
    window = (duration * (1 - fraction), duration)
    out = []
    for series, meta in cluster_soak.TREND_SERIES.items():
        limits = criteria["trend"][series]
        nodes = [n for kind in meta["nodes"] for n in _nodes(samples, kind)]
        said, failed = [], []
        for name in nodes:
            trend = theil_sen(series_points(samples, name, series), window, criteria["trend_min_pairs"])
            if trend is None:
                failed.append(f"{name}: too few points to fit a trend")
                continue
            grew = trend.growth > limits["min_abs_growth"] and trend.growth_pct > limits["max_growth_pct"]
            pct = "unbounded" if math.isinf(trend.growth_pct) else f"{trend.growth_pct:+.1f}%"
            text = f"{name} {_fmt(trend.growth, meta['unit'])} ({pct}) from {_fmt(trend.level, meta['unit'])}"
            said.append(text)
            if grew:
                failed.append(text)
        out.append(
            Criterion(
                f"trend_{series}",
                f"no node's {meta['label']} shows a growth trend",
                bool(nodes) and not failed,
                "; ".join(said) or "no node measured",
                f"growth over the last {fraction:.0%} of the run at most {limits['max_growth_pct']}% "
                f"or {_fmt(limits['min_abs_growth'], meta['unit'])}, per node",
                "; ".join(failed[:6]),
            )
        )
    return out


def check_ceilings(profile: dict, samples: list[dict]) -> list[Criterion]:
    criteria = profile["criteria"]
    out = []
    for series, limit, kind, title in (
        ("data_dir_bytes", criteria["max_data_dir_bytes"], "data", "every data directory stayed under its ceiling"),
        ("rss_bytes", criteria["max_rss_bytes"], None, "every node's memory stayed under its ceiling"),
    ):
        peaks = {}
        for name in _nodes(samples, kind):
            values = [v for _, v, _ in series_points(samples, name, series)]
            if values:
                peaks[name] = max(values)
        over = {name: peak for name, peak in peaks.items() if peak > limit}
        out.append(
            Criterion(
                f"ceiling_{series}",
                title,
                bool(peaks) and not over,
                "; ".join(f"{name} peak {_fmt(peak, 'bytes')}" for name, peak in sorted(peaks.items()))
                or "nothing measured",
                f"at most {_fmt(limit, 'bytes')}",
            )
        )
    return out


def check_segments(finals: dict) -> Criterion:
    replicas = (finals or {}).get("replicas") or {}
    problems = []
    for name in ("data-1", "data-2", "data-3"):
        replica = replicas.get(name)
        if replica is None:
            problems.append(f"{name}: not verified")
        elif replica.get("failed"):
            problems.append(f"{name}: {', '.join(replica['failed'][:3])}")
        elif not replica.get("segments"):
            problems.append(f"{name}: no segment to verify")
    return Criterion(
        "segment_verify",
        "vtopctl segment verify is clean on every replica",
        not problems,
        "; ".join(
            f"{name} {r.get('segments', 0)} segments, {len(r.get('failed') or [])} failed"
            for name, r in sorted(replicas.items())
        )
        or "no replica verified",
        "every segment of all three replicas verifies",
        "; ".join(problems),
    )


def _cluster_titles(criteria: list[Criterion]) -> list[Criterion]:
    for criterion in criteria:
        criterion.title = criterion.title.replace("the engine recovered", "the cluster recovered")
    return criteria


def evaluate(
    profile: dict,
    samples: list[dict],
    events: list[dict],
    leaders: list[dict],
    finals: dict,
    history: dict | None,
) -> dict:
    samples = sorted(samples, key=lambda s: s["elapsed_s"])
    events = sorted(events, key=lambda e: e["elapsed_s"])
    criteria: list[Criterion] = [
        soak_eval.check_run_completed(profile, samples, events),
        check_samples(profile, samples),
        *_cluster_titles(soak_eval.check_faults(profile, events)),
        check_unplanned_exits(events),
        check_history(history),
        check_progress(profile, events, history),
        check_reads(profile, events),
        check_read_integrity(events),
        check_range_leaders(samples, leaders),
        check_meta_leaders(samples),
        *check_trends(profile, samples),
        *check_ceilings(profile, samples),
        check_segments(finals),
    ]
    return {
        "evaluator_version": EVALUATOR_VERSION,
        "profile": profile["name"],
        "passed": all(c.passed for c in criteria),
        "criteria": [c.as_dict() for c in criteria],
    }


def _json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise InputError(f"cannot read {path}: {error}") from None


def evaluate_run_dir(profile: dict, run_dir: Path) -> dict:
    paths = {name: run_dir / name for name in (
        "samples.jsonl", "events.jsonl", "leaders.jsonl", "finals.json", "history-check.json"
    )}
    samples = read_jsonl(paths["samples.jsonl"])
    events = read_jsonl(paths["events.jsonl"])
    leaders = read_jsonl(paths["leaders.jsonl"]) if paths["leaders.jsonl"].exists() else []
    for index, row in enumerate(samples):
        if not isinstance(row.get("elapsed_s"), (int, float)):
            raise InputError(f"{paths['samples.jsonl']} record {index + 1} has no numeric elapsed_s")
    for index, row in enumerate(events):
        if not isinstance(row.get("elapsed_s"), (int, float)) or not isinstance(row.get("kind"), str):
            raise InputError(f"{paths['events.jsonl']} record {index + 1} lacks elapsed_s or kind")
    verdict = evaluate(
        profile,
        samples,
        events,
        leaders,
        _json(paths["finals.json"], {}),
        _json(paths["history-check.json"], None),
    )
    verdict["inputs"] = {name: sha256_of(path) for name, path in paths.items()}
    return verdict


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Decide whether a cluster soak run passed.")
    parser.add_argument("--profile", required=True, help="a profile name (7d, smoke) or a path to one")
    parser.add_argument("--run-dir", required=True, type=Path, help="the run directory the driver wrote")
    parser.add_argument("--out", type=Path, help="where to write verdict.json (default: <run-dir>/verdict.json)")
    args = parser.parse_args(argv)
    try:
        profile = cluster_soak.load(args.profile)
        verdict = evaluate_run_dir(profile, args.run_dir)
    except (cluster_soak.ProfileError, InputError) as error:
        print(f"cluster_soak_eval: {error}", file=sys.stderr)
        return 2
    out = args.out or args.run_dir / "verdict.json"
    out.write_text(json.dumps(verdict, indent=2) + "\n")
    print(soak_eval.render(verdict).replace("soak verdict", "cluster soak verdict", 1))
    return 0 if verdict["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
