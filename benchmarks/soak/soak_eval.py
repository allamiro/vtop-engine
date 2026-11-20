#!/usr/bin/env python3
"""The soak's pass/fail evaluator (#726).

It reads what a run recorded and decides, criterion by criterion, whether the
run proves what the issue asks for. It asks the engine nothing: the samples,
the events and the final checks are files the driver wrote, and the verdict
is a function of those files and the profile, so a maintainer can re-run it
over an archived evidence tarball and get the same answer.

Inputs, in a run directory:

  samples.jsonl  one object per sample: elapsed_s, lifetime (which engine
                 process), engine_up, rss_bytes, open_fds, work_dir_bytes,
                 ledger_bytes, disk_free_bytes, metrics (the engine's own
                 counters, summed over their labels) and, when taken, lag
                 (the topic's end offset and the group's committed offset).
  events.jsonl   run_start, engine_start, engine_exit, fault_start,
                 fault_end, recovered, prune, audit, scrub, run_end.
  finals.json    the checks after the load stopped and the engine drained:
                 audit_coverage, scrub, tail.

A criterion with nothing to judge fails; it does not pass vacuously. A run
that injected no fault, or sampled nothing, or ran no scrub, is the run most
likely to look healthy, so each of those is its own criterion.

Exit status: 0 every criterion passed, 1 one or more failed, 2 an input could
not be read.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path

import soak_profile

EVALUATOR_VERSION = 1

# The counters the evaluator reads from `metrics` (the driver sums each over
# its labels). Names are the engine's own, as /metrics renders them.
M_COMMITS = "vtop_commits_total"
M_VERIFIED = "vtop_verified_total"
M_VERIFY_FAILURES = "vtop_verification_failures_total"
M_BATCH_SUM = "vtop_batch_duration_seconds_sum"
M_BATCH_COUNT = "vtop_batch_duration_seconds_count"

# A trend is fitted on at most this many bucket medians per engine lifetime,
# so the pairwise fit stays small for 8,000 samples and noise within a bucket
# is averaged away before it is fitted.
MAX_BUCKETS_PER_LIFETIME = 240


class InputError(ValueError):
    """A recorded input that cannot be read, naming the file and line."""


@dataclass
class Criterion:
    id: str
    title: str
    passed: bool
    observed: str
    expected: str
    detail: str = ""

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "title": self.title,
            "passed": self.passed,
            "observed": self.observed,
            "expected": self.expected,
            "detail": self.detail,
        }


@dataclass
class Trend:
    """A fitted trend over one series."""

    n_points: int
    n_pairs: int
    slope_per_s: float
    window_s: float
    level: float

    @property
    def growth(self) -> float:
        return self.slope_per_s * self.window_s

    @property
    def growth_pct(self) -> float:
        if self.level > 0:
            return 100.0 * self.growth / self.level
        return math.inf if self.growth > 0 else 0.0


# ---------------------------------------------------------------------------
# Reading the recorded files
# ---------------------------------------------------------------------------


def read_jsonl(path: Path) -> list[dict]:
    """Every line of a JSONL file as an object. A line that is not one is an
    error that names the line: a truncated or hand-edited record must not be
    skipped, since skipping it would change the verdict."""
    rows: list[dict] = []
    try:
        text = path.read_text()
    except OSError as error:
        raise InputError(f"cannot read {path}: {error}") from None
    for number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise InputError(f"{path} line {number} is not JSON ({error})") from None
        if not isinstance(row, dict):
            raise InputError(f"{path} line {number} is not a JSON object")
        rows.append(row)
    return rows


def sha256_of(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ---------------------------------------------------------------------------
# Small numeric helpers
# ---------------------------------------------------------------------------


def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile of a non-empty list."""
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100.0 * len(ordered)))
    return ordered[rank - 1]


def counter_increase(values: list[float | None]) -> float:
    """How far a counter rose over its scrapes, across engine restarts.

    A counter starts again at zero when the process does, so a value below the
    one before it is a reset, and the new value is what it counted since. A
    `kill -9` loses what the engine counted after its last scrape, so across a
    kill this is a lower bound, never an over-count.
    """
    total = 0.0
    previous: float | None = None
    for value in values:
        if value is None:
            continue
        if previous is None:
            total += value
        elif value >= previous:
            total += value - previous
        else:
            total += value
        previous = value
    return total


def theil_sen(points: list[tuple[float, float, int]], window: tuple[float, float], min_pairs: int) -> Trend | None:
    """The Theil-Sen trend of (time, value, lifetime) points inside `window`.

    Points are first reduced to the median of each time bucket, per engine
    lifetime, and slopes are taken only between buckets of the same lifetime:
    an engine that restarts drops its memory and open files back to where they
    started, and that is not a trend. The median of the pairwise slopes does not
    move for a burst while a fault is active, which a least-squares line would.
    Returns None when there are fewer than `min_pairs` usable pairs, so the
    caller can fail the criterion instead of passing on nothing.
    """
    low, high = window
    if high <= low:
        return None
    inside = [(t, v, life) for t, v, life in points if low <= t <= high]
    if not inside:
        return None
    width = max((high - low) / MAX_BUCKETS_PER_LIFETIME, 1e-9)
    buckets: dict[tuple[int, int], list[float]] = {}
    times: dict[tuple[int, int], list[float]] = {}
    for t, v, life in inside:
        key = (life, int((t - low) // width))
        buckets.setdefault(key, []).append(v)
        times.setdefault(key, []).append(t)
    reduced = sorted(
        (statistics.fmean(times[key]), statistics.median(values), key[0]) for key, values in buckets.items()
    )
    # Neighbouring buckets give slopes dominated by noise; pair buckets that
    # are at least two bucket widths apart.
    gap = 2 * width
    slopes: list[float] = []
    for i, (ti, vi, li) in enumerate(reduced):
        for tj, vj, lj in reduced[i + 1 :]:
            if lj == li and tj - ti >= gap:
                slopes.append((vj - vi) / (tj - ti))
    if len(slopes) < min_pairs:
        return None
    return Trend(
        n_points=len(inside),
        n_pairs=len(slopes),
        slope_per_s=statistics.median(slopes),
        window_s=high - low,
        level=statistics.median(v for _, v, _ in reduced),
    )


# ---------------------------------------------------------------------------
# Deriving series from the samples
# ---------------------------------------------------------------------------


def fault_windows(profile: dict, events: list[dict]) -> list[tuple[float, float]]:
    """The spans during which a fault is active or the engine is still
    recovering from it: from fault_start to the `recovered` event, plus two lag
    intervals for the archive to catch up. A fault with no recovery event
    holds its span for the recovery timeout."""
    grace = 2 * profile["lag_interval_s"]
    recovered = {e["id"]: e for e in events if e.get("kind") == "recovered" and "id" in e}
    end_of_fault = {e["id"]: e for e in events if e.get("kind") == "fault_end" and "id" in e}
    windows = []
    for event in events:
        if event.get("kind") != "fault_start":
            continue
        start = event["elapsed_s"]
        fid = event.get("id")
        if fid in recovered:
            end = recovered[fid]["elapsed_s"]
        elif fid in end_of_fault:
            end = end_of_fault[fid]["elapsed_s"] + profile["recovery_timeout_s"]
        else:
            end = start + profile["recovery_timeout_s"]
        windows.append((start, end + grace))
    return windows


def in_any(t: float, windows: list[tuple[float, float]]) -> bool:
    return any(low <= t <= high for low, high in windows)


def series_points(profile: dict, samples: list[dict], events: list[dict], name: str) -> list[tuple[float, float, int]]:
    """(elapsed_s, value, lifetime) for one trend series, with the samples the
    series leaves out already removed (see soak_profile.TREND_SERIES)."""
    stop = load_end(profile, events)
    exclude = soak_profile.TREND_SERIES[name]["fault_windows"] == "exclude"
    windows = fault_windows(profile, events) if exclude else []
    points: list[tuple[float, float, int]] = []
    previous: dict | None = None
    for sample in samples:
        t = sample["elapsed_s"]
        life = sample.get("lifetime", 0)
        value: float | None = None
        if name == "batch_seconds":
            # Counters are compared between consecutive samples that carry them
            # (a lag-only sample has none) from the same engine process.
            m = sample.get("metrics")
            if m:
                p = (previous or {}).get("metrics") or {}
                if (
                    previous is not None
                    and previous.get("lifetime", 0) == life
                    and M_BATCH_COUNT in m
                    and M_BATCH_COUNT in p
                ):
                    dc = m[M_BATCH_COUNT] - p[M_BATCH_COUNT]
                    ds = m.get(M_BATCH_SUM, 0) - p.get(M_BATCH_SUM, 0)
                    if dc > 0 and ds >= 0:
                        value = ds / dc
                previous = sample
        elif name == "lag_seconds":
            lag = sample.get("lag")
            if lag and lag.get("end") is not None and lag.get("committed") is not None and t <= stop:
                value = max(0, lag["end"] - lag["committed"]) / profile["rate_records_per_s"]
        else:
            if sample.get("engine_up"):
                value = sample.get(name)
        if value is None or (exclude and in_any(t, windows)):
            continue
        points.append((t, float(value), life))
    return points


# ---------------------------------------------------------------------------
# The criteria
# ---------------------------------------------------------------------------


def _fmt(value: float, unit: str) -> str:
    if unit == "bytes":
        for suffix, scale in (("GiB", 2**30), ("MiB", 2**20), ("KiB", 2**10)):
            if abs(value) >= scale:
                return f"{value / scale:.1f} {suffix}"
        return f"{value:.0f} B"
    if unit == "seconds":
        return f"{value:.1f} s"
    return f"{value:.0f}"


def check_run_completed(profile: dict, samples: list[dict], events: list[dict]) -> Criterion:
    end = [e for e in events if e.get("kind") == "run_end"]
    last = max((s["elapsed_s"] for s in samples), default=0.0)
    expected = f"run_end recorded, not aborted, and sampled to at least {profile['duration_s']} s"
    if not end:
        return Criterion(
            "run_completed", "the run completed", False, f"no run_end event; last sample at {last:.0f} s", expected
        )
    aborted = end[-1].get("aborted")
    if aborted:
        return Criterion("run_completed", "the run completed", False, f"aborted: {aborted}", expected)
    if last < profile["duration_s"]:
        return Criterion(
            "run_completed",
            "the run completed",
            False,
            f"last sample at {last:.0f} s of {profile['duration_s']} s",
            expected,
        )
    return Criterion("run_completed", "the run completed", True, f"sampled to {last:.0f} s", expected)


def load_end(profile: dict, events: list[dict]) -> float:
    """When the load stopped: the producer's stop, or the profile's duration when
    no stop was recorded. What comes after is the engine draining and the final
    checks running, with nothing being produced, so it is not part of the rate
    or of the steady state."""
    stops = [e["elapsed_s"] for e in events if e.get("kind") == "producer_stopped"]
    return min(stops) if stops else float(profile["duration_s"])


def _coverage(times: list[float], wanted: float, limit: float, high: float) -> tuple[bool, str]:
    """Whether `times` (inside [0, high]) number at least `wanted` with no gap
    over `limit`, and how to say so."""
    inside = sorted(t for t in times if 0 <= t <= high)
    gap = max((b - a for a, b in zip(inside, inside[1:])), default=math.inf)
    ok = len(inside) >= wanted and gap <= limit
    return ok, f"{len(inside)} (of at least {wanted:.0f}), largest gap {gap:.0f} s" if len(
        inside
    ) > 1 else f"{len(inside)}"


def check_samples(profile: dict, samples: list[dict]) -> Criterion:
    """Each kind of observation is counted on its own: the resource samples (the
    engine's memory, open files, ledger and work directory, one record each
    `sample_interval_s`) and the lag samples (the topic and the group, one each
    `lag_interval_s`) share a file, and one kind must not stand in for the
    other. A record that observed nothing (the engine was down, a query failed)
    is not a sample of either."""
    criteria = profile["criteria"]
    duration = float(profile["duration_s"])
    limit = criteria["max_sample_gap_s"]
    fraction = criteria["min_samples_fraction"]
    # An observation counts when it observed something: a resource record with
    # the engine's measurements (not one written while the engine was down or
    # unreachable), and a lag record with both offsets (not one a failed query
    # left empty).
    resource = [s["elapsed_s"] for s in samples if s.get("engine_up") is True and s.get("rss_bytes") is not None]
    lag = [
        s["elapsed_s"]
        for s in samples
        if isinstance(s.get("lag"), dict) and s["lag"].get("end") is not None and s["lag"].get("committed") is not None
    ]
    ok_resource, said_resource = _coverage(
        resource, fraction * duration / profile["sample_interval_s"], limit, duration
    )
    ok_lag, said_lag = _coverage(lag, fraction * duration / profile["lag_interval_s"], limit, duration)
    return Criterion(
        "samples",
        "the run was sampled throughout",
        ok_resource and ok_lag,
        f"resource samples {said_resource}; lag samples {said_lag}",
        f"each kind at least {fraction:.0%} of its interval's worth over {duration:.0f} s, no gap over {limit} s",
    )


def check_faults(profile: dict, events: list[dict]) -> list[Criterion]:
    plan = soak_profile.build_plan(profile)
    started = {e["id"]: e for e in events if e.get("kind") == "fault_start"}
    ended = {e["id"]: e for e in events if e.get("kind") == "fault_end"}
    recovered = {e["id"]: e for e in events if e.get("kind") == "recovered"}
    problems: list[str] = []
    by_kind: dict[str, int] = {}
    for fault in plan:
        start, end, rec = started.get(fault.id), ended.get(fault.id), recovered.get(fault.id)
        if start is None:
            problems.append(f"#{fault.id} {fault.kind} was never injected")
            continue
        if start.get("fault") != fault.kind:
            problems.append(f"#{fault.id} was {start.get('fault')}, the plan says {fault.kind}")
            continue
        if end is None or not end.get("ok"):
            problems.append(f"#{fault.id} {fault.kind} did not land: {(end or {}).get('detail', 'no fault_end event')}")
            continue
        if rec is None or not rec.get("ok"):
            problems.append(
                f"#{fault.id} {fault.kind} did not recover: {(rec or {}).get('detail', 'no recovered event')}"
            )
            continue
        by_kind[fault.kind] = by_kind.get(fault.kind, 0) + 1
    kinds = ", ".join(f"{k} {n}" for k, n in sorted(by_kind.items())) or "none"
    done = sum(by_kind.values())
    executed = Criterion(
        "faults_executed",
        "every planned fault landed and the engine recovered",
        not problems,
        f"{done} of {len(plan)} planned faults ({kinds})",
        f"all {len(plan)}, one every {profile['fault_interval_s']} s",
        "; ".join(problems[:5]) + (f"; and {len(problems) - 5} more" if len(problems) > 5 else ""),
    )
    times = [r["recovery_s"] for r in recovered.values() if isinstance(r.get("recovery_s"), (int, float))]
    limit = profile["recovery_timeout_s"]
    worst = max(times, default=None)
    recovery = Criterion(
        "recovery_time",
        "recovery after each fault is within the timeout",
        worst is not None and worst <= limit,
        "no recovery recorded" if worst is None else f"slowest {worst:.0f} s, median {statistics.median(times):.0f} s",
        f"at most {limit} s",
    )
    return [executed, recovery]


def check_unplanned_exits(events: list[dict]) -> Criterion:
    bad = [e for e in events if e.get("kind") == "engine_exit" and not e.get("planned")]
    return Criterion(
        "no_unplanned_exit",
        "the engine never exits except when a fault stops it",
        not bad,
        f"{len(bad)} unplanned exit(s)",
        "0",
        "; ".join(f"at {e['elapsed_s']:.0f} s, status {e.get('exit_code')}" for e in bad[:5]),
    )


def check_commit_rule(samples: list[dict]) -> Criterion:
    """The core invariant, as the engine's counters show it: it never commits
    past what it verified (`vtop_commits_total` <= `vtop_verified_total`), and
    no verification ever failed. A scrape reads the two counters a moment
    apart, so a single sample with one more commit than verifications could be
    that moment; a breach counts when two samples in a row in the same engine
    lifetime show it, since a real one does not heal."""
    breaches: list[str] = []
    previous_bad: dict | None = None
    failures = []
    for sample in samples:
        metrics = sample.get("metrics")
        if not metrics:
            # A lag-only row, or a scrape that failed: it says nothing about
            # the counters, so it neither shows a breach nor ends a candidate.
            # (A restart ends it: the lifetime is compared below.)
            continue
        failures.append(metrics.get(M_VERIFY_FAILURES))
        if M_COMMITS not in metrics or M_VERIFIED not in metrics:
            previous_bad = None
            continue
        excess = metrics[M_COMMITS] - metrics[M_VERIFIED]
        if excess > 0:
            if previous_bad is not None and previous_bad.get("lifetime") == sample.get("lifetime"):
                breaches.append(f"{excess:.0f} commits beyond verifications at {sample['elapsed_s']:.0f} s")
            previous_bad = sample
        else:
            previous_bad = None
    failed = counter_increase(failures)
    seen = sum(1 for s in samples if (s.get("metrics") or {}).get(M_COMMITS) is not None)
    problems = breaches[:3]
    if failed:
        problems.append(f"{failed:.0f} verification failure(s)")
    if not seen:
        problems.append("no sample carried the engine's counters")
    return Criterion(
        "commit_rule",
        "commits never pass verifications, and no verification failed",
        not problems,
        f"{len(breaches)} breach(es), {failed:.0f} verification failure(s), over {seen} scraped samples",
        "0 breaches, 0 failures, counters scraped",
        "; ".join(problems),
    )


def check_trends(profile: dict, samples: list[dict], events: list[dict]) -> list[Criterion]:
    criteria = profile["criteria"]
    duration = profile["duration_s"]
    fraction = criteria["trend_window_fraction"]
    window = (duration * (1 - fraction), float(duration))
    out = []
    for name, meta in soak_profile.TREND_SERIES.items():
        limits = criteria["trend"][name]
        expected = (
            f"growth over the last {fraction:.0%} of the run at most {limits['max_growth_pct']}% "
            f"or {_fmt(limits['min_abs_growth'], meta['unit'])}"
        )
        title = f"{meta['label']} shows no growth trend"
        trend = theil_sen(series_points(profile, samples, events, name), window, criteria["trend_min_pairs"])
        if trend is None:
            out.append(
                Criterion(
                    f"trend_{name}",
                    title,
                    False,
                    "too few points to fit a trend",
                    expected,
                    "a trend needs samples from one engine process across the window; "
                    f"at least {criteria['trend_min_pairs']} pairs of buckets",
                )
            )
            continue
        grew = trend.growth > limits["min_abs_growth"] and trend.growth_pct > limits["max_growth_pct"]
        pct = "unbounded" if math.isinf(trend.growth_pct) else f"{trend.growth_pct:+.1f}%"
        out.append(
            Criterion(
                f"trend_{name}",
                title,
                not grew,
                f"{_fmt(trend.growth, meta['unit'])} over the window ({pct}) from a level of "
                f"{_fmt(trend.level, meta['unit'])}; {trend.n_points} samples, {trend.n_pairs} pairs",
                expected,
            )
        )
    return out


def check_lag(profile: dict, samples: list[dict], events: list[dict]) -> Criterion:
    values = [v for _, v, _ in series_points(profile, samples, events, "lag_seconds")]
    cap = profile["criteria"]["max_steady_lag_p95_s"]
    if not values:
        return Criterion(
            "lag_steady",
            "archive lag stays bounded between faults",
            False,
            "no lag sample outside a fault",
            f"p95 at most {cap} s",
        )
    p95 = percentile(values, 95)
    return Criterion(
        "lag_steady",
        "archive lag stays bounded between faults",
        p95 <= cap,
        f"p95 {p95:.1f} s, maximum {max(values):.1f} s over {len(values)} samples",
        f"p95 at most {cap} s",
    )


def check_rate(profile: dict, samples: list[dict], events: list[dict]) -> Criterion:
    """The rate the topic's end offset advanced at while the load ran: the
    drain and the final checks after the producer stops are not production."""
    stop = load_end(profile, events)
    points = sorted(
        (s["elapsed_s"], s["lag"]["end"])
        for s in samples
        if s.get("lag") and s["lag"].get("end") is not None and s["elapsed_s"] <= stop
    )
    target = profile["rate_records_per_s"]
    fraction = profile["criteria"]["min_rate_fraction"]
    expected = f"at least {fraction:.0%} of {target} records/s while the load ran"
    if len(points) < 2:
        return Criterion(
            "rate", "the stated rate was produced", False, "fewer than two topic end-offset samples", expected
        )
    produced = points[-1][1] - points[0][1]
    seconds = points[-1][0] - points[0][0]
    rate = produced / seconds if seconds > 0 else 0.0
    return Criterion(
        "rate",
        "the stated rate was produced",
        rate >= fraction * target,
        f"{rate:.0f} records/s ({produced} records in {seconds:.0f} s)",
        expected,
    )


def check_prune(profile: dict, events: list[dict]) -> Criterion:
    prunes = [e for e in events if e.get("kind") == "prune"]
    failed = [e for e in prunes if not e.get("ok")]
    wanted = profile["duration_s"] / profile["prune_interval_s"]
    minimum = profile["criteria"]["min_prune_fraction"] * wanted
    ok = len(prunes) - len(failed)
    deleted = sum(e.get("deleted") or 0 for e in prunes if e.get("ok"))
    return Criterion(
        "ledger_prune",
        "prune-ledger ran on its schedule and succeeded",
        not failed and ok >= minimum,
        f"{ok} succeeded, {len(failed)} failed, {deleted} ledger rows deleted",
        f"at least {minimum:.0f} runs, none failed",
        "; ".join(str(e.get("detail", ""))[:160] for e in failed[:3]),
    )


def check_audits(events: list[dict], finals: dict) -> Criterion:
    audits = [e for e in events if e.get("kind") == "audit"]
    failed = [e for e in audits if not e.get("ok")]
    final = finals.get("audit_coverage") or {}
    problems = [f"{e.get('trigger')}: {str(e.get('detail', ''))[:160]}" for e in failed[:3]]
    if not final:
        problems.append("the final audit-coverage --check-storage was not recorded")
    elif not final.get("ok"):
        problems.append(f"final audit-coverage --check-storage failed: {str(final.get('detail', ''))[:200]}")
    return Criterion(
        "zero_coverage_holes",
        "audit-coverage finds no hole, after every fault and at the end",
        not problems and bool(audits),
        f"{len(audits) - len(failed)} of {len(audits)} audits clean; final "
        + ("clean" if final.get("ok") else "failed" if final else "missing"),
        "every audit clean, at least one run, final audit-coverage --check-storage clean",
        "; ".join(problems) if audits else "no audit-coverage run was recorded",
    )


def check_scrub(profile: dict, events: list[dict], finals: dict) -> Criterion:
    scrubs = [e for e in events if e.get("kind") == "scrub"]
    failed = [e for e in scrubs if not e.get("ok")]
    final = finals.get("scrub") or {}
    minimum = profile["criteria"]["min_scrub_batches"]
    checked = final.get("batches_checked")
    problems = [f"{e.get('mode')}: {str(e.get('detail', ''))[:160]}" for e in failed[:3]]
    if not final:
        problems.append("the final scrub was not recorded")
    elif not final.get("ok"):
        problems.append(f"final scrub failed: {str(final.get('detail', ''))[:200]}")
    elif not isinstance(checked, int) or checked < minimum:
        problems.append(f"final scrub checked {checked} batches; at least {minimum} are expected")
    return Criterion(
        "scrub_clean",
        "scrub is clean: every sampled pass and the final full pass",
        not problems,
        f"{len(scrubs) - len(failed)} of {len(scrubs)} sampled passes clean; final scrub checked {checked} batches"
        + (", clean" if final.get("ok") else ""),
        f"all clean; the final pass reads at least {minimum} batches",
        "; ".join(problems),
    )


def check_tail(finals: dict) -> Criterion:
    tail = finals.get("tail") or {}
    partitions = tail.get("partitions") or {}
    problems = []
    for partition, row in sorted(partitions.items()):
        committed, end = row.get("committed"), row.get("end")
        if committed is None or end is None:
            problems.append(f"partition {partition}: committed {committed}, end {end} (not read)")
        elif committed != end:
            problems.append(f"partition {partition}: committed {committed} of {end}")
    if not partitions:
        problems.append("the final committed offsets were not recorded")
    return Criterion(
        "tail_drained",
        "after the load stopped, the group committed every record",
        not problems,
        f"{len(partitions)} partition(s) read"
        + (f"; {'; '.join(problems[:3])}" if problems else ", each committed to its end"),
        "committed offset equals the end offset on every partition",
    )


def check_shutdown(finals: dict) -> Criterion:
    stop = finals.get("engine_stop") or {}
    code = stop.get("exit_code")
    return Criterion(
        "clean_shutdown",
        "the drained engine stops cleanly on SIGTERM",
        code == 0,
        f"exit status {code}" if "exit_code" in stop else f"not recorded ({stop.get('error', 'no engine_stop')})",
        "exit status 0",
    )


def check_disk(profile: dict, samples: list[dict]) -> Criterion:
    floor = profile["criteria"]["min_free_gib_floor"] * 2**30
    free = [s["disk_free_bytes"] for s in samples if s.get("disk_free_bytes") is not None]
    expected = f"at least {profile['criteria']['min_free_gib_floor']} GiB free throughout"
    if not free:
        return Criterion("disk_headroom", "the host never ran short of disk", False, "no disk sample", expected)
    return Criterion(
        "disk_headroom",
        "the host never ran short of disk",
        min(free) >= floor,
        f"lowest {_fmt(min(free), 'bytes')} free",
        expected,
    )


def evaluate(profile: dict, samples: list[dict], events: list[dict], finals: dict) -> dict:
    """The verdict: every criterion, and whether all of them passed."""
    samples = sorted(samples, key=lambda s: s["elapsed_s"])
    events = sorted(events, key=lambda e: e["elapsed_s"])
    criteria: list[Criterion] = [
        check_run_completed(profile, samples, events),
        check_samples(profile, samples),
        *check_faults(profile, events),
        check_unplanned_exits(events),
        check_commit_rule(samples),
        *check_trends(profile, samples, events),
        check_lag(profile, samples, events),
        check_rate(profile, samples, events),
        check_prune(profile, events),
        check_audits(events, finals),
        check_scrub(profile, events, finals),
        check_tail(finals),
        check_shutdown(finals),
        check_disk(profile, samples),
    ]
    return {
        "evaluator_version": EVALUATOR_VERSION,
        "profile": profile["name"],
        "passed": all(c.passed for c in criteria),
        "criteria": [c.as_dict() for c in criteria],
    }


def render(verdict: dict) -> str:
    lines = [f"soak verdict ({verdict['profile']}): {'PASS' if verdict['passed'] else 'FAIL'}"]
    for c in verdict["criteria"]:
        lines.append(f"  {'PASS' if c['passed'] else 'FAIL'}  {c['title']}")
        lines.append(f"        observed: {c['observed']}")
        if not c["passed"]:
            lines.append(f"        expected: {c['expected']}")
            if c.get("detail"):
                lines.append(f"        detail:   {c['detail']}")
    return "\n".join(lines)


@dataclass
class RunFiles:
    samples: Path
    events: Path
    finals: Path
    loaded: dict = field(default_factory=dict)


def evaluate_run_dir(profile: dict, run_dir: Path) -> dict:
    files = RunFiles(run_dir / "samples.jsonl", run_dir / "events.jsonl", run_dir / "finals.json")
    samples = read_jsonl(files.samples)
    events = read_jsonl(files.events)
    try:
        finals = json.loads(files.finals.read_text()) if files.finals.exists() else {}
    except (OSError, json.JSONDecodeError) as error:
        raise InputError(f"cannot read {files.finals}: {error}") from None
    for index, row in enumerate(samples):
        if not isinstance(row.get("elapsed_s"), (int, float)):
            raise InputError(f"{files.samples} record {index + 1} has no numeric elapsed_s")
    for index, row in enumerate(events):
        if not isinstance(row.get("elapsed_s"), (int, float)) or not isinstance(row.get("kind"), str):
            raise InputError(f"{files.events} record {index + 1} lacks elapsed_s or kind")
    verdict = evaluate(profile, samples, events, finals)
    verdict["inputs"] = {
        "samples.jsonl": sha256_of(files.samples),
        "events.jsonl": sha256_of(files.events),
        "finals.json": sha256_of(files.finals),
    }
    return verdict


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Decide whether a soak run passed.")
    parser.add_argument("--profile", required=True, help="a profile name (72h, ci-smoke) or a path to one")
    parser.add_argument("--run-dir", required=True, type=Path, help="the run directory the driver wrote")
    parser.add_argument("--out", type=Path, help="where to write verdict.json (default: <run-dir>/verdict.json)")
    args = parser.parse_args(argv)
    try:
        profile = soak_profile.load(args.profile)
        verdict = evaluate_run_dir(profile, args.run_dir)
    except (soak_profile.ProfileError, InputError) as error:
        print(f"soak_eval: {error}", file=sys.stderr)
        return 2
    out = args.out or args.run_dir / "verdict.json"
    out.write_text(json.dumps(verdict, indent=2) + "\n")
    print(render(verdict))
    return 0 if verdict["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
