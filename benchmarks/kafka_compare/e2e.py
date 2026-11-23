"""Metric 3 of the Kafka comparison (#596, #600): end-to-end latency, from the producer's CreateTime to the
moment the consumer's poll() returns the record.

The consumer (client/E2eLatency.java) counts each record's latency into a 1 ms histogram and prints it; the
harness turns that into sparse rows of `e2e_hist.csv`, one per (e2e row, bucket), and the e2e row's percentiles
come from the same histogram, so the table and the CDF cannot disagree. The buckets are:

- 0 to 59999 ms, one millisecond wide (a record's CreateTime has millisecond resolution, so that is the
  resolution of the data);
- 60000, meaning 60 s or more;
- -1, a negative latency: a clock or timestamp fault, and a run with any is refused, not charted.

The rules the report relies on are enforced here: the counts sum to the records consumed, and a run that
consumed a different number of records than it produced is `failed`, saying how many, so a gap cannot hide
inside a CDF.
"""
from __future__ import annotations

from collections import Counter
from typing import NamedTuple

from kafka_compare import stats

OVERFLOW_MS = 60_000
NEGATIVE = -1

# The percentiles an e2e row carries, as exact fractions (integer rank, no float rounding of 0.999 * n).
PERCENTILES = {"lat_p50_ms": (50, 100), "lat_p95_ms": (95, 100), "lat_p99_ms": (99, 100),
               "lat_p999_ms": (999, 1000)}


class E2eRefused(Exception):
    """The histogram cannot stand for the run; the message says why."""


def bucket(latency_ms: int) -> int:
    if latency_ms < 0:
        return NEGATIVE
    return min(latency_ms, OVERFLOW_MS)


def histogram(latencies_ms: list[int]) -> dict[int, int]:
    return dict(sorted(Counter(bucket(v) for v in latencies_ms).items()))


def parse_output(text: str) -> tuple[int, dict[int, int]]:
    """What E2eLatency prints: a `consumed N` line, then `bucket count` lines. Returns (N, histogram)."""
    consumed: int | None = None
    out: dict[int, int] = {}
    for line in text.splitlines():
        parts = line.split()
        if not parts:
            continue
        if parts[0] == "consumed":
            if len(parts) != 2 or not parts[1].isdigit() or consumed is not None:
                raise E2eRefused(f"consumer line {line[:60]!r} is not a single `consumed N`")
            consumed = int(parts[1])
            continue
        if len(parts) != 2:
            raise E2eRefused(f"histogram line {line[:60]!r} is not `bucket count`")
        try:
            lo, count = int(parts[0]), int(parts[1])
        except ValueError:
            raise E2eRefused(f"histogram line {line[:60]!r} holds a number that is not whole") from None
        if (lo != NEGATIVE and not 0 <= lo <= OVERFLOW_MS) or count < 1 or lo in out:
            raise E2eRefused(f"histogram line {line[:60]!r} is out of range or repeated")
        out[lo] = count
    if consumed is None:
        raise E2eRefused("the consumer printed no `consumed N` line")
    if sum(out.values()) != consumed:
        raise E2eRefused(f"the histogram's counts sum to {sum(out.values())}, but the consumer says it "
                         f"consumed {consumed}")
    return consumed, out


class Outcome(NamedTuple):
    status: str
    records: int
    records_produced: int
    sustained: bool
    note: str


def judge(hist: dict[int, int], produced: int, offered_rate: int, achieved_rate: float) -> Outcome:
    """The e2e row's status for one repetition."""
    if hist.get(NEGATIVE, 0):
        raise E2eRefused(f"{hist[NEGATIVE]} record(s) were consumed before their CreateTime: the clock or the "
                         "timestamps are wrong, and no latency from this run can stand")
    consumed = sum(hist.values())
    sustained = stats.sustained(offered_rate, achieved_rate)
    if consumed < produced:
        return Outcome("failed", consumed, produced, sustained, f"missing {produced - consumed} records")
    if consumed > produced:
        extra = consumed - produced
        return Outcome("failed", consumed, produced, sustained,
                       f"consumed {extra} more record{'' if extra == 1 else 's'} than were produced")
    return Outcome("ok", consumed, produced, sustained, "" if sustained else "offered rate not sustained")


def percentile_ms(hist: dict[int, int], num: int, den: int) -> float:
    """The num/den quantile of a histogram, as the lower edge of the bucket holding that rank."""
    total = sum(hist.values())
    if not total:
        raise E2eRefused("an empty histogram has no percentiles")
    rank = -(-total * num // den)  # ceil, exact
    seen = 0
    for lo, count in sorted(hist.items()):
        seen += count
        if seen >= rank:
            return float(lo)
    return float(max(hist))


def common_rate(kafka_saturation: list[float], vtop_saturation: list[float], load_pct: int) -> int:
    """The offered rate of the `common` load basis: `load_pct` of the SLOWER system's median saturation for
    the cell, floored to a whole record per second. Both systems, and every profile, are offered it, so
    their latencies are read at one load."""
    if not kafka_saturation:
        raise E2eRefused("no Kafka saturation repetitions to derive the common rate from")
    if not vtop_saturation:
        raise E2eRefused("no VTOP saturation repetitions to derive the common rate from")
    slower = min(stats.median(kafka_saturation), stats.median(vtop_saturation))
    try:
        return stats.derive_offered_rates([slower], (load_pct,))[load_pct]
    except ValueError as exc:
        raise E2eRefused(str(exc)) from None
