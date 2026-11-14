"""Metric 3 of the Kafka comparison (#596): end-to-end latency, from the
producer's CreateTime to the consumer's poll() return.

The consumer (client/E2eLatency.java) counts each record's latency into a
1 ms histogram, and the harness turns that into sparse `e2e_hist.csv` rows.
The buckets are:
- 0 to 59999 ms, one millisecond wide;
- 60000, meaning 60 s or more;
- -1, a negative latency. That is a clock or timestamp fault, and a run with
  any is refused.

The rules the report relies on are enforced here: the counts sum to the
records consumed, and a run that consumed fewer than it produced is `failed`
with the missing count, so a gap cannot hide inside a CDF.
"""
from __future__ import annotations

from collections import Counter
from typing import NamedTuple

from kafka_compare import stats

OVERFLOW_MS = 60_000
NEGATIVE = -1

COLUMNS = ("run_id", "system", "record_size", "partitions", "producers", "compression", "profile", "repetition",
           "bucket_lo_ms", "count")


class E2eRefused(Exception):
    """The histogram cannot stand for the run; the message says why."""


def bucket(latency_ms: int) -> int:
    if latency_ms < 0:
        return NEGATIVE
    return min(latency_ms, OVERFLOW_MS)


def histogram(latencies_ms: list[int]) -> dict[int, int]:
    return dict(sorted(Counter(bucket(v) for v in latencies_ms).items()))


def parse_histogram(text: str) -> dict[int, int]:
    """The consumer's `bucket count` lines."""
    out: dict[int, int] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) != 2:
            raise E2eRefused(f"histogram line {line[:60]!r} is not `bucket count`")
        try:
            lo, count = int(parts[0]), int(parts[1])
        except ValueError:
            raise E2eRefused(f"histogram line {line[:60]!r} holds a number that is not whole") from None
        if lo != NEGATIVE and not 0 <= lo <= OVERFLOW_MS or count < 0 or lo in out:
            raise E2eRefused(f"histogram line {line[:60]!r} is out of range or repeated")
        out[lo] = count
    return out


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
        return Outcome("failed", consumed, produced, sustained,
                       f"consumed {consumed - produced} more records than were produced")
    return Outcome("ok", consumed, produced, sustained, "" if sustained else "offered rate not sustained")


def percentile_ms(hist: dict[int, int], q: float) -> float:
    """The q-quantile (0 < q <= 1) of a histogram, as the bucket's lower edge."""
    total = sum(hist.values())
    if not total:
        raise E2eRefused("an empty histogram has no percentiles")
    rank = q * total
    seen = 0
    for lo, count in sorted(hist.items()):
        seen += count
        if seen >= rank:
            return float(lo)
    return float(max(hist))
