"""The arithmetic the comparison rests on, kept small enough to test exhaustively.

Two rules from #595 live here:

- rule 6 (open-loop latency): the offered rates for the latency runs are a
  fraction of KAFKA's measured saturation for that cell, and the same rates are
  offered to VTOP. `derive_offered_rates` is the only place they are computed.
- rule 7 (repetition): a cell is reported as the median of its repetitions with
  the min-max spread, never as its best run. `summarize` is that reduction.
"""
from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from typing import NamedTuple

LOAD_FRACTIONS = (25, 50, 75)


def percentile(values: Iterable[float], q: float) -> float:
    """The q-th percentile (0..100) by linear interpolation between closest ranks.

    The same definition as numpy's default (`linear`), written out so the
    harness needs no numpy. An empty input or a q outside 0..100 is an error,
    never a silent zero: a percentile of nothing is not a measurement.
    """
    data = sorted(float(v) for v in values)
    if not data:
        raise ValueError("percentile of an empty sample")
    if not 0.0 <= q <= 100.0 or math.isnan(q):
        raise ValueError(f"percentile q must be within 0..100, got {q}")
    if any(math.isnan(v) for v in data):
        raise ValueError("percentile of a sample containing NaN")
    if len(data) == 1:
        return data[0]
    rank = (len(data) - 1) * q / 100.0
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return data[low]
    return data[low] + (data[high] - data[low]) * (rank - low)


def median(values: Iterable[float]) -> float:
    return percentile(values, 50.0)


class Summary(NamedTuple):
    median: float
    low: float
    high: float
    n: int


def summarize(values: Sequence[float]) -> Summary:
    """Median and min-max across repetitions (#595 rule 7)."""
    if not values:
        raise ValueError("cannot summarize zero repetitions")
    return Summary(median(values), min(values), max(values), len(values))


def derive_offered_rates(kafka_saturation_records_per_sec: Sequence[float],
                         fractions: Sequence[int] = LOAD_FRACTIONS) -> dict[int, int]:
    """Offered rates (records/s) for the fixed-rate runs of one cell.

    Each is `fraction`% of the MEDIAN of Kafka's saturation repetitions for the
    cell, floored to a whole record per second (the perf tool takes an integer
    rate, and flooring never offers more than the stated fraction). Both
    systems are then offered exactly these rates.

    Refuses on no repetitions, on a non-positive or non-finite saturation, and
    on a fraction that rounds to a rate below 1 record/s: each of those would
    hand the perf tool a rate it reads as "unthrottled" (-1) or nonsense.
    """
    if not kafka_saturation_records_per_sec:
        raise ValueError("no Kafka saturation repetitions to derive offered rates from")
    for value in kafka_saturation_records_per_sec:
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"Kafka saturation must be a positive finite rate, got {value!r}")
    base = median(kafka_saturation_records_per_sec)
    rates = {}
    for fraction in fractions:
        if not 0 < fraction < 100:
            raise ValueError(f"load fraction must be within 1..99 percent, got {fraction}")
        rate = math.floor(base * fraction / 100.0)
        if rate < 1:
            raise ValueError(f"{fraction}% of {base} records/s is below 1 record/s")
        rates[fraction] = rate
    return rates


def records_for_duration(rate: int, seconds: float, minimum: int = 1000) -> int:
    """How many records a fixed-rate run sends to last about `seconds`."""
    if rate < 1 or seconds <= 0:
        raise ValueError(f"rate and duration must be positive, got rate={rate} seconds={seconds}")
    return max(minimum, int(rate * seconds))


def sustained(offered: float, achieved: float, tolerance: float = 0.05) -> bool:
    """Whether a fixed-rate run actually ran at its offered rate.

    The perf tool throttles a closed loop: when the cluster cannot keep up, the
    producer's buffer fills, send() blocks, and the achieved rate falls under
    the offered one. Latency from such a run is NOT the latency at that offered
    load (coordinated omission), so a run more than `tolerance` under its offer
    is flagged and the report says so instead of plotting it as if sustained.
    """
    if offered <= 0:
        raise ValueError(f"offered rate must be positive, got {offered}")
    return achieved >= offered * (1.0 - tolerance)
