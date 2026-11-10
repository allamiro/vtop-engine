"""The #481 rate criterion, read from a long-lived run's own engine_metrics.csv.

The criterion: with `upload.max_egress_bytes_per_second` = R, every one-second
bucket of the shaper's own counter (`vtop_upload_egress_bytes_total`) is at or
below R, and the total over the window is within 10% of R times its length. It
is judged on the ENGINE's counter, never on host-wide network bytes: those carry
framing, retransmissions and every other process on the machine, and the cap is
a promise about the bodies VTOP hands its HTTP client.

Every number here comes from rows the runner already wrote (lib/engine_run.py);
nothing is re-measured. A bucket is one scheduled scrape interval — the row's
`interval_seconds`, recovered at the runner's precision from `delta /
delta_per_second` where both exist — and a run scraped more often than once a
second is refused rather than judged per sub-second interval. The opening span (launch to first answer) is left out,
as the runner leaves it out of `engine_<name>_max_per_second`: its length is
whatever the start took, not a one-second bucket.

The engine creates the counter on the shaper's first admission, so samples
taken before then carry no row for it. Those buckets are counted as ZERO bytes
over their interval rather than skipped: a skipped bucket would shorten the
denominator and flatter the total, while it really is a second in which the
capped engine sent nothing.
"""
from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass

COUNTER = "vtop_upload_egress_bytes_total"

# The shaper's design (crates/vtop-upload/src/transport.rs): capacity R/32,
# refill R - R/32, so any one-second window admits at most R and a saturated
# shaper sustains 31/32 of it. The prediction a measured ratio is read against.
PREDICTED_SUSTAINED_FRACTION = 31 / 32
TOTAL_TOLERANCE = 0.10


@dataclass(frozen=True)
class Bucket:
    """One scheduled scrape interval and what the shaper admitted inside it."""

    index: int
    elapsed_seconds: float
    interval_seconds: float
    # The scrape latencies at the bucket's two edges. A counter is read
    # somewhere inside its scrape, so the span the two reads cover lies in
    # [interval - latency, interval + previous_latency]; the bounds below use it.
    previous_latency_seconds: float
    latency_seconds: float
    bytes: float

    @property
    def rate(self) -> float:
        """The bucket's rate as the runner's own `delta_per_second` reads it."""
        return self.bytes / self.interval_seconds

    @property
    def rate_upper_bound(self) -> float:
        """The highest rate the bucket can have had, given its edges' latencies."""
        span = self.interval_seconds - self.latency_seconds
        return self.bytes / span if span > 0 else float("inf")


# The criterion is about one-second windows. A bucket shorter than that is not
# one: the shaper legitimately admits up to R/32 in a burst, so a 0.1 s bucket
# can read above R while every one-second window holding it stays below R.
# Judging such buckets would fail compliant runs, so they are refused instead.
# The floor sits a little under 1 s because a scheduled one-second scrape
# lands a few milliseconds either side of its deadline.
MIN_BUCKET_SECONDS = 0.95

# The columns read here. A file missing one is not an engine_metrics.csv.
REQUIRED_COLUMNS = ("elapsed_seconds", "interval_seconds", "scrape_latency_seconds",
                    "metric", "type", "delta", "delta_per_second")


def _float(value: str, what: str, row: dict) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"engine_metrics.csv row has no numeric {what} ({value!r}): {row}") \
            from None


def _precise_interval(rounded: float, delta: str, per_second: str, row: dict) -> float:
    """The bucket's interval at the runner's own precision.

    The CSV rounds `interval_seconds` to milliseconds, but the runner computed
    `delta_per_second` from the unrounded interval (lib/engine_run.py), so
    delta / delta_per_second recovers that interval to the precision the rate
    was recorded at. A bucket near the cap must be judged on the rate the run
    recorded, not on one recomputed from a rounded denominator.
    """
    if delta in ("", None) or per_second in ("", None):
        return rounded
    d = _float(delta, "delta", row)
    rate = _float(per_second, "delta_per_second", row)
    if d > 0 and rate > 0:
        return d / rate
    return rounded


def read_buckets(path: str, counter: str = COUNTER) -> list[Bucket]:
    """Every scheduled bucket in a run's engine_metrics.csv, in order.

    Samples are grouped by `elapsed_seconds`, which every row of one scrape
    shares. The first sample is the opening span and is not a bucket.
    """
    samples: dict[str, dict] = {}
    order: list[str] = []
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            missing = [c for c in REQUIRED_COLUMNS if c not in (reader.fieldnames or ())]
            if missing:
                raise ValueError(f"{path} is not an engine_metrics.csv: no {', '.join(missing)} "
                                 "column")
            for row in reader:
                if None in row or any(row[c] is None for c in REQUIRED_COLUMNS):
                    raise ValueError(f"{path} has a truncated row: {row}")
                key = row["elapsed_seconds"]
                sample = samples.get(key)
                if sample is None:
                    sample = {"row": row, "bytes": 0.0, "counter_seen": False,
                              "interval": None}
                    samples[key] = sample
                    order.append(key)
                if row["metric"] == counter:
                    if row["type"] != "counter":
                        raise ValueError(f"{counter} is typed {row['type']!r}, not counter: "
                                         f"{row}")
                    if row["delta"] != "":
                        sample["bytes"] += _float(row["delta"], "delta", row)
                    sample["interval"] = _precise_interval(
                        _float(row["interval_seconds"], "interval_seconds", row),
                        row["delta"], row["delta_per_second"], row)
                    sample["counter_seen"] = True
    except (csv.Error, UnicodeDecodeError) as exc:
        raise ValueError(f"{path} cannot be read as CSV: {exc}") from None
    if not order:
        raise ValueError(f"{path} holds no samples")
    if not any(samples[key]["counter_seen"] for key in order):
        raise ValueError(
            f"{path} never carries {counter}: the engine exports it only when a cap is set and "
            "the shaper has admitted a byte, so this run was uncapped or never uploaded")
    buckets: list[Bucket] = []
    previous_latency = None
    for position, key in enumerate(order):
        row = samples[key]["row"]
        latency = _float(row["scrape_latency_seconds"], "scrape_latency_seconds", row)
        if position == 0:
            previous_latency = latency
            continue
        interval = samples[key]["interval"]
        if interval is None:
            interval = _float(row["interval_seconds"], "interval_seconds", row)
        if interval <= 0:
            raise ValueError(f"{path} has a non-positive interval at elapsed "
                             f"{row['elapsed_seconds']}: {row}")
        buckets.append(Bucket(
            index=len(buckets) + 1,
            elapsed_seconds=_float(row["elapsed_seconds"], "elapsed_seconds", row),
            interval_seconds=interval,
            previous_latency_seconds=previous_latency,
            latency_seconds=latency,
            bytes=samples[key]["bytes"],
        ))
        previous_latency = latency
    return buckets


def evaluate(buckets: list[Bucket], cap: float) -> dict:
    """The verdict on both halves of the criterion, with the numbers behind it."""
    if cap <= 0:
        raise ValueError(f"a cap of {cap} cannot be evaluated")
    if not buckets:
        raise ValueError("no scheduled buckets to evaluate")
    short = [b.index for b in buckets if b.interval_seconds < MIN_BUCKET_SECONDS]
    if short:
        raise ValueError(
            f"{len(short)} bucket(s) are shorter than {MIN_BUCKET_SECONDS} s (first: bucket "
            f"{short[0]}); the criterion is about one-second windows and the shaper may burst "
            "R/32 inside a shorter one, so a sub-second scrape interval cannot be judged here - "
            "rerun with engine_scrape_interval_seconds of at least 1")
    worst = max(buckets, key=lambda b: b.rate)
    largest = max(buckets, key=lambda b: b.bytes)
    over = [b.index for b in buckets if b.rate > cap]
    bound_over = [b.index for b in buckets if b.rate_upper_bound > cap]
    total_bytes = sum(b.bytes for b in buckets)
    total_seconds = sum(b.interval_seconds for b in buckets)
    ratio = total_bytes / (cap * total_seconds)
    every_bucket = not over
    total_within = abs(ratio - 1.0) <= TOTAL_TOLERANCE
    return {
        "counter": COUNTER,
        "cap_bytes_per_second": cap,
        "buckets": len(buckets),
        "window_seconds": round(total_seconds, 3),
        "total_bytes": total_bytes,
        "ratio_to_cap": round(ratio, 6),
        "predicted_ratio": round(PREDICTED_SUSTAINED_FRACTION, 6),
        "total_tolerance": TOTAL_TOLERANCE,
        "worst_bucket": {
            "index": worst.index,
            "elapsed_seconds": worst.elapsed_seconds,
            "interval_seconds": worst.interval_seconds,
            "bytes": worst.bytes,
            "rate": round(worst.rate, 3),
            "ratio_to_cap": round(worst.rate / cap, 6),
            "rate_upper_bound": round(worst.rate_upper_bound, 3),
        },
        "largest_bucket_bytes": {
            "index": largest.index,
            "bytes": largest.bytes,
            "interval_seconds": largest.interval_seconds,
            "ratio_to_cap": round(largest.bytes / cap, 6),
        },
        "buckets_over_cap": over,
        # Buckets whose rate COULD have exceeded the cap once the scrape latency
        # at their closing edge is charged against them. Reported, not judged:
        # the criterion is on the runner's per-second column, and this says how
        # much the bucket edges' uncertainty leaves room for.
        "buckets_whose_latency_bound_exceeds_cap": bound_over,
        "zero_byte_buckets": sum(1 for b in buckets if b.bytes == 0),
        "max_scrape_latency_seconds": max(b.latency_seconds for b in buckets),
        "pass_every_bucket_at_or_below_cap": every_bucket,
        "pass_total_within_tolerance": total_within,
        "pass": every_bucket and total_within,
    }


BUCKET_COLUMNS = ("index", "elapsed_seconds", "interval_seconds", "previous_latency_seconds",
                  "latency_seconds", "bytes", "rate", "ratio_to_cap", "rate_upper_bound")


def write_buckets(buckets: list[Bucket], cap: float, path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        out = csv.writer(fh)
        out.writerow(BUCKET_COLUMNS)
        for b in buckets:
            fields = asdict(b)
            out.writerow([fields["index"], fields["elapsed_seconds"], fields["interval_seconds"],
                          fields["previous_latency_seconds"], fields["latency_seconds"],
                          repr(b.bytes), round(b.rate, 3), round(b.rate / cap, 6),
                          round(b.rate_upper_bound, 3)])


def write_verdict(verdict: dict, path: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(verdict, fh, indent=2, sort_keys=True)
        fh.write("\n")
