"""Parsing what Kafka's own perf tools print, and nothing else.

The one client both systems are measured with (#595 rule 2) is the JVM
`kafka-producer-perf-test.sh` / `kafka-consumer-perf-test.sh` shipped in the
pinned `apache/kafka` image. Its numbers are taken from its own summary lines,
verbatim; nothing is recomputed from a partial log.

A summary that does not parse, or that reports zero records, NaN latency or a
missing steady-state line when warm-up was asked for, is a FAILED run: the
caller writes a refusal row, never a guessed one.
"""
from __future__ import annotations

import math
import re
from typing import NamedTuple

_NUM = r"([0-9]+(?:\.[0-9]+)?|NaN)"

# e.g. "1800000 steady state records sent, 148087.206911 records/sec (14.12 MB/sec),
#       1581.93 ms avg latency, 4301.00 ms max latency, 1173 ms 50th, 3773 ms 95th,
#       4189 ms 99th, 4290 ms 99.9th."
_PRODUCER_SUMMARY = re.compile(
    r"^(?P<records>[0-9]+) (?P<steady>steady state )?records sent, "
    rf"(?P<rps>{_NUM}) records/sec \((?P<mbps>{_NUM}) MB/sec\), "
    rf"(?P<avg>{_NUM}) ms avg latency, (?P<max>{_NUM}) ms max latency, "
    rf"(?P<p50>{_NUM}) ms 50th, (?P<p95>{_NUM}) ms 95th, "
    rf"(?P<p99>{_NUM}) ms 99th, (?P<p999>{_NUM}) ms 99\.9th\.\s*$"
)


class ProducerResult(NamedTuple):
    records: int
    records_per_sec: float
    mib_per_sec: float  # the tool's "MB" is 1024*1024 bytes
    lat_avg_ms: float
    lat_max_ms: float
    lat_p50_ms: float
    lat_p95_ms: float
    lat_p99_ms: float
    lat_p999_ms: float
    steady_state: bool


class ConsumerResult(NamedTuple):
    records: int
    mib: float
    records_per_sec: float  # fetch.nMsg.sec: excludes the group join
    mib_per_sec: float      # fetch.MB.sec
    rebalance_ms: float
    fetch_ms: float


class PerfParseError(ValueError):
    """The tool's output does not hold a usable result; the message says why."""


def _finite(name: str, text: str) -> float:
    value = float(text)
    if math.isnan(value) or math.isinf(value):
        raise PerfParseError(f"{name} is {text}: the run produced no usable latency")
    return value


def parse_producer(output: str, expect_steady_state: bool) -> ProducerResult:
    """The result of one producer perf run.

    With warm-up (`--warmup-records`) the tool prints the whole-run summary and
    then a `steady state` one that excludes the warm-up; only the latter is
    taken, and its absence is an error rather than a silent fallback to the
    whole-run line (#595 rule 7: warm-up excluded).
    """
    whole: re.Match | None = None
    steady: re.Match | None = None
    for line in output.splitlines():
        match = _PRODUCER_SUMMARY.match(line.strip())
        if not match:
            continue
        if match.group("steady"):
            steady = match
        else:
            whole = match
    chosen = steady if expect_steady_state else whole
    if chosen is None:
        kind = "steady-state summary" if expect_steady_state else "summary"
        raise PerfParseError(f"no {kind} line in the producer perf tool's output")
    records = int(chosen.group("records"))
    if records <= 0:
        raise PerfParseError("the producer perf tool reports 0 records sent")
    return ProducerResult(
        records=records,
        records_per_sec=_finite("records/sec", chosen.group("rps")),
        mib_per_sec=_finite("MB/sec", chosen.group("mbps")),
        lat_avg_ms=_finite("avg latency", chosen.group("avg")),
        lat_max_ms=_finite("max latency", chosen.group("max")),
        lat_p50_ms=_finite("p50", chosen.group("p50")),
        lat_p95_ms=_finite("p95", chosen.group("p95")),
        lat_p99_ms=_finite("p99", chosen.group("p99")),
        lat_p999_ms=_finite("p99.9", chosen.group("p999")),
        steady_state=bool(chosen.group("steady")),
    )


_CONSUMER_HEADER = (
    "start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, "
    "rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec"
)


def parse_consumer(output: str, expected_records: int) -> ConsumerResult:
    """The result of one consumer perf run, from the line under its header.

    The rates are the tool's `fetch.*` columns, which exclude the group join
    (Kafka's group.initial.rebalance.delay.ms alone is 3 s by default, and VTOP
    has no such delay, so the whole-run columns would compare join policies,
    not reads). A run that consumed fewer records than asked timed out on an
    empty fetch and is refused.
    """
    lines = [line.strip() for line in output.splitlines()]
    try:
        index = lines.index(_CONSUMER_HEADER)
    except ValueError as exc:
        raise PerfParseError("no summary header in the consumer perf tool's output") from exc
    if index + 1 >= len(lines):
        raise PerfParseError("the consumer perf tool printed a header and no result")
    fields = [field.strip() for field in lines[index + 1].split(",")]
    if len(fields) != 10:
        raise PerfParseError(f"consumer summary has {len(fields)} fields, expected 10: {lines[index + 1]!r}")
    try:
        mib = float(fields[2])
        records = int(fields[4])
        rebalance_ms = float(fields[6])
        fetch_ms = float(fields[7])
        fetch_mibps = float(fields[8])
        fetch_rps = float(fields[9])
    except ValueError as exc:
        raise PerfParseError(f"consumer summary does not parse: {lines[index + 1]!r}") from exc
    if records < expected_records:
        raise PerfParseError(f"consumed {records} records, asked for {expected_records}")
    numeric = {"data.consumed.in.MB": mib, "rebalance.time.ms": rebalance_ms, "fetch.time.ms": fetch_ms,
               "fetch.MB.sec": fetch_mibps, "fetch.nMsg.sec": fetch_rps}
    for name, value in numeric.items():
        if not math.isfinite(value) or value < 0:
            raise PerfParseError(f"consumer {name} is {value}: not a finite, non-negative number")
    if fetch_ms <= 0 or fetch_rps <= 0:
        raise PerfParseError(f"consumer fetch time {fetch_ms} ms at {fetch_rps} records/s gives no rate")
    return ConsumerResult(records, mib, fetch_rps, fetch_mibps, rebalance_ms, fetch_ms)


_CONFIG_LINE = re.compile(r"^\s+([a-z0-9._-]+) = (.*)$")
# The pinned image's log4j2 layout is `%m (%c)%n`, so the multi-line config
# message ends with the logger that wrote it on a line of its own. That line,
# and only that line, closes a complete dump.
_CONFIG_END = re.compile(r"^\s*\(org\.apache\.kafka\.common\.config\.AbstractConfig\)\s*$")


def parse_client_config(output: str, banner: str = "ProducerConfig values:") -> dict[str, str]:
    """The client's resolved configuration, from the INFO dump it logs at start.

    Recorded in environment.json (#595 rule 8) so the producer settings both
    systems ran with are the client's own word, not the harness's assumption.
    A dump cut off before its closing logger line, or broken by a line that is
    neither a setting nor that close, is an error, never a partial config.
    """
    values: dict[str, str] | None = None
    inside = False
    for line in output.splitlines():
        if banner in line:
            inside, values = True, {}
            continue
        if not inside:
            continue
        match = _CONFIG_LINE.match(line)
        if match:
            values[match.group(1)] = match.group(2).strip()
        elif _CONFIG_END.match(line) and values:
            return values
        else:
            raise PerfParseError(f"'{banner}' block is malformed at {line.strip()[:80]!r}")
    if values is None:
        raise PerfParseError(f"no '{banner}' block in the client's output")
    raise PerfParseError(f"'{banner}' block is truncated: no closing logger line after {len(values)} settings")
