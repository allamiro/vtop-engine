"""The failover benchmark's parsing and arithmetic (#597), kept free of Docker so it can be tested.

Every number in the failover CSV comes from a function here, applied to what a
run left on disk: the producer's acknowledgement timeline, the leader
observer's log, the verification consumer's output.

Definitions (the report repeats them next to the numbers):

- **Unavailability window**: the longest gap between two consecutive successful
  acknowledgements that ends after the fault and starts before the end of the
  post-fault window. For a crash this is the issue's "last acknowledgement
  before the fault to the first after it"; it also catches an outage that
  begins a little AFTER the fault signal, which is what an orderly stop does
  (the leader moves while the process shuts down).
- **Time to new leader**: from the fault to the first observation, by the
  system's own metadata (Kafka: any broker's Metadata answer; VTOP: the
  range lease), of a leader other than the one faulted.
- **Latency**: acknowledgement time minus the record's INTENDED send time on
  the fixed-rate schedule (origin + sequence / rate). Measured against the
  schedule, not the actual send, so the records an outage held back are
  charged for the wait (no coordinated omission).
- **Acknowledged records lost**: acknowledged sequences missing from, or not
  byte-exact in, a full read of the partition afterwards.
"""
from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterable, Sequence
from typing import NamedTuple

from kafka_compare import stats

FAULTS = ("kill9_leader", "stop_leader", "partition_leader")
FAULT_LABELS = {
    "kill9_leader": "kill -9 of the leader",
    "stop_leader": "orderly stop of the leader",
    "partition_leader": "network partition of the leader",
}


class ParseError(ValueError):
    """A run's raw output is not what the parser expects; the message says where."""


class AckEvent(NamedTuple):
    """`records` consecutive sequences starting at `first_seq`, acknowledged at `t_ms` (unix ms)."""

    t_ms: float
    first_seq: int
    records: int


# ------------------------------------------------------------------------ Kafka side


class VerifiableProducerRun(NamedTuple):
    startup_ms: float
    acks: list[AckEvent]
    errors: list[tuple[float, str, str]]  # (t_ms, exception, message)
    tool_data: dict
    non_json_lines: int


def parse_verifiable_producer(lines: Iterable[str]) -> VerifiableProducerRun:
    """The JSON lines `kafka-verifiable-producer.sh` prints, one per send outcome.

    Values are the record's sequence number as text ("0", "1", ...), which the
    loss check relies on. Log lines interleaved on stdout are counted, not parsed.
    """
    startup = None
    acks: list[AckEvent] = []
    errors: list[tuple[float, str, str]] = []
    tool_data: dict = {}
    other = 0
    for line in lines:
        line = line.strip()
        if not line.startswith("{"):
            if line:
                other += 1
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            other += 1
            continue
        name = event.get("name")
        if name == "startup_complete":
            startup = float(event["timestamp"])
        elif name == "producer_send_success":
            value = event.get("value")
            if not isinstance(value, str) or not value.isdigit():
                raise ParseError(f"producer_send_success with a value that is not a sequence: {value!r}")
            acks.append(AckEvent(float(event["timestamp"]), int(value), 1))
        elif name == "producer_send_error":
            errors.append((float(event["timestamp"]), str(event.get("exception", "")),
                           str(event.get("message", ""))))
        elif name == "tool_data":
            tool_data = event
    if startup is None:
        raise ParseError("no startup_complete line: the producer never started")
    acks.sort()
    return VerifiableProducerRun(startup, acks, errors, tool_data, other)


_ERROR_CODE = re.compile(r"Error: ([A-Z_]+)")


def kafka_client_warnings(stderr: str) -> dict[str, int]:
    """What the JVM producer logged (WARN) about trouble.

    `retries`: produce requests that failed and were put back to be sent again -
    the Sender's "retrying" lines (an error answer) and its "invalid metadata
    error in produce request" lines (the connection to the leader dropped; the
    batch waits for a metadata refresh). `connect_failures`: attempts to reach a
    broker that failed. `code:*`: the error codes the retried answers carried.
    """
    retries = 0
    codes: Counter[str] = Counter()
    connect_failures = 0
    for line in stderr.splitlines():
        if "retrying (" in line:
            retries += 1
            match = _ERROR_CODE.search(line)
            if match:
                codes[match.group(1)] += 1
        elif "invalid metadata error in produce request" in line:
            retries += 1
        elif "could not be established" in line or "Error connecting to node" in line:
            connect_failures += 1
    out = {"retries": retries, "connect_failures": connect_failures}
    out.update({f"code:{code}": count for code, count in sorted(codes.items())})
    return out


_CONSUMER_LINE = re.compile(r"^Offset:(\d+)\t(.*)$")


def parse_console_consumer(text: str) -> list[tuple[int, str]]:
    """(offset, value) per record from `kafka-console-consumer.sh --property print.offset=true`."""
    records = []
    for line in text.splitlines():
        match = _CONSUMER_LINE.match(line)
        if match:
            records.append((int(match.group(1)), match.group(2)))
        elif line.startswith("Offset:"):
            raise ParseError(f"consumer line not in the expected shape: {line[:80]!r}")
    return records


class LossCheck(NamedTuple):
    acked: int
    acked_lost: int
    duplicates: int
    unacked_present: int
    unexpected: int
    first_lost: list[int]


def kafka_loss_check(acked_sequences: Iterable[int], consumed_values: Iterable[str]) -> LossCheck:
    """Every acknowledged sequence must be read back exactly once, byte-exact.

    A value is byte-exact when it is exactly the decimal text the producer sent
    (so "007" or "7 " is not record 7). Duplicates are extra copies of any value;
    `unacked_present` are records written but never acknowledged (allowed:
    the client gave up on them, the broker had them); `unexpected` are values
    the producer never sent.
    """
    acked = set(acked_sequences)
    seen: Counter[int] = Counter()
    unexpected = 0
    for value in consumed_values:
        if value.isdigit() and str(int(value)) == value:
            seen[int(value)] += 1
        else:
            unexpected += 1
    lost = sorted(acked - set(seen))
    duplicates = sum(count - 1 for count in seen.values() if count > 1)
    unacked_present = len(set(seen) - acked)
    return LossCheck(len(acked), len(lost), duplicates, unacked_present, unexpected, lost[:10])


def kafka_time_to_leader(probe_lines: Iterable[str], fault_ms: float, old_leader: int,
                         tolerance_ms: float = 0.0) -> tuple[float, int] | None:
    """(ms from the fault, new leader id) at the EARLIEST Metadata answer, from any broker,
    naming a leader other than `old_leader`, or None if none did.

    Observations up to `tolerance_ms` before the fault count (the probe's clock and
    the fault's are aligned only that well); an answer naming -1 (no leader) is not
    a new leader. The earliest by timestamp, not the first line: the probe's
    per-broker threads append in turn, so file order is not guaranteed to be time
    order.
    """
    best = None
    for line in probe_lines:
        if not line.strip():
            continue
        event = json.loads(line)
        t_ms = event["t_us"] / 1000.0
        if t_ms < fault_ms - tolerance_ms or not event.get("ok"):
            continue
        leader = event.get("leader")
        if leader is not None and leader >= 0 and leader != old_leader and (best is None or t_ms < best[0]):
            best = (t_ms, leader)
    return None if best is None else (best[0] - fault_ms, best[1])


# ------------------------------------------------------------------------- VTOP side


def parse_ack_log(text: str) -> list[AckEvent]:
    """`vtop-node produce --ack-log` lines (`<unix_micros> <acked_total> <latency_micros>`),
    possibly from several invocations appending in turn, as AckEvents.

    Each batch covers the sequences between the previous line's total and its own:
    a restarted producer resumes at the acknowledged floor, which is the last total.
    A total that goes backwards is refused - it would mean two producers or a lost floor.
    """
    events = []
    previous = 0
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) != 3 or not all(part.isdigit() for part in parts):
            raise ParseError(f"ack log line {number} is not '<micros> <total> <latency>': {line[:80]!r}")
        t_us, total, _latency = (int(part) for part in parts)
        if total <= previous:
            raise ParseError(f"ack log line {number}: acknowledged total {total} after {previous}")
        events.append(AckEvent(t_us / 1000.0, previous, total - previous))
        previous = total
    return events


def vtop_time_to_leader(lease_lines: Iterable[str], fault_ms: float, old_holder: str,
                        tolerance_ms: float = 0.0) -> tuple[float, str, int] | None:
    """(ms from the fault, new holder, its epoch) at the first lease observation naming a
    holder other than `old_holder`, or None."""
    for line in lease_lines:
        parts = line.split()
        if len(parts) != 3:
            continue  # "<t> unreadable"
        t_ms = int(parts[0]) / 1000.0
        if t_ms < fault_ms - tolerance_ms:
            continue
        if parts[1] != old_holder:
            return t_ms - fault_ms, parts[1], int(parts[2])
    return None


def lease_holder_at(lease_lines: Iterable[str], t_ms: float) -> tuple[str, int] | None:
    """The holder and epoch the lease observer last saw at or before `t_ms`."""
    current = None
    for line in lease_lines:
        parts = line.split()
        if len(parts) != 3:
            continue
        if int(parts[0]) / 1000.0 > t_ms:
            break
        current = (parts[1], int(parts[2]))
    return current


def driver_summary(driver_log: str) -> dict[str, object]:
    """Produce attempts the VTOP driver made, how many ended early, and why (classed)."""
    attempts = 0
    failed = 0
    reasons: Counter[str] = Counter()
    targets = []
    for line in driver_log.splitlines():
        if " produce holder=" in line:
            attempts += 1
            match = re.search(r"holder=(\S+) epoch=(\d+)", line)
            if match:
                target = (match.group(1), int(match.group(2)))
                if not targets or targets[-1] != target:
                    targets.append(target)
        match = re.search(r" exit=(\d+) .*reason=(.*)$", line)
        if match and match.group(1) != "0":
            failed += 1
            reasons[classify_vtop_reason(match.group(2))] += 1
    return {"attempts": attempts, "failed_attempts": failed, "reasons": dict(sorted(reasons.items())),
            "leaders_used": len(targets)}


def classify_vtop_reason(reason: str) -> str:
    text = reason.lower()
    for needle, label in (("no answer to a", "request timeout"), ("failed to lookup", "name not resolvable"),
                          ("tls handshake eof", "node not serving (handshake closed)"),
                          ("quorum not reached", "quorum not reached (Overloaded)"),
                          ("connection refused", "connection refused"), ("fenced", "fenced epoch"),
                          ("not the leader", "not leader"), ("notleader", "not leader"),
                          ("interrupted", "connection lost"), ("reset", "connection lost"),
                          ("rejected", "produce rejected")):
        if needle in text:
            return label
    return "other: " + reason.strip()[:60] if reason.strip() else "other"


_VERIFY_DONE = re.compile(r"verify_done records=(\d+) next_offset=(\d+) gaps=(\d+) high_watermark=(\d+)")


class VerifyResult(NamedTuple):
    passed: bool
    records: int | None
    gaps: int | None
    high_watermark: int | None
    error: str


def parse_vtop_verify(stdout: str, stderr: str, exit_code: int) -> VerifyResult:
    """`vtop-node verify`: byte-exact, in-order, every visible record checked against the
    producer's sequence; passing with records >= the acknowledged floor means no
    acknowledged record was lost and none is duplicated (a duplicate shifts every later
    record off its position and fails the content check)."""
    match = _VERIFY_DONE.search(stdout)
    records = gaps = hwm = None
    if match:
        records, _next, gaps, hwm = (int(group) for group in match.groups())
    error = stderr.strip().splitlines()[-1] if stderr.strip() else ""
    return VerifyResult(exit_code == 0 and match is not None, records, gaps, hwm, error)


# ------------------------------------------------------------------- both: arithmetic


class Gap(NamedTuple):
    gap_ms: float
    start_rel_ms: float  # gap start relative to the fault
    end_rel_ms: float


def unavailability(ack_times_ms: Sequence[float], fault_ms: float, post_ms: float) -> Gap | None:
    """The longest gap between consecutive acknowledgements that ends after the fault and
    starts before `fault + post_ms` (it may END later: a slow recovery is a long gap, not
    a missing one); None when nothing at all was acknowledged after the fault (no
    recovery). Requires an acknowledgement before the fault."""
    times = sorted(ack_times_ms)
    if not times or times[0] > fault_ms:
        raise ValueError("no acknowledgement before the fault: the run never reached steady state")
    best = None
    for previous, current in zip(times, times[1:]):
        if current <= fault_ms:
            continue
        if previous >= fault_ms + post_ms:
            break
        gap = current - previous
        if best is None or gap > best.gap_ms:
            best = Gap(gap, previous - fault_ms, current - fault_ms)
    return best


def max_gap(ack_times_ms: Sequence[float], start_ms: float, end_ms: float) -> float:
    """The longest gap between consecutive acknowledgements both inside [start, end)."""
    inside = sorted(t for t in ack_times_ms if start_ms <= t < end_ms)
    if len(inside) < 2:
        raise ValueError("fewer than two acknowledgements in the window")
    return max(b - a for a, b in zip(inside, inside[1:]))


def record_latencies(events: Iterable[AckEvent], origin_ms: float, rate: float) -> list[tuple[float, float]]:
    """(intended send ms, latency ms) per acknowledged record, on the schedule
    `origin + sequence / rate`. A record acknowledged twice counts once (its first ack)."""
    if rate <= 0:
        raise ValueError(f"rate must be positive, got {rate}")
    seen = set()
    out = []
    per_record = 1000.0 / rate
    for event in sorted(events):
        for seq in range(event.first_seq, event.first_seq + event.records):
            if seq in seen:
                continue
            seen.add(seq)
            intended = origin_ms + seq * per_record
            out.append((intended, event.t_ms - intended))
    return out


def window_percentile(latencies: Iterable[tuple[float, float]], start_ms: float, end_ms: float,
                      q: float) -> float | None:
    """The q-th percentile latency of the records INTENDED to be sent in [start, end)."""
    values = [latency for intended, latency in latencies if start_ms <= intended < end_ms]
    if not values:
        return None
    return stats.percentile(values, q)


def ack_rate_bins(events: Iterable[AckEvent], fault_ms: float, start_rel_ms: int, end_rel_ms: int,
                  bin_ms: int) -> list[tuple[int, int]]:
    """Records acknowledged per bin, (bin start relative to the fault, records), every bin
    in [start, end) present, empty ones as 0."""
    if bin_ms <= 0 or end_rel_ms <= start_rel_ms:
        raise ValueError("bins need a positive width and a non-empty range")
    count = (end_rel_ms - start_rel_ms + bin_ms - 1) // bin_ms
    bins = [0] * count
    for event in events:
        rel = event.t_ms - fault_ms
        if start_rel_ms <= rel < end_rel_ms:
            bins[int((rel - start_rel_ms) // bin_ms)] += event.records
    return [(start_rel_ms + index * bin_ms, records) for index, records in enumerate(bins)]


def clock_offset(samples: Iterable[tuple[int, int, int]]) -> tuple[float, float]:
    """(offset ms, uncertainty ms) of the Docker VM clock relative to the host, from
    (host before ns, VM ns, host after ns) probes: the tightest bracket wins, the
    VM reading is assumed at its midpoint, and the uncertainty is half the bracket."""
    best = None
    for before, vm, after in samples:
        if after < before:
            raise ValueError("a clock probe ended before it began")
        width = after - before
        if best is None or width < best[0]:
            best = (width, vm - (before + after) / 2)
    if best is None:
        raise ValueError("no clock probes")
    return best[1] / 1e6, best[0] / 2e6
