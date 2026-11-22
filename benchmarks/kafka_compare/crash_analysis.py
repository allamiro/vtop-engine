"""The whole-replica-set crash benchmark's parsing and arithmetic (#599), free of Docker so it can be tested.

Every number in crash.csv comes from a function here, applied to what a crash left
behind: the producer's acknowledgement log, the read-backs after the restart, and
the two canary files.

Definitions (the report repeats them next to the numbers):

- **Acknowledged**: the client printed `producer_send_success` for the record, with
  `acks=all`, before the cluster was killed. The client is killed right after the
  cluster, so nothing is acknowledged after the crash.
- **Lost**: an acknowledged record that a consumer reading the partition from the
  start, after the cluster is back and a sentinel record has been acknowledged
  (see `crash.py`), does not find. A record that was written but never
  acknowledged (`unacked_present`) is not a loss and not a duplicate.
- **Duplicate**: an extra copy of a record id in that read.
- **Reordered**: a record read after a record with a larger id.
- **Offset mismatch**: an acknowledged record that is present, but not at the offset
  the broker acknowledged it at.
- **Lost window**: the age, at the moment of the kill, of the oldest lost
  acknowledgement.
- **Canary**: two files on the data filesystem. The one that was fsynced must be whole
  after the crash. The one that was only written must be absent or short when the
  crash drops the page cache, and whole when it does not. A crash whose canary does
  not behave is `refused`: the method did not do what it claims, so the row says
  nothing about the system.
"""
from __future__ import annotations

import json
import math
import random
import re
from collections.abc import Iterable, Sequence
from typing import NamedTuple

CRASH_MODES = ("sigkill", "flakey-drop")
CRASH_MODE_LABELS = {
    "sigkill": "SIGKILL of all three nodes at once (the host's page cache survives)",
    "flakey-drop": "SIGKILL of all three nodes at once, then every write the page cache had not "
                   "flushed is dropped (dm-flakey drop_writes): power loss of the whole replica set",
}
# The configurations: (system, profile). VTOP has one configuration and is
# paired with both Kafka profiles (it acknowledges after a quorum fsync under either).
CONFIGS = {
    "kafka-defaults": ("kafka", "defaults"),
    "kafka-fsync-parity": ("kafka", "fsync-parity"),
    "vtop-shipped": ("vtop", "defaults"),
}
VALUE_SEP = "."
CANARY_BYTES = 1048576
MIN_RECORD_SIZE = 64


class ParseError(ValueError):
    """A run's raw output is not what the parser expects; the message says where."""


# ------------------------------------------------------------------------ the records


def value_prefix(record_size: int, crash: int) -> str:
    """The prefix every record of one crash carries: the value is `<prefix>.<n>`.

    `kafka-verifiable-producer.sh` sends the counter as the value and has no
    record-size flag, only `--value-prefix`, so the size is set through the prefix.
    The counter adds a dot and up to 6 digits, so a record is `record_size` bytes
    minus at most 6.
    """
    head = f"kc599-c{crash:03d}-"
    length = record_size - 1 - 6
    if record_size < MIN_RECORD_SIZE:
        raise ValueError(f"record size {record_size} is below the {MIN_RECORD_SIZE} bytes the crash id and "
                         "counter need")
    return (head + "x" * length)[:length]


def sentinel_value(crash: int) -> str:
    return f"sentinel-c{crash:03d}"


class Ack(NamedTuple):
    t_ms: float
    seq: int
    offset: int


class ProducerRun(NamedTuple):
    startup_ms: float
    acks: list[Ack]
    errors: int
    sent: int | None


def parse_acks(lines: Iterable[str], prefix: str) -> ProducerRun:
    """The JSON lines `kafka-verifiable-producer.sh` prints, one per send outcome.

    Every acknowledged value must be `<prefix>.<n>`; log lines interleaved on
    stdout are skipped. `sent` is the tool's own count when it got to print it
    (a producer that is killed never does).
    """
    startup = None
    acks: list[Ack] = []
    errors = 0
    sent = None
    for line in lines:
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = event.get("name")
        if name == "startup_complete":
            startup = float(event["timestamp"])
        elif name == "producer_send_success":
            value = event.get("value")
            head, _, tail = value.rpartition(VALUE_SEP) if isinstance(value, str) else ("", "", "")
            if head != prefix or not tail.isdigit() or str(int(tail)) != tail:
                raise ParseError(f"producer_send_success with {value!r}, not a record this crash sent "
                                 f"(expected {prefix[:12]}....<n>)")
            acks.append(Ack(float(event["timestamp"]), int(tail), int(event["offset"])))
        elif name == "producer_send_error":
            errors += 1
        elif name == "tool_data":
            sent = int(event["sent"]) if "sent" in event else None
    if startup is None:
        raise ParseError("no startup_complete line: the producer never started")
    acks.sort()
    return ProducerRun(startup, acks, errors, sent)


_CONSUMER_LINE = re.compile(r"^Offset:(\d+)\t(.*)$")


class Consumed(NamedTuple):
    offset: int
    kind: str  # "record" (this crash's), "sentinel", or "other"
    seq: int | None


def parse_consumed(text: str, prefix: str, sentinel: str) -> list[Consumed]:
    """(offset, kind, id) per record from `kafka-console-consumer.sh --property print.offset=true`."""
    records = []
    for line in text.splitlines():
        match = _CONSUMER_LINE.match(line)
        if not match:
            if line.startswith("Offset:"):
                raise ParseError(f"consumer line not in the expected shape: {line[:80]!r}")
            continue
        offset, value = int(match.group(1)), match.group(2)
        head, _, tail = value.rpartition(VALUE_SEP)
        if value == sentinel:
            records.append(Consumed(offset, "sentinel", None))
        elif head == prefix and tail.isdigit() and str(int(tail)) == tail:
            records.append(Consumed(offset, "record", int(tail)))
        else:
            records.append(Consumed(offset, "other", None))
    return records


# --------------------------------------------------------------------------- the diff


class Diff(NamedTuple):
    acked: int
    acked_lost: int
    lost: list[int]
    lost_first_seq: int | None
    lost_last_seq: int | None
    lost_runs: int
    duplicates: int
    reordered: int
    offset_mismatch: int
    unacked_present: int
    unexpected: int


def diff_acked(acks: Sequence[Ack], consumed: Sequence[Consumed]) -> Diff:
    """Every acknowledged id against what a full read of the partition holds."""
    offsets_of: dict[int, list[int]] = {}
    highest = -1
    reordered = 0
    unexpected = 0
    for item in sorted(consumed, key=lambda c: c.offset):
        if item.kind == "sentinel":
            continue
        if item.kind != "record":
            unexpected += 1
            continue
        assert item.seq is not None
        if item.seq < highest:
            reordered += 1
        highest = max(highest, item.seq)
        offsets_of.setdefault(item.seq, []).append(item.offset)
    acked = {a.seq: a for a in acks}
    lost = sorted(seq for seq in acked if seq not in offsets_of)
    runs = sum(1 for i, seq in enumerate(lost) if i == 0 or seq != lost[i - 1] + 1)
    mismatch = sum(1 for seq, a in acked.items() if seq in offsets_of and a.offset not in offsets_of[seq])
    duplicates = sum(len(offsets) - 1 for offsets in offsets_of.values())
    return Diff(acked=len(acked), acked_lost=len(lost), lost=lost, lost_first_seq=lost[0] if lost else None,
                lost_last_seq=lost[-1] if lost else None, lost_runs=runs, duplicates=duplicates,
                reordered=reordered, offset_mismatch=mismatch, unacked_present=len(set(offsets_of) - set(acked)),
                unexpected=unexpected)


def acked_in_window(acks: Iterable[Ack], trigger_ms: float, window_s: float) -> int:
    """Acknowledgements in the `window_s` seconds before the kill."""
    return sum(1 for a in acks if trigger_ms - window_s * 1000 <= a.t_ms <= trigger_ms)


def lost_window_seconds(acks: Iterable[Ack], lost: Iterable[int], trigger_ms: float) -> float | None:
    """The age, at the kill, of the oldest lost acknowledgement; None when nothing was lost."""
    wanted = set(lost)
    times = [a.t_ms for a in acks if a.seq in wanted]
    return (trigger_ms - min(times)) / 1000 if times else None


# ------------------------------------------------------------------------------ canary


class Canary(NamedTuple):
    """Bytes found after the crash in the file that was fsynced and the one that was not."""

    synced_bytes: int
    synced_expected: int
    unsynced_bytes: int
    unsynced_expected: int


class Verdict(NamedTuple):
    ok: bool
    reason: str


def judge_canary(mode: str, canary: Canary) -> Verdict:
    """Did the crash do what its mode says? Wording follows the operator-message rule:
    what happened, what that means, what to do."""
    if canary.synced_bytes != canary.synced_expected:
        return Verdict(False, (
            f"the canary that was fsynced is missing or short after the crash ({canary.synced_bytes} bytes of "
            f"{canary.synced_expected}). The data disk does not keep what an fsync returned for, so no result "
            "from this crash means anything. Check the device under the data directory and that fsync reaches "
            "it, then repeat."))
    if mode == "flakey-drop" and canary.unsynced_bytes >= canary.unsynced_expected:
        return Verdict(False, (
            f"the unsynced canary survived whole ({canary.unsynced_bytes} bytes). The crash did not drop the "
            "page cache, so this row measures a process kill, not a power loss. Check that the data directory "
            "is on the dm-flakey device and that drop_writes was loaded, then repeat."))
    if mode == "sigkill" and canary.unsynced_bytes < canary.unsynced_expected:
        return Verdict(False, (
            f"the unsynced canary is short after a process kill ({canary.unsynced_bytes} of "
            f"{canary.unsynced_expected} bytes), but a killed process leaves the page cache intact. Something "
            "else dropped the cache; the row is not the control it claims to be. Repeat on a quiet host."))
    return Verdict(True, "")


def canary_flags(canary: Canary) -> dict[str, bool]:
    return {"canary_synced_ok": canary.synced_bytes == canary.synced_expected,
            "canary_unsynced_lost": canary.unsynced_bytes < canary.unsynced_expected}


# ------------------------------------------------------------------- schedule and bounds


def trigger_schedule(seed: int, crashes: int, low_s: float, high_s: float) -> list[float]:
    """Seconds after the producer starts at which crash i is triggered, uniform in
    [low_s, high_s]. Crash i's time depends on the seed and on i only, so every
    configuration sees the same schedule (a paired design), a longer run extends
    it, and a resumed run agrees with the one it resumes."""
    if not (0 < low_s <= high_s):
        raise ValueError(f"trigger window [{low_s}, {high_s}] must be positive and ordered")
    return [round(random.Random(f"kc599:{seed}:{i}").uniform(low_s, high_s), 3) for i in range(1, crashes + 1)]


def upper_bound_95(events: int, trials: int) -> float:
    """The one-sided 95 % upper confidence bound on the chance that a crash loses
    something, given `events` of `trials` crashes that did (exact binomial,
    Clopper-Pearson). For 0 of 30 it is 9.5 %, for 0 of 20 it is 13.9 %."""
    if trials < 1 or events < 0 or events > trials:
        raise ValueError(f"{events} of {trials} is not a count of crashes")
    if events == trials:
        return 1.0
    if events == 0:
        return 1 - 0.05 ** (1 / trials)
    low, high = 0.0, 1.0
    for _ in range(80):
        p = (low + high) / 2
        cdf = sum(math.comb(trials, k) * p**k * (1 - p) ** (trials - k) for k in range(events + 1))
        if cdf > 0.05:
            low = p
        else:
            high = p
    return (low + high) / 2


def suspect_method(rows: Iterable[dict], min_crashes: int) -> str | None:
    """The message to stop the run with when Kafka's defaults lost nothing in `min_crashes`
    or more power-loss crashes: page-cache loss is what the method is for, and Kafka as
    shipped does not fsync, so zero losses means the method is probably not working."""
    ok = [r for r in rows if r["config"] == "kafka-defaults" and r["crash_mode"] == "flakey-drop"
          and r["status"] == "ok"]
    if len(ok) < min_crashes or any(int(float(r["acked_lost"])) > 0 for r in ok):
        return None
    return (f"the Kafka defaults configuration lost nothing in {len(ok)} crashes. That is the outcome the method "
            "should not give if page-cache loss is happening, since Kafka as shipped acknowledges before it "
            "flushes. The run stopped before any chart. Check that the data directory is on the dm-flakey "
            "device and that drop_writes was loaded, and compare with a run on a different kernel.")
