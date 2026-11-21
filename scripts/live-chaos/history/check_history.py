#!/usr/bin/env python3
"""Check a live-chaos operation history (#728, criterion A4 of #714).

A scenario records what its clients did and saw, as JSON lines that
`vtop-node produce --history` and `vtop-node verify --dump` append:

  produce  invoke  before a batch goes out: its producer sequences
                   [first_sequence, first_sequence + count)
           ok      acknowledged: the batch's records sit at
                   [committed_next_offset - count, committed_next_offset)
           fail    refused by the broker before anything was appended (a
                   wrong range, a sequence conflict), or any refusal from a
                   leader the scenario fenced (`produce --fenced-probe`): the
                   records must never be visible
           info    no answer (the leader died, the connection closed), or an
                   answer that does not settle it ("quorum not reached" comes
                   after the leader's own append, a storage error can come part
                   way through one, and `Fenced` can come from the lease check
                   the broker repeats after replicating): the batch is
                   indeterminate, visible once or not at all
           (an invoke with no outcome, from a process killed mid-batch, is
           indeterminate too)
  read     records a run of visible records: `count` records at consecutive
                   offsets from `offset`, keys carrying consecutive producer
                   sequences from `first_sequence`; `values_match` says every
                   value is the producer's value for its sequence
           unkeyed a visible record whose key is not a producer sequence
           end     the read's committed high watermark

Each `verify --dump` is one read, ordered by its `end` event's time. The
history is checked against the last read; earlier reads are checked against
it too. The invariants:

  lost          every acknowledged record is visible at its acknowledged
                offset, with its own key and value
  duplicate     no offset is acknowledged for two different records, no
                record is acknowledged at two offsets, and no
                acknowledged or indeterminate record is visible a second time:
                a record is its producer's (id, epoch, sequence), so a retry
                from a new process sends the same records, not new ones
  order         a producer's acknowledgements move forward: each acknowledged
                batch sits above the one acknowledged before it
  phantom       every visible record is an acknowledged one, or a whole
                indeterminate batch; a refused batch (`fail`) is never visible,
                which is what a fenced leader's records would be
  rewritten     a record an earlier read saw below the later read's high
                watermark is the same record in the later read
  hwm           the committed high watermark never goes backwards across reads
  unkeyed       no visible record lacks a producer key (with --allow-unkeyed,
                such records are reported and not counted as violations)

Usage: check_history.py [--allow-unkeyed] HISTORY.jsonl [HISTORY.jsonl ...]
Prints a JSON summary; exits 1 when any invariant is violated, 2 on bad input.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field

# How many violations of one kind the summary lists in full.
LISTED = 20
# Steps the search for a tiling of one visible group may take before it gives
# up and reports the group unexplained.
SEARCH_BUDGET = 100_000


@dataclass
class Batch:
    process: str
    batch: int
    first_sequence: int
    count: int
    outcome: str = "info"  # ok | fail | info
    committed_next_offset: int | None = None
    fencing_epoch: int | None = None
    time_us: int = 0

    def name(self) -> str:
        return (
            f"{self.process} batch {self.batch} (sequences {self.first_sequence}.."
            f"{self.first_sequence + self.count - 1})"
        )

    def identity(self) -> str:
        """The producer the records belong to, across processes: `produce`
        names its process `produce:<producer id>:<producer epoch>:<pid>`, and
        a retry from a new process under the same producer id and epoch sends
        the same logical records, which the log must hold at most once."""
        parts = self.process.split(":")
        if len(parts) == 4 and parts[0] == "produce":
            return ":".join(parts[:3])
        return self.process

    def logical(self) -> set[tuple[str, int]]:
        identity = self.identity()
        return {(identity, self.first_sequence + i) for i in range(self.count)}


@dataclass
class Read:
    process: str
    end_time_us: int = 0
    high_watermark: int | None = None
    # offset -> (sequence or None for unkeyed, values_match)
    records: dict[int, tuple[int | None, bool]] = field(default_factory=dict)


def load(lines: list[str]) -> tuple[list[Batch], list[Read]]:
    batches: dict[tuple[str, int], Batch] = {}
    reads: dict[str, Read] = {}
    for number, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as err:
            raise ValueError(f"line {number}: not JSON: {err}") from err
        op, kind, process = event.get("op"), event.get("type"), event.get("process")
        if op == "produce":
            key = (process, event["batch"])
            if kind == "invoke":
                batches[key] = Batch(
                    process=process,
                    batch=event["batch"],
                    first_sequence=event["first_sequence"],
                    count=event["count"],
                    fencing_epoch=event.get("fencing_epoch"),
                    time_us=event.get("time_us", 0),
                )
            elif kind in ("ok", "fail", "info"):
                if key not in batches:
                    raise ValueError(f"line {number}: {kind} for a batch never invoked: {key}")
                batches[key].outcome = kind
                if kind == "ok":
                    batches[key].committed_next_offset = event["committed_next_offset"]
            else:
                raise ValueError(f"line {number}: unknown produce event {kind!r}")
        elif op == "read":
            read = reads.setdefault(process, Read(process=process))
            if kind == "records":
                for i in range(event["count"]):
                    read.records[event["offset"] + i] = (
                        event["first_sequence"] + i,
                        bool(event["values_match"]),
                    )
            elif kind == "unkeyed":
                read.records[event["offset"]] = (None, False)
            elif kind == "end":
                read.high_watermark = event["high_watermark"]
                read.end_time_us = event.get("time_us", 0)
            else:
                raise ValueError(f"line {number}: unknown read event {kind!r}")
        else:
            raise ValueError(f"line {number}: unknown op {op!r}")
    finished = [r for r in reads.values() if r.high_watermark is not None]
    finished.sort(key=lambda r: r.end_time_us)
    return list(batches.values()), finished


def check(batches: list[Batch], reads: list[Read], allow_unkeyed: bool = False) -> dict:
    violations: dict[str, list[str]] = defaultdict(list)
    notes: list[str] = []
    if not reads:
        violations["input"].append("the history holds no completed read")
        return summarize(batches, reads, violations, notes)
    last = reads[-1]
    visible = last.records

    # Acknowledged batches: each record at its acknowledged offset. One offset
    # may be acknowledged again for the same logical record (the broker
    # answers a resent, already-acknowledged batch with its original offsets),
    # never for a different one; and one logical record is acknowledged at one
    # offset only, or a retry was appended a second time.
    claimed: dict[int, tuple[Batch, tuple[str, int]]] = {}
    placed: dict[tuple[str, int], tuple[int, Batch]] = {}
    acked = [b for b in batches if b.outcome == "ok"]
    for batch in acked:
        start = batch.committed_next_offset - batch.count
        identity = batch.identity()
        for i in range(batch.count):
            offset, sequence = start + i, batch.first_sequence + i
            record = (identity, sequence)
            other = claimed.get(offset)
            if other is not None and other[1] != record:
                violations["duplicate"].append(
                    f"offset {offset} is acknowledged twice, for different records: by "
                    f"{other[0].name()} and {batch.name()}"
                )
            first = placed.get(record)
            if first is not None and first[0] != offset:
                violations["duplicate"].append(
                    f"record {sequence} of {identity} is acknowledged at offset {first[0]} by "
                    f"{first[1].name()} and again at offset {offset} by {batch.name()}"
                )
            claimed.setdefault(offset, (batch, record))
            placed.setdefault(record, (offset, batch))
            seen = visible.get(offset)
            if seen is None:
                violations["lost"].append(
                    f"acknowledged record {sequence} of {batch.name()} is not visible at its "
                    f"acknowledged offset {offset} (high watermark {last.high_watermark})"
                )
            elif seen[0] != sequence or not seen[1]:
                violations["lost"].append(
                    f"offset {offset} holds "
                    + (f"sequence {seen[0]}" if seen[0] is not None else "an unkeyed record")
                    + ("" if seen[1] else " with a value that is not the producer's")
                    + f", not acknowledged record {sequence} of {batch.name()}"
                )

    # A producer's acknowledgements move forward.
    by_process: dict[str, list[Batch]] = defaultdict(list)
    for batch in acked:
        by_process[batch.process].append(batch)
    for process, mine in by_process.items():
        mine.sort(key=lambda b: b.batch)
        for before, after in zip(mine, mine[1:]):
            if after.committed_next_offset - after.count < before.committed_next_offset:
                violations["order"].append(
                    f"{after.name()} was acknowledged at offsets "
                    f"{after.committed_next_offset - after.count}.."
                    f"{after.committed_next_offset - 1}, not above {before.name()} at .."
                    f"{before.committed_next_offset - 1}"
                )

    # Everything visible and unclaimed must be a whole indeterminate batch.
    unclaimed = sorted(o for o in visible if o not in claimed)
    groups: list[list[int]] = []
    for offset in unclaimed:
        sequence = visible[offset][0]
        if (
            groups
            and groups[-1][-1] + 1 == offset
            and sequence is not None
            and visible[groups[-1][-1]][0] is not None
            and visible[groups[-1][-1]][0] + 1 == sequence
        ):
            groups[-1].append(offset)
        else:
            groups.append([offset])
    indeterminate = [b for b in batches if b.outcome == "info"]
    refused = [b for b in batches if b.outcome == "fail"]
    used: set[tuple[str, int]] = set()
    # Logical records already visible: an unanswered attempt cannot explain a
    # second copy of a record its producer's retry was acknowledged for.
    surfaced: set[tuple[str, int]] = set()
    for batch in acked:
        surfaced |= batch.logical()
    for group in groups:
        sequences = [visible[o][0] for o in group]
        if sequences[0] is None:
            message = f"offsets {group[0]}..{group[-1]} hold records with no producer key"
            if allow_unkeyed:
                notes.append(message)
            else:
                violations["unkeyed"].append(message)
            continue
        all_values_match = all(visible[o][1] for o in group)
        explained = _explain(sequences, all_values_match, indeterminate, used, surfaced)
        if explained is not None:
            continue
        matching_refused = [b.name() for b in refused if _covers(b, sequences)]
        already_visible = [
            b.name()
            for b in batches
            if b.outcome == "ok" or (b.outcome == "info" and (b.process, b.batch) in used)
            if _covers(b, sequences)
        ]
        if already_visible and all_values_match:
            violations["duplicate"].append(
                f"offsets {group[0]}..{group[-1]} (sequences {sequences[0]}..{sequences[-1]}) "
                f"repeat records already visible elsewhere, of {already_visible[0]}"
            )
        elif matching_refused:
            violations["phantom"].append(
                f"offsets {group[0]}..{group[-1]} (sequences {sequences[0]}..{sequences[-1]}) "
                f"are visible, and only a refused produce wrote them: {matching_refused[0]}"
            )
        else:
            violations["phantom"].append(
                f"offsets {group[0]}..{group[-1]} (sequences {sequences[0]}..{sequences[-1]}) "
                "are visible, and no acknowledged or indeterminate batch accounts for them"
            )

    # Earlier reads: the high watermark never regresses, and what they saw
    # below the later read's mark is what the later read holds.
    for before, after in zip(reads, reads[1:]):
        if after.high_watermark < before.high_watermark:
            violations["hwm"].append(
                f"the committed high watermark went back from {before.high_watermark} "
                f"({before.process}) to {after.high_watermark} ({after.process})"
            )
    for earlier in reads[:-1]:
        for offset, record in sorted(earlier.records.items()):
            if offset >= last.high_watermark:
                continue
            later = visible.get(offset)
            if later != record:
                violations["rewritten"].append(
                    f"offset {offset} held {record} in {earlier.process} and holds {later} "
                    f"in {last.process}"
                )
    return summarize(batches, reads, violations, notes)


def _covers(batch: Batch, sequences: list[int]) -> bool:
    return batch.first_sequence <= sequences[0] and sequences[-1] < batch.first_sequence + batch.count


def _explain(
    sequences: list[int],
    values_match: bool,
    indeterminate: list[Batch],
    used: set[tuple[str, int]],
    surfaced: set[tuple[str, int]],
) -> list[Batch] | None:
    """Indeterminate batches that tile `sequences` whole, in order, each used
    once and none repeating a logical record already visible; `None` when no
    tiling exists. A batch is appended whole or not at all. The search
    backtracks: the first batch that fits a position can lead to a dead end
    another would not (an unanswered 0..4 and an unanswered 0..9 under a
    visible 0..9). Batches of one producer with the same sequences are
    interchangeable, so each position tries one of each shape, the earliest."""
    if not values_match:
        return None
    by_start: dict[int, list[Batch]] = defaultdict(list)
    for batch in sorted(indeterminate, key=lambda b: (b.time_us, b.process, b.batch)):
        by_start[batch.first_sequence].append(batch)
    chosen: list[Batch] = []
    budget = [SEARCH_BUDGET]

    def search(position: int) -> bool:
        if position == len(sequences):
            return True
        budget[0] -= 1
        if budget[0] < 0:
            return False
        shapes: set[tuple[str, int]] = set()
        for batch in by_start.get(sequences[position], []):
            shape = (batch.identity(), batch.count)
            if (
                shape in shapes
                or (batch.process, batch.batch) in used
                or position + batch.count > len(sequences)
                or sequences[position + batch.count - 1] != batch.first_sequence + batch.count - 1
            ):
                continue
            logical = batch.logical()
            if logical & surfaced:
                continue
            shapes.add(shape)
            used.add((batch.process, batch.batch))
            surfaced.update(logical)
            chosen.append(batch)
            if search(position + batch.count):
                return True
            chosen.pop()
            surfaced.difference_update(logical)
            used.discard((batch.process, batch.batch))
        return False

    return list(chosen) if search(0) else None

def summarize(batches, reads, violations, notes) -> dict:
    counts = defaultdict(int)
    for batch in batches:
        counts[batch.outcome] += 1
    return {
        "valid": not violations,
        "batches": dict(counts),
        "acknowledged_records": sum(b.count for b in batches if b.outcome == "ok"),
        "reads": len(reads),
        "high_watermark": reads[-1].high_watermark if reads else None,
        "visible_records": len(reads[-1].records) if reads else 0,
        "violations": {
            kind: {"count": len(items), "first": items[:LISTED]}
            for kind, items in sorted(violations.items())
        },
        "notes": notes,
    }


def main(argv: list[str]) -> int:
    args = argv[1:]
    allow_unkeyed = "--allow-unkeyed" in args
    paths = [a for a in args if a != "--allow-unkeyed"]
    if not paths:
        print(__doc__, file=sys.stderr)
        return 2
    lines: list[str] = []
    try:
        for path in paths:
            with open(path, encoding="utf-8") as handle:
                lines.extend(handle.readlines())
        batches, reads = load(lines)
    except (OSError, ValueError, KeyError) as err:
        print(f"check_history: {err}", file=sys.stderr)
        return 2
    result = check(batches, reads, allow_unkeyed)
    print(json.dumps(result, indent=2))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
