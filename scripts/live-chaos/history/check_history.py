#!/usr/bin/env python3
"""Check a live-chaos operation history (#728, criterion A4 of #714).

A scenario records what its clients did and saw, as JSON lines that
`vtop-node produce --history` and `vtop-node verify --dump` append:

  produce  invoke  before a batch goes out: its producer sequences
                   [first_sequence, first_sequence + count)
           ok      acknowledged: the batch's records sit at
                   [committed_next_offset - count, committed_next_offset)
           fail    refused by the broker before anything was appended (a
                   fenced leader, a wrong range, a sequence conflict): the
                   records must never be visible
           info    no answer (the leader died, the connection closed), or an
                   answer that does not settle it ("quorum not reached" comes
                   after the leader's own append, a storage error can come part
                   way through one): the batch is indeterminate, visible once
                   or not at all
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
  duplicate     no two acknowledged batches claim one offset, and no
                acknowledged or indeterminate record is visible a second time
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

    # Acknowledged batches: each record at its acknowledged offset.
    claimed: dict[int, Batch] = {}
    acked = [b for b in batches if b.outcome == "ok"]
    for batch in acked:
        start = batch.committed_next_offset - batch.count
        for i in range(batch.count):
            offset, sequence = start + i, batch.first_sequence + i
            other = claimed.get(offset)
            if other is not None:
                violations["duplicate"].append(
                    f"offset {offset} is acknowledged twice: by {other.name()} and {batch.name()}"
                )
            claimed[offset] = batch
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
        explained = _explain(sequences, all_values_match, indeterminate, used)
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
) -> list[Batch] | None:
    """Indeterminate batches that tile `sequences` whole, in order, each used
    once; `None` when none do. A batch is appended whole or not at all."""
    if not values_match:
        return None
    position, chosen = 0, []
    while position < len(sequences):
        candidates = [
            b
            for b in indeterminate
            if (b.process, b.batch) not in used
            and b.first_sequence == sequences[position]
            and position + b.count <= len(sequences)
        ]
        if not candidates:
            for b in chosen:
                used.discard((b.process, b.batch))
            return None
        batch = min(candidates, key=lambda b: (b.time_us, b.process, b.batch))
        used.add((batch.process, batch.batch))
        chosen.append(batch)
        position += batch.count
    return chosen


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
