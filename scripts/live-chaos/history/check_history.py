#!/usr/bin/env python3
"""Check a live-chaos operation history (#728, criterion A4 of #714).

A scenario records what its clients did and saw, as JSON lines that
`vtop-node produce --history` and `vtop-node verify --dump` append:

  produce  invoke  before a batch goes out: its producer sequences
                   [first_sequence, first_sequence + count)
           ok      acknowledged: the batch's records sit at the offsets the
                   broker answered with (`first_offset`, or `offsets` when
                   they are not consecutive: a resent batch is answered with
                   its original offsets under a later watermark); a history
                   without them places the records at
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
           end     the read's committed high watermark, and `start_offset`,
                   the offset it read from (0 unless retention had reclaimed
                   a prefix: `verify --from-offset`)

Each `verify --dump` is one read, ordered by its `end` event's time. The
history is checked against the last read; earlier reads are checked against
it too. Below the last read's `start_offset` (a prefix retention reclaimed,
in a long run such as the cluster soak, #727) the latest earlier read that
held an offset stands for it, so a record reclaimed before any read saw it
is unproven, and counted lost. The invariants:

  lost          every acknowledged record is visible at its acknowledged
                offset, with its own key and value
  duplicate     no offset is acknowledged for two different records, no
                record is acknowledged at two offsets, and no
                acknowledged or indeterminate record is visible a second time:
                a record is its producer's (id, epoch, sequence), so a retry
                from a new process sends the same records, not new ones
  order         a producer's records sit in sequence order: per producer id
                and epoch, across its processes, a higher sequence is
                acknowledged at a higher offset
  phantom       every visible record is an acknowledged one, or a whole
                indeterminate batch; a refused batch (`fail`) is never visible,
                which is what a fenced leader's records would be
  rewritten     a record an earlier read saw is the same record in every
                later read that covers its offset (from the later read's
                start offset to its high watermark)
  missed        a read holds every record a later read holds inside the
                earlier read's window (its start offset to its high
                watermark): a committed record is visible to every read that
                covers it
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
    # The offsets the broker answered with, one per record, when the history
    # records them.
    offsets: list[int] | None = None
    fencing_epoch: int | None = None
    time_us: int = 0
    # This invocation, among every batch in the history: a process name can
    # come round again (a reused pid), so (process, batch) is not unique.
    uid: int = 0

    def offset_of(self, index: int) -> int:
        """Where record `index` of an acknowledged batch sits."""
        if self.offsets is not None:
            return self.offsets[index]
        return self.committed_next_offset - self.count + index

    def name(self) -> str:
        return (
            f"{self.process} batch {self.batch} (sequences {self.first_sequence}.."
            f"{self.first_sequence + self.count - 1})"
        )

    def identity(self) -> str:
        """The producer the records belong to, across processes: `produce`
        names its process `produce:<producer id>:<producer epoch>:<pid>:<start
        time>`, and
        a retry from a new process under the same producer id and epoch sends
        the same logical records, which the log must hold at most once."""
        parts = self.process.split(":")
        if len(parts) >= 4 and parts[0] == "produce":
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
    # The offset the read started at: below it, retention had reclaimed the
    # records, and the read says nothing about them.
    start_offset: int = 0
    # As recorded: runs of (offset, first sequence, count, values_match), and
    # the offsets of unkeyed records. A week-long history holds hundreds of
    # reads (the cluster soak, #727), so a read is expanded to one entry per
    # offset only while it is being checked.
    runs: list[tuple[int, int, int, bool]] = field(default_factory=list)
    unkeyed: list[int] = field(default_factory=list)

    def records(self) -> dict[int, tuple[int | None, bool]]:
        """offset -> (sequence, or None for an unkeyed record; values_match)"""
        out: dict[int, tuple[int | None, bool]] = {}
        for offset, first, count, values_match in self.runs:
            for i in range(count):
                out[offset + i] = (first + i, values_match)
        for offset in self.unkeyed:
            out[offset] = (None, False)
        return out


def load(lines: list[str]) -> tuple[list[Batch], list[Read]]:
    batches: dict[tuple[str, int], Batch] = {}
    reads: dict[str, Read] = {}
    retired: list[Batch] = []
    finished: list[Read] = []
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
                # A repeated name is a new invocation, not the old one (review
                # of #878): the earlier batch keeps its outcome, and what
                # follows answers this one.
                if key in batches:
                    retired.append(batches[key])
                batches[key] = Batch(
                    process=process,
                    batch=event["batch"],
                    first_sequence=event["first_sequence"],
                    count=event["count"],
                    fencing_epoch=event.get("fencing_epoch"),
                    time_us=event.get("time_us", 0),
                    uid=number,
                )
            elif kind in ("ok", "fail", "info"):
                if key not in batches:
                    raise ValueError(f"line {number}: {kind} for a batch never invoked: {key}")
                batches[key].outcome = kind
                if kind == "ok":
                    batch = batches[key]
                    batch.committed_next_offset = event["committed_next_offset"]
                    if "offsets" in event:
                        batch.offsets = [int(o) for o in event["offsets"]]
                    elif "first_offset" in event:
                        batch.offsets = [event["first_offset"] + i for i in range(batch.count)]
                    if batch.offsets is not None and len(batch.offsets) != batch.count:
                        raise ValueError(
                            f"line {number}: {len(batch.offsets)} offsets for a {batch.count}-record batch"
                        )
            else:
                raise ValueError(f"line {number}: unknown produce event {kind!r}")
        elif op == "read":
            read = reads.setdefault(process, Read(process=process))
            if kind == "records":
                read.runs.append(
                    (event["offset"], event["first_sequence"], event["count"], bool(event["values_match"]))
                )
            elif kind == "unkeyed":
                read.unkeyed.append(event["offset"])
            elif kind == "end":
                read.high_watermark = event["high_watermark"]
                read.end_time_us = event.get("time_us", 0)
                read.start_offset = event.get("start_offset", 0)
                # An `end` closes the read: a later read under the same name
                # (a reused pid, in a run of days) is a read of its own
                # (review of #881).
                finished.append(reads.pop(process))
            else:
                raise ValueError(f"line {number}: unknown read event {kind!r}")
        else:
            raise ValueError(f"line {number}: unknown op {op!r}")
    finished.sort(key=lambda r: r.end_time_us)
    return retired + list(batches.values()), finished


def check(batches: list[Batch], reads: list[Read], allow_unkeyed: bool = False) -> dict:
    violations: dict[str, list[str]] = defaultdict(list)
    notes: list[str] = []
    if not reads:
        violations["input"].append("the history holds no completed read")
        return summarize(batches, reads, violations, notes)
    last = reads[-1]
    # Reads in order: each record must be what every earlier read saw at its
    # offset, and an offset an earlier read held must still be there in a later
    # read that covers it. `latest` is the newest record seen at each offset.
    latest: dict[int, tuple[tuple[int | None, bool], Read]] = {}
    for before, after in zip(reads, reads[1:]):
        if after.high_watermark < before.high_watermark:
            violations["hwm"].append(
                f"the committed high watermark went back from {before.high_watermark} "
                f"({before.process}) to {after.high_watermark} ({after.process})"
            )
    # Offsets a read covered and did not see: a later read that holds a
    # record there shows the earlier one missed a committed record. Promotion
    # markers are holes in every read, and never filled.
    holes: dict[int, Read] = {}
    records: dict[int, tuple[int | None, bool]] = {}
    for read in reads:
        records = read.records()
        for offset in range(read.start_offset, read.high_watermark):
            now = records.get(offset)
            if now is None:
                holes.setdefault(offset, read)
            seen = latest.get(offset)
            if seen is not None and now != seen[0]:
                violations["rewritten"].append(
                    f"offset {offset} held {seen[0]} in {seen[1].process} and holds {now} "
                    f"in {read.process}"
                )
        for offset, record in records.items():
            hole = holes.pop(offset, None)
            if hole is not None:
                violations["missed"].append(
                    f"offset {offset} holds {record} in {read.process} but {hole.process} did not "
                    f"see it below its high watermark {hole.high_watermark}"
                )
            latest[offset] = (record, read)
    # What the history shows at each offset: the last read, and below its start
    # offset whatever the newest earlier read saw there.
    visible = records
    if last.start_offset > 0:
        for offset, (record, _) in latest.items():
            if offset < last.start_offset:
                visible[offset] = record

    # Acknowledged batches: each record at its acknowledged offset. One offset
    # may be acknowledged again for the same logical record (the broker
    # answers a resent, already-acknowledged batch with its original offsets),
    # never for a different one; and one logical record is acknowledged at one
    # offset only, or a retry was appended a second time.
    claimed: dict[int, tuple[Batch, tuple[str, int]]] = {}
    placed: dict[tuple[str, int], tuple[int, Batch]] = {}
    acked = [b for b in batches if b.outcome == "ok"]
    for batch in acked:
        identity = batch.identity()
        for i in range(batch.count):
            offset, sequence = batch.offset_of(i), batch.first_sequence + i
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
                    f"acknowledged offset {offset} (high watermark {last.high_watermark}"
                    + (
                        f"; no read saw it before retention reclaimed offsets below "
                        f"{last.start_offset}"
                        if offset < last.start_offset
                        else ""
                    )
                    + ")"
                )
            elif seen[0] != sequence or not seen[1]:
                violations["lost"].append(
                    f"offset {offset} holds "
                    + (f"sequence {seen[0]}" if seen[0] is not None else "an unkeyed record")
                    + ("" if seen[1] else " with a value that is not the producer's")
                    + f", not acknowledged record {sequence} of {batch.name()}"
                )

    # A producer's records sit in sequence order: per logical producer (id
    # and epoch, across its invocations), a higher sequence is acknowledged at
    # a higher offset. That covers records within a batch, batches of one
    # process, and a restarted process under the same id and epoch, while a
    # retry acknowledged at its original offsets places nothing new (review
    # of #878).
    by_identity: dict[str, list[tuple[int, int, Batch]]] = defaultdict(list)
    for (identity, sequence), (offset, batch) in placed.items():
        by_identity[identity].append((sequence, offset, batch))
    for identity, records in by_identity.items():
        records.sort(key=lambda r: r[0])
        for (s0, o0, b0), (s1, o1, b1) in zip(records, records[1:]):
            if o1 <= o0:
                violations["order"].append(
                    f"record {s1} of {identity} was acknowledged at offset {o1} ({b1.name()}), "
                    f"not above record {s0} at offset {o0} ({b0.name()})"
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
    used: set[int] = set()
    # Logical records already visible: an unanswered attempt cannot explain a
    # second copy of a record its producer's retry was acknowledged for.
    surfaced: set[tuple[str, int]] = set()
    for batch in acked:
        surfaced |= batch.logical()
    keyed = [g for g in groups if visible[g[0]][0] is not None]
    # First, one assignment of whole unanswered batches to every keyed group
    # at once: committing to the first tiling of one group can leave a later
    # group none, where another tiling of the first would have served both
    # (review of #878). When there is none, each group is explained on its
    # own below, which is what names the groups that cannot be.
    def acked_at(offset: int):
        holder = claimed.get(offset)
        return holder[1] if holder is not None else None

    def task_of(group: list[int]) -> Task:
        return Task(
            [visible[o][0] for o in group],
            all(visible[o][1] for o in group),
            group[0],
            group[-1],
            acked_at,
        )

    tasks = [task_of(g) for g in keyed]
    if _explain_all(tasks, indeterminate, used, surfaced):
        keyed_explained = True
    else:
        keyed_explained = False
        used.clear()
        surfaced.clear()
        for batch in acked:
            surfaced |= batch.logical()
    for group in groups:
        sequences = [visible[o][0] for o in group]
        if sequences[0] is not None and keyed_explained:
            continue
        if sequences[0] is None:
            message = f"offsets {group[0]}..{group[-1]} hold records with no producer key"
            if allow_unkeyed:
                notes.append(message)
            else:
                violations["unkeyed"].append(message)
            continue
        all_values_match = all(visible[o][1] for o in group)
        if _explain(task_of(group), indeterminate, used, surfaced):
            continue
        matching_refused = [b.name() for b in refused if _covers(b, sequences)]
        already_visible = [
            b.name()
            for b in batches
            if b.outcome == "ok" or (b.outcome == "info" and b.uid in used)
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

    return summarize(batches, reads, violations, notes)


def _covers(batch: Batch, sequences: list[int]) -> bool:
    return batch.first_sequence <= sequences[0] and sequences[-1] < batch.first_sequence + batch.count


def _tilings(
    task: "Task",
    indeterminate: list[Batch],
    used: set[int],
    surfaced: set[tuple[str, int]],
    budget: list[int],
):
    """Each way indeterminate batches tile a group's sequences whole, in
    order, each used once and none repeating a logical record already
    visible. Yields with the tiling applied to `used` and `surfaced` (a batch
    is appended whole or not at all) and takes it back when resumed, so a
    caller that stops at a tiling keeps it. The search backtracks: the first
    batch that fits a position can lead to a dead end another would not (an
    unanswered 0..4 and an unanswered 0..9 under a visible 0..9). Batches of
    one producer with the same sequences are interchangeable, so each
    position tries one of each shape, the earliest. `budget` bounds the steps
    across every search.

    The first batch may begin before the group, and the last may end after
    it, where the records outside the group are visible right beside it and
    acknowledged there as the same logical records: a retry of part of an
    unanswered batch is answered with the offsets the batch already holds
    (review of #878). Those records are the acknowledgement's, not a second
    copy."""
    sequences = task.sequences
    if not task.values_match:
        return
    covering: dict[int, list[Batch]] = defaultdict(list)
    for batch in sorted(indeterminate, key=lambda b: (b.time_us, b.process, b.batch)):
        for sequence in range(batch.first_sequence, batch.first_sequence + batch.count):
            covering[sequence].append(batch)

    def beside(batch: Batch, offsets: range, first_sequence: int) -> bool:
        identity = batch.identity()
        return all(
            task.acked_at(offset) == (identity, first_sequence + i) for i, offset in enumerate(offsets)
        )

    def choices(position: int):
        """The batches that may tile from `position`, each with the records
        it puts inside the group. Checked as each is reached, against the
        search's state at that moment."""
        shapes: set[tuple[str, int, int]] = set()
        sequence = sequences[position]
        for batch in covering.get(sequence, []):
            shape = (batch.identity(), batch.first_sequence, batch.count)
            before = sequence - batch.first_sequence
            inside = min(batch.count - before, len(sequences) - position)
            after = batch.count - before - inside
            if (
                shape in shapes
                or batch.uid in used
                or (before and position != 0)
                or (after and position + inside != len(sequences))
                or sequences[position + inside - 1] != sequence + inside - 1
                or not beside(batch, range(task.first_offset - before, task.first_offset), batch.first_sequence)
                or not beside(batch, range(task.last_offset + 1, task.last_offset + 1 + after), sequence + inside)
            ):
                continue
            identity = batch.identity()
            logical = {(identity, sequence + i) for i in range(inside)}
            if logical & surfaced:
                continue
            shapes.add(shape)
            yield batch, inside, logical

    # An explicit stack, not recursion (review of #878): a run explained by a
    # thousand unanswered batches is a thousand levels deep, past Python's
    # own limit; the budget is what bounds the search. A frame is a position,
    # its remaining choices, and the choice it has applied.
    if budget[0] <= 0:
        return
    budget[0] -= 1
    stack: list[list] = [[0, choices(0), None]]
    while stack:
        frame = stack[-1]
        if frame[2] is not None:
            batch, logical = frame[2]
            surfaced.difference_update(logical)
            used.discard(batch.uid)
            frame[2] = None
        chosen = next(frame[1], None)
        if chosen is None:
            stack.pop()
            continue
        batch, inside, logical = chosen
        used.add(batch.uid)
        surfaced.update(logical)
        frame[2] = (batch, logical)
        position = frame[0] + inside
        if position == len(sequences):
            yield
            continue
        budget[0] -= 1
        if budget[0] < 0:
            # Out of steps: take back every choice still applied, and give up.
            for open_frame in stack:
                if open_frame[2] is not None:
                    applied, applied_logical = open_frame[2]
                    surfaced.difference_update(applied_logical)
                    used.discard(applied.uid)
            return
        stack.append([position, choices(position), None])


@dataclass
class Task:
    """One visible group to explain: its sequences, whether every value is the
    producer's, its offsets, and which logical record is acknowledged where."""

    sequences: list[int]
    values_match: bool
    first_offset: int
    last_offset: int
    acked_at: "object"


def _explain(
    task: Task,
    indeterminate: list[Batch],
    used: set[int],
    surfaced: set[tuple[str, int]],
) -> bool:
    """Whether one group has a tiling; the first found is kept applied."""
    tilings = _tilings(task, indeterminate, used, surfaced, [SEARCH_BUDGET])
    return next(tilings, False) is None


def _explain_all(
    tasks: list[Task],
    indeterminate: list[Batch],
    used: set[int],
    surfaced: set[tuple[str, int]],
) -> bool:
    """Whether every group has a tiling at once, backtracking across groups;
    the assignment found is kept applied, and nothing is when there is none."""
    budget = [SEARCH_BUDGET]
    # One suspended search per group, in order; resuming one takes its tiling
    # back and tries the next. A stack of generators rather than recursion,
    # for the same reason as in _tilings.
    searches: list = []
    while len(searches) < len(tasks):
        index = len(searches)
        searches.append(_tilings(tasks[index], indeterminate, used, surfaced, budget))
        while next(searches[-1], False) is not None:
            searches.pop()
            if not searches:
                return False
    return True


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
        "visible_records": len(reads[-1].records()) if reads else 0,
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
