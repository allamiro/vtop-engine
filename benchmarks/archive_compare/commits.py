"""The offset sampler (#746 "live"): time to source-committed and the consumer group's lag, from the broker.

Every offset a group commits is a record of `__consumer_offsets`, and the broker stamps it. The lab's `commits`
service (compose.yml) writes each one, as it happens, to `commits.jsonl` with the Kafka tools' own formatter, so
the harness needs no 100 ms polling loop and misses no commit between two samples. This module reads that file:

- `parse` turns it into `Commit`s (the formatter writes JSON objects with no separator between them);
- `Timeline` answers, for one group, the latest committed offset of a partition at a time, and when the group
  first committed past a given record (its source offset was committed);
- `lag` is the records the group has not yet committed past, against the partition end offsets the producer wrote.

The producer is the topic's only writer, in order, so a record's partition and offset are known without asking the
broker (`producer.partition_of` and the per-partition count); `run.py` checks the end offsets before a tool starts.
A committed offset is the NEXT offset to read, so a record at offset `o` is committed once the group committed
an offset greater than `o`.
"""
from __future__ import annotations

import json
from collections.abc import Iterable
from typing import NamedTuple


class Commit(NamedTuple):
    group: str
    topic: str
    partition: int
    offset: int
    at: float  # epoch seconds, the broker's stamp (ms resolution)


def parse(text: str) -> list[Commit]:
    """Offset commits in the formatter's output. Group metadata, tombstones and a partly written tail are skipped."""
    decoder = json.JSONDecoder()
    out: list[Commit] = []
    index = 0
    while index < len(text):
        while index < len(text) and text[index] in " \t\r\n":
            index += 1
        if index >= len(text):
            break
        try:
            doc, index = decoder.raw_decode(text, index)
        except ValueError:
            break  # the file is being written: the tail is not a whole object yet
        key, value = doc.get("key") or {}, doc.get("value")
        # type 1 (and 0) is an offset commit; group metadata and the like carry other types.
        if key.get("type") not in (0, 1) or not value:
            continue
        data, body = key.get("data") or {}, value.get("data") or {}
        if "offset" not in body or "partition" not in data:
            continue
        out.append(Commit(str(data["group"]), str(data["topic"]), int(data["partition"]), int(body["offset"]),
                          int(body.get("commitTimestamp", 0)) / 1000.0))
    return out


def read(path: str) -> list[Commit]:
    try:
        with open(path, encoding="utf-8") as handle:
            return parse(handle.read())
    except FileNotFoundError:
        return []


class Timeline:
    """One group's commits on one topic, in the order the broker wrote them."""

    def __init__(self, commits: Iterable[Commit], group: str, topic: str = "bench"):
        self.by_partition: dict[int, list[Commit]] = {}
        for commit in commits:
            if commit.group == group and commit.topic == topic:
                self.by_partition.setdefault(commit.partition, []).append(commit)
        for series in self.by_partition.values():
            series.sort(key=lambda c: c.at)

    def committed(self, partition: int, at: float | None = None) -> int:
        """The offset the group had committed for the partition at `at` (the latest when None); 0 if none."""
        best = 0
        for commit in self.by_partition.get(partition, ()):
            if at is not None and commit.at > at:
                break
            best = max(best, commit.offset)
        return best

    def committed_at(self, partition: int, offset: int) -> float | None:
        """When the group first committed past the record at `offset` of `partition`; None if it never did."""
        for commit in self.by_partition.get(partition, ()):
            if commit.offset > offset:
                return commit.at
        return None

    def lag(self, ends: list[int], at: float | None = None) -> int:
        """Records not yet committed past, summed over the partitions, against their end offsets."""
        return sum(max(end - self.committed(partition, at), 0) for partition, end in enumerate(ends))

    def last_commit_at(self) -> float | None:
        stamps = [series[-1].at for series in self.by_partition.values() if series]
        return max(stamps) if stamps else None


def source_committed_ms(timeline: Timeline, produced: Iterable[tuple[int, int, int, float]], *,
                        after: float = 0.0) -> list[float]:
    """Milliseconds from each record's produce time to the group's first commit past it.

    `produced` is the producer's log: (sequence, partition, offset, epoch seconds). Records produced before
    `after` (the warm-up) are left out; a record the group never committed past is left out too (the caller
    counts them against the produced total)."""
    out = []
    for _seq, partition, offset, at in produced:
        if at < after:
            continue
        committed = timeline.committed_at(partition, offset)
        if committed is not None:
            out.append(max(committed - at, 0.0) * 1000.0)
    return out


def read_produced(path: str) -> list[tuple[int, int, int, float]]:
    """The producer's `seq,partition,offset,epoch_seconds` log."""
    out = []
    try:
        with open(path, encoding="utf-8") as handle:
            for raw in handle:
                parts = raw.strip().split(",")
                if len(parts) == 4:
                    out.append((int(parts[0]), int(parts[1]), int(parts[2]), float(parts[3])))
    except FileNotFoundError:
        pass
    return out
