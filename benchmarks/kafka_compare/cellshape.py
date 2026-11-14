"""The pieces of a cell's shape that need no Docker (#596): several producers
in one client container, and the smoke read-back over several partitions.

Kept apart from run.py so each rule is a pure function with a test: how N
producers' summaries combine into one row, how their output is split, and
what the smoke gate requires of a multi-partition, compressed read-back.
"""
from __future__ import annotations

import hashlib
import re
from typing import NamedTuple

from kafka_compare import perftool

# ----------------------------------------------------------------------------- producers

_MARK = "=== kc-producer {index} exit {code} ==="
_MARK_RE = re.compile(r"^=== kc-producer (\d+) exit (-?\d+) ===$", re.MULTILINE)


def multi_producer_script(commands: list[list[str]]) -> str:
    """A POSIX shell script that runs every producer command at once in one
    container and prints each one's output after a marker carrying its exit
    status. The image's shell is POSIX, so no bash features are used."""
    def quote(arg: str) -> str:
        return "'" + arg.replace("'", "'\\''") + "'"

    lines = ["set -u"]
    for index, command in enumerate(commands):
        lines.append(f"( {' '.join(quote(a) for a in command)} > /tmp/kc-p{index}.out 2>&1;"
                     f" echo $? > /tmp/kc-p{index}.code ) &")
    lines.append("wait")
    for index in range(len(commands)):
        lines.append(f"echo \"=== kc-producer {index} exit $(cat /tmp/kc-p{index}.code) ===\"")
        lines.append(f"cat /tmp/kc-p{index}.out")
    return "\n".join(lines) + "\n"


def split_outputs(output: str, count: int) -> list[tuple[int, str]]:
    """Each producer's (exit status, output), in order, from the script's output."""
    marks = list(_MARK_RE.finditer(output))
    if [int(m.group(1)) for m in marks] != list(range(count)):
        raise perftool.PerfParseError(f"expected {count} producer outputs, found markers "
                                      f"{[m.group(1) for m in marks]}")
    parts = []
    for i, mark in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(output)
        parts.append((int(mark.group(2)), output[mark.end():end]))
    return parts


class Combined(NamedTuple):
    result: perftool.ProducerResult
    lat_basis: str


def combine(results: list[perftool.ProducerResult]) -> Combined:
    """N producers as one row (#596 plan D11): records and throughput are
    sums; percentiles cannot be merged, so the row carries the WORST
    producer's, and says so (`worst-of-N`). Average latency is the
    record-weighted mean, which is exact."""
    if not results:
        raise ValueError("no producer results to combine")
    if len(results) == 1:
        return Combined(results[0], "single")
    records = sum(r.records for r in results)
    return Combined(perftool.ProducerResult(
        records=records,
        records_per_sec=sum(r.records_per_sec for r in results),
        mib_per_sec=sum(r.mib_per_sec for r in results),
        lat_avg_ms=sum(r.lat_avg_ms * r.records for r in results) / records if records else 0.0,
        lat_max_ms=max(r.lat_max_ms for r in results),
        lat_p50_ms=max(r.lat_p50_ms for r in results),
        lat_p95_ms=max(r.lat_p95_ms for r in results),
        lat_p99_ms=max(r.lat_p99_ms for r in results),
        lat_p999_ms=max(r.lat_p999_ms for r in results),
        steady_state=all(r.steady_state for r in results),
    ), f"worst-of-{len(results)}")


# ----------------------------------------------------------------------------- smoke


def smoke_records(count: int, size: int) -> list[tuple[str, str]]:
    """`count` distinct (key, value) pairs, each value exactly `size` bytes,
    reproducible. Keyed, so the client's partitioner spreads them over every
    partition of the cell."""
    out = []
    for index in range(count):
        key = f"k{index:08d}"
        head = f"smoke-{index:08d}-"
        body = hashlib.sha256(f"kc-smoke-{index}".encode()).hexdigest()
        while len(head) + len(body) < size:
            body += hashlib.sha256(body.encode()).hexdigest()
        out.append((key, (head + body)[:size]))
    return out


class SmokeRead(NamedTuple):
    partition: int
    create_time_ms: int
    key: str
    value: str


_READ = re.compile(r"^CreateTime:(\d+)\t(.*?)\t(.*)$")


def parse_smoke_read(partition: int, output: str) -> list[SmokeRead]:
    """kafka-console-consumer's lines with print.timestamp and print.key."""
    reads = []
    for line in output.split("\n"):
        if not line:
            continue
        match = _READ.match(line)
        if not match:
            raise RuntimeError(f"smoke: partition {partition} printed an unexpected line {line[:60]!r}")
        reads.append(SmokeRead(partition, int(match.group(1)), match.group(2), match.group(3)))
    return reads


def check_smoke(produced: list[tuple[str, str]], reads: list[SmokeRead], partitions: int,
                window_ms: tuple[int, int]) -> None:
    """The smoke gate (#596 plan 4.3): raise unless what was read back is
    exactly what was produced.

    - the union over partitions is exactly the produced set, each record once;
    - each partition holds its records in produced order (order is defined
      per partition only);
    - every partition was written when there is more than one, so the
      partitioner really spread the cell;
    - every CreateTime lies inside the smoke's own produce window, which
      catches a gateway that rewrites timestamps before end-to-end latency
      relies on them.
    """
    index = {key: (i, value) for i, (key, value) in enumerate(produced)}
    seen: dict[str, int] = {}
    for read in reads:
        if read.key not in index:
            raise RuntimeError(f"smoke: partition {read.partition} returned key {read.key[:40]!r}, never produced")
        if index[read.key][1] != read.value:
            raise RuntimeError(f"smoke: record {read.key} differs: read {read.value[:40]!r}...")
        seen[read.key] = seen.get(read.key, 0) + 1
    duplicated = sorted(k for k, n in seen.items() if n > 1)
    if duplicated:
        raise RuntimeError(f"smoke: {len(duplicated)} record(s) read more than once, first {duplicated[0]}")
    missing = len(produced) - len(seen)
    if missing:
        first = next(k for k, _ in produced if k not in seen)
        raise RuntimeError(f"smoke: {missing} of {len(produced)} records were not read back, first {first}")
    for p in range(partitions):
        order = [index[r.key][0] for r in reads if r.partition == p]
        if partitions > 1 and not order:
            raise RuntimeError(f"smoke: partition {p} holds none of the {len(produced)} records")
        if any(b <= a for a, b in zip(order, order[1:])):
            raise RuntimeError(f"smoke: partition {p} returned its records out of produced order")
    start, end = window_ms
    outside = [r for r in reads if not start <= r.create_time_ms <= end]
    if outside:
        r = outside[0]
        raise RuntimeError(f"smoke: {len(outside)} record(s) carry a CreateTime outside the produce window "
                           f"{start}..{end}, first {r.key} at {r.create_time_ms}: the timestamps were rewritten")
