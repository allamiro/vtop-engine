"""The seeded Kafka producer (#746 "Dataset"): JSON records with a unique id, written as `key|value` lines.

Every record is a pure function of (seed, run tag, sequence, size), so the same seed gives the same bytes on
every host. The value is one compact JSON object of EXACTLY `size` bytes:

    {"id":"vtb-<run tag>-<sequence>","p":<partition>,"s":<sequence>,"m":"<filler>"}

`id` is the token `compare_common.checker` finds in whatever a tool writes; `p` is the partition the record's key
hashes to, so a record is known by (partition, sequence). The ids, not Kafka offsets, are the ground truth:
Vector does not preserve offsets and the checker never needs them.

The key is `k<sequence>`; the broker's default partitioner (murmur2 of the key, mod the partition count)
puts the record on partition `partition_of(seq)`, and, with one producer writing in order, at the offset
`offset_of` counts. `run.py` checks the partition's end offsets against `counts` before it starts a tool, so a
partitioner that disagreed would be caught, not assumed.

In a run the producer is a command in the lab; its stdout is piped into `kafka-console-producer.sh` (acks=all)
in the source's container:

    python -m archive_compare.producer backlog --count N [--start S] --size 1024 --seed 746 --tag abcd1234
    python -m archive_compare.producer live --rate R --seconds T --size 1024 --seed 746 --tag abcd1234 \\
        --log /logs/produced.csv

`live` writes at `rate` records/s open loop (a record is written when it is due, in bursts of 10 ms) and logs
`seq,partition,offset,epoch_seconds` for every record, on the lab's clock, which is the store's clock.
"""
from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable, Iterator
from typing import IO

PARTITIONS = 3
KEY_SEPARATOR = "|"
WORDS = ("auth", "session", "opened", "closed", "user", "alice", "bob", "carol", "denied", "granted", "route",
         "edge", "core", "latency", "bytes", "packets", "dropped", "allowed", "policy", "rule", "tcp", "udp",
         "flow", "src", "dst", "port", "kernel", "audit", "login", "token", "refresh", "expired", "cache",
         "miss", "hit", "queue", "depth", "retry", "backoff", "timeout", "status", "ok", "error", "warn")
MASK32 = 0xFFFFFFFF


def make_id(tag: str, seq: int) -> str:
    # The token compare_common.checker.make_id writes (and its regex reads), repeated so that the producer runs in
    # the lab's python image from this package alone.
    return f"vtb-{tag}-{seq:010d}"


def murmur2(data: bytes) -> int:
    """Kafka's `Utils.murmur2` (the default partitioner's hash), as an unsigned 32-bit value."""
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    h = (seed ^ length) & MASK32
    for i in range(length // 4):
        i4 = i * 4
        k = (data[i4] | data[i4 + 1] << 8 | data[i4 + 2] << 16 | data[i4 + 3] << 24) & MASK32
        k = (k * m) & MASK32
        k ^= k >> 24
        k = (k * m) & MASK32
        h = ((h * m) & MASK32) ^ k
    tail = length & ~3
    rest = length % 4
    if rest == 3:
        h ^= data[tail + 2] << 16
    if rest >= 2:
        h ^= data[tail + 1] << 8
    if rest >= 1:
        h ^= data[tail]
        h = (h * m) & MASK32
    h ^= h >> 13
    h = (h * m) & MASK32
    h ^= h >> 15
    return h & MASK32


def key_of(seq: int) -> str:
    return f"k{seq:010d}"


def partition_of(seq: int, partitions: int = PARTITIONS) -> int:
    """The partition the broker's default partitioner picks for this record's key."""
    return (murmur2(key_of(seq).encode()) & 0x7FFFFFFF) % partitions


def record(seed: int, tag: str, seq: int, size: int, partitions: int = PARTITIONS) -> bytes:
    """The record's value: compact JSON of exactly `size` bytes, a pure function of its arguments."""
    import random

    head = f'{{"id":"{make_id(tag, seq)}","p":{partition_of(seq, partitions)},"s":{seq},"m":"'
    tail = '"}'
    room = size - len(head) - len(tail)
    if room < 0:
        raise ValueError(f"a {size}-byte record cannot hold the {len(head) + len(tail)}-byte frame")
    rng = random.Random(f"{seed}:{seq}")
    filler = ""
    while len(filler) < room:
        filler += " ".join(rng.choices(WORDS, k=24)) + " "
    return (head + filler[:room] + tail).encode()


def line(seed: int, tag: str, seq: int, size: int, partitions: int = PARTITIONS) -> bytes:
    """`key|value\\n`, the console producer's input."""
    return key_of(seq).encode() + KEY_SEPARATOR.encode() + record(seed, tag, seq, size, partitions) + b"\n"


def counts(start: int, count: int, partitions: int = PARTITIONS) -> list[int]:
    """How many of the records start .. start+count-1 land on each partition."""
    out = [0] * partitions
    for seq in range(start, start + count):
        out[partition_of(seq, partitions)] += 1
    return out


def layout(count: int, partitions: int = PARTITIONS) -> list[tuple[int, int]]:
    """(partition, offset) of each record 0..count-1: the producer is the topic's only writer and writes in order,
    so a record's offset is the number of earlier records on its partition."""
    seen = [0] * partitions
    out = []
    for seq in range(count):
        part = partition_of(seq, partitions)
        out.append((part, seen[part]))
        seen[part] += 1
    return out


def backlog(out: IO[bytes], seed: int, tag: str, start: int, count: int, size: int,
            partitions: int = PARTITIONS) -> int:
    """Write `count` records from sequence `start`; returns the bytes of the values (the source bytes)."""
    written = 0
    for seq in range(start, start + count):
        data = line(seed, tag, seq, size, partitions)
        out.write(data)
        written += len(data) - len(key_of(seq)) - len(KEY_SEPARATOR)
    out.flush()
    return written


def live(out: IO[bytes], log: IO[str], *, seed: int, tag: str, start: int, rate: float, seconds: float,
         size: int, partitions: int = PARTITIONS, base_offsets: list[int] | None = None,
         clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep) -> int:
    """Write at `rate` records/s for `seconds`, open loop. Logs `seq,partition,offset,epoch_seconds` per record.

    `base_offsets` is each partition's end offset when the run starts (0 for a fresh topic): a record's offset
    is its partition's count so far, since this producer is the topic's only writer. Returns the next sequence."""
    offsets = list(base_offsets or [0] * partitions)
    began = clock()
    seq = start
    while True:
        elapsed = clock() - began
        if elapsed >= seconds:
            break
        due = int(elapsed * rate) - (seq - start)
        if due > 0:
            for _ in range(due):
                out.write(line(seed, tag, seq, size, partitions))
                part = partition_of(seq, partitions)
                log.write(f"{seq},{part},{offsets[part]},{clock():.6f}\n")
                offsets[part] += 1
                seq += 1
            out.flush()
            log.flush()
        sleep(0.01)
    return seq


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("backlog", "live", "sample"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--seed", type=int, default=746)
        cmd.add_argument("--tag", default="abcd1234")
        cmd.add_argument("--size", type=int, default=1024)
        cmd.add_argument("--start", type=int, default=0)
        cmd.add_argument("--partitions", type=int, default=PARTITIONS)
        if name == "backlog":
            cmd.add_argument("--count", type=int, required=True)
        elif name == "sample":
            cmd.add_argument("--count", type=int, default=3)
        else:
            cmd.add_argument("--rate", type=float, required=True)
            cmd.add_argument("--seconds", type=float, required=True)
            cmd.add_argument("--log", required=True)
            cmd.add_argument("--base-offsets", default="", help="comma-separated end offsets, one per partition")
    args = parser.parse_args(argv)
    out = sys.stdout.buffer
    if args.command == "sample":
        for seq in range(args.start, args.start + args.count):
            sys.stdout.write(line(args.seed, args.tag, seq, args.size, args.partitions).decode())
        return 0
    if args.command == "backlog":
        backlog(out, args.seed, args.tag, args.start, args.count, args.size, args.partitions)
        return 0
    base = [int(x) for x in args.base_offsets.split(",") if x] or None
    with open(args.log, "a", encoding="utf-8") as log:
        live(out, log, seed=args.seed, tag=args.tag, start=args.start, rate=args.rate, seconds=args.seconds,
             size=args.size, partitions=args.partitions, base_offsets=base)
    return 0


def records(seed: int, tag: str, start: int, count: int, size: int, partitions: int = PARTITIONS) -> Iterator[bytes]:
    for seq in range(start, start + count):
        yield record(seed, tag, seq, size, partitions)


if __name__ == "__main__":
    sys.exit(main())
