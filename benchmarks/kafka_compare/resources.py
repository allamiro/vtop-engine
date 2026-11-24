"""Metric 4 of the Kafka comparison (#596, #600): what each system's containers spent on a run, from their
own cgroup v2 counters.

A container sees its own cgroup at /sys/fs/cgroup, and the harness reads the files there with
`docker exec <node> cat`: once before a measured run and once after it, for each of the system's three nodes.
The parsers here are pure, so each is tested against fixture text, a multi-device io.stat included.

- cpu_seconds: the delta of `usage_usec` in cpu.stat (user and system time of every process in the container).
- disk_write_bytes and disk_read_bytes: the deltas of `wbytes` and `rbytes` in io.stat, summed over the devices
  it lists. These are the bytes the container's processes caused to be written to and read from block
  devices, which is what "bytes written per byte delivered" means: replication and log overhead included.

The window is opened and closed the same way for both systems: `sync` on every node before each read, and a
2 s settle before the closing one, so write-back that a broker had not flushed yet (Kafka under `defaults`
acknowledges without an fsync) is counted in the run that caused it rather than in the next.
"""
from __future__ import annotations

import re
from typing import Callable, NamedTuple

CGROUP_ROOT = "/sys/fs/cgroup"
COUNTER_SOURCE = f"{CGROUP_ROOT}/cpu.stat:usage_usec; {CGROUP_ROOT}/io.stat:rbytes,wbytes"
CLOSE_MODE = "sync+settle=2s"
SETTLE_SECONDS = 2.0


class CgroupRefused(Exception):
    """The counters cannot be read the way metric 4 needs; the message says why."""


def check_v2(controllers: str) -> None:
    """`cgroup.controllers` exists only on cgroup v2 and must list the controllers metric 4 reads. cgroup v1 is
    refused, not approximated: its counters are not the ones metric 4 is defined on."""
    present = set(controllers.split())
    missing = {"cpu", "io"} - present
    if missing:
        raise CgroupRefused(f"cgroup v2 with the cpu and io controllers is required; this cgroup lists "
                            f"{sorted(present) or 'nothing'} (missing {sorted(missing)}). cgroup v1 hosts are "
                            "refused: its counters are not the ones metric 4 is defined on")


def cpu_usage_usec(cpu_stat: str) -> int:
    for line in cpu_stat.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == "usage_usec":
            try:
                return int(parts[1])
            except ValueError:
                raise CgroupRefused(f"cpu.stat: usage_usec is {parts[1]!r}, not a whole number") from None
    raise CgroupRefused("cpu.stat has no usage_usec")


class Io(NamedTuple):
    rbytes: int
    wbytes: int
    devices: tuple[str, ...]


_IO_LINE = re.compile(r"^(\d+:\d+)\s+(.*)$")


def io_bytes(io_stat: str) -> Io:
    """rbytes and wbytes summed over every device io.stat lists, and which devices those were (`major:minor`).
    A cgroup whose io.stat lists none is refused: zero bytes written would be read as a measurement."""
    rbytes = wbytes = 0
    devices = []
    for line in io_stat.splitlines():
        match = _IO_LINE.match(line.strip())
        if not match:
            continue
        fields = dict(field.split("=", 1) for field in match.group(2).split() if "=" in field)
        try:
            rbytes += int(fields.get("rbytes", "0"))
            wbytes += int(fields.get("wbytes", "0"))
        except ValueError:
            raise CgroupRefused(f"io.stat line {line.strip()[:60]!r} holds a byte count that is not a number") \
                from None
        devices.append(match.group(1))
    if not devices:
        raise CgroupRefused("io.stat lists no device: the cgroup has done no block I/O yet, or the io controller "
                            "is not accounting this container, so a written-bytes figure would be a guess")
    return Io(rbytes, wbytes, tuple(sorted(devices)))


class Snapshot(NamedTuple):
    usage_usec: int
    io: Io


def snapshot(read: Callable[[str], str]) -> Snapshot:
    """One reading of a window's edge counters. `read(name)` returns the file's text from the node's cgroup."""
    return Snapshot(cpu_usage_usec(read("cpu.stat")), io_bytes(read("io.stat")))


class Window(NamedTuple):
    cpu_seconds: float
    disk_write_bytes: int
    disk_read_bytes: int
    devices: tuple[str, ...]


def window(start: Snapshot, end: Snapshot) -> Window:
    """The counters' deltas over a window."""
    if end.usage_usec < start.usage_usec or end.io.wbytes < start.io.wbytes or end.io.rbytes < start.io.rbytes:
        raise CgroupRefused("a cgroup counter went backwards over the window: the container was restarted, so "
                            "the window measures two processes")
    return Window(
        cpu_seconds=(end.usage_usec - start.usage_usec) / 1e6,
        disk_write_bytes=end.io.wbytes - start.io.wbytes,
        disk_read_bytes=end.io.rbytes - start.io.rbytes,
        devices=tuple(sorted(set(start.io.devices) | set(end.io.devices))),
    )


def total(per_node: dict[str, Window]) -> Window:
    """A system's window: its nodes' summed."""
    if not per_node:
        raise CgroupRefused("no node window to total")
    return Window(
        cpu_seconds=sum(w.cpu_seconds for w in per_node.values()),
        disk_write_bytes=sum(w.disk_write_bytes for w in per_node.values()),
        disk_read_bytes=sum(w.disk_read_bytes for w in per_node.values()),
        devices=tuple(sorted({d for w in per_node.values() for d in w.devices})),
    )


class Closer(NamedTuple):
    """The I/O the harness does around a window, injected so its order can be tested: `sync(node)`,
    `sleep(seconds)`, `snap(node)`."""
    sync: Callable[[str], None]
    sleep: Callable[[float], None]
    snap: Callable[[str], Snapshot]


def open_window(nodes: list[str], io: Closer) -> dict[str, Snapshot]:
    """Flush every node, then read its counters: dirty pages from before the window are written before it
    starts and are not charged to it."""
    for node in nodes:
        io.sync(node)
    return {node: io.snap(node) for node in nodes}


def close_window(nodes: list[str], io: Closer) -> dict[str, Snapshot]:
    """Flush every node, let write-back settle, then read: what a node had acknowledged but not yet written is
    counted, for both systems alike."""
    for node in nodes:
        io.sync(node)
    io.sleep(SETTLE_SECONDS)
    return {node: io.snap(node) for node in nodes}
