"""Metric 4 of the Kafka comparison (#596): what each node's container spent
on a run, from its own cgroup v2 counters.

A container sees its own cgroup at /sys/fs/cgroup, and the harness reads
the files there with `docker exec <node> cat`: once at the start of the
recorded window, once at its end, and `memory.stat` / `memory.current`
every second in between for the two peaks. The parsers here are pure, so
each is tested against fixture text, a multi-device io.stat included.

- cpu_seconds: the delta of `usage_usec` in cpu.stat.
- rss_peak_bytes: the largest `anon` in memory.stat over the window. Page
  cache is left out on purpose. mem_current_peak_bytes is the largest
  `memory.current`, page cache included.
- disk_write_bytes and disk_read_bytes: deltas of `wbytes` and `rbytes`,
  summed over the devices io.stat lists.

The window is closed the same way for both systems (`close_mode`): `sync`
before the first read and again before the last, then a 2 s settle, so
write-back a broker had not flushed (Kafka under `defaults`) is counted
rather than missed.
"""
from __future__ import annotations

import re
from typing import Callable, NamedTuple

CGROUP_ROOT = "/sys/fs/cgroup"
COUNTER_SOURCE = (f"{CGROUP_ROOT}/cpu.stat:usage_usec; {CGROUP_ROOT}/memory.stat:anon (1 s samples); "
                  f"{CGROUP_ROOT}/memory.current (1 s samples); {CGROUP_ROOT}/io.stat:rbytes,wbytes")
CLOSE_MODE = "sync+settle=2s"
SETTLE_SECONDS = 2.0


class CgroupRefused(Exception):
    """The counters cannot be read the way metric 4 needs; the message says why."""


def check_v2(controllers: str) -> None:
    """`cgroup.controllers` exists only on cgroup v2 and must list the
    controllers metric 4 reads. cgroup v1 is refused, not approximated."""
    present = set(controllers.split())
    missing = {"cpu", "memory", "io"} - present
    if missing:
        raise CgroupRefused(f"cgroup v2 with the cpu, memory and io controllers is required; this cgroup lists "
                            f"{sorted(present) or 'nothing'} (missing {sorted(missing)}). cgroup v1 hosts are refused: "
                            "its counters are not the ones metric 4 is defined on")


def _keyed(text: str, what: str) -> dict[str, int]:
    out = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        key, value = parts
        try:
            out[key] = int(value)
        except ValueError:
            raise CgroupRefused(f"{what}: {key} is {value!r}, not a whole number") from None
    return out


def cpu_usage_usec(cpu_stat: str) -> int:
    values = _keyed(cpu_stat, "cpu.stat")
    if "usage_usec" not in values:
        raise CgroupRefused("cpu.stat has no usage_usec")
    return values["usage_usec"]


def memory_anon(memory_stat: str) -> int:
    values = _keyed(memory_stat, "memory.stat")
    if "anon" not in values:
        raise CgroupRefused("memory.stat has no anon")
    return values["anon"]


def memory_current(text: str) -> int:
    try:
        return int(text.strip())
    except ValueError:
        raise CgroupRefused(f"memory.current is {text.strip()[:40]!r}, not a whole number") from None


class Io(NamedTuple):
    rbytes: int
    wbytes: int
    devices: tuple[str, ...]


_IO_LINE = re.compile(r"^(\d+:\d+)\s+(.*)$")


def io_bytes(io_stat: str) -> Io:
    """rbytes and wbytes summed over every device io.stat lists, and which
    devices those were (`major:minor`)."""
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
    return Io(rbytes, wbytes, tuple(sorted(devices)))


class Snapshot(NamedTuple):
    usage_usec: int
    io: Io


def snapshot(read: Callable[[str], str]) -> Snapshot:
    """One reading of the window's edge counters. `read(name)` returns the
    file's text from the node's cgroup directory."""
    return Snapshot(cpu_usage_usec(read("cpu.stat")), io_bytes(read("io.stat")))


class Window(NamedTuple):
    cpu_seconds: float
    rss_peak_bytes: int
    mem_current_peak_bytes: int
    disk_write_bytes: int
    disk_read_bytes: int
    devices: tuple[str, ...]


def window(start: Snapshot, end: Snapshot, anon_samples: list[int], current_samples: list[int]) -> Window:
    """The counters' deltas over a window, and the sampled peaks."""
    if end.usage_usec < start.usage_usec or end.io.wbytes < start.io.wbytes or end.io.rbytes < start.io.rbytes:
        raise CgroupRefused("a cgroup counter went backwards over the window: the container was restarted, so "
                            "the window measures two processes")
    if not anon_samples or not current_samples:
        raise CgroupRefused("no memory sample was taken inside the window")
    return Window(
        cpu_seconds=(end.usage_usec - start.usage_usec) / 1e6,
        rss_peak_bytes=max(anon_samples),
        mem_current_peak_bytes=max(current_samples),
        disk_write_bytes=end.io.wbytes - start.io.wbytes,
        disk_read_bytes=end.io.rbytes - start.io.rbytes,
        devices=tuple(sorted(set(start.io.devices) | set(end.io.devices))),
    )


class Closer(NamedTuple):
    """The I/O the harness does around a window, injected so its order can be
    tested: `sync(node)`, `sleep(seconds)`, `snap(node)`."""
    sync: Callable[[str], None]
    sleep: Callable[[float], None]
    snap: Callable[[str], Snapshot]


def open_window(nodes: list[str], io: Closer) -> dict[str, Snapshot]:
    """Flush every node, then read its counters: dirty pages from before the
    window are written before it starts and are not charged to it."""
    for node in nodes:
        io.sync(node)
    return {node: io.snap(node) for node in nodes}


def close_window(nodes: list[str], io: Closer) -> dict[str, Snapshot]:
    """Flush every node, let write-back settle, then read: what a node had
    acknowledged but not yet written is counted, for both systems alike."""
    for node in nodes:
        io.sync(node)
    io.sleep(SETTLE_SECONDS)
    return {node: io.snap(node) for node in nodes}
