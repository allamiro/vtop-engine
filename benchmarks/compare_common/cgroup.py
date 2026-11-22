"""CPU-seconds and peak RSS of one tool container over a window (#744 section 1, ranked metrics).

The tool container's own cgroup counters, read through the Docker Engine API
(`/containers/<id>/stats`): `cpu_stats.cpu_usage.total_usage` is cumulative
CPU time in nanoseconds, so CPU-seconds over a window is the difference of two
samples; memory is sampled every interval and the window's peak RSS is the
largest anonymous-memory sample (cgroup v2 `anon`, v1 `rss`; page cache is
excluded, since it is the kernel's, not the tool's). The peak is therefore a
SAMPLED peak, at `interval` resolution, and the CSV says so in its column name
(`peak_rss_sampled_bytes`).

The parsing is separate from the sampling so it is tested on recorded API
answers.
"""
from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any, NamedTuple


class Sample(NamedTuple):
    at: float
    cpu_ns: int
    rss_bytes: int


def parse_stats(answer: dict[str, Any], at: float) -> Sample:
    cpu = int(((answer.get("cpu_stats") or {}).get("cpu_usage") or {}).get("total_usage") or 0)
    memory = answer.get("memory_stats") or {}
    stats = memory.get("stats") or {}
    if "anon" in stats:
        rss = int(stats["anon"])
    elif "rss" in stats:
        rss = int(stats["rss"])
    else:
        rss = int(memory.get("usage") or 0) - int(stats.get("inactive_file") or 0)
    return Sample(at, cpu, max(rss, 0))


class Window(NamedTuple):
    cpu_seconds: float
    peak_rss_sampled_bytes: int
    samples: int


def window(samples: list[Sample], start: float, end: float) -> Window:
    """CPU-seconds and sampled peak RSS between start and end (the samples nearest inside them).

    Refuses fewer than two samples inside the window, and a counter that went
    backwards (the container restarted inside the window: its cgroup is new,
    so the difference would be meaningless; a crash cell sums per life)."""
    inside = sorted((s for s in samples if start <= s.at <= end), key=lambda s: s.at)
    if len(inside) < 2:
        raise ValueError(f"{len(inside)} cgroup samples inside the window; at least 2 are needed")
    for before, after in zip(inside, inside[1:]):
        if after.cpu_ns < before.cpu_ns:
            raise ValueError("the CPU counter went backwards inside the window (the container restarted)")
    return Window((inside[-1].cpu_ns - inside[0].cpu_ns) / 1e9, max(s.rss_bytes for s in inside), len(inside))


class Sampler:
    """Samples a container every `interval` seconds on a thread until stopped."""

    def __init__(self, container: str, fetch: Callable[[str], dict[str, Any]], interval: float = 0.5,
                 clock: Callable[[], float] = time.time):
        self.container, self.fetch, self.interval, self.clock = container, fetch, interval, clock
        self.samples: list[Sample] = []
        self.errors: list[str] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.samples.append(parse_stats(self.fetch(self.container), self.clock()))
            except Exception as exc:  # noqa: BLE001 - a failed sample is recorded, not fatal
                self.errors.append(str(exc))
            self._stop.wait(self.interval)

    def __enter__(self) -> Sampler:
        self._thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
