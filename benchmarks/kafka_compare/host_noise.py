"""What else the benchmark host was doing during a run (#597 re-run, as #483 did).

A failover run shares the Docker host with whatever else runs there: other
projects' containers, other sessions' cargo builds in Docker, a Lima VM.
None of it is stopped; it is SAMPLED, every `interval` seconds, into
`host_noise.csv` in the run directory, and summarised per repetition and for
the whole run in environment.json, so a reader can see how noisy each
repetition's host was instead of being told it was quiet.

Columns: the host's 1/5/15-minute load average, its CPU idle percentage
(macOS `top`), the Docker containers running (all, this benchmark's `kc-*`,
`rust:*` build containers), and the Lima VMs running (by name).
"""
from __future__ import annotations

import csv
import datetime as dt
import os
import re
import shutil
import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from typing import Any

COLUMNS = ("timestamp_utc", "t_unix", "load1", "load5", "load15", "cpu_idle_pct", "docker_running",
           "docker_benchmark", "docker_rust_builds", "lima_running")


def _run(args: Sequence[str]) -> str:
    try:
        return subprocess.run(list(args), capture_output=True, text=True, timeout=20).stdout
    except (OSError, subprocess.SubprocessError):
        return ""


def _cpu_idle() -> float | None:
    match = re.search(r"([0-9.]+)% idle", _run(["top", "-l", "1", "-n", "0"]))
    return float(match.group(1)) if match else None


def _containers() -> tuple[int, int, int]:
    lines = [line.split("\t") for line in _run(["docker", "ps", "--format", "{{.Names}}\t{{.Image}}"]).splitlines()
             if line.strip()]
    benchmark = sum(1 for parts in lines if parts[0].startswith("kc-"))
    rust = sum(1 for parts in lines if len(parts) > 1 and parts[1].startswith("rust:"))
    return len(lines), benchmark, rust


def _lima_running() -> str:
    if not shutil.which("limactl"):
        return ""
    names = [line.split()[0] for line in _run(["limactl", "list", "--format", "{{.Name}} {{.Status}}"]).splitlines()
             if line.strip().endswith("Running")]
    return ";".join(sorted(names))


def sample() -> dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc)
    load1, load5, load15 = os.getloadavg()
    running, benchmark, rust = _containers()
    return {"timestamp_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "t_unix": round(now.timestamp(), 1),
            "load1": round(load1, 2), "load5": round(load5, 2), "load15": round(load15, 2),
            "cpu_idle_pct": _cpu_idle(), "docker_running": running, "docker_benchmark": benchmark,
            "docker_rust_builds": rust, "lima_running": _lima_running()}


class Sampler:
    """Samples every `interval` seconds into `path` until `stop()`; the first sample at once.

    Samples are scheduled from a monotonic deadline (start + k * interval), so the
    cadence is `interval` and not `interval` plus the probe's own time. A probe that
    raises is recorded in `errors` (time and exception) and sampling carries on: one
    failed `docker ps` must not end the record, nor leave a gap no one is told about.
    """

    def __init__(self, path: str, interval: float = 30.0, probe: Callable[[], dict[str, Any]] = sample):
        self.path, self.interval, self.probe = path, interval, probe
        self.errors: list[str] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        with open(path, "w", newline="") as handle:
            csv.writer(handle).writerow(COLUMNS)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=60)

    def _loop(self) -> None:
        origin = time.monotonic()
        k = 0
        while True:
            try:
                row = self.probe()
                with open(self.path, "a", newline="") as handle:
                    csv.writer(handle).writerow(["" if row.get(c) is None else row[c] for c in COLUMNS])
            except Exception as exc:  # recorded, and the next sample still runs
                stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                self.errors.append(f"{stamp} {type(exc).__name__}: {exc}"[:300])
            k += 1
            # The next deadline still in the future: a probe slower than the interval
            # skips the slots it overran rather than firing them back to back.
            next_at = origin + k * self.interval
            now = time.monotonic()
            if next_at < now:
                k = int((now - origin) // self.interval) + 1
                next_at = origin + k * self.interval
            if self._stop.wait(next_at - now):
                return


def read(path: str) -> list[dict[str, str]]:
    with open(path, newline="") as handle:
        return list(csv.DictReader(handle))


def summarize(rows: list[dict[str, str]]) -> dict[str, Any]:
    """Mean and max load, mean and min CPU idle, max containers, and every Lima VM seen."""
    if not rows:
        return {"samples": 0}
    loads = [float(r["load1"]) for r in rows]
    idle = [float(r["cpu_idle_pct"]) for r in rows if r["cpu_idle_pct"]]
    lima = sorted({name for r in rows for name in r["lima_running"].split(";") if name})
    return {
        "samples": len(rows),
        "load1_mean": round(sum(loads) / len(loads), 2), "load1_max": max(loads),
        "cpu_idle_pct_mean": round(sum(idle) / len(idle), 1) if idle else None,
        "cpu_idle_pct_min": min(idle) if idle else None,
        "docker_running_max": max(int(r["docker_running"]) for r in rows),
        "docker_other_max": max(int(r["docker_running"]) - int(r["docker_benchmark"]) for r in rows),
        "docker_rust_builds_max": max(int(r["docker_rust_builds"]) for r in rows),
        "lima_running_seen": lima,
    }


def window(rows: list[dict[str, str]], start: float, end: float) -> list[dict[str, str]]:
    """The samples taken in [start, end] (unix seconds)."""
    return [r for r in rows if start <= float(r["t_unix"]) <= end]
