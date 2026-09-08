"""System-metrics sampler.

Samples CPU / memory (and disk / network when available) at a fixed interval
while a benchmark runs. Uses psutil when installed; otherwise falls back to the
`ps` utility for CPU%/RSS of the benchmark process tree (disk/net report 0).
"""
from __future__ import annotations

import os
import subprocess
import threading
from collections.abc import Callable
from datetime import datetime, timezone

try:  # optional dependency
    import psutil  # type: ignore
    _HAS_PSUTIL = True
except Exception:  # pragma: no cover
    psutil = None  # type: ignore
    _HAS_PSUTIL = False


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolved_exe_matches(pid: int, wanted: str) -> bool:
    """Resolve /proc/<pid>/exe and compare its basename — the ps fallback's
    equivalent of psutil's Process.exe(), so a symlink-named command still
    matches its resolved target (review). Silently false where /proc is
    absent (macOS) or the link is unreadable; the comm check already ran."""
    try:
        return os.path.basename(os.path.realpath(f"/proc/{pid}/exe")) == wanted
    except Exception:
        return False


def _proc_matches(p, wanted: str) -> bool:
    """True when a process is the engine under `wanted` (a RESOLVED
    basename): match either its command name or its resolved executable
    basename, since a VTOPCTL_BIN symlink and its target can differ, and
    either side reaching `wanted` is the engine (review)."""
    for getter in (lambda: p.name(), lambda: os.path.basename(p.exe())):
        try:
            if getter() == wanted:
                return True
        except Exception:
            continue
    return False


def _ps_tree_cpu_rss(root_pid: int, proc_name: str | None = None) -> tuple[float, float]:
    """Sum %cpu and RSS(MB) for `root_pid` and its descendants via `ps`.

    With `proc_name`, only descendants whose command basename matches are
    counted — the engine, not the harness sharing the tree (#476)."""
    try:
        out = subprocess.check_output(
            ["ps", "-Ao", "pid=,ppid=,%cpu=,rss=,comm="], text=True, timeout=5)
    except Exception:
        return 0.0, 0.0
    children: dict[int, list[int]] = {}
    stat: dict[int, tuple[float, float, str, int]] = {}
    for line in out.splitlines():
        parts = line.split(None, 4)
        if len(parts) < 5:
            continue
        try:
            pid, ppid, cpu, rss = int(parts[0]), int(parts[1]), float(parts[2]), float(parts[3])
        except ValueError:
            continue
        children.setdefault(ppid, []).append(pid)
        stat[pid] = (cpu, rss, os.path.basename(parts[4]), pid)
    seen: set[int] = set()
    stack = [root_pid]
    cpu_sum = rss_sum = 0.0
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        if pid in stat:
            cpu, rss, comm, ppid_pid = stat[pid]
            # Match the psutil path (review): the ps `comm` can be the
            # symlink name or truncated, so also resolve the executable
            # through /proc when the comm does not match, so a VTOPCTL_BIN
            # symlink whose target basename differs is still counted.
            if proc_name is None or comm == proc_name or _resolved_exe_matches(ppid_pid, proc_name):
                cpu_sum += cpu
                rss_sum += rss
        stack.extend(children.get(pid, []))
    return cpu_sum, rss_sum / 1024.0  # rss KB -> MB


class SystemMonitor:
    """Background sampler. `emit` receives a dict per sample."""

    def __init__(self, emit: Callable[[dict], None], interval: float = 1.0,
                 root_pid: int | None = None, container: str | None = None,
                 proc_name: str | None = None) -> None:
        self.emit = emit
        self.interval = max(0.1, float(interval))
        self.root_pid = root_pid or os.getpid()
        # When set, host-mode sampling counts only descendants whose
        # executable basename matches (#476, review): the engine, not the
        # generator or seeder that share the runner's tree. None keeps the
        # whole-tree behaviour every non-benchmark caller relies on.
        self.proc_name = proc_name
        # A containerized engine is no descendant of this process (#476,
        # review): sampling the runner's process tree there measures the
        # benchmark harness and a transient compose client, and every
        # cpu/memory conclusion drawn from it is about the wrong program.
        # When set, cpu/memory come from the container's own accounting via
        # docker stats; threads/open-files are unavailable through that
        # surface and report 0 rather than a number about the wrong process.
        self.container = container
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.samples: list[dict] = []
        self._base_disk = None
        self._base_net = None

    def _counters(self):
        disk_r = disk_w = net_tx = net_rx = 0.0
        if _HAS_PSUTIL:
            try:
                d = psutil.disk_io_counters()
                n = psutil.net_io_counters()
                if self._base_disk is None:
                    self._base_disk = (d.read_bytes, d.write_bytes)
                    self._base_net = (n.bytes_sent, n.bytes_recv)
                disk_r = (d.read_bytes - self._base_disk[0]) / 1e6
                disk_w = (d.write_bytes - self._base_disk[1]) / 1e6
                net_tx = (n.bytes_sent - self._base_net[0]) / 1e6
                net_rx = (n.bytes_recv - self._base_net[1]) / 1e6
            except Exception:
                pass
        return disk_r, disk_w, net_tx, net_rx

    def _container_cpu_mem(self) -> tuple[float, float]:
        try:
            out = subprocess.check_output(
                ["docker", "stats", "--no-stream", "--format",
                 "{{.CPUPerc}} {{.MemUsage}}", self.container],
                text=True, timeout=10).strip()
            cpu_part, mem_part = out.split()[0], out.split()[1]
            cpu = float(cpu_part.rstrip("%"))
            # LONGEST SUFFIX FIRST (review, two finders): every docker unit
            # ends in B, so checking "B" early matched "123.4MiB", raised
            # on float("123.4Mi"), and the broad handler zeroed every
            # container sample — the exact wrong-number this sampler was
            # added to stop producing.
            unit_scale = [("KIB", 1 / 1024), ("MIB", 1.0), ("GIB", 1024.0),
                          ("KB", 1 / 1000), ("MB", 1.0), ("GB", 1000.0),
                          ("B", 1 / 1e6)]
            for unit, scale in unit_scale:
                if mem_part.upper().endswith(unit):
                    return cpu, float(mem_part[: -len(unit)]) * scale
            return cpu, 0.0
        except Exception:
            return 0.0, 0.0

    def _sample(self) -> dict:
        if self.container is not None:
            cpu, rss = self._container_cpu_mem()
            threads, open_files = 0, 0
        elif _HAS_PSUTIL:
            try:
                proc = psutil.Process(self.root_pid)
                procs = [proc] + proc.children(recursive=True)
                if self.proc_name is not None:
                    procs = [p for p in procs if _proc_matches(p, self.proc_name)]
                cpu = 0.0
                rss = 0.0
                for p in procs:
                    try:
                        cpu += p.cpu_percent(interval=None)
                        rss += p.memory_info().rss / (1024 * 1024)
                    except Exception:
                        continue
                threads = sum((p.num_threads() for p in procs if p.is_running()), 0)
                open_files = 0
                for p in procs:
                    try:
                        open_files += len(p.open_files())
                    except Exception:
                        pass
            except Exception:
                cpu, rss, threads, open_files = 0.0, 0.0, 0, 0
        else:
            cpu, rss = _ps_tree_cpu_rss(self.root_pid, self.proc_name)
            threads, open_files = 0, 0
        disk_r, disk_w, net_tx, net_rx = self._counters()
        return {
            "timestamp": iso_now(),
            "cpu_percent": round(cpu, 2),
            "memory_mb": round(rss, 2),
            "disk_read_mb": round(disk_r, 3),
            "disk_write_mb": round(disk_w, 3),
            "network_tx_mb": round(net_tx, 3),
            "network_rx_mb": round(net_rx, 3),
            "open_files": open_files,
            "active_threads": threads,
            "queue_depth": 0,
        }

    def _run(self) -> None:
        if _HAS_PSUTIL:
            try:
                psutil.Process(self.root_pid).cpu_percent(interval=None)
            except Exception:
                pass
        while not self._stop.is_set():
            s = self._sample()
            self.samples.append(s)
            # Don't emit if we were asked to stop while sampling — avoids
            # writing to result files that __exit__'s caller may now close.
            if self._stop.is_set():
                break
            self.emit(s)
            self._stop.wait(self.interval)

    def __enter__(self) -> SystemMonitor:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        # Join WITHOUT a timeout so the sampler thread is guaranteed finished
        # before the caller closes the result files (no write-after-close). The
        # loop wakes every `interval` and each sample is bounded (psutil is fast;
        # the `ps` fallback has a 5s timeout), so this returns promptly.
        self._stop.set()
        if self._thread:
            self._thread.join()
