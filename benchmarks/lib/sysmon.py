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


def _ps_tree_cpu_rss(root_pid: int) -> tuple[float, float]:
    """Sum %cpu and RSS(MB) for `root_pid` and its descendants via `ps`."""
    try:
        out = subprocess.check_output(
            ["ps", "-Ao", "pid=,ppid=,%cpu=,rss="], text=True, timeout=5)
    except Exception:
        return 0.0, 0.0
    children: dict[int, list[int]] = {}
    stat: dict[int, tuple[float, float]] = {}
    for line in out.splitlines():
        parts = line.split(None, 3)
        if len(parts) < 4:
            continue
        try:
            pid, ppid, cpu, rss = int(parts[0]), int(parts[1]), float(parts[2]), float(parts[3])
        except ValueError:
            continue
        children.setdefault(ppid, []).append(pid)
        stat[pid] = (cpu, rss)
    seen: set[int] = set()
    stack = [root_pid]
    cpu_sum = rss_sum = 0.0
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        if pid in stat:
            cpu_sum += stat[pid][0]
            rss_sum += stat[pid][1]
        stack.extend(children.get(pid, []))
    return cpu_sum, rss_sum / 1024.0  # rss KB -> MB


class SystemMonitor:
    """Background sampler. `emit` receives a dict per sample."""

    def __init__(self, emit: Callable[[dict], None], interval: float = 1.0,
                 root_pid: int | None = None) -> None:
        self.emit = emit
        self.interval = max(0.1, float(interval))
        self.root_pid = root_pid or os.getpid()
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

    def _sample(self) -> dict:
        if _HAS_PSUTIL:
            try:
                proc = psutil.Process(self.root_pid)
                procs = [proc] + proc.children(recursive=True)
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
            cpu, rss = _ps_tree_cpu_rss(self.root_pid)
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
