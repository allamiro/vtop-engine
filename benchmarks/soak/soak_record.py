"""What a soak run records, and the state its threads share (#726).

Two JSONL files and one lock. `samples.jsonl` and `events.jsonl` are appended
as they happen and flushed on every line, so a driver that dies mid-run
leaves a record the evaluator can still read up to that point. Times are
seconds on the driver's own monotonic clock since the load started: the
engine's clock is the thing the clock-step fault moves, so it is never what a
record is timed by.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


class Recorder:
    def __init__(self, run_dir: Path, clock: Callable[[], float] = time.monotonic):
        self.run_dir = run_dir
        self._clock = clock
        self._origin = clock()
        self._lock = threading.Lock()
        self._samples = open(run_dir / "samples.jsonl", "a", buffering=1)
        self._events = open(run_dir / "events.jsonl", "a", buffering=1)

    def elapsed(self) -> float:
        return round(self._clock() - self._origin, 3)

    def restart_origin(self) -> None:
        """Time zero: the load has started."""
        self._origin = self._clock()

    @staticmethod
    def _stamp() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def sample(self, **fields) -> dict:
        row = {"elapsed_s": self.elapsed(), "ts": self._stamp(), **fields}
        with self._lock:
            self._samples.write(json.dumps(row, sort_keys=True) + "\n")
        return row

    def event(self, kind: str, **fields) -> dict:
        row = {"elapsed_s": self.elapsed(), "ts": self._stamp(), "kind": kind, **fields}
        with self._lock:
            self._events.write(json.dumps(row, sort_keys=True) + "\n")
        return row

    def close(self) -> None:
        with self._lock:
            self._samples.close()
            self._events.close()


class RunState:
    """What the sampler, the scheduler and the injectors agree on: which engine
    process is running (`lifetime`), whether it is down on purpose, and whether
    the run has been told to stop."""

    def __init__(self):
        self._lock = threading.Lock()
        self.lifetime = 0
        self.down_planned = False
        self.shutting_down = False
        self.abort: str | None = None
        self.unplanned_exits = 0

    def engine_started(self) -> int:
        """A new engine process is about to start: samples from here on belong
        to a new lifetime, so no trend is fitted across the restart."""
        with self._lock:
            self.lifetime += 1
            return self.lifetime

    def current_lifetime(self) -> int:
        with self._lock:
            return self.lifetime

    @contextmanager
    def planned_down(self):
        """The engine is about to be stopped by a fault: its exit is not a
        finding for as long as the block runs."""
        with self._lock:
            self.down_planned = True
        try:
            yield
        finally:
            with self._lock:
                self.down_planned = False

    def begin_shutdown(self) -> None:
        """The run is over and the engine is about to be stopped for good: its
        absence from here on is not a finding."""
        with self._lock:
            self.shutting_down = True

    def is_down_planned(self) -> bool:
        with self._lock:
            return self.down_planned or self.shutting_down

    def request_abort(self, reason: str) -> None:
        with self._lock:
            if self.abort is None:
                self.abort = reason

    def aborted(self) -> str | None:
        with self._lock:
            return self.abort
