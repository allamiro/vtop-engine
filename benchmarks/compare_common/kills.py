"""The seeded kill schedule, the same for every tool (#744 rule 17, section 8 test 13).

A crash cell kills the tool `kills_per_run` times per run, at points drawn from
a recorded seed. The draw depends only on the seed, the kill kind, the run
(repetition) and the kill's index, never on the tool, so every tool meets the
same schedule:

- `K1`: at the START of the Nth store request that is a PUT (an object or a
  part), counted since the tool's last (re)start; the request is never
  forwarded;
- `K2`: right AFTER the store's answer to the Nth PUT was delivered to the
  tool, before the tool's next request is forwarded;
- `K3`: at a random time, `at_seconds` after the tool's last (re)start;
- `K4` (spool kind): inside a rotation window the harness holds: the harness
  rotates the file at `at_seconds`, and the kill follows `window_ms` later.
"""
from __future__ import annotations

import random
from typing import NamedTuple

KINDS = ("K1", "K2", "K3", "K4")


class KillPoint(NamedTuple):
    kind: str
    run: int
    index: int
    n: int | None  # K1, K2: the Nth PUT since the last (re)start
    at_seconds: float | None  # K3: seconds after the last (re)start; K4: when the harness rotates
    window_ms: int | None  # K4: how long after the rotation the kill follows


def schedule(seed: int, kind: str, run: int, kills_per_run: int = 3, *, n_range: tuple[int, int] = (2, 12),
             seconds_range: tuple[float, float] = (5.0, 40.0), window_ms_range: tuple[int, int] = (0, 500)
             ) -> list[KillPoint]:
    if kind not in KINDS:
        raise ValueError(f"kill kind {kind!r} is not one of {KINDS}")
    if kills_per_run < 1:
        raise ValueError("a crash run has at least one kill")
    rng = random.Random(f"{seed}:{kind}:{run}")
    out = []
    for index in range(kills_per_run):
        n = at = window = None
        if kind in ("K1", "K2"):
            n = rng.randint(*n_range)
        elif kind == "K3":
            at = round(rng.uniform(*seconds_range), 3)
        else:
            at = round(rng.uniform(*seconds_range), 3)
            window = rng.randint(*window_ms_range)
        out.append(KillPoint(kind, run, index, n, at, window))
    return out


KILLS_COLUMNS = ("run_id", "system", "profile", "cell", "repetition", "index", "kind", "trigger", "n",
                 "at_seconds", "window_ms", "killed_at", "kill_delay_ms", "restarted_at",
                 "seconds_to_first_object_after_restart", "last_log_line")
