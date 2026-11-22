"""The `verify-fault` cell's accounting (#746, #744 use case (e)).

The tap flips one bit in the body of a read-back (a GET of an object) for one object in `one_in`, chosen by a
seeded hash of the object's path (`compare_common.tap.fault_selected`), so every tool's corrupted set is decided by
the same rule. Nothing in the bucket is corrupted: only the tool's view of it is. Four numbers per run:

- `corrupted_objects`: the data objects the rule selects, whether or not a tool ever reads them;
- `corrupted_read_back`: of those, the ones the tool read back, so the tap flipped a bit in what it saw;
- `corrupted_noticed`: of those, the ones the tool reacted to before its source offset passed them: it wrote the
  object (or its records) again after the corrupt read;
- `corrupted_committed_past`: selected objects whose records the group committed past with no reaction. A tool that
  never reads an object back cannot notice it, and every selected object it committed counts here: that is the
  measured result, not a ranking (a design difference, #746).
"""
from __future__ import annotations

from collections.abc import Iterable
from typing import NamedTuple

from compare_common import tap


class Outcome(NamedTuple):
    corrupted_objects: int
    corrupted_read_back: int
    corrupted_noticed: int
    corrupted_committed_past: int

    def columns(self) -> dict[str, int]:
        return self._asdict()


def selected_keys(keys: Iterable[str], bucket: str, seed: int, one_in: int) -> set[str]:
    """The bucket keys the tap's rule selects. The tap hashes the request path, `/<bucket>/<key>`."""
    return {key for key in keys if tap.fault_selected(seed, f"/{bucket}/{key}", one_in)}


def flipped_keys(tap_corrupted: Iterable[str], bucket: str) -> set[str]:
    """The bucket keys of the paths the tap actually flipped (`Tap.corrupted`)."""
    prefix = f"/{bucket}/"
    return {path[len(prefix):] for path in tap_corrupted if path.startswith(prefix)}


def put_after_flip(trace: Iterable[dict[str, str]], bucket: str) -> set[str]:
    """Bucket keys that a PUT or DELETE followed, in the tap's trace, after the flipped read of the same key."""
    prefix = f"/{bucket}/"
    flipped_at: dict[str, float] = {}
    out: set[str] = set()
    for row in sorted(trace, key=lambda r: float(r.get("t_start") or 0)):
        path = row.get("path", "")
        if not path.startswith(prefix):
            continue
        key, at = path[len(prefix):], float(row.get("t_start") or 0)
        if row.get("flipped") == "1":
            flipped_at.setdefault(key, at)
        elif row.get("method") in ("PUT", "DELETE") and key in flipped_at and at > flipped_at[key]:
            out.add(key)
    return out


def classify(selected: set[str], flipped: set[str], reacted: set[str], committed: set[str]) -> Outcome:
    """The four counts. `reacted` are keys the tool wrote or deleted again, or whose records another object holds,
    after the flipped read; `committed` are keys whose every record the group committed past."""
    read_back = selected & flipped
    noticed = read_back & reacted
    past = (selected & committed) - noticed
    return Outcome(len(selected), len(read_back), len(noticed), len(past))
