"""Where one committed run's generated page and images go (#600, #744).

Every generator writes to fixed paths: one page and one image directory per kind
(`docs/benchmarks/kafka-comparison.md` and `docs/benchmarks/img/` for the Kafka
throughput run, `docs/benchmarks/archive-comparison.md` and `docs/benchmarks/archive/img/`
for the archive comparison, and so on). `regenerate.py` refuses two committed runs that
write the same file, so a second run of a kind (a new run beside the one it must not
replace, or the two halves of a split plan) had no place to go.

A run directory may hold a `report.json` that says where ITS page and images go:

    {"doc": "docs/benchmarks/kafka-comparison-aws-v0.11.0.md",
     "img_dir": "docs/benchmarks/img/aws-v0.11.0"}

The file is part of the run, committed with it, and the drift check covers it like any
other file of the run. A run without one writes the kind's default paths, exactly as
before, so no committed output changes. `--doc` and `--img-dir` on a report's command
line still win over the file.

A crash run's `report.json` may also name `latency_run`: the id of a throughput run in the same results
directory whose end-to-end latencies are drawn beside the crash results (#600). Only the Kafka report
asks for it (`allow_latency_run=True`); every other kind's file is exactly `doc` and `img_dir`.

A Kafka comparison run's `report.json` may say which earlier run it replaces (`allow_supersedes=True`):

    {"supersedes": "20261109T232328Z"}

The named run must be a committed run of the same kind (throughput, failover or crash: `run_kind`) in the
same results directory, started before this one (by `environment.json`'s `started_at`: run ids are free text,
so their order says nothing), and the chain may not loop. `regenerate.py` then hands only the current runs
to the report: a superseded run's raw data stays committed, and nothing is drawn from it, so the newer run
can write the pages and images the older one did. With `supersedes`, `doc` and `img_dir` are optional (both
or neither): left out, the run writes its kind's default paths, which is how a newer run takes over the
kind's page. `readme_charts.py` draws the one current run of each kind it charts (throughput, failover) and
refuses when there is not exactly one.

The paths are checked, not trusted: the page is one `.md` file directly under
`docs/benchmarks/` (never `index.md`), and the images are a directory UNDER the kind's
own image directory, which is where `regenerate.py` looks for output no run writes, so
a redirected run cannot leave stale output nobody checks.
"""
from __future__ import annotations

import json
import os
import posixpath
import re
from typing import NamedTuple

FILE = "report.json"
DOCS = "docs/benchmarks"
KEYS = ("doc", "img_dir")
OPTIONAL = ("latency_run",)
SUPERSEDES = "supersedes"
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class OutputsRefused(Exception):
    """`report.json` is malformed or names a path outside what a run may write; the message says which."""


class Outputs(NamedTuple):
    doc: str | None  # absolute; None only beside `supersedes`, for the kind's default
    img_dir: str | None  # absolute; None only beside `supersedes`, for the kind's default
    latency_run: str | None = None  # a run id, not a path
    supersedes: str | None = None  # a run id, not a path


def run_kind(run_dir: str) -> str:
    """Which report a run directory goes to, by the file it holds, as report.py dispatches it."""
    if os.path.isfile(os.path.join(run_dir, "failover.csv")):
        return "failover"
    if os.path.isfile(os.path.join(run_dir, "crash.csv")):
        return "crash"
    return "throughput"


def check_supersedes(value: object, own_id: str, path: str) -> str:
    """`supersedes` is one other run's id."""
    if not isinstance(value, str) or not _RUN_ID.match(value) or ".." in value:
        raise OutputsRefused(f"{path}: supersedes is {value!r}; it must be the id of a run in the same results "
                             "directory (one path component of letters, digits, '.', '_' or '-')")
    if value == own_id:
        raise OutputsRefused(f"{path}: the run supersedes itself ({value!r})")
    return value


def _clean(value: object, key: str, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise OutputsRefused(f"{path}: {key} must be a path as a non-empty string, not {value!r}")
    if "\\" in value or value.startswith("/") or any(part in ("", ".", "..") for part in value.split("/")):
        raise OutputsRefused(f"{path}: {key} is {value!r}, which is not a plain relative path under {DOCS}/ "
                             "(no leading or doubled slash, no `.` or `..` part, no backslash)")
    return value


def resolve(run_dir: str, repo: str, img_root: str, allow_latency_run: bool = False,
            allow_supersedes: bool = False) -> Outputs | None:
    """The run's own output paths, or None when it has no `report.json` (the kind's defaults apply).

    `img_root` is the kind's image directory relative to the repository
    (`docs/benchmarks/img`, `docs/benchmarks/archive/img`): the run's `img_dir` must be below it."""
    path = os.path.join(run_dir, FILE)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OutputsRefused(f"{path} is not readable JSON: {exc}") from exc
    optional = (OPTIONAL if allow_latency_run else ()) + ((SUPERSEDES,) if allow_supersedes else ())
    allowed = KEYS + optional
    superseding = isinstance(data, dict) and allow_supersedes and SUPERSEDES in data
    if (not isinstance(data, dict) or (not superseding and any(key not in data for key in KEYS))
            or any(key not in allowed for key in data)):
        said = f"exactly the keys {', '.join(KEYS)}" if not optional else \
            f"the keys {', '.join(KEYS)}, and optionally {', '.join(optional)}"
        raise OutputsRefused(f"{path} must be an object with {said}, not "
                             f"{sorted(data) if isinstance(data, dict) else type(data).__name__}")
    supersedes = check_supersedes(data[SUPERSEDES], os.path.basename(os.path.normpath(run_dir)), path) \
        if superseding else None
    if superseding and not any(key in data for key in KEYS):
        return Outputs(None, None, _latency_run(data, path), supersedes)
    if superseding and not all(key in data for key in KEYS):
        raise OutputsRefused(f"{path}: beside supersedes, give doc and img_dir both or neither (neither writes the "
                             "kind's default paths)")
    doc = _clean(data["doc"], "doc", path)
    img = _clean(data["img_dir"], "img_dir", path)
    if posixpath.dirname(doc) != DOCS or not doc.endswith(".md") or posixpath.basename(doc) == "index.md":
        raise OutputsRefused(f"{path}: doc is {doc!r}; it must be one Markdown page directly under {DOCS}/, "
                             "and not index.md")
    if not img.startswith(img_root + "/"):
        raise OutputsRefused(f"{path}: img_dir is {img!r}; for this kind it must be a directory under "
                             f"{img_root}/, where the drift check looks for output no run writes")
    return Outputs(os.path.join(repo, *doc.split("/")), os.path.join(repo, *img.split("/")),
                   _latency_run(data, path), supersedes)


def _latency_run(data: dict, path: str) -> str | None:
    latency = data.get("latency_run")
    if "latency_run" in data and (not isinstance(latency, str) or not _RUN_ID.match(latency) or ".." in latency):
        raise OutputsRefused(f"{path}: latency_run is {latency!r}; it must be the id of a run in the same results "
                             "directory (one path component of letters, digits, '.', '_' or '-')")
    return latency
