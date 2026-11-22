#!/usr/bin/env python3
"""The cluster-plane soak's profile, fault plan, recorder and sampler (#727).

`cluster-soak.sh` drives the run: three metadata and three data nodes from
the live-chaos library, a producer and a reader that never stop, and a fault
every `fault_interval_s`. This module is the part of it that is easier to get
right in Python than in shell, and that the evaluator must agree with:

  plan          the faults the run injects, one line each. The plan is the
                engine soak's (`benchmarks/soak/soak_profile.build_plan`, #726)
                over this profile, so the driver and the evaluator derive it
                from the same file and cannot drift apart.
  event         append one event to events.jsonl
  sampler       sample every node until the run's stop file appears
  leaders       every leadership a data node logged, as leaders.jsonl
  shim-source   the clock-skew shim's C source, read out of live-chaos
                scenario 07 the way the engine soak reads it
  field         print one value out of the profile, for the shell

Times in the records are seconds since the run started on the system's
monotonic clock (`origin` in the run directory), which every process on the
host shares and which the clock-step fault does not move.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[2]
sys.path.insert(0, str(REPO_ROOT / "benchmarks" / "soak"))

import soak_lab  # noqa: E402
import soak_profile  # noqa: E402
from soak_profile import ProfileError  # noqa: E402

PROFILE_DIR = HERE / "profiles"

# The faults the driver can inject. A profile names a subset, in its order.
FAULT_KINDS = (
    "range_leader_kill",
    "meta_leader_kill",
    "partition",
    "pause",
    "disk_full",
    "clock_step",
)

# Per-node series the evaluator holds to a trend limit (and, for the data
# directory, to an absolute ceiling).
TREND_SERIES = {
    "rss_bytes": {"label": "resident memory", "unit": "bytes", "nodes": ("meta", "data")},
    "open_fds": {"label": "open files", "unit": "files", "nodes": ("meta", "data")},
    "data_dir_bytes": {"label": "data directory", "unit": "bytes", "nodes": ("data",)},
}

REQUIRED_KEYS = (
    "name",
    "description",
    "duration_s",
    "first_fault_after_s",
    "fault_interval_s",
    "quiet_tail_s",
    "fault_cycle",
    "fault_hold_s",
    "clock_step_seconds",
    "recovery_timeout_s",
    "rate_records_per_s",
    "batch",
    "value_bytes",
    "sample_interval_s",
    "read_interval_s",
    "max_segment_bytes",
    "retention_bytes",
    "data_tmpfs_mib",
    "criteria",
)

INTEGER_KEYS = (
    "duration_s",
    "first_fault_after_s",
    "fault_interval_s",
    "quiet_tail_s",
    "recovery_timeout_s",
    "rate_records_per_s",
    "batch",
    "value_bytes",
    "sample_interval_s",
    "read_interval_s",
    "max_segment_bytes",
    "retention_bytes",
    "data_tmpfs_mib",
)

CRITERIA_KEYS = (
    "trend_window_fraction",
    "trend_min_pairs",
    "trend",
    "max_data_dir_bytes",
    "max_rss_bytes",
    "min_samples_fraction",
    "max_sample_gap_s",
    "min_acked_fraction",
    "max_read_gap_s",
)


def _positive_int(profile: dict, key: str) -> int:
    value = profile.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ProfileError(f"profile key '{key}' must be a positive integer, got {value!r}")
    return value


def validate(profile: dict) -> dict:
    """Return the profile when it can be run; raise ProfileError naming the key."""
    if not isinstance(profile, dict):
        raise ProfileError("a profile is a JSON object")
    missing = [key for key in REQUIRED_KEYS if key not in profile]
    if missing:
        raise ProfileError(f"profile is missing keys: {', '.join(missing)}")
    for key in INTEGER_KEYS:
        _positive_int(profile, key)
    cycle = profile["fault_cycle"]
    if not isinstance(cycle, list) or not cycle:
        raise ProfileError("profile key 'fault_cycle' must be a non-empty list")
    for kind in cycle:
        if kind not in FAULT_KINDS:
            raise ProfileError(f"fault_cycle names '{kind}', which is not one of {', '.join(FAULT_KINDS)}")
    hold = profile["fault_hold_s"]
    if not isinstance(hold, dict):
        raise ProfileError("profile key 'fault_hold_s' must be an object")
    for kind in cycle:
        if not isinstance(hold.get(kind), int) or hold[kind] < 0:
            raise ProfileError(f"fault_hold_s must give a non-negative integer for '{kind}'")
    steps = profile["clock_step_seconds"]
    if not isinstance(steps, list) or not all(isinstance(s, int) and not isinstance(s, bool) and s != 0 for s in steps):
        raise ProfileError("clock_step_seconds must be a list of non-zero integers")
    if "clock_step" in cycle and not steps:
        raise ProfileError("clock_step is in fault_cycle but clock_step_seconds is empty")
    if profile["quiet_tail_s"] >= profile["duration_s"]:
        raise ProfileError("quiet_tail_s must be shorter than duration_s")
    if profile["retention_bytes"] < 2 * profile["max_segment_bytes"]:
        raise ProfileError("retention_bytes must hold at least two segments (max_segment_bytes)")
    if profile["data_tmpfs_mib"] * 2**20 < 2 * profile["retention_bytes"]:
        raise ProfileError("data_tmpfs_mib must be at least twice retention_bytes, or retention cannot keep up")
    # The history proves an acknowledged record was kept only if some read saw
    # it before retention reclaimed it: the retained window must outlast
    # several read intervals at the offered rate.
    record_bytes = profile["value_bytes"] + 64
    window_s = profile["retention_bytes"] / (profile["rate_records_per_s"] * record_bytes)
    if window_s < 4 * profile["read_interval_s"]:
        raise ProfileError(
            f"retention_bytes keeps about {window_s:.0f} s of records at the offered rate; it must keep "
            f"at least four read intervals ({4 * profile['read_interval_s']} s), or records can be "
            "reclaimed before a read sees them"
        )
    criteria = profile["criteria"]
    if not isinstance(criteria, dict):
        raise ProfileError("profile key 'criteria' must be an object")
    absent = [key for key in CRITERIA_KEYS if key not in criteria]
    if absent:
        raise ProfileError(f"criteria is missing keys: {', '.join(absent)}")
    fraction = criteria["trend_window_fraction"]
    if not isinstance(fraction, (int, float)) or not 0 < fraction <= 1:
        raise ProfileError("criteria.trend_window_fraction must be in (0, 1]")
    trend = criteria["trend"]
    if not isinstance(trend, dict):
        raise ProfileError("criteria.trend must be an object")
    for series in TREND_SERIES:
        entry = trend.get(series)
        if not isinstance(entry, dict):
            raise ProfileError(f"criteria.trend has no entry for '{series}'")
        for key in ("max_growth_pct", "min_abs_growth"):
            value = entry.get(key)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0:
                raise ProfileError(f"criteria.trend.{series}.{key} must be a non-negative number")
    if criteria["max_data_dir_bytes"] < profile["retention_bytes"]:
        raise ProfileError("criteria.max_data_dir_bytes must be at least retention_bytes")
    plan = build_plan(profile)
    if not plan:
        raise ProfileError("the profile schedules no fault: duration_s is too short")
    unscheduled = [kind for kind in cycle if kind not in {fault.kind for fault in plan}]
    if unscheduled:
        raise ProfileError(f"the plan never reaches {', '.join(unscheduled)}: lengthen duration_s")
    return profile


def build_plan(profile: dict) -> list[soak_profile.PlannedFault]:
    """The engine soak's plan over this profile: one fault every
    `fault_interval_s` from `first_fault_after_s`, cycling through
    `fault_cycle`, none in the last `quiet_tail_s`."""
    return soak_profile.build_plan(profile)


def clock_step_for(profile: dict, occurrence: int) -> int:
    return soak_profile.clock_step_for(profile, occurrence)


def load(name_or_path: str) -> dict:
    """A profile by name (`7d`) or by path, validated."""
    path = Path(name_or_path)
    if not path.suffix:
        path = PROFILE_DIR / f"{name_or_path}.json"
    try:
        profile = json.loads(path.read_text())
    except FileNotFoundError:
        known = ", ".join(sorted(p.stem for p in PROFILE_DIR.glob("*.json")))
        raise ProfileError(f"no profile at {path} (known: {known})") from None
    except json.JSONDecodeError as error:
        raise ProfileError(f"{path} is not valid JSON: {error}") from None
    return validate(profile)


# ---------------------------------------------------------------------------
# Recording
# ---------------------------------------------------------------------------


def elapsed(run_dir: Path) -> float:
    origin = float((run_dir / "origin").read_text())
    return round(time.monotonic() - origin, 3)


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _value(text: str):
    """A `key=value` argument's value: JSON when it parses (numbers, true,
    false, null, quoted strings), the text itself otherwise."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def append(path: Path, row: dict) -> None:
    # One write of one line on an O_APPEND descriptor: the driver, the
    # producer loop, the reader loop and the sampler append to the same files.
    line = (json.dumps(row, sort_keys=True) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, line)
    finally:
        os.close(fd)


def record_event(run_dir: Path, kind: str, fields: dict) -> dict:
    row = {"elapsed_s": elapsed(run_dir), "ts": _stamp(), "kind": kind, **fields}
    append(run_dir / "events.jsonl", row)
    return row


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

METRIC_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)")

# What the sampler keeps from each node's /metrics, by node kind. Summed over
# labels; a gauge a node does not export in its current role is left out.
META_METRICS = ("vtop_meta_raft_term", "vtop_meta_raft_leader_id")
DATA_METRICS = (
    "vtop_broker_candidate_leading",
    "vtop_broker_held_fencing_epoch",
    "vtop_broker_lease_active",
    "vtop_broker_log_start_offset",
    "vtop_broker_local_committed_offset",
    "vtop_broker_cluster_committed_offset",
    "vtop_broker_retention_retained_bytes",
)


def parse_metrics(text: str, wanted: tuple[str, ...]) -> dict:
    out: dict = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        match = METRIC_LINE.match(line)
        if not match:
            continue
        name, labels, value = match[1], match[2] or "", match[3]
        try:
            number = float(value)
        except ValueError:
            continue
        if name == "vtop_meta_raft_state":
            if 'state="leader"' in labels and number == 1:
                out["raft_leader"] = True
            continue
        if name in wanted:
            out[name] = out.get(name, 0.0) + number
    return out


def scrape(addr: str, wanted: tuple[str, ...], timeout: float = 3.0) -> dict | None:
    try:
        with urllib.request.urlopen(f"http://{addr}/metrics", timeout=timeout) as response:
            return parse_metrics(response.read().decode(errors="replace"), wanted)
    except (urllib.error.URLError, OSError, ValueError):
        return None


def proc_stats(pid: int) -> dict | None:
    """Resident memory and open files of a live process, from /proc."""
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except OSError:
        return None
    if re.search(r"^State:\s+Z", status, re.M):
        return None
    rss = re.search(r"^VmRSS:\s+(\d+) kB", status, re.M)
    try:
        fds = len(os.listdir(f"/proc/{pid}/fd"))
    except OSError:
        fds = None
    return {"rss_bytes": int(rss[1]) * 1024 if rss else None, "open_fds": fds}


def dir_bytes(path: Path) -> int | None:
    if not path.is_dir():
        return None
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_blocks * 512
            except OSError:
                pass
    return total


def node_state(workdir: Path, name: str) -> tuple[int | None, int]:
    """The node's current pid (None while it is down) and lifetime."""
    nodes = workdir / "nodes"
    try:
        pid = int((nodes / f"{name}.pid").read_text().strip())
    except (OSError, ValueError):
        pid = None
    try:
        life = int((nodes / f"{name}.life").read_text().strip())
    except (OSError, ValueError):
        life = 0
    return pid, life


def sample_once(run_dir: Path, workdir: Path, layout: dict) -> dict:
    """One record per node: up, lifetime, the process's memory and open files,
    the data directory's size and the gauges the evaluator reads."""
    row: dict = {"elapsed_s": elapsed(run_dir), "ts": _stamp(), "nodes": {}}
    for name, node in layout.items():
        pid, life = node_state(workdir, name)
        stats = proc_stats(pid) if pid else None
        entry: dict = {"kind": node["kind"], "lifetime": life, "up": stats is not None}
        if stats:
            entry.update(stats)
        wanted = META_METRICS if node["kind"] == "meta" else DATA_METRICS
        metrics = scrape(node["metrics"], wanted) if stats else None
        if metrics is not None:
            entry["metrics"] = metrics
        if node.get("data_dir"):
            entry["data_dir_bytes"] = dir_bytes(Path(node["data_dir"]))
        if node.get("disk"):
            try:
                st = os.statvfs(node["disk"])
                entry["disk_free_bytes"] = st.f_bavail * st.f_frsize
            except OSError:
                pass
        row["nodes"][name] = entry
    append(run_dir / "samples.jsonl", row)
    return row


def sampler(run_dir: Path, workdir: Path, interval: float) -> int:
    layout = json.loads((run_dir / "nodes.json").read_text())
    stop = run_dir / "stop-sampler"
    while not stop.exists():
        started = time.monotonic()
        sample_once(run_dir, workdir, layout)
        remaining = interval - (time.monotonic() - started)
        while remaining > 0 and not stop.exists():
            time.sleep(min(remaining, 1.0))
            remaining -= 1.0
    return 0


# ---------------------------------------------------------------------------
# Leadership as the data nodes logged it
# ---------------------------------------------------------------------------

ROLE_LINE = re.compile(r"data_node_role_changed role=leader\b.*?\bepoch=(\d+)")


def leaders(log_dir: Path) -> list[dict]:
    """Every `role=leader` transition in every data node's logs, one row per
    line: which node, in which of its lifetimes, at which epoch. Each node
    lifetime logs to its own file, `data-candidate-<n>-<lifetime>.log`."""
    rows = []
    pattern = re.compile(r"^data-candidate-(\d+)-(\d+)\.log$")
    for path in sorted(log_dir.iterdir()):
        match = pattern.match(path.name)
        if not match:
            continue
        for number, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
            role = ROLE_LINE.search(line)
            if role:
                rows.append(
                    {
                        "node": f"data-{match[1]}",
                        "lifetime": int(match[2]),
                        "epoch": int(role[1]),
                        "log": path.name,
                        "line": number,
                    }
                )
    return rows


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    p.add_argument("profile")
    p = sub.add_parser("field")
    p.add_argument("profile")
    p.add_argument("key", help="a top-level key, or criteria.<key>")
    p = sub.add_parser("step")
    p.add_argument("profile")
    p.add_argument("occurrence", type=int)
    p = sub.add_parser("event")
    p.add_argument("run_dir", type=Path)
    p.add_argument("kind")
    p.add_argument("fields", nargs="*", help="key=value; the value is JSON when it parses")
    p = sub.add_parser("sampler")
    p.add_argument("run_dir", type=Path)
    p.add_argument("workdir", type=Path)
    p.add_argument("interval", type=float)
    p = sub.add_parser("leaders")
    p.add_argument("log_dir", type=Path)
    sub.add_parser("shim-source")
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            for fault in build_plan(load(args.profile)):
                print(fault.id, fault.at_s, fault.kind, fault.hold_s)
        elif args.command == "field":
            value = load(args.profile)
            for part in args.key.split("."):
                value = value[part]
            print(json.dumps(value) if isinstance(value, (dict, list)) else value)
        elif args.command == "step":
            print(clock_step_for(load(args.profile), args.occurrence))
        elif args.command == "event":
            fields = {}
            for item in args.fields:
                key, sep, text = item.partition("=")
                if not sep:
                    parser.error(f"event field '{item}' is not key=value")
                fields[key] = _value(text)
            record_event(args.run_dir, args.kind, fields)
        elif args.command == "sampler":
            return sampler(args.run_dir, args.workdir, args.interval)
        elif args.command == "leaders":
            for row in leaders(args.log_dir):
                print(json.dumps(row, sort_keys=True))
        elif args.command == "shim-source":
            sys.stdout.write(soak_lab.extract_skew_shim_source())
    except ProfileError as error:
        print(f"cluster_soak: {error}", file=sys.stderr)
        return 2
    except KeyError as error:
        print(f"cluster_soak: no such profile key {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
