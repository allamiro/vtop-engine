"""The `link` kind (#747): upload over a long, lossy or policed link, VTOP against aws-cli, rclone and s5cmd.

Cells (#747 "Cells"; the `calibrate` cell of the issue is not a row of its
own: every run is bracketed by an iperf3 calibration immediately before and
after it, both recorded on the run's row, and a run whose two calibrations
differ by more than 10% is refused naming both):

- `smoke` (correctness): a few small files on the unshaped `lan` profile, every
  tool, read back by the harness; proves the lab end to end in minutes;
- `committed` (correctness): the documented defaults on a shaped profile,
  judged only by objects committed of N. The one cell a hosted runner may run,
  and only where the recorded bottleneck is the link and the tool's CPU share
  stayed under `HOSTED_CPU_SHARE_LIMIT` (#744 section 4.4);
- `defaults` (performance): each tool at its documented defaults, all N files,
  on the three shaped profiles;
- `equal-conns` (performance): each tool set to 4, 8 and 16 concurrent TCP
  connections in total (workers x parts in flight, `connections_of`);
- `tuned-grid` (performance): the same grid size for every tool (at most 6
  points over its own knobs) on a reduced workload;
- `tuned` (performance): each tool's best grid point re-run on all N files;
- `neighbour` (performance): the #478 instrument, a plain TCP iperf3 flow alone,
  beside the tool, and alone again; Jain's index only beside both per-flow
  goodputs.

Profiles name what each tool's "committed" means (#744 section 2.1): VTOP
`strong` (the object is verified against its stored digest before the source
moves), aws-cli and s5cmd `exit-code` (a command returning 0), rclone
`size-and-checksum` (its own post-upload check). None of them is trusted here:
objects committed of N is the HARNESS's read-back of every object, by sha256
against the seed's manifest, never a tool's exit code.
"""
from __future__ import annotations

import json
import math
from collections.abc import Iterable, Sequence
from typing import Any, NamedTuple

from compare_common import schema

SYSTEMS = ("aws-cli", "vtop", "rclone", "s5cmd")
LINK_PROFILES = ("lan", "wan100", "wan100-loss1", "wan100-policed")
SHAPED = ("wan100", "wan100-loss1", "wan100-policed")
POLICED = "wan100-policed"
POLICER_MBPS = 10.0  # the policed profile's reference is the token rate (#747)
CALIBRATION_TOLERANCE = 0.10
HOSTED_CPU_SHARE_LIMIT = 0.5  # of the tool container's CPU limit (#747 test 8)
CPU_BOTTLENECK_SHARE = 0.8  # a tool above this share of its CPUs is CPU-bound, not link-bound
MAX_GRID = 6
CONNECTION_TARGETS = (4, 8, 16)
MIB = 1024 * 1024

CELLS = {
    "smoke": "correctness",
    "committed": "correctness",
    "defaults": "performance",
    "equal-conns": "performance",
    "tuned-grid": "performance",
    "tuned": "performance",
    "neighbour": "performance",
}
PROFILES = {
    "aws-cli": ("exit-code",),
    "vtop": ("strong",),
    "rclone": ("size-and-checksum",),
    "s5cmd": ("exit-code",),
}

READBACK_COLUMNS = ("files_expected", "objects_committed", "objects_mismatched", "objects_extra", "payload_bytes")
TIME_COLUMNS = ("seconds_to_complete", "upload_seconds", "link_reference_mbps", "calib_before_mbps",
                "calib_after_mbps", "goodput_mbps", "goodput_fraction", "vtop_upload_goodput_fraction")
COST_COLUMNS = ("wire_bytes", "wire_bytes_per_payload_byte", "cpu_seconds", "cpu_seconds_per_gib", "cpu_share",
                "bottleneck")
SETTING_COLUMNS = ("settings", "connections_actual", "grid_size", "exit_code")
NEIGHBOUR_COLUMNS = ("neighbour_alone_before_mbps", "neighbour_with_mbps", "neighbour_alone_after_mbps",
                     "tool_with_mbps", "jain_index")
METRIC_COLUMNS = READBACK_COLUMNS + TIME_COLUMNS + COST_COLUMNS + SETTING_COLUMNS + NEIGHBOUR_COLUMNS

REQUIRED = {
    "smoke": READBACK_COLUMNS + ("seconds_to_complete", "calib_before_mbps", "calib_after_mbps"),
    "committed": READBACK_COLUMNS + ("seconds_to_complete", "calib_before_mbps", "calib_after_mbps",
                                     "cpu_share", "bottleneck"),
    "defaults": READBACK_COLUMNS + TIME_COLUMNS[:-1] + COST_COLUMNS,
    "equal-conns": READBACK_COLUMNS + TIME_COLUMNS[:-1] + COST_COLUMNS + ("settings", "connections_actual"),
    "tuned-grid": READBACK_COLUMNS + TIME_COLUMNS[:-1] + ("settings", "grid_size"),
    "tuned": READBACK_COLUMNS + TIME_COLUMNS[:-1] + COST_COLUMNS + ("settings", "grid_size"),
    "neighbour": READBACK_COLUMNS + ("seconds_to_complete",) + NEIGHBOUR_COLUMNS,
}


class LinkRefused(Exception):
    """A setting, calibration or read-back the harness will not record as a result; the message says why."""


# ------------------------------------------------------------- read-back


class Readback(NamedTuple):
    files_expected: int
    objects_committed: int
    objects_mismatched: int
    objects_extra: int
    payload_bytes: int
    missing: list[str]


def judge_readback(manifest: dict[str, tuple[int, str]], objects: Iterable[tuple[str, int, str]]) -> Readback:
    """Objects committed of N, from the HARNESS's read-back (#747 test 3).

    `manifest` is {file name: (size, sha256)} as the seed wrote it; `objects`
    is (key, size, sha256) of every object the harness read back from the
    tool's prefix. A file is committed when an object with its exact size and
    sha256 is in the store, whatever the key (VTOP names objects by batch, the
    other tools by file). The tool's exit code is not an input: a tool that
    exits 0 after writing a truncated object scores that file as missing.
    VTOP's manifests and receipts are neither committed nor extra."""
    stored: dict[tuple[int, str], int] = {}
    mismatched = extra = 0
    wanted = {(size, sha) for size, sha in manifest.values()}
    names_by_key = {(size, sha): name for name, (size, sha) in manifest.items()}
    for key, size, sha in objects:
        if key.endswith((".manifest.json", ".receipt.json")):
            continue
        if (size, sha) in wanted:
            stored[(size, sha)] = stored.get((size, sha), 0) + 1
        elif any(key.endswith(name) for name in manifest):
            mismatched += 1
        else:
            extra += 1
    committed = [k for k in wanted if k in stored]
    # A file stored under more than one key is committed once; every copy beyond the first is extra
    # (a duplicate-output regression must show, review of #907).
    extra += sum(count - 1 for count in stored.values())
    return Readback(len(manifest), len(committed), mismatched, extra, sum(size for size, _sha in committed),
                    sorted(names_by_key[k] for k in wanted if k not in stored))


# ----------------------------------------------------------- calibration


def check_calibrations(link_profile: str, before_mbps: float, after_mbps: float,
                       tolerance: float = CALIBRATION_TOLERANCE) -> float:
    """The run's link reference, or LinkRefused naming both calibrations (#747 test 2).

    The reference is the mean of the two iperf3 calibrations that bracket the
    run; on the policed profile it is the token rate (a policer holds no
    queue, so TCP through it settles below its rate by design). Two
    calibrations more than `tolerance` apart (relative to their mean) mean the
    link was not one link for the run. The unshaped `lan` profile is a
    calibration of the Docker bridge, not of a link: its reference is recorded
    and never gated. Nor is the policed profile's: its reference is the token
    rate, not a measurement, and TCP through a queue-less policer is erratic
    by design, so its two calibrations are recorded beside the row and not
    gated."""
    for value in (before_mbps, after_mbps):
        if not (isinstance(value, (int, float)) and math.isfinite(value) and value > 0):
            raise LinkRefused(f"a calibration of {value!r} Mbit/s measured nothing")
    mean = (before_mbps + after_mbps) / 2.0
    if link_profile in SHAPED and link_profile != POLICED:
        spread = abs(before_mbps - after_mbps) / mean
        if spread > tolerance:
            raise LinkRefused(f"the calibrations before ({before_mbps} Mbit/s) and after ({after_mbps} Mbit/s) the "
                              f"run differ by {spread:.1%}, more than {tolerance:.0%}: the link was not one link "
                              "for the whole run, so the run is refused rather than recorded")
    if link_profile == POLICED:
        return POLICER_MBPS
    return round(mean, 3)


# ----------------------------------------------------------- connections


class ToolKnobs(NamedTuple):
    """How one tool's knobs map onto TCP connections to the store."""

    workers: str  # the knob that runs objects (or requests) in parallel
    parts: str | None  # the knob that runs parts of one object in parallel
    threshold_key: str | None  # multipart threshold knob
    chunk_key: str | None  # part size knob
    default_threshold: int  # bytes: an object at or above it is multipart
    default_chunk: int


KNOBS = {
    # VTOP: transports.tcp_tls.max_concurrency x parts_in_flight (docs/configure/reference.md);
    # multipart at 8 MiB with 8 MiB parts by default.
    "vtop": ToolKnobs("max_concurrency", "parts_in_flight", None, "part_size_bytes", 8 * MIB, 8 * MIB),
    # aws-cli: max_concurrent_requests is ONE pool shared by files and parts (aws s3 configuration docs),
    # so it is the connection count by itself.
    "aws-cli": ToolKnobs("max_concurrent_requests", None, "multipart_threshold", "multipart_chunksize",
                         8 * MIB, 8 * MIB),
    # rclone: --transfers files at once, --s3-upload-concurrency parts of each; multipart only from
    # --s3-upload-cutoff (200 MiB by default), so a 20 MiB file is one PUT at the defaults.
    "rclone": ToolKnobs("transfers", "upload_concurrency", "upload_cutoff", "chunk_size", 200 * MIB, 5 * MIB),
    # s5cmd: --numworkers files at once, cp --concurrency parts of each, --part-size MiB (default 50).
    "s5cmd": ToolKnobs("numworkers", "concurrency", None, "part_size", 50 * MIB, 50 * MIB),
}
DEFAULT_SETTINGS = {
    "vtop": {"max_concurrency": 8, "parts_in_flight": 4},
    "aws-cli": {"max_concurrent_requests": 10},
    "rclone": {"transfers": 4, "upload_concurrency": 4},
    "s5cmd": {"numworkers": 256, "concurrency": 5},
}


def parts_per_object(tool: str, settings: dict[str, Any], object_bytes: int) -> int:
    knobs = KNOBS[tool]
    chunk = int(settings.get(knobs.chunk_key, knobs.default_chunk)) if knobs.chunk_key else knobs.default_chunk
    if tool == "s5cmd":
        chunk = int(settings.get("part_size", knobs.default_chunk // MIB)) * MIB
    threshold = int(settings.get(knobs.threshold_key, knobs.default_threshold)) if knobs.threshold_key else chunk
    if object_bytes < threshold or object_bytes <= chunk:
        return 1
    return math.ceil(object_bytes / chunk)


def connections_of(tool: str, settings: dict[str, Any], object_bytes: int, files: int) -> int:
    """Concurrent TCP connections the settings open on this workload: workers x parts in flight,
    each capped by what the workload offers (files, parts per object)."""
    if tool not in KNOBS:
        raise LinkRefused(f"no connection model for {tool!r}")
    knobs = KNOBS[tool]
    merged = {**DEFAULT_SETTINGS[tool], **settings}
    workers = int(merged[knobs.workers])
    if tool == "aws-cli":
        return min(workers, files * parts_per_object(tool, merged, object_bytes))
    parts = int(merged[knobs.parts]) if knobs.parts else 1
    return min(workers, files) * min(parts, parts_per_object(tool, merged, object_bytes))


def equal_connections(tool: str, target: int) -> dict[str, int]:
    """The settings that give `target` connections: `target` workers, one part each."""
    knobs = KNOBS[tool]
    out = {knobs.workers: target}
    if knobs.parts:
        out[knobs.parts] = 1
    return out


def check_connections(tool: str, settings: dict[str, Any], target: int, object_bytes: int, files: int) -> int:
    """Refuse a setting that misses the target (#747 test 4); returns the connection count."""
    actual = connections_of(tool, settings, object_bytes, files)
    if actual != target:
        raise LinkRefused(f"{tool} with {json.dumps(settings, sort_keys=True)} opens {actual} connections on "
                          f"{files} objects of {object_bytes} bytes, not the {target} the cell asks for")
    return actual


def check_grids(grids: dict[str, Sequence[dict[str, Any]]], systems: Sequence[str]) -> int:
    """Every tool gets a grid of the same size, at most MAX_GRID points (#744 rule 14, #747 test 6)."""
    sizes = {system: len(grids.get(system) or ()) for system in systems}
    if len(set(sizes.values())) != 1:
        raise LinkRefused(f"the tuned grids differ in size: {sizes}; every tool gets the same number of points")
    size = next(iter(sizes.values()))
    if not 1 <= size <= MAX_GRID:
        raise LinkRefused(f"a tuned grid of {size} points; between 1 and {MAX_GRID}")
    return size


# ------------------------------------------------------------ counters


def wire_bytes(before: dict[str, int], after: dict[str, int]) -> int:
    """Upload bytes on the wire, from the MIDDLEBOX's engine-facing interface counters (#747 test 9).

    `before` and `after` are the interface's `rx_bytes` (packets arriving from
    the tools are the upload direction), read in the middlebox, so every
    retransmission and every request header counts. A tool's own transfer
    report is never an input."""
    delta = int(after["rx_bytes"]) - int(before["rx_bytes"])
    if delta < 0:
        raise LinkRefused("the middlebox's rx_bytes went backwards (the interface was recreated mid-run)")
    return delta


def bottleneck(link_profile: str, cpu_share: float | None) -> str:
    if link_profile not in SHAPED:
        return "unknown"
    if cpu_share is None:
        return "unknown"
    return "cpu" if cpu_share >= CPU_BOTTLENECK_SHARE else "link"


def jain(neighbour_with_mbps: float | None, tool_with_mbps: float | None) -> float:
    """Jain's index over the two flows, refused unless BOTH per-flow goodputs exist (#747 test 10)."""
    from lib import competitor

    if neighbour_with_mbps is None or tool_with_mbps is None or neighbour_with_mbps == "" or tool_with_mbps == "":
        raise LinkRefused("Jain's index is written only beside both per-flow goodputs; one is missing")
    return competitor.jain_index([float(neighbour_with_mbps), float(tool_with_mbps)])


# ------------------------------------------------------------ per flow


def parse_flow_counters(iptables_output: str) -> dict[str, int]:
    """{comment: bytes} from `iptables -nvx -L <chain>`: the middlebox's per-flow counting rules
    (`-m comment --comment vtb-<flow>`, no target), which count only packets that crossed the shaped
    link, since the shaping drops on the ifb before the IP layer sees them."""
    out: dict[str, int] = {}
    for line in iptables_output.splitlines():
        fields = line.split()
        found = [f for f in fields if f.startswith("vtb-")]
        if len(fields) > 2 and fields[1].isdigit() and found:
            out[found[0]] = out.get(found[0], 0) + int(fields[1])
    return out


def window_mbps(samples: Sequence[tuple[float, dict[str, int]]], flow: str, start: float, end: float) -> float:
    """A flow's rate over [start, end] from timestamped counter samples: the first sample at or after
    `start` to the last at or before `end`. Both flows of a fairness index are taken over the SAME
    window this way, so Jain's index compares like with like (#747 test 10, review of #907)."""
    inside = [(at, counters) for at, counters in samples if start <= at <= end and flow in counters]
    if len(inside) < 2 or inside[-1][0] - inside[0][0] <= 0:
        raise LinkRefused(f"fewer than two counter samples of {flow} inside the window {start:.1f}-{end:.1f}")
    delta = inside[-1][1][flow] - inside[0][1][flow]
    if delta < 0:
        raise LinkRefused(f"the {flow} counter went backwards inside the window")
    return delta * 8 / 1e6 / (inside[-1][0] - inside[0][0])


def hosted_refusal(bottleneck_: str | None, cpu_share: float | None) -> str | None:
    """Why a hosted `committed` row cannot count, or None: the recorded bottleneck must be the link AND the
    tool's CPU share at most HOSTED_CPU_SHARE_LIMIT (#747 test 8). One rule for the runner, which records a
    `refused` row, and for the loader, which refuses an ok row that breaks it (review of #907)."""
    if bottleneck_ != "link" or cpu_share is None or cpu_share > HOSTED_CPU_SHARE_LIMIT:
        return (f"a hosted row is accepted only where the recorded bottleneck is the link and the tool's CPU "
                f"share is at most {HOSTED_CPU_SHARE_LIMIT} (bottleneck {bottleneck_!r}, cpu_share {cpu_share!r})")
    return None


def _int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def run_status(cell: str, exit_code: Any, out: dict[str, Any], committed: int, expected: int,
               tool_output: str = "") -> tuple[str, str]:
    """(status, note) of one tool run (#744 rule 18: a failure is recorded as measured, never retried).

    `failed` when the tool exited non-zero or timed out, when a smoke did not commit every file, or when
    a column an ok row of the cell must carry could not be measured (nothing committed, no CPU samples):
    an ok row with a hole would refuse the whole run at load (review of #907). The note always carries
    objects committed of N, which the read-back measured whatever the tool did."""
    counted = f"{committed} of {expected} files committed"
    if exit_code not in (0, None):
        return "failed", f"exited {exit_code} ({counted}): {' '.join(tool_output.split())[-400:]}"
    if cell == "smoke" and (committed != expected or _int(out.get("objects_extra")) or
                            _int(out.get("objects_mismatched"))):
        return "failed", (f"{counted}, {_int(out.get('objects_extra'))} extra and "
                          f"{_int(out.get('objects_mismatched'))} mismatched objects")
    missing = [column for column in REQUIRED.get(cell, ()) if out.get(column) in ("", None)]
    if missing:
        return "failed", f"could not measure {', '.join(missing)} ({counted})"
    return "ok", ""


# --------------------------------------------------------------- rows


def _num(row: dict[str, str], column: str) -> float | None:
    try:
        return float(row[column])
    except (KeyError, ValueError):
        return None


def check_row(row: dict[str, str]) -> list[str]:
    problems = []
    if row.get("jain_index", "") != "" and (row.get("neighbour_with_mbps", "") == "" or
                                            row.get("tool_with_mbps", "") == ""):
        problems.append("a Jain index without both per-flow goodputs beside it")
    if row["system"] != "vtop" and row.get("vtop_upload_goodput_fraction", "") != "":
        problems.append("vtop_upload_goodput_fraction is VTOP's upload-only goodput and belongs on VTOP rows only")
    if row["status"] != "ok":
        return problems
    cell = row["cell"]
    missing = [column for column in REQUIRED.get(cell, ()) if row.get(column, "") == ""]
    if missing:
        problems.append(f"an ok {cell} row lacks {', '.join(missing)}")
    if row["system"] == "vtop" and cell in ("defaults", "equal-conns", "tuned") and \
            row.get("vtop_upload_goodput_fraction", "") == "":
        problems.append("a VTOP row must carry its upload-only goodput beside its end-to-end goodput "
                        "(vtop_upload_goodput_fraction), never in its place")
    before, after = _num(row, "calib_before_mbps"), _num(row, "calib_after_mbps")
    if before and after and row.get("link_profile") in SHAPED and row.get("link_profile") != POLICED:
        try:
            check_calibrations(row["link_profile"], before, after)
        except LinkRefused as exc:
            problems.append(f"an ok row whose calibrations disagree: {exc}")
    if cell == "smoke" and not missing and (row["objects_committed"] != row["files_expected"] or
                                            _int(row["objects_extra"]) or _int(row["objects_mismatched"])):
        problems.append(f"a smoke row with {row['objects_committed']} of {row['files_expected']} objects "
                        f"committed, {row['objects_extra']} extra and {row['objects_mismatched']} mismatched "
                        "is not ok")
    if cell == "equal-conns" and not missing and row["connections_actual"] != row["connections"]:
        problems.append(f"connections_actual {row['connections_actual']} is not the cell's {row['connections']}")
    if cell in ("tuned-grid", "tuned") and not missing and not 1 <= int(float(row["grid_size"])) <= MAX_GRID:
        problems.append(f"grid_size {row['grid_size']} is not between 1 and {MAX_GRID}")
    if row["host_class"] == "hosted" and cell == "committed":
        reason = hosted_refusal(row.get("bottleneck"), _num(row, "cpu_share"))
        if reason:
            problems.append(reason)
        if row.get("cpu_seconds_per_gib", "") != "":
            problems.append("CPU-seconds from a hosted runner are never recorded")
    return problems


KIND = schema.Kind(
    name="link",
    family="link",
    use_cases=("c",),
    variant_columns=("link_profile", "connections", "grid_point"),
    metric_columns=METRIC_COLUMNS,
    cells=CELLS,
    profiles=PROFILES,
    forbidden=None,
    check_row=check_row,
    rotate_systems=True,
)
