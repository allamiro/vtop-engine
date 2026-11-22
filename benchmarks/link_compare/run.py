#!/usr/bin/env python3
"""Upload over a long, lossy or policed link: VTOP against aws-cli, rclone and s5cmd (#747).

    python3 benchmarks/link_compare/run.py --plan smoke                 # minutes, any host, unshaped
    python3 benchmarks/link_compare/run.py --plan full --dry-run        # the cells and projected hours
    python3 benchmarks/link_compare/run.py --plan full --budget-minutes 600
    python3 benchmarks/link_compare/run.py --plan full --resume <run-id> --budget-minutes 600
    python3 benchmarks/link_compare/run.py --check benchmarks/link_compare/results/<run-id>

The same seeded files, from the same local volume, to the same lab MinIO,
through the same #477 middlebox (`compose.yml`), by each tool in turn, in an
order rotated by one per repetition. Shaped profiles (wan100, wan100-loss1,
wan100-policed, from `egress_baseline.PROFILES`) need a Linux kernel with
`ifb`; `lib/netem.py` refuses one without it, naming it.

Per run: the shape is installed; iperf3 measures the link alone before and
after the tool (a run whose two calibrations differ by more than 10% is
refused, naming both); the middlebox's interface counters and the tool
container's cgroup are read around the tool; then, outside the timed window,
the HARNESS lists the bucket and reads every object back, comparing sha256
against the seed's manifest. Objects committed of N comes from that read-back,
never from a tool's exit code. Results land in `results/<run-id>/link.csv`
and `environment.json` (`compare_common.schema`), one row per run, appended
and fsynced as each finishes, so `--resume` never repeats one.
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import shlex
import subprocess
import sys
import threading
import time
from dataclasses import replace
from typing import Any

HERE = os.path.dirname(os.path.realpath(__file__))
BENCH = os.path.dirname(HERE)
REPO = os.path.dirname(BENCH)
if BENCH not in sys.path:
    sys.path.insert(0, BENCH)

from compare_common import cgroup, docker_api, environment, lab, registry, runner, schema  # noqa: E402
from compare_common import plan as plans  # noqa: E402
from link_compare import kind as link  # noqa: E402
from link_compare.kind import KIND  # noqa: E402

PLANS = os.path.join(HERE, "plans.json")
COMPOSE = os.path.join(HERE, "compose.yml")
RESULTS = os.path.join(HERE, "results")
CONFIGS = {
    "vtop": "benchmarks/link_compare/tools/vtop/config.yaml.in",
    "aws-cli": "benchmarks/link_compare/tools/aws-cli/config",
    "rclone": "benchmarks/link_compare/tools/rclone/rclone.conf",
    "s5cmd": "benchmarks/link_compare/tools/s5cmd/defaults.args",
}
EXTRA_TRACKED = ("benchmarks/link_compare/tools/vtop/run.sh", "benchmarks/link_compare/compose.yml",
                 "benchmarks/link_compare/gen.py")
TOOL_LIMITS = "2 CPUs, 2g, no swap"
TOOL_CPUS = 2.0
CALIBRATION_SECONDS = 10
CALIBRATION_OMIT = 3
SEED = 747


def log(message: str) -> None:
    lab.log("link-compare", message)


# --------------------------------------------------------------------- lab


class Lab:
    def __init__(self, prefix: str, vtop_image: str, run_dir: str):
        self.prefix = prefix
        self.project = prefix
        self.vtop_image = vtop_image
        self.run_dir = run_dir
        self.dataset: tuple[int, float] | None = None
        self.manifest: dict[str, tuple[int, str]] = {}
        self.engine_iface: str | None = None

    def env(self, tool_cfg: str | None = None) -> dict[str, str]:
        out = {"LAB": self.prefix, "VTOP_IMAGE": self.vtop_image}
        if tool_cfg:
            out["TOOL_CFG"] = tool_cfg
        return out

    def compose(self, *args: str, tool_cfg: str | None = None, timeout: float = 900,
                check: bool = True) -> subprocess.CompletedProcess:
        return lab.compose(self.project, [COMPOSE], *args, env=self.env(tool_cfg), timeout=timeout, check=check)

    def container(self, service: str) -> str:
        return f"{self.prefix}-{service}"

    def up(self) -> None:
        self.compose("up", "-d", "--wait", "minio", "netem", "probe", timeout=600)
        from lib import netem

        self.engine_iface = netem.resolve_engine_iface(netem.docker_exec(self.container("netem")))

    def down(self) -> None:
        self.compose("--profile", "tools", "down", "-v", "--remove-orphans", timeout=300, check=False)

    def mc(self, *args: str, timeout: float = 600) -> str:
        return lab.sh(["docker", "exec", self.container("minio"), "mc", *args], timeout=timeout).stdout

    def ensure_dataset(self, files: int, file_mib: float, seed: int) -> None:
        if self.dataset == (files, file_mib):
            return
        out = self.compose("--profile", "tools", "run", "--rm", "--name", self.container("gen"), "gen", "/data",
                           str(files), str(file_mib), str(seed), timeout=1800).stdout
        manifest = json.loads(out.strip().splitlines()[-1])
        self.manifest = {name: (int(size), sha) for name, (size, sha) in manifest.items()}
        self.dataset = (files, file_mib)

    def rx_bytes(self) -> dict[str, int]:
        out = lab.sh(["docker", "exec", self.container("netem"), "cat",
                      f"/sys/class/net/{self.engine_iface}/statistics/rx_bytes"]).stdout
        return {"rx_bytes": int(out.strip())}

    def readback(self, bucket: str) -> tuple[list[tuple[str, int, str]], dict[str, float]]:
        """(key, size, sha256) of every object in the bucket, read through the store, and LastModified."""
        from compare_common import checker

        listing = [json.loads(line) for line in self.mc("ls", "--json", "--recursive", f"lab/{bucket}").splitlines()
                   if line.strip()]
        files = [entry for entry in listing if entry.get("type") == "file"]
        keys = [entry["key"] for entry in files]
        sums: dict[str, str] = {}
        for start in range(0, len(keys), 64):
            chunk = keys[start:start + 64]
            script = 'b="$1"; shift; for k in "$@"; do printf "%s\\t" "$k"; mc cat "lab/$b/$k" | sha256sum; done'
            out = lab.sh(["docker", "exec", self.container("minio"), "sh", "-c", script, "sh", bucket, *chunk],
                         timeout=1800).stdout
            for line in out.splitlines():
                key, _, rest = line.partition("\t")
                sums[key] = rest.split()[0] if rest.split() else ""
        objects = [(entry["key"], int(entry.get("size", 0)), sums.get(entry["key"], "")) for entry in files]
        modified = {entry["key"]: checker.parse_time(entry["lastModified"]) for entry in files
                    if entry.get("lastModified")}
        return objects, modified


# ------------------------------------------------------------------- tools


def render_config(tool: str, settings: dict[str, Any], bucket: str, out_dir: str, file_bytes: int) -> str | None:
    """The tool's config directory for this run (TOOL_CFG), or None where the tool has no file."""
    os.makedirs(out_dir, exist_ok=True)
    if tool == "vtop":
        with open(os.path.join(REPO, CONFIGS["vtop"]), encoding="utf-8") as handle:
            text = handle.read()
        concurrency = transports = ""
        if settings:
            width = int(settings.get("max_concurrency", 8))
            concurrency = f"  max_concurrent_batches: {width}\n"
            transports = ("  transports:\n    tcp_tls:\n" +
                          "".join(f"      {key}: {int(value)}\n" for key, value in sorted(settings.items())))
        text = (text.replace("@MAX_BYTES@", str(file_bytes + 1024 * 1024)).replace("@BUCKET@", bucket)
                .replace("@PREFIX@", "").replace("@CONCURRENCY@", concurrency).replace("@TRANSPORTS@", transports))
        with open(os.path.join(out_dir, "config.yaml"), "w", encoding="utf-8") as handle:
            handle.write(text)
        with open(os.path.join(HERE, "tools", "vtop", "run.sh"), encoding="utf-8") as src, \
                open(os.path.join(out_dir, "run.sh"), "w", encoding="utf-8") as dst:
            dst.write(src.read())
        return out_dir
    if tool == "aws-cli":
        with open(os.path.join(REPO, CONFIGS["aws-cli"]), encoding="utf-8") as handle:
            text = handle.read()
        for key in ("max_concurrent_requests", "multipart_threshold", "multipart_chunksize"):
            if key in settings:
                text += f"  {key} = {int(settings[key])}\n"
        with open(os.path.join(out_dir, "config"), "w", encoding="utf-8") as handle:
            handle.write(text)
        return out_dir
    if tool == "rclone":
        with open(os.path.join(REPO, CONFIGS["rclone"]), encoding="utf-8") as src, \
                open(os.path.join(out_dir, "rclone.conf"), "w", encoding="utf-8") as dst:
            dst.write(src.read())
        return out_dir
    return None


def s5cmd_base_args() -> list[str]:
    with open(os.path.join(REPO, CONFIGS["s5cmd"]), encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip() and not line.lstrip().startswith("#")]


def tool_command(tool: str, settings: dict[str, Any], bucket: str, files: int) -> list[str]:
    if tool == "vtop":
        return [str(files)]
    if tool == "aws-cli":
        return ["s3", "cp", "--recursive", "--no-progress", "--endpoint-url", "http://netem:9200", "/data/files/",
                f"s3://{bucket}/"]
    if tool == "rclone":
        flags = []
        for key, flag in (("transfers", "--transfers"), ("upload_concurrency", "--s3-upload-concurrency"),
                          ("chunk_size", "--s3-chunk-size"), ("upload_cutoff", "--s3-upload-cutoff")):
            if key in settings:
                value = int(settings[key])
                flags.append(f"{flag}={value}" if key in ("transfers", "upload_concurrency") else f"{flag}={value}B")
        return ["copy", "/data/files", f"lab:{bucket}", *flags]
    if tool == "s5cmd":
        top = s5cmd_base_args()
        if "numworkers" in settings:
            top.append(f"--numworkers={int(settings['numworkers'])}")
        cp = ["cp"]
        if "concurrency" in settings:
            cp.append(f"--concurrency={int(settings['concurrency'])}")
        if "part_size" in settings:
            cp.append(f"--part-size={int(settings['part_size'])}")
        return [*top, *cp, "/data/files/*", f"s3://{bucket}/"]
    raise ValueError(tool)


class ToolRun:
    """One tool container, from `docker compose run` to its exit, with its cgroup sampled."""

    def __init__(self, lab_: Lab, tool: str, settings: dict[str, Any], bucket: str, files: int, file_bytes: int,
                 cfg_dir: str, timeout: float):
        self.lab, self.tool, self.timeout = lab_, tool, timeout
        self.name = f"{lab_.prefix}-{tool}-run"
        self.cfg = render_config(tool, settings, bucket, cfg_dir, file_bytes)
        self.command = tool_command(tool, settings, bucket, files)
        self.started = self.ended = 0.0
        self.code: Any = None
        self.output = ""
        self.done_at: float | None = None
        self.sampler = cgroup.Sampler(self.name, docker_api.stats, interval=0.5)

    def run(self) -> None:
        argv = ["docker", "compose", "-p", self.lab.project, "-f", COMPOSE, "--profile", "tools", "run", "--rm",
                "--name", self.name, self.tool, *self.command]
        env = dict(os.environ, **self.lab.env(self.cfg))
        with self.sampler:
            self.started = time.time()
            try:
                proc = subprocess.run(argv, capture_output=True, text=True, timeout=self.timeout, env=env)
                self.code = proc.returncode
                self.output = (proc.stdout + proc.stderr)[-4000:]
                for line in proc.stdout.splitlines():
                    if line.startswith("done_at="):
                        self.done_at = float(line.split()[0].split("=", 1)[1])
            except subprocess.TimeoutExpired:
                self.code = "timeout"
                subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
            self.ended = time.time()

    def cpu_seconds(self) -> float | None:
        samples = self.sampler.samples
        if len(samples) < 2:
            return None
        return max(s.cpu_ns for s in samples) / 1e9


# --------------------------------------------------------------- one run


def link_shape(lab_: Lab, profile: str):
    """(the tool's shape, the calibration's shape), None on lan."""
    import egress_baseline
    from lib import netem

    if profile == "lan":
        return None, None
    shape = replace(netem.NetemShape.from_scenario(egress_baseline.PROFILES[profile]),
                    container=lab_.container("netem"))
    calibration = shape if profile == link.POLICED else shape.rate_calibration_twin()
    return shape, calibration


def calibrate(lab_: Lab, shape) -> float:
    from lib import netem

    probe = netem.docker_exec(lab_.container("probe"))
    if shape is None:
        return netem.probe_throughput_mbps(probe, "netem", CALIBRATION_SECONDS, CALIBRATION_OMIT)
    with netem.shaped(shape, run=netem.docker_exec(lab_.container("netem")), log=lambda _m: None):
        return netem.probe_throughput_mbps(probe, "netem", CALIBRATION_SECONDS, CALIBRATION_OMIT)


@contextlib.contextmanager
def shaped_or_not(lab_: Lab, shape):
    from lib import netem

    if shape is None:
        yield
        return
    with netem.shaped(shape, run=netem.docker_exec(lab_.container("netem")), log=log):
        yield


def settings_for(item: plans.Planned, plan: dict[str, Any], csv_path: str, files: int, file_bytes: int
                 ) -> tuple[dict[str, Any], int | None]:
    """(settings, grid size) for this run's cell."""
    grids = plan.get("grids") or {}
    grid_size = link.check_grids(grids, plan["systems"]) if grids else None
    if item.cell == "equal-conns":
        target = int(item.variant["connections"])
        settings = link.equal_connections(item.system, target)
        link.check_connections(item.system, settings, target, file_bytes, files)
        return settings, None
    if item.cell == "tuned-grid":
        index = int(str(item.variant["grid_point"]).lstrip("g")) - 1
        return dict(grids[item.system][index]), grid_size
    if item.cell == "tuned":
        return best_grid_point(csv_path, item, grids), grid_size
    return {}, None


def best_grid_point(csv_path: str, item: plans.Planned, grids: dict[str, Any]) -> dict[str, Any]:
    rows = schema.read_rows(csv_path, KIND)
    candidates = [r for r in rows if r["cell"] == "tuned-grid" and r["system"] == item.system and r["status"] == "ok"
                  and r["link_profile"] == item.variant["link_profile"]
                  and r["objects_committed"] == r["files_expected"] and r["goodput_fraction"] != ""]
    if not candidates:
        raise runner.CellRefused(f"no {item.system} grid point on {item.variant['link_profile']} committed every "
                                 "file, so there is no best point to re-run")
    best = max(candidates, key=lambda r: (float(r["goodput_fraction"]), -int(r["grid_point"].lstrip("g"))))
    return dict(grids[item.system][int(best["grid_point"].lstrip("g")) - 1])


def execute(lab_: Lab, env: dict[str, Any], item: plans.Planned, counter: list[int]) -> dict[str, Any]:
    files = int(item.params.get("files", 48))
    file_mib = float(item.params.get("file_mib", 20))
    file_bytes = int(file_mib * 1024 * 1024)
    profile = item.variant["link_profile"]
    hosted = env["comparable"]["host_class"] == "hosted"
    csv_path = os.path.join(lab_.run_dir, "link.csv")
    try:
        settings, grid_size = settings_for(item, env["plan"], csv_path, files, file_bytes)
    except link.LinkRefused as exc:
        raise runner.CellRefused(str(exc)) from exc
    lab_.ensure_dataset(files, file_mib, int(item.params.get("seed", SEED)))
    counter[0] += 1
    bucket = f"{lab_.prefix}-{counter[0]:04d}".lower()
    lab_.mc("mb", "--ignore-existing", f"lab/{bucket}")
    cfg_dir = os.path.join(lab_.run_dir, "configs", f"{counter[0]:04d}-{item.system}")
    tool = ToolRun(lab_, item.system, settings, bucket, files, file_bytes, cfg_dir,
                   timeout=float(item.params.get("timeout_minutes", 60)) * 60)
    shape, calibration_shape = link_shape(lab_, profile)
    out: dict[str, Any] = {"settings": json.dumps(settings, sort_keys=True), "grid_size": grid_size or ""}
    if item.cell == "equal-conns":
        out["connections_actual"] = link.connections_of(item.system, settings, file_bytes, files)
    before_mbps = calibrate(lab_, calibration_shape)
    with shaped_or_not(lab_, shape):
        if item.cell == "neighbour":
            out.update(neighbour(lab_, tool, int(item.params.get("neighbour_seconds", 120))))
        else:
            counters_before = lab_.rx_bytes()
            tool.run()
            counters_after = lab_.rx_bytes()
            wire = link.wire_bytes(counters_before, counters_after)
            out["wire_bytes"] = wire
    after_mbps = calibrate(lab_, calibration_shape)
    out.update({"calib_before_mbps": before_mbps, "calib_after_mbps": after_mbps})
    try:
        reference = link.check_calibrations(profile, before_mbps, after_mbps)
    except link.LinkRefused as exc:
        raise runner.CellRefused(str(exc)) from exc
    objects, modified = lab_.readback(bucket)
    judged = link.judge_readback(lab_.manifest, objects)
    end = tool.done_at if tool.done_at else tool.ended
    seconds = max(end - tool.started, 1e-3)
    committed_keys = [key for key, size, sha in objects if (size, sha) in set(lab_.manifest.values())]
    last_visible = max((modified[key] for key in committed_keys if key in modified), default=None)
    out.update({
        "files_expected": judged.files_expected, "objects_committed": judged.objects_committed,
        "objects_mismatched": judged.objects_mismatched, "objects_extra": judged.objects_extra,
        "payload_bytes": judged.payload_bytes, "seconds_to_complete": f"{seconds:.2f}",
        "link_reference_mbps": reference, "exit_code": tool.code,
    })
    goodput = judged.payload_bytes * 8 / 1e6 / seconds
    out["goodput_mbps"] = f"{goodput:.3f}"
    out["goodput_fraction"] = f"{goodput / reference:.4f}"
    if last_visible is not None:
        upload_seconds = max(last_visible - tool.started, 1e-3)
        out["upload_seconds"] = f"{upload_seconds:.2f}"
        if item.system == "vtop":
            out["vtop_upload_goodput_fraction"] = \
                f"{judged.payload_bytes * 8 / 1e6 / upload_seconds / reference:.4f}"
    if "wire_bytes" in out and judged.payload_bytes:
        out["wire_bytes_per_payload_byte"] = f"{out['wire_bytes'] / judged.payload_bytes:.4f}"
    cpu = tool.cpu_seconds()
    if cpu is not None:
        share = cpu / (seconds * TOOL_CPUS)
        out["cpu_share"] = f"{share:.4f}"
        out["bottleneck"] = link.bottleneck(profile, share)
        out["cpu_seconds"] = "" if hosted else f"{cpu:.2f}"
        if not hosted and judged.payload_bytes:
            out["cpu_seconds_per_gib"] = f"{cpu / (judged.payload_bytes / 2**30):.2f}"
    else:
        out["bottleneck"] = "unknown"
    if hosted and item.cell == "committed" and out.get("bottleneck") != "link":
        out.update({"status": "refused", "note": f"a hosted row needs the link as its bottleneck; recorded "
                                                 f"{out.get('bottleneck')!r} (cpu_share {out.get('cpu_share')!r})"})
    if tool.code not in (0, None):
        out["note"] = f"{item.system} exited {tool.code}: {' '.join(tool.output.split())[-400:]}"
    if item.cell == "smoke" and judged.objects_committed != judged.files_expected:
        out["status"] = "failed"
        out["note"] = (f"{judged.objects_committed} of {judged.files_expected} files committed (missing "
                       f"{', '.join(judged.missing[:5])}); " + out.get("note", ""))
    with open(os.path.join(cfg_dir, "tool-output.txt"), "w", encoding="utf-8") as handle:
        handle.write(f"$ {shlex.join(tool.command)}\nexit {tool.code}\n{tool.output}\n")
    lab_.mc("rb", "--force", f"lab/{bucket}")
    return out


def neighbour(lab_: Lab, tool: ToolRun, seconds: int) -> dict[str, Any]:
    """The #478 instrument: an iperf3 flow alone, beside the tool, and alone again."""
    from lib import competitor

    spec = competitor.CompetitorSpec(mode="bulk", seconds=seconds, container=lab_.container("probe"))
    run_probe = competitor.docker_exec(spec.container, spec.collection_timeout())
    alone_before = competitor.measure(spec, run_probe, "alone before").mbps
    thread = threading.Thread(target=tool.run, daemon=True)
    thread.start()
    time.sleep(15)  # the tool's ramp, outside the shared window
    counters_before = lab_.rx_bytes()
    with_tool = competitor.measure(spec, run_probe, "with the tool")
    counters_after = lab_.rx_bytes()
    still_running = thread.is_alive()
    thread.join()
    if not still_running:
        raise runner.CellRefused("the tool finished inside the shared window, so the window was not contended "
                                 "throughout; give it more files or a shorter neighbour_seconds")
    alone_after = competitor.measure(spec, run_probe, "alone after").mbps
    if competitor.solo_agreement(alone_before, alone_after) > link.CALIBRATION_TOLERANCE:
        raise runner.CellRefused(f"the neighbour alone measured {alone_before} Mbit/s before and {alone_after} after: "
                                 "the link changed under the run")
    tool_bytes = max(link.wire_bytes(counters_before, counters_after) - with_tool.bytes, 0)
    tool_mbps = tool_bytes * 8 / 1e6 / with_tool.seconds
    return {"neighbour_alone_before_mbps": alone_before, "neighbour_with_mbps": with_tool.mbps,
            "neighbour_alone_after_mbps": alone_after, "tool_with_mbps": f"{tool_mbps:.3f}",
            "jain_index": link.jain(with_tool.mbps, tool_mbps)}


# --------------------------------------------------------------------- main


def describe_for(images: dict[str, str], configs: dict[str, Any]):
    def describe(item: plans.Planned) -> dict[str, str]:
        reg = registry.load()
        return {"system_version": registry.product(reg, item.system)["version"] if item.system != "vtop"
                else environment.git("rev-parse", "--short=12", "HEAD"),
                "image_digest": images[item.system],
                "config_sha256": configs[item.system][item.profile]["sha256"]}
    return describe


def telemetry_lines() -> dict[str, str]:
    out = {}
    for system, rel in CONFIGS.items():
        with open(os.path.join(REPO, rel), encoding="utf-8") as handle:
            lines = [line.strip("# \n") for line in handle if line.lstrip("# ").lower().startswith("telemetry:")]
        out[system] = lines[0] if lines else ""
    return out


def check_run(run_dir: str) -> int:
    try:
        env = schema.read_environment(run_dir)
        rows = schema.read_rows(os.path.join(run_dir, "link.csv"), KIND)
        problems = schema.check_rows(rows, KIND, env)
        extra, missing = schema.check_completeness(rows, KIND, env["plan"])
    except schema.RunRefused as exc:
        print(f"check refused: {exc}", file=sys.stderr)
        return 2
    expected = len(plans.expected_keys(env["plan"], KIND))
    print(f"{expected - len(missing)} of {expected} expected rows present")
    for problem in problems + extra:
        print(f"problem: {problem}")
    for key in missing:
        print(f"missing: {key}")
    return 0 if not (problems or extra or missing) else 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--plan", default="smoke", help="smoke, hosted or full (plans.json)")
    parser.add_argument("--tools", default=None, help="a comma list, a subset of the plan's systems")
    parser.add_argument("--host-class", default=None, help="override the plan's host class (laptop, hosted, "
                                                           "dedicated); the smoke plan is laptop by default")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--resume", default=None, metavar="RUN_ID")
    parser.add_argument("--budget-minutes", type=float, default=None)
    parser.add_argument("--cooldown-seconds", type=float, default=0.0)
    parser.add_argument("--max-hours", type=float, default=48.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--private", action="store_true")
    parser.add_argument("--rebuild", action="store_true", help="rebuild the VTOP image")
    parser.add_argument("--vtop-image", default=None,
                        help="use this VTOP image instead of building docker/Dockerfile (a smoke on a laptop "
                             "with a prebuilt engine); its id is recorded like any other")
    parser.add_argument("--lab-prefix", default="vtop-link", help="container and bucket prefix")
    parser.add_argument("--check", default=None, metavar="RUN_DIR")
    parser.add_argument("--keep-lab", action="store_true", help="leave the lab up afterwards")
    args = parser.parse_args(argv)
    if args.check:
        return check_run(args.check)
    try:
        plan = plans.select(plans.load_plans(PLANS), args.plan)
        if args.tools:
            wanted = [tool.strip() for tool in args.tools.split(",") if tool.strip()]
            unknown = sorted(set(wanted) - set(plan["systems"]))
            if unknown:
                raise plans.PlanRefused(f"--tools names {unknown}, not in the plan's {plan['systems']}")
            plan["systems"] = [system for system in plan["systems"] if system in wanted]
        if args.host_class:
            plan["host_class"] = args.host_class
        plans.check_plan(plan, KIND)
        link.check_grids(plan.get("grids") or {}, plan["systems"])
        hours = plans.check_budget(plan, KIND, args.max_hours, args.cooldown_seconds)
    except (plans.PlanRefused, link.LinkRefused) as exc:
        print(f"plan refused: {exc}", file=sys.stderr)
        return 2
    print(plans.describe(plan, KIND, args.cooldown_seconds))
    if args.dry_run:
        return 0
    reg = registry.load()
    try:
        registry.check(reg)
        for system in plan["systems"]:
            registry.require_runnable(reg, system, private=args.private, use_case="c")
    except registry.RegistryError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    drift = environment.config_drift(list(CONFIGS.values()) + list(EXTRA_TRACKED))
    if drift:
        print(f"refused: tool configuration differs from HEAD ({', '.join(drift)}); the CSV records the committed "
              "file's hash, so commit it first", file=sys.stderr)
        return 2
    lab.ensure_minio_image()
    if "vtop" not in plan["systems"]:
        vtop_image = "vtop-compare:unused"
    elif args.vtop_image:
        vtop_image = args.vtop_image
    else:
        vtop_image = lab.ensure_vtop_image(args.rebuild)
    images = {}
    for system in plan["systems"]:
        if system == "vtop":
            images[system] = lab.image_id(vtop_image)
        else:
            ref = registry.product(reg, system)["image"]
            lab.ensure_image(ref)
            images[system] = ref.split("@", 1)[1]
    configs = environment.record_configs({system: {profile: CONFIGS[system] for profile in KIND.profiles[system]}
                                          for system in plan["systems"]})
    import egress_baseline

    comparable = environment.host_facts(probe_disk=plan["host_class"] != "hosted")
    comparable.update({
        "host_class": plan["host_class"], "vtop_commit": environment.git("rev-parse", "HEAD"),
        "vtop_engine_tree": lab.engine_tree(), "vtop_image": vtop_image if "vtop" in plan["systems"] else "",
        "vtop_image_id": images.get("vtop", ""),
        "tool_images": {system: images[system] for system in plan["systems"]}, "tool_limits": TOOL_LIMITS,
        "middlebox_image": "nicolaka/netshoot:v0.16", "link_profiles": egress_baseline.PROFILES,
        "store_image_id": lab.image_id(lab.MINIO_IMAGE),
    })
    run_id = lab.check_run_id(args.resume or args.run_id or lab.new_run_id("link"))
    run_dir = os.path.join(RESULTS, run_id)
    if args.resume:
        env = schema.read_environment(run_dir)
        try:
            runner.check_resume(env, comparable)
        except schema.RunRefused as exc:
            print(f"refused: {exc}", file=sys.stderr)
            return 2
    else:
        if os.path.exists(run_dir):
            print(f"refused: {run_dir} exists; a run never overwrites another", file=sys.stderr)
            return 2
        os.makedirs(run_dir)
        env = environment.new_environment("link", run_id, plan, comparable, configs, telemetry=telemetry_lines(),
                                          private=args.private,
                                          extra={"projected_hours": round(hours, 2), "seed": SEED})
    environment.save(run_dir, env)
    the_lab = Lab(args.lab_prefix, vtop_image, run_dir)
    counter = [0]
    try:
        the_lab.down()
        the_lab.up()
        outcome = runner.run_plan(run_dir, KIND, env, describe_for(images, configs),
                                  lambda item: execute(the_lab, env, item, counter),
                                  budget_minutes=args.budget_minutes, cooldown_seconds=args.cooldown_seconds, log=log)
    finally:
        if not args.keep_lab:
            the_lab.down()
    if outcome.remaining == 0:
        env["run_state"] = "complete"
        env["finished_at"] = environment.utc_now()
    environment.save(run_dir, env)
    log(f"{outcome.done} runs done, {outcome.skipped} already there, {outcome.remaining} remaining; "
        f"results in {os.path.relpath(run_dir, REPO)}")
    return 0 if outcome.remaining == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
