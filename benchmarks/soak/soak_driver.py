#!/usr/bin/env python3
"""The soak driver (#726): bring the lab up, load it, break it on a schedule,
watch it, and decide.

    python3 benchmarks/soak/soak_driver.py --profile ci-smoke --out /tmp/soak
    python3 benchmarks/soak/soak_driver.py --profile 72h --out /srv/soak \\
        --data-dir /srv/soak/data --build-engine-from vtop-engine:local

What it does, in order:

  1. preflight: Docker, free disk, the profile, the engine image
  2. the lab: Kafka, MinIO and toxiproxy up; the topic created; the producer
     started at the profile's rate; the engine started
  3. the run, for `duration_s`: a sampler records the engine's memory, open
     files, work directory, ledger and counters; the archive lag is read from
     the broker; `prune-ledger` and a sampled `scrub` run on their schedules;
     the fault plan is injected, and after each fault the engine must catch up
     and `audit-coverage` must find no hole
  4. the end: the producer stops, the engine drains, then `audit-coverage
     --check-storage`, a full `scrub` and the committed offsets are checked
     against the topic's end, and the engine is stopped with SIGTERM
  5. the verdict: soak_eval over what was recorded, an evidence tarball, and
     the lab torn down

Exit status: 0 passed, 1 the run did not pass, 2 it could not start.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import shutil
import socket
import sys
import tarfile
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))  # benchmarks/, for lib.prom_text

import soak_eval  # noqa: E402
import soak_faults  # noqa: E402
import soak_lab  # noqa: E402
import soak_profile  # noqa: E402
from soak_record import Recorder, RunState  # noqa: E402

from lib import prom_text  # noqa: E402

# The engine's own counters the samples keep, each summed over its labels.
# Everything else the engine exposes is in the hourly raw snapshots.
SAMPLED_FAMILIES = (
    "vtop_commits_total",
    "vtop_verified_total",
    "vtop_verification_failures_total",
    "vtop_failed_total",
    "vtop_records_total",
    "vtop_bytes_in_total",
    "vtop_bytes_out_total",
    "vtop_batches_total",
    "vtop_source_read_errors_total",
    "vtop_inflight_batches",
    "vtop_batch_duration_seconds_sum",
    "vtop_batch_duration_seconds_count",
)

# Unplanned engine exits tolerated (each is restarted and recorded) before the
# run gives up: a crash loop proves nothing more with each lap.
MAX_UNPLANNED_EXITS = 3
MAX_CONSECUTIVE_FAULT_FAILURES = 3


def summed_families(text: str, names=SAMPLED_FAMILIES) -> dict[str, float]:
    """The listed families of one /metrics scrape, each summed over its label
    sets. A family the engine has not emitted yet is left out, not zero."""
    exposition = prom_text.parse(text)
    totals: dict[str, float] = {}
    for series in exposition.series.values():
        if series.name in names:
            totals[series.name] = totals.get(series.name, 0.0) + series.value
    return totals


def render_engine_config(profile: dict) -> str:
    """The engine's config.yaml for the lab: one Kafka topic to the lab's MinIO
    through the proxy, a SQLite ledger and the profile's batching."""
    e = profile["engine"]
    return f"""# Written by benchmarks/soak/soak_driver.py for the {profile["name"]} soak profile.
engine:
  name: vtop-soak
  tenant: soak
  state_store: "sqlite:///data/state/state.db"
  work_dir: "/data/work"
  log_level: info
batching:
  max_records: {e["max_records"]}
  max_bytes: {e["max_bytes"]}
  max_batch_age_seconds: {e["max_batch_age_seconds"]}
  source_poll_wait_ms: 250
  idle_poll_interval_ms: 500
  max_concurrent_batches: {e["max_concurrent_batches"]}
compression:
  type: {e["compression"]}
  level: {e["compression_level"]}
checksum:
  algorithm: sha256
sources:
  kafka:
    enabled: true
    bootstrap_servers: ["toxiproxy:{soak_lab.PROXY_PORTS["kafka"]}"]
    consumer_group: "{soak_lab.GROUP}"
    topic_include_regex: "^{soak_lab.TOPIC}$"
    auto_offset_reset: earliest
    enable_auto_commit: false
upload:
  backend: s3_native
  bucket: "{soak_lab.BUCKET}"
  prefix: "soak"
  endpoint_url: "http://toxiproxy:{soak_lab.PROXY_PORTS["minio"]}"
  region: us-east-1
  force_path_style: true
  verify_tls: false
  create_bucket: true
  require_strong_verification: true
"""


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# A structured log line at a level an operator reads past. vtopctl logs as JSON
# when asked for a JSON report, on the stream the report's errors use too.
_ROUTINE_LOG = re.compile(r'^\{"timestamp":.*"level":"(?:TRACE|DEBUG|INFO|WARN)"')


def tail(text: str, lines: int = 12) -> str:
    """The last lines of a command's output that say something: routine log
    lines are dropped, so an error that follows them is not pushed out."""
    kept = [line for line in text.strip().splitlines() if not _ROUTINE_LOG.match(line)]
    return "\n".join(kept[-lines:])


class Driver:
    def __init__(
        self, profile: dict, lab: soak_lab.Lab, run_dir: Path, rec: Recorder, state: RunState, disk_path: Path
    ):
        self.profile = profile
        self.lab = lab
        self.run_dir = run_dir
        self.rec = rec
        self.state = state
        self.disk_path = disk_path
        self.stop = threading.Event()
        self.restart_lock = threading.Lock()
        self.producer_stopping = False
        self.finals: dict = {}
        self.ctx = soak_faults.Context(lab=lab, profile=profile, rec=rec, state=state)
        # How often recovery is polled: each poll runs a Kafka tool.
        self.poll_s = max(2.0, min(10.0, profile["lag_interval_s"] / 3))

    # -- background threads --------------------------------------------

    def _loop(self, interval: float, body) -> None:
        due = time.monotonic()
        while not self.stop.is_set():
            try:
                body()
            except Exception as error:  # the observer must outlive what it observes
                self.rec.event("observer_error", where=body.__name__, detail=f"{type(error).__name__}: {error}"[:300])
            due += interval
            delay = due - time.monotonic()
            if delay < 0:  # a slow tick: skip the ticks it overran, do not burst
                due = time.monotonic()
                delay = 0
            self.stop.wait(delay)

    def sample_tick(self) -> None:
        lab, rec, state = self.lab, self.rec, self.state
        row: dict = {"lifetime": state.current_lifetime(), "engine_up": False}
        running = lab.running("engine")
        if running:
            try:
                row.update(lab.engine_probe())
                row["engine_up"] = True
                text = lab.scrape_metrics()
                row["metrics"] = summed_families(text)
                self._maybe_raw(text)
            except (soak_lab.LabError, ValueError, prom_text.PromParseError) as error:
                row["sample_error"] = str(error)[:200]
        else:
            self._engine_down()
        row["disk_free_bytes"] = shutil.disk_usage(self.disk_path).free
        rec.sample(**row)

    _raw_due = 0.0

    def _maybe_raw(self, text: str) -> None:
        now = self.rec.elapsed()
        if now < self._raw_due:
            return
        self._raw_due = now + self.profile["raw_snapshot_interval_s"]
        raw = self.run_dir / "raw"
        raw.mkdir(exist_ok=True)
        with gzip.open(raw / f"metrics-{int(now):07d}s.txt.gz", "wt") as handle:
            handle.write(text)

    def _engine_down(self) -> None:
        """The engine is not running. If a fault did not stop it, that is a
        finding: record it, start the engine again, and give up after a few."""
        if self.state.is_down_planned() or self.state.aborted():
            return
        with self.restart_lock:
            if self.state.is_down_planned() or self.lab.running("engine"):
                return
            info = self.lab.state("engine")
            self.state.unplanned_exits += 1
            self.rec.event(
                "engine_exit",
                lifetime=self.state.current_lifetime(),
                planned=False,
                exit_code=info.get("exit_code"),
                oom_killed=info.get("oom_killed"),
            )
            if self.state.unplanned_exits > MAX_UNPLANNED_EXITS:
                self.state.request_abort(
                    f"the engine exited {self.state.unplanned_exits} times without a fault stopping it"
                )
                return
            self.state.engine_started()
            self.rec.event("engine_start", lifetime=self.state.current_lifetime(), reason="unplanned")
            self.lab.start_engine(timeout=self.profile["recovery_timeout_s"])

    def lag_tick(self) -> None:
        lab, p = self.lab, self.profile
        if not lab.running("producer") and not self.producer_stopping and not self.stop.is_set():
            self.rec.event("producer_restart", reason="the producer was not running")
            try:
                lab.start_producer()
            except soak_lab.LabError as error:
                self.rec.event("observer_error", where="producer_restart", detail=str(error)[:300])
        try:
            lag = lab.lag(p["partitions"])
            self.rec.sample(lifetime=self.state.current_lifetime(), lag=lag)
        except soak_lab.LabError as error:
            self.rec.sample(lifetime=self.state.current_lifetime(), lag=None, lag_error=str(error)[:200])

    # -- periodic work -------------------------------------------------

    def prune(self) -> None:
        # Zero days: every committed row older than now goes, except the newest
        # of each source, which recovery needs. `prune-ledger` is the schedule
        # that keeps the ledger flat.
        result = self.lab.vtopctl("prune-ledger", "--older-than-days", "0", "--limit", "500", timeout=300)
        deleted = soak_lab.parse_pruned(result.out) if result.ok else None
        self.rec.event("prune", ok=result.ok, deleted=deleted, detail=tail(result.text()))

    def scrub_sample(self) -> None:
        n = self.profile["scrub_sample_batches"]
        result = self.lab.vtopctl("scrub", "--sample", str(n), timeout=600, json_report=True)
        summary = self._scrub_summary(result)
        self.rec.event(
            "scrub", mode="sample", ok=result.ok and summary.get("passed", False), detail=tail(result.text()), **summary
        )

    @staticmethod
    def _scrub_summary(result: soak_lab.Result) -> dict:
        try:
            return soak_lab.scrub_summary(soak_lab.parse_json_report(result.out))
        except ValueError:
            return {}

    def audit(self, trigger: str, storage: bool = False) -> dict:
        args = ["audit-coverage"] + (["--check-storage"] if storage else [])
        result = self.lab.vtopctl(*args, timeout=self.profile["check_timeout_s"])
        # The audit exits 0 over a gap in a Kafka partition (compaction and
        # transaction markers can explain one). This workload has neither, so
        # a gap it reports is a record nothing archived.
        gaps = soak_lab.audit_gaps(result.out)
        ok = result.ok and not gaps
        detail = tail(result.text(), 20)
        if gaps:
            detail = f"{len(gaps)} unexplained gap(s): {'; '.join(gaps[:5])}\n{detail}"
        self.rec.event("audit", trigger=trigger, ok=ok, storage=storage, gaps=len(gaps), detail=detail[:2000])
        return {"ok": ok, "exit": result.code, "gaps": len(gaps), "detail": detail[:4000]}

    # -- faults --------------------------------------------------------

    def run_fault(self, fault: soak_profile.PlannedFault, occurrence: int, late_s: float) -> bool:
        rec = self.rec
        rec.event(
            "fault_start",
            id=fault.id,
            fault=fault.kind,
            planned_at_s=fault.at_s,
            late_s=round(late_s, 1),
            hold_s=fault.hold_s,
        )
        try:
            detail = soak_faults.inject(self.ctx, fault, occurrence)
        except (soak_faults.FaultError, soak_lab.LabError) as error:
            rec.event("fault_end", id=fault.id, fault=fault.kind, ok=False, detail=str(error)[:500])
            return False
        rec.event("fault_end", id=fault.id, fault=fault.kind, ok=True, detail=detail)
        ok, why, seconds = self.wait_recovered()
        rec.event("recovered", id=fault.id, fault=fault.kind, ok=ok, recovery_s=round(seconds, 1), detail=why)
        if ok:
            self.audit(f"fault:{fault.id}")
        return ok

    def wait_recovered(self) -> tuple[bool, str, float]:
        """After a fault: the engine runs, serves /metrics, the producer runs,
        the group's committed offset has moved, and the archive lag is back
        under the steady-state bound."""
        lab, p = self.lab, self.profile
        started = time.monotonic()
        deadline = started + p["recovery_timeout_s"]
        cap_s = p["criteria"]["max_steady_lag_p95_s"]
        baseline: int | None = None
        last = "no reading"
        while time.monotonic() < deadline:
            if self.state.aborted():
                return False, f"aborted: {self.state.aborted()}", time.monotonic() - started
            try:
                if not lab.running("producer"):
                    lab.start_producer()
                lab.scrape_metrics()
                lag = lab.lag(p["partitions"])
                if baseline is None:
                    baseline = lag["committed"]
                lag_s = (lag["end"] - lag["committed"]) / p["rate_records_per_s"]
                last = f"committed {lag['committed']} (was {baseline}), lag {lag_s:.1f} s, bound {cap_s} s"
                if lag["committed"] > baseline and lag_s <= cap_s:
                    return True, last, time.monotonic() - started
            except soak_lab.LabError as error:
                last = str(error)[:200]
            time.sleep(self.poll_s)
        return False, f"not recovered within {p['recovery_timeout_s']} s: {last}", time.monotonic() - started

    # -- the run -------------------------------------------------------

    def schedule(self) -> list[tuple[float, int, str, object]]:
        """(due, priority, kind, payload) for everything the run does, by due
        time. Faults come before the periodic work that falls at the same time."""
        p = self.profile
        items: list[tuple[float, int, str, object]] = []
        occurrences: dict[str, int] = {}
        for fault in soak_profile.build_plan(p):
            occurrences[fault.kind] = occurrences.get(fault.kind, 0) + 1
            items.append((float(fault.at_s), 0, "fault", (fault, occurrences[fault.kind] - 1)))
        for kind, interval, priority in (("prune", p["prune_interval_s"], 1), ("scrub", p["scrub_interval_s"], 2)):
            at = interval
            while at < p["duration_s"]:
                items.append((float(at), priority, kind, None))
                at += interval
        return sorted(items, key=lambda item: (item[0], item[1]))

    def sleep_until(self, elapsed: float) -> None:
        while not self.state.aborted():
            remaining = elapsed - self.rec.elapsed()
            if remaining <= 0:
                return
            time.sleep(min(remaining, 1.0))

    def main_loop(self) -> None:
        p = self.profile
        failures = 0
        next_periodic: dict[str, float] = {}
        for due, _prio, kind, payload in self.schedule():
            if self.state.aborted():
                return
            self.sleep_until(due)
            if self.state.aborted():
                return
            now = self.rec.elapsed()
            if kind == "fault":
                fault, occurrence = payload  # type: ignore[misc]
                ok = self.run_fault(fault, occurrence, max(0.0, now - due))
                failures = 0 if ok else failures + 1
                if failures >= MAX_CONSECUTIVE_FAULT_FAILURES:
                    self.state.request_abort(f"{failures} faults in a row did not land or did not recover")
            else:
                # A prune or scrub that fell due while a fault ran is skipped
                # when the next one is already due: no burst of catch-up runs.
                if next_periodic.get(kind, 0.0) > due:
                    continue
                getattr(self, "prune" if kind == "prune" else "scrub_sample")()
                interval = p["prune_interval_s"] if kind == "prune" else p["scrub_interval_s"]
                next_periodic[kind] = (int(self.rec.elapsed() // interval) + 1) * interval
        self.sleep_until(float(p["duration_s"]))

    def finish(self) -> None:
        """Stop the load, let the engine drain, and run the checks that need
        everything to have settled."""
        lab, p, rec = self.lab, self.profile, self.rec
        aborted = self.state.aborted()
        self.producer_stopping = True  # the watchdog must not start it again
        lab.docker("stop", "--time", "30", lab.container("producer"), timeout=90)
        rec.event("producer_stopped")
        drained, last = self.wait_drained(0 if aborted else p["drain_timeout_s"])
        rec.event("drain", ok=drained, detail=last)
        try:
            offsets = lab.partition_offsets(p["partitions"])
            self.finals["tail"] = {"partitions": {str(k): v for k, v in sorted(offsets.items())}}
        except soak_lab.LabError as error:
            self.finals["tail"] = {"error": str(error)[:300]}
        self.finals["audit_coverage"] = self.audit("final", storage=True)
        scrub = lab.vtopctl("scrub", timeout=p["check_timeout_s"], json_report=True)
        summary = self._scrub_summary(scrub)
        ok = scrub.ok and bool(summary.get("passed"))
        self.finals["scrub"] = {"ok": ok, "exit": scrub.code, "detail": tail(scrub.text(), 20), **summary}
        rec.event("scrub", mode="full", ok=ok, detail=tail(scrub.text()), **summary)
        try:
            self.state.begin_shutdown()
            stopped = lab.stop_engine()
            self.finals["engine_stop"] = {"exit_code": stopped.get("exit_code")}
        except soak_lab.LabError as error:
            self.finals["engine_stop"] = {"error": str(error)[:300]}

    def wait_drained(self, timeout: float) -> tuple[bool, str]:
        lab, p = self.lab, self.profile
        deadline = time.monotonic() + timeout
        last = "not read"
        while True:
            try:
                lag = lab.lag(p["partitions"])
                last = f"committed {lag['committed']} of {lag['end']}"
                if lag["committed"] == lag["end"]:
                    return True, last
            except soak_lab.LabError as error:
                last = str(error)[:200]
            if time.monotonic() >= deadline:
                return False, last
            time.sleep(self.poll_s)


# ---------------------------------------------------------------------------
# Setup and teardown
# ---------------------------------------------------------------------------


def build_engine_image(lab: soak_lab.Lab, base: str, tag: str, profile: dict) -> None:
    """Layer the soak's additions over a built engine image: the clock-skew
    shim (compiled from live-chaos scenario 07's own source, once per step the
    profile uses) and the entrypoint that loads it."""
    steps = sorted({s for s in profile["clock_step_seconds"]})
    with tempfile.TemporaryDirectory(prefix="vtop-soak-build-") as tmp:
        context = Path(tmp)
        for name in ("Dockerfile.engine", "soak-entrypoint.sh"):
            shutil.copy(HERE / name, context / name)
        (context / "realtime-skew.c").write_text(soak_lab.extract_skew_shim_source())
        lab.require(
            lab.docker(
                "build",
                "--file",
                str(context / "Dockerfile.engine"),
                "--build-arg",
                f"ENGINE_IMAGE={base}",
                "--build-arg",
                f"SKEW_SECONDS={' '.join(str(s) for s in steps)}",
                "--tag",
                tag,
                str(context),
                timeout=1800,
            ),
            f"building the soak engine image from {base}",
        )


def preflight(profile: dict, lab: soak_lab.Lab, disk_path: Path) -> None:
    info = lab.docker("info", "--format", "{{.ServerVersion}}", timeout=30)
    if not info.ok:
        raise soak_lab.LabError(f"docker is not available: {info.text()[:200]}")
    if not lab.docker("image", "inspect", lab.engine_image, timeout=30).ok:
        raise soak_lab.LabError(
            f"the engine image {lab.engine_image} does not exist: build it (docker build -f docker/Dockerfile), "
            "then pass --build-engine-from <that tag> to add the soak layer"
        )
    free_gib = shutil.disk_usage(disk_path).free / 2**30
    if free_gib < profile["min_free_gib"]:
        raise soak_lab.LabError(
            f"{free_gib:.0f} GiB free at {disk_path}; the {profile['name']} profile needs at least "
            f"{profile['min_free_gib']} GiB (docs/SOAK_VALIDATION.md has the arithmetic)"
        )


def collect_logs(lab: soak_lab.Lab, run_dir: Path) -> None:
    logs = run_dir / "logs"
    logs.mkdir(exist_ok=True)
    for service in ("engine", "producer", "kafka", "minio", "toxiproxy"):
        result = lab.docker("logs", "--timestamps", "--tail", "3000000", lab.container(service), timeout=600)
        with gzip.open(logs / f"{service}.log.gz", "wt") as handle:
            handle.write(result.out + result.err)


def write_evidence(run_dir: Path, out: Path) -> Path:
    """The evidence tarball: everything the verdict rests on, with a SHA256SUMS
    of every file in it, so a maintainer can attach it to the release and a
    reader can re-run soak_eval over it."""
    sums = []
    for path in sorted(p for p in run_dir.rglob("*") if p.is_file() and p.name != "SHA256SUMS"):
        sums.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(run_dir)}")
    (run_dir / "SHA256SUMS").write_text("\n".join(sums) + "\n")
    with tarfile.open(out, "w:gz") as tar:
        tar.add(run_dir, arcname=run_dir.name)
    return out


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", required=True, help="72h, ci-smoke, or a path to a profile JSON")
    parser.add_argument("--out", type=Path, required=True, help="where the run directory and the evidence tarball go")
    parser.add_argument(
        "--engine-image", default="vtop-engine:soak", help="the engine image the lab runs (default vtop-engine:soak)"
    )
    parser.add_argument(
        "--build-engine-from",
        metavar="IMAGE",
        help="build --engine-image from this engine image plus the soak layer (shim, entrypoint) before starting",
    )
    parser.add_argument(
        "--project", default="vtop-soak", help="compose project and container-name prefix (default vtop-soak)"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="keep the lab's data (Kafka, MinIO, the engine's ledger and work directory) here, on a disk with the "
        "profile's free space, instead of in Docker volumes",
    )
    parser.add_argument(
        "--subnet",
        metavar="CIDR",
        help="give the lab's network this subnet, for a host whose Docker address pools are used up",
    )
    parser.add_argument("--keep-lab", action="store_true", help="leave the containers and volumes up at the end")
    return parser.parse_args(argv)


def prepare_run_dir(profile: dict, out: Path, data_dir: Path | None) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = (out / f"soak-{profile['name']}-{stamp}").resolve()
    for sub in ("config", "ctl", "raw", "logs"):
        (run_dir / sub).mkdir(parents=True)
    (run_dir / "ctl").chmod(0o777)  # the engine's user writes the shim's marker here
    (run_dir / "config" / "config.yaml").write_text(render_engine_config(profile))
    (run_dir / "profile.json").write_text(json.dumps(profile, indent=2) + "\n")
    shutil.copy(soak_lab.COMPOSE_FILE, run_dir / "docker-compose.soak.yml")
    if data_dir:
        for sub in ("kafka", "minio", "engine-state", "engine-work"):
            (data_dir / sub).mkdir(parents=True, exist_ok=True)
            (data_dir / sub).chmod(0o777)
    return run_dir


def run_soak(
    profile: dict,
    lab: soak_lab.Lab,
    run_dir: Path,
    out: Path,
    disk_path: Path,
    *,
    build_from: str | None = None,
    keep_lab: bool = False,
    poll_s: float | None = None,
) -> int:
    """One run, start to evidence. Returns the exit status: 0 passed, 1 did not
    pass, 2 could not start."""
    (run_dir / "host.json").write_text(
        json.dumps({**lab.host_info(), "profile": profile["name"], "engine_image": lab.engine_image}, indent=2) + "\n"
    )
    rec = Recorder(run_dir)
    state = RunState()
    driver = Driver(profile, lab, run_dir, rec, state, disk_path)
    if poll_s is not None:
        driver.poll_s = poll_s
    print(f"soak: {profile['name']} profile, run directory {run_dir}", flush=True)
    threads: list[threading.Thread] = []
    started = False
    try:
        if build_from:
            build_engine_image(lab, build_from, lab.engine_image, profile)
        preflight(profile, lab, disk_path)
        lab.up("kafka", "minio", "toxiproxy")
        lab.create_topic(profile["partitions"], profile["kafka_retention_hours"])
        lab.set_preload(None)
        state.engine_started()
        lab.prepare_data_dirs()
        lab.up("engine")
        lab.wait_ready(profile["recovery_timeout_s"])
        lab.start_producer()
        rec.restart_origin()
        started = True
        rec.event("run_start", profile=profile["name"], rate=profile["rate_records_per_s"])
        rec.event("engine_start", lifetime=state.current_lifetime(), reason="initial")
        threads = [
            threading.Thread(
                target=driver._loop,
                args=(profile["sample_interval_s"], driver.sample_tick),
                daemon=True,
                name="sampler",
            ),
            threading.Thread(
                target=driver._loop, args=(profile["lag_interval_s"], driver.lag_tick), daemon=True, name="lag"
            ),
        ]
        for thread in threads:
            thread.start()
        driver.main_loop()
        driver.finish()
    except (soak_lab.LabError, soak_faults.FaultError, KeyboardInterrupt) as error:
        if not started:
            print(f"soak_driver: could not start: {error}", file=sys.stderr)
            collect_logs(lab, run_dir)
            if not keep_lab:
                lab.down()
            rec.close()
            return 2
        state.request_abort(f"{type(error).__name__}: {error}"[:400])
        try:
            driver.finish()
        except Exception as again:  # collecting what is left matters more than why this failed too
            rec.event("observer_error", where="finish", detail=str(again)[:300])
    finally:
        driver.stop.set()
        for thread in threads:
            thread.join(timeout=120)
    rec.event("run_end", aborted=state.aborted())
    rec.close()
    (run_dir / "finals.json").write_text(json.dumps(driver.finals, indent=2) + "\n")
    collect_logs(lab, run_dir)
    verdict = soak_eval.evaluate_run_dir(profile, run_dir)
    (run_dir / "verdict.json").write_text(json.dumps(verdict, indent=2) + "\n")
    print(soak_eval.render(verdict))
    evidence = write_evidence(run_dir, out / f"{run_dir.name}.tar.gz")
    print(f"soak: evidence tarball {evidence}", flush=True)
    if not keep_lab:
        lab.down()
    return 0 if verdict["passed"] else 1


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        profile = soak_profile.load(args.profile)
    except soak_profile.ProfileError as error:
        print(f"soak_driver: {error}", file=sys.stderr)
        return 2
    run_dir = prepare_run_dir(profile, args.out, args.data_dir)
    lab = soak_lab.Lab(
        args.project,
        run_dir,
        args.engine_image,
        toxiproxy_port=free_port(),
        data_dir=args.data_dir.resolve() if args.data_dir else None,
        subnet=args.subnet,
        load={
            "SOAK_RATE": profile["rate_records_per_s"],
            "SOAK_RECORD_BYTES": profile["record_bytes"],
            "SOAK_ENGINE_MEM": profile["engine"]["memory_limit"],
        },
    )
    return run_soak(
        profile,
        lab,
        run_dir,
        args.out,
        args.data_dir or run_dir,
        build_from=args.build_engine_from,
        keep_lab=args.keep_lab,
    )


if __name__ == "__main__":
    sys.exit(main())
