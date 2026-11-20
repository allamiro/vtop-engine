"""A whole run of the driver against a scripted lab: setup, load, every fault
kind, the periodic work, the final checks, the verdict and the evidence (#726).

The profile is a few seconds long, so this is about the driver's flow and what
it records, not about any engine. The first test pins that a healthy run
passes; the others break one thing each and expect the verdict to say so.
"""

import copy
import json
import sys
import tarfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import soak_driver  # noqa: E402
import soak_profile  # noqa: E402
from soak_fakelab import FakeLab  # noqa: E402

TINY = {
    "name": "tiny",
    "description": "a few seconds, for the driver's own tests",
    "duration_s": 6,
    "rate_records_per_s": 300,
    "record_bytes": 700,
    "partitions": 2,
    "sample_interval_s": 1,
    "lag_interval_s": 1,
    "raw_snapshot_interval_s": 2,
    "prune_interval_s": 3,
    "scrub_interval_s": 4,
    "scrub_sample_batches": 5,
    "fault_interval_s": 2,
    "first_fault_after_s": 1,
    "quiet_tail_s": 1,
    "fault_cycle": ["kill9", "clock_step", "partition"],
    "fault_hold_s": {"kill9": 0, "clock_step": 0, "partition": 0},
    "clock_step_seconds": [3600],
    "recovery_timeout_s": 5,
    "drain_timeout_s": 5,
    "check_timeout_s": 5,
    "min_free_gib": 0,
    "kafka_retention_hours": 1,
    "engine": {
        "max_records": 100,
        "max_bytes": 1048576,
        "max_batch_age_seconds": 1,
        "max_concurrent_batches": 1,
        "compression": "gzip",
        "compression_level": 1,
        "memory_limit": "1g",
    },
    "criteria": {
        "trend_window_fraction": 0.8,
        "trend_min_pairs": 1,
        "trend": {name: {"max_growth_pct": 1000, "min_abs_growth": 1e12} for name in soak_profile.TREND_SERIES},
        "min_rate_fraction": 0.1,
        "max_steady_lag_p95_s": 60,
        "min_samples_fraction": 0.5,
        "max_sample_gap_s": 5,
        "min_free_gib_floor": 0,
        "min_scrub_batches": 1,
        "min_prune_fraction": 0.3,
    },
}


# The tiny run has faults back to back, so no stretch is quiet enough for the
# three series that leave fault windows out (work directory, batch duration,
# archive lag) to have anything to fit. They fail as "too few points", which
# is the evaluator refusing to pass on nothing; every other criterion passes.
NO_QUIET_STRETCH = {"trend_work_dir_bytes", "trend_batch_seconds", "trend_lag_seconds"}


@pytest.fixture
def profile():
    return soak_profile.validate(copy.deepcopy(TINY))


def run(profile, tmp_path, lab=None):
    lab = lab or FakeLab()
    out = tmp_path / "out"
    out.mkdir()
    run_dir = soak_driver.prepare_run_dir(profile, out, None)
    code = soak_driver.run_soak(profile, lab, run_dir, out, tmp_path, poll_s=0.05)
    return code, lab, run_dir, out


@pytest.fixture(scope="module")
def healthy(tmp_path_factory):
    """One healthy run, shared by the tests that only read what it recorded."""
    return run(soak_profile.validate(copy.deepcopy(TINY)), tmp_path_factory.mktemp("healthy"))


def events(run_dir):
    return [json.loads(line) for line in (run_dir / "events.jsonl").read_text().splitlines()]


def verdict(run_dir):
    return json.loads((run_dir / "verdict.json").read_text())


def failed(run_dir):
    return {c["id"] for c in verdict(run_dir)["criteria"] if not c["passed"]}


def test_a_healthy_run_fails_only_for_want_of_a_quiet_stretch_and_writes_the_evidence(healthy):
    code, lab, run_dir, out = healthy
    assert failed(run_dir) == NO_QUIET_STRETCH, [c for c in verdict(run_dir)["criteria"] if not c["passed"]]
    assert code == 1
    kinds = {e["fault"] for e in events(run_dir) if e["kind"] == "fault_end" and e["ok"]}
    assert kinds == {"kill9", "clock_step", "partition"}
    tarball = out / f"{run_dir.name}.tar.gz"
    with tarfile.open(tarball) as tar:
        names = {n.split("/", 1)[1] for n in tar.getnames() if "/" in n}
    for expected in (
        "samples.jsonl",
        "events.jsonl",
        "finals.json",
        "verdict.json",
        "profile.json",
        "host.json",
        "config/config.yaml",
        "docker-compose.soak.yml",
        "SHA256SUMS",
        "logs",
    ):
        assert expected in names, (expected, sorted(names))


def test_the_lab_is_brought_up_in_order_and_torn_down(healthy):
    _, lab, _, _ = healthy
    log = lab.log
    assert log.index("up kafka minio toxiproxy") < log.index("create_topic") < log.index("prepare_data_dirs")
    assert log.index("prepare_data_dirs") < log.index("up engine") < log.index("start_producer")
    assert log[-1] == "down"


def test_periodic_work_and_the_final_checks_ran(healthy):
    _, lab, run_dir, _ = healthy
    commands = [call[0] for call in lab.calls]
    assert commands.count("prune-ledger") >= 1 and "scrub" in commands
    storage_audits = [c for c in lab.calls if c[0] == "audit-coverage" and "--check-storage" in c]
    assert len(storage_audits) == 1
    finals = json.loads((run_dir / "finals.json").read_text())
    tail = finals["tail"]["partitions"]["0"]
    assert finals["scrub"]["ok"] and tail["committed"] == tail["end"]
    assert finals["engine_stop"]["exit_code"] == 0


def test_nothing_restarts_the_producer_or_the_engine_after_the_run_stops_them(healthy):
    _, lab, run_dir, _ = healthy
    recorded = events(run_dir)
    stopped = next(e for e in recorded if e["kind"] == "producer_stopped")
    assert not [e for e in recorded if e["kind"] == "producer_restart" and e["elapsed_s"] >= stopped["elapsed_s"]]
    assert not [e for e in recorded if e["kind"] == "engine_exit" and not e["planned"]]
    assert lab.engine_running is False


def test_every_fault_is_followed_by_an_audit_once_the_engine_recovered(healthy):
    _, _, run_dir, _ = healthy
    recorded = events(run_dir)
    for fault_id in {e["id"] for e in recorded if e["kind"] == "fault_start"}:
        recovered = next(e for e in recorded if e["kind"] == "recovered" and e["id"] == fault_id)
        audit = next(e for e in recorded if e["kind"] == "audit" and e.get("trigger") == f"fault:{fault_id}")
        assert recovered["ok"] and audit["elapsed_s"] >= recovered["elapsed_s"]


def test_a_hole_and_a_dirty_scrub_each_fail_their_own_criterion(profile, tmp_path):
    lab = FakeLab()
    lab.audit_ok = False
    lab.scrub_ok = False
    code, _, run_dir, _ = run(profile, tmp_path, lab)
    assert code == 1 and {"zero_coverage_holes", "scrub_clean"} <= failed(run_dir)


def test_a_fault_that_does_not_land_fails_the_run_and_the_run_goes_on(profile, tmp_path):
    lab = FakeLab()
    lab.reach_ignores_proxy = True  # the partition cannot be proven
    code, lab, run_dir, _ = run(profile, tmp_path, lab)
    assert code == 1 and "faults_executed" in failed(run_dir)
    ended = [e for e in events(run_dir) if e["kind"] == "fault_end"]
    assert any(not e["ok"] for e in ended) and any(e["ok"] for e in ended)
    assert lab.proxies == {"kafka": True, "minio": True}


def test_an_engine_that_dies_without_a_fault_is_recorded_restarted_and_fails_the_run(profile, tmp_path):
    lab = FakeLab()
    original = lab.scrape_metrics
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 3:
            lab.engine_running = False  # dies on its own, outside any fault
        return original()

    lab.scrape_metrics = flaky
    code, _, run_dir, _ = run(profile, tmp_path, lab)
    recorded = events(run_dir)
    unplanned = [e for e in recorded if e["kind"] == "engine_exit" and not e["planned"]]
    assert unplanned and any(e.get("reason") == "unplanned" for e in recorded if e["kind"] == "engine_start")
    assert code == 1 and "no_unplanned_exit" in failed(run_dir)


def test_a_lab_that_will_not_start_exits_two_and_writes_no_verdict(profile, tmp_path):
    lab = FakeLab()

    def refuse(*services):
        raise soak_driver.soak_lab.LabError("compose up kafka failed: port is already allocated")

    lab.up = refuse
    code, _, run_dir, _ = run(profile, tmp_path, lab)
    assert code == 2 and not (run_dir / "verdict.json").exists()


def test_the_samples_carry_what_the_evaluator_reads(healthy):
    _, _, run_dir, _ = healthy
    rows = [json.loads(line) for line in (run_dir / "samples.jsonl").read_text().splitlines()]
    full = [r for r in rows if r.get("engine_up")]
    needed = {"rss_bytes", "open_fds", "work_dir_bytes", "ledger_bytes", "disk_free_bytes", "lifetime"}
    assert full and all(needed <= r.keys() for r in full)
    assert any(r.get("lag") for r in rows)
    assert max(r["lifetime"] for r in rows) >= 3  # kill9 and clock_step each started new processes
