"""The soak evaluator decides pass or fail from what a run recorded (#726).

Every test builds a healthy synthetic run of the ci-smoke profile and breaks
one thing, then asserts the criterion that should catch it fails and the rest
do not. The first test pins that the healthy run passes: a verdict that fails
everything would make every other test here pass for the wrong reason.
"""

import copy
import json
import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import soak_eval  # noqa: E402
import soak_profile  # noqa: E402

# The ci-smoke profile's shape (540 s, a fault every 75 s) with the 72h
# profile's trend limits, so the tests judge growth as the real run will; the
# smoke profile's own loose limits are tested at the end.
PROFILE = soak_profile.load("ci-smoke")
PROFILE["criteria"]["trend"] = soak_profile.load("72h")["criteria"]["trend"]


def enospc_probe(fault_id, at):
    """What the injector records when a disk-full fault landed and lifted."""
    return {
        "elapsed_s": at,
        "kind": "fault_probe",
        "id": fault_id,
        "fault": "enospc",
        "write_failed_enospc": True,
        "engine_enospc_lines": 4,
        "writes_resumed": True,
    }


def healthy_run(
    profile=PROFILE,
    rss=lambda t, life: 200e6,
    fds=lambda t, life: 60,
    ledger=lambda t, life: 2e6,
    work=lambda t, life: 20e6,
    lag_s=lambda t: 20.0,
    seed=7,
):
    """samples, events, finals for a run in which nothing is wrong."""
    rng = random.Random(seed)
    plan = soak_profile.build_plan(profile)
    events = [{"elapsed_s": 0.0, "kind": "run_start"}, {"elapsed_s": 0.0, "kind": "engine_start", "lifetime": 1}]
    restarts = []  # elapsed times at which the engine process is replaced
    for fault in plan:
        t = float(fault.at_s)
        events.append({"elapsed_s": t, "kind": "fault_start", "id": fault.id, "fault": fault.kind})
        if fault.kind in ("kill9", "clock_step"):
            events.append({"elapsed_s": t + 1, "kind": "engine_exit", "planned": True, "exit_code": 137})
            restarts.append(t + 3)
        if fault.kind == "enospc":
            events.append(enospc_probe(fault.id, t + fault.hold_s + 3))
        events.append(
            {"elapsed_s": t + fault.hold_s + 4, "kind": "fault_end", "id": fault.id, "fault": fault.kind, "ok": True}
        )
        events.append(
            {
                "elapsed_s": t + fault.hold_s + 14,
                "kind": "recovered",
                "id": fault.id,
                "fault": fault.kind,
                "ok": True,
                "recovery_s": 10.0,
            }
        )
        events.append({"elapsed_s": t + fault.hold_s + 15, "kind": "audit", "trigger": f"fault:{fault.id}", "ok": True})
    at = profile["prune_interval_s"]
    while at < profile["duration_s"]:
        events.append({"elapsed_s": float(at), "kind": "prune", "ok": True, "deleted": 5})
        at += profile["prune_interval_s"]
    events.append({"elapsed_s": 150.0, "kind": "scrub", "mode": "sample", "ok": True})
    events.append({"elapsed_s": profile["duration_s"] + 20.0, "kind": "run_end", "aborted": None})

    samples = []
    produced = committed = 0
    t = 0.0
    while t <= profile["duration_s"] + 5:
        life = 1 + sum(1 for r in restarts if r <= t)
        produced += profile["rate_records_per_s"] * profile["sample_interval_s"]
        committed = max(0, produced - int(lag_s(t) * profile["rate_records_per_s"]))
        batches = int(t / 10)
        samples.append(
            {
                "elapsed_s": t,
                "lifetime": life,
                "engine_up": True,
                "rss_bytes": rss(t, life) + rng.uniform(-1e5, 1e5),
                "open_fds": fds(t, life) + rng.choice([-1, 0, 1]),
                "work_dir_bytes": work(t, life) + rng.uniform(-1e4, 1e4),
                "ledger_bytes": ledger(t, life) + rng.uniform(-1e3, 1e3),
                "disk_free_bytes": 50 * 2**30,
                "metrics": {
                    soak_eval.M_COMMITS: batches,
                    soak_eval.M_VERIFIED: batches,
                    soak_eval.M_VERIFY_FAILURES: 0,
                    soak_eval.M_BATCH_SUM: batches * 12.0,
                    soak_eval.M_BATCH_COUNT: batches,
                },
                "lag": {"end": produced, "committed": committed},
            }
        )
        t += profile["sample_interval_s"]
    finals = {
        "audit_coverage": {"ok": True},
        "scrub": {"ok": True, "batches_checked": 40, "findings": 0},
        "tail": {"partitions": {"0": {"committed": 100, "end": 100}, "1": {"committed": 90, "end": 90}}},
        "engine_stop": {"exit_code": 0},
    }
    return samples, events, finals


def verdict_of(samples, events, finals, profile=PROFILE):
    return soak_eval.evaluate(profile, samples, events, finals)


def failed(verdict):
    return {c["id"] for c in verdict["criteria"] if not c["passed"]}


def test_a_healthy_run_passes_every_criterion():
    v = verdict_of(*healthy_run())
    assert failed(v) == set(), [c for c in v["criteria"] if not c["passed"]]
    assert v["passed"] is True


def test_a_restart_sawtooth_is_not_a_trend():
    # Memory climbs while an engine process lives and drops when a fault
    # replaces it: the flat level across the run is what matters.
    start = {}

    def rss(t, life):
        start.setdefault(life, t)
        return 200e6 + (t - start[life]) * 1e3

    v = verdict_of(*healthy_run(rss=rss))
    assert "trend_rss_bytes" not in failed(v)


def test_steady_memory_growth_fails_the_rss_trend():
    v = verdict_of(*healthy_run(rss=lambda t, life: 200e6 + t * 2e6))
    assert failed(v) == {"trend_rss_bytes"}


def test_growth_before_the_window_is_not_counted():
    # The first 20% of the run is warm-up; the issue measures the last 80%.
    v = verdict_of(*healthy_run(rss=lambda t, life: 200e6 + min(t, 90) * 2e6))
    assert "trend_rss_bytes" not in failed(v)


def test_a_small_absolute_growth_is_not_a_trend():
    # 100% relative growth of a tiny series stays under the absolute floor.
    v = verdict_of(*healthy_run(fds=lambda t, life: 2 + t / 540 * 2))
    assert "trend_open_fds" not in failed(v)


def test_open_file_growth_fails():
    v = verdict_of(*healthy_run(fds=lambda t, life: 60 + t * 2))
    assert failed(v) == {"trend_open_fds"}


def test_ledger_growth_fails_the_ledger_trend():
    v = verdict_of(*healthy_run(ledger=lambda t, life: 2e6 + t * 1e6))
    assert failed(v) == {"trend_ledger_bytes"}


def test_work_dir_growth_between_faults_fails():
    v = verdict_of(*healthy_run(work=lambda t, life: 20e6 + t * 5e6))
    assert failed(v) == {"trend_work_dir_bytes"}


def test_work_dir_filling_during_an_outage_is_not_a_trend():
    # A store outage legitimately fills the work directory; samples inside the
    # fault window (until the engine recovers) are left out of that series.
    p = PROFILE
    outage = next(f for f in soak_profile.build_plan(p) if f.kind == "store_outage")

    def work(t, life):
        inside = outage.at_s <= t <= outage.at_s + outage.hold_s + 14
        return 20e6 + (4e9 if inside else 0)

    v = verdict_of(*healthy_run(work=work))
    assert "trend_work_dir_bytes" not in failed(v)


def test_rss_during_a_fault_still_counts():
    # Memory is judged on every sample: a leak during a fault is a leak.
    p = PROFILE
    fault = soak_profile.build_plan(p)[2]
    samples, events, finals = healthy_run()
    for s in samples:
        if fault.at_s <= s["elapsed_s"] <= fault.at_s + fault.hold_s + 14:
            s["rss_bytes"] += 4e9
    # a burst inside one window moves no median: robust, and not failed
    assert "trend_rss_bytes" not in failed(verdict_of(samples, events, finals))


def test_latency_growth_fails_the_batch_duration_trend():
    samples, events, finals = healthy_run()
    for s in samples:
        n = s["metrics"][soak_eval.M_BATCH_COUNT]
        s["metrics"][soak_eval.M_BATCH_SUM] = sum(12.0 + i * 1.0 for i in range(int(n)))
    assert "trend_batch_seconds" in failed(verdict_of(samples, events, finals))


def test_archive_lag_growth_fails_the_lag_trend_and_the_bound():
    v = verdict_of(*healthy_run(lag_s=lambda t: 5 + t * 0.5))
    assert {"trend_lag_seconds", "lag_steady"} <= failed(v)


def test_a_trend_with_too_few_points_fails_instead_of_passing_on_nothing():
    samples, events, finals = healthy_run()
    samples = [s for s in samples if s["elapsed_s"] < 110 or s["elapsed_s"] > 530]
    v = verdict_of(samples, events, finals)
    assert "trend_rss_bytes" in failed(v)
    detail = next(c for c in v["criteria"] if c["id"] == "trend_rss_bytes")
    assert "too few" in detail["observed"]


def test_a_run_with_no_faults_fails():
    samples, events, finals = healthy_run()
    events = [e for e in events if not e["kind"].startswith("fault") and e["kind"] != "recovered"]
    v = verdict_of(samples, events, finals)
    assert "faults_executed" in failed(v)


def test_a_fault_that_did_not_land_fails_and_says_why():
    samples, events, finals = healthy_run()
    for e in events:
        if e["kind"] == "fault_end" and e["id"] == 3:
            e["ok"] = False
            e["detail"] = "the engine still reaches kafka through the proxy"
    v = verdict_of(samples, events, finals)
    criterion = next(c for c in v["criteria"] if c["id"] == "faults_executed")
    assert not criterion["passed"] and "#3" in criterion["detail"] and "still reaches kafka" in criterion["detail"]


def test_a_fault_the_engine_did_not_recover_from_fails():
    samples, events, finals = healthy_run()
    for e in events:
        if e["kind"] == "recovered" and e["id"] == 2:
            e["ok"] = False
    assert "faults_executed" in failed(verdict_of(samples, events, finals))


def test_the_wrong_fault_kind_for_a_planned_slot_fails():
    samples, events, finals = healthy_run()
    for e in events:
        if e["kind"] == "fault_start" and e["id"] == 1:
            e["fault"] = "partition"
    assert "faults_executed" in failed(verdict_of(samples, events, finals))


def test_slow_recovery_fails():
    samples, events, finals = healthy_run()
    for e in events:
        if e["kind"] == "recovered" and e["id"] == 1:
            e["recovery_s"] = PROFILE["recovery_timeout_s"] + 1
    assert "recovery_time" in failed(verdict_of(samples, events, finals))


def test_an_exit_no_fault_caused_fails():
    samples, events, finals = healthy_run()
    events.append({"elapsed_s": 200.0, "kind": "engine_exit", "planned": False, "exit_code": 139})
    assert failed(verdict_of(samples, events, finals)) == {"no_unplanned_exit"}


def test_commits_ahead_of_verifications_in_two_samples_in_a_row_fails():
    samples, events, finals = healthy_run()
    for s in samples[40:43]:
        s["metrics"][soak_eval.M_COMMITS] = s["metrics"][soak_eval.M_VERIFIED] + 3
    assert failed(verdict_of(samples, events, finals)) == {"commit_rule"}


def test_a_lag_only_record_between_two_breaching_scrapes_does_not_hide_the_breach():
    samples, events, finals = healthy_run()
    for s in samples[40:42]:
        s["metrics"][soak_eval.M_COMMITS] = s["metrics"][soak_eval.M_VERIFIED] + 3
    between = {
        "elapsed_s": (samples[40]["elapsed_s"] + samples[41]["elapsed_s"]) / 2,
        "lifetime": samples[40]["lifetime"],
        "lag": {"end": 1, "committed": 1},
    }
    samples.insert(41, between)
    assert failed(verdict_of(samples, events, finals)) == {"commit_rule"}


def test_a_restart_between_two_breaching_scrapes_is_not_one_breach():
    samples, events, finals = healthy_run()
    for s in samples[40:42]:
        s["metrics"][soak_eval.M_COMMITS] = s["metrics"][soak_eval.M_VERIFIED] + 3
    samples[41]["lifetime"] = samples[40]["lifetime"] + 10  # a different engine process
    assert "commit_rule" not in failed(verdict_of(samples, events, finals))


def test_one_scrape_that_caught_a_commit_before_its_verification_is_not_a_breach():
    samples, events, finals = healthy_run()
    samples[40]["metrics"][soak_eval.M_COMMITS] = samples[40]["metrics"][soak_eval.M_VERIFIED] + 1
    assert "commit_rule" not in failed(verdict_of(samples, events, finals))


def test_a_verification_failure_fails_even_across_a_restart():
    samples, events, finals = healthy_run()
    for s in samples[60:]:
        s["metrics"][soak_eval.M_VERIFY_FAILURES] = 1
    assert failed(verdict_of(samples, events, finals)) == {"commit_rule"}


def test_no_scraped_counters_fails_the_commit_rule():
    samples, events, finals = healthy_run()
    for s in samples:
        s["metrics"] = None
    assert "commit_rule" in failed(verdict_of(samples, events, finals))


def test_a_failed_audit_after_a_fault_fails_zero_holes():
    samples, events, finals = healthy_run()
    audit = next(e for e in events if e["kind"] == "audit")
    audit["ok"] = False
    audit["detail"] = "partition 0: hole at offsets 4000..5999"
    v = verdict_of(samples, events, finals)
    assert failed(v) == {"zero_coverage_holes"}
    assert "hole at offsets" in next(c for c in v["criteria"] if c["id"] == "zero_coverage_holes")["detail"]


def test_a_missing_final_audit_fails():
    samples, events, finals = healthy_run()
    del finals["audit_coverage"]
    assert failed(verdict_of(samples, events, finals)) == {"zero_coverage_holes"}


def test_no_audit_ran_at_all_fails():
    samples, events, finals = healthy_run()
    events = [e for e in events if e["kind"] != "audit"]
    assert "zero_coverage_holes" in failed(verdict_of(samples, events, finals))


def test_a_scrub_finding_fails():
    samples, events, finals = healthy_run()
    finals["scrub"] = {"ok": False, "batches_checked": 40, "findings": 2, "detail": "mismatch in batch b-17"}
    assert failed(verdict_of(samples, events, finals)) == {"scrub_clean"}


def test_a_failed_sampled_scrub_fails():
    samples, events, finals = healthy_run()
    next(e for e in events if e["kind"] == "scrub")["ok"] = False
    assert failed(verdict_of(samples, events, finals)) == {"scrub_clean"}


def test_a_scrub_that_checked_almost_nothing_fails():
    samples, events, finals = healthy_run()
    finals["scrub"]["batches_checked"] = 1
    assert failed(verdict_of(samples, events, finals)) == {"scrub_clean"}


def test_a_missing_scrub_fails():
    samples, events, finals = healthy_run()
    del finals["scrub"]
    assert failed(verdict_of(samples, events, finals)) == {"scrub_clean"}


def test_a_group_that_did_not_reach_the_end_fails():
    samples, events, finals = healthy_run()
    finals["tail"]["partitions"]["1"]["committed"] = 80
    assert failed(verdict_of(samples, events, finals)) == {"tail_drained"}


def test_an_unread_tail_fails():
    samples, events, finals = healthy_run()
    finals["tail"] = {"error": "broker unreachable"}
    assert failed(verdict_of(samples, events, finals)) == {"tail_drained"}


def test_an_engine_that_does_not_stop_cleanly_fails():
    samples, events, finals = healthy_run()
    finals["engine_stop"] = {"exit_code": 137}
    assert failed(verdict_of(samples, events, finals)) == {"clean_shutdown"}


def test_a_low_rate_fails():
    samples, events, finals = healthy_run()
    for i, s in enumerate(samples):
        s["lag"]["end"] = i * 100  # a tenth of the stated rate
        s["lag"]["committed"] = max(0, i * 100 - 6000)
    assert "rate" in failed(verdict_of(samples, events, finals))


def test_failed_prunes_fail():
    samples, events, finals = healthy_run()
    next(e for e in events if e["kind"] == "prune")["ok"] = False
    assert failed(verdict_of(samples, events, finals)) == {"ledger_prune"}


def test_a_prune_schedule_that_never_ran_fails():
    samples, events, finals = healthy_run()
    events = [e for e in events if e["kind"] != "prune"]
    assert failed(verdict_of(samples, events, finals)) == {"ledger_prune"}


def test_running_short_of_disk_fails():
    samples, events, finals = healthy_run()
    samples[30]["disk_free_bytes"] = 1 * 2**30
    assert failed(verdict_of(samples, events, finals)) == {"disk_headroom"}


def test_an_aborted_run_fails_and_names_why():
    samples, events, finals = healthy_run()
    events[-1] = {"elapsed_s": 300.0, "kind": "run_end", "aborted": "3 faults in a row did not land"}
    v = verdict_of(samples, events, finals)
    assert "run_completed" in failed(v)
    assert "3 faults in a row" in next(c for c in v["criteria"] if c["id"] == "run_completed")["observed"]


def test_a_run_with_no_end_fails():
    samples, events, finals = healthy_run()
    events = [e for e in events if e["kind"] != "run_end"]
    assert "run_completed" in failed(verdict_of(samples, events, finals))


def test_a_sampler_that_stopped_fails():
    samples, events, finals = healthy_run()
    samples = [s for s in samples if s["elapsed_s"] < 200 or s["elapsed_s"] > 400]
    assert "samples" in failed(verdict_of(samples, events, finals))


def test_the_drain_after_the_load_stops_does_not_dilute_the_rate():
    # Producing exactly on target for the whole run, then five idle minutes of
    # draining and checks: the rate is the production interval's.
    samples, events, finals = healthy_run()
    last = samples[-1]
    for i in range(1, 60):
        row = copy.deepcopy(last)
        row["elapsed_s"] = PROFILE["duration_s"] + 5 + i * 5
        row["lag"] = {"end": last["lag"]["end"], "committed": last["lag"]["end"]}
        samples.append(row)
    events.append({"elapsed_s": float(PROFILE["duration_s"]), "kind": "producer_stopped"})
    v = verdict_of(samples, events, finals)
    assert "rate" not in failed(v), next(c for c in v["criteria"] if c["id"] == "rate")


def test_a_rate_that_really_is_low_still_fails_with_the_load_end_recorded():
    samples, events, finals = healthy_run()
    for i, s in enumerate(samples):
        s["lag"]["end"] = i * 100
        s["lag"]["committed"] = max(0, i * 100 - 6000)
    events.append({"elapsed_s": float(PROFILE["duration_s"]), "kind": "producer_stopped"})
    assert "rate" in failed(verdict_of(samples, events, finals))


def test_lag_records_cannot_stand_in_for_missing_resource_samples():
    # Half the resource samples are missing, but lag-only records fill the file:
    # the count of rows is fine and the resource coverage is not.
    samples, events, finals = healthy_run()
    for i, s in enumerate(samples):
        if i % 2:
            for key in ("engine_up", "rss_bytes", "open_fds", "work_dir_bytes", "ledger_bytes", "metrics"):
                s.pop(key, None)
    v = verdict_of(samples, events, finals)
    criterion = next(c for c in v["criteria"] if c["id"] == "samples")
    assert not criterion["passed"] and "resource samples" in criterion["observed"]


def test_records_that_observed_nothing_are_not_samples():
    # Four of five resource records say the engine could not be probed, and the
    # lag queries failed as often: the file is full of rows, and the run is not
    # observed.
    samples, events, finals = healthy_run()
    for i, s in enumerate(samples):
        if i % 5:
            for key in ("rss_bytes", "open_fds", "work_dir_bytes", "ledger_bytes", "metrics"):
                s.pop(key, None)
            s["engine_up"] = False
            s["sample_error"] = "engine probe exit 1"
            s["lag"] = None
    v = verdict_of(samples, events, finals)
    criterion = next(c for c in v["criteria"] if c["id"] == "samples")
    assert not criterion["passed"]


def test_a_blind_prefix_or_suffix_fails_even_when_what_remains_is_evenly_spaced():
    # No gap between the observations that exist, and a third of the run
    # unobserved at one end.
    samples, events, finals = healthy_run()
    prefix_gone = [s for s in samples if s["elapsed_s"] >= 180]
    assert "samples" in failed(verdict_of(prefix_gone, events, finals))
    suffix_gone = [s for s in samples if s["elapsed_s"] <= 360]
    assert "samples" in failed(verdict_of(suffix_gone, events, finals))


def test_missing_lag_samples_fail_the_sampling_criterion_on_their_own():
    samples, events, finals = healthy_run()
    for i, s in enumerate(samples):
        if i % 4:
            s.pop("lag", None)
    assert "samples" in failed(verdict_of(samples, events, finals))


def test_no_samples_at_all_fails_without_raising():
    v = verdict_of([], healthy_run()[1], healthy_run()[2])
    assert v["passed"] is False and "samples" in failed(v)


# -- the pieces --------------------------------------------------------------


def test_counter_increase_handles_resets_and_gaps():
    assert soak_eval.counter_increase([0, 5, 9]) == 9
    assert soak_eval.counter_increase([0, 5, 2, 4]) == 5 + 2 + 2  # 5, reset to 2, then 4
    assert soak_eval.counter_increase([None, 3, None, 3]) == 3
    assert soak_eval.counter_increase([]) == 0


def test_theil_sen_recovers_a_slope_and_ignores_an_outlier_burst():
    points = [(t, 100 + 2 * t, 1) for t in range(0, 200, 2)]
    points[10] = (points[10][0], 1e9, 1)
    trend = soak_eval.theil_sen(points, (0, 200), min_pairs=5)
    assert trend is not None and trend.slope_per_s == pytest.approx(2, rel=0.05)


def test_theil_sen_never_pairs_across_lifetimes():
    # two lifetimes, each flat, the second higher: slope within each is zero
    points = [(t, 100, 1) for t in range(0, 100)] + [(t, 900, 2) for t in range(100, 200)]
    trend = soak_eval.theil_sen(points, (0, 200), min_pairs=5)
    assert trend is not None and trend.slope_per_s == 0


def test_theil_sen_declines_when_there_are_too_few_pairs():
    assert soak_eval.theil_sen([(1, 1, 1), (2, 2, 1)], (0, 10), min_pairs=5) is None


def test_percentile_is_nearest_rank():
    assert soak_eval.percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 95) == 10
    assert soak_eval.percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 50) == 5


# -- files and the command line ---------------------------------------------


def write_run(directory: Path, samples, events, finals):
    (directory / "samples.jsonl").write_text("".join(json.dumps(s) + "\n" for s in samples))
    (directory / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
    (directory / "finals.json").write_text(json.dumps(finals))


def test_cli_exit_zero_for_a_pass_and_writes_the_verdict(tmp_path, capsys):
    write_run(tmp_path, *healthy_run())
    assert soak_eval.main(["--profile", "ci-smoke", "--run-dir", str(tmp_path)]) == 0
    verdict = json.loads((tmp_path / "verdict.json").read_text())
    assert verdict["passed"] is True
    assert set(verdict["inputs"]) == {"samples.jsonl", "events.jsonl", "finals.json"}
    assert "PASS" in capsys.readouterr().out


def test_cli_exit_one_for_a_failure_and_prints_what_failed(tmp_path, capsys):
    samples, events, finals = healthy_run()
    finals["scrub"]["ok"] = False
    write_run(tmp_path, samples, events, finals)
    assert soak_eval.main(["--profile", "ci-smoke", "--run-dir", str(tmp_path)]) == 1
    out = capsys.readouterr().out
    assert "FAIL" in out and "scrub is clean" in out


def test_cli_exit_two_for_a_truncated_record_naming_the_line(tmp_path, capsys):
    write_run(tmp_path, *healthy_run())
    with open(tmp_path / "samples.jsonl", "a") as handle:
        handle.write('{"elapsed_s": 1.0, "rss_by')
    assert soak_eval.main(["--profile", "ci-smoke", "--run-dir", str(tmp_path)]) == 2
    err = capsys.readouterr().err
    assert "samples.jsonl line" in err and "not JSON" in err


def test_cli_exit_two_for_a_missing_run_directory(tmp_path, capsys):
    assert soak_eval.main(["--profile", "ci-smoke", "--run-dir", str(tmp_path / "absent")]) == 2


def test_cli_exit_two_for_an_unknown_profile(tmp_path):
    write_run(tmp_path, *healthy_run())
    assert soak_eval.main(["--profile", "nonesuch", "--run-dir", str(tmp_path)]) == 2


def test_the_same_inputs_give_the_same_verdict(tmp_path):
    run = healthy_run()
    assert verdict_of(*copy.deepcopy(run)) == verdict_of(*copy.deepcopy(run))


def test_the_smoke_profiles_loose_limits_pass_what_the_strict_ones_catch():
    smoke = soak_profile.load("ci-smoke")
    samples, events, finals = healthy_run(smoke, rss=lambda t, life: 200e6 + t * 2e6)
    assert "trend_rss_bytes" not in failed(soak_eval.evaluate(smoke, samples, events, finals))


def test_the_72h_profile_judges_a_healthy_synthetic_72h_run():
    p72 = soak_profile.load("72h")
    samples, events, finals = healthy_run(p72)
    finals["scrub"]["batches_checked"] = 5000
    v = soak_eval.evaluate(p72, samples, events, finals)
    assert failed(v) == set(), [c for c in v["criteria"] if not c["passed"]]


# -- the disk-full fault --------------------------------------------------------


def enospc_fault():
    return next(f for f in soak_profile.build_plan(PROFILE) if f.kind == "enospc")


def test_a_run_with_a_disk_full_fault_is_asked_to_show_the_engine_felt_it():
    v = verdict_of(*healthy_run())
    criterion = next(c for c in v["criteria"] if c["id"] == "enospc_recovered")
    assert criterion["passed"] and criterion["observed"] == "1 of 1 disk-full faults clean"


def test_a_profile_without_a_disk_full_fault_is_not_asked_for_one():
    profile = copy.deepcopy(PROFILE)
    assert "enospc" in profile["fault_cycle"]  # the shipped profile has one; this one is without
    profile["fault_cycle"] = [k for k in profile["fault_cycle"] if k != "enospc"]
    v = verdict_of(*healthy_run(profile), profile=profile)
    assert "enospc_recovered" not in {c["id"] for c in v["criteria"]}


def test_a_disk_full_fault_with_no_record_of_what_it_proved_fails():
    samples, events, finals = healthy_run()
    events = [e for e in events if e["kind"] != "fault_probe"]
    v = verdict_of(samples, events, finals)
    assert failed(v) == {"enospc_recovered"}
    assert (
        "no record that a write failed with ENOSPC"
        in next(c for c in v["criteria"] if c["id"] == "enospc_recovered")["detail"]
    )


def test_a_disk_full_fault_the_engine_never_logged_fails():
    samples, events, finals = healthy_run()
    next(e for e in events if e["kind"] == "fault_probe")["engine_enospc_lines"] = 0
    v = verdict_of(samples, events, finals)
    assert failed(v) == {"enospc_recovered"}
    assert (
        "the engine logged no disk-full condition"
        in next(c for c in v["criteria"] if c["id"] == "enospc_recovered")["detail"]
    )


def test_a_disk_full_fault_after_which_writes_did_not_resume_fails():
    samples, events, finals = healthy_run()
    next(e for e in events if e["kind"] == "fault_probe")["writes_resumed"] = False
    assert failed(verdict_of(samples, events, finals)) == {"enospc_recovered"}


def test_a_disk_full_fault_whose_write_never_failed_fails():
    samples, events, finals = healthy_run()
    next(e for e in events if e["kind"] == "fault_probe")["write_failed_enospc"] = False
    assert failed(verdict_of(samples, events, finals)) == {"enospc_recovered"}


def test_a_commit_beyond_a_verification_while_the_disk_was_full_fails_the_fault_as_well():
    fault = enospc_fault()
    samples, events, finals = healthy_run()
    inside = [s for s in samples if fault.at_s + 2 <= s["elapsed_s"] <= fault.at_s + 12]
    assert len(inside) >= 2
    for s in inside:
        s["metrics"][soak_eval.M_COMMITS] = s["metrics"][soak_eval.M_VERIFIED] + 1
    v = verdict_of(samples, events, finals)
    assert {"enospc_recovered", "commit_rule"} <= failed(v)
    assert "commits beyond verifications" in next(c for c in v["criteria"] if c["id"] == "enospc_recovered")["detail"]


def test_a_hole_found_by_the_audit_after_the_disk_full_fault_fails_it():
    fault = enospc_fault()
    samples, events, finals = healthy_run()
    audit = next(e for e in events if e["kind"] == "audit" and e["trigger"] == f"fault:{fault.id}")
    audit["ok"] = False
    v = verdict_of(samples, events, finals)
    assert {"enospc_recovered", "zero_coverage_holes"} <= failed(v)
