"""The soak profiles load, validate and imply the plan the issue asks for (#726)."""

import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import soak_profile  # noqa: E402


@pytest.fixture
def smoke():
    return soak_profile.load("ci-smoke")


def test_both_profiles_load():
    assert soak_profile.load("72h")["name"] == "72h"
    assert soak_profile.load("ci-smoke")["name"] == "ci-smoke"


def test_the_72h_profile_states_the_issues_load_and_cadence():
    p = soak_profile.load("72h")
    assert p["duration_s"] == 72 * 3600
    assert p["rate_records_per_s"] == 1000
    assert p["record_bytes"] == 700
    assert p["fault_interval_s"] == 30 * 60
    assert set(p["fault_cycle"]) == {"kill9", "broker_restart", "partition", "store_outage", "clock_step"}
    assert p["criteria"]["trend_window_fraction"] == 0.8


def test_the_72h_plan_is_a_fault_every_thirty_minutes_with_a_quiet_tail():
    p = soak_profile.load("72h")
    plan = soak_profile.build_plan(p)
    gaps = {b.at_s - a.at_s for a, b in zip(plan, plan[1:])}
    assert gaps == {1800}
    assert plan[0].at_s == 1800
    assert plan[-1].at_s <= p["duration_s"] - p["quiet_tail_s"]
    assert {f.kind for f in plan} == set(p["fault_cycle"])
    # every kind is injected many times, not once
    assert min(sum(1 for f in plan if f.kind == k) for k in p["fault_cycle"]) >= 20


def test_the_smoke_plan_reaches_every_kind_and_the_clock_both_ways(smoke):
    plan = soak_profile.build_plan(smoke)
    assert {f.kind for f in plan} == set(smoke["fault_cycle"])
    steps = {soak_profile.clock_step_for(smoke, i) for i in range(2)}
    assert steps == {3600, -3600}


def test_ids_are_sequential_from_one(smoke):
    assert [f.id for f in soak_profile.build_plan(smoke)] == list(range(1, len(soak_profile.build_plan(smoke)) + 1))


def test_a_missing_key_is_named(smoke):
    broken = copy.deepcopy(smoke)
    del broken["rate_records_per_s"]
    with pytest.raises(soak_profile.ProfileError, match="rate_records_per_s"):
        soak_profile.validate(broken)


def test_an_unknown_fault_kind_is_refused(smoke):
    broken = copy.deepcopy(smoke)
    broken["fault_cycle"].append("meteor")
    with pytest.raises(soak_profile.ProfileError, match="meteor"):
        soak_profile.validate(broken)


def test_a_profile_that_schedules_no_fault_is_refused(smoke):
    broken = copy.deepcopy(smoke)
    broken["first_fault_after_s"] = broken["duration_s"]
    with pytest.raises(soak_profile.ProfileError, match="schedules no fault"):
        soak_profile.validate(broken)


def test_a_plan_that_never_reaches_a_kind_is_refused(smoke):
    broken = copy.deepcopy(smoke)
    broken["fault_interval_s"] = 200  # only three faults fit: clock_step is never reached
    with pytest.raises(soak_profile.ProfileError, match="never reaches"):
        soak_profile.validate(broken)


def test_a_trend_without_a_limit_is_refused(smoke):
    broken = copy.deepcopy(smoke)
    del broken["criteria"]["trend"]["rss_bytes"]
    with pytest.raises(soak_profile.ProfileError, match="rss_bytes"):
        soak_profile.validate(broken)


def test_an_unknown_profile_name_lists_the_known_ones():
    with pytest.raises(soak_profile.ProfileError, match="72h"):
        soak_profile.load("nope")
