"""Each fault injector lands its fault, proves it, and lifts it (#726)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import soak_faults  # noqa: E402
import soak_profile  # noqa: E402
from soak_fakelab import FakeLab  # noqa: E402
from soak_record import Recorder, RunState  # noqa: E402

PROFILE = soak_profile.load("ci-smoke")


@pytest.fixture
def ctx(tmp_path):
    lab = FakeLab()
    lab.now = 1_700_000_000
    rec = Recorder(tmp_path)
    state = RunState()
    state.engine_started()
    context = soak_faults.Context(
        lab=lab, profile=PROFILE, rec=rec, state=state, sleep=lambda s: None, now=lambda: lab.now
    )
    yield context
    rec.close()


def planned(kind, hold=0):
    return soak_profile.PlannedFault(id=1, at_s=60, kind=kind, hold_s=hold)


def events(ctx):
    ctx.rec.close()
    import json

    return [json.loads(line) for line in (ctx.rec.run_dir / "events.jsonl").read_text().splitlines()]


def test_every_kind_a_profile_can_name_has_an_injector():
    assert set(soak_faults.INJECTORS) == set(soak_profile.FAULT_KINDS)


def test_kill9_kills_with_sigkill_restarts_and_starts_a_new_lifetime(ctx):
    detail = soak_faults.inject(ctx, planned("kill9"), 0)
    assert ctx.lab.log == ["kill", "start"] and "137" in detail
    assert ctx.state.current_lifetime() == 2
    recorded = events(ctx)
    assert [e["kind"] for e in recorded] == ["engine_exit", "engine_start"]
    assert recorded[0]["planned"] is True


def test_kill9_that_did_not_die_by_sigkill_is_an_error(ctx):
    ctx.lab.kill_exit = 0
    with pytest.raises(soak_faults.FaultError, match="not 137"):
        soak_faults.inject(ctx, planned("kill9"), 0)


def test_kill9_with_the_engine_already_down_is_an_error_not_a_pass(ctx):
    ctx.lab.engine_running = False
    with pytest.raises(soak_faults.FaultError, match="not running"):
        soak_faults.inject(ctx, planned("kill9"), 0)


def test_the_engine_is_down_on_purpose_only_inside_the_fault(ctx):
    soak_faults.inject(ctx, planned("kill9"), 0)
    assert ctx.state.is_down_planned() is False


def test_broker_restart_restarts_the_container_and_sees_a_new_start_time(ctx):
    detail = soak_faults.inject(ctx, planned("broker_restart"), 0)
    assert "docker restart --time 30 c-kafka" in ctx.lab.log and "t0 -> t1" in detail


def test_partition_cuts_both_proxies_proves_it_and_lifts_it(ctx):
    detail = soak_faults.inject(ctx, planned("partition", 20), 0)
    log = ctx.lab.log
    assert log.index("proxy kafka False") < log.index("proxy kafka True")
    assert ctx.lab.proxies == {"kafka": True, "minio": True}
    assert "kafka and minio unreachable" in detail


def test_store_outage_cuts_only_the_store(ctx):
    soak_faults.inject(ctx, planned("store_outage", 20), 0)
    assert "proxy kafka False" not in ctx.lab.log and "proxy minio False" in ctx.lab.log


def test_a_partition_that_did_not_land_is_an_error_and_the_proxies_come_back(ctx):
    ctx.lab.reach_ignores_proxy = True
    with pytest.raises(soak_faults.FaultError, match="the fault did not land"):
        soak_faults.inject(ctx, planned("partition", 20), 0)
    assert ctx.lab.proxies == {"kafka": True, "minio": True}


def test_a_probe_that_cannot_connect_even_before_the_fault_is_an_error(ctx):
    ctx.lab.cannot_reach_at_all = True
    with pytest.raises(soak_faults.FaultError, match="could not reach kafka before the fault"):
        soak_faults.inject(ctx, planned("partition", 20), 0)
    assert "proxy kafka False" not in ctx.lab.log


def test_clock_step_runs_the_engine_in_a_shifted_clock_then_back(ctx):
    detail = soak_faults.inject(ctx, planned("clock_step", 25), 0)
    log = [x for x in ctx.lab.log if x.startswith("preload") or x in ("stop", "start")]
    assert log == ["stop", "preload 3600", "start", "stop", "preload None", "start"]
    assert "+3600" in detail
    assert [e["reason"] for e in events(ctx) if e["kind"] == "engine_start"] == ["clock_step_+3600s", "clock_step_back"]
    assert ctx.state.current_lifetime() == 3


def test_the_second_clock_step_goes_the_other_way(ctx):
    soak_faults.inject(ctx, planned("clock_step", 25), 1)
    assert "preload -3600" in ctx.lab.log


def test_a_shim_that_did_not_load_fails_the_step(ctx):
    ctx.lab.shim_loads = False
    with pytest.raises(soak_faults.FaultError, match="did not load"):
        soak_faults.inject(ctx, planned("clock_step", 25), 0)


def test_a_shim_that_shifted_the_wrong_amount_fails_the_step(ctx):
    original = ctx.lab.start_engine

    def wrong(timeout=120):
        original(timeout)
        ctx.lab.skew_marker_value = ctx.lab.now + 60

    ctx.lab.start_engine = wrong
    with pytest.raises(soak_faults.FaultError, match="from the host's; the step is 3600"):
        soak_faults.inject(ctx, planned("clock_step", 25), 0)
