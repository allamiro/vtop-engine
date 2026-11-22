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
    # the last preload None is the one that guarantees the next start is on the real clock
    assert log == ["stop", "preload 3600", "start", "stop", "preload None", "start", "preload None"]
    assert "+3600" in detail
    assert [e["reason"] for e in events(ctx) if e["kind"] == "engine_start"] == ["clock_step_+3600s", "clock_step_back"]
    assert ctx.state.current_lifetime() == 3


def test_the_second_clock_step_goes_the_other_way(ctx):
    soak_faults.inject(ctx, planned("clock_step", 25), 1)
    assert "preload -3600" in ctx.lab.log


def test_an_engine_that_exits_during_the_skewed_hold_is_a_failure_not_a_planned_stop(ctx):
    seconds = {"n": 0}

    def sleep(_):
        seconds["n"] += 1
        if seconds["n"] == 5:
            ctx.lab.engine_running = False  # it dies on its own, mid-hold

    ctx.sleep = sleep
    with pytest.raises(soak_faults.FaultError, match="exited .* during the .*hold"):
        soak_faults.inject(ctx, planned("clock_step", 25), 0)
    assert ctx.lab.preload is None  # whatever restarts it starts on the real clock
    assert ctx.state.is_down_planned() is False  # the sampler will see this exit as unplanned
    assert [e for e in events(ctx) if e["kind"] == "engine_exit"][-1]["planned"] is True  # only the deliberate stop
    assert len([e for e in events(ctx) if e["kind"] == "engine_exit"]) == 1


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


# -- enospc -------------------------------------------------------------------


def sleeps(ctx):
    """Make the injector's sleeps visible in the lab's log, in order with its calls."""
    ctx.sleep = lambda s: ctx.lab.log.append("sleep")


def test_enospc_fills_the_work_directory_holds_it_full_and_frees_the_space(ctx):
    sleeps(ctx)
    detail = soak_faults.inject(ctx, planned("enospc", 5), 0)
    log = ctx.lab.log
    assert log.index("mount tmpfs") < log.index("fill") < log.index("sleep") < log.index("unmount tmpfs")
    assert log.count("sleep") >= 5  # held for the fault's window, then lifted
    assert ctx.lab.tmpfs is False
    assert "ENOSPC" in detail and "writes resumed" in detail


def test_enospc_records_what_it_proved_for_the_evaluator(ctx):
    soak_faults.inject(ctx, planned("enospc", 5), 0)
    probe = next(e for e in events(ctx) if e["kind"] == "fault_probe")
    assert probe["id"] == 1 and probe["fault"] == "enospc"
    assert probe["write_failed_enospc"] is True and probe["writes_resumed"] is True
    assert probe["engine_enospc_lines"] == 1


def test_enospc_starts_with_a_control_write_that_must_work(ctx):
    ctx.lab.control_fails = True
    with pytest.raises(soak_faults.FaultError, match="failed before the fault"):
        soak_faults.inject(ctx, planned("enospc", 5), 0)
    assert "mount tmpfs" not in ctx.lab.log


def test_enospc_that_never_filled_the_directory_is_an_error_and_the_tmpfs_is_lifted(ctx):
    ctx.lab.fill_lands = False
    with pytest.raises(soak_faults.FaultError, match="the fault did not land"):
        soak_faults.inject(ctx, planned("enospc", 5), 0)
    assert ctx.lab.tmpfs is False and "unmount tmpfs" in ctx.lab.log


def test_enospc_whose_write_failed_for_another_reason_is_not_proven(ctx):
    ctx.lab.fail_text = "sh: can't create /data/work/.soak-probe: Permission denied"
    with pytest.raises(soak_faults.FaultError, match="not with ENOSPC"):
        soak_faults.inject(ctx, planned("enospc", 5), 0)
    assert ctx.lab.tmpfs is False


def test_enospc_the_engine_never_felt_is_an_error_after_a_grace_period(ctx):
    sleeps(ctx)
    ctx.lab.engine_felt_it = False
    with pytest.raises(soak_faults.FaultError, match="logged no 'No space left on device'"):
        soak_faults.inject(ctx, planned("enospc", 5), 0)
    assert ctx.lab.log.count("sleep") == 5 + soak_faults.ENOSPC_LOG_GRACE_S
    assert ctx.lab.tmpfs is False
    assert not [e for e in events(ctx) if e["kind"] == "fault_probe"]


def test_enospc_whose_space_is_not_freed_is_an_error(ctx):
    ctx.lab.unmount_leaves_mount = True
    with pytest.raises(soak_faults.FaultError, match="mounts after the tmpfs was lifted"):
        soak_faults.inject(ctx, planned("enospc", 5), 0)


def test_enospc_whose_writes_do_not_resume_is_an_error(ctx):
    ctx.lab.freed_dir_still_refuses = True
    with pytest.raises(soak_faults.FaultError, match="still fails after the space was freed"):
        soak_faults.inject(ctx, planned("enospc", 5), 0)


def test_enospc_where_the_tmpfs_did_not_mount_is_an_error_not_a_pass(ctx):
    ctx.lab.mount_refused = True
    with pytest.raises(soak_faults.soak_lab.LabError, match="permission denied"):
        soak_faults.inject(ctx, planned("enospc", 5), 0)
    assert "unmount tmpfs" not in ctx.lab.log and not [e for e in events(ctx) if e["kind"] == "fault_probe"]


def test_an_engine_that_exits_while_its_disk_is_full_is_a_finding_not_a_planned_stop(ctx):
    seconds = {"n": 0}

    def sleep(_):
        seconds["n"] += 1
        if seconds["n"] == 2:
            ctx.lab.engine_running = False  # it dies on its own, mid-hold

    ctx.sleep = sleep
    with pytest.raises(soak_faults.FaultError, match="exited .* during the .*hold"):
        soak_faults.inject(ctx, planned("enospc", 5), 0)
    assert ctx.state.is_down_planned() is False  # the sampler sees this exit as unplanned
    assert "unmount tmpfs" not in ctx.lab.log  # the mount went with the engine's namespace
    assert not [e for e in events(ctx) if e["kind"] == "engine_exit"]


def test_enospc_with_the_engine_already_down_is_an_error_not_a_pass(ctx):
    ctx.lab.engine_running = False
    with pytest.raises(soak_faults.FaultError, match="not running"):
        soak_faults.inject(ctx, planned("enospc", 5), 0)


def test_the_preflight_lays_the_tmpfs_over_the_idle_engine_and_lifts_it(ctx):
    soak_faults.preflight_enospc(ctx)
    assert ctx.lab.log == ["mount tmpfs", "unmount tmpfs"]


def test_the_preflight_refuses_a_host_that_cannot_mount_or_lift_it(ctx):
    ctx.lab.mount_refused = True
    with pytest.raises(soak_faults.soak_lab.LabError, match="permission denied"):
        soak_faults.preflight_enospc(ctx)
    ctx.lab.mount_refused = False
    ctx.lab.unmount_leaves_mount = True
    with pytest.raises(soak_faults.FaultError, match="could not be lifted"):
        soak_faults.preflight_enospc(ctx)


def test_the_preflight_notices_a_mount_that_did_not_appear(ctx):
    ctx.lab.work_mount_depth = lambda: 1  # the mount command succeeded and changed nothing
    with pytest.raises(soak_faults.FaultError, match="did not appear"):
        soak_faults.preflight_enospc(ctx)
