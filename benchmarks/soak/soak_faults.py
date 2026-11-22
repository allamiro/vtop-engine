"""The soak's fault injectors (#726).

Each injector does one thing to the lab, proves the fault landed, and lifts
it. A fault that did not land is an error that names what was observed, not a
passing step: a run whose partition never partitioned is the run that looks
healthiest.

  kill9           SIGKILL the engine, then start it again
  broker_restart  restart the Kafka broker (a graceful stop, then a start)
  partition       cut the engine off from the broker AND the store
  store_outage    cut the engine off from the store only
  clock_step      restart the engine with its wall clock moved by a step, hold
                  it there, then restart it with the clock back
  enospc          fill the engine's work directory until a write to it fails
                  with ENOSPC, hold it full, then free the space

These are the scenarios of scripts/live-chaos at the scale of one engine:
kill -9 is scenario 04's fault, the partition is 06's (through a proxy
instead of a network namespace, since the engine and the broker are
containers), and the clock step is 07's shim, built from the very source
that scenario embeds, and the full disk is the sweep's enospc leg
(scripts/live-chaos/engine/run-sweep.sh, ENGINE_SWEEP_FAULT=enospc: a
size-limited tmpfs at the work directory) made in a running engine's
container. See docs/SOAK_VALIDATION.md.

System model: a crash-recovery engine process, a partially synchronous
network, honest nodes. Invariants (checked by the evaluator, not here):
safety, the engine never commits past what it verified and the archive has no
hole; liveness, once the fault is lifted the engine catches up within the
recovery timeout. Timers may only affect liveness: the clock step must leave
safety intact and let liveness return once the clock is back. A full work
directory must stop batches, never commit one that was not verified, and let
the engine carry on from the ledger once space is back.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

import soak_lab
import soak_profile
from soak_record import Recorder, RunState

# How far the shim's recorded time may be from the host's, plus the step, for
# the step to count as applied. The step is an hour; the Docker VM's clock can
# differ from the host's by seconds, so this is wide, and still far from zero.
SKEW_TOLERANCE_S = 300


# How long, after the hold, the engine may take to show the disk-full condition
# in its log before the fault counts as one the engine never felt.
ENOSPC_LOG_GRACE_S = 30


class FaultError(RuntimeError):
    """A fault that did not land, or could not be lifted."""


@dataclass
class Context:
    lab: soak_lab.Lab
    profile: dict
    rec: Recorder
    state: RunState
    sleep: Callable[[float], None] = time.sleep
    now: Callable[[], float] = time.time


def _restart_engine(ctx: Context, reason: str) -> None:
    ctx.state.engine_started()
    ctx.rec.event("engine_start", lifetime=ctx.state.current_lifetime(), reason=reason)
    ctx.lab.start_engine(timeout=ctx.profile["recovery_timeout_s"])


def kill9(ctx: Context, fault: soak_profile.PlannedFault, occurrence: int) -> str:
    lab = ctx.lab
    if not lab.running("engine"):
        raise FaultError("the engine was not running when kill -9 was due")
    with ctx.state.planned_down():
        stopped = lab.kill_engine()
        if stopped.get("exit_code") != 137:
            raise FaultError(f"the engine exited with status {stopped.get('exit_code')} after SIGKILL, not 137")
        ctx.rec.event("engine_exit", lifetime=ctx.state.current_lifetime(), planned=True, exit_code=137)
        if fault.hold_s:
            ctx.sleep(fault.hold_s)
        _restart_engine(ctx, "after_kill9")
    return "SIGKILL (exit 137), restarted"


def broker_restart(ctx: Context, fault: soak_profile.PlannedFault, occurrence: int) -> str:
    lab = ctx.lab
    before = lab.state("kafka")
    if not before.get("running"):
        raise FaultError("the broker was not running when its restart was due")
    lab.require(lab.docker("restart", "--time", "30", lab.container("kafka"), timeout=120), "docker restart kafka")
    after = lab.wait_state(
        "kafka",
        lambda s: s.get("running") and s.get("health") == "healthy" and s.get("started_at") != before.get("started_at"),
        ctx.profile["recovery_timeout_s"],
        "after the broker restart",
    )
    if after["started_at"] == before["started_at"]:
        raise FaultError("the broker's start time did not change: it was not restarted")
    return f"broker restarted ({before['started_at']} -> {after['started_at']})"


def _cut(ctx: Context, proxies: list[str], hold_s: int) -> str:
    lab = ctx.lab
    reachable_after = [p for p in soak_lab.PROXY_PORTS if p not in proxies]
    # A control first: a probe that cannot connect even when nothing is cut
    # would "prove" every fault landed.
    for name in soak_lab.PROXY_PORTS:
        if not lab.engine_can_reach(name):
            raise FaultError(f"the engine could not reach {name} before the fault: there is nothing to cut")
    for name in proxies:
        lab.set_proxy(name, False)
    try:
        for name in proxies:
            if lab.proxy_enabled(name):
                raise FaultError(f"proxy {name} still reports enabled after it was disabled")
            if lab.engine_can_reach(name):
                raise FaultError(
                    f"the engine still reaches {name} through the proxy with it disabled: the fault did not land"
                )
        for name in reachable_after:
            if not lab.engine_can_reach(name):
                raise FaultError(f"the engine cannot reach {name}, which this fault leaves up")
        ctx.sleep(hold_s)
    finally:
        # Lifted even when the proof above failed: a fault must never outlive
        # its own error.
        for name in proxies:
            lab.set_proxy(name, True)
    deadline = time.monotonic() + 60
    for name in proxies:
        while not lab.engine_can_reach(name):
            if time.monotonic() >= deadline:
                raise FaultError(f"the engine cannot reach {name} 60 s after the proxy was enabled again")
            ctx.sleep(1)
    return f"{' and '.join(proxies)} unreachable from the engine for {hold_s} s, then restored"


def partition(ctx: Context, fault: soak_profile.PlannedFault, occurrence: int) -> str:
    return _cut(ctx, ["kafka", "minio"], fault.hold_s)


def store_outage(ctx: Context, fault: soak_profile.PlannedFault, occurrence: int) -> str:
    return _cut(ctx, ["minio"], fault.hold_s)


def _check_skew(ctx: Context, step: int) -> int:
    """The shim loaded and shifted the clock by `step` (0 for no shim)."""
    marker = ctx.lab.skew_marker()
    if step == 0:
        if marker is not None and abs(marker - ctx.now()) > SKEW_TOLERANCE_S:
            raise FaultError(f"a shim is still loaded: its recorded time is {marker - ctx.now():.0f} s from the host's")
        return 0
    if marker is None:
        raise FaultError(
            "the clock-skew shim did not load: it recorded no time (LD_PRELOAD refused, or a static binary)"
        )
    shifted = marker - ctx.now()
    if abs(shifted - step) > SKEW_TOLERANCE_S:
        raise FaultError(f"the shim's clock is {shifted:.0f} s from the host's; the step is {step} s")
    return round(shifted)


def _hold_watching_engine(ctx: Context, seconds: int, what: str) -> None:
    """Hold a fault for `seconds`, looking at the engine every second. The
    engine is meant to survive the hold; one that exits during it is a finding,
    not a planned stop, so the hold ends with an error and the sampler, which
    sees the exit outside any planned stop, records it."""
    for _ in range(seconds):
        ctx.sleep(1)
        state = ctx.lab.state("engine")
        if not state.get("running"):
            ctx.lab.set_preload(None)  # so whatever restarts it starts on the real clock
            raise FaultError(
                f"the engine exited (status {state.get('exit_code')}) during the {what} instead of surviving it"
            )


def clock_step(ctx: Context, fault: soak_profile.PlannedFault, occurrence: int) -> str:
    lab = ctx.lab
    step = soak_profile.clock_step_for(ctx.profile, occurrence)
    if not lab.running("engine"):
        raise FaultError("the engine was not running when its clock step was due")
    # An engine's clock cannot be moved from outside its process, so the step
    # is taken the way a host's clock step meets a daemon: the daemon is
    # started, or restarted, into a clock that is no longer where it was. Only
    # the two restarts are planned downtime; the hold between them is not.
    try:
        with ctx.state.planned_down():
            stopped = lab.stop_engine()
            ctx.rec.event(
                "engine_exit", lifetime=ctx.state.current_lifetime(), planned=True, exit_code=stopped.get("exit_code")
            )
            lab.set_preload(step)
            lab.clear_skew_marker()
            _restart_engine(ctx, f"clock_step_{step:+d}s")
        applied = _check_skew(ctx, step)
        _hold_watching_engine(ctx, fault.hold_s, f"{fault.hold_s} s hold with its clock {applied:+d} s off")
        with ctx.state.planned_down():
            stopped = lab.stop_engine()
            ctx.rec.event(
                "engine_exit", lifetime=ctx.state.current_lifetime(), planned=True, exit_code=stopped.get("exit_code")
            )
            lab.set_preload(None)
            lab.clear_skew_marker()
            _restart_engine(ctx, "clock_step_back")
        _check_skew(ctx, 0)
    finally:
        lab.set_preload(None)  # a fault must not leave the next start on a skewed clock
    return f"engine ran with its clock {applied:+d} s from the host's for {fault.hold_s} s, then restarted with it back"


def _enospc_lines(text: str) -> int:
    return sum(1 for line in text.splitlines() if soak_lab.ENOSPC_TEXT in line)


def _fill_work_until_full(ctx: Context) -> None:
    """Fill the work directory until a write to it fails with ENOSPC, and show
    that it does. The engine writes there too, so a few rounds are allowed for
    space it frees between ours."""
    lab = ctx.lab
    for _ in range(5):
        lab.fill_work()
        probe = lab.probe_work_write()
        if not probe.ok:
            if soak_lab.ENOSPC_TEXT not in probe.text():
                raise FaultError(f"a write to the work directory failed, but not with ENOSPC: {probe.text()[-200:]}")
            return
    raise FaultError("the work directory still accepted a write after it was filled five times: the fault did not land")


def _lift_enospc(ctx: Context, depth_before: int) -> None:
    """Free the space and show that writes work again. An engine that is not
    running has no mount to lift: the tmpfs went with its namespace."""
    lab = ctx.lab
    if not lab.running("engine"):
        return
    lab.unmount_work_tmpfs()
    depth = lab.work_mount_depth()
    if depth != depth_before:
        raise FaultError(f"the work directory has {depth} mounts after the tmpfs was lifted, it had {depth_before}")
    resumed = lab.probe_work_write()
    if not resumed.ok:
        raise FaultError(
            f"a write to the work directory still fails after the space was freed: {resumed.text()[-200:]}"
        )


def enospc(ctx: Context, fault: soak_profile.PlannedFault, occurrence: int) -> str:
    lab = ctx.lab
    if not lab.running("engine"):
        raise FaultError("the engine was not running when its disk-full fault was due")
    # A control first: a write that fails before anything is done would "prove"
    # that the disk was filled.
    control = lab.probe_work_write()
    if not control.ok:
        raise FaultError(f"a write to the work directory failed before the fault: {control.text()[-200:]}")
    depth_before = lab.work_mount_depth()
    since = lab.daemon_time()
    mounted = False
    try:
        lab.mount_work_tmpfs()
        mounted = True
        depth = lab.work_mount_depth()
        if depth != depth_before + 1:
            raise FaultError(
                f"the tmpfs is not mounted over the work directory ({depth} mounts, expected {depth_before + 1})"
            )
        _fill_work_until_full(ctx)
        _hold_watching_engine(ctx, fault.hold_s, f"{fault.hold_s} s hold with its work directory full")
        logged = _enospc_lines(lab.engine_log_since(since))
        waited = 0
        while not logged and waited < ENOSPC_LOG_GRACE_S:
            ctx.sleep(1)
            waited += 1
            logged = _enospc_lines(lab.engine_log_since(since))
        if not logged:
            raise FaultError(
                f"the engine ran {fault.hold_s + waited} s with its work directory full and logged no "
                f"'{soak_lab.ENOSPC_TEXT}': the fault did not reach it"
            )
    finally:
        # Lifted even when the proof above failed: a fault must never outlive
        # its own error.
        if mounted:
            _lift_enospc(ctx, depth_before)
    ctx.rec.event(
        "fault_probe",
        id=fault.id,
        fault="enospc",
        write_failed_enospc=True,
        engine_enospc_lines=logged,
        writes_resumed=True,
        tmpfs_bytes=soak_lab.ENOSPC_TMPFS_BYTES,
    )
    return (
        f"work directory full ({soak_lab.ENOSPC_TMPFS_BYTES} byte tmpfs, writes failed with ENOSPC) for "
        f"{fault.hold_s} s; the engine logged {logged} line(s) of it; space freed, writes resumed"
    )


def preflight_enospc(ctx: Context) -> None:
    """Before the load starts: lay the tmpfs over the idle engine's work
    directory and lift it. A host whose Docker refuses a privileged helper, or
    whose engine image has no nsenter, finds out now and not at the first
    disk-full fault, hours in."""
    lab = ctx.lab
    depth = lab.work_mount_depth()
    lab.mount_work_tmpfs()
    try:
        if lab.work_mount_depth() != depth + 1:
            raise FaultError("the tmpfs did not appear over the engine's work directory: the enospc fault cannot run")
    finally:
        lab.unmount_work_tmpfs()
    if lab.work_mount_depth() != depth:
        raise FaultError("the tmpfs over the engine's work directory could not be lifted")


INJECTORS = {
    "kill9": kill9,
    "broker_restart": broker_restart,
    "partition": partition,
    "store_outage": store_outage,
    "clock_step": clock_step,
    "enospc": enospc,
}

assert set(INJECTORS) == set(soak_profile.FAULT_KINDS), "every fault kind in a profile has an injector"


def inject(ctx: Context, fault: soak_profile.PlannedFault, occurrence: int) -> str:
    """Inject one planned fault and return what happened, in a sentence."""
    return INJECTORS[fault.kind](ctx, fault, occurrence)
