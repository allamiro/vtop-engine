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

These are the scenarios of scripts/live-chaos at the scale of one engine:
kill -9 is scenario 04's fault, the partition is 06's (through a proxy
instead of a network namespace, since the engine and the broker are
containers), and the clock step is 07's shim, built from the very source
that scenario embeds. See docs/SOAK_VALIDATION.md.

System model: a crash-recovery engine process, a partially synchronous
network, honest nodes. Invariants (checked by the evaluator, not here):
safety, the engine never commits past what it verified and the archive has no
hole; liveness, once the fault is lifted the engine catches up within the
recovery timeout. Timers may only affect liveness: the clock step must leave
safety intact and let liveness return once the clock is back.
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


def clock_step(ctx: Context, fault: soak_profile.PlannedFault, occurrence: int) -> str:
    lab = ctx.lab
    step = soak_profile.clock_step_for(ctx.profile, occurrence)
    if not lab.running("engine"):
        raise FaultError("the engine was not running when its clock step was due")
    # An engine's clock cannot be moved from outside its process, so the step
    # is taken the way a host's clock step meets a daemon: the daemon is
    # started, or restarted, into a clock that is no longer where it was.
    with ctx.state.planned_down():
        stopped = lab.stop_engine()
        ctx.rec.event(
            "engine_exit", lifetime=ctx.state.current_lifetime(), planned=True, exit_code=stopped.get("exit_code")
        )
        lab.set_preload(step)
        lab.clear_skew_marker()
        _restart_engine(ctx, f"clock_step_{step:+d}s")
        applied = _check_skew(ctx, step)
        ctx.sleep(fault.hold_s)
        stopped = lab.stop_engine()
        ctx.rec.event(
            "engine_exit", lifetime=ctx.state.current_lifetime(), planned=True, exit_code=stopped.get("exit_code")
        )
        lab.set_preload(None)
        lab.clear_skew_marker()
        _restart_engine(ctx, "clock_step_back")
        _check_skew(ctx, 0)
    return f"engine ran with its clock {applied:+d} s from the host's for {fault.hold_s} s, then restarted with it back"


INJECTORS = {
    "kill9": kill9,
    "broker_restart": broker_restart,
    "partition": partition,
    "store_outage": store_outage,
    "clock_step": clock_step,
}

assert set(INJECTORS) == set(soak_profile.FAULT_KINDS), "every fault kind in a profile has an injector"


def inject(ctx: Context, fault: soak_profile.PlannedFault, occurrence: int) -> str:
    """Inject one planned fault and return what happened, in a sentence."""
    return INJECTORS[fault.kind](ctx, fault, occurrence)
