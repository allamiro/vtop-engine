"""Tests for benchmarks/lib/netem.py (#477): the link a netem scenario asks
for, the `tc` program it becomes, and the install/remove discipline around a
run — all against a scripted command runner, so nothing here starts a
container, holds CAP_NET_ADMIN or puts a qdisc on the machine running the
suite.

The driver's whole job is to make the pipe lossy, policed and queued, which
toxiproxy cannot do. A shape that is built wrong still installs, still runs,
and still gets recorded, so the program itself — the argument lists — is what
these tests read.
"""

import json

import pytest

from lib import netem
from lib.netem import (
    ENGINE_IFACE,
    IFB_DEV,
    CalibrationError,
    NetemShape,
    bdp_bytes,
    gemodel_params,
)
from lib.scenario import DEFAULTS, Scenario
from lib.shaping import (
    SHAPING_COLUMNS,
    Shape,
    ShapingError,
    require_endpoint_reaches_the_shape,
    shape_from_scenario,
    shaped_run,
)
from run_matrix import IncomparableRuns, matrix_row, refuse_mixed_drivers

# What an unshaped middlebox answers. Both strings are real iproute2 output:
# the kernel's own default root qdisc, which is not ours, and the error a
# device that was never created gives.
CLEAN_QDISC_SHOW = "qdisc noqueue 0: root refcnt 2"
ABSENT_DEVICE = f'Cannot find device "{IFB_DEV}"'


# What `ip -o -4 addr show` reports on the middlebox: the store side on the
# compose file's pinned 10.77.0.0/24, and the engine side anywhere else. The
# device NAMES are deliberately not eth0/eth1 in this fixture — the driver must
# not be able to pass by assuming them (review: Compose's `priority` orders
# attachment but does not determine device names).
TWO_INTERFACES = (
    "1: lo    inet 127.0.0.1/8 scope host lo\\       valid_lft forever\n"
    "2: enp1s0    inet 172.18.0.3/16 brd 172.18.255.255 scope global enp1s0\n"
    "3: enp2s0    inet 10.77.0.2/24 brd 10.77.0.255 scope global enp2s0\n"
)


class ScriptedMiddlebox:
    """Answers each `tc`/`ip` command from a script and records what was run.

    The real driver reaches the middlebox through `docker exec`; this stands
    in its place, so a test can refuse one command and then read what the
    driver did about it.
    """

    def __init__(self, qdisc_show=CLEAN_QDISC_SHOW, ifb_present=False,
                 refuse=None, refusal=(2, "RTNETLINK answers: Operation not permitted"),
                 raise_on=None, addr_show=None):
        self.calls = []
        self.qdisc_show = qdisc_show
        self.ifb_present = ifb_present
        self.refuse = refuse
        self.refusal = refusal
        self.raise_on = raise_on
        # Two addressed interfaces, as the compose file gives the middlebox:
        # one on the store subnet and one facing the engine. The driver picks
        # the engine's by ELIMINATING the store's, never by name, because
        # Compose does not promise which device gets which name.
        self.addr_show = TWO_INTERFACES if addr_show is None else addr_show

    def __call__(self, argv):
        self.calls.append(list(argv))
        if argv[:4] == ["ip", "-o", "-4", "addr"]:
            return 0, self.addr_show
        if argv[:3] == ["tc", "qdisc", "show"]:
            return 0, self.qdisc_show
        if argv[:3] == ["ip", "link", "add"]:
            # The atomic claim: the kernel refuses a second `add` of a device
            # that exists, which is what makes it a lock rather than a check.
            if self.ifb_present:
                return 1, 'RTNETLINK answers: File exists'
            self.ifb_present = True
            return 0, ""
        if argv[:3] == ["ip", "link", "show"]:
            if self.ifb_present:
                return 0, f"7: {IFB_DEV}: <BROADCAST,NOARP> mtu 1500 qdisc noop state DOWN"
            return 1, ABSENT_DEVICE
        joined = " ".join(argv)
        if self.raise_on and self.raise_on in joined:
            # What `docker_exec` raises when the middlebox stops answering
            # (a timeout, a container that went away) rather than answering
            # with a non-zero exit.
            raise ShapingError(f"`{joined}` in the middlebox did not finish in 20.0s")
        if self.refuse and self.refuse in joined:
            return self.refusal
        return 0, ""

    def installed(self):
        """What the driver actually ran, minus the two commands it uses to
        LOOK at the middlebox before touching it."""
        looks = (["tc", "qdisc", "show"], ["ip", "link", "show"])
        return [c for c in self.calls
                if c[:3] not in looks and c[:4] != ["ip", "-o", "-4", "addr"]]


def full_shape(**overrides):
    """A shape with every knob turned on, so one program carries them all.
    101 ms and 21 ms are odd on purpose: the halves must not both round the
    same way or the emulated round trip is not the one the scenario asked
    for."""
    values = dict(latency_ms=101, jitter_ms=21, loss_pct=1.0, loss_model="random",
                  bottleneck_kbps=10000, buffer_bdp=1.0, policer_kbps=5000)
    values.update(overrides)
    return NetemShape(**values)


def netem_scenario(**values):
    """A netem scenario as the loader hands one over: flat keys, read with
    `.get`."""
    return Scenario({"shaping_driver": "netem", **values})


def arg_after(cmd, word):
    """The value `tc` reads for a keyword, e.g. `rate` -> `10000kbit`."""
    return cmd[cmd.index(word) + 1]


def qdiscs_on(program, dev):
    """Every `tc qdisc add` in the program for one device, in order."""
    return [c for c in program
            if c[:3] == ["tc", "qdisc", "add"] and arg_after(c, "dev") == dev]


def only_filter(program):
    filters = [c for c in program if c[:3] == ["tc", "filter", "add"]]
    assert len(filters) == 1, (
        "the upload is redirected by exactly one ingress filter; a second one would "
        f"mirror every packet twice and double the offered load, got {filters}")
    return filters[0]


# --------------------------------------------------------------------------
# The shape a scenario asks for
# --------------------------------------------------------------------------


def test_a_netem_scenario_with_no_shape_at_all_is_refused():
    # The driver has no api_url to switch on, so naming it IS the opt-in. A
    # netem row with no impairment would be an unshaped run recorded as a
    # shaped one, and the matrix would compare it against a real link.
    with pytest.raises(ValueError, match="no delay, loss, bottleneck or policer"):
        NetemShape.from_scenario(netem_scenario())


def test_jitter_without_a_latency_to_spread_is_refused():
    with pytest.raises(ValueError, match="shaping_jitter_ms needs a shaping_latency_ms"):
        NetemShape.from_scenario(netem_scenario(shaping_jitter_ms=20, shaping_loss_pct=1.0))


def test_a_loss_model_the_kernel_does_not_have_is_refused_by_name():
    with pytest.raises(ValueError, match="shaping_loss_model") as exc:
        NetemShape.from_scenario(netem_scenario(shaping_loss_model="pareto",
                                                shaping_loss_pct=1.0))
    assert "pareto" in str(exc.value) and "gemodel" in str(exc.value), (
        "the refusal must name the model that was asked for and the ones that exist: "
        "a typo silently falling back to `random` would file a bursty-link claim "
        "against an independent-loss run")


def test_a_loss_model_with_no_loss_to_shape_is_refused():
    # The model names how loss ARRIVES, not how much. Accepting it with zero
    # loss would record `gemodel` on a row whose link never dropped a packet.
    with pytest.raises(ValueError, match="shaping_loss_pct is 0") as exc:
        NetemShape.from_scenario(netem_scenario(shaping_loss_model="gemodel",
                                                shaping_latency_ms=100))
    assert "shaping_loss_model" in str(exc.value), (
        "the refusal must name both keys; the fix is to set one of them, and the "
        "message is the only place the operator learns which")


def test_a_bottleneck_and_its_buffer_are_refused_apart():
    # tbf's queue IS the bottleneck buffer, so the rate and the depth are one
    # decision. Either alone would put a number in the summary against a
    # buffer depth nobody chose — and buffer depth is the whole difference
    # between a bufferbloated link and a shallow one.
    with pytest.raises(ValueError, match="shaping_buffer_bdp needs a shaping_bottleneck_kbps"):
        NetemShape.from_scenario(netem_scenario(shaping_buffer_bdp=1.0,
                                                shaping_latency_ms=100))
    with pytest.raises(ValueError, match="shaping_buffer_bdp needs a shaping_latency_ms") as exc:
        NetemShape.from_scenario(netem_scenario(shaping_buffer_bdp=1.0,
                                                shaping_bottleneck_kbps=10000))
    assert "bandwidth-delay product" in str(exc.value), (
        "a BDP multiple with no delay is zero bytes of buffer, and the message has to "
        "say why the two keys are one")
    with pytest.raises(ValueError, match="shaping_bottleneck_kbps needs a shaping_buffer_bdp"):
        NetemShape.from_scenario(netem_scenario(shaping_bottleneck_kbps=10000,
                                                shaping_latency_ms=100))


def test_a_link_that_drops_everything_is_refused():
    with pytest.raises(ValueError, match="shaping_loss_pct") as exc:
        NetemShape.from_scenario(netem_scenario(shaping_loss_pct=100))
    assert "measures nothing" in str(exc.value), (
        "at 100% loss no batch ever completes, so the run would soak until its "
        "deadline and file a zero as if the engine had produced it")
    with pytest.raises(ValueError, match="shaping_loss_pct"):
        NetemShape.from_scenario(netem_scenario(shaping_loss_pct=100.5))


def test_a_netem_knob_is_a_number_or_refused_by_name():
    # `float()` alone takes True as 1.0 and a stray string as a silent 0, and
    # the run would then apply and record a shape the scenario never asked
    # for.
    with pytest.raises(ValueError, match="shaping_loss_pct"):
        NetemShape.from_scenario(netem_scenario(shaping_loss_pct=True))
    with pytest.raises(ValueError, match="shaping_loss_pct"):
        NetemShape.from_scenario(netem_scenario(shaping_loss_pct="lossy"))
    with pytest.raises(ValueError, match="shaping_latency_ms"):
        NetemShape.from_scenario(netem_scenario(shaping_latency_ms=1.9))
    with pytest.raises(ValueError, match="shaping_loss_pct"):
        NetemShape.from_scenario(netem_scenario(shaping_loss_pct=-1))
    # Fractional loss is the point of a separate rule: 0.5% is a real link.
    assert NetemShape.from_scenario(netem_scenario(shaping_loss_pct=0.5)).loss_pct == 0.5
    assert NetemShape.from_scenario(netem_scenario(shaping_latency_ms="100")).latency_ms == 100


# --------------------------------------------------------------------------
# The tc program it becomes
# --------------------------------------------------------------------------


def test_the_bottleneck_carries_the_rate_and_the_buffer_it_was_configured_for():
    shape = full_shape()
    tbf = qdiscs_on(shape.tc_program(), IFB_DEV)[0]
    assert arg_after(tbf, "rate") == "10000kbit", (
        "tbf's rate IS the emulated bottleneck; a wrong unit or a dropped suffix here "
        "produces a link nobody configured and a calibration failure nobody can read")
    assert int(arg_after(tbf, "limit")) == shape.buffer_bytes(), (
        "tbf's `limit` is the bottleneck buffer, and the buffer is what decides whether "
        "a transport sees loss or standing delay — the two behaviours this driver exists "
        "to tell apart")
    assert shape.buffer_bytes() == bdp_bytes(10000, 101), (
        "at 1.0x BDP the queue must hold exactly one round trip of bytes; an arithmetic "
        "slip would file every buffer-depth comparison against the wrong depth")
    assert int(arg_after(tbf, "burst")) >= 1600, (
        "tbf refuses a burst below rate/HZ, so a shape that installs at all needs the "
        "floor — a refused qdisc fails the run rather than shaping it")


def test_the_impairment_is_the_bottlenecks_child_not_a_second_root():
    program = full_shape().tc_program()
    ifb_qdiscs = qdiscs_on(program, IFB_DEV)
    roots = [c for c in ifb_qdiscs if "root" in c]
    assert len(roots) == 1 and "tbf" in roots[0], (
        "only the bottleneck may be root: a netem installed as a second root would "
        "REPLACE tbf, and the run would measure delay and loss on an unlimited link "
        "while the summary recorded a bottleneck")
    child = [c for c in ifb_qdiscs if "netem" in c][0]
    assert arg_after(child, "parent") == "1:1", (
        "netem hangs under tbf so packets are rate-limited into the queue first and "
        "impaired second — the order a real bottleneck link applies them")
    assert arg_after(roots[0], "handle") == "1:", "the child's parent must be the tbf it names"
    assert ifb_qdiscs.index(roots[0]) < ifb_qdiscs.index(child), (
        "the parent must exist before the child is attached, or the kernel refuses the "
        "second command and the shape half-installs")


def test_the_upload_is_redirected_onto_the_ifb_and_the_policer_drops_ahead_of_it():
    program = full_shape().tc_program()
    ingress = [c for c in program if "ingress" in c and c[:3] == ["tc", "qdisc", "add"]]
    assert len(ingress) == 1 and arg_after(ingress[0], "dev") == ENGINE_IFACE, (
        "the ingress hook belongs on the interface the engine's packets ARRIVE on; "
        "anywhere else and the upload direction is not the one being shaped")
    filt = only_filter(program)
    assert program.index(ingress[0]) < program.index(filt), (
        "the filter attaches to the ingress qdisc, so the qdisc has to exist first")
    assert arg_after(filt, "parent") == "ffff:", "the filter must sit on the ingress hook"
    i_police, i_mirred = filt.index("police"), filt.index("mirred")
    assert i_police < i_mirred, (
        "a policer that ran AFTER the redirect would police nothing: the packet has "
        "already left for the ifb, where tbf's queue would absorb exactly the burst a "
        "policer is defined not to have")
    police = filt[i_police:i_mirred]
    assert arg_after(police, "rate") == "5000kbit", "the policer runs at the configured rate"
    assert arg_after(police, "conform-exceed") == "drop/pipe", (
        "exceed must DROP and conform must fall through to the redirect; anything else "
        "either queues (which is what a policer is not) or drops the whole flow")
    assert filt[i_mirred:] == ["mirred", "egress", "redirect", "dev", IFB_DEV], (
        "the redirect is the only path onto the ifb — without it the bottleneck and the "
        "impairment sit on a device no packet ever reaches, and the run is unshaped "
        "while the summary says netem")


def test_an_unpoliced_shape_installs_no_policer():
    filt = only_filter(full_shape(policer_kbps=0).tc_program())
    assert "police" not in filt, (
        "a policer drops above its rate with no queue at all; installing one nobody "
        "asked for would attribute its drops to the link under test")


def test_the_download_direction_carries_delay_and_never_loss():
    shape = full_shape()
    download = [c for c in qdiscs_on(shape.tc_program(), ENGINE_IFACE) if "netem" in c]
    assert len(download) == 1, "one netem on the way back to the engine, carrying its half"
    assert arg_after(download[0], "delay") == "50ms", "the download half of the round trip"
    assert "loss" not in download[0], (
        "loss lands on the DATA direction alone, so `shaping_loss_pct` means loss on the "
        "upload rather than a number whose end-to-end effect depends on which way a "
        "packet was going")
    assert shape.describe()["loss_direction"] == "upload", (
        "the summary has to record which direction the loss was on, or the number cannot "
        "be reproduced from the row")


def test_the_round_trip_splits_across_the_directions_with_the_odd_millisecond_on_the_upload():
    shape = full_shape(latency_ms=101, jitter_ms=21)
    assert shape.upload_delay_ms() + shape.download_delay_ms() == 101, (
        "the two halves ARE the configured round trip; a lost millisecond makes every "
        "latency-sensitive result a comparison against a link the scenario never named")
    assert shape.upload_delay_ms() == 51, (
        "the odd millisecond rides with the data, matching the toxiproxy driver's split "
        "so the two drivers' latency rows mean the same thing")
    upload = [c for c in qdiscs_on(shape.tc_program(), IFB_DEV) if "netem" in c][0]
    download = [c for c in qdiscs_on(shape.tc_program(), ENGINE_IFACE) if "netem" in c][0]
    assert arg_after(upload, "delay") == "51ms" and arg_after(download, "delay") == "50ms"
    # The jitter splits the same way, and as a normal distribution: netem's
    # default spread is uniform, and a real path's delay variation is not.
    assert upload[upload.index("delay") + 2:upload.index("delay") + 5] == [
        "11ms", "distribution", "normal"]
    assert download[download.index("delay") + 2:download.index("delay") + 5] == [
        "10ms", "distribution", "normal"]


# --------------------------------------------------------------------------
# How the loss arrives
# --------------------------------------------------------------------------


def test_gemodel_reproduces_the_requested_average_loss():
    for loss_pct in (0.5, 1.0, 5.0, 20.0):
        p, r = gemodel_params(loss_pct)
        # With the bad state losing everything, the stationary loss rate is
        # the fraction of time spent there: p / (p + r).
        average = 100.0 * p / (p + r)
        assert average == pytest.approx(loss_pct, abs=0.01), (
            f"a gemodel row at {loss_pct}% must average {loss_pct}% loss, or a bursty run "
            "cannot be compared against the random run at the same number — which is the "
            "only comparison the model exists for")
        assert 100.0 / r == pytest.approx(2.0), (
            "the mean burst is 1/r packets; a burst length that drifts with the loss "
            "percentage would make two gemodel rows two different links")


def test_a_bursty_link_and_a_random_link_are_the_same_average_and_a_different_link():
    bursty = full_shape(loss_model="gemodel", loss_pct=1.0)
    independent = full_shape(loss_model="random", loss_pct=1.0)
    bursty_netem = [c for c in qdiscs_on(bursty.tc_program(), IFB_DEV) if "netem" in c][0]
    random_netem = [c for c in qdiscs_on(independent.tc_program(), IFB_DEV) if "netem" in c][0]
    assert arg_after(random_netem, "loss") == "random"
    assert arg_after(bursty_netem, "loss") == "gemodel"
    assert bursty_netem != random_netem, (
        "the same `shaping_loss_pct` must reach the kernel as two different qdiscs: if "
        "both rows installed the same model, the bursty-versus-independent comparison "
        "this driver was built for would be a comparison of a link with itself")
    p, r = gemodel_params(1.0)
    assert bursty_netem[bursty_netem.index("loss") + 2:] == [f"{p}%", f"{r}%"], (
        "netem reads gemodel's p and r in that order; swapping them would invert the "
        "burst structure while the recorded loss percentage stayed right")


# --------------------------------------------------------------------------
# Installing it, and taking it off again
# --------------------------------------------------------------------------


def test_a_shape_that_cannot_be_installed_fails_the_run():
    box = ScriptedMiddlebox(refuse="mirred")
    shape = full_shape()
    proceeded = []
    with pytest.raises(ShapingError, match="the middlebox refused") as exc:
        with netem.shaped(shape, run=box, log=lambda _: None):
            proceeded.append("the scenario")
    assert proceeded == [], (
        "a run whose pipe would not shape must not run at all: it would archive at line "
        "rate and file the numbers under the shape it asked for")
    assert "Operation not permitted" in str(exc.value), (
        "the middlebox's own words are the only clue to a missing CAP_NET_ADMIN or a "
        "kernel without the module")
    assert box.calls[-4:] == shape.teardown_program("enp1s0"), (
        "half a shape is worse than none — it is a link nobody chose, and the summary "
        "would name the one that was asked for; every teardown command runs before the "
        "failure is reported")


def test_a_middlebox_that_is_already_shaped_is_refused_not_stacked_on():
    occupied = ScriptedMiddlebox(qdisc_show="qdisc netem 8001: root refcnt 2 delay 50ms")
    with pytest.raises(ShapingError, match="already shaped") as exc:
        netem.apply(full_shape(), occupied)
    assert "netem 8001" in str(exc.value), (
        "the refusal has to say WHAT it found: the operator decides whether it is a live "
        "neighbour or an interrupted run's leftover, and this driver cannot")
    # The claim is taken and released; nothing else is touched. The leftover
    # qdiscs in particular are NOT deleted — they belong to whoever installed
    # them, and removing them would leave that run measuring an unshaped link
    # while its summary said netem.
    assert occupied.installed() == [["ip", "link", "add", IFB_DEV, "type", "ifb"],
                                    ["ip", "link", "del", IFB_DEV]], (
        "nothing is installed on top and nothing is deleted — one run cannot tell a live "
        "neighbour from a stale one, and guessing either way corrupts a measurement")

    leftover = ScriptedMiddlebox(ifb_present=True)
    with pytest.raises(ShapingError, match=f"{IFB_DEV} already exists"):
        netem.apply(full_shape(), leftover)
    # The claim is ATTEMPTED and refused by the kernel — that attempt IS the
    # lock, and it is the only command the loser gets to run. Nothing is
    # installed, and the leftover ifb is not deleted: this run cannot tell a
    # live neighbour from a stale leftover, and guessing either way corrupts
    # somebody's measurement.
    assert leftover.installed() == [["ip", "link", "add", IFB_DEV, "type", "ifb"]], (
        "an ifb from an interrupted run still carries its qdiscs; reusing it would shape "
        "this run with the last one's link")


def test_every_teardown_command_is_attempted_and_the_ones_that_stayed_are_reported():
    shape = full_shape()
    box = ScriptedMiddlebox(refuse="qdisc del dev enp1s0 root")
    with pytest.raises(ShapingError, match="did not unshape") as exc:
        netem.clear(shape, box, "enp1s0")
    assert box.calls == shape.teardown_program("enp1s0"), (
        "a removal that stopped at the first failure would leave the rest of the shape "
        "installed, and the NEXT scenario would run through a link it never asked for")
    assert "qdisc del dev enp1s0 root" in str(exc.value), (
        "the report names the command that stayed, because unshaping by hand is the only "
        "way out and the operator needs the exact one")
    assert "Operation not permitted" in str(exc.value)


def test_a_middlebox_that_stops_answering_mid_teardown_still_gets_every_command():
    # `docker_exec` RAISES on a timeout or a vanished container rather than
    # answering; one such command must not skip the others.
    shape = full_shape()
    box = ScriptedMiddlebox(raise_on="qdisc del dev enp1s0 ingress")
    with pytest.raises(ShapingError, match="did not unshape") as exc:
        netem.clear(shape, box, "enp1s0")
    assert box.calls == shape.teardown_program("enp1s0"), (
        "the ifb outlives a middlebox that went quiet, and a leftover device is what the "
        "next run's refusal trips over")
    assert "did not finish in" in str(exc.value), (
        "a transport failure and a refused removal both mean the pipe may still be "
        "shaped, so both are reported the same way")


def test_an_absent_device_is_not_a_failed_teardown():
    # A shape with no bottleneck installs no qdisc on the ifb, so removing one
    # is expected to fail. A run must not be failed on the way out for
    # removing what was never there.
    #
    # KNOWN GAP, reported rather than pinned: iproute2 >= 5 answers a missing
    # ROOT qdisc with "Error: Cannot delete qdisc with handle of zero." and a
    # missing ingress with "Error: Cannot find specified qdisc on specified
    # device.", and `_absent` matches neither.
    shape = full_shape()
    box = ScriptedMiddlebox(refuse=" del ", refusal=(1, ABSENT_DEVICE))
    netem.clear(shape, box, "enp1s0")
    assert box.calls == shape.teardown_program("enp1s0"), "every piece is still attempted"
    # best_effort is for the rollback inside `apply`, where the caller is
    # already reporting a failure of its own.
    hard = ScriptedMiddlebox(refuse=" del ")
    netem.clear(shape, hard, "enp1s0", best_effort=True)


# --------------------------------------------------------------------------
# Calibration: the link the emulator actually produced
# --------------------------------------------------------------------------


# A real iperf3 `-J` report, cut to the keys the probe reads.
IPERF3_REPORT = json.dumps({
    "start": {"connected": [{"local_host": "172.30.0.9", "remote_host": "172.30.0.4"}],
              "version": "iperf 3.9", "timestamp": {"timesecs": 1757836800}},
    "intervals": [{"sum": {"start": 0, "end": 1.0, "bits_per_second": 9903112.0}}],
    "end": {"sum_sent": {"start": 0, "end": 5.0, "bytes": 6170888,
                         "bits_per_second": 9873421.5, "retransmits": 12},
            "sum_received": {"bits_per_second": 9701004.0},
            "cpu_utilization_percent": {"host_total": 1.7}},
})


def test_a_measured_link_inside_the_band_passes_and_one_outside_it_names_both_numbers():
    shape = full_shape(bottleneck_kbps=10000)
    for measured in (10.0, 9.0, 11.0, 9.6):
        netem.check_calibration(shape, measured)
    with pytest.raises(CalibrationError) as exc:
        netem.check_calibration(shape, 7.4)
    assert "7.4" in str(exc.value) and "10.0" in str(exc.value), (
        "the refusal must carry the measured number AND the configured one: they are the "
        "evidence that the emulator, not the engine, is what went wrong")
    with pytest.raises(CalibrationError, match="outside"):
        netem.check_calibration(shape, 14.2)
    # No bottleneck, nothing to calibrate against: a delay-only or loss-only
    # link has no configured rate to measure.
    netem.check_calibration(NetemShape(latency_ms=100), 942.0)


def test_the_probe_reads_an_iperf3_report_and_refuses_anything_else():
    asked = []

    def run(argv):
        asked.append(argv)
        return 0, IPERF3_REPORT

    assert netem.probe_throughput_mbps(run, "netem", seconds=5) == 9.701, (
        "the RECEIVED rate in megabits is the link being calibrated: `sum_sent` counts "
        "bytes the client handed to its socket, and at the far end of a token bucket that "
        "includes everything still sitting in the buffer — a correctly shaped link would "
        "read fast and fail a calibration the measurement itself invented")
    assert asked[0][:2] == ["iperf3", "-c"] and "-J" in asked[0], (
        "the probe must ask for JSON: iperf3's human output would parse as garbage and "
        "the run would be refused for the emulator's health rather than its own")
    assert "-O" in asked[0], (
        "the first seconds fill the bottleneck's queue and run below the rate; averaging "
        "them in reads the link slow and refuses a healthy emulator")

    # sum_received is the subject, but a report that carries only the sender's
    # view still yields a number rather than a failure the operator cannot act
    # on.
    sent_only = json.dumps({"end": {"sum_sent": {"bits_per_second": 9873421.5}}})
    assert netem.probe_throughput_mbps(lambda _a: (0, sent_only), "netem") == 9.873, (
        "a probe that cannot find the receiver's view falls back to the sender's rather "
        "than reporting a broken emulator")

    with pytest.raises(CalibrationError, match="not an iperf3 JSON report"):
        netem.probe_throughput_mbps(lambda _a: (0, "iperf3: warning - unable to see server"),
                                    "netem")
    with pytest.raises(CalibrationError, match="not an iperf3 JSON report"):
        netem.probe_throughput_mbps(lambda _a: (0, json.dumps({"end": {}})), "netem")
    with pytest.raises(CalibrationError) as exc:
        netem.probe_throughput_mbps(lambda _a: (1, "iperf3: error - unable to connect"), "netem")
    assert "--profile netem" in str(exc.value), (
        "an unreachable probe is almost always a stack that was never started, and the "
        "message is where the operator finds the command that starts it")


# --------------------------------------------------------------------------
# The dispatch: which driver a scenario gets
# --------------------------------------------------------------------------


def test_the_scenario_chooses_its_driver_and_an_unknown_one_is_refused():
    assert isinstance(shape_from_scenario(netem_scenario(shaping_latency_ms=100)), NetemShape)
    # toxiproxy stays the default: rewriting the bundled scenarios onto a new
    # driver would make their recorded numbers incomparable to their future
    # ones.
    default = shape_from_scenario(Scenario({"shaping_api_url": "http://127.0.0.1:8474",
                                            "shaping_bandwidth_kbps": 1250}))
    assert isinstance(default, Shape) and default.driver == "toxiproxy"
    assert shape_from_scenario(Scenario({})) is None, "no api_url and no driver is unshaped"
    with pytest.raises(ValueError, match="shaping_driver 'netm'") as exc:
        shape_from_scenario(Scenario({"shaping_driver": "netm"}))
    assert "toxiproxy" in str(exc.value) and "netem" in str(exc.value), (
        "a misspelled driver must name the ones that exist; falling back to the default "
        "would run the scenario on a shaper it did not ask for")


def test_a_netem_knob_under_the_toxiproxy_driver_is_refused_rather_than_ignored():
    # The failure this harness spends most of its refusals preventing: a knob
    # that is recorded and applied by nothing. toxiproxy terminates TCP, so
    # there is no packet for `shaping_loss_pct` to drop — but the column would
    # carry the number all the way into the matrix.
    with pytest.raises(ValueError, match="only the netem driver reads") as exc:
        shape_from_scenario(Scenario({"shaping_api_url": "http://127.0.0.1:8474",
                                      "shaping_bandwidth_kbps": 1250,
                                      "shaping_loss_pct": 1.0}))
    assert "shaping_loss_pct" in str(exc.value), "the refusal names the key to move or remove"
    with pytest.raises(ValueError, match="shaping_policer_kbps"):
        shape_from_scenario(Scenario({"shaping_api_url": "http://127.0.0.1:8474",
                                      "shaping_bandwidth_kbps": 1250,
                                      "shaping_policer_kbps": 5000}))
    # A netem key left at its DEFAULT is not "set": it says nothing, so it
    # refuses nothing. This is not a nicety — every scenario file the loader
    # produces carries all five keys from lib/scenario.py's DEFAULTS, so a
    # check that read a default as a choice would refuse every toxiproxy run
    # and every unshaped run in the suite before it seeded a byte.
    assert shape_from_scenario(Scenario({"shaping_api_url": "http://127.0.0.1:8474",
                                         "shaping_bandwidth_kbps": 1250,
                                         "shaping_loss_pct": 0}))
    assert shape_from_scenario(Scenario(dict(DEFAULTS))) is None, (
        "a scenario that named no shape at all must load: the netem keys it carries are "
        "the loader's, not the author's")
    assert shape_from_scenario(Scenario({**DEFAULTS,
                                         "shaping_api_url": "http://127.0.0.1:8474",
                                         "shaping_bandwidth_kbps": 1250})) is not None, (
        "and neither may the defaults refuse a toxiproxy shape the harness has been "
        "recording since #403")


def test_shaped_run_drives_the_middlebox_for_the_block_and_unshapes_it_after():
    box = ScriptedMiddlebox()
    sc = netem_scenario(shaping_latency_ms=100, shaping_loss_pct=1.0)
    with shaped_run(sc, runner=box, log=lambda _: None) as active:
        assert isinstance(active, NetemShape) and active.loss_pct == 1.0
        installed = list(box.calls)
    assert box.calls[len(installed):] == active.teardown_program("enp1s0"), (
        "the pipe is unshaped on every exit, or the next scenario in the matrix inherits "
        "a lossy link it never asked for and its numbers are quietly wrong")


def test_a_netem_row_and_a_toxiproxy_row_carry_the_same_columns():
    netem_row = full_shape().flat_columns()
    toxiproxy_row = Shape("http://x", "minio", 1250, 100, 20).flat_columns()
    assert set(netem_row) == set(toxiproxy_row) == set(SHAPING_COLUMNS), (
        "a column that appears only for the runs that set it makes a results directory "
        "two shapes of file, and the matrix reads one")
    assert netem_row["shaping_driver"] == "netem" and netem_row["shaping_proxy"] == "", (
        "the driver column is what tells a reader which emulator produced the row, and a "
        "netem row was never near a proxy")
    assert netem_row["shaping_bandwidth_kbps"] == "", (
        "`shaping_bandwidth_kbps` is toxiproxy's KILOBYTES; leaving a netem KILOBITS rate "
        "in it would be read in the wrong unit by a factor of eight")
    assert full_shape().describe()["rate_unit"] == "kilobits_per_second", (
        "the unit travels with the number, because the two drivers disagree about it")


# --------------------------------------------------------------------------
# The gate: the engine must actually cross the middlebox
# --------------------------------------------------------------------------


def netem_gate_scenario(**overrides):
    """A netem scenario that reaches the store the only way it may: from a
    container, over a backend that dials the endpoint, at the middlebox."""
    values = dict(backend="s3_native", runner_mode="container",
                  endpoint_url=f"http://{netem.NETEM_SERVICE}:{netem.NETEM_PORT}",
                  shaping_latency_ms=100)
    values.update(overrides)
    return netem_scenario(**values)


def test_a_netem_run_that_would_go_around_the_middlebox_is_refused():
    endpoint = f"http://{netem.NETEM_SERVICE}:{netem.NETEM_PORT}"
    require_endpoint_reaches_the_shape(netem_gate_scenario(), endpoint)

    # A host process reaches the store without crossing the lab network's L3
    # path, so every qdisc would shape nothing while the summary said netem.
    with pytest.raises(ShapingError, match="runner_mode: container"):
        require_endpoint_reaches_the_shape(netem_gate_scenario(runner_mode="host"), endpoint)
    with pytest.raises(ShapingError, match="never dials endpoint_url"):
        require_endpoint_reaches_the_shape(netem_gate_scenario(backend="mock"), endpoint)
    # An endpoint naming the store directly crosses no qdisc at all.
    with pytest.raises(ShapingError, match="is not the netem middlebox") as exc:
        require_endpoint_reaches_the_shape(
            netem_gate_scenario(endpoint_url="http://minio:9000"), "http://minio:9000")
    assert f"http://{netem.NETEM_SERVICE}:{netem.NETEM_PORT}" in str(exc.value), (
        "the refusal spells the endpoint a netem scenario needs; guessing it is how a "
        "scenario ends up shaped on paper only")

    # The host and port are PARSED, not matched as a substring of the URL
    # (review): every one of these contains the string "netem" and none of
    # them is the middlebox, so a substring test would hand the run to another
    # machine while the summary recorded a shaped one.
    for impostor in (f"http://{netem.NETEM_SERVICE}-staging:{netem.NETEM_PORT}",
                     f"http://minio:9000/{netem.NETEM_SERVICE}",
                     f"http://minio:9000/?via={netem.NETEM_SERVICE}",
                     f"http://{netem.NETEM_SERVICE}:9000"):
        with pytest.raises(ShapingError, match="is not the netem middlebox"):
            require_endpoint_reaches_the_shape(
                netem_gate_scenario(endpoint_url=impostor), impostor)
    # VTOP_S3_ENDPOINT_URL outranks the scenario, so the EFFECTIVE endpoint is
    # the one that has to cross the middlebox.
    with pytest.raises(ShapingError, match="is not the scenario's"):
        require_endpoint_reaches_the_shape(netem_gate_scenario(), "http://minio:9000")


# --------------------------------------------------------------------------
# The matrix: what may share one table
# --------------------------------------------------------------------------


def test_two_shaping_drivers_cannot_share_one_comparison_table():
    with pytest.raises(IncomparableRuns) as exc:
        refuse_mixed_drivers([{"scenario_name": "13-shaped", "shaping_driver": "toxiproxy"},
                              {"scenario_name": "15-lossy", "shaping_driver": "netem"}])
    assert "toxiproxy" in str(exc.value) and "netem" in str(exc.value), (
        "the refusal must name BOTH drivers: the fix is to run the comparison on one of "
        "them, and the reader cannot choose without knowing which two are in the table")
    assert "different links" in str(exc.value), (
        "a per-connection bandwidth toxic and an L3 token bucket with a real buffer are "
        "not the same pipe, and a reader with two rows side by side will compare them "
        "whatever the driver column says")


def test_a_shaped_row_beside_an_unshaped_one_is_the_comparison_shaping_exists_for():
    refuse_mixed_drivers([{"scenario_name": "01-baseline", "shaping_driver": ""},
                          {"scenario_name": "15-lossy", "shaping_driver": "netem"}])
    # An unshaped row may also carry no shaping column at all.
    refuse_mixed_drivers([{"scenario_name": "01-baseline"},
                          {"scenario_name": "13-shaped", "shaping_driver": "toxiproxy"}])
    refuse_mixed_drivers([])


# ---------------------------------------------------------------------------
# Regressions from the first review round (#477)
# ---------------------------------------------------------------------------


def test_a_pure_loss_shape_tears_down_exactly_what_it_installed():
    # The headline capability of this driver is loss, and a pure-loss shape
    # sets no latency — so it installs no root qdisc on the engine interface.
    # A blanket teardown asked `tc` to delete one anyway, `tc` answered
    # "Cannot delete qdisc with handle of zero", and `clear` raised out of
    # `shaped()`'s own finally at the end of a perfectly good run. The
    # teardown now mirrors the install.
    shape = NetemShape(loss_pct=1.0, run_token="t")
    installed = {" ".join(cmd) for cmd in shape.tc_program("enp1s0")}
    torn_down = [" ".join(cmd) for cmd in shape.teardown_program("enp1s0")]
    assert not any("dev enp1s0 root" in cmd for cmd in installed), (
        "a shape with no latency installs no root qdisc on the engine interface"
    )
    assert not any("dev enp1s0 root" in cmd for cmd in torn_down), (
        "and must not try to delete one: tc refuses, and the refusal was reported as a "
        "failed unshape at the end of a run that was entirely fine")
    # The pieces it DID install are still removed.
    assert any("dev enp1s0 ingress" in cmd for cmd in torn_down)
    assert any("dev ifb0 root" in cmd for cmd in torn_down)
    assert any("link del ifb0" in cmd for cmd in torn_down)


def test_a_policer_only_shape_installs_no_qdisc_it_then_fails_to_remove():
    # A policer is an action on the ingress filter, not a qdisc: with no
    # latency, no loss and no bottleneck there is nothing on the ifb root
    # either. Two blanket deletes, two refusals, one failed run.
    shape = NetemShape(policer_kbps=10_000, run_token="t")
    torn_down = [" ".join(cmd) for cmd in shape.teardown_program("enp1s0")]
    assert not any("dev enp1s0 root" in cmd for cmd in torn_down)
    assert not any("dev ifb0 root" in cmd for cmd in torn_down)
    assert torn_down == ["tc qdisc del dev enp1s0 ingress", "ip link del ifb0"], (
        "exactly the ingress hook and the device it feeds — the only two things a "
        "policer-only shape creates")


def test_absence_is_recognised_in_every_wording_the_tools_use():
    # Three different sentences for three different absences. Only the last
    # was covered when this was written against `ip link del` alone, so a
    # teardown of a qdisc that was never installed read as a failure.
    for message in (
        "Error: Cannot delete qdisc with handle of zero.",
        "Error: Cannot find specified qdisc on specified device.",
        'Cannot find device "ifb0"',
        "RTNETLINK answers: Invalid argument",
    ):
        assert netem._absent(message), (
            f"{message!r} means the thing was already gone, which is success for a "
            "teardown; reporting it as a failure aborts a run that finished cleanly")
    assert not netem._absent("RTNETLINK answers: Operation not permitted"), (
        "a REFUSED removal must still be reported: that one leaves the pipe shaped and "
        "the next run inherits it")


def test_a_rollback_that_could_not_finish_says_so():
    # apply() undoes its own partial install before reporting. A middlebox
    # that just refused a command is exactly the one whose teardown may also
    # be refused, so claiming a clean box that nothing checked is how the
    # next run inherits a shape nobody chose.
    def everything_is_refused(argv):
        # The two reads the driver does before touching anything succeed, and
        # so does the claim — the point is a middlebox that refuses the WORK
        # and then refuses the rollback too.
        if argv[:4] == ["ip", "-o", "-4", "addr"]:
            return 0, TWO_INTERFACES
        if argv[:2] == ["tc", "qdisc"] and "show" in argv:
            return 0, ""
        if argv[:3] == ["ip", "link", "add"]:
            return 0, ""
        return 1, "RTNETLINK answers: Operation not permitted"

    with pytest.raises(ShapingError) as exc:
        netem.apply(NetemShape(latency_ms=100, run_token="t"), everything_is_refused)
    assert "ROLLBACK DID NOT FINISH" in str(exc.value), (
        "the operator has to know the middlebox is still partly shaped, or they will "
        "retry and be refused by the stacking guard with no idea why")


def test_calibrate_can_be_driven_without_a_container():
    # The one function that composes install -> probe -> check -> teardown was
    # unreachable from the suite because it built its own probe runner. It
    # takes one now, so the sequence itself is testable.
    calls = []

    def middlebox(argv):
        calls.append(argv[0])
        if argv[:4] == ["ip", "-o", "-4", "addr"]:
            return 0, TWO_INTERFACES
        if "show" in argv:
            return (0, "") if argv[0] == "tc" else (1, 'Device "ifb0" does not exist.')
        return 0, ""

    def probe(argv):
        assert argv[0] == "iperf3", "the probe measures with iperf3, alone on the link"
        return 0, json.dumps({"end": {"sum_received": {"bits_per_second": 9_600_000.0}}})

    measured = netem.calibrate(NetemShape(latency_ms=100, bottleneck_kbps=10_000,
                                          buffer_bdp=1.0, run_token="t"),
                               run=middlebox, probe=probe, log=lambda _m: None)
    assert measured == 9.6
    assert "ip" in calls, "the shape is installed for the measurement and removed after"


def test_an_unshaped_baseline_can_sit_beside_a_netem_row():
    # The comparison the netem driver exists to make. Every scenario carries
    # the loader's default `shaping_driver: toxiproxy`, and the matrix fills a
    # missing column from the scenario — so an unshaped run used to claim a
    # driver it never ran, and the mixed-driver refusal fired on exactly the
    # pair it is written to allow.
    unshaped = matrix_row({"scenario_name": "01-baseline",
                           **{column: "" for column in SHAPING_COLUMNS},
                           "scenario": {"shaping_driver": "toxiproxy", "name": "base"}})
    shaped_row = matrix_row({"scenario_name": "14-lossy-wan", "shaping_driver": "netem",
                             "scenario": {"shaping_driver": "netem", "name": "lossy"}})
    assert unshaped["shaping_driver"] == "", (
        "what the run DID beats what the scenario asked for: an unshaped run ran on no "
        "driver, whatever default its scenario file carries")
    refuse_mixed_drivers([unshaped, shaped_row])

    # ... and the refusal still fires on two real drivers.
    proxied = matrix_row({"scenario_name": "13-shaped", "shaping_driver": "toxiproxy",
                          "scenario": {"shaping_driver": "toxiproxy"}})
    with pytest.raises(IncomparableRuns):
        refuse_mixed_drivers([proxied, shaped_row])


def test_the_queue_limit_makes_room_for_the_packets_still_in_the_delay_line():
    # netem's `limit` bounds every packet the qdisc HOLDS, and on a shape that
    # also delays, packets waiting out their delay occupy it before a byte of
    # congestion backlog does. The first version of this fix counted only the
    # buffer, so on the bundled 10 Mbit/s / 100 ms link half of a 1-BDP limit
    # went to the delay line and scenarios 14 and 16 ran with about HALF the
    # bottleneck buffer they recorded (review).
    shape = NetemShape(latency_ms=100, bottleneck_kbps=10_000, buffer_bdp=1.0)
    assert shape.buffer_bytes() == 125_000, "1 BDP of a 10 Mbit/s, 100 ms link"
    assert shape.delay_occupancy_bytes() == 62_500, (
        "50 ms of upload delay at 10 Mbit/s is in flight, not queued"
    )
    held = shape.netem_limit_packets() * 1500
    assert held - shape.delay_occupancy_bytes() >= 125_000, (
        "what remains after the delay line must be the CONFIGURED buffer; "
        "shaping_buffer_bdp is read as the congestion queue, and a column that "
        "says 1.0 while the queue is 0.5 makes the deep-buffer and shallow-buffer "
        "scenarios differ by less than they claim"
    )

    # A shape with no delay has no occupancy to make room for, so the limit is
    # the buffer alone — the earlier behaviour, unchanged where it was right.
    flat = NetemShape(latency_ms=0, bottleneck_kbps=10_000, buffer_bdp=0.0)
    assert flat.delay_occupancy_bytes() == 0


def test_the_configured_buffer_survives_the_delay_line_at_every_awkward_rate_and_rtt():
    # The fourth defect in this one number, and the third found by staring at a
    # single example (review): a 1006 kbit/s, 334 ms, 1-BDP link holds 21000.25
    # bytes in flight, the old floor recorded 21000, and 42000 + 21000 is
    # exactly 42 packets — so the sum's ceiling had nothing left to round and
    # the link ran on 41999.75 bytes of buffer beneath a column reading 42000.
    #
    # A test that pinned that example would have been the fifth defect waiting
    # to happen, so this pins the INVARIANT instead: after the delay line, at
    # least the configured buffer must remain, at every rate and every round
    # trip. The occupancy it judges against is computed here — exactly, in
    # rationals, and independently of the module — because a test that asked
    # the module how much its own line holds would agree with whatever rounding
    # the module chose, which is exactly the failure being closed.
    from fractions import Fraction

    from lib.netem import MTU_BYTES, NetemShape

    # Three sigma, spelled out rather than imported from JITTER_HEADROOM_SIGMAS,
    # for the reason the jitter test spells it out too: a reserve read out of
    # the module under test is a reserve nobody checked.
    sigmas = 3

    # Deliberately unround rates and round trips — nothing divisible by 8, odd
    # and even millisecond counts, and buffer depths that scale the fraction
    # differently. The reported case (1006 kbit/s, 334 ms, 1 BDP) is inside it,
    # but so are ~10,000 others — and eight of them, not one, broke the
    # invariant under the old floor, which is the point of sweeping.
    for kbps in range(997, 1061):
        for rtt in range(299, 341):
            for jitter, bdp in ((0, 1.0), (7, 1.0), (20, 0.5), (13, 4.0)):
                shape = NetemShape(latency_ms=rtt, jitter_ms=jitter,
                                   bottleneck_kbps=kbps, buffer_bdp=bdp)
                # Half the round trip with the odd millisecond on the upload —
                # written out here, then checked against the module's own
                # accessor, so a split that MOVED fails loudly instead of
                # quietly changing what this sweep reserves against.
                upload_ms = (rtt + 1) // 2
                assert upload_ms == shape.upload_delay_ms(), (
                    "the direction split changed; this sweep is now reserving against a "
                    "delay line the shape does not install, and would pass while checking "
                    "the wrong link"
                )
                # The kilo and the milli cancel: kbit/s x ms / 8 is bytes, and
                # as a Fraction it is the exact count, quarter-bytes included.
                held_ms = upload_ms + sigmas * ((jitter + 1) // 2)
                in_flight = Fraction(kbps * held_ms, 8)
                limit = shape.netem_limit_packets()
                remaining = limit * MTU_BYTES - in_flight
                assert remaining >= shape.buffer_bytes(), (
                    f"{kbps} kbit/s, {rtt} ms, {jitter} ms jitter, {bdp} BDP: the delay "
                    f"line holds {float(in_flight)} bytes and the limit is {limit} packets, "
                    f"leaving {float(remaining)} behind it for a configured buffer of "
                    f"{shape.buffer_bytes()}. Short of that, netem drops on its own limit "
                    "before tbf's queue is anywhere near full: loss the emulator invented, "
                    "recorded in no column, beside a buffer depth that overstates itself"
                )
                assert shape.delay_occupancy_bytes() >= in_flight, (
                    f"{kbps} kbit/s, {rtt} ms, {jitter} ms jitter: the reported occupancy "
                    f"{shape.delay_occupancy_bytes()} understates the {float(in_flight)} "
                    "bytes actually in flight, and every limit derived from it inherits "
                    "the shortfall"
                )
