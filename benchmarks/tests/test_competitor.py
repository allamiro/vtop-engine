"""Tests for benchmarks/lib/competitor.py (#478): the flow beside the upload,
the three phases that bracket it, and every refusal that stands between a
fairness number and a results file — all against a scripted command runner, so
nothing here starts a container, an iperf3 or a shaped link.

The measurement's whole value is that it cannot be quietly wrong. A competitor
that never started, a bottleneck that drifted between the brackets, a
contended window with no VTOP in its tail, or a rate read off the sender's
side of a full queue would each produce a plausible number and a false claim
about what the engine costs its neighbour — so those are what these tests read.
"""

import csv
import json
import os
import subprocess
import sys
import threading
import time

import pytest

from lib import competitor, netem
from lib.competitor import (
    COMPETITOR_COLUMNS,
    SOLO_AGREEMENT_TOLERANCE,
    CompetitorSpec,
    contended,
    describe_competitor_line,
    jain_index,
    parse_report,
)
from lib.metrics import CSV_HEADERS, ResultsWriter, _summary_md
from lib.netem import NetemShape
from lib.scenario import DEFAULTS, Scenario
from lib.shaping import COMPETITOR_KEY, Shape, ShapingError, shape_from_scenario
from run_matrix import COMPARE_COLS, matrix_row

COMPOSE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "docker-compose.benchmark.yml")


def iperf_intervals(per_second, omit=0, ramp_mbps=None, step=1.0):
    """The report's own interval stream, in the shape a real iperf3 emits one.

    Modelled on a report captured from iperf 3.16 through this module's own argv
    (`-O 3 -t 6`), because every one of its oddities is load-bearing and none of
    them would be guessed:

      * the ramp's seconds ARE in the stream, flagged `omitted`, and there is
        one fewer of them than `-O` asks for;
      * the measured entries restart the report's clock at zero;
      * the first measured entry straddles that restart — its `seconds` reaches
        two seconds back across it while its `bytes` are the post-restart ones
        only, which is why iperf3's own `bits_per_second` for that entry reads
        at half the flow's rate and why nothing may read it.

    A fixture that smoothed any of these over would let a mapping that is off by
    the whole ramp pass, which is the error an earlier round fixed at the
    harness's end and this one must not reintroduce at iperf3's.

    `step` is the reporting interval (iperf3's `-i`). A real run uses one
    second; the scripted contended flow uses a fraction of one, because it now
    has to spend the wall clock its stream claims (see ScriptedCompetitor), and
    a suite of thirty-second windows is not a suite anyone runs. Bytes scale
    with it, so `per_second` stays a rate.
    """
    stream = []
    ramp = per_second[0] if ramp_mbps is None else ramp_mbps
    for i in range(max(0, omit - 1)):
        stream.append({"sum": {"start": i * step, "end": (i + 1) * step, "seconds": step,
                               "bytes": int(ramp * step * 1e6 / 8), "omitted": True}})
    at = 0.0
    for i, rate in enumerate(per_second):
        straddles = i == 0 and omit
        end = at + step
        stream.append({"sum": {
            # `start` is what a real report carries and what the module refuses
            # to trust: on the straddling entry iperf3 fills it from a clock
            # frame it has already thrown away.
            "start": step if straddles else at,
            "end": end,
            "seconds": 2.0 * step if straddles else step,
            "bytes": int(rate * step * 1e6 / 8),
            "bits_per_second": rate * 1e6 / (2.0 if straddles else 1.0),
            "omitted": False}})
        at = end
    return stream


def iperf_stream(intervals=(), end=None, error=None):
    """An `iperf3 --json-stream` output: one event per line, in iperf3's order.

    `error` is followed by an empty `end`, as the real one is (captured live
    against a closed port on netshoot:v0.13).
    """
    events = [{"event": "start", "data": {"version": "iperf 3.16+"}}]
    events += [{"event": "interval", "data": entry} for entry in intervals]
    if error is not None:
        events.append({"event": "error", "data": error})
        end = {} if end is None else end
    if end is not None:
        events.append({"event": "end", "data": end})
    return "\n".join(json.dumps(event) for event in events)


def iperf_report(mbps, seconds=30.0, sent_mbps=None, per_second=None, omit=0,
                 ramp_mbps=None, intervals=None, step=1.0):
    """An iperf3 --json-stream report, in the shape the real one has.

    `sent_mbps` defaults ABOVE the received rate on purpose: that is what a
    real report through a token bucket looks like, because the sender counts
    bytes still sitting in the bottleneck's queue as sent. The interval stream
    is built at the SENT rate for the same reason: iperf3's intervals are the
    client's own sending side and add up to `sum_sent`, never `sum_received`.

    `per_second` hands a test an UNEVEN flow — one rate per interval of the
    window — which is what makes "the rate over the covered span" and "the
    window's average" two different numbers rather than the same one twice.
    """
    sent = mbps * 1.2 if sent_mbps is None else sent_mbps
    if intervals is None:
        intervals = iperf_intervals(
            per_second or [sent] * max(1, int(round(seconds / step))),
            omit=omit, ramp_mbps=ramp_mbps, step=step)
    return iperf_stream(intervals, end={
        "sum_sent": {"bits_per_second": sent * 1e6, "seconds": seconds,
                     "bytes": int(sent * 1e6 * seconds / 8), "retransmits": 42},
        "sum_received": {"bits_per_second": mbps * 1e6, "seconds": seconds,
                         "bytes": int(mbps * 1e6 * seconds / 8)},
        "sender_tcp_congestion": "cubic",
    })


# The scripted contended window: how long it measures, and the interval it is
# cut into. Short because the scripted flow now spends the wall clock its own
# stream claims; cut finely enough that a 20 ms engine cycle sits inside one
# interval rather than across several.
SCRIPTED_WINDOW_SECONDS = 0.3
SCRIPTED_STEP_SECONDS = 0.05


def stream_arrivals(text, opened_at, omit):
    """Where each line of a scripted stream would ARRIVE, for a flow whose
    measured window opened at `opened_at`.

    What a real iperf3 does: the start event as it connects, each interval's
    line as that interval ends, the end event as the window closes. The window
    clock is the one the module derives — the last measured interval ends where
    the receiver's summary says the window did — so a fixture with a hole or an
    uneven stream is dated exactly as a live one would be.
    """
    events = [json.loads(line) for line in text.splitlines() if line.strip()]
    measured = [event["data"]["sum"] for event in events
                if event["event"] == "interval" and not event["data"]["sum"].get("omitted")]
    end = next((event["data"] for event in events if event["event"] == "end"), {}) or {}
    seconds = float((end.get("sum_received") or {}).get("seconds", 0.0))
    origin = (max(float(entry["end"]) for entry in measured) - seconds) if measured else 0.0
    stamps = []
    for event in events:
        if event["event"] == "interval":
            entry = event["data"]["sum"]
            if entry.get("omitted"):
                stamps.append(opened_at - omit + float(entry["end"]))
            else:
                stamps.append(opened_at + float(entry["end"]) - origin)
        elif event["event"] == "start":
            stamps.append(opened_at - omit)
        else:
            stamps.append(opened_at + seconds)
    return stamps


class ScriptedCompetitor:
    """Answers each iperf3 invocation from a list of rates and records the
    argv it was given.

    Stands in for `competitor.docker_exec`, so a test can hand the three phases
    three different links and read what the module concluded.

    THE CONTENDED PHASE IS PHYSICAL (review). The module places that window from
    the arrival stamps of its stream's lines, and a stamp cannot lie in the
    future of the instant the runner returned — so this flow opens its window at
    a real instant (`opened_at`: the call, plus the ramp `-O` asks for, plus
    `setup`), stamps each line where a live iperf3 would have written it, and
    does not return until the window has closed and `teardown` has passed. The
    solo phases return at once and unstamped: nothing places them.
    """

    def __init__(self, rates, seconds=30.0, refuse=None, delay=0.0, output=None, during=None,
                 contended_report=None, setup=0.0, teardown=0.0, stamps=None):
        self.rates = list(rates)
        self.seconds = seconds
        self.refuse = refuse
        self.delay = delay
        self.output = output
        self.during = during
        # The middle phase's report, when a test needs it to be something other
        # than a flat one — an uneven flow, an omitted ramp, a stream with a
        # hole. Only the contended window's shape decides the fairness index, so
        # only it is worth overriding, and the two solo windows stay flat and
        # agreeing so a drift refusal never fires in a test about something else.
        self.contended_report = contended_report
        # THE OVERHEAD A REAL `docker exec` SPENDS AROUND THE WINDOW, split the
        # way the run could not see when it had only the command's two ends:
        # `setup` before measuring began (startup, connect), `teardown` after it
        # ended (the closing exchange, exit, the trip back). Held to deadlines
        # rather than slept, so the cycles a test runs come out of this budget
        # instead of extending it. The middle phase only, like
        # `contended_report` and for the same reason.
        self.setup = setup
        self.teardown = teardown
        # A test about a runner whose stamps are WRONG supplies a function from
        # the honest stamps to the ones it returns.
        self.stamps = stamps
        self.entered_at = None
        self.opened_at = None
        self.calls = []

    def __call__(self, argv):
        entered = time.monotonic()
        self.calls.append(list(argv))
        phase = len(self.calls)
        if self.delay:
            time.sleep(self.delay)
        omit = int(argv[argv.index("-O") + 1]) if "-O" in argv else 0
        self.entered_at = entered
        self.opened_at = entered + self.delay + omit + (self.setup if phase == 2 else 0.0)
        # WHAT HAPPENED WHILE THIS WINDOW WAS OPEN, run on the flow's own
        # thread and given the 1-based phase number and this flow, whose
        # `opened_at` is the instant a test places its cycles against. Placed
        # here, "inside the window" is a sequencing fact: this runs before the
        # flow can take its closing sample.
        if self.during is not None:
            self.during(phase, self)
        if self.refuse is not None:
            code, out = self.refuse
            return code, out, None
        if self.output is not None:
            return 0, self.output, None
        if phase != 2:
            return 0, iperf_report(self.rates.pop(0), self.seconds), None
        if self.contended_report is not None:
            self.rates.pop(0)
            text = self.contended_report
        else:
            text = iperf_report(self.rates.pop(0), SCRIPTED_WINDOW_SECONDS,
                                step=SCRIPTED_STEP_SECONDS)
        honest = stream_arrivals(text, self.opened_at, omit)
        returns_at = max(honest) + self.teardown
        time.sleep(max(0.0, returns_at - time.monotonic()))
        stamps = honest if self.stamps is None else self.stamps(honest)
        return 0, text, tuple(stamps)


def netem_shape(**overrides):
    values = dict(latency_ms=100, bottleneck_kbps=10_000, buffer_bdp=1.0, run_token="t")
    values.update(overrides)
    return NetemShape(**values)


def contended_scenario(**overrides):
    """A scenario as the loader hands one over: every default stamped on, plus
    the netem link and the competitor key an author actually wrote."""
    values = {**DEFAULTS, "shaping_driver": "netem", "backend": "s3_native",
              "runner_mode": "container", "endpoint_url": "http://netem:9200",
              "shaping_latency_ms": 100, "shaping_bottleneck_kbps": 10000,
              "shaping_buffer_bdp": 1.0, "duration_seconds": 300,
              COMPETITOR_KEY: "bulk:60"}
    values.update(overrides)
    return Scenario(values)


def spec(seconds=30, omit=0):
    # omit=0 by default so a test's scripted competitor is not waiting out a
    # real ramp. The ramp's own effect on the window boundary has its own test,
    # which sets it.
    return CompetitorSpec(mode="bulk", seconds=seconds, omit=omit)


def scripted_spec(omit=0):
    """The spec a three-phase test runs under.

    A zero-second window, because the scripted contended flow measures a
    fraction of a second of real time (SCRIPTED_WINDOW_SECONDS) and a spec that
    asked for thirty would refuse it as a flow cut short. That refusal has its
    own test; these are about everything after it.
    """
    return CompetitorSpec(mode="bulk", seconds=0, omit=omit)


# --------------------------------------------------------------------------
# The key: what a scenario may ask for
# --------------------------------------------------------------------------


def test_the_competitor_key_names_a_rate_mode_and_a_window():
    got = CompetitorSpec.from_scenario(contended_scenario(), netem_shape())
    assert got is not None and got.mode == "bulk" and got.seconds == 60, (
        "the key carries both halves: a duration with no mode would leave the traffic "
        "model unrecorded, and a mode with no duration would leave the harness to pick "
        "how long a scenario runs")
    assert got.wall_seconds() == 60 + got.omit, (
        "iperf3 runs the omitted ramp BEFORE it starts the clock on -t, so a window costs "
        "the sum of both — the number every duration check has to be made against")


def test_a_scenario_that_named_no_competitor_gets_none_and_no_columns():
    # The loader stamps `shaping_competitor` onto every scenario in the tree.
    # Reading its own default as an author's choice would start an iperf3
    # beside every unshaped run in the suite.
    assert CompetitorSpec.from_scenario(Scenario(dict(DEFAULTS)), None) is None
    assert CompetitorSpec.from_scenario(
        contended_scenario(**{COMPETITOR_KEY: ""}), netem_shape()) is None
    assert set(competitor.blank_columns()) == set(COMPETITOR_COLUMNS), (
        "a run with no competitor states the columns blank rather than omitting them: a "
        "column that appears only for the runs that set it makes a results directory two "
        "shapes of file")
    assert set(competitor.blank_columns().values()) == {""}


@pytest.mark.parametrize("bad,expected", [
    ("60", "<mode>:<seconds>"),
    ("bulk", "<mode>:<seconds>"),
    ("bulk:", "<mode>:<seconds>"),
    (":60", "<mode>:<seconds>"),
    ("burst:60", "not one of"),
    ("bulk:sixty", "not a whole number"),
    ("bulk:3", "outside"),
    ("bulk:6000", "outside"),
])
def test_a_competitor_key_that_cannot_be_read_is_refused_by_name(bad, expected):
    # Each of these would otherwise be guessed at, and a guessed traffic model
    # is recorded as if it were the one the scenario asked for.
    with pytest.raises(ValueError, match=expected):
        CompetitorSpec.from_scenario(contended_scenario(**{COMPETITOR_KEY: bad}),
                                     netem_shape())


def test_a_competitor_under_the_toxiproxy_driver_is_refused_by_name():
    # The acceptance criterion. toxiproxy terminates TCP and meters each
    # connection separately, so the two flows never queue behind one another:
    # the index would come out near 1.0 on a link where VTOP could be taking
    # everything, which is worse than no number at all.
    with pytest.raises(ShapingError, match=COMPETITOR_KEY) as exc:
        shape_from_scenario(Scenario({**DEFAULTS,
                                      "shaping_api_url": "http://127.0.0.1:8474",
                                      "shaping_bandwidth_kbps": 1250,
                                      COMPETITOR_KEY: "bulk:60"}))
    assert "no shared queue" in str(exc.value), (
        "the refusal has to say WHY a per-connection toxic cannot answer this question, "
        "or the next author raises the toxic's rate and tries again")
    assert "netem" in str(exc.value), "and name the driver that can"
    # An unshaped scenario resolves to the same default driver and is refused
    # by the same sentence: there is no bottleneck to share at all.
    with pytest.raises(ShapingError, match="no bottleneck to share"):
        shape_from_scenario(Scenario({**DEFAULTS, COMPETITOR_KEY: "bulk:60"}))
    # ... and the netem scenario the key exists for still loads.
    assert isinstance(shape_from_scenario(contended_scenario()), NetemShape)


def test_a_competitor_needs_a_queue_the_two_flows_actually_share():
    # A netem shape with no tbf installs no queue, so the two flows meet on
    # whatever the host's veth pair does that afternoon and the index measures
    # the machine. A policer is not a substitute: it drops above its rate and
    # holds nothing.
    with pytest.raises(ShapingError, match="shaping_bottleneck_kbps"):
        CompetitorSpec.from_scenario(
            contended_scenario(shaping_bottleneck_kbps=0, shaping_buffer_bdp=0),
            NetemShape(latency_ms=100, policer_kbps=10_000, run_token="t"))
    with pytest.raises(ShapingError, match="unshaped"):
        CompetitorSpec.from_scenario(contended_scenario(), None)


def test_a_competitor_window_the_engine_would_not_outlast_is_refused_before_the_run():
    # A window that outlives the engine's loop is a solo measurement wearing
    # the contended label. Refused from the arithmetic, before a seed byte
    # exists, as well as from what actually happened (below).
    with pytest.raises(ShapingError, match="duration_seconds") as exc:
        CompetitorSpec.from_scenario(
            contended_scenario(duration_seconds=30, **{COMPETITOR_KEY: "bulk:60"}),
            netem_shape())
    assert "60s window" in str(exc.value) and "30" in str(exc.value), (
        "the refusal names both numbers, because the fix is to change one of them")
    # A drain-once scenario has no duration at all, and is the same refusal.
    with pytest.raises(ShapingError, match="duration_seconds"):
        CompetitorSpec.from_scenario(contended_scenario(duration_seconds=0), netem_shape())


def test_the_competitor_dials_the_middlebox_from_the_probe_container():
    # The claim the whole measurement rests on: the competitor's packets cross
    # the SAME ingress hook, the same policer and the same tbf as the upload's.
    # The probe container sits on the engine-facing network, and the hook
    # matches every packet arriving there — narrow that filter to the store's
    # port and the competitor silently stops being shaped while still
    # recording a fairness number.
    got = spec()
    assert got.server == netem.NETEM_SERVICE, (
        "the competitor dials the middlebox itself, which is where the iperf3 server the "
        "calibration probe already uses is running")
    assert got.container == netem.NETEM_PROBE_CONTAINER, (
        "and runs in the probe's container, which is on the engine-facing network — the "
        "side the upload arrives from")
    ingress = [cmd for cmd in netem_shape().tc_program("enp1s0")
               if cmd[:3] == ["tc", "filter", "add"]]
    assert len(ingress) == 1, "the shape installs exactly one ingress filter"
    assert "protocol" in ingress[0] and ingress[0][ingress[0].index("protocol") + 1] == "all", (
        "the hook must match every protocol; a filter narrowed to the store's traffic "
        "would leave the competing flow unshaped and the fairness number meaningless")
    assert ["u32", "match", "u32", "0", "0"] == ingress[0][
        ingress[0].index("u32"):ingress[0].index("u32") + 5], (
        "and every packet — the competitor shares the queue only because nothing in the "
        "filter distinguishes its packets from the upload's")


def test_the_middlebox_still_runs_the_iperf3_server_the_competitor_dials():
    # The compose file and lib/competitor.py have to agree in writing, or a
    # contended run fails at its first phase with a connection refused.
    with open(COMPOSE, encoding="utf-8") as fh:
        text = fh.read()
    assert f"iperf3 -s -p {competitor.IPERF_SERVER_PORT} -D" in text, (
        f"the netem middlebox must keep serving iperf3 on port "
        f"{competitor.IPERF_SERVER_PORT}: it is the far end of both the calibration probe "
        "and the competing flow, and no other service in the lab offers one")


def test_the_command_carries_the_window_the_ramp_and_the_json_report():
    # The real default ramp, not the tests' zero: this is the one test about
    # what iperf3 is actually asked to do.
    argv = CompetitorSpec(mode="bulk", seconds=45).argv()
    assert argv[:2] == ["iperf3", "-c"], "a plain TCP bulk flow, not a second VTOP upload"
    assert "-t" in argv and argv[argv.index("-t") + 1] == "45"
    assert "-O" in argv and argv[argv.index("-O") + 1] == str(competitor.OMIT_SECONDS), (
        "the slow-start ramp is omitted from iperf3's own summary, or a share measured "
        "across it understates whichever flow started later — always the competitor")
    assert competitor.JSON_STREAM_FLAG in argv and "-J" not in argv, (
        "the report is parsed, not scraped from the human-facing output — and STREAMED, one "
        "event per line as each interval ends, because the arrival of those lines is what "
        "places the contended window on the run's clock (review)")
    assert "-b" not in argv, (
        "a bulk competitor is congestion-controlled: a paced flow cannot lose a share it "
        "never asked for, so its goodput would say nothing about fairness")
    assert spec(45).command_timeout() > spec(45).wall_seconds(), (
        "the command's own timeout must outlast the window it is running, or a 60-second "
        "competitor is killed mid-flow and reported as a container that stopped answering")


# --------------------------------------------------------------------------
# Reading the report
# --------------------------------------------------------------------------


def test_the_received_rate_is_recorded_and_never_the_sent_one():
    # At the far end of a token bucket the sender counts bytes still sitting in
    # the bottleneck's queue. Reading them would report the competing flow as
    # less harmed than it was, which is the one direction this measurement must
    # not fail in.
    sample = parse_report(iperf_report(4.0, seconds=30.0, sent_mbps=4.8), spec(30), "with VTOP")
    assert sample.mbps == 4.0, (
        "4.8 Mbit/s is what the sender handed to its socket; 4.0 is what crossed the link, "
        "and only the second is the neighbour's experience")
    assert sample.congestion == "cubic", (
        "the competitor's share depends on its control law, so a recorded number that did "
        "not name it could not be reproduced")
    assert sample.retransmits == 42, "and what the flow paid to get it, where iperf3 says"


def test_a_report_with_no_receiver_summary_is_refused_rather_than_falling_back():
    # The calibration probe falls back to the sender's view; this must not.
    # That fallback's error points in the direction that exonerates VTOP, and
    # a fairness number that fails safe has to fail loudly instead.
    only_sent = iperf_stream(iperf_intervals([9.0] * 3),
                             end={"sum_sent": {"bits_per_second": 9e6, "seconds": 30.0,
                                               "bytes": 1}})
    with pytest.raises(ShapingError, match="no receiver summary") as exc:
        parse_report(only_sent, spec(30), "alone before")
    assert "less harmed than it was" in str(exc.value), (
        "the refusal explains why the obvious substitute is not taken, or the next reader "
        "adds it back as an obvious improvement")
    with pytest.raises(ShapingError, match="not an iperf3 --json-stream report"):
        parse_report("iperf3: error - unable to connect", spec(30), "alone before")
    with pytest.raises(ShapingError, match="unable to connect to server"):
        parse_report(iperf_stream(error="unable to connect to server"), spec(30), "alone after")


def test_a_flow_cut_short_is_refused_rather_than_recorded_over_a_window_nobody_chose():
    # A killed server still produces a JSON report, and its rate is a real
    # number over four seconds of a thirty-second window.
    with pytest.raises(ShapingError, match="cut short") as exc:
        parse_report(iperf_report(4.0, seconds=4.0), spec(30), "with VTOP")
    assert "4.0s of the 30s window" in str(exc.value), (
        "the refusal says what it saw: the operator needs to know the flow ended early, "
        "not merely that something was wrong")


def test_a_competitor_that_cannot_be_started_fails_the_run():
    # The acceptance criterion. A contended scenario whose competitor never
    # started is an uncontended scenario, and recording it under the contended
    # name is the failure this harness spends most of its refusals preventing.
    refused = ScriptedCompetitor([], refuse=(1, "iperf3: error - unable to connect to server: "
                                                "Connection refused"))
    with pytest.raises(ShapingError, match="could not be started") as exc:
        competitor.measure(spec(30), refused, "alone before")
    assert "Connection refused" in str(exc.value), (
        "iperf3's own words reach the operator: an exit code alone sends them looking at "
        "the wrong container")
    assert "--profile netem" in str(exc.value), "and the refusal says how to fix it"


# --------------------------------------------------------------------------
# The three phases
# --------------------------------------------------------------------------


# How long a scripted engine cycle is made to last. A cycle that begins and
# returns at the same clock reading carries bytes that crossed the link in no
# time at all, which is not a rate and which the module refuses; a real cycle
# takes seconds. Twenty milliseconds is enough to make the attributed span a
# measurement rather than a rounding artifact, and small enough that the suite
# does not notice.
SCRIPTED_CYCLE_SECONDS = 0.02


def wait_for(predicate, what):
    """Deadline-poll one of the boundaries the competitor's own threads take.

    Bounded AND asserted: a wait that gave up quietly would let the test go on
    to place a cycle against a boundary that does not exist yet, and then
    assert confidently about the wrong window.
    """
    deadline = time.monotonic() + 5
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.005)
    assert predicate(), (
        f"the contended window never {what} within 5s, so this test would be reasoning "
        "about a boundary that was never taken")


def wait_until(instant, what):
    """Deadline-poll the monotonic clock to an ABSOLUTE instant.

    Its own function rather than a lambda written inside a loop: a lambda closed
    over a loop variable binds late, so every placement would wait for the last
    one's offset and every cycle would land in the same place.
    """
    wait_for(lambda: time.monotonic() >= instant, what)


class ScriptedEngine:
    """The engine's committed-byte total, moved one whole CYCLE at a time.

    The real total only moves when a `process_once` returns, so a test that
    wants bytes on one side of a boundary has to place a CYCLE there rather
    than a sample. `cycle()` does what the runner's loop does — take the bytes,
    then note the interval the call occupied — with the start SUPPLIED instead
    of measured, so a cycle that straddles an edge of the window is constructed
    rather than raced for.
    """

    def __init__(self):
        self.total = 0

    def __call__(self) -> int:
        return self.total

    def cycle(self, handle, started_at, committed, uncounted=0):
        time.sleep(SCRIPTED_CYCLE_SECONDS)
        # The return is stamped BEFORE the total moves, as the runner stamps it
        # before parsing the outcomes that move it.
        returned_at = time.monotonic()
        self.total += committed
        handle.note_progress(started_at, returned_at, uncounted)


# How far past the opening a cycle "at" the opening begins. The module places
# the window at an arrival stamp minus an interval's end, and the scripted flow
# built that stamp as the opening plus the same end — a float round trip that
# can land one ulp late and turn a cycle meant to begin exactly at the edge
# into one that crossed it. A microsecond is far below anything a test here
# places deliberately and far above that rounding.
ON_THE_EDGE = 1e-6


def cycles_inside_the_window(engine, placements):
    """A `during` hook that runs engine cycles while the contended flow is open.

    `placements` are (seconds before the opening boundary, bytes) pairs: 0 is a
    cycle that began exactly as the window opened, and 1.0 is one that began a
    second before it — the straddle whose bytes cannot be split. A third
    element, when present, is the cycle's count of uncommitted batches. Every cycle
    placed here returns before the flow does, so the only edge a test still has
    to construct by hand is a cycle that returns AFTER the window closed.

    The boundary is the scripted flow's own `opened_at` — the instant its stream
    will date the window to — and the hook WAITS for it before running anything:
    a cycle handed a start in the future would be recorded as having returned
    before it began.
    """

    def during(phase, box):
        if phase != 2:  # the contended window is the second of the three
            return
        handle = box.handle
        wait_until(box.opened_at, "reached the instant its window opened")
        for before, committed, *uncounted in placements:
            engine.cycle(handle, box.opened_at - before + ON_THE_EDGE, committed,
                         *uncounted)

    return during


def run_three_phases(rates, vtop_bytes_after=1_000_000, **kwargs):
    """Drive one contended run end to end against a scripted competitor.

    The engine's bytes are placed as a CYCLE inside the contended window rather
    than as a counter that happens to have moved (review). The window is only
    ever charged with cycles it wholly contains, so a helper that moved a total
    without saying which call moved it would model a runner whose share cannot
    be attributed at all, and every test built on it would be refused rather
    than measured. The cycle runs on the flow's own thread, after the sample
    taken when iperf's omitted ramp ends and before the one taken when the flow
    returns, which makes "inside the window" a sequencing fact and not a sleep.
    """
    engine = ScriptedEngine()
    box = ScriptedCompetitor(
        rates, during=cycles_inside_the_window(engine, [(0, vtop_bytes_after)]), **kwargs)

    with contended(scripted_spec(), vtop_bytes=engine, run=box, log=lambda _m: None) as handle:
        box.handle = handle
        handle.start_with_vtop()
        # A real run spends its engine loop here — minutes of it — so the
        # competing flow has long finished by the time close() is reached.
        # Waiting for the flow's own boundary reproduces that ordering instead
        # of racing the scheduler, and keeps the overran refusal meaning what
        # it says: the ENGINE stopped first.
        wait_for(lambda: handle._collected is not None, "collected its report")
        handle.close()
    return handle, box


def test_the_three_phases_run_alone_then_beside_vtop_then_alone_again():
    handle, box = run_three_phases([9.0, 4.0, 9.2])
    assert len(box.calls) == 3, (
        "three windows, in one installation of one shape: without the second solo one a "
        "bottleneck that drifted mid-run is indistinguishable from an engine that took "
        "bandwidth")
    assert handle.before.mbps == 9.0 and handle.contended.mbps == 4.0
    assert handle.after.mbps == 9.2
    columns = handle.flat_columns()
    assert columns["competitor_goodput_mbps_with"] == 4.0
    assert columns["competitor_goodput_mbps_without"] == 9.1, (
        "the solo column is the MEAN of the two brackets, which are already proven to "
        "agree within 10%, so neither end is privileged and the mean can hide no spread")
    assert 0.5 <= columns["competitor_jain_index"] <= 1.0


def test_vtops_own_share_of_the_contended_window_is_recorded_beside_the_index():
    # An index alone is symmetric: 0.61 is equally true of an engine that
    # starved its neighbour and a neighbour that starved the engine. The two
    # flows' own numbers are what say which one won.
    handle, _ = run_three_phases([9.0, 4.0, 9.0], vtop_bytes_after=5_000_000)
    assert handle.vtop_mbps > 0, (
        "VTOP's committed bytes over the same window are the second flow in the index; "
        "without them the fairness number cannot be attributed to anyone")
    assert handle.flat_columns()["vtop_goodput_mbps_with"] == handle.vtop_mbps
    record = handle.describe()
    assert record["jain_over"] == ["competitor", "vtop"], (
        "the index names the flows it is over, because a fairness index over the same "
        "flow measured twice would be a different quantity wearing the same name")
    assert "excludes framing" in record["vtop_goodput_is"], (
        "and states what it excludes: committed object bytes understate the engine, "
        "which can only make the index look fairer than the link was")


def test_two_solo_windows_that_disagree_by_more_than_ten_percent_fail_the_run():
    # The acceptance criterion, with a stubbed competitor returning divergent
    # samples: an unstable bottleneck cannot support a fairness claim, because
    # the drop measured with VTOP running cannot be told apart from the drift.
    with pytest.raises(ShapingError, match="disagree by") as exc:
        run_three_phases([9.0, 4.0, 7.0])
    assert "9.0" in str(exc.value) and "7.0" in str(exc.value), (
        "the refusal names BOTH numbers: the reader has to see which way the link moved "
        "and by how much before deciding whether to rerun or to fix the lab")
    assert "cannot support a fairness claim" in str(exc.value)
    # Just inside the band still runs: the tolerance is a band, not a wish.
    edge = 9.0 * (1 - SOLO_AGREEMENT_TOLERANCE / 2)
    handle, _ = run_three_phases([9.0, 4.0, round(edge, 3)])
    assert handle.jain is not None


def test_a_contended_window_that_outlived_the_engines_loop_is_refused():
    # The live half of the duration check. If the engine's block ends while the
    # flow is still running, the tail of the contended window had no VTOP in
    # it, so its goodput is partly a solo measurement under a contended name.
    box = ScriptedCompetitor([9.0, 4.0, 9.0], delay=0.5)
    with pytest.raises(ShapingError, match="engine's window closed") as exc:
        with contended(scripted_spec(), vtop_bytes=lambda: 0, run=box, log=lambda _m: None) as handle:
            handle.start_with_vtop()
            # The block closes while the flow is still going — exactly what a
            # soak whose duration ran out early looks like.
            handle.close()
    assert "solo measurement under a contended name" in str(exc.value)
    assert len(box.calls) == 2, (
        "and no second solo window is measured: the run is already refused, and a fourth "
        "number would only make the refusal harder to read")


def test_a_block_that_ends_in_the_flows_closing_tail_is_not_an_overrun():
    # The flow's THREAD outlives its measured window by iperf3's closing
    # exchange, the report's last lines and docker exec's teardown. A block that
    # ends in that tail had VTOP in every measured second, and used to be refused
    # because the thread was still alive (review). Overrun is judged against the
    # measured window as placed, not against the thread.
    engine = ScriptedEngine()
    box = ScriptedCompetitor([9.0, 4.0, 9.0], teardown=1.0,
                             during=cycles_inside_the_window(engine, [(0, 1_000)]))
    with contended(scripted_spec(), vtop_bytes=engine, run=box, log=lambda _m: None) as handle:
        box.handle = handle
        handle.start_with_vtop()
        window_end = box.opened_at + SCRIPTED_WINDOW_SECONDS
        wait_for(lambda: time.monotonic() > window_end + 0.2, "passed the measured window's end")
        assert handle._thread.is_alive(), (
            "the scenario under test needs the flow still in its closing tail when the block ends")
        handle.close()
    assert handle.contended is not None and handle.jain is not None, (
        "a block that outlasted every measured second must be measured, not refused as an overrun")


def test_a_flow_that_failed_reports_its_own_failure_and_not_the_scheduling_one():
    # Found live, with the middlebox's link deliberately broken mid-flow. A
    # competitor that cannot run also finishes late, and both refusals were
    # true — but "lengthen duration_seconds" sends the operator to change a
    # scenario knob when the middlebox is what is broken.
    def refused_after_a_moment(argv):
        if not hasattr(refused_after_a_moment, "seen"):
            refused_after_a_moment.seen = True
            return 0, iperf_report(9.0), None
        time.sleep(0.5)
        return 1, "iperf3: error - the server is busy running a test", None

    with pytest.raises(ShapingError, match="the server is busy") as exc:
        with contended(scripted_spec(), vtop_bytes=lambda: 0, run=refused_after_a_moment,
                       log=lambda _m: None) as handle:
            handle.start_with_vtop()
            handle.close()
    assert "engine's window closed" not in str(exc.value), (
        "the flow's own diagnosis outranks the scheduling complaint, or the operator "
        "spends the next run changing a knob that was never the problem")


def test_the_collecting_side_waits_longer_than_the_command_it_is_collecting():
    # Also found live: the two timeouts raced and the join won by a hair, so
    # the runner's precise message ("the flow was cut off") was replaced by
    # this side's generic one. The thread must always reach its own conclusion
    # first.
    assert spec(60).collection_timeout() > spec(60).command_timeout(), (
        "a collecting side that gives up first throws away the only diagnosis anyone "
        "has of why the flow did not finish")


def test_a_contended_window_that_was_never_opened_is_refused():
    # A runner that forgot to start the middle phase would otherwise record a
    # 'with VTOP' column that no flow ever measured.
    box = ScriptedCompetitor([9.0, 9.0])
    with pytest.raises(ShapingError, match="never opened"):
        with contended(scripted_spec(), vtop_bytes=lambda: 0, run=box,
                       log=lambda _m: None) as handle:
            handle.close()
    assert len(box.calls) == 1, "only the first solo window ran"


def test_a_block_that_stops_early_gets_no_second_solo_window():
    # The runner refuses a bad seed interval, or a dead seeder, with a plain
    # `return` rather than an exception. Measuring the second window on the way
    # out of one would spend a minute on it and could then raise a drift
    # complaint of its own, burying the reason the run actually stopped.
    box = ScriptedCompetitor([9.0, 4.0, 2.0])

    def a_run_that_refuses_midway():
        with contended(scripted_spec(), vtop_bytes=lambda: 0, run=box, log=lambda _m: None) as handle:
            handle.start_with_vtop()
            return 2  # e.g. seed_interval_seconds <= 0

    assert a_run_that_refuses_midway() == 2, "the block's own refusal is what the caller sees"
    assert len(box.calls) == 2, (
        "the contended flow is collected and the second solo window is not measured: a run "
        "that stopped for its own reason must not be re-explained as a fairness failure")


def test_a_failing_run_keeps_its_own_exception_and_stops_the_flow():
    # The second solo window would raise refusals of its own from inside a
    # failure whose first exception is the one that explains the run.
    box = ScriptedCompetitor([9.0, 4.0, 2.0])
    with pytest.raises(RuntimeError, match="the engine died"):
        with contended(scripted_spec(), vtop_bytes=lambda: 0, run=box, log=lambda _m: None) as handle:
            handle.start_with_vtop()
            raise RuntimeError("the engine died")
            # unreachable: the block never reaches its close()
    assert len(box.calls) == 2, (
        "the contended flow is still collected — a docker exec outliving the run would put "
        "a competing flow across the NEXT one — but nothing further is measured or judged")
    assert not any(t.name == "competitor" and t.is_alive() for t in threading.enumerate()), (
        "and no competitor thread survives the run")


def test_a_competitor_that_fails_in_the_contended_window_fails_the_run():
    # The failure arrives on a thread; it must reach the runner rather than be
    # swallowed into a missing column.
    box = ScriptedCompetitor([9.0], refuse=None)

    calls = []

    def flaky(argv):
        calls.append(argv)
        if len(calls) == 1:
            return 0, iperf_report(9.0), None
        return 1, "iperf3: error - control socket has closed unexpectedly", None

    with pytest.raises(ShapingError, match="control socket has closed"):
        with contended(scripted_spec(), vtop_bytes=lambda: 0, run=flaky, log=lambda _m: None) as handle:
            handle.start_with_vtop()
            handle.close()
    assert box.calls == [], "the scripted box is unused here; the flaky runner is the subject"


# --------------------------------------------------------------------------
# Jain's index
# --------------------------------------------------------------------------


def test_jains_index_is_one_for_an_even_split_and_a_half_when_one_flow_takes_everything():
    assert jain_index([5.0, 5.0]) == 1.0
    assert jain_index([10.0, 0.0]) == 0.5, (
        "two flows, one starved: the floor of the index is 1/n, which is what makes 0.5 "
        "the worst possible two-flow outcome rather than an arbitrary low number")
    assert 0.5 < jain_index([8.0, 2.0]) < 1.0
    assert jain_index([8.0, 2.0]) == jain_index([2.0, 8.0]), (
        "the index is symmetric — which is exactly why it is never reported without the "
        "two per-flow numbers that say which of them won")


def test_a_fairness_index_over_two_idle_flows_is_refused_rather_than_returned():
    with pytest.raises(ShapingError, match="undefined"):
        jain_index([0.0, 0.0])


# --------------------------------------------------------------------------
# What reaches the results files
# --------------------------------------------------------------------------


def summary_with_competitor(handle):
    return {"run_id": "r1", "scenario_name": "18-competing-flow", "start_time": "t",
            "end_time": "t", "scenario": {"format": "jsonl"},
            "competitor": handle.describe(), **handle.flat_columns()}


def test_a_contended_run_writes_both_flows_and_the_index_into_a_results_directory(tmp_path):
    # The header test the acceptance criterion asks for, over a results
    # directory this test generates: the columns exist, and they are NOT BLANK
    # for a run that named a competitor.
    handle, _ = run_three_phases([9.0, 4.0, 9.0], vtop_bytes_after=20_000_000)
    summary = summary_with_competitor(handle)
    writer = ResultsWriter(str(tmp_path), "contended-run")
    writer.row("metrics.csv", summary)
    writer.write_summary(summary)
    writer.close()

    with open(os.path.join(str(tmp_path), "contended-run", "metrics.csv"), encoding="utf-8") as fh:
        row = next(iter(csv.DictReader(fh)))
    for column in COMPETITOR_COLUMNS:
        assert column in row, f"{column} is missing from metrics.csv"
        assert row[column] not in ("", "None"), (
            f"{column} is blank for a run that named a competitor: a contended run that "
            "records no fairness number is an uncontended run under the wrong name")

    matrix = matrix_row(summary)
    for column in COMPETITOR_COLUMNS:
        assert column in COMPARE_COLS, f"{column} is missing from the comparison matrix"
        assert matrix[column] not in ("", "None")


def test_a_run_with_no_competitor_states_the_columns_blank_rather_than_omitting_them():
    # The matrix fills a column it cannot find from the SCENARIO, which is how
    # an unshaped baseline once came to claim a driver it never ran.
    row = matrix_row({"scenario_name": "16-contended-bottleneck",
                      **competitor.blank_columns(),
                      "scenario": {"shaping_competitor": "bulk:60"}})
    for column in COMPETITOR_COLUMNS:
        assert row[column] == "", (
            f"{column} must stay blank for a run that started no competitor, whatever the "
            "scenario file asked for: what the run MEASURED outranks what it requested")


def test_the_fairness_index_never_reaches_a_results_file_without_the_two_flows():
    # The index is symmetric, so it cannot say which flow won. Every file that
    # carries it must carry the per-flow numbers beside it — asserted over the
    # headers themselves, so a future column list cannot drop one of them.
    headers = {**{name: cols for name, cols in CSV_HEADERS.items()},
               "matrix.csv": list(COMPARE_COLS)}
    carrying = [name for name, cols in headers.items() if "competitor_jain_index" in cols]
    assert carrying, (
        "no results file carries the index at all, so this test is checking nothing")
    for name in carrying:
        for column in ("competitor_goodput_mbps_with", "competitor_goodput_mbps_without",
                       "vtop_goodput_mbps_with"):
            assert column in headers[name], (
                f"{name} carries competitor_jain_index without {column}: an index alone is "
                "symmetric and does not say which flow won")


def test_the_summary_the_runner_prints_carries_the_competing_flow(tmp_path):
    # summary.md is the artifact the runner points an operator at when a run
    # ends, and a newly added value has already been left out of it once.
    handle, _ = run_three_phases([9.0, 4.0, 9.0], vtop_bytes_after=20_000_000)
    md = _summary_md(summary_with_competitor(handle))
    assert "Competing flow" in md, "the human-facing table must name the measurement"
    assert "4.0 Mbit/s with VTOP" in md and "9.0 Mbit/s alone" in md, (
        "with both per-flow numbers in it: a reader of summary.md must not have to open "
        "the JSON to find out what the run cost its neighbour")
    assert "Jain" in md
    assert "none (no competing flow)" in _summary_md({"scenario": {}}), (
        "and a run that started none says so, rather than leaving an empty cell that "
        "reads like a missing measurement")


class KeysRead(dict):
    """A record that remembers every key a renderer asked it for, nested ones
    as `outer.inner`, so a test can compare what a renderer READS against what
    describe() WRITES."""

    def __init__(self, record, read, prefix=""):
        super().__init__(record)
        self._read = read
        self._prefix = prefix

    def _wrap(self, key, value):
        self._read.add(self._prefix + key)
        if isinstance(value, dict):
            return KeysRead(value, self._read, f"{self._prefix}{key}.")
        return value

    def get(self, key, default=None):
        return self._wrap(key, super().get(key, default))

    def __getitem__(self, key):
        return self._wrap(key, super().__getitem__(key))


def written_keys(record, prefix=""):
    keys = set()
    for key, value in record.items():
        keys.add(prefix + key)
        if isinstance(value, dict):
            keys |= written_keys(value, f"{prefix}{key}.")
    return keys


def test_every_key_the_human_facing_lines_read_is_one_describe_writes():
    # THE FINDING (review). describe() records the window's overhead as
    # `competitor_window_unaccounted_seconds` and the summary.md line looked up
    # `window_unaccounted_seconds`, so the value was silently never printed — a
    # `.get()` with a fallback cannot tell a renamed key from an absent value.
    # Asserted over what the renderers actually READ, so the next renamed key
    # fails here instead of vanishing from summary.md.
    from run_benchmark import _bottleneck

    handle, _ = run_three_phases([9.0, 4.0, 9.0], vtop_bytes_after=20_000_000)
    record = handle.describe()
    read: set = set()
    describe_competitor_line(KeysRead(record, read))
    _bottleneck({"competitor": KeysRead(record, read)})
    assert read, "the renderers read nothing, so this test is checking nothing"
    missing = sorted(read - written_keys(record))
    assert not missing, (
        f"the human-facing lines read {missing}, which describe() never writes: each renders "
        "as '?' or not at all, and summary.md silently loses the value")

    line = describe_competitor_line(record)
    assert f"{record['competitor_window_unaccounted_seconds']}s of command overhead" in line, (
        "the recorded overhead reaches the prose, from the key describe() actually records")
    assert f"to within {record['competitor_window_sync_spread_seconds']}s" in line, (
        "and so does the spread the window's placement rests on")


def test_the_prose_puts_the_two_flows_before_the_index():
    line = describe_competitor_line({
        "mode": "bulk", "window_seconds": 60, "solo_mean_mbps": 9.1,
        "solo_disagreement_pct": 2.2, "share_of_solo_pct": 44.0,
        "with_vtop": {"goodput_mbps": 4.0}, "vtop_goodput_mbps": 5.1,
        "competitor_goodput_mbps_over_vtop_window": 4.4, "jain_index": 0.99})
    assert line.index("4.0") < line.index("Jain"), (
        "the per-flow numbers come first: an index leading the sentence is the summary a "
        "reader remembers, and it is the half that cannot say which flow won")
    assert "9.1" in line and "5.1" in line, "both flows and the baseline are in one line"
    assert "4.4" in line and line.index("4.4") < line.index("Jain"), (
        "and the index's OWN second rate is beside VTOP's, before the index: a reader who "
        "paired 0.99 with the 4.0 above it would be reading it against a rate over a "
        "different span from the one it was taken on")


def test_the_shaping_and_competitor_columns_stay_distinct():
    # Two tuples reach the same files. A collision would make one of them
    # silently overwrite the other in every row.
    from lib.shaping import SHAPING_COLUMNS
    assert not set(SHAPING_COLUMNS) & set(COMPETITOR_COLUMNS)
    assert not set(Shape("http://x", "minio", 1250, 100, 20).flat_columns()) & set(
        COMPETITOR_COLUMNS), (
        "a shaped row and a contended row are written into one dict, and a shared key "
        "would drop whichever was written first")


def test_the_resource_summary_covers_only_the_engines_own_window():
    # SystemMonitor is the outermost context, so it samples through both solo
    # windows — during which the engine is deliberately idle and the competitor
    # is saturating the link. duration_seconds already excludes those seconds;
    # without the same bound the resource columns described a different
    # interval from the one printed beside them (review).
    import os
    import tempfile

    from run_benchmark import _sys_summary

    directory = tempfile.mkdtemp()
    with open(os.path.join(directory, "system_metrics.csv"), "w") as handle:
        handle.write("timestamp,cpu_percent,memory_mb,disk_read_mb,disk_write_mb,"
                     "network_tx_mb,network_rx_mb\n")
        # idle solo window, busy engine window, idle solo window
        handle.write("2026-01-01T00:00:00Z,0,10,0,0,0,0\n")
        handle.write("2026-01-01T00:01:00Z,80,10,0,0,0,0\n")
        handle.write("2026-01-01T00:02:00Z,0,10,0,0,0,0\n")

    unbounded = _sys_summary(directory)
    assert unbounded["cpu_avg_percent"] < 30, (
        "unbounded, the two idle solo windows drag the engine's average down by two "
        "thirds — the dilution this bound exists to remove"
    )

    bounded = _sys_summary(directory, since_iso="2026-01-01T00:00:30Z",
                           until_iso="2026-01-01T00:01:30Z")
    assert bounded["cpu_avg_percent"] == 80, (
        "bounded to the engine's window, cpu_avg_percent is the engine's own load, "
        "which is what makes a contended run comparable with an uncontended one"
    )

    # A sample with no timestamp is KEPT: dropping it would silently narrow the
    # sample set on exactly the runs where the sampler is already misbehaving.
    with open(os.path.join(directory, "system_metrics.csv"), "a") as handle:
        handle.write(",50,10,0,0,0,0\n")
    assert _sys_summary(directory, since_iso="2026-01-01T00:00:30Z",
                        until_iso="2026-01-01T00:01:30Z")["cpu_max_percent"] == 80


def test_vtops_window_opens_after_iperfs_omitted_ramp():
    # iperf3 runs `-O <omit>` seconds before the window it reports, so a
    # boundary taken when the flow starts covers seconds the competitor's own
    # number does not: a bulk:10 run compared a 10s receiver summary against
    # 13s of VTOP bytes, and Jain's index was computed from two rates over
    # different intervals (review). The ramp is one of the two facts the window
    # is now derived from rather than a delay anything sleeps out.
    engine = ScriptedEngine()
    box = ScriptedCompetitor(
        [9.0, 4.0, 9.0],
        # A cycle half a second into the ramp, and one at the earliest instant
        # the window can have opened. Only the second may be charged to it.
        during=cycles_inside_the_window(engine,
                                        [(0.5, 7_000_000), (0, 5_000_000)]))

    started = time.monotonic()
    with contended(scripted_spec(omit=1),
                   vtop_bytes=engine, run=box, log=lambda _m: None) as handle:
        box.handle = handle
        handle.start_with_vtop()
        wait_for(lambda: handle._collected is not None, "collected its report")
        handle.close()

    opened_at, _closes_at = handle._window
    assert opened_at - started >= 1.0, (
        "the window must open only once the ramp iperf3 omits has passed, or VTOP is "
        "measured over an interval the competitor's own summary excludes"
    )
    assert handle.vtop_attributed_cycles == 1, (
        "and the cycle that was uploading during the ramp is charged to neither side: its "
        "bytes crossed the link before the competitor's own summary begins, so pairing "
        "them with the flow's post-ramp seconds is the mismatch the ramp bound exists to "
        "prevent")
    assert handle.vtop_bytes_in_window == 5_000_000, (
        "only the cycle that began at the boundary reaches the share")
    assert handle.vtop_mbps > 0, "and the share over that window is still recorded"


def test_a_window_that_saw_no_engine_cycle_is_refused_rather_than_recorded():
    # The engine's committed-byte total moves only when a process_once returns.
    # A cycle spanning the whole window commits outside both edges, so the
    # difference reads zero for an engine that was uploading steadily — and a
    # fairness index built on that zero credits the competitor with a share it
    # never had to fight for (review). Unknown is not zero.
    box = ScriptedCompetitor([9.0, 4.0, 9.0])
    with contended(scripted_spec(), vtop_bytes=lambda: 0, run=box,
                   log=lambda _m: None) as handle:
        handle.start_with_vtop()
        wait_for(lambda: handle._collected is not None, "collected its report")
        with pytest.raises(ShapingError) as exc:
            handle.close()
    assert "unknown, not zero" in str(exc.value), (
        "the refusal must say why the zero cannot be used, or the next reader raises the "
        "window and wonders why"
    )

    # With a cycle observed WHOLLY INSIDE the window, the same zero is a
    # MEASUREMENT: the engine really did commit nothing while its neighbour was
    # measuring, and that is a fair-share result worth recording rather than
    # the absence of one.
    engine = ScriptedEngine()
    box = ScriptedCompetitor(
        [9.0, 4.0, 9.0], during=cycles_inside_the_window(engine, [(0, 0)]))
    with contended(scripted_spec(), vtop_bytes=engine, run=box, log=lambda _m: None) as handle:
        box.handle = handle
        handle.start_with_vtop()
        wait_for(lambda: handle._collected is not None, "collected its report")
        handle.close()
    assert handle.vtop_attributed_cycles == 1 and handle.vtop_bytes_in_window == 0, (
        "an engine cycle that ran wholly inside the window and committed nothing is a "
        "real zero, not an unknown one: refusing it would throw away the very result a "
        "fairness measurement exists to find")
    assert handle.vtop_mbps == 0.0 and handle.jain is not None, (
        "and it still reaches the index, which is where an engine that yielded the whole "
        "link to its neighbour is supposed to show up")


# --------------------------------------------------------------------------
# Which cycles the contended window may be charged with
# --------------------------------------------------------------------------


def test_a_cycle_that_began_before_the_window_opened_is_charged_to_neither_side():
    # iperf3 omits its ramp, so the window opens seconds after the flow starts
    # and the engine is already mid-cycle when it does. That cycle's bytes
    # arrive as ONE step the counter takes when the call returns, and the step
    # includes everything the cycle uploaded during the ramp — so charging the
    # window with it credits the contended interval with uploads that happened
    # before the interval existed (review). There is no honest way to split the
    # step, because the run does not know when inside the call each byte left.
    engine = ScriptedEngine()
    box = ScriptedCompetitor(
        [9.0, 4.0, 9.0],
        during=cycles_inside_the_window(engine,
                                        [(1.0, 7_000_000), (0, 1_000_000)]))
    with contended(scripted_spec(), vtop_bytes=engine, run=box, log=lambda _m: None) as handle:
        box.handle = handle
        handle.start_with_vtop()
        wait_for(lambda: handle._collected is not None, "collected its report")
        handle.close()

    assert handle.vtop_attributed_cycles == 1, (
        "only the cycle that began after the window opened may be charged to it: the one "
        "that began a second earlier crossed the edge, and a window is charged with whole "
        "cycles or with nothing")
    assert handle.vtop_bytes_in_window == 1_000_000, (
        "the straddling cycle's 7 MB were partly uploaded before the window opened, and "
        "counting them would report VTOP as having taken eight times the share it took "
        "from the flow it was actually measured against")
    assert handle.vtop_bytes_across_window == 8_000_000, (
        "the boundary-to-boundary difference still sees all 8 MB, which is precisely why "
        "it cannot BE the share — it is recorded beside the share so a reader can see how "
        "much of the engine's movement could not be placed")


def test_a_cycle_that_returned_after_the_window_closed_is_charged_to_neither_side():
    # The mirror image, and the one a boundary difference gets wrong in the
    # other direction: a cycle that was uploading while the competitor measured
    # but returned after the flow finished moves the counter outside the window
    # entirely, so the difference credits the window with none of it. An
    # unsplittable straddle either way — refused a share, not given a guess.
    engine = ScriptedEngine()
    box = ScriptedCompetitor(
        [9.0, 4.0, 9.0],
        during=cycles_inside_the_window(engine, [(0, 1_000_000)]))
    with contended(scripted_spec(), vtop_bytes=engine, run=box, log=lambda _m: None) as handle:
        box.handle = handle
        handle.start_with_vtop()
        wait_for(lambda: handle._collected is not None, "collected its report")
        # Began at the opening boundary, so inside the window, and returns now
        # — after the flow has taken its closing sample.
        engine.cycle(handle, box.opened_at + ON_THE_EDGE, 7_000_000)
        handle.close()

    assert handle.vtop_attributed_cycles == 1, (
        "the cycle that returned after the window closed is not one the window contains, "
        "however much of it ran inside")
    assert handle.vtop_bytes_in_window == 1_000_000, (
        "and its 7 MB stay out of the share: a cycle is attributed by the interval it "
        "occupied, not by which side of an edge its counter update happened to land on")
    assert handle.vtop_bytes_across_window == 1_000_000, (
        "the boundary difference misses those 7 MB completely — the same counter, wrong "
        "in the opposite direction, which is why neither edge can be trusted on its own")


def test_the_window_is_charged_with_the_cycles_it_wholly_contains_over_their_own_span():
    # What the rule is FOR: two cycles inside the window contribute the two
    # steps they made, summed, over the interval those steps actually cover.
    engine = ScriptedEngine()
    box = ScriptedCompetitor(
        [9.0, 4.0, 9.0],
        during=cycles_inside_the_window(engine,
                                        [(0, 2_000_000), (0, 3_000_000)]))
    with contended(scripted_spec(), vtop_bytes=engine, run=box, log=lambda _m: None) as handle:
        box.handle = handle
        handle.start_with_vtop()
        wait_for(lambda: handle._collected is not None, "collected its report")
        handle.close()

    assert handle.vtop_attributed_cycles == 2
    assert handle.vtop_bytes_in_window == 5_000_000, (
        "each cycle contributes the difference IT made to the total, so two cycles inside "
        "the window are the sum of their two steps — not a re-read of a counter that "
        "cannot say which call moved it")
    assert handle.vtop_window_seconds >= 2 * SCRIPTED_CYCLE_SECONDS, (
        "the span runs from the first attributed cycle's start to the last one's return, "
        "so a second cycle inside the window lengthens it; a span that covered only the "
        "last cycle would divide two cycles' bytes by one cycle's time")
    assert handle.vtop_window_seconds < handle.contended.seconds, (
        "and it is NOT the competitor's window: the straddling time whose bytes were "
        "dropped is not in the denominator either, or the rate would be bytes from one "
        "interval over the duration of another")
    assert handle.vtop_mbps == pytest.approx(
        handle.vtop_bytes_in_window * 8 / 1e6 / handle.vtop_window_seconds, rel=0.05), (
        "the recorded rate must be rebuildable from the two numbers recorded beside it, "
        "or summary.json states a goodput a reader cannot check (the tolerance is the "
        "record's own rounding of the span)")
    assert handle.describe()["vtop_window_is"].startswith("the span from the first"), (
        "and the record says which interval that is, because a reader who assumed the "
        "competitor's window would misread every contended run in the tree")


def test_a_window_of_only_straddling_cycles_is_refused_even_though_progress_landed_inside_it():
    # THE CASE THE EARLIER GUARD LET THROUGH (review). Two cycles, one crossing
    # each edge: progress lands inside the window — the first cycle returns
    # there and the second begins there — so a guard that only asked "did any
    # cycle return inside the window" was satisfied, and went on to attribute a
    # byte total made entirely of uploads that also happened outside it.
    engine = ScriptedEngine()
    box = ScriptedCompetitor(
        [9.0, 4.0, 9.0],
        during=cycles_inside_the_window(engine, [(1.0, 7_000_000)]))
    with contended(scripted_spec(), vtop_bytes=engine, run=box, log=lambda _m: None) as handle:
        box.handle = handle
        handle.start_with_vtop()
        wait_for(lambda: handle._collected is not None, "collected its report")
        engine.cycle(handle, box.opened_at + ON_THE_EDGE, 3_000_000)
        with pytest.raises(ShapingError) as exc:
            handle.close()

    message = str(exc.value)
    assert "no engine cycle lay wholly inside" in message
    assert "2 cycle(s) ran" in message and "2 of them overlapped" in message, (
        "the refusal says what it SAW: an operator told only that the share is unknown "
        "cannot tell a window that kept missing the engine's cycle from an engine that "
        "never ran at all")
    assert "unknown, not zero" in message, (
        "and why the obvious substitute is refused — a recorded zero would credit the "
        "competitor with a fair-share result it never had to fight for")
    assert "WHOLE engine cycle" in message and "shorten the cycle" in message, (
        "naming both of the things an operator can actually change, or the next run is "
        "the same run")


def test_a_batch_that_failed_inside_the_window_leaves_vtops_share_unknown_rather_than_smaller():
    # THE FINDING (review). A batch that uploaded its object and then failed
    # verification put its bytes in the same queue the competitor was measured
    # in, but the engine reports no size for it, so the committed total never
    # moves. Charged as-is, a window whose uploads all failed reads as VTOP at
    # 0 Mbit/s beside a starved competitor — an index that clears the engine of
    # exactly the traffic that did the starving.
    engine = ScriptedEngine()
    box = ScriptedCompetitor(
        [9.0, 4.0, 9.0],
        during=cycles_inside_the_window(engine, [(0, 1_000_000), (0, 0, 3)]))
    with contended(scripted_spec(), vtop_bytes=engine, run=box, log=lambda _m: None) as handle:
        box.handle = handle
        handle.start_with_vtop()
        wait_for(lambda: handle._collected is not None, "collected its report")
        with pytest.raises(ShapingError) as exc:
            handle.close()

    message = str(exc.value)
    assert "3 uncommitted batch(es)" in message and "1 of the 2 engine cycle(s)" in message, (
        "the refusal says what it saw — how many batches, in how many of the attributed "
        "cycles — so an operator can find them in batch_metrics.csv")
    assert "unknown, not zero and not the smaller number" in message, (
        "and names both substitutes it refuses: a zero, and the partial count of the "
        "batches that did commit, which understates the engine by the failed uploads")
    assert handle.jain is None and len(box.calls) == 2, (
        "no index is computed and no closing solo window is spent on a run already refused")


def test_a_failure_in_a_cycle_outside_the_indexs_span_does_not_void_the_share():
    # The refusal is scoped to what the two rates are taken over. A cycle that
    # crossed the window's opening edge is excluded from the span entirely, so
    # its failed batches move neither VTOP's rate nor the competitor's over that
    # span — refusing on them would throw away a measurement nothing was wrong
    # with.
    engine = ScriptedEngine()
    box = ScriptedCompetitor(
        [9.0, 4.0, 9.0],
        during=cycles_inside_the_window(engine, [(1.0, 0, 2), (0, 1_000_000)]))
    with contended(scripted_spec(), vtop_bytes=engine, run=box, log=lambda _m: None) as handle:
        box.handle = handle
        handle.start_with_vtop()
        wait_for(lambda: handle._collected is not None, "collected its report")
        handle.close()

    assert handle.vtop_attributed_cycles == 1 and handle.vtop_bytes_in_window == 1_000_000, (
        "the straddling cycle with failures is excluded as every straddling cycle is, and "
        "the share stands on the one whole cycle inside the window")
    assert handle.jain is not None


def test_a_cycle_noted_as_returning_before_it_began_is_refused():
    # The end is the caller's clock read now, not one taken inside the handle,
    # so the handle can no longer guarantee the order itself. Two reads swapped
    # by a caller would otherwise build a negative interval that a later span
    # computation silently absorbs.
    handle = competitor.Contention(scripted_spec(), ScriptedCompetitor([9.0]),
                                   vtop_bytes=lambda: 0, log=lambda _m: None)
    with pytest.raises(ShapingError, match="returning 1.000s before it began"):
        handle.note_progress(10.0, 9.0)


# --------------------------------------------------------------------------
# The runner's side of the cycle log
# --------------------------------------------------------------------------


def test_uncommitted_batches_and_failed_calls_are_what_the_committed_total_cannot_see():
    from run_benchmark import uncounted_batches

    committed = {"batch_id": "b1", "committed": True, "final_state": "source_committed"}
    failed = {"batch_id": "b2", "committed": False, "final_state": "failed", "metrics": None}
    verified = {"batch_id": "b3", "committed": False, "final_state": "verified"}
    nothing_to_read = {"batch_id": "", "committed": False, "final_state": "discovered"}

    assert uncounted_batches(0, [committed, nothing_to_read]) == 0, (
        "a committed batch is in the total, and the engine's empty-read marker uploaded "
        "nothing — neither may void a contended run")
    assert uncounted_batches(0, [committed, failed, verified]) == 2, (
        "every batch that did not commit is counted, whatever stage it stopped at: the "
        "engine's output does not say whether a failed batch reached the store")
    assert uncounted_batches(1, []) == 1, (
        "and a call that exited nonzero counts even with no outcomes — it may have uploaded "
        "any number of batches before it stopped and printed nothing")


class RecordingContention:
    """The runner's side of the contended bracket, recorded rather than judged.

    `close()` raises so the runner stops right there: what these tests read is
    what the loop handed the handle, and nothing after the bracket.
    """

    def __init__(self):
        self.noted = []

    def start_with_vtop(self):
        pass

    def note_progress(self, started_at, ended_at, uncounted_batches=0):
        self.noted.append((started_at, ended_at, uncounted_batches))

    def close(self):
        raise RuntimeError("bracket closed")


def test_the_runner_stamps_a_cycles_end_when_process_once_returns_not_after_its_bookkeeping(
        tmp_path, monkeypatch):
    # THE FINDING (review). The runner noted a cycle only after parsing its
    # outcomes and writing each batch's flushed CSV rows, and the handle read the
    # clock then — so every cycle was stretched by harness time in which the
    # engine uploaded nothing, enough to carry a cycle that returned inside the
    # window across its closing edge. A writer made slow here makes that
    # bookkeeping impossible to miss.
    import contextlib

    import run_benchmark

    bookkeeping = 0.2
    recording = RecordingContention()
    returned = []
    calls = {"n": 0}
    committed = {"batch_id": "b1", "committed": True, "final_state": "source_committed",
                 "object_uri": "s3://bucket/b1",
                 "metrics": {"compressed_bytes": 10, "uncompressed_bytes": 20,
                             "total_ms": 5, "object_upload_ms": 2}}
    failed = {"batch_id": "b2", "committed": False, "final_state": "failed", "metrics": None}

    def process_once(*_a, **_k):
        calls["n"] += 1
        result = [(0, [failed], "verify failed"), (1, [], "boom"),
                  (0, [committed], "")][min(calls["n"], 3) - 1]
        returned.append(time.monotonic())
        return result

    class SlowWriter(run_benchmark.ResultsWriter):
        def row(self, name, row):
            if name == "batch_metrics.csv":
                time.sleep(bookkeeping)
            return super().row(name, row)

    @contextlib.contextmanager
    def contended(_spec, vtop_bytes):
        yield recording

    monkeypatch.setattr(run_benchmark.competitor.CompetitorSpec, "from_scenario",
                        classmethod(lambda cls, sc, shape: object()))
    monkeypatch.setattr(run_benchmark.competitor, "contended", contended)
    monkeypatch.setattr(run_benchmark, "ResultsWriter", SlowWriter)
    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path", lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config", lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint", lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.engine, "process_once", process_once)
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 1, "bytes": 1024})
    scenario = tmp_path / "contended.yaml"
    scenario.write_text(
        "name: contended-bookkeeping\n"
        "backend: s3_native\n"
        "endpoint_url: http://localhost:9000\n"
        "volume: 1\n"
        "duration_seconds: 0.5\n"
        "sys_sample_interval: 0.1\n",
        encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["run_benchmark.py", str(scenario),
                                      "--results-dir", str(tmp_path / "results")])
    with pytest.raises(RuntimeError, match="bracket closed"):
        run_benchmark.main()

    assert len(recording.noted) >= 3 and len(recording.noted) == len(returned), (
        "one note per cycle, every cycle — a skipped note folds two cycles into one interval")
    for index, (started_at, ended_at, _) in enumerate(recording.noted):
        came_back = returned[index]
        assert started_at <= came_back <= ended_at + 1e-9
        assert ended_at - came_back < bookkeeping / 2, (
            f"the cycle was recorded as ending {ended_at - came_back:.3f}s after process_once "
            "returned: the harness's own parsing and flushed CSV writes were charged to the "
            "engine's cycle, which can push a cycle that returned inside the contended window "
            "across its closing edge and widens the span VTOP's bytes are divided by")
    assert [noted[2] for noted in recording.noted[:3]] == [1, 1, 0], (
        "and each cycle carries what its committed total cannot see: a failed batch, a call "
        "that exited nonzero, then a clean commit — the handle refuses a share built on "
        "either of the first two")


def test_the_cumulative_counters_are_rebased_at_the_engine_boundary():
    # Filtering rows is not enough for a counter (review). SystemMonitor takes
    # its baseline when the monitor starts — before the first solo window — so
    # every retained row is still cumulative from it, and the first solo
    # phase's traffic stays embedded in the maxima even after the CPU average
    # is corrected. The counters are rebased on the engine's own boundary.
    import os
    import tempfile

    from run_benchmark import _sys_summary

    directory = tempfile.mkdtemp()
    with open(os.path.join(directory, "system_metrics.csv"), "w") as handle:
        handle.write("timestamp,cpu_percent,memory_mb,disk_read_mb,disk_write_mb,"
                     "network_tx_mb,network_rx_mb\n")
        # solo window: the competitor puts 500 MB on the wire
        handle.write("2026-01-01T00:00:00Z,0,10,0,0,0,0\n")
        handle.write("2026-01-01T00:00:20Z,0,10,0,0,500,0\n")
        # engine window: the engine adds 40 more
        handle.write("2026-01-01T00:01:00Z,80,10,0,0,540,0\n")

    bounded = _sys_summary(directory, since_iso="2026-01-01T00:00:30Z",
                           until_iso="2026-01-01T00:01:30Z")
    assert bounded["network_tx_mb"] == 40, (
        "the engine's own window moved 40 MB; reporting 540 charges it with the "
        "competitor's solo phase, which is the traffic this bound exists to exclude"
    )


# --------------------------------------------------------------------------
# Which SPAN the fairness index is taken over
# --------------------------------------------------------------------------


def cycles_at(engine, placements):
    """A `during` hook that runs engine cycles at real offsets into the flow.

    `placements` are (seconds after the flow's launch plus its ramp, bytes)
    pairs, and the wait for each one is a real deadline poll rather than a start
    supplied to `note_progress`. `cycles_inside_the_window` constructs its
    starts, which is right for tests about which side of an EDGE a cycle falls
    on; these tests are about where in the flow's own run — and so in its
    interval stream — a cycle lands, so the cycle has to occupy a span the clock
    agrees with.

    The offsets are from the LAUNCH, not from the window: with a flow that spends
    `setup` before measuring, the window opens later than the launch by exactly
    that, and it is where the module puts the window relative to the launch that
    these tests measure.
    """

    def during(phase, box):
        if phase != 2:  # the contended window is the second of the three
            return
        omit = box.opened_at - box.entered_at - box.setup
        launched = box.entered_at + omit
        for after, committed in placements:
            wait_until(launched + after, f"reached {after}s past its launch")
            engine.cycle(box.handle, time.monotonic(), committed)

    return during


def run_with_report(report, vtop_bytes=2_400, placements=None, setup=0.0, teardown=0.0,
                    stamps=None):
    """One contended run whose middle phase reports exactly `report`.

    The two solo windows stay flat and agreeing, so a drift refusal never fires
    in a test that is about the contended window's own shape.
    """
    engine = ScriptedEngine()
    during = (cycles_at(engine, placements) if placements
              else cycles_inside_the_window(engine, [(0, vtop_bytes)]))
    box = ScriptedCompetitor([9.0, 8.0, 9.0], during=during, contended_report=report,
                             setup=setup, teardown=teardown, stamps=stamps)
    with contended(scripted_spec(), vtop_bytes=engine, run=box, log=lambda _m: None) as handle:
        box.handle = handle
        handle.start_with_vtop()
        wait_for(lambda: handle._collected is not None, "collected its report")
        handle.close()
    return handle


# A competitor that ran slowly for the window's first five seconds and then at
# ten times that. Real flows do this — a queue that fills, a retransmission
# burst, the engine's own concurrency ramping — and it is the case in which the
# window's average describes neither half of the window.
UNEVEN_PER_SECOND = [1.0] * 5 + [10.0] * 25
UNEVEN_SECONDS = len(UNEVEN_PER_SECOND) * SCRIPTED_STEP_SECONDS


def uneven_report():
    return iperf_report(8.0, seconds=UNEVEN_SECONDS, sent_mbps=8.5,
                        per_second=UNEVEN_PER_SECOND, step=SCRIPTED_STEP_SECONDS)


def test_the_index_is_taken_over_the_span_vtop_was_measured_over_not_the_windows_average():
    # THE FINDING (review). VTOP's share is charged cycle by cycle, so its rate
    # covers the cycles the window wholly contained — a span nested inside the
    # window. Pairing that with the competitor's average over the whole 60
    # seconds compares a slice against an average, and Jain's index is the
    # headline number the milestone reports.
    handle = run_with_report(uneven_report())

    assert handle.contended.mbps == 8.0, (
        "the whole-window figure is still recorded: it is the honest answer to what the "
        "neighbour got over the minute, and the index's span is a slice of that minute")
    assert handle.competitor_mbps_in_vtop_window < handle.contended.mbps / 5, (
        "the engine's cycles all landed in the window's slow opening seconds, so the "
        "competitor's rate over that span is a fraction of its average — which is the "
        "whole point: the two are different numbers and only one of them is comparable "
        "with VTOP's")

    same_span = jain_index([handle.competitor_mbps_in_vtop_window, handle.vtop_mbps])
    from_the_average = jain_index([handle.contended.mbps, handle.vtop_mbps])
    assert handle.jain == same_span, (
        "the recorded index must be the one over two rates covering ONE interval, or it "
        "is a fairness number about no particular stretch of the run")
    assert handle.jain - from_the_average > 0.25, (
        "and the two differ materially here: an index built from the window's average "
        "would report a link the engine dominated when the two flows were in fact "
        "sharing it evenly over every second either of them was measured on")

    columns = handle.flat_columns()
    assert columns["competitor_goodput_mbps_over_vtop_window"] == \
        handle.competitor_mbps_in_vtop_window, (
        "the index's own second input reaches the results files beside it: an index whose "
        "inputs cannot both be read back is one a reader has to take on trust")
    assert columns["competitor_goodput_mbps_with"] == handle.contended.mbps, (
        "without displacing the whole-window figure — dropping that would answer 'what "
        "did this cost the neighbour' with a number about the engine's cycles instead")


def test_a_flow_whose_rate_changed_is_not_represented_by_its_average_over_the_window():
    # The same engine placement against a flat flow and against an uneven one.
    # If the covered span were being filled in from `end.sum_received` the two
    # would come out identical, and this test would pass trivially.
    flat = run_with_report(iperf_report(8.0, seconds=SCRIPTED_WINDOW_SECONDS, sent_mbps=8.5,
                                        step=SCRIPTED_STEP_SECONDS))
    assert flat.competitor_mbps_in_vtop_window == pytest.approx(flat.contended.mbps, rel=0.02), (
        "a flow that held one rate all window is the same rate over any span of it, so "
        "the covered-span figure must agree with the average here — the span machinery "
        "must not invent a difference where the stream reports none")

    uneven = run_with_report(uneven_report())
    assert uneven.contended.mbps == flat.contended.mbps, (
        "the two runs report the same whole-window average by construction, which is what "
        "makes the next assertion about the stream and not about the summary")
    assert uneven.competitor_mbps_in_vtop_window < flat.competitor_mbps_in_vtop_window / 5, (
        "and with the same average the covered-span rates differ by most of an order of "
        "magnitude, because the stream says the flow was slow exactly where the engine's "
        "cycles fell. A rate read off the average could not tell these two runs apart")

    record = uneven.describe()
    assert record["jain_span_seconds"] == record["vtop_window_seconds"], (
        "the record names the ONE span the index is over, or a reader assumes it is the "
        "competitor's window and misreads every contended run in the tree")
    assert record["competitor_bytes_over_vtop_window"] == \
        uneven.competitor_bytes_in_vtop_window, (
        "with the bytes beside the rate, so summary.json states a goodput a reader can "
        "rebuild rather than one they must take")
    assert record["competitor_intervals_over_vtop_window"] >= 1, (
        "and how many of the report's own seconds the span touched: a rate derived from "
        "one interval and one derived from fifty are not equally trustworthy")


def test_the_covered_spans_bytes_are_the_receivers_and_never_the_senders():
    # The module's loudest rule, at the one place the new arithmetic could
    # quietly break it: iperf3's interval entries are the CLIENT's sending side
    # and add up to sum_sent, which at the far end of a token bucket counts
    # bytes still in the queue. Taken literally they would report the competing
    # flow as less harmed than it was — the one direction this measurement must
    # not fail in — so they supply the shape and the receiver supplies the size.
    sample = parse_report(iperf_report(4.0, seconds=6.0, sent_mbps=8.0), spec(6), "with VTOP")
    whole, touched = competitor.competitor_bytes_over(sample, 1_000.0, 1_000.0, 1_006.0)
    assert whole == sample.bytes, (
        "over the whole window the derived figure is the receiver's own total, exactly: "
        "the stream is only ever asked how those bytes were spread across the seconds")
    assert touched == 6, "and every second of the window is accounted for"

    sender_bytes = sum(slot.bytes for slot in sample.intervals)
    assert sender_bytes > sample.bytes * 1.5, (
        "the fixture's stream really is the inflated sender's view, so a test that passed "
        "by reading it would have to be reading twice the bytes that crossed the link")
    half, _ = competitor.competitor_bytes_over(sample, 1_000.0, 1_000.0, 1_003.0)
    assert half == pytest.approx(sample.bytes / 2, rel=0.01), (
        "and half the window is half the receiver's bytes on a flat flow, never half the "
        "sender's — the queue's worth of un-arrived bytes must not reappear in a slice")


def test_the_omitted_ramps_seconds_are_left_out_of_the_competitors_stream():
    # iperf3 puts the ramp it omits from its own summary INTO the interval
    # stream and flags it, and restarts the report's clock when the ramp ends.
    # A mapping that trusted the entries as they come would charge the window's
    # opening seconds with the ramp's bytes — which is the off-by-the-ramp error
    # a previous round fixed at the harness's end, arriving at iperf3's.
    sample = parse_report(
        iperf_report(8.0, seconds=6.0, sent_mbps=9.0, per_second=[9.0] * 6, omit=3,
                     ramp_mbps=0.5),
        CompetitorSpec(mode="bulk", seconds=6, omit=3), "with VTOP")

    assert len(sample.intervals) == 6, (
        "six measured seconds survive and the ramp's do not: an omitted second left in "
        "would be mapped onto the measured window a second time, over the seconds that "
        "already have their own entry")
    assert [round(slot.start, 3) for slot in sample.intervals] == [0.0, 1.0, 2.0, 3.0, 4.0, 5.0], (
        "and they are placed from the start of the window iperf3 MEASURED, which is where "
        "the harness took its opening boundary. Anchored on the ramp instead they would "
        "begin at 3.0, and anchored on the straddling entry's own `start` — a value iperf3 "
        "fills from a clock frame it has already discarded — at 1.0")
    assert sample.intervals[-1].end == pytest.approx(6.0), (
        "and the last one ends where the receiver's summary says the window did")
    assert sum(slot.bytes for slot in sample.intervals) == 6 * int(9.0 * 1e6 / 8), (
        "the surviving bytes are the measured ones only: the ramp's slow-start bytes are "
        "not the flow's rate and spreading the receiver's total over them would understate "
        "every second of the window")

    opening_second, _ = competitor.competitor_bytes_over(sample, 1_000.0, 1_000.0, 1_001.0)
    assert opening_second * 8 / 1e6 == pytest.approx(8.0, rel=1e-6), (
        "so the window's first second reads at the flow's own rate. With the ramp left in "
        "it reads high, because that second would carry two entries' bytes and only one "
        "second of time — and the number that error lands in is the fairness index")


def test_a_report_with_no_interval_stream_is_refused_rather_than_averaged():
    # Unknown is not "use the average": the average is over the whole window and
    # VTOP's share is over the cycles inside it, so filling the span in from the
    # summary is exactly the mismatch this round exists to remove.
    stripped = "\n".join(line for line in iperf_report(4.0, seconds=30.0).splitlines()
                         if json.loads(line)["event"] != "interval")
    with pytest.raises(ShapingError, match="no interval stream") as exc:
        parse_report(stripped, spec(30), "with VTOP")
    assert "two different spans" in str(exc.value), (
        "the refusal says why the obvious substitute is refused, or the next reader adds "
        "the whole-window average back as an obvious improvement")
    assert "netshoot" in str(exc.value), "and names the iperf3 that does emit one"

    with pytest.raises(ShapingError, match="no interval stream"):
        parse_report(iperf_report(4.0, seconds=30.0, intervals=[]), spec(30), "alone before")

    # A report that is ALL ramp: every entry flagged, nothing measured.
    all_ramp = iperf_report(4.0, seconds=30.0,
                            intervals=iperf_intervals([], omit=4, ramp_mbps=0.5))
    with pytest.raises(ShapingError, match="flagged omitted") as exc:
        parse_report(all_ramp, CompetitorSpec(mode="bulk", seconds=30, omit=3), "with VTOP")
    assert "ramp and nothing else" in str(exc.value)

    # And an entry this module cannot read is refused rather than skipped past:
    # a skipped entry leaves a hole, and the receiver's bytes would then be
    # spread across a window the stream no longer describes.
    holed = iperf_intervals([9.0] * 3)
    holed[1]["sum"].pop("bytes")
    with pytest.raises(ShapingError, match="cannot read"):
        parse_report(iperf_report(4.0, seconds=3.0, intervals=holed), spec(3), "with VTOP")


def test_a_span_the_report_cannot_speak_for_is_refused_rather_than_filled_in():
    # A stream with a hole in it, and the engine's only whole cycle inside the
    # hole. There is no rate to report over that span — the competitor's report
    # says nothing about those seconds — and inventing one from the surrounding
    # seconds would put a guess into the headline index.
    with_a_hole = iperf_report(4.0, seconds=0.6, intervals=[
        {"sum": {"start": 0.0, "end": 0.01, "seconds": 0.01, "bytes": 100_000,
                 "omitted": False}},
        {"sum": {"start": 0.5, "end": 0.6, "seconds": 0.1, "bytes": 1_000_000,
                 "omitted": False}}])
    with pytest.raises(ShapingError, match="no interval of the competing flow's report") as exc:
        run_with_report(with_a_hole, placements=[(0.03, 1_000_000)])
    message = str(exc.value)
    assert "0-0.600s" in message and "after the window opened" in message, (
        "the refusal says what it SAW — how long the span was, where it began, and how far "
        "the report's own stream reaches — or an operator cannot tell a holed report from "
        "a window that missed the engine entirely")
    assert "would have to be invented" in message, (
        "and why no number is recorded: a fairness index means nothing unless both of its "
        "rates cover one interval")

    # A stream that carries no bytes at all cannot place the receiver's total in
    # time either, however completely it covers the span.
    empty = parse_report(iperf_report(4.0, seconds=6.0, per_second=[0.0] * 6),
                         spec(6), "with VTOP")
    with pytest.raises(ShapingError, match="every interval of its own stream carries none"):
        competitor.competitor_bytes_over(empty, 1_000.0, 1_000.0, 1_006.0)


def test_a_stream_that_only_touches_the_span_at_both_ends_does_not_cover_it():
    # THE FINDING (review). A stream with a hole under the span still overlaps
    # it — intervals at 0-1s and 5-6s against an engine span of 0.5-5.5s touch
    # it at both ends — and a guard that asked only whether ANY interval
    # overlapped was satisfied by that. The four seconds in between were then
    # treated as four seconds in which the competitor received nothing, while
    # the rate's denominator still covered all five: a plausible number, an
    # understated flow, and an index that flatters VTOP by exactly the missing
    # stretch. Which is the one direction this measurement must never fail in.
    holed = parse_report(iperf_report(4.0, seconds=6.0, intervals=[
        {"sum": {"start": 0.0, "end": 1.0, "seconds": 1.0, "bytes": 1_000_000,
                 "omitted": False}},
        {"sum": {"start": 5.0, "end": 6.0, "seconds": 1.0, "bytes": 1_000_000,
                 "omitted": False}}]), spec(6), "with VTOP")

    with pytest.raises(ShapingError, match="says nothing about") as exc:
        competitor.competitor_bytes_over(holed, 1_000.0, 1_000.5, 1_005.5)
    message = str(exc.value)
    assert "4.000s of the 5.000s span" in message, (
        "the refusal says HOW MUCH of the span the report cannot speak for: four seconds "
        "missing from five is a different fault from four milliseconds, and only the "
        "operator can tell a starved middlebox from a rounding")
    assert "1.000-5.000s" in message and "0.500-5.500s" in message, (
        "and WHERE — the stretch it cannot speak for and the span that needed it — or the "
        "reader cannot tell whether the hole was under the engine's cycles or beside them")
    assert "less harmed than it was" in message, (
        "and which way the error would have pointed, because a refusal that reads as "
        "fussiness gets relaxed by the next person who meets it")



def test_a_hole_outside_the_span_is_refused_because_it_skews_the_proportion():
    # The receiver's total is spread in proportion to the STREAM'S bytes, so a
    # hole anywhere in the measured window drops its bytes from the denominator
    # and hands them to the seconds that remain (review). A span lying wholly
    # inside a surviving interval used to be measured from a holed report — and
    # with 4 of 6 seconds missing, credited with three times its real share.
    holed = parse_report(iperf_report(4.0, seconds=6.0, intervals=[
        {"sum": {"start": 0.0, "end": 1.0, "seconds": 1.0, "bytes": 1_000_000,
                 "omitted": False}},
        {"sum": {"start": 5.0, "end": 6.0, "seconds": 1.0, "bytes": 1_000_000,
                 "omitted": False}}]), spec(6), "with VTOP")
    with pytest.raises(ShapingError, match="1.000-5.000s of its 6s measured window") as exc:
        competitor.competitor_bytes_over(holed, 1_000.0, 1_000.2, 1_000.8)
    assert "whichever side of VTOP's span the hole sits on" in str(exc.value), (
        "the refusal must say why a hole beside the span matters, or it reads as fussiness")

    # Five entries for a six-second window: the stream is laid down so its last
    # entry ends where the window does, so the missing second is the first one.
    # VTOP's span, 2-4s, is covered — and is still refused.
    short = parse_report(iperf_report(4.0, seconds=6.0, intervals=[
        {"sum": {"start": float(second), "end": float(second + 1), "seconds": 1.0,
                 "bytes": 500_000, "omitted": False}} for second in range(5)]),
        spec(6), "with VTOP")
    with pytest.raises(ShapingError, match="0.000-1.000s of its 6s measured window"):
        competitor.competitor_bytes_over(short, 1_000.0, 1_002.0, 1_004.0)


def test_intervals_that_only_nearly_meet_are_contiguous_rather_than_gapped():
    # The other side of the same rule. iperf3 prints each interval's start, end
    # and duration rounded to six decimals, independently, and this module
    # derives an entry's beginning as `end - seconds` — so two entries that are
    # contiguous in the flow's own clock arrive a rounding, and then a float
    # subtraction, apart. Compared exactly, every join in every real report
    # reads as a hole and every contended run is refused; compared loosely
    # enough to swallow a missing second, the rule above is not a rule.
    ticks = [(1.000120, 1.000120), (2.000166, 1.000045), (3.000201, 1.000033)]
    sample = parse_report(iperf_report(4.0, seconds=3.0, intervals=[
        {"sum": {"start": end - seconds, "end": end, "seconds": seconds,
                 "bytes": 1_000_000, "omitted": False}} for end, seconds in ticks]),
        spec(3), "with VTOP")

    # The premise, checked against a LITERAL rather than against the module's
    # own constant: what this fixture has to be is a stream whose joins are
    # inexact and far below a missing entry, and a premise written in terms of
    # the tolerance would move with it and pass whatever the tolerance became.
    joins = [sample.intervals[i + 1].start - sample.intervals[i].end
             for i in range(len(sample.intervals) - 1)]
    assert all(0 < join < 1e-4 for join in joins), (
        f"the fixture's own joins {joins} must be positive and far below a millisecond, or "
        "this test would pass on a stream that an exact comparison would have accepted too "
        "and would prove nothing about the tolerance")

    whole, touched = competitor.competitor_bytes_over(sample, 0.0, 0.0, 3.0)
    assert touched == 3 and whole == sample.bytes, (
        "a span across all three of them is covered by all three of them: the receiver's "
        "own total comes back, and a run is not refused for arithmetic iperf3 performed "
        "on the way to printing its report")


# --------------------------------------------------------------------------
# Where the competitor's measured window sat on the run's clock
# --------------------------------------------------------------------------


# A flow whose command takes 1.2s and whose report says it measured 0.8 of
# them. The 0.4s left over is the wall clock no report describes — docker's
# startup and the TCP connect at one end, iperf3's closing exchange and its
# exit at the other. The command's two ends alone cannot say how it split; the
# stream's arrival stamps can, and these tests put all of it at one end or the
# other and check the window lands where the flow actually measured.
OVERHEAD_SECONDS = 0.4
OVERHEADED_MEASURED_SECONDS = 0.8


def overheaded_report(per_step=None):
    return iperf_report(4.0, seconds=OVERHEADED_MEASURED_SECONDS, per_second=per_step,
                        step=0.1)


def test_a_cycle_that_ran_during_the_flows_setup_is_charged_to_neither_side():
    # THE FINDING (review). The opening boundary used to be a thread that slept
    # the omitted ramp and called the instant it woke the start of the window.
    # That sleep began before `docker exec` had spawned anything, so docker's
    # startup, the DNS lookup and the TCP connect elapsed inside it and the
    # boundary landed BEFORE iperf3 started measuring. A cycle 0.2s in ran while
    # the command was still connecting; one at 0.6s ran inside the window the
    # stream places at 0.4-1.2s.
    handle = run_with_report(
        overheaded_report(), setup=OVERHEAD_SECONDS,
        placements=[(0.2, 7_000_000), (0.6, 1_000_000)])

    assert handle.vtop_attributed_cycles == 1, (
        "a cycle that ran before the competitor's own stream says it began measuring is "
        "charged to neither side: a share paired with seconds the flow never reported is the "
        "corruption the placed boundary exists to prevent")
    assert handle.vtop_bytes_in_window == 1_000_000, (
        "so the setup cycle's 7 MB stay out of the share — with the old sleep they were "
        "eight ninths of it, and the fairness index reported an engine that had taken the "
        "link during seconds the competitor's summary does not cover")
    assert handle.window_unaccounted_seconds == pytest.approx(OVERHEAD_SECONDS, abs=0.1), (
        "and the run RECORDS the plumbing's overhead around the window rather than assuming "
        "it away")
    assert handle.window_setup_seconds == pytest.approx(OVERHEAD_SECONDS, abs=0.1), (
        "together with how much of it fell BEFORE measuring began — the split a run holding "
        "only the command's two ends could not see, and the one that decides which seconds "
        "of the flow the engine's span is read against")
    assert handle.vtop_bytes_across_window == 1_000_000, (
        "the counter's movement across the window excludes the setup cycle too: those 7 MB "
        "were already in the total when the window opened")


def test_a_cycle_that_returned_in_the_collection_tail_is_charged_to_neither_side():
    # The far edge. A report is collected at iperf3's last measured second PLUS
    # the time the client took to exchange results, exit and travel back
    # through docker exec, and the report says nothing about that tail. A cycle
    # returning inside it would stretch the shared span past the last second
    # either flow was measured on.
    handle = run_with_report(
        overheaded_report(), teardown=OVERHEAD_SECONDS,
        placements=[(0.6, 1_000_000), (0.95, 7_000_000)])

    assert handle.vtop_attributed_cycles == 1, (
        "only the cycle that ran while the competitor was measuring may be charged to the "
        "shared span; the one that returned after the stream's own last second is charged to "
        "neither side, exactly like a cycle that crossed an edge")
    assert handle.vtop_bytes_in_window == 1_000_000, (
        "and its 7 MB stay out of the share rather than being divided by a span the "
        "competitor's report cannot cover")
    assert handle.competitor_intervals_in_vtop_window == 1, (
        "the competitor's side of the index comes from the interval that really does cover "
        "the span, which is what makes the two rates comparable at all")
    assert handle.window_setup_seconds == pytest.approx(0.0, abs=0.1), (
        "and the run sees that this overhead fell AFTER the window, not before it")
    assert handle.vtop_bytes_across_window == 8_000_000, (
        "the counter's own movement across the window still sees both cycles, and is still "
        "never divided: the gap from the attributed figure is how much could not be placed")


# A flow slow for the first half of its measured window and ten times faster for
# the second, cut into 0.1s intervals.
HALF_SLOW_PER_STEP = [1.0] * 4 + [10.0] * 4


def test_the_stream_is_laid_down_where_its_lines_arrived_not_at_the_latest_admissible_opening():
    # THE FINDING (review). With only the command's two ends, a window of the
    # report's length could have opened anywhere from the launch to the
    # collection minus its length, and the stream was laid down from the LATEST
    # of those — silently assuming every second of overhead fell before
    # measuring began. Two flows with the same 0.4s of overhead, one spending it
    # connecting and one spending it closing, then produced the SAME placement,
    # and for the second the engine's span was read against seconds of the flow
    # 0.4s earlier than the ones it actually overlapped: the competitor's rate and
    # Jain's index described different seconds from the engine cycles beside
    # them.
    #
    # One engine cycle, 0.45s after the launch, in both runs. Where the overhead
    # was the closing tail, the window opened at the launch and the cycle sits
    # in its FAST second half (0.45s in); where it was the setup, the window
    # opened at 0.4s and the cycle sits in its SLOW first half (0.05s in).
    tail = run_with_report(overheaded_report(HALF_SLOW_PER_STEP), teardown=OVERHEAD_SECONDS,
                           placements=[(0.45, 1_000_000)])
    setup = run_with_report(overheaded_report(HALF_SLOW_PER_STEP), setup=OVERHEAD_SECONDS,
                            placements=[(0.45, 1_000_000)])

    average = tail.contended.mbps
    assert tail.competitor_mbps_in_vtop_window > average, (
        f"the cycle ran during the flow's fast half, so the competitor's rate over its span "
        f"must read above the window's {average} Mbit/s average — "
        f"{tail.competitor_mbps_in_vtop_window} means the stream was laid down 0.4s late, "
        "from the latest opening the command's ends allowed, and the engine's span was read "
        "against the flow's slow seconds instead")
    assert setup.competitor_mbps_in_vtop_window < average, (
        "and with the same overhead spent before measuring, the same wall-clock cycle falls "
        "in the slow half and must read below it")
    assert tail.competitor_mbps_in_vtop_window > 5 * setup.competitor_mbps_in_vtop_window, (
        "the two runs are identical at the command's two ends; only the stream's own arrival "
        "stamps tell them apart, and the rates they produce differ by the flow's own 10x step")


def test_a_stream_that_arrived_in_a_burst_is_refused_rather_than_placed():
    # A pipe that buffered the stream delivers its lines together, so their
    # arrivals date the moment the buffer flushed and not the seconds they
    # report. Placing the window from them is the ambiguity this whole
    # apparatus removes, arriving by another road.
    with pytest.raises(ShapingError, match="buffered") as exc:
        run_with_report(overheaded_report(), placements=[(0.3, 1_000_000)],
                        stamps=lambda honest: [max(honest)] * len(honest))
    assert "0.8" in str(exc.value) or "0.7" in str(exc.value), (
        "the refusal says how far the lines' placements disagreed, which is what separates a "
        "buffered stream from a jittery one")


def test_stamps_that_contradict_the_commands_own_two_ends_are_refused():
    # A stamp from before the command was launched cannot date a second the
    # command measured: the runner's clock and this run's are not one clock.
    with pytest.raises(ShapingError, match="contradict each other"):
        run_with_report(overheaded_report(), placements=[(0.3, 1_000_000)],
                        stamps=lambda honest: [stamp - 5.0 for stamp in honest])


def test_a_contended_window_whose_stream_was_not_read_live_is_refused():
    # A runner that collected the output whole cannot place the window, only
    # bound it — which is what the review found. The solo phases need no
    # placement; the contended one refuses to go without.
    engine = ScriptedEngine()

    class Collected(ScriptedCompetitor):
        def __call__(self, argv):
            code, out, _stamps = super().__call__(argv)
            return code, out, None

    box = Collected([9.0, 4.0, 9.0], during=cycles_inside_the_window(engine, [(0, 1_000)]))
    with contended(scripted_spec(), vtop_bytes=engine, run=box, log=lambda _m: None) as handle:
        box.handle = handle
        handle.start_with_vtop()
        wait_for(lambda: handle._collected is not None, "collected its report")
        with pytest.raises(ShapingError, match="no source stamp") as exc:
            handle.close()
    assert "runner bug" in str(exc.value)


def test_the_runner_stamps_each_line_as_it_arrives_rather_than_at_exit(tmp_path):
    # The stamps are only worth anything if they are taken as lines ARRIVE. A
    # runner that read the output at exit would stamp every line with one
    # instant, and the placement would silently become the latest opening again.
    script = tmp_path / "ticks.py"
    script.write_text(
        "import sys, time\n"
        "for i in range(3):\n"
        "    print(i, flush=True)\n"
        "    time.sleep(0.2)\n")
    code, out, stamps = competitor.stamped_command([sys.executable, str(script)], timeout=10)
    assert code == 0 and out.splitlines() == ["0", "1", "2"]
    gaps = [stamps[i + 1] - stamps[i] for i in range(len(stamps) - 1)]
    assert all(gap >= 0.15 for gap in gaps), (
        f"lines written 0.2s apart must be stamped about 0.2s apart, not {gaps}: stamps "
        "taken at exit would date every interval of the stream to the moment it ended")

    failing = tmp_path / "fails.py"
    failing.write_text("import sys\nprint('partial')\n"
                       "sys.stderr.write('iperf3: unrecognized option: json-stream\\n')\n"
                       "sys.exit(1)\n")
    code, out, stamps = competitor.stamped_command([sys.executable, str(failing)], timeout=10)
    assert code == 1 and "unrecognized option" in out and stamps is None, (
        "a failed command's stderr reaches the refusal — iperf3 says why it failed there — "
        "and it carries no stamps, because nothing will be placed from it")

    hangs = tmp_path / "hangs.py"
    hangs.write_text("import time\ntime.sleep(30)\n")
    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        competitor.stamped_command([sys.executable, str(hangs)], timeout=0.3)
    assert time.monotonic() - started < 10, (
        "and a command that outlives its timeout is killed, not waited on")


def test_an_iperf3_that_cannot_stream_its_report_is_refused_by_name():
    # netshoot's musl getopt prints the option without its dashes, so the
    # refusal is matched on the bare name. The generic "start the netem stack"
    # would be the wrong advice for a stack that is up with an old iperf3.
    old = ScriptedCompetitor([], refuse=(1, "iperf3: unrecognized option: json-stream\n"
                                            "Usage: iperf3 [-s|-c host] [options]"))
    with pytest.raises(ShapingError, match="does not support --json-stream") as exc:
        competitor.measure(spec(30), old, "alone before")
    assert "netshoot:v0.16" in str(exc.value), "and the refusal names the image that does"


def test_a_whole_document_report_is_refused_rather_than_read_without_placement():
    # The same numbers in a `-J` document carry no arrival to date them by, and
    # reading them would quietly bring back the bounded placement.
    document = json.dumps({"intervals": iperf_intervals([9.0] * 3),
                           "end": {"sum_received": {"bits_per_second": 8e6, "seconds": 3.0,
                                                    "bytes": 3_000_000}}})
    with pytest.raises(ShapingError, match="whole-document"):
        parse_report(document, spec(3), "with VTOP")


def test_a_policer_in_front_of_the_shared_queue_is_refused_whatever_its_rate():
    # The ingress hook polices BEFORE it redirects to the tbf. At or below the
    # bottleneck the policer is the real constraint and the flows never reach the
    # queue they are meant to share; above it, it still drops the bursts the tbf
    # would absorb — and the rate calibration removes it, so the run would be
    # labelled with a link it did not use (review).
    for policer in (5_000, 10_000, 20_000):
        with pytest.raises(ShapingError, match="shaping_policer_kbps") as exc:
            CompetitorSpec.from_scenario(
                contended_scenario(),
                NetemShape(latency_ms=100, bottleneck_kbps=10_000, buffer_bdp=1.0,
                           policer_kbps=policer, run_token="t"))
        assert str(policer) in str(exc.value), (
            "the refusal names the policer rate, so the scenario's author knows which key to drop")


def _system_metrics(rows):
    import os
    import tempfile
    directory = tempfile.mkdtemp()
    with open(os.path.join(directory, "system_metrics.csv"), "w") as handle:
        handle.write("timestamp,cpu_percent,memory_mb,disk_read_mb,disk_write_mb,"
                     "network_tx_mb,network_rx_mb\n")
        for row in rows:
            handle.write(row + "\n")
    return directory


def test_the_counter_baseline_is_read_at_the_boundary_not_off_the_last_earlier_row():
    # The last sample before the engine's window can be a whole sampling interval
    # stale; the solo phase's traffic in that gap was charged to the engine. A
    # reading taken AT the boundary is the baseline when one exists (review).
    from run_benchmark import _sys_summary

    directory = _system_metrics([
        "2026-01-01T00:00:28Z,0,10,0,480,0,0",   # 2 s before the boundary
        "2026-01-01T00:01:00Z,80,10,0,540,0,0",  # engine window
    ])
    bounded = _sys_summary(directory, since_iso="2026-01-01T00:00:30Z",
                           until_iso="2026-01-01T00:01:30Z",
                           counter_base={"disk_write_mb": 500.0})
    assert bounded["disk_write_mb"] == 40, (
        f"the engine wrote 40 MB after the boundary reading of 500; reporting "
        f"{bounded['disk_write_mb']} charges it with the 20 MB written between the last "
        "sample and the boundary")


def test_the_counter_delta_ends_at_the_closing_boundary_not_at_the_last_earlier_row():
    # Symmetric to the opening edge (review): with the rows after the window
    # filtered out, the delta used to end at the last sample BEFORE the closing
    # boundary, dropping up to one interval of the engine's own final traffic.
    from run_benchmark import _sys_summary

    directory = _system_metrics([
        "2026-01-01T00:00:31Z,80,10,0,510,0,0",
        "2026-01-01T00:01:28Z,80,10,0,530,0,0",  # 2 s before the closing boundary
        "2026-01-01T00:01:40Z,0,10,0,600,0,0",   # after it: the closing solo window
    ])
    bounded = _sys_summary(directory, since_iso="2026-01-01T00:00:30Z",
                           until_iso="2026-01-01T00:01:30Z",
                           counter_base={"disk_write_mb": 500.0},
                           counter_end={"disk_write_mb": 545.0})
    assert bounded["disk_write_mb"] == 45, (
        f"the engine wrote 45 MB between the two boundary readings; reporting "
        f"{bounded['disk_write_mb']} ends the delta at the last sample before the close and "
        "drops the engine's final writes")


def test_a_contended_run_withholds_the_host_wide_network_counters_rather_than_charge_the_engine():
    # psutil's network counters are host-wide, and the engine's window contains
    # the competing flow by design — no boundary reading can separate the two.
    # A number that is mostly the neighbour's must not sit in the engine's
    # columns as if it were the engine's (review). Unknown is not zero.
    from run_benchmark import _sys_summary

    directory = _system_metrics([
        "2026-01-01T00:01:00Z,80,10,0,0,575,5",  # 75 MB of it is the competitor
    ])
    contended = _sys_summary(directory, since_iso="2026-01-01T00:00:30Z",
                             until_iso="2026-01-01T00:01:30Z",
                             counter_base={"network_tx_mb": 500.0, "network_rx_mb": 0.0},
                             counter_end={"network_tx_mb": 580.0, "network_rx_mb": 5.0})
    assert contended["network_tx_mb"] == "" and contended["network_rx_mb"] == "", (
        f"a contended run reported network_tx_mb={contended['network_tx_mb']!r}: a figure "
        "dominated by the competing flow, filed as the engine's own traffic")
    assert "competing flow" in contended.get("network_counters_withheld", ""), (
        "the empty columns must say why they are empty, or they read as a sampler failure")
    uncontended = _sys_summary(directory)
    assert uncontended["network_tx_mb"] == 575, (
        "an uncontended run keeps its network columns exactly as before")


def test_the_monitor_reads_its_counters_on_demand_on_the_samples_own_basis():
    from lib.sysmon import SystemMonitor
    monitor = SystemMonitor(lambda _row: None, interval=60)
    reading = monitor.counters_now()
    assert set(reading) == {"disk_read_mb", "disk_write_mb", "network_tx_mb", "network_rx_mb"}, (
        "counters_now must return exactly the cumulative columns _sys_summary rebases")
    assert all(value >= 0 for value in reading.values()), (
        "the first reading sets the baseline, so no counter can start negative")


def test_the_runner_places_by_the_stamp_written_at_the_source_not_by_arrival(monkeypatch):
    # A delay common to every line — an overloaded or remote relay — moved every
    # arrival stamp by the same amount, invisible to the spread between lines
    # (review). The runner now stamps each line beside iperf3 and returns THOSE
    # stamps; the host's arrivals only prove the clock is shared.
    from lib import competitor as comp

    captured = {}
    relay_delay = 2.0  # seconds, the same for every line

    def fake_stamped_command(argv, timeout):
        captured["argv"] = argv
        written = (100.0, 101.0, 102.0)
        out = "\n".join(f"{t:.6f} {{\"event\":\"interval\",\"n\":{i}}}"
                        for i, t in enumerate(written))
        return 0, out, tuple(t + relay_delay for t in written)

    monkeypatch.setattr(comp, "stamped_command", fake_stamped_command)
    code, out, stamps = comp.docker_exec("probe", 30)(["iperf3", "-c", "netem"])
    assert code == 0
    assert stamps == (100.0, 101.0, 102.0), (
        f"the runner returned {stamps}: the relay's common delay leaked into the placement, "
        "which is the shift no comparison between lines can see")
    assert out.splitlines()[0] == '{"event":"interval","n":0}', (
        "the stamp prefix must be stripped before the stream is parsed")
    argv = captured["argv"]
    assert argv[:3] == ["docker", "exec", "probe"] and "pipefail" in argv[5], (
        f"the command must run through the in-container stamper under pipefail: {argv}")
    assert argv[-3:] == ["iperf3", "-c", "netem"] and comp.SOURCE_STAMPER in argv, (
        "the caller's command reaches the container unchanged, beside the stamper")


def test_a_source_stamp_later_than_its_arrival_is_refused_as_another_kernels_clock():
    from lib import competitor as comp

    with pytest.raises(ShapingError, match="another kernel"):
        comp.split_source_stamps("5000.000000 {}", (12.0,), "probe")
    with pytest.raises(ShapingError, match="no source stamp"):
        comp.split_source_stamps('{"event":"start"}', (12.0,), "probe")
    lines, stamps = comp.split_source_stamps("11.999000 {}", (12.0,), "probe")
    assert (lines, stamps) == ("{}", (11.999,)), (
        "a line written a millisecond before it was read is exactly what a shared clock looks like")
