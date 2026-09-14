"""The flow beside the upload: what VTOP costs its neighbour (#478).

Raising upload concurrency wins throughput on a long link for a mechanical
reason — loss-based TCP's share is per connection, so N connections take
roughly N times one flow's share of a shared bottleneck. The engine already
makes that decision: `batching.max_concurrent_batches` defaults to 8 and the
AIMD width controller grows it back on every cycle the store did not refuse.
A shaped WAN link never refuses. A thin pipe is a RATE, not a 429, so under
exactly the conditions this milestone measures the controller sees nothing to
back off from — and nothing in this repository measured the cost of that until
this module.

The number that authorises a concurrency or rate knob is therefore NOT VTOP's
goodput. It is the goodput of the flow sharing the bottleneck, measured with
VTOP running and again with the link to itself. That flow is plain TCP
(iperf3): two VTOP uploads sharing a link prove nothing about what a neighbour
experiences.

THREE PHASES, in this order, inside ONE installation of the shape:

    competitor alone -> competitor with VTOP -> competitor alone again

The bracket is the point. A single solo measurement cannot tell a bottleneck
that drifted mid-run from an engine that took bandwidth, so a drift would be
absorbed into the fairness claim and reported as VTOP's doing. Two solo
measurements that disagree by more than 10% mean the link was not the same
link throughout, and the run is REFUSED naming both numbers rather than
recorded — an unstable bottleneck cannot support a fairness claim. And the
three phases share one `tc` installation for the same reason: reinstalling the
shape between them would measure how reproducible `apply()` is, not how steady
the link was.

WHERE THE COMPETITOR RUNS. In the calibration probe's container
(`vtop-bench-netem-probe`), dialling the iperf3 server the middlebox already
runs on port 5201. That pair is not a convenience: the probe sits on the
engine-facing network, so its packets arrive on the same ingress hook as the
upload's, are policed by the same `tc police` action, are redirected onto the
same ifb, and queue in the same tbf. It is a flow in the engine's own
bottleneck queue, which is precisely what toxiproxy cannot offer — a
per-connection bandwidth toxic gives two flows no shared queue at all, and a
fairness index computed across one would be meaningless. `lib/shaping.py`
refuses that combination by name.

THE RECEIVED RATE, never the sent one — the same reasoning as
`netem.probe_throughput_mbps`, and it matters more here. At the far end of a
token bucket `sum_sent` counts bytes still sitting in the bottleneck's queue,
so a competitor measured that way reads high; a competitor that reads high
looks less harmed than it was, and the harm is the whole measurement. Unlike
the calibration probe this module does NOT fall back to `sum_sent` when the
receiver's view is missing: the fallback's error points in the direction that
exonerates VTOP, and a fairness number that fails safe must fail loudly. The
report's per-second `intervals` ARE the sender's, and they are read — but only
ever for the SHAPE of the window, never its size. See `competitor_bytes_over`.

Nothing here imports the engine. The command runner is injectable, exactly as
in `netem.docker_exec`, so the tests never need a container, an iperf3 or a
capability.
"""
from __future__ import annotations

import json
import shlex
import subprocess
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from . import netem
from .shaping import COMPETITOR_KEY, ShapingError, competitor_requested

# The iperf3 server the middlebox already runs (docker-compose.benchmark.yml,
# the `netem` service's command). Named here so a compose edit that drops it
# fails a test rather than a run, and dialled explicitly rather than by
# iperf3's default so the two files agree in writing.
IPERF_SERVER_PORT = 5201

# The competitor's rate modes. Exactly one today, and any other spelling is
# refused rather than treated as this one: a mode name that silently became
# `bulk` would file a run under a traffic model nobody ran. `bulk` is a
# congestion-controlled flow with no pacing — the one that actually competes
# for the queue, and the only model this issue needs (a web-like mix, an
# RTT-unfairness sweep and multi-flow convergence are worth doing later and
# decide nothing about whether a knob is safe).
COMPETITOR_MODES = ("bulk",)

# The ramp iperf3 omits from its own summary (`-O`). At a continental round
# trip most of the first couple of seconds is slow start, and a share measured
# across the ramp understates the flow that is ramping — for the solo phases
# that would understate the empty link, and for the contended phase it would
# understate whichever flow started later, which is always the competitor.
OMIT_SECONDS = 3

# The bounds on a window. Below the floor the omitted ramp is a large fraction
# of what is left and the number is mostly slow start; above the ceiling a
# single scenario spends more than half an hour on three windows, which is a
# mistake rather than an intent. Both are refused by name.
MIN_WINDOW_SECONDS = 10
MAX_WINDOW_SECONDS = 600

# How far the two solo measurements may sit from each other before the run is
# refused. The issue's band: a bottleneck that moved by more than this between
# the brackets was not one bottleneck, and the difference it produced cannot be
# told apart from the difference VTOP produced.
SOLO_AGREEMENT_TOLERANCE = 0.10

# How much of the window iperf3 must actually have measured. A flow cut short
# — a server that went away, a container that was killed — still emits a JSON
# report, and its rate would be a real number over a window nobody chose.
MIN_MEASURED_FRACTION = 0.9

# How far apart two of the report's own interval boundaries may sit and still
# count as MEETING rather than as a hole between them. iperf3 prints each
# interval's start, end and duration rounded to six decimals, and this module
# derives an entry's beginning as `end - seconds`, so two entries that are
# contiguous in the flow's own clock arrive a printed rounding — and then a
# float subtraction — away from each other. Compared exactly, every join in
# every real report reads as a hole and every run is refused. A millisecond is
# three orders of magnitude above that noise and three below the smallest hole
# it must never swallow: a hole is a MISSING ENTRY, and the shortest entry
# iperf3 emits is a tenth of a second (`-i 0.1`; this module asks for the
# default, a whole one).
STREAM_JOIN_TOLERANCE_SECONDS = 1e-3

# Slack over the window for the command to start, connect and be collected.
# The runner's own timeout is NOT netem's: `netem.COMMAND_TIMEOUT_SECONDS` is
# 40 s, right for a 13-second calibration probe and lethal to a 60-second
# competitor, which it would kill mid-flow and report as a middlebox that
# stopped answering.
COMMAND_SLACK_SECONDS = 30.0

# How much longer than that the collecting side waits. The two timeouts race
# otherwise, and the join wins by a hair — found live, with a link deliberately
# broken mid-flow: the command's own timeout had built the precise message
# ("the flow was cut off, so the window it was measuring is not the window that
# ran") and the join expired first, so what reached the operator was the vaguer
# "did not finish". The thread's diagnosis is the better one, so it is given
# time to arrive.
COLLECTION_GRACE_SECONDS = 10.0

# The flat columns, in the order the results files carry them. The index comes
# LAST and never travels alone: Jain's index is symmetric, so 0.61 says the two
# flows split the link unevenly and says nothing about which of them won. The
# per-flow numbers on its left are what answer that, and `metrics.py`,
# `run_matrix.py` and `summary.md` all splice this one tuple.
#
# The index's OWN TWO INPUTS sit immediately to its left (review). VTOP's share
# is a rate over the engine cycles the window wholly contained, so the
# competitor's whole-window average is not the number it may be compared
# against — that pairs a slice with an average and misstates the index by
# whatever the two spans differ by. `competitor_goodput_mbps_over_vtop_window`
# is the competitor over the SAME span, and it is what the index is built from.
# `competitor_goodput_mbps_with` stays: it is the honest answer to "what did the
# neighbour get", which is the whole minute and not the engine's slice of it.
COMPETITOR_COLUMNS = ("competitor_goodput_mbps_with", "competitor_goodput_mbps_without",
                      "competitor_goodput_mbps_over_vtop_window",
                      "vtop_goodput_mbps_with", "competitor_jain_index")


def blank_columns() -> dict[str, str]:
    """The competitor columns for a run that had none.

    Blank, never absent (#477's lesson): a column that appears only for the
    runs that set it makes a results directory two shapes of file, and the
    matrix fills a column it cannot find from the scenario.
    """
    return {column: "" for column in COMPETITOR_COLUMNS}


def jain_index(rates: list[float]) -> float:
    """Jain's fairness index over the flows sharing the bottleneck.

    (sum x)^2 / (n * sum x^2): 1.0 when every flow got the same share, 1/n when
    one flow took everything. Reported only ALONGSIDE the per-flow goodput,
    never instead of it — the index is symmetric, so it cannot say which flow
    won, and "0.6" is equally true of an engine that starved its neighbour and
    a neighbour that starved the engine.
    """
    total = sum(rates)
    squares = sum(rate * rate for rate in rates)
    if total <= 0 or squares <= 0:
        raise ShapingError(
            f"a fairness index over {rates} is undefined: no flow moved any data, so "
            "there was no bottleneck to share and nothing to be fair about")
    return round(total * total / (len(rates) * squares), 4)


def docker_exec(container: str, timeout: float) -> Callable[[list[str]], tuple[int, str]]:
    """Run a command in a lab container. Injectable so the tests never need a
    container or an iperf3.

    Its own copy rather than `netem.docker_exec` because of the timeout: that
    one is fixed at 40 s for a 13-second calibration probe, and a 60-second
    competitor under it would be killed mid-flow and reported as a container
    that stopped answering — a measurement error dressed as an infrastructure
    one.
    """

    def run(argv: list[str]) -> tuple[int, str]:
        try:
            proc = subprocess.run(["docker", "exec", container, *argv],
                                  capture_output=True, text=True, timeout=timeout)
        except FileNotFoundError as exc:
            raise ShapingError(
                "docker is not on PATH, so the competing flow cannot be started: a "
                "contended scenario needs the lab stack") from exc
        except subprocess.TimeoutExpired as exc:
            raise ShapingError(
                f"`{shlex.join(argv)}` in {container} did not finish in {timeout}s. The "
                "competing flow was cut off, so the window it was measuring is not the "
                "window that ran") from exc
        # stderr is JOINED, never dropped: iperf3 reports "unable to connect"
        # there, and a refusal that printed only an exit code would send an
        # operator looking at the wrong container.
        out = (proc.stdout or "") + (proc.stderr or "")
        return proc.returncode, out.strip()

    return run


Runner = Callable[[list[str]], tuple[int, str]]


@dataclass(frozen=True)
class CompetitorInterval:
    """One entry of the competitor's own per-second stream, on the window's clock.

    `start` and `end` are seconds from the beginning of the window iperf3
    MEASURED — the one its `end` summary describes, and the one `Contention`
    places on the run's own clock after the fact. Everything iperf3 does to its
    own clock around the ramp it omits is normalised away in
    `_measured_intervals`, once, so nothing downstream has to know about it.
    """

    start: float
    end: float
    bytes: int


@dataclass(frozen=True)
class CompetitorSample:
    """One iperf3 window, as iperf3 reported it.

    `seconds` is what the report says it measured, not what the spec asked for:
    the two differ when a flow is cut short, and only the first is a fact about
    this run.

    `intervals` is the same window cut into seconds. It is carried rather than
    discarded because the flow's average over the whole window is NOT the
    number Jain's index may be built from: VTOP's share is a rate over the
    engine cycles the window wholly contained, and the two are only comparable
    when both are taken over that one span.
    """

    mbps: float
    seconds: float
    bytes: int
    retransmits: int | None
    congestion: str
    intervals: tuple[CompetitorInterval, ...]

    def __post_init__(self) -> None:
        # The invariant every reader downstream leans on, made unforgeable
        # rather than left a convention. `parse_report` already refuses a report
        # with no stream, and this is what stops a sample built some other way
        # from reaching the span arithmetic — where the symptom would be a bare
        # "max() arg is an empty sequence" instead of a sentence.
        if not self.intervals:
            raise ShapingError(
                "a competitor sample was built without the interval stream its window was "
                "cut into, so nothing can say what the flow's rate was over the engine's "
                "own span. This is a runner bug, not a scenario one")

    def describe(self) -> dict[str, Any]:
        return {"goodput_mbps": self.mbps, "measured_seconds": self.seconds,
                "bytes": self.bytes, "retransmits": self.retransmits,
                "congestion_control": self.congestion,
                # How finely the window can be cut. A reader comparing the
                # index's span against this one knows whether the competitor's
                # rate over it rests on a whole stream or on two entries.
                "measured_intervals": len(self.intervals)}


@dataclass(frozen=True)
class CompetitorSpec:
    """The competing flow a scenario asks for, validated at load.

    Flat, like every other scenario knob, because the dependency-free fallback
    parser is flat by design:

        shaping_competitor: bulk:60      # <rate mode>:<seconds per window>

    Three windows of that length run per contended scenario, so the key costs
    roughly 3 * (seconds + the omitted ramp) of wall clock.
    """

    mode: str
    seconds: int
    omit: int = OMIT_SECONDS
    server: str = netem.NETEM_SERVICE
    port: int = IPERF_SERVER_PORT
    container: str = netem.NETEM_PROBE_CONTAINER

    @classmethod
    def from_scenario(cls, scenario, shape=None) -> CompetitorSpec | None:
        """The competitor this scenario wants, or None for a run with none.

        Validated when the scenario is LOADED, like the shape it needs, so a
        bad knob fails before the seed data exists rather than after a soak.
        """
        # "Did the author set this" is asked against the schema default, never
        # against emptiness: the loader stamps every scenario with all of
        # DEFAULTS, and the same test written as a truthiness check has twice
        # read a key's non-empty default as an author's choice. This key's
        # default happens to be "", and the rule is followed anyway — the trap
        # is in the habit, not in the value.
        if not competitor_requested(scenario):
            return None
        spec = cls._parse(str(scenario.get(COMPETITOR_KEY, "")).strip())
        _require_a_shared_queue(shape)
        _require_the_engine_outlasts_the_window(spec, scenario)
        return spec

    @classmethod
    def _parse(cls, raw: str) -> CompetitorSpec:
        mode, sep, window = raw.partition(":")
        mode, window = mode.strip(), window.strip()
        if not sep or not mode or not window:
            raise ValueError(
                f"{COMPETITOR_KEY} {raw!r} is not <mode>:<seconds> — e.g. `bulk:60`. The "
                "duration is spelled out rather than defaulted because it is charged to "
                "the run three times, once per phase")
        if mode not in COMPETITOR_MODES:
            raise ValueError(
                f"{COMPETITOR_KEY} mode {mode!r} is not one of {list(COMPETITOR_MODES)}. A "
                "mode this harness does not implement is refused rather than run as "
                "`bulk`: the recorded fairness number would name a traffic model that "
                "never existed")
        try:
            seconds = int(window)
        except ValueError as exc:
            raise ValueError(
                f"{COMPETITOR_KEY} window {window!r} is not a whole number of seconds") from exc
        if not MIN_WINDOW_SECONDS <= seconds <= MAX_WINDOW_SECONDS:
            raise ValueError(
                f"{COMPETITOR_KEY} window {seconds}s is outside "
                f"{MIN_WINDOW_SECONDS}-{MAX_WINDOW_SECONDS}s. Below the floor the omitted "
                f"{OMIT_SECONDS}s ramp is most of the measurement; above the ceiling one "
                "scenario spends half an hour on three windows")
        return cls(mode=mode, seconds=seconds)

    # ------------------------------------------------------------ the command

    def argv(self) -> list[str]:
        """The iperf3 client command, as data, so a test can read it.

        `-O` omits the ramp from iperf3's own summary; `-J` is the report this
        module parses. No `-b`: a `bulk` competitor is congestion-controlled,
        because a paced flow cannot lose a share it never asked for and its
        goodput would say nothing about fairness.
        """
        return ["iperf3", "-c", self.server, "-p", str(self.port),
                "-t", str(self.seconds), "-O", str(self.omit), "-J"]

    def command_timeout(self) -> float:
        return self.seconds + self.omit + COMMAND_SLACK_SECONDS

    def collection_timeout(self) -> float:
        """How long the collecting side waits for the flow's own thread.

        Strictly longer than the command's timeout, so the runner inside the
        thread always reaches its own conclusion first and the operator gets
        the specific message instead of this side's generic one.
        """
        return self.command_timeout() + COLLECTION_GRACE_SECONDS

    def wall_seconds(self) -> int:
        """What one phase costs in wall clock: iperf3 runs the omitted ramp
        BEFORE it starts the clock on `-t`, so a window is the sum of both."""
        return self.seconds + self.omit

    def describe(self) -> dict[str, Any]:
        return {"mode": self.mode, "window_seconds": self.seconds,
                "omit_seconds": self.omit, "phase_wall_seconds": self.wall_seconds(),
                "server": f"{self.server}:{self.port}", "container": self.container}


def _require_a_shared_queue(shape) -> None:
    """A fairness number needs a queue the two flows actually share.

    The driver check lives in `lib/shaping.py` — toxiproxy shapes each
    connection separately, so there is no queue at all. This is the same rule
    one level in: a netem shape with no `shaping_bottleneck_kbps` installs no
    tbf, so the two flows meet on whatever the host's veth pair happens to do
    that afternoon, and the index would measure the machine. A policer is not
    a substitute either — it drops above its rate and holds nothing, so there
    is still nothing to share.
    """
    if shape is None:
        raise ShapingError(
            f"{COMPETITOR_KEY} is set but this scenario is unshaped: two flows on an "
            "unshaped path share no bottleneck, so the fairness number would describe "
            "the host's own link on the day it ran. Shape the run with the netem "
            "middlebox (shaping_driver: netem, shaping_bottleneck_kbps, "
            "shaping_buffer_bdp)")
    if not getattr(shape, "bottleneck_kbps", 0):
        raise ShapingError(
            f"{COMPETITOR_KEY} needs a shaping_bottleneck_kbps: without a tbf there is no "
            "queue for the two flows to share, and a fairness index over a link that is "
            "not the constraint measures the host rather than the engine. A policer does "
            "not count — it drops above its rate and holds no queue")


def _require_the_engine_outlasts_the_window(spec: CompetitorSpec, scenario) -> None:
    """The with-VTOP window must fit inside a run the engine is still working.

    A competitor window that outlives the engine's own loop is a solo
    measurement wearing the contended label: the tail of it has no VTOP in it
    at all. `Contention.close()` catches that live, from what actually
    happened; this catches the arithmetic before a seed byte is written, which
    is where a scenario file's mistake belongs.
    """
    duration = float(scenario.get("duration_seconds", 0) or 0)
    if duration < spec.wall_seconds():
        raise ShapingError(
            f"{COMPETITOR_KEY} asks for a {spec.seconds}s window (plus a {spec.omit}s "
            f"ramp, so {spec.wall_seconds()}s of flow) but duration_seconds is "
            f"{duration:g}: the engine's loop would end while the competitor was still "
            "running, and the tail of the contended measurement would be a solo one "
            "under a contended name. Lengthen duration_seconds or shorten the window")


# ------------------------------------------------------------------ measuring


def _measured_intervals(report: Any, out: str, spec: CompetitorSpec, phase: str,
                        measured_seconds: float) -> tuple[CompetitorInterval, ...]:
    """The report's per-second stream, ramp excluded, on the window's own clock.

    WHY THE STREAM IS READ AT ALL. VTOP's share is charged cycle by cycle, so it
    is a rate over the span of the cycles the window wholly contained — nested
    inside the window, not equal to it. Comparing that against the competitor's
    average over the WHOLE window is comparing a slice with an average, and
    Jain's index is then wrong by however much the flow's rate moved during the
    minute (review). The stream is what lets the competitor be measured over the
    same span instead.

    THREE THINGS A CAPTURED REPORT SETTLES, none of which are safe to assume
    (iperf 3.16, `-O 3 -t 6`, run through this module's own argv):

      * the omitted ramp's seconds ARE in the stream, flagged `omitted`. Left
        in, they map onto the measured window's clock a second time and charge
        its opening seconds with the ramp's bytes — the off-by-the-ramp error a
        previous round fixed at the harness's end, reappearing at this one.
      * iperf3 restarts the report's clock when the ramp ends, so the measured
        entries begin at zero again. Trusting `start` would therefore be off by
        the whole ramp against an iperf3 that did NOT restart it, so the origin
        is DERIVED: the last measured entry ends where the window ends, and the
        receiver's summary says how long the window was.
      * the first measured entry straddles that restart. Its `seconds` reaches
        back across the ramp while its `bytes` are the post-restart ones only,
        so iperf3's own `bits_per_second` for it reads at half the flow's actual
        rate. Its span is clamped to the window's start, which is where those
        bytes belong, and the entry's `bits_per_second` is never read.
    """
    raw = report.get("intervals") if isinstance(report, dict) else None
    if not isinstance(raw, list) or not raw:
        raise ShapingError(
            f"the {phase} competitor's iperf3 report carries no interval stream "
            f"(`intervals`): {out[:400]}. Without it the competing flow has only a "
            f"whole-window average over {measured_seconds}s, and VTOP's share is a rate "
            "over the engine cycles inside that window — a fairness index assembled from "
            "the two would be comparing rates over two different spans. Run the lab's own "
            "iperf3 (the netshoot middlebox), not a build or a wrapper that strips the "
            "stream out of the report")
    reported: list[tuple[float, float, int]] = []
    try:
        for entry in raw:
            summary = entry["sum"]
            if summary.get("omitted"):
                continue
            reported.append((float(summary["end"]), float(summary["seconds"]),
                             int(summary["bytes"])))
    except (KeyError, TypeError, ValueError) as exc:
        raise ShapingError(
            f"the {phase} competitor's iperf3 report has an interval this module cannot "
            f"read — each needs sum.end, sum.seconds and sum.bytes: {out[:400]}. A "
            "malformed entry is refused rather than skipped over: skipping leaves a hole "
            "in the stream, and the receiver's bytes would then be spread across a window "
            "the stream no longer describes"
        ) from exc
    if not reported:
        raise ShapingError(
            f"every interval in the {phase} competitor's report is flagged omitted, so the "
            f"report describes its {spec.omit}s ramp and nothing else: {out[:400]}. There "
            "is no measured stream to place the window's bytes in time with, and VTOP's "
            "share of the window cannot be compared against a flow the report will not "
            "resolve past its own ramp")
    # The origin the whole mapping hangs on. The stream's last measured entry
    # ends where the window ends, and the receiver's summary gives the window's
    # length, so the difference is where the stream's clock puts the window's
    # start — zero on an iperf3 that restarts it, the ramp on one that does not.
    origin = max(end for end, _seconds, _count in reported) - measured_seconds
    stream = []
    for end, seconds, count in reported:
        finish = end - origin
        begin = max(0.0, finish - seconds)
        if finish - begin <= 0:
            # An entry with no duration cannot be placed on a clock at all.
            # iperf3 emits one only as a rounding artifact at the very end of a
            # window, carrying nothing worth spreading, and the magnitude comes
            # from the receiver's total either way — so dropping it moves the
            # shape by nothing and leaves every remaining span divisible.
            continue
        stream.append(CompetitorInterval(begin, finish, count))
    if not stream:
        raise ShapingError(
            f"no interval in the {phase} competitor's report has any duration: {out[:400]}. "
            "A stream of instants cannot say how the flow's bytes were spread across its "
            "window, so there is nothing to measure VTOP's own span against")
    return tuple(stream)


def competitor_bytes_over(sample: CompetitorSample, opened_at: float,
                          span_from: float, span_to: float) -> tuple[int, int]:
    """What the competitor RECEIVED during one span of the monotonic clock.

    THE MAGNITUDE IS THE RECEIVER'S AND ONLY THE SHAPE IS THE STREAM'S. iperf3's
    interval entries are the client's sending side — they add up to `sum_sent`,
    never to `sum_received` — and this module refuses the sender's view
    everywhere else for one reason: at the far end of a token bucket it counts
    bytes still sitting in the queue, so it reads high, and a competitor that
    reads high looks less harmed than it was. Reading the stream literally would
    put exactly that flattering error into the headline index. So the stream is
    asked only HOW the flow's bytes were distributed across its window, and the
    receiver's own total says how many there were. The single assumption is that
    the sender-to-receiver gap — one bottleneck queue's worth — is spread in
    proportion to the flow's own rate, which is what a tbf holding a roughly
    constant queue does.

    A partly covered interval is PRORATED, which a straddling engine cycle is
    not, and the difference is not a double standard: an interval IS a rate
    measurement over its own second, so a fraction of it is that measured rate
    over a fraction of a second. The engine's committed-byte counter says
    nothing whatever about the interior of a `process_once`, which is why a
    cycle crossing an edge is charged to neither side instead.

    THE OVERLAPS HAVE TO COVER THE SPAN, not merely touch it (review). A stream
    with a hole under the span still overlaps it at both ends — intervals at
    0-1s and 5-6s "touch" a span of 0.5-5.5s — and the seconds in between are
    then treated as seconds in which the competing flow received nothing, while
    the rate's denominator still covers all five. The flow comes out understated
    and VTOP comes out fairer, which is the direction this measurement must
    never fail in, so a gap is refused rather than filled in with a zero.

    Returns the received bytes and how many intervals the span touched.
    """
    stream_ends = max(slot.end for slot in sample.intervals)
    total = 0.0
    inside = 0.0
    overlaps: list[tuple[float, float]] = []
    for slot in sample.intervals:
        total += slot.bytes
        begins = max(opened_at + slot.start, span_from)
        finishes = min(opened_at + slot.end, span_to)
        if finishes - begins <= 0:
            continue
        overlaps.append((begins, finishes))
        inside += slot.bytes * min(1.0, (finishes - begins) / (slot.end - slot.start))
    if not overlaps:
        raise ShapingError(
            f"no interval of the competing flow's report covers the {span_to - span_from:.3f}s "
            f"span VTOP's share was measured over, which begins {span_from - opened_at:.3f}s "
            f"after the window opened; the report's own stream runs 0-{stream_ends:.3f}s. The "
            "competitor's rate over that span would have to be invented, and a fairness "
            "index means nothing unless both of its rates cover one interval. This is a "
            "report with a hole in its stream, not a scenario knob")
    # Walked in order, and a hole is the first entry that begins after the
    # stretch already covered ends. Sorted rather than assumed sorted: the
    # entries come from a JSON document, and an out-of-order one would
    # otherwise read as a hole followed by an overlap.
    reached = span_from
    hole: tuple[float, float] | None = None
    for begins, finishes in sorted(overlaps):
        if begins > reached + STREAM_JOIN_TOLERANCE_SECONDS:
            hole = (reached, begins)
            break
        reached = max(reached, finishes)
    if hole is None and reached < span_to - STREAM_JOIN_TOLERANCE_SECONDS:
        hole = (reached, span_to)
    if hole is not None:
        raise ShapingError(
            f"the competing flow's report says nothing about {hole[1] - hole[0]:.3f}s of the "
            f"{span_to - span_from:.3f}s span VTOP's share was measured over: the span runs "
            f"{span_from - opened_at:.3f}-{span_to - opened_at:.3f}s after the window opened "
            f"and the first stretch of it the report's own stream cannot speak for runs "
            f"{hole[0] - opened_at:.3f}-{hole[1] - opened_at:.3f}s (the stream itself runs "
            f"0-{stream_ends:.3f}s, in {len(sample.intervals)} entries). Overlapping the span "
            "at both ends is not covering it: spreading the receiver's total over only the "
            "parts the stream kept, while the rate's denominator still covers the whole span, "
            "records the competing flow as having received less than it did — less harmed "
            "than it was, the one direction this measurement must never fail in — and the "
            "fairness index flatters VTOP by exactly that much. Rerun with the lab's own "
            "iperf3 (the netshoot middlebox) and check the middlebox was not starved of CPU "
            "while it was reporting; this is a report with a hole in it, not a scenario knob")
    if total <= 0:
        raise ShapingError(
            f"the competing flow's report says it received {sample.bytes} byte(s) while "
            "every interval of its own stream carries none: there is no shape to spread "
            "that total across, so its rate over the span VTOP was measured on cannot be "
            "derived. The report contradicts itself and is refused rather than averaged")
    return int(round(sample.bytes * inside / total)), len(overlaps)


def parse_report(out: str, spec: CompetitorSpec, phase: str) -> CompetitorSample:
    """One `iperf3 --json` report as a sample, or a refusal naming what it saw.

    `sum_received` only. `sum_sent` counts what the client handed to its socket,
    which at the far end of a token bucket includes everything still queued —
    a 100 ms buffer on a 10 Mbit/s link is 125 kB the sender has "sent" and the
    link has not carried. How much that inflates the rate depends on the
    window, since it is one queue's worth divided by it (measured at about 1%
    over ten seconds on the lab's one-BDP link, and four times that on the
    four-BDP one); the DIRECTION never changes, and it is the direction that
    makes the engine look innocent. Goodput is what arrived, and the neighbour
    only ever experienced what arrived.
    """
    try:
        report = json.loads(out)
    except ValueError as exc:
        raise ShapingError(
            f"the {phase} competitor's output was not an iperf3 JSON report: {out[:400]}"
        ) from exc
    if isinstance(report, dict) and report.get("error"):
        raise ShapingError(
            f"iperf3 refused the {phase} competitor: {report['error']}")
    try:
        end = report["end"]
        summary = end["sum_received"]
        mbps = round(float(summary["bits_per_second"]) / 1e6, 3)
        seconds = round(float(summary["seconds"]), 3)
        transferred = int(summary["bytes"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ShapingError(
            f"the {phase} competitor's iperf3 report carries no receiver summary "
            f"(end.sum_received): {out[:400]}. The sender's own count is not read as a "
            "substitute — it includes bytes still sitting in the bottleneck's queue, so "
            "it would report the competing flow as less harmed than it was"
        ) from exc
    if seconds < spec.seconds * MIN_MEASURED_FRACTION:
        raise ShapingError(
            f"the {phase} competitor measured only {seconds}s of the {spec.seconds}s "
            f"window it was given: the flow was cut short, and its {mbps} Mbit/s is a "
            "real rate over a window nobody chose")
    # LAST, deliberately: a report that is missing its receiver summary, or that
    # describes a flow cut short, has already been refused by the sentence that
    # names what is actually wrong with it. Reading the stream first would
    # replace those diagnoses with a complaint about `intervals` and send the
    # operator after the wrong fault. Read on EVERY phase rather than only the
    # contended one, so an iperf3 whose report this module cannot use is found
    # by the first solo window instead of after three of them have run.
    intervals = _measured_intervals(report, out, spec, phase, seconds)
    sent = end.get("sum_sent") or {}
    return CompetitorSample(
        mbps=mbps, seconds=seconds, bytes=transferred,
        # Linux iperf3 reports the sender's retransmissions; other platforms
        # and other modes do not, and None records "not reported" rather than
        # a zero that reads as "none happened".
        retransmits=sent.get("retransmits") if isinstance(sent.get("retransmits"), int) else None,
        # WHICH control law actually ran, when iperf3 says so. The competitor's
        # share depends on it, so a recorded fairness number that did not name
        # it could not be reproduced.
        congestion=str(end.get("sender_tcp_congestion", "") or ""),
        intervals=intervals)


def measure(spec: CompetitorSpec, run: Runner, phase: str) -> CompetitorSample:
    """Run one window and return what it measured. Raises if it could not run.

    A competitor that cannot be started fails the RUN (#478): a contended
    scenario whose competitor never started is an uncontended scenario, and
    filing it under the contended name is the failure this harness spends most
    of its refusals preventing.
    """
    code, out = run(spec.argv())
    if code != 0:
        raise ShapingError(
            f"the {phase} competitor could not be started in {spec.container} "
            f"(`{shlex.join(spec.argv())}` exited {code}): {out[:400]}. Start the netem "
            "stack — docker compose -f benchmarks/docker-compose.benchmark.yml "
            "--profile netem up -d — and check the middlebox's iperf3 server is up")
    return parse_report(out, spec, phase)


def solo_agreement(before: float, after: float) -> float:
    """How far the two bracketing solo measurements sit apart, as a fraction.

    Relative to their MEAN, so the answer does not depend on which of the two
    is called the baseline — a difference measured against the larger and a
    difference measured against the smaller are two different numbers, and a
    threshold that changed with the ordering would pass or fail on the order
    the phases happened to run in.
    """
    mean = (before + after) / 2.0
    if mean <= 0:
        raise ShapingError(
            f"both solo competitor windows measured nothing ({before} and {after} "
            "Mbit/s): the competing flow moved no data on an idle link, so there is no "
            "baseline for the contended window to be compared against")
    return abs(before - after) / mean


@dataclass(frozen=True)
class EngineCycle:
    """One `process_once` call: the interval it occupied and the bytes it added.

    An INTERVAL rather than an instant, because the engine's committed-byte
    total is not a live counter. It moves in one step when a whole cycle
    returns, and the bytes in that step crossed the link at some unknown moment
    between the call starting and returning — so the only thing the run knows
    about them is that they belong somewhere in `[started_at, ended_at]`.

    That is why a cycle is the smallest unit the contended window can be
    charged with, and why one that straddles either edge of the window is
    charged to neither side: splitting it would mean inventing the moment each
    byte left, and the invented split would land in `vtop_goodput_mbps_with`
    and in Jain's index as though it had been measured.
    """

    started_at: float
    ended_at: float
    committed_bytes: int


class Contention:
    """One contended run's three phases, and the numbers they produced.

    Held as an object rather than returned as a tuple because the middle phase
    is CONCURRENT: it is started by the runner once the engine has work to do,
    it runs beside the engine, and it is collected when the engine's block
    ends. Every refusal below exists because the alternative is a fairness
    number that nothing produced.
    """

    def __init__(self, spec: CompetitorSpec, run: Runner,
                 vtop_bytes: Callable[[], int],
                 log: Callable[[str], None] = print) -> None:
        self.spec = spec
        self._run = run
        self._vtop_bytes = vtop_bytes
        self._log = log
        self.before: CompetitorSample | None = None
        self.after: CompetitorSample | None = None
        self.contended: CompetitorSample | None = None
        self.vtop_mbps: float | None = None
        self.vtop_bytes_in_window: int = 0
        self.vtop_window_seconds: float = 0.0
        # How many engine cycles the window could actually be charged with, and
        # what the engine's total moved by across the window's two edges
        # regardless of attribution. The second is recorded and never divided:
        # the gap between the two is how much of the engine's movement could
        # not be placed, which is the number that tells an operator whether the
        # window is long enough for this engine's cycle.
        self.vtop_attributed_cycles: int = 0
        self.vtop_bytes_across_window: int = 0
        # The flow's wall clock its own report cannot account for: docker's
        # startup and the TCP connect before iperf3 began measuring, its closing
        # exchange and exit after it stopped. The run sees the sum and cannot
        # see how it split, so this is exactly how loosely the measured window
        # is pinned to the run's clock — and how much shorter the span anything
        # may be attributed to is than the window itself. See `close()`.
        self.window_unaccounted_seconds: float = 0.0
        # The competitor over THAT SAME span, from its own interval stream
        # (review). The index's two inputs have to be rates over one interval,
        # and the flow's whole-window average is not one of them: it is an
        # average over the minute, and VTOP's is a rate over the cycles inside
        # it. Recorded beside the average rather than instead of it.
        self.competitor_mbps_in_vtop_window: float | None = None
        self.competitor_bytes_in_vtop_window: int = 0
        self.competitor_intervals_in_vtop_window: int = 0
        self.jain: float | None = None
        self.solo_after_seconds: float = 0.0
        # Whether the block reached its own end and closed the bracket. The
        # context manager reads it on the way out: anything that did NOT close
        # is a run that stopped for its own reason, and its exit must collect
        # the flow without measuring or judging anything further.
        self.closed = False
        self._thread: threading.Thread | None = None
        self._outcome: list[Any] = []
        # The flow's own two clock reads, taken on its own thread: when its
        # command was launched, and when the report was back with the engine's
        # committed total beside it. Between them lies every instant the
        # measured window could possibly occupy, which is what lets `close()`
        # place that window without anything having sampled it live.
        self._launched_at: float | None = None
        self._collected: tuple[float, int] | None = None
        # Where `close()` decided the measured window sat, once the report was
        # in hand. Kept so the decision can be read rather than re-derived.
        self._window: tuple[float, float] | None = None
        # The engine's cycles as intervals, with the bytes each one added — see
        # `note_progress`. Intervals rather than samples, because a total that
        # only moves when a whole call returns cannot be attributed to a window
        # any finer than the call itself.
        self._cycles: list[EngineCycle] = []
        # The committed total as of the last cycle counted, so each cycle
        # contributes a DIFFERENCE and not the whole counter. Re-read when the
        # contended window is opened; see `start_with_vtop`.
        self._counted_through: int = 0
        # The same read, kept: it is the counter's floor for the whole block,
        # and `close()` needs it to say what the counter stood at when the
        # window opened — an instant nothing can sample live, because it is
        # only known once the report is in hand.
        self._opened_from: int = 0

    # ------------------------------------------------------------ phase one

    def measure_alone(self, phase: str) -> CompetitorSample:
        sample = measure(self.spec, self._run, phase)
        self._log(f"[bench] competitor {phase}: {sample.mbps} Mbit/s over "
                  f"{sample.seconds}s on the empty link")
        return sample

    def open_alone(self) -> None:
        """The first bracket, on the way into the block: the empty link, on
        the shape the contended window will run on."""
        self.before = self.measure_alone("alone before")

    # ------------------------------------------------------------ phase two

    def start_with_vtop(self) -> None:
        """Open the contended window, beside a running engine.

        Called by the runner AFTER the seed exists, so the window covers the
        engine at work rather than the harness writing files: a competitor that
        started first would spend its opening seconds measuring an empty link
        and record the average as contended.

        NOTHING HERE SAMPLES THE OPENING BOUNDARY (review). Where the window
        iperf3 measured sat on this run's clock is worked out in `close()`, from
        the report and from the flow's own two clock reads. It used to be
        sampled live, by a second thread that slept the omitted ramp and called
        the instant it woke the opening boundary — and that thread began its
        sleep before `docker exec` had spawned anything, so docker's startup,
        the DNS lookup and the TCP connect all elapsed inside the sleep and the
        boundary landed BEFORE iperf3 started measuring by however long the
        setup took. Engine cycles from the ramp were then paired with the
        competitor's post-ramp seconds, which is the one error the whole span
        apparatus exists to prevent. Nothing needed that sample anyway: every
        cycle carries its own two clock reads, so a window worked out afterwards
        can be applied to the cycle log afterwards.
        """
        if self._thread is not None:
            raise ShapingError("the contended window was already opened; one run, one window")

        # THE FLOOR EVERY CYCLE'S CONTRIBUTION IS MEASURED FROM. Taken here
        # rather than in __init__ because the runner opens the window as the
        # last thing before the engine's loop: nothing can move the total
        # between this line and the first `process_once`, so the first cycle's
        # difference is that cycle's own bytes rather than the whole run's.
        self._counted_through = self._vtop_bytes()
        self._opened_from = self._counted_through

        def _flow() -> None:
            # THE FIRST OF THE TWO FACTS THAT PIN THE WINDOW: iperf3 cannot have
            # begun measuring before the command that runs it was launched, let
            # alone before the ramp it omits had elapsed. Read on this thread
            # rather than before `start()`, so a thread the scheduler got to
            # late does not widen the window by its own late start.
            self._launched_at = time.monotonic()
            try:
                self._outcome.append(measure(self.spec, self._run, "with VTOP"))
            except BaseException as exc:  # noqa: BLE001 - re-raised in close()
                self._outcome.append(exc)
            finally:
                # AND THE SECOND: the report is in hand, so the window it
                # describes has closed. The engine's counter is read at the same
                # instant rather than when close() eventually runs — close() is
                # called after the whole engine loop, and the bundled scenario
                # runs 150s around a 60s window, so a counter read there would
                # describe an interval the competitor was never measured over.
                # In the `finally` so a flow that raised still records where it
                # got to.
                self._collected = (time.monotonic(), self._vtop_bytes())

        self._thread = threading.Thread(target=_flow, name="competitor", daemon=True)
        self._thread.start()
        self._log(f"[bench] competitor beside VTOP: {self.spec.mode} for "
                  f"{self.spec.seconds}s (+{self.spec.omit}s ramp)")

    def note_progress(self, started_at: float) -> None:
        """Record the cycle that has just returned: its interval, and its bytes.

        `started_at` is the monotonic clock read the runner took BEFORE the
        `process_once` call that is now returning, and both halves of the
        interval are load-bearing (review). An earlier round noted only the
        moment a cycle returned and refused a window in which nothing returned
        at all — but the engine's total still moves one WHOLE cycle at a time,
        so any cycle that crosses an edge of the window mis-attributes every
        byte it carries: one that began during iperf3's omitted ramp and
        returned inside the window hands the window uploads that happened
        before the window existed, and one that uploaded inside the window but
        returned after it closed hands it none of them. A second cycle
        returning in between made the old guard pass and changed neither error.

        So the interval is what is recorded, and `close()` charges the window
        only with cycles it wholly contains. Cheap by construction — the
        caller's clock read, one clock read here, one int — so it costs a
        contended run nothing to call after every cycle, which is exactly how
        often it must be called: each cycle's bytes are a difference against
        the last one counted, and a skipped call folds two cycles into one
        interval that is then likely to straddle an edge and be dropped.
        """
        ended_at = time.monotonic()
        total = self._vtop_bytes()
        # Clamped only because a negative difference would become a negative
        # goodput and an index computed over nonsense. The counter is a
        # monotonic sum of committed batches, so this can fire on a runner bug
        # and on nothing else.
        self._cycles.append(
            EngineCycle(started_at, ended_at, max(0, total - self._counted_through)))
        self._counted_through = total

    # ---------------------------------------------------------- phase three

    def abandon(self) -> None:
        """The block did not close the bracket: it raised, refused, or returned
        early. Collect the flow and measure nothing more.

        A second solo window on the way out of a stopped run would take a
        minute and could then raise a refusal of its own — and the reason the
        run stopped is the one worth reporting. The thread is still joined: a
        docker exec outliving the run would put a competing flow across the
        NEXT one.
        """
        if self._thread is None:
            return
        self._thread.join(timeout=self.spec.collection_timeout())
        if self._thread.is_alive():
            self._log("[bench] WARNING: the competing flow did not stop; it may still be "
                      f"crossing the middlebox — docker exec {self.spec.container} pkill iperf3")

    def close(self) -> None:
        """Collect the contended window, measure the link alone again, judge.

        Called by the block, as its last act inside the shaped region — not by
        the context manager's exit, which cannot tell a block that finished
        from one that gave up.

        Raises rather than records wherever the three numbers cannot support a
        fairness claim: a competitor that never started, a contended window
        that outlived the engine's own loop, or two solo windows that disagree.
        """
        if self._thread is None:
            raise ShapingError(
                f"{COMPETITOR_KEY} is set but the contended window was never opened: the "
                "run would record a competitor's goodput 'with VTOP' that no flow ever "
                "measured. This is a runner bug, not a scenario one")
        # STILL RUNNING means the engine's loop ended first, so the tail of the
        # contended window had no VTOP in it. Joined first either way — the
        # flow must not outlive the run — and the overshoot is measured from
        # the join rather than assumed, so the refusal says what it saw.
        closed_at = time.monotonic()
        overran = self._thread.is_alive()
        self._thread.join(timeout=self.spec.collection_timeout())
        if self._thread.is_alive():
            raise ShapingError(
                f"the competing flow in {self.spec.container} did not finish within "
                f"{self.spec.collection_timeout()}s of its window; the run cannot be "
                "recorded with a number nobody collected")
        if not self._outcome:
            raise ShapingError(
                "the competing flow's thread finished without reporting anything, which "
                "is a runner bug: a fairness number must not be assembled around a "
                "window whose result nobody holds")
        # THE FLOW'S OWN FAILURE FIRST, and the overrun after it (found live):
        # a flow that could not run at all also finishes late, and reporting
        # that as "lengthen duration_seconds" sends the operator to change a
        # scenario knob when the middlebox is what is broken. A flow that ran
        # fine and merely outlived the engine's loop has no exception to
        # report, so the ordering costs that case nothing.
        if isinstance(self._outcome[0], BaseException):
            raise self._outcome[0]
        if overran:
            raise ShapingError(
                f"the engine's window closed {time.monotonic() - closed_at:.1f}s before the "
                f"competitor's did: the last part of the {self.spec.seconds}s contended "
                "window had no VTOP traffic in it, so its goodput is partly a solo "
                "measurement under a contended name. Lengthen duration_seconds, or "
                f"shorten {COMPETITOR_KEY}'s window")
        self.contended = self._outcome[0]

        # Read AFTER the join, because the flow takes both of these on its own
        # thread: before the join they may legitimately not exist yet.
        if self._launched_at is None or self._collected is None:
            raise ShapingError(
                "the competing flow's thread finished without recording when it launched "
                "its command or when the report came back, so the window that report "
                "describes cannot be placed on this run's clock. VTOP's share would then be "
                "charged to a span nobody located. This is a runner bug")
        launched_at = self._launched_at
        collected_at, closing_bytes = self._collected
        measured = self.contended.seconds

        # WHERE THE MEASURED WINDOW SAT ON THIS RUN'S CLOCK (review). Derived
        # from the report now that it is in hand, never sampled live beside the
        # flow. The report says how LONG the window it measured was, and two
        # facts from the run say where a window of that length can sit:
        #
        #   * it did not open before `launched_at + omit` — the command had not
        #     been launched before `launched_at`, and iperf3 runs the ramp it
        #     omits before it starts measuring;
        #   * it had closed by `collected_at` — the report was already back
        #     through docker exec by then.
        #
        # The wall clock those two leave over is the setup at one end and
        # iperf3's closing exchange and exit at the other, and the run sees only
        # their sum: nothing here can tell how it split. So the span anything
        # may be attributed to is what EVERY admissible position of the window
        # contains — the intersection of the earliest window the run allows and
        # the latest. It is shorter than the measured window by that unaccounted
        # overhead, and in exchange every instant of it was inside the window
        # the competitor's own summary describes, whichever way the overhead
        # actually fell. The sleeping timer this replaced bought the opposite:
        # a boundary that moved with docker's startup and landed before iperf3
        # had begun measuring.
        #
        # The collection tail needs no clamp of its own under this arithmetic —
        # it is part of the same overhead, and the far bound is the EARLIEST the
        # window can have closed. Nor does the interval stream:
        # `_measured_intervals` anchors the stream's last entry to the end of
        # this same measured window, so a stream laid down from `opened_at`
        # reaches `opened_at + measured`, at or past both bounds below.
        opened_at = max(launched_at + self.spec.omit, collected_at - measured)
        window_to = min(collected_at, launched_at + self.spec.omit + measured)
        self._window = (opened_at, window_to)
        self.window_unaccounted_seconds = round(
            max(0.0, (collected_at - launched_at) - self.spec.omit - measured), 3)
        if window_to <= opened_at:
            raise ShapingError(
                f"the {measured}s window the competing flow reports measuring cannot be "
                f"placed inside the {collected_at - launched_at:.3f}s its command actually "
                f"ran, allowing for the {self.spec.omit}s ramp iperf3 runs before it starts "
                "measuring: no instant is inside every admissible position of that window, "
                "so VTOP's share would be charged to a span the competitor may never have "
                "been measuring. Either the flow returned before its own ramp could elapse, "
                "or its report describes a longer window than the command that carried it — "
                "both are broken plumbing rather than a scenario knob")

        # Recorded, never divided: the engine's whole movement across the
        # window, straddling cycles included. Its gap from the attributed figure
        # below is how much of that movement could not be placed, which is what
        # says whether the window is long enough for this engine's cycle.
        #
        # The counter is no longer sampled AT the opening boundary — there is no
        # live instant to sample it at, the boundary being known only now — so
        # what it stood at there is reconstructed from the cycle log: the floor
        # read when the block opened the window, plus every cycle that had
        # already returned by the boundary. The closing figure stays the
        # counter's own, so bytes the log never saw (committed after the last
        # cycle this block noted) still show up in the gap this figure exists to
        # expose.
        counted_at_open = self._opened_from + sum(
            cycle.committed_bytes for cycle in self._cycles if cycle.ended_at <= opened_at)
        self.vtop_bytes_across_window = max(0, closing_bytes - counted_at_open)

        # CHARGED CYCLE BY CYCLE, NOT EDGE MINUS EDGE (review). The difference
        # just above is two reads of a counter that moves one whole
        # `process_once` at a time, so it credits the window with every byte of
        # a cycle that returned inside it — including the uploads that cycle
        # made before the window opened — and with none of a cycle that
        # uploaded inside it and returned after it closed. An earlier round
        # refused only the case where NOTHING returned inside the window, which
        # left both of those mis-attributions standing whenever some other
        # cycle happened to return in between. There is no honest way to split
        # a straddling cycle — the run does not know when within the call each
        # byte left — so it is charged to neither side.
        attributed = [cycle for cycle in self._cycles
                      if cycle.started_at >= opened_at and cycle.ended_at <= window_to]
        if not attributed:
            straddling = sum(1 for cycle in self._cycles
                             if cycle.ended_at >= opened_at and cycle.started_at <= window_to)
            raise ShapingError(
                f"no engine cycle lay wholly inside the {self.spec.seconds}s contended "
                f"window: {len(self._cycles)} cycle(s) ran during this block and "
                f"{straddling} of them overlapped the {window_to - opened_at:.3f}s of it "
                "every admissible placement of the report's own measured window contains, "
                "but every one of those crossed an edge. The engine's committed-byte total "
                "only moves when a process_once call returns, so a cycle that began before "
                "the window opened — or returned after it closed, the window closing where "
                "the competitor's own last measured second could have — carries bytes from "
                "outside it that cannot be separated from the bytes inside. VTOP's share of "
                "this window is therefore unknown, not zero — and recording a zero would credit the "
                "competitor with a fair-share result it never had to fight for. Lengthen "
                f"{COMPETITOR_KEY}'s window so it spans at least one WHOLE engine cycle, "
                "or shorten the cycle")

        # THE DENOMINATOR IS THE INTERVAL THE NUMERATOR CAME FROM. The
        # attributed bytes were committed between the first attributed cycle's
        # start and the last one's return, so that span is what they are
        # divided by — not the competitor's whole window, which would be bytes
        # from one interval over the duration of another. Dividing by the
        # competitor's window instead would charge the engine with the
        # straddling time whose bytes were deliberately dropped, and the error
        # would have no fixed sign — worse than a bias that can be stated.
        #
        # This span is now the SHARED one: the competitor is re-measured over
        # it just below, from its own interval stream, so Jain's index is built
        # from two rates over one interval rather than a slice against an
        # average (review). It is recorded as `vtop_window_seconds` and named in
        # `describe()`, so a reader can see it is not the competitor's 60
        # seconds and can rebuild both of the index's inputs from it.
        covered_from = min(cycle.started_at for cycle in attributed)
        covered_to = max(cycle.ended_at for cycle in attributed)
        covered = covered_to - covered_from
        if covered <= 0:
            raise ShapingError(
                "the engine cycles inside the contended window began and returned at the "
                "same instant, so their bytes crossed the link in no time at all and there "
                "is no rate to record. This is a runner bug: note_progress must be given "
                "the clock read taken BEFORE process_once, not one taken after it")
        # The division uses the RAW span and only the record is rounded (found
        # by a test): rounding first turns any span under a millisecond into a
        # zero, and the guard against dividing by that zero then reports an
        # engine which moved megabytes as 0 Mbit/s.
        self.vtop_window_seconds = round(covered, 3)
        self.vtop_bytes_in_window = sum(cycle.committed_bytes for cycle in attributed)
        self.vtop_attributed_cycles = len(attributed)
        self.vtop_mbps = round(self.vtop_bytes_in_window * 8 / 1e6 / covered, 3)

        # THE COMPETITOR OVER THE SAME SPAN (review). Its whole-window figure is
        # an average over the minute; the span above can be a slice of that
        # minute, and a flow's rate moves within a minute — a queue that fills,
        # a retransmission burst, the engine's own concurrency ramping. Pairing
        # the slice with the average was the last thing left mismatched about
        # the index, and it is the index that the milestone reports. The raw
        # span again, not the rounded record, for the same reason the line above
        # uses it.
        self.competitor_bytes_in_vtop_window, self.competitor_intervals_in_vtop_window = (
            competitor_bytes_over(self.contended, opened_at, covered_from, covered_to))
        self.competitor_mbps_in_vtop_window = round(
            self.competitor_bytes_in_vtop_window * 8 / 1e6 / covered, 3)

        solo_started = time.monotonic()
        self.after = self.measure_alone("alone after")
        self.solo_after_seconds = round(time.monotonic() - solo_started, 3)

        if self.before is None:
            raise ShapingError(
                "the first solo window was never measured, so there is nothing for the "
                "contended one to be compared against; this is a runner bug")
        drift = solo_agreement(self.before.mbps, self.after.mbps)
        if drift > SOLO_AGREEMENT_TOLERANCE:
            raise ShapingError(
                f"the two solo competitor windows disagree by {drift * 100:.1f}%: "
                f"{self.before.mbps} Mbit/s before the run and {self.after.mbps} Mbit/s "
                f"after, outside the ±{int(SOLO_AGREEMENT_TOLERANCE * 100)}% band. The "
                "bottleneck was not the same bottleneck throughout, so the drop measured "
                "with VTOP running cannot be told apart from the drift, and an unstable "
                "bottleneck cannot support a fairness claim. This run is refused rather "
                "than recorded")
        # The two flows that actually shared the queue, over ONE span of it:
        # the competitor's received rate across the engine's attributed cycles,
        # and what the engine committed during them. Not `contended.mbps` — that
        # is the same flow averaged over the whole window, and an index over one
        # rate and one average is an index over nothing in particular.
        self.jain = jain_index([self.competitor_mbps_in_vtop_window, self.vtop_mbps])
        self.closed = True
        self._log(f"[bench] competitor with VTOP: {self.contended.mbps} Mbit/s vs "
                  f"{self.solo_mbps()} Mbit/s alone ({self.share_of_solo():.0%} of it); "
                  f"over the {self.vtop_attributed_cycles} engine cycle(s) inside it "
                  f"({self.vtop_window_seconds}s) VTOP {self.vtop_mbps} Mbit/s against the "
                  f"flow's {self.competitor_mbps_in_vtop_window}; Jain {self.jain}")

    # ------------------------------------------------------------- the record

    def _require_all_three(self) -> None:
        """Every phase must have produced a sample before anything is written.

        The columns are assembled from three separate measurements, and a
        missing one would be rendered into a CSV as the string "None" — a cell
        that reads like a value and is a hole. Refused instead, naming the
        phase that is missing.
        """
        missing = [name for name, sample in (("alone before", self.before),
                                             ("with VTOP", self.contended),
                                             ("alone after", self.after))
                   if sample is None]
        # The index's own second input is checked beside it: it is set in the
        # same breath as the index, so a None here means close() was left
        # half-run, and a results file would carry the index's column filled and
        # one of the two rates it was built from empty.
        if missing or self.jain is None or self.competitor_mbps_in_vtop_window is None:
            raise ShapingError(
                f"the contended run has no {', '.join(missing) or 'fairness index'} to "
                "record: a results file must never carry a number the run did not produce")

    def solo_mbps(self) -> float:
        """The competitor's goodput with VTOP idle: the MEAN of the brackets.

        The mean rather than either end, because neither bracket is privileged
        — and it is only ever a mean of two numbers already proven to agree
        within 10%, so it cannot hide a spread. Both are recorded individually
        in summary.json.
        """
        self._require_all_three()
        return round((self.before.mbps + self.after.mbps) / 2.0, 3)

    def share_of_solo(self) -> float:
        self._require_all_three()
        return self.contended.mbps / self.solo_mbps() if self.solo_mbps() else 0.0

    def flat_columns(self) -> dict[str, Any]:
        self._require_all_three()
        return {"competitor_goodput_mbps_with": self.contended.mbps,
                "competitor_goodput_mbps_without": self.solo_mbps(),
                "competitor_goodput_mbps_over_vtop_window":
                    self.competitor_mbps_in_vtop_window,
                "vtop_goodput_mbps_with": self.vtop_mbps,
                "competitor_jain_index": self.jain}

    def describe(self) -> dict[str, Any]:
        """What summary.json records: every phase, its own window, and the
        arithmetic between them, so a reader can rebuild the index rather than
        take it."""
        self._require_all_three()
        return {
            **self.spec.describe(),
            "rate_unit": "megabits_per_second",
            "alone_before": self.before.describe(),
            "with_vtop": self.contended.describe(),
            "alone_after": self.after.describe(),
            "solo_mean_mbps": self.solo_mbps(),
            "solo_disagreement_pct": round(
                solo_agreement(self.before.mbps, self.after.mbps) * 100, 2),
            "share_of_solo_pct": round(self.share_of_solo() * 100, 2),
            # VTOP's own share: the bytes it COMMITTED, which is the payload it
            # put on the wire and not the wire's own cost. TLS framing,
            # manifest objects and retransmissions are excluded, so this
            # understates the engine. The bias is stated rather than corrected:
            # a correction would be a guess, and a guess in a fairness number
            # is worse than a bound.
            "vtop_committed_bytes": self.vtop_bytes_in_window,
            "vtop_window_seconds": self.vtop_window_seconds,
            "vtop_attributed_cycles": self.vtop_attributed_cycles,
            # What the engine's total moved by across the competitor's own two
            # edges, attributable or not. Recorded beside the figure above and
            # never divided by anything: the gap between them is the movement
            # that straddled an edge, and a reader who sees a large gap knows
            # the window is short relative to this engine's cycle even though
            # the run was not refused.
            "vtop_bytes_across_competitor_window": self.vtop_bytes_across_window,
            # HOW LOOSELY the competitor's measured window is pinned to this
            # run's clock, recorded rather than assumed away. The window's
            # length is the report's; its position is only known to within the
            # wall clock the report cannot account for, so the span above is
            # shortened by this much and the per-second stream is placed within
            # the window to this accuracy. A reader comparing it against
            # jain_span_seconds can see how much of the window that cost.
            "competitor_window_unaccounted_seconds": self.window_unaccounted_seconds,
            "competitor_window_unaccounted_is": (
                "the flow's wall clock its own report does not describe — docker's startup "
                "and the TCP connect before iperf3 began measuring, its closing exchange and "
                "exit after it stopped. The run sees the sum and cannot see how it split "
                "between the two ends, so the span it may attribute anything to is inset "
                "from the measured window's two edges by this much between them"),
            "vtop_goodput_mbps": self.vtop_mbps,
            "vtop_goodput_is": "committed object bytes; excludes framing, manifests, retries",
            "vtop_window_is": (
                "the span from the first attributed engine cycle's start to the last "
                "one's return; cycles straddling either edge of the competitor's window "
                "are excluded because their bytes cannot be split, so this is nested "
                "inside the competitor's window rather than equal to it. BOTH of the "
                "index's rates are taken over this span"),
            # The competitor over that same span — the index's other input, and
            # the reason it is an index over one interval rather than a slice
            # measured against a minute's average. Its bytes are recorded beside
            # it so the rate can be rebuilt rather than taken on trust.
            "competitor_goodput_mbps_over_vtop_window": self.competitor_mbps_in_vtop_window,
            "competitor_bytes_over_vtop_window": self.competitor_bytes_in_vtop_window,
            "competitor_intervals_over_vtop_window": self.competitor_intervals_in_vtop_window,
            "competitor_goodput_over_vtop_window_is": (
                "the receiver's byte total for the window, spread across it in the "
                "proportion the report's own per-second stream gives and restricted to "
                "vtop_window_seconds; the stream is the sender's, so it supplies the "
                "shape only and never the magnitude"),
            "jain_index": self.jain,
            "jain_over": ["competitor", "vtop"],
            # WHICH span the index is over, stated next to it. A reader who
            # assumed the competitor's whole window would be reading the number
            # this field exists to stop being misread.
            "jain_span_seconds": self.vtop_window_seconds,
        }


@contextmanager
def contended(spec: CompetitorSpec | None, vtop_bytes: Callable[[], int],
              run: Runner | None = None,
              log: Callable[[str], None] = print) -> Iterator[Contention | None]:
    """Bracket the block with the competitor's three phases.

    Entered INSIDE the shaped block, so all three windows cross one
    installation of one shape. The first solo window runs on the way in; the
    block opens the contended one when the engine has work, and CLOSES the
    bracket itself with `close()` as its last act.

    Closing is the block's job rather than this exit's because the block has
    ways of stopping that are not exceptions: the runner refuses a bad
    `seed_interval_seconds`, or a dead seeder, with a plain `return`. Measuring
    a second solo window on the way out of one of those would spend a minute
    on it and could then raise a drift refusal of its own, burying the reason
    the run actually stopped under one the operator never asked about. So a
    block that did not close gets the flow COLLECTED — a docker exec outliving
    the run would put a competing flow across the next one — and nothing else.
    A block that forgets to close is not silently forgiven either: every
    accessor on the handle refuses until all three phases exist.
    """
    if spec is None:
        yield None
        return
    handle = Contention(spec, run or docker_exec(spec.container, spec.command_timeout()),
                        vtop_bytes, log)
    handle.open_alone()
    try:
        yield handle
    finally:
        if not handle.closed:
            handle.abandon()


# ------------------------------------------------------- the human-facing line


def describe_competitor_line(record: dict | None) -> str:
    """A recorded contended run as one line of prose, for summary.md.

    Takes `describe()`'s dict rather than the object, because a results
    directory outlives the process that wrote it. The two per-flow numbers come
    FIRST and the index last: summary.md is the artifact the runner prints a
    path to when a run ends, and it has already been the place a newly added
    value was left out of once (#480, review).
    """
    if not record:
        return "none (no competing flow)"
    with_vtop = record.get("with_vtop") or {}
    line = (f"{record.get('mode', '?')} TCP: "
            f"{with_vtop.get('goodput_mbps', '?')} Mbit/s with VTOP vs "
            f"{record.get('solo_mean_mbps', '?')} Mbit/s alone "
            f"({record.get('share_of_solo_pct', '?')}% of it); "
            # THE INDEX'S OWN TWO RATES, side by side, and the span they share.
            # "in the same window" would still be a claim the run cannot make —
            # the span is the engine cycles the window wholly contained, nested
            # inside the competitor's window and not equal to it — but the two
            # numbers ARE over that one span, so the sentence names it once and
            # gives both. The cycle count is what lets a reader tell a span
            # covering most of the window from one covering a sliver of it.
            f"over the {record.get('vtop_attributed_cycles', '?')} engine cycle(s) inside "
            f"it VTOP {record.get('vtop_goodput_mbps', '?')} Mbit/s against the flow's "
            f"{record.get('competitor_goodput_mbps_over_vtop_window', '?')}; "
            f"Jain {record.get('jain_index', '?')}")
    # And how tightly the window could be PLACED (review, and #480's lesson
    # applied rather than restated): the run cannot see where inside its own
    # command's wall clock iperf3's measured window sat, only that it sat
    # somewhere in it, so the attributable span is short of the measured one by
    # the setup and teardown it cannot split. That is a diagnostic rather than
    # a result — but it is the number that says how much of the window the
    # index actually saw, and leaving a newly added value out of summary.md is
    # precisely the omission this docstring already records once.
    unaccounted = record.get("window_unaccounted_seconds")
    placed = f", {unaccounted}s of the window unplaceable" if unaccounted else ""
    return (f"{line} — over {record.get('window_seconds', '?')}s, solo windows "
            f"{record.get('solo_disagreement_pct', '?')}% apart{placed}")
