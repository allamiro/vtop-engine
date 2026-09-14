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

# How far the per-interval placements of ONE stream may disagree before the
# stream is refused as not having been written live (review). Each interval
# line's source stamp (see SOURCE_STAMPER), minus where that interval ends on the
# window's own clock, is an estimate of when the window opened — late by however
# long iperf3 took to write that one line. Measured live, the estimates of one
# stream agree to within a few milliseconds. A stream that was BUFFERED before
# the stamper saw it — iperf3's stdout block-buffered into the pipe — disagrees
# by whole intervals instead: every line of a buffered block is stamped at once,
# so its estimates spread by the block's own length, and its stamps date
# nothing. A delay AFTER the stamper, in the relay to this host, no longer moves
# any estimate at all. A quarter of the default
# one-second interval sits two orders of magnitude above the live jitter and
# well below the smallest buffered block, which is one whole interval.
STREAM_SYNC_TOLERANCE_SECONDS = 0.25

# The flag this module places the contended window with. Named so the refusal
# for an iperf3 that lacks it can say exactly what is missing: it arrived in
# iperf 3.17 (the netshoot:v0.13 build reports itself as "3.16+" and carries it;
# netshoot:v0.16 is iperf 3.21).
JSON_STREAM_FLAG = "--json-stream"

# The stamper that runs BESIDE iperf3, inside the probe container (review). The
# host used to stamp each line when it READ it, which dates the line late by its
# whole trip through the containerd relay and docker exec's pipe — and a delay
# common to every line (an overloaded or remote relay) shifts all of them
# equally, where no spread check between lines can see it. So each line is
# stamped where it is WRITTEN: this program reads iperf3's stdout inside the
# container and prefixes every line with `time.monotonic()`.
#
# That stamp is on the run's own clock because a container does not get its own
# CLOCK_MONOTONIC: Docker creates no time namespace, so the probe container and
# this process read the same kernel clock (measured: host arrival minus
# container stamp was 0.07–0.37 ms through docker exec). Where that is NOT true
# — a VM-backed Docker, whose containers run on another kernel — the stamps are
# refused rather than trusted: see `split_source_stamps`.
#
# readline, not iteration: a text-mode `for line in sys.stdin` may wait to fill
# a read-ahead buffer, which is the very batching the stamp exists to exclude.
SOURCE_STAMPER = (
    "import sys, time\n"
    "while True:\n"
    "    line = sys.stdin.readline()\n"
    "    if not line:\n"
    "        break\n"
    "    sys.stdout.write('%.6f %s' % (time.monotonic(), line))\n"
    "    sys.stdout.flush()\n")

# How much LATER than its host arrival a source stamp may claim to be before the
# two are refused as different clocks. A shared clock cannot put a line's writing
# after its reading at all; the slack only absorbs the float formatting of the
# stamp. Anything beyond it is not jitter, it is another kernel's clock.
CLOCK_AGREEMENT_TOLERANCE_SECONDS = 5e-3

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


def stamped_command(argv: list[str], timeout: float) -> tuple[int, str, tuple[float, ...] | None]:
    """Run a command, stamping each line of its stdout with the monotonic clock
    the instant the line ARRIVES — read live, never collected at exit (review).

    The stamps are what place the contended window on the run's clock. A
    command collected whole at exit says only that its output existed by then,
    which pins the window iperf3 measured to somewhere inside the command's wall
    clock and no tighter; the docker startup, the connect and the closing
    exchange around it are one lump the run cannot split. A line stamped as it
    arrives dates the interval it reports, to within the time that one line
    spent in the pipe.

    Returns the exit code, the output and the stamps. On success the output is
    stdout alone and the stamps are one per non-blank line of it, so the parser
    can pair them; on failure stderr is JOINED, never dropped — iperf3 reports
    "unable to connect" and "unrecognized option" there, and a refusal that
    printed only an exit code would send an operator looking at the wrong
    container — and the stamps are None, because nothing will be placed.

    Raises FileNotFoundError when the program is missing and
    subprocess.TimeoutExpired when it outlives `timeout` (it is killed first),
    so the caller can name the container in its own sentence.
    """
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, bufsize=1)
    # stderr drained on its own thread: a command that fills the stderr pipe
    # while this thread blocks on stdout would otherwise deadlock both ends.
    errors: list[str] = []
    drain = threading.Thread(target=lambda: errors.append(proc.stderr.read()),
                             name="competitor-stderr", daemon=True)
    drain.start()
    expired = threading.Event()

    def _expire() -> None:
        expired.set()
        proc.kill()

    timer = threading.Timer(timeout, _expire)
    timer.daemon = True
    timer.start()
    lines: list[str] = []
    stamps: list[float] = []
    try:
        for line in proc.stdout:
            # FIRST, before anything else touches the line: every statement
            # between the read and the clock read is error in the placement.
            arrived = time.monotonic()
            if line.strip():
                lines.append(line.rstrip("\n"))
                stamps.append(arrived)
        proc.wait()
    finally:
        timer.cancel()
        drain.join(timeout=5.0)
    if expired.is_set():
        raise subprocess.TimeoutExpired(argv, timeout)
    out = "\n".join(lines)
    if proc.returncode != 0:
        joined = (out + "\n" + "".join(errors)).strip()
        return proc.returncode, joined, None
    return proc.returncode, out, tuple(stamps)


def docker_exec(container: str, timeout: float) -> Runner:
    """Run a command in a lab container. Injectable so the tests never need a
    container or an iperf3.

    Its own copy rather than `netem.docker_exec` for two reasons. The timeout:
    that one is fixed at 40 s for a 13-second calibration probe, and a 60-second
    competitor under it would be killed mid-flow and reported as a container
    that stopped answering — a measurement error dressed as an infrastructure
    one. And the stamps: this one reads the output as it arrives, which is what
    lets `close()` place the contended window rather than bound it.
    """

    def run(argv: list[str]) -> tuple[int, str, tuple[float, ...] | None]:
        # The command runs through the source stamper (see SOURCE_STAMPER), under
        # pipefail so iperf3's own exit status — not the stamper's — is the one
        # this runner reports. $0 names the wrapper, $1 is the stamper's source,
        # and the rest is the command exactly as the caller built it.
        wrapped = ["sh", "-c", 'set -o pipefail; s="$1"; shift; "$@" | python3 -c "$s"',
                   "competitor", SOURCE_STAMPER, *argv]
        try:
            code, out, arrivals = stamped_command(["docker", "exec", container, *wrapped],
                                                  timeout)
        except FileNotFoundError as exc:
            raise ShapingError(
                "docker is not on PATH, so the competing flow cannot be started: a "
                "contended scenario needs the lab stack") from exc
        except subprocess.TimeoutExpired as exc:
            raise ShapingError(
                f"`{shlex.join(argv)}` in {container} did not finish in {timeout}s. The "
                "competing flow was cut off, so the window it was measuring is not the "
                "window that ran") from exc
        if code != 0 or arrivals is None:
            return code, out, arrivals
        return (code, *split_source_stamps(out, arrivals, container))

    return run


def split_source_stamps(out: str, arrivals: tuple[float, ...],
                        container: str) -> tuple[str, tuple[float, ...]]:
    """Strip the source stamper's prefixes, returning the lines and the stamps
    they were WRITTEN at — the ones the window is placed by.

    The host's own arrival stamps are kept for one purpose only: to prove the
    source stamps are on this process's clock. A line cannot be written after
    it was read, so a source stamp later than its arrival means the container
    runs on a different kernel's monotonic clock, and every placement made from
    it would be an offset between two machines' uptimes (review).
    """
    raw = out.splitlines()
    if len(raw) != len(arrivals):
        raise ShapingError(
            f"the competitor's output in {container} has {len(raw)} line(s) but {len(arrivals)} "
            "arrival stamp(s), so the stamps cannot be paired with the lines they date. This is "
            "a runner bug")
    lines, stamps = [], []
    for index, line in enumerate(raw):
        arrived = arrivals[index]
        stamp, _, text = line.partition(" ")
        try:
            written = float(stamp)
        except ValueError:
            raise ShapingError(
                f"line {index + 1} of the competitor's output in {container} carries no source "
                f"stamp ({line[:120]!r}): the stamper beside iperf3 did not run, so nothing "
                "says when that line was written and the contended window cannot be placed. "
                "The probe container needs python3 (nicolaka/netshoot has it)") from None
        if written - arrived > CLOCK_AGREEMENT_TOLERANCE_SECONDS:
            raise ShapingError(
                f"the competitor's line {index + 1} claims to have been written "
                f"{written - arrived:.3f}s AFTER this process read it. A line cannot be read "
                f"before it is written, so {container} is not reading this host's monotonic "
                "clock — a VM-backed Docker runs containers on another kernel — and its "
                "stamps cannot place the window on this run's clock. The netem lab is "
                "Linux-only for this reason among others")
        lines.append(text)
        stamps.append(written)
    return "\n".join(lines), tuple(stamps)


# A command runner: exit code, output, and one arrival stamp per non-blank
# output line — or None where the lines were not observed as they arrived. Only
# the contended window needs the stamps, and it refuses a runner that has none.
Runner = Callable[[list[str]], tuple[int, str, "tuple[float, ...] | None"]]


@dataclass(frozen=True)
class CompetitorInterval:
    """One entry of the competitor's own per-second stream, on the window's clock.

    `start` and `end` are seconds from the beginning of the window iperf3
    MEASURED — the one its `end` summary describes, and the one `Contention`
    places on the run's own clock after the fact. Everything iperf3 does to its
    own clock around the ramp it omits is normalised away in
    `_measured_intervals`, once, so nothing downstream has to know about it.

    `emitted_at` is the monotonic instant this interval's line was WRITTEN,
    stamped beside iperf3 inside the probe container on the kernel clock this
    process shares (see SOURCE_STAMPER), or None where the report was not read
    live. iperf3 emits the line when the interval ends, so `emitted_at - end` is
    where this entry puts the window's opening on the run's clock — late only by
    iperf3's own write and one local pipe, and not at all by the relay that
    carries the line to the host. See `Contention.close()`.
    """

    start: float
    end: float
    bytes: int
    emitted_at: float | None = None


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

        `-O` omits the ramp from iperf3's own summary. `--json-stream` rather
        than `-J` (review): the same report, one event per line, and each
        interval's line is written when that interval ENDS rather than all of
        them at exit — which is what lets the host date the interval stream by
        reading it, instead of inferring where a window of the right length
        might have sat. No `-b`: a `bulk` competitor is congestion-controlled,
        because a paced flow cannot lose a share it never asked for and its
        goodput would say nothing about fairness.
        """
        return ["iperf3", "-c", self.server, "-p", str(self.port),
                "-t", str(self.seconds), "-O", str(self.omit), JSON_STREAM_FLAG]

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
    # Nor may a policer sit IN FRONT of the queue (review). The ingress hook
    # polices before it redirects to the tbf, so a policer at or below the
    # bottleneck is the real constraint and the two flows never reach the queue
    # they are supposed to share; one above it still drops the bursts the tbf
    # would have absorbed. Either way the index would credit a queueless
    # dropper's decisions to queue sharing — and the rate calibration removes
    # the policer, so the run would be labelled with a tbf rate it did not use.
    # One mechanism per fairness number: refused whatever its rate.
    policer = getattr(shape, "policer_kbps", 0)
    if policer:
        raise ShapingError(
            f"{COMPETITOR_KEY} cannot be combined with shaping_policer_kbps ({policer}): the "
            "policer drops ahead of the shared tbf queue, so the fairness index would "
            "describe a queueless dropper rather than the queue the two flows are meant to "
            "share, and calibration measures the link without it. Measure the policed "
            "uplink (scenario 15) and the shared queue in separate runs")


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
                        measured_seconds: float,
                        arrivals: list[float | None] | None = None,
                        ) -> tuple[CompetitorInterval, ...]:
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

    `arrivals`, when given, is one host arrival stamp per entry of `intervals`,
    in order, and each surviving entry carries its own. The clamp above moves
    only an entry's BEGINNING; its end, which is the instant its line was
    written, is never moved, so every stamp stays paired with the edge it dates.
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
    if arrivals is not None and len(arrivals) != len(raw):
        raise ShapingError(
            f"the {phase} competitor's report carries {len(raw)} interval(s) but "
            f"{len(arrivals)} arrival stamp(s): the stamps can no longer be paired with the "
            "intervals they date, so the stream cannot be placed on the run's clock. This "
            "is a runner bug")
    reported: list[tuple[float, float, int, float | None]] = []
    try:
        for index, entry in enumerate(raw):
            summary = entry["sum"]
            if summary.get("omitted"):
                continue
            reported.append((float(summary["end"]), float(summary["seconds"]),
                             int(summary["bytes"]),
                             None if arrivals is None else arrivals[index]))
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
    origin = max(end for end, _seconds, _count, _arrived in reported) - measured_seconds
    stream = []
    for end, seconds, count, arrived in reported:
        finish = end - origin
        begin = max(0.0, finish - seconds)
        if finish - begin <= 0:
            # An entry with no duration cannot be placed on a clock at all.
            # iperf3 emits one only as a rounding artifact at the very end of a
            # window, carrying nothing worth spreading, and the magnitude comes
            # from the receiver's total either way — so dropping it moves the
            # shape by nothing and leaves every remaining span divisible.
            continue
        stream.append(CompetitorInterval(begin, finish, count, arrived))
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
    # THE DENOMINATOR NEEDS THE WHOLE WINDOW TOO (review). The receiver's total
    # is spread in proportion to the stream's bytes, so `total` must be the
    # stream's bytes over the SAME window the receiver's total covers. A hole
    # anywhere — even one wholly outside the span above, which the coverage check
    # there cannot see — drops that second's bytes from `total` and scales the
    # span's share of the receiver's total UP by exactly that much: the competing
    # flow recorded as having received more during VTOP's span than it did, and
    # the index skewed by a second nobody measured. Checked across the whole
    # measured window, 0 to the report's own `seconds`.
    reached = 0.0
    window_hole: tuple[float, float] | None = None
    for slot in sorted(sample.intervals, key=lambda entry: entry.start):
        if slot.start > reached + STREAM_JOIN_TOLERANCE_SECONDS:
            window_hole = (reached, slot.start)
            break
        reached = max(reached, slot.end)
    if window_hole is None and reached < sample.seconds - STREAM_JOIN_TOLERANCE_SECONDS:
        window_hole = (reached, sample.seconds)
    if window_hole is not None:
        raise ShapingError(
            f"the competing flow's stream says nothing about {window_hole[0]:.3f}-"
            f"{window_hole[1]:.3f}s of its {sample.seconds:g}s measured window. The receiver's "
            "total is spread across the span VTOP was measured over in proportion to the "
            "stream's bytes, so every second of the window has to be in the stream: a missing "
            "one leaves its bytes out of the proportion and hands them to the seconds that "
            "remain, whichever side of VTOP's span the hole sits on. Rerun with the lab's own "
            "iperf3 (the netshoot middlebox) and check the middlebox was not starved of CPU "
            "while it was reporting; this is a report with a hole in it, not a scenario knob")
    if total <= 0:
        raise ShapingError(
            f"the competing flow's report says it received {sample.bytes} byte(s) while "
            "every interval of its own stream carries none: there is no shape to spread "
            "that total across, so its rate over the span VTOP was measured on cannot be "
            "derived. The report contradicts itself and is refused rather than averaged")
    return int(round(sample.bytes * inside / total)), len(overlaps)


def _read_stream(out: str, phase: str, arrivals: tuple[float, ...] | None,
                 ) -> tuple[dict[str, Any], list[float | None] | None]:
    """An `iperf3 --json-stream` output as the report it streams, and the
    arrival stamp of each of its intervals.

    The stream is the `-J` report cut into events — `start`, one `interval` per
    reporting interval, `end` (and `error`, which iperf3 still follows with an
    empty `end`) — so it is reassembled into that report's shape and everything
    downstream reads it exactly as before. What the stream adds is the stamps.

    A WHOLE `-J` DOCUMENT IS REFUSED BY NAME rather than read: it carries the
    same numbers, but it was written at exit, so no stamp can date anything in
    it — and accepting it would quietly bring back the window placement this
    format exists to remove, from a wrapper or an argv that was edited back.
    """
    try:
        whole = json.loads(out)
    except ValueError:
        whole = None
    if isinstance(whole, dict) and "event" not in whole:
        raise ShapingError(
            f"the {phase} competitor produced a whole-document iperf3 report (`-J`), not the "
            f"line-per-event stream `{JSON_STREAM_FLAG}` writes: {out[:400]}. The document "
            "is written when the flow exits, so nothing in it says when any of its seconds "
            "happened, and the contended window could only be bounded, not placed — which "
            "lets the engine's span be paired with the wrong seconds of the flow. Run the "
            f"competitor with {JSON_STREAM_FLAG}")
    lines = [line for line in out.splitlines() if line.strip()]
    if arrivals is not None and len(arrivals) != len(lines):
        raise ShapingError(
            f"the {phase} competitor's output has {len(lines)} line(s) but {len(arrivals)} "
            "arrival stamp(s), so the stamps cannot be paired with the lines they date. "
            "This is a runner bug")
    report: dict[str, Any] = {"intervals": []}
    interval_arrivals: list[float | None] = []
    for index, line in enumerate(lines):
        try:
            event = json.loads(line)
        except ValueError:
            event = None
        if not isinstance(event, dict) or "event" not in event:
            raise ShapingError(
                f"the {phase} competitor's output was not an iperf3 {JSON_STREAM_FLAG} report "
                f"— line {index + 1} is not an event: {out[:400]}")
        kind, data = event.get("event"), event.get("data")
        if kind == "interval":
            report["intervals"].append(data)
            interval_arrivals.append(None if arrivals is None else arrivals[index])
        elif kind == "end":
            report["end"] = data
        elif kind == "error":
            report["error"] = data
        elif kind == "start":
            report["start"] = data
        # Any other event is a later iperf3's addition and carries nothing this
        # module reads; it is passed over rather than refused, because refusing
        # it would fail every run on an upgrade that changed no number.
    return report, (None if arrivals is None else interval_arrivals)


def parse_report(out: str, spec: CompetitorSpec, phase: str,
                 arrivals: tuple[float, ...] | None = None) -> CompetitorSample:
    """One `iperf3 --json-stream` report as a sample, or a refusal naming what
    it saw. `arrivals`, when the runner read the output live, is one stamp per
    non-blank line; see `_read_stream`.

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
    report, interval_arrivals = _read_stream(out, phase, arrivals)
    if report.get("error"):
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
    intervals = _measured_intervals(report, out, spec, phase, seconds, interval_arrivals)
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
    code, out, arrivals = run(spec.argv())
    if code != 0 and "unrecognized option" in out and JSON_STREAM_FLAG.lstrip("-") in out:
        # Its own sentence rather than the generic one below: "start the netem
        # stack" is the wrong advice for a stack that is up and running an
        # iperf3 too old to stream its report. Matched without the dashes
        # because getopt implementations differ on whether they echo them —
        # netshoot's musl build prints `unrecognized option: json-stream`.
        raise ShapingError(
            f"the iperf3 in {spec.container} does not support {JSON_STREAM_FLAG} (iperf 3.17 "
            f"or later): {out[:400]}. The contended window is placed on the run's clock by "
            "reading that stream as it arrives, and without it the window could only be "
            "bounded — which lets VTOP's span be paired with the wrong seconds of the "
            "competing flow. Use the lab's netshoot image (nicolaka/netshoot:v0.16 in "
            "docker-compose.benchmark.yml) rather than falling back to -J")
    if code != 0:
        raise ShapingError(
            f"the {phase} competitor could not be started in {spec.container} "
            f"(`{shlex.join(spec.argv())}` exited {code}): {out[:400]}. Start the netem "
            "stack — docker compose -f benchmarks/docker-compose.benchmark.yml "
            "--profile netem up -d — and check the middlebox's iperf3 server is up")
    return parse_report(out, spec, phase, arrivals)


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

    `uncounted_batches` is how many things in the cycle may have put bytes on
    the link that `committed_bytes` does not hold (review): a batch that did not
    commit, or a `process_once` that exited nonzero. The engine reports sizes
    only for batches that reached VERIFIED — a batch that failed after its
    upload, at verification, reports no metrics at all, and a call that errors
    out prints no outcomes — so the bytes those put on the wire, which the
    competitor had to share the queue with, are UNKNOWN. A count rather than a
    flag so the refusal can say how many.
    """

    started_at: float
    ended_at: float
    committed_bytes: int
    uncounted_batches: int = 0


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
        # The flow's wall clock its own report does not describe: docker's
        # startup and the TCP connect before iperf3 began measuring, its closing
        # exchange and exit after it stopped. A diagnostic of the plumbing's
        # cost, and no longer a bound on where the window sat — the stream's
        # arrival stamps place it (review). See `close()`.
        self.window_unaccounted_seconds: float = 0.0
        # How that overhead split: the part that fell BEFORE the window opened,
        # which the run could not see when it had only the two ends of the
        # command. The rest is the closing tail.
        self.window_setup_seconds: float = 0.0
        # How far the per-interval placements of the window disagreed with one
        # another: the jitter of the pipe the stream crossed, and the evidence
        # that it was read live rather than buffered.
        self.window_sync_spread_seconds: float = 0.0
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
        # measured window could possibly occupy — no longer what places the
        # window (the stream's own arrival stamps do), but what `close()`
        # checks that placement against.
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
        the report's own stream, stamped line by line as it arrived, and checked
        against the flow's own two clock reads. It used to be
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

    def note_progress(self, started_at: float, ended_at: float,
                      uncounted_batches: int = 0) -> None:
        """Record the cycle that has just returned: its interval, and its bytes.

        `started_at` is the monotonic clock read the runner took BEFORE the
        `process_once` call that is now returning, and `ended_at` the one it
        took the instant that call RETURNED — both the caller's, and both
        load-bearing (review).

        THE END IS THE CALLER'S TOO, not a clock read taken here (review). The
        runner calls this only after it has parsed every outcome of the cycle
        and written each batch's rows to three CSV files, which flush; a clock
        read here stamped all of that harness bookkeeping onto the cycle. The
        engine was not uploading during it, so the stretched cycle could cross
        the window's closing edge it had in fact returned inside — and be
        dropped from the share — or widen the span the attributed bytes are
        divided by, understating VTOP's rate over it.

        `uncounted_batches` is the runner's count of what the committed total
        cannot see in this cycle; see `EngineCycle`.

        Why an interval at all (review): an earlier round noted only the
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
        caller's two clock reads and one int — so it costs a
        contended run nothing to call after every cycle, which is exactly how
        often it must be called: each cycle's bytes are a difference against
        the last one counted, and a skipped call folds two cycles into one
        interval that is then likely to straddle an edge and be dropped.
        """
        if ended_at < started_at:
            raise ShapingError(
                f"an engine cycle was noted as returning {started_at - ended_at:.3f}s before "
                "it began: the runner passed its two clock reads the wrong way round, or from "
                "two different clocks. This is a runner bug")
        total = self._vtop_bytes()
        # Clamped only because a negative difference would become a negative
        # goodput and an index computed over nonsense. The counter is a
        # monotonic sum of committed batches, so this can fire on a runner bug
        # and on nothing else.
        self._cycles.append(
            EngineCycle(started_at, ended_at, max(0, total - self._counted_through),
                        max(0, int(uncounted_batches))))
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
        # The engine's block ends HERE. Whether that was before the competitor's
        # measured window ended is judged below, once the window is placed —
        # not from whether the flow's thread is still alive (review): the thread
        # outlives the measured window by iperf3's closing exchange, the report's
        # last lines and docker exec's teardown, so a block ending in that tail
        # had VTOP traffic in every measured second and was still refused.
        closed_at = time.monotonic()
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

        # WHERE THE MEASURED WINDOW SAT ON THIS RUN'S CLOCK (review, twice).
        #
        # An earlier round derived it from the command's two ends alone: the
        # window cannot have opened before `launched_at + omit`, and must have
        # closed by `collected_at`, so a window of the report's length sat
        # somewhere between. That bounds the window; it does not place it. The
        # wall clock the two ends leave over — docker's startup and the connect
        # at one end, iperf3's closing exchange at the other — is one lump the
        # run could not split, and laying the interval stream down from the
        # LATEST admissible opening assumed all of it fell before measuring
        # began. Whatever fell after instead shifted the whole stream: the
        # engine's span was read against the wrong seconds of the flow, and the
        # competitor's rate over it and Jain's index described different seconds
        # from the engine cycles they were paired with.
        #
        # So the window is PLACED, from the stream itself. iperf3 writes each
        # interval's line when that interval ends, and the stamper beside it
        # stamped each line as it was WRITTEN, on the clock this process reads,
        # so every measured interval says where the window opened: its writing
        # minus where it ends on the window's own clock. Stamped at the source
        # rather than on arrival (review): an arrival stamp is late by the
        # relay, and a relay delay common to every line shifts every estimate
        # together, invisible to any comparison between them. Each estimate is
        # late only by iperf3's write and never early, so the earliest is the
        # tightest, and it is the one taken. The two ends of the command are kept — as a CONSISTENCY CHECK
        # on the placement, no longer as the placement.
        earliest = launched_at + self.spec.omit
        latest = collected_at - measured
        self.window_unaccounted_seconds = round(
            max(0.0, (collected_at - launched_at) - self.spec.omit - measured), 3)
        unstamped = sum(1 for slot in self.contended.intervals if slot.emitted_at is None)
        if unstamped:
            raise ShapingError(
                f"{unstamped} of the {len(self.contended.intervals)} measured interval(s) in the "
                "contended competitor's report carry no source stamp, so the runner did not "
                "read its stream as it was written and the window cannot be placed on this run's "
                "clock — only bounded, which lets VTOP's span be paired with the wrong seconds "
                "of the competing flow. This is a runner bug: the contended window needs a "
                "runner that stamps each line of the stream as it is written")
        placements = [slot.emitted_at - slot.end for slot in self.contended.intervals]
        synced = min(placements)
        spread = max(placements) - synced
        self.window_sync_spread_seconds = round(spread, 3)
        self.window_setup_seconds = round(max(0.0, synced - earliest), 3)
        if spread > STREAM_SYNC_TOLERANCE_SECONDS:
            raise ShapingError(
                f"the contended competitor's interval lines place its window's opening "
                f"{spread:.3f}s apart from one another, beyond the "
                f"{STREAM_SYNC_TOLERANCE_SECONDS}s a stream read live stays within: the lines "
                "were buffered somewhere between iperf3 and this process and arrived in "
                "bursts, so their arrival no longer dates the seconds they report. Placing the "
                "window from them would pair VTOP's span with the wrong seconds of the flow. "
                "Check that the competitor runs through `docker exec` without a wrapper that "
                "buffers its output, and that the middlebox was not starved of CPU")
        if (synced < earliest - STREAM_SYNC_TOLERANCE_SECONDS
                or synced > latest + STREAM_SYNC_TOLERANCE_SECONDS):
            raise ShapingError(
                f"the contended competitor's own stream places its {measured}s window opening "
                f"{synced - launched_at:.3f}s after the command was launched, but a window of "
                f"that length can only have opened between {earliest - launched_at:.3f}s (the "
                f"launch plus the {self.spec.omit}s ramp iperf3 omits) and "
                f"{latest - launched_at:.3f}s (its length before the report came back). The "
                "stamps and the command's own two ends contradict each other, so neither can "
                "be trusted to say where VTOP's share belongs: the report describes a longer "
                "window than the command that carried it, or the stamps were not taken on "
                "this run's clock. Both are broken plumbing rather than a scenario knob")
        # Clamped into the admissible bounds only to absorb the tolerance just
        # allowed; the stream itself is laid down from `synced`, unclamped, so
        # each interval stays where its own line put it.
        opened_at = max(synced, earliest)
        window_to = min(synced + measured, collected_at)
        self._window = (opened_at, window_to)
        # THE ENGINE MUST OUTLAST THE MEASURED WINDOW, judged against the window
        # as placed. A block that closed before `synced + measured` left the last
        # part of the contended window with no VTOP in it, so its goodput is
        # partly a solo measurement under a contended name. `synced` is the
        # earliest placement, so this end is never later than the true one and
        # the check cannot refuse a block that actually outlasted the window.
        solo_tail = (synced + measured) - closed_at
        if solo_tail > 0:
            raise ShapingError(
                f"the engine's window closed {solo_tail:.1f}s before the competitor's "
                f"measured window did: the last part of the {self.spec.seconds}s contended "
                "window had no VTOP traffic in it, so its goodput is partly a solo "
                "measurement under a contended name. Lengthen duration_seconds, or "
                f"shorten {COMPETITOR_KEY}'s window")
        if window_to <= opened_at:
            raise ShapingError(
                f"the {measured}s window the competing flow reports measuring leaves no span "
                "on this run's clock once placed from its own stream, so VTOP's share would be "
                "charged to a span the competitor was never measuring. This is broken "
                "plumbing rather than a scenario knob")

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
                f"{straddling} of them overlapped the {window_to - opened_at:.3f}s window the "
                "competitor's own stream placed on this run's clock, but every one of those "
                "crossed an edge. The engine's committed-byte total "
                "only moves when a process_once call returns, so a cycle that began before "
                "the window opened — or returned after it closed — carries bytes from "
                "outside it that cannot be separated from the bytes inside. VTOP's share of "
                "this window is therefore unknown, not zero — and recording a zero would credit the "
                "competitor with a fair-share result it never had to fight for. Lengthen "
                f"{COMPETITOR_KEY}'s window so it spans at least one WHOLE engine cycle, "
                "or shorten the cycle")

        # A CYCLE THAT PUT UNKNOWN BYTES ON THE LINK VOIDS THE SHARE (review).
        # The numerator is committed bytes, and the only sizes the engine
        # reports are for batches that reached VERIFIED: one that uploaded its
        # object and then failed verification reports no metrics, and a
        # process_once that errored prints no outcomes at all. Their uploads
        # still sat in the same tbf queue as the competitor — they are part of
        # why the competitor's rate over this span is what it is — while adding
        # nothing to VTOP's. A span of failed batches reads as VTOP at 0 Mbit/s
        # beside a starved neighbour, and an index that blames the link; a span
        # where only some failed reads as a smaller engine than the one the
        # competitor queued behind.
        #
        # Refused rather than estimated — the engine says nothing about how far
        # a failed batch got — and refused only over the ATTRIBUTED cycles:
        # they alone make up the span both of the index's rates are taken over,
        # so a failure in a straddling cycle outside it moves neither rate. Why
        # the wire is not counted instead is in the README's contended-run
        # section: the counters the lab can read are not VTOP's goodput either.
        uncounted = [cycle for cycle in attributed if cycle.uncounted_batches]
        if uncounted:
            raise ShapingError(
                f"{sum(cycle.uncounted_batches for cycle in uncounted)} uncommitted batch(es) "
                f"or failed engine call(s) in {len(uncounted)} of the {len(attributed)} engine "
                "cycle(s) inside the contended window: whatever they uploaded before failing "
                "crossed the shared queue without reaching VTOP's committed-byte total, "
                "because the engine reports no size for a batch that did not reach VERIFIED "
                "and no outcomes at all for a call that errored. VTOP's share of this window "
                "is therefore unknown, not zero and not the smaller number the committed "
                "batches add up to — either would understate the engine by exactly the "
                "traffic the competitor was contending with, and the fairness index would "
                "clear VTOP of it. A contended run must not fail batches: find out why these "
                "did (batch_metrics.csv carries their final state) and rerun")

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
            competitor_bytes_over(self.contended, synced, covered_from, covered_to))
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
            # HOW the competitor's measured window was placed on this run's
            # clock, recorded rather than asserted (review). The window's length
            # is the report's; its position is the stream's, read line by line
            # as it arrived. The plumbing's overhead around it is recorded with
            # the part of it that fell before measuring began, which is the
            # split a run holding only the command's two ends could not see —
            # and the spread is the evidence the stream was read live.
            "competitor_window_unaccounted_seconds": self.window_unaccounted_seconds,
            "competitor_window_unaccounted_is": (
                "the flow's wall clock its own report does not describe — docker's startup "
                "and the TCP connect before iperf3 began measuring, its closing exchange and "
                "exit after it stopped. A cost of the plumbing, not an uncertainty in where "
                "the window sat"),
            "competitor_window_setup_seconds": self.window_setup_seconds,
            "competitor_window_sync_spread_seconds": self.window_sync_spread_seconds,
            "competitor_window_placed_by": (
                "the arrival of each interval line of iperf3's --json-stream on the host, less "
                "where that interval ends on the window's own clock; the earliest such estimate "
                "is taken, so the placement is late by at most the fastest line's time in the "
                "pipe. competitor_window_setup_seconds is how much of the unaccounted overhead "
                "fell before the window opened; competitor_window_sync_spread_seconds is how "
                "far the per-line estimates disagreed, refused above "
                f"{STREAM_SYNC_TOLERANCE_SECONDS}s as a stream that was not read live"),
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
    # And how the window was PLACED (review, and #480's lesson applied rather
    # than restated): the stream's spread is the evidence the placement rests
    # on, and the plumbing's overhead beside it says what the run spent around
    # the window. Diagnostics rather than results — but leaving a newly added
    # value out of summary.md is precisely the omission this docstring already
    # records once, and it happened again here: this line read
    # `window_unaccounted_seconds` while describe() records
    # `competitor_window_unaccounted_seconds`, so it was silently never printed
    # (review). A test now renders this line from describe()'s own output and
    # checks every key read here is one describe() writes.
    unaccounted = record.get("competitor_window_unaccounted_seconds")
    spread = record.get("competitor_window_sync_spread_seconds")
    placed = ""
    if spread is not None:
        placed += f", window placed from its live stream to within {spread}s"
    if unaccounted is not None:
        placed += (f", {unaccounted}s of command overhead "
                   f"({record.get('competitor_window_setup_seconds', '?')}s of it before "
                   "measuring began)")
    return (f"{line} — over {record.get('window_seconds', '?')}s, solo windows "
            f"{record.get('solo_disagreement_pct', '?')}% apart{placed}")
