"""The netem middlebox: a link that actually bites (#477).

`shaping.py`'s toxiproxy driver can make the pipe thin and long. It cannot
make it LOSSY, it cannot make it POLICED, and it cannot give two flows a
queue to share — toxiproxy terminates TCP and applies bandwidth/latency
toxics per connection, so there is no packet to drop and no buffer to fill.
Those are exactly the conditions under which one transport beats another, so
a lab that cannot produce them cannot answer the question the egress epic
(#475) asks.

This driver puts a real forwarding middlebox in the path and shapes it with
`tc`. The middlebox is a container on the `netem` profile holding
CAP_NET_ADMIN; the engine and the store hold none. netem and tbf run THERE
and never on the sender, where the transmit-side queue and the local qdisc
reshape the very traffic being measured.

    shaping_driver: netem            # toxiproxy (default) | netem
    shaping_latency_ms: 100          # round trip, split across directions
    shaping_jitter_ms: 20            # spread on that round trip
    shaping_loss_pct: 1.0            # on the DATA direction
    shaping_loss_model: gemodel      # random | gemodel (bursty)
    shaping_bottleneck_kbps: 10000   # KILOBITS/s, tc's unit
    shaping_buffer_bdp: 1.0          # bottleneck buffer, in BDP multiples
    shaping_policer_kbps: 0          # KILOBITS/s; a policer holds no queue

UNITS, because two keys disagree and the disagreement is inherited: the
toxiproxy driver's `shaping_bandwidth_kbps` is KILOBYTES per second, because
that is what toxiproxy's bandwidth toxic takes. Every netem key here is
KILOBITS per second, because that is what `tc` takes. The recorded columns
name their unit so a number is never read in the wrong one.

WHERE EACH SHAPE SITS. The middlebox has two interfaces: one facing the
engine, one facing the store. That is what makes the directions separable
without inspecting a single packet:

  * packets ARRIVING on the engine-side interface are the upload, and only
    the upload — the store's replies arrive on the other interface. They are
    redirected with `act_mirred` onto an `ifb` device, where the impairment
    (netem: half the delay, all of the loss) and the bottleneck (tbf: rate and
    buffer depth, as netem's child) are applied. A `tc police` action on the same ingress
    hook, ahead of the redirect, is the policer: it drops above its rate and
    holds no queue, so it emits no queueing-delay signal at all.
  * packets LEAVING on the engine-side interface are the download, and only
    the download. They carry the other half of the delay.

Loss is applied to the upload direction alone, so `shaping_loss_pct` means
exactly "loss on the data path" rather than a number whose end-to-end effect
depends on which way a packet was going. `benchmarks/README.md` says so, and
`describe()` records it beside the number.

Nothing here imports the engine: it drives `tc` through `docker exec`, the
way the rest of the harness drives the binary. The command runner is
injectable, so the tests never need a container or a capability.
"""
from __future__ import annotations

import json
import math
import shlex
import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from fractions import Fraction
from typing import Any
from urllib.parse import urlsplit

from .shaping import SHAPEABLE_BACKENDS, ShapingError

# The middlebox: the compose SERVICE name is what the engine resolves on the
# lab network, the container name is what `docker exec` addresses, and the
# published port is what a scenario's endpoint_url names.
# The driver's name as a scenario spells it, and the compose SERVICE name.
# They are the same string today and are still two different things: one is a
# key in a benchmark scenario, the other is a hostname on the lab network.
NETEM_DRIVER = "netem"
NETEM_SERVICE = "netem"
NETEM_CONTAINER = "vtop-bench-netem"
NETEM_PORT = 9200
# The calibration probe runs in its OWN container, not the engine's: the
# emulator's rate has to be measured with nothing else on the link, or the
# number validates the engine and the emulator together and neither alone.
NETEM_PROBE_CONTAINER = "vtop-bench-netem-probe"

# The ifb the upload direction is redirected onto. Created per run, which is
# also how a run CLAIMS the middlebox — see `apply`.
IFB_DEV = "ifb0"

# The middlebox's store-side network (docker-compose.benchmark.yml pins the
# subnet). The engine-facing interface is identified as the one that is NOT on
# it, rather than by name: Compose's `priority` orders attachment but the
# reference is explicit that it does NOT determine device names like `eth0`
# (review). Getting this wrong is silent and total — every qdisc would land on
# the store-facing interface, so a bottleneck calibration would probe an
# unshaped path and a policer would shape the wrong direction — so the name is
# discovered from the addresses at install time and refused if it is ambiguous.
STORE_SUBNET_PREFIX = "10.77."

# A default only for building a program in a test; production callers pass the
# discovered name.
ENGINE_IFACE = "eth0"

# netem's two loss models. `random` is independent per packet — the textbook
# model, and the wrong one for a real link. `gemodel` is Gilbert-Elliott: loss
# arrives in bursts, which is what a wireless hop or a policed uplink actually
# does, and what a loss-based control law responds to differently.
LOSS_MODELS = ("random", "gemodel")

# The mean burst length gemodel is configured for, in packets. The model's
# `r` parameter is the bad->good transition probability, so the mean run of
# lost packets is 1/r; r = 50% gives runs of two. Fixed rather than exposed:
# one more knob would need its own calibration to mean anything, and the
# scenarios that need a different burst structure can say so when they exist.
GEMODEL_MEAN_BURST_PACKETS = 2.0

# tbf needs a burst at least rate/HZ or it cannot reach its rate; a common
# kernel HZ is 250, and the floor keeps a very slow link from a burst so
# small tbf refuses it.
TBF_BURST_DIVISOR = 250
TBF_MIN_BURST_BYTES = 1600

# The ethernet MTU the delay line's byte estimate is converted at, into the
# packets netem's `limit` counts. See `netem_limit_packets`.
MTU_BYTES = 1500

# How deep into the jitter distribution the delay line is sized for, in
# standard deviations. netem's `distribution normal` draws the delay around
# the configured mean with the jitter as its sigma, so half of every draw is
# above the mean and the delay line holds more than the mean says. Three
# sigma covers all but roughly one draw in a thousand.
JITTER_HEADROOM_SIGMAS = 3

# How far out of reach the delay line's limit is set, as a multiple of what it
# can legitimately hold. See `netem_limit_packets`: the multiple is what covers
# the arrivals ABOVE the bottleneck rate that a filling or dropping queue lets
# into the delay line, measured at twice the rate on the lab's 8-flow link.
DELAY_LINE_HEADROOM = 4

# netem's own default limit, in packets. The delay line's limit never goes
# below it: a shape that asks for a bottleneck must not get a SHALLOWER delay
# line than one that asks for none.
NETEM_DEFAULT_LIMIT_PACKETS = 1000

COMMAND_TIMEOUT_SECONDS = 40.0
# The calibration probe: long enough for the bottleneck's queue to fill and
# the rate to settle, short enough that every shaped run can afford it. At a
# continental round trip most of the first couple of seconds is slow start,
# so they are OMITTED from the summary rather than averaged into it (iperf3
# -O): a rate measured across the ramp understates the link and would fail a
# perfectly good emulator.
PROBE_SECONDS = 10
PROBE_OMIT_SECONDS = 3


def _non_negative_number(value: Any, key: str) -> float:
    """A number >= 0, exactly — zero included, which is why it is not called
    "positive": an unset knob arrives as 0 and must pass, while the per-knob
    rules in `from_scenario` decide which combinations of zero contradict. `float()` alone would take True as 1.0 and a
    stray string as a silent 0, and the run would then apply and record a
    shape the scenario never asked for — the same rule the toxiproxy driver's
    `_non_negative_int` keeps, widened because a loss percentage and a buffer
    depth are genuinely fractional."""
    if isinstance(value, bool):
        raise ValueError(f"{key} must be a number, got {value!r}")
    try:
        out = float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a number, got {value!r}") from exc
    if out != out or out in (float("inf"), float("-inf")):
        raise ValueError(f"{key} must be a finite number, got {value!r}")
    if out < 0:
        raise ValueError(f"{key} must be >= 0, got {out}")
    return out


def _non_negative_int(value: Any, key: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{key} must be an integer, got {value!r}")
    out = _non_negative_number(value, key)
    if out != int(out):
        raise ValueError(f"{key} must be an integer, got {value!r}")
    return int(out)


def gemodel_params(loss_pct: float) -> tuple[float, float]:
    """Gilbert-Elliott (p, r) for a target average loss.

    With the bad state losing everything, the stationary loss rate is
    p / (p + r): the fraction of time spent in the bad state. Fixing the mean
    burst length at 1/r and solving for p gives a model whose AVERAGE loss is
    the number the scenario asked for, while its arrival pattern is bursty.
    Stated here because a gemodel row and a random row at the same
    `shaping_loss_pct` are the same average and a different link.
    """
    r = 100.0 / GEMODEL_MEAN_BURST_PACKETS
    p = r * loss_pct / (100.0 - loss_pct)
    return round(p, 4), round(r, 4)


def bdp_bytes(bottleneck_kbps: int, latency_ms: int) -> int:
    """The bandwidth-delay product of the emulated link, in bytes."""
    return int(bottleneck_kbps * 1000 / 8 * (latency_ms / 1000.0))


@dataclass(frozen=True)
class NetemShape:
    """The shape a netem scenario asks for, validated at load."""

    latency_ms: int = 0
    jitter_ms: int = 0
    loss_pct: float = 0.0
    loss_model: str = "random"
    bottleneck_kbps: int = 0
    buffer_bdp: float = 0.0
    policer_kbps: int = 0
    container: str = NETEM_CONTAINER
    run_token: str = "run"

    driver = NETEM_DRIVER

    @classmethod
    def from_scenario(cls, scenario) -> NetemShape:
        latency = _non_negative_int(
            scenario.get("shaping_latency_ms", 0), "shaping_latency_ms")
        jitter = _non_negative_int(
            scenario.get("shaping_jitter_ms", 0), "shaping_jitter_ms")
        loss = _non_negative_number(scenario.get("shaping_loss_pct", 0), "shaping_loss_pct")
        model = str(scenario.get("shaping_loss_model", "random") or "random").strip()
        bottleneck = _non_negative_int(
            scenario.get("shaping_bottleneck_kbps", 0), "shaping_bottleneck_kbps")
        buffer_bdp = _non_negative_number(
            scenario.get("shaping_buffer_bdp", 0), "shaping_buffer_bdp")
        policer = _non_negative_int(
            scenario.get("shaping_policer_kbps", 0), "shaping_policer_kbps")

        if model not in LOSS_MODELS:
            raise ValueError(
                f"shaping_loss_model {model!r} is not one of {list(LOSS_MODELS)}")
        if loss >= 100.0:
            raise ValueError(
                f"shaping_loss_pct {loss} drops the whole link; a run through it "
                "measures nothing")
        if not (latency or loss or bottleneck or policer):
            raise ValueError(
                "shaping_driver is netem but no delay, loss, bottleneck or policer is "
                "set: a shaped run with no shape is an unshaped run filed under the "
                "wrong name")
        if jitter and not latency:
            raise ValueError("shaping_jitter_ms needs a shaping_latency_ms to spread")
        if model != "random" and not loss:
            raise ValueError(
                f"shaping_loss_model is {model!r} but shaping_loss_pct is 0: the model "
                "names how loss ARRIVES, and no loss arrives")
        if buffer_bdp and not bottleneck:
            raise ValueError(
                "shaping_buffer_bdp needs a shaping_bottleneck_kbps: a buffer depth is "
                "a multiple of the bandwidth-delay product, and there is no bandwidth")
        if buffer_bdp and not latency:
            raise ValueError(
                "shaping_buffer_bdp needs a shaping_latency_ms: the delay half of the "
                "bandwidth-delay product would be zero, and the buffer with it")
        if bottleneck and not buffer_bdp:
            raise ValueError(
                "shaping_bottleneck_kbps needs a shaping_buffer_bdp: tbf's queue IS the "
                "bottleneck buffer, and leaving it to a default would file a number "
                "against a buffer depth nobody chose")

        from .shaping import new_run_token
        return cls(latency_ms=latency, jitter_ms=jitter, loss_pct=loss, loss_model=model,
                   bottleneck_kbps=bottleneck, buffer_bdp=buffer_bdp, policer_kbps=policer,
                   run_token=new_run_token())

    # ---------------------------------------------------------------- shape

    def buffer_bytes(self) -> int:
        """tbf's queue: the bottleneck buffer, in BDP multiples. Floored at
        one full-size packet, because a queue that cannot hold a segment is a
        dropper, not a buffer."""
        return max(1500, int(bdp_bytes(self.bottleneck_kbps, self.latency_ms) * self.buffer_bdp))

    def burst_bytes(self) -> int:
        return max(TBF_MIN_BURST_BYTES,
                   int(self.bottleneck_kbps * 1000 / 8 / TBF_BURST_DIVISOR))

    def rate_calibration_twin(self) -> NetemShape:
        """This shape with the impairments removed, for the rate gate.

        Same bottleneck, same buffer, same mean delay — the link whose RATE is
        being calibrated — with loss, the policer and the JITTER dropped,
        because each suppresses TCP goodput below the configured rate by
        design, and a gate that measured through them would refuse a correctly
        built link.

        Jitter joined the list with the layout that made the buffer honest
        (review). netem's normally distributed delay reorders packets, TCP
        reads the reordering as loss, and on a queue that is really one BDP deep
        the flow backs off: measured live at 10 Mbit/s, 100 ms and 20 ms of
        jitter, a single flow received 8.91-9.15 Mbit/s at 1 BDP against
        9.34-9.62 at 3.5-4 BDP and 9.51-9.61 with no jitter — the bucket
        delivering its rate every time, and the gate's 9.0 floor sitting inside
        the spread. The old layout never showed it because its real queue, at
        GSO-sized packets, was several times the one it recorded.
        """
        return replace(self, loss_pct=0.0, loss_model="random", policer_kbps=0, jitter_ms=0)

    def netem_limit_packets(self) -> int:
        """The delay line's packet limit — deliberately NOT the bottleneck
        buffer, which is tbf's byte `limit` and nothing else.

        WHY THE TWO QUEUES ARE SEPARATE QDISCS (review, the fourth round on this
        one number). The upload used to be tbf with netem as its child. A
        classless child REPLACES tbf's internal bfifo, so netem's packet `limit`
        became the only queue limit there was — and it bounded everything netem
        held: the congestion backlog, and every packet still riding out its
        delay. So the limit had to be the buffer PLUS a reserve for the delay
        line, and a reserve is sized for the worst case. Whenever the line held
        less than that — below saturation, at the start of a flow, on every
        jitter draw short of three sigma — congestion backlog spent the slack:
        at 10 Mbit/s, 100 ms, 1 BDP and 20 ms of jitter, 37.5 kB (0.3 BDP) of
        queue no column records. No reserve can fix that, because the mean
        occupancy has the same slack in smaller form. And it was not a byte
        limit at all: measured live on the lab's own path, the skbs arriving
        from a TSO sender are GSO aggregates of ~3 kB, so a limit of 125
        "MTU-sized" packets held 381 kB.

        So the upload is now netem at the ROOT, with tbf as its child. netem
        holds each packet in its time-ordered delay line until it is due and
        only then hands it to tbf, whose own bfifo keeps tbf's byte `limit`:
        the congestion queue is enforced by itself, in bytes, and jitter cannot
        lend it anything. The packet timing is unchanged — in both layouts a
        packet's delay starts when it arrives and its wait for tokens starts
        when the delay has run out — and so is where loss applies: on netem's
        enqueue, before either queue.

        THIS limit therefore bounds only the delay line, which on a real link is
        a length of wire and has no capacity to exceed. It is not a queue the
        emulated link has, so it is set out of reach rather than to a value:
        `DELAY_LINE_HEADROOM` times what the line and the congestion queue could
        hold together at the MTU, floored at netem's own default. The buffer is
        in the sum for two reasons: a queue that is filling admits arrivals
        above the bottleneck rate into the delay line — measured at about twice
        `rate x delay` at peak — and some kernels count the child's packets
        against the parent's limit. A limit this far out of reach costs only
        memory the traffic never uses; the one that is too small drops packets
        the emulated link never lost.

        Measured on a live middlebox rig (8 TCP flows through the ingress, ifb
        and qdiscs this program installs, 10 Mbit/s, 100 ms): tbf's backlog
        peaked at 124,214 bytes of a 125,000-byte limit at 1 BDP and 499,950 of
        500,000 at 4 BDP, with 0 and 20 ms of jitter alike; every drop was
        tbf's, none netem's; and the delay line peaked at 48 packets (130 kB)
        under limits of 1000-1600.
        """
        reachable = self.delay_occupancy_bytes() + self.buffer_bytes()
        return max(NETEM_DEFAULT_LIMIT_PACKETS,
                   -(-DELAY_LINE_HEADROOM * reachable // MTU_BYTES))

    def delay_occupancy_bytes(self) -> int:
        """Bytes in flight inside the upload delay line at the bottleneck rate,
        at the deep end of its jitter: `rate x (upload delay + 3 sigma)`.

        Not buffer: this is data the link is carrying, not data queued behind a
        full link. It sizes the delay line's limit, and nothing about the
        congestion queue depends on it any more — tbf's byte limit is the
        buffer, whatever the delay line holds. Rounded up and computed exactly
        (`kbps x held_ms / 8` bytes, the kilo and the milli cancelling) because
        that is the harmless direction for a limit, and because an earlier
        round's rounding error is not one worth reintroducing where it can no
        longer bite.
        """
        held_ms = self.upload_delay_ms() + JITTER_HEADROOM_SIGMAS * self._upload_jitter_ms()
        return math.ceil(Fraction(self.bottleneck_kbps * held_ms) / 8)

    def _netem_args(self, delay_ms: int, jitter_ms: int, with_loss: bool,
                    limit_packets: int | None = None) -> list[str]:
        args = ["netem"]
        if limit_packets is not None:
            args += ["limit", str(limit_packets)]
        if delay_ms or jitter_ms:
            args += ["delay", f"{delay_ms}ms"]
            if jitter_ms:
                # A distribution, not a square wave: netem's default jitter is
                # uniform, and a real path's delay variation is not.
                args += [f"{jitter_ms}ms", "distribution", "normal"]
        if with_loss and self.loss_pct:
            if self.loss_model == "gemodel":
                p, r = gemodel_params(self.loss_pct)
                args += ["loss", "gemodel", f"{p}%", f"{r}%"]
            else:
                args += ["loss", "random", f"{self.loss_pct}%"]
        return args

    def upload_delay_ms(self) -> int:
        """Half the round trip, with the odd millisecond on the upload — the
        direction the data goes, matching the toxiproxy driver's split."""
        half, odd = divmod(self.latency_ms, 2)
        return half + odd

    def download_delay_ms(self) -> int:
        return self.latency_ms // 2

    def _upload_jitter_ms(self) -> int:
        half, odd = divmod(self.jitter_ms, 2)
        return half + odd

    def tc_program(self, engine_iface: str = ENGINE_IFACE) -> list[list[str]]:
        """Every `tc`/`ip` command that installs this shape, in order.

        Returned as data rather than run here so the whole program is a unit
        test's subject: a shape is a string of tc arguments, and the way that
        string goes wrong is by being built wrong, not by the kernel refusing
        a correct one.
        """
        cmds: list[list[str]] = [
            # The ifb the upload direction is mirrored onto. Created per run
            # and removed after, so a leftover device from an interrupted run
            # is visible as an error rather than silently reused.
            ["ip", "link", "add", IFB_DEV, "type", "ifb"],
            ["ip", "link", "set", IFB_DEV, "up"],
        ]

        # UPLOAD: netem (the delay and the loss) at the root, with tbf (the
        # bottleneck and its buffer) as its child. Two qdiscs, two queues: the
        # delay line is netem's and the congestion queue is tbf's own byte-limited
        # bfifo, so `shaping_buffer_bdp` is enforced exactly and nothing the delay
        # line leaves unused can be borrowed by it — see `netem_limit_packets`.
        # Without a tbf there is no configured buffer to hold, so netem keeps its
        # own default (#516 is that gap).
        upload_netem = self._netem_args(
            self.upload_delay_ms(), self._upload_jitter_ms(), with_loss=True,
            limit_packets=self.netem_limit_packets() if self.bottleneck_kbps else None)
        bottleneck = ["tbf", "rate", f"{self.bottleneck_kbps}kbit",
                      "burst", str(self.burst_bytes()), "limit", str(self.buffer_bytes())]
        if len(upload_netem) > 1:
            cmds.append(["tc", "qdisc", "add", "dev", IFB_DEV, "root", "handle", "1:"]
                        + upload_netem)
            if self.bottleneck_kbps:
                cmds.append(["tc", "qdisc", "add", "dev", IFB_DEV, "parent", "1:1",
                             "handle", "10:"] + bottleneck)
        elif self.bottleneck_kbps:
            # Unreachable from a validated scenario — a bottleneck needs a buffer
            # and a buffer needs a latency — but a shape built directly still
            # gets its bottleneck rather than silently none.
            cmds.append(["tc", "qdisc", "add", "dev", IFB_DEV, "root", "handle", "1:"]
                        + bottleneck)

        # The ingress hook on the engine-facing interface: the policer first
        # (it drops above its rate and queues nothing), then the redirect onto
        # the ifb where the shaping lives.
        cmds.append(["tc", "qdisc", "add", "dev", engine_iface, "handle", "ffff:", "ingress"])
        filter_cmd = ["tc", "filter", "add", "dev", engine_iface, "parent", "ffff:",
                      "protocol", "all", "prio", "1", "u32", "match", "u32", "0", "0"]
        if self.policer_kbps:
            filter_cmd += ["action", "police", "rate", f"{self.policer_kbps}kbit",
                           "burst", str(max(TBF_MIN_BURST_BYTES,
                                            int(self.policer_kbps * 1000 / 8 / TBF_BURST_DIVISOR))),
                           # exceed -> drop, conform -> keep going to the
                           # redirect. A policer has no queue, which is the
                           # whole point of asking for one.
                           "conform-exceed", "drop/pipe"]
        filter_cmd += ["action", "mirred", "egress", "redirect", "dev", IFB_DEV]
        cmds.append(filter_cmd)

        # DOWNLOAD: the other half of the delay, on the way back to the
        # engine. No loss and no rate — the data direction carries those, and
        # a number is easier to read when only one direction is impaired.
        download_netem = self._netem_args(self.download_delay_ms(), self.jitter_ms // 2,
                                          with_loss=False)
        if len(download_netem) > 1:
            cmds.append(["tc", "qdisc", "add", "dev", engine_iface, "root", "handle", "2:"]
                        + download_netem)
        return cmds

    def teardown_program(self, engine_iface: str = ENGINE_IFACE) -> list[list[str]]:
        """Removing the shape, in the reverse order it was built.

        MIRRORS `tc_program`: only the pieces that shape actually installs are
        removed. A blanket teardown looked harmless — a missing qdisc is not an
        error, surely — but `tc` disagrees, and differently per case: deleting
        an absent root gives "Cannot delete qdisc with handle of zero", an
        absent ingress gives "Cannot find specified qdisc on specified device",
        and only an absent device gives the "Cannot find device" that `_absent`
        was originally written around. A pure-loss shape (no latency, which is
        this driver's headline capability) installs no root on the engine
        interface, so a blanket teardown reported a failure at the end of a
        perfectly good run and `shaped()` raised out of its own `finally`.
        Every command is still attempted whatever the previous one did — a
        qdisc left behind shapes the next run without saying so.
        """
        cmds: list[list[str]] = []
        if len(self._netem_args(self.download_delay_ms(), self.jitter_ms // 2,
                                with_loss=False)) > 1:
            cmds.append(["tc", "qdisc", "del", "dev", engine_iface, "root"])
        cmds.append(["tc", "qdisc", "del", "dev", engine_iface, "ingress"])
        if self.bottleneck_kbps or len(
                self._netem_args(self.upload_delay_ms(), self._upload_jitter_ms(),
                                 with_loss=True)) > 1:
            cmds.append(["tc", "qdisc", "del", "dev", IFB_DEV, "root"])
        cmds.append(["ip", "link", "del", IFB_DEV])
        return cmds

    # --------------------------------------------------------------- record

    def describe(self) -> dict[str, Any]:
        """What the summary records, units included."""
        return {
            "driver": self.driver,
            "container": self.container,
            "latency_ms": self.latency_ms,
            "jitter_ms": self.jitter_ms,
            "loss_pct": self.loss_pct,
            "loss_model": self.loss_model,
            "bottleneck_kbps": self.bottleneck_kbps,
            "buffer_bdp": self.buffer_bdp,
            "buffer_bytes": self.buffer_bytes() if self.bottleneck_kbps else 0,
            "policer_kbps": self.policer_kbps,
            # The rate keys are KILOBITS here and KILOBYTES on the toxiproxy
            # driver; the unit travels with the number so the two are never
            # read as one.
            "rate_unit": "kilobits_per_second",
            "scope": "aggregate_l3",
            "loss_direction": "upload",
            "run_token": self.run_token,
        }

    def flat_columns(self) -> dict[str, Any]:
        return {
            "shaping_driver": self.driver,
            "shaping_proxy": "",
            "shaping_bandwidth_kbps": "",
            "shaping_latency_ms": self.latency_ms,
            "shaping_jitter_ms": self.jitter_ms,
            "shaping_scope": "aggregate_l3",
            "shaping_loss_pct": self.loss_pct,
            "shaping_loss_model": self.loss_model,
            "shaping_bottleneck_kbps": self.bottleneck_kbps,
            "shaping_buffer_bdp": self.buffer_bdp,
            "shaping_policer_kbps": self.policer_kbps,
        }


# ------------------------------------------------------------------ running


def docker_exec(container: str) -> Callable[[list[str]], tuple[int, str]]:
    """Run a command in the middlebox. Injectable so the tests never need a
    container, a capability, or a kernel module."""

    def run(argv: list[str]) -> tuple[int, str]:
        try:
            proc = subprocess.run(["docker", "exec", container, *argv],
                                  capture_output=True, text=True,
                                  timeout=COMMAND_TIMEOUT_SECONDS)
        except FileNotFoundError as exc:
            raise ShapingError(
                "docker is not on PATH, so the netem middlebox cannot be driven: "
                "a netem-shaped scenario needs the lab stack") from exc
        except subprocess.TimeoutExpired as exc:
            raise ShapingError(
                f"`{shlex.join(argv)}` in {container} did not finish in "
                f"{COMMAND_TIMEOUT_SECONDS}s") from exc
        out = (proc.stdout or "") + (proc.stderr or "")
        return proc.returncode, out.strip()

    return run


Runner = Callable[[list[str]], tuple[int, str]]


def resolve_engine_iface(run: Runner) -> str:
    """Which interface faces the ENGINE, discovered rather than assumed.

    Every qdisc hangs off this one, so naming it wrongly is silent and total:
    the shaping would land on the store-facing interface, a bottleneck
    calibration would probe an unshaped path, and a policer would meter the
    wrong direction — all while the summary recorded the requested shape.
    Compose's `priority` orders attachment but its reference is explicit that
    it does not determine device names, so `eth0` is a guess (review).

    The middlebox has exactly two addressed interfaces; the store-facing one is
    on the compose file's pinned store subnet, so the other is the engine's.
    Anything but exactly one candidate is refused, naming what was seen.
    """
    code, out = run(["ip", "-o", "-4", "addr", "show"])
    if code != 0:
        raise ShapingError(
            f"could not read the middlebox's interfaces (exit {code}): {out}. Without them "
            "the shaping cannot be placed, and placing it by guess would shape the wrong "
            "direction while the summary recorded the right one")
    candidates = []
    for line in out.splitlines():
        fields = line.split()
        # `N: <iface>    inet <addr>/<len> ...`
        if len(fields) < 4 or fields[2] != "inet":
            continue
        iface, addr = fields[1], fields[3]
        if iface == "lo" or addr.startswith(STORE_SUBNET_PREFIX):
            continue
        candidates.append(iface)
    unique = sorted(set(candidates))
    if len(unique) != 1:
        raise ShapingError(
            f"expected exactly one engine-facing interface on the middlebox (an addressed "
            f"interface outside the store subnet {STORE_SUBNET_PREFIX}0.0/24), found "
            f"{unique or 'none'} in:\n{out}\nThe shaping cannot be placed without knowing "
            "which side the engine is on")
    return unique[0]


def _installed_qdiscs(run: Runner, engine_iface: str) -> list[str]:
    """Any qdisc this driver installs that is ALREADY on the middlebox.

    The toxiproxy driver refuses a proxy carrying somebody's toxics; the same
    rule holds here, for the same reason. A leftover qdisc from an interrupted
    run would shape this one on top of its own shape while the summary said
    otherwise, and nothing is deleted or stacked on: one run cannot tell a
    live neighbour from a stale one.
    """
    found = []
    code, out = run(["tc", "qdisc", "show", "dev", engine_iface])
    if code == 0:
        for line in out.splitlines():
            fields = line.split()
            # `qdisc <kind> <handle> root|ingress ...`; the kernel's own
            # default root (noqueue/pfifo_fast/mq) is not ours.
            if len(fields) > 1 and fields[1] not in ("noqueue", "pfifo_fast", "mq", "fq_codel"):
                found.append(f"{engine_iface}: {line.strip()}")
    return found


def apply(shape: NetemShape, run: Runner) -> str:
    """Claim the middlebox, check it, install the shape. Returns the interface.

    The CLAIM is `ip link add ifb0`, and it is first because the kernel makes
    it atomic: a second `add` of an existing device fails, so of two runs
    racing for one middlebox exactly one wins and the other is refused before
    it has touched a qdisc. The read-then-create it replaces was a check with a
    window in it (review) — both runs could find the box clean, and the loser's
    rollback would then tear down the WINNER's qdiscs, leaving the winner
    running unshaped while recording netem. The same reasoning as the
    toxiproxy driver's lock toxic, one layer down.
    """
    engine_iface = resolve_engine_iface(run)
    code, out = run(["ip", "link", "add", IFB_DEV, "type", "ifb"])
    if code != 0:
        raise ShapingError(
            f"the netem middlebox {shape.container} is claimed: {IFB_DEV} already exists "
            f"({out}). Another shaped run holds it, or one was interrupted before it cleaned "
            "up; nothing is removed here, because this run cannot tell a live neighbour from "
            f"a stale leftover. Wait for it, or clear it by hand — docker exec "
            f"{shape.container} ip link del {IFB_DEV}")
    # Claimed. Every refusal from here tears the claim down on the way out.
    present = _installed_qdiscs(run, engine_iface)
    if present:
        # Release ONLY the claim — never the qdiscs that are already there.
        # They belong to whoever installed them, and a full teardown here would
        # delete a neighbour's shaping and leave THEIR run measuring an
        # unshaped link while recording netem (review: a rollback must remove
        # only what this run conclusively owns).
        run(["ip", "link", "del", IFB_DEV])
        raise ShapingError(
            f"the netem middlebox {shape.container} is already shaped ({'; '.join(present)}): "
            "an interrupted run left qdiscs behind. They would shape this run on top of its "
            "own shape and the summary would not say so, so nothing is stacked on them. "
            f"Remove them by hand — docker exec {shape.container} tc qdisc del dev "
            f"{engine_iface} root — or restart the netem stack")
    done: list[list[str]] = []
    for cmd in shape.tc_program(engine_iface)[1:]:
        code, out = run(cmd)
        if code != 0:
            # Undo what this call installed before reporting: a half-installed
            # shape is the one thing worse than none, because it is a shape
            # nobody chose and the summary would name the one that was asked
            # for. The rollback's OWN result is carried into the message
            # (review): a middlebox that just refused a command is exactly the
            # one whose teardown may also be refused, and claiming a clean box
            # that nothing checked is how the next run inherits a shape.
            stayed = _teardown(shape, run, engine_iface)
            rolled_back = (f"The {len(done)} command(s) installed before it were removed again"
                           if not stayed else
                           f"AND THE ROLLBACK DID NOT FINISH: {', '.join(stayed)} — the "
                           "middlebox is still partly shaped and the next run will be refused "
                           "until it is cleared by hand")
            raise ShapingError(
                f"the middlebox refused `{shlex.join(cmd)}` (exit {code}): {out}. "
                f"{rolled_back}")
        done.append(cmd)
    return engine_iface


def _teardown(shape: NetemShape, run: Runner, engine_iface: str) -> list[str]:
    """Attempt every removal; return the ones that stayed.

    Split out from `clear` so the rollback inside `apply` can report what it
    actually achieved rather than assuming it achieved everything.
    """
    stayed = []
    for cmd in shape.teardown_program(engine_iface):
        try:
            code, out = run(cmd)
        except ShapingError as exc:
            stayed.append(f"{shlex.join(cmd)} ({exc})")
            continue
        # An absence is the normal case for a piece this shape never installed
        # or for a teardown running twice; see _ABSENT_PHRASES.
        if code != 0 and not _absent(out):
            stayed.append(f"{shlex.join(cmd)} (exit {code}: {out})")
    return stayed


def clear(shape: NetemShape, run: Runner, engine_iface: str,
          best_effort: bool = False) -> None:
    """Remove this run's shape. Every command is tried whatever the previous
    one did; an absent qdisc is fine, a removal that FAILS is reported after
    every one was attempted."""
    stayed = _teardown(shape, run, engine_iface)
    if stayed and not best_effort:
        raise ShapingError(
            f"the netem middlebox did not unshape: {', '.join(stayed)}. The pipe is still "
            "shaped — remove the qdiscs by hand or restart the netem stack")


# What `tc` and `ip` say when the thing being removed was never there. Three
# different sentences for three different absences, and only the first was
# covered when this was written against `ip link del` alone (review):
#
#   tc qdisc del dev X root      -> "Cannot delete qdisc with handle of zero"
#   tc qdisc del dev X ingress   -> "Cannot find specified qdisc on specified device"
#   ip link del ifb0             -> 'Cannot find device "ifb0"'
#
# Matched as substrings and lowercased, because the wording has already moved
# once: older iproute2 answered the qdisc cases with "Invalid argument", which
# is still listed so a teardown on an older host is not reported as a failure.
_ABSENT_PHRASES = (
    "cannot find device",
    "cannot delete qdisc with handle of zero",
    "cannot find specified qdisc",
    "no such file",
    "invalid argument",
    "does not exist",
)


def _absent(out: str) -> bool:
    low = out.lower()
    return any(phrase in low for phrase in _ABSENT_PHRASES)


# -------------------------------------------------------------- calibration


class CalibrationError(ShapingError):
    """The emulator did not produce the link it was configured for.

    Its own class because it is a different failure from "the shape would not
    install": the shape installed, and it does not bite the way it was asked
    to. A run through it would file numbers against a link that does not
    exist.
    """


def probe_throughput_mbps(run: Runner, server: str, seconds: int = PROBE_SECONDS,
                          omit: int = PROBE_OMIT_SECONDS) -> float:
    """iperf3 through the shaped path, alone: megabits per second.

    Run from the probe container, so the number is the EMULATOR's, measured
    with no VTOP traffic beside it.

    The RECEIVED rate, not the sent one. iperf3's `sum_sent` counts what the
    client handed to its socket, and at the far end of a token bucket that
    includes everything still sitting in the bottleneck's queue — a 100 ms
    buffer on a 10 Mbit/s link is 125 kB of bytes the sender has "sent" and
    the link has not carried. Measured that way a correctly shaped link reads
    about 20% fast, which is a calibration failure invented by the
    measurement. `sum_received` is what actually crossed, which is the thing
    being calibrated.
    """
    code, out = run(["iperf3", "-c", server, "-t", str(seconds), "-O", str(omit), "-J"])
    if code != 0:
        raise CalibrationError(
            f"the calibration probe could not reach the middlebox at {server} (exit {code}): "
            f"{out}. Start the netem stack — docker compose -f "
            "benchmarks/docker-compose.benchmark.yml --profile netem up -d")
    try:
        report = json.loads(out)
        end = report["end"]
        # sum_received is the calibration subject; sum_sent is the fallback
        # only because a future iperf3 mode might not report the receiver's
        # view, and a probe that cannot read its own report should say so
        # rather than silently pick the wrong number.
        summary = end.get("sum_received") or end["sum_sent"]
        bits = float(summary["bits_per_second"])
    except (ValueError, KeyError, TypeError) as exc:
        raise CalibrationError(
            f"the calibration probe's output was not an iperf3 JSON report: {out[:400]}"
        ) from exc
    return round(bits / 1e6, 3)


# How far the measured link may sit from the configured one before the run is
# refused. netem is bounded by kernel timer granularity and its accuracy
# degrades as the rate climbs; 10% is the issue's band, and a run outside it
# is a broken emulator rather than a slow engine.
CALIBRATION_TOLERANCE = 0.10


def check_calibration(shape: NetemShape, measured_mbps: float) -> None:
    if not shape.bottleneck_kbps:
        return
    configured = shape.bottleneck_kbps / 1000.0
    low = configured * (1 - CALIBRATION_TOLERANCE)
    high = configured * (1 + CALIBRATION_TOLERANCE)
    if not (low <= measured_mbps <= high):
        raise CalibrationError(
            f"the emulator measured {measured_mbps} Mbit/s where it was configured for "
            f"{configured} Mbit/s, outside the ±{int(CALIBRATION_TOLERANCE * 100)}% band "
            f"({low:.3f}–{high:.3f}). An emulator that does not produce the link it was "
            "asked for files numbers against a link that does not exist; this run is "
            "refused rather than recorded")


def calibrate(shape: NetemShape, run: Runner | None = None,
              probe: Runner | None = None,
              log: Callable[[str], None] = print) -> float:
    """Install the shape, measure it alone with iperf3, tear it down again.

    A PREFLIGHT, before the seed data exists: an emulator that is not
    producing the link it was configured for should fail the run before it
    costs anything, and the number it did produce belongs in the summary
    beside every result the run goes on to file. The shape is removed again
    so the measured block installs its own — the program is identical, and a
    calibration that left the pipe shaped would be indistinguishable from an
    interrupted run's leftovers to the very check that guards against them.
    """
    run = run or docker_exec(shape.container)
    probe = probe or docker_exec(NETEM_PROBE_CONTAINER)
    # The rate gate is measured on the shape's IMPAIRMENT-FREE twin (review).
    # Calibrating through the configured loss measures what a loss-based
    # control law achieves, not what the token bucket delivers: scenario 14's
    # own reasoning puts one flow at 2-3 Mbit/s on its 10 Mbit/s link, so
    # gating that against 9-11 would refuse a correctly built link and the
    # headline lossy benchmark could never start at all. The policer is dropped
    # for the same reason — it holds no queue, so TCP through one settles well
    # below its rate BY DESIGN. What remains is exactly what the gate asks:
    # does this bottleneck deliver the rate it was configured for.
    with shaped(shape.rate_calibration_twin(), run=run, log=lambda _m: None):
        measured = probe_throughput_mbps(probe, NETEM_SERVICE)
    log(f"[bench] emulator calibration: {measured} Mbit/s through the shaped path"
        + (f" (configured {shape.bottleneck_kbps / 1000.0} Mbit/s)"
           if shape.bottleneck_kbps else " (no bottleneck configured)"))
    check_calibration(shape, measured)
    return measured


# ------------------------------------------------------------------ the gate


def require_endpoint_reaches_the_middlebox(scenario, effective_endpoint: str) -> None:
    """A netem-shaped run must go THROUGH the middlebox, and from inside the
    lab's network.

    The middlebox has nothing to sit between while the engine is a host
    process (#476 is this issue's dependency), and an endpoint naming the
    store directly would go around every qdisc while the summary recorded a
    shape.
    """
    backend = str(scenario.get("backend", "") or "")
    if backend not in SHAPEABLE_BACKENDS:
        raise ShapingError(
            f"backend {backend!r} never dials endpoint_url, so a shape on it would measure "
            f"nothing through the pipe and record a shape anyway; a shaped scenario needs "
            f"one of {list(SHAPEABLE_BACKENDS)}")
    mode = str(scenario.get("runner_mode", "host") or "host")
    if mode != "container":
        raise ShapingError(
            "a netem-shaped scenario needs runner_mode: container. The middlebox sits in the "
            "lab network's L3 path, and a host process reaches the store without crossing it "
            "— the run would be unshaped and the summary would say netem")
    declared = str(scenario.get("endpoint_url", "") or "")
    if effective_endpoint != declared:
        raise ShapingError(
            f"the effective endpoint {effective_endpoint!r} is not the scenario's {declared!r}: "
            "a shaped run must go through the middlebox, and VTOP_S3_ENDPOINT_URL is sending "
            "the engine around it — unset it, or point it at the middlebox")
    # The HOST and the PORT, parsed — not a substring of the URL. `netem` as a
    # substring also matches `netem-staging`, a path component, or a query
    # parameter, and each of those is a different machine that would be handed
    # this run while the summary recorded a shaped one.
    try:
        parts = urlsplit(effective_endpoint)
        host, port = parts.hostname, parts.port
    except ValueError:
        host, port = None, None
    if host != NETEM_SERVICE or port != NETEM_PORT:
        raise ShapingError(
            f"the endpoint {effective_endpoint!r} is not the netem middlebox "
            f"({NETEM_SERVICE}:{NETEM_PORT}) — it resolves to host {host!r} port {port!r}: "
            "the engine would reach the store without crossing the qdiscs, and the summary "
            f"would record a shape nothing applied. A netem scenario's endpoint_url is "
            f"http://{NETEM_SERVICE}:{NETEM_PORT}")


@contextmanager
def shaped(shape: NetemShape, run: Runner | None = None,
           log: Callable[[str], None] = print) -> Iterator[NetemShape]:
    """Shape the middlebox for the duration of the block, and unshape it after
    — on a failed run, on a keyboard interrupt, on any way out."""
    run = run or docker_exec(shape.container)
    engine_iface = apply(shape, run)
    log(f"[bench] netem on {shape.container}: {shape.latency_ms} ms round trip "
        f"±{shape.jitter_ms} ms, {shape.loss_pct}% loss ({shape.loss_model}), "
        f"{shape.bottleneck_kbps or 'unlimited'} kbit/s bottleneck"
        + (f" at {shape.buffer_bdp}x BDP" if shape.bottleneck_kbps else "")
        + (f", policed at {shape.policer_kbps} kbit/s" if shape.policer_kbps else ""))
    try:
        yield shape
    finally:
        clear(shape, run, engine_iface)
        log(f"[bench] netem removed from {shape.container}")
