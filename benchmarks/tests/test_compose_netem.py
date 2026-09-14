"""The netem middlebox's place in the lab's compose file (#477).

The middlebox is the one container in this lab that holds a capability, and
that is the whole reason to lint it: a privilege added for one service has a
way of spreading to its neighbours, and the neighbours here are the very
services whose measurements the capability exists to shape. These tests read
the compose file itself rather than a rendered copy, so they hold whether or
not docker is installed and whether or not PyYAML is.

The reader below is deliberately small and deliberately strict: it understands
exactly the shape this file is written in, and it FAILS rather than returning
an empty answer when it cannot find what it expects. A lint that quietly finds
no services would pass every assertion in this module while checking nothing.
"""
from __future__ import annotations

import os
import re

import pytest

COMPOSE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "docker-compose.benchmark.yml")

SERVICE_RE = re.compile(r"^  ([a-z][a-z0-9_-]*):\s*$")
TOP_LEVEL_RE = re.compile(r"^([a-z][a-z0-9_-]*):\s*$")


def read_services() -> dict[str, list[str]]:
    """Every service in the compose file, mapped to its own lines.

    Raises rather than returning nothing: an empty result would make each
    assertion below vacuously true.
    """
    with open(COMPOSE) as fh:
        lines = fh.read().splitlines()
    services: dict[str, list[str]] = {}
    section = None
    current = None
    for line in lines:
        top = TOP_LEVEL_RE.match(line)
        if top:
            section = top.group(1)
            current = None
            continue
        if section != "services":
            continue
        found = SERVICE_RE.match(line)
        if found:
            current = found.group(1)
            services[current] = []
            continue
        if current is not None:
            services[current].append(line)
    if not services:
        raise AssertionError(
            f"{COMPOSE} parsed to no services at all: this lint's reader no longer "
            "understands the file's shape, and every assertion below would pass "
            "without checking anything")
    return services


def value_of(body: list[str], key: str) -> str | None:
    for line in body:
        stripped = line.strip()
        if stripped.startswith(f"{key}:"):
            return stripped.split(":", 1)[1].strip()
    return None


def test_the_middlebox_is_the_only_service_that_holds_a_capability():
    # The privilege is the point of the review (#477): tc, ifb and the
    # forwarding rules need NET_ADMIN, and nothing else in this lab may
    # acquire it as a side effect of the middlebox existing. If a second
    # service ever needs it, that is a decision someone should have to make
    # in front of this assertion.
    services = read_services()
    with_cap_add = {name for name, body in services.items() if value_of(body, "cap_add")}
    assert with_cap_add == {"netem"}, (
        f"exactly one service may hold a capability in the benchmark lab; found "
        f"{sorted(with_cap_add)}. A shaped measurement is only trustworthy while the "
        "shaping is confined to the box that does the shaping")
    assert "NET_ADMIN" in (value_of(services["netem"], "cap_add") or ""), (
        "the middlebox needs NET_ADMIN or it cannot install a qdisc, and a netem run "
        "would then measure an unshaped link under a shaped name")


def test_the_engine_and_the_store_still_drop_every_capability():
    # The two services whose numbers the lab produces. Their hardening is
    # what makes the middlebox's capability a bounded exception rather than
    # a loosening of the lab.
    services = read_services()
    for name in ("vtop-engine", "minio", "netem-probe"):
        assert name in services, f"{name} is not in the benchmark compose file any more"
        assert value_of(services[name], "cap_drop") == "[ALL]", (
            f"{name} must keep cap_drop: [ALL]; the middlebox exists precisely so that "
            "the measured services never need a capability of their own")
        assert value_of(services[name], "cap_add") is None, (
            f"{name} has gained a capability: the shaping belongs on the middlebox, and "
            "a capability here is either unnecessary or a hole")


def test_the_netem_services_are_behind_their_own_profile():
    # An unshaped scenario must not pay for the middlebox, and — more to the
    # point — must not have a forwarding box with NET_ADMIN sitting in its
    # lab at all.
    services = read_services()
    for name in ("netem", "netem-probe"):
        assert value_of(services[name], "profiles") == '["netem"]', (
            f"{name} must stay on the netem profile, so a lab brought up for an unshaped "
            "run holds no privileged container")


def test_the_middlebox_publishes_no_port():
    # Docker's userland proxy TERMINATES TCP for a published port. Publishing
    # one here would put a second TCP-terminating hop into a path whose entire
    # purpose is to be L3 — the same defect that makes toxiproxy unable to
    # answer this milestone's question. lib/netem.py refuses a host-mode
    # scenario for the same reason; this keeps the compose file from quietly
    # offering the bypass that refusal exists to prevent.
    services = read_services()
    assert value_of(services["netem"], "ports") is None, (
        "the netem middlebox must not publish a port: a published port is reached "
        "through docker's userland TCP proxy, and a run through it is not the L3 path "
        "the qdiscs shape")


def test_the_store_is_reachable_on_the_middlebox_side_at_a_fixed_address():
    # The middlebox DNATs to a literal address rather than to a name (see the
    # compose comment): a host on two networks answers its own name two ways,
    # and a middlebox forwarding to the wrong one measures the wrong hop.
    services = read_services()
    body = "\n".join(services["minio"])
    assert "netem-store" in body and "10.77.0.10" in body, (
        "MinIO must carry a fixed address on the middlebox's store-side network, or the "
        "DNAT target in the middlebox's start-up rules names nothing")
    netem_body = "\n".join(services["netem"])
    assert "10.77.0.10:9000" in netem_body, (
        "the middlebox's DNAT rule must name the store's store-side address; if the two "
        "drift apart the forwarded connection lands somewhere nobody chose")


def test_the_middlebox_endpoint_is_recognised_as_the_lab_store():
    # The lab's credential fallback keys off "is this endpoint the lab's own
    # store" (#477). The middlebox forwards at L3 to that same MinIO, so a
    # netem run must get the lab credentials — without this the engine
    # authenticates with nothing and every upload fails, which reads as a
    # transport result rather than the configuration mistake it is.
    from lib import engine

    through_the_box = {"backend": "s3_native", "shaping_driver": "netem",
                       "endpoint_url": "http://netem:9200"}
    assert engine._shaped_by_the_bundled_middlebox(through_the_box), (
        "an endpoint naming the bundled middlebox is the lab's store one hop out, and "
        "must be handed the lab's credentials")

    # ... and ONLY that box. An endpoint pointed straight at the store while
    # the scenario claims netem would go around every qdisc; lib/netem.py
    # refuses it, and the credential decision must not disagree.
    around_the_box = {"backend": "s3_native", "shaping_driver": "netem",
                      "endpoint_url": "http://minio:9000"}
    assert not engine._shaped_by_the_bundled_middlebox(around_the_box), (
        "an endpoint that bypasses the middlebox is not a netem run, and the credential "
        "decision must not quietly bless one")

    # A toxiproxy scenario must not be judged by the middlebox rule either.
    proxied = {"backend": "s3_native", "shaping_driver": "toxiproxy",
               "endpoint_url": "http://netem:9200"}
    assert not engine._shaped_by_the_bundled_middlebox(proxied), (
        "the driver decides which box is in the path; a toxiproxy scenario naming the "
        "middlebox's port is a mistake, not a lab endpoint")


# A scenario each driver's RUNNER would actually accept, trimmed to the keys
# the preflight reads. Since that prediction started asking whether the engine
# reaches its shaper (review), a three-line scenario no longer stands in for a
# real one: a netem scenario without `runner_mode: container` is refused before
# the results writer exists, so it declares a driver it can never contribute a
# row on. Writing the pins against runnable scenarios is what keeps them about
# the mixed-driver refusal rather than about the shape of the stub.
RUNNABLE_TOXIPROXY = (
    "backend: s3_native\n"
    "endpoint_url: http://localhost:9100\n"
    "shaping_api_url: http://127.0.0.1:8474\n"
    "shaping_bandwidth_kbps: 1250\n"
)
RUNNABLE_NETEM = (
    "backend: s3_native\n"
    "runner_mode: container\n"
    "endpoint_url: http://netem:9200\n"
    "shaping_driver: netem\n"
    "shaping_latency_ms: 100\n"
)


@pytest.fixture
def no_endpoint_override(monkeypatch):
    """Unset VTOP_S3_ENDPOINT_URL for the preflight pins.

    The runner resolves that variable OVER the scenario's own endpoint, and so
    does the preflight that predicts it. A stray override in the shell running
    these tests would make every shaped scenario below look like one that
    bypasses its shaper — every assertion here would then pass while checking
    nothing, which is the one way a test like this fails silently.
    """
    monkeypatch.delenv("VTOP_S3_ENDPOINT_URL", raising=False)


def write_scenarios(directory, **bodies) -> dict:
    """Named scenario files in `directory`, returned as name -> path.

    Keyed by NAME rather than returned as a list: every pin below is an
    argument about which scenarios belong in one table, and a call site reading
    `files["proxied"], files["shaped"]` says that, where `[a, b]` does not.
    """
    paths = {}
    for name, body in bodies.items():
        path = os.path.join(directory, f"{name}.yaml")
        with open(path, "w") as handle:
            handle.write(body)
        paths[name] = path
    return paths


def test_an_incomparable_scenario_list_is_refused_before_anything_runs(no_endpoint_override):
    # The refusal was correct and useless: it fired after every run completed,
    # and `--all` spans toxiproxy and netem today, so the ordinary path burned
    # 20+ minutes of soaks before reporting a conflict readable from the
    # scenario files (review).
    import tempfile

    import run_matrix

    files = write_scenarios(
        tempfile.mkdtemp(),
        proxied="name: a\n" + RUNNABLE_TOXIPROXY,
        shaped="name: b\n" + RUNNABLE_NETEM,
        plain="name: c\n",
        broken="name: d\nshaping_driver: not-a-driver\n",
        unrunnable="name: e\n" + RUNNABLE_TOXIPROXY + "volume: nope\n",
    )

    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.refuse_mixed_drivers_in_scenarios([files["proxied"], files["shaped"]])

    # A shaped list beside an unshaped one is the comparison shaping exists
    # for, and must still start.
    run_matrix.refuse_mixed_drivers_in_scenarios([files["shaped"], files["plain"]])

    # An unreadable scenario is NOT this check's refusal: the runner reports a
    # bad scenario far better, per scenario, and a matrix that would not start
    # because one file has a typo in an unrelated key is worse than the problem.
    run_matrix.refuse_mixed_drivers_in_scenarios([files["broken"], files["plain"]])

    # And a scenario that DECLARES a driver but cannot run must not contribute
    # one (review). `load_scenario` only parses and applies defaults, so this
    # loads fine, fails in the runner, and is excluded from the matrix by
    # run_one — while the netem scenario beside it produces a perfectly good
    # row. Counting its driver here would abort a matrix that used to succeed:
    # predicting a conflict must never be worse than discovering one.
    run_matrix.refuse_mixed_drivers_in_scenarios([files["unrunnable"], files["shaped"]])

    # ... and the real conflict is still caught before anything runs.
    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.refuse_mixed_drivers_in_scenarios([files["proxied"], files["shaped"]])


def test_the_queue_limit_rounds_up_so_the_configured_buffer_survives():
    # Flooring the packet conversion loses up to one MTU, and the loss comes
    # out of the CONGESTION buffer rather than the delay line — a 10 Mbit/s,
    # 50 ms, 1-BDP link recorded 62500 bytes of queue and had 61750 (review).
    # Rounding up can only ever add a fraction of a packet, which is the
    # harmless direction.
    from lib.netem import MTU_BYTES, NetemShape

    shape = NetemShape(latency_ms=50, bottleneck_kbps=10_000, buffer_bdp=1.0)
    held = shape.netem_limit_packets() * MTU_BYTES
    assert held - shape.delay_occupancy_bytes() >= shape.buffer_bytes(), (
        "after the delay line, at least the configured buffer must remain; a queue "
        "smaller than the column beside it makes the BDP scenarios differ by less "
        "than they claim"
    )


def test_an_invalid_runner_mode_does_not_take_the_matrix_down_with_it(no_endpoint_override):
    # runner_mode is validated by the runner before it creates a row, so a
    # scenario carrying a typo in it will not produce one — and counting its
    # declared driver aborts a matrix whose other scenarios were fine (review).
    import tempfile

    import run_matrix

    files = write_scenarios(
        tempfile.mkdtemp(),
        typoed="name: a\n" + RUNNABLE_TOXIPROXY + "runner_mode: typo\n",
        shaped="name: b\n" + RUNNABLE_NETEM,
        proxied="name: c\n" + RUNNABLE_TOXIPROXY,
    )
    run_matrix.refuse_mixed_drivers_in_scenarios([files["typoed"], files["shaped"]])

    # The control, without which the line above passes whether or not the mode
    # is judged: the SAME netem scenario beside a toxiproxy one that can run is
    # still the conflict this preflight exists to catch. If this stops raising,
    # the pin above has become a test of nothing.
    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.refuse_mixed_drivers_in_scenarios([files["proxied"], files["shaped"]])


def test_a_netem_scenario_that_cannot_reach_the_middlebox_is_not_counted(no_endpoint_override):
    # The preflight's prediction must never be OPTIMISTIC about a scenario the
    # runner refuses deterministically (review). A netem scenario left in the
    # default `runner_mode: host` builds its shape and names a valid mode, so
    # the prediction said "will run" — but `require_endpoint_reaches_the_shape`
    # refuses every host-mode netem run before the results writer exists: the
    # middlebox sits in the lab network's L3 path and a host process never
    # crosses it. Counting its driver aborted the whole matrix, including the
    # toxiproxy scenarios beside it that would have produced good rows.
    import tempfile

    import run_matrix

    files = write_scenarios(
        tempfile.mkdtemp(),
        # netem, but host mode — the runner's own refusal, verbatim in its
        # effect: no row, ever, on any machine.
        host_netem="name: a\nbackend: s3_native\nendpoint_url: http://netem:9200\n"
                   "shaping_driver: netem\nshaping_latency_ms: 100\n",
        # netem, but aimed straight at the store: the qdiscs are bypassed and
        # the summary would record a shape nothing applied.
        around_the_box="name: b\nbackend: s3_native\nrunner_mode: container\n"
                       "endpoint_url: http://minio:9000\n"
                       "shaping_driver: netem\nshaping_latency_ms: 100\n",
        proxied="name: c\n" + RUNNABLE_TOXIPROXY,
        shaped="name: d\n" + RUNNABLE_NETEM,
    )

    for unreachable in ("host_netem", "around_the_box"):
        run_matrix.refuse_mixed_drivers_in_scenarios([files[unreachable], files["proxied"]])

    # ... while the netem scenario that CAN reach its middlebox still conflicts
    # with the same toxiproxy scenario. Without this the two lines above would
    # pass on a preflight that had simply stopped counting netem at all.
    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.refuse_mixed_drivers_in_scenarios([files["shaped"], files["proxied"]])


def test_the_bundled_scenarios_still_reach_the_preflight_as_two_drivers(no_endpoint_override):
    # `--all` spans toxiproxy (13) and netem (14, 15, 16) today, and that is
    # the ordinary path this preflight was written for. Every check added to
    # the prediction can only ever EXCLUDE a scenario, so the way this stops
    # working is silently: one over-strict rule drops the bundled netem
    # scenarios, the refusal never fires again, and a matrix nobody can read
    # gets written after 20+ minutes of soaks.
    import glob

    import run_matrix

    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    bundled = sorted(glob.glob(os.path.join(here, "scenarios", "*.yaml")))
    assert bundled, "the bundled scenarios must be found, or this test checks nothing"
    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.refuse_mixed_drivers_in_scenarios(bundled)


def test_the_delay_line_reserves_headroom_for_the_jitter_it_configures():
    # netem's `limit` bounds every packet the qdisc holds, and the upload delay
    # is configured with `distribution normal` — the jitter is a standard
    # deviation, so half of every draw sits above the mean and the delay line
    # holds more than `rate x upload_delay_ms` while those packets wait
    # (review). Reserving only the mean spends the difference out of the
    # congestion buffer's permits, and netem drops on its own limit before
    # tbf's queue is full. Those drops are the emulator's, they land in no
    # column, and the scenario would file them against the LINK.
    from lib.netem import MTU_BYTES, NetemShape

    # Three standard deviations, spelled out here rather than imported from
    # lib.netem's JITTER_HEADROOM_SIGMAS: a reserve this test read out of the
    # module it is testing would agree with any depth the module chose,
    # including none. A normal draw exceeds three sigma about once in a
    # thousand packets, and shrinking that has to be argued against this
    # assertion rather than inherited from it.
    sigmas = 3

    steady = NetemShape(latency_ms=100, bottleneck_kbps=10_000, buffer_bdp=1.0)
    jittery = NetemShape(latency_ms=100, jitter_ms=20, bottleneck_kbps=10_000,
                         buffer_bdp=1.0)

    assert jittery.buffer_bytes() == steady.buffer_bytes(), (
        "the two links differ only in jitter; the configured congestion buffer is "
        "the same number, and it is the number both record"
    )
    assert jittery.delay_occupancy_bytes() > steady.delay_occupancy_bytes(), (
        "a delay line with jitter holds more than one without, and a limit that "
        "does not know it silently shrinks the buffer the column reports"
    )

    # Judged against the arguments actually INSTALLED, not against the same
    # arithmetic twice: the tc program is what the kernel sees, so a headroom
    # that never reaches the qdisc is a headroom that does not exist.
    upload = [cmd for cmd in jittery.tc_program()
              if "netem" in cmd and "limit" in cmd]
    assert len(upload) == 1, f"exactly one limited netem qdisc is installed: {upload}"
    args = upload[0]
    delay_ms = int(args[args.index("delay") + 1].removesuffix("ms"))
    jitter_ms = int(args[args.index("delay") + 2].removesuffix("ms"))
    assert args[args.index("delay") + 3:args.index("delay") + 5] == ["distribution", "normal"], (
        "the reserve is sized for a normal draw; if the distribution changes, the "
        "number of sigmas has to be re-argued rather than inherited"
    )
    limit_packets = int(args[args.index("limit") + 1])

    deep_sample_ms = delay_ms + sigmas * jitter_ms
    in_flight = jittery.bottleneck_kbps * 1000 / 8 * deep_sample_ms / 1000.0
    assert limit_packets * MTU_BYTES >= in_flight + jittery.buffer_bytes(), (
        f"on a {deep_sample_ms} ms delay sample the line holds {in_flight:.0f} bytes and "
        f"the qdisc must still hold the {jittery.buffer_bytes()} byte buffer beside it, "
        f"but its limit is only {limit_packets} packets ({limit_packets * MTU_BYTES} "
        "bytes): short of that netem drops packets the link never lost, and a "
        "jitter-plus-bottleneck run reports loss the emulator invented"
    )

    # A shape with no jitter pays nothing for this: the reserve is the jitter's,
    # and the links measured without one keep the numbers they were measured at.
    assert steady.delay_occupancy_bytes() == 62_500, (
        "50 ms of upload delay at 10 Mbit/s, unchanged — an unjittered scenario's "
        "recorded queue must not move because a jittered one needed headroom"
    )


def test_a_combination_the_runner_refuses_outright_contributes_no_driver():
    # Concurrent seeding writes to the final path, so a whole-file format lets
    # the engine commit a half-written object — the runner exits 2 on that
    # combination before any row exists. A scenario that exits 2 cannot
    # contribute a driver, or the preflight aborts a matrix whose other
    # scenarios were fine (review). Same class as runner_mode and the endpoint.
    import os
    import tempfile

    import run_matrix

    directory = tempfile.mkdtemp()

    def scenario(name: str, body: str) -> str:
        path = os.path.join(directory, name)
        with open(path, "w") as handle:
            handle.write(body)
        return path

    refused = scenario("a.yaml",
                       "name: a\nshaping_api_url: http://127.0.0.1:8474\n"
                       "shaping_bandwidth_kbps: 1250\nshaping_latency_ms: 100\n"
                       "backend: s3_native\nendpoint_url: http://localhost:9100\n"
                       "duration_seconds: 60\nseed_concurrently: true\nformat: binary\n")
    shaped = scenario("b.yaml",
                      "name: b\nshaping_driver: netem\nshaping_latency_ms: 100\n"
                      "shaping_bottleneck_kbps: 10000\nshaping_buffer_bdp: 1.0\n"
                      "backend: s3_native\nrunner_mode: container\n"
                      "endpoint_url: http://netem:9200\n")

    run_matrix.refuse_mixed_drivers_in_scenarios([refused, shaped])

    # A non-positive seed interval is refused by the runner the same way, and
    # is excluded here for the same reason.
    bad_interval = scenario("c.yaml",
                            "name: c\nshaping_api_url: http://127.0.0.1:8474\n"
                            "shaping_bandwidth_kbps: 1250\nshaping_latency_ms: 100\n"
                            "backend: s3_native\nendpoint_url: http://localhost:9100\n"
                            "duration_seconds: 60\nseed_concurrently: true\n"
                            "seed_interval_seconds: 0\n")
    run_matrix.refuse_mixed_drivers_in_scenarios([bad_interval, shaped])

    # The same toxiproxy scenario WITHOUT the refused combination is runnable,
    # so the conflict is still caught — the exclusion must not swallow real ones.
    runnable = scenario("d.yaml",
                        "name: d\nshaping_api_url: http://127.0.0.1:8474\n"
                        "shaping_bandwidth_kbps: 1250\nshaping_latency_ms: 100\n"
                        "backend: s3_native\nendpoint_url: http://localhost:9100\n")
    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.refuse_mixed_drivers_in_scenarios([runnable, shaped])


def test_a_padded_driver_name_is_still_counted_as_its_driver():
    # `selected_driver` strips whitespace, so `shaping_driver: " netem "` RUNS
    # as netem — while a raw comparison in the preflight called it unshaped and
    # skipped it, and a netem scenario has no `shaping_api_url` to be caught by
    # instead (review). Reading the key differently from the runner is how a
    # driver goes uncounted and a conflict reaches the soaks.
    import os
    import tempfile

    import run_matrix

    directory = tempfile.mkdtemp()

    def scenario(name: str, body: str) -> str:
        path = os.path.join(directory, name)
        with open(path, "w") as handle:
            handle.write(body)
        return path

    netem_base = ("backend: s3_native\nrunner_mode: container\n"
                  "endpoint_url: http://netem:9200\nshaping_bottleneck_kbps: 10000\n"
                  "shaping_buffer_bdp: 1.0\nshaping_latency_ms: 100\n")
    padded = scenario("a.yaml", 'name: a\nshaping_driver: " netem "\n' + netem_base)
    proxied = scenario("b.yaml", "name: b\nshaping_api_url: http://127.0.0.1:8474\n"
                                 "shaping_bandwidth_kbps: 1250\nshaping_latency_ms: 100\n"
                                 "backend: s3_native\nendpoint_url: http://localhost:9100\n")

    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.refuse_mixed_drivers_in_scenarios([padded, proxied])


def test_a_zero_pipeline_width_contributes_no_driver():
    # A zero width is preserved into the engine config on purpose so the
    # engine's own validation refuses it by name rather than clamping it
    # silently — which means the scenario produces no row, and a scenario that
    # produces no row must not contribute a driver (review).
    import os
    import tempfile

    import run_matrix

    directory = tempfile.mkdtemp()

    def scenario(name: str, body: str) -> str:
        path = os.path.join(directory, name)
        with open(path, "w") as handle:
            handle.write(body)
        return path

    netem_base = ("backend: s3_native\nrunner_mode: container\n"
                  "endpoint_url: http://netem:9200\nshaping_bottleneck_kbps: 10000\n"
                  "shaping_buffer_bdp: 1.0\nshaping_latency_ms: 100\n")
    zero_width = scenario("a.yaml", "name: a\nshaping_api_url: http://127.0.0.1:8474\n"
                                    "shaping_bandwidth_kbps: 1250\nshaping_latency_ms: 100\n"
                                    "backend: s3_native\nendpoint_url: http://localhost:9100\n"
                                    "max_concurrent_batches: 0\n")
    shaped = scenario("b.yaml", "name: b\nshaping_driver: netem\n" + netem_base)

    run_matrix.refuse_mixed_drivers_in_scenarios([zero_width, shaped])


def test_a_pipeline_width_the_engine_cannot_read_as_an_integer_contributes_no_driver(
        no_endpoint_override, tmp_path):
    # The sixth member of the family, and the quietest one (review). The YAML
    # loader hands `max_concurrent_batches: 1.5` over as a Python FLOAT and
    # `: true` as a Python BOOL, and `int()` accepts both — 1 either way — so
    # the positivity check passed them while `write_engine_config` wrote the
    # value through verbatim and the engine refused it at config load
    # ("invalid type: floating point `1.5`, expected usize", "invalid type:
    # boolean `true`, expected usize"; both confirmed against the serde_yaml
    # the engine deserializes with). No row, but a counted driver — which is
    # the matrix-aborting shape this whole preflight exists to stop causing.
    import run_matrix
    from lib.engine import write_engine_config

    netem_base = ("backend: s3_native\nrunner_mode: container\n"
                  "endpoint_url: http://netem:9200\nshaping_bottleneck_kbps: 10000\n"
                  "shaping_buffer_bdp: 1.0\nshaping_latency_ms: 100\n")
    toxiproxy_base = ("shaping_api_url: http://127.0.0.1:8474\n"
                      "shaping_bandwidth_kbps: 1250\nshaping_latency_ms: 100\n"
                      "backend: s3_native\nendpoint_url: http://localhost:9100\n")
    files = write_scenarios(str(tmp_path), shaped="name: b\nshaping_driver: netem\n" + netem_base)
    shaped = files["shaped"]

    # `2.0` is the seductive one: it IS an integer to a reader and to `int()`,
    # and it is still a float on the wire into serde, which refuses it exactly
    # as it refuses 1.5. A rule written around "looks whole" would let it back.
    for spelling in ("1.5", "2.0", "true"):
        widths = write_scenarios(
            str(tmp_path),
            **{"w": f"name: a\n{toxiproxy_base}max_concurrent_batches: {spelling}\n"})
        run_matrix.refuse_mixed_drivers_in_scenarios([widths["w"], shaped])

    # ... and a width the engine CAN read is still counted, or the exclusion
    # has started swallowing the real conflicts it exists beside.
    good = write_scenarios(
        str(tmp_path),
        good=f"name: a\n{toxiproxy_base}max_concurrent_batches: 4\n")["good"]
    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.refuse_mixed_drivers_in_scenarios([good, shaped])

    # The preflight judges the RENDERED value because rendering is what the
    # engine receives. That is only the right question while the config writer
    # keeps writing the value verbatim, so pin the coupling here: if it ever
    # starts coercing or quoting this knob, the rule above has to be re-derived
    # rather than quietly answering about a line nobody writes any more.
    for width in (1.5, True, 4):
        config = tmp_path / f"engine-{width}.yaml"
        write_engine_config({"max_concurrent_batches": width}, str(tmp_path / "work"),
                            str(tmp_path / "state.db"), str(tmp_path / "seed" / "*"),
                            str(config))
        assert f"  max_concurrent_batches: {width}\n" in config.read_text(), (
            f"the config writer must still pass {width!r} through untouched; the "
            "preflight predicts the engine's answer by rendering the value the same "
            "way, and a writer that coerced or quoted it would make that prediction "
            "wrong in whichever direction nobody noticed"
        )
