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


# A scenario for each driver, trimmed to the keys the matrix reads to ORDER its
# runs. Whether any of them would actually run is deliberately not these
# fixtures' business any more: the matrix no longer predicts it, and the tests
# below decide it by what their scripted runner returns.
TOXIPROXY = (
    "backend: s3_native\n"
    "endpoint_url: http://localhost:9100\n"
    "shaping_api_url: http://127.0.0.1:8474\n"
    "shaping_bandwidth_kbps: 1250\n"
)
NETEM = (
    "backend: s3_native\n"
    "runner_mode: container\n"
    "endpoint_url: http://netem:9200\n"
    "shaping_driver: netem\n"
    "shaping_latency_ms: 100\n"
)


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


def test_the_bottleneck_buffer_is_tbfs_own_byte_limit_at_every_link_the_scenarios_shape():
    # Three review rounds converted the buffer into netem PACKETS and then
    # reserved room in the same limit for the delay line, and each round's
    # reserve was wrong in a new way. The layout that ended it: netem at the
    # root holds the delay line, tbf beneath it keeps its own bfifo, and the
    # buffer is tbf's `limit` in BYTES — no conversion to round, no reserve to
    # share (review).
    from lib.netem import IFB_DEV, NetemShape

    for latency, jitter, bdp in ((50, 0, 1.0), (100, 0, 1.0), (100, 20, 1.0), (100, 0, 4.0)):
        shape = NetemShape(latency_ms=latency, jitter_ms=jitter, bottleneck_kbps=10_000,
                           buffer_bdp=bdp)
        on_ifb = [cmd for cmd in shape.tc_program() if cmd[:3] == ["tc", "qdisc", "add"]
                  and cmd[cmd.index("dev") + 1] == IFB_DEV]
        tbf = [cmd for cmd in on_ifb if "tbf" in cmd]
        assert len(tbf) == 1 and "parent" in tbf[0], (
            f"{latency} ms, {jitter} ms jitter, {bdp} BDP: the bottleneck must be netem's "
            "child, not its parent — as the parent its bfifo is replaced by netem's queue "
            "and the byte limit applies to nothing")
        assert int(tbf[0][tbf[0].index("limit") + 1]) == shape.buffer_bytes(), (
            "the configured buffer is the queue, in bytes, exactly")


def test_the_jitter_reserve_lands_on_the_delay_line_and_never_on_the_congestion_queue():
    # netem's delay is `distribution normal` with the jitter as its sigma, so
    # the line holds more than `rate x delay` on its deep draws, and its limit
    # has to allow for them. Under the old layout that allowance lived in the
    # ONLY queue limit, so every draw short of three sigma lent the difference
    # to congestion backlog: 37.5 kB, 0.3 BDP, at 10 Mbit/s and 20 ms (review).
    # Now the reserve may move the delay line's limit and nothing else.
    from lib.netem import MTU_BYTES, NetemShape

    # Three standard deviations, spelled out here rather than imported: a
    # reserve read out of the module under test agrees with any depth the
    # module chose, including none.
    sigmas = 3

    steady = NetemShape(latency_ms=100, bottleneck_kbps=10_000, buffer_bdp=1.0)
    jittery = NetemShape(latency_ms=100, jitter_ms=20, bottleneck_kbps=10_000,
                         buffer_bdp=1.0)

    def installed(shape, kind):
        return [cmd for cmd in shape.tc_program() if cmd[:3] == ["tc", "qdisc", "add"]
                and kind in cmd and "ifb0" in cmd]

    assert all("parent" in cmd for cmd in installed(jittery, "tbf")), (
        "an unchanged tbf proves nothing while tbf is the root, whose byte limit its "
        "netem child replaces"
    )
    assert installed(jittery, "tbf") == installed(steady, "tbf"), (
        "the congestion queue is identical with and without jitter; a difference is a "
        "delay-line reserve congestion backlog can borrow"
    )

    # Judged against the arguments actually INSTALLED on the delay line.
    upload = installed(jittery, "netem")
    assert len(upload) == 1, f"exactly one netem on the upload: {upload}"
    args = upload[0]
    delay_ms = int(args[args.index("delay") + 1].removesuffix("ms"))
    jitter_ms = int(args[args.index("delay") + 2].removesuffix("ms"))
    assert args[args.index("delay") + 3:args.index("delay") + 5] == ["distribution", "normal"], (
        "the reserve is sized for a normal draw; if the distribution changes, the "
        "number of sigmas has to be re-argued rather than inherited"
    )
    limit_packets = int(args[args.index("limit") + 1])
    in_flight = jittery.bottleneck_kbps * 1000 / 8 * (delay_ms + sigmas * jitter_ms) / 1000.0
    assert limit_packets * MTU_BYTES >= in_flight, (
        f"a {delay_ms + sigmas * jitter_ms} ms delay sample holds {in_flight:.0f} bytes in "
        f"the line, beyond its limit of {limit_packets} packets: netem would drop packets "
        "the link never lost"
    )


# --------------------------------------------------------------------------
# The mixed-driver refusal: decided by rows that exist, never by a prediction
# --------------------------------------------------------------------------


class ScriptedRuns:
    """Stands in for running one scenario: answers each path with the row its
    run produced, or None for a run that produced none, and records the order
    it was asked in. What decides a row here is the SCRIPT, never the file —
    which is the point: the matrix may no longer ask the file."""

    def __init__(self, rows: dict):
        self.rows = rows
        self.calls: list[str] = []

    def __call__(self, path):
        name = os.path.splitext(os.path.basename(path))[0]
        self.calls.append(name)
        return self.rows.get(name)


def shaped_row(driver: str, name: str) -> dict:
    return {"scenario_name": name, "shaping_driver": driver}


def test_a_real_conflict_is_refused_at_the_second_drivers_first_row_before_the_rest_run(tmp_path):
    # The finding the preflight was written for (review): the refusal used to
    # fire after EVERY scenario had run, and `--all` spans both drivers. It now
    # fires the moment two drivers have produced rows — and the runs are ordered
    # so that moment comes first: one scenario per declared driver, ahead of the
    # unshaped ones listed before them.
    import run_matrix

    files = write_scenarios(
        str(tmp_path), plain1="name: p1\n", plain2="name: p2\n",
        proxied="name: a\n" + TOXIPROXY, plain3="name: p3\n", shaped="name: b\n" + NETEM,
        shaped2="name: c\n" + NETEM)
    runs = ScriptedRuns({"plain1": {"scenario_name": "p1"}, "plain2": {"scenario_name": "p2"},
                         "plain3": {"scenario_name": "p3"},
                         "proxied": shaped_row("toxiproxy", "a"),
                         "shaped": shaped_row("netem", "b"), "shaped2": shaped_row("netem", "c")})
    order = ["plain1", "plain2", "proxied", "plain3", "shaped", "shaped2"]

    with pytest.raises(run_matrix.IncomparableRuns) as exc:
        run_matrix.collect_rows([files[name] for name in order], runs)
    assert runs.calls == ["proxied", "shaped"], (
        f"ran {runs.calls}: a conflict must cost one successful run per driver, not the "
        "unshaped scenarios listed ahead of them and not the rest of the matrix")
    message = str(exc.value)
    assert "netem, toxiproxy" in message and "4 were not run" in message, (
        "the refusal names both drivers and says how much of the matrix it spared, so the "
        "operator knows the rest never started rather than wondering where its rows went")


def test_a_scenario_that_produces_no_row_contributes_no_driver_whatever_its_file_says(tmp_path):
    # THE SERIES THIS REPLACES (review, seven rounds). A prediction of whether a
    # scenario would produce a row kept missing one of the runner's or the
    # engine's rules, and each miss aborted a matrix that had always succeeded.
    # Nothing is predicted now: a scenario whose run produces no row adds no
    # driver, for any reason — these are the three the last round found, and the
    # runner script is what says they fail, exactly as the engine would.
    import run_matrix

    files = write_scenarios(
        str(tmp_path),
        bad_compression="name: a\n" + TOXIPROXY + "compression: typo\n",
        zero_records="name: b\n" + TOXIPROXY + "batch_max_records: 0\n",
        zero_bytes="name: c\n" + TOXIPROXY + "batch_max_bytes: 0\n",
        shaped="name: d\n" + NETEM)
    for invalid in ("bad_compression", "zero_records", "zero_bytes"):
        runs = ScriptedRuns({invalid: None, "shaped": shaped_row("netem", "d")})
        rows = run_matrix.collect_rows([files[invalid], files["shaped"]], runs)
        assert rows == [shaped_row("netem", "d")], (
            f"{invalid} declares toxiproxy but its run produced no row, so the netem row "
            "beside it is the whole matrix — refusing it would be a prediction costing more "
            "than the conflict it predicted")

    # The control: the SAME file, had its run produced a row, IS a conflict. The
    # decision turned on the run's answer and on nothing the file says.
    runs = ScriptedRuns({"bad_compression": shaped_row("toxiproxy", "a"),
                         "shaped": shaped_row("netem", "d")})
    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.collect_rows([files["bad_compression"], files["shaped"]], runs)


def test_a_fractional_duration_that_runs_is_counted_and_refused_before_the_rest(tmp_path):
    # The opposite direction of the same series (review): the prediction read
    # `duration_seconds` with `int()` while the runner reads it with `float()`.
    # Both loaders hand a bare `1200.5` over as a float, which `int()` accepts;
    # the quoted `"1200.5"` arrives as a string, which `int()` refuses and
    # `float()` does not — so that scenario was judged unrunnable, left
    # uncounted, and the conflict reached both twenty-minute soaks. A run that
    # produces a row is counted because it produced one, however its knobs are
    # spelled.
    import run_matrix

    files = write_scenarios(
        str(tmp_path), plain="name: p\n",
        long_proxied="name: a\n" + TOXIPROXY + 'duration_seconds: "1200.5"\n',
        shaped="name: b\n" + NETEM, other="name: q\n")
    runs = ScriptedRuns({"plain": {"scenario_name": "p"}, "other": {"scenario_name": "q"},
                         "long_proxied": shaped_row("toxiproxy", "a"),
                         "shaped": shaped_row("netem", "b")})
    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.collect_rows(
            [files[name] for name in ("plain", "long_proxied", "shaped", "other")], runs)
    assert runs.calls == ["long_proxied", "shaped"], (
        "the fractional-duration scenario is scheduled and counted like any other, and the "
        "refusal lands before the unshaped runs either side of it")


def test_a_declared_driver_that_fails_is_retried_from_the_next_scenario_declaring_it(tmp_path):
    # The order is a heuristic, and the refusal must not depend on it being
    # right: when the first netem scenario produces nothing (its lab is down, or
    # a knob is wrong), the next netem one is still tried ahead of the unshaped
    # queue, so a real conflict is still found as early as the rows allow.
    import run_matrix

    files = write_scenarios(
        str(tmp_path), proxied="name: a\n" + TOXIPROXY, broken_netem="name: b\n" + NETEM,
        plain="name: p\n", shaped="name: c\n" + NETEM)
    runs = ScriptedRuns({"proxied": shaped_row("toxiproxy", "a"), "broken_netem": None,
                         "plain": {"scenario_name": "p"}, "shaped": shaped_row("netem", "c")})
    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.collect_rows(
            [files[name] for name in ("proxied", "broken_netem", "plain", "shaped")], runs)
    assert runs.calls == ["proxied", "broken_netem", "shaped"]


def test_a_matrix_that_does_not_conflict_keeps_its_rows_in_list_order(tmp_path):
    # Reordering the RUNS is an implementation detail of finding a conflict
    # early; the table a reader opens is still in the order they listed.
    import run_matrix

    files = write_scenarios(str(tmp_path), plain="name: p\n", shaped="name: b\n" + NETEM,
                            broken="name: x\nshaping_driver: not-a-driver\n",
                            padded='name: c\nshaping_driver: " netem "\n' + NETEM.replace(
                                "shaping_driver: netem\n", ""))
    runs = ScriptedRuns({"plain": {"scenario_name": "p"}, "shaped": shaped_row("netem", "b"),
                         "broken": None, "padded": shaped_row("netem", "c")})
    rows = run_matrix.collect_rows(
        [files[name] for name in ("plain", "broken", "shaped", "padded")], runs)
    assert [row["scenario_name"] for row in rows] == ["p", "b", "c"], (
        "a shaped run beside an unshaped one and a second run on the same driver is the "
        "comparison shaping exists for, and it is written in the operator's order")
    assert runs.calls[0] == "shaped", (
        "the declared netem scenario ran first; an unreadable one is scheduled as unshaped "
        "and still gets its run, which is where its real error is reported")
    assert run_matrix.declared_driver(files["padded"]) == "netem", (
        "a padded driver name is read the way the runner reads it, so it is scheduled as "
        "the driver it will actually run under")


def test_the_bundled_scenarios_schedule_one_run_of_each_driver_first(tmp_path):
    # `--all` spans toxiproxy (13) and netem (14, 15, 16): the ordinary path.
    # With both labs up, the conflict is found after two soaks rather than after
    # all sixteen scenarios.
    import glob

    import run_matrix

    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    bundled = sorted(glob.glob(os.path.join(here, "scenarios", "*.yaml")))
    assert bundled, "the bundled scenarios must be found, or this test checks nothing"
    declared = {os.path.splitext(os.path.basename(path))[0]: run_matrix.declared_driver(path)
                for path in bundled}
    runs = ScriptedRuns({name: (shaped_row(driver, name) if driver else {"scenario_name": name})
                         for name, driver in declared.items()})
    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.collect_rows(bundled, runs)
    assert len(runs.calls) == 2 and {declared[name] for name in runs.calls} == {
        "toxiproxy", "netem"}, f"ran {runs.calls} before refusing"


def test_the_matrix_writes_no_table_when_its_rows_conflict(tmp_path, monkeypatch):
    # End to end through main(): the refusal has to land before matrix.csv
    # exists, because a table that exists is a table somebody reads.
    import json
    import sys

    import run_matrix

    files = write_scenarios(str(tmp_path), proxied="name: a\n" + TOXIPROXY,
                            shaped="name: b\n" + NETEM, plain="name: p\n")
    drivers = {"proxied": "toxiproxy", "shaped": "netem", "plain": ""}
    ran = []

    def run_one(path, results_dir):
        name = os.path.splitext(os.path.basename(path))[0]
        ran.append(name)
        run_dir = os.path.join(results_dir, f"run-{name}")
        os.makedirs(run_dir)
        with open(os.path.join(run_dir, "summary.json"), "w") as fh:
            json.dump({"scenario_name": name, "shaping_driver": drivers[name],
                       "scenario": {}}, fh)
        return run_dir

    monkeypatch.setattr(run_matrix, "run_one", run_one)
    results = tmp_path / "results"
    monkeypatch.setattr(sys, "argv", ["run_matrix.py", files["plain"], files["proxied"],
                                      files["shaped"], "--results-dir", str(results)])
    with pytest.raises(run_matrix.IncomparableRuns):
        run_matrix.main()
    assert ran == ["proxied", "shaped"], "the unshaped scenario never started"
    assert not glob_matrix_tables(results), "and no comparison table was written"


def glob_matrix_tables(results) -> list:
    import glob
    return glob.glob(os.path.join(str(results), "matrix-*", "matrix.*"))
