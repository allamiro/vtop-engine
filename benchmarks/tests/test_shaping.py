"""Tests for benchmarks/lib/shaping.py (#403): the shape a scenario asks for,
the toxics it becomes, and the apply/remove discipline around a run — all
against a scripted client, so no socket is ever opened."""

import os
import textwrap

import pytest

from lib.engine import _is_lab_endpoint
from lib.metrics import _shaping_cell
from lib.netem import NetemShape
from lib.scenario import DEFAULTS, Scenario, load_scenario
from lib.shaping import (
    LOCK_TOXIC,
    SHAPING_COLUMNS,
    TOXIC_KINDS,
    Shape,
    ShapingError,
    apply,
    describe_shape_line,
    require_endpoint_through_proxy,
    shape_from_scenario,
    shaped,
)

SCENARIO_DIR = os.path.join(os.path.dirname(__file__), "..", "scenarios")


def scenario(**values):
    return Scenario({"shaping_api_url": "", **values})


class ScriptedClient:
    """Answers each request from a script and records what was asked."""

    def __init__(self, api_url="http://127.0.0.1:8474", statuses=None):
        self.api_url = api_url
        self.calls = []
        self.statuses = dict(statuses or {})

    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        default = {"GET": 200, "POST": 201, "DELETE": 204}[method]
        # A status may be scripted per toxic name, or per method and path.
        named = (method, path, (body or {}).get("name")) if isinstance(body, dict) else None
        status = self.statuses.get(named, self.statuses.get((method, path), default))
        payload = (
            {"name": "minio", "listen": "[::]:9100", "upstream": "minio:9000", "toxics": []}
            if method == "GET" else None)
        return status, payload


# --------------------------------------------------------------------------
# The shape
# --------------------------------------------------------------------------


def test_no_api_url_means_unshaped():
    assert Shape.from_scenario(scenario()) is None
    assert Shape.from_scenario(scenario(shaping_api_url="   ")) is None


def test_the_shape_is_read_from_flat_keys_and_the_api_url_is_normalized():
    shape = Shape.from_scenario(scenario(
        shaping_api_url="http://127.0.0.1:8474/", shaping_proxy="minio",
        shaping_bandwidth_kbps=1250, shaping_latency_ms=100, shaping_jitter_ms=20))
    assert (shape.api_url, shape.proxy, shape.bandwidth_kbps, shape.latency_ms, shape.jitter_ms) == (
        "http://127.0.0.1:8474", "minio", 1250, 100, 20)
    assert len(shape.run_token) == 8, "a token per run"
    # The record names its DRIVER and its UNIT since #477: two drivers now
    # write these rows, their rate keys disagree about kilobytes versus
    # kilobits, and a reader with two summaries in front of them cannot be
    # asked to remember which was which.
    assert shape.describe() == {"driver": "toxiproxy",
                                "proxy": "minio", "bandwidth_kbps": 1250,
                                "latency_ms": 100, "jitter_ms": 20,
                                "rate_unit": "kilobytes_per_second",
                                "scope": "per_connection", "run_token": shape.run_token}
    # The netem columns are present and BLANK on a toxiproxy row: a column
    # that appears only for the runs that set it makes a results directory
    # two shapes of file, and the matrix reads one.
    assert shape.flat_columns() == {"shaping_driver": "toxiproxy",
                                    "shaping_proxy": "minio", "shaping_bandwidth_kbps": 1250,
                                    "shaping_latency_ms": 100, "shaping_jitter_ms": 20,
                                    "shaping_scope": "per_connection",
                                    "shaping_loss_pct": "", "shaping_loss_model": "",
                                    "shaping_bottleneck_kbps": "", "shaping_buffer_bdp": "",
                                    "shaping_policer_kbps": ""}
    assert set(shape.flat_columns()) == set(SHAPING_COLUMNS), (
        "every shaping column exists on every shaped row, whichever driver wrote it — "
        "a results directory with two shapes of header is not one comparison")
    assert shape.toxic_names() == [f"vtop_{shape.run_token}_{k}" for k in TOXIC_KINDS]


def test_a_shaped_run_with_no_shape_is_refused():
    with pytest.raises(ValueError, match="both 0"):
        Shape.from_scenario(scenario(shaping_api_url="http://127.0.0.1:8474"))


def test_a_shape_knob_is_an_integer_or_refused():
    base = dict(shaping_api_url="http://x", shaping_bandwidth_kbps=10)
    with pytest.raises(ValueError, match="shaping_latency_ms must be an integer, got 1.9"):
        Shape.from_scenario(scenario(**base, shaping_latency_ms=1.9))
    with pytest.raises(ValueError, match="shaping_latency_ms must be an integer, got True"):
        Shape.from_scenario(scenario(**base, shaping_latency_ms=True))
    with pytest.raises(ValueError, match="shaping_latency_ms must be an integer"):
        Shape.from_scenario(scenario(**base, shaping_latency_ms="2.0"))
    assert Shape.from_scenario(scenario(**base, shaping_latency_ms=100.0)).latency_ms == 100
    assert Shape.from_scenario(scenario(**base, shaping_latency_ms="100")).latency_ms == 100


class ResettingRemovalClient(ScriptedClient):
    """Loses the connection on the first removal after the shape is applied."""

    def request(self, method, path, body=None):
        if method == "GET":
            return super().request(method, path, body)
        applied = any(m == "POST" for m, _, _ in self.calls)
        self.calls.append((method, path, body))
        if method == "DELETE" and applied and path.endswith("_bandwidth_up"):
            raise ShapingError("toxiproxy at http://x is not answering (connection reset)")
        return {"GET": 200, "POST": 201, "DELETE": 204}[method], None


def test_cleanup_tries_every_toxic_past_a_transport_error():
    client = ResettingRemovalClient()
    sc = scenario(shaping_api_url="http://127.0.0.1:8474", shaping_bandwidth_kbps=1250,
                  shaping_latency_ms=100)
    with pytest.raises(ShapingError, match="_bandwidth_up \\(toxiproxy"):
        with shaped(sc, client_factory=lambda url: client, log=lambda _: None):
            pass
    tail = [p for m, p, _ in client.calls[-5:] if m == "DELETE"]
    assert len(tail) == 5, "the other three, and the claim, were still attempted"
    assert tail[-1].endswith(LOCK_TOXIC), "the claim goes last"


def test_bad_knobs_are_refused_by_name():
    with pytest.raises(ValueError, match="shaping_bandwidth_kbps"):
        Shape.from_scenario(scenario(shaping_api_url="http://x", shaping_bandwidth_kbps=-1))
    with pytest.raises(ValueError, match="shaping_latency_ms must be an integer"):
        Shape.from_scenario(scenario(shaping_api_url="http://x", shaping_latency_ms="slow"))
    with pytest.raises(ValueError, match="shaping_jitter_ms needs"):
        Shape.from_scenario(scenario(shaping_api_url="http://x", shaping_bandwidth_kbps=10,
                                     shaping_jitter_ms=5))


def test_bandwidth_shapes_both_directions_and_latency_splits_the_round_trip():
    shape = Shape("http://x", "minio", 1250, 101, 21)
    toxics = shape.toxics()
    assert [t["name"] for t in toxics] == shape.toxic_names()
    up, down, lat_up, lat_down = toxics
    assert (up["type"], up["stream"], up["attributes"]) == ("bandwidth", "upstream", {"rate": 1250})
    assert (down["stream"], down["attributes"]) == ("downstream", {"rate": 1250})
    # 101 ms round trip: 51 up (the request's side carries the odd
    # millisecond), 50 down; the jitter splits the same way.
    assert lat_up["attributes"] == {"latency": 51, "jitter": 11}
    assert lat_down["attributes"] == {"latency": 50, "jitter": 10}
    assert all(t["toxicity"] == 1.0 for t in toxics)


def test_latency_only_and_bandwidth_only_shapes_carry_only_their_toxics():
    assert [t["name"] for t in Shape("http://x", "minio", 0, 100, 0).toxics()] == [
        "vtop_run_latency_up", "vtop_run_latency_down"]
    assert [t["name"] for t in Shape("http://x", "minio", 500, 0, 0).toxics()] == [
        "vtop_run_bandwidth_up", "vtop_run_bandwidth_down"]


# --------------------------------------------------------------------------
# Applying it
# --------------------------------------------------------------------------


def test_apply_claims_the_proxy_checks_it_and_installs_this_runs_toxics():
    client = ScriptedClient()
    shape = Shape("http://127.0.0.1:8474", "minio", 1250, 100, 20, "cafe0001")
    apply(shape, client)
    methods = [(m, p) for m, p, _ in client.calls]
    # The claim first, then the look, then the shape.
    assert methods[0] == ("POST", "/proxies/minio/toxics")
    assert client.calls[0][2]["name"] == LOCK_TOXIC
    assert client.calls[0][2]["attributes"] == {"latency": 0, "jitter": 0}, "shapes nothing"
    assert methods[1] == ("GET", "/proxies/minio")
    assert methods[2:] == [("POST", "/proxies/minio/toxics")] * 4
    assert [b["name"] for _, _, b in client.calls[2:]] == shape.toxic_names()
    assert all(n.startswith("vtop_cafe0001_") for n in shape.toxic_names())


def test_a_proxy_claimed_by_another_run_is_refused_at_the_claim():
    client = ScriptedClient(statuses={("POST", "/proxies/minio/toxics", LOCK_TOXIC): 409})
    with pytest.raises(ShapingError, match="claimed by another shaped run"):
        apply(Shape("http://x", "minio", 1250, 0, 0), client)
    assert len(client.calls) == 1, "refused at the claim: nothing read, nothing installed, nothing deleted"


def test_shaped_installs_the_shape_it_was_given():
    client = ScriptedClient()
    sc = scenario(shaping_api_url="http://127.0.0.1:8474", shaping_bandwidth_kbps=1250)
    given = Shape.from_scenario(sc)
    with shaped(sc, client_factory=lambda url: client, log=lambda _: None, shape=given) as shape:
        assert shape is given
    installed = [b["name"] for m, _, b in client.calls if m == "POST" and b["name"] != LOCK_TOXIC]
    assert installed == [t["name"] for t in given.toxics()], "the judged instance's token"
    assert all(n.startswith(f"vtop_{given.run_token}_") for n in installed)


def test_a_proxy_already_shaped_by_another_run_is_refused_not_replaced():
    class Occupied(ScriptedClient):
        def request(self, method, path, body=None):
            status, payload = super().request(method, path, body)
            if method == "GET":
                payload = dict(payload, toxics=[{"name": "vtop_deadbeef_latency_up", "enabled": True}])
            return status, payload

    client = Occupied()
    shape = Shape("http://x", "minio", 1250, 0, 0)
    with pytest.raises(ShapingError, match="another shaped run"):
        apply(shape, client)
    deleted = [p.rsplit("/", 1)[1] for m, p, _ in client.calls if m == "DELETE"]
    assert deleted == [*shape.toxic_names(), LOCK_TOXIC], "only this run's names, and its claim"
    assert "vtop_deadbeef_latency_up" not in deleted, "another run's toxics are never deleted"


def test_the_bundled_proxy_name_must_front_the_bundled_store():
    class Elsewhere(ScriptedClient):
        def request(self, method, path, body=None):
            status, payload = super().request(method, path, body)
            if method == "GET":
                payload = dict(payload, upstream="store.example:9000")
            return status, payload

    with pytest.raises(ShapingError, match="not the bundled MinIO"):
        apply(Shape("http://x", "minio", 1250, 0, 0), Elsewhere())
    # Another name may forward anywhere; it just gets no lab credentials.
    apply(Shape("http://x", "other", 1250, 0, 0), Elsewhere())


def test_a_foreign_toxic_on_the_proxy_is_refused_by_name():
    class Occupied(ScriptedClient):
        def request(self, method, path, body=None):
            status, payload = super().request(method, path, body)
            if method == "GET":
                payload = dict(payload, toxics=[{"name": "someones_slow_close", "enabled": True}])
            return status, payload

    with pytest.raises(ShapingError, match="someones_slow_close"):
        apply(Shape("http://x", "minio", 1250, 0, 0), Occupied())


def test_a_missing_proxy_names_the_file_that_registers_it():
    client = ScriptedClient(statuses={("POST", "/proxies/minio/toxics"): 404})
    with pytest.raises(ShapingError, match="toxiproxy.json"):
        apply(Shape("http://x", "minio", 1250, 0, 0), client)
    assert len(client.calls) == 1, "nothing is applied to a proxy that is not there"


def test_a_refused_toxic_fails_the_run_by_name():
    client = ScriptedClient(
        statuses={("POST", "/proxies/minio/toxics", "vtop_run_bandwidth_up"): 400})
    with pytest.raises(ShapingError, match="_bandwidth_up"):
        apply(Shape("http://x", "minio", 1250, 0, 0), client)


class HalfRefusingClient(ScriptedClient):
    """Accepts the first toxic and refuses the second."""

    def request(self, method, path, body=None):
        if method == "GET":
            return super().request(method, path, body)
        self.calls.append((method, path, body))
        if method == "POST":
            posts = sum(1 for m, _, _ in self.calls if m == "POST")
            return (201 if posts <= 2 else 400), None  # the claim, one toxic, then no
        return {"GET": 200, "DELETE": 204}[method], None


def test_half_a_shape_is_rolled_back_before_the_failure_is_reported():
    client = HalfRefusingClient()
    shape = Shape("http://x", "minio", 1250, 100, 0)
    with pytest.raises(ShapingError, match="removed again"):
        apply(shape, client)
    # After the refused POST: every toxic of this run deleted again, and the
    # claim released last.
    tail = [(m, p) for m, p, _ in client.calls[-5:]]
    assert tail == [("DELETE", f"/proxies/minio/toxics/{n}")
                    for n in [*shape.toxic_names(), LOCK_TOXIC]]


class FailingRemovalClient(ScriptedClient):
    """Removes cleanly before the shape is applied, and fails one removal
    on the way out — the leftover this test is about."""

    def request(self, method, path, body=None):
        if method == "GET":
            return super().request(method, path, body)
        applied = any(m == "POST" for m, _, _ in self.calls)
        self.calls.append((method, path, body))
        if method == "DELETE" and applied and path.endswith("_latency_up"):
            return 500, None
        return {"GET": 200, "POST": 201, "DELETE": 204}[method], None


def test_a_removal_that_fails_is_reported_not_announced_as_removed():
    client = FailingRemovalClient()
    sc = scenario(shaping_api_url="http://127.0.0.1:8474", shaping_latency_ms=100)
    with pytest.raises(ShapingError, match="_latency_up \\(HTTP 500\\)"):
        with shaped(sc, client_factory=lambda url: client, log=lambda _: None):
            pass
    # Every name was still tried before the report.
    deletes = [p for m, p, _ in client.calls if m == "DELETE"]
    assert len(deletes) == len(TOXIC_KINDS) + 1, "every name, and the claim, tried on exit"


def test_an_endpoint_that_is_not_the_proxys_listener_is_refused():
    client = ScriptedClient()
    shape = Shape("http://x", "minio", 1250, 0, 0)
    apply(shape, client, endpoint="http://localhost:9100")
    with pytest.raises(ShapingError, match="listens on port 9100"):
        apply(shape, client, endpoint="http://localhost:9000")


def test_the_recorded_shape_says_it_is_per_connection():
    assert Shape("http://x", "minio", 1250, 100, 20).describe()["scope"] == "per_connection"


def test_an_endpoint_override_that_bypasses_the_proxy_is_refused():
    sc = scenario(shaping_api_url="http://127.0.0.1:8474", shaping_bandwidth_kbps=10,
                  endpoint_url="http://localhost:9100", backend="s3_native")
    require_endpoint_through_proxy(sc, "http://localhost:9100")
    with pytest.raises(ShapingError, match="around it"):
        require_endpoint_through_proxy(sc, "http://localhost:9000")


def test_only_a_backend_that_dials_the_endpoint_can_be_shaped():
    sc = scenario(shaping_api_url="http://127.0.0.1:8474", shaping_bandwidth_kbps=10,
                  endpoint_url="http://localhost:9100", backend="mock")
    with pytest.raises(ShapingError, match="never dials endpoint_url"):
        require_endpoint_through_proxy(sc, "http://localhost:9100")


def test_a_remote_store_on_the_proxys_port_is_not_the_proxy():
    sc = scenario(shaping_api_url="http://127.0.0.1:8474", shaping_bandwidth_kbps=10,
                  endpoint_url="http://store.example:9100", backend="s3_native")
    with pytest.raises(ShapingError, match="not the proxy"):
        require_endpoint_through_proxy(sc, "http://store.example:9100")


def test_the_shape_is_removed_on_every_exit():
    client = ScriptedClient()
    sc = scenario(shaping_api_url="http://127.0.0.1:8474", shaping_bandwidth_kbps=1250)
    lines = []
    with shaped(sc, client_factory=lambda url: client, log=lines.append) as shape:
        assert shape.bandwidth_kbps == 1250
        applied = len(client.calls)
    removed = client.calls[applied:]
    assert removed == [("DELETE", f"/proxies/minio/toxics/{n}", None)
                       for n in [*shape.toxic_names(), LOCK_TOXIC]]
    assert any("shaping minio" in line for line in lines)
    assert any("removed" in line for line in lines)

    # A failing block still unshapes the pipe: the next scenario must not
    # inherit a constraint it never asked for.
    client = ScriptedClient()
    with pytest.raises(RuntimeError, match="boom"):
        with shaped(sc, client_factory=lambda url: client, log=lambda _: None) as shape:
            names = shape.toxic_names()
            raise RuntimeError("boom")
    assert client.calls[-5:] == [("DELETE", f"/proxies/minio/toxics/{n}", None)
                                 for n in [*names, LOCK_TOXIC]]


def test_an_unshaped_scenario_touches_nothing():
    calls = []
    with shaped(scenario(), client_factory=lambda url: calls.append(url), log=lambda _: None) as shape:
        assert shape is None
    assert calls == []


# --------------------------------------------------------------------------
# The scenario file and the lab endpoint
# --------------------------------------------------------------------------


def test_the_shaped_soak_scenario_carries_its_shape(tmp_path):
    here = __import__("os").path.dirname(__file__)
    path = __import__("os").path.join(here, "..", "scenarios", "13-backpressure-soak-shaped.yaml")
    sc = load_scenario(path)
    shape = Shape.from_scenario(sc)
    assert (shape.api_url, shape.proxy, shape.bandwidth_kbps, shape.latency_ms, shape.jitter_ms) == (
        "http://127.0.0.1:8474", "minio", 1250, 100, 20)
    assert sc.endpoint_url == "http://localhost:9100", "the engine talks to the proxy"
    assert sc.backend == "s3_native" and sc.seed_concurrently is True
    assert sc.max_concurrent_batches == 1, "one connection is the pipe"


def test_a_flat_scenario_file_parses_the_shaping_keys_without_pyyaml(tmp_path):
    p = tmp_path / "s.yaml"
    p.write_text(textwrap.dedent("""
        backend: s3_native
        endpoint_url: http://localhost:9100
        shaping_api_url: http://127.0.0.1:8474
        shaping_bandwidth_kbps: 640
        shaping_latency_ms: 80
    """), encoding="utf-8")
    sc = load_scenario(str(p))
    shape = Shape.from_scenario(sc)
    assert (shape.api_url, shape.proxy, shape.bandwidth_kbps, shape.latency_ms, shape.jitter_ms) == (
        "http://127.0.0.1:8474", "minio", 640, 80, 0)


def test_the_lab_credentials_follow_the_bundled_proxy_only():
    from lib.engine import _shaped_by_the_bundled_proxy
    assert _shaped_by_the_bundled_proxy(scenario(shaping_api_url="http://x"))
    assert _shaped_by_the_bundled_proxy(scenario(shaping_api_url="http://x", shaping_proxy="minio"))
    assert not _shaped_by_the_bundled_proxy(scenario(shaping_api_url="http://x", shaping_proxy="other"))
    assert not _shaped_by_the_bundled_proxy(scenario())


def test_the_shaped_proxy_port_is_the_lab_only_when_shaped():
    # The proxy fronts the same lab MinIO, so its loopback port keeps the lab
    # credential fallbacks — for a shaped scenario. Unshaped, 9100 is
    # somebody else's service and gets no keys.
    assert _is_lab_endpoint("http://localhost:9100", shaped=True)
    assert not _is_lab_endpoint("http://localhost:9100")
    assert _is_lab_endpoint("http://127.0.0.1:9000")
    assert _is_lab_endpoint("http://127.0.0.1:9000", shaped=True)
    assert not _is_lab_endpoint("http://localhost:4566", shaped=True)


# --------------------------------------------------------------------------
# The human-facing cell (#477): summary.md's shaped-pipe row, now that two
# drivers write it
# --------------------------------------------------------------------------


def full_netem_shape(**overrides) -> NetemShape:
    """A netem shape with every knob turned on, so a renderer that silently
    drops one is caught here rather than by an operator wondering why a run
    that was policed does not say so."""
    return NetemShape(**{"latency_ms": 100, "jitter_ms": 20, "loss_pct": 1.0,
                         "loss_model": "gemodel", "bottleneck_kbps": 10000,
                         "buffer_bdp": 1.0, "policer_kbps": 5000, **overrides})


def test_the_netem_cell_names_its_driver_and_its_rate_in_the_unit_it_was_recorded_in():
    # The bug this pins: the cell used to read toxiproxy's `proxy` and
    # `bandwidth_kbps` off a netem record, which carries neither, and label the
    # result KB/s. A 10 Mbit/s bottleneck therefore printed as
    # `None: unlimited KB/s aggregate l3, 100 ms RTT ±20 ms` — no driver, no
    # rate, and the other driver's unit — in the one artifact the runner points
    # an operator at when the run finishes.
    cell = _shaping_cell({"shaping": full_netem_shape().describe()})
    assert cell == ("netem: 10000 kbit/s bottleneck aggregate l3, 1.0x BDP buffer, "
                    "100 ms RTT ±20 ms, 1.0% loss (gemodel) on upload, "
                    "policed at 5000 kbit/s"), (
        "the summary's shaped-pipe row is where a p95 is read together with the link that "
        "produced it; every part of the link a netem scenario can ask for has to be in it, "
        "or the number is read against a pipe the reader has to guess at")
    assert "unlimited" not in cell, (
        "a configured bottleneck reported as unlimited is the original defect: it hides the "
        "constraint the whole run was measuring against")
    assert "KB/s" not in cell, (
        "tc's rates are KILOBITS; labelling them KB/s reads the link as eight times its "
        "real size, which is worse than printing no unit at all")


def test_the_netem_cell_states_only_the_impairments_the_scenario_asked_for():
    # A cell that printed "0% loss" and "unlimited kbit/s" on every row would
    # bury the rows that DO lose packets, and those are the rows the netem
    # driver exists to produce. Scenario 15 is the real shape here: a policer
    # and no bottleneck, so no queue and no BDP buffer to report.
    policed = _shaping_cell({"shaping": NetemShape(latency_ms=100,
                                                   policer_kbps=10000).describe()})
    assert policed == ("netem: unlimited kbit/s bottleneck aggregate l3, 100 ms RTT ±0 ms, "
                       "policed at 10000 kbit/s"), (
        "a policer holds no queue, so there is no buffer depth to name; saying '0.0x BDP' "
        "would describe a bottleneck the scenario deliberately did not configure")
    assert "loss" not in policed, (
        "scenario 15's drops are the POLICER's, not netem's loss model; a '0% loss' phrase "
        "here invites the reader to conclude nothing was dropped")
    lossy = _shaping_cell({"shaping": NetemShape(latency_ms=100, loss_pct=0.5,
                                                 loss_model="gemodel").describe()})
    assert "0.5% loss (gemodel) on upload" in lossy, (
        "the MODEL travels with the percentage: a gemodel row and a random row at the same "
        "average loss are the same number and a different link, and the direction is stated "
        "because loss is applied to the data path alone")


def test_the_cell_reads_its_unit_off_the_record_and_never_assumes_one():
    # The two drivers disagree about what a "kbps" is, so the unit is data, not
    # a constant in the renderer. Swapping only `rate_unit` must move the label
    # — if it does not, some branch is hardcoding a unit and will eventually
    # hardcode the wrong one.
    record = full_netem_shape().describe()
    assert "10000 kbit/s" in describe_shape_line(record)
    assert "10000 KB/s" in describe_shape_line({**record, "rate_unit": "kilobytes_per_second"}), (
        "the label follows the recorded unit, or the renderer is deciding the unit itself "
        "and a third driver would be mislabelled the day it is added")
    # An unknown unit is printed as recorded rather than guessed at: a cell that
    # reads oddly can be traced back to the run that wrote it, while a cell that
    # confidently names the wrong unit cannot be told from a correct one.
    assert "furlongs_per_fortnight" in describe_shape_line(
        {**record, "rate_unit": "furlongs_per_fortnight"})


def test_the_toxiproxy_cell_still_says_everything_it_said_before_and_now_names_its_driver():
    cell = _shaping_cell({"shaping": Shape("http://x", "minio", 1250, 100, 20).describe()})
    assert cell.endswith("minio: 1250 KB/s per connection, 100 ms RTT ±20 ms"), (
        "every shaped run recorded since #403 reads this way, and a results directory is "
        "compared across releases: the toxiproxy row must not be reworded out from under "
        "the numbers already filed against it")
    assert cell.startswith("toxiproxy "), (
        "two drivers write this row now, and toxiproxy's KB/s and netem's kbit/s look "
        "identical in a column; the driver is what tells a reader which one to read")


def test_an_unshaped_run_still_renders_as_unshaped():
    # Most scenarios in the suite are unshaped, so this is the cell that
    # appears most often; a driver name or a rate here would claim a pipe that
    # was never in the path.
    assert _shaping_cell({}) == "none (unshaped)"
    assert _shaping_cell({"shaping": None}) == "none (unshaped)"


# --------------------------------------------------------------------------
# Knobs the selected driver does not read (#477)
# --------------------------------------------------------------------------


def netem_scenario(**overrides) -> Scenario:
    """A netem scenario the way the loader produces one: every key in the
    schema present, because that is what makes "did the author set this" a
    question about defaults rather than about emptiness."""
    return Scenario({**DEFAULTS, "shaping_driver": "netem",
                     "shaping_latency_ms": 100, **overrides})


def test_a_toxiproxy_knob_under_the_netem_driver_is_refused_rather_than_ignored():
    # The conversion this refusal is about: a 1250 KB/s toxiproxy scenario is
    # copied, `shaping_driver` is changed to netem, and the rate is left behind.
    # Nothing in the netem driver reads it, so the link came out UNLIMITED while
    # summary.scenario still showed the rate that was asked for and the resolved
    # shaping column was blank — a corrupted experiment with no refusal in it.
    with pytest.raises(ValueError, match="only the toxiproxy driver reads") as exc:
        shape_from_scenario(netem_scenario(shaping_bandwidth_kbps=1250))
    assert "shaping_bandwidth_kbps" in str(exc.value), (
        "the refusal names the key to move or remove, or the author has to bisect their own "
        "scenario file to find out which line was ignored")
    assert "KILOBYTES" in str(exc.value) and "KILOBITS" in str(exc.value), (
        "the message must say why the harness will not just convert the number: the two "
        "drivers' rates differ by a factor of eight, and a silent conversion would file a "
        "measurement against a pipe nobody configured")
    # The other two toxiproxy-only keys carry the same hazard: an api_url points
    # at a shaper this run never contacts, and a proxy name that is not the
    # default says the author believed a proxy was in the path.
    with pytest.raises(ValueError, match="shaping_api_url"):
        shape_from_scenario(netem_scenario(shaping_api_url="http://127.0.0.1:8474"))
    with pytest.raises(ValueError, match="shaping_proxy"):
        shape_from_scenario(netem_scenario(shaping_proxy="other"))


def test_a_toxiproxy_key_left_at_its_default_is_not_a_choice_the_author_made():
    # The trap this guard was written around, and the reason it compares
    # against the SCHEMA rather than testing for emptiness: `shaping_proxy`
    # defaults to the non-empty string "minio", so an emptiness test would read
    # the loader's own stamp as the author's intent and refuse every netem
    # scenario in the tree before it seeded a byte.
    assert DEFAULTS["shaping_proxy"] == "minio", (
        "if this default ever becomes empty the guard still works, but the test below stops "
        "proving the thing it was written to prove")
    assert shape_from_scenario(netem_scenario()) is not None, (
        "a netem scenario carrying the loader's defaults for the toxiproxy keys must load; "
        "it named no proxy, it just failed to delete keys it never wrote")
    assert shape_from_scenario(netem_scenario(shaping_proxy="minio",
                                              shaping_api_url="",
                                              shaping_bandwidth_kbps=0)) is not None, (
        "and spelling the defaults out by hand is the same statement as leaving them out")


def test_a_netem_knob_under_the_toxiproxy_driver_is_still_refused():
    # The original direction, re-pinned here because both guards now share
    # `_left_at_default` and a change to one can silently break the other.
    with pytest.raises(ValueError, match="only the netem driver reads") as exc:
        shape_from_scenario(Scenario({**DEFAULTS,
                                      "shaping_api_url": "http://127.0.0.1:8474",
                                      "shaping_bandwidth_kbps": 1250,
                                      "shaping_loss_pct": 1.0}))
    assert "shaping_loss_pct" in str(exc.value)
    assert shape_from_scenario(Scenario({**DEFAULTS,
                                         "shaping_api_url": "http://127.0.0.1:8474",
                                         "shaping_bandwidth_kbps": 1250})) is not None, (
        "and the defaults may not refuse a toxiproxy shape the harness has been recording "
        "since #403")


def test_every_bundled_scenario_still_loads_under_both_driver_guards():
    # A guard that reads a default as a choice refuses the whole suite, and it
    # does so at load time, before a byte is seeded — so the cheapest place to
    # catch it is here, over every scenario the repository ships.
    paths = sorted(os.path.join(SCENARIO_DIR, f) for f in os.listdir(SCENARIO_DIR)
                   if f.endswith((".yaml", ".yml")))
    assert len(paths) >= 13, (
        "the sweep found almost no scenarios, so it is proving nothing: the directory moved "
        "or the suffix filter stopped matching")
    drivers = set()
    for path in paths:
        shape = shape_from_scenario(load_scenario(path))
        if shape is not None:
            drivers.add(shape.driver)
    assert drivers == {"toxiproxy", "netem"}, (
        f"the sweep exercised {sorted(drivers)}: both guards are only proven harmless once a "
        "real scenario of each driver has been through them, and the netem scenarios (14-16) "
        "are the ones the new refusal could break")
