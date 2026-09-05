"""Tests for benchmarks/lib/shaping.py (#403): the shape a scenario asks for,
the toxics it becomes, and the apply/remove discipline around a run — all
against a scripted client, so no socket is ever opened."""

import textwrap

import pytest

from lib.engine import _is_lab_endpoint
from lib.scenario import Scenario, load_scenario
from lib.shaping import (
    TOXIC_NAMES,
    Shape,
    ShapingError,
    apply,
    require_endpoint_through_proxy,
    shaped,
)


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
        status = self.statuses.get((method, path), default)
        payload = {"name": "minio", "listen": "[::]:9100"} if method == "GET" else None
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
    assert shape == Shape("http://127.0.0.1:8474", "minio", 1250, 100, 20)
    assert shape.describe() == {"proxy": "minio", "bandwidth_kbps": 1250,
                                "latency_ms": 100, "jitter_ms": 20,
                                "scope": "per_connection"}


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
        applied = any(m == "POST" for m, _, _ in self.calls)
        self.calls.append((method, path, body))
        if method == "DELETE" and applied and path.endswith("vtop_bandwidth_up"):
            raise ShapingError("toxiproxy at http://x is not answering (connection reset)")
        return {"GET": 200, "POST": 201, "DELETE": 204}[method], None


def test_cleanup_tries_every_toxic_past_a_transport_error():
    client = ResettingRemovalClient()
    sc = scenario(shaping_api_url="http://127.0.0.1:8474", shaping_bandwidth_kbps=1250,
                  shaping_latency_ms=100)
    with pytest.raises(ShapingError, match="vtop_bandwidth_up \\(toxiproxy"):
        with shaped(sc, client_factory=lambda url: client, log=lambda _: None):
            pass
    tail = [p for m, p, _ in client.calls[-4:] if m == "DELETE"]
    assert len(tail) == 4, "the other three were still attempted"


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
    assert [t["name"] for t in toxics] == list(TOXIC_NAMES)
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
        "vtop_latency_up", "vtop_latency_down"]
    assert [t["name"] for t in Shape("http://x", "minio", 500, 0, 0).toxics()] == [
        "vtop_bandwidth_up", "vtop_bandwidth_down"]


# --------------------------------------------------------------------------
# Applying it
# --------------------------------------------------------------------------


def test_apply_checks_the_proxy_replaces_our_toxics_and_installs_the_shape():
    client = ScriptedClient()
    shape = Shape("http://127.0.0.1:8474", "minio", 1250, 100, 20)
    apply(shape, client)
    methods = [(m, p) for m, p, _ in client.calls]
    assert methods[0] == ("GET", "/proxies/minio")
    # Ours are removed first, by name, so an interrupted run's leftovers are
    # replaced rather than stacked.
    assert methods[1:5] == [("DELETE", f"/proxies/minio/toxics/{n}") for n in TOXIC_NAMES]
    assert methods[5:] == [("POST", "/proxies/minio/toxics")] * 4
    assert [b["name"] for _, _, b in client.calls[5:]] == list(TOXIC_NAMES)


def test_a_foreign_toxic_on_the_proxy_is_refused_by_name():
    class Occupied(ScriptedClient):
        def request(self, method, path, body=None):
            status, payload = super().request(method, path, body)
            if method == "GET":
                payload = dict(payload, toxics=[
                    {"name": "vtop_latency_up", "enabled": True},   # ours: replaced, not refused
                    {"name": "someones_slow_close", "enabled": True},
                ])
            return status, payload

    with pytest.raises(ShapingError, match="someones_slow_close"):
        apply(Shape("http://x", "minio", 1250, 0, 0), Occupied())

    class OnlyOurs(ScriptedClient):
        def request(self, method, path, body=None):
            status, payload = super().request(method, path, body)
            if method == "GET":
                payload = dict(payload, toxics=[{"name": "vtop_bandwidth_up", "enabled": True}])
            return status, payload

    apply(Shape("http://x", "minio", 1250, 0, 0), OnlyOurs())


def test_a_missing_proxy_names_the_file_that_registers_it():
    client = ScriptedClient(statuses={("GET", "/proxies/minio"): 404})
    with pytest.raises(ShapingError, match="toxiproxy.json"):
        apply(Shape("http://x", "minio", 1250, 0, 0), client)
    assert len(client.calls) == 1, "nothing is applied to a proxy that is not there"


def test_a_refused_toxic_fails_the_run_by_name():
    client = ScriptedClient(statuses={("POST", "/proxies/minio/toxics"): 400})
    with pytest.raises(ShapingError, match="vtop_bandwidth_up"):
        apply(Shape("http://x", "minio", 1250, 0, 0), client)


class HalfRefusingClient(ScriptedClient):
    """Accepts the first toxic and refuses the second."""

    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        if method == "POST":
            posts = sum(1 for m, _, _ in self.calls if m == "POST")
            return (201 if posts == 1 else 400), None
        return {"GET": 200, "DELETE": 204}[method], None


def test_half_a_shape_is_rolled_back_before_the_failure_is_reported():
    client = HalfRefusingClient()
    with pytest.raises(ShapingError, match="removed again"):
        apply(Shape("http://x", "minio", 1250, 100, 0), client)
    # After the refused POST: every toxic of ours deleted again.
    tail = [(m, p) for m, p, _ in client.calls[-4:]]
    assert tail == [("DELETE", f"/proxies/minio/toxics/{n}") for n in TOXIC_NAMES]


class FailingRemovalClient(ScriptedClient):
    """Removes cleanly before the shape is applied, and fails one removal
    on the way out — the leftover this test is about."""

    def request(self, method, path, body=None):
        applied = any(m == "POST" for m, _, _ in self.calls)
        self.calls.append((method, path, body))
        if method == "DELETE" and applied and path.endswith("vtop_latency_up"):
            return 500, None
        return {"GET": 200, "POST": 201, "DELETE": 204}[method], None


def test_a_removal_that_fails_is_reported_not_announced_as_removed():
    client = FailingRemovalClient()
    sc = scenario(shaping_api_url="http://127.0.0.1:8474", shaping_latency_ms=100)
    with pytest.raises(ShapingError, match="vtop_latency_up \\(HTTP 500\\)"):
        with shaped(sc, client_factory=lambda url: client, log=lambda _: None):
            pass
    # Every name was still tried before the report.
    deletes = [p for m, p, _ in client.calls if m == "DELETE"]
    assert len(deletes) >= 2 * len(TOXIC_NAMES), "cleared before apply and again on exit"


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
    assert removed == [("DELETE", f"/proxies/minio/toxics/{n}", None) for n in TOXIC_NAMES]
    assert any("shaping minio" in line for line in lines)
    assert any("removed" in line for line in lines)

    # A failing block still unshapes the pipe: the next scenario must not
    # inherit a constraint it never asked for.
    client = ScriptedClient()
    with pytest.raises(RuntimeError, match="boom"):
        with shaped(sc, client_factory=lambda url: client, log=lambda _: None):
            raise RuntimeError("boom")
    assert client.calls[-4:] == [("DELETE", f"/proxies/minio/toxics/{n}", None)
                                 for n in TOXIC_NAMES]


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
    assert shape == Shape("http://127.0.0.1:8474", "minio", 1250, 100, 20)
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
    assert Shape.from_scenario(sc) == Shape("http://127.0.0.1:8474", "minio", 640, 80, 0)


def test_the_shaped_proxy_port_is_the_lab_only_when_shaped():
    # The proxy fronts the same lab MinIO, so its loopback port keeps the lab
    # credential fallbacks — for a shaped scenario. Unshaped, 9100 is
    # somebody else's service and gets no keys.
    assert _is_lab_endpoint("http://localhost:9100", shaped=True)
    assert not _is_lab_endpoint("http://localhost:9100")
    assert _is_lab_endpoint("http://127.0.0.1:9000")
    assert _is_lab_endpoint("http://127.0.0.1:9000", shaped=True)
    assert not _is_lab_endpoint("http://localhost:4566", shaped=True)
