"""Bandwidth shaping between the engine and the store (#403).

A thin pipe is a RATE, not a loss. The loopback lab never shows what the
engine does when the upload path is the bottleneck, and that is the normal
condition at an edge site archiving over a WAN to a store it does not own.
toxiproxy sits between the engine and MinIO — no root, no CAP_NET_ADMIN,
scriptable per scenario — and its bandwidth and latency toxics turn the pipe
into the constraint the soak measures against.

The scenario names the toxiproxy API, the proxy, and the shape; the runner
applies the toxics for the run and removes them afterwards, whatever the run
did. Flat keys, like every other scenario knob (the fallback parser is flat by
design):

    shaping_api_url: http://127.0.0.1:8474   # "" = unshaped, the default
    shaping_proxy: minio                     # registered from toxiproxy.json
    shaping_bandwidth_kbps: 1250             # KB/s each way PER CONNECTION; 0 = unlimited
    shaping_latency_ms: 100                  # round trip, split across directions
    shaping_jitter_ms: 20                    # spread on that round trip

The bandwidth toxic is PER CONNECTION (toxiproxy limits each connection it
proxies), so the aggregate pipe is the shape times the connections in flight.
A scenario that wants a fixed pipe keeps one upload in flight
(`max_concurrent_batches: 1`); scenario 13 does. The recorded shape says so.

Nothing here imports the engine: it drives toxiproxy's HTTP API with the
standard library, the way the rest of the harness drives the binary.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

# Ours, by name, so a stale toxic from an interrupted run is replaced rather
# than stacked, and nothing an operator added by hand is ever touched.
TOXIC_NAMES = (
    "vtop_bandwidth_up",
    "vtop_bandwidth_down",
    "vtop_latency_up",
    "vtop_latency_down",
)

REQUEST_TIMEOUT_SECONDS = 5.0


class ShapingError(RuntimeError):
    """The shape a scenario asked for could not be applied.

    Raised, never logged and skipped: a shaped scenario that ran unshaped
    would file a number under the wrong name, which is worse than no number.
    """


def _non_negative_int(value: Any, key: str) -> int:
    """An integer, exactly (review): `int()` would quietly turn 1.9 into 1
    and True into 1, and the run would then apply and record a shape the
    scenario never asked for."""
    if isinstance(value, bool):
        raise ValueError(f"{key} must be an integer, got {value!r}")
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{key} must be an integer, got {value!r}")
        value = int(value)
    try:
        out = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be an integer, got {value!r}") from exc
    if out < 0:
        raise ValueError(f"{key} must be >= 0, got {out}")
    return out


@dataclass(frozen=True)
class Shape:
    api_url: str
    proxy: str
    bandwidth_kbps: int
    latency_ms: int
    jitter_ms: int

    @classmethod
    def from_scenario(cls, scenario) -> Shape | None:
        """The shape a scenario asks for, or None for an unshaped run.

        Validated when the scenario is loaded, not when the toxics are
        applied: a bad knob must fail before the seed data exists.
        """
        api = str(scenario.get("shaping_api_url", "") or "").strip()
        if not api:
            return None
        bandwidth = _non_negative_int(
            scenario.get("shaping_bandwidth_kbps", 0), "shaping_bandwidth_kbps")
        latency = _non_negative_int(
            scenario.get("shaping_latency_ms", 0), "shaping_latency_ms")
        jitter = _non_negative_int(
            scenario.get("shaping_jitter_ms", 0), "shaping_jitter_ms")
        if bandwidth == 0 and latency == 0:
            raise ValueError(
                "shaping_api_url is set but shaping_bandwidth_kbps and "
                "shaping_latency_ms are both 0: a shaped run with no shape is an "
                "unshaped run filed under the wrong name")
        if jitter and not latency:
            raise ValueError("shaping_jitter_ms needs a shaping_latency_ms to spread")
        proxy = str(scenario.get("shaping_proxy", "minio") or "minio").strip()
        return cls(api.rstrip("/"), proxy, bandwidth, latency, jitter)

    def toxics(self) -> list[dict[str, Any]]:
        """The toxics, in the order they are applied.

        Bandwidth in BOTH directions: the upload's bytes go up, and a
        response that trickles back holds the request open just as long.
        Latency is a round trip, so each direction carries half of it — the
        odd millisecond rides upstream, where the request is.
        """
        out: list[dict[str, Any]] = []
        if self.bandwidth_kbps:
            for name, stream in (("vtop_bandwidth_up", "upstream"),
                                 ("vtop_bandwidth_down", "downstream")):
                out.append({"name": name, "type": "bandwidth", "stream": stream,
                            "toxicity": 1.0,
                            "attributes": {"rate": self.bandwidth_kbps}})
        if self.latency_ms:
            down, odd = divmod(self.latency_ms, 2)
            jitter_down, jitter_odd = divmod(self.jitter_ms, 2)
            out.append({"name": "vtop_latency_up", "type": "latency",
                        "stream": "upstream", "toxicity": 1.0,
                        "attributes": {"latency": down + odd,
                                       "jitter": jitter_down + jitter_odd}})
            out.append({"name": "vtop_latency_down", "type": "latency",
                        "stream": "downstream", "toxicity": 1.0,
                        "attributes": {"latency": down, "jitter": jitter_down}})
        return out

    def describe(self) -> dict[str, Any]:
        """What the summary records: the shape, so a number is never read
        without the pipe it was measured through — and its scope, because
        toxiproxy's bandwidth toxic limits each CONNECTION, so the aggregate
        is this times the connections in flight."""
        return {"proxy": self.proxy, "bandwidth_kbps": self.bandwidth_kbps,
                "latency_ms": self.latency_ms, "jitter_ms": self.jitter_ms,
                "scope": "per_connection"}


class ToxiproxyClient:
    """The few calls the shaper makes, over the standard library.

    `opener` is injectable so the tests never open a socket.
    """

    def __init__(self, api_url: str, opener: Callable[..., Any] | None = None):
        self.api_url = api_url.rstrip("/")
        self._open = opener or urllib.request.urlopen

    def request(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        data = None if body is None else json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json"} if data is not None else {}
        req = urllib.request.Request(self.api_url + path, data=data, method=method,
                                     headers=headers)
        try:
            with self._open(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as exc:
            return exc.code, None
        except (urllib.error.URLError, OSError) as exc:
            raise ShapingError(
                f"toxiproxy at {self.api_url} is not answering ({exc}): start the "
                "shaped stack — docker compose -f benchmarks/docker-compose.benchmark.yml "
                "--profile shaped up -d") from exc


def _port_of(endpoint: str) -> int | None:
    try:
        return urlsplit(endpoint).port
    except ValueError:
        return None


def _listener_port(listen: Any) -> int | None:
    # toxiproxy reports "[::]:9100" or "0.0.0.0:9100"; the port is what the
    # engine's endpoint must name.
    if not isinstance(listen, str) or ":" not in listen:
        return None
    try:
        return int(listen.rsplit(":", 1)[1])
    except ValueError:
        return None


def apply(shape: Shape, client: ToxiproxyClient, endpoint: str | None = None) -> None:
    """Install the shape on its proxy, replacing any toxic of ours already
    there. With `endpoint`, the engine's effective endpoint must name the
    proxy's own listener port: a scenario declaring the store's direct port
    would otherwise bypass every toxic while the summary recorded a shape."""
    status, proxy = client.request("GET", f"/proxies/{shape.proxy}")
    if status == 404:
        raise ShapingError(
            f"toxiproxy has no proxy {shape.proxy!r}: the shaped stack registers it "
            "from benchmarks/toxiproxy.json at startup")
    if status != 200:
        raise ShapingError(f"toxiproxy answered HTTP {status} for proxy {shape.proxy!r}")
    # A toxic that is not ours (review) would stack with the shape and go
    # unrecorded: refused by name, never measured through.
    foreign = sorted(
        str(t.get("name")) for t in ((proxy or {}).get("toxics") or [])
        if isinstance(t, dict) and t.get("name") not in TOXIC_NAMES and t.get("enabled", True))
    if foreign:
        raise ShapingError(
            f"proxy {shape.proxy!r} already carries toxic(s) {foreign} that are not this "
            "harness's: they would shape the run on top of the scenario's shape and the "
            "summary would not say so — remove them, or run without this proxy")
    if endpoint is not None:
        listener = _listener_port((proxy or {}).get("listen"))
        wanted = _port_of(endpoint)
        if listener is None or wanted != listener:
            raise ShapingError(
                f"the engine's endpoint {endpoint!r} does not reach proxy {shape.proxy!r}, "
                f"which listens on port {listener}: the run would go around the shape")
    clear(shape, client)
    for toxic in shape.toxics():
        try:
            status, _ = client.request("POST", f"/proxies/{shape.proxy}/toxics", toxic)
        except ShapingError:
            # Half a shape is not a shape: what was accepted is removed
            # before the failure is reported, so nothing lingers for the
            # next run.
            clear(shape, client)
            raise
        if status not in (200, 201):
            clear(shape, client)
            raise ShapingError(
                f"toxiproxy refused toxic {toxic['name']} on {shape.proxy!r} (HTTP {status}); "
                "the toxics accepted before it were removed again")


def clear(shape: Shape, client: ToxiproxyClient) -> None:
    """Remove our toxics, by name. Absent ones are fine; a removal that FAILS
    is reported after every name was tried, because a toxic left behind
    shapes the next run without saying so."""
    stayed = []
    for name in TOXIC_NAMES:
        # Every name is tried whatever the previous one did (review): a
        # reset on one removal must not leave the others installed.
        try:
            status, _ = client.request("DELETE", f"/proxies/{shape.proxy}/toxics/{name}")
        except ShapingError as exc:
            stayed.append(f"{name} ({exc})")
            continue
        if status not in (200, 204, 404):
            stayed.append(f"{name} (HTTP {status})")
    if stayed:
        raise ShapingError(
            f"toxiproxy did not remove {', '.join(stayed)} from {shape.proxy!r}: the pipe is "
            "still shaped — remove them by hand or restart the shaped stack")


SHAPEABLE_BACKENDS = ("s3_native",)


def _host_of(url: str) -> str | None:
    """The host, with the loopback spellings unified: the proxy is published
    on the same host the toxiproxy API is, and 'localhost' and '127.0.0.1'
    are that host twice."""
    try:
        host = urlsplit(url).hostname
    except ValueError:
        return None
    if host in ("localhost", "127.0.0.1", "::1"):
        return "loopback"
    return host


def require_endpoint_through_proxy(scenario, effective_endpoint: str) -> None:
    """A shaped scenario's engine must reach the store through the proxy the
    shape is on. The scenario's own `endpoint_url` names the proxy; an
    environment override that differs would bypass the toxics while the
    summary recorded a shape. Only a backend that dials the endpoint can be
    shaped at all — a mock or local backend measures nothing through it —
    and the endpoint's host must be the host the toxiproxy API is reached
    at: a remote store on the proxy's port is not the proxy."""
    backend = str(scenario.get("backend", "") or "")
    if backend not in SHAPEABLE_BACKENDS:
        raise ShapingError(
            f"backend {backend!r} never dials endpoint_url, so a shape on it would measure "
            f"nothing through the pipe and record a shape anyway; a shaped scenario needs one "
            f"of {list(SHAPEABLE_BACKENDS)}")
    api_host = _host_of(str(scenario.get("shaping_api_url", "") or ""))
    endpoint_host = _host_of(effective_endpoint)
    if endpoint_host is None or endpoint_host != api_host:
        raise ShapingError(
            f"the endpoint {effective_endpoint!r} is on host {endpoint_host!r} but toxiproxy is "
            f"reached at host {api_host!r}: the proxy is published beside its API, and a store "
            "elsewhere on the same port is not the proxy")
    declared = str(scenario.get("endpoint_url", "") or "")
    if effective_endpoint != declared:
        raise ShapingError(
            f"the effective endpoint {effective_endpoint!r} is not the scenario's "
            f"{declared!r}: a shaped run must go through the proxy the shape is on, and "
            "VTOP_S3_ENDPOINT_URL is sending the engine around it — unset it, or point it "
            "at the proxy")


@contextmanager
def shaped(scenario, client_factory: Callable[[str], ToxiproxyClient] = ToxiproxyClient,
           log: Callable[[str], None] = print,
           endpoint: str | None = None) -> Iterator[Shape | None]:
    """Shape the pipe for the duration of the block, and unshape it after.

    The removal runs on every exit — a failed run, a keyboard interrupt — so
    a following unshaped scenario never inherits a pipe it did not ask for.
    """
    shape = Shape.from_scenario(scenario)
    if shape is None:
        yield None
        return
    client = client_factory(shape.api_url)
    apply(shape, client, endpoint)
    log(f"[bench] shaping {shape.proxy}: {shape.bandwidth_kbps or 'unlimited'} KB/s each way, "
        f"{shape.latency_ms} ms round trip ±{shape.jitter_ms} ms")
    try:
        yield shape
    finally:
        clear(shape, client)
        log(f"[bench] shaping removed from {shape.proxy}")
