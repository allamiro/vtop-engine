"""The proxy hop is a claim the harness has to be able to check (#484).

The lab's `h3` profile puts a TLS-terminating reverse proxy in front of MinIO
so there is something in the lab that speaks HTTP/3 at all. The first thing
measured through it is the EXISTING TCP path, because a proxy hop changes the
topology — an extra process, an extra TLS termination, an extra copy of every
byte — and a later transport comparison that used the direct number as its
baseline would attribute all of that to the wire.

That makes two harness decisions load-bearing, and neither shows up in a
result file:

  - whether the run was handed the lab's credentials. The store behind the
    proxy is the same store, but the port is not, and the credential
    predicate keys off host AND port. Get it wrong in one direction and every
    upload fails authentication, which reads as a transport result; get it
    wrong in the other and a stranger's loopback TLS service is handed the
    lab's keys.
  - whether the run actually went through the proxy. An endpoint override
    outranks the scenario file, so a run can declare the hop and take the
    direct path, and nothing in the numbers would say so.

Both are pinned here, together with the bundled scenario that depends on them.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib import engine  # noqa: E402
from lib.scenario import DEFAULTS, Scenario, load_scenario  # noqa: E402

SCENARIO_14 = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "scenarios", "17-proxy-hop-tcp-baseline.yaml")

PROXY_ENDPOINT = "https://localhost:9443"


def scenario(**overrides):
    values = dict(DEFAULTS)
    values.update(overrides)
    return Scenario(values)


def proxied(**overrides):
    """A scenario that declares the hop and points at the proxy."""
    values = {"backend": "s3_native", "endpoint_url": PROXY_ENDPOINT,
              "h3_proxy": True}
    values.update(overrides)
    return scenario(**values)


@pytest.fixture(autouse=True)
def a_lab_this_machine_has_not_already_configured(tmp_path, monkeypatch):
    """Neither the developer's shell nor their `.env` may answer for a case.

    The port the proxy is published on is resolved the way compose resolves it
    — the shell, then `benchmarks/.env` — so as soon as an operator files
    `VTOP_H3_PORT` there to dodge a busy 9443, every expectation in this module
    that spells 9443 is answered by their machine rather than by the code under
    test. The endpoints below name the default because that is what the bundled
    scenario names; the override has its own module.
    """
    monkeypatch.delenv("VTOP_H3_PORT", raising=False)
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / "no-such.env"))


@pytest.fixture
def clean_env(tmp_path, monkeypatch):
    """No inherited credentials, no inherited endpoint, no inherited .env, and
    a CA file that exists — so each test states its own conditions."""
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))  # absent
    for var in ("VTOP_S3_ENDPOINT_URL", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3",
                "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
                "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "SSL_CERT_FILE"):
        monkeypatch.delenv(var, raising=False)
    ca = tmp_path / "ca.pem"
    ca.write_text("-- not a real CA, only a file that exists --\n", encoding="utf-8")
    monkeypatch.setattr(engine, "H3_PROXY_CA_FILE", str(ca))
    return ca


# --------------------------------------------------------------------------
# Which endpoint is the lab's
# --------------------------------------------------------------------------


def test_the_proxy_port_is_the_lab_only_for_a_scenario_that_declared_it():
    assert engine._is_lab_endpoint(PROXY_ENDPOINT, fronted=True), (
        "a declared proxy hop fronts the same MinIO, so the lab credentials "
        "must follow the store to port 9443"
    )
    assert not engine._is_lab_endpoint(PROXY_ENDPOINT), (
        "loopback 9443 is an entirely ordinary address for somebody else's TLS "
        "service; without the declaration it must never be handed the lab keys"
    )


def test_declaring_the_hop_does_not_widen_anything_else():
    # The declaration admits ONE extra port. If it admitted more, a scenario
    # could name any loopback service and inherit the lab's identity for it.
    assert not engine._is_lab_endpoint("https://localhost:4566", fronted=True)
    assert not engine._is_lab_endpoint("https://rgw.example.net:9443", fronted=True), (
        "the predicate is host AND port; a remote store on the proxy's port is "
        "not the proxy"
    )
    assert engine._is_lab_endpoint("http://localhost:9000", fronted=True), (
        "the direct store stays the lab store whatever else a scenario declares"
    )


def test_the_credentials_and_the_lab_ca_follow_a_declared_hop(clean_env):
    env = engine._backend_env(proxied())
    assert env["AWS_ACCESS_KEY_ID"] == "minioadmin", (
        "the store behind the proxy is the lab's MinIO; without the fallbacks "
        "every upload fails authentication and reads as a transport failure"
    )
    assert env["SSL_CERT_FILE"] == str(clean_env), (
        "the engine verifies certificates even with verify_tls false "
        "(vtop-upload/src/s3_native.rs), so the lab CA must reach its trust "
        "store or the handshake fails before a byte is uploaded"
    )


def test_an_undeclared_run_at_the_proxy_port_gets_neither(clean_env):
    env = engine._backend_env(
        scenario(backend="s3_native", endpoint_url=PROXY_ENDPOINT))
    assert "AWS_ACCESS_KEY_ID" not in env, (
        "without the declaration this is somebody else's loopback TLS service, "
        "and environment keys outrank every other link in the SDK's credential "
        "chain"
    )
    assert "SSL_CERT_FILE" not in env, (
        "a run the lab does not own must not be quietly told to trust a CA the "
        "lab minted; what a client trusts is the operator's decision"
    )


def test_an_operator_ca_bundle_is_not_overwritten(clean_env, monkeypatch):
    monkeypatch.setenv("SSL_CERT_FILE", "/etc/pki/tls/certs/ca-bundle.crt")
    env = engine._backend_env(proxied())
    assert env["SSL_CERT_FILE"] == "/etc/pki/tls/certs/ca-bundle.crt", (
        "a trust store the operator exported is operator topology; the harness "
        "never overrides it — the handshake then fails loudly rather than the "
        "harness silently redefining what the run trusts"
    )


def test_the_container_wire_endpoint_names_the_proxy_service_not_the_store():
    assert engine.container_wire_endpoint(PROXY_ENDPOINT, fronted=True) == \
        "https://h3-proxy:9443", (
        "inside the compose network the proxy is reached by service name; "
        "translating it to the store would send the engine around the very hop "
        "the run is measuring"
    )
    assert engine.container_wire_endpoint(PROXY_ENDPOINT) == PROXY_ENDPOINT, (
        "an undeclared endpoint is the operator's topology and is never rewritten"
    )


# --------------------------------------------------------------------------
# Whether the run really took the hop
# --------------------------------------------------------------------------


def test_a_declared_hop_pointed_at_the_proxy_is_accepted(clean_env):
    engine.require_endpoint_through_h3_proxy(proxied())


def test_a_scenario_that_declares_nothing_is_not_judged(clean_env):
    # Every other scenario in the suite must be unaffected by this gate.
    engine.require_endpoint_through_h3_proxy(
        scenario(backend="s3_native", endpoint_url="http://localhost:9000"))


def test_an_endpoint_override_that_bypasses_the_proxy_is_refused(clean_env, monkeypatch):
    monkeypatch.setenv("VTOP_S3_ENDPOINT_URL", "http://localhost:9000")
    with pytest.raises(ValueError, match="around the proxy"):
        engine.require_endpoint_through_h3_proxy(proxied())


def test_a_plaintext_endpoint_under_the_declaration_is_refused(clean_env):
    with pytest.raises(ValueError, match="TLS-only"):
        engine.require_endpoint_through_h3_proxy(
            proxied(endpoint_url="http://localhost:9443"))


def test_declaring_the_hop_while_naming_the_store_is_refused(clean_env):
    # The failure this exists for: a copy of scenario 12 that gained
    # `h3_proxy: true` and kept its endpoint would be filed as the
    # topology-controlled baseline while measuring the direct path.
    with pytest.raises(ValueError, match="loopback"):
        engine.require_endpoint_through_h3_proxy(
            proxied(endpoint_url="http://localhost:9000"))


def test_a_backend_that_never_dials_the_endpoint_is_refused(clean_env):
    with pytest.raises(ValueError, match="never took"):
        engine.require_endpoint_through_h3_proxy(proxied(backend="mock"))


def test_container_mode_is_refused_rather_than_half_supported(clean_env):
    # The hardened engine container mounts the binary and the run root, and
    # nothing else — so the lab CA is not in it, and the run would die at the
    # handshake with a trust error that reads like a broken proxy.
    with pytest.raises(ValueError, match="not mounted into the engine container"):
        engine.require_endpoint_through_h3_proxy(proxied(runner_mode="container"))


def test_a_missing_lab_ca_names_the_generator(clean_env, tmp_path, monkeypatch):
    monkeypatch.setattr(engine, "H3_PROXY_CA_FILE", str(tmp_path / "absent.pem"))
    with pytest.raises(ValueError, match="gen-h3-certs.sh"):
        engine.require_endpoint_through_h3_proxy(proxied())


# --------------------------------------------------------------------------
# The scenario that depends on all of the above
# --------------------------------------------------------------------------


def test_the_bundled_baseline_scenario_really_takes_the_hop(clean_env):
    sc = load_scenario(SCENARIO_14)
    assert sc.get("h3_proxy") is True, (
        "scenario 17 is the topology control; without the declaration it is "
        "just scenario 12 pointed at a port the harness does not recognise"
    )
    assert sc.get("transport") == "tcp_tls", (
        "the control measures the EXISTING wire through the new topology; a "
        "different transport here would confound the two changes it exists to "
        "separate"
    )
    engine.require_endpoint_through_h3_proxy(sc)


def test_the_proxy_hop_is_a_recorded_column_everywhere():
    # The same rule runner_mode and transport are held to: a throughput number
    # or a p95 read without knowing whether a proxy sat in front of the store
    # is a number compared against the wrong baseline — and here that is not
    # hypothetical, since scenario 17 exists to be a DIFFERENT baseline from
    # scenario 12.
    from lib.metrics import CSV_HEADERS
    assert "h3_proxy" in CSV_HEADERS["metrics.csv"], (
        "metrics.csv must record whether the run went through the proxy hop"
    )
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
    from benchmarks.run_matrix import COMPARE_COLS
    assert "h3_proxy" in COMPARE_COLS, (
        "the matrix is where scenario 17 and scenario 12 sit side by side; "
        "without this column the two rows are indistinguishable"
    )


def test_the_bundled_baseline_differs_from_scenario_12_only_in_topology_keys():
    # The pair is the measurement. If any other knob drifts apart, the
    # difference between the two runs stops being the change of path. (What
    # the path change itself carries — TLS, and whatever ALPN the SDK then
    # negotiates — is inherent and is named in scenario 17's description; it
    # is not a knob and cannot be pinned here.)
    direct = load_scenario(os.path.join(
        os.path.dirname(SCENARIO_14), "12-backpressure-soak-minio.yaml"))
    proxied_sc = load_scenario(SCENARIO_14)
    topology_keys = {"name", "description", "endpoint_url", "h3_proxy", "transport"}
    differing = {k for k in set(direct.values) | set(proxied_sc.values)
                 if direct.get(k) != proxied_sc.get(k)}
    assert differing <= topology_keys, (
        f"scenario 17 and scenario 12 differ in {sorted(differing - topology_keys)} "
        "as well as the topology: the proxy hop's cost is measured by the "
        "difference between these two runs, so every other knob must match"
    )


def test_the_sdk_own_endpoint_channels_cannot_take_the_engine_around_the_proxy():
    # `_effective_endpoint` knows about VTOP_S3_ENDPOINT_URL and the scenario
    # value. The AWS SDK additionally consumes AWS_ENDPOINT_URL_S3 and
    # AWS_ENDPOINT_URL when it builds the client, and vtop-upload validates
    # those for SCHEME without knowing anything about this proxy — so a host
    # with either set could reach the store directly while every row still
    # recorded h3_proxy (review). A topology claim is only worth making if
    # every channel that could break it is checked.
    import os

    from lib.engine import require_endpoint_through_h3_proxy

    scenario = {"backend": "s3_native", "h3_proxy": True, "runner_mode": "host",
                "endpoint_url": "https://localhost:9443"}

    for var in ("AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"):
        previous = os.environ.pop(var, None)
        try:
            os.environ[var] = "http://localhost:9000"
            with pytest.raises(ValueError) as exc:
                require_endpoint_through_h3_proxy(scenario)
            assert var in str(exc.value), (
                f"the refusal must name {var}, or the operator cannot tell which of the "
                "several endpoint channels is sending the engine around the proxy"
            )
        finally:
            os.environ.pop(var, None)
            if previous is not None:
                os.environ[var] = previous

    # Even a value that happens to AGREE with the proxy is refused: a second
    # source of truth for the one thing this scenario exists to pin is a
    # second thing that can drift.
    previous = os.environ.pop("AWS_ENDPOINT_URL_S3", None)
    try:
        os.environ["AWS_ENDPOINT_URL_S3"] = "https://localhost:9443"
        with pytest.raises(ValueError):
            require_endpoint_through_h3_proxy(scenario)
    finally:
        os.environ.pop("AWS_ENDPOINT_URL_S3", None)
        if previous is not None:
            os.environ["AWS_ENDPOINT_URL_S3"] = previous


def test_the_host_port_override_reaches_the_measurement_not_only_the_check(clean_env):
    # VTOP_H3_PORT moves the publish because 9443 is an unremarkable choice for
    # somebody else's TLS service. The override used to move the proxy and
    # verify-h3.sh with it, while the scenario kept dialling 9443 and the
    # validation rejected anything else — so the escape hatch reported a
    # healthy proxy and made the proxy BASELINE unusable (review). An override
    # that works for the check and not for the measurement is worse than none.
    import os

    from lib.engine import (
        H3_PROXY_CONTAINER_PORT,
        container_wire_endpoint,
        h3_scenario_endpoint,
        require_endpoint_through_h3_proxy,
    )

    scenario = {"backend": "s3_native", "h3_proxy": True, "runner_mode": "host",
                "endpoint_url": "https://localhost:9443"}
    previous = os.environ.pop("VTOP_H3_PORT", None)
    try:
        # Without the override the scenario's own value stands.
        assert h3_scenario_endpoint(scenario) == "https://localhost:9443"
        require_endpoint_through_h3_proxy(scenario)

        os.environ["VTOP_H3_PORT"] = "9543"
        assert h3_scenario_endpoint(scenario) == "https://localhost:9543", (
            "the run must dial where the proxy is actually published, or it reaches "
            "nothing while the verification script reports a healthy proxy"
        )
        require_endpoint_through_h3_proxy(scenario)

        # The CONTAINER port never moves: Alt-Svc advertises the port nginx
        # listens on, and a client told a different number reaches nothing.
        assert container_wire_endpoint("https://localhost:9543", fronted=True) == (
            f"https://h3-proxy:{H3_PROXY_CONTAINER_PORT}"
        ), "only the host side of the publish moves; inside the lab the port is fixed"

        os.environ["VTOP_H3_PORT"] = "not-a-port"
        with pytest.raises(ValueError, match="VTOP_H3_PORT"):
            h3_scenario_endpoint(scenario)
    finally:
        os.environ.pop("VTOP_H3_PORT", None)
        if previous is not None:
            os.environ["VTOP_H3_PORT"] = previous


def test_an_ipv6_proxy_endpoint_survives_the_port_substitution(clean_env):
    # urlsplit strips the brackets from `[::1]`, so reassembling without them
    # yields `https://::1:9443` — not a URL, and the topology validation then
    # fails reading its port (review). The lab certificate carries a `::1`
    # SAN, so the form is offered rather than hypothetical.
    from lib.engine import h3_scenario_endpoint

    scenario = {"backend": "s3_native", "h3_proxy": True, "runner_mode": "host",
                "endpoint_url": "https://[::1]:9443"}
    assert h3_scenario_endpoint(scenario) == "https://[::1]:9443", (
        "an IPv6 host must stay bracketed through the substitution, or the endpoint the "
        "run dials is not parseable and the proxy is unreachable by the address its own "
        "certificate advertises"
    )

    # A name is untouched by the bracketing.
    named = dict(scenario, endpoint_url="https://localhost:9443")
    assert h3_scenario_endpoint(named) == "https://localhost:9443"
