"""The authority the HTTP/3 probe sends has to be one (#484).

`h3_probe.py` takes a URL, and `urlsplit(...).hostname` returns an IPv6 literal
WITHOUT the brackets the URL spelled it with. Building `:authority` as
`f"{host}:{port}"` from that turned `https://[::1]:9443/` into `::1:9443` — not
an authority but an address, whose last group reads as the port (review). The
proxy forwards that authority as the Host the store verifies a SigV4 signature
over, so a malformed one does not fail as a syntax error; it fails as a
credential mismatch, or not at all until something parses it.

The probe imports aioquic at module level, and the benchmark CI job installs
pytest and PyYAML and nothing else — aioquic lives inside the pinned image
verify-h3.sh runs. So the aioquic names the module binds at import are stubbed
here: nothing under test touches them, and a test that skipped whenever aioquic
was absent would never run anywhere it is collected.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types

import pytest

BENCH_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROBE = os.path.join(BENCH_DIR, "h3_probe.py")


@pytest.fixture
def probe_module(monkeypatch):
    class _Stub:
        def __init__(self, *args, **kwargs) -> None:
            pass

    names = {
        "aioquic": {},
        "aioquic.asyncio": {},
        "aioquic.asyncio.client": {"connect": None},
        "aioquic.asyncio.protocol": {"QuicConnectionProtocol": _Stub},
        "aioquic.h3": {},
        "aioquic.h3.connection": {"H3_ALPN": ["h3"], "H3Connection": _Stub},
        "aioquic.h3.events": {"DataReceived": _Stub, "H3Event": _Stub,
                              "HeadersReceived": _Stub},
        "aioquic.quic": {},
        "aioquic.quic.configuration": {"QuicConfiguration": _Stub},
        "aioquic.quic.events": {"HandshakeCompleted": _Stub, "QuicEvent": _Stub},
    }
    for name, attrs in names.items():
        module = types.ModuleType(name)
        for attr, value in attrs.items():
            setattr(module, attr, value)
        monkeypatch.setitem(sys.modules, name, module)

    spec = importlib.util.spec_from_file_location("h3_probe_under_test", PROBE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_an_ipv6_literal_keeps_its_brackets_in_the_authority(probe_module):
    # REGRESSION: `--url https://[::1]:9443/` sent `:authority: ::1:9443`.
    from urllib.parse import urlsplit

    parts = urlsplit("https://[::1]:9443/health")
    authority = probe_module._authority(parts.hostname, parts.port)
    assert authority == "[::1]:9443", (
        f"the probe named {authority!r} as the authority of https://[::1]:9443/. Without "
        "brackets the port is indistinguishable from the address's last group, and the "
        "proxy forwards that string as the Host the store checks a signature over"
    )


def test_a_name_and_an_ipv4_literal_are_not_bracketed(probe_module):
    assert probe_module._authority("localhost", 9443) == "localhost:9443", (
        "brackets are the IPv6 literal's syntax only; `[localhost]:9443` is not a "
        "valid authority either"
    )
    assert probe_module._authority("127.0.0.1", 9443) == "127.0.0.1:9443", (
        "an IPv4 literal has no colon to disambiguate and must not be bracketed"
    )
