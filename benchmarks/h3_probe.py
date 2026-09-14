"""A minimal HTTP/3 client, used to prove the lab proxy really speaks it (#484).

WHY THIS EXISTS. The lab's whole datagram story rests on one fact: something
answers over HTTP/3. A proxy that quietly served HTTP/1.1 or HTTP/2 instead
would satisfy every scenario in the suite and make every later measurement a
measurement of TCP under another name. So the check has to be able to FAIL, and
the failure has to be specific.

WHY NOT CURL. The host's curl (7.76.1) has no HTTP/3, and neither do the
official curl container images; the builds that do are single-maintainer images
that #507 taught this repository not to depend on. aioquic is a maintained QUIC
implementation with published wheels, and running it inside the pinned official
Python image keeps the supply chain to "one official base image plus one pinned
package" — see verify-h3.sh, which is what invokes this.

WHAT IT ASSERTS. Only `h3` is offered as an ALPN protocol, so the server either
completes a QUIC handshake agreeing to HTTP/3 or the handshake fails outright;
there is no version this can fall back to. The negotiated protocol is then read
off the handshake event and compared to what was expected, because "the client
believes it spoke h3" is exactly the assumption worth checking. verify-h3.sh
pairs this with the server's own access log, which is the witness the client
cannot influence.

Standalone by design: it imports aioquic and the standard library, nothing from
the harness, and is never imported by it.
"""
from __future__ import annotations

import argparse
import asyncio
import socket
import sys
from urllib.parse import urlsplit

from aioquic.asyncio.client import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DataReceived, H3Event, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import HandshakeCompleted, QuicEvent


class H3Probe(QuicConnectionProtocol):
    """One request, one response, and the ALPN the handshake actually agreed."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._http = H3Connection(self._quic)
        self.alpn: str | None = None
        self._events: dict[int, list[H3Event]] = {}
        self._waiters: dict[int, asyncio.Future] = {}

    async def get(self, authority: str, path: str) -> list[H3Event]:
        stream_id = self._quic.get_next_available_stream_id()
        self._http.send_headers(
            stream_id,
            [
                (b":method", b"GET"),
                (b":scheme", b"https"),
                (b":authority", authority.encode()),
                (b":path", path.encode()),
                (b"user-agent", b"vtop-bench-h3-probe"),
            ],
            end_stream=True,
        )
        waiter: asyncio.Future = asyncio.get_running_loop().create_future()
        self._events[stream_id] = []
        self._waiters[stream_id] = waiter
        self.transmit()
        return await asyncio.shield(waiter)

    def quic_event_received(self, event: QuicEvent) -> None:
        # The handshake event carries the negotiated ALPN. This is the single
        # value the whole check turns on, so it is read from the connection
        # rather than inferred from the fact that a response arrived.
        if isinstance(event, HandshakeCompleted):
            self.alpn = event.alpn_protocol
        for http_event in self._http.handle_event(event):
            self._http_event_received(http_event)

    def _http_event_received(self, event: H3Event) -> None:
        if not isinstance(event, (HeadersReceived, DataReceived)):
            return
        pending = self._events.get(event.stream_id)
        if pending is None:
            return
        pending.append(event)
        if event.stream_ended:
            waiter = self._waiters.pop(event.stream_id, None)
            if waiter is not None and not waiter.done():
                waiter.set_result(self._events.pop(event.stream_id))


def _status_of(events: list[H3Event]) -> str:
    for event in events:
        if isinstance(event, HeadersReceived):
            for name, value in event.headers:
                if name == b":status":
                    return value.decode()
    return "none"


def _body_bytes(events: list[H3Event]) -> int:
    return sum(len(e.data) for e in events if isinstance(e, DataReceived))


def _addresses(host: str, port: int) -> list[str]:
    """Every address `host` resolves to, IPv4 first and without duplicates."""
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_DGRAM)
    ordered: list[str] = []
    for _family, _, _, _, sockaddr in sorted(
            infos, key=lambda i: 0 if i[0] == socket.AF_INET else 1):
        address = str(sockaddr[0])
        if address not in ordered:
            ordered.append(address)
    return ordered or [host]


async def probe(url: str, ca_file: str | None, expect_alpn: str,
                expect_status: str, timeout: float) -> int:
    parts = urlsplit(url)
    if parts.scheme != "https":
        print(f"h3_probe FAIL: {url} is not https:// — HTTP/3 has no plaintext form",
              file=sys.stderr)
        return 2
    host = parts.hostname or "localhost"
    port = parts.port or 443
    authority = f"{host}:{port}"
    path = parts.path or "/"
    if parts.query:
        path = f"{path}?{parts.query}"

    configuration = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    # SNI and certificate verification use the NAME from the URL even when the
    # datagrams go to a literal address below, so the lab's certificate is
    # checked against the name a client would actually use.
    configuration.server_name = host
    if ca_file:
        configuration.load_verify_locations(cafile=ca_file)

    try:
        candidates = _addresses(host, port)
    except OSError as exc:
        print(f"h3_probe FAIL: cannot resolve {host!r}: {exc}", file=sys.stderr)
        return 2
    # Every resolved address is TRIED, IPv4 first, and the one that answered is
    # reported. The reason is a real failure: `localhost` resolves to ::1 before
    # 127.0.0.1 on many hosts, Docker's default port publish is IPv4-only, and
    # aioquic dials only the first address it is given — so the probe took an
    # ICMP refusal from ::1 and reported "nothing speaks HTTP/3 here" about a
    # proxy that was serving it perfectly on the other family.
    budget = max(3.0, timeout / len(candidates))
    attempts: list[str] = []
    negotiated: str | None = None
    events: list[H3Event] = []
    reached = ""

    for address in candidates:
        async def exchange(address: str = address) -> tuple[str | None, list[H3Event]]:
            async with connect(address, port, configuration=configuration,
                               create_protocol=H3Probe, wait_connected=True) as client:
                return client.alpn, await client.get(authority, path)

        try:
            # The HANDSHAKE is inside the deadline too, not only the request: a
            # UDP port with nothing behind it swallows the initial packets, and
            # an unbounded wait there would hang the check instead of failing
            # it — which is exactly the case this program has to report.
            negotiated, events = await asyncio.wait_for(exchange(), budget)
        except asyncio.TimeoutError:
            attempts.append(f"{address}: no QUIC handshake within {budget:.0f}s")
            continue
        except Exception as exc:
            # Everything from "connection refused" to "the certificate is not
            # trusted" lands here, and the text of the exception is the most
            # useful thing this program can say.
            attempts.append(f"{address}: {type(exc).__name__}: {exc}")
            continue
        reached = address
        break

    if not reached:
        print(f"h3_probe FAIL: no HTTP/3 exchange with {url}; tried "
              + "; ".join(attempts), file=sys.stderr)
        return 3

    alpn = negotiated or "none"
    status = _status_of(events)
    summary = (f"url={url} address={reached} alpn={alpn} status={status} "
               f"body_bytes={_body_bytes(events)}")
    if alpn != expect_alpn:
        print(f"h3_probe FAIL: negotiated ALPN {alpn!r}, expected {expect_alpn!r} — "
              f"{summary}", file=sys.stderr)
        return 5
    if expect_status and status != expect_status:
        print(f"h3_probe FAIL: HTTP status {status}, expected {expect_status} — "
              f"the QUIC connection is up but the origin behind the proxy is not "
              f"answering: {summary}", file=sys.stderr)
        return 6
    print(f"h3_probe OK {summary}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--url", required=True,
                    help="https:// URL to fetch over HTTP/3")
    ap.add_argument("--ca", default=None,
                    help="PEM CA bundle to verify the server against")
    ap.add_argument("--expect-alpn", default="h3",
                    help="the ALPN the handshake must agree on (default: h3)")
    ap.add_argument("--expect-status", default="200",
                    help="the HTTP status the origin must answer with; "
                         "empty string to accept any")
    ap.add_argument("--timeout", type=float, default=15.0,
                    help="seconds to wait for the response (default: 15)")
    args = ap.parse_args()
    return asyncio.run(probe(args.url, args.ca, args.expect_alpn,
                             args.expect_status, args.timeout))


if __name__ == "__main__":
    sys.exit(main())
