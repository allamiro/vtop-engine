"""The S3 tap (#744 rule 17): a pass-through HTTP proxy in front of the store, for crash and fault cells only.

The tap listens under the store's DNS name and forwards every request,
unchanged, to the real store running under another name, so SigV4's signed
Host header still matches. It counts the store requests that are PUTs (an
object, a part or a manifest) since the tool's last (re)start and fires the
seeded kill points of `kills.py`:

- K1 at the START of the Nth PUT: the request is not forwarded, the kill
  callback runs, and the connection is dropped;
- K2 right AFTER the store's answer to the Nth PUT is written to the tool's
  socket: no request on any connection is forwarded from then until the kill
  callback has returned (the gate), so the tool cannot reach the store again
  before it dies. What it does locally in the gap between our write and the
  kill (milliseconds) is the race every tool runs equally; the gap is
  recorded per kill (`kill_delay_ms`).

In the `verify-fault` cell it flips one bit in the body of a GET answer for one
object in `one_in` (chosen by a seeded hash of the key, so every tool's
corrupted set is decided by the same rule), and records which keys it
corrupted.

Every request is one line of a trace (CSV): times, method, path, query,
status, bytes each way, its PUT number, and whether it was flipped or killed.
Performance cells never run through the tap (#744 rule 17): `refuse_in_cell`
is the check every runner calls.

    python3 -m compare_common.tap --listen 0.0.0.0:9000 --upstream minio-real:9000 \\
        --control 0.0.0.0:9300 --trace /logs/tap.csv [--fault-seed 7 --fault-one-in 10]

The control port takes `POST /arm` ({"kind": "K1"|"K2", "n": N, "container": NAME}),
`POST /reset` (a restarted tool counts PUTs from zero) and `GET /state`.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import sys
import time
import urllib.parse
from collections.abc import Awaitable, Callable
from typing import Any

TAP_CELLS = ("crash-K1", "crash-K2", "crash-K3", "crash-K4-rotation", "verify-fault", "down-while-rotating")
TRACE_COLUMNS = ("t_start", "t_end", "conn", "method", "path", "query", "status", "req_bytes", "resp_bytes",
                 "put_n", "flipped", "killed")
MAX_HEAD = 64 * 1024


class TapRefused(Exception):
    pass


def refuse_in_cell(cell: str) -> None:
    """The tap belongs to crash and fault cells; a drain or live cell through it is refused (#744 rule 17)."""
    if cell not in TAP_CELLS:
        raise TapRefused(f"the S3 tap is used only in crash and fault cells, not in {cell!r}: performance cells "
                         "go straight to the store and count requests from the store's own metrics")


# The query parameters an object read can carry: a version, the SDK's operation tag, a part, and response
# overrides. Any other one names a sub-resource (a listing, the parts of an upload, tags, an ACL), not the body.
_OBJECT_READ_PARAMS = ("versionid", "x-id", "partnumber")


def is_object_get(path: str, query: str) -> bool:
    """A GET of an object's bytes: a key under the bucket, and no sub-resource in the query (review of #906)."""
    if "/" not in path.strip("/"):
        return False  # the bucket itself: a listing or a bucket sub-resource
    for name, _value in urllib.parse.parse_qsl(query, keep_blank_values=True):
        name = name.lower()
        if name not in _OBJECT_READ_PARAMS and not name.startswith("response-"):
            if name.startswith("x-amz-"):
                continue  # signature and SDK bookkeeping parameters
            return False
    return True


def fault_selected(seed: int, key: str, one_in: int) -> bool:
    digest = hashlib.sha256(f"{seed}:{key}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % one_in == 0


def flip_bit(body: bytes, seed: int, key: str) -> bytes:
    if not body:
        return body
    digest = hashlib.sha256(f"{seed}:{key}:bit".encode()).digest()
    position = int.from_bytes(digest[:8], "big") % (len(body) * 8)
    data = bytearray(body)
    data[position // 8] ^= 1 << (position % 8)
    return bytes(data)


def parse_head(head: bytes) -> tuple[str, dict[str, str]]:
    lines = head.decode("latin-1").split("\r\n")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line:
            continue
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()
    return lines[0], headers


async def read_head(reader: asyncio.StreamReader) -> bytes | None:
    try:
        head = await reader.readuntil(b"\r\n\r\n")
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None
        raise
    except asyncio.LimitOverrunError as exc:
        raise TapRefused("an HTTP head over the limit") from exc
    if len(head) > MAX_HEAD:
        raise TapRefused("an HTTP head over the limit")
    return head


async def pipe_body(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, headers: dict[str, str],
                    *, until_close: bool = False, mutate: Callable[[bytes], bytes] | None = None) -> int:
    """Copy one message body framed by `headers`; returns the body bytes copied."""
    if "chunked" in headers.get("transfer-encoding", "").lower():
        total = 0
        collected = bytearray() if mutate else None
        while True:
            size_line = await reader.readuntil(b"\r\n")
            size = int(size_line.split(b";")[0].strip(), 16)
            if size == 0:
                trailer = b""
                while True:
                    line = await reader.readuntil(b"\r\n")
                    trailer += line
                    if line == b"\r\n":
                        break
                if collected is not None:
                    body = mutate(bytes(collected))
                    writer.write(f"{len(body):x}\r\n".encode() + body + b"\r\n")
                writer.write(b"0\r\n" + trailer)
                return total
            chunk = await reader.readexactly(size + 2)
            total += size
            if collected is not None:
                collected.extend(chunk[:-2])
            else:
                writer.write(size_line + chunk)
                await writer.drain()
    if "content-length" in headers:
        remaining = int(headers["content-length"])
        if mutate:
            body = mutate(await reader.readexactly(remaining))
            writer.write(body)
            return len(body)
        total = remaining
        while remaining:
            chunk = await reader.read(min(remaining, 1 << 16))
            if not chunk:
                raise asyncio.IncompleteReadError(b"", remaining)
            writer.write(chunk)
            await writer.drain()
            remaining -= len(chunk)
        return total
    if until_close:
        body = await reader.read()
        writer.write(mutate(body) if mutate else body)
        return len(body)
    return 0


class Tap:
    def __init__(self, upstream: tuple[str, int], *, trace_path: str | None = None,
                 kill: Callable[[str], Awaitable[None]] | None = None,
                 fault_seed: int | None = None, fault_one_in: int = 10,
                 clock: Callable[[], float] = time.time):
        self.upstream = upstream
        self.kill = kill
        self.fault_seed = fault_seed
        self.fault_one_in = fault_one_in
        self.clock = clock
        self.put_count = 0
        self.armed: dict[str, Any] | None = None
        self.fired: list[dict[str, Any]] = []
        self.corrupted: list[str] = []
        self.gets: list[str] = []
        self._gate: asyncio.Event | None = None
        self._conn = 0
        self._trace = None
        self._trace_writer = None
        if trace_path:
            new = not os.path.exists(trace_path) or os.path.getsize(trace_path) == 0
            self._trace = open(trace_path, "a", newline="", encoding="utf-8")  # noqa: SIM115
            self._trace_writer = csv.writer(self._trace)
            if new:
                self._trace_writer.writerow(TRACE_COLUMNS)

    @property
    def gate(self) -> asyncio.Event:
        """Open unless a K2 kill is pending. Made on first use, inside the running loop (Python 3.9 binds an
        Event to the loop current at its creation)."""
        if self._gate is None:
            self._gate = asyncio.Event()
            self._gate.set()
        return self._gate

    # ------------------------------------------------------------ control

    def arm(self, kind: str, n: int, container: str) -> None:
        if kind not in ("K1", "K2"):
            raise TapRefused(f"the tap fires K1 and K2 only, not {kind!r}")
        if n < 1:
            raise TapRefused("n counts PUTs from 1")
        self.armed = {"kind": kind, "n": int(n), "container": container}

    def reset(self) -> None:
        """The tool (re)started: count its PUTs from zero."""
        self.put_count = 0

    def state(self) -> dict[str, Any]:
        return {"put_count": self.put_count, "armed": self.armed, "fired": self.fired,
                "corrupted": self.corrupted, "gets": len(self.gets)}

    async def _fire(self, kind: str, n: int, delivered_at: float) -> None:
        armed = self.armed or {}
        self.armed = None
        started = self.clock()
        if self.kill:
            await self.kill(armed.get("container", ""))
        done = self.clock()
        self.fired.append({"kind": kind, "n": n, "container": armed.get("container", ""), "trigger_at": delivered_at,
                           "killed_at": done, "kill_delay_ms": round((done - delivered_at) * 1000, 1),
                           "kill_call_ms": round((done - started) * 1000, 1)})

    def _trace_row(self, values: dict[str, Any]) -> None:
        if self._trace_writer:
            self._trace_writer.writerow([values.get(column, "") for column in TRACE_COLUMNS])
            self._trace.flush()

    def close(self) -> None:
        if self._trace:
            self._trace.close()

    # ------------------------------------------------------------ proxy

    async def handle(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
        self._conn += 1
        conn = self._conn
        upstream_reader, upstream_writer = await asyncio.open_connection(*self.upstream, limit=MAX_HEAD)
        try:
            while True:
                if not await self._one(conn, client_reader, client_writer, upstream_reader, upstream_writer):
                    break
        except (asyncio.IncompleteReadError, ConnectionError, TapRefused):
            pass
        finally:
            for writer in (client_writer, upstream_writer):
                writer.close()

    async def _one(self, conn: int, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter,
                   upstream_reader: asyncio.StreamReader, upstream_writer: asyncio.StreamWriter) -> bool:
        head = await read_head(client_reader)
        if head is None:
            return False
        started = self.clock()
        request_line, headers = parse_head(head)
        method, target = request_line.split(" ")[:2]
        url = urllib.parse.urlsplit(target)
        await self.gate.wait()
        row = {"t_start": f"{started:.3f}", "conn": conn, "method": method, "path": url.path, "query": url.query}
        put_n = None
        if method == "PUT":
            self.put_count += 1
            put_n = self.put_count
            row["put_n"] = put_n
            armed = self.armed
            if armed and armed["kind"] == "K1" and armed["n"] == put_n:
                row.update({"killed": "K1", "t_end": f"{self.clock():.3f}"})
                self._trace_row(row)
                await self._fire("K1", put_n, started)
                return False
        upstream_writer.write(head)
        await upstream_writer.drain()
        request_bytes = 0
        response_head = None
        if headers.get("expect", "").lower() == "100-continue":
            interim = await read_head(upstream_reader)
            if interim is None:
                return False
            if interim.split(b" ", 2)[1] == b"100":
                client_writer.write(interim)
                await client_writer.drain()
            else:
                response_head = interim  # the store refused before the body: relay and stop this connection
        if response_head is None:
            request_bytes = await pipe_body(client_reader, upstream_writer, headers)
            await upstream_writer.drain()
            response_head = await read_head(upstream_reader)
            while response_head is not None and response_head.split(b" ", 2)[1].startswith(b"1"):
                response_head = await read_head(upstream_reader)
            if response_head is None:
                return False
        status_line, response_headers = parse_head(response_head)
        status = int(status_line.split(" ")[1])
        client_writer.write(response_head)
        mutate = None
        key = urllib.parse.unquote(url.path)
        if method == "GET" and status < 300 and is_object_get(url.path, url.query):
            self.gets.append(key)
            if self.fault_seed is not None and fault_selected(self.fault_seed, key, self.fault_one_in):
                seed = self.fault_seed

                def mutate(body: bytes, key: str = key) -> bytes:
                    return flip_bit(body, seed, key)

                self.corrupted.append(key)
                row["flipped"] = "1"
        no_body = method == "HEAD" or status in (204, 304) or 100 <= status < 200
        response_bytes = 0
        if not no_body:
            close_framed = "content-length" not in response_headers and \
                "chunked" not in response_headers.get("transfer-encoding", "").lower()
            response_bytes = await pipe_body(upstream_reader, client_writer, response_headers,
                                             until_close=close_framed, mutate=mutate)
        await client_writer.drain()
        delivered = self.clock()
        row.update({"status": status, "req_bytes": request_bytes, "resp_bytes": response_bytes,
                    "t_end": f"{delivered:.3f}"})
        armed = self.armed
        if put_n is not None and armed and armed["kind"] == "K2" and armed["n"] == put_n and status < 300:
            self.gate.clear()
            try:
                row["killed"] = "K2"
                self._trace_row(row)
                await self._fire("K2", put_n, delivered)
            finally:
                self.gate.set()
            return False
        self._trace_row(row)
        return response_headers.get("connection", "").lower() != "close"


# ------------------------------------------------------------------ server


async def _control(tap: Tap, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        head = await read_head(reader)
        if head is None:
            return
        request_line, headers = parse_head(head)
        method, path = request_line.split(" ")[:2]
        body = await reader.readexactly(int(headers.get("content-length", "0") or 0))
        status, answer = 200, {}
        try:
            if method == "POST" and path == "/arm":
                spec = json.loads(body or b"{}")
                tap.arm(spec["kind"], int(spec["n"]), spec.get("container", ""))
            elif method == "POST" and path == "/reset":
                tap.reset()
            elif method != "GET" or path != "/state":
                status, answer = 404, {"error": f"no {method} {path}"}
        except (TapRefused, KeyError, ValueError) as exc:
            status, answer = 400, {"error": str(exc)}
        if status == 200:
            answer = tap.state()
        data = json.dumps(answer).encode()
        writer.write(f"HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {len(data)}\r\n"
                     "Connection: close\r\n\r\n".encode() + data)
        await writer.drain()
    finally:
        writer.close()


def _hostport(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    return host or "0.0.0.0", int(port)


async def serve(args: argparse.Namespace) -> None:
    from compare_common import docker_api

    async def kill(container: str) -> None:
        if container:
            await asyncio.get_running_loop().run_in_executor(None, docker_api.kill, container)

    tap = Tap(_hostport(args.upstream), trace_path=args.trace, kill=kill, fault_seed=args.fault_seed,
              fault_one_in=args.fault_one_in)
    proxy = await asyncio.start_server(tap.handle, *_hostport(args.listen), limit=MAX_HEAD)
    control = await asyncio.start_server(lambda r, w: _control(tap, r, w), *_hostport(args.control))
    print(f"tap: {args.listen} -> {args.upstream}, control {args.control}", flush=True)
    async with proxy, control:
        await asyncio.gather(proxy.serve_forever(), control.serve_forever())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--listen", default="0.0.0.0:9000")
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--control", default="0.0.0.0:9300")
    parser.add_argument("--trace", default=None)
    parser.add_argument("--fault-seed", type=int, default=None)
    parser.add_argument("--fault-one-in", type=int, default=10)
    args = parser.parse_args(argv)
    try:
        asyncio.run(serve(args))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
