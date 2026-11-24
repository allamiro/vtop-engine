#!/usr/bin/env python3
"""A fault-injecting HTTP proxy for the store, for the failed-verification drill (#735).

  fault-proxy.py <listen-port> <upstream-host:port> <control-file> <stats-file>

Every request is passed to the upstream unchanged, with its Host header, so the
engine's signature still holds. While the control file exists, the proxy makes
the store wrong about data objects (a key that is not a manifest or a receipt,
with no `versionId` query), the way the control file's first word says:

  get   a successful GET comes back with its first byte changed: the store
        "returns something other than what was written"
  head  a successful HEAD reports another `x-amz-checksum-sha256` than the one
        the engine sent: the store's own checksum disagrees with the engine's

The proxy appends one line to the stats file for each response it changed, so
the drill can show the fault was met.

It reads a request and a response whole. That is enough for the drill's
objects, which are a few kilobytes; it is not a general proxy.
"""
import http.client
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def is_data_object(path):
    # An object key, not a bucket or a listing, and not a manifest or receipt.
    target = path.split("?", 1)[0]
    if "?" in path and "versionId" in path.split("?", 1)[1]:
        return False
    low = target.lower()
    return target.count("/") >= 2 and "manifest" not in low and "receipt" not in low


def fault_mode(control):
    """The first word of the control file, or None while it does not exist."""
    try:
        with open(control) as f:
            return (f.read().split() or ["get"])[0]
    except OSError:
        return None


def header_text(value):
    """A header name or value with no line break, so nothing the store sends can start a header or a body of its own."""
    return str(value).replace("\r", "").replace("\n", "")


def alter_base64(value):
    """The same length, and another value."""
    return ("B" if value[:1] == "A" else "A") + value[1:]


def make_handler(upstream, control, stats):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def read_body(self):
            if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                out = b""
                while True:
                    size = int(self.rfile.readline().split(b";")[0].strip() or b"0", 16)
                    if size == 0:
                        while self.rfile.readline().strip():
                            pass
                        return out
                    out += self.rfile.read(size)
                    self.rfile.readline()
            n = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(n) if n else b""

        def handle_any(self):
            body = self.read_body()
            conn = http.client.HTTPConnection(upstream[0], upstream[1], timeout=300)
            conn.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
            for k, v in self.headers.items():
                if k.lower() in HOP_BY_HOP or k.lower() == "content-length":
                    continue
                conn.putheader(k, v)
            if body or self.command in ("PUT", "POST"):
                conn.putheader("Content-Length", str(len(body)))
            conn.endheaders(body)
            resp = conn.getresponse()
            data = resp.read() if self.command != "HEAD" else b""
            mode = fault_mode(control)
            changed = False
            if resp.status == 200 and mode and is_data_object(self.path):
                if mode == "get" and self.command == "GET" and data:
                    data = bytes([data[0] ^ 0x01]) + data[1:]
                    changed = True
                elif mode == "head" and self.command == "HEAD":
                    changed = True
                if changed:
                    with open(stats, "a") as f:
                        f.write(self.command + " " + self.path.split("?", 1)[0] + "\n")
            self.send_response(resp.status, resp.reason)
            for k, v in resp.getheaders():
                if k.lower() in HOP_BY_HOP or k.lower() == "content-length":
                    continue
                if changed and mode == "head" and k.lower() == "x-amz-checksum-sha256":
                    v = alter_base64(v)
                self.send_header(header_text(k), header_text(v))
            if self.command == "HEAD":
                length = resp.getheader("Content-Length")
                if length is not None:
                    self.send_header("Content-Length", header_text(length))
            else:
                self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(data)
            conn.close()
            return changed

        do_GET = do_PUT = do_POST = do_DELETE = do_HEAD = handle_any

    return Handler


def main(argv):
    if len(argv) != 5:
        sys.stderr.write(__doc__)
        return 2
    host, _, port = argv[2].rpartition(":")
    handler = make_handler((host, int(port)), argv[3], argv[4])
    server = ThreadingHTTPServer(("127.0.0.1", int(argv[1])), handler)
    server.daemon_threads = True
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
