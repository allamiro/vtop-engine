"""The spool generator (#748 "Setup"): seeded RFC 5424 lines into size-rotated files, and the one rotation hook.

Every line carries a unique id token, `id=vtb-<run tag>-<sequence>`
(`compare_common.checker.make_id`), in its message, so the independent checker
finds it whether a tool stores the raw line or wraps it in JSON. A line is a
pure function of (seed, run tag, sequence, line size), so the same seed gives
the same bytes on every host (`line`).

The generator is the ONLY writer of the spool, and it owns rotation, so a
rotation never races a write (`Spool.rotate`). Both styles are the same
function for every tool:

- `rename`: `app.log` is renamed to `app.log.<k>` (k = 1, 2, ... never reused)
  and a new, empty `app.log` is created;
- `copytruncate`: `app.log` is copied to `app.log.<k>`, then truncated to zero
  in place (same inode).

Rotated files are never deleted during a run: the spool is the source of truth
the checker's expected set is drawn from.

In a run the generator is a service (`serve`), driven over a small HTTP
control port by the harness, so the backlog, the live writer and every
rotation happen inside the Docker VM, on the same volume the tools read (a
host bind mount would cross a virtualised filesystem whose change
notifications tools cannot rely on):

    python3 -m spool_compare.generator serve --spool /spool --control 0.0.0.0:9400

`POST /setup {"source", "rotation", "seed", "tag", "line_bytes", "rotate_bytes"}`,
`POST /backlog {"count": N}`, `POST /rotate`, `POST /live {"rate": R, "seconds": T}`,
`GET /state`, `GET /written` (one `seq,epoch_seconds` line per line written live).
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

ACTIVE = "app.log"
STYLES = ("rename", "copytruncate")
APPS = ("sshd", "nginx", "kernel", "cron", "postfix", "sudo")
HOSTS = ("edge-1", "edge-2", "core-1", "core-2")
FILLER = "abcdefghijklmnopqrstuvwxyz0123456789" * 64
BASE_EPOCH = 1_795_000_000  # 2026-11-17, fixed so the backlog's timestamps do not depend on the clock


def make_id(tag: str, seq: int) -> str:
    # The same token compare_common.checker.make_id writes (and its regex reads); repeated here so the
    # generator runs in the harness container from this file and its package alone.
    return f"vtb-{tag}-{seq:010d}"


def line(seed: int, tag: str, seq: int, size: int = 200) -> str:
    """One RFC 5424 line (no trailing newline) of exactly `size` bytes, a pure function of its arguments."""
    rng = random.Random(f"{seed}:{seq}")
    stamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(BASE_EPOCH + seq // 1000))
    head = (f"<{rng.randint(1, 191)}>1 {stamp}.{seq % 1000:03d}Z {rng.choice(HOSTS)} {rng.choice(APPS)} "
            f"{rng.randint(1, 65535)} ID{rng.randint(1, 99)} - id={make_id(tag, seq)} msg=")
    if len(head) >= size:
        raise ValueError(f"a {size}-byte line cannot hold the {len(head)}-byte header")
    offset = rng.randrange(len(FILLER) // 2)
    return head + FILLER[offset:offset + size - len(head)]


class Spool:
    """One spool directory: the active file, its rotated predecessors, and the rotation hook."""

    def __init__(self, directory: str, rotation: str, *, seed: int, tag: str, line_bytes: int = 200,
                 rotate_bytes: int = 64 * 2**20, clock=time.time):
        if rotation not in STYLES:
            raise ValueError(f"rotation {rotation!r} is not one of {STYLES}")
        self.directory, self.rotation = directory, rotation
        self.seed, self.tag, self.line_bytes, self.rotate_bytes = seed, tag, line_bytes, rotate_bytes
        self.clock = clock
        self.next_seq = 0
        self.rotations: list[dict[str, Any]] = []
        self.written: list[tuple[int, float]] = []
        self.lock = threading.Lock()
        os.makedirs(directory, exist_ok=True)
        self.path = os.path.join(directory, ACTIVE)
        open(self.path, "a").close()

    def _size(self) -> int:
        return os.path.getsize(self.path)

    def rotate(self) -> dict[str, Any]:
        """The rotation hook: the same for every tool, chosen only by the spool's style."""
        with self.lock:
            return self._rotate()

    def _rotate(self) -> dict[str, Any]:
        k = len(self.rotations) + 1
        target = os.path.join(self.directory, f"{ACTIVE}.{k}")
        if os.path.exists(target):
            raise FileExistsError(f"{target} exists: rotated files are never overwritten")
        if self.rotation == "rename":
            os.rename(self.path, target)
            open(self.path, "a").close()
        else:
            shutil.copyfile(self.path, target)
            with open(self.path, "r+b") as handle:
                handle.truncate(0)
                handle.flush()
                os.fsync(handle.fileno())
        event = {"k": k, "style": self.rotation, "file": os.path.basename(target), "at": self.clock(),
                 "next_seq": self.next_seq}
        self.rotations.append(event)
        return event

    def write(self, count: int, *, record_times: bool = False) -> int:
        """Append `count` lines, rotating whenever the active file reaches `rotate_bytes`. Returns bytes written."""
        written = 0
        with self.lock:
            handle = open(self.path, "ab")  # noqa: SIM115 - reopened after each rotation
            try:
                for _ in range(count):
                    if handle.tell() >= self.rotate_bytes:
                        handle.flush()
                        os.fsync(handle.fileno())
                        handle.close()
                        self._rotate()
                        handle = open(self.path, "ab")  # noqa: SIM115
                    data = (line(self.seed, self.tag, self.next_seq, self.line_bytes) + "\n").encode()
                    handle.write(data)
                    if record_times:
                        handle.flush()
                        self.written.append((self.next_seq, self.clock()))
                    self.next_seq += 1
                    written += len(data)
                handle.flush()
                os.fsync(handle.fileno())
            finally:
                handle.close()
        return written

    def files(self) -> list[str]:
        return sorted(os.listdir(self.directory))

    def expected(self) -> range:
        return range(0, self.next_seq)


def live(spool: Spool, rate: float, seconds: float, stop: threading.Event, sleep=time.sleep) -> None:
    """Write at `rate` lines/s for `seconds`, open loop: a line is written when it is due, in bursts per 10 ms."""
    start = spool.clock()
    sent = 0
    while not stop.is_set():
        elapsed = spool.clock() - start
        if elapsed >= seconds:
            break
        due = int(elapsed * rate) - sent
        if due > 0:
            spool.write(due, record_times=True)
            sent += due
        sleep(0.01)


# ------------------------------------------------------------------- service


class _State:
    spool: Spool | None = None
    root = "/spool"
    live_thread: threading.Thread | None = None
    stop = threading.Event()


def _handler(state: _State):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:  # quiet
            pass

        def _answer(self, status: int, body: Any, text: bool = False) -> None:
            data = body.encode() if text else json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/plain" if text else "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802
            spool = state.spool
            if self.path == "/state":
                self._answer(200, {} if spool is None else {
                    "next_seq": spool.next_seq, "rotations": spool.rotations, "files": spool.files(),
                    "live_running": bool(state.live_thread and state.live_thread.is_alive())})
            elif self.path == "/written" and spool is not None:
                self._answer(200, "".join(f"{seq},{at:.6f}\n" for seq, at in spool.written), text=True)
            else:
                self._answer(404, {"error": self.path})

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            try:
                spec = json.loads(self.rfile.read(length) or b"{}")
                if self.path == "/setup":
                    directory = os.path.join(state.root, spec["source"])
                    state.spool = Spool(directory, spec["rotation"], seed=int(spec["seed"]), tag=spec["tag"],
                                        line_bytes=int(spec.get("line_bytes", 200)),
                                        rotate_bytes=int(spec.get("rotate_bytes", 64 * 2**20)))
                    self._answer(200, {"directory": directory})
                elif state.spool is None:
                    self._answer(409, {"error": "POST /setup first"})
                elif self.path == "/backlog":
                    started = time.time()
                    written = state.spool.write(int(spec["count"]))
                    self._answer(200, {"bytes": written, "next_seq": state.spool.next_seq,
                                       "seconds": round(time.time() - started, 3)})
                elif self.path == "/rotate":
                    self._answer(200, state.spool.rotate())
                elif self.path == "/live":
                    state.stop.clear()
                    state.live_thread = threading.Thread(
                        target=live, args=(state.spool, float(spec["rate"]), float(spec["seconds"]), state.stop),
                        daemon=True)
                    state.live_thread.start()
                    self._answer(200, {"started": time.time()})
                elif self.path == "/stop":
                    state.stop.set()
                    if state.live_thread:
                        state.live_thread.join(timeout=10)
                    self._answer(200, {"next_seq": state.spool.next_seq})
                else:
                    self._answer(404, {"error": self.path})
            except (KeyError, ValueError, OSError) as exc:
                self._answer(400, {"error": str(exc)})

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve")
    serve.add_argument("--spool", default="/spool")
    serve.add_argument("--control", default="0.0.0.0:9400")
    sample = sub.add_parser("sample", help="print lines to stdout")
    sample.add_argument("--seed", type=int, default=1)
    sample.add_argument("--tag", default="abcd1234")
    sample.add_argument("--count", type=int, default=5)
    sample.add_argument("--line-bytes", type=int, default=200)
    args = parser.parse_args(argv)
    if args.command == "sample":
        for seq in range(args.count):
            print(line(args.seed, args.tag, seq, args.line_bytes))
        return 0
    state = _State()
    state.root = args.spool
    host, _, port = args.control.rpartition(":")
    server = ThreadingHTTPServer((host or "0.0.0.0", int(port)), _handler(state))
    print(f"generator: spool {args.spool}, control {args.control}", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
