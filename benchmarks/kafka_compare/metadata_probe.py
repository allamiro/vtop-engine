#!/usr/bin/env python3
"""Who leads the partition, as each Kafka broker says it (#597): the Kafka side's
"time to new leader" observer.

    python3 metadata_probe.py --brokers kafka-1:9092,kafka-2:9092,kafka-3:9092 \
        --topic bench --out /out/metadata.jsonl --stop-file /out/stop

One thread per broker sends a Metadata request (v4, the oldest version Kafka 4
still serves) every `--interval`
seconds and writes a JSON line whenever that broker's answer CHANGES: reachable
or not, the partition's leader, its ISR. Standard library only, so it runs in a
stock python image on the cluster's network, where the brokers' advertised
names resolve and its clock is the Docker VM's, the clock the producer stamps
its acknowledgements with.

It feeds no client: the producer finds leaders through its own metadata
refreshes. It only says when the cluster began to name the new leader.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import struct
import threading
import time

API_METADATA = 3
VERSION = 4


class ProtocolError(Exception):
    pass


def encode_request(correlation_id: int, topic: str, client_id: str = "kc-metadata-probe") -> bytes:
    """A size-prefixed Metadata v4 request for one topic, auto-creation off."""
    client = client_id.encode()
    name = topic.encode()
    body = (struct.pack(">hhi", API_METADATA, VERSION, correlation_id) + struct.pack(">h", len(client)) + client
            + struct.pack(">i", 1) + struct.pack(">h", len(name)) + name + struct.pack(">b", 0))
    return struct.pack(">i", len(body)) + body


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def take(self, fmt: str):
        size = struct.calcsize(fmt)
        if self.pos + size > len(self.data):
            raise ProtocolError(f"response truncated at byte {self.pos} reading {fmt}")
        value = struct.unpack_from(fmt, self.data, self.pos)
        self.pos += size
        return value[0] if len(value) == 1 else value

    def string(self, nullable: bool = False) -> str | None:
        length = self.take(">h")
        if length < 0:
            if nullable:
                return None
            raise ProtocolError(f"null string at byte {self.pos}")
        if self.pos + length > len(self.data):
            raise ProtocolError(f"string of {length} bytes runs past the response")
        text = self.data[self.pos:self.pos + length].decode()
        self.pos += length
        return text

    def int32s(self) -> list[int]:
        count = self.take(">i")
        if count < 0:
            return []
        return [self.take(">i") for _ in range(count)]


def decode_response(payload: bytes, topic: str, partition: int = 0) -> dict:
    """The fields of a Metadata v4 response (without its size prefix) the probe records.

    Returns {"correlation_id", "brokers": {id: "host:port"}, "controller", "leader", "isr",
    "replicas", "error"}; a topic or partition error is reported, not raised, since a
    broker that answers "leader not available" is itself an observation.
    """
    reader = _Reader(payload)
    correlation_id = reader.take(">i")
    reader.take(">i")  # throttle_time_ms
    brokers = {}
    for _ in range(reader.take(">i")):
        node_id = reader.take(">i")
        host = reader.string()
        port = reader.take(">i")
        reader.string(nullable=True)  # rack
        brokers[node_id] = f"{host}:{port}"
    reader.string(nullable=True)  # cluster_id
    controller = reader.take(">i")
    out = {"correlation_id": correlation_id, "brokers": brokers, "controller": controller, "leader": None,
           "isr": None, "replicas": None, "error": None}
    found = False
    for _ in range(reader.take(">i")):
        topic_error = reader.take(">h")
        name = reader.string()
        reader.take(">b")  # is_internal
        for _ in range(reader.take(">i")):
            part_error = reader.take(">h")
            index = reader.take(">i")
            leader = reader.take(">i")
            replicas = reader.int32s()
            isr = reader.int32s()
            if name == topic and index == partition:
                found = True
                out.update(leader=leader, isr=sorted(isr), replicas=sorted(replicas))
                if part_error:
                    out["error"] = f"partition error {part_error}"
        if name == topic and topic_error:
            out["error"] = f"topic error {topic_error}"
    if not found and out["error"] is None:
        out["error"] = "partition not in response"
    return out


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = []
    while size:
        chunk = sock.recv(size)
        if not chunk:
            raise ConnectionError("broker closed the connection")
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)


def _poll(address: str, topic: str, out_path: str, stop_file: str, interval: float, timeout: float,
          lock: threading.Lock) -> None:
    host, _, port = address.rpartition(":")
    sock = None
    correlation = 0
    last = None
    while not os.path.exists(stop_file):
        started = time.time()
        observed: dict
        try:
            if sock is None:
                sock = socket.create_connection((host, int(port)), timeout=timeout)
                sock.settimeout(timeout)
            correlation += 1
            sock.sendall(encode_request(correlation, topic))
            size = struct.unpack(">i", _recv_exact(sock, 4))[0]
            answer = decode_response(_recv_exact(sock, size), topic)
            if answer["correlation_id"] != correlation:
                raise ProtocolError(f"correlation {answer['correlation_id']} for request {correlation}")
            observed = {"ok": True, "leader": answer["leader"], "isr": answer["isr"], "error": answer["error"]}
        except (OSError, ProtocolError, ConnectionError) as exc:
            if sock is not None:
                sock.close()
                sock = None
            observed = {"ok": False, "leader": None, "isr": None, "error": type(exc).__name__}
        if observed != last:
            # Stamped under the lock, so the file is in time order across the
            # per-broker threads (a stamp taken before waiting for the lock
            # could land after a later one).
            with lock, open(out_path, "a") as handle:
                line = {"t_us": int(time.time() * 1e6), "broker": address, **observed}
                handle.write(json.dumps(line, sort_keys=True) + "\n")
            last = observed
        time.sleep(max(0.0, interval - (time.time() - started)))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--brokers", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--stop-file", required=True)
    parser.add_argument("--interval", type=float, default=0.1)
    parser.add_argument("--timeout", type=float, default=0.5)
    args = parser.parse_args()
    lock = threading.Lock()
    threads = [threading.Thread(target=_poll, args=(broker, args.topic, args.out, args.stop_file, args.interval,
                                                    args.timeout, lock), daemon=True)
               for broker in args.brokers.split(",")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
