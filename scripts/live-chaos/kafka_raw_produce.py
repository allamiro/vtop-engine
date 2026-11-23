#!/usr/bin/env python3
"""One Kafka Produce request on the wire, and what the broker answered (#751).

Usage: kafka_raw_produce.py <port> <topic> <key> <value> <wait-seconds>

Connects to 127.0.0.1:<port>, sends ONE Produce (v3, acks=all, one record in
a v2 batch with no producer id) for partition 0 of <topic>, and prints one line:

  acknowledged offset=<n>       the broker appended it (error code 0)
  refused code=<n> name=<NAME>  the broker answered with an error code
  closed                        the connection closed with no answer
  no-reply                      nothing came back within <wait-seconds>
  unreachable <reason>          the connection could not be made

Exit status is 0 for every one of those: the caller judges the answer.

Why not a stock client: a stock client asks Metadata first and goes where it
is told, so it can never be made to send a Produce to a node that no longer
leads. This is the request a client holding metadata from before a failover
sends to the old leader, which is the case scenario 32 asserts is refused.
"""

import socket
import struct
import sys
import time

TABLE = []
for n in range(256):
    c = n
    for _ in range(8):
        c = (c >> 1) ^ 0x82F63B78 if c & 1 else c >> 1
    TABLE.append(c)


def crc32c(data):
    c = 0xFFFFFFFF
    for b in data:
        c = TABLE[(c ^ b) & 0xFF] ^ (c >> 8)
    return c ^ 0xFFFFFFFF


# The Castagnoli check value: a wrong table would make every batch corrupt,
# and a corrupt batch is refused too, which would read as a pass.
assert crc32c(b"123456789") == 0xE3069283


def varint(value):
    value = (value << 1) ^ (value >> 63)
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def string(text):
    raw = text.encode()
    return struct.pack(">h", len(raw)) + raw


def batch(key, value):
    """A v2 record batch of one record, laid out as RecordBatch::encode does."""
    now = int(time.time() * 1000)
    body = (
        bytes([0])  # record attributes
        + varint(0)  # timestamp delta
        + varint(0)  # offset delta
        + varint(len(key))
        + key
        + varint(len(value))
        + value
        + varint(0)  # no headers
    )
    record = varint(len(body)) + body
    # attributes, lastOffsetDelta, base and max timestamp, producer id and
    # epoch (none), base sequence (none), record count
    covered = struct.pack(">hiqqqhii", 0, 0, now, now, -1, -1, -1, 1) + record
    tail = struct.pack(">bI", 2, crc32c(covered)) + covered
    # baseOffset, batchLength (everything after it), partitionLeaderEpoch
    return struct.pack(">qii", 0, 4 + len(tail), -1) + tail


NAMES = {
    0: "NONE",
    2: "CORRUPT_MESSAGE",
    3: "UNKNOWN_TOPIC_OR_PARTITION",
    6: "NOT_LEADER_OR_FOLLOWER",
    7: "REQUEST_TIMED_OUT",
    19: "NOT_ENOUGH_REPLICAS",
    20: "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
    74: "FENCED_LEADER_EPOCH",
    75: "UNKNOWN_LEADER_EPOCH",
    87: "INVALID_RECORD",
}


def read_exactly(sock, count, deadline):
    out = bytearray()
    while len(out) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        sock.settimeout(remaining)
        chunk = sock.recv(count - len(out))
        if not chunk:
            raise ConnectionError
        out += chunk
    return bytes(out)


def main():
    port, topic = int(sys.argv[1]), sys.argv[2]
    key, value = sys.argv[3].encode(), sys.argv[4].encode()
    wait = float(sys.argv[5])
    records = batch(key, value)
    correlation = 7531
    request = struct.pack(">hhi", 0, 3, correlation) + string("vtop-live-chaos")
    # transactional id null, acks=all, the broker's own wait for replicas
    request += struct.pack(">hhi", -1, -1, int(min(wait, 30) * 1000))
    request += struct.pack(">i", 1) + string(topic)
    request += struct.pack(">ii", 1, 0) + struct.pack(">i", len(records)) + records
    deadline = time.monotonic() + wait
    try:
        sock = socket.create_connection(("127.0.0.1", port), timeout=wait)
    except OSError as error:
        print("unreachable %s" % error)
        return
    try:
        sock.sendall(struct.pack(">i", len(request)) + request)
        size = struct.unpack(">i", read_exactly(sock, 4, deadline))[0]
        reply = read_exactly(sock, size, deadline)
    except TimeoutError:
        print("no-reply")
        return
    except (ConnectionError, OSError):
        print("closed")
        return
    finally:
        sock.close()
    got, topics = struct.unpack_from(">ii", reply, 0)
    if got != correlation or topics != 1:
        sys.exit("unexpected reply: correlation %d, %d topics" % (got, topics))
    name_len = struct.unpack_from(">h", reply, 8)[0]
    at = 10 + name_len
    partitions, index, code, offset = struct.unpack_from(">iihq", reply, at)
    if partitions != 1 or index != 0:
        sys.exit("unexpected reply: %d partitions, partition %d" % (partitions, index))
    if code == 0:
        print("acknowledged offset=%d" % offset)
    else:
        print("refused code=%d name=%s" % (code, NAMES.get(code, "UNKNOWN")))


if __name__ == "__main__":
    main()
