#!/usr/bin/env python3
"""Tests for the disaster-recovery drills' own helpers (#735): the checks the
drills trust must be able to fail."""
import gzip
import http.client
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


checks = load("dr_checks", "dr_checks.py")
proxy = load("fault_proxy", "fault-proxy.py")


def rec(i):
    return ("dr-rec-%06d payload\n" % i).encode()


class CountRecords(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.dir.cleanup()

    def put(self, name, body, gz=True):
        path = os.path.join(self.dir.name, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(gzip.compress(body) if gz else body)

    def test_complete_archive_has_no_hole_and_no_duplicate(self):
        self.put("a/one.gz", b"".join(rec(i) for i in range(0, 5)))
        self.put("a/two.gz", b"".join(rec(i) for i in range(5, 10)))
        got = checks.count_records(self.dir.name, "dr-rec-", 10)
        self.assertEqual((got["missing"], got["duplicates"], got["unique"]), (0, 0, 10))
        self.assertEqual(got["data_objects"], 2)

    def test_a_missing_range_is_named(self):
        self.put("a/one.gz", b"".join(rec(i) for i in range(0, 4)))
        self.put("a/two.gz", b"".join(rec(i) for i in range(6, 10)))
        got = checks.count_records(self.dir.name, "dr-rec-", 10)
        self.assertEqual(got["missing"], 2)
        self.assertEqual(got["missing_first"], "4,5")

    def test_a_record_held_twice_is_a_duplicate(self):
        self.put("a/one.gz", b"".join(rec(i) for i in range(0, 6)))
        self.put("b/two.gz", b"".join(rec(i) for i in range(4, 8)))
        got = checks.count_records(self.dir.name, "dr-rec-", 8)
        self.assertEqual((got["missing"], got["duplicates"]), (0, 2))

    def test_manifests_hold_no_records(self):
        self.put("a/one.gz", b"".join(rec(i) for i in range(0, 3)))
        self.put("a/one.manifest.json", b'{"batch_id": "b", "record_count": 3}', gz=False)
        got = checks.count_records(self.dir.name, "dr-rec-", 3)
        self.assertEqual(got["data_objects"], 1)

    def test_a_corrupt_object_counts_as_missing_not_as_a_crash(self):
        self.put("a/one.gz", b"".join(rec(i) for i in range(0, 3)))
        path = os.path.join(self.dir.name, "a/one.gz")
        data = bytearray(open(path, "rb").read())
        data[len(data) // 2] ^= 0xFF
        open(path, "wb").write(bytes(data[:-8]))
        got = checks.count_records(self.dir.name, "dr-rec-", 3)
        self.assertEqual(got["missing"], 3)

    def test_the_result_file_types_its_values(self):
        tsv = os.path.join(self.dir.name, "r.tsv")
        out = os.path.join(self.dir.name, "r.json")
        open(tsv, "w").write("rto_ms\t1500\nrto_ms\t1600\npass\ttrue\nnote\ta b\n")
        checks.write_result(tsv, out, "demo")
        doc = json.load(open(out))
        self.assertEqual(doc, {"drill": "demo", "rto_ms": 1600, "pass": True, "note": "a b"})


class Upstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    seen_host = []

    def log_message(self, *_a):
        pass

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", "6")
        self.send_header("x-amz-checksum-sha256", "AbCdEf==")
        self.end_headers()

    def do_GET(self):
        Upstream.seen_host.append(self.headers.get("Host"))
        body = b"ABCDEF"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self):
        n = int(self.headers.get("Content-Length") or 0)
        got = self.rfile.read(n)
        self.send_response(200)
        self.send_header("Content-Length", str(len(got)))
        self.end_headers()
        self.wfile.write(got)


class FaultProxy(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.control = os.path.join(self.tmp.name, "fault")
        self.stats = os.path.join(self.tmp.name, "stats")
        self.up = HTTPServer(("127.0.0.1", 0), Upstream)
        threading.Thread(target=self.up.serve_forever, daemon=True).start()
        handler = proxy.make_handler(("127.0.0.1", self.up.server_port), self.control, self.stats)
        self.px = proxy.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.px.daemon_threads = True
        threading.Thread(target=self.px.serve_forever, daemon=True).start()

    def tearDown(self):
        self.px.shutdown()
        self.up.shutdown()
        self.tmp.cleanup()

    def get(self, path):
        c = http.client.HTTPConnection("127.0.0.1", self.px.server_port)
        c.request("GET", path, headers={"Host": "signed.example:1"})
        r = c.getresponse()
        return r.status, r.read()

    def test_passes_through_until_the_control_file_exists(self):
        self.assertEqual(self.get("/bkt/p/obj.gz"), (200, b"ABCDEF"))
        open(self.control, "w").close()
        status, body = self.get("/bkt/p/obj.gz")
        self.assertEqual((status, body), (200, b"@BCDEF"))
        self.assertEqual(open(self.stats).read().strip(), "GET /bkt/p/obj.gz")
        self.assertIn("signed.example:1", Upstream.seen_host)

    def test_manifests_and_pinned_versions_are_never_changed(self):
        open(self.control, "w").close()
        self.assertEqual(self.get("/bkt/p/obj.manifest.json")[1], b"ABCDEF")
        self.assertEqual(self.get("/bkt/p/obj.gz?versionId=1")[1], b"ABCDEF")

    def head(self, path):
        c = http.client.HTTPConnection("127.0.0.1", self.px.server_port)
        c.request("HEAD", path)
        r = c.getresponse()
        r.read()
        return r.status, r.getheader("x-amz-checksum-sha256"), r.getheader("Content-Length")

    def test_head_mode_alters_the_stores_checksum_and_leaves_bodies_alone(self):
        self.assertEqual(self.head("/bkt/p/obj.gz"), (200, "AbCdEf==", "6"))
        with open(self.control, "w") as f:
            f.write("head\n")
        self.assertEqual(self.head("/bkt/p/obj.gz"), (200, "BbCdEf==", "6"))
        self.assertEqual(self.head("/bkt/p/obj.manifest.json")[1], "AbCdEf==")
        self.assertEqual(self.get("/bkt/p/obj.gz")[1], b"ABCDEF")

    def test_a_put_passes_through_whole(self):
        c = http.client.HTTPConnection("127.0.0.1", self.px.server_port)
        c.request("PUT", "/bkt/p/obj.gz", body=b"payload")
        r = c.getresponse()
        self.assertEqual(r.read(), b"payload")


if __name__ == "__main__":
    unittest.main()
