"""Unit tests for the engine crash sweep's checks (#725).

Run: python3 -m unittest discover -s scripts/live-chaos/engine -p 'test_*.py'
"""

import gzip
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import sweep_checks as sc

SCHEMA = (
    "CREATE TABLE batches (batch_id TEXT PRIMARY KEY, source_type TEXT, state TEXT, "
    "progress_end_json TEXT, created_at TEXT)"
)


def marker(end, topic="t", partition=0, group="g"):
    return json.dumps(
        {
            "source_type": "kafka",
            "topic": topic,
            "partition": partition,
            "start_offset": 0,
            "end_offset": end,
            "consumer_group": group,
        }
    )


class PointsTest(unittest.TestCase):
    def test_reads_the_indented_names_only(self):
        text = (
            "Error: configuration error: VTOP_FAILPOINT=\"x\" names no failpoint...:\n"
            "  before_insert\n  after_insert\n  not a point\n"
            "error: the same message again:\n  before_insert\n  after_insert\n"
        )
        self.assertEqual(sc.parse_points(text), ["before_insert", "after_insert"])

    def test_an_unlisted_point_is_reported(self):
        self.assertEqual(sc.complete(["a", "b", "c"], ["a", "c"]), ["b"])
        self.assertEqual(sc.complete(["a"], ["a"]), [])


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.db = str(Path(self.dir.name) / "state.db")
        conn = sqlite3.connect(self.db)
        conn.execute(SCHEMA)
        rows = [
            ("b1", "source_committed", marker(19), "1"),
            ("b2", "verified", marker(39), "2"),
            ("b3", "checksummed", marker(49), "3"),
            ("other", "source_committed", marker(999, group="other"), "4"),
        ]
        conn.executemany(
            "INSERT INTO batches VALUES (?, 'kafka', ?, ?, ?)", rows
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        self.dir.cleanup()

    def test_summarises_one_partition_and_group(self):
        s = sc.ledger_summary(self.db, "t", 0, "g")
        self.assertEqual(s["rows"], 3)
        self.assertEqual(s["verified_end"], 39)
        self.assertEqual(s["committed_end"], 19)
        self.assertEqual(
            [r["state"] for r in s["in_flight"]], ["VERIFIED", "CHECKSUMMED"]
        )

    def test_a_missing_ledger_has_no_rows(self):
        s = sc.ledger_summary(str(Path(self.dir.name) / "absent.db"), "t", 0, "g")
        self.assertEqual(s["rows"], 0)
        self.assertIsNone(s["verified_end"])


class CommitRuleTest(unittest.TestCase):
    def test_a_commit_at_the_verified_end_holds(self):
        ok, _ = sc.commit_rule("40", {"verified_end": 39, "states": {}}, "p", "at kill")
        self.assertTrue(ok)

    def test_no_commit_holds(self):
        ok, _ = sc.commit_rule("-", {"verified_end": None, "states": {}}, "p", "at kill")
        self.assertTrue(ok)

    def test_a_commit_past_the_verified_end_is_a_violation_naming_both(self):
        ok, msg = sc.commit_rule(
            "50", {"verified_end": 39, "states": {"VERIFIED": 1}}, "p", "at kill"
        )
        self.assertFalse(ok)
        self.assertIn("COMMIT RULE VIOLATED", msg)
        self.assertIn("is 50", msg)
        self.assertIn("ends at 39", msg)

    def test_a_commit_with_nothing_verified_is_a_violation(self):
        ok, _ = sc.commit_rule("1", {"verified_end": None, "states": {}}, "p", "at kill")
        self.assertFalse(ok)


class ReadbackTest(unittest.TestCase):
    def test_counts_missing_and_duplicated_records(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "tenant=x"
            root.mkdir()
            (root / "a.txt.gz").write_bytes(
                gzip.compress(b"r000000 x\nr000001 y\n")
            )
            (root / "b.txt.gz").write_bytes(gzip.compress(b"r000001 y\nr000003 z\n"))
            (root / "a.manifest.json").write_text('{"r000002": 1}')
            out = sc.readback(d, 4, "r")
            self.assertEqual(out["objects"], 2)
            self.assertEqual(out["missing"], ["r000002"])
            self.assertEqual(out["duplicated_ids"], 1)
            self.assertEqual(out["extra_copies"], 1)


if __name__ == "__main__":
    unittest.main()
