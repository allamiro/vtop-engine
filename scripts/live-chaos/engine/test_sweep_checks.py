"""Unit tests for the engine crash sweep's checks (#725).

Run: python3 -m unittest discover -s scripts/live-chaos/engine -p 'test_*.py'
"""

import gzip
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import sweep_checks as sc

SCHEMA = (
    "CREATE TABLE batches (batch_id TEXT PRIMARY KEY, source_type TEXT, state TEXT, "
    "progress_end_json TEXT, created_at TEXT, verification_class TEXT, "
    "object_uri TEXT, manifest_uri TEXT)"
)
INSERT = "INSERT INTO batches VALUES (?, 'kafka', ?, ?, ?, ?, ?, ?)"


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
        ok = ("strong", "s3://b/o", "s3://b/m")
        rows = [
            ("b1", "source_committed", marker(19), "1", *ok),
            ("b2", "verified", marker(39), "2", *ok),
            ("b3", "checksummed", marker(49), "3", None, None, None),
            ("other", "source_committed", marker(999, group="other"), "4", None, None, None),
        ]
        conn.executemany(INSERT, rows)
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

    def test_a_committed_row_without_the_verified_evidence_is_named(self):
        conn = sqlite3.connect(self.db)
        conn.execute(INSERT, ("b4", "source_committed", marker(59), "5", None, "s3://b/o", None))
        conn.commit()
        conn.close()
        s = sc.ledger_summary(self.db, "t", 0, "g")
        self.assertEqual(
            s["committed_unverified"],
            [
                {
                    "batch_id": "b4",
                    "end_offset": 59,
                    "lacking": ["verification_class", "manifest_uri"],
                }
            ],
        )
        # The other group's unverified committed row is not this sweep's.
        self.assertNotIn("other", json.dumps(s["committed_unverified"]))

    def test_a_missing_ledger_has_no_rows(self):
        s = sc.ledger_summary(str(Path(self.dir.name) / "absent.db"), "t", 0, "g")
        self.assertEqual(s["rows"], 0)
        self.assertIsNone(s["verified_end"])

# What `kafka-consumer-groups.sh --describe` (apache/kafka:4.3.1) printed,
# stdout and stderr together as run-sweep.sh captures them. The tool exits 0
# in every one of these cases.
DESCRIBE_COMMITTED = """
Consumer group 'g' has no active members.

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
g               other           0          7               9               2               -               -               -
g               t               0          20              50              30              -               -               -
"""
DESCRIBE_ACTIVE_UNCOMMITTED = """
GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                         HOST            CLIENT-ID
g               t               0          -               50              -               rdkafka-1b2c-4d5e                   /10.0.0.2       rdkafka
"""
DESCRIBE_OTHER_TOPIC_ONLY = """
Consumer group 'g' has no active members.

GROUP           TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
g               other           0          7               9               2               -               -               -
"""
DESCRIBE_NOT_FOUND = """
Error: Executing consumer group command failed due to org.apache.kafka.common.errors.GroupIdNotFoundException: Group g not found.
java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.GroupIdNotFoundException: Group g not found.
	at java.base/java.util.concurrent.CompletableFuture.reportGet(Unknown Source)
"""
DESCRIBE_TIMEOUT = """[2026-11-20 06:30:45,121] WARN [AdminClient clientId=adminclient-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Node may not be available. (org.apache.kafka.clients.NetworkClient)

Error: Executing consumer group command failed due to org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment. Call: describeConsumerGroups(api=FIND_COORDINATOR)
java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment. Call: describeConsumerGroups(api=FIND_COORDINATOR)
"""


class GroupOffsetTest(unittest.TestCase):
    """An offset the harness could not read must never pass as no offset."""

    def test_reads_the_partitions_committed_offset(self):
        self.assertEqual(sc.group_offset(DESCRIBE_COMMITTED, "g", "t", 0), "20")

    def test_an_active_member_without_a_commit_is_none(self):
        self.assertEqual(sc.group_offset(DESCRIBE_ACTIVE_UNCOMMITTED, "g", "t", 0), "-")

    def test_an_answered_table_without_the_partition_is_none(self):
        self.assertEqual(sc.group_offset(DESCRIBE_OTHER_TOPIC_ONLY, "g", "t", 0), "-")

    def test_a_group_the_broker_does_not_know_is_none(self):
        self.assertEqual(sc.group_offset(DESCRIBE_NOT_FOUND, "g", "t", 0), "-")

    def test_another_groups_not_found_is_not_this_groups_answer(self):
        with self.assertRaises(ValueError):
            sc.group_offset(DESCRIBE_NOT_FOUND, "g2", "t", 0)

    def test_a_timed_out_query_is_an_error_naming_it(self):
        with self.assertRaises(ValueError) as ctx:
            sc.group_offset(DESCRIBE_TIMEOUT, "g", "t", 0)
        self.assertIn("TimeoutException", str(ctx.exception))

    def test_no_output_is_an_error(self):
        with self.assertRaises(ValueError) as ctx:
            sc.group_offset("", "g", "t", 0)
        self.assertIn("(no output)", str(ctx.exception))

    def test_an_offset_that_is_not_a_number_is_an_error(self):
        bad = DESCRIBE_COMMITTED.replace(" 20 ", " 2x ")
        with self.assertRaises(ValueError):
            sc.group_offset(bad, "g", "t", 0)

    def test_the_cli_fails_on_a_timeout_and_prints_nothing_on_stdout(self):
        script = str(Path(sc.__file__))
        out = subprocess.run(
            [sys.executable, script, "group-offset", "g", "t", "0"],
            input=DESCRIBE_TIMEOUT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(out.returncode, 1)
        self.assertEqual(out.stdout, "")
        self.assertIn("could not read the committed offset of group g", out.stderr)


class CommitRuleTest(unittest.TestCase):
    def test_a_commit_at_the_verified_end_holds(self):
        ok, _ = sc.commit_rule("40", {"verified_end": 39, "states": {}}, "p", "at kill")
        self.assertTrue(ok)

    def test_no_commit_holds(self):
        ok, _ = sc.commit_rule("-", {"verified_end": None, "states": {}}, "p", "at kill")
        self.assertTrue(ok)

    def test_an_unread_offset_is_not_taken_for_no_commit(self):
        ok, msg = sc.commit_rule("", {"verified_end": None, "states": {}}, "p", "at kill")
        self.assertFalse(ok)
        self.assertIn("was not read", msg)

    def test_a_commit_past_the_verified_end_is_a_violation_naming_both(self):
        ok, msg = sc.commit_rule(
            "50", {"verified_end": 39, "states": {"VERIFIED": 1}}, "p", "at kill"
        )
        self.assertFalse(ok)
        self.assertIn("COMMIT RULE VIOLATED", msg)
        self.assertIn("is 50", msg)
        self.assertIn("ends at 39", msg)

    def test_a_committed_row_that_skipped_verified_is_a_violation(self):
        ledger = {
            "verified_end": 19,
            "states": {"SOURCE_COMMITTED": 1},
            "committed_unverified": [
                {"batch_id": "b1", "end_offset": 19, "lacking": ["verification_class"]}
            ],
        }
        ok, msg = sc.commit_rule("20", ledger, "p", "after the restart")
        self.assertFalse(ok)
        self.assertIn("COMMIT RULE VIOLATED", msg)
        self.assertIn("without the VERIFIED write's evidence", msg)
        self.assertIn("b1", msg)

    def test_a_commit_with_nothing_verified_is_a_violation(self):
        ok, msg = sc.commit_rule("1", {"verified_end": None, "states": {}}, "p", "at kill")
        self.assertFalse(ok)
        self.assertIn("no range is recorded as VERIFIED", msg)


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
