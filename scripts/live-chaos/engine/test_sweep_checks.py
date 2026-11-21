"""Unit tests for the engine crash sweep's checks (#725).

Run: python3 -m unittest discover -s scripts/live-chaos/engine -p 'test_*.py'
"""

import gzip
import json
import re
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

def file_marker(end, path="/in/sweep.log"):
    return json.dumps(
        {
            "source_type": "file",
            "path": path,
            "inode": 7,
            "start_byte": 0,
            "end_byte": end,
            "file_size": 3000,
            "mtime": "2026-11-20T00:00:00Z",
        }
    )


# What `psql ... json_agg` prints for the PostgreSQL ledger: states in lower
# case, one object per row, the columns the checks read.
def pg_rows(*rows):
    return [
        {
            "batch_id": batch_id,
            "state": state,
            "progress_end_json": end_json,
            "verification_class": verification_class,
            "object_uri": object_uri,
            "manifest_uri": manifest_uri,
            "created_at": f"2026-11-20T00:00:0{n}Z",
        }
        for n, (batch_id, state, end_json, verification_class, object_uri, manifest_uri) in enumerate(rows)
    ]


class FileAndPostgresLedgerTest(unittest.TestCase):
    """The same summary from a file source's rows and from PostgreSQL's."""

    OK = ("strong", "s3://b/o", "s3://b/m")

    def rows(self):
        return pg_rows(
            ("f1", "source_committed", file_marker(1300), *self.OK),
            ("f2", "verified", file_marker(2600), *self.OK),
            ("f3", "checksummed", file_marker(3000), None, None, None),
            ("elsewhere", "source_committed", file_marker(9999, path="/in/other.log"), None, None, None),
        )

    def test_summarises_one_file(self):
        s = sc.summarize(self.rows(), sc.Source("file", "/in/sweep.log"))
        self.assertEqual(s["rows"], 3)
        self.assertEqual(s["verified_end"], 2600)
        self.assertEqual(s["committed_end"], 1300)
        self.assertEqual(s["newest_state"], "CHECKSUMMED")
        # Another file's unverified committed row is not this sweep's.
        self.assertEqual(s["committed_unverified"], [])

    def test_a_file_row_committed_without_the_evidence_is_named_by_its_byte(self):
        rows = self.rows() + pg_rows(("f4", "source_committed", file_marker(3000), None, "s3://b/o", None))
        s = sc.summarize(rows, sc.Source("file", "/in/sweep.log"))
        self.assertEqual(
            s["committed_unverified"],
            [{"batch_id": "f4", "end_byte": 3000, "lacking": ["verification_class", "manifest_uri"]}],
        )

    def test_a_kafka_summary_from_postgres_rows_ignores_file_rows(self):
        rows = pg_rows(
            ("b1", "source_committed", marker(19), *self.OK),
            ("f1", "source_committed", file_marker(1300), *self.OK),
        )
        s = sc.summarize(rows, sc.Source("kafka", "t", "0", "g"))
        self.assertEqual((s["rows"], s["committed_end"]), (1, 19))

    def test_the_cli_reads_rows_from_stdin_and_names_a_bad_source(self):
        script = str(Path(sc.__file__))
        out = subprocess.run(
            [sys.executable, script, "ledger", "--rows-json", "file", "/in/sweep.log"],
            input=json.dumps(self.rows()),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertEqual(json.loads(out.stdout)["verified_end"], 2600)
        bad = subprocess.run(
            [sys.executable, script, "ledger", "--rows-json", "kafka", "t"],
            input="[]",
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(bad.returncode, 2)
        self.assertIn("a source is", bad.stderr)

    def test_an_empty_ledger_has_no_newest_state(self):
        s = sc.summarize([], sc.Source("file", "/in/sweep.log"))
        self.assertIsNone(s["newest_state"])


class FileCommitRuleTest(unittest.TestCase):
    def test_a_cursor_at_the_verified_end_holds(self):
        ok, _ = sc.commit_rule("2600", {"verified_end": 2600, "states": {}}, "p", "at the kill", "file")
        self.assertTrue(ok)

    def test_a_cursor_past_the_verified_end_is_a_violation(self):
        ok, msg = sc.commit_rule(
            "3000", {"verified_end": 2600, "states": {"VERIFIED": 1}}, "p", "at the kill", "file"
        )
        self.assertFalse(ok)
        self.assertIn("COMMIT RULE VIOLATED", msg)
        self.assertIn("file source's ledger cursor", msg)

    def test_a_kafka_commit_may_sit_one_past_the_last_offset_a_file_cursor_may_not(self):
        ledger = {"verified_end": 39, "states": {}}
        self.assertTrue(sc.commit_rule("40", ledger, "p", "w")[0])
        self.assertFalse(sc.commit_rule("40", ledger, "p", "w", "file")[0])


def listed_points():
    """The failpoints the engine defines, read from its source."""
    text = (Path(__file__).resolve().parents[3] / "crates/vtop-cli/src/failpoints.rs").read_text()
    block = text[text.index("pub const POINTS"):]
    consts = dict(re.findall(r'pub const (\w+): &str = "(\w+)";', text))
    names = re.findall(r"^\s+([A-Z_]+),$", block[: block.index("];")], re.M)
    return [consts[n] for n in names]


class KillStateTest(unittest.TestCase):
    def test_every_point_the_engine_defines_has_an_expected_state(self):
        points = listed_points()
        self.assertEqual(len(points), 21)
        self.assertEqual(sorted(points), sorted(sc.KILL_STATE))

    def test_the_table_agrees_with_the_in_repo_test(self):
        # The same states as state_at_kill in tests/failpoints.rs.
        text = (Path(__file__).resolve().parents[3] / "crates/vtop-cli/tests/failpoints.rs").read_text()
        body = text[text.index("fn state_at_kill"): text.index("/// F2")]
        for point, state in sc.KILL_STATE.items():
            const = point.upper()
            arm_start = body.find(f"failpoints::{const}")
            self.assertGreaterEqual(arm_start, 0, point)
            if state is None:
                self.assertIn("=> return None", body[arm_start:arm_start + 80])
            else:
                after = body[arm_start:]
                # The first state name after the point's arm start.
                found = re.search(r'"([A-Z_]+)"', after).group(1)
                self.assertEqual(found, state, point)

    def test_a_row_in_the_state_the_point_sits_beside_holds(self):
        ok, _ = sc.kill_state("before_source_commit", {"newest_state": "VERIFIED", "states": {}})
        self.assertTrue(ok)
        ok, _ = sc.kill_state("before_insert", {"newest_state": None, "states": {}})
        self.assertTrue(ok)

    def test_a_source_commit_reached_before_verified_is_named(self):
        # What the engine's ledger holds when its source commit runs ahead
        # of the VERIFIED write: the batch is still MANIFEST_UPLOADED.
        for point in ("before_source_commit", "after_source_commit_before_ledger"):
            ok, msg = sc.kill_state(point, {"newest_state": "MANIFEST_UPLOADED", "states": {"MANIFEST_UPLOADED": 1}})
            self.assertFalse(ok, point)
            self.assertIn("a MANIFEST_UPLOADED row", msg)
            self.assertIn("must find a VERIFIED row", msg)

    def test_the_batch_at_the_point_need_not_be_the_newest(self):
        # Batches overlap across cycles: when one is SOURCE_COMMITTED at the
        # point, a window read after it can already be VERIFIED and waiting.
        ok, msg = sc.kill_state(
            "after_ledger_source_committed",
            {"newest_state": "VERIFIED", "states": {"SOURCE_COMMITTED": 1, "VERIFIED": 1}},
        )
        self.assertTrue(ok, msg)
        self.assertIn("a ledger row is SOURCE_COMMITTED", msg)

    def test_a_row_where_none_should_be_is_named(self):
        ok, msg = sc.kill_state("before_insert", {"newest_state": "BATCHING", "states": {}})
        self.assertFalse(ok)
        self.assertIn("must find no row yet", msg)

    def test_an_unknown_point_is_an_error(self):
        ok, msg = sc.kill_state("invented", {"newest_state": None})
        self.assertFalse(ok)
        self.assertIn("no expected ledger state", msg)


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
            (root / "a.receipt.json").write_text('{"r000002": 1}')
            out = sc.readback(d, 4, "r")
            self.assertEqual(out["objects"], 2)
            self.assertEqual(out["missing"], ["r000002"])
            self.assertEqual(out["duplicated_ids"], 1)
            self.assertEqual(out["extra_copies"], 1)

    def test_an_object_whose_source_path_says_manifest_is_still_counted(self):
        # A file source's path is in the object's name: the sweep's own point
        # name `after_manifest_put_before_ledger` was in it, and the object
        # was skipped as if it were a manifest: 50 records "missing".
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "source=_in_after_manifest_put_before_ledger.log"
            root.mkdir()
            (root / "vtop-x_after_manifest_receipt_log-b0-1560-i1.raw.gz").write_bytes(
                gzip.compress(b"r000000 x\nr000001 y\n")
            )
            out = sc.readback(d, 2, "r")
            self.assertEqual(out["objects"], 1)
            self.assertEqual(out["missing"], [])


if __name__ == "__main__":
    unittest.main()
