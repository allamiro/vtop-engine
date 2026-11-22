"""Tests for check_control.py (#728).

Each test builds a control-plane history by hand, a clean one or one shaped
like a bug the checker exists to catch, and pins the verdict. Run with:

    python3 -m unittest discover -s scripts/live-chaos/history -p 'test_*.py'
"""

from __future__ import annotations

import io
import json
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import check_control  # noqa: E402

A, B, C = "uuid-a", "uuid-b", "uuid-c"


def status(t, node_id, term, state="Follower", vote_term=None, voted_for=None, committed=False, ok=True):
    return {"op": "meta_status", "via": str(node_id), "invoke_ms": t, "complete_ms": t + 5, "ok": ok,
            "node_id": node_id, "term": term, "state": state, "leader": voted_for,
            "vote_term": term if vote_term is None else vote_term, "voted_for": voted_for,
            "vote_committed": committed}


def read(t, epoch, holder=None, expires=None, index=None, span=5, ok=True):
    return {"op": "lease_read", "via": "1", "invoke_ms": t, "complete_ms": t + span, "ok": ok,
            "found": True, "range_epoch": epoch, "applied_index": epoch * 10 if index is None else index,
            "holder": holder, "lease_epoch": epoch if holder else None, "expires_at_ms": expires}


def node(t, name, uuid, active, epoch, held=None, span=2, ok=True):
    return {"op": "node_lease", "node": name, "node_uuid": uuid, "invoke_ms": t, "complete_ms": t + span,
            "ok": ok, "active": active, "epoch": epoch, "held": epoch if held is None else held}


def clean_failover() -> list[dict]:
    """A holds epoch 1, dies; B takes epoch 2 after A's lease expires at 1000."""
    return [
        status(0, 1, 1, "Leader", voted_for=1, committed=True),
        status(0, 2, 1, "Follower", voted_for=1, committed=True),
        read(10, 1, A, expires=1000),
        node(20, "c1", A, True, 1),
        node(20, "c2", B, False, 0),
        read(500, 1, A, expires=1000),
        node(600, "c1", A, True, 1),
        node(900, "c1", A, False, 1, ok=False),  # killed
        read(1100, 2, B, expires=7000),
        node(1200, "c2", B, True, 2),
        status(1300, 3, 1, "Follower", voted_for=1, committed=True),
        read(1400, 2, B, expires=7000),
    ]


def run(events):
    return check_control.check(events)


class Clean(unittest.TestCase):
    def test_a_clean_failover_is_valid(self):
        result = run(clean_failover())
        self.assertTrue(result["valid"], result["violations"])
        self.assertEqual(result["epochs_led"], {"1": [A], "2": [B]})
        self.assertEqual(result["lease_epochs"], [1, 2])

    def test_failed_observations_are_evidence_of_nothing(self):
        events = clean_failover() + [read(1500, 0, C, ok=False), node(1500, "c3", C, True, 1, ok=False)]
        self.assertTrue(run(events)["valid"])

    def test_overlapping_reads_may_see_either_epoch(self):
        # B began before A ended: either order is a linearization.
        events = [read(0, 2, B, span=100), read(50, 1, A, span=100)]
        self.assertTrue(run(events)["valid"])

    def test_an_operator_grant_without_a_deadline_is_not_held_to_it(self):
        events = [read(0, 1, A), read(100, 2, B), node(200, "c1", A, True, 1)]
        self.assertTrue(run(events)["valid"], run(events)["violations"])

    def test_a_leader_acting_before_its_deadline_is_not_stale(self):
        events = [read(0, 1, A, expires=1000), read(100, 2, B, expires=7000), node(500, "c1", A, True, 1)]
        self.assertTrue(run(events)["valid"])

    def test_a_new_term_with_a_new_leader_is_valid(self):
        events = [status(0, 1, 1, "Leader", voted_for=1, committed=True),
                  status(10, 2, 2, "Leader", voted_for=2, committed=True)]
        self.assertTrue(run(events)["valid"])


class Violations(unittest.TestCase):
    def kinds(self, events):
        result = run(events)
        self.assertFalse(result["valid"])
        return set(result["violations"])

    def test_two_raft_leaders_in_one_term(self):
        events = [status(0, 1, 3, "Leader"), status(0, 2, 3, "Leader")]
        self.assertEqual(self.kinds(events), {"meta_leader"})

    def test_a_committed_vote_naming_another_leader(self):
        events = [status(0, 1, 3, "Leader"), status(0, 2, 3, "Follower", voted_for=2, committed=True)]
        self.assertEqual(self.kinds(events), {"meta_leader"})

    def test_an_uncommitted_vote_names_no_leader(self):
        events = [status(0, 1, 3, "Leader"), status(0, 2, 3, "Candidate", voted_for=2, committed=False)]
        self.assertTrue(run(events)["valid"])

    def test_one_lease_epoch_with_two_holders(self):
        events = [read(0, 4, A), read(100, 4, B)]
        self.assertEqual(self.kinds(events), {"lease_holder"})

    def test_a_stale_lease_read(self):
        # The second read began after the first ended and went back an epoch:
        # a read served from a deposed metadata leader's state.
        events = [read(0, 5, B, index=50), read(100, 4, A, index=60)]
        self.assertIn("lease_order", self.kinds(events))

    def test_a_range_not_found_after_it_was_found(self):
        # Review of #904: an answered "not found" is a read too.
        gone = dict(read(100, 0, index=0), found=False, range_epoch=None)
        self.assertEqual(self.kinds([read(0, 5, B, index=50), gone]), {"lease_order", "meta_index"})

    def test_not_found_before_the_range_exists_is_valid(self):
        early = dict(read(0, 0, index=1), found=False, range_epoch=None)
        self.assertTrue(run([early, read(100, 1, A, index=10)])["valid"])

    def test_an_applied_index_going_back(self):
        events = [read(0, 5, B, index=60), read(100, 5, B, index=50)]
        self.assertEqual(self.kinds(events), {"meta_index"})

    def test_two_data_nodes_leading_one_epoch(self):
        events = [read(0, 3, A, expires=9000), node(10, "c1", A, True, 3), node(20, "c2", B, True, 3)]
        self.assertEqual(self.kinds(events), {"epoch_leader"})

    def test_a_data_node_leading_an_epoch_granted_to_another(self):
        events = [read(0, 3, A, expires=9000), node(10, "c2", B, True, 3)]
        self.assertEqual(self.kinds(events), {"epoch_leader"})

    def test_a_data_node_leading_an_epoch_metadata_never_granted(self):
        events = [read(0, 2, A, expires=9000), node(10, "c2", B, True, 3), read(100, 2, A, expires=9000)]
        self.assertEqual(self.kinds(events), {"unknown_grant"})

    def test_a_deposed_leader_that_keeps_acting(self):
        # The #641 / pause shape from the lease side: A's lease ran out at
        # 1000, B was granted epoch 2, and A still says it holds epoch 1,
        # for longer than a cached gauge could repeat it.
        events = clean_failover() + [node(1600, "c1", A, True, 1), node(4000, "c1", A, True, 1)]
        self.assertEqual(self.kinds(events), {"stale_leader"})

    def test_one_late_claim_may_be_a_cached_gauge(self):
        # A scrape that cannot take the lease view repeats the last value
        # (review of #904): one such observation, or a few within 2 s, is
        # not evidence the node still acts.
        events = clean_failover() + [node(1600, "c1", A, True, 1), node(2000, "c1", A, True, 1)]
        self.assertTrue(run(events)["valid"], run(events)["violations"])

    def test_a_held_epoch_that_disagrees_is_no_claim(self):
        # The broker refuses every request while the two differ.
        events = [read(0, 3, A, expires=9000), node(10, "c2", B, True, 3, held=2)]
        self.assertTrue(run(events)["valid"])


class Stalled(unittest.TestCase):
    """--stall-ms: a cluster that keeps granting the range and never serves (#640)."""

    def stalled_history(self):
        # A held epoch 1, died; the lease keeps being granted (2, 3, 4) to
        # candidates whose promotions never succeed, so none acts.
        return [
            read(0, 1, A, expires=1000), node(10, "c1", A, True, 1),
            read(2000, 2, B, expires=8000), read(9000, 3, C, expires=15000),
            read(16000, 4, B, expires=22000), node(20000, "c2", B, False, 4),
        ]

    def test_off_by_default(self):
        self.assertTrue(run(self.stalled_history())["valid"])

    def test_a_cluster_that_stops_electing_is_stalled(self):
        result = check_control.check(self.stalled_history(), stall_ms=10_000)
        self.assertEqual(set(result["violations"]), {"stalled"})

    def test_a_short_gap_is_not_a_stall(self):
        self.assertTrue(check_control.check(self.stalled_history(), stall_ms=30_000)["valid"])

    def test_a_cluster_that_ends_with_a_leader_is_not_stalled(self):
        events = self.stalled_history() + [node(21000, "c3", C, True, 5), read(20500, 5, C, expires=30000)]
        self.assertTrue(check_control.check(events, stall_ms=10_000)["valid"])

    def test_the_flag_is_read_from_the_command_line(self):
        handle = tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False)
        handle.write("".join(json.dumps(e) + "\n" for e in self.stalled_history()))
        handle.close()
        self.addCleanup(pathlib.Path(handle.name).unlink)
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(io.StringIO()):
            code = check_control.main(["check_control.py", "--stall-ms", "10000", handle.name])
        self.assertEqual(code, 1, out.getvalue())
        self.assertIn("stalled", json.loads(out.getvalue())["violations"])


class Coverage(unittest.TestCase):
    """--require-observations: a recorder that saw nothing checked nothing (review of #904)."""

    def test_off_by_default_an_empty_history_is_valid(self):
        self.assertTrue(run([])["valid"])

    def test_an_empty_history_fails_when_observations_are_required(self):
        result = check_control.check([], require_observations=True)
        self.assertEqual(set(result["violations"]), {"coverage"})
        self.assertEqual(result["violations"]["coverage"]["count"], 3)

    def test_only_failed_observations_fail_when_required(self):
        events = [status(0, 1, 1, ok=False), read(0, 1, A, ok=False), node(0, "c1", A, True, 1, ok=False)]
        self.assertEqual(set(check_control.check(events, require_observations=True)["violations"]), {"coverage"})

    def test_one_answered_observation_of_each_kind_is_enough(self):
        self.assertTrue(check_control.check(clean_failover(), require_observations=True)["valid"])

    def test_the_flags_combine_on_the_command_line(self):
        handle = tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False)
        handle.write(json.dumps(read(0, 1, A, ok=False)) + "\n")
        handle.close()
        self.addCleanup(pathlib.Path(handle.name).unlink)
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(io.StringIO()):
            code = check_control.main(["check_control.py", "--stall-ms", "5", "--require-observations", handle.name])
        self.assertEqual(code, 1, out.getvalue())
        self.assertIn("coverage", json.loads(out.getvalue())["violations"])


class Input(unittest.TestCase):
    def write(self, text: str) -> str:
        handle = tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False)
        handle.write(text)
        handle.close()
        self.addCleanup(pathlib.Path(handle.name).unlink)
        return handle.name

    def main(self, *paths):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = check_control.main(["check_control.py", *paths])
        return code, out.getvalue(), err.getvalue()

    def test_a_cut_last_line_is_ignored_and_noted(self):
        body = "".join(json.dumps(e) + "\n" for e in clean_failover()) + '{"op": "node_le'
        code, out, _ = self.main(self.write(body))
        self.assertEqual(code, 0, out)
        self.assertIn("cut short", out)

    def test_a_bad_line_in_the_middle_is_bad_input(self):
        body = '{"op": "node_le\n' + "".join(json.dumps(e) + "\n" for e in clean_failover())
        code, _, err = self.main(self.write(body))
        self.assertEqual(code, 2)
        self.assertIn("not JSON", err)

    def test_an_unknown_op_is_bad_input(self):
        code, _, err = self.main(self.write(json.dumps({"op": "produce", "invoke_ms": 0, "complete_ms": 1, "ok": True}) + "\n"))
        self.assertEqual(code, 2)
        self.assertIn("not a control observation", err)

    def test_a_violation_exits_1_with_the_summary(self):
        events = [status(0, 1, 3, "Leader"), status(0, 2, 3, "Leader")]
        code, out, _ = self.main(self.write("".join(json.dumps(e) + "\n" for e in events)))
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["violations"]["meta_leader"]["count"], 1)


class Recorder(unittest.TestCase):
    """record_control.py's parsing, without a cluster."""

    def test_node_metrics_are_parsed_into_a_claim(self):
        import http.server
        import threading

        import record_control

        body = (
            "# HELP vtop_broker_lease_active x\n"
            'vtop_broker_lease_active{topic="t",range="r"} 1\n'
            'vtop_broker_meta_fencing_epoch{topic="t",range="r"} 7\n'
            'vtop_broker_held_fencing_epoch{topic="t",range="r"} 7\n'
            'vtop_broker_candidate_leading{topic="t",range="r"} 1\n'
        ).encode()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                self.send_response(200)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        event = record_control.node_lease("c1", f"127.0.0.1:{server.server_port}", A)
        self.assertTrue(event["ok"])
        self.assertEqual((event["active"], event["epoch"], event["held"], event["leading"]), (True, 7, 7, 1))
        dead = record_control.node_lease("c2", "127.0.0.1:1", B)
        self.assertFalse(dead["ok"])

    def test_a_response_cut_short_is_a_failed_observation(self):
        # A node killed while it answers: the body ends before its
        # Content-Length (review of #904). The observation fails; it does
        # not raise and end the polling thread.
        import http.server
        import threading

        import record_control

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                self.send_response(200)
                self.send_header("Content-Length", "1000")
                self.end_headers()
                self.wfile.write(b"vtop_broker_lease_active 1\n")

            def log_message(self, *_):
                pass

        server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        event = record_control.node_lease("c1", f"127.0.0.1:{server.server_port}", A)
        self.assertFalse(event["ok"])

    def test_a_raising_observation_does_not_end_the_thread(self):
        import record_control

        calls = {"n": 0}
        written = []

        class Sink:
            def write(self, event):
                written.append(event)
                if len(written) >= 2:
                    record_control.STOP.set()

        def observe():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom")
            return {"op": "node_lease", "n": calls["n"]}

        record_control.STOP.clear()
        self.addCleanup(record_control.STOP.clear)
        with redirect_stderr(io.StringIO()):
            record_control.poll(1, observe, Sink())
        self.assertEqual([e["n"] for e in written], [2, 3])


if __name__ == "__main__":
    unittest.main()
