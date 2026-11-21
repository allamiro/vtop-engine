"""Unit tests for the cluster soak's profile, recorder and evaluator (#727).

The evaluator is shown a fabricated healthy run, which must pass every
criterion, and then that run with one thing wrong at a time, each of which
must fail the criterion that owns it and nothing else.
"""

import contextlib
import copy
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import cluster_soak as cs  # noqa: E402
import cluster_soak_eval as ce  # noqa: E402

NODES = ("meta-1", "meta-2", "meta-3", "data-1", "data-2", "data-3")


def healthy_run(profile):
    """What a clean run of `profile` records."""
    duration = profile["duration_s"]
    interval = profile["sample_interval_s"]
    samples = []
    t = 0.0
    while t <= duration:
        nodes = {}
        for name in NODES:
            kind = name.split("-")[0]
            wobble = (int(t) * 7919 % 13) * 4096
            entry = {
                "kind": kind,
                "lifetime": 1,
                "up": True,
                "rss_bytes": 50_000_000 + wobble,
                "open_fds": 40 + int(t) % 3,
                "metrics": {},
            }
            if kind == "meta":
                entry["metrics"] = {"vtop_meta_raft_term": 3.0}
                if name == "meta-1":
                    entry["metrics"]["raft_leader"] = True
            else:
                entry["data_dir_bytes"] = 2_000_000 + wobble
                entry["metrics"] = {
                    "vtop_broker_candidate_leading": 1.0 if name == "data-1" else 0.0,
                    "vtop_broker_held_fencing_epoch": 1.0,
                }
            nodes[name] = entry
        samples.append({"elapsed_s": t, "nodes": nodes})
        t += interval
    events = [{"elapsed_s": 0.0, "kind": "run_start"}, {"elapsed_s": 1.0, "kind": "load_start"}]
    for fault in cs.build_plan(profile):
        events += [
            {"elapsed_s": fault.at_s, "kind": "fault_start", "id": fault.id, "fault": fault.kind},
            {"elapsed_s": fault.at_s + fault.hold_s, "kind": "fault_end", "id": fault.id, "ok": True},
            {"elapsed_s": fault.at_s + fault.hold_s + 3, "kind": "recovered", "id": fault.id, "ok": True,
             "recovery_s": 3},
        ]
    t = 0.0
    while t <= duration:
        events.append({"elapsed_s": t + 2, "kind": "read", "ok": True})
        t += profile["read_interval_s"]
    events += [
        {"elapsed_s": duration - 5.0, "kind": "producer_stopped"},
        {"elapsed_s": duration - 4.0, "kind": "final_read", "ok": True},
        {"elapsed_s": float(duration), "kind": "run_end"},
    ]
    leaders = [{"node": "data-1", "lifetime": 1, "epoch": 1, "log": "data-candidate-1-1.log", "line": 9}]
    finals = {"replicas": {f"data-{n}": {"segments": 5, "active_tails": 1, "failed": []} for n in (1, 2, 3)}}
    history = {
        "valid": True,
        "acknowledged_records": profile["rate_records_per_s"] * duration,
        "reads": 24,
        "batches": {"ok": 100},
        "violations": {},
    }
    return {"samples": samples, "events": events, "leaders": leaders, "finals": finals, "history": history}


def verdict(profile, run):
    return ce.evaluate(profile, run["samples"], run["events"], run["leaders"], run["finals"], run["history"])


def failed(result):
    return {c["id"] for c in result["criteria"] if not c["passed"]}


class Profiles(unittest.TestCase):
    def test_the_shipped_profiles_validate(self):
        for name in ("7d", "smoke"):
            profile = cs.load(name)
            self.assertEqual(profile["name"], name)

    def test_the_7d_profile_runs_every_fault_kind_hourly_for_a_week(self):
        profile = cs.load("7d")
        plan = cs.build_plan(profile)
        self.assertEqual(profile["duration_s"], 7 * 24 * 3600)
        self.assertEqual(profile["fault_interval_s"], 3600)
        self.assertEqual({f.kind for f in plan}, set(cs.FAULT_KINDS))
        self.assertGreaterEqual(len(plan), 160)

    def test_retention_must_outlast_several_reads(self):
        profile = copy.deepcopy(cs.load("smoke"))
        profile["read_interval_s"] = 600
        with self.assertRaisesRegex(cs.ProfileError, "read intervals"):
            cs.validate(profile)

    def test_an_unknown_fault_is_refused(self):
        profile = copy.deepcopy(cs.load("smoke"))
        profile["fault_cycle"].append("meteor")
        with self.assertRaisesRegex(cs.ProfileError, "meteor"):
            cs.validate(profile)


class Recorder(unittest.TestCase):
    def test_metrics_are_summed_over_labels_and_the_raft_role_is_read(self):
        text = "\n".join(
            [
                "# HELP vtop_meta_raft_term Current Raft term",
                'vtop_meta_raft_state{state="leader"} 1',
                'vtop_meta_raft_state{state="follower"} 0',
                "vtop_meta_raft_term 7",
                'vtop_broker_log_start_offset{topic="t",range="r"} 40',
                'vtop_broker_log_start_offset{topic="t",range="s"} 2',
            ]
        )
        meta = cs.parse_metrics(text, cs.META_METRICS)
        self.assertEqual(meta, {"raft_leader": True, "vtop_meta_raft_term": 7.0})
        self.assertEqual(cs.parse_metrics(text, cs.DATA_METRICS)["vtop_broker_log_start_offset"], 42.0)

    def test_leaderships_are_read_from_every_node_lifetime_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            logs = Path(tmp)
            (logs / "data-candidate-2-1.log").write_text(
                "x\nINFO data_node_role_changed role=leader range=r epoch=4 from=follower\n"
            )
            (logs / "data-candidate-2-3.log").write_text("INFO data_node_role_changed role=follower epoch=5\n")
            (logs / "meta-1-1.log").write_text("data_node_role_changed role=leader epoch=9\n")
            rows = cs.leaders(logs)
        self.assertEqual(rows, [{"node": "data-2", "lifetime": 1, "epoch": 4,
                                 "log": "data-candidate-2-1.log", "line": 2}])

    def test_events_carry_the_time_since_the_run_started(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = Path(tmp)
            (run / "origin").write_text(repr(cs.time.monotonic() - 12.5))
            cs.main(["event", str(run), "fault_end", "id=3", "ok=true", "detail=cut off"])
            row = json.loads((run / "events.jsonl").read_text())
        self.assertEqual((row["kind"], row["id"], row["ok"], row["detail"]), ("fault_end", 3, True, "cut off"))
        self.assertGreaterEqual(row["elapsed_s"], 12.5)


class Evaluator(unittest.TestCase):
    def setUp(self):
        self.profile = cs.load("smoke")
        self.run = healthy_run(self.profile)

    def test_a_healthy_run_passes_every_criterion(self):
        result = verdict(self.profile, self.run)
        self.assertTrue(result["passed"], [c for c in result["criteria"] if not c["passed"]])

    def test_metrics_that_stop_answering_fail_the_sampling(self):
        # Review of #881: processes measured, /metrics silent after the start.
        for sample in self.run["samples"][1:]:
            for node in sample["nodes"].values():
                node.pop("metrics", None)
        self.assertIn("samples", failed(verdict(self.profile, self.run)))

    def test_two_data_nodes_leading_one_epoch_fails(self):
        self.run["leaders"].append({"node": "data-3", "lifetime": 2, "epoch": 1, "log": "x", "line": 1})
        self.assertEqual(failed(verdict(self.profile, self.run)), {"one_range_leader_per_epoch"})

    def test_two_data_nodes_sampled_leading_one_epoch_fails(self):
        self.run["samples"][40]["nodes"]["data-2"]["metrics"]["vtop_broker_candidate_leading"] = 1.0
        self.assertEqual(failed(verdict(self.profile, self.run)), {"one_range_leader_per_epoch"})

    def test_two_metadata_leaders_in_one_term_fails(self):
        self.run["samples"][10]["nodes"]["meta-2"]["metrics"]["raft_leader"] = True
        self.assertEqual(failed(verdict(self.profile, self.run)), {"one_meta_leader_per_term"})

    def test_a_lost_record_fails_the_history(self):
        self.run["history"]["valid"] = False
        self.run["history"]["violations"] = {"lost": {"count": 1, "first": ["acknowledged record 7 ..."]}}
        result = verdict(self.profile, self.run)
        self.assertEqual(failed(result), {"history"})
        history = next(c for c in result["criteria"] if c["id"] == "history")
        self.assertIn("lost: acknowledged record 7", history["detail"])

    def test_no_history_check_fails(self):
        self.run["history"] = None
        self.assertEqual(failed(verdict(self.profile, self.run)), {"history", "progress"})

    def test_a_memory_leak_fails_the_trend(self):
        for sample in self.run["samples"]:
            sample["nodes"]["data-2"]["rss_bytes"] = 50_000_000 + int(sample["elapsed_s"] * 1_000_000)
        self.assertEqual(failed(verdict(self.profile, self.run)), {"trend_rss_bytes"})

    def test_a_restart_is_not_a_trend(self):
        # Memory drops back at each restart; within each lifetime it is flat.
        for sample in self.run["samples"]:
            life = 1 + int(sample["elapsed_s"] // 180)
            node = sample["nodes"]["data-2"]
            node["lifetime"] = life
            node["rss_bytes"] = 20_000_000 * life
        self.assertTrue(verdict(self.profile, self.run)["passed"])

    def test_a_data_directory_over_its_ceiling_fails(self):
        self.run["samples"][30]["nodes"]["data-3"]["data_dir_bytes"] = 9_000_000
        self.assertEqual(failed(verdict(self.profile, self.run)), {"ceiling_data_dir_bytes"})

    def test_a_segment_that_does_not_verify_fails(self):
        self.run["finals"]["replicas"]["data-2"]["failed"] = ["00000000000000000040.segment"]
        self.assertEqual(failed(verdict(self.profile, self.run)), {"segment_verify"})

    def test_a_replica_with_nothing_verified_fails(self):
        del self.run["finals"]["replicas"]["data-3"]
        self.assertEqual(failed(verdict(self.profile, self.run)), {"segment_verify"})

    def test_an_unplanned_exit_fails(self):
        self.run["events"].append({"elapsed_s": 300.0, "kind": "node_exit", "node": "meta-2", "planned": False})
        self.assertEqual(failed(verdict(self.profile, self.run)), {"no_unplanned_exit"})

    def test_a_fault_that_did_not_land_fails(self):
        for event in self.run["events"]:
            if event["kind"] == "fault_end" and event["id"] == 4:
                event["ok"] = False
                event["detail"] = "disk filled with 9000000 bytes left"
        self.assertEqual(failed(verdict(self.profile, self.run)), {"faults_executed"})

    def test_a_slow_recovery_fails(self):
        for event in self.run["events"]:
            if event["kind"] == "recovered" and event["id"] == 2:
                event["recovery_s"] = 500
        self.assertEqual(failed(verdict(self.profile, self.run)), {"recovery_time"})

    def test_reads_that_stopped_fail(self):
        self.run["events"] = [
            e for e in self.run["events"] if not (e["kind"] == "read" and e["elapsed_s"] > 200)
        ]
        self.assertEqual(failed(verdict(self.profile, self.run)), {"reads"})

    def test_a_read_stopped_by_what_the_leader_served_fails_among_good_reads(self):
        # Review of #881: verify stops before recording such a read, so only
        # the driver's mark carries it.
        self.run["events"].append({"elapsed_s": 301.0, "kind": "read", "ok": False, "integrity": True,
                                   "node": "data-2", "detail": "record 812 was exposed at or above "
                                   "committed high watermark 800"})
        self.assertEqual(failed(verdict(self.profile, self.run)), {"read_integrity"})

    def test_a_read_that_failed_for_want_of_a_leader_is_not_an_integrity_failure(self):
        self.run["events"].append({"elapsed_s": 301.0, "kind": "read", "ok": False, "integrity": False,
                                   "detail": "fetch rejected: Fenced"})
        self.assertTrue(verdict(self.profile, self.run)["passed"])

    def test_a_stalled_producer_fails(self):
        self.run["history"]["acknowledged_records"] = 10
        self.assertEqual(failed(verdict(self.profile, self.run)), {"progress"})

    def test_a_run_that_ended_early_fails(self):
        self.run["events"] = [e for e in self.run["events"] if e["kind"] != "run_end"]
        self.assertIn("run_completed", failed(verdict(self.profile, self.run)))

    def test_a_run_directory_is_judged_from_its_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            for name, rows in (("samples.jsonl", self.run["samples"]), ("events.jsonl", self.run["events"]),
                               ("leaders.jsonl", self.run["leaders"])):
                (run_dir / name).write_text("".join(json.dumps(r) + "\n" for r in rows))
            (run_dir / "finals.json").write_text(json.dumps(self.run["finals"]))
            (run_dir / "history-check.json").write_text(json.dumps(self.run["history"]))
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(ce.main(["--profile", "smoke", "--run-dir", str(run_dir)]), 0)
            saved = json.loads((run_dir / "verdict.json").read_text())
            self.assertTrue(saved["passed"])
            self.assertEqual(set(saved["inputs"]), {"samples.jsonl", "events.jsonl", "leaders.jsonl",
                                                    "finals.json", "history-check.json"})
            (run_dir / "events.jsonl").write_text("not json\n")
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(ce.main(["--profile", "smoke", "--run-dir", str(run_dir)]), 2)


if __name__ == "__main__":
    unittest.main()
