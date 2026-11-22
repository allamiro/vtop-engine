"""The archive comparison (#746): the seeded producer, the checker's counts on a fake bucket, the offset sampler, the
cell and quiescence rules, the configuration rules, and the shared tests of #744 section 8 for the `archive` kind.
No Docker: the drivers take their actions as callables or are stubbed."""
from __future__ import annotations

import gzip
import io
import json
import os
import re

import pytest

from archive_compare import check_bucket, commits, faults, fixture, producer, run
from archive_compare.kind import KIND, TOOLS
from compare_common import checker, kills, lab, registry, runner, schema
from compare_common import plan as plans
from compare_common import tap as tap_module

HERE = os.path.dirname(os.path.abspath(__file__))
ARCHIVE = os.path.join(os.path.dirname(HERE), "archive_compare")
TAG = "abcd1234"
WORKFLOW_PLANS = ("smoke", "hosted-store-kills", "hosted-time-kills-fault")


def _read(rel):
    with open(os.path.join(lab.REPO, rel), encoding="utf-8") as handle:
        return handle.read()


# ------------------------------------------------------------ the producer


@pytest.mark.parametrize("key, signed", [
    (b"21", -973932308), (b"foobar", -790332482), (b"a-little-bit-long-string", -985981536),
    (b"a-little-bit-longer-string", -1486304829), (b"abc", 479470107),
    (b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971)])
def test_murmur2_is_kafkas_hash_so_a_records_partition_is_known_before_it_is_sent(key, signed):
    # The vectors are Kafka's own (clients' UtilsTest): the default partitioner hashes the key with this.
    assert producer.murmur2(key) - (2**32 if producer.murmur2(key) >= 2**31 else 0) == signed


def test_a_record_is_a_pure_function_of_seed_tag_sequence_and_size_and_exactly_that_size():
    for size in (100, 1024):
        values = [producer.record(746, TAG, seq, size) for seq in range(300)]
        assert values == [producer.record(746, TAG, seq, size) for seq in range(300)]
        assert values != [producer.record(747, TAG, seq, size) for seq in range(300)]
        assert all(len(value) == size for value in values)
    one = json.loads(producer.record(746, TAG, 7, 1024))
    assert one["id"] == checker.make_id(TAG, 7) == producer.make_id(TAG, 7)
    assert one["s"] == 7 and one["p"] == producer.partition_of(7)
    with pytest.raises(ValueError, match="cannot hold"):
        producer.record(746, TAG, 7, 40)


def test_every_record_carries_exactly_one_id_token_and_its_sequence_is_unique():
    found = [checker.ID_TOKEN.findall(producer.record(746, TAG, seq, 100)) for seq in range(2000)]
    assert all(len(f) == 1 for f in found)
    assert [int(f[0][1]) for f in found] == list(range(2000))


def test_a_line_is_key_separator_value_and_the_value_never_holds_the_separator():
    data = producer.line(746, TAG, 5, 100)
    key, _, value = data.partition(producer.KEY_SEPARATOR.encode())
    assert key == producer.key_of(5).encode() and data.endswith(b"\n")
    assert producer.KEY_SEPARATOR.encode() not in value


def test_partitions_are_even_and_the_layout_gives_each_records_offset():
    counts = producer.counts(0, 3000)
    assert sum(counts) == 3000 and max(counts) - min(counts) < 150
    layout = producer.layout(3000)
    assert [sum(1 for part, _ in layout if part == p) for p in range(3)] == counts
    for part in range(3):
        offsets = [off for p, off in layout if p == part]
        assert offsets == list(range(len(offsets)))  # in order, from 0


def test_backlog_returns_the_source_bytes_it_wrote():
    out = io.BytesIO()
    assert producer.backlog(out, 746, TAG, 0, 50, 100) == 50 * 100
    assert out.getvalue().count(b"\n") == 50


def test_live_writes_open_loop_at_the_rate_and_logs_every_record_with_its_offset():
    clock = {"now": 1000.0}
    log = io.StringIO()
    out = io.BytesIO()

    def sleep(seconds):
        clock["now"] += seconds

    nxt = producer.live(out, log, seed=746, tag=TAG, start=0, rate=500.0, seconds=2.0, size=100,
                        clock=lambda: clock["now"], sleep=sleep)
    rows = [line.split(",") for line in log.getvalue().splitlines()]
    assert nxt == len(rows) == out.getvalue().count(b"\n")
    assert 990 <= nxt <= 1000  # 500 per second for two seconds, a burst per 10 ms
    assert [int(r[0]) for r in rows] == list(range(nxt))
    assert [(int(r[1]), int(r[2])) for r in rows] == producer.layout(nxt)
    assert all(1000.0 <= float(r[3]) <= 1002.0 for r in rows)


# ------------------------------------------- 2, 3 the checker on a fake bucket


def _bucket(per_object=40, count=200):
    """A fake S3: an in-memory map of key to bytes, holding records 0..count-1 across gzip objects."""
    objects = {}
    for start in range(0, count, per_object):
        stop = min(start + per_object, count)
        body = b"".join(producer.record(746, TAG, seq, 100) + b"\n" for seq in range(start, stop))
        objects[f"vtop/source=bench/format=jsonl/{start:06d}.jsonl.gz"] = gzip.compress(body, mtime=0)
    return objects


def test_checker_counts_a_gap_a_duplicate_and_a_torn_gzip_member_exactly():
    objects = _bucket()
    clean = check_bucket.check(checker.Snapshot(dict(objects), {}, []), 200, TAG)
    assert (clean.gaps, clean.duplicates, clean.torn_objects, clean.orphan_objects, clean.unexpected) == (0,) * 5
    broken = dict(objects)
    # a gap: record 13 is in no object
    key = sorted(broken)[0]
    lines = gzip.decompress(broken[key]).split(b"\n")
    broken[key] = gzip.compress(b"\n".join(line for line in lines if checker.make_id(TAG, 13).encode() not in line),
                                mtime=0)
    # a duplicate: record 90 appears in a second object
    broken["vector/dup.log.gz"] = gzip.compress(producer.record(746, TAG, 90, 100) + b"\n", mtime=0)
    # a torn gzip member: the object is cut mid-stream
    whole = gzip.compress(b"".join(producer.record(746, TAG, seq, 100) + b"\n" for seq in range(0, 40)), mtime=0)
    broken["vtop/torn.jsonl.gz"] = whole[: len(whole) // 2]
    result = check_bucket.check(checker.Snapshot(broken, {}, []), 200, TAG)
    assert (result.gaps, result.duplicates, result.torn_objects) == (1, 1, 1)
    assert result.gap_sample == [13] and result.torn_keys == ["vtop/torn.jsonl.gz"]


def test_checker_treats_vtop_manifest_and_receipt_objects_as_metadata():
    objects = _bucket()
    objects["vtop/source=bench/format=jsonl/b1.manifest.json"] = json.dumps({"batch": "b1", "records": 40}).encode()
    objects["vtop/source=bench/format=jsonl/b1.receipt.json"] = json.dumps({"verified": True}).encode()
    result = check_bucket.check(checker.Snapshot(objects, {}, []), 200, TAG)
    assert result.metadata_objects == 2 and result.data_objects == 5
    assert (result.gaps, result.duplicates, result.torn_objects, result.orphan_objects) == (0, 0, 0, 0)
    assert check_bucket.is_metadata("vtop/x/b1.manifest.json") and not check_bucket.is_metadata("vtop/x/o.jsonl.gz")


def test_connects_temporary_keys_and_incomplete_uploads_are_orphans_listed_by_name():
    objects = _bucket()
    objects[".temp-upload/bench/0/6f1c/connect/bench/0/000000000039.json"] = b"{}"
    objects[".indexes/bench/0/lock"] = b"x"
    result = check_bucket.check(checker.Snapshot(objects, {}, ["vtop/source=bench/big.jsonl.gz"]), 200, TAG)
    assert result.orphan_objects == 3
    assert sorted(result.orphan_keys) == [".indexes/bench/0/lock",
                                          ".temp-upload/bench/0/6f1c/connect/bench/0/000000000039.json",
                                          "incomplete-upload:vtop/source=bench/big.jsonl.gz"]
    assert result.data_objects == 5 and result.gaps == 0  # orphans hold no record, and are not counted as data


def test_a_tool_that_rewrites_the_value_is_read_back_by_content_and_a_changed_record_is_wrong():
    same = producer.record(746, TAG, 3, 100)
    spaced = json.dumps(json.loads(same), separators=(", ", ": ")).encode()  # same JSON, other bytes
    changed = same.replace(b'"p":', b'"q":')
    objects = {"a.gz": gzip.compress(same + b"\n" + spaced.replace(b"-0000000003", b"-0000000003") + b"\n"),
               "b.gz": gzip.compress(producer.record(746, TAG, 4, 100) + b"\n"
                                     + changed.replace(b"-0000000003", b"-0000000005") + b"\n")}
    exact, same_json, wrong = check_bucket.verify_records(checker.Snapshot(objects, {}, []), 746, TAG, 100)
    assert (exact, same_json, wrong) == (2, 1, [5])


def test_the_checker_cli_prints_counts_and_exits_nonzero_on_a_gap(tmp_path, capsys):
    root = tmp_path / "objects"
    root.mkdir()
    (root / "a.gz").write_bytes(gzip.compress(b"".join(producer.record(746, TAG, s, 100) + b"\n" for s in range(9))))
    assert check_bucket.main(["--objects", str(root), "--tag", TAG, "--expected", "9"]) == 0
    assert json.loads(capsys.readouterr().out)["present_unique"] == 9
    assert check_bucket.main(["--objects", str(root), "--tag", TAG, "--expected", "10"]) == 1
    assert json.loads(capsys.readouterr().out)["gaps"] == 1


# --------------------------------------------------------- the offset sampler


def _commit(group, partition, offset, ms, topic="bench", type_=1):
    return json.dumps({"key": {"type": type_, "data": {"group": group, "topic": topic, "partition": partition}},
                       "value": {"version": 4, "data": {"offset": offset, "leaderEpoch": 0, "metadata": "",
                                                        "commitTimestamp": ms}}})


def test_the_formatters_output_has_no_separator_and_a_partly_written_tail_is_left_for_the_next_read():
    text = (_commit("g", 0, 10, 1_000_000) + _commit("g", 1, 7, 1_500_000) + '{"key":{"type":1,"data":{"gro')
    parsed = commits.parse(text)
    assert parsed == [commits.Commit("g", "bench", 0, 10, 1000.0), commits.Commit("g", "bench", 1, 7, 1500.0)]
    assert commits.parse(_commit("g", 0, 10, 1_000_000, type_=2)) == []  # group metadata, not an offset commit
    assert commits.parse('{"key":{"type":1,"data":{"group":"g","topic":"bench","partition":0}},"value":null}') == []


def test_a_record_is_committed_once_the_group_commits_past_its_offset_and_lag_is_what_is_left():
    stream = commits.parse("".join([
        _commit("tool", 0, 10, 100_000), _commit("tool", 0, 30, 130_000), _commit("tool", 1, 5, 110_000),
        _commit("other", 0, 99, 90_000), _commit("tool", 0, 20, 120_000)]))
    line = commits.Timeline(stream, "tool")
    assert line.committed_at(0, 9) == 100.0       # offset 10 is past the record at offset 9
    assert line.committed_at(0, 10) == 120.0      # not past 10 until the commit of 20
    assert line.committed_at(0, 30) is None       # never committed past
    assert line.committed(0) == 30 and line.committed(0, at=125.0) == 20 and line.committed(2) == 0
    assert line.lag([30, 5, 4]) == 4              # partition 2 has 4 records and no commit
    assert line.lag([30, 9, 0], at=115.0) == 20 + 4   # 30-10 on partition 0, 9-5 on 1, 0 on 2
    assert commits.Timeline(stream, "nobody").lag([3, 3, 3]) == 9
    assert line.last_commit_at() == 130.0


def test_time_to_source_committed_skips_the_warmup_and_a_record_never_committed():
    line = commits.Timeline(commits.parse(_commit("g", 0, 3, 105_000)), "g")
    produced = [(0, 0, 0, 100.0), (1, 0, 1, 104.0), (2, 0, 2, 104.5), (3, 0, 3, 104.9), (4, 1, 0, 104.9)]
    assert commits.source_committed_ms(line, produced) == pytest.approx([5000.0, 1000.0, 500.0])
    assert commits.source_committed_ms(line, produced, after=104.0) == pytest.approx([1000.0, 500.0])


def test_the_producers_log_is_read_back(tmp_path):
    path = tmp_path / "produced.csv"
    path.write_text("0,2,0,100.5\n1,0,0,100.6\nnot a row\n")
    assert commits.read_produced(str(path)) == [(0, 2, 0, 100.5), (1, 0, 0, 100.6)]
    assert commits.read_produced(str(tmp_path / "missing.csv")) == []


# ----------------------------------------------------- quiescence, windows


class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def test_quiescence_needs_zero_lag_an_object_and_a_quiet_period_and_fails_a_tool_that_exited():
    clock = _Clock()
    objects = {"n": 0}
    lag = {"n": 50}

    def listing():
        if clock.now < 20:
            objects["n"] = int(clock.now // 10) + 1
        return [{"size": 10, "key": str(i)} for i in range(objects["n"])]

    def lag_now():
        if clock.now >= 40:
            lag["n"] = 0
        return lag["n"]

    got = run.quiesce(listing, lambda: True, lag_now, quiet=30, timeout=600, clock=clock, sleep=clock.sleep, poll=5)
    assert len(got) == 2 and clock.now == 40  # the last object at 10, so quiet at 40, and the lag is 0 from 40
    clock2 = _Clock()
    with pytest.raises(runner.ToolFailed, match="exited before the run reached quiescence"):
        run.quiesce(lambda: [], lambda: False, lambda: 5, quiet=30, timeout=600, clock=clock2, sleep=clock2.sleep)
    clock3 = _Clock()
    with pytest.raises(runner.ToolFailed, match=r"the group's lag 7 records"):
        run.quiesce(lambda: [{"size": 1}], lambda: True, lambda: 7, quiet=10, timeout=60, clock=clock3,
                    sleep=clock3.sleep)


def test_the_drain_window_ends_at_the_later_of_the_last_object_and_lag_reaching_zero():
    assert run.drain_window(100.0, 130.0, 150.0) == 50.0   # Connect: offsets flush after the objects land
    assert run.drain_window(100.0, 130.0, 120.0) == 30.0   # Vector's disk buffer: committed before delivered
    assert run.drain_window(100.0, 130.0, None) == 30.0
    assert run.drain_window(100.0, 90.0, None) == pytest.approx(1e-3)


def test_the_resource_window_is_the_throughput_window_not_the_quiescence_wait():
    assert run.usage_end(100.0, 40.0, 400.0, 0.5) == 140.0
    assert run.usage_end(100.0, 0.1, 400.0, 0.5) == 101.0
    assert run.usage_end(100.0, 40.0, 120.0, 0.5) == 120.0


def test_the_harness_polling_is_taken_out_of_the_tools_request_counts():
    counts = {"PUT": 40, "GET": 0, "HEAD": 3, "LIST": 61, "POST": 0, "DELETE": 0, "OTHER": 0}
    harness = {"PUT": 0, "GET": 0, "HEAD": 60, "LIST": 60, "POST": 0, "DELETE": 0, "OTHER": 0}
    assert run.without_harness(counts, harness) == {"PUT": 40, "GET": 0, "HEAD": 0, "LIST": 1, "POST": 0,
                                                    "DELETE": 0, "OTHER": 0}


# -------------------------------------------------- 6 the live cell's rate


def _drain_row(env, tmp_path, system, rate, size="1024", rep=1, status="ok", profile=None):
    item = plans.Planned(system, profile or KIND.profiles[system][0], "drain", rep, {"record_size": size}, {},
                         "performance")
    row = runner.base_row(env, item, system_version="x", image_digest="x",
                          config_sha256=env["configs"][system][item.profile]["sha256"])
    row.update({"status": status, "note": "" if status == "ok" else "why", "started_at": "t", "wall_seconds": "1",
                "records_per_sec": rate})
    schema.append_row(os.path.join(str(tmp_path), "archive.csv"), KIND, row)


def test_live_offered_rate_is_a_fraction_of_the_slowest_tools_drain_rate_and_an_unsustained_tool_is_marked(
        tmp_path):
    env = fixture.environment("r")
    for system, rate in (("vtop", "30000"), ("vtop", "34000"), ("vtop", "32000"), ("vector", "90000"),
                         ("connect-lenses", "50000"), ("connect-lenses", "10000")):
        _drain_row(env, tmp_path, system, rate)
    item = plans.Planned("vtop", "strong", "live", 1, {"record_size": 1024}, {}, "performance")
    # the slowest tool's median: connect-lenses (10000, 50000) -> 30000; vtop's median is 32000
    assert run.live_rate(str(tmp_path), item, 0.5) == 15000.0
    assert run.live_rate(str(tmp_path), item._replace(variant={"record_size": 100}), 0.5, fixed=500) == 500.0
    with pytest.raises(runner.CellRefused, match="no drain result for this record size"):
        run.live_rate(str(tmp_path), item._replace(variant={"record_size": 100}), 0.5)
    # a tool that cannot sustain the offer is not sustained, and a tool that lost records did not either
    assert run.live_sustained(15000.0, 15000.0, 0)
    assert not run.live_sustained(15000.0, 9000.0, 0)
    assert not run.live_sustained(15000.0, 15000.0, 4)


def test_a_failed_drain_row_does_not_set_the_live_rate(tmp_path):
    env = fixture.environment("r")
    _drain_row(env, tmp_path, "vtop", "30000")
    _drain_row(env, tmp_path, "vector", "", status="failed")
    item = plans.Planned("vtop", "strong", "live", 1, {"record_size": 1024}, {}, "performance")
    assert run.live_rate(str(tmp_path), item, 0.5) == 15000.0


# ------------------------------------------- 7 a tool that fails at start


class _StubLab(run.Lab):
    """A Lab with Docker answered by the test."""

    def __init__(self, system="vector"):
        self.system, self.service, self.logs = system, TOOLS[system]["service"], "/nonexistent"
        self.calls = []

    def compose(self, *args, **kw):
        self.calls.append(args)

        class Done:
            stdout = "abc123\n"
            stderr = ""

        return Done()


def test_a_tool_that_exits_nonzero_at_start_is_a_refused_row_with_its_stderr_in_the_note(tmp_path, monkeypatch):
    monkeypatch.setattr(run.time, "sleep", lambda s: None)
    monkeypatch.setattr(lab, "container_exit", lambda name: {"running": False, "exit_code": 78, "error": ""})
    monkeypatch.setattr(lab, "logs_tail", lambda name, n=20: "ERROR vector: invalid sink: unknown codec raw_msg")
    tool = _StubLab()
    with pytest.raises(runner.CellRefused, match=r"vector exited 78 at start: ERROR vector: invalid sink"):
        tool.start_tool()
    # Through the runner: a `refused` row carrying the tool's own error, not a harness fault.
    env = fixture.environment("r")
    env["plan"] = {"kind": "archive", "name": "t", "host_class": "laptop", "systems": ["vector"],
                   "profiles": {"vector": ["acks"]},
                   "cells": [{"cell": "smoke", "repetitions": 1, "minutes_per_run": 1,
                              "variants": [{"record_size": 1024, "records": 10}]}]}
    describe = lambda item: {"system_version": "x", "image_digest": "x",  # noqa: E731
                             "config_sha256": env["configs"]["vector"]["acks"]["sha256"]}
    runner.run_plan(str(tmp_path), KIND, env, describe, lambda item: _StubLab().start_tool(), log=lambda m: None)
    rows = schema.read_rows(os.path.join(str(tmp_path), "archive.csv"), KIND)
    assert [r["status"] for r in rows] == ["refused"] and "unknown codec raw_msg" in rows[0]["note"]


def test_a_sigkill_during_the_first_seconds_is_a_kill_the_tap_fired_not_a_failed_start(monkeypatch):
    monkeypatch.setattr(run.time, "sleep", lambda s: None)
    monkeypatch.setattr(lab, "container_exit", lambda name: {"running": False, "exit_code": 137, "error": ""})
    assert isinstance(_StubLab().start_tool(), float)


# --------------------------------------------- 8 profiles refused in cells


def test_vtop_profile_verify_off_is_refused_in_crash_cells(tmp_path):
    for cell in ("crash-K1", "crash-K2", "crash-K3", "verify-fault"):
        assert "deliberate weakening" in KIND.forbidden("vtop", "verify-off", cell)
    assert KIND.forbidden("vtop", "verify-off", "drain") is None
    assert KIND.forbidden("vtop", "strong", "crash-K2") is None
    plan = plans.select(plans.load_plans(os.path.join(ARCHIVE, "plans.json")), "full")
    plan["cells"] = [dict(c) for c in plan["cells"] if c["cell"] != "smoke"]
    for cell in plan["cells"]:
        if cell["cell"] == "crash-K2":
            cell["profiles"] = {"vtop": ["strong", "verify-off"]}
    with pytest.raises(plans.PlanRefused, match="deliberate weakening"):
        plans.check_plan(plan, KIND)
    # and the runner refuses it as a row, before any Docker work, if a plan were edited into one
    driver = run.Runner(None, fixture.environment("r"), str(tmp_path), "")
    item = plans.Planned("vtop", "verify-off", "crash-K2", 1, {"record_size": 1024}, {"records": 10}, "correctness")
    with pytest.raises(runner.CellRefused, match="deliberate weakening"):
        driver.execute(item)


def test_the_tap_is_refused_in_a_drain_or_live_cell_and_only_k1_k2_and_the_fault_cell_use_it(tmp_path):
    for cell in ("drain", "live", "smoke"):
        with pytest.raises(tap_module.TapRefused, match="only in crash and fault cells"):
            run.Lab("p", "vector", "/dev/null", cell=cell, vtop_image="", tap=True, logs=str(tmp_path / cell),
                    cpus="1", memory="2g", partitions=3)
    assert [run.uses_tap(c) for c in KIND.cells] == [False, False, False, True, True, False, True]
    assert not any(run.uses_tap(c) for c in ("smoke", "drain", "live", "crash-K3"))


# -------------------------------------------------- 9 verify-fault


def test_verify_fault_counts_a_corrupt_object_committed_past_even_when_the_tool_never_read_it():
    keys = [f"vector/2026/11/22/obj-{i}.log.gz" for i in range(400)]
    selected = faults.selected_keys(keys, "bench", 746, 10)
    assert 20 <= len(selected) <= 70 and selected == faults.selected_keys(reversed(keys), "bench", 746, 10)
    # A tool that never reads back: nothing flipped, nothing reacted, everything committed.
    never = faults.classify(selected, set(), set(), set(keys))
    assert never == faults.Outcome(len(selected), 0, 0, len(selected))
    # A tool that read the corrupt objects back, noticed and wrote them again: not committed past.
    noticed = faults.classify(selected, set(selected), set(selected), set(keys))
    assert noticed == faults.Outcome(len(selected), len(selected), len(selected), 0)
    # Read the corrupt bytes and committed anyway: a corrupt read-back committed past, not noticed.
    ignored = faults.classify(selected, set(selected), set(), set(keys))
    assert ignored.corrupted_noticed == 0 and ignored.corrupted_committed_past == len(selected)
    # An object the tool had not committed yet is not committed past.
    assert faults.classify(selected, set(), set(), set()).corrupted_committed_past == 0


def test_the_taps_paths_and_trace_map_back_to_bucket_keys():
    assert faults.flipped_keys(["/bench/vtop/a.gz", "/other/x"], "bench") == {"vtop/a.gz"}
    trace = [{"t_start": "1.0", "method": "GET", "path": "/bench/vtop/a.gz", "flipped": "1"},
             {"t_start": "2.0", "method": "PUT", "path": "/bench/vtop/a.gz"},
             {"t_start": "0.5", "method": "PUT", "path": "/bench/vtop/b.gz"},
             {"t_start": "3.0", "method": "GET", "path": "/bench/vtop/b.gz", "flipped": "1"}]
    assert faults.put_after_flip(trace, "bench") == {"vtop/a.gz"}  # b's PUT came before its flipped read
    selected = faults.selected_keys(["a", "b", "c"], "bench", 3, 1)
    assert selected == {"a", "b", "c"}  # one in one


# ----------------------------------------------------------- kill points


def test_every_tool_meets_the_same_kill_schedule_for_a_cell_and_repetition():
    for cell in ("crash-K1", "crash-K2", "crash-K3"):
        runs = [plans.Planned(system, profile, cell, 2, {"record_size": 1024},
                              {"kill_seed": 746, "kills_per_run": 3}, "correctness")
                for system in KIND.profiles for profile in KIND.profiles[system]
                if not KIND.forbidden(system, profile, cell)]
        schedules = {tuple(run.kill_points(item)) for item in runs}
        assert len(schedules) == 1 and len(next(iter(schedules))) == 3
    assert run.kill_points(plans.Planned("vtop", "strong", "drain", 1, {"record_size": 1024}, {}, "performance")) == []


# ------------------------------------ configuration rules and the lab files


def test_tools_json_is_the_one_list_of_tools_and_every_profile_has_a_committed_config():
    assert {s: tuple(v["profiles"]) for s, v in TOOLS.items()} == KIND.profiles == {
        "connect-lenses": ("exactly-once",), "vtop": ("strong", "verify-off"), "vector": ("acks", "disk-buffer")}
    for spec in TOOLS.values():
        compose_service = spec["service"]
        assert re.search(rf"(?m)^  {re.escape(compose_service)}:\s*$", _read("benchmarks/archive_compare/compose.yml"))
        for rel in list(spec["profiles"].values()) + list((spec.get("extra_configs") or {}).values()):
            assert os.path.isfile(os.path.join(lab.REPO, rel)), rel
        assert spec["telemetry"] and spec["group"]


def test_a_profile_added_to_tools_json_is_run_without_a_change_to_run_py(tmp_path):
    path = tmp_path / "tools.json"
    data = json.load(open(os.path.join(ARCHIVE, "tools.json"), encoding="utf-8"))  # noqa: SIM115
    data["systems"]["vector"]["profiles"]["acks-small-batch"] = data["systems"]["vector"]["profiles"]["acks"]
    path.write_text(json.dumps(data))
    from archive_compare import kind as kind_module

    assert tuple(kind_module.load_tools(str(path))["vector"]["profiles"]) == ("acks", "disk-buffer", "acks-small-batch")


def test_every_tool_config_states_its_telemetry_setting_and_turns_it_off():
    for profiles in run.CONFIGS.values():
        for rel in profiles.values():
            assert re.search(r"(?m)^#\s*telemetry:", _read(rel)), f"{rel} does not state its telemetry setting"
    for rel in run.CONFIGS["vector"].values():
        assert re.search(r"(?m)^api:\s*\n\s+enabled:\s*false", _read(rel))
    assert 'OTEL_SDK_DISABLED: "true"' in _read("benchmarks/archive_compare/compose.yml")
    assert re.search(r"(?m)^#\s*telemetry:", _read(TOOLS["connect-lenses"]["profiles"]["exactly-once"]))


def test_compose_limits_are_identical_within_a_limit_class():
    limits = lab.compose_limits(_read("benchmarks/archive_compare/compose.yml"))
    tools = {TOOLS[s]["service"]: limits.get(TOOLS[s]["service"]) for s in TOOLS}
    assert all(tools.values()), f"every tool declares limits: {tools}"
    assert len({tuple(sorted(v.items())) for v in tools.values()}) == 1, tools
    one = next(iter(tools.values()))
    assert set(one) == {"cpus", "mem_limit", "memswap_limit"} and one["mem_limit"] == one["memswap_limit"]
    assert "${TOOL_CPUS:-2}" == one["cpus"] and "${TOOL_MEMORY:-2g}" == one["mem_limit"]  # the class of #746
    assert "-Xmx1g" in _read("benchmarks/archive_compare/compose.yml")  # Connect's JVM heap, recorded in the CSV


def test_pins_match_products_toml():
    reg = registry.load()
    text = _read("benchmarks/archive_compare/compose.yml")
    assert f"image: {registry.product(reg, 'vector')['image']}\n" in text
    kafka = registry.product(reg, "kafka")["image"]
    assert run.KAFKA_IMAGE == kafka and kafka in text
    assert lab.MINIO_IMAGE in text and lab.HARNESS_IMAGE in text
    for image in re.findall(r"(?m)^\s+image:\s*(\S+)", text):
        assert "@sha256:" in image or image.startswith(
            ("${VTOP_IMAGE", "${KAFKA_IMAGE", "vtop-minio:", "${HARNESS_IMAGE", "vtop-connect-lenses:")), image
    # The Connect runtime is the source's image, and the connector is the registry's release, by hash.
    docker = _read(TOOLS["connect-lenses"]["dockerfile"])
    lenses = registry.product(reg, "connect-lenses")
    assert f"FROM {lenses['image']}" in docker
    assert all("@sha256:" in line for line in docker.splitlines() if line.startswith("FROM "))  # every base by digest
    assert f"--checksum=sha256:{lenses['archive_sha256']}" in docker and lenses["archive_url"] in docker
    assert TOOLS["connect-lenses"]["image_tag"] == f"vtop-connect-lenses:{lenses['version']}"


def test_the_three_tools_are_given_the_same_object_normalisation():
    vtop = _read(TOOLS["vtop"]["profiles"]["strong"])
    connect = _read(TOOLS["connect-lenses"]["profiles"]["exactly-once"])
    for rel in TOOLS["vector"]["profiles"].values():
        vector = _read(rel)
        assert "max_bytes: 33554432" in vector and "timeout_secs: 60" in vector and "compression: gzip" in vector
        assert "codec: raw_message" in vector and "method: newline_delimited" in vector
        assert "concurrency: adaptive" in vector and "group_id: vector-bench" in vector
    assert "max_bytes: 33554432" in vtop and "max_batch_age_seconds: 60" in vtop and "max_concurrent_batches: 8" in vtop
    assert re.search(r"type: gzip\s*\n\s*level: 6", vtop)
    assert "'flush.size'=33554432,'flush.interval'=60" in connect and "compression.codec=GZIP" in connect
    assert "tasks.max=3" in connect and "STOREAS `JSON`" in connect


def test_each_tools_commit_rule_setting_is_the_one_the_profile_names():
    vtop = _read(TOOLS["vtop"]["profiles"]["strong"])
    off = _read(TOOLS["vtop"]["profiles"]["verify-off"])
    assert "enable_auto_commit: false" in vtop and "require_strong_verification: true" in vtop
    assert "consumer_group: vtop-bench" in vtop and 'topic_include_regex: "^bench$"' in vtop
    assert "require_strong_verification: false" in off and "algorithm: none" in off
    assert "enable_auto_commit: false" in off
    acks = _read(TOOLS["vector"]["profiles"]["acks"])
    disk = _read(TOOLS["vector"]["profiles"]["disk-buffer"])
    assert re.search(r"acknowledgements:\s*\n\s+enabled: true", acks) and "when_full: block" in acks
    assert re.search(r"buffer:\s*\n\s+type: memory", acks) and re.search(r"buffer:\s*\n\s+type: disk", disk)
    connect = _read(TOOLS["connect-lenses"]["profiles"]["exactly-once"])
    assert "connect.s3.exactly.once.enable=true" in connect
    worker = _read(TOOLS["connect-lenses"]["extra_configs"]["worker"])
    assert "schemas.enable=false" in worker and "plugin.path=/opt/connectors" in worker


def test_the_licence_recorded_for_vtop_is_mit():
    reg = registry.load()
    assert registry.product(reg, "vtop")["licence"] == "MIT"
    for rel in ("benchmarks/archive_compare/README.md",):
        assert "Apache-2.0" not in re.findall(r"(?m)^.*VTOP.*Apache-2\.0.*$", _read(rel)), rel
    assert re.search(r"VTOP[^\n]*\bMIT\b", _read("benchmarks/archive_compare/README.md"))


def test_the_archive_package_names_no_tooling_that_wrote_it():
    for directory, _dirs, files in os.walk(ARCHIVE):
        if "results" in directory:
            continue
        for name in files:
            if name.endswith((".py", ".md", ".yml", ".yaml", ".json", ".properties", "Dockerfile")):
                with open(os.path.join(directory, name), encoding="utf-8") as handle:
                    text = handle.read()
                assert not re.search(r"(?i)claude|codex|co-authored|generated with", text), name
    assert not re.search(r"(?i)claude|codex|co-authored|generated with", _read(".github/workflows/archive-compare.yml"))


# ---------------------------------------------- plans and durations


@pytest.mark.parametrize("name", ["smoke", "hosted", "hosted-store-kills", "hosted-time-kills-fault", "full"])
def test_every_shipped_plan_is_one_the_kind_can_run_and_names_committed_configs(name):
    plan = plans.select(plans.load_plans(os.path.join(ARCHIVE, "plans.json")), name)
    plans.check_plan(plan, KIND)
    run.check_hosted(plan)
    for item in plans.schedule(plan, KIND):
        assert os.path.isfile(run.require_config(item.system, item.profile))


def test_hosted_plans_hold_correctness_cells_only_and_a_hosted_plan_with_a_performance_cell_is_refused():
    for name in ("smoke", "hosted", "hosted-store-kills", "hosted-time-kills-fault"):
        plan = plans.select(plans.load_plans(os.path.join(ARCHIVE, "plans.json")), name)
        assert {KIND.cells[c["cell"]] for c in plan["cells"]} == {"correctness"}
    plan = plans.select(plans.load_plans(os.path.join(ARCHIVE, "plans.json")), "full")
    plan["host_class"] = "hosted"
    with pytest.raises(plans.PlanRefused, match="drain, live on a hosted host class"):
        run.check_hosted(plan)


def test_durations_are_plan_settings_a_smoke_is_minutes_and_the_full_live_cell_names_its_seconds():
    loaded = plans.load_plans(os.path.join(ARCHIVE, "plans.json"))
    smoke, full = (plans.select(loaded, n) for n in ("smoke", "full"))
    assert plans.projected_hours(smoke, KIND) < 1.0
    # every plan the workflow offers fits one hosted job (6 hours is GitHub's limit) with the image builds
    for name in WORKFLOW_PLANS:
        assert plans.projected_hours(plans.select(loaded, name), KIND) < 4.5, name
    live = next(c for c in full["cells"] if c["cell"] == "live")
    assert live["params"]["warmup_seconds"] == 120 and live["params"]["measure_seconds"] == 600
    assert live["params"]["live_fraction"] == 0.5
    # a plan states how long each run is expected to take; the live run's own seconds are inside that estimate
    assert live["minutes_per_run"] * 60 >= live["params"]["warmup_seconds"] + live["params"]["measure_seconds"]


def test_the_smoke_cell_checks_every_record_and_the_crash_cell_of_the_smoke_plan_fires_its_trigger():
    smoke = plans.select(plans.load_plans(os.path.join(ARCHIVE, "plans.json")), "smoke")
    by_cell = {c["cell"]: c for c in smoke["cells"]}
    assert by_cell["smoke"]["variants"][0]["records"] >= 1000
    crash = by_cell["crash-K2"]
    assert crash["params"]["kills_per_run"] == 1 and crash["params"]["n_range"][1] <= 3


# ------------------------------------ shared #744 tests, archive kind


def test_a_partial_or_truncated_archive_run_is_refused(tmp_path):
    run_dir = fixture.write_run(str(tmp_path))
    rows, env = schema.load_run(run_dir, KIND)
    assert len(rows) == len(plans.expected_keys(env["plan"], KIND))
    path = os.path.join(run_dir, "archive.csv")
    with open(path) as handle:
        lines = handle.readlines()
    with open(path, "w") as handle:
        handle.writelines(lines[:-3])
    with pytest.raises(schema.RunRefused, match="expected rows are missing"):
        schema.load_run(run_dir, KIND)


def test_a_smoke_row_with_a_gap_is_not_ok():
    row = {c: "" for c in KIND.columns}
    row.update({"status": "ok", "cell": "smoke", "records_expected": "10", "records_present": "9", "gaps": "1",
                "duplicates": "0", "torn_objects": "0", "orphan_objects": "0", "unexpected": "0"})
    assert any("not ok" in problem for problem in KIND.check_row(row))
    row["gaps"] = "0"
    assert KIND.check_row(row) == []


def test_nothing_is_timed_or_crashed_for_a_system_whose_smoke_did_not_pass(tmp_path):
    env = fixture.environment("r")
    driver = run.Runner(None, env, str(tmp_path), "")
    item = plans.Planned("vector", "acks", "drain", 1, {"record_size": 1024}, {"records": 10}, "performance")
    assert driver.smoke_failed(item) is None  # no smoke yet in this run
    row = runner.base_row(env, item._replace(cell="smoke", metric_class="correctness"), system_version="x",
                          image_digest="x", config_sha256="x")
    row.update({"status": "failed", "note": "smoke: 3 gaps", "started_at": "t", "wall_seconds": "1"})
    schema.append_row(os.path.join(str(tmp_path), "archive.csv"), KIND, row)
    assert "did not pass its smoke" in driver.smoke_failed(item)
    assert driver.smoke_failed(item._replace(system="vtop", profile="strong")) is None
    assert driver.smoke_failed(item._replace(cell="smoke")) is None
    with pytest.raises(runner.CellRefused, match="did not pass its smoke"):
        driver.execute(item)


def test_a_tool_whose_documented_best_config_cannot_be_written_is_a_refused_row_not_a_weaker_one(monkeypatch):
    monkeypatch.setitem(run.CONFIGS, "vector", {"acks": TOOLS["vector"]["profiles"]["acks"]})
    with pytest.raises(runner.CellRefused, match="no weaker configuration is substituted"):
        run.require_config("vector", "disk-buffer")


def test_every_archive_system_is_registered_for_use_case_a():
    reg = registry.load()
    for system in KIND.profiles:
        registry.require_runnable(reg, system, private=False, use_case="a")
    assert registry.family_order(reg, "archive")[:3] == ["connect-lenses", "vtop", "vector"]


def test_the_environment_block_records_the_extra_configs_and_deviations():
    hashes = run.extra_config_hashes(["connect-lenses", "vtop"])
    assert set(hashes) == {"connect-lenses"} and set(hashes["connect-lenses"]) == {"worker"}
    assert hashes["connect-lenses"]["worker"] == schema.sha256_text(
        _read(TOOLS["connect-lenses"]["extra_configs"]["worker"]))
    paths = run.config_paths(["connect-lenses"])
    assert TOOLS["connect-lenses"]["dockerfile"] in paths and "benchmarks/archive_compare/compose.yml" in paths
    assert any("Connect" in d and "orphan" in d for d in run.DEVIATIONS)


# --------------------------------------------------- resume (review of #906-#908)


def _args(**kw):
    import argparse

    base = dict(plan=None, smoke=False, tools=None, host_class=None)
    base.update(kw)
    return argparse.Namespace(**base)


def test_a_resume_continues_the_recorded_plan_and_a_disagreeing_argument_is_refused():
    plan = plans.select(plans.load_plans(os.path.join(ARCHIVE, "plans.json")), "hosted")
    plan["systems"] = ["vtop"]
    recorded = {"plan": plan}
    assert run.plan_for(_args(), recorded)["systems"] == ["vtop"]
    assert run.plan_for(_args(plan="hosted", tools="vtop"), recorded)["name"] == "hosted"
    with pytest.raises(plans.PlanRefused, match="cannot resume with --plan full"):
        run.plan_for(_args(plan="full"), recorded)
    with pytest.raises(plans.PlanRefused, match="--tools vector"):
        run.plan_for(_args(tools="vector"), recorded)
    fresh = run.plan_for(_args(smoke=True, tools="vtop,vector", host_class="hosted"), None)
    assert fresh["systems"] == ["vector", "vtop"] or set(fresh["systems"]) == {"vtop", "vector"}
    assert fresh["host_class"] == "hosted" and fresh["name"] == "smoke"


def test_the_run_directory_and_cli_shape_match_the_sibling_harnesses():
    # the same flags as spool_compare and link_compare: one muscle memory for the three harnesses
    import inspect

    source = inspect.getsource(run.main)
    for flag in ("--plan", "--smoke", "--tools", "--host-class", "--run-id", "--resume", "--budget-minutes",
                 "--cooldown-seconds", "--max-hours", "--tool-cpus", "--tool-memory", "--private", "--dry-run",
                 "--check", "--results", "--no-disk-probe", "--rebuild"):
        assert f'"{flag}"' in source, flag


def test_the_checker_reads_the_whole_bucket_a_runs_one_tool_in_a_fresh_one():
    assert "store.snapshot_commands(\"\", BUCKET, \"\", inner)" in open(os.path.join(ARCHIVE, "run.py")).read()
    assert kills.KILLS_COLUMNS[0] == "run_id"


@pytest.mark.parametrize("name", ["smoke", "hosted", "hosted-store-kills", "full"])
def test_every_put_trigger_is_reachable_by_the_tool_that_makes_the_fewest_puts(name):
    # Vector writes one object per 32 MiB, or per 60 s of an idle tail, from all three partitions together: the
    # fewest PUTs of the three tools. A life holding `remaining` bytes makes at most remaining/32 MiB + 1 PUTs, and a
    # kill at the Nth PUT (K1: before it, K2: after it) uses up N-1 (N) whole objects. If the Nth PUT cannot happen the
    # trigger never fires, and the first one failing makes the whole run a failed row (review of #908 found this
    # for the spool comparison).
    object_bytes = 32 * 2**20
    plan = plans.select(plans.load_plans(os.path.join(ARCHIVE, "plans.json")), name)
    for cell in plan["cells"]:
        if cell["cell"] not in ("crash-K1", "crash-K2"):
            continue
        params, kind = cell["params"], cell["cell"].split("-")[1]
        for variant in cell["variants"]:
            for repetition in range(1, cell["repetitions"] + 1):
                remaining = variant["records"] * variant["record_size"]
                for point in kills.schedule(params["kill_seed"], kind, repetition, params["kills_per_run"],
                                            n_range=tuple(params["n_range"])):
                    assert remaining // object_bytes + 1 >= point.n, (name, cell["cell"], repetition, point)
                    remaining = max(remaining - (point.n - 1 if kind == "K1" else point.n) * object_bytes, 0)


def test_every_run_checks_its_images_first_so_a_pruned_image_is_rebuilt_not_a_failed_run(monkeypatch, tmp_path):
    seen = []
    monkeypatch.setattr(run.lab, "ensure_vtop_image", lambda rebuild: seen.append("vtop") or "vtop-compare:x")
    monkeypatch.setattr(run.lab, "ensure_minio_image", lambda: seen.append("minio"))
    monkeypatch.setattr(run.lab, "ensure_image", lambda ref: seen.append(ref.split("@")[0]))
    built = []
    monkeypatch.setattr(run.lab, "sh", lambda args, **kw: built.append(args[:3]) or type(
        "R", (), {"returncode": 1, "stdout": "", "stderr": ""})())
    driver = run.Runner(None, fixture.environment("r"), str(tmp_path), "")
    driver.ensure_images("connect-lenses")
    assert "minio" in seen and any(ref.startswith("apache/kafka") for ref in seen)
    assert any(args[:2] == ["docker", "build"] for args in built)  # the inspect said it is gone, so it is built
    seen.clear()
    driver.ensure_images("vtop")
    assert seen[0] == "vtop"
