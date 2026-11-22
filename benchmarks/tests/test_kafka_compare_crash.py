"""The whole-replica-set crash benchmark's arithmetic, schema and refusals (#599).

Every number in crash.csv comes through crash_analysis, applied to what a crash left
on disk: the producer's acknowledgement log, the read-backs, the canary files. Each
definition is pinned here: which acknowledged record counts as lost, when a crash
row is `refused` and not `ok`, when the whole run is suspect, and when the loader
refuses to hand a run to a report.
"""
import copy
import json
import os

import pytest

from kafka_compare import crash_analysis as ca
from kafka_compare import crash_schema as cs
from kafka_compare import schema

PREFIX = "kc599-" + "x" * 20

# ------------------------------------------------------------------------ parsing


def ack_line(t, seq, offset, prefix=PREFIX):
    return json.dumps({"timestamp": t, "name": "producer_send_success", "key": None, "value": f"{prefix}.{seq}",
                       "offset": offset, "partition": 0, "topic": "bench"})


def test_the_producer_log_becomes_one_ack_per_record_with_its_offset():
    lines = [json.dumps({"timestamp": 1000, "name": "startup_complete"}), "[2026] WARN a log line",
             ack_line(1010, 1, 1), ack_line(1005, 0, 0),
             json.dumps({"timestamp": 1500, "name": "producer_send_error", "exception": "TimeoutException",
                         "message": "Expiring", "value": f"{PREFIX}.2"}),
             json.dumps({"timestamp": 9999, "name": "tool_data", "sent": 3, "acked": 2})]
    run = ca.parse_acks(lines, PREFIX)
    assert run.startup_ms == 1000
    assert run.acks == [ca.Ack(1005, 0, 0), ca.Ack(1010, 1, 1)]
    assert run.errors == 1 and run.sent == 3


def test_a_producer_that_never_started_is_refused():
    with pytest.raises(ca.ParseError, match="startup_complete"):
        ca.parse_acks([json.dumps({"timestamp": 1, "name": "tool_data"})], PREFIX)


def test_an_ack_for_a_value_this_crash_did_not_send_is_refused():
    lines = [json.dumps({"timestamp": 0, "name": "startup_complete"}), ack_line(1, 0, 0, prefix="other")]
    with pytest.raises(ca.ParseError, match="not a record this crash sent"):
        ca.parse_acks(lines, PREFIX)


def test_the_value_prefix_gives_the_requested_record_size_within_the_digits_of_the_counter():
    prefix = ca.value_prefix(1024, crash=7)
    sizes = {len(f"{prefix}.{n}") for n in (0, 9, 999_999)}
    assert max(sizes) <= 1024 and min(sizes) >= 1024 - 6
    assert prefix.startswith("kc599-c007-")
    assert ca.value_prefix(1024, crash=7) == prefix  # reproducible
    assert ca.value_prefix(1024, crash=8) != prefix


def test_a_tiny_record_size_is_refused():
    with pytest.raises(ValueError, match="record size"):
        ca.value_prefix(20, crash=1)


def consumed_text(*values, start=0):
    return "".join(f"Offset:{start + i}\t{v}\n" for i, v in enumerate(values)) + "Processed a total of 9 messages\n"


def test_consumer_lines_are_records_sentinels_or_other():
    text = consumed_text(f"{PREFIX}.0", f"{PREFIX}.1", "sentinel-c001", "garbage", f"{PREFIX}.007")
    got = ca.parse_consumed(text, PREFIX, "sentinel-c001")
    assert [(c.offset, c.kind, c.seq) for c in got] == [
        (0, "record", 0), (1, "record", 1), (2, "sentinel", None), (3, "other", None), (4, "other", None)]


def test_a_consumer_line_in_a_shape_nobody_expected_is_refused():
    with pytest.raises(ca.ParseError):
        ca.parse_consumed("Offset:x\t1\n", PREFIX, "s")


# --------------------------------------------------------------------------- the diff


def acks(*pairs, t0=1000):
    return [ca.Ack(t0 + i * 10, seq, off) for i, (seq, off) in enumerate(pairs)]


def read(*seqs, start=0):
    return ca.parse_consumed(consumed_text(*[f"{PREFIX}.{s}" for s in seqs], start=start), PREFIX, "sentinel")


def test_the_diff_finds_exactly_the_acknowledged_ids_that_are_missing():
    # 0..5 acknowledged; 2 and 3 and 5 are gone; 6 is present and was never acknowledged;
    # 4 appears twice; 1 is read twice at different offsets.
    given = acks((0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5))
    got = ca.diff_acked(given, read(0, 1, 4, 4, 6))
    assert got.acked == 6
    assert got.acked_lost == 3 and got.lost == [2, 3, 5]
    assert got.lost_first_seq == 2 and got.lost_last_seq == 5 and got.lost_runs == 2
    assert got.duplicates == 1
    assert got.unacked_present == 1


def test_nothing_lost_is_zero_everywhere():
    got = ca.diff_acked(acks((0, 0), (1, 1)), read(0, 1, 2))
    assert (got.acked_lost, got.lost_runs, got.duplicates, got.reordered, got.offset_mismatch) == (0, 0, 0, 0, 0)
    assert got.lost_first_seq is None and got.lost_last_seq is None and got.lost == []


def test_reordering_is_a_record_that_comes_after_a_larger_id():
    got = ca.diff_acked(acks((0, 0), (1, 1), (2, 2), (3, 3)), read(0, 2, 1, 3))
    assert got.reordered == 1 and got.acked_lost == 0


def test_an_acknowledged_offset_that_now_holds_another_record_is_counted():
    # 1 was acknowledged at offset 1 but is now at offset 2 (a gap replaced by something else)
    got = ca.diff_acked(acks((0, 0), (1, 1)), read(0, 9, 1))
    assert got.acked_lost == 0 and got.offset_mismatch == 1


def test_values_that_are_not_records_of_this_crash_are_counted_apart():
    consumed = ca.parse_consumed(consumed_text(f"{PREFIX}.0", "foreign", "sentinel"), PREFIX, "sentinel")
    got = ca.diff_acked(acks((0, 0)), consumed)
    assert got.unexpected == 1 and got.acked_lost == 0


def test_an_id_acknowledged_in_the_last_seconds_is_counted_in_the_window():
    given = [ca.Ack(1_000, 0, 0), ca.Ack(95_000, 1, 1), ca.Ack(99_000, 2, 2)]
    assert ca.acked_in_window(given, trigger_ms=100_000, window_s=10) == 2
    assert ca.acked_in_window(given, trigger_ms=100_000, window_s=2) == 1


def test_the_lost_window_is_the_age_of_the_oldest_lost_acknowledgement_at_the_trigger():
    given = [ca.Ack(60_000, 0, 0), ca.Ack(90_000, 1, 1), ca.Ack(99_000, 2, 2)]
    assert ca.lost_window_seconds(given, [1, 2], trigger_ms=100_000) == pytest.approx(10.0)
    assert ca.lost_window_seconds(given, [], trigger_ms=100_000) is None


# ------------------------------------------------------------------------------ canary


def canary(**kw):
    base = {"synced_bytes": 1048576, "synced_expected": 1048576, "unsynced_bytes": 0, "unsynced_expected": 1048576}
    base.update(kw)
    return ca.Canary(**base)


def test_a_failed_canary_makes_the_row_refused_not_ok_and_not_dropped():
    # power-loss mode: the synced file must be whole and the unsynced one absent or short.
    assert ca.judge_canary("flakey-drop", canary()).ok
    short = ca.judge_canary("flakey-drop", canary(synced_bytes=4096))
    assert not short.ok and "fsynced" in short.reason and "4096" in short.reason
    survived = ca.judge_canary("flakey-drop", canary(unsynced_bytes=1048576))
    assert not survived.ok and "unsynced" in survived.reason
    # a partly written unsynced file is allowed: short is "lost" too
    assert ca.judge_canary("flakey-drop", canary(unsynced_bytes=4096)).ok


def test_a_process_kill_keeps_the_page_cache_so_both_canaries_must_survive():
    assert ca.judge_canary("sigkill", canary(unsynced_bytes=1048576)).ok
    lost = ca.judge_canary("sigkill", canary(unsynced_bytes=0))
    assert not lost.ok and "page cache" in lost.reason


def test_the_canary_flags_are_what_the_csv_records():
    c = canary(unsynced_bytes=4096)
    assert ca.canary_flags(c) == {"canary_synced_ok": True, "canary_unsynced_lost": True}
    assert ca.canary_flags(canary(unsynced_bytes=1048576)) == {"canary_synced_ok": True,
                                                              "canary_unsynced_lost": False}


# ------------------------------------------------------------------- schedule and bounds


def test_the_schedule_is_identical_for_every_configuration_given_the_seed():
    a = ca.trigger_schedule(599, 30, 25.0, 55.0)
    b = ca.trigger_schedule(599, 30, 25.0, 55.0)
    assert a == b and len(a) == 30
    assert all(25.0 <= t <= 55.0 for t in a)
    assert ca.trigger_schedule(600, 30, 25.0, 55.0) != a
    # a longer run extends the schedule and does not reshuffle it, so a resumed or widened run pairs
    assert ca.trigger_schedule(599, 40, 25.0, 55.0)[:30] == a


def test_a_schedule_window_must_be_ordered_and_positive():
    with pytest.raises(ValueError):
        ca.trigger_schedule(1, 3, 30.0, 10.0)
    with pytest.raises(ValueError):
        ca.trigger_schedule(1, 3, 0.0, 10.0)


def test_the_upper_bound_for_0_of_30_is_9_5_percent_and_for_0_of_20_is_13_9():
    assert ca.upper_bound_95(0, 30) == pytest.approx(0.0950, abs=5e-4)
    assert ca.upper_bound_95(0, 20) == pytest.approx(0.1391, abs=5e-4)


def test_the_upper_bound_with_losses_is_the_exact_binomial_one():
    # 1 of 30 -> 14.86 % (Clopper-Pearson, one-sided 95 %): P(X <= 1 | p) = 0.05
    assert ca.upper_bound_95(1, 30) == pytest.approx(0.1486, abs=5e-4)
    assert ca.upper_bound_95(30, 30) == 1.0
    with pytest.raises(ValueError):
        ca.upper_bound_95(5, 3)


def rowdict(config="kafka-defaults", mode="flakey-drop", crash=1, lost=0, status="ok"):
    return {"config": config, "crash_mode": mode, "crash": str(crash), "acked_lost": str(lost), "status": status}


def test_a_run_where_kafka_defaults_loses_nothing_in_every_crash_is_flagged_as_a_suspect_method():
    rows = [rowdict(crash=i) for i in range(1, 6)]
    message = ca.suspect_method(rows, min_crashes=5)
    assert message and "lost nothing in 5 crashes" in message and "dm-flakey" in message
    # fewer crashes than the threshold, a loss, or another configuration: not suspect
    assert ca.suspect_method(rows[:4], min_crashes=5) is None
    assert ca.suspect_method(rows[:4] + [rowdict(crash=5, lost=12)], min_crashes=5) is None
    assert ca.suspect_method([rowdict(config="vtop-shipped", crash=i) for i in range(1, 9)], min_crashes=5) is None
    # a process kill keeps the page cache, so losing nothing there is the expected outcome
    assert ca.suspect_method([rowdict(mode="sigkill", crash=i) for i in range(1, 9)], min_crashes=5) is None
    # refused crashes do not count as crashes that lost nothing
    assert ca.suspect_method([rowdict(crash=i, status="refused") for i in range(1, 9)], min_crashes=5) is None


# ------------------------------------------------------------------------------ schema


def matrix(crashes=3):
    return {"configs": ["kafka-defaults", "kafka-fsync-parity", "vtop-shipped"], "crash_modes": ["flakey-drop"],
            "crashes": crashes, "rate": 1000, "record_size": 1024, "seed": 599, "trigger_window_s": [25.0, 55.0]}


def environment(m=None):
    env = {"schema_version": 2, "kind": "crash", "run_id": "crash-test", "run_state": "complete",
           "comparable": {"host_disk": "nvme ext4", "vtop_commit": "c" * 40, "kafka_image": "apache/kafka:4.3.2",
                          "client": "kafka-verifiable-producer 4.3.2 (JVM), both systems",
                          "seed": 599},
           "matrix": m or matrix()}
    return env


def good_row(env, config="kafka-defaults", mode="flakey-drop", crash=1, **over):
    system = "vtop" if config.startswith("vtop") else "kafka"
    row = {"run_id": "crash-test", "env_fingerprint": schema.fingerprint(env), "vtop_commit": "c" * 40,
           "kafka_image": "apache/kafka:4.3.2", "system": system, "config": config,
           "profile": "fsync-parity" if config == "kafka-fsync-parity" else "defaults", "crash_mode": mode,
           "crash": crash, "status": "ok", "seed": 599, "trigger_after_s": 30.0,
           "trigger_method": "dm-flakey drop_writes after SIGKILL of all three", "offered_rate": 1000,
           "record_size": 1024, "client": "kafka-verifiable-producer", "idempotent": True, "send_errors": 0,
           "acked": 39990, "acked_last_seq": 39989, "acked_in_last_10s": 9990, "read_records": 39000,
           "read_missing_before_sentinel": 990, "acked_lost": 990, "lost_first_seq": 39000,
           "lost_last_seq": 39989, "lost_runs": 1, "lost_window_s": 10.5, "present_unacked": 0,
           "duplicates": 0, "reordered": 0, "offset_mismatch": 0, "unexpected": 0, "canary_synced_ok": True,
           "canary_unsynced_lost": True, "sentinel_needed": False, "kill_to_drop_ms": 120,
           "recovery_seconds": 41.0, "needed_manual_repair": False, "started_at": "2026-11-22T10:00:00+00:00",
           "wall_seconds": 150.0, "note": ""}
    row.update(over)
    return row


def full_rows(env, crashes=3, **over):
    rows = []
    for config in env["matrix"]["configs"]:
        for mode in env["matrix"]["crash_modes"]:
            for crash in range(1, crashes + 1):
                rows.append(good_row(env, config, mode, crash, **over))
    return rows


def write_run(directory, env, rows):
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, "environment.json"), "w") as handle:
        json.dump(env, handle)
    path = os.path.join(directory, "crash.csv")
    cs.write_header(path)
    cs.append_rows(path, rows)
    return str(directory)


def test_a_complete_run_loads(tmp_path):
    env = environment()
    run = cs.load_crash_run(write_run(tmp_path / "r", env, full_rows(env)))
    assert len(run.rows) == 9 and run.environment["kind"] == "crash"


def test_the_expected_keys_are_every_configuration_mode_and_crash():
    keys = cs.expected_keys(matrix(crashes=2))
    assert len(keys) == 6 and ("vtop-shipped", "flakey-drop", 2) in keys


def test_a_partial_run_is_refused_naming_the_configuration(tmp_path):
    env = environment()
    rows = [r for r in full_rows(env) if not (r["config"] == "vtop-shipped" and r["crash"] > 1)]
    with pytest.raises(schema.ReportRefused, match=r"vtop-shipped.*flakey-drop.*1 of 3"):
        cs.load_crash_run(write_run(tmp_path / "r", env, rows))


def test_the_loader_refuses_fewer_ok_crashes_per_configuration_than_it_is_asked_for(tmp_path):
    env = environment(matrix(crashes=3))
    directory = write_run(tmp_path / "r", env, full_rows(env))
    assert cs.load_crash_run(directory, min_ok=3)
    with pytest.raises(schema.ReportRefused, match=r"kafka-defaults.*3 ok crashes.*at least 30"):
        cs.load_crash_run(directory, min_ok=30)


def test_refused_crashes_stay_rows_but_do_not_count_as_ok(tmp_path):
    env = environment()
    rows = full_rows(env)
    rows[0] = good_row(env, crash=1, status="refused", canary_synced_ok=False, acked_lost="", note="canary failed")
    directory = write_run(tmp_path / "r", env, rows)
    assert cs.load_crash_run(directory)  # a refused row is not a hole
    with pytest.raises(schema.ReportRefused, match="2 ok crashes"):
        cs.load_crash_run(directory, min_ok=3)


MUTATIONS = [
    (lambda e, rows: rows[0].update(canary_synced_ok=False), "canary"),
    (lambda e, rows: rows[0].update(canary_unsynced_lost=False), "canary"),
    (lambda e, rows: rows[0].update(note="", status="failed"), "must say why"),
    (lambda e, rows: rows[0].update(status="maybe"), "status"),
    (lambda e, rows: rows[0].update(crash_mode="kernel-panic"), "crash_mode"),
    (lambda e, rows: rows[0].update(config="redpanda"), "config"),
    (lambda e, rows: rows[1].update(vtop_commit="d" * 40), "mixes"),
    (lambda e, rows: rows.append(dict(rows[0])), "duplicate"),
    (lambda e, rows: rows[0].update(acked_lost=""), "acked_lost"),
    (lambda e, rows: rows[0].update(acked_lost=5, lost_first_seq="", lost_last_seq=""), "lost_first_seq"),
    (lambda e, rows: rows[0].update(acked_lost=0, lost_first_seq=3), "lost_first_seq"),
    (lambda e, rows: rows[0].update(acked=10, acked_lost=11), "more acknowledged records lost"),
    (lambda e, rows: e.update(run_state="interrupted"), "run_state"),
    (lambda e, rows: e.update(kind="failover"), "not a crash run"),
    (lambda e, rows: e["matrix"].update(crash_modes=["flakey-drop", "sigkill"]), "partial"),
]


@pytest.mark.parametrize(("mutate", "reason"), MUTATIONS)
def test_the_loader_refuses_what_it_cannot_honestly_use(tmp_path, mutate, reason):
    env = environment()
    rows = full_rows(env)
    mutate(env, rows)
    if "env_fingerprint" in rows[0]:
        for row in rows:
            row["env_fingerprint"] = schema.fingerprint(environment())
    with pytest.raises(schema.ReportRefused, match=reason):
        cs.load_crash_run(write_run(tmp_path / "r", env, rows))


def test_a_row_written_under_another_environment_is_refused(tmp_path):
    env = environment()
    rows = full_rows(env)
    env["comparable"]["host_disk"] = "a different disk"
    with pytest.raises(schema.ReportRefused, match="fingerprint"):
        cs.load_crash_run(write_run(tmp_path / "r", env, rows))


def test_unknown_columns_are_refused_at_write_time(tmp_path):
    path = str(tmp_path / "crash.csv")
    cs.write_header(path)
    with pytest.raises(ValueError, match="unknown columns"):
        cs.append_rows(path, [{"nope": 1}])


def test_a_sigkill_row_must_say_both_canaries_survived(tmp_path):
    env = environment(dict(matrix(), crash_modes=["sigkill"]))
    rows = full_rows(env, canary_unsynced_lost=False, acked_lost=0, lost_first_seq="",
                     lost_last_seq="", lost_runs=0)
    assert cs.load_crash_run(write_run(tmp_path / "r", env, rows))
    rows = full_rows(env, canary_unsynced_lost=True, acked_lost=0, lost_first_seq="",
                     lost_last_seq="", lost_runs=0)
    with pytest.raises(schema.ReportRefused, match="canary"):
        cs.load_crash_run(write_run(tmp_path / "r2", env, rows))


def test_the_matrix_is_checked_before_any_docker_work():
    cs.check_matrix(matrix())
    for bad in ({"crashes": 0}, {"configs": []}, {"crash_modes": ["power"]}, {"configs": ["nope"]}, {"rate": 0},
                {"record_size": 10}, {"trigger_window_s": [30.0, 10.0]}, {"seed": "x"}):
        with pytest.raises(cs.PlanRefused):
            cs.check_matrix(dict(matrix(), **bad))


def test_the_projection_grows_with_crashes_and_a_matrix_over_the_cap_is_refused():
    small = cs.projected_seconds(matrix(crashes=2))
    large = cs.projected_seconds(matrix(crashes=20))
    assert large > 9 * small
    assert cs.check_budget(matrix(crashes=2), max_hours=1.0) < 1.0
    with pytest.raises(cs.PlanRefused, match="over --max-hours"):
        cs.check_budget(matrix(crashes=30), max_hours=1.0)


def test_check_lists_what_a_run_still_owes(tmp_path, capsys):
    env = environment()
    env["run_state"] = "interrupted"
    rows = [r for r in full_rows(env) if not (r["config"] == "kafka-fsync-parity" and r["crash"] == 3)]
    directory = write_run(tmp_path / "r", env, rows)
    found = cs.check_run(directory)
    assert found.missing == [("kafka-fsync-parity", "flakey-drop", 3)]
    assert cs.check_main(directory) == 2
    assert "kafka-fsync-parity flakey-drop 3" in capsys.readouterr().err
    full = copy.deepcopy(env)
    full["run_state"] = "complete"
    good = write_run(tmp_path / "ok", full, full_rows(full))
    assert cs.check_main(good) == 0
