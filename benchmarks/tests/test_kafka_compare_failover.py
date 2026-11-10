"""The failover benchmark's arithmetic, parsers and refusals (#597).

Every number the failover report shows comes through these functions, so each
definition is pinned here: what an unavailability window is, which latency a
record is charged, when a read-back counts a record as lost or duplicated, and
when the report refuses to draw at all.
"""
import copy
import json
import os
import struct

import pytest

from kafka_compare import failover_schema as fschema
from kafka_compare import failover_stats as fstats
from kafka_compare import metadata_probe, schema

# --------------------------------------------------------------------- Kafka parsing


def vp_lines(acks, errors=(), startup=1000):
    lines = [json.dumps({"timestamp": startup, "name": "startup_complete"}), "[2026] WARN some log line"]
    for t, value in acks:
        lines.append(json.dumps({"timestamp": t, "name": "producer_send_success", "key": None, "value": str(value),
                                 "offset": value, "partition": 0, "topic": "bench"}))
    for t, value in errors:
        lines.append(json.dumps({"timestamp": t, "name": "producer_send_error", "exception": "TimeoutException",
                                 "message": "Expiring", "value": str(value)}))
    lines.append(json.dumps({"timestamp": 9999, "name": "tool_data", "sent": 3, "acked": len(acks)}))
    return lines


def test_verifiable_producer_lines_become_one_ack_per_record():
    run = fstats.parse_verifiable_producer(vp_lines([(1003, 1), (1002, 0)], errors=[(1500, 2)]))
    assert run.startup_ms == 1000
    assert run.acks == [fstats.AckEvent(1002, 0, 1), fstats.AckEvent(1003, 1, 1)]
    assert run.errors == [(1500, "TimeoutException", "Expiring")]
    assert run.non_json_lines == 1
    assert run.tool_data["acked"] == 2


def test_a_producer_that_never_started_is_refused():
    with pytest.raises(fstats.ParseError, match="startup_complete"):
        fstats.parse_verifiable_producer([json.dumps({"timestamp": 1, "name": "tool_data"})])


def test_an_ack_whose_value_is_not_a_sequence_is_refused():
    line = json.dumps({"timestamp": 1, "name": "producer_send_success", "value": "x7"})
    with pytest.raises(fstats.ParseError, match="not a sequence"):
        fstats.parse_verifiable_producer([json.dumps({"timestamp": 0, "name": "startup_complete"}), line])


def test_client_warnings_count_retries_codes_and_connection_failures():
    stderr = "\n".join([
        "[2026] WARN [Producer clientId=p] Got error produce response with correlation id 9 on topic-partition "
        "bench-0, retrying (2147483646 attempts left). Error: NOT_LEADER_OR_FOLLOWER (Sender)",
        "[2026] WARN [Producer clientId=p] Got error produce response with correlation id 10 on topic-partition "
        "bench-0, retrying (2147483645 attempts left). Error: NOT_LEADER_OR_FOLLOWER (Sender)",
        "[2026] WARN [Producer clientId=p] Connection to node 1 (kafka-1/172.18.0.2:9092) could not be established.",
        # The trial run's exact lines when the leader was killed:
        "[2026-11-10 18:38:14,625] WARN [Producer clientId=producer-1] Received invalid metadata error in produce "
        "request on partition bench-0 due to org.apache.kafka.common.errors.NetworkException: Disconnected from node "
        "3 Going to request metadata update now (org.apache.kafka.clients.producer.internals.Sender)",
        "[2026-11-10 18:38:14,681] WARN [Producer clientId=producer-1] Error connecting to node kafka-3:9092 (id: 3 "
        "rack: null isFenced: false) (org.apache.kafka.clients.NetworkClient)",
        "java.net.UnknownHostException: kafka-3: Name does not resolve",
    ])
    assert fstats.kafka_client_warnings(stderr) == {
        "retries": 3, "connect_failures": 2, "code:NOT_LEADER_OR_FOLLOWER": 2}


def test_console_consumer_lines_parse_and_odd_ones_are_refused():
    assert fstats.parse_console_consumer("Offset:0\t0\nOffset:1\t1\nProcessed a total of 2 messages\n") == [
        (0, "0"), (1, "1")]
    with pytest.raises(fstats.ParseError):
        fstats.parse_console_consumer("Offset:x\t0\n")


def test_the_loss_check_is_byte_exact_and_counts_duplicates_and_unacked_records():
    check = fstats.kafka_loss_check([0, 1, 2, 3], ["0", "1", "1", "3", "4", "007", "abc"])
    assert check.acked == 4
    assert check.acked_lost == 1 and check.first_lost == [2]
    assert check.duplicates == 1
    assert check.unacked_present == 1  # "4" was written, never acknowledged: not a loss
    assert check.unexpected == 2  # "007" is not the text of record 7, "abc" was never sent


def test_a_clean_read_back_has_nothing_lost_or_doubled():
    check = fstats.kafka_loss_check(range(5), [str(n) for n in range(6)])
    assert (check.acked_lost, check.duplicates, check.unacked_present, check.unexpected) == (0, 0, 1, 0)


def probe_line(t_ms, broker, ok=True, leader=1, isr=(1, 2, 3)):
    return json.dumps({"t_us": int(t_ms * 1000), "broker": broker, "ok": ok, "leader": leader,
                       "isr": list(isr) if ok else None, "error": None if ok else "TimeoutError"})


def test_kafka_time_to_leader_is_the_first_answer_naming_another_leader():
    lines = [probe_line(900, "kafka-2"), probe_line(1100, "kafka-1", ok=False), probe_line(1500, "kafka-2", leader=-1),
             probe_line(9200, "kafka-3", leader=2, isr=(2, 3)), probe_line(9300, "kafka-2", leader=2)]
    assert fstats.kafka_time_to_leader(lines, 1000, old_leader=1) == (8200, 2)
    assert fstats.kafka_time_to_leader(lines[:3], 1000, old_leader=1) is None


def test_an_observation_inside_the_clock_tolerance_counts():
    lines = [probe_line(990, "kafka-2", leader=3)]
    assert fstats.kafka_time_to_leader(lines, 1000, old_leader=1) is None
    assert fstats.kafka_time_to_leader(lines, 1000, old_leader=1, tolerance_ms=20) == (-10, 3)


# ---------------------------------------------------------------------- VTOP parsing


def test_an_ack_log_across_restarts_is_one_sequence_space():
    text = "1000000 100 900\n1020000 200 800\n9000000 300 30000000\n9010000 400 700\n"
    assert fstats.parse_ack_log(text) == [fstats.AckEvent(1000, 0, 100), fstats.AckEvent(1020, 100, 100),
                                          fstats.AckEvent(9000, 200, 100), fstats.AckEvent(9010, 300, 100)]


def test_an_ack_log_that_goes_backwards_or_is_malformed_is_refused():
    with pytest.raises(fstats.ParseError, match="after 200"):
        fstats.parse_ack_log("1000 200 1\n2000 150 1\n")
    with pytest.raises(fstats.ParseError, match="line 1"):
        fstats.parse_ack_log("1000 200\n")


def test_vtop_time_to_leader_reads_the_lease_log():
    a1, a2 = "aaaaaaaa-0000-0000-0000-0000000000a1", "aaaaaaaa-0000-0000-0000-0000000000a2"
    lines = [f"500000 {a1} 1", "1200000 unreadable", f"16800000 {a2} 2"]
    assert fstats.vtop_time_to_leader(lines, 1000, a1) == (15800, a2, 2)
    assert fstats.vtop_time_to_leader(lines[:2], 1000, a1) is None
    assert fstats.lease_holder_at(lines, 10_000) == (a1, 1)
    assert fstats.lease_holder_at(lines, 20_000) == (a2, 2)


def test_driver_log_summary_counts_attempts_failures_and_leaders():
    log = "\n".join([
        "1 start total=10 rate=1",
        "2 attempt=1 produce holder=aaa1 epoch=1 addr=vtop-1:9400 first=0",
        "3 attempt=1 exit=3 acked=500 reason=produce interrupted: connection reset by peer",
        "4 attempt=2 produce holder=aaa1 epoch=1 addr=vtop-1:9400 first=500",
        "5 attempt=2 exit=1 acked=500 reason=error: connect vtop-1:9400: failed to lookup address information",
        "6 attempt=3 produce holder=aaa2 epoch=2 addr=vtop-2:9400 first=500",
        "7 attempt=3 exit=0 acked=10 reason=",
    ])
    assert fstats.driver_summary(log) == {
        "attempts": 3, "failed_attempts": 2, "leaders_used": 2,
        "reasons": {"connection lost": 1, "name not resolvable": 1}}


def test_verify_output_passes_only_with_its_done_line_and_exit_zero():
    done = "verify_done records=1000 next_offset=1001 gaps=1 high_watermark=1001\n"
    assert fstats.parse_vtop_verify(done, "", 0) == fstats.VerifyResult(True, 1000, 1, 1001, "")
    failed = fstats.parse_vtop_verify("", "error: record at offset 7 content mismatch\n", 1)
    assert not failed.passed and "content mismatch" in failed.error
    assert not fstats.parse_vtop_verify(done, "error: only 1000 records visible", 1).passed


# ------------------------------------------------------------------------- arithmetic


def test_unavailability_is_the_gap_spanning_the_fault_for_a_crash():
    acks = [0, 10, 20, 30, 5030, 5040]
    gap = fstats.unavailability(acks, fault_ms=25, post_ms=60_000)
    assert gap == fstats.Gap(5000, 5, 5005)


def test_unavailability_catches_an_outage_that_starts_after_the_signal():
    # An orderly stop: acknowledgements continue for 2 s, then leadership moves.
    acks = [0, 10, 20, 1000, 2000, 2300, 2310]
    gap = fstats.unavailability(acks, fault_ms=15, post_ms=60_000)
    assert gap == fstats.Gap(1000, 985, 1985)


def test_no_acknowledgement_after_the_fault_is_no_recovery_and_none_before_is_refused():
    assert fstats.unavailability([0, 10, 20], fault_ms=25, post_ms=1000) is None
    # A recovery after the window is still a recovery: the gap is long, not missing.
    assert fstats.unavailability([0, 10, 5000], fault_ms=25, post_ms=1000) == fstats.Gap(4990, -15, 4975)
    with pytest.raises(ValueError, match="before the fault"):
        fstats.unavailability([30, 40], fault_ms=25, post_ms=1000)


def test_max_gap_is_taken_inside_the_window_only():
    assert fstats.max_gap([0, 5, 100, 101, 400], 1, 200) == 95
    with pytest.raises(ValueError):
        fstats.max_gap([0, 500], 1, 200)


def test_latency_is_charged_from_the_schedule_not_the_actual_send():
    # 1,000 records/s from t=0: record s is due at s ms. A batch of 3 acked at 10 ms,
    # then an outage: the next batch (3..5) acknowledged at 2,000 ms.
    events = [fstats.AckEvent(10, 0, 3), fstats.AckEvent(2000, 3, 3)]
    lat = fstats.record_latencies(events, origin_ms=0, rate=1000)
    assert lat == [(0, 10), (1, 9), (2, 8), (3, 1997), (4, 1996), (5, 1995)]
    assert fstats.window_percentile(lat, 3, 6, 50) == 1996
    assert fstats.window_percentile(lat, 100, 200, 99) is None


def test_a_record_acknowledged_twice_counts_once_at_its_first_ack():
    events = [fstats.AckEvent(10, 0, 2), fstats.AckEvent(50, 1, 2)]
    assert fstats.record_latencies(events, 0, 1000) == [(0, 10), (1, 9), (2, 48)]
    with pytest.raises(ValueError):
        fstats.record_latencies(events, 0, 0)


def test_ack_rate_bins_cover_every_bin_and_drop_what_is_outside():
    events = [fstats.AckEvent(900, 0, 5), fstats.AckEvent(1000, 5, 1), fstats.AckEvent(1150, 6, 2),
              fstats.AckEvent(5000, 8, 9)]
    assert fstats.ack_rate_bins(events, fault_ms=1000, start_rel_ms=-100, end_rel_ms=200, bin_ms=100) == [
        (-100, 5), (0, 1), (100, 2)]
    with pytest.raises(ValueError):
        fstats.ack_rate_bins(events, 0, 0, 0, 100)


def test_clock_offset_uses_the_tightest_probe():
    samples = [(0, 40_000_000, 100_000_000), (1_000_000_000, 1_022_000_000, 1_020_000_000)]
    assert fstats.clock_offset(samples) == (12.0, 10.0)
    with pytest.raises(ValueError):
        fstats.clock_offset([])
    with pytest.raises(ValueError):
        fstats.clock_offset([(10, 5, 9)])


# --------------------------------------------------------------------- metadata probe


def metadata_response(correlation, leader, isr, partition_error=0):
    def string(text):
        return struct.pack(">h", len(text)) + text.encode()

    body = struct.pack(">i", correlation) + struct.pack(">i", 0)
    body += struct.pack(">i", 2)
    for node in (1, 2):
        body += struct.pack(">i", node) + string(f"kafka-{node}") + struct.pack(">i", 9092) + struct.pack(">h", -1)
    body += string("cluster") + struct.pack(">i", 2)
    body += struct.pack(">i", 1) + struct.pack(">h", 0) + string("bench") + struct.pack(">b", 0)
    body += struct.pack(">i", 1) + struct.pack(">h", partition_error) + struct.pack(">i", 0) + struct.pack(">i", leader)
    body += struct.pack(">i", 3) + b"".join(struct.pack(">i", n) for n in (1, 2, 3))
    body += struct.pack(">i", len(isr)) + b"".join(struct.pack(">i", n) for n in isr)
    return body


def test_metadata_v4_request_is_framed_for_one_topic():
    frame = metadata_probe.encode_request(7, "bench", client_id="c")
    size, api_key, version, correlation = struct.unpack_from(">ihhi", frame)
    assert (size, api_key, version, correlation) == (len(frame) - 4, 3, 4, 7)
    assert frame.endswith(struct.pack(">i", 1) + struct.pack(">h", 5) + b"bench" + b"\x00")


def test_metadata_v4_response_names_the_leader_and_isr():
    answer = metadata_probe.decode_response(metadata_response(7, leader=2, isr=(3, 2)), "bench")
    assert answer["correlation_id"] == 7 and answer["leader"] == 2 and answer["isr"] == [2, 3]
    assert answer["brokers"] == {1: "kafka-1:9092", 2: "kafka-2:9092"} and answer["error"] is None
    errored = metadata_probe.decode_response(metadata_response(8, leader=-1, isr=(), partition_error=5), "bench")
    assert errored["leader"] == -1 and errored["error"] == "partition error 5"
    assert metadata_probe.decode_response(metadata_response(9, 1, (1,)), "other")["error"] == \
        "partition not in response"


def test_a_truncated_metadata_response_is_a_protocol_error():
    with pytest.raises(metadata_probe.ProtocolError):
        metadata_probe.decode_response(metadata_response(7, 2, (2,))[:-3], "bench")


# --------------------------------------------------------------------- CSV refusals

MATRIX = {"systems": ["kafka", "vtop"], "faults": list(fstats.FAULTS), "repetitions": 2, "offered_rate": 100}


def environment():
    return {"kind": "failover", "run_id": "fo-test", "run_state": "complete", "started_at": "2026-11-10T00:00:00+00:00",
            "comparable": {"host_cpu": "Test", "vtop_commit": "a" * 40, "profile": "defaults"},
            "matrix": copy.deepcopy(MATRIX)}


def rows_for(env):
    fp = schema.fingerprint(env)
    rows, timeline = [], []
    for system in MATRIX["systems"]:
        for fault in MATRIX["faults"]:
            for rep in (1, 2):
                rows.append({
                    "run_id": "fo-test", "env_fingerprint": fp, "vtop_commit": "a" * 40, "kafka_image": "k",
                    "profile": "defaults", "system": system, "client_path": fschema.CLIENT_PATHS[system],
                    "fault": fault, "repetition": rep, "status": "ok", "offered_rate": 100, "records_target": 9500,
                    "records_acked": 9500, "unavailability_ms": 5000.0, "gap_start_ms": -3.0,
                    "baseline_max_gap_ms": 20.0, "time_to_leader_ms": 4000.0, "p99_before_ms": 3.0,
                    "p99_after_ms": 4000.0, "p50_after_ms": 10.0, "max_latency_ms": 5000.0, "send_errors": 0,
                    "retries": 2, "acked_lost": 0, "duplicates": 0, "verify": "passed", "note": ""})
                for t in (0, 100):
                    timeline.append({"run_id": "fo-test", "env_fingerprint": fp, "system": system, "fault": fault,
                                     "repetition": rep, "t_ms": t, "records": 10})
    return rows, timeline


def write_run(directory, env, rows, timeline):
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, "environment.json"), "w") as handle:
        json.dump(env, handle)
    fschema.write_header(os.path.join(directory, "failover.csv"))
    fschema.append_rows(os.path.join(directory, "failover.csv"), rows)
    fschema.write_header(os.path.join(directory, "timeline.csv"), fschema.TIMELINE_COLUMNS)
    fschema.append_rows(os.path.join(directory, "timeline.csv"), timeline, fschema.TIMELINE_COLUMNS)
    return str(directory)


def test_a_complete_run_loads(tmp_path):
    env = environment()
    rows, timeline = rows_for(env)
    run = fschema.load_failover_run(write_run(tmp_path / "r", env, rows, timeline))
    assert len(run.rows) == 12 and len(run.timeline) == 24


@pytest.mark.parametrize("mutate, reason", [
    (lambda env, rows, tl: rows.pop(), "partial"),
    (lambda env, rows, tl: rows.append(dict(rows[0])), "duplicate rows"),
    (lambda env, rows, tl: rows[0].update(vtop_commit="b" * 40), "mixes 2 values of vtop_commit"),
    (lambda env, rows, tl: rows[0].update(status="failed", note=""), "must say why"),
    (lambda env, rows, tl: rows[0].update(unavailability_ms=""), "unavailability_ms"),
    (lambda env, rows, tl: rows[0].update(verify="maybe"), "verify is"),
    (lambda env, rows, tl: rows[0].update(acked_lost=""), "acked_lost"),
    (lambda env, rows, tl: rows[0].update(client_path="kafka-jvm", system="vtop"), "measured through"),
    (lambda env, rows, tl: rows[0].update(fault="kill_controller"), "unknown fault"),
    (lambda env, rows, tl: rows[0].update(time_to_leader_ms=""), "no time to leader"),
    (lambda env, rows, tl: env.update(run_state="interrupted"), "did not finish"),
    (lambda env, rows, tl: env.update(kind="throughput"), "not a failover run"),
    (lambda env, rows, tl: env["comparable"].update(host_cpu="Other"), "fingerprints to"),
    (lambda env, rows, tl: tl.pop(), "different numbers of bins"),
    (lambda env, rows, tl: [tl.remove(p) for p in list(tl) if p["repetition"] == 1 and p["system"] == "vtop"
                            and p["fault"] == "stop_leader"], "no points for the ok repetitions"),
    (lambda env, rows, tl: tl[0].update(run_id="other"), "another run"),
])
def test_the_report_refuses_what_it_cannot_honestly_draw(tmp_path, mutate, reason):
    env = environment()
    rows, timeline = rows_for(env)
    mutate(env, rows, timeline)
    directory = write_run(tmp_path / "r", env, rows, timeline)
    with pytest.raises(schema.ReportRefused, match=reason):
        fschema.load_failover_run(directory)


def test_a_failed_read_back_may_leave_the_counts_empty_but_must_say_failed(tmp_path):
    env = environment()
    rows, timeline = rows_for(env)
    rows[0].update(verify="FAILED: record at offset 7 content mismatch", acked_lost="", duplicates="")
    run = fschema.load_failover_run(write_run(tmp_path / "r", env, rows, timeline))
    assert run.rows[0]["verify"].startswith("FAILED")


def test_a_failed_repetition_is_a_row_not_a_hole(tmp_path):
    env = environment()
    rows, timeline = rows_for(env)
    rows[0].update(status="failed", note="nothing acknowledged after the fault: no recovery", unavailability_ms="")
    timeline = [p for p in timeline if not (p["system"] == rows[0]["system"] and p["fault"] == rows[0]["fault"]
                                            and p["repetition"] == rows[0]["repetition"])]
    run = fschema.load_failover_run(write_run(tmp_path / "r", env, rows, timeline))
    assert run.rows[0]["status"] == "failed"


def test_missing_files_are_refused(tmp_path):
    with pytest.raises(schema.ReportRefused, match="no environment.json"):
        fschema.load_failover_run(str(tmp_path))
    env = environment()
    rows, timeline = rows_for(env)
    directory = write_run(tmp_path / "r", env, rows, timeline)
    os.remove(os.path.join(directory, "timeline.csv"))
    with pytest.raises(schema.ReportRefused, match="no timeline.csv"):
        fschema.load_failover_run(directory)


def test_unknown_columns_are_refused_at_write_time(tmp_path):
    path = str(tmp_path / "f.csv")
    fschema.write_header(path)
    with pytest.raises(ValueError, match="unknown columns"):
        fschema.append_rows(path, [{"bogus": 1}])
