"""The runner's schema-3 measurements (#596, #600) without Docker: the resource window around a measured run, the
end-to-end stage, and that what the runner writes is what the report's loader accepts.

Everything that would touch Docker (clusters, the perf tools, the consumer container, `docker exec`) is replaced
by a fake with the same return shape; the runner's own logic and `schema.load_run` are the real ones."""
import argparse
import json
import os
import subprocess

import pytest

from kafka_compare import e2e, perftool, plan, resources, run, schema


def fake_args(**overrides):
    base = dict(reps=2, no_consume=False, fixed_seconds=1.0, smoke_records=10, no_e2e=False, no_fsync_parity=False,
                no_resources=False)
    base.update(overrides)
    return argparse.Namespace(**base)


# ----------------------------------------------------------------------------------------------- the matrix


def test_the_default_matrix_asks_for_resources_and_the_end_to_end_stage_with_kafkas_fsync_parity():
    matrix = run.build_matrix(fake_args(), [100], {100: 2000})
    assert matrix["resources"] is True
    assert matrix["e2e"] == {"load_basis": "common", "load_pct": 50, "extra_profiles": ["fsync-parity"]}
    plan.check_matrix(matrix)
    assert run.e2e_targets(matrix) == [("kafka", "defaults"), ("vtop", "defaults"), ("kafka", "fsync-parity")]


def test_each_new_measurement_has_its_own_off_switch_and_the_matrix_says_so():
    bare = run.build_matrix(fake_args(no_e2e=True, no_resources=True), [100], {100: 2000})
    assert bare["resources"] is False and "e2e" not in bare
    no_fsync = run.build_matrix(fake_args(no_fsync_parity=True), [100], {100: 2000})
    assert "extra_profiles" not in no_fsync["e2e"]
    assert run.e2e_targets(no_fsync) == [("kafka", "defaults"), ("vtop", "defaults")]


def test_kafka_fsync_parity_is_a_second_compose_file_and_a_topic_flush_setting(monkeypatch):
    base = run.compose_files("kafka")
    layered = run.compose_files("kafka", "fsync-parity")
    assert layered[:len(base)] == base and layered[-1].endswith("compose.kafka.fsync-parity.yml")
    assert run.compose_files("vtop", "fsync-parity") == run.compose_files("vtop")
    with open(layered[-1]) as handle:
        assert handle.read().count('KAFKA_LOG_FLUSH_INTERVAL_MESSAGES: "1"') == 3
    calls = []
    monkeypatch.setattr(run, "compose",
                        lambda system, *args, **kwargs: calls.append(("compose", kwargs.get("profile"))))
    monkeypatch.setattr(run, "client", lambda system, command, **_k: calls.append(("client", command)))
    run.cluster_up("kafka", "img", "fsync-parity")
    topic = next(command for kind, command in calls if kind == "client")
    assert topic[-2:] == ["--config", "flush.messages=1"]
    calls.clear()
    run.cluster_up("kafka", "img")
    assert "flush.messages=1" not in next(command for kind, command in calls if kind == "client")


# ------------------------------------------------------------------------------------------ resource window


class FakeCounters:
    """Counters that grow by one CPU second and 1000 written bytes per read, per node."""

    def __init__(self, fail_on: int | None = None):
        self.reads = {}
        self.fail_on = fail_on

    def closer(self):
        def snap(node):
            self.reads[node] = self.reads.get(node, 0) + 1
            if self.fail_on == sum(self.reads.values()):
                raise resources.CgroupRefused("io.stat lists no device")
            n = self.reads[node]
            return resources.Snapshot(n * 1_000_000, resources.Io(0, n * 1000, ("259:0",)))

        return resources.Closer(sync=lambda node: None, sleep=lambda seconds: None, snap=snap)


def test_a_measured_run_is_bracketed_and_its_three_nodes_are_summed():
    ctx = {"resources": True, "closer": FakeCounters().closer()}
    value, window, error = run.measure(ctx, "kafka", lambda: "ran")
    assert (value, error) == ("ran", "")
    assert window.total.cpu_seconds == 3.0 and window.total.disk_write_bytes == 3000
    assert window.end_ms >= window.start_ms
    row = {"status": "ok"}
    run.attach_resources(ctx, row, window, error, 123_456)
    assert row["delivered_bytes"] == 123_456 and row["cpu_seconds"] == 3.0 and row["disk_write_bytes"] == 3000
    assert row["disk_read_bytes"] == 0 and row["window_end_ms"] >= row["window_start_ms"]


def test_the_nodes_are_the_three_compose_containers_of_the_system():
    assert run.node_names("vtop") == ["kc-vtop-vtop-1-1", "kc-vtop-vtop-2-1", "kc-vtop-vtop-3-1"]
    assert run.node_names("kafka")[0] == run.SYSTEM_SPEC["kafka"]["leader"]


def test_a_counter_that_cannot_be_read_costs_the_row_its_resources_not_the_run_its_measurement():
    ctx = {"resources": True, "closer": FakeCounters(fail_on=2).closer()}
    ran = []
    value, window, error = run.measure(ctx, "kafka", lambda: ran.append(1) or "ran")
    assert ran == [1] and value == "ran" and window is None and "opening the resource window" in error
    row = {"status": "ok", "note": ""}
    run.attach_resources(ctx, row, window, error, 1)
    assert row["status"] == "failed" and row["note"].startswith("resources: ")


def test_without_resources_nothing_is_read_and_the_row_is_untouched():
    def boom(*_a):
        raise AssertionError("read a counter")

    ctx = {"resources": False, "closer": resources.Closer(boom, boom, boom)}
    value, window, error = run.measure(ctx, "vtop", lambda: 7)
    assert (value, window, error) == (7, None, "")
    row = {"status": "ok"}
    assert run.attach_resources(ctx, row, None, "", 1) == {"status": "ok"}


def test_a_failed_row_carries_no_resources():
    ctx = {"resources": True}
    row = {"status": "failed", "note": "timed out"}
    assert run.attach_resources(ctx, row, None, "", 1) == {"status": "failed", "note": "timed out"}


# ------------------------------------------------------------------------------------------- the e2e row

PRODUCER = perftool.ProducerResult(1000, 10_000.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, True)
SUMMARY = "READY\nconsumed 1000\n1 600\n2 390\n10 10\n"


def blank_row():
    return {"status": "", "note": ""}


def test_an_ok_end_to_end_row_takes_its_percentiles_from_the_histogram():
    row, hist = run.finish_e2e_row(blank_row(), PRODUCER, "", 3.0, 0, SUMMARY, "", 10_000, 1000)
    assert hist == {1: 600, 2: 390, 10: 10}
    assert row["status"] == "ok" and row["records"] == 1000 and row["records_produced"] == 1000
    assert (row["lat_p50_ms"], row["lat_p95_ms"], row["lat_p99_ms"], row["lat_p999_ms"]) == (1.0, 2.0, 2.0, 10.0)
    assert row["sustained"] is True and row["offered_rate"] == 10_000 and row["load_pct"] == 50


@pytest.mark.parametrize("out, err, code, match", [
    ("READY\nconsumed 990\n1 990\n", "", 0, "missing 10 records"),
    ("READY\nconsumed 1000\n-1 1\n1 999\n", "", 0, "before their CreateTime"),
    ("READY\n", "Exception in thread main", 1, "exit 1"),
    ("", "", None, "no `consumed N`"),
])
def test_an_end_to_end_run_that_cannot_stand_is_a_failed_row_saying_why(out, err, code, match):
    row, hist = run.finish_e2e_row(blank_row(), PRODUCER, "", 3.0, code, out, err, 10_000, 1000)
    assert row["status"] == "failed" and match in row["note"] and hist is None


def test_a_producer_that_failed_makes_the_row_failed_with_its_note():
    row, hist = run.finish_e2e_row(blank_row(), None, "exit 1; boom", 3.0, 0, SUMMARY, "", 10_000, 1000)
    assert row["status"] == "failed" and row["note"] == "exit 1; boom" and hist is None


def test_an_unsustained_rate_is_an_ok_row_flagged_not_sustained():
    slow = perftool.ProducerResult(1000, 5_000.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, True)
    row, hist = run.finish_e2e_row(blank_row(), slow, "", 3.0, 0, SUMMARY, "", 10_000, 1000)
    assert row["status"] == "ok" and row["sustained"] is False and "not sustained" in row["note"] and hist


def test_the_consumer_is_one_container_in_the_pinned_image_with_the_source_mounted_read_only(monkeypatch):
    seen = {}

    class FakeProc:
        stdout = iter(["READY\n"])
        stderr = iter([])

        def poll(self):
            return 0

    def popen(args, **kwargs):
        seen["args"] = args
        return FakeProc()

    monkeypatch.setattr(run.subprocess, "Popen", popen)
    consumer = run.E2eConsumer("vtop", 1000)
    consumer.wait_ready(5)
    args = seen["args"]
    assert args[:3] == ["docker", "run", "--rm"] and run.KAFKA_IMAGE in args
    assert f"{run.CLIENT_DIR}:/client:ro" in args and "kc-vtop-bench" in args
    assert args[args.index(run.KAFKA_IMAGE) + 1:][:4] == ["java", "-cp", "/opt/kafka/libs/*",
                                                           "/client/E2eLatency.java"]
    assert os.path.isfile(os.path.join(run.CLIENT_DIR, "E2eLatency.java"))


# ---------------------------------------------------------------- what the runner writes, the loader reads

CONSUMER_PERF = ("start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, "
                 "rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec\n"
                 "2026-11-09 23:14:25:820, 2026-11-09 23:14:26:927, 1.0, 1.0, {n}, 1000.0, 10, 1000, 1.0, 1000.0\n")


def producer_result(rate):
    return perftool.ProducerResult(1000, float(rate), 1.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, True)


@pytest.fixture
def fake_cluster(monkeypatch):
    saturation = {"kafka": 40_000.0, "vtop": 20_000.0}

    def producer(system, records, size, rate, timeout, info_logging=False):
        return producer_result(saturation[system] if rate < 0 else rate), "", 1.0, ""

    def e2e_once(system, size, rate, records, timeout):
        return (producer_result(rate), "", 1.0, "", 0,
                f"READY\nconsumed {records}\n2 {records - 20}\n9 20\n", "")

    monkeypatch.setattr(run, "cluster_up", lambda *_a: None)
    monkeypatch.setattr(run, "cluster_down", lambda *_a: None)
    monkeypatch.setattr(run, "smoke", lambda *_a: None)
    monkeypatch.setattr(run, "_save_leader_log", lambda *_a: None)
    monkeypatch.setattr(run, "_observe_kafka", lambda *_a: None)
    monkeypatch.setattr(run, "run_producer", producer)
    monkeypatch.setattr(run, "run_e2e_once", e2e_once)
    monkeypatch.setattr(run, "client", lambda *_a, **_k: subprocess.CompletedProcess(
        [], 0, CONSUMER_PERF.format(n=2000), ""))


def simulate(tmp_path, matrix_args):
    sizes, sat_records = [100], {100: 2000}
    matrix = run.build_matrix(matrix_args, sizes, sat_records)
    comparable = {"vtop_commit": "a" * 40, "kafka_image": run.KAFKA_IMAGE, "client": run.client_name(),
                  "host_disk": "Test SSD, SSD, NVMe, ext4", "profile": "defaults"}
    env = {"schema_version": schema.SCHEMA_VERSION, "kind": "throughput", "run_id": "sim", "run_state": "running",
           "comparable": comparable, "matrix": matrix, "observed": {}, "events": []}
    run_dir = str(tmp_path / "sim")
    os.makedirs(run_dir)
    ctx = {"run_id": "sim", "run_dir": run_dir, "csv": os.path.join(run_dir, "results.csv"), "env": env,
           "fingerprint": schema.fingerprint(env), "vtop_image": "vtop-bench:x", "reps": matrix_args.reps,
           "sat_records": sat_records, "fixed_seconds": 1.0, "smoke_records": 10, "sat_timeout": 10.0,
           "consume": True, "matrix": matrix, "resources": bool(matrix.get("resources")),
           "e2e": bool(matrix.get("e2e")), "closer": FakeCounters().closer(), "saturation": {},
           "hist_csv": os.path.join(run_dir, schema.E2E_HIST)}
    schema.write_header(ctx["csv"])
    if ctx["e2e"]:
        schema.write_header(ctx["hist_csv"], schema.E2E_HIST_COLUMNS)
    rates = run.run_cell(ctx, "kafka", 100, None)
    run.run_cell(ctx, "vtop", 100, rates)
    if ctx["e2e"]:
        run.run_e2e_stage(ctx, 100)
    env["run_state"] = "complete"
    with open(os.path.join(run_dir, "environment.json"), "w") as handle:
        json.dump(env, handle)
    return run_dir, ctx


def test_a_simulated_run_is_complete_and_loads_with_every_new_measurement(tmp_path, fake_cluster):
    run_dir, ctx = simulate(tmp_path, fake_args())
    loaded = schema.load_run(run_dir)
    assert loaded.version == 3
    assert ctx["env"]["observed"]["e2e_rates"] == {"100": 10_000}  # half of the slower system's saturation
    ok_e2e = [r for r in loaded.rows if r["mode"] == "e2e" and r["status"] == "ok"]
    assert {(r["system"], r["profile"]) for r in ok_e2e} == {("kafka", "defaults"), ("vtop", "defaults"),
                                                             ("kafka", "fsync-parity")}
    assert {r["offered_rate"] for r in ok_e2e} == {"10000"} and len(loaded.hist) == len(ok_e2e) == 6
    measured = [r for r in loaded.rows if r["mode"] in schema.RESOURCE_MODES and r["status"] == "ok"]
    assert measured and all(r["delivered_bytes"] and r["cpu_seconds"] == "3.0" for r in measured)
    consume = next(r for r in measured if r["mode"] == "consume")
    assert consume["delivered_bytes"] == str(2000 * 100)


def test_a_simulated_run_without_the_new_measurements_is_still_a_complete_run(tmp_path, fake_cluster):
    run_dir, _ = simulate(tmp_path, fake_args(no_e2e=True, no_resources=True))
    loaded = schema.load_run(run_dir)
    assert loaded.hist == {} and not os.path.exists(os.path.join(run_dir, schema.E2E_HIST))
    assert {r["cpu_seconds"] for r in loaded.rows} == {""}


def test_no_common_rate_is_a_refused_row_for_every_target_never_a_hole(tmp_path, monkeypatch, fake_cluster):
    monkeypatch.setattr(run, "run_producer", lambda *_a, **_k: (None, "exit 1", 1.0, ""))
    run_dir, _ = simulate(tmp_path, fake_args())
    rows = schema.read_rows(os.path.join(run_dir, "results.csv"))[0]
    refused = [r for r in rows if r["mode"] == "e2e"]
    assert len(refused) == 3 * 2 and {r["status"] for r in refused} == {"refused"}
    assert all("no common offered rate" in r["note"] for r in refused)


def test_a_consumer_that_missed_records_is_a_failed_row_and_leaves_no_histogram_line(tmp_path, monkeypatch,
                                                                                      fake_cluster):
    monkeypatch.setattr(run, "run_e2e_once", lambda system, size, rate, records, timeout: (
        producer_result(rate), "", 1.0, "", 0, f"READY\nconsumed {records - 5}\n2 {records - 5}\n", ""))
    run_dir, _ = simulate(tmp_path, fake_args())
    with open(os.path.join(run_dir, schema.E2E_HIST)) as handle:
        assert handle.read().strip() == ",".join(schema.E2E_HIST_COLUMNS)
    rows = schema.read_rows(os.path.join(run_dir, "results.csv"))[0]
    failed = [r for r in rows if r["mode"] == "e2e"]
    assert {r["status"] for r in failed} == {"failed"} and all("missing 5 records" in r["note"] for r in failed)
    assert schema.load_run(run_dir).hist == {}


def test_the_preflight_names_the_flag_that_turns_each_measurement_off(monkeypatch):
    matrix = run.build_matrix(fake_args(), [100], {100: 2000})

    def no_v2(args, **_k):
        return subprocess.CompletedProcess(args, 0, "cpuset cpu memory pids", "")

    monkeypatch.setattr(run, "sh", no_v2)
    with pytest.raises(SystemExit, match="--no-resources"):
        run.preflight_clients(matrix)

    def no_compiler(args, **_k):
        if "cat" in args:
            return subprocess.CompletedProcess(args, 0, "cpuset cpu io memory pids", "")
        raise RuntimeError("error: can't find javac")

    monkeypatch.setattr(run, "sh", no_compiler)
    with pytest.raises(SystemExit, match="--no-e2e"):
        run.preflight_clients(matrix)

    def fine(args, **_k):
        return subprocess.CompletedProcess(args, 0, "cpuset cpu io memory pids" if "cat" in args else "ok\n", "")

    monkeypatch.setattr(run, "sh", fine)
    run.preflight_clients(matrix)


def test_the_common_rate_and_the_histogram_helpers_are_the_modules_own():
    assert run.E2E_LOAD_PCT == 50 and run.e2e is e2e
