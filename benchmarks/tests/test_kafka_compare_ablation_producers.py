"""The group commit ablation's producer axis and the leader's counters (#752).

One producer never has two requests in the broker, so group commit has
nothing to merge in the 1-producer cell. The ablation therefore runs several
producers on the one partition (1, 2, 4, 16), each its own client container,
and reads the leader's group-commit and produce-phase counters around every
saturation run, so the report can say how many requests shared a barrier and
where the time went. What these tests hold:

- `--producers` is an axis of the matrix, the plan charges it, and a run owes
  its rows at every count;
- N producers share a run's records and offered rate, their throughput adds
  up and their percentiles are the worst of the N (`lat_basis` `worst-of-N`);
- the counters are parsed from the node's text, recorded per saturation
  repetition under the row's key, and a missed scrape is an event, not a stop;
- the report draws its charts and main tables at one producer count and the
  ablation at every count, with mean group size, overlapped groups and the
  per-request phase times, and says why the 1-producer cell cannot show group
  commit, that partitions do not merge, and that pipelining is not built.
"""
import argparse
import copy
import csv
import subprocess

import pytest
from test_kafka_compare_group_commit import AXIS, LEADER, axis_environment
from test_kafka_compare_schema_v2 import complete_rows, write_run

from kafka_compare import plan, report, run, schema

SWEEP = {**AXIS, "producers": [1, 2], "record_sizes": [100]}

METRICS = """\
# HELP vtop_broker_group_commit_requests_total Produce requests folded into commit groups
# TYPE vtop_broker_group_commit_requests_total counter
vtop_broker_group_commit_requests_total {requests}
vtop_broker_group_commits_total {commits}
vtop_broker_group_commit_overlapped_total {overlapped}
vtop_broker_group_commit_enabled 1
vtop_broker_produce_phase_seconds_total{{phase="local_fsync"}} {fsync}
vtop_broker_produce_phase_seconds_total{{phase="replicate"}} {replicate}
vtop_broker_produce_phase_seconds_total{{phase="commit"}} 0.5
"""


def metrics(requests=0, commits=0, overlapped=0, fsync=0.0, replicate=0.0):
    return METRICS.format(requests=requests, commits=commits, overlapped=overlapped, fsync=fsync,
                          replicate=replicate)


# ------------------------------------------------------------------- the plan


def test_the_ablation_matrix_owes_vtop_rows_at_every_producer_count_and_setting():
    matrix = {**AXIS, "producers": [1, 2, 4, 16], "record_sizes": [100, 1024]}
    keys = plan.expected_keys(matrix)
    saturation = {(k[2], k[4], k[8]) for k in keys if k[0] == "vtop" and k[1] == "saturation"}
    assert saturation == {(size, producers, setting) for size in (100, 1024) for producers in (1, 2, 4, 16)
                          for setting in ("off", "on")}
    assert {k[4] for k in keys if k[0] == "kafka"} == {1, 2, 4, 16}


def test_the_plan_charges_every_producer_count():
    one = plan.projected_seconds({**AXIS, "producers": [1]})
    four = plan.projected_seconds({**AXIS, "producers": [1, 2, 4, 16]})
    assert four == pytest.approx(4 * one)


@pytest.mark.parametrize("text, want", [("1", [1]), ("16,1,4,2", [1, 2, 4, 16])])
def test_the_producers_option_names_the_counts_in_ascending_order(text, want):
    assert run.parse_producers(text) == want


@pytest.mark.parametrize("text", ["", "0", "1,1", "two", "1,-2"])
def test_a_bad_producers_option_is_refused(text):
    with pytest.raises(SystemExit):
        run.parse_producers(text)


def _args(**overrides):
    args = run.build_parser().parse_args([])
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def test_the_matrix_records_the_producer_counts():
    args = _args(producers="1,2,4,16")
    sizes, sat_records = run.check_args(args)
    assert run.build_matrix(args, sizes, sat_records)["producers"] == [1, 2, 4, 16]
    assert run.build_matrix(_args(), sizes, sat_records)["producers"] == [1]


def test_too_few_records_for_the_producers_are_refused_before_any_docker_work():
    with pytest.raises(SystemExit, match="cannot be shared by 16 producers"):
        run.check_args(_args(producers="1,16", sat_records="100:100,1024:100"))


# ------------------------------------------------------------- the producers


def test_shares_differ_by_at_most_one_and_add_up():
    assert run.split_evenly(10, 4) == [3, 3, 2, 2]
    assert sum(run.split_evenly(3_000_000, 16)) == 3_000_000


def _result(records, rps, p99, avg=1.0):
    return run.perftool.ProducerResult(records=records, records_per_sec=rps, mib_per_sec=rps / 10000,
                                       lat_avg_ms=avg, lat_max_ms=p99 * 2, lat_p50_ms=p99 / 4,
                                       lat_p95_ms=p99 / 2, lat_p99_ms=p99, lat_p999_ms=p99 * 1.5,
                                       steady_state=True)


def test_producers_report_their_worst_percentiles_and_a_record_weighted_average():
    combined = run.combine_producers([_result(100, 1000.0, 10.0, avg=1.0), _result(300, 2000.0, 40.0, avg=3.0)])
    assert combined.records == 400
    # 0.1 s and 0.15 s windows: 400 records in the longer one, not 1000 + 2000 records/s.
    assert combined.records_per_sec == pytest.approx(400 / 0.15)
    assert combined.lat_p99_ms == 40.0 and combined.lat_p50_ms == 10.0 and combined.lat_max_ms == 80.0
    assert combined.lat_avg_ms == pytest.approx((100 * 1.0 + 300 * 3.0) / 400)


def test_producers_finishing_at_different_times_are_not_summed():
    # The review's example: 100,000 records in 1 s and 100,000 in 2 s. Adding the two rates says
    # 150,000 records/s; 200,000 records were delivered in 2 s, which is 100,000 records/s.
    fast, slow = _result(100_000, 100_000.0, 5.0), _result(100_000, 50_000.0, 5.0)
    assert fast.records_per_sec + slow.records_per_sec == 150_000.0
    for ends in (None, [1.0, 2.0]):
        combined = run.combine_producers([fast, slow], ends)
        assert combined.records == 200_000
        assert combined.records_per_sec == pytest.approx(100_000.0)
        assert combined.mib_per_sec == pytest.approx((fast.mib_per_sec * 1 + slow.mib_per_sec * 2) / 2)


def test_the_shared_window_runs_from_the_earliest_start_to_the_latest_end():
    one = _result(100_000, 100_000.0, 5.0)  # a 1 s window each
    # Ending together, the windows coincide: the floor, one window long.
    assert run.shared_window([one, one], [5.0, 5.0]) == pytest.approx(1.0)
    # One second apart, they span 2 s: 200,000 records in 2 s, not 200,000 records/s.
    assert run.shared_window([one, one], [1.0, 2.0]) == pytest.approx(2.0)
    assert run.combine_producers([one, one], [1.0, 2.0]).records_per_sec == pytest.approx(100_000.0)


def test_n_producers_share_the_records_and_the_offered_rate(monkeypatch):
    calls = []

    def fake(system, records, size, rate, timeout, info_logging=False):
        calls.append((records, rate))
        return _result(records, float(rate if rate > 0 else 1000), 5.0), "", 2.0, f"out {records}"

    monkeypatch.setattr(run, "run_producer", fake)
    result, note, wall, output = run.run_producers("vtop", 4, 1000, 100, 402, 60.0)
    assert sorted(calls) == sorted([(250, 101), (250, 101), (250, 100), (250, 100)])
    assert note == "" and wall == 2.0 and output.count("=== producer") == 4
    # 1000 records over the longest window (250 records at 100 records/s, 2.5 s), the producers' starts a
    # few milliseconds apart at most.
    assert result.records_per_sec == pytest.approx(400.0, rel=0.01)
    calls.clear()
    run.run_producers("vtop", 2, 1000, 100, -1, 60.0)
    assert sorted(calls) == [(500, -1), (500, -1)]


def test_one_failed_producer_fails_the_row_and_is_named(monkeypatch):
    outcomes = iter([(_result(10, 1.0, 1.0), "", 1.0, ""), (None, "exit 1; boom", 1.0, "")])
    monkeypatch.setattr(run, "run_producer", lambda *a, **k: next(outcomes))
    result, note, _wall, _output = run.run_producers("vtop", 2, 100, 100, -1, 60.0)
    # Which thread drew the failing outcome is up to the scheduler; the note names whichever it was.
    assert result is None and " of 2: exit 1; boom" in note


def test_a_rate_too_small_to_share_is_a_failed_row_not_a_zero_rate(monkeypatch):
    monkeypatch.setattr(run, "run_producer", lambda *a, **k: pytest.fail("ran a producer at 0 records/s"))
    result, note, _wall, _output = run.run_producers("vtop", 16, 1000, 100, 10, 60.0)
    assert result is None and "cannot be shared by 16 producers" in note


# -------------------------------------------------------------- the counters


def test_the_counters_are_parsed_from_the_nodes_text():
    parsed = run.parse_counters(metrics(requests=12, commits=4, overlapped=3, fsync=1.25, replicate=2.5))
    assert parsed == {
        "vtop_broker_group_commit_requests_total": 12.0,
        "vtop_broker_group_commits_total": 4.0,
        "vtop_broker_group_commit_overlapped_total": 3.0,
        'vtop_broker_produce_phase_seconds_total{phase="local_fsync"}': 1.25,
        'vtop_broker_produce_phase_seconds_total{phase="replicate"}': 2.5,
        'vtop_broker_produce_phase_seconds_total{phase="commit"}': 0.5,
    }


def test_a_counter_that_went_backwards_is_left_out_not_recorded_negative():
    before = {"a": 10.0, "b": 5.0, "c": 1.0}
    assert run.counter_deltas(before, {"a": 15.0, "b": 2.0}) == {"a": 5.0}


def _ctx(tmp_path, gcs):
    csv_path = str(tmp_path / "results.csv")
    schema.write_header(csv_path)
    env = {"comparable": {"vtop_commit": "a" * 40}, "observed": {}, "events": []}
    return {"run_id": "r", "run_dir": str(tmp_path), "csv": csv_path, "env": env, "fingerprint": "f" * 16,
            "vtop_image": "vtop-bench:x", "reps": 2, "sat_records": {100: 1000}, "fixed_seconds": 1.0,
            "smoke_records": 1, "sat_timeout": 5.0, "consume": False, "group_commit_configs": gcs}


def _fake_cluster(monkeypatch, scrapes):
    monkeypatch.setattr(run, "cluster_up", lambda system, image, leader_config=None: None)
    monkeypatch.setattr(run, "cluster_down", lambda *a, **k: None)
    monkeypatch.setattr(run, "smoke", lambda *a, **k: None)
    monkeypatch.setattr(run, "_save_leader_log", lambda *a, **k: None)
    monkeypatch.setattr(run, "client", lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""))
    monkeypatch.setattr(run, "scrape_leader_metrics", lambda system: next(scrapes))
    monkeypatch.setattr(run, "run_producer",
                        lambda system, records, size, rate, timeout, info_logging=False:
                        (_result(records, 500.0, 2.0), "", 1.0, ""))


def test_each_saturation_repetition_records_what_the_counters_moved_by(monkeypatch, tmp_path):
    scrapes = iter([
        metrics(),  # the gauge check after the cluster came up
        metrics(requests=0, commits=0), metrics(requests=40, commits=10, overlapped=4, fsync=1.0, replicate=2.0),
        metrics(requests=40, commits=10, overlapped=4, fsync=1.0, replicate=2.0),
        metrics(requests=100, commits=20, overlapped=9, fsync=1.5, replicate=4.0),
    ])
    _fake_cluster(monkeypatch, scrapes)
    ctx = _ctx(tmp_path, {"on": run.leader_config(LEADER, "on", {"enabled"})})
    run.run_cell(ctx, "vtop", 100, {25: 40, 50: 80, 75: 120}, group_commit="on", producers=4)
    entries = ctx["env"]["observed"]["vtop_produce_counters"]
    assert [(e["mode"], e["record_size"], e["producers"], e["group_commit"], e["repetition"]) for e in entries] \
        == [("saturation", 100, 4, "on", 1), ("saturation", 100, 4, "on", 2)]
    assert entries[0]["deltas"]["vtop_broker_group_commit_requests_total"] == 40.0
    assert entries[1]["deltas"]["vtop_broker_group_commits_total"] == 10.0
    assert entries[1]["deltas"]['vtop_broker_produce_phase_seconds_total{phase="replicate"}'] == 2.0
    with open(ctx["csv"], newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["producers"] for row in rows} == {"4"}
    assert {row["lat_basis"] for row in rows if row["status"] == "ok"} == {"worst-of-4"}


def test_a_missed_scrape_is_an_event_and_the_run_goes_on(monkeypatch, tmp_path):
    def flaky():
        yield metrics()  # the gauge check
        while True:
            yield None

    source = flaky()

    def scrape(_system):
        text = next(source)
        if text is None:
            raise RuntimeError("docker exec ... exited 1: curl failed")
        return text

    _fake_cluster(monkeypatch, iter([]))
    monkeypatch.setattr(run, "scrape_leader_metrics", scrape)
    ctx = _ctx(tmp_path, {"on": run.leader_config(LEADER, "on", {"enabled"})})
    run.run_cell(ctx, "vtop", 100, {25: 40, 50: 80, 75: 120}, group_commit="on")
    assert "vtop_produce_counters" not in ctx["env"]["observed"]
    assert any("produce counters not read" in event["event"] for event in ctx["env"]["events"])
    with open(ctx["csv"], newline="") as handle:
        assert {row["status"] for row in csv.DictReader(handle)} == {"ok"}


def test_kafka_and_an_unaxised_vtop_read_no_counters(monkeypatch, tmp_path):
    _fake_cluster(monkeypatch, iter([]))
    monkeypatch.setattr(run, "scrape_leader_metrics", lambda system: pytest.fail("scraped"))
    monkeypatch.setattr(run, "_observe_kafka", lambda ctx: ctx["env"]["observed"].update(kafka_topic=""))
    ctx = _ctx(tmp_path, {})
    run.run_cell(ctx, "kafka", 100, None, producers=2)
    run.run_cell(ctx, "vtop", 100, {25: 40, 50: 80, 75: 120}, producers=2)


# ---------------------------------------------------------------- the report


def _sweep_rows(env):
    rows = complete_rows(env)
    for row in rows:
        if row["status"] == "ok" and row.get("lat_basis"):
            row["lat_basis"] = schema.expected_lat_basis(str(row["producers"]))
        if row["system"] == "kafka":
            row["records_per_sec"] = 30000.0
        elif row["mode"] == "saturation":
            gained = 1.0 if row["producers"] == 1 else 2.0
            row["records_per_sec"] = 1000.0 * (gained if row["group_commit"] == "on" else 1.0)
        if row.get("records_per_sec") is not None:
            row["mib_per_sec"] = row["records_per_sec"] * row["record_size"] / 2**20
        if row["mode"] == "fixed_rate":
            row["offered_rate"] = 7500
            row["sustained"] = "true"
    return rows


def _counters(size, producers, setting, rep, requests, commits, overlapped, fsync, replicate, commit):
    return {"mode": "saturation", "record_size": size, "producers": producers, "group_commit": setting,
            "repetition": rep, "deltas": {
                "vtop_broker_group_commit_requests_total": requests,
                "vtop_broker_group_commits_total": commits,
                "vtop_broker_group_commit_overlapped_total": overlapped,
                'vtop_broker_produce_phase_seconds_total{phase="local_fsync"}': fsync,
                'vtop_broker_produce_phase_seconds_total{phase="replicate"}': replicate,
                'vtop_broker_produce_phase_seconds_total{phase="commit"}': commit}}


def sweep_environment():
    env = axis_environment(SWEEP)
    counters = []
    for rep in (1, 2, 3):
        counters.append(_counters(100, 1, "on", rep, 100, 100, 0, 1.0, 2.0, 0.1))
        # 2 producers: 600 requests in 200 groups over the three repetitions, half of them overlapped.
        counters.append(_counters(100, 2, "on", rep, 200, 66 + (rep <= 2), 33 + (rep == 1), 1.0, 2.0, 0.1))
        counters.append({"mode": "saturation", "record_size": 100, "producers": 2, "group_commit": "off",
                         "repetition": rep, "deltas": {
                             'vtop_broker_produce_phase_seconds_total{phase="local_fsync"}': 3.0}})
    env["observed"]["vtop_produce_counters"] = counters
    return env


@pytest.fixture
def swept(tmp_path):
    # Drawing needs matplotlib, which only the benchmark-charts job installs; it runs these there.
    pytest.importorskip("matplotlib")
    env = sweep_environment()
    directory = write_run(str(tmp_path / "run"), env, _sweep_rows(env))
    docs = tmp_path / "docs"
    report.build(directory, str(docs / "img"), str(docs / "kafka-comparison.md"))
    text = (docs / "kafka-comparison.md").read_text()
    return text, text.split("## Group commit ablation")[1].split("\n## ")[0]


def test_the_ablation_has_a_row_per_producer_count_with_the_counters(swept):
    _text, section = swept
    # 1 producer: one request a group, no overlap; 3 x 0.1 s of commit over 300 requests is 1 ms each.
    assert "| 100 B | 1 | " in section and "| 1.00x | 1.00 | 0% | 10.00 / 20.00 / 1.00 |" in section
    # 2 producers: 600 requests in 200 groups, 100 of them overlapped; 3 s of fsync over 600 is 5 ms each.
    assert "| 100 B | 2 | " in section and "| 2.00x | 3.00 | 50% | 5.00 / 10.00 / 0.50 |" in section


def test_the_charts_and_main_tables_are_at_one_producer(swept):
    text, _section = swept
    main = text.split("## Group commit ablation")[0]
    assert "every table but the group commit ablation are at 1 producer" in main
    # The 2-producer saturation (2000 records/s on) is in the ablation only, never pooled into a median.
    assert "2,000" not in main


def test_the_report_says_what_one_producer_and_several_partitions_cannot_show(swept):
    _text, section = swept
    assert "One producer cannot show group commit." in section
    assert "producer turnstile" in section and "answers a Produce request before it" in section
    assert "Partitions do not merge." in section
    assert "Pipelining one producer's requests is not built." in section


def test_a_run_with_several_producers_and_no_ablation_is_still_refused(tmp_path):
    matrix = {**SWEEP, "group_commits": ["on"]}
    env = axis_environment(matrix)
    loaded = schema.load_run(write_run(str(tmp_path / "run"), env, _sweep_rows(env)))
    with pytest.raises(schema.ReportRefused, match="values of producers"):
        report.check_drawable(loaded.rows, loaded.environment)


def test_a_malformed_counters_entry_is_refused(tmp_path):
    env = sweep_environment()
    env["observed"]["vtop_produce_counters"][0]["deltas"]["vtop_broker_group_commits_total"] = -1
    directory = write_run(str(tmp_path / "run"), env, _sweep_rows(env))
    with pytest.raises(schema.ReportRefused, match="malformed"):
        report.build(directory, str(tmp_path / "img"), str(tmp_path / "doc.md"))


def test_the_counters_are_not_part_of_the_fingerprint():
    env = sweep_environment()
    bare = copy.deepcopy(env)
    bare["observed"] = {}
    assert schema.fingerprint(env) == schema.fingerprint(bare)


def test_the_parser_help_names_the_axis():
    assert "--producers" in run.build_parser().format_help()
    assert isinstance(run.build_parser().parse_args(["--producers", "1,2"]), argparse.Namespace)
