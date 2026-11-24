"""`run.py --producers N` and `--partitions N` (#596's axes, #752's ablation at more than one producer).

Every row used to be 1 partition and 1 producer: `run_cell` hard-coded both and `build_matrix` wrote `[1]`. One
idempotent producer's appends are serialised in VTOP's gateway, so group commit had nothing to merge.

- Producers: N concurrent `kafka-producer-perf-test` containers, each its own idempotent producer, each offered
  `rate / N` and sent `records / N`. The row sums their records and rates and carries the worst of their
  percentiles, `lat_basis = worst-of-N`.
- Partitions: Kafka's topic is created with N partitions and the smoke reads every one. VTOP's gateway serves
  one partition per node process, one range each, so N partitions is N replicated ranges, 3N processes, which
  this harness's three single-process nodes at Kafka's per-node limits do not hold: VTOP's rows at N > 1 are
  `refused` rows saying so.
- The end-to-end stage is measured at 1 partition and 1 producer only; a run of another shape has none.
- One value of each per run (a sweep is several runs), recorded in the matrix's existing keys: schema 4 holds.
"""
import csv
import os
import subprocess

import kafka_compare_stub as stub
import pytest

from kafka_compare import plan, run, schema

RATES = {25: 75_000, 50: 150_000, 75: 225_000}
HERE_FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def rows_of(path):
    with open(path, newline="") as handle:
        return list(csv.DictReader(handle))


def args(*extra):
    return run.build_parser().parse_args(["--sizes", "100", "--sat-records", "100:30000", *extra])


@pytest.mark.parametrize("flag", ["--producers", "--partitions"])
def test_the_matrix_records_the_one_value_asked_for(flag):
    a = args(flag, "4")
    matrix = run.build_matrix(a, *run.check_args(a))
    key = flag.lstrip("-")
    assert matrix[key] == [4]
    assert matrix["partitions" if key == "producers" else "producers"] == [1]


@pytest.mark.parametrize("flag", ["--producers", "--partitions"])
@pytest.mark.parametrize("value", ["0", "-1"])
def test_a_count_below_one_is_refused(flag, value):
    with pytest.raises(SystemExit, match=f"{flag} {value}"):
        run.check_args(args(flag, value))


@pytest.mark.parametrize("flag", ["--producers", "--partitions"])
def test_a_run_of_another_shape_has_no_end_to_end_stage(flag):
    a = args(flag, "2")
    assert "e2e" not in run.build_matrix(a, *run.check_args(a))
    one = args()
    assert "e2e" in run.build_matrix(one, *run.check_args(one))


def test_the_plan_gives_the_end_to_end_mode_to_the_one_partition_one_producer_cell_only():
    matrix = {"record_sizes": [100], "partitions": [1, 4], "producers": [1, 2], "acks": ["all"], "repetitions": 1,
              "load_pcts": [50], "consume": False, "e2e": {"load_basis": "common", "load_pct": 50}}
    modes = {(cell.partitions, cell.producers): cell.modes for cell in plan.axes(matrix).cells}
    assert modes[(1, 1)] == ("saturation", "fixed_rate", "e2e")
    assert modes[(4, 2)] == modes[(1, 2)] == modes[(4, 1)] == ("saturation", "fixed_rate")
    assert {(key[3], key[4]) for key in plan.expected_keys(matrix) if key[1] == "e2e"} == {(1, 1)}


def test_n_producers_share_the_records_and_the_rate_and_the_row_is_their_sum(monkeypatch, tmp_path):
    calls = stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr)
    ctx = stub.ctx_for(run, str(tmp_path / "run"), producers=4, fixed_seconds=30.0)
    run.run_cell(ctx, "kafka", 100, None)
    saturation = [call for call in calls["producer"] if call[2] < 0][-4:]
    assert saturation == [("kafka", 7_500, -1)] * 4  # 30000 records, 4 producers, unthrottled
    # Four stub producers saturate together at 1.2 M records/s, so the 25 % leg offers 300k/s for 30 s: 9 M records.
    first_leg = [call for call in calls["producer"] if call[2] > 0][:4]
    assert first_leg == [("kafka", 2_250_000, 75_000)] * 4
    rows = rows_of(ctx["csv"])
    assert {row["producers"] for row in rows} == {"4"}
    sat = next(row for row in rows if row["mode"] == "saturation")
    # Each stub producer saturates at 300k records/s and sends 6000 steady-state records (7500 less warm-up).
    assert (sat["records"], float(sat["records_per_sec"]), sat["lat_basis"]) == ("24000", 1_200_000.0, "worst-of-4")


def test_the_latency_of_n_producers_is_the_worst_of_their_percentiles(monkeypatch, tmp_path):
    stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr)
    real = run.run_producer
    seen = []

    def producer(system, records, size, rate, timeout, info_logging=False):
        result, note, wall, output = real(system, records, size, rate, timeout)
        seen.append(1)
        slow = len(seen) % 3 == 0  # one producer in three is slower at every percentile
        return result._replace(lat_p50_ms=result.lat_p50_ms * (10 if slow else 1),
                               lat_p999_ms=result.lat_p999_ms * (10 if slow else 1)), note, wall, output

    monkeypatch.setattr(run, "run_producer", producer)
    ctx = stub.ctx_for(run, str(tmp_path / "run"), producers=3)
    run.run_cell(ctx, "kafka", 100, None)
    sat = next(row for row in rows_of(ctx["csv"]) if row["mode"] == "saturation")
    assert (float(sat["lat_p50_ms"]), float(sat["lat_p999_ms"]), sat["lat_basis"]) == (20.0, 50.0, "worst-of-3")


def test_one_failed_producer_fails_the_row_naming_it(monkeypatch, tmp_path):
    stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr)
    real = run.run_producer
    seen = []

    def producer(system, records, size, rate, timeout, info_logging=False):
        seen.append(1)
        if rate > 0 and len(seen) % 2 == 0:
            return None, "exit 1; org.apache.kafka.common.errors.TimeoutException", 1.0, ""
        return real(system, records, size, rate, timeout)

    monkeypatch.setattr(run, "run_producer", producer)
    ctx = stub.ctx_for(run, str(tmp_path / "run"), producers=2)
    run.run_cell(ctx, "kafka", 100, None)
    fixed = [row for row in rows_of(ctx["csv"]) if row["mode"] == "fixed_rate"]
    assert {row["status"] for row in fixed} == {"failed"}
    assert "producer 2 of 2: exit 1; org.apache.kafka.common.errors.TimeoutException" in fixed[0]["note"]


def test_the_producers_split_a_total_exactly():
    assert run.split_evenly(10, 3) == [4, 3, 3]
    assert sum(run.split_evenly(225_000, 4)) == 225_000


def test_kafka_s_topic_is_created_with_the_partitions_asked_for(monkeypatch):
    commands = []
    monkeypatch.setattr(run, "compose", lambda *a, **k: None)
    monkeypatch.setattr(run, "client", lambda system, command, **k: commands.append(command))
    run.cluster_up("kafka", "vtop-bench:x", partitions=4)
    [create] = commands
    assert create[create.index("--partitions") + 1] == "4"


def test_the_smoke_reads_every_partition_and_checks_each_one_s_order(monkeypatch):
    records = run.smoke_records(6, 40)
    by_partition = {0: [records[0], records[3]], 1: [records[1], records[4]], 2: [records[2], records[5]]}
    reads = []

    def client(system, command, **kwargs):
        if "kafka-console-consumer.sh" in command[0]:
            partition = int(command[command.index("--partition") + 1])
            reads.append(partition)
            return subprocess.CompletedProcess(command, 0, "".join(r + "\n" for r in by_partition[partition]), "")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(run, "client", client)
    run.smoke("kafka", 6, 40, 3)
    assert reads == [0, 1, 2]
    by_partition[1].reverse()
    with pytest.raises(RuntimeError, match="partition 1 holds the smoke's records out of the order produced"):
        run.smoke("kafka", 6, 40, 3)
    by_partition[1] = [records[1]]
    with pytest.raises(RuntimeError, match="read back 5 records over 3 partitions, produced 6"):
        run.smoke("kafka", 6, 40, 3)


def test_vtop_at_more_than_one_partition_is_refused_rows_and_no_cluster(monkeypatch, tmp_path):
    calls = stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr)
    ctx = stub.ctx_for(run, str(tmp_path / "run"), partitions=2)
    assert run.run_cell(ctx, "vtop", 100, RATES) is None
    assert calls["up"] == [] and calls["producer"] == []
    rows = rows_of(ctx["csv"])
    assert {(row["status"], row["partitions"]) for row in rows} == {("refused", "2")}
    assert "one partition per node process" in rows[0]["note"]
    assert "kafka.partitions" in rows[0]["note"]


@pytest.mark.parametrize("flags, basis", [(["--producers", "2"], "worst-of-2"), (["--partitions", "3"], "single")])
def test_a_whole_run_of_either_shape_is_complete_and_loads_as_schema_4(monkeypatch, tmp_path, flags, basis):
    stub.install(run, str(tmp_path), set_attr=monkeypatch.setattr)
    assert run.main(stub.ARGS + flags + ["--run-id", "shape"]) == 0
    directory = os.path.join(str(tmp_path), "results", "shape")
    assert run.check(directory) == 0
    loaded = schema.load_run(directory)
    assert loaded.environment["schema_version"] == 4
    assert not any(row["mode"] == "e2e" for row in loaded.rows)
    ok = [row for row in loaded.rows if row["status"] == "ok" and row["mode"] != "consume"]
    assert ok and {row["lat_basis"] for row in ok} == {basis}


@pytest.mark.parametrize("flags, said", [
    (["--producers", "4"], "4 producers, worst of their percentiles"),
    (["--partitions", "3"], "VTOP at 3 partitions is not measured"),
])
def test_the_report_says_how_a_run_of_another_shape_was_combined_or_left_out(monkeypatch, tmp_path, flags, said):
    from kafka_compare import report

    stub.install(run, str(tmp_path), set_attr=monkeypatch.setattr)
    assert run.main(stub.ARGS + flags + ["--run-id", "shape"]) == 0
    gaps = report.known_gaps(schema.load_run(os.path.join(str(tmp_path), "results", "shape")).environment)
    assert said in gaps
    assert "One gateway, one partition" not in gaps or "--producers" in flags


def test_a_one_partition_one_producer_report_is_unchanged(monkeypatch, tmp_path):
    from kafka_compare import report

    gaps = report.known_gaps(schema.load_run(os.path.join(HERE_FIXTURES, "kafka-schema4-runner")).environment)
    assert "worst of their percentiles" not in gaps and "is not measured." not in gaps
