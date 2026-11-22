"""Schema 3 of the Kafka comparison's results (#596, #600): end-to-end latency histograms, CPU and bytes written,
and the durability profile's latency cost.

Schema 3 adds four columns to results.csv (`delivered_bytes`, `cpu_seconds`, `disk_write_bytes`,
`disk_read_bytes`), a sidecar `e2e_hist.csv` that holds each end-to-end row's 1 ms latency histogram, and two
matrix keys (`resources`, and `e2e.extra_profiles`, the profiles Kafka alone is also measured under at the
common offered rate). Every rule the report relies on is enforced on load. Runs written before it, schema 1
and schema 2, load unchanged.
"""
import copy
import csv
import os

import pytest

from kafka_compare import e2e, plan, schema
from test_kafka_compare_schema_v2 import PHASE_1, complete_rows, environment, write_run

V3 = {
    "record_sizes": [100], "partitions": [1], "producers": [1], "compressions": ["none"], "acks": ["all"],
    "profiles": ["defaults"], "repetitions": 2, "load_pcts": [25, 50, 75], "load_bases": {"kafka": [25, 50, 75]},
    "consume": True, "e2e": {"load_basis": "common", "load_pct": 50, "extra_profiles": ["fsync-parity"]},
    "resources": True, "smoke_records": 10,
}
HIST = {1: 500, 2: 400, 10: 99, 100: 1}  # 1000 records
KEY_COLUMNS = ("system", "record_size", "partitions", "producers", "compression", "acks", "profile",
               "repetition", "load_basis", "load_pct")


def env3(matrix=V3):
    env = environment(matrix)
    env["schema_version"] = 3
    env["comparable"]["host_disk"] = "Test SSD, SSD, NVMe, ext4"
    return env


def rows3(env):
    rows = complete_rows(env)
    for row in rows:
        if env["matrix"].get("resources") and row["mode"] in ("saturation", "fixed_rate", "consume", "e2e"):
            row.update(delivered_bytes=100_000, cpu_seconds=2.5, disk_write_bytes=300_000, disk_read_bytes=0,
                       window_start_ms=1000, window_end_ms=21000)
        if row["mode"] == "e2e":
            row.update({column: e2e.percentile_ms(HIST, *fraction) for column, fraction in e2e.PERCENTILES.items()})
    return rows


def write_hist(directory, rows):
    path = os.path.join(directory, schema.E2E_HIST)
    schema.write_header(path, schema.E2E_HIST_COLUMNS)
    for row in rows:
        if row["mode"] == "e2e" and row["status"] == "ok":
            for bucket, count in HIST.items():
                schema.append_row(path, {**{k: row[k] for k in KEY_COLUMNS}, "bucket_ms": bucket, "count": count},
                                  schema.E2E_HIST_COLUMNS)


def make(tmp_path, env=None, rows=None, hist=True):
    env = env or env3()
    rows = rows if rows is not None else rows3(env)
    directory = write_run(str(tmp_path / "run"), env, rows, schema.COLUMNS)
    if hist:
        write_hist(directory, rows)
    return directory


@pytest.fixture
def run_dir(tmp_path):
    return make(tmp_path)


def refused(directory, match):
    with pytest.raises(schema.ReportRefused, match=match):
        schema.load_run(directory)


def rewrite(directory, mutate, name="results.csv"):
    path = os.path.join(directory, name)
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames
        rows = list(reader)
    rows = mutate(rows)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def first(rows, **criteria):
    return next(row for row in rows if all(row[k] == v for k, v in criteria.items()))


# ------------------------------------------------------------------------------------------------ layout


def test_v3_adds_four_columns_after_the_window_and_keeps_the_v2_layout_as_it_was():
    assert schema.SCHEMA_VERSION == 3
    new = ("delivered_bytes", "cpu_seconds", "disk_write_bytes", "disk_read_bytes")
    at = schema.COLUMNS_V2.index("window_end_ms") + 1
    assert schema.COLUMNS == schema.COLUMNS_V2[:at] + new + schema.COLUMNS_V2[at:]
    assert schema.COLUMNS_V2[-6:] == ("lat_basis", "started_at", "window_start_ms", "window_end_ms",
                                      "wall_seconds", "note")
    assert schema.KEY == ("system", "mode", "record_size", "partitions", "producers", "compression", "acks",
                          "profile", "repetition", "load_basis", "load_pct")


def test_a_complete_v3_run_loads_with_its_histograms(run_dir):
    loaded = schema.load_run(run_dir)
    assert loaded.version == 3
    ok_e2e = [row for row in loaded.rows if row["mode"] == "e2e" and row["status"] == "ok"]
    assert len(ok_e2e) == 6  # 2 repetitions x (Kafka and VTOP under defaults, Kafka under fsync-parity)
    assert set(loaded.hist) == {schema.row_key(row) for row in ok_e2e}
    assert all(hist == HIST for hist in loaded.hist.values())


def test_an_extra_profile_is_measured_for_kafka_only_and_only_in_e2e():
    keys = schema.expected_keys(V3)
    extra = {key for key in keys if key[7] == "fsync-parity"}
    assert extra == {("kafka", "e2e", 100, 1, 1, "none", "all", "fsync-parity", rep, "common", "50")
                     for rep in (1, 2)}
    assert {key for key in keys if key[7] == "defaults" and key[1] == "e2e"} == {
        (system, "e2e", 100, 1, 1, "none", "all", "defaults", rep, "common", "50")
        for system in ("kafka", "vtop") for rep in (1, 2)}


def test_the_plan_refuses_an_extra_profile_it_cannot_run():
    for bad, match in ((["defaults"], "already"), (["nope"], "not a list"), ("fsync-parity", "not a list"),
                       ([], "not a list")):
        matrix = copy.deepcopy(V3)
        matrix["e2e"]["extra_profiles"] = bad
        with pytest.raises(plan.PlanRefused, match=match):
            plan.check_matrix(matrix)
    matrix = copy.deepcopy(V3)
    matrix["resources"] = "yes"
    with pytest.raises(plan.PlanRefused, match="resources"):
        plan.check_matrix(matrix)
    plan.check_matrix(V3)


def test_the_extra_profile_and_the_resource_windows_are_charged_to_the_projection():
    bare = copy.deepcopy(V3)
    del bare["resources"]
    del bare["e2e"]["extra_profiles"]
    assert plan.projected_seconds(V3) > plan.projected_seconds(bare)


# ------------------------------------------------------------------------------------- old runs still load


def test_a_v2_csv_loads_with_the_new_columns_blank(tmp_path):
    env = environment(copy.deepcopy(V3))
    env["matrix"].pop("resources")
    env["matrix"]["e2e"].pop("extra_profiles")
    rows = complete_rows(env)
    directory = write_run(str(tmp_path / "v2"), env, rows, schema.COLUMNS_V2)
    write_hist(directory, rows)
    loaded = schema.load_run(directory)
    assert loaded.version == 2
    assert {row[c] for row in loaded.rows for c in ("delivered_bytes", "cpu_seconds", "disk_write_bytes",
                                                    "disk_read_bytes")} == {""}


def test_the_committed_phase_1_run_still_loads_as_schema_1_with_no_histograms():
    loaded = schema.load_run(PHASE_1)
    assert loaded.version == 1 and loaded.hist == {}
    assert {row["delivered_bytes"] for row in loaded.rows} == {""}


# --------------------------------------------------------------------------- the versions agree with each other


def test_the_new_matrix_keys_need_schema_3_in_environment_json(tmp_path):
    for mutate in (lambda e: e["matrix"].pop("resources"), lambda e: e["matrix"]["e2e"].pop("extra_profiles")):
        env = env3()
        mutate(env)
        env["schema_version"] = 2
        directory = make(tmp_path / str(id(mutate)), env, rows3(env), hist=True)
        refused(directory, "schema 3|schema_version 2")


def test_a_v3_csv_needs_a_schema_3_environment_and_the_reverse(tmp_path):
    env = env3()
    env["schema_version"] = 2
    refused(make(tmp_path / "a", env), "schema_version 2")
    env = env3({**V3, "resources": False})
    rows = rows3(env)
    directory = write_run(str(tmp_path / "b" / "run"), env, rows, schema.COLUMNS_V2)
    write_hist(directory, rows)
    refused(directory, "schema_version 3")


def test_a_schema_4_environment_is_newer_than_this_report(tmp_path):
    env = env3()
    env["schema_version"] = 4
    refused(make(tmp_path, env), "schema_version 4")


# ---------------------------------------------------------------------------------------------- resources


def test_a_resources_run_must_carry_the_four_columns_on_every_ok_measured_row(run_dir):
    def blank_one(rows):
        first(rows, mode="saturation", system="kafka", repetition="1")["cpu_seconds"] = ""
        return rows

    rewrite(run_dir, blank_one)
    refused(run_dir, "cpu_seconds")


def test_a_run_that_did_not_ask_for_resources_must_leave_them_blank(tmp_path):
    env = env3()
    rows = rows3(env)
    env["matrix"]["resources"] = False
    refused(make(tmp_path, env, rows), "resources")


@pytest.mark.parametrize("column,value", [("cpu_seconds", "-1"), ("cpu_seconds", "nan"), ("delivered_bytes", "0"),
                                          ("disk_write_bytes", "1.5"), ("disk_read_bytes", "-3")])
def test_a_resource_figure_that_cannot_be_one_is_refused(run_dir, column, value):
    def poison(rows):
        first(rows, mode="consume", system="vtop", repetition="1")[column] = value
        return rows

    rewrite(run_dir, poison)
    refused(run_dir, column)


def test_a_resource_row_without_its_window_is_refused(run_dir):
    def lose_window(rows):
        row = first(rows, mode="saturation", system="vtop", repetition="2")
        row["window_start_ms"] = row["window_end_ms"] = ""
        return rows

    rewrite(run_dir, lose_window)
    refused(run_dir, "window")


def test_a_failed_row_carries_no_resources(run_dir):
    def fail(rows):
        row = first(rows, mode="saturation", system="kafka", repetition="1")
        row["status"], row["note"] = "failed", "timed out"
        return rows

    rewrite(run_dir, fail)
    refused(run_dir, "delivered_bytes")


# ------------------------------------------------------------------------------------------ histograms


def test_a_missing_histogram_file_is_refused(tmp_path):
    refused(make(tmp_path, hist=False), "e2e_hist.csv")


def test_a_histogram_that_does_not_sum_to_the_rows_records_is_refused(run_dir):
    def short(rows):
        rows[0]["count"] = str(int(rows[0]["count"]) - 1)
        return rows

    rewrite(run_dir, short, schema.E2E_HIST)
    refused(run_dir, "sums to 999")


def test_a_row_whose_percentiles_disagree_with_its_histogram_is_refused(run_dir):
    def disagree(rows):
        first(rows, mode="e2e", system="vtop", repetition="1")["lat_p99_ms"] = "3.0"
        return rows

    rewrite(run_dir, disagree)
    refused(run_dir, "lat_p99_ms")


def test_a_histogram_for_a_row_that_is_not_an_ok_e2e_row_is_refused(run_dir):
    def stray(rows):
        rows.append({**rows[0], "system": "vtop", "repetition": "9"})
        return rows

    rewrite(run_dir, stray, schema.E2E_HIST)
    refused(run_dir, "no ok e2e row")


@pytest.mark.parametrize("bucket,count,match", [("60001", "1", "bucket_ms"), ("-1", "1", "bucket_ms"),
                                                ("1.5", "1", "bucket_ms"), ("5", "0", "count")])
def test_a_malformed_histogram_line_is_refused(run_dir, bucket, count, match):
    def poison(rows):
        rows[0]["bucket_ms"], rows[0]["count"] = bucket, count
        return rows

    rewrite(run_dir, poison, schema.E2E_HIST)
    refused(run_dir, match)


def test_a_repeated_bucket_is_refused(run_dir):
    def repeat(rows):
        rows.append(dict(rows[0]))
        return rows

    rewrite(run_dir, repeat, schema.E2E_HIST)
    refused(run_dir, "twice")


def test_a_run_with_no_e2e_in_its_matrix_has_no_histogram_file(tmp_path):
    env = env3()
    env["matrix"]["e2e"] = None
    env["matrix"]["resources"] = False
    rows = [row for row in rows3(env) if row["mode"] != "e2e"]
    directory = make(tmp_path, env, rows, hist=False)
    assert schema.load_run(directory).hist == {}
    write_hist(directory, complete_rows(env3()))
    refused(directory, "e2e_hist.csv")
