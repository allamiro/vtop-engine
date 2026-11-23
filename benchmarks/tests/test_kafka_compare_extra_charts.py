"""The arithmetic and the refusals behind the end-to-end CDF, the resource charts and the profile latency chart
(#596, #600): pure functions over a loaded schema-4 run, so nothing here needs matplotlib. The images themselves
are in test_kafka_compare_extra_report.py."""
import os

import pytest
from test_kafka_compare_schema_v4 import V3, env3, make, rows3

from kafka_compare import colours, e2e, extra_charts, report, run_outputs, schema

CONFIG_HISTS = {
    ("kafka", "defaults"): {1: 900, 2: 90, 5: 10},
    ("kafka", "fsync-parity"): {2: 100, 4: 700, 8: 190, 30: 10},
    ("vtop", "defaults"): {6: 300, 9: 600, 20: 90, 50: 10},
}


def hist_for(row):
    return CONFIG_HISTS.get((row["system"], row["profile"]), CONFIG_HISTS[(row["system"], "defaults")])


def report_matrix():
    return {**V3, "record_sizes": [100, 1024], "sat_records": {"100": 1000, "1024": 1000}, "fixed_seconds": 30,
            "warmup_fraction": 0.2}


def report_run(tmp_path, mutate=None, matrix=None):
    """A complete schema-4 run whose end-to-end rows carry a different histogram per configuration."""
    from test_kafka_compare_report import COMPARABLE

    env = env3(matrix or report_matrix())
    env["comparable"] = {**COMPARABLE, "host_disk": "Test SSD, SSD, NVMe, ext4", "vtop_group_commit": {"settings": {}}}
    env.update(started_at="2026-11-09T00:00:00+00:00", finished_at="2026-11-09T01:00:00+00:00",
               other_containers_at_start_count=0)
    rows = rows3(env)
    for row in rows:
        if row["mode"] == "e2e":
            hist = hist_for(row)
            row.update(records=sum(hist.values()), records_produced=sum(hist.values()),
                       **{c: e2e.percentile_ms(hist, *f) for c, f in e2e.PERCENTILES.items()})
        if row["mode"] == "saturation" and env["matrix"].get("resources"):
            row.update(cpu_seconds=4.0 if row["system"] == "kafka" else 9.0, delivered_bytes=2**30,
                       disk_write_bytes=3 * 2**30 if row["system"] == "kafka" else 0)
    if mutate:
        mutate(env, rows)
    fp = schema.fingerprint(env)
    for row in rows:
        row["env_fingerprint"] = fp
    directory = make(tmp_path, env, rows, hist=False)
    path = os.path.join(directory, schema.E2E_HIST)
    schema.write_header(path, schema.E2E_HIST_COLUMNS)
    for row in rows:
        if row["mode"] == "e2e" and row["status"] == "ok":
            for bucket, count in hist_for(row).items():
                schema.append_row(path, {**{k: row[k] for k in schema.E2E_HIST_KEY}, "bucket_ms": bucket,
                                         "count": count}, schema.E2E_HIST_COLUMNS)
    return directory


# --------------------------------------------------------------------------------------- the arithmetic


def test_a_cdf_is_the_running_share_of_records_at_or_under_each_bucket():
    assert extra_charts.cdf({1: 500, 2: 400, 10: 99, 100: 1}) == [(1, 0.5), (2, 0.9), (10, 0.999), (100, 1.0)]
    assert extra_charts.cdf({7: 3}) == [(7, 1.0)]
    with pytest.raises(ValueError, match="empty"):
        extra_charts.cdf({})


def test_a_cdfs_quantile_is_the_one_the_histogram_gives_the_row():
    hist = {1: 500, 2: 400, 10: 99, 100: 1}
    for column, (num, den) in e2e.PERCENTILES.items():
        want = e2e.percentile_ms(hist, num, den)
        first_at_or_over = next(ms for ms, share in extra_charts.cdf(hist) if share >= num / den)
        assert first_at_or_over == want, column


def test_repetitions_are_pooled_by_adding_their_counts():
    assert extra_charts.pooled([{1: 2, 5: 1}, {1: 1, 9: 4}]) == {1: 3, 5: 1, 9: 4}


def test_cpu_seconds_per_gib_and_bytes_written_per_byte_delivered():
    row = {"cpu_seconds": "3", "delivered_bytes": str(2**29), "disk_write_bytes": str(3 * 2**29)}
    assert extra_charts.cpu_seconds_per_gib(row) == 6.0
    assert extra_charts.write_amplification(row) == 3.0
    assert extra_charts.write_amplification({**row, "disk_write_bytes": "0"}) == 0.0


def test_the_three_configurations_are_ordered_named_and_coloured_by_the_registry():
    assert extra_charts.CONFIGS == (("kafka", "defaults"), ("kafka", "fsync-parity"), ("vtop", "defaults"))
    assert [extra_charts.CONFIG_NAME[c] for c in extra_charts.CONFIGS] == ["kafka-defaults", "kafka-fsync-parity",
                                                                           "vtop-shipped"]
    style = extra_charts.config_style(("kafka", "fsync-parity"))
    assert style["color"] == colours.colour("kafka", "light")
    assert style["color"] == extra_charts.config_style(("kafka", "defaults"))["color"]
    # Two Kafka profiles share the hue, so a second channel tells them apart.
    assert (style["marker"], style["hatch"]) != (extra_charts.config_style(("kafka", "defaults"))["marker"],
                                                 extra_charts.config_style(("kafka", "defaults"))["hatch"])


# ------------------------------------------------------------------------------------ cells of a loaded run


def test_a_latency_cell_is_the_median_and_range_over_the_repetitions(tmp_path):
    run = schema.load_run(report_run(tmp_path))
    cell = extra_charts.e2e_cell(run.rows, "kafka", "defaults", 100, "lat_p99_ms")
    assert (cell["n"], cell["ok"], cell["sustained"], cell["offered"]) == (2, 2, True, 500)
    assert cell["summary"].median == e2e.percentile_ms(CONFIG_HISTS[("kafka", "defaults")], 99, 100)
    assert extra_charts.configs(run.rows) == [(*config, "") for config in extra_charts.CONFIGS]


def test_a_not_sustained_repetition_makes_the_cell_not_sustained(tmp_path):
    def slow(_env, rows):
        row = next(r for r in rows if r["mode"] == "e2e" and r["system"] == "vtop" and r["repetition"] == 2)
        row["sustained"] = "false"
        row["note"] = "offered rate not sustained"

    run = schema.load_run(report_run(tmp_path, slow))
    assert extra_charts.e2e_cell(run.rows, "vtop", "defaults", 100, "lat_p50_ms")["sustained"] is False
    assert extra_charts.e2e_cell(run.rows, "kafka", "defaults", 100, "lat_p50_ms")["sustained"] is True


def test_a_resource_cell_divides_each_repetition_before_taking_the_median(tmp_path):
    run = schema.load_run(report_run(tmp_path))
    cpu = extra_charts.resource_cell(run.rows, "kafka", "saturation", 100, "cpu_per_gib")
    assert (cpu["summary"].median, cpu["summary"].low, cpu["summary"].high) == (4.0, 4.0, 4.0)
    assert extra_charts.resource_cell(run.rows, "vtop", "saturation", 100, "cpu_per_gib")["summary"].median == 9.0
    # A system that wrote nothing is a zero, and it is a bar of height zero with its label, not a missing one.
    zero = extra_charts.resource_cell(run.rows, "vtop", "saturation", 100, "write_amp")
    assert zero["summary"].median == 0.0 and zero["ok"] == 2
    assert extra_charts.resource_cell(run.rows, "kafka", "saturation", 100, "write_amp")["summary"].median == 3.0


def test_a_run_says_which_of_the_three_new_things_it_has(tmp_path):
    env = schema.load_run(report_run(tmp_path)).environment
    assert extra_charts.has_e2e(env) and extra_charts.has_resources(env) and extra_charts.has_profile_cost(env)
    bare = report_matrix()
    bare.pop("resources")
    bare["e2e"] = {"load_basis": "common", "load_pct": 50}
    env = schema.load_run(report_run(tmp_path / "b", matrix=bare)).environment
    assert extra_charts.has_e2e(env) and not extra_charts.has_resources(env)
    assert not extra_charts.has_profile_cost(env)


# ----------------------------------------------------------------------------------------- what is drawn


def test_the_report_draws_e2e_rows_the_common_basis_and_kafkas_extra_profile(tmp_path):
    run = schema.load_run(report_run(tmp_path))
    report.check_drawable(run.rows, run.environment, run.hist)


def test_an_e2e_row_without_its_histogram_is_still_refused_by_name(tmp_path):
    run = schema.load_run(report_run(tmp_path))
    with pytest.raises(schema.ReportRefused, match="end-to-end"):
        report.check_drawable(run.rows, run.environment, {})


@pytest.mark.parametrize("change,match", [
    ({"partitions": [1, 6]}, "partitions"),
    ({"producers": [1, 4]}, "producers"),
    ({"compressions": ["none", "zstd"]}, "compression"),
    ({"profiles": ["defaults", "fsync-parity"]}, "profile"),
])
def test_every_other_undrawn_dimension_still_refuses_by_name(tmp_path, change, match):
    matrix = {**report_matrix(), **change}
    if "profiles" in change:
        matrix["e2e"] = {"load_basis": "common", "load_pct": 50}
        matrix["vtop_profiles_share_config"] = True
    run = schema.load_run(report_run(tmp_path, matrix=matrix))
    with pytest.raises(schema.ReportRefused, match=match):
        report.check_drawable(run.rows, run.environment, run.hist)


def test_a_non_default_profile_on_anything_but_kafkas_e2e_rows_is_refused(tmp_path):
    run = schema.load_run(report_run(tmp_path))
    rows = [dict(r) for r in run.rows]
    next(r for r in rows if r["mode"] == "saturation" and r["system"] == "kafka")["profile"] = "fsync-parity"
    with pytest.raises(schema.ReportRefused, match="profile"):
        report.check_drawable(rows, run.environment, run.hist)


# ----------------------------------------------------------------------------- the crash page's pairing


def test_report_json_may_name_the_latency_run_and_nothing_else_extra(tmp_path):
    def write(text):
        (tmp_path / "report.json").write_text(text)

    base = '"doc": "docs/benchmarks/kafka-crash.md", "img_dir": "docs/benchmarks/img/aws"'
    write("{" + base + ', "latency_run": "v0110-596"}')
    own = run_outputs.resolve(str(tmp_path), "/repo", "docs/benchmarks/img", allow_latency_run=True)
    assert own.latency_run == "v0110-596"
    write("{" + base + "}")
    assert run_outputs.resolve(str(tmp_path), "/repo", "docs/benchmarks/img", allow_latency_run=True).latency_run \
        is None
    for bad in ('"latency_run": "../other"', '"latency_run": ""', '"latency_run": 3', '"extra": 1'):
        write("{" + base + ", " + bad + "}")
        with pytest.raises(run_outputs.OutputsRefused):
            run_outputs.resolve(str(tmp_path), "/repo", "docs/benchmarks/img", allow_latency_run=True)
    write("{" + base + ', "latency_run": "v0110-596"}')
    with pytest.raises(run_outputs.OutputsRefused, match="latency_run"):
        run_outputs.resolve(str(tmp_path), "/repo", "docs/benchmarks/img")


def test_a_zero_is_the_digit_zero_and_a_bar_label_never_runs_wide():
    assert extra_charts.value_text(0.0) == "0" and extra_charts.value_text(0) == "0"
    assert extra_charts.value_text(3.0) == "3.00" and extra_charts.value_text(26_844.0) == "27K"
    summary = extra_charts.stats.summarize([0.0, 0.0])
    assert extra_charts.range_text(summary) == "0 (0-0)"


def test_the_report_text_that_says_a_measurement_is_missing_is_replaced_only_for_a_run_that_has_it(tmp_path):
    old = {"matrix": {"load_pcts": [25]}}
    assert report.reading(old, 25, False) == report.reading({"matrix": {}}, 25, False)
    assert "unrecorded probe" in report.reading(old, 25, False)
    run = schema.load_run(report_run(tmp_path))
    text = report.reading(run.environment, 25, False)
    assert "unrecorded probe" not in text and "phase-2 item" not in text
    assert "CPU and bytes written" in text
    assert "not measured yet" in report.known_gaps({"matrix": {"record_sizes": [100], "partitions": [1],
                                                               "producers": [1]},
                                                    "comparable": {"profile": "defaults"}})
    assert "not measured yet" not in report.known_gaps(run.environment)
