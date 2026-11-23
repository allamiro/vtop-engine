"""Schema 2 of the Kafka comparison's results CSV, and the plan that says what a
run owes (#596).

Schema 2 carries the full matrix: compression, two profiles, the `common`
load basis, the end-to-end mode, an acks=1 slice, and VTOP's rows repeated
under the profile whose configuration it shares. Every rule the report relies
on is enforced on load, so a run that breaks one is refused by name. The
committed phase-1 run (schema 1) must keep loading unchanged.
"""
import copy
import csv
import json
import os

import pytest

from kafka_compare import plan, report, run, schema

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
PHASE_1 = os.path.join(REPO, "benchmarks", "kafka_compare", "results", "20261109T232328Z")

FULL = {
    "record_sizes": [100, 1024, 10240], "partitions": [1, 6], "producers": [1, 4],
    "compressions": ["none", "zstd"], "acks": ["all"],
    "acks_slice": [{"record_size": 1024, "partitions": 1, "producers": 1, "compression": "none", "acks": "1",
                    "modes": ["saturation"]}],
    "profiles": ["defaults", "fsync-parity"], "vtop_profiles_share_config": True,
    "repetitions": 3, "load_pcts": [25, 50, 75],
    "load_bases": {"kafka": [25, 50, 75], "common": [25, 50, 75, 90]},
    "consume": True, "e2e": {"load_basis": "common", "load_pct": 50},
    "smoke_records": 10000,
}

SMALL = {
    "record_sizes": [100], "partitions": [1], "producers": [1], "compressions": ["none", "zstd"],
    "acks": ["all"], "profiles": ["defaults", "fsync-parity"], "vtop_profiles_share_config": True,
    "repetitions": 3, "load_pcts": [25, 50, 75], "load_bases": {"kafka": [25, 50, 75], "common": [50]},
    "consume": False, "e2e": {"load_basis": "common", "load_pct": 50}, "smoke_records": 10,
}

COMPARABLE = {
    "host_cpu": "Test CPU", "vtop_commit": "a" * 40, "kafka_image": "apache/kafka:4.3.2@sha256:00",
    "client": "perf-test 4.3.2 (JVM)",
}


def environment(matrix=SMALL):
    return {"run_id": "v2-run", "run_state": "complete", "comparable": copy.deepcopy(COMPARABLE),
            "matrix": copy.deepcopy(matrix), "observed": {}, "events": []}


def complete_rows(env):
    """One plausible schema-2 row per expected key. VTOP's fsync-parity rows
    repeat its defaults rows, as the harness records them."""
    fp = schema.fingerprint(env)
    rows = []
    for (system, mode, size, partitions, producers, compression, acks, profile, group_commit, rep, basis,
         pct) in sorted(schema.expected_keys(env["matrix"])):
        row = {
            "run_id": env["run_id"], "env_fingerprint": fp, "vtop_commit": COMPARABLE["vtop_commit"],
            "kafka_image": COMPARABLE["kafka_image"], "client": COMPARABLE["client"], "system": system,
            "mode": mode, "record_size": size, "partitions": partitions, "producers": producers,
            "compression": compression, "acks": acks, "profile": profile, "group_commit": group_commit,
            "repetition": rep,
            "load_basis": basis, "load_pct": pct, "status": "ok", "records": 1000, "records_per_sec": 1000.0,
            "mib_per_sec": 0.1, "started_at": "2026-11-14T00:00:00+00:00", "wall_seconds": 1.0, "note": "",
            "same_config_as": "",
        }
        if (system == "vtop" and profile == "fsync-parity" and mode in ("saturation", "consume")
                and "defaults" in env["matrix"]["profiles"]):
            row["same_config_as"] = "defaults"
        if mode in ("saturation", "fixed_rate", "e2e"):
            row.update(lat_p50_ms=1.0, lat_p95_ms=2.0, lat_p99_ms=3.0, lat_p999_ms=4.0,
                       lat_basis="worst-of-4" if producers == 4 else "single")
        if mode in ("fixed_rate", "e2e"):
            row.update(offered_rate=500, sustained="true")
        if mode == "e2e":
            row.update(records_produced=1000, window_start_ms=1000, window_end_ms=21000)
            row.pop("records_per_sec")
            row.pop("mib_per_sec")
        rows.append(row)
    return rows


def write_run(directory, env, rows, columns=schema.COLUMNS_V2):
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, "environment.json"), "w") as handle:
        json.dump(env, handle)
    path = os.path.join(directory, "results.csv")
    schema.write_header(path, columns)
    for row in rows:
        schema.append_row(path, row, columns)
    return directory


@pytest.fixture
def run_dir(tmp_path):
    env = environment()
    return write_run(str(tmp_path / "run"), env, complete_rows(env))


def rewrite(run_dir, mutate):
    path = os.path.join(run_dir, "results.csv")
    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows = mutate(rows)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=schema.COLUMNS_V2)
        writer.writeheader()
        writer.writerows(rows)


def refused(run_dir, match):
    with pytest.raises(schema.ReportRefused, match=match):
        schema.load_run(run_dir)


def first(rows, **criteria):
    return next(row for row in rows if all(row[k] == v for k, v in criteria.items()))


# ------------------------------------------------------------------------ layout


def test_v3_header_is_exactly_the_documented_columns():
    assert schema.COLUMNS_V3 == (
        "run_id", "env_fingerprint", "vtop_commit", "kafka_image", "client", "system", "mode", "record_size",
        "partitions", "producers", "compression", "acks", "profile", "group_commit", "same_config_as", "repetition",
        "load_basis", "load_pct", "offered_rate", "status", "sustained", "records", "records_produced",
        "records_per_sec", "mib_per_sec", "lat_avg_ms", "lat_max_ms", "lat_p50_ms", "lat_p95_ms", "lat_p99_ms",
        "lat_p999_ms", "lat_basis", "started_at", "window_start_ms", "window_end_ms", "wall_seconds", "note")
    assert schema.KEY == ("system", "mode", "record_size", "partitions", "producers", "compression", "acks",
                          "profile", "group_commit", "repetition", "load_basis", "load_pct")
    assert set(schema.KEY) <= set(schema.COLUMNS_V3) <= set(schema.COLUMNS)
    assert "profile" not in schema.RUN_CONSTANT and "profile" in schema.RUN_CONSTANT_V1


def test_a_complete_v3_run_loads(run_dir):
    loaded = schema.load_run(run_dir)
    assert loaded.version == 3
    assert len(loaded.rows) == len(schema.expected_keys(SMALL))


# ------------------------------------------------------------------ schema 1


def test_v1_run_loads_as_schema_2_with_what_phase_1_did():
    loaded = schema.load_run(PHASE_1)
    assert loaded.version == 1
    assert {row["compression"] for row in loaded.rows} == {"none"}
    assert {row["profile"] for row in loaded.rows} == {"defaults"}
    assert {row["load_basis"] for row in loaded.rows if row["mode"] == "fixed_rate"} == {"kafka"}
    assert {row["load_basis"] for row in loaded.rows if row["mode"] != "fixed_rate"} == {""}
    ok_producer = [row for row in loaded.rows if row["status"] == "ok" and row["mode"] != "consume"]
    assert ok_producer and {row["lat_basis"] for row in ok_producer} == {"single"}
    assert len(loaded.rows) == len(schema.expected_keys(loaded.environment["matrix"]))


def test_v1_matrix_owes_exactly_the_rows_phase_1_wrote():
    with open(os.path.join(PHASE_1, "results.csv"), newline="") as handle:
        written = list(csv.DictReader(handle))
    loaded = schema.load_run(PHASE_1)
    keys = {schema.row_key(row) for row in loaded.rows}
    assert keys == schema.expected_keys(loaded.environment["matrix"])
    assert len(keys) == len(written)


def test_profile_is_a_key_not_a_run_constant_but_mixing_commits_is_still_refused(run_dir):
    assert {row["profile"] for row in schema.load_run(run_dir).rows} == {"defaults", "fsync-parity"}

    def two_commits(rows):
        rows[-1]["vtop_commit"] = "c" * 40
        return rows

    rewrite(run_dir, two_commits)
    refused(run_dir, "mixes 2 values of vtop_commit")


def test_a_v1_csv_still_holds_profile_constant(tmp_path):
    from test_kafka_compare_report import complete_rows as v1_rows
    from test_kafka_compare_report import environment as v1_environment
    from test_kafka_compare_report import write_run as write_v1

    env = v1_environment()
    rows = v1_rows(env)
    rows[0]["profile"] = "fsync-parity"
    directory = write_v1(str(tmp_path / "v1"), env, rows)
    refused(directory, "mixes 2 values of profile")


# ------------------------------------------------------------- completeness


def test_expected_keys_cover_3_sizes_x_2_partitions_x_2_producers_x_2_codecs_x_2_profiles_x_2_systems():
    keys = schema.expected_keys(FULL)
    main = {k for k in keys if k[6] == "all"}
    shapes = {(k[2], k[3], k[4], k[5], k[7], k[0]) for k in main}
    assert len(shapes) == 3 * 2 * 2 * 2 * 2 * 2
    per_shape_and_rep = 1 + (3 + 4) + 1 + 1  # saturation, kafka and common rates, consume, e2e
    assert len(main) == len(shapes) * 3 * per_shape_and_rep
    acks_one = {k for k in keys if k[6] == "1"}
    assert acks_one == {("kafka" if s == 0 else "vtop", "saturation", 1024, 1, 1, "none", "1", p, "", r, "", "")
                        for s in (0, 1) for p in ("defaults", "fsync-parity") for r in (1, 2, 3)}


def test_a_missing_zstd_6_partition_4_producer_cell_is_refused_by_name(tmp_path):
    env = environment(FULL)
    rows = [row for row in complete_rows(env)
            if not (row["compression"] == "zstd" and row["partitions"] == 6 and row["producers"] == 4
                    and row["system"] == "vtop" and row["record_size"] == 10240)]
    directory = write_run(str(tmp_path / "full"), env, rows)
    with pytest.raises(schema.ReportRefused, match=r"partial: .* first \[\('vtop', '[a-z_0-9]+', 10240, 6, 4, "
                                                   r"'zstd'"):
        schema.load_run(directory)


def test_a_duplicate_key_is_refused(run_dir):
    def duplicate(rows):
        return rows + [dict(rows[0])]

    rewrite(run_dir, duplicate)
    refused(run_dir, "duplicate rows")


def test_a_row_outside_the_matrix_is_refused(run_dir):
    def gzip(rows):
        row = dict(rows[0])
        row["compression"] = "gzip"
        return rows + [row]

    rewrite(run_dir, gzip)
    refused(run_dir, "outside the recorded matrix")


# ------------------------------------------------------------- row rules


@pytest.mark.parametrize("column, value, match", [
    ("compression", "brotli", "unknown compression"),
    ("acks", "0", "acks is '0'"),
    ("profile", "turbo", "unknown profile"),
])
def test_unknown_values_are_refused(run_dir, column, value, match):
    def poke(rows):
        rows[0][column] = value
        return rows

    rewrite(run_dir, poke)
    refused(run_dir, match)


def test_only_vtop_may_repeat_another_profiles_measurement(run_dir):
    def kafka_copy(rows):
        first(rows, system="kafka", profile="fsync-parity")["same_config_as"] = "defaults"
        return rows

    rewrite(run_dir, kafka_copy)
    refused(run_dir, "only VTOP")


def test_same_config_as_names_the_other_profile(run_dir):
    def itself(rows):
        first(rows, system="vtop", profile="fsync-parity")["same_config_as"] = "fsync-parity"
        return rows

    rewrite(run_dir, itself)
    refused(run_dir, "must name the other profile")


@pytest.mark.parametrize("mode, basis, match", [
    ("fixed_rate", "", "a fixed_rate row has load_basis ''"),
    ("fixed_rate", "vtop", "a fixed_rate row has load_basis 'vtop'"),
    ("saturation", "kafka", "a saturation row has load_basis 'kafka'"),
])
def test_the_load_basis_belongs_to_rated_rows(run_dir, mode, basis, match):
    def poke(rows):
        first(rows, mode=mode)["load_basis"] = basis
        return rows

    rewrite(run_dir, poke)
    refused(run_dir, match)


def test_four_producers_report_worst_of_four_and_a_bad_basis_is_refused(run_dir):
    def bad(rows):
        first(rows, mode="saturation", status="ok")["lat_basis"] = "median-of-4"
        return rows

    rewrite(run_dir, bad)
    refused(run_dir, "lat_basis is 'median-of-4'")


def test_an_ok_e2e_row_must_have_consumed_what_was_produced(run_dir):
    def short(rows):
        first(rows, mode="e2e")["records"] = "999"
        return rows

    rewrite(run_dir, short)
    refused(run_dir, "consumed 999 of 1000")


def test_a_failed_e2e_row_says_how_many_are_missing(run_dir):
    def failed(rows):
        row = first(rows, mode="e2e")
        row.update(status="failed", records="999", note="missing 1 records")
        return rows

    rewrite(run_dir, failed)
    assert schema.load_run(run_dir)


def test_records_produced_is_for_e2e_rows_only(run_dir):
    def poke(rows):
        first(rows, mode="saturation")["records_produced"] = "1000"
        return rows

    rewrite(run_dir, poke)
    refused(run_dir, "records_produced is for e2e rows only")


@pytest.mark.parametrize("start, end", [("1000", ""), ("2000", "1000"), ("1e3", "2000")])
def test_a_window_is_a_start_and_an_end_after_it(run_dir, start, end):
    def poke(rows):
        first(rows, mode="saturation").update(window_start_ms=start, window_end_ms=end)
        return rows

    rewrite(run_dir, poke)
    refused(run_dir, "window|whole number")


# --------------------------------------------------------------------- plan


def test_plan_refuses_a_malformed_axis():
    for key, value in (("compressions", ["none", "none"]), ("profiles", ["quick"]),
                       ("load_bases", {"vtop": [50]}), ("load_bases", {"common": [100]}),
                       ("e2e", {"load_basis": "common", "load_pct": 0})):
        with pytest.raises(plan.PlanRefused):
            plan.check_matrix({**SMALL, key: value})


def test_projected_hours_over_the_limit_refuses_before_any_docker_call(monkeypatch):
    calls = []
    monkeypatch.setattr(run, "sh", lambda args, **_: calls.append(args))
    monkeypatch.setattr(run, "check_equal_limits", lambda: calls.append("limits"))
    with pytest.raises(SystemExit, match="over --max-hours 0.5"):
        run.main(["--sizes", "100,1024", "--sat-records", "100:10,1024:10", "--max-hours", "0.5"])
    assert calls == []


def test_vtop_sharing_its_configuration_saves_one_bring_up_and_its_unrated_runs_per_extra_profile():
    """#596: VTOP brings its cluster up and makes its saturation, consume and
    e2e runs once per cell; its fixed-rate runs are per profile, because their
    rates come from that profile's Kafka saturation."""
    t = plan.DEFAULT_TIMING
    shared = plan.projected_seconds(FULL)
    separate = plan.projected_seconds({**FULL, "vtop_profiles_share_config": False})
    main_cells, slice_cells, reps = 3 * 2 * 2 * 2, 1, 3
    unrated_main = t["saturation"] + t["consume"] + t["e2e"]
    saved = (main_cells * (t["bring_up"] + t["warm_up"] + reps * unrated_main)
             + slice_cells * (t["bring_up"] + t["warm_up"] + reps * t["saturation"]))
    assert separate - shared == pytest.approx(saved)


# ------------------------------------------------------------------- --check


def test_an_interrupted_run_is_incomplete_and_the_check_command_exits_2_listing_missing_keys(run_dir, capsys):
    with open(os.path.join(run_dir, "environment.json")) as handle:
        env = json.load(handle)
    env["run_state"] = "interrupted"
    with open(os.path.join(run_dir, "environment.json"), "w") as handle:
        json.dump(env, handle)

    def drop_vtop_e2e(rows):
        return [row for row in rows if not (row["system"] == "vtop" and row["mode"] == "e2e")]

    rewrite(run_dir, drop_vtop_e2e)
    assert run.main(["--check", run_dir]) == 2
    err = capsys.readouterr().err
    assert "missing 12 " in err  # 2 codecs x 2 profiles x 3 repetitions
    assert "vtop e2e 100 1 1 zstd all fsync-parity - 3 common 50" in err


def test_the_check_command_exits_0_on_a_complete_run(run_dir, capsys):
    assert run.main(["--check", run_dir]) == 0
    assert "144 of 144 expected rows present" in capsys.readouterr().out


def test_the_check_command_refuses_rows_from_another_environment(run_dir, capsys):
    def other(rows):
        for row in rows:
            row["env_fingerprint"] = "0" * 16
        return rows

    rewrite(run_dir, other)
    assert run.main(["--check", run_dir]) == 2
    assert "fingerprints to" in capsys.readouterr().err


# ------------------------------------------------------------------- report


def test_the_report_refuses_dimensions_it_does_not_draw(run_dir):
    loaded = schema.load_run(run_dir)
    with pytest.raises(schema.ReportRefused, match="compression is none, zstd"):
        report.check_drawable(loaded.rows, loaded.environment)


def test_the_report_refuses_common_basis_rates_it_does_not_draw(tmp_path):
    env = environment({**SMALL, "compressions": ["none"], "profiles": ["defaults"], "e2e": None})
    env["comparable"]["profile"] = "defaults"
    directory = write_run(str(tmp_path / "one"), env, complete_rows(env))
    loaded = schema.load_run(directory)
    with pytest.raises(schema.ReportRefused, match="load bases"):
        report.check_drawable(loaded.rows, loaded.environment)


def test_the_report_draws_the_phase_1_run():
    loaded = schema.load_run(PHASE_1)
    report.check_drawable(loaded.rows, loaded.environment)


def test_the_projection_charges_each_fixed_rate_run_its_configured_duration():
    """Review: --fixed-seconds 7200 on the default matrix is days of
    fixed-rate runs, and must not pass a 30-hour budget."""
    short = {**SMALL, "fixed_seconds": 30}
    long = {**SMALL, "fixed_seconds": 7200}
    fixed_runs = 2 * 2 * 2 * 3 * 4  # cells x profiles x systems x reps x rates: every system runs every profile's
    assert plan.projected_seconds(long) - plan.projected_seconds(short) == pytest.approx(
        fixed_runs * (7200 - 30))
    with pytest.raises(plan.PlanRefused, match="over --max-hours 30"):
        plan.check_budget(long, 30)


def test_kafka_rates_must_be_the_load_pcts_the_report_draws():
    """Review: the report draws Kafka-relative rows by load_pcts, so a
    load_bases.kafka list that differs is refused, not half drawn."""
    with pytest.raises(plan.PlanRefused, match="differs from matrix.load_pcts"):
        plan.check_matrix({**SMALL, "load_bases": {"kafka": [25, 50, 75, 90]}})
    plan.check_matrix({**SMALL, "load_bases": {"kafka": [25, 50, 75], "common": [90]}})


# ----------------------------------------------------------------------- review of 66670fb


def test_a_repeat_must_match_a_measured_row_under_the_named_profile(run_dir):
    def changed(rows):
        first(rows, system="vtop", profile="fsync-parity", mode="saturation")["records_per_sec"] = "1.5"
        return rows

    rewrite(run_dir, changed)
    refused(run_dir, "differs from it in records_per_sec")


def test_two_repeats_naming_each_other_are_not_a_measurement(run_dir):
    def circular(rows):
        first(rows, system="vtop", profile="defaults", mode="saturation", repetition="1",
              compression="none")["same_config_as"] = "fsync-parity"
        return rows

    rewrite(run_dir, circular)
    refused(run_dir, "not in the run as a measured row")


def test_a_rated_row_cannot_repeat_another_profile(run_dir):
    def rated(rows):
        first(rows, system="vtop", profile="fsync-parity", mode="fixed_rate")["same_config_as"] = "defaults"
        return rows

    rewrite(run_dir, rated)
    refused(run_dir, "rated rows are run per profile")


@pytest.mark.parametrize("producers, basis, ok", [
    ("1", "worst-of-7", False), ("4", "single", False), ("4", "worst-of-4", True), ("12", "worst-of-12", True),
])
def test_lat_basis_names_the_producers(run_dir, producers, basis, ok):
    def poke(rows):
        row = first(rows, mode="saturation", system="kafka")
        row.update(producers=producers, lat_basis=basis)
        return rows

    rewrite(run_dir, poke)
    if ok:
        with pytest.raises(schema.ReportRefused) as raised:
            schema.load_run(run_dir)
        assert "lat_basis" not in str(raised.value)  # refused later, as a key outside the matrix
    else:
        refused(run_dir, "lat_basis is")


def test_the_report_refuses_one_value_its_text_does_not_say(tmp_path):
    env = environment({**SMALL, "compressions": ["zstd"], "profiles": ["fsync-parity"], "e2e": None,
                       "load_bases": {"kafka": [25, 50, 75]}})
    env["comparable"]["profile"] = "defaults"
    directory = write_run(str(tmp_path / "zstd"), env, complete_rows(env))
    loaded = schema.load_run(directory)
    with pytest.raises(schema.ReportRefused, match="compression is zstd"):
        report.check_drawable(loaded.rows, loaded.environment)


def test_the_report_refuses_e2e_rows_it_does_not_draw(tmp_path):
    env = environment({**SMALL, "compressions": ["none"], "profiles": ["defaults"],
                       "load_bases": {"kafka": [25, 50, 75]}})
    env["comparable"]["profile"] = "defaults"
    directory = write_run(str(tmp_path / "e2e"), env, complete_rows(env))
    loaded = schema.load_run(directory)
    with pytest.raises(schema.ReportRefused, match="end-to-end"):
        report.check_drawable(loaded.rows, loaded.environment)


def test_check_says_a_run_with_every_row_but_not_complete_is_not_done(run_dir, capsys):
    with open(os.path.join(run_dir, "environment.json")) as handle:
        env = json.load(handle)
    env["run_state"] = "running"
    with open(os.path.join(run_dir, "environment.json"), "w") as handle:
        json.dump(env, handle)
    assert run.main(["--check", run_dir]) == 2
    assert "run_state is 'running'" in capsys.readouterr().err


def test_a_record_count_run_is_charged_the_bytes_it_moves():
    small = {**SMALL, "sat_records": {"100": 1000}}
    big = {**SMALL, "sat_records": {"100": 300_000_000}}
    assert plan.projected_seconds(big) > plan.projected_seconds(small) + 3600
    with pytest.raises(plan.PlanRefused, match="over --max-hours"):
        plan.check_budget(big, 30)


def test_the_committed_phase_1_matrix_projects_at_least_its_real_duration():
    """Phase 1 ran from 23:23 to 01:02, 1.65 hours."""
    with open(os.path.join(PHASE_1, "environment.json")) as handle:
        matrix = json.load(handle)["matrix"]
    assert plan.projected_seconds(matrix) / 3600 >= 1.65


@pytest.mark.parametrize("key, value", [("load_bases", {"kafka": 5}), ("acks_slice", 5), ("fixed_seconds", None),
                                        ("acks", ["all", "all"]), ("vtop_profiles_share_config", "yes"),
                                        ("record_sizes", None)])
def test_a_malformed_matrix_is_a_refusal_not_a_traceback(key, value):
    with pytest.raises(plan.PlanRefused):
        plan.check_matrix({**SMALL, key: value})
