"""The group-commit ablation of the Kafka comparison (#752 PR 6, #596).

`vtop-node` takes a `group_commit:` section (#832); the harness runs VTOP with
it off and with it on, so the official run reports both. Group commit is a
VTOP-only axis: Kafka has no counterpart, so a Kafka row leaves the column
blank. What these tests hold:

- the setting is a dimension of the matrix and of the row key, so a run that
  requested both settings owes the rows of both (`--check`, the report);
- the harness writes only the key the node build under test accepts, and
  refuses before any Docker work when that build has no such section;
- the setting is applied (the node's own gauge says so) or the run stops;
- environment.json records the settings and the configuration written (results
  schema 3), and the report and charts draw each setting as its own series;
- schema 1 and schema 2 runs still load, as runs with no such axis.
"""
import copy
import csv
import json
import os
import subprocess

import pytest
from test_kafka_compare_report import MATRIX as REPORT_MATRIX
from test_kafka_compare_report import environment as report_environment
from test_kafka_compare_schema_v2 import PHASE_1, SMALL, complete_rows, environment, refused, write_run

from kafka_compare import footer, plan, report, run, schema

AXIS = {**REPORT_MATRIX, "compressions": ["none"], "profiles": ["defaults"], "load_bases": {"kafka": [25, 50, 75]},
        "group_commits": ["off", "on"]}
SETTINGS = {"off": {"enabled": False}, "on": {"enabled": True}}


def axis_environment(matrix=AXIS):
    env = report_environment()
    env["matrix"] = copy.deepcopy(matrix)
    env["schema_version"] = 3
    env["comparable"]["host_disk"] = "Test SSD, SSD, NVMe, ext4"
    env["comparable"]["vtop_group_commit"] = {"settings": {name: SETTINGS[name]
                                                           for name in matrix.get("group_commits") or []}}
    return env


@pytest.fixture
def axis_run(tmp_path):
    env = axis_environment()
    return write_run(str(tmp_path / "run"), env, complete_rows(env))


def rewrite_rows(directory, mutate, columns=schema.COLUMNS_V3):
    path = os.path.join(directory, "results.csv")
    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows = mutate(rows)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


# ------------------------------------------------------------------ the plan


def test_a_run_that_asks_for_both_settings_owes_both():
    both = plan.expected_keys(AXIS)
    none = plan.expected_keys({k: v for k, v in AXIS.items() if k != "group_commits"})
    vtop = {key[8] for key in both if key[0] == "vtop"}
    kafka = {key[8] for key in both if key[0] == "kafka"}
    assert vtop == {"off", "on"} and kafka == {""}
    # VTOP's rows double and Kafka's do not.
    assert len([k for k in both if k[0] == "vtop"]) == 2 * len([k for k in none if k[0] == "vtop"])
    assert len([k for k in both if k[0] == "kafka"]) == len([k for k in none if k[0] == "kafka"])


def test_a_matrix_without_the_axis_owes_what_it_always_did():
    keys = plan.expected_keys({k: v for k, v in AXIS.items() if k != "group_commits"})
    assert {key[8] for key in keys} == {""}


def test_one_setting_is_an_axis_of_one():
    keys = plan.expected_keys({**AXIS, "group_commits": ["on"]})
    assert {key[8] for key in keys if key[0] == "vtop"} == {"on"}


@pytest.mark.parametrize("value", [[], ["maybe"], ["on", "on"], "on", [True], ["off", ""]])
def test_a_malformed_group_commits_axis_is_refused(value):
    with pytest.raises(plan.PlanRefused, match="group_commits"):
        plan.check_matrix({**AXIS, "group_commits": value})


def test_vtop_is_charged_once_per_setting():
    one = plan.projected_seconds({**AXIS, "group_commits": ["on"]})
    two = plan.projected_seconds(AXIS)
    # Without shared profiles Kafka and VTOP cost the same per setting: the second setting adds VTOP's share.
    assert two == pytest.approx(one * 1.5)
    assert plan.projected_seconds({k: v for k, v in AXIS.items() if k != "group_commits"}) == pytest.approx(one)


def test_the_plan_names_the_settings_it_will_run():
    assert "group commit off, on" in plan.summary(AXIS)
    assert "group commit" not in plan.summary({k: v for k, v in AXIS.items() if k != "group_commits"})


# ---------------------------------------------------------------- the schema


def test_results_schema_3_adds_group_commit_and_the_v2_layout_is_kept():
    assert schema.SCHEMA_VERSION >= 3
    assert "group_commit" in schema.COLUMNS_V3 and "group_commit" in schema.COLUMNS
    assert "group_commit" not in schema.COLUMNS_V2
    assert [c for c in schema.COLUMNS_V3 if c != "group_commit"] == list(schema.COLUMNS_V2)
    assert schema.KEY[schema.KEY.index("profile") + 1] == "group_commit"


def test_a_complete_axis_run_loads_and_keys_match_the_matrix(axis_run):
    loaded = schema.load_run(axis_run)
    assert loaded.version == 3
    assert {row["group_commit"] for row in loaded.rows if row["system"] == "vtop"} == {"off", "on"}
    assert {row["group_commit"] for row in loaded.rows if row["system"] == "kafka"} == {""}
    assert {schema.row_key(row) for row in loaded.rows} == schema.expected_keys(AXIS)


def test_check_says_which_setting_a_run_still_owes(axis_run):
    def drop_on(rows):
        return [row for row in rows if not (row["system"] == "vtop" and row["group_commit"] == "on")]

    rewrite_rows(axis_run, drop_on)
    found = schema.check_run(axis_run)
    assert found.missing and {key[8] for key in found.missing} == {"on"}
    assert {key[0] for key in found.missing} == {"vtop"}
    assert run.check(axis_run) == 2


def test_check_passes_a_run_that_has_both(axis_run):
    env_path = os.path.join(axis_run, "environment.json")
    with open(env_path) as handle:
        env = json.load(handle)
    env["run_state"] = "complete"
    with open(env_path, "w") as handle:
        json.dump(env, handle)
    assert run.check(axis_run) == 0


def test_a_partial_axis_run_is_refused_by_the_report(axis_run):
    rewrite_rows(axis_run, lambda rows: [r for r in rows if not (r["system"] == "vtop" and r["group_commit"] == "on"
                                                                 and r["mode"] == "saturation")])
    refused(axis_run, "partial")


def test_a_kafka_row_with_a_group_commit_setting_is_refused(axis_run):
    def mark(rows):
        next(r for r in rows if r["system"] == "kafka")["group_commit"] = "on"
        return rows

    rewrite_rows(axis_run, mark)
    refused(axis_run, "kafka row .*group_commit|group_commit.*kafka")


def test_an_unknown_group_commit_value_is_refused(axis_run):
    def mark(rows):
        next(r for r in rows if r["system"] == "vtop")["group_commit"] = "maybe"
        return rows

    rewrite_rows(axis_run, mark)
    refused(axis_run, "group_commit is 'maybe'")


def test_a_vtop_row_with_no_setting_in_an_axis_run_is_outside_the_matrix(axis_run):
    def blank(rows):
        next(r for r in rows if r["system"] == "vtop")["group_commit"] = ""
        return rows

    rewrite_rows(axis_run, blank)
    with pytest.raises(schema.ReportRefused):
        schema.load_run(axis_run)


def test_a_schema_2_csv_loads_as_a_run_with_no_axis(tmp_path):
    env = environment()
    rows = complete_rows(env)
    for row in rows:
        row.pop("group_commit")
    directory = write_run(str(tmp_path / "v2"), env, rows, schema.COLUMNS_V2)
    loaded = schema.load_run(directory)
    assert loaded.version == 2
    assert {row["group_commit"] for row in loaded.rows} == {""}
    assert {schema.row_key(row) for row in loaded.rows} == schema.expected_keys(SMALL)


def test_the_committed_schema_1_run_has_no_axis():
    loaded = schema.load_run(PHASE_1)
    assert {row["group_commit"] for row in loaded.rows} == {""}


def test_the_environment_records_the_settings_and_the_configuration_written(tmp_path):
    env = axis_environment()
    assert env["comparable"]["vtop_group_commit"]["settings"] == SETTINGS
    directory = write_run(str(tmp_path / "ok"), env, complete_rows(env))
    schema.load_run(directory)


def test_a_schema_3_run_that_did_not_record_group_commit_is_refused(tmp_path):
    env = axis_environment()
    del env["comparable"]["vtop_group_commit"]
    refused(write_run(str(tmp_path / "x"), env, complete_rows(env)), "vtop_group_commit")


def test_recorded_settings_must_agree_with_the_matrix(tmp_path):
    env = axis_environment()
    env["comparable"]["vtop_group_commit"] = {"settings": {"on": {"enabled": True}}}
    refused(write_run(str(tmp_path / "x"), env, complete_rows(env)), "group_commits")


def test_a_recorded_setting_must_say_what_its_name_says(tmp_path):
    env = axis_environment()
    env["comparable"]["vtop_group_commit"]["settings"]["on"] = {"enabled": False}
    refused(write_run(str(tmp_path / "x"), env, complete_rows(env)), "enabled")


def test_an_axis_in_an_older_environment_is_refused(tmp_path):
    env = axis_environment()
    env["schema_version"] = 2
    refused(write_run(str(tmp_path / "x"), env, complete_rows(env)), "schema_version 2")


def test_a_group_commit_setting_changes_the_fingerprint():
    one, two = axis_environment(), axis_environment()
    two["comparable"]["vtop_group_commit"] = {"settings": {"off": {"enabled": False}}}
    assert schema.fingerprint(one) != schema.fingerprint(two)


def test_a_failover_environment_does_not_carry_group_commit(monkeypatch):
    from kafka_compare import failover

    monkeypatch.setattr(failover.phase1, "capture_environment", lambda *a, **k: {
        "comparable": {"vtop_group_commit": {"settings": {}}, "profile": "defaults"}, "matrix": {}})
    monkeypatch.setattr(failover, "engine_commit", lambda: "c" * 40)
    monkeypatch.setattr(failover.phase1, "git", lambda *a: "d" * 40)
    env = failover.capture_environment("r", {}, "img", {}, "disk")
    assert "vtop_group_commit" not in env["comparable"]


# -------------------------------------------------------------- the runner

NODE_WITH_SECTION = """\
pub struct DataNodeConfig {
    pub role: DataRole,
    /// Group commit (#752).
    #[serde(default)]
    pub group_commit: Option<GroupCommitSection>,
}

pub struct GroupCommitSection {
    #[serde(default = "default_group_commit_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub max_delay_ms: u64,
    pub max_records: usize,
}
"""

NODE_WITHOUT_SECTION = """\
pub struct DataNodeConfig {
    pub role: DataRole,
    pub observability: Option<ObservabilitySection>,
}
"""

LEADER = "role: leader\nobservability: { listen: \"0.0.0.0:9600\" }\n"


@pytest.mark.parametrize("text, want", [
    ("off,on", ["off", "on"]), ("on,off", ["off", "on"]), ("on", ["on"]), ("off", ["off"]),
])
def test_the_group_commit_option_names_the_settings_in_one_order(text, want):
    assert run.parse_group_commits(text) == want


@pytest.mark.parametrize("text", ["", "maybe", "on,on", "off,,on", "true", "ON"])
def test_a_bad_group_commit_option_is_refused(text):
    with pytest.raises(SystemExit, match="--group-commit"):
        run.parse_group_commits(text)


def test_the_node_keys_come_from_its_source():
    assert run.node_group_commit_keys(NODE_WITH_SECTION) == {"enabled", "max_delay_ms", "max_records"}
    assert run.node_group_commit_keys(NODE_WITHOUT_SECTION) is None


def test_a_node_without_the_section_refuses_the_request_in_words():
    with pytest.raises(SystemExit) as refused_:
        run.require_group_commit_support(NODE_WITHOUT_SECTION, ["off", "on"])
    text = str(refused_.value)
    assert "group_commit" in text and "does not accept" in text and "#832" in text
    assert "without --group-commit" in text  # what to do next


def test_a_node_with_a_section_but_no_enabled_key_is_refused():
    source = NODE_WITH_SECTION.replace("pub enabled: bool,", "pub on: bool,")
    with pytest.raises(SystemExit, match="`enabled`"):
        run.require_group_commit_support(source, ["on"])


def test_a_node_with_the_section_is_accepted_and_the_keys_returned():
    assert "enabled" in run.require_group_commit_support(NODE_WITH_SECTION, ["off", "on"])


def test_the_request_is_refused_before_any_docker_work(monkeypatch):
    def fake_git(*args):
        assert args[0] == "show" and args[1] == f"HEAD:{run.NODE_CONFIG}"
        return NODE_WITHOUT_SECTION

    def no_docker(args, **_kwargs):
        raise AssertionError(f"ran {args[:3]} before refusing the request")

    monkeypatch.setattr(run, "git", fake_git)
    monkeypatch.setattr(run, "sh", no_docker)
    with pytest.raises(SystemExit, match="does not accept"):
        run.main(["--group-commit", "off,on"])


def test_without_the_option_the_node_source_is_not_even_read(monkeypatch):
    monkeypatch.setattr(run, "git", lambda *a: pytest.fail("read the node source with no group commit asked"))
    args = run.build_parser().parse_args([])
    sizes, sat = run.check_args(args)
    assert "group_commits" not in run.build_matrix(args, sizes, sat)


def test_the_matrix_records_the_settings_asked_for(monkeypatch):
    monkeypatch.setattr(run, "git", lambda *a: NODE_WITH_SECTION)
    args = run.build_parser().parse_args(["--group-commit", "on,off"])
    sizes, sat = run.check_args(args)
    assert run.build_matrix(args, sizes, sat)["group_commits"] == ["off", "on"]


def test_off_is_written_explicitly_so_a_changed_default_cannot_turn_it_on():
    text = run.leader_config(LEADER, "off", {"enabled", "max_delay_ms"})
    assert text.startswith(LEADER)
    assert text.rstrip().endswith("group_commit:\n  enabled: false")
    assert "enabled: true" in run.leader_config(LEADER, "on", {"enabled"})


def test_only_the_key_the_node_accepts_is_written():
    text = run.leader_config(LEADER, "on", {"enabled", "max_delay_ms", "max_records"})
    block = text[len(LEADER):]
    assert [line.strip() for line in block.splitlines() if line.strip() and not line.startswith("#")] == [
        "group_commit:", "enabled: true"]


def test_a_leader_config_that_already_sets_group_commit_is_refused():
    with pytest.raises(SystemExit, match="already sets group_commit"):
        run.leader_config(LEADER + "group_commit: { enabled: true }\n", "on", {"enabled"})


def test_the_leader_config_a_run_ships_has_no_group_commit_of_its_own():
    base = run._read(os.path.join(run.HERE, "vtop", "leader.yaml"))
    assert not [line for line in base.splitlines() if line.startswith("group_commit")]


def test_the_compose_file_takes_the_leader_config_from_the_run():
    with open(os.path.join(run.HERE, "compose.vtop.yml")) as handle:
        text = handle.read()
    assert "${VTOP_LEADER_CONFIG:-./vtop/leader.yaml}:/etc/vtop/node.yaml:ro" in text


def test_compose_is_given_the_generated_config(monkeypatch):
    seen = {}

    def fake_sh(args, **kwargs):
        seen.update(env=kwargs.get("env"))
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(run, "sh", fake_sh)
    run.compose("vtop", "ps", vtop_image="vtop-bench:x", leader_config="/runs/r/configs/leader-on.yaml")
    assert seen["env"]["VTOP_LEADER_CONFIG"] == "/runs/r/configs/leader-on.yaml"
    run.compose("vtop", "ps", vtop_image="vtop-bench:x")
    assert "VTOP_LEADER_CONFIG" not in seen["env"]


GAUGE = "# HELP vtop_broker_group_commit_enabled x\n# TYPE vtop_broker_group_commit_enabled gauge\n" \
        "vtop_broker_group_commit_enabled {}\n"


@pytest.mark.parametrize("setting, value", [("on", 1), ("off", 0)])
def test_a_node_that_applied_the_setting_passes(setting, value):
    run.check_group_commit_applied(setting, GAUGE.format(value))


@pytest.mark.parametrize("setting, value", [("on", 0), ("off", 1)])
def test_a_node_that_did_not_apply_the_setting_stops_the_run(setting, value):
    with pytest.raises(run.GroupCommitNotApplied, match=f"group commit {setting}"):
        run.check_group_commit_applied(setting, GAUGE.format(value))


def test_a_node_that_exports_no_gauge_stops_the_run():
    with pytest.raises(run.GroupCommitNotApplied, match="vtop_broker_group_commit_enabled"):
        run.check_group_commit_applied("on", "vtop_broker_other 1\n")


def test_a_failed_application_is_not_swallowed_as_a_refused_cell():
    # run_cell turns RuntimeError into `refused` rows and carries on with the next cell;
    # a setting that was not applied would repeat in every cell, so it must not be one.
    assert not issubclass(run.GroupCommitNotApplied, RuntimeError)


def test_environment_capture_records_the_settings_and_configs(monkeypatch, tmp_path):
    def fake_sh(args, **_kwargs):
        out = {"info": '{"NCPU": 8}', "version": '{"Server": {}}', "inspect": '[{"Id": "sha256:00"}]'}
        if args[:2] == ["docker", "info"]:
            return subprocess.CompletedProcess(args, 0, out["info"], "")
        if args[:2] == ["docker", "version"]:
            return subprocess.CompletedProcess(args, 0, out["version"], "")
        if args[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(args, 0, out["inspect"], "")
        if args[0] == "git":
            return subprocess.CompletedProcess(args, 0, "", "")
        return subprocess.CompletedProcess(args, 0, "Filesystem Type\n/dev/vda1 ext4 1 1 1 1% /probe\n", "")

    monkeypatch.setattr(run, "sh", fake_sh)
    configs = {"off": run.leader_config(LEADER, "off", {"enabled"}), "on": run.leader_config(LEADER, "on", {"enabled"})}
    env = run.capture_environment("r", {"group_commits": ["off", "on"]}, "vtop-bench:x", {"cpus": "2"}, "disk",
                                  group_commit_configs=configs)
    assert env["schema_version"] == schema.SCHEMA_VERSION
    assert env["comparable"]["vtop_group_commit"] == {"settings": SETTINGS}
    assert env["configs"]["vtop_leader_group_commit_on"] == configs["on"]
    assert env["configs"]["vtop_leader_group_commit_off"] == configs["off"]
    plain = run.capture_environment("r", {}, "vtop-bench:x", {"cpus": "2"}, "disk")
    assert plain["comparable"]["vtop_group_commit"] == {"settings": {}}
    assert not [k for k in plain["configs"] if k.startswith("vtop_leader_group_commit")]


def _ctx(tmp_path, gcs):
    csv_path = str(tmp_path / "results.csv")
    schema.write_header(csv_path)
    env = {"comparable": {"vtop_commit": "a" * 40}, "observed": {}, "events": []}
    return {"run_id": "r", "run_dir": str(tmp_path), "csv": csv_path, "env": env, "fingerprint": "f" * 16,
            "vtop_image": "vtop-bench:x", "reps": 1, "sat_records": {100: 1000}, "fixed_seconds": 1.0,
            "smoke_records": 1, "sat_timeout": 5.0, "consume": False, "group_commit_configs": gcs}


def _fake_cluster(monkeypatch):
    calls = {"up": [], "applied": []}
    monkeypatch.setattr(run, "cluster_up", lambda system, image, leader_config=None, **_k:
                        calls["up"].append(leader_config))
    monkeypatch.setattr(run, "cluster_down", lambda *a, **k: None)
    monkeypatch.setattr(run, "smoke", lambda *a, **k: None)
    monkeypatch.setattr(run, "_save_leader_log", lambda *a, **k: None)
    monkeypatch.setattr(run, "client", lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""))

    def scrape(_system):
        with open(calls["up"][-1]) as handle:
            return GAUGE.format(1 if "enabled: true" in handle.read() else 0)

    monkeypatch.setattr(run, "scrape_leader_metrics", scrape)
    result = run.perftool.ProducerResult(records=1000, records_per_sec=500.0, mib_per_sec=0.05, lat_avg_ms=1.0,
                                         lat_max_ms=2.0, lat_p50_ms=1.0, lat_p95_ms=1.0, lat_p99_ms=2.0,
                                         lat_p999_ms=2.0, steady_state=True)
    monkeypatch.setattr(run, "run_producer", lambda *a, **k: (result, "", 1.0, ""))
    return calls


def test_a_vtop_cell_runs_with_the_config_of_its_setting_and_its_rows_say_so(monkeypatch, tmp_path):
    calls = _fake_cluster(monkeypatch)
    ctx = _ctx(tmp_path, {"off": run.leader_config(LEADER, "off", {"enabled"}),
                          "on": run.leader_config(LEADER, "on", {"enabled"})})
    for setting in ("off", "on"):
        run.run_cell(ctx, "vtop", 100, {25: 10, 50: 20, 75: 30}, group_commit=setting)
    assert len(calls["up"]) == 2
    assert "enabled: false" in open(calls["up"][0]).read() and "enabled: true" in open(calls["up"][1]).read()
    with open(ctx["csv"], newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["group_commit"] for row in rows[:len(rows) // 2]} == {"off"}
    assert {row["group_commit"] for row in rows[len(rows) // 2:]} == {"on"}
    assert {row["system"] for row in rows} == {"vtop"}


def test_a_kafka_cell_leaves_the_column_blank(monkeypatch, tmp_path):
    calls = _fake_cluster(monkeypatch)
    monkeypatch.setattr(run, "_observe_kafka", lambda ctx: ctx["env"]["observed"].update(kafka_topic=""))
    ctx = _ctx(tmp_path, {"off": "x", "on": "y"})
    run.run_cell(ctx, "kafka", 100, None)
    with open(ctx["csv"], newline="") as handle:
        assert {row["group_commit"] for row in csv.DictReader(handle)} == {""}
    assert calls["up"] == [None]


def test_a_setting_the_node_did_not_apply_aborts_the_run_not_the_cell(monkeypatch, tmp_path):
    calls = _fake_cluster(monkeypatch)
    # The node keeps reporting 0 while "on" was asked for.
    monkeypatch.setattr(run, "scrape_leader_metrics", lambda system: GAUGE.format(0))
    ctx = _ctx(tmp_path, {"on": run.leader_config(LEADER, "on", {"enabled"})})
    with pytest.raises(run.GroupCommitNotApplied):
        run.run_cell(ctx, "vtop", 100, {25: 10, 50: 20, 75: 30}, group_commit="on")
    with open(ctx["csv"], newline="") as handle:
        assert list(csv.DictReader(handle)) == []  # nothing was measured, nothing is called refused
    assert calls["up"]


def test_a_run_without_the_axis_scrapes_nothing(monkeypatch, tmp_path):
    _fake_cluster(monkeypatch)
    monkeypatch.setattr(run, "scrape_leader_metrics", lambda system: pytest.fail("scraped without the axis"))
    ctx = _ctx(tmp_path, {})
    run.run_cell(ctx, "vtop", 100, {25: 10, 50: 20, 75: 30})


# ----------------------------------------------------------------- the report


def _axis_rows(env, off_rps=1000.0, on_rps=2500.0):
    rows = complete_rows(env)
    for row in rows:
        if row["system"] == "kafka":
            row["records_per_sec"] = 30000.0
        elif row["group_commit"] == "on":
            row["records_per_sec"] = on_rps
        elif row["group_commit"] == "off":
            row["records_per_sec"] = off_rps
        if row.get("records_per_sec") is not None:
            row["mib_per_sec"] = row["records_per_sec"] * row["record_size"] / 2**20
        if row["mode"] == "fixed_rate":
            row["offered_rate"] = 7500
            row["sustained"] = "true"
    return rows


@pytest.fixture
def built(tmp_path):
    env = axis_environment()
    directory = write_run(str(tmp_path / "run"), env, _axis_rows(env))
    docs = tmp_path / "docs"
    paths = report.build(directory, str(docs / "img"), str(docs / "kafka-comparison.md"))
    return docs, paths, (docs / "kafka-comparison.md").read_text()


def test_each_setting_is_its_own_series_in_the_tables(built):
    _docs, _paths, text = built
    assert "VTOP, group commit off" in text and "VTOP, group commit on" in text
    assert "## Group commit ablation" in text
    # 2500 / 1000 records/s on the saturation cell.
    assert "2.50x" in text


def test_the_ablation_table_gives_each_record_size_off_on_and_the_ratio(built):
    _docs, _paths, text = built
    section = text.split("## Group commit ablation")[1].split("\n## ")[0]
    assert "| record size | group commit off records/s | group commit on records/s | on / off |" in section
    assert section.count("| 100 B |") == 1


def test_the_environment_table_names_the_settings(built):
    _docs, _paths, text = built
    assert "| `vtop_group_commit` | `off, on` |" in text


def test_the_method_says_what_was_varied_and_what_was_not(built):
    _docs, _paths, text = built
    assert "`group_commit: {enabled: false}`" in text and "`group_commit: {enabled: true}`" in text
    assert "Kafka has no counterpart" in text


def test_the_known_cause_text_does_not_claim_group_commit_is_unwired(built):
    _docs, _paths, text = built
    assert "not wired into the node" not in text


def test_the_images_are_drawn_and_the_footer_names_the_settings(built):
    docs, paths, _text = built
    pngs = [p for p in paths if p.endswith(".png")]
    assert pngs and all(os.path.getsize(p) > 0 for p in pngs)
    svg = [p for p in paths if p.endswith(".svg")][0]
    with open(svg, encoding="utf-8") as handle:
        body = handle.read()
    assert "group commit: off, on (VTOP only)" in body


def test_an_unaxised_footer_is_unchanged():
    env = axis_environment()
    env["matrix"] = {k: v for k, v in env["matrix"].items() if k != "group_commits"}
    env["comparable"]["vtop_group_commit"] = {"settings": {}}
    env["comparable"].update({"host_cpu": "c", "host_logical_cpus": "1", "host_os": "o", "docker_platform": "d",
                              "docker_vm_cpus": 1, "docker_vm_kernel": "k", "volume_filesystem": "v",
                              "node_limits": {"cpus": "2", "mem_limit": "2g"}, "kafka_image": "k@s"})
    env.update(started_at="2026-11-09T00:00:00+00:00")
    assert "group commit" not in footer.footer_text(env)


def _loaded_rows(tmp_path):
    env = axis_environment()
    return schema.load_run(write_run(str(tmp_path / "run"), env, _axis_rows(env))).rows


def test_pooling_the_settings_into_one_median_is_refused(tmp_path):
    rows = _loaded_rows(tmp_path)
    with pytest.raises(schema.ReportRefused, match="group commit"):
        report.cell_summary(rows, "vtop", "saturation", 100, "records_per_sec")


def test_a_cell_summary_can_select_one_setting(tmp_path):
    rows = _loaded_rows(tmp_path)
    off = report.cell_summary(rows, "vtop", "saturation", 100, "records_per_sec", group_commit="off")
    on = report.cell_summary(rows, "vtop", "saturation", 100, "records_per_sec", group_commit="on")
    assert off["summary"].median == pytest.approx(1000.0) and on["summary"].median == pytest.approx(2500.0)


def test_a_run_with_one_setting_draws_a_series_labelled_with_it(tmp_path):
    matrix = {**AXIS, "group_commits": ["on"]}
    env = axis_environment(matrix)
    directory = write_run(str(tmp_path / "run"), env, _axis_rows(env))
    docs = tmp_path / "docs"
    report.build(directory, str(docs / "img"), str(docs / "kafka-comparison.md"))
    text = (docs / "kafka-comparison.md").read_text()
    assert "VTOP, group commit on" in text and "group commit off" not in text.split("## Group commit ablation")[0]


def test_no_third_colour_is_needed(built):
    # Both settings are VTOP's one validated colour, told apart by hatch, marker and line style,
    # so colours.py's two validated slots are enough.
    from kafka_compare import colours

    series = report.series_for(axis_environment())
    assert len({s.color for s in series if s.system == "vtop"}) == 1
    assert len({s.hatch for s in series if s.system == "vtop"}) == 2
    assert len({s.marker for s in series if s.system == "vtop"}) == 2
    assert {s.color for s in series} <= {colours.colour(system) for system in ("kafka", "vtop")}


def test_without_the_axis_the_series_are_the_two_systems_as_before():
    env = axis_environment()
    env["matrix"] = {k: v for k, v in env["matrix"].items() if k != "group_commits"}
    series = report.series_for(env)
    assert [s.label for s in series] == ["Apache Kafka", "VTOP (Kafka gateway)"]
    assert [s.group_commit for s in series] == ["", ""]


def test_the_matrix_copy_in_the_environment_is_not_mutated_by_drawing(tmp_path):
    env = axis_environment()
    before = copy.deepcopy(env)
    directory = write_run(str(tmp_path / "run"), env, _axis_rows(env))
    docs = tmp_path / "docs"
    report.build(directory, str(docs / "img"), str(docs / "kafka-comparison.md"))
    with open(os.path.join(directory, "environment.json")) as handle:
        assert json.load(handle) == before
