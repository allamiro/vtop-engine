"""The group-commit ablation (#752) and schema 4's measurements (#600) in one run: VTOP's end-to-end rows,
resource figures and latency pairing are kept apart per group-commit setting, never pooled.

Docker is replaced by the fakes of test_kafka_compare_harness_metrics; the runner, the plan, the loader and the
chart arithmetic are the real ones."""
import json
import os

from test_kafka_compare_harness_metrics import CONSUMER_PERF, FakeCounters, fake_args, producer_result

from kafka_compare import crash_report, plan, run, schema
from kafka_compare import extra_charts as ec

SATURATION = {("kafka", ""): 40_000.0, ("vtop", "off"): 10_000.0, ("vtop", "on"): 20_000.0}


def fake_cluster(monkeypatch):
    state = {"setting": ""}

    def cluster_up(system, image, profile="defaults", leader_config=None):
        state["setting"] = "" if leader_config is None else leader_config.rsplit("-", 1)[1].split(".")[0]

    def producer(system, records, size, rate, timeout, info_logging=False):
        key = (system, state["setting"] if system == "vtop" else "")
        return producer_result(SATURATION[key] if rate < 0 else rate), "", 1.0, ""

    def e2e_once(system, size, rate, records, timeout, image):
        return (producer_result(rate), "", 1.0, "", 0, f"READY\nconsumed {records}\n2 {records - 20}\n9 20\n", "")

    def scrape(_system):
        value = 1 if state["setting"] == "on" else 0
        return f"# TYPE {run.GROUP_COMMIT_GAUGE} gauge\n{run.GROUP_COMMIT_GAUGE} {value}\n"

    monkeypatch.setattr(run, "cluster_up", cluster_up)
    monkeypatch.setattr(run, "cluster_down", lambda *_a, **_k: None)
    monkeypatch.setattr(run, "smoke", lambda *_a: None)
    monkeypatch.setattr(run, "_save_leader_log", lambda *_a: None)
    monkeypatch.setattr(run, "_observe_kafka", lambda *_a: None)
    monkeypatch.setattr(run, "run_producer", producer)
    monkeypatch.setattr(run, "run_e2e_once", e2e_once)
    monkeypatch.setattr(run, "scrape_leader_metrics", scrape)
    monkeypatch.setattr(run, "client", lambda *_a, **_k: run.subprocess.CompletedProcess(
        [], 0, CONSUMER_PERF.format(n=2000), ""))


def simulate(tmp_path, monkeypatch):
    fake_cluster(monkeypatch)
    args = fake_args(group_commit="off,on")
    matrix = run.build_matrix(args, [100], {100: 2000})
    settings = run.parse_group_commits(args.group_commit)
    base = run._read(run.LEADER_CONFIG)
    configs = {s: run.leader_config(base, s, {"enabled"}) for s in settings}
    comparable = {"vtop_commit": "a" * 40, "kafka_image": run.KAFKA_IMAGE, "client": run.client_name(),
                  "host_disk": "Test SSD, SSD, NVMe, ext4", "profile": "defaults",
                  "vtop_group_commit": {"settings": {s: {"enabled": s == "on"} for s in settings}}}
    env = {"schema_version": schema.SCHEMA_VERSION, "kind": "throughput", "run_id": "sim", "run_state": "running",
           "comparable": comparable, "matrix": matrix, "observed": {}, "events": []}
    run_dir = str(tmp_path / "sim")
    os.makedirs(run_dir)
    ctx = {"run_id": "sim", "run_dir": run_dir, "csv": os.path.join(run_dir, "results.csv"), "env": env,
           "fingerprint": schema.fingerprint(env), "vtop_image": "vtop-bench:x", "reps": args.reps,
           "sat_records": {100: 2000}, "fixed_seconds": 1.0, "smoke_records": 10, "sat_timeout": 10.0,
           "consume": True, "group_commit_configs": configs, "matrix": matrix, "resources": True, "e2e": True,
           "closer": FakeCounters().closer(), "saturation": {}, "hist_csv": os.path.join(run_dir, schema.E2E_HIST),
           "e2e_image": "kc-e2e-client:test"}
    schema.write_header(ctx["csv"])
    schema.write_header(ctx["hist_csv"], schema.E2E_HIST_COLUMNS)
    rates = run.run_cell(ctx, "kafka", 100, None)
    for setting in settings:
        run.run_cell(ctx, "vtop", 100, rates, group_commit=setting)
    run.run_e2e_stage(ctx, 100)
    env["run_state"] = "complete"
    with open(os.path.join(run_dir, "environment.json"), "w") as handle:
        json.dump(env, handle)
    return run_dir, ctx


def test_the_plan_owes_vtops_end_to_end_rows_once_per_setting_and_kafkas_extra_profile_once():
    matrix = run.build_matrix(fake_args(group_commit="off,on"), [100], {100: 2000})
    e2e = {(k[0], k[7], k[8]) for k in plan.expected_keys(matrix) if k[1] == "e2e"}
    assert e2e == {("kafka", "defaults", ""), ("vtop", "defaults", "off"), ("vtop", "defaults", "on"),
                   ("kafka", "fsync-parity", "")}
    assert run.e2e_targets(matrix) == [("kafka", "defaults", ""), ("vtop", "defaults", "off"),
                                       ("vtop", "defaults", "on"), ("kafka", "fsync-parity", "")]


def test_a_run_varying_group_commit_with_every_new_measurement_is_complete_and_keeps_the_settings_apart(
        tmp_path, monkeypatch):
    run_dir, ctx = simulate(tmp_path, monkeypatch)
    loaded = schema.load_run(run_dir)
    assert loaded.version == 4
    # Half of the slowest series' saturation: VTOP with group commit off.
    assert ctx["env"]["observed"]["e2e_rates"] == {"100": 5_000}
    ok_e2e = [r for r in loaded.rows if r["mode"] == "e2e" and r["status"] == "ok"]
    assert {(r["system"], r["profile"], r["group_commit"]) for r in ok_e2e} == {
        ("kafka", "defaults", ""), ("vtop", "defaults", "off"), ("vtop", "defaults", "on"),
        ("kafka", "fsync-parity", "")}
    assert len(loaded.hist) == len(ok_e2e) == 8
    confirmed = [e for e in ctx["env"]["events"] if "confirmed" in e["event"]]
    assert {e["group_commit"] for e in confirmed} == {"off", "on"}
    assert len(confirmed) == 4  # each setting, for its cell and for its end-to-end target


def test_the_charts_draw_one_series_per_setting_and_never_pool_them(tmp_path, monkeypatch):
    run_dir, _ = simulate(tmp_path, monkeypatch)
    loaded = schema.load_run(run_dir)
    configs = ec.configs(loaded.rows)
    assert configs == [("kafka", "defaults", ""), ("kafka", "fsync-parity", ""), ("vtop", "defaults", "off"),
                       ("vtop", "defaults", "on")]
    assert [ec.label_of(c) for c in configs][2:] == ["VTOP, group commit off", "VTOP, group commit on"]
    assert ec.config_style(configs[2])["hatch"] != ec.config_style(configs[3])["hatch"]
    for config in configs:
        assert ec.config_cell(loaded.rows, config, 100, "lat_p50_ms")["n"] == 2  # its own repetitions only
    off = ec.resource_cell(loaded.rows, "vtop", "saturation", 100, "cpu_per_gib", group_commit="off")
    on = ec.resource_cell(loaded.rows, "vtop", "saturation", 100, "cpu_per_gib", group_commit="on")
    assert off["n"] == on["n"] == 2 and off["summary"] is not None and on["summary"] is not None
    text = "\n".join(ec.sections(loaded, {k: f"{k}.png" for k in ec.IMAGES}, "e2e_hist.csv"))
    assert "VTOP, group commit off" in text and "VTOP, group commit on" in text
    assert "VTOP is measured once per group-commit setting" in text


def test_a_crash_configuration_is_paired_with_the_latency_series_at_its_setting():
    have = [("kafka", "defaults", ""), ("kafka", "fsync-parity", ""), ("vtop", "defaults", "off"),
            ("vtop", "defaults", "on")]
    shipped_off = {"comparable": {"vtop_group_commit_shipped_default": False}}
    assert crash_report.latency_config("kafka-fsync-parity", shipped_off, have) == ("kafka", "fsync-parity", "")
    assert crash_report.latency_config("vtop-group-commit-on", shipped_off, have) == ("vtop", "defaults", "on")
    assert crash_report.latency_config("vtop-group-commit-off", shipped_off, have) == ("vtop", "defaults", "off")
    # As shipped is the setting the node ships with when the latency run varied it ...
    assert crash_report.latency_config("vtop-shipped", shipped_off, have) == ("vtop", "defaults", "off")
    # ... and the latency run's unvaried VTOP when it did not.
    assert crash_report.latency_config("vtop-shipped", shipped_off, [("vtop", "defaults", "")]) == (
        "vtop", "defaults", "")
