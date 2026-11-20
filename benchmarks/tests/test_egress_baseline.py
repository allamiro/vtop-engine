"""The #483 sweep driver: what it asks the engine for, and how it judges.

The gate is a decision about whether a whole transport gets built, so the
arithmetic that produces it is pinned here: the goodput a run is credited
with, the best-tuned selection and its tie rule, the two thresholds, the noise
rule, and the charts being a pure function of the data.
"""
import json
import os
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import egress_baseline as eb  # noqa: E402
from lib import engine, netem, shaping  # noqa: E402
from lib.scenario import load_scenario  # noqa: E402

# ------------------------------------------------------------ the workload


def _load(tmp_path, text):
    path = tmp_path / "s.yaml"
    path.write_text(text, encoding="utf-8")
    return load_scenario(str(path))


def test_a_grid_point_reaches_the_engine_config_as_its_tuning(tmp_path):
    sc = _load(tmp_path, eb.scenario_yaml("wan100", 4, 2, "t"))
    cfg = tmp_path / "engine.yaml"
    engine.write_engine_config(sc, str(tmp_path / "w"), str(tmp_path / "s.db"),
                               str(tmp_path / "in/*"), str(cfg), key_prefix="p")
    text = cfg.read_text(encoding="utf-8")
    # The engine takes the minimum of the batch width and the transport's
    # max_concurrency, so both must say 4 or the point is not the one named.
    assert "max_concurrent_batches: 4" in text, text
    for line in ("tcp_tls:", "max_concurrency: 4", "parts_in_flight: 2",
                 f"part_size_bytes: {eb.PART_BYTES}"):
        assert line in text, f"{line!r} missing from the engine config:\n{text}"
    assert "whole_file: true" in text and "max_records: 1" in text, (
        "one whole file per batch is what makes each batch one 20 MiB object")


def test_the_default_point_carries_no_tuning_at_all(tmp_path):
    sc = _load(tmp_path, eb.scenario_yaml("wan100", None, None, "t"))
    cfg = tmp_path / "engine.yaml"
    engine.write_engine_config(sc, str(tmp_path / "w"), str(tmp_path / "s.db"),
                               str(tmp_path / "in/*"), str(cfg), key_prefix="p")
    text = cfg.read_text(encoding="utf-8")
    assert "transports:" not in text and "max_concurrent_batches" not in text, (
        f"the untuned default must run on the shipped values:\n{text}")


@pytest.mark.parametrize("profile", ["wan100", "wan100-loss1", "wan100-policed"])
def test_every_shaped_profile_is_a_valid_netem_shape(tmp_path, profile):
    sc = _load(tmp_path, eb.scenario_yaml(profile, 2, 1, "t"))
    shape = shaping.shape_from_scenario(sc)
    assert isinstance(shape, netem.NetemShape)
    assert shape.latency_ms == 100


def test_the_lossy_profile_is_one_percent_bursty_on_the_clean_profiles_link(tmp_path):
    lossy = shaping.shape_from_scenario(_load(tmp_path, eb.scenario_yaml("wan100-loss1", 2, 1, "t")))
    clean = shaping.shape_from_scenario(_load(tmp_path, eb.scenario_yaml("wan100", 2, 1, "t")))
    assert (lossy.loss_pct, lossy.loss_model) == (1.0, "gemodel")
    # Everything but the loss identical, or the p95 ratio compares two links.
    assert lossy.rate_calibration_twin().tc_program() == clean.rate_calibration_twin().tc_program()


def test_the_lan_profile_is_unshaped_but_still_crosses_the_middlebox(tmp_path):
    sc = _load(tmp_path, eb.scenario_yaml("lan", 2, 1, "t"))
    assert shaping.shape_from_scenario(sc) is None
    assert sc.get("endpoint_url") == "http://netem:9200"


def test_the_backlog_is_two_waves_and_never_fewer_than_four():
    assert [eb.objects_for(c) for c in (1, 2, 4, 8, 16)] == [4, 4, 8, 16, 32]
    assert eb.objects_for(None) == 16, "the default's width is the shipped eight"
    assert eb.objects_for(1, "lan") == eb.LAN_OBJECTS


# ------------------------------------------------------------- one run


def _batch(status="committed", nbytes=eb.OBJECT_BYTES, up=1000, ver=100):
    return {"batch_id": "b", "batch_status": status, "compressed_bytes": nbytes,
            "upload_duration_ms": up, "verify_duration_ms": ver,
            "manifest_upload_duration_ms": 10, "checksum_duration_ms": 5,
            "total_batch_duration_ms": up + ver + 15}


def _meta(profile="wan100"):
    return {"run_id": "r", "phase": "grid", "profile": profile, "config": "c1-p1",
            "c": "1", "pif": "1", "rep": "1", "engine_cpu_seconds": "2.0",
            "store_cpu_seconds": "1.0"}


def test_goodput_divides_committed_bytes_by_the_cycles_that_committed_them():
    cycles = [{"cycle_seconds": "16.0", "committed_batches": "1"},
              # the empty closing cycle is not busy time
              {"cycle_seconds": "0.1", "committed_batches": "0"}]
    row, objs = eb.summarize_run(_meta(), {"start_time": "2026-11-10T00:00:00+00:00",
                                           "emulator_validation_mbps": 9.6},
                                 cycles, [_batch(), _batch(status="failed")])
    assert row["busy_seconds"] == 16.0
    assert row["goodput_mbps"] == pytest.approx(eb.OBJECT_BYTES * 8 / 16.0 / 1e6, abs=1e-4)
    assert row["fraction_of_link"] == pytest.approx(row["goodput_mbps"] / 9.6, abs=1e-4)
    assert row["objects_committed"] == 1 and row["objects_failed"] == 1
    assert len(objs) == 2, "every batch is kept in objects.csv, failed ones included"


def test_the_policed_row_is_read_against_the_policer_and_the_lan_against_nothing():
    summary = {"start_time": "2026-11-10T00:00:00+00:00", "emulator_validation_mbps": 400.0}
    cycles = [{"cycle_seconds": "16.0", "committed_batches": "1"}]
    policed, _ = eb.summarize_run(_meta("wan100-policed"), summary, cycles, [_batch()])
    assert policed["link_mbps"] == 10.0, (
        "the probe runs on the twin WITHOUT the policer, so it is not the policed link")
    lan, _ = eb.summarize_run(_meta("lan"), summary, cycles, [_batch()])
    assert lan["link_mbps"] == "" and lan["fraction_of_link"] == ""


# --------------------------------------------------------------- the gate


def test_best_config_breaks_a_tie_toward_fewer_connections():
    points = {"c16-p4": [0.95, 0.96], "c2-p1": [0.94, 0.94], "c1-p1": [0.80, 0.81]}
    conns = {"c16-p4": 64, "c2-p1": 2, "c1-p1": 1}
    assert eb.best_config(points, conns) == "c2-p1", (
        "0.94 is within two points of 0.955; the two-connection point wins the tie")
    points["c2-p1"] = [0.92, 0.92]
    assert eb.best_config(points, conns) == "c16-p4", "3.5 points is not a tie"


def _gate_runs(clean_fracs, lossy_fracs, best="c4-p2"):
    runs = []
    for profile, fracs in (("wan100", clean_fracs), ("wan100-loss1", lossy_fracs)):
        for i, f in enumerate(fracs):
            for phase in ("grid", "confirm"):
                runs.append({"run_id": f"{profile}-{phase}-{i}", "profile": profile,
                             "phase": phase, "config": best, "c": "4", "pif": "2",
                             "fraction_of_link": str(f), "object_upload_p95_ms": "1000"})
        runs.append({"run_id": f"{profile}-worse", "profile": profile, "phase": "grid",
                     "config": "c1-p1", "c": "1", "pif": "1",
                     "fraction_of_link": "0.3", "object_upload_p95_ms": "1000"})
    return runs


def _objects(runs, profile_ms):
    return [{"run_id": r["run_id"], "status": "committed",
             "object_upload_ms": str(profile_ms[r["profile"]])}
            for r in runs if r["phase"] == "confirm"]


def test_a_tuned_path_that_fills_the_link_keeps_the_gate_closed():
    runs = _gate_runs([0.93, 0.94, 0.95], [0.90, 0.91, 0.92])
    gate = eb.evaluate_gate(runs, _objects(runs, {"wan100": 60000, "wan100-loss1": 66000}))
    assert gate["profiles"]["wan100"]["best_config"] == "c4-p2"
    assert gate["profiles"]["wan100-loss1"]["opens"] is False
    assert gate["p95_ratio"]["ratio"] == pytest.approx(1.1)
    assert gate["datagram_issues_open"] is False


def test_unused_goodput_above_a_fifth_on_either_profile_opens_the_gate():
    runs = _gate_runs([0.93, 0.94, 0.95], [0.70, 0.72, 0.75])
    gate = eb.evaluate_gate(runs, _objects(runs, {"wan100": 60000, "wan100-loss1": 66000}))
    assert gate["profiles"]["wan100-loss1"]["unused_median"] == pytest.approx(0.28)
    assert gate["profiles"]["wan100-loss1"]["opens"] is True
    assert gate["datagram_issues_open"] is True


def test_a_p95_ratio_above_two_opens_the_gate_alone():
    runs = _gate_runs([0.93, 0.94, 0.95], [0.90, 0.91, 0.92])
    gate = eb.evaluate_gate(runs, _objects(runs, {"wan100": 60000, "wan100-loss1": 130000}))
    assert gate["p95_ratio"]["opens"] is True
    assert gate["datagram_issues_open"] is True


def test_a_threshold_inside_the_repetitions_range_is_reported_as_noise():
    runs = _gate_runs([0.93, 0.94, 0.95], [0.75, 0.82, 0.86])
    gate = eb.evaluate_gate(runs, _objects(runs, {"wan100": 60000, "wan100-loss1": 66000}))
    lossy = gate["profiles"]["wan100-loss1"]
    assert lossy["opens"] is False and lossy["within_noise"] is True, lossy


def test_the_gate_is_undecided_without_confirmation_runs():
    runs = [r for r in _gate_runs([0.9], [0.9]) if r["phase"] == "grid"]
    assert eb.evaluate_gate(runs, [])["datagram_issues_open"] is None


def test_the_gate_numbers_are_the_ones_the_doc_commits_to():
    doc = os.path.join(os.path.dirname(eb.HERE), "docs", "EGRESS_TRANSPORT_RESEARCH.md")
    with open(doc, encoding="utf-8") as fh:
        text = fh.read()
    assert f"`U` > {eb.GATE_UNUSED_FRACTION:.2f}" in text
    assert f"`R` > {eb.GATE_P95_RATIO:.1f}" in text


# ------------------------------------------------------------ bottleneck


def test_bottleneck_names_follow_the_fixed_rule():
    assert eb.name_bottleneck("wan100", 0.95, 0.5, 0.1, 0.1) == "link (rate)"
    assert eb.name_bottleneck("wan100", 0.60, 0.30, 0.1, 0.1) == "verification"
    assert eb.name_bottleneck("wan100-loss1", 0.40, 0.05, 0.1, 0.1) == "link (per-flow window)"
    assert eb.name_bottleneck("lan", 0.0, 0.1, 5.0, 0.8) == "CPU (engine)"
    assert eb.name_bottleneck("lan", 0.0, 0.1, 0.8, 5.0) == "CPU (store)"
    assert eb.name_bottleneck("lan", 0.0, 0.1, 1.2, 0.9) == "store I/O (no CPU saturated)", (
        "a busier engine is not a CPU bottleneck while five of six cores idle")


# ---------------------------------------------------------------- charts


def test_the_charts_are_a_pure_function_of_the_points():
    runs = _gate_runs([0.93, 0.94], [0.80, 0.85])
    objs = _objects(runs, {"wan100": 60000, "wan100-loss1": 66000})
    for r in runs:
        r.update({"goodput_mbps": "9", "link_mbps": "9.6", "busy_seconds": "10",
                  "store_cpu_seconds": "1", "engine_cores_busy": "0.1",
                  "manifest_mean_ms": "10", "cpu_seconds_per_gib": "1",
                  "objects_failed": "0", "competitor_goodput_mbps_with": "",
                  "competitor_goodput_mbps_without": "", "vtop_goodput_mbps_with": "",
                  "competitor_jain_index": ""})
    for o in objs:
        o.update({"verify_ms": "100", "batch_total_ms": "1000"})
    points = eb.aggregate_points(runs, objs)

    def render():
        return eb.svg_small_multiples(points, ("wan100", "wan100-loss1"), "fraction_median",
                                      "t", "y", 1.05, [0, 0.5, 1], ref=0.8, ref_label="g")

    first = render()
    assert first == render()
    assert first.count("<circle") >= 2 and "stroke-dasharray" in first


def test_a_contended_row_is_scenario_18_with_only_the_width_replaced(tmp_path):
    with open(eb.CONTENDED_BASE, encoding="utf-8") as fh:
        base = fh.read()
    sc = _load(tmp_path, eb.contended_yaml(base, 4, 2, "contended"))
    original = _load(tmp_path, base)
    assert sc.get("max_concurrent_batches") == 4
    assert sc.get("transports")["tcp_tls"]["max_concurrency"] == 4
    for key in ("shaping_competitor", "shaping_bottleneck_kbps", "shaping_buffer_bdp",
                "shaping_latency_ms", "batch_max_records", "duration_seconds", "volume"):
        assert sc.get(key) == original.get(key), key
    default = _load(tmp_path, eb.contended_yaml(base, None, None, "contended"))
    assert default.get("max_concurrent_batches") == 8 and not default.get("transports")


def test_the_generated_measurement_rows_pass_the_documents_claims_lint(tmp_path):
    point = {"profile": "wan100-policed", "config": "default", "phase": "grid", "c": "",
             "pif": "", "runs": "2", "goodput_mbps_median": "", "goodput_mbps_min": "nan",
             "goodput_mbps_max": "nan", "fraction_median": "", "object_upload_p95_ms_pooled": "",
             "verify_mean_ms": "", "cpu_seconds_per_gib_median": "", "objects_failed": "16",
             "competitor_goodput_mbps_with": "", "competitor_goodput_mbps_without": "",
             "bottleneck": "link (per-flow window)"}
    contended = dict(point, profile="wan100", config="c8-p1", c="8", pif="1",
                     phase="contended", goodput_mbps_median="3.1", goodput_mbps_min="3.0",
                     goodput_mbps_max="3.2", fraction_median="0.33", objects_failed="0",
                     competitor_goodput_mbps_with="4.7", competitor_goodput_mbps_without="9.5")
    rows = eb.measurement_rows([point, contended],
                               {("wan100-policed", "default", "grid"): "2026-11-10",
                                ("wan100", "c8-p1", "contended"): "2026-11-11"})
    doc = tmp_path / "doc.md"
    doc.write_text("# t\n\n## Measurements\n\n" + "\n".join(rows) + "\n\n## After\n",
                   encoding="utf-8")
    lint = os.path.join(os.path.dirname(eb.HERE), "scripts", "check-egress-claims.sh")
    proc = subprocess.run(["bash", lint, str(doc)], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    assert "16 object(s) failed" in rows[2] and "4.70 / 9.50" in rows[3]


def test_a_run_overlapping_an_outside_workload_is_labelled_not_dropped():
    samples = [{"timestamp_utc": "2026-11-11T01:00:00Z", "rq4_containers_running": "0"},
               {"timestamp_utc": "2026-11-11T01:10:00Z", "rq4_containers_running": "3"},
               {"timestamp_utc": "2026-11-11T01:40:00Z", "rq4_containers_running": "0"}]
    windows = eb.busy_intervals(samples, "rq4")
    assert len(windows) == 1
    before = {"started_utc": "2026-11-11T01:00:00+00:00", "wall_seconds": "300"}
    across = {"started_utc": "2026-11-11T01:05:00+00:00", "wall_seconds": "600"}
    after = {"started_utc": "2026-11-11T01:41:00+00:00", "wall_seconds": "60"}
    assert [eb.contention_label(m, windows) for m in (before, across, after)] == ["", "rq4", ""]


@pytest.mark.parametrize("value", ["", "nan", "inf"])
def test_a_confirmation_run_without_a_finite_fraction_is_refused_not_judged(value):
    # Blank would read as zero (the whole link unused, opening the gate);
    # "nan" — what collect writes when a run has objects but no busy time —
    # would make the median NaN, which compares false and closes it.
    runs = _gate_runs([0.93, 0.94, 0.95], [0.90, 0.91, 0.92])
    next(r for r in runs if r["profile"] == "wan100" and r["phase"] == "confirm")[
        "fraction_of_link"] = value
    with pytest.raises(ValueError, match="no finite fraction_of_link"):
        eb.evaluate_gate(runs, _objects(runs, {"wan100": 60000, "wan100-loss1": 66000}))


def test_a_point_that_committed_nothing_names_no_bottleneck():
    run = {"run_id": "r", "profile": "wan100-policed", "config": "default", "phase": "grid",
           "c": "", "pif": "", "fraction_of_link": "", "goodput_mbps": "nan", "link_mbps": "10",
           "busy_seconds": "0", "store_cpu_seconds": "1", "engine_cores_busy": "",
           "manifest_mean_ms": "", "cpu_seconds_per_gib": "", "objects_failed": "16",
           "competitor_goodput_mbps_with": "", "competitor_goodput_mbps_without": "",
           "vtop_goodput_mbps_with": "", "competitor_jain_index": ""}
    failed = [{"run_id": "r", "status": "failed", "object_upload_ms": "", "verify_ms": "",
               "batch_total_ms": "0"}]
    (point,) = eb.aggregate_points([run], failed)
    assert point["bottleneck"] == "none named: no object committed"
    assert point["verify_share_of_batch"] == ""


def test_a_value_above_the_chart_scale_is_marked_off_scale_not_drawn_at_the_top():
    def render(value):
        point = {"profile": "wan100", "phase": "grid", "config": "c1-p1", "v": value}
        return eb.svg_small_multiples([point], ("wan100",), "v", "t", "y", 400, [0, 400])

    inside, outside = render("300"), render("900")
    assert "(off scale)" in outside and "(off scale)" not in inside
    # The legend draws circles too; the off-scale point must not add one.
    assert outside.count("<circle") == inside.count("<circle") - 1


# ------------------------------------------- the committed dataset, as a gate

DATA = os.path.join(eb.HERE, "measurements", "483-tcp-baseline")


def _committed_dataset():
    return (eb._read_csv(os.path.join(DATA, "runs.csv")),
            eb._read_csv(os.path.join(DATA, "objects.csv")))


@pytest.mark.parametrize("missing", ["wan100", "wan100-loss1"])
def test_a_gated_profile_with_no_grid_runs_leaves_the_gate_undecided_never_closed(missing):
    # The datagram issues close on this verdict, so a dataset that cannot
    # select a best-tuned point on one required profile must not report the
    # gate closed: that criterion is undecided, and so is the gate.
    runs, objects = _committed_dataset()
    runs = [r for r in runs if not (r["profile"] == missing and r["phase"] == "grid")]
    gate = eb.evaluate_gate(runs, objects)
    assert gate["datagram_issues_open"] is None, gate
    assert gate["profiles"][missing]["opens"] is None, gate["profiles"]


def test_the_committed_verdict_is_closed_and_matches_gate_json():
    runs, objects = _committed_dataset()
    gate = eb.evaluate_gate(runs, objects)
    with open(os.path.join(DATA, "gate.json"), encoding="utf-8") as fh:
        assert json.load(fh) == json.loads(json.dumps(gate, sort_keys=True))
    assert gate["datagram_issues_open"] is False


def test_one_criterion_crossed_opens_the_gate_even_if_another_is_undecided():
    runs = _gate_runs([0.93, 0.94, 0.95], [0.70, 0.72, 0.75])
    runs = [r for r in runs if not (r["profile"] == "wan100" and r["phase"] == "grid")]
    gate = eb.evaluate_gate(runs, _objects(runs, {"wan100": 60000, "wan100-loss1": 66000}))
    assert gate["profiles"]["wan100"]["opens"] is None
    assert gate["profiles"]["wan100-loss1"]["opens"] is True
    assert gate["datagram_issues_open"] is True, (
        "the gate opens if EITHER threshold is crossed; an undecided criterion beside a "
        "crossed one cannot close it")
