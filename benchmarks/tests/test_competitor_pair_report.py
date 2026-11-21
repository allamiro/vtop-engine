"""The competitor-pair job's table and committed record (#610): every figure is
read from a half's own summary.json, a refused half is listed without figures,
and only a repetition whose two halves were both accepted counts as a pair."""
from __future__ import annotations

import csv
import json
import os

import competitor_pair_report as report


def _half(out, order, repetition, scenario, exit_code, run_id, summary=None):
    with open(os.path.join(out, "runs.tsv"), "a") as handle:
        if handle.tell() == 0:
            handle.write("order\trepetition\tscenario\texit_code\trun_id\n")
        handle.write(f"{order}\t{repetition}\t{scenario}\t{exit_code}\t{run_id}\n")
    if summary is not None:
        run_dir = os.path.join(out, "results", run_id)
        os.makedirs(run_dir)
        with open(os.path.join(run_dir, "summary.json"), "w") as handle:
            json.dump(summary, handle)
        with open(os.path.join(run_dir, "metrics.csv"), "w") as handle:
            handle.write("cycle\n1\n")


def _summary(with_mbps, share, jain, cap=""):
    return {
        "upload_max_egress_bytes_per_second": cap,
        "emulator_validation_mbps": 9.55,
        "competitor_goodput_mbps_with": with_mbps,
        "competitor_goodput_mbps_without": 9.56,
        "vtop_goodput_mbps_with": 3.0,
        "competitor_jain_index": jain,
        "successful_batches": 100,
        "failed_batches": 0,
        "competitor": {"share_of_solo_pct": share, "vtop_attributed_cycles": 4, "vtop_window_seconds": 80.0,
                       "alone_before": {"goodput_mbps": 9.56}, "alone_after": {"goodput_mbps": 9.57}},
    }


def test_a_missing_runs_file_means_the_pair_never_started(tmp_path, capsys):
    assert report.main([str(tmp_path)]) == 0
    assert "never started" in capsys.readouterr().out
    assert not (tmp_path / "pair_runs.csv").exists()


def test_a_pair_is_a_repetition_whose_two_halves_were_both_accepted(tmp_path):
    out = str(tmp_path)
    _half(out, 1, 1, 22, 0, "u1", _summary(5.0, 52.0, 0.95))
    _half(out, 2, 1, 23, 0, "c1", _summary(7.5, 78.0, 0.99, cap=262144))
    _half(out, 3, 2, 23, 2, "c2")  # refused: no summary
    _half(out, 4, 2, 22, 0, "u2", _summary(5.2, 54.0, 0.96))
    assert report.main([out]) == 0

    record = json.load(open(tmp_path / "pair_record.json"))
    assert (record["halves"], record["halves_accepted"], record["halves_refused_or_failed"]) == (4, 3, 1)
    assert record["complete_pairs"] == 1 and record["repetitions"] == 2
    first, second = record["pairs"]
    assert first["complete"] and not second["complete"]
    assert first["without_cap"]["competitor_goodput_mbps_with"] == 5.0
    assert first["with_cap"]["upload_max_egress_bytes_per_second"] == 262144
    assert first["competitor_goodput_mbps_with_minus_without_cap"] == 2.5
    assert second["capped_exit_code"] == "2" and "without_cap" not in second
    assert record["median_over_complete_pairs"]["with_cap"]["share_of_solo_pct"] == 78.0


def test_the_csv_lists_a_refused_half_without_figures(tmp_path):
    out = str(tmp_path)
    _half(out, 1, 1, 22, 2, "refused")
    report.main([out])
    rows = list(csv.DictReader(open(tmp_path / "pair_runs.csv")))
    assert rows[0]["exit_code"] == "2" and rows[0]["competitor_goodput_mbps_with"] == ""


def test_medians_are_over_complete_pairs_only(tmp_path):
    out = str(tmp_path)
    for rep, (u, c) in enumerate([(5.0, 7.0), (5.4, 7.4), (9.9, 9.9)], start=1):
        _half(out, rep * 2 - 1, rep, 22, 0, f"u{rep}", _summary(u, 50.0, 0.95))
        _half(out, rep * 2, rep, 23, 0 if rep < 3 else 2, f"c{rep}", _summary(c, 70.0, 0.99) if rep < 3 else None)
    report.main([out])
    record = json.load(open(tmp_path / "pair_record.json"))
    assert record["complete_pairs"] == 2
    assert record["median_over_complete_pairs"]["without_cap"]["competitor_goodput_mbps_with"] == 5.2


def test_the_bundle_keeps_only_accepted_halves_raw_files(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    (out / "environment").mkdir()
    (out / "environment" / "environment.json").write_text("{}")
    (out / "logs").mkdir()
    (out / "logs" / "1.log").write_text("x")
    _half(str(out), 1, 1, 22, 0, "ok", _summary(5.0, 52.0, 0.95))
    _half(str(out), 2, 1, 23, 2, "bad")
    dest = tmp_path / "bundle"
    assert report.main([str(out), "--bundle", str(dest)]) == 0
    assert (dest / "runs" / "ok" / "summary.json").exists() and (dest / "runs" / "ok" / "metrics.csv").exists()
    assert not (dest / "runs" / "bad").exists()
    for name in ("runs.tsv", "pair_runs.csv", "pair_record.json", "environment/environment.json", "logs/1.log"):
        assert (dest / name).exists(), name
