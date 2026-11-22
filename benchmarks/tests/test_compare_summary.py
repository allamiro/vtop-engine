"""The cross-product summary page, without the plotting stack (#744 section 5).

What needs matplotlib (a page with charts) is in test_compare_summary_charts.py, which runs in the
benchmark-charts CI job. Here: the chart list, the rules, the page with nothing measured, the refusals
(a run the loader refuses, never drawn), and the wiring into regenerate.py, the docs nav and CI.
"""
from __future__ import annotations

import importlib
import os
import re

import pytest

from compare_common import regenerate, registry, schema, summary
from compare_common import report as creport

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.dirname(HERE)
REPO = os.path.dirname(BENCH)


def _page(tmp_path, runs=None, name="p"):
    doc = tmp_path / f"{name}.md"
    img = tmp_path / f"{name}-img"
    summary.build(runs or {}, str(tmp_path), doc_path=str(doc), img_dir=str(img))
    return doc.read_text(), img


def test_every_use_case_a_to_e_has_charts_from_a_kind_that_measures_it():
    summary.check_summary_spec()
    assert [case.id for case in summary.USE_CASES] == list("abcde")
    for case in summary.USE_CASES:
        assert case.charts
        for chart in case.charts:
            assert case.id in importlib.import_module(f"{chart.kind}_compare.report").REPORT.kind.use_cases


def test_the_summary_knows_every_comparison_kind():
    assert sorted(summary.KIND_ORDER) == sorted(regenerate.kinds())


def test_the_headline_metrics_are_all_drawn():
    # #744 section 5.1: throughput, crash gaps and duplicates, verification behaviour, CPU and bytes per GiB.
    wanted = {"records", "lines", "goodput", "gaps", "dups", "past", "cpu", "wire", "get", "committed"}
    drawn = {m for case in summary.USE_CASES for chart in case.charts for m in chart.metrics}
    assert wanted <= drawn


def test_chart_names_are_unique_and_prefixed():
    names = [chart.name for case in summary.USE_CASES for chart in case.charts]
    assert len(names) == len(set(names)) and all(n.startswith("summary-") for n in names)


def test_rules_1_to_21_are_stated_in_order_on_the_page(tmp_path):
    assert [n for n, _ in summary.RULES] == list(range(1, 22))
    page, _ = _page(tmp_path)
    section = page.split("## Fairness rules", 1)[1].split("\n## ", 1)[0]
    for number, text in summary.RULES:
        assert f"\n{number}. {text}" in section


def test_nothing_measured_says_not_measured_yet_in_every_cell_and_draws_nothing(tmp_path):
    page, img = _page(tmp_path)
    assert page.startswith(summary.GENERATED_MARK)
    assert not img.exists() and "![" not in page
    assert "Nothing is measured yet" in page
    reg = registry.load()
    for case in summary.USE_CASES:
        for chart in case.charts:
            block = page.split(f"### {chart.title}\n", 1)[1].split("\n##", 1)[0]
            spec = summary.report_spec(chart.kind)
            systems = summary.expected_systems(chart.kind, reg)
            for metric_id in chart.metrics:
                metric = summary.metric_of(spec, metric_id)
                line = next(row for row in block.splitlines() if row.startswith(f"| {creport.md_cell(metric.label)} |"))
                assert line.count(summary.NOT_MEASURED) == len(systems), line
    assert not re.search(r"\d\.\d", page.split("## Products", 1)[0].split("## What is measured yet", 1)[1]
                         .replace("2026-", "")), "a number appeared although nothing is measured"


def test_the_page_is_deterministic_with_nothing_measured(tmp_path):
    one, _ = _page(tmp_path, name="a")
    two, _ = _page(tmp_path, name="b")
    assert one.replace("a-img", "x") == two.replace("b-img", "x")


def test_the_committed_page_is_the_one_the_committed_runs_produce():
    # With no committed run (the measured runs need the maintainer's hardware) the page is the all-empty one:
    # a hand edit, or a change to this module without regenerating, is drift. CI's regenerate step covers the
    # case with runs.
    if any(regenerate.committed_runs(kind) for kind in summary.KIND_ORDER):
        pytest.skip("a kind has a committed run: regenerate.py is the check")
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        doc = os.path.join(tmp, "page.md")
        summary.build({}, REPO, doc_path=doc, img_dir=os.path.join(tmp, "img"))
        with open(doc, encoding="utf-8") as fresh, open(os.path.join(REPO, summary.DOC_REL), encoding="utf-8") as kept:
            assert fresh.read() == kept.read(), "docs/benchmarks/comparison-summary.md: run regenerate.py"


def test_the_products_table_states_licence_version_and_evidence_from_the_registry(tmp_path):
    page, _ = _page(tmp_path)
    reg = registry.load()
    table = page.split("## Products, versions and licences", 1)[1].split("\n## ", 1)[0]
    for pid in ("vtop", "kafka", "connect-lenses", "vector", "fluent-bit", "otel-collector", "aws-cli", "rclone",
                "s5cmd"):
        entry = registry.product(reg, pid)
        row = next(line for line in table.splitlines() if line.startswith(f"| {creport.md_cell(entry['name'])} |"))
        assert creport.md_cell(entry["licence"]) in row, row
        assert entry["evidence_url"] in row.replace("\\", "") and entry["retrieved"] in row
    assert registry.product(reg, "vtop")["licence"] == "MIT"
    assert "VTOP's licence is MIT" in page


def test_a_product_that_may_not_be_published_is_never_listed(tmp_path):
    page, _ = _page(tmp_path)
    reg = registry.load()
    for entry in reg["products"].values():
        if not entry["publish_ok"]:
            assert entry["name"] not in page


def test_the_limits_name_one_host_minio_and_the_plan(tmp_path):
    page, _ = _page(tmp_path)
    limits = page.split("## Limits of this evidence", 1)[1].split("\n## ", 1)[0]
    assert "One host" in limits and "MinIO" in limits and "plan" in limits


def test_a_stub_that_means_nothing_was_recorded_reads_not_measured_yet():
    metric = creport.Metric("m", "label", "col", "higher", ("c",))
    assert summary._fmt(creport.Value(None, "no row", 0), metric) == summary.NOT_MEASURED
    assert summary._fmt(creport.Value(None, "not measured", 1), metric) == summary.NOT_MEASURED
    assert summary._fmt(creport.Value(None, "failed", 3), metric) == "failed"
    assert summary._fmt(creport.Value(None, "not sustained", 3), metric) == "not sustained"


# ---------------------------------------------------------------- refusals


def _fixture_run(kind, tmp_path):
    return importlib.import_module(f"{kind}_compare.fixture").write_run(str(tmp_path / f"runs-{kind}"))


def test_a_run_the_loader_refuses_is_refused_with_exit_2_and_nothing_written(tmp_path):
    import json

    run = _fixture_run("archive", tmp_path)
    path = os.path.join(run, "environment.json")
    with open(path, encoding="utf-8") as handle:
        env = json.load(handle)
    env["run_state"] = "running"
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(env, handle)
    out = tmp_path / "out"
    code = summary.main(["--run", f"archive={run}", "--doc", str(out / "p.md"), "--img-dir", str(out / "img")])
    assert code == 2 and not out.exists()


def test_a_private_run_is_never_drawn(tmp_path):
    import json

    run = _fixture_run("spool", tmp_path)
    path = os.path.join(run, "environment.json")
    with open(path, encoding="utf-8") as handle:
        env = json.load(handle)
    env["private"] = True
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(env, handle)
    out = tmp_path / "out"
    assert summary.main(["--run", f"spool={run}", "--doc", str(out / "p.md"), "--img-dir", str(out / "img")]) == 2
    assert not out.exists()


def test_a_run_of_the_wrong_kind_a_kind_named_twice_and_an_unknown_kind_are_refused(tmp_path, capsys):
    run = _fixture_run("spool", tmp_path)
    out = tmp_path / "out"
    base = ["--doc", str(out / "p.md"), "--img-dir", str(out / "img")]
    assert summary.main(["--run", f"archive={run}", *base]) == 2  # a spool run offered as the archive run
    assert summary.main(["--run", f"spool={run}", "--run", f"spool={run}", *base]) == 2
    assert summary.main(["--run", f"kafka={run}", *base]) == 2
    assert summary.main(["--run", "spool", *base]) == 2
    assert not out.exists()
    assert "summary refused" in capsys.readouterr().err


def test_two_committed_runs_of_a_kind_are_refused(monkeypatch):
    monkeypatch.setattr(regenerate, "committed_runs",
                        lambda kind, repo=REPO: ["/r/one", "/r/two"] if kind == "link" else [])
    with pytest.raises(schema.RunRefused, match="2 committed link runs"):
        summary.committed_runs_by_kind()


# ------------------------------------------------------------------ wiring


def test_regenerate_draws_the_summary_from_the_runs_it_found_and_checks_its_output(monkeypatch):
    seen = {}
    monkeypatch.setattr(regenerate, "committed_runs",
                        lambda kind, repo=REPO: [f"/runs/{kind}-1"] if kind == "link" else [])
    monkeypatch.setattr(regenerate.kr, "check_stack", lambda pins: None)
    monkeypatch.setattr(regenerate.kr, "check_font", lambda: None)
    monkeypatch.setattr(regenerate.kr, "check_runs_unchanged", lambda runs: None)
    monkeypatch.setattr(regenerate, "run_report", lambda kind, run: [f"/out/{kind}.md"])

    def build(runs, repo):
        seen["runs"] = runs
        return ["/out/summary.md"]

    def orphans(outputs, written):
        seen["outputs"], seen["written"] = outputs, written
        return []

    monkeypatch.setattr(summary, "build", build)
    monkeypatch.setattr(regenerate, "orphans", orphans)
    monkeypatch.setattr(regenerate.kr, "keep_committed_pngs", lambda owner: [])
    assert regenerate.main([]) == 0
    assert seen["runs"] == {"link": "/runs/link-1"}
    assert seen["outputs"]["summary"] == (summary.IMG_REL, summary.DOC_REL)
    assert os.path.realpath("/out/summary.md") in seen["written"]


def test_regenerate_refuses_when_the_summary_cannot_be_drawn(monkeypatch, capsys):
    monkeypatch.setattr(regenerate, "committed_runs", lambda kind, repo=REPO: [])
    monkeypatch.setattr(regenerate.kr, "check_runs_unchanged", lambda runs: None)

    def build(runs, repo):
        raise schema.RunRefused("a run is private")

    monkeypatch.setattr(summary, "build", build)
    assert regenerate.main([]) == 2
    assert "the summary cannot be drawn: a run is private" in capsys.readouterr().err


def test_the_docs_nav_lists_the_summary_page_and_the_page_exists():
    with open(os.path.join(REPO, "mkdocs.yml"), encoding="utf-8") as handle:
        nav = handle.read()
    assert summary.DOC_REL.replace("docs/", "") in nav
    assert os.path.isfile(os.path.join(REPO, summary.DOC_REL))


def test_the_chart_job_runs_the_summary_tests_and_regenerate_covers_them():
    with open(os.path.join(REPO, ".github", "workflows", "ci.yml"), encoding="utf-8") as handle:
        ci = handle.read()
    assert "tests/test_compare_summary_charts.py" in ci  # the file that needs matplotlib
    assert "benchmarks/compare_common/regenerate.py" in ci
