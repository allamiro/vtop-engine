"""Every comparison kind's report, drawn from its synthetic fixture run (#744 section 5).

Each `<kind>_compare` package with a `report.py` provides `fixture.write_run(parent)`,
a complete run with made-up numbers. These tests draw it with the pinned plotting
stack, so they run in the benchmark-charts CI job (the generic benchmarks job has
no matplotlib and skips them).
"""
from __future__ import annotations

import csv
import importlib
import json
import os
import re

import pytest

pytest.importorskip("matplotlib")

from compare_common import regenerate, schema  # noqa: E402
from compare_common import report as creport  # noqa: E402

KINDS = regenerate.kinds()


def _build(kind, tmp_path, name):
    fixture = importlib.import_module(f"{kind}_compare.fixture")
    spec = importlib.import_module(f"{kind}_compare.report").REPORT
    run = fixture.write_run(str(tmp_path / f"runs-{name}"))
    out = tmp_path / f"out-{name}"
    written = creport.build(run, spec, str(tmp_path), doc_path=str(out / "page.md"), img_dir=str(out / "img"))
    return spec, written, out


def test_every_kind_is_found():
    assert "archive" in KINDS


@pytest.mark.parametrize("kind", KINDS)
def test_the_fixture_run_draws_every_chart_and_the_page(kind, tmp_path):
    spec, written, out = _build(kind, tmp_path, "a")
    names = {os.path.basename(p) for p in written}
    for chart in spec.charts:
        assert {f"{chart.name}.png", f"{chart.name}.svg"} <= names
    page = (out / "page.md").read_text()
    assert page.startswith(creport.GENERATED_MARK)
    assert "## Scoreboard" in page and "## Where VTOP is behind, level and ahead" in page
    for chart in spec.charts:
        for metric in chart.metrics:
            # The scoreboard has a VTOP row for every metric (#744 section 8 test 10).
            assert re.search(rf"^\| {re.escape(creport.md_cell(metric.label))} \|", page, re.M), metric.label


def test_secondary_profiles_appear_in_the_tables_and_not_in_the_charts_or_ranking(tmp_path):
    # Review of #906: the full plan runs vtop/verify-off and vector/disk-buffer; their values are in the
    # tables as columns of their own, and the scoreboard still ranks primary profiles only.
    _spec, _written, out = _build("archive", tmp_path, "a")
    page = (out / "page.md").read_text()
    assert "verify-off (secondary)" in page and "disk-buffer (secondary)" in page
    scoreboard = page.split("## Scoreboard", 1)[1].split("\n## ", 1)[0]
    ranked = "\n".join(line for line in scoreboard.splitlines() if line.startswith("|"))
    assert "verify-off" not in ranked and "disk-buffer" not in ranked


@pytest.mark.parametrize("kind", KINDS)
def test_the_output_is_deterministic(kind, tmp_path):
    _spec, first, out_a = _build(kind, tmp_path, "a")
    _spec, second, out_b = _build(kind, tmp_path, "b")
    for path in first:
        if path.endswith((".svg", ".md")):
            other = path.replace(str(out_a), str(out_b))
            with open(path, "rb") as a, open(other, "rb") as b:
                left, right = a.read(), b.read()
            if path.endswith(".md"):  # the page names its own run directory
                left, right = left.replace(b"runs-a", b"runs-x"), right.replace(b"runs-b", b"runs-x")
            assert left == right, path


@pytest.mark.parametrize("kind", KINDS)
def test_a_truncated_run_is_refused_with_exit_2_and_nothing_written(kind, tmp_path):
    fixture = importlib.import_module(f"{kind}_compare.fixture")
    report = importlib.import_module(f"{kind}_compare.report")
    run = fixture.write_run(str(tmp_path / "runs"))
    csv_path = os.path.join(run, f"{report.REPORT.kind.name}.csv")
    with open(csv_path) as handle:
        lines = handle.read().splitlines(keepends=True)
    with open(csv_path, "w") as handle:
        handle.writelines(lines[:-3])
    out = tmp_path / "out"
    code = report.main([run, "--doc", str(out / "page.md"), "--img-dir", str(out / "img")])
    assert code == 2 and not out.exists()


@pytest.mark.parametrize("kind", KINDS)
def test_the_footer_is_in_every_image_s_metadata(kind, tmp_path):
    _spec, written, _out = _build(kind, tmp_path, "a")
    for path in written:
        if path.endswith(".svg"):
            with open(path, encoding="utf-8") as handle:
                assert "host class" in handle.read(), path


def test_report_refuses_when_a_system_has_no_row_in_a_cell_it_was_expected_in(tmp_path):
    from archive_compare import fixture, report

    run = fixture.write_run(str(tmp_path / "runs"))
    csv_path = os.path.join(run, "archive.csv")
    with open(csv_path) as handle:
        lines = handle.read().splitlines(keepends=True)
    kept = [line for line in lines if ",vector,acks,live," not in line]
    assert len(kept) < len(lines)
    with open(csv_path, "w") as handle:
        handle.writelines(kept)
    with pytest.raises(schema.RunRefused, match="vector/acks/live"):
        creport.build(run, report.REPORT, str(tmp_path), doc_path=str(tmp_path / "p.md"),
                      img_dir=str(tmp_path / "img"))


# The plans the hosted runners and the AWS round run do not run every cell (no drain or live cells on a hosted
# plan). A chart whose cells did not run is left out and named, not drawn empty above a table with no rows.
PARTIAL_PLANS = {"archive": "hosted", "spool": "hosted-crash"}


@pytest.mark.parametrize("kind", sorted(PARTIAL_PLANS))
def test_a_plan_that_did_not_run_a_charts_cells_leaves_the_chart_out_and_says_so(kind, tmp_path):
    fixture = importlib.import_module(f"{kind}_compare.fixture")
    spec = importlib.import_module(f"{kind}_compare.report").REPORT
    run = fixture.write_run(str(tmp_path / "runs"), plan_name=PARTIAL_PLANS[kind], host_class="hosted")
    with open(os.path.join(run, f"{spec.kind.name}.csv"), newline="") as handle:
        ran = {row["cell"] for row in csv.DictReader(handle)}
    out = tmp_path / "out"
    written = creport.build(run, spec, str(tmp_path), doc_path=str(out / "page.md"), img_dir=str(out / "img"))
    page = (out / "page.md").read_text()
    names = {os.path.basename(p) for p in written}
    drawn = [c for c in spec.charts if ran & {cell for m in c.metrics for cell in m.cells}]
    left_out = [c for c in spec.charts if c not in drawn]
    assert drawn and left_out, "the partial plan must run some of the charts' cells and not others"
    for chart in drawn:
        assert f"{chart.name}.png" in names and f"![{chart.title}](" in page
    for chart in left_out:
        assert f"{chart.name}.png" not in names and f"![{chart.title}](" not in page
    assert "Not run in this plan, so not drawn: " + ", ".join(c.title for c in left_out) + "." in page


@pytest.mark.parametrize("kind", KINDS)
def test_a_full_plan_draws_every_chart_and_says_nothing_was_left_out(kind, tmp_path):
    _spec, _written, out = _build(kind, tmp_path, "a")
    assert "Not run in this plan" not in (out / "page.md").read_text()


@pytest.mark.parametrize("kind", KINDS)
def test_a_report_json_sends_the_run_s_page_and_images_beside_the_default_ones(kind, tmp_path):
    fixture = importlib.import_module(f"{kind}_compare.fixture")
    spec = importlib.import_module(f"{kind}_compare.report").REPORT
    run = fixture.write_run(str(tmp_path / "runs"))
    with open(os.path.join(run, "report.json"), "w") as handle:
        json.dump({"doc": f"docs/benchmarks/{kind}-comparison-aws.md",
                   "img_dir": f"docs/benchmarks/{spec.img_dir}/aws"}, handle)
    written = creport.build(run, spec, str(tmp_path))
    assert str(tmp_path / "docs" / "benchmarks" / f"{kind}-comparison-aws.md") in written
    assert all(p.startswith(str(tmp_path / "docs" / "benchmarks")) for p in written)
    assert not (tmp_path / "docs" / "benchmarks" / f"{spec.doc_name}.md").exists()
    assert (tmp_path / "docs" / "benchmarks" / spec.img_dir / "aws").is_dir()
    page = (tmp_path / "docs" / "benchmarks" / f"{kind}-comparison-aws.md").read_text()
    assert f"![{spec.charts[0].title}](" + os.path.relpath(
        tmp_path / "docs" / "benchmarks" / spec.img_dir / "aws" / f"{spec.charts[0].name}.png",
        tmp_path / "docs" / "benchmarks") in page


def test_a_report_json_outside_the_kinds_directories_is_refused_as_a_refused_run(tmp_path):
    from archive_compare import fixture, report

    run = fixture.write_run(str(tmp_path / "runs"))
    with open(os.path.join(run, "report.json"), "w") as handle:
        json.dump({"doc": "docs/benchmarks/a.md", "img_dir": "docs/benchmarks/img/a"}, handle)
    with pytest.raises(schema.RunRefused, match="must be a directory under docs/benchmarks/archive/img/"):
        creport.build(run, report.REPORT, str(tmp_path))
    assert not (tmp_path / "docs").exists()
