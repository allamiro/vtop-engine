"""Every comparison kind's report, drawn from its synthetic fixture run (#744 section 5).

Each `<kind>_compare` package with a `report.py` provides `fixture.write_run(parent)`,
a complete run with made-up numbers. These tests draw it with the pinned plotting
stack, so they run in the benchmark-charts CI job (the generic benchmarks job has
no matplotlib and skips them).
"""
from __future__ import annotations

import importlib
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
    kept = [line for line in lines if not (",vector,acks,live," in line)]
    assert len(kept) < len(lines)
    with open(csv_path, "w") as handle:
        handle.writelines(kept)
    with pytest.raises(schema.RunRefused, match="vector/acks/live"):
        creport.build(run, report.REPORT, str(tmp_path), doc_path=str(tmp_path / "p.md"),
                      img_dir=str(tmp_path / "img"))
