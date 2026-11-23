"""The cross-product summary with charts: nothing, some or every kind measured (#744 section 5).

Drawn from the three kinds' synthetic fixture runs with the pinned plotting stack, so this runs in the
benchmark-charts CI job (the generic benchmarks job has no matplotlib and skips it).
"""
from __future__ import annotations

import csv
import importlib
import os
import re

import pytest

pytest.importorskip("matplotlib")

from compare_common import registry, summary  # noqa: E402
from compare_common import report as creport  # noqa: E402


def _runs(tmp_path, kinds, tag="r"):
    return {kind: importlib.import_module(f"{kind}_compare.fixture").write_run(str(tmp_path / f"{tag}-{kind}"))
            for kind in kinds}


def _build(tmp_path, kinds, name="a", runs=None):
    runs = runs if runs is not None else _runs(tmp_path, kinds, name)
    doc, img = tmp_path / f"{name}.md", tmp_path / f"{name}-img"
    written = summary.build(runs, str(tmp_path), doc_path=str(doc), img_dir=str(img))
    return doc.read_text(), img, written


def test_every_kind_measured_draws_every_chart_with_a_source_and_a_scoreboard_row_per_metric(tmp_path):
    page, img, written = _build(tmp_path, summary.KIND_ORDER)
    names = {os.path.basename(p) for p in written}
    for case in summary.USE_CASES:
        for chart in case.charts:
            assert {f"{chart.name}.png", f"{chart.name}.svg"} <= names
            block = page.split(f"### {chart.title}\n", 1)[1].split("\n##", 1)[0]
            assert f"![{chart.title}]" in block and "Source: `" in block
            spec = summary.report_spec(chart.kind)
            for metric_id in chart.metrics:
                label = creport.md_cell(summary.metric_of(spec, metric_id).label)
                # the metric's table, then its scoreboard rows (#744 section 8 test 10: VTOP's is always there)
                assert block.count(f"**{label}**") == 1
                assert re.search(rf"^\| {re.escape(label)} \| .* \|$", block, re.M)
    # Every kind is measured: the status table and the verdicts say so. (A table may still say "not measured
    # yet" in a secondary-profile column whose profile the plan does not run in that cell.)
    top = page.split("## (a)", 1)[0]
    assert "not measured yet" not in top.replace("What is measured yet", "")


def test_vtop_last_is_in_the_scoreboard_and_in_the_sentences_that_say_it_is_behind(tmp_path):
    # The fixtures give VTOP the worst archive throughput and the worst clean-link goodput on purpose.
    page, _img, _ = _build(tmp_path, ("archive", "link"))
    behind = page.split("## Where VTOP is behind, level and ahead", 1)[1].split("\n## ", 1)[0]
    assert "VTOP is behind on:" in behind and "records/s" in behind
    row = next(line for line in page.splitlines() if line.startswith("| records/s | drain, 1 KiB |"))
    assert row.rstrip().endswith("| behind |")


def test_some_kinds_measured_and_the_rest_not_measured_yet(tmp_path):
    page, img, written = _build(tmp_path, ("archive",))
    names = {os.path.basename(p) for p in written}
    assert "summary-a-throughput.png" in names and "summary-d-archive-crash.png" in names
    assert not any(n.startswith(("summary-b", "summary-c", "summary-d-spool")) for n in names)
    status = page.split("## What is measured yet", 1)[1].split("\n## ", 1)[0]
    assert re.search(r"^\| archive \| .* \| `archive-fixture` \|", status, re.M)
    assert re.search(r"^\| link \| \(c\) \| not measured yet \|", status, re.M)
    spool = page.split("### (b) Drain throughput\n", 1)[1].split("\n##", 1)[0]
    assert "not measured yet" in spool and "![" not in spool
    both = page.split("### (d) Spool to object storage", 1)[1].split("\n##", 1)[0]
    assert "not measured yet" in both
    assert "(d) Kafka to object storage: what a kill -9 costs\n\nSummed" in page and "![(d) Kafka" in page
    assert not (img / "summary-c-goodput.png").exists()


def test_a_failed_tool_is_a_stub_in_the_chart_and_the_table_not_a_missing_bar(tmp_path):
    runs = _runs(tmp_path, ("archive",))
    path = os.path.join(runs["archive"], "archive.csv")
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        fields, rows = reader.fieldnames, list(reader)
    for row in rows:
        if row["system"] == "vector" and row["profile"] == "acks" and row["cell"] == "drain":
            row.update(status="failed", note="vector exited 1: sink error")
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    page, _img, written = _build(tmp_path, (), runs=runs)
    assert any(p.endswith("summary-a-throughput.svg") for p in written)
    table = page.split("**records/s**", 1)[1].split("\n\n", 2)[1]
    line = next(line for line in table.splitlines() if line.startswith("| drain, 1 KiB |"))
    assert "| failed |" in line
    scored = next(line for line in page.splitlines() if line.startswith("| records/s | drain, 1 KiB |"))
    assert "Vector" not in scored.split("|")[3], "a failed tool has no value to be best with"


def test_each_chart_s_source_tool_versions_and_licences_come_from_its_run_and_the_registry(tmp_path):
    page, _img, _ = _build(tmp_path, summary.KIND_ORDER)
    products = page.split("## Products, versions and licences", 1)[1].split("\n## ", 1)[0]
    reg = registry.load()
    for pid in ("vector", "fluent-bit", "rclone"):
        name = registry.product(reg, pid)["name"]
        row = next(line for line in products.splitlines() if line.startswith(f"| {name} |"))
        assert "fixture" in row, row  # the version the run recorded, not the registry's pin
        assert f" {registry.product(reg, pid)['licence']} " in row
    vtop = next(line for line in products.splitlines() if line.startswith("| VTOP |"))
    assert "commit 000000000000" in vtop and "MIT" in vtop


def test_the_output_is_deterministic(tmp_path):
    runs = _runs(tmp_path, summary.KIND_ORDER)
    _p1, img_a, first = _build(tmp_path, (), name="a", runs=runs)
    _p2, img_b, second = _build(tmp_path, (), name="b", runs=runs)
    assert len(first) == len(second)
    for path in first:
        if path.endswith((".svg", ".md")):
            other = path.replace(str(tmp_path / "a"), str(tmp_path / "b"))
            with open(path, "rb") as a, open(other, "rb") as b:
                left, right = a.read(), b.read()
            assert left.replace(b"a-img", b"x").replace(b"/a.md", b"") == right.replace(b"b-img", b"x") \
                .replace(b"/b.md", b""), path


def test_the_footer_is_in_every_summary_chart_s_metadata(tmp_path):
    _page, _img, written = _build(tmp_path, summary.KIND_ORDER)
    svgs = [p for p in written if p.endswith(".svg")]
    assert svgs
    for path in svgs:
        with open(path, encoding="utf-8") as handle:
            assert "host class" in handle.read(), path


def test_the_summary_draws_each_chart_with_the_family_colours_of_its_kind(tmp_path):
    from compare_common import colours

    reg = registry.load()
    for case in summary.USE_CASES:
        for chart in case.charts:
            kind = summary.report_spec(chart.kind).kind
            order = registry.family_order(reg, kind.family)
            drawn = colours.series(order, summary.expected_systems(chart.kind, reg))
            assert drawn["vtop"] == colours.SLOTS[1][1], "VTOP is always slot 2, orange"
