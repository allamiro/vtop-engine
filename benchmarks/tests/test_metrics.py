"""Tests for benchmarks/lib/metrics.py.

The harness had zero tests while accumulating several real defects, two of which
are pinned here: Markdown tables emitted unescaped values (a `|` in any field
silently corrupted the table), and percentile maths is the sort of thing that is
quietly wrong at the edges.
"""

import math

import pytest

from lib.metrics import _md_cell, _summary_md, new_run_id, percentile

# --------------------------------------------------------------------------
# Markdown escaping — a real bug: an unescaped `|` breaks the whole table.
# --------------------------------------------------------------------------


def test_md_cell_escapes_pipes():
    # A raw pipe would end the cell early and shift every later column.
    assert _md_cell("a|b") == r"a\|b"


def test_md_cell_escapes_backslashes_before_pipes():
    # Order matters: escaping `|` first would turn `\` + `|` into `\\|`, which
    # renders as a literal backslash followed by a column break.
    assert _md_cell(r"a\b") == r"a\\b"
    assert _md_cell(r"a\|b") == r"a\\\|b"


def test_md_cell_flattens_newlines():
    # A newline inside a cell terminates the table row in Markdown.
    assert "\n" not in _md_cell("line1\nline2")
    assert "\r" not in _md_cell("line1\r\nline2")
    assert _md_cell("line1\nline2") == "line1 line2"


def test_md_cell_accepts_non_strings():
    assert _md_cell(42) == "42"
    assert _md_cell(None) == "None"
    assert _md_cell(1.5) == "1.5"


def test_summary_md_escapes_hostile_values():
    # An end-to-end guard: a scenario name containing a pipe must not be able to
    # corrupt the generated summary table.
    md = _summary_md(
        {
            "scenario_name": "evil|name",
            "run_id": "r1",
            "start_time": "t",
            "end_time": "t",
            "scenario": {"format": "js|onl"},
        }
    )
    assert r"evil\|name" in md, "scenario name was not escaped"
    # And a hostile value nested in the scenario knobs table.
    assert r"js\|onl" in md, "scenario knob value was not escaped"


# --------------------------------------------------------------------------
# Percentiles
# --------------------------------------------------------------------------


def test_percentile_empty_is_zero_not_an_error():
    # Called on runs that produced no batches; must not raise.
    assert percentile([], 95) == 0.0


def test_percentile_single_value():
    assert percentile([7.0], 50) == 7.0
    assert percentile([7.0], 99) == 7.0


def test_percentile_bounds_are_min_and_max():
    vals = [1.0, 2.0, 3.0, 4.0, 100.0]
    assert percentile(vals, 0) == 1.0
    assert percentile(vals, 100) == 100.0


def test_percentile_interpolates_between_samples():
    # p50 of 1..4 sits between 2 and 3 -> 2.5 with linear interpolation.
    assert percentile([1.0, 2.0, 3.0, 4.0], 50) == 2.5


def test_percentile_is_order_independent():
    a = percentile([5.0, 1.0, 4.0, 2.0, 3.0], 75)
    b = percentile([1.0, 2.0, 3.0, 4.0, 5.0], 75)
    assert a == b


def test_percentile_is_monotonic():
    vals = [float(i) for i in range(1, 101)]
    seq = [percentile(vals, p) for p in (0, 25, 50, 75, 90, 99, 100)]
    assert seq == sorted(seq), f"percentiles must not decrease: {seq}"


@pytest.mark.parametrize("pct", [0, 1, 50, 95, 99, 100])
def test_percentile_result_is_within_the_data_range(pct):
    vals = [3.0, 9.0, 27.0, 81.0]
    got = percentile(vals, pct)
    assert min(vals) <= got <= max(vals)
    assert not math.isnan(got)


# --------------------------------------------------------------------------
# Run ids
# --------------------------------------------------------------------------


def test_new_run_id_includes_scenario_and_is_unique():
    a = new_run_id("my-scenario")
    b = new_run_id("my-scenario")
    assert "my-scenario" in a
    assert a != b, "run ids must be unique or results overwrite each other"
