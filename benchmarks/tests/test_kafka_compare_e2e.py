"""Metric 3's histogram and its rules (#596)."""
import pytest

from kafka_compare import e2e


def test_buckets_are_1_ms_wide_with_an_overflow_and_a_negative_bucket():
    assert e2e.histogram([0, 0, 5, 59_999, 60_000, 3_600_000, -3]) == {
        -1: 1, 0: 2, 5: 1, 59_999: 1, 60_000: 2}


def test_histogram_counts_must_sum_to_consumed():
    outcome = e2e.judge({1: 600, 2: 400}, produced=1000, offered_rate=100, achieved_rate=100)
    assert outcome == e2e.Outcome("ok", 1000, 1000, True, "")


def test_a_negative_bucket_is_refused():
    with pytest.raises(e2e.E2eRefused, match="before their CreateTime"):
        e2e.judge({-1: 1, 3: 999}, produced=1000, offered_rate=100, achieved_rate=100)


def test_produced_not_equal_consumed_makes_the_row_failed_with_the_missing_count():
    outcome = e2e.judge({1: 990}, produced=1000, offered_rate=100, achieved_rate=100)
    assert (outcome.status, outcome.note) == ("failed", "missing 10 records")
    outcome = e2e.judge({1: 1001}, produced=1000, offered_rate=100, achieved_rate=100)
    assert outcome.status == "failed" and "more records" in outcome.note


def test_an_unsustained_e2e_run_is_marked_unsustained():
    outcome = e2e.judge({1: 1000}, produced=1000, offered_rate=1000, achieved_rate=500)
    assert outcome.status == "ok" and outcome.sustained is False
    assert "not sustained" in outcome.note


def test_the_consumers_lines_parse_and_bad_ones_are_refused():
    assert e2e.parse_histogram("0 5\n12 3\n60000 1\n") == {0: 5, 12: 3, 60_000: 1}
    for bad in ("0 5\n0 6\n", "60001 1\n", "x 1\n", "1 2 3\n", "-2 1\n"):
        with pytest.raises(e2e.E2eRefused):
            e2e.parse_histogram(bad)


def test_percentiles_come_from_the_histogram():
    hist = {1: 50, 2: 40, 10: 9, 100: 1}
    assert e2e.percentile_ms(hist, 0.5) == 1
    assert e2e.percentile_ms(hist, 0.99) == 10
    assert e2e.percentile_ms(hist, 1.0) == 100
