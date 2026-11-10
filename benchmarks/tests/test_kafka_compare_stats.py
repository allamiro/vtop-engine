"""The arithmetic of the Kafka comparison (#595 rules 6 and 7): percentiles,
the median-and-spread reduction, and the offered rates derived from Kafka's
saturation."""
import pytest

from kafka_compare import stats

# ------------------------------------------------------------------ percentile


def test_percentile_interpolates_linearly_between_closest_ranks():
    # numpy.percentile([1, 2, 3, 4], q) with the default `linear` method.
    assert stats.percentile([1, 2, 3, 4], 50) == 2.5
    assert stats.percentile([1, 2, 3, 4], 25) == 1.75
    assert stats.percentile([1, 2, 3, 4], 75) == 3.25


def test_percentile_endpoints_are_the_extremes():
    data = [7, 3, 9, 1]
    assert stats.percentile(data, 0) == 1
    assert stats.percentile(data, 100) == 9


def test_percentile_does_not_depend_on_input_order():
    assert stats.percentile([5, 1, 4, 2, 3], 50) == stats.percentile([1, 2, 3, 4, 5], 50) == 3


def test_tail_percentiles_of_one_to_a_hundred():
    data = list(range(1, 101))
    assert stats.percentile(data, 99) == pytest.approx(99.01)
    assert stats.percentile(data, 99.9) == pytest.approx(99.901)


def test_percentile_of_one_value_is_that_value():
    assert stats.percentile([42.0], 99.9) == 42.0


@pytest.mark.parametrize("q", [-0.1, 100.1, float("nan")])
def test_percentile_refuses_q_outside_range(q):
    with pytest.raises(ValueError):
        stats.percentile([1, 2, 3], q)


def test_percentile_of_nothing_is_an_error_not_zero():
    with pytest.raises(ValueError, match="empty"):
        stats.percentile([], 50)


def test_percentile_refuses_nan_in_the_sample():
    with pytest.raises(ValueError, match="NaN"):
        stats.percentile([1.0, float("nan")], 50)


# ------------------------------------------------------------------- summarize


def test_summary_is_median_and_min_max_not_the_best_run():
    summary = stats.summarize([220.0, 400.0, 290.0])
    assert summary.median == 290.0
    assert (summary.low, summary.high, summary.n) == (220.0, 400.0, 3)


def test_summary_of_an_even_count_is_the_midpoint():
    assert stats.summarize([1.0, 2.0, 3.0, 10.0]).median == 2.5


def test_summary_of_zero_repetitions_is_refused():
    with pytest.raises(ValueError):
        stats.summarize([])


# --------------------------------------------------------------- offered rates


def test_offered_rates_are_fractions_of_the_median_saturation():
    # Median of the three repetitions (200), not the best (300) or the mean.
    assert stats.derive_offered_rates([100.0, 300.0, 200.0]) == {25: 50, 50: 100, 75: 150}


def test_offered_rates_floor_so_no_rate_exceeds_its_stated_fraction():
    rates = stats.derive_offered_rates([1001.0])
    assert rates == {25: 250, 50: 500, 75: 750}
    for fraction, rate in rates.items():
        assert rate <= 1001.0 * fraction / 100


def test_offered_rates_use_only_the_fractions_asked_for():
    assert stats.derive_offered_rates([400.0], fractions=(10, 90)) == {10: 40, 90: 360}


@pytest.mark.parametrize("bad", [[], [0.0], [-5.0], [float("nan")], [float("inf")], [100.0, 0.0]])
def test_offered_rates_refuse_a_saturation_that_is_not_a_positive_rate(bad):
    with pytest.raises(ValueError):
        stats.derive_offered_rates(bad)


def test_offered_rates_refuse_a_rate_below_one_record_per_second():
    # 25% of 3 records/s floors to 0, which the perf tool would read as a
    # meaningless throttle; refused rather than silently offered.
    with pytest.raises(ValueError, match="below 1 record/s"):
        stats.derive_offered_rates([3.0])


@pytest.mark.parametrize("fraction", [0, 100, 150])
def test_offered_rates_refuse_fractions_outside_one_to_ninety_nine(fraction):
    with pytest.raises(ValueError):
        stats.derive_offered_rates([1000.0], fractions=(fraction,))


# ----------------------------------------------------------- run length, sustain


def test_fixed_rate_run_lasts_about_the_target_duration():
    assert stats.records_for_duration(73277, 30) == 2198310
    assert stats.records_for_duration(10, 5) == 1000  # the floor keeps a tiny run measurable


def test_fixed_rate_run_length_refuses_non_positive_inputs():
    with pytest.raises(ValueError):
        stats.records_for_duration(0, 30)
    with pytest.raises(ValueError):
        stats.records_for_duration(100, 0)
    for seconds in (float("nan"), float("inf")):
        with pytest.raises(ValueError):
            stats.records_for_duration(100, seconds)


def test_sustained_allows_five_percent_and_no_more():
    assert stats.sustained(1000, 950.0)
    assert not stats.sustained(1000, 949.0)
    assert stats.sustained(1000, 1010.0)


def test_sustained_refuses_a_non_positive_offer():
    with pytest.raises(ValueError):
        stats.sustained(0, 10.0)


def test_load_fractions_are_the_issue_s():
    assert stats.LOAD_FRACTIONS == (25, 50, 75)
