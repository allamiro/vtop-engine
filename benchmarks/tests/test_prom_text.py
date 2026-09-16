"""The stdlib Prometheus text parser the long-lived engine's scraper reads with (#510).

The parser is strict because a failed read inside a measured window refuses the
run: a line it skipped instead would be a counter that silently stopped moving,
which reads exactly like an engine that stopped uploading.
"""
import math
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib import prom_text  # noqa: E402

# Shaped like what vtop_core::telemetry renders through the prometheus crate.
ENGINE_BODY = """\
# HELP vtop_bytes_out_total Compressed bytes uploaded to object storage
# TYPE vtop_bytes_out_total counter
vtop_bytes_out_total{format="jsonl",source_type="file",tenant="default"} 4096
vtop_bytes_out_total{format="cef",source_type="file",tenant="default"} 1024
# HELP vtop_upload_width Uploads kept in flight this cycle
# TYPE vtop_upload_width gauge
vtop_upload_width 8
# HELP vtop_stage_duration_seconds Wall-clock duration of each pipeline stage
# TYPE vtop_stage_duration_seconds histogram
vtop_stage_duration_seconds_bucket{stage="compress",le="0.001"} 3
vtop_stage_duration_seconds_bucket{stage="compress",le="+Inf"} 5
vtop_stage_duration_seconds_sum{stage="compress"} 0.0042
vtop_stage_duration_seconds_count{stage="compress"} 5
"""


def test_the_engines_own_exposition_parses_into_series_and_types():
    expo = prom_text.parse(ENGINE_BODY)
    assert expo.types["vtop_bytes_out_total"] == "counter"
    assert expo.types["vtop_upload_width"] == "gauge"
    assert len(expo.series) == 7, "every sample line is one series, labelled lines included"
    assert expo.total("vtop_bytes_out_total") == 5120, (
        "a summary column sums a metric across its label sets")


def test_histogram_components_are_counters_so_they_can_be_bucketed_per_second():
    expo = prom_text.parse(ENGINE_BODY)
    assert expo.kind("vtop_stage_duration_seconds_bucket") == "counter"
    assert expo.kind("vtop_stage_duration_seconds_count") == "counter"
    assert expo.kind("vtop_upload_width") == "gauge", (
        "a gauge differenced as a counter is a rate of nothing")


def test_a_family_the_endpoint_never_exposed_is_unknown_not_zero():
    expo = prom_text.parse(ENGINE_BODY)
    assert expo.total("vtop_upload_throttled_total") is None, (
        "absent means never incremented OR not in this binary; zero would claim to know which")


def test_label_order_does_not_change_a_series_identity():
    a = prom_text.parse('# TYPE m counter\nm{b="2",a="1"} 1\n')
    b = prom_text.parse('# TYPE m counter\nm{a="1",b="2"} 1\n')
    assert list(a.series) == list(b.series), (
        "the same series scraped twice must key the same, or every delta becomes a new series")


def test_escaped_label_values_and_special_values_and_timestamps_are_read():
    expo = prom_text.parse(
        '# TYPE m gauge\n'
        'm{path="C:\\\\logs\\\\a \\"b\\"\\nc"} +Inf 1700000000000\n'
        'n NaN\n'
        'o -Inf\n')
    (series,) = [s for s in expo.series.values() if s.name == "m"]
    assert dict(series.labels)["path"] == 'C:\\logs\\a "b"\nc'
    assert math.isinf(series.value)
    assert math.isnan(expo.series["n"].value)


@pytest.mark.parametrize("body", [
    "vtop_x_total{tenant=default} 1\n",           # unquoted label value
    "vtop_x_total{tenant=\"a\" 1\n",               # unterminated label set
    "vtop_x_total 12abc\n",                        # not a number
    "vtop_x_total 1 2 3\n",                        # too many fields
    "# TYPE vtop_x_total countr\n",                # unknown type
    "vtop_x_total 1\nvtop_x_total 2\n",            # one series twice
    "{tenant=\"a\"} 1\n",                          # no metric name
])
def test_a_body_this_parser_cannot_read_raises_rather_than_skipping_the_line(body):
    with pytest.raises(prom_text.PromParseError):
        prom_text.parse(body)
