"""Parsing the JVM perf tools' output (#596). The fixtures are the tools' own
lines from real runs against both clusters on the benchmark host."""
import pytest

from kafka_compare import perftool

WITH_WARMUP = """\
Warmup first 200000 records. Steady state results will print after the complete test summary.
In steady state.
180361 records sent, 36072.2 records/sec (3.44 MB/sec), 3704.9 ms avg latency, 4301.0 ms max latency.
910792 records sent, 182085.6 records/sec (17.37 MB/sec), 1908.4 ms avg latency, 2839.0 ms max latency.
2000000 records sent, 153468.385513 records/sec (14.64 MB/sec), 1674.32 ms avg latency, 4301.00 ms max latency, \
1433 ms 50th, 3721 ms 95th, 4176 ms 99th, 4289 ms 99.9th.
1800000 steady state records sent, 148087.206911 records/sec (14.12 MB/sec), 1581.93 ms avg latency, \
4301.00 ms max latency, 1173 ms 50th, 3773 ms 95th, 4189 ms 99th, 4290 ms 99.9th.
"""

WITHOUT_WARMUP = """\
123870 records sent, 24764.1 records/sec (2.36 MB/sec), 3238.9 ms avg latency, 4787.0 ms max latency.
200000 records sent, 24560.972615 records/sec (2.34 MB/sec), 3763.77 ms avg latency, 7542.00 ms max latency, \
3911 ms 50th, 6977 ms 95th, 7415 ms 99th, 7541 ms 99.9th.
"""

# Every record refused (the gateway does not decompress zstd): the tool still
# prints a summary, of nothing.
ALL_REFUSED = """\
org.apache.kafka.common.errors.UnsupportedCompressionTypeException: batch 0: record batch is compressed (codec 4), \
which this gateway does not decompress
0 records sent, 0.000000 records/sec (0.00 MB/sec), NaN ms avg latency, 0.00 ms max latency, 0 ms 50th, \
0 ms 95th, 0 ms 99th, 0 ms 99.9th.
"""

CONSUMER = """\
start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, \
fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
2026-11-09 23:14:25:820, 2026-11-09 23:14:26:927, 14.3477, 12.9609, 150447, 135905.1491, 774, 333, 43.0863, \
451792.7928
"""


def test_the_steady_state_line_is_taken_when_warmup_was_asked_for():
    result = perftool.parse_producer(WITH_WARMUP, expect_steady_state=True)
    assert result.steady_state
    assert result.records == 1800000
    assert result.records_per_sec == pytest.approx(148087.206911)
    assert result.mib_per_sec == pytest.approx(14.12)
    assert (result.lat_p50_ms, result.lat_p95_ms, result.lat_p99_ms, result.lat_p999_ms) == (1173, 3773, 4189, 4290)
    assert result.lat_avg_ms == pytest.approx(1581.93)


def test_progress_lines_are_never_mistaken_for_the_summary():
    # The 5-second progress lines have no percentiles; only summaries parse.
    result = perftool.parse_producer(WITH_WARMUP, expect_steady_state=False)
    assert result.records == 2000000 and not result.steady_state


def test_a_missing_steady_state_line_is_an_error_not_a_fallback():
    with pytest.raises(perftool.PerfParseError, match="steady-state"):
        perftool.parse_producer(WITHOUT_WARMUP, expect_steady_state=True)


def test_a_run_without_warmup_parses_its_one_summary():
    result = perftool.parse_producer(WITHOUT_WARMUP, expect_steady_state=False)
    assert result.records == 200000 and result.lat_p999_ms == 7541


def test_a_run_that_sent_nothing_is_refused():
    with pytest.raises(perftool.PerfParseError, match="0 records"):
        perftool.parse_producer(ALL_REFUSED, expect_steady_state=False)


def test_nan_latency_is_refused():
    text = ALL_REFUSED.replace("0 records sent", "5 records sent")
    with pytest.raises(perftool.PerfParseError, match="NaN"):
        perftool.parse_producer(text, expect_steady_state=False)


def test_empty_output_is_refused():
    with pytest.raises(perftool.PerfParseError):
        perftool.parse_producer("", expect_steady_state=True)


def test_consumer_rates_are_the_fetch_columns_excluding_the_join():
    result = perftool.parse_consumer(CONSUMER, expected_records=150000)
    assert result.records == 150447
    assert result.records_per_sec == pytest.approx(451792.7928)
    assert result.mib_per_sec == pytest.approx(43.0863)
    assert result.rebalance_ms == 774 and result.fetch_ms == 333


def test_a_consumer_that_read_fewer_records_than_asked_is_refused():
    with pytest.raises(perftool.PerfParseError, match="consumed 150447 records, asked for 200000"):
        perftool.parse_consumer(CONSUMER, expected_records=200000)


def test_a_consumer_output_without_its_header_is_refused():
    with pytest.raises(perftool.PerfParseError, match="header"):
        perftool.parse_consumer(CONSUMER.splitlines()[1], expected_records=1)


def test_a_truncated_consumer_line_is_refused():
    with pytest.raises(perftool.PerfParseError, match="fields"):
        perftool.parse_consumer(CONSUMER.rsplit(",", 2)[0], expected_records=1)


def test_the_client_config_dump_is_read_from_its_banner():
    text = (
        "[2026-11-09 23:12:01,000] INFO ProducerConfig values: \n"
        "\tacks = -1\n\tbatch.size = 16384\n\tenable.idempotence = true\n\tlinger.ms = 5\n"
        " (org.apache.kafka.common.config.AbstractConfig)\n"
        "[2026-11-09 23:12:01,100] INFO something else\n\tunrelated = 1\n"
    )
    assert perftool.parse_client_config(text) == {
        "acks": "-1", "batch.size": "16384", "enable.idempotence": "true", "linger.ms": "5",
    }


def test_no_config_banner_is_an_error():
    with pytest.raises(perftool.PerfParseError):
        perftool.parse_client_config("nothing here")


def test_a_config_dump_cut_off_before_its_close_is_refused():
    text = "[2026-11-09 23:12:01,000] INFO ProducerConfig values: \n\tacks = -1\n\tbatch.size = 16384\n"
    with pytest.raises(perftool.PerfParseError, match="truncated"):
        perftool.parse_client_config(text)


def test_a_config_dump_broken_by_another_line_is_refused():
    text = (
        "[2026-11-09 23:12:01,000] INFO ProducerConfig values: \n\tacks = -1\n"
        "[2026-11-09 23:12:01,100] INFO something else\n\tlinger.ms = 5\n"
        " (org.apache.kafka.common.config.AbstractConfig)\n"
    )
    with pytest.raises(perftool.PerfParseError, match="malformed"):
        perftool.parse_client_config(text)


def _consumer_with(fields: dict[int, str]) -> str:
    header, line = CONSUMER.strip().split("\n")
    values = [value.strip() for value in line.split(",")]
    for index, value in fields.items():
        values[index] = value
    return header + "\n" + ", ".join(values) + "\n"


@pytest.mark.parametrize("fields", [
    {7: "NaN"}, {6: "NaN"}, {8: "Infinity"}, {9: "-5.0"}, {2: "-1"}, {9: "0.0"}, {7: "0"},
])
def test_a_consumer_summary_with_a_non_finite_negative_or_zero_rate_is_refused(fields):
    with pytest.raises(perftool.PerfParseError):
        perftool.parse_consumer(_consumer_with(fields), expected_records=1)
