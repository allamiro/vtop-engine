"""Several producers and the multi-partition smoke gate (#596), without Docker."""
import pytest

from kafka_compare import cellshape, perftool


def result(records=1000, rps=100.0, p99=5.0, avg=2.0):
    return perftool.ProducerResult(records, rps, rps / 1000, avg, 9.0, 1.0, 3.0, p99, 8.0, True)


def test_four_producers_split_the_rate_and_report_worst_of_four_percentiles():
    combined = cellshape.combine([result(rps=100, p99=5), result(rps=110, p99=12), result(rps=90, p99=7),
                                  result(rps=100, p99=6)])
    assert combined.lat_basis == "worst-of-4"
    assert combined.result.records == 4000
    assert combined.result.records_per_sec == pytest.approx(400)
    assert combined.result.lat_p99_ms == 12
    assert combined.result.lat_avg_ms == pytest.approx(2.0)


def test_one_producer_is_its_own_row():
    one = result()
    assert cellshape.combine([one]) == (one, "single")


def test_the_script_runs_every_producer_and_its_output_splits_back():
    script = cellshape.multi_producer_script([["tool", "--a", "it's"], ["tool", "--b"]])
    assert script.count(") &") == 2 and "wait" in script
    assert "'it'\\''s'" in script
    output = "=== kc-producer 0 exit 0 ===\nfirst\n=== kc-producer 1 exit 1 ===\nsecond\n"
    assert cellshape.split_outputs(output, 2) == [(0, "\nfirst\n"), (1, "\nsecond\n")]
    with pytest.raises(perftool.PerfParseError, match="expected 3"):
        cellshape.split_outputs(output, 3)


def reads_for(produced, partitions, t=1000):
    out = []
    for i, (key, value) in enumerate(produced):
        out.append(cellshape.SmokeRead(i % partitions, t + i, key, value))
    return out


@pytest.fixture
def produced():
    return cellshape.smoke_records(60, 100)


def test_a_clean_six_partition_read_back_passes(produced):
    cellshape.check_smoke(produced, reads_for(produced, 6), 6, (1000, 2000))


def test_smoke_detects_a_lost_a_duplicated_and_a_misordered_record_per_partition(produced):
    reads = reads_for(produced, 6)
    with pytest.raises(RuntimeError, match="1 of 60 records were not read back"):
        cellshape.check_smoke(produced, reads[:-1], 6, (1000, 2000))
    with pytest.raises(RuntimeError, match="read more than once"):
        cellshape.check_smoke(produced, reads + [reads[3]], 6, (1000, 2000))
    swapped = list(reads)
    swapped[0], swapped[6] = swapped[6], swapped[0]  # both partition 0
    with pytest.raises(RuntimeError, match="partition 0 returned its records out of produced order"):
        cellshape.check_smoke(produced, swapped, 6, (1000, 2000))


def test_smoke_refuses_a_rewritten_create_time(produced):
    reads = reads_for(produced, 6)
    reads[5] = reads[5]._replace(create_time_ms=5000)
    with pytest.raises(RuntimeError, match="CreateTime outside the produce window"):
        cellshape.check_smoke(produced, reads, 6, (1000, 2000))


def test_smoke_requires_every_partition_to_be_written(produced):
    with pytest.raises(RuntimeError, match="partition 5 holds none"):
        cellshape.check_smoke(produced, reads_for(produced, 5), 6, (1000, 2000))


def test_smoke_refuses_altered_bytes(produced):
    reads = reads_for(produced, 1)
    reads[2] = reads[2]._replace(value="x" * 100)
    with pytest.raises(RuntimeError, match="differs"):
        cellshape.check_smoke(produced, reads, 1, (1000, 2000))


def test_the_consumer_lines_parse_with_key_and_create_time():
    reads = cellshape.parse_smoke_read(3, "CreateTime:1700000000123\tk00000001\tsmoke-x\n")
    assert reads == [cellshape.SmokeRead(3, 1700000000123, "k00000001", "smoke-x")]
    with pytest.raises(RuntimeError, match="unexpected line"):
        cellshape.parse_smoke_read(0, "NoTimestamp\tk\tv\n")


def test_smoke_values_are_exactly_the_record_size():
    assert {len(v) for _, v in cellshape.smoke_records(20, 1024)} == {1024}
    assert len({k for k, _ in cellshape.smoke_records(20, 10)}) == 20
