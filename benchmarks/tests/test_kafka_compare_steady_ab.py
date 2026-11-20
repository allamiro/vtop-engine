"""The steady-state A/B's per-run numbers (#613 follow-up)."""
import pytest

from kafka_compare import steady_ab


def _ack_log(origin_ms, rate, batch, n, rtt_us, stall_at=None, stall_ms=0.0):
    """n batches acked on schedule (each when its last record is due, plus its round trip),
    one of them held `stall_ms` longer."""
    lines, t_shift = [], 0.0
    for k in range(1, n + 1):
        if stall_at == k:
            t_shift += stall_ms
        due = origin_ms + (k * batch - 1) * 1000.0 / rate
        t_us = int((due + rtt_us / 1000.0 + t_shift) * 1000)
        lines.append(f"{t_us} {k * batch} {rtt_us}")
    return "\n".join(lines) + "\n"


def test_a_run_on_schedule_has_batch_spaced_gaps_and_latency_near_the_round_trip():
    text = _ack_log(1_000_000.0, 5000, 100, 600, 5000)
    m = steady_ab.steady_metrics(text, 1_000_000.0, 5000, warmup_s=1.0)
    assert m["max_gap_ms"] == pytest.approx(20.0, abs=0.01)  # 100 records at 5,000/s
    assert m["rtt_p99_ms"] == 5.0
    # Records wait in the batch until its last one is due: 0-19.8 ms, plus the 5 ms round trip.
    assert 5.0 <= m["lat_p50_ms"] <= 25.0 and m["lat_max_ms"] == pytest.approx(24.8, abs=0.01)


def test_a_stall_shows_as_the_longest_gap_and_a_latency_tail():
    text = _ack_log(1_000_000.0, 5000, 100, 600, 5000, stall_at=300, stall_ms=500.0)
    m = steady_ab.steady_metrics(text, 1_000_000.0, 5000, warmup_s=1.0)
    assert m["max_gap_ms"] == pytest.approx(520.0, abs=0.01)
    assert m["lat_max_ms"] > 500


def test_too_few_acknowledgements_in_the_window_is_an_error():
    with pytest.raises(ValueError, match="fewer than two"):
        steady_ab.steady_metrics(_ack_log(1_000_000.0, 5000, 100, 3, 5000), 1_000_000.0, 5000, warmup_s=60.0)


def test_the_report_cites_a_committed_ab_with_its_numbers(tmp_path):
    import csv
    import json

    from kafka_compare import failover_report

    assert failover_report.steady_ab_note(str(tmp_path)) == ""  # no A/B beside the run
    ab = tmp_path / "steady-ab"
    ab.mkdir()
    (ab / "environment.json").write_text(json.dumps({
        "engines": {"A": {"image": "img:a"}, "B": {"image": "img:b"}}, "client_image": "img:b",
        "rate": 5000, "seconds": 120}))
    with open(ab / "ab.csv", "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=steady_ab.COLUMNS)
        writer.writeheader()
        for run, rnd, engine, gap, load in (("r1-A", 1, "A", 811.7, 19.97), ("r1-B", 1, "B", 264.9, 10.48),
                                            ("r2-A", 2, "A", 41.7, 6.86), ("r2-B", 2, "B", 36.7, 7.12)):
            writer.writerow({"run": run, "round": rnd, "engine": engine, "max_gap_ms": gap, "lat_p99_ms": 28.0,
                             "rtt_p99_ms": 9.0, "load1_max": load})
    note = failover_report.steady_ab_note(str(tmp_path))
    assert "A = `img:a`; B = `img:b`; interleaved over 2 rounds" in note
    assert "longest ack gap A 427 ms (42 ms - 812 ms), B 151 ms (37 ms - 265 ms)" in note
    assert "Each image's longest gap came in its own busiest round (A 812 ms at load 19.97; B 265 ms at load " \
           "10.48)" in note
