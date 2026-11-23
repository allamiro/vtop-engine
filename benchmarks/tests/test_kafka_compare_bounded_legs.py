"""A fixed-rate leg has a bounded length (the v0.11.1 AWS round's lesson 1).

A fixed-rate leg used to send `offered rate x --fixed-seconds` records with a wall limit of
`max(--sat-timeout, 6 x records / rate)`. A system that cannot hold the offered rate then runs as long as its
shortfall: VTOP at 100 B held about 6.2k records/s against an offered share of Kafka's ~360k, so each
"30 s" leg ran 7 to 20 minutes. Now the record count is sized from the system's OWN saturation median
(`min(rate, own median) x fixed_seconds`, the producer still throttled at the offered rate), the wall limit is
`3 x fixed_seconds + 60 s`, and a leg that hits it is a failed, unsustained row that carries the last achieved
rate the tool printed.
"""
import csv
import subprocess

import kafka_compare_stub as stub
import pytest

from kafka_compare import perftool, plan, run

RATES = {25: 75_000, 50: 150_000, 75: 225_000}  # shares of a Kafka saturating at 300k records/s


def rows_of(ctx):
    with open(ctx["csv"], newline="") as handle:
        return list(csv.DictReader(handle))


def fixed_calls(calls):
    return [(system, records, rate) for system, records, rate in calls["producer"] if rate > 0]


def test_a_system_below_the_offered_rate_is_sent_its_own_saturation_times_the_target(monkeypatch, tmp_path):
    calls = stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr)
    ctx = stub.ctx_for(run, str(tmp_path / "run"), fixed_seconds=30.0)
    run.run_cell(ctx, "vtop", 100, RATES)
    # VTOP saturates at 6000 records/s: each leg is sized 6000 x 30 s, and still offered Kafka's share.
    assert fixed_calls(calls) == [("vtop", 180_000, 75_000), ("vtop", 180_000, 150_000), ("vtop", 180_000, 225_000)]


def test_a_system_that_holds_the_offered_rate_is_sized_by_that_rate_as_before(monkeypatch, tmp_path):
    calls = stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr)
    ctx = stub.ctx_for(run, str(tmp_path / "run"), fixed_seconds=30.0)
    run.run_cell(ctx, "kafka", 100, None)
    assert fixed_calls(calls) == [("kafka", 2_250_000, 75_000), ("kafka", 4_500_000, 150_000),
                                  ("kafka", 6_750_000, 225_000)]


def test_a_sized_leg_says_so_and_is_not_sustained(monkeypatch, tmp_path):
    stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr)
    ctx = stub.ctx_for(run, str(tmp_path / "run"), fixed_seconds=30.0)
    run.run_cell(ctx, "vtop", 100, RATES)
    fixed = [row for row in rows_of(ctx) if row["mode"] == "fixed_rate"]
    assert {row["status"] for row in fixed} == {"ok"}
    assert {row["sustained"] for row in fixed} == {"false"}
    note = fixed[0]["note"]
    assert "offered 75000 records/s" in note and "achieved 6000" in note
    assert "sized from this system's saturation median (6000 records/s" in note


def test_the_wall_limit_of_a_leg_is_three_targets_and_a_minute(monkeypatch, tmp_path):
    stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr)
    timeouts = []
    real = run.run_producer
    monkeypatch.setattr(run, "run_producer", lambda system, records, size, rate, timeout, info_logging=False:
                        (timeouts.append((rate, timeout)), real(system, records, size, rate, timeout))[1])
    ctx = stub.ctx_for(run, str(tmp_path / "run"), fixed_seconds=30.0, sat_timeout=1800.0)
    run.run_cell(ctx, "vtop", 100, RATES)
    assert [timeout for rate, timeout in timeouts if rate > 0] == [150.0, 150.0, 150.0]
    assert plan.fixed_rate_wall_limit(30.0) == 150.0


# What kafka-producer-perf-test prints every five seconds before its summary (from a real run, as in
# test_kafka_compare_perftool.py), then cut off by the timeout.
PROGRESS = """\
Warmup first 200000 records. Steady state results will print after the complete test summary.
In steady state.
30715 records sent, 6142.0 records/sec (0.59 MB/sec), 4704.9 ms avg latency, 5301.0 ms max latency.
31090 records sent, 6216.8 records/sec (0.59 MB/sec), 4908.4 ms avg latency, 5839.0 ms max latency.
"""


def test_the_last_progress_line_gives_the_achieved_rate():
    assert perftool.last_progress_rate(PROGRESS) == 6216.8
    assert perftool.last_progress_rate("nothing printed\n") is None


def test_a_summary_line_is_not_taken_for_progress():
    summary = ("2000000 records sent, 153468.385513 records/sec (14.64 MB/sec), 1674.32 ms avg latency, "
               "4301.00 ms max latency, 1433 ms 50th, 3721 ms 95th, 4176 ms 99th, 4289 ms 99.9th.\n")
    assert perftool.last_progress_rate(PROGRESS + summary) == 6216.8


def _timing_out_client(output):
    removed = []

    def sh(args, **kwargs):
        if args[:2] == ["docker", "rm"]:
            removed.append(args[-1])
            return subprocess.CompletedProcess(args, 0, "", "")
        # subprocess.run hands back what the child printed before the timeout, as bytes, whatever `text=` says.
        raise subprocess.TimeoutExpired(args, kwargs.get("timeout"), output=output.encode(), stderr=b"")

    return sh, removed


def test_a_leg_that_hits_the_limit_is_failed_with_the_last_rate_and_its_container_removed(monkeypatch):
    sh, removed = _timing_out_client(PROGRESS)
    monkeypatch.setattr(run, "sh", sh)
    result, note, _wall, output = run.run_producer("vtop", 180_000, 100, 75_000, 150.0)
    assert result is None
    assert note.startswith("timed out after 150 s") and "last achieved rate 6216.8 records/s" in note
    assert "6216.8 records/sec" in output
    assert len(removed) == 1 and removed[0].startswith("kc-client-")


def test_a_timed_out_leg_with_no_progress_line_says_so(monkeypatch):
    sh, _ = _timing_out_client("")
    monkeypatch.setattr(run, "sh", sh)
    _result, note, _wall, _output = run.run_producer("vtop", 180_000, 100, 75_000, 150.0)
    assert note.startswith("timed out after 150 s") and "no progress line" in note


def test_a_timed_out_leg_is_a_failed_unsustained_row(monkeypatch, tmp_path):
    stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr)
    real = run.run_producer

    def producer(system, records, size, rate, timeout, info_logging=False):
        if rate > 0:
            return None, "timed out after 150 s; last achieved rate 6216.8 records/s", 150.0, PROGRESS
        return real(system, records, size, rate, timeout)

    monkeypatch.setattr(run, "run_producer", producer)
    ctx = stub.ctx_for(run, str(tmp_path / "run"), fixed_seconds=30.0)
    run.run_cell(ctx, "vtop", 100, RATES)
    fixed = [row for row in rows_of(ctx) if row["mode"] == "fixed_rate"]
    assert [(row["status"], row["sustained"]) for row in fixed] == [("failed", "false")] * 3
    assert "6216.8 records/s" in fixed[0]["note"]


@pytest.mark.parametrize("fixed_seconds, limit", [(30.0, 150.0), (1.0, 63.0), (120.0, 420.0)])
def test_the_bound_is_one_formula(fixed_seconds, limit):
    assert plan.fixed_rate_wall_limit(fixed_seconds) == limit
