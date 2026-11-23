"""Metric 3 (end-to-end latency) and metric 4 (CPU and bytes written) of the Kafka comparison (#596, #600):
the histogram the consumer prints, the rules it is judged by, and the cgroup v2 counters read around a window.

Pure parsers and arithmetic, tested against fixture text; no Docker."""
import pytest

from kafka_compare import e2e, resources

# ------------------------------------------------------------------------------------------ e2e histogram


def test_buckets_are_1_ms_wide_with_an_overflow_and_a_negative_bucket():
    assert e2e.histogram([0, 0, 5, 59_999, 60_000, 3_600_000, -3]) == {-1: 1, 0: 2, 5: 1, 59_999: 1, 60_000: 2}


def test_the_consumers_lines_parse_and_bad_ones_are_refused():
    assert e2e.parse_output("consumed 9\n0 5\n12 3\n60000 1\n") == (9, {0: 5, 12: 3, 60_000: 1})
    for bad in ("0 5\n", "consumed 5\n0 5\n0 6\n", "consumed 1\n60001 1\n", "consumed 1\nx 1\n",
                "consumed 1\n1 2 3\n", "consumed 1\n-2 1\n", "consumed x\n", "consumed 3\n1 2\n"):
        with pytest.raises(e2e.E2eRefused):
            e2e.parse_output(bad)


def test_the_counts_must_sum_to_what_the_consumer_says_it_consumed():
    with pytest.raises(e2e.E2eRefused, match="sum to 2"):
        e2e.parse_output("consumed 3\n1 2\n")


def test_percentiles_are_the_buckets_lower_edge_by_integer_rank():
    hist = {1: 500, 2: 400, 10: 99, 100: 1}
    assert [e2e.percentile_ms(hist, num, den) for num, den in e2e.PERCENTILES.values()] == [1.0, 10.0, 10.0, 10.0]
    assert e2e.percentile_ms(hist, 1, 1) == 100.0
    assert e2e.percentile_ms({7: 1}, 1, 2) == 7.0
    with pytest.raises(e2e.E2eRefused, match="empty"):
        e2e.percentile_ms({}, 1, 2)


def test_a_negative_bucket_is_refused_whatever_else_is_in_the_run():
    with pytest.raises(e2e.E2eRefused, match="before their CreateTime"):
        e2e.judge({-1: 1, 3: 999}, produced=1000, offered_rate=100, achieved_rate=100)


def test_produced_not_equal_consumed_is_a_failed_row_that_says_how_many_are_missing():
    ok = e2e.judge({1: 600, 2: 400}, produced=1000, offered_rate=100, achieved_rate=100)
    assert ok == e2e.Outcome("ok", 1000, 1000, True, "")
    short = e2e.judge({1: 990}, produced=1000, offered_rate=100, achieved_rate=100)
    assert (short.status, short.note) == ("failed", "missing 10 records")
    over = e2e.judge({1: 1001}, produced=1000, offered_rate=100, achieved_rate=100)
    assert over.status == "failed" and "1 more record" in over.note


def test_an_unsustained_e2e_run_is_marked_so_and_keeps_its_latencies():
    outcome = e2e.judge({1: 1000}, produced=1000, offered_rate=1000, achieved_rate=500)
    assert outcome.status == "ok" and outcome.sustained is False and "not sustained" in outcome.note


def test_the_common_rate_is_a_share_of_the_slower_systems_median_saturation():
    assert e2e.common_rate([24_000, 25_000, 24_500], [290_000, 300_000, 310_000], 50) == 12_250
    assert e2e.common_rate([400, 500, 600], [100, 100, 100], 50) == 50
    with pytest.raises(e2e.E2eRefused, match="no Kafka saturation"):
        e2e.common_rate([], [1], 50)
    with pytest.raises(e2e.E2eRefused, match="below 1 record"):
        e2e.common_rate([1], [1], 50)


# ---------------------------------------------------------------------------------- cgroup v2 counters

CPU_STAT = "usage_usec 1500000\nuser_usec 1000000\nsystem_usec 500000\nnr_periods 0\n"
IO_STAT = ("259:0 rbytes=4096 wbytes=1048576 rios=1 wios=256 dbytes=0 dios=0\n"
           "8:16 rbytes=0 wbytes=2097152 rios=0 wios=512 dbytes=0 dios=0\n")


def test_cgroup_parsers_read_cpu_stat_and_a_multi_device_io_stat():
    assert resources.cpu_usage_usec(CPU_STAT) == 1_500_000
    assert resources.io_bytes(IO_STAT) == resources.Io(4096, 3 * 1048576, ("259:0", "8:16"))


def test_cgroup_v1_and_missing_controllers_are_refused_not_approximated():
    with pytest.raises(resources.CgroupRefused, match="cgroup v1 hosts are refused"):
        resources.check_v2("")
    with pytest.raises(resources.CgroupRefused, match=r"missing \['io'\]"):
        resources.check_v2("cpuset cpu memory pids")
    resources.check_v2("cpuset cpu io memory hugetlb pids rdma misc")


def test_a_malformed_or_missing_counter_is_refused_by_name():
    with pytest.raises(resources.CgroupRefused, match="usage_usec"):
        resources.cpu_usage_usec("user_usec 1\n")
    with pytest.raises(resources.CgroupRefused, match="not a whole number"):
        resources.cpu_usage_usec("usage_usec lots\n")
    with pytest.raises(resources.CgroupRefused, match="no device"):
        resources.io_bytes("")


def test_a_window_is_the_delta_of_two_snapshots_summed_over_the_nodes():
    one = resources.window(resources.Snapshot(1_000_000, resources.Io(0, 1000, ("259:0",))),
                           resources.Snapshot(3_500_000, resources.Io(10, 5000, ("259:0",))))
    two = resources.window(resources.Snapshot(0, resources.Io(0, 0, ("259:0",))),
                           resources.Snapshot(500_000, resources.Io(0, 2000, ("259:0",))))
    assert one == resources.Window(2.5, 4000, 10, ("259:0",))
    assert resources.total({"n1": one, "n2": two}) == resources.Window(3.0, 6000, 10, ("259:0",))


def test_a_counter_going_backwards_means_a_restart_and_is_refused():
    start = resources.Snapshot(5, resources.Io(0, 10, ("259:0",)))
    with pytest.raises(resources.CgroupRefused, match="restarted"):
        resources.window(start, resources.Snapshot(4, resources.Io(0, 10, ("259:0",))))


def recording_closer(calls):
    return resources.Closer(sync=lambda n: calls.append(("sync", n)), sleep=lambda s: calls.append(("sleep", s)),
                            snap=lambda n: calls.append(("snap", n)) or resources.Snapshot(0, resources.Io(0, 0, ())))


@pytest.mark.parametrize("nodes", [["kafka-1", "kafka-2", "kafka-3"], ["vtop-1", "vtop-2", "vtop-3"]])
def test_both_systems_are_synced_before_each_read_and_settled_before_the_last(nodes):
    calls = []
    io = recording_closer(calls)
    resources.open_window(nodes, io)
    assert calls == [("sync", n) for n in nodes] + [("snap", n) for n in nodes]
    calls.clear()
    resources.close_window(nodes, io)
    assert calls == ([("sync", n) for n in nodes] + [("sleep", resources.SETTLE_SECONDS)]
                     + [("snap", n) for n in nodes])
