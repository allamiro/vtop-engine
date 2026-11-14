"""Metric 4's cgroup v2 parsers and window, against fixture text (#596)."""
import pytest

from kafka_compare import cgroup

CPU_STAT = "usage_usec 1500000\nuser_usec 1000000\nsystem_usec 500000\nnr_periods 0\n"
MEMORY_STAT = "anon 104857600\nfile 52428800\nkernel 1024\n"
IO_STAT = ("259:0 rbytes=4096 wbytes=1048576 rios=1 wios=256 dbytes=0 dios=0\n"
           "8:16 rbytes=0 wbytes=2097152 rios=0 wios=512 dbytes=0 dios=0\n")


def test_cgroup_parsers_read_cpu_stat_memory_stat_and_multi_device_io_stat():
    assert cgroup.cpu_usage_usec(CPU_STAT) == 1_500_000
    assert cgroup.memory_anon(MEMORY_STAT) == 104_857_600
    assert cgroup.memory_current("209715200\n") == 209_715_200
    assert cgroup.io_bytes(IO_STAT) == cgroup.Io(4096, 3 * 1048576, ("259:0", "8:16"))


def test_cgroup_v1_is_refused_with_a_message():
    with pytest.raises(cgroup.CgroupRefused, match="cgroup v1 hosts are refused"):
        cgroup.check_v2("")
    with pytest.raises(cgroup.CgroupRefused, match="missing \\['io'\\]"):
        cgroup.check_v2("cpuset cpu memory pids")
    cgroup.check_v2("cpuset cpu io memory hugetlb pids rdma misc")


def test_a_malformed_counter_is_refused_by_name():
    with pytest.raises(cgroup.CgroupRefused, match="usage_usec"):
        cgroup.cpu_usage_usec("user_usec 1\n")
    with pytest.raises(cgroup.CgroupRefused, match="not a whole number"):
        cgroup.memory_anon("anon lots\n")


def test_the_window_is_deltas_and_sampled_peaks():
    start = cgroup.Snapshot(1_000_000, cgroup.Io(0, 1000, ("259:0",)))
    end = cgroup.Snapshot(3_500_000, cgroup.Io(10, 5000, ("259:0", "8:16")))
    w = cgroup.window(start, end, [5, 9, 7], [20, 30, 25])
    assert w == cgroup.Window(2.5, 9, 30, 4000, 10, ("259:0", "8:16"))


def test_a_counter_going_backwards_means_a_restart_and_is_refused():
    start = cgroup.Snapshot(5, cgroup.Io(0, 10, ()))
    with pytest.raises(cgroup.CgroupRefused, match="restarted"):
        cgroup.window(start, cgroup.Snapshot(4, cgroup.Io(0, 10, ())), [1], [1])
    with pytest.raises(cgroup.CgroupRefused, match="no memory sample"):
        cgroup.window(start, cgroup.Snapshot(6, cgroup.Io(0, 10, ())), [], [1])


def recording_closer(calls):
    return cgroup.Closer(sync=lambda n: calls.append(("sync", n)), sleep=lambda s: calls.append(("sleep", s)),
                         snap=lambda n: calls.append(("snap", n)) or cgroup.Snapshot(0, cgroup.Io(0, 0, ())))


@pytest.mark.parametrize("nodes", [["kafka-1", "kafka-2", "kafka-3"], ["vtop-1", "vtop-2", "vtop-3"]])
def test_window_close_runs_sync_before_the_final_read_for_both_systems(nodes):
    calls = []
    io = recording_closer(calls)
    cgroup.open_window(nodes, io)
    assert calls == [("sync", n) for n in nodes] + [("snap", n) for n in nodes]
    calls.clear()
    cgroup.close_window(nodes, io)
    assert calls == ([("sync", n) for n in nodes] + [("sleep", cgroup.SETTLE_SECONDS)]
                     + [("snap", n) for n in nodes])
