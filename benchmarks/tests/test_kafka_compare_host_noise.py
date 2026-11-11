"""The failover run's host-contention record (#597 re-run): sampled, summarised, reported."""
import time

from kafka_compare import failover_report, host_noise


def _row(t, load1, idle, running, bench, rust, lima):
    return {"timestamp_utc": "2026-11-11T00:00:00Z", "t_unix": t, "load1": load1, "load5": 1.0, "load15": 1.0,
            "cpu_idle_pct": idle, "docker_running": running, "docker_benchmark": bench, "docker_rust_builds": rust,
            "lima_running": lima}


def test_the_sampler_writes_a_row_at_once_and_stops(tmp_path):
    probes = iter([_row(1.0, 2.0, 80.0, 12, 3, 1, "vtop-bench-483a"), _row(2.0, 9.0, 40.0, 14, 3, 2, "")])
    path = str(tmp_path / "host_noise.csv")
    sampler = host_noise.Sampler(path, interval=0.05, probe=lambda: next(probes, _row(3.0, 1.0, 90.0, 9, 3, 0, "")))
    sampler.start()
    time.sleep(0.2)
    sampler.stop()
    rows = host_noise.read(path)
    assert len(rows) >= 2 and tuple(rows[0]) == host_noise.COLUMNS
    assert rows[0]["lima_running"] == "vtop-bench-483a"
    time.sleep(0.2)  # several intervals: a sampler still running would add rows
    assert len(host_noise.read(path)) == len(rows)


def test_a_failing_probe_is_recorded_and_sampling_goes_on(tmp_path):
    calls = []

    def probe():
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("docker ps timed out")
        return _row(float(len(calls)), 1.0, 90.0, 9, 0, 0, "")

    path = str(tmp_path / "host_noise.csv")
    sampler = host_noise.Sampler(path, interval=0.05, probe=probe)
    sampler.start()
    time.sleep(0.3)
    sampler.stop()
    assert len(sampler.errors) == 1 and "RuntimeError: docker ps timed out" in sampler.errors[0]
    assert len(host_noise.read(path)) >= 3  # samples after the failure still landed


def test_samples_keep_the_interval_whatever_the_probe_costs(tmp_path):
    stamps = []

    def slow_probe():
        stamps.append(time.monotonic())
        time.sleep(0.06)  # 60% of the interval
        return _row(1.0, 1.0, 90.0, 9, 0, 0, "")

    sampler = host_noise.Sampler(str(tmp_path / "h.csv"), interval=0.1, probe=slow_probe)
    sampler.start()
    time.sleep(1.05)
    sampler.stop()
    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    # Waiting a full interval AFTER each probe would space them about 0.16 s apart.
    assert len(gaps) >= 7 and sum(gaps) / len(gaps) < 0.13


def test_the_summary_keeps_the_worst_of_the_run_and_every_vm_seen():
    rows = [{k: str(v) for k, v in _row(1.0, 2.0, 80.0, 12, 3, 1, "vtop-bench-483a").items()},
            {k: str(v) for k, v in _row(2.0, 9.0, 40.0, 14, 3, 2, "").items()},
            {k: str(v) for k, v in _row(3.0, 4.0, "", 9, 0, 0, "vm-b;vtop-bench-483a").items()}]
    rows[2]["cpu_idle_pct"] = ""
    summary = host_noise.summarize(rows)
    assert summary == {"samples": 3, "load1_mean": 5.0, "load1_max": 9.0, "cpu_idle_pct_mean": 60.0,
                       "cpu_idle_pct_min": 40.0, "docker_running_max": 14, "docker_other_max": 11,
                       "docker_rust_builds_max": 2, "lima_running_seen": ["vm-b", "vtop-bench-483a"]}
    assert [r["t_unix"] for r in host_noise.window(rows, 1.5, 3.0)] == ["2.0", "3.0"]
    assert host_noise.summarize([]) == {"samples": 0}


def test_the_report_states_the_contention_and_the_busiest_repetitions():
    env = {"comparable": {"host_logical_cpus": "18"}, "host_noise": {
        "file": "host_noise.csv", "interval_s": 30,
        "run": {"samples": 10, "load1_mean": 5.0, "load1_max": 21.0, "cpu_idle_pct_mean": 60.0,
                "cpu_idle_pct_min": 20.0, "docker_running_max": 14, "docker_other_max": 11,
                "docker_rust_builds_max": 2, "lima_running_seen": ["vtop-bench-483a"]},
        "per_repetition": {"vtop-kill9_leader-1": {"samples": 5, "load1_max": 21.0},
                           "kafka-kill9_leader-1": {"samples": 5, "load1_max": 4.0}}}}
    text = failover_report.host_noise_paragraph(env)
    assert "21.0 at the highest, on 18 logical CPUs" in text
    assert "`vtop-bench-483a`" in text and "vtop-kill9_leader-1 (21.0)" in text
    assert failover_report.host_noise_paragraph({"comparable": {}}) == ""
