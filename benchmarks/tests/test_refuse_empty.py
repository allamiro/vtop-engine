"""A benchmark whose engine never produced a batch must refuse loudly (#499).

Scenario 07 failed config validation on every cycle, yet the runner buried the
nonzero exits as `errors` and returned 0 with an all-zeros summary — a non-run
filed as a fast success. The runner must instead exit nonzero and leave the
engine's own stderr on disk, so the failure is neither invisible nor mute.
"""
import glob
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import run_benchmark  # noqa: E402
from run_benchmark import measured_nothing  # noqa: E402

# --------------------------------------------------------------------------
# The pure decision
# --------------------------------------------------------------------------


def test_no_batches_and_an_error_measured_nothing():
    assert measured_nothing(success=0, failed=0, errors=1) is True


def test_a_committed_batch_is_a_real_measurement():
    assert measured_nothing(success=3, failed=0, errors=1) is False


def test_a_verify_failure_is_a_real_measurement():
    # mock_fail and the fault scenarios MEASURE failure; that is a result, not
    # an empty run.
    assert measured_nothing(success=0, failed=2, errors=0) is False


def test_a_clean_empty_input_is_not_a_refusal():
    # No errors AND no input seeded — a genuinely empty run — is a legitimate
    # zero, not a buried failure, and must still pass.
    assert measured_nothing(success=0, failed=0, errors=0, files_seeded=0) is False


def test_seeded_input_that_produced_no_batch_measured_nothing():
    # Every seeded file failing the adapter read leaves the engine exiting 0
    # with no outcomes (#499): errors stays 0, but input WAS seeded and nothing
    # came out, so the run measured nothing and must be refused.
    assert measured_nothing(success=0, failed=0, errors=0, files_seeded=5) is True


def test_seeded_input_that_committed_is_a_real_measurement():
    assert measured_nothing(success=2, failed=0, errors=0, files_seeded=5) is False


# --------------------------------------------------------------------------
# End to end: the runner refuses and surfaces the stderr
# --------------------------------------------------------------------------


def _write_scenario(tmp_path):
    path = tmp_path / "empty.yaml"
    path.write_text(
        "name: refuse-empty\n"
        "backend: s3_native\n"
        "endpoint_url: http://localhost:9000\n"
        "volume: 4\n"
        "duration_seconds: 0\n"
        "sys_sample_interval: 0.1\n",
        encoding="utf-8",
    )
    return str(path)


def test_a_run_that_measures_nothing_exits_nonzero_and_writes_the_stderr(
        tmp_path, monkeypatch):
    results = tmp_path / "results"

    # The engine fails config validation on every cycle, exactly as scenario 07
    # did: nonzero exit, no outcomes, a telling stderr.
    boom = "error: configuration error: upload.command_binary must be absolute"
    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path",
                        lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config",
                        lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint",
                        lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.engine, "process_once",
                        lambda *a, **k: (1, [], boom))
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 4, "bytes": 4096})

    monkeypatch.setattr(sys, "argv",
                        ["run_benchmark.py", _write_scenario(tmp_path),
                         "--results-dir", str(results)])
    rc = run_benchmark.main()

    assert rc == 4, "a run that measured nothing must exit nonzero, not 0"
    run_dirs = glob.glob(str(results / "*"))
    assert len(run_dirs) == 1, run_dirs
    log = os.path.join(run_dirs[0], "engine-stderr.log")
    assert os.path.exists(log), "the engine's stderr must be surfaced on disk"
    with open(log, encoding="utf-8") as fh:
        assert "command_binary" in fh.read(), "the surfaced stderr must be the real one"


def test_an_empty_stderr_failure_still_refuses_and_writes_the_log(
        tmp_path, monkeypatch):
    # A cycle can exit nonzero having written nothing to stderr. The refusal
    # must still fire, and the log the message names must still exist (empty),
    # so the message never points at a file that is not there.
    results = tmp_path / "results"
    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path",
                        lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config",
                        lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint",
                        lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.engine, "process_once",
                        lambda *a, **k: (1, [], ""))
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 4, "bytes": 4096})

    monkeypatch.setattr(sys, "argv",
                        ["run_benchmark.py", _write_scenario(tmp_path),
                         "--results-dir", str(results)])
    assert run_benchmark.main() == 4
    run_dirs = glob.glob(str(results / "*"))
    log = os.path.join(run_dirs[0], "engine-stderr.log")
    assert os.path.exists(log), (
        "the refusal names engine-stderr.log, so it must exist even when the "
        "engine wrote nothing"
    )


def test_seeded_reads_that_all_fail_at_exit_zero_still_refuse(tmp_path, monkeypatch):
    # The engine exits 0 with no outcomes when every seeded file fails the
    # adapter read (logged, skipped). errors stays 0, but input was seeded, so
    # the run measured nothing and must still refuse — and the read-error stderr
    # must be surfaced even though the subprocess exit was zero (#499).
    results = tmp_path / "results"
    reads = "error: adapter: failed to read seeded file: permission denied"
    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path",
                        lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config",
                        lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint",
                        lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.engine, "process_once",
                        lambda *a, **k: (0, [], reads))
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 4, "bytes": 4096})

    monkeypatch.setattr(sys, "argv",
                        ["run_benchmark.py", _write_scenario(tmp_path),
                         "--results-dir", str(results)])
    assert run_benchmark.main() == 4, (
        "a run whose seeded input produced no batch must refuse even at exit 0"
    )
    run_dirs = glob.glob(str(results / "*"))
    log = os.path.join(run_dirs[0], "engine-stderr.log")
    assert os.path.exists(log)
    with open(log, encoding="utf-8") as fh:
        assert "failed to read" in fh.read(), (
            "the read-error diagnostic must be surfaced even at exit 0"
        )


def test_an_idle_cycle_does_not_lock_out_a_later_diagnostic(tmp_path, monkeypatch):
    # In a sustained soak an idle poll returns no outcomes with empty stderr
    # before a later cycle fails with a real message. The first NON-EMPTY
    # diagnostic must be the one surfaced, not the empty idle poll (#499).
    results = tmp_path / "results"
    calls = {"n": 0}
    boom = "error: adapter read failed on cycle two"

    def _once(*a, **k):
        calls["n"] += 1
        # First poll idle (empty), every later poll fails with a real stderr.
        return (0, [], "") if calls["n"] == 1 else (0, [], boom)

    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path",
                        lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config",
                        lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint",
                        lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.engine, "process_once", _once)
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 2, "bytes": 2048})

    scenario = tmp_path / "soak.yaml"
    scenario.write_text(
        "name: idle-then-fail\n"
        "backend: s3_native\n"
        "endpoint_url: http://localhost:9000\n"
        "volume: 2\n"
        "duration_seconds: 1\n"
        "seed_concurrently: true\n"
        "seed_interval_seconds: 0.05\n"
        "sys_sample_interval: 0.05\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "argv",
                        ["run_benchmark.py", str(scenario),
                         "--results-dir", str(results)])
    assert run_benchmark.main() == 4
    run_dirs = glob.glob(str(results / "*"))
    with open(os.path.join(run_dirs[0], "engine-stderr.log"), encoding="utf-8") as fh:
        assert "cycle two" in fh.read(), (
            "the first non-empty diagnostic must win over an earlier idle poll"
        )


def test_a_genuine_failure_wins_over_a_benign_idle_warning(tmp_path, monkeypatch):
    # An idle poll can emit a benign NON-EMPTY warning at rc == 0 before a later
    # cycle genuinely fails (rc != 0). The genuine failure's diagnostic must be
    # the one surfaced, not the benign warning that happened to come first (#499
    # follow-up): a real failure is preferred over any provisional message.
    results = tmp_path / "results"
    calls = {"n": 0}

    def _once(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            return (0, [], "warning: slow disk, retrying")   # benign, rc 0
        return (1, [], "error: the real failure on a later cycle")  # rc != 0

    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path",
                        lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config",
                        lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint",
                        lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.engine, "process_once", _once)
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 2, "bytes": 2048})

    scenario = tmp_path / "soak.yaml"
    scenario.write_text(
        "name: warn-then-fail\n"
        "backend: s3_native\n"
        "endpoint_url: http://localhost:9000\n"
        "volume: 2\n"
        "duration_seconds: 1\n"
        "seed_concurrently: true\n"
        "seed_interval_seconds: 0.05\n"
        "sys_sample_interval: 0.05\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "argv",
                        ["run_benchmark.py", str(scenario),
                         "--results-dir", str(results)])
    assert run_benchmark.main() == 4
    run_dirs = glob.glob(str(results / "*"))
    with open(os.path.join(run_dirs[0], "engine-stderr.log"), encoding="utf-8") as fh:
        body = fh.read()
    assert "the real failure" in body, "the genuine failure diagnostic must win"
    assert "slow disk" not in body, "a benign idle warning must not bury the failure"


def test_an_empty_failure_does_not_lock_out_a_later_failure(tmp_path, monkeypatch):
    # The first nonzero-exit cycle can carry an EMPTY stderr (a crash with no
    # message) before a later nonzero-exit cycle emits the real reason. An empty
    # failure carries nothing, so it must not latch and lock out the later
    # diagnostic — otherwise the refusal writes an empty log despite a real
    # message having been printed (#499 follow-up).
    results = tmp_path / "results"
    calls = {"n": 0}
    boom = "error: the real failure with a message on a later cycle"

    def _once(*a, **k):
        calls["n"] += 1
        # First failure has no message; every later failure does.
        return (1, [], "") if calls["n"] == 1 else (1, [], boom)

    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path",
                        lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config",
                        lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint",
                        lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.engine, "process_once", _once)
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 2, "bytes": 2048})

    scenario = tmp_path / "soak.yaml"
    scenario.write_text(
        "name: empty-fail-then-fail\n"
        "backend: s3_native\n"
        "endpoint_url: http://localhost:9000\n"
        "volume: 2\n"
        "duration_seconds: 1\n"
        "seed_concurrently: true\n"
        "seed_interval_seconds: 0.05\n"
        "sys_sample_interval: 0.05\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "argv",
                        ["run_benchmark.py", str(scenario),
                         "--results-dir", str(results)])
    assert run_benchmark.main() == 4
    run_dirs = glob.glob(str(results / "*"))
    with open(os.path.join(run_dirs[0], "engine-stderr.log"), encoding="utf-8") as fh:
        assert "the real failure" in fh.read(), (
            "an empty first failure must not lock out a later failure's message"
        )


def test_an_empty_failure_does_not_erase_a_provisional_message(tmp_path, monkeypatch):
    # A benign rc-0 read-error message is held provisionally, then a nonzero-exit
    # cycle fails with EMPTY stderr. The empty failure must not overwrite the
    # provisional message with nothing: the read error is still the only reason
    # we have for the run measuring nothing (#499 follow-up).
    results = tmp_path / "results"
    calls = {"n": 0}
    provisional = "warning: failed to read every seeded object"

    def _once(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            return (0, [], provisional)   # benign read error, rc 0
        return (1, [], "")                # genuine failure, but no message

    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path",
                        lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config",
                        lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint",
                        lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.engine, "process_once", _once)
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 2, "bytes": 2048})

    scenario = tmp_path / "soak.yaml"
    scenario.write_text(
        "name: warn-then-empty-fail\n"
        "backend: s3_native\n"
        "endpoint_url: http://localhost:9000\n"
        "volume: 2\n"
        "duration_seconds: 1\n"
        "seed_concurrently: true\n"
        "seed_interval_seconds: 0.05\n"
        "sys_sample_interval: 0.05\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "argv",
                        ["run_benchmark.py", str(scenario),
                         "--results-dir", str(results)])
    assert run_benchmark.main() == 4
    run_dirs = glob.glob(str(results / "*"))
    with open(os.path.join(run_dirs[0], "engine-stderr.log"), encoding="utf-8") as fh:
        assert "failed to read" in fh.read(), (
            "an empty failure must not erase a held provisional diagnostic"
        )


def test_a_run_that_committed_a_batch_still_succeeds(tmp_path, monkeypatch):
    # The guard must not fire on a healthy run: one committed batch, exit 0.
    results = tmp_path / "results"
    outcome = [{
        "batch_id": "b1", "committed": True, "final_state": "source_committed",
        "object_uri": "s3://telemetry-jsonl/b1",
        "metrics": {"compressed_bytes": 10, "uncompressed_bytes": 20,
                    "total_ms": 5, "object_upload_ms": 2},
    }]
    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path",
                        lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config",
                        lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint",
                        lambda *a, **k: "")
    calls = {"n": 0}

    def _once(*a, **k):
        # First cycle commits a batch; the drain-once loop then sees an empty
        # second cycle and stops.
        calls["n"] += 1
        return (0, outcome, "") if calls["n"] == 1 else (0, [], "")

    monkeypatch.setattr(run_benchmark.engine, "process_once", _once)
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 4, "bytes": 4096})

    monkeypatch.setattr(sys, "argv",
                        ["run_benchmark.py", _write_scenario(tmp_path),
                         "--results-dir", str(results)])
    assert run_benchmark.main() == 0
