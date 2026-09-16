"""The benchmark must never delete a seed directory it did not create.

`--seed-dir` is caller-supplied and can point at real data; recursively removing
it would be silent data loss. Only a directory the benchmark generated itself is
scratch that may be cleaned up.
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from run_benchmark import should_remove_seed_dir  # noqa: E402


def test_generated_seed_dir_is_removed_by_default():
    assert should_remove_seed_dir(seed_dir_is_ours=True, keep_seed=False) is True


def test_generated_seed_dir_is_kept_with_keep_seed():
    assert should_remove_seed_dir(seed_dir_is_ours=True, keep_seed=True) is False


def test_caller_supplied_seed_dir_is_never_removed():
    # The regression this file exists for: a --seed-dir the caller passed must
    # survive regardless of --keep-seed.
    assert should_remove_seed_dir(seed_dir_is_ours=False, keep_seed=False) is False
    assert should_remove_seed_dir(seed_dir_is_ours=False, keep_seed=True) is False


def test_caller_supplied_directory_still_exists_after_the_decision():
    """End-to-end intent: honour the decision and the user's files survive."""
    import shutil

    with tempfile.TemporaryDirectory() as parent:
        user_dir = os.path.join(parent, "important-data")
        os.makedirs(user_dir)
        precious = os.path.join(user_dir, "keep-me.txt")
        with open(precious, "w") as fh:
            fh.write("real data")

        # Simulate the cleanup branch for a caller-supplied directory.
        if should_remove_seed_dir(seed_dir_is_ours=False, keep_seed=False):
            shutil.rmtree(user_dir, ignore_errors=True)

        assert os.path.exists(precious), "caller-supplied seed data was deleted"


def test_empty_seed_dir_is_treated_as_generated_scratch():
    """`--seed-dir ""` falls through to mkdtemp, so we own it and must clean it.

    Allocation uses truthiness (`args.seed_dir or mkdtemp()`), so ownership must
    use the same test. Deciding ownership with `is None` would mark a directory
    we created as caller-owned and leak it on every run.
    """
    for supplied in ("", None):
        seed_dir_is_ours = not supplied
        assert seed_dir_is_ours is True
        assert should_remove_seed_dir(seed_dir_is_ours, keep_seed=False) is True

    # A real caller-supplied path is still never removed.
    assert (not "/real/path") is False
    assert should_remove_seed_dir(seed_dir_is_ours=False, keep_seed=False) is False


# --------------------------------------------------------------------------
# a refused long-lived window (#510) cleans up like any other refusal
# --------------------------------------------------------------------------


def _refusing_window_run(tmp_path, monkeypatch, extra_argv=()):
    """Drive run_benchmark.main() to a refused `engine_mode: run` window, with
    every mkdtemp the run makes landing in its own `scratch` directory, and
    return (scratch, run_dir, the ResultsWriter the run used)."""
    import glob

    import pytest

    import run_benchmark
    from lib import engine_run

    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(run_benchmark.tempfile, "tempdir", str(scratch))
    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path", lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config", lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint", lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 4, "bytes": 4096})

    def _no_cycles(*a, **k):
        raise AssertionError("a long-lived run must not fall back to per-cycle process-once")

    monkeypatch.setattr(run_benchmark.engine, "process_once", _no_cycles)

    def _refuse_after_some_samples(binary, config_path, sc, results_dir):
        # What a real refusal leaves behind: the samples it did take and the
        # engine's own output, both the only diagnosis of why it was refused.
        for name in (engine_run.ENGINE_METRICS_PARTIAL_CSV, engine_run.ENGINE_LOG):
            with open(os.path.join(results_dir, name), "w", encoding="utf-8") as fh:
                fh.write("evidence\n")
        raise engine_run.ScrapeStopped("scraping stopped 1.2s into the window")

    monkeypatch.setattr(run_benchmark.engine_run, "measure_window", _refuse_after_some_samples)
    writers = []

    class RecordingWriter(run_benchmark.ResultsWriter):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            writers.append(self)

    monkeypatch.setattr(run_benchmark, "ResultsWriter", RecordingWriter)
    scenario = tmp_path / "refused.yaml"
    scenario.write_text("name: refused-window\nbackend: mock\nvolume: 4\nduration_seconds: 1\n"
                        "engine_mode: run\nseed_concurrently: true\nseed_interval_seconds: 0.05\n"
                        "sys_sample_interval: 0.1\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["run_benchmark.py", str(scenario),
                                      "--results-dir", str(tmp_path / "results"), *extra_argv])
    with pytest.raises(engine_run.EngineWindowError):
        run_benchmark.main()
    (run_dir,) = glob.glob(str(tmp_path / "results" / "*"))
    (writer,) = writers
    return scratch, run_dir, writer


def test_a_refused_engine_window_removes_the_runs_own_scratch_and_keeps_its_evidence(
        tmp_path, monkeypatch):
    from lib import engine_run

    scratch, run_dir, writer = _refusing_window_run(tmp_path, monkeypatch)
    assert sorted(os.listdir(scratch)) == [], (
        "a refused window must remove the seed, work and state directories the run created; "
        "leaking them on every refusal fills the temp disk across a matrix")
    assert all(fh.closed for fh in writer._files.values()), (
        "the results writer must be closed on the refusal path, not left holding every CSV open")
    for name in (engine_run.ENGINE_METRICS_PARTIAL_CSV, engine_run.ENGINE_LOG):
        assert os.path.exists(os.path.join(run_dir, name)), (
            f"{name} is the only record of why the window was refused; cleanup removes scratch, "
            "never the results directory")


def test_a_refused_engine_window_never_removes_a_caller_supplied_seed_dir(tmp_path, monkeypatch):
    user_dir = tmp_path / "important-data"
    user_dir.mkdir()
    precious = user_dir / "keep-me.txt"
    precious.write_text("real data", encoding="utf-8")
    scratch, _run_dir, _writer = _refusing_window_run(
        tmp_path, monkeypatch, extra_argv=("--seed-dir", str(user_dir)))
    assert precious.exists(), "a caller-supplied --seed-dir is never scratch, refusal or not"
    assert sorted(os.listdir(scratch)) == [], (
        "the work and state directories are still the run's own and still go")
