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
# End to end: every way out of a run removes the scratch it owns (review)
# --------------------------------------------------------------------------

import contextlib  # noqa: E402
import json  # noqa: E402
import time  # noqa: E402

import pytest  # noqa: E402

import run_benchmark  # noqa: E402
from lib.shaping import ShapingError  # noqa: E402

SCRATCH_PREFIXES = ("vtop-seed-", "vtop-work-", "vtop-state-", "vtop-recovery-empty-")


class RefusingContention:
    """A contended run whose bracket refuses as it closes, the way two solo
    windows that disagree do."""

    def start_with_vtop(self):
        pass

    def note_progress(self, _started_at, _ended_at, _uncounted_batches=0):
        pass

    def close(self):
        raise ShapingError("the two solo competitor windows disagree by 63.4%")


def _refuse_at_close(monkeypatch):
    monkeypatch.setattr(run_benchmark.competitor.CompetitorSpec, "from_scenario",
                        classmethod(lambda cls, sc, shape: object()))

    @contextlib.contextmanager
    def contended(_spec, vtop_bytes):
        yield RefusingContention()

    monkeypatch.setattr(run_benchmark.competitor, "contended", contended)
    return ShapingError, "disagree by"


def _engine_raises(monkeypatch):
    def process_once(*_a, **_k):
        raise RuntimeError("the engine binary vanished mid-run")

    monkeypatch.setattr(run_benchmark.engine, "process_once", process_once)
    return RuntimeError, "vanished"


def _seeder_dies(monkeypatch):
    calls = []

    def generate_dataset(*_a, **_k):
        calls.append(1)
        if len(calls) > 1:
            raise OSError("no space left on device")
        return {"files": 4, "bytes": 4096}

    monkeypatch.setattr(run_benchmark.seed, "generate_dataset", generate_dataset)
    return None, 3


@pytest.mark.parametrize("refusal", [_refuse_at_close, _engine_raises, _seeder_dies],
                         ids=["contention-refuses-at-close", "engine-raises",
                              "seeder-dies"])
@pytest.mark.parametrize("caller_seed_dir", [False, True],
                         ids=["owned-seed-dir", "caller-seed-dir"])
def test_a_refused_run_removes_its_owned_scratch_and_keeps_its_results_dir(
        tmp_path, monkeypatch, refusal, caller_seed_dir):
    # A refusal that RAISES — a competitor's drift or unreadable report, an
    # engine that died — left main() past the cleanup at its bottom, and so did
    # the seeder's early return: every refused run leaked its seed, work and
    # state directories into the temp dir. The results directory must survive,
    # because it is the record of the refused run.
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(run_benchmark.tempfile, "tempdir", str(scratch))
    results = tmp_path / "results"
    scenario = tmp_path / "refused.yaml"
    scenario.write_text(
        "name: refused-run\n"
        "backend: s3_native\n"
        "endpoint_url: http://localhost:9000\n"
        "volume: 4\n"
        "duration_seconds: 0.3\n"
        "seed_concurrently: true\n"
        "seed_interval_seconds: 0.05\n"
        "sys_sample_interval: 0.1\n",
        encoding="utf-8")
    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path", lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config", lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint", lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.engine, "process_once", lambda *a, **k: (0, [], ""))
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 4, "bytes": 4096})
    raised, expected = refusal(monkeypatch)

    argv = ["run_benchmark.py", str(scenario), "--results-dir", str(results)]
    precious = None
    if caller_seed_dir:
        supplied = tmp_path / "callers-data"
        supplied.mkdir()
        precious = supplied / "keep-me.txt"
        precious.write_text("real data")
        argv += ["--seed-dir", str(supplied)]
    monkeypatch.setattr(sys, "argv", argv)

    if raised is None:
        assert run_benchmark.main() == expected, "the run is refused with its own exit code"
    else:
        with pytest.raises(raised, match=expected):
            run_benchmark.main()

    leaked = sorted(name for name in os.listdir(scratch) if name.startswith(SCRATCH_PREFIXES))
    assert not leaked, (
        f"the refused run leaked {leaked}: scratch the run created must go on every way out, "
        "or a matrix of refused scenarios fills the temp dir one run at a time")
    assert len(os.listdir(results)) == 1, (
        "and the results directory stays: it is the record of the run that was refused")
    if precious is not None:
        assert precious.exists(), "a caller-supplied seed dir is never removed, refused or not"


class SlowToCloseContention:
    """A contended run that closes cleanly but slowly: the flow's closing
    exchange and docker exec's teardown, then a short closing solo window."""

    solo_after_seconds = 0.1

    def start_with_vtop(self):
        pass

    def note_progress(self, _started_at, _ended_at, _uncounted_batches=0):
        pass

    def close(self):
        time.sleep(0.6)

    def describe(self):
        return {}

    def flat_columns(self):
        return run_benchmark.competitor.blank_columns()


def test_a_contended_runs_duration_excludes_everything_closing_the_contention_took(
        tmp_path, monkeypatch):
    # The engine's window ends before close(); close() waits out the flow's
    # collection tail, places the window and measures the closing solo window.
    # Only the solo window's own length used to be subtracted, so the collection
    # tail was billed to an engine that had already stopped and every
    # throughput column was understated by it (review).
    monkeypatch.setattr(run_benchmark.tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(run_benchmark.competitor.CompetitorSpec, "from_scenario",
                        classmethod(lambda cls, sc, shape: object()))

    @contextlib.contextmanager
    def contended(_spec, vtop_bytes):
        yield SlowToCloseContention()

    monkeypatch.setattr(run_benchmark.competitor, "contended", contended)
    results = tmp_path / "results"
    scenario = tmp_path / "slow-close.yaml"
    scenario.write_text(
        "name: slow-close\nbackend: s3_native\nendpoint_url: http://localhost:9000\n"
        "volume: 4\nduration_seconds: 0.3\nsys_sample_interval: 0.1\n", encoding="utf-8")
    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path", lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config", lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint", lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.engine, "process_once", lambda *a, **k: (0, [], ""))
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 4, "bytes": 4096})
    monkeypatch.setattr(sys, "argv", ["run_benchmark.py", str(scenario),
                                      "--results-dir", str(results)])
    run_benchmark.main()
    (run_dir,) = os.listdir(results)
    with open(os.path.join(results, run_dir, "summary.json"), encoding="utf-8") as fh:
        duration = json.load(fh)["duration_seconds"]
    assert duration < 0.3 + 0.4, (
        f"duration_seconds is {duration}: the 0.6 s close() spent after the engine's window "
        "ended — 0.5 s of it collection tail, not solo window — was billed to the engine")


@pytest.mark.parametrize("caller_seed_dir", [False, True],
                         ids=["owned-seed-dir", "caller-seed-dir"])
def test_an_engine_that_raises_stops_the_concurrent_seeder_before_scratch_goes(
        tmp_path, monkeypatch, caller_seed_dir):
    # The seeder was stopped and joined only on the loop's normal way out, so
    # an engine that RAISED left main() through its finally with the daemon
    # thread still writing: the owned seed dir was removed and then recreated
    # by the next round's makedirs, and a caller-supplied one kept filling for
    # as long as an embedding process lived (review).
    import threading

    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setattr(run_benchmark.tempfile, "tempdir", str(scratch))
    results = tmp_path / "results"
    scenario = tmp_path / "engine-raises.yaml"
    scenario.write_text(
        "name: engine-raises\nbackend: s3_native\nendpoint_url: http://localhost:9000\n"
        "volume: 2\nduration_seconds: 30\nseed_concurrently: true\n"
        "seed_interval_seconds: 0.02\nsys_sample_interval: 0.1\n", encoding="utf-8")
    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path", lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config", lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint", lambda *a, **k: "")

    rounds = []

    def generate_dataset(out_dir, *_a, prefix="evt", **_k):
        # What the real generator does to the directory: create it, then write.
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, f"{prefix}-{len(rounds)}.log"), "w") as fh:
            fh.write("x\n")
        rounds.append(1)
        return {"files": 1, "bytes": 2}

    monkeypatch.setattr(run_benchmark.seed, "generate_dataset", generate_dataset)

    def process_once(*_a, **_k):
        # Held until the seeder has written twice, so it is mid-run when the
        # engine dies rather than not yet started.
        deadline = time.monotonic() + 5
        while len(rounds) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        raise RuntimeError("the engine binary vanished mid-run")

    monkeypatch.setattr(run_benchmark.engine, "process_once", process_once)

    argv = ["run_benchmark.py", str(scenario), "--results-dir", str(results)]
    precious = None
    if caller_seed_dir:
        supplied = tmp_path / "callers-data"
        supplied.mkdir()
        precious = supplied / "keep-me.txt"
        precious.write_text("real data")
        argv += ["--seed-dir", str(supplied)]
    monkeypatch.setattr(sys, "argv", argv)

    with pytest.raises(RuntimeError, match="vanished"):
        run_benchmark.main()

    alive = [t for t in threading.enumerate() if t.name == "seeder" and t.is_alive()]
    rounds_at_exit = len(rounds)
    time.sleep(0.2)  # ten seeding intervals
    written_after = len(rounds) - rounds_at_exit
    assert not alive and not written_after, (
        f"{len(alive)} seeder thread(s) still running after main() raised, and "
        f"{written_after} round(s) written in the 0.2 s after it: the run's scratch was "
        "removed with its load generator still writing into it")
    leaked = sorted(name for name in os.listdir(scratch) if name.startswith(SCRATCH_PREFIXES))
    assert not leaked, f"the owned seed dir came back after the cleanup removed it: {leaked}"
    if precious is not None:
        assert precious.exists(), "a caller-supplied seed dir is never removed, refused or not"


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

    def _refuse_after_some_samples(binary, config_path, sc, results_dir, **kwargs):
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
