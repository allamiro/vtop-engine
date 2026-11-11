"""The operator egress ceiling (#481) in the harness: the cap reaches the
engine, the run records it, and the rate criterion is judged on the engine's
own counter.

The criterion's arithmetic is checked here on hand-built engine_metrics.csv
files, because the live runs it judges cannot be part of a unit suite and a
checker that cannot fail would file every run as a pass.
"""
from __future__ import annotations

import csv
import glob
import json
import os

import pytest

import egress_cap_check
import run_matrix
from lib import egress_cap, engine_run
from lib.engine import egress_cap_of, write_engine_config
from lib.metrics import CSV_HEADERS
from lib.scenario import load_scenario

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCENARIOS = os.path.join(HERE, "scenarios")
MIB = 1024 * 1024


def config_text(tmp_path, scenario) -> str:
    path = str(tmp_path / "engine.yaml")
    write_engine_config(scenario, str(tmp_path / "work"), str(tmp_path / "state.db"),
                        str(tmp_path / "seed" / "*"), path)
    with open(path, encoding="utf-8") as fh:
        return fh.read()


# --- the cap reaches the engine ----------------------------------------------


def test_a_capped_scenario_writes_the_cap_into_the_upload_block(tmp_path):
    text = config_text(tmp_path, {"backend": "s3_native",
                                  "upload_max_egress_bytes_per_second": 4 * MIB})
    upload = text.split("\nupload:\n", 1)[1]
    assert "  max_egress_bytes_per_second: 4194304\n" in upload, (
        "a capped scenario must RUN capped: the cap has to land in the engine's upload "
        f"block, or the run measures the uncapped pipeline under a capped name. Got:\n{text}")


def test_an_unset_cap_writes_nothing(tmp_path):
    for unset in ({}, {"upload_max_egress_bytes_per_second": ""},
                  {"upload_max_egress_bytes_per_second": None}):
        text = config_text(tmp_path, {"backend": "s3_native", **unset})
        assert "max_egress_bytes_per_second" not in text, (
            f"{unset} is unset, and unset must write today's config exactly: {text}")


def test_a_zero_cap_is_written_through_for_the_engine_to_refuse(tmp_path):
    # A truthiness test would drop 0 and run uncapped while summary.json
    # recorded 0. The engine refuses a cap below 1024 B/s at load, and that
    # refusal is the outcome an operator's config would get.
    text = config_text(tmp_path, {"backend": "s3_native",
                                  "upload_max_egress_bytes_per_second": 0})
    assert "  max_egress_bytes_per_second: 0\n" in text, text
    assert egress_cap_of({"upload_max_egress_bytes_per_second": 0}) == "0"


def test_the_cap_is_a_column_wherever_a_run_is_compared():
    assert "upload_max_egress_bytes_per_second" in CSV_HEADERS["metrics.csv"], (
        "the with- and without-cap runs of the competitor pair differ in the cap alone; "
        "metrics.csv without it shows two identical conditions")
    assert "upload_max_egress_bytes_per_second" in run_matrix.COMPARE_COLS


def test_the_shapers_counter_is_a_surfaced_summary_series():
    assert ("vtop_upload_egress_bytes_total", "counter") in engine_run.SUMMARY_SERIES
    columns = engine_run.summary_column_names()
    assert "engine_upload_egress_bytes_total_window" in columns
    assert "engine_upload_egress_bytes_total_max_per_second" in columns


# --- the bundled scenarios ---------------------------------------------------


RATE_SCENARIOS = {
    "19-egress-cap-rate-1mib.yaml": 1 * MIB,
    "20-egress-cap-rate-4mib.yaml": 4 * MIB,
    "21-egress-cap-rate-16mib.yaml": 16 * MIB,
}


def _knobs(path: str, *also: str) -> dict:
    values = dict(load_scenario(path).values)
    for key in ("name", "description", "upload_max_egress_bytes_per_second", *also):
        values.pop(key, None)
    return values


# What a rate scenario sizes to its own R: the backlog seeded before launch and
# the concurrent seeder's pace. Everything else must be identical across R.
SEEDING_KNOBS = ("volume", "backlog_multiplier", "seed_interval_seconds")


def test_the_rate_scenarios_differ_in_the_cap_and_its_seeding_alone():
    paths = {name: os.path.join(SCENARIOS, name) for name in RATE_SCENARIOS}
    for name, path in paths.items():
        sc = load_scenario(path)
        assert int(sc.get("upload_max_egress_bytes_per_second")) == RATE_SCENARIOS[name], name
        assert sc.get("engine_mode") == engine_run.LONG_LIVED, (
            f"{name}: the cap's token bucket lives in the engine process, and only a "
            "long-lived engine exposes its counter for a minute")
        assert sc.get("backend") == "s3_native", (
            f"{name}: every other backend refuses a cap at startup")
        assert int(sc.get("duration_seconds")) == 60, name
    baseline = _knobs(paths["19-egress-cap-rate-1mib.yaml"], *SEEDING_KNOBS)
    for name, path in paths.items():
        assert _knobs(path, *SEEDING_KNOBS) == baseline, (
            f"{name} differs from scenario 19 in more than the cap and its seeding")


def test_every_rate_scenario_offers_more_than_its_cap():
    # The ratio half of the criterion is a statement about the shaper only if
    # the shaper is the bottleneck: the concurrent seeder must offer more than
    # R, or the window measures the seeder (the first 16 MiB/s lab run did).
    medium_mean = 5.5 * MIB
    for name, cap in RATE_SCENARIOS.items():
        sc = load_scenario(os.path.join(SCENARIOS, name))
        assert sc.get("seed_concurrently") is True and sc.get("compression") == "none", name
        assert sc.get("file_size") == "medium", name
        per_round = max(1, round(int(sc.get("volume")) * float(sc.get("backlog_multiplier"))))
        offered = per_round * medium_mean / float(sc.get("seed_interval_seconds"))
        assert offered >= 1.2 * cap, f"{name} offers {offered:.0f} B/s against a cap of {cap}"


def test_the_competitor_pair_differs_in_the_cap_alone():
    uncapped = load_scenario(os.path.join(SCENARIOS, "22-competing-flow-bulk-uncapped.yaml"))
    capped = load_scenario(os.path.join(SCENARIOS, "23-competing-flow-bulk-capped.yaml"))
    assert egress_cap_of(uncapped) == ""
    assert egress_cap_of(capped) == "262144"
    for sc in (uncapped, capped):
        assert sc.get("engine_mode") == engine_run.PROCESS_ONCE, (
            "the competitor refuses a long-lived engine: VTOP's share is attributed per cycle")
        assert sc.get("shaping_competitor")
    assert _knobs(os.path.join(SCENARIOS, "22-competing-flow-bulk-uncapped.yaml")) == \
        _knobs(os.path.join(SCENARIOS, "23-competing-flow-bulk-capped.yaml"))


def test_no_bundled_scenario_caps_a_backend_that_refuses_a_cap():
    for path in sorted(glob.glob(os.path.join(SCENARIOS, "*.yaml"))):
        sc = load_scenario(path)
        if egress_cap_of(sc) != "":
            assert sc.get("backend") == "s3_native", (
                f"{os.path.basename(path)} caps backend {sc.get('backend')!r}, which refuses "
                "the cap at startup — the scenario could never run")


# --- the criterion -------------------------------------------------------------


def write_metrics(path, samples, counter=egress_cap.COUNTER):
    """`samples`: (elapsed, interval, latency, cumulative counter value or None)."""
    with open(path, "w", newline="", encoding="utf-8") as fh:
        out = csv.writer(fh)
        out.writerow(engine_run.ENGINE_METRICS_COLUMNS)
        previous = None
        for position, (elapsed, interval, latency, value) in enumerate(samples):
            common = ["2026-11-10T00:00:00Z", f"{elapsed:.3f}",
                      f"{interval:.3f}", f"{latency:.6f}"]
            out.writerow(common + ["vtop_commits_total", "", "counter", "0", "0",
                                   "" if position == 0 else "0.0"])
            if value is not None:
                delta = value - (previous or 0)
                out.writerow(common + [counter, "", "counter", str(value), str(delta),
                                       "" if position == 0 else repr(delta / interval)])
                previous = value


def steady(cap, seconds, fraction=31 / 32, absent_first=0):
    samples = [(0.05, 0.05, 0.002, None)]
    total = 0.0
    for second in range(1, seconds + 1):
        if second <= absent_first:
            samples.append((0.05 + second, 1.0, 0.002, None))
            continue
        total += cap * fraction
        samples.append((0.05 + second, 1.0, 0.002, total))
    return samples


def test_a_saturated_shaper_passes_at_the_predicted_ratio(tmp_path):
    path = str(tmp_path / "engine_metrics.csv")
    write_metrics(path, steady(MIB, 60))
    verdict = egress_cap.evaluate(egress_cap.read_buckets(path), MIB)
    assert verdict["buckets"] == 60, "the opening span is not a bucket"
    assert verdict["ratio_to_cap"] == pytest.approx(31 / 32, abs=1e-6)
    assert verdict["pass"] is True, verdict


def test_one_bucket_over_the_cap_fails_the_bucket_half(tmp_path):
    samples = steady(MIB, 60)
    elapsed, interval, latency, value = samples[30]
    samples[30] = (elapsed, interval, latency, value + 0.1 * MIB)  # +10% in bucket 30 only
    for i in range(31, len(samples)):
        e, d, lat, v = samples[i]
        samples[i] = (e, d, lat, v + 0.1 * MIB)
    path = str(tmp_path / "engine_metrics.csv")
    write_metrics(path, samples)
    verdict = egress_cap.evaluate(egress_cap.read_buckets(path), MIB)
    assert verdict["buckets_over_cap"] == [30], verdict
    assert verdict["worst_bucket"]["index"] == 30
    assert verdict["pass_every_bucket_at_or_below_cap"] is False
    assert verdict["pass_total_within_tolerance"] is True
    assert verdict["pass"] is False


def test_a_starved_window_fails_the_total_half(tmp_path):
    path = str(tmp_path / "engine_metrics.csv")
    write_metrics(path, steady(MIB, 60, fraction=0.8))
    verdict = egress_cap.evaluate(egress_cap.read_buckets(path), MIB)
    assert verdict["pass_every_bucket_at_or_below_cap"] is True
    assert verdict["pass_total_within_tolerance"] is False, verdict
    assert verdict["pass"] is False


def test_seconds_before_the_counter_exists_count_as_zero_not_as_missing(tmp_path):
    # The engine creates the counter on the shaper's first admission. Skipping
    # the seconds before it would shrink the denominator and flatter the ratio.
    path = str(tmp_path / "engine_metrics.csv")
    write_metrics(path, steady(MIB, 60, absent_first=10))
    buckets = egress_cap.read_buckets(path)
    verdict = egress_cap.evaluate(buckets, MIB)
    assert len(buckets) == 60 and verdict["zero_byte_buckets"] == 10
    assert verdict["ratio_to_cap"] == pytest.approx(50 / 60 * 31 / 32, abs=1e-6)
    assert verdict["pass_total_within_tolerance"] is False


def test_a_run_without_the_counter_cannot_be_judged(tmp_path):
    path = str(tmp_path / "engine_metrics.csv")
    write_metrics(path, steady(MIB, 5), counter="vtop_some_other_total")
    with pytest.raises(ValueError, match="never carries vtop_upload_egress_bytes_total"):
        egress_cap.read_buckets(path)


def test_the_latency_bound_is_reported_beside_the_bucket(tmp_path):
    path = str(tmp_path / "engine_metrics.csv")
    samples = [(0.05, 0.05, 0.002, None), (1.05, 1.0, 0.05, MIB * 0.99)]
    write_metrics(path, samples)
    (bucket,) = egress_cap.read_buckets(path)
    assert bucket.rate == pytest.approx(MIB * 0.99)
    assert bucket.rate_upper_bound == pytest.approx(MIB * 0.99 / 0.95)
    verdict = egress_cap.evaluate([bucket], MIB)
    assert verdict["buckets_over_cap"] == []
    assert verdict["buckets_whose_latency_bound_exceeds_cap"] == [1]


def test_the_checker_writes_its_files_and_exits_by_the_verdict(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    write_metrics(str(run_dir / engine_run.ENGINE_METRICS_CSV), steady(MIB, 60, fraction=0.8))
    (run_dir / "summary.json").write_text(json.dumps(
        {"upload_max_egress_bytes_per_second": str(MIB)}))
    assert egress_cap_check.main([str(run_dir)]) == 1, "a failed criterion must exit nonzero"
    verdict = json.loads((run_dir / "egress_verdict.json").read_text())
    assert verdict["pass"] is False and verdict["cap_bytes_per_second"] == MIB
    with open(run_dir / "egress_buckets.csv", newline="", encoding="utf-8") as fh:
        assert len(list(csv.DictReader(fh))) == 60


def test_the_checker_refuses_an_uncapped_run(tmp_path, capsys):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "summary.json").write_text(json.dumps({"upload_max_egress_bytes_per_second": ""}))
    assert egress_cap_check.main([str(run_dir)]) == 2
    assert "uncapped" in capsys.readouterr().err


# --- review fixes (#481) -------------------------------------------------------


def test_a_yaml_null_spelling_of_the_cap_is_refused_not_written():
    # Written through as given, "null" or "~" reaches the engine as YAML null,
    # which is unset: the run would go uncapped under a capped label.
    for spelling in ("null", "Null", "NULL", "~", "1e6", "4 MiB", "4194304\n  x: 1", True):
        with pytest.raises(ValueError, match="upload_max_egress_bytes_per_second"):
            egress_cap_of({"upload_max_egress_bytes_per_second": spelling})
    assert egress_cap_of({"upload_max_egress_bytes_per_second": " 4194304 "}) == "4194304"
    assert egress_cap_of({"upload_max_egress_bytes_per_second": -5}) == "-5", (
        "a negative cap still goes through for the engine to refuse")


def test_a_signed_or_zero_padded_cap_is_written_in_canonical_form(tmp_path):
    # "+4194304" is the same cap the scenario parser reads as int 4194304; the
    # engine config and the recorded column carry one spelling of it.
    for spelling in ("+4194304", "004194304", "+004194304", 4194304):
        assert egress_cap_of({"upload_max_egress_bytes_per_second": spelling}) == "4194304"
    text = config_text(tmp_path, {"backend": "s3_native",
                                  "upload_max_egress_bytes_per_second": "+4194304"})
    assert "  max_egress_bytes_per_second: 4194304\n" in text, text


def test_initial_seed_is_a_non_negative_integer_or_entropy():
    from lib.scenario import initial_seed

    assert initial_seed({}) == 0, "absent keeps OS entropy, every older scenario's behaviour"
    assert initial_seed({"initial_seed": 481}) == 481
    assert initial_seed({"initial_seed": "481"}) == 481
    for bad in (-1, "abc", True, 1.5):
        with pytest.raises(ValueError, match="initial_seed"):
            initial_seed({"initial_seed": bad})


def _ideal_total_ratio(cap, up_front, interval, rounds):
    """The 60 s total an IDEAL shaper (refill R - R/32, no engine gaps) could
    reach on this supply, as a fraction of R x 60, in 10 ms steps."""
    arrivals = {round(k * interval * 100): b for k, b in enumerate(rounds, 1)}
    supply, sent, refill = float(up_front), 0.0, (cap - cap // 32) / 100
    for tick in range(6000):
        supply += arrivals.get(tick, 0)
        sent += min(refill, supply - sent)
    return sent / (cap * 60)


def test_the_rate_scenarios_pin_a_dataset_that_cannot_starve_the_criterion(tmp_path,
                                                                            monkeypatch):
    # Replays scenarios 19 and 20's seeding - the pinned up-front volume, then
    # the concurrent rounds at their deterministic seeds - against an ideal
    # shaper, and asserts the workload alone cannot pull the 60 s total under
    # the 0.9 criterion. With OS entropy, two up-front files drawn at their
    # 1 MiB minimum limit scenario 19 to 0.864 of R x 60 (review). Scenario 21
    # is checked for the pin only: its up-front volume is 60 files, which no
    # draw makes small, and generating it here would write ~350 MB.
    from lib import seed as seed_lib
    from lib.scenario import initial_seed, reseed_count

    for name in RATE_SCENARIOS:
        sc = load_scenario(os.path.join(SCENARIOS, name))
        assert initial_seed(sc) != 0, f"{name} must pin its up-front dataset"

    # The up-front volume is generated for real: its second file's size
    # depends on the randomness the first file's content consumed.
    up_front = {}
    for name in ("19-egress-cap-rate-1mib.yaml", "20-egress-cap-rate-4mib.yaml"):
        sc = load_scenario(os.path.join(SCENARIOS, name))
        up_front[name] = seed_lib.generate_dataset(
            str(tmp_path / name), sc.format, int(sc.volume), sc.file_size,
            seed=initial_seed(sc))["bytes"]

    # A round is ONE file, whose size is drawn right after its seed, before any
    # content: the target is exact without writing it (a written file is at
    # least its target, so this is the conservative side).
    monkeypatch.setattr(seed_lib, "_write_text_file", lambda path, target, fmt: target)
    for name in ("19-egress-cap-rate-1mib.yaml", "20-egress-cap-rate-4mib.yaml"):
        sc = load_scenario(os.path.join(SCENARIOS, name))
        assert reseed_count(int(sc.volume), float(sc.get("backlog_multiplier"))) == 1, name
        interval = float(sc.get("seed_interval_seconds"))
        rounds = [seed_lib.generate_dataset(str(tmp_path / "r"), sc.format, 1, sc.file_size,
                                            seed=10_000 + k, prefix=f"evt{k}")["bytes"]
                  for k in range(1, int(60 / interval) + 1)]
        ratio = _ideal_total_ratio(RATE_SCENARIOS[name], up_front[name], interval, rounds)
        assert ratio >= 0.95, (
            f"{name}: its own seeding limits even an ideal shaper to {ratio:.4f} of R x 60")
        if name.startswith("19"):
            # The check can fail: the smallest entropy draw does starve it.
            starved = _ideal_total_ratio(RATE_SCENARIOS[name], 2 * MIB, interval, rounds)
            assert starved < 0.9, starved


def test_the_matrix_takes_the_cap_the_run_recorded_not_the_scenarios_spelling():
    assert "upload_max_egress_bytes_per_second" in run_matrix.RESOLVED_COLUMNS
    row = run_matrix.matrix_row({
        "upload_max_egress_bytes_per_second": "",
        "scenario": {"upload_max_egress_bytes_per_second": None},
    })
    assert row["upload_max_egress_bytes_per_second"] == "", row


def test_a_sub_second_bucket_is_refused_rather_than_judged(tmp_path):
    # A 0.1 s bucket may legitimately read above R while every one-second
    # window holding it stays below R; judging it would fail compliant runs.
    path = str(tmp_path / "engine_metrics.csv")
    samples = [(0.05, 0.05, 0.002, None)]
    for tick in range(1, 101):
        samples.append((0.05 + tick / 10, 0.1, 0.002, tick * MIB * 31 / 32 / 10))
    write_metrics(path, samples)
    with pytest.raises(ValueError, match="shorter than"):
        egress_cap.evaluate(egress_cap.read_buckets(path), MIB)


def test_a_bucket_is_judged_at_the_runners_precision_not_the_rounded_interval(tmp_path):
    # interval_seconds is rounded to milliseconds in the CSV; delta_per_second
    # was computed from the unrounded interval and is what the run recorded.
    path = str(tmp_path / "engine_metrics.csv")
    precise = 0.99951
    delta = MIB * 0.9998
    with open(path, "w", newline="", encoding="utf-8") as fh:
        out = csv.writer(fh)
        out.writerow(engine_run.ENGINE_METRICS_COLUMNS)
        out.writerow(["t", "0.050", "0.050", "0.001", egress_cap.COUNTER, "", "counter",
                      "0", "0", ""])
        out.writerow(["t", "1.050", f"{precise:.3f}", "0.001", egress_cap.COUNTER, "",
                      "counter", repr(delta), repr(delta), repr(round(delta / precise, 6))])
    (bucket,) = egress_cap.read_buckets(path)
    assert bucket.interval_seconds == pytest.approx(precise, rel=1e-9)
    assert bucket.rate > MIB, "the rate the run recorded is over the cap..."
    assert delta / round(precise, 3) <= MIB, "...and the rounded interval would have hidden it"
    assert egress_cap.evaluate([bucket], MIB)["buckets_over_cap"] == [1]


@pytest.mark.parametrize("body", [
    "not,a,metrics,file\n1,2,3,4\n",
    ",".join(engine_run.ENGINE_METRICS_COLUMNS) + "\n0.05,0.05\n",
    ",".join(engine_run.ENGINE_METRICS_COLUMNS) + "\nt,x,1.0,0.001,"
    + egress_cap.COUNTER + ",,counter,1,1,1\n",
])
def test_a_malformed_metrics_file_cannot_be_judged_and_says_so(tmp_path, capsys, body):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / engine_run.ENGINE_METRICS_CSV).write_text(body)
    (run_dir / "summary.json").write_text(json.dumps(
        {"upload_max_egress_bytes_per_second": str(MIB)}))
    assert egress_cap_check.main([str(run_dir)]) == 2
    assert "egress_cap_check:" in capsys.readouterr().err


def test_a_committed_record_is_judged_from_its_selected_series(tmp_path):
    # benchmarks/measurements/ keeps engine_metrics_selected.csv, not the full
    # file; the checker must still be able to re-judge such a record.
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    write_metrics(str(run_dir / egress_cap_check.SELECTED_METRICS_CSV), steady(MIB, 60))
    (run_dir / "summary.json").write_text(json.dumps(
        {"upload_max_egress_bytes_per_second": str(MIB)}))
    assert egress_cap_check.main([str(run_dir), "--out", str(tmp_path / "out")]) == 0


def _sample_starts(interval: float, sample_seconds: float, samples: int) -> list[float]:
    """Drive SystemMonitor's loop in VIRTUAL time, synchronously, and return
    when each sample started. Nothing here reads the wall clock, so a busy
    worker cannot move a start (review: the first version slept for real)."""
    from lib.sysmon import SystemMonitor

    clock = [0.0]
    starts: list[float] = []
    monitor = SystemMonitor(lambda sample: None, interval=interval)

    def sample():
        starts.append(clock[0])
        clock[0] += sample_seconds
        if len(starts) == samples:
            monitor._stop.set()
        return {}

    def wait(seconds):
        assert seconds > 0, f"the loop asked to wait {seconds} s: a burst, not a schedule"
        clock[0] += seconds

    monitor._sample = sample
    monitor._clock = lambda: clock[0]
    monitor._wait = wait
    monitor._run()
    return starts


def test_system_samples_keep_the_configured_cadence_when_a_sample_takes_time():
    # Container mode's `docker stats --no-stream` takes about a second, and
    # "sample, then wait an interval" stretched every period by that much: the
    # rate runs' rows were 2 s apart at a 1 s interval (review). Scheduled from
    # deadlines, a 0.2 s sample inside a 0.3 s interval keeps the 0.3 s grid;
    # the old loop started one every 0.5 s.
    starts = _sample_starts(interval=0.3, sample_seconds=0.2, samples=6)
    assert starts == pytest.approx([0.0, 0.3, 0.6, 0.9, 1.2, 1.5]), starts


def test_a_sample_longer_than_the_interval_skips_to_the_next_slot_without_a_burst():
    # A 0.5 s sample at a 0.3 s interval cannot keep the grid; it must land on
    # the next free grid point (0.6, 1.2, ...) rather than fire immediately to
    # catch up on the slots it covered.
    starts = _sample_starts(interval=0.3, sample_seconds=0.5, samples=4)
    assert starts == pytest.approx([0.0, 0.6, 1.2, 1.8]), starts


def test_every_committed_rate_record_rejudges_to_its_committed_verdict(tmp_path):
    root = os.path.join(HERE, "measurements", "481-egress-ceiling", "rate")
    runs = sorted(glob.glob(os.path.join(root, "*")))
    assert len(runs) == 4, runs
    for run in runs:
        out = tmp_path / os.path.basename(run)
        egress_cap_check.main([run, "--out", str(out)])
        with open(os.path.join(run, "egress_verdict.json"), encoding="utf-8") as fh:
            committed = json.load(fh)
        assert json.loads((out / "egress_verdict.json").read_text()) == committed, run


def test_a_pinned_dataset_never_overwrites_a_file_already_in_the_seed_dir(tmp_path):
    # With initial_seed the names repeat exactly, so a rerun into the same
    # --seed-dir used to reopen the previous run's paths with "w" (review).
    from lib import seed as seed_lib

    seed_dir = tmp_path / "seed"
    seed_lib.generate_dataset(str(seed_dir), "jsonl", 2, "small", seed=481, prefix="aaaaaa-evt")
    before = {p.name: p.read_bytes() for p in seed_dir.iterdir()}
    with pytest.raises(FileExistsError):
        seed_lib.generate_dataset(str(seed_dir), "jsonl", 2, "small", seed=481,
                                  prefix="aaaaaa-evt")
    after = {p.name: p.read_bytes() for p in seed_dir.iterdir()}
    assert after == before, "a refused generation must leave the existing files untouched"

    # A second run gets its own namespace: same bytes, new names, nothing lost.
    seed_lib.generate_dataset(str(seed_dir), "jsonl", 2, "small", seed=481, prefix="bbbbbb-evt")
    files = sorted(seed_dir.iterdir())
    assert len(files) == 4
    first = sorted(p.read_bytes() for p in files if p.name.startswith("aaaaaa"))
    second = sorted(p.read_bytes() for p in files if p.name.startswith("bbbbbb"))
    assert first == second, "the pinned seed must still reproduce the same bytes"


def test_a_generation_that_collides_part_way_leaves_nothing_behind(tmp_path):
    # A collision on a LATER file used to leave the earlier files of the same
    # call in the seed dir, where the next run counted them as input (review).
    from lib import seed as seed_lib

    scout = tmp_path / "scout"
    seed_lib.generate_dataset(str(scout), "jsonl", 3, "small", seed=481, prefix="t-evt")
    names = sorted(p.name for p in scout.iterdir())
    assert len(names) == 3

    seed_dir = tmp_path / "seed"
    seed_dir.mkdir()
    (seed_dir / names[2]).write_text("the caller's own file\n")
    with pytest.raises(FileExistsError):
        seed_lib.generate_dataset(str(seed_dir), "jsonl", 3, "small", seed=481, prefix="t-evt")
    assert sorted(p.name for p in seed_dir.iterdir()) == [names[2]], (
        "the two files the failed call created before the collision must be removed")
    assert (seed_dir / names[2]).read_text() == "the caller's own file\n", (
        "the file that was already there is not this call's and must be untouched")


def test_a_generation_that_fails_mid_file_removes_its_partial_file_too(tmp_path, monkeypatch):
    from lib import seed as seed_lib

    real = seed_lib._write_text_file
    calls = []

    def failing_on_the_second(path, target, fmt):
        calls.append(path)
        if len(calls) == 2:
            with open(path, "x", encoding="utf-8") as fh:
                fh.write("half a fi")
            raise OSError(28, "No space left on device")
        return real(path, target, fmt)

    monkeypatch.setattr(seed_lib, "_write_text_file", failing_on_the_second)
    seed_dir = tmp_path / "seed"
    with pytest.raises(OSError, match="No space left"):
        seed_lib.generate_dataset(str(seed_dir), "jsonl", 3, "small", seed=481, prefix="t-evt")
    assert list(seed_dir.iterdir()) == [], "the complete file and the partial one are both gone"


def test_every_seeding_call_in_the_runner_is_namespaced_by_the_run():
    # The runner's three generations (up front, concurrent rounds, per-cycle
    # reseeds) must each carry the run's tag, or a rerun into the same
    # --seed-dir collides on the deterministic names.
    import re

    with open(os.path.join(HERE, "run_benchmark.py"), encoding="utf-8") as fh:
        source = fh.read()
    calls = re.findall(r"seed\.generate_dataset\((.*?)\)\n", source, flags=re.S)
    assert len(calls) == 3, calls
    for call in calls:
        assert 'prefix=f"{run_tag}-' in call, call
