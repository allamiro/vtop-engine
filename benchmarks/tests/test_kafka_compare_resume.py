"""`run.py --resume RUN_ID` continues an interrupted run (the v0.11.1 AWS round's lesson 4).

Modelled on crash.py's: the run directory is reopened, the resume is refused unless the matrix, the
environment fingerprint, the VTOP commit and the image digests equal the recorded ones, and every unit whose
rows are all present is skipped. A unit is one system's cell of one record size at one group-commit setting,
or the end-to-end stage of one size. A unit with only some of its rows is re-run whole: those rows are moved to
`logs/partial-<tag>.csv` and an event says so, so no key is ever written twice. A skipped Kafka unit's offered
rates come from `environment.json`'s `observed.offered_rates`.
"""
import csv
import json
import os

import kafka_compare_stub as stub
import pytest

from kafka_compare import run, schema

RUN_ID = "resume-me"


class Killed(BaseException):
    """A kill, as the runner sees it."""


def run_dir_of(root):
    return os.path.join(root, "results", RUN_ID)


def rows_of(directory, name="results.csv"):
    with open(os.path.join(directory, name), newline="") as handle:
        return list(csv.DictReader(handle))


def environment_of(directory):
    with open(os.path.join(directory, "environment.json"), encoding="utf-8") as handle:
        return json.load(handle)


def interrupted_run(monkeypatch, root, *, kill_when):
    """A run killed at the first producer call `kill_when(system, rate, leader_config)` accepts."""
    holder = {}

    def producer(system, records, rate):
        leader = holder["calls"]["leader"][-1] or ""
        if kill_when(system, rate, os.path.basename(leader)):
            raise Killed()

    holder["calls"] = stub.install(run, root, set_attr=monkeypatch.setattr, hooks={"producer": producer})
    with pytest.raises(Killed):
        run.main(stub.ARGS + ["--run-id", RUN_ID])
    return run_dir_of(root)


def in_gc_on_fixed_leg(system, rate, leader):
    return system == "vtop" and rate > 0 and leader.endswith("group-commit-on.yaml")


def resume(monkeypatch, root, extra=()):
    calls = stub.install(run, root, set_attr=monkeypatch.setattr)
    code = run.main(stub.ARGS + ["--resume", RUN_ID, *extra])
    return code, calls


def test_an_interrupted_run_says_so_and_owes_rows(monkeypatch, tmp_path):
    directory = interrupted_run(monkeypatch, str(tmp_path), kill_when=in_gc_on_fixed_leg)
    assert environment_of(directory)["run_state"] == "interrupted"
    assert run.check(directory) == 2


def test_a_resume_runs_only_what_is_owed_and_completes_the_run(monkeypatch, tmp_path):
    root = str(tmp_path)
    directory = interrupted_run(monkeypatch, root, kill_when=in_gc_on_fixed_leg)
    code, calls = resume(monkeypatch, root)
    assert code == 0
    # Kafka's cell and VTOP's group-commit-off cell were complete: neither cluster is brought up again. The
    # gc-on cell is re-run, then the end-to-end stage's four targets, which had not started.
    assert [system for system, _partitions in calls["up"]] == ["vtop", "kafka", "vtop", "vtop", "kafka"]
    assert os.path.basename(calls["leader"][0]).endswith("group-commit-on.yaml")
    assert run.check(directory) == 0
    env = environment_of(directory)
    assert env["run_state"] == "complete"
    keys = [schema.row_key(row) for row in rows_of(directory)]
    assert len(keys) == len(set(keys)) == len(schema.expected_keys(env["matrix"]))


def test_a_unit_s_partial_rows_are_moved_aside_and_the_move_is_recorded(monkeypatch, tmp_path):
    root = str(tmp_path)
    directory = interrupted_run(monkeypatch, root, kill_when=in_gc_on_fixed_leg)
    before = [row for row in rows_of(directory) if row["group_commit"] == "on"]
    assert [row["mode"] for row in before] == ["saturation"]  # killed at its first fixed-rate leg
    resume(monkeypatch, root)
    moved = rows_of(os.path.join(directory, "logs"), "partial-vtop-100B-gc-on.csv")
    assert moved == before
    events = [event["event"] for event in environment_of(directory)["events"]]
    assert any(event.startswith("resumed:") for event in events)
    assert any("vtop-100B-gc-on: 1 of its" in event and "partial-vtop-100B-gc-on.csv" in event for event in events)


def test_a_skipped_kafka_unit_gives_vtop_the_rates_it_derived(monkeypatch, tmp_path):
    root = str(tmp_path)
    directory = interrupted_run(monkeypatch, root, kill_when=in_gc_on_fixed_leg)
    resume(monkeypatch, root)
    rates = {row["offered_rate"] for row in rows_of(directory) if row["mode"] == "fixed_rate"}
    assert rates == {"75000", "150000", "225000"}  # shares of the stub Kafka's 300k records/s, from the first run


def test_a_partial_end_to_end_stage_is_re_run_whole_with_its_histograms(monkeypatch, tmp_path):
    root = str(tmp_path)
    seen = {"e2e": 0}

    def in_second_e2e_target(system, rate, leader):
        # The end-to-end stage offers 3000 records/s (50 % of the stub VTOP's 6000); its warm-up and its rep
        # are two calls per target, so the third call is the second target's warm-up.
        if rate == 3000:
            seen["e2e"] += 1
        return seen["e2e"] == 3

    directory = interrupted_run(monkeypatch, root, kill_when=in_second_e2e_target)
    assert [row["system"] for row in rows_of(directory) if row["mode"] == "e2e"] == ["kafka"]
    assert len(rows_of(directory, schema.E2E_HIST)) == 2
    code, calls = resume(monkeypatch, root)
    assert code == 0 and [system for system, _ in calls["up"]] == ["kafka", "vtop", "vtop", "kafka"]
    assert run.check(directory) == 0
    assert len(rows_of(os.path.join(directory, "logs"), "partial-e2e-100B.csv")) == 1
    assert len(rows_of(os.path.join(directory, "logs"), "partial-e2e-100B-e2e_hist.csv")) == 2
    assert len(rows_of(directory, schema.E2E_HIST)) == 8  # two buckets for each of the four targets
    schema.load_run(directory)


def test_a_resume_with_another_matrix_is_refused(monkeypatch, tmp_path):
    root = str(tmp_path)
    interrupted_run(monkeypatch, root, kill_when=in_gc_on_fixed_leg)
    stub.install(run, root, set_attr=monkeypatch.setattr)
    argv = [arg if arg != "1" else "2" for arg in stub.ARGS]  # --reps 2 and --fixed-seconds 2
    with pytest.raises(SystemExit, match=f"--resume {RUN_ID}: the matrix differs"):
        run.main(argv + ["--resume", RUN_ID])


def test_a_resume_on_another_image_is_refused_naming_it(monkeypatch, tmp_path):
    root = str(tmp_path)
    interrupted_run(monkeypatch, root, kill_when=in_gc_on_fixed_leg)
    stub.install(run, root, set_attr=monkeypatch.setattr)
    capture = run.capture_environment

    def rebuilt(*args, **kwargs):
        env = capture(*args, **kwargs)
        env["comparable"]["vtop_image_id"] = "sha256:" + "9" * 64
        return env

    monkeypatch.setattr(run, "capture_environment", rebuilt)
    with pytest.raises(SystemExit, match="vtop_image_id"):
        run.main(stub.ARGS + ["--resume", RUN_ID])


def test_a_complete_run_is_not_resumed(monkeypatch, tmp_path):
    root = str(tmp_path)
    stub.install(run, root, set_attr=monkeypatch.setattr)
    assert run.main(stub.ARGS + ["--run-id", RUN_ID]) == 0
    with pytest.raises(SystemExit, match="is complete"):
        run.main(stub.ARGS + ["--resume", RUN_ID])


def test_resume_and_run_id_together_are_refused(monkeypatch, tmp_path):
    stub.install(run, str(tmp_path), set_attr=monkeypatch.setattr)
    with pytest.raises(SystemExit, match="--resume continues"):
        run.main(stub.ARGS + ["--resume", RUN_ID, "--run-id", "other"])


def test_a_resume_of_a_run_that_does_not_exist_is_refused(monkeypatch, tmp_path):
    stub.install(run, str(tmp_path), set_attr=monkeypatch.setattr)
    with pytest.raises(SystemExit, match="no environment.json"):
        run.main(stub.ARGS + ["--resume", RUN_ID])


def test_histogram_rows_of_an_end_to_end_stage_with_no_result_row_yet_are_moved_aside(monkeypatch, tmp_path):
    # Review of #949: a kill between the histogram rows and the row that vouches for them left buckets with no
    # row; the stage ran again and wrote them twice, and the report refused the run.
    root = str(tmp_path)

    def histogram_then_kill(ctx, row, hist):
        key = {column: row[column] for column in schema.E2E_HIST_KEY}
        for bucket, count in sorted(hist.items()):
            schema.append_row(ctx["hist_csv"], {**key, "bucket_ms": bucket, "count": count}, schema.E2E_HIST_COLUMNS)
        raise Killed()

    stub.install(run, root, set_attr=monkeypatch.setattr)
    monkeypatch.setattr(run, "write_e2e_result", histogram_then_kill)
    with pytest.raises(Killed):
        run.main(stub.ARGS + ["--run-id", RUN_ID])
    directory = run_dir_of(root)
    assert not [row for row in rows_of(directory) if row["mode"] == "e2e"]
    assert len(rows_of(directory, schema.E2E_HIST)) == 2
    monkeypatch.undo()
    code, _calls = resume(monkeypatch, root)
    assert code == 0 and run.check(directory) == 0
    schema.load_run(directory)  # refused before: "gives bucket 1 ms twice"
    assert len(rows_of(os.path.join(directory, "logs"), "partial-e2e-100B-e2e_hist.csv")) == 2


def test_kafka_s_offered_rates_are_on_disk_before_its_first_fixed_rate_leg(monkeypatch, tmp_path):
    # Review of #949: they were saved only after the whole cell, so a SIGKILL late in Kafka's cell (its rows all
    # written, no `except` run) left a unit the resume skipped with no rates to give VTOP. What is on disk is
    # read at the moment the first leg starts, as a SIGKILL there would leave it.
    seen = []

    def read_disk(system, rate, leader):
        if rate > 0 and not seen:
            seen.append(environment_of(run_dir_of(str(tmp_path)))["observed"].get("offered_rates"))
        return rate > 0

    interrupted_run(monkeypatch, str(tmp_path), kill_when=read_disk)
    assert seen == [{"100": {"25": 75000, "50": 150000, "75": 225000}}]


def test_a_skipped_kafka_unit_with_no_recorded_rates_derives_them_from_its_rows(monkeypatch, tmp_path):
    root = str(tmp_path)
    directory = interrupted_run(monkeypatch, root, kill_when=in_gc_on_fixed_leg)
    env = environment_of(directory)
    del env["observed"]["offered_rates"]
    with open(os.path.join(directory, "environment.json"), "w", encoding="utf-8") as handle:
        json.dump(env, handle)
    code, _calls = resume(monkeypatch, root)
    assert code == 0
    vtop_on = [row for row in rows_of(directory) if row["group_commit"] == "on" and row["mode"] == "fixed_rate"]
    assert {(row["status"], row["offered_rate"]) for row in vtop_on} == {("ok", "75000"), ("ok", "150000"),
                                                                        ("ok", "225000")}
