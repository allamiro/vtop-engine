"""A cell's events reach environment.json as they happen (the v0.11.1 AWS round's lesson 3).

`confirm_group_commit` and the smoke appended to the in-memory environment, and `save_environment` ran only
after a whole cell, so a run killed mid-cell lost the record that its group-commit setting had been confirmed,
or that its smoke had passed. Each is now saved when it is recorded, and so is a cell's refusal, after its
leader log.
"""
import json
import os

import kafka_compare_stub as stub
import pytest

from kafka_compare import run

RATES = {25: 75_000, 50: 150_000, 75: 225_000}
LEADER = "role: leader\n"


class Killed(BaseException):
    """What a kill looks like to the runner: not an Exception, so no cell path catches it."""


def saved_events(run_dir):
    with open(os.path.join(run_dir, "environment.json"), encoding="utf-8") as handle:
        return [event["event"] for event in json.load(handle)["events"]]


def on_ctx(tmp_path):
    return stub.ctx_for(run, str(tmp_path / "run"),
                        group_commit_configs={"on": run.leader_config(LEADER, "on", {"enabled"})})


def kill(*_args):
    raise Killed()


def test_a_confirmed_setting_is_on_disk_before_the_smoke(monkeypatch, tmp_path):
    stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr, hooks={"smoke": kill})
    ctx = on_ctx(tmp_path)
    with pytest.raises(Killed):
        run.run_cell(ctx, "vtop", 100, RATES, group_commit="on")
    assert saved_events(ctx["run_dir"]) == ["group commit on confirmed: vtop_broker_group_commit_enabled 1"]


def test_a_setting_the_node_refused_is_on_disk_when_the_run_stops(monkeypatch, tmp_path):
    stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr)
    monkeypatch.setattr(run, "scrape_leader_metrics", lambda system: stub.GAUGE.format(0))
    ctx = on_ctx(tmp_path)
    with pytest.raises(run.GroupCommitNotApplied):
        run.run_cell(ctx, "vtop", 100, RATES, group_commit="on")
    [event] = saved_events(ctx["run_dir"])
    assert "group commit on was asked for" in event


def test_a_passed_smoke_is_on_disk_before_the_warm_up(monkeypatch, tmp_path):
    stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr, hooks={"producer": kill})
    ctx = stub.ctx_for(run, str(tmp_path / "run"))
    with pytest.raises(Killed):
        run.run_cell(ctx, "kafka", 100, None)
    assert saved_events(ctx["run_dir"]) == ["smoke passed: 10 records byte-exact"]


def _refusing_cluster(*_args):
    raise RuntimeError("docker compose up exited 1: port in use")


def test_a_refused_cell_is_on_disk_when_the_cell_returns(monkeypatch, tmp_path):
    stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr, hooks={"cluster_up": _refusing_cluster})
    ctx = stub.ctx_for(run, str(tmp_path / "run"))
    assert run.run_cell(ctx, "kafka", 100, None) is None
    [event] = saved_events(ctx["run_dir"])
    assert event.startswith("cluster refused before timing: docker compose up exited 1")


def test_a_refused_end_to_end_cell_is_on_disk_when_the_cell_returns(monkeypatch, tmp_path):
    stub.install(run, str(tmp_path / "root"), set_attr=monkeypatch.setattr, hooks={"cluster_up": _refusing_cluster})
    ctx = stub.ctx_for(run, str(tmp_path / "run"), e2e=True, e2e_image="kc-e2e-client:stub")
    ctx["hist_csv"] = os.path.join(ctx["run_dir"], run.schema.E2E_HIST)
    run.run_e2e_cell(ctx, "kafka", "defaults", 100, 3_000)
    [event] = saved_events(ctx["run_dir"])
    assert event.startswith("cluster refused before timing: docker compose up exited 1")
