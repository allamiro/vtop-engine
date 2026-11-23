"""A newer committed run replaces an earlier one's pages and charts (#600): `supersedes` in `report.json`.

`regenerate.py` refuses two committed runs that write the same file, `report.json` could only send a run's
output somewhere else, and `readme_charts.py` named its two runs by id. So a newer run of a kind could not take
over the kind's page, and the README kept quoting the laptop run. Now a run's `report.json` may say
`"supersedes": "<run-id>"`: a committed run of the same kind in the same results directory that started earlier.
`regenerate.py` does not hand a superseded run to the report (its raw data stays committed), still refuses two
current runs writing one file, and `readme_charts.py` draws the one current run of each kind through the same
helper, refusing when there is not exactly one.

These tests build the results directory in a temporary git repository, as `test_kafka_compare_regenerate.py`
does, and stand in for report.py with a function that writes the page each run's outputs name.
"""
import json
import os
import subprocess

import pytest

import readme_charts
from kafka_compare import regenerate, run_outputs

RESULTS = regenerate.RESULTS


def git(repo, *args):
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@localhost", *args], cwd=repo, check=True,
                   capture_output=True)


@pytest.fixture
def repo(tmp_path):
    git(tmp_path, "init", "-q")
    return tmp_path


KIND_FILE = {"throughput": "results.csv", "failover": "failover.csv", "crash": "crash.csv"}


def add_run(repo, run_id, started_at, kind="throughput", report=None):
    directory = repo / RESULTS / run_id
    directory.mkdir(parents=True)
    env = {"run_id": run_id, "started_at": started_at}
    if kind == "failover":
        env["kind"] = "failover"
    (directory / "environment.json").write_text(json.dumps(env))
    (directory / KIND_FILE[kind]).write_text("x\n")
    if report is not None:
        (directory / "report.json").write_text(json.dumps(report))
    return str(directory)


def commit(repo):
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "c")


def fake_report(repo):
    """What report.py writes for a run, reduced to its page: the run's own `doc`, else its kind's default."""
    defaults = {"throughput": "kafka-comparison.md", "failover": "kafka-failover.md", "crash": "kafka-crash.md"}

    def report(run_dir):
        own = run_outputs.resolve(run_dir, str(repo), "docs/benchmarks/img", allow_latency_run=True,
                                  allow_supersedes=True)
        doc = own.doc if own and own.doc else os.path.join(str(repo), "docs", "benchmarks",
                                                           defaults[run_outputs.run_kind(run_dir)])
        os.makedirs(os.path.dirname(doc), exist_ok=True)
        with open(doc, "w") as handle:
            handle.write(os.path.basename(run_dir))
        return [os.path.realpath(doc)]

    return report


def names(paths):
    return [os.path.basename(path) for path in paths]


def test_a_later_run_that_supersedes_an_earlier_one_writes_the_kind_s_page_alone(repo):
    add_run(repo, "20261109T232328Z", "2026-11-09T23:23:30+00:00")
    add_run(repo, "aws-v0.11.1", "2026-11-22T10:00:00+00:00", report={"supersedes": "20261109T232328Z"})
    commit(repo)
    runs = regenerate.committed_runs(str(repo))
    current = regenerate.current_runs(runs, str(repo))
    assert names(current) == ["aws-v0.11.1"]
    owner = regenerate.regenerate(current, report=fake_report(repo))
    assert names(owner.values()) == ["aws-v0.11.1"]
    assert (repo / "docs" / "benchmarks" / "kafka-comparison.md").read_text() == "aws-v0.11.1"


def test_two_current_runs_writing_one_page_are_still_refused(repo):
    add_run(repo, "20261109T232328Z", "2026-11-09T23:23:30+00:00")
    add_run(repo, "aws-v0.11.1", "2026-11-22T10:00:00+00:00")
    commit(repo)
    current = regenerate.current_runs(regenerate.committed_runs(str(repo)), str(repo))
    assert len(current) == 2
    with pytest.raises(regenerate.Refused, match="two runs write the same file"):
        regenerate.regenerate(current, report=fake_report(repo))


def test_a_superseding_run_may_still_name_its_own_page(repo):
    add_run(repo, "20261109T232328Z", "2026-11-09T23:23:30+00:00")
    add_run(repo, "aws-v0.11.1", "2026-11-22T10:00:00+00:00",
            report={"supersedes": "20261109T232328Z", "doc": "docs/benchmarks/kafka-comparison-aws.md",
                    "img_dir": "docs/benchmarks/img/aws"})
    commit(repo)
    current = regenerate.current_runs(regenerate.committed_runs(str(repo)), str(repo))
    owner = regenerate.regenerate(current, report=fake_report(repo))
    assert names(owner) == ["kafka-comparison-aws.md"]


@pytest.mark.parametrize("target_kind, match", [("failover", "is a failover run, not a throughput run"),
                                                ("crash", "is a crash run, not a throughput run")])
def test_a_run_of_another_kind_cannot_be_superseded(repo, target_kind, match):
    add_run(repo, "earlier", "2026-11-09T23:23:30+00:00", kind=target_kind)
    add_run(repo, "later", "2026-11-22T10:00:00+00:00", report={"supersedes": "earlier"})
    commit(repo)
    with pytest.raises(regenerate.Refused, match=match):
        regenerate.current_runs(regenerate.committed_runs(str(repo)), str(repo))


def test_a_run_that_is_not_committed_in_the_same_directory_cannot_be_superseded(repo):
    add_run(repo, "later", "2026-11-22T10:00:00+00:00", report={"supersedes": "gone"})
    commit(repo)
    with pytest.raises(regenerate.Refused, match="supersedes 'gone', which is not a committed run"):
        regenerate.current_runs(regenerate.committed_runs(str(repo)), str(repo))


def test_only_an_earlier_run_can_be_superseded(repo):
    add_run(repo, "first", "2026-11-22T10:00:00+00:00", report={"supersedes": "second"})
    add_run(repo, "second", "2026-11-23T10:00:00+00:00")
    commit(repo)
    with pytest.raises(regenerate.Refused, match="started .* not before"):
        regenerate.current_runs(regenerate.committed_runs(str(repo)), str(repo))


def test_a_cycle_is_refused(repo):
    add_run(repo, "a", "2026-11-22T10:00:00+00:00", report={"supersedes": "b"})
    add_run(repo, "b", "2026-11-22T10:00:00+00:00", report={"supersedes": "a"})
    commit(repo)
    with pytest.raises(regenerate.Refused, match="cycle"):
        regenerate.current_runs(regenerate.committed_runs(str(repo)), str(repo))


def test_a_run_cannot_supersede_itself():
    with pytest.raises(run_outputs.OutputsRefused, match="itself"):
        run_outputs.check_supersedes("a", "a", "x/report.json")


def test_a_chain_leaves_only_its_newest_run(repo):
    add_run(repo, "one", "2026-11-01T00:00:00+00:00")
    add_run(repo, "two", "2026-11-02T00:00:00+00:00", report={"supersedes": "one"})
    add_run(repo, "three", "2026-11-03T00:00:00+00:00", report={"supersedes": "two"})
    commit(repo)
    assert names(regenerate.current_runs(regenerate.committed_runs(str(repo)), str(repo))) == ["three"]


def test_the_current_run_of_a_kind_is_the_one_not_superseded(repo):
    add_run(repo, "20261109T232328Z", "2026-11-09T23:23:30+00:00")
    add_run(repo, "failover-1", "2026-11-11T03:25:56+00:00", kind="failover")
    add_run(repo, "aws-v0.11.1", "2026-11-22T10:00:00+00:00", report={"supersedes": "20261109T232328Z"})
    commit(repo)
    assert regenerate.current_run("throughput", str(repo)) == f"{RESULTS}/aws-v0.11.1"
    assert regenerate.current_run("failover", str(repo)) == f"{RESULTS}/failover-1"


def test_two_current_runs_of_a_kind_leave_no_current_one(repo):
    add_run(repo, "20261109T232328Z", "2026-11-09T23:23:30+00:00")
    add_run(repo, "aws-v0.11.1", "2026-11-22T10:00:00+00:00", report={"doc": "docs/benchmarks/kafka-aws.md",
                                                                       "img_dir": "docs/benchmarks/img/aws"})
    commit(repo)
    with pytest.raises(regenerate.Refused, match="2 current throughput runs .*20261109T232328Z.*aws-v0.11.1"):
        regenerate.current_run("throughput", str(repo))
    with pytest.raises(regenerate.Refused, match="no current crash run"):
        regenerate.current_run("crash", str(repo))


def test_supersedes_alone_is_a_complete_report_json(tmp_path):
    (tmp_path / "report.json").write_text(json.dumps({"supersedes": "20261109T232328Z"}))
    own = run_outputs.resolve(str(tmp_path), "/repo", "docs/benchmarks/img", allow_supersedes=True)
    assert own == run_outputs.Outputs(None, None, None, "20261109T232328Z")


@pytest.mark.parametrize("content, reason", [
    ({"supersedes": "x", "doc": "docs/benchmarks/a.md"}, "both or neither"),
    ({"supersedes": "../x"}, "supersedes is '../x'"),
    ({"supersedes": 7}, "supersedes is 7"),
])
def test_a_malformed_supersedes_is_refused(tmp_path, content, reason):
    (tmp_path / "report.json").write_text(json.dumps(content))
    with pytest.raises(run_outputs.OutputsRefused, match=reason):
        run_outputs.resolve(str(tmp_path), "/repo", "docs/benchmarks/img", allow_supersedes=True)


def test_a_kind_that_does_not_take_supersedes_refuses_it(tmp_path):
    (tmp_path / "report.json").write_text(json.dumps({"supersedes": "x", "doc": "docs/benchmarks/a.md",
                                                      "img_dir": "docs/benchmarks/archive/img/a"}))
    with pytest.raises(run_outputs.OutputsRefused, match="supersedes"):
        run_outputs.resolve(str(tmp_path), "/repo", "docs/benchmarks/archive/img")


def test_this_repository_has_one_current_run_of_each_kind_the_readme_draws():
    # True of whatever runs are committed (review of #955: naming today's ids here would fail the day a run
    # supersedes one, as the ingest intends): every supersedes in the repository is valid, and each of the
    # README's two runs is a current, committed run of its kind.
    runs = regenerate.committed_runs()
    current = {os.path.basename(run): run for run in regenerate.current_runs(runs)}
    assert set(current) <= {os.path.basename(run) for run in runs}
    for resolve, kind in ((readme_charts.throughput_run, "throughput"), (readme_charts.failover_run, "failover")):
        rel = resolve()
        assert rel.startswith(f"{RESULTS}/") and os.path.basename(rel) in current
        assert run_outputs.run_kind(current[os.path.basename(rel)]) == kind


def test_the_readme_s_runs_follow_a_superseding_run(repo, monkeypatch):
    add_run(repo, "20261109T232328Z", "2026-11-09T23:23:30+00:00")
    add_run(repo, "failover-20261111T032549Z", "2026-11-11T03:25:56+00:00", kind="failover")
    add_run(repo, "aws-v0.11.1", "2026-11-22T10:00:00+00:00", report={"supersedes": "20261109T232328Z"})
    commit(repo)
    monkeypatch.setattr(readme_charts, "RUNS_REPO", str(repo))
    assert readme_charts.throughput_run() == f"{RESULTS}/aws-v0.11.1"
    assert readme_charts.failover_run() == f"{RESULTS}/failover-20261111T032549Z"
