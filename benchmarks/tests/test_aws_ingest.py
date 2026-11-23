"""The v0.11.0 AWS round's ingest, rehearsed (docs/benchmarks/aws-v0.11.0.md, benchmarks/README.md).

The round produces one results directory per benchmark: the Kafka throughput run (#596), the failover run
(#597), the crash run (#599), and the archive (#746), link (#747) and spool (#748) comparisons. The steps on the
day are: `cp -r` it under its kind's `results/`, commit it, run the regenerate scripts, commit what they wrote.
This test does exactly that in a scratch git repository holding a copy of the harness code, with a run of every
kind (the committed Kafka runs and the kinds' fixtures standing in for the AWS ones), and then checks the three
outcomes the page promises:

1. every kind's report path regenerates from a run placed under its committed results location, writes the
   page the page says it writes, and a second regeneration changes nothing (the CI drift check);
2. a second run of a kind that keeps the default paths is refused, and a `report.json` in it puts its page and
   images beside the first one's (the laptop's committed run stays; the AWS run is added);
3. a run that takes the default paths instead replaces the committed one's output in place.

The tests share one scratch repository and run in the order written: each starts from what the one before left.
It needs the pinned plotting stack (CI's `benchmark-charts` job), and skips without it.
"""
import os
import shutil
import subprocess
import sys

import pytest

pytest.importorskip("matplotlib")
pytest.importorskip("PIL")

from kafka_compare import regenerate as kr  # noqa: E402

try:
    kr.check_stack(kr.read_pins())
    kr.check_font()
except kr.Refused as exc:  # a developer's own environment; CI installs the pinned stack
    pytest.skip(f"the pinned plotting stack is not installed: {exc}", allow_module_level=True)

from test_kafka_compare_crash_report import synthetic_run  # noqa: E402

from archive_compare import fixture as archive_fixture  # noqa: E402
from link_compare import fixture as link_fixture  # noqa: E402
from spool_compare import fixture as spool_fixture  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.dirname(HERE)
REPO = os.path.dirname(BENCH)
COMMITTED = os.path.join(BENCH, "kafka_compare", "results")
THROUGHPUT = os.path.join(COMMITTED, "20261109T232328Z")
FAILOVER = os.path.join(COMMITTED, "failover-20261111T032549Z")

# What each kind's run writes by default, relative to the repository.
DEFAULT_PAGES = {
    "kafka": ["docs/benchmarks/kafka-comparison.md", "docs/benchmarks/kafka-failover.md",
              "docs/benchmarks/kafka-crash.md"],
    "compare": ["docs/benchmarks/archive-comparison.md", "docs/benchmarks/link-comparison.md",
                "docs/benchmarks/spool-comparison.md"],
}


def git(lab, *args):
    return subprocess.run(["git", *args], cwd=lab, check=True, capture_output=True, text=True).stdout


def commit(lab, message):
    git(lab, "add", "-A")
    git(lab, "commit", "-q", "-m", message)


def regenerate(lab):
    """Both regenerate scripts, as the drift check runs them; (exit code, output) of the first that fails."""
    for script in ("kafka_compare/regenerate.py", "compare_common/regenerate.py"):
        proc = subprocess.run([sys.executable, os.path.join(lab, "benchmarks", script)], cwd=lab,
                              capture_output=True, text=True)
        if proc.returncode != 0:
            return proc.returncode, proc.stderr
    return 0, ""


def drift(lab):
    return git(lab, "status", "--porcelain", "--", "docs/benchmarks/").splitlines()


@pytest.fixture(scope="module")
def lab(tmp_path_factory):
    """A scratch repository with the harness code (no tests, no committed runs) and an empty docs/benchmarks."""
    root = str(tmp_path_factory.mktemp("aws-ingest") / "repo")
    shutil.copytree(BENCH, os.path.join(root, "benchmarks"),
                    ignore=shutil.ignore_patterns("results", "tests", "__pycache__", "soak", ".pytest_cache"))
    os.makedirs(os.path.join(root, "docs", "benchmarks"))
    with open(os.path.join(root, "docs", "benchmarks", ".gitkeep"), "w"):
        pass
    git(root, "init", "-q", "-b", "main")
    git(root, "config", "user.name", "ingest test")
    git(root, "config", "user.email", "ingest@example.invalid")
    commit(root, "harness code")
    return root


def place(lab, kind, source, run_id):
    """`cp -r <source> benchmarks/<kind>_compare/results/<run-id>`."""
    destination = os.path.join(lab, "benchmarks", f"{kind}_compare", "results", run_id)
    shutil.copytree(source, destination)
    return destination


def test_the_round_s_results_regenerate_where_they_are_placed_and_stay_drift_free(lab, tmp_path):
    runs = tmp_path / "round"
    place(lab, "kafka", THROUGHPUT, "v0110-596-c6i4x")
    place(lab, "kafka", FAILOVER, "failover-v0110-c6i4x")
    place(lab, "kafka", synthetic_run(runs / "crash-v0110-c6i4x"), "crash-v0110-c6i4x")
    place(lab, "archive", archive_fixture.write_run(str(runs / "a"), "archive-v0110"), "archive-v0110")
    place(lab, "link", link_fixture.write_run(str(runs / "l"), "link-full-v0110"), "link-full-v0110")
    place(lab, "spool", spool_fixture.write_run(str(runs / "s"), "spool-full-v0110"), "spool-full-v0110")
    commit(lab, "the AWS round's results")
    code, err = regenerate(lab)
    assert code == 0, err
    written = {line[3:] for line in drift(lab)}
    for page in DEFAULT_PAGES["kafka"] + DEFAULT_PAGES["compare"]:
        assert page in written or any(w.startswith(page) for w in written), (page, sorted(written))
    commit(lab, "the generated reports")
    assert regenerate(lab) == (0, "")
    assert drift(lab) == []


def test_a_second_run_of_a_kind_is_refused_until_it_has_its_own_paths(lab, tmp_path):
    # The laptop's throughput run stays in the repository as a committed run (the README quotes it); the AWS
    # run is added beside it. Without a report.json both write docs/benchmarks/kafka-comparison.md.
    second = place(lab, "kafka", THROUGHPUT, "v0110-596-c6i8x")
    commit(lab, "a second throughput run")
    code, err = regenerate(lab)
    assert code == 2 and "two runs write the same file" in err
    with open(os.path.join(second, "report.json"), "w") as handle:
        handle.write('{"doc": "docs/benchmarks/kafka-comparison-aws-v0.11.0.md", '
                     '"img_dir": "docs/benchmarks/img/aws-v0.11.0"}\n')
    commit(lab, "the second run's own paths")
    code, err = regenerate(lab)
    assert code == 0, err
    new = {line[3:] for line in drift(lab)}
    assert "docs/benchmarks/kafka-comparison-aws-v0.11.0.md" in new
    assert "docs/benchmarks/img/aws-v0.11.0/" in new or any(n.startswith("docs/benchmarks/img/aws-v0.11.0/")
                                                             for n in new)
    commit(lab, "the second run's reports")
    assert regenerate(lab) == (0, "") and drift(lab) == []
    # The first run's output is untouched by the second's.
    assert os.path.isfile(os.path.join(lab, "docs", "benchmarks", "kafka-comparison.md"))


def test_the_same_holds_for_a_comparison_kind_and_a_left_behind_page_is_caught(lab):
    second = place(lab, "archive", os.path.join(lab, "benchmarks", "archive_compare", "results", "archive-v0110"),
                   "archive-time-kills-v0110")
    commit(lab, "a second archive run")
    code, err = regenerate(lab)
    assert code == 2 and "two runs write the same file" in err
    with open(os.path.join(second, "report.json"), "w") as handle:
        handle.write('{"doc": "docs/benchmarks/archive-comparison-time-kills.md", '
                     '"img_dir": "docs/benchmarks/archive/img/time-kills"}\n')
    commit(lab, "its own paths")
    code, err = regenerate(lab)
    assert code == 0, err
    commit(lab, "its reports")
    assert regenerate(lab) == (0, "") and drift(lab) == []
    # Deleting the run and leaving its page behind is caught (in a copy, so the lab stays as it is): the page
    # carries the generated mark and the images sit under the kind's directory, which the check watches.
    stale = lab + "-stale"
    shutil.copytree(lab, stale)
    git(stale, "rm", "-r", "-q", "--", os.path.relpath(second, lab))
    commit(stale, "the run is removed, its output is not")
    code, err = regenerate(stale)
    assert code == 2 and "committed output that no run writes" in err
    assert "archive-comparison-time-kills.md" in err and "archive/img/time-kills" in err


def test_a_run_that_keeps_the_default_paths_replaces_the_committed_one_in_place(lab):
    # The README's other route: retire the committed run and add the new one under the default names.
    git(lab, "rm", "-r", "-q", "--", "benchmarks/kafka_compare/results/v0110-596-c6i4x")
    place(lab, "kafka", THROUGHPUT, "v0110-596-replacement")
    commit(lab, "replace the first throughput run")
    code, err = regenerate(lab)
    assert code == 0, err
    assert os.path.isfile(os.path.join(lab, "docs", "benchmarks", "kafka-comparison.md"))
    commit(lab, "its reports, if they changed") if drift(lab) else None
    assert regenerate(lab) == (0, "") and drift(lab) == []
