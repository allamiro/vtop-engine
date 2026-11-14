"""The `benchmark-charts` CI job: what it runs, on which changes, with which stack (#600).

The drift check and the chart tests are only worth something if CI runs them.
Each test here reads `.github/workflows/ci.yml` as text (PyYAML is optional
in the harness, and the benchmarks job also runs without it) and fails on one
way the job could stop covering the charts while staying green:

- a test file that needs matplotlib or Pillow skips in the generic
  `benchmarks` job, which installs neither, so it runs only if the chart job
  names it (the failover render tests skipped everywhere until #600);
- a change to a chart test alone must start the job;
- the stack is the hash-locked one, on one Python patch version.
"""
import glob
import os
import re

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.dirname(HERE)
REPO = os.path.dirname(BENCH)
CI = os.path.join(REPO, ".github", "workflows", "ci.yml")
PINS = os.path.join(BENCH, "kafka_compare", "requirements-report.txt")


def ci_text():
    with open(CI, encoding="utf-8") as handle:
        return handle.read()


def job_block(name):
    """The lines of one job under `jobs:`, from its key to the next two-space key."""
    text = ci_text()
    match = re.search(rf"^  {re.escape(name)}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)", text, re.S | re.M)
    assert match, f"ci.yml has no job {name!r}"
    return match.group(1)


def filter_block(name):
    """The path globs of one dorny/paths-filter entry (`<name>:` then `- '<glob>'` lines)."""
    text = ci_text()
    match = re.search(rf"^( +){re.escape(name)}:\n((?:\1  (?:- .*|#.*)\n)+)", text, re.M)
    assert match, f"ci.yml has no paths filter {name!r}"
    return re.findall(r"- '([^']+)'", match.group(2))


def needs_plotting_stack(path):
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    return bool(re.search(r"importorskip\(\s*[\"'](?:matplotlib|PIL)", text)
                or re.search(r"^\s*(?:import|from)\s+(?:matplotlib|PIL)\b", text, re.M))


def chart_job_test_files():
    block = job_block("benchmark-charts")
    files = set()
    for line in re.findall(r"python -m pytest ([^\n]+)", block):
        files.update(word for word in line.split() if word.startswith("tests/"))
    return files


def test_every_test_file_that_needs_the_plotting_stack_runs_in_the_chart_job():
    needing = {os.path.relpath(path, BENCH) for path in sorted(glob.glob(os.path.join(HERE, "test_*.py")))
               if needs_plotting_stack(path)}
    assert needing, "no test file needs matplotlib or Pillow: this check has nothing to check"
    missing = sorted(needing - chart_job_test_files())
    assert not missing, (
        f"these test files need matplotlib or Pillow, which only the benchmark-charts job installs, "
        f"but that job does not run them, so their tests skip everywhere: {missing}")


def test_the_chart_job_runs_only_test_files_that_exist():
    gone = sorted(path for path in chart_job_test_files() if not os.path.isfile(os.path.join(BENCH, path)))
    assert not gone, f"the benchmark-charts job names test files that do not exist: {gone}"


def _covered(path, globs):
    def match(pattern):
        regex = re.escape(pattern).replace(r"\*\*", ".*").replace(r"\*", "[^/]*")
        return re.fullmatch(regex, path) is not None
    return any(match(pattern) for pattern in globs)


def test_a_change_to_any_chart_test_starts_the_chart_job():
    globs = filter_block("benchmark_charts")
    uncovered = sorted(path for path in chart_job_test_files()
                       if not _covered(os.path.join("benchmarks", path), globs))
    assert not uncovered, f"the benchmark_charts paths filter misses these test files: {uncovered}"
    for path in ("benchmarks/tests/test_kafka_compare_some_new_test.py", "docs/COMPARISON.md"):
        assert _covered(path, globs), f"the benchmark_charts paths filter misses {path}"


def test_the_chart_job_pins_the_python_patch_version():
    versions = re.findall(r'python-version:\s*"([^"]+)"', job_block("benchmark-charts"))
    assert versions, "the benchmark-charts job sets no python-version"
    for version in versions:
        assert re.fullmatch(r"\d+\.\d+\.\d+", version), (
            f"the benchmark-charts job asks for Python {version!r}: a floating minor version moves "
            "with the runner image, so pin the patch")


def test_the_chart_job_installs_everything_with_hash_checking():
    # The test tools too: pytest's own dependencies are otherwise whatever is newest at run time.
    block = job_block("benchmark-charts")
    install = [line.strip() for line in block.splitlines() if "pip install" in line and "echo" not in line]
    assert any("requirements-report.txt" in line for line in install), \
        "the benchmark-charts job does not install requirements-report.txt"
    for line in install:
        assert "--require-hashes" in line, f"installed without --require-hashes: {line}"
        assert re.search(r"-r \S+\.txt", line) and not re.search(r"\s[A-Za-z][\w.-]*==", line), \
            f"installs a package by name, outside a hash-locked file: {line}"


def test_the_chart_job_fails_when_a_test_skips_for_want_of_the_plotting_stack():
    # The file list above is read as text; this is the run-time half: a test that skipped because
    # matplotlib or Pillow was missing fails the job, however the skip was written.
    block = job_block("benchmark-charts")
    assert re.search(r"python -m pytest [^\n]*-rs", block), "the chart job's pytest does not report skips (-rs)"
    assert re.search(r"grep -E? ?[\"'][^\"']*SKIPPED[^\"']*matplotlib", block), \
        "the chart job does not fail on a skip that names matplotlib"


TEST_PINS = os.path.join(BENCH, "kafka_compare", "requirements-report-test.txt")


@pytest.mark.parametrize("path", [PINS, TEST_PINS], ids=os.path.basename)
def test_every_lock_the_chart_job_installs_is_hash_locked(path):
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    entries = re.findall(r"^([A-Za-z0-9_.-]+)==([^\s\\]+)((?:\s*\\\n\s+--hash=sha256:[0-9a-f]{64})*)", text, re.M)
    assert entries, f"{path} pins nothing"
    unhashed = sorted(name for name, _version, hashes in entries if "--hash=sha256:" not in hashes)
    assert not unhashed, f"{path}: these pins carry no hash, so pip cannot check what it installs: {unhashed}"


def test_the_test_tools_lock_agrees_with_the_plotting_stack():
    # Both files go into one pip command; a package in both must be the same version.
    def pins(path):
        with open(path, encoding="utf-8") as handle:
            return dict(re.findall(r"^([A-Za-z0-9_.-]+)==([^\s\\;]+)", handle.read(), re.M))
    report, tools = pins(PINS), pins(TEST_PINS)
    assert "pytest" in tools
    clash = {name: (report[name], tools[name]) for name in set(report) & set(tools) if report[name] != tools[name]}
    assert not clash, f"the two locks pin these packages differently: {clash}"


