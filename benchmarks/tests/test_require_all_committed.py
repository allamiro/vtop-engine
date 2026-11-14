"""A scenario that must commit every object fails the run when it does not (#663).

The shipped upload defaults once committed nothing on a policed uplink, and
the harness filed that as a measurement with exit 0. Scenario 24 asserts the
outcome instead: `require_all_committed: true` turns any failed batch, an
empty result, or (for whole-file input) a missing object into a failed run.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib import seed  # noqa: E402
from lib.scenario import load_scenario  # noqa: E402
from run_benchmark import unmet_commit_requirement  # noqa: E402

SCENARIOS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "scenarios")
POLICED = os.path.join(SCENARIOS, "24-policed-uplink-default-width.yaml")


def whole_file(**extra):
    return {"require_all_committed": True, "whole_file": True, "format": "jsonl", **extra}


def test_off_by_default_so_a_measured_failure_stays_a_result():
    # The fault scenarios measure failure; without the opt-in nothing changes.
    assert unmet_commit_requirement({}, success=0, failed=16, files_seeded=16) == ""


def test_every_object_committed_meets_it():
    assert unmet_commit_requirement(whole_file(), success=16, failed=0, files_seeded=16) == ""


def test_the_policed_uplink_before_663_fails_it():
    # What the #483 baseline recorded for the shipped defaults: 16 of 16 failed.
    why = unmet_commit_requirement(whole_file(), success=0, failed=16, files_seeded=16)
    assert "16 batch(es) failed" in why


def test_one_failed_batch_fails_it_even_when_the_rest_committed():
    why = unmet_commit_requirement(whole_file(), success=15, failed=1, files_seeded=16)
    assert "1 batch(es) failed" in why


def test_nothing_committed_and_nothing_failed_fails_it():
    assert unmet_commit_requirement(whole_file(), success=0, failed=0, files_seeded=16) == (
        "no object committed")


def test_a_whole_file_object_that_never_became_a_batch_fails_it():
    # A file the engine never turned into a batch is neither a success nor a
    # failure; for whole-file input the seeded count still exposes it.
    why = unmet_commit_requirement(whole_file(), success=15, failed=0, files_seeded=16)
    assert why == "15 of 16 seeded object(s) committed"


def test_line_input_is_held_only_to_what_its_batches_can_show():
    # Line formats pack many files into one batch, so fewer batches than files
    # is normal there and must not fail the run.
    sc = {"require_all_committed": True, "format": "jsonl"}
    assert unmet_commit_requirement(sc, success=3, failed=0, files_seeded=400) == ""
    assert "batch(es) failed" in unmet_commit_requirement(sc, success=3, failed=1,
                                                          files_seeded=400)


def test_binary_input_counts_as_whole_file():
    sc = {"require_all_committed": True, "format": "binary"}
    assert unmet_commit_requirement(sc, success=1, failed=0, files_seeded=2) != ""


def test_the_regression_scenario_asserts_the_outcome_at_the_shipped_defaults():
    sc = load_scenario(POLICED)
    assert sc.get("require_all_committed") is True
    assert sc.get("shaping_driver") == "netem"
    assert sc.get("shaping_policer_kbps") == 10000
    assert sc.get("shaping_latency_ms") == 100
    # No queue: a bottleneck is a buffer, and the policed link has none.
    assert not sc.get("shaping_bottleneck_kbps")
    # The shipped width and multipart settings are what is under test, so the
    # scenario must not tune them.
    assert sc.get("max_concurrent_batches") is None
    assert not sc.get("transports")
    assert sc.get("whole_file") is True
    assert sc.get("file_size") == 20 * 1024 * 1024
    assert sc.get("batch_max_bytes") > sc.get("file_size")


@pytest.mark.parametrize("size", [20 * 1024 * 1024, "20971520"])
def test_an_exact_file_size_is_honoured(size):
    assert seed._pick_size(size) == 20 * 1024 * 1024  # noqa: SLF001


@pytest.mark.parametrize("size", [0, -1])
def test_a_non_positive_exact_size_is_refused(size):
    with pytest.raises(ValueError, match="positive number of bytes"):
        seed._pick_size(size)  # noqa: SLF001


def test_a_bool_is_not_an_exact_size():
    # True is an int to Python; it must fall through to the size classes.
    lo, hi = seed.SIZE_CLASSES["small"]
    assert lo <= seed._pick_size(True) <= hi  # noqa: SLF001


def test_an_exact_size_seeds_files_of_that_size(tmp_path):
    totals = seed.generate_dataset(str(tmp_path), "jsonl", 2, 4096, seed=663)
    assert totals["files"] == 2
    sizes = sorted(os.path.getsize(tmp_path / name) for name in os.listdir(tmp_path))
    # A line format stops at the first line that reaches the size.
    assert all(4096 <= s < 4096 + 1024 for s in sizes), sizes
