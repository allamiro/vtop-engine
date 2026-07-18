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
