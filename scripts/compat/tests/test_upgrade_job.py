"""Tests for ci.yml's upgrade-compat job (#743): scenario 22 reads the newest
release's source from its tag (`git show "$tag:crates/vtop-meta/..."`), so a
job whose checkout is shallow must fetch that tag before the scenario runs.

The fetch is taken from the workflow and run against a shallow clone of a
throwaway repository, so the test fails if the line is dropped or no longer
brings in the tag's tree.

Run with: python3 -m unittest discover -s scripts/compat/tests -p 'test_upgrade_job.py'
"""

import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[3]
CI = ROOT / ".github/workflows/ci.yml"
SCENARIO = "22-meta-rolling-upgrade.sh"
SOURCE = "crates/vtop-meta/src/command.rs"
TAG = "v9.9.9"


def upgrade_job() -> dict:
    return yaml.safe_load(CI.read_text())["jobs"]["upgrade-compat"]


def scenario_index(steps: list) -> int:
    for i, step in enumerate(steps):
        if SCENARIO in (step.get("run") or ""):
            return i
    raise AssertionError(f"no step of upgrade-compat runs {SCENARIO}")


def tag_fetches(steps: list) -> list[str]:
    """The `git fetch` lines of the steps before scenario 22 that name a tag."""
    lines = []
    for step in steps[: scenario_index(steps)]:
        for line in (step.get("run") or "").splitlines():
            line = line.strip()
            if line.startswith("git fetch") and "refs/tags/" in line:
                lines.append(line)
    return lines


def git(cwd, *args, check=True) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_AUTHOR_NAME": "compat",
        "GIT_AUTHOR_EMAIL": "compat@example.invalid",
        "GIT_COMMITTER_NAME": "compat",
        "GIT_COMMITTER_EMAIL": "compat@example.invalid",
    }
    return subprocess.run(
        ["git", *args], cwd=cwd, env=env, check=check, capture_output=True, text=True
    )


class UpgradeJob(unittest.TestCase):
    def test_a_shallow_checkout_fetches_the_tag_before_scenario_22(self):
        steps = upgrade_job()["steps"]
        checkout = next(s for s in steps if str(s.get("uses", "")).startswith("actions/checkout@"))
        if str((checkout.get("with") or {}).get("fetch-depth", 1)) == "0":
            return  # a full clone has every tag
        self.assertTrue(
            tag_fetches(steps),
            f"the upgrade-compat checkout is shallow and no step before {SCENARIO} fetches "
            "the release's tag, which the scenario reads with `git show`",
        )

    @unittest.skipUnless(shutil.which("git"), "needs git")
    def test_the_fetch_brings_in_the_tags_tree_on_a_shallow_clone(self):
        lines = tag_fetches(upgrade_job()["steps"])
        if not lines:
            self.skipTest("the checkout is not shallow")
        with tempfile.TemporaryDirectory() as tmp:
            origin = pathlib.Path(tmp, "origin")
            (origin / SOURCE).parent.mkdir(parents=True)
            git(tmp, "init", "-q", "-b", "main", str(origin))
            (origin / SOURCE).write_text("pub const SUPPORTED_FEATURE_LEVEL: u32 = 7;\n")
            git(origin, "add", "-A")
            git(origin, "commit", "-q", "-m", "release")
            git(origin, "tag", "-a", TAG, "-m", TAG)
            (origin / SOURCE).write_text("pub const SUPPORTED_FEATURE_LEVEL: u32 = 9;\n")
            git(origin, "commit", "-q", "-am", "after the release")

            clone = pathlib.Path(tmp, "clone")
            git(tmp, "clone", "-q", "--depth=1", "--no-tags", origin.as_uri(), str(clone))
            self.assertNotEqual(git(clone, "show", f"{TAG}:{SOURCE}", check=False).returncode, 0)

            # Each line as the step runs it, with the step's `tag` set.
            for line in lines:
                subprocess.run(
                    ["bash", "-euo", "pipefail", "-c", line], cwd=clone, check=True,
                    capture_output=True,
                    env={**os.environ, "tag": TAG, "GIT_CONFIG_NOSYSTEM": "1",
                         "GIT_CONFIG_GLOBAL": os.devnull},
                )
            shown = git(clone, "show", f"{TAG}:{SOURCE}", check=False)
            self.assertEqual(shown.returncode, 0, shown.stderr)
            self.assertIn("= 7;", shown.stdout)


if __name__ == "__main__":
    unittest.main()
