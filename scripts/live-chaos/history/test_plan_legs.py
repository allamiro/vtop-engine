"""The chaos campaign's leg planner (#728): which runner labels get the hosted
6-hour job limit and which the self-hosted 5-day one (review of #904)."""

import os
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import plan_legs  # noqa: E402


class HostedOrSelfHosted(unittest.TestCase):
    def test_every_github_provided_label_is_hosted(self):
        for label in ("ubuntu-latest", "ubuntu-24.04", "ubuntu-22.04", "ubuntu-24.04-arm", "macos-15", "windows-2025"):
            self.assertTrue(plan_legs.is_github_hosted(label), label)

    def test_any_other_label_is_self_hosted(self):
        for label in ("self-hosted", "chaos-box", "linux-x64-big"):
            self.assertFalse(plan_legs.is_github_hosted(label), label)


class Plan(unittest.TestCase):
    def test_a_default_split_fits_a_hosted_runner(self):
        p = plan_legs.plan(24, 6, "ubuntu-latest")
        self.assertEqual(p["legs"], [1, 2, 3, 4, 5, 6])
        self.assertEqual(p["timeout_minutes"], 4 * 60 + 30)

    def test_a_versioned_hosted_image_gets_the_hosted_limit_not_the_self_hosted_one(self):
        # 24 h on one leg is 1,470 minutes: over the 360 a hosted job can run.
        for label in ("ubuntu-latest", "ubuntu-24.04", "ubuntu-24.04-arm"):
            with self.assertRaises(ValueError, msg=label) as raised:
                plan_legs.plan(24, 1, label)
            self.assertIn("GitHub-hosted", str(raised.exception))
            self.assertIn("raise legs to at least 5", str(raised.exception))

    def test_a_self_hosted_runner_may_run_a_long_leg(self):
        self.assertEqual(plan_legs.plan(24, 1, "chaos-box")["timeout_minutes"], 1470)

    def test_a_self_hosted_leg_has_its_own_limit(self):
        with self.assertRaises(ValueError) as raised:
            plan_legs.plan(130, 1, "chaos-box")  # 7,830 minutes
        self.assertIn("self-hosted", str(raised.exception))

    def test_a_leg_of_exactly_the_limit_is_allowed(self):
        self.assertEqual(plan_legs.plan(5.5, 1, "ubuntu-24.04")["timeout_minutes"], 360)

    def test_nonsense_is_refused(self):
        for hours, legs in ((0, 1), (-1, 1), (24, 0), (24, 33)):
            with self.assertRaises(ValueError, msg=(hours, legs)):
                plan_legs.plan(hours, legs, "ubuntu-latest")


class Cli(unittest.TestCase):
    script = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "plan_legs.py")

    def run_cli(self, **env):
        return subprocess.run(
            [sys.executable, self.script], env={**os.environ, **env}, capture_output=True, text=True, check=False
        )

    def test_it_writes_the_three_outputs(self):
        r = self.run_cli(HOURS="12", LEGS="3", RUNNER="ubuntu-24.04")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(r.stdout.splitlines(), ["legs=[1, 2, 3]", "hours_per_leg=4.0000", "timeout_minutes=270"])

    def test_a_refused_plan_exits_non_zero_with_the_reason(self):
        r = self.run_cli(HOURS="24", LEGS="1", RUNNER="ubuntu-24.04")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("raise legs to at least", r.stderr)


if __name__ == "__main__":
    unittest.main()
