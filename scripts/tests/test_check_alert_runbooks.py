"""Unit tests for scripts/check-alert-runbooks.py (#720).

Each test copies the fixtures in fixtures/alert-runbooks/, breaks one thing,
and asserts the checker fails and names the alert. Run with:

    python3 -m unittest discover -s scripts/tests -p 'test_check_alert_runbooks.py'
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import shutil
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures" / "alert-runbooks"
SCRIPT = HERE.parent / "check-alert-runbooks.py"

_spec = importlib.util.spec_from_file_location("check_alert_runbooks", SCRIPT)
checker = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(checker)

class CheckerCase(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir)
        for name in ("rules.yaml", "tests.yaml", "runbooks.md"):
            shutil.copy(FIXTURES / name, self.dir / name)

    def edit(self, name: str, old: str, new: str) -> None:
        path = self.dir / name
        text = path.read_text()
        self.assertIn(old, text, f"fixture {name} no longer contains {old!r}")
        path.write_text(text.replace(old, new))

    def run_checker(self, *extra: str) -> tuple[int, str]:
        args = [
            "--rules", str(self.dir / "rules.yaml"),
            "--tests", str(self.dir / "tests.yaml"),
            "--runbooks", str(self.dir / "runbooks.md"),
            *extra,
        ]
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = checker.main(args)
        return code, out.getvalue() + err.getvalue()

    def assert_fails(self, *needles: str, extra: tuple[str, ...] = ()) -> None:
        code, output = self.run_checker(*extra)
        self.assertEqual(code, 1, f"expected a failure, got:\n{output}")
        for needle in needles:
            self.assertIn(needle, output)


class TheFixturesPass(CheckerCase):
    def test_the_unbroken_fixtures_pass(self) -> None:
        code, output = self.run_checker()
        self.assertEqual(code, 0, output)
        self.assertIn("2 alerts", output)



class RunbookSections(CheckerCase):
    def test_an_alert_with_no_runbook_section_fails_and_names_it(self) -> None:
        self.edit("runbooks.md", "## VtopFixtureTwo\n", "## Something else\n")
        self.assert_fails("alert VtopFixtureTwo has no section", "#vtopfixturetwo, which is not a heading")

    def test_a_link_to_a_missing_heading_fails_and_names_it(self) -> None:
        self.edit("rules.yaml", "#vtopfixtureone", "#vtopfixtureon")
        self.assert_fails("alert VtopFixtureOne's runbook_url points at #vtopfixtureon, which is not a heading")

    def test_a_link_to_another_alerts_section_fails(self) -> None:
        self.edit("rules.yaml", "#vtopfixtureone", "#vtopfixturetwo")
        self.assert_fails("alert VtopFixtureOne's runbook_url points at the section 'VtopFixtureTwo'")

    def test_a_link_with_no_fragment_fails(self) -> None:
        self.edit("rules.yaml", "runbooks/#vtopfixtureone", "runbooks/")
        self.assert_fails("alert VtopFixtureOne's runbook_url", "names no section")

    def test_a_link_to_another_page_fails_for_the_rules_file(self) -> None:
        self.edit("rules.yaml", "operate/runbooks/#vtopfixtureone", "operate/monitoring/#vtopfixtureone")
        self.assert_fails("alert VtopFixtureOne links its runbook at https://allamiro.github.io/vtop-engine/operate/monitoring/")

    def test_a_section_for_an_alert_that_does_not_exist_fails(self) -> None:
        self.edit("runbooks.md", "## Related pages\n", "## VtopRetired\n\n## Related pages\n")
        self.assert_fails("section 'VtopRetired' is for an alert that does not exist")

    def test_a_heading_inside_a_code_fence_is_not_a_section(self) -> None:
        # The fixture's fenced "## VtopInsideAFence" would otherwise be reported.
        code, output = self.run_checker()
        self.assertEqual(code, 0, output)
        self.assertNotIn("VtopInsideAFence", output)

    def test_a_repeated_alert_anchor_fails(self) -> None:
        self.edit("runbooks.md", "## Related pages\n", "## VtopFixtureOne\n")
        self.assert_fails("heading 'VtopFixtureOne' repeats the anchor #vtopfixtureone")


class UnitTests(CheckerCase):
    def test_an_alert_with_no_unit_test_fails_and_names_it(self) -> None:
        self.edit("tests.yaml", "alertname: VtopFixtureTwo", "alertname: VtopFixtureOne")
        self.assert_fails(
            "alert VtopFixtureTwo has no unit test that expects it to fire",
            "alert VtopFixtureTwo has no unit test that expects it to stay quiet",
        )

    def test_an_alert_with_only_a_firing_test_fails(self) -> None:
        self.edit("tests.yaml", "        exp_alerts: []\n  - name: two", "        exp_alerts:\n          - exp_labels: {severity: page}\n  - name: two")
        self.assert_fails("alert VtopFixtureOne has no unit test that expects it to stay quiet")

    def test_a_test_for_an_alert_that_does_not_exist_fails(self) -> None:
        self.edit("tests.yaml", "  - name: one is quiet\n    alert_rule_test:\n      - alertname: VtopFixtureOne",
                  "  - name: one is quiet\n    alert_rule_test:\n      - alertname: VtopGone\n        eval_time: 1m\n      - alertname: VtopFixtureOne")
        self.assert_fails("checks alert VtopGone, which does not exist")


class RuleShape(CheckerCase):
    def test_an_unknown_severity_fails(self) -> None:
        self.edit("rules.yaml", "severity: ticket", "severity: warning")
        self.assert_fails("alert VtopFixtureTwo has severity 'warning'")

    def test_a_missing_description_fails(self) -> None:
        self.edit("rules.yaml", '          description: "Do the second thing."\n', "")
        self.assert_fails("alert VtopFixtureTwo has no description annotation")


class Slugs(unittest.TestCase):
    """The GitHub rules that pymdownx.slugs.slugify(case="lower") applies."""

    CASES = {
        "VtopNoCommits": "vtopnocommits",
        "What it means": "what-it-means",
        "Object Lock (S3) checks": "object-lock-s3-checks",
        "vtop_commits_total stalls": "vtop_commits_total-stalls",
        "A <code>tag</code> here": "a-tag-here",
    }

    def test_slugs_follow_the_github_rules(self) -> None:
        for text, slug in self.CASES.items():
            self.assertEqual(checker.slugify(text), slug, text)

    def test_slugs_match_pymdownx_when_it_is_installed(self) -> None:
        try:
            from pymdownx.slugs import slugify
        except ImportError:
            self.skipTest("pymdownx is not installed here (the docs build has it)")
        reference = slugify(case="lower")
        for text in self.CASES:
            self.assertEqual(checker.slugify(text), reference(text, "-"), text)


class TheShippedRules(unittest.TestCase):
    def test_the_repository_rules_runbooks_and_tests_pass(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = checker.main([])
        self.assertEqual(code, 0, err.getvalue())


if __name__ == "__main__":
    unittest.main()
