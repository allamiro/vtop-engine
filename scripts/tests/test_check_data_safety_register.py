"""Tests for scripts/check-data-safety-register.py.

Each failure the check exists to catch has a fixture here, and each test
asserts the message names the issue, so a check that stopped detecting a case
(or reported it under the wrong issue) fails. Run from the repository root:

    python3 -m unittest discover -s scripts/tests -p 'test_check_data_safety_register.py' -v
"""

from __future__ import annotations

import datetime
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "check-data-safety-register.py"
REAL_REGISTER = SCRIPT.parent.parent / "docs" / "reference" / "data-safety-register.md"


def load():
    spec = importlib.util.spec_from_file_location("check_data_safety_register", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    # dataclasses look their module up in sys.modules while the class is built.
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


HEADER = (
    "| Issue | Class | Component | Introduced | Found | Fixed | Found by | What happened |\n"
    "|---|---|---|---|---|---|---|---|\n"
)


def row(n: int, fixed: str = "#1 · v0.1.0 · 2026-01-02") -> str:
    url = f"https://github.com/allamiro/vtop-engine/issues/{n}"
    return f"| [#{n}]({url}) | lost range | engine | abc1234 | 2026-01-01 | {fixed} | review | x |\n"


def register(rows: str, considered: str = "") -> str:
    return (
        "# Data-safety register\n\nIntro.\n\n## Register\n\n" + HEADER + rows
        + "\n## Considered and not labelled\n\n| Issue | Why it is not labelled |\n|---|---|\n"
        + considered
    )


def issue(n: int, state: str = "CLOSED", created: str = "2026-01-01T10:00:00Z") -> dict:
    return {"number": n, "state": state, "createdAt": created, "title": f"issue {n}"}


class CheckTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load()

    def problems(self, text: str, issues: list[dict]) -> list[str]:
        return self.mod.check(self.mod.parse_register(text), issues)

    def test_a_consistent_register_passes(self):
        text = register(row(10) + row(12, "open"), "| [#11](u) | duplicates only |\n")
        self.assertEqual(self.problems(text, [issue(10), issue(12, "OPEN")]), [])

    def test_a_labelled_issue_without_a_row_fails(self):
        problems = self.problems(register(row(10)), [issue(10), issue(12)])
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("#12", problems[0])
        self.assertIn("no row", problems[0])

    def test_a_row_for_an_issue_without_the_label_fails(self):
        problems = self.problems(register(row(10) + row(12)), [issue(10)])
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("#12", problems[0])
        self.assertIn("does not carry the data-safety label", problems[0])

    def test_a_row_saying_open_for_a_closed_issue_fails(self):
        problems = self.problems(register(row(10, "open")), [issue(10, "CLOSED")])
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("#10", problems[0])
        self.assertIn("closed", problems[0])

    def test_a_row_with_a_fix_for_an_open_issue_fails(self):
        problems = self.problems(register(row(10)), [issue(10, "OPEN")])
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("#10", problems[0])
        self.assertIn("open", problems[0])

    def test_a_closed_issue_with_an_empty_fixed_cell_fails(self):
        for empty in ("", "—", "-"):
            with self.subTest(fixed=empty):
                problems = self.problems(register(row(10, empty)), [issue(10)])
                self.assertEqual(len(problems), 1, problems)
                self.assertIn("#10", problems[0])
                self.assertIn("empty", problems[0])

    def test_a_duplicate_row_fails(self):
        problems = self.problems(register(row(10) + row(10)), [issue(10)])
        self.assertTrue(any("#10" in p and "more than one row" in p for p in problems), problems)

    def test_rows_out_of_issue_order_fail(self):
        problems = self.problems(register(row(12) + row(10)), [issue(10), issue(12)])
        self.assertTrue(any("#10" in p and "order" in p for p in problems), problems)

    def test_a_considered_issue_that_carries_the_label_fails(self):
        text = register(row(10), "| [#11](u) | duplicates only |\n")
        problems = self.problems(text, [issue(10), issue(11)])
        self.assertTrue(any("#11" in p and "considered" in p for p in problems), problems)

    def test_a_changed_table_header_is_refused_rather_than_misread(self):
        text = register(row(10)).replace("| Found by |", "| Source |")
        with self.assertRaises(self.mod.RegisterError):
            self.mod.parse_register(text)

    def test_a_register_with_no_rows_is_refused(self):
        with self.assertRaises(self.mod.RegisterError):
            self.mod.parse_register(register(""))


class QuietDaysTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load()

    def test_counts_from_the_most_recently_opened_issue(self):
        issues = [issue(5, created="2026-11-11T02:16:18Z"), issue(9, created="2026-10-01T00:00:00Z")]
        days, latest = self.mod.quiet_days(issues, datetime.date(2026, 11, 14))
        self.assertEqual(days, 3)
        self.assertEqual(latest["number"], 5)


class CommandLineTests(unittest.TestCase):
    """The script end to end, reading issues from a file instead of `gh`."""

    @classmethod
    def setUpClass(cls):
        cls.mod = load()

    def run_main(self, text: str, issues: list[dict], *extra: str) -> tuple[int, str]:
        with tempfile.TemporaryDirectory() as d:
            reg = pathlib.Path(d, "register.md")
            reg.write_text(text, encoding="utf-8")
            js = pathlib.Path(d, "issues.json")
            js.write_text(json.dumps(issues), encoding="utf-8")
            out = io.StringIO()
            with redirect_stdout(out):
                code = self.mod.main(["--register", str(reg), "--issues-json", str(js), *extra])
            return code, out.getvalue()

    def test_exit_zero_and_quiet_days_on_a_consistent_register(self):
        code, out = self.run_main(
            register(row(10)), [issue(10, created="2026-11-11T00:00:00Z")],
            "--quiet-days", "--today", "2026-11-14",
        )
        self.assertEqual(code, 0, out)
        self.assertIn("3 days since #10", out)

    def test_exit_one_and_names_the_issue_on_a_missing_row(self):
        code, out = self.run_main(register(row(10)), [issue(10), issue(12)])
        self.assertEqual(code, 1, out)
        self.assertIn("#12", out)


class RealRegisterTests(unittest.TestCase):
    """The committed register parses, and its rows are in the shape the check expects."""

    def test_the_committed_register_parses(self):
        mod = load()
        parsed = mod.parse_register(REAL_REGISTER.read_text(encoding="utf-8"))
        self.assertGreater(len(parsed.rows), 0)
        numbers = [r.number for r in parsed.rows]
        self.assertEqual(numbers, sorted(set(numbers)))


if __name__ == "__main__":
    unittest.main()
