"""Tests for scripts/check-advisory-review.py.

Each case the check exists to catch has a fixture here, and each test asserts
the message names the advisory id, so a check that stopped detecting a case (or
reported it under the wrong id) fails. Run from the repository root:

    python3 -m unittest discover -s scripts/tests -p 'test_check_advisory_review.py' -v
"""

from __future__ import annotations

import datetime
import importlib.util
import io
import pathlib
import sys
import tempfile
import unittest
import unittest.mock
from contextlib import redirect_stdout

SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "check-advisory-review.py"
REPO = SCRIPT.parent.parent

A = "RUSTSEC-2026-0001"
B = "RUSTSEC-2026-0002"
AUDIT_ONLY = "RUSTSEC-2026-0253"
REVIEWED = "2026-11-16"
DAY0 = datetime.date(2026, 11, 16)


def load():
    spec = importlib.util.spec_from_file_location("check_advisory_review", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def entry(i: str, date: str | None = REVIEWED) -> str:
    return f'    "{i}", # reviewed: {date}\n' if date is not None else f'    "{i}",\n'


def toml(entries: str) -> str:
    return (
        "# header\n\n[advisories]\nyanked = \"warn\"\nignore = [\n"
        "    # a comment line, and a blank line, are allowed between entries\n\n"
        + entries + "]\n\n[bans]\nmultiple-versions = \"warn\"\n"
    )


def row(i: str, date: str = REVIEWED) -> str:
    return f"| {i} | `crate 0.1.0` | why | {date} |\n"


def model(rows: str) -> str:
    return (
        "# Security model\n\n## 12. Container\n\ntext\n\n## 13. Supply-chain security\n\n"
        "Currently tracked:\n\n"
        "| Advisory | Crate | Why it is accepted for now | Last reviewed |\n"
        "|----------|-------|----------------------------|---------------|\n"
        + rows + "\n## 14. Next\n\n| Advisory | Other |\n|---|---|\n| RUSTSEC-2000-0001 | x |\n"
    )


class Fixture:
    """A repository root holding the three files the check reads."""

    def __init__(self, audit: str, deny: str, md: str):
        self.dir = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.dir.name)
        (self.root / ".cargo").mkdir()
        (self.root / "docs").mkdir()
        (self.root / ".cargo/audit.toml").write_text(audit)
        (self.root / "deny.toml").write_text(deny)
        (self.root / "docs/SECURITY_MODEL.md").write_text(md)

    def __enter__(self):
        return self.root

    def __exit__(self, *exc):
        self.dir.cleanup()


def consistent(date: str = REVIEWED):
    return (
        toml(entry(A, date) + entry(B, date) + entry(AUDIT_ONLY, date)),
        toml(entry(A, date) + entry(B, date)),
        model(row(A, date) + row(B, date) + row(AUDIT_ONLY, date)),
    )


class CheckTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load()

    def run_check(self, audit: str, deny: str, md: str, today: datetime.date = DAY0):
        with Fixture(audit, deny, md) as root:
            return self.mod.check_root(root, today)

    def assert_one_problem(self, result, *needles):
        self.assertEqual(len(result.problems), 1, result.problems)
        for n in needles:
            self.assertIn(n, result.problems[0])

    def test_consistent_files_pass(self):
        r = self.run_check(*consistent())
        self.assertEqual(r.problems, [])
        self.assertEqual(r.warnings, [])
        # Every id is reported with the days it has left.
        for i in (A, B, AUDIT_ONLY):
            self.assertTrue(any(i in line and "90 days left" in line for line in r.report), r.report)

    def test_an_id_without_a_date_fails(self):
        audit, deny, md = consistent()
        r = self.run_check(audit.replace(entry(A), entry(A, None)), deny, md)
        self.assert_one_problem(r, A, "audit.toml", "no review date")

    def test_a_trailing_comment_that_is_not_a_date_fails(self):
        audit, deny, md = consistent()
        r = self.run_check(audit, deny.replace(entry(B), f'    "{B}", # name constraints\n'), md)
        self.assert_one_problem(r, B, "deny.toml", "no review date")

    def test_a_malformed_date_fails(self):
        # "20261116" is accepted by date.fromisoformat on Python 3.11 and later.
        for bad in ("2026-13-01", "2026/11/16", "16-11-2026", "20261116", "soon", "2026-11-16 by me"):
            with self.subTest(date=bad):
                audit, deny, md = consistent()
                r = self.run_check(audit.replace(entry(A), entry(A, bad)), deny, md)
                self.assertTrue(r.problems, bad)
                self.assertTrue(any(A in p and "not a date" in p for p in r.problems), r.problems)

    def test_a_malformed_date_in_the_table_fails(self):
        audit, deny, md = consistent()
        r = self.run_check(audit, deny, md.replace(row(B), row(B, "Nov 2026")))
        self.assertTrue(any(B in p and "SECURITY_MODEL.md" in p and "not a date" in p for p in r.problems), r.problems)

    def test_a_date_in_the_future_fails(self):
        r = self.run_check(*consistent("2026-11-17"))
        self.assertEqual(len(r.problems), 3, r.problems)
        self.assertTrue(all("in the future" in p for p in r.problems), r.problems)

    def test_age_boundaries(self):
        # Decision F: warn from day 75, fail after day 90.
        cases = [(74, 0, 0), (75, 0, 3), (90, 0, 3), (91, 3, 0)]
        for days, problems, warnings in cases:
            with self.subTest(days=days):
                r = self.run_check(*consistent(), today=DAY0 + datetime.timedelta(days=days))
                self.assertEqual(len(r.problems), problems, r.problems)
                self.assertEqual(len(r.warnings), warnings, r.warnings)
                for p in r.problems:
                    self.assertIn(f"{days} days ago", p)
                for w in r.warnings:
                    self.assertIn(f"{90 - days} days left", w)

    def test_the_two_files_disagreeing_on_a_date_fails(self):
        audit, deny, md = consistent()
        r = self.run_check(audit, deny.replace(entry(A), entry(A, "2026-11-10")), md)
        self.assertTrue(any(A in p and "2026-11-10" in p and REVIEWED in p and "deny.toml" in p for p in r.problems), r.problems)

    def test_the_table_disagreeing_on_a_date_fails(self):
        audit, deny, md = consistent()
        r = self.run_check(audit, deny, md.replace(row(A), row(A, "2026-11-10")))
        self.assert_one_problem(r, A, "SECURITY_MODEL.md", "2026-11-10")

    def test_an_id_missing_from_the_table_fails(self):
        audit, deny, md = consistent()
        r = self.run_check(audit, deny, md.replace(row(B), ""))
        self.assert_one_problem(r, B, "no row")

    def test_a_table_row_without_an_ignore_fails(self):
        audit, deny, md = consistent()
        extra = "RUSTSEC-2026-0003"
        r = self.run_check(audit, deny, md.replace(row(A), row(A) + row(extra)))
        self.assert_one_problem(r, extra, "not ignored")

    def test_a_table_row_naming_two_ids_fails(self):
        audit, deny, md = consistent()
        r = self.run_check(audit, deny, md.replace(row(A), f"| {A} / {B} | `crate` | why | {REVIEWED} |\n"))
        self.assertTrue(any("one advisory per row" in p for p in r.problems), r.problems)

    def test_an_id_only_in_deny_fails(self):
        audit, deny, md = consistent()
        r = self.run_check(audit.replace(entry(B), ""), deny, md)
        self.assertTrue(any(B in p and "deny.toml" in p and "audit.toml" in p for p in r.problems), r.problems)

    def test_an_id_only_in_audit_fails_unless_it_is_audit_only(self):
        audit, deny, md = consistent()
        r = self.run_check(audit, deny.replace(entry(B), ""), md)
        self.assert_one_problem(r, B, "not in deny.toml")
        # The fixture's AUDIT_ONLY id is in audit.toml alone and passes (the
        # consistent case above), because the script names it.
        self.assertIn(AUDIT_ONLY, self.mod.AUDIT_ONLY)

    def test_an_audit_only_id_listed_in_deny_fails(self):
        audit, deny, md = consistent()
        r = self.run_check(audit, deny.replace(entry(B), entry(B) + entry(AUDIT_ONLY)), md)
        self.assert_one_problem(r, AUDIT_ONLY, "audit-only")

    def test_an_audit_only_name_with_no_ignore_left_fails(self):
        # The allow-list in the script must not outlive the ignore it excuses.
        audit, deny, md = consistent()
        r = self.run_check(audit.replace(entry(AUDIT_ONLY), ""), deny, md.replace(row(AUDIT_ONLY), ""))
        self.assert_one_problem(r, AUDIT_ONLY, "AUDIT_ONLY")

    def test_a_duplicated_id_fails(self):
        audit, deny, md = consistent()
        r = self.run_check(audit.replace(entry(A), entry(A) + entry(A)), deny, md)
        self.assert_one_problem(r, A, "more than once")

    def test_a_duplicated_table_row_fails(self):
        audit, deny, md = consistent()
        r = self.run_check(audit, deny, md.replace(row(A), row(A) + row(A)))
        self.assert_one_problem(r, A, "more than one row")

    def test_an_inline_table_entry_is_refused(self):
        audit, deny, md = consistent()
        inline = f'    {{ id = "{A}", reason = "x" }}, # reviewed: {REVIEWED}\n'
        with self.assertRaises(self.mod.ReviewError) as ctx:
            self.run_check(audit, deny.replace(entry(A), inline), md)
        self.assertIn("deny.toml", str(ctx.exception))

    def test_two_entries_on_one_line_are_refused(self):
        audit, deny, md = consistent()
        with self.assertRaises(self.mod.ReviewError):
            self.run_check(audit.replace(entry(A), f'    "{A}", "{B}", # reviewed: {REVIEWED}\n'), deny, md)

    def test_a_one_line_ignore_array_is_refused(self):
        audit, deny, md = consistent()
        one_line = '[advisories]\nignore = ["%s"]\n' % A
        with self.assertRaises(self.mod.ReviewError):
            self.run_check(audit, one_line, md)

    def test_a_file_without_an_ignore_array_is_refused(self):
        audit, deny, md = consistent()
        with self.assertRaises(self.mod.ReviewError):
            self.run_check(audit, "[advisories]\nyanked = \"warn\"\n", md)

    def test_a_table_without_the_last_reviewed_column_is_refused(self):
        audit, deny, md = consistent()
        with self.assertRaises(self.mod.ReviewError):
            self.run_check(audit, deny, md.replace("| Last reviewed |", "| Reviewed |"))

    def test_the_live_repository_files_are_consistent(self):
        # Ages depend on the day the test runs, so they are measured from the
        # newest date in the files; everything else must be clean.
        r = self.mod.check_root(REPO, None)
        newest = max(r.dates)
        r = self.mod.check_root(REPO, newest)
        self.assertEqual(r.problems, [])


class MainTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load()

    def main(self, files, *args):
        out = io.StringIO()
        with Fixture(*files) as root, redirect_stdout(out):
            code = self.mod.main(["--root", str(root), *args])
        return code, out.getvalue()

    def test_exit_0_when_clean(self):
        code, out = self.main(consistent(), "--today", "2026-11-20")
        self.assertEqual(code, 0, out)
        self.assertNotIn("::warning", out)

    def test_exit_0_with_annotations_when_a_review_is_due(self):
        code, out = self.main(consistent(), "--today", "2027-02-01")  # day 77
        self.assertEqual(code, 0, out)
        self.assertIn("::warning", out)
        self.assertIn("13 days left", out)

    def test_exit_1_when_a_review_is_overdue(self):
        code, out = self.main(consistent(), "--today", "2027-02-15")  # day 91
        self.assertEqual(code, 1, out)
        self.assertIn("::error", out)
        self.assertIn(A, out)

    def test_exit_2_when_a_file_is_missing(self):
        out = io.StringIO()
        with Fixture(*consistent()) as root, redirect_stdout(out):
            (root / "deny.toml").unlink()
            code = self.mod.main(["--root", str(root), "--today", "2026-11-20"])
        self.assertEqual(code, 2, out.getvalue())

    def test_a_malformed_today_is_a_usage_error(self):
        with self.assertRaises(SystemExit):
            with redirect_stdout(io.StringIO()), unittest.mock.patch("sys.stderr", io.StringIO()):
                self.mod.main(["--today", "tomorrow"])


if __name__ == "__main__":
    unittest.main()
