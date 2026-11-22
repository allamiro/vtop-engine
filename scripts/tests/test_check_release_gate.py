"""Tests for scripts/check-release-gate.py.

Each test starts from a minimal repository whose gate page passes, breaks one
thing, and asserts the check fails naming it. A final test runs the check on
the real repository. Run from the repository root:

    python3 -m unittest discover -s scripts/tests -p 'test_check_release_gate.py' -v
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import io
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "check-release-gate.py"
REPO = SCRIPT.parent.parent


def load():
    spec = importlib.util.spec_from_file_location("check_release_gate", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


MOD = load()
TODAY = dt.date(2026, 11, 22)
RUN = "https://github.com/allamiro/vtop-engine/actions/runs/"

SOURCES = {
    "crates/a/tests/t.rs": (
        "#[test]\nfn holds() {}\n\n"
        "#[test]\n#[ignore]\nfn slow() {}\n"
    ),
    "scripts/x.py": "def test_one():\n    pass\n",
    ".github/workflows/ci.yml": (
        "name: CI\non: push\njobs:\n"
        "  build:\n    runs-on: ubuntu-latest\n"
        "  live-chaos: # the scenarios\n    runs-on: ubuntu-latest\n"
        "env:\n  later: 1\n"
    ),
    "docs/page.md": "# Page\n",
}


def page(rows: dict[str, tuple[str, str, str]] | None = None, verdict: dict[str, str] | None = None,
         checked: str = "2026-11-22") -> str:
    """A gate page: every criterion met with `docs/page.md` as evidence, unless overridden."""
    rows = rows or {}
    body = []
    for cid in MOD.CRITERIA:
        status, evidence, open_ = rows.get(cid, ("met", "`docs/page.md`", "none"))
        body.append(f"| {cid} | the criterion | {status} | {evidence} | {open_} | {checked} |")
    statuses = [rows.get(c, ("met",))[0] for c in MOD.CRITERIA]
    rc = all(s == "met" for c, s in zip(MOD.CRITERIA, statuses) if c != "A1")
    final = all(s == "met" for s in statuses)
    v = {
        "Evaluated": "2026-11-22",
        "Met": str(statuses.count("met")),
        "Partial": str(statuses.count("partial")),
        "Not met": str(statuses.count("not met")),
        "1.0.0-rc.1 may be cut": "yes" if rc else "no",
        "1.0.0 may be cut": "yes" if final else "no",
    }
    v.update(verdict or {})
    vrows = "\n".join(f"| {k} | {val} |" for k, val in v.items())
    return (
        "# Gate\n\n<!-- verdict:begin -->\n\n| Item | Value |\n|---|---|\n" + vrows + "\n\n<!-- verdict:end -->\n\n"
        "<!-- criteria:begin -->\n\n| ID | Criterion | Status | Evidence | Open | Checked |\n|---|---|---|---|---|---|\n"
        + "\n".join(body) + "\n\n<!-- criteria:end -->\n"
    )


class Fake:
    """A stand-in for the GitHub API: dict of path -> JSON (None is a 404)."""

    def __init__(self, data: dict | None = None):
        self.data = data or {}
        self.calls: list[str] = []

    def __call__(self, path: str):
        self.calls.append(path)
        return self.data.get(path)


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        for rel, text in SOURCES.items():
            f = self.root / rel
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_text(text)
        self.git("init", "-q")
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "feat: the first change (#5)")
        self.git("commit", "-q", "--allow-empty", "-m", "fix: the second (#7) and more (#8)")
        self.git("commit", "-q", "--allow-empty", "-m", "docs: refers to #9 but is not its merge")
        self.doc = self.root / "docs/reference/release-gate.md"
        self.doc.parent.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self.tmp.cleanup()

    def git(self, *args):
        subprocess.run(["git", "-C", str(self.root), "-c", "user.name=t", "-c", "user.email=t@example.com",
                        "-c", "commit.gpgsign=false", *args], check=True, capture_output=True)

    def run_check(self, text: str, online: Fake | None = None) -> tuple[int, str]:
        self.doc.write_text(text)
        argv = ["--doc", str(self.doc), "--root", str(self.root), "--today", TODAY.isoformat()]
        if online is not None:
            argv.append("--online")
        out = io.StringIO()
        with redirect_stdout(out):
            code = MOD.main(argv, fetch=online)
        return code, out.getvalue()

    def assertFails(self, text: str, needle: str, online: Fake | None = None):
        code, out = self.run_check(text, online)
        self.assertEqual(code, 1, out)
        self.assertIn(needle, out)


class Passing(Base):
    def test_the_minimal_page_passes(self):
        code, out = self.run_check(page())
        self.assertEqual(code, 0, out)
        self.assertIn("30 criteria", out)

    def test_every_kind_of_reference_resolves(self):
        ev = ("`crates/a/tests/t.rs::holds`; `scripts/x.py::test_one`; `.github/workflows/ci.yml::live-chaos`; "
              "`crates/a/tests`; #5 and #8")
        code, out = self.run_check(page({"A2": ("met", ev, "none")}))
        self.assertEqual(code, 0, out)

    def test_rc_may_be_cut_when_only_a1_is_open(self):
        text = page({"A1": ("not met", "`docs/page.md`", "#741")})
        self.assertIn("| 1.0.0-rc.1 may be cut | yes |", text)
        code, out = self.run_check(text)
        self.assertEqual(code, 0, out)


class Structure(Base):
    def test_a_missing_criterion_fails(self):
        text = page().replace("| B3 | the criterion | met | `docs/page.md` | none | 2026-11-22 |\n", "")
        self.assertFails(text, "missing ['B3']")

    def test_an_unknown_criterion_fails(self):
        text = page().replace("| H2 |", "| H3 |")
        self.assertFails(text, "not a criterion of #714: ['H3']")

    def test_criteria_out_of_order_fail(self):
        a = "| A1 | the criterion | met | `docs/page.md` | none | 2026-11-22 |"
        b = "| A2 | the criterion | met | `docs/page.md` | none | 2026-11-22 |"
        text = page().replace(a, "@@").replace(b, a).replace("@@", b)
        self.assertFails(text, "out of order")

    def test_a_missing_marker_cannot_run(self):
        code, out = self.run_check(page().replace("<!-- criteria:end -->", ""))
        self.assertEqual(code, 2, out)

    def test_a_short_row_cannot_run(self):
        code, out = self.run_check(page().replace("| none | 2026-11-22 |", "| 2026-11-22 |", 1))
        self.assertEqual(code, 2, out)


class Statuses(Base):
    def test_an_unknown_status_fails(self):
        self.assertFails(page({"A3": ("done", "`docs/page.md`", "none")}), "status 'done'")

    def test_a_met_row_with_an_open_issue_fails(self):
        self.assertFails(page({"A3": ("met", "`docs/page.md`", "#726")}), "a met row names open issues")

    def test_a_row_not_met_without_an_issue_fails(self):
        self.assertFails(page({"A3": ("not met", "nothing yet", "none")}), "must name the issue")

    def test_an_open_cell_that_is_not_issues_fails(self):
        self.assertFails(page({"A3": ("partial", "`docs/page.md`", "soon")}), "Open must be")

    def test_a_met_row_without_evidence_fails(self):
        self.assertFails(page({"A3": ("met", "it works", "none")}), "names no evidence")

    def test_a_partial_row_without_evidence_fails(self):
        self.assertFails(page({"A3": ("partial", "some of it", "#726")}), "names no evidence")

    def test_a_row_not_met_may_name_no_evidence(self):
        code, out = self.run_check(page({"A3": ("not met", "nothing has run", "#726")}))
        self.assertEqual(code, 0, out)


class Evidence(Base):
    def test_a_missing_file_fails(self):
        self.assertFails(page({"A2": ("met", "`scripts/gone.sh`", "none")}), "`scripts/gone.sh` does not exist")

    def test_a_missing_rust_test_fails(self):
        self.assertFails(page({"A2": ("met", "`crates/a/tests/t.rs::renamed`", "none")}), "no test function `renamed`")

    def test_an_ignored_rust_test_fails(self):
        self.assertFails(page({"A2": ("met", "`crates/a/tests/t.rs::slow`", "none")}), "#[ignore]d")

    def test_a_missing_python_test_fails(self):
        self.assertFails(page({"A2": ("met", "`scripts/x.py::test_two`", "none")}), "no test function `test_two`")

    def test_a_missing_workflow_job_fails(self):
        self.assertFails(page({"A2": ("met", "`.github/workflows/ci.yml::soak`", "none")}), "has no job `soak`")

    def test_a_key_outside_jobs_is_not_a_job(self):
        self.assertFails(page({"A2": ("met", "`.github/workflows/ci.yml::later`", "none")}), "has no job `later`")

    def test_a_name_inside_a_directory_fails(self):
        self.assertFails(page({"A2": ("met", "`crates/a/tests::holds`", "none")}), "is a directory")

    def test_a_pull_request_not_merged_fails(self):
        self.assertFails(page({"A2": ("met", "#6", "none")}), "#6 is not a pull request merged")

    def test_a_mention_in_a_subject_is_not_a_merge(self):
        self.assertFails(page({"A2": ("met", "#9", "none")}), "#9 is not a pull request merged")

    def test_inline_code_and_run_links_are_not_pull_requests(self):
        ev = f"`docs/page.md` (`#6` is a fixture); [run]({RUN}6)"
        code, out = self.run_check(page({"A2": ("met", ev, "none")}))
        self.assertEqual(code, 0, out)


class Dates(Base):
    def test_a_checked_date_that_is_not_a_date_fails(self):
        self.assertFails(page(checked="2026-13-01"), "is not a YYYY-MM-DD date")

    def test_a_checked_date_after_evaluated_fails(self):
        self.assertFails(page(verdict={"Evaluated": "2026-11-20"}), "after the page's Evaluated date")

    def test_a_checked_date_before_714_fails(self):
        self.assertFails(page(checked="2026-11-13", verdict={"Evaluated": "2026-11-13"}), "before #714")

    def test_evaluated_in_the_future_fails(self):
        self.assertFails(page(verdict={"Evaluated": "2026-11-23"}), "is in the future")


class Verdict(Base):
    def test_a_wrong_count_fails(self):
        self.assertFails(page(verdict={"Met": "29"}), "the verdict says Met 29, the rows have 30")

    def test_rc_claimed_while_a_criterion_is_open_fails(self):
        text = page({"B1": ("not met", "`docs/page.md`", "#722")}, verdict={"1.0.0-rc.1 may be cut": "yes"})
        self.assertFails(text, "1.0.0-rc.1 may be cut 'yes'; the rows say 'no'")

    def test_final_claimed_while_a1_is_open_fails(self):
        text = page({"A1": ("not met", "`docs/page.md`", "#741")}, verdict={"1.0.0 may be cut": "yes"})
        self.assertFails(text, "1.0.0 may be cut 'yes'; the rows say 'no'")

    def test_a_missing_verdict_row_fails(self):
        self.assertFails(page().replace("| Partial | 0 |\n", ""), "the verdict has no 'Partial' row")


class Online(Base):
    def api(self, extra: dict | None = None) -> Fake:
        data = {
            "repos/allamiro/vtop-engine/issues/726": {"state": "open"},
            "repos/allamiro/vtop-engine/issues/727": {"state": "closed"},
            "repos/allamiro/vtop-engine/issues/715": {"state": "closed"},
            "repos/allamiro/vtop-engine/issues/878": {"state": "closed", "pull_request": {}},
            "repos/allamiro/vtop-engine/actions/runs/11": {"conclusion": "success"},
            "repos/allamiro/vtop-engine/actions/runs/12": {"conclusion": "failure"},
            "repos/allamiro/vtop-engine/pulls/5": {"merged_at": "2026-11-20T00:00:00Z"},
            "repos/allamiro/vtop-engine/pulls/6": {"merged_at": None},
        }
        data.update(extra or {})
        return Fake(data)

    def test_open_issues_and_runs_that_exist_pass(self):
        text = page({"A3": ("not met", f"[run]({RUN}11)", "#726, #727")})
        code, out = self.run_check(text, self.api())
        self.assertEqual(code, 0, out)

    def test_a_missing_run_fails(self):
        self.assertFails(page({"A2": ("met", f"[run]({RUN}99)", "none")}), "CI run 99 does not exist", self.api())

    def test_a_failed_run_in_a_met_row_fails(self):
        self.assertFails(page({"A2": ("met", f"[run]({RUN}12)", "none")}), "must have succeeded", self.api())

    def test_a_failed_run_in_a_partial_row_passes(self):
        code, out = self.run_check(page({"A3": ("partial", f"[run]({RUN}12)", "#726")}), self.api())
        self.assertEqual(code, 0, out)

    def test_a_missing_issue_fails(self):
        self.assertFails(page({"A3": ("not met", "x", "#1234")}), "#1234 (named by A3) does not exist", self.api())

    def test_a_pull_request_in_open_fails(self):
        self.assertFails(page({"A3": ("not met", "x", "#878")}), "is a pull request, not an issue", self.api())

    def test_a_row_held_open_only_by_closed_issues_fails(self):
        self.assertFails(page({"A1": ("not met", "x", "#715")}), "every issue it names is closed", self.api())

    def test_without_git_history_pull_requests_are_read_from_the_api(self):
        shutil.rmtree(self.root / ".git")
        api = self.api()
        self.assertFails(page({"A2": ("met", "#5 and #6", "none")}), "#6 is not a merged pull request", api)
        self.assertIn("repos/allamiro/vtop-engine/pulls/5", api.calls)

    def test_without_git_history_or_online_it_cannot_run(self):
        shutil.rmtree(self.root / ".git")
        code, out = self.run_check(page())
        self.assertEqual(code, 2, out)
        self.assertIn("fetch-depth: 0", out)

    def test_no_history_skips_pull_requests_and_still_checks_files(self):
        shutil.rmtree(self.root / ".git")
        self.doc.write_text(page({"A2": ("met", "#6 and `scripts/gone.sh`", "none")}))
        out = io.StringIO()
        with redirect_stdout(out):
            code = MOD.main(["--doc", str(self.doc), "--root", str(self.root), "--today", TODAY.isoformat(), "--no-history"])
        self.assertEqual(code, 1, out.getvalue())
        self.assertIn("`scripts/gone.sh` does not exist", out.getvalue())
        self.assertNotIn("#6", out.getvalue())

    def test_offline_does_not_call_the_api(self):
        api = self.api()
        self.doc.write_text(page({"A2": ("met", f"[run]({RUN}99)", "none")}))
        with redirect_stdout(io.StringIO()):
            code = MOD.main(["--doc", str(self.doc), "--root", str(self.root), "--today", TODAY.isoformat()], fetch=api)
        self.assertEqual(code, 0)
        self.assertEqual(api.calls, [])


class RealRepository(unittest.TestCase):
    def test_the_real_page_passes_offline(self):
        if MOD.merged_prs_from_git(REPO) is None:
            self.skipTest("shallow clone: the docs workflow fetches the full history")
        out = io.StringIO()
        with redirect_stdout(out):
            code = MOD.main([])
        self.assertEqual(code, 0, out.getvalue())


if __name__ == "__main__":
    unittest.main()
