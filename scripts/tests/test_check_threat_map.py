"""Tests for scripts/check-threat-map.py.

Each test starts from a minimal repository whose threat map passes, breaks one
thing, and asserts the check fails naming it. A final test runs the check on
the real repository. Run from the repository root:

    python3 -m unittest discover -s scripts/tests -p 'test_check_threat_map.py' -v
"""

from __future__ import annotations

import importlib.util
import io
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "check-threat-map.py"
REPO = SCRIPT.parent.parent


def load():
    spec = importlib.util.spec_from_file_location("check_threat_map", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


MOD = load()

HEADER = "| ID | Threat | Control | Test or CI check | Residual risk |\n|----|----|----|----|----|\n"
GAP_HEADER = "| ID | Missing | What closes it |\n|----|----|----|\n"

SOURCES = {
    "crates/a/src/lib.rs": (
        "pub fn enforce() {}\n"
        "pub const LIMIT: usize = 1;\n"
        "#[cfg(test)]\nmod tests {\n"
        "    #[test]\n    fn the_control_holds() {}\n"
        "    #[test]\n    #[ignore]\n    fn a_skipped_test() {}\n"
        "    #[test]\n    #[ignore = \"needs a broker\"]\n    fn a_skipped_test_with_a_reason() {}\n"
        "    // fn only_in_a_comment_above() {}\n"
        "    #[test]\n    fn the_control_holds_longer() {}\n"
        "}\n"
    ),
    "crates/a/src/layouts.rs": (
        "#[cfg(test)]\nmod tests {\n"
        "    /* a block comment\n    #[test]\n    fn removed_in_a_block() {}\n    */\n"
        "    /* outer /* nested */ still a comment\n    fn removed_in_a_nested_block() {}\n    */\n"
        "    #[test]\n    fn kept_after_the_comments() {}\n"
        "    #[test]\n    #[ignore]\n    #[tokio::test(\n        flavor = \"multi_thread\",\n        worker_threads = 2\n    )]\n"
        "    fn ignored_with_a_multiline_attribute() {}\n"
        "    #[ignore]\n    /// a doc comment between attributes\n    #[test]\n    fn ignored_above_a_doc_comment() {}\n"
        "    // #[ignore]\n    #[test]\n    fn not_ignored_by_a_comment() {}\n"
        "    #[tokio::test(\n        flavor = \"multi_thread\",\n    )]\n    fn multiline_attribute_and_run() {}\n"
        "    const QUOTED: &str = \"fn quoted_in_a_string() {}\";\n"
        "    const MULTI: &str = \"line one\nfn quoted_on_a_second_line() {}\";\n"
        "    const RAW: &str = r#\"a \"quoted\" fn quoted_in_a_raw_string() {}\n// not a comment\"#;\n"
        "    #[test]\n    fn after_a_raw_string_with_quotes() {}\n"
        "    const S: &str = \"/* not a comment\";\n    #[test]\n    fn after_a_string_with_a_comment_opener() {}\n"
        "    const C: char = '\"';\n    #[test]\n    fn after_a_quote_char() {}\n"
        "}\n"
    ),
    "scripts/tests/test_x.py": "class T:\n    def test_it_holds(self):\n        pass\n",
    ".github/workflows/ci.yml": "jobs:\n  audit:\n    name: supply-chain (cargo audit)\n",
    "deny.toml": "[sources]\nunknown-git = \"deny\"\n",
    "scripts/scenario.sh": "#!/usr/bin/env bash\necho ok\n",
}


def row(id_, control="`crates/a/src/lib.rs::enforce`", test="`crates/a/src/lib.rs::the_control_holds`"):
    return f"| {id_} | a threat | {control} | {test} | none known |\n"


def good_rows() -> dict[str, str]:
    return {
        "A1": row("A1"),
        "I1": row("I1"),
        "M1": row("M1"),
        "R1": row("R1"),
        "N1": row("N1"),
        "K1": row("K1"),
        "T1": row("T1"),
        "S1": row("S1"),
    }


def document(rows: dict[str, str], gaps: str = "", extra: str = "") -> str:
    body = "".join(rows.values())
    text = f"# Security Model\n\nintro\n\n{MOD.SECTION}\n\nprose with `inline code` and `i32::MAX`.\n\n{HEADER}{body}"
    if gaps:
        text += f"\n### Gaps\n\n{GAP_HEADER}{gaps}"
    return text + extra


class Repo:
    """A temporary repository: the sources, and a document to check."""

    def __init__(self, text: str, sources: dict[str, str] | None = None):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        for name, content in (SOURCES if sources is None else sources).items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        self.doc = self.root / "docs/SECURITY_MODEL.md"
        self.doc.parent.mkdir(parents=True, exist_ok=True)
        self.doc.write_text(text, encoding="utf-8")

    def run(self, *flags: str) -> tuple[int, str]:
        out = io.StringIO()
        with redirect_stdout(out):
            code = MOD.main(["--doc", str(self.doc), "--root", str(self.root), *flags])
        return code, out.getvalue()

    def problems(self) -> list[str]:
        return MOD.check(MOD.parse(self.doc.read_text(encoding="utf-8")), self.root)[0]

    def close(self):
        self.tmp.cleanup()


class CheckTests(unittest.TestCase):
    def repo(self, text: str, sources: dict[str, str] | None = None) -> Repo:
        r = Repo(text, sources)
        self.addCleanup(r.close)
        return r

    def assert_fails(self, repo: Repo, *fragments: str):
        code, out = repo.run()
        self.assertEqual(code, 1, out)
        for fragment in fragments:
            self.assertIn(fragment, out)

    def test_a_consistent_map_passes(self):
        code, out = self.repo(document(good_rows())).run()
        self.assertEqual(code, 0, out)
        self.assertIn("8 threats", out)
        self.assertIn("0 gaps", out)

    def test_inline_code_that_is_not_a_path_is_not_a_reference(self):
        rows = good_rows()
        rows["A1"] = row("A1", control="`crates/a/src/lib.rs::enforce` and `ServerConfig::max_sessions` and `transfer_peers`")
        self.assertEqual(self.repo(document(rows)).problems(), [])

    def test_a_file_at_the_repository_root_is_a_reference(self):
        rows = good_rows()
        rows["A1"] = row("A1", control="`deny.toml::unknown-git`")
        self.assertEqual(self.repo(document(rows)).problems(), [])
        rows["A1"] = row("A1", control="`deny.toml::unknown-registry`")
        self.assert_fails(self.repo(document(rows)), "A1", "unknown-registry")
        rows["A1"] = row("A1", control="`gone.toml::unknown-git`")
        self.assert_fails(self.repo(document(rows)), "A1", "gone.toml", "does not exist")

    def test_a_missing_file_fails_naming_it(self):
        rows = good_rows()
        rows["A1"] = row("A1", control="`crates/a/src/gone.rs::enforce`")
        self.assert_fails(self.repo(document(rows)), "A1", "crates/a/src/gone.rs", "does not exist")

    def test_a_renamed_control_symbol_fails(self):
        rows = good_rows()
        rows["M1"] = row("M1", control="`crates/a/src/lib.rs::enforce_renamed`")
        self.assert_fails(self.repo(document(rows)), "M1", "enforce_renamed")

    def test_a_symbol_matches_as_a_whole_word(self):
        rows = good_rows()
        rows["M1"] = row("M1", control="`crates/a/src/lib.rs::enforc`")
        self.assert_fails(self.repo(document(rows)), "M1", "no symbol `enforc`")

    def test_a_deleted_test_fails(self):
        rows = good_rows()
        rows["R1"] = row("R1", test="`crates/a/src/lib.rs::the_test_that_was_deleted`")
        self.assert_fails(self.repo(document(rows)), "R1", "the_test_that_was_deleted", "no test function")

    def test_a_test_reference_must_be_a_function_not_any_mention(self):
        rows = good_rows()
        rows["N1"] = row("N1", test="`crates/a/src/lib.rs::only_in_a_comment_above`")
        self.assert_fails(self.repo(document(rows)), "N1", "no test function")

    def test_a_test_name_that_only_prefixes_another_does_not_pass(self):
        rows = good_rows()
        rows["N1"] = row("N1", test="`crates/a/src/lib.rs::the_control`")
        self.assert_fails(self.repo(document(rows)), "N1", "no test function")

    def test_an_ignored_test_fails_whether_or_not_it_gives_a_reason(self):
        for name in ("a_skipped_test", "a_skipped_test_with_a_reason"):
            rows = good_rows()
            rows["K1"] = row("K1", test=f"`crates/a/src/lib.rs::{name}`")
            self.assert_fails(self.repo(document(rows)), "K1", "#[ignore]d")

    def test_a_block_commented_test_is_not_a_test_even_when_nested(self):
        for name in ("removed_in_a_block", "removed_in_a_nested_block"):
            rows = good_rows()
            rows["K1"] = row("K1", test=f"`crates/a/src/layouts.rs::{name}`")
            self.assert_fails(self.repo(document(rows)), "K1", "no test function")

    def test_a_declaration_quoted_in_a_string_is_not_a_test(self):
        for name in ("quoted_in_a_string", "quoted_on_a_second_line", "quoted_in_a_raw_string"):
            rows = good_rows()
            rows["K1"] = row("K1", test=f"`crates/a/src/layouts.rs::{name}`")
            self.assert_fails(self.repo(document(rows)), "K1", "no test function")

    def test_comment_stripping_does_not_hide_tests_after_strings_and_chars(self):
        for name in ("kept_after_the_comments", "after_a_string_with_a_comment_opener", "after_a_quote_char", "after_a_raw_string_with_quotes", "not_ignored_by_a_comment", "multiline_attribute_and_run"):
            rows = good_rows()
            rows["K1"] = row("K1", test=f"`crates/a/src/layouts.rs::{name}`")
            self.assertEqual(self.repo(document(rows)).problems(), [], name)

    def test_an_ignore_is_found_through_multiline_attributes_and_doc_comments(self):
        for name in ("ignored_with_a_multiline_attribute", "ignored_above_a_doc_comment"):
            rows = good_rows()
            rows["K1"] = row("K1", test=f"`crates/a/src/layouts.rs::{name}`")
            self.assert_fails(self.repo(document(rows)), "K1", "#[ignore]d")

    def test_a_python_test_is_found_by_def(self):
        rows = good_rows()
        rows["T1"] = row("T1", test="`scripts/tests/test_x.py::test_it_holds`")
        self.assertEqual(self.repo(document(rows)).problems(), [])
        rows["T1"] = row("T1", test="`scripts/tests/test_x.py::test_gone`")
        self.assert_fails(self.repo(document(rows)), "T1", "test_gone")

    def test_a_workflow_job_or_a_script_can_be_the_check(self):
        rows = good_rows()
        rows["S1"] = row("S1", test="`.github/workflows/ci.yml::supply-chain (cargo audit)`, `scripts/scenario.sh`")
        self.assertEqual(self.repo(document(rows)).problems(), [])
        rows["S1"] = row("S1", test="`.github/workflows/ci.yml::supply-chain (cargo deny)`")
        self.assert_fails(self.repo(document(rows)), "S1", "cargo deny")

    def test_a_cell_with_no_reference_and_no_gap_fails(self):
        rows = good_rows()
        rows["A1"] = row("A1", control="the engine refuses it")
        self.assert_fails(self.repo(document(rows)), "A1", "control cell names no reference")
        rows = good_rows()
        rows["A1"] = row("A1", test="")
        self.assert_fails(self.repo(document(rows)), "A1", "test cell names no reference")

    def test_a_duplicate_id_fails(self):
        rows = good_rows()
        rows["A1b"] = row("A1")
        self.assert_fails(self.repo(document(rows)), "A1", "used twice")

    def test_an_unknown_group_or_a_malformed_id_fails(self):
        for bad in ("Z1", "A", "a1", "A1x"):
            rows = good_rows()
            rows["bad"] = row(bad)
            self.assert_fails(self.repo(document(rows)), bad, "group letter")

    def test_a_group_with_no_row_fails_so_a_plane_cannot_drop_out(self):
        for letter in MOD.GROUPS:
            rows = good_rows()
            del rows[f"{letter}1"]
            self.assert_fails(self.repo(document(rows)), f"no row for group {letter}")

    def test_a_gap_cell_needs_a_row_in_the_gaps_table(self):
        rows = good_rows()
        rows["K1"] = row("K1", test="GAP: nothing opens the sessions")
        self.assert_fails(self.repo(document(rows)), "K1", "gaps table has no row")

    def test_a_listed_gap_must_still_be_a_gap(self):
        gaps = "| K1 | test | add one |\n"
        self.assert_fails(self.repo(document(good_rows(), gaps)), "K1", "no cell of its row is a GAP")

    def test_the_gaps_table_must_name_the_side_that_is_missing(self):
        rows = good_rows()
        rows["K1"] = row("K1", control="GAP: no control", test="GAP: no test")
        self.assert_fails(self.repo(document(rows, "| K1 | test | add both |\n")), "K1", "says test is missing, the row says both")
        self.assertEqual(self.repo(document(rows, "| K1 | both | add both |\n")).problems(), [])
        self.assert_fails(self.repo(document(rows, "| K1 | maybe | x |\n")), "Missing must be one of")

    def test_a_gap_is_reported_and_passes_when_listed(self):
        rows = good_rows()
        rows["K1"] = row("K1", test="GAP: nothing opens the sessions")
        code, out = self.repo(document(rows, "| K1 | test | add one |\n")).run()
        self.assertEqual(code, 0, out)
        self.assertIn("1 gap listed", out)

    def test_a_gap_cell_may_name_a_reference_and_it_is_checked(self):
        rows = good_rows()
        rows["K1"] = row("K1", control="GAP: `crates/a/src/gone.rs` has no check")
        self.assert_fails(self.repo(document(rows, "| K1 | control | add one |\n")), "gone.rs", "does not exist")

    def test_a_gap_listed_twice_fails(self):
        rows = good_rows()
        rows["K1"] = row("K1", test="GAP: none")
        gaps = "| K1 | test | a |\n| K1 | test | b |\n"
        self.assert_fails(self.repo(document(rows, gaps)), "K1", "lists it twice")

    def test_lines_prints_the_current_line_of_every_reference(self):
        code, out = self.repo(document(good_rows())).run("--lines")
        self.assertEqual(code, 0, out)
        self.assertIn("A1 control: crates/a/src/lib.rs:1  enforce", out)
        self.assertIn("A1 test: crates/a/src/lib.rs:6  the_control_holds", out)

    def test_a_document_without_the_section_or_a_table_cannot_be_read(self):
        code, out = self.repo("# Security Model\n").run()
        self.assertEqual(code, 2, out)
        self.assertIn("not found", out)
        code, out = self.repo(f"{MOD.SECTION}\n\nno table here\n").run()
        self.assertEqual(code, 2, out)
        self.assertIn("no threat table", out)

    def test_a_changed_header_or_cell_count_cannot_be_read(self):
        text = document(good_rows()).replace("Residual risk", "Risk")
        code, out = self.repo(text).run()
        self.assertEqual(code, 2, out)
        self.assertIn("expected", out)
        text = document(good_rows()).replace("| none known |\n| I1", "| none known | extra |\n| I1", 1)
        code, out = self.repo(text).run()
        self.assertEqual(code, 2, out)
        self.assertIn("cells", out)

    def test_a_table_in_another_section_is_not_read(self):
        text = document(good_rows()) + "\n## 18. Something else\n\n| a | b |\n|---|---|\n| 1 | 2 |\n"
        self.assertEqual(self.repo(text).run()[0], 0)

    def test_a_missing_document_cannot_be_read(self):
        with redirect_stdout(io.StringIO()):
            self.assertEqual(MOD.main(["--doc", "/nonexistent/SECURITY_MODEL.md"]), 2)


class RealRepositoryTests(unittest.TestCase):
    def test_the_real_threat_map_holds_and_covers_the_cluster_plane_and_the_gateway(self):
        out = io.StringIO()
        with redirect_stdout(out):
            code = MOD.main([])
        self.assertEqual(code, 0, out.getvalue())
        parsed = MOD.parse((REPO / "docs/SECURITY_MODEL.md").read_text(encoding="utf-8"))
        groups = {r.id[0] for r in parsed.rows}
        self.assertEqual(groups, set(MOD.GROUPS))
        # The three crates the issue names, and the gateway, each appear in a control.
        controls = " ".join(r.control for r in parsed.rows)
        for crate in ("vtop-meta", "vtop-node", "vtop-broker", "vtop-kafka"):
            self.assertIn(f"crates/{crate}/", controls)


if __name__ == "__main__":
    unittest.main()
