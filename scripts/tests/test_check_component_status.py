"""Tests for scripts/check-component-status.py.

Each test builds a minimal repository whose status page is well formed and
whose other pages claim nothing, changes one thing, and asserts the check
names it. A final test runs the check on the real repository. Run from the
repository root:

    python3 -m unittest discover -s scripts/tests -p 'test_check_component_status.py' -v
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "check-component-status.py"
REPO = SCRIPT.parent.parent


def load():
    spec = importlib.util.spec_from_file_location("check_component_status", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


MOD = load()
GATEWAY = "Kafka gateway"
ENGINE = "Archive engine core"


def page(today: dict[str, str] | None = None, decision: str = "PROPOSED", status: str = "PROPOSED",
         open_criteria: dict[str, str] | None = None, drop_row: str | None = None,
         drop_section: str | None = None, extra_row: str | None = None) -> str:
    today = today or {}
    open_criteria = open_criteria or {}
    rows = []
    for name in MOD.COMPONENTS:
        if name == drop_row:
            continue
        rows.append(f"| {name} | {today.get(name, 'preview')} | production | {decision} |")
    if extra_row:
        rows.append(extra_row)
    out = [f"# Component status\n\nDecision status: {status}\n", MOD.BEGIN, "",
           "| Component | Today | At 1.0 | Decision |", "|---|---|---|---|", *rows, "", MOD.END, "",
           "## Evidence per component\n"]
    for name in MOD.COMPONENTS:
        if name == drop_section:
            continue
        out.append(f"### {name}\n")
        out.append("- **Why:** because.")
        out.append("- **Tests and CI:** some.")
        out.append(f"- **Open criteria:** {open_criteria.get(name, 'A1, B1.')}")
        out.append("- **Docs:** README.\n")
    return "\n".join(out)


class CheckTests(unittest.TestCase):
    def repo(self, status_page: str | None = None, **files: str) -> pathlib.Path:
        tmp = pathlib.Path(tempfile.mkdtemp())
        base = {
            MOD.PAGE: status_page if status_page is not None else page(),
            "README.md": "# VTOP\n\nSee [status](docs/reference/component-status.md).\n",
            "docs/index.md": "# Home\n\nA prototype.\n",
        }
        for rel, text in {**base, **files}.items():
            path = tmp / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        return tmp

    def problems(self, **kw) -> list[str]:
        return MOD.check(self.repo(**kw))

    def test_a_well_formed_page_passes(self):
        self.assertEqual(self.problems(), [])

    def test_a_missing_component_row_fails(self):
        out = self.problems(status_page=page(drop_row=GATEWAY))
        self.assertTrue(any(f"no row for {GATEWAY!r}" in p for p in out), out)

    def test_an_unknown_component_fails(self):
        out = self.problems(status_page=page(extra_row="| The datagram transport | preview | preview | PROPOSED |"))
        self.assertTrue(any("not a known component" in p for p in out), out)

    def test_a_duplicate_row_fails(self):
        out = self.problems(status_page=page(extra_row=f"| {GATEWAY} | preview | preview | PROPOSED |"))
        self.assertTrue(any("listed twice" in p for p in out), out)

    def test_a_label_outside_the_three_fails(self):
        out = self.problems(status_page=page(today={GATEWAY: "beta"}))
        self.assertTrue(any("'beta'" in p and "Today" in p for p in out), out)

    def test_a_bad_decision_cell_fails(self):
        out = self.problems(status_page=page(decision="MAYBE"))
        self.assertTrue(any("Decision is 'MAYBE'" in p for p in out), out)

    def test_a_confirmed_status_with_proposed_rows_fails(self):
        out = self.problems(status_page=page(status="CONFIRMED"))
        self.assertTrue(any("'Decision status: CONFIRMED' but the rows say PROPOSED" in p for p in out), out)

    def test_a_proposed_status_with_every_row_confirmed_fails(self):
        out = self.problems(status_page=page(decision="CONFIRMED", status="PROPOSED"))
        self.assertTrue(any("'Decision status: PROPOSED' but the rows say CONFIRMED" in p for p in out), out)

    def test_every_row_confirmed_with_a_confirmed_status_passes(self):
        self.assertEqual(self.problems(status_page=page(decision="CONFIRMED", status="CONFIRMED")), [])

    def test_a_missing_evidence_section_fails(self):
        out = self.problems(status_page=page(drop_section=GATEWAY))
        self.assertTrue(any(f"no '### {GATEWAY}' evidence section" in p for p in out), out)

    def test_a_production_label_with_open_criteria_fails(self):
        out = self.problems(status_page=page(today={ENGINE: "production"}))
        self.assertTrue(any("production today but its Open criteria" in p for p in out), out)

    def test_a_production_label_with_none_open_passes(self):
        self.assertEqual(
            self.problems(status_page=page(today={ENGINE: "production"}, open_criteria={ENGINE: "none."})),
            [],
        )

    def test_a_missing_readme_link_fails(self):
        out = self.problems(**{"README.md": "# VTOP\n"})
        self.assertTrue(any("does not link" in p for p in out), out)

    def test_a_component_claim_above_its_label_fails(self):
        out = self.problems(**{"docs/deploy/x.md": "The Kafka gateway is production-ready.\n"})
        self.assertEqual(len(out), 1, out)
        self.assertIn("docs/deploy/x.md:1", out[0])
        self.assertIn("Kafka gateway", out[0])

    def test_a_claim_about_the_cluster_plane_is_held_to_its_three_parts(self):
        out = self.problems(**{"docs/a.md": "The cluster plane is production grade.\n"})
        self.assertEqual(len(out), 1, out)
        self.assertIn("Metadata plane", out[0])

    def test_a_denied_claim_passes(self):
        text = "The Kafka gateway is not production-ready. It is a prototype.\n"
        self.assertEqual(self.problems(**{"docs/a.md": text}), [])

    def test_a_conditional_claim_passes(self):
        text = "Until the soak passes, nobody should call the cluster plane ready for production.\n"
        self.assertEqual(self.problems(**{"docs/a.md": text}), [])

    def test_a_claim_wrapped_across_lines_is_read_as_one_sentence(self):
        # Review of #826: each physical line used to be judged alone, so a
        # wrapped denial failed and a wrapped claim passed.
        denied = "The Kafka gateway is not\nproduction-ready.\n"
        self.assertEqual(self.problems(**{"docs/a.md": denied}), [])
        claimed = "The Kafka gateway is ready\nfor production.\n"
        out = self.problems(**{"docs/a.md": claimed})
        self.assertEqual(len(out), 1, out)
        self.assertIn("docs/a.md:1", out[0])

    def test_blocks_are_not_joined_across_list_items_headings_or_blank_lines(self):
        text = "- The Kafka gateway is not\n- production-ready.\n"
        blocks = [b for _, b in MOD.paragraphs(text)]
        self.assertEqual(blocks, ["- The Kafka gateway is not", "- production-ready."])
        text = "Not this one.\n\n# The Kafka gateway\nis production-ready.\n"
        self.assertEqual([n for n, _ in MOD.paragraphs(text)], [1, 3, 4])

    def test_a_positive_claim_with_a_qualifier_word_after_it_fails(self):
        # Review of #826: a qualifier anywhere in the sentence used to excuse it.
        for text in (
            "The Kafka gateway is production-ready without additional configuration.\n",
            "The Kafka gateway is production-ready and requires no special setup.\n",
        ):
            with self.subTest(text=text):
                out = self.problems(**{"docs/a.md": text})
                self.assertEqual(len(out), 1, out)

    def test_a_claim_conditioned_after_it_passes(self):
        for text in (
            "The Kafka gateway becomes production-ready once #751 lands.\n",
            "The Kafka gateway is production-ready only if TLS is on.\n",
            "## What a production-grade Kafka gateway needs\n",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.problems(**{"docs/a.md": text}), [])

    def test_each_claim_in_a_sentence_is_judged_on_its_own(self):
        # Review of #826: only the first claim of a sentence was read, so a
        # denied claim hid an unsupported one after it.
        text = "The archive engine is not production-ready, but the Kafka gateway is production-ready.\n"
        out = self.problems(**{"docs/a.md": text})
        self.assertEqual(len(out), 1, out)
        self.assertIn(GATEWAY, out[0])
        self.assertNotIn(ENGINE, out[0])
        text = "The Kafka gateway is production-ready, but the archive engine is not production-ready.\n"
        out = self.problems(**{"docs/a.md": text})
        self.assertEqual(len(out), 1, out)
        self.assertIn(GATEWAY, out[0])
        self.assertNotIn(ENGINE, out[0])
        text = "The archive engine is not production-ready, but VTOP is production-ready.\n"
        out = self.problems(**{"docs/a.md": text})
        self.assertEqual(len(out), 1, out)
        self.assertIn("of VTOP", out[0])

    def test_one_denial_covers_claims_joined_to_it(self):
        for text in (
            "The Kafka gateway is not production-ready or battle-tested.\n",
            "The Kafka gateway is neither production-ready nor battle-tested.\n",
            "The Kafka gateway is not production-ready, battle-tested, or enterprise-grade.\n",
        ):
            with self.subTest(text=text):
                self.assertEqual(self.problems(**{"docs/a.md": text}), [])

    def test_a_project_wide_claim_fails_while_any_row_is_below_production(self):
        out = self.problems(**{"README.md": "[s](docs/reference/component-status.md)\n\nVTOP is production-ready.\n"})
        self.assertEqual(len(out), 1, out)
        self.assertIn("of VTOP", out[0])

    def test_a_claim_about_another_product_is_not_held_to_the_project_rule(self):
        # Review of #826: no component named used to mean "about VTOP".
        for text in ("Apache Kafka is production-ready.\n", "Kubernetes is battle-tested.\n"):
            with self.subTest(text=text):
                self.assertEqual(self.problems(**{"docs/a.md": text}), [])
        for text in ("This project is production-ready.\n", "The VTOP engine is production grade.\n"):
            with self.subTest(text=text):
                self.assertEqual(len(self.problems(**{"docs/a.md": text})), 1)

    def test_a_claim_inside_a_code_fence_or_comment_is_ignored(self):
        text = "```\nThe Kafka gateway is production-ready.\n```\n<!-- Kafka gateway production-ready -->\n"
        self.assertEqual(self.problems(**{"docs/a.md": text}), [])

    def test_the_status_page_and_the_internal_drafts_are_not_scanned(self):
        text = "The Kafka gateway is production-ready.\n"
        self.assertEqual(self.problems(**{"docs/INVENTION_DISCLOSURE_DRAFT.md": text}), [])

    def test_a_claim_at_or_below_the_label_passes(self):
        out = self.problems(
            status_page=page(today={GATEWAY: "production"}, open_criteria={GATEWAY: "none."}),
            **{"docs/a.md": "The Kafka gateway is production-ready.\n"},
        )
        self.assertEqual(out, [])

    def test_a_missing_page_is_an_error_not_a_pass(self):
        tmp = pathlib.Path(tempfile.mkdtemp())
        with self.assertRaises(MOD.PageError):
            MOD.check(tmp)

    def test_the_real_repository_is_consistent(self):
        self.assertEqual(MOD.check(REPO), [])


if __name__ == "__main__":
    unittest.main()


class StripCommentsTests(unittest.TestCase):
    """HTML comments are removed by a scan that carries its state across lines."""

    def test_comments_are_removed_on_one_line_and_across_lines(self):
        strip = MOD.strip_comments
        cases = [
            ("a <!-- b --> c", False, ("a  c", False)),
            ("a <!-- b", False, ("a ", True)),
            ("still in", True, ("", True)),
            ("end --> after", True, (" after", False)),
            ("x <!-- 1 --> y <!-- 2 --> z", False, ("x  y  z", False)),
            ("--> a <!-- b", True, (" a ", True)),
            ("no comment", False, ("no comment", False)),
        ]
        for line, state, want in cases:
            with self.subTest(line=line, in_comment=state):
                self.assertEqual(strip(line, state), want)

    def test_a_claim_inside_a_multi_line_comment_is_not_read(self):
        text = "Fine prose.\n<!--\nVTOP is production-ready.\n-->\nMore prose.\n"
        lines = [s for _, s in MOD.sentences(text)]
        self.assertFalse(any("production-ready" in s for s in lines), lines)
