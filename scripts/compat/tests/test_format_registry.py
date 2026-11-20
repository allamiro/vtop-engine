"""Tests for scripts/check-format-registry.py (#743): the registry check must
be able to fail for each kind of drift it exists to catch.

Run with: python3 -m unittest discover -s scripts/compat/tests
"""

import importlib.util
import pathlib
import tempfile
import textwrap
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("check_format_registry", ROOT / "scripts/check-format-registry.py")
registry_check = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(registry_check)

SOURCE = textwrap.dedent(
    """\
    const MAGIC: &[u8; 8] = b"VTOPDEMO";
    pub const DEMO_VERSION: u32 = 1;
    const COMMAND_KIND_FIRST: u16 = 1;
    const COMMAND_KIND_SECOND: u16 = 2;
    const MAX_DEMO_VERSION_BYTES: usize = 64;

    #[cfg(test)]
    mod tests {
        const TEST_VERSION: u32 = 9;
        const FOREIGN: &[u8; 8] = b"VTOPELSE";
    }
    """
)

REGISTRY = textwrap.dedent(
    """\
    formats:
      - id: 1
        name: Demo
        class: B
        marker: "`VTOPDEMO` v1"
        status: evolving
        at_1_0: stable
        constants:
          - {file: crates/demo/src/lib.rs, name: MAGIC, value: 'b"VTOPDEMO"'}
          - {file: crates/demo/src/lib.rs, name: DEMO_VERSION, value: '1'}
        tag_sets:
          - file: crates/demo/src/lib.rs
            prefix: COMMAND_KIND_
            tags: {COMMAND_KIND_FIRST: 1, COMMAND_KIND_SECOND: 2}
    not_formats:
      - {file: crates/demo/src/lib.rs, name: MAX_DEMO_VERSION_BYTES, why: a bound}
    """
)

PAGE = "# Page\n\n<!-- inventory:begin -->\n<!-- inventory:end -->\n"


class RegistryCheck(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        (self.root / "crates/demo/src").mkdir(parents=True)
        (self.root / "scripts/compat").mkdir(parents=True)
        (self.root / "docs/reference").mkdir(parents=True)
        self.source(SOURCE)
        (self.root / "scripts/compat/formats.yaml").write_text(REGISTRY)
        (self.root / "docs/reference/compatibility.md").write_text(PAGE)
        self.assertEqual(self.run_check("--write"), 0)

    def tearDown(self):
        self.tmp.cleanup()

    def source(self, text):
        (self.root / "crates/demo/src/lib.rs").write_text(text)

    def run_check(self, *args):
        return registry_check.main(["--root", str(self.root), *args])

    def problems(self):
        return registry_check.check_source(self.root, registry_check.load(self.root))

    def test_a_registry_that_matches_passes(self):
        self.assertEqual(self.problems(), [])
        self.assertEqual(self.run_check(), 0)

    def test_a_new_magic_with_no_entry_fails(self):
        self.source(SOURCE + 'const OTHER: [u8; 8] = *b"VTOPNEW1";\n')
        # After the test module too: only the module is skipped, not the
        # rest of the file.
        self.assertIn("magic VTOPNEW1", " ".join(self.problems()))
        self.source(SOURCE.replace("#[cfg(test)]", 'const OTHER: [u8; 8] = *b"VTOPNEW1";\n\n#[cfg(test)]'))
        self.assertIn("magic VTOPNEW1", " ".join(self.problems()))

    def test_a_new_version_constant_with_no_entry_fails(self):
        self.source(SOURCE.replace("const MAX_DEMO", "const SIDECAR_VERSION: u8 = 1;\nconst MAX_DEMO"))
        self.assertIn("SIDECAR_VERSION = 1 is in no entry", " ".join(self.problems()))

    def test_a_version_bump_without_a_registry_edit_fails(self):
        self.source(SOURCE.replace("DEMO_VERSION: u32 = 1", "DEMO_VERSION: u32 = 2"))
        self.assertIn("DEMO_VERSION is 2", " ".join(self.problems()))

    def test_a_new_command_kind_without_a_registry_edit_fails(self):
        for third in ("3", "0x3", "3u16", "COMMAND_KIND_SECOND + 1"):
            self.source(SOURCE.replace("const MAX_DEMO", f"const COMMAND_KIND_THIRD: u16 = {third};\nconst MAX_DEMO"))
            self.assertIn("COMMAND_KIND_THIRD", " ".join(self.problems()), third)

    def test_renumbered_or_colliding_tags_fail(self):
        swapped = SOURCE.replace("COMMAND_KIND_FIRST: u16 = 1", "COMMAND_KIND_FIRST: u16 = 9")
        self.source(swapped)
        self.assertIn("COMMAND_KIND_FIRST", " ".join(self.problems()))
        both = SOURCE.replace("COMMAND_KIND_SECOND: u16 = 2", "COMMAND_KIND_SECOND: u16 = 1")
        self.source(both)
        self.assertIn("COMMAND_KIND_SECOND", " ".join(self.problems()))

    def test_a_plural_versions_set_is_a_format_constant(self):
        self.source(SOURCE.replace("const MAX_DEMO", 'const LEGACY_VERSIONS: [&str; 2] = ["0.1", "0.2"];\nconst MAX_DEMO'))
        self.assertIn("LEGACY_VERSIONS", " ".join(self.problems()))

    def test_an_out_of_line_test_module_hides_nothing_after_it(self):
        self.source(SOURCE.replace("#[cfg(test)]\nmod tests {", "#[cfg(test)]\nmod extra_tests;\nconst NEW_VERSION: u8 = 1;\nfn f() {}\n\n#[cfg(test)]\nmod tests {"))
        self.assertIn("NEW_VERSION", " ".join(self.problems()))

    def test_the_file_of_an_out_of_line_test_module_is_not_scanned(self):
        self.source(SOURCE.replace("#[cfg(test)]\nmod tests {", "#[cfg(test)]\nmod race_tests;\n\n#[cfg(test)]\nmod tests {"))
        (self.root / "crates/demo/src/race_tests.rs").write_text(
            'const FIXTURE_VERSION: u8 = 7;\nconst FIXTURE: &[u8; 8] = b"VTOPTEST";\n')
        self.assertEqual(self.problems(), [])
        # A nested module's file sits in the parent module's directory.
        (self.root / "crates/demo/src/race_tests.rs").unlink()
        (self.root / "crates/demo/src/sub").mkdir()
        (self.root / "crates/demo/src/sub.rs").write_text("#[cfg(test)]\nmod fence_tests;\n")
        (self.root / "crates/demo/src/sub/fence_tests.rs").write_text("const FIXTURE_VERSION: u8 = 7;\n")
        self.assertEqual(self.problems(), [])

    def test_byte_raw_strings_and_nested_comments_in_a_test_module_hide_nothing_after_it(self):
        for literal in ('let _ = br#"a" { "#;', 'let _ = br"\\";', "/* /* */ { */"):
            body = SOURCE.replace('const FOREIGN', literal + "\n        const FOREIGN")
            self.source(body + 'const NEW_VERSION: u8 = 1;\n')
            self.assertIn("NEW_VERSION", " ".join(self.problems()), literal)

    def test_spaces_inside_a_string_value_are_compared_exactly(self):
        registry = REGISTRY.replace("value: '1'}", "value: '1'}\n      - {file: crates/demo/src/lib.rs, name: DEMO_SCHEMA_PREFIX, value: '\"-- demo \"'}")
        (self.root / "scripts/compat/formats.yaml").write_text(registry)
        self.source(SOURCE + 'const DEMO_SCHEMA_PREFIX: &str = "-- demo ";\n')
        self.assertEqual(self.problems(), [])
        self.source(SOURCE + 'const DEMO_SCHEMA_PREFIX: &str = "--  demo ";\n')
        self.assertIn("DEMO_SCHEMA_PREFIX", " ".join(self.problems()))

    def test_a_not_formats_row_cannot_switch_off_a_listed_constant(self):
        (self.root / "scripts/compat/formats.yaml").write_text(
            REGISTRY + "  - {file: crates/demo/src/lib.rs, name: DEMO_VERSION, why: sneaky}\n")
        self.assertIn("both", " ".join(self.problems()))

    def test_a_marker_can_quote_a_constant_and_follows_it(self):
        registry = REGISTRY.replace('marker: "`VTOPDEMO` v1"', 'marker: "`VTOPDEMO` v{DEMO_VERSION}"')
        (self.root / "scripts/compat/formats.yaml").write_text(registry)
        self.assertEqual(self.run_check("--write"), 0)
        page = (self.root / "docs/reference/compatibility.md").read_text()
        self.assertIn("`VTOPDEMO` v1 |", page)

    def test_a_constant_the_sources_no_longer_hold_fails(self):
        self.source(SOURCE.replace("pub const DEMO_VERSION: u32 = 1;\n", ""))
        self.assertIn("no longer hold", " ".join(self.problems()))

    def test_test_modules_are_not_scanned(self):
        joined = " ".join(self.problems())
        self.assertNotIn("TEST_VERSION", joined)
        self.assertNotIn("VTOPELSE", joined)

    def test_an_indented_test_module_inside_a_production_module_is_not_scanned(self):
        # Review of #783: rustfmt indents the attribute of a test module
        # nested in an inline module, and only a column-zero attribute was
        # recognised, so its fixtures read as production formats.
        nested = textwrap.dedent(
            """\
            mod inner {
                pub fn f() {}

                #[cfg(test)]
                mod tests {
                    const NESTED_TEST_VERSION: u32 = 9;
                    const NESTED: &[u8; 8] = b"VTOPNEST";
                }
            }
            """
        )
        self.source(SOURCE + nested)
        joined = " ".join(self.problems())
        self.assertNotIn("NESTED_TEST_VERSION", joined)
        self.assertNotIn("VTOPNEST", joined)
        self.assertEqual(self.problems(), [])
        # The production module goes on after it, and is still scanned.
        self.source(SOURCE + nested.replace("    }\n}\n", "    }\n\n    const INNER_VERSION: u8 = 1;\n}\n"))
        self.assertIn("INNER_VERSION", " ".join(self.problems()))

    def test_a_commented_out_declaration_does_not_mask_a_version_bump(self):
        # Review of #783: comments were scanned as code, so an old
        # declaration kept in a comment after the live one overwrote its
        # value and a bump passed against a registry that still said 1.
        bumped = SOURCE.replace("DEMO_VERSION: u32 = 1", "DEMO_VERSION: u32 = 2")
        for comment in (
            "/* was:\npub const DEMO_VERSION: u32 = 1;\n*/\n",
            "// pub const DEMO_VERSION: u32 = 1;\n",
            "/* /* nested */\nconst DEMO_VERSION: u32 = 1;\n*/\n",
        ):
            self.source(bumped.replace("const COMMAND_KIND_FIRST", comment + "const COMMAND_KIND_FIRST"))
            self.assertIn("DEMO_VERSION is 2", " ".join(self.problems()), comment)

    def test_a_commented_out_declaration_does_not_stand_in_for_a_removed_one(self):
        self.source(SOURCE.replace("pub const DEMO_VERSION: u32 = 1;", "/*\npub const DEMO_VERSION: u32 = 1;\n*/"))
        self.assertIn("no longer hold", " ".join(self.problems()))
        # Nor is a commented-out tag a new one.
        self.source(SOURCE.replace("const COMMAND_KIND_SECOND", "/*\nconst COMMAND_KIND_THIRD: u16 = 3;\n*/\nconst COMMAND_KIND_SECOND"))
        self.assertEqual(self.problems(), [])

    def test_a_magic_named_in_a_comment_is_not_a_format(self):
        self.source(SOURCE.replace("pub const DEMO_VERSION", '/// Replaces the old `b"VTOPOLD1"` header.\n/* b"VTOPOLD2" */\npub const DEMO_VERSION'))
        self.assertEqual(self.problems(), [])
        # A comment marker inside a string is not a comment.
        self.source(SOURCE + 'const URL: &str = "http://x";\nconst OTHER: [u8; 8] = *b"VTOPNEW1";\n')
        self.assertIn("magic VTOPNEW1", " ".join(self.problems()))

    def test_the_same_constant_in_two_scopes_of_one_file_fails(self):
        # Review of #783: a second declaration of a listed name in another
        # module of the same file overwrote the first, so the registered one
        # could change unseen.
        legacy = "\nmod legacy {\n    pub const DEMO_VERSION: u32 = 1;\n}\n"
        self.source(SOURCE.replace("DEMO_VERSION: u32 = 1", "DEMO_VERSION: u32 = 2") + legacy)
        self.assertIn("DEMO_VERSION is declared 2 times", " ".join(self.problems()))
        self.source(SOURCE + legacy)
        self.assertIn("DEMO_VERSION is declared 2 times", " ".join(self.problems()))
        # A tag too.
        self.source(SOURCE + "\nmod legacy {\n    const COMMAND_KIND_FIRST: u16 = 1;\n}\n")
        self.assertIn("COMMAND_KIND_FIRST is declared 2 times", " ".join(self.problems()))

    def test_a_hand_edited_inventory_table_fails_and_write_restores_it(self):
        page = self.root / "docs/reference/compatibility.md"
        page.write_text(page.read_text().replace("| evolving |", "| stable |"))
        self.assertEqual(self.run_check(), 1)
        self.assertEqual(self.run_check("--write"), 0)
        self.assertEqual(self.run_check(), 0)

    def test_a_not_formats_row_needs_a_reason(self):
        (self.root / "scripts/compat/formats.yaml").write_text(REGISTRY.replace(", why: a bound", ""))
        self.assertIn("needs a why", " ".join(self.problems()))

    def test_a_fixture_or_fuzz_target_that_does_not_exist_fails(self):
        (self.root / "scripts/compat/formats.yaml").write_text(
            REGISTRY.replace("    tag_sets:", "    fixtures: [crates/demo/tests/fixtures/gone]\n    fuzz: [gone]\n    tag_sets:")
        )
        joined = " ".join(self.problems())
        self.assertIn("fixture crates/demo/tests/fixtures/gone does not exist", joined)
        self.assertIn("fuzz target gone does not exist", joined)


class TheRepository(unittest.TestCase):
    def test_registry_names_every_format(self):
        self.assertEqual(registry_check.check_source(ROOT, registry_check.load(ROOT)), [])

    def test_the_page_table_is_what_the_registry_generates(self):
        self.assertEqual(registry_check.main(["--root", str(ROOT)]), 0)


if __name__ == "__main__":
    unittest.main()
