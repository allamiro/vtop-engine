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


class RegistryCase(unittest.TestCase):
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


class RegistryCheck(RegistryCase):
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


    def test_a_cell_the_matrix_does_not_run_fails_and_so_does_one_no_format_names(self):
        (self.root / "scripts/compat/expected.yaml").write_text("cells:\n  ledger:\n    since: v0.9.0\n    directions: [upgrade]\n")
        self.assertIn("cell ledger is named by no entry", " ".join(self.problems()))
        (self.root / "scripts/compat/formats.yaml").write_text(
            REGISTRY.replace("    tag_sets:", "    cells: [ledger, gone]\n    tag_sets:")
        )
        joined = " ".join(self.problems())
        self.assertIn("cell gone is not a cell of", joined)
        self.assertNotIn("cell ledger is not a cell", joined)
        self.assertNotIn("cell ledger is named by no entry", joined)


GOLDEN_REGISTRY = REGISTRY.replace(
    "    tag_sets:",
    "    fixtures: [crates/demo/tests/fixtures]\n    tag_sets:",
) + "golden_lock: scripts/compat/golden.lock\n"

EXPECTED_CELLS = (
    "cells:\n  demo:\n    since: v0.9.0\n    directions: [upgrade, forward]\n"
)


class GoldenRegistryCase(RegistryCase):
    """A registry whose entry names a fixture directory and a lock file."""

    def setUp(self):
        super().setUp()
        self.fixtures = self.root / "crates/demo/tests/fixtures"
        self.fixtures.mkdir(parents=True)
        (self.fixtures / "frame.bin").write_bytes(b"\x00VTOPDEMO\x01")
        (self.fixtures / "state.json").write_text('{"v": 1}\n')
        self.registry(GOLDEN_REGISTRY)
        self.assertEqual(self.run_check("--write-lock"), 0)

    def registry(self, text):
        (self.root / "scripts/compat/formats.yaml").write_text(text)

    def lock_text(self):
        return (self.root / "scripts/compat/golden.lock").read_text()

    def joined(self):
        return "\n".join(self.problems())


class GoldenLock(GoldenRegistryCase):
    def test_a_regenerated_lock_passes_and_lists_every_golden_file_sorted(self):
        self.assertEqual(self.problems(), [])
        rows = [line.split("  ", 1) for line in self.lock_text().splitlines() if not line.startswith("#")]
        self.assertEqual([path for _, path in rows],
                         ["crates/demo/tests/fixtures/frame.bin", "crates/demo/tests/fixtures/state.json"])
        self.assertTrue(all(len(digest) == 64 for digest, _ in rows))

    def test_the_lock_is_in_the_form_sha256sum_reads(self):
        import hashlib
        digest = hashlib.sha256(b'{"v": 1}\n').hexdigest()
        self.assertIn(f"{digest}  crates/demo/tests/fixtures/state.json", self.lock_text())

    def test_a_changed_golden_file_fails_naming_the_file_and_the_format(self):
        (self.fixtures / "state.json").write_text('{"v": 2}\n')
        joined = self.joined()
        self.assertIn("crates/demo/tests/fixtures/state.json", joined)
        self.assertIn("changed", joined)
        self.assertIn("format 1 (Demo)", joined)
        self.assertIn("--write-lock", joined)
        self.assertEqual(self.run_check(), 1)

    def test_regenerating_the_lock_is_the_deliberate_way_to_accept_the_change(self):
        (self.fixtures / "state.json").write_text('{"v": 2}\n')
        self.assertEqual(self.run_check("--write-lock"), 0)
        self.assertEqual(self.problems(), [])

    def test_a_new_golden_file_with_no_lock_row_fails(self):
        (self.fixtures / "extra.bin").write_bytes(b"x")
        joined = self.joined()
        self.assertIn("crates/demo/tests/fixtures/extra.bin", joined)
        self.assertIn("not in the lock", joined)

    def test_a_deleted_golden_file_fails(self):
        (self.fixtures / "frame.bin").unlink()
        joined = self.joined()
        self.assertIn("crates/demo/tests/fixtures/frame.bin", joined)
        self.assertIn("no longer exists", joined)

    def test_a_fixture_no_format_names_fails(self):
        (self.root / "crates/other/tests/fixtures").mkdir(parents=True)
        (self.root / "crates/other/tests/fixtures/stray.bin").write_bytes(b"x")
        joined = self.joined()
        self.assertIn("crates/other/tests/fixtures/stray.bin", joined)
        self.assertIn("belongs to no format", joined)

    def test_snapshots_and_golden_corpus_seeds_are_golden_files_too(self):
        (self.root / "crates/demo/tests/snapshots").mkdir()
        (self.root / "crates/demo/tests/snapshots/a.snap").write_text("x")
        (self.root / "fuzz/corpus/demo").mkdir(parents=True)
        (self.root / "fuzz/corpus/demo/golden-one.seed").write_bytes(b"x")
        (self.root / "fuzz/corpus/demo/plain.seed").write_bytes(b"x")
        joined = self.joined()
        self.assertIn("crates/demo/tests/snapshots/a.snap", joined)
        self.assertIn("fuzz/corpus/demo/golden-one.seed", joined)
        self.assertNotIn("plain.seed", joined)

    def test_golden_paths_may_be_a_glob_and_are_locked(self):
        (self.root / "fuzz/corpus/demo").mkdir(parents=True)
        (self.root / "fuzz/corpus/demo/golden-one.seed").write_bytes(b"x")
        self.registry(GOLDEN_REGISTRY.replace(
            "    tag_sets:", "    golden: ['fuzz/corpus/demo/golden-*.seed']\n    tag_sets:"))
        self.assertEqual(self.run_check("--write-lock"), 0)
        self.assertIn("fuzz/corpus/demo/golden-one.seed", self.lock_text())
        (self.root / "fuzz/corpus/demo/golden-one.seed").write_bytes(b"y")
        self.assertIn("golden-one.seed", self.joined())

    def test_a_golden_pattern_that_matches_nothing_fails(self):
        self.registry(GOLDEN_REGISTRY.replace(
            "    tag_sets:", "    golden: ['fuzz/corpus/none/golden-*.seed']\n    tag_sets:"))
        self.assertIn("matches no file", self.joined())

    def test_a_readme_is_not_a_golden_file(self):
        (self.fixtures / "README.md").write_text("notes")
        self.assertEqual(self.problems(), [])
        self.assertNotIn("README.md", self.lock_text())

    def test_a_missing_lock_file_fails(self):
        (self.root / "scripts/compat/golden.lock").unlink()
        self.assertIn("golden.lock", self.joined())

    def test_a_file_two_formats_name_is_reported_with_both(self):
        two = GOLDEN_REGISTRY.replace("not_formats:", """  - id: 2
    name: Other
    class: B
    marker: none
    status: evolving
    at_1_0: stable
    fixtures: [crates/demo/tests/fixtures/state.json]
not_formats:""")
        self.registry(two)
        (self.fixtures / "state.json").write_text('{"v": 3}\n')
        joined = self.joined()
        self.assertIn("format 1 (Demo)", joined)
        self.assertIn("format 2 (Other)", joined)

    def test_the_lock_is_off_when_the_registry_names_none(self):
        self.registry(REGISTRY)
        self.assertEqual(self.problems(), [])


FROZEN_BLOCK = """    state: frozen
    frozen:
      at: v0.12.0
      digest: "{digest}"
"""


class Frozen(GoldenRegistryCase):
    def setUp(self):
        super().setUp()
        (self.root / "scripts/compat/expected.yaml").write_text(EXPECTED_CELLS)
        self.base = GOLDEN_REGISTRY.replace("    tag_sets:", "    cells: [demo]\n    fuzz: [demo_decode]\n    tag_sets:")
        (self.root / "fuzz/fuzz_targets").mkdir(parents=True)
        (self.root / "fuzz/fuzz_targets/demo_decode.rs").write_text("")
        self.registry(self.base)
        self.assertEqual(self.problems(), [])

    def digest(self):
        entry = registry_check.load(self.root)["formats"][0]
        return registry_check.entry_digest(self.root, entry)

    def freeze(self, extra="", digest=None):
        block = FROZEN_BLOCK.format(digest=digest or self.digest()) + extra
        self.registry(self.base.replace("    cells: [demo]\n", "    cells: [demo]\n" + block))

    def test_an_entry_without_a_state_is_unfrozen_and_an_unknown_state_is_refused(self):
        self.assertEqual(self.problems(), [])
        for state in ("frozn", "stable", "Frozen", "null"):
            self.registry(self.base.replace("    cells: [demo]\n", f"    cells: [demo]\n    state: {state}\n"))
            self.assertIn("unknown state", self.joined(), state)

    def test_an_explicit_unfrozen_state_is_accepted(self):
        self.registry(self.base.replace("    cells: [demo]\n", "    cells: [demo]\n    state: unfrozen\n"))
        self.assertEqual(self.problems(), [])

    def test_a_frozen_format_with_the_right_digest_passes(self):
        self.freeze()
        self.assertEqual(self.problems(), [])
        # The inventory table shows the state.
        self.assertEqual(self.run_check("--write"), 0)
        self.assertIn("frozen at v0.12.0", (self.root / "docs/reference/compatibility.md").read_text())
        self.assertEqual(self.run_check(), 0)

    def test_a_frozen_format_needs_its_release_and_its_digest(self):
        self.registry(self.base.replace("    cells: [demo]\n", "    cells: [demo]\n    state: frozen\n"))
        self.assertIn("no frozen block", self.joined())
        self.registry(self.base.replace("    cells: [demo]\n",
                                        "    cells: [demo]\n    state: frozen\n    frozen: {digest: abc}\n"))
        joined = self.joined()
        self.assertIn("frozen.at", joined)
        self.assertIn("64 hex", joined)

    def test_a_frozen_block_on_an_unfrozen_entry_is_refused(self):
        self.registry(self.base.replace(
            "    cells: [demo]\n",
            "    cells: [demo]\n    state: unfrozen\n    frozen: {at: v0.12.0, digest: %s}\n" % ("a" * 64)))
        self.assertIn("is not frozen but has a frozen block", self.joined())

    def test_a_changed_golden_file_of_a_frozen_format_needs_a_recorded_exception(self):
        self.freeze()
        (self.fixtures / "state.json").write_text('{"v": 2}\n')
        self.assertEqual(self.run_check("--write-lock"), 1)
        joined = self.joined()
        self.assertIn("format 1 (Demo) is frozen at v0.12.0", joined)
        self.assertIn("frozen.exceptions", joined)
        # The message hands over the digest to record.
        self.assertIn(self.digest(), joined)

    def test_a_version_bump_of_a_frozen_format_needs_a_recorded_exception(self):
        self.freeze()
        self.source(SOURCE.replace("DEMO_VERSION: u32 = 1", "DEMO_VERSION: u32 = 2"))
        self.registry(self.registry_text().replace("name: DEMO_VERSION, value: '1'", "name: DEMO_VERSION, value: '2'"))
        self.assertIn("is frozen at v0.12.0", self.joined())

    def test_a_new_tag_in_a_frozen_field_list_needs_a_recorded_exception(self):
        self.freeze()
        self.source(SOURCE.replace("const MAX_DEMO", "const COMMAND_KIND_THIRD: u16 = 3;\nconst MAX_DEMO"))
        self.registry(self.registry_text().replace(
            "COMMAND_KIND_SECOND: 2}", "COMMAND_KIND_SECOND: 2, COMMAND_KIND_THIRD: 3}"))
        self.assertIn("is frozen at v0.12.0", self.joined())

    def test_a_new_golden_file_under_a_frozen_format_needs_a_recorded_exception(self):
        self.freeze()
        (self.fixtures / "later.bin").write_bytes(b"x")
        self.assertEqual(self.run_check("--write-lock"), 1)
        self.assertIn("is frozen at v0.12.0", self.joined())

    def test_a_recorded_exception_lets_the_change_through_once(self):
        self.freeze()
        (self.fixtures / "state.json").write_text('{"v": 2}\n')
        self.assertEqual(self.run_check("--write-lock"), 1)
        new = self.digest()
        self.freeze(extra=f"      exceptions:\n        - {{digest: \"{new}\", ref: \"#901\", why: the field is renamed}}\n",
                    digest=registry_check.load(self.root)["formats"][0]["frozen"]["digest"])
        self.assertEqual(self.problems(), [])

    def test_an_exception_needs_a_digest_a_reference_and_a_reason(self):
        self.freeze(extra="      exceptions:\n        - {digest: abc}\n")
        joined = self.joined()
        self.assertIn("64 hex", joined)
        self.assertIn("needs a ref", joined)
        self.assertIn("needs a why", joined)

    def test_only_the_latest_exception_is_current(self):
        self.freeze()
        first = self.digest()
        (self.fixtures / "state.json").write_text('{"v": 2}\n')
        self.assertEqual(self.run_check("--write-lock"), 1)
        second = self.digest()
        original = registry_check.load(self.root)["formats"][0]["frozen"]["digest"]
        rows = (f"      exceptions:\n        - {{digest: \"{first}\", ref: \"#1\", why: a}}\n"
                f"        - {{digest: \"{second}\", ref: \"#2\", why: b}}\n")
        self.freeze(extra=rows, digest=original)
        self.assertEqual(self.problems(), [])
        # Going back to the first state is a change too: the latest row is the one that counts.
        (self.fixtures / "state.json").write_text('{"v": 1}\n')
        self.assertEqual(self.run_check("--write-lock"), 1)
        self.assertIn("is frozen at v0.12.0", self.joined())

    def test_the_digest_command_prints_what_to_record(self):
        import contextlib
        import io
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(self.run_check("--digest", "1"), 0)
        self.assertEqual(out.getvalue().strip(), self.digest())
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.run_check("--digest", "99"), 1)

    def test_the_digest_follows_constants_tags_and_golden_bytes_not_the_prose(self):
        before = self.digest()
        self.registry(self.base.replace("status: evolving", "status: stable").replace("marker:", "marker: x #"))
        self.assertEqual(self.digest(), before)
        (self.fixtures / "state.json").write_text('{"v": 9}\n')
        self.assertNotEqual(self.digest(), before)

    def test_a_frozen_format_needs_golden_files_cells_both_ways_and_fuzz_or_a_recorded_waiver(self):
        no_cells = self.base.replace("    cells: [demo]\n", "").replace("    fuzz: [demo_decode]\n", "")
        (self.root / "scripts/compat/expected.yaml").write_text("cells: {}\n")
        self.registry(no_cells)
        digest = self.digest()
        self.registry(no_cells.replace("    tag_sets:", FROZEN_BLOCK.format(digest=digest) + "    tag_sets:"))
        joined = self.joined()
        self.assertIn("upgrade and forward cells", joined)
        self.assertIn("no fuzz target", joined)
        waived = FROZEN_BLOCK.format(digest=digest) + "      waive: {cells: a wire format has no released binary, fuzz: a text format}\n"
        self.registry(no_cells.replace("    tag_sets:", waived + "    tag_sets:"))
        self.assertEqual(self.problems(), [])

    def test_a_frozen_format_with_no_golden_files_is_refused(self):
        bare = REGISTRY.replace("    tag_sets:", "    cells: [demo]\n    fuzz: [demo_decode]\n    tag_sets:")
        bare = bare + "golden_lock: scripts/compat/golden.lock\n"
        for file in self.fixtures.iterdir():
            file.unlink()
        self.registry(bare)
        self.assertEqual(self.run_check("--write-lock"), 0)
        digest = self.digest()
        self.registry(bare.replace("    tag_sets:", FROZEN_BLOCK.format(digest=digest) + "    tag_sets:"))
        self.assertIn("no golden files", self.joined())

    def registry_text(self):
        return (self.root / "scripts/compat/formats.yaml").read_text()


class FrozenHistory(unittest.TestCase):
    """--base: a frozen format cannot be un-frozen, re-dated or re-digested
    by editing the registry alone."""

    def reg(self, **frozen):
        entry = {"id": 1, "name": "Demo", "state": "frozen",
                 "frozen": dict({"at": "v0.12.0", "digest": "a" * 64}, **frozen)}
        return {"formats": [entry]}

    def test_the_same_registry_passes(self):
        self.assertEqual(registry_check.check_history(self.reg(), self.reg()), [])

    def test_freezing_an_unfrozen_format_is_allowed(self):
        base = {"formats": [{"id": 1, "name": "Demo"}]}
        self.assertEqual(registry_check.check_history(base, self.reg()), [])

    def test_unfreezing_is_refused(self):
        head = {"formats": [{"id": 1, "name": "Demo", "state": "unfrozen"}]}
        self.assertIn("was frozen at v0.12.0 and is not any more", " ".join(registry_check.check_history(self.reg(), head)))

    def test_dropping_a_frozen_format_is_refused(self):
        self.assertIn("format 1 (Demo) was frozen", " ".join(registry_check.check_history(self.reg(), {"formats": []})))

    def test_changing_the_release_or_the_frozen_digest_is_refused(self):
        joined = " ".join(registry_check.check_history(self.reg(), self.reg(at="v0.13.0", digest="b" * 64)))
        self.assertIn("frozen.at", joined)
        self.assertIn("frozen.digest", joined)

    def test_exceptions_can_be_added_but_not_removed_or_rewritten(self):
        row = {"digest": "c" * 64, "ref": "#1", "why": "x"}
        base = self.reg(exceptions=[row])
        self.assertEqual(registry_check.check_history(base, self.reg(exceptions=[row, dict(row, digest="d" * 64)])), [])
        self.assertIn("exceptions", " ".join(registry_check.check_history(base, self.reg())))
        self.assertIn("exceptions", " ".join(registry_check.check_history(base, self.reg(exceptions=[dict(row, why="y")]))))


class FrozenHistoryFromGit(RegistryCase):
    def git(self, *args):
        import subprocess
        subprocess.run(["git", "-C", str(self.root), "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
                       check=True, capture_output=True)

    def test_base_reads_the_registry_at_a_revision_and_refuses_an_unfreeze(self):
        digest = "a" * 64
        frozen = REGISTRY.replace("    tag_sets:", FROZEN_BLOCK.format(digest=digest) + "    tag_sets:")
        self.git("init", "-q")
        (self.root / "scripts/compat/formats.yaml").write_text(frozen)
        self.git("add", "-A")
        self.git("commit", "-q", "-m", "base")
        (self.root / "scripts/compat/formats.yaml").write_text(REGISTRY)
        import contextlib
        import io
        with contextlib.redirect_stderr(io.StringIO()) as err:
            self.assertEqual(self.run_check("--base", "HEAD"), 1)
        self.assertIn("was frozen at v0.12.0 and is not any more", err.getvalue())

    def test_a_base_with_no_registry_yet_is_skipped_and_a_bad_revision_fails(self):
        self.git("init", "-q")
        (self.root / "README").write_text("x")
        self.git("add", "README")
        self.git("commit", "-q", "-m", "base")
        self.assertEqual(self.run_check("--base", "HEAD"), 0)
        import contextlib
        import io
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.run_check("--base", "no-such-rev"), 1)


class TheRepository(unittest.TestCase):
    def test_registry_names_every_format(self):
        self.assertEqual(registry_check.check_source(ROOT, registry_check.load(ROOT)), [])

    def test_every_golden_file_is_locked_and_every_state_is_known(self):
        registry = registry_check.load(ROOT)
        self.assertEqual(registry["golden_lock"], "scripts/compat/golden.lock")
        for entry in registry["formats"]:
            self.assertIn(entry.get("state", "unfrozen"), registry_check.STATES)

    def test_the_page_table_is_what_the_registry_generates(self):
        self.assertEqual(registry_check.main(["--root", str(ROOT)]), 0)


if __name__ == "__main__":
    unittest.main()
