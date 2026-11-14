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
          - {file: crates/demo/src/lib.rs, prefix: COMMAND_KIND_, max: 2}
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
        self.source(SOURCE.replace("const MAX_DEMO", "const COMMAND_KIND_THIRD: u16 = 3;\nconst MAX_DEMO"))
        self.assertIn("highest COMMAND_KIND_* in crates/demo/src/lib.rs is 3", " ".join(self.problems()))

    def test_a_constant_the_sources_no_longer_hold_fails(self):
        self.source(SOURCE.replace("pub const DEMO_VERSION: u32 = 1;\n", ""))
        self.assertIn("no longer hold", " ".join(self.problems()))

    def test_test_modules_are_not_scanned(self):
        joined = " ".join(self.problems())
        self.assertNotIn("TEST_VERSION", joined)
        self.assertNotIn("VTOPELSE", joined)

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
