"""Tests for scripts/config-keys-since.py.

Each test builds a throwaway git repository from the fixture trees in
scripts/tests/fixtures/config-keys (v1 is a release, v2 the next one), tags
both, and runs the script between the tags. A last group reads the real
config.rs of this repository. Run from the repository root:

    python3 -m unittest discover -s scripts/tests -p 'test_config_keys_since.py' -v
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "config-keys-since.py"
REPO = SCRIPT.parent.parent
FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures" / "config-keys"


def load():
    spec = importlib.util.spec_from_file_location("config_keys_since", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


MOD = load()


def git(repo: pathlib.Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.invalid", *args],
        check=True,
        capture_output=True,
    )


def lay_out(repo: pathlib.Path, version: str) -> None:
    for source, target in (
        ("config.rs", MOD.CONFIG_RS),
        ("reference.md", MOD.REFERENCE_MD),
        ("streams.md", MOD.STREAMS_MD),
    ):
        path = repo / target
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(FIXTURES / version / source, path)


def run(repo: pathlib.Path, *args: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    code = 0
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = MOD.main([*args, "--repo", str(repo)])
        except SystemExit as exit_:
            code = exit_.code if isinstance(exit_.code, int) else 1
            if isinstance(exit_.code, str):
                err.write(exit_.code)
    return code, out.getvalue(), err.getvalue()


class FixtureRefs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.repo = pathlib.Path(cls.tmp.name)
        git(cls.repo, "init", "-q")
        lay_out(cls.repo, "v1")
        git(cls.repo, "add", "-A")
        git(cls.repo, "commit", "-q", "-m", "release one")
        git(cls.repo, "tag", "v1")
        lay_out(cls.repo, "v2")
        git(cls.repo, "add", "-A")
        git(cls.repo, "commit", "-q", "-m", "release two")
        git(cls.repo, "tag", "v2")
        code, out, _err = run(cls.repo, "v1", "v2", "--json")
        assert code == 0, out
        cls.added = {item["key"]: item for item in json.loads(out)}

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_lists_exactly_the_new_keys(self):
        self.assertEqual(
            sorted(self.added),
            sorted(
                [
                    "engine.work_cap",
                    "engine.pinned_zone",
                    "engine.region_id",
                    "engine.labelName",
                    "engine.audit_sink",
                    "security.new_toggle",
                    "upload.lock",
                    "upload.transports.<name>.burst",
                    "streams[].label",
                ]
            ),
        )

    def test_a_key_in_a_section_that_rejects_unknown_keys_is_refused(self):
        item = self.added["security.new_toggle"]
        self.assertEqual(item["kind"], "refused")
        self.assertIn("Release-gating", item["note"])

    def test_a_key_with_a_default_is_harmless_while_unset(self):
        item = self.added["engine.work_cap"]
        self.assertEqual(item["kind"], "default")
        self.assertIn("harmless while unset", item["note"])
        self.assertIn("`5`", item["note"])

    def test_a_default_in_the_per_transport_section_is_found(self):
        # The reference documents `upload.transports`, the schema has
        # `upload.transports.<name>`.
        self.assertEqual(self.added["upload.transports.<name>.burst"]["kind"], "default")

    def test_the_sentence_of_the_reference_about_older_engines_is_quoted(self):
        item = self.added["engine.audit_sink"]
        self.assertEqual(item["kind"], "reference")
        self.assertIn("An engine before this release ignores the key and writes no audit records.", item["note"])

    def test_a_block_is_listed_once_with_the_keys_inside_and_the_blocks_sentence(self):
        item = self.added["upload.lock"]
        self.assertEqual(item["kind"], "reference")
        self.assertEqual(sorted(item["inside"]), ["upload.lock.days", "upload.lock.mode"])
        # A link in the sentence is written as its text, not as markdown.
        self.assertIn("An engine older than v2 ignores the block and writes unlocked objects, so read this first.", item["note"])
        self.assertNotIn("](", item["note"])
        self.assertNotIn("upload.lock.mode", self.added)

    def test_a_key_the_reference_does_not_speak_of_is_left_for_a_decision(self):
        item = self.added["engine.pinned_zone"]
        self.assertEqual(item["kind"], "undecided")
        self.assertIn("decide before release", item["note"])

    def test_a_key_with_no_default_is_marked_required(self):
        self.assertTrue(self.added["engine.region_id"]["required"])
        self.assertFalse(self.added["engine.work_cap"]["required"])
        self.assertFalse(self.added["engine.pinned_zone"]["required"])

    def test_a_renamed_field_is_listed_under_its_yaml_name_and_a_skipped_one_not_at_all(self):
        self.assertIn("engine.labelName", self.added)
        self.assertNotIn("engine.label_name", self.added)
        self.assertNotIn("engine.resolved_zone", self.added)
        self.assertNotIn("configured_fields", self.added)

    def test_a_streams_key_is_listed_under_its_entry(self):
        self.assertEqual(self.added["streams[].label"]["kind"], "undecided")

    def test_markdown_names_the_older_ref_and_every_key(self):
        code, out, _err = run(self.repo, "v1", "v2")
        self.assertEqual(code, 0)
        self.assertTrue(out.startswith("### Config keys an engine older than v1 ignores\n"))
        for key in self.added:
            self.assertIn(f"- `{key}`:", out)
        self.assertIn("Required by this build", out)
        self.assertIn("New block; keys inside: `days`, `mode`.", out)

    def test_the_older_refs_strictness_decides_not_the_newer(self):
        # security was strict in v1 as well. Swap the refs: nothing is new.
        code, out, _err = run(self.repo, "v2", "v1", "--json")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), [])

    def test_no_new_key_says_so(self):
        code, out, _err = run(self.repo, "v2", "v2")
        self.assertEqual(code, 0)
        self.assertIn("No `config.yaml` or `streams.yaml` key was added since v2.", out)

    def test_a_ref_that_is_not_there_fails(self):
        code, _out, err = run(self.repo, "v0", "v2")
        self.assertNotEqual(code, 0)
        self.assertIn("v0 is not a commit", err)

    def test_a_ref_without_the_config_source_fails(self):
        empty = pathlib.Path(self.tmp.name) / "empty"
        empty.mkdir()
        git(empty, "init", "-q")
        (empty / "README").write_text("x")
        git(empty, "add", "-A")
        git(empty, "commit", "-q", "-m", "nothing")
        code, _out, err = run(empty, "HEAD", "HEAD")
        self.assertNotEqual(code, 0)
        self.assertIn("is not in", err)


class StrictnessInNestedBlocks(unittest.TestCase):
    """A new key inside a new block is the block's, whatever the block holds."""

    SOURCE = """
pub struct VtopConfig {
    pub engine: EngineConfig,
}

pub struct EngineConfig {
    pub name: String,
}
"""
    NEWER = """
pub struct VtopConfig {
    pub engine: EngineConfig,
    #[serde(default)]
    pub extra: ExtraConfig,
}

pub struct EngineConfig {
    pub name: String,
}

#[serde(deny_unknown_fields)]
pub struct ExtraConfig {
    #[serde(default)]
    pub flag: bool,
}
"""

    def test_a_new_strict_block_is_ignored_whole_by_an_older_engine(self):
        added = MOD.added_keys(self.SOURCE, self.NEWER, {})
        self.assertEqual([a.path for a in added], ["extra"])
        # `extra` sits in the root, which has no deny_unknown_fields.
        self.assertNotEqual(added[0].note_kind, "refused")
        self.assertEqual(added[0].inside, ["extra.flag"])

    def test_a_key_added_later_to_that_block_is_refused(self):
        newest = self.NEWER.replace(
            "    pub flag: bool,\n", "    pub flag: bool,\n    #[serde(default)]\n    pub more: bool,\n"
        )
        added = MOD.added_keys(self.NEWER, newest, {})
        self.assertEqual([(a.path, a.note_kind) for a in added], [("extra.more", "refused")])


class RealSchema(unittest.TestCase):
    """The parser against this repository's config.rs."""

    @classmethod
    def setUpClass(cls):
        source = (REPO / MOD.CONFIG_RS).read_text(encoding="utf-8")
        cls.keys = MOD.schema(source)

    def test_known_keys_are_found_under_their_paths(self):
        for path in (
            "engine.state_store",
            "batching.adaptive_width.enabled",
            "manifest.mac.keys[].id",
            "upload.object_lock.retain_days",
            "upload.transports.<name>.part_size_bytes",
            "sources.file.delete_quiet_seconds",
            "security.allow_loose_secret_permissions",
            "streams",
            "streams[].source_name",
            "streams[].checksum.algorithm",
        ):
            self.assertIn(path, self.keys)

    def test_a_skipped_field_is_not_a_key(self):
        self.assertNotIn("configured_fields", self.keys)

    def test_every_key_check_config_reference_documents_is_found(self):
        spec = importlib.util.spec_from_file_location(
            "check_config_reference", REPO / "scripts" / "check-config-reference.py"
        )
        checker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(checker)
        source = (REPO / MOD.CONFIG_RS).read_text(encoding="utf-8")
        leaves = {path.rsplit(".", 1)[-1].replace("[]", "") for path in self.keys}
        for struct in checker.STRUCTS:
            for name in checker.fields(source, struct):
                self.assertIn(name, leaves, f"{struct}.{name}")

    def test_the_sections_that_reject_unknown_keys_are_the_ones_the_reference_names(self):
        strict = MOD.strict_sections((REPO / MOD.CONFIG_RS).read_text(encoding="utf-8"))
        for name in ("SecurityConfig", "ManifestMacConfig", "ManifestKeyConfig", "ObjectLockConfig", "EgressTuning"):
            self.assertIn(name, strict)
        self.assertNotIn("UploadConfig", strict)

    def test_no_key_is_new_between_a_ref_and_itself(self):
        code, out, _err = run(REPO, "HEAD", "HEAD", "--json")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out), [])


if __name__ == "__main__":
    unittest.main()
