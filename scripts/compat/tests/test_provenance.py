"""Tests for scripts/compat/check_provenance.py (#743): a fixture that claims
to come from a released binary must say which, and the claim must check out
against that release's SHA256SUMS and the fixture's own bytes.

Run with: python3 -m unittest discover -s scripts/compat/tests
"""

import hashlib
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import check_provenance as prov  # noqa: E402

ASSET = "vtop-engine-0.9.0-x86_64-unknown-linux-gnu.tar.gz"
ASSET_SHA = "ab" * 32


def sums(tag):
    if tag != "v0.9.0":
        raise prov.ProvenanceError(f"no release {tag}")
    return f"{ASSET_SHA}  ./{ASSET}\n{'cd' * 32}  ./SHA256SUMS-not-really\n"


class Provenance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.fixture = self.root / "crates/vtop-state/tests/fixtures/ledger-sqlite/v0.9.0"
        self.fixture.mkdir(parents=True)
        (self.fixture / "ledger.db").write_bytes(b"sqlite bytes")

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, **overrides):
        fields = {
            "tag": "v0.9.0",
            "asset": ASSET,
            "asset_sha256": ASSET_SHA,
            "binary": "vtopctl",
            "command": "vtopctl process-once --source file --config config.yaml",
            "input": "three lines in input.log",
            "files": {"ledger.db": hashlib.sha256(b"sqlite bytes").hexdigest()},
        }
        fields.update(overrides)
        fields = {k: v for k, v in fields.items() if v is not None}
        import yaml

        (self.fixture / "PROVENANCE").write_text(yaml.safe_dump(fields))

    def problems(self):
        return prov.check(self.root, sums)

    def test_a_true_provenance_passes(self):
        self.write()
        self.assertEqual(self.problems(), [])

    def test_a_tag_directory_without_provenance_fails(self):
        problems = self.problems()
        self.assertEqual(len(problems), 1)
        self.assertIn("no PROVENANCE", problems[0])

    def test_an_asset_digest_the_release_does_not_list_fails(self):
        self.write(asset_sha256="ef" * 32)
        self.assertIn("SHA256SUMS", " ".join(self.problems()))

    def test_an_asset_of_another_release_fails(self):
        self.write(asset="vtop-engine-0.8.0-x86_64-unknown-linux-gnu.tar.gz")
        self.assertIn("not an archive of v0.9.0", " ".join(self.problems()))

    def test_a_tag_that_does_not_match_its_directory_fails(self):
        self.write(tag="v0.8.0")
        self.assertIn("directory says v0.9.0", " ".join(self.problems()))

    def test_a_missing_field_fails(self):
        self.write(command=None)
        self.assertIn("command", " ".join(self.problems()))

    def test_an_edited_fixture_file_fails(self):
        self.write()
        (self.fixture / "ledger.db").write_bytes(b"edited by hand")
        self.assertIn("ledger.db", " ".join(self.problems()))

    def test_a_fixture_file_the_provenance_does_not_list_fails(self):
        self.write()
        (self.fixture / "extra.json").write_text("{}")
        self.assertIn("extra.json", " ".join(self.problems()))

    def test_a_listed_file_that_is_missing_fails(self):
        self.write(files={"ledger.db": hashlib.sha256(b"sqlite bytes").hexdigest(),
                          "gone.db": "00" * 32})
        self.assertIn("gone.db", " ".join(self.problems()))

    def test_an_asset_listed_twice_fails(self):
        self.write()
        twice = lambda tag: sums(tag) + f"{ASSET_SHA}  ./{ASSET}\n"
        self.assertIn("more than once", " ".join(prov.check(self.root, twice)))

    def test_a_files_entry_outside_the_fixture_fails(self):
        self.write(files={"ledger.db": hashlib.sha256(b"sqlite bytes").hexdigest(),
                          "../../../../../../outside": "00" * 32})
        self.assertIn("outside its fixture directory", " ".join(self.problems()))

    def test_a_files_key_that_is_not_a_path_fails(self):
        self.write(files={1: "00" * 32})
        self.assertIn("path string", " ".join(self.problems()))

    def test_directories_that_are_not_release_tags_are_not_fixtures_of_a_release(self):
        other = self.root / "crates/vtop-core/tests/fixtures/manifests"
        other.mkdir(parents=True)
        (other / "v0.4.json").write_text("{}")
        self.write()
        self.assertEqual(self.problems(), [])


if __name__ == "__main__":
    unittest.main()
