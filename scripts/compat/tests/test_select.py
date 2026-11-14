"""Tests for scripts/compat/select.py (#743): which releases each cell runs,
and what each is expected to give.

Run with: python3 -m unittest discover -s scripts/compat/tests
"""

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import select_cells as select  # noqa: E402

TAGS = [
    "v0.1.0", "v0.2.0", "v0.2.1", "v0.3.0", "v0.3.1", "v0.4.0", "v0.5.0",
    "v0.6.0", "v0.7.0", "v0.8.0", "v0.9.0", "v0.9.1-rc.1", "v0.10.0-rc.1",
    "not-a-tag",
]

EXPECTED = {
    "cells": {
        "manifest": {"since": "v0.7.0", "directions": ["forward"]},
        "ledger-sqlite": {"since": "v0.7.0", "directions": ["upgrade", "forward"]},
    },
    "built_from_source": [],
    "exceptions": [
        {
            "cell": "manifest",
            "direction": "forward",
            "older_than": "v0.8.0",
            "expect": "hash-failure",
            "why": "the documented cutoff",
        },
    ],
}


def rows(matrix):
    return sorted((c["cell"], c["tag"], c["direction"], c["expect"]) for c in matrix)


class Releases(unittest.TestCase):
    def test_finals_only_and_the_newest_patch_of_each_minor(self):
        self.assertEqual(
            select.releases(TAGS),
            ["v0.1.0", "v0.2.1", "v0.3.1", "v0.4.0", "v0.5.0", "v0.6.0",
             "v0.7.0", "v0.8.0", "v0.9.0"],
        )

    def test_a_new_patch_replaces_its_minor_the_day_it_is_tagged(self):
        self.assertEqual(select.releases(["v0.8.0", "v0.8.1", "v0.9.0"]), ["v0.8.1", "v0.9.0"])

    def test_versions_compare_as_numbers_not_text(self):
        self.assertEqual(select.releases(["v0.9.0", "v0.10.0", "v1.0.0"]), ["v0.9.0", "v0.10.0", "v1.0.0"])


class Matrix(unittest.TestCase):
    def test_the_full_matrix_runs_every_release_since_each_cell_applies(self):
        self.assertEqual(
            rows(select.matrix(EXPECTED, TAGS, "full")),
            [
                ("ledger-sqlite", "v0.7.0", "forward", "pass"),
                ("ledger-sqlite", "v0.7.0", "upgrade", "pass"),
                ("ledger-sqlite", "v0.8.0", "forward", "pass"),
                ("ledger-sqlite", "v0.8.0", "upgrade", "pass"),
                ("ledger-sqlite", "v0.9.0", "forward", "pass"),
                ("ledger-sqlite", "v0.9.0", "upgrade", "pass"),
                ("manifest", "v0.7.0", "forward", "hash-failure"),
                ("manifest", "v0.8.0", "forward", "pass"),
                ("manifest", "v0.9.0", "forward", "pass"),
            ],
        )

    def test_a_pull_request_runs_the_previous_minor_and_every_exception(self):
        # The documented cutoffs stay checked on every pull request: a
        # refusal that silently became a pass, or the reverse, is the change
        # a reviewer most needs to see.
        self.assertEqual(
            rows(select.matrix(EXPECTED, TAGS, "pr")),
            [
                ("ledger-sqlite", "v0.9.0", "forward", "pass"),
                ("ledger-sqlite", "v0.9.0", "upgrade", "pass"),
                ("manifest", "v0.7.0", "forward", "hash-failure"),
                ("manifest", "v0.9.0", "forward", "pass"),
            ],
        )

    def test_an_exact_tag_exception(self):
        expected = dict(EXPECTED, exceptions=[
            {"cell": "ledger-sqlite", "direction": "forward", "tags": ["v0.8.0"],
             "expect": "refuse", "message": "newer than this release", "why": "x"},
        ])
        got = {(c["tag"], c["direction"]): c for c in select.matrix(expected, TAGS, "full")
               if c["cell"] == "ledger-sqlite"}
        self.assertEqual(got[("v0.8.0", "forward")]["expect"], "refuse")
        self.assertEqual(got[("v0.8.0", "forward")]["message"], "newer than this release")
        self.assertEqual(got[("v0.8.0", "upgrade")]["expect"], "pass")
        self.assertEqual(got[("v0.9.0", "forward")]["expect"], "pass")

    def test_a_tag_built_from_source_runs_no_released_binary_cell(self):
        expected = dict(EXPECTED, built_from_source=[{"tag": "v0.8.0", "why": "x"}])
        self.assertNotIn("v0.8.0", {c["tag"] for c in select.matrix(expected, TAGS, "full")})

    def test_every_cell_says_where_it_came_from(self):
        for cell in select.matrix(EXPECTED, TAGS, "full"):
            self.assertEqual(cell["source"], "release")


class Validation(unittest.TestCase):
    def check_refused(self, expected, needle):
        with self.assertRaises(select.ExpectedError) as caught:
            select.matrix(expected, TAGS, "full")
        self.assertIn(needle, str(caught.exception))

    def test_an_exception_for_an_unknown_cell_is_refused(self):
        self.check_refused(
            dict(EXPECTED, exceptions=[{"cell": "nope", "direction": "forward",
                                        "tags": ["v0.9.0"], "expect": "refuse",
                                        "message": "m", "why": "x"}]),
            "nope",
        )

    def test_an_exception_needs_a_reason(self):
        self.check_refused(
            dict(EXPECTED, exceptions=[{"cell": "manifest", "direction": "forward",
                                        "tags": ["v0.9.0"], "expect": "hash-failure"}]),
            "why",
        )

    def test_a_refusal_must_say_which_message(self):
        self.check_refused(
            dict(EXPECTED, exceptions=[{"cell": "manifest", "direction": "forward",
                                        "tags": ["v0.9.0"], "expect": "refuse", "why": "x"}]),
            "message",
        )

    def test_an_exception_matching_no_release_is_refused(self):
        # A stale row would otherwise sit unnoticed, claiming a cutoff that
        # nothing checks.
        self.check_refused(
            dict(EXPECTED, exceptions=[{"cell": "manifest", "direction": "forward",
                                        "tags": ["v0.4.0"], "expect": "hash-failure",
                                        "why": "x"}]),
            "matches no",
        )

    def test_two_exceptions_for_one_cell_are_refused(self):
        row = {"cell": "manifest", "direction": "forward", "tags": ["v0.7.0"],
               "expect": "hash-failure", "why": "x"}
        self.check_refused(dict(EXPECTED, exceptions=[row, EXPECTED["exceptions"][0]]), "twice")

    def test_an_unknown_direction_is_refused(self):
        self.check_refused(
            dict(EXPECTED, cells={"manifest": {"since": "v0.7.0", "directions": ["sideways"]}},
                 exceptions=[]),
            "sideways",
        )


class TheCommittedFile(unittest.TestCase):
    def test_expected_yaml_is_valid_against_the_real_tags(self):
        expected = select.load(select.DEFAULT_EXPECTED)
        full = select.matrix(expected, TAGS, "full")
        self.assertTrue(full)
        manifest = {c["tag"]: c["expect"] for c in full if c["cell"] == "manifest"}
        # Unchanged from the manifest-compat job this replaces.
        self.assertEqual(manifest, {"v0.7.0": "hash-failure", "v0.8.0": "pass", "v0.9.0": "pass"})


if __name__ == "__main__":
    unittest.main()
