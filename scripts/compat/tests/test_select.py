"""Tests for scripts/compat/select_cells.py (#743): which releases each cell runs,
and what each is expected to give.

Run with: python3 -m unittest discover -s scripts/compat/tests
"""

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import select_cells as select  # noqa: E402
import yaml  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[3]
FORMATS = ROOT / "scripts/compat/formats.yaml"
META_FIXTURES = ROOT / "crates/vtop-meta/tests/fixtures/meta"

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


class PublishedReleases(unittest.TestCase):
    def test_drafts_and_prereleases_are_not_selected(self):
        listed = [
            {"tagName": "v0.11.0", "isDraft": False, "isPrerelease": True},
            {"tagName": "v0.10.3", "isDraft": True, "isPrerelease": False},
            {"tagName": "v0.10.2", "isDraft": False, "isPrerelease": False},
            {"tagName": "v0.9.0", "isDraft": False, "isPrerelease": False},
        ]
        self.assertEqual(select.published_tags(listed), ["v0.10.2", "v0.9.0"])

    def test_a_final_looking_prerelease_is_not_the_newest_minor(self):
        listed = [
            {"tagName": tag, "isDraft": False, "isPrerelease": False} for tag in TAGS
        ] + [{"tagName": "v0.11.0", "isDraft": False, "isPrerelease": True}]
        tags = select.published_tags(listed)
        self.assertNotIn("v0.11.0", tags)
        self.assertEqual(select.releases(tags)[-1], "v0.9.0")


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

    def test_a_pull_request_runs_the_newest_minor_and_each_cutoffs_boundary(self):
        # A documented cutoff stays checked on every pull request, on both
        # sides: the newest release it names and the first one after it. A
        # refusal that silently became a pass, or the first passing reader
        # starting to fail, is the change a reviewer most needs to see. The
        # cost stays at two tags per exception row, whatever its range.
        self.assertEqual(
            rows(select.matrix(EXPECTED, TAGS, "pr")),
            [
                ("ledger-sqlite", "v0.9.0", "forward", "pass"),
                ("ledger-sqlite", "v0.9.0", "upgrade", "pass"),
                ("manifest", "v0.7.0", "forward", "hash-failure"),
                ("manifest", "v0.8.0", "forward", "pass"),
                ("manifest", "v0.9.0", "forward", "pass"),
            ],
        )

    def test_a_wide_exception_adds_only_its_boundary_to_a_pull_request(self):
        expected = dict(EXPECTED, exceptions=[
            {"cell": "ledger-sqlite", "direction": "forward", "older_than": "v0.9.0",
             "expect": "refuse", "message": "newer", "why": "x"},
        ])
        got = rows(select.matrix(expected, TAGS, "pr"))
        self.assertEqual(
            [r for r in got if r[0] == "ledger-sqlite"],
            [
                ("ledger-sqlite", "v0.8.0", "forward", "refuse"),
                ("ledger-sqlite", "v0.9.0", "forward", "pass"),
                ("ledger-sqlite", "v0.9.0", "upgrade", "pass"),
            ],
        )

    def test_an_only_newest_cell_runs_the_newest_release_alone_on_a_pull_request(self):
        cells = dict(EXPECTED["cells"], ledger={"since": "v0.7.0", "directions": ["forward"],
                                                "only_newest": True})
        expected = dict(EXPECTED, cells=cells)
        got = [r for r in rows(select.matrix(expected, TAGS, "pr")) if r[0] == "ledger"]
        self.assertEqual(got, [("ledger", "v0.9.0", "forward", "pass")])
        # A newer release replaces it the day it is published.
        got = [r for r in rows(select.matrix(expected, TAGS + ["v0.10.0"], "pr")) if r[0] == "ledger"]
        self.assertEqual(got, [("ledger", "v0.10.0", "forward", "pass")])

    def test_the_nightly_run_adds_n_minus_2_to_an_only_newest_cell(self):
        # The nightly matrix covers the two previous minors, N-1 and N-2, of
        # every cell, also of one a pull request runs on the newest alone.
        cells = dict(EXPECTED["cells"], ledger={"since": "v0.7.0", "directions": ["forward"],
                                                "only_newest": True})
        expected = dict(EXPECTED, cells=cells)
        got = [r for r in rows(select.matrix(expected, TAGS, "full")) if r[0] == "ledger"]
        self.assertEqual(got, [("ledger", "v0.8.0", "forward", "pass"), ("ledger", "v0.9.0", "forward", "pass")])
        got = [r for r in rows(select.matrix(expected, TAGS + ["v0.10.0"], "full")) if r[0] == "ledger"]
        self.assertEqual(got, [("ledger", "v0.10.0", "forward", "pass"), ("ledger", "v0.9.0", "forward", "pass")])

    def test_an_n_minus_2_that_predates_the_cell_is_not_invented(self):
        cells = dict(EXPECTED["cells"], young={"since": "v0.9.0", "directions": ["forward"], "only_newest": True})
        got = [r for r in rows(select.matrix(dict(EXPECTED, cells=cells), TAGS, "full")) if r[0] == "young"]
        self.assertEqual(got, [("young", "v0.9.0", "forward", "pass")])

    def test_every_cell_says_how_many_minors_behind_the_newest_release_it_runs(self):
        got = {(c["cell"], c["tag"]): c["age"] for c in select.matrix(EXPECTED, TAGS, "full")}
        self.assertEqual(got[("ledger-sqlite", "v0.9.0")], "N-1")
        self.assertEqual(got[("ledger-sqlite", "v0.8.0")], "N-2")
        self.assertEqual(got[("ledger-sqlite", "v0.7.0")], "N-3")
        # Newer releases move the labels the day they are tagged.
        got = {(c["cell"], c["tag"]): c["age"] for c in select.matrix(EXPECTED, TAGS + ["v0.10.0"], "pr")}
        self.assertEqual(got[("ledger-sqlite", "v0.10.0")], "N-1")

    def test_the_nightly_run_has_n_minus_1_and_n_minus_2_for_every_cell_and_direction(self):
        full = select.matrix(select.load(select.DEFAULT_EXPECTED), TAGS + ["v0.10.0", "v0.10.4"], "full")
        for name in {c["cell"] for c in full}:
            for direction in {c["direction"] for c in full if c["cell"] == name}:
                ages = {c["age"] for c in full if c["cell"] == name and c["direction"] == direction}
                self.assertTrue({"N-1", "N-2"} <= ages, (name, direction, ages))

    def test_a_pull_request_stays_on_n_minus_1_apart_from_the_exception_boundaries(self):
        pr = select.matrix(EXPECTED, TAGS + ["v0.10.0"], "pr")
        self.assertEqual({c["age"] for c in pr if c["cell"] == "ledger-sqlite"}, {"N-1"})

    def test_an_upgrade_cell_cannot_be_limited_to_the_newest_release(self):
        # N-2 to N is supported for state at rest, so what N-2 wrote must be
        # opened by this build: an upgrade cell may not skip it.
        for only in (True, ["upgrade"]):
            cells = dict(EXPECTED["cells"], ledger={"since": "v0.7.0", "directions": ["upgrade"], "only_newest": only})
            expected = dict(EXPECTED, cells=cells)
            with self.assertRaises(select.ExpectedError) as caught:
                select.matrix(expected, TAGS, "full")
            self.assertIn("state at rest", str(caught.exception))

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


    def test_only_newest_can_name_the_directions_it_limits(self):
        cells = dict(EXPECTED["cells"], ledger={
            "since": "v0.7.0", "directions": ["upgrade", "forward"], "only_newest": ["forward"],
        })
        expected = dict(EXPECTED, cells=cells)
        got = sorted((c["direction"], c["tag"]) for c in select.matrix(expected, TAGS, "full") if c["cell"] == "ledger")
        self.assertEqual(got, [("forward", "v0.8.0"), ("forward", "v0.9.0"),
                               ("upgrade", "v0.7.0"), ("upgrade", "v0.8.0"), ("upgrade", "v0.9.0")])
        pr = sorted((c["direction"], c["tag"]) for c in select.matrix(expected, TAGS, "pr") if c["cell"] == "ledger")
        self.assertEqual(pr, [("forward", "v0.9.0"), ("upgrade", "v0.9.0")])

    def test_a_nightly_only_cell_runs_in_the_full_matrix_and_never_on_a_pull_request(self):
        cells = dict(EXPECTED["cells"], heavy={
            "since": "v0.9.0", "directions": ["forward"], "source": "build", "nightly_only": True,
        })
        expected = dict(EXPECTED, cells=cells)
        full = [c for c in select.matrix(expected, TAGS, "full") if c["cell"] == "heavy"]
        self.assertEqual([c["tag"] for c in full], ["v0.9.0"])
        self.assertEqual({c["source"] for c in full}, {"build"})
        self.assertEqual([c for c in select.matrix(expected, TAGS, "pr") if c["cell"] == "heavy"], [])


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
                                        "older_than": "v0.5.0", "expect": "hash-failure",
                                        "why": "x"}]),
            "matches no",
        )

    def test_two_exceptions_for_one_cell_are_refused(self):
        row = {"cell": "manifest", "direction": "forward", "tags": ["v0.7.0"],
               "expect": "hash-failure", "why": "x"}
        self.check_refused(dict(EXPECTED, exceptions=[row, EXPECTED["exceptions"][0]]), "twice")

    def test_a_listed_tag_that_is_not_a_release_the_cell_runs_is_refused(self):
        self.check_refused(
            dict(EXPECTED, exceptions=[{"cell": "manifest", "direction": "forward",
                                        "tags": ["v0.7.0", "v0.4.0"], "expect": "hash-failure",
                                        "why": "x"}]),
            "v0.4.0",
        )

    def test_a_cell_without_directions_is_refused(self):
        for directions in (None, [], ["forward", "forward"]):
            cell = {"since": "v0.7.0"}
            if directions is not None:
                cell["directions"] = directions
            self.check_refused(dict(EXPECTED, cells={"manifest": cell}, exceptions=[]), "direction")

    def test_a_built_from_source_tag_that_is_not_a_release_is_refused(self):
        self.check_refused(dict(EXPECTED, built_from_source=[{"tag": "v0.4.1", "why": "x"}]), "v0.4.1")

    def test_an_empty_matrix_is_refused(self):
        self.check_refused(
            dict(EXPECTED, cells={"manifest": {"since": "v9.0.0", "directions": ["forward"]}},
                 exceptions=[]),
            "no cell",
        )

    def test_only_newest_must_be_a_boolean(self):
        self.check_refused(
            dict(EXPECTED, cells={"manifest": {"since": "v0.7.0", "directions": ["forward"],
                                               "only_newest": "yes"}}, exceptions=[]),
            "only_newest",
        )

    def test_an_unknown_source_is_refused(self):
        self.check_refused(
            dict(EXPECTED, cells={"manifest": {"since": "v0.7.0", "directions": ["forward"], "source": "dream"}},
                 exceptions=[]),
            "dream",
        )

    def test_only_newest_may_name_only_the_cells_directions(self):
        self.check_refused(
            dict(EXPECTED, cells={"manifest": {"since": "v0.7.0", "directions": ["forward"],
                                               "only_newest": ["upgrade"]}}, exceptions=[]),
            "only_newest",
        )

    def test_an_unknown_direction_is_refused(self):
        self.check_refused(
            dict(EXPECTED, cells={"manifest": {"since": "v0.7.0", "directions": ["sideways"]}},
                 exceptions=[]),
            "sideways",
        )


class TheCommittedFile(unittest.TestCase):
    def test_expected_yaml_is_valid_against_the_released_tags(self):
        expected = select.load(select.DEFAULT_EXPECTED)
        full = select.matrix(expected, TAGS, "full")
        self.assertTrue(full)
        manifest = {c["tag"]: c["expect"] for c in full if c["cell"] == "manifest"}
        # Unchanged from the manifest-compat job this replaces.
        self.assertEqual(manifest, {"v0.7.0": "hash-failure", "v0.8.0": "pass", "v0.9.0": "pass"})

    def test_a_release_below_the_attributed_level_refuses_the_raised_meta_directory(self):
        """meta.sh's forward direction raises this build's directory to its
        highest feature level, then re-registers a node thousands of times.
        From ATTRIBUTED_COMMANDS_FEATURE_LEVEL (#706) the leader commits each
        of those commands as command kind 40, which a release whose highest
        level is below it cannot decode: it refuses the directory as written
        by a newer release, so the cell cannot expect `pass` for it (the
        failure the Compat run of #870 showed). The highest level a release
        accepts is the one its committed fixture was raised to."""
        registry = yaml.safe_load(FORMATS.read_text(encoding="utf-8"))
        constants = {
            c["name"]: int(c["value"])
            for entry in registry["formats"]
            for c in entry.get("constants") or []
            if c["file"] == "crates/vtop-meta/src/command.rs" and c["value"].isdigit()
        }
        attributed = constants["ATTRIBUTED_COMMANDS_FEATURE_LEVEL"]
        supported = constants["SUPPORTED_FEATURE_LEVEL"]
        levels = {}
        for fixture in sorted(META_FIXTURES.iterdir()):
            expect = fixture / "expect.txt"
            if expect.is_file():
                fields = dict(line.split(" ", 1) for line in expect.read_text().splitlines() if " " in line)
                levels[fixture.name] = int(fields["feature_level"])
        self.assertTrue(levels, f"no released meta fixture under {META_FIXTURES}")
        full = select.matrix(select.load(select.DEFAULT_EXPECTED), TAGS + list(levels), "full")
        forward = {c["tag"]: c for c in full if c["cell"] == "meta" and c["direction"] == "forward"}
        for tag, level in levels.items():
            if not level < attributed <= supported:
                continue
            with self.subTest(tag=tag, release_level=level, attributed=attributed):
                self.assertEqual(forward[tag]["expect"], "refuse")
                self.assertIn("written by a newer release", forward[tag]["message"])


if __name__ == "__main__":
    unittest.main()
