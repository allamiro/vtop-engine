#!/usr/bin/env python3
"""Choose the compatibility matrix's cells (#743).

A cell is one format, one released tag and one direction:

* ``upgrade``: the released binary writes, this build reads;
* ``forward``: this build writes, the released binary reads.

The releases come from the repository's tags, finals only, the newest patch
of each minor, so a new release joins the day its tag exists. Each cell's
expected result is ``pass`` unless ``scripts/compat/expected.yaml`` lists an
exception, so adding a refusal means editing a reviewed file.

A cell with ``only_newest: true`` runs the newest release alone in both modes;
``only_newest: [forward]`` does so for the listed directions only (a ledger is
read by every release since ``since`` when it is upgraded, but only the
previous release may share one with this build). A cell may also say
``source: build`` (its old binary needs a feature the published archives lack,
so the tag is built from source with it) and ``nightly_only: true`` (too heavy
for a pull request: only ``full`` runs it).

Modes:

* ``full`` (nightly, and on demand): every release since each cell applies;
* ``pr``: the newest released minor, plus, for each exception row, its
  boundary: the newest release the row names and the first release after it,
  so a documented cutoff is checked on both sides on every pull request at
  a cost of two releases per row.

Usage::

    select_cells.py --mode full|pr [--expected FILE] [--tags-from-git | TAG ...]

Prints ``{"include": [...]}`` for a GitHub Actions matrix, and the tags it
lists as built from source on stderr.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys

DEFAULT_EXPECTED = pathlib.Path(__file__).resolve().parent / "expected.yaml"
DIRECTIONS = ("upgrade", "forward")
SOURCES = ("release", "build")
RESULTS = ("pass", "refuse", "hash-failure")
FINAL = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")


class ExpectedError(ValueError):
    """expected.yaml says something that cannot be right."""


def version(tag: str) -> tuple[int, int, int]:
    match = FINAL.match(tag)
    if not match:
        raise ExpectedError(f"{tag!r} is not a final release tag (vX.Y.Z)")
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def releases(tags: list[str]) -> list[str]:
    """Final tags only, the newest patch of each minor, oldest first."""
    newest: dict[tuple[int, int], str] = {}
    for tag in tags:
        if not FINAL.match(tag):
            continue
        key = version(tag)[:2]
        if key not in newest or version(tag) > version(newest[key]):
            newest[key] = tag
    return sorted(newest.values(), key=version)


def published_tags(listed: list[dict]) -> list[str]:
    """The tags of releases that are published, not drafts and not prereleases.

    ``listed`` is ``gh release list --json tagName,isDraft,isPrerelease``. A
    draft has no assets yet. A release marked prerelease is refused by
    fetch-release.sh even when its tag looks final (v0.11.0), so selecting it
    would fail every cell instead of running the newest stable release.
    """
    return [
        row["tagName"]
        for row in listed
        if not row.get("isDraft") and not row.get("isPrerelease")
    ]


def load(path: pathlib.Path) -> dict:
    import yaml  # PyYAML, pinned in scripts/compat/requirements.txt

    with open(path, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _exception_tags(row: dict, candidates: list[str]) -> list[str]:
    if "tags" in row and "older_than" in row:
        raise ExpectedError(f"an exception names both tags and older_than: {row}")
    if "tags" in row:
        for tag in row["tags"]:
            version(tag)
            if tag not in candidates:
                raise ExpectedError(f"an exception names {tag}, which is not a release the cell runs: {row}")
        return [tag for tag in candidates if tag in row["tags"]]
    if "older_than" in row:
        bound = version(row["older_than"])
        return [tag for tag in candidates if version(tag) < bound]
    raise ExpectedError(f"an exception names neither tags nor older_than: {row}")


def matrix(expected: dict, tags: list[str], mode: str) -> list[dict]:
    if mode not in ("full", "pr"):
        raise ExpectedError(f"unknown mode {mode!r}")
    cells = expected.get("cells") or {}
    if not cells:
        raise ExpectedError("expected.yaml lists no cells")
    released = releases(tags)
    from_source = set()
    for row in expected.get("built_from_source") or []:
        if not row.get("why"):
            raise ExpectedError(f"a built_from_source entry needs a why: {row}")
        version(row["tag"])
        if row["tag"] not in released:
            raise ExpectedError(f"built_from_source names {row['tag']}, which is not a release")
        from_source.add(row["tag"])

    available = [tag for tag in released if tag not in from_source]
    newest_minor = available[-1] if available else None

    per_cell: dict[tuple[str, str], list[str]] = {}
    for name, cell in cells.items():
        since = version(cell["since"])
        directions = cell.get("directions") or []
        if not directions:
            raise ExpectedError(f"cell {name}: no directions")
        if len(set(directions)) != len(directions):
            raise ExpectedError(f"cell {name}: a direction is listed twice")
        for direction in directions:
            if direction not in DIRECTIONS:
                raise ExpectedError(f"cell {name}: unknown direction {direction!r}")
        only_newest = cell.get("only_newest", False)
        if isinstance(only_newest, bool):
            newest_only = set(directions) if only_newest else set()
        elif isinstance(only_newest, list) and all(d in directions for d in only_newest):
            newest_only = set(only_newest)
        else:
            raise ExpectedError(
                f"cell {name}: only_newest must be true, false or a list of the cell's directions"
            )
        source = cell.get("source", "release")
        if source not in SOURCES:
            raise ExpectedError(f"cell {name}: unknown source {source!r}; one of {SOURCES}")
        if not isinstance(cell.get("nightly_only", False), bool):
            raise ExpectedError(f"cell {name}: nightly_only must be true or false")
        applies = [tag for tag in available if version(tag) >= since]
        for direction in directions:
            # A format only the previous release may share with this build
            # (the ledger, #743) runs the newest release alone, in the
            # directions that say so.
            per_cell[(name, direction)] = applies[-1:] if direction in newest_only else applies

    outcome: dict[tuple[str, str, str], dict] = {}
    boundary: set[tuple[str, str, str]] = set()
    for row in expected.get("exceptions") or []:
        name = row.get("cell")
        if name not in cells:
            raise ExpectedError(f"an exception names cell {name!r}, which expected.yaml does not list")
        direction = row.get("direction")
        if direction not in (cells[name].get("directions") or []):
            raise ExpectedError(f"an exception for {name} names direction {direction!r}, which the cell does not run")
        if row.get("expect") not in RESULTS or row.get("expect") == "pass":
            raise ExpectedError(f"an exception for {name} expects {row.get('expect')!r}; one of {RESULTS[1:]}")
        if not row.get("why"):
            raise ExpectedError(f"an exception for {name} {direction} needs a why")
        if row["expect"] == "refuse" and not row.get("message"):
            raise ExpectedError(f"a refusal for {name} {direction} must give the message the older binary prints")
        matched = _exception_tags(row, per_cell[(name, direction)])
        if not matched:
            raise ExpectedError(f"an exception for {name} {direction} matches no released tag the cell runs: {row}")
        for tag in matched:
            key = (name, tag, direction)
            if key in outcome:
                raise ExpectedError(f"{name} {tag} {direction} has an exception twice")
            outcome[key] = {"expect": row["expect"], "message": row.get("message", "")}
        # The row's boundary: the newest release it names, and the first
        # release after it (the first that must not need the row).
        last = matched[-1]
        boundary.add((name, last, direction))
        after = per_cell[(name, direction)].index(last) + 1
        if after < len(per_cell[(name, direction)]):
            boundary.add((name, per_cell[(name, direction)][after], direction))

    out = []
    for name, cell in cells.items():
        for direction in cell["directions"]:
            for tag in per_cell[(name, direction)]:
                key = (name, tag, direction)
                if mode == "pr" and cell.get("nightly_only"):
                    continue
                if mode == "pr" and tag != newest_minor and key not in boundary:
                    continue
                result = outcome.get(key, {"expect": "pass", "message": ""})
                out.append({
                    "cell": name,
                    "tag": tag,
                    "direction": direction,
                    "expect": result["expect"],
                    "message": result["message"],
                    "source": cell.get("source", "release"),
                })
    if not out:
        raise ExpectedError("no cell to run: every cell's since is newer than every release")
    return out


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mode", choices=("full", "pr"), required=True)
    parser.add_argument("--expected", type=pathlib.Path, default=DEFAULT_EXPECTED)
    parser.add_argument("--tags-from-git", action="store_true")
    parser.add_argument("--tags-from-releases", action="store_true",
                        help="the tags of published GitHub releases, no drafts or prereleases (needs gh)")
    parser.add_argument("tags", nargs="*")
    args = parser.parse_args(argv)
    tags = list(args.tags)
    if args.tags_from_releases:
        # Only tags with a published, non-draft, non-prerelease release: a
        # tag whose release is still being built, or failed, has no assets to
        # run, and fetch-release.sh refuses a prerelease (review).
        tags += published_tags(json.loads(subprocess.run(
            ["gh", "release", "list", "--limit", "1000",
             "--json", "tagName,isDraft,isPrerelease"],
            check=True, capture_output=True, text=True,
        ).stdout))
    if args.tags_from_git:
        tags += subprocess.run(
            ["git", "tag", "--list", "v[0-9]*"], check=True, capture_output=True, text=True
        ).stdout.split()
    if not releases(tags):
        print("no final release tags found", file=sys.stderr)
        return 1
    expected = load(args.expected)
    try:
        cells = matrix(expected, tags, args.mode)
    except ExpectedError as error:
        print(f"{args.expected}: {error}", file=sys.stderr)
        return 1
    for row in expected.get("built_from_source") or []:
        print(f"built from source, no released-binary cell: {row['tag']} ({row['why']})", file=sys.stderr)
    print(json.dumps({"include": cells}))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
