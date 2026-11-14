#!/usr/bin/env python3
"""Choose the compatibility matrix's cells (#743).

A cell is one format, one released tag and one direction:

* ``upgrade``: the released binary writes, this build reads;
* ``forward``: this build writes, the released binary reads.

The releases come from the repository's tags, finals only, the newest patch
of each minor, so a new release joins the day its tag exists. Each cell's
expected result is ``pass`` unless ``scripts/compat/expected.yaml`` lists an
exception, so adding a refusal means editing a reviewed file.

Modes:

* ``full`` (nightly, and on demand): every release since each cell applies;
* ``pr``: the newest released minor only, plus every cell an exception names,
  so a documented cutoff is checked on every pull request.

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
    from_source = set()
    for row in expected.get("built_from_source") or []:
        if not row.get("why"):
            raise ExpectedError(f"a built_from_source entry needs a why: {row}")
        version(row["tag"])
        from_source.add(row["tag"])

    available = [tag for tag in releases(tags) if tag not in from_source]
    newest_minor = available[-1] if available else None

    per_cell: dict[str, list[str]] = {}
    for name, cell in cells.items():
        since = version(cell["since"])
        for direction in cell.get("directions") or []:
            if direction not in DIRECTIONS:
                raise ExpectedError(f"cell {name}: unknown direction {direction!r}")
        per_cell[name] = [tag for tag in available if version(tag) >= since]

    outcome: dict[tuple[str, str, str], dict] = {}
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
        matched = _exception_tags(row, per_cell[name])
        if not matched:
            raise ExpectedError(f"an exception for {name} {direction} matches no released tag the cell runs: {row}")
        for tag in matched:
            key = (name, tag, direction)
            if key in outcome:
                raise ExpectedError(f"{name} {tag} {direction} has an exception twice")
            outcome[key] = {"expect": row["expect"], "message": row.get("message", "")}

    out = []
    for name, cell in cells.items():
        for direction in cell["directions"]:
            for tag in per_cell[name]:
                key = (name, tag, direction)
                if mode == "pr" and tag != newest_minor and key not in outcome:
                    continue
                result = outcome.get(key, {"expect": "pass", "message": ""})
                out.append({
                    "cell": name,
                    "tag": tag,
                    "direction": direction,
                    "expect": result["expect"],
                    "message": result["message"],
                    "source": "release",
                })
    return out


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--mode", choices=("full", "pr"), required=True)
    parser.add_argument("--expected", type=pathlib.Path, default=DEFAULT_EXPECTED)
    parser.add_argument("--tags-from-git", action="store_true")
    parser.add_argument("tags", nargs="*")
    args = parser.parse_args(argv)
    tags = list(args.tags)
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
