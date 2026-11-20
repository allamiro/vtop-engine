#!/usr/bin/env python3
"""Fail when the threat map in docs/SECURITY_MODEL.md names code that is gone.

Section 17 of docs/SECURITY_MODEL.md has one row per threat, with the control
that stops it and the test or CI check that proves the control. The rows are
written by hand, and a rename or a deleted test would leave them pointing at
nothing, so this check reads the tables and fails when:

  - a row's control or test cell has no reference and is not marked GAP;
  - a reference names a file that does not exist;
  - a reference names a symbol the file no longer contains (a test reference
    must be a function: `fn name` in Rust, `def name` in Python);
  - a Rust test reference is `#[ignore]`d, so CI would not run it;
  - an ID is used twice, or does not start with a known group letter;
  - a group has no row at all, so a whole plane cannot drop out of the map;
  - the gaps table and the rows disagree: a GAP cell with no gaps row, a gaps
    row for a cell that is not a GAP, or a gaps row naming the wrong side.

A reference is written in backticks, as `path` or `path::symbol`. The path is
relative to the repository root and has a directory part or a file extension
(`deny.toml`); other inline code is
prose and is not checked. A GAP cell may name references (the file whose
absence is the gap, say), and they are checked too. The symbol is matched as a whole word, so a
longer name does not stand in for a shorter one. Line numbers are not written
in the document, because every unrelated edit would move them; `--lines`
prints the current `file:line` of every reference instead.

Usage:
  scripts/check-threat-map.py [--lines] [--doc FILE] [--root DIR]

Exit status: 0 consistent, 1 inconsistent, 2 the document cannot be read the
way the check expects.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys
from dataclasses import dataclass, field

ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_DOC = ROOT / "docs/SECURITY_MODEL.md"

SECTION = "## 17. Threat map"
ROW_COLUMNS = ["ID", "Threat", "Control", "Test or CI check", "Residual risk"]
GAP_COLUMNS = ["ID", "Missing", "What closes it"]
# The groups the map must cover. A group with no row fails the check, so the
# cluster plane and the gateway cannot be dropped from the map without a
# reviewer seeing this list change.
GROUPS = {
    "A": "archive engine",
    "I": "audit trail and operator-insider",
    "M": "metadata plane (peer and admin)",
    "R": "replica plane",
    "N": "native client plane",
    "K": "Kafka gateway",
    "T": "transport on every plane",
    "S": "node host and data at rest",
}
MISSING = {"control", "test", "both"}
REF = re.compile(r"`([^`]+)`")


class MapError(Exception):
    """The document cannot be read the way the check expects."""


@dataclass
class Row:
    id: str
    line: int
    control: str
    test: str


@dataclass
class Gap:
    id: str
    line: int
    missing: str


@dataclass
class Parsed:
    rows: list = field(default_factory=list)
    gaps: list = field(default_factory=list)


def _cells(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def _tables(lines: list[str], start: int, end: int):
    """Every table between lines `start` and `end`: (header, [(line_no, cells)])."""
    i = start
    while i < end:
        if lines[i].lstrip().startswith("|") and i + 1 < end and re.match(r"^\s*\|[\s:|-]+\|\s*$", lines[i + 1]):
            header = _cells(lines[i])
            body = []
            j = i + 2
            while j < end and lines[j].lstrip().startswith("|"):
                body.append((j + 1, _cells(lines[j])))
                j += 1
            yield header, body
            i = j
        else:
            i += 1


def parse(text: str) -> Parsed:
    lines = text.splitlines()
    start = next((i for i, l in enumerate(lines) if l.strip() == SECTION), None)
    if start is None:
        raise MapError(f'heading "{SECTION}" not found')
    end = next((i for i in range(start + 1, len(lines)) if re.match(r"^## \S", lines[i])), len(lines))
    parsed = Parsed()
    for header, body in _tables(lines, start + 1, end):
        if header == ROW_COLUMNS:
            for line, cells in body:
                if len(cells) != len(ROW_COLUMNS):
                    raise MapError(f"line {line}: {len(cells)} cells, expected {len(ROW_COLUMNS)}")
                parsed.rows.append(Row(cells[0], line, cells[2], cells[3]))
        elif header == GAP_COLUMNS:
            for line, cells in body:
                if len(cells) != len(GAP_COLUMNS):
                    raise MapError(f"line {line}: {len(cells)} cells, expected {len(GAP_COLUMNS)}")
                parsed.gaps.append(Gap(cells[0], line, cells[1].lower()))
        else:
            raise MapError(f"a table under section 17 has the columns {header}; expected {ROW_COLUMNS} or {GAP_COLUMNS}")
    if not parsed.rows:
        raise MapError("section 17 has no threat table")
    return parsed


def is_gap(cell: str) -> bool:
    return cell.startswith("GAP")


PATH = re.compile(r"[\w./-]+")
# A file at the repository root (`deny.toml`) has no directory part.
FILE = re.compile(r"\.(rs|py|sh|yml|yaml|toml|md|sql|json|tpl|txt)$")


def refs(cell: str) -> list[tuple[str, str | None]]:
    """The references in a cell: backticked `path` or `path::symbol`.

    Any other inline code (a config key, a function named in prose) is not a
    reference. A path has a directory part or a file extension, so
    `ServerConfig::max_sessions` and `i32::MAX` are prose, and `deny.toml` is not.
    """
    out = []
    for raw in REF.findall(cell):
        path, _, symbol = raw.partition("::")
        path = path.strip()
        if PATH.fullmatch(path) and ("/" in path or FILE.search(path)):
            out.append((path, symbol.strip() or None))
    return out


def _word(symbol: str) -> re.Pattern:
    return re.compile(r"(?<![A-Za-z0-9_])" + re.escape(symbol) + r"(?![A-Za-z0-9_])")


def _test_pattern(path: str, symbol: str) -> re.Pattern | None:
    if path.endswith(".rs"):
        return re.compile(r"\bfn\s+" + re.escape(symbol) + r"(?![A-Za-z0-9_])")
    if path.endswith(".py"):
        return re.compile(r"\bdef\s+" + re.escape(symbol) + r"(?![A-Za-z0-9_])")
    return None


RAW_OPEN = re.compile(r'r(#*)"')


def strip_rust_comments(text: str, blank_literals: bool = False) -> str:
    """`text` with every comment blanked and every newline kept.

    Line comments, block comments (which nest in Rust) and doc comments go;
    string, raw string and character literals are scanned so that a `/*` inside a string
    does not start a comment; with `blank_literals` their contents are blanked
    too, so a declaration quoted in a string is not a declaration. Line numbers
    are unchanged. It is a
    scanner for this check, not a Rust lexer: it does not handle byte-string
    prefixes beyond the plain `b""`, and a miss there can only hide a test from the check, which
    then fails loudly instead of passing.
    """
    out: list[str] = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        two = text[i : i + 2]
        if two == "//":
            while i < n and text[i] != "\n":
                out.append(" ")
                i += 1
        elif two == "/*":
            depth = 0
            while i < n:
                if text[i : i + 2] == "/*":
                    depth += 1
                    out.append("  ")
                    i += 2
                elif text[i : i + 2] == "*/":
                    depth -= 1
                    out.append("  ")
                    i += 2
                    if depth == 0:
                        break
                else:
                    out.append("\n" if text[i] == "\n" else " ")
                    i += 1
        elif c == "r" and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")) and (raw := RAW_OPEN.match(text, i)):
            # A raw string: r"..." or r#"..."# with any number of hashes.
            close = '"' + "#" * len(raw.group(1))
            end = text.find(close, raw.end())
            end = n if end < 0 else end + len(close)
            for ch in text[i:end]:
                out.append("\n" if ch == "\n" else " " if blank_literals else ch)
            i = end
        elif c == '"':
            out.append(c)
            i += 1
            while i < n and text[i] != '"':
                if text[i] == "\\" and i + 1 < n:
                    out.append(" " if blank_literals else text[i])
                    i += 1
                out.append("\n" if text[i] == "\n" else " " if blank_literals else text[i])
                i += 1
            if i < n:
                out.append('"')
                i += 1
        elif c == "'" and (text[i + 2 : i + 3] == "'" or (text[i + 1 : i + 2] == "\\" and "'" in text[i + 2 : i + 6])):
            end = text.index("'", i + 2 if text[i + 1] != "\\" else i + 3)
            out.append(" " * (end + 1 - i) if blank_literals else text[i : end + 1])
            i = end + 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


IGNORE = re.compile(r"#\[\s*ignore\b")


def _ignored(lines: list[str], index: int) -> bool:
    """Whether the attributes above line `index` include #[ignore].

    `lines` have their comments blanked. The attribute block is every line
    back to the end of the previous item (a line ending in `;`, `{` or `}`),
    so an attribute that spans several lines is read whole.
    """
    block: list[str] = []
    j = index - 1
    while j >= 0 and len(block) < 60:
        s = lines[j].strip()
        if s.endswith((";", "{", "}")):
            break
        block.append(s)
        j -= 1
    return bool(IGNORE.search(" ".join(reversed(block))))


def resolve(root: pathlib.Path, path: str, symbol: str | None, is_test: bool):
    """The (problem, line) for one reference; problem is None when it holds."""
    file = root / path
    if not file.is_file():
        return f"`{path}` does not exist", None
    if symbol is None:
        return None, 1
    try:
        text = file.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        return f"`{path}` cannot be read: {e}", None
    # A test that is commented out, or quoted in a string, is not a test; a control may be cited by
    # the comment or message that states it, so only tests lose their comments.
    if is_test and path.endswith(".rs"):
        text = strip_rust_comments(text, blank_literals=True)
    lines = text.splitlines()
    pattern = (_test_pattern(path, symbol) if is_test else None) or _word(symbol)
    for n, line in enumerate(lines):
        if is_test and path.endswith(".py") and line.lstrip().startswith("#"):
            continue
        if pattern.search(line):
            if is_test and path.endswith(".rs") and _ignored(lines, n):
                return f"test `{path}::{symbol}` is #[ignore]d, so CI does not run it", None
            return None, n + 1
    kind = "test function" if is_test and _test_pattern(path, symbol) else "symbol"
    return f"`{path}` has no {kind} `{symbol}`", None


def check(parsed: Parsed, root: pathlib.Path) -> tuple[list[str], list[str]]:
    """The problems, and one `file:line` line per resolved reference."""
    problems: list[str] = []
    resolved: list[str] = []
    seen: set[str] = set()
    covered: set[str] = set()
    gap_cells: dict[str, str] = {}

    for row in parsed.rows:
        where = f"{row.id} (line {row.line})"
        if not re.fullmatch(r"[A-Z]\d+", row.id) or row.id[0] not in GROUPS:
            problems.append(f"{where}: the ID must be a group letter from {sorted(GROUPS)} and a number")
        if row.id in seen:
            problems.append(f"{where}: the ID is used twice")
        seen.add(row.id)
        covered.add(row.id[:1])

        missing = []
        for side, cell, is_test in (("control", row.control, False), ("test", row.test, True)):
            found = refs(cell)
            if is_gap(cell):
                missing.append(side)
            elif not found:
                problems.append(f"{where}: the {side} cell names no reference and is not marked GAP")
            for path, symbol in found:
                problem, line = resolve(root, path, symbol, is_test)
                if problem:
                    problems.append(f"{where}: {problem}")
                else:
                    resolved.append(f"{row.id} {side}: {path}:{line}" + (f"  {symbol}" if symbol else ""))
        if missing:
            gap_cells[row.id] = "both" if len(missing) == 2 else missing[0]

    for letter, name in GROUPS.items():
        if letter not in covered:
            problems.append(f"no row for group {letter} ({name}): a plane cannot drop out of the map")

    listed: dict[str, Gap] = {}
    for gap in parsed.gaps:
        if gap.id in listed:
            problems.append(f"{gap.id} (line {gap.line}): the gaps table lists it twice")
        listed[gap.id] = gap
        if gap.missing not in MISSING:
            problems.append(f"{gap.id} (line {gap.line}): Missing must be one of {sorted(MISSING)}, not {gap.missing!r}")
    for id_, side in sorted(gap_cells.items()):
        gap = listed.get(id_)
        if gap is None:
            problems.append(f"{id_}: its {side} cell is a GAP but the gaps table has no row for it")
        elif gap.missing != side:
            problems.append(f"{id_} (line {gap.line}): the gaps table says {gap.missing} is missing, the row says {side}")
    for id_, gap in sorted(listed.items()):
        if id_ not in gap_cells:
            problems.append(f"{id_} (line {gap.line}): in the gaps table, but no cell of its row is a GAP: remove it")
    return problems, resolved


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--doc", type=pathlib.Path, default=DEFAULT_DOC)
    ap.add_argument("--root", type=pathlib.Path, default=ROOT, help="the repository root references are relative to")
    ap.add_argument("--lines", action="store_true", help="print the current file:line of every reference")
    args = ap.parse_args(argv)

    try:
        parsed = parse(args.doc.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, MapError) as e:
        print(f"threat map check: cannot run: {e}")
        return 2

    problems, resolved = check(parsed, args.root)
    if args.lines:
        print("\n".join(resolved))
    if problems:
        print(f"threat map check: {args.doc} names code or tests that do not exist, or disagrees with itself:")
        for p in problems:
            print(f"  {p}")
        return 1
    gaps = len(parsed.gaps)
    print(f"threat map check: {len(parsed.rows)} threats in {len(GROUPS)} groups, "
          f"{len(resolved)} references resolve, {gaps} gap{'s' if gaps != 1 else ''} listed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
