#!/usr/bin/env python3
"""Fail when an ignored security advisory has not been reviewed for 90 days.

`.cargo/audit.toml` and `deny.toml` each keep an `ignore` list of RustSec ids
that CI accepts, and docs/SECURITY_MODEL.md section 13 explains each one. An
ignore that nobody re-checks outlives the reason it was added, so every id
carries the day it was last reviewed, on its own line:

    "RUSTSEC-2026-0387", # reviewed: 2026-11-16

and the same date in the "Last reviewed" column of the section 13 table. This
check reads the three files and fails when:

  - an id has no review date, or a date that is not YYYY-MM-DD, or a date in
    the future;
  - a date is more than 90 days old (it warns, and still passes, from day 75);
  - the two files, or a file and the table, give an id different dates;
  - an id is listed twice in a file, or has two table rows;
  - an id is in deny.toml but not in .cargo/audit.toml (the source of truth),
    or in .cargo/audit.toml but not in deny.toml, unless it is named in
    AUDIT_ONLY below;
  - an id is ignored but has no table row, or a table row names an id that is
    not ignored.

It refuses (exit 2) a file it cannot read the way it expects: no
`[advisories]` `ignore = [` ... `]` block written one entry per line, an entry
written as an inline table, or a table without the "Last reviewed" column.
Those forms are valid TOML, but the date has to sit on the id's own line for a
reviewer to see it, so the check accepts only that one shape.

Usage:
  scripts/check-advisory-review.py [--today YYYY-MM-DD] [--root DIR]

Exit status: 0 every review is current (warnings may be printed), 1 a review
is overdue or the files disagree, 2 a file could not be read.
"""

from __future__ import annotations

import argparse
import datetime
import pathlib
import re
import sys
from dataclasses import dataclass, field

AUDIT = ".cargo/audit.toml"
DENY = "deny.toml"
MODEL = "docs/SECURITY_MODEL.md"
SECTION = "## 13."
DATE_COLUMN = "Last reviewed"
WARN_DAYS = 75
FAIL_DAYS = 90

# Ids ignored in .cargo/audit.toml but deliberately absent from deny.toml,
# because cargo-deny never reports them (deny.toml says why at the end of its
# ignore list). Anything else in one file must be in the other. A name here
# whose ignore has been removed is itself a failure, so the list cannot go
# stale.
AUDIT_ONLY = {
    "RUSTSEC-2026-0253": "lru is unsound, and cargo-deny's default "
    '`unsound = "workspace"` reports only direct dependencies',
}

ID = re.compile(r"RUSTSEC-\d{4}-\d{4}")
ENTRY = re.compile(r'^\s*"(?P<id>[^"]*)"\s*,?\s*(?:#(?P<comment>.*))?$')
REVIEWED = re.compile(r"^\s*reviewed:\s*(?P<date>.*?)\s*$")
ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ReviewError(Exception):
    """A file cannot be read the way the check expects."""


@dataclass
class Entry:
    id: str
    date: str | None  # the text after "reviewed:", or None when absent
    line: int


@dataclass
class Result:
    problems: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    report: list = field(default_factory=list)
    dates: list = field(default_factory=list)  # every date that parsed


def parse_ignores(text: str, name: str) -> list[Entry]:
    """The entries of the `[advisories]` `ignore` array, one per line."""
    lines = text.splitlines()
    try:
        start = next(i for i, l in enumerate(lines) if l.strip() == "[advisories]")
    except StopIteration:
        raise ReviewError(f"{name}: no [advisories] section") from None
    opening = None
    for i in range(start + 1, len(lines)):
        stripped = lines[i].strip()
        if stripped.startswith("[") and not stripped.startswith("[["):
            break  # the next section
        if re.match(r"^ignore\s*=", stripped):
            if re.match(r"^ignore\s*=\s*\[\s*(#.*)?$", stripped):
                opening = i
                break
            raise ReviewError(f"{name}:{i + 1}: write the ignore array one entry per line, "
                              "starting with `ignore = [` on a line of its own")
    if opening is None:
        raise ReviewError(f"{name}: no `ignore = [` in [advisories]")
    entries = []
    for i in range(opening + 1, len(lines)):
        stripped = lines[i].strip()
        if stripped.startswith("]"):
            return entries
        if not stripped or stripped.startswith("#"):
            continue
        m = ENTRY.match(lines[i])
        if not m:
            raise ReviewError(f"{name}:{i + 1}: cannot read `{stripped}`; write each ignore as "
                              '"RUSTSEC-YYYY-NNNN", # reviewed: YYYY-MM-DD (one per line, no inline table)')
        comment = m.group("comment")
        r = REVIEWED.match(comment) if comment is not None else None
        entries.append(Entry(m.group("id"), r.group("date") if r else None, i + 1))
    raise ReviewError(f"{name}: the ignore array opened on line {opening + 1} is never closed")


def _cells(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def parse_table(text: str, name: str) -> list[Entry]:
    """The section 13 table's rows: the advisory cell and the date cell."""
    lines = text.splitlines()
    try:
        start = next(i for i, l in enumerate(lines) if l.startswith(SECTION))
    except StopIteration:
        raise ReviewError(f"{name}: no `{SECTION}` section") from None
    end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith("## ")), len(lines))
    header = None
    for i in range(start, end):
        if lines[i].startswith("|") and "Advisory" in _cells(lines[i]):
            header = i
            break
    if header is None:
        raise ReviewError(f"{name}: no advisory table in section 13")
    cols = _cells(lines[header])
    if DATE_COLUMN not in cols:
        raise ReviewError(f"{name}:{header + 1}: the advisory table has no `{DATE_COLUMN}` column")
    at = cols.index(DATE_COLUMN)
    rows = []
    for i in range(header + 2, end):
        if not lines[i].startswith("|"):
            break
        cells = _cells(lines[i])
        ids = ID.findall(cells[0])
        if len(ids) != 1:
            rows.append(Entry(cells[0], None, -(i + 1)))  # flagged by check()
            continue
        rows.append(Entry(ids[0], cells[at] if at < len(cells) else "", i + 1))
    return rows


def _date(text: str | None) -> datetime.date | None:
    if text is None or not ISO.match(text):
        return None
    try:
        return datetime.date.fromisoformat(text)
    except ValueError:
        return None


def check(audit: list[Entry], deny: list[Entry], table: list[Entry],
          today: datetime.date | None) -> Result:
    """Compare the three lists; ages are skipped when today is None."""
    res = Result()
    sources = [(AUDIT, audit), (DENY, deny), (MODEL, table)]
    by_source: dict[str, dict[str, Entry]] = {}
    for name, entries in sources:
        seen: dict[str, Entry] = {}
        for e in entries:
            where = f"{name}:{abs(e.line)}"
            if e.line < 0:
                res.problems.append(f"{where}: `{e.id}`: name one advisory per row, so each id has its own date")
                continue
            if not ID.fullmatch(e.id):
                res.problems.append(f"{where}: `{e.id}` is not a RustSec id (RUSTSEC-YYYY-NNNN)")
                continue
            if e.id in seen:
                what = "has more than one row" if name == MODEL else "is listed more than once"
                res.problems.append(f"{where}: {e.id} {what} (first on line {seen[e.id].line})")
                continue
            seen[e.id] = e
            if e.date is None:
                res.problems.append(f"{where}: {e.id} has no review date; "
                                    "add `# reviewed: YYYY-MM-DD` after it")
            elif _date(e.date) is None:
                res.problems.append(f"{where}: {e.id}: review date `{e.date}` is not a date (YYYY-MM-DD)")
            else:
                res.dates.append(_date(e.date))
        by_source[name] = seen

    a, d, t = by_source[AUDIT], by_source[DENY], by_source[MODEL]
    for i in sorted(d.keys() - a.keys()):
        res.problems.append(f"{DENY}:{d[i].line}: {i} is ignored in {DENY} but not in {AUDIT}, "
                            "which is the source of truth")
    for i in sorted(a.keys() - d.keys() - AUDIT_ONLY.keys()):
        res.problems.append(f"{AUDIT}:{a[i].line}: {i} is not in {DENY}; mirror it there with the same date, "
                            "or, if cargo-deny never reports it, add it to AUDIT_ONLY in this script")
    for i in sorted(d.keys() & AUDIT_ONLY.keys()):
        res.problems.append(f"{DENY}:{d[i].line}: {i} is audit-only ({AUDIT_ONLY[i]}); "
                            "remove it from AUDIT_ONLY in this script if cargo-deny now reports it")
    for i in sorted(AUDIT_ONLY.keys() - a.keys()):
        res.problems.append(f"{i} is named in AUDIT_ONLY in this script but no longer ignored in {AUDIT}; "
                            "remove the name")
    ignored = a.keys() | d.keys()
    for i in sorted(ignored - t.keys()):
        res.problems.append(f"{MODEL}: {i} is ignored but the section 13 table has no row for it")
    for i in sorted(t.keys() - ignored):
        res.problems.append(f"{MODEL}:{t[i].line}: the table has a row for {i}, which is not ignored "
                            f"in {AUDIT} or {DENY}; remove the row")

    for i in sorted(ignored):
        dates = {}
        for name in (AUDIT, DENY, MODEL):
            e = by_source[name].get(i)
            if e is not None and _date(e.date) is not None:
                dates[name] = e.date
        if len(set(dates.values())) > 1:
            said = ", ".join(f"{n} says {v}" for n, v in dates.items())
            res.problems.append(f"{i}: the review dates disagree ({said}); a review moves all three together")
            continue
        if not dates or today is None:
            continue
        reviewed = _date(next(iter(dates.values())))
        age = (today - reviewed).days
        left = FAIL_DAYS - age
        where = f"{AUDIT}:{a[i].line}" if i in a else f"{DENY}:{d[i].line}"
        if age < 0:
            res.problems.append(f"{where}: {i} was reviewed on {reviewed}, which is in the future (today is {today})")
        elif age > FAIL_DAYS:
            res.problems.append(f"{where}: {i} was last reviewed {age} days ago ({reviewed}), more than "
                                f"{FAIL_DAYS}; review it (docs/SECURITY_MODEL.md section 13) and move the date")
        else:
            line = f"{i} reviewed {reviewed}, {age} days ago, {left} days left"
            res.report.append(line)
            if age >= WARN_DAYS:
                res.warnings.append(f"{where}: {line}; the review is due, and this check fails from day {FAIL_DAYS + 1}")
    return res


def check_root(root: pathlib.Path, today: datetime.date | None) -> Result:
    def read(rel: str) -> str:
        try:
            return (root / rel).read_text(encoding="utf-8")
        except OSError as e:
            raise ReviewError(f"{rel}: {e.strerror}") from None

    return check(parse_ignores(read(AUDIT), AUDIT), parse_ignores(read(DENY), DENY),
                 parse_table(read(MODEL), MODEL), today)


def _annotation(kind: str, message: str) -> str:
    # `path:line: text` becomes a GitHub annotation on that line.
    m = re.match(r"^([\w./-]+):(\d+): (.*)$", message, re.S)
    if m:
        return f"::{kind} file={m.group(1)},line={m.group(2)}::{m.group(3)}"
    return f"::{kind}::{message}"


def _iso(text: str) -> datetime.date:
    d = _date(text)
    if d is None:
        raise argparse.ArgumentTypeError(f"not a date (YYYY-MM-DD): {text}")
    return d


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--today", type=_iso, default=None, help="the day to measure ages from (default: today, UTC)")
    ap.add_argument("--root", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parent.parent,
                    help="the repository root")
    args = ap.parse_args(argv)
    today = args.today or datetime.datetime.now(datetime.timezone.utc).date()
    try:
        res = check_root(args.root, today)
    except ReviewError as e:
        print(f"advisory review check: cannot run: {e}")
        return 2
    for line in res.report:
        print(line)
    for w in res.warnings:
        print(_annotation("warning", w))
    if res.problems:
        print(f"advisory review check: {len(res.problems)} problem(s) on {today}:")
        for p in res.problems:
            print(_annotation("error", p))
        return 1
    print(f"advisory review check: {len(res.report)} ignored advisories, every review within "
          f"{FAIL_DAYS} days on {today}" + (f"; {len(res.warnings)} due soon" if res.warnings else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
