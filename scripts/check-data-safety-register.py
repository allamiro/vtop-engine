#!/usr/bin/env python3
"""Fail when the data-safety register and the `data-safety` label disagree.

docs/reference/data-safety-register.md has one row per issue labelled
`data-safety`. The label is applied by hand after triage, and the register is
written by hand, so either can change without the other. This check reads the
labelled issues from GitHub and the register's main table, and fails when:

  - a labelled issue has no row;
  - a row names an issue that does not carry the label;
  - a row's Fixed cell says "open" for a closed issue, or names a fix for an
    open one;
  - a closed issue's Fixed cell is empty;
  - a row's Found date is not the date its issue was opened (UTC), which is
    the date the quiet period is counted from;
  - an issue has two rows, or the rows are out of issue order;
  - an issue in the "considered and not labelled" table carries the label;
  - in the "found and fixed in a pull request" table (data-safety bugs found
    while working and fixed in the pull request that found them, with no
    issue filed), a pull request has two rows, the rows are out of order, a
    Found cell is not a date, or a number is a labelled issue (whose row
    belongs in the main table).

With --quiet-days it also prints the days since the most recent data-safety
bug was found: the most recently opened labelled issue, or the latest Found
date in the pull-request table, whichever is later. That is the data-safety
quiet period the release checklist records
(docs/reference/data-safety-register.md, "Quiet period").

Usage:
  scripts/check-data-safety-register.py [--quiet-days]
      reads the issues with `gh issue list` (needs GH_TOKEN or `gh auth`)
  scripts/check-data-safety-register.py --issues-json FILE [--today YYYY-MM-DD]
      reads them from FILE instead: the JSON `gh issue list --json
      number,state,createdAt,title` prints (the tests use this)

Exit status: 0 consistent, 1 inconsistent, 2 the register or the issue list
could not be read.
"""

from __future__ import annotations

import argparse
import datetime
import json
import pathlib
import re
import subprocess
import sys
from dataclasses import dataclass, field

LABEL = "data-safety"
DEFAULT_REGISTER = pathlib.Path(__file__).resolve().parent.parent / "docs/reference/data-safety-register.md"
# The register's columns, in order. A changed header is refused rather than
# parsed by position, so a renamed or reordered column cannot make the check
# read the wrong cell as the Fixed cell.
COLUMNS = ["Issue", "Class", "Component", "Introduced", "Found", "Fixed", "Found by", "What happened"]
MAIN_HEADING = "## Register"
CONSIDERED_HEADING = "## Considered and not labelled"
PR_HEADING = "## Found and fixed in a pull request"
PR_COLUMNS = ["Pull request"] + COLUMNS[1:]
# gh's --limit; a result this long may have been cut off, so it is refused.
LIMIT = 1000
EMPTY = {"", "-", "—", "–"}


class RegisterError(Exception):
    """The register (or the issue list) cannot be read as the check expects."""


@dataclass
class Row:
    number: int
    fixed: str
    line: int
    found: str = ""


@dataclass
class Register:
    rows: list = field(default_factory=list)
    considered: list = field(default_factory=list)
    # Rows of the pull-request table: a Row whose number is the pull request.
    pr_rows: list = field(default_factory=list)


def _cells(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def _section(lines: list[str], heading: str) -> int:
    for i, line in enumerate(lines):
        if line.strip() == heading:
            return i
    raise RegisterError(f'heading "{heading}" not found')


def _table(lines: list[str], start: int) -> tuple[list[str], list[tuple[int, list[str]]]]:
    """The first table after line `start`: its header cells and its body rows."""
    i = start + 1
    while i < len(lines) and not lines[i].lstrip().startswith("|"):
        if lines[i].startswith("## "):
            raise RegisterError(f"no table under {lines[start].strip()!r}")
        i += 1
    if i + 1 >= len(lines):
        raise RegisterError(f"no table under {lines[start].strip()!r}")
    header = _cells(lines[i])
    body = []
    j = i + 2  # skip the |---| delimiter row
    while j < len(lines) and lines[j].lstrip().startswith("|"):
        body.append((j + 1, _cells(lines[j])))
        j += 1
    return header, body


def _issue_number(cell: str, line: int) -> int:
    m = re.search(r"#(\d+)", cell)
    if m is None:
        raise RegisterError(f"line {line}: no issue number in {cell!r}")
    return int(m.group(1))


def parse_register(text: str) -> Register:
    lines = text.splitlines()
    header, body = _table(lines, _section(lines, MAIN_HEADING))
    if header != COLUMNS:
        raise RegisterError(f"the register's columns are {header}, expected {COLUMNS}")
    if not body:
        raise RegisterError("the register's table has no rows")
    fixed_at = COLUMNS.index("Fixed")
    found_at = COLUMNS.index("Found")
    reg = Register()
    for line, cells in body:
        if len(cells) != len(COLUMNS):
            raise RegisterError(f"line {line}: {len(cells)} cells, expected {len(COLUMNS)}")
        reg.rows.append(Row(_issue_number(cells[0], line), cells[fixed_at], line, cells[found_at]))
    _, considered = _table(lines, _section(lines, CONSIDERED_HEADING))
    reg.considered = [_issue_number(cells[0], line) for line, cells in considered]
    if any(line.strip() == PR_HEADING for line in lines):
        header, body = _table(lines, _section(lines, PR_HEADING))
        if header != PR_COLUMNS:
            raise RegisterError(f"the pull-request table's columns are {header}, expected {PR_COLUMNS}")
        for line, cells in body:
            if len(cells) != len(PR_COLUMNS):
                raise RegisterError(f"line {line}: {len(cells)} cells, expected {len(PR_COLUMNS)}")
            reg.pr_rows.append(Row(_issue_number(cells[0], line), cells[fixed_at], line, cells[found_at]))
    return reg


def _says_open(fixed: str) -> bool:
    return fixed.lower().startswith("open")


def check(reg: Register, issues: list[dict]) -> list[str]:
    labelled = {i["number"]: i for i in issues}
    problems = []
    seen: set[int] = set()
    previous = None
    for r in reg.rows:
        if r.number in seen:
            problems.append(f"#{r.number} has more than one row (line {r.line})")
        seen.add(r.number)
        if previous is not None and r.number < previous:
            problems.append(f"#{r.number} is out of issue order: it follows #{previous} (line {r.line})")
        previous = r.number
        issue = labelled.get(r.number)
        if issue is None:
            problems.append(
                f"#{r.number} has a row (line {r.line}) but does not carry the {LABEL} label: "
                f"label it, or move it to the considered table with the reason"
            )
            continue
        opened = issue["createdAt"][:10]
        if r.found != opened:
            problems.append(f"#{r.number} was opened on {opened}, but its Found cell says {r.found!r} (line {r.line})")
        is_open = issue["state"].upper() == "OPEN"
        if is_open and not _says_open(r.fixed):
            problems.append(f'#{r.number} is open, but its Fixed cell says {r.fixed!r}: write "open" (line {r.line})')
        elif not is_open and _says_open(r.fixed):
            problems.append(f"#{r.number} is closed, but its Fixed cell says open: name the fix (line {r.line})")
        elif not is_open and r.fixed in EMPTY:
            problems.append(f"#{r.number} is closed, but its Fixed cell is empty: name the PR, release and date (line {r.line})")
    for n in sorted(set(labelled) - seen):
        problems.append(f"#{n} carries the {LABEL} label but has no row: add one ({labelled[n].get('title', '')})")
    for n in reg.considered:
        if n in labelled:
            problems.append(f"#{n} is in the considered table but carries the {LABEL} label: move it to the register")
    seen_prs: set[int] = set()
    previous = None
    for r in reg.pr_rows:
        if r.number in seen_prs:
            problems.append(f"pull request #{r.number} has more than one row (line {r.line})")
        seen_prs.add(r.number)
        if previous is not None and r.number < previous:
            problems.append(f"pull request #{r.number} is out of order: it follows #{previous} (line {r.line})")
        previous = r.number
        if r.number in labelled:
            problems.append(
                f"#{r.number} is in the pull-request table but is a labelled issue: its row belongs "
                f"in the register table (line {r.line})"
            )
        try:
            datetime.date.fromisoformat(r.found)
        except ValueError:
            problems.append(f"pull request #{r.number}'s Found cell {r.found!r} is not a YYYY-MM-DD date (line {r.line})")
    return problems


def quiet_days(issues: list[dict], today: datetime.date, pr_rows: list | None = None) -> tuple[int, dict]:
    """Days since the most recent data-safety bug was found, and that bug: a
    labelled issue (by the day it was opened), or a pull-request row (by its
    Found date, as `{"number", "createdAt", "pull_request": True}`)."""
    found = list(issues)
    for r in pr_rows or []:
        try:
            datetime.date.fromisoformat(r.found)
        except ValueError:
            continue
        found.append({"number": r.number, "createdAt": r.found, "pull_request": True})

    def opened(i: dict) -> datetime.date:
        return datetime.date.fromisoformat(i["createdAt"][:10])

    latest = max(found, key=opened)
    return (today - opened(latest)).days, latest


def _gh_issues(repo: str | None) -> list[dict]:
    cmd = ["gh", "issue", "list", "--label", LABEL, "--state", "all", "--limit", str(LIMIT),
           "--json", "number,state,createdAt,title"]
    if repo:
        cmd += ["--repo", repo]
    try:
        out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
    except (OSError, subprocess.CalledProcessError) as e:
        detail = getattr(e, "stderr", "") or str(e)
        raise RegisterError(f"`{' '.join(cmd)}` failed: {detail.strip()}")
    return json.loads(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--register", type=pathlib.Path, default=DEFAULT_REGISTER)
    ap.add_argument("--issues-json", type=pathlib.Path, help="read the labelled issues from this file, not gh")
    ap.add_argument("--repo", help="OWNER/NAME for gh (default: the current repository)")
    ap.add_argument("--quiet-days", action="store_true", help="also print the days since the last labelled issue was opened")
    ap.add_argument("--today", type=datetime.date.fromisoformat, default=None, help="YYYY-MM-DD (default: today, UTC)")
    args = ap.parse_args(argv)

    try:
        if args.issues_json:
            issues = json.loads(args.issues_json.read_text(encoding="utf-8"))
        else:
            issues = _gh_issues(args.repo)
        if len(issues) >= LIMIT:
            raise RegisterError(f"{len(issues)} labelled issues reached the list limit ({LIMIT}); raise LIMIT")
        reg = parse_register(args.register.read_text(encoding="utf-8"))
    except (OSError, ValueError, RegisterError) as e:
        print(f"data-safety register check: cannot run: {e}")
        return 2

    problems = check(reg, issues)
    if problems:
        print(f"data-safety register check: {args.register} disagrees with the {LABEL} label:")
        for p in problems:
            print(f"  {p}")
    else:
        open_count = sum(1 for i in issues if i["state"].upper() == "OPEN")
        print(f"data-safety register check: {len(reg.rows)} rows match the {len(issues)} "
              f"labelled issues ({open_count} open)")
    if args.quiet_days and (issues or reg.pr_rows):
        today = args.today or datetime.datetime.now(datetime.timezone.utc).date()
        days, latest = quiet_days(issues, today, reg.pr_rows)
        if latest.get("pull_request"):
            print(f"quiet period: {days} days since pull request #{latest['number']} found a "
                  f"data-safety bug ({latest['createdAt'][:10]}), the most recent one")
        else:
            print(f"quiet period: {days} days since #{latest['number']} was opened "
                  f"({latest['createdAt'][:10]}), the most recent {LABEL} issue")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
