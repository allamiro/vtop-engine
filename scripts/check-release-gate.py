#!/usr/bin/env python3
"""Fail when the 1.0 release gate names evidence that does not exist, or
disagrees with itself.

docs/reference/release-gate.md holds one row per criterion of the
production-readiness bar (#714): its status, the evidence for it, the issues
that close the gap and the date it was last checked (#741). The rows are
written by hand, and a renamed test, a removed CI job or a pull request that
never merged would leave a row citing nothing. This check reads the page and
fails when:

  - the table does not list exactly the criteria of #714, in order;
  - a status is not met, partial or not met; a met row names an open issue or
    no evidence; a row that is not met names no issue;
  - an Evidence reference does not resolve:
      `path`              the file or directory exists;
      `path::name`        a .rs or .py file defines a function `name` (a Rust
                          one not #[ignore]d), any other file contains the
                          word;
      `.github/workflows/F.yml::job`   F.yml has a job with that id;
      #N                  a pull request merged into this branch (a squash
                          commit whose subject ends in "(#N)");
      a CI run link       with --online, a run of this repository, and in a met
                          row a successful one;
  - a Checked date is not a date, is later than the Evaluated date or earlier
    than #714 (2026-11-14), or Evaluated is in the future;
  - the Met, Partial and Not met counts or either verdict line disagree with
    the rows: rc.1 only when every criterion but A1 is met, 1.0.0 only when
    every one is.

With --online it also asks the GitHub API (GH_TOKEN or GITHUB_TOKEN) whether
each Open issue exists and is an issue, and fails a row that is not met while
every issue it names is closed: that row needs re-evaluating. Without full git
history (a shallow clone) pull requests are checked through the API, so a
shallow clone needs --online, or --no-history to skip them: ci.yml runs it
that way on every pull request, so a renamed test or job fails there even
when no docs path changed.

Usage:
  scripts/check-release-gate.py [--online | --no-history] [--doc FILE] [--root DIR] [--today YYYY-MM-DD]

Exit status: 0 consistent, 1 inconsistent, 2 the page cannot be read the way
the check expects (or the evidence cannot be checked at all).
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import pathlib
import re
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Callable

ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_DOC = ROOT / "docs/reference/release-gate.md"
REPO = "allamiro/vtop-engine"

# The criteria of #714 section 2, in its order. A criterion added to or
# dropped from the bar changes this list in the same commit as the page.
CRITERIA = [
    "A1", "A2", "A3", "A4", "A5", "A6",
    "B1", "B2", "B3", "B4",
    "C1", "C2", "C3", "C4", "C5",
    "D1", "D2", "D3", "D4", "D5",
    "E1", "E2", "E3", "E4", "E5", "E6",
    "F1",
    "G1",
    "H1", "H2",
]
QUIET_PERIOD = "A1"
STATUSES = ("met", "partial", "not met")
# #714 was written on this date; no criterion was checked before it.
EARLIEST = dt.date(2026, 11, 14)

COLUMNS = ["ID", "Criterion", "Status", "Evidence", "Open", "Checked"]
BEGIN, END = "<!-- criteria:begin -->", "<!-- criteria:end -->"
VBEGIN, VEND = "<!-- verdict:begin -->", "<!-- verdict:end -->"
VERDICT_COLUMNS = ["Item", "Value"]
VERDICT_ITEMS = ["Evaluated", "Met", "Partial", "Not met", "1.0.0-rc.1 may be cut", "1.0.0 may be cut"]

TICKED = re.compile(r"`([^`]+)`")
ISSUE = re.compile(r"(?<![\w/&])#(\d+)\b")
RUN = re.compile(r"https://github\.com/" + re.escape(REPO) + r"/actions/runs/(\d+)")
WORKFLOW = re.compile(r"^\.github/workflows/[\w.-]+\.ya?ml$")
PATH = re.compile(r"[\w./-]+")
FILE = re.compile(r"\.(rs|py|sh|yml|yaml|toml|md|sql|json|txt)$")


class GateError(Exception):
    """The page cannot be read the way the check expects."""


@dataclass
class Row:
    id: str
    line: int
    status: str
    evidence: str
    open: str
    checked: str


@dataclass
class Page:
    rows: list = field(default_factory=list)
    verdict: dict = field(default_factory=dict)


def _cells(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def _block(lines: list[str], begin: str, end: str) -> tuple[int, int]:
    try:
        b = next(i for i, l in enumerate(lines) if l.strip() == begin)
        e = next(i for i in range(b + 1, len(lines)) if lines[i].strip() == end)
    except StopIteration:
        raise GateError(f"the markers {begin} and {end} were not found in order") from None
    return b, e


def _table(lines: list[str], b: int, e: int, columns: list[str]) -> list[tuple[int, list[str]]]:
    rows = [(i + 1, _cells(lines[i])) for i in range(b + 1, e) if lines[i].lstrip().startswith("|")]
    if len(rows) < 2 or rows[0][1] != columns:
        raise GateError(f"the table between line {b + 1} and line {e + 1} must have the columns {columns}")
    if not re.fullmatch(r"[\s:|-]+", lines[rows[1][0] - 1].strip()):
        raise GateError(f"line {rows[1][0]}: the header separator is missing")
    body = rows[2:]
    for line, cells in body:
        if len(cells) != len(columns):
            raise GateError(f"line {line}: {len(cells)} cells, expected {len(columns)}")
    return body


def parse(text: str) -> Page:
    lines = text.splitlines()
    page = Page()
    b, e = _block(lines, BEGIN, END)
    for line, c in _table(lines, b, e, COLUMNS):
        page.rows.append(Row(c[0], line, c[2], c[3], c[4], c[5]))
    b, e = _block(lines, VBEGIN, VEND)
    for line, c in _table(lines, b, e, VERDICT_COLUMNS):
        if c[0] in page.verdict:
            raise GateError(f"line {line}: the verdict lists {c[0]!r} twice")
        page.verdict[c[0]] = (line, c[1])
    return page


def _load_threat_map():
    """The threat map check's resolver, so both checks read a test the same way."""
    path = pathlib.Path(__file__).resolve().parent / "check-threat-map.py"
    spec = importlib.util.spec_from_file_location("check_threat_map_for_gate", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


THREAT_MAP = _load_threat_map()


def references(cell: str) -> tuple[list[tuple[str, str | None]], list[int], list[int]]:
    """The (path, name) references, the pull requests and the CI runs in an Evidence cell."""
    paths = []
    for raw in TICKED.findall(cell):
        path, _, name = raw.partition("::")
        path = path.strip()
        if PATH.fullmatch(path) and ("/" in path or FILE.search(path)):
            paths.append((path, name.strip() or None))
    runs = [int(n) for n in RUN.findall(cell)]
    # A run link's digits are not a pull request; neither is inline code.
    prose = TICKED.sub(" ", RUN.sub(" ", cell))
    prs = [int(n) for n in ISSUE.findall(prose)]
    return paths, prs, runs


def workflow_jobs(text: str) -> set[str]:
    """The job ids of a workflow file: the keys two spaces in under `jobs:`."""
    jobs: set[str] = set()
    inside = False
    for line in text.splitlines():
        if re.match(r"^jobs:\s*(#.*)?$", line):
            inside = True
            continue
        if inside and re.match(r"^\S", line) and not line.startswith("#"):
            break
        m = re.match(r"^  ([A-Za-z0-9_-]+):\s*(#.*)?$", line)
        if inside and m:
            jobs.add(m.group(1))
    return jobs


def resolve(root: pathlib.Path, path: str, name: str | None) -> str | None:
    """The problem with one `path` or `path::name` reference, or None."""
    target = root / path
    if not target.exists():
        return f"`{path}` does not exist"
    if name is None:
        return None
    if not target.is_file():
        return f"`{path}` is a directory, so `{path}::{name}` names nothing"
    if WORKFLOW.match(path):
        try:
            jobs = workflow_jobs(target.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError) as e:
            return f"`{path}` cannot be read: {e}"
        if name not in jobs:
            return f"`{path}` has no job `{name}`"
        return None
    problem, _ = THREAT_MAP.resolve(root, path, name, path.endswith((".rs", ".py")))
    return problem


def merged_prs_from_git(root: pathlib.Path) -> set[int] | None:
    """Pull requests squash-merged into HEAD, or None when history is shallow or absent."""
    try:
        shallow = subprocess.run(["git", "-C", str(root), "rev-parse", "--is-shallow-repository"],
                                 capture_output=True, text=True, check=True).stdout.strip()
        if shallow != "false":
            return None
        log = subprocess.run(["git", "-C", str(root), "log", "--format=%s", "HEAD"],
                             capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return None
    return {int(m.group(1)) for s in log.splitlines() if (m := re.search(r"\(#(\d+)\)\s*$", s))}


Fetch = Callable[[str], "dict | None"]


def github_fetch(path: str) -> dict | None:
    """GET api.github.com/<path>; None on a 404. Other failures raise GateError."""
    req = urllib.request.Request(f"https://api.github.com/{path}",
                                 headers={"Accept": "application/vnd.github+json", "User-Agent": "check-release-gate"})
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise GateError(f"GitHub API {path}: HTTP {e.code}") from None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
        raise GateError(f"GitHub API {path}: {e}") from None


def _date(text: str) -> dt.date | None:
    try:
        return dt.date.fromisoformat(text) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) else None
    except ValueError:
        return None


def check(page: Page, root: pathlib.Path, today: dt.date, merged: set[int] | None,
          fetch: Fetch | None, skip_prs: bool = False) -> list[str]:
    problems: list[str] = []

    ids = [r.id for r in page.rows]
    if ids != CRITERIA:
        missing = [c for c in CRITERIA if c not in ids]
        extra = [i for i in ids if i not in CRITERIA]
        twice = sorted({i for i in ids if ids.count(i) > 1})
        detail = "; ".join(x for x in (
            f"missing {missing}" if missing else "",
            f"not a criterion of #714: {extra}" if extra else "",
            f"listed twice: {twice}" if twice else "",
        ) if x) or "out of order"
        problems.append(f"the table must list the {len(CRITERIA)} criteria of #714 in order: {detail}")

    verdict = page.verdict
    for item in VERDICT_ITEMS:
        if item not in verdict:
            problems.append(f"the verdict has no {item!r} row")
    for item, (line, _) in verdict.items():
        if item not in VERDICT_ITEMS:
            problems.append(f"line {line}: the verdict has an unknown row {item!r}")
    evaluated = None
    if "Evaluated" in verdict:
        line, text = verdict["Evaluated"]
        evaluated = _date(text)
        if evaluated is None:
            problems.append(f"line {line}: Evaluated {text!r} is not a YYYY-MM-DD date")
        elif evaluated > today:
            problems.append(f"line {line}: Evaluated {evaluated} is in the future (today is {today})")
        elif evaluated < EARLIEST:
            problems.append(f"line {line}: Evaluated {evaluated} is before #714 ({EARLIEST})")

    open_issues: dict[int, list[str]] = {}
    for row in page.rows:
        where = f"{row.id} (line {row.line})"
        status = row.status.lower()
        if status not in STATUSES:
            problems.append(f"{where}: status {row.status!r} is not one of {list(STATUSES)}")

        checked = _date(row.checked)
        if checked is None:
            problems.append(f"{where}: Checked {row.checked!r} is not a YYYY-MM-DD date")
        elif checked < EARLIEST:
            problems.append(f"{where}: Checked {checked} is before #714 ({EARLIEST})")
        elif evaluated is not None and checked > evaluated:
            problems.append(f"{where}: Checked {checked} is after the page's Evaluated date {evaluated}")

        issues = [int(n) for n in ISSUE.findall(row.open)]
        if row.open.strip().lower() == "none":
            if status != "met":
                problems.append(f"{where}: a row that is {status!r} must name the issue that closes the gap")
        elif not issues:
            problems.append(f"{where}: Open must be \"none\" or name issues as #N, not {row.open!r}")
        elif status == "met":
            problems.append(f"{where}: a met row names open issues {['#%d' % n for n in issues]}: "
                            "the criterion is not met, or the issues belong elsewhere")
        if status != "met":
            for n in issues:
                open_issues.setdefault(n, []).append(row.id)

        paths, prs, runs = references(row.evidence)
        if status in ("met", "partial") and not (paths or prs or runs):
            problems.append(f"{where}: a {status} row names no evidence (a file, test, CI job, pull request or run)")
        for path, name in paths:
            problem = resolve(root, path, name)
            if problem:
                problems.append(f"{where}: {problem}")
        for n in prs:
            if skip_prs:
                continue
            if merged is not None:
                if n not in merged:
                    problems.append(f"{where}: #{n} is not a pull request merged into this branch")
            elif fetch is not None:
                pr = fetch(f"repos/{REPO}/pulls/{n}")
                if pr is None or not pr.get("merged_at"):
                    problems.append(f"{where}: #{n} is not a merged pull request")
        for run in runs:
            if fetch is None:
                continue
            data = fetch(f"repos/{REPO}/actions/runs/{run}")
            if data is None:
                problems.append(f"{where}: CI run {run} does not exist")
            elif status == "met" and data.get("conclusion") != "success":
                problems.append(f"{where}: CI run {run} concluded {data.get('conclusion')!r}, "
                                "and a met row's runs must have succeeded")

    if fetch is not None:
        state: dict[int, str] = {}
        for n in sorted(open_issues):
            data = fetch(f"repos/{REPO}/issues/{n}")
            if data is None:
                problems.append(f"#{n} (named by {', '.join(open_issues[n])}) does not exist")
            elif "pull_request" in data:
                problems.append(f"#{n} (named by {', '.join(open_issues[n])}) is a pull request, not an issue")
            else:
                state[n] = data.get("state", "")
        for row in page.rows:
            issues = [int(n) for n in ISSUE.findall(row.open)]
            if row.status.lower() != "met" and issues and all(state.get(n) == "closed" for n in issues):
                problems.append(f"{row.id} (line {row.line}): every issue it names is closed "
                                f"({', '.join('#%d' % n for n in issues)}): re-evaluate the row")

    counts = {s: sum(1 for r in page.rows if r.status.lower() == s) for s in STATUSES}
    for item, status in (("Met", "met"), ("Partial", "partial"), ("Not met", "not met")):
        if item in verdict:
            line, text = verdict[item]
            if text != str(counts[status]):
                problems.append(f"line {line}: the verdict says {item} {text}, the rows have {counts[status]}")
    rc = all(r.status.lower() == "met" for r in page.rows if r.id != QUIET_PERIOD)
    final = rc and all(r.status.lower() == "met" for r in page.rows)
    for item, ok in (("1.0.0-rc.1 may be cut", rc), ("1.0.0 may be cut", final)):
        if item in verdict:
            line, text = verdict[item]
            want = "yes" if ok else "no"
            if text.lower() != want:
                problems.append(f"line {line}: the verdict says {item} {text!r}; the rows say {want!r}")
    return problems


def main(argv: list[str] | None = None, fetch: Fetch | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--doc", type=pathlib.Path, default=DEFAULT_DOC)
    ap.add_argument("--root", type=pathlib.Path, default=ROOT, help="the repository root references are relative to")
    ap.add_argument("--online", action="store_true", help="also check runs, issues and (in a shallow clone) pull requests with the GitHub API")
    ap.add_argument("--no-history", action="store_true",
                    help="skip the pull request checks, for a shallow clone that has no history and no API; "
                         "ci.yml runs it so, the docs workflow checks the pull requests")
    ap.add_argument("--today", type=dt.date.fromisoformat, default=dt.datetime.now(dt.timezone.utc).date())
    args = ap.parse_args(argv)

    try:
        page = parse(args.doc.read_text(encoding="utf-8"))
        merged = merged_prs_from_git(args.root)
        if args.online and fetch is None:
            fetch = github_fetch
        if not args.online:
            fetch = None
        if merged is None and fetch is None and not args.no_history:
            raise GateError("git history is shallow or missing, so merged pull requests cannot be read: "
                            "fetch the full history (fetch-depth: 0) or pass --online")
        problems = check(page, args.root, args.today, merged, fetch, skip_prs=merged is None and fetch is None)
    except (OSError, UnicodeDecodeError, GateError) as e:
        print(f"release gate check: cannot run: {e}")
        return 2

    if problems:
        print(f"release gate check: {args.doc} names evidence that does not exist, or disagrees with itself:")
        for p in problems:
            print(f"  {p}")
        return 1
    runs = sum(len(references(r.evidence)[2]) for r in page.rows)
    print(f"release gate check: {len(page.rows)} criteria, every reference resolves"
          + (f" ({runs} CI runs and the open issues checked online)" if fetch
             else " (offline: CI runs and issues not checked" + (", nor pull requests)" if merged is None else ")")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
