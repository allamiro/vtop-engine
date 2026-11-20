#!/usr/bin/env python3
"""Fail when SECURITY.md loses a part the disclosure policy needs.

SECURITY.md is what GitHub shows as the repository's security policy, and the
production-readiness bar (#714, criterion D5) asks it to name a private
channel, an acknowledgement time, a fix target by severity, the supported
versions and the coordinated-disclosure rule. It is prose, edited by hand, so
this check reads it and fails when:

  - the private-advisory link, or the fallback ("Security contact request")
    for when private reporting is off, is missing;
  - no line gives an acknowledgement time in days;
  - the fix-target table lacks a critical, high, medium or low row, or a row
    has an empty target;
  - the "Supported versions" section is missing or empty, or has no
    "| X.Y.x | yes |" row for the workspace version in Cargo.toml, so a minor
    release cannot leave the policy naming the previous one;
  - the "Coordinated disclosure" section is missing or gives no day count;
  - a relative link in SECURITY.md points at a path that does not exist;
  - blank issues are off (.github/ISSUE_TEMPLATE/config.yml) and no issue form
    is titled "Security contact request", so the fallback cannot be used;
  - README.md or CONTRIBUTING.md does not link to SECURITY.md, or
    docs/security-policy.md does not include it.

Usage: scripts/check-security-policy.py [REPO_ROOT]
"""

from __future__ import annotations

import pathlib
import re
import sys

ADVISORY_URL = "https://github.com/allamiro/vtop-engine/security/advisories/new"
FALLBACK_TITLE = "Security contact request"
SEVERITIES = ["Critical", "High", "Medium", "Low"]
INCLUDE = "<!-- vtop:include SECURITY.md -->"
# Inline links: [text](target). The target stops at whitespace or ')'.
LINK = re.compile(r"\[[^\]]*\]\(\s*<?([^)\s>]+)>?")
DAYS = re.compile(r"\b\d+\s+(?:working\s+)?days?\b", re.I)


def _section(text: str, heading: str) -> str | None:
    """The body of `## heading`, up to the next `## ` heading; None if absent."""
    m = re.search(r"^## " + re.escape(heading) + r"[ \t]*\n(.*?)(?=^## |\Z)", text, re.M | re.S)
    return None if m is None else m.group(1).strip()


def _table_row(text: str, first_cell: str) -> list[str] | None:
    for line in text.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if line.lstrip().startswith("|") and cells and cells[0] == first_cell:
            return cells
    return None


def _policy_problems(root: pathlib.Path, text: str) -> list[str]:
    problems = []
    if ADVISORY_URL not in text:
        problems.append(f"SECURITY.md does not give the private-advisory link {ADVISORY_URL}")
    if FALLBACK_TITLE not in text:
        problems.append(
            f'SECURITY.md does not give the fallback for when private reporting is off '
            f'(an issue titled "{FALLBACK_TITLE}")'
        )
    if not any("acknowledg" in line.lower() and DAYS.search(line) for line in text.splitlines()):
        problems.append("SECURITY.md gives no acknowledgement time (a line naming the acknowledgement and a number of days)")
    for severity in SEVERITIES:
        row = _table_row(text, severity)
        if row is None:
            problems.append(f"SECURITY.md's fix-target table has no {severity.lower()} row (| {severity} | <fix target> |)")
        elif len(row) < 2 or not row[1]:
            problems.append(f"SECURITY.md's fix-target table gives {severity.lower()} no target")
    supported = _section(text, "Supported versions")
    if not supported:
        problems.append('SECURITY.md has no "## Supported versions" section, or it is empty')
    else:
        current = _workspace_minor(root)
        row = _table_row(supported, f"{current}.x") if current else None
        if current and not (row and len(row) > 1 and row[1].lower().startswith("yes")):
            problems.append(
                f'SECURITY.md\'s "Supported versions" section has no "| {current}.x | yes |" row, '
                f"but Cargo.toml's workspace version is {current}: update it with the release"
            )
    disclosure = _section(text, "Coordinated disclosure")
    if disclosure is None:
        problems.append('SECURITY.md has no "## Coordinated disclosure" section')
    elif not DAYS.search(disclosure):
        problems.append("SECURITY.md's coordinated disclosure section gives no number of days before disclosure")
    for target in LINK.findall(text):
        if re.match(r"^[a-z][a-z0-9+.-]*:", target, re.I) or target.startswith("#"):
            continue  # external, or an anchor on the same page
        path = target.split("#", 1)[0]
        if not (root / path).exists():
            problems.append(f"SECURITY.md links to {path}, which does not exist")
    return problems


def _workspace_minor(root: pathlib.Path) -> str | None:
    """MAJOR.MINOR of [workspace.package] version in Cargo.toml, if there is one."""
    cargo = root / "Cargo.toml"
    if not cargo.is_file():
        return None
    m = re.search(r'^\[workspace\.package\][^\[]*?^version\s*=\s*"(\d+)\.(\d+)\.',
                  cargo.read_text(encoding="utf-8"), re.M | re.S)
    return f"{m.group(1)}.{m.group(2)}" if m else None


def _fallback_problems(root: pathlib.Path) -> list[str]:
    """The fallback is an issue titled FALLBACK_TITLE. With blank issues off,
    only an issue form can open one, so a form must carry that title."""
    templates = root / ".github" / "ISSUE_TEMPLATE"
    config = templates / "config.yml"
    text = config.read_text(encoding="utf-8") if config.is_file() else ""
    if not re.search(r"^blank_issues_enabled:\s*false\b", text, re.M):
        return []
    for form in sorted(templates.glob("*.y*ml")):
        if form.name.startswith("config."):
            continue
        if re.search(r'^title:\s*["\']?' + re.escape(FALLBACK_TITLE), form.read_text(encoding="utf-8"), re.M):
            return []
    return [
        f'blank_issues_enabled is false in {config.relative_to(root)}, and no issue form is titled '
        f'"{FALLBACK_TITLE}", so the fallback SECURITY.md describes cannot be opened'
    ]


def check(root: pathlib.Path) -> list[str]:
    policy = root / "SECURITY.md"
    if not policy.is_file():
        return ["SECURITY.md does not exist at the repository root, where GitHub looks for it"]
    problems = _policy_problems(root, policy.read_text(encoding="utf-8"))
    problems += _fallback_problems(root)
    for name in ("README.md", "CONTRIBUTING.md"):
        p = root / name
        text = p.read_text(encoding="utf-8") if p.is_file() else ""
        if not re.search(r"\]\(\s*<?(?:\./)?SECURITY\.md[)#>]", text):
            problems.append(f"{name} does not link to SECURITY.md")
    page = root / "docs" / "security-policy.md"
    if not (page.is_file() and INCLUDE in page.read_text(encoding="utf-8")):
        problems.append(f"docs/security-policy.md does not include the policy ({INCLUDE})")
    return problems


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    root = pathlib.Path(argv[0] if argv else ".")
    problems = check(root)
    if problems:
        print("security policy check: failed:")
        for p in problems:
            print(f"  {p}")
        return 1
    print("security policy check: SECURITY.md names the private channel and its fallback, an "
          "acknowledgement time, fix targets for critical, high, medium and low, the supported "
          "versions and the disclosure rule; README.md, CONTRIBUTING.md and the docs site link to it")
    return 0


if __name__ == "__main__":
    sys.exit(main())
