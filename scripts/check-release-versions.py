#!/usr/bin/env python3
"""Fail when a place readers copy the release version from names another one.

The release bump moves the workspace version in Cargo.toml. These places are
edited by hand, and each has gone stale before (the container page pulled
0.8.0 through v0.9.0 and v0.10.0; the README's BibTeX stayed at 0.8.0):

  - docs/install/container.md: every pinned image reference
    (ghcr.io/allamiro/vtop-engine:X.Y.Z) and the "currently X.Y.Z" note beside
    the `latest` pull. At least one pinned reference must exist;
  - README.md: the BibTeX `version = {X.Y.Z}`;
  - CITATION.cff: `version: "X.Y.Z"`;
  - helm/vtop/Chart.yaml: `appVersion: "X.Y.Z"` (the chart's default image tag).

Each must equal the workspace version, so a bump PR that misses one fails.

Usage: scripts/check-release-versions.py [REPO_ROOT]
"""

from __future__ import annotations

import pathlib
import re
import sys

IMAGE = "ghcr.io/allamiro/vtop-engine"
# A pinned tag: the image followed by ':' and anything but whitespace, a
# backtick or a closing bracket. `latest` is checked separately.
PINNED = re.compile(re.escape(IMAGE) + r":([^\s`)\]]+)")
CURRENTLY = re.compile(r"\bcurrently\s+`?(\d+\.\d+\.\d+[^\s`]*?)`?(?=[\s.,;)]|$)")
WORKSPACE_VERSION = re.compile(r'^version\s*=\s*"([^"]+)"', re.M)
BIBTEX = re.compile(r"^\s*version\s*=\s*\{([^}]*)\}", re.M)
CITATION = re.compile(r'^version:\s*"?([^"\s]+)"?\s*$', re.M)
APP_VERSION = re.compile(r'^appVersion:\s*"?([^"\s]+)"?\s*$', re.M)


def workspace_version(root: pathlib.Path) -> str:
    text = (root / "Cargo.toml").read_text()
    section = text.split("[workspace.package]", 1)
    if len(section) != 2:
        raise SystemExit("Cargo.toml has no [workspace.package] section")
    m = WORKSPACE_VERSION.search(section[1])
    if not m:
        raise SystemExit("Cargo.toml [workspace.package] has no version")
    return m.group(1)


def _line(text: str, pos: int) -> int:
    return text.count("\n", 0, pos) + 1


def check(root: pathlib.Path) -> list[str]:
    version = workspace_version(root)
    problems: list[str] = []

    page = root / "docs/install/container.md"
    text = page.read_text()
    pinned = 0
    for m in PINNED.finditer(text):
        tag = m.group(1)
        if tag == "latest":
            continue
        pinned += 1
        if tag != version:
            problems.append(
                f"{page.relative_to(root)}:{_line(text, m.start())}: pulls {IMAGE}:{tag}, "
                f"but the workspace version is {version}"
            )
    if pinned == 0:
        problems.append(f"{page.relative_to(root)}: no pinned {IMAGE}:{version} reference")
    for m in CURRENTLY.finditer(text):
        if m.group(1) != version:
            problems.append(
                f"{page.relative_to(root)}:{_line(text, m.start())}: says currently {m.group(1)}, "
                f"but the workspace version is {version}"
            )

    for rel, pattern, what in (
        ("README.md", BIBTEX, "the BibTeX version"),
        ("CITATION.cff", CITATION, "version"),
        ("helm/vtop/Chart.yaml", APP_VERSION, "appVersion"),
    ):
        text = (root / rel).read_text()
        found = pattern.findall(text)
        if not found:
            problems.append(f"{rel}: {what} not found")
        for value in found:
            if value != version:
                problems.append(f"{rel}: {what} is {value}, but the workspace version is {version}")
    return problems


def main(argv: list[str]) -> int:
    root = pathlib.Path(argv[1] if len(argv) > 1 else ".").resolve()
    problems = check(root)
    if problems:
        for p in problems:
            print(p)
        print(f"{len(problems)} release-version problem(s): move each to the workspace version")
        return 1
    print(f"release versions: every copied version names {workspace_version(root)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
