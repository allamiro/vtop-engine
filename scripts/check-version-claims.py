#!/usr/bin/env python3
"""Fail when a version the docs state has drifted from the workspace version.

The workspace version in Cargo.toml is the one the release workflow checks
the tag against. Several other files repeat it by hand, and nothing tied them
to it: the README's BibTeX entry still said 0.8.0 after 0.9.0 shipped, and
the install pages printed the previous chart and image. This check reads each
of them and names every one that disagrees:

- README.md, the BibTeX `version = {X.Y.Z}`;
- CITATION.cff, `version: "X.Y.Z"`;
- helm/vtop/Chart.yaml, `appVersion: "X.Y.Z"` (the default image tag);
- docs/install/helm.md, the chart version and appVersion rows (against
  Chart.yaml) and the `--branch vX.Y.Z` checkout;
- docs/install/*.md, every `ghcr.io/allamiro/vtop-engine:X.Y.Z` image.

It also refuses a dated status lead-in such as "As of August 2026 (repo at
v0.3.0)" in any page: that sentence was true once and then stood for six
releases. A status section describes `main` and points at ROADMAP.md for
what each release shipped.

Usage: scripts/check-version-claims.py [REPO_ROOT]
"""

import pathlib
import re
import sys

SEMVER = r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?"
IMAGE = re.compile(r"ghcr\.io/allamiro/vtop-engine:(" + SEMVER + r")\b")
BRANCH = re.compile(r"--branch v(" + SEMVER + r")\b")
DATED_LEAD_IN = re.compile(r"\bas of\b[^\n]{0,60}\(\s*repo at v?\d", re.IGNORECASE)


def line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def first(pattern: str, text: str):
    return re.search(pattern, text, re.MULTILINE)


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    problems: list[str] = []

    def read(rel: str) -> str:
        path = root / rel
        if not path.is_file():
            problems.append(f"{rel}: file not found")
            return ""
        return path.read_text(encoding="utf-8")

    def expect(rel: str, text: str, match, what: str, want: str) -> None:
        if match is None:
            problems.append(f"{rel}: no {what} found; expected {want}")
        elif match.group(1) != want:
            problems.append(
                f"{rel}:{line_of(text, match.start())}: {what} is "
                f"{match.group(1)}, expected {want}"
            )

    cargo = read("Cargo.toml")
    package = re.search(r"^\[workspace\.package\]\s*$(.*?)(?=^\[|\Z)", cargo, re.M | re.S)
    version_line = (
        re.search(r'^version\s*=\s*"(' + SEMVER + r')"', package.group(1), re.MULTILINE)
        if package
        else None
    )
    if version_line is None:
        print("version claims: no [workspace.package] version in Cargo.toml", file=sys.stderr)
        return 1
    version = version_line.group(1)

    readme = read("README.md")
    expect("README.md", readme,
           first(r"^\s*version\s*=\s*\{(" + SEMVER + r")\}", readme),
           "BibTeX version", version)

    citation = read("CITATION.cff")
    expect("CITATION.cff", citation,
           first(r'^version:\s*"?(' + SEMVER + r')"?\s*$', citation),
           "version", version)

    chart = read("helm/vtop/Chart.yaml")
    expect("helm/vtop/Chart.yaml", chart,
           first(r'^appVersion:\s*"?(' + SEMVER + r')"?\s*$', chart),
           "appVersion", version)
    chart_version = first(r"^version:\s*\"?(" + SEMVER + r")\"?\s*$", chart)

    helm_page = read("docs/install/helm.md")
    if chart_version is not None:
        expect("docs/install/helm.md", helm_page,
               first(r"^\|\s*Chart version\s*\|\s*`(" + SEMVER + r")`", helm_page),
               "chart version row", chart_version.group(1))
    expect("docs/install/helm.md", helm_page,
           first(r"^\|\s*`appVersion`[^|]*\|\s*`(" + SEMVER + r")`", helm_page),
           "appVersion row", version)
    expect("docs/install/helm.md", helm_page,
           BRANCH.search(helm_page), "--branch checkout", f"{version}")

    for page in sorted((root / "docs" / "install").glob("*.md")):
        rel = page.relative_to(root).as_posix()
        text = page.read_text(encoding="utf-8")
        for m in IMAGE.finditer(text):
            if m.group(1) != version:
                problems.append(
                    f"{rel}:{line_of(text, m.start())}: image tag {m.group(1)}, "
                    f"expected {version}"
                )

    pages = [root / "README.md", root / "CONTRIBUTING.md"]
    pages += sorted((root / "docs").rglob("*.md")) if (root / "docs").is_dir() else []
    for page in pages:
        if not page.is_file():
            continue
        rel = page.relative_to(root).as_posix()
        text = page.read_text(encoding="utf-8")
        for m in DATED_LEAD_IN.finditer(text):
            problems.append(
                f"{rel}:{line_of(text, m.start())}: dated status lead-in "
                f"{m.group(0)!r} (\"repo at\" a version); describe main and "
                "point at ROADMAP.md for each release"
            )

    if problems:
        print(f"version claims: {len(problems)} disagree with the workspace "
              f"version {version} (Cargo.toml):")
        for p in problems:
            print(f"  {p}")
        return 1
    print(f"version claims: all agree with {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
