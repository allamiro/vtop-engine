#!/usr/bin/env python3
"""Check the provenance of fixtures written by released binaries (#743).

A fixture from a released binary lives in ``crates/<crate>/tests/fixtures/
<format>/<tag>/``, where ``<tag>`` is a final release (``vX.Y.Z``). That
directory must hold a ``PROVENANCE`` file (YAML) saying which published
binary wrote it and how:

    tag: v0.9.0
    asset: vtop-engine-0.9.0-x86_64-unknown-linux-gnu.tar.gz
    asset_sha256: <the archive's sha256, as the release's SHA256SUMS lists it>
    binary: vtopctl
    command: <the command line that wrote the fixture>
    input: <what it was given>
    files:
      <name>: <sha256 of that fixture file>

The check fails when a tag directory has no PROVENANCE, a field is missing,
the asset is not that release's or its digest is not the one (listed once)
in the release's SHA256SUMS, or a fixture file differs from the digest
PROVENANCE recorded for it, or is missing from or absent in the ``files``
list. So a fixture edited without its PROVENANCE is caught. What it does
not prove on its own is that the recorded digests are what the released
binary wrote: regenerate-fixtures.sh records them as it writes the fixture,
and review of the PROVENANCE edit is what keeps them honest.

Usage: check_provenance.py [repo-root]   (needs ``gh`` and GH_TOKEN)
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import re
import subprocess
import sys
from typing import Callable

FINAL = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
REPO = os.environ.get("FETCH_RELEASE_REPO", "allamiro/vtop-engine")
FIELDS = ("tag", "asset", "asset_sha256", "binary", "command", "input", "files")
HEX64 = re.compile(r"^[0-9a-f]{64}$")


class ProvenanceError(RuntimeError):
    pass


def release_sums(tag: str) -> str:
    """The release's SHA256SUMS, from GitHub."""
    result = subprocess.run(
        ["gh", "release", "download", tag, "--repo", REPO, "--pattern", "SHA256SUMS", "--output", "-"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise ProvenanceError(f"cannot read the SHA256SUMS of {tag}: {result.stderr.strip()}")
    return result.stdout


def fixture_dirs(root: pathlib.Path) -> list[pathlib.Path]:
    found = []
    for fixtures in sorted(root.glob("crates/*/tests/fixtures")):
        for path in sorted(fixtures.rglob("*")):
            if path.is_dir() and FINAL.match(path.name):
                found.append(path)
    return found


def sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check(root: pathlib.Path, sums_for: Callable[[str], str]) -> list[str]:
    import yaml  # PyYAML, pinned in scripts/compat/requirements.txt

    problems: list[str] = []
    cache: dict[str, str] = {}
    for directory in fixture_dirs(root):
        where = directory.relative_to(root)
        provenance = directory / "PROVENANCE"
        if not provenance.is_file():
            problems.append(f"{where}: no PROVENANCE, so nothing says which released binary wrote it")
            continue
        try:
            fields = yaml.safe_load(provenance.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as error:
            problems.append(f"{where}/PROVENANCE: not YAML: {error}")
            continue
        missing = [name for name in FIELDS if not fields.get(name)]
        if missing:
            problems.append(f"{where}/PROVENANCE: missing {', '.join(missing)}")
            continue
        tag = str(fields["tag"])
        if tag != directory.name:
            problems.append(f"{where}/PROVENANCE: says {tag}; its directory says {directory.name}")
            continue
        asset = str(fields["asset"])
        if not re.fullmatch(rf"vtop-engine-{re.escape(tag[1:])}-[a-z0-9_]+-[a-z0-9_-]+\.tar\.gz", asset):
            problems.append(f"{where}/PROVENANCE: {asset} is not an archive of {tag}")
            continue
        digest = str(fields["asset_sha256"])
        if tag not in cache:
            try:
                cache[tag] = sums_for(tag)
            except ProvenanceError as error:
                problems.append(f"{where}: {error}")
                continue
        listed: dict[str, list[str]] = {}
        for parts in (line.split() for line in cache[tag].splitlines()):
            if len(parts) == 2:
                listed.setdefault(parts[1].removeprefix("./"), []).append(parts[0])
        if len(listed.get(asset, [])) > 1:
            problems.append(f"{where}/PROVENANCE: {asset} is listed more than once in {tag}'s SHA256SUMS")
        elif listed.get(asset, [None])[0] != digest:
            problems.append(
                f"{where}/PROVENANCE: {asset} sha256 {digest} is not what {tag}'s SHA256SUMS lists "
                f"({(listed.get(asset) or ['nothing'])[0]})"
            )
        files = fields["files"]
        if not isinstance(files, dict):
            problems.append(f"{where}/PROVENANCE: files must map each fixture file to its sha256")
            continue
        present = {
            str(path.relative_to(directory))
            for path in directory.rglob("*")
            if path.is_file() and path.name != "PROVENANCE"
        }
        for name in sorted(present - set(files)):
            problems.append(f"{where}/{name}: not listed in PROVENANCE files")
        if not all(isinstance(name, str) for name in files):
            problems.append(f"{where}/PROVENANCE: every files key must be a path string")
            continue
        for name, want in sorted(files.items()):
            path = directory / name
            if not path.resolve().is_relative_to(directory.resolve()):
                problems.append(f"{where}/PROVENANCE: {name} is outside its fixture directory")
                continue
            if not path.is_file():
                problems.append(f"{where}/{name}: listed in PROVENANCE but missing")
            elif not HEX64.match(str(want)):
                problems.append(f"{where}/PROVENANCE: {name} has no sha256")
            elif sha256(path) != want:
                problems.append(
                    f"{where}/{name}: sha256 {sha256(path)} is not the {want} the released binary wrote"
                )
    return problems


def main(argv: list[str]) -> int:
    root = pathlib.Path(argv[0] if argv else ".").resolve()
    problems = check(root, release_sums)
    for problem in problems:
        print(f"error: {problem}", file=sys.stderr)
    count = len(fixture_dirs(root))
    if problems:
        print(f"{len(problems)} provenance problem(s) in {count} released-binary fixture(s)", file=sys.stderr)
        return 1
    print(f"ok: {count} released-binary fixture(s), each with a PROVENANCE that checks out")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
