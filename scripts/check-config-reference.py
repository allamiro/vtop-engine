#!/usr/bin/env python3
"""Fail when a config.yaml key in config.rs is missing from the reference page.

docs/configure/reference.md is written by hand so each key can carry its
meaning, which means a new field in crates/vtop-core/src/config.rs could land
without a row. This check reads the field names of every struct the page
documents (honouring `#[serde(rename = ...)]` and skipping `#[serde(skip)]`)
and names each one the page does not mention.

Usage: scripts/check-config-reference.py [REPO_ROOT]
"""

import pathlib
import re
import sys

STRUCTS = [
    "VtopConfig", "EngineConfig", "BatchingConfig", "AdaptiveWidthConfig",
    "CompressionConfig", "ChecksumConfig", "SourcesConfig", "KafkaSourceConfig",
    "FileSourceConfig", "SyslogSpoolConfig", "UploadConfig", "EgressTuning",
    "PartitioningConfig", "ManifestConfig",
]


def fields(src: str, struct: str) -> list[str]:
    m = re.search(r"pub struct " + struct + r" \{(.*?)\n\}", src, re.S)
    if m is None:
        sys.exit(f"config reference check: struct {struct} not found in config.rs")
    out, rename, skip = [], None, False
    for line in m.group(1).split("\n"):
        r = re.search(r'rename = "(\w+)"', line)
        if r:
            rename = r.group(1)
        if "#[serde(skip)]" in line:
            skip = True
        f = re.match(r"\s*pub (\w+):", line)
        if f:
            if not skip:
                out.append(rename or f.group(1))
            rename, skip = None, False
    return out


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    src = (root / "crates/vtop-core/src/config.rs").read_text(encoding="utf-8")
    doc = (root / "docs/configure/reference.md").read_text(encoding="utf-8")
    missing = [
        f"{s}.{name}"
        for s in STRUCTS
        for name in fields(src, s)
        if not re.search(r"`[\w.<>]*\b" + re.escape(name) + r"\b", doc)
    ]
    if missing:
        print("config reference check: docs/configure/reference.md does not document:")
        for m in missing:
            print(f"  {m}")
        return 1
    total = sum(len(fields(src, s)) for s in STRUCTS)
    print(f"config reference check: all {total} keys documented")
    return 0


if __name__ == "__main__":
    sys.exit(main())
