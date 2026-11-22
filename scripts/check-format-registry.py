#!/usr/bin/env python3
"""Keep the format registry true (#743).

scripts/compat/formats.yaml lists every persisted and wire format, and for
each the source constants that define it. This check reads the sources and
fails when:

* a magic (a ``b"VT..."`` byte string), a constant whose name carries
  VERSION, SCHEMA, MAGIC, PROTOCOL or FEATURE_LEVEL, is in no entry and not
  in ``not_formats``: a format, or a version of one, the inventory does not
  know;
* a listed constant's value differs from the registry's: a version bump, a
  new feature level or a new magic made without editing the registry, so
  without the review the policy asks for;
* the highest tag of a listed tag set (command kinds, snapshot value tags,
  key categories) differs from the registry's;
* the registry names something the sources no longer hold;
* a fuzz target in fuzz/fuzz_targets is named by no entry's ``fuzz`` list and
  is not in ``fuzz_other`` with a reason;
* an entry names a compatibility cell that scripts/compat/expected.yaml does
  not list, or that file lists a cell no entry names;
* a format constant, or a tag, is declared twice in one file (in two
  modules): the registry names a constant by file and name, so it could not
  tell which declaration it records;
* the inventory table on docs/reference/compatibility.md differs from what
  the registry generates;
* a golden file (a committed fixture, snapshot or golden fuzz seed) differs
  from the SHA-256 the lock records, has no row in it, is gone, or is named
  by no format: the byte lock (policy rule 6);
* an entry's ``state`` is not one of ``unfrozen`` and ``frozen``, a frozen
  entry lacks what freezing needs, or a frozen entry's constants, tags,
  magics or golden bytes no longer match the digest recorded when it froze
  (or when its last recorded exception was made).

Test code is not scanned: ``crates/*/tests``, ``benches``, and every
``#[cfg(test)]`` module (its braces matched, skipping strings, raw and byte
strings, char literals and nested comments). Nor are comments: a declaration
or magic kept in one is not code.

Not covered, so a reviewer still has to look: native protocol and VTPM
message kinds (match arms, not constants), ``effective_version`` (a literal
in the JSON it prints), ``FORMAT_NAME``, and magics or versions written in
other shapes than a ``b"VT..."`` literal or a constant whose name carries
one of the tokens above. A ``marker`` may quote one of its entry's constants
as ``{NAME}``, so the table follows the value.

The byte lock. ``golden_lock`` in the registry names a lock file
(scripts/compat/golden.lock, one ``sha256sum``-style row per golden file).
A golden file is every file an entry names under ``fixtures`` or ``golden``
(a file, a directory, or a glob), and every committed file under
``crates/*/tests/fixtures``, ``crates/*/tests/snapshots``,
``docs/surface`` and ``fuzz/corpus/**/golden-*`` must be named by an entry.
Changing one of them deliberately is a registry-side act:
``check-format-registry.py --write-lock`` rewrites the lock, and the diff
of the lock is what a reviewer reads.

Frozen formats. ``state: frozen`` with a ``frozen`` block records the release
it froze at and a digest of everything that defines the format here: its
constants, magics, tag sets and the SHA-256 of its golden files
(``--digest ID`` prints it). After that the digest must not move, so a
version bump, a new tag or a changed golden file fails until the change is
recorded as an exception in ``frozen.exceptions`` (the new digest, a
reference and a reason). With ``--base REV`` the registry is also compared
with the one at REV, so a frozen format cannot be quietly un-frozen,
re-dated or re-digested, and its recorded exceptions only grow.

Usage:
    check-format-registry.py                check
    check-format-registry.py --write        regenerate the page's inventory table
    check-format-registry.py --write-lock   regenerate the golden-file lock
    check-format-registry.py --digest ID    print a format's freeze digest
    check-format-registry.py --base REV     also compare frozen states with REV
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
REGISTRY = pathlib.Path("scripts/compat/formats.yaml")
EXPECTED = pathlib.Path("scripts/compat/expected.yaml")
PAGE = pathlib.Path("docs/reference/compatibility.md")
BEGIN = "<!-- inventory:begin -->"
END = "<!-- inventory:end -->"
STATES = ("unfrozen", "frozen")
# Where golden files live, whatever an entry names: every file found here
# must belong to a format (the byte lock, policy rule 6).
GOLDEN_GLOBS = (
    "crates/*/tests/fixtures/**/*",
    "crates/*/tests/snapshots/**/*",
    "docs/surface/**/*",
    "fuzz/corpus/**/golden-*",
)
# Prose beside a fixture, not an input a test reads.
GOLDEN_SKIP = {"README.md"}
LOCK_HEADER = (
    "# The byte lock on golden files (#743, scripts/check-format-registry.py).\n"
    "# One row per golden file: its SHA-256 and path. Do not edit by hand: a\n"
    "# changed golden file is a format change, accepted deliberately with\n"
    "#   python3 scripts/check-format-registry.py --write-lock\n"
    "# and the diff of this file is what review reads.\n"
)
FINAL_OR_RC = re.compile(r"^v\d+\.\d+\.\d+(-[0-9A-Za-z.]+)?$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
GENERATED_NOTE = (
    "<!-- Generated by scripts/check-format-registry.py --write from "
    "scripts/compat/formats.yaml. Edit the registry, not this table. -->"
)

CONST = re.compile(
    r"^[ \t]*(?:pub(?:\([^)]*\))?[ \t]+)?(?:const|static)[ \t]+([A-Z][A-Z0-9_]*)[ \t]*:[^=\n]*=\s*([^;]*?);",
    re.M,
)
MAGIC = re.compile(r'b"(VT[A-Z0-9]{2,6})"')
TOKENS = (
    {"VERSION"}, {"VERSIONS"}, {"SCHEMA"}, {"SCHEMAS"}, {"SCHEME"}, {"MAGIC"},
    {"PROTOCOL"}, {"FEATURE", "LEVEL"},
)
# A raw string, also as a byte or C string (br"..", cr#".."#).
RAW_STRING = re.compile(r'[bc]?r(#*)"')
CHAR_LITERAL = re.compile(r"'(\\.|[^\\'])'")
# Indented too: rustfmt indents a test module nested in an inline module.
TEST_MODULE = re.compile(r"^[ \t]*#\[cfg\(test\)\]\s*\n\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*", re.M)


def _literal_or_comment(text: str, i: int) -> tuple[str, int] | None:
    """("comment" | "literal", end) when a comment, a string (plain, raw,
    byte or C) or a char literal starts at ``i``; None otherwise."""
    if text.startswith("//", i):
        newline = text.find("\n", i)
        return "comment", len(text) if newline < 0 else newline
    if text.startswith("/*", i):
        # Block comments nest in Rust.
        nesting = 0
        j = i
        while j < len(text):
            if text.startswith("/*", j):
                nesting += 1
                j += 2
            elif text.startswith("*/", j):
                nesting -= 1
                j += 2
                if nesting == 0:
                    return "comment", j
            else:
                j += 1
        return "comment", len(text)
    raw = RAW_STRING.match(text, i)
    if raw and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
        closing = '"' + raw.group(1)
        j = text.find(closing, raw.end())
        return "literal", len(text) if j < 0 else j + len(closing)
    if text[i] == '"':
        j = i + 1
        while j < len(text) and text[j] != '"':
            j += 2 if text[j] == "\\" else 1
        return "literal", min(j + 1, len(text))
    char = CHAR_LITERAL.match(text, i) if text[i] == "'" else None
    if char:
        return "literal", char.end()
    return None


def without_comments(text: str) -> str:
    """The text with every comment blanked (line breaks kept), and string
    and char literals untouched: a declaration or magic kept in a comment
    is not code, and must neither satisfy the registry nor shadow the live
    declaration's value (review of #783)."""
    out: list[str] = []
    i = 0
    plain = 0
    while i < len(text):
        found = _literal_or_comment(text, i)
        if found is None:
            i += 1
            continue
        kind, end = found
        if kind == "comment":
            out.append(text[plain:i])
            out.append(re.sub(r"[^\n]", " ", text[i:end]))
            plain = end
        i = end
    out.append(text[plain:])
    return "".join(out)


def _block_end(text: str, opening: int) -> int:
    """The index just past the brace that closes the one at ``opening``,
    skipping braces inside comments, string and char literals."""
    depth = 0
    i = opening
    while i < len(text):
        found = _literal_or_comment(text, i)
        if found is not None:
            i = found[1]
            continue
        c = text[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return len(text)


def production_source(text: str, out_of_line: list[str] | None = None) -> str:
    """The file without its ``#[cfg(test)]`` modules. The names of test
    modules declared out of line (``mod tests;``) are appended to
    ``out_of_line``, so their files can be skipped too."""
    while True:
        match = TEST_MODULE.search(text)
        if not match:
            return text
        if text.startswith(";", match.end()):
            # `mod tests;`: only the declaration goes; the module's own
            # file is skipped by the caller.
            if out_of_line is not None:
                out_of_line.append(match.group(1))
            text = text[: match.start()] + text[match.end() + 1:]
            continue
        opening = text.find("{", match.end())
        if opening < 0:
            return text[: match.start()]
        text = text[: match.start()] + text[_block_end(text, opening):]


def is_format_name(name: str) -> bool:
    parts = set(name.split("_"))
    return any(tokens <= parts for tokens in TOKENS)


def normalise(value: str) -> str:
    """A value as written, with line breaks (and the indentation after them)
    folded to one space, so a multi-line value compares stably while the
    spaces inside a one-line string literal still count."""
    return re.sub(r"\s*\n\s*", " ", str(value).strip())


def int_value(value: str) -> int | None:
    """An integer literal (decimal or hex, with an optional type suffix)."""
    match = re.fullmatch(r"(0x[0-9a-fA-F_]+|[0-9_]+)(?:[ui](?:8|16|32|64|128|size))?", value.strip())
    if not match:
        return None
    return int(match.group(1).replace("_", ""), 0)


def scan(root: pathlib.Path):
    """(constants, magics, sources, repeated) of the production code under
    crates/*/src, comments blanked.

    constants: {(file, NAME): value} for format-like names;
    magics: {(file, MAGIC)};
    sources: {file: production text}, for the tag sets;
    repeated: {(file, NAME): count} for every constant name declared more
    than once in one file (in different modules), whose value the check
    could otherwise take from either declaration.
    """
    constants: dict[tuple[str, str], str] = {}
    repeated: dict[tuple[str, str], int] = {}
    magics: set[tuple[str, str]] = set()
    sources: dict[str, str] = {}
    paths = sorted(root.glob("crates/*/src/**/*.rs"))
    texts: dict[pathlib.Path, str] = {}
    test_files: set[pathlib.Path] = set()
    for path in paths:
        names: list[str] = []
        texts[path] = production_source(without_comments(path.read_text(encoding="utf-8")), names)
        # A child module's file sits beside a mod.rs, lib.rs or main.rs, and
        # in a directory named after any other file.
        base = path.parent if path.name in ("mod.rs", "lib.rs", "main.rs") else path.with_suffix("")
        for name in names:
            test_files.update({base / f"{name}.rs", base / name / "mod.rs"})
    for path in paths:
        if path in test_files or any(parent in {f.parent for f in test_files if f.name == "mod.rs"}
                                     for parent in path.parents):
            continue
        rel = path.relative_to(root).as_posix()
        text = texts[path]
        sources[rel] = text
        counts: dict[str, int] = {}
        for match in CONST.finditer(text):
            counts[match.group(1)] = counts.get(match.group(1), 0) + 1
            if is_format_name(match.group(1)):
                constants[(rel, match.group(1))] = normalise(match.group(2))
        repeated.update({(rel, name): n for name, n in counts.items() if n > 1})
        for match in MAGIC.finditer(text):
            magics.add((rel, match.group(1)))
    return constants, magics, sources, repeated


def tag_set(text: str, prefix: str) -> dict[str, str]:
    """Every constant named ``prefix*`` and its value as written."""
    return {
        m.group(1): normalise(m.group(2))
        for m in CONST.finditer(text)
        if m.group(1).startswith(prefix)
    }


def load(root: pathlib.Path) -> dict:
    import yaml  # PyYAML, pinned in scripts/compat/requirements.txt

    with open(root / REGISTRY, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def sha256_of(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 16), b""):
            digest.update(block)
    return digest.hexdigest()


def expand(root: pathlib.Path, pattern: str) -> list[str]:
    """The files a registry path stands for, as sorted posix paths from the
    root: the file itself, every file under a directory, or the files a glob
    matches. Prose (README.md) is not a golden file."""
    if any(c in pattern for c in "*?["):
        found = [p for p in root.glob(pattern) if p.is_file()]
    else:
        target = root / pattern
        found = [p for p in target.rglob("*") if p.is_file()] if target.is_dir() else (
            [target] if target.is_file() else []
        )
    return sorted(
        p.relative_to(root).as_posix() for p in found if p.name not in GOLDEN_SKIP
    )


def entry_golden(root: pathlib.Path, entry: dict) -> list[str]:
    """Every golden file one entry names."""
    files: set[str] = set()
    for pattern in list(entry.get("fixtures") or []) + list(entry.get("golden") or []):
        files.update(expand(root, pattern))
    return sorted(files)


def owners(root: pathlib.Path, registry: dict) -> dict[str, list[str]]:
    """{golden file: [the formats that name it]}."""
    named: dict[str, list[str]] = {}
    for entry in registry.get("formats") or []:
        for file in entry_golden(root, entry):
            named.setdefault(file, []).append(f"format {entry.get('id')} ({entry.get('name')})")
    return named


def lock_rows(root: pathlib.Path, registry: dict) -> dict[str, str]:
    return {file: sha256_of(root / file) for file in sorted(owners(root, registry))}


def read_lock(path: pathlib.Path) -> dict[str, str]:
    rows: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        digest, _, file = line.partition("  ")
        rows[file] = digest
    return rows


def write_lock(root: pathlib.Path, registry: dict) -> pathlib.Path:
    path = root / registry["golden_lock"]
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(f"{digest}  {file}\n" for file, digest in lock_rows(root, registry).items())
    path.write_text(LOCK_HEADER + body, encoding="utf-8")
    return path


def check_golden(root: pathlib.Path, registry: dict) -> list[str]:
    """The byte lock: a golden file changed, added, removed or owned by no
    format, against the lock the registry names."""
    if not registry.get("golden_lock"):
        return []
    problems: list[str] = []
    lock_path = root / registry["golden_lock"]
    if not lock_path.is_file():
        return [
            f"{registry['golden_lock']} does not exist; run scripts/check-format-registry.py "
            "--write-lock and commit it"
        ]
    locked = read_lock(lock_path)
    named = owners(root, registry)
    for entry in registry.get("formats") or []:
        for pattern in entry.get("golden") or []:
            if not expand(root, pattern):
                problems.append(
                    f"format {entry.get('id')} ({entry.get('name')}): golden path {pattern} matches no file"
                )
    for file in sorted(named):
        who = ", ".join(named[file])
        if file not in locked:
            problems.append(
                f"{file} ({who}) is not in the lock {registry['golden_lock']}. A new golden file is a "
                "format change: run scripts/check-format-registry.py --write-lock and review the diff"
            )
        elif sha256_of(root / file) != locked[file]:
            problems.append(
                f"golden file {file} changed ({who}): the lock says {locked[file][:12]}, the file is "
                f"{sha256_of(root / file)[:12]}. A changed golden file is a format change: if it is "
                "deliberate, run scripts/check-format-registry.py --write-lock and review the diff"
            )
    for file in sorted(set(locked) - set(named)):
        if (root / file).exists():
            problems.append(
                f"{file} is in the lock but no format names it any more; name it under an entry's "
                "fixtures or golden, or regenerate the lock"
            )
        else:
            problems.append(
                f"golden file {file} no longer exists but the lock records it; removing a golden file "
                "is a format change: run scripts/check-format-registry.py --write-lock"
            )
    found: set[str] = set()
    for pattern in GOLDEN_GLOBS:
        found.update(
            p.relative_to(root).as_posix()
            for p in root.glob(pattern)
            if p.is_file() and p.name not in GOLDEN_SKIP
        )
    for file in sorted(found - set(named)):
        problems.append(
            f"{file} is a golden file that belongs to no format: name it under an entry's fixtures or "
            f"golden in {REGISTRY}"
        )
    return problems


def entry_digest(root: pathlib.Path, entry: dict) -> str:
    """What defines a format here, as one SHA-256: its constants, magics and
    tag sets as the registry records them, and the digest of every golden file
    it names. Prose (name, marker, status) is not part of it."""
    payload = {
        "id": entry.get("id"),
        "constants": sorted(
            [item["file"], item["name"], normalise(item["value"])] for item in entry.get("constants") or []
        ),
        "magics": sorted([item["file"], item["magic"]] for item in entry.get("magics") or []),
        "tag_sets": sorted(
            [item["file"], item["prefix"], sorted((k, int(v)) for k, v in (item.get("tags") or {}).items())]
            for item in entry.get("tag_sets") or []
        ),
        "golden": [[file, sha256_of(root / file)] for file in entry_golden(root, entry)],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def check_state(root: pathlib.Path, entry: dict, cells: dict) -> list[str]:
    """The entry's state, and for a frozen one what freezing needs and
    whether it still matches what was frozen."""
    label = f"format {entry.get('id')} ({entry.get('name')})"
    state = entry.get("state", "unfrozen")
    block = entry.get("frozen")
    if state not in STATES:
        return [f"{label}: unknown state {state!r}; one of {', '.join(STATES)}"]
    if state == "unfrozen":
        if block is not None:
            return [f"{label} is not frozen but has a frozen block; a freeze is not undone by editing the registry"]
        return []
    if not isinstance(block, dict):
        return [f"{label} is frozen and has no frozen block (frozen.at, frozen.digest)"]
    problems: list[str] = []
    at = block.get("at")
    if not isinstance(at, str) or not FINAL_OR_RC.match(at):
        problems.append(f"{label}: frozen.at must be the release it froze at, such as v0.12.0")
    recorded = block.get("digest")
    if not isinstance(recorded, str) or not HEX64.match(recorded):
        problems.append(f"{label}: frozen.digest must be 64 hex digits (scripts/check-format-registry.py --digest {entry.get('id')})")
    waive = block.get("waive") or {}
    if not isinstance(waive, dict) or set(waive) - {"cells", "fuzz"}:
        problems.append(f"{label}: frozen.waive may hold only cells and fuzz, each with a reason")
        waive = {}
    for key, why in waive.items():
        if not why:
            problems.append(f"{label}: frozen.waive.{key} needs a reason")
    if not entry_golden(root, entry):
        problems.append(f"{label} is frozen with no golden files: the byte lock has nothing to hold")
    directions = {
        d for cell in entry.get("cells") or [] for d in (cells.get(cell) or {}).get("directions") or []
    }
    if directions != {"upgrade", "forward"} and "cells" not in waive:
        problems.append(
            f"{label} is frozen without upgrade and forward cells in {EXPECTED} (it has "
            f"{sorted(directions) or 'none'}); add them, or record frozen.waive.cells with the reason"
        )
    if not entry.get("fuzz") and "fuzz" not in waive:
        problems.append(
            f"{label} is frozen with no fuzz target; add one, or record frozen.waive.fuzz with the reason"
        )
    current = [recorded] if isinstance(recorded, str) else []
    for index, row in enumerate(block.get("exceptions") or []):
        where = f"{label}: frozen.exceptions[{index}]"
        if not isinstance(row, dict):
            problems.append(f"{where} must be a mapping with digest, ref and why")
            continue
        if not isinstance(row.get("digest"), str) or not HEX64.match(row["digest"]):
            problems.append(f"{where} needs a digest of 64 hex digits")
        else:
            current.append(row["digest"])
        if not row.get("ref"):
            problems.append(f"{where} needs a ref (the issue or pull request that approved the change)")
        if not row.get("why"):
            problems.append(f"{where} needs a why")
    if current and not problems:
        now = entry_digest(root, entry)
        if now != current[-1]:
            problems.append(
                f"{label} is frozen at {at}, and its constants, tags, magics or golden files no longer "
                f"match the frozen state (digest {now[:12]}, recorded {current[-1][:12]}). A frozen "
                f"format changes only by a recorded exception: add "
                f'{{digest: "{now}", ref: "<issue or PR>", why: "<reason>"}} to its frozen.exceptions '
                f"in {REGISTRY}"
            )
    return problems


def check_history(base: dict, head: dict) -> list[str]:
    """A frozen format of the base registry must still be frozen, at the same
    release, with the same digest and its recorded exceptions intact."""
    problems: list[str] = []
    now = {entry.get("id"): entry for entry in head.get("formats") or []}
    for old in base.get("formats") or []:
        if old.get("state") != "frozen":
            continue
        label = f"format {old.get('id')} ({old.get('name')})"
        before = old.get("frozen") or {}
        entry = now.get(old.get("id"))
        if entry is None:
            problems.append(f"{label} was frozen at {before.get('at')} and is gone from {REGISTRY}")
            continue
        if entry.get("state") != "frozen":
            problems.append(
                f"{label} was frozen at {before.get('at')} and is not any more; a freeze is "
                "not undone by editing the registry"
            )
            continue
        after = entry.get("frozen") or {}
        for key in ("at", "digest", "waive"):
            if before.get(key) != after.get(key):
                problems.append(
                    f"{label}: frozen.{key} changed from {before.get(key)!r} to {after.get(key)!r}; "
                    "what a format froze at is never edited, a change is a recorded exception"
                )
        was = before.get("exceptions") or []
        if (after.get("exceptions") or [])[: len(was)] != was:
            problems.append(
                f"{label}: frozen.exceptions lost or rewrote a recorded exception; they only grow"
            )
    return problems


def base_registry(root: pathlib.Path, rev: str) -> dict | None:
    """The registry at ``rev``; None when it did not exist there yet."""
    import yaml  # PyYAML, pinned in scripts/compat/requirements.txt

    def git(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True)

    if git("rev-parse", "--verify", "--quiet", f"{rev}^{{commit}}").returncode != 0:
        raise ValueError(f"{rev} is not a revision of {root}")
    shown = git("show", f"{rev}:{REGISTRY.as_posix()}")
    if shown.returncode != 0:
        return None
    return yaml.safe_load(shown.stdout)


def check_source(root: pathlib.Path, registry: dict) -> list[str]:
    constants, magics, sources, repeated = scan(root)
    problems: list[str] = []
    # A name declared in two modules of one file has two values, and the
    # registry, which keys on (file, name), cannot say which one is the
    # format: one of them could change unseen (review of #783).
    tag_prefixes = {
        (item["file"], item["prefix"])
        for entry in registry.get("formats") or []
        for item in entry.get("tag_sets") or []
    }
    for (file, name), count in sorted(repeated.items()):
        if is_format_name(name) or any(f == file and name.startswith(p) for f, p in tag_prefixes):
            problems.append(
                f"{file}: {name} is declared {count} times in different modules of one file; the "
                f"registry names a constant by file and name, so give each its own name"
            )
    claimed_constants: dict[tuple[str, str], str] = {}
    claimed_magics: set[tuple[str, str]] = set()
    seen_ids = set()
    named_cells: set[str] = set()
    expected_path = root / EXPECTED
    matrix_cells: set[str] = set()
    cell_table: dict = {}
    if expected_path.is_file():
        import yaml  # PyYAML, pinned in scripts/compat/requirements.txt

        with open(expected_path, encoding="utf-8") as handle:
            cell_table = (yaml.safe_load(handle) or {}).get("cells") or {}
        matrix_cells = set(cell_table)
    for entry in registry.get("formats") or []:
        label = f"format {entry.get('id')} ({entry.get('name')})"
        for key in ("id", "name", "class", "marker", "status", "at_1_0"):
            if not entry.get(key):
                problems.append(f"{label}: missing {key}")
        if entry.get("id") in seen_ids:
            problems.append(f"{label}: id used twice")
        seen_ids.add(entry.get("id"))
        if entry.get("class") not in ("A", "B", "C", "D"):
            problems.append(f"{label}: class must be A, B, C or D")
        for item in entry.get("constants") or []:
            key = (item["file"], item["name"])
            if key in claimed_constants:
                problems.append(f"{label}: {item['file']} {item['name']} is listed twice")
            claimed_constants[key] = normalise(item["value"])
            # A magic that is a listed constant's value is claimed by it.
            for magic in MAGIC.findall(str(item["value"])):
                claimed_magics.add((item["file"], magic))
        for item in entry.get("magics") or []:
            claimed_magics.add((item["file"], item["magic"]))
        for item in entry.get("tag_sets") or []:
            text = sources.get(item["file"])
            found = tag_set(text, item["prefix"]) if text is not None else {}
            want = {name: int(value) for name, value in (item.get("tags") or {}).items()}
            if not found:
                problems.append(f"{label}: no {item['prefix']}* constants in {item['file']}")
                continue
            seen: dict[int, str] = {}
            for name, written in sorted(found.items()):
                value = int_value(written)
                if value is None:
                    problems.append(
                        f"{item['file']}: {name} = {written} is not an integer literal; a tag must "
                        "be written as one, so this check can compare it"
                    )
                    continue
                if value in seen:
                    problems.append(f"{item['file']}: {name} and {seen[value]} are both tag {value}")
                seen[value] = name
                if name not in want:
                    problems.append(
                        f"{item['file']}: {name} = {written} is not in the registry's "
                        f"{item['prefix']}* set. A new tag is a format change: edit "
                        "scripts/compat/formats.yaml in the same change"
                    )
                elif want[name] != value:
                    problems.append(
                        f"{item['file']}: {name} is {value}; the registry says {want[name]}. A tag is "
                        "never renumbered (ddia-encoding-evolution rule 1)"
                    )
            for name in sorted(set(want) - set(found)):
                problems.append(f"{label}: {name} is in the registry but not in {item['file']}")
        problems.extend(check_state(root, entry, cell_table))
        for fixture in entry.get("fixtures") or []:
            if not (root / fixture).exists():
                problems.append(f"{label}: fixture {fixture} does not exist")
        for target in entry.get("fuzz") or []:
            if not (root / "fuzz/fuzz_targets" / f"{target}.rs").is_file():
                problems.append(f"{label}: fuzz target {target} does not exist")
        for cell in entry.get("cells") or []:
            named_cells.add(cell)
            if cell not in matrix_cells:
                problems.append(
                    f"{label}: cell {cell} is not a cell of {EXPECTED} (scripts/compat/cell.sh runs "
                    "the cells that file lists)"
                )
    # Every fuzz target is some format's decoder, or says why it is not
    # (`fuzz_other`): a target added without a registry row fails here, so the
    # fuzz coverage the page claims is the coverage there is.
    named_targets = {t for entry in registry.get("formats") or [] for t in entry.get("fuzz") or []}
    other_targets = registry.get("fuzz_other") or {}
    for target, why in sorted(other_targets.items()):
        if not str(why or "").strip():
            problems.append(f"fuzz_other {target}: needs a why")
        if not (root / "fuzz/fuzz_targets" / f"{target}.rs").is_file():
            problems.append(f"fuzz_other {target}: fuzz target {target} does not exist")
        if target in named_targets:
            problems.append(f"fuzz target {target} is both a format's and in fuzz_other")
    targets_dir = root / "fuzz/fuzz_targets"
    if targets_dir.is_dir():
        for path in sorted(targets_dir.glob("*.rs")):
            if path.stem not in named_targets and path.stem not in other_targets:
                problems.append(
                    f"fuzz target {path.stem} is named by no format in {REGISTRY}: add it to the "
                    "`fuzz` list of the format whose decoder it exercises, or to `fuzz_other` with "
                    "a reason"
                )
    for cell in sorted(matrix_cells - named_cells):
        problems.append(
            f"{EXPECTED}: cell {cell} is named by no entry of {REGISTRY}: a compatibility cell "
            "belongs to the format it records"
        )
    for item in registry.get("not_formats") or []:
        if not item.get("why"):
            problems.append(f"not_formats {item.get('name')}: needs a why")
        if (item["file"], item["name"]) in claimed_constants:
            problems.append(
                f"not_formats {item['name']} in {item['file']}: listed both as a format constant "
                "and as not one"
            )
            continue
        claimed_constants[(item["file"], item["name"])] = None  # type: ignore[assignment]

    for key, value in sorted(constants.items()):
        file, name = key
        if key not in claimed_constants:
            problems.append(
                f"{file}: {name} = {value} is in no entry of {REGISTRY}. A new format, or a "
                "version of one, needs an entry (or a not_formats row saying why it is not one)"
            )
        elif claimed_constants[key] is not None and claimed_constants[key] != value:
            problems.append(
                f"{file}: {name} is {value}; {REGISTRY} says {claimed_constants[key]}. A changed "
                "version marker is a format change: edit the registry in the same change"
            )
    for key in sorted(set(claimed_constants) - set(constants)):
        problems.append(f"{REGISTRY} names {key[1]} in {key[0]}, which the sources no longer hold")
    for key in sorted(magics - claimed_magics):
        problems.append(f"{key[0]}: magic {key[1]} is in no entry of {REGISTRY}")
    for key in sorted(claimed_magics - magics):
        problems.append(f"{REGISTRY} names magic {key[1]} in {key[0]}, which the sources no longer hold")
    problems.extend(check_golden(root, registry))
    return problems


def cell(text) -> str:
    return normalise(text).replace("|", "\\|")


def render(registry: dict) -> str:
    lines = [
        GENERATED_NOTE,
        "| # | Format | Version marker | Class | Status now | At 1.0 | State |",
        "|---|---|---|---|---|---|---|",
    ]
    for entry in sorted(registry["formats"], key=lambda e: e["id"]):
        values = {
            item["name"]: normalise(item["value"]).strip('"')
            for item in entry.get("constants") or []
        }
        entry = dict(entry, marker=re.sub(
            r"\{([A-Z][A-Z0-9_]*)\}", lambda m: values.get(m.group(1), m.group(0)), entry["marker"]
        ))
        lines.append(
            "| {id} | {name} | {marker} | {cls} | {status} | {at} | {state} |".format(
                id=entry["id"],
                name=cell(entry["name"]),
                marker=cell(entry["marker"]),
                cls=entry["class"],
                status=cell(entry["status"]),
                at=cell(entry["at_1_0"]),
                state=cell(
                    f"frozen at {(entry.get('frozen') or {}).get('at')}"
                    if entry.get("state") == "frozen" else "unfrozen"
                ),
            )
        )
    return "\n".join(lines) + "\n"


def page_with_table(page: str, table: str) -> str:
    start = page.index(BEGIN) + len(BEGIN)
    end = page.index(END)
    return page[:start] + "\n" + table + page[end:]


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Keep the format registry true (#743).")
    parser.add_argument("--write", action="store_true", help="regenerate the page's inventory table")
    parser.add_argument("--write-lock", action="store_true",
                        help="regenerate the golden-file lock, deliberately accepting every golden file as it is")
    parser.add_argument("--digest", type=int, metavar="ID", help="print the freeze digest of format ID")
    parser.add_argument("--base", metavar="REV",
                        help="also compare frozen states with the registry at this git revision")
    parser.add_argument("--root", type=pathlib.Path, default=ROOT)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    registry = load(root)
    if args.digest is not None:
        wanted = [e for e in registry["formats"] if e["id"] == args.digest]
        if not wanted:
            print(f"error: {REGISTRY} has no format {args.digest}", file=sys.stderr)
            return 1
        print(entry_digest(root, wanted[0]))
        return 0
    if args.write_lock:
        if not registry.get("golden_lock"):
            print(f"error: {REGISTRY} names no golden_lock", file=sys.stderr)
            return 1
        print(f"wrote {write_lock(root, registry).relative_to(root)}")
    page_path = root / PAGE
    page = page_path.read_text(encoding="utf-8")
    if BEGIN not in page or END not in page:
        print(f"error: {PAGE} has no {BEGIN} ... {END} block", file=sys.stderr)
        return 1
    wanted = page_with_table(page, render(registry))
    if args.write:
        page_path.write_text(wanted, encoding="utf-8")
        print(f"wrote the inventory table of {PAGE}")
    problems = check_source(root, registry)
    if args.base:
        try:
            before = base_registry(root, args.base)
        except ValueError as error:
            print(f"error: {error}", file=sys.stderr)
            return 1
        if before is not None:
            problems.extend(check_history(before, registry))
    if not args.write and wanted != page:
        problems.append(
            f"{PAGE}: the inventory table differs from what {REGISTRY} generates; run "
            "scripts/check-format-registry.py --write"
        )
    for problem in problems:
        print(f"error: {problem}", file=sys.stderr)
    if problems:
        return 1
    count = len(registry["formats"])
    print(f"ok: {count} formats; every magic and version constant in the sources has an entry")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
