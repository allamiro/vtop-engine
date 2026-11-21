#!/usr/bin/env python3
"""Fail when a change removes a command, flag, value, metric or label with no
entry in docs/deprecations.md (#719).

The snapshots in docs/surface/ are generated from the code, and a Rust test
fails when they drift from it, so a change to the surface always edits them.
This check compares them with a base revision and asks, for every line a
change removed or narrowed, for an entry in docs/deprecations.md naming the
item. Additions need nothing.

What needs an entry:

- metrics (docs/surface/metrics-*.txt, `name type label,...`): a family that
  is gone or changed type needs a `metric` entry; a label that is gone needs
  a `label` entry (`vtop_x_total{reason}`) or one for the family.
- commands (the other files, see crates/vtop-surface/src/commands.rs): a
  command that is gone, or lost an alias, needs a `command` entry. An
  argument that is gone, lost a spelling, changed between a flag and an
  option, became required, lost its repeatability or moved, needs a `flag`
  entry, as does a new required argument on a command that already existed
  and an argument that took any value and now accepts only a listed set. A
  value that is no longer accepted needs a `value` entry (`vtopctl x
  --source syslog`) or one for the flag. A `command` entry also covers
  everything under that command.

A changed default, value name, help text or hidden marker needs no entry:
every invocation that worked before still parses.

Usage:
  scripts/check-surface-deprecations.py --base REV [--repo ROOT]
  scripts/check-surface-deprecations.py --base-dir DIR [--repo ROOT]

--base reads the base snapshots with `git show REV:docs/surface/...`;
--base-dir reads them from a directory (the tests use it).
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys
from dataclasses import dataclass, field

SURFACE = "docs/surface"
DEPRECATIONS = "docs/deprecations.md"
KINDS = ("command", "flag", "value", "metric", "label")
HEADER = ["Kind", "Name", "Deprecated in", "Removed in", "Replacement", "Notes"]
VERSION = re.compile(r"^v\d+\.\d+\.\d+$")
FLAG_WORDS = {"flag", "count", "multiple", "required", "global", "hidden"}


class TableError(Exception):
    pass


@dataclass(frozen=True)
class Need:
    """A removal, and the entry that would cover it."""

    file: str
    kind: str
    name: str
    why: str


@dataclass
class Arg:
    spellings: set[str] = field(default_factory=set)
    takes: str = ""  # flag, count or value
    # None when the snapshot lists no `values=`: any value is accepted.
    values: set[str] | None = None
    attrs: dict[str, str] = field(default_factory=dict)
    words: set[str] = field(default_factory=set)


def lines_of(text: str) -> list[str]:
    return [
        line.rstrip()
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


# --- metrics -----------------------------------------------------------------


def parse_metrics(text: str) -> dict[str, tuple[str, set[str]]]:
    families = {}
    for line in lines_of(text):
        words = line.split(" ")
        if len(words) not in (2, 3):
            raise ValueError(f"not a metric line: {line!r}")
        labels = set(words[2].split(",")) if len(words) == 3 else set()
        families[words[0]] = (words[1], labels)
    return families


def metric_needs(file: str, base: str, head: str) -> list[Need]:
    before, after = parse_metrics(base), parse_metrics(head)
    needs = []
    for name, (kind, labels) in sorted(before.items()):
        if name not in after:
            needs.append(Need(file, "metric", name, "the family is gone"))
            continue
        new_kind, new_labels = after[name]
        if new_kind != kind:
            needs.append(
                Need(file, "metric", name, f"its type changed from {kind} to {new_kind}")
            )
        for label in sorted(labels - new_labels):
            needs.append(Need(file, "label", f"{name}{{{label}}}", "the label is gone"))
    return needs


# --- commands ----------------------------------------------------------------


def parse_commands(text: str) -> tuple[dict[str, set[str]], dict[str, Arg]]:
    """Command paths with their aliases, and arguments keyed by
    `path spelling`."""
    commands: dict[str, set[str]] = {}
    args: dict[str, Arg] = {}
    for line in lines_of(text):
        words = line.split(" ")
        at = next(
            (i for i, w in enumerate(words) if i > 0 and w[:1] in ("-", "<")), None
        )
        if at is None:
            path = []
            aliases: set[str] = set()
            for word in words:
                if word.startswith("aliases="):
                    aliases = set(word[len("aliases="):].split(","))
                elif "=" in word or word in FLAG_WORDS:
                    continue
                else:
                    path.append(word)
            commands[" ".join(path)] = aliases
            continue
        key = " ".join(words[: at + 1])
        arg = Arg(spellings={words[at]})
        for word in words[at + 1:]:
            name, eq, value = word.partition("=")
            if not eq:
                if word in ("flag", "count"):
                    arg.takes = word
                arg.words.add(word)
                continue
            if name == "short":
                arg.spellings.add(value)
            elif name in ("aliases", "short-aliases"):
                arg.spellings.update(value.split(","))
            elif name == "values":
                arg.values = set(value.split(","))
            elif name == "value":
                arg.takes = "value"
            else:
                arg.attrs[name] = value
        if not arg.takes:
            arg.takes = "value"
        args[key] = arg
    return commands, args


def command_needs(file: str, base: str, head: str) -> list[Need]:
    before_commands, before_args = parse_commands(base)
    after_commands, after_args = parse_commands(head)
    needs = []
    for path, aliases in sorted(before_commands.items()):
        if path not in after_commands:
            needs.append(Need(file, "command", path, "the command is gone"))
        elif aliases - after_commands[path]:
            gone = ",".join(sorted(aliases - after_commands[path]))
            needs.append(Need(file, "command", path, f"its alias {gone} is gone"))
    for key, arg in sorted(before_args.items()):
        new = after_args.get(key)
        if new is None:
            needs.append(Need(file, "flag", key, "the argument is gone"))
            continue
        reasons = []
        if arg.spellings - new.spellings:
            reasons.append("spelling " + ",".join(sorted(arg.spellings - new.spellings)) + " is gone")
        if arg.takes != new.takes:
            reasons.append(f"it changed from {arg.takes} to {new.takes}")
        if "required" in new.words and "required" not in arg.words:
            reasons.append("it became required")
        if "multiple" in arg.words and "multiple" not in new.words:
            reasons.append("it can no longer be repeated")
        if "global" in arg.words and "global" not in new.words:
            reasons.append("it is no longer global")
        if "position" in arg.attrs and arg.attrs["position"] != new.attrs.get("position"):
            reasons.append(f"it moved from position {arg.attrs['position']}")
        if arg.values is None and new.values is not None:
            reasons.append("it took any value and now accepts only " + ",".join(sorted(new.values)))
        for reason in reasons:
            needs.append(Need(file, "flag", key, reason))
        # Dropping a restriction (values= gone) widens what parses: no entry.
        if arg.values is not None and new.values is not None:
            for value in sorted(arg.values - new.values):
                needs.append(Need(file, "value", f"{key} {value}", "the value is no longer accepted"))
    # An invocation of a command that already existed fails without a new
    # required argument; one on a new command breaks nothing.
    for key, new in sorted(after_args.items()):
        command = key.rsplit(" ", 1)[0]
        if key not in before_args and command in before_commands and "required" in new.words:
            needs.append(Need(file, "flag", key, "a command that existed gained it as a required argument"))
    return needs


# --- the deprecations table --------------------------------------------------


def parse_table(text: str) -> list[tuple[str, str]]:
    """The (kind, name) of every entry. Raises TableError naming the line of
    the first malformed row."""
    rows = text.splitlines()
    start = None
    for number, line in enumerate(rows):
        if line.strip().startswith("|") and cells(line) == HEADER:
            start = number
            break
    if start is None:
        raise TableError(
            f"{DEPRECATIONS}: no table with the header | " + " | ".join(HEADER) + " |"
        )
    entries = []
    for number in range(start + 2, len(rows)):
        line = rows[number]
        if not line.strip().startswith("|"):
            break
        row = cells(line)
        where = f"{DEPRECATIONS} line {number + 1}"
        if len(row) != len(HEADER):
            raise TableError(f"{where}: {len(row)} cells, the header has {len(HEADER)}")
        kind, name, deprecated, removed = row[0], row[1].strip("`"), row[2], row[3]
        if kind not in KINDS:
            raise TableError(f"{where}: kind {kind!r} is not one of {', '.join(KINDS)}")
        if not name:
            raise TableError(f"{where}: the name is empty")
        if kind in ("metric", "label") and not name.startswith("vtop_"):
            raise TableError(f"{where}: a {kind} name starts with vtop_, not {name!r}")
        if kind == "label" and not re.fullmatch(r"vtop_\w+\{\w+\}", name):
            raise TableError(f"{where}: a label is written vtop_family{{label}}, not {name!r}")
        if kind in ("command", "flag", "value") and name.split(" ")[0] not in ("vtopctl", "vtop-node"):
            raise TableError(f"{where}: a {kind} starts with vtopctl or vtop-node, not {name!r}")
        if not VERSION.match(deprecated):
            raise TableError(f"{where}: 'Deprecated in' is a release like v0.11.0, not {deprecated!r}")
        if not (VERSION.match(removed) or removed == "not scheduled"):
            raise TableError(
                f"{where}: 'Removed in' is a release like v0.13.0 or 'not scheduled', not {removed!r}"
            )
        entries.append((kind, name))
    return entries


def cells(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def covered(need: Need, entries: list[tuple[str, str]]) -> bool:
    if (need.kind, need.name) in entries:
        return True
    if need.kind == "label":
        family = need.name.split("{", 1)[0]
        return ("metric", family) in entries
    if need.kind in ("command", "flag", "value"):
        if need.kind == "value":
            flag = need.name.rsplit(" ", 1)[0]
            if ("flag", flag) in entries:
                return True
        words = need.name.split(" ")
        for kind, name in entries:
            if kind != "command":
                continue
            prefix = name.split(" ")
            if words[: len(prefix)] == prefix:
                return True
    return False


# --- driver ------------------------------------------------------------------


def needs_for(file: str, base: str, head: str) -> list[Need]:
    if pathlib.PurePath(file).name.startswith("metrics"):
        return metric_needs(file, base, head)
    return command_needs(file, base, head)


def check(base: dict[str, str], head: dict[str, str], table: str) -> tuple[list[Need], list[Need]]:
    """(covered, uncovered) removals across every snapshot file."""
    entries = parse_table(table)
    found = []
    for file in sorted(set(base) | set(head)):
        found += needs_for(file, base.get(file, ""), head.get(file, ""))
    return (
        [n for n in found if covered(n, entries)],
        [n for n in found if not covered(n, entries)],
    )


def git_snapshots(repo: pathlib.Path, rev: str) -> dict[str, str]:
    listed = subprocess.run(
        ["git", "-C", str(repo), "ls-tree", "--name-only", f"{rev}:{SURFACE}"],
        capture_output=True,
        text=True,
    )
    if listed.returncode != 0:
        # The base predates the snapshots: there is nothing to remove from.
        return {}
    out = {}
    for name in listed.stdout.split():
        if name.endswith(".txt"):
            out[f"{SURFACE}/{name}"] = subprocess.run(
                ["git", "-C", str(repo), "show", f"{rev}:{SURFACE}/{name}"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout
    return out


def dir_snapshots(root: pathlib.Path) -> dict[str, str]:
    return {
        f"{SURFACE}/{path.name}": path.read_text()
        for path in sorted(root.glob("*.txt"))
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--base", help="the revision to compare with")
    source.add_argument("--base-dir", type=pathlib.Path, help="the base snapshots, as files")
    parser.add_argument("--repo", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parent.parent)
    args = parser.parse_args(argv)

    head = dir_snapshots(args.repo / SURFACE)
    base = git_snapshots(args.repo, args.base) if args.base else dir_snapshots(args.base_dir)
    try:
        ok, missing = check(base, head, (args.repo / DEPRECATIONS).read_text())
    except (TableError, ValueError) as error:
        print(f"surface deprecations: {error}", file=sys.stderr)
        return 1
    for need in ok:
        print(f"ok: {need.kind} {need.name} ({need.why}) has its entry in {DEPRECATIONS}")
    if missing:
        print(
            f"surface deprecations: {len(missing)} removal(s) from the operator-facing surface "
            f"have no entry in {DEPRECATIONS}:",
            file=sys.stderr,
        )
        for need in missing:
            print(f"  {need.file}: {need.kind} `{need.name}`: {need.why}", file=sys.stderr)
        print(
            f"Dashboards, alerts and scripts use these names. Keep the old name working if you "
            f"can; otherwise add a row per item to the table in {DEPRECATIONS} (kind, name, the "
            f"release that deprecates it, the release that removes it, the replacement) and an "
            f"upgrade note to the release notes. A `command` entry covers everything under the "
            f"command, a `metric` entry its labels, a `flag` entry its values.",
            file=sys.stderr,
        )
        return 1
    print(f"surface deprecations: {len(ok)} removal(s), each with its entry" if ok else
          "surface deprecations: nothing removed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
