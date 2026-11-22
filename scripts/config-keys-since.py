#!/usr/bin/env python3
"""List the config keys added between two git refs, for a release's notes.

An engine ignores a `config.yaml` or `streams.yaml` key it does not know: most
sections do not reject unknown keys, so an older engine reads a config that
sets a new key and does nothing with it, without a word. The release notes
list each such key and say whether ignoring it is harmless (#743). This script
writes that list instead of someone reading the diff of
crates/vtop-core/src/config.rs by hand.

It reads the schema the way serde does: the struct fields of config.rs from
the root types (`VtopConfig` for config.yaml, `StreamsConfig` for
streams.yaml), through `Option`, `Vec` and string-keyed maps, honouring
`#[serde(rename = ...)]` and `#[serde(skip)]`. It diffs the key sets at the two
refs and prints the keys that are new, each with one line:

* refused: the key sits in a section that rejects unknown keys
  (`#[serde(deny_unknown_fields)]`) at the older ref, so an older engine will
  not start with it set. Release-gating.
* ignored, with the sentence of docs/configure/reference.md that says what an
  older engine does with it, quoted, when the reference has one.
* ignored and harmless while unset, when the reference gives the key a
  default.
* ignored, with a request to decide, when the reference says neither.

A key inside a block that is itself new is listed with the block, which is
what an older engine ignores. A key this build requires (no default) is
marked: a config written for the older engine must add it before it loads.

Usage:
    scripts/config-keys-since.py <from-ref> [<to-ref>]   (to-ref defaults to HEAD)
    scripts/config-keys-since.py v0.10.0
    scripts/config-keys-since.py v0.10.0 --json

Run it in the repository whose refs are named, or pass `--repo DIR`. The
config source and the reference are read from the refs with `git show`, so
the working tree does not matter.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field

CONFIG_RS = "crates/vtop-core/src/config.rs"
REFERENCE_MD = "docs/configure/reference.md"
STREAMS_MD = "docs/configure/streams.md"

# (file the keys are written in, root type in config.rs, reference page,
# prefix the page's rows are under)
ROOTS = [
    ("config.yaml", "VtopConfig", REFERENCE_MD, ""),
    ("streams.yaml", "StreamsConfig", STREAMS_MD, "streams[]"),
]


@dataclass
class Field:
    key: str  # the name in the YAML (after `rename`)
    type_text: str
    has_default: bool
    optional: bool


@dataclass
class Struct:
    name: str
    strict: bool
    container_default: bool
    fields: list[Field] = field(default_factory=list)


@dataclass
class Key:
    path: str
    declared_in: str  # the struct that holds the field
    required: bool
    file: str  # config.yaml or streams.yaml
    via_enum: bool = False  # declared in the struct of an untagged enum's variant
    # The outermost optional, defaulted or repeated block above the key: a
    # required key there is required only where that block is written.
    required_when: str | None = None


file_of_root = {root: file for file, root, _page, _prefix in ROOTS}


# --- config.rs -------------------------------------------------------------


def _strip_comments(text: str) -> str:
    return re.sub(r"//[^\n]*", "", text)


def _match_brace(text: str, open_at: int) -> int:
    depth = 0
    for at in range(open_at, len(text)):
        if text[at] == "{":
            depth += 1
        elif text[at] == "}":
            depth -= 1
            if depth == 0:
                return at
    raise ValueError("unbalanced braces in config.rs")


def _split_top_level(body: str) -> list[str]:
    parts, depth, start = [], 0, 0
    for at, char in enumerate(body):
        if char in "<([{":
            depth += 1
        elif char in ">)]}":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(body[start:at])
            start = at + 1
    tail = body[start:]
    if tail.strip():
        parts.append(tail)
    return parts


def _attributes(text: str) -> list[str]:
    """The contents of every `#[...]` in `text`, brackets balanced."""
    out, at = [], 0
    while True:
        start = text.find("#[", at)
        if start < 0:
            return out
        depth, end = 0, start + 1
        for end in range(start + 1, len(text)):
            if text[end] == "[":
                depth += 1
            elif text[end] == "]":
                depth -= 1
                if depth == 0:
                    break
        out.append(text[start + 2 : end])
        at = end + 1


def parse_structs(source: str) -> dict[str, Struct]:
    """Every `pub struct Name { ... }` with named fields in `source`."""
    structs: dict[str, Struct] = {}
    for found in re.finditer(r"^pub struct (\w+)\s*\{", source, re.M):
        name = found.group(1)
        open_at = found.end() - 1
        body = _strip_comments(source[open_at + 1 : _match_brace(source, open_at)])
        # Container attributes: the contiguous `#[...]`/comment lines above.
        above = source[: found.start()].rstrip("\n").split("\n")
        header: list[str] = []
        while above and above[-1].lstrip().startswith(("#[", "//", ")]")):
            header.append(above.pop())
        container = "\n".join(reversed(header))
        container_attrs = [a for a in _attributes(container) if a.startswith("serde")]
        struct = Struct(
            name=name,
            strict=any("deny_unknown_fields" in a for a in container_attrs),
            container_default=any(
                re.search(r"\bdefault\b", a) for a in container_attrs
            ),
        )
        container_rename = next(
            (
                m.group(1)
                for a in container_attrs
                for m in [re.search(r'rename_all\s*=\s*"(\w+)"', a)]
                if m
            ),
            None,
        )
        for chunk in _split_top_level(body):
            attrs = [a for a in _attributes(chunk) if a.startswith("serde")]
            declaration = re.sub(r"#\[(?:[^\[\]]|\[[^\]]*\])*\]", "", chunk).strip()
            parsed = re.match(r"pub\s+(\w+)\s*:\s*(.+)$", declaration, re.S)
            if not parsed:
                continue
            if any(re.search(r"\bskip\b", a) for a in attrs):
                continue
            rename = next(
                (
                    m.group(1)
                    for a in attrs
                    for m in [re.search(r'\brename\s*=\s*"([^"]+)"', a)]
                    if m
                ),
                None,
            )
            type_text = " ".join(parsed.group(2).split())
            key = rename or parsed.group(1)
            if container_rename and not rename and container_rename != "snake_case":
                # No struct in config.rs renames its fields; if one starts to,
                # a key would be misspelled here, so stop and say so.
                raise ValueError(
                    f"{name}: rename_all = {container_rename!r} on a struct is not read by "
                    "scripts/config-keys-since.py; teach it before relying on its list"
                )
            struct.fields.append(
                Field(
                    key=key,
                    type_text=type_text,
                    has_default=any(re.search(r"\bdefault\b", a) for a in attrs),
                    optional=type_text.startswith("Option<"),
                )
            )
        structs[name] = struct
    return structs


@dataclass
class Enum:
    name: str
    untagged: bool
    variant_types: list[str]  # the type held by each variant with data


def parse_enums(source: str) -> dict[str, Enum]:
    """Every `pub enum Name { ... }`: whether it is untagged, and the types its
    variants hold (a unit variant holds none)."""
    enums: dict[str, Enum] = {}
    for found in re.finditer(r"^pub enum (\w+)\s*\{", source, re.M):
        open_at = found.end() - 1
        body = _strip_comments(source[open_at + 1 : _match_brace(source, open_at)])
        above = source[: found.start()].rstrip("\n").split("\n")
        header: list[str] = []
        while above and above[-1].lstrip().startswith(("#[", "//", ")]")):
            header.append(above.pop())
        attrs = [a for a in _attributes("\n".join(header)) if a.startswith("serde")]
        types: list[str] = []
        for chunk in _split_top_level(body):
            chunk = re.sub(r"#\[(?:[^\[\]]|\[[^\]]*\])*\]", "", chunk).strip()
            held = re.match(r"\w+\s*\((.*)\)$", chunk, re.S)
            if held:
                types.append(" ".join(held.group(1).split()))
            elif re.match(r"\w+\s*\{", chunk):
                types.append("{struct variant}")
        enums[found.group(1)] = Enum(
            found.group(1), any(re.search(r"\buntagged\b", a) for a in attrs), types
        )
    return enums


def _unwrap(type_text: str) -> tuple[str, str]:
    """(inner type name, path suffix) for a field type.

    `Option<X>` and `Box<X>` are X; `Vec<X>` is X under `[]`; a string-keyed
    map is X under `<name>`. A type that is not one of these is its own name.
    """
    suffix = ""
    text = type_text
    while True:
        match = re.match(r"(Option|Box|Vec|BTreeMap|HashMap)\s*<(.*)>$", text)
        if not match:
            break
        wrapper, inner = match.groups()
        if wrapper == "Vec":
            suffix += "[]"
        elif wrapper in ("BTreeMap", "HashMap"):
            parts = _split_top_level(inner)
            suffix += ".<name>"
            inner = parts[-1]
        text = inner.strip()
    return re.sub(r"^.*::", "", text), suffix


def walk(
    structs: dict[str, Struct], enums: dict[str, Enum], root: str
) -> dict[str, Key]:
    """Every key path reachable from `root`, with where it is declared.

    An untagged enum is read as each of its variants' structs at the same
    path (`engine.state_store` is a URL or `{env: ...}` or `{file: ...}`). An
    enum whose variants carry data and is not untagged is keyed by variant
    name, which this does not read, so it stops rather than list a schema it
    only half understood.
    """
    keys: dict[str, Key] = {}

    def visit(
        struct_name: str,
        base: str,
        active: tuple[str, ...],
        via_enum: bool = False,
        guard: str | None = None,
    ) -> None:
        struct = structs[struct_name]
        for item in struct.fields:
            path = f"{base}.{item.key}" if base else item.key
            # A field of an enum variant is required only when that variant is
            # the one written; a config using another form still loads.
            required = not (
                item.has_default or item.optional or struct.container_default or via_enum
            )
            keys[path] = Key(
                path=path,
                declared_in=struct_name,
                required=required,
                file=file_of_root[root],
                via_enum=via_enum,
                required_when=guard,
            )
            inner, suffix = _unwrap(item.type_text)
            optional_here = bool(
                item.has_default or item.optional or struct.container_default or suffix
            )
            inner_guard = guard or (path + suffix if optional_here else None)
            if inner in structs and inner not in active:
                visit(inner, path + suffix, active + (struct_name,), False, inner_guard)
            elif inner in enums and enums[inner].variant_types:
                if not enums[inner].untagged:
                    raise ValueError(
                        f"{struct_name}.{item.key}: enum {inner} holds data but is not "
                        "untagged; scripts/config-keys-since.py does not read it, so its "
                        "keys would be missing from the list"
                    )
                for variant in enums[inner].variant_types:
                    held, held_suffix = _unwrap(variant)
                    if held in structs and held not in active:
                        visit(
                            held,
                            path + suffix + held_suffix,
                            active + (struct_name,),
                            via_enum=True,
                            guard=inner_guard,
                        )

    visit(root, "", ())
    return keys


def schema(source: str) -> dict[str, Key]:
    structs = parse_structs(source)
    enums = parse_enums(source)
    keys: dict[str, Key] = {}
    for _file, root, _page, _prefix in ROOTS:
        if root in structs:
            keys.update(walk(structs, enums, root))
    return keys


def strict_sections(source: str) -> set[str]:
    return {name for name, struct in parse_structs(source).items() if struct.strict}


# --- the reference ---------------------------------------------------------


@dataclass
class Row:
    default: str
    meaning: str


def parse_reference(md: str, root_prefix: str = "") -> tuple[dict[str, Row], dict[str, str]]:
    """(rows, prose) of a reference page.

    rows: the full key path -> its table row. prose: the path of a section ->
    its paragraphs outside tables and code fences, one string.
    """
    rows: dict[str, Row] = {}
    prose: dict[str, list[str]] = {}
    section = root_prefix
    in_fence = False
    paragraph: list[str] = []

    def flush() -> None:
        if paragraph:
            prose.setdefault(section, []).append(" ".join(paragraph))
            paragraph.clear()

    for line in md.split("\n"):
        if line.lstrip().startswith("```"):
            flush()
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        heading = re.match(r"#{2,4}\s+(.*)$", line)
        if heading:
            flush()
            named = re.match(r"`([\w.<>\[\]-]+)`", heading.group(1))
            if named:
                name = named.group(1)
                section = f"{root_prefix}.{name}" if root_prefix else name
            else:
                section = root_prefix
            continue
        if line.startswith("|"):
            flush()
            cells = [cell.strip() for cell in line.strip().strip("|").split(" | ")]
            if len(cells) >= 3:
                key = re.match(r"`([^`]+)`$", cells[0])
                if key:
                    full = f"{section}.{key.group(1)}" if section else key.group(1)
                    rows[full] = Row(default=cells[1], meaning=" | ".join(cells[2:]))
            continue
        if line.strip() == "":
            flush()
        else:
            paragraph.append(line.strip())
    flush()
    return rows, {key: "\n".join(value) for key, value in prose.items()}


OLDER = re.compile(
    r"\b(an? |the )?(engine|release|version)s?\s+(before|older than|earlier than)\b"
    r"|\b(older|earlier)\s+(engine|release|version)s?\b",
    re.I,
)


def _plain(text: str) -> str:
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)  # links to their text
    return " ".join(text.replace("**", "").split())


def older_engine_sentence(key: str, row: Row | None, prose: dict[str, str]) -> str | None:
    """The sentence of the reference that says what an older engine does.

    Looked for in the key's own row, then in the paragraphs of its section
    that name it in backticks. The sentence is the one that matches, so a
    paragraph about several keys yields only the part about this release.
    """
    leaf = key.rsplit(".", 1)[-1].replace("[]", "")
    candidates: list[str] = []
    if row is not None:
        candidates.append(row.meaning)
    section = key.rsplit(".", 1)[0] if "." in key else ""
    for paragraph in prose.get(section, "").split("\n"):
        if f"`{leaf}" in paragraph or f"`{key}`" in paragraph:
            candidates.append(paragraph)
    # A block has a section of its own: what it says about older engines is
    # about the whole block.
    if key in prose:
        candidates.extend(prose[key].split("\n"))
    for text in candidates:
        for sentence in re.split(r"(?<=[.!?])\s+", _plain(text)):
            if OLDER.search(sentence):
                return sentence
    return None


# --- the diff --------------------------------------------------------------


def git_show(repo: str, ref: str, path: str) -> str | None:
    result = subprocess.run(
        ["git", "-C", repo, "show", f"{ref}:{path}"],
        capture_output=True,
        text=True,
    )
    return result.stdout if result.returncode == 0 else None


def resolve(repo: str, ref: str) -> str:
    result = subprocess.run(
        ["git", "-C", repo, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.exit(f"config-keys-since: {ref} is not a commit of {repo}")
    return result.stdout.strip()


@dataclass
class Added:
    path: str
    note_kind: str  # refused | reference | default | undecided
    note: str
    required: bool
    inside: list[str]  # the new keys under this one (a new block)
    required_when: str | None = None  # required only where this block is written


def added_keys(
    old_source: str, new_source: str, reference_by_file: dict[str, str]
) -> list[Added]:
    old_keys = schema(old_source)
    new_keys = schema(new_source)
    old_structs = parse_structs(old_source)
    old_strict = {name for name, struct in old_structs.items() if struct.strict}
    fresh = sorted(set(new_keys) - set(old_keys))
    fresh_set = set(fresh)

    def parent(path: str) -> str | None:
        """The key that holds `path`: `a.b[].c` is held by `a.b`, and
        `a.<name>.c` by `a`."""
        cut = path.rfind(".")
        if cut <= 0:
            return None
        holder = path[:cut]
        for ending in ("[]", ".<name>"):
            if holder.endswith(ending):
                holder = holder[: -len(ending)]
        return holder or None

    def top_of(path: str) -> str:
        """The shortest new prefix of a new key: the block an older engine
        meets first."""
        best = path
        walker: str | None = path
        while walker is not None:
            if walker in fresh_set:
                best = walker
            walker = parent(walker)
        return best

    rows: dict[str, tuple[dict[str, Row], dict[str, str]]] = {}
    for file, _root, page, prefix in ROOTS:
        rows[file] = parse_reference(reference_by_file.get(page, ""), prefix)

    grouped: dict[str, list[str]] = {}
    for path in fresh:
        grouped.setdefault(top_of(path), []).append(path)

    out: list[Added] = []
    for top in sorted(grouped):
        key = new_keys[top]
        reference_rows, prose = rows[key.file]
        row = next(
            (reference_rows[c] for c in _row_names(top) if c in reference_rows), None
        )
        # Does the struct that holds the key reject unknown keys at the older
        # ref? Then an older engine refuses the config instead of ignoring it.
        refused = key.declared_in in old_strict
        # A new variant of an untagged enum is a new shape of the value: the
        # older enum has no variant that accepts it, so the config fails to
        # load, whatever the variant's own fields say.
        new_shape = key.via_enum and key.declared_in not in old_structs
        sentence = older_engine_sentence(top, row, prose)
        inside = [p for p in grouped[top] if p != top]
        if new_shape:
            kind, note = (
                "refused",
                "refused by an older engine: its value at this path has no form that "
                "accepts this key, so it will not start with it set. Release-gating.",
            )
        elif refused:
            kind, note = (
                "refused",
                "refused by an older engine: the section rejects unknown keys, so "
                "it will not start with this key set. Release-gating.",
            )
        elif sentence:
            kind, note = "reference", f'ignored by an older engine. Reference: "{sentence}"'
        elif row is not None and _has_default(row.default):
            kind, note = (
                "default",
                "ignored by an older engine, and harmless while unset: the "
                f"default is {_plain(row.default)}.",
            )
        else:
            kind, note = (
                "undecided",
                "ignored by an older engine. Unset by default, and the reference "
                "does not say what an older engine loses; decide before release.",
            )
        out.append(Added(top, kind, note, key.required, inside, key.required_when))
    return out


def _row_names(path: str) -> list[str]:
    """The names a key's row may carry: the reference documents the tuning of
    every transport under `upload.transports`, not `upload.transports.<name>`."""
    return [path, path.replace(".<name>", "")]


def _has_default(cell: str) -> bool:
    text = _plain(cell).strip().lower()
    return bool(text) and not text.startswith(("-", "required"))


def render(added: list[Added], previous: str, current: str) -> str:
    lines = [
        "### Config keys an engine older than "
        f"{previous} ignores",
        "",
    ]
    if not added:
        lines.append(f"No `config.yaml` or `streams.yaml` key was added since {previous}.")
        return "\n".join(lines) + "\n"
    lines.append(
        f"Keys added since {previous} (as of {current}). An older engine reads a "
        "config that sets them and ignores them without a word, except where a "
        "line says it refuses the config. Upgrade every engine before relying on "
        "one."
    )
    lines.append("")
    for item in added:
        line = f"- `{item.path}`: {item.note}"
        if item.inside:
            names = ", ".join(f"`{p.removeprefix(item.path + '.')}`" for p in item.inside)
            line += f" New block; keys inside: {names}."
        if item.required and item.required_when:
            line += (
                f" Required by this build wherever `{item.required_when}` is set: "
                "a config written for the older engine that sets it must add it."
            )
        elif item.required:
            line += (
                " Required by this build: a config written for the older engine "
                "must add it before it loads."
            )
        lines.append(line)
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("from_ref", help="the older ref, e.g. the previous tag")
    parser.add_argument("to_ref", nargs="?", default="HEAD", help="the newer ref (HEAD)")
    parser.add_argument("--repo", default=".", help="the repository (the working directory)")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    args = parser.parse_args(argv)

    resolve(args.repo, args.from_ref)
    resolve(args.repo, args.to_ref)
    old_source = git_show(args.repo, args.from_ref, CONFIG_RS)
    new_source = git_show(args.repo, args.to_ref, CONFIG_RS)
    if new_source is None:
        sys.exit(f"config-keys-since: {CONFIG_RS} is not in {args.to_ref}")
    if old_source is None:
        sys.exit(f"config-keys-since: {CONFIG_RS} is not in {args.from_ref}")
    pages = {
        page: git_show(args.repo, args.to_ref, page) or ""
        for page in (REFERENCE_MD, STREAMS_MD)
    }
    try:
        added = added_keys(old_source, new_source, pages)
    except ValueError as error:
        sys.exit(f"config-keys-since: {error}")
    if args.json:
        print(
            json.dumps(
                [
                    {
                        "key": a.path,
                        "kind": a.note_kind,
                        "note": a.note,
                        "required": a.required,
                        "required_when": a.required_when,
                        "inside": a.inside,
                    }
                    for a in added
                ],
                indent=2,
            )
        )
    else:
        sys.stdout.write(render(added, args.from_ref, args.to_ref))
    return 0


if __name__ == "__main__":
    sys.exit(main())
