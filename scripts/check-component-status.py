#!/usr/bin/env python3
"""Fail when the component status table is malformed, or the README and the
docs claim more than it says.

docs/reference/component-status.md labels every component production, preview
or experimental (#740, criterion H2 of #714). The labels are decided by the
maintainer, so a wrong word anywhere else in the docs is a claim the project
has not made. This check reads the table and fails when:

  - the table lacks a component of COMPONENTS, lists one that is not there,
    lists one twice, or has a label other than production, preview or
    experimental;
  - a row's Decision cell is not PROPOSED or CONFIRMED, or the page's
    "Decision status:" line disagrees with the rows (PROPOSED while any row is
    proposed, CONFIRMED when none is);
  - a component has no "### <name>" section with the four lines Why, Tests and
    CI, Open criteria and Docs, or its Today label is production while its
    Open criteria line is not "none";
  - the README or a page of the site says a component is production-ready (or
    "production grade", "battle-tested" and the like) and the table's
    Today label for it is lower, or says it of VTOP as a whole while any
    component is not production. A sentence that denies, conditions or plans
    the claim ("not production-ready", "until", "the bar for") is allowed,
    and each claim in a sentence is judged on its own;
  - the README does not link to the status page.

Usage: scripts/check-component-status.py [REPO_ROOT]
Exit status: 0 consistent, 1 inconsistent, 2 the page cannot be read.
"""

from __future__ import annotations

import pathlib
import re
import sys

PAGE = "docs/reference/component-status.md"
LABELS = ("experimental", "preview", "production")  # lowest first
RANK = {label: i for i, label in enumerate(LABELS)}
DECISIONS = ("PROPOSED", "CONFIRMED")
HEADER = ["Component", "Today", "At 1.0", "Decision"]
BEGIN, END = "<!-- status:begin -->", "<!-- status:end -->"
SECTION_LINES = ("Why", "Tests and CI", "Open criteria", "Docs")

# Each component, and the words other pages use for it. A sentence that names
# one of these and claims production readiness is held to that row's Today
# label. The key is the table's exact text.
COMPONENTS: dict[str, str] = {
    "Archive engine core": r"archive engine|\bthe engine\b|commit rule",
    "Kafka source": r"kafka source|sources\.kafka",
    "File source": r"file source|sources\.file",
    "Syslog spool source": r"syslog(?: spool)? source|spool source|sources\.syslog",
    "S3-compatible upload (`s3_native`)": r"\bs3_native\b|native s3|s3-compatible upload",
    "Local directory upload (`localfs`)": r"\blocalfs\b|local ?fs backend|local directory upload",
    "Command-line upload backends (`awscli`, `s3cmd`, `minio`)": r"\bawscli\b|\bs3cmd\b|command backends?",
    "SQLite state store": r"sqlite",
    "PostgreSQL state store": r"postgres(?:ql)?",
    "Manifests 0.1 to 0.3": r"manifests?",
    "Manifest 0.4 and MAC key rotation": r"manifest mac|mac key|key rotation|0\.4 manifest",
    "Verification, scrub and coverage audit": r"\bscrub\b|coverage audit|verification receipts?",
    "S3 Object Lock profile": r"object lock",
    "Audit trail": r"audit trail",
    "Prometheus metrics, alert rules and dashboards": r"prometheus|alert rules|dashboards?",
    "OpenTelemetry export": r"opentelemetry|\botlp\b",
    "Metadata plane (`vtop-meta`)": r"metadata plane|\bvtop-meta\b|raft",
    "Data node (`vtop-node`)": r"data node|\bvtop-node\b",
    "Replicated broker (`vtop-broker`)": r"\bvtop-broker\b|replicated broker|native broker",
    "Kafka gateway": r"kafka gateway",
    "Helm chart": r"helm chart|\bchart\b",
}
# The cluster plane is the three components above it: a sentence about "the
# cluster plane" is held to the lowest of them.
GROUPS: dict[str, tuple[str, ...]] = {
    r"cluster plane": (
        "Metadata plane (`vtop-meta`)",
        "Data node (`vtop-node`)",
        "Replicated broker (`vtop-broker`)",
    ),
}

CLAIM = re.compile(
    r"\b(?:production[- ](?:ready|grade|quality|supported)|ready for production"
    r"|suitable for production|battle[- ]tested|enterprise[- ]grade|mission[- ]critical)\b",
    re.I,
)
# Words that make a sentence something other than an assertion, when they
# come BEFORE the claim: a negation ("is not production-ready"), a modal or a
# plan ("will be", "the bar for", "the path to"), or a condition that opens the
# sentence ("Until the soak passes, ..."). After the claim, the same words are
# usually part of a positive assertion ("production-ready without extra
# configuration", "production-ready and requires no setup"), so they do not
# count there (review of #826).
QUALIFIER_BEFORE = re.compile(
    r"\b(?:what|how|not|no|never|nor|neither|none|nobody|isn't|aren't|wasn't|cannot|can't|won't|"
    r"until|unless|before|yet|if|whether|when|once|needs?|needed|requires?|bar|criteria|"
    r"criterion|checklist|will|would|planned|target|toward|towards|path|stage|ladder)\b",
    re.I,
)
# After the claim, only a subordinator that makes the claim conditional
# counts: "production-ready once #727 lands", "... until 1.0", "... if TLS is on".
QUALIFIER_AFTER = re.compile(r"^[\s,]*(?:only\s+)?(?:until|unless|once|if|when|after)\b", re.I)


# Claims joined only by "and", "or", "nor" or a comma ("not production-ready
# or battle-tested") are one assertion: a qualifier before the first covers
# them all.
COORDINATED = re.compile(r"^[\s,]*(?:and|or|nor)?[\s,]*$", re.I)


def assertions(sentence: str):
    """(claim, before, after, about) for each readiness assertion in a sentence.

    A sentence can hold several ("The engine is not production-ready, but the
    Kafka gateway is production-ready"), and each is judged on its own text
    (review of #826): `before` runs from the end of the previous assertion to
    this claim, `after` from this claim to the next assertion or the end, and
    `about` is the text that names what the claim is about - `before` and the
    claim, plus `after` for the last assertion only (the text between two
    claims belongs to the second: "..., but the gateway is production-ready").
    """
    groups: list[list[re.Match]] = []
    for claim in CLAIM.finditer(sentence):
        if groups and COORDINATED.match(sentence[groups[-1][-1].end() : claim.start()]):
            groups[-1].append(claim)
        else:
            groups.append([claim])
    for i, group in enumerate(groups):
        first, last = group[0], group[-1]
        begin = groups[i - 1][-1].end() if i else 0
        end = groups[i + 1][0].start() if i + 1 < len(groups) else len(sentence)
        before, after = sentence[begin : first.start()], sentence[last.end() : end]
        about = sentence[begin : last.end()] + (after if i + 1 == len(groups) else "")
        yield first, before, after, about


def qualified(before: str, after: str) -> bool:
    """Whether a qualifier negates or conditions a claim, by its position."""
    if QUALIFIER_BEFORE.search(before):
        return True
    return bool(QUALIFIER_AFTER.search(after))


# A claim names no component is held to the project-wide rule only when it
# names the project: "Apache Kafka is production-ready" is about another
# product, not VTOP (review of #826).
PROJECT = re.compile(r"\b(?:vtop(?:[- ]engine)?|this (?:project|engine|software|release|system)|the project)\b", re.I)
# Pages and trees the scan skips: the status page itself, the two internal
# drafts the site does not publish, and generated or vendored trees.
SKIP = {
    PAGE,
    "docs/INVENTION_DISCLOSURE_DRAFT.md",
    "docs/PRIOR_ART_SEARCH_PLAN.md",
}
EXTRA_PAGES = ("README.md", "helm/vtop/README.md", "observability/README.md", "benchmarks/README.md")

FENCE = re.compile(r"^\s*(```|~~~)")
SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z`*\[])")


class PageError(Exception):
    """The status page cannot be read as the check expects."""


def parse_table(text: str) -> list[tuple[str, str, str, str, int]]:
    """The rows of the status table: (component, today, at 1.0, decision, line)."""
    if BEGIN not in text or END not in text:
        raise PageError(f"{PAGE}: the table must sit between {BEGIN} and {END}")
    start = text.index(BEGIN)
    body = text[start + len(BEGIN) : text.index(END)]
    first_line = text.count("\n", 0, start) + 2
    rows = []
    header_seen = False
    for offset, raw in enumerate(body.splitlines()):
        line = raw.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if set("".join(cells)) <= set("-: "):
            continue
        if not header_seen:
            if cells != HEADER:
                raise PageError(f"{PAGE}: the table header is {cells}, expected {HEADER}")
            header_seen = True
            continue
        if len(cells) != len(HEADER):
            raise PageError(f"{PAGE}:{first_line + offset}: a row has {len(cells)} cells, expected {len(HEADER)}")
        name, today, at_1, decision = (re.sub(r"[*_]", "", c) if i else c for i, c in enumerate(cells))
        rows.append((name, today, at_1, decision, first_line + offset))
    if not header_seen:
        raise PageError(f"{PAGE}: no table between the markers")
    return rows


def sections(text: str) -> dict[str, str]:
    """Each `### name` section of the page, by name."""
    out: dict[str, str] = {}
    current = None
    for line in text.splitlines():
        if line.startswith("### "):
            current = line[4:].strip()
            out[current] = ""
        elif line.startswith("## "):
            current = None
        elif current is not None:
            out[current] += line + "\n"
    return out


def check_page(text: str) -> tuple[list[str], dict[str, str]]:
    """Problems with the page itself, and each component's Today label."""
    problems: list[str] = []
    rows = parse_table(text)
    seen: dict[str, str] = {}
    for name, today, at_1, decision, line in rows:
        where = f"{PAGE}:{line}"
        if name not in COMPONENTS:
            problems.append(f"{where}: {name!r} is not a known component")
            continue
        if name in seen:
            problems.append(f"{where}: {name!r} is listed twice")
            continue
        for column, value in (("Today", today), ("At 1.0", at_1)):
            if value not in LABELS:
                problems.append(f"{where}: {name}: {column} is {value!r}, expected production, preview or experimental")
        if decision not in DECISIONS:
            problems.append(f"{where}: {name}: Decision is {decision!r}, expected PROPOSED or CONFIRMED")
        seen[name] = today
    for name in COMPONENTS:
        if name not in seen:
            problems.append(f"{PAGE}: the table has no row for {name!r}")

    proposed = any(d == "PROPOSED" for _, _, _, d, _ in rows)
    status = re.search(r"^Decision status:\s*(\S+)\s*$", text, re.M)
    if status is None:
        problems.append(f"{PAGE}: no 'Decision status: PROPOSED|CONFIRMED' line")
    else:
        want = "PROPOSED" if proposed else "CONFIRMED"
        if status.group(1) != want:
            problems.append(
                f"{PAGE}: 'Decision status: {status.group(1)}' but the rows say {want}: "
                "a page is CONFIRMED only when no row is PROPOSED"
            )

    detail = sections(text)
    for name, today, _at_1, _decision, _line in rows:
        if name not in COMPONENTS:
            continue
        body = detail.get(name)
        if body is None:
            problems.append(f"{PAGE}: no '### {name}' evidence section")
            continue
        found: dict[str, str] = {}
        for item in SECTION_LINES:
            m = re.search(rf"^- \*\*{re.escape(item)}:\*\*\s*(.+)$", body, re.M)
            if m is None:
                problems.append(f"{PAGE}: '### {name}' has no '- **{item}:**' line")
            else:
                found[item] = m.group(1).strip()
        if today == "production" and found.get("Open criteria", "none").rstrip(".").lower() != "none":
            problems.append(f"{PAGE}: {name} is production today but its Open criteria line is not 'none'")
    return problems, seen


def scan_targets(root: pathlib.Path) -> list[pathlib.Path]:
    pages = [root / p for p in EXTRA_PAGES]
    docs = root / "docs"
    if docs.is_dir():
        pages += sorted(docs.rglob("*.md"))
    return [p for p in pages if p.is_file() and p.relative_to(root).as_posix() not in SKIP]


def strip_comments(line: str, in_comment: bool) -> tuple[str, bool]:
    """The visible part of one line, and whether a comment is still open after it.

    A plain scan rather than a regular expression: an HTML comment can open on
    one line and close on a later one, and several can open and close on the
    same line, so the state carries from line to line.
    """
    visible: list[str] = []
    rest = line
    while rest:
        if in_comment:
            end = rest.find("-->")
            if end < 0:
                return "".join(visible), True
            rest = rest[end + 3 :]
            in_comment = False
        else:
            start = rest.find("<!--")
            if start < 0:
                visible.append(rest)
                break
            visible.append(rest[:start])
            rest = rest[start + 4 :]
            in_comment = True
    return "".join(visible), in_comment


# A line that starts a block of its own: a heading, a list item, a table row
# or a blockquote marker. Prose that continues on the next line is joined to
# it, the way Markdown renders a soft line break (review of #826).
BLOCK_START = re.compile(r"^\s*(?:#{1,6}\s|[-*+]\s|\d+[.)]\s|\||>)")


def paragraphs(text: str):
    """(first line number, joined text) for each prose block outside code
    fences and HTML comments; soft-wrapped lines are joined with a space."""
    in_fence = False
    in_comment = False
    start, parts = 0, []
    for number, raw in enumerate(text.splitlines(), 1):
        if FENCE.match(raw):
            if parts:
                yield start, " ".join(parts)
                start, parts = 0, []
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        line, in_comment = strip_comments(raw, in_comment)
        if not line.strip() or BLOCK_START.match(line):
            if parts:
                yield start, " ".join(parts)
                start, parts = 0, []
            if not line.strip():
                continue
            line = re.sub(r"^\s*>\s?", "", line)
        if not parts:
            start = number
        parts.append(line.strip())
        # A table row and a heading are one line each: nothing joins them.
        if re.match(r"^\s*(?:\||#{1,6}\s)", line):
            yield start, " ".join(parts)
            start, parts = 0, []
    if parts:
        yield start, " ".join(parts)


def sentences(text: str):
    """(line number, sentence) for prose outside code fences and comments.

    The line number is the first line of the block the sentence is in."""
    for number, block in paragraphs(text):
        for piece in SENTENCE_END.split(block):
            if piece.strip():
                yield number, piece


def claim_problems(root: pathlib.Path, today: dict[str, str]) -> list[str]:
    problems: list[str] = []
    project_ok = all(label == "production" for label in today.values())
    for path in scan_targets(root):
        rel = path.relative_to(root).as_posix()
        for number, sentence in sentences(path.read_text()):
            for claim, before, after, about in assertions(sentence):
                if qualified(before, after):
                    continue
                named = [name for name, alias in COMPONENTS.items() if re.search(alias, about, re.I)]
                for alias, members in GROUPS.items():
                    if re.search(alias, about, re.I):
                        named += [m for m in members if m not in named]
                named = [n for n in named if n in today]
                over = [n for n in named if today[n] != "production"]
                if over:
                    problems.append(
                        f"{rel}:{number}: says {claim.group(0)!r} of {', '.join(over)}, which {PAGE} "
                        f"labels {today[over[0]]}: {sentence.strip()[:110]}"
                    )
                elif not named and not project_ok and PROJECT.search(about):
                    problems.append(
                        f"{rel}:{number}: says {claim.group(0)!r} of VTOP, but {PAGE} does not label "
                        f"every component production: {sentence.strip()[:110]}"
                    )
    return problems


def check(root: pathlib.Path) -> list[str]:
    page = root / PAGE
    if not page.is_file():
        raise PageError(f"{PAGE} does not exist")
    problems, today = check_page(page.read_text())
    problems += claim_problems(root, today)
    readme = root / "README.md"
    if not readme.is_file() or "reference/component-status.md" not in readme.read_text():
        problems.append("README.md: does not link to docs/reference/component-status.md")
    return problems


def main(argv: list[str]) -> int:
    root = pathlib.Path(argv[1] if len(argv) > 1 else ".").resolve()
    try:
        problems = check(root)
    except PageError as e:
        print(e)
        return 2
    if problems:
        for p in problems:
            print(p)
        print(f"{len(problems)} component-status problem(s): the table is the claim, so change the page or the table")
        return 1
    print("component status: the table is well formed, and the README and the docs claim no more than it says")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
