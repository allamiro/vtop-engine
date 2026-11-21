#!/usr/bin/env python3
"""Every shipped alert has a runbook section, a working runbook link and a unit test.

Checks, for observability/prometheus/vtop-rules.yaml (#720):

* each alert has a `severity` label (page or ticket) and `summary`,
  `description` and `runbook_url` annotations;
* each `runbook_url` is the published runbook page plus a fragment, and the
  fragment is the anchor of that alert's own section in
  docs/operate/runbooks.md, computed with the slug rules mkdocs.yml configures
  (pymdownx.slugs.slugify, case lower: the GitHub rules), so a link that
  resolves here resolves on the site and on GitHub;
* every alert section in the runbook (a level-two heading named Vtop...) is
  for an alert that exists;
* each alert has a promtool unit test that expects it to fire and one that
  expects it to stay quiet, in observability/prometheus/tests/vtop-rules.test.yaml,
  and no test names an alert that does not exist.

The chart's PrometheusRule serves helm/vtop/files/vtop-rules.yaml verbatim
(only the runbook page can be swapped, keeping each fragment); the helm job in
.github/workflows/ci.yml checks that, and the prometheus-rules job that the
copy equals this file, so checking this file checks the chart's links too.

Exit status: 0 when every check passes, 1 with one line per problem.
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from pathlib import Path
from urllib.parse import urlsplit

import yaml

ROOT = Path(__file__).resolve().parent.parent
RULES = ROOT / "observability/prometheus/vtop-rules.yaml"
TESTS = ROOT / "observability/prometheus/tests/vtop-rules.test.yaml"
RUNBOOKS = ROOT / "docs/operate/runbooks.md"
# site_url in mkdocs.yml plus the page's path under use_directory_urls.
RUNBOOK_BASE = "https://allamiro.github.io/vtop-engine/operate/runbooks/"
SEVERITIES = ("page", "ticket")
ANNOTATIONS = ("summary", "description", "runbook_url")
# The runbook path as messages name it; main() sets it from --runbooks.
RUNBOOKS_LABEL = "docs/operate/runbooks.md"

_TAGS = re.compile(r"</?[^>]*>", re.UNICODE)
_INVALID = re.compile(r"[^\w\- ]", re.UNICODE)
_SEP = re.compile(r" ", re.UNICODE)
_HEADING = re.compile(r"^(#{1,6})[ \t]+(.+?)[ \t]*#*[ \t]*$")
_FENCE = re.compile(r"^[ \t]*(```|~~~)")


def slugify(text: str) -> str:
    """pymdownx.slugs.slugify(case="lower") on a heading's text.

    Markdown inline syntax is not rendered first, so a heading for an alert
    is expected to be the bare alert name.
    """
    slug = _TAGS.sub("", text)
    slug = unicodedata.normalize("NFC", slug).strip()
    slug = _INVALID.sub("", slug).lower()
    return _SEP.sub("-", slug)


def headings(markdown: str) -> list[tuple[int, int, str]]:
    """(line number, level, text) for every ATX heading outside a code fence."""
    found, fenced = [], False
    for number, line in enumerate(markdown.splitlines(), start=1):
        if _FENCE.match(line):
            fenced = not fenced
            continue
        if fenced:
            continue
        match = _HEADING.match(line)
        if match:
            found.append((number, len(match.group(1)), match.group(2)))
    return found


def alerts_of(groups: object, where: str, problems: list[str]) -> list[dict]:
    if not isinstance(groups, list):
        problems.append(f"{where}: no `groups` list")
        return []
    alerts = []
    for group in groups:
        for rule in (group or {}).get("rules") or []:
            if isinstance(rule, dict) and "alert" in rule:
                alerts.append(rule)
    if not alerts:
        problems.append(f"{where}: no alert rules found")
    return alerts


def check_alerts(
    alerts: list[dict],
    sections: dict[str, str],
    where: str,
    base: str | None,
    problems: list[str],
) -> None:
    """`sections` maps a heading's anchor to its text."""
    seen = set()
    for rule in alerts:
        name = rule["alert"]
        if name in seen:
            problems.append(f"{where}: alert {name} is defined more than once")
        seen.add(name)
        severity = (rule.get("labels") or {}).get("severity")
        if severity not in SEVERITIES:
            problems.append(
                f"{where}: alert {name} has severity {severity!r}; expected one of {', '.join(SEVERITIES)}"
            )
        annotations = rule.get("annotations") or {}
        for key in ANNOTATIONS:
            if not str(annotations.get(key) or "").strip():
                problems.append(f"{where}: alert {name} has no {key} annotation")
        url = str(annotations.get("runbook_url") or "")
        if not url:
            continue
        parts = urlsplit(url)
        page = url.split("#", 1)[0]
        if base is not None and page != base:
            problems.append(
                f"{where}: alert {name} links its runbook at {page}, not the runbook page {base}"
            )
        if not parts.fragment:
            problems.append(f"{where}: alert {name}'s runbook_url {url} names no section (no #fragment)")
            continue
        heading = sections.get(parts.fragment)
        if heading is None:
            problems.append(
                f"{where}: alert {name}'s runbook_url points at #{parts.fragment}, "
                f"which is not a heading in {RUNBOOKS_LABEL}"
            )
        elif heading != name:
            problems.append(
                f"{where}: alert {name}'s runbook_url points at the section "
                f"'{heading}', not at its own section '{name}'"
            )


def check_runbook_sections(
    markdown: str, alert_names: set[str], problems: list[str]
) -> dict[str, str]:
    sections: dict[str, str] = {}
    for line, level, text in headings(markdown):
        anchor = slugify(text)
        if anchor in sections:
            # MkDocs numbers a repeated anchor (#x_1), so a link meant for the
            # second heading lands on the first. Only an alert's anchor is
            # linked from a rule, so only those must be unique.
            if text.startswith("Vtop") or sections[anchor].startswith("Vtop"):
                problems.append(
                    f"{RUNBOOKS_LABEL}:{line}: heading '{text}' repeats the anchor #{anchor}; "
                    "a runbook link to it would land on the first heading with that anchor"
                )
            continue
        sections[anchor] = text
        if level == 2 and text.startswith("Vtop") and text not in alert_names:
            problems.append(
                f"{RUNBOOKS_LABEL}:{line}: section '{text}' is for an alert that does not exist"
            )
    for name in sorted(alert_names):
        if not any(text == name for text in sections.values()):
            problems.append(f"{RUNBOOKS_LABEL}: alert {name} has no section (a '## {name}' heading)")
    return sections


def check_tests(tests_doc: object, alert_names: set[str], where: str, problems: list[str]) -> None:
    firing: set[str] = set()
    quiet: set[str] = set()
    for case in (tests_doc or {}).get("tests") or []:
        for check in case.get("alert_rule_test") or []:
            name = check.get("alertname")
            if name not in alert_names:
                problems.append(f"{where}: test '{case.get('name', '?')}' checks alert {name}, which does not exist")
                continue
            (firing if check.get("exp_alerts") else quiet).add(name)
    for name in sorted(alert_names):
        if name not in firing:
            problems.append(f"{where}: alert {name} has no unit test that expects it to fire")
        if name not in quiet:
            problems.append(f"{where}: alert {name} has no unit test that expects it to stay quiet")


def main(argv: list[str] | None = None) -> int:
    global RUNBOOKS_LABEL
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--rules", type=Path, default=RULES)
    parser.add_argument("--tests", type=Path, default=TESTS)
    parser.add_argument("--runbooks", type=Path, default=RUNBOOKS)
    parser.add_argument("--base", default=RUNBOOK_BASE, help="the runbook page the rules file must link")
    args = parser.parse_args(argv)
    RUNBOOKS_LABEL = str(args.runbooks)

    problems: list[str] = []
    rules_doc = yaml.safe_load(args.rules.read_text()) or {}
    alerts = alerts_of(rules_doc.get("groups"), str(args.rules), problems)
    names = {rule["alert"] for rule in alerts}
    sections = check_runbook_sections(args.runbooks.read_text(), names, problems)
    check_alerts(alerts, sections, str(args.rules), args.base, problems)
    check_tests(yaml.safe_load(args.tests.read_text()), names, str(args.tests), problems)

    for problem in problems:
        print(f"error: {problem}", file=sys.stderr)
    if problems:
        print(f"{len(problems)} problem(s); see docs/operate/runbooks.md and observability/prometheus/", file=sys.stderr)
        return 1
    print(f"{len(names)} alerts: each has a runbook section its link resolves to, and a firing and a quiet unit test")
    return 0


if __name__ == "__main__":
    sys.exit(main())
