#!/usr/bin/env python3
"""Gate a cargo-mutants run on its mutation score (#729).

Reads the `outcomes.json` cargo-mutants writes, counts each mutant by its
summary, and computes

    score = caught / (caught + missed + timeout)

Unviable mutants (ones that do not compile) are left out, as cargo-mutants
itself leaves them out. A timeout counts against the score: a mutant the
tests could not finish on is not a mutant they caught.

Every missed and timed-out mutant is printed with its location, and the
per-file counts go to stdout and, with --summary, to a Markdown file (the
job's step summary).

Exit codes:
  0  the score is at or above --min-score (or --report-only)
  1  the score is below --min-score
  2  the run is not evidence: the file is missing or not JSON, the baseline
     failed, a mutant has an outcome this script does not classify, there
     are no viable mutants, or a file named by --expect-file has none

Usage:
  scripts/mutants-gate.py --outcomes mutants.out/outcomes.json --min-score 0.90 \\
      [--expect-file crates/vtop-core/src/replay.rs ...] [--summary FILE]
  scripts/mutants-gate.py --outcomes mutants.out/outcomes.json --report-only
"""

import argparse
import collections
import json
import pathlib
import sys

COUNTED = ("CaughtMutant", "MissedMutant", "Timeout", "Unviable")


class NotEvidence(Exception):
    """The outcomes cannot be scored; the gate refuses rather than passes."""


def score(caught, missed, timeout):
    viable = caught + missed + timeout
    return None if viable == 0 else caught / viable


def describe(mutant):
    """The mutant as cargo-mutants names it, or rebuilt from its parts."""
    name = mutant.get("name")
    if name:
        return name
    line = (mutant.get("span") or {}).get("start", {}).get("line", "?")
    function = (mutant.get("function") or {}).get("function_name", "?")
    return f"{mutant.get('file', '?')}:{line}: replace {function} with {mutant.get('replacement', '?')}"


def tally(document):
    """Per-file counts and the list of missed or timed-out mutants."""
    entries = document.get("outcomes") if isinstance(document, dict) else None
    if not isinstance(entries, list):
        raise NotEvidence("outcomes.json has no `outcomes` list")
    files = collections.OrderedDict()
    survivors = []
    for entry in entries:
        scenario = entry.get("scenario")
        summary = entry.get("summary")
        if scenario == "Baseline":
            if summary != "Success":
                raise NotEvidence(
                    f"the baseline (unmutated) build or tests did not pass (summary {summary!r}); "
                    "no mutant result means anything until it does"
                )
            continue
        mutant = scenario.get("Mutant") if isinstance(scenario, dict) else None
        if mutant is None:
            raise NotEvidence(f"unrecognised scenario {scenario!r}")
        if summary not in COUNTED:
            raise NotEvidence(
                f"mutant {describe(mutant)} has outcome {summary!r}, which is neither caught, "
                "missed, timeout nor unviable (a --check run tests nothing)"
            )
        counts = files.setdefault(mutant.get("file", "?"), collections.Counter())
        counts[summary] += 1
        if summary in ("MissedMutant", "Timeout"):
            survivors.append((summary, describe(mutant)))
    return files, survivors


def render(files, survivors, total, threshold, report_only):
    lines = []
    for path, counts in files.items():
        s = score(counts["CaughtMutant"], counts["MissedMutant"], counts["Timeout"])
        lines.append(
            f"{path}  {counts['CaughtMutant']} caught  {counts['MissedMutant']} missed  "
            f"{counts['Timeout']} timeout  {counts['Unviable']} unviable  "
            f"score {'n/a' if s is None else f'{s:.3f}'}"
        )
    caught = sum(c["CaughtMutant"] for c in files.values())
    missed = sum(c["MissedMutant"] for c in files.values())
    timeout = sum(c["Timeout"] for c in files.values())
    unviable = sum(c["Unviable"] for c in files.values())
    verdict = "report only" if report_only else f"threshold {threshold:.3f}"
    lines.append(
        f"total  {caught} caught  {missed} missed  {timeout} timeout  {unviable} unviable  "
        f"score {'n/a' if total is None else f'{total:.3f}'}  ({verdict})"
    )
    if survivors:
        lines.append("")
        lines.append("Mutants the tests did not catch:")
        lines.extend(f"  {kind}: {name}" for kind, name in survivors)
    return lines


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outcomes", default="mutants.out/outcomes.json")
    parser.add_argument("--min-score", type=float)
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument("--expect-file", action="append", default=[])
    parser.add_argument("--summary", help="append a Markdown report to this file")
    parser.add_argument("--title", default="Mutation testing")
    args = parser.parse_args(argv)

    if not args.report_only:
        if args.min_score is None or not 0.0 <= args.min_score <= 1.0:
            print(f"mutants gate: --min-score must be a fraction in 0..1, got {args.min_score!r}")
            return 2

    try:
        try:
            document = json.loads(pathlib.Path(args.outcomes).read_text())
        except OSError as error:
            raise NotEvidence(f"cannot read {args.outcomes}: {error}") from error
        except json.JSONDecodeError as error:
            raise NotEvidence(f"{args.outcomes} is not JSON: {error}") from error
        files, survivors = tally(document)
        caught = sum(c["CaughtMutant"] for c in files.values())
        missed = sum(c["MissedMutant"] for c in files.values())
        timeout = sum(c["Timeout"] for c in files.values())
        total = score(caught, missed, timeout)
        if total is None:
            raise NotEvidence(
                "no viable mutants: nothing was tested, so there is no score "
                "(did the --file globs match any source?)"
            )
        for expected in args.expect_file:
            counts = files.get(expected, collections.Counter())
            if counts["CaughtMutant"] + counts["MissedMutant"] + counts["Timeout"] == 0:
                raise NotEvidence(f"{expected} has no viable mutant in this run (renamed, or left out of --file?)")
    except NotEvidence as refusal:
        print(f"mutants gate: refused: {refusal}")
        if args.summary:
            with open(args.summary, "a", encoding="utf-8") as out:
                out.write(f"## {args.title}\n\nRefused: {refusal}\n")
        return 2

    lines = render(files, survivors, total, args.min_score or 0.0, args.report_only)
    print("\n".join(lines))
    if args.summary:
        with open(args.summary, "a", encoding="utf-8") as out:
            out.write(f"## {args.title}\n\n```\n" + "\n".join(lines) + "\n```\n")

    if args.report_only:
        return 0
    if total < args.min_score:
        print(
            f"mutants gate: score {total:.3f} is below the threshold {args.min_score:.3f}; "
            "add a test that fails on each mutant listed above"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
