#!/usr/bin/env python3
"""Regenerate every cross-product chart and page from the committed runs, and check for drift (#744).

    python3 -m pip install --require-hashes -r benchmarks/kafka_compare/requirements-report.txt
    python3 benchmarks/compare_common/regenerate.py
    git status --porcelain -- docs/benchmarks/        # what CI checks next

Every run directory COMMITTED under `benchmarks/<kind>_compare/results/` is
handed to that kind's `report.py`. The rules are the Kafka comparison's
(`kafka_compare/regenerate.py`, whose checks this reuses): the pinned plotting
stack and matplotlib's own DejaVu Sans; a committed run whose files on disk
differ from HEAD is refused; two runs writing one file are refused; committed
output under a kind's image directory, or a page carrying the generated mark,
that no run writes is refused as stale; and a PNG whose pixels did not change
keeps its committed bytes.

A kind with no committed run yet (the measured runs need the maintainer's
hardware) is reported and skipped, and its output directory must then hold
nothing: output with no run behind it is stale.

The cross-product summary (`summary.py`, docs/benchmarks/comparison-summary.md
and its charts) is written last, from the same committed runs, and is checked
like any kind's output. Unlike a kind's page it exists with no run at all (every
cell "not measured yet"), so a missing or hand-edited page is drift.
"""
from __future__ import annotations

import argparse
import glob
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.realpath(__file__))
BENCH = os.path.dirname(HERE)
REPO = os.path.dirname(BENCH)
if BENCH not in sys.path:
    sys.path.insert(0, BENCH)

from compare_common import registry, schema, summary  # noqa: E402
from kafka_compare import regenerate as kr  # noqa: E402

OUTPUT = "docs/benchmarks"


def kinds(bench: str = BENCH) -> list[str]:
    """Every `<kind>_compare` package that has a report.py (not the Kafka comparison, which has its own)."""
    out = []
    for path in sorted(glob.glob(os.path.join(bench, "*_compare", "report.py"))):
        name = os.path.basename(os.path.dirname(path))
        if name != "kafka_compare":
            out.append(name[: -len("_compare")])
    return out


def committed_runs(kind: str, repo: str = REPO) -> list[str]:
    prefix = f"benchmarks/{kind}_compare/results"
    runs = set()
    for name in kr._committed_names(repo, prefix):
        parts = name[len(prefix) + 1:].split("/")
        if len(parts) > 1:
            runs.add(os.path.join(repo, prefix, parts[0]))
    return sorted(runs)


def report_spec(kind: str):
    import importlib

    return importlib.import_module(f"{kind}_compare.report").REPORT


def run_report(kind: str, run_dir: str) -> list[str]:
    script = os.path.join(BENCH, f"{kind}_compare", "report.py")
    proc = subprocess.run([sys.executable, script, run_dir], capture_output=True, text=True)
    if proc.returncode != 0:
        raise kr.ReportFailed(run_dir, proc.returncode, proc.stderr.strip())
    return [os.path.realpath(line) for line in proc.stdout.splitlines() if line.strip()]


def orphans(kind_outputs: dict[str, tuple[str, str]], written: set[str], repo: str = REPO) -> list[str]:
    """Committed output of a kind (its image directory, its page) that no run wrote, and any committed page
    under docs/benchmarks carrying this generator's mark that no run wrote: a run that sends its page elsewhere
    with a `report.json` (run_outputs.py) leaves that page behind when the run is deleted."""
    from compare_common import report as creport

    stale = []
    for _kind, (img_dir, doc) in sorted(kind_outputs.items()):
        for name in kr._committed_names(repo, img_dir) + kr._committed_names(repo, doc):
            if os.path.realpath(os.path.join(repo, name)) not in written:
                stale.append(name)
    for name in kr._committed_names(repo, OUTPUT):
        path = os.path.join(repo, name)
        if name.endswith(".md") and os.path.isfile(path) and os.path.realpath(path) not in written:
            with open(path, encoding="utf-8") as handle:
                if creport.GENERATED_MARK in handle.read():
                    stale.append(name)
    return sorted(set(stale))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_args(argv)
    try:
        names = kinds()
        if not names:
            raise kr.Refused("no comparison kind found under benchmarks/*_compare/")
        outputs = {}
        all_runs: dict[str, list[str]] = {}
        for kind in names:
            spec = report_spec(kind)
            outputs[kind] = (f"{OUTPUT}/{spec.img_dir}", f"{OUTPUT}/{spec.doc_name}.md")
            all_runs[kind] = committed_runs(kind)
        if any(all_runs.values()):
            kr.check_stack(kr.read_pins())
            kr.check_font()
        for runs in all_runs.values():
            kr.check_runs_unchanged(runs)
        owner: dict[str, str] = {}
        clashes = []
        for kind, runs in all_runs.items():
            for run in runs:
                for path in run_report(kind, run):
                    if path in owner:
                        clashes.append(f"{path} (by {owner[path]} and {run})")
                    owner[path] = run
        if clashes:
            raise kr.Refused("two runs write the same file, so only the last is checked: " + "; ".join(clashes))
        # The summary draws the same runs once more, in its own files.
        try:
            written = summary.build({kind: runs[0] for kind, runs in all_runs.items() if runs}, REPO)
        except (schema.RunRefused, registry.RegistryError) as exc:
            raise kr.Refused(f"the summary cannot be drawn: {exc}") from exc
        for path in written:
            owner[os.path.realpath(path)] = "the summary"
        outputs["summary"] = (summary.IMG_REL, summary.DOC_REL)
        stale = orphans(outputs, set(owner))
        if stale:
            raise kr.Refused("committed output that no run writes (stale; delete it or restore its run): "
                             + ", ".join(stale))
        kept = kr.keep_committed_pngs(owner)
    except kr.Refused as exc:
        print(f"regenerate refused: {exc}", file=sys.stderr)
        return 2
    except kr.ReportFailed as exc:
        print(f"regenerate failed: {exc}", file=sys.stderr)
        return exc.code
    for kind, runs in all_runs.items():
        if not runs:
            print(f"{kind}: no committed run yet; nothing to regenerate, and no committed output may exist")
        for run in runs:
            print(f"{kind}: run {os.path.relpath(run, REPO)}")
    for path in sorted(owner):
        note = " (pixels unchanged: committed bytes kept)" if path in kept else ""
        print(f"  wrote {os.path.relpath(path, REPO)}{note}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
