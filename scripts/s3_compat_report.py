#!/usr/bin/env python3
"""Generate the S3 compatibility page and the README block from committed results (#722).

Reads tests/s3-compat/results/<id>.json (one run per server, refreshed by a
scheduled job's PR, never by hand) and tests/s3-compat/servers/<id>.yaml (the
display name and notes), and writes:

- docs/reference/s3-compatibility.md, the table of which server passed or
  refused which capability, with the verification method each case saw;
- the block between <!-- s3-compat:start --> and <!-- s3-compat:end --> in
  README.md, naming the servers whose latest results are all as expected.

Every cell is derived from a recorded outcome: a capability with no result
line is "not tested", never a pass. `--check` regenerates in memory and fails
on any difference from what is committed, and on a results file whose image
digest is not the one its server's compose file pins (a bump of the pin makes
the committed results stale until a run of the new image replaces them).

`--ingest DIR` turns the `results-<id>.jsonl` files a CI run uploaded into
`tests/s3-compat/results/<id>.json` (one run per server, the newest wins),
then regenerates. The scheduled `s3-compat-results` workflow does that and
opens a pull request; results are never edited by hand.

Usage: python3 scripts/s3_compat_report.py [--check | --ingest DIR] [--repo DIR]
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys

import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = "tests/s3-compat/results"
SERVERS = "tests/s3-compat/servers"
COMPOSE = "docker/s3-servers"
PAGE = "docs/reference/s3-compatibility.md"
README = "README.md"
START = "<!-- s3-compat:start -->"
END = "<!-- s3-compat:end -->"

CAPABILITIES = [
    "put_get_head",
    "path_style",
    "virtual_host",
    "checksum_sha256_enforced",
    "checksum_sha256_returned",
    "checksum_crc32_default",
    "multipart",
    "multipart_abort",
    "multipart_list_uploads",
    "multipart_resume",
    "versioning",
    "object_lock",
    "object_lock_enforced",
    "object_lock_retention_clear",
    "conditional_put",
]

OUTCOMES = {"pass", "fail", "refused", "not_applicable", "not_run", "unexpected_pass", "unexpected_fail"}

# What a cell says, and its order of precedence (the first that applies wins).
UNEXPECTED = "**unexpected**"
REFUSED = "refused as designed"
NOT_APPLICABLE = "not applicable"
KNOWN_FAILURE = "known failure"
UNKNOWN_FAILED = "failed (unknown)"
UNKNOWN_PASSED = "passed (unknown)"
PASSED = "tested: pass"
NOT_TESTED = "not tested"


class Refused(Exception):
    """Input the report will not turn into a table."""


def load_results(repo: str) -> list[dict]:
    runs = []
    for path in sorted(glob.glob(os.path.join(repo, RESULTS, "*.json"))):
        with open(path, encoding="utf-8") as handle:
            try:
                run = json.load(handle)
            except json.JSONDecodeError as err:
                raise Refused(f"{os.path.relpath(path, repo)}: not JSON: {err}") from err
        stem = os.path.splitext(os.path.basename(path))[0]
        rel = os.path.relpath(path, repo)
        for key in ("server", "run_date", "cases"):
            if key not in run:
                raise Refused(f"{rel}: no {key!r}")
        if run["server"] != stem:
            raise Refused(f"{rel}: server {run['server']!r} does not match the file name")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(run["run_date"])):
            raise Refused(f"{rel}: run_date {run['run_date']!r} is not YYYY-MM-DD")
        for case in run["cases"]:
            if case.get("server") != stem:
                raise Refused(f"{rel}: case {case.get('case')!r} is for server {case.get('server')!r}")
            if case.get("outcome") not in OUTCOMES:
                raise Refused(f"{rel}: case {case.get('case')!r} has outcome {case.get('outcome')!r}")
            for cap in case.get("capabilities", []):
                if cap not in CAPABILITIES:
                    raise Refused(f"{rel}: case {case.get('case')!r} names capability {cap!r}")
        run["_file"] = rel
        runs.append(run)
    return runs


def load_servers(repo: str) -> dict[str, dict]:
    servers = {}
    for path in sorted(glob.glob(os.path.join(repo, SERVERS, "*.yaml"))):
        with open(path, encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
        servers[data["id"]] = data
    return servers


def pinned_digest(repo: str, server: str) -> str | None:
    """The digest the server's compose file pins for its `server` image; None
    when the image is built from source (the lab's MinIO, pinned by commit in
    its Dockerfile) or there is no compose file."""
    path = os.path.join(repo, COMPOSE, server, "compose.yml")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as handle:
        compose = yaml.safe_load(handle)
    service = (compose.get("services") or {}).get("server") or {}
    if "build" in service:
        return None
    match = re.search(r"@(sha256:[0-9a-f]{64})$", str(service.get("image", "")))
    return match.group(1) if match else None


def cell(cases: list[dict], cap: str) -> str:
    """One capability's cell, from the cases that named it."""
    # A case the harness could not run (`not_run`, with why) tested nothing.
    mine = [c for c in cases if cap in c.get("capabilities", []) and c["outcome"] != "not_run"]
    if not mine:
        return NOT_TESTED
    if any(not c.get("expected", True) for c in mine):
        return UNEXPECTED
    decided = [c for c in mine if c.get("deciding_capability") == cap]
    for outcome, text in (("refused", REFUSED), ("not_applicable", NOT_APPLICABLE)):
        if any(c["outcome"] == outcome for c in decided):
            return text
    for c in decided:
        if c["outcome"] == "fail":
            match = re.search(r"#(\d+)", c.get("expectation", ""))
            return f"{KNOWN_FAILURE} (#{match.group(1)})" if match else UNKNOWN_FAILED
    if any(c["outcome"] == "pass" for c in decided):
        return UNKNOWN_PASSED
    if any(c["outcome"] == "pass" for c in mine):
        return PASSED
    return NOT_TESTED


def methods(cases: list[dict]) -> str:
    seen = sorted({c["method"] for c in cases if c.get("method") and c["outcome"] == "pass"})
    return ", ".join(f"`{m}`" for m in seen) or "-"


def all_expected(run: dict) -> bool:
    return bool(run["cases"]) and all(c.get("expected", True) for c in run["cases"])


def render_page(runs: list[dict], servers: dict[str, dict]) -> str:
    out = [
        "<!-- Generated by scripts/s3_compat_report.py from tests/s3-compat/results/; do not edit. -->",
        "",
        "# S3 compatibility",
        "",
        "Which S3 servers VTOP's live tests have run against, and what each did. Every cell is",
        "derived from a recorded test outcome in `tests/s3-compat/results/`: a capability with",
        "no result is **not tested**, never assumed. *Refused as designed* means the server lacks",
        "the capability and VTOP refused it, saying what is wrong, what it did and what to do,",
        "rather than weakening silently. How the matrix runs: [S3 compatibility matrix](../operate/s3-compatibility-matrix.md).",
        "",
    ]
    if not runs:
        out += ["No results are committed yet.", ""]
        return "\n".join(out)
    out += ["## Runs", "", "| Server | Version | Image digest | Run date | VTOP commit | All as expected |", "|---|---|---|---|---|---|"]
    for run in runs:
        name = servers.get(run["server"], {}).get("name") or run["server"]
        digest = run.get("image_digest") or "-"
        if digest.startswith("sha256:"):
            digest = f"`{digest[:19]}…`"
        commit = run.get("vtop_commit") or "-"
        commit = f"`{commit[:10]}`" if commit != "-" else commit
        out.append(
            f"| {name} | {run.get('server_version') or '-'} | {digest} | {run['run_date']} | {commit} | "
            f"{'yes' if all_expected(run) else '**no**'} |"
        )
    out += ["", "## Capabilities", ""]
    header = "| Capability | " + " | ".join(run["server"] for run in runs) + " |"
    out += [header, "|---|" + "---|" * len(runs)]
    for cap in CAPABILITIES:
        out.append(f"| `{cap}` | " + " | ".join(cell(run["cases"], cap) for run in runs) + " |")
    out.append("| verification method seen | " + " | ".join(methods(run["cases"]) for run in runs) + " |")
    notes = []
    for run in runs:
        caps = (servers.get(run["server"], {}) or {}).get("capabilities", {}) or {}
        for cap in CAPABILITIES:
            state = caps.get(cap)
            if isinstance(state, dict) and state.get("note"):
                notes.append(f"- {run['server']}, `{cap}`: {state['note']}")
    if notes:
        out += ["", "## Notes", "", "From each server's capability file; notes, not test results.", ""] + notes
    out += ["", "## Cases", ""]
    for run in runs:
        out += [f"### {run['server']}", "", "| Case | Capabilities | Expectation | Outcome | Method |", "|---|---|---|---|---|"]
        for c in sorted(run["cases"], key=lambda c: c["case"]):
            caps = ", ".join(f"`{x}`" for x in c.get("capabilities", []))
            out.append(f"| `{c['case']}` | {caps} | {c.get('expectation', '-')} | {c['outcome']} | {c.get('method') or '-'} |")
        out.append("")
    return "\n".join(out)


def render_readme_block(runs: list[dict], servers: dict[str, dict]) -> str:
    good = [servers.get(r["server"], {}).get("name") or r["server"] for r in runs if all_expected(r)]
    if not runs:
        body = (
            "No S3 compatibility results are committed yet; see "
            "[S3 compatibility](docs/reference/s3-compatibility.md) for how they are produced."
        )
    elif good:
        tested = ", ".join(good)
        body = (
            f"The live S3 tests last ran, with every outcome as expected, against: {tested}. "
            "Which server supports which capability and verification method: "
            "[S3 compatibility](docs/reference/s3-compatibility.md)."
        )
    else:
        body = (
            "No S3 server's committed live-test results are all as expected yet; see "
            "[S3 compatibility](docs/reference/s3-compatibility.md)."
        )
    return f"{START}\n{body}\n{END}"


def splice_readme(text: str, block: str) -> str:
    if text.count(START) != 1 or text.count(END) != 1:
        raise Refused(f"{README} must hold exactly one {START} ... {END} block")
    head, rest = text.split(START, 1)
    _, tail = rest.split(END, 1)
    return head + block + tail


def check_digests(runs: list[dict], repo: str) -> list[str]:
    problems = []
    for run in runs:
        pinned = pinned_digest(repo, run["server"])
        if pinned is not None and run.get("image_digest") != pinned:
            problems.append(
                f"{run['_file']} names image {run.get('image_digest')}, but {COMPOSE}/{run['server']}/compose.yml "
                f"pins {pinned}: the results are stale; the s3-compat-results workflow refreshes them from a run "
                "of the pinned image"
            )
    return problems


def ingest(source: str, repo: str) -> list[str]:
    """Write tests/s3-compat/results/<id>.json from each results-<id>.jsonl under `source`."""
    written = []
    for path in sorted(glob.glob(os.path.join(source, "**", "results-*.jsonl"), recursive=True)):
        with open(path, encoding="utf-8") as handle:
            cases = [json.loads(line) for line in handle if line.strip()]
        if not cases:
            continue
        server = cases[0]["server"]
        if any(c.get("server") != server for c in cases):
            raise Refused(f"{path}: lines for more than one server")
        first = cases[0]
        run = {
            "server": server,
            "server_version": first.get("server_version"),
            "image_digest": first.get("image_digest"),
            "vtop_commit": first.get("vtop_commit"),
            "run_date": os.environ.get("S3_COMPAT_RUN_DATE") or _today(),
            "cases": sorted(cases, key=lambda c: c["case"]),
        }
        os.makedirs(os.path.join(repo, RESULTS), exist_ok=True)
        out = os.path.join(repo, RESULTS, f"{server}.json")
        with open(out, "w", encoding="utf-8") as handle:
            json.dump(run, handle, indent=2, sort_keys=True)
            handle.write("\n")
        written.append(os.path.relpath(out, repo))
    return written


def _today() -> str:
    import datetime

    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="fail on drift instead of writing")
    mode.add_argument("--ingest", metavar="DIR", help="first turn a run's results-<id>.jsonl files into committed results")
    parser.add_argument("--repo", default=REPO)
    args = parser.parse_args(argv)
    try:
        if args.ingest:
            for written in ingest(args.ingest, args.repo):
                print(f"s3_compat_report: wrote {written}")
        runs = load_results(args.repo)
        servers = load_servers(args.repo)
        page = render_page(runs, servers)
        with open(os.path.join(args.repo, README), encoding="utf-8") as handle:
            readme = handle.read()
        new_readme = splice_readme(readme, render_readme_block(runs, servers))
    except Refused as err:
        print(f"s3_compat_report: {err}", file=sys.stderr)
        return 2
    page_path = os.path.join(args.repo, PAGE)
    if args.check:
        problems = check_digests(runs, args.repo)
        try:
            with open(page_path, encoding="utf-8") as handle:
                committed = handle.read()
        except FileNotFoundError:
            committed = None
        if committed != page:
            problems.append(f"{PAGE} differs from what the committed results produce")
        if new_readme != readme:
            problems.append(f"{README}'s s3-compat block differs from what the committed results produce")
        for problem in problems:
            print(f"s3_compat_report: {problem}", file=sys.stderr)
        if problems:
            print("Run 'python3 scripts/s3_compat_report.py' and commit the result.", file=sys.stderr)
            return 1
        return 0
    os.makedirs(os.path.dirname(page_path), exist_ok=True)
    with open(page_path, "w", encoding="utf-8") as handle:
        handle.write(page)
    with open(os.path.join(args.repo, README), "w", encoding="utf-8") as handle:
        handle.write(new_readme)
    return 0


if __name__ == "__main__":
    sys.exit(main())
