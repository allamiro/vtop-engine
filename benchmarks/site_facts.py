#!/usr/bin/env python3
"""Every figure and line the README's cards (and, later, the site's landing page) show, from one place.

    python3 benchmarks/site_facts.py            # prints the facts as JSON

`site_facts()` reads committed files only and returns display strings and
numbers together: a bar's length as a fraction, and the text it prints. The
cards (readme_cards.py) and the alt text in README.md draw on these fields and
format nothing themselves, so a number in an image is the number in the data.
It needs no matplotlib.

The measurements go through readme_charts.py's loaders (which go through
kafka_compare's own, refusing a partial or mixed run), never a second parser:

- failover:   benchmarks/kafka_compare/results/failover-20261111T032549Z/failover.csv
- link share: benchmarks/measurements/483-tcp-baseline/points.csv and its environment.json
- policed:    benchmarks/measurements/663-policed-uplink/runs.csv
- throughput: benchmarks/kafka_compare/results/20261109T232328Z/results.csv

The rest is read from the repository as it stands: the commit rule and the
quick-start commands from README.md (so a card and the text cannot differ),
the version from Cargo.toml, the Kafka and MinIO images from
docker-compose.yml and ci.yml, librdkafka from Cargo.lock, and whether an
`ee/` directory exists. S3 servers beyond the lab's appear when the
compatibility matrix (#722) commits its results under tests/s3-compat/results/.

A sentence that states a finding is built from the numbers it rests on, so a
re-run that changes the result changes the sentence rather than leaving it
false.

Refuses (`Refused`, exit 2) on what the loaders refuse, and on a source file
that is missing or no longer holds the field read from it.
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys
from typing import Any

HERE = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, HERE)

import readme_charts  # noqa: E402
from kafka_compare import footer, report, schema  # noqa: E402
from kafka_compare.failover_stats import FAULT_LABELS  # noqa: E402

REPO = readme_charts.REPO
SCHEMA = 1
README = "README.md"
COMPOSE = "docker-compose.yml"
CI = ".github/workflows/ci.yml"
KAFKA_MATRIX = ".github/workflows/kafka-matrix.yml"
POSTGRES_MATRIX = ".github/workflows/postgres-matrix.yml"
CARGO_TOML = "Cargo.toml"
CARGO_LOCK = "Cargo.lock"
EGRESS_ENV = "benchmarks/measurements/483-tcp-baseline/environment.json"
S3_RESULTS = "tests/s3-compat/results"
CARDS = ("card-hero", "card-link-share", "card-throughput", "card-tested")

HEADLINE = "Source progress moves only after the stored object is verified."
CLIENT_PATHS = {"vtop-native": "VTOP's native client", "kafka-jvm": "Kafka's JVM client"}


class Refused(readme_charts.Refused):
    """The facts cannot be vouched for; the message says why. A readme_charts.Refused, so
    readme_charts.py's main() reports it and exits 2 like its own refusals."""


# ------------------------------------------------------------- repository text


def _read(rel: str, repo: str) -> str:
    path = os.path.join(repo, rel)
    if not os.path.isfile(path):
        raise Refused(f"{rel} is missing")
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def _one(pattern: str, text: str, where: str, what: str, flags: int = 0) -> re.Match:
    match = re.search(pattern, text, flags)
    if not match:
        raise Refused(f"{where}: no {what}")
    return match


def _section(text: str, heading: str, where: str) -> str:
    match = _one(rf"^## {re.escape(heading)}\n(.*?)(?=^## |\Z)", text, where, f"'## {heading}' section", re.S | re.M)
    return match.group(1)


def _fences(text: str, lang: str) -> list[list[str]]:
    return [block.strip("\n").split("\n") for block in re.findall(rf"^```{lang}\n(.*?)^```", text, re.S | re.M)]


def commit_rule(repo: str = REPO) -> str:
    """The rule as README.md's `## The one rule` states it, in its first ```text fence."""
    blocks = _fences(_section(_read(README, repo), "The one rule", README), "text")
    if not blocks or len(blocks[0]) != 1:
        raise Refused(f"{README}: '## The one rule' does not open with a one-line ```text fence")
    return blocks[0][0]


def quickstart(repo: str = REPO) -> dict[str, list[str]]:
    """The two ```bash fences under README.md's `## Quick start`: the Docker lab, then a build from source."""
    blocks = _fences(_section(_read(README, repo), "Quick start", README), "bash")
    if len(blocks) != 2:
        raise Refused(f"{README}: '## Quick start' has {len(blocks)} bash fences, not two (lab, then build)")
    return {"lab": blocks[0], "build": blocks[1]}


def version(repo: str = REPO) -> str:
    text = _read(CARGO_TOML, repo)
    section = _one(r"^\[workspace\.package\]\n(.*?)(?=^\[|\Z)", text, CARGO_TOML, "[workspace.package]", re.S | re.M)
    return _one(r'^version = "([^"]+)"', section.group(1), CARGO_TOML, "workspace version", re.M).group(1)


def _images(text: str, name: str) -> list[str]:
    return sorted(set(re.findall(rf"^\s*image:\s*{re.escape(name)}:(\S+)\s*$", text, re.M)))


def _pins(path: str, pattern: str, what: str, repo: str) -> list[str]:
    """The releases a matrix workflow pins, in the order of its `PINNED_IMAGES` list, and nothing that
    only looks like one: each line must name its image with the same release it starts with."""
    found = re.findall(pattern, _read(path, repo), re.M)
    if not found:
        raise Refused(f"{path}: no pinned {what} found")
    return found


def _span(releases: list[str], key) -> tuple[str, str]:
    ordered = sorted(releases, key=key)
    return ordered[0], ordered[-1]


def _nightly(path: str, repo: str) -> str:
    return "nightly" if re.search(r"^\s+schedule:\s*$", _read(path, repo), re.M) else "on pull requests"


def kafka_matrix(repo: str = REPO) -> dict[str, str]:
    """The Apache Kafka releases the live-broker suite runs against, from the workflow that pins them."""
    # A line may end with the version `kafka-broker.sh version` must print (#834), which for an Apache
    # release is the release itself; any other trailing word means the line is not one of these.
    pin = r"^\s+(\d+(?:\.\d+)+) apache/kafka:\1@sha256:[0-9a-f]{64}(?:\s+\1)?\s*$"
    releases = _pins(KAFKA_MATRIX, pin, "Kafka release", repo)
    low, high = _span(releases, lambda v: tuple(int(n) for n in v.split(".")))
    return {"name": "Apache Kafka", "version": f"{low} to {high}",
            "ran": f"{len(releases)} releases, live-broker tests, {_nightly(KAFKA_MATRIX, repo)}",
            "source": KAFKA_MATRIX}


def postgres_matrix(repo: str = REPO) -> dict[str, str]:
    """The PostgreSQL majors the state store's battery runs against, from the workflow that pins them."""
    majors = _pins(POSTGRES_MATRIX, r"^\s+(\d+) postgres:\1@sha256:[0-9a-f]{64}\s*$", "PostgreSQL major", repo)
    low, high = _span(majors, int)
    return {"name": "PostgreSQL", "version": f"{low} to {high}",
            "ran": f"{len(majors)} majors, state-store tests, {_nightly(POSTGRES_MATRIX, repo)}",
            "source": POSTGRES_MATRIX}


def link_conditions(repo: str = REPO) -> dict[str, str]:
    """The lossy 100 ms link's round trip, rate and loss, from the #483 run's own description of its links."""
    links = json.loads(_read(EGRESS_ENV, repo)).get("links", {})
    clean, lossy = links.get("wan100", ""), links.get("wan100-loss1", "")
    kbit = int(_one(r"tbf (\d+) kbit/s", clean, EGRESS_ENV, "tbf rate in links.wan100").group(1))
    return {
        "rtt_ms": _one(r"(\d+) ms round trip", clean, EGRESS_ENV, "round trip in links.wan100").group(1),
        # :g keeps a fractional rate (1500 kbit/s is 1.5 Mbit/s) and prints a whole one without ".0".
        "rate_mbit": f"{kbit / 1000:g}",
        "loss_pct": _one(r"(\d+)% \w+ loss", lossy, EGRESS_ENV, "loss in links.wan100-loss1").group(1),
    }


def _client(text: str, where: str) -> tuple[str, str]:
    """('JVM', '4.3.2') from a client string such as `kafka-producer-perf-test/... 4.3.2 (JVM)`."""
    match = _one(r"\s(\d+(?:\.\d+)+) \((\w+)\)", text, where, "client version and runtime")
    return match.group(2), match.group(1)


def _ci_runs_live_s3_on_minio(ci: str) -> bool:
    """Whether ci.yml runs the live S3 tests against the lab's MinIO on a pull request.

    Before #837 a job started it with `docker compose up -d minio`; since #837 ci.yml calls the
    S3 compatibility matrix (`s3-compat-matrix.yml`) with `servers` naming minio, whose leg builds
    the same docker/minio image. Either form counts; a matrix call without minio does not.
    """
    if re.search(r"docker compose up -d minio\b", ci):
        return True
    for call in re.finditer(r"uses: \./\.github/workflows/s3-compat-matrix\.yml[ \t]*\n((?:[ \t]+\S.*\n?)*)", ci):
        servers = re.search(r"servers:\s*'?\[([^\]]*)\]", call.group(1))
        if servers and re.search(r"[\"']minio[\"']", servers.group(1)):
            return True
    return False


def s3_servers(repo: str = REPO) -> dict[str, Any]:
    """The lab's MinIO, plus one entry per committed result of the S3 compatibility matrix (#722).

    Until #722's report lands there are no results, and the strip says when more servers will appear.
    Each results file names its `server` and `version`; anything else in it belongs to #722's report.
    """
    compose = _read(COMPOSE, repo)
    lab = _images(compose, "vtop-minio")
    if len(lab) != 1:
        raise Refused(f"{COMPOSE}: {len(lab)} vtop-minio releases, not one")
    places = ["Docker lab"]
    if _ci_runs_live_s3_on_minio(_read(CI, repo)):
        places.append("live S3 tests in CI")
    entries = []
    for path in sorted(glob.glob(os.path.join(repo, S3_RESULTS, "*.json"))):
        rel = os.path.relpath(path, repo)
        with open(path, encoding="utf-8") as handle:
            result = json.load(handle)
        if not isinstance(result, dict) or not all(isinstance(result.get(k), str) and result[k]
                                                   for k in ("server", "version")):
            raise Refused(f"{rel}: a matrix result must name its server and version")
        entries.append({"name": result["server"], "version": result["version"], "ran": "S3 compatibility matrix",
                        "source": rel})
    return {
        "lab": {"name": "MinIO", "version": lab[0], "ran": " and ".join(places), "source": COMPOSE},
        "matrix": {"available": bool(entries), "entries": entries,
                   "placeholder": "More S3 servers: when the matrix is published (#722)"},
    }


# ------------------------------------------------------------------- the facts


def _source(path: str) -> dict[str, str]:
    """A data file's path, and the short name a card prints for it: the run's directory."""
    return {"path": path, "text": path.split("/")[-2]}


def _seconds(ms: float) -> str:
    return f"{ms / 1000:.1f} s"


def _share_words(committed: int, seeded: int) -> str:
    if committed == 0:
        return "nothing"
    if committed == seeded:
        return "everything"
    return f"{committed} of {seeded}"


def _tiles(fo, egress, conditions, policed, tp, ratios) -> list[dict[str, Any]]:
    vtop = fo["per_system"]["vtop"]
    kill = {system: fo["cells"][("kill9_leader", system)].median for system in ("kafka", "vtop")}
    sooner = "Kafka" if kill["kafka"] < kill["vtop"] else "VTOP"
    safe = vtop["lost"] == 0 and vtop["duplicates"] == 0 and vtop["read_back"] == vtop["repetitions"]
    # A failover whose read-back did not finish (verify FAILED, or a failed or refused row) has no lost or
    # duplicated count: the tile must not read "0 lost" over it.
    unverified = vtop["repetitions"] - vtop["read_back"]
    counted = f"{vtop['lost']} lost, {vtop['duplicates']} duplicated"
    if unverified:
        value = f"{unverified} of {vtop['repetitions']} not verified"
        detail = (f"The read-back did not finish in {unverified} of {vtop['repetitions']} leader failovers, so "
                  f"their lost and duplicated records are not known; the other {vtop['read_back']}: {counted}. ")
    else:
        value = counted
        detail = f"{vtop['repetitions']} leader failovers, {vtop['acked']:,} records read back, through " \
                 f"{CLIENT_PATHS[vtop['client_path']]}. "
    lossy = egress["default"]["wan100-loss1"]
    single = egress["grid"][("wan100-loss1", 1)][0]
    before, after = policed[("policed", "before")], policed[("policed", "after")]
    low, high = min(ratios), max(ratios)
    failover_csv = f"{readme_charts.FAILOVER_RUN}/failover.csv"
    return [
        {"id": "failover", "label": "Acknowledged records lost or duplicated, VTOP",
         "value": value,
         "detail": f"{detail}{sooner} resumed sooner after a "
                   f"{FAULT_LABELS['kill9_leader']}: {_seconds(kill['kafka'])} for Kafka, "
                   f"{_seconds(kill['vtop'])} for VTOP.",
         "condition": f"{fo['per_cell']} repetitions per fault, {len(readme_charts.FAULT_ORDER)} faults, one host",
         "tone": "positive" if safe else "neutral", "badge": None, "source": _source(failover_csv)},
        {"id": "link-share", "label": f"Share of a lossy {conditions['rtt_ms']} ms link carried",
         "value": f"{lossy[0]:.3f}",
         "detail": f"The shipped default, median of {lossy[3]} runs. One upload at a time carried {single[1]:.2f}.",
         "condition": f"{conditions['rate_mbit']} Mbit/s, {conditions['rtt_ms']} ms, "
                      f"{conditions['loss_pct']}% burst loss, emulated",
         "tone": "positive", "badge": None, "source": _source(readme_charts.EGRESS)},
        {"id": "policed", "label": "Objects committed on a policed uplink",
         "value": f"{after['committed']} of {after['seeded']}",
         "detail": f"{before['committed']} of {before['seeded']} before the fix in #666, which lets a "
                   "request's deadline grow with its body.",
         "condition": f"{conditions['rate_mbit']} Mbit/s policer, no queue",
         "tone": "positive" if after["committed"] == after["seeded"] else "neutral", "badge": None,
         "source": _source(readme_charts.POLICED)},
        {"id": "throughput", "label": "Kafka's produce rate over VTOP's Kafka gateway",
         "value": f"{low:.1f}x to {high:.1f}x",
         "detail": f"Records per second at saturation, same JVM producer, {tp['cpus']} CPUs per node. "
                   + ("Here VTOP is behind." if low > 1 else "VTOP is not behind in every cell."),
         "condition": "each system as shipped, one host",
         "tone": "neutral", "badge": "Behind" if low > 1 else None,
         "source": _source(f"{readme_charts.THROUGHPUT_RUN}/results.csv")},
    ]


def _link_share(egress, conditions) -> dict[str, Any]:
    grid = egress["grid"][("wan100-loss1", 1)]
    lossy = egress["default"]["wan100-loss1"]
    scale = max(1.0, lossy[0], *(point[1] for point in grid))
    rows = [{"label": f"{c} upload at a time" if c == 1 else f"{c} at once", "value": median,
             "text": f"{median:.2f}", "fraction": median / scale} for c, median, _low, _high in grid]
    return {
        "title": "Share of the link carried, by uploads in flight",
        "subtitle": f"Lossy {conditions['rtt_ms']} ms link: {conditions['rate_mbit']} Mbit/s, "
                    f"{conditions['loss_pct']}% burst loss. One part in flight per object; "
                    f"median of {egress['grid_runs']} runs per row.",
        "rows": rows,
        "default": {"label": "Shipped default", "value": lossy[0], "text": f"{lossy[0]:.3f}",
                    "fraction": lossy[0] / scale, "note": f"at its own part settings, median of {lossy[3]} runs"},
        "reference": {"label": "the whole link", "value": 1.0, "fraction": 1.0 / scale},
        "source": _source(readme_charts.EGRESS),
    }


def _recovery(fo) -> dict[str, Any]:
    top = max(fo["cells"][(fault, system)].high for fault in readme_charts.FAULT_ORDER
              for system in ("kafka", "vtop"))
    faults = []
    for fault in readme_charts.FAULT_ORDER:
        entry = {"label": FAULT_LABELS[fault]}
        for system in ("kafka", "vtop"):
            cell = fo["cells"][(fault, system)]
            entry[system] = {"value": cell.median / 1000, "text": _seconds(cell.median),
                             "range_text": f"{_seconds(cell.low)} to {_seconds(cell.high)}",
                             "fraction": cell.median / top}
        faults.append(entry)
    return {"title": "Seconds without an acknowledgement after a fault", "faults": faults,
            "source": _source(f"{readme_charts.FAILOVER_RUN}/failover.csv")}


def _throughput(tp, ratios) -> dict[str, Any]:
    top = max(tp["cells"][(size, system, "mib")].high for size in tp["sizes"] for system in ("kafka", "vtop"))
    groups = []
    for size, ratio in zip(tp["sizes"], ratios):
        group = {"label": f"{report.size_label(size)} records", "ratio_text": f"Kafka: {ratio:.1f}x VTOP's records/s"}
        for system in ("kafka", "vtop"):
            cell = tp["cells"][(size, system, "mib")]
            group[system] = {"value": cell.median, "text": f"{cell.median:.2f}",
                             "range_text": f"{cell.low:.2f} to {cell.high:.2f}", "fraction": cell.median / top,
                             "low_fraction": cell.low / top, "high_fraction": cell.high / top}
        groups.append(group)
    return {
        "title": "Produce throughput at saturation, MiB/s",
        "subtitle": f"Same JVM producer, acks=all, {tp['cpus']} CPUs per node. Bar: median of "
                    f"{tp['repetitions']} repetitions; whisker: min to max.",
        "series": {"kafka": "Apache Kafka", "vtop": "VTOP (Kafka gateway)"},
        "groups": groups,
        "ratio_text": f"Kafka carries {min(ratios):.1f}x to {max(ratios):.1f}x VTOP's records per second.",
        "source": _source(f"{readme_charts.THROUGHPUT_RUN}/results.csv"),
    }


def _results(fo, egress, policed, ratios, tp) -> list[dict[str, Any]]:
    unverified = fo["repetitions"] - fo["read_back"]
    clean = fo["lost"] == 0 and fo["duplicates"] == 0 and not unverified
    kill = {system: fo["cells"][("kill9_leader", system)].median for system in ("kafka", "vtop")}
    sooner = "Kafka" if kill["kafka"] < kill["vtop"] else "VTOP"
    grid = egress["grid"][("wan100-loss1", 1)]
    best = max(grid, key=lambda point: point[1])
    before, after = policed[("policed", "before")], policed[("policed", "after")]
    low = min(ratios)
    return [
        {"id": "failover",
         "finding": ("No acknowledged record was lost or duplicated by either system in the run. " if clean else
                     f"The read-back did not finish in {unverified} of {fo['repetitions']} failovers, so what "
                     "they lost or duplicated is not known. " if unverified else
                     "Acknowledged records were lost or duplicated in the run. ")
         + f"{sooner} resumed sooner after a {FAULT_LABELS['kill9_leader']}.",
         "figure": f"{fo['lost']} lost, {fo['duplicates']} duplicated, {fo['repetitions']} failovers"
                   + (f", {unverified} not verified" if unverified else ""),
         "condition": f"{fo['per_cell']} repetitions per fault and system, one host",
         "data": f"{readme_charts.FAILOVER_RUN}/failover.csv", "method": "docs/benchmarks/kafka-failover.md"},
        {"id": "link-share",
         "finding": ("Concurrent uploads fill a long, lossy link; one at a time does not."
                     if best[1] > grid[0][1] else "One upload at a time carried as much of the lossy link as more."),
         "figure": f"{grid[0][1]:.2f} with one upload, {best[1]:.2f} with {best[0]} at once",
         "condition": "emulated link, one host",
         "data": readme_charts.EGRESS, "method": "docs/EGRESS_TRANSPORT_RESEARCH.md"},
        {"id": "policed",
         "finding": f"A link that drops what exceeds its rate committed {_share_words(**_count(before))} "
                    f"before #666 and {_share_words(**_count(after))} after.",
         "figure": f"{before['committed']} of {before['seeded']}, then {after['committed']} of {after['seeded']}",
         "condition": "policed uplink, shipped upload defaults, one host",
         "data": readme_charts.POLICED, "method": "docs/EGRESS_TRANSPORT_RESEARCH.md"},
        {"id": "throughput",
         "finding": ("VTOP's Kafka gateway is slower than Kafka" if low > 1 else
                     "VTOP's Kafka gateway is not slower than Kafka in every cell")
         + ", and a Kafka client cannot yet follow a VTOP failover.",
         "figure": f"{min(ratios):.1f}x to {max(ratios):.1f}x",
         "condition": f"same JVM producer, {tp['cpus']} CPUs per node, one host",
         "data": f"{readme_charts.THROUGHPUT_RUN}/results.csv", "method": "docs/benchmarks/kafka-comparison.md"},
    ]


def _count(cell: dict[str, Any]) -> dict[str, int]:
    return {"committed": cell["committed"], "seeded": cell["seeded"]}


def _tested(repo, tp, fo, s3) -> dict[str, Any]:
    compose, ci = _read(COMPOSE, repo), _read(CI, repo)
    places: dict[str, list[str]] = {}
    for env in (tp["environment"], fo["environment"]):
        tag = env["comparable"]["kafka_image"].split("@")[0].partition(":")[2]
        places.setdefault(tag, [])
        if "benchmarks" not in places[tag]:
            places[tag].append("benchmarks")
    for tag in _images(compose, "apache/kafka"):
        places.setdefault(tag, []).append("Docker lab")
    for tag in _images(ci, "apache/kafka"):
        places.setdefault(tag, []).append("live-broker tests in CI")
    if not places:
        raise Refused("no Kafka version found in the runs, docker-compose.yml or ci.yml")
    kafka = [kafka_matrix(repo)] + [{"name": "Apache Kafka", "version": tag, "ran": " and ".join(where)}
                                    for tag, where in sorted(places.items(), reverse=True)]
    clients = {}
    for env, key in ((tp["environment"], "client"), (fo["environment"], "client_kafka")):
        runtime, ver = _client(env["comparable"][key], f"{env['run_id']}/environment.json {key}")
        clients[(runtime, ver)] = {"name": f"Kafka {runtime} client", "version": ver, "ran": "benchmarks"}
    lock = _one(r'name = "rdkafka-sys"\nversion = "[^"+]*\+([^"]+)"', _read(CARGO_LOCK, repo), CARGO_LOCK,
                "rdkafka-sys with a bundled librdkafka version")
    client_list = list(clients.values()) + [
        {"name": "librdkafka", "version": lock.group(1), "ran": "the engine's Kafka source"}]
    return {"kafka": kafka, "clients": client_list, "state_store": [postgres_matrix(repo)],
            "s3": [s3["lab"]] + s3["matrix"]["entries"],
            "s3_matrix": s3["matrix"]}


def site_facts(repo: str = REPO) -> dict[str, Any]:
    try:
        fo = readme_charts.failover(repo)
        egress = readme_charts.egress_points(repo)
        policed = readme_charts.policed_runs(repo)
        tp = readme_charts.throughput(repo)
    except (OSError, KeyError) as exc:
        raise Refused(f"a measurement file is missing or lacks a field: {exc}") from exc
    for key in (("policed", "before"), ("policed", "after")):
        if key not in policed:
            raise Refused(f"{readme_charts.POLICED}: no {key[1]} runs on the policed link")
    ratios = [tp["cells"][(size, "kafka", "rps")].median / tp["cells"][(size, "vtop", "rps")].median
              for size in tp["sizes"]]
    conditions = link_conditions(repo)
    ver = version(repo)
    major = ver.split(".")[0]
    prototype = int(major) == 0
    facts = {
        "schema": SCHEMA,
        "prototype": prototype,
        "hero": {
            "pill": f"Prototype, {major}.x" if prototype else f"Release {major}.x",
            "headline": HEADLINE,
            "rule": commit_rule(repo),
            "caveat": footer.CAVEAT[0].upper() + footer.CAVEAT[1:] + ".",
            "section": "Measured, not claimed",
        },
        "tiles": _tiles(fo, egress, conditions, policed, tp, ratios),
        "charts": {"link_share": _link_share(egress, conditions), "recovery": _recovery(fo),
                   "throughput": _throughput(tp, ratios)},
        "results": _results(fo, egress, policed, ratios, tp),
        "tested": _tested(repo, tp, fo, s3_servers(repo)),
        "quickstart": quickstart(repo),
        "editions": {
            "ee_dir_exists": os.path.isdir(os.path.join(repo, "ee")),
            "core": "MIT",
            "enterprise": ("An ee/ directory exists; what is in it is under LICENSE-EE."
                           if os.path.isdir(os.path.join(repo, "ee")) else
                           "No ee/ directory exists yet; nothing in the repository is under LICENSE-EE today."),
        },
        "footers": {"throughput": footer.footer_text(tp["environment"]),
                    "failover": footer.footer_text(fo["environment"])},
    }
    facts["alts"] = alts(facts)
    for tile in facts["tiles"]:
        if not os.path.isfile(os.path.join(repo, tile["source"]["path"])):
            raise Refused(f"tile {tile['id']}: its source {tile['source']['path']} is missing")
    return facts


# ---------------------------------------------------------------- the alt text


def alts(facts: dict[str, Any]) -> dict[str, str]:
    """The README's alt text for each card, from the fields the card draws. Never a double quote:
    the docs build's <picture> pattern and the HTML attribute both end at one."""
    hero = facts["hero"]
    tiles = " ".join(f"{tile['label']}: {tile['value']}" + (f" ({tile['badge']})" if tile["badge"] else "")
                     + f". {tile['detail']}" for tile in facts["tiles"])
    link = facts["charts"]["link_share"]
    rows = ", ".join(f"{row['label']} {row['text']}" for row in link["rows"])
    tp = facts["charts"]["throughput"]
    groups = " ".join(f"{group['label']}: {tp['series']['kafka']} {group['kafka']['text']}, "
                      f"{tp['series']['vtop']} {group['vtop']['text']}." for group in tp["groups"])
    tested = facts["tested"]
    chips = "; ".join(f"{item['name']} {item['version']} ({item['ran']})"
                      for item in tested["kafka"] + tested["clients"] + tested["state_store"] + tested["s3"])
    out = {
        "card-hero": f"{hero['pill']}. {hero['headline']} The rule: {hero['rule']} {hero['section']} "
                     f"({hero['caveat'][0].lower()}{hero['caveat'][1:].rstrip('.')}). {tiles}",
        "card-link-share": f"Bar list. {link['title']}. {link['subtitle']} {rows}. "
                           f"{link['default']['label']}: {link['default']['text']}, {link['default']['note']}, "
                           f"against {link['reference']['label']} at {link['reference']['value']:.1f}.",
        "card-throughput": f"Column chart. {tp['title']}. {groups} {tp['ratio_text']}",
        "card-tested": f"Tested against: {chips}."
                       + ("" if tested["s3_matrix"]["available"] else f" {tested['s3_matrix']['placeholder']}."),
    }
    for name, text in out.items():
        if any(char in text for char in '"<>&'):
            raise Refused(f"alt text for {name} holds a character an HTML attribute cannot carry as typed: {text}")
    return out


def to_json(facts: dict[str, Any]) -> str:
    return json.dumps(facts, indent=2, ensure_ascii=False) + "\n"


def write(path: str, repo: str = REPO) -> str:
    """Write the facts as JSON to `path` (relative to `repo`); byte-identical for the same inputs."""
    out = os.path.join(repo, path)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(to_json(site_facts(repo)))
    return out


def main(argv: list[str] | None = None) -> int:
    del argv
    try:
        sys.stdout.write(to_json(site_facts()))
    except (readme_charts.Refused, schema.ReportRefused) as exc:
        print(f"site_facts refused: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
