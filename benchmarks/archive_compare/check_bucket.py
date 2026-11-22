#!/usr/bin/env python3
"""The archive checker (#746 "Checker"): what is in the bucket, judged against the seeded record ids.

    python3 benchmarks/archive_compare/check_bucket.py --objects <mirror dir> --tag <run tag> --expected <N> \\
        [--listing listing.jsonl] [--incomplete incomplete.jsonl]

It gunzips every data object, parses its JSON lines and compares the set of embedded ids with the seeded
expectation (`compare_common.checker`, which counts `gaps`, `duplicates`, `torn_objects`, `orphan_objects` and
`unexpected`). It never reads a tool's state. The only tool knowledge is which keys are not data:

- metadata: VTOP's manifest and receipt objects (`<batch>.manifest.json`, `<batch>.receipt.json`);
- orphans: the temporary keys a tool leaves in the bucket, here Kafka Connect's (Lenses) `.temp-upload/` objects
  (the staging step of its exactly-once commit) and `.indexes/` entries (its commit-chain bookkeeping), which the
  issue counts and lists by name, and every incomplete multipart upload (the checker adds those itself).

The whole bucket is read: a run has one tool and a fresh bucket, so a key outside the tool's own prefix is itself
a finding. The ids are `vtb-<run tag>-<sequence>` (`producer.make_id`), found in the raw line bytes, so a tool
that stores the value as it came (VTOP, Vector) and one that rewrites it as JSON (Connect) are read alike.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.realpath(__file__))
BENCH = os.path.dirname(HERE)
if BENCH not in sys.path:
    sys.path.insert(0, BENCH)

from archive_compare import producer  # noqa: E402
from compare_common import checker  # noqa: E402

ORPHAN_SEGMENTS = (".temp-upload", ".indexes")
METADATA_SUFFIXES = (".manifest.json", ".receipt.json")


def is_metadata(key: str) -> bool:
    """VTOP's manifest and receipt objects: written beside each batch, never records."""
    return key.endswith(METADATA_SUFFIXES)


def is_orphan(key: str) -> bool:
    """A temporary key a tool leaves behind: Connect's `.temp-upload/` and `.indexes/` entries."""
    return any(segment in ORPHAN_SEGMENTS for segment in key.split("/"))


def check(snapshot: checker.Snapshot, expected: int | range, tag: str) -> checker.Result:
    """Judge one bucket snapshot against the records 0 .. expected-1 (or the given range)."""
    ids = expected if isinstance(expected, range) else range(0, int(expected))
    return checker.check(snapshot, ids, tag, is_metadata=is_metadata, is_orphan=is_orphan)


def verify_records(snapshot: checker.Snapshot, seed: int, tag: str, size: int,
                   partitions: int = producer.PARTITIONS) -> tuple[int, int, list[int]]:
    """Every record read back against the seeded one: (byte-exact, equal as JSON only, sequences that differ).

    A tool that stores the Kafka value as it came (VTOP, Vector) must match byte for byte; one that parses and
    writes it again (Connect's JSON converter) may differ in bytes and not in content. A line whose content differs,
    or whose id is not a record of this run, is `wrong`."""
    import json

    exact = same_json = 0
    wrong: list[int] = []
    tagb = tag.encode()
    for key in sorted(snapshot.objects):
        if is_orphan(key) or is_metadata(key):
            continue
        text = checker.decode(snapshot.objects[key])
        if text is None:
            continue
        for raw in text.split(b"\n"):
            if not raw.strip():
                continue
            found = [seq for found_tag, seq in checker.ID_TOKEN.findall(raw) if found_tag == tagb]
            if len(found) != 1:
                continue  # a torn or foreign line: the checker counts it, not this
            seq = int(found[0])
            want = producer.record(seed, tag, seq, size, partitions)
            if raw == want:
                exact += 1
                continue
            try:
                equal = json.loads(raw) == json.loads(want)
            except ValueError:
                equal = False
            if equal:
                same_json += 1
            else:
                wrong.append(seq)
    return exact, same_json, wrong


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--objects", required=True, help="a local mirror of the bucket (mc mirror)")
    parser.add_argument("--tag", required=True, help="the run tag embedded in every id")
    parser.add_argument("--expected", type=int, required=True, help="records 0..N-1 were produced")
    parser.add_argument("--listing", default=None, help="mc ls --json --recursive, for LastModified")
    parser.add_argument("--incomplete", default=None, help="mc ls --json --recursive --incomplete")
    args = parser.parse_args(argv)
    listing = checker.read_json_lines(args.listing) if args.listing else []
    incomplete = [entry.get("key", "") for entry in checker.read_json_lines(args.incomplete)
                  if entry.get("key")] if args.incomplete else []
    result = check(checker.snapshot_from_dir(args.objects, listing, incomplete), args.expected, args.tag)
    print(json.dumps({**result.counts(), "gap_sample": result.gap_sample, "torn_keys": result.torn_keys,
                      "orphan_keys": result.orphan_keys}, indent=1))
    return 0 if not (result.gaps or result.duplicates or result.torn_objects or result.unexpected) else 1


if __name__ == "__main__":
    sys.exit(main())
