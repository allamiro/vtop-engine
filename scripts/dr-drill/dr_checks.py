#!/usr/bin/env python3
"""Checks the disaster-recovery drills (#735) make from outside the engine.

Each subcommand reads something the store holds or the drill wrote and prints
one JSON object, or fails with a message that names what it found.

  records <dir> <prefix> <expected>
      <dir> is a copy of a bucket prefix. Every record the drill produced is
      numbered ("<prefix>NNNNNN ..."), so the data objects (gzip or plain, the
      manifests and receipts hold no record text) say exactly which of the
      numbers 0 .. expected-1 the archive holds, which it lacks and which it
      holds more than once.

  records-of <dir> <prefix> <expected> <keys file> <key prefix>
      The same, for the objects the keys file names alone (the committed ones:
      the objects a failed attempt left hold records too). Each key has the
      key prefix, the prefix <dir> was copied from, stripped.

  result <tsv> <out> <drill>
      Turn the drill's key/value lines into the JSON result file.
"""
import gzip
import json
import os
import re
import zlib
import sys


def read_object(path):
    with open(path, "rb") as f:
        raw = f.read()
    if raw[:2] == b"\x1f\x8b":
        try:
            raw = gzip.decompress(raw)
        except (OSError, EOFError, zlib.error):
            # A truncated or corrupt object holds no readable record.
            return b""
    return raw


def count_records(root, prefix, expected, only=None):
    """With `only`, a set of paths relative to root: count those objects alone."""
    pattern = re.compile(re.escape(prefix).encode() + rb"(\d+)")
    seen = {}
    data_objects = 0
    for dirpath, _dirs, files in os.walk(root):
        for name in sorted(files):
            full = os.path.join(dirpath, name)
            if only is not None and os.path.relpath(full, root) not in only:
                continue
            body = read_object(full)
            ids = [int(m.group(1)) for m in pattern.finditer(body)]
            if not ids:
                continue
            data_objects += 1
            for i in ids:
                seen[i] = seen.get(i, 0) + 1
    wanted = range(expected)
    missing = [i for i in wanted if i not in seen]
    found = sum(seen.values())
    unique = sum(1 for i in wanted if i in seen)
    return {
        "expected": expected,
        "found": found,
        "unique": unique,
        "missing": len(missing),
        "missing_first": ",".join(str(i) for i in missing[:5]) or "-",
        "duplicates": sum(c - 1 for i, c in seen.items() if c > 1),
        "outside_expected": sum(1 for i in seen if i >= expected),
        "data_objects": data_objects,
    }


def write_result(tsv, out, drill):
    doc = {"drill": drill}
    with open(tsv) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            key, _, value = line.partition("\t")
            if value in ("true", "false"):
                doc[key] = value == "true"
                continue
            try:
                doc[key] = int(value)
            except ValueError:
                doc[key] = value
    with open(out, "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")


def main(argv):
    if len(argv) == 5 and argv[1] == "records":
        print(json.dumps(count_records(argv[2], argv[3], int(argv[4]))))
        return 0
    if len(argv) == 7 and argv[1] == "records-of":
        with open(argv[5]) as f:
            keys = {line.strip() for line in f if line.strip()}
        strip = argv[6]
        only = {k[len(strip):] if k.startswith(strip) else k for k in keys}
        print(json.dumps(count_records(argv[2], argv[3], int(argv[4]), only)))
        return 0
    if len(argv) == 5 and argv[1] == "result":
        write_result(argv[2], argv[3], argv[4])
        return 0
    sys.stderr.write(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
