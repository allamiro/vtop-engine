"""The independent checker (#744 rule 16): what is in the bucket, judged against the seeded ids.

It reads a snapshot of one tool's prefix (every object's bytes and
LastModified, plus the multipart uploads left incomplete) and the set of ids
the seed wrote, and counts:

- `gaps`: expected ids in no object;
- `duplicates`: extra copies (an id present three times is two duplicates);
- `torn_objects`: data objects that cannot be decompressed, or hold a line
  that carries no record id or more than one (a partial or merged line);
- `orphan_objects`: temporary keys a tool left behind (the kind names its
  patterns, e.g. Connect's `.temp-upload/`) and incomplete multipart uploads;
- `unexpected`: ids that are not in the seeded set (a flipped digit, or
  another run's records).

It never reads a tool's own state or metadata to decide any of these: the
only tool-specific knowledge is which keys are metadata (VTOP's manifests and
receipts) and which are temporary, both passed in by the kind.

Every record carries an id token `vtb-<run tag>-<sequence>` (`make_id`),
whatever the format around it: a JSON value, a JSON string that holds JSON (a
tool that wraps the value), or a syslog line. The checker finds the token in
the raw line bytes, so it needs no parser per tool.
"""
from __future__ import annotations

import gzip
import json
import os
import re
import zlib
from collections import Counter
from collections.abc import Callable, Iterable
from typing import Any, NamedTuple

ID_TOKEN = re.compile(rb"vtb-([0-9a-z]{4,16})-([0-9]{10})(?![0-9])")
GZIP_MAGIC = b"\x1f\x8b"


def make_id(run_tag: str, seq: int) -> str:
    if not re.fullmatch(r"[0-9a-z]{4,16}", run_tag):
        raise ValueError(f"run tag {run_tag!r} must be 4-16 lowercase letters or digits")
    if not 0 <= seq < 10**10:
        raise ValueError(f"sequence {seq} is out of range")
    return f"vtb-{run_tag}-{seq:010d}"


class Snapshot(NamedTuple):
    """One prefix of one bucket: {key: bytes}, {key: LastModified epoch seconds}, incomplete uploads."""

    objects: dict[str, bytes]
    last_modified: dict[str, float]
    incomplete_uploads: list[str]


class Result(NamedTuple):
    expected: int
    present_unique: int
    gaps: int
    duplicates: int
    torn_objects: int
    orphan_objects: int
    unexpected: int
    data_objects: int
    metadata_objects: int
    data_bytes: int
    gap_sample: list[int]
    torn_keys: list[str]
    orphan_keys: list[str]
    # seq -> the earliest LastModified of an object holding it (for time to store-visible).
    first_visible: dict[int, float]
    object_sizes: list[int]

    def counts(self) -> dict[str, int]:
        return {name: getattr(self, name) for name in
                ("expected", "present_unique", "gaps", "duplicates", "torn_objects", "orphan_objects",
                 "unexpected", "data_objects", "metadata_objects")}


def decode(body: bytes) -> bytes | None:
    """The object's text: gunzipped (every member) when it is gzip, else as stored. None when torn."""
    if body[:2] == GZIP_MAGIC:
        try:
            return gzip.decompress(body)
        except (OSError, EOFError, zlib.error):
            return None
    return body


def ids_in(text: bytes, run_tag: str) -> tuple[list[int], list[int], bool]:
    """(this run's sequences, other ids' sequences, torn) from an object's decoded text.

    Torn: a non-empty line with no id token, or with more than one."""
    ours, foreign, torn = [], [], False
    tag = run_tag.encode()
    for line in text.split(b"\n"):
        if not line.strip():
            continue
        found = ID_TOKEN.findall(line)
        if len(found) != 1:
            torn = True
        for found_tag, seq in found:
            (ours if found_tag == tag else foreign).append(int(seq))
    return ours, foreign, torn


def check(snapshot: Snapshot, expected: Iterable[int], run_tag: str, *,
          is_metadata: Callable[[str], bool] = lambda key: False,
          is_orphan: Callable[[str], bool] = lambda key: False) -> Result:
    expected_set = set(expected)
    seen: Counter[int] = Counter()
    first_visible: dict[int, float] = {}
    unexpected = 0
    torn_keys, orphan_keys = [], []
    data_objects = metadata_objects = data_bytes = 0
    sizes = []
    for key in sorted(snapshot.objects):
        body = snapshot.objects[key]
        if is_orphan(key):
            orphan_keys.append(key)
            continue
        if is_metadata(key):
            metadata_objects += 1
            continue
        data_objects += 1
        data_bytes += len(body)
        sizes.append(len(body))
        text = decode(body)
        if text is None:
            torn_keys.append(key)
            continue
        ours, foreign, torn = ids_in(text, run_tag)
        if torn:
            torn_keys.append(key)
        modified = snapshot.last_modified.get(key)
        for seq in ours:
            if seq in expected_set:
                seen[seq] += 1
                if modified is not None and (seq not in first_visible or modified < first_visible[seq]):
                    first_visible[seq] = modified
            else:
                unexpected += 1
        unexpected += len(foreign)
    orphan_keys.extend(f"incomplete-upload:{name}" for name in sorted(snapshot.incomplete_uploads))
    gaps = sorted(expected_set - set(seen))
    return Result(
        expected=len(expected_set),
        present_unique=len(seen),
        gaps=len(gaps),
        duplicates=sum(count - 1 for count in seen.values()),
        torn_objects=len(torn_keys),
        orphan_objects=len(orphan_keys),
        unexpected=unexpected,
        data_objects=data_objects,
        metadata_objects=metadata_objects,
        data_bytes=data_bytes,
        gap_sample=gaps[:20],
        torn_keys=torn_keys,
        orphan_keys=orphan_keys,
        first_visible=first_visible,
        object_sizes=sizes,
    )


# --------------------------------------------------------------- snapshots


def parse_time(text: str) -> float:
    """An RFC 3339 / ISO 8601 UTC time (as `mc ls --json` prints LastModified) as epoch seconds, ms kept."""
    import datetime as dt

    value = text.strip().replace("Z", "+00:00")
    # Python < 3.11 takes at most 6 fractional digits; Go prints up to 9.
    match = re.match(r"^(.*T\d\d:\d\d:\d\d)(\.\d+)?(.*)$", value)
    if not match:
        raise ValueError(f"not a timestamp: {text!r}")
    head, frac, tail = match.groups()
    frac = (frac or "")[:7]
    return dt.datetime.fromisoformat(head + frac + (tail or "+00:00")).timestamp()


def snapshot_from_dir(root: str, listing: Iterable[dict[str, Any]] = (),
                      incomplete: Iterable[str] = ()) -> Snapshot:
    """A snapshot from a local mirror of the prefix (`mc mirror`), with LastModified from `mc ls --json` lines."""
    objects = {}
    for directory, _dirs, files in os.walk(root):
        for name in files:
            path = os.path.join(directory, name)
            key = os.path.relpath(path, root).replace(os.sep, "/")
            with open(path, "rb") as handle:
                objects[key] = handle.read()
    modified = {}
    for entry in listing:
        if entry.get("type") == "file" and entry.get("key") and entry.get("lastModified"):
            modified[entry["key"]] = parse_time(entry["lastModified"])
    return Snapshot(objects, modified, list(incomplete))


def read_json_lines(path: str) -> list[dict[str, Any]]:
    out = []
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out
