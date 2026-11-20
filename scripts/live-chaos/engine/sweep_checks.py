#!/usr/bin/env python3
"""Checks the engine crash sweep (#725) makes from outside the engine.

Each subcommand reads something the engine wrote, or the broker or the store
holds, and prints one JSON object or fails with a message that names what it
saw. None of them asks the engine what it did: the ledger is opened read-only
with Python's own SQLite (a read-write connection would checkpoint the WAL a
`kill -9` left behind when it closes, and the restart would not see what the
crash left), and archived records are counted by decompressing the objects
the store holds.

  points STDERR_FILE
      The failpoint list a `failpoints` build printed when refusing an
      unknown point: one name per line, indented two spaces.
  ledger DB TOPIC PARTITION GROUP
      The ledger's rows for one Kafka partition and consumer group: state
      counts, the highest end offset of a VERIFIED or SOURCE_COMMITTED row,
      the highest end offset of a SOURCE_COMMITTED row, the rows in an
      in-flight state, and the SOURCE_COMMITTED rows that carry no evidence
      of having been VERIFIED (no verification class, which only the VERIFIED
      write sets, or no object or manifest).
  group-offset GROUP TOPIC PARTITION < DESCRIBE_OUTPUT
      The group's committed offset for one partition, read from what
      `kafka-consumer-groups.sh --describe --group GROUP` printed (stdin):
      the number, or `-` only when the broker answered that the group has
      none (the offsets table printed without a committed offset for the
      partition, or the group not found). Anything else (a timeout, a
      coordinator error, no output) exits 1 naming what was printed: the
      tool exits 0 even when the query failed, and an unread offset is not
      an absent one.
  commit-rule COMMITTED LEDGER_JSON POINT WHEN
      The commit rule at one instant, from both sides. At the source: the
      group's committed offset on the broker (`-` for none, as group-offset
      established it) never passes the
      end of the highest VERIFIED range, i.e. committed <= verified_end + 1
      (Kafka commits the NEXT offset to read). In the ledger: no
      SOURCE_COMMITTED row lacks the VERIFIED write's evidence. Exits 1
      naming the numbers or the rows when either fails.
  readback DIR RECORDS PREFIX
      Every record id PREFIX%06d for 0 <= i < RECORDS found in the objects
      under DIR, counted: missing ids, duplicated ids and the extra copies.
  complete LISTED_FILE RAN_FILE
      Every listed point ran (F3): exits 1 naming the ones that did not.
"""

from __future__ import annotations

import gzip
import json
import os
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path

POINT_NAME = re.compile(r"^[a-z_]+$")
# Rows whose batch has not reached a terminal or handed-back state: the
# engine was still working on them when it stopped.
IN_FLIGHT = {
    "BATCHING",
    "SEALED",
    "COMPRESSED",
    "CHECKSUMMED",
    "OBJECT_UPLOADED",
    "MANIFEST_UPLOADED",
    "VERIFIED",
}
VERIFIED_OR_LATER = {"VERIFIED", "SOURCE_COMMITTED"}


def parse_points(text: str) -> list[str]:
    """The first block of indented names: the CLI can print the error twice."""
    points: list[str] = []
    for line in text.splitlines():
        if line.startswith("  ") and POINT_NAME.match(line.strip()):
            points.append(line.strip())
        elif points:
            break
    return points


def ledger_summary(db: str, topic: str, partition: int, group: str) -> dict:
    summary = {
        "rows": 0,
        "states": {},
        "verified_end": None,
        "committed_end": None,
        "in_flight": [],
        "committed_unverified": [],
    }
    if not os.path.exists(db):
        return summary
    uri = Path(db).resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        rows = conn.execute(
            "SELECT batch_id, state, progress_end_json, verification_class, "
            "object_uri, manifest_uri FROM batches "
            "WHERE source_type = 'kafka' ORDER BY created_at, batch_id"
        ).fetchall()
    except sqlite3.OperationalError as err:
        # The kill can land before the schema exists (before the first open
        # finished): no table is no rows.
        if "no such table" in str(err):
            return summary
        raise
    finally:
        conn.close()
    states: Counter = Counter()
    for batch_id, state, end_json, verification_class, object_uri, manifest_uri in rows:
        # The ledger stores states in lower case.
        state = state.upper()
        marker = json.loads(end_json)
        if (
            marker.get("topic") != topic
            or int(marker.get("partition", -1)) != partition
            or marker.get("consumer_group") != group
        ):
            continue
        summary["rows"] += 1
        states[state] += 1
        offset = int(marker["end_offset"])
        if state in VERIFIED_OR_LATER:
            summary["verified_end"] = max(summary["verified_end"] or -1, offset)
        if state == "SOURCE_COMMITTED":
            summary["committed_end"] = max(summary["committed_end"] or -1, offset)
            # Only the VERIFIED write sets the class (mark_verified), so a
            # committed row without one reached SOURCE_COMMITTED without it.
            lacking = [
                name
                for name, value in (
                    ("verification_class", verification_class),
                    ("object_uri", object_uri),
                    ("manifest_uri", manifest_uri),
                )
                if not value
            ]
            if lacking:
                summary["committed_unverified"].append(
                    {"batch_id": batch_id, "end_offset": offset, "lacking": lacking}
                )
        if state in IN_FLIGHT:
            summary["in_flight"].append({"batch_id": batch_id, "state": state})
    summary["states"] = dict(sorted(states.items()))
    return summary


# The header of the offsets table `kafka-consumer-groups.sh --describe`
# prints once the broker has answered for the group.
DESCRIBE_HEADER = re.compile(r"^GROUP\s+TOPIC\s+PARTITION\s+CURRENT-OFFSET\b")
OFFSET = re.compile(r"^(?:0|[1-9][0-9]*)$")


def group_offset(text: str, group: str, topic: str, partition: int) -> str:
    """The committed offset `--describe` reports, or `-` for an established none.

    Raises ValueError when the output does not show the broker's answer. The
    tool prints its errors (a coordinator timeout, a broker it cannot reach)
    and still exits 0, so the absence of a row proves nothing by itself: an
    absence counts only when the offsets table was printed, or when the
    broker said the group does not exist.
    """
    header_seen = False
    found: list[str] = []
    for line in text.splitlines():
        if DESCRIBE_HEADER.match(line):
            header_seen = True
            continue
        fields = line.split()
        if (
            header_seen
            and len(fields) >= 4
            and fields[0] == group
            and fields[1] == topic
            and fields[2] == str(partition)
        ):
            found.append(fields[3])
    if len(found) > 1:
        raise ValueError(
            f"the describe output lists {topic}/{partition} for group {group} "
            f"{len(found)} times ({', '.join(found)}); expected once"
        )
    if found:
        value = found[0]
        if value == "-" or OFFSET.match(value):
            return value
        raise ValueError(
            f"the committed offset of group {group} on {topic}/{partition} is "
            f"'{value}', neither a number nor '-'"
        )
    if header_seen:
        # The broker answered for the group and it has no committed offset
        # for this partition.
        return "-"
    not_found = (
        f"GroupIdNotFoundException: Group {group} not found",
        f"Consumer group '{group}' does not exist",
    )
    if any(marker in text for marker in not_found):
        return "-"
    lines = [line for line in text.splitlines() if line.strip()]
    shown = " | ".join(lines[:3]) if lines else "(no output)"
    raise ValueError(
        f"could not read the committed offset of group {group}: the describe "
        f"output has no offsets table and does not say the group is absent: {shown}"
    )


def commit_rule(committed: str, ledger: dict, point: str, when: str) -> tuple[bool, str]:
    unverified = ledger.get("committed_unverified") or []
    if unverified:
        return False, (
            f"{point} ({when}): COMMIT RULE VIOLATED: {len(unverified)} ledger row(s) are "
            f"SOURCE_COMMITTED without the VERIFIED write's evidence: "
            f"{json.dumps(unverified)}. Ledger states: {json.dumps(ledger['states'])}"
        )
    if committed == "-":
        return True, f"{point} ({when}): the group has no committed offset"
    if not OFFSET.match(committed):
        # Only group-offset's answer is accepted: an empty or unparsed value
        # is an offset nobody read, not an absent one.
        return False, (
            f"{point} ({when}): the consumer group's committed offset was not read "
            f"(got '{committed}'), so the commit rule cannot be checked"
        )
    committed_at = int(committed)
    verified_end = ledger.get("verified_end")
    ceiling = 0 if verified_end is None else verified_end + 1
    if committed_at <= ceiling:
        return True, (
            f"{point} ({when}): committed offset {committed_at} <= {ceiling} "
            f"(highest VERIFIED range ends at {verified_end})"
        )
    verified = (
        "no range is recorded as VERIFIED or SOURCE_COMMITTED"
        if verified_end is None
        else f"the highest range recorded as VERIFIED or SOURCE_COMMITTED ends at {verified_end}"
    )
    return False, (
        f"{point} ({when}): COMMIT RULE VIOLATED: the consumer group's committed offset "
        f"is {committed_at}, but in the ledger {verified}, so offsets "
        f"{ceiling}..{committed_at - 1} were committed at the source before they were "
        f"verified. Ledger states: {json.dumps(ledger['states'])}"
    )


def readback(directory: str, records: int, prefix: str) -> dict:
    found: Counter = Counter()
    objects = 0
    for path in sorted(Path(directory).rglob("*")):
        if not path.is_file():
            continue
        name = path.name
        # Manifests and verification receipts are JSON about objects, not
        # archived records.
        if "manifest" in name or "receipt" in name:
            continue
        data = path.read_bytes()
        if name.endswith(".gz"):
            data = gzip.decompress(data)
        elif name.endswith(".zst"):
            raise SystemExit(f"{path}: zstd objects are not decoded here; run the sweep with gzip")
        objects += 1
        for line in data.decode("utf-8", errors="replace").splitlines():
            token = line.split(" ", 1)[0]
            if token.startswith(prefix):
                found[token] += 1
    expected = [f"{prefix}{i:06d}" for i in range(records)]
    missing = [r for r in expected if r not in found]
    duplicated = {r: n for r, n in found.items() if n > 1}
    unexpected = sorted(set(found) - set(expected))
    return {
        "objects": objects,
        "distinct": len(set(found) & set(expected)),
        "missing": missing,
        "duplicated_ids": len(duplicated),
        "extra_copies": sum(n - 1 for n in duplicated.values()),
        "unexpected": unexpected,
    }


def complete(listed: list[str], ran: list[str]) -> list[str]:
    return [p for p in listed if p not in set(ran)]


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    cmd, args = argv[1], argv[2:]
    if cmd == "points" and len(args) == 1:
        points = parse_points(Path(args[0]).read_text(errors="replace"))
        print("\n".join(points))
        return 0 if points else 1
    if cmd == "ledger" and len(args) == 4:
        print(json.dumps(ledger_summary(args[0], args[1], int(args[2]), args[3])))
        return 0
    if cmd == "group-offset" and len(args) == 3:
        try:
            print(group_offset(sys.stdin.read(), args[0], args[1], int(args[2])))
        except ValueError as err:
            print(err, file=sys.stderr)
            return 1
        return 0
    if cmd == "commit-rule" and len(args) == 4:
        ok, message = commit_rule(args[0], json.loads(args[1]), args[2], args[3])
        print(message, file=sys.stdout if ok else sys.stderr)
        return 0 if ok else 1
    if cmd == "readback" and len(args) == 3:
        print(json.dumps(readback(args[0], int(args[1]), args[2])))
        return 0
    if cmd == "complete" and len(args) == 2:
        listed = Path(args[0]).read_text().split()
        ran = Path(args[1]).read_text().split() if Path(args[1]).exists() else []
        missing = complete(listed, ran)
        if missing:
            print(
                "the sweep did not run every failpoint the binary lists; not run: "
                + ", ".join(missing),
                file=sys.stderr,
            )
            return 1
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
