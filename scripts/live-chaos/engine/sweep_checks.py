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
  ledger (--sqlite DB | --rows-json) (kafka TOPIC PARTITION GROUP | file PATH |
          syslog_spool PATH)
      The ledger's rows for one source: a Kafka partition and consumer group,
      or one file or spool file path. The rows come from a SQLite ledger opened read-only
      (--sqlite), or from JSON on stdin (--rows-json), which is how the
      PostgreSQL ledger is read: the harness asks `psql` for the batches as
      a JSON array and this reads what it printed. Prints the state counts,
      the highest end position of a VERIFIED or SOURCE_COMMITTED row, the
      highest end position of a SOURCE_COMMITTED row, the rows in an
      in-flight state, the state of the newest live row, and the
      SOURCE_COMMITTED rows that carry no evidence of having been VERIFIED
      (no verification class, which only the VERIFIED write sets, or no
      object or manifest).
  kill-state POINT LEDGER_JSON
      The ledger at the instant a failpoint killed the engine: the newest
      live row is in the state the point sits beside (the table the PR for
      #725 pinned in crates/vtop-cli/tests/failpoints.rs), and no row exists
      before the first insert. For the source commit this is the check that
      holds for a file source, whose committed position is in the engine's
      memory and not on the source: the engine reached the commit with the
      batch VERIFIED, never before.
  group-offset GROUP TOPIC PARTITION < DESCRIBE_OUTPUT
      The group's committed offset for one partition, read from what
      `kafka-consumer-groups.sh --describe --group GROUP` printed (stdin):
      the number, or `-` only when the broker answered that the group has
      none (the offsets table printed without a committed offset for the
      partition, or the group not found). Anything else (a timeout, a
      coordinator error, no output) exits 1 naming what was printed: the
      tool exits 0 even when the query failed, and an unread offset is not
      an absent one.
  commit-rule COMMITTED LEDGER_JSON POINT WHEN [kafka|file|syslog_spool]
      The commit rule at one instant, from both sides. At the source: for
      Kafka, the group's committed offset on the broker (`-` for none, as
      group-offset established it) never passes the end of the highest
      VERIFIED range, i.e. committed <= verified_end + 1 (Kafka commits the
      NEXT offset to read). For a file or a spool file, whose committed
      position lives in the engine and is recorded only by the ledger, COMMITTED is the
      ledger's cursor (the end byte of the highest SOURCE_COMMITTED row, `-`
      for none) and may not pass the highest VERIFIED end byte. In the
      ledger: no SOURCE_COMMITTED row lacks the VERIFIED write's evidence.
      Exits 1 naming the numbers or the rows when either fails.
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


# The states a batch passes through on its way to the source commit. Rows in
# the others (FAILED, REPLAY_REQUIRED, SUPERSEDED) are history, not the batch
# the engine was working on.
LIVE = IN_FLIGHT | {"SOURCE_COMMITTED"}
ROW_COLUMNS = (
    "batch_id, state, progress_end_json, verification_class, object_uri, manifest_uri"
)

# What a row's state is at the instant each failpoint kills the engine, for
# the batch that hit it first: the state before the transition a `before_`
# point sits in front of,
# the state a `after_` point sits behind. `None`: no row yet. The same table
# crates/vtop-cli/tests/failpoints.rs pins against the in-repo build.
KILL_STATE = {
    "before_insert": None,
    "after_insert": "BATCHING",
    "before_ledger_sealed": "BATCHING",
    "after_ledger_sealed": "SEALED",
    "before_ledger_compressed": "SEALED",
    "after_ledger_compressed": "COMPRESSED",
    "before_ledger_checksummed": "COMPRESSED",
    "after_ledger_checksummed": "CHECKSUMMED",
    "before_ledger_object_uploaded": "CHECKSUMMED",
    "after_ledger_object_uploaded": "OBJECT_UPLOADED",
    "before_ledger_manifest_uploaded": "OBJECT_UPLOADED",
    "after_ledger_manifest_uploaded": "MANIFEST_UPLOADED",
    "before_ledger_verified": "MANIFEST_UPLOADED",
    "after_ledger_verified": "VERIFIED",
    "before_ledger_source_committed": "VERIFIED",
    "after_ledger_source_committed": "SOURCE_COMMITTED",
    "mid_object_put": "CHECKSUMMED",
    "after_object_put_before_ledger": "CHECKSUMMED",
    "after_manifest_put_before_ledger": "OBJECT_UPLOADED",
    "before_source_commit": "VERIFIED",
    "after_source_commit_before_ledger": "VERIFIED",
}


class Source:
    """The part of the ledger one sweep reads: a Kafka partition or a file."""

    def __init__(self, kind: str, *fields: str):
        if kind == "kafka" and len(fields) == 3:
            self.topic, self.partition, self.group = fields[0], int(fields[1]), fields[2]
            self.end_key = "end_offset"
        elif kind in ("file", "syslog_spool") and len(fields) == 1:
            self.path = fields[0]
            self.end_key = "end_byte"
        else:
            raise ValueError(
                "a source is `kafka TOPIC PARTITION GROUP`, `file PATH` or "
                f"`syslog_spool PATH`, got {kind} {fields}"
            )
        self.kind = kind

    def owns(self, marker: dict) -> bool:
        if marker.get("source_type") not in (None, self.kind):
            return False
        if self.kind == "kafka":
            return (
                marker.get("topic") == self.topic
                and int(marker.get("partition", -1)) == self.partition
                and marker.get("consumer_group") == self.group
            )
        return marker.get("path") == self.path


def sqlite_rows(db: str, source_type: str) -> list[dict]:
    """The ledger's batches, oldest first, read without a write connection."""
    if not os.path.exists(db):
        return []
    uri = Path(db).resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        fetched = conn.execute(
            f"SELECT {ROW_COLUMNS} FROM batches WHERE source_type = ? "
            "ORDER BY created_at, batch_id",
            (source_type,),
        ).fetchall()
    except sqlite3.OperationalError as err:
        # The kill can land before the schema exists (before the first open
        # finished): no table is no rows.
        if "no such table" in str(err):
            return []
        raise
    finally:
        conn.close()
    names = [c.strip() for c in ROW_COLUMNS.split(",")]
    return [dict(zip(names, row)) for row in fetched]


def summarize(rows: list[dict], source: Source) -> dict:
    summary = {
        "rows": 0,
        "states": {},
        "verified_end": None,
        "committed_end": None,
        "in_flight": [],
        "newest_state": None,
        "committed_unverified": [],
    }
    states: Counter = Counter()
    newest_end = None
    for row in rows:
        # The ledger stores states in lower case.
        state = row["state"].upper()
        marker = json.loads(row["progress_end_json"])
        if not source.owns(marker):
            continue
        summary["rows"] += 1
        states[state] += 1
        position = int(marker[source.end_key])
        if state in VERIFIED_OR_LATER:
            summary["verified_end"] = max(summary["verified_end"] or -1, position)
        if state == "SOURCE_COMMITTED":
            summary["committed_end"] = max(summary["committed_end"] or -1, position)
            # Only the VERIFIED write sets the class (mark_verified), so a
            # committed row without one reached SOURCE_COMMITTED without it.
            lacking = [
                name
                for name, value in (
                    ("verification_class", row["verification_class"]),
                    ("object_uri", row["object_uri"]),
                    ("manifest_uri", row["manifest_uri"]),
                )
                if not value
            ]
            if lacking:
                summary["committed_unverified"].append(
                    {"batch_id": row["batch_id"], source.end_key: position, "lacking": lacking}
                )
        if state in IN_FLIGHT:
            summary["in_flight"].append({"batch_id": row["batch_id"], "state": state})
        if state in LIVE and (newest_end is None or position >= newest_end):
            newest_end = position
            summary["newest_state"] = state
    summary["states"] = dict(sorted(states.items()))
    return summary


def ledger_summary(db: str, topic: str, partition: int, group: str) -> dict:
    """The SQLite ledger's rows for one Kafka partition and consumer group."""
    return summarize(sqlite_rows(db, "kafka"), Source("kafka", topic, str(partition), group))


def kill_state(point: str, ledger: dict) -> tuple[bool, str]:
    if point not in KILL_STATE:
        return False, (
            f"{point}: the sweep has no expected ledger state for this failpoint; add it to "
            f"KILL_STATE in sweep_checks.py and to the table in tests/failpoints.rs"
        )
    expected = KILL_STATE[point]
    seen = ledger.get("newest_state")
    if seen == expected:
        return True, f"{point}: the newest live ledger row is {expected or 'absent'}, as it must be"
    # Batches stay in flight across cycles (docs/ARCHITECTURE.md section 5.2),
    # so the batch that hit the point is not always the newest: a window read
    # after it can already be uploading, or VERIFIED and waiting for it to
    # commit. The point is where it says when SOME row is in the state it
    # sits beside. Whether a commit ran ahead of its batch's VERIFIED is the
    # commit-rule check's to answer, from the group's offset and the ledger.
    if expected is not None and ledger.get("states", {}).get(expected, 0) > 0:
        return True, (
            f"{point}: a ledger row is {expected}, as it must be; the newest is {seen}, a batch "
            f"read after the one at the point"
        )
    what = "no live row" if seen is None else f"a {seen} row"
    want = "no row yet" if expected is None else f"a {expected} row"
    return False, (
        f"{point} (at the kill): the ledger holds {what}, but a kill at this point must find "
        f"{want}: the engine reached the point out of order. Ledger states: "
        f"{json.dumps(ledger.get('states', {}))}"
    )


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


def commit_rule(
    committed: str, ledger: dict, point: str, when: str, kind: str = "kafka"
) -> tuple[bool, str]:
    unverified = ledger.get("committed_unverified") or []
    if unverified:
        return False, (
            f"{point} ({when}): COMMIT RULE VIOLATED: {len(unverified)} ledger row(s) are "
            f"SOURCE_COMMITTED without the VERIFIED write's evidence: "
            f"{json.dumps(unverified)}. Ledger states: {json.dumps(ledger['states'])}"
        )
    holder = {
        "kafka": "the consumer group",
        "syslog_spool": "the spool source's ledger cursor",
    }.get(kind, "the file source's ledger cursor")
    if committed == "-":
        return True, f"{point} ({when}): {holder} has no committed position"
    if not OFFSET.match(committed):
        # Only group-offset's answer is accepted: an empty or unparsed value
        # is an offset nobody read, not an absent one.
        return False, (
            f"{point} ({when}): {holder}'s committed position was not read "
            f"(got '{committed}'), so the commit rule cannot be checked"
        )
    committed_at = int(committed)
    verified_end = ledger.get("verified_end")
    # Kafka commits the NEXT offset to read; a file's end byte is already
    # the position after the last byte read.
    ahead = 1 if kind == "kafka" else 0
    ceiling = 0 if verified_end is None else verified_end + ahead
    if committed_at <= ceiling:
        return True, (
            f"{point} ({when}): committed position {committed_at} <= {ceiling} "
            f"(highest VERIFIED range ends at {verified_end})"
        )
    verified = (
        "no range is recorded as VERIFIED or SOURCE_COMMITTED"
        if verified_end is None
        else f"the highest range recorded as VERIFIED or SOURCE_COMMITTED ends at {verified_end}"
    )
    return False, (
        f"{point} ({when}): COMMIT RULE VIOLATED: {holder}'s committed position "
        f"is {committed_at}, but in the ledger {verified}, so positions "
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
        # archived records. By suffix: an object's name carries the source's
        # path, which can say "manifest" or "receipt" itself, and an object
        # skipped for that would be counted as a hole.
        if name.endswith((".manifest.json", ".receipt.json")):
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
    if cmd == "ledger" and len(args) >= 3 and args[0] in ("--sqlite", "--rows-json"):
        try:
            if args[0] == "--sqlite":
                db, kind, fields = args[1], args[2], args[3:]
                source = Source(kind, *fields)
                rows = sqlite_rows(db, source.kind)
            else:
                kind, fields = args[1], args[2:]
                source = Source(kind, *fields)
                rows = json.loads(sys.stdin.read() or "[]")
        except ValueError as err:
            print(err, file=sys.stderr)
            return 2
        print(json.dumps(summarize(rows, source)))
        return 0
    if cmd == "kill-state" and len(args) == 2:
        ok, message = kill_state(args[0], json.loads(args[1]))
        print(message, file=sys.stdout if ok else sys.stderr)
        return 0 if ok else 1
    if cmd == "group-offset" and len(args) == 3:
        try:
            print(group_offset(sys.stdin.read(), args[0], args[1], int(args[2])))
        except ValueError as err:
            print(err, file=sys.stderr)
            return 1
        return 0
    if cmd == "commit-rule" and len(args) in (4, 5):
        ok, message = commit_rule(
            args[0], json.loads(args[1]), args[2], args[3], *(args[4:] or ["kafka"])
        )
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
