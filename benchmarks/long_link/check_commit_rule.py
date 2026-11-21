"""Check the commit rule at one instant: the consumer group's committed
offset on each partition must not pass the ranges the ledger records as
VERIFIED or SOURCE_COMMITTED (read from a copy of the SQLite ledger).

Usage: python3 check_commit_rule.py RUN_DIR GROUP     exit 1 on a violation
"""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile

KAFKA = "vtop-longlink-kafka"


def main() -> int:
    run, group = sys.argv[1], sys.argv[2]
    # The group's offsets FIRST, then the ledger: the engine writes VERIFIED
    # before it commits to Kafka, so a ledger read after the offsets is at
    # least as new as they are. In the other order, an engine still running
    # commits between the two reads and the check reports a false violation.
    described = subprocess.run(
        ["docker", "exec", KAFKA, "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092",
         "--describe", "--group", group], capture_output=True, text=True, check=False).stdout
    tmp = tempfile.mkdtemp()
    for suffix in ("", "-wal", "-shm"):
        src = os.path.join(run, "state", "vtop.db" + suffix)
        if os.path.exists(src):
            shutil.copy(src, os.path.join(tmp, "vtop.db" + suffix))
    rows = sqlite3.connect(os.path.join(tmp, "vtop.db")).execute(
        "select state, progress_start_json, progress_end_json from batches").fetchall()
    committed = {}
    for line in described.splitlines():
        f = line.split()
        if len(f) >= 4 and f[0] == group and f[2].isdigit() and f[3].isdigit():
            committed[int(f[2])] = int(f[3])
    cover: dict = {}
    states: dict = {}
    for state, start, end in rows:
        start, end = json.loads(start), json.loads(end)
        states[state] = states.get(state, 0) + 1
        if state in ("verified", "source_committed"):
            cover.setdefault(start["partition"], []).append((start["start_offset"], end["end_offset"]))
    bad = []
    for partition, offset in committed.items():
        reach = 0
        for first, last in sorted(cover.get(partition, [])):
            if first > reach:
                break
            reach = max(reach, last + 1)
        if offset > reach:
            bad.append(f"partition {partition}: the group's offset {offset} passes the verified or committed "
                       f"ranges, which end at {reach}")
    verdict = "commit rule held" if not bad else "VIOLATION: " + "; ".join(bad)
    print(f"group offsets {committed}, ledger states {states}: {verdict}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
