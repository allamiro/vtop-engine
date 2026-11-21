#!/usr/bin/env bash
# How state.db and app.log in this directory were made (#626): by the
# RELEASED v0.9.0 vtopctl, not by the code under test (ddia-encoding-evolution
# rule 4). Run inside ubuntu:24.04 with the release's
# vtop-engine-0.9.0-aarch64-unknown-linux-gnu directory mounted at /v090 and
# an output directory at /out (its SHA-256 checked against the release's
# SHA256SUMS). The ledger was then checkpointed (`PRAGMA
# wal_checkpoint(TRUNCATE)`), so the committed file holds every row.
#
# What it holds: a committed range [0, 48) and two REPLAY_REQUIRED rows from
# failed uploads, [48, 82) and, after the file grew and the replay read to its
# end, [48, 114): user_version 1, no sealed_at column. The test re-roots the
# markers' path and inode to where it writes app.log, nothing else.
set -euo pipefail
B=/v090/vtopctl
F=/vtop-fixture
mkdir -p $F/store $F/work
: > $F/notadir
write_cfg() {
cat > "$F/$1" <<YAML
engine:
  name: fixture-090
  state_store: "sqlite://$F/state.db"
  work_dir: $F/work
batching: {}
compression: {type: gzip, level: 6}
checksum: {algorithm: sha256}
sources:
  file:
    enabled: true
    paths: ["$F/app.log"]
upload:
  backend: localfs
  bucket: telemetry-data
  local_path: $2
partitioning: {}
YAML
}
write_cfg good.yaml $F/store
# A store path through a regular file: every upload fails (ENOTDIR).
write_cfg bad.yaml $F/notadir/store
for i in 1 2 3; do echo "fixture first $i" >> $F/app.log; done
$B process-once --source file --config $F/good.yaml > /out/run1.log 2>&1 || echo "run1 exit $?"
for i in 1 2; do echo "fixture second $i" >> $F/app.log; done
$B process-once --source file --config $F/bad.yaml > /out/run2.log 2>&1 || echo "run2 exit $?"
for i in 1 2; do echo "fixture third $i" >> $F/app.log; done
$B process-once --source file --config $F/bad.yaml > /out/run3.log 2>&1 || echo "run3 exit $?"
cp $F/state.db* $F/app.log /out/
