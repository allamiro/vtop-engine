#!/usr/bin/env bash
# The ledger scenario every ledger and digest cell of the compatibility matrix
# runs with a RELEASED vtopctl (#743), and that regenerate-fixtures.sh runs to
# write a fixture. One input file, one localfs archive, one SQLite ledger,
# in the order a real engine meets them:
#
#   1. three lines, a good store: a batch commits (a digest candidate);
#   2. two lines, a store path through a regular file (every upload fails,
#      ENOTDIR): a REPLAY_REQUIRED row;
#   3. two more, still failing: a second REPLAY_REQUIRED row, read to the end
#      of the grown file;
#   4. two more, a good store: the replay and the new bytes commit;
#   5. prune-ledger digests the oldest committed row and deletes it (the
#      newest committed row of the source always survives);
#   6. two more, failing again: a REPLAY_REQUIRED row is left in the ledger.
#
# So the ledger ends with committed rows, replay-required rows, a digest of a
# pruned row, and objects and manifests in the archive.
#
# Usage: ledger-scenario.sh <vtopctl> <dir>
# With LEDGER_SCENARIO_POSTGRES=1 the ledger is a PostgreSQL database instead:
# VTOP_STATE_STORE names it (the configs read it with `state_store: {env:
# VTOP_STATE_STORE}`), `migrate` runs first, and the digests go to <dir>/
# ledger-digests. The binary must have been built with the `postgres` feature.
# <dir> must not exist. It gets state.db (checkpointed, no WAL), app.log,
# store/ (the archive), work/, ledger-digests/ (beside state.db, as the
# engine's default) and good.yaml / bad.yaml (the two configs). Every path in
# the ledger is under <dir>, so a cell that runs another binary over the same
# <dir> sees the same sources.
set -euo pipefail

[[ $# -eq 2 ]] || { echo "usage: ledger-scenario.sh <vtopctl> <dir>" >&2; exit 2; }
B="$1"
D="$2"
[[ -x "$B" ]] || { echo "$B is not executable" >&2; exit 2; }
[[ ! -e "$D" ]] || { echo "$D exists" >&2; exit 2; }
mkdir -p "$D/store" "$D/work"
D="$(cd "$D" && pwd)"
: > "$D/notadir"
: > "$D/app.log"

if [[ -n "${LEDGER_SCENARIO_POSTGRES:-}" ]]; then
  [[ -n "${VTOP_STATE_STORE:-}" ]] || { echo "LEDGER_SCENARIO_POSTGRES needs VTOP_STATE_STORE" >&2; exit 2; }
  STATE_STORE='{env: VTOP_STATE_STORE}'
  DIGESTS=(--digest-dir "$D/ledger-digests")
else
  STATE_STORE="\"sqlite://$D/state.db\""
  DIGESTS=()
fi

write_cfg() {
  cat > "$D/$1" <<YAML
engine:
  name: compat-ledger
  state_store: $STATE_STORE
  work_dir: $D/work
batching: {}
compression: {type: gzip, level: 6}
checksum: {algorithm: sha256}
sources:
  file:
    enabled: true
    paths: ["$D/app.log"]
upload:
  backend: localfs
  bucket: telemetry-data
  local_path: $2
partitioning: {}
YAML
}
write_cfg good.yaml "$D/store"
write_cfg bad.yaml "$D/notadir/store"

lines() { # <label> <count>
  local i
  for ((i = 1; i <= $2; i++)); do echo "compat $1 $i" >> "$D/app.log"; done
}
run() { # <config> <expect: ok|fail>
  local rc=0
  "$B" process-once --source file --config "$D/$1" > "$D/last.log" 2>&1 || rc=$?
  if [[ "$2" == ok && $rc -ne 0 ]]; then
    echo "process-once with $1 failed ($rc), expected success:" >&2
    cat "$D/last.log" >&2
    exit 1
  fi
}

if [[ -n "${LEDGER_SCENARIO_POSTGRES:-}" ]]; then
  "$B" migrate --config "$D/good.yaml" > "$D/migrate.log" 2>&1 \
    || { cat "$D/migrate.log" >&2; exit 1; }
fi
lines first 3
run good.yaml ok
lines second 2
run bad.yaml fail
lines third 2
run bad.yaml fail
lines fourth 2
run good.yaml ok
"$B" prune-ledger --config "$D/good.yaml" ${DIGESTS[@]+"${DIGESTS[@]}"} --older-than-days 0 --limit 1 > "$D/prune.log" 2>&1 \
  || { cat "$D/prune.log" >&2; exit 1; }
lines fifth 2
run bad.yaml fail
rm -f "$D/last.log"

# One file, no -wal or -shm beside it: copy-able as a fixture.
if [[ -z "${LEDGER_SCENARIO_POSTGRES:-}" ]] && command -v sqlite3 > /dev/null 2>&1; then
  sqlite3 "$D/state.db" 'PRAGMA wal_checkpoint(TRUNCATE);' > /dev/null
fi
