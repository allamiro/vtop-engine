#!/usr/bin/env bash
# Helpers the ledger cells of the compatibility matrix share (#743). Sourced.
# The caller sets CELL, DIRECTION and TAG (for messages) and WORKDIR.

log() { echo "[compat/$CELL/$DIRECTION/$TAG] $*" >&2; }
fail() { echo "[compat/$CELL/$DIRECTION/$TAG] FAIL: $*" >&2; exit 1; }

# run <name> <command...>: run a command, keep its output in
# $WORKDIR/logs/<name>.log, and set RC to its exit status without stopping.
run() {
  local name="$1"
  shift
  mkdir -p "$WORKDIR/logs"
  RC=0
  "$@" > "$WORKDIR/logs/$name.log" 2>&1 || RC=$?
  log "$name: exit $RC"
}

# must_pass <name> <command...>: run, and fail with the output unless it
# exited 0.
must_pass() {
  run "$@"
  if [[ "$RC" -ne 0 ]]; then
    sed 's/^/    /' "$WORKDIR/logs/$1.log" >&2
    fail "$1 exited $RC, expected 0"
  fi
}

# has_command <vtopctl> <subcommand>: whether that release has the command.
has_command() {
  "$1" "$2" --help > /dev/null 2>&1
}

# ledger_digest <state.db>: a hash of the ledger's logical content (every
# table's rows and the schema version), read through SQLite so that rows
# still in the write-ahead log count. Two equal digests mean nothing changed.
ledger_digest() {
  python3 -I - "$1" << 'PY'
import hashlib, sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
h = hashlib.sha256()
h.update(str(con.execute("PRAGMA user_version").fetchone()[0]).encode())
for line in con.iterdump():
    h.update(line.encode())
    h.update(b"\n")
print(h.hexdigest())
PY
}

user_version() {
  python3 -I - "$1" << 'PY'
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
print(con.execute("PRAGMA user_version").fetchone()[0])
PY
}

# batch_states <state.db>: "<batch_id> <state>" per row, sorted.
batch_states() {
  python3 -I - "$1" << 'PY'
import sqlite3, sys
con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
for batch_id, state in sorted(con.execute("SELECT batch_id, state FROM batches")):
    print(batch_id, state)
PY
}

append_lines() { # <file> <label> <count>
  local i
  for ((i = 1; i <= $3; i++)); do echo "compat $2 $i" >> "$1"; done
}

# committed_stay_committed <before-file> <after-file>: every batch that was
# source_committed before is still, and no row is gone.
committed_stay_committed() {
  local id state
  while read -r id state; do
    [[ "$state" == source_committed ]] || continue
    grep -qx "$id source_committed" "$2" || fail "batch $id was committed and no longer is"
  done < "$1"
  while read -r id state; do
    grep -q "^$id " "$2" || fail "batch $id is gone from the ledger"
  done < "$1"
}

# file_mode <path>: the octal permission bits, as four digits.
file_mode() {
  python3 -I -c 'import os, sys; print(format(os.stat(sys.argv[1]).st_mode & 0o7777, "04o"))' "$1"
}

# adopt_ledger <head-vtopctl> <dir>: a ledger an older release created has the
# mode its umask gave it (0644 under the usual 022), and this build refuses to
# read a ledger other local users can (#733, #775). That is an upgrade step the
# operator takes (`chmod 0600`), so the cell checks the refusal, checks that
# it changed nothing, and then takes the step. A release that creates the ledger
# owner-only needs none, and the cell skips it.
adopt_ledger() {
  local head="$1" dir="$2" before f mode
  mode="$(file_mode "$dir/state.db")"
  if [[ "$mode" == 0600 ]]; then
    # A release that already creates its ledger owner-only (the hardening of
    # #733 and #775, from the release after v0.10.1) needs no step.
    log "the release created the ledger owner-only (0600): nothing to adopt"
    return 0
  fi
  [[ "$mode" == 0644 ]] \
    || fail "the release left the ledger at mode $mode, neither the 0644 its umask gives nor the 0600 of a release that hardens it"
  before="$(ledger_digest "$dir/state.db")"
  run head-refuses-a-readable-ledger "$head" status --config "$dir/good.yaml"
  [[ "$RC" -ne 0 ]] || fail "this build opened a ledger with mode 0644; it must refuse one other users can read"
  grep -q 'Refused: the file was not read' "$WORKDIR/logs/head-refuses-a-readable-ledger.log" \
    || { sed 's/^/    /' "$WORKDIR/logs/head-refuses-a-readable-ledger.log" >&2; fail "the refusal does not say the file was not read"; }
  [[ "$(ledger_digest "$dir/state.db")" == "$before" ]] || fail "the refusal changed the ledger"
  for f in "$dir"/state.db "$dir"/state.db-wal "$dir"/state.db-shm; do
    if [[ -e "$f" ]]; then chmod 0600 "$f"; fi
  done
}

# require_ledger <dir> <who>: the scenario left a SQLite ledger with rows. A
# binary that runs and archives nothing must fail the cell, not pass it.
require_ledger() {
  [[ -s "$1/state.db" ]] || fail "$2 wrote no ledger (no state.db after the scenario)"
}
