#!/usr/bin/env bash
# Ledger compatibility (#626): a ledger THIS build has opened, written and
# pruned must keep working under the previous release, which ignores the
# columns it does not know.
#
# Only N-1 may share a ledger with this build (#743). What N-1 meets from
# here: the nullable `sealed_at` column, rows that carry it, and prune
# digests whose rows carry it. Each must leave N-1 working: it archives,
# reports status, audits coverage over the digests, and writes NULL into the
# column, and this build then carries on over what N-1 wrote.
#
#   1. This build archives two batches into a SQLite ledger and prunes the
#      older committed row into a digest (`prune-ledger --older-than-days 0`):
#      the ledger and the digest now hold rows with `sealed_at`.
#   2. The OLD vtopctl, on the same ledger and config: `status`,
#      `audit-coverage` (reads the digest) and `process-once` over newly
#      appended lines must all succeed.
#      Before the old release runs, this build leaves an INCOMPLETE row too
#      (an upload to a store path that cannot exist fails it into
#      REPLAY_REQUIRED, with `sealed_at`), so the old release's recovery
#      claims, replays and commits a row this build wrote.
#   3. This build runs `process-once` and `audit-coverage` again over what the
#      old one wrote. The old row's `sealed_at` is checked to be NULL when the
#      old release predates the column (before v0.10.0), and set when it
#      writes it too (v0.10.0 on) (sqlite3
#      must be on the PATH; LEDGER_COMPAT_NO_SQLITE3=1 skips this check
#      explicitly), this build's rows to carry one, and `user_version` to be
#      what the old release reads.
#
# With LEDGER_COMPAT_POSTGRES set to a PostgreSQL URL (a throwaway database
# on localhost), the same runs against PostgreSQL: the OLD release migrates
# the database first, this build migrates it (adding the column), and the
# OLD release must still archive on it. Both binaries need the `postgres`
# feature; the release binaries do not have it, so CI runs the SQLite half.
#
# Usage: ledger-compat.sh <new-vtopctl> <old-label> <old-vtopctl>
set -euo pipefail

NEW_BIN="${1:?usage: ledger-compat.sh <new-vtopctl> <old-label> <old-vtopctl>}"
OLD_LABEL="${2:?old release label}"
OLD_BIN="${3:?old vtopctl binary}"
[[ -x "$NEW_BIN" ]] || { echo "FAIL: $NEW_BIN is not executable" >&2; exit 1; }
[[ -x "$OLD_BIN" ]] || { echo "FAIL: $OLD_BIN is not executable" >&2; exit 1; }

WORKDIR="${LEDGER_COMPAT_WORKDIR:-$(mktemp -d "${TMPDIR:-/tmp}/vtop-ledger-compat.XXXXXX")}"
mkdir -p "$WORKDIR/logs"

log() { echo "[ledger-compat/$OLD_LABEL] $*"; }
# Whether a release label (vX.Y.Z, or X.Y.Z) names a release that writes
# `sealed_at`: the column arrived in v0.10.0 (#755). Older releases ignore it.
knows_sealed_at() {
  local v="${1#v}"
  [[ "$(printf '%s\n%s\n' "0.10.0" "${v%%-*}" | sort -V | head -n 1)" == "0.10.0" ]]
}
fail() { echo "[ledger-compat/$OLD_LABEL] FAIL: $*" >&2; exit 1; }

# run <label> <binary> <args...>: run a vtopctl command, keep its log, fail
# naming the log when it fails.
run() {
  local label="$1" bin="$2"
  shift 2
  "$bin" "$@" > "$WORKDIR/logs/$label.log" 2>&1 \
    || fail "$label failed (exit $?); see $WORKDIR/logs/$label.log"
}

if ! command -v sqlite3 > /dev/null && [[ -z "${LEDGER_COMPAT_NO_SQLITE3:-}" ]]; then
  fail "sqlite3 is not on the PATH; install it, or set LEDGER_COMPAT_NO_SQLITE3=1 to skip the column checks"
fi

# Only keys every replayed release knows. $3, when set, names the file the
# store path runs through: every upload fails (ENOTDIR).
write_config() {
  local dir="$1" store="$2" name="${3:-config.yaml}" local_path="$1/store"
  mkdir -p "$dir"
  if [[ "$name" != config.yaml ]]; then
    : > "$dir/notadir"
    local_path="$dir/notadir/store"
  fi
  cat > "$dir/$name" <<YAML
engine:
  name: ledger-compat
  state_store: $store
  work_dir: $dir/work
batching: {}
compression: {type: gzip, level: 6}
checksum: {algorithm: sha256}
sources:
  file:
    enabled: true
    paths: ["$dir/input.log"]
upload:
  backend: localfs
  bucket: telemetry-data
  local_path: $local_path
partitioning: {}
YAML
}

append() { printf '%s\n' "$2" >> "$1/input.log"; }

# The steps both backends share. $1: the directory, $2: a log-name prefix.
exercise() {
  local dir="$1" p="$2" cfg="$1/config.yaml"
  append "$dir" "$p first"
  run "$p-new-1" "$NEW_BIN" process-once --source file --config "$cfg"
  append "$dir" "$p second"
  run "$p-new-2" "$NEW_BIN" process-once --source file --config "$cfg"
  run "$p-new-prune" "$NEW_BIN" prune-ledger --config "$cfg" \
    --digest-dir "$dir/digests" --older-than-days 0
  compgen -G "$dir/digests/*" > /dev/null \
    || fail "$p: prune-ledger wrote no digest; see $WORKDIR/logs/$p-new-prune.log"
  grep -q '"sealed_at"' "$dir"/digests/* \
    || fail "$p: the digest rows carry no sealed_at, so the old reader is not tested on it"

  # An incomplete row of this build's, for the old release's recovery.
  append "$dir" "$p third"
  "$NEW_BIN" process-once --source file --config "$dir/broken.yaml" \
    > "$WORKDIR/logs/$p-new-broken.log" 2>&1 || true
  grep -q 'state=failed' "$WORKDIR/logs/$p-new-broken.log" \
    || fail "$p: this build left no failed row; see $WORKDIR/logs/$p-new-broken.log"

  log "$p: $OLD_LABEL reads the ledger and the digest this build wrote"
  run "$p-old-status" "$OLD_BIN" status --config "$cfg"
  run "$p-old-audit" "$OLD_BIN" audit-coverage --config "$cfg" --digest-dir "$dir/digests"
  run "$p-old-process" "$OLD_BIN" process-once --source file --config "$cfg"
  grep -q 'committed=true' "$WORKDIR/logs/$p-old-process.log" \
    || fail "$p: $OLD_LABEL did not commit on the ledger this build opened; see $WORKDIR/logs/$p-old-process.log"

  log "$p: this build carries on over what $OLD_LABEL wrote"
  append "$dir" "$p fourth"
  run "$p-new-3" "$NEW_BIN" process-once --source file --config "$cfg"
  grep -q 'committed=true' "$WORKDIR/logs/$p-new-3.log" \
    || fail "$p: this build did not commit after $OLD_LABEL; see $WORKDIR/logs/$p-new-3.log"
  run "$p-new-audit" "$NEW_BIN" audit-coverage --config "$cfg" --digest-dir "$dir/digests"
}

log "SQLite: this build archives and prunes, then $OLD_LABEL works on the same ledger"
sqlite_dir="$WORKDIR/sqlite"
write_config "$sqlite_dir" "\"sqlite://$sqlite_dir/state.db\""
write_config "$sqlite_dir" "\"sqlite://$sqlite_dir/state.db\"" broken.yaml
exercise "$sqlite_dir" sqlite
if command -v sqlite3 > /dev/null; then
  db="$sqlite_dir/state.db"
  version="$(sqlite3 "$db" 'PRAGMA user_version')"
  [[ "$version" == 1 ]] || fail "SQLite user_version is $version; $OLD_LABEL reads 1"
  null_rows="$(sqlite3 "$db" "SELECT count(*) FROM batches WHERE sealed_at IS NULL")"
  set_rows="$(sqlite3 "$db" "SELECT count(*) FROM batches WHERE sealed_at IS NOT NULL")"
  [[ "$set_rows" -ge 1 ]] || fail "no row of this build's carries sealed_at"
  if knows_sealed_at "$OLD_LABEL"; then
    # v0.10.0 added the column (#755), so a release from then on writes it
    # too: every row, the old release's included, must carry the instant.
    [[ "$null_rows" -eq 0 ]] || fail "$null_rows row(s) left sealed_at NULL, but $OLD_LABEL writes it"
    log "SQLite: user_version $version, all $set_rows rows carry sealed_at ($OLD_LABEL writes it too)"
  else
    # Every row this build wrote carries the instant, so the rows without one
    # are the old release's, which ignores the column.
    [[ "$null_rows" -ge 1 ]] || fail "no row of $OLD_LABEL's left sealed_at NULL"
    log "SQLite: user_version $version, $OLD_LABEL's row NULL, $set_rows rows of this build's with sealed_at"
  fi
else
  log "SQLite: LEDGER_COMPAT_NO_SQLITE3 is set; the column values were not inspected"
fi

if [[ -n "${LEDGER_COMPAT_POSTGRES:-}" ]]; then
  log "PostgreSQL: $OLD_LABEL migrates, this build migrates, then $OLD_LABEL works on it"
  export VTOP_STATE_STORE="$LEDGER_COMPAT_POSTGRES"
  pg_dir="$WORKDIR/postgres"
  write_config "$pg_dir" "{env: VTOP_STATE_STORE}"
  write_config "$pg_dir" "{env: VTOP_STATE_STORE}" broken.yaml
  run pg-old-migrate "$OLD_BIN" migrate --config "$pg_dir/config.yaml"
  run pg-new-migrate "$NEW_BIN" migrate --config "$pg_dir/config.yaml"
  exercise "$pg_dir" postgres
  run pg-old-migrate-again "$OLD_BIN" migrate --config "$pg_dir/config.yaml"
  run pg-new-status "$NEW_BIN" status --config "$pg_dir/config.yaml"
fi

log "OK: $OLD_LABEL keeps working on a ledger this build opened, migrated and pruned"
if [[ -z "${LEDGER_COMPAT_KEEP:-}" && -z "${LEDGER_COMPAT_WORKDIR:-}" ]]; then
  rm -rf "$WORKDIR"
fi
