#!/usr/bin/env bash
# Backup and restore drill for the state store (#734).
#
# A live engine archives a file that keeps growing. While it runs, the drill
# takes a backup of its ledger, lets the engine go on, kills the engine
# (SIGKILL) and destroys the ledger. It then restores the backup the way
# docs/operate/backup-and-recovery.md says, starts the engine again and
# proves the engine resumed correctly:
#
#   - `vtopctl audit-coverage --check-storage` reports no hole and every
#     committed object and manifest present;
#   - `vtopctl scrub --sample 100` reads the sampled objects back clean;
#   - every record the source ever held is in an object a committed ledger or
#     digest row names, counted from the objects themselves, not from the
#     audit's arithmetic;
#   - the audit trail's hash chain still verifies.
#
# It measures, and fails when a number is past the bound the page documents:
#
#   RPO  how much ledger history the restore lost: the age gap between the
#        newest write the destroyed ledger held and the newest the restored
#        one holds, read from the rows' own `updated_at`. The archive loses
#        nothing: the source still holds every range that was not committed,
#        and the engine reads it again (a duplicate, never a hole).
#   RTO  from the first step of the restore until the engine commits a batch
#        again (restore, migrate, engine start, recovery, first commit).
#
# Modes:
#   sqlite         `sqlite3 .backup` (the online backup API) while the engine
#                  runs, plus the digest directory; restored by copying back.
#   postgres-dump  `pg_dump --format=custom` while the engine runs; restored
#                  with `pg_restore` into a new database, then `vtopctl migrate`.
#   postgres-pitr  `pg_basebackup` plus continuous WAL archiving; the primary
#                  and its data are destroyed, and a new server is recovered
#                  from the base backup and the archive, once to the end of the
#                  archive (the RPO) and once to a time before the end.
#
# The PostgreSQL modes start their own server from the image named in
# RESTORE_DRILL_PG_IMAGE (CI: the digest-pinned image of the major under test,
# from postgres-matrix.yml) and need docker. The SQLite mode needs sqlite3.
#
# Usage: restore-drill.sh <sqlite|postgres-dump|postgres-pitr> <vtopctl>
#
# Environment:
#   RESTORE_DRILL_PG_IMAGE   the PostgreSQL image (postgres modes)
#   RESTORE_DRILL_PG_MAJOR   when set, the drill fails unless the server is this major
#   RESTORE_DRILL_PG_NETWORK a docker network mode for the PostgreSQL containers
#                            (for example container:<name> to share the
#                            engine's own network namespace when the engine
#                            runs in a container); the server is then
#                            reached on 127.0.0.1:5432. Default: unset, the
#                            container publishes a port on 127.0.0.1.
#   RESTORE_DRILL_WORKDIR    where the drill works; default a new temp dir
#   RESTORE_DRILL_RESULT     where the JSON result goes; default <workdir>/result.json
#   RESTORE_DRILL_KEEP       set to keep the workdir and containers on success
#
# The bounds below are the page's: the drill refuses to run when the page
# lists a different number.
set -euo pipefail
umask 077

# ---- bounds ------------------------------------------------------------------
# Seconds. docs/operate/backup-and-recovery.md has a row "| `NAME` | VALUE |"
# for each; this script checks that it does.
VTOP_DRILL_BACKUP_INTERVAL_S=8   # SQLite, dump: the engine runs this long after the backup
VTOP_DRILL_ARCHIVE_TIMEOUT_S=5   # PITR: archive_timeout on the primary
VTOP_DRILL_SQLITE_RPO_S=13       # backup interval + 5
VTOP_DRILL_DUMP_RPO_S=13         # backup interval + 5
VTOP_DRILL_PITR_RPO_S=10         # archive timeout + 5
VTOP_DRILL_SQLITE_RTO_S=60
VTOP_DRILL_DUMP_RTO_S=90
VTOP_DRILL_PITR_RTO_S=120

MODE="${1:?usage: restore-drill.sh <sqlite|postgres-dump|postgres-pitr> <vtopctl>}"
BIN="${2:?usage: restore-drill.sh <sqlite|postgres-dump|postgres-pitr> <vtopctl>}"
case "$MODE" in
  sqlite | postgres-dump | postgres-pitr) ;;
  *) echo "FAIL: unknown mode '$MODE'" >&2; exit 2 ;;
esac
[[ -x "$BIN" ]] || { echo "FAIL: $BIN is not executable" >&2; exit 2; }
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"
((BASH_VERSINFO[0] >= 5)) || { echo "FAIL: bash 5 or later is needed (EPOCHREALTIME)" >&2; exit 2; }

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOC="$REPO/docs/operate/backup-and-recovery.md"

case "$MODE" in
  sqlite) RPO_BOUND_S=$VTOP_DRILL_SQLITE_RPO_S; RTO_BOUND_S=$VTOP_DRILL_SQLITE_RTO_S ;;
  postgres-dump) RPO_BOUND_S=$VTOP_DRILL_DUMP_RPO_S; RTO_BOUND_S=$VTOP_DRILL_DUMP_RTO_S ;;
  postgres-pitr) RPO_BOUND_S=$VTOP_DRILL_PITR_RPO_S; RTO_BOUND_S=$VTOP_DRILL_PITR_RTO_S ;;
esac

WORKDIR="${RESTORE_DRILL_WORKDIR:-$(mktemp -d "${TMPDIR:-/tmp}/vtop-restore-drill.XXXXXX")}"
mkdir -p "$WORKDIR"
WORKDIR="$(cd "$WORKDIR" && pwd)"
# A directory a drill has used holds its ledger, backup and archive: a second
# drill on top of it would restore from, and audit, the first one's files.
[[ -z "$(ls -A "$WORKDIR")" ]] \
  || { echo "[restore-drill/$MODE] FAIL: $WORKDIR is not empty; name a new directory in RESTORE_DRILL_WORKDIR" >&2; exit 2; }
RESULT="${RESTORE_DRILL_RESULT:-$WORKDIR/result.json}"
LOGS="$WORKDIR/logs"
INPUT="$WORKDIR/input.log"
STORE="$WORKDIR/store"
DIGESTS="$WORKDIR/digests"
BK="$WORKDIR/backup"
DB="$WORKDIR/state.db"
CFG="$WORKDIR/config.yaml"
mkdir -p "$LOGS" "$STORE" "$BK"
: > "$INPUT"

RUN_ID="$$"
PG_IMAGE="${RESTORE_DRILL_PG_IMAGE:-}"
PG_CONT=""
PG_CONTAINERS=()
PG_VOLUMES=()
ENGINE_PID=""
WRITER_PID=""

log() { echo "[restore-drill/$MODE] $*"; }
fail() {
  echo "[restore-drill/$MODE] FAIL: $*" >&2
  local f
  for f in "$LOGS"/*.log; do
    [[ -f "$f" ]] || continue
    echo "--- tail of $f" >&2
    tail -n 15 "$f" | cut -c1-400 >&2 || true
  done
  exit 1
}

# shellcheck disable=SC2329  # invoked via `trap`
cleanup() {
  local status=$?
  [[ -z "$ENGINE_PID" ]] || kill -9 "$ENGINE_PID" 2> /dev/null || true
  [[ -z "$WRITER_PID" ]] || kill "$WRITER_PID" 2> /dev/null || true
  if [[ -n "${RESTORE_DRILL_KEEP:-}" && $status -eq 0 ]]; then
    log "kept $WORKDIR${PG_CONTAINERS[*]:+ and containers ${PG_CONTAINERS[*]}}"
    return
  fi
  local c v
  for c in ${PG_CONTAINERS[@]+"${PG_CONTAINERS[@]}"}; do docker rm -fv "$c" > /dev/null 2>&1 || true; done
  for v in ${PG_VOLUMES[@]+"${PG_VOLUMES[@]}"}; do docker volume rm -f "$v" > /dev/null 2>&1 || true; done
  # Keep the logs of a failure; remove a clean run's directory unless the
  # caller named it (CI reads the result and the logs from there).
  if [[ $status -eq 0 && -z "${RESTORE_DRILL_WORKDIR:-}" ]]; then rm -rf "$WORKDIR"; fi
}
trap cleanup EXIT

# ---- the page's numbers are the script's -------------------------------------
check_doc() {
  [[ -f "$DOC" ]] || fail "$DOC not found"
  local name value
  for name in VTOP_DRILL_BACKUP_INTERVAL_S VTOP_DRILL_ARCHIVE_TIMEOUT_S \
    VTOP_DRILL_SQLITE_RPO_S VTOP_DRILL_DUMP_RPO_S VTOP_DRILL_PITR_RPO_S \
    VTOP_DRILL_SQLITE_RTO_S VTOP_DRILL_DUMP_RTO_S VTOP_DRILL_PITR_RTO_S; do
    value="${!name}"
    grep -qF "| \`$name\` | $value |" "$DOC" \
      || fail "the page $DOC has no row '| \`$name\` | $value |': the bounds there and in this script must match"
  done
}
check_doc

# ---- time --------------------------------------------------------------------
now_us() { local t="$EPOCHREALTIME"; echo "${t//[.,]/}"; }
ms_since() { echo $((($(now_us) - $1) / 1000)); }
secs() { printf '%d.%03d' $(($1 / 1000)) $(($1 % 1000)); }

wait_until() { # <timeout seconds> <what> <predicate...>
  local deadline=$((SECONDS + $1)) what="$2"
  shift 2
  until "$@"; do
    if [[ -n "$ENGINE_PID" ]] && ! kill -0 "$ENGINE_PID" 2> /dev/null; then
      ENGINE_PID=""
      fail "the engine exited while waiting for $what"
    fi
    ((SECONDS < deadline)) || fail "timed out waiting for $what"
    sleep 0.2
  done
}

# ---- the ledger, whichever store it is in ------------------------------------
ledger_q() {
  case "$MODE" in
    sqlite) sqlite3 -cmd '.timeout 5000' "$DB" "$1" ;;
    *) docker exec "$PG_CONT" psql --no-psqlrc -U vtop -d vtop -tA -c "$1" ;;
  esac
}
# Ledger query that prints a number (0 when the ledger cannot be read yet).
q_num() { local n; n="$(ledger_q "$1" 2> /dev/null | head -n 1 || true)"; echo "${n:-0}"; }

SQL_COMMITTED="SELECT count(*) FROM batches WHERE state='source_committed'"
SQL_NEWEST="SELECT max(updated_at) FROM batches"
if [[ "$MODE" == sqlite ]]; then
  SQL_COMMITTED_END="SELECT COALESCE(MAX(CAST(json_extract(progress_end_json,'\$.end_byte') AS INTEGER)),0) FROM batches WHERE state='source_committed'"
else
  SQL_COMMITTED_END="SELECT COALESCE(MAX(((progress_end_json::json)->>'end_byte')::bigint),0) FROM batches WHERE state='source_committed'"
fi
committed() { q_num "$SQL_COMMITTED"; }
committed_end() { q_num "$SQL_COMMITTED_END"; }
committed_ge() { (($(committed) >= $1)); }
committed_gt() { (($(committed) > $1)); }
committed_end_ge() { (($(committed_end) >= $1)); }
newest_update() { ledger_q "$SQL_NEWEST" 2> /dev/null | head -n 1; }
# Milliseconds from timestamp $2 to timestamp $1 (RFC 3339 text).
ts_diff_ms() {
  sqlite3 :memory: "SELECT CAST(ROUND((julianday('$1') - julianday('$2')) * 86400000) AS INTEGER)"
}

# ---- config, source, engine --------------------------------------------------
write_config() {
  local store_line
  if [[ "$MODE" == sqlite ]]; then
    store_line="\"sqlite://$DB\""
  else
    store_line="{env: VTOP_STATE_STORE}"
  fi
  cat > "$CFG" << YAML
engine:
  name: restore-drill
  state_store: $store_line
  work_dir: $WORKDIR/work
  ledger_digest_dir: $DIGESTS
batching:
  max_records: 40
  max_batch_age_seconds: 2
  idle_poll_interval_ms: 200
compression: {type: gzip, level: 1}
checksum: {algorithm: sha256}
sources:
  file:
    enabled: true
    paths: ["$INPUT"]
upload:
  backend: localfs
  bucket: telemetry-data
  local_path: $STORE
partitioning: {}
YAML
}

PAD="0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnop"
start_writer() {
  (
    n="$(wc -l < "$INPUT")"
    while :; do
      n=$((n + 1))
      printf 'restore-drill seq=%d %s\n' "$n" "$PAD" >> "$INPUT"
      sleep 0.05
    done
  ) &
  WRITER_PID=$!
}
stop_writer() {
  [[ -z "$WRITER_PID" ]] || { kill "$WRITER_PID" 2> /dev/null || true; wait "$WRITER_PID" 2> /dev/null || true; }
  WRITER_PID=""
}
source_lines() { wc -l < "$INPUT" | tr -d ' '; }
source_bytes() { wc -c < "$INPUT" | tr -d ' '; }

ENGINE_RUN=0
start_engine() {
  ENGINE_RUN=$((ENGINE_RUN + 1))
  "$BIN" run --config "$CFG" > "$LOGS/engine-$ENGINE_RUN.log" 2>&1 &
  ENGINE_PID=$!
}
kill_engine() { # a crash, not a shutdown
  kill -9 "$ENGINE_PID" 2> /dev/null || true
  wait "$ENGINE_PID" 2> /dev/null || true
  ENGINE_PID=""
}
stop_engine() { # SIGTERM: stop after the pass and flush; exit 0
  local pid="$ENGINE_PID" rc=0
  ENGINE_PID=""
  kill -TERM "$pid" 2> /dev/null || true
  wait "$pid" || rc=$?
  [[ $rc -eq 0 ]] || fail "the engine exited $rc on SIGTERM; see $LOGS/engine-$ENGINE_RUN.log"
}

ctl() { # <label> <args...>: a vtopctl command, its output kept
  local label="$1"
  shift
  "$BIN" "$@" > "$LOGS/$label.log" 2>&1 || fail "vtopctl $* failed (exit $?); see $LOGS/$label.log"
}

# ---- PostgreSQL --------------------------------------------------------------
need_pg() {
  [[ -n "$PG_IMAGE" ]] || fail "RESTORE_DRILL_PG_IMAGE must name the PostgreSQL image to drill"
  command -v docker > /dev/null || fail "docker is needed for the PostgreSQL modes"
}
# Run the image's own client tools in a container that mounts the given volumes.
pg_vol() {
  local v="$1"
  PG_VOLUMES+=("$v")
  docker volume create "$v" > /dev/null
}
pg_start() { # <container> <extra docker args...> -- <postgres args...>
  local c="$1"
  shift
  PG_CONTAINERS+=("$c")
  local -a net=(-p 127.0.0.1::5432)
  [[ -z "${RESTORE_DRILL_PG_NETWORK:-}" ]] || net=(--network "$RESTORE_DRILL_PG_NETWORK")
  docker run -d --name "$c" --cpus=1 -e POSTGRES_USER=vtop -e POSTGRES_PASSWORD=vtop \
    -e POSTGRES_DB=vtop -e PGDATA=/pgdata "${net[@]}" "$@" > /dev/null
}
pg_url() {
  local hostport
  if [[ -n "${RESTORE_DRILL_PG_NETWORK:-}" ]]; then
    echo "postgres://vtop:vtop@127.0.0.1:5432/vtop"
    return
  fi
  hostport="$(docker port "$PG_CONT" 5432/tcp | head -n 1)"
  echo "postgres://vtop:vtop@127.0.0.1:${hostport##*:}/vtop"
}
# True once the final server (not the entrypoint's temporary one, which only
# listens on its socket) takes connections over TCP and is not in recovery.
# A server that exited is a failure now, with its log, not a timeout later.
pg_ready() {
  if [[ "$(docker inspect -f '{{.State.Running}}' "$PG_CONT" 2> /dev/null || echo false)" != true ]]; then
    docker logs "$PG_CONT" > "$LOGS/$PG_CONT.log" 2>&1 || true
    fail "the PostgreSQL server in $PG_CONT exited; its log is $LOGS/$PG_CONT.log"
  fi
  [[ "$(docker exec -e PGPASSWORD=vtop "$PG_CONT" psql --no-psqlrc -h 127.0.0.1 -U vtop -d vtop -tA \
    -c 'SELECT pg_is_in_recovery()' 2> /dev/null || true)" == f ]]
}
pg_use() { # <container>: the ledger the engine and the queries use from now on
  PG_CONT="$1"
  wait_until 120 "PostgreSQL in $PG_CONT to take connections" pg_ready
  if [[ -n "${RESTORE_DRILL_PG_MAJOR:-}" ]]; then
    local version
    version="$(docker exec "$PG_CONT" psql --no-psqlrc -U vtop -d vtop -tA -c 'SHOW server_version')"
    [[ "$version" == "$RESTORE_DRILL_PG_MAJOR".* || "$version" == "$RESTORE_DRILL_PG_MAJOR" ]] \
      || fail "this drill is for PostgreSQL $RESTORE_DRILL_PG_MAJOR but $PG_CONT reports server_version '$version'; check RESTORE_DRILL_PG_IMAGE"
  fi
  VTOP_STATE_STORE="$(pg_url)"
  export VTOP_STATE_STORE
}

# ---- the proof that the engine resumed correctly -----------------------------
# $1 label. The ledger and the bucket are read as an operator would, after the
# engine has stopped.
prove_resumed() {
  local label="$1" lines bytes
  stop_writer
  lines="$(source_lines)"
  bytes="$(source_bytes)"
  wait_until 60 "the engine to archive the source to its end ($bytes bytes)" committed_end_ge "$bytes"
  stop_engine

  ctl "$label-status" status --config "$CFG"
  # The ledger is whole and the hash chain of its audit trail verifies.
  ctl "$label-audit-verify" audit verify --config "$CFG"

  # Zero holes, and every committed object and manifest present.
  "$BIN" audit-coverage --config "$CFG" --digest-dir "$DIGESTS" --check-storage \
    > "$LOGS/$label-audit-coverage.log" 2>&1 \
    || fail "audit-coverage found a problem after the restore ($label); see $LOGS/$label-audit-coverage.log"
  grep -q '^storage: every committed object and manifest is present' "$LOGS/$label-audit-coverage.log" \
    || fail "audit-coverage did not check storage after the restore ($label); see $LOGS/$label-audit-coverage.log"
  if grep -Eq '^(FAIL|[[:space:]]+(hole|gap|unproven|pending))' "$LOGS/$label-audit-coverage.log"; then
    fail "audit-coverage reports a hole, gap, unproven or pending range after the restore ($label); see $LOGS/$label-audit-coverage.log"
  fi

  # A sample read back from storage, clean.
  "$BIN" scrub --config "$CFG" --digest-dir "$DIGESTS" --sample 100 --seed 734 \
    > "$LOGS/$label-scrub.log" 2>&1 \
    || fail "scrub found a problem after the restore ($label); see $LOGS/$label-scrub.log"
  SCRUBBED="$(sed -n 's/^scrubbed \([0-9]*\) of \([0-9]*\) committed.*/\1/p' "$LOGS/$label-scrub.log" | head -n 1)"
  [[ "${SCRUBBED:-0}" -ge 1 ]] || fail "scrub checked no batch after the restore ($label); see $LOGS/$label-scrub.log"

  # Every record the source held is in an object a committed row names,
  # counted from the objects.
  local uris="$WORKDIR/$label-uris" seqs="$WORKDIR/$label-seqs" uri missing
  ledger_q "SELECT object_uri FROM batches WHERE state='source_committed'" > "$uris"
  if compgen -G "$DIGESTS/ledger-digest-*.jsonl" > /dev/null; then
    grep -ho '"object_uri": *"[^"]*"' "$DIGESTS"/ledger-digest-*.jsonl | sed 's/.*: *"\(.*\)"/\1/' >> "$uris" || true
  fi
  : > "$seqs"
  while IFS= read -r uri; do
    [[ -n "$uri" ]] || continue
    zcat -f "$STORE/${uri#s3://}" 2> /dev/null | grep -o 'seq=[0-9]*' | cut -d= -f2 >> "$seqs" \
      || fail "an object a committed row names is unreadable: $uri"
  done < <(sort -u "$uris")
  missing="$(sort -un "$seqs" | awk -v n="$lines" -v want=1 '
    { for (; want < $1; want++) { miss++; if (miss <= 5) ex = ex " " want } want = $1 + 1 }
    END { for (; want <= n; want++) { miss++; if (miss <= 5) ex = ex " " want } print miss + 0 ex }')"
  [[ "${missing%% *}" == 0 ]] \
    || fail "after the restore ($label) the archive lacks ${missing%% *} of the source's $lines record(s); first missing seq:${missing#* }"
  log "$label: resumed; audit-coverage clean, scrub clean ($SCRUBBED batch(es) read back), all $lines source record(s) archived"
}

# ---- one drill ---------------------------------------------------------------
write_config
[[ "$MODE" == sqlite ]] || need_pg
start_writer

BACKUP_MS=0
RESTORE_MS=0
RTO_MS=0
CAUGHT_UP_MS=0
DESTROYED_NEWEST=""
DESTROYED_COMMITTED=0
DESTROYED_END=0
RESTORED_NEWEST=""
RESTORED_COMMITTED=0
RESTORED_END=0
SCRUBBED=0
EXTRA_JSON=""

# The state of the ledger at the instant of the disaster, read from the
# destroyed store itself, before it is destroyed.
read_destroyed() {
  DESTROYED_NEWEST="$(newest_update)"
  DESTROYED_COMMITTED="$(committed)"
  DESTROYED_END="$(committed_end)"
}
read_restored() {
  RESTORED_NEWEST="$(newest_update)"
  RESTORED_COMMITTED="$(committed)"
  RESTORED_END="$(committed_end)"
  # A backup that captured none of what the engine had written (a plain copy
  # of a SQLite file whose rows are still in the -wal file) restores to an
  # empty ledger. The engine would archive the source again from its start
  # and every other check would pass, so the drill refuses it here.
  [[ -n "$RESTORED_NEWEST" && "$RESTORED_COMMITTED" -ge 1 ]] \
    || fail "the restored ledger holds no committed batch: the backup captured none of what the engine had written"
}
first_commit_after_restore() {
  wait_until 120 "the first commit after the restore" committed_gt "$RESTORED_COMMITTED"
}

# ---- numbers ------------------------------------------------------------------
RPO_MS=0
write_result() {
  local records rows_lost summary
  RPO_MS="$(ts_diff_ms "$DESTROYED_NEWEST" "$RESTORED_NEWEST")"
  [[ "$RPO_MS" =~ ^-?[0-9]+$ ]] || fail "could not compare the ledgers' newest writes ('$DESTROYED_NEWEST' and '$RESTORED_NEWEST')"
  ((RPO_MS >= 0)) || RPO_MS=0
  records="$(source_lines)"
  rows_lost=$((DESTROYED_COMMITTED - RESTORED_COMMITTED))
  {
    printf '{\n'
    printf '  "mode": "%s",\n' "$MODE"
    printf '  "postgres_image": "%s",\n' "$PG_IMAGE"
    printf '  "bounds_s": {"rpo": %s, "rto": %s},\n' "$RPO_BOUND_S" "$RTO_BOUND_S"
    printf '  "measured_ms": {"rpo": %s, "rto": %s, "backup": %s, "restore_steps": %s, "caught_up": %s},\n' \
      "$RPO_MS" "$RTO_MS" "$BACKUP_MS" "$RESTORE_MS" "$CAUGHT_UP_MS"
    printf '  "ledger": {"committed_rows_destroyed": %s, "committed_rows_restored": %s, "committed_rows_lost": %s, "bytes_to_archive_again": %s},\n' \
      "$DESTROYED_COMMITTED" "$RESTORED_COMMITTED" "$rows_lost" "$((DESTROYED_END - RESTORED_END))"
    printf '  "source_records": %s,\n' "$records"
    printf '  "batches_scrubbed": %s%s\n' "${SCRUBBED:-0}" "$EXTRA_JSON"
    printf '}\n'
  } > "$RESULT"
  [[ "${1:-}" != quiet ]] || return 0
  summary="$MODE: RPO $(secs "$RPO_MS") s (bound $RPO_BOUND_S s), RTO $(secs "$RTO_MS") s (bound $RTO_BOUND_S s); backup took $(secs "$BACKUP_MS") s; $rows_lost committed row(s) lost from the ledger and archived again; $records source record(s), none missing"
  log "$summary"
  cat "$RESULT"
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
      echo "### restore drill: $MODE${PG_IMAGE:+ ($PG_IMAGE)}"
      echo
      echo "| | measured | bound |"
      echo "|---|---|---|"
      echo "| RPO | $(secs "$RPO_MS") s | $RPO_BOUND_S s |"
      echo "| RTO | $(secs "$RTO_MS") s | $RTO_BOUND_S s |"
      echo
      echo "Backup took $(secs "$BACKUP_MS") s. $rows_lost committed ledger row(s) were lost and archived again; $records source record(s), none missing."
    } >> "$GITHUB_STEP_SUMMARY"
  fi
}
enforce_bounds() {
  ((RPO_MS <= RPO_BOUND_S * 1000)) \
    || fail "RPO $(secs "$RPO_MS") s is past the documented bound of $RPO_BOUND_S s"
  ((RTO_MS <= RTO_BOUND_S * 1000)) \
    || fail "RTO $(secs "$RTO_MS") s is past the documented bound of $RTO_BOUND_S s"
}


case "$MODE" in
  # ---------------------------------------------------------------------------
  sqlite)
    start_engine
    wait_until 120 "six committed batches" committed_ge 6

    # Pruning leaves a digest, which the audit and the scrub still account for.
    ctl prune prune-ledger --config "$CFG" --digest-dir "$DIGESTS" --older-than-days 0
    compgen -G "$DIGESTS/ledger-digest-*.jsonl" > /dev/null || fail "prune-ledger wrote no digest"

    # The backup, taken while the engine runs: the online backup API, then the
    # digest directory (after the database, so no digest a backed-up row
    # needs is missing).
    log "backup while the engine runs"
    T=$(now_us)
    sqlite3 "$DB" ".timeout 10000" ".backup '$BK/state.db'"
    cp -a "$DIGESTS" "$BK/digests"
    BACKUP_MS=$(ms_since "$T")
    [[ "$(sqlite3 "$BK/state.db" 'PRAGMA integrity_check')" == ok ]] || fail "the backup fails integrity_check"

    sleep "$VTOP_DRILL_BACKUP_INTERVAL_S"

    log "disaster: the engine is killed and the ledger destroyed"
    kill_engine
    read_destroyed
    mkdir -p "$WORKDIR/destroyed"
    cp -a "$DB" "$WORKDIR/destroyed/"
    rm -f "$DB" "$DB-wal" "$DB-shm"
    rm -rf "$DIGESTS"

    log "restore"
    T_RESTORE=$(now_us)
    cp "$BK/state.db" "$DB"
    cp -a "$BK/digests" "$DIGESTS"
    chmod 0600 "$DB"
    [[ "$(sqlite3 "$DB" 'PRAGMA integrity_check')" == ok ]] || fail "the restored ledger fails integrity_check"
    RESTORE_MS=$(ms_since "$T_RESTORE")
    read_restored
    start_engine
    first_commit_after_restore
    RTO_MS=$(ms_since "$T_RESTORE")
    sleep 2
    prove_resumed restored
    CAUGHT_UP_MS=$(ms_since "$T_RESTORE")
    ;;

  # ---------------------------------------------------------------------------
  postgres-dump)
    pg_start "vtop-734-drill-$RUN_ID-pg" "$PG_IMAGE"
    pg_use "vtop-734-drill-$RUN_ID-pg"
    ctl migrate migrate --config "$CFG"
    start_engine
    wait_until 120 "six committed batches" committed_ge 6
    ctl prune prune-ledger --config "$CFG" --digest-dir "$DIGESTS" --older-than-days 0
    compgen -G "$DIGESTS/ledger-digest-*.jsonl" > /dev/null || fail "prune-ledger wrote no digest"

    log "backup while the engine runs (pg_dump)"
    T=$(now_us)
    docker exec "$PG_CONT" pg_dump -U vtop -d vtop --format=custom > "$BK/ledger.dump"
    cp -a "$DIGESTS" "$BK/digests"
    BACKUP_MS=$(ms_since "$T")
    [[ -s "$BK/ledger.dump" ]] || fail "pg_dump wrote nothing"
    docker exec -i "$PG_CONT" pg_restore -l < "$BK/ledger.dump" > /dev/null || fail "the dump's table of contents does not read"

    sleep "$VTOP_DRILL_BACKUP_INTERVAL_S"

    log "disaster: the engine is killed and the database dropped"
    kill_engine
    read_destroyed
    docker exec "$PG_CONT" psql --no-psqlrc -U vtop -d postgres -v ON_ERROR_STOP=1 \
      -c 'DROP DATABASE vtop WITH (FORCE)' > /dev/null
    rm -rf "$DIGESTS"

    log "restore"
    T_RESTORE=$(now_us)
    docker exec "$PG_CONT" psql --no-psqlrc -U vtop -d postgres -v ON_ERROR_STOP=1 \
      -c 'CREATE DATABASE vtop' > /dev/null
    docker exec -i "$PG_CONT" pg_restore -U vtop -d vtop --exit-on-error < "$BK/ledger.dump" \
      > "$LOGS/pg_restore.log" 2>&1 || fail "pg_restore failed; see $LOGS/pg_restore.log"
    cp -a "$BK/digests" "$DIGESTS"
    # Always, before any engine starts: it re-creates the commit rule's
    # trigger from this release's function.
    ctl migrate-after-restore migrate --config "$CFG"
    RESTORE_MS=$(ms_since "$T_RESTORE")
    read_restored
    start_engine
    first_commit_after_restore
    RTO_MS=$(ms_since "$T_RESTORE")
    sleep 2
    prove_resumed restored
    CAUGHT_UP_MS=$(ms_since "$T_RESTORE")
    ;;

  # ---------------------------------------------------------------------------
  postgres-pitr)
    P="vtop-734-drill-$RUN_ID"
    V_WAL="$P-wal"
    V_BASE="$P-base"
    V_DATA="$P-data"
    for v in "$V_WAL" "$V_BASE" "$V_DATA"; do pg_vol "$v"; done
    # The archive and the base backup are on storage of their own, which
    # survives the loss of the data volume; owned by the server's user.
    for v in "$V_WAL" "$V_BASE"; do
      docker run --rm --user root --entrypoint sh -v "$v:/v" "$PG_IMAGE" \
        -c 'chown postgres:postgres /v && chmod 700 /v'
    done
    pg_start "$P-primary" -v "$V_DATA:/pgdata" -v "$V_WAL:/walarch" -v "$V_BASE:/basebackup" "$PG_IMAGE" \
      -c archive_mode=on \
      -c 'archive_command=test ! -f /walarch/%f && cp %p /walarch/%f' \
      -c "archive_timeout=${VTOP_DRILL_ARCHIVE_TIMEOUT_S}"
    pg_use "$P-primary"
    ctl migrate migrate --config "$CFG"
    start_engine
    wait_until 120 "six committed batches" committed_ge 6
    ctl prune prune-ledger --config "$CFG" --digest-dir "$DIGESTS" --older-than-days 0
    compgen -G "$DIGESTS/ledger-digest-*.jsonl" > /dev/null || fail "prune-ledger wrote no digest"

    log "base backup while the engine runs (pg_basebackup)"
    T=$(now_us)
    docker exec "$PG_CONT" pg_basebackup -U vtop -D /basebackup/base -X stream -c fast \
      > "$LOGS/pg_basebackup.log" 2>&1 || fail "pg_basebackup failed; see $LOGS/pg_basebackup.log"
    cp -a "$DIGESTS" "$BK/digests"
    BACKUP_MS=$(ms_since "$T")

    # A moment between the backup and the disaster to recover to, with the
    # engine still committing on both sides of it.
    sleep 3
    # In the form recovery_target_time takes, and the same instant as RFC 3339
    # for comparing with the rows' own timestamps.
    T_TARGET="$(ledger_q "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') || '+00'" | head -n 1)"
    T_TARGET_ISO="${T_TARGET/ /T}"
    T_TARGET_ISO="${T_TARGET_ISO%+00}Z"
    sleep $((VTOP_DRILL_ARCHIVE_TIMEOUT_S * 2 + 2))

    log "disaster: the engine is killed and the server and its data are lost"
    kill_engine
    read_destroyed
    docker kill "$PG_CONT" > /dev/null
    docker rm -fv "$PG_CONT" > /dev/null
    docker volume rm -f "$V_DATA" > /dev/null
    rm -rf "$DIGESTS"

    # Recover a new server from the base backup and the archive. $1 the
    # container, $2 its data volume, $3 an optional recovery target time.
    pitr_server() {
      local c="$1" vol="$2" target="${3:-}"
      local -a extra=()
      pg_vol "$vol"
      docker run --rm --user root --entrypoint sh -v "$V_BASE:/basebackup:ro" -v "$vol:/pgdata" "$PG_IMAGE" \
        -c 'cp -a /basebackup/base/. /pgdata/ && chown -R postgres:postgres /pgdata && chmod 700 /pgdata && touch /pgdata/recovery.signal'
      [[ -z "$target" ]] || extra=(-c "recovery_target_time=$target" -c recovery_target_inclusive=on)
      pg_start "$c" -v "$vol:/pgdata" -v "$V_WAL:/walarch:ro" "$PG_IMAGE" \
        -c 'restore_command=cp /walarch/%f %p' -c recovery_target_action=promote \
        ${extra[@]+"${extra[@]}"}
      pg_use "$c"
    }

    log "restore to the end of the archive"
    T_RESTORE=$(now_us)
    pitr_server "$P-restored" "$P-data-restored"
    cp -a "$BK/digests" "$DIGESTS"
    ctl migrate-after-restore migrate --config "$CFG"
    RESTORE_MS=$(ms_since "$T_RESTORE")
    read_restored
    start_engine
    first_commit_after_restore
    RTO_MS=$(ms_since "$T_RESTORE")
    sleep 2
    prove_resumed restored
    CAUGHT_UP_MS=$(ms_since "$T_RESTORE")
    LATEST_NEWEST="$RESTORED_NEWEST"
    docker rm -fv "$PG_CONT" > /dev/null
    # The numbers are settled by the first restore; the bounds are enforced
    # before the second so that a failure there is not taken for this one.
    write_result quiet
    enforce_bounds

    # The same base backup and archive, recovered to a moment before the end:
    # the way back from a mistake, not from a lost host.
    log "restore to a point in time ($T_TARGET)"
    pitr_server "$P-targeted" "$P-data-targeted" "$T_TARGET"
    TARGET_NEWEST="$(newest_update)"
    TARGET_COMMITTED="$(committed)"
    [[ "$(ts_diff_ms "$TARGET_NEWEST" "$LATEST_NEWEST")" -lt 0 ]] \
      || fail "recovery to $T_TARGET did not stop early: the ledger's newest write is $TARGET_NEWEST, as late as the full recovery's $LATEST_NEWEST"
    # The database's clock and the engine's agree to the millisecond here (one
    # host); a row stamped after the target would be a write the target excludes.
    [[ "$(sqlite3 :memory: "SELECT julianday('$TARGET_NEWEST') <= julianday('$T_TARGET_ISO')")" == 1 ]] \
      || fail "recovery to $T_TARGET kept a write from after it: the newest is $TARGET_NEWEST"
    ctl migrate-after-target migrate --config "$CFG"
    start_engine
    prove_resumed targeted
    EXTRA_JSON=$(printf ',\n  "point_in_time": {"target": "%s", "newest_write_restored": "%s", "committed_rows_restored": %s}' \
      "$T_TARGET" "$TARGET_NEWEST" "$TARGET_COMMITTED")
    ;;
esac

write_result
enforce_bounds
log "OK"
