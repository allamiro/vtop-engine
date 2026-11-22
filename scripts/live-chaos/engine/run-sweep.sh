#!/usr/bin/env bash
# Engine crash sweep (#725): kill the archive engine with SIGKILL at every
# failpoint, restart it, and check that nothing was lost and nothing was
# committed at the source before it was verified.
#
# System model: one crash-recovery engine process (`vtopctl run`), a real
# source (a Kafka broker, a file, or a syslog spool file), a real S3 store (the lab's MinIO) and a
# ledger (SQLite, or PostgreSQL); a partially synchronous network; honest
# processes. The fault is a SIGKILL of the engine at one line, chosen by a
# failpoint (crates/vtop-cli/src/failpoints.rs) in a build with the
# `failpoints` feature. Optionally the store is down, or the engine's work
# directory is full, while it runs towards that line (ENGINE_SWEEP_FAULT).
# Nothing else fails.
#
# Invariants, checked for every point:
#   safety   the commit rule: the source's committed position never passes
#            the end of the highest range the ledger records as VERIFIED, and
#            no SOURCE_COMMITTED row in the ledger lacks what the VERIFIED
#            write leaves (checked at the kill, and again after the restart
#            has drained). For Kafka the position is the consumer group's
#            committed offset on the broker. A file's committed position
#            lives in the engine, not on the file, and the ledger is its
#            durable record, so for a file the position is the ledger's
#            cursor (the highest SOURCE_COMMITTED end byte), and the check
#            that bites at the kill is the next one
#   safety   the order: at the kill the newest ledger row is in the state the
#            failpoint sits beside (sweep_checks.py KILL_STATE), so the
#            engine never reached the source commit with a batch that was
#            not yet VERIFIED, whatever the source
#   safety   no hole: `audit-coverage --check-storage` passes, `scrub` reads
#            every committed object back clean, and every produced record is
#            found in the objects the store holds, counted by this harness
#   safety   duplicates only where the at-least-once window is open: none
#            for a kill before any object of the batch is in the store, and
#            at most one batch's records after (docs/ARCHITECTURE.md section
#            13, "duplicate, never a hole")
#   liveness the restarted engine, without the failpoint, commits every
#            record within ENGINE_SWEEP_POINT_TIMEOUT_SECONDS and stops
#            cleanly on SIGTERM
#
# Each point gets its own ledger (a SQLite file or a PostgreSQL database),
# work directory and object prefix, and, for Kafka, its own consumer group
# over one topic the harness seeds once (for a file, one input file written
# once), so a failure names its point and keeps its logs (CHAOS_KEEP=1 keeps
# every point's).
#
# Needs: a `failpoints` build of vtopctl (CHAOS_VTOPCTL, or
# target/$CHAOS_PROFILE/vtopctl), built with `--features failpoints,postgres`
# for the PostgreSQL ledger; for Kafka, a running Kafka container with the
# Apache CLI tools (ENGINE_SWEEP_KAFKA_CONTAINER, found by image when unset);
# for PostgreSQL, a running postgres container whose psql the harness uses
# (ENGINE_SWEEP_PG_CONTAINER); MinIO from docker-compose.yml (`docker compose
# up -d minio`), reached by the engine at ENGINE_SWEEP_S3_ENDPOINT and by the
# harness through `docker compose run minio-init`; docker, python3, timeout,
# tar; root or passwordless sudo for ENGINE_SWEEP_FAULT=enospc, which mounts
# a tmpfs.
#
#   cargo build -p vtop-cli --features failpoints --bin vtopctl
#   CHAOS_PROFILE=debug scripts/live-chaos/engine/run-sweep.sh
#   ENGINE_SWEEP_POINTS=before_source_commit,after_source_commit_before_ledger ...
#   ENGINE_SWEEP_SOURCE=file ENGINE_SWEEP_BACKEND=sqlite ...
#
# Knobs (validated below):
#   ENGINE_SWEEP_POINTS       `all` (default: every point the binary lists,
#                             and the run fails if one was not run) or a
#                             comma-separated subset
#   ENGINE_SWEEP_BACKEND      `sqlite` (default) or `postgres`
#   ENGINE_SWEEP_SOURCE       `kafka` (default), `file` or `syslog_spool` (a
#                             spool file, read by the spool source)
#   ENGINE_SWEEP_FAULT        `none` (default), `store_outage` (MinIO is
#                             stopped while the engine runs towards the
#                             failpoint, for ENGINE_SWEEP_FAULT_SECONDS, then
#                             started again) or `enospc` (the engine's work
#                             directory is a full 16 KiB tmpfs for as long,
#                             then space is freed)
#   ENGINE_SWEEP_FAULT_SECONDS how long the fault holds (default 12; a
#                             point reached sooner is killed with the fault
#                             still on)
#   ENGINE_SWEEP_RECORDS      records produced (default 50)
#   ENGINE_SWEEP_BATCH_RECORDS records per batch (default 20)
#   ENGINE_SWEEP_POINT_TIMEOUT_SECONDS bound on each wait (default 90)
#   ENGINE_SWEEP_KAFKA_BOOTSTRAP  the broker as the engine sees it
#                             (default localhost:9092)
#   ENGINE_SWEEP_KAFKA_CONTAINER  container with /opt/kafka/bin
#   ENGINE_SWEEP_PG_CONTAINER container running PostgreSQL (psql inside it
#                             reads and creates the ledgers)
#   ENGINE_SWEEP_PG_USER, ENGINE_SWEEP_PG_PASSWORD  the role (default
#                             vtop / vtop)
#   ENGINE_SWEEP_PG_HOSTPORT  the server as the engine sees it; plaintext
#                             is accepted only on loopback (default
#                             localhost:5432)
#   ENGINE_SWEEP_MINIO_CONTAINER  the store's container, for store_outage
#                             (default: the compose service `minio`)
#   ENGINE_SWEEP_S3_ENDPOINT  the store as the engine sees it
#                             (default http://127.0.0.1:9000)
#   ENGINE_SWEEP_BUCKET       bucket (default telemetry-sweep; created by the
#                             engine)
#
# Budget: 11 to 12 s a point with a debug build, measured locally (239 s for
# all 21 points, 55 s for the five pull-request points), plus the fault's
# hold for a point that waits on the store or the disk; the measurements are
# in docs/LIVE_CHAOS_VALIDATION.md. The workflow's per-point wait is bounded
# by ENGINE_SWEEP_POINT_TIMEOUT_SECONDS.
#
# Not covered here: power loss (ledger_power_loss.rs).
set -euo pipefail
umask 077
export CHAOS_NO_CERTS=1
SCENARIO="engine-sweep"
# shellcheck source=scripts/live-chaos/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
CHECKS="$SCRIPT_DIR/engine/sweep_checks.py"

POINTS_KNOB="${ENGINE_SWEEP_POINTS:-all}"
BACKEND="${ENGINE_SWEEP_BACKEND:-sqlite}"
SOURCE="${ENGINE_SWEEP_SOURCE:-kafka}"
FAULT="${ENGINE_SWEEP_FAULT:-none}"
FAULT_SECONDS="${ENGINE_SWEEP_FAULT_SECONDS:-12}"
RECORDS="${ENGINE_SWEEP_RECORDS:-50}"
BATCH_RECORDS="${ENGINE_SWEEP_BATCH_RECORDS:-20}"
POINT_TIMEOUT="${ENGINE_SWEEP_POINT_TIMEOUT_SECONDS:-90}"
KAFKA_BOOTSTRAP="${ENGINE_SWEEP_KAFKA_BOOTSTRAP:-localhost:9092}"
KAFKA_CONTAINER="${ENGINE_SWEEP_KAFKA_CONTAINER:-}"
PG_CONTAINER="${ENGINE_SWEEP_PG_CONTAINER:-}"
PG_USER="${ENGINE_SWEEP_PG_USER:-vtop}"
PG_PASSWORD="${ENGINE_SWEEP_PG_PASSWORD:-vtop}"
PG_HOSTPORT="${ENGINE_SWEEP_PG_HOSTPORT:-localhost:5432}"
MINIO_CONTAINER="${ENGINE_SWEEP_MINIO_CONTAINER:-}"
S3_ENDPOINT="${ENGINE_SWEEP_S3_ENDPOINT:-http://127.0.0.1:9000}"
BUCKET="${ENGINE_SWEEP_BUCKET:-telemetry-sweep}"
RECORD_PREFIX="sweep-record-"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

require_integer_in_range ENGINE_SWEEP_RECORDS "$RECORDS" 1 100000
require_integer_in_range ENGINE_SWEEP_BATCH_RECORDS "$BATCH_RECORDS" 1 "$RECORDS"
require_integer_in_range ENGINE_SWEEP_POINT_TIMEOUT_SECONDS "$POINT_TIMEOUT" 5 3600
require_integer_in_range ENGINE_SWEEP_FAULT_SECONDS "$FAULT_SECONDS" 3 600
[[ "$BACKEND" == "sqlite" || "$BACKEND" == "postgres" ]] \
  || fail "ENGINE_SWEEP_BACKEND must be sqlite or postgres, got '$BACKEND'"
[[ "$SOURCE" == "kafka" || "$SOURCE" == "file" || "$SOURCE" == "syslog_spool" ]] \
  || fail "ENGINE_SWEEP_SOURCE must be kafka, file or syslog_spool, got '$SOURCE'"
[[ "$FAULT" == "none" || "$FAULT" == "store_outage" || "$FAULT" == "enospc" ]] \
  || fail "ENGINE_SWEEP_FAULT must be none, store_outage or enospc, got '$FAULT'"
[[ "$PG_USER" =~ ^[a-z_][a-z0-9_]*$ && "$PG_PASSWORD" =~ ^[A-Za-z0-9_-]+$ \
  && "$PG_HOSTPORT" =~ ^[A-Za-z0-9.-]+:[0-9]+$ ]] \
  || fail "ENGINE_SWEEP_PG_USER, _PG_PASSWORD or _PG_HOSTPORT is not a plain name, password or host:port"
[[ "$BUCKET" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]] \
  || fail "ENGINE_SWEEP_BUCKET is not a valid bucket name: '$BUCKET'"
[[ "$POINTS_KNOB" =~ ^[a-z_,]+$ ]] \
  || fail "ENGINE_SWEEP_POINTS must be 'all' or comma-separated point names, got '$POINTS_KNOB'"
for tool in docker python3 timeout tar; do
  require_command "$tool" "The engine sweep needs $tool on PATH."
done
[[ -x "$VTOPCTL" ]] \
  || fail "missing $VTOPCTL: build it with 'cargo build -p vtop-cli --features failpoints --bin vtopctl' (set CHAOS_PROFILE or CHAOS_VTOPCTL to where it is)"

if [[ "$SOURCE" == "kafka" && -z "$KAFKA_CONTAINER" ]]; then
  KAFKA_CONTAINER="$(docker ps --filter "ancestor=apache/kafka:4.3.1" --format '{{.ID}}' | head -1)"
  [[ -n "$KAFKA_CONTAINER" ]] \
    || fail "no running apache/kafka:4.3.1 container; start one or set ENGINE_SWEEP_KAFKA_CONTAINER"
fi
kafka_tool() { # <tool> <args...> — an Apache Kafka CLI tool inside the broker's container
  local tool="$1"
  shift
  timeout 120 docker exec -i "$KAFKA_CONTAINER" "/opt/kafka/bin/$tool" \
    --bootstrap-server localhost:9092 "$@"
}

if [[ "$BACKEND" == "postgres" && -z "$PG_CONTAINER" ]]; then
  fail "ENGINE_SWEEP_BACKEND=postgres needs ENGINE_SWEEP_PG_CONTAINER, the container running PostgreSQL"
fi
pg_psql() { # <database> <sql> — one statement through the server's own psql
  timeout 60 docker exec -i -e PGOPTIONS='-c client_min_messages=warning' "$PG_CONTAINER" \
    psql --no-psqlrc -v ON_ERROR_STOP=1 -tA \
    -U "$PG_USER" -d "$1" -c "$2"
}

if [[ "$FAULT" == "store_outage" && -z "$MINIO_CONTAINER" ]]; then
  MINIO_CONTAINER="$(cd "$REPO_ROOT" && docker compose ps -q minio | head -1)"
  [[ -n "$MINIO_CONTAINER" ]] \
    || fail "ENGINE_SWEEP_FAULT=store_outage: no running compose service 'minio'; start it or set ENGINE_SWEEP_MINIO_CONTAINER"
fi

as_root() {
  if (( EUID == 0 )); then "$@"; else sudo -n "$@"; fi
}

init_workdir
RUN_ID="$(date +%s)-$$"
TOPIC="vtop-sweep-$RUN_ID"
INPUT="$WORKDIR/input/sweep-$RUN_ID.log"
SOURCE_BYTES=0

# What the faults leave behind must not outlive the run: lib.sh's cleanup
# calls this before it removes the work directory.
chaos_extra_cleanup() {
  if [[ -f "$WORKDIR/mounts" ]]; then
    while read -r mount_point; do
      as_root umount "$mount_point" 2> /dev/null || true
    done < "$WORKDIR/mounts"
  fi
  if [[ "$FAULT" == "store_outage" ]]; then
    docker start "$MINIO_CONTAINER" > /dev/null 2>&1 || true
  fi
}

# ---------------------------------------------------------------------------
# The point list comes from the binary, not from this script: a `failpoints`
# build refuses an unknown point and lists every one it knows. A binary
# without the feature would start instead, so this is also the check that the
# sweep is about to kill anything at all.
# ---------------------------------------------------------------------------
probe="$WORKDIR/probe"
mkdir -p "$probe/in"
cat > "$probe/config.yaml" <<EOF
engine:
  name: vtop-sweep-probe
  tenant: sweep
  state_store: "sqlite://$probe/state.db"
  work_dir: "$probe/work"
batching:
  max_records: 10
  max_bytes: 1048576
  max_batch_age_seconds: 60
compression:
  type: none
checksum:
  algorithm: sha256
sources:
  file:
    enabled: true
    paths: ["$probe/in/*.log"]
upload:
  backend: localfs
  local_path: "$probe/store"
  bucket: "probe"
EOF
probe_code=0
VTOP_FAILPOINT=__list_points__ timeout 60 "$VTOPCTL" process-once --source file \
  --config "$probe/config.yaml" > "$probe/out.log" 2>&1 || probe_code=$?
python3 "$CHECKS" points "$probe/out.log" > "$WORKDIR/listed-points" \
  || fail "$VTOPCTL listed no failpoints (exit $probe_code): it was not built with --features failpoints. Output:
$(tail -n 5 "$probe/out.log")"
mapfile -t LISTED < "$WORKDIR/listed-points"
log "the binary lists ${#LISTED[@]} failpoints"

if [[ "$POINTS_KNOB" == "all" ]]; then
  SELECTED=("${LISTED[@]}")
else
  IFS=',' read -r -a SELECTED <<< "$POINTS_KNOB"
  for p in "${SELECTED[@]}"; do
    grep -qx "$p" "$WORKDIR/listed-points" \
      || fail "ENGINE_SWEEP_POINTS names '$p', which the binary does not list: $(tr '\n' ' ' < "$WORKDIR/listed-points")"
  done
fi

# Points before any object of the batch is in the store: a replay rebuilds
# the batch from the source, and no copy of its records is in the store yet.
NO_DUPLICATE_POINTS=" before_insert after_insert before_ledger_sealed after_ledger_sealed \
before_ledger_compressed after_ledger_compressed before_ledger_checksummed \
after_ledger_checksummed mid_object_put "
allowed_duplicates() { # <point> -> the most extra copies the docs allow
  if [[ "$NO_DUPLICATE_POINTS" == *" $1 "* ]]; then echo 0; else echo "$BATCH_RECORDS"; fi
}

# ---------------------------------------------------------------------------
# Seed the source once. Records are numbered, so a hole names the record.
# ---------------------------------------------------------------------------
if [[ "$SOURCE" == "kafka" ]]; then
  kafka_tool kafka-topics.sh --create --if-not-exists --topic "$TOPIC" \
    --partitions 1 --replication-factor 1 > /dev/null
  for ((i = 0; i < RECORDS; i++)); do
    printf '%s%06d sweep payload for the engine crash sweep %s\n' "$RECORD_PREFIX" "$i" "$RUN_ID"
  done | kafka_tool kafka-console-producer.sh --topic "$TOPIC" > /dev/null
  end_offset="$(kafka_tool kafka-get-offsets.sh --topic "$TOPIC" | awk -F: '{print $3}')"
  [[ "$end_offset" == "$RECORDS" ]] \
    || fail "seeded $TOPIC with $RECORDS records, but its end offset is '$end_offset'"
  log "topic $TOPIC holds $RECORDS records; bucket $BUCKET at $S3_ENDPOINT; kafka at $KAFKA_BOOTSTRAP"
else
  mkdir -p "$(dirname "$INPUT")"
  for ((i = 0; i < RECORDS; i++)); do
    printf '%s%06d sweep payload for the engine crash sweep %s\n' "$RECORD_PREFIX" "$i" "$RUN_ID"
  done > "$INPUT"
  SOURCE_BYTES="$(wc -c < "$INPUT" | tr -d ' ')"
  log "file $INPUT holds $RECORDS records ($SOURCE_BYTES bytes); bucket $BUCKET at $S3_ENDPOINT"
fi
log "ledger: $BACKEND; fault: $FAULT"

emit_engine_config() { # <dir> <group> <prefix> <point> <state-store>
  local dir="$1" group="$2" prefix="$3" point="$4" state_store="$5" multipart="" source_block=""
  # The point inside a multipart upload needs one: a one-byte threshold
  # sends every object as a multipart upload (one part at this size).
  if [[ "$point" == "mid_object_put" ]]; then
    multipart="  multipart_threshold_bytes: 1"
  fi
  if [[ "$SOURCE" == "kafka" ]]; then
    source_block="  kafka:
    enabled: true
    bootstrap_servers: [\"$KAFKA_BOOTSTRAP\"]
    consumer_group: \"$group\"
    topic_include_regex: \"^${TOPIC}\$\"
    auto_offset_reset: earliest
    enable_auto_commit: false"
  else
    # A spool file is read as a file is, a line per record, by the spool
    # source's own adapter: its cursor, markers and replay.
    source_block="  $SOURCE:
    enabled: true
    paths: [\"$INPUT\"]"
  fi
  cat > "$dir/config.yaml" <<EOF
engine:
  name: vtop-sweep
  tenant: sweep
  state_store: $state_store
  work_dir: "$dir/work"
  log_level: info
batching:
  max_records: $BATCH_RECORDS
  max_bytes: 104857600
  max_batch_age_seconds: 2
  source_poll_wait_ms: 250
  idle_poll_interval_ms: 500
  # One batch in flight, so the batch a kill interrupts is the only one
  # whose records can be archived twice.
  max_concurrent_batches: 1
compression:
  type: gzip
  level: 1
checksum:
  algorithm: sha256
sources:
$source_block
upload:
  backend: s3_native
  bucket: "$BUCKET"
  prefix: "$prefix"
  endpoint_url: "$S3_ENDPOINT"
  region: us-east-1
  force_path_style: true
  verify_tls: false
  create_bucket: true
  require_strong_verification: true
$multipart
EOF
}

# group_committed <group> — print the group's committed offset on the
# partition, or `-` when the broker answered that it has none. Fails, with
# what the tool printed on stderr, when the answer was not read: the tool
# exits 0 even when its query timed out, and an unread offset taken for an
# absent one would pass the very premature commit the sweep looks for.
group_committed() {
  local out code=0
  out="$(kafka_tool kafka-consumer-groups.sh --describe --group "$1" 2>&1)" || code=$?
  if (( code != 0 )); then
    echo "kafka-consumer-groups.sh --describe --group $1 exited $code: $(tail -n 3 <<< "$out" | tr '\n' ' ')" >&2
    return 1
  fi
  python3 "$CHECKS" group-offset "$1" "$TOPIC" 0 <<< "$out"
}

# group_committed_settled <group> — group_committed, tried up to
# GROUP_QUERY_ATTEMPTS times. Used where the engine is not running, so the
# offset cannot move between attempts; a query that never answers fails the
# check, naming every attempt's error.
GROUP_QUERY_ATTEMPTS=3
group_committed_settled() {
  local group="$1" attempt errors="" value
  for ((attempt = 1; attempt <= GROUP_QUERY_ATTEMPTS; attempt++)); do
    if value="$(group_committed "$group" 2> "$WORKDIR/group-query.err")"; then
      echo "$value"
      return 0
    fi
    errors+=" [attempt $attempt] $(tr '\n' ' ' < "$WORKDIR/group-query.err")"
    (( attempt < GROUP_QUERY_ATTEMPTS )) && sleep 2
  done
  echo "could not read the committed offset of group $group from the broker in $GROUP_QUERY_ATTEMPTS attempts:$errors" >&2
  return 1
}

# The ledger as the checks read it: one JSON summary of the rows for this
# source. SQLite is opened read-only by the checks themselves (a read-write
# connection would checkpoint the WAL a kill left); PostgreSQL is asked
# through psql for the rows as JSON, which the checks then read.
POINT_DB="" # the PostgreSQL database of the point being run
ledger_json() { # <dir> <group>
  local dir="$1" group="$2" rows
  local -a src
  if [[ "$SOURCE" == "kafka" ]]; then src=(kafka "$TOPIC" 0 "$group"); else src=("$SOURCE" "$INPUT"); fi
  if [[ "$BACKEND" == "sqlite" ]]; then
    python3 "$CHECKS" ledger --sqlite "$dir/state.db" "${src[@]}"
    return
  fi
  rows="$(pg_psql "$POINT_DB" "SELECT COALESCE(json_agg(t ORDER BY t.created_at, t.batch_id), '[]'::json) FROM (SELECT batch_id, state, progress_end_json, verification_class, object_uri, manifest_uri, created_at FROM batches WHERE source_type = '$SOURCE') t" 2>&1)" \
    || { echo "could not read the PostgreSQL ledger $POINT_DB: $rows" >&2; return 1; }
  python3 "$CHECKS" ledger --rows-json "${src[@]}" <<< "$rows"
}

# ledger_cursor <ledger-json> — a file's committed position: the ledger's
# cursor, the end byte of the highest SOURCE_COMMITTED row, or `-`.
ledger_cursor() {
  python3 -c 'import json,sys; c=json.loads(sys.argv[1])["committed_end"]; print("-" if c is None else c)' "$1"
}

check_commit_rule() { # <point> <dir> <group> <when>
  local point="$1" dir="$2" group="$3" when="$4" committed="" ledger
  # The broker's offset first, then the ledger: the engine is stopped, so
  # neither moves, and a failed read stops the point.
  if [[ "$SOURCE" == "kafka" ]]; then
    committed="$(group_committed_settled "$group" 2> "$dir/group-query.err")" \
      || fail "$point ($when): the commit rule was not checked: $(cat "$dir/group-query.err")"
  fi
  ledger="$(ledger_json "$dir" "$group" 2> "$dir/ledger-query.err")" \
    || fail "$point ($when): the ledger was not read: $(cat "$dir/ledger-query.err")"
  [[ "$SOURCE" == "kafka" ]] || committed="$(ledger_cursor "$ledger")"
  printf '%s %s committed=%s ledger=%s\n' "$when" "$point" "$committed" "$ledger" >> "$dir/commit-rule.log"
  python3 "$CHECKS" commit-rule "$committed" "$ledger" "$point" "$when" "$SOURCE" >> "$dir/commit-rule.log" 2>&1 \
    || fail "$(tail -n 1 "$dir/commit-rule.log")"
  if [[ "$when" == "at the kill" ]]; then
    python3 "$CHECKS" kill-state "$point" "$ledger" >> "$dir/commit-rule.log" 2>&1 \
      || fail "$(tail -n 1 "$dir/commit-rule.log")"
  fi
}

drained() { # <dir> <group> — succeeds once every record is committed
  local dir="$1" group="$2" committed="" ledger want last
  # The engine is running, so a failed query is not retried here: it is
  # "not drained yet", and engine_drain's bounded wait reports it.
  if [[ "$SOURCE" == "kafka" ]]; then
    # The broker holds the next offset to read, RECORDS when drained; the
    # ledger's last row ends at RECORDS - 1.
    if ! committed="$(group_committed "$group" 2>&1)"; then
      echo "the group's committed offset was not read: $committed"
      return 1
    fi
    want="$RECORDS"
    last=$((RECORDS - 1))
  else
    # A file's committed position is the end byte of the ledger's last row,
    # the file's size when drained.
    want="$SOURCE_BYTES"
    last="$SOURCE_BYTES"
  fi
  if ! ledger="$(ledger_json "$dir" "$group" 2>&1)"; then
    echo "the ledger was not read: $ledger"
    return 1
  fi
  [[ "$SOURCE" == "kafka" ]] || committed="$(ledger_cursor "$ledger")"
  if [[ "$committed" == "$want" ]] \
    && python3 -c 'import json,sys; s=json.loads(sys.argv[1]); sys.exit(0 if s["committed_end"] == int(sys.argv[2]) and not s["in_flight"] else 1)' \
      "$ledger" "$last"; then
    return 0
  fi
  echo "committed=$committed of $want; ledger=$ledger"
  return 1
}

# ---------------------------------------------------------------------------
# Faults: on while the engine runs armed towards its failpoint, off before
# the kill is awaited.
# ---------------------------------------------------------------------------
wait_healthy() { # <container> — healthy, or the container's last state
  local deadline=$((SECONDS + 60)) status=""
  while (( SECONDS < deadline )); do
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$1" 2>&1)"
    [[ "$status" == "healthy" || "$status" == "running" ]] && return 0
    sleep 1
  done
  fail "$1 is not healthy 60s after it was started: $status"
}

fault_on() { # <dir>
  local dir="$1"
  case "$FAULT" in
    store_outage)
      docker stop "$MINIO_CONTAINER" > /dev/null || fail "could not stop the store $MINIO_CONTAINER"
      log "fault on: the store is stopped"
      ;;
    enospc)
      mkdir -p "$dir/work"
      as_root mount -t tmpfs -o "size=16k,uid=$(id -u),gid=$(id -g),mode=0700" tmpfs "$dir/work" \
        || fail "could not mount a tmpfs at $dir/work; the enospc leg needs root or passwordless sudo"
      echo "$dir/work" >> "$WORKDIR/mounts"
      dd if=/dev/zero of="$dir/work/filler" bs=4096 count=4 2> /dev/null \
        || fail "could not fill the 16 KiB tmpfs at $dir/work"
      if printf x >> "$dir/work/probe" 2> /dev/null; then
        fail "the tmpfs at $dir/work is not full: the enospc leg would not exercise ENOSPC"
      fi
      log "fault on: $dir/work is a full 16 KiB tmpfs"
      ;;
  esac
}

fault_off() { # <dir>
  local dir="$1"
  case "$FAULT" in
    store_outage)
      docker start "$MINIO_CONTAINER" > /dev/null || fail "could not start the store $MINIO_CONTAINER"
      wait_healthy "$MINIO_CONTAINER"
      log "fault off: the store is back"
      ;;
    enospc)
      rm -f "$dir/work/filler" "$dir/work/probe"
      log "fault off: space is free in $dir/work"
      ;;
  esac
}

# fault_hold <pid> <log> — wait while the fault holds: until the engine dies
# (the point fired with the fault on) or FAULT_SECONDS pass with the engine
# alive. A point that waits on the store or the disk is then reached only
# after fault_off. An engine still alive must have FELT the fault, as its log
# shows: a leg whose fault never reached the engine proves nothing.
fault_hold() {
  local pid="$1" logfile="$2" deadline=$((SECONDS + FAULT_SECONDS)) felt pattern
  case "$FAULT" in
    store_outage) pattern='ERROR.*batch failed.*(create_bucket|put_object|upload|dispatch failure)' ;;
    enospc) pattern='No space left on device' ;;
  esac
  while kill -0 "$pid" 2> /dev/null && (( SECONDS < deadline )); do
    sleep 0.2
  done
  if kill -0 "$pid" 2> /dev/null; then
    felt="$(grep -cE "$pattern" "$logfile" || true)"
    (( felt > 0 )) \
      || fail "the engine ran $FAULT_SECONDS s with the $FAULT fault on and its log shows no failure from it (looked for /$pattern/), so this leg proved nothing. Last log lines:
$(tail -n 10 "$logfile")"
    log "the engine ran $FAULT_SECONDS s with the fault on, logged $felt failure(s) from it, and did not reach the failpoint"
  else
    log "the failpoint fired with the fault on"
  fi
}

declare -a SUMMARY=()
RAN_FILE="$WORKDIR/ran-points"
: > "$RAN_FILE"
sweep_started=$SECONDS

for point in "${SELECTED[@]}"; do
  started=$SECONDS
  SCENARIO="engine-sweep/$point"
  dir="$WORKDIR/points/$point"
  mkdir -p "$dir"
  group="vtop-sweep-$RUN_ID-$point"
  prefix="sweep-$RUN_ID/$point"
  if [[ "$BACKEND" == "postgres" ]]; then
    POINT_DB="vtop_sweep_$point"
    pg_psql postgres "DROP DATABASE IF EXISTS $POINT_DB" > /dev/null
    pg_psql postgres "CREATE DATABASE $POINT_DB" > /dev/null
    # The engine reads the URL from the environment, as a deployment does.
    export VTOP_STATE_STORE="postgres://$PG_USER:$PG_PASSWORD@$PG_HOSTPORT/$POINT_DB?sslmode=disable"
    emit_engine_config "$dir" "$group" "$prefix" "$point" "{ env: VTOP_STATE_STORE }"
    "$VTOPCTL" migrate --config "$dir/config.yaml" > "$dir/migrate.log" 2>&1 \
      || fail "vtopctl migrate failed on $POINT_DB: $(tail -n 5 "$dir/migrate.log")"
  else
    emit_engine_config "$dir" "$group" "$prefix" "$point" "\"sqlite://$dir/state.db\""
  fi

  # 1. Run armed until the failpoint kills the engine, with the fault on
  #    while it runs towards the line.
  fault_on "$dir"
  engine_start "$dir/config.yaml" "$dir/kill.log" "$point"
  if [[ "$FAULT" != "none" ]]; then
    fault_hold "$ENGINE_PID" "$dir/kill.log"
    fault_off "$dir"
  fi
  engine_wait_killed "$ENGINE_PID" "$point" "$dir/kill.log" "$POINT_TIMEOUT"
  log "killed by SIGKILL at the failpoint"

  # 2. The commit rule at the instant of the kill, before anything restarts.
  check_commit_rule "$point" "$dir" "$group" "at the kill"

  # 3. Restart without the failpoint and drain to the end of the source.
  engine_start "$dir/config.yaml" "$dir/restart.log"
  engine_drain "$ENGINE_PID" "$dir/restart.log" "$POINT_TIMEOUT" drained "$dir" "$group"
  check_commit_rule "$point" "$dir" "$group" "after the restart"

  # 4. No hole, by the engine's own checkers and by an independent count.
  assert_archive_coverage "$dir/config.yaml" "$dir/coverage.log"
  readback_records "$BUCKET" "$prefix" "$dir/readback"
  counts="$(python3 "$CHECKS" readback "$dir/readback" "$RECORDS" "$RECORD_PREFIX")"
  echo "$counts" > "$dir/readback.json"
  read -r missing extra unexpected < <(python3 -c 'import json,sys; c=json.loads(sys.argv[1]); print(len(c["missing"]), c["extra_copies"], len(c["unexpected"]))' "$counts")
  (( missing == 0 )) || fail "$missing of $RECORDS records are missing from the store (a hole): $counts"
  (( unexpected == 0 )) || fail "the store holds records the sweep did not produce: $counts"
  allowed="$(allowed_duplicates "$point")"
  (( extra <= allowed )) \
    || fail "$extra duplicate record copies after a kill at $point; at most $allowed are allowed there: $counts"

  echo "$point" >> "$RAN_FILE"
  SUMMARY+=("$(printf '%-36s PASS  %3ss  duplicates %s (allowed %s)' "$point" "$((SECONDS - started))" "$extra" "$allowed")")
  log "PASS in $((SECONDS - started))s: $RECORDS records archived, $extra duplicate copies (allowed $allowed)"
  if [[ "$FAULT" == "enospc" ]]; then
    as_root umount "$dir/work" || fail "could not unmount $dir/work"
  fi
  if [[ "${CHAOS_KEEP:-0}" != "1" ]]; then
    rm -rf "$dir/work" "$dir/readback"
  fi
done

SCENARIO="engine-sweep"
# F3: a full sweep ran every point the binary lists.
if [[ "$POINTS_KNOB" == "all" ]]; then
  python3 "$CHECKS" complete "$WORKDIR/listed-points" "$RAN_FILE" \
    || fail "the sweep is incomplete"
fi
echo
echo "=== engine crash sweep ($SOURCE source, $BACKEND ledger, fault: $FAULT): ${#SUMMARY[@]} of ${#LISTED[@]} points, $RECORDS records each, $((SECONDS - sweep_started))s ==="
printf '%s\n' "${SUMMARY[@]}"
