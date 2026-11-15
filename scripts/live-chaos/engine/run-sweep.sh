#!/usr/bin/env bash
# Engine crash sweep (#725): kill the archive engine with SIGKILL at every
# failpoint, restart it, and check that nothing was lost and nothing was
# committed at the source before it was verified.
#
# System model: one crash-recovery engine process (`vtopctl run`), a real
# Kafka broker as the source, a real S3 store (the lab's MinIO) and a SQLite
# ledger; a partially synchronous network; honest processes. The fault is a
# SIGKILL of the engine at one line, chosen by a failpoint
# (crates/vtop-cli/src/failpoints.rs) in a build with the `failpoints`
# feature. Nothing else fails.
#
# Invariants, checked for every point:
#   safety   the commit rule: the consumer group's committed offset on the
#            broker never passes the end of the highest range the ledger
#            records as VERIFIED (checked at the kill, and again after the
#            restart has drained)
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
# Each point gets its own consumer group, ledger, work directory and object
# prefix, over one topic the harness seeds once, so a failure names its point
# and keeps its logs (CHAOS_KEEP=1 keeps every point's).
#
# Needs: a `failpoints` build of vtopctl (CHAOS_VTOPCTL, or
# target/$CHAOS_PROFILE/vtopctl); a running Kafka container with the Apache
# CLI tools (ENGINE_SWEEP_KAFKA_CONTAINER, found by image when unset); MinIO
# from docker-compose.yml (`docker compose up -d minio`), reached by the
# engine at ENGINE_SWEEP_S3_ENDPOINT and by the harness through
# `docker compose run minio-init`; docker, python3, timeout, tar.
#
#   cargo build -p vtop-cli --features failpoints --bin vtopctl
#   CHAOS_PROFILE=debug scripts/live-chaos/engine/run-sweep.sh
#   ENGINE_SWEEP_POINTS=before_source_commit,after_source_commit_before_ledger ...
#
# Knobs (validated below):
#   ENGINE_SWEEP_POINTS       `all` (default: every point the binary lists,
#                             and the run fails if one was not run) or a
#                             comma-separated subset
#   ENGINE_SWEEP_RECORDS      records produced (default 50)
#   ENGINE_SWEEP_BATCH_RECORDS records per batch (default 20)
#   ENGINE_SWEEP_POINT_TIMEOUT_SECONDS bound on each wait (default 90)
#   ENGINE_SWEEP_KAFKA_BOOTSTRAP  the broker as the engine sees it
#                             (default localhost:9092)
#   ENGINE_SWEEP_KAFKA_CONTAINER  container with /opt/kafka/bin
#   ENGINE_SWEEP_S3_ENDPOINT  the store as the engine sees it
#                             (default http://127.0.0.1:9000)
#   ENGINE_SWEEP_BUCKET       bucket (default telemetry-sweep; created by the
#                             engine)
#
# Budget: 9 to 12 s a point with a debug build, measured locally (238 s for
# all 21 points); the measurement is in docs/LIVE_CHAOS_VALIDATION.md. The
# workflow's per-point wait is bounded by ENGINE_SWEEP_POINT_TIMEOUT_SECONDS.
#
# Not covered here (#725 PR 2): PostgreSQL, the file source, a store outage
# before the kill, ENOSPC.
set -euo pipefail
umask 077
export CHAOS_NO_CERTS=1
SCENARIO="engine-sweep"
# shellcheck source=scripts/live-chaos/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
CHECKS="$SCRIPT_DIR/engine/sweep_checks.py"

POINTS_KNOB="${ENGINE_SWEEP_POINTS:-all}"
RECORDS="${ENGINE_SWEEP_RECORDS:-50}"
BATCH_RECORDS="${ENGINE_SWEEP_BATCH_RECORDS:-20}"
POINT_TIMEOUT="${ENGINE_SWEEP_POINT_TIMEOUT_SECONDS:-90}"
KAFKA_BOOTSTRAP="${ENGINE_SWEEP_KAFKA_BOOTSTRAP:-localhost:9092}"
KAFKA_CONTAINER="${ENGINE_SWEEP_KAFKA_CONTAINER:-}"
S3_ENDPOINT="${ENGINE_SWEEP_S3_ENDPOINT:-http://127.0.0.1:9000}"
BUCKET="${ENGINE_SWEEP_BUCKET:-telemetry-sweep}"
RECORD_PREFIX="sweep-record-"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

require_integer_in_range ENGINE_SWEEP_RECORDS "$RECORDS" 1 100000
require_integer_in_range ENGINE_SWEEP_BATCH_RECORDS "$BATCH_RECORDS" 1 "$RECORDS"
require_integer_in_range ENGINE_SWEEP_POINT_TIMEOUT_SECONDS "$POINT_TIMEOUT" 5 3600
[[ "$BUCKET" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]] \
  || fail "ENGINE_SWEEP_BUCKET is not a valid bucket name: '$BUCKET'"
[[ "$POINTS_KNOB" =~ ^[a-z_,]+$ ]] \
  || fail "ENGINE_SWEEP_POINTS must be 'all' or comma-separated point names, got '$POINTS_KNOB'"
for tool in docker python3 timeout tar; do
  require_command "$tool" "The engine sweep needs $tool on PATH."
done
[[ -x "$VTOPCTL" ]] \
  || fail "missing $VTOPCTL: build it with 'cargo build -p vtop-cli --features failpoints --bin vtopctl' (set CHAOS_PROFILE or CHAOS_VTOPCTL to where it is)"

if [[ -z "$KAFKA_CONTAINER" ]]; then
  KAFKA_CONTAINER="$(docker ps --filter "ancestor=apache/kafka:4.3.1" --format '{{.ID}}' | head -1)"
fi
[[ -n "$KAFKA_CONTAINER" ]] \
  || fail "no running apache/kafka:4.3.1 container; start one or set ENGINE_SWEEP_KAFKA_CONTAINER"
kafka_tool() { # <tool> <args...> — an Apache Kafka CLI tool inside the broker's container
  local tool="$1"
  shift
  timeout 120 docker exec -i "$KAFKA_CONTAINER" "/opt/kafka/bin/$tool" \
    --bootstrap-server localhost:9092 "$@"
}

init_workdir
RUN_ID="$(date +%s)-$$"
TOPIC="vtop-sweep-$RUN_ID"

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
# Seed the topic once. Records are numbered, so a hole names the record.
# ---------------------------------------------------------------------------
kafka_tool kafka-topics.sh --create --if-not-exists --topic "$TOPIC" \
  --partitions 1 --replication-factor 1 > /dev/null
for ((i = 0; i < RECORDS; i++)); do
  printf '%s%06d sweep payload for the engine crash sweep %s\n' "$RECORD_PREFIX" "$i" "$RUN_ID"
done | kafka_tool kafka-console-producer.sh --topic "$TOPIC" > /dev/null
end_offset="$(kafka_tool kafka-get-offsets.sh --topic "$TOPIC" | awk -F: '{print $3}')"
[[ "$end_offset" == "$RECORDS" ]] \
  || fail "seeded $TOPIC with $RECORDS records, but its end offset is '$end_offset'"
log "topic $TOPIC holds $RECORDS records; bucket $BUCKET at $S3_ENDPOINT; kafka at $KAFKA_BOOTSTRAP"

emit_engine_config() { # <dir> <group> <prefix> <point>
  local dir="$1" group="$2" prefix="$3" point="$4" multipart=""
  # The point inside a multipart upload needs one: a one-byte threshold
  # sends every object as a multipart upload (one part at this size).
  if [[ "$point" == "mid_object_put" ]]; then
    multipart="  multipart_threshold_bytes: 1"
  fi
  cat > "$dir/config.yaml" <<EOF
engine:
  name: vtop-sweep
  tenant: sweep
  state_store: "sqlite://$dir/state.db"
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
  kafka:
    enabled: true
    bootstrap_servers: ["$KAFKA_BOOTSTRAP"]
    consumer_group: "$group"
    topic_include_regex: "^${TOPIC}\$"
    auto_offset_reset: earliest
    enable_auto_commit: false
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

group_committed() { # <group> -> the group's committed offset, or -
  local out
  out="$(kafka_tool kafka-consumer-groups.sh --describe --group "$1" 2>/dev/null || true)"
  awk -v t="$TOPIC" '$2 == t && $3 == "0" { print $4; found = 1 } END { if (!found) print "-" }' <<< "$out"
}

check_commit_rule() { # <point> <dir> <group> <when>
  local point="$1" dir="$2" group="$3" when="$4" committed ledger
  committed="$(group_committed "$group")"
  ledger="$(python3 "$CHECKS" ledger "$dir/state.db" "$TOPIC" 0 "$group")"
  printf '%s %s committed=%s ledger=%s\n' "$when" "$point" "$committed" "$ledger" >> "$dir/commit-rule.log"
  python3 "$CHECKS" commit-rule "$committed" "$ledger" "$point" "$when" >> "$dir/commit-rule.log" 2>&1 \
    || fail "$(tail -n 1 "$dir/commit-rule.log")"
}

drained() { # <dir> <group> — succeeds once every record is committed
  local dir="$1" group="$2" committed ledger
  committed="$(group_committed "$group")"
  ledger="$(python3 "$CHECKS" ledger "$dir/state.db" "$TOPIC" 0 "$group")"
  if [[ "$committed" == "$RECORDS" ]] \
    && python3 -c 'import json,sys; s=json.loads(sys.argv[1]); sys.exit(0 if s["committed_end"] == int(sys.argv[2]) - 1 and not s["in_flight"] else 1)' \
      "$ledger" "$RECORDS"; then
    return 0
  fi
  echo "group committed=$committed of $RECORDS; ledger=$ledger"
  return 1
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
  emit_engine_config "$dir" "$group" "$prefix" "$point"

  # 1. Run armed until the failpoint kills the engine.
  engine_start "$dir/config.yaml" "$dir/kill.log" "$point"
  engine_wait_killed "$ENGINE_PID" "$point" "$dir/kill.log" "$POINT_TIMEOUT"
  log "killed by SIGKILL at the failpoint"

  # 2. The commit rule at the instant of the kill, before anything restarts.
  check_commit_rule "$point" "$dir" "$group" "at the kill"

  # 3. Restart without the failpoint and drain to the end of the topic.
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
echo "=== engine crash sweep: ${#SUMMARY[@]} of ${#LISTED[@]} points, $RECORDS records each, $((SECONDS - sweep_started))s ==="
printf '%s\n' "${SUMMARY[@]}"
