#!/usr/bin/env bash
# Disaster-recovery drill: the bucket is lost, with no replica (#735,
# criterion E4).
#
# An engine archives a Kafka topic into a store, with its ledger in PostgreSQL.
# The store's data is destroyed while the engine runs, the topic goes on
# receiving records, and part of what the source held has meanwhile expired (the
# broker's retention). The ledger is intact and says every batch is committed.
# The drill follows docs/operate/disaster-recovery.md, "The bucket is lost":
#
#   1. find out what is gone: `vtopctl audit-coverage --check-storage` and
#      `vtopctl scrub` name every committed object and manifest that is missing;
#   2. keep the old ledger as the record of the loss;
#   3. a new store and a new ledger, and the consumer group reset to the start
#      of the topic: the engine archives again everything the source still holds.
#
# Pass criteria:
#   - the loss is reported exactly: the audit and the scrub both fail and name
#     as many missing objects as there were committed batches, none more;
#   - everything the source still holds is archived again, with no hole: the new
#     bucket holds every record from the broker's first retained offset on;
#   - what the source no longer held is lost, and the drill reports the loss
#     exactly: the records missing from the new archive are exactly the ones
#     the old ledger committed below the first retained offset, no fewer
#     and no more;
#   - the new ledger passes `audit-coverage --check-storage`, `scrub` and
#     `audit verify`.
#
# Measured and bounded (docs/operate/disaster-recovery.md): RTO, from the first
# recovery step until every retained record is archived again. The RPO is the
# source's retention: the records the source still held are recovered, and the
# rest are lost and reported.
#
# Usage: bucket-lost.sh <vtopctl> [--handover | --check | --teardown]
# shellcheck source=scripts/dr-drill/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
# shellcheck disable=SC2034  # DR_PAGE_SECTION and DR_HANDOVER_VARS are read by lib.sh
dr_init bucket-lost "$@"

VTOP_DR_BUCKET_LOST_RTO_S=120
dr_check_doc VTOP_DR_BUCKET_LOST_RTO_S=$VTOP_DR_BUCKET_LOST_RTO_S

BUCKET=dr-bucket-lost
REC=dr-rec-
BEFORE=120  # archived before the loss
DURING=60   # produced while the store is gone
EXPIRED=40  # the first records, which the source no longer holds at recovery
TOTAL=$((BEFORE + DURING))
BATCH=20
DB_OLD=dr_bl_old
DB_NEW=dr_bl_new
TOPIC="dr-bucket-lost-$DR_RUN_ID"
GROUP="dr-bucket-lost-$DR_RUN_ID"
CFG="$DR_WORKDIR/config.yaml"
DIGESTS="$DR_WORKDIR/digests"
PREFIX="bucket-lost"

DR_PAGE_SECTION="The bucket is lost"
DR_HANDOVER_VARS=(OLD_ROWS STORE_PORT TOPIC GROUP)

committed_rows() { dr_sql_num "SELECT count(*) FROM batches WHERE state='source_committed'"; }
committed_records() { dr_sql_num "SELECT COALESCE(sum(record_count),0) FROM batches WHERE state='source_committed'"; }
open_rows() { dr_sql_num "SELECT count(*) FROM batches WHERE state NOT IN ('source_committed','superseded','failed','replay_required')"; }
drained() { (($(committed_records) >= $1)) && (($(open_rows) == 0)); }

lab() {
  dr_minio_start store
  STORE_PORT="${DR_PORT[minio-store]}"
  dr_bucket_make store "$BUCKET"
  dr_pg_start ledger
  dr_kafka_start k
  dr_pg_admin "CREATE DATABASE $DB_OLD"
  dr_pg_db "$DB_OLD"
  mkdir -p "$DIGESTS"
  dr_config "$CFG" source=kafka bootstrap="$DR_KAFKA_BOOTSTRAP" topic="$TOPIC" group="$GROUP" \
    endpoint="$(dr_s3_endpoint store)" bucket="$BUCKET" prefix="$PREFIX" work="$DR_WORKDIR/work" digests="$DIGESTS" batch="$BATCH"
  dr_ctl migrate migrate --config "$CFG"

  # ---- a working archive -----------------------------------------------------------
  dr_kafka_topic "$TOPIC"
  dr_kafka_produce "$TOPIC" 0 "$BEFORE" "$REC" before
  dr_engine_start "$CFG"
  wait_until 120 "the first $BEFORE records to be archived" drained "$BEFORE"
  OLD_ROWS="$(committed_rows)"
  log "before the loss: $OLD_ROWS committed batch(es) holding $BEFORE records"

  # ---- the disaster -------------------------------------------------------------------
  log "disaster: the store and its data are destroyed while the engine runs"
  dr_kill minio-store
  dr_kafka_produce "$TOPIC" "$BEFORE" "$DURING" "$REC" during
  sleep 8
  log "the source's retention expires its first $EXPIRED records"
  dr_kafka_trim "$TOPIC" "$EXPIRED"
  dr_engine_stop_any
  log "the engine was stopped (exit $DR_RC): what it had not archived is read again on the next start"
  # The replacement store is the lab's infrastructure, not a step of the page: a
  # new, empty store at the old address, as the page's step 3 asks for.
  log "a new, empty store at the old address"
  DR_MINIO_PORT="$STORE_PORT" dr_minio_start store
  dr_bucket_make store "$BUCKET"
}

handover_text() {
  echo "  The store's data is destroyed while the engine ran; the engine is stopped (it exited $DR_RC on SIGTERM: expected)."
  echo "  The ledger (database $DB_OLD) is intact and says $OLD_ROWS batches are committed ($BEFORE records). The source,"
  echo "  Kafka topic $TOPIC (consumer group $GROUP), then received $DURING more records ($TOTAL in all), and its retention"
  echo "  has expired the first $EXPIRED, which the broker no longer holds."
  echo "  A new, empty store is up at the old address with the bucket $BUCKET (the lab's provisioning, the page's step 3)."
  echo "  Engine configuration: $CFG (unchanged); digest directory $DIGESTS."
  echo "  Name the databases as the drill does, so that the configuration and --check find them: keep the old ledger as"
  echo "  ${DB_OLD}_lost_bucket, and make the new one $DB_NEW."
}

recover() {
  log "recovery step 1: find out what is gone"
  dr_ctl_rc found-audit audit-coverage --config "$CFG" --digest-dir "$DIGESTS" --check-storage
  crit_eq "the audit of a lost bucket exits non-zero" "$([[ $DR_RC -ne 0 ]] && echo nonzero || echo zero)" nonzero
  dr_ctl_rc found-scrub scrub --config "$CFG" --digest-dir "$DIGESTS"
  crit_eq "the scrub of a lost bucket exits non-zero" "$([[ $DR_RC -ne 0 ]] && echo nonzero || echo zero)" nonzero
  crit_test "the scrub reports exactly the $OLD_ROWS committed batches as missing" "$(grep '^results:' "$DR_LOGS/found-scrub.log" || true)" \
    grep -q "missing=$OLD_ROWS " "$DR_LOGS/found-scrub.log"
  local missing_objects
  missing_objects="$(grep -c '^FAIL storage' "$DR_LOGS/found-audit.log" || true)"
  crit_eq "the audit names an object and a manifest for each committed batch, and no more" "$missing_objects" "$((OLD_ROWS * 2))"

  # ---- step 4: keep the old ledger as the record of the loss ---------------------------
  log "recovery: keep the old ledger; a new ledger"
  T0=$(now_us)
  docker exec "${DR_CONT[pg-$DR_PG]}" pg_dump -U vtop -d "$DB_OLD" --format=custom > "$DR_WORKDIR/old-ledger.dump"
  [[ -s "$DR_WORKDIR/old-ledger.dump" ]] || fail "pg_dump wrote nothing"
  dr_pg_admin "ALTER DATABASE $DB_OLD RENAME TO ${DB_OLD}_lost_bucket"
  dr_pg_admin "CREATE DATABASE $DB_NEW"
  dr_pg_db "$DB_NEW"
  dr_ctl migrate-after migrate --config "$CFG"
  rm -rf "${DIGESTS:?}" "${DR_WORKDIR:?}/work"
  mkdir -p "$DIGESTS"

  # ---- step 5: archive again what the source still holds --------------------------------
  dr_kafka_tool kafka-consumer-groups.sh --group "$GROUP" --topic "$TOPIC" --reset-offsets --to-earliest --execute \
    > "$DR_LOGS/reset.log" 2>&1 || fail "could not reset the consumer group; see $DR_LOGS/reset.log"
  crit_test "the group is reset to the first retained offset ($EXPIRED)" "$(tail -n 2 "$DR_LOGS/reset.log" | tr '\n' ' ')" \
    grep -qE "[[:space:]]$EXPIRED\$" "$DR_LOGS/reset.log"
  dr_engine_start "$CFG"
  wait_until 180 "the engine to archive the $((TOTAL - EXPIRED)) retained records" drained "$((TOTAL - EXPIRED))"
  RTO_MS=$(ms_since "$T0")
  dr_engine_stop
}

check() {
  dr_pg_db "$DB_NEW"
  local retained=$((TOTAL - EXPIRED)) first_retained lost_from_ledger
  dr_readback store "$BUCKET" "$PREFIX" "$DR_WORKDIR/rb"
  dr_count "$DR_WORKDIR/rb" "$REC" "$TOTAL"
  crit_eq "records archived again: every one the source still held" "$DR_UNIQUE" "$retained"
  crit_eq "records lost: exactly the ones the source no longer held" "$DR_MISSING" "$EXPIRED"
  crit_eq "the first records lost are the first records expired" "$DR_MISSING_FIRST" "0,1,2,3,4"
  crit_eq "no record is archived twice" "$DR_DUPLICATES" 0
  # The loss report from the old ledger, the record the page says to keep: the
  # committed batches whose records are below the first offset the broker still
  # holds, which the page's command reads.
  first_retained="$(dr_kafka_tool kafka-get-offsets.sh --topic "$TOPIC" --time -2 | awk -F: '{print $3}')"
  crit_eq "the first retained offset, as the page's command reads it" "$first_retained" "$EXPIRED"
  lost_from_ledger="$(docker exec "${DR_CONT[pg-$DR_PG]}" psql --no-psqlrc -U vtop -d "${DB_OLD}_lost_bucket" -tA -c \
    "SELECT COALESCE(sum(record_count),0) FROM batches WHERE state='source_committed' AND source_type='kafka' AND ((progress_end_json::json)->>'end_offset')::bigint < $first_retained" 2> "$DR_LOGS/loss-report.err" || true)"
  log "the old ledger's loss report: ${lost_from_ledger:-unreadable} record(s) committed below offset $EXPIRED"
  crit_eq "the old ledger's loss report names exactly the records that are lost" "${lost_from_ledger:-unreadable}" "$DR_MISSING"
  dr_checks recovered "$CFG" "$DIGESTS"
  crit_eq "the new ledger: audit-coverage --check-storage exits 0" "$DR_AUDIT_RC" 0
  crit_eq "the new ledger: scrub exits 0" "$DR_SCRUB_RC" 0
  crit_eq "the new ledger: audit verify exits 0" "$DR_VERIFY_RC" 0
  crit_rto "RTO, from the first recovery step to the retained records archived again" "$RTO_MS" "$VTOP_DR_BUCKET_LOST_RTO_S"

  dr_set rto_ms "$RTO_MS"
  dr_set committed_batches_lost_from_the_store "$OLD_ROWS"
  dr_set records_total "$TOTAL"
  dr_set records_archived_again "$DR_UNIQUE"
  dr_set records_lost "$DR_MISSING"
  dr_set records_archived_twice "$DR_DUPLICATES"
  dr_set old_ledger_loss_report_records "${lost_from_ledger:-unreadable}"
  dr_check_summary "$DR_MISSING"
}

dr_run_drill
