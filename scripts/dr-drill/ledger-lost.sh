#!/usr/bin/env bash
# Disaster-recovery drill: the ledger is lost and there is no backup of it,
# while the bucket is intact (#735, criterion E4).
#
# An engine archives a source into a store, with its ledger in PostgreSQL.
# The ledger's database is destroyed, and the engine with it. No backup exists.
# The drill follows docs/operate/disaster-recovery.md, "The ledger is lost, with
# no backup": a new database, `vtopctl migrate`, the same configuration, start.
#
# It runs three times, once for each way a source keeps its place:
#
#   kafka          the consumer group's committed offset is on the broker, so the
#                  new ledger starts where the lost one ended. Pass criteria:
#                  every record the topic ever held is in the archive, none
#                  twice; the new ledger proves exactly the records archived
#                  since the loss, and the drill reports the number it can no
#                  longer prove.
#   kafka-rebuild  the same, with the page's extra step for a source that still
#                  holds its history: the consumer group is reset to the start
#                  of the topic before the engine first starts on the new
#                  ledger. The engine reads everything again and finds each
#                  object at its key, so nothing is written twice and the new
#                  ledger proves every record. Pass criteria: no record missing,
#                  none twice, the ledger proves them all.
#   file           a file's position is seeded from the ledger. The new ledger has
#                  none, so the file is read again from its start; each object is
#                  found at its key and reused. Pass criteria: every record is in
#                  the archive, the records archived before the loss twice at
#                  most, and the new ledger proves them all.
#
# Measured and bounded (docs/operate/disaster-recovery.md):
#   RPO  the archive loses no record. What is lost is the ledger's history of
#        the batches already archived: reported as batches and records.
#   RTO  from the first recovery step (the new database) until the engine
#        commits a batch again.
#
# Usage: ledger-lost.sh <vtopctl> [--handover | --check | --teardown] [--leg file|kafka|kafka-rebuild]
# shellcheck source=scripts/dr-drill/lib.sh
# shellcheck disable=SC2034  # DR_LEGS, DR_PAGE_SECTION and DR_HANDOVER_VARS are read by lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
dr_init ledger-lost "$@"

VTOP_DR_LEDGER_LOST_RTO_S=60
dr_check_doc VTOP_DR_LEDGER_LOST_RTO_S=$VTOP_DR_LEDGER_LOST_RTO_S

BUCKET=dr-ledger-lost
REC=dr-rec-
BEFORE=120 # records archived before the loss
DURING=60  # records the source receives while the ledger is gone

DR_LEGS=(file kafka kafka-rebuild)
DR_PAGE_SECTION="The ledger is lost, with no backup"
DR_HANDOVER_VARS=(PRE_ROWS PRE_RECORDS)

committed_rows() { dr_sql_num "SELECT count(*) FROM batches WHERE state='source_committed'"; }
committed_records() { dr_sql_num "SELECT COALESCE(sum(record_count),0) FROM batches WHERE state='source_committed'"; }
open_rows() { dr_sql_num "SELECT count(*) FROM batches WHERE state NOT IN ('source_committed','superseded')"; }
# The engine is drained when the ledger holds the records it should and nothing
# is in flight.
committed_records_ge() { (($(committed_records) >= $1)) && (($(open_rows) == 0)); }
committed_rows_gt() { (($(committed_rows) > $1)); }
gen() { local i; for ((i = $1; i < $2; i++)); do printf '%s%06d payload for the disaster-recovery drill %s\n' "$REC" "$i" "$3"; done; }

# ctx <leg>: the leg's names, the same in every phase.
ctx() {
  L_LEG="$1"
  L_SRC="${1%%-*}"
  L_DB="dr_ll_${1//-/_}"
  L_PREFIX="ledger-lost/$1"
  L_CFG="$DR_WORKDIR/$1.yaml"
  L_DIGESTS="$DR_WORKDIR/$1-digests"
  L_INPUT="$DR_WORKDIR/$1-input.log"
  L_TOTAL=$((BEFORE + DURING))
  L_TOPIC="dr-ledger-lost-$DR_RUN_ID-$1"
  L_GROUP="dr-ledger-lost-$DR_RUN_ID-$1"
}

lab_shared() {
  dr_minio_start store
  dr_bucket_make store "$BUCKET"
  dr_pg_start ledger
  dr_kafka_start k
}

# lab <leg>: a working archive, then the disaster.
lab() {
  ctx "$1"
  log "=== leg: $L_LEG"
  dr_pg_admin "CREATE DATABASE $L_DB"
  dr_pg_db "$L_DB"
  mkdir -p "$L_DIGESTS"
  if [[ "$L_SRC" == kafka ]]; then
    dr_config "$L_CFG" source=kafka bootstrap="$DR_KAFKA_BOOTSTRAP" topic="$L_TOPIC" group="$L_GROUP" \
      endpoint="$(dr_s3_endpoint store)" bucket="$BUCKET" prefix="$L_PREFIX" work="$DR_WORKDIR/$L_LEG-work" digests="$L_DIGESTS"
    dr_kafka_topic "$L_TOPIC"
    dr_kafka_produce "$L_TOPIC" 0 "$BEFORE" "$REC" before
  else
    dr_config "$L_CFG" source=file input="$L_INPUT" \
      endpoint="$(dr_s3_endpoint store)" bucket="$BUCKET" prefix="$L_PREFIX" work="$DR_WORKDIR/$L_LEG-work" digests="$L_DIGESTS"
    gen 0 "$BEFORE" before > "$L_INPUT"
  fi
  dr_ctl "$L_LEG-migrate" migrate --config "$L_CFG"

  # ---- a working archive ---------------------------------------------------------
  dr_engine_start "$L_CFG"
  wait_until 120 "the engine to archive the first $BEFORE records" committed_records_ge "$BEFORE"
  dr_engine_stop
  PRE_ROWS="$(committed_rows)"
  PRE_RECORDS="$(committed_records)"
  dr_ctl "$L_LEG-audit-verify-before" audit verify --config "$L_CFG"
  log "before the loss: $PRE_ROWS committed batch(es), $PRE_RECORDS record(s); audit trail: $(grep -E '^audit chain' "$DR_LOGS/$L_LEG-audit-verify-before.log")"
  dr_readback store "$BUCKET" "$L_PREFIX" "$DR_WORKDIR/$L_LEG-rb-before"
  dr_count "$DR_WORKDIR/$L_LEG-rb-before" "$REC" "$BEFORE"
  crit_eq "$L_LEG: the archive holds every record before the loss" "$DR_UNIQUE" "$BEFORE"

  # ---- the disaster ----------------------------------------------------------------
  log "disaster: the ledger's database is destroyed; there is no backup"
  dr_pg_admin "DROP DATABASE $L_DB WITH (FORCE)"
  rm -rf "${L_DIGESTS:?}" "${DR_WORKDIR:?}/${L_LEG:?}-work"
  mkdir -p "$L_DIGESTS"
  # The source goes on receiving while the ledger is gone.
  if [[ "$L_SRC" == kafka ]]; then
    dr_kafka_produce "$L_TOPIC" "$BEFORE" "$DURING" "$REC" during
  else
    gen "$BEFORE" "$L_TOTAL" during >> "$L_INPUT"
  fi
}

handover_text() {
  ctx "$1"
  echo "  The ledger's database $L_DB on the ledger server is destroyed, with the digest directory's contents and the"
  echo "  engine's work directory. There is no backup of it. The archive in bucket $BUCKET (prefix $L_PREFIX) is intact"
  echo "  and holds the $BEFORE records archived before the loss; the source then received $DURING more."
  if [[ "$L_SRC" == kafka ]]; then
    echo "  Source: Kafka topic $L_TOPIC, consumer group $L_GROUP."
  else
    echo "  Source: the file $L_INPUT, which holds all $L_TOTAL records."
  fi
  echo "  The engine is stopped. Its configuration is $L_CFG (unchanged); the digest directory is $L_DIGESTS."
  echo "  Create the new ledger under the same database name, $L_DB, so that the configuration and --check find it."
  case "$L_LEG" in
    kafka-rebuild) echo "  This leg exercises the page's extra step for a source that still holds its history: after it, the new ledger must prove every record." ;;
    kafka) echo "  This leg does not take the extra step: the new ledger is expected to prove only the records archived since the loss." ;;
    file) echo "  A file source is read again from its start; the objects already archived are found and reused." ;;
  esac
}

# recover <leg>: the page's steps.
recover() {
  ctx "$1"
  local want_ledger
  log "recovery: a new database, vtopctl migrate, the same config, start"
  T0=$(now_us)
  dr_pg_admin "CREATE DATABASE $L_DB"
  dr_ctl "$L_LEG-migrate-after" migrate --config "$L_CFG"
  mkdir -p "$L_DIGESTS"
  if [[ "$L_LEG" == kafka-rebuild ]]; then
    # The page's step for a source that still holds its history: read it all
    # again into the new ledger. It must come before the engine's first start
    # on the new ledger: the engine will not move a consumer group behind a
    # ledger that already proves the records.
    dr_kafka_tool kafka-consumer-groups.sh --group "$L_GROUP" --topic "$L_TOPIC" --reset-offsets --to-earliest --execute \
      > "$DR_LOGS/$L_LEG-reset.log" 2>&1 || fail "could not reset the consumer group; see $DR_LOGS/$L_LEG-reset.log"
  fi
  dr_engine_start "$L_CFG"
  wait_until 120 "the first commit after the loss" committed_rows_gt 0
  RTO_MS=$(ms_since "$T0")
  if [[ "$L_SRC" == kafka && "$L_LEG" != kafka-rebuild ]]; then want_ledger=$DURING; else want_ledger=$L_TOTAL; fi
  wait_until 120 "the engine to archive the rest of the source" committed_records_ge "$want_ledger"
  dr_engine_stop
  CAUGHT_MS=$(ms_since "$T0")
}

# check <leg>: the pass criteria, from the lab's state alone.
check() {
  ctx "$1"
  dr_pg_db "$L_DB"
  local post_rows post_records unproven
  post_rows="$(committed_rows)"
  post_records="$(committed_records)"
  dr_readback store "$BUCKET" "$L_PREFIX" "$DR_WORKDIR/$L_LEG-rb-after"
  dr_count "$DR_WORKDIR/$L_LEG-rb-after" "$REC" "$L_TOTAL"
  crit_eq "$L_LEG: no record is missing from the archive" "$DR_MISSING" 0
  case "$L_LEG" in
    kafka)
      crit_eq "$L_LEG: no record is archived twice" "$DR_DUPLICATES" 0
      crit_eq "$L_LEG: the new ledger proves the records archived since the loss" "$post_records" "$DURING"
      ;;
    kafka-rebuild)
      crit_eq "$L_LEG: no record is archived twice: the objects were found and reused" "$DR_DUPLICATES" 0
      crit_ge "$L_LEG: the new ledger proves every record" "$post_records" "$L_TOTAL"
      ;;
    file)
      crit_le "$L_LEG: records archived twice are at most those archived before the loss" "$DR_DUPLICATES" "$BEFORE" " records"
      crit_ge "$L_LEG: the new ledger proves every record" "$post_records" "$L_TOTAL"
      ;;
  esac
  dr_checks "$L_LEG-after" "$L_CFG" "$L_DIGESTS"
  crit_eq "$L_LEG: audit-coverage --check-storage exits 0" "$DR_AUDIT_RC" 0
  crit_eq "$L_LEG: scrub exits 0" "$DR_SCRUB_RC" 0
  crit_eq "$L_LEG: audit verify exits 0" "$DR_VERIFY_RC" 0
  crit_rto "$L_LEG: RTO, to the first commit" "$RTO_MS" "$VTOP_DR_LEDGER_LOST_RTO_S"
  crit_rto "$L_LEG: time to be caught up" "$CAUGHT_MS" "$VTOP_DR_LEDGER_LOST_RTO_S"

  unproven=$((DR_UNIQUE - post_records))
  ((unproven > 0)) || unproven=0
  local k="${L_LEG//-/_}"
  dr_set "${k}_rto_ms" "$RTO_MS"
  dr_set "${k}_caught_up_ms" "$CAUGHT_MS"
  dr_set "${k}_records_total" "$L_TOTAL"
  dr_set "${k}_records_missing" "$DR_MISSING"
  dr_set "${k}_records_archived_twice" "$DR_DUPLICATES"
  dr_set "${k}_ledger_rows_lost" "$PRE_ROWS"
  dr_set "${k}_ledger_records_lost" "$PRE_RECORDS"
  dr_set "${k}_ledger_rows_after" "$post_rows"
  dr_set "${k}_ledger_records_after" "$post_records"
  dr_set "${k}_records_in_archive_not_proven_by_ledger" "$unproven"
  dr_check_summary "$DR_MISSING"
  log "$L_LEG: first commit $(secs "$RTO_MS") s, caught up $(secs "$CAUGHT_MS") s; the ledger lost $PRE_ROWS batch(es) ($PRE_RECORDS records); archive: $DR_UNIQUE of $L_TOTAL records, $DR_DUPLICATES archived twice, $DR_MISSING missing; $unproven record(s) in the archive that the new ledger does not prove"
}

dr_run_drill
