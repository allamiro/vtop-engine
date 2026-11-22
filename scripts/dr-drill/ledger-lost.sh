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
# Usage: ledger-lost.sh <vtopctl>
# shellcheck source=scripts/dr-drill/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
dr_init ledger-lost "${1:-}"

VTOP_DR_LEDGER_LOST_RTO_S=60
dr_check_doc VTOP_DR_LEDGER_LOST_RTO_S=$VTOP_DR_LEDGER_LOST_RTO_S

BUCKET=dr-ledger-lost
REC=dr-rec-
BEFORE=120 # records archived before the loss
DURING=60  # records the source receives while the ledger is gone

dr_minio_start store
dr_bucket_make store "$BUCKET"
dr_pg_start ledger
dr_kafka_start k

committed_rows() { dr_sql_num "SELECT count(*) FROM batches WHERE state='source_committed'"; }
committed_records() { dr_sql_num "SELECT COALESCE(sum(record_count),0) FROM batches WHERE state='source_committed'"; }
open_rows() { dr_sql_num "SELECT count(*) FROM batches WHERE state NOT IN ('source_committed','superseded')"; }
# The engine is drained when the ledger holds the records it should and nothing
# is in flight.
committed_records_ge() { (($(committed_records) >= $1)) && (($(open_rows) == 0)); }
committed_rows_gt() { (($(committed_rows) > $1)); }
gen() { local i; for ((i = $1; i < $2; i++)); do printf '%s%06d payload for the disaster-recovery drill %s\n' "$REC" "$i" "$3"; done; }

# leg <file|kafka|kafka-rebuild>
leg() {
  local leg="$1" src="${1%%-*}" db="dr_ll_${1//-/_}" prefix="ledger-lost/$1" cfg="$DR_WORKDIR/$1.yaml"
  local digests="$DR_WORKDIR/$1-digests" input="$DR_WORKDIR/$1-input.log" total=$((BEFORE + DURING))
  local topic="dr-ledger-lost-$DR_RUN_ID-$1" group="dr-ledger-lost-$DR_RUN_ID-$1" want_ledger
  local pre_rows pre_records post_rows post_records unproven rto_ms rebuilt_ms
  log "=== leg: $leg"
  dr_pg_admin "CREATE DATABASE $db"
  dr_pg_db "$db"
  mkdir -p "$digests"
  if [[ "$src" == kafka ]]; then
    dr_config "$cfg" source=kafka bootstrap="$DR_KAFKA_BOOTSTRAP" topic="$topic" group="$group" \
      endpoint="$(dr_s3_endpoint store)" bucket="$BUCKET" prefix="$prefix" work="$DR_WORKDIR/$leg-work" digests="$digests"
    dr_kafka_topic "$topic"
    dr_kafka_produce "$topic" 0 "$BEFORE" "$REC" before
  else
    dr_config "$cfg" source=file input="$input" \
      endpoint="$(dr_s3_endpoint store)" bucket="$BUCKET" prefix="$prefix" work="$DR_WORKDIR/$leg-work" digests="$digests"
    gen 0 "$BEFORE" before > "$input"
  fi
  dr_ctl "$leg-migrate" migrate --config "$cfg"

  # ---- a working archive ---------------------------------------------------------
  dr_engine_start "$cfg"
  wait_until 120 "the engine to archive the first $BEFORE records" committed_records_ge "$BEFORE"
  dr_engine_stop
  pre_rows="$(committed_rows)"
  pre_records="$(committed_records)"
  dr_ctl "$leg-audit-verify-before" audit verify --config "$cfg"
  log "before the loss: $pre_rows committed batch(es), $pre_records record(s); audit trail: $(grep -E '^audit chain' "$DR_LOGS/$leg-audit-verify-before.log")"
  dr_readback store "$BUCKET" "$prefix" "$DR_WORKDIR/$leg-rb-before"
  dr_count "$DR_WORKDIR/$leg-rb-before" "$REC" "$BEFORE"
  crit_eq "$leg: the archive holds every record before the loss" "$DR_UNIQUE" "$BEFORE"

  # ---- the disaster ---------------------------------------------------------------
  log "disaster: the ledger's database is destroyed; there is no backup"
  dr_pg_admin "DROP DATABASE $db WITH (FORCE)"
  rm -rf "$digests" "$DR_WORKDIR/$leg-work"
  # The source goes on receiving while the ledger is gone.
  if [[ "$src" == kafka ]]; then
    dr_kafka_produce "$topic" "$BEFORE" "$DURING" "$REC" during
  else
    gen "$BEFORE" "$total" during >> "$input"
  fi

  # ---- the documented recovery -----------------------------------------------------
  log "recovery: a new database, vtopctl migrate, the same config, start"
  T0=$(now_us)
  dr_pg_admin "CREATE DATABASE $db"
  dr_ctl "$leg-migrate-after" migrate --config "$cfg"
  mkdir -p "$digests"
  if [[ "$leg" == kafka-rebuild ]]; then
    # The page's step for a source that still holds its history: read it all
    # again into the new ledger. It must come before the engine's first start
    # on the new ledger: the engine will not move a consumer group behind a
    # ledger that already proves the records.
    dr_kafka_tool kafka-consumer-groups.sh --group "$group" --topic "$topic" --reset-offsets --to-earliest --execute \
      > "$DR_LOGS/$leg-reset.log" 2>&1 || fail "could not reset the consumer group; see $DR_LOGS/$leg-reset.log"
  fi
  dr_engine_start "$cfg"
  wait_until 120 "the first commit after the loss" committed_rows_gt 0
  rto_ms=$(ms_since "$T0")
  if [[ "$src" == kafka && "$leg" != kafka-rebuild ]]; then want_ledger=$DURING; else want_ledger=$total; fi
  wait_until 120 "the engine to archive the rest of the source" committed_records_ge "$want_ledger"
  dr_engine_stop
  rebuilt_ms=$(ms_since "$T0")

  post_rows="$(committed_rows)"
  post_records="$(committed_records)"
  dr_readback store "$BUCKET" "$prefix" "$DR_WORKDIR/$leg-rb-after"
  dr_count "$DR_WORKDIR/$leg-rb-after" "$REC" "$total"
  crit_eq "$leg: no record is missing from the archive" "$DR_MISSING" 0
  case "$leg" in
    kafka)
      crit_eq "$leg: no record is archived twice" "$DR_DUPLICATES" 0
      crit_eq "$leg: the new ledger proves the records archived since the loss" "$post_records" "$DURING"
      ;;
    kafka-rebuild)
      crit_eq "$leg: no record is archived twice: the objects were found and reused" "$DR_DUPLICATES" 0
      crit_ge "$leg: the new ledger proves every record" "$post_records" "$total"
      ;;
    file)
      crit_le "$leg: records archived twice are at most those archived before the loss" "$DR_DUPLICATES" "$BEFORE" " records"
      crit_ge "$leg: the new ledger proves every record" "$post_records" "$total"
      ;;
  esac
  dr_checks "$leg-after" "$cfg" "$digests"
  crit_eq "$leg: audit-coverage --check-storage exits 0" "$DR_AUDIT_RC" 0
  crit_eq "$leg: scrub exits 0" "$DR_SCRUB_RC" 0
  crit_eq "$leg: audit verify exits 0" "$DR_VERIFY_RC" 0
  crit_le "$leg: RTO, to the first commit" "$((rto_ms / 1000))" "$VTOP_DR_LEDGER_LOST_RTO_S" " s"
  crit_le "$leg: time to be caught up" "$((rebuilt_ms / 1000))" "$VTOP_DR_LEDGER_LOST_RTO_S" " s"

  unproven=$((DR_UNIQUE - post_records))
  ((unproven > 0)) || unproven=0
  local k="${leg//-/_}"
  dr_set "${k}_rto_ms" "$rto_ms"
  dr_set "${k}_caught_up_ms" "$rebuilt_ms"
  dr_set "${k}_records_total" "$total"
  dr_set "${k}_records_missing" "$DR_MISSING"
  dr_set "${k}_records_archived_twice" "$DR_DUPLICATES"
  dr_set "${k}_ledger_rows_lost" "$pre_rows"
  dr_set "${k}_ledger_records_lost" "$pre_records"
  dr_set "${k}_ledger_rows_after" "$post_rows"
  dr_set "${k}_ledger_records_after" "$post_records"
  dr_set "${k}_records_in_archive_not_proven_by_ledger" "$unproven"
  log "$leg: first commit $(secs "$rto_ms") s, caught up $(secs "$rebuilt_ms") s; the ledger lost $pre_rows batch(es) ($pre_records records); archive: $DR_UNIQUE of $total records, $DR_DUPLICATES archived twice, $DR_MISSING missing; $unproven record(s) in the archive that the new ledger does not prove"
}

for L in file kafka kafka-rebuild; do leg "$L"; done
dr_finish
