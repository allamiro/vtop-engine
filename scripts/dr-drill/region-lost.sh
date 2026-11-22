#!/usr/bin/env bash
# Disaster-recovery drill: a whole region is lost (#735, criterion E4).
#
# The primary region holds the engine, its PostgreSQL ledger and the bucket. The
# DR region holds a replica of the bucket (the store's own bucket replication,
# MinIO here, S3 cross-region replication in production) and the ledger's most
# recent backup (`pg_dump`, as docs/operate/backup-and-recovery.md has it). The
# source, a Kafka topic, is outside both regions and survives, as the page
# requires. The primary region is destroyed while the engine runs and the topic
# goes on receiving records. The drill follows docs/operate/disaster-recovery.md,
# "A region is lost": restore the ledger in the DR region, point the engine at
# the replica, check, and start. It runs two legs, which differ in what the
# disaster finds in the DR region:
#
#   replicated  The replica is complete, and the ledger backup is older than the
#               last batches. Pass criteria: the check after the restore passes;
#               the engine, finding the consumer group ahead of the restored
#               ledger, resumes from the ledger's cursor and says so; it archives
#               what the backup did not know, finding the replica's objects and
#               reusing them; nothing is missing or archived twice, and the ledger
#               proves every record again. The ledger loses the rows written since
#               the backup.
#   lagging     The replica is behind: it was cut off from the primary for the
#               last stretch of archiving (the region's link was down), so
#               batches the ledger backup holds committed have no object in the
#               DR region. Pass criteria: the check after the restore fails and
#               names exactly those objects; the page's universal repair (a new
#               ledger, the group reset to the start of the topic) archives
#               them again from the source, and nothing is missing at the end.
#
# The universal repair (shared with docs/operate/disaster-recovery.md, "The
# ledger is lost"): a new database, `vtopctl migrate`, the consumer group reset
# to the start of what the source still holds, start. Every object the store
# has is found and reused, each one the store lacks is written, and the new
# ledger proves all of it.
#
# Measured and bounded (docs/operate/disaster-recovery.md):
#   RPO  of the ledger: the committed rows lost with the primary region; of the
#        archive: the records whose objects the DR region lacked at the
#        disaster. Neither loses a record while the source still holds it.
#   RTO  from the first recovery step in the DR region until the engine has
#        archived the source again.
#
# Usage: region-lost.sh <vtopctl>
# shellcheck source=scripts/dr-drill/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
dr_init region-lost "${1:-}"

VTOP_DR_REGION_LOST_RTO_S=180
dr_check_doc VTOP_DR_REGION_LOST_RTO_S=$VTOP_DR_REGION_LOST_RTO_S

BUCKET=dr-region-lost
REC=dr-rec-
FIRST=120  # archived, and backed up, before the last stretch
LAST=60    # archived in the last stretch, after the backup (and cut off, when lagging)
DURING=60  # produced while the region is gone
TOTAL=$((FIRST + LAST + DURING))
BATCH=20

dr_kafka_start k

committed_rows() { dr_sql_num "SELECT count(*) FROM batches WHERE state='source_committed'"; }
committed_records() { dr_sql_num "SELECT COALESCE(sum(record_count),0) FROM batches WHERE state='source_committed'"; }
open_rows() { dr_sql_num "SELECT count(*) FROM batches WHERE state NOT IN ('source_committed','superseded','failed','replay_required')"; }
drained() { (($(committed_records) >= $1)) && (($(open_rows) == 0)); }
committed_rows_gt() { (($(committed_rows) > $1)); }
# The ledger's cursor for the topic's partition: the offset after the highest committed record.
CURSOR_SQL="SELECT COALESCE(max(((progress_end_json::json)->>'end_offset')::bigint), -1) + 1 FROM batches WHERE source_type='kafka' AND state='source_committed'"

# replicated <prefix>: the DR region holds as many objects as the primary.
replicated() {
  local primary dr
  primary="$(dr_mc primary ls --recursive "local/$BUCKET/$1" | wc -l | tr -d ' ')"
  dr="$(dr_mc dr ls --recursive "local/$BUCKET/$1" | wc -l | tr -d ' ')"
  [[ "$primary" == "$dr" && "$primary" != 0 ]]
}

# leg <replicated|lagging>
leg() {
  local leg="$1" k="${1//-/_}" db_primary="dr_rl_${1//-/_}" db_dr="dr_rl_${1//-/_}_dr"
  local topic="dr-region-lost-$DR_RUN_ID-$1" group="dr-region-lost-$DR_RUN_ID-$1" prefix="region-lost/$1"
  local bk="$DR_WORKDIR/$1-backup" digests="$DR_WORKDIR/$1-digests" cfg_primary="$DR_WORKDIR/$1-primary.yaml" cfg_dr="$DR_WORKDIR/$1-dr.yaml"
  local backup_rows backup_records dead_rows dead_records restored_rows cursor
  local rto_ms caught_ms
  log "=== leg: $leg"
  mkdir -p "$bk" "$digests"

  # ---- the primary region and the DR region ---------------------------------------
  dr_minio_start primary
  dr_minio_start dr
  dr_bucket_make primary "$BUCKET" versioning
  dr_bucket_make dr "$BUCKET" versioning
  dr_mc primary replicate add "local/$BUCKET" \
    --remote-bucket "http://$DR_S3_USER:$DR_S3_PASSWORD@127.0.0.1:${DR_PORT[minio-dr]}/$BUCKET" \
    --replicate "delete,delete-marker,existing-objects" > "$DR_LOGS/$leg-replicate-add.log" 2>&1 \
    || fail "could not set up bucket replication; see $DR_LOGS/$leg-replicate-add.log"
  dr_pg_start primary
  dr_pg_admin "CREATE DATABASE $db_primary"
  dr_pg_db "$db_primary"
  dr_config "$cfg_primary" source=kafka bootstrap="$DR_KAFKA_BOOTSTRAP" topic="$topic" group="$group" \
    endpoint="$(dr_s3_endpoint primary)" bucket="$BUCKET" prefix="$prefix" work="$DR_WORKDIR/$leg-work-primary" digests="$digests" batch="$BATCH" versioning=true
  dr_ctl "$leg-migrate" migrate --config "$cfg_primary"
  dr_kafka_topic "$topic"
  dr_kafka_produce "$topic" 0 "$FIRST" "$REC" first

  # ---- a working archive, and the ledger's backup ----------------------------------
  dr_engine_start "$cfg_primary"
  wait_until 120 "the first $FIRST records to be archived" drained "$FIRST"
  log "backup: pg_dump of the ledger, and the digest directory"
  docker exec "${DR_CONT[pg-primary]}" pg_dump -U vtop -d "$db_primary" --format=custom > "$bk/ledger.dump"
  mkdir -p "$bk/digests"
  cp -a "$digests/." "$bk/digests/"
  [[ -s "$bk/ledger.dump" ]] || fail "pg_dump wrote nothing"
  backup_rows="$(committed_rows)"
  backup_records="$(committed_records)"

  # ---- the last stretch ------------------------------------------------------------
  # The first stretch has reached the DR region: what is missing there later is
  # only what the last stretch's cut-off costs.
  wait_until 60 "replication to deliver the first stretch" replicated "$prefix"
  if [[ "$leg" == lagging ]]; then
    log "the DR region is cut off from the primary: replication stops"
    docker stop "${DR_CONT[minio-dr]}" > /dev/null
  fi
  dr_kafka_produce "$topic" "$FIRST" "$LAST" "$REC" last
  wait_until 120 "the last stretch to be archived" drained $((FIRST + LAST))
  if [[ "$leg" == lagging ]]; then
    # The newest backup: the ledger holds the last stretch committed, and the DR
    # region has no object of it.
    docker exec "${DR_CONT[pg-primary]}" pg_dump -U vtop -d "$db_primary" --format=custom > "$bk/ledger.dump"
    backup_rows="$(committed_rows)"
    backup_records="$(committed_records)"
  else
    wait_until 60 "replication to deliver the last stretch" replicated "$prefix"
  fi

  # ---- the disaster ----------------------------------------------------------------
  dead_rows="$(committed_rows)"
  dead_records="$(committed_records)"
  log "disaster: the primary region is lost: its engine, ledger and bucket"
  dr_engine_kill
  dr_kill pg-primary
  dr_kill minio-primary
  if [[ "$leg" == lagging ]]; then
    docker start "${DR_CONT[minio-dr]}" > /dev/null
    dr_wait_minio dr
  fi
  dr_kafka_produce "$topic" $((FIRST + LAST)) "$DURING" "$REC" during
  dr_readback dr "$BUCKET" "$prefix" "$DR_WORKDIR/$leg-rb-dr-at-disaster"
  dr_count "$DR_WORKDIR/$leg-rb-dr-at-disaster" "$REC" $((FIRST + LAST))
  local in_dr="$DR_UNIQUE" archive_rpo=$((FIRST + LAST - DR_UNIQUE))
  log "at the disaster: the dead ledger held $dead_rows committed batch(es) ($dead_records records), the backup $backup_rows ($backup_records); the DR bucket held $in_dr of the $((FIRST + LAST)) archived records"

  # ---- the documented recovery, in the DR region ------------------------------------
  log "recovery: a ledger server in the DR region, the backup restored"
  T0=$(now_us)
  dr_pg_start dr
  dr_pg_admin "CREATE DATABASE $db_dr"
  docker exec -i "${DR_CONT[pg-dr]}" pg_restore -U vtop -d "$db_dr" --exit-on-error < "$bk/ledger.dump" \
    > "$DR_LOGS/$leg-pg-restore.log" 2>&1 || fail "pg_restore failed; see $DR_LOGS/$leg-pg-restore.log"
  dr_pg_db "$db_dr"
  rm -rf "$digests"
  mkdir -p "$digests"
  cp -a "$bk/digests/." "$digests/"
  dr_config "$cfg_dr" source=kafka bootstrap="$DR_KAFKA_BOOTSTRAP" topic="$topic" group="$group" \
    endpoint="$(dr_s3_endpoint dr)" bucket="$BUCKET" prefix="$prefix" work="$DR_WORKDIR/$leg-work-dr" digests="$digests" batch="$BATCH" versioning=true
  dr_ctl "$leg-migrate-dr" migrate --config "$cfg_dr"
  restored_rows="$(committed_rows)"
  cursor="$(dr_sql_num "$CURSOR_SQL")"
  log "restored: $restored_rows committed batch(es), cursor at offset $cursor"
  crit_eq "$leg: the restored ledger is the backup's" "$restored_rows" "$backup_rows"

  # The check the page says to run before the engine starts.
  dr_ctl_rc "$leg-check" audit-coverage --config "$cfg_dr" --digest-dir "$digests" --check-storage
  local check_rc=$DR_RC
  local missing_objects
  missing_objects="$(grep -c '^FAIL storage' "$DR_LOGS/$leg-check.log" || true)"

  if [[ "$leg" == lagging ]]; then
    crit_eq "$leg: the check after the restore fails" "$([[ $check_rc -ne 0 ]] && echo nonzero || echo zero)" nonzero
    local want_missing=$(((backup_rows - FIRST / BATCH) * 2)) # an object and a manifest for each batch of the last stretch
    crit_eq "$leg: the check names exactly the objects the DR region lacks (an object and a manifest for each batch)" "$missing_objects" "$want_missing"
    repair_universal "$leg" "$cfg_dr" "$digests" "$db_dr" "$topic" "$group"
  else
    crit_eq "$leg: the check after the restore passes" "$check_rc" 0
    dr_engine_start "$cfg_dr"
    wait_until 120 "the first commit in the DR region" committed_rows_gt "$restored_rows"
    rto_ms=$(ms_since "$T0")
    wait_until 120 "the engine to archive the source" caught_up "$group" "$topic" "$TOTAL"
    caught_ms=$(ms_since "$T0")
    dr_engine_stop
    crit_test "$leg: the engine found the consumer group ahead of the restored ledger and resumed from the ledger" "" \
      grep -q 'ahead of the ledger' "$(dr_engine_log)"
    finish_checks "$leg" "$cfg_dr" "$digests" dr "$prefix" "$TOTAL"
    crit_eq "$leg: no record is archived twice (the replica's objects were found and reused)" "$DR_DUPLICATES" 0
    crit_ge "$leg: the restored ledger, once the engine has caught up, proves every record" "$(committed_records)" "$TOTAL"
  fi
  dr_set "${k}_rpo_ledger_rows_lost" "$((dead_rows - restored_rows))"
  dr_set "${k}_rpo_archive_records_not_in_dr_region" "$archive_rpo"
  dr_set "${k}_rto_ms" "${rto_ms:-$REPAIR_RTO_MS}"
  dr_set "${k}_caught_up_ms" "${caught_ms:-$REPAIR_RTO_MS}"
  log "$leg: the ledger lost $((dead_rows - restored_rows)) committed batch(es); the DR region lacked $archive_rpo archived record(s); first commit $(secs "${rto_ms:-$REPAIR_RTO_MS}") s, caught up $(secs "${caught_ms:-$REPAIR_RTO_MS}") s"
  # The legs share the broker; tear the stores and servers down.
  dr_kill minio-dr
  dr_kill pg-dr
}

# caught_up <group> <topic> <offset>: the group has committed up to the offset and nothing is in flight.
caught_up() { [[ "$(dr_kafka_group_offset "$1" "$2")" == "$3" ]] && (($(open_rows) == 0)); }

# repair_universal <leg> <config> <digests> <db> <topic> <group>: the page's
# repair: a new ledger, the group reset to the start of the topic, start.
repair_universal() {
  local leg="$1" cfg="$2" digests="$3" db="$4" topic="$5" group="$6"
  log "repair: keep the restored ledger, make a new one, reset the group to the start of the topic"
  local T1
  T1=$(now_us)
  dr_pg_admin "ALTER DATABASE $db RENAME TO ${db}_kept"
  dr_pg_admin "CREATE DATABASE $db"
  dr_ctl "$leg-migrate-repair" migrate --config "$cfg"
  rm -rf "$digests" "$DR_WORKDIR/$leg-work-dr"
  mkdir -p "$digests"
  dr_kafka_tool kafka-consumer-groups.sh --group "$group" --topic "$topic" --reset-offsets --to-earliest --execute \
    > "$DR_LOGS/$leg-reset-earliest.log" 2>&1 || fail "could not reset the consumer group; see $DR_LOGS/$leg-reset-earliest.log"
  dr_engine_start "$cfg"
  wait_until 240 "the engine to archive the whole source again" drained "$TOTAL"
  REPAIR_RTO_MS=$(ms_since "$T1")
  dr_engine_stop
  finish_checks "$leg" "$cfg" "$digests" dr "region-lost/$leg" "$TOTAL"
  crit_eq "$leg: after the repair no record is archived twice (found objects were reused)" "$DR_DUPLICATES" 0
  crit_ge "$leg: the new ledger proves every record" "$(committed_records)" "$TOTAL"
}

# finish_checks <leg> <config> <digests> <region> <prefix> <total>: the archive in
# the region holds every record, and the ledger passes the page's three checks.
FINAL_N=0
finish_checks() {
  local leg="$1" cfg="$2" digests="$3" region="$4" prefix="$5" total="$6" dest
  FINAL_N=$((FINAL_N + 1))
  dest="$DR_WORKDIR/$leg-rb-final-$FINAL_N"
  dr_readback "$region" "$BUCKET" "$prefix" "$dest"
  dr_count "$dest" "$REC" "$total"
  crit_eq "$leg: no record is missing from the DR region's archive" "$DR_MISSING" 0
  dr_checks "$leg-final-$FINAL_N" "$cfg" "$digests"
  crit_eq "$leg: audit-coverage --check-storage exits 0" "$DR_AUDIT_RC" 0
  crit_eq "$leg: scrub exits 0" "$DR_SCRUB_RC" 0
  crit_eq "$leg: audit verify exits 0" "$DR_VERIFY_RC" 0
}

for L in replicated lagging; do leg "$L"; done
dr_finish
