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
# Usage: region-lost.sh <vtopctl> [--handover | --check | --teardown] [--leg replicated|lagging]
# shellcheck source=scripts/dr-drill/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
# shellcheck disable=SC2034  # DR_LEGS, DR_PAGE_SECTION and DR_HANDOVER_VARS are read by lib.sh
dr_init region-lost "$@"

VTOP_DR_REGION_LOST_RTO_S=180
dr_check_doc VTOP_DR_REGION_LOST_RTO_S=$VTOP_DR_REGION_LOST_RTO_S

BUCKET="dr-region-lost"
REC="dr-rec-"
FIRST=120  # archived, and backed up, before the last stretch
LAST=60    # archived in the last stretch, after the backup (and cut off, when lagging)
DURING=60  # produced while the region is gone
TOTAL=$((FIRST + LAST + DURING))
BATCH=20

DR_LEGS=(replicated lagging)
DR_PAGE_SECTION="A region is lost"
DR_HANDOVER_VARS=(BACKUP_ROWS BACKUP_RECORDS DEAD_ROWS DEAD_RECORDS ARCHIVE_RPO IN_DR)

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

# ctx <leg>: the leg's names, the same in every phase.
ctx() {
  L_LEG="$1"
  L_K="${1//-/_}"
  L_DBP="dr_rl_${1//-/_}"
  L_DBD="dr_rl_${1//-/_}_dr"
  L_TOPIC="dr-region-lost-$DR_RUN_ID-$1"
  L_GROUP="dr-region-lost-$DR_RUN_ID-$1"
  L_PREFIX="region-lost/$1"
  L_BK="$DR_WORKDIR/$1-backup"
  L_DIGESTS="$DR_WORKDIR/$1-digests"
  L_WORK_DR="$DR_WORKDIR/$1-work-dr"
  L_CFG_P="$DR_WORKDIR/$1-primary.yaml"
  L_CFG_D="$DR_WORKDIR/$1-dr.yaml"
}

lab_shared() {
  dr_kafka_start k
}

# lab <leg>: the primary and DR regions, a working archive and a backup, then the disaster.
lab() {
  ctx "$1"
  log "=== leg: $L_LEG"
  mkdir -p "$L_BK" "$L_DIGESTS"

  # ---- the primary region and the DR region ---------------------------------------
  dr_minio_start primary
  dr_minio_start dr
  dr_bucket_make primary "$BUCKET" versioning
  dr_bucket_make dr "$BUCKET" versioning
  dr_mc primary replicate add "local/$BUCKET" \
    --remote-bucket "http://$DR_S3_USER:$DR_S3_PASSWORD@127.0.0.1:${DR_PORT[minio-dr]}/$BUCKET" \
    --replicate "delete,delete-marker,existing-objects" > "$DR_LOGS/$L_LEG-replicate-add.log" 2>&1 \
    || fail "could not set up bucket replication; see $DR_LOGS/$L_LEG-replicate-add.log"
  dr_pg_start primary
  dr_pg_admin "CREATE DATABASE $L_DBP"
  dr_pg_db "$L_DBP"
  dr_config "$L_CFG_P" source=kafka bootstrap="$DR_KAFKA_BOOTSTRAP" topic="$L_TOPIC" group="$L_GROUP" \
    endpoint="$(dr_s3_endpoint primary)" bucket="$BUCKET" prefix="$L_PREFIX" work="$DR_WORKDIR/$L_LEG-work-primary" digests="$L_DIGESTS" batch="$BATCH" versioning=true
  dr_ctl "$L_LEG-migrate" migrate --config "$L_CFG_P"
  dr_kafka_topic "$L_TOPIC"
  dr_kafka_produce "$L_TOPIC" 0 "$FIRST" "$REC" first

  # ---- a working archive, and the ledger's backup ----------------------------------
  dr_engine_start "$L_CFG_P"
  wait_until 120 "the first $FIRST records to be archived" drained "$FIRST"
  log "backup: pg_dump of the ledger, and the digest directory"
  docker exec "${DR_CONT[pg-primary]}" pg_dump -U vtop -d "$L_DBP" --format=custom > "$L_BK/ledger.dump"
  mkdir -p "$L_BK/digests"
  cp -a "$L_DIGESTS/." "$L_BK/digests/"
  [[ -s "$L_BK/ledger.dump" ]] || fail "pg_dump wrote nothing"
  BACKUP_ROWS="$(committed_rows)"
  BACKUP_RECORDS="$(committed_records)"

  # ---- the last stretch ------------------------------------------------------------
  # The first stretch has reached the DR region: what is missing there later is
  # only what the last stretch's cut-off costs.
  wait_until 60 "replication to deliver the first stretch" replicated "$L_PREFIX"
  if [[ "$L_LEG" == lagging ]]; then
    log "the DR region is cut off from the primary: replication stops"
    docker stop "${DR_CONT[minio-dr]}" > /dev/null
  fi
  dr_kafka_produce "$L_TOPIC" "$FIRST" "$LAST" "$REC" last
  wait_until 120 "the last stretch to be archived" drained $((FIRST + LAST))
  if [[ "$L_LEG" == lagging ]]; then
    # The newest backup: the ledger holds the last stretch committed, and the DR
    # region has no object of it.
    docker exec "${DR_CONT[pg-primary]}" pg_dump -U vtop -d "$L_DBP" --format=custom > "$L_BK/ledger.dump"
    BACKUP_ROWS="$(committed_rows)"
    BACKUP_RECORDS="$(committed_records)"
  else
    wait_until 60 "replication to deliver the last stretch" replicated "$L_PREFIX"
  fi

  # ---- the disaster ----------------------------------------------------------------
  DEAD_ROWS="$(committed_rows)"
  DEAD_RECORDS="$(committed_records)"
  log "disaster: the primary region is lost: its engine, ledger and bucket"
  dr_engine_kill
  dr_kill pg-primary
  dr_kill minio-primary
  if [[ "$L_LEG" == lagging ]]; then
    docker start "${DR_CONT[minio-dr]}" > /dev/null
    dr_wait_minio dr
  fi
  dr_kafka_produce "$L_TOPIC" $((FIRST + LAST)) "$DURING" "$REC" during
  dr_readback dr "$BUCKET" "$L_PREFIX" "$DR_WORKDIR/$L_LEG-rb-dr-at-disaster"
  dr_count "$DR_WORKDIR/$L_LEG-rb-dr-at-disaster" "$REC" $((FIRST + LAST))
  IN_DR="$DR_UNIQUE"
  ARCHIVE_RPO=$((FIRST + LAST - DR_UNIQUE))
  log "at the disaster: the dead ledger held $DEAD_ROWS committed batch(es) ($DEAD_RECORDS records), the backup $BACKUP_ROWS ($BACKUP_RECORDS); the DR bucket held $IN_DR of the $((FIRST + LAST)) archived records"

  # The DR region's ledger server is the lab's infrastructure: an empty server,
  # which is where the page's step 2 starts (its own steps begin at `createdb`).
  # A run's clock starts before it, so the server's start is counted as it always was.
  T0=$(now_us)
  dr_pg_start dr
}

handover_text() {
  ctx "$1"
  echo "  The primary region is lost: its engine, its ledger (database $L_DBP) and its bucket. The source, Kafka topic"
  echo "  $L_TOPIC (consumer group $L_GROUP), survives and has received $((FIRST + LAST + DURING)) records, $DURING of them since the disaster."
  echo "  The DR region has a replica of the bucket $BUCKET (prefix $L_PREFIX) and an empty ledger server."
  echo "  The ledger's latest backup: $L_BK/ledger.dump, with the digest directory in $L_BK/digests."
  echo "  The primary's engine configuration is $L_CFG_P. Step 3 is yours: write the DR configuration to $L_CFG_D"
  echo "  (the same, with the DR store's endpoint and a work directory of your own) and use the digest directory $L_DIGESTS."
  echo "  Restore into a database named $L_DBD, so that the configuration and --check find it."
  if [[ "$L_LEG" == lagging ]]; then
    echo "  This leg: the DR replica was behind when the region died, so the step 4 check is expected to fail."
  else
    echo "  This leg: the DR replica is complete, and the backup is older than the last batches."
  fi
}

recover() {
  ctx "$1"
  local restored_rows cursor
  log "recovery: the backup restored in the DR region"
  dr_pg_admin "CREATE DATABASE $L_DBD"
  docker exec -i "${DR_CONT[pg-dr]}" pg_restore -U vtop -d "$L_DBD" --exit-on-error < "$L_BK/ledger.dump" \
    > "$DR_LOGS/$L_LEG-pg-restore.log" 2>&1 || fail "pg_restore failed; see $DR_LOGS/$L_LEG-pg-restore.log"
  dr_pg_db "$L_DBD"
  rm -rf "${L_DIGESTS:?}"
  mkdir -p "$L_DIGESTS"
  cp -a "$L_BK/digests/." "$L_DIGESTS/"
  dr_config "$L_CFG_D" source=kafka bootstrap="$DR_KAFKA_BOOTSTRAP" topic="$L_TOPIC" group="$L_GROUP" \
    endpoint="$(dr_s3_endpoint dr)" bucket="$BUCKET" prefix="$L_PREFIX" work="$L_WORK_DR" digests="$L_DIGESTS" batch="$BATCH" versioning=true
  dr_ctl "$L_LEG-migrate-dr" migrate --config "$L_CFG_D"
  restored_rows="$(committed_rows)"
  cursor="$(dr_sql_num "$CURSOR_SQL")"
  log "restored: $restored_rows committed batch(es), cursor at offset $cursor"
  crit_eq "$L_LEG: the restored ledger is the backup's" "$restored_rows" "$BACKUP_ROWS"

  # The check the page says to run before the engine starts.
  dr_ctl_rc "$L_LEG-check" audit-coverage --config "$L_CFG_D" --digest-dir "$L_DIGESTS" --check-storage
  local check_rc=$DR_RC missing_objects
  missing_objects="$(grep -c '^FAIL storage' "$DR_LOGS/$L_LEG-check.log" || true)"

  if [[ "$L_LEG" == lagging ]]; then
    crit_eq "$L_LEG: the check after the restore fails" "$([[ $check_rc -ne 0 ]] && echo nonzero || echo zero)" nonzero
    local want_missing=$(((BACKUP_ROWS - FIRST / BATCH) * 2)) # an object and a manifest for each batch of the last stretch
    crit_eq "$L_LEG: the check names exactly the objects the DR region lacks (an object and a manifest for each batch)" "$missing_objects" "$want_missing"
    repair_universal
    RTO_MS="$REPAIR_RTO_MS"
    CAUGHT_MS="$REPAIR_RTO_MS"
  else
    crit_eq "$L_LEG: the check after the restore passes" "$check_rc" 0
    dr_engine_start "$L_CFG_D"
    wait_until 120 "the first commit in the DR region" committed_rows_gt "$restored_rows"
    RTO_MS=$(ms_since "$T0")
    wait_until 120 "the engine to archive the source" caught_up "$L_GROUP" "$L_TOPIC" "$TOTAL"
    CAUGHT_MS=$(ms_since "$T0")
    dr_engine_stop
    crit_test "$L_LEG: the engine found the consumer group ahead of the restored ledger and resumed from the ledger" "" \
      grep -q 'ahead of the ledger' "$(dr_engine_log)"
  fi
}

# caught_up <group> <topic> <offset>: the group has committed up to the offset and nothing is in flight.
caught_up() { [[ "$(dr_kafka_group_offset "$1" "$2")" == "$3" ]] && (($(open_rows) == 0)); }

# repair_universal: the page's repair: keep the restored ledger, a new ledger,
# the group reset to the start of the topic, start.
repair_universal() {
  log "repair: keep the restored ledger, make a new one, reset the group to the start of the topic"
  local t1
  t1=$(now_us)
  dr_pg_admin "ALTER DATABASE $L_DBD RENAME TO ${L_DBD}_kept"
  dr_pg_admin "CREATE DATABASE $L_DBD"
  dr_ctl "$L_LEG-migrate-repair" migrate --config "$L_CFG_D"
  rm -rf "${L_DIGESTS:?}" "${L_WORK_DR:?}"
  mkdir -p "$L_DIGESTS"
  dr_kafka_tool kafka-consumer-groups.sh --group "$L_GROUP" --topic "$L_TOPIC" --reset-offsets --to-earliest --execute \
    > "$DR_LOGS/$L_LEG-reset-earliest.log" 2>&1 || fail "could not reset the consumer group; see $DR_LOGS/$L_LEG-reset-earliest.log"
  dr_engine_start "$L_CFG_D"
  wait_until 240 "the engine to archive the whole source again" drained "$TOTAL"
  REPAIR_RTO_MS=$(ms_since "$t1")
  dr_engine_stop
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

check() {
  ctx "$1"
  [[ -f "$L_CFG_D" ]] || fail "the DR configuration $L_CFG_D does not exist: step 3 writes it"
  dr_pg_db "$L_DBD"
  finish_checks "$L_LEG" "$L_CFG_D" "$L_DIGESTS" dr "$L_PREFIX" "$TOTAL"
  crit_eq "$L_LEG: no record is archived twice (found objects were reused)" "$DR_DUPLICATES" 0
  crit_ge "$L_LEG: the ledger, once the engine has caught up, proves every record" "$(committed_records)" "$TOTAL"
  crit_rto "$L_LEG: RTO, to the engine having archived the source" "$CAUGHT_MS" "$VTOP_DR_REGION_LOST_RTO_S"
  dr_set "${L_K}_rpo_ledger_rows_lost" "$((DEAD_ROWS - BACKUP_ROWS))"
  dr_set "${L_K}_rpo_archive_records_not_in_dr_region" "$ARCHIVE_RPO"
  dr_set "${L_K}_rto_ms" "$RTO_MS"
  dr_set "${L_K}_caught_up_ms" "$CAUGHT_MS"
  dr_check_summary "$DR_MISSING"
  log "$L_LEG: the ledger lost $((DEAD_ROWS - BACKUP_ROWS)) committed batch(es); the DR region lacked $ARCHIVE_RPO archived record(s); first commit $(secs "$RTO_MS") s, caught up $(secs "$CAUGHT_MS") s"
}

# The legs share the broker; a run tears the stores and servers down between them.
leg_done() {
  dr_kill minio-dr
  dr_kill pg-dr
}

dr_run_drill
