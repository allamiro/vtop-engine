#!/usr/bin/env bash
# Disaster-recovery drill: a disk fills (#735, criterion E4).
#
# An engine archives a file into a store, with a SQLite ledger. While it runs, one
# of the disks it writes to is filled to the last byte, the source goes on
# growing, and the disk stays full for a while. The drill then follows
# docs/operate/disaster-recovery.md, "A disk is full": free the space and, if
# the engine is not running, start it. It runs three times, once per disk:
#
#   work    the engine's work directory, where it stages a batch before the
#           upload
#   ledger  the disk the SQLite ledger is on
#   store   the object store's own disk
#
# Pass criteria, for each disk:
#   - the commit rule holds under the fault: a range the ledger records as
#     committed has its object and manifest in the store
#     (`audit-coverage --check-storage` exits 0 at the end);
#   - once the space is free the archive holds every record, with no restart
#     needed or with the one restart the page says, and the duplicates are
#     at most one batch's (the at-least-once window);
#   - the scrub reads every committed object back clean;
#   - the failure shows where the page says it does: the engine's log names
#     the full disk.
#
# Measured and bounded (docs/operate/disaster-recovery.md): the time from
# freeing the space until every record is committed (RTO).
#
# The ledger and work-directory legs mount a tmpfs, which needs root or
# passwordless sudo, as the engine crash sweep's enospc leg does. The store leg
# gives MinIO a small tmpfs of its own, with no privilege needed.
#
# Usage: disk-full.sh <vtopctl>
# shellcheck source=scripts/dr-drill/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
dr_init disk-full "${1:-}"
command -v sqlite3 > /dev/null || fail "sqlite3 is needed"

VTOP_DR_DISK_FULL_RTO_S=90
dr_check_doc VTOP_DR_DISK_FULL_RTO_S=$VTOP_DR_DISK_FULL_RTO_S

BUCKET=dr-disk-full
REC=dr-rec-
BEFORE=40
DURING=60
TOTAL=$((BEFORE + DURING))
BATCH=20
HOLD_S=20 # how long the disk stays full with the engine trying

gen() { local i; for ((i = $1; i < $2; i++)); do printf '%s%06d payload for the disaster-recovery drill\n' "$REC" "$i"; done; }

# fill <dir>: write to the last byte.
fill() {
  dd if=/dev/zero of="$1/filler" bs=512 2> /dev/null || true
  if printf x >> "$1/probe" 2> /dev/null; then fail "$1 is not full"; fi
  rm -f "$1/probe"
}
# mount_small <dir> <size>: a tmpfs this big, owned by the current user.
mount_small() {
  mkdir -p "$1"
  dr_as_root mount -t tmpfs -o "size=$2,uid=$(id -u),gid=$(id -g),mode=0700" tmpfs "$1" \
    || fail "could not mount a tmpfs at $1; the work and ledger legs need root or passwordless sudo"
  DR_MOUNTS+=("$1")
}

dr_minio_start base
dr_bucket_make base "$BUCKET"

leg() { # <work|ledger|store>
  local leg="$1" dir="$DR_WORKDIR/$1" region=base
  local db="$dir/state.db" work="$dir/work" input="$dir/input.log" prefix="disk-full/$1" cfg="$dir/config.yaml" full_dir
  mkdir -p "$dir"
  log "=== leg: $leg"
  case "$leg" in
    work)
      mount_small "$work" 4m
      full_dir="$work"
      ;;
    ledger)
      mkdir -p "$dir/ledger"
      mount_small "$dir/ledger" 4m
      db="$dir/ledger/state.db"
      full_dir="$dir/ledger"
      ;;
    store)
      region="small"
      dr_minio_start small --tmpfs /data:rw,size=64m,uid=10002,gid=10002,mode=0700 -e MINIO_CI_CD=1
      dr_bucket_make small "$BUCKET"
      full_dir=""
      ;;
  esac
  dr_config "$cfg" state="sqlite://$db" source=file input="$input" endpoint="$(dr_s3_endpoint "$region")" \
    bucket="$BUCKET" prefix="$prefix" work="$work" batch="$BATCH"
  gen 0 "$BEFORE" > "$input"
  committed_rows() { dr_sqlite_num "$db" "SELECT count(*) FROM batches WHERE state='source_committed'"; }
  committed_records() { dr_sqlite_num "$db" "SELECT COALESCE(sum(record_count),0) FROM batches WHERE state='source_committed'"; }
  open_rows() { dr_sqlite_num "$db" "SELECT count(*) FROM batches WHERE state NOT IN ('source_committed','superseded','failed','replay_required')"; }
  drained() { (($(committed_records) >= $1)) && (($(open_rows) == 0)); }

  dr_engine_start "$cfg"
  wait_until 90 "the first $BEFORE records to be archived" drained "$BEFORE"
  ROWS_BEFORE="$(committed_rows)"

  log "fault: the $leg disk is full"
  case "$leg" in
    work | ledger) fill "$full_dir" ;;
    store)
      # Fill the store's disk through the store itself: objects until it refuses.
      docker exec "${DR_CONT[minio-small]}" sh -c 'dd if=/dev/zero of=/data/filler bs=512 2>/dev/null; true'
      ;;
  esac
  gen "$BEFORE" "$TOTAL" >> "$input"
  sleep "$HOLD_S"
  local alive=yes
  dr_engine_alive || alive=no
  ROWS_DURING="$(committed_rows)"
  log "$leg: after ${HOLD_S}s full, the engine is alive: $alive; $((ROWS_DURING - ROWS_BEFORE)) batch(es) committed meanwhile"
  FELT="$(grep -ciE 'no space left|disk is full|database or disk is full|StorageFull|XMinioStorageFull|insufficient|ENOSPC|os error 28' "$(dr_engine_log)" || true)"
  if ((FELT > 0)); then crit ok "$leg: the engine's log names the full disk" "$FELT line(s)"; else crit bad "$leg: the engine's log names the full disk"; fi

  log "recovery: free the space"
  case "$leg" in
    work | ledger) rm -f "$full_dir/filler" ;;
    store) docker exec "${DR_CONT[minio-small]}" rm -f /data/filler ;;
  esac
  T0=$(now_us)
  RESTARTED=no
  if ! dr_engine_alive; then
    log "the engine is not running: start it"
    dr_engine_start "$cfg"
    RESTARTED=yes
  fi
  wait_until 180 "the engine to archive every record" drained "$TOTAL"
  RTO_MS=$(ms_since "$T0")
  dr_engine_stop

  dr_checks "$leg" "$cfg" -
  crit_eq "$leg: audit-coverage --check-storage exits 0" "$DR_AUDIT_RC" 0
  crit_eq "$leg: scrub exits 0" "$DR_SCRUB_RC" 0
  crit_le "$leg: RTO, from freeing the space to every record committed" "$((RTO_MS / 1000))" "$VTOP_DR_DISK_FULL_RTO_S" " s"
  dr_readback "$region" "$BUCKET" "$prefix" "$dir/rb"
  dr_sqlite "$db" "SELECT object_uri FROM batches WHERE state='source_committed'" | sed 's#^s3://[^/]*/##' > "$dir/committed-keys"
  dr_count_committed "$dir/rb" "$prefix" "$dir/committed-keys" "$REC" "$TOTAL"
  crit_eq "$leg: no record is missing from the committed objects" "$DR_MISSING" 0
  crit_le "$leg: records archived twice are at most one batch's" "$DR_DUPLICATES" "$BATCH" " records"
  dr_set "${leg}_engine_survived_the_fault" "$alive"
  dr_set "${leg}_restart_needed" "$RESTARTED"
  dr_set "${leg}_rto_ms" "$RTO_MS"
  dr_set "${leg}_batches_committed_while_full" "$((ROWS_DURING - ROWS_BEFORE))"
  dr_set "${leg}_records_missing" "$DR_MISSING"
  dr_set "${leg}_records_archived_twice" "$DR_DUPLICATES"
  log "$leg: RTO $(secs "$RTO_MS") s; engine survived the fault: $alive; restart needed: $RESTARTED; $DR_MISSING missing, $DR_DUPLICATES twice"
}

for L in work ledger store; do leg "$L"; done
dr_finish
