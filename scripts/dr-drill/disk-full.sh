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
# Usage: disk-full.sh <vtopctl> [--handover | --check | --teardown] [--leg work|ledger|store]
# shellcheck source=scripts/dr-drill/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
# shellcheck disable=SC2034  # DR_LEGS, DR_PAGE_SECTION and DR_HANDOVER_VARS are read by lib.sh
dr_init disk-full "$@"
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

DR_LEGS=(work ledger store)
DR_PAGE_SECTION="A disk is full"
DR_HANDOVER_VARS=(ROWS_BEFORE ROWS_DURING ALIVE FELT)

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

# ctx <leg>: the leg's names, the same in every phase.
ctx() {
  L_LEG="$1"
  L_DIR="$DR_WORKDIR/$1"
  L_REGION=base
  L_DBFILE="$L_DIR/state.db"
  L_WORK="$L_DIR/work"
  L_INPUT="$L_DIR/input.log"
  L_PREFIX="disk-full/$1"
  L_CFG="$L_DIR/config.yaml"
  L_FULL_DIR=""
  case "$1" in
    work) L_FULL_DIR="$L_WORK" ;;
    ledger)
      L_DBFILE="$L_DIR/ledger/state.db"
      L_FULL_DIR="$L_DIR/ledger"
      ;;
    store) L_REGION=small ;;
  esac
}
committed_rows() { dr_sqlite_num "$L_DBFILE" "SELECT count(*) FROM batches WHERE state='source_committed'"; }
committed_records() { dr_sqlite_num "$L_DBFILE" "SELECT COALESCE(sum(record_count),0) FROM batches WHERE state='source_committed'"; }
open_rows() { dr_sqlite_num "$L_DBFILE" "SELECT count(*) FROM batches WHERE state NOT IN ('source_committed','superseded','failed','replay_required')"; }
drained() { (($(committed_records) >= $1)) && (($(open_rows) == 0)); }

lab_shared() {
  dr_minio_start base
  dr_bucket_make base "$BUCKET"
}

# lab <leg>: a working archive, then the disk fills.
lab() {
  ctx "$1"
  mkdir -p "$L_DIR"
  log "=== leg: $L_LEG"
  case "$L_LEG" in
    work) mount_small "$L_WORK" 4m ;;
    ledger)
      mkdir -p "$L_DIR/ledger"
      mount_small "$L_DIR/ledger" 4m
      ;;
    store)
      dr_minio_start small --tmpfs /data:rw,size=64m,uid=10002,gid=10002,mode=0700 -e MINIO_CI_CD=1
      dr_bucket_make small "$BUCKET"
      ;;
  esac
  dr_config "$L_CFG" state="sqlite://$L_DBFILE" source=file input="$L_INPUT" endpoint="$(dr_s3_endpoint "$L_REGION")" \
    bucket="$BUCKET" prefix="$L_PREFIX" work="$L_WORK" batch="$BATCH"
  gen 0 "$BEFORE" > "$L_INPUT"

  dr_engine_start "$L_CFG"
  wait_until 90 "the first $BEFORE records to be archived" drained "$BEFORE"
  ROWS_BEFORE="$(committed_rows)"

  log "fault: the $L_LEG disk is full"
  case "$L_LEG" in
    work | ledger) fill "$L_FULL_DIR" ;;
    store)
      # Fill the store's disk through the store itself: objects until it refuses.
      docker exec "${DR_CONT[minio-small]}" sh -c 'dd if=/dev/zero of=/data/filler bs=512 2>/dev/null; true'
      ;;
  esac
  gen "$BEFORE" "$TOTAL" >> "$L_INPUT"
  sleep "$HOLD_S"
  ALIVE=yes
  dr_engine_alive || ALIVE=no
  ROWS_DURING="$(committed_rows)"
  log "$L_LEG: after ${HOLD_S}s full, the engine is alive: $ALIVE; $((ROWS_DURING - ROWS_BEFORE)) batch(es) committed meanwhile"
  FELT="$(grep -ciE 'no space left|disk is full|database or disk is full|StorageFull|XMinioStorageFull|insufficient|ENOSPC|os error 28' "$(dr_engine_log)" || true)"
  if ((FELT > 0)); then crit ok "$L_LEG: the engine's log names the full disk" "$FELT line(s)"; else crit bad "$L_LEG: the engine's log names the full disk"; fi
}

handover_text() {
  ctx "$1"
  echo "  The $L_LEG disk is full to the last byte, and the source went on growing: it holds $TOTAL records, the engine has"
  echo "  archived $BEFORE. The disk has been full for ${HOLD_S}s; the engine was alive when it was handed over: $ALIVE."
  echo "  The engine's configuration is $L_CFG; its log is $(dr_engine_log)."
  case "$L_LEG" in
    work) echo "  The full disk is the engine's work directory, $L_WORK (a 4 MiB tmpfs); its space is held by $L_WORK/filler." ;;
    ledger) echo "  The full disk is the one the SQLite ledger is on, $L_FULL_DIR (a 4 MiB tmpfs); its space is held by $L_FULL_DIR/filler." ;;
    store) echo "  The full disk is the object store's own: container ${DR_CONT[minio-small]}, whose /data is a 64 MiB tmpfs held by /data/filler." ;;
  esac
  echo "  The ledger is SQLite: $L_DBFILE (query it with sqlite3)."
}

# recover <leg>: free the space and, if the engine is not running, start it.
recover() {
  ctx "$1"
  log "recovery: free the space"
  case "$L_LEG" in
    work | ledger) rm -f "${L_FULL_DIR:?}/filler" ;;
    store) docker exec "${DR_CONT[minio-small]}" rm -f /data/filler ;;
  esac
  T0=$(now_us)
  RESTARTED=no
  if ! dr_engine_alive; then
    log "the engine is not running: start it"
    dr_engine_start "$L_CFG"
    RESTARTED=yes
  fi
  wait_until 180 "the engine to archive every record" drained "$TOTAL"
  RTO_MS=$(ms_since "$T0")
  dr_engine_stop
}

# check <leg>: the pass criteria, from the lab's state alone.
check() {
  ctx "$1"
  dr_checks "$L_LEG" "$L_CFG" -
  crit_eq "$L_LEG: audit-coverage --check-storage exits 0" "$DR_AUDIT_RC" 0
  crit_eq "$L_LEG: scrub exits 0" "$DR_SCRUB_RC" 0
  crit_rto "$L_LEG: RTO, from freeing the space to every record committed" "$RTO_MS" "$VTOP_DR_DISK_FULL_RTO_S"
  dr_readback "$L_REGION" "$BUCKET" "$L_PREFIX" "$L_DIR/rb"
  dr_sqlite "$L_DBFILE" "SELECT object_uri FROM batches WHERE state='source_committed'" | sed 's#^s3://[^/]*/##' > "$L_DIR/committed-keys"
  dr_count_committed "$L_DIR/rb" "$L_PREFIX" "$L_DIR/committed-keys" "$REC" "$TOTAL"
  crit_eq "$L_LEG: no record is missing from the committed objects" "$DR_MISSING" 0
  crit_le "$L_LEG: records archived twice are at most one batch's" "$DR_DUPLICATES" "$BATCH" " records"
  dr_set "${L_LEG}_engine_survived_the_fault" "$ALIVE"
  dr_set "${L_LEG}_restart_needed" "${RESTARTED:-not known after a handover}"
  dr_set "${L_LEG}_rto_ms" "$RTO_MS"
  dr_set "${L_LEG}_batches_committed_while_full" "$((ROWS_DURING - ROWS_BEFORE))"
  dr_set "${L_LEG}_records_missing" "$DR_MISSING"
  dr_set "${L_LEG}_records_archived_twice" "$DR_DUPLICATES"
  dr_check_summary "$DR_MISSING"
  log "$L_LEG: RTO $(secs "$RTO_MS") s; engine survived the fault: $ALIVE; restart needed: ${RESTARTED:-not known after a handover}; $DR_MISSING missing, $DR_DUPLICATES twice"
}

dr_run_drill
