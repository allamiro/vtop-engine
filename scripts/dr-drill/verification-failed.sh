#!/usr/bin/env bash
# Disaster-recovery drill: verification fails (#735, criterion E4).
#
# Three ways a stored object can fail to verify, and the runbook for each
# (docs/operate/disaster-recovery.md, "Verification fails"). A proxy between the
# engine and the store makes the store wrong on request.
#
#   engine    The store's own checksum for a data object disagrees with the one
#             the engine sent (the proxy alters the SHA-256 a HEAD reports). Pass
#             criteria: nothing is committed while it holds (the commit rule: no
#             SOURCE_COMMITTED row without a strong verification); the failure is
#             counted in `vtop_verification_failures_total` and logged as an
#             object verification failure; when the store is right again the
#             engine commits every batch with no restart, and the committed
#             objects hold every record once; the objects the failed attempts
#             left are reported by the coverage audit, as the page says.
#
#   readback  The store returns other bytes than it holds, while the engine reads
#             a batch back (BLAKE3 verification reads the body): the proxy
#             changes the first byte of every data object a GET returns. The
#             same pass criteria as the engine leg. Before #735 this leg was
#             reported as an I/O error and not counted; the first run of this
#             drill found it.
#
#   rest      A committed object is overwritten in the store (bit rot, a bad
#             restore) behind the engine's back. Pass criteria: the scrub reports
#             it as `mismatched`, naming the batch, and exits non-zero; removing
#             the bad version, the page's step, brings the good one back and the
#             scrub is clean; a deleted object with no version to restore is
#             reported as `missing`, exactly, and is a loss the drill reports.
#
# Measured and bounded (docs/operate/disaster-recovery.md): from the store being
# right again until the engine has committed everything (RTO, the two engine
# legs), and from the scrub's finding until a clean scrub (rest leg).
#
# Usage: verification-failed.sh <vtopctl> [--handover | --check | --teardown] [--leg engine|readback|rest]
# shellcheck source=scripts/dr-drill/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
# shellcheck disable=SC2034  # DR_LEGS, DR_PAGE_SECTION and DR_HANDOVER_VARS are read by lib.sh
dr_init verification-failed "$@"
command -v sqlite3 > /dev/null || fail "sqlite3 is needed"

VTOP_DR_VERIFICATION_RTO_S=60
VTOP_DR_VERIFICATION_REPAIR_S=60
dr_check_doc VTOP_DR_VERIFICATION_RTO_S=$VTOP_DR_VERIFICATION_RTO_S VTOP_DR_VERIFICATION_REPAIR_S=$VTOP_DR_VERIFICATION_REPAIR_S

BUCKET=dr-verification
REC=dr-rec-
BEFORE=40
DURING=60
TOTAL=$((BEFORE + DURING))
CONTROL="$DR_WORKDIR/fault-on"
STATS="$DR_WORKDIR/fault-hits"

DR_LEGS=(engine readback rest)
DR_PAGE_SECTION="Verification fails"
DR_HANDOVER_VARS=(PROXY_PID PROXY_PORT EP HITS FAILURES ROWS_BEFORE ROWS_DURING KEY BATCH_ID L_METRICS_PORT)

PROXY_PID=""
dr_stop_extra() { [[ -z "$PROXY_PID" ]] || kill "$PROXY_PID" 2> /dev/null || true; }

gen() { local i; for ((i = $1; i < $2; i++)); do printf '%s%06d payload for the disaster-recovery drill\n' "$REC" "$i"; done; }

# ctx <leg>: the leg's names, the same in every phase.
ctx() {
  L_LEG="$1"
  L_DIR="$DR_WORKDIR/$1"
  L_PREFIX="verification/$1"
  L_DBFILE="$L_DIR/state.db"
  L_INPUT="$L_DIR/input.log"
  L_CFG="$L_DIR/config.yaml"
  L_CFG_DIRECT="$L_DIR/config-direct.yaml"
  case "$1" in
    engine) L_MODE=head L_ALGO=sha256 ;;
    readback) L_MODE=get L_ALGO=blake3 ;;
    rest) L_MODE="" L_ALGO=sha256 ;;
  esac
}
committed_rows() { dr_sqlite_num "$L_DBFILE" "SELECT count(*) FROM batches WHERE state='source_committed'"; }
committed_records() { dr_sqlite_num "$L_DBFILE" "SELECT COALESCE(sum(record_count),0) FROM batches WHERE state='source_committed'"; }
open_rows() { dr_sqlite_num "$L_DBFILE" "SELECT count(*) FROM batches WHERE state NOT IN ('source_committed','superseded','failed','replay_required')"; }
# Drained: every record is committed.
drained() { (($(committed_records) >= $1)); }

lab_shared() {
  dr_minio_start store
  dr_bucket_make store "$BUCKET" versioning
  EP="$(dr_s3_endpoint store)"
  : > "$STATS"
  PROXY_PORT="$(dr_free_port)"
  dr_bg "$DR_LOGS/fault-proxy.log" python3 "$DR_LIB_DIR/fault-proxy.py" "$PROXY_PORT" "127.0.0.1:${DR_PORT[minio-store]}" "$CONTROL" "$STATS"
  PROXY_PID="$DR_BG_PID"
  wait_until 20 "the fault proxy to listen" curl -s -o /dev/null "http://127.0.0.1:$PROXY_PORT/"
}

# lab <leg>: a working archive, then the fault.
lab() {
  ctx "$1"
  mkdir -p "$L_DIR"
  log "=== leg: $L_LEG"
  if [[ "$L_LEG" == rest ]]; then
    lab_rest
    return
  fi
  local metrics_port
  dr_config "$L_CFG" state="sqlite://$L_DBFILE" source=file input="$L_INPUT" endpoint="http://127.0.0.1:$PROXY_PORT" \
    bucket="$BUCKET" prefix="$L_PREFIX" work="$L_DIR/work" checksum="$L_ALGO"
  # The scrub and the audit read the store directly, not through the proxy.
  dr_config "$L_CFG_DIRECT" state="sqlite://$L_DBFILE" source=file input="$L_INPUT" endpoint="$EP" \
    bucket="$BUCKET" prefix="$L_PREFIX" work="$L_DIR/work" checksum="$L_ALGO"
  metrics_port="$(dr_free_port)"
  L_METRICS_PORT="$metrics_port"

  gen 0 "$BEFORE" > "$L_INPUT"
  dr_engine_start "$L_CFG" VTOP_METRICS_ADDR="127.0.0.1:$metrics_port"
  wait_until 90 "the first $BEFORE records to be archived" drained "$BEFORE"
  ROWS_BEFORE="$(committed_rows)"

  log "fault ($L_MODE): the store is wrong about every data object"
  echo "$L_MODE" > "$CONTROL"
  : > "$STATS"
  gen "$BEFORE" "$TOTAL" >> "$L_INPUT"
  sleep 20
  ROWS_DURING="$(committed_rows)"
  HITS="$(wc -l < "$STATS" | tr -d ' ')"
  FAILURES="$(metric vtop_verification_failures_total)"
  crit_eq "$L_LEG: nothing is committed while verification fails" "$ROWS_DURING" "$ROWS_BEFORE"
  if ((HITS > 0)); then crit ok "$L_LEG: the fault was met" "$HITS altered response(s)"; else crit bad "$L_LEG: the fault was met" "the proxy altered nothing"; fi
  if dr_engine_alive; then crit ok "$L_LEG: the engine keeps running under the fault"; else crit bad "$L_LEG: the engine keeps running under the fault"; fi
  if ((FAILURES > 0)); then crit ok "$L_LEG: vtop_verification_failures_total counts it" "$FAILURES"; else crit bad "$L_LEG: vtop_verification_failures_total counts it" "0"; fi
  if grep -q 'object verification failed' "$(dr_engine_log)"; then crit ok "$L_LEG: the log says the object verification failed"; else crit bad "$L_LEG: the log says the object verification failed"; fi
}
metric() { curl -fs "http://127.0.0.1:$L_METRICS_PORT/metrics" 2> /dev/null | awk -v n="$1" '$1 ~ "^"n { s += $2 } END { print s + 0 }'; }

# lab_rest: a clean archive, then one committed object is overwritten behind the engine's back.
lab_rest() {
  dr_config "$L_CFG_DIRECT" state="sqlite://$L_DBFILE" source=file input="$L_INPUT" endpoint="$EP" \
    bucket="$BUCKET" prefix="$L_PREFIX" work="$L_DIR/work" checksum="$L_ALGO"
  gen 0 "$TOTAL" > "$L_INPUT"
  dr_engine_start "$L_CFG_DIRECT"
  wait_until 120 "the engine to commit every record" drained "$TOTAL"
  dr_engine_stop
  KEY="$(dr_sqlite "$L_DBFILE" "SELECT object_uri FROM batches WHERE state='source_committed' ORDER BY created_at LIMIT 1" | sed 's#^s3://[^/]*/##')"
  BATCH_ID="$(dr_sqlite "$L_DBFILE" "SELECT batch_id FROM batches WHERE object_uri='s3://$BUCKET/$KEY'")"
  log "the object of batch $BATCH_ID is overwritten in the store"
  echo "this is not the archived object" | dr_mc_pipe store "local/$BUCKET/$KEY"
}

handover_text() {
  ctx "$1"
  if [[ "$L_LEG" == rest ]]; then
    echo "  A committed object of the archive was overwritten in the store behind the engine's back: s3://$BUCKET/$KEY"
    echo "  (batch $BATCH_ID). The archive holds $TOTAL records in the store, which keeps object versions; the engine is stopped."
    echo "  The engine's configuration is $L_CFG_DIRECT (it talks to the store directly); the ledger is SQLite: $L_DBFILE."
    return
  fi
  echo "  The store is wrong about every data object the engine reads back: an altered response from a faulty gateway"
  echo "  ($L_MODE, checksum $L_ALGO). It has been that way for 20 s; $HITS response(s) were altered, nothing was committed"
  echo "  meanwhile, and the engine is running (pid $DR_ENGINE_PID) and keeps trying. $TOTAL records are in its source."
  echo "  The engine talks to the store through a fault-injecting proxy, http://127.0.0.1:$PROXY_PORT, which stands for the"
  echo "  faulty gateway. The fault is on while the file $CONTROL exists; the store itself, $EP, is right."
  echo "  The engine's configuration is $L_CFG; for the scrub and the audit use $L_CFG_DIRECT (the store, directly)."
  echo "  The ledger is SQLite: $L_DBFILE. The engine's metrics are at http://127.0.0.1:$L_METRICS_PORT/metrics."
  echo "  Fixing the store, for this lab, is removing $CONTROL. Do not restart the engine."
}

# recover <leg>
recover() {
  ctx "$1"
  if [[ "$L_LEG" == rest ]]; then
    recover_rest
    return
  fi
  log "recovery: the store is right again; nothing is restarted"
  rm -f "$CONTROL"
  T0=$(now_us)
  wait_until 120 "the engine to commit every record" drained "$TOTAL"
  RTO_MS=$(ms_since "$T0")
  dr_engine_stop
}

recover_rest() {
  dr_ctl_rc rest-scrub-found scrub --config "$L_CFG_DIRECT"
  local found_rc=$DR_RC
  T0=$(now_us)
  crit_eq "rest: the scrub of an overwritten object exits non-zero" "$([[ $found_rc -ne 0 ]] && echo nonzero || echo zero)" nonzero
  if grep -q "mismatched=1 " "$DR_LOGS/rest-scrub-found.log"; then
    crit ok "rest: exactly one object is mismatched"
  else
    crit bad "rest: exactly one object is mismatched" "$(grep '^results:' "$DR_LOGS/rest-scrub-found.log" || true)"
  fi
  if grep -q "$BATCH_ID" "$DR_LOGS/rest-scrub-found.log"; then crit ok "rest: the scrub names batch $BATCH_ID"; else crit bad "rest: the scrub names batch $BATCH_ID"; fi

  log "recovery: remove the bad version, so that the previous one is current"
  local bad_version
  bad_version="$(dr_mc store ls --versions --json "local/$BUCKET/$KEY" | python3 -c '
import json, sys
vs = [json.loads(l) for l in sys.stdin if l.strip()]
print(max(vs, key=lambda v: v.get("versionOrdinal", 0))["versionId"] if vs else "")')"
  [[ -n "$bad_version" ]] || fail "could not find the bad version of $KEY"
  dr_mc store rm --version-id "$bad_version" "local/$BUCKET/$KEY" > /dev/null
  dr_ctl_rc rest-scrub-repaired scrub --config "$L_CFG_DIRECT"
  RTO_MS=$(ms_since "$T0")
}

# check <leg>: the pass criteria, from the lab's state alone.
check() {
  ctx "$1"
  if [[ "$L_LEG" == rest ]]; then
    check_rest
    return
  fi
  local stranded unref
  crit_rto "$L_LEG: RTO, from the store being right to everything committed" "$RTO_MS" "$VTOP_DR_VERIFICATION_RTO_S"
  stranded="$(open_rows)"
  crit_eq "$L_LEG: the failed attempts are marked failed: no ledger row is left in flight" "$stranded" 0

  dr_checks "$L_LEG" "$L_CFG_DIRECT" -
  crit_eq "$L_LEG: audit-coverage --check-storage exits 0" "$DR_AUDIT_RC" 0
  crit_eq "$L_LEG: scrub exits 0" "$DR_SCRUB_RC" 0
  unref="$(sed -n 's/^unreferenced objects: \([0-9]*\) object.*/\1/p' "$DR_LOGS/$L_LEG-audit-coverage.log" | head -n 1)"
  unref="${unref:-0}"
  log "$L_LEG: the coverage audit reports $unref unreferenced object(s) from the failed attempts"
  dr_readback store "$BUCKET" "$L_PREFIX" "$L_DIR/rb"
  # Committed objects only: the failed attempts' objects hold records too.
  dr_sqlite "$L_DBFILE" "SELECT object_uri FROM batches WHERE state='source_committed'" | sed 's#^s3://[^/]*/##' > "$L_DIR/committed-keys"
  dr_count_committed "$L_DIR/rb" "$L_PREFIX" "$L_DIR/committed-keys" "$REC" "$TOTAL"
  crit_eq "$L_LEG: the committed objects hold every record" "$DR_UNIQUE" "$TOTAL"
  crit_eq "$L_LEG: no record is committed twice" "$DR_DUPLICATES" 0
  dr_set "${L_LEG}_rto_ms" "$RTO_MS"
  dr_set "${L_LEG}_altered_responses" "$HITS"
  dr_set "${L_LEG}_verification_failures_counted" "$FAILURES"
  dr_set "${L_LEG}_unreferenced_objects" "$unref"
  dr_set "${L_LEG}_rows_left_in_flight" "$stranded"
  dr_set "${L_LEG}_records_missing" "$DR_MISSING"
  dr_check_summary "$DR_MISSING"
}

check_rest() {
  dr_ctl_rc rest-scrub-final scrub --config "$L_CFG_DIRECT"
  crit_eq "rest: after removing the bad version the scrub exits 0" "$DR_RC" 0
  crit_rto "rest: from the finding to a clean scrub" "$RTO_MS" "$VTOP_DR_VERIFICATION_REPAIR_S"
  dr_set rest_repair_ms "$RTO_MS"
  dr_set records "$TOTAL"
  if [[ $DR_RC -eq 0 ]]; then
    dr_check_summary 0
  else
    dr_check_summary "$(dr_sqlite "$L_DBFILE" "SELECT record_count FROM batches WHERE batch_id='$BATCH_ID'")"
  fi
}

# leg_done <leg>: after the rest leg's own criteria, a run shows the page's last
# case: a deleted object with no version to restore. It destroys the object, so
# it is a run's, never --check's.
leg_done() {
  ctx "$1"
  [[ "$L_LEG" == rest ]] || return 0
  log "a deleted object has no version to restore"
  dr_mc store rm --versions --force "local/$BUCKET/$KEY" > /dev/null
  dr_ctl_rc rest-scrub-lost scrub --config "$L_CFG_DIRECT"
  crit_eq "rest: the scrub of a deleted object exits non-zero" "$([[ $DR_RC -ne 0 ]] && echo nonzero || echo zero)" nonzero
  if grep -q "missing=1 " "$DR_LOGS/rest-scrub-lost.log"; then
    crit ok "rest: the loss is reported exactly: one object missing"
  else
    crit bad "rest: the loss is reported exactly: one object missing" "$(grep '^results:' "$DR_LOGS/rest-scrub-lost.log" || true)"
  fi
  dr_ctl_rc rest-audit-lost audit-coverage --config "$L_CFG_DIRECT" --check-storage
  crit_eq "rest: audit-coverage --check-storage also fails on it" "$([[ $DR_RC -ne 0 ]] && echo nonzero || echo zero)" nonzero
  dr_set rest_records_lost "$(dr_sqlite "$L_DBFILE" "SELECT record_count FROM batches WHERE batch_id='$BATCH_ID'")"
  dr_set rest_batch_lost "$BATCH_ID"
}

dr_run_drill
