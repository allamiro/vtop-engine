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
# Usage: verification-failed.sh <vtopctl>
# shellcheck source=scripts/dr-drill/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
dr_init verification-failed "${1:-}"
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
: > "$STATS"

dr_minio_start store
dr_bucket_make store "$BUCKET" versioning
EP="$(dr_s3_endpoint store)"

PROXY_PID=""
dr_stop_extra() { [[ -z "$PROXY_PID" ]] || kill "$PROXY_PID" 2> /dev/null || true; }
PROXY_PORT="$(dr_free_port)"
python3 "$DR_LIB_DIR/fault-proxy.py" "$PROXY_PORT" "127.0.0.1:${DR_PORT[minio-store]}" "$CONTROL" "$STATS" \
  > "$DR_LOGS/fault-proxy.log" 2>&1 &
PROXY_PID=$!
wait_until 20 "the fault proxy to listen" curl -s -o /dev/null "http://127.0.0.1:$PROXY_PORT/"

gen() { local i; for ((i = $1; i < $2; i++)); do printf '%s%06d payload for the disaster-recovery drill\n' "$REC" "$i"; done; }

# engine_leg <name> <fault mode> <checksum algorithm>
engine_leg() {
  local leg="$1" mode="$2" algo="$3" dir="$DR_WORKDIR/$1" prefix="verification/$1"
  local db="$dir/state.db" input="$dir/input.log" cfg="$dir/config.yaml" cfg_direct="$dir/config-direct.yaml"
  local rows_before rows_during hits failures rto_ms unref metrics_port stranded
  mkdir -p "$dir"
  log "=== leg: $leg"
  dr_config "$cfg" state="sqlite://$db" source=file input="$input" endpoint="http://127.0.0.1:$PROXY_PORT" \
    bucket="$BUCKET" prefix="$prefix" work="$dir/work" checksum="$algo"
  # The scrub and the audit read the store directly, not through the proxy.
  dr_config "$cfg_direct" state="sqlite://$db" source=file input="$input" endpoint="$EP" \
    bucket="$BUCKET" prefix="$prefix" work="$dir/work" checksum="$algo"
  committed_rows() { dr_sqlite_num "$db" "SELECT count(*) FROM batches WHERE state='source_committed'"; }
  committed_records() { dr_sqlite_num "$db" "SELECT COALESCE(sum(record_count),0) FROM batches WHERE state='source_committed'"; }
  open_rows() { dr_sqlite_num "$db" "SELECT count(*) FROM batches WHERE state NOT IN ('source_committed','superseded','failed','replay_required')"; }
  # Drained: every record is committed.
  drained() { (($(committed_records) >= $1)); }
  metrics_port="$(dr_free_port)"
  metric() { curl -fs "http://127.0.0.1:$metrics_port/metrics" 2> /dev/null | awk -v n="$1" '$1 ~ "^"n { s += $2 } END { print s + 0 }'; }

  gen 0 "$BEFORE" > "$input"
  dr_engine_start "$cfg" VTOP_METRICS_ADDR="127.0.0.1:$metrics_port"
  wait_until 90 "the first $BEFORE records to be archived" drained "$BEFORE"
  rows_before="$(committed_rows)"

  log "fault ($mode): the store is wrong about every data object"
  echo "$mode" > "$CONTROL"
  : > "$STATS"
  gen "$BEFORE" "$TOTAL" >> "$input"
  sleep 20
  rows_during="$(committed_rows)"
  hits="$(wc -l < "$STATS" | tr -d ' ')"
  failures="$(metric vtop_verification_failures_total)"
  crit_eq "$leg: nothing is committed while verification fails" "$rows_during" "$rows_before"
  if ((hits > 0)); then crit ok "$leg: the fault was met" "$hits altered response(s)"; else crit bad "$leg: the fault was met" "the proxy altered nothing"; fi
  if dr_engine_alive; then crit ok "$leg: the engine keeps running under the fault"; else crit bad "$leg: the engine keeps running under the fault"; fi
  if ((failures > 0)); then crit ok "$leg: vtop_verification_failures_total counts it" "$failures"; else crit bad "$leg: vtop_verification_failures_total counts it" "0"; fi
  if grep -q 'object verification failed' "$(dr_engine_log)"; then crit ok "$leg: the log says the object verification failed"; else crit bad "$leg: the log says the object verification failed"; fi

  log "recovery: the store is right again; nothing is restarted"
  rm -f "$CONTROL"
  T0=$(now_us)
  wait_until 120 "the engine to commit every record" drained "$TOTAL"
  rto_ms=$(ms_since "$T0")
  dr_engine_stop
  crit_le "$leg: RTO, from the store being right to everything committed" "$((rto_ms / 1000))" "$VTOP_DR_VERIFICATION_RTO_S" " s"
  stranded="$(open_rows)"
  crit_eq "$leg: the failed attempts are marked failed: no ledger row is left in flight" "$stranded" 0

  dr_checks "$leg" "$cfg_direct" -
  crit_eq "$leg: audit-coverage --check-storage exits 0" "$DR_AUDIT_RC" 0
  crit_eq "$leg: scrub exits 0" "$DR_SCRUB_RC" 0
  unref="$(sed -n 's/^unreferenced objects: \([0-9]*\) object.*/\1/p' "$DR_LOGS/$leg-audit-coverage.log" | head -n 1)"
  unref="${unref:-0}"
  log "$leg: the coverage audit reports $unref unreferenced object(s) from the failed attempts"
  dr_readback store "$BUCKET" "$prefix" "$dir/rb"
  # Committed objects only: the failed attempts' objects hold records too.
  dr_sqlite "$db" "SELECT object_uri FROM batches WHERE state='source_committed'" | sed 's#^s3://[^/]*/##' > "$dir/committed-keys"
  dr_count_committed "$dir/rb" "$prefix" "$dir/committed-keys" "$REC" "$TOTAL"
  crit_eq "$leg: the committed objects hold every record" "$DR_UNIQUE" "$TOTAL"
  crit_eq "$leg: no record is committed twice" "$DR_DUPLICATES" 0
  dr_set "${leg}_rto_ms" "$rto_ms"
  dr_set "${leg}_altered_responses" "$hits"
  dr_set "${leg}_verification_failures_counted" "$failures"
  dr_set "${leg}_unreferenced_objects" "$unref"
  dr_set "${leg}_rows_left_in_flight" "$stranded"
  dr_set "${leg}_records_missing" "$DR_MISSING"
}

engine_leg engine head sha256
engine_leg readback get blake3

# ================= leg: a committed object fails at rest ==========================
log "=== leg: rest"
DB="$DR_WORKDIR/engine/state.db"
CFG_DIRECT="$DR_WORKDIR/engine/config-direct.yaml"
KEY="$(dr_sqlite "$DB" "SELECT object_uri FROM batches WHERE state='source_committed' ORDER BY created_at LIMIT 1" | sed 's#^s3://[^/]*/##')"
BATCH="$(dr_sqlite "$DB" "SELECT batch_id FROM batches WHERE object_uri='s3://$BUCKET/$KEY'")"
log "the object of batch $BATCH is overwritten in the store"
echo "this is not the archived object" | dr_mc_pipe store "local/$BUCKET/$KEY"
dr_ctl_rc rest-scrub-found scrub --config "$CFG_DIRECT"
FOUND_RC=$DR_RC
T0=$(now_us)
crit_eq "rest: the scrub of an overwritten object exits non-zero" "$([[ $FOUND_RC -ne 0 ]] && echo nonzero || echo zero)" nonzero
if grep -q "mismatched=1 " "$DR_LOGS/rest-scrub-found.log"; then
  crit ok "rest: exactly one object is mismatched"
else
  crit bad "rest: exactly one object is mismatched" "$(grep '^results:' "$DR_LOGS/rest-scrub-found.log" || true)"
fi
if grep -q "$BATCH" "$DR_LOGS/rest-scrub-found.log"; then crit ok "rest: the scrub names batch $BATCH"; else crit bad "rest: the scrub names batch $BATCH"; fi

log "recovery: remove the bad version, so that the previous one is current"
BAD_VERSION="$(dr_mc store ls --versions --json "local/$BUCKET/$KEY" | python3 -c '
import json, sys
vs = [json.loads(l) for l in sys.stdin if l.strip()]
print(max(vs, key=lambda v: v.get("versionOrdinal", 0))["versionId"] if vs else "")')"
[[ -n "$BAD_VERSION" ]] || fail "could not find the bad version of $KEY"
dr_mc store rm --version-id "$BAD_VERSION" "local/$BUCKET/$KEY" > /dev/null
dr_ctl_rc rest-scrub-repaired scrub --config "$CFG_DIRECT"
REPAIR_MS=$(ms_since "$T0")
crit_eq "rest: after removing the bad version the scrub exits 0" "$DR_RC" 0
crit_le "rest: from the finding to a clean scrub" "$((REPAIR_MS / 1000))" "$VTOP_DR_VERIFICATION_REPAIR_S" " s"
dr_set rest_repair_ms "$REPAIR_MS"

log "a deleted object has no version to restore"
dr_mc store rm --versions --force "local/$BUCKET/$KEY" > /dev/null
dr_ctl_rc rest-scrub-lost scrub --config "$CFG_DIRECT"
crit_eq "rest: the scrub of a deleted object exits non-zero" "$([[ $DR_RC -ne 0 ]] && echo nonzero || echo zero)" nonzero
if grep -q "missing=1 " "$DR_LOGS/rest-scrub-lost.log"; then
  crit ok "rest: the loss is reported exactly: one object missing"
else
  crit bad "rest: the loss is reported exactly: one object missing" "$(grep '^results:' "$DR_LOGS/rest-scrub-lost.log" || true)"
fi
dr_ctl_rc rest-audit-lost audit-coverage --config "$CFG_DIRECT" --check-storage
crit_eq "rest: audit-coverage --check-storage also fails on it" "$([[ $DR_RC -ne 0 ]] && echo nonzero || echo zero)" nonzero
LOST_RECORDS="$(dr_sqlite "$DB" "SELECT record_count FROM batches WHERE batch_id='$BATCH'")"
dr_set rest_records_lost "$LOST_RECORDS"
dr_set rest_batch_lost "$BATCH"
dr_set records "$TOTAL"
dr_finish
