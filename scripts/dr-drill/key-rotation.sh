#!/usr/bin/env bash
# Disaster-recovery drill: rotate the manifest MAC key, then withdraw the old
# one (#735, criterion E4).
#
# An engine archives a file into a store with a manifest MAC key in
# `manifest_mac_key_env`, the way every archive was signed before key ids. The
# drill then runs docs/operate/manifest-key-rotation.md literally, as
# docs/operate/disaster-recovery.md sends the operator there:
#
#   1. add the new key as verify-only, and read the keyring back from
#      `validate --print-effective`;
#   3. switch the active key;
#   4. scrub, and read the count of manifests by the key that verified them;
#   5. tombstone the old key, as for a key that was withdrawn.
#
# Pass criteria:
#   - after step 4 every manifest, written before key ids, under the old key
#     with an id and under the new key, verifies: scrub exits 0, no result but
#     `ok`, and the counts by key add up to the batches archived;
#   - after step 5 the scrub reports exactly the manifests the old key signed
#     as `unauthenticated`, each with the message the page documents, exits
#     non-zero, and reports none of the new key's;
#   - taking the id out of `retired` and back into `keys` makes the scrub clean
#     again: withdrawal is reversible, because no manifest was changed;
#   - a keyring that has forgotten the old key altogether fails with the page's
#     "not in this keyring" message, not with a bare error;
#   - no record is lost: the archive holds every record once.
#
# Measured and bounded: the time the procedure's commands take
# (docs/operate/disaster-recovery.md).
#
# Usage: key-rotation.sh <vtopctl>
# shellcheck source=scripts/dr-drill/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
dr_init key-rotation "${1:-}"
command -v sqlite3 > /dev/null || fail "sqlite3 is needed"

VTOP_DR_KEY_ROTATION_S=120
dr_check_doc VTOP_DR_KEY_ROTATION_S=$VTOP_DR_KEY_ROTATION_S

BUCKET=dr-key-rotation
REC=dr-rec-
PER_PHASE=40
DB="$DR_WORKDIR/state.db"
INPUT="$DR_WORKDIR/input.log"
PREFIX=key-rotation
KEYDIR="$DR_WORKDIR/keys"
mkdir -p "$KEYDIR"
chmod 0700 "$KEYDIR"

dr_minio_start store
dr_bucket_make store "$BUCKET" versioning
EP="$(dr_s3_endpoint store)"

new_key() { python3 -c 'import secrets; print(secrets.token_hex(32))'; }
VTOP_MANIFEST_MAC_KEY="$(new_key)" # the key the archive was signed with before key ids
export VTOP_MANIFEST_MAC_KEY
new_key > "$KEYDIR/q4-2026"
chmod 0600 "$KEYDIR/q4-2026"

KEYRING_BASE="  write_version: \"0.4\"
  mac:"
# cfg <name> <manifest block | -> [top-level yaml]
cfg() {
  local path="$DR_WORKDIR/$1.yaml" manifest="$2" top="${3:-}"
  if [[ "$manifest" == - ]]; then manifest=""; fi
  dr_config "$path" state="sqlite://$DB" source=file input="$INPUT" endpoint="$EP" bucket="$BUCKET" \
    prefix="$PREFIX" work="$DR_WORKDIR/work" manifest="$manifest" top="$top"
  echo "$path"
}

committed_rows() { dr_sqlite_num "$DB" "SELECT count(*) FROM batches WHERE state='source_committed'"; }
rows_ge() { (($(committed_rows) >= $1)); }

# archive <config> <first> <count>: append records to the source, run the engine
# until they are committed, stop it.
archive() {
  local config="$1" first="$2" count="$3" i want
  for ((i = first; i < first + count; i++)); do printf '%s%06d payload for the disaster-recovery drill\n' "$REC" "$i"; done >> "$INPUT"
  want=$(($(committed_rows) + count / 20))
  dr_engine_start "$config"
  wait_until 90 "the engine to commit $want batches" rows_ge "$want"
  dr_engine_stop
}

T_START=$(now_us)

# ---- the archive as it is before the rotation: one key, no key id ---------------
: > "$INPUT"
C0="$(cfg before - 'manifest_mac_key_env: VTOP_MANIFEST_MAC_KEY')"
archive "$C0" 0 "$PER_PHASE"
dr_ctl_rc scrub-before scrub --config "$C0"
crit_eq "before the rotation: scrub exits 0" "$DR_RC" 0

# ---- step 1: add the new key as verify-only, everywhere --------------------------
RING_Q3="$KEYRING_BASE
    active: q3-2026
    keys:
      - {id: q3-2026, env: VTOP_MANIFEST_MAC_KEY, legacy: true}
      - {id: q4-2026, file: $KEYDIR/q4-2026}"
C1="$(cfg step1 "$RING_Q3")"
dr_ctl step1-validate validate --config "$C1" --print-effective
WANT_LINE='manifest_mac: keyring: active key q3-2026 (env, legacy); verify-only: q4-2026 (file); retired: none (global)'
if grep -qF "$WANT_LINE" "$DR_LOGS/step1-validate.log"; then
  crit ok "step 1: validate --print-effective shows the keyring the page shows"
else
  crit bad "step 1: validate --print-effective shows the keyring the page shows" "$(grep manifest_mac "$DR_LOGS/step1-validate.log" || echo 'no manifest_mac line')"
fi
# Batches written while q3-2026 is still the active key now carry its id.
archive "$C1" "$PER_PHASE" "$PER_PHASE"

# ---- step 3: switch the active key ------------------------------------------------
RING_Q4="$KEYRING_BASE
    active: q4-2026
    keys:
      - {id: q3-2026, env: VTOP_MANIFEST_MAC_KEY, legacy: true}
      - {id: q4-2026, file: $KEYDIR/q4-2026}"
C3="$(cfg step3 "$RING_Q4")"
archive "$C3" $((PER_PHASE * 2)) "$PER_PHASE"
TOTAL=$((PER_PHASE * 3))
BATCHES=$((TOTAL / 20))

# ---- step 4: scrub, and read the counts by key ------------------------------------
dr_ctl_rc scrub-step4 scrub --config "$C3"
crit_eq "step 4: scrub exits 0" "$DR_RC" 0
crit_test "step 4: scrub PASSED" "" grep -q "scrub PASSED" "$DR_LOGS/scrub-step4.log"
Q3_BATCHES=$(((PER_PHASE * 2) / 20)) # the unkeyed ones and the ones signed under q3-2026
Q4_BATCHES=$((PER_PHASE / 20))
KEYS_LINE="$(grep '^manifests authenticated by key:' "$DR_LOGS/scrub-step4.log" || true)"
crit_eq "step 4: manifests by key" "$KEYS_LINE" "manifests authenticated by key: q3-2026=$Q3_BATCHES q4-2026=$Q4_BATCHES"
crit_test "step 4: all $BATCHES manifests ok" "$(grep '^results:' "$DR_LOGS/scrub-step4.log" || true)" \
  grep -q "ok=$BATCHES " "$DR_LOGS/scrub-step4.log"

# ---- step 5: tombstone the old key -------------------------------------------------
RING_RETIRED="$KEYRING_BASE
    active: q4-2026
    keys:
      - {id: q4-2026, file: $KEYDIR/q4-2026}
    retired: [q3-2026]"
C5="$(cfg step5 "$RING_RETIRED")"
dr_ctl_rc scrub-step5 scrub --config "$C5"
crit_eq "step 5: scrub of a withdrawn key exits non-zero" "$([[ $DR_RC -ne 0 ]] && echo nonzero || echo zero)" nonzero
crit_test "step 5: exactly the $Q3_BATCHES manifests the old key signed are unauthenticated" "$(grep '^results:' "$DR_LOGS/scrub-step5.log" || true)" \
  grep -q "unauthenticated=$Q3_BATCHES " "$DR_LOGS/scrub-step5.log"
crit_test "step 5: the new key's $Q4_BATCHES manifests still verify" "" grep -q "ok=$Q4_BATCHES " "$DR_LOGS/scrub-step5.log"
crit_test "step 5: the retired-key message is the page's" "" \
  grep -qF 'it names key id "q3-2026", which is retired (manifest.mac.retired)' "$DR_LOGS/scrub-step5.log"
crit_test "step 5: the no-key-id message is the page's" "" \
  grep -qF 'it names no key id and no key in manifest.mac.keys is marked legacy: true' "$DR_LOGS/scrub-step5.log"

# Withdrawal is reversible: no manifest was changed.
dr_ctl_rc scrub-restored scrub --config "$C3"
crit_eq "taking the id out of retired and back into keys: scrub exits 0" "$DR_RC" 0

# A keyring that forgot the old key, rather than tombstoning it.
RING_FORGOT="$KEYRING_BASE
    active: q4-2026
    keys:
      - {id: q4-2026, file: $KEYDIR/q4-2026}"
C6="$(cfg forgot "$RING_FORGOT")"
dr_ctl_rc scrub-forgot scrub --config "$C6"
crit_eq "a keyring that lacks the old key: scrub exits non-zero" "$([[ $DR_RC -ne 0 ]] && echo nonzero || echo zero)" nonzero
crit_test "a keyring that lacks the old key: the manifests with no key id name the cause" "" \
  grep -qF 'no key in manifest.mac.keys is marked legacy: true' "$DR_LOGS/scrub-forgot.log"
crit_test "a keyring that lacks the old key: the manifests with its id name the cause" "" \
  grep -qF 'which is not in this keyring' "$DR_LOGS/scrub-forgot.log"

PROC_MS=$(ms_since "$T_START")
crit_le "the procedure's commands" "$((PROC_MS / 1000))" "$VTOP_DR_KEY_ROTATION_S" " s"

# ---- nothing was lost --------------------------------------------------------------
dr_readback store "$BUCKET" "$PREFIX" "$DR_WORKDIR/rb"
dr_count "$DR_WORKDIR/rb" "$REC" "$TOTAL"
crit_eq "the archive holds every record" "$DR_UNIQUE" "$TOTAL"
crit_eq "no record is archived twice" "$DR_DUPLICATES" 0

dr_set procedure_ms "$PROC_MS"
dr_set batches "$BATCHES"
dr_set manifests_signed_by_old_key "$Q3_BATCHES"
dr_set manifests_signed_by_new_key "$Q4_BATCHES"
dr_set records "$TOTAL"
dr_set records_missing "$DR_MISSING"
dr_finish
