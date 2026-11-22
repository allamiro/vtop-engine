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
# Usage: key-rotation.sh <vtopctl> [--handover | --check | --teardown]
#
# With --handover the lab is the archive as it is before the rotation (one key,
# no key id) and the new key's file; the second person runs the procedure, writes
# the configuration they end step 4 with to $DR_DRILL_WORKDIR/rotated.yaml, and
# --check judges the archive and that configuration.
# shellcheck source=scripts/dr-drill/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
# shellcheck disable=SC2034  # DR_PAGE_SECTION and DR_HANDOVER_* are read by lib.sh
dr_init key-rotation "$@"
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
ROTATED="$DR_WORKDIR/rotated.yaml"

DR_PAGE_SECTION="Key rotation"
DR_HANDOVER_VARS=(VTOP_MANIFEST_MAC_KEY EP)

new_key() { python3 -c 'import secrets; print(secrets.token_hex(32))'; }

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

# lab: the archive as it is before the rotation: one key, no key id.
lab() {
  mkdir -p "$KEYDIR"
  chmod 0700 "$KEYDIR"
  dr_minio_start store
  dr_bucket_make store "$BUCKET" versioning
  EP="$(dr_s3_endpoint store)"
  VTOP_MANIFEST_MAC_KEY="$(new_key)" # the key the archive was signed with before key ids
  export VTOP_MANIFEST_MAC_KEY
  DR_HANDOVER_ENV+=("VTOP_MANIFEST_MAC_KEY=$VTOP_MANIFEST_MAC_KEY")
  new_key > "$KEYDIR/q4-2026"
  chmod 0600 "$KEYDIR/q4-2026"
  # What a second person needs to add records, numbered on from the archive's.
  cat > "$DR_WORKDIR/append-records.sh" << 'SH'
#!/usr/bin/env bash
# append-records.sh <count>: append <count> numbered records to the source file.
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
first="$(wc -l < "$dir/input.log" | tr -d ' ')"
for ((i = first; i < first + ${1:?usage: append-records.sh <count>}; i++)); do
  printf 'dr-rec-%06d payload for the disaster-recovery drill\n' "$i"
done >> "$dir/input.log"
SH
  chmod 0700 "$DR_WORKDIR/append-records.sh"

  T_START=$(now_us)
  : > "$INPUT"
  C0="$(cfg before - 'manifest_mac_key_env: VTOP_MANIFEST_MAC_KEY')"
  archive "$C0" 0 "$PER_PHASE"
  dr_ctl_rc scrub-before scrub --config "$C0"
  crit_eq "before the rotation: scrub exits 0" "$DR_RC" 0
}

handover_text() {
  echo "  The archive was written the way every archive was before key ids: $PER_PHASE records in $((PER_PHASE / 20)) batches, one"
  echo "  manifest MAC key, no key id, in the environment variable VTOP_MANIFEST_MAC_KEY (exported by lab.env). A new key,"
  echo "  for the id q4-2026, is in $KEYDIR/q4-2026. The old key is to become the id q3-2026 (legacy: true)."
  echo "  The engine is stopped. The configuration before the rotation is $DR_WORKDIR/before.yaml; the source is the file"
  echo "  $INPUT, and $DR_WORKDIR/append-records.sh <count> appends numbered records to it, for the batches"
  echo "  the procedure archives under each key. The ledger is SQLite: $DB. The store is $EP, bucket $BUCKET."
  echo "  The procedure is docs/operate/manifest-key-rotation.md, which this section of the page sends you to:"
  echo "  steps 1, 3, 4 and 5 (add the new key verify-only; switch the active key; scrub; tombstone the old key)."
  echo "  Do the steps with records archived under each key, then put the configuration you end step 4 with"
  echo "  (the new key active, the old one still verifying) in $ROTATED before you run --check."
}

# recover: the procedure, as the page runs it.
recover() {
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
  C3="$(ring_q4_config step3)"
  archive "$C3" $((PER_PHASE * 2)) "$PER_PHASE"
  cp "$C3" "$ROTATED"

  # ---- step 4: scrub, and read the counts by key ------------------------------------
  dr_ctl_rc scrub-step4 scrub --config "$C3"
  crit_eq "step 4: scrub exits 0" "$DR_RC" 0
  crit_test "step 4: scrub PASSED" "" grep -q "scrub PASSED" "$DR_LOGS/scrub-step4.log"
  local q3=$(((PER_PHASE * 2) / 20)) q4=$((PER_PHASE / 20)) batches=$(((PER_PHASE * 3) / 20)) keys_line
  keys_line="$(grep '^manifests authenticated by key:' "$DR_LOGS/scrub-step4.log" || true)"
  crit_eq "step 4: manifests by key" "$keys_line" "manifests authenticated by key: q3-2026=$q3 q4-2026=$q4"
  crit_test "step 4: all $batches manifests ok" "$(grep '^results:' "$DR_LOGS/scrub-step4.log" || true)" \
    grep -q "ok=$batches " "$DR_LOGS/scrub-step4.log"
}

# ring_q4_config <name>: the configuration step 3 leaves, written by this drill.
ring_q4_config() {
  local ring="$KEYRING_BASE
    active: q4-2026
    keys:
      - {id: q3-2026, env: VTOP_MANIFEST_MAC_KEY, legacy: true}
      - {id: q4-2026, file: $KEYDIR/q4-2026}"
  cfg "$1" "$ring"
}

# check: the pass criteria, from the archive and the configuration the procedure ended step 4 with.
check() {
  [[ -f "$ROTATED" ]] || fail "$ROTATED does not exist: put the configuration you end step 4 with there"
  local batches total q3 q4 keys_line
  batches="$(committed_rows)"
  total="$(wc -l < "$INPUT" | tr -d ' ')"

  # After step 4: the person's configuration verifies every manifest, under both keys.
  dr_ctl_rc scrub-rotated scrub --config "$ROTATED"
  crit_eq "after step 4: scrub exits 0" "$DR_RC" 0
  crit_test "after step 4: scrub PASSED" "" grep -q "scrub PASSED" "$DR_LOGS/scrub-rotated.log"
  crit_test "after step 4: every one of the $batches manifests is ok" "$(grep '^results:' "$DR_LOGS/scrub-rotated.log" || true)" \
    grep -q "ok=$batches " "$DR_LOGS/scrub-rotated.log"
  keys_line="$(grep '^manifests authenticated by key:' "$DR_LOGS/scrub-rotated.log" || true)"
  q3="$(sed -n 's/.*q3-2026=\([0-9][0-9]*\).*/\1/p' <<< "$keys_line")"
  q4="$(sed -n 's/.*q4-2026=\([0-9][0-9]*\).*/\1/p' <<< "$keys_line")"
  q3="${q3:-0}"
  q4="${q4:-0}"
  crit_eq "after step 4: the counts by key add up to the batches" "$((q3 + q4))" "$batches"
  crit_ge "after step 4: the old key signed manifests" "$q3" 1
  crit_ge "after step 4: the new key signed manifests" "$q4" 1

  # After step 5: the old key withdrawn (written here, from the lab's keys).
  local c5 c6 ring_retired ring_forgot
  ring_retired="$KEYRING_BASE
    active: q4-2026
    keys:
      - {id: q4-2026, file: $KEYDIR/q4-2026}
    retired: [q3-2026]"
  c5="$(cfg step5 "$ring_retired")"
  dr_ctl_rc scrub-step5 scrub --config "$c5"
  crit_eq "step 5: scrub of a withdrawn key exits non-zero" "$([[ $DR_RC -ne 0 ]] && echo nonzero || echo zero)" nonzero
  crit_test "step 5: exactly the $q3 manifests the old key signed are unauthenticated" "$(grep '^results:' "$DR_LOGS/scrub-step5.log" || true)" \
    grep -q "unauthenticated=$q3 " "$DR_LOGS/scrub-step5.log"
  crit_test "step 5: the new key's $q4 manifests still verify" "" grep -q "ok=$q4 " "$DR_LOGS/scrub-step5.log"
  crit_test "step 5: the retired-key message is the page's" "" \
    grep -qF 'it names key id "q3-2026", which is retired (manifest.mac.retired)' "$DR_LOGS/scrub-step5.log"
  # Withdrawal is reversible: no manifest was changed.
  dr_ctl_rc scrub-restored scrub --config "$ROTATED"
  crit_eq "taking the id out of retired and back into keys: scrub exits 0" "$DR_RC" 0

  # A keyring that forgot the old key, rather than tombstoning it.
  ring_forgot="$KEYRING_BASE
    active: q4-2026
    keys:
      - {id: q4-2026, file: $KEYDIR/q4-2026}"
  c6="$(cfg forgot "$ring_forgot")"
  dr_ctl_rc scrub-forgot scrub --config "$c6"
  crit_eq "a keyring that lacks the old key: scrub exits non-zero" "$([[ $DR_RC -ne 0 ]] && echo nonzero || echo zero)" nonzero
  crit_test "a keyring that lacks the old key: the manifests with its id name the cause" "" \
    grep -qF 'which is not in this keyring' "$DR_LOGS/scrub-forgot.log"
  # A run's archive has batches with no key id; one a person made may not.
  if [[ $DR_MODE == run ]] || grep -q 'it names no key id' "$DR_LOGS/scrub-step5.log"; then
    crit_test "step 5: the no-key-id message is the page's" "" \
      grep -qF 'it names no key id and no key in manifest.mac.keys is marked legacy: true' "$DR_LOGS/scrub-step5.log"
    crit_test "a keyring that lacks the old key: the manifests with no key id name the cause" "" \
      grep -qF 'no key in manifest.mac.keys is marked legacy: true' "$DR_LOGS/scrub-forgot.log"
  fi

  [[ $DR_MODE != run ]] || RTO_MS=$(ms_since "$T_START")
  crit_rto "the procedure's commands" "$RTO_MS" "$VTOP_DR_KEY_ROTATION_S"

  # ---- nothing was lost --------------------------------------------------------------
  dr_readback store "$BUCKET" "$PREFIX" "$DR_WORKDIR/rb"
  dr_count "$DR_WORKDIR/rb" "$REC" "$total"
  crit_eq "the archive holds every record" "$DR_UNIQUE" "$total"
  crit_eq "no record is archived twice" "$DR_DUPLICATES" 0

  dr_set procedure_ms "$RTO_MS"
  dr_set batches "$batches"
  dr_set manifests_signed_by_old_key "$q3"
  dr_set manifests_signed_by_new_key "$q4"
  dr_set records "$total"
  dr_set records_missing "$DR_MISSING"
  dr_check_summary "$DR_MISSING"
}

dr_run_drill
