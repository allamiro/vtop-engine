#!/usr/bin/env bash
# Manifest compatibility (#671 release B): a version 0.4 archive written by
# THIS build must verify under every released reader the upgrade order names,
# and fail, as documented, under the releases before it.
#
# The committed fixtures (crates/vtop-core/tests/fixtures/manifests/) prove
# that today's readers read what old writers wrote. This proves the other
# direction for the one place an old binary meets new data: an auditor or a
# scrub still on an older release reading an archive a release-B engine
# wrote. upgrade-compat.sh does not cover it: it opens a vtop-node data
# directory, not an engine archive.
#
# Per old binary:
#   1. This build's vtopctl archives one file to a localfs store with
#      manifest.write_version "0.4", verification receipts on, and a MAC key
#      (the non-secret fixture key, 11 x 32). Its own verify-manifest and
#      `scrub --require-receipts` must pass.
#   2. The OLD vtopctl, with its own ledger (as an auditor's would be) over
#      the same archive and key, runs verify-manifest on that manifest:
#        expect `pass`          the self-hash and keyed MAC verify (v0.8.0+);
#        expect `hash-failure`  it reports the self-hash failure, the
#                               documented cutoff (v0.7.0 and earlier
#                               re-encode the manifest without the fields
#                               they do not know).
#   3. Where the old release has `validate` (v0.8.0+), it accepts the config
#      carrying manifest.write_version and manifest.verification_receipts:
#      an older engine ignores keys it does not know in that section.
#
# Usage: manifest-compat.sh <new-vtopctl> <old-label> <old-vtopctl> <pass|hash-failure>
set -euo pipefail

NEW_BIN="${1:?usage: manifest-compat.sh <new-vtopctl> <old-label> <old-vtopctl> <pass|hash-failure>}"
OLD_LABEL="${2:?old release label}"
OLD_BIN="${3:?old vtopctl binary}"
EXPECT="${4:?expected result: pass or hash-failure}"
[[ -x "$NEW_BIN" ]] || { echo "FAIL: $NEW_BIN is not executable" >&2; exit 1; }
[[ -x "$OLD_BIN" ]] || { echo "FAIL: $OLD_BIN is not executable" >&2; exit 1; }
case "$EXPECT" in
  pass|hash-failure) ;;
  *) echo "FAIL: expected result must be pass or hash-failure, got $EXPECT" >&2; exit 1 ;;
esac

WORKDIR="${MANIFEST_COMPAT_WORKDIR:-$(mktemp -d "${TMPDIR:-/tmp}/vtop-manifest-compat.XXXXXX")}"
mkdir -p "$WORKDIR/logs" "$WORKDIR/store"

log() { echo "[manifest-compat/$OLD_LABEL] $*"; }
fail() { echo "[manifest-compat/$OLD_LABEL] FAIL: $*" >&2; exit 1; }

# The fixture key every signed fixture uses: 11 repeated 32 times. Not a
# secret; it exists so the MAC path is exercised.
VTOP_COMPAT_MAC="$(printf '11%.0s' $(seq 32))"
export VTOP_COMPAT_MAC

printf 'release B manifest compat fixture\n' > "$WORKDIR/input.log"

# Only keys every replayed release knows, plus the manifest section, which
# releases before it ignore (no deny_unknown_fields on the engine config).
write_config() {
  local name="$1" ledger="$2" work="$3"
  cat > "$WORKDIR/$name" <<YAML
engine:
  name: manifest-compat
  state_store: "sqlite://$WORKDIR/$ledger"
  work_dir: $WORKDIR/$work
batching: {}
compression: {type: none}
checksum: {algorithm: sha256}
manifest_mac_key_env: VTOP_COMPAT_MAC
manifest:
  write_version: "0.4"
  verification_receipts: true
sources:
  file:
    enabled: true
    paths: ["$WORKDIR/input.log"]
upload:
  backend: localfs
  bucket: telemetry-data
  local_path: $WORKDIR/store
partitioning: {}
YAML
}
write_config new.yaml new.db new-work
write_config old.yaml old.db old-work

log "this build archives one batch (manifest 0.4, signed, with a receipt)"
"$NEW_BIN" process-once --source file --config "$WORKDIR/new.yaml" \
  > "$WORKDIR/logs/new-process-once.log" 2>&1 \
  || fail "process-once failed; see $WORKDIR/logs/new-process-once.log"

mapfile -t manifests < <(find "$WORKDIR/store" -name '*.manifest.json' | sort)
[[ ${#manifests[@]} -eq 1 ]] || fail "expected one manifest, found ${#manifests[@]}"
manifest="${manifests[0]}"
receipt="${manifest%.manifest.json}.receipt.json"
[[ -f "$receipt" ]] || fail "no verification receipt beside $manifest"
grep -q '"version": "0.4"' "$manifest" || fail "the manifest is not version 0.4: $manifest"
grep -q '"verification_plan"' "$manifest" || fail "the 0.4 manifest has no verification_plan"
grep -q '"mac": "' "$receipt" || fail "the receipt is not signed: $receipt"
uri="s3://${manifest#"$WORKDIR/store/"}"
log "manifest $uri"

"$NEW_BIN" verify-manifest --config "$WORKDIR/new.yaml" --manifest "$uri" \
  > "$WORKDIR/logs/new-verify.log" 2>&1 \
  || fail "this build's verify-manifest failed; see $WORKDIR/logs/new-verify.log"
"$NEW_BIN" scrub --config "$WORKDIR/new.yaml" --require-receipts \
  > "$WORKDIR/logs/new-scrub.log" 2>&1 \
  || fail "this build's scrub --require-receipts failed; see $WORKDIR/logs/new-scrub.log"
grep -q 'receipts: .*ok=1' "$WORKDIR/logs/new-scrub.log" \
  || fail "the scrub did not count the receipt ok; see $WORKDIR/logs/new-scrub.log"

log "$OLD_LABEL verify-manifest, expecting $EXPECT"
set +e
"$OLD_BIN" verify-manifest --config "$WORKDIR/old.yaml" --manifest "$uri" \
  > "$WORKDIR/logs/old-verify.log" 2>&1
status=$?
set -e
case "$EXPECT" in
  pass)
    [[ $status -eq 0 ]] \
      || fail "$OLD_LABEL rejected a release-B 0.4 manifest (exit $status); see $WORKDIR/logs/old-verify.log"
    grep -q 'PASSED' "$WORKDIR/logs/old-verify.log" \
      || fail "$OLD_LABEL exited 0 without PASSED; see $WORKDIR/logs/old-verify.log"
    ;;
  hash-failure)
    [[ $status -ne 0 ]] \
      || fail "$OLD_LABEL passed a 0.4 manifest it should report as tampered; see $WORKDIR/logs/old-verify.log"
    grep -q 'does not match its embedded sha256' "$WORKDIR/logs/old-verify.log" \
      || fail "$OLD_LABEL failed for another reason than the self-hash; see $WORKDIR/logs/old-verify.log"
    ;;
esac

if "$OLD_BIN" validate --help > /dev/null 2>&1; then
  "$OLD_BIN" validate --config "$WORKDIR/old.yaml" \
    > "$WORKDIR/logs/old-validate.log" 2>&1 \
    || fail "$OLD_LABEL refused a config with the manifest section; see $WORKDIR/logs/old-validate.log"
  log "$OLD_LABEL validate accepts manifest.write_version and manifest.verification_receipts"
else
  log "$OLD_LABEL has no validate command; config check skipped"
fi

log "OK: $OLD_LABEL verify-manifest gave the expected result ($EXPECT)"
if [[ -z "${MANIFEST_COMPAT_KEEP:-}" && -z "${MANIFEST_COMPAT_WORKDIR:-}" ]]; then
  rm -rf "$WORKDIR"
fi
