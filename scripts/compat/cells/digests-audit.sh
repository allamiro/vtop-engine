#!/usr/bin/env bash
# The digests-audit cell (#743): the prune digest and the audit trail across a
# release and this build. ledger-scenario.sh makes both: `prune-ledger`
# writes a digest (a copy of the ledger's schema that outlives the ledger)
# and, from v0.10.0 on, two `audit_events` rows.
#
#   upgrade  the released vtopctl writes them; this build's `audit-coverage`
#            and `scrub` read the digest and `audit verify` and `audit list`
#            read the audit chain;
#   forward  this build writes them; the released vtopctl reads the digest
#            (`audit-coverage`, and `scrub` where the release has it) and
#            the chain (`audit verify` and `audit list`, where it has them).
#
# A command a release does not have is skipped for it, not failed: the cell
# runs what that release can. Every command that runs must exit 0, report
# PASSED, and the audit list must show the prune; or, where expected.yaml says
# `refuse`, the first command that fails must print the message and leave the
# ledger exactly as it was.
#
# Usage: digests-audit.sh <tag> <direction> <expect> <released-bin-dir> <head-bin-dir> <workdir> [message]
set -euo pipefail
[[ $# -ge 6 ]] || { echo "usage: digests-audit.sh <tag> <direction> <expect> <released-bin-dir> <head-bin-dir> <workdir> [message]" >&2; exit 2; }
TAG="$1" DIRECTION="$2" EXPECT="$3" RELEASED="$4/vtopctl" HEAD="$5/vtopctl" WORKDIR="$6"
MESSAGE="${7:-}"
CELL=digests-audit
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/compat/cells/lib.sh
source "$here/lib.sh"

case "$EXPECT" in
  pass | refuse) ;;
  *) fail "the digests-audit cell expects pass or refuse, not $EXPECT" ;;
esac
DIR="$WORKDIR/ledger"
rm -rf "$DIR"
mkdir -p "$WORKDIR"

case "$DIRECTION" in
  upgrade) writer="$RELEASED" reader="$HEAD" ;;
  forward) writer="$HEAD" reader="$RELEASED" ;;
  *) fail "no direction $DIRECTION" ;;
esac

(umask 022 && "$here/../ledger-scenario.sh" "$writer" "$DIR") > /dev/null 2> "$WORKDIR/scenario.log" \
  || { cat "$WORKDIR/scenario.log" >&2; fail "the writer could not make the ledger and its digest"; }
if [[ "$DIRECTION" == upgrade ]]; then
  adopt_ledger "$HEAD" "$DIR"
fi
require_ledger "$DIR" "the writer"
ls "$DIR"/state.db.ledger-digests/ledger-digest-*.jsonl > /dev/null \
  || fail "prune-ledger wrote no digest"
digest_rows="$(cat "$DIR"/state.db.ledger-digests/ledger-digest-*.jsonl | grep -c '"batch_id"' || true)"
[[ "$digest_rows" -ge 1 ]] || fail "the digest holds no rows"
if [[ "$DIRECTION" == forward ]]; then
  # The reader must meet what this build adds to a digest row (#626): the
  # `sealed_at` key, which a release before it ignores as an unknown field.
  grep -q '"sealed_at"' "$DIR"/state.db.ledger-digests/ledger-digest-*.jsonl \
    || fail "this build's digest rows carry no sealed_at, so the release is not tested on it"
fi

ledger_before="$(ledger_digest "$DIR/state.db")"
refused=""
ran=""

# step <name> <must-contain> <command...>: run it unless an earlier step was
# refused. A non-zero exit is a refusal (judged below); a zero exit must
# print <must-contain>.
step() {
  local name="$1" needle="$2"
  shift 2
  [[ -z "$refused" ]] || return 0
  run "$name" "$@"
  if [[ "$RC" -ne 0 ]]; then
    refused="$name"
    return 0
  fi
  grep -qE -- "$needle" "$WORKDIR/logs/$name.log" \
    || { sed 's/^/    /' "$WORKDIR/logs/$name.log" >&2; fail "$name exited 0 without '$needle' in its output"; }
  ran="$ran $name"
}

step reader-audit-coverage 'coverage audit PASSED' "$reader" audit-coverage --config "$DIR/good.yaml"
if [[ -z "$refused" ]]; then
  grep -qE "from [1-9][0-9]* digest file" "$WORKDIR/logs/reader-audit-coverage.log" \
    || { cat "$WORKDIR/logs/reader-audit-coverage.log" >&2; fail "audit-coverage did not read the digest"; }
fi
if has_command "$reader" scrub; then
  step reader-scrub 'scrub PASSED' "$reader" scrub --config "$DIR/good.yaml"
fi
# The audit chain exists from v0.10.0, so it needs a writer and a reader that
# both have it.
if has_command "$writer" audit && has_command "$reader" audit; then
  step reader-audit-verify 'verdict: PASSED' "$reader" audit verify --config "$DIR/good.yaml"
  step reader-audit-list 'ledger\.prune' "$reader" audit list --config "$DIR/good.yaml"
else
  log "no audit chain on one side (it starts with v0.10.0): not checked"
fi

case "$EXPECT" in
  pass)
    if [[ -n "$refused" ]]; then
      sed 's/^/    /' "$WORKDIR/logs/$refused.log" >&2
      fail "$refused exited non-zero; expected pass"
    fi
    log "ok: $digest_rows digested row(s) read by:$ran"
    ;;
  refuse)
    [[ -n "$refused" ]] || fail "expected a refusal and every command passed"
    grep -qE -- "$MESSAGE" "$WORKDIR/logs/$refused.log" \
      || { sed 's/^/    /' "$WORKDIR/logs/$refused.log" >&2; fail "$refused refused, but its output does not match '$MESSAGE'"; }
    [[ "$(ledger_digest "$DIR/state.db")" == "$ledger_before" ]] \
      || fail "the refusal changed the ledger"
    log "ok: $refused refused with the expected message and changed nothing"
    ;;
  *) fail "the digests-audit cell expects pass or refuse, not $EXPECT" ;;
esac
