#!/usr/bin/env bash
# The ledger-sqlite cell (#743): a SQLite ledger across a release and this
# build.
#
#   upgrade  the released vtopctl writes the ledger (ledger-scenario.sh);
#            this build opens it, reads every row, audits and scrubs it,
#            archives new input, and leaves every committed batch committed;
#   forward  this build writes it; the released vtopctl runs `status`,
#            `list-batches` and `process-once` over it. Expected `pass`: the
#            same, with nothing committed lost. Expected `refuse`: some
#            command exits non-zero with <message> in its output, and the
#            ledger's content (and `PRAGMA user_version`) is exactly as this
#            build left it.
#
# Usage: ledger-sqlite.sh <tag> <direction> <expect> <released-bin-dir> <head-bin-dir> <workdir> [message]
set -euo pipefail
[[ $# -ge 6 ]] || { echo "usage: ledger-sqlite.sh <tag> <direction> <expect> <released-bin-dir> <head-bin-dir> <workdir> [message]" >&2; exit 2; }
TAG="$1" DIRECTION="$2" EXPECT="$3" RELEASED="$4/vtopctl" HEAD="$5/vtopctl" WORKDIR="$6"
MESSAGE="${7:-}"
CELL=ledger-sqlite
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/compat/cells/lib.sh
source "$here/lib.sh"

DIR="$WORKDIR/ledger"
rm -rf "$DIR"
mkdir -p "$WORKDIR"

case "$DIRECTION" in
  upgrade)
    [[ "$EXPECT" == pass ]] || fail "an upgrade cell expects pass, not $EXPECT"
    (umask 022 && "$here/../ledger-scenario.sh" "$RELEASED" "$DIR") > /dev/null 2> "$WORKDIR/scenario.log" \
      || { cat "$WORKDIR/scenario.log" >&2; fail "the released $TAG could not write the ledger"; }
    adopt_ledger "$HEAD" "$DIR"
    batch_states "$DIR/state.db" > "$WORKDIR/before.txt"
    [[ -s "$WORKDIR/before.txt" ]] || fail "the release wrote no rows"
    log "the release left $(wc -l < "$WORKDIR/before.txt") rows, schema version $(user_version "$DIR/state.db")"

    must_pass head-status "$HEAD" status --config "$DIR/good.yaml"
    must_pass head-list-batches "$HEAD" --json list-batches --config "$DIR/good.yaml"
    must_pass head-audit-coverage "$HEAD" audit-coverage --config "$DIR/good.yaml"
    grep -q 'coverage audit PASSED' "$WORKDIR/logs/head-audit-coverage.log" \
      || fail "audit-coverage did not report PASSED"
    # Committed objects read back and verify again, from the ledger and from
    # the digest the release wrote for the row it pruned.
    must_pass head-scrub "$HEAD" scrub --config "$DIR/good.yaml"
    grep -q 'scrub PASSED' "$WORKDIR/logs/head-scrub.log" || fail "scrub did not report PASSED"

    # Recovery rewinds to the ranges the release handed back, then new input is
    # archived behind them.
    must_pass head-replay "$HEAD" replay --config "$DIR/good.yaml"
    append_lines "$DIR/app.log" head 2
    must_pass head-process-once "$HEAD" process-once --source file --config "$DIR/good.yaml"
    # A second recovery retires the handed-back row the new batch covered.
    must_pass head-replay-again "$HEAD" replay --config "$DIR/good.yaml"
    batch_states "$DIR/state.db" > "$WORKDIR/after.txt"
    committed_stay_committed "$WORKDIR/before.txt" "$WORKDIR/after.txt"
    if grep -qE ' (failed|replay_required)$' "$WORKDIR/after.txt"; then
      cat "$WORKDIR/after.txt" >&2
      fail "a handed-back range was not archived again after process-once"
    fi
    must_pass head-audit-coverage-after "$HEAD" audit-coverage --config "$DIR/good.yaml"
    [[ "$(user_version "$DIR/state.db")" == 1 ]] \
      || fail "after this build's open the ledger is at schema version $(user_version "$DIR/state.db"), not 1"
    log "ok: opened, read, audited, scrubbed and archived new input; $(grep -c ' source_committed$' "$WORKDIR/after.txt") committed"
    ;;

  forward)
    "$here/../ledger-scenario.sh" "$HEAD" "$DIR" > /dev/null 2> "$WORKDIR/scenario.log" \
      || { cat "$WORKDIR/scenario.log" >&2; fail "this build could not write the ledger"; }
    batch_states "$DIR/state.db" > "$WORKDIR/before.txt"
    before_digest="$(ledger_digest "$DIR/state.db")"
    log "this build left $(wc -l < "$WORKDIR/before.txt") rows, schema version $(user_version "$DIR/state.db")"
    append_lines "$DIR/app.log" released 2

    refused=""
    for step in status list-batches process-once; do
      case "$step" in
        process-once) run "released-$step" "$RELEASED" process-once --source file --config "$DIR/good.yaml" ;;
        *) run "released-$step" "$RELEASED" "$step" --config "$DIR/good.yaml" ;;
      esac
      if [[ "$RC" -ne 0 ]]; then
        refused="$step"
        break
      fi
    done

    case "$EXPECT" in
      pass)
        if [[ -n "$refused" ]]; then
          sed 's/^/    /' "$WORKDIR/logs/released-$refused.log" >&2
          fail "the released $TAG exited $RC on $refused over a ledger this build wrote; expected pass"
        fi
        batch_states "$DIR/state.db" > "$WORKDIR/after.txt"
        committed_stay_committed "$WORKDIR/before.txt" "$WORKDIR/after.txt"
        log "ok: the released $TAG read and extended the ledger this build wrote"
        # And the column rules (#626): the release ignores `sealed_at` and
        # writes NULL (or the instant, from v0.10.0), and this build carries on
        # over what it wrote. SQLite half only; the release binaries have no
        # `postgres` feature (see the ledger-postgres cell).
        LEDGER_COMPAT_KEEP=1 LEDGER_COMPAT_WORKDIR="$WORKDIR/ledger-compat" \
          "$here/../../ledger-compat.sh" "$HEAD" "$TAG" "$RELEASED" \
          || fail "scripts/ledger-compat.sh failed (its logs are in $WORKDIR/ledger-compat/logs)"
        ;;
      refuse)
        [[ -n "$refused" ]] || fail "expected the released $TAG to refuse the ledger, and it ran status, list-batches and process-once"
        grep -qE -- "$MESSAGE" "$WORKDIR/logs/released-$refused.log" \
          || { sed 's/^/    /' "$WORKDIR/logs/released-$refused.log" >&2; fail "$refused refused, but its output does not match '$MESSAGE'"; }
        [[ "$(ledger_digest "$DIR/state.db")" == "$before_digest" ]] \
          || fail "the refusal changed the ledger: a refusal must leave it exactly as it was"
        log "ok: the released $TAG refused at $refused with the expected message and changed nothing"
        ;;
      *) fail "the ledger cell expects pass or refuse, not $EXPECT" ;;
    esac
    ;;
  *) fail "no direction $DIRECTION" ;;
esac
