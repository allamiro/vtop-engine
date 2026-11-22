#!/usr/bin/env bash
# The ledger-postgres cell (#743): the PostgreSQL ledger across a release and
# this build. Both vtopctl binaries must have the `postgres` feature, which the
# published archives do not, so the workflow builds the release's tag from
# source for this cell (a release whose archive has the feature would be
# run as published).
#
#   upgrade  the release migrates a fresh database and writes the scenario
#            (ledger-scenario.sh); this build migrates it (the additive
#            columns), then reports status, audits, scrubs, recovers, archives
#            new input and leaves every committed batch committed; a second
#            migrate changes nothing;
#   forward  this build migrates and writes it; the release then runs
#            `status`, `list-batches` and `process-once` on it (only N-1
#            shares a PostgreSQL ledger with this build). The same
#            `pass`/`refuse` expectations as ledger-sqlite.
#
# Environment: LEDGER_POSTGRES_URL, the URL of a throwaway server's admin
# role (postgres://user:password@host:port); PSQL, the command that runs psql
# against it (default `psql`; `docker exec -i <container> psql` works);
# LEDGER_POSTGRES_ADMIN_URL, the same server as PSQL reaches it, when that
# differs from the URL vtopctl uses (a published port on a different number).
# Each run creates and drops its own database.
#
# Usage: ledger-postgres.sh <tag> <direction> <expect> <old-bin-dir> <head-bin-dir> <workdir> [message]
set -euo pipefail
[[ $# -ge 6 ]] || { echo "usage: ledger-postgres.sh <tag> <direction> <expect> <old-bin-dir> <head-bin-dir> <workdir> [message]" >&2; exit 2; }
TAG="$1" DIRECTION="$2" EXPECT="$3" OLD="$4/vtopctl" HEAD="$5/vtopctl" WORKDIR="$6"
MESSAGE="${7:-}"
CELL=ledger-postgres
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/compat/cells/lib.sh
source "$here/lib.sh"
: "${LEDGER_POSTGRES_URL:?LEDGER_POSTGRES_URL names the throwaway server}"
read -r -a PSQL <<< "${PSQL:-psql}"

DB="compat_$(printf '%s_%s' "${TAG//[^0-9a-z]/_}" "$DIRECTION" | tr '[:upper:]' '[:lower:]')"
DIR="$WORKDIR/ledger"
rm -rf "$DIR"
mkdir -p "$WORKDIR"
admin() { "${PSQL[@]}" "${LEDGER_POSTGRES_ADMIN_URL:-$LEDGER_POSTGRES_URL}/postgres" -v ON_ERROR_STOP=1 -q -c "$1" > /dev/null; }
admin "DROP DATABASE IF EXISTS $DB"
admin "CREATE DATABASE $DB"
trap 'admin "DROP DATABASE IF EXISTS $DB" || true' EXIT
export VTOP_STATE_STORE="$LEDGER_POSTGRES_URL/$DB"
export LEDGER_SCENARIO_POSTGRES=1

# batch_states_via <vtopctl>: "<batch_id> <state>" per row, sorted.
batch_states_via() {
  "$1" --json list-batches --config "$DIR/good.yaml" 2> /dev/null \
    | python3 -I -c '
import json, sys
text = sys.stdin.read()
start = text.index("[")
for row in sorted(json.loads(text[start:]), key=lambda r: r["batch_id"]):
    print(row["batch_id"], row["state"])
'
}

case "$DIRECTION" in
  upgrade)
    [[ "$EXPECT" == pass ]] || fail "an upgrade cell expects pass, not $EXPECT"
    "$here/../ledger-scenario.sh" "$OLD" "$DIR" > /dev/null 2> "$WORKDIR/scenario.log" \
      || { cat "$WORKDIR/scenario.log" >&2; fail "the release could not migrate and write the ledger"; }
    batch_states_via "$OLD" > "$WORKDIR/before.txt"
    [[ -s "$WORKDIR/before.txt" ]] || fail "the release wrote no rows"
    log "the release left $(wc -l < "$WORKDIR/before.txt") rows"

    must_pass head-migrate "$HEAD" migrate --config "$DIR/good.yaml"
    must_pass head-status "$HEAD" status --config "$DIR/good.yaml"
    must_pass head-audit-coverage "$HEAD" audit-coverage --config "$DIR/good.yaml" --digest-dir "$DIR/ledger-digests"
    grep -q 'coverage audit PASSED' "$WORKDIR/logs/head-audit-coverage.log" || fail "audit-coverage did not report PASSED"
    must_pass head-scrub "$HEAD" scrub --config "$DIR/good.yaml" --digest-dir "$DIR/ledger-digests"
    must_pass head-replay "$HEAD" replay --config "$DIR/good.yaml"
    append_lines "$DIR/app.log" head 2
    must_pass head-process-once "$HEAD" process-once --source file --config "$DIR/good.yaml"
    must_pass head-replay-again "$HEAD" replay --config "$DIR/good.yaml"
    batch_states_via "$HEAD" > "$WORKDIR/after.txt"
    committed_stay_committed "$WORKDIR/before.txt" "$WORKDIR/after.txt"
    if grep -qE ' (failed|replay_required)$' "$WORKDIR/after.txt"; then
      cat "$WORKDIR/after.txt" >&2
      fail "a handed-back range was not archived again"
    fi
    must_pass head-migrate-again "$HEAD" migrate --config "$DIR/good.yaml"
    batch_states_via "$HEAD" > "$WORKDIR/after-migrate.txt"
    cmp -s "$WORKDIR/after.txt" "$WORKDIR/after-migrate.txt" || fail "a second migrate changed the ledger"
    log "ok: migrated, read, audited, scrubbed, recovered and archived new input"
    ;;

  forward)
    "$here/../ledger-scenario.sh" "$HEAD" "$DIR" > /dev/null 2> "$WORKDIR/scenario.log" \
      || { cat "$WORKDIR/scenario.log" >&2; fail "this build could not migrate and write the ledger"; }
    batch_states_via "$HEAD" > "$WORKDIR/before.txt"
    log "this build left $(wc -l < "$WORKDIR/before.txt") rows"
    append_lines "$DIR/app.log" released 2
    refused=""
    for step in status list-batches process-once; do
      case "$step" in
        process-once) run "released-$step" "$OLD" process-once --source file --config "$DIR/good.yaml" ;;
        *) run "released-$step" "$OLD" "$step" --config "$DIR/good.yaml" ;;
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
          fail "the release exited $RC on $refused over a ledger this build wrote; expected pass"
        fi
        batch_states_via "$HEAD" > "$WORKDIR/after.txt"
        committed_stay_committed "$WORKDIR/before.txt" "$WORKDIR/after.txt"
        must_pass head-status "$HEAD" status --config "$DIR/good.yaml"
        log "ok: the release read and extended the ledger this build migrated and wrote"
        ;;
      refuse)
        [[ -n "$refused" ]] || fail "expected the release to refuse, and every command passed"
        grep -qE -- "$MESSAGE" "$WORKDIR/logs/released-$refused.log" \
          || { sed 's/^/    /' "$WORKDIR/logs/released-$refused.log" >&2; fail "$refused refused, but its output does not match '$MESSAGE'"; }
        batch_states_via "$HEAD" > "$WORKDIR/after.txt"
        cmp -s "$WORKDIR/before.txt" "$WORKDIR/after.txt" || fail "the refusal changed the ledger"
        log "ok: the release refused at $refused with the expected message and changed nothing"
        ;;
      *) fail "the ledger cell expects pass or refuse, not $EXPECT" ;;
    esac
    ;;
  *) fail "no direction $DIRECTION" ;;
esac
