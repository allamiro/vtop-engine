#!/usr/bin/env bash
# Run one cell of the compatibility matrix (#743): one format, one released
# tag, one direction, with the expected result scripts/compat/expected.yaml
# gives it (scripts/compat/select_cells.py picks the cells).
#
#   upgrade  the released binary writes, this build reads;
#   forward  this build writes, the released binary reads.
#
# Usage: cell.sh <cell> <tag> <direction> <expect> <released-bin-dir> <head-bin-dir> [message]
#   <released-bin-dir> is what scripts/compat/fetch-release.sh printed;
#   <head-bin-dir> holds this build's vtopctl;
#   [message] is the text a `refuse` expectation must find.
# COMPAT_WORKDIR keeps the cell's files and logs there.
set -euo pipefail

usage() {
  echo "usage: cell.sh <cell> <tag> <direction> <expect> <released-bin-dir> <head-bin-dir> [message]" >&2
  exit 2
}
[[ $# -ge 6 ]] || usage
CELL="$1" TAG="$2" DIRECTION="$3" EXPECT="$4" RELEASED="$5" HEAD="$6"
MESSAGE="${7:-}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKDIR="${COMPAT_WORKDIR:-$(mktemp -d "${TMPDIR:-/tmp}/vtop-compat.XXXXXX")}"
mkdir -p "$WORKDIR"

fail() { echo "[compat/$CELL/$DIRECTION/$TAG] FAIL: $*" >&2; exit 1; }

for bin in "$RELEASED/vtopctl" "$HEAD/vtopctl"; do
  [[ -x "$bin" ]] || fail "$bin is not executable"
done
[[ "$EXPECT" != refuse || -n "$MESSAGE" ]] || fail "a refuse expectation needs the message to find"

case "$CELL:$DIRECTION" in
  manifest:forward)
    case "$EXPECT" in
      pass | hash-failure) ;;
      *) fail "the manifest cell expects pass or hash-failure, not $EXPECT" ;;
    esac
    MANIFEST_COMPAT_KEEP=1 MANIFEST_COMPAT_WORKDIR="$WORKDIR" \
      exec "$here/../manifest-compat.sh" "$HEAD/vtopctl" "$TAG" "$RELEASED/vtopctl" "$EXPECT"
    ;;
  ledger-sqlite:* | digests-audit:* | ledger-postgres:* | segments:*)
    # Each cell script takes the directories, and keeps its own logs.
    exec "$here/cells/$CELL.sh" "$TAG" "$DIRECTION" "$EXPECT" "$RELEASED" "$HEAD" \
      "$WORKDIR/$CELL-$DIRECTION" "$MESSAGE"
    ;;
  *) fail "no cell $CELL with direction $DIRECTION" ;;
esac
