#!/bin/bash
# The wire cell (#743): the native protocol (`vtop-protocol`, VTPW) between a
# released vtop-node and this build, in the order a rolling upgrade serves
# clients in, and its reverse.
#
#   upgrade  a node of the RELEASE serves, and this build's `produce` and
#            `verify` are the client: a client upgraded before its brokers;
#   forward  a node of THIS BUILD serves, and the release's `produce` and
#            `verify` are the client: a broker upgraded before its clients.
#
# A leader and a follower run (quorum durability, so the replica plane is
# exercised in the same direction as the client plane), 2000 records are
# produced in batches, and the client reads them all back. It is
# scripts/upgrade-compat.sh with UPGRADE_WIRE set, which stops after the
# client's `verify`. The release is the published vtop-node verified by
# fetch-release.sh, as for every cell.
#
# Usage: wire.sh <tag> <direction> <expect> <released-bin-dir> <head-bin-dir> <workdir> [message]
set -euo pipefail
[[ $# -ge 6 ]] || { echo "usage: wire.sh <tag> <direction> <expect> <released-bin-dir> <head-bin-dir> <workdir> [message]" >&2; exit 2; }
TAG="$1" DIRECTION="$2" EXPECT="$3" OLD="$4/vtop-node" NEW="$5/vtop-node" WORKDIR="$6"
CELL=wire
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/compat/cells/lib.sh
source "$here/lib.sh"
[[ "$EXPECT" == pass ]] || fail "the wire cell expects pass, not $EXPECT"
[[ -x "$OLD" && -x "$NEW" ]] || fail "$OLD and $NEW must be executable"

case "$DIRECTION" in
  upgrade) wire=old-node-new-client ;;
  forward) wire=new-node-old-client ;;
  *) fail "no direction $DIRECTION" ;;
esac
# The same profile rule as the segments cell: v0.2.x cannot roll.
IFS=. read -r major minor _ <<< "${TAG#v}"
profile=rolling
if ((major == 0 && minor < 3)); then profile=legacy; fi

rm -rf "${WORKDIR:?}/wire"
UPGRADE_WIRE="$wire" UPGRADE_KEEP=1 UPGRADE_WORKDIR="$WORKDIR/wire" \
  UPGRADE_NATIVE_PORT="${COMPAT_WIRE_PORT:-9700}" \
  "$here/../../upgrade-compat.sh" "$TAG" "$OLD" "$NEW" "$profile" \
  || fail "the native protocol failed between $TAG and this build ($wire, profile $profile); logs in $WORKDIR/wire/logs"
log "ok: $wire, $profile"
