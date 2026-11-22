#!/usr/bin/env bash
# The segments cell (#743): a data node's directory written by a released
# vtop-node must open under this build, quarantine nothing, serve every record
# byte for byte, and serve it again after a clean-stop restart. It runs
# scripts/upgrade-compat.sh with the PUBLISHED vtop-node (the same archive
# fetch-release.sh verified for every other cell), once per profile the
# release can write:
#
#   legacy      before v0.3.0, which cannot roll: the single `range.active`;
#   rolling     from v0.3.0: sealed segments, index, commit marker, producer
#               snapshot, manifest and the broker's epoch journals, format v1;
#   rolling-v2  from v0.5.0, which added `segment_format: v2`: the v2 header,
#               record frames and chunk sidecar, which the default writer
#               does not write.
#
# Upgrade only: nothing here asks an older vtop-node to open what this build
# wrote, and downgrade of a data directory is not tested or supported.
#
# Usage: segments.sh <tag> <direction> <expect> <released-bin-dir> <head-bin-dir> <workdir> [message]
set -euo pipefail
[[ $# -ge 6 ]] || { echo "usage: segments.sh <tag> <direction> <expect> <released-bin-dir> <head-bin-dir> <workdir> [message]" >&2; exit 2; }
TAG="$1" DIRECTION="$2" EXPECT="$3" OLD="$4/vtop-node" NEW="$5/vtop-node" WORKDIR="$6"
CELL=segments
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/compat/cells/lib.sh
source "$here/lib.sh"
[[ "$DIRECTION" == upgrade ]] || fail "the segments cell only runs upgrade, not $DIRECTION"
[[ "$EXPECT" == pass ]] || fail "the segments cell expects pass, not $EXPECT"
[[ -x "$OLD" ]] || fail "$OLD is not executable"
[[ -x "$NEW" ]] || fail "$NEW is not executable (the workflow builds this build's vtop-node beside vtopctl)"

IFS=. read -r major minor _ <<< "${TAG#v}"
profiles=()
if ((major == 0 && minor < 3)); then
  profiles=(legacy)
else
  profiles=(rolling)
  # The key arrived in v0.5.0; older releases refuse an unknown key.
  if ((major > 0 || minor >= 5)); then profiles+=(rolling-v2); fi
fi

port="${COMPAT_SEGMENTS_PORT:-9700}"
for profile in "${profiles[@]}"; do
  log "profile $profile"
  rm -rf "${WORKDIR:?}/$profile"
  UPGRADE_KEEP=1 UPGRADE_WORKDIR="$WORKDIR/$profile" UPGRADE_NATIVE_PORT="$port" \
    "$here/../../upgrade-compat.sh" "$TAG" "$OLD" "$NEW" "$profile" \
    || fail "upgrade-compat.sh failed for the $profile profile (logs in $WORKDIR/$profile/logs)"
  port=$((port + 20))
done
log "ok: $TAG opens under this build in: ${profiles[*]}"
