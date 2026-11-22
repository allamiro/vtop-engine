#!/usr/bin/env bash
# Write the data-directory fixtures of one release (#743): run
# scripts/upgrade-compat.sh's OLD phase with that release's PUBLISHED
# vtop-node (fetched and verified by fetch-release.sh, never rebuilt) and keep
# the directory it wrote, under
#
#   crates/vtop-log/tests/fixtures/segments/<tag>/<profile>/
#       range-*.segment / .active   segment files (format v1, or v2 for rolling-v2)
#       range-*.index, .chunks      the offset index, and the v2 chunk sidecar
#       range-*.commit              the commit marker
#       range-*.manifest.json       the sealed segment's manifest
#       range-*.producers           the producer snapshot
#       epochs, fencing-epochs      the broker's producer and fencing-epoch journals
#   crates/vtop-log/tests/fixtures/segments/<tag>/PROVENANCE
#
# Profiles: legacy (before v0.3.0), rolling (v0.3.0 on), rolling-v2 (v0.5.0 on);
# see scripts/upgrade-compat.sh. The leader and a follower run, 300 records
# are produced with quorum durability, and both are stopped with SIGKILL: the
# directory is what a crashed node of that release leaves.
#
# The files are not reproducible byte for byte (the segments carry times and
# the writer's own bookkeeping); the PROVENANCE digests are of the files as
# committed. Nothing in them names this machine: the node and cluster ids are
# the fixed ones in upgrade-compat.sh, and the payloads are synthetic.
#
# Usage: regenerate-segment-fixtures.sh <tag> [target]
# Environment: COSIGN (default `cosign`), COMPAT_CACHE, FIXTURE_IMAGE.
set -euo pipefail

[[ $# -ge 1 && $# -le 2 ]] || { echo "usage: regenerate-segment-fixtures.sh <tag> [target]" >&2; exit 2; }
TAG="$1"
case "$(uname -m)" in
  x86_64 | amd64) DEFAULT_TARGET=x86_64-unknown-linux-gnu ;;
  arm64 | aarch64) DEFAULT_TARGET=aarch64-unknown-linux-gnu ;;
  *) DEFAULT_TARGET=x86_64-unknown-linux-gnu ;;
esac
TARGET="${2:-$DEFAULT_TARGET}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
IMAGE="${FIXTURE_IMAGE:-ubuntu:24.04@sha256:9c7f200c07e3d4a03e7249d2c16bf66a51120fb876aa2242cf31451412543289}"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/vtop-segment-fixture.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
CACHE="${COMPAT_CACHE:-$scratch/cache}"

bindir="$("$here/fetch-release.sh" "$TAG" "$CACHE" "$TARGET")"
VERSION="${TAG#v}"
ASSET="vtop-engine-$VERSION-$TARGET.tar.gz"
digest="$(awk -v a="$ASSET" '$2 == a || $2 == "./" a { print $1 }' "$CACHE/$TAG/SHA256SUMS")"
[[ "$digest" =~ ^[0-9a-f]{64}$ ]] || { echo "$ASSET is not listed once in the release's SHA256SUMS" >&2; exit 1; }

IFS=. read -r major minor _ <<< "$VERSION"
if ((major == 0 && minor < 3)); then
  profiles=(legacy)
elif ((major == 0 && minor < 5)); then
  profiles=(rolling)
else
  profiles=(rolling rolling-v2)
fi

# As the ledger fixtures: an older release links shared libraries the image
# lacks, so the container installs them as root, and runs each profile as an
# unprivileged user. Loopback only, so no network is needed once installed.
mkdir -p "$scratch/out"
chmod 0777 "$scratch/out"
docker run --rm --cpus=2 --memory=2g \
  -e HOME=/tmp \
  -v "$bindir:/bin-under-test:ro" \
  -v "$root/scripts:/scripts:ro" \
  -v "$scratch/out:/out" \
  "$IMAGE" bash -c '
    set -e
    apt-get update -qq > /dev/null
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq libsasl2-2 libssl3t64 libcurl4t64 zlib1g openssl > /dev/null
    port=9700
    for profile in '"${profiles[*]}"'; do
      setpriv --reuid=10001 --regid=10001 --clear-groups env \
        UPGRADE_STOP_AFTER_OLD=1 UPGRADE_KEEP=1 UPGRADE_RECORDS=300 \
        UPGRADE_WORKDIR=/out/$profile UPGRADE_NATIVE_PORT=$port \
        bash /scripts/upgrade-compat.sh '"$TAG"' /bin-under-test/vtop-node /bin-under-test/vtop-node $profile
      port=$((port + 20))
    done
    setpriv --reuid=10001 --regid=10001 --clear-groups chmod -R a+rwX /out
  '

dest="$root/crates/vtop-log/tests/fixtures/segments/$TAG"
rm -rf "$dest"
for profile in "${profiles[@]}"; do
  mkdir -p "$dest/$profile"
  cp "$scratch/out/$profile/data/"* "$dest/$profile/"
done

sha() {
  if command -v sha256sum > /dev/null 2>&1; then sha256sum "$1" | awk '{ print $1 }'; else shasum -a 256 "$1" | awk '{ print $1 }'; fi
}
{
  echo "# Written by scripts/compat/regenerate-segment-fixtures.sh $TAG $TARGET."
  echo "tag: $TAG"
  echo "asset: $ASSET"
  echo "asset_sha256: $digest"
  echo "binary: vtop-node"
  echo "command: >-"
  echo "  scripts/upgrade-compat.sh with UPGRADE_STOP_AFTER_OLD=1 and UPGRADE_RECORDS=300,"
  echo "  once per profile (${profiles[*]}): a leader and a follower of the"
  echo "  release, vtop-node produce --records 300 --batch 64 --durability quorum,"
  echo "  both stopped with SIGKILL; the leader's data directory"
  echo "input: >-"
  echo "  300 synthetic records of vtop-node produce, in topic upgrade.v1"
  echo "files:"
  (cd "$dest" && find . -type f ! -name PROVENANCE | sed 's|^\./||' | LC_ALL=C sort) | while read -r f; do
    echo "  $f: $(sha "$dest/$f")"
  done
} > "$dest/PROVENANCE"
echo "wrote $dest" >&2
