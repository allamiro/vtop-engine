#!/usr/bin/env bash
# Write the metadata-directory fixtures of one release (#743): run
# scripts/compat/meta-scenario.sh with that release's PUBLISHED vtop-node and
# vtopctl (fetched and verified by scripts/compat/fetch-release.sh, never
# rebuilt) and keep the directory the voter left, under
#
#   crates/vtop-meta/tests/fixtures/meta/<tag>/
#       data/log-*.vmlog                  the Raft log chunk
#       data/snapshot-*.vmsnap            the state snapshot
#       data/meta.hardstate               the vote and term
#       data/meta.applied                 the applied frontier
#       data/meta.purged                  the purged frontier
#       data/meta.membership_log_id       the membership's log id
#       expect.txt                        what the release reported (applied
#                                         index, feature level, a record's
#                                         generation, the voter set)
#       PROVENANCE
#
# One voter on plaintext loopback, raised to the highest feature level the
# release accepts, 5100 re-registrations of one node (the Raft engine takes a
# snapshot every 5000 entries and purges behind it), then SIGKILL: the
# directory is what a crashed voter of that release leaves.
#
# The files are not reproducible byte for byte (the log and hard state carry
# the writer's own bookkeeping); the PROVENANCE digests are of the files as
# committed. Nothing in them names this machine: the cluster, node and topic
# ids are the fixed ones in meta-scenario.sh and the addresses are synthetic.
#
# Usage: regenerate-meta-fixtures.sh <tag> [target]
# Environment: COSIGN (default `cosign`), COMPAT_CACHE, FIXTURE_IMAGE.
set -euo pipefail

[[ $# -ge 1 && $# -le 2 ]] || { echo "usage: regenerate-meta-fixtures.sh <tag> [target]" >&2; exit 2; }
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
scratch="$(mktemp -d "${TMPDIR:-/tmp}/vtop-meta-fixture.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
CACHE="${COMPAT_CACHE:-$scratch/cache}"

bindir="$("$here/fetch-release.sh" "$TAG" "$CACHE" "$TARGET")"
VERSION="${TAG#v}"
ASSET="vtop-engine-$VERSION-$TARGET.tar.gz"
digest="$(awk -v a="$ASSET" '$2 == a || $2 == "./" a { print $1 }' "$CACHE/$TAG/SHA256SUMS")"
[[ "$digest" =~ ^[0-9a-f]{64}$ ]] || { echo "$ASSET is not listed once in the release's SHA256SUMS" >&2; exit 1; }

# An older release links shared libraries the image lacks, so the container
# installs them as root and runs the scenario as an unprivileged user.
# Loopback only, so no network is needed once installed.
mkdir -p "$scratch/out"
chmod 0777 "$scratch/out"
docker run --rm --cpus=2 --memory=2g --hostname compat-fixture \
  -e USER=compat -e HOME=/tmp \
  -v "$bindir:/bin-under-test:ro" \
  -v "$root/scripts/compat/meta-scenario.sh:/meta-scenario.sh:ro" \
  -v "$scratch/out:/out" \
  "$IMAGE" bash -c '
    set -e
    apt-get update -qq > /dev/null
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq libsasl2-2 libssl3t64 libcurl4t64 zlib1g python3 > /dev/null
    setpriv --reuid=10001 --regid=10001 --clear-groups env LEVEL=max ENTRIES=5100 \
      bash /meta-scenario.sh /bin-under-test/vtop-node /bin-under-test/vtopctl /out/run
    setpriv --reuid=10001 --regid=10001 --clear-groups chmod -R a+rwX /out/run
  '

dest="$root/crates/vtop-meta/tests/fixtures/meta/$TAG"
rm -rf "$dest"
mkdir -p "$dest/data"
cp "$scratch/out/run/data/"* "$dest/data/"
cp "$scratch/out/run/expect.txt" "$dest/expect.txt"
level="$(awk '$1 == "feature_level" { print $2 }' "$dest/expect.txt")"

sha() {
  if command -v sha256sum > /dev/null 2>&1; then sha256sum "$1" | awk '{ print $1 }'; else shasum -a 256 "$1" | awk '{ print $1 }'; fi
}
{
  echo "# Written by scripts/compat/regenerate-meta-fixtures.sh $TAG $TARGET."
  echo "tag: $TAG"
  echo "asset: $ASSET"
  echo "asset_sha256: $digest"
  echo "binary: vtop-node"
  echo "command: >-"
  echo "  scripts/compat/meta-scenario.sh with LEVEL=max and ENTRIES=5100: one voter"
  echo "  of the release on plaintext loopback (vtop-node meta), vtopctl meta init,"
  echo "  register-node three times, create-topic, enable-features up to the highest"
  echo "  level the release accepts ($level), 5100 register-node re-registrations of"
  echo "  one node; the voter stopped with SIGKILL; its data directory"
  echo "input: >-"
  echo "  synthetic nodes, one topic and the fixed ids in meta-scenario.sh"
  echo "files:"
  (cd "$dest" && find . -type f ! -name PROVENANCE | sed 's|^\./||' | LC_ALL=C sort) | while read -r f; do
    echo "  $f: $(sha "$dest/$f")"
  done
} > "$dest/PROVENANCE"
echo "wrote $dest" >&2
