#!/usr/bin/env bash
# Write the ledger fixtures of one release (#743): run scripts/compat/
# ledger-scenario.sh with that release's PUBLISHED vtopctl (fetched and
# verified by fetch-release.sh, never rebuilt) and keep what it wrote under
#
#   crates/vtop-cli/tests/fixtures/ledger-sqlite/<tag>/
#       state.db                 the SQLite ledger, checkpointed
#       app.log                  the input file the ledger's markers name
#       ledger-digests/*.jsonl   the digest prune-ledger wrote
#       PROVENANCE               tag, asset, its SHA-256 from the release's
#                                SHA256SUMS, command, input, and each file's
#                                SHA-256 (scripts/compat/check_provenance.py)
#
# The scenario runs in a container with a fixed host name and a user with no
# passwd entry, so that what the audit trail records about the actor (a user
# name and a host name) is a placeholder and not the person who regenerated
# the fixture. The fixture files are not reproducible byte for byte (batch
# ids and times are random); the PROVENANCE digests are of the files as
# committed.
#
# Usage: regenerate-fixtures.sh <tag> [target]
#   target defaults to the Linux archive of this machine's architecture.
# Environment: COSIGN (default `cosign`), COMPAT_CACHE (downloads; default
# a temporary directory), FIXTURE_IMAGE (default: ubuntu 24.04, pinned by digest).
set -euo pipefail

[[ $# -ge 1 && $# -le 2 ]] || { echo "usage: regenerate-fixtures.sh <tag> [target]" >&2; exit 2; }
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
scratch="$(mktemp -d "${TMPDIR:-/tmp}/vtop-fixture.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
CACHE="${COMPAT_CACHE:-$scratch/cache}"

bindir="$("$here/fetch-release.sh" "$TAG" "$CACHE" "$TARGET")"
VERSION="${TAG#v}"
ASSET="vtop-engine-$VERSION-$TARGET.tar.gz"
sums="$CACHE/$TAG/SHA256SUMS"
digest="$(awk -v a="$ASSET" '$2 == a || $2 == "./" a { print $1 }' "$sums")"
[[ "$digest" =~ ^[0-9a-f]{64}$ ]] || { echo "$ASSET is not listed once in $sums" >&2; exit 1; }

# The container sees the scenario and the binaries read-only and writes only
# under /out. An older release links librdkafka's shared libraries, which the
# image lacks, so the container installs them as root and then runs the
# scenario as a user with no entry in /etc/passwd (so the audit actor's name
# comes from $USER, and the host name is the fixed one below).
mkdir -p "$scratch/out"
chmod 0777 "$scratch/out"
docker run --rm --hostname compat-fixture \
  --cpus=2 --memory=2g \
  -e USER=compat -e HOME=/tmp \
  -v "$bindir:/bin-under-test:ro" \
  -v "$here/ledger-scenario.sh:/ledger-scenario.sh:ro" \
  -v "$scratch/out:/out" \
  "$IMAGE" bash -c '
    set -e
    apt-get update -qq > /dev/null
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq libsasl2-2 libssl3t64 libcurl4t64 zlib1g > /dev/null
    exec setpriv --reuid=10001 --regid=10001 --clear-groups bash /ledger-scenario.sh /bin-under-test/vtopctl /out/run
  '

run="$scratch/out/run"
python3 - "$run/state.db" <<'PY'
import sqlite3, sys
# One file, no -wal or -shm beside it.
con = sqlite3.connect(sys.argv[1])
con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
con.close()
PY

dest="$root/crates/vtop-cli/tests/fixtures/ledger-sqlite/$TAG"
rm -rf "$dest"
mkdir -p "$dest/ledger-digests"
cp "$run/state.db" "$run/app.log" "$dest/"
cp "$run"/state.db.ledger-digests/ledger-digest-*.jsonl "$dest/ledger-digests/"

# A name or a host of the person who ran this must not be in a committed file.
if LC_ALL=C grep -rl -a -e "$(id -un)" -e "$(hostname)" "$dest" > /dev/null 2>&1; then
  echo "the fixture mentions this user or host name; not keeping it" >&2
  rm -rf "$dest"
  exit 1
fi

{
  echo "# Written by scripts/compat/regenerate-fixtures.sh $TAG $TARGET."
  echo "tag: $TAG"
  echo "asset: $ASSET"
  echo "asset_sha256: $digest"
  echo "binary: vtopctl"
  echo "command: >-"
  echo "  scripts/compat/ledger-scenario.sh /bin-under-test/vtopctl: process-once --source file"
  echo "  (good store, failing store twice, good store), prune-ledger --older-than-days 0"
  echo "  --limit 1, process-once with a failing store, then a WAL checkpoint"
  echo "input: >-"
  echo "  app.log in this directory: eleven lines, appended between the runs"
  echo "files:"
  (cd "$dest" && find . -type f ! -name PROVENANCE | sed 's|^\./||' | LC_ALL=C sort) | while read -r f; do
    if command -v sha256sum > /dev/null 2>&1; then
      h="$(sha256sum "$dest/$f" | awk '{ print $1 }')"
    else
      h="$(shasum -a 256 "$dest/$f" | awk '{ print $1 }')"
    fi
    echo "  $f: $h"
  done
} > "$dest/PROVENANCE"
echo "wrote $dest" >&2
