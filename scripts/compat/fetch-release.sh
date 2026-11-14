#!/usr/bin/env bash
# Fetch one release's published binaries and prove they are what the release
# workflow built (#743). Every compatibility cell that runs an old release
# runs THIS output, never a rebuild: a rebuild of a tag proves what today's
# toolchain makes of that source, not what operators installed.
#
# Checks, in order, each failing closed:
#   1. the tag is a final release (vX.Y.Z), and it has an archive for TARGET;
#   2. the archive is listed in the release's SHA256SUMS, and its digest
#      matches (a corrupted or swapped download);
#   3. its Sigstore bundle verifies against the release workflow AT THAT TAG
#      (who built it: SHA256SUMS sits beside the archive and proves nothing
#      about its origin on its own);
#   4. the archive holds exactly one top-level directory, no absolute or
#      parent paths, and an executable vtop-node and vtopctl in it;
#   5. where the binaries run on this host, each reports the tag's version.
#
# Usage: fetch-release.sh <tag> <cache-dir> [target]
#   target defaults to x86_64-unknown-linux-gnu.
# Prints the directory holding vtop-node and vtopctl on stdout; everything
# else goes to stderr. Downloads are kept in <cache-dir>/<tag>/ and
# re-verified on every call, so a cached copy is never trusted on its own.
#
# Exit codes: 0 verified; 1 a check failed; 2 usage; 3 the release has no
# archive for TARGET.
#
# Environment: GH_TOKEN for `gh`; COSIGN (default `cosign`) names the
# binary; FETCH_RELEASE_REPO (default allamiro/vtop-engine).
set -euo pipefail

usage() { echo "usage: fetch-release.sh <tag> <cache-dir> [target]" >&2; exit 2; }
TAG="${1:-}"
CACHE="${2:-}"
TARGET="${3:-x86_64-unknown-linux-gnu}"
[[ -n "$TAG" && -n "$CACHE" ]] || usage
REPO="${FETCH_RELEASE_REPO:-allamiro/vtop-engine}"
COSIGN="${COSIGN:-cosign}"

log() { echo "[fetch-release/$TAG] $*" >&2; }
fail() { echo "[fetch-release/$TAG] FAIL: $*" >&2; exit 1; }

[[ "$TAG" =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]] \
  || fail "$TAG is not a final release tag (vX.Y.Z)"
VERSION="${TAG#v}"
ASSET="vtop-engine-$VERSION-$TARGET.tar.gz"
BUNDLE="$ASSET.sigstore.json"
IDENTITY="https://github.com/$REPO/.github/workflows/release.yml@refs/tags/$TAG"
ISSUER="https://token.actions.githubusercontent.com"

dir="$(mkdir -p "$CACHE/$TAG" && cd "$CACHE/$TAG" && pwd)"

download() {
  local names
  names="$(gh release view "$TAG" --repo "$REPO" --json assets --jq '.assets[].name')" \
    || fail "cannot read the assets of $TAG"
  if ! grep -qxF "$ASSET" <<< "$names"; then
    echo "[fetch-release/$TAG] $TAG has no $ASSET" >&2
    exit 3
  fi
  grep -qxF "$BUNDLE" <<< "$names" || fail "$TAG has $ASSET but no Sigstore bundle $BUNDLE"
  grep -qxF SHA256SUMS <<< "$names" || fail "$TAG has no SHA256SUMS"
  rm -f "$dir/$ASSET" "$dir/$BUNDLE" "$dir/SHA256SUMS"
  gh release download "$TAG" --repo "$REPO" --dir "$dir" \
    --pattern "$ASSET" --pattern "$BUNDLE" --pattern SHA256SUMS >&2 \
    || fail "download failed"
}

# 2 and 3: the digest and who built it. Prints why on failure and returns
# non-zero, so a bad CACHED copy can be fetched once more before failing.
verify() {
  local expected actual
  # SHA256SUMS lines are `<hex>  ./<name>` (the release job runs sha256sum
  # over ./*); accept a bare name too.
  expected="$(awk -v a="$ASSET" '$2 == a || $2 == "./" a { print $1 }' "$dir/SHA256SUMS")" \
    || { echo "[fetch-release/$TAG] cannot read SHA256SUMS" >&2; return 1; }
  [[ "$expected" =~ ^[0-9a-f]{64}$ ]] \
    || { echo "[fetch-release/$TAG] $ASSET is not listed once in SHA256SUMS" >&2; return 1; }
  if command -v sha256sum > /dev/null 2>&1; then
    actual="$(sha256sum "$dir/$ASSET" | awk '{ print $1 }')"
  else
    actual="$(shasum -a 256 "$dir/$ASSET" | awk '{ print $1 }')"
  fi
  [[ "$actual" =~ ^[0-9a-f]{64}$ ]] \
    || { echo "[fetch-release/$TAG] cannot hash $ASSET" >&2; return 1; }
  [[ "$actual" == "$expected" ]] \
    || { echo "[fetch-release/$TAG] $ASSET has sha256 $actual; SHA256SUMS says $expected" >&2; return 1; }
  log "sha256 $actual matches SHA256SUMS"
  "$COSIGN" verify-blob "$dir/$ASSET" --bundle "$dir/$BUNDLE" \
    --certificate-identity "$IDENTITY" --certificate-oidc-issuer "$ISSUER" >&2 \
    || { echo "[fetch-release/$TAG] the Sigstore bundle does not verify against $IDENTITY" >&2; return 1; }
  log "Sigstore bundle verified: $IDENTITY"
}

if [[ -s "$dir/$ASSET" && -s "$dir/$BUNDLE" && -s "$dir/SHA256SUMS" ]]; then
  # A cached copy is checked like a fresh one. One that fails is fetched
  # again once (a cache entry is never rewritten under its key, so a bad one
  # would otherwise stay red forever); the fresh copy must then pass.
  if ! verify; then
    log "the cached copy failed its checks; downloading it again"
    download
    verify || fail "the release's own files fail the checks above"
  fi
else
  download
  verify || fail "the release's files fail the checks above"
fi

# 4. What is inside, before anything is extracted.
listing="$(tar -tzf "$dir/$ASSET")" || fail "$ASSET is not a readable gzip tar"
# Regular files and directories only: no link, device or fifo.
types="$(tar -tvzf "$dir/$ASSET" | cut -c1 | sort -u | tr -d '\n')" \
  || fail "$ASSET is not a readable gzip tar"
[[ "$types" =~ ^[-d]+$ ]] || fail "$ASSET holds members that are not files or directories ($types)"
top="vtop-engine-$VERSION-$TARGET"
while IFS= read -r entry; do
  [[ -n "$entry" ]] || continue
  case "$entry" in
    /*) fail "$ASSET holds an absolute path: $entry" ;;
  esac
  [[ "/$entry/" != *"/../"* ]] || fail "$ASSET holds a parent path: $entry"
  [[ "$entry" == "$top" || "$entry" == "$top/"* ]] \
    || fail "$ASSET holds $entry outside $top/"
done <<< "$listing"
out="$dir/extracted"
rm -rf "$out"
mkdir -p "$out"
tar -xzf "$dir/$ASSET" -C "$out" --no-same-owner || fail "extracting $ASSET failed"
bin="$out/$top"
for name in vtop-node vtopctl; do
  [[ -f "$bin/$name" && ! -L "$bin/$name" ]] || fail "$ASSET has no $name file in $top/"
  [[ -x "$bin/$name" ]] || fail "$top/$name is not executable"
done
log "archive holds vtop-node and vtopctl under $top/ ($(grep -c . <<< "$listing") entries)"

# 5. The version, where this host can run the binaries.
host="$(uname -m)-$(uname -s)"
case "$host:$TARGET" in
  x86_64-Linux:x86_64-unknown-linux-gnu | aarch64-Linux:aarch64-unknown-linux-gnu | \
  arm64-Darwin:aarch64-apple-darwin | x86_64-Darwin:x86_64-apple-darwin)
    for name in vtop-node vtopctl; do
      reported="$("$bin/$name" --version 2>&1)" || fail "$name --version failed: $reported"
      [[ "$reported" == *" $VERSION" ]] \
        || fail "$name reports '$reported', not version $VERSION"
    done
    log "both binaries report $VERSION"
    ;;
  *) log "binaries are for $TARGET; not run on $host" ;;
esac

echo "$bin"
