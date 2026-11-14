#!/usr/bin/env bats
# Tests for scripts/compat/fetch-release.sh (#743): every check it makes must
# be capable of failing. `gh` and `cosign` are stand-ins on PATH, so the
# tests need no network: the fake `gh` serves files from a directory that
# plays the release, and the fake `cosign` passes or fails on demand.

setup() {
    FETCH="${BATS_TEST_DIRNAME}/../compat/fetch-release.sh"
    WORK="$(mktemp -d)"
    RELEASE="$WORK/release"
    CACHE="$WORK/cache"
    BIN="$WORK/bin"
    mkdir -p "$RELEASE" "$CACHE" "$BIN"
    TAG=v9.8.7
    TARGET=x86_64-unknown-linux-gnu
    ASSET="vtop-engine-9.8.7-$TARGET.tar.gz"

    cat > "$BIN/gh" <<'GH'
#!/usr/bin/env bash
# gh release view TAG --repo R --json assets --jq ...  -> one name per line
# gh release download TAG --repo R --dir D --pattern P ... -> copies
set -euo pipefail
case "$1 $2" in
  "release view") ls "$FAKE_RELEASE" ;;
  "release download")
    shift 2
    dir=""
    patterns=()
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --dir) dir="$2"; shift 2 ;;
        --pattern) patterns+=("$2"); shift 2 ;;
        *) shift ;;
      esac
    done
    for p in "${patterns[@]}"; do cp "$FAKE_RELEASE/$p" "$dir/"; done
    ;;
  *) echo "fake gh: unexpected $*" >&2; exit 9 ;;
esac
GH
    cat > "$BIN/cosign" <<'COSIGN'
#!/usr/bin/env bash
# Records its arguments; fails when FAKE_COSIGN_FAIL is set.
echo "$*" > "$FAKE_COSIGN_ARGS"
[[ -z "${FAKE_COSIGN_FAIL:-}" ]]
COSIGN
    chmod +x "$BIN/gh" "$BIN/cosign"
    export PATH="$BIN:$PATH" FAKE_RELEASE="$RELEASE" FAKE_COSIGN_ARGS="$WORK/cosign-args"
    unset FAKE_COSIGN_FAIL
}

teardown() {
    rm -rf "$WORK"
}

# A release whose archive holds `files` (paths under the archive root).
make_release() {
    local root="$WORK/build"
    rm -rf "$root"
    mkdir -p "$root/vtop-engine-9.8.7-$TARGET"
    for f in "$@"; do
        mkdir -p "$(dirname "$root/vtop-engine-9.8.7-$TARGET/$f")"
        printf '#!/bin/sh\necho "%s 9.8.7"\n' "$(basename "$f")" > "$root/vtop-engine-9.8.7-$TARGET/$f"
        chmod +x "$root/vtop-engine-9.8.7-$TARGET/$f"
    done
    tar -czf "$RELEASE/$ASSET" -C "$root" "vtop-engine-9.8.7-$TARGET"
    echo bundle > "$RELEASE/$ASSET.sigstore.json"
    (cd "$RELEASE" && sha256sum "./$ASSET" "./$ASSET.sigstore.json" > SHA256SUMS)
}

@test "a release that checks out prints the directory holding both binaries" {
    make_release vtop-node vtopctl
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 0 ]
    dir="${lines[-1]}"
    [ -x "$dir/vtop-node" ]
    [ -x "$dir/vtopctl" ]
    # The bundle is checked against the release workflow at THIS tag.
    grep -q -- "--certificate-identity https://github.com/allamiro/vtop-engine/.github/workflows/release.yml@refs/tags/$TAG" "$WORK/cosign-args"
    grep -q -- "--certificate-oidc-issuer https://token.actions.githubusercontent.com" "$WORK/cosign-args"
}

@test "an archive whose digest is not the one in SHA256SUMS fails" {
    make_release vtop-node vtopctl
    printf 'swapped' >> "$RELEASE/$ASSET"
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"SHA256SUMS says"* ]]
}

@test "an archive SHA256SUMS does not list fails" {
    make_release vtop-node vtopctl
    sed -i "/$ASSET\$/d" "$RELEASE/SHA256SUMS"
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"not listed once in SHA256SUMS"* ]]
}

@test "a bundle that does not verify fails, even with a matching digest" {
    make_release vtop-node vtopctl
    export FAKE_COSIGN_FAIL=1
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"Sigstore bundle does not verify"* ]]
}

@test "a release with no archive for the target exits 3" {
    make_release vtop-node vtopctl
    run bash "$FETCH" "$TAG" "$CACHE" aarch64-unknown-linux-gnu
    [ "$status" -eq 3 ]
}

@test "an archive with no Sigstore bundle fails" {
    make_release vtop-node vtopctl
    rm "$RELEASE/$ASSET.sigstore.json"
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"no Sigstore bundle"* ]]
}

@test "an archive without vtopctl fails" {
    make_release vtop-node
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"no vtopctl"* ]]
}

@test "an archive with a path outside its directory fails before extracting" {
    make_release vtop-node vtopctl
    mkdir -p "$WORK/evil"
    echo x > "$WORK/evil/outside"
    tar -czf "$RELEASE/$ASSET" -C "$WORK/build" "vtop-engine-9.8.7-$TARGET" -C "$WORK/evil" outside
    (cd "$RELEASE" && sha256sum "./$ASSET" > SHA256SUMS)
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"outside vtop-engine-9.8.7-$TARGET/"* ]]
    [ ! -e "$CACHE/$TAG/extracted/outside" ]
}

@test "a cached download is checked again, not trusted" {
    make_release vtop-node vtopctl
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 0 ]
    printf 'rot' >> "$CACHE/$TAG/$ASSET"
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"SHA256SUMS says"* ]]
}

@test "a pre-release tag is refused" {
    run bash "$FETCH" v1.0.0-rc.1 "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"not a final release tag"* ]]
}

@test "usage errors exit 2" {
    run bash "$FETCH" "$TAG"
    [ "$status" -eq 2 ]
}

# --- scripts/compat/cell.sh -------------------------------------------------

make_bins() {
    mkdir -p "$WORK/released" "$WORK/head"
    printf '#!/bin/sh\n' > "$WORK/released/vtopctl"
    printf '#!/bin/sh\n' > "$WORK/head/vtopctl"
    chmod +x "$WORK/released/vtopctl" "$WORK/head/vtopctl"
}

@test "a cell that does not exist fails, naming it" {
    make_bins
    run bash "${BATS_TEST_DIRNAME}/../compat/cell.sh" nope v0.9.0 forward pass "$WORK/released" "$WORK/head"
    [ "$status" -eq 1 ]
    [[ "$output" == *"no cell nope with direction forward"* ]]
}

@test "a refusal with no message to find fails" {
    make_bins
    run bash "${BATS_TEST_DIRNAME}/../compat/cell.sh" manifest v0.9.0 forward refuse "$WORK/released" "$WORK/head"
    [ "$status" -eq 1 ]
    [[ "$output" == *"needs the message"* ]]
}

@test "the manifest cell refuses an expectation it cannot check" {
    make_bins
    run bash "${BATS_TEST_DIRNAME}/../compat/cell.sh" manifest v0.9.0 forward refuse "$WORK/released" "$WORK/head" "newer"
    [ "$status" -eq 1 ]
    [[ "$output" == *"expects pass or hash-failure"* ]]
}

@test "a released binary that is missing fails before anything runs" {
    make_bins
    rm "$WORK/released/vtopctl"
    run bash "${BATS_TEST_DIRNAME}/../compat/cell.sh" manifest v0.9.0 forward pass "$WORK/released" "$WORK/head"
    [ "$status" -eq 1 ]
    [[ "$output" == *"is not executable"* ]]
}
