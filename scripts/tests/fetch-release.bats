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
  "release view")
    case "$*" in
      *isDraft*) echo "draft=${FAKE_DRAFT:-false} prerelease=${FAKE_PRERELEASE:-false}" ;;
      *) ls "$FAKE_RELEASE" ;;
    esac
    ;;
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
# Reports FAKE_COSIGN_VERSION (default v3.0.2) for `version`; otherwise records
# its arguments and fails when FAKE_COSIGN_FAIL is set.
if [[ "${1:-}" == version ]]; then
  printf 'GitVersion:    %s\n' "${FAKE_COSIGN_VERSION:-v3.0.2}"
  exit 0
fi
echo "$*" > "$FAKE_COSIGN_ARGS"
[[ -z "${FAKE_COSIGN_FAIL:-}" ]]
COSIGN
    chmod +x "$BIN/gh" "$BIN/cosign"
    export PATH="$BIN:$PATH" FAKE_RELEASE="$RELEASE" FAKE_COSIGN_ARGS="$WORK/cosign-args"
    unset FAKE_COSIGN_FAIL FAKE_COSIGN_VERSION FAKE_DRAFT FAKE_PRERELEASE
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

# SHA256SUMS for the release as it now is.
reseal() {
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

@test "cosign v2 cannot read the releases' bundles, so it is refused" {
    make_release vtop-node vtopctl
    export FAKE_COSIGN_VERSION=v2.4.1
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"not cosign v3 or later"* ]]
    [ ! -e "$WORK/cosign-args" ]
}

@test "a cosign that does not say its version is refused" {
    make_release vtop-node vtopctl
    export FAKE_COSIGN_VERSION=unknown
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"not cosign v3 or later"* ]]
}

@test "a draft release is not downloaded" {
    make_release vtop-node vtopctl
    export FAKE_DRAFT=true
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"not a published final release"* ]]
    [ ! -e "$CACHE/$TAG/$ASSET" ]
}

@test "a pre-release is not downloaded" {
    make_release vtop-node vtopctl
    export FAKE_PRERELEASE=true
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"not a published final release"* ]]
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
    echo x > "$WORK/build/outside"
    tar -czf "$RELEASE/$ASSET" -C "$WORK/build" "vtop-engine-9.8.7-$TARGET" outside
    reseal
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"outside vtop-engine-9.8.7-$TARGET/"* ]]
    [ ! -e "$CACHE/$TAG/extracted/outside" ]
}

@test "a cached download is checked again, and a bad one is fetched again" {
    make_release vtop-node vtopctl
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 0 ]
    printf 'rot' >> "$CACHE/$TAG/$ASSET"
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 0 ]
    [[ "$output" == *"SHA256SUMS says"* ]]
    [[ "$output" == *"downloading it again"* ]]
}

@test "a bad cached copy of a bad release still fails" {
    make_release vtop-node vtopctl
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 0 ]
    printf 'swapped' >> "$RELEASE/$ASSET"
    printf 'rot' >> "$CACHE/$TAG/$ASSET"
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
}

# raw_tar <name>...: a gzip tar of the good archive plus one regular file
# under each <name>, written byte by byte, because tar itself strips leading
# slashes and `..` when it writes.
raw_tar() {
    perl -e '
        sub header {
            my ($name, $mode, $size, $type) = @_;
            my $h = pack("a100 a8 a8 a8 a12 a12 a8 a1 a100 a6 a2 a32 a32 a8 a8 a155 a12",
                $name, sprintf("%07o", $mode), "0000000", "0000000",
                sprintf("%011o", $size), "00000000000", " " x 8, $type, "",
                "ustar", "00", "", "", "", "", "", "");
            my $sum = unpack("%32C*", $h);
            substr($h, 148, 8) = sprintf("%06o\0 ", $sum);
            return $h;
        }
        sub entry {
            my ($name, $mode, $body, $type) = @_;
            my $out = header($name, $mode, length($body), $type) . $body;
            my $pad = (512 - length($body) % 512) % 512;
            return $out . ("\0" x $pad);
        }
        my $top = shift @ARGV;
        my $tar = entry("$top/", 0755, "", "5");
        for my $bin ("vtop-node", "vtopctl") {
            $tar .= entry("$top/$bin", 0755, "#!/bin/sh\necho \"$bin 9.8.7\"\n", "0");
        }
        $tar .= entry($_, 0644, "x", "0") for @ARGV;
        print $tar . ("\0" x 1024);
    ' "vtop-engine-9.8.7-$TARGET" "$@" | gzip > "$RELEASE/$ASSET"
    reseal
}

@test "an absolute path in the archive fails before extracting" {
    make_release vtop-node vtopctl
    raw_tar /etc/evil
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    # tar's own listing may already strip the prefix, which leaves the name
    # outside the archive's directory: refused either way, unextracted.
    [[ "$output" == *"absolute path"* || "$output" == *"outside vtop-engine-9.8.7-$TARGET/"* ]]
    [ ! -e "$CACHE/$TAG/extracted" ]
}

@test "a parent path in the archive fails before extracting" {
    make_release vtop-node vtopctl
    raw_tar "vtop-engine-9.8.7-$TARGET/../evil"
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    # tar's own listing may already strip the prefix, which leaves the name
    # outside the archive's directory: refused either way, unextracted.
    [[ "$output" == *"parent path"* || "$output" == *"outside vtop-engine-9.8.7-$TARGET/"* ]]
    [ ! -e "$CACHE/$TAG/extracted" ]
}

@test "the perl-written archive itself checks out" {
    make_release vtop-node vtopctl
    raw_tar "vtop-engine-9.8.7-$TARGET/extra"
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 0 ]
}

@test "a symlinked binary fails" {
    make_release vtop-node
    ln -s /bin/sh "$WORK/build/vtop-engine-9.8.7-$TARGET/vtopctl"
    tar -czf "$RELEASE/$ASSET" -C "$WORK/build" "vtop-engine-9.8.7-$TARGET"
    reseal
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"not files or directories"* ]]
}

@test "a binary that is not executable fails" {
    make_release vtop-node vtopctl
    chmod -x "$WORK/build/vtop-engine-9.8.7-$TARGET/vtopctl"
    tar -czf "$RELEASE/$ASSET" -C "$WORK/build" "vtop-engine-9.8.7-$TARGET"
    reseal
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"is not executable"* ]]
}

@test "an archive listed twice in SHA256SUMS fails" {
    make_release vtop-node vtopctl
    (cd "$RELEASE" && sha256sum "./$ASSET" >> SHA256SUMS)
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"not listed once"* ]]
}

@test "a binary that reports another version fails, where the host can run it" {
    [[ "$(uname -m)-$(uname -s)" == x86_64-Linux ]] || skip "the binaries run only on x86_64 Linux"
    make_release vtop-node vtopctl
    printf '#!/bin/sh\necho "vtopctl 9.8.6"\n' > "$WORK/build/vtop-engine-9.8.7-$TARGET/vtopctl"
    tar -czf "$RELEASE/$ASSET" -C "$WORK/build" "vtop-engine-9.8.7-$TARGET"
    reseal
    run bash "$FETCH" "$TAG" "$CACHE" "$TARGET"
    [ "$status" -eq 1 ]
    [[ "$output" == *"not version 9.8.7"* ]]
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

@test "the ledger cell refuses an expectation it cannot check" {
    make_bins
    run bash "${BATS_TEST_DIRNAME}/../compat/cell.sh" ledger-sqlite v0.9.0 forward hash-failure "$WORK/released" "$WORK/head"
    [ "$status" -eq 1 ]
    [[ "$output" == *"the ledger cell expects pass or refuse"* ]]
}

@test "a ledger cell fails on binaries that archive nothing, in each direction" {
    make_bins
    run bash "${BATS_TEST_DIRNAME}/../compat/cell.sh" ledger-sqlite v0.9.0 forward pass "$WORK/released" "$WORK/head"
    [ "$status" -ne 0 ]
    [[ "$output" == *"[compat/ledger-sqlite/forward/v0.9.0] FAIL: this build wrote no ledger"* ]]
    run bash "${BATS_TEST_DIRNAME}/../compat/cell.sh" ledger-sqlite v0.9.0 upgrade pass "$WORK/released" "$WORK/head"
    [ "$status" -ne 0 ]
    [[ "$output" == *"[compat/ledger-sqlite/upgrade/v0.9.0] FAIL: the released v0.9.0 wrote no ledger"* ]]
    run bash "${BATS_TEST_DIRNAME}/../compat/cell.sh" digests-audit v0.9.0 forward pass "$WORK/released" "$WORK/head"
    [ "$status" -ne 0 ]
    [[ "$output" == *"[compat/digests-audit/forward/v0.9.0] FAIL: the writer wrote no ledger"* ]]
}

@test "a released binary that is missing fails before anything runs" {
    make_bins
    rm "$WORK/released/vtopctl"
    run bash "${BATS_TEST_DIRNAME}/../compat/cell.sh" manifest v0.9.0 forward pass "$WORK/released" "$WORK/head"
    [ "$status" -eq 1 ]
    [[ "$output" == *"is not executable"* ]]
}
