#!/usr/bin/env bash
# The failpoints feature (#725) is never in a shipped build.
#
# A `failpoints` build of vtopctl kills itself with SIGKILL wherever
# VTOP_FAILPOINT says. It exists for the engine crash sweep and must never
# reach a release archive, the container image or the chart. This check
# fails when:
#   1. the workspace's default features, which every release build uses,
#      resolve vtop-cli with `failpoints` (cargo tree, inverted on vtop-cli,
#      over the whole workspace and its normal and build edges, so a member
#      that depended on vtop-cli with the feature would be seen as a release
#      build sees it); a
#      positive control first proves the same query does see the feature
#      when it is asked for, so a changed output format cannot pass by
#      matching nothing;
#   2. a cargo build in release.yml or docker/Dockerfile names the feature,
#      or turns on every feature with --all-features.
#
# With --binary it checks a built vtopctl instead: the line the failpoint
# prints just before the kill is compiled only into a `failpoints` build, so
# a binary that contains it can kill itself. The text is read from
# crates/vtop-cli/src/failpoints.rs first, so rewording the message cannot
# make this check search for a string that no longer exists.
# --expect-present is the positive control, run on the sweep's own build.
#
#   scripts/check-failpoints-absent.sh   (needs cargo; reads the lock file)
#   scripts/check-failpoints-absent.sh --binary target/release/vtopctl
#   scripts/check-failpoints-absent.sh --binary target/debug/vtopctl --expect-present
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

fail() { printf 'check-failpoints-absent: FAIL: %s\n' "$*" >&2; exit 1; }

if [[ "${1:-}" == "--binary" ]]; then
  binary="${2:-}"
  expect="${3:-}"
  [[ -n "$binary" && -f "$binary" ]] || fail "--binary needs the path of a built vtopctl, got '${binary}'"
  [[ -z "$expect" || "$expect" == "--expect-present" ]] || fail "unknown argument '$expect'"
  marker="is being killed here on purpose"
  grep -qF "$marker" crates/vtop-cli/src/failpoints.rs \
    || fail "crates/vtop-cli/src/failpoints.rs no longer prints '$marker' before a kill; update this check to the new text"
  # grep -c, not -q: -q stops reading at the first match and the pipe's
  # writer can then fail with SIGPIPE under pipefail.
  found="$(LC_ALL=C grep -caF "$marker" "$binary" || true)"
  if [[ "$expect" == "--expect-present" ]]; then
    (( found > 0 )) || fail "$binary was meant to be a failpoints build, but it does not contain '$marker'; this check could not tell the two apart"
    echo "check-failpoints-absent: $binary is a failpoints build (the positive control)"
  else
    (( found == 0 )) || fail "$binary contains the failpoint kill ('$marker'): it was built with the failpoints feature and can kill itself on request"
    echo "check-failpoints-absent: $binary has no failpoint code"
  fi
  exit 0
fi

features() { # <extra cargo args...> — vtop-cli's resolved features, one per line
  cargo tree --locked --workspace -e normal,build,features -i vtop-cli --prefix none "$@" \
    | grep -o '^vtop-cli feature "[a-z0-9_-]*"' | sort -u
}

control="$(features --features vtop-cli/failpoints)" \
  || fail "cargo tree could not resolve vtop-cli with --features failpoints"
grep -qx 'vtop-cli feature "failpoints"' <<< "$control" \
  || fail "the positive control did not see the feature it enabled; cargo tree printed:
$control"

shipped="$(features)" || fail "cargo tree could not resolve vtop-cli's default features"
if grep -qx 'vtop-cli feature "failpoints"' <<< "$shipped"; then
  fail "vtop-cli's default features include failpoints, so every release build would carry code that kills the engine on request. Resolved features:
$shipped"
fi

for file in .github/workflows/release.yml docker/Dockerfile; do
  [[ -f "$file" ]] || fail "$file is missing; update this check if it moved"
  if grep -nE 'cargo .*(--all-features|failpoints)' "$file"; then
    fail "$file builds with the failpoints feature (or --all-features) on the lines above"
  fi
done

echo "check-failpoints-absent: vtop-cli's default features are: $(tr '\n' ' ' <<< "$shipped")"
echo "check-failpoints-absent: release.yml and docker/Dockerfile build without failpoints"
