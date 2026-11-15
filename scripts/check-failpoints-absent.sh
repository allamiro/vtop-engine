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
#   scripts/check-failpoints-absent.sh   (needs cargo; reads the lock file)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

fail() { printf 'check-failpoints-absent: FAIL: %s\n' "$*" >&2; exit 1; }

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
