#!/usr/bin/env bash
# Regenerate the operator-facing surface snapshots in docs/surface/ (#719):
# the vtopctl and vtop-node command trees, and every metric family the engine
# and a node export. Run it after adding, renaming or removing a command,
# flag, accepted value, metric or label, and commit the result with the
# change. A removal or rename also needs a row in docs/deprecations.md, which
# scripts/check-surface-deprecations.py checks on every pull request.
#
# The snapshot tests write instead of compare when VTOP_UPDATE_SURFACE=1;
# this runs them that way, for both crates. No cargo feature changes the
# surface, so the default build is the one to generate from.
set -euo pipefail

cd "$(dirname "$0")/.."
export VTOP_UPDATE_SURFACE=1
cargo test --locked -p vtop-cli --test surface_snapshot
cargo test --locked -p vtop-node surface_snapshot
git status --short -- docs/surface/
