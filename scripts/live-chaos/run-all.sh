#!/usr/bin/env bash
# Run every live chaos scenario (#215) in sequence and summarize.
#
#   cargo build --release -p vtop-node -p vtop-cli --no-default-features
#   scripts/live-chaos/run-all.sh
#
# Scenarios are independent; each brings up its own cluster in its own
# scratch dir and tears it down. CHAOS_PROFILE=debug runs debug binaries.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
declare -a RESULTS=()
overall=0

for scenario in "$SCRIPT_DIR"/scenarios/*.sh; do
  name="$(basename "$scenario")"
  echo "=== $name ==="
  scenario_workdir=""
  if [[ -n "${CHAOS_WORKDIR:-}" ]]; then
    scenario_workdir="${CHAOS_WORKDIR%/}/${name%.sh}"
  fi
  # Exit 77 is a SKIP: the scenario needs something this run did not provide
  # (scenario 22: a previous release's binary). It is reported, not counted
  # as a pass or a failure.
  code=0
  if [[ -n "$scenario_workdir" ]]; then
    CHAOS_WORKDIR="$scenario_workdir" bash "$scenario" || code=$?
  else
    bash "$scenario" || code=$?
  fi
  case "$code" in
    0) RESULTS+=("PASS $name") ;;
    77) RESULTS+=("SKIP $name") ;;
    *)
      RESULTS+=("FAIL $name")
      overall=1
      ;;
  esac
done

echo
echo "=== live-chaos summary ==="
printf '%s\n' "${RESULTS[@]}"
exit $overall
