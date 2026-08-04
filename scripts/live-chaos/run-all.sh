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
  if [[ -n "$scenario_workdir" ]] \
    && CHAOS_WORKDIR="$scenario_workdir" bash "$scenario"; then
    RESULTS+=("PASS $name")
  elif [[ -z "$scenario_workdir" ]] && bash "$scenario"; then
    RESULTS+=("PASS $name")
  else
    RESULTS+=("FAIL $name")
    overall=1
  fi
done

echo
echo "=== live-chaos summary ==="
printf '%s\n' "${RESULTS[@]}"
exit $overall
