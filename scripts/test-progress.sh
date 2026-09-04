#!/usr/bin/env bash
# Run the workspace test suite with a live progress bar.
#
# `cargo test --workspace` prints nothing for minutes at a time — it compiles
# every crate, then runs each test binary in turn — so a long run is
# indistinguishable from a hung one. This splits it into the two phases cargo
# already has and draws each of them against a real denominator:
#
#   compile   crates built, out of the workspace's crate count
#   test      test binaries run, out of the binaries cargo produced
#
# Both numbers come from cargo itself (`cargo metadata`, and the JSON message
# stream of `--no-run`), so the bar cannot drift from what is actually
# happening — a progress bar that guesses is worse than none, because it is
# believed.
#
# Usage:
#   scripts/test-progress.sh                 # whole workspace
#   scripts/test-progress.sh -p vtop-broker  # any cargo test args pass through
#
# Exit status is the suite's: any failing binary fails the run, and every
# failure is reprinted at the end so it is not lost above the bar.
set -uo pipefail

BOLD=$'\033[1m'; RED=$'\033[31m'; GREEN=$'\033[32m'; RESET=$'\033[0m'
[[ -t 1 ]] || { BOLD=""; RED=""; GREEN=""; RESET=""; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

bar() { # <done> <total> <label>
  local done="$1" total="$2" label="$3" width=32
  [[ "$total" -gt 0 ]] || total=1
  local filled=$(( done * width / total ))
  [[ "$filled" -gt "$width" ]] && filled=$width
  local pct=$(( done * 100 / total ))
  printf '\r%s[%s%s] %3d%%%s %-46s' \
    "$BOLD" \
    "$(printf '%*s' "$filled" '' | tr ' ' '#')" \
    "$(printf '%*s' $(( width - filled )) '')" \
    "$pct" "$RESET" "$label"
}

# ---------------------------------------------------------------------------
# Phase 1 — compile. The denominator is the workspace's own crate count plus
# its dependencies, which cargo will report as it goes; using the dependency
# count rather than the workspace count keeps the bar honest on a cold build,
# where the dependencies ARE the wait.
# ---------------------------------------------------------------------------
TOTAL_CRATES="$(cargo metadata --format-version 1 2>/dev/null \
  | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["packages"]))' 2>/dev/null || echo 0)"
[[ "$TOTAL_CRATES" -gt 0 ]] || TOTAL_CRATES=200

printf '%scompiling%s (%s crates in the graph)\n' "$BOLD" "$RESET" "$TOTAL_CRATES"
compiled=0
cargo test --workspace --no-run --message-format=json "$@" \
  > "$WORK/build.json" 2> "$WORK/build.err" &
BUILD_PID=$!
while kill -0 "$BUILD_PID" 2>/dev/null; do
  compiled="$(grep -c '"reason":"compiler-artifact"' "$WORK/build.json" 2>/dev/null || echo 0)"
  bar "$compiled" "$TOTAL_CRATES" "building"
  sleep 0.3
done
wait "$BUILD_PID"; BUILD_RC=$?
bar "$TOTAL_CRATES" "$TOTAL_CRATES" "built"
printf '\n'

if [[ "$BUILD_RC" -ne 0 ]]; then
  printf '%scompile failed%s\n' "$RED" "$RESET"
  # cargo's human-readable diagnostics went to stderr; the JSON stream on
  # stdout is not what a person wants to read.
  tail -40 "$WORK/build.err"
  exit "$BUILD_RC"
fi

# ---------------------------------------------------------------------------
# Phase 2 — run. Each test binary is one unit, which is the granularity a
# person actually waits on: "12 of 19 binaries" answers "how much longer".
# ---------------------------------------------------------------------------
python3 - "$WORK/build.json" > "$WORK/binaries" <<'PY'
import json, sys
seen = []
for line in open(sys.argv[1]):
    try:
        msg = json.loads(line)
    except ValueError:
        continue
    # `executable` is non-null exactly for the artifacts cargo would run.
    exe = msg.get("executable")
    if exe and msg.get("profile", {}).get("test") and exe not in seen:
        seen.append(exe)
print("\n".join(seen))
PY

TOTAL_BINS="$(grep -c . "$WORK/binaries" || true)"
[[ "${TOTAL_BINS:-0}" -gt 0 ]] || { printf 'no test binaries were produced\n'; exit 0; }

printf '%srunning%s (%s test binaries)\n' "$BOLD" "$RESET" "$TOTAL_BINS"
ran=0; passed=0; failed=0; failing_bins=()
while IFS= read -r exe; do
  name="$(basename "$exe" | sed 's/-[0-9a-f]\{16\}$//')"
  bar "$ran" "$TOTAL_BINS" "$name"
  "$exe" --test-threads="${TEST_THREADS:-4}" > "$WORK/out.$ran" 2>&1
  rc=$?
  # `test result: ok. N passed; M failed` — the counts, not a guess.
  p="$(grep -hoE 'test result: [a-zA-Z]+\. [0-9]+ passed' "$WORK/out.$ran" \
      | grep -oE '[0-9]+ passed' | grep -oE '[0-9]+' | paste -sd+ - | bc 2>/dev/null || echo 0)"
  f="$(grep -hoE '[0-9]+ failed' "$WORK/out.$ran" | grep -oE '[0-9]+' | paste -sd+ - | bc 2>/dev/null || echo 0)"
  passed=$(( passed + ${p:-0} ))
  failed=$(( failed + ${f:-0} ))
  if [[ "$rc" -ne 0 ]]; then
    failing_bins+=("$name:$WORK/out.$ran")
  fi
  ran=$(( ran + 1 ))
  bar "$ran" "$TOTAL_BINS" "$name  ${GREEN}${passed} passed${RESET}$( [[ "$failed" -gt 0 ]] && printf ' %s%s failed%s' "$RED" "$failed" "$RESET" )"
done < "$WORK/binaries"
printf '\n'

if [[ "${#failing_bins[@]}" -gt 0 ]]; then
  printf '\n%s%d binary/binaries failed%s\n' "$RED" "${#failing_bins[@]}" "$RESET"
  for entry in "${failing_bins[@]}"; do
    printf '\n%s=== %s ===%s\n' "$BOLD" "${entry%%:*}" "$RESET"
    # The failure list and the panics, not the whole passing roll-call.
    grep -E '^(test .*FAILED|thread .* panicked|assertion|---- .* stdout)' -A3 "${entry#*:}" \
      | head -40
  done
  printf '\n%s%d passed, %d failed%s\n' "$RED" "$passed" "$failed" "$RESET"
  exit 1
fi

printf '%s%d tests passed across %d binaries%s\n' "$GREEN" "$passed" "$TOTAL_BINS" "$RESET"
