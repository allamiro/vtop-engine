#!/usr/bin/env bash
# Run the workspace test suite with a live progress bar.
#
# `cargo test --workspace` prints nothing for minutes at a time — it compiles
# every crate, then runs each test binary in turn — so a long run is
# indistinguishable from a hung one. This splits it into the phases cargo
# already has and draws each of them against a real denominator:
#
#   compile   packages built, out of the packages in the selection's graph
#   test      test binaries run, out of the binaries cargo produced
#   doctest   the documentation tests cargo would have run, as one unit
#
# Both denominators come from cargo itself (`cargo metadata` with the same
# manifest, features and target as the build, and the JSON message stream of
# `--no-run`), so the bar cannot drift from what is actually happening — a
# progress bar that guesses is worse than none, because it is believed. When
# a number cannot be obtained, the script refuses to run rather than draw an
# invented one.
#
# Usage:
#   scripts/test-progress.sh                       # whole workspace
#   scripts/test-progress.sh -p vtop-broker        # cargo's package selection
#   scripts/test-progress.sh --exclude vtop-kafka  # the workspace minus one
#   scripts/test-progress.sh -- --ignored          # everything after `--` goes
#                                                  # to every test binary
#   scripts/test-progress.sh lease -- --nocapture  # a test-name filter, too
#
# Arguments split exactly as `cargo test` splits them: options before `--`
# are cargo's (package selection, features, target), a bare word before `--`
# is a test-name filter, and everything after `--` is the harness's. The
# harness's thread count is cargo's own default unless TEST_THREADS is set.
# Each binary runs through cargo's configured `target.<triple>.runner` when
# one is set, from the same sources cargo reads (the environment, then the
# project's `.cargo/config.toml` files, then the home one).
#
# Exit status is the suite's: any failing binary or doctest fails the run,
# and every failure is reprinted at the end so it is not lost above the bar.
set -uo pipefail

BOLD=$'\033[1m'; RED=$'\033[31m'; GREEN=$'\033[32m'; RESET=$'\033[0m'
[[ -t 1 ]] || { BOLD=""; RED=""; GREEN=""; RESET=""; }

die() { printf 'test-progress: %s\n' "$*" >&2; exit 2; }
for tool in cargo rustc python3 awk; do
  command -v "$tool" >/dev/null 2>&1 || die "$tool is required and was not found on PATH"
done

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

bar() { # <done> <total> <label>
  local done="$1" total="$2" label="$3" width=32
  [[ "$total" -gt 0 ]] || total=1
  [[ "$done" -gt "$total" ]] && done=$total
  local filled=$(( done * width / total ))
  local pct=$(( done * 100 / total ))
  printf '\r%s[%s%s] %3d%%%s %-46s' \
    "$BOLD" \
    "$(printf '%*s' "$filled" '' | tr ' ' '#')" \
    "$(printf '%*s' $(( width - filled )) '')" \
    "$pct" "$RESET" "$label"
}

# ---------------------------------------------------------------------------
# Argument split. cargo_args go to the compile; bin_args go to every test
# binary. A bare word before `--` is cargo's positional test-name filter,
# which cargo would itself hand to the binary — so it goes there here too.
# Options that take a value are listed so their value is not mistaken for a
# filter. The graph-shaping options (manifest, features, target) are ALSO
# collected for `cargo metadata`, so the denominator describes the graph
# the build actually resolves rather than the default one.
# ---------------------------------------------------------------------------
cargo_args=(); bin_args=(); metadata_args=(); scoped=0; past_sep=0
expect_value=""   # the option whose value the next argument is, if any
TARGET=""
for arg in "$@"; do
  if (( past_sep )); then
    bin_args+=("$arg")
    continue
  fi
  if [[ "$arg" == "--" ]]; then
    past_sep=1
    continue
  fi
  if [[ -n "$expect_value" ]]; then
    cargo_args+=("$arg")
    case "$expect_value" in
      --manifest-path|--features|-F) metadata_args+=("$expect_value" "$arg") ;;
      --target) TARGET="$arg" ;;
    esac
    expect_value=""
    continue
  fi
  case "$arg" in
    -p|--package|--exclude|--features|-F|--target|--target-dir|--manifest-path|\
    --profile|-j|--jobs|--test|--bin|--example|--bench|--color|--config|-Z|\
    --message-format)
      cargo_args+=("$arg"); expect_value="$arg" ;;
    --manifest-path=*|--features=*)
      cargo_args+=("$arg"); metadata_args+=("$arg") ;;
    --all-features|--no-default-features)
      cargo_args+=("$arg"); metadata_args+=("$arg") ;;
    --target=*)
      cargo_args+=("$arg"); TARGET="${arg#--target=}" ;;
    -*)
      cargo_args+=("$arg") ;;
    *)
      bin_args+=("$arg") ;;
  esac
  # `--exclude` is NOT a selection: cargo requires it alongside --workspace,
  # so an exclusion alone must keep the default workspace selection.
  case "$arg" in
    -p|-p*|--package|--package=*|--workspace|--all) scoped=1 ;;
  esac
done
[[ -z "$expect_value" ]] || die "option $expect_value is missing its value"
# `--workspace` is the DEFAULT selection, never an override: a caller who
# named packages gets exactly those.
(( scoped )) || cargo_args=(--workspace ${cargo_args[@]+"${cargo_args[@]}"})
if [[ -n "${TEST_THREADS:-}" ]]; then
  case " ${bin_args[*]+"${bin_args[*]}"} " in
    *" --test-threads"*) ;;
    *) bin_args+=("--test-threads=$TEST_THREADS") ;;
  esac
fi

HOST="$(rustc -vV | sed -n 's/^host: //p')"
[[ -n "$HOST" ]] || die "could not determine the host triple from rustc"
TRIPLE="${TARGET:-$HOST}"

# ---------------------------------------------------------------------------
# The configured target runner, resolved the way cargo resolves it: the
# environment variable first, then every `.cargo/config.toml` from the
# working directory upward, then the home directory's. Cargo's own resolver
# for this is unstable on the command line (`cargo config get` needs -Z), so
# the same sources are read directly; a runner is a string or an argv list.
# ---------------------------------------------------------------------------
runner_for() { # <triple> — prints the runner argv, one element per line
  local triple="$1" env_name
  env_name="CARGO_TARGET_$(printf '%s' "$triple" | tr 'a-z-' 'A-Z_')_RUNNER"
  if [[ -n "${!env_name:-}" ]]; then
    printf '%s\n' "${!env_name}"
    return
  fi
  python3 - "$triple" "$PWD" "${CARGO_HOME:-$HOME/.cargo}" <<'PY'
import os, sys, tomllib
triple, cwd, cargo_home = sys.argv[1], sys.argv[2], sys.argv[3]
paths = []
here = cwd
while True:
    for name in ("config.toml", "config"):
        paths.append(os.path.join(here, ".cargo", name))
    parent = os.path.dirname(here)
    if parent == here:
        break
    here = parent
for name in ("config.toml", "config"):
    paths.append(os.path.join(cargo_home, name))
for path in paths:
    try:
        with open(path, "rb") as handle:
            config = tomllib.load(handle)
    except FileNotFoundError:
        continue
    runner = config.get("target", {}).get(triple, {}).get("runner")
    if runner is None:
        continue
    if isinstance(runner, str):
        print(runner)
    else:
        print("\n".join(runner))
    break
PY
}
RUNNER=()
while IFS= read -r part; do
  [[ -n "$part" ]] && RUNNER+=("$part")
done < <(runner_for "$TRIPLE")
# A single-string runner is a shell word list, exactly as cargo treats it.
if [[ "${#RUNNER[@]}" -eq 1 ]]; then
  # shellcheck disable=SC2206 # word-splitting the configured string is the point
  RUNNER=(${RUNNER[0]})
fi

# ---------------------------------------------------------------------------
# Phase 1 — compile. Numerator and denominator are the same unit — packages —
# so the bar cannot overrun: cargo emits one `compiler-artifact` per TARGET
# (a library, its test harness, each integration test), so counting messages
# would count one package several times. The denominator is the resolve
# graph for the SAME manifest, features and target as the build, filtered
# to the platform being built for; a scoped run builds a subset of it and
# the bar says so.
# ---------------------------------------------------------------------------
TOTAL_PACKAGES="$(cargo metadata --format-version 1 --filter-platform "$TRIPLE" \
    ${metadata_args[@]+"${metadata_args[@]}"} 2>"$WORK/metadata.err" \
  | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["resolve"]["nodes"]))' 2>>"$WORK/metadata.err")"
[[ "${TOTAL_PACKAGES:-0}" -gt 0 ]] || {
  cat "$WORK/metadata.err" >&2
  die "cargo metadata did not yield a package count; refusing to draw a guessed bar"
}

built_packages() {
  # Distinct packages with at least one artifact so far; a missing file
  # (the first poll on a cold build) is zero, not an error.
  [[ -f "$WORK/build.json" ]] || { echo 0; return; }
  grep '"reason":"compiler-artifact"' "$WORK/build.json" 2>/dev/null \
    | grep -oE '"package_id":"[^"]*"' | sort -u | wc -l | tr -d ' '
}

printf '%scompiling%s (%s packages in the graph)\n' "$BOLD" "$RESET" "$TOTAL_PACKAGES"
# `json-render-diagnostics`: artifacts stay on stdout as JSON for the bar,
# and rustc's diagnostics are rendered to stderr as a person would read them
# — plain `json` buries the actual error inside `compiler-message` records.
cargo test "${cargo_args[@]}" --no-run --message-format=json-render-diagnostics \
  > "$WORK/build.json" 2> "$WORK/build.err" &
BUILD_PID=$!
while kill -0 "$BUILD_PID" 2>/dev/null; do
  bar "$(built_packages)" "$TOTAL_PACKAGES" "building"
  sleep 0.3
done
wait "$BUILD_PID"; BUILD_RC=$?
bar "$(built_packages)" "$TOTAL_PACKAGES" "built"
printf '\n'

if [[ "$BUILD_RC" -ne 0 ]]; then
  printf '%scompile failed%s\n' "$RED" "$RESET"
  cat "$WORK/build.err"
  exit "$BUILD_RC"
fi

# ---------------------------------------------------------------------------
# Phase 2 — run. Each test binary is one unit, which is the granularity a
# person actually waits on: "12 of 19 binaries" answers "how much longer".
# ---------------------------------------------------------------------------
python3 - "$WORK/build.json" > "$WORK/binaries" <<'PY' || exit 2
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
[[ "${TOTAL_BINS:-0}" -gt 0 ]] || {
  # A build that produced nothing runnable is not a passing suite: cargo
  # itself reports "running 0 tests" and exits 0 only when the selection
  # legitimately has none, and we cannot tell that apart from a broken
  # parse here — so say what happened and refuse to claim success.
  die "the build produced no test binaries (selection: ${cargo_args[*]})"
}

sum_counts() { # <passed|failed> <file> — libtest's own totals, from its
  # `test result:` lines only: a test that PRINTS "7 passed" is not a result.
  grep -hE '^test result: ' "$2" | grep -oE "[0-9]+ $1" | awk '{s+=$1} END{print s+0}'
}

printf '%srunning%s (%s test binaries' "$BOLD" "$RESET" "$TOTAL_BINS"
[[ "${#RUNNER[@]}" -gt 0 ]] && printf ' via runner %s' "${RUNNER[*]}"
printf ')\n'
ran=0; passed=0; failed=0; failing_bins=()
while IFS= read -r exe; do
  name="$(basename "$exe" | sed 's/-[0-9a-f]\{16\}$//')"
  bar "$ran" "$TOTAL_BINS" "$name"
  # The `[@]+` expansions keep an empty array from tripping `set -u` on the
  # bash 3.2 macOS ships.
  ${RUNNER[@]+"${RUNNER[@]}"} "$exe" ${bin_args[@]+"${bin_args[@]}"} > "$WORK/out.$ran" 2>&1
  rc=$?
  p="$(sum_counts passed "$WORK/out.$ran")"
  f="$(sum_counts failed "$WORK/out.$ran")"
  passed=$(( passed + p ))
  failed=$(( failed + f ))
  if [[ "$rc" -ne 0 ]]; then
    failing_bins+=("$name:$WORK/out.$ran")
  fi
  ran=$(( ran + 1 ))
  bar "$ran" "$TOTAL_BINS" "$name  ${GREEN}${passed} passed${RESET}$( [[ "$failed" -gt 0 ]] && printf ' %s%s failed%s' "$RED" "$failed" "$RESET" )"
done < "$WORK/binaries"
printf '\n'

# ---------------------------------------------------------------------------
# Phase 3 — doctests. `cargo test` runs them and this split flow would not:
# `--no-run` cannot build them (rustdoc compiles them on the fly) and they
# produce no binary to run. One unit, drawn while cargo runs them, so a
# failing doctest fails the run exactly as it would under `cargo test`.
# ---------------------------------------------------------------------------
printf '%sdoctests%s\n' "$BOLD" "$RESET"
bar 0 1 "doctests"
cargo test "${cargo_args[@]}" --doc -- ${bin_args[@]+"${bin_args[@]}"} \
  > "$WORK/out.doc" 2>&1
DOC_RC=$?
p="$(sum_counts passed "$WORK/out.doc")"
f="$(sum_counts failed "$WORK/out.doc")"
passed=$(( passed + p ))
failed=$(( failed + f ))
if [[ "$DOC_RC" -ne 0 ]]; then
  failing_bins+=("doctests:$WORK/out.doc")
fi
bar 1 1 "doctests  ${GREEN}${passed} passed${RESET}$( [[ "$failed" -gt 0 ]] && printf ' %s%s failed%s' "$RED" "$failed" "$RESET" )"
printf '\n'

if [[ "${#failing_bins[@]}" -gt 0 ]]; then
  printf '\n%s%d binary/binaries failed%s\n' "$RED" "${#failing_bins[@]}" "$RESET"
  for entry in "${failing_bins[@]}"; do
    printf '\n%s=== %s ===%s\n' "$BOLD" "${entry%%:*}" "$RESET"
    # The failure list and the panics, not the whole passing roll-call.
    grep -E '^(test .*FAILED|thread .* panicked|assertion|---- .* stdout|error)' -A3 "${entry#*:}" \
      | head -40
  done
  printf '\n%s%d passed, %d failed%s\n' "$RED" "$passed" "$failed" "$RESET"
  exit 1
fi

printf '%s%d tests passed across %d binaries plus doctests%s\n' "$GREEN" "$passed" "$TOTAL_BINS" "$RESET"
