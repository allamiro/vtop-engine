#!/usr/bin/env bash
# Run the workspace test suite with a live progress bar.
#
# `cargo test --workspace` prints nothing for minutes at a time — it compiles
# every crate, then runs each test binary in turn — so a long run is
# indistinguishable from a hung one. This splits it into the phases cargo
# already has and draws each of them against a real denominator:
#
#   compile   packages built, out of the packages the selection resolves to
#   test      test binaries run, out of the binaries cargo produced
#   doctest   the documentation tests cargo would have run, as one unit
#
# Both denominators come from cargo itself (`cargo metadata` under the same
# manifest, features, target and lock flags as the build, narrowed to the
# dependency closure of the selected packages; and the JSON message stream
# of `--no-run`), so the bar cannot drift from what is actually happening —
# a progress bar that guesses is worse than none, because it is believed.
# When a number cannot be obtained, the script refuses to run rather than
# draw an invented one.
#
# CARGO RUNS THE TESTS. The run phase is one `cargo test` invocation with the
# caller's exact arguments, and the bar is drawn from the `Running` and
# `Doc-tests` lines cargo prints as it goes — so the working directory, the
# configured runner, `[env]` configuration, dynamic-library paths, target
# selection and doctests are all cargo's own, not an imitation of them. The
# script never executes a test binary itself.
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
# are cargo's, everything after `--` is the harness's, and both reach cargo
# unchanged — the only arguments this script adds are `--no-run` with a JSON
# message format for the compile phase and `--no-fail-fast` for the run
# phase (so every binary runs and every failure is reported, rather than the
# first). `--no-run` compiles and stops; `--message-format` is refused,
# because the compile phase needs the JSON stream. The harness's thread
# count is cargo's own default unless TEST_THREADS is set.
#
# Exit status is cargo's: any failing binary or doctest fails the run, and
# every failure is reprinted at the end so it is not lost above the bar.
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
# filter. What shapes the graph or the resolution (manifest, features,
# target, lock flags) is ALSO collected for `cargo metadata`; what selects
# packages is collected for the closure; what selects targets decides the
# doctest phase; `--config` overrides feed the runner lookup.
# ---------------------------------------------------------------------------
cargo_args=(); bin_args=(); metadata_args=(); packages=(); excludes=(); config_overrides=()
past_sep=0; expect_value=""; workspace_flag=0; target_selected=0; doc_only=0; no_run=0
TARGET_ARG=""; MANIFEST_PATH=""
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
      -p|--package) packages+=("$arg") ;;
      --exclude) excludes+=("$arg") ;;
      --manifest-path) metadata_args+=("$expect_value" "$arg"); MANIFEST_PATH="$arg" ;;
      --features|-F) metadata_args+=("$expect_value" "$arg") ;;
      --target) TARGET_ARG="$arg" ;;
      # A --config override shapes resolution (sources, net, registries)
      # as much as it shapes the runner, so metadata sees it too.
      --config) config_overrides+=("$arg"); metadata_args+=("$expect_value" "$arg") ;;
    esac
    expect_value=""
    continue
  fi
  case "$arg" in
    --message-format|--message-format=*)
      die "--message-format is this script's to choose: the compile phase needs cargo's JSON stream" ;;
    -p|--package|--exclude|--features|-F|--target|--target-dir|--manifest-path|\
    --profile|-j|--jobs|--test|--bin|--example|--bench|--color|--config|-Z)
      cargo_args+=("$arg"); expect_value="$arg" ;;
    -p?*)
      cargo_args+=("$arg"); packages+=("${arg#-p}") ;;
    --package=*)
      cargo_args+=("$arg"); packages+=("${arg#--package=}") ;;
    --exclude=*)
      cargo_args+=("$arg"); excludes+=("${arg#--exclude=}") ;;
    --manifest-path=*)
      cargo_args+=("$arg"); metadata_args+=("$arg"); MANIFEST_PATH="${arg#--manifest-path=}" ;;
    --features=*|--all-features|--no-default-features)
      cargo_args+=("$arg"); metadata_args+=("$arg") ;;
    --locked|--offline|--frozen)
      cargo_args+=("$arg"); metadata_args+=("$arg") ;;
    --target=*)
      cargo_args+=("$arg"); TARGET_ARG="${arg#--target=}" ;;
    --config=*)
      cargo_args+=("$arg"); config_overrides+=("${arg#--config=}"); metadata_args+=("$arg") ;;
    --workspace|--all)
      cargo_args+=("$arg"); workspace_flag=1 ;;
    --doc)
      doc_only=1 ;;
    --no-run)
      no_run=1 ;;
    --lib|--bins|--tests|--examples|--benches|--all-targets|\
    --test=*|--bin=*|--example=*|--bench=*)
      cargo_args+=("$arg"); target_selected=1 ;;
    -*)
      cargo_args+=("$arg") ;;
    *)
      bin_args+=("$arg") ;;
  esac
  case "$arg" in
    --test|--bin|--example|--bench) target_selected=1 ;;
  esac
done
[[ -z "$expect_value" ]] || die "option $expect_value is missing its value"
if (( doc_only && target_selected )); then
  # Cargo's own refusal, reproduced before anything runs rather than
  # discovered as a doctest phase that reports nothing.
  die "can't mix --doc with other target selecting options"
fi
if (( doc_only && no_run )); then
  die "can't mix --doc with --no-run"
fi
# `--workspace` is the DEFAULT selection, never an override: a caller who
# named packages gets exactly those. An exclusion is not a selection — cargo
# requires it alongside --workspace — so it keeps the default. A manifest
# path IS a selection under cargo (the package it names), so it keeps the
# default off and the closure below resolves to that package.
if (( ! workspace_flag )) && [[ "${#packages[@]}" -eq 0 && -z "$MANIFEST_PATH" ]]; then
  cargo_args=(--workspace ${cargo_args[@]+"${cargo_args[@]}"})
  workspace_flag=1
fi
if [[ -n "${TEST_THREADS:-}" ]]; then
  case " ${bin_args[*]+"${bin_args[*]}"} " in
    *" --test-threads"*) ;;
    *) bin_args+=("--test-threads=$TEST_THREADS") ;;
  esac
fi

HOST="$(rustc -vV | sed -n 's/^host: //p')"
[[ -n "$HOST" ]] || die "could not determine the host triple from rustc"
# The triple the graph is resolved for: an explicit --target, else the host.
# (CARGO_BUILD_TARGET and build.target are cargo's to honour in the build;
# they only shape the denominator here, and an explicit --target covers the
# cross case the denominator can see.)
TRIPLE="${TARGET_ARG:-$HOST}"

# ---------------------------------------------------------------------------
# Phase 1 — compile. Numerator and denominator are the same unit — packages —
# so the bar cannot overrun: cargo emits one `compiler-artifact` per TARGET
# (a library, its test harness, each integration test), so counting messages
# would count one package several times. The denominator is the dependency
# closure of the SELECTED packages in the resolve graph cargo builds for the
# same manifest, features, target and lock flags: the workspace's default
# members (minus exclusions), or the packages named with -p.
# ---------------------------------------------------------------------------
# The helper scripts are written to files ONCE and invoked by path: a here-
# document nested inside a command substitution is parsed differently by the
# bash 3.2 macOS ships (quotes and brackets in the body can end the
# substitution early), and a script that runs on one bash and not another
# is worse than one that never used the construct.
cat > "$WORK/closure.py" <<'PY'
# The dependency closure of the selected packages over cargo's resolve
# graph. Modes: `count` prints how many packages the build will touch;
# `libs` prints how many selected roots have a library target (the doctest
# units cargo will run).
import fnmatch, json, os, sys
mode = sys.argv[1]
with open(sys.argv[2]) as handle:
    meta = json.load(handle)
workspace_flag = sys.argv[3] == "1"
manifest_path = sys.argv[4]
count = int(sys.argv[5])
selected, excluded = sys.argv[6:6 + count], sys.argv[6 + count:]
by_id = {package["id"]: package for package in meta["packages"]}
members = meta["workspace_members"]
default_members = meta.get("workspace_default_members") or members
if manifest_path and not workspace_flag and not selected:
    # The default for an explicit manifest is the package it names, as
    # under cargo.
    wanted = os.path.realpath(manifest_path)
    named_members = [pid for pid in members if os.path.realpath(by_id[pid]["manifest_path"]) == wanted]
    if named_members:
        default_members = named_members

def named(spec, package):
    # -p accepts a name (with the usual glob patterns), name@version, or a
    # path; match the forms a person types.
    name = package["name"]
    if spec in (name, name + "@" + package["version"], name + ":" + package["version"]):
        return True
    if spec.rstrip("/") == package["manifest_path"].rsplit("/", 1)[0]:
        return True
    return any(ch in spec for ch in "*?[") and fnmatch.fnmatchcase(name, spec)

if selected:
    roots = []
    for spec in selected:
        matches = [pid for pid in members if named(spec, by_id[pid])]
        if not matches:
            sys.exit("package " + spec + " is not a member of this workspace")
        roots.extend(matches)
else:
    roots = list(members if workspace_flag else default_members)
roots = [pid for pid in roots if not any(named(spec, by_id[pid]) for spec in excluded)]
if not roots:
    sys.exit("the selection excludes every package")

if mode == "libs":
    print(sum(1 for pid in roots if any("lib" in target["kind"] or "rlib" in target["kind"] for target in by_id[pid]["targets"])))
    sys.exit(0)

deps = {node["id"]: node["deps"] for node in meta["resolve"]["nodes"]}
root_set = set(roots)
seen = set()
stack = list(roots)
while stack:
    pid = stack.pop()
    if pid in seen:
        continue
    seen.add(pid)
    # A root builds its dev-dependencies too, whichever way the walk reached
    # it; a dependency builds only its normal and build dependencies.
    is_root = pid in root_set
    for dep in deps.get(pid, []):
        kinds = {kind.get("kind") for kind in dep.get("dep_kinds", [])} or {None}
        if is_root or kinds & {None, "build"}:
            stack.append(dep["pkg"])
print(len(seen))
PY
cat > "$WORK/binaries.py" <<'PY'
# How many distinct test executables the --no-run JSON stream produced.
import json, sys
seen = set()
for line in open(sys.argv[1]):
    try:
        msg = json.loads(line)
    except ValueError:
        continue
    exe = msg.get("executable")
    if exe and msg.get("profile", {}).get("test"):
        seen.add(exe)
print(len(seen))
PY
closure() { # <mode> — the closure helper over the resolved metadata
  python3 "$WORK/closure.py" "$1" "$WORK/metadata.json" "$workspace_flag" "$MANIFEST_PATH" \
    "${#packages[@]}" ${packages[@]+"${packages[@]}"} ${excludes[@]+"${excludes[@]}"}
}

cargo metadata --format-version 1 --filter-platform "$TRIPLE" \
    ${metadata_args[@]+"${metadata_args[@]}"} > "$WORK/metadata.json" 2>"$WORK/metadata.err" \
  || { cat "$WORK/metadata.err" >&2; die "cargo metadata failed; refusing to draw a guessed bar"; }
TOTAL_PACKAGES="$(closure count 2>>"$WORK/metadata.err")"
[[ "${TOTAL_PACKAGES:-0}" -gt 0 ]] 2>/dev/null || {
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

sum_counts() { # <passed|failed> <file> — libtest's own totals, from its
  # `test result:` lines only: a test that PRINTS "7 passed" is not a result.
  grep -hE '^test result: ' "$2" | grep -oE "[0-9]+ $1" | awk '{s+=$1} END{print s+0}'
}

passed=0; failed=0; TOTAL_BINS=0

if (( ! doc_only )); then
  printf '%scompiling%s (%s packages in the selection)\n' "$BOLD" "$RESET" "$TOTAL_PACKAGES"
  # `json-render-diagnostics`: artifacts stay on stdout as JSON for the bar,
  # and rustc's diagnostics are rendered to stderr as a person would read
  # them — plain `json` buries the actual error inside `compiler-message`
  # records.
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
  if (( no_run )); then
    # Cargo's --no-run: compile, but don't run tests. The compile IS this
    # script's first phase, so the request is honoured by stopping here.
    printf '%scompiled; --no-run given, nothing executed%s\n' "$GREEN" "$RESET"
    exit 0
  fi

  # -------------------------------------------------------------------------
  # Phase 2 — run, THROUGH CARGO. The denominator is the number of test
  # binaries the compile produced (plus the doctest targets cargo will
  # build on the fly, counted as one unit per package with a library), and
  # the numerator is the `Running …` / `Doc-tests …` lines cargo prints
  # before each one — cargo's own progress, read as it happens.
  # -------------------------------------------------------------------------
  TOTAL_BINS="$(python3 "$WORK/binaries.py" "$WORK/build.json")" || exit 2
  [[ "${TOTAL_BINS:-0}" -gt 0 ]] || {
    # A build that produced nothing runnable is not a passing suite: say
    # what happened and refuse to claim success.
    die "the build produced no test binaries (selection: ${cargo_args[*]})"
  }
fi

# Doctest units: one per selected package with a library target, unless a
# target selector (which skips doctests under cargo) was given.
DOC_UNITS=0
if (( ! target_selected )); then
  DOC_UNITS="$(closure libs)" || exit 2
fi
TOTAL_UNITS=$(( TOTAL_BINS + DOC_UNITS ))

printf '%srunning%s (%s test binaries, %s doctest targets, through cargo)\n' "$BOLD" "$RESET" "$TOTAL_BINS" "$DOC_UNITS"
ran=0; passed=0; failed=0; current="cargo test"
bar 0 "$TOTAL_UNITS" "$current"
# `--no-fail-fast`: every binary runs, so the summary names every failure
# rather than the first. Cargo's progress lines are on stderr; the tests'
# own output is interleaved, and everything is kept for the failure report.
cargo test ${cargo_args[@]+"${cargo_args[@]}"} --no-fail-fast -- ${bin_args[@]+"${bin_args[@]}"} \
  > "$WORK/run.log" 2>&1 &
RUN_PID=$!
while :; do
  alive=1; kill -0 "$RUN_PID" 2>/dev/null || alive=0
  # Re-read the whole log each tick: cheap, and it needs no cursor state.
  # POSIX classes, not \s: the sed macOS ships does not know \s, and a
  # label that kept its indentation would be the only symptom.
  ran="$(grep -cE '^[[:space:]]+(Running|Doc-tests) ' "$WORK/run.log" 2>/dev/null || true)"
  ran="${ran:-0}"
  current="$(grep -E '^[[:space:]]+(Running|Doc-tests) ' "$WORK/run.log" 2>/dev/null | tail -1 \
    | sed -E 's/^[[:space:]]+//; s/ \(.*\)$//')"
  passed="$(sum_counts passed "$WORK/run.log")"
  failed="$(sum_counts failed "$WORK/run.log")"
  bar "$ran" "$TOTAL_UNITS" "${current:-cargo test}  ${GREEN}${passed} passed${RESET}$( [[ "$failed" -gt 0 ]] && printf ' %s%s failed%s' "$RED" "$failed" "$RESET" )"
  (( alive )) || break
  sleep 0.3
done
wait "$RUN_PID"; RUN_RC=$?
printf '\n'

if [[ "$RUN_RC" -ne 0 ]]; then
  printf '\n%scargo test exited %d%s\n' "$RED" "$RUN_RC" "$RESET"
  # The failure list and the panics, not the whole passing roll-call; plus
  # cargo's own closing summary of which binaries failed.
  grep -E '^(test .*FAILED|thread .* panicked|assertion|---- .* stdout|error: |    [a-z_-]+ \(|failures:)' "$WORK/run.log" \
    | head -60
  printf '\n%s%d passed, %d failed%s\n' "$RED" "$passed" "$failed" "$RESET"
  exit "$RUN_RC"
fi

if (( doc_only )); then
  printf '%s%d doctests passed%s\n' "$GREEN" "$passed" "$RESET"
elif (( target_selected )); then
  printf '%s%d tests passed across %d binaries%s\n' "$GREEN" "$passed" "$TOTAL_BINS" "$RESET"
else
  printf '%s%d tests passed across %d binaries plus doctests%s\n' "$GREEN" "$passed" "$TOTAL_BINS" "$RESET"
fi
