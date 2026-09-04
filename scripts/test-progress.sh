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
# Usage:
#   scripts/test-progress.sh                       # whole workspace
#   scripts/test-progress.sh -p vtop-broker        # cargo's package selection
#   scripts/test-progress.sh --exclude vtop-kafka  # the workspace minus one
#   scripts/test-progress.sh -- --ignored          # everything after `--` goes
#                                                  # to every test binary
#   scripts/test-progress.sh lease -- --nocapture  # a test-name filter, too
#
# Arguments split exactly as `cargo test` splits them: options before `--`
# are cargo's (package selection, features, target, lock flags), a bare
# word before `--` is a test-name filter, and everything after `--` is the
# harness's. A target selector (`--lib`, `--test NAME`, `--bin NAME`, …)
# narrows the run as it does under cargo and, as under cargo, skips the
# doctests; `--doc` runs only them, and is refused beside a selector as
# cargo refuses it; `--no-run` compiles and stops. A `--manifest-path`
# naming a workspace member selects that member, as it does under cargo.
# The harness's thread count is cargo's own default unless TEST_THREADS is
# set.
#
# Each binary runs from its package's root directory, as cargo runs it, and
# through cargo's configured `target.<triple>.runner` when one is set — read
# from the sources cargo reads, in cargo's precedence: `--config` overrides,
# the environment, the project's `.cargo/config.toml` files upward from the
# working directory, then the home one. The triple follows the same
# precedence (`--target`, `CARGO_BUILD_TARGET`, `build.target`, the host).
# A runner under a `[target.'cfg(...)']` table cannot be evaluated here and
# is refused rather than silently bypassed.
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
    -p|--package|--exclude|--features|-F|--target|--target-dir|--manifest-path|\
    --profile|-j|--jobs|--test|--bin|--example|--bench|--color|--config|-Z|\
    --message-format)
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

# ---------------------------------------------------------------------------
# Cargo's configuration, read the way cargo reads it. Cargo's own resolver
# for this (`cargo config get`) is unstable, so the same sources are read
# in the same precedence: `--config` overrides (a KEY=VALUE snippet or a
# file path), the environment, every `.cargo/config[.toml]` from the working
# directory upward, then the home one. Resolves the target triple
# (`--target`, `CARGO_BUILD_TARGET`, `build.target`, host) and that
# triple's runner, printed one field per line: `target=<triple>`, then
# `runner=<argv element>` lines, then `cfg_runner=1` if any `[target.'cfg(…)']`
# table names a runner — which this script cannot evaluate and refuses.
# ---------------------------------------------------------------------------
HOST="$(rustc -vV | sed -n 's/^host: //p')"
[[ -n "$HOST" ]] || die "could not determine the host triple from rustc"
cargo_config() {
  python3 - "$HOST" "$TARGET_ARG" "$PWD" "${CARGO_HOME:-$HOME/.cargo}" \
    ${config_overrides[@]+"${config_overrides[@]}"} <<'PY'
import os, shlex, sys, tomllib

host, target_arg, cwd, cargo_home, *overrides = sys.argv[1:]

def merge(base, extra):
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merge(base[key], value)
        elif key not in base:  # higher precedence already set it
            base[key] = value

def load_file(path):
    try:
        with open(path, "rb") as handle:
            return tomllib.load(handle)
    except FileNotFoundError:
        return {}

config = {}
# 1. --config overrides, in order (later ones win, so merge in reverse).
for override in reversed(overrides):
    if os.path.isfile(override):
        merge(config, load_file(override))
    else:
        merge(config, tomllib.loads(override))
# 2. The environment, for the two keys this script needs.
if os.environ.get("CARGO_BUILD_TARGET"):
    merge(config, {"build": {"target": os.environ["CARGO_BUILD_TARGET"]}})
# 3. Project config files from cwd upward, then 4. the home directory's.
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
    merge(config, load_file(path))

target = target_arg or config.get("build", {}).get("target") or host
if isinstance(target, list):
    target = target[0] if len(target) == 1 else host
print(f"target={target}")

env_runner = os.environ.get("CARGO_TARGET_" + target.upper().replace("-", "_") + "_RUNNER")
tables = config.get("target", {})
runner = None
# --config overrides and files both landed in `config`; the environment
# sits between them in cargo's precedence, so only a --config override
# outranks it.
# Later overrides win, as they do for cargo.
for override in reversed(overrides):
    snippet = load_file(override) if os.path.isfile(override) else tomllib.loads(override)
    candidate = snippet.get("target", {}).get(target, {}).get("runner")
    if candidate is not None:
        runner = candidate
        break
if runner is None and env_runner:
    runner = env_runner
if runner is None:
    runner = tables.get(target, {}).get("runner")
if isinstance(runner, str):
    runner = shlex.split(runner)
for part in runner or []:
    print(f"runner={part}")
if any(key.startswith("cfg(") and "runner" in value for key, value in tables.items() if isinstance(value, dict)):
    print("cfg_runner=1")
PY
}
TRIPLE=""; RUNNER=(); CFG_RUNNER=0
while IFS= read -r line; do
  case "$line" in
    target=*) TRIPLE="${line#target=}" ;;
    runner=*) RUNNER+=("${line#runner=}") ;;
    cfg_runner=1) CFG_RUNNER=1 ;;
  esac
done < <(cargo_config)
[[ -n "$TRIPLE" ]] || die "could not resolve the build target from cargo's configuration"
if (( CFG_RUNNER )); then
  die "a runner is configured under a [target.'cfg(...)'] table, which this script cannot evaluate; run cargo test directly"
fi

# ---------------------------------------------------------------------------
# Phase 1 — compile. Numerator and denominator are the same unit — packages —
# so the bar cannot overrun: cargo emits one `compiler-artifact` per TARGET
# (a library, its test harness, each integration test), so counting messages
# would count one package several times. The denominator is the dependency
# closure of the SELECTED packages in the resolve graph cargo builds for the
# same manifest, features, target and lock flags: the workspace's default
# members (minus exclusions), or the packages named with -p.
# ---------------------------------------------------------------------------
cargo metadata --format-version 1 --filter-platform "$TRIPLE" \
    ${metadata_args[@]+"${metadata_args[@]}"} > "$WORK/metadata.json" 2>"$WORK/metadata.err" \
  || { cat "$WORK/metadata.err" >&2; die "cargo metadata failed; refusing to draw a guessed bar"; }
TOTAL_PACKAGES="$(python3 - "$WORK/metadata.json" "$workspace_flag" "$MANIFEST_PATH" "${#packages[@]}" \
    ${packages[@]+"${packages[@]}"} ${excludes[@]+"${excludes[@]}"} 2>>"$WORK/metadata.err" <<'PY'
import json, os, sys
with open(sys.argv.pop(1)) as handle:
    meta = json.load(handle)
workspace_flag = sys.argv[1] == "1"
manifest_path = sys.argv[2]
count = int(sys.argv[3])
selected, excluded = sys.argv[4:4 + count], sys.argv[4 + count:]
by_id = {package["id"]: package for package in meta["packages"]}
members = meta["workspace_members"]
default_members = meta.get("workspace_default_members") or members
if manifest_path and not workspace_flag and not selected:
    # Cargo's default for an explicit manifest is the package it names.
    wanted = os.path.realpath(manifest_path)
    named = [pid for pid in members if os.path.realpath(by_id[pid]["manifest_path"]) == wanted]
    if named:
        default_members = named

def named(spec, package):
    # `-p` accepts a name, name@version, or a path; match the forms a person types.
    name = package["name"]
    return spec in (name, f"{name}@{package['version']}", f"{name}:{package['version']}") \
        or spec.rstrip("/") == package["manifest_path"].rsplit("/", 1)[0]

if selected:
    roots = []
    for spec in selected:
        matches = [pid for pid in members if named(spec, by_id[pid])]
        if not matches:
            sys.exit(f"package `{spec}` is not a member of this workspace")
        roots.extend(matches)
else:
    roots = list(members if workspace_flag else default_members)
roots = [pid for pid in roots if not any(named(spec, by_id[pid]) for spec in excluded)]
if not roots:
    sys.exit("the selection excludes every package")

deps = {node["id"]: node["deps"] for node in meta["resolve"]["nodes"]}
root_set = set(roots)
seen = set()
stack = list(roots)
while stack:
    pid = stack.pop()
    if pid in seen:
        continue
    seen.add(pid)
    # A root builds its dev-dependencies too — whether it was reached as a
    # root or first met as another root's dependency — while a dependency
    # builds only its normal and build dependencies.
    is_root = pid in root_set
    for dep in deps.get(pid, []):
        kinds = {kind.get("kind") for kind in dep.get("dep_kinds", [])} or {None}
        if is_root or kinds & {None, "build"}:
            stack.append(dep["pkg"])
print(len(seen))
PY
)"
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

passed=0; failed=0; failing_bins=(); TOTAL_BINS=0

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
  # Phase 2 — run. Each test binary is one unit, which is the granularity a
  # person actually waits on: "12 of 19 binaries" answers "how much longer".
  # Each line is `<executable>\t<package root>`: cargo runs every test from
  # its package's root directory, and so does this.
  # -------------------------------------------------------------------------
  python3 - "$WORK/build.json" > "$WORK/binaries" <<'PY' || exit 2
import json, os, sys
seen = []
for line in open(sys.argv[1]):
    try:
        msg = json.loads(line)
    except ValueError:
        continue
    # `executable` is non-null exactly for the artifacts cargo would run.
    exe = msg.get("executable")
    if exe and msg.get("profile", {}).get("test") and exe not in [e for e, _ in seen]:
        seen.append((exe, os.path.dirname(msg["manifest_path"])))
print("\n".join(f"{exe}\t{root}" for exe, root in seen))
PY

  TOTAL_BINS="$(grep -c . "$WORK/binaries" || true)"
  [[ "${TOTAL_BINS:-0}" -gt 0 ]] || {
    # A build that produced nothing runnable is not a passing suite: cargo
    # itself reports "running 0 tests" and exits 0 only when the selection
    # legitimately has none, and we cannot tell that apart from a broken
    # parse here — so say what happened and refuse to claim success.
    die "the build produced no test binaries (selection: ${cargo_args[*]})"
  }

  printf '%srunning%s (%s test binaries' "$BOLD" "$RESET" "$TOTAL_BINS"
  [[ "${#RUNNER[@]}" -gt 0 ]] && printf ' via runner %s' "${RUNNER[*]}"
  printf ')\n'
  ran=0
  while IFS=$'\t' read -r exe root; do
    name="$(basename "$exe" | sed 's/-[0-9a-f]\{16\}$//')"
    bar "$ran" "$TOTAL_BINS" "$name"
    # The `[@]+` expansions keep an empty array from tripping `set -u` on
    # the bash 3.2 macOS ships.
    (cd "$root" && ${RUNNER[@]+"${RUNNER[@]}"} "$exe" ${bin_args[@]+"${bin_args[@]}"}) \
      > "$WORK/out.$ran" 2>&1
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
fi

# ---------------------------------------------------------------------------
# Phase 3 — doctests. `cargo test` runs them and this split flow would not:
# `--no-run` cannot build them (rustdoc compiles them on the fly) and they
# produce no binary to run. One unit, drawn while cargo runs them, so a
# failing doctest fails the run exactly as it would under `cargo test`.
# Skipped when the caller selected targets, as cargo skips them — and
# cargo refuses `--doc` beside a target selector, so this must too.
# ---------------------------------------------------------------------------
if (( target_selected )); then
  printf '%sdoctests%s skipped: a target selector was given, as under cargo test\n' "$BOLD" "$RESET"
else
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
fi

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

if (( doc_only )); then
  printf '%s%d doctests passed%s\n' "$GREEN" "$passed" "$RESET"
elif (( target_selected )); then
  printf '%s%d tests passed across %d binaries%s\n' "$GREEN" "$passed" "$TOTAL_BINS" "$RESET"
else
  printf '%s%d tests passed across %d binaries plus doctests%s\n' "$GREEN" "$passed" "$TOTAL_BINS" "$RESET"
fi
