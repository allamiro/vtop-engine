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
# Both denominators come from cargo itself (`cargo tree` for the packages
# the selection builds, under the same manifest, features, target and lock
# flags as the build; `cargo metadata` for the doctest targets; and the JSON
# message stream of `--no-run`), so the bar cannot drift from what is happening —
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
# first; a caller's own `--no-fail-fast` is absorbed, since cargo refuses
# the flag twice). `--no-run` compiles and stops; `--message-format` is
# refused, because the compile phase needs the JSON stream. The harness's
# thread count is cargo's own default unless TEST_THREADS is set.
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
past_sep=0; expect_value=""; workspace_flag=0; target_selected=0; doc_only=0; no_run=0; target_args=0
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
      --target) TARGET_ARG="$arg"; target_args=$(( target_args + 1 )) ;;
      # A --config override shapes resolution (sources, net, registries)
      # as much as it shapes the runner, so metadata sees it too.
      --config) config_overrides+=("$arg"); metadata_args+=("$expect_value" "$arg") ;;
    esac
    expect_value=""
    continue
  fi
  case "$arg" in
    -h|--help)
      # This script's usage is the header above; cargo's is cargo's. Neither
      # is served by compiling the workspace first.
      awk 'NR > 1 && !/^#/ { exit } NR > 1 { sub(/^# ?/, ""); print }' "$0"
      printf '\nFor cargo'"'"'s own options: cargo test --help\n'
      exit 0 ;;
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
      cargo_args+=("$arg"); TARGET_ARG="${arg#--target=}"; target_args=$(( target_args + 1 )) ;;
    --config=*)
      cargo_args+=("$arg"); config_overrides+=("${arg#--config=}"); metadata_args+=("$arg") ;;
    --workspace|--all)
      cargo_args+=("$arg"); workspace_flag=1 ;;
    --doc)
      # Kept for the run phase — it IS the selection — and remembered so
      # the compile phase, which cargo refuses beside --doc, is skipped.
      cargo_args+=("$arg"); doc_only=1 ;;
    --no-fail-fast)
      # The run phase adds this itself, and cargo refuses it twice.
      ;;
    -q|--quiet)
      die "quiet mode suppresses the Running/Doc-tests lines the bar is drawn from; run cargo test -q directly" ;;
    -F?*)
      cargo_args+=("$arg"); metadata_args+=("$arg") ;;
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
if (( target_args > 1 )); then
  # Cargo builds the selection once per --target; the bar models one graph
  # and would count one of them as the whole.
  die "--target was given $target_args times; this script draws one graph — pick one target per run"
fi
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
# The triple the graph is resolved for, in cargo's precedence: --target,
# then build.target from the config hierarchy — --config overrides FIRST
# (a command-line override outranks the environment under cargo), then
# CARGO_BUILD_TARGET, then .cargo/config[.toml] upward from here, then the
# home one (the extensionless file wins where both exist, as cargo warns it
# does) — then the host. A build.target that lists several triples is
# refused rather than guessed at: cargo builds the selection once per
# triple, and a bar drawn for one of them would count it as the whole. The
# BUILD needs none of this — cargo resolves its own target — but the
# denominator must describe the graph cargo builds.
build_target() {
  python3 - "$PWD" "${CARGO_HOME:-$HOME/.cargo}" "${CARGO_BUILD_TARGET:-}" \
    ${config_overrides[@]+"${config_overrides[@]}"} <<'PY'
import os, sys, tomllib
cwd, cargo_home, env_target, *overrides = sys.argv[1:]
def target_of(config, where):
    target = config.get("build", {}).get("target")
    if isinstance(target, list):
        if len(target) > 1:
            sys.exit("build.target in " + where + " names " + str(len(target))
                     + " triples; cargo builds the selection once per triple and this "
                     "bar draws one graph. Pass --target to choose one for this run.")
        target = target[0] if target else None
    return target
for override in reversed(overrides):
    try:
        config = tomllib.load(open(override, "rb")) if os.path.isfile(override) else tomllib.loads(override)
    except (OSError, tomllib.TOMLDecodeError):
        continue
    if target_of(config, "--config " + override):
        print(target_of(config, override)); sys.exit(0)
if env_target:
    print(env_target); sys.exit(0)
dirs = []
here = cwd
while True:
    dirs.append(here)
    parent = os.path.dirname(here)
    if parent == here:
        break
    here = parent
dirs.append(cargo_home)
for directory in dirs:
    for name in ("config", "config.toml"):
        path = os.path.join(directory, ".cargo" if directory != cargo_home else "", name)
        try:
            config = tomllib.load(open(path, "rb"))
        except (OSError, tomllib.TOMLDecodeError):
            continue
        if target_of(config, path):
            print(target_of(config, path)); sys.exit(0)
PY
}
if [[ -n "$TARGET_ARG" ]]; then
  TRIPLE="$TARGET_ARG"
else
  # tomllib arrived in Python 3.11; an older python3 passes the tool check
  # above and fails inside the lookup with a traceback that names neither
  # the version nor the way around it.
  python3 -c 'import tomllib' 2>/dev/null \
    || die "reading cargo's config for build.target needs Python 3.11 or newer (tomllib); \
python3 here is $(python3 -V 2>&1). Upgrade, or pass --target to skip the lookup"
  TRIPLE="$(build_target)" || die "could not settle the build target"
fi
TRIPLE="${TRIPLE:-$HOST}"

# ---------------------------------------------------------------------------
# Phase 1 — compile. Numerator and denominator are the same unit — packages —
# so the bar cannot overrun: cargo emits one `compiler-artifact` per TARGET
# (a library, its test harness, each integration test), so counting messages
# would count one package several times. The denominator is what `cargo
# tree` resolves for the SELECTION — the workspace's default members (minus
# exclusions), or the packages named with -p — under the same manifest,
# features, target and lock flags. Not the `cargo metadata` graph: that one
# resolves features WORKSPACE-WIDE, so an unselected member enabling a
# feature on a shared dependency adds that dependency's optional deps to the
# graph though `cargo test -p a` never builds them (review). Measured against
# the artifacts the build actually produced, the metadata closure overcounted
# on every probe and `cargo tree` matched to the package on every one.
# ---------------------------------------------------------------------------
# The helper scripts are written to files ONCE and invoked by path: a here-
# document nested inside a command substitution is parsed differently by the
# bash 3.2 macOS ships (quotes and brackets in the body can end the
# substitution early), and a script that runs on one bash and not another
# is worse than one that never used the construct.
cat > "$WORK/closure.py" <<'PY'
# The selected roots over cargo's metadata: `libs` prints how many of them
# have a library target with doctests on (the doctest units cargo will run).
# The package COUNT is cargo tree's, not this graph's — see the phase-1 note.
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

def version_matches(wanted, version):
    # A pkgid spec's version may be abbreviated: `0`, `0.1`, or `0.1.2` all
    # name 0.1.2 (review).
    return version == wanted or version.startswith(wanted + ".")

def source_of(pkgid):
    # "path+file:///x#name@1.0" -> "file:///x"; the kind prefix is optional in
    # a spec and the fragment is not part of the source.
    source = pkgid.split("#", 1)[0]
    return source.split("+", 1)[1] if "+" in source.split("://", 1)[0] else source

def named(spec, package):
    # -p accepts a name (with the usual glob patterns), name@version or
    # name:version (version possibly abbreviated), a path, or a fully
    # qualified pkgid (`path+file:///x#0.1.0`, `file:///x#name@0.1.0`);
    # match the forms a person types (review).
    name = package["name"]
    if "#" in spec:
        source, fragment = spec.rsplit("#", 1)
        if source_of(source).rstrip("/") != source_of(package["id"]).rstrip("/"):
            return False
        if "@" in fragment:
            spec_name, wanted = fragment.split("@", 1)
            return spec_name == name and version_matches(wanted, package["version"])
        return version_matches(fragment, package["version"])
    if spec == name:
        return True
    for separator in ("@", ":"):
        if separator in spec:
            spec_name, wanted = spec.rsplit(separator, 1)
            if spec_name == name and version_matches(wanted, package["version"]):
                return True
    if spec.rstrip("/") == package["manifest_path"].rsplit("/", 1)[0]:
        return True
    return any(ch in spec for ch in "*?[") and fnmatch.fnmatchcase(name, spec)

if selected:
    matched = []
    for spec in selected:
        matches = [pid for pid in members if named(spec, by_id[pid])]
        if not matches:
            sys.exit("package " + spec + " is not a member of this workspace")
        matched.extend(matches)
    # `--workspace -p a` selects the WORKSPACE under cargo: the -p is
    # checked for existence and otherwise ignored, so it is here too.
    roots = list(members) if workspace_flag else matched
else:
    roots = list(members if workspace_flag else default_members)
roots = [pid for pid in roots if not any(named(spec, by_id[pid]) for spec in excluded)]
if not roots:
    sys.exit("the selection excludes every package")

if mode == "libs":
    # A library with `doctest = false` produces no Doc-tests unit under
    # cargo, so it must not be counted as one here. A proc-macro crate IS
    # a library for this purpose: cargo runs its doctests too, under a
    # target kind that names neither lib nor rlib.
    print(sum(1 for pid in roots if any(
        any(kind in target["kind"] for kind in ("lib", "rlib", "proc-macro"))
        and target.get("doctest", True)
        for target in by_id[pid]["targets"])))
    sys.exit(0)
sys.exit("unknown mode " + mode)
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
# The selection, spelled the way cargo test resolved it: `--workspace`
# (with exclusions) when that is in force, else the named packages; a
# manifest path alone rides in with the metadata arguments and selects what
# it selects under cargo. Dev-dependencies ride along for the workspace
# members shown, which is what the test build compiles.
selection=()
if (( workspace_flag )); then
  selection+=(--workspace)
  for excluded in ${excludes[@]+"${excludes[@]}"}; do
    selection+=(--exclude "$excluded")
  done
else
  for package in ${packages[@]+"${packages[@]}"}; do
    selection+=(-p "$package")
  done
fi
TOTAL_PACKAGES="$(cargo tree ${selection[@]+"${selection[@]}"} \
    ${metadata_args[@]+"${metadata_args[@]}"} \
    --target "$TRIPLE" --edges normal,build,dev --prefix none --format '{p}' 2>"$WORK/tree.err" \
  | sed 's/ (\*)$//' | sort -u | grep -c .)"
[[ "${TOTAL_PACKAGES:-0}" -gt 0 ]] 2>/dev/null || {
  cat "$WORK/tree.err" >&2
  die "cargo tree did not yield a package count; refusing to draw a guessed bar"
}

built_packages() {
  # Distinct packages with at least one COMPILED artifact so far — a build
  # script's executable is not one (review): it is emitted before the
  # script has run, and a package with a slow build script would otherwise
  # read as built for the whole of it. A dependency has exactly one
  # artifact, so this is exact for all but the selected roots, whose extra
  # test targets can complete a little after the package first counts. A
  # missing file (the first poll on a cold build) is zero, not an error.
  [[ -f "$WORK/build.json" ]] || { echo 0; return; }
  grep '"reason":"compiler-artifact"' "$WORK/build.json" 2>/dev/null \
    | grep -v '"kind":\["custom-build"\]' \
    | grep -oE '"package_id":"[^"]*"' | sort -u | wc -l | tr -d ' '
}

# Cargo colours its status lines whenever `--color always` or
# CARGO_TERM_COLOR=always says so — CI does — and a coloured `Running` line
# carries escape sequences before its indentation, which the anchored
# patterns below would never match. Every read of the log goes through
# this. A literal escape byte, not `\e`: the sed macOS ships knows neither
# that nor `\x1b`.
ESC="$(printf '\033')"
plain_log() { # <file> — the log with its escape sequences removed
  [[ -f "$1" ]] || return 0
  sed "s/${ESC}\[[0-9;]*[A-Za-z]//g" "$1"
}
# rustc's rendered diagnostics from a log: each block from a `warning:` or
# `error:` line to the blank line that closes it. The bar hides the log the
# warnings were printed into, and a warning that vanishes because a test
# run succeeded is a warning nobody fixes.
diagnostics() { # <file>
  plain_log "$1" | awk '
    /^(warning|error)(\[[^]]*\])?: / { block = 1 }
    /^ *(Compiling|Checking|Finished|Running|Doc-tests) / { block = 0 }
    block { print }
    block && /^$/ { block = 0 }'
}
sum_counts() { # <passed|failed> <file> — libtest's own totals, from its
  # `test result:` lines only: a test that PRINTS "7 passed" is not a result.
  plain_log "$2" | grep -hE '^test result: ' | grep -oE "[0-9]+ $1" | awk '{s+=$1} END{print s+0}'
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
  # `>/dev/null`, not `-q`: under pipefail a grep that stops reading at the
  # first match hands sed a broken pipe and the condition reads false.
  if plain_log "$WORK/build.err" | grep -E '^warning(\[[^]]*\])?: ' >/dev/null; then
    printf '%swarnings from the compile%s\n' "$BOLD" "$RESET"
    diagnostics "$WORK/build.err"
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
  # the numerator is the `test result:` lines libtest prints as each one
  # finishes — cargo's own progress, read as it happens; the `Running …` /
  # `Doc-tests …` line names the unit under way.
  # -------------------------------------------------------------------------
  TOTAL_BINS="$(python3 "$WORK/binaries.py" "$WORK/build.json")" || exit 2
fi

# Doctest units: one per selected package with a library target, unless a
# target selector (which skips doctests under cargo) was given.
DOC_UNITS=0
if (( ! target_selected )); then
  DOC_UNITS="$(closure libs)" || exit 2
fi
TOTAL_UNITS=$(( TOTAL_BINS + DOC_UNITS ))
if (( ! doc_only && TOTAL_UNITS == 0 )); then
  # Nothing runnable AND no doctest target is not a passing suite: say so
  # and refuse to claim success. Zero binaries ALONE is not fatal — a library
  # with `test = false` and doctests still on builds no executable, and its
  # doctests are compiled during the run (review).
  die "the build produced no test binaries and the selection has no doctest targets (selection: ${cargo_args[*]})"
fi

printf '%srunning%s (%s test binaries, %s doctest targets, through cargo)\n' "$BOLD" "$RESET" "$TOTAL_BINS" "$DOC_UNITS"
ran=0; passed=0; failed=0; current="cargo test"
# A caller who asked for the tests' own output (--nocapture in either
# spelling, --show-output) or for the harness's (--list, --help) gets it
# streamed as cargo prints it; a bar over a log nobody sees would hide the
# very thing they asked for. The log is still kept for the counts.
stream=0
case " ${bin_args[*]+"${bin_args[*]}"} " in
  *" --nocapture "*|*" --no-capture "*|*" --show-output "*|*" --list "*|*" --help "*|*" -h "*) stream=1 ;;
esac
if (( stream )); then
  cargo test ${cargo_args[@]+"${cargo_args[@]}"} --no-fail-fast -- ${bin_args[@]+"${bin_args[@]}"} 2>&1 \
    | tee "$WORK/run.log"
  RUN_RC="${PIPESTATUS[0]}"
  passed="$(sum_counts passed "$WORK/run.log")"
  failed="$(sum_counts failed "$WORK/run.log")"
  if [[ "$RUN_RC" -ne 0 ]]; then
    printf '\n%s%d passed, %d failed (cargo test exited %d)%s\n' "$RED" "$passed" "$failed" "$RUN_RC" "$RESET"
    exit "$RUN_RC"
  fi
  printf '%s%d tests passed (output streamed as requested)%s\n' "$GREEN" "$passed" "$RESET"
  exit 0
fi
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
  # FINISHED units, not started ones (review): `Running` precedes a binary,
  # `test result:` closes it (and closes each doctest unit), so a single
  # slow binary reads as in progress rather than complete.
  ran="$(plain_log "$WORK/run.log" | grep -cE '^test result: ' || true)"
  ran="${ran:-0}"
  current="$(plain_log "$WORK/run.log" | grep -E '^[[:space:]]+(Running|Doc-tests) ' | tail -1 \
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
  # The failures, not the passing roll-call, and every one of them — the
  # whole point of --no-fail-fast is that the summary is complete.
  # Every `failures:` section libtest prints — the captured output of each
  # failed test, panic message and assertion bodies included, then the list
  # — up to that binary's `test result:` line; plus rustc's rendered blocks,
  # which is where a doctest that failed to compile says why (review).
  plain_log "$WORK/run.log" | awk '
    /^failures:$/ { show = 1 }
    show { print }
    /^test result: / { show = 0 }'
  diagnostics "$WORK/run.log"
  printf '\n%s%d passed, %d failed%s\n' "$RED" "$passed" "$failed" "$RESET"
  exit "$RUN_RC"
fi

# Doctests compile during the run, so their warnings land in the run log.
if plain_log "$WORK/run.log" | grep -E '^warning(\[[^]]*\])?: ' >/dev/null; then
  printf '%swarnings from the run%s\n' "$BOLD" "$RESET"
  diagnostics "$WORK/run.log"
fi
if (( doc_only )); then
  printf '%s%d doctests passed%s\n' "$GREEN" "$passed" "$RESET"
elif (( target_selected )); then
  printf '%s%d tests passed across %d binaries%s\n' "$GREEN" "$passed" "$TOTAL_BINS" "$RESET"
else
  printf '%s%d tests passed across %d binaries plus doctests%s\n' "$GREEN" "$passed" "$TOTAL_BINS" "$RESET"
fi
