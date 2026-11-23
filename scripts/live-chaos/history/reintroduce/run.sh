#!/usr/bin/env bash
# Re-introduce a fixed data-safety bug into a scratch build and show the
# history checkers catch it (#728, criterion A4 of #714).
#
# The checkers' unit tests feed them hand-built histories shaped like each
# bug. That shows the rule is right, not that a real run with the bug breaks
# it. This runner does the second: it copies the tree, applies one patch from
# this directory that undoes the bug's fix, and runs that build, judging what
# it recorded with THIS tree's checkers. Two modes:
#
#   live   (the default) builds vtop-node and vtopctl from the copy and runs
#          the scenarios that reach the bug's shape against that build.
#          After every run, pass or fail, it runs check_history.py and
#          check_control.py (with --stall-ms) over what the run recorded, so
#          a scenario that stops at its own assertion is still judged by the
#          histories. Live processes reach the shapes only by luck of timing.
#   --sweep  runs, in the copy, the tests that reach the shapes by
#          construction, with VTOP_SWEEP_HISTORY_DIR set so each writes the
#          client history of its runs: the promotion crash sweep
#          (crates/vtop-node/src/promotion_crash_sweep.rs: the real replica
#          code on simulated disks, a crash at every storage operation) and
#          the loopback test of a returning deposed leader
#          (crates/vtop-broker/tests/networked_replication.rs: the real
#          networked replica set over local TLS). Then check_history.py
#          judges every history written. The tests failing on the patched
#          build is expected and is not the verdict; the checker's is.
#
#   611  a fence discards a suffix the candidate merely lacks (undoes #616)
#   641  a returning deposed leader's sessions open unfenced (undoes #649)
#   261  a diverged replica is not reconciled at the fence, so it acks the
#        new leader's records over its own (undoes #263)
#   640  a follower files caught-up records under its own adopted epoch
#        (undoes #648)
#
# The patches never ship: they live here, are applied to a copy, and the
# build they make runs only under this script.
#
# Usage: run.sh <611|641|261|640> [runs]
#        run.sh --sweep <611|641|261|640>
#
# Environment:
#   REINTRODUCE_OUT         where the copy, the build and the runs go
#                           (default: a new temp directory)
#   REINTRODUCE_TARGET_DIR  CARGO_TARGET_DIR for the patched build
#                           (default: <out>/target)
#   REINTRODUCE_SEEDS       space-separated seeds, one per run (default: random)
#   REINTRODUCE_SCENARIOS   override the scenarios run for the bug
#   REINTRODUCE_STALL_MS    the control checker's --stall-ms (default 15000):
#                           a run whose history ends with this long granted
#                           and unserved is flagged `stalled`, which is how a
#                           bug that stops the cluster (#640) shows
#   VTOP_SWEEP_HISTORY_FAILING  (--sweep) failing runs each swept shape
#                           writes a history for (default 5)
#
# Exit status: 0 the checkers flagged at least one run or history
# (DETECTED); 1 none was flagged (MISSED: the summary says what each did);
# 2 the patch, the copy or the build failed, or (--sweep) the tests wrote no
# history or the checker could not read one.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../../../.." && pwd)"
CHAOS="$REPO_ROOT/scripts/live-chaos"

MODE=live
if [[ "${1:-}" == --sweep ]]; then
  MODE=sweep
  shift
fi
BUG="${1:-}"
RUNS="${2:-3}"
case "$BUG" in
  611 | 641 | 261) DEFAULT_SCENARIOS="23-acked-suffix-failover" ;;
  640) DEFAULT_SCENARIOS="14-candidate-failover 23-acked-suffix-failover" ;;
  *)
    echo "usage: $0 <611|641|261|640> [runs] | $0 --sweep <611|641|261|640>" >&2
    exit 2
    ;;
esac
if [[ "$MODE" == sweep && $# -gt 1 ]]; then
  echo "--sweep takes no run count: the sweep is deterministic" >&2
  exit 2
fi
[[ "$RUNS" =~ ^[1-9][0-9]*$ ]] || { echo "runs must be a positive integer, got '$RUNS'" >&2; exit 2; }
read -r -a SCENARIOS <<< "${REINTRODUCE_SCENARIOS:-$DEFAULT_SCENARIOS}"
read -r -a SEEDS <<< "${REINTRODUCE_SEEDS:-}"
PATCH="$HERE/$BUG.patch"
STALL_MS="${REINTRODUCE_STALL_MS:-15000}"
[[ "$STALL_MS" =~ ^[0-9]+$ ]] || { echo "REINTRODUCE_STALL_MS must be milliseconds, got '$STALL_MS'" >&2; exit 2; }

OUT="${REINTRODUCE_OUT:-$(mktemp -d "${TMPDIR:-/tmp}/vtop-reintroduce-$BUG.XXXXXX")}"
mkdir -p "$OUT"
SRC="$OUT/src"
TARGET="${REINTRODUCE_TARGET_DIR:-$OUT/target}"
SUMMARY="$OUT/summary.jsonl"
: > "$SUMMARY"

log() { echo "[reintroduce/$BUG] $*"; }
setup_fail() { echo "[reintroduce/$BUG] SETUP FAILED: $*" >&2; exit 2; }

# --- the copy: every tracked and untracked-but-not-ignored file --------------
rm -rf "$SRC"
mkdir -p "$SRC"
# Without git (a worktree mounted into a container whose .git points
# elsewhere), everything but the build output and the repository metadata.
if git -C "$REPO_ROOT" rev-parse --git-dir > /dev/null 2>&1; then
  (cd "$REPO_ROOT" && git ls-files --cached --others --exclude-standard -z \
    | while IFS= read -r -d '' f; do if [[ -e "$f" ]]; then printf '%s\0' "$f"; fi; done \
    | tar --null -T - -cf -) | tar -xf - -C "$SRC" \
    || setup_fail "could not copy the tree"
else
  (cd "$REPO_ROOT" && tar --exclude=./target --exclude=./.git --exclude=./site -cf - .) \
    | tar -xf - -C "$SRC" || setup_fail "could not copy the tree"
fi
# patch(1), not git apply: the copy is no repository, and inside another one
# git apply would read paths against that repository's root.
(cd "$SRC" && patch -p1 --batch --forward --dry-run < "$PATCH" > /dev/null \
  && patch -p1 --batch --forward < "$PATCH" > /dev/null) \
  || setup_fail "$PATCH no longer applies: the code it undoes has moved; refresh the patch"
log "patch applied to $SRC"

# --- --sweep: the deterministic tests, judged by their client histories -----
if [[ "$MODE" == sweep ]]; then
  HISTORIES="$OUT/histories"
  rm -rf "$HISTORIES"
  mkdir -p "$HISTORIES"
  # The check each bug's shape predicts, printed beside what was found. The
  # verdict is any violation; the prediction says whether it is the right one.
  case "$BUG" in
    611) PREDICTED="lost (acknowledged records truncated at the fence)" ;;
    261) PREDICTED="rewritten or lost (a diverged replica acknowledged records it does not hold)" ;;
    640) PREDICTED="unavailable (no candidate established after a second failover)" ;;
    641) PREDICTED="rewritten (a deposed leader's records under the new leader's mark)" ;;
  esac
  # Built first, so a patch that breaks the build is exit 2 and never
  # mistaken for a test failing on the bug.
  (cd "$SRC" && CARGO_TARGET_DIR="$TARGET" cargo test -p vtop-node --bin vtop-node --no-run \
    && CARGO_TARGET_DIR="$TARGET" cargo test -p vtop-broker --test networked_replication --no-run) \
    > "$OUT/build.log" 2>&1 || setup_fail "the patched test build failed; see $OUT/build.log"
  log "built the tests with #$BUG re-introduced"
  for target in "vtop-node --bin vtop-node promotion_crash_sweep" \
    "vtop-broker --test networked_replication deposed_leader"; do
    read -r -a args <<< "$target"
    started=$SECONDS
    code=0
    (cd "$SRC" && CARGO_TARGET_DIR="$TARGET" VTOP_SWEEP_HISTORY_DIR="$HISTORIES" \
      cargo test -p "${args[@]}" -- --nocapture) >> "$OUT/tests.log" 2>&1 || code=$?
    log "cargo test -p $target: exit $code ($((SECONDS - started))s; a failure is expected with the bug)"
  done
  files=("$HISTORIES"/*.jsonl)
  [[ -e "${files[0]}" ]] || setup_fail "the patched tests wrote no history; see $OUT/tests.log"
  flagged=0
  errors=0
  for file in "${files[@]}"; do
    name="$(basename "$file" .jsonl)"
    code=0
    python3 "$CHAOS/history/check_history.py" "$file" > "$HISTORIES/$name.verdict.json" 2>&1 || code=$?
    case "$code" in 0) verdict=valid ;; 1) verdict=VIOLATED ;; *) verdict=error ;; esac
    kinds="$(python3 -c 'import json, sys
try:
    print(",".join(sorted(json.load(open(sys.argv[1]))["violations"])) or "-")
except (OSError, ValueError, KeyError):
    print("-")' "$HISTORIES/$name.verdict.json")"
    log "history $name: $verdict, violations $kinds"
    printf '{"bug":%s,"mode":"sweep","history":"%s","verdict":"%s","violations":"%s"}\n' \
      "$BUG" "$name" "$verdict" "$kinds" >> "$SUMMARY"
    [[ "$verdict" == VIOLATED ]] && flagged=$((flagged + 1))
    [[ "$verdict" == error ]] && errors=$((errors + 1))
  done
  log "the shape of #$BUG predicts: $PREDICTED"
  ((errors == 0)) || setup_fail "check_history.py could not read $errors history file(s); see $HISTORIES/*.verdict.json"
  if ((flagged > 0)); then
    log "DETECTED: check_history.py flagged $flagged of ${#files[@]} history file(s) with #$BUG re-introduced; see $SUMMARY"
    exit 0
  fi
  log "MISSED: none of the ${#files[@]} history file(s) with #$BUG re-introduced broke a history rule; see $SUMMARY"
  exit 1
fi

# --- the patched build -------------------------------------------------------
(cd "$SRC" && CARGO_TARGET_DIR="$TARGET" cargo build -p vtop-node --bin vtop-node \
  && CARGO_TARGET_DIR="$TARGET" cargo build -p vtop-cli --no-default-features --bin vtopctl) \
  > "$OUT/build.log" 2>&1 || setup_fail "the patched build failed; see $OUT/build.log"
NODE_BIN="$TARGET/debug/vtop-node"
CTL_BIN="$TARGET/debug/vtopctl"
log "built $NODE_BIN with #$BUG re-introduced"

# --- the runs ----------------------------------------------------------------
judge() { # <workdir> — prints "<data verdict> <control verdict> <kinds>"
  local wd="$1" data=none control=none kinds="" code
  local files=("$wd"/history/*.jsonl) cfiles=("$wd"/history/control/*.jsonl)
  if [[ -e "${files[0]}" ]]; then
    code=0
    python3 "$CHAOS/history/check_history.py" "${files[@]}" > "$wd/reintroduce-history.json" 2>&1 || code=$?
    case "$code" in 0) data=valid ;; 1) data=VIOLATED ;; *) data=error ;; esac
  fi
  if [[ -e "${cfiles[0]}" ]]; then
    code=0
    python3 "$CHAOS/history/check_control.py" --stall-ms "$STALL_MS" "${cfiles[@]}" \
      > "$wd/reintroduce-control.json" 2>&1 || code=$?
    case "$code" in 0) control=valid ;; 1) control=VIOLATED ;; *) control=error ;; esac
  fi
  kinds="$(python3 - "$wd" << 'PY'
import json, pathlib, sys
wd = pathlib.Path(sys.argv[1])
kinds = []
for name in ("reintroduce-history.json", "reintroduce-control.json"):
    try:
        kinds += sorted(json.loads((wd / name).read_text()).get("violations", {}))
    except (OSError, ValueError):
        pass
print(",".join(kinds) or "-")
PY
)"
  # A history with no completed read (the scenario stopped before it read)
  # is reported as an `input` violation; it judged nothing.
  [[ "$data" == VIOLATED && "$kinds" == input* && "$kinds" != *,* ]] && data=noread
  echo "$data $control $kinds"
}

detected=0
for ((i = 1; i <= RUNS; i++)); do
  seed="${SEEDS[$((i - 1))]:-$((RANDOM * 32768 + RANDOM))}"
  for scenario in "${SCENARIOS[@]}"; do
    wd="$OUT/run-$i-$scenario"
    rm -rf "$wd"
    started=$SECONDS
    code=0
    CHAOS_KEEP=1 CHAOS_WORKDIR="$wd" CHAOS_PROFILE=debug \
      CHAOS_VTOP_NODE="$NODE_BIN" CHAOS_VTOPCTL="$CTL_BIN" CHAOS_611_SEED="$seed" \
      bash "$CHAOS/scenarios/$scenario.sh" > "$OUT/run-$i-$scenario.log" 2>&1 || code=$?
    read -r data control kinds <<< "$(judge "$wd")"
    log "run $i $scenario seed=$seed: scenario exit $code, data history $data, control history $control, violations $kinds ($((SECONDS - started))s)"
    printf '{"bug":%s,"run":%d,"scenario":"%s","seed":%s,"scenario_exit":%d,"data":"%s","control":"%s","violations":"%s","seconds":%d}\n' \
      "$BUG" "$i" "$scenario" "$seed" "$code" "$data" "$control" "$kinds" "$((SECONDS - started))" >> "$SUMMARY"
    [[ "$data" == VIOLATED || "$control" == VIOLATED ]] && detected=$((detected + 1))
  done
done

if ((detected > 0)); then
  log "DETECTED: the checkers flagged $detected run(s) with #$BUG re-introduced; see $SUMMARY"
  exit 0
fi
log "MISSED: no run with #$BUG re-introduced broke a history rule; see $SUMMARY (a run that \
skipped never reached the bug's shape; a run whose scenario failed was caught by its own assertion)"
exit 1
