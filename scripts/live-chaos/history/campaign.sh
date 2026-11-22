#!/usr/bin/env bash
# A long nemesis campaign over the cluster plane, judged by its histories
# (#728, criterion A4 of #714: "a 24-hour campaign shows zero violations").
#
# It runs the live-chaos scenarios that hold a nemesis, one after another with
# a fresh seed each time, until the time is up:
#
#   04  kill -9 of the leader mid-produce            (kill; data history)
#   05  a follower's disk fills                      (disk full)
#   06  the metadata leader is partitioned away      (partition)
#   07  a node's clock is skewed                     (clock skew)
#   09  the range leader is killed, a follower promoted, the old one
#       restarted and fenced                         (kill; data history)
#   14  three candidates, the leader killed, the role follows the lease
#                                                    (kill; data and control histories)
#   23  the #611 shape: a follower killed, the leader killed, a follower
#       frozen and thawed                            (kill, pause; data and
#                                                    control histories)
#
# Every scenario asserts its own outcome, and the ones that record histories
# run both checkers before they pass, so a run fails on a broken assertion or
# a broken history alike. The campaign records each run's verdict, keeps the
# work directory of every run that did not pass (and only its history and
# checker reports for a run that did), and exits non-zero if any run failed.
#
# Usage: campaign.sh
#
# Environment:
#   CAMPAIGN_HOURS      how long to run (default 24; fractions allowed)
#   CAMPAIGN_SCENARIOS  space-separated scenario numbers (default "04 05 06 07 09 14 23")
#   CAMPAIGN_OUT        where runs and the summary go (default: a new temp dir)
#   CAMPAIGN_MAX_RUNS   stop after this many runs as well (default: no limit)
#   CAMPAIGN_STOP_ON_FAILURE=1  stop at the first failed run
# plus anything lib.sh reads (CHAOS_PROFILE, CHAOS_VTOP_NODE, CHAOS_VTOPCTL).
#
# Output: <out>/runs.jsonl (one line per run), <out>/summary.json, and
# <out>/runs/<n>-<scenario>/ for each run.
# Exit status: 0 every run passed or skipped; 1 a run failed; 2 bad arguments.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHAOS="$(cd "$HERE/.." && pwd)"

HOURS="${CAMPAIGN_HOURS:-24}"
read -r -a WANTED <<< "${CAMPAIGN_SCENARIOS:-04 05 06 07 09 14 23}"
OUT="${CAMPAIGN_OUT:-$(mktemp -d "${TMPDIR:-/tmp}/vtop-campaign.XXXXXX")}"
MAX_RUNS="${CAMPAIGN_MAX_RUNS:-0}"

[[ "$HOURS" =~ ^[0-9]+(\.[0-9]+)?$ ]] || { echo "CAMPAIGN_HOURS must be a number, got '$HOURS'" >&2; exit 2; }
[[ "$MAX_RUNS" =~ ^[0-9]+$ ]] || { echo "CAMPAIGN_MAX_RUNS must be an integer, got '$MAX_RUNS'" >&2; exit 2; }
SCRIPTS=()
for n in "${WANTED[@]}"; do
  match=("$CHAOS"/scenarios/"$n"-*.sh)
  [[ -e "${match[0]}" && ${#match[@]} -eq 1 ]] || { echo "no single scenario numbered '$n'" >&2; exit 2; }
  SCRIPTS+=("${match[0]}")
done

mkdir -p "$OUT/runs"
RUNS_FILE="$OUT/runs.jsonl"
: > "$RUNS_FILE"
SECONDS_TOTAL="$(python3 -c "print(int(float('$HOURS') * 3600))")"
END=$((SECONDS + SECONDS_TOTAL))
log() { echo "[campaign] $(date -u +%FT%TZ) $*"; }
log "running ${WANTED[*]} for $HOURS h into $OUT"

verdicts() { # <workdir> — "<data> <control> <kinds>" from the reports the scenario kept
  python3 - "$1" << 'PY'
import json, pathlib, sys
logs = pathlib.Path(sys.argv[1]) / "logs"
out, kinds = [], []
for name in ("history-check.json", "control-check.json"):
    try:
        d = json.loads((logs / name).read_text())
    except (OSError, ValueError):
        out.append("none")
        continue
    out.append("valid" if d.get("valid") else "VIOLATED")
    kinds += sorted(d.get("violations", {}))
print(out[0], out[1], ",".join(kinds) or "-")
PY
}

n=0
failed=0
while ((SECONDS < END)); do
  ((MAX_RUNS == 0 || n < MAX_RUNS)) || break
  script="${SCRIPTS[$((n % ${#SCRIPTS[@]}))]}"
  name="$(basename "$script" .sh)"
  n=$((n + 1))
  seed=$((RANDOM * 32768 + RANDOM))
  wd="$OUT/runs/$(printf '%05d' "$n")-$name"
  started=$SECONDS
  code=0
  CHAOS_KEEP=1 CHAOS_WORKDIR="$wd" CHAOS_611_SEED="$seed" bash "$script" > "$wd.log" 2>&1 || code=$?
  read -r data control kinds <<< "$(verdicts "$wd")"
  case "$code" in
    0) verdict=PASS ;;
    77) verdict=SKIP ;;
    *)
      verdict=FAIL
      failed=$((failed + 1))
      ;;
  esac
  printf '{"run":%d,"scenario":"%s","seed":%d,"exit":%d,"verdict":"%s","data_history":"%s","control_history":"%s","violations":"%s","seconds":%d}\n' \
    "$n" "$name" "$seed" "$code" "$verdict" "$data" "$control" "$kinds" "$((SECONDS - started))" >> "$RUNS_FILE"
  log "run $n $name seed=$seed: $verdict (exit $code), data history $data, control history $control ($((SECONDS - started))s)"
  if [[ "$verdict" != FAIL ]]; then
    # A run that passed keeps its histories and the checkers' reports, not
    # its data directories: a day of runs would otherwise fill the disk.
    find "$wd" -mindepth 1 -maxdepth 1 ! -name history ! -name logs -exec rm -rf {} + 2> /dev/null || true
  fi
  if [[ "$verdict" == FAIL && "${CAMPAIGN_STOP_ON_FAILURE:-0}" == 1 ]]; then
    log "stopping at the first failure, as asked"
    break
  fi
done

python3 - "$RUNS_FILE" "$OUT/summary.json" "$HOURS" << 'PY'
import json, sys
from collections import Counter, defaultdict
runs = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
per = defaultdict(Counter)
for r in runs:
    per[r["scenario"]][r["verdict"]] += 1
summary = {
    "hours_requested": float(sys.argv[3]),
    "runs": len(runs),
    "seconds_run": sum(r["seconds"] for r in runs),
    "verdicts": dict(Counter(r["verdict"] for r in runs)),
    "per_scenario": {k: dict(v) for k, v in sorted(per.items())},
    "history_violations": [r for r in runs if "VIOLATED" in (r["data_history"], r["control_history"])],
    "failed_runs": [r for r in runs if r["verdict"] == "FAIL"],
}
json.dump(summary, open(sys.argv[2], "w"), indent=2)
print(json.dumps({k: summary[k] for k in ("runs", "seconds_run", "verdicts", "per_scenario")}, indent=2))
PY

if ((failed > 0)); then
  log "FAILED: $failed of $n run(s) failed; see $OUT/summary.json"
  exit 1
fi
log "PASSED: $n run(s), none failed"
exit 0
