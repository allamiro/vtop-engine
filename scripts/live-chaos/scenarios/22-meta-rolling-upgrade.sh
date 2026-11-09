#!/usr/bin/env bash
# Scenario 22 — a rolling upgrade of the metadata tier, previous release to
# this one (#554).
#
# Three voters start on the PREVIOUS release (CHAOS_OLD_VTOP_NODE) and are
# replaced one at a time by this build, the way a StatefulSet rolls them. The
# hazard: a voter that cannot decode an entry fails the whole AppendEntries
# batch and appends nothing after it, so a new leader that proposed a command
# kind the old voters do not know would stall the tier mid-rollout. This build
# proposes nothing above the cluster's feature level, so at every mixed step
# the tier must keep committing and every voter must converge.
#
# After the last voter is replaced, the operator raises the feature level; the
# new record must be readable by every node, including one restarted on it.
#
# Required: CHAOS_OLD_VTOP_NODE, a vtop-node binary built from the previous
# release tag. This scenario does not build it; CI's upgrade job does.
#
# Without it the scenario SKIPS (exit 77, which run-all.sh reports as SKIP),
# before the harness is loaded: a suite run on a workstation has no previous
# release to hand, and that is not a failure of this build.
OLD_VTOP_NODE="${CHAOS_OLD_VTOP_NODE:-}"
if [[ -z "$OLD_VTOP_NODE" ]]; then
  echo "SKIP: set CHAOS_OLD_VTOP_NODE to a vtop-node built from the previous release tag"
  exit 77
fi
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
[[ -x "$OLD_VTOP_NODE" ]] || fail "CHAOS_OLD_VTOP_NODE=$OLD_VTOP_NODE is not executable"

require_binaries
init_workdir

NEW_VTOP_NODE="$VTOP_NODE"
log "rolling $("$OLD_VTOP_NODE" --version) -> $("$NEW_VTOP_NODE" --version)"

# A write must COMMIT within the progress deadline. Bounded, because the
# failure this scenario exists to catch - voters that cannot decode an entry
# and append nothing after it - shows up as a write that never commits, and an
# unbounded call would hang instead of failing with a reason. (Converged
# applied indexes alone cannot show it: an entry no quorum accepts is applied
# nowhere.)
commit_write() { # <label> <uuid-suffix (2 hex)>
  local label="$1" suffix="$2" leader
  leader="$(wait_meta_leader 1 2 3)"
  timeout --kill-after=1 "$PROGRESS_TIMEOUT_SECONDS" \
    "$VTOPCTL" --json meta register-node \
    --node-uuid "cccccccc-0000-0000-0000-0000000000$suffix" \
    --addr "$REGISTER_HOST_PREFIX.$((16#$suffix)):$REGISTER_PORT" \
    --config "$(emit_admin_config "$leader")" > /dev/null \
    || fail "$label: a write did not commit within ${PROGRESS_TIMEOUT_SECONDS}s (leader $leader): the tier stopped committing"
}

# A node's applied index, asked for within <seconds>: a status request a
# stalled node never answers must not hold the loop past its deadline
# (review), so the call is killed when its budget runs out.
applied_on() { # <node-id> <seconds>
  local budget=$(($2 > 0 ? $2 : 1))
  timeout --kill-after=1 "$budget" \
    "$VTOPCTL" --json meta status --config "$(emit_admin_config "$1")" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["last_applied"]["index"])'
}

# Every voter reaches the leader's applied index, within the progress deadline.
await_converged() { # <label>
  local label="$1" target id deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
  target="$(applied_on "$(wait_meta_leader 1 2 3)" "$PROGRESS_TIMEOUT_SECONDS")" \
    || fail "$label: the leader did not report its applied index"
  for id in 1 2 3; do
    while true; do
      local applied
      applied="$(applied_on "$id" $((deadline - SECONDS)) 2>/dev/null || echo 0)"
      [[ "$applied" -ge "$target" ]] && break
      [[ $SECONDS -lt $deadline ]] \
        || fail "$label: node $id applied $applied, the leader $target - a voter stopped appending"
      sleep 0.2
    done
  done
  log "$label: all voters at applied index >= $target"
}

# --- the previous release ----------------------------------------------------
declare -A PID
VTOP_NODE="$OLD_VTOP_NODE"
PID[1]=$(start_meta_node 1 2 3)
PID[2]=$(start_meta_node 2 1 3)
PID[3]=$(start_meta_node 3 1 2)
meta_admin 1 init --members 1,2,3 > /dev/null
commit_write "previous release" 01
await_converged "previous release"

# --- replace each voter with this build ---------------------------------------
VTOP_NODE="$NEW_VTOP_NODE"
suffix=2
for id in 1 2 3; do
  stop_node_now "${PID[$id]}"
  case "$id" in
    1) PID[1]=$(start_meta_node 1 2 3) ;;
    2) PID[2]=$(start_meta_node 2 1 3) ;;
    3) PID[3]=$(start_meta_node 3 1 2) ;;
  esac
  # A write through whichever node leads now - old or new - must commit
  # and reach every voter, old ones included.
  commit_write "node $id upgraded" "$(printf '%02x' "$suffix")"
  suffix=$((suffix + 1))
  await_converged "node $id upgraded"
done

# --- every voter is upgraded: raise the feature level -------------------------
LEADER="$(wait_meta_leader 1 2 3)"
RAISED="$(meta_admin "$LEADER" enable-features --level 4)"
[[ "$RAISED" == *'"generation": 4'* || "$RAISED" == *'"generation":4'* ]] \
  || fail "enable-features did not report level 4: $RAISED"
await_converged "feature level raised"

# The cluster-config record is now in every log; a node restarted on it must
# open it, and asking again is a no-op, not an error.
stop_node_now "${PID[2]}"
PID[2]=$(start_meta_node 2 1 3)
LEADER="$(wait_meta_leader 1 2 3)"
AGAIN="$(meta_admin "$LEADER" enable-features --level 3)"
[[ "$AGAIN" == *'"generation": 4'* || "$AGAIN" == *'"generation":4'* ]] \
  || fail "a lower level must leave 4 in force: $AGAIN"
commit_write "restart after the raise" "$(printf '%02x' "$suffix")"
await_converged "restart after the raise"
log "rolled the metadata tier from the previous release and raised the feature level"
