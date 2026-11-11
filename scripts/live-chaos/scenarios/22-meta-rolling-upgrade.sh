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
# Then the data nodes (#634): the upgraded tier lists what each node runs and
# raises the report level, and a leader on this build must replicate to a
# follower on the previous release - which predates the replica version
# handshake - as well as to one on this build.
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

# --- what each node runs (#634) ----------------------------------------------
# The upgraded tier serves the cluster-versions read. The nodes registered
# above are placeholders: no range, no report, so a raise does not wait for
# them. The level that lets data nodes report is raised on the metadata
# tier's word alone - nothing could vouch for a data node before it.
LEADER="$(wait_meta_leader 1 2 3)"
VERSIONS="$(meta_admin "$LEADER" node-versions --level 8)" \
  || fail "node-versions refused a raise no counted node holds back: $VERSIONS"
python3 -c '
import json, sys
view = json.loads(sys.argv[1])
assert view["feature_level"] == 4, view["feature_level"]
assert view["nodes"], "the registered nodes are listed"
assert all(not node["counted"] and node["report"] is None for node in view["nodes"]), view
assert view["blockers"] == [], view["blockers"]
' "$VERSIONS" || fail "node-versions did not describe the placeholders: $VERSIONS"
REPORTING="$("$VTOPCTL" --json meta enable-features --level 7 --config "$(emit_admin_config "$LEADER")")" \
  || fail "the report level must be raised on the metadata tier alone: $REPORTING"
[[ "$REPORTING" == *'"generation": 7'* || "$REPORTING" == *'"generation":7'* ]] \
  || fail "enable-features did not report level 7: $REPORTING"
await_converged "report level raised"

# --- the replica plane across releases (#634) --------------------------------
# A follower on the PREVIOUS release and one on this build, behind a leader on
# this build, at a static epoch. The leader opens every session with a version
# hello; the old follower answers it the way every release before the
# handshake does, and the leader must fall back to the old protocol rather than
# drop it. No data node here reads a level, so nobody is refused, and every
# acknowledged record must reach BOTH followers.
VTOP_NODE="$OLD_VTOP_NODE"
start_follower 1 > /dev/null
VTOP_NODE="$NEW_VTOP_NODE"
start_follower 2 > /dev/null
start_leader > /dev/null
MIXED_RECORDS=200
"$VTOP_NODE" produce --client-config "$(emit_client_config)" --addr "$(native_addr)" \
  --records "$MIXED_RECORDS" --batch 20 --durability quorum \
  --acked-file "$WORKDIR/acked-mixed" > "$WORKDIR/logs/produce-mixed.log" 2>&1 \
  || fail "produce across releases failed: $(tail -3 "$WORKDIR/logs/produce-mixed.log")"
for n in 1 2; do
  deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
  until [[ "$(follower_committed_offset "$n" 2>/dev/null || echo 0)" -ge "$MIXED_RECORDS" ]]; do
    [[ $SECONDS -lt $deadline ]] \
      || fail "follower $n ($([[ $n == 1 ]] && echo previous || echo this) release) stopped at $(follower_committed_offset "$n" 2>/dev/null || echo '?') of $MIXED_RECORDS acknowledged records"
    (( deadline - SECONDS > 1 )) && sleep 0.2
  done
done
if grep -q "protocol error" "$WORKDIR/logs/data-follower-1.log"; then
  fail "the previous release's follower dropped a session: $(grep "protocol error" "$WORKDIR/logs/data-follower-1.log" | head -1)"
fi
log "a leader on this build replicated to followers on both releases"
