#!/usr/bin/env bash
# Scenario 01 — grow the metadata group 3 -> 5 under sustained metadata and
# native producer load.
#
# Invariants: the proposal stream never loses a committed command, every node
# converges on voters [1..5], and the new nodes catch up to the leader's
# applied index (snapshot/log replication over the real TCP transport).
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

RECORDS="${CHAOS_GROW_RECORDS:-100000}"
BATCH="${CHAOS_GROW_BATCH:-100}"
PROGRESS_FLOOR="${CHAOS_GROW_PROGRESS_FLOOR:-1000}"
require_integer_in_range CHAOS_GROW_RECORDS "$RECORDS" 1 100000000
require_integer_in_range CHAOS_GROW_BATCH "$BATCH" 1 "$RECORDS"
require_integer_in_range CHAOS_GROW_PROGRESS_FLOOR "$PROGRESS_FLOOR" 1 "$RECORDS"

start_meta_node 1 2 3 4 5 > /dev/null
start_meta_node 2 1 3 4 5 > /dev/null
start_meta_node 3 1 2 4 5 > /dev/null
meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
log "3-node group up, leader=$LEADER_ID"

# Run the native quorum data plane concurrently. It is intentionally a
# separate process assembly today (the production control/data daemon does
# not exist yet), but this makes the membership transition compete with real
# producer, replication, fsync, and fetch work instead of a synthetic sleep.
F1_PID="$(start_follower 1)"
F2_PID="$(start_follower 2)"
DATA_LEADER_PID="$(start_leader)"
CLIENT_CFG="$(emit_client_config)"
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$RECORDS" --batch "$BATCH" --durability quorum \
  --acked-file "$WORKDIR/acked" > "$WORKDIR/logs/produce.log" 2>&1 &
PRODUCER_PID=$!
echo "$PRODUCER_PID" >> "$WORKDIR/pids"
deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
until [[ -s "$WORKDIR/acked" && "$(cat "$WORKDIR/acked")" -ge "$PROGRESS_FLOOR" ]]; do
  kill -0 "$PRODUCER_PID" 2>/dev/null \
    || fail "native producer ended before membership work began: $(tail -3 "$WORKDIR/logs/produce.log")"
  [[ $SECONDS -lt $deadline ]] || fail "native producer made no progress"
  sleep 0.1
done
log "native quorum producer active during membership growth"

# Sustained commit load: unique RegisterNode proposals through the leader.
# Every one either acks or the scenario fails — no silent loss.
propose_loop() {
  local n
  for n in $(seq 16 79); do
    propose_register "$LEADER_ID" "$(printf '%02x' "$n")" > /dev/null \
      || { echo "proposal $n failed" >> "$WORKDIR/proposal-failures"; return 1; }
    echo "$n" > "$WORKDIR/proposal-progress"
  done
}
propose_loop &
LOAD_PID=$!

start_meta_node 4 1 2 3 5 > /dev/null
start_meta_node 5 1 2 3 4 > /dev/null
meta_admin "$LEADER_ID" add-learner --node-id 4 > /dev/null
meta_admin "$LEADER_ID" add-learner --node-id 5 > /dev/null
log "learners 4,5 added mid-load"
meta_admin "$LEADER_ID" change-membership --voters 1,2,3,4,5 > /dev/null
log "membership change to [1..5] committed mid-load"

kill -0 "$PRODUCER_PID" 2>/dev/null \
  || fail "native producer did not remain active through membership change"

wait "$LOAD_PID" || fail "proposal load lost a command: $(cat "$WORKDIR/proposal-failures" 2>/dev/null)"
log "proposal load finished without a single lost commit"

for id in 1 2 3 4 5; do
  VOTERS="$(meta_status_field "$id" "['membership']['voters']")"
  [[ "$VOTERS" == "[1, 2, 3, 4, 5]" ]] || fail "node $id sees voters $VOTERS"
done

LEADER_APPLIED="$(meta_status_field "$LEADER_ID" "['last_applied']['index']")"
for id in 4 5; do
  deadline=$((SECONDS + ELECTION_TIMEOUT_SECONDS))
  while true; do
    APPLIED="$(meta_status_field "$id" "['last_applied']['index']" 2>/dev/null || echo 0)"
    [[ "$APPLIED" == "null" ]] && APPLIED=0
    [[ "$APPLIED" -ge "$LEADER_APPLIED" ]] && break
    [[ $SECONDS -lt $deadline ]] || fail "node $id stuck at applied=$APPLIED < leader=$LEADER_APPLIED"
    sleep 0.3
  done
done
log "new voters caught up: applied>=$LEADER_APPLIED on nodes 4 and 5"

wait "$PRODUCER_PID" \
  || fail "native producer failed: $(tail -3 "$WORKDIR/logs/produce.log")"
ACKED="$(cat "$WORKDIR/acked")"
[[ "$ACKED" -eq "$RECORDS" ]] || fail "native producer acked $ACKED != $RECORDS"
"$VTOP_NODE" verify --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --expect-at-least "$ACKED" > "$WORKDIR/logs/verify.log" 2>&1 \
  || fail "native verification failed: $(tail -3 "$WORKDIR/logs/verify.log")"
log "native producer/fetch stream remained byte-exact through membership growth"

stop_node_now "$DATA_LEADER_PID"
stop_node_now "$F1_PID"
stop_node_now "$F2_PID"
seal_and_verify_active leader "$WORKDIR/data-leader/range.active"
seal_and_verify_active follower-1 "$WORKDIR/data-follower-1/range.active"
seal_and_verify_active follower-2 "$WORKDIR/data-follower-2/range.active"
