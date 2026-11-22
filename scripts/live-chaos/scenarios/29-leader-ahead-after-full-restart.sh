#!/usr/bin/env bash
# 29 — a whole replica set killed at once, restarted with the leader holding
# records both followers lack (#599).
#
# A leader's retransmission buffer dies with it, so after a restart the only
# copy of a record the followers never received is the leader's own log. Here
# both followers are killed, the leader appends and fsyncs one batch that no
# quorum ever acknowledges (the client is told "quorum not reached"), and then
# the leader is killed with SIGKILL and all three nodes are started again. The
# restarted leader's log is strictly longer than both followers', and nothing
# in memory remembers what they lack.
#
# Invariants: produce resumes within a bound with no operator action, the
# leader's quorum counts both followers again, every acknowledged record is
# read back byte-exact and in order, and both followers end holding the whole
# log. The batch only the leader held is kept: a leader's log is authoritative
# at its epoch, and a record its client was never told about may appear.
#
# The plane is PLAINTEXT and the range is in the default (v1) format, the shape
# the #599 benchmark runs. A plaintext session carries no fence, and catching a
# follower up from the leader's log was gated on one; and a v1 frame stores the
# producer identity merged with its epoch, which the catch-up could not
# re-send. So the followers were never caught up.
export CHAOS_TRANSPORT="${CHAOS_TRANSPORT:-plaintext}"
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

SHARED="${CHAOS_AHEAD_SHARED:-1000}"
LEADER_ONLY="${CHAOS_AHEAD_LEADER_ONLY:-100}"
AFTER="${CHAOS_AHEAD_AFTER:-1000}"
BATCH=100
RECOVERY_SECONDS="${CHAOS_AHEAD_RECOVERY_SECONDS:-60}"
require_integer_in_range CHAOS_AHEAD_SHARED "$SHARED" 100 1000000
require_integer_in_range CHAOS_AHEAD_LEADER_ONLY "$LEADER_ONLY" 1 1000000
require_integer_in_range CHAOS_AHEAD_AFTER "$AFTER" 100 1000000
require_integer_in_range CHAOS_AHEAD_RECOVERY_SECONDS "$RECOVERY_SECONDS" 5 3600
(( SHARED % BATCH == 0 && AFTER % BATCH == 0 )) \
  || fail "the shared and after counts must be multiples of the batch ($BATCH)"

F1_PID="$(start_follower 1)"
F2_PID="$(start_follower 2)"
LEADER_PID="$(start_leader)"
CLIENT_CFG="$(emit_client_config)"
log "data plane up over $CHAOS_TRANSPORT, leader pid=$LEADER_PID"

# The records every replica holds.
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$SHARED" --batch "$BATCH" --durability quorum \
  --acked-file "$WORKDIR/acked-shared" > "$WORKDIR/logs/produce-shared.log" 2>&1 \
  || fail "quorum produce failed (see $WORKDIR/logs/produce-shared.log)"
await_replicas_settled "$SHARED" "$(emit_node_status_config)"
log "all three replicas hold $SHARED records"

# Both followers go down. The leader alone appends the next batch and
# fsyncs it, then refuses it: no quorum can acknowledge it.
stop_node_now "$F1_PID"
stop_node_now "$F2_PID"
set +e
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$LEADER_ONLY" --batch "$LEADER_ONLY" --durability quorum \
  --first-sequence "$SHARED" --request-timeout-ms 20000 \
  > "$WORKDIR/logs/produce-leader-only.log" 2>&1
LEADER_ONLY_EXIT=$?
set -e
[[ $LEADER_ONLY_EXIT -ne 0 ]] \
  || fail "a quorum produce with both followers down was acknowledged"
grep -q "quorum not reached" "$WORKDIR/logs/produce-leader-only.log" \
  || fail "the unacknowledged batch was not refused for want of a quorum: $(tail -3 "$WORKDIR/logs/produce-leader-only.log")"
log "the leader alone holds $LEADER_ONLY more records, refused for want of a quorum"

# The whole replica set dies at once, then all three come back.
stop_node_now "$LEADER_PID"
LOG_AHEAD=$((SHARED + LEADER_ONLY))
RESTARTED_AT=$SECONDS
F1_PID="$(start_follower 1)"
F2_PID="$(start_follower 2)"
LEADER_PID="$(start_leader)"
log "all three restarted; the leader's log ($LOG_AHEAD) is longer than both followers' ($SHARED)"

# The next write must succeed on its own. A refused batch (quorum not
# reached) is retried, as a client does, until the bound.
deadline=$((RESTARTED_AT + RECOVERY_SECONDS))
until "$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$AFTER" --batch "$BATCH" --durability quorum \
  --first-sequence "$LOG_AHEAD" --acked-file "$WORKDIR/acked-after" \
  --request-timeout-ms 5000 > "$WORKDIR/logs/produce-after.log" 2>&1; do
  [[ $SECONDS -lt $deadline ]] \
    || fail "no quorum produce succeeded within ${RECOVERY_SECONDS}s of the restart: $(tail -3 "$WORKDIR/logs/produce-after.log"); the leader's follower_connected: $(metric_value "$(data_metrics_addr 0)" vtop_broker_follower_connected 2>&1 | tr '\n' ' ')"
  sleep 0.5
done
log "quorum produce resumed $((SECONDS - RESTARTED_AT))s after the restart, inside the ${RECOVERY_SECONDS}s bound"

TOTAL=$((LOG_AHEAD + AFTER))
"$VTOP_NODE" verify --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --expect-at-least "$TOTAL" > "$WORKDIR/logs/verify.log" 2>&1 \
  || { tail -3 "$WORKDIR/logs/verify.log" >&2; fail "records lost or misordered after the restart"; }
log "every record byte-exact and in order: $(grep verify_done "$WORKDIR/logs/verify.log")"

await_replicas_settled "$TOTAL" "$(emit_node_status_config)"
PROBE_CFG="$(emit_replica_probe_config)"
for n in 1 2; do
  STATUS="$("$VTOP_NODE" replica-status --client-config "$PROBE_CFG" \
    --addr "$(replica_addr "$n")")"
  LOCAL="${STATUS#*local_committed_offset=}"
  LOCAL="${LOCAL%% *}"
  [[ "$LOCAL" -eq "$TOTAL" ]] || fail "follower $n committed offset $LOCAL != $TOTAL"
done
log "both followers hold all $TOTAL records"

stop_node_now "$LEADER_PID"
stop_node_now "$F1_PID"
stop_node_now "$F2_PID"
seal_and_verify_active leader "$WORKDIR/data-leader"
seal_and_verify_active follower-1 "$WORKDIR/data-follower-1"
seal_and_verify_active follower-2 "$WORKDIR/data-follower-2"
