#!/usr/bin/env bash
# Scenario 13 — orderly shutdown on real processes (#280).
#
# Every earlier scenario stops nodes with SIGKILL, because until #280 that was
# the only stop there was: vtop-node installed no signal handler, so every pod
# deletion, rolling update, and systemd restart waited out the grace period
# and then crashed the process anyway. Durability never depended on a clean
# exit — scenario 04 proves that — but every rollout paid the full grace
# period per node, every restart took the crash-recovery path, and a departing
# leader let its lease lapse on the metadata deadline instead of handing the
# range back.
#
# What it proves, in order:
#   1. SIGTERM actually stops the node, within a deadline — the helper FAILS
#      the scenario if the signal is ignored, so #280 cannot silently reopen;
#   2. the departing leader RELEASES its range lease on the way out (asserted
#      from its own log), so the replacement acquires the range without
#      waiting out the lease deadline — the operational win of the change;
#   3. every record acknowledged before the stop is still readable after the
#      handoff;
#   4. the stopping node wrote its final commit boundary (the
#      `data_node_stopped ... committed=` marker), so the next open has no
#      torn tail to truncate;
#   5. a gracefully stopped follower drains the same way, and the stopped
#      leader's sealed artifact still verifies offline.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

RECORDS="${CHAOS_SHUTDOWN_RECORDS:-1500}"
BATCH="${CHAOS_SHUTDOWN_BATCH:-100}"
require_integer_in_range CHAOS_SHUTDOWN_RECORDS "$RECORDS" 1 100000000
require_integer_in_range CHAOS_SHUTDOWN_BATCH "$BATCH" 1 "$RECORDS"

# --- metadata plane ---------------------------------------------------------
M1=$(start_meta_node 1 2 3)
M2=$(start_meta_node 2 1 3)
M3=$(start_meta_node 3 1 2)
log "meta nodes up: $M1 $M2 $M3"
meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
log "meta leader elected: node $LEADER_ID"

meta_admin "$LEADER_ID" create-topic \
  --name "$TOPIC" --topic-uuid "$TOPIC_UUID" --root-range-uuid "$RANGE_ID" > /dev/null \
  || fail "could not create the topic in metadata"
for node in "$LEADER_UUID" "$FOLLOWER1_UUID" "$FOLLOWER2_UUID"; do
  meta_admin "$LEADER_ID" register-node \
    --node-uuid "$node" --addr "$REGISTER_HOST_PREFIX.1:$REGISTER_PORT" > /dev/null \
    || fail "could not register data node $node"
done
log "metadata knows the topic and all three data nodes"

# --- data plane -------------------------------------------------------------
EXPECTED_FIRST_EPOCH=1
F1=$(start_follower 1 "" "$EXPECTED_FIRST_EPOCH")
F2=$(start_follower 2 "" "$EXPECTED_FIRST_EPOCH")
DL=$(start_leader_with_lease "$LEADER_ID")
log "data nodes up: leader=$DL followers=$F1,$F2"

EPOCH_BEFORE="$(await_lease_holder "$LEADER_ID" "$LEADER_UUID")"
[[ "$EPOCH_BEFORE" == "$EXPECTED_FIRST_EPOCH" ]] \
  || fail "first grant minted epoch $EPOCH_BEFORE, not $EXPECTED_FIRST_EPOCH"
log "leader holds the range at epoch $EPOCH_BEFORE"

# --- produce to completion, then stop the leader ORDERLY --------------------
# The produce COMPLETES before the stop: an interrupted producer is scenario
# 09's subject. This scenario is about what a stop that is not a crash buys.
CLIENT_CFG="$(emit_client_config_at_epoch "$EPOCH_BEFORE")"
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$RECORDS" --batch "$BATCH" --durability quorum \
  --acked-file "$WORKDIR/acked" > "$WORKDIR/logs/produce.log" 2>&1 \
  || fail "quorum produce failed"
ACKED="$(cat "$WORKDIR/acked")"
[[ "$ACKED" -eq "$RECORDS" ]] || fail "only $ACKED of $RECORDS were acknowledged"
log "$ACKED records acknowledged under quorum produce"

STOP_STARTED=$SECONDS
stop_node_gracefully "the range leader" "$DL"
STOP_TOOK=$((SECONDS - STOP_STARTED))
log "leader stopped on SIGTERM in ${STOP_TOOK}s — the signal is handled, not ignored"

LEADER_LOG="$WORKDIR/logs/data-leader-lease.log"
grep -q "shutdown_signal received" "$LEADER_LOG" \
  || fail "the leader never logged the shutdown signal; its exit was not the drain path"
# The release is the operational win: asserted from the departing leader's own
# log, which is deterministic where timing margins are not.
grep -q "range lease released for shutdown" "$LEADER_LOG" \
  || fail "the departing leader did not release its lease; failover is back to waiting \
out the metadata deadline, which is the cost half of #280 reopened"
# The final commit boundary marker proves the quiesce ran: the next open of
# this directory has no torn tail to truncate.
grep -q "data_node_stopped role=leader" "$LEADER_LOG" \
  || fail "the leader exited without writing its final commit boundary marker"
log "the departing leader released the lease and wrote its final commit boundary"

# --- the replacement takes the RELEASED range -------------------------------
F1_OFFSET="$(follower_committed_offset 1)"
F2_OFFSET="$(follower_committed_offset 2)"
if [[ "$F1_OFFSET" -ge "$F2_OFFSET" ]]; then
  PROMOTE_N=1 PROMOTE_UUID="$FOLLOWER1_UUID" PROMOTE_PID="$F1"
  OTHER_N=2 OTHER_PID="$F2"
else
  PROMOTE_N=2 PROMOTE_UUID="$FOLLOWER2_UUID" PROMOTE_PID="$F2"
  OTHER_N=1 OTHER_PID="$F1"
fi
log "follower offsets: f1=$F1_OFFSET f2=$F2_OFFSET; promoting follower $PROMOTE_N"

stop_node_now "$PROMOTE_PID"
NEW=$(start_promoted_follower "$PROMOTE_N" "$LEADER_ID")
ACQUIRE_STARTED=$SECONDS
EPOCH_AFTER="$(await_lease_holder "$LEADER_ID" "$PROMOTE_UUID" "$ELECTION_TIMEOUT_SECONDS")"
ACQUIRE_TOOK=$((SECONDS - ACQUIRE_STARTED))
# Generous sanity bound, not the primary assertion (the log line above is):
# against a RELEASED lease the acquisition is immediate, where an unreleased
# one cannot be granted until the old deadline passes.
LEASE_SECONDS=$(((LEASE_DURATION_MS + 999) / 1000))
[[ "$ACQUIRE_TOOK" -le "$LEASE_SECONDS" ]] \
  || fail "acquiring the released range took ${ACQUIRE_TOOK}s, longer than the whole \
lease deadline of ${LEASE_SECONDS}s; the release evidently did not take effect"
log "follower $PROMOTE_N took the released range at epoch $EPOCH_AFTER in ${ACQUIRE_TOOK}s"

# --- nothing acknowledged was lost ------------------------------------------
CLIENT_AFTER="$(emit_client_config_at_epoch "$EPOCH_AFTER")"
await_verified_floor "$CLIENT_AFTER" "$(native_addr)" "$ACKED"
log "every one of the $ACKED acknowledged records is readable after the orderly handoff"

# --- the follower drains the same way ---------------------------------------
stop_node_gracefully "the remaining follower" "$OTHER_PID"
grep -q "data_node_stopped role=follower" "$WORKDIR/logs/data-follower-$OTHER_N.log" \
  || fail "the follower exited without its final commit boundary marker"
log "the remaining follower drained on SIGTERM with its final commit boundary written"

# --- the cleanly stopped leader's artifact still verifies --------------------
seal_and_verify_active old-leader "$WORKDIR/data-leader"

stop_node_now "$NEW"
log "PASS"
