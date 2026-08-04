#!/usr/bin/env bash
# Scenario 04 — kill -9 the data-plane leader mid-produce under quorum acks.
#
# Invariants: every acknowledged record survives a hard leader death (byte
# exact after recovery of the same directory), the committed HWM never
# regresses below the acknowledged floor, and nothing above the HWM is ever
# exposed to a consumer.
#
# Honest scope: the native data plane has no leader election yet — this
# validates durability and recovery, NOT failover. The restart re-opens the
# killed leader's directory in standalone mode.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

RECORDS="${CHAOS_KILL9_RECORDS:-2000000}"
BATCH="${CHAOS_KILL9_BATCH:-200}"
ACK_FLOOR="${CHAOS_KILL9_ACK_FLOOR:-5000}"
require_integer_in_range CHAOS_KILL9_RECORDS "$RECORDS" 1 100000000
require_integer_in_range CHAOS_KILL9_BATCH "$BATCH" 1 "$RECORDS"
require_integer_in_range CHAOS_KILL9_ACK_FLOOR "$ACK_FLOOR" 1 "$RECORDS"

F1_PID="$(start_follower 1)"
F2_PID="$(start_follower 2)"
LEADER_PID="$(start_leader)"
CLIENT_CFG="$(emit_client_config)"
log "data plane up, leader pid=$LEADER_PID"

# Produce a large stream with per-batch quorum acks; kill the leader while
# the stream is in flight.
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$RECORDS" --batch "$BATCH" --durability quorum \
  --acked-file "$WORKDIR/acked" > "$WORKDIR/logs/produce.log" 2>&1 &
PRODUCER=$!
echo "$PRODUCER" >> "$WORKDIR/pids"

# Wait until real progress, then murder the leader mid-stream.
deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
until [[ -s "$WORKDIR/acked" && "$(cat "$WORKDIR/acked")" -ge "$ACK_FLOOR" ]]; do
  [[ $SECONDS -lt $deadline ]] || fail "producer made no progress"
  kill -0 "$PRODUCER" 2>/dev/null || fail "producer died early: $(tail -3 "$WORKDIR/logs/produce.log")"
  sleep 0.1
done
kill -9 "$LEADER_PID"
log "leader killed with SIGKILL mid-produce"

set +e
wait "$PRODUCER"
PRODUCER_EXIT=$?
set -e
[[ $PRODUCER_EXIT -eq 3 ]] || fail "producer exit $PRODUCER_EXIT; expected 3 (interrupted)"
FLOOR="$(cat "$WORKDIR/acked")"
[[ "$FLOOR" -ge "$ACK_FLOOR" ]] || fail "implausible acked floor $FLOOR"
log "acknowledged floor at kill: $FLOOR records"

# Recover the murdered leader's directory and verify byte-for-byte that the
# floor survived. Verify also proves nothing above the recovered HWM is
# served: it reads records strictly below the HWM and must end exactly there.
STANDALONE_PID="$(start_standalone)"
"$VTOP_NODE" verify --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --expect-at-least "$FLOOR" > "$WORKDIR/logs/verify.log" 2>&1 \
  || { tail -3 "$WORKDIR/logs/verify.log" >&2; fail "acked records lost after kill -9"; }
log "recovery verified: $(grep verify_done "$WORKDIR/logs/verify.log")"

# The followers hold the replicated prefix too: each one's local committed
# offset must be at (or beyond) the acknowledged floor.
PROBE_CFG="$(emit_replica_probe_config)"
for n in 1 2; do
  STATUS="$("$VTOP_NODE" replica-status --client-config "$PROBE_CFG" \
    --addr "$(replica_addr "$n")")"
  LOCAL="${STATUS#*local_committed_offset=}"
  LOCAL="${LOCAL%% *}"
  NEXT="${STATUS##*next_offset=}"
  [[ "$LOCAL" -ge "$FLOOR" ]] || fail "follower $n committed offset $LOCAL < floor $FLOOR"
  [[ "$NEXT" -ge "$FLOOR" ]] || fail "follower $n next_offset $NEXT < floor $FLOOR"
done
log "both followers hold >= $FLOOR records; durability invariant holds"

stop_node_now "$STANDALONE_PID"
stop_node_now "$F1_PID"
stop_node_now "$F2_PID"
seal_and_verify_active recovered-leader "$WORKDIR/data-leader/range.active"
seal_and_verify_active follower-1 "$WORKDIR/data-follower-1/range.active"
seal_and_verify_active follower-2 "$WORKDIR/data-follower-2/range.active"
