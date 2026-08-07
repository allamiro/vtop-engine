#!/usr/bin/env bash
# Scenario 00 — cold bring-up of the full live cluster.
#
# 3 metadata Raft processes over real mTLS TCP on real disks: init, elect,
# commit proposals. 1 leader + 2 followers on the data plane: quorum produce,
# byte-exact verify, follower HWM probes. Every later scenario builds on this.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

RECORDS="${CHAOS_BRINGUP_RECORDS:-5000}"
BATCH="${CHAOS_BRINGUP_BATCH:-100}"
require_integer_in_range CHAOS_BRINGUP_RECORDS "$RECORDS" 1 100000000
require_integer_in_range CHAOS_BRINGUP_BATCH "$BATCH" 1 "$RECORDS"

# --- metadata plane ---------------------------------------------------------
M1=$(start_meta_node 1 2 3)
M2=$(start_meta_node 2 1 3)
M3=$(start_meta_node 3 1 2)
log "meta nodes up: $M1 $M2 $M3"

meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
log "meta leader elected: node $LEADER_ID"

for suffix in 01 02 03 04 05; do
  propose_register "$LEADER_ID" "$suffix" > /dev/null
done
APPLIED="$(meta_status_field "$LEADER_ID" "['last_applied']['index']")"
[[ "$APPLIED" -ge 5 ]] || fail "leader applied index $APPLIED after 5 proposals"

# A follower answers status and sees the same membership.
for id in 1 2 3; do
  VOTERS="$(meta_status_field "$id" "['membership']['voters']")"
  [[ "$VOTERS" == "[1, 2, 3]" ]] || fail "node $id sees voters $VOTERS"
done
log "metadata plane healthy: applied=$APPLIED voters=[1,2,3]"

# --- data plane --------------------------------------------------------------
F1=$(start_follower 1)
F2=$(start_follower 2)
DL=$(start_leader)
log "data nodes up: leader=$DL followers=$F1,$F2"

CLIENT_CFG="$(emit_client_config)"
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$RECORDS" --batch "$BATCH" --durability quorum \
  --acked-file "$WORKDIR/acked" > "$WORKDIR/logs/produce.log" 2>&1 \
  || fail "quorum produce failed (see $WORKDIR/logs/produce.log)"
ACKED="$(cat "$WORKDIR/acked")"
[[ "$ACKED" -eq "$RECORDS" ]] || fail "acked $ACKED != $RECORDS"

"$VTOP_NODE" verify --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --expect-at-least 5000 > "$WORKDIR/logs/verify.log" 2>&1 \
  || fail "verify failed (see $WORKDIR/logs/verify.log)"
log "quorum produce + byte-exact verify: 5000 records"

PROBE_CFG="$(emit_replica_probe_config)"
for n in 1 2; do
  STATUS="$("$VTOP_NODE" replica-status --client-config "$PROBE_CFG" \
    --addr "$(replica_addr "$n")")"
  log "follower $n: $STATUS"
  LOCAL="${STATUS#*local_committed_offset=}"
  LOCAL="${LOCAL%% *}"
  NEXT="${STATUS##*next_offset=}"
  [[ "$LOCAL" -eq 5000 ]] || fail "follower $n committed offset $LOCAL != 5000"
  [[ "$NEXT" -eq 5000 ]] || fail "follower $n next_offset $NEXT != 5000"
done
log "both followers hold all 5000 replicated records"

# Quiesce every replica, recover its durable boundary, seal it, and run the
# repository's independent offline verifier against each artifact.
stop_node_now "$DL"
stop_node_now "$F1"
stop_node_now "$F2"
seal_and_verify_active leader "$WORKDIR/data-leader"
seal_and_verify_active follower-1 "$WORKDIR/data-follower-1"
seal_and_verify_active follower-2 "$WORKDIR/data-follower-2"
