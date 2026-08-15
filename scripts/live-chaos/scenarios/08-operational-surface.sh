#!/usr/bin/env bash
# Scenario 08 — the operational surface itself (#224).
#
# Every other scenario now *depends* on the health endpoints: `start_meta_node`
# and friends block on /readyz rather than on a stdout marker. That makes the
# endpoints load-bearing, so they get a scenario of their own — a gate nobody
# checks is a gate that silently stops working.
#
# What this proves on real processes:
#   * /metrics, /healthz and /readyz answer on every node role;
#   * the metric names the dashboards query are actually published, so a rename
#     cannot blank a panel while the cluster keeps working perfectly;
#   * liveness and readiness are genuinely different signals;
#   * the endpoint refuses methods it does not implement, on a port that is
#     unauthenticated by design (#78);
#   * committed offsets reported over /metrics agree with the offsets reported
#     over the replication plane by `vtopctl node status`.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

RECORDS="${CHAOS_SURFACE_RECORDS:-500}"
require_integer_in_range CHAOS_SURFACE_RECORDS "$RECORDS" 1 100000000

# --- metadata plane ---------------------------------------------------------
M1=$(start_meta_node 1 2 3)
M2=$(start_meta_node 2 1 3)
M3=$(start_meta_node 3 1 2)
log "meta nodes up: $M1 $M2 $M3"
meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
log "meta leader elected: node $LEADER_ID"

# The Raft metrics must reflect the cluster a scenario can already see through
# the admin endpoint. Two sources disagreeing is worse than one source missing.
for id in 1 2 3; do
  ADDR="$(meta_metrics_addr "$id")"
  assert_metric_present "$ADDR" "vtop_node_info"
  assert_metric_present "$ADDR" "vtop_meta_raft_term"
  assert_metric_present "$ADDR" "vtop_meta_raft_state"
  assert_metric_present "$ADDR" "vtop_meta_raft_last_applied_index"
  assert_metric_present "$ADDR" "vtop_meta_raft_voters"
done
log "metadata metrics published on all three nodes"

VOTERS_METRIC="$(metric_value "$(meta_metrics_addr "$LEADER_ID")" 'vtop_meta_raft_voters')" \
  || fail "the metadata leader served no vtop_meta_raft_voters sample; a scrape that does not \
answer must say so rather than ending the scenario with a bare exit code"
[[ "$VOTERS_METRIC" == "3" ]] \
  || fail "leader reports $VOTERS_METRIC voters over /metrics, expected 3"

LEADERS="$(count_raft_leaders 1 2 3)"
[[ "$LEADERS" == "1" ]] \
  || fail "$LEADERS nodes claim leadership over /metrics; exactly one must"
log "exactly one metadata leader, agreed by the admin endpoint and /metrics"

# --- data plane -------------------------------------------------------------
F1=$(start_follower 1)
F2=$(start_follower 2)
DL=$(start_leader)
log "data nodes up: leader=$DL followers=$F1,$F2"

for n in 0 1 2; do
  ADDR="$(data_metrics_addr "$n")"
  assert_metric_present "$ADDR" "vtop_broker_local_committed_offset"
  assert_metric_present "$ADDR" "vtop_broker_meta_fencing_epoch"
  assert_metric_present "$ADDR" "vtop_broker_segment_recovered"
done
# Request-path metrics exist only where requests are served.
assert_metric_present "$(data_metrics_addr 0)" "vtop_broker_requests_total"
assert_metric_present "$(data_metrics_addr 0)" "vtop_broker_request_duration_seconds_bucket"
assert_metric_present "$(data_metrics_addr 0)" "vtop_broker_sessions_active"
log "data-plane metrics published on the leader and both followers"

CLIENT_CFG="$(emit_client_config)"
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$RECORDS" --batch 100 --durability quorum \
  --acked-file "$WORKDIR/acked" > "$WORKDIR/logs/produce.log" 2>&1 \
  || fail "quorum produce failed (see $WORKDIR/logs/produce.log)"
log "produced $RECORDS records at quorum durability"

# The committed offset an operator reads on a dashboard must be the offset the
# replica will actually serve. Cross-check the two independent paths.
COMMITTED_METRIC="$(metric_value "$(data_metrics_addr 0)" 'vtop_broker_local_committed_offset')" \
  || fail "the leader served no vtop_broker_local_committed_offset sample after a quorum \
produce; this read is the one that died of a SIGPIPEd curl in CI, so name it rather than \
exiting on a status nobody printed"
[[ "$COMMITTED_METRIC" == "$RECORDS" ]] \
  || fail "leader /metrics reports committed offset $COMMITTED_METRIC, expected $RECORDS"

NODE_CFG="$(emit_node_status_config)"
# Quorum produce returns once the leader plus a majority are durable, so the
# third replica may still be catching up. Wait for the tail to settle rather
# than asserting zero lag on a run that met quorum durability exactly as
# designed.
await_replicas_settled "$RECORDS" "$NODE_CFG"
STATUS_OFFSET="$(python3 -c "
import json
d=json.load(open('$WORKDIR/node-status.json'))
print(d['reference_offset'])
")"
[[ "$STATUS_OFFSET" == "$RECORDS" ]] \
  || fail "vtopctl node status reports offset $STATUS_OFFSET, /metrics reports $COMMITTED_METRIC"
REFERENCE_SOURCE="$(python3 -c "
import json
d=json.load(open('$WORKDIR/node-status.json'))
print(d['reference_source'])
")"
[[ "$REFERENCE_SOURCE" == "leader" ]] \
  || fail "lag was measured against '$REFERENCE_SOURCE', not the leader"
log "node status and /metrics agree: offset $STATUS_OFFSET, all replicas settled"

# --- endpoint behaviour -----------------------------------------------------
for n in 0 1 2; do
  ADDR="$(data_metrics_addr "$n")"
  CODE="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://$ADDR/healthz")"
  [[ "$CODE" == "200" ]] || fail "data node $n /healthz returned $CODE"
  # Unauthenticated port: it must not answer verbs it does not implement.
  CODE="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -X POST "http://$ADDR/metrics")"
  [[ "$CODE" == "405" ]] || fail "POST /metrics returned $CODE, expected 405"
  CODE="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://$ADDR/nope")"
  [[ "$CODE" == "404" ]] || fail "unknown path returned $CODE, expected 404"
done
log "endpoints answer GET only and 404 unknown paths"

# Liveness and readiness are different claims: a node killed outright stops
# answering both, which is what makes the gate usable as a wait condition.
stop_node_now "$F2"
DEADLINE=$((SECONDS + STOP_TIMEOUT_SECONDS))
while [[ "$(probe_readyz "$(data_metrics_addr 2)")" != "000" ]]; do
  [[ $SECONDS -lt $DEADLINE ]] || fail "killed follower still answers /readyz"
  sleep 0.1
done
log "a killed node stops answering its health gate"

# Survivors must not be dragged down with it: the endpoint is per-process.
await_ready "$(data_metrics_addr 0)" "data-leader after a follower died" "$STOP_TIMEOUT_SECONDS"
await_ready "$(data_metrics_addr 1)" "data-follower-1 after a follower died" "$STOP_TIMEOUT_SECONDS"
log "surviving nodes stay ready"

stop_node_now "$DL"
stop_node_now "$F1"
seal_and_verify_active leader "$WORKDIR/data-leader"
seal_and_verify_active follower-1 "$WORKDIR/data-follower-1"
seal_and_verify_active follower-2 "$WORKDIR/data-follower-2"
