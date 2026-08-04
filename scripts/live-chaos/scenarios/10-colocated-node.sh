#!/usr/bin/env bash
# Scenario 10 — one process, both planes (#215).
#
# Every other scenario runs the deployment nobody actually uses: six processes
# for three machines, a `meta` and a `data` invocation each. This one runs the
# co-located form the issue exists for — `vtop-node node` hosting a metadata
# voter and a data replica in one process — and asserts the properties
# co-location claims:
#
#   1. ONE observability endpoint serves both planes: the same /metrics scrape
#      carries metadata Raft state and broker offsets;
#   2. /readyz is the CONJUNCTION of the roles — it reports ready, and both
#      planes then demonstrably serve (admin RPCs and quorum-of-one produce);
#   3. shared fate is real: killing the process takes both planes down, and a
#      restart recovers the data plane byte-exact.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

RECORDS="${CHAOS_COLOCATED_RECORDS:-500}"
BATCH="${CHAOS_COLOCATED_BATCH:-50}"
require_integer_in_range CHAOS_COLOCATED_RECORDS "$RECORDS" 1 100000000
require_integer_in_range CHAOS_COLOCATED_BATCH "$BATCH" 1 "$RECORDS"

# --- one process, both roles -------------------------------------------------
CN=$(start_colocated_node 1)
log "colocated node up: $CN"

# The shared gate must open — and once it does, BOTH planes must answer, or
# the conjunction the runner promises is a lie.
await_ready "$(data_metrics_addr 0)" "colocated-node"
log "single /readyz reports ready"

meta_admin 1 init --members 1 > /dev/null || fail "metadata plane did not accept init"
LEADER_ID="$(wait_meta_leader 1)"
log "metadata plane serves admin RPCs (leader: node $LEADER_ID)"

# --- one scrape, both planes -------------------------------------------------
assert_metric_present "$(data_metrics_addr 0)" "vtop_meta_raft_state"
assert_metric_present "$(data_metrics_addr 0)" "vtop_broker_local_committed_offset"
log "one /metrics endpoint carries metadata Raft state AND broker offsets"

# --- the data plane serves ---------------------------------------------------
CLIENT_CFG="$(emit_client_config)"
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(colocated_native_addr 1)" \
  --records "$RECORDS" --batch "$BATCH" --durability local-fsync \
  --acked-file "$WORKDIR/acked" > "$WORKDIR/logs/produce.log" 2>&1 \
  || fail "produce against the colocated data plane failed (see $WORKDIR/logs/produce.log)"
ACKED="$(cat "$WORKDIR/acked")"
[[ "$ACKED" -eq "$RECORDS" ]] || fail "acked $ACKED of $RECORDS records"
log "data plane acknowledged all $ACKED records"

# --- shared fate -------------------------------------------------------------
kill9_pid "$CN"
stop_node_now "$CN"
CN2=$(start_colocated_node 1)
await_ready "$(data_metrics_addr 0)" "colocated-node-restarted"
"$VTOP_NODE" verify --client-config "$CLIENT_CFG" --addr "$(colocated_native_addr 1)" \
  --expect-at-least "$ACKED" > "$WORKDIR/logs/verify.log" 2>&1 \
  || fail "post-restart verify failed (see $WORKDIR/logs/verify.log)"
log "process killed and restarted; every acknowledged record survived, byte-exact"

stop_node_now "$CN2"
seal_and_verify_active colocated "$WORKDIR/data-colocated-1/range.active"
log "sealed artifact verifies offline"
