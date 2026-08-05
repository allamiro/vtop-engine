#!/usr/bin/env bash
# Scenario 11 — admin transport authorization (#238).
#
# The admin endpoint authenticates every client via mTLS, but until #238 it did
# not authorize: any certificate the CA had signed — including the ones every
# data node already holds to speak the replication protocol — could rewrite
# cluster membership or grant a range lease to an arbitrary holder. This
# scenario proves the policy is enforced by the running binary over the real
# transport, and, just as importantly, that enforcing it does not break the
# lease path that failover depends on.
#
# Invariants:
#   1. An operator certificate retains the full surface (membership changes).
#   2. A data-node certificate is REFUSED membership changes, and the refusal
#      says so rather than failing for some incidental reason.
#   3. A data-node certificate may still READ — an operator locked out of
#      status during an incident is a worse outcome than a node reading state
#      it could infer anyway.
#   4. A data node can still acquire its OWN range lease. This is the invariant
#      that matters most: the lease agent on every data node proposes through
#      this endpoint, so a policy that broke it would break failover itself.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

# Turn the policy on for every metadata node this scenario starts. Exported
# before the first start_meta_node so it lands in the emitted configs; every
# other scenario leaves it unset and keeps exercising the permissive endpoint.
export META_ADMIN_OPERATORS="vtop-admin"

start_meta_node 1 2 3 > /dev/null
start_meta_node 2 1 3 > /dev/null
start_meta_node 3 1 2 > /dev/null
meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
log "3-node group up under admin authorization, leader=$LEADER_ID"

# ---------------------------------------------------------------------------
# 1. The operator certificate keeps the full surface.
#
# All three are cluster-scoped under the policy — membership, topic lifecycle,
# and node registration — so this is the assertion that the policy does not
# simply refuse everything.
# ---------------------------------------------------------------------------
meta_admin "$LEADER_ID" add-learner --node-id 4 > /dev/null \
  || fail "the operator certificate must retain membership changes"
meta_admin "$LEADER_ID" create-topic \
  --name "$TOPIC" --topic-uuid "$TOPIC_UUID" --root-range-uuid "$RANGE_ID" > /dev/null \
  || fail "the operator certificate must retain topic creation"
for node in "$LEADER_UUID" "$FOLLOWER1_UUID" "$FOLLOWER2_UUID"; do
  meta_admin "$LEADER_ID" register-node \
    --node-uuid "$node" --addr "$REGISTER_HOST_PREFIX.1:$REGISTER_PORT" > /dev/null \
    || fail "the operator certificate must retain node registration ($node)"
done
log "operator cert: membership, topic, and node registration all accepted"

# ---------------------------------------------------------------------------
# 2. A data-node certificate is refused a membership change.
#
# `data-1` is the leader broker's certificate: CN is its node UUID, signed by
# the same CA as the operator cert. Before #238 this call succeeded.
# ---------------------------------------------------------------------------
refusal="$(meta_admin_as "$LEADER_ID" data-1 add-learner --node-id 5 2>&1)" && {
  fail "a data-node certificate must NOT be able to change cluster membership"
}
# The refusal must be the authorization check, not a connection error, a TLS
# mismatch, or a malformed request — any of which would let this assertion pass
# while the policy did nothing.
grep -qi "unauthorized" <<< "$refusal" \
  || fail "refusal must name authorization, got: $refusal"
log "data-node cert: add-learner refused as unauthorized"

# ---------------------------------------------------------------------------
# 3. The same certificate may still read.
# ---------------------------------------------------------------------------
meta_admin_as "$LEADER_ID" data-1 status > /dev/null \
  || fail "reads must stay open to any authenticated client"
log "data-node cert: status still permitted"

# ---------------------------------------------------------------------------
# 4. The lease path still works — the invariant failover depends on.
#
# The leader's lease agent proposes AcquireRangeLease through this same
# enforcing endpoint, presenting the very certificate refused in step 2. It
# must be permitted, because the holder it names is itself.
# ---------------------------------------------------------------------------
# Followers watch metadata for their epoch (#239), so they need no seeding —
# and their watchers authenticate to this very endpoint under the policy being
# tested, which makes this scenario cover the node-scoped read path too.
start_follower 1 "" "" "$LEADER_ID" > /dev/null
start_follower 2 "" "" "$LEADER_ID" > /dev/null
# Followers first: verified promotion (#223) needs a quorum of replica-status
# answers before the leader will serve.
start_leader_with_lease "$LEADER_ID" > /dev/null
epoch="$(await_lease_holder "$LEADER_ID" "$LEADER_UUID")"
[[ -n "$epoch" ]] || fail "leader did not acquire its own lease under authorization"
log "data-node cert: acquired its own range lease at epoch $epoch"

log "PASS: admin authorization enforced without breaking the lease path"
