#!/usr/bin/env bash
# Scenario 03 — replace a live metadata voter with a fresh, empty node.
#
# Node 4 joins with an empty disk, catches up as a learner (snapshot / log
# replication over real TCP), is promoted while node 2 is demoted, and the
# retired node's death then costs the group nothing. This is Raft voter
# replacement only; sealed data-replica repair/retirement remains separate.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

start_meta_node 1 2 3 4 > /dev/null
start_meta_node 2 1 3 4 > /dev/null
start_meta_node 3 1 2 4 > /dev/null
meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
log "3-node group up, leader=$LEADER_ID"

# Build history worth replicating before the new node exists.
for n in $(seq 32 47); do
  propose_register "$LEADER_ID" "$(printf '%02x' "$n")" > /dev/null
done
LEADER_APPLIED="$(meta_status_field "$LEADER_ID" "['last_applied']['index']")"
log "committed history through applied=$LEADER_APPLIED"

start_meta_node 4 1 2 3 > /dev/null
meta_admin "$LEADER_ID" add-learner --node-id 4 > /dev/null

deadline=$((SECONDS + ELECTION_TIMEOUT_SECONDS))
while true; do
  APPLIED="$(meta_status_field 4 "['last_applied']['index']" 2>/dev/null || echo 0)"
  [[ "$APPLIED" == "null" ]] && APPLIED=0
  [[ "$APPLIED" -ge "$LEADER_APPLIED" ]] && break
  [[ $SECONDS -lt $deadline ]] || fail "learner 4 stuck at applied=$APPLIED < $LEADER_APPLIED"
  sleep 0.3
done
log "empty node 4 caught up as learner (applied=$APPLIED)"

# Promote 4, demote 2 — replacement is verified-caught-up BEFORE the old
# voter is removed, never after.
meta_admin "$LEADER_ID" change-membership --voters 1,3,4 > /dev/null
VOTERS="$(meta_status_field 1 "['membership']['voters']")"
[[ "$VOTERS" == "[1, 3, 4]" ]] || fail "voters after replacement: $VOTERS"
log "membership now [1,3,4]"

# Retiring the replaced node must not dent availability: kill it hard and
# keep committing on a full quorum of the new set. Node 2 was the second
# process started, so its pid is line 2 of the pidfile.
kill -9 "$(sed -n '2p' "$WORKDIR/pids")" 2>/dev/null || true
LEADER_ID="$(wait_meta_leader 1 3 4)"
propose_register "$LEADER_ID" 60 > /dev/null
propose_register "$LEADER_ID" 61 > /dev/null
log "node 2 killed post-replacement; group still commits via leader $LEADER_ID"
