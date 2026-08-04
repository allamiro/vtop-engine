#!/usr/bin/env bash
# Scenario 02 — shrink metadata membership 5 -> 3 with the leader removed.
#
# Invariants: the group elects a new leader from the survivors, the removed
# ex-leader is fenced — proposals through it are refused, it never reports
# itself Leader again — and the survivors keep committing.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

for id in 1 2 3 4 5; do
  peers=()
  for peer in 1 2 3 4 5; do [[ "$peer" != "$id" ]] && peers+=("$peer"); done
  start_meta_node "$id" "${peers[@]}" > /dev/null
done
meta_admin 1 init --members 1,2,3,4,5 > /dev/null
OLD_LEADER="$(wait_meta_leader 1 2 3 4 5)"
log "5-node group up, leader=$OLD_LEADER"

propose_register "$OLD_LEADER" 10 > /dev/null

# Voters = everyone except the current leader; proposed THROUGH that leader.
SURVIVORS=()
for id in 1 2 3 4 5; do [[ "$id" != "$OLD_LEADER" ]] && SURVIVORS+=("$id"); done
KEEP="$(IFS=,; echo "${SURVIVORS[*]:0:3}")"
meta_admin "$OLD_LEADER" change-membership --voters "$KEEP" > /dev/null
log "membership changed to [$KEEP], removing leader $OLD_LEADER"

NEW_LEADER=""
deadline=$((SECONDS + ELECTION_TIMEOUT_SECONDS))
while [[ $SECONDS -lt $deadline ]]; do
  for id in ${KEEP//,/ }; do
    state="$(meta_status_field "$id" "['server_state']" 2>/dev/null || echo unknown)"
    if [[ "$state" == "Leader" ]]; then NEW_LEADER="$id"; break 2; fi
  done
  sleep 0.3
done
[[ -n "$NEW_LEADER" ]] || fail "no survivor took leadership after the shrink"
[[ "$NEW_LEADER" != "$OLD_LEADER" ]] || fail "removed leader still leads"
log "survivor $NEW_LEADER took leadership"

# Survivors still commit.
propose_register "$NEW_LEADER" 11 > /dev/null
log "post-shrink commit through new leader succeeded"

# The fenced ex-leader must refuse proposals (it is no longer a voter and
# must not silently commit). Accept either an explicit refusal or, at worst,
# proof that it no longer claims leadership.
if meta_admin "$OLD_LEADER" register-node \
    --node-uuid "cccccccc-0000-0000-0000-000000000012" \
    --addr "$REGISTER_HOST_PREFIX.18:$REGISTER_PORT" > /dev/null 2>&1; then
  fail "removed leader $OLD_LEADER accepted a proposal after losing membership"
fi
OLD_STATE="$(meta_status_field "$OLD_LEADER" "['server_state']" 2>/dev/null || echo unreachable)"
[[ "$OLD_STATE" != "Leader" ]] || fail "removed node still reports Leader"
log "fenced ex-leader refuses proposals (state=$OLD_STATE)"
