#!/usr/bin/env bash
# Scenario 06 — partition the metadata leader with packet filtering, heal, and
# assert stale-leader fencing.
#
# The whole scenario re-execs inside an unprivileged user+network namespace,
# where iptables drops both directions of the leader's Raft peer port while
# leaving its admin port reachable. The old process stays live throughout.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
if [[ "${VTOP_CHAOS_NETNS:-0}" != "1" ]]; then
  require_network_namespace
  exec unshare -Urnm env VTOP_CHAOS_NETNS=1 bash "$0"
fi
mount -t tmpfs tmpfs /run
mkdir -p /run/netns
export VTOP_META_HOST_PREFIX="${CHAOS_NET_PREFIX:-10.215.0}"
export VTOP_META_NETNS_PREFIX="vtop-meta-"

require_binaries
init_workdir

STALE_WRITE_TIMEOUT_SECONDS="${CHAOS_STALE_WRITE_TIMEOUT_SECONDS:-3}"
require_integer_in_range CHAOS_STALE_WRITE_TIMEOUT_SECONDS "$STALE_WRITE_TIMEOUT_SECONDS" 1 300

ip link add vtop-br0 type bridge
ip addr add 10.215.0.254/24 dev vtop-br0
ip link set vtop-br0 up
for id in 1 2 3; do
  ns="$VTOP_META_NETNS_PREFIX$id"
  host_veth="vtop-veth$id"
  ip netns add "$ns"
  ip link add "$host_veth" type veth peer name eth0 netns "$ns"
  ip link set "$host_veth" master vtop-br0
  ip link set "$host_veth" up
  ip netns exec "$ns" ip link set lo up
  ip netns exec "$ns" ip addr add "$(meta_host "$id")/24" dev eth0
  ip netns exec "$ns" ip link set eth0 up
done

start_meta_node 1 2 3 > /dev/null
start_meta_node 2 1 3 > /dev/null
start_meta_node 3 1 2 > /dev/null
meta_admin 1 init --members 1,2,3 > /dev/null
OLD_LEADER="$(wait_meta_leader 1 2 3)"
propose_register "$OLD_LEADER" 20 > /dev/null
OLD_TERM="$(meta_status_field "$OLD_LEADER" "['current_term']")"
log "leader=$OLD_LEADER term=$OLD_TERM"

LEADER_PEER_PORT="$(meta_peer_port "$OLD_LEADER")"
LEADER_NS="$VTOP_META_NETNS_PREFIX$OLD_LEADER"
META_PEER_FIRST_PORT="$(meta_peer_port 1)"
META_PEER_LAST_PORT="$(meta_peer_port 5)"
ip netns exec "$LEADER_NS" iptables -I OUTPUT 1 -p tcp \
  --dport "$META_PEER_FIRST_PORT:$META_PEER_LAST_PORT" -j DROP
ip netns exec "$LEADER_NS" iptables -I INPUT 1 -p tcp --dport "$LEADER_PEER_PORT" -j DROP
log "leader $OLD_LEADER isolated at Raft peer port $LEADER_PEER_PORT"

# A client write routed to the isolated stale leader must not complete. Keep
# its identity unique; after healing, successfully registering the same node
# through the new leader proves the timed-out request never committed.
set +e
timeout "$STALE_WRITE_TIMEOUT_SECONDS" "$VTOPCTL" --json meta register-node \
  --node-uuid "cccccccc-0000-0000-0000-000000000022" \
  --addr "$REGISTER_HOST_PREFIX.34:$REGISTER_PORT" --config "$(emit_admin_config "$OLD_LEADER")" \
  > "$WORKDIR/logs/stale-proposal.log" 2>&1
STALE_EXIT=$?
set -e
[[ $STALE_EXIT -ne 0 ]] || fail "isolated leader committed a proposal without quorum"

SURVIVORS=()
for id in 1 2 3; do [[ "$id" != "$OLD_LEADER" ]] && SURVIVORS+=("$id"); done

NEW_LEADER=""
deadline=$((SECONDS + ELECTION_TIMEOUT_SECONDS))
while [[ $SECONDS -lt $deadline ]]; do
  for id in "${SURVIVORS[@]}"; do
    state="$(meta_status_field "$id" "['server_state']" 2>/dev/null || echo unknown)"
    [[ "$state" == "Leader" ]] && { NEW_LEADER="$id"; break 2; }
  done
  sleep 0.3
done
[[ -n "$NEW_LEADER" ]] || fail "survivors did not elect a leader"
NEW_TERM="$(meta_status_field "$NEW_LEADER" "['current_term']")"
[[ "$NEW_TERM" -gt "$OLD_TERM" ]] || fail "new term $NEW_TERM not beyond old term $OLD_TERM"
propose_register "$NEW_LEADER" 21 > /dev/null
log "survivor $NEW_LEADER leads term $NEW_TERM and commits"

ip netns exec "$LEADER_NS" iptables -D OUTPUT -p tcp \
  --dport "$META_PEER_FIRST_PORT:$META_PEER_LAST_PORT" -j DROP
ip netns exec "$LEADER_NS" iptables -D INPUT -p tcp --dport "$LEADER_PEER_PORT" -j DROP
log "peer partition healed"

# The healed node must converge to follower of the new term.
deadline=$((SECONDS + ELECTION_TIMEOUT_SECONDS))
while true; do
  state="$(meta_status_field "$OLD_LEADER" "['server_state']" 2>/dev/null || echo unknown)"
  seen_leader="$(meta_status_field "$OLD_LEADER" "['current_leader']" 2>/dev/null || echo null)"
  if [[ "$state" != "Leader" && "$seen_leader" == "$NEW_LEADER" ]]; then
    break
  fi
  [[ $SECONDS -lt $deadline ]] || fail "healed leader stuck at state=$state leader=$seen_leader"
  sleep 0.3
done
log "old leader stepped down and follows node $NEW_LEADER"

# Post-heal, the group is one linearizable history: the ex-leader's applied
# index catches up to the new leader's.
TARGET="$(meta_status_field "$NEW_LEADER" "['last_applied']['index']")"
deadline=$((SECONDS + ELECTION_TIMEOUT_SECONDS))
while true; do
  APPLIED="$(meta_status_field "$OLD_LEADER" "['last_applied']['index']" 2>/dev/null || echo 0)"
  [[ "$APPLIED" == "null" ]] && APPLIED=0
  [[ "$APPLIED" -ge "$TARGET" ]] && break
  [[ $SECONDS -lt $deadline ]] || fail "ex-leader stuck at applied=$APPLIED < $TARGET"
  sleep 0.3
done
log "histories converged at applied=$APPLIED; no divergent commit survived the partition"

propose_register "$NEW_LEADER" 22 > /dev/null
log "timed-out stale proposal was absent; its identity committed cleanly through the new leader"

if meta_admin "$OLD_LEADER" register-node \
    --node-uuid "cccccccc-0000-0000-0000-000000000023" \
    --addr "$REGISTER_HOST_PREFIX.35:$REGISTER_PORT" > /dev/null 2>&1; then
  fail "former leader $OLD_LEADER accepted a proposal after stepping down"
fi
log "former leader refuses direct proposals after the heal"
