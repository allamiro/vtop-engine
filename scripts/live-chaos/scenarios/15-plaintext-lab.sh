#!/usr/bin/env bash
# 15 — the plaintext lab (#294): every plane without TLS, on loopback, run
# through the whole harness once, and every refusal the mode carries proved
# by a real process.
#
# Slices 1–5 of #294 gave each plane a plaintext mode and the node its
# transport knobs. This scenario is the deployment surface those knobs were
# for — a loopback lab that needs no certificates — and it is the only test
# of the whole thing at once: a metadata group whose peer and admin planes
# are plaintext, a static leader and two followers whose replica and native
# planes are plaintext, vtopctl and the node's own client dialing each the
# way it listens.
#
# The refusals matter as much as the happy path, because plaintext is safe
# only where the binary keeps it: a plaintext listener asked to bind off
# loopback must not start; a role that promotes (a leased replicated leader)
# must not start on a plaintext replica plane, since fences are refused
# there; and a TLS client against a plaintext plane must fail rather than
# be quietly served — cross-mode is an error on both sides.
set -euo pipefail
export CHAOS_TRANSPORT=plaintext
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

RECORDS="${CHAOS_PLAINTEXT_RECORDS:-2000}"
require_integer_in_range CHAOS_PLAINTEXT_RECORDS "$RECORDS" 1 100000000

M1=$(start_meta_node 1 2 3)
M2=$(start_meta_node 2 1 3)
M3=$(start_meta_node 3 1 2)
log "meta nodes up on plaintext peer and admin planes: $M1 $M2 $M3"
meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
log "meta leader elected over the plaintext peer plane: node $LEADER_ID"
grep -q "PLAINTEXT and UNAUTHENTICATED" "$WORKDIR/logs/meta-1.log" \
  || fail "a plaintext admin endpoint must announce itself as unauthenticated at startup"
log "the admin plane says what plaintext admits"

F1=$(start_follower 1)
F2=$(start_follower 2)
DL=$(start_leader)
log "data nodes up on plaintext replica and native planes: leader=$DL followers=$F1,$F2"
grep -q "native endpoint .* is PLAINTEXT" "$WORKDIR/logs/data-leader.log" \
  || fail "a plaintext native endpoint must announce itself at startup"

CLIENT_CFG="$(emit_client_config)"
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$RECORDS" --batch 100 --durability quorum \
  --acked-file "$WORKDIR/acked" > "$WORKDIR/logs/produce.log" 2>&1 \
  || fail "quorum produce over plaintext failed (see $WORKDIR/logs/produce.log)"
[[ "$(cat "$WORKDIR/acked")" -eq "$RECORDS" ]] || fail "acked $(cat "$WORKDIR/acked") != $RECORDS"
"$VTOP_NODE" verify --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --expect-at-least "$RECORDS" > "$WORKDIR/logs/verify.log" 2>&1 \
  || fail "verify over plaintext failed (see $WORKDIR/logs/verify.log)"
log "quorum produce + byte-exact verify over plaintext: $RECORDS records"

# A quorum acknowledges the leader and ONE follower (review): the other may
# still be applying, so both are awaited under the progress deadline before
# their offsets are read.
await_replicas_settled "$RECORDS" "$(emit_node_status_config)"
PROBE_CFG="$(emit_replica_probe_config)"
for n in 1 2; do
  STATUS="$("$VTOP_NODE" replica-status --client-config "$PROBE_CFG" --addr "$(replica_addr "$n")")"
  LOCAL="${STATUS#*local_committed_offset=}"
  LOCAL="${LOCAL%% *}"
  [[ "$LOCAL" -eq "$RECORDS" ]] || fail "follower $n committed offset $LOCAL != $RECORDS over plaintext"
done
log "both followers hold all $RECORDS records, read over the plaintext replica plane"
"$VTOPCTL" node status --config "$(emit_node_status_config)" > "$WORKDIR/logs/node-status.log" 2>&1 \
  || fail "vtopctl node status over plaintext failed (see $WORKDIR/logs/node-status.log)"
log "vtopctl node status reads every replica over plaintext"

# --- the refusals ----------------------------------------------------------

# A plaintext listener off loopback is refused before anything binds.
EXPOSED="$WORKDIR/data-follower-exposed.yaml"
sed "s|^replica_listen: .*|replica_listen: \"0.0.0.0:$(replica_port 3)\"|" \
  "$(emit_follower_config 3 "$WORKDIR/data-follower-exposed")" > "$EXPOSED"
if timeout 20 "$VTOP_NODE" data --config "$EXPOSED" > "$WORKDIR/logs/exposed.log" 2>&1; then
  fail "a plaintext replica listener on 0.0.0.0 started; it must be refused"
fi
grep -q "plaintext-on-any-interface" "$WORKDIR/logs/exposed.log" \
  || fail "the exposure refusal must name the way out (see $WORKDIR/logs/exposed.log)"
log "a plaintext listener off loopback is refused at startup, naming plaintext-on-any-interface"

# A role that promotes cannot serve a plaintext replica plane: fences are
# refused there, so the range could never be taken.
LEASED="$(emit_leader_config_with_lease "$LEADER_ID" plaintext-leased)"
# On its own ports and data dir (first run): sharing the live leader's, a
# taken port spoke first — non-zero, and no verdict — and the grep below is
# what told the two apart. Now the only thing that can refuse it is the knob.
sed -e "s|^data_dir: .*|data_dir: $WORKDIR/data-leader-leased|" \
    -e "s|^native_listen: .*|native_listen: \"$(old_leader_native_addr)\"|" \
    -e "s|^replica_listen: .*|replica_listen: \"$(replica_addr 3)\"|" \
    -e "s|^observability: .*|observability: { listen: \"$(data_metrics_addr 3)\" }|" \
    "$LEASED" | install_config "$LEASED"
if timeout 20 "$VTOP_NODE" data --config "$LEASED" > "$WORKDIR/logs/leased.log" 2>&1; then
  fail "a leased replicated leader started on a plaintext replica plane; it must be refused"
fi
grep -q "replica_transport: tls" "$WORKDIR/logs/leased.log" \
  || fail "the promotion refusal must name the knob (see $WORKDIR/logs/leased.log)"
log "a leased replicated leader is refused on a plaintext replica plane, naming replica_transport"

# Cross-mode is an error on both sides: a TLS client against the plaintext
# native plane, and a TLS operator against the plaintext admin plane.
TLS_CLIENT="$(CHAOS_TRANSPORT=tls emit_client_config_at_epoch "$FENCING_EPOCH" 2)"
if timeout 20 "$VTOP_NODE" produce --client-config "$TLS_CLIENT" --addr "$(native_addr)" \
  --records 10 --batch 10 --durability quorum > "$WORKDIR/logs/cross-native.log" 2>&1; then
  fail "a TLS client was served by the plaintext native plane"
fi
log "a TLS client against the plaintext native plane fails: $(tail -1 "$WORKDIR/logs/cross-native.log")"
TLS_ADMIN="$(CHAOS_TRANSPORT=tls emit_admin_config_as 1 data-1)"
if timeout 20 "$VTOPCTL" meta status --config "$TLS_ADMIN" > "$WORKDIR/logs/cross-admin.log" 2>&1; then
  fail "a TLS operator was served by the plaintext admin plane"
fi
log "a TLS operator against the plaintext admin plane fails: $(tail -1 "$WORKDIR/logs/cross-admin.log")"

stop_node_now "$DL"
stop_node_now "$F1"
stop_node_now "$F2"
seal_and_verify_active leader "$WORKDIR/data-leader"
log "PASS"
