#!/usr/bin/env bash
# Scenario 09 — range leader failover on real processes (#223).
#
# Every earlier data-plane scenario validates DURABILITY: kill the leader, and
# what was acknowledged survives. None of them validate FAILOVER, because until
# #223 there was nothing to fail over to — the range simply stopped.
#
# This one kills a range leader under sustained quorum produce and asserts that
# a follower takes the range, that nothing acknowledged is lost, and that the
# dead leader cannot come back and write.
#
# What it proves, in order:
#   1. a follower acquires the lease within the TTL and begins serving;
#   2. every record acknowledged before the mid-flight kill is still readable
#      after, including when the interrupted producer never learned its fate;
#   3. the new leader's committed boundary was established from a quorum, not
#      inherited by assumption;
#   4. the old leader, restarted, never reports ready and is refused with a
#      stale epoch — because it is FENCED, not merely unreachable;
#   5. every replica's sealed artifact independently verifies.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

RECORDS="${CHAOS_FAILOVER_RECORDS:-3000}"
BATCH="${CHAOS_FAILOVER_BATCH:-100}"
require_integer_in_range CHAOS_FAILOVER_RECORDS "$RECORDS" 1 100000000
require_integer_in_range CHAOS_FAILOVER_BATCH "$BATCH" 1 "$RECORDS"

# --- metadata plane ---------------------------------------------------------
M1=$(start_meta_node 1 2 3)
M2=$(start_meta_node 2 1 3)
M3=$(start_meta_node 3 1 2)
log "meta nodes up: $M1 $M2 $M3"
meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
log "meta leader elected: node $LEADER_ID"

# The range must exist in metadata before anyone can hold a lease on it.
meta_admin "$LEADER_ID" create-topic \
  --name "$TOPIC" --topic-uuid "$TOPIC_UUID" --root-range-uuid "$RANGE_ID" > /dev/null \
  || fail "could not create the topic in metadata"
for node in "$LEADER_UUID" "$FOLLOWER1_UUID" "$FOLLOWER2_UUID"; do
  meta_admin "$LEADER_ID" register-node \
    --node-uuid "$node" --addr "$REGISTER_HOST_PREFIX.1:$REGISTER_PORT" > /dev/null \
    || fail "could not register data node $node"
done
log "metadata knows the topic and all three data nodes"

# --- data plane, leadership driven by the metadata lease --------------------
# A follower refuses replica appends whose epoch differs from its configured
# one, and followers have no lease agent — so the harness starts them at the
# epoch metadata is ABOUT to mint. A fresh range's first acquisition always
# mints epoch 1; the assertion below fails loudly if that assumption breaks.
EXPECTED_FIRST_EPOCH=1
F1=$(start_follower 1 "" "$EXPECTED_FIRST_EPOCH")
F2=$(start_follower 2 "" "$EXPECTED_FIRST_EPOCH")
# Followers first: verified promotion (#223) needs a quorum of replica-status
# answers before the leader will serve, so a leader started into an empty
# replica set would refuse promotion and never report ready.
DL=$(start_leader_with_lease "$LEADER_ID")
log "data nodes up: leader=$DL followers=$F1,$F2"

EPOCH_BEFORE="$(await_lease_holder "$LEADER_ID" "$LEADER_UUID")"
[[ "$EPOCH_BEFORE" == "$EXPECTED_FIRST_EPOCH" ]] \
  || fail "first grant minted epoch $EPOCH_BEFORE, not $EXPECTED_FIRST_EPOCH; the followers \
were seeded with the wrong epoch and would refuse every replica append"
log "leader holds the range at epoch $EPOCH_BEFORE"

# --- produce, and kill the leader mid-flight --------------------------------
# The producer runs in the background and the kill lands once a floor of
# acknowledgements is on record — while batches are still in flight. Killing
# after a completed produce would only re-prove durability; the race between
# an interrupted producer and a lease election is the thing this scenario adds.
CLIENT_CFG="$(emit_client_config_at_epoch "$EPOCH_BEFORE")"
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$RECORDS" --batch "$BATCH" --durability quorum \
  --acked-file "$WORKDIR/acked" > "$WORKDIR/logs/produce.log" 2>&1 &
PRODUCER=$!
KILL_FLOOR=$((RECORDS / 10))
[[ "$KILL_FLOOR" -ge 1 ]] || KILL_FLOOR=1
await_acked_floor "$WORKDIR/acked" "$KILL_FLOOR"

kill9_pid "$DL"
stop_node_now "$DL"
wait "$PRODUCER" || true # exit 3 = interrupted mid-stream, which is the point
ACKED="$(cat "$WORKDIR/acked")"
[[ "$ACKED" -gt 0 ]] || fail "nothing was acknowledged before the kill"
if [[ "$ACKED" -ge "$RECORDS" ]]; then
  log "WARNING: the producer finished all $RECORDS records before the kill landed; \
this run exercised durability but not an interrupted producer"
else
  log "range leader killed (SIGKILL) mid-flight; acknowledged floor: $ACKED of $RECORDS"
fi

# --- a follower must take the range -----------------------------------------
# Promote the follower that holds the acknowledged floor. Quorum produce only
# guarantees the floor reached the leader plus SOME majority — for any given
# final batch the other follower may have been the fast quorum member — so
# promoting a fixed follower would fail runs where the old quorum behaved
# exactly as designed.
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

# The original follower process must be gone before another process opens the
# same active segment; a promotion is a handoff, not a second writer.
stop_node_now "$PROMOTE_PID"
NEW=$(start_promoted_follower "$PROMOTE_N" "$LEADER_ID")
EPOCH_AFTER="$(await_lease_holder "$LEADER_ID" "$PROMOTE_UUID" "$ELECTION_TIMEOUT_SECONDS")"
log "follower $PROMOTE_N took the range at epoch $EPOCH_AFTER"

[[ "$EPOCH_AFTER" -gt "$EPOCH_BEFORE" ]] \
  || fail "failover epoch $EPOCH_AFTER did not advance past $EPOCH_BEFORE; the old leader is not fenced"

# The remaining follower still validates replica appends at the OLD epoch, and
# no follower-side watcher exists yet to teach it the new one — so the harness
# restarts it at the granted epoch, standing in for that watcher. Without this
# the new leader could never catch it up, and the quorum boundary could sit
# below the acknowledged floor forever.
stop_node_now "$OTHER_PID"
OTHER_PID=$(start_follower "$OTHER_N" "" "$EPOCH_AFTER")
log "follower $OTHER_N restarted at epoch $EPOCH_AFTER to rejoin replication"

# --- nothing acknowledged may be lost ---------------------------------------
# Resume traffic on the new leader first. This is not decoration: nothing else
# advances the proven boundary. Verified promotion published the floor the
# 2-of-2 quorum could vouch for at that instant — which sits below $ACKED
# whenever the restarted follower is the lagging one — and quorum-acking a
# fresh batch is what forces the replication stream to push the backlog to
# that follower and move the boundary past the pre-kill floor.
#
# The batch is written under a BUMPED PRODUCER EPOCH — the mechanism for
# resuming a producer whose session did not survive. The interrupted sequence
# space cannot be resumed blind: sequences must be gap-free, and promotion
# truncates to the verified quorum floor, which sits at neither $ACKED nor
# $RECORDS, so any first-sequence the harness picks is a guess the broker
# rejects as a gap. Nor can the client sidestep it with a fresh producer id —
# the broker requires producer id to equal the authenticated principal.
# Bumping the producer epoch opens a fresh sequence space (state is keyed on
# `(producer_id, producer_epoch)`) and fences the pre-failover session, which
# is the invariant a real client relies on after losing its own.
#
# (This previously sent --first-sequence $RECORDS under producer epoch 1,
# reasoning that starting past the interrupted range avoids idempotent dedupe.
# It does — but it also creates a sequence gap, which the broker refuses. That
# assertion could only pass when the "mid-flight" kill happened to land after
# the producer had already written all $RECORDS records, i.e. when the thing
# the scenario exists to test did not actually happen.)
VERIFY_PRODUCER_EPOCH=2
VERIFY_CFG="$(emit_client_config_at_epoch "$EPOCH_AFTER" "$VERIFY_PRODUCER_EPOCH")"
# Retried until the new leader can reach quorum. The follower was restarted
# moments ago and the leader's replication stream to it is established
# asynchronously, so the first attempt can legitimately find zero followers
# durable — that is the stream still connecting, not a durability failure. A
# single attempt here made the scenario depend on winning that race.
produce_deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
until "$VTOP_NODE" produce --client-config "$VERIFY_CFG" --addr "$(native_addr)" \
  --records "$BATCH" --batch "$BATCH" \
  --durability quorum > "$WORKDIR/logs/produce-after-failover.log" 2>&1; do
  [[ $SECONDS -lt $produce_deadline ]] \
    || fail "post-failover produce never reached quorum within ${PROGRESS_TIMEOUT_SECONDS}s: \
$(tail -1 "$WORKDIR/logs/produce-after-failover.log")"
  sleep 0.2
done
log "the new leader acknowledged fresh quorum writes"

# Retried while the boundary settles; the produce above guarantees it arrives.
#
# Content is byte-verified through $ACKED — exactly the records the pre-kill
# producer was told were durable, which is the claim being made. The fresh
# batch above it came from a bumped producer epoch whose sequences restart at
# 0, so its bytes are not derivable from its offsets; it is still checked for
# offset contiguity and against the high watermark.
await_verified_floor "$VERIFY_CFG" "$(native_addr)" "$ACKED" "$ACKED"
log "every one of the $ACKED acknowledged records survived the failover, byte-exact"

# --- the dead leader must not be able to write again ------------------------
# Restarting it against its old directory is the realistic operator mistake:
# the process comes back believing it still leads — carrying its OLD epoch,
# exactly as a real restart would. It gets its own ports (the promoted
# follower owns the range's real ones), and the assertion waits until its
# lease agent has actually OBSERVED the rival grant before probing: only then
# is the refusal the one fencing provides, rather than a trivial mismatch a
# blank epoch would produce. `/readyz` never going ready is asserted too: a
# load balancer must never route to it.
OLD=$(start_fenced_old_leader "$LEADER_ID" "$EPOCH_BEFORE")
await_metric_at_least "$(data_metrics_addr 3)" vtop_broker_meta_fencing_epoch "$EPOCH_AFTER" \
  "restarted-old-leader never observed the rival grant"
await_not_ready "$(data_metrics_addr 3)" "restarted-old-leader"
assert_fenced_produce "$(old_leader_native_addr)" "$EPOCH_BEFORE" \
  "the restarted old leader accepted a produce under its stale epoch"
log "the restarted old leader observed the rival grant at epoch $EPOCH_AFTER and is fenced"
stop_node_now "$OLD"

# --- artifacts must independently verify ------------------------------------
stop_node_now "$NEW"
stop_node_now "$OTHER_PID"
seal_and_verify_active new-leader "$WORKDIR/data-follower-$PROMOTE_N"
seal_and_verify_active other-follower "$WORKDIR/data-follower-$OTHER_N"
log "all surviving replica artifacts verify offline"
