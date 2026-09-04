#!/usr/bin/env bash
# Scenario 14 — failover with NO restarts: the role follows the lease (#284).
#
# Every scenario above this one performs failover by restarting processes
# with rewritten configs — the role flip, the port takeover, the follower
# list, the epoch floor. That is a harness move, not an operator one, and it
# is exactly what a Kubernetes pod cannot do: an address never moves between
# pods. Candidate mode retires the choreography: three identical configs,
# and the role is decided by the lease, live.
#
# What it proves, in order:
#   1. three candidates start from ONE config shape and exactly one takes
#      the range — the winner is not scripted;
#   2. quorum produce lands against the winner's OWN native address;
#   3. the leader is SIGKILLed and a SURVIVOR takes the range with no
#      process started, no config rewritten, and no port moved;
#   4. the new leader serves quorum produce at the new epoch on its own
#      address, and every record acknowledged before the kill is intact;
#   5. the two roles the survivors ended in are visible in their logs as
#      role transitions, not restarts.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

# A v2 range (#240): the promotion boundary marker this scenario asserts is
# a record only a v2 frame can carry under an identity consumers are
# shielded from — a v1 range keeps the pre-marker publication path, and
# the marker line awaited below would never appear.
export CHAOS_SEGMENT_FORMAT=v2

require_binaries
init_workdir

RECORDS="${CHAOS_CANDIDATE_RECORDS:-600}"
BATCH="${CHAOS_CANDIDATE_BATCH:-50}"
require_integer_in_range CHAOS_CANDIDATE_RECORDS "$RECORDS" 1 100000000
require_integer_in_range CHAOS_CANDIDATE_BATCH "$BATCH" 1 65536

# --- metadata plane ---------------------------------------------------------
M1=$(start_meta_node 1 2 3)
M2=$(start_meta_node 2 1 3)
M3=$(start_meta_node 3 1 2)
log "meta nodes up: $M1 $M2 $M3"
meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
log "meta leader elected: node $LEADER_ID"
meta_admin "$LEADER_ID" create-topic \
  --name "$TOPIC" --topic-uuid "$TOPIC_UUID" --root-range-uuid "$RANGE_ID" > /dev/null \
  || fail "could not create the topic in metadata"
# The lease is granted only to a REGISTERED node — an acquisition naming an
# unknown holder is refused, silently on the agent side (a refused acquire is
# indistinguishable from a lost race by design). Found the hard way: three
# healthy candidates polling forever while metadata refused every one of them.
for node in "$LEADER_UUID" "$FOLLOWER1_UUID" "$FOLLOWER2_UUID"; do
  meta_admin "$LEADER_ID" register-node \
    --node-uuid "$node" --addr "$REGISTER_HOST_PREFIX.1:$REGISTER_PORT" > /dev/null \
    || fail "could not register data node $node"
done
log "metadata knows the topic and all three candidates"

# --- three identical candidates ---------------------------------------------
C1=$(start_candidate 1 "$LEADER_ID")
C2=$(start_candidate 2 "$LEADER_ID")
C3=$(start_candidate 3 "$LEADER_ID")
log "three candidates up from one config shape: $C1 $C2 $C3"

read -r HOLDER EPOCH <<<"$(await_any_lease_holder "$LEADER_ID")"
WINNER="$(candidate_by_uuid "$HOLDER")"
log "candidate $WINNER ($HOLDER) took the range at epoch $EPOCH — unscripted"
await_log_line "$WORKDIR/logs/data-candidate-$WINNER.log" \
  "data_node_role_changed role=leader" \
  "the winner must record its promotion as a ROLE CHANGE, not a restart"

# --- quorum produce against the winner's OWN address ------------------------
ACKED_FILE="$WORKDIR/acked"
CLIENT_CFG="$(emit_client_config_at_epoch "$EPOCH")"
# Retried under the deadline: the FOLLOWING candidates adopt the granted
# epoch from their own next metadata poll, and a produce racing that poll
# legitimately finds replicas that refuse — the stream connecting, not a
# durability failure (scenario 09's lesson, inherited).
produce_deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
until "$VTOP_NODE" produce --client-config "$CLIENT_CFG" \
  --addr "$(candidate_native_addr "$WINNER")" \
  --records "$RECORDS" --batch "$BATCH" --durability quorum \
  --acked-file "$ACKED_FILE" > "$WORKDIR/logs/produce.log" 2>&1; do
  [[ $SECONDS -lt $produce_deadline ]] \
    || fail "quorum produce to the winner never reached quorum within \
${PROGRESS_TIMEOUT_SECONDS}s: $(tail -3 "$WORKDIR/logs/produce.log")"
  sleep 0.2
done
await_acked_floor "$ACKED_FILE" "$RECORDS"
ACKED="$(cat "$ACKED_FILE")"
log "$ACKED records acknowledged under quorum at candidate $WINNER's own address"

# The published mark before the kill (#240, §5.4.2): the new leader must
# never publish below it. Read after the produce completed, so it is the
# settled mark rather than a racing lower bound; a failed read is a failure
# of the scenario, never a silent zero.
HWM_BEFORE="$(metric_value "$(data_metrics_addr "$((WINNER + 10))")" \
  vtop_broker_cluster_committed_offset)" \
  || fail "the leader's published high-water mark could not be read before the kill"
HWM_BEFORE="${HWM_BEFORE%.*}"
log "published high-water mark before the kill: $HWM_BEFORE"

# --- kill the leader; a survivor takes the range, nothing restarts ----------
case "$WINNER" in
  1) VICTIM_PID="$C1" ;;
  2) VICTIM_PID="$C2" ;;
  3) VICTIM_PID="$C3" ;;
esac
kill9_pid "$VICTIM_PID"
log "leader (candidate $WINNER) SIGKILLed mid-hold"

# The dead leader's unexpired lease is still on record — correctly — so the
# wait is for the holder to CHANGE, with a deadline covering the lapse plus
# an election round.
read -r NEW_HOLDER NEW_EPOCH <<<"$(await_lease_holder_changed "$LEADER_ID" "$HOLDER" \
  $((LEASE_DURATION_MS / 1000 + ELECTION_TIMEOUT_SECONDS)))"
[[ "$NEW_EPOCH" -gt "$EPOCH" ]] \
  || fail "a new hold must mint a newer epoch: old $EPOCH, new $NEW_EPOCH"
NEW_LEADER="$(candidate_by_uuid "$NEW_HOLDER")"
log "candidate $NEW_LEADER ($NEW_HOLDER) took the range at epoch $NEW_EPOCH — no restart"

await_log_line "$WORKDIR/logs/data-candidate-$NEW_LEADER.log" \
  "data_node_role_changed role=leader" \
  "the survivor must have BECOME the leader in place; a restart would be scenario 09"

# --- the new leader serves, on its own address, and nothing acked was lost --
# A BUMPED PRODUCER EPOCH, not just the new fencing epoch (review): the
# fresh batch reuses sequence numbers 0.., and under the original producer
# epoch the broker deduplicates them against the first batch — the produce
# would "succeed" while proving nothing about fresh quorum writes. Scenario
# 09's post-promotion ritual, inherited.
CLIENT_CFG2="$(emit_client_config_at_epoch "$NEW_EPOCH" 2)"
produce_deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
until "$VTOP_NODE" produce --client-config "$CLIENT_CFG2" \
  --addr "$(candidate_native_addr "$NEW_LEADER")" \
  --records "$BATCH" --batch "$BATCH" --durability quorum \
  --acked-file "$WORKDIR/acked-after" > "$WORKDIR/logs/produce-after.log" 2>&1; do
  [[ $SECONDS -lt $produce_deadline ]] \
    || fail "post-failover quorum produce never reached quorum within \
${PROGRESS_TIMEOUT_SECONDS}s: $(tail -3 "$WORKDIR/logs/produce-after.log")"
  sleep 0.2
done
await_acked_floor "$WORKDIR/acked-after" "$BATCH"
log "quorum produce resumed at epoch $NEW_EPOCH on candidate $NEW_LEADER's own address"

# Content is verifiable only through $ACKED: the post-failover batch was
# written under a bumped producer epoch with sequences restarting at 0, so
# its bytes are not reconstructible from offsets — structure is still
# checked through the tail (await_verified_floor's own contract).
# One boundary marker per promoted epoch, invisible to the consumer (#240):
# the current epoch bounds how many offsets the view may skip.
CHAOS_MAX_OFFSET_GAPS="$NEW_EPOCH" \
  await_verified_floor "$CLIENT_CFG2" "$(candidate_native_addr "$NEW_LEADER")" "$ACKED" "$ACKED"
log "every one of the $ACKED pre-kill acknowledged records is intact after the failover"

# --- a published high-water mark never regresses (#240, §5.4.2) -------------
# In-place promotion is the same protocol as a restart's: the survivor that
# took the range appended a marker of its new epoch and published only once
# a quorum held it — the line in its log is the evidence — and what it
# published covers what the killed leader had already published.
await_log_line "$WORKDIR/logs/data-candidate-$NEW_LEADER.log" \
  "boundary_marker_published epoch=$NEW_EPOCH" \
  "the promoted candidate must publish its boundary through a quorum-acked marker of its \
own epoch, never by trusting the probe's count"
await_metric_at_least "$(data_metrics_addr "$((NEW_LEADER + 10))")" \
  vtop_broker_cluster_committed_offset "$HWM_BEFORE" \
  "the high-water mark REGRESSED across the in-place failover: the killed leader had \
published $HWM_BEFORE and the survivor publishes less"
log "the survivor published its boundary through its own epoch's marker, at or above the \
$HWM_BEFORE the killed leader had published"

# --- a candidate that cannot be measured cannot be operated ----------------
# Pinned here because this suite is the composition proof and this gap walked
# straight through it: candidate mode registered no role collector, so
# vtop_broker_local_committed_offset — the metric this very harness reads to
# pick a promotion target, and the one every dashboard and the k8s smoke
# read — did not exist on a candidate at all. Nothing in the suite asked, so
# nothing noticed until a live cluster did.
#
# Asked of BOTH roles: the leader that just took the range, and a survivor
# that is following it. One collector answers for whichever role holds the
# range, and the names must not change when the role does — that is what
# lets a panel built for a statically-rendered range keep working here.
# THE SURVIVOR, derived — never guessed (review). The candidates are 1, 2, 3,
# so the one that is neither the SIGKILLed WINNER nor the NEW_LEADER is
# 6 minus the two. Naming a fixed ordinal here would have curled a dead
# endpoint whenever the election came out differently, failing the scenario
# for the crime of electing another pod — the exact class of bug this suite
# exists to catch, in this suite.
FOLLOWING=$(( 6 - WINNER - NEW_LEADER ))
for n in "$NEW_LEADER" "$FOLLOWING"; do
  # DEADLINE-POLLED, because a follower can still be applying the tail when
  # the leader's own verification returns: quorum means a MAJORITY acked, so
  # one replica legitimately trails. A one-shot read here would test the
  # timing rather than the replication. `absent` and "too low" fail with
  # different messages, which is the distinction that matters: the first
  # means candidate mode exports no role collector at all (the #284
  # regression this pins), the second means replication is behind.
  await_metric_at_least "$(data_metrics_addr "$((n + 10))")" \
    vtop_broker_local_committed_offset "$ACKED" \
    "candidate $n (a replica nobody can measure is one nobody can operate; \
this very suite reads this metric to choose a promotion target)"
done
await_metric_equals "$(data_metrics_addr "$((NEW_LEADER + 10))")" \
  vtop_broker_candidate_leading 1 \
  "the candidate holding the range must say so — who serves the range has to be \
answerable from metrics alone"
await_metric_equals "$(data_metrics_addr "$((FOLLOWING + 10))")" \
  vtop_broker_candidate_leading 0 \
  "a following candidate must not claim the range, or metrics cannot tell the \
holder from its followers"
# And the authorization gauge separates them too: adopting the holder's epoch
# is not holding the lease.
await_metric_equals "$(data_metrics_addr "$((NEW_LEADER + 10))")" \
  vtop_broker_lease_active 1 "the holder is the authorized leaseholder"
await_metric_equals "$(data_metrics_addr "$((FOLLOWING + 10))")" \
  vtop_broker_lease_active 0 \
  "a following candidate adopts the holder's epoch and activates its view at it, \
so only the role keeps this from reporting three leaseholders for one range"
log "both roles are observable through one collector: offsets exported, and the holder \
is identifiable from metrics alone"

log "PASS"
