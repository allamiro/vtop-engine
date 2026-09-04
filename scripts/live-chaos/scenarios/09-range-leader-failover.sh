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
# A READ THAT DID NOT ANSWER IS NOT AN OFFSET OF ZERO. This value chooses
# which replica takes the range, so a failed scrape must stop the scenario
# rather than nominate the replica whose endpoint merely went quiet — that
# would hand the range to the follower WITHOUT the acknowledged floor, in a
# scenario whose whole claim is that no acknowledged record is lost.
# WAITED FOR, not sampled once: the gauge is legitimately absent for a moment
# when a non-blocking read meets the append path, and right after this there
# may be no earlier sample standing (review). A single read would abort a
# healthy run; a deadline decides that a replica truly never answered.
F1_OFFSET="$(await_follower_committed_offset 1)" \
  || fail "follower 1 served no committed-offset sample after the kill within \
${PROGRESS_TIMEOUT_SECONDS}s, so which replica holds the acknowledged floor is unknown; \
promoting on that would be a guess"
F2_OFFSET="$(await_follower_committed_offset 2)" \
  || fail "follower 2 served no committed-offset sample after the kill within \
${PROGRESS_TIMEOUT_SECONDS}s, so which replica holds the acknowledged floor is unknown; \
promoting on that would be a guess"
if [[ "$F1_OFFSET" -ge "$F2_OFFSET" ]]; then
  PROMOTE_N=1 PROMOTE_UUID="$FOLLOWER1_UUID" PROMOTE_PID="$F1"
  OTHER_N=2 OTHER_PID="$F2"
else
  PROMOTE_N=2 PROMOTE_UUID="$FOLLOWER2_UUID" PROMOTE_PID="$F2"
  OTHER_N=1 OTHER_PID="$F1"
fi
log "follower offsets: f1=$F1_OFFSET f2=$F2_OFFSET; promoting follower $PROMOTE_N"

# THE GAP THE PROMOTION OPENS, recorded because it decides whether the rest of
# this run can converge at all (#340). Promoting the replica that holds the
# acknowledged floor is correct and not negotiable — it is what keeps the
# floor readable — but when the two followers sit at different offsets it
# leaves the SURVIVOR below the new leader's tip by the difference. A follower
# only applies a batch whose expected_base_offset equals its own next_offset,
# and a leader can only close such a gap out of its in-memory retransmission
# buffer, which a process that has just been promoted never filled for offsets
# it never sent. Promotion advances the committed watermark; it does not
# truncate the new leader's log back to the proven floor. So a non-zero gap
# here is the configuration the node config documents as unable to catch up in
# place — "behind a leader promoted after the gap opened". That is repairable
# rather than terminal: `vtopctl node repair --seal-tail` into an empty
# directory seals the leader's tail so the transferred prefix reaches its
# position, and the replica catches up from there (#306, shipped).
# BOTH OFFSETS ARE KNOWN-GOOD BY CONSTRUCTION, and this arithmetic depends on
# it: an unread offset that had been substituted with 0 would fabricate a gap
# out of two replicas standing level, and the verdict below would then declare
# — definitively, ahead of every other piece of evidence — that quorum was
# impossible at any deadline (review). It cannot happen here because the reads
# above fail the scenario by name rather than returning a number nobody read.
# If those reads ever stop doing that, this line stops being sound.
REJOIN_GAP=$((F1_OFFSET > F2_OFFSET ? F1_OFFSET - F2_OFFSET : F2_OFFSET - F1_OFFSET))
if [[ "$REJOIN_GAP" -gt 0 ]]; then
  log "NOTE: follower $OTHER_N is $REJOIN_GAP record(s) behind the promoted replica, so it \
starts below the new leader's tip (the adopt-window gap)"
fi

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
#
# EVERY ATTEMPT ALSO SAMPLES THE LEADER'S VIEW OF THE REJOINING FOLLOWER —
# the evidence #340 needed and did not have. That failure reads
#
#   produce rejected: Overloaded quorum not reached: 0 follower ack(s)
#
# and despite the error code this is NOT admission control refusing work: by
# the time it is emitted the leader has already appended AND fsynced those
# records into its own log, and what expired is its bounded wait for follower
# acknowledgements (2s per follower, not configurable on a deployed node). The
# count is of ack tasks that returned true, and it reaches zero through five
# paths a produce error cannot tell apart — no stream, a stream that timed
# out, a follower that acked but still trails the leader's tip, a refusal
# (fencing looks exactly like silence here), and back-pressure inside the
# leader. "The follower is slow on a disk somebody else is saturating" and
# "the follower never rejoined" are two of those, they point at different
# bugs, and the soak that followed #340 could not choose between them because
# it never reproduced: 40/40 passed. So the next occurrence chooses for
# itself, the way #318's lease wait was taught to report what it observed
# rather than only its last read.
#
# The discriminator is published by the leader, per follower: `connected` is
# whether it holds a replication stream at all, `durable_offset` is what that
# follower has acknowledged. The promoted leader serves them on the range's
# own metrics address and is configured with exactly ONE follower, so an
# unlabelled read cannot pick up the wrong one.
LEADER_METRICS="$(data_metrics_addr 0)"
stream_seen=0
stream_last=""
durable_first=""
durable_prev=""
durable_last=""
durable_seen=0
last_read_answered=0
reads_ok=0
reads_failed=0

# ONE SCRAPE PER SAMPLE, not one per gauge (review). Two reasons, and both
# matter more than the line count:
#
#   COHERENCE — `connected` and `durable_offset` describe the same follower at
#   the same instant only if they come out of the same response. Read
#   separately they can straddle a reconnect, and the verdict below is built
#   out of exactly that pair.
#
#   THE BOUND — this runs inside a loop whose deadline the failure message
#   quotes. Two scrapes at curl's 5s ceiling could push a run 10s past the
#   bound it advertises, per attempt, and the low CHAOS_PROGRESS_TIMEOUT_SECONDS
#   values the harness supports are where that reads worst. One scrape, and its
#   timeout clamped to what is actually left, means diagnosing the failure
#   cannot outlast the failure's own deadline.
#
# It also sidesteps a hazard in the shared helper: `metric_value` pipes curl
# into `grep -m1`, and grep exiting on the first match can SIGPIPE a curl still
# writing a large /metrics body — which under `pipefail` marks a GOOD sample as
# a failed read. Parsing a captured body has no pipeline to fail.
sample_rejoining_follower() { # <seconds-left>
  local budget="$1" body connected durable
  # NO TIME LEFT MEANS NO OBSERVATION, and the freshness flag has to say so.
  # The final produce attempt can itself cross the deadline, and returning here
  # while `last_read_answered` still holds the PREVIOUS attempt's 1 would let
  # the verdict describe the follower "at the deadline" from a sample taken
  # before a multi-second produce (review).
  if [[ "$budget" -le 0 ]]; then
    last_read_answered=0
    return 0
  fi
  [[ "$budget" -gt 5 ]] && budget=5
  # A SCRAPE THAT FAILED IS IGNORANCE, not a follower that is disconnected —
  # counted apart so a run whose leader stopped answering can never be
  # reported as a stream that never came up (the distinction #318 exists to
  # preserve, and the reason follower_committed_offset is not used here: it
  # collapses an unreachable node into a legitimate offset of 0).
  if ! body="$(curl -s --max-time "$budget" "http://$LEADER_METRICS/metrics" 2>/dev/null)" \
    || [[ -z "$body" ]]; then
    reads_failed=$((reads_failed + 1))
    last_read_answered=0
    return 0
  fi
  reads_ok=$((reads_ok + 1))
  last_read_answered=1
  # TWO FACTS, not one. `stream_seen` is sticky — it answers "was there ever a
  # stream", which is what separates a rejoin that never happened from one that
  # did. `stream_last` is the state at the most recent answering scrape, which
  # is what any claim ABOUT THE DEADLINE has to rest on: a follower that
  # connected, advanced, and then dropped would otherwise be reported as still
  # catching up on the strength of history, while the newest coherent sample
  # said its stream was down (review).
  connected="$(awk '/^vtop_broker_follower_connected/ {v=$NF} END {print v}' <<< "$body")"
  if [[ -n "$connected" ]]; then
    stream_last="${connected%.*}"
    if [[ "$stream_last" -ge 1 ]]; then
      stream_seen=1
    fi
  fi
  # The durable offset is tracked SEPARATELY from the scrape that carried it:
  # a leader that answers without publishing this gauge yet is not a follower
  # whose offset stood still, and only `durable_seen` can tell those apart
  # (review).
  durable="$(awk '/^vtop_broker_follower_durable_offset/ {v=$NF} END {print v}' <<< "$body")"
  if [[ -n "$durable" ]]; then
    durable_seen=1
    if [[ -z "$durable_first" ]]; then
      durable_first="$durable"
    fi
    durable_prev="$durable_last"
    durable_last="$durable"
  fi
}

produce_deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
attempts=0
until "$VTOP_NODE" produce --client-config "$VERIFY_CFG" --addr "$(native_addr)" \
  --records "$BATCH" --batch "$BATCH" \
  --durability quorum > "$WORKDIR/logs/produce-after-failover.log" 2>&1; do
  attempts=$((attempts + 1))
  sample_rejoining_follower "$((produce_deadline - SECONDS))"
  if [[ $SECONDS -ge $produce_deadline ]]; then
    if [[ "$REJOIN_GAP" -gt 0 ]]; then
      # NAMED FIRST, because it explains the observation rather than competing
      # with it: with the survivor below the new leader's tip, every replica
      # append is refused for a base offset that cannot match, and no deadline
      # can change that. This is the answer to the fork #340 was filed with —
      # it is "never", and for a documented reason rather than a slow disk.
      verdict="follower $OTHER_N was $REJOIN_GAP record(s) behind the promoted replica when \
the range moved, so it starts below the new leader's tip and no batch it is offered can match \
its next_offset. A promoted leader cannot backfill that gap — it can only retransmit what its \
own buffer holds, and it never sent those offsets — so this run could not have reached quorum \
at ANY deadline (the catch-up limitation the node config documents; the remedy is a fresh \
repair with --seal-tail, #306). Read the \
promotion line above: this is not a timing flake"
    elif [[ "$reads_ok" -eq 0 ]]; then
      verdict="the leader's metrics endpoint never answered, so NOTHING is known about the \
rejoining follower — do not read this as a stall"
    elif [[ "$durable_seen" -eq 0 ]]; then
      # The leader answered but never published this follower's durable offset,
      # so there is no progress reading to reason from. Saying "it never moved"
      # here would invent an observation nobody made (review).
      verdict="the leader answered $reads_ok scrape(s) but never published a durable offset \
for follower $OTHER_N, so how far it got is UNKNOWN — the stream was $(
        [[ "$stream_seen" -eq 1 ]] && echo "up at least once" || echo "never seen up"
      ), and that is all this run observed"
    elif [[ "$stream_seen" -eq 0 ]]; then
      verdict="the leader never held a replication stream to follower $OTHER_N, so it could \
not have acked: this is #340's REJOIN-STALL fork, not a bound that is too tight"
    elif [[ "$last_read_answered" -eq 0 ]]; then
      # The samples on record may be minutes old: two advancing readings
      # followed by a run of failed scrapes would otherwise be reported as
      # "still advancing when the clock ran out", which is a claim about a
      # moment nobody observed (review). What is known is what was seen, and
      # when it stopped being seen.
      verdict="the last scrape(s) did not answer, so the follower's state AT the deadline is \
unknown; the last readings that did answer were durable offset ${durable_first:-unknown} -> \
${durable_last:-unknown} with the stream $(
        [[ "$stream_seen" -eq 1 ]] && echo "seen up" || echo "never seen up"
      ) ($reads_ok answered, $reads_failed failed)"
    elif [[ "$stream_last" == "0" ]]; then
      # The newest coherent reading says the stream is DOWN, whatever the
      # offsets did earlier. A drop is not the same failure as never having
      # connected, and it is not a bound that is too tight either — so it is
      # named as itself rather than folded into a movement verdict.
      verdict="the leader had a stream to follower $OTHER_N earlier — its durable offset \
reached ${durable_last:-unknown} — but the last scrape that answered says the stream is DOWN. \
A stream that came up and then dropped is neither fork of #340: look for what disconnected it \
(the follower restarting, or a refusal tearing the session down) before suspecting the disk"
    elif [[ -n "$durable_prev" && "$durable_last" != "$durable_prev" ]]; then
      # RECENT movement, not any movement (review). A follower that advanced
      # once and then stopped satisfies "last != first" for the rest of the
      # wait, and reporting that as catching-up would send the next reader
      # after a slow disk while the replica sat still.
      verdict="the stream was up and follower $OTHER_N's durable offset was STILL advancing \
when the clock ran out ($durable_first -> $durable_last, moving between the last two \
samples): this is #340's TOO-TIGHT-BOUND fork — durable and catching up, just not inside \
${PROGRESS_TIMEOUT_SECONDS}s — not a stall"
    elif [[ -n "$durable_first" && "$durable_last" != "$durable_first" ]]; then
      verdict="follower $OTHER_N's durable offset advanced $durable_first -> $durable_last \
and then STOPPED before the clock ran out: AMBIGUOUS between a follower that stalled after \
partial progress and one merely between acks. Compare the last value against the leader's \
own tip before choosing a suspect"
    else
      verdict="the leader held a stream to follower $OTHER_N and its durable offset never \
moved off ${durable_first:-unknown}: connected and acknowledging nothing, which is NEITHER \
fork #340 was filed with. A refusal looks exactly like this — a follower on the wrong epoch \
misses silently — so suspect the restart-at-the-granted-epoch step above before the disk"
    fi
    fail "post-failover produce never reached quorum within ${PROGRESS_TIMEOUT_SECONDS}s over \
$attempts failed attempt(s): $verdict ($reads_ok metric read(s) answered, $reads_failed failed). \
Last refusal: $(tail -1 "$WORKDIR/logs/produce-after-failover.log")"
  fi
  sleep 0.2
done
log "the new leader acknowledged fresh quorum writes"

# THE WATERMARK NEVER REGRESSES ACROSS A TRANSITION (#240 slice 3): the
# promoted leader publishes only on a quorum-acked entry of its OWN epoch —
# the boundary marker its promotion fires, or the produce that just
# succeeded, whichever landed first — never on the probe's arithmetic
# alone. By this point one of those own-epoch entries has provably
# quorum-acked, so the published watermark must stand at or above every
# offset acknowledged before the kill. Deadline-polled like every read
# here; a regression is the §5.4.2 failure this arc exists to close.
await_metric_at_least "$(data_metrics_addr 0)" \
  vtop_broker_cluster_committed_offset "$ACKED" \
  || fail "the promoted leader's published watermark never reached the pre-kill \
acknowledged floor ($ACKED): the §5.4.2 non-regression is broken — records producers \
were told were durable sit above what the new leadership publishes"
log "the published watermark covers the pre-kill acknowledged floor: §5.4.2 non-regression holds"

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
