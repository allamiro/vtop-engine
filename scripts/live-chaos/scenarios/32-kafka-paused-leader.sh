#!/usr/bin/env bash
# 32 — a range leader that stalls past its lease and comes back is deposed:
# it acknowledges nothing, and a Kafka client that still aims at it is
# refused by name (#751).
#
# Scenarios 27 and 28 SIGKILL the leader, so its sockets close and a client
# learns at once that it is gone. A stall is the case a lease exists for: a
# leader that is paused (a GC or VM pause, a hung disk) keeps its sockets and
# its beliefs, a survivor legitimately takes the range while it sleeps, and
# then it wakes up still thinking it leads, with requests queued for it. Here
# the stall is SIGSTOP and SIGCONT, as scenario 23 does to a replica.
#
# System model: crash-recovery processes, a partially synchronous network,
# honest nodes. Safety rests on the fencing epoch the replicas check, not on
# the lease timers; the timers decide only how soon things move.
#
# Three candidates serve the Kafka gateway (as 27). ONE idempotent librdkafka
# producer (kcat, acks=all, stock timeouts) bootstrapped from all three
# streams paced records; after CHAOS_KAFKA_PAUSE_AFTER_ACKS acknowledgements
# the leader is SIGSTOPped. Then, in order:
#   1. a survivor takes the range at a newer epoch, changes role in place and
#      serves (liveness);
#   2. while the old leader is still stopped, ONE raw Kafka Produce is sent to
#      it, the request a client holding pre-failover metadata sends: the
#      kernel accepts the connection and queues the bytes, so the old leader
#      reads it the moment it wakes, before or after its lease agent notices
#      the lease is gone (that race is the point);
#   3. the producer resumes through the new leader with no restart: librdkafka
#      gives up on the stalled leader after its request timeout (30 s by
#      default), refreshes metadata from a survivor and carries on;
#   4. the old leader is SIGCONTed.
# What it asserts:
#   safety   the queued Produce is NOT acknowledged (any refusal, a closed
#            connection or no answer, never error code 0): an acknowledgement
#            from a node whose epoch the replicas have fenced would be a write
#            the new leader lacks;
#   safety   every record the producer was acknowledged for reads back
#            through the new leader once and in order, and nothing else does:
#            no record from either probe aimed at the old leader;
#   liveness after the resume the old leader's vtop_kafka_gateway_serving goes
#            to 0 within the lease renew plus poll interval (and a 2 s scrape
#            margin), it logs a role change to follower, its Metadata names
#            the new leader (a stock client bootstrapped from it alone is
#            redirected), and a second raw Produce to it is refused with
#            NOT_LEADER_OR_FOLLOWER;
#   liveness the producer finishes every record with no failed delivery and no
#            fatal idempotence error, and the time from the pause to its
#            acknowledgements resuming is printed and bounded by
#            CHAOS_KAFKA_PAUSE_LIMIT_SECONDS (90: the client's 30 s request
#            timeout, which a paused leader's open socket makes it wait out,
#            plus the lease and the stream's own pace);
#   control  the same raw Produce sent to the NEW leader is acknowledged, so a
#            refusal above is the old leader's answer and not a malformed
#            request.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

# A v2 range, as 27: the promotion boundary marker is a record the gateway's
# fetch hides from a Kafka consumer.
export CHAOS_SEGMENT_FORMAT=v2

require_binaries
KCAT="$(command -v kcat || command -v kafkacat || true)"
[[ -n "$KCAT" ]] || fail "scenario 32 needs kcat, librdkafka's client: brew install kcat / apt-get install kafkacat"
transport_plaintext && fail "scenario 32 needs TLS: candidates cannot run under CHAOS_TRANSPORT=plaintext"
init_workdir

RECORDS="${CHAOS_KAFKA_RECORDS:-4000}"
PAUSE_AFTER_ACKS="${CHAOS_KAFKA_PAUSE_AFTER_ACKS:-800}"
RESUME_ACKS="${CHAOS_KAFKA_RESUME_ACKS:-200}"
PACE_EVERY="${CHAOS_KAFKA_PACE_EVERY:-10}"
PACE_SLEEP="${CHAOS_KAFKA_PACE_SLEEP:-0.1}"
PAUSE_LIMIT_SECONDS="${CHAOS_KAFKA_PAUSE_LIMIT_SECONDS:-90}"
require_integer_in_range CHAOS_KAFKA_RECORDS "$RECORDS" 1000 10000000
RECORDS=$((10#$RECORDS))
require_integer_in_range CHAOS_KAFKA_PAUSE_AFTER_ACKS "$PAUSE_AFTER_ACKS" 1 "$((RECORDS / 2))"
PAUSE_AFTER_ACKS=$((10#$PAUSE_AFTER_ACKS))
require_integer_in_range CHAOS_KAFKA_RESUME_ACKS "$RESUME_ACKS" 1 "$((RECORDS / 4))"
RESUME_ACKS=$((10#$RESUME_ACKS))
require_integer_in_range CHAOS_KAFKA_PACE_EVERY "$PACE_EVERY" 1 "$RECORDS"
require_integer_in_range CHAOS_KAFKA_PAUSE_LIMIT_SECONDS "$PAUSE_LIMIT_SECONDS" 1 3600
PAUSE_LIMIT_SECONDS=$((10#$PAUSE_LIMIT_SECONDS))
[[ "$PACE_SLEEP" =~ ^[0-9]+([.][0-9]+)?$ ]] \
  || fail "CHAOS_KAFKA_PACE_SLEEP must be a non-negative decimal, got '$PACE_SLEEP'"

bring_up_kafka_candidates
OLD="$WINNER"
OLD_PID="${CAND_PIDS[$OLD]}"
OLD_PORT="$(candidate_kafka_port "$OLD")"
await_metric_equals "$(data_metrics_addr "$((OLD + 10))")" \
  vtop_kafka_gateway_serving 1 \
  "the candidate holding the range must report its Kafka gateway as serving"

# --- one idempotent producer, bootstrapped from all three --------------------
INPUT="$WORKDIR/kafka-input.txt"
awk -v n="$RECORDS" 'BEGIN {
  srand(32)
  for (i = 1; i <= n; i++) printf "k%06d:v%06d-%08x\n", i, i, int(rand() * 4294967295)
}' > "$INPUT"
ACKS="$WORKDIR/logs/kcat-produce.log"
: > "$ACKS"
KCAT_DEBUG=()
[[ -n "${CHAOS_KCAT_DEBUG:-}" ]] && KCAT_DEBUG=(-d "$CHAOS_KCAT_DEBUG")
# Stock settings except acks=all and idempotence, as 27: what a client does
# with no operator is what the number measures.
"$KCAT" -P -b "$KAFKA_BOOTSTRAP" -t "$TOPIC" -p 0 -K: -v -v \
  -X acks=all -X enable.idempotence=true \
  ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} \
  < <(awk -v every="$PACE_EVERY" -v pause="$PACE_SLEEP" \
    '{ print; fflush(); if (NR % every == 0) system("sleep " pause) }' "$INPUT") \
  2> >(stamp_stdin_ms > "$ACKS") &
PRODUCER=$!
echo "$PRODUCER" >> "$WORKDIR/pids"
log "one idempotent producer (pid $PRODUCER) streaming $RECORDS records, bootstrapped from $KAFKA_BOOTSTRAP"

acked_count() { grep -c 'Message delivered' "$ACKS" || true; }

deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
until [[ "$(acked_count)" -ge "$PAUSE_AFTER_ACKS" ]]; do
  kill -0 "$PRODUCER" 2>/dev/null \
    || fail "the producer ended before $PAUSE_AFTER_ACKS acknowledgements: $(tail -3 "$ACKS")"
  [[ $SECONDS -lt $deadline ]] \
    || fail "the producer reached only $(acked_count) acknowledgements in ${PROGRESS_TIMEOUT_SECONDS}s: $(tail -3 "$ACKS")"
  sleep 0.1
done

# --- pause the leader ----------------------------------------------------------
PAUSE_MS="$(wall_ms)"
pause_pid "$OLD_PID"
ACKS_AT_PAUSE="$(acked_count)"
log "candidate $OLD (the leader, pid $OLD_PID) SIGSTOPped with $ACKS_AT_PAUSE records acknowledged and the producer mid-stream"

# 1. A survivor takes the range. The paused process's lease is on record until
# it lapses, so this waits out the rest of it.
read -r NEW_HOLDER NEW_EPOCH <<<"$(await_lease_holder_changed "$(lease_readers_without "$OLD")" "$HOLDER" \
  $((LEASE_DURATION_MS / 1000 + ELECTION_TIMEOUT_SECONDS)))"
NEW_LEADER="$(candidate_by_uuid "$NEW_HOLDER")"
[[ "$NEW_EPOCH" -gt "$EPOCH" ]] || fail "a new hold must mint a newer epoch: old $EPOCH, new $NEW_EPOCH"
log "candidate $NEW_LEADER took the range at epoch $NEW_EPOCH while candidate $OLD sleeps at epoch $EPOCH"
await_log_line "$(candidate_log "$NEW_LEADER")" "data_node_role_changed role=leader" \
  "the survivor must become the leader in place"
await_log_line "$(candidate_log "$NEW_LEADER")" \
  "kafka gateway for range .* is now serving at epoch $NEW_EPOCH" \
  "the promoted candidate's gateway must say it is serving, at the new epoch"
await_metric_equals "$(data_metrics_addr "$((NEW_LEADER + 10))")" \
  vtop_kafka_gateway_serving 1 \
  "the new leader's gateway must report itself serving"

# 2. The request a client holding pre-failover metadata sends to the old
# leader, queued in its socket while it sleeps. Its wait outlasts the rest of
# the pause, the resume and a refusal's own deadline.
QUEUED="$WORKDIR/queued-probe.txt"
kafka_raw_produce "$OLD_PORT" "stale-queued" "sent-while-paused" \
  $((PAUSE_LIMIT_SECONDS + 2 * PROGRESS_TIMEOUT_SECONDS)) > "$QUEUED" 2>&1 &
QUEUED_PID=$!
echo "$QUEUED_PID" >> "$WORKDIR/pids"
log "one raw Produce queued for paused candidate $OLD on 127.0.0.1:$OLD_PORT"

# 3. The producer resumes through the new leader. RESUME_ACKS acknowledgements
# past the count at the pause cannot all be stragglers the old leader sent
# before it stopped: the pace allows a handful in flight.
deadline=$((SECONDS + PAUSE_LIMIT_SECONDS))
until [[ "$(acked_count)" -ge $((ACKS_AT_PAUSE + RESUME_ACKS)) ]]; do
  kill -0 "$PRODUCER" 2>/dev/null \
    || fail "the producer ended while the old leader was paused, with $(acked_count) of $RECORDS acknowledged: $(tail -3 "$ACKS")"
  [[ $SECONDS -lt $deadline ]] \
    || fail "the producer did not resume through candidate $NEW_LEADER within ${PAUSE_LIMIT_SECONDS}s of the pause: $(acked_count) acknowledged, $ACKS_AT_PAUSE at the pause: $(tail -3 "$ACKS")"
  sleep 0.2
done
log "the producer resumed through candidate $NEW_LEADER while candidate $OLD is still stopped: $(acked_count) acknowledged"
# Still stopped: the resume above must not have come from the old leader.
[[ "$(ps -o stat= -p "$OLD_PID" 2>/dev/null || true)" == T* ]] \
  || fail "candidate $OLD is no longer stopped before the scenario resumed it"

# 4. Wake the old leader.
CONT_MS="$(wall_ms)"
resume_pid "$OLD_PID"
log "candidate $OLD SIGCONTed $(( (CONT_MS - PAUSE_MS) / 1000 )) s after it was stopped"

# --- the deposed leader ----------------------------------------------------------
# Safety: the queued Produce must not be acknowledged. Every answer but an
# acknowledgement is acceptable here, and which one came is reported: it
# depends on whether the old gateway read the request before or after its
# lease agent demoted it.
set +e
wait "$QUEUED_PID"
set -e
QUEUED_ANSWER="$(tr -d '\n' < "$QUEUED")"
[[ -n "$QUEUED_ANSWER" ]] || fail "the queued probe printed nothing (see $QUEUED)"
[[ "$QUEUED_ANSWER" != acknowledged* ]] \
  || fail "candidate $OLD acknowledged a Produce after it resumed ($QUEUED_ANSWER), while candidate $NEW_LEADER holds the range at epoch $NEW_EPOCH: a deposed leader acknowledged a write"
log "the Produce queued while candidate $OLD slept was answered: $QUEUED_ANSWER"

# Liveness: the gateway stops claiming to serve, promptly. One renew and one
# poll is the most the lease agent can take to read the rival's hold once it
# runs again; 2 s more covers the scrape and this loop's own cadence.
NOT_SERVING_LIMIT_MS=$((LEASE_RENEW_MS + LEASE_POLL_MS + 2000))
await_metric_equals "$(data_metrics_addr "$((OLD + 10))")" \
  vtop_kafka_gateway_serving 0 \
  "the resumed old leader's gateway must stop claiming to serve"
NOT_SERVING_MS=$(( $(wall_ms) - CONT_MS ))
[[ "$NOT_SERVING_MS" -le "$NOT_SERVING_LIMIT_MS" ]] \
  || fail "candidate $OLD's gateway still served ${NOT_SERVING_MS} ms after it resumed, over the ${NOT_SERVING_LIMIT_MS} ms of one renew, one poll and the scrape margin"
await_log_line "$(candidate_log "$OLD")" "data_node_role_changed role=follower" \
  "the resumed old leader must change role to follower in place"
log "candidate $OLD stopped serving ${NOT_SERVING_MS} ms after it resumed and is a follower"

# What a stock client bootstrapped from the old leader alone is told now:
# the new leader, so it is redirected rather than refused.
assert_metadata_names_leader "$OLD" "$NEW_LEADER"
# And what a client that skips Metadata gets: refused by name.
STALE_ANSWER="$(kafka_raw_produce "$OLD_PORT" "stale-refused" "sent-after-resume" "$PROGRESS_TIMEOUT_SECONDS")"
[[ "$STALE_ANSWER" == "refused code=6 name=NOT_LEADER_OR_FOLLOWER" ]] \
  || fail "a Produce to the resumed old leader was answered '$STALE_ANSWER', expected NOT_LEADER_OR_FOLLOWER"
await_log_line "$(candidate_log "$OLD")" "Answered NOT_LEADER_OR_FOLLOWER" \
  "the old leader must log that it refused a produce for the range it no longer holds"
log "candidate $OLD names candidate $NEW_LEADER in Metadata and refuses a Produce with NOT_LEADER_OR_FOLLOWER"

# --- the same producer finishes ------------------------------------------------
finish_deadline=$((SECONDS + RECORDS * 3 / 100 + PROGRESS_TIMEOUT_SECONDS))
while kill -0 "$PRODUCER" 2>/dev/null; do
  [[ $SECONDS -lt $finish_deadline ]] \
    || fail "the producer had not finished $RECORDS records, with $(acked_count) acknowledged: $(tail -3 "$ACKS")"
  sleep 0.2
done
set +e
wait "$PRODUCER"
PRODUCER_EXIT=$?
set -e
for _ in $(seq 1 50); do
  [[ "$(acked_count)" -ge "$RECORDS" ]] && break
  sleep 0.1
done
[[ $PRODUCER_EXIT -eq 0 ]] \
  || fail "the producer exited $PRODUCER_EXIT after the pause: $(tail -3 "$ACKS")"
! grep -q 'Delivery failed' "$ACKS" \
  || fail "a delivery failed across the pause: $(grep -m 3 'Delivery failed' "$ACKS")"
! grep -qi 'fatal' "$ACKS" \
  || fail "the idempotent producer hit a fatal error: $(grep -im 3 'fatal' "$ACKS")"
[[ "$(acked_count)" -eq "$RECORDS" ]] \
  || fail "$(acked_count) of $RECORDS records were acknowledged"
log "the same producer finished all $RECORDS records across the pause with no failed delivery"

read -r FIRST_ACK_MS RESUME_MS GAP_MS < <(ack_outage_report "$ACKS" "$PAUSE_MS" 'Message delivered')
log "pause to first acknowledgement after it: ${FIRST_ACK_MS} ms; pause to the end of the outage the client saw: ${RESUME_MS} ms (longest silence ${GAP_MS} ms)"
[[ "$RESUME_MS" -le $((PAUSE_LIMIT_SECONDS * 1000)) ]] \
  || fail "the client waited ${RESUME_MS} ms from the pause for an acknowledgement, over the ${PAUSE_LIMIT_SECONDS} s limit"

# --- control: the same request to the leader is acknowledged ---------------------
CONTROL_ANSWER="$(kafka_raw_produce "$(candidate_kafka_port "$NEW_LEADER")" "control" "sent-to-the-leader" "$PROGRESS_TIMEOUT_SECONDS")"
[[ "$CONTROL_ANSWER" == acknowledged* ]] \
  || fail "the control Produce to the leader, candidate $NEW_LEADER, was answered '$CONTROL_ANSWER': the probe itself is wrong, so the refusals above prove nothing"
log "the same raw Produce sent to candidate $NEW_LEADER: $CONTROL_ANSWER"

# --- every acknowledged record reads back, once, in order, and nothing else -----
# Read to the end of the partition, not to a count: a record either probe had
# appended would sit after the stream, where a counted read would stop short.
OUTPUT="$WORKDIR/kafka-output.txt"
"$KCAT" -C -b "$KAFKA_BOOTSTRAP" -t "$TOPIC" -p 0 -o beginning -e \
  -K: -f '%k:%s\n' ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} > "$OUTPUT" 2>> "$WORKDIR/logs/kcat-consume.log" \
  || fail "consume failed: $(tail -3 "$WORKDIR/logs/kcat-consume.log")"
STALE="$(grep -c '^stale-' "$OUTPUT" || true)"
[[ "$STALE" -eq 0 ]] \
  || fail "$STALE record(s) sent to the deposed leader are in the new leader's log: $(grep -m 2 '^stale-' "$OUTPUT" | tr '\n' ' ')"
EXPECTED="$WORKDIR/kafka-expected.txt"
{ cat "$INPUT"; echo "control:sent-to-the-leader"; } > "$EXPECTED"
cmp -s "$EXPECTED" "$OUTPUT" \
  || { diff "$EXPECTED" "$OUTPUT" | head -5 >&2; fail "the records read back are not the $RECORDS acknowledged ones and the control, once each and in order"; }
log "all $RECORDS acknowledged records and the control read back through candidate $NEW_LEADER byte-exact, in order, once each; nothing sent to candidate $OLD"

printf 'KAFKA_PAUSED_LEADER client=librdkafka %s pause_to_first_ack_ms=%s pause_to_resume_ms=%s longest_silence_ms=%s paused_ms=%s cont_to_not_serving_ms=%s queued_probe="%s" stale_probe="%s" records=%s\n' \
  "$(kafka_failover_conditions "$OLD")" "$FIRST_ACK_MS" "$RESUME_MS" "$GAP_MS" \
  "$((CONT_MS - PAUSE_MS))" "$NOT_SERVING_MS" "$QUEUED_ANSWER" "$STALE_ANSWER" "$RECORDS" \
  > "$WORKDIR/paused-leader-result.txt"
cat "$WORKDIR/paused-leader-result.txt"
log "PASS"
