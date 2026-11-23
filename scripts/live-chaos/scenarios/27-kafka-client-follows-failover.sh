#!/usr/bin/env bash
# 27 — a Kafka client follows a failover with no restart and no config
# change (#751).
#
# Three candidates (#284), each serving the Kafka gateway and each listing all
# three as brokers in `kafka.brokers`, share one range. ONE idempotent
# librdkafka producer (`kcat`, enable.idempotence=true, acks=all) is started
# against a bootstrap list of all three and streams keyed records at a steady
# pace. When it has a few hundred acknowledgements the range's leader is
# SIGKILLed. Nothing else is touched: the client is not restarted and not
# reconfigured, the survivors are not restarted and not re-rendered. A
# survivor takes the lease and serves; the client's next metadata refresh
# names it as the partition's leader; the stream carries on.
#
# What it proves, in order:
#   1. a gateway on a candidate that does NOT hold the range lists all three
#      brokers and names the holder as the partition's leader in Metadata,
#      which is how a client that asked the wrong member is redirected (the
#      gauge vtop_kafka_gateway_serving is 1 on exactly one candidate);
#   2. the client's acknowledgements keep coming after the kill, from the
#      same process, with no failed delivery and no fatal idempotence error;
#   3. every record the client was acknowledged for reads back through the
#      new leader byte-exact, in produced order, and once (a retry that
#      reaches a log already holding the record is deduplicated; whether one
#      does depends on the timing of the kill, so the count is reported);
#   4. the survivor that took the range changed role in place (a role-change line,
#      not a start), and its gateway says it is serving;
#   5. the time from the kill to the first acknowledgement after it is
#      measured and printed (the outage the client saw), and bounded by
#      CHAOS_KAFKA_FAILOVER_LIMIT_SECONDS.
#
# The shape matches the benchmark's failover cell (#597) so the number is
# comparable with what the native client measured there.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

# A v2 range: the promotion boundary marker (#240) is a record the gateway's
# fetch must hide from a Kafka consumer, and this is the format a replicated
# range is recommended to use.
export CHAOS_SEGMENT_FORMAT=v2

require_binaries
KCAT="$(command -v kcat || command -v kafkacat || true)"
[[ -n "$KCAT" ]] || fail "scenario 27 needs kcat, librdkafka's client: brew install kcat / apt-get install kafkacat"
transport_plaintext && fail "scenario 27 needs TLS: candidates cannot run under CHAOS_TRANSPORT=plaintext"
init_workdir

RECORDS="${CHAOS_KAFKA_RECORDS:-4000}"
KILL_AFTER_ACKS="${CHAOS_KAFKA_KILL_AFTER_ACKS:-800}"
PACE_EVERY="${CHAOS_KAFKA_PACE_EVERY:-10}"
PACE_SLEEP="${CHAOS_KAFKA_PACE_SLEEP:-0.1}"
FAILOVER_LIMIT_SECONDS="${CHAOS_KAFKA_FAILOVER_LIMIT_SECONDS:-60}"
require_integer_in_range CHAOS_KAFKA_RECORDS "$RECORDS" 1000 10000000
RECORDS=$((10#$RECORDS))
require_integer_in_range CHAOS_KAFKA_KILL_AFTER_ACKS "$KILL_AFTER_ACKS" 1 "$((RECORDS / 2))"
KILL_AFTER_ACKS=$((10#$KILL_AFTER_ACKS))
require_integer_in_range CHAOS_KAFKA_PACE_EVERY "$PACE_EVERY" 1 "$RECORDS"
require_integer_in_range CHAOS_KAFKA_FAILOVER_LIMIT_SECONDS "$FAILOVER_LIMIT_SECONDS" 1 3600
# The pace reaches awk's system() as text: a non-negative decimal.
[[ "$PACE_SLEEP" =~ ^[0-9]+([.][0-9]+)?$ ]] \
  || fail "CHAOS_KAFKA_PACE_SLEEP must be a non-negative decimal, got '$PACE_SLEEP'"

bring_up_kafka_candidates
FOLLOWING_A=$(( WINNER % 3 + 1 ))
FOLLOWING_B=$(( FOLLOWING_A % 3 + 1 ))

# --- the gateway on a candidate that does not hold the range ----------------
# One candidate serves, the others refuse and say who does. The gauge is read
# per scrape, so a poll under a deadline, not a one-shot read.
await_metric_equals "$(data_metrics_addr "$((WINNER + 10))")" \
  vtop_kafka_gateway_serving 1 \
  "the candidate holding the range must report its Kafka gateway as serving"
for n in "$FOLLOWING_A" "$FOLLOWING_B"; do
  await_metric_equals "$(data_metrics_addr "$((n + 10))")" \
    vtop_kafka_gateway_serving 0 \
    "candidate $n follows, so its Kafka gateway must not claim to serve"
done
# What a client bootstrapped from a FOLLOWING candidate is told: every broker,
# and the holder as the partition's leader. A client that dialled the wrong
# member first is redirected by this answer alone.
# A member learns the holder from its next lease poll, so this is a deadline
# poll, not a one-shot read.
assert_metadata_names_leader() { # <bootstrap-candidate> <leader-candidate>
  local listing="" deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
  until grep -q '^ *3 brokers' <<<"$listing" \
    && grep -q "partition 0, leader $2," <<<"$listing"; do
    [[ $SECONDS -lt $deadline ]] \
      || fail "candidate $1 does not list three brokers and name candidate $2 (the lease holder) as the leader of partition 0: $listing"
    listing="$("$KCAT" -L -b "127.0.0.1:$(candidate_kafka_port "$1")" -t "$TOPIC" 2>&1)" || true
    sleep 0.2
  done
}
assert_metadata_names_leader "$FOLLOWING_A" "$WINNER"
assert_metadata_names_leader "$FOLLOWING_B" "$WINNER"
log "exactly one gateway serves: candidate $WINNER; candidates $FOLLOWING_A and $FOLLOWING_B list all three brokers and name $WINNER as the leader"

# --- one idempotent producer, bootstrapped from all three --------------------
INPUT="$WORKDIR/kafka-input.txt"
awk -v n="$RECORDS" 'BEGIN {
  srand(26)
  for (i = 1; i <= n; i++) printf "k%06d:v%06d-%08x\n", i, i, int(rand() * 4294967295)
}' > "$INPUT"

# librdkafka prints one "Message delivered" line per acknowledged record at
# verbosity 3 (kcat -v -v), on stderr; each is stamped as it arrives, which is
# when the client learned of the acknowledgement. Failures are logged at
# verbosity 1, so the same file carries them.
ACKS="$WORKDIR/logs/kcat-produce.log"
: > "$ACKS"
# Stock settings except the two the scenario is about: acks=all and
# idempotence. Not shortened timeouts, not a tuned retry count: what a client
# does with no operator, which is what the number measures.
KCAT_DEBUG=()
[[ -n "${CHAOS_KCAT_DEBUG:-}" ]] && KCAT_DEBUG=(-d "$CHAOS_KCAT_DEBUG")
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
until [[ "$(acked_count)" -ge "$KILL_AFTER_ACKS" ]]; do
  kill -0 "$PRODUCER" 2>/dev/null \
    || fail "the producer ended before $KILL_AFTER_ACKS acknowledgements: $(tail -3 "$ACKS")"
  [[ $SECONDS -lt $deadline ]] \
    || fail "the producer reached only $(acked_count) acknowledgements in ${PROGRESS_TIMEOUT_SECONDS}s: $(tail -3 "$ACKS")"
  sleep 0.1
done
log "$(acked_count) acknowledgements through candidate $WINNER; the stream has $((RECORDS - $(acked_count))) records to go"

# --- kill the leader ---------------------------------------------------------
KILL_MS="$(wall_ms)"
kill9_pid "${CAND_PIDS[$WINNER]}"
log "candidate $WINNER (the leader) SIGKILLed with the producer mid-stream"

read -r NEW_HOLDER NEW_EPOCH <<<"$(await_lease_holder_changed "$META_LEADER_ID" "$HOLDER" \
  $((LEASE_DURATION_MS / 1000 + ELECTION_TIMEOUT_SECONDS)))"
NEW_LEADER="$(candidate_by_uuid "$NEW_HOLDER")"
[[ "$NEW_EPOCH" -gt "$EPOCH" ]] || fail "a new hold must mint a newer epoch: old $EPOCH, new $NEW_EPOCH"
log "candidate $NEW_LEADER took the range at epoch $NEW_EPOCH, no restart"
await_log_line "$WORKDIR/logs/data-candidate-$NEW_LEADER.log" \
  "data_node_role_changed role=leader" \
  "the survivor must become the leader in place"
await_log_line "$WORKDIR/logs/data-candidate-$NEW_LEADER.log" \
  "kafka gateway for range .* is now serving at epoch $NEW_EPOCH" \
  "the promoted candidate's gateway must say it is serving, at the new epoch"
await_metric_equals "$(data_metrics_addr "$((NEW_LEADER + 10))")" \
  vtop_kafka_gateway_serving 1 \
  "the new leader's gateway must report itself serving"
# A survivor that follows now names the new leader: the answer a client's
# metadata refresh gets from either survivor.
for n in 1 2 3; do
  [[ "$n" == "$WINNER" || "$n" == "$NEW_LEADER" ]] && continue
  assert_metadata_names_leader "$n" "$NEW_LEADER"
done

# --- the same client finishes the stream -------------------------------------
# The client is given the whole stream's time plus the failover limit, and it
# is the SAME process: nothing here starts, signals or reconfigures it.
finish_deadline=$((SECONDS + FAILOVER_LIMIT_SECONDS + RECORDS * 3 / 100 + PROGRESS_TIMEOUT_SECONDS))
while kill -0 "$PRODUCER" 2>/dev/null; do
  [[ $SECONDS -lt $finish_deadline ]] \
    || fail "the producer had not finished $RECORDS records $((finish_deadline - SECONDS + FAILOVER_LIMIT_SECONDS))s after the kill, with $(acked_count) acknowledged: $(tail -3 "$ACKS")"
  sleep 0.2
done
set +e
wait "$PRODUCER"
PRODUCER_EXIT=$?
set -e
# The stamping process outlives kcat by the time it takes to drain its pipe.
for _ in $(seq 1 50); do
  [[ "$(acked_count)" -ge "$RECORDS" ]] && break
  sleep 0.1
done
[[ $PRODUCER_EXIT -eq 0 ]] \
  || fail "the producer exited $PRODUCER_EXIT after the failover: $(tail -3 "$ACKS")"
! grep -q 'Delivery failed' "$ACKS" \
  || fail "a delivery failed across the failover: $(grep -m 3 'Delivery failed' "$ACKS")"
! grep -qi 'fatal' "$ACKS" \
  || fail "the idempotent producer hit a fatal error: $(grep -im 3 'fatal' "$ACKS")"
[[ "$(acked_count)" -eq "$RECORDS" ]] \
  || fail "$(acked_count) of $RECORDS records were acknowledged"
log "the same producer finished all $RECORDS records across the failover with no failed delivery"

# --- the number: from the kill to the first acknowledgement after it --------
# Two readings of the same acknowledgement times. The first acknowledgement
# after the kill can be a straggler the dead leader sent just before it died,
# so the headline is the end of the longest silence that ENDS after the kill:
# the gap the client lived through.
read -r FIRST_ACK_MS RESUME_MS GAP_MS < <(ack_outage_report "$ACKS" "$KILL_MS" 'Message delivered')
LIMIT_MS=$((FAILOVER_LIMIT_SECONDS * 1000))
log "kill to first acknowledgement after it: ${FIRST_ACK_MS} ms; kill to the end of the outage the client saw: ${RESUME_MS} ms (longest silence ${GAP_MS} ms)"
[[ "$RESUME_MS" -le "$LIMIT_MS" ]] \
  || fail "the client waited ${RESUME_MS} ms from the kill for an acknowledgement, over the ${LIMIT_MS} ms limit"
printf 'KAFKA_FAILOVER client=librdkafka kill_to_first_ack_ms=%s kill_to_resume_ms=%s longest_silence_ms=%s records=%s\n' \
  "$FIRST_ACK_MS" "$RESUME_MS" "$GAP_MS" "$RECORDS" > "$WORKDIR/failover-result.txt"
cat "$WORKDIR/failover-result.txt"

# --- every acknowledged record reads back, once, in order -------------------
# Through the new leader, by a client bootstrapped from all three: the
# readback's own metadata names whoever leads. The input is the acknowledged
# set (every record was acknowledged, checked above), so byte-for-byte
# equality with it is "all there, none twice, in order" at once.
OUTPUT="$WORKDIR/kafka-output.txt"
"$KCAT" -C -b "$KAFKA_BOOTSTRAP" -t "$TOPIC" -p 0 -o beginning -e -c "$RECORDS" \
  -K: -f '%k:%s\n' ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} > "$OUTPUT" 2>> "$WORKDIR/logs/kcat-consume.log" \
  || fail "consume failed: $(tail -3 "$WORKDIR/logs/kcat-consume.log")"
GOT="$(wc -l < "$OUTPUT" | tr -d ' ')"
[[ "$GOT" -eq "$RECORDS" ]] || fail "read back $GOT records, produced $RECORDS"
DUPLICATE_KEYS="$(cut -d: -f1 "$OUTPUT" | sort | uniq -d | wc -l | tr -d ' ')"
[[ "$DUPLICATE_KEYS" -eq 0 ]] \
  || fail "$DUPLICATE_KEYS key(s) read back twice: $(cut -d: -f1 "$OUTPUT" | sort | uniq -d | head -3 | tr '\n' ' ')"
cmp -s "$INPUT" "$OUTPUT" \
  || { diff "$INPUT" "$OUTPUT" | head -5 >&2; fail "the bytes read back differ from the bytes produced"; }
log "all $RECORDS acknowledged records read back through candidate $NEW_LEADER byte-exact, in order, once each"

# The watermark counts records, not attempts: every offset below it is one of
# the records or one of the boundary markers a v2 range holds (one per epoch,
# which Kafka does not show), so a retry that had been appended twice would
# push it past that.
WATERMARK="$("$KCAT" -Q -b "$KAFKA_BOOTSTRAP" -t "$TOPIC:0:-1" 2>/dev/null | awk '{ print $NF }')"
log "high watermark through Kafka: $WATERMARK ($RECORDS records and up to $NEW_EPOCH boundary markers)"
[[ "$WATERMARK" -ge "$RECORDS" && "$WATERMARK" -le $((RECORDS + NEW_EPOCH)) ]] \
  || fail "the high watermark is $WATERMARK, outside $RECORDS to $((RECORDS + NEW_EPOCH)): a retry appended twice, or a record never landed"

# What the new leader's log says about the client's retries. Informational:
# whether a batch was in flight and already held at a quorum when the leader
# died is the timing of the kill, so a zero here is not a failure.
RETRIES="$(grep -c 'idempotent retry acknowledged with its original offset' "$WORKDIR/logs/data-candidate-$NEW_LEADER.log" || true)"
log "retried sets the new leader acknowledged with their original offsets: $RETRIES"

# --- the survivors changed role in place, or stayed followers ----------------
for n in 1 2 3; do
  [[ "$n" == "$WINNER" ]] && continue
  kill -0 "${CAND_PIDS[$n]}" 2>/dev/null || fail "candidate $n is not running: a survivor must outlive the failover"
done
for n in 1 2 3; do
  [[ "$n" == "$WINNER" || "$n" == "$NEW_LEADER" ]] && continue
  await_metric_equals "$(data_metrics_addr "$((n + 10))")" \
    vtop_kafka_gateway_serving 0 \
    "candidate $n still follows, so its Kafka gateway must not claim to serve"
done
log "PASS"
