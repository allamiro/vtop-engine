#!/usr/bin/env bash
# 18 — an idempotent stock producer against a three-node native cluster (#457,
# the first slice's acceptance): librdkafka, as `kcat`, produces through the
# gateway on the leader with enable.idempotence=true — InitProducerId mints
# its producer id here, and every batch carries that id and its sequences —
# and BOTH followers are stopped (SIGSTOP) mid-stream for longer than the
# client's request timeout. The leader holds the appends for a quorum that is
# not answering; the client times out and RETRIES them, more than once; when
# the followers resume, the held appends complete and every retry meets the
# log's per-record duplicate check: acknowledged with the offset the records
# already have, appended once. The same client reads everything back
# byte-exact and in order, no key twice; the leader's log names the retries
# it acknowledged; the native path seals and verifies the log.
#
# Why a stall of both followers and not a kill: a kill leaves a quorum (17
# proves that) and nothing times out; a stalled leader would be a failover.
# Two stopped followers are a quorum that WILL answer, late — exactly the
# timeout-then-retry the single-writer bridge duplicated (#225 phase 1).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
require_binaries

KCAT="$(command -v kcat || command -v kafkacat || true)"
[[ -n "$KCAT" ]] || fail "scenario 18 needs kcat, librdkafka's client: brew install kcat / apt-get install kafkacat"

init_workdir

# kafka-2 of the lib's port families (kafka-0 is 16's, kafka-1 is 17's).
KAFKA_PORT="$(kafka_port 2)"
RECORDS="${CHAOS_KAFKA_RECORDS:-20000}"
require_integer_in_range CHAOS_KAFKA_RECORDS "$RECORDS" 5000 10000000
ACK_FLOOR="${CHAOS_KAFKA_ACK_FLOOR:-$(( 10#$RECORDS / 4 ))}"
PACE_EVERY="${CHAOS_KAFKA_PACE_EVERY:-$(( 10#$RECORDS / 40 ))}"
PACE_SLEEP="${CHAOS_KAFKA_PACE_SLEEP:-0.2}"
# The pace reaches awk's system() as text (review): a non-negative decimal.
[[ "$PACE_SLEEP" =~ ^[0-9]+([.][0-9]+)?$ ]] \
  || fail "CHAOS_KAFKA_PACE_SLEEP must be a non-negative decimal, got '$PACE_SLEEP'"
require_integer_in_range CHAOS_KAFKA_ACK_FLOOR "$ACK_FLOOR" 1 "$RECORDS"
require_integer_in_range CHAOS_KAFKA_PACE_EVERY "$PACE_EVERY" 1 "$RECORDS"
# The stall outlasts the client's request timeout several times over, so the
# batches in flight when it starts are retried more than once before the
# quorum answers — at least one of those retries reaches the log AFTER the
# original append landed, and that is the retry the log deduplicates.
STALL_SECONDS="${CHAOS_KAFKA_STALL_SECONDS:-12}"
REQUEST_TIMEOUT_MS="${CHAOS_KAFKA_REQUEST_TIMEOUT_MS:-2000}"
require_integer_in_range CHAOS_KAFKA_STALL_SECONDS "$STALL_SECONDS" 3 120
require_integer_in_range CHAOS_KAFKA_REQUEST_TIMEOUT_MS "$REQUEST_TIMEOUT_MS" 1000 60000
(( STALL_SECONDS * 1000 >= REQUEST_TIMEOUT_MS * 3 )) \
  || fail "CHAOS_KAFKA_STALL_SECONDS ($STALL_SECONDS) must be at least three request timeouts ($REQUEST_TIMEOUT_MS ms) so retries land inside the stall"
[[ -n "${TOPIC:-}" ]] || fail "lib.sh did not set TOPIC"

F1_PID="$(start_follower 1)"
F2_PID="$(start_follower 2)"
CFG="$WORKDIR/data-leader-kafka.yaml"
{
  cat "$(emit_leader_config leader)"
  echo "kafka: { listen: \"127.0.0.1:$KAFKA_PORT\" }"
} | install_config "$CFG"
LEADER_PID="$(start_node "data-leader-kafka" "data_node_ready" data --config "$CFG")"
await_ready "$(data_metrics_addr 0)" "data-leader-kafka"
grep -q "kafka=127.0.0.1:$KAFKA_PORT" "$WORKDIR/logs/data-leader-kafka.log" \
  || fail "the leader's ready line does not name the kafka listener (see $WORKDIR/logs/data-leader-kafka.log)"
log "three-node data plane up; the leader serves Kafka on 127.0.0.1:$KAFKA_PORT (pid $LEADER_PID)"

INPUT="$WORKDIR/kafka-input.txt"
awk -v n="$RECORDS" 'BEGIN {
  srand(18)
  for (i = 1; i <= n; i++) printf "k%06d:v%06d-%08x\n", i, i, int(rand() * 4294967295)
}' > "$INPUT"

# ONE idempotent librdkafka producer for the whole stream. enable.idempotence
# makes the client ask for InitProducerId, carry the id and sequences on
# every batch, retry until message.timeout.ms, and keep at most five requests
# in flight — the stock configuration a real producer runs with. The request
# timeout is short so the stall produces retries; the message timeout is
# long so none of them gives up.
KCAT_DEBUG=()
[[ -n "${CHAOS_KCAT_DEBUG:-}" ]] && KCAT_DEBUG=(-d "$CHAOS_KCAT_DEBUG")
awk -v every="$PACE_EVERY" -v pause="$PACE_SLEEP" \
  '{ print; fflush(); if (NR % every == 0) system("sleep " pause) }' "$INPUT" \
  | "$KCAT" -P -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC" -p 0 -K: \
    -X acks=all -X enable.idempotence=true \
    -X "request.timeout.ms=$REQUEST_TIMEOUT_MS" -X message.timeout.ms=180000 \
    ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} \
    > "$WORKDIR/logs/kcat-produce.log" 2>&1 &
PRODUCER=$!
echo "$PRODUCER" >> "$WORKDIR/pids"

kafka_watermark() {
  "$KCAT" -Q -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC:0:-1" 2>/dev/null | awk '{ print $NF }'
}

deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
reached=0
while (( SECONDS < deadline )); do
  mark="$(kafka_watermark)"
  if [[ "$mark" =~ ^[0-9]+$ && "$mark" -ge "$ACK_FLOOR" ]]; then
    reached=1
    break
  fi
  kill -0 "$PRODUCER" 2>/dev/null || fail "the producer ended before reaching the floor: $(tail -3 "$WORKDIR/logs/kcat-produce.log")"
  (( deadline - SECONDS > 1 )) && sleep 0.1
done
[[ $reached -eq 1 ]] || fail "the producer made no progress to $ACK_FLOOR within ${PROGRESS_TIMEOUT_SECONDS}s: $(tail -3 "$WORKDIR/logs/kcat-produce.log")"
kill -0 "$PRODUCER" 2>/dev/null \
  || fail "the producer finished before the stall could land inside its stream; slow it with CHAOS_KAFKA_PACE_SLEEP"
log "watermark past $ACK_FLOOR with the producer still streaming"

# Both followers stop answering. The leader keeps every append it cannot
# commit; the client's requests time out and it retries them.
kill -STOP "$F1_PID" "$F2_PID"
log "both followers stopped (SIGSTOP) for ${STALL_SECONDS}s: no quorum answers, the client's ${REQUEST_TIMEOUT_MS}ms requests time out and are retried"
sleep "$STALL_SECONDS"
kill -CONT "$F1_PID" "$F2_PID"
log "followers resumed; the held appends complete and the retries meet the duplicate check"

set +e
wait "$PRODUCER"
PRODUCER_EXIT=$?
set -e
[[ $PRODUCER_EXIT -eq 0 ]] \
  || fail "the producer exited $PRODUCER_EXIT after the stall: $(tail -3 "$WORKDIR/logs/kcat-produce.log")"
! grep -q 'Delivery failed' "$WORKDIR/logs/kcat-produce.log" \
  || fail "a delivery failed across the stall: $(grep -m 3 'Delivery failed' "$WORKDIR/logs/kcat-produce.log")"
! grep -qi 'fatal' "$WORKDIR/logs/kcat-produce.log" \
  || fail "the idempotent producer hit a fatal error: $(grep -im 3 'fatal' "$WORKDIR/logs/kcat-produce.log")"

# The watermark is the count of records, not of attempts.
WATERMARK="$(kafka_watermark)"
[[ "$WATERMARK" -eq "$RECORDS" ]] || fail "the watermark is $WATERMARK after the stream, not $RECORDS: a retry appended twice, or a record never landed"
log "the producer finished all $RECORDS records across the stall; the watermark is $RECORDS, not one more"

# The retries happened, and were deduplicated: the gateway names each set it
# acknowledged with an offset the records already had.
DEDUPED="$(grep -c 'idempotent retry acknowledged with its original offset' "$WORKDIR/logs/data-leader-kafka.log" || true)"
[[ "$DEDUPED" -ge 1 ]] \
  || fail "the leader's log names no deduplicated retry: the stall produced none (lengthen CHAOS_KAFKA_STALL_SECONDS or shorten CHAOS_KAFKA_REQUEST_TIMEOUT_MS), or the gateway did not log it"
log "$DEDUPED retried set(s) acknowledged with their original offsets"

OUTPUT="$WORKDIR/kafka-output.txt"
"$KCAT" -C -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC" -p 0 -o beginning -e -c "$RECORDS" \
  -K: -f '%k:%s\n' ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} > "$OUTPUT" 2>> "$WORKDIR/logs/kcat-consume.log" \
  || fail "consume failed: $(tail -3 "$WORKDIR/logs/kcat-consume.log")"
GOT="$(wc -l < "$OUTPUT" | tr -d ' ')"
[[ "$GOT" -eq "$RECORDS" ]] || fail "consumed $GOT records, produced $RECORDS"
DUPLICATE_KEYS="$(cut -d: -f1 "$OUTPUT" | sort | uniq -d | wc -l | tr -d ' ')"
[[ "$DUPLICATE_KEYS" -eq 0 ]] || fail "$DUPLICATE_KEYS key(s) read back twice: $(cut -d: -f1 "$OUTPUT" | sort | uniq -d | head -3 | tr '\n' ' ')"
cmp -s "$INPUT" "$OUTPUT" \
  || { diff "$INPUT" "$OUTPUT" | head -5 >&2; fail "the bytes read back differ from the bytes produced"; }
log "all $RECORDS records read back byte-exact, in order, no key twice"

PROBE_CFG="$(emit_replica_probe_config)"
STATUS="$("$VTOP_NODE" replica-status --client-config "$PROBE_CFG" --addr "$(replica_addr 1)")"
LOCAL="${STATUS#*local_committed_offset=}"
LOCAL="${LOCAL%% *}"
[[ "$LOCAL" -ge "$RECORDS" ]] || fail "follower 1 committed offset $LOCAL < $RECORDS acknowledged"
log "follower 1 committed offset $LOCAL >= $RECORDS: every acknowledgement was a quorum's"

stop_node_gracefully leader "$LEADER_PID"
stop_node_now "$F1_PID"
stop_node_now "$F2_PID"
seal_and_verify_active leader "$WORKDIR/data-leader"
seal_and_verify_active follower-1 "$WORKDIR/data-follower-1"
log "PASS"
