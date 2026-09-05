#!/usr/bin/env bash
# 17 — a stock Kafka client against a three-node native cluster (#225, the
# issue's acceptance): librdkafka, as `kcat`, produces through the gateway on
# the leader with acks=all, a follower is killed mid-stream and the stream
# goes on over the quorum that is left, the same client reads everything back
# byte-exact and in order, and the native path seals and verifies the log the
# gateway wrote.
#
# Why kcat: it is librdkafka — the client library under every non-JVM Kafka
# client — with no JVM to carry into the lab. Its producer speaks ApiVersions,
# Metadata and Produce; its simple consumer (no group) speaks Metadata,
# ListOffsets and Fetch. That is exactly the phase-1 surface the gateway
# serves, and a client that negotiates versions on its own is a stronger
# witness than the hand-framed exchange of scenario 16.
#
# Idempotence stays off on the producer: the gateway serves no InitProducerId
# (phase 1), and a client asking for it would be refused by name.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
KCAT="$(command -v kcat || command -v kafkacat || true)"
[[ -n "$KCAT" ]] || fail "scenario 17 needs kcat, librdkafka's client: brew install kcat / apt-get install kafkacat"
init_workdir

# kafka-1 of the lib's port families (kafka-0 is scenario 16's), judged for
# collisions in preflight and moved with CHAOS_KAFKA_BASE_PORT.
KAFKA_PORT="$(kafka_port 1)"
RECORDS="${CHAOS_KAFKA_RECORDS:-20000}"
# At least 5,000 (review): kcat reads its input in blocks, so a run smaller
# than a block is produced as one burst whatever the feeder's pacing, and
# the follower kill has no stream to land in. A twelve-record smoke run is
# refused here by name rather than failing at the kill.
require_integer_in_range CHAOS_KAFKA_RECORDS "$RECORDS" 5000 10000000
# The floor the kill waits for, and the pacing, both scale to the run.
ACK_FLOOR="${CHAOS_KAFKA_ACK_FLOOR:-$(( 10#$RECORDS / 4 ))}"
# The producer is paced — a pause every PACE_EVERY records, one fortieth of
# the run — so the stream lasts seconds instead of the one second librdkafka
# needs for 20,000 small records, and the follower kill lands INSIDE it.
PACE_EVERY="${CHAOS_KAFKA_PACE_EVERY:-$(( 10#$RECORDS / 40 ))}"
PACE_SLEEP="${CHAOS_KAFKA_PACE_SLEEP:-0.2}"
# The pace reaches awk's system() as text (review): a non-negative decimal.
[[ "$PACE_SLEEP" =~ ^[0-9]+([.][0-9]+)?$ ]] \
  || fail "CHAOS_KAFKA_PACE_SLEEP must be a non-negative decimal, got '$PACE_SLEEP'"
require_integer_in_range CHAOS_KAFKA_ACK_FLOOR "$ACK_FLOOR" 1 "$RECORDS"
require_integer_in_range CHAOS_KAFKA_PACE_EVERY "$PACE_EVERY" 1 "$RECORDS"
[[ -n "${TOPIC:-}" ]] || fail "lib.sh did not set TOPIC"

F1_PID="$(start_follower 1)"
F2_PID="$(start_follower 2)"
CFG="$WORKDIR/data-leader-kafka.yaml"
{
  cat "$(emit_leader_config leader)"
  # Loopback only, the default the node enforces: Kafka's protocol carries
  # no vtop identity, so the listener admits whoever reaches it.
  echo "kafka: { listen: \"127.0.0.1:$KAFKA_PORT\" }"
} | install_config "$CFG"
LEADER_PID="$(start_node "data-leader-kafka" "data_node_ready" data --config "$CFG")"
await_ready "$(data_metrics_addr 0)" "data-leader-kafka"
grep -q "kafka=127.0.0.1:$KAFKA_PORT" "$WORKDIR/logs/data-leader-kafka.log" \
  || fail "the leader's ready line does not name the kafka listener (see $WORKDIR/logs/data-leader-kafka.log)"
log "three-node data plane up; the leader serves Kafka on 127.0.0.1:$KAFKA_PORT (pid $LEADER_PID)"

# The input: one keyed record per line, bytes the consumer must hand back
# unchanged. The seed makes a failure reproducible.
INPUT="$WORKDIR/kafka-input.txt"
awk -v n="$RECORDS" 'BEGIN {
  srand(17)
  for (i = 1; i <= n; i++) printf "k%06d:v%06d-%08x\n", i, i, int(rand() * 4294967295)
}' > "$INPUT"

# ONE librdkafka producer for the whole stream, in the background, with
# acks=all: every delivery report is a quorum acknowledgement from the
# native broker behind the gateway. `-K:` splits each line into key and
# value; `-p 0` is the range's one partition. The same process must be
# alive when the follower dies and finish clean afterwards (review): two
# producers around the kill would prove nothing about a stream surviving it.
# CHAOS_KCAT_DEBUG=broker,protocol,msg turns on librdkafka's own trace, the
# first thing to read when a delivery fails.
# (Expanded with the `[@]+` idiom below: bash 3.2 treats an empty array as
# unset under `set -u`.)
KCAT_DEBUG=()
[[ -n "${CHAOS_KCAT_DEBUG:-}" ]] && KCAT_DEBUG=(-d "$CHAOS_KCAT_DEBUG")
awk -v every="$PACE_EVERY" -v pause="$PACE_SLEEP" \
  '{ print; fflush(); if (NR % every == 0) system("sleep " pause) }' "$INPUT" \
  | "$KCAT" -P -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC" -p 0 -K: \
    -X acks=all -X enable.idempotence=false -X message.timeout.ms=60000 ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} \
    > "$WORKDIR/logs/kcat-produce.log" 2>&1 &
PRODUCER=$!
echo "$PRODUCER" >> "$WORKDIR/pids"

# The watermark through the gateway's own ListOffsets (`kcat -Q`, LATEST):
# the count of records the cluster has acknowledged so far.
kafka_watermark() {
  "$KCAT" -Q -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC:0:-1" 2>/dev/null | awk '{ print $NF }'
}
# Wait for real progress, then kill a follower while the producer is still
# streaming — and prove it was.
deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
reached=0
while (( SECONDS < deadline )); do
  mark="$(kafka_watermark)"
  if [[ "$mark" =~ ^[0-9]+$ && "$mark" -ge "$ACK_FLOOR" ]]; then
    reached=1
    break
  fi
  kill -0 "$PRODUCER" 2>/dev/null || fail "the producer ended before reaching the floor: $(tail -3 "$WORKDIR/logs/kcat-produce.log")"
  # No sleep past the deadline (review): only while a poll still fits.
  (( deadline - SECONDS > 1 )) && sleep 0.1
done
[[ $reached -eq 1 ]] || fail "the producer made no progress to $ACK_FLOOR within ${PROGRESS_TIMEOUT_SECONDS}s: $(tail -3 "$WORKDIR/logs/kcat-produce.log")"
kill -0 "$PRODUCER" 2>/dev/null \
  || fail "the producer finished before the kill could land inside its stream; slow it with CHAOS_KAFKA_PACE_SLEEP"
log "watermark past $ACK_FLOOR with the producer still streaming"

# A follower dies hard. The leader and follower 1 are still a majority of
# three, so the stream must go on — and every acknowledgement is still a
# quorum acknowledgement.
kill -9 "$F2_PID"
log "follower 2 killed with SIGKILL mid-stream; the leader and follower 1 are the quorum left"
set +e
wait "$PRODUCER"
PRODUCER_EXIT=$?
set -e
[[ $PRODUCER_EXIT -eq 0 ]] \
  || fail "the producer exited $PRODUCER_EXIT after the follower kill: $(tail -3 "$WORKDIR/logs/kcat-produce.log")"
! grep -q 'Delivery failed' "$WORKDIR/logs/kcat-produce.log" \
  || fail "a delivery failed across the follower kill: $(grep -m 3 'Delivery failed' "$WORKDIR/logs/kcat-produce.log")"
WATERMARK="$(kafka_watermark)"
[[ "$WATERMARK" -eq "$RECORDS" ]] || fail "the watermark is $WATERMARK after the stream, not $RECORDS"
log "the same producer finished all $RECORDS records across the kill; the watermark is $RECORDS"

# The same client reads the partition back from the beginning: `-o beginning`
# is a ListOffsets for the earliest offset, `-e` stops at the high watermark
# and `-c` at the count. The bytes must be the input, in the input's order.
OUTPUT="$WORKDIR/kafka-output.txt"
"$KCAT" -C -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC" -p 0 -o beginning -e -c "$RECORDS" \
  -K: -f '%k:%s\n' ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} > "$OUTPUT" 2>> "$WORKDIR/logs/kcat-consume.log" \
  || fail "consume failed: $(tail -3 "$WORKDIR/logs/kcat-consume.log")"
GOT="$(wc -l < "$OUTPUT" | tr -d ' ')"
[[ "$GOT" -eq "$RECORDS" ]] || fail "consumed $GOT records, produced $RECORDS"
cmp -s "$INPUT" "$OUTPUT" \
  || { diff "$INPUT" "$OUTPUT" | head -5 >&2; fail "the bytes read back differ from the bytes produced"; }
log "all $RECORDS records read back byte-exact and in order through the gateway"

# The surviving follower holds the replicated prefix: its committed offset
# reaches every acknowledged record.
PROBE_CFG="$(emit_replica_probe_config)"
STATUS="$("$VTOP_NODE" replica-status --client-config "$PROBE_CFG" --addr "$(replica_addr 1)")"
LOCAL="${STATUS#*local_committed_offset=}"
LOCAL="${LOCAL%% *}"
[[ "$LOCAL" -ge "$RECORDS" ]] || fail "follower 1 committed offset $LOCAL < $RECORDS acknowledged"
log "follower 1 committed offset $LOCAL >= $RECORDS: every acknowledgement was a quorum's"

# The native path's word on the log the gateway wrote: an orderly stop, then
# the sealed segments of the leader and the surviving follower pass
# `vtopctl segment verify` — the same bytes, proven, not merely read back.
stop_node_gracefully leader "$LEADER_PID"
stop_node_now "$F1_PID"
seal_and_verify_active leader "$WORKDIR/data-leader"
seal_and_verify_active follower-1 "$WORKDIR/data-follower-1"
log "PASS"
