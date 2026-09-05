#!/usr/bin/env bash
# 19 — a stock group consumer against the gateway (#457, slice 2): librdkafka,
# as `kcat -G`, joins a consumer group on the leader's gateway — FindCoordinator,
# JoinGroup, SyncGroup, Heartbeat, OffsetFetch, LeaveGroup — is assigned the
# range's one partition by its own assignor, reads every record the producer
# wrote, byte-exact and in order, and leaves. Auto-commit is ON, at a short
# interval, so commits are certain to be sent; the durable offset store is
# the next slice, and a gateway without one REFUSES every commit by name —
# the leader's log names them, the client's log carries the refusal, and the
# client reads and exits clean all the same. Nothing was remembered that
# would have been forgotten.
#
# Why kcat -G: it is librdkafka's consumer group protocol end to end, with
# the client-side range assignor doing the assigning — exactly what the
# gateway must coordinate without doing the assigning itself.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
require_binaries

KCAT="$(command -v kcat || command -v kafkacat || true)"
[[ -n "$KCAT" ]] || fail "scenario 19 needs kcat, librdkafka's client: brew install kcat / apt-get install kafkacat"

init_workdir

# kafka-3 of the lib's port families (16, 17 and 18 hold kafka-0..2).
KAFKA_PORT="$(kafka_port 3)"
RECORDS="${CHAOS_KAFKA_RECORDS:-5000}"
require_integer_in_range CHAOS_KAFKA_RECORDS "$RECORDS" 1 10000000
RECORDS=$((10#$RECORDS))
GROUP="chaos-19"
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
  srand(19)
  for (i = 1; i <= n; i++) printf "k%06d:v%06d-%08x\n", i, i, int(rand() * 4294967295)
}' > "$INPUT"

KCAT_DEBUG=()
[[ -n "${CHAOS_KCAT_DEBUG:-}" ]] && KCAT_DEBUG=(-d "$CHAOS_KCAT_DEBUG")
"$KCAT" -P -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC" -p 0 -K: -X acks=all \
  ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} < "$INPUT" > "$WORKDIR/logs/kcat-produce.log" 2>&1 \
  || fail "produce failed: $(tail -3 "$WORKDIR/logs/kcat-produce.log")"
WATERMARK="$("$KCAT" -Q -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC:0:-1" 2>/dev/null | awk '{ print $NF }')"
[[ "$WATERMARK" -eq "$RECORDS" ]] || fail "the watermark is $WATERMARK after producing, not $RECORDS"
log "$RECORDS records produced; the watermark is $RECORDS"

# ONE group consumer, the whole group protocol: it finds its coordinator
# (this gateway), joins, is the leader of a group of one, assigns itself the
# one partition, syncs, heartbeats, fetches no committed offset (nothing was
# ever committed) and starts from the earliest, reads to the end (-e) and
# leaves — committing as it goes and on the way out, every commit refused.
OUTPUT="$WORKDIR/kafka-output.txt"
CONSUME_LOG="$WORKDIR/logs/kcat-consume.log"
set +e
timeout "$((10#$PROGRESS_TIMEOUT_SECONDS * 2))" "$KCAT" -G "$GROUP" -b "127.0.0.1:$KAFKA_PORT" \
  -X auto.offset.reset=earliest -X enable.auto.commit=true -X auto.commit.interval.ms=500 \
  -X session.timeout.ms=10000 \
  -e -K: -f '%k:%s\n' ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} "$TOPIC" > "$OUTPUT" 2> "$CONSUME_LOG"
CONSUME_EXIT=$?
set -e
[[ $CONSUME_EXIT -eq 0 ]] \
  || fail "the group consumer exited $CONSUME_EXIT: $(tail -5 "$CONSUME_LOG")"
grep -q "assigned: $TOPIC \[0\]" "$CONSUME_LOG" \
  || fail "the consumer's rebalance did not assign it the partition: $(grep -m 3 -i 'rebalanc\|assign\|error' "$CONSUME_LOG" | tr '\n' ' ')"
GOT="$(wc -l < "$OUTPUT" | tr -d ' ')"
[[ "$GOT" -eq "$RECORDS" ]] || fail "the group consumer read $GOT records, not $RECORDS: $(tail -3 "$CONSUME_LOG")"
cmp -s "$INPUT" "$OUTPUT" \
  || { diff "$INPUT" "$OUTPUT" | head -5 >&2; fail "the bytes the group consumer read differ from the bytes produced"; }
log "the group consumer was assigned $TOPIC [0] by its own assignor and read all $RECORDS records byte-exact, in order"

# Every commit was refused BY NAME — on the leader's log, and on the
# client's — and nothing else went wrong for the client: a gateway without
# a durable store keeps no offset it would forget, and says so.
REFUSED="$(grep -c 'OffsetCommit refused' "$WORKDIR/logs/data-leader-kafka.log" || true)"
[[ "$REFUSED" -ge 1 ]] \
  || fail "the leader's log names no refused OffsetCommit: the client's commits were swallowed, or never sent"
grep -i 'commit' "$CONSUME_LOG" | grep -qiE 'fail|error|unsupported' \
  || fail "the client's log does not carry the refused commit: $(grep -i 'commit' "$CONSUME_LOG" | head -3 | tr '\n' ' ')"
OTHER_ERRORS="$(grep -i 'error' "$CONSUME_LOG" | grep -vi 'commit' || true)"
[[ -z "$OTHER_ERRORS" ]] || fail "the consumer logged an error other than the refused commits: $(printf '%s' "$OTHER_ERRORS" | head -3 | tr '\n' ' ')"
log "every commit was refused by name ($REFUSED on the leader's log, the refusal on the client's); the client read everything and exited clean"

stop_node_gracefully leader "$LEADER_PID"
stop_node_now "$F1_PID"
stop_node_now "$F2_PID"
seal_and_verify_active leader "$WORKDIR/data-leader"
log "PASS"
