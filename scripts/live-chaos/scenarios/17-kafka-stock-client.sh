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

KAFKA_PORT="${CHAOS_KAFKA_PORT:-$((NATIVE_PORT + 21))}"
RECORDS="${CHAOS_KAFKA_RECORDS:-20000}"
require_integer_in_range CHAOS_KAFKA_PORT "$KAFKA_PORT" 1024 65535
require_integer_in_range CHAOS_KAFKA_RECORDS "$RECORDS" 2 10000000
HALF=$((RECORDS / 2))
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

# librdkafka's producer with acks=all: every delivery report is a quorum
# acknowledgement from the native broker behind the gateway. `-K:` splits
# each line into key and value; `-p 0` is the range's one partition.
# CHAOS_KCAT_DEBUG=broker,protocol,msg turns on librdkafka's own trace, the
# first thing to read when a delivery fails.
# (Expanded with the `[@]+` idiom below: bash 3.2 treats an empty array as
# unset under `set -u`.)
KCAT_DEBUG=()
[[ -n "${CHAOS_KCAT_DEBUG:-}" ]] && KCAT_DEBUG=(-d "$CHAOS_KCAT_DEBUG")
kcat_produce() { # <from-line> <to-line>
  sed -n "$1,$2p" "$INPUT" | "$KCAT" -P -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC" -p 0 -K: \
    -X acks=all -X enable.idempotence=false -X message.timeout.ms=60000 ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} \
    >> "$WORKDIR/logs/kcat-produce.log" 2>&1
}
kcat_produce 1 "$HALF" \
  || fail "the first half was not produced: $(tail -3 "$WORKDIR/logs/kcat-produce.log")"
log "records 1..$HALF produced through the gateway with acks=all"

# A follower dies hard. The leader and follower 1 are still a majority of
# three, so the stream must go on — and every acknowledgement is still a
# quorum acknowledgement.
kill -9 "$F2_PID"
log "follower 2 killed with SIGKILL; the leader and follower 1 are the quorum left"
kcat_produce $((HALF + 1)) "$RECORDS" \
  || fail "produce did not continue across the follower kill: $(tail -3 "$WORKDIR/logs/kcat-produce.log")"
log "records $((HALF + 1))..$RECORDS produced on the quorum left"

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
