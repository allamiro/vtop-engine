#!/usr/bin/env bash
# 25 — compressed produce from a stock client (#596): librdkafka, as `kcat`,
# produces through the gateway on a standalone node with each codec Kafka
# defines (gzip, snappy, lz4, zstd), and the same client reads every record back
# byte-exact and in order. The gateway decompresses each batch and stores
# native records, so the read-back is an uncompressed fetch of what the
# producer compressed.
#
# Which batches librdkafka actually compresses is read from its own debug
# lines, never assumed. Against this gateway librdkafka 2.8.0 compresses zstd
# and logs "Broker does not support compression type gzip: not compressing
# batch" for gzip, snappy and lz4, then sends those batches uncompressed. Its
# feature check gates those three codecs on protocol versions below the
# gateway's floor (Produce v3, the first with v2 record batches). So zstd
# must arrive compressed. The other three are round-tripped and reported as
# compressed or not. The gateway's unit tests decode batches the pinned Java
# producer compressed with each codec (tests/fixtures).
#
# A codec the local kcat was built without is skipped by name, never passed:
# `kcat -V` lists librdkafka's builtin features.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
require_binaries

KCAT="$(command -v kcat || command -v kafkacat || true)"
[[ -n "$KCAT" ]] || fail "scenario 25 needs kcat, librdkafka's client: brew install kcat / apt-get install kafkacat"
init_workdir

# kafka-0 of the lib's port families (scenario 16's; scenarios run one at a
# time).
KAFKA_PORT="$(kafka_port 0)"
RECORDS="${CHAOS_KAFKA_RECORDS:-5000}"
require_integer_in_range CHAOS_KAFKA_RECORDS "$RECORDS" 100 10000000
RECORDS=$((10#$RECORDS))
SERVED_CODECS="gzip snappy lz4 zstd"
# The codecs librdkafka must send compressed here (see the header).
MUST_COMPRESS="zstd"
[[ -n "${TOPIC:-}" ]] || fail "lib.sh did not set TOPIC"

FEATURES="$("$KCAT" -V 2>&1 | sed -n 's/.*builtin\.features[= ]*\([^) ]*\).*/\1/p' | tr ',' ' ')"
[[ -n "$FEATURES" ]] || fail "kcat -V did not list librdkafka's builtin features: $("$KCAT" -V 2>&1 | head -3)"
has_feature() { [[ " $FEATURES " == *" $1 "* ]]; }

CFG="$WORKDIR/data-standalone-kafka.yaml"
{
  cat "$(emit_leader_config standalone)"
  echo "kafka: { listen: \"127.0.0.1:$KAFKA_PORT\" }"
} | install_config "$CFG"
PID="$(start_node "data-standalone-kafka" "data_node_ready" data --config "$CFG")"
await_ready "$(data_metrics_addr 0)" "data-standalone-kafka"
log "standalone node up with a kafka listener on 127.0.0.1:$KAFKA_PORT (pid $PID)"

kafka_watermark() {
  "$KCAT" -Q -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC:0:-1" 2>/dev/null | awk '{ print $NF }'
}

# Compressible values, so every codec has something to do: a repeated
# phrase with the record number in it.
EXPECTED="$WORKDIR/kafka-expected.txt"
: > "$EXPECTED"
produced=0
ran=0
compressed_codecs=0
for codec in $SERVED_CODECS; do
  if ! has_feature "$codec"; then
    log "SKIP $codec: this kcat's librdkafka was built without it (builtin.features: $FEATURES)"
    continue
  fi
  INPUT="$WORKDIR/kafka-input-$codec.txt"
  awk -v n="$RECORDS" -v c="$codec" 'BEGIN {
    for (i = 1; i <= n; i++) printf "%s-k%06d:%s value %06d archive verify commit archive verify commit\n", c, i, c, i
  }' > "$INPUT"
  PLOG="$WORKDIR/logs/kcat-produce-$codec.log"
  "$KCAT" -P -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC" -p 0 -K: -z "$codec" \
    -X acks=all -X enable.idempotence=false -X linger.ms=50 -d msg \
    < "$INPUT" > "$PLOG" 2>&1 \
    || fail "$codec produce failed: $(tail -3 "$PLOG")"
  ! grep -q 'Delivery failed' "$PLOG" \
    || fail "a $codec delivery failed: $(grep -m 3 'Delivery failed' "$PLOG")"
  sent_compressed="$(grep -c "Produce MessageSet with .*, $codec)" "$PLOG" || true)"
  sent_plain="$(grep -c 'Produce MessageSet with .*, uncompressed)' "$PLOG" || true)"
  if [[ "$sent_compressed" -gt 0 && "$sent_plain" -eq 0 ]]; then
    log "librdkafka sent all $sent_compressed $codec batch(es) compressed"
    compressed_codecs=$((compressed_codecs + 1))
  elif [[ " $MUST_COMPRESS " == *" $codec "* ]]; then
    fail "librdkafka sent $sent_plain of $((sent_compressed + sent_plain)) $codec batch(es) uncompressed: $(grep -m 1 -i 'not compressing' "$PLOG" || true)"
  else
    log "librdkafka sent $codec batches uncompressed ($(grep -m 1 -o 'does not support compression type [a-z0-9]*: not compressing batch' "$PLOG" || echo "$sent_plain uncompressed"), $sent_compressed compressed): round-tripped, not counted as decompression coverage"
  fi
  cat "$INPUT" >> "$EXPECTED"
  produced=$((produced + RECORDS))
  ran=$((ran + 1))
  mark="$(kafka_watermark)"
  [[ "$mark" -eq "$produced" ]] || fail "after the $codec stream the watermark is $mark, not $produced"
  log "$RECORDS records produced with compression.codec=$codec; watermark $mark"
done
[[ $ran -gt 0 && $compressed_codecs -gt 0 ]] \
  || { log "SKIP: this kcat compressed none of $SERVED_CODECS against the gateway"; exit 77; }

OUTPUT="$WORKDIR/kafka-output.txt"
"$KCAT" -C -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC" -p 0 -o beginning -e -c "$produced" \
  -K: -f '%k:%s\n' > "$OUTPUT" 2> "$WORKDIR/logs/kcat-consume.log" \
  || fail "consume failed: $(tail -3 "$WORKDIR/logs/kcat-consume.log")"
GOT="$(wc -l < "$OUTPUT" | tr -d ' ')"
[[ "$GOT" -eq "$produced" ]] || fail "consumed $GOT records, produced $produced"
cmp -s "$EXPECTED" "$OUTPUT" \
  || { diff "$EXPECTED" "$OUTPUT" | head -5 >&2; fail "the bytes read back differ from the bytes produced"; }
log "all $produced records read back byte-exact and in order"

stop_node_gracefully standalone "$PID"
log "PASS"
