#!/usr/bin/env bash
# 21 — two members of one group over two partitions, one each (#457 slice 4b):
# two standalone nodes, each leading one Kafka partition of the same topic,
# advertise each other in `kafka.partitions`. A stock group (kcat -G,
# librdkafka's range assignor) of two members finds the one coordinator the
# topology elects, is assigned one partition each, and each member reads only
# the records produced to its partition — byte-exact, no overlap.
#
# Standalone: the criterion is the assignment, not a durable cursor. Commits
# are refused by name (no metadata plane), as scenario 19; the members still
# read and exit clean. The coordinator election and the assignment covering
# both partitions are what this scenario proves.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
require_binaries

KCAT="$(command -v kcat || command -v kafkacat || true)"
[[ -n "$KCAT" ]] || fail "scenario 21 needs kcat, librdkafka's client: brew install kcat / apt-get install kafkacat"

init_workdir

P0_PORT="$(kafka_port 4)"
P1_PORT="$(kafka_port 5)"
NATIVE1="$((NATIVE_PORT + 2))"
RANGE_P1="${CHAOS_RANGE_ID_P1:-aaaaaaaa-0000-0000-0000-0000000000c2}"
SEGMENT_P1="${CHAOS_SEGMENT_ID_P1:-aaaaaaaa-0000-0000-0000-0000000000d2}"
RECORDS="${CHAOS_KAFKA_RECORDS:-2000}"
require_integer_in_range CHAOS_KAFKA_RECORDS "$RECORDS" 1 10000000
RECORDS=$((10#$RECORDS))
GROUP="chaos-21"
[[ -n "${TOPIC:-}" ]] || fail "lib.sh did not set TOPIC"

emit_partition_node() {
  local label="$1" uuid="$2" cert="$3" data_dir="$4" range_id="$5" segment_id="$6"
  local native_port="$7" replica_n="$8" metrics_n="$9" kafka_node="${10}" kafka_part="${11}"
  local kafka_port="${12}"
  local cfg="$WORKDIR/data-$label.yaml"
  {
    echo "role: standalone"
    echo "node_uuid: $uuid"
    echo "cluster_id: $CLUSTER_ID"
    echo "data_dir: $data_dir"
    echo "fencing_epoch: $FENCING_EPOCH"
    echo "range: { topic: \"$TOPIC\", topic_epoch: 1, range_id: $range_id, range_generation: 0 }"
    echo "segment_id: $segment_id"
    echo "native_listen: \"$DATA_HOST:$native_port\""
    echo "replica_listen: \"$(replica_addr "$replica_n")\""
    if transport_plaintext; then
      echo "replica_transport: plaintext"
      echo "native_transport: plaintext"
    else
      echo "replica_tls: { ca: $CERTS/ca.pem, cert: $CERTS/$cert.pem, key: $CERTS/$cert-key.pem }"
      echo "native_tls: { ca: $CERTS/ca.pem, cert: $CERTS/$cert.pem, key: $CERTS/$cert-key.pem }"
    fi
    echo "principal_id: $PRINCIPAL_ID"
    echo "observability: { listen: \"$(data_metrics_addr "$metrics_n")\" }"
    echo "kafka:"
    echo "  listen: \"127.0.0.1:$kafka_port\""
    echo "  topic: \"$TOPIC\""
    echo "  node_id: $kafka_node"
    echo "  partition: $kafka_part"
    echo "  partitions:"
    echo "    - { partition: 0, node_id: 1, host: \"127.0.0.1\", port: $P0_PORT }"
    echo "    - { partition: 1, node_id: 2, host: \"127.0.0.1\", port: $P1_PORT }"
  } | install_config "$cfg"
  echo "$cfg"
}

CFG0="$(emit_partition_node p0 "$LEADER_UUID" data-1 "$WORKDIR/data-p0" \
  "$RANGE_ID" "$SEGMENT_ID" "$NATIVE_PORT" 0 0 1 0 "$P0_PORT")"
CFG1="$(emit_partition_node p1 "$FOLLOWER1_UUID" data-2 "$WORKDIR/data-p1" \
  "$RANGE_P1" "$SEGMENT_P1" "$NATIVE1" 1 1 2 1 "$P1_PORT")"

P0_PID="$(start_node "data-kafka-p0" "data_node_ready" data --config "$CFG0")"
P1_PID="$(start_node "data-kafka-p1" "data_node_ready" data --config "$CFG1")"
await_ready "$(data_metrics_addr 0)" "data-kafka-p0"
await_ready "$(data_metrics_addr 1)" "data-kafka-p1"
grep -q "kafka=127.0.0.1:$P0_PORT" "$WORKDIR/logs/data-kafka-p0.log" \
  || fail "partition 0's ready line does not name its kafka listener"
grep -q "kafka=127.0.0.1:$P1_PORT" "$WORKDIR/logs/data-kafka-p1.log" \
  || fail "partition 1's ready line does not name its kafka listener"
log "two standalone gateways up: partition 0 on 127.0.0.1:$P0_PORT, partition 1 on 127.0.0.1:$P1_PORT"

BOOT="127.0.0.1:$P0_PORT,127.0.0.1:$P1_PORT"
INPUT0="$WORKDIR/kafka-p0.txt"
INPUT1="$WORKDIR/kafka-p1.txt"
awk -v n="$RECORDS" 'BEGIN {
  srand(21)
  for (i = 1; i <= n; i++) printf "p0-k%06d:v%06d-%08x\n", i, i, int(rand() * 4294967295)
}' > "$INPUT0"
awk -v n="$RECORDS" 'BEGIN {
  srand(2101)
  for (i = 1; i <= n; i++) printf "p1-k%06d:v%06d-%08x\n", i, i, int(rand() * 4294967295)
}' > "$INPUT1"

KCAT_DEBUG=()
[[ -n "${CHAOS_KCAT_DEBUG:-}" ]] && KCAT_DEBUG=(-d "$CHAOS_KCAT_DEBUG")
"$KCAT" -P -b "$BOOT" -t "$TOPIC" -p 0 -K: -X acks=all \
  ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} < "$INPUT0" > "$WORKDIR/logs/kcat-produce-p0.log" 2>&1 \
  || fail "produce to partition 0 failed: $(tail -3 "$WORKDIR/logs/kcat-produce-p0.log")"
"$KCAT" -P -b "$BOOT" -t "$TOPIC" -p 1 -K: -X acks=all \
  ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} < "$INPUT1" > "$WORKDIR/logs/kcat-produce-p1.log" 2>&1 \
  || fail "produce to partition 1 failed: $(tail -3 "$WORKDIR/logs/kcat-produce-p1.log")"
log "$RECORDS records produced to each partition"

OUT0="$WORKDIR/kafka-out-0.txt"
OUT1="$WORKDIR/kafka-out-1.txt"
LOG0="$WORKDIR/logs/kcat-consume-0.log"
LOG1="$WORKDIR/logs/kcat-consume-1.log"
set +e
timeout "$((10#$PROGRESS_TIMEOUT_SECONDS * 3))" "$KCAT" -G "$GROUP" -b "$BOOT" \
  -X auto.offset.reset=earliest -X enable.auto.commit=true -X auto.commit.interval.ms=500 \
  -X session.timeout.ms=10000 \
  -e -K: -f '%k:%s\n' ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} "$TOPIC" > "$OUT0" 2> "$LOG0" &
C0=$!
timeout "$((10#$PROGRESS_TIMEOUT_SECONDS * 3))" "$KCAT" -G "$GROUP" -b "$BOOT" \
  -X auto.offset.reset=earliest -X enable.auto.commit=true -X auto.commit.interval.ms=500 \
  -X session.timeout.ms=10000 \
  -e -K: -f '%k:%s\n' ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} "$TOPIC" > "$OUT1" 2> "$LOG1" &
C1=$!
wait "$C0"
E0=$?
wait "$C1"
E1=$?
set -e
[[ $E0 -eq 0 ]] || fail "consumer 0 exited $E0: $(tail -8 "$LOG0")"
[[ $E1 -eq 0 ]] || fail "consumer 1 exited $E1: $(tail -8 "$LOG1")"

assign0="$(grep -E "assigned: $TOPIC \[[01]\]" "$LOG0" | tail -1 || true)"
assign1="$(grep -E "assigned: $TOPIC \[[01]\]" "$LOG1" | tail -1 || true)"
[[ -n "$assign0" ]] || fail "consumer 0 was not assigned a partition: $(grep -m 5 -i 'rebalanc\|assign\|error' "$LOG0" | tr '\n' ' ')"
[[ -n "$assign1" ]] || fail "consumer 1 was not assigned a partition: $(grep -m 5 -i 'rebalanc\|assign\|error' "$LOG1" | tr '\n' ' ')"
part0="$(printf '%s' "$assign0" | sed -n 's/.*\[\(.\).*/\1/p')"
part1="$(printf '%s' "$assign1" | sed -n 's/.*\[\(.\).*/\1/p')"
[[ "$part0" != "$part1" ]] \
  || fail "both members were assigned partition $part0; each must get exactly one"
log "consumer 0 assigned [$part0], consumer 1 assigned [$part1]"

got0="$(wc -l < "$OUT0" | tr -d ' ')"
got1="$(wc -l < "$OUT1" | tr -d ' ')"
[[ "$got0" -eq "$RECORDS" ]] || fail "consumer 0 (partition $part0) read $got0 records, not $RECORDS"
[[ "$got1" -eq "$RECORDS" ]] || fail "consumer 1 (partition $part1) read $got1 records, not $RECORDS"

if [[ "$part0" == "0" ]]; then
  diff -u "$INPUT0" "$OUT0" >/dev/null || fail "consumer 0 did not read partition 0 byte-exact"
  diff -u "$INPUT1" "$OUT1" >/dev/null || fail "consumer 1 did not read partition 1 byte-exact"
else
  diff -u "$INPUT1" "$OUT0" >/dev/null || fail "consumer 0 did not read partition 1 byte-exact"
  diff -u "$INPUT0" "$OUT1" >/dev/null || fail "consumer 1 did not read partition 0 byte-exact"
fi

if sort "$OUT0" "$OUT1" | uniq -d | grep -q .; then
  fail "a record was read by both members"
fi

log "two members, two partitions, one each; each read $RECORDS records byte-exact"
log "scenario 21 passed"
