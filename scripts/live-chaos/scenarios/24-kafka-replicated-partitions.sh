#!/usr/bin/env bash
# 24 — two replicated Kafka partitions led from different hosts (#596): the
# shape the Kafka comparison needs for its multi-partition cells, which no
# other scenario runs. Scenario 21 has two partitions on standalone nodes, and
# scenario 17 one partition on a leader with followers. Here each partition is
# a range with a leader and two followers, the two leaders sit on different
# hosts, every host carries a replica of both ranges under ONE identity (its
# certificate), and each leader names both partitions in `kafka.partitions`:
#
#   host 1 (data-1)  leads partition 0      follows partition 1
#   host 2 (data-2)  follows partition 0    leads partition 1
#   host 3 (data-3)  follows partition 0    follows partition 1
#
# ONE idempotent librdkafka producer (kcat, enable.idempotence=true, acks=all)
# writes keyed records that its partitioner spreads over both partitions, so
# the producer id one leader mints (InitProducerId) is carried to the other
# leader's partition as well. Then a group consumer reads both partitions back:
# the union is exactly the input, each record once, and each partition's
# records are in the order they were produced. Each range's followers hold
# its acknowledged records (replica-status), so every acknowledgement was a
# quorum's on both ranges.
#
# No metadata plane, as in the benchmark: the leaders are static, and
# committed offsets are refused by name (scenario 19); the consumer reads
# anyway. A failure here means the replicated multi-partition topology the
# benchmark relies on does not work with a stock client, and is filed as a bug.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
require_binaries

KCAT="$(command -v kcat || command -v kafkacat || true)"
[[ -n "$KCAT" ]] || fail "scenario 24 needs kcat, librdkafka's client: brew install kcat / apt-get install kafkacat"

init_workdir

RECORDS="${CHAOS_KAFKA_RECORDS:-4000}"
require_integer_in_range CHAOS_KAFKA_RECORDS "$RECORDS" 100 10000000
RECORDS=$((10#$RECORDS))
GROUP="chaos-24"
[[ -n "${TOPIC:-}" ]] || fail "lib.sh did not set TOPIC"

# Partition 0 is the lib's range; partition 1 a second range of the topic.
RANGE_P0="$RANGE_ID"
SEGMENT_P0="$SEGMENT_ID"
RANGE_P1="${CHAOS_RANGE_ID_P1:-aaaaaaaa-0000-0000-0000-0000000000c2}"
SEGMENT_P1="${CHAOS_SEGMENT_ID_P1:-aaaaaaaa-0000-0000-0000-0000000000d2}"
[[ "$RANGE_P0" != "$RANGE_P1" ]] || fail "the two partitions need distinct ranges, both are $RANGE_P0"

# Listeners. Partition 0 uses the lib's leader and follower ports; partition
# 1's leader takes native-p1 and replica-3, and its two followers take
# replica-4 and replica-5 with data-metrics 4 and 5, which no other scenario
# uses. The replica base may therefore be at most 65535-5.
require_integer_in_range CHAOS_REPLICA_BASE_PORT "$REPLICA_BASE_PORT" 1024 65530
P0_KAFKA="$(kafka_port 4)"
P1_KAFKA="$(kafka_port 5)"
P0_NATIVE="$NATIVE_PORT"
P1_NATIVE="$((NATIVE_PORT + 2))"

host_uuid() {
  case "$1" in
    1) echo "$LEADER_UUID" ;;
    2) echo "$FOLLOWER1_UUID" ;;
    3) echo "$FOLLOWER2_UUID" ;;
    *) fail "unknown host $1" ;;
  esac
}

tls_lines() { # <cert-basename> <native: yes|no>
  if transport_plaintext; then
    echo "replica_transport: plaintext"
    [[ "$2" == yes ]] && echo "native_transport: plaintext"
  else
    echo "replica_tls: { ca: $CERTS/ca.pem, cert: $CERTS/$1.pem, key: $CERTS/$1-key.pem }"
    [[ "$2" == yes ]] && echo "native_tls: { ca: $CERTS/ca.pem, cert: $CERTS/$1.pem, key: $CERTS/$1-key.pem }"
  fi
  true
}

# emit_replica <label> <host> <role> <partition> <range> <segment> <replica-n>
#   <metrics-n> [native-port kafka-port kafka-node-id follower-host:replica-n...]
emit_replica() {
  local label="$1" host="$2" role="$3" part="$4" range="$5" segment="$6"
  local replica_n="$7" metrics_n="$8"
  shift 8
  local cfg="$WORKDIR/data-$label.yaml"
  {
    echo "role: $role"
    echo "node_uuid: $(host_uuid "$host")"
    echo "cluster_id: $CLUSTER_ID"
    echo "data_dir: $WORKDIR/data-$label"
    echo "fencing_epoch: $FENCING_EPOCH"
    echo "range: { topic: \"$TOPIC\", topic_epoch: 1, range_id: $range, range_generation: 0 }"
    echo "segment_id: $segment"
    echo "replica_listen: \"$(replica_addr "$replica_n")\""
    echo "observability: { listen: \"$(data_metrics_addr "$metrics_n")\" }"
    if [[ "$role" == leader ]]; then
      local native="$1" kafka="$2" kafka_node="$3"
      shift 3
      echo "native_listen: \"$DATA_HOST:$native\""
      echo "principal_id: $PRINCIPAL_ID"
      echo "followers:"
      local follower fhost fn
      for follower in "$@"; do
        fhost="${follower%%:*}"
        fn="${follower##*:}"
        echo "  - { node_uuid: $(host_uuid "$fhost"), addr: \"$(replica_addr "$fn")\", server_name: \"localhost\" }"
      done
      tls_lines "data-$host" yes
      echo "kafka:"
      echo "  listen: \"127.0.0.1:$kafka\""
      echo "  topic: \"$TOPIC\""
      echo "  node_id: $kafka_node"
      echo "  partition: $part"
      echo "  partitions:"
      echo "    - { partition: 0, node_id: 1, host: \"127.0.0.1\", port: $P0_KAFKA }"
      echo "    - { partition: 1, node_id: 2, host: \"127.0.0.1\", port: $P1_KAFKA }"
    else
      tls_lines "data-$host" no
    fi
  } | install_config "$cfg"
  echo "$cfg"
}

start_replica() { # <label> <metrics-n> <config>
  local pid
  pid="$(start_node "data-$1" "data_node_ready" data --config "$3")"
  await_ready "$(data_metrics_addr "$2")" "data-$1"
  echo "$pid"
}

# Followers first, so each leader's quorum is there when it starts.
P0_F2="$(start_replica p0-host2 1 "$(emit_replica p0-host2 2 follower 0 "$RANGE_P0" "$SEGMENT_P0" 1 1)")"
P0_F3="$(start_replica p0-host3 2 "$(emit_replica p0-host3 3 follower 0 "$RANGE_P0" "$SEGMENT_P0" 2 2)")"
P1_F1="$(start_replica p1-host1 4 "$(emit_replica p1-host1 1 follower 1 "$RANGE_P1" "$SEGMENT_P1" 4 4)")"
P1_F3="$(start_replica p1-host3 5 "$(emit_replica p1-host3 3 follower 1 "$RANGE_P1" "$SEGMENT_P1" 5 5)")"
P0_L="$(start_replica p0-host1 0 "$(emit_replica p0-host1 1 leader 0 "$RANGE_P0" "$SEGMENT_P0" 0 0 \
  "$P0_NATIVE" "$P0_KAFKA" 1 2:1 3:2)")"
P1_L="$(start_replica p1-host2 3 "$(emit_replica p1-host2 2 leader 1 "$RANGE_P1" "$SEGMENT_P1" 3 3 \
  "$P1_NATIVE" "$P1_KAFKA" 2 1:4 3:5)")"
grep -q "role=leader .*kafka=127.0.0.1:$P0_KAFKA" "$WORKDIR/logs/data-p0-host1.log" \
  || fail "partition 0's leader is not a replicated leader serving kafka on $P0_KAFKA: $(grep data_node_ready "$WORKDIR/logs/data-p0-host1.log" || true)"
grep -q "role=leader .*kafka=127.0.0.1:$P1_KAFKA" "$WORKDIR/logs/data-p1-host2.log" \
  || fail "partition 1's leader is not a replicated leader serving kafka on $P1_KAFKA: $(grep data_node_ready "$WORKDIR/logs/data-p1-host2.log" || true)"
log "two replicated partitions up: p0 led on host 1 (pid $P0_L), p1 led on host 2 (pid $P1_L), six replicas on three hosts (followers $P0_F2 $P0_F3 $P1_F1 $P1_F3)"

BOOT="127.0.0.1:$P0_KAFKA,127.0.0.1:$P1_KAFKA"
INPUT="$WORKDIR/kafka-input.txt"
awk -v n="$RECORDS" 'BEGIN {
  srand(24)
  for (i = 1; i <= n; i++) printf "k%06d:v%06d-%08x\n", i, i, int(rand() * 4294967295)
}' > "$INPUT"

KCAT_DEBUG=()
[[ -n "${CHAOS_KCAT_DEBUG:-}" ]] && KCAT_DEBUG=(-d "$CHAOS_KCAT_DEBUG")
# No -p: librdkafka's partitioner hashes each key to a partition, so one
# producer, under one producer id, writes to both leaders.
"$KCAT" -P -b "$BOOT" -t "$TOPIC" -K: -X acks=all -X enable.idempotence=true \
  -X message.timeout.ms=60000 -d eos,msg \
  < "$INPUT" > "$WORKDIR/logs/kcat-produce.log" 2>&1 \
  || fail "the idempotent produce failed: $(tail -3 "$WORKDIR/logs/kcat-produce.log")"
! grep -q 'Delivery failed' "$WORKDIR/logs/kcat-produce.log" \
  || fail "a delivery failed: $(grep -m 3 'Delivery failed' "$WORKDIR/logs/kcat-produce.log")"

watermark() { # <partition>
  "$KCAT" -Q -b "$BOOT" -t "$TOPIC:$1:-1" 2>/dev/null | awk '{ print $NF }'
}
W0="$(watermark 0)"
W1="$(watermark 1)"
[[ "$W0" =~ ^[0-9]+$ && "$W1" =~ ^[0-9]+$ ]] || fail "could not read both watermarks: p0='$W0' p1='$W1'"
[[ "$W0" -gt 0 && "$W1" -gt 0 ]] \
  || fail "the partitioner did not spread the keys: p0 holds $W0, p1 $W1; both partitions must be written"
[[ $((W0 + W1)) -eq "$RECORDS" ]] || fail "the watermarks sum to $((W0 + W1)) (p0 $W0, p1 $W1), not the $RECORDS produced"
# One producer id, minted by one leader, carried to both partitions: read
# from librdkafka's own debug lines (`-d eos,msg`), not assumed.
PLOG="$WORKDIR/logs/kcat-produce.log"
PID_LINE="$(grep -o 'Acquired PID{Id:[0-9]*,Epoch:[0-9]*}' "$PLOG" || true)"
[[ "$(printf '%s\n' "$PID_LINE" | grep -c 'Acquired')" -eq 1 ]] \
  || fail "the producer did not acquire exactly one producer id: '${PID_LINE:-none}'"
PID_TAG="${PID_LINE#Acquired }"
for part in 0 1; do
  grep -F "$TOPIC [$part]: Produce MessageSet" "$PLOG" | grep -qF "$PID_TAG" \
    || fail "partition $part was not written under the producer's id $PID_TAG: $(grep -m 1 -F "$TOPIC [$part]: Produce MessageSet" "$PLOG" || echo 'no produce line')"
  ! grep -F "$TOPIC [$part]: Produce MessageSet" "$PLOG" | grep -vqF "$PID_TAG" \
    || fail "partition $part saw a batch under another producer id"
done
log "one idempotent producer ($PID_TAG) wrote $RECORDS records: $W0 to partition 0, $W1 to partition 1"

OUT="$WORKDIR/kafka-output.txt"
set +e
timeout "$((10#$PROGRESS_TIMEOUT_SECONDS * 3))" "$KCAT" -G "$GROUP" -b "$BOOT" \
  -X auto.offset.reset=earliest -X enable.auto.commit=true -X auto.commit.interval.ms=500 \
  -X session.timeout.ms=10000 \
  -e -f '%p %k:%s\n' ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} "$TOPIC" \
  > "$OUT" 2> "$WORKDIR/logs/kcat-consume.log"
CODE=$?
set -e
[[ $CODE -eq 0 ]] || fail "the group consumer exited $CODE: $(tail -8 "$WORKDIR/logs/kcat-consume.log")"

GOT="$(wc -l < "$OUT" | tr -d ' ')"
[[ "$GOT" -eq "$RECORDS" ]] || fail "the group read $GOT records, not the $RECORDS produced"
# Exactly the input, each record once.
cut -d' ' -f2- "$OUT" | sort > "$WORKDIR/kafka-read-sorted.txt"
sort "$INPUT" > "$WORKDIR/kafka-input-sorted.txt"
cmp -s "$WORKDIR/kafka-input-sorted.txt" "$WORKDIR/kafka-read-sorted.txt" \
  || { diff "$WORKDIR/kafka-input-sorted.txt" "$WORKDIR/kafka-read-sorted.txt" | head -5 >&2; fail "the records read are not exactly the records produced"; }
# Each partition in produced order: its records, in read order, are a
# subsequence of the input in input order. The input's record numbers rise,
# so each partition's numbers must rise too.
for part in 0 1; do
  count="$(awk -v p="$part" '$1 == p' "$OUT" | wc -l | tr -d ' ')"
  want="$W0"; [[ "$part" == 1 ]] && want="$W1"
  [[ "$count" -eq "$want" ]] || fail "partition $part read $count records, its watermark is $want"
  awk -v p="$part" '$1 == p { n = substr($2, 2, 6) + 0; if (n <= last) { print "out of order at " $2; exit 1 } last = n }' "$OUT" \
    || fail "partition $part was not read in produced order"
done
log "the group read both partitions: exactly the $RECORDS records produced, each once, each partition in produced order"

# Every acknowledgement was a quorum's, on both ranges: a follower of each
# holds the leader's acknowledged records.
probe_config() { # <label> <range>
  local cfg="$WORKDIR/replica-probe-$1.yaml"
  {
    echo "cluster_id: $CLUSTER_ID"
    echo "principal_id: $PRINCIPAL_ID"
    echo "producer_id: $PRODUCER_ID"
    echo "producer_epoch: 1"
    echo "fencing_epoch: $FENCING_EPOCH"
    echo "range: { topic: \"$TOPIC\", topic_epoch: 1, range_id: $2, range_generation: 0 }"
    echo "server_name: \"localhost\""
    if ! transport_plaintext; then
      echo "tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-1.pem, key: $CERTS/data-1-key.pem }"
    fi
  } | install_config "$cfg"
  echo "$cfg"
}
follower_committed() { # <label> <range> <replica-n>
  local status
  status="$("$VTOP_NODE" replica-status --client-config "$(probe_config "$1" "$2")" \
    --addr "$(replica_addr "$3")" 2>> "$WORKDIR/logs/replica-status.log")"
  status="${status#*local_committed_offset=}"
  echo "${status%% *}"
}
C0="$(follower_committed p0 "$RANGE_P0" 2)"
C1="$(follower_committed p1 "$RANGE_P1" 5)"
[[ "$C0" =~ ^[0-9]+$ && "$C0" -ge "$W0" ]] || fail "partition 0's follower on host 3 committed '$C0' < $W0 acknowledged"
[[ "$C1" =~ ^[0-9]+$ && "$C1" -ge "$W1" ]] || fail "partition 1's follower on host 3 committed '$C1' < $W1 acknowledged"
log "host 3 holds both ranges' acknowledged records: p0 committed $C0 >= $W0, p1 committed $C1 >= $W1"

log "PASS"
