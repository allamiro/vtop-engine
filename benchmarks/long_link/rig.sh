#!/usr/bin/env bash
# The long-link rig: a Kafka broker and MinIO on one Docker network, with a
# round trip of DELAY_MS added on the path back from MinIO to the engine, so
# every request the engine sends the store waits that long for its answer and
# its ACKs. The delay sits on MinIO's side, not the engine's, so the engine's
# own send queue holds no delayed packets (TCP small queues would otherwise
# throttle the sender) and its tcp_slow_start_after_idle setting is the only
# one that matters. Kafka is local to the engine, as a sink beside its broker.
#
#   benchmarks/long_link/rig.sh up      # network, broker, store, delay, data
#   benchmarks/long_link/rig.sh down    # remove all of it
#
# Knobs: DELAY_MS (default 200), DATA_BYTES (default 1073741824), SUBNET
# (default 10.251.77.0/24; Docker's own pools can be exhausted on a busy host).
set -euo pipefail
DELAY_MS="${DELAY_MS:-200}"
DATA_BYTES="${DATA_BYTES:-1073741824}"
SUBNET="${SUBNET:-10.251.77.0/24}"
PREFIX="${SUBNET%.*}"
NET=vtop-longlink-net
KAFKA=vtop-longlink-kafka
MINIO=vtop-longlink-minio
ENGINE_IP="$PREFIX.30"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${LONGLINK_WORK:-$HERE/work}"

up() {
  docker network create --subnet "$SUBNET" "$NET" >/dev/null
  docker run -d --name "$KAFKA" --network "$NET" --ip "$PREFIX.10" --cpus=1 --memory=2g \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://$KAFKA:9092" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_LOG_RETENTION_HOURS=-1 -e KAFKA_HEAP_OPTS="-Xmx1g -Xms1g" \
    apache/kafka:3.9.1 >/dev/null
  docker run -d --name "$MINIO" --network "$NET" --ip "$PREFIX.20" --cpus=1 --memory=2g \
    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
    "${MINIO_IMAGE:-vtop-minio:RELEASE.2025-09-07T16-13-09Z}" server /data >/dev/null
  # The delay, in MinIO's network namespace, on packets to the engine only.
  docker run --rm --network "container:$MINIO" --cap-add NET_ADMIN alpine:3.21 sh -c "
    apk add -q iproute2 >/dev/null &&
    tc qdisc add dev eth0 root handle 1: prio bands 3 priomap 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 &&
    tc qdisc add dev eth0 parent 1:3 handle 30: netem delay ${DELAY_MS}ms limit 200000 &&
    tc filter add dev eth0 parent 1:0 protocol ip prio 1 u32 match ip dst $ENGINE_IP/32 flowid 1:3"
  for _ in $(seq 1 60); do
    docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break
    sleep 2
  done
  mkdir -p "$WORK"
  python3 "$HERE/gen.py" "$DATA_BYTES" 11 > "$WORK/data.jsonl"
  for spec in p1:1 p4:4; do
    topic="${spec%%:*}"
    docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --create --topic "$topic" --partitions "${spec##*:}" --replication-factor 1 >/dev/null
    docker exec -i "$KAFKA" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic "$topic" --producer-property batch.size=1048576 --producer-property linger.ms=20 \
      --producer-property compression.type=lz4 < "$WORK/data.jsonl" >/dev/null
  done
  wc -l < "$WORK/data.jsonl" | tr -d ' ' > "$WORK/records"
  rm -f "$WORK/data.jsonl"
  echo "rig up: ${DELAY_MS} ms round trip to the store, $(cat "$WORK/records") records in p1 and p4"
}

down() {
  docker rm -f "$KAFKA" "$MINIO" vtop-longlink-engine >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
