#!/usr/bin/env bash
# A single-node KRaft broker for the Kafka integration tests (#721).
#
# One script starts the broker for the CI matrix (.github/workflows/kafka-matrix.yml)
# and for a local run, so the two cannot drift apart: the same listeners, the
# same seed topic and the same group-coordinator warm-up.
#
# Usage:
#   scripts/kafka-broker.sh up <image> [mode]   start the broker and wait until it answers
#   scripts/kafka-broker.sh version             print the version the broker's jars report
#   scripts/kafka-broker.sh seed                create and fill it_events, warm up the coordinator
#   scripts/kafka-broker.sh logs                print the broker's log
#   scripts/kafka-broker.sh down                stop and remove the broker
#
# Modes: `plain` (the default) is one PLAINTEXT listener on 9092.
#
# Environment:
#   KAFKA_BROKER_NAME     container name (default vtop-kafka-broker)
#   KAFKA_ADVERTISED_HOST the host clients are told to connect to (default
#                         localhost: a test on the runner reaches the published
#                         port; set it to the container name when the tests run
#                         in another container on KAFKA_DOCKER_NETWORK)
#   KAFKA_PUBLISH_PORT    host port 9092 is published on (default 9092; empty
#                         publishes nothing)
#   KAFKA_DOCKER_NETWORK  a Docker network to attach the broker to (optional)
#   KAFKA_BROKER_CPUS, KAFKA_BROKER_MEMORY
#                         `docker run --cpus` and `--memory` (optional; for a
#                         laptop that runs a build beside the broker)
#   KAFKA_UP_TIMEOUT      seconds to wait for the broker to answer (default 120)
set -euo pipefail

NAME="${KAFKA_BROKER_NAME:-vtop-kafka-broker}"
ADVERTISED_HOST="${KAFKA_ADVERTISED_HOST:-localhost}"
PUBLISH_PORT="${KAFKA_PUBLISH_PORT-9092}"
NETWORK="${KAFKA_DOCKER_NETWORK:-}"
UP_TIMEOUT="${KAFKA_UP_TIMEOUT:-120}"
BIN=/opt/kafka/bin
SEED_TOPIC=it_events
SEED_RECORDS=40
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() {
  echo "kafka-broker: $*" >&2
  exit 1
}

case "$UP_TIMEOUT" in
  '' | *[!0-9]*) die "KAFKA_UP_TIMEOUT must be a whole number of seconds, got '$UP_TIMEOUT'" ;;
esac

in_broker() {
  docker exec "$NAME" "$@"
}

cmd_up() {
  local image="${1:-}" mode="${2:-plain}"
  [ -n "$image" ] || die "usage: $0 up <image> [mode]"
  case "$mode" in
    plain) ;;
    *) die "unknown mode '$mode'; known: plain" ;;
  esac
  if docker container inspect "$NAME" >/dev/null 2>&1; then
    die "a container named $NAME already exists; run '$0 down' first"
  fi

  local args=(--detach --name "$NAME" --hostname "$NAME")
  [ -n "$PUBLISH_PORT" ] && args+=(--publish "$PUBLISH_PORT:9092")
  [ -n "$NETWORK" ] && args+=(--network "$NETWORK")
  [ -n "${KAFKA_BROKER_CPUS:-}" ] && args+=(--cpus "$KAFKA_BROKER_CPUS")
  [ -n "${KAFKA_BROKER_MEMORY:-}" ] && args+=(--memory "$KAFKA_BROKER_MEMORY")

  # The single-node KRaft layout the kafka-integration job in ci.yml has
  # always used: broker and controller in one process, every internal topic
  # at replication 1, and no initial rebalance delay so a test's first poll
  # is not spent waiting for one.
  args+=(
    -e "KAFKA_NODE_ID=1"
    -e "KAFKA_PROCESS_ROLES=broker,controller"
    -e "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093"
    -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://$ADVERTISED_HOST:9092"
    -e "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093"
    -e "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER"
    -e "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
    -e "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1"
    -e "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1"
    -e "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1"
    -e "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0"
  )
  docker run "${args[@]}" "$image" >/dev/null

  # Answering a topic list over the plaintext listener is what the CI job's
  # health check asked for. A broker that never gets there fails with its
  # last answer and its log, not a bare timeout.
  local deadline=$((SECONDS + UP_TIMEOUT)) out=""
  until out=$(in_broker "$BIN/kafka-topics.sh" --bootstrap-server localhost:9092 --list 2>&1); do
    if ! docker container inspect -f '{{.State.Running}}' "$NAME" 2>/dev/null | grep -qx true; then
      docker logs --tail 50 "$NAME" >&2 || true
      die "the broker container $NAME exited before it answered"
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      docker logs --tail 50 "$NAME" >&2 || true
      die "the broker did not answer a topic list within ${UP_TIMEOUT}s; last answer: $out"
    fi
    sleep 2
  done
  echo "kafka-broker: $NAME is up ($image, mode $mode, advertised as $ADVERTISED_HOST:9092)"
}

cmd_version() {
  in_broker "$BIN/kafka-topics.sh" --version | awk 'NF { print $1; exit }'
}

cmd_seed() {
  # The tests assert a NON-EMPTY read of this topic, so an empty topic fails
  # them rather than letting them pass vacuously.
  in_broker "$BIN/kafka-topics.sh" --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$SEED_TOPIC" --partitions 1 --replication-factor 1
  bash "$HERE/../docker/seed-events.sh" cef "$SEED_RECORDS" \
    | docker exec -i "$NAME" "$BIN/kafka-console-producer.sh" \
      --bootstrap-server localhost:9092 --topic "$SEED_TOPIC"
  in_broker "$BIN/kafka-get-offsets.sh" --bootstrap-server localhost:9092 --topic "$SEED_TOPIC"

  # Warm up the group coordinator. The adapter asks the broker for the
  # group's committed offsets, which needs the coordinator and the
  # __consumer_offsets topic; a fresh broker creates them only on the first
  # group operation, and until then every read fails with NotCoordinator.
  # Consuming once under a throwaway group forces them into existence.
  in_broker "$BIN/kafka-console-consumer.sh" --bootstrap-server localhost:9092 \
    --topic "$SEED_TOPIC" --group ci-warmup --from-beginning --max-messages 1 \
    --timeout-ms 30000 >/dev/null
  in_broker "$BIN/kafka-consumer-groups.sh" --bootstrap-server localhost:9092 --list
}

cmd_logs() {
  docker logs "$NAME"
}

cmd_down() {
  docker rm --force --volumes "$NAME" >/dev/null 2>&1 || true
}

sub="${1:-}"
[ "$#" -gt 0 ] && shift
case "$sub" in
  up) cmd_up "$@" ;;
  version) cmd_version ;;
  seed) cmd_seed ;;
  logs) cmd_logs ;;
  down) cmd_down ;;
  *) die "usage: $0 {up <image> [mode]|version|seed|logs|down}" ;;
esac
