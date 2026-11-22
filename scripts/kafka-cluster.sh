#!/usr/bin/env bash
# A three-broker Apache Kafka cluster for the Kafka source's live tests
# (#721, B2 of the production bar): multi-broker, with a partition leader that
# a test can stop and start while the source reads.
#
# The CI job (.github/workflows/kafka-cluster.yml) and a local run start it
# with this script, so the two cannot drift apart.
#
# Usage:
#   scripts/kafka-cluster.sh up <image>   start three KRaft nodes and wait until
#                                         all three are in the cluster
#   scripts/kafka-cluster.sh seed         create and fill it_events (replication 3)
#                                         and warm up the group coordinator
#   scripts/kafka-cluster.sh client-env   print the VTOP_TEST_KAFKA_* settings
#                                         (tests/kafka_broker_env.rs and
#                                         tests/integration_kafka_cluster.rs)
#   scripts/kafka-cluster.sh logs         print every node's log
#   scripts/kafka-cluster.sh down         stop and remove the cluster
#
# Layout. Every node is broker and controller (three voters), so the cluster
# keeps a quorum and leaders for every partition with one node down. The nodes
# share one network namespace, owned by a small holder container that also
# publishes the three client ports: node N listens on 9N92 for clients (9192,
# 9292, 9392) and on 9N93 for the controller quorum, all advertised as
# localhost. Stopping a node therefore never takes the others' network with
# it, and no Docker network has to be created (a host whose address pools are
# used up cannot create one). New topics default to replication 3. The
# broker's min.insync.replicas stays at Kafka's default (1): the live suite
# creates its own topics at replication 1, which a cluster-wide minimum of 2
# would refuse produces to, and the restart tests set min.insync.replicas 2
# on the topics they create.
#
# Environment:
#   KAFKA_CLUSTER_NAME    prefix of the container names (default vtop-kafka-cluster);
#                         nodes are <name>-1, -2, -3 and the holder <name>-net
#   KAFKA_CLUSTER_PUBLISH publish the client ports on the host (default 1; 0 for
#                         a run whose tests join the holder's network instead)
#   KAFKA_BROKER_CPUS, KAFKA_BROKER_MEMORY
#                         `docker run --cpus` and `--memory` for each node (optional)
#   KAFKA_UP_TIMEOUT      seconds to wait for the cluster (default 180)
set -euo pipefail

NAME="${KAFKA_CLUSTER_NAME:-vtop-kafka-cluster}"
PUBLISH="${KAFKA_CLUSTER_PUBLISH:-1}"
UP_TIMEOUT="${KAFKA_UP_TIMEOUT:-180}"
HOLDER="$NAME-net"
NODES="1 2 3"
BIN=/opt/kafka/bin
SEED_TOPIC=it_events
SEED_RECORDS=40
# Any 22-character base64 id; fixed, so every node formats into one cluster.
CLUSTER_ID=dnRvcC1rYWZrYS1jbHVzdA
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() {
  echo "kafka-cluster: $*" >&2
  exit 1
}

case "$UP_TIMEOUT" in
  '' | *[!0-9]*) die "KAFKA_UP_TIMEOUT must be a whole number of seconds, got '$UP_TIMEOUT'" ;;
esac

client_port() { echo "9${1}92"; }
controller_port() { echo "9${1}93"; }
node() { echo "$NAME-$1"; }

bootstrap() {
  local n addrs=()
  for n in $NODES; do addrs+=("localhost:$(client_port "$n")"); done
  local IFS=,
  echo "${addrs[*]}"
}

# The node a tool runs in: the first one that is running.
running_node() {
  local n
  for n in $NODES; do
    if docker container inspect -f '{{.State.Running}}' "$(node "$n")" 2>/dev/null | grep -qx true; then
      node "$n"
      return
    fi
  done
  die "no node of $NAME is running; run '$0 up <image>' first"
}

in_cluster() {
  docker exec "$(running_node)" "$@"
}

cmd_up() {
  local image="${1:-}"
  [ -n "$image" ] || die "usage: $0 up <image>"
  case "$image" in *redpanda*) die "the cluster is Apache Kafka only" ;; esac
  local n
  for n in $NODES; do
    if docker container inspect "$(node "$n")" >/dev/null 2>&1; then
      die "a container named $(node "$n") already exists; run '$0 down' first"
    fi
  done

  local holder_args=(--detach --name "$HOLDER" --hostname "$HOLDER")
  if [ "$PUBLISH" = 1 ]; then
    for n in $NODES; do holder_args+=(--publish "$(client_port "$n"):$(client_port "$n")"); done
  fi
  # The image itself holds the namespace: no second image to pull.
  docker run "${holder_args[@]}" --entrypoint sleep "$image" infinity >/dev/null

  local voters="" sep=""
  for n in $NODES; do
    voters="$voters$sep$n@localhost:$(controller_port "$n")"
    sep=,
  done
  for n in $NODES; do
    local args=(--detach --name "$(node "$n")" --network "container:$HOLDER")
    [ -n "${KAFKA_BROKER_CPUS:-}" ] && args+=(--cpus "$KAFKA_BROKER_CPUS")
    [ -n "${KAFKA_BROKER_MEMORY:-}" ] && args+=(--memory "$KAFKA_BROKER_MEMORY")
    args+=(
      -e "CLUSTER_ID=$CLUSTER_ID"
      -e "KAFKA_NODE_ID=$n"
      -e "KAFKA_PROCESS_ROLES=broker,controller"
      -e "KAFKA_LISTENERS=PLAINTEXT://:$(client_port "$n"),CONTROLLER://:$(controller_port "$n")"
      -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:$(client_port "$n")"
      -e "KAFKA_CONTROLLER_QUORUM_VOTERS=$voters"
      -e "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER"
      -e "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
      -e "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT"
      -e "KAFKA_LOG_DIRS=/tmp/kraft-logs-$n"
      -e "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=3"
      -e "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3"
      -e "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=2"
      -e "KAFKA_DEFAULT_REPLICATION_FACTOR=3"
      -e "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0"
    )
    docker run "${args[@]}" "$image" >/dev/null
  done

  # Up means every node is a registered broker: a cluster answering with two
  # would pass a test that needed the third.
  local deadline=$((SECONDS + UP_TIMEOUT)) out="" brokers=0
  while :; do
    out=$(in_cluster "$BIN/kafka-broker-api-versions.sh" --bootstrap-server "$(bootstrap)" 2>&1) || true
    brokers=$(printf '%s\n' "$out" | grep -c '^localhost:9[0-9]92 (id: [0-9]' || true)
    [ "$brokers" -eq 3 ] && break
    if [ "$SECONDS" -ge "$deadline" ]; then
      cmd_logs | tail -n 80 >&2 || true
      die "the cluster did not register three brokers within ${UP_TIMEOUT}s (saw $brokers); last answer: $out"
    fi
    sleep 2
  done
  echo "kafka-cluster: $NAME is up ($image, three nodes, bootstrap $(bootstrap))"
}

cmd_seed() {
  local bs
  bs=$(bootstrap)
  in_cluster "$BIN/kafka-topics.sh" --bootstrap-server "$bs" \
    --create --if-not-exists --topic "$SEED_TOPIC" --partitions 1 --replication-factor 3
  bash "$HERE/../docker/seed-events.sh" cef "$SEED_RECORDS" \
    | docker exec -i "$(running_node)" "$BIN/kafka-console-producer.sh" \
      --bootstrap-server "$bs" --topic "$SEED_TOPIC" --producer-property acks=all
  in_cluster "$BIN/kafka-get-offsets.sh" --bootstrap-server "$bs" --topic "$SEED_TOPIC"
  # The same group-coordinator warm-up as scripts/kafka-broker.sh: the first
  # group operation creates __consumer_offsets (replication 3 here).
  in_cluster "$BIN/kafka-console-consumer.sh" --bootstrap-server "$bs" \
    --topic "$SEED_TOPIC" --group ci-warmup --from-beginning --max-messages 1 \
    --timeout-ms 60000 >/dev/null
  in_cluster "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$bs" --list
}

# The settings the tests read. VTOP_TEST_KAFKA_CLUSTER_NODES maps each node id
# to its container, in id order, so a test can stop the broker that leads a
# partition.
cmd_client_env() {
  local n names=()
  for n in $NODES; do names+=("$(node "$n")"); done
  echo "VTOP_TEST_KAFKA=$(bootstrap)"
  local IFS=,
  echo "VTOP_TEST_KAFKA_CLUSTER_NODES=${names[*]}"
}

cmd_logs() {
  local n
  for n in $NODES; do
    echo "=== $(node "$n")"
    docker logs "$(node "$n")" 2>&1 || true
  done
}

cmd_down() {
  local n
  for n in $NODES; do docker rm --force --volumes "$(node "$n")" >/dev/null 2>&1 || true; done
  docker rm --force "$HOLDER" >/dev/null 2>&1 || true
}

sub="${1:-}"
[ "$#" -gt 0 ] && shift
case "$sub" in
  up) cmd_up "$@" ;;
  seed) cmd_seed ;;
  client-env) cmd_client_env ;;
  logs) cmd_logs ;;
  down) cmd_down ;;
  *) die "usage: $0 {up <image>|seed|client-env|logs|down}" ;;
esac
