#!/usr/bin/env bash
# A single-node broker for the Kafka integration tests (#721).
#
# One script starts the broker for the CI matrix (.github/workflows/kafka-matrix.yml)
# and for a local run, so the two cannot drift apart: the same listeners, the
# same users, the same seed topic and the same group-coordinator warm-up.
#
# Usage:
#   scripts/kafka-broker.sh up <image> [mode]   start the broker and wait until it answers
#   scripts/kafka-broker.sh version             print the version the broker reports
#   scripts/kafka-broker.sh seed                create and fill it_events, warm up the coordinator
#   scripts/kafka-broker.sh client-env [mech]   print the VTOP_TEST_KAFKA_* settings for the
#                                               running broker (tests/kafka_broker_env.rs)
#   scripts/kafka-broker.sh logs                print the broker's log
#   scripts/kafka-broker.sh down                stop and remove the broker (and its certificates)
#
# Modes, for an Apache Kafka image (a Redpanda image takes `plain` only):
#   plain           one PLAINTEXT listener on 9092 (the default)
#   tls             an SSL listener on 9092; the server certificate is signed by a
#                   CA the script generates (client-env prints its path)
#   sasl-plaintext  a SASL_PLAINTEXT listener on 9092 with PLAIN, SCRAM-SHA-256
#                   and SCRAM-SHA-512 enabled; user `vtop`
#   sasl-ssl        the same SASL listener over TLS (SASL_SSL)
# Every mode also has an unpublished PLAINTEXT listener on 9094 inside the
# container, which this script uses to create the seed topic and the SCRAM
# users, so none of its own calls depends on the credentials under test.
# Mutual TLS is not offered: the engine cannot present a client certificate.
#
# Environment:
#   KAFKA_BROKER_NAME     container name (default vtop-kafka-broker)
#   KAFKA_ADVERTISED_HOST the host clients are told to connect to (default
#                         localhost: a test on the runner reaches the published
#                         port; set it to the container name when the tests run
#                         in another container on KAFKA_DOCKER_NETWORK). A TLS
#                         mode puts it in the certificate's names.
#   KAFKA_PUBLISH_PORT    host port 9092 is published on (default 9092; empty
#                         publishes nothing)
#   KAFKA_DOCKER_NETWORK  a Docker network to attach the broker to (optional)
#   KAFKA_BROKER_CPUS, KAFKA_BROKER_MEMORY
#                         `docker run --cpus` and `--memory` (optional; for a
#                         laptop that runs a build beside the broker)
#   KAFKA_CERT_DIR        where a TLS mode writes its certificates (default
#                         $TMPDIR/<container name>-certs)
#   KAFKA_UP_TIMEOUT      seconds to wait for the broker to answer (default 120)
set -euo pipefail

NAME="${KAFKA_BROKER_NAME:-vtop-kafka-broker}"
ADVERTISED_HOST="${KAFKA_ADVERTISED_HOST:-localhost}"
PUBLISH_PORT="${KAFKA_PUBLISH_PORT-9092}"
NETWORK="${KAFKA_DOCKER_NETWORK:-}"
UP_TIMEOUT="${KAFKA_UP_TIMEOUT:-120}"
CERT_DIR="${KAFKA_CERT_DIR:-${TMPDIR:-/tmp}/$NAME-certs}"
BIN=/opt/kafka/bin
SEED_TOPIC=it_events
SEED_RECORDS=40
# Where the broker's own tools reach it without the credentials under test.
ADMIN_PORT=9094
CERTS_IN_BROKER=/etc/vtop-kafka-certs
CERT_MARKER=.vtop-kafka-broker-certs
# The test user. The password protects a broker that lives for one job on a
# runner and is published only on its own host's port; it is public here on
# purpose, so a leg and a local run use the same one. The wrong one is what the
# wrong-credential tests (tests/integration_kafka_auth.rs) present.
SASL_USER=vtop
SASL_PASSWORD=vtop-it-password-7f3a
SASL_WRONG_PASSWORD=vtop-wrong-password-91c2
SASL_MECHANISMS="PLAIN SCRAM-SHA-256 SCRAM-SHA-512"
MODE_LABEL=vtop.kafka-broker.mode
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

is_redpanda() {
  case "$1" in
    *redpanda*) return 0 ;;
    *) return 1 ;;
  esac
}

# The image and the mode of the running container, for the commands that run
# after `up` and are not told either.
broker_image() {
  docker container inspect -f '{{.Config.Image}}' "$NAME" 2>/dev/null \
    || die "no container named $NAME; run '$0 up <image>' first"
}
broker_mode() {
  docker container inspect -f "{{index .Config.Labels \"$MODE_LABEL\"}}" "$NAME" 2>/dev/null \
    || die "no container named $NAME; run '$0 up <image>' first"
}

# The port the broker's own tools use: the client listener when it is
# plaintext, otherwise the unpublished admin listener.
admin_port() {
  if [ "$(broker_mode)" = plain ]; then echo 9092; else echo "$ADMIN_PORT"; fi
}

# Certificates for the TLS modes, made with openssl on the host: a CA, a
# server certificate it signed (names: localhost, the advertised host, the
# container name, 127.0.0.1), and a second CA that signed nothing, for the test
# that gives a client the wrong authority. The broker reads the PEM keystore
# (key then certificate); the tests read ca.pem and other-ca.pem. Test
# material, valid for a week, never reused between runs.
make_certs() {
  command -v openssl >/dev/null 2>&1 || die "openssl is needed to make the broker's certificates"
  # Only a directory this script made is ever emptied, so a KAFKA_CERT_DIR
  # that names something else is refused and not deleted.
  if [ -e "$CERT_DIR" ] && [ ! -e "$CERT_DIR/$CERT_MARKER" ]; then
    die "KAFKA_CERT_DIR $CERT_DIR exists and is not a certificate directory of this script; name a new one"
  fi
  rm -rf "$CERT_DIR"
  mkdir -p "$CERT_DIR"
  : >"$CERT_DIR/$CERT_MARKER"
  (
    cd "$CERT_DIR"
    openssl req -x509 -newkey rsa:2048 -nodes -days 7 -subj "/CN=vtop-test-ca" \
      -keyout ca.key -out ca.pem 2>/dev/null
    openssl req -x509 -newkey rsa:2048 -nodes -days 7 -subj "/CN=vtop-other-ca" \
      -keyout other-ca.key -out other-ca.pem 2>/dev/null
    openssl req -newkey rsa:2048 -nodes -subj "/CN=$ADVERTISED_HOST" \
      -keyout broker.key -out broker.csr 2>/dev/null
    printf 'subjectAltName=DNS:localhost,DNS:%s,DNS:%s,IP:127.0.0.1\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n' \
      "$ADVERTISED_HOST" "$NAME" >broker.ext
    openssl x509 -req -in broker.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
      -days 7 -extfile broker.ext -out broker.crt 2>/dev/null
    openssl pkcs8 -topk8 -nocrypt -in broker.key -out broker.pk8
    cat broker.pk8 broker.crt >broker.pem
    rm -f broker.csr broker.ext broker.key broker.pk8 broker.crt ca.srl ca.key other-ca.key
  )
  # The broker runs as another user, and reads the mount.
  chmod 755 "$CERT_DIR"
  chmod 644 "$CERT_DIR"/*.pem
}

cmd_up() {
  local image="${1:-}" mode="${2:-plain}"
  [ -n "$image" ] || die "usage: $0 up <image> [mode]"
  case "$mode" in
    plain | tls | sasl-plaintext | sasl-ssl) ;;
    *) die "unknown mode '$mode'; known: plain, tls, sasl-plaintext, sasl-ssl" ;;
  esac
  if is_redpanda "$image" && [ "$mode" != plain ]; then
    die "a Redpanda broker is started in plain mode only, not '$mode'"
  fi
  if docker container inspect "$NAME" >/dev/null 2>&1; then
    die "a container named $NAME already exists; run '$0 down' first"
  fi

  local args=(--detach --name "$NAME" --hostname "$NAME" --label "$MODE_LABEL=$mode")
  [ -n "$PUBLISH_PORT" ] && args+=(--publish "$PUBLISH_PORT:9092")
  [ -n "$NETWORK" ] && args+=(--network "$NETWORK")
  [ -n "${KAFKA_BROKER_CPUS:-}" ] && args+=(--cpus "$KAFKA_BROKER_CPUS")
  [ -n "${KAFKA_BROKER_MEMORY:-}" ] && args+=(--memory "$KAFKA_BROKER_MEMORY")

  local command=()
  if is_redpanda "$image"; then
    # One core, one gigabyte, no host tuning checks: what a container on a CI
    # runner can honour. The container's own rpk is the admin client.
    command=(redpanda start --mode dev-container --smp 1 --memory 1G
      --reserve-memory 0M --overprovisioned --node-id 0 --check=false
      --kafka-addr "PLAINTEXT://0.0.0.0:9092"
      --advertise-kafka-addr "PLAINTEXT://$ADVERTISED_HOST:9092")
  else
    # The single-node KRaft layout the kafka-integration job in ci.yml has
    # always used: broker and controller in one process, every internal topic
    # at replication 1, and no initial rebalance delay so a test's first poll
    # is not spent waiting for one.
    local listeners advertised protocols
    case "$mode" in
      plain)
        listeners="PLAINTEXT://:9092,CONTROLLER://:9093"
        advertised="PLAINTEXT://$ADVERTISED_HOST:9092"
        protocols="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
        ;;
      *)
        # The client listener is named CLIENT because the image turns an
        # underscore in a setting's name into a dot, and SASL_SSL has one.
        local client_protocol
        case "$mode" in
          tls) client_protocol=SSL ;;
          sasl-plaintext) client_protocol=SASL_PLAINTEXT ;;
          sasl-ssl) client_protocol=SASL_SSL ;;
        esac
        listeners="CLIENT://:9092,ADMIN://:$ADMIN_PORT,CONTROLLER://:9093"
        advertised="CLIENT://$ADVERTISED_HOST:9092,ADMIN://localhost:$ADMIN_PORT"
        protocols="CONTROLLER:PLAINTEXT,ADMIN:PLAINTEXT,CLIENT:$client_protocol"
        args+=(-e "KAFKA_INTER_BROKER_LISTENER_NAME=ADMIN")
        ;;
    esac
    case "$mode" in
      tls | sasl-ssl)
        make_certs
        args+=(
          -v "$CERT_DIR:$CERTS_IN_BROKER:ro"
          -e "KAFKA_SSL_KEYSTORE_TYPE=PEM"
          -e "KAFKA_SSL_KEYSTORE_LOCATION=$CERTS_IN_BROKER/broker.pem"
          -e "KAFKA_SSL_CLIENT_AUTH=none"
        )
        ;;
    esac
    case "$mode" in
      sasl-*)
        # `___` is a dash and `__` an underscore in a setting's name, so
        # SCRAM___SHA___512 is the mechanism SCRAM-SHA-512.
        args+=(
          -e "KAFKA_SASL_ENABLED_MECHANISMS=${SASL_MECHANISMS// /,}"
          -e "KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG=org.apache.kafka.common.security.plain.PlainLoginModule required user_${SASL_USER}=\"${SASL_PASSWORD}\";"
          -e "KAFKA_LISTENER_NAME_CLIENT_SCRAM___SHA___256_SASL_JAAS_CONFIG=org.apache.kafka.common.security.scram.ScramLoginModule required;"
          -e "KAFKA_LISTENER_NAME_CLIENT_SCRAM___SHA___512_SASL_JAAS_CONFIG=org.apache.kafka.common.security.scram.ScramLoginModule required;"
        )
        ;;
    esac
    args+=(
      -e "KAFKA_NODE_ID=1"
      -e "KAFKA_PROCESS_ROLES=broker,controller"
      -e "KAFKA_LISTENERS=$listeners"
      -e "KAFKA_ADVERTISED_LISTENERS=$advertised"
      -e "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093"
      -e "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER"
      -e "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=$protocols"
      -e "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1"
      -e "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1"
      -e "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1"
      -e "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0"
    )
  fi
  docker run "${args[@]}" "$image" ${command[@]+"${command[@]}"} >/dev/null

  # Answering a topic list is what the CI job's health check asked for. A
  # broker that never gets there fails with its last answer and its log, not
  # a bare timeout.
  local deadline=$((SECONDS + UP_TIMEOUT)) out=""
  until out=$(admin_answers 2>&1); do
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

  case "$mode" in
    sasl-*) create_scram_users "$deadline" ;;
  esac
  case "$mode" in
    plain) ;;
    *) wait_for_client_listener "$mode" "$deadline" ;;
  esac
  echo "kafka-broker: $NAME is up ($image, mode $mode, advertised as $ADVERTISED_HOST:9092)"
}

# Whether the broker answers over the listener that needs no credentials.
admin_answers() {
  if is_redpanda "$(broker_image)"; then
    in_broker rpk topic list
  else
    in_broker "$BIN/kafka-topics.sh" --bootstrap-server "localhost:$(admin_port)" --list
  fi
}

# SCRAM credentials live in the cluster metadata, not in the JAAS file, so
# they are created over the admin listener once the broker answers. PLAIN's
# user is in the JAAS file already.
create_scram_users() {
  local deadline="$1" out="" mechanism
  # One mechanism per request: the broker refuses a user altered twice in one.
  for mechanism in SCRAM-SHA-256 SCRAM-SHA-512; do
    until out=$(in_broker "$BIN/kafka-configs.sh" --bootstrap-server "localhost:$ADMIN_PORT" \
      --alter --entity-type users --entity-name "$SASL_USER" \
      --add-config "$mechanism=[iterations=4096,password=$SASL_PASSWORD]" 2>&1); do
      if [ "$SECONDS" -ge "$deadline" ]; then
        die "could not create the $mechanism credential within ${UP_TIMEOUT}s; last answer: $out"
      fi
      sleep 2
    done
  done
}

# The client settings file a tool in the container uses to talk over the
# client listener, the way the engine's client does.
write_client_properties() {
  local mode="$1" mechanism="$2" file=/tmp/vtop-client.properties lines=()
  case "$mode" in
    tls) lines+=("security.protocol=SSL") ;;
    sasl-plaintext) lines+=("security.protocol=SASL_PLAINTEXT") ;;
    sasl-ssl) lines+=("security.protocol=SASL_SSL") ;;
  esac
  case "$mode" in
    tls | sasl-ssl)
      lines+=("ssl.truststore.type=PEM" "ssl.truststore.location=$CERTS_IN_BROKER/ca.pem")
      ;;
  esac
  case "$mode" in
    sasl-*)
      local module=org.apache.kafka.common.security.scram.ScramLoginModule
      [ "$mechanism" = PLAIN ] && module=org.apache.kafka.common.security.plain.PlainLoginModule
      lines+=("sasl.mechanism=$mechanism"
        "sasl.jaas.config=$module required username=\"$SASL_USER\" password=\"$SASL_PASSWORD\";")
      ;;
  esac
  printf '%s\n' "${lines[@]}" | docker exec -i "$NAME" sh -c "cat > $file"
}

# A listener that is up and a credential that is accepted are two facts: the
# tests would otherwise start against a listener whose SCRAM credentials have
# not reached it yet, and fail for the broker's start-up. Every mechanism the
# mode offers must list the topics, over the published listener, before `up`
# returns.
wait_for_client_listener() {
  local mode="$1" deadline="$2" mechanism out=""
  local mechanisms="-"
  case "$mode" in sasl-*) mechanisms="$SASL_MECHANISMS" ;; esac
  for mechanism in $mechanisms; do
    write_client_properties "$mode" "$mechanism"
    until out=$(in_broker "$BIN/kafka-topics.sh" --bootstrap-server "localhost:9092" \
      --command-config /tmp/vtop-client.properties --list 2>&1); do
      if [ "$SECONDS" -ge "$deadline" ]; then
        docker logs --tail 50 "$NAME" >&2 || true
        die "the $mode listener did not accept $mechanism within ${UP_TIMEOUT}s; last answer: $out"
      fi
      sleep 2
    done
  done
}

cmd_version() {
  if is_redpanda "$(broker_image)"; then
    in_broker rpk version | awk '/^rpk version:/ { print $3; exit }'
  else
    in_broker "$BIN/kafka-topics.sh" --version | awk 'NF { print $1; exit }'
  fi
}

cmd_seed() {
  local image port
  image=$(broker_image)
  if is_redpanda "$image"; then
    # The tests assert a NON-EMPTY read of this topic, so an empty topic
    # fails them rather than letting them pass vacuously.
    in_broker rpk topic create "$SEED_TOPIC" --partitions 1 --replicas 1
    bash "$HERE/../docker/seed-events.sh" cef "$SEED_RECORDS" \
      | docker exec -i "$NAME" rpk topic produce "$SEED_TOPIC" >/dev/null
    in_broker rpk topic describe "$SEED_TOPIC" --print-partitions
    # The same group-coordinator warm-up as below.
    in_broker rpk topic consume "$SEED_TOPIC" --group ci-warmup --offset start --num 1 >/dev/null
    in_broker rpk group list
    return
  fi
  port=$(admin_port)
  in_broker "$BIN/kafka-topics.sh" --bootstrap-server "localhost:$port" \
    --create --if-not-exists --topic "$SEED_TOPIC" --partitions 1 --replication-factor 1
  bash "$HERE/../docker/seed-events.sh" cef "$SEED_RECORDS" \
    | docker exec -i "$NAME" "$BIN/kafka-console-producer.sh" \
      --bootstrap-server "localhost:$port" --topic "$SEED_TOPIC"
  in_broker "$BIN/kafka-get-offsets.sh" --bootstrap-server "localhost:$port" --topic "$SEED_TOPIC"

  # Warm up the group coordinator. The adapter asks the broker for the
  # group's committed offsets, which needs the coordinator and the
  # __consumer_offsets topic; a fresh broker creates them only on the first
  # group operation, and until then every read fails with NotCoordinator.
  # Consuming once under a throwaway group forces them into existence.
  in_broker "$BIN/kafka-console-consumer.sh" --bootstrap-server "localhost:$port" \
    --topic "$SEED_TOPIC" --group ci-warmup --from-beginning --max-messages 1 \
    --timeout-ms 30000 >/dev/null
  in_broker "$BIN/kafka-consumer-groups.sh" --bootstrap-server "localhost:$port" --list
}

# The settings tests/kafka_broker_env.rs reads, for the running broker, as
# KEY=VALUE lines: a workflow appends them to $GITHUB_ENV, a shell exports
# them. The mechanism matters only in a SASL mode (default SCRAM-SHA-512).
cmd_client_env() {
  local mechanism="${1:-SCRAM-SHA-512}" mode
  mode=$(broker_mode)
  echo "VTOP_TEST_KAFKA=$ADVERTISED_HOST:9092"
  case "$mode" in
    plain) return ;;
    tls) echo "VTOP_TEST_KAFKA_SECURITY_PROTOCOL=SSL" ;;
    sasl-plaintext) echo "VTOP_TEST_KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT" ;;
    sasl-ssl) echo "VTOP_TEST_KAFKA_SECURITY_PROTOCOL=SASL_SSL" ;;
  esac
  case "$mode" in
    sasl-*)
      case " $SASL_MECHANISMS " in
        *" $mechanism "*) ;;
        *) die "unknown SASL mechanism '$mechanism'; known: $SASL_MECHANISMS" ;;
      esac
      echo "VTOP_TEST_KAFKA_SASL_MECHANISM=$mechanism"
      echo "VTOP_TEST_KAFKA_SASL_USERNAME=$SASL_USER"
      echo "VTOP_TEST_KAFKA_SASL_PASSWORD=$SASL_PASSWORD"
      echo "VTOP_TEST_KAFKA_SASL_WRONG_PASSWORD=$SASL_WRONG_PASSWORD"
      ;;
  esac
  case "$mode" in
    tls | sasl-ssl)
      echo "VTOP_TEST_KAFKA_SSL_CA=$CERT_DIR/ca.pem"
      echo "VTOP_TEST_KAFKA_SSL_OTHER_CA=$CERT_DIR/other-ca.pem"
      ;;
  esac
}

cmd_logs() {
  docker logs "$NAME"
}

cmd_down() {
  docker rm --force --volumes "$NAME" >/dev/null 2>&1 || true
  if [ -e "$CERT_DIR/$CERT_MARKER" ]; then rm -rf "$CERT_DIR"; fi
}

sub="${1:-}"
[ "$#" -gt 0 ] && shift
case "$sub" in
  up) cmd_up "$@" ;;
  version) cmd_version ;;
  seed) cmd_seed ;;
  client-env) cmd_client_env "$@" ;;
  logs) cmd_logs ;;
  down) cmd_down ;;
  *) die "usage: $0 {up <image> [mode]|version|seed|client-env [mechanism]|logs|down}" ;;
esac
