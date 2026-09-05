#!/usr/bin/env bash
# 16 — the Kafka gateway on a standalone node (#225): a data node serves
# Kafka's wire protocol beside its native plane, and a client that knows only
# Kafka's framing can ask it what it is and what it holds.
#
# The gateway is `vtop-kafka` wired into `vtop-node`: a `kafka:` block on a
# leader or standalone node binds a listener next to the native one, backed
# by the node's own broker. This scenario proves the WIRING — the config
# reaches the listener, the listener answers Kafka frames, and Metadata names
# the range's topic — with nothing but bash and /dev/tcp, so the harness
# stays free of a Kafka client. The stock-client round trip (kafka-console-*,
# librdkafka) is the issue's acceptance and needs a client the lab does not
# carry; it lives in a compose lane.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

KAFKA_PORT="${CHAOS_KAFKA_PORT:-$((NATIVE_PORT + 20))}"
require_integer_in_range CHAOS_KAFKA_PORT "$KAFKA_PORT" 1024 65535

CFG="$WORKDIR/data-standalone-kafka.yaml"
{
  cat "$(emit_leader_config standalone)"
  # Loopback only, the default the node enforces: Kafka's protocol carries
  # no vtop identity, so the listener admits whoever reaches it.
  echo "kafka: { listen: \"127.0.0.1:$KAFKA_PORT\" }"
} | install_config "$CFG"
[[ -n "${TOPIC:-}" ]] || fail "lib.sh did not set TOPIC"

PID="$(start_node "data-standalone-kafka" "data_node_ready" data --config "$CFG")"
grep -q "kafka=127.0.0.1:$KAFKA_PORT" "$WORKDIR/logs/data-standalone-kafka.log" \
  || fail "the ready line does not name the kafka listener (see $WORKDIR/logs/data-standalone-kafka.log)"
grep -q "speaks Kafka's protocol, which carries no vtop identity" "$WORKDIR/logs/data-standalone-kafka.log" \
  || fail "the node did not warn that the gateway is unauthenticated"
log "standalone node up with a kafka listener on 127.0.0.1:$KAFKA_PORT (pid $PID)"

# One Kafka exchange over /dev/tcp: a length-prefixed request in hex, the
# length-prefixed reply back as hex.
kafka_exchange() {
  local body_hex="$1"
  local len=$(( ${#body_hex} / 2 ))
  exec 3<>"/dev/tcp/127.0.0.1/$KAFKA_PORT"
  # Bytes as %b data, never as a format string: the length as four escapes,
  # then every hex pair of the body as one.
  local escaped i
  escaped="$(printf '\\x%02x\\x%02x\\x%02x\\x%02x' $(( (len >> 24) & 255 )) $(( (len >> 16) & 255 )) $(( (len >> 8) & 255 )) $(( len & 255 )))"
  for (( i = 0; i < ${#body_hex}; i += 2 )); do
    escaped+="\\x${body_hex:i:2}"
  done
  printf '%b' "$escaped" >&3
  local reply_len_hex reply_len
  reply_len_hex="$(head -c 4 <&3 | od -An -tx1 | tr -d ' \n')"
  reply_len=$((16#$reply_len_hex))
  head -c "$reply_len" <&3 | od -An -tx1 | tr -d ' \n'
  exec 3>&-
}

# ApiVersions v0, correlation 7, client id "chaos": the reply carries the
# correlation back, no error, and the five served api keys.
REPLY="$(kafka_exchange "001200000000000700056368616f73")"
[[ "$REPLY" == 000000070000* ]] \
  || fail "ApiVersions v0 did not answer correlation 7 without error: $REPLY"
[[ "$REPLY" == 00000007000000000005* ]] \
  || fail "ApiVersions v0 did not list five api keys: $REPLY"
log "ApiVersions v0 answered: correlation echoed, no error, five api keys served"

# Metadata v1, correlation 8, every topic (a null array): the reply names
# the range's topic and this gateway as its one broker.
REPLY="$(kafka_exchange "000300010000000800056368616f73ffffffff")"
TOPIC_HEX="$(printf '%s' "$TOPIC" | od -An -tx1 | tr -d ' \n')"
[[ "$REPLY" == 00000008* ]] || fail "Metadata v1 did not answer correlation 8: $REPLY"
[[ "$REPLY" == *"$TOPIC_HEX"* ]] \
  || fail "Metadata v1 does not name the range topic $TOPIC: $REPLY"
log "Metadata v1 names the range topic '$TOPIC' behind the gateway"

# A version this gateway does not serve is refused, not mis-parsed: Produce
# v2 closes the connection.
if REPLY="$(kafka_exchange "000000020000000900056368616f73" 2>/dev/null)" && [[ -n "$REPLY" ]]; then
  fail "Produce v2 (below the served range) was answered rather than refused: $REPLY"
fi
log "a version outside the served range is refused by closing the connection"

stop_pid "$PID" "data-standalone-kafka" 2>/dev/null || kill "$PID" 2>/dev/null || true
log "PASS"
