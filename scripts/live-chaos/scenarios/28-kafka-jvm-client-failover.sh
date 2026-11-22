#!/usr/bin/env bash
# 28 — the JVM Kafka client follows a failover, under both metadata recovery
# strategies (#751).
#
# Scenario 27 is librdkafka. The JVM client is the other half of "a stock
# Kafka client", and it is not the same client: it keeps no bootstrap list once
# it holds a metadata response, and which recovery it does when its leader
# dies is a setting, `metadata.recovery.strategy` (KIP-899):
#
#   rebootstrap  the 4.x default: if no broker it knows answers for
#                `metadata.recovery.rebootstrap.trigger.ms` it starts over from
#                the bootstrap list;
#   none         the client only ever refreshes from brokers it has been told
#                about.
#
# Neither is a plan for a failover, and that is the point of listing every
# member in `kafka.brokers`: the client never loses all of them, so its
# ordinary metadata refresh finds the new leader under EITHER strategy. This
# scenario runs the same failover once per strategy and reports, for each, the
# time from the kill to the first acknowledgement after it.
#
# Per strategy, on a fresh cluster (three candidates serving Kafka, as 27):
#   1. kafka-verifiable-producer, the Apache Kafka distribution's own test
#      client, with enable.idempotence=true and acks=all, produces a paced
#      stream bootstrapped from all three;
#   2. after a number of acknowledgements the leader is SIGKILLed; nothing else
#      is touched, and the producer is not restarted;
#   3. the producer finishes with every send acknowledged and none failed;
#   4. kafka-console-consumer reads partition 0 back from the beginning: every
#      acknowledged value is at exactly the offset it was acknowledged at,
#      once, and the values are in produced order.
#
# The client is the one in an Apache Kafka distribution: CHAOS_KAFKA_HOME names
# an unpacked one, or CHAOS_KAFKA_IMAGE an apache/kafka image to copy
# /opt/kafka out of with docker (the digest-pinned images .github/workflows/
# kafka-matrix.yml tests). A JVM (17 or newer) is on PATH either way. With
# neither set the scenario SKIPS (exit 77, which run-all.sh reports as SKIP),
# before the harness is loaded: a workstation has no Kafka distribution to
# hand, and that is not a failure of this build.
KAFKA_HOME="${CHAOS_KAFKA_HOME:-}"
KAFKA_IMAGE="${CHAOS_KAFKA_IMAGE:-}"
if [[ -z "$KAFKA_HOME" && -z "$KAFKA_IMAGE" ]]; then
  echo "SKIP: set CHAOS_KAFKA_HOME to an unpacked Apache Kafka distribution, or CHAOS_KAFKA_IMAGE to an apache/kafka image"
  exit 77
fi
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

# A v2 range, as 27.
export CHAOS_SEGMENT_FORMAT=v2

require_binaries
require_command java "Install a JVM, 17 or newer: the Kafka distribution's client tools run on it."
transport_plaintext && fail "scenario 28 needs TLS: candidates cannot run under CHAOS_TRANSPORT=plaintext"
init_workdir
BASE_WORKDIR="$WORKDIR"

RECORDS="${CHAOS_KAFKA_RECORDS:-3000}"
KILL_AFTER_ACKS="${CHAOS_KAFKA_KILL_AFTER_ACKS:-600}"
RATE="${CHAOS_KAFKA_RATE:-100}"
FAILOVER_LIMIT_SECONDS="${CHAOS_KAFKA_FAILOVER_LIMIT_SECONDS:-60}"
STRATEGIES="${CHAOS_KAFKA_STRATEGIES:-rebootstrap none}"
require_integer_in_range CHAOS_KAFKA_RECORDS "$RECORDS" 1000 10000000
RECORDS=$((10#$RECORDS))
require_integer_in_range CHAOS_KAFKA_KILL_AFTER_ACKS "$KILL_AFTER_ACKS" 1 "$((RECORDS / 2))"
KILL_AFTER_ACKS=$((10#$KILL_AFTER_ACKS))
require_integer_in_range CHAOS_KAFKA_RATE "$RATE" 10 100000
RATE=$((10#$RATE))
require_integer_in_range CHAOS_KAFKA_FAILOVER_LIMIT_SECONDS "$FAILOVER_LIMIT_SECONDS" 1 3600
for strategy in $STRATEGIES; do
  case "$strategy" in
    rebootstrap | none) ;;
    *) fail "CHAOS_KAFKA_STRATEGIES lists '$strategy': metadata.recovery.strategy is rebootstrap or none" ;;
  esac
done

# --- the Apache Kafka distribution -------------------------------------------
if [[ -z "$KAFKA_HOME" ]]; then
  require_command docker "CHAOS_KAFKA_IMAGE needs docker to copy the distribution out of the image."
  KAFKA_HOME="$BASE_WORKDIR/kafka"
  container="$(docker create "$KAFKA_IMAGE")" \
    || fail "could not create a container from CHAOS_KAFKA_IMAGE=$KAFKA_IMAGE"
  docker cp "$container:/opt/kafka" "$KAFKA_HOME" > /dev/null \
    || { docker rm "$container" > /dev/null 2>&1 || true; fail "$KAFKA_IMAGE holds no /opt/kafka"; }
  docker rm "$container" > /dev/null
fi
for tool in kafka-verifiable-producer kafka-console-consumer; do
  [[ -x "$KAFKA_HOME/bin/$tool.sh" ]] || fail "$KAFKA_HOME/bin/$tool.sh is not an executable Kafka tool"
done
clients_jar=("$KAFKA_HOME"/libs/kafka-clients-*.jar)
log "Kafka client tools: $KAFKA_HOME (${clients_jar[0]##*/}), JVM: $(java -version 2>&1 | head -1)"

# run_variant <strategy> — one failover, on a fresh cluster in its own
# directory, so the two runs share nothing but the binaries and the
# certificates.
run_variant() {
  local strategy="$1"
  WORKDIR="$BASE_WORKDIR/$strategy"
  mkdir -p "$WORKDIR/logs"
  : > "$WORKDIR/pids"
  SCENARIO="27/$strategy"
  log "--- metadata.recovery.strategy=$strategy ---"

  bring_up_kafka_candidates

  local props="$WORKDIR/producer.properties" out="$WORKDIR/producer-out.txt"
  {
    echo "enable.idempotence=true"
    # The tool sends with no retries unless told; an idempotent producer is
    # refused with none, and it is the retries that carry a send across a
    # failover. The client default (the delivery timeout is what bounds them).
    echo "retries=2147483647"
    echo "metadata.recovery.strategy=$strategy"
  } > "$props"
  : > "$out"
  # The distribution's scripts pick JVM flags from the environment; the tools'
  # default heap is small and is left alone.
  "$KAFKA_HOME/bin/kafka-verifiable-producer.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$TOPIC" \
    --max-messages "$RECORDS" --throughput "$RATE" --acks -1 \
    --producer.config "$props" \
    > >(stamp_stdin_ms > "$out") 2> "$WORKDIR/logs/jvm-producer.err" &
  local producer=$!
  echo "$producer" >> "$WORKDIR/pids"
  log "kafka-verifiable-producer (pid $producer, idempotent, acks=all) streaming $RECORDS records at $RATE/s, bootstrapped from $KAFKA_BOOTSTRAP"

  acked_count() { grep -c '"name":"producer_send_success"' "$out" || true; }

  local deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS + 30))
  until [[ "$(acked_count)" -ge "$KILL_AFTER_ACKS" ]]; do
    kill -0 "$producer" 2>/dev/null \
      || fail "the producer ended before $KILL_AFTER_ACKS acknowledgements: $(tail -3 "$WORKDIR/logs/jvm-producer.err")"
    [[ $SECONDS -lt $deadline ]] \
      || fail "the producer reached only $(acked_count) acknowledgements in time: $(tail -3 "$WORKDIR/logs/jvm-producer.err")"
    sleep 0.1
  done
  log "$(acked_count) acknowledgements through candidate $WINNER"

  local kill_ms
  kill_ms="$(wall_ms)"
  kill9_pid "${CAND_PIDS[$WINNER]}"
  log "candidate $WINNER (the leader) SIGKILLed with the producer mid-stream"

  local new_holder new_epoch new_leader
  read -r new_holder new_epoch <<<"$(await_lease_holder_changed "$META_LEADER_ID" "$HOLDER" \
    $((LEASE_DURATION_MS / 1000 + ELECTION_TIMEOUT_SECONDS)))"
  new_leader="$(candidate_by_uuid "$new_holder")"
  [[ "$new_epoch" -gt "$EPOCH" ]] || fail "a new hold must mint a newer epoch: old $EPOCH, new $new_epoch"
  log "candidate $new_leader took the range at epoch $new_epoch, no restart"
  await_log_line "$WORKDIR/logs/data-candidate-$new_leader.log" \
    "kafka gateway for range .* is now serving at epoch $new_epoch" \
    "the promoted candidate's gateway must say it is serving, at the new epoch"

  local finish_deadline=$((SECONDS + FAILOVER_LIMIT_SECONDS + RECORDS / RATE + PROGRESS_TIMEOUT_SECONDS))
  while kill -0 "$producer" 2>/dev/null; do
    [[ $SECONDS -lt $finish_deadline ]] \
      || fail "the producer had not finished by the deadline, with $(acked_count) of $RECORDS acknowledged: $(tail -3 "$WORKDIR/logs/jvm-producer.err")"
    sleep 0.2
  done
  local producer_exit=0
  wait "$producer" || producer_exit=$?
  for _ in $(seq 1 50); do
    grep -q '"name":"shutdown_complete"' "$out" && break
    sleep 0.1
  done
  [[ $producer_exit -eq 0 ]] \
    || fail "the producer exited $producer_exit after the failover: $(tail -3 "$WORKDIR/logs/jvm-producer.err")"
  ! grep -q '"name":"producer_send_error"' "$out" \
    || fail "a send failed across the failover: $(grep -m 2 '"name":"producer_send_error"' "$out")"
  [[ "$(acked_count)" -eq "$RECORDS" ]] \
    || fail "$(acked_count) of $RECORDS sends were acknowledged"
  log "the same producer finished all $RECORDS sends across the failover, none failed"

  local first resume silence
  read -r first resume silence < <(ack_outage_report "$out" "$kill_ms" '"name":"producer_send_success"')
  log "kill to first acknowledgement after it: ${first} ms; kill to the end of the outage the client saw: ${resume} ms (longest silence ${silence} ms)"
  [[ "$resume" -le $((FAILOVER_LIMIT_SECONDS * 1000)) ]] \
    || fail "the client waited ${resume} ms from the kill for an acknowledgement, over the limit of ${FAILOVER_LIMIT_SECONDS} s"
  printf 'KAFKA_FAILOVER client=jvm strategy=%s kill_to_first_ack_ms=%s kill_to_resume_ms=%s longest_silence_ms=%s records=%s\n' \
    "$strategy" "$first" "$resume" "$silence" "$RECORDS" | tee -a "$BASE_WORKDIR/failover-results.txt"

  # Every acknowledgement is a (value, offset) the client was given. The
  # readback prints each record's offset and value, so each acknowledged
  # record can be looked up where it was acknowledged: present, at that
  # offset, once, with the values in produced order.
  local readback="$WORKDIR/readback.txt" formatter_option=--property
  # Newer distributions name the formatter's option differently and print a
  # deprecation notice on stdout for the old one.
  # (A herestring, not a pipe: grep -q closing early would fail the pipeline.)
  local consumer_help
  consumer_help="$("$KAFKA_HOME/bin/kafka-console-consumer.sh" --help 2>&1 || true)"
  [[ "$consumer_help" == *--formatter-property* ]] && formatter_option=--formatter-property
  "$KAFKA_HOME/bin/kafka-console-consumer.sh" \
    --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$TOPIC" --partition 0 --offset earliest \
    --max-messages "$RECORDS" --timeout-ms 60000 \
    "$formatter_option" print.offset=true "$formatter_option" print.value=true \
    > "$readback" 2> "$WORKDIR/logs/jvm-consumer.err" \
    || fail "the readback failed: $(tail -3 "$WORKDIR/logs/jvm-consumer.err")"
  python3 -c '
import json, sys
acks, readback, expected = sys.argv[1], sys.argv[2], int(sys.argv[3])
acked = []
for line in open(acks):
    _, _, rest = line.partition(" ")
    if "\"name\":\"producer_send_success\"" in rest:
        event = json.loads(rest)
        acked.append((int(event["offset"]), str(event["value"])))
if len(acked) != expected:
    sys.exit("%d acknowledgements, expected %d" % (len(acked), expected))
read = []
for line in open(readback):
    head, _, value = line.rstrip("\n").partition("\t")
    if not head.startswith("Offset:"):
        sys.exit("unexpected readback line: %r" % line)
    read.append((int(head[len("Offset:"):]), value))
if len(read) != expected:
    sys.exit("read back %d records, %d were acknowledged" % (len(read), expected))
if len({value for _, value in read}) != len(read):
    sys.exit("a value was read back twice")
if read != acked:
    for position, (a, r) in enumerate(zip(acked, read)):
        if a != r:
            sys.exit("record %d: acknowledged %r, read back %r" % (position, a, r))
    sys.exit("acknowledged and read-back records differ")
if [offset for offset, _ in read] != sorted(offset for offset, _ in read):
    sys.exit("offsets are not increasing")
' "$out" "$readback" "$RECORDS" \
    || fail "the acknowledged records and the readback disagree (see $readback and $out)"
  log "all $RECORDS acknowledged records read back at the offsets they were acknowledged at, once each, in order"

  local n
  for n in 1 2 3; do
    kill9_pid "${CAND_PIDS[$n]}"
  done
  local pid
  while read -r pid; do
    kill -9 "$pid" 2>/dev/null || true
  done < "$WORKDIR/pids"
  wait 2> /dev/null || true
}

: > "$BASE_WORKDIR/failover-results.txt"
for strategy in $STRATEGIES; do
  run_variant "$strategy"
done
WORKDIR="$BASE_WORKDIR"
SCENARIO="28-kafka-jvm-client-failover"
log "results:"
sed 's/^/  /' "$BASE_WORKDIR/failover-results.txt"
log "PASS"
