#!/usr/bin/env bash
# 25 — group commit under a leader kill (#752): the data node's group commit is
# switched on (`group_commit: {enabled: true}`), FOUR idempotent librdkafka
# producers (`kcat`, enable.idempotence=true, acks=all) stream keyed records
# through the gateway of one leader with two followers, and the leader is
# SIGKILLed mid-stream. The same config restarts it. The producers' connections
# drop, they reconnect and retry the batches they never saw acknowledged, and
# every retry meets the log's per-record duplicate check. Each producer's
# records read back byte-exact and in the order it sent them, no key twice,
# none missing, and every follower holds all of them.
#
# What this adds to scenarios 17 and 18: the commit path is the shared one.
# Concurrent requests from four connections are folded into groups that share
# ONE local fsync and ONE replication round, and a request is acknowledged only
# after both. A kill between a group's append and its acknowledgement leaves
# members the client never heard about: if a group could be acknowledged before
# its barrier, or half-applied, or a retry could append twice, the read-back
# would miss a record or show one twice. The counters prove the groups were
# shared before the kill (`requests_total` greater than `commits_total`), and
# that the restarted node batches again.
#
# Why a restart and not a failover: a Kafka client cannot yet follow a failover
# through a candidate node (#751), so the leader returns on its own address.
# Safety does not depend on that; the data this checks is what survives the
# kill.
#
# `CHAOS_GROUP_COMMIT_DELAY_MS` (default 2) is the group's window. The shipped
# default is 0 (natural batching), under which sharing depends on the barrier
# being longer than the gap between requests; a loopback lab's barrier is
# short, so a 2 ms window makes the sharing the scenario asserts reliable
# without changing what is being proven.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
require_binaries

KCAT="$(command -v kcat || command -v kafkacat || true)"
[[ -n "$KCAT" ]] || fail "scenario 25 needs kcat, librdkafka's client: brew install kcat / apt-get install kafkacat"

init_workdir

KAFKA_PORT="$(kafka_port 5)"
PRODUCERS=4
PER_PRODUCER="${CHAOS_GC_RECORDS:-4000}"
require_integer_in_range CHAOS_GC_RECORDS "$PER_PRODUCER" 1000 2000000
RECORDS=$(( PRODUCERS * 10#$PER_PRODUCER ))
ACK_FLOOR="${CHAOS_GC_ACK_FLOOR:-$(( RECORDS / 5 ))}"
PACE_EVERY="${CHAOS_GC_PACE_EVERY:-$(( 10#$PER_PRODUCER / 40 ))}"
PACE_SLEEP="${CHAOS_GC_PACE_SLEEP:-0.2}"
DELAY_MS="${CHAOS_GROUP_COMMIT_DELAY_MS:-2}"
REQUEST_TIMEOUT_MS="${CHAOS_GC_REQUEST_TIMEOUT_MS:-3000}"
[[ "$PACE_SLEEP" =~ ^[0-9]+([.][0-9]+)?$ ]] \
  || fail "CHAOS_GC_PACE_SLEEP must be a non-negative decimal, got '$PACE_SLEEP'"
require_integer_in_range CHAOS_GC_ACK_FLOOR "$ACK_FLOOR" 1 "$RECORDS"
require_integer_in_range CHAOS_GC_PACE_EVERY "$PACE_EVERY" 1 "$PER_PRODUCER"
require_integer_in_range CHAOS_GROUP_COMMIT_DELAY_MS "$DELAY_MS" 0 999
require_integer_in_range CHAOS_GC_REQUEST_TIMEOUT_MS "$REQUEST_TIMEOUT_MS" 1000 60000
[[ -n "${TOPIC:-}" ]] || fail "lib.sh did not set TOPIC"

F1_PID="$(start_follower 1)"
F2_PID="$(start_follower 2)"
CFG="$WORKDIR/data-leader-gc.yaml"
{
  cat "$(emit_leader_config leader)"
  echo "kafka: { listen: \"127.0.0.1:$KAFKA_PORT\" }"
  echo "group_commit: { enabled: true, max_delay_ms: $DELAY_MS }"
} | install_config "$CFG"
LEADER_PID="$(start_node "data-leader-gc" "data_node_ready" data --config "$CFG")"
await_ready "$(data_metrics_addr 0)" "data-leader-gc"
LEADER_LOG="$WORKDIR/logs/data-leader-gc.log"
grep -q "group commit on for range" "$LEADER_LOG" \
  || fail "the leader did not say at startup that group commit is on (see $LEADER_LOG)"
[[ "$(sample_metric "$(data_metrics_addr 0)" vtop_broker_group_commit_enabled)" == "1" ]] \
  || fail "vtop_broker_group_commit_enabled is not 1 on a leader configured with group_commit.enabled"
log "three-node data plane up; group commit on (window ${DELAY_MS} ms); the leader serves Kafka on 127.0.0.1:$KAFKA_PORT (pid $LEADER_PID)"

# One input per producer, its keys prefixed with the producer's number so each
# producer's records can be picked out of the one partition afterwards.
for p in $(seq 1 "$PRODUCERS"); do
  awk -v n="$PER_PRODUCER" -v p="$p" 'BEGIN {
    srand(25 * 100 + p)
    for (i = 1; i <= n; i++) printf "p%d-k%06d:v%06d-%08x\n", p, i, i, int(rand() * 4294967295)
  }' > "$WORKDIR/kafka-input-$p.txt"
done

kafka_watermark() {
  "$KCAT" -Q -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC:0:-1" 2>/dev/null | awk '{ print $NF }'
}

# Four independent processes, so four connections and four producer ids: the
# requests reach the broker concurrently, which is what a group is made of.
# The message timeout outlasts the kill and the restart, so none gives up, and
# `-E` keeps kcat running while every broker connection is down: by default it
# exits on that, and the stream would end at the kill instead of surviving it.
# A fatal client error still ends it, and the scenario fails on that.
PRODUCER_PIDS=()
for p in $(seq 1 "$PRODUCERS"); do
  awk -v every="$PACE_EVERY" -v pause="$PACE_SLEEP" \
    '{ print; fflush(); if (NR % every == 0) system("sleep " pause) }' "$WORKDIR/kafka-input-$p.txt" \
    | "$KCAT" -P -E -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC" -p 0 -K: \
      -X acks=all -X enable.idempotence=true \
      -X "request.timeout.ms=$REQUEST_TIMEOUT_MS" -X message.timeout.ms=240000 \
      > "$WORKDIR/logs/kcat-produce-$p.log" 2>&1 &
  PRODUCER_PIDS+=("$!")
  echo "$!" >> "$WORKDIR/pids"
done

deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
reached=0
while (( SECONDS < deadline )); do
  mark="$(kafka_watermark)"
  if [[ "$mark" =~ ^[0-9]+$ && "$mark" -ge "$ACK_FLOOR" ]]; then
    reached=1
    break
  fi
  for pid in "${PRODUCER_PIDS[@]}"; do
    kill -0 "$pid" 2>/dev/null || fail "a producer ended before the watermark reached $ACK_FLOOR"
  done
  (( deadline - SECONDS > 1 )) && sleep 0.1
done
[[ $reached -eq 1 ]] || fail "the producers made no progress to $ACK_FLOOR within ${PROGRESS_TIMEOUT_SECONDS}s"
for pid in "${PRODUCER_PIDS[@]}"; do
  kill -0 "$pid" 2>/dev/null \
    || fail "a producer finished before the kill could land inside its stream; slow it with CHAOS_GC_PACE_SLEEP"
done

# The groups were shared while the stream ran: more requests than groups.
PRE_REQUESTS="$(sample_metric "$(data_metrics_addr 0)" vtop_broker_group_commit_requests_total)" \
  || fail "vtop_broker_group_commit_requests_total is not exported by a leader with group commit on"
PRE_COMMITS="$(sample_metric "$(data_metrics_addr 0)" vtop_broker_group_commits_total)" \
  || fail "vtop_broker_group_commits_total is not exported by a leader with group commit on"
(( PRE_REQUESTS > PRE_COMMITS && PRE_COMMITS > 0 )) \
  || fail "four concurrent producers shared no barrier before the kill: $PRE_REQUESTS requests in $PRE_COMMITS groups (see $LEADER_LOG)"
log "before the kill: $PRE_REQUESTS produce requests shared $PRE_COMMITS barriers (watermark past $ACK_FLOOR, all four producers streaming)"

kill -9 "$LEADER_PID"
log "leader killed with SIGKILL mid-stream"
sleep 1

LEADER_PID="$(start_node "data-leader-gc-restarted" "data_node_ready" data --config "$CFG")"
await_ready "$(data_metrics_addr 0)" "data-leader-gc-restarted"
grep -q "group commit on for range" "$WORKDIR/logs/data-leader-gc-restarted.log" \
  || fail "the restarted leader did not say that group commit is on"
log "leader restarted from the same config (pid $LEADER_PID); the producers reconnect and retry"

for i in "${!PRODUCER_PIDS[@]}"; do
  p=$((i + 1))
  set +e
  wait "${PRODUCER_PIDS[$i]}"
  code=$?
  set -e
  [[ $code -eq 0 ]] \
    || fail "producer $p exited $code after the restart: $(tail -3 "$WORKDIR/logs/kcat-produce-$p.log")"
  ! grep -q 'Delivery failed' "$WORKDIR/logs/kcat-produce-$p.log" \
    || fail "producer $p had a delivery fail across the kill: $(grep -m 3 'Delivery failed' "$WORKDIR/logs/kcat-produce-$p.log")"
  ! grep -qi 'fatal' "$WORKDIR/logs/kcat-produce-$p.log" \
    || fail "producer $p hit a fatal error: $(grep -im 3 'fatal' "$WORKDIR/logs/kcat-produce-$p.log")"
done
log "all $PRODUCERS producers finished their $PER_PRODUCER records across the kill"

WATERMARK="$(kafka_watermark)"
[[ "$WATERMARK" -eq "$RECORDS" ]] \
  || fail "the watermark is $WATERMARK after the streams, not $RECORDS: a retry appended twice, or a record never landed"

OUTPUT="$WORKDIR/kafka-output.txt"
"$KCAT" -C -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC" -p 0 -o beginning -e -c "$RECORDS" \
  -K: -f '%k:%s\n' > "$OUTPUT" 2>> "$WORKDIR/logs/kcat-consume.log" \
  || fail "consume failed: $(tail -3 "$WORKDIR/logs/kcat-consume.log")"
GOT="$(wc -l < "$OUTPUT" | tr -d ' ')"
[[ "$GOT" -eq "$RECORDS" ]] || fail "consumed $GOT records, produced $RECORDS"
DUPLICATE_KEYS="$(cut -d: -f1 "$OUTPUT" | sort | uniq -d | wc -l | tr -d ' ')"
[[ "$DUPLICATE_KEYS" -eq 0 ]] \
  || fail "$DUPLICATE_KEYS key(s) read back twice: $(cut -d: -f1 "$OUTPUT" | sort | uniq -d | head -3 | tr '\n' ' ')"
for p in $(seq 1 "$PRODUCERS"); do
  grep "^p$p-" "$OUTPUT" > "$WORKDIR/kafka-output-$p.txt" || true
  cmp -s "$WORKDIR/kafka-input-$p.txt" "$WORKDIR/kafka-output-$p.txt" \
    || { diff "$WORKDIR/kafka-input-$p.txt" "$WORKDIR/kafka-output-$p.txt" | head -5 >&2
         fail "producer $p's records read back differ from the records it sent, in bytes or in order"; }
done
log "all $RECORDS records read back: each producer's byte-exact and in its own order, no key twice, none missing"

# The restarted leader batches too: its counters moved from zero.
POST_COMMITS="$(sample_metric "$(data_metrics_addr 0)" vtop_broker_group_commits_total)" \
  || fail "the restarted leader does not export vtop_broker_group_commits_total"
(( POST_COMMITS > 0 )) || fail "the restarted leader sealed no group while the producers finished"
log "after the restart the leader sealed $POST_COMMITS more group(s)"

# POLLED, not read once (review): a quorum acknowledgement needs only one of
# the two followers, so the other may still be catching up when the consumer
# finishes. Each must reach every acknowledged record within the deadline.
PROBE_CFG="$(emit_replica_probe_config)"
for n in 1 2; do
  deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
  while :; do
    LOCAL=""
    if STATUS="$("$VTOP_NODE" replica-status --client-config "$PROBE_CFG" \
      --addr "$(replica_addr "$n")" 2>/dev/null)"; then
      LOCAL="${STATUS#*local_committed_offset=}"
      LOCAL="${LOCAL%% *}"
      [[ "$LOCAL" =~ ^[0-9]+$ && "$LOCAL" -ge "$RECORDS" ]] && break
    fi
    [[ $SECONDS -lt $deadline ]] \
      || fail "follower $n committed offset ${LOCAL:-unread} < $RECORDS acknowledged after ${PROGRESS_TIMEOUT_SECONDS}s"
    sleep 0.2
  done
done
log "both followers committed every one of the $RECORDS records: each acknowledgement was a quorum's"

stop_node_gracefully leader "$LEADER_PID"
stop_node_now "$F1_PID"
stop_node_now "$F2_PID"
seal_and_verify_active leader "$WORKDIR/data-leader"
seal_and_verify_active follower-1 "$WORKDIR/data-follower-1"
log "PASS"
