#!/usr/bin/env bash
# 33 — a stock Kafka consumer group follows a failover (#751).
#
# Scenarios 27, 28, 30 and 31 follow a failover with a producer. A consumer
# group follows it by a longer path: the range's leader is also the group's
# coordinator (FindCoordinator names the lease holder), and group membership
# is the coordinator's memory, which is NOT carried across a failover (a
# stated limit: docs/deploy/cluster-plane.md). What survives is the committed
# offset, which lives on the metadata plane. So after the kill the consumer
# must find the new coordinator through FindCoordinator, be refused under its
# old generation (UNKNOWN_MEMBER_ID), rejoin with JoinGroup and SyncGroup, read
# its committed offset back with OffsetFetch from the new holder, and fetch on.
# The records it had consumed but not yet committed are delivered again:
# at-least-once, which is what auto-commit promises.
#
# System model: crash-recovery processes, a partially synchronous network,
# honest nodes. Per client, on a fresh cluster (three candidates serving
# Kafka, CHAOS_KAFKA_SHAPE as 27):
#   1. ONE group consumer is started, bootstrapped from all three, with
#      auto-commit on: librdkafka as `kcat -G`, at librdkafka's own 5 s
#      interval, and the JVM client as `kafka-console-consumer --group`, every
#      CHAOS_KAFKA_COMMIT_INTERVAL_MS (500), when CHAOS_KAFKA_HOME or
#      CHAOS_KAFKA_IMAGE names a distribution (otherwise that client is
#      skipped and the log says so). kcat cannot set librdkafka's interval:
#      its -X tries the topic configuration first, and the high-level
#      consumer's `auto.commit.interval.ms` and `enable.auto.commit` share
#      their names with legacy topic properties, which is where they land;
#   2. one idempotent librdkafka producer streams paced records;
#   3. after the consumer has delivered CHAOS_KAFKA_KILL_AFTER_CONSUMED records
#      the group's committed offset is read from the metadata plane, and the
#      range's leader (and with it the group's coordinator) is SIGKILLed.
#      Nothing else is touched: the consumer is not restarted or
#      reconfigured.
# What it asserts:
#   liveness the SAME consumer process delivers every produced record, and
#            the time from the kill to the end of the longest silence in its
#            deliveries (the rejoin and resume it lived through) is printed
#            and bounded by CHAOS_KAFKA_FAILOVER_LIMIT_SECONDS;
#   safety   every produced record is delivered at least once, with the value
#            it was produced with, at one offset, first delivered in produced
#            order; every rewind (a delivery at or below an offset already
#            delivered: the resume from a committed offset) goes back no
#            further than the offset committed BEFORE the kill, so what is
#            delivered again is records after the last commit and nothing
#            else; nothing was reset by OFFSET_OUT_OF_RANGE;
#   commits  the committed offset kept up before the kill: it covers every
#            record delivered a commit interval and 2 s before the kill. The
#            redeliveries are counted, not bounded by a number: librdkafka
#            keeps fetching through the failover while its commits wait for a
#            coordinator, so all it delivers until it rejoins comes again
#            (measured first: 1,720 records of a 15 s lease), and what bounds
#            them is the offset committed, asserted above;
#   end      the group's committed offset on the plane reaches the end of the
#            partition (the Kafka high watermark), and a fresh member of the
#            same group reads nothing.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

# A v2 range, as 27.
export CHAOS_SEGMENT_FORMAT=v2

require_binaries
KCAT="$(command -v kcat || command -v kafkacat || true)"
[[ -n "$KCAT" ]] || fail "scenario 33 needs kcat, librdkafka's client: brew install kcat / apt-get install kafkacat"
transport_plaintext && fail "scenario 33 needs TLS: candidates cannot run under CHAOS_TRANSPORT=plaintext"
init_workdir
BASE_WORKDIR="$WORKDIR"
BASE_SCENARIO="$SCENARIO"

RECORDS="${CHAOS_KAFKA_RECORDS:-3000}"
KILL_AFTER_CONSUMED="${CHAOS_KAFKA_KILL_AFTER_CONSUMED:-800}"
COMMIT_INTERVAL_MS="${CHAOS_KAFKA_COMMIT_INTERVAL_MS:-500}"
PACE_EVERY="${CHAOS_KAFKA_PACE_EVERY:-10}"
PACE_SLEEP="${CHAOS_KAFKA_PACE_SLEEP:-0.1}"
FAILOVER_LIMIT_SECONDS="${CHAOS_KAFKA_FAILOVER_LIMIT_SECONDS:-60}"
CLIENTS="${CHAOS_KAFKA_GROUP_CLIENTS:-librdkafka jvm}"
require_integer_in_range CHAOS_KAFKA_RECORDS "$RECORDS" 1000 10000000
RECORDS=$((10#$RECORDS))
require_integer_in_range CHAOS_KAFKA_KILL_AFTER_CONSUMED "$KILL_AFTER_CONSUMED" 1 "$((RECORDS / 2))"
KILL_AFTER_CONSUMED=$((10#$KILL_AFTER_CONSUMED))
require_integer_in_range CHAOS_KAFKA_COMMIT_INTERVAL_MS "$COMMIT_INTERVAL_MS" 100 60000
COMMIT_INTERVAL_MS=$((10#$COMMIT_INTERVAL_MS))
require_integer_in_range CHAOS_KAFKA_PACE_EVERY "$PACE_EVERY" 1 "$RECORDS"
require_integer_in_range CHAOS_KAFKA_FAILOVER_LIMIT_SECONDS "$FAILOVER_LIMIT_SECONDS" 1 3600
FAILOVER_LIMIT_SECONDS=$((10#$FAILOVER_LIMIT_SECONDS))
[[ "$PACE_SLEEP" =~ ^[0-9]+([.][0-9]+)?$ ]] \
  || fail "CHAOS_KAFKA_PACE_SLEEP must be a non-negative decimal, got '$PACE_SLEEP'"
# librdkafka's default auto.commit.interval.ms, which kcat cannot change (see
# the header).
LIBRDKAFKA_COMMIT_INTERVAL_MS=5000
for client in $CLIENTS; do
  case "$client" in
    librdkafka | jvm) ;;
    *) fail "CHAOS_KAFKA_GROUP_CLIENTS lists '$client': the clients are librdkafka and jvm" ;;
  esac
done
LAST_KEY="$(printf 'k%06d' "$RECORDS")"

# run_client <client> — one failover under one consumer, on a fresh cluster in
# its own directory.
run_client() {
  local client="$1"
  WORKDIR="$BASE_WORKDIR/$client"
  mkdir -p "$WORKDIR/logs"
  : > "$WORKDIR/pids"
  SCENARIO="$BASE_SCENARIO/$client"
  log "--- consumer: $client ---"

  bring_up_kafka_candidates
  local group="chaos-33-$client"

  local input="$WORKDIR/kafka-input.txt"
  awk -v n="$RECORDS" 'BEGIN {
    srand(33)
    for (i = 1; i <= n; i++) printf "k%06d:v%06d-%08x\n", i, i, int(rand() * 4294967295)
  }' > "$input"

  # --- the consumer: every delivery stamped as it is printed ------------------
  local consumed="$WORKDIR/consumed.txt" clog="$WORKDIR/logs/consumer.log" consumer
  : > "$consumed"
  : > "$clog"
  # The consumer's group debug is on (librdkafka's `cgrp` facility, the
  # joins, the commits and their answers), so a commit course that goes
  # wrong says why in its own log; CHAOS_KCAT_DEBUG replaces the list.
  local kcat_debug=(-d "${CHAOS_KCAT_DEBUG:-cgrp}")
  local interval_ms="$COMMIT_INTERVAL_MS"
  if [[ "$client" == "librdkafka" ]]; then
    # Stock settings apart from where a group with nothing committed starts
    # (a topic property, which is where kcat puts it): auto-commit is on by
    # default, every 5 s. -u: unbuffered, so a delivery is stamped when it is
    # printed.
    interval_ms="$LIBRDKAFKA_COMMIT_INTERVAL_MS"
    "$KCAT" -G "$group" -b "$KAFKA_BOOTSTRAP" -u \
      -X auto.offset.reset=earliest \
      -f '%o:%k:%s\n' ${kcat_debug[@]+"${kcat_debug[@]}"} "$TOPIC" \
      > >(stamp_stdin_ms > "$consumed") 2> >(stamp_stdin_ms > "$clog") &
    consumer=$!
  else
    local props="$WORKDIR/consumer.properties" consumer_help formatter_option=--property config_option=--consumer.config
    {
      echo "auto.offset.reset=earliest"
      echo "enable.auto.commit=true"
      echo "auto.commit.interval.ms=$COMMIT_INTERVAL_MS"
    } > "$props"
    # Newer distributions rename two options and print a notice for the old
    # names (a herestring, not a pipe, as 28).
    consumer_help="$("$KAFKA_HOME/bin/kafka-console-consumer.sh" --help 2>&1 || true)"
    [[ "$consumer_help" == *--formatter-property* ]] && formatter_option=--formatter-property
    [[ "$consumer_help" == *--command-config* ]] && config_option=--command-config
    "$KAFKA_HOME/bin/kafka-console-consumer.sh" \
      --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$TOPIC" --group "$group" \
      "$config_option" "$props" \
      "$formatter_option" print.offset=true "$formatter_option" print.key=true \
      "$formatter_option" print.value=true \
      > >(stamp_stdin_ms > "$consumed") 2> >(stamp_stdin_ms > "$clog") &
    consumer=$!
  fi
  echo "$consumer" >> "$WORKDIR/pids"
  log "one $client group consumer (pid $consumer) in group $group, auto-commit every ${interval_ms} ms, bootstrapped from $KAFKA_BOOTSTRAP"
  delivered_count() { grep -c . "$consumed" || true; }
  consumer_alive() {
    kill -0 "$consumer" 2>/dev/null \
      || fail "the $client consumer (pid $consumer) exited during the run: $(tail -3 "$clog")"
  }

  # --- the producer, as 27 -----------------------------------------------------
  local acks="$WORKDIR/logs/kcat-produce.log" producer
  : > "$acks"
  "$KCAT" -P -b "$KAFKA_BOOTSTRAP" -t "$TOPIC" -p 0 -K: -v -v \
    -X acks=all -X enable.idempotence=true \
    < <(awk -v every="$PACE_EVERY" -v pause="$PACE_SLEEP" \
      '{ print; fflush(); if (NR % every == 0) system("sleep " pause) }' "$input") \
    2> >(stamp_stdin_ms > "$acks") &
  producer=$!
  echo "$producer" >> "$WORKDIR/pids"
  acked_count() { grep -c 'Message delivered' "$acks" || true; }

  # Where the group's committed offset stands each second, beside how many
  # records the consumer has delivered: the gap between the two is what a
  # failover delivers again, so its course is reported, not only its end.
  local samples="$WORKDIR/cursor-samples.txt" sampler
  : > "$samples"
  (
    while :; do
      printf '%s %s %s\n' "$(wall_ms)" \
        "$(group_cursor_offset "$group" "$META_LEADER_ID" || echo '?')" \
        "$(grep -c . "$consumed" || true)" >> "$samples"
      sleep 1
    done
  ) &
  sampler=$!
  echo "$sampler" >> "$WORKDIR/pids"

  local deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS + 30))
  until [[ "$(delivered_count)" -ge "$KILL_AFTER_CONSUMED" ]]; do
    consumer_alive
    [[ $SECONDS -lt $deadline ]] \
      || fail "the consumer delivered only $(delivered_count) records in time ($(acked_count) produced): $(tail -3 "$clog")"
    sleep 0.1
  done

  # --- the committed offset before the kill, then the kill --------------------
  local committed_before=""
  deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
  until [[ -n "$committed_before" && "$committed_before" -ge 0 ]]; do
    committed_before="$(group_cursor_offset "$group" "$META_LEADER_ID")" || committed_before=""
    [[ $SECONDS -lt $deadline ]] \
      || fail "the metadata plane holds no committed offset for group $group after $(delivered_count) deliveries (last read: ${committed_before:-none})"
    [[ -n "$committed_before" && "$committed_before" -ge 0 ]] || sleep 0.2
  done
  kill9_pid "$sampler"
  wait "$sampler" 2>/dev/null || true
  log "the committed offset each second before the kill (wall ms, committed, delivered): $(tr '\n' ';' < "$samples")"
  local kill_ms delivered_at_kill
  kill_ms="$(wall_ms)"
  kill9_pid "${CAND_PIDS[$WINNER]}"
  delivered_at_kill="$(delivered_count)"
  log "candidate $WINNER (the leader and the group's coordinator) SIGKILLed with $delivered_at_kill records delivered and offset $committed_before committed"

  local new_holder new_epoch new_leader
  read -r new_holder new_epoch <<<"$(await_lease_holder_changed "$(lease_readers_without "$WINNER")" "$HOLDER" \
    $((LEASE_DURATION_MS / 1000 + ELECTION_TIMEOUT_SECONDS)))"
  new_leader="$(candidate_by_uuid "$new_holder")"
  [[ "$new_epoch" -gt "$EPOCH" ]] || fail "a new hold must mint a newer epoch: old $EPOCH, new $new_epoch"
  await_log_line "$(candidate_log "$new_leader")" \
    "kafka gateway for range .* is now serving at epoch $new_epoch" \
    "the promoted candidate's gateway must say it is serving, at the new epoch"
  log "candidate $new_leader took the range (and the group) at epoch $new_epoch"

  # --- the producer finishes ----------------------------------------------------
  local finish_deadline=$((SECONDS + FAILOVER_LIMIT_SECONDS + RECORDS * 3 / 100 + PROGRESS_TIMEOUT_SECONDS))
  while kill -0 "$producer" 2>/dev/null; do
    [[ $SECONDS -lt $finish_deadline ]] \
      || fail "the producer had not finished, with $(acked_count) of $RECORDS acknowledged: $(tail -3 "$acks")"
    sleep 0.2
  done
  local producer_exit=0
  wait "$producer" || producer_exit=$?
  for _ in $(seq 1 50); do
    [[ "$(acked_count)" -ge "$RECORDS" ]] && break
    sleep 0.1
  done
  [[ $producer_exit -eq 0 && "$(acked_count)" -eq "$RECORDS" ]] \
    || fail "the producer exited $producer_exit with $(acked_count) of $RECORDS acknowledged: $(tail -3 "$acks")"
  local watermark
  watermark="$("$KCAT" -Q -b "$KAFKA_BOOTSTRAP" -t "$TOPIC:0:-1" 2>/dev/null | awk '{ print $NF }')"
  require_integer_in_range "the high watermark" "$watermark" "$RECORDS" $((RECORDS + new_epoch))

  # --- the same consumer delivers the last record ------------------------------
  deadline=$((SECONDS + FAILOVER_LIMIT_SECONDS + PROGRESS_TIMEOUT_SECONDS))
  # kcat separates with a colon, the JVM formatter with a tab.
  local sep=$'[:\t]'
  until grep -qE "${sep}${LAST_KEY}${sep}" "$consumed"; do
    consumer_alive
    [[ $SECONDS -lt $deadline ]] \
      || fail "the consumer had not delivered $LAST_KEY, with $(delivered_count) deliveries: $(tail -3 "$clog")"
    sleep 0.2
  done
  consumer_alive
  log "the same $client consumer delivered the last record, $(delivered_count) deliveries in all"

  # --- its committed offset reaches the end of the partition -------------------
  local committed_end="" readers
  readers="$(lease_readers_without "$WINNER")"
  deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
  until [[ "$committed_end" == "$watermark" ]]; do
    consumer_alive
    committed_end="$(group_cursor_offset "$group" "$readers")" || committed_end=""
    [[ $SECONDS -lt $deadline ]] \
      || fail "group $group's committed offset is ${committed_end:-unreadable}, not the end of the partition ($watermark), ${PROGRESS_TIMEOUT_SECONDS}s after its last delivery"
    [[ "$committed_end" == "$watermark" ]] || sleep 0.2
  done
  log "group $group's committed offset is $committed_end, the end of the partition"
  # Stopped the way an operator stops it: SIGTERM, so it leaves the group
  # (a member that does not leave holds the next one's join until its
  # session lapses). Bounded: a consumer that ignores it is killed, and the
  # exit status is reported, not judged.
  stop_pid "$consumer"
  local stop_deadline=$((SECONDS + STOP_TIMEOUT_SECONDS * 3)) consumer_exit=0
  while kill -0 "$consumer" 2>/dev/null && [[ $SECONDS -lt $stop_deadline ]]; do
    sleep 0.2
  done
  kill -0 "$consumer" 2>/dev/null && kill9_pid "$consumer"
  wait "$consumer" 2>/dev/null || consumer_exit=$?
  sleep 0.5
  # What the consumer and the gateways said about commits and the group, for
  # the reader of a failure below (and of a pass).
  log "the consumer's group lines: $(grep -iE 'rebalanc|assign|revok|error|fail' "$clog" | grep -viE 'commit' \
    | head -12 | cut -c1-240 | tr '\n' '|')"
  log "the consumer's commit lines ($(grep -ciE 'commit' "$clog" || true) in all): $(grep -iE 'commit' "$clog" \
    | head -30 | cut -c1-300 | tr '\n' '|')"
  local n
  for n in 1 2 3; do
    log "candidate $n's group and commit lines: $(grep -E 'kafka offsets|kafka (OffsetCommit|JoinGroup|SyncGroup|Heartbeat)|NOT_COORDINATOR|coordinator' \
      "$(candidate_log "$n")" | head -8 | cut -c1-300 | tr '\n' '|')"
  done

  # --- what it delivered ---------------------------------------------------------
  local verdict
  verdict="$(python3 -c '
import sys
consumed, expected_path, committed_before = sys.argv[1], sys.argv[2], int(sys.argv[3])
kill_ms, interval_ms = int(sys.argv[4]), int(sys.argv[5])
expected = []
for line in open(expected_path):
    key, _, value = line.rstrip("\n").partition(":")
    expected.append((key, value))
values = dict(expected)
deliveries = []
for line in open(consumed):
    stamp, _, rest = line.rstrip("\n").partition(" ")
    if not stamp.isdigit() or not rest:
        continue
    if rest.startswith("Offset:"):
        head, key, value = rest.split("\t", 2)
        offset = int(head[len("Offset:"):])
    else:
        offset, key, value = rest.split(":", 2)
        offset = int(offset)
    deliveries.append((int(stamp), offset, key, value))
offset_of, first_order, rewinds, previous = {}, [], [], -1
for _, offset, key, value in deliveries:
    if key not in values:
        sys.exit("delivered a record nobody produced: %s at offset %d" % (key, offset))
    if value != values[key]:
        sys.exit("%s delivered with value %r, produced with %r" % (key, value, values[key]))
    if offset_of.setdefault(key, offset) != offset:
        sys.exit("%s delivered at offset %d and at offset %d" % (key, offset_of[key], offset))
    if offset <= previous:
        rewinds.append((previous, offset))
    if key not in first_order:
        first_order.append(key)
    previous = offset
missing = [key for key, _ in expected if key not in offset_of]
if missing:
    sys.exit("%d produced records never delivered, first %s" % (len(missing), missing[0]))
if first_order != [key for key, _ in expected]:
    sys.exit("first deliveries are not in produced order")
for before, to in rewinds:
    if to < committed_before:
        sys.exit("the consumer went back from offset %d to %d, below offset %d committed before the kill: a commit was lost or an offset reset" % (before, to, committed_before))
# The commits kept up: everything delivered a commit interval and 2 s before
# the kill was committed by then.
due = [offset for stamp, offset, _, _ in deliveries if stamp <= kill_ms - interval_ms - 2000]
if due and committed_before < max(due) + 1:
    sys.exit("offset %d was committed at the kill, behind offset %d delivered %d ms before it: the commits did not keep up with a %d ms interval" % (committed_before, max(due), interval_ms + 2000, interval_ms))
redelivered = len(deliveries) - len(offset_of)
print(redelivered, len(rewinds), min((to for _, to in rewinds), default=-1))
' "$consumed" "$input" "$committed_before" "$kill_ms" "$interval_ms")" || fail "the $client consumer's deliveries are wrong: $verdict (see $consumed)"
  local redelivered rewind_count rewound_to
  read -r redelivered rewind_count rewound_to <<<"$verdict"
  log "every record delivered at least once, in order: $redelivered redelivered over $rewind_count rewind(s), the lowest to offset $rewound_to (committed before the kill: $committed_before)"
  # Debug lines (librdkafka's level 7) narrate; a reset is reported at a
  # warning or an error level.
  local resets
  resets="$(awk '!/[ |]%7[|]/ && tolower($0) ~ /out of range|offset reset|offset_out_of_range/' "$clog")"
  [[ -z "$resets" ]] \
    || fail "the consumer reset its offset: $(head -2 <<<"$resets" | tr '\n' ' ')"

  # --- the times ----------------------------------------------------------------
  local first resume silence assigned="n/a"
  read -r first resume silence < <(ack_outage_report "$consumed" "$kill_ms" "")
  [[ "$resume" -le $((FAILOVER_LIMIT_SECONDS * 1000)) ]] \
    || fail "the consumer delivered nothing new for ${resume} ms after the kill, over the limit of ${FAILOVER_LIMIT_SECONDS} s"
  if [[ "$client" == "librdkafka" ]]; then
    # kcat says when its group is assigned the partition; the first such line
    # after the kill is the rejoin.
    assigned="$(python3 -c '
import sys
kill = int(sys.argv[2])
for line in open(sys.argv[1]):
    stamp, _, rest = line.partition(" ")
    if stamp.isdigit() and int(stamp) > kill and "assigned:" in rest:
        print(int(stamp) - kill)
        break
else:
    print("none")
' "$clog" "$kill_ms")"
    [[ "$assigned" != "none" ]] \
      || fail "kcat never reported the partition assigned again after the kill: $(grep -iE 'rebalanc|assign|revok' "$clog" | tail -3 | tr '\n' ' ')"
  fi
  log "kill to the consumer's first delivery after it: ${first} ms; to the end of the silence it saw: ${resume} ms (longest silence ${silence} ms); to the partition assigned again: ${assigned} ms"
  printf 'KAFKA_GROUP_FAILOVER client=%s %s kill_to_first_delivery_ms=%s kill_to_resume_ms=%s longest_silence_ms=%s kill_to_assigned_ms=%s redelivered=%s rewinds=%s committed_before_kill=%s commit_interval_ms=%s consumer_exit=%s records=%s\n' \
    "$client" "$(kafka_failover_conditions "$WINNER")" "$first" "$resume" "$silence" "$assigned" \
    "$redelivered" "$rewind_count" "$committed_before" "$interval_ms" "$consumer_exit" "$RECORDS" \
    | tee -a "$BASE_WORKDIR/group-failover-results.txt"

  # --- a fresh member of the same group reads nothing ----------------------------
  local fresh="$WORKDIR/fresh-member.txt" fresh_exit=0
  timeout "$((PROGRESS_TIMEOUT_SECONDS * 2))" "$KCAT" -G "$group" -b "$KAFKA_BOOTSTRAP" \
    -X auto.offset.reset=earliest -e -f '%o:%k:%s\n' "$TOPIC" \
    > "$fresh" 2> "$WORKDIR/logs/fresh-member.log" || fresh_exit=$?
  [[ $fresh_exit -eq 0 ]] \
    || fail "a fresh member of group $group exited $fresh_exit: $(tail -3 "$WORKDIR/logs/fresh-member.log")"
  [[ ! -s "$fresh" ]] \
    || fail "a fresh member of group $group read $(grep -c . "$fresh") record(s) past the committed end: $(head -2 "$fresh" | tr '\n' ' ')"
  log "a fresh member of group $group starts at the committed end and reads nothing"

  local pid
  for n in 1 2 3; do
    kill9_pid "${CAND_PIDS[$n]}"
  done
  while read -r pid; do
    kill -9 "$pid" 2>/dev/null || true
  done < "$WORKDIR/pids"
  wait 2>/dev/null || true
}

: > "$BASE_WORKDIR/group-failover-results.txt"
FAILED_CLIENTS=()
for client in $CLIENTS; do
  if [[ "$client" == "jvm" ]]; then
    if [[ -z "${CHAOS_KAFKA_HOME:-}" && -z "${CHAOS_KAFKA_IMAGE:-}" ]]; then
      log "the JVM consumer is not run: set CHAOS_KAFKA_HOME or CHAOS_KAFKA_IMAGE for it"
      continue
    fi
    require_command java "Install a JVM, 17 or newer: the Kafka distribution's client tools run on it."
    kafka_distribution "$BASE_WORKDIR" kafka-console-consumer
  fi
  # Each client in a subshell of its own, so one that fails does not keep
  # the other from being run and reported: two clients' evidence from one
  # run. errexit is set again inside, since a subshell tested by `if` or
  # `||` would run with it off. The EXIT trap is not inherited, so what a
  # failed client left running is stopped here.
  set +e
  ( set -e; run_client "$client" )
  code=$?
  set -e
  if [[ $code -ne 0 ]]; then
    FAILED_CLIENTS+=("$client")
    while read -r pid; do
      kill -9 "$pid" 2>/dev/null || true
    done < "$BASE_WORKDIR/$client/pids"
    wait 2>/dev/null || true
  fi
done
WORKDIR="$BASE_WORKDIR"
SCENARIO="$BASE_SCENARIO"
log "results:"
sed 's/^/  /' "$BASE_WORKDIR/group-failover-results.txt"
[[ ${#FAILED_CLIENTS[@]} -eq 0 ]] \
  || fail "the consumer group did not follow the failover under: ${FAILED_CLIENTS[*]} (each client's reason is above)"
log "PASS"
