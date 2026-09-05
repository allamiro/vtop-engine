#!/usr/bin/env bash
# 20 — a group's committed offset is a cursor on the metadata plane (#457,
# slice 2b): a stock group consumer (kcat -G, librdkafka) reads the first half
# of the records and leaves, committing; a second consumer under the same
# group name resumes exactly where the first stopped — nothing re-read,
# nothing skipped — and the operator reads the same position back from the
# plane as an UNPINNED cursor bound to the range's lineage, through the same
# linearizable read every admin read makes. The gateway remembered nothing
# itself: the position lived on the plane between the two consumers.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
export CHAOS_SEGMENT_FORMAT=v2
require_binaries

KCAT="$(command -v kcat || command -v kafkacat || true)"
[[ -n "$KCAT" ]] || fail "scenario 20 needs kcat, librdkafka's client: brew install kcat / apt-get install kafkacat"

init_workdir

# kafka-3 of the lib's port families, as scenario 19: the two never run at once.
KAFKA_PORT="$(kafka_port 3)"
RECORDS="${CHAOS_KAFKA_RECORDS:-5000}"
require_integer_in_range CHAOS_KAFKA_RECORDS "$RECORDS" 2 10000000
RECORDS=$((10#$RECORDS))
HALF=$((RECORDS / 2))
GROUP="chaos-20"
[[ -n "${TOPIC:-}" ]] || fail "lib.sh did not set TOPIC"

# The metadata plane: three nodes, the topic, the data nodes registered.
M1=$(start_meta_node 1 2 3)
M2=$(start_meta_node 2 1 3)
M3=$(start_meta_node 3 1 2)
log "meta nodes up: $M1 $M2 $M3"
meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
log "meta leader elected: node $LEADER_ID"
meta_admin "$LEADER_ID" create-topic \
  --name "$TOPIC" --topic-uuid "$TOPIC_UUID" --root-range-uuid "$RANGE_ID" > /dev/null \
  || fail "could not create the topic in metadata"
for node in "$LEADER_UUID" "$FOLLOWER1_UUID" "$FOLLOWER2_UUID"; do
  meta_admin "$LEADER_ID" register-node \
    --node-uuid "$node" --addr "$REGISTER_HOST_PREFIX.1:$REGISTER_PORT" > /dev/null \
    || fail "could not register data node $node"
done

# The data plane: two followers at the first grant's epoch, and a leader that
# holds the range under a lease AND serves Kafka — the lease is what gives the
# gateway a metadata plane to commit cursors to.
EXPECTED_FIRST_EPOCH=1
F1_PID=$(start_follower 1 "" "$EXPECTED_FIRST_EPOCH")
F2_PID=$(start_follower 2 "" "$EXPECTED_FIRST_EPOCH")
CFG="$WORKDIR/data-leader-kafka.yaml"
{
  cat "$(emit_leader_config_with_lease "$LEADER_ID" lease)"
  echo "kafka: { listen: \"127.0.0.1:$KAFKA_PORT\" }"
} | install_config "$CFG"
LEADER_PID="$(start_node "data-leader-kafka" "data_node_ready" data --config "$CFG")"
await_ready "$(data_metrics_addr 0)" "data-leader-kafka"
EPOCH="$(await_lease_holder "$LEADER_ID" "$LEADER_UUID")"
[[ "$EPOCH" == "$EXPECTED_FIRST_EPOCH" ]] \
  || fail "first grant minted epoch $EPOCH, not $EXPECTED_FIRST_EPOCH"
grep -q "kafka=127.0.0.1:$KAFKA_PORT" "$WORKDIR/logs/data-leader-kafka.log" \
  || fail "the leader's ready line does not name the kafka listener (see $WORKDIR/logs/data-leader-kafka.log)"
log "leader holds the range at epoch $EPOCH and serves Kafka on 127.0.0.1:$KAFKA_PORT (pid $LEADER_PID)"

INPUT="$WORKDIR/kafka-input.txt"
awk -v n="$RECORDS" 'BEGIN {
  srand(20)
  for (i = 1; i <= n; i++) printf "k%06d:v%06d-%08x\n", i, i, int(rand() * 4294967295)
}' > "$INPUT"
KCAT_DEBUG=()
[[ -n "${CHAOS_KCAT_DEBUG:-}" ]] && KCAT_DEBUG=(-d "$CHAOS_KCAT_DEBUG")
kafka_watermark() {
  "$KCAT" -Q -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC:0:-1" 2>/dev/null | awk '{ print $NF }'
}
# A lease-held range's log begins with the promotion marker (#240) — a record
# the broker filters from consumer output but which holds an offset — so the
# first record a consumer sees is not offset 0. Every position below is
# measured from where the log stands before the first produce: what the plane
# stores is the NEXT OFFSET TO CONSUME, marker included, as Kafka's own
# committed offsets count every record of the log.
BASE="$(kafka_watermark)"
require_integer_in_range "watermark before producing" "$BASE" 0 1000
BASE=$((10#$BASE))
log "the log stands at $BASE before producing (the promotion marker the broker filters from consumer output)"
"$KCAT" -P -b "127.0.0.1:$KAFKA_PORT" -t "$TOPIC" -p 0 -K: -X acks=all \
  ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} < "$INPUT" > "$WORKDIR/logs/kcat-produce.log" 2>&1 \
  || fail "produce failed: $(tail -3 "$WORKDIR/logs/kcat-produce.log")"
WATERMARK="$(kafka_watermark)"
[[ "$WATERMARK" -eq $((BASE + RECORDS)) ]] || fail "the watermark is $WATERMARK after producing, not $((BASE + RECORDS))"
log "$RECORDS records produced; the watermark is $WATERMARK"

# consume <count> <output> <log>: one group consumer under $GROUP, auto-commit
# on and a commit on close (librdkafka's default with auto-commit), exits
# after <count> records.
consume() {
  local count="$1" output="$2" clog="$3" rc
  set +e
  timeout "$((10#$PROGRESS_TIMEOUT_SECONDS * 2))" "$KCAT" -G "$GROUP" -b "127.0.0.1:$KAFKA_PORT" \
    -X auto.offset.reset=earliest -X enable.auto.commit=true -X auto.commit.interval.ms=500 \
    -X session.timeout.ms=10000 \
    -c "$count" -K: -f '%k:%s\n' ${KCAT_DEBUG[@]+"${KCAT_DEBUG[@]}"} "$TOPIC" > "$output" 2> "$clog"
  rc=$?
  set -e
  [[ $rc -eq 0 ]] || fail "the group consumer exited $rc: $(tail -5 "$clog")"
  grep -q "assigned: $TOPIC \[0\]" "$clog" \
    || fail "the consumer's rebalance did not assign it the partition: $(grep -m 3 -i 'rebalanc\|assign\|error' "$clog" | tr '\n' ' ')"
  local errors
  errors="$(grep -i 'error\|fail' "$clog" || true)"
  [[ -z "$errors" ]] || fail "the consumer logged an error: $(printf '%s' "$errors" | head -3 | tr '\n' ' ')"
}

# cursor_offset: the group's cursor on the plane, by the group's Kafka name
# under the cluster id — as the gateway derives it — through the admin read.
cursor_field() {
  local out="$WORKDIR/group-cursor.json"
  # Bounded and retried (review): a linearizable read can be refused while the
  # metadata plane is between leaders, and one refusal must not end a scenario
  # that is otherwise making progress.
  meta_admin_read "$out" "$LEADER_ID" group-cursor --group-name "$GROUP" \
    --cluster-id "$CLUSTER_ID" --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" \
    || fail "could not read the group cursor: $(tail -2 "$out.stderr" 2>/dev/null | tr '\n' ' ')"
  python3 -c "
import json
d = json.load(open('$out'))
print($1)
"
}

# Before anything is committed: the group is unknown to the plane.
FOUND="$(cursor_field 'd["group_found"]')"
[[ "$FOUND" == "False" ]] || fail "the plane knows group $GROUP before any consumer committed: $FOUND"

# First half: read HALF records and leave, committing.
OUT1="$WORKDIR/kafka-output-1.txt"
consume "$HALF" "$OUT1" "$WORKDIR/logs/kcat-consume-1.log"
GOT1="$(wc -l < "$OUT1" | tr -d ' ')"
[[ "$GOT1" -eq "$HALF" ]] || fail "the first consumer read $GOT1 records, not $HALF"
head -n "$HALF" "$INPUT" | cmp -s - "$OUT1" \
  || fail "the first consumer's bytes differ from the first $HALF records produced"
log "the first consumer read records 1..$HALF byte-exact and left"

# The plane holds the position — the NEXT offset to consume — as an unpinned
# cursor in the range's lineage.
OFFSET="$(cursor_field 'd["cursor"]["record_offset"]')"
PINNED="$(cursor_field 'd["cursor"]["pinned"]')"
[[ "$OFFSET" -eq $((BASE + HALF)) ]] || fail "the cursor on the plane is at $OFFSET, not $((BASE + HALF))"
[[ "$PINNED" == "False" ]] || fail "the cursor is pinned to a segment; a head cursor is unpinned"
# Bound to the identity, not just to the number (review): the topic epoch the
# range is served at, and the range's lineage generation — zero on a range
# nothing has split or merged.
EPOCH_BOUND="$(cursor_field 'd["cursor"]["topic_epoch"]')"
LINEAGE="$(cursor_field 'd["cursor"]["range_generation"]')"
[[ "$EPOCH_BOUND" -eq 1 ]] || fail "the cursor is bound to topic epoch $EPOCH_BOUND, not the served 1"
[[ "$LINEAGE" -eq 0 ]] || fail "the cursor is bound to lineage generation $LINEAGE, not 0"
log "the plane's cursor for $GROUP is $OFFSET, unpinned: exactly where the first consumer stopped (the next offset to consume)"

# Second half: a new consumer under the same group resumes at the cursor.
OUT2="$WORKDIR/kafka-output-2.txt"
consume "$((RECORDS - HALF))" "$OUT2" "$WORKDIR/logs/kcat-consume-2.log"
GOT2="$(wc -l < "$OUT2" | tr -d ' ')"
[[ "$GOT2" -eq "$((RECORDS - HALF))" ]] || fail "the second consumer read $GOT2 records, not $((RECORDS - HALF))"
tail -n "+$((HALF + 1))" "$INPUT" | cmp -s - "$OUT2" \
  || { diff <(tail -n "+$((HALF + 1))" "$INPUT") "$OUT2" | head -5 >&2; fail "the second consumer did not resume exactly at record $((HALF + 1))"; }
cat "$OUT1" "$OUT2" | cmp -s "$INPUT" - \
  || fail "the two consumers together did not read exactly what was produced"
OFFSET2="$(cursor_field 'd["cursor"]["record_offset"]')"
[[ "$OFFSET2" -eq $((BASE + RECORDS)) ]] || fail "the cursor on the plane is at $OFFSET2 after the second consumer, not $((BASE + RECORDS))"
log "the second consumer resumed at record $((HALF + 1)) and read to $RECORDS; the plane's cursor is $OFFSET2"

# The gateway kept nothing it would forget: no commit was refused by name.
REFUSED="$(grep -c 'OffsetCommit refused' "$WORKDIR/logs/data-leader-kafka.log" || true)"
[[ "$REFUSED" -eq 0 ]] || fail "the leader's log names $REFUSED refused OffsetCommit(s); with a metadata plane none is refused by name"

stop_node_gracefully leader "$LEADER_PID"
stop_node_now "$F1_PID"
stop_node_now "$F2_PID"
seal_and_verify_active leader "$WORKDIR/data-leader"
log "PASS"
