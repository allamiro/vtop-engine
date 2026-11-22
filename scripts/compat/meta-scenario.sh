#!/usr/bin/env bash
# Write a metadata node's directory with a given vtop-node and vtopctl (#743):
# one voter on plaintext loopback, a history that leaves every file kind the
# metadata tier persists, then SIGKILL, which is what a crashed voter leaves.
#
# The history, through the admin endpoint:
#   1. `meta init --members 1` (a membership entry and the first leader);
#   2. three nodes registered and one topic created (log entries of two kinds);
#   3. with LEVEL set, `meta enable-features --level LEVEL` (the cluster
#      configuration record, which a voter that predates the level refuses);
#   4. ENTRIES re-registrations of the first node, each raising its generation
#      by one. The Raft engine takes a snapshot every 5000 log entries and
#      purges behind it, so 5100 entries (the default) leave a snapshot file
#      next to the log, which the other files alone would not.
#
# What is left in <workdir>/data:
#   log-*.vmlog                      the log chunks (VTOPMLG1 chunk, VTOPMLE1 entries)
#   snapshot-*.vmsnap                the state snapshot (VTOPMSN1)
#   meta.hardstate, meta.applied, meta.purged, meta.membership_log_id
# and in <workdir>/expect.txt what the writer itself reported, one `key value`
# per line, which a reader of the directory is checked against:
#   cluster_id, voters, feature_level, node_generation, node_addr, topic,
#   last_applied_index.
#
# Usage: meta-scenario.sh <vtop-node> <vtopctl> <workdir>
# Environment: LEVEL (default 0: leave the level at its baseline; max: the
#              highest level the writer accepts; or a number),
#              ENTRIES (default 5100), META_PORT_BASE (default 9100).
set -euo pipefail

[[ $# -eq 3 ]] || { echo "usage: meta-scenario.sh <vtop-node> <vtopctl> <workdir>" >&2; exit 2; }
NODE="$1" CTL="$2" WORKDIR="$3"
LEVEL="${LEVEL:-0}"
ENTRIES="${ENTRIES:-5100}"
PORT_BASE="${META_PORT_BASE:-9100}"
[[ "$LEVEL" =~ ^([0-9]+|max)$ && "$ENTRIES" =~ ^[0-9]+$ ]] || { echo "LEVEL is a number or max, ENTRIES a number" >&2; exit 2; }
[[ -x "$NODE" && -x "$CTL" ]] || { echo "$NODE and $CTL must be executable" >&2; exit 2; }

CLUSTER_ID=11111111-2222-3333-4444-555555555555
NODE_UUIDS=(cccccccc-0000-0000-0000-000000000001 cccccccc-0000-0000-0000-000000000002 cccccccc-0000-0000-0000-000000000003)
TOPIC_NAME=compat.v1
TOPIC_UUID=bbbbbbbb-0000-0000-0000-0000000000b1
RANGE_UUID=bbbbbbbb-0000-0000-0000-0000000000c1
PEER="127.0.0.1:$PORT_BASE"
ADMIN="127.0.0.1:$((PORT_BASE + 100))"

fail() { echo "[meta-scenario] FAIL: $*" >&2; exit 1; }

rm -rf "${WORKDIR:?}"
mkdir -p "$WORKDIR/data" "$WORKDIR/logs"
cat > "$WORKDIR/meta.yaml" << EOF
node_id: 1
cluster_id: $CLUSTER_ID
data_dir: $WORKDIR/data
peer_listen: "$PEER"
admin_listen: "$ADMIN"
peers:
  - { id: 1, addr: "$PEER", server_name: "localhost" }
peer_transport: plaintext
admin_transport: plaintext
EOF
cat > "$WORKDIR/admin.yaml" << EOF
endpoint: "$ADMIN"
server_name: "localhost"
transport: plaintext
EOF

"$NODE" meta --config "$WORKDIR/meta.yaml" > "$WORKDIR/logs/node.log" 2>&1 &
NODE_PID=$!
trap 'kill -9 "$NODE_PID" 2> /dev/null || true' EXIT

for _ in $(seq 1 100); do
  grep -q '^meta_node_ready' "$WORKDIR/logs/node.log" 2> /dev/null && break
  kill -0 "$NODE_PID" 2> /dev/null || fail "the node exited; see $WORKDIR/logs/node.log"
  sleep 0.1
done
grep -q '^meta_node_ready' "$WORKDIR/logs/node.log" || fail "the node did not become ready"

# A fixed, increasing request id per command, so the log's content does not
# depend on when it was written.
# (Set in the caller's shell: a command substitution would increment a copy.)
seq_id=0
RID=
next_rid() {
  seq_id=$((seq_id + 1))
  RID="$(printf 'dddddddd-0000-0000-0000-%012x' "$seq_id")"
}
ctl() { # <subcommand...>: run `vtopctl meta` quietly, keep the last output
  "$CTL" --json meta "$@" --config "$WORKDIR/admin.yaml" 2> "$WORKDIR/logs/ctl.err" > "$WORKDIR/logs/ctl.out" \
    || { cat "$WORKDIR/logs/ctl.err" >&2; fail "vtopctl meta $1 failed"; }
}

ctl init --members 1
for i in 0 1 2; do
  next_rid
  ctl register-node --node-uuid "${NODE_UUIDS[$i]}" --addr "10.0.0.$((i + 1)):9200" --request-id "$RID"
done
next_rid
ctl create-topic --name "$TOPIC_NAME" --topic-uuid "$TOPIC_UUID" --root-range-uuid "$RANGE_UUID" --request-id "$RID"
# The baseline level every release starts a cluster at.
FEATURE_LEVEL=3
level_in_force() {
  ctl node-versions
  python3 -I -c 'import json,sys; print(json.load(sys.stdin)["feature_level"])' < "$WORKDIR/logs/ctl.out"
}
if [[ "$LEVEL" == max ]]; then
  # The highest level this writer accepts: each release refuses a raise above
  # the level it supports (its vtopctl before proposing, its state machine
  # when applying), so go down from above any level there is.
  for candidate in 12 11 10 9 8 7 6 5 4; do
    next_rid
    if "$CTL" --json meta enable-features --level "$candidate" --request-id "$RID" \
      --config "$WORKDIR/admin.yaml" > "$WORKDIR/logs/ctl.out" 2> "$WORKDIR/logs/ctl.err" \
      && grep -q '"response": "Ack' "$WORKDIR/logs/ctl.out" \
      && [[ "$(level_in_force)" == "$candidate" ]]; then
      FEATURE_LEVEL="$candidate"
      break
    fi
  done
  ((FEATURE_LEVEL > 3)) || fail "no feature level above the baseline could be raised"
elif ((LEVEL > 0)); then
  next_rid
  ctl enable-features --level "$LEVEL" --request-id "$RID"
  grep -q '"response": "Ack' "$WORKDIR/logs/ctl.out" \
    || fail "enable-features was not acknowledged: $(cat "$WORKDIR/logs/ctl.out")"
  [[ "$(level_in_force)" == "$LEVEL" ]] || fail "the cluster does not report feature level $LEVEL"
  FEATURE_LEVEL="$LEVEL"
fi
# The first node's generation is 0; each re-registration names the one it
# replaces, so the loop ends at ENTRIES.
for ((i = 0; i < ENTRIES; i++)); do
  next_rid
  ctl register-node --node-uuid "${NODE_UUIDS[0]}" --addr "10.0.0.1:9200" \
    --expected-generation "$i" --request-id "$RID"
done

ctl status
APPLIED="$(python3 -I -c 'import json,sys; print(json.load(sys.stdin)["last_applied"]["index"])' < "$WORKDIR/logs/ctl.out")"
ctl membership
python3 -I -c 'import json,sys; assert json.load(sys.stdin)["voters"] == [1]' < "$WORKDIR/logs/ctl.out" \
  || fail "the voter set is not [1]"

{
  echo "cluster_id $CLUSTER_ID"
  echo "voters 1"
  echo "feature_level $FEATURE_LEVEL"
  echo "node_uuid ${NODE_UUIDS[0]}"
  echo "node_generation $ENTRIES"
  echo "node_addr 10.0.0.1:9200"
  echo "topic $TOPIC_NAME"
  echo "last_applied_index $APPLIED"
} > "$WORKDIR/expect.txt"

kill -9 "$NODE_PID" 2> /dev/null || true
wait "$NODE_PID" 2> /dev/null || true
trap - EXIT
echo "[meta-scenario] wrote $WORKDIR/data (last applied $APPLIED, level $FEATURE_LEVEL)" >&2
