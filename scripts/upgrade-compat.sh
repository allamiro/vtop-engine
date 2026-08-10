#!/usr/bin/env bash
# Upgrade compatibility: a data directory written by an OLD release must open
# under the CURRENT build (#291).
#
# Every durability test builds its fixtures with the code under test, so the
# on-disk format is only ever validated against itself — the one bug class
# that reaches users and cannot be fixed forward: the operator upgrades, the
# node refuses its own data, and the range is down until someone downgrades.
#
# Phases, per old binary:
#   1. The OLD binary serves a range and its own `produce` writes real
#      records (rolled into sealed segments where the old version could roll;
#      a single `range.active` where it could not — v0.2.x, which is exactly
#      the compatibility claim `open_range` makes).
#   2. The old node is stopped with SIGKILL — deliberately: pre-#280 releases
#      have no SIGTERM handler, so the crash path IS their upgrade path.
#   3. The NEW binary opens the SAME directory with the SAME config and must:
#      report ready, quarantine nothing, serve every record byte-exactly
#      (`verify --expect-at-least`), and stop cleanly on SIGTERM (#280).
#
# DOWNGRADE IS NOT TESTED AND NOT SUPPORTED — stated in ROADMAP.md rather
# than left as an untested assumption, per the issue.
#
# Usage: upgrade-compat.sh <old-label> <old-vtop-node> <new-vtop-node> [profile]
#   profile `legacy`  (default): minimal config every release parses; the old
#     version cannot roll, so the fixture is the single-`range.active` shape.
#   profile `rolling`: adds the #313 threshold keys with small bounds so the
#     fixture holds sealed segments plus a tail (v0.3.0+ only; older releases
#     refuse unknown config keys, which is itself the compatibility contract).
set -euo pipefail

OLD_LABEL="${1:?usage: upgrade-compat.sh <old-label> <old-vtop-node> <new-vtop-node> [profile]}"
OLD_BIN="${2:?old vtop-node binary}"
NEW_BIN="${3:?new vtop-node binary}"
PROFILE="${4:-legacy}"
[[ -x "$OLD_BIN" ]] || { echo "FAIL: $OLD_BIN is not executable" >&2; exit 1; }
[[ -x "$NEW_BIN" ]] || { echo "FAIL: $NEW_BIN is not executable" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKDIR="${UPGRADE_WORKDIR:-$(mktemp -d "${TMPDIR:-/tmp}/vtop-upgrade.XXXXXX")}"
mkdir -p "$WORKDIR/logs"
RECORDS="${UPGRADE_RECORDS:-2000}"
# Small enough that a batch fits the rolling profile's 16 KiB group bound.
BATCH="${UPGRADE_BATCH:-64}"
NATIVE_PORT="${UPGRADE_NATIVE_PORT:-9700}"
READY_TIMEOUT="${UPGRADE_READY_TIMEOUT_SECONDS:-30}"
STOP_TIMEOUT="${UPGRADE_STOP_TIMEOUT_SECONDS:-10}"

# Identities and fixture values mirror the live-chaos harness exactly — the
# combination proven in CI against every release this script replays.
CLUSTER_ID="aaaaaaaa-0000-0000-0000-0000000000c0"
RANGE_ID="aaaaaaaa-0000-0000-0000-0000000000d0"
SEGMENT_ID="aaaaaaaa-0000-0000-0000-0000000000e0"
NODE_UUID="aaaaaaaa-0000-0000-0000-0000000000a1"
FOLLOWER_UUID="aaaaaaaa-0000-0000-0000-0000000000a2"
PRINCIPAL_ID="aaaaaaaa-0000-0000-0000-0000000000ce"
TOPIC="upgrade.v1"
FENCING_EPOCH=18

log() { echo "[upgrade-compat/$OLD_LABEL] $*"; }
fail() { echo "[upgrade-compat/$OLD_LABEL] FAIL: $*" >&2; exit 1; }

# Nodes started by this script die with it: a failed assertion must not leak
# processes holding the fixture ports into the next invocation. Tracked in a
# FILE, not a shell array — start_node runs inside command substitution, so
# an array append there would land in a subshell and vanish.
cleanup() {
  if [[ -f "$WORKDIR/pids" ]]; then
    while read -r pid; do
      kill -9 "$pid" 2>/dev/null || true
    done < "$WORKDIR/pids"
  fi
}
trap cleanup EXIT

CERTS="$WORKDIR/certs"
"$SCRIPT_DIR/live-chaos/gen-certs.sh" "$CERTS" 1 -- "$NODE_UUID" "$FOLLOWER_UUID" > /dev/null

DATA_DIR="$WORKDIR/data"
FOLLOWER_DIR="$WORKDIR/data-follower"
mkdir -p "$DATA_DIR" "$FOLLOWER_DIR"
REPLICA_PORT=$((NATIVE_PORT + 1))

# Configs: only fields every supported release understands, plus the rolling
# thresholds for profiles that may use them. Old releases use
# `deny_unknown_fields`, so anything newer would refuse to parse — which is
# itself a compatibility statement: a config that worked before the upgrade
# keeps working after it.
#
# The OLD phase runs a leader + one follower: v0.2.1's leader requires at
# least one follower, and quorum produce is what advances the committed HWM
# identically on every release. The directory under test is the LEADER's.
range_yaml() {
  echo "range:"
  echo "  topic: $TOPIC"
  echo "  topic_epoch: 1"
  echo "  range_id: $RANGE_ID"
  echo "  range_generation: 0"
}
thresholds_yaml() {
  if [[ "$PROFILE" == "rolling" ]]; then
    # Small bounds so the fixture actually rolls; keys exist from #313 on.
    echo "max_record_bytes: 4096"
    echo "max_group_bytes: 16384"
    echo "max_segment_bytes: 16384"
    echo "max_segment_records: 1000"
  fi
}
OLD_LEADER_CFG="$WORKDIR/old-leader.yaml"
{
  echo "role: leader"
  echo "node_uuid: $NODE_UUID"
  echo "cluster_id: $CLUSTER_ID"
  echo "data_dir: $DATA_DIR"
  echo "fencing_epoch: $FENCING_EPOCH"
  range_yaml
  echo "segment_id: $SEGMENT_ID"
  echo "native_listen: 127.0.0.1:$NATIVE_PORT"
  echo "principal_id: $PRINCIPAL_ID"
  echo "followers:"
  echo "  - { node_uuid: $FOLLOWER_UUID, addr: \"127.0.0.1:$REPLICA_PORT\", server_name: \"localhost\" }"
  echo "replica_tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-1.pem, key: $CERTS/data-1-key.pem }"
  echo "native_tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-1.pem, key: $CERTS/data-1-key.pem }"
  thresholds_yaml
} > "$OLD_LEADER_CFG"
OLD_FOLLOWER_CFG="$WORKDIR/old-follower.yaml"
{
  echo "role: follower"
  echo "node_uuid: $FOLLOWER_UUID"
  echo "cluster_id: $CLUSTER_ID"
  echo "data_dir: $FOLLOWER_DIR"
  echo "fencing_epoch: $FENCING_EPOCH"
  range_yaml
  echo "segment_id: $SEGMENT_ID"
  echo "replica_listen: \"127.0.0.1:$REPLICA_PORT\""
  echo "replica_tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-2.pem, key: $CERTS/data-2-key.pem }"
  thresholds_yaml
} > "$OLD_FOLLOWER_CFG"
# The NEW phase opens the leader's DIRECTORY as a standalone role: the
# directory is what is under test, and standalone (which v0.2.1 predates) is
# the current build's single-process shape for exactly this situation.
NEW_CFG="$WORKDIR/new-node.yaml"
{
  echo "role: standalone"
  echo "node_uuid: $NODE_UUID"
  echo "cluster_id: $CLUSTER_ID"
  echo "data_dir: $DATA_DIR"
  echo "fencing_epoch: $FENCING_EPOCH"
  range_yaml
  echo "segment_id: $SEGMENT_ID"
  echo "native_listen: 127.0.0.1:$NATIVE_PORT"
  echo "principal_id: $PRINCIPAL_ID"
  echo "replica_tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-1.pem, key: $CERTS/data-1-key.pem }"
  echo "native_tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-1.pem, key: $CERTS/data-1-key.pem }"
  thresholds_yaml
} > "$NEW_CFG"

CLIENT_CFG="$WORKDIR/client.yaml"
{
  echo "cluster_id: $CLUSTER_ID"
  echo "principal_id: $PRINCIPAL_ID"
  echo "producer_id: $PRINCIPAL_ID"
  echo "producer_epoch: 1"
  echo "fencing_epoch: $FENCING_EPOCH"
  echo "range:"
  echo "  topic: $TOPIC"
  echo "  topic_epoch: 1"
  echo "  range_id: $RANGE_ID"
  echo "  range_generation: 0"
  echo "server_name: \"localhost\""
  echo "tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-1.pem, key: $CERTS/data-1-key.pem }"
} > "$CLIENT_CFG"

start_node() { # <binary> <config> <log-name> -> pid
  local binary="$1" config="$2" name="$3"
  "$binary" data --config "$config" > "$WORKDIR/logs/$name.log" 2>&1 &
  local pid=$!
  local deadline=$((SECONDS + READY_TIMEOUT))
  until grep -q "data_node_ready" "$WORKDIR/logs/$name.log" 2>/dev/null; do
    kill -0 "$pid" 2>/dev/null \
      || fail "$name exited before ready; log tail: $(tail -3 "$WORKDIR/logs/$name.log")"
    [[ $SECONDS -lt $deadline ]] \
      || fail "$name not ready within ${READY_TIMEOUT}s; log tail: $(tail -3 "$WORKDIR/logs/$name.log")"
    sleep 0.2
  done
  echo "$pid" >> "$WORKDIR/pids"
  echo "$pid"
}

# A gone process OR a zombie counts as stopped: the nodes are reparented
# when start_node's command-substitution subshell exits, and a container
# whose PID 1 does not reap orphans leaves them as zombies `kill -0` still
# sees — the same reasoning as the live-chaos harness's stop_node_now.
is_stopped() { # <pid>
  if ! kill -0 "$1" 2>/dev/null; then
    return 0
  fi
  local state
  state="$(ps -o stat= -p "$1" 2>/dev/null | tr -d ' ')"
  [[ "$state" == Z* ]]
}

kill_hard() { # <pid>
  kill -9 "$1" 2>/dev/null || true
  local deadline=$((SECONDS + STOP_TIMEOUT))
  until is_stopped "$1"; do
    [[ $SECONDS -lt $deadline ]] || fail "pid $1 did not stop"
    sleep 0.05
  done
}

# --- phase 1: the old release writes a real directory ------------------------
OLD_FOLLOWER_PID="$(start_node "$OLD_BIN" "$OLD_FOLLOWER_CFG" "old-follower")"
OLD_PID="$(start_node "$OLD_BIN" "$OLD_LEADER_CFG" "old-node")"
log "old leader + follower ($OLD_LABEL) ready, producing $RECORDS records"
"$OLD_BIN" produce --client-config "$CLIENT_CFG" --addr "127.0.0.1:$NATIVE_PORT" \
  --records "$RECORDS" --batch "$BATCH" --durability quorum \
  > "$WORKDIR/logs/old-produce.log" 2>&1 \
  || fail "old produce failed: $(tail -3 "$WORKDIR/logs/old-produce.log")"
"$OLD_BIN" verify --client-config "$CLIENT_CFG" --addr "127.0.0.1:$NATIVE_PORT" \
  --expect-at-least "$RECORDS" > "$WORKDIR/logs/old-verify.log" 2>&1 \
  || fail "the old binary cannot read its own records: $(tail -3 "$WORKDIR/logs/old-verify.log")"
SEALED_COUNT=$(find "$DATA_DIR" -name '*.segment' | wc -l | tr -d ' ')
if [[ "$PROFILE" == "rolling" ]]; then
  [[ "$SEALED_COUNT" -ge 1 ]] \
    || fail "the rolling profile must produce sealed segments; the fixture did not roll"
fi
log "old directory written: $SEALED_COUNT sealed segment(s) plus the active tail"
# SIGKILL, deliberately: pre-#280 releases ignore SIGTERM, so this is the stop
# every real upgrade of those releases performs.
kill_hard "$OLD_PID"
kill_hard "$OLD_FOLLOWER_PID"
log "old nodes stopped (SIGKILL — the pre-#280 upgrade path)"

# --- phase 2: the current build must open that directory ---------------------
NEW_PID="$(start_node "$NEW_BIN" "$NEW_CFG" "new-node")"
log "current build opened the $OLD_LABEL directory and reported ready"
if grep -qi "quarantin" "$WORKDIR/logs/new-node.log"; then
  fail "the current build quarantined artifacts from a $OLD_LABEL directory: \
$(grep -i quarantin "$WORKDIR/logs/new-node.log" | head -2)"
fi
"$NEW_BIN" verify --client-config "$CLIENT_CFG" --addr "127.0.0.1:$NATIVE_PORT" \
  --expect-at-least "$RECORDS" > "$WORKDIR/logs/new-verify.log" 2>&1 \
  || fail "records written by $OLD_LABEL are not readable under the current build: \
$(tail -3 "$WORKDIR/logs/new-verify.log")"
log "every record written by $OLD_LABEL reads back byte-exact under the current build"

# The current build stops orderly (#280): SIGTERM must drain within the
# deadline and write the final commit boundary.
kill "$NEW_PID" 2>/dev/null || true
deadline=$((SECONDS + STOP_TIMEOUT))
until is_stopped "$NEW_PID"; do
  [[ $SECONDS -lt $deadline ]] \
    || fail "the current build ignored SIGTERM after an upgrade open (#280 reopened)"
  sleep 0.05
done
grep -q "data_node_stopped" "$WORKDIR/logs/new-node.log" \
  || fail "the current build exited without its final commit boundary marker"
log "the current build drained on SIGTERM over the upgraded directory"

# And the directory reopens once more — the post-upgrade restart an operator
# will actually perform after a clean stop — and must still SERVE everything:
# reaching ready proves the open, not the data. A wrong final commit boundary
# would pass a readiness check and lose records anyway.
AGAIN_PID="$(start_node "$NEW_BIN" "$NEW_CFG" "again-node")"
"$NEW_BIN" verify --client-config "$CLIENT_CFG" --addr "127.0.0.1:$NATIVE_PORT" \
  --expect-at-least "$RECORDS" > "$WORKDIR/logs/again-verify.log" 2>&1 \
  || fail "records did not survive the clean-stop restart: \
$(tail -3 "$WORKDIR/logs/again-verify.log")"
log "every record still serves after the clean-stop restart"
kill_hard "$AGAIN_PID"
log "PASS: a $OLD_LABEL data directory opens, serves, and survives under the current build"
if [[ -z "${UPGRADE_KEEP:-}" ]]; then
  rm -rf "$WORKDIR"
fi
exit 0
