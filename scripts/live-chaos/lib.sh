#!/usr/bin/env bash
# Shared plumbing for the live 3-node chaos scenarios (#215).
#
# Every scenario sources this file. Nodes are real OS processes started from
# target/release; scenarios kill -9, partition, and starve them, then assert
# invariants with byte-exact verification and admin status probes.
#
# Environment overrides:
#   CHAOS_WORKDIR   exact scratch directory (default: mktemp under $TMPDIR,
#                   falling back to /tmp)
#   CHAOS_TMPDIR    parent for generated workdirs (default: $TMPDIR or /tmp)
#   CHAOS_KEEP=1    keep the workdir on exit (for debugging)
#   CHAOS_PROFILE   cargo profile dir to run from (default: release; the
#                   scenarios assert correctness, so debug also works)
#   CHAOS_MIN_FREE_MIB minimum free scratch space (default: 256)
#   CHAOS_SHIM_DIR  writable directory for compiled fault shims (default:
#                   target/live-chaos)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT_DIR="$REPO_ROOT/scripts/live-chaos"
CHAOS_PROFILE="${CHAOS_PROFILE:-release}"
VTOP_NODE="$REPO_ROOT/target/$CHAOS_PROFILE/vtop-node"
VTOPCTL="$REPO_ROOT/target/$CHAOS_PROFILE/vtopctl"

CLUSTER_ID="${CHAOS_CLUSTER_ID:-11111111-2222-3333-4444-555555555555}"
RANGE_ID="${CHAOS_RANGE_ID:-aaaaaaaa-0000-0000-0000-0000000000c1}"
SEGMENT_ID="${CHAOS_SEGMENT_ID:-aaaaaaaa-0000-0000-0000-0000000000d1}"
# The broker requires the producer id to equal the authenticated session
# principal, so the harness uses one identity for both.
PRINCIPAL_ID="${CHAOS_PRINCIPAL_ID:-aaaaaaaa-0000-0000-0000-0000000000ce}"
PRODUCER_ID="$PRINCIPAL_ID"
LEADER_UUID="${CHAOS_LEADER_UUID:-aaaaaaaa-0000-0000-0000-0000000000a1}"
FOLLOWER1_UUID="${CHAOS_FOLLOWER1_UUID:-aaaaaaaa-0000-0000-0000-0000000000a2}"
FOLLOWER2_UUID="${CHAOS_FOLLOWER2_UUID:-aaaaaaaa-0000-0000-0000-0000000000a3}"
FENCING_EPOCH="${CHAOS_FENCING_EPOCH:-18}"
TOPIC="${CHAOS_TOPIC:-chaos.v1}"

META_HOST="${CHAOS_META_HOST:-127.0.0.1}"
DATA_HOST="${CHAOS_DATA_HOST:-127.0.0.1}"
META_PEER_BASE_PORT="${CHAOS_META_PEER_BASE_PORT:-9100}"
META_ADMIN_BASE_PORT="${CHAOS_META_ADMIN_BASE_PORT:-9200}"
REPLICA_BASE_PORT="${CHAOS_REPLICA_BASE_PORT:-9300}"
NATIVE_PORT="${CHAOS_NATIVE_PORT:-9400}"
REGISTER_HOST_PREFIX="${CHAOS_REGISTER_HOST_PREFIX:-10.0.0}"
REGISTER_PORT="${CHAOS_REGISTER_PORT:-9200}"
READY_TIMEOUT_SECONDS="${CHAOS_READY_TIMEOUT_SECONDS:-20}"
ELECTION_TIMEOUT_SECONDS="${CHAOS_ELECTION_TIMEOUT_SECONDS:-30}"
PROGRESS_TIMEOUT_SECONDS="${CHAOS_PROGRESS_TIMEOUT_SECONDS:-30}"
STOP_TIMEOUT_SECONDS="${CHAOS_STOP_TIMEOUT_SECONDS:-10}"

meta_peer_port()  { echo "$((META_PEER_BASE_PORT + $1))"; }
meta_admin_port() { echo "$((META_ADMIN_BASE_PORT + $1))"; }
meta_host() {
  if [[ -n "${VTOP_META_HOST_PREFIX:-}" ]]; then
    echo "$VTOP_META_HOST_PREFIX.$1"
  else
    echo "$META_HOST"
  fi
}
replica_port()    { echo "$((REPLICA_BASE_PORT + $1))"; }
native_addr()     { echo "$DATA_HOST:$NATIVE_PORT"; }
replica_addr()    { echo "$DATA_HOST:$(replica_port "$1")"; }

SCENARIO="${SCENARIO:-$(basename "${0%.sh}")}"

log()  { printf '[%s] %s\n' "$SCENARIO" "$*"; }
fail() { log "FAIL: $*"; exit 1; }

require_command() { # <command> <installation/remediation hint>
  local command="$1" hint="$2"
  command -v "$command" > /dev/null 2>&1 \
    || fail "required command '$command' is unavailable. $hint"
}

preflight_common_tools() {
  require_command openssl "Install OpenSSL or make it available on PATH."
  require_command python3 "Install Python 3 or make it available on PATH."
  require_command df "Install a POSIX df implementation so scratch capacity can be checked."
  require_command awk "Install a POSIX awk implementation."
  require_command grep "Install a POSIX grep implementation."
  require_command sed "Install a POSIX sed implementation."
}

require_integer_in_range() { # <name> <value> <minimum> <maximum>
  local name="$1" value="$2" minimum="$3" maximum="$4"
  [[ "$value" =~ ^[0-9]+$ ]] \
    || fail "$name must be an integer, got '$value'"
  local numeric=$((10#$value))
  (( numeric >= minimum && numeric <= maximum )) \
    || fail "$name must be between $minimum and $maximum, got '$value'"
}

preflight_settings() {
  [[ "$CHAOS_PROFILE" =~ ^[A-Za-z0-9_-]+$ ]] \
    || fail "CHAOS_PROFILE contains unsupported characters: '$CHAOS_PROFILE'"
  require_integer_in_range CHAOS_META_PEER_BASE_PORT "$META_PEER_BASE_PORT" 1024 65529
  require_integer_in_range CHAOS_META_ADMIN_BASE_PORT "$META_ADMIN_BASE_PORT" 1024 65529
  require_integer_in_range CHAOS_REPLICA_BASE_PORT "$REPLICA_BASE_PORT" 1024 65533
  require_integer_in_range CHAOS_NATIVE_PORT "$NATIVE_PORT" 1024 65535
  require_integer_in_range CHAOS_REGISTER_PORT "$REGISTER_PORT" 1 65535
  require_integer_in_range CHAOS_FENCING_EPOCH "$FENCING_EPOCH" 1 2147483647
  require_integer_in_range CHAOS_READY_TIMEOUT_SECONDS "$READY_TIMEOUT_SECONDS" 1 3600
  require_integer_in_range CHAOS_ELECTION_TIMEOUT_SECONDS "$ELECTION_TIMEOUT_SECONDS" 1 3600
  require_integer_in_range CHAOS_PROGRESS_TIMEOUT_SECONDS "$PROGRESS_TIMEOUT_SECONDS" 1 3600
  require_integer_in_range CHAOS_STOP_TIMEOUT_SECONDS "$STOP_TIMEOUT_SECONDS" 1 3600

  local labels=() endpoints=() id existing
  for id in 1 2 3 4 5; do
    labels+=("meta-peer-$id" "meta-admin-$id")
    endpoints+=(
      "$(meta_host "$id"):$(meta_peer_port "$id")"
      "$(meta_host "$id"):$(meta_admin_port "$id")"
    )
  done
  for id in 1 2; do
    labels+=("replica-$id")
    endpoints+=("$(replica_addr "$id")")
  done
  labels+=("native")
  endpoints+=("$(native_addr)")
  for ((id = 0; id < ${#endpoints[@]}; id++)); do
    for ((existing = 0; existing < id; existing++)); do
      if [[ "${endpoints[$id]}" == "${endpoints[$existing]}" ]]; then
        fail "configured listen-address collision: ${labels[$id]} and ${labels[$existing]} both use ${endpoints[$id]}. Choose distinct CHAOS_*_HOST/PORT values."
      fi
    done
  done
}

preflight_workdir() {
  local minimum_mib="${CHAOS_MIN_FREE_MIB:-256}"
  [[ "$minimum_mib" =~ ^[0-9]+$ ]] \
    || fail "CHAOS_MIN_FREE_MIB must be a non-negative integer, got '$minimum_mib'"

  local write_probe="$WORKDIR/.vtop-write-probe-$$"
  if ! (umask 077; printf 'vtop-live-chaos\n' > "$write_probe"); then
    fail "scratch directory is not writable: $WORKDIR. Set CHAOS_WORKDIR to a writable local filesystem."
  fi
  rm -f "$write_probe"

  local available_kib
  available_kib="$(LC_ALL=C df -Pk "$WORKDIR" 2>/dev/null | awk 'NR == 2 { print $4 }')"
  if [[ ! "$available_kib" =~ ^[0-9]+$ ]]; then
    fail "could not determine free space for $WORKDIR. Set CHAOS_WORKDIR to a local filesystem visible to df."
  fi
  if (( available_kib < minimum_mib * 1024 )); then
    fail "only $((available_kib / 1024)) MiB is free at $WORKDIR; at least $minimum_mib MiB is required. Set CHAOS_WORKDIR to a larger filesystem or lower CHAOS_MIN_FREE_MIB only after reviewing scenario sizes."
  fi

  # A noexec scratch mount is supported: generated LD_PRELOAD shims are linked
  # under target/live-chaos instead. Report it because ad-hoc executable fault
  # helpers must use an exec-enabled path, and make the remedy explicit.
  local exec_probe="$WORKDIR/.vtop-exec-probe-$$"
  printf '#!/bin/sh\nexit 0\n' > "$exec_probe"
  chmod 700 "$exec_probe"
  if ! "$exec_probe" > /dev/null 2>&1; then
    log "NOTICE: $WORKDIR does not permit executable files (likely a noexec mount); the harness will keep generated shims under $REPO_ROOT/target/live-chaos. For other executable fixtures, set TMPDIR or VTOP_TEST_EXEC_TMPDIR to a writable exec-enabled directory."
  fi
  rm -f "$exec_probe"
}

require_mount_namespace() {
  require_command unshare "Install util-linux and enable unprivileged user namespaces, or run this scenario in a container that permits mount namespaces."
  require_command mount "Install util-linux mount support."
  local probe_dir="$WORKDIR/.vtop-mountns-probe"
  mkdir -p "$probe_dir"
  if ! unshare -rm bash -c \
    'mount -t tmpfs -o size=1m tmpfs "$1" && touch "$1/probe"' bash "$probe_dir" \
    > /dev/null 2>&1; then
    rmdir "$probe_dir" 2>/dev/null || true
    fail "unprivileged user/mount namespaces or tmpfs mounts are unavailable. Enable the host's unprivileged-userns setting, or run in a container/VM with user and mount namespaces enabled."
  fi
  rmdir "$probe_dir" 2>/dev/null || true
}

require_network_namespace() {
  require_command unshare "Install util-linux and enable unprivileged user namespaces."
  require_command mount "Install util-linux mount support."
  require_command ip "Install iproute2."
  require_command iptables "Install iptables with namespace support."
  require_command timeout "Install GNU coreutils."
  if ! unshare -Urnm bash -c \
    'mount -t tmpfs tmpfs /run && mkdir -p /run/netns && ip link add vtop-preflight type bridge && iptables -L >/dev/null' \
    > /dev/null 2>&1; then
    fail "network-namespace packet filtering is unavailable. Enable unprivileged user/network namespaces and iptables, or run scenario 06 in a Linux container/VM with those capabilities."
  fi
}

prepare_shim_dir() {
  SHIM_DIR="${CHAOS_SHIM_DIR:-$REPO_ROOT/target/live-chaos}"
  if ! mkdir -p "$SHIM_DIR"; then
    fail "cannot create fault-shim directory $SHIM_DIR. Set CHAOS_SHIM_DIR to a writable local directory."
  fi
  local probe="$SHIM_DIR/.vtop-shim-write-probe-$$"
  if ! (umask 077; printf 'probe\n' > "$probe"); then
    fail "fault-shim directory is not writable: $SHIM_DIR. Set CHAOS_SHIM_DIR to a writable local directory."
  fi
  rm -f "$probe"
}

# start_node runs in $(...) subshells, so PIDs are tracked in a file, not a
# shell array — otherwise cleanup would never see them.
cleanup() {
  local code=$?
  if [[ -n "${WORKDIR:-}" && -f "$WORKDIR/pids" ]]; then
    while read -r pid; do
      kill -9 "$pid" 2>/dev/null || true
    done < "$WORKDIR/pids"
  fi
  wait 2>/dev/null || true
  if [[ "${CHAOS_KEEP:-0}" != "1" && "${WORKDIR_GENERATED:-0}" == "1" ]]; then
    rm -rf "$WORKDIR"
  elif [[ -n "${WORKDIR:-}" ]]; then
    log "workdir kept: $WORKDIR"
  fi
  if [[ $code -eq 0 ]]; then log "PASS"; else log "FAILED (exit $code)"; fi
  exit $code
}
trap cleanup EXIT

init_workdir() {
  if [[ -n "${CHAOS_WORKDIR:-}" ]]; then
    WORKDIR="$CHAOS_WORKDIR"
    WORKDIR_GENERATED=0
    mkdir -p "$WORKDIR" \
      || fail "cannot create CHAOS_WORKDIR=$WORKDIR; choose a writable local filesystem"
  else
    local scratch_root="${CHAOS_TMPDIR:-${TMPDIR:-/tmp}}"
    require_command mktemp "Install GNU/coreutils mktemp or provide an exact CHAOS_WORKDIR."
    if [[ -n "${CHAOS_TMPDIR:-}" && ! -d "$scratch_root" ]]; then
      mkdir -p "$scratch_root" \
        || fail "cannot create CHAOS_TMPDIR=$scratch_root; choose a writable parent directory"
    fi
    [[ -d "$scratch_root" ]] \
      || fail "temporary root does not exist: $scratch_root. Create it or set TMPDIR/CHAOS_WORKDIR to a writable directory."
    WORKDIR="$(mktemp -d "$scratch_root/vtop-chaos.XXXXXX")" \
      || fail "cannot create scratch space under $scratch_root. Set TMPDIR or CHAOS_WORKDIR to a writable filesystem."
    WORKDIR_GENERATED=1
  fi
  preflight_workdir
  mkdir -p "$WORKDIR/logs" \
    || fail "cannot create log directory under $WORKDIR; set CHAOS_WORKDIR to a writable filesystem"
  CERTS="$WORKDIR/certs"
  "$SCRIPT_DIR/gen-certs.sh" "$CERTS" 1 2 3 4 5 6 -- \
    "$LEADER_UUID" "$FOLLOWER1_UUID" "$FOLLOWER2_UUID" > /dev/null
  log "workdir=$WORKDIR"
}

require_binaries() {
  preflight_settings
  preflight_common_tools
  [[ -x "$VTOP_NODE" ]] || fail "missing $VTOP_NODE — build vtop-node with the Cargo profile selected by CHAOS_PROFILE (release uses --release; debug uses the default dev profile)"
  [[ -x "$VTOPCTL" ]] || fail "missing $VTOPCTL — build vtop-cli --no-default-features with the Cargo profile selected by CHAOS_PROFILE (release uses --release; debug uses the default dev profile)"
  "$VTOP_NODE" --help > /dev/null 2>&1 \
    || fail "cannot execute $VTOP_NODE. Its filesystem may be mounted noexec; rebuild with CARGO_TARGET_DIR on an exec-enabled filesystem."
  "$VTOPCTL" --help > /dev/null 2>&1 \
    || fail "cannot execute $VTOPCTL. Its filesystem may be mounted noexec; rebuild with CARGO_TARGET_DIR on an exec-enabled filesystem."
}

# ---------------------------------------------------------------------------
# Config emission
# ---------------------------------------------------------------------------

# emit_meta_config <node-id> <peer-ids...>
emit_meta_config() {
  local id="$1"; shift
  local cfg="$WORKDIR/meta-$id.yaml"
  {
    echo "node_id: $id"
    echo "cluster_id: $CLUSTER_ID"
    echo "data_dir: $WORKDIR/meta-$id"
    echo "peer_listen: \"$(meta_host "$id"):$(meta_peer_port "$id")\""
    echo "admin_listen: \"$(meta_host "$id"):$(meta_admin_port "$id")\""
    echo "peers:"
    local peer
    for peer in "$@"; do
      echo "  - { id: $peer, addr: \"$(meta_host "$peer"):$(meta_peer_port "$peer")\", server_name: \"localhost\" }"
    done
    echo "tls: { ca: $CERTS/ca.pem, cert: $CERTS/meta-$id.pem, key: $CERTS/meta-$id-key.pem }"
  } > "$cfg"
  echo "$cfg"
}

# emit_admin_config <node-id> — client config for vtopctl meta
emit_admin_config() {
  local id="$1"
  local cfg="$WORKDIR/admin-$id.yaml"
  {
    echo "endpoint: \"$(meta_host "$id"):$(meta_admin_port "$id")\""
    echo "server_name: \"localhost\""
    echo "ca_cert: $CERTS/ca.pem"
    echo "client_cert: $CERTS/admin.pem"
    echo "client_key: $CERTS/admin-key.pem"
  } > "$cfg"
  echo "$cfg"
}

emit_range_yaml() {
  echo "range: { topic: \"$TOPIC\", topic_epoch: 1, range_id: $RANGE_ID, range_generation: 0 }"
}

# emit_leader_config <role: leader|standalone>
emit_leader_config() {
  local role="$1"
  local cfg="$WORKDIR/data-leader-$role.yaml"
  {
    echo "role: $role"
    echo "node_uuid: $LEADER_UUID"
    echo "cluster_id: $CLUSTER_ID"
    echo "data_dir: $WORKDIR/data-leader"
    echo "fencing_epoch: $FENCING_EPOCH"
    emit_range_yaml
    echo "segment_id: $SEGMENT_ID"
    echo "native_listen: \"$(native_addr)\""
    if [[ "$role" == "leader" ]]; then
      echo "followers:"
      echo "  - { node_uuid: $FOLLOWER1_UUID, addr: \"$(replica_addr 1)\", server_name: \"localhost\" }"
      echo "  - { node_uuid: $FOLLOWER2_UUID, addr: \"$(replica_addr 2)\", server_name: \"localhost\" }"
    fi
    echo "replica_tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-1.pem, key: $CERTS/data-1-key.pem }"
    echo "native_tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-1.pem, key: $CERTS/data-1-key.pem }"
    echo "principal_id: $PRINCIPAL_ID"
  } > "$cfg"
  echo "$cfg"
}

# emit_follower_config <n: 1|2> [data_dir]
emit_follower_config() {
  local n="$1"
  local dir="${2:-$WORKDIR/data-follower-$n}"
  local uuid cert
  case "$n" in
    1) uuid="$FOLLOWER1_UUID"; cert="data-2" ;;
    2) uuid="$FOLLOWER2_UUID"; cert="data-3" ;;
    *) fail "unknown follower $n" ;;
  esac
  local cfg="$WORKDIR/data-follower-$n.yaml"
  {
    echo "role: follower"
    echo "node_uuid: $uuid"
    echo "cluster_id: $CLUSTER_ID"
    echo "data_dir: $dir"
    echo "fencing_epoch: $FENCING_EPOCH"
    emit_range_yaml
    echo "segment_id: $SEGMENT_ID"
    echo "replica_listen: \"$(replica_addr "$n")\""
    echo "replica_tls: { ca: $CERTS/ca.pem, cert: $CERTS/$cert.pem, key: $CERTS/$cert-key.pem }"
  } > "$cfg"
  echo "$cfg"
}

emit_client_config() {
  local cfg="$WORKDIR/client.yaml"
  {
    echo "cluster_id: $CLUSTER_ID"
    echo "principal_id: $PRINCIPAL_ID"
    echo "producer_id: $PRODUCER_ID"
    echo "producer_epoch: 1"
    echo "fencing_epoch: $FENCING_EPOCH"
    emit_range_yaml
    echo "server_name: \"localhost\""
    echo "tls: { ca: $CERTS/ca.pem, cert: $CERTS/client.pem, key: $CERTS/client-key.pem }"
  } > "$cfg"
  echo "$cfg"
}

# Client config that authenticates as the LEADER on the replica plane
# (replica-status probes).
emit_replica_probe_config() {
  local cfg="$WORKDIR/replica-probe.yaml"
  {
    echo "cluster_id: $CLUSTER_ID"
    echo "principal_id: $PRINCIPAL_ID"
    echo "producer_id: $PRODUCER_ID"
    echo "producer_epoch: 1"
    echo "fencing_epoch: $FENCING_EPOCH"
    emit_range_yaml
    echo "server_name: \"localhost\""
    echo "tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-1.pem, key: $CERTS/data-1-key.pem }"
  } > "$cfg"
  echo "$cfg"
}

# ---------------------------------------------------------------------------
# Process lifecycle
# ---------------------------------------------------------------------------

# start_command <log-name> <ready-marker> <command...> — echoes the PID
start_command() {
  local name="$1" marker="$2"; shift 2
  local logfile="$WORKDIR/logs/$name.log"
  "$@" > "$logfile" 2>&1 &
  local pid=$!
  echo "$pid" >> "$WORKDIR/pids"
  local deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
  until grep -q "$marker" "$logfile" 2>/dev/null; do
    if ! kill -0 "$pid" 2>/dev/null; then
      sed 's/^/    /' "$logfile" >&2 || true
      if grep -q "Address already in use" "$logfile" 2>/dev/null; then
        fail "$name could not bind its listen address. Stop the process using the configured ports, select different CHAOS_*_PORT values, or run the harness in an isolated host/network namespace."
      fi
      fail "$name exited before becoming ready"
    fi
    [[ $SECONDS -lt $deadline ]] \
      || fail "$name not ready after ${READY_TIMEOUT_SECONDS}s (see $logfile)"
    sleep 0.1
  done
  echo "$pid"
}

start_node() { # <log-name> <ready-marker> <vtop-node args...>
  local name="$1" marker="$2"; shift 2
  start_command "$name" "$marker" "$VTOP_NODE" "$@"
}

start_meta_node() { # <id> <peer-ids...>
  local id="$1"; shift
  local cfg; cfg="$(emit_meta_config "$id" "$@")"
  if [[ -n "${VTOP_META_NETNS_PREFIX:-}" ]]; then
    start_command "meta-$id" "meta_node_ready" \
      ip netns exec "$VTOP_META_NETNS_PREFIX$id" "$VTOP_NODE" meta --config "$cfg"
  else
    start_node "meta-$id" "meta_node_ready" meta --config "$cfg"
  fi
}

start_leader()      { start_node "data-leader" "data_node_ready" data --config "$(emit_leader_config leader)"; }
start_standalone()  { start_node "data-standalone" "data_node_ready" data --config "$(emit_leader_config standalone)"; }
start_follower()    { start_node "data-follower-$1" "data_node_ready" data --config "$(emit_follower_config "$@")"; }

stop_pid() { kill "$1" 2>/dev/null || true; }
kill9_pid() { kill -9 "$1" 2>/dev/null || true; }

stop_node_now() { # <pid>
  local pid="$1" deadline=$((SECONDS + STOP_TIMEOUT_SECONDS))
  kill -9 "$pid" 2>/dev/null || true
  while kill -0 "$pid" 2>/dev/null; do
    local state
    state="$(ps -o stat= -p "$pid" 2>/dev/null || true)"
    [[ "$state" == Z* ]] && return 0
    [[ $SECONDS -lt $deadline ]] || fail "pid $pid did not stop"
    sleep 0.05
  done
}

seal_and_verify_active() { # <label> <active-path>
  local label="$1" active="$2" sealed="${2%.active}.segment"
  [[ -f "$active" ]] || fail "$label active segment missing: $active"
  "$VTOP_NODE" seal-active --path "$active" > "$WORKDIR/logs/verify-$label.log" 2>&1 \
    || fail "$label seal failed: $(tail -3 "$WORKDIR/logs/verify-$label.log")"
  "$VTOPCTL" segment verify "$sealed" --require self >> "$WORKDIR/logs/verify-$label.log" 2>&1 \
    || fail "$label segment verify failed: $(tail -5 "$WORKDIR/logs/verify-$label.log")"
  log "$label sealed artifact passed vtopctl segment verify"
}

# ---------------------------------------------------------------------------
# Admin helpers (vtopctl meta against node <id>)
# ---------------------------------------------------------------------------

meta_admin() { # <node-id> <subcommand + args...>
  local id="$1"; shift
  "$VTOPCTL" --json meta "$@" --config "$(emit_admin_config "$id")"
}

meta_status_field() { # <node-id> <jq-ish python field path>
  local id="$1" field="$2"
  meta_admin "$id" status | python3 -c "
import json,sys
data = json.load(sys.stdin)
value = data$field
print(json.dumps(value) if not isinstance(value, str) else value)
"
}

# Waits until some node in the list reports a leader; echoes the leader id.
wait_meta_leader() { # <node-ids...>
  local deadline=$((SECONDS + ELECTION_TIMEOUT_SECONDS))
  while [[ $SECONDS -lt $deadline ]]; do
    local id
    for id in "$@"; do
      local leader
      leader="$(meta_status_field "$id" "['current_leader']" 2>/dev/null || echo null)"
      if [[ "$leader" != "null" && -n "$leader" ]]; then
        echo "$leader"
        return 0
      fi
    done
    sleep 0.3
  done
  fail "no meta leader elected within ${ELECTION_TIMEOUT_SECONDS}s"
}

# Propose a RegisterNode command through node <id>; used as commit load.
propose_register() { # <via-node-id> <target-uuid-suffix (2 hex)>
  local via="$1" suffix="$2"
  meta_admin "$via" register-node \
    --node-uuid "cccccccc-0000-0000-0000-0000000000$suffix" \
    --addr "$REGISTER_HOST_PREFIX.$((16#$suffix)):$REGISTER_PORT"
}
