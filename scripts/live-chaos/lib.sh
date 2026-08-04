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
# Metadata's UUID for the topic, distinct from the wire-level topic NAME above.
TOPIC_UUID="${CHAOS_TOPIC_UUID:-aaaaaaaa-0000-0000-0000-0000000000b1}"
# Lease pacing for the failover scenario (#223). Short so a scenario does not
# sit through a production-length TTL; safety does not depend on these values.
LEASE_DURATION_MS="${CHAOS_LEASE_DURATION_MS:-6000}"
LEASE_RENEW_MS="${CHAOS_LEASE_RENEW_MS:-2000}"
LEASE_POLL_MS="${CHAOS_LEASE_POLL_MS:-500}"

META_HOST="${CHAOS_META_HOST:-127.0.0.1}"
DATA_HOST="${CHAOS_DATA_HOST:-127.0.0.1}"
META_PEER_BASE_PORT="${CHAOS_META_PEER_BASE_PORT:-9100}"
META_ADMIN_BASE_PORT="${CHAOS_META_ADMIN_BASE_PORT:-9200}"
REPLICA_BASE_PORT="${CHAOS_REPLICA_BASE_PORT:-9300}"
NATIVE_PORT="${CHAOS_NATIVE_PORT:-9400}"
# Observability endpoints (#224). Every node gets one so scenarios can gate on
# /readyz instead of grepping stdout: a ready marker is a one-shot edge, and a
# scenario that needs to know whether a node is STILL ready after a partition or
# a disk fills has nothing to re-grep for.
META_METRICS_BASE_PORT="${CHAOS_META_METRICS_BASE_PORT:-9500}"
DATA_METRICS_BASE_PORT="${CHAOS_DATA_METRICS_BASE_PORT:-9600}"
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
meta_metrics_port() { echo "$((META_METRICS_BASE_PORT + $1))"; }
meta_metrics_addr() { echo "$(meta_host "$1"):$(meta_metrics_port "$1")"; }
# 0 is the leader/standalone; 1 and 2 are the followers.
data_metrics_port() { echo "$((DATA_METRICS_BASE_PORT + $1))"; }
data_metrics_addr() { echo "$DATA_HOST:$(data_metrics_port "$1")"; }
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
  # Health gating (#224) polls each node's /readyz over HTTP.
  require_command curl "Install curl; the harness gates node readiness on the /readyz endpoint."
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
  # Ceilings are the highest port each family actually derives: metadata ids
  # run 1..5 so the top base is 65535-5, and data indices run 0..2 so the top
  # base is 65535-2. A tighter bound would reject a valid override.
  require_integer_in_range CHAOS_META_METRICS_BASE_PORT "$META_METRICS_BASE_PORT" 1024 65530
  require_integer_in_range CHAOS_DATA_METRICS_BASE_PORT "$DATA_METRICS_BASE_PORT" 1024 65533
  require_integer_in_range CHAOS_REGISTER_PORT "$REGISTER_PORT" 1 65535
  require_integer_in_range CHAOS_FENCING_EPOCH "$FENCING_EPOCH" 1 2147483647
  require_integer_in_range CHAOS_READY_TIMEOUT_SECONDS "$READY_TIMEOUT_SECONDS" 1 3600
  require_integer_in_range CHAOS_ELECTION_TIMEOUT_SECONDS "$ELECTION_TIMEOUT_SECONDS" 1 3600
  require_integer_in_range CHAOS_PROGRESS_TIMEOUT_SECONDS "$PROGRESS_TIMEOUT_SECONDS" 1 3600
  require_integer_in_range CHAOS_STOP_TIMEOUT_SECONDS "$STOP_TIMEOUT_SECONDS" 1 3600

  local labels=() endpoints=() id existing
  for id in 1 2 3 4 5; do
    labels+=("meta-peer-$id" "meta-admin-$id" "meta-metrics-$id")
    endpoints+=(
      "$(meta_host "$id"):$(meta_peer_port "$id")"
      "$(meta_host "$id"):$(meta_admin_port "$id")"
      "$(meta_metrics_addr "$id")"
    )
  done
  for id in 0 1 2; do
    labels+=("replica-$id")
    endpoints+=("$(replica_addr "$id")")
  done
  labels+=("native")
  endpoints+=("$(native_addr)")
  for id in 0 1 2; do
    labels+=("data-metrics-$id")
    endpoints+=("$(data_metrics_addr "$id")")
  done
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
    echo "observability: { listen: \"$(meta_metrics_addr "$id")\" }"
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
    # The leader answers the replica-status RPC too, so lag is measured
    # against its boundary rather than the furthest-ahead follower (#224).
    echo "replica_listen: \"$(replica_addr 0)\""
    if [[ "$role" == "leader" ]]; then
      echo "followers:"
      echo "  - { node_uuid: $FOLLOWER1_UUID, addr: \"$(replica_addr 1)\", server_name: \"localhost\" }"
      echo "  - { node_uuid: $FOLLOWER2_UUID, addr: \"$(replica_addr 2)\", server_name: \"localhost\" }"
    fi
    echo "replica_tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-1.pem, key: $CERTS/data-1-key.pem }"
    echo "native_tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-1.pem, key: $CERTS/data-1-key.pem }"
    echo "principal_id: $PRINCIPAL_ID"
    echo "observability: { listen: \"$(data_metrics_addr 0)\" }"
  } > "$cfg"
  echo "$cfg"
}

# emit_lease_yaml <meta-id> — the `lease` block that hands leadership to the
# metadata plane (#223).
emit_lease_yaml() {
  local id="$1"
  echo "lease:"
  echo "  admin_endpoint: \"$(meta_host "$id"):$(meta_admin_port "$id")\""
  echo "  server_name: \"localhost\""
  echo "  topic_uuid: $TOPIC_UUID"
  echo "  tls: { ca: $CERTS/ca.pem, cert: $CERTS/meta-$id.pem, key: $CERTS/meta-$id-key.pem }"
  echo "  lease_duration_ms: $LEASE_DURATION_MS"
  echo "  renew_interval_ms: $LEASE_RENEW_MS"
  echo "  poll_interval_ms: $LEASE_POLL_MS"
}

# emit_follower_config <n: 1|2> [data_dir] [fencing_epoch]
#
# The epoch override exists for the lease-driven scenarios (#223): a follower
# refuses replica appends whose epoch differs from its configured one, and
# followers have no lease agent yet — nothing on a follower watches metadata.
# Until the applied-state watcher lands, the harness stands in for it by
# starting (and, across a failover, restarting) followers at the epoch
# metadata actually granted.
emit_follower_config() {
  local n="$1"
  local dir="${2:-$WORKDIR/data-follower-$n}"
  local epoch="${3:-$FENCING_EPOCH}"
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
    echo "fencing_epoch: $epoch"
    emit_range_yaml
    echo "segment_id: $SEGMENT_ID"
    echo "replica_listen: \"$(replica_addr "$n")\""
    echo "replica_tls: { ca: $CERTS/ca.pem, cert: $CERTS/$cert.pem, key: $CERTS/$cert-key.pem }"
    echo "observability: { listen: \"$(data_metrics_addr "$n")\" }"
  } > "$cfg"
  echo "$cfg"
}

# Config for `vtopctl node status`: the leader plus both followers, so lag is
# measured against the replica whose commit boundary defines the range.
emit_node_status_config() {
  local cfg="$WORKDIR/node-status.yaml"
  {
    emit_range_yaml
    echo "ca_cert: $CERTS/ca.pem"
    echo "client_cert: $CERTS/data-1.pem"
    echo "client_key: $CERTS/data-1-key.pem"
    echo "replicas:"
    echo "  - { node_uuid: $LEADER_UUID, addr: \"$(replica_addr 0)\", server_name: \"localhost\", role: leader }"
    echo "  - { node_uuid: $FOLLOWER1_UUID, addr: \"$(replica_addr 1)\", server_name: \"localhost\" }"
    echo "  - { node_uuid: $FOLLOWER2_UUID, addr: \"$(replica_addr 2)\", server_name: \"localhost\" }"
  } > "$cfg"
  echo "$cfg"
}

# A client config pinned to a specific fencing epoch, for proving that a stale
# epoch is refused.
emit_client_config_at_epoch() {
  local epoch="$1"
  local cfg="$WORKDIR/client-epoch-$epoch.yaml"
  {
    echo "cluster_id: $CLUSTER_ID"
    echo "principal_id: $PRINCIPAL_ID"
    echo "producer_id: $PRODUCER_ID"
    echo "producer_epoch: 1"
    echo "fencing_epoch: $epoch"
    emit_range_yaml
    echo "server_name: \"localhost\""
    echo "tls: { ca: $CERTS/ca.pem, cert: $CERTS/data-1.pem, key: $CERTS/data-1-key.pem }"
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

# ---------------------------------------------------------------------------
# Health gating (#224)
# ---------------------------------------------------------------------------
#
# The ready MARKER on stdout proves a node reached the end of its startup path
# exactly once. The /readyz LEVEL proves it is servable right now, which is a
# different and stronger claim: a marker cannot go back to false when a leader
# is fenced, a partition heals, or a disk fills. Scenarios that care about the
# current state ask the endpoint.

# probe_readyz <addr> — echoes the HTTP status; 000 when unreachable.
#
# curl already writes `000` and exits non-zero when it cannot connect, so an
# `|| echo 000` fallback would concatenate a second one and yield `000000` —
# which matches no comparison and silently turns every "wait until it stops
# answering" into a timeout. Take curl's own output and normalise anything that
# is not three digits.
probe_readyz() {
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "http://$1/readyz" 2>/dev/null)"
  case "$code" in
    [0-9][0-9][0-9]) echo "$code" ;;
    *) echo "000" ;;
  esac
}

# await_ready <addr> <what> [timeout-seconds] — block until /readyz is 200.
await_ready() {
  local addr="$1" what="$2" limit="${3:-$READY_TIMEOUT_SECONDS}"
  local deadline=$((SECONDS + limit)) code
  while :; do
    code="$(probe_readyz "$addr")"
    [[ "$code" == "200" ]] && return 0
    [[ $SECONDS -lt $deadline ]] || break
    sleep 0.1
  done
  # Serve the reason, not just the code: /readyz answers "not ready: <why>",
  # which is the whole point of the level carrying a reason.
  local body
  body="$(curl -s --max-time 2 "http://$addr/readyz" 2>/dev/null || echo '<unreachable>')"
  fail "$what not ready after ${limit}s (http $code): $body"
}

# await_not_ready <addr> <what> [timeout-seconds] — block until /readyz is NOT
# 200. Used where a scenario asserts that a node correctly withdrew itself, e.g.
# a leaseholder that metadata has fenced.
await_not_ready() {
  local addr="$1" what="$2" limit="${3:-$READY_TIMEOUT_SECONDS}"
  local deadline=$((SECONDS + limit)) code
  while :; do
    code="$(probe_readyz "$addr")"
    [[ "$code" != "200" ]] && return 0
    [[ $SECONDS -lt $deadline ]] || break
    sleep 0.1
  done
  fail "$what still reports ready after ${limit}s; it should have withdrawn"
}

# metric_value <addr> <metric-name> — the value of a single unlabelled sample,
# or of the first sample when the metric carries labels. Prometheus text is
# `name{labels} value`, so the value is always the last field.
metric_value() {
  local addr="$1" metric="$2"
  curl -s --max-time 5 "http://$addr/metrics" 2>/dev/null \
    | grep -m1 "^$metric" \
    | awk '{print $NF}'
}

# count_raft_leaders <meta-ids...> — how many nodes report themselves leader.
# Anything but 1 is an incident: 0 is an outage, 2 is split brain.
count_raft_leaders() {
  local id total=0 value
  for id in "$@"; do
    value="$(curl -s --max-time 5 "http://$(meta_metrics_addr "$id")/metrics" 2>/dev/null \
      | awk -F' ' '/^vtop_meta_raft_state\{state="leader"\}/ {print $NF}')"
    [[ "$value" == "1" ]] && total=$((total + 1))
  done
  echo "$total"
}

# await_replicas_settled <expected-offset> <config> — block until every replica
# reports the offset, then return.
#
# Quorum produce acknowledges once the leader plus a MAJORITY of followers are
# durable, so a third replica can legitimately still be catching up the instant
# produce returns. Asserting zero lag immediately would fail a run that
# satisfied quorum durability exactly as designed. Waiting for the tail to
# settle is the assertion that is actually true.
await_replicas_settled() {
  local expected="$1" cfg="$2"
  local deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS)) lags
  while :; do
    if "$VTOPCTL" node status --config "$cfg" --json > "$WORKDIR/node-status.json" 2>&1; then
      lags="$(python3 -c "
import json
d=json.load(open('$WORKDIR/node-status.json'))
print(','.join(str(r['lag_records']) for r in d['replicas']))
")"
      [[ "$lags" == "0,0,0" ]] && return 0
    fi
    [[ $SECONDS -lt $deadline ]] \
      || fail "replicas did not all reach offset $expected within ${PROGRESS_TIMEOUT_SECONDS}s (lags: ${lags:-unknown})"
    sleep 0.2
  done
}

# ---------------------------------------------------------------------------
# Range leases (#223)
# ---------------------------------------------------------------------------

# lease_field <meta-id> <jq-ish python path> — read one field of the range lease
# through the linearizable read.
lease_field() { # <meta-id> <python expression over `d`>
  local id="$1" expr="$2"
  "$VTOPCTL" --json meta range-lease --config "$(emit_admin_config "$id")" \
    --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" 2>/dev/null \
    | python3 -c "
import json,sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
print($expr)
" 2>/dev/null
}

# await_lease_holder <meta-id> <expected-holder-uuid> [timeout] — block until
# metadata reports the range held by that node; echoes the fencing epoch.
#
# This is the failover assertion: it is what proves a follower actually took
# the range rather than the range simply stopping.
await_lease_holder() {
  local id="$1" expected="$2" limit="${3:-$ELECTION_TIMEOUT_SECONDS}"
  local deadline=$((SECONDS + limit)) holder epoch
  while :; do
    # A transient failed read — a metadata group mid-election refuses the
    # linearizable read — consumes its share of the timeout rather than
    # aborting the scenario under `set -e`.
    holder="$(lease_field "$id" "d['lease']['holder_node_uuid'] if d.get('lease') else ''")" \
      || holder=""
    if [[ "$holder" == "$expected" ]]; then
      epoch="$(lease_field "$id" "d['lease']['fencing_epoch']")" || epoch=""
      if [[ -n "$epoch" ]]; then
        echo "$epoch"
        return 0
      fi
    fi
    [[ $SECONDS -lt $deadline ]] \
      || fail "range not held by $expected after ${limit}s (holder: ${holder:-none})"
    sleep 0.2
  done
}

# assert_fenced_produce <addr> <stale-epoch> <message> — a produce presenting a
# superseded epoch must be refused BECAUSE it is fenced.
#
# Quorum durability, not local-fsync: a replicated leader rejects local-fsync
# as an invalid durability before it ever reaches the epoch check, so that
# refusal would prove nothing. And the failure must name fencing — accepting
# any failed produce would let a connection refused or a TLS mismatch pass as
# a fencing proof.
assert_fenced_produce() {
  local addr="$1" epoch="$2" message="$3"
  if "$VTOP_NODE" produce --client-config "$(emit_client_config_at_epoch "$epoch")" \
    --addr "$addr" --records 1 --batch 1 --durability quorum \
    > "$WORKDIR/logs/fenced-produce.log" 2>&1; then
    fail "$message"
  fi
  grep -qi "fenced" "$WORKDIR/logs/fenced-produce.log" \
    || fail "produce failed for a reason other than fencing (see \
$WORKDIR/logs/fenced-produce.log): $message"
}

# await_acked_floor <acked-file> <floor> — block until the producer's persisted
# acknowledged floor reaches <floor>. The producer rewrites the file after
# every acknowledged batch, so this is how a scenario knows a kill will land
# mid-flight rather than after the fact.
await_acked_floor() {
  local file="$1" floor="$2"
  local deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS)) acked
  while :; do
    acked="$(cat "$file" 2>/dev/null)" || acked=""
    [[ -n "$acked" && "$acked" -ge "$floor" ]] && return 0
    [[ $SECONDS -lt $deadline ]] \
      || fail "acknowledged floor ${acked:-0} never reached $floor within ${PROGRESS_TIMEOUT_SECONDS}s"
    sleep 0.1
  done
}

# follower_committed_offset <n> — a follower's durable commit boundary, from
# its own metrics endpoint. Used after a leader kill to promote the replica
# that actually holds the acknowledged floor: quorum produce only guarantees
# the floor reached SOME majority, not any one particular follower.
follower_committed_offset() {
  local n="$1" value
  value="$(metric_value "$(data_metrics_addr "$n")" vtop_broker_local_committed_offset)" \
    || value=""
  echo "${value:-0}"
}

# await_verified_floor <client-cfg> <addr> <floor> — retry verify until the
# acknowledged floor is readable. Immediately after a failover the boundary a
# quorum could PROVE may sit below the floor until the new leader's replication
# stream catches the lagging follower up; the eventual assertion is the one
# that is actually promised.
await_verified_floor() {
  local cfg="$1" addr="$2" floor="$3"
  local deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
  while :; do
    if "$VTOP_NODE" verify --client-config "$cfg" --addr "$addr" \
      --expect-at-least "$floor" > "$WORKDIR/logs/verify-after-failover.log" 2>&1; then
      return 0
    fi
    [[ $SECONDS -lt $deadline ]] \
      || fail "post-failover verify never covered the $floor acknowledged records \
(see $WORKDIR/logs/verify-after-failover.log)"
    sleep 0.5
  done
}

# assert_metric_present <addr> <metric-name> — the scrape contract, checked
# live. A renamed metric silently blanks a dashboard panel while the node keeps
# working perfectly, so the harness pins the names it publishes.
assert_metric_present() {
  local addr="$1" metric="$2" body
  body="$(curl -s --max-time 5 "http://$addr/metrics" 2>/dev/null || true)"
  grep -q "^$metric" <<< "$body" \
    || fail "$metric is missing from http://$addr/metrics; a renamed metric blanks its dashboard panel"
}

start_node() { # <log-name> <ready-marker> <vtop-node args...>
  local name="$1" marker="$2"; shift 2
  start_command "$name" "$marker" "$VTOP_NODE" "$@"
}

start_meta_node() { # <id> <peer-ids...>
  local id="$1" pid; shift
  local cfg; cfg="$(emit_meta_config "$id" "$@")"
  if [[ -n "${VTOP_META_NETNS_PREFIX:-}" ]]; then
    pid="$(start_command "meta-$id" "meta_node_ready" \
      ip netns exec "$VTOP_META_NETNS_PREFIX$id" "$VTOP_NODE" meta --config "$cfg")"
  else
    pid="$(start_node "meta-$id" "meta_node_ready" meta --config "$cfg")"
  fi
  # A node inside its own network namespace is not reachable from here, so the
  # marker remains the only available signal for those. Everywhere else the
  # health gate is authoritative.
  if [[ -z "${VTOP_META_NETNS_PREFIX:-}" ]]; then
    await_ready "$(meta_metrics_addr "$id")" "meta-$id"
  fi
  echo "$pid"
}

start_leader() {
  local pid; pid="$(start_node "data-leader" "data_node_ready" data --config "$(emit_leader_config leader)")"
  await_ready "$(data_metrics_addr 0)" "data-leader"
  echo "$pid"
}

# emit_leader_config_with_lease <meta-id> [label] — a leader whose fencing epoch
# is metadata's to decide (#223), rather than a fixed configured value.
#
# The configured epoch is rewritten to 0: it is only the monotonic FLOOR of the
# broker's epoch view, and metadata mints epochs from 1. Keeping the harness's
# fixed epoch (18) would sit the floor above every real grant, so the broker
# would ignore them all and keep serving the static epoch — the scenario would
# then pass without metadata-driven fencing ever being exercised.
emit_leader_config_with_lease() {
  local id="$1" label="${2:-lease}"
  local cfg="$WORKDIR/data-leader-$label.yaml"
  emit_leader_config leader > /dev/null
  {
    sed 's/^fencing_epoch: .*/fencing_epoch: 0/' "$WORKDIR/data-leader-leader.yaml"
    emit_lease_yaml "$id"
  } > "$cfg"
  echo "$cfg"
}

# start_leader_with_lease <meta-id> [label]
start_leader_with_lease() {
  local id="$1" label="${2:-lease}" pid cfg
  cfg="$(emit_leader_config_with_lease "$id" "$label")"
  pid="$(start_node "data-leader-$label" "data_node_ready" data --config "$cfg")"
  await_ready "$(data_metrics_addr 0)" "data-leader-$label"
  echo "$pid"
}

# start_promoted_follower <n: 1|2> <meta-id> — restart follower `n` as a
# lease-driven leader over the data directory it already replicated into.
#
# This is the failover path: the replica that has the data becomes the one that
# serves it, and its lease agent must win the range from the dead leader. Its
# identity, certificate, and remaining-follower list are all derived from `n`;
# the original follower process must already be stopped, or two brokers would
# hold the same active segment.
start_promoted_follower() {
  local n="$1" id="$2" pid cfg uuid cert other other_uuid
  case "$n" in
    1) uuid="$FOLLOWER1_UUID"; cert="data-2"; other=2; other_uuid="$FOLLOWER2_UUID" ;;
    2) uuid="$FOLLOWER2_UUID"; cert="data-3"; other=1; other_uuid="$FOLLOWER1_UUID" ;;
    *) fail "start_promoted_follower supports followers 1 and 2, not '$n'" ;;
  esac
  cfg="$WORKDIR/data-promoted-$n.yaml"
  {
    # Same range and segment, but now serving the native port and driven by a
    # lease rather than a fixed epoch. Epoch floor 0 for the same reason as
    # emit_leader_config_with_lease: metadata's grants must not be ignored.
    echo "role: leader"
    echo "node_uuid: $uuid"
    echo "cluster_id: $CLUSTER_ID"
    echo "data_dir: $WORKDIR/data-follower-$n"
    echo "fencing_epoch: 0"
    emit_range_yaml
    echo "segment_id: $SEGMENT_ID"
    echo "native_listen: \"$(native_addr)\""
    echo "replica_listen: \"$(replica_addr 0)\""
    echo "followers:"
    echo "  - { node_uuid: $other_uuid, addr: \"$(replica_addr "$other")\", server_name: \"localhost\" }"
    echo "replica_tls: { ca: $CERTS/ca.pem, cert: $CERTS/$cert.pem, key: $CERTS/$cert-key.pem }"
    echo "native_tls: { ca: $CERTS/ca.pem, cert: $CERTS/$cert.pem, key: $CERTS/$cert-key.pem }"
    echo "principal_id: $PRINCIPAL_ID"
    echo "observability: { listen: \"$(data_metrics_addr 0)\" }"
    emit_lease_yaml "$id"
  } > "$cfg"
  pid="$(start_node "data-promoted-$n" "data_node_ready" data --config "$cfg")"
  echo "$pid"
}

# start_fenced_old_leader <meta-id> <old-epoch> — restart the dead leader
# against its old data directory, on DISTINCT ports, and do NOT gate on
# /readyz.
#
# Distinct ports because the promoted follower now owns the native and
# replica-0 addresses; reusing them would kill the restarted process on a bind
# error while `await_ready` happily polled the promoted follower's endpoint —
# the assertion would then be exercising the wrong process entirely. And no
# /readyz gate because this node must NEVER become ready: metadata holds a
# live lease for its rival, so its lease agent keeps the broker fenced. The
# caller asserts exactly that.
#
# The held epoch is seeded with the OLD grant, exactly as a restarted operator
# process would carry it — not 0. With 0, the stale-epoch produce would be
# refused by a trivial epoch mismatch before the lease machinery ever ran; the
# caller must instead wait for this node's metadata view to reflect the rival
# grant (await_metric_at_least on vtop_broker_meta_fencing_epoch) so the
# refusal it asserts is the one fencing actually provides.
start_fenced_old_leader() {
  local id="$1" old_epoch="$2" pid
  local cfg="$WORKDIR/data-leader-restarted.yaml"
  {
    sed -e "s/^fencing_epoch: .*/fencing_epoch: $old_epoch/" \
        -e "s|^native_listen: .*|native_listen: \"$(old_leader_native_addr)\"|" \
        -e "s|^replica_listen: .*|replica_listen: \"$(replica_addr 3)\"|" \
        -e "s|^observability: .*|observability: { listen: \"$(data_metrics_addr 3)\" }|" \
        "$WORKDIR/data-leader-leader.yaml"
    emit_lease_yaml "$id"
  } > "$cfg"
  pid="$(start_node "data-leader-restarted" "data_node_ready" data --config "$cfg")"
  echo "$pid"
}

# await_metric_at_least <addr> <metric> <floor> <what> — block until a scraped
# gauge reaches <floor>.
await_metric_at_least() {
  local addr="$1" metric="$2" floor="$3" what="$4"
  local deadline=$((SECONDS + PROGRESS_TIMEOUT_SECONDS)) value
  while :; do
    value="$(metric_value "$addr" "$metric")" || value=""
    if [[ -n "$value" && "${value%.*}" -ge "$floor" ]]; then
      return 0
    fi
    [[ $SECONDS -lt $deadline ]] \
      || fail "$what: $metric stayed at '${value:-absent}' (< $floor) for ${PROGRESS_TIMEOUT_SECONDS}s"
    sleep 0.2
  done
}

# The restarted old leader's native address: one past the range's real port.
old_leader_native_addr() { echo "$DATA_HOST:$((NATIVE_PORT + 1))"; }

start_standalone() {
  local pid; pid="$(start_node "data-standalone" "data_node_ready" data --config "$(emit_leader_config standalone)")"
  await_ready "$(data_metrics_addr 0)" "data-standalone"
  echo "$pid"
}

# emit_colocated_config <meta-id> [peer-ids...] — one process, both planes
# (#215): a metadata voter and a standalone data replica composed from the
# same emitters the split-process scenarios use, under ONE observability
# endpoint. Per-role observability blocks are stripped because the co-located
# runner rejects them by design.
# The colocated node <id>'s native produce/fetch address. One per host: two
# co-located processes must not fight over one listener.
colocated_native_addr() { echo "$DATA_HOST:$((NATIVE_PORT + $1 - 1))"; }

emit_colocated_config() {
  local id="$1"; shift
  local cfg="$WORKDIR/colocated-$id.yaml"
  local uuid cert
  # Identity, directory, and ports all derive from the id: a second
  # co-located host must not reuse the first one's segment directory or bind
  # its listeners.
  case "$id" in
    1) uuid="$LEADER_UUID"; cert="data-1" ;;
    2) uuid="$FOLLOWER1_UUID"; cert="data-2" ;;
    3) uuid="$FOLLOWER2_UUID"; cert="data-3" ;;
    *) fail "colocated node ids 1-3 are supported, not '$id'" ;;
  esac
  emit_meta_config "$id" "$@" > /dev/null
  # `peers:` with NO entries is YAML null, which the typed config refuses, so
  # the bare key is dropped only then; with entries it must stay, or the list
  # items underneath would be orphaned and the YAML would not parse at all.
  local strip_peers=()
  [[ $# -eq 0 ]] && strip_peers=('-e' '/^peers:$/d')
  {
    echo "meta:"
    sed -e '/^observability:/d' "${strip_peers[@]}" -e 's/^/  /' "$WORKDIR/meta-$id.yaml"
    echo "data:"
    # Each host carries an independent STANDALONE range for now: replicated
    # ranges under co-location wait on follower-side epoch propagation (the
    # lease watcher tracked as follow-up to #223).
    echo "  role: standalone"
    echo "  node_uuid: $uuid"
    echo "  cluster_id: $CLUSTER_ID"
    echo "  data_dir: $WORKDIR/data-colocated-$id"
    echo "  fencing_epoch: $FENCING_EPOCH"
    echo "  $(emit_range_yaml)"
    echo "  segment_id: $SEGMENT_ID"
    echo "  native_listen: \"$(colocated_native_addr "$id")\""
    echo "  replica_listen: \"$(replica_addr $((id - 1)))\""
    echo "  replica_tls: { ca: $CERTS/ca.pem, cert: $CERTS/$cert.pem, key: $CERTS/$cert-key.pem }"
    echo "  native_tls: { ca: $CERTS/ca.pem, cert: $CERTS/$cert.pem, key: $CERTS/$cert-key.pem }"
    echo "  principal_id: $PRINCIPAL_ID"
    echo "observability: { listen: \"$(data_metrics_addr $((id - 1)))\" }"
  } > "$cfg"
  echo "$cfg"
}

# start_colocated_node <meta-id> [peer-ids...] — the six-to-three process
# change, exercised for real: `vtop-node node` hosting both roles.
#
# Readiness is deliberately NOT awaited here: the shared /readyz is the
# conjunction of both roles, and asserting when it opens (and when it must
# not yet be open) is the scenario's job.
start_colocated_node() {
  local id="$1" pid; shift
  pid="$(start_node "colocated-$id" "colocated_node_starting" node \
    --config "$(emit_colocated_config "$id" "$@")")"
  echo "$pid"
}

start_follower() {
  local n="$1" pid
  pid="$(start_node "data-follower-$n" "data_node_ready" data --config "$(emit_follower_config "$@")")"
  await_ready "$(data_metrics_addr "$n")" "data-follower-$n"
  echo "$pid"
}

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
