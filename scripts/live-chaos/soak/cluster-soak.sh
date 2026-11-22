#!/usr/bin/env bash
# The cluster-plane soak (#727, criterion A3 of #714).
#
#   scripts/live-chaos/soak/cluster-soak.sh PROFILE RUN_DIR
#
# Three metadata nodes and three data nodes (candidates, as scenario 14) from
# the live-chaos library, under a producer that never stops and a reader that
# reads the range every `read_interval_s`, with one fault every
# `fault_interval_s` from the profile's cycle:
#
#   range_leader_kill  SIGKILL the range's leaseholder, restart it after the hold
#   meta_leader_kill   SIGKILL the metadata Raft leader, restart it after the hold
#   partition          cut one node off the network (its own namespace's
#                      interface drops everything) for the hold: the range
#                      leader, the metadata leader and a range follower in turn
#   pause              SIGSTOP the range leader, then the metadata leader, in
#                      turn, and SIGCONT it after the hold
#   disk_full          fill a data node's disk (each data directory is its own
#                      tmpfs) to the last byte: a follower, then the leader, in
#                      turn, and free it after the hold
#   clock_step         restart the range leader, then the metadata leader, in
#                      turn, under scenario 07's clock shim stepped by the
#                      profile's next `clock_step_seconds`; restart it on the
#                      true clock after the hold
#
# Every node runs in its own network namespace on one bridge, so a partition
# is a real one. The range has retention on (`retention_bytes`), so the disk
# a data node uses is bounded by design and the soak judges that it is.
#
# What the run records, in RUN_DIR:
#   profile.json      the profile the run used
#   events.jsonl      run_start, node_start, node_exit, fault_start, fault_end,
#                     recovered, produce_session, read, producer_stopped,
#                     run_end (cluster_soak.py event)
#   samples.jsonl     every `sample_interval_s`, per node: up, lifetime, memory,
#                     open files, data directory size, and its gauges
#   history/          the operation history (#728): every produced batch and
#                     every read
#   leaders.jsonl     every `role=leader` a data node logged, with its epoch
#   finals.json       after the load stops: `vtopctl segment verify` on every
#                     segment of every replica
#   history-check.json, verdict.json, verdict.txt: the checker's and the
#                     evaluator's answers
#   work/             the nodes' configs, logs and data
#
# The verdict is cluster_soak_eval.py over those files; re-run it over an
# archived run directory to get the same answer. Exit status: 0 the verdict
# passed, 1 it failed, 2 the run could not start.
#
# Needs Linux with unprivileged user, mount and network namespaces (as live-chaos
# scenarios 05 and 06 do), iptables, gcc (the clock shim), python3, and the
# harness binaries (see run-all.sh). See docs/CLUSTER_SOAK_VALIDATION.md.
SCENARIO=cluster-soak
# shellcheck source=../lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"
SOAK_DIR="$SCRIPT_DIR/soak"
SOAK_PY=(python3 "$SOAK_DIR/cluster_soak.py")

PROFILE_ARG="${1:-}"
RUN_DIR="${2:-}"
[[ -n "$PROFILE_ARG" && -n "$RUN_DIR" ]] || { echo "usage: $0 PROFILE RUN_DIR" >&2; exit 2; }

if [[ "${VTOP_SOAK_INNER:-0}" != "1" ]]; then
  # The outer half: check what can be checked before the namespace, run the
  # soak inside one, then judge what it recorded whatever became of it.
  "${SOAK_PY[@]}" field "$PROFILE_ARG" name > /dev/null || exit 2
  mkdir -p "$RUN_DIR" || exit 2
  RUN_DIR="$(cd "$RUN_DIR" && pwd)"
  [[ ! -e "$RUN_DIR/events.jsonl" ]] \
    || { echo "$RUN_DIR already holds a run; give each run its own directory" >&2; exit 2; }
  require_binaries
  require_network_namespace
  WORKDIR="$RUN_DIR" require_mount_namespace
  require_command gcc "Install GCC; the clock step builds scenario 07's shim."
  code=0
  unshare -Urnm env VTOP_SOAK_INNER=1 bash "$0" "$PROFILE_ARG" "$RUN_DIR" || code=$?
  log "the soak's driver exited with status $code; judging what it recorded"
  # From the logs, which outlive the run's namespace, so a run that ended
  # early is judged on the leaderships it did log.
  if [[ -d "$RUN_DIR/work/logs" ]]; then
    "${SOAK_PY[@]}" leaders "$RUN_DIR/work/logs" > "$RUN_DIR/leaders.jsonl" || true
  fi
  histories=("$RUN_DIR"/history/*.jsonl)
  if [[ -e "${histories[0]}" ]]; then
    python3 "$SCRIPT_DIR/history/check_history.py" "${histories[@]}" \
      > "$RUN_DIR/history-check.json" 2> "$RUN_DIR/history-check.stderr" || true
  fi
  verdict=0
  python3 "$SOAK_DIR/cluster_soak_eval.py" --profile "$RUN_DIR/profile.json" --run-dir "$RUN_DIR" \
    > "$RUN_DIR/verdict.txt" || verdict=$?
  cat "$RUN_DIR/verdict.txt"
  # The trap would print PASS or FAILED for the outer shell; the verdict is
  # what this run says.
  trap - EXIT
  case "$verdict" in
    0) exit 0 ;;
    1) exit 1 ;;
    *) exit 2 ;;
  esac
fi

# --- inside the user, mount and network namespace ------------------------------
export CHAOS_WORKDIR="$RUN_DIR/work"
export CHAOS_LEASE_ADMIN_PEERS=1
export CHAOS_SEGMENT_FORMAT=v2
# A restart recovers a log that has run for days, and a debug build is slow at
# it: the ready marker gets longer than a scenario's 20 s.
READY_TIMEOUT_SECONDS="${CHAOS_READY_TIMEOUT_SECONDS:-300}"
STOP_TIMEOUT_SECONDS="${CHAOS_STOP_TIMEOUT_SECONDS:-60}"

cp "$PROFILE_ARG" "$RUN_DIR/profile.json" 2> /dev/null \
  || cp "$SOAK_DIR/profiles/$PROFILE_ARG.json" "$RUN_DIR/profile.json"
PROFILE="$RUN_DIR/profile.json"
field() { "${SOAK_PY[@]}" field "$PROFILE" "$1"; }
DURATION="$(field duration_s)"
RATE="$(field rate_records_per_s)"
BATCH="$(field batch)"
VALUE_BYTES="$(field value_bytes)"
SAMPLE_INTERVAL="$(field sample_interval_s)"
READ_INTERVAL="$(field read_interval_s)"
RECOVERY_TIMEOUT="$(field recovery_timeout_s)"
# One read of the retained range takes seconds; a read that has not finished
# in this long is stuck, not slow.
READ_TIMEOUT="${CHAOS_SOAK_READ_TIMEOUT_SECONDS:-300}"
# What `verify` says when the leader served something the log forbids
# (crates/vtop-node/src/client.rs): a record at or above the committed high
# watermark, offsets going backwards, the watermark going back mid-read.
READ_INTEGRITY_ERRORS="exposed at or above committed high watermark|offset went backwards|committed high watermark regressed|passed the committed high watermark"
TMPFS_MIB="$(field data_tmpfs_mib)"
RETENTION_BYTES="$(field retention_bytes)"
export CHAOS_MAX_SEGMENT_BYTES CHAOS_MAX_GROUP_BYTES CHAOS_MAX_RECORD_BYTES
CHAOS_MAX_SEGMENT_BYTES="$(field max_segment_bytes)"
# A group must fit in a segment and a record in a group, with headroom (the
# node refuses equal sizes), so small segments take smaller groups.
CHAOS_MAX_GROUP_BYTES=$((CHAOS_MAX_SEGMENT_BYTES / 4))
CHAOS_MAX_RECORD_BYTES=$((CHAOS_MAX_GROUP_BYTES / 4))

python3 -c 'import time; print(repr(time.monotonic()))' > "$RUN_DIR/origin"
event() { "${SOAK_PY[@]}" event "$RUN_DIR" "$@" || true; }
json_string() { python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1"; }
elapsed() { python3 -c "import time; print(int(time.monotonic() - $(cat "$RUN_DIR/origin")))"; }

require_binaries
init_workdir
mkdir -p "$WORKDIR/nodes" "$RUN_DIR/history"
PRODUCE_HISTORY="$RUN_DIR/history/produce.jsonl"
READ_HISTORY="$RUN_DIR/history/reads.jsonl"

# --- the network: one bridge, every node in its own namespace -----------------
NET="${CHAOS_SOAK_NET_PREFIX:-10.216.0}"
mount -t tmpfs tmpfs /run
mkdir -p /run/netns
ip link set lo up
ip link add vtop-br0 type bridge
ip addr add "$NET.254/24" dev vtop-br0
ip link set vtop-br0 up
make_netns() { # <namespace> <address> <host-side veth name>
  ip netns add "$1"
  ip link add "$3" type veth peer name eth0 netns "$1"
  ip link set "$3" master vtop-br0
  ip link set "$3" up
  ip netns exec "$1" ip link set lo up
  ip netns exec "$1" ip addr add "$2/24" dev eth0
  ip netns exec "$1" ip link set eth0 up
}
export VTOP_META_HOST_PREFIX="$NET" VTOP_META_NETNS_PREFIX="vtop-meta-"
for id in 1 2 3; do make_netns "vtop-meta-$id" "$NET.$id" "vtop-vm$id"; done
for n in 1 2 3; do make_netns "vtop-data-$n" "$NET.$((10 + n))" "vtop-vd$n"; done

# Data node n (1..3) answers at $NET.(10+n). The library's address helpers
# take the indices it uses (replica index n-1, metrics index n+10); anything
# else stays where the library puts it.
data_host() { echo "$NET.$((10 + $1))"; }
replica_addr() {
  if (( $1 >= 0 && $1 <= 2 )); then echo "$(data_host $(($1 + 1))):$(replica_port "$1")"
  else echo "$DATA_HOST:$(replica_port "$1")"; fi
}
candidate_native_addr() { echo "$(data_host "$1"):$((NATIVE_PORT + 10 + $1))"; }
data_metrics_addr() {
  if (( $1 >= 11 && $1 <= 13 )); then echo "$(data_host $(($1 - 10))):$(data_metrics_port "$1")"
  else echo "$DATA_HOST:$(data_metrics_port "$1")"; fi
}

# --- nodes, each lifetime with its own log --------------------------------------
# nodes/<name>.pid is the running process, nodes/<name>.life counts its
# lifetimes (the sampler reads both), and nodes/<name>.down says the run took
# it down on purpose.
next_life() { echo $(( $(cat "$WORKDIR/nodes/$1.life" 2> /dev/null || echo 0) + 1 )); }
node_pid() { cat "$WORKDIR/nodes/$1.pid" 2> /dev/null || true; }
node_started() { # <name> <pid> <life>
  rm -f "$WORKDIR/nodes/$1.down"
  echo "$2" > "$WORKDIR/nodes/$1.pid"
  echo "$3" > "$WORKDIR/nodes/$1.life"
  event node_start "node=$1" "pid=$2" "lifetime=$3"
}

start_soak_meta() { # <id> [env assignments...]
  local id="$1" life pid peers=() p; shift
  for p in 1 2 3; do [[ "$p" != "$id" ]] && peers+=("$p"); done
  life="$(next_life "meta-$id")"
  # The environment (the clock shim) goes to the node alone, inside the
  # namespace: on `ip` itself, the shim would write its marker from `ip` even
  # when the node cannot load it (review of #881).
  pid="$(start_command "meta-$id-$life" "meta_node_ready" \
    ip netns exec "vtop-meta-$id" env "$@" "$VTOP_NODE" meta --config "$(emit_meta_config "$id" "${peers[@]}")")" \
    || return 1
  node_started "meta-$id" "$pid" "$life"
}

emit_soak_candidate_config() { # <n>
  local n="$1" cfg
  cfg="$(emit_candidate_config "$n" 1)"
  # The data directory on the node's own tmpfs, and retention on.
  sed -i "s#^data_dir: .*#data_dir: $WORKDIR/disk-$n/data#" "$cfg"
  echo "retention: { max_total_bytes: $RETENTION_BYTES }" >> "$cfg"
  echo "$cfg"
}

start_soak_candidate() { # <n> [env assignments...]
  local n="$1" life pid; shift
  life="$(next_life "data-$n")"
  pid="$(start_command "data-candidate-$n-$life" "data_node_ready" \
    ip netns exec "vtop-data-$n" env "$@" "$VTOP_NODE" data --config "$(emit_soak_candidate_config "$n")")" \
    || return 1
  node_started "data-$n" "$pid" "$life"
}

start_node_named() { # <name> [env assignments...]
  local name="$1"; shift
  case "$name" in
    meta-*) start_soak_meta "${name#meta-}" "$@" ;;
    data-*) start_soak_candidate "${name#data-}" "$@" ;;
  esac
}

# stop_node_named <name> <signal> — stop it on purpose and wait for it to go.
stop_node_named() {
  local name="$1" signal="$2" pid deadline=$((SECONDS + STOP_TIMEOUT_SECONDS))
  pid="$(node_pid "$name")"
  touch "$WORKDIR/nodes/$name.down"
  rm -f "$WORKDIR/nodes/$name.pid"
  [[ -n "$pid" ]] || return 0
  kill "-$signal" "$pid" 2> /dev/null || true
  kill -CONT "$pid" 2> /dev/null || true
  while kill -0 "$pid" 2> /dev/null && [[ "$(ps -o stat= -p "$pid" 2> /dev/null)" != Z* ]]; do
    if (( SECONDS >= deadline )); then kill -9 "$pid" 2> /dev/null || true; fi
    sleep 0.1
  done
  event node_exit "node=$name" "pid=$pid" planned=true "signal=\"$signal\""
}

ALL_NODES=(meta-1 meta-2 meta-3 data-1 data-2 data-3)

# watch_nodes — a node that is not down on purpose and has gone is an
# unplanned exit: record it and start it again (every 30 s until it starts),
# so one finding does not end the run. Called once a second while the run
# waits.
watch_nodes() {
  local name pid retry
  for name in "${ALL_NODES[@]}"; do
    [[ -e "$WORKDIR/nodes/$name.down" ]] && continue
    pid="$(node_pid "$name")"
    if [[ -n "$pid" ]]; then
      kill -0 "$pid" 2> /dev/null && [[ "$(ps -o stat= -p "$pid" 2> /dev/null)" != Z* ]] && continue
      rm -f "$WORKDIR/nodes/$name.pid"
      event node_exit "node=$name" "pid=$pid" planned=false
      log "UNPLANNED: $name (pid $pid) exited; starting it again"
    fi
    retry="$(cat "$WORKDIR/nodes/$name.retry" 2> /dev/null || echo 0)"
    (( SECONDS >= retry )) || continue
    echo $((SECONDS + 30)) > "$WORKDIR/nodes/$name.retry"
    start_node_named "$name" 2> /dev/null || event node_restart_failed "node=$name"
  done
}

# soak_sleep <seconds> — wait, watching the nodes.
soak_sleep() {
  local until=$((SECONDS + $1))
  while (( SECONDS < until )); do watch_nodes; sleep 1; done
}

# --- who leads -------------------------------------------------------------------
# lib.sh's lease_field, bounded (review of #881): the admin client bounds its
# connect but not the answer, so a metadata node that accepts and never
# answers (paused, wedged) would hold the driver, which also runs the faults,
# and keep it from trying the other two nodes.
LEASE_READ_TIMEOUT="${CHAOS_SOAK_LEASE_READ_TIMEOUT_SECONDS:-10}"
lease_field() { # <meta-id> <python expression over `d`>
  local id="$1" expr="$2"
  timeout --kill-after=2 "$LEASE_READ_TIMEOUT" \
    "$VTOPCTL" --json meta range-lease --config "$(emit_admin_config "$id")" \
    --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" 2> /dev/null \
    | python3 -c "
import json,sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
print($expr)
" 2> /dev/null
}
# current_lease — "<holder-uuid> <epoch>" from whichever metadata node answers.
current_lease() {
  local m holder epoch
  for m in 1 2 3; do
    holder="$(lease_field "$m" "d['lease']['holder_node_uuid'] if d.get('lease') else ''")" || continue
    [[ -n "$holder" ]] || continue
    epoch="$(lease_field "$m" "d['lease']['fencing_epoch']")" || continue
    echo "$holder $epoch"
    return 0
  done
  return 1
}
range_leader() { # echoes the data node ordinal holding the range
  local holder epoch
  read -r holder epoch < <(current_lease) || return 1
  [[ -n "$holder" ]] && candidate_by_uuid "$holder"
}
meta_leader() { # echoes the metadata node that says it leads
  local id value
  for id in 1 2 3; do
    value="$(curl -s --max-time 3 "http://$(meta_metrics_addr "$id")/metrics" 2> /dev/null \
      | awk '/^vtop_meta_raft_state\{state="leader"\}/ {print $NF}')"
    [[ "$value" == "1" ]] && { echo "$id"; return 0; }
  done
  return 1
}
await_range_leader() {
  local deadline=$((SECONDS + RECOVERY_TIMEOUT)) n
  until n="$(range_leader 2> /dev/null)" && [[ -n "$n" ]]; do
    (( SECONDS < deadline )) || return 1
    sleep 1
  done
  echo "$n"
}
await_meta_leader() {
  local deadline=$((SECONDS + RECOVERY_TIMEOUT)) id
  until id="$(meta_leader)"; do
    (( SECONDS < deadline )) || return 1
    sleep 1
  done
  echo "$id"
}

# --- the producer: one session after another, each a new producer epoch --------
acked_total() {
  local total=0 f
  for f in "$WORKDIR"/acked-*; do
    [[ -f "$f" ]] || continue
    total=$((total + $(cat "$f" 2> /dev/null || echo 0)))
  done
  echo "$total"
}

producer_loop() {
  local session=0 holder epoch n cfg pid code
  while [[ ! -e "$RUN_DIR/stop-producer" ]]; do
    if read -r holder epoch < <(current_lease) && [[ -n "$holder" ]] \
      && n="$(candidate_by_uuid "$holder" 2> /dev/null)"; then
      session=$((session + 1))
      # A new producer epoch per session: sequences restart at 0 and the
      # previous session is fenced (emit_client_config_at_epoch).
      cfg="$(emit_client_config_at_epoch "$epoch" "$session")"
      # The stop request can arrive while the lease was being read: launch
      # nothing once it is there (review of #881).
      [[ -e "$RUN_DIR/stop-producer" ]] && break
      event produce_session "session=$session" "node=data-$n" "epoch=$epoch"
      "$VTOP_NODE" produce --client-config "$cfg" --addr "$(candidate_native_addr "$n")" \
        --records 1000000000000 --batch "$BATCH" --value-bytes "$VALUE_BYTES" --rate "$RATE" \
        --durability quorum --request-timeout-ms 10000 \
        --acked-file "$WORKDIR/acked-$session" --history "$PRODUCE_HISTORY" \
        >> "$WORKDIR/logs/produce.log" 2>&1 &
      pid=$!
      echo "$pid" > "$WORKDIR/producer.pid"
      # The stop file is written before the shutdown reads producer.pid, so
      # either that read sees this pid or this check sees the file.
      [[ -e "$RUN_DIR/stop-producer" ]] && kill "$pid" 2> /dev/null
      code=0
      wait "$pid" || code=$?
      event produce_session_end "session=$session" "exit_code=$code" \
        "acked=$(cat "$WORKDIR/acked-$session" 2> /dev/null || echo 0)"
    fi
    sleep 1
  done
}

# --- the reader: the range from its log start, every read_interval_s -----------
read_once() {
  local holder epoch n start out code=0 hwm
  read -r holder epoch < <(current_lease) || { event read ok=false 'detail="no lease holder"'; return 1; }
  n="$(candidate_by_uuid "$holder" 2> /dev/null)" || return 1
  # Retention moves the log start; a read below it is refused, so a read
  # starts where the leader says its log starts.
  start="$(metric_value "$(data_metrics_addr $((n + 10)))" vtop_broker_log_start_offset 2> /dev/null || echo 0)"
  start="${start%.*}"
  out="$WORKDIR/logs/read.log"
  # Bounded (review of #881): verify has no deadline of its own, and a node
  # that accepts a connection and never answers (a paused leader, a disk that
  # stalls) would hold the reader, and the final read, forever. A read that
  # runs out of time is a failed read, and the run goes on to its verdict.
  timeout --kill-after=5 "$READ_TIMEOUT" "$VTOP_NODE" verify --client-config "$(emit_client_config_at_epoch "$epoch" 1)" \
    --addr "$(candidate_native_addr "$n")" --expect-at-least 0 --verify-content-through 0 \
    --max-offset-gaps 1000000000000 --value-bytes "$VALUE_BYTES" --from-offset "$start" \
    --dump "$READ_HISTORY" > "$out" 2>&1 || code=$?
  if [[ $code -eq 0 ]]; then
    hwm="$(sed -n 's/.*high_watermark=\([0-9]*\).*/\1/p' "$out" | tail -1)"
    event read ok=true "node=data-$n" "epoch=$epoch" "start_offset=$start" "high_watermark=${hwm:-null}"
  else
    # A read can fail because the cluster was busy (a refused fence, a
    # dropped connection, the deadline above), or because what the leader
    # served broke the log's promises; verify stops before it records the
    # read then, so the second kind is marked here and fails the run (review
    # of #881).
    integrity=false
    grep -qE "$READ_INTEGRITY_ERRORS" "$out" && integrity=true
    event read ok=false "node=data-$n" "epoch=$epoch" "start_offset=$start" \
      "integrity=$integrity" "detail=$(json_string "$(tail -c 300 "$out")")"
  fi
  return $code
}

reader_loop() {
  local next
  while [[ ! -e "$RUN_DIR/stop-reader" ]]; do
    next=$((SECONDS + READ_INTERVAL))
    read_once || true
    while (( SECONDS < next )) && [[ ! -e "$RUN_DIR/stop-reader" ]]; do sleep 1; done
  done
}

# --- the faults -------------------------------------------------------------------
SHIMS="$WORKDIR/shims"
mkdir -p "$SHIMS"
"${SOAK_PY[@]}" shim-source > "$SHIMS/skew.c"
shim_for() { # <seconds> — build scenario 07's shim for this step once
  local so="$SHIMS/skew$1.so"
  [[ -f "$so" ]] || gcc -shared -fPIC -O2 -DVTOP_SKEW_SECONDS="$1" -o "$so" "$SHIMS/skew.c" -ldl
  echo "$so"
}

netns_of() { case "$1" in meta-*) echo "vtop-meta-${1#meta-}" ;; data-*) echo "vtop-data-${1#data-}" ;; esac; }

# Each fault runs in this shell, says what it hit and how in FAULT_TARGET and
# FAULT_DETAIL, and returns non-zero when it did not land.
FAULT_TARGET="" FAULT_DETAIL=""
said() { FAULT_TARGET="$1" FAULT_DETAIL="$2"; }
fault_range_leader_kill() { # <hold> <occurrence>
  local n
  n="$(await_range_leader)" || { said "" "no range leader to kill"; return 1; }
  stop_node_named "data-$n" KILL
  soak_sleep "$1"
  start_soak_candidate "$n" || { said "data-$n" "did not come back"; return 1; }
  said "data-$n" "killed, restarted after $1 s"
}
fault_meta_leader_kill() { # <hold> <occurrence>
  local id
  id="$(await_meta_leader)" || { said "" "no metadata leader to kill"; return 1; }
  stop_node_named "meta-$id" KILL
  soak_sleep "$1"
  start_soak_meta "$id" || { said "meta-$id" "did not come back"; return 1; }
  said "meta-$id" "killed, restarted after $1 s"
}
fault_partition() { # <hold> <occurrence>: the range leader, the metadata leader, a follower
  local target ns n addr landed=no
  case $(($2 % 3)) in
    0) n="$(await_range_leader)" || { said "" "no range leader"; return 1; }; target="data-$n" ;;
    1) n="$(await_meta_leader)" || { said "" "no metadata leader"; return 1; }; target="meta-$n" ;;
    *) n="$(await_range_leader)" || { said "" "no range leader"; return 1; }; target="data-$((n % 3 + 1))" ;;
  esac
  ns="$(netns_of "$target")"
  ip netns exec "$ns" iptables -I INPUT 1 -i eth0 -j DROP
  ip netns exec "$ns" iptables -I OUTPUT 1 -o eth0 -j DROP
  # Landed: its metrics endpoint no longer answers across the bridge.
  case "$target" in
    meta-*) addr="$(meta_metrics_addr "${target#meta-}")" ;;
    *) addr="$(data_metrics_addr $((${target#data-} + 10)))" ;;
  esac
  [[ "$(probe_readyz "$addr")" == "000" ]] && landed=yes
  soak_sleep "$1"
  ip netns exec "$ns" iptables -D INPUT -i eth0 -j DROP
  ip netns exec "$ns" iptables -D OUTPUT -o eth0 -j DROP
  said "$target" "isolated for $1 s (unreachable while cut: $landed)"
  [[ "$landed" == yes ]]
}
fault_pause() { # <hold> <occurrence>: the range leader, then the metadata leader
  local target n pid state
  if (( $2 % 2 == 0 )); then
    n="$(await_range_leader)" || { said "" "no range leader"; return 1; }; target="data-$n"
  else
    n="$(await_meta_leader)" || { said "" "no metadata leader"; return 1; }; target="meta-$n"
  fi
  pid="$(node_pid "$target")"
  kill -STOP "$pid"
  state="$(ps -o stat= -p "$pid" 2> /dev/null || true)"
  soak_sleep "$1"
  kill -CONT "$pid" 2> /dev/null || true
  said "$target" "SIGSTOP for $1 s (state while stopped: ${state:-gone})"
  [[ "$state" == T* ]]
}
fault_disk_full() { # <hold> <occurrence>: a follower, then the leader
  local n disk free
  n="$(await_range_leader)" || { said "" "no range leader"; return 1; }
  if (( $2 % 2 == 0 )); then n=$((n % 3 + 1)); fi
  disk="$WORKDIR/disk-$n"
  dd if=/dev/zero of="$disk/ballast" bs=65536 2> /dev/null || true
  free="$(df -B1 --output=avail "$disk" | tail -1 | tr -d ' ')"
  soak_sleep "$1"
  rm -f "$disk/ballast"
  said "data-$n" "disk filled for $1 s with $free bytes left"
  (( free < 1048576 ))
}
fault_clock_step() { # <hold> <occurrence>: the range leader, then the metadata leader
  local target n step so marker shifted before after
  step="$("${SOAK_PY[@]}" step "$PROFILE" "$2")"
  if (( $2 % 2 == 0 )); then
    n="$(await_range_leader)" || { said "" "no range leader"; return 1; }; target="data-$n"
  else
    n="$(await_meta_leader)" || { said "" "no metadata leader"; return 1; }; target="meta-$n"
  fi
  so="$(shim_for "$step")"
  marker="$WORKDIR/skew-$target"
  rm -f "$marker"
  stop_node_named "$target" TERM
  before="$(date +%s)"
  start_node_named "$target" "LD_PRELOAD=$so" "VTOP_SKEW_LOADED_MARKER=$marker" \
    || { said "$target" "did not start under the shim"; return 1; }
  after="$(date +%s)"
  # Landed: the shim recorded the time it gives the node, `step` from ours.
  shifted="$(cat "$marker" 2> /dev/null || echo)"
  soak_sleep "$1"
  stop_node_named "$target" TERM
  start_node_named "$target" || { said "$target" "did not come back on the true clock"; return 1; }
  said "$target" "clock stepped $step s for $1 s (shim time ${shifted:-none})"
  [[ -n "$shifted" ]] && (( shifted >= before + step - 5 && shifted <= after + step + 5 ))
}

# --- recovery: every node up and the producer acknowledging again ---------------
await_recovery() { # <fault-id> <since-acked>
  local deadline=$((SECONDS + RECOVERY_TIMEOUT)) started=$SECONDS name down
  while (( SECONDS < deadline )); do
    watch_nodes
    down=""
    for name in "${ALL_NODES[@]}"; do
      [[ -n "$(node_pid "$name")" ]] || down="$down $name"
    done
    if [[ -z "$down" ]] && (( $(acked_total) > $2 )) && range_leader > /dev/null 2>&1; then
      event recovered "id=$1" ok=true "recovery_s=$((SECONDS - started))"
      return 0
    fi
    sleep 1
  done
  event recovered "id=$1" ok=false "recovery_s=$((SECONDS - started))" \
    "detail=\"no acknowledgement within ${RECOVERY_TIMEOUT} s of the fault's end${down:+; down:$down}\""
  return 1
}

# --- bring-up -----------------------------------------------------------------------
cat > "$RUN_DIR/nodes.json" <<EOF
{
  "meta-1": {"kind": "meta", "metrics": "$(meta_metrics_addr 1)"},
  "meta-2": {"kind": "meta", "metrics": "$(meta_metrics_addr 2)"},
  "meta-3": {"kind": "meta", "metrics": "$(meta_metrics_addr 3)"},
  "data-1": {"kind": "data", "metrics": "$(data_metrics_addr 11)", "data_dir": "$WORKDIR/disk-1/data", "disk": "$WORKDIR/disk-1"},
  "data-2": {"kind": "data", "metrics": "$(data_metrics_addr 12)", "data_dir": "$WORKDIR/disk-2/data", "disk": "$WORKDIR/disk-2"},
  "data-3": {"kind": "data", "metrics": "$(data_metrics_addr 13)", "data_dir": "$WORKDIR/disk-3/data", "disk": "$WORKDIR/disk-3"}
}
EOF
event run_start "profile=\"$(field name)\"" "duration_s=$DURATION" "host=\"$(uname -srm)\""

for id in 1 2 3; do start_soak_meta "$id"; done
ADMIN_LOG="$WORKDIR/logs/admin.log"
meta_admin 1 init --members 1,2,3 > /dev/null 2>> "$ADMIN_LOG"
META_LEADER="$(wait_meta_leader 1 2 3 2>> "$ADMIN_LOG")"
# A new cluster starts at the base feature level; an operator raises it to
# what the build supports, and the soak runs what production would. Below
# level 6 a promotion records no lineage verdicts, and below 7 a node cannot
# report its release (#634).
FEATURE_LEVEL="$(sed -n 's/^pub const SUPPORTED_FEATURE_LEVEL: u32 = \([0-9][0-9]*\);.*/\1/p' \
  "$REPO_ROOT/crates/vtop-meta/src/command.rs")"
[[ -n "$FEATURE_LEVEL" ]] || fail "could not read this build's SUPPORTED_FEATURE_LEVEL"
meta_admin "$META_LEADER" enable-features --level "$FEATURE_LEVEL" > /dev/null 2>> "$ADMIN_LOG" \
  || fail "could not raise the metadata feature level to $FEATURE_LEVEL (see $ADMIN_LOG)"
meta_admin "$META_LEADER" create-topic \
  --name "$TOPIC" --topic-uuid "$TOPIC_UUID" --root-range-uuid "$RANGE_ID" > /dev/null 2>> "$ADMIN_LOG" \
  || fail "could not create the topic in metadata (see $ADMIN_LOG)"
for node in "$LEADER_UUID" "$FOLLOWER1_UUID" "$FOLLOWER2_UUID"; do
  meta_admin "$META_LEADER" register-node \
    --node-uuid "$node" --addr "$REGISTER_HOST_PREFIX.1:$REGISTER_PORT" > /dev/null 2>> "$ADMIN_LOG" \
    || fail "could not register data node $node (see $ADMIN_LOG)"
done
for n in 1 2 3; do
  mkdir -p "$WORKDIR/disk-$n"
  mount -t tmpfs -o "size=${TMPFS_MIB}m" tmpfs "$WORKDIR/disk-$n"
  mkdir -p "$WORKDIR/disk-$n/data"
  start_soak_candidate "$n"
done
FIRST="$(await_range_leader)" || fail "no candidate took the range"
log "metadata leader $META_LEADER; range leader data-$FIRST"

# The load starts now. The fault plan's times count from the run's start,
# bring-up included.
event load_start
"${SOAK_PY[@]}" sampler "$RUN_DIR" "$WORKDIR" "$SAMPLE_INTERVAL" 2> "$WORKDIR/logs/sampler.log" &
echo $! >> "$WORKDIR/pids"
producer_loop &
PRODUCER_LOOP_PID=$!
echo "$PRODUCER_LOOP_PID" >> "$WORKDIR/pids"
reader_loop &
echo $! >> "$WORKDIR/pids"

# --- the fault schedule ---------------------------------------------------------------
declare -A OCCURRENCE=()
mapfile -t PLAN < <("${SOAK_PY[@]}" plan "$PROFILE")
for line in "${PLAN[@]}"; do
  read -r id at kind hold <<< "$line"
  while (( $(elapsed) < at )); do watch_nodes; sleep 1; done
  occurrence="${OCCURRENCE[$kind]:-0}"
  OCCURRENCE[$kind]=$((occurrence + 1))
  event fault_start "id=$id" "fault=\"$kind\"" "occurrence=$occurrence" "hold_s=$hold"
  log "fault #$id: $kind (hold $hold s)"
  ok=true
  said "" ""
  "fault_$kind" "$hold" "$occurrence" || ok=false
  event fault_end "id=$id" "ok=$ok" "target=$(json_string "$FAULT_TARGET")" \
    "detail=$(json_string "$FAULT_DETAIL")"
  log "fault #$id: $FAULT_TARGET: $FAULT_DETAIL (landed: $ok)"
  # A node the fault took down and could not bring back is the watcher's now.
  for name in "${ALL_NODES[@]}"; do
    [[ -n "$(node_pid "$name")" ]] || rm -f "$WORKDIR/nodes/$name.down"
  done
  await_recovery "$id" "$(acked_total)" || log "fault #$id: no recovery within ${RECOVERY_TIMEOUT} s"
done

while (( $(elapsed) < DURATION )); do watch_nodes; sleep 1; done

# --- the end: stop the load, read once more, verify every replica --------------------
# stop_producer LOOP_PID — ask the producer loop to stop, end the producer it
# is running right now (the loop's own child, not whatever producer.pid last
# named), and return 0 only once the loop and its producer have exited, so the
# final read never overlaps production (review of #881).
stop_producer() {
  local loop_pid=$1 deadline=$((SECONDS + 60))
  touch "$RUN_DIR/stop-producer"
  while (( SECONDS < deadline )) && kill -0 "$loop_pid" 2> /dev/null; do
    pkill -TERM -P "$loop_pid" 2> /dev/null || true
    sleep 1
  done
  if kill -0 "$loop_pid" 2> /dev/null; then
    pkill -KILL -P "$loop_pid" 2> /dev/null || true
    kill -KILL "$loop_pid" 2> /dev/null || true
    sleep 1
  fi
  wait "$loop_pid" 2> /dev/null || true
  ! kill -0 "$loop_pid" 2> /dev/null && ! pgrep -f "vtop-node produce" > /dev/null
}
touch "$RUN_DIR/stop-reader"
if stop_producer "$PRODUCER_LOOP_PID"; then producer_exited=true; else producer_exited=false; fi
wait_for_loops() { local deadline=$((SECONDS + 60)); while (( SECONDS < deadline )) && pgrep -f "vtop-node (produce|verify)" > /dev/null; do sleep 1; done; }
wait_for_loops
event producer_stopped "acked=$(acked_total)" "exited=$producer_exited"
[[ "$producer_exited" == true ]] || fail "the producer did not exit within 60 s of the stop; the final read would overlap production"
final_ok=false
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if read_once; then final_ok=true; break; fi
  sleep 5
done
event final_read "ok=$final_ok"

# Let the followers take the tail before they stop.
leader="$(range_leader 2> /dev/null || echo)"
if [[ -n "$leader" ]]; then
  target="$(metric_value "$(data_metrics_addr $((leader + 10)))" vtop_broker_cluster_committed_offset || echo 0)"
  deadline=$((SECONDS + 120))
  for n in 1 2 3; do
    while (( SECONDS < deadline )); do
      have="$(metric_value "$(data_metrics_addr $((n + 10)))" vtop_broker_local_committed_offset || echo -1)"
      (( ${have%.*} >= ${target%.*} )) && break
      sleep 1
    done
  done
fi
# One sample past the duration before the sampler stops: the run is judged
# complete only when it was observed to the end (review of #881).
deadline=$((SECONDS + 3 * SAMPLE_INTERVAL + 30))
until python3 -c "
import json, sys
rows = open(sys.argv[1]).read().splitlines()
sys.exit(0 if rows and json.loads(rows[-1])['elapsed_s'] >= float(sys.argv[2]) else 1)
" "$RUN_DIR/samples.jsonl" "$DURATION" 2> /dev/null || (( SECONDS >= deadline )); do
  sleep 1
done
touch "$RUN_DIR/stop-sampler"
for name in data-1 data-2 data-3 meta-1 meta-2 meta-3; do stop_node_named "$name" TERM; done

# vtopctl segment verify on every segment of every replica; the active tail is
# sealed first (seal_and_verify_active's method, recorded instead of failing).
python3 - "$RUN_DIR/finals.json" > /dev/null <<'EOF'
import json, sys
json.dump({"replicas": {}}, open(sys.argv[1], "w"))
EOF
for n in 1 2 3; do
  dir="$WORKDIR/disk-$n/data" vlog="$WORKDIR/logs/segment-verify-data-$n.log"
  : > "$vlog"
  segments=0 failed=() actives=0
  for active in "$dir"/*.active; do
    [[ -f "$active" ]] || continue
    actives=$((actives + 1))
    "$VTOP_NODE" seal-active --path "$active" >> "$vlog" 2>&1 || failed+=("seal:$(basename "$active")")
  done
  for segment in "$dir"/*.segment; do
    [[ -f "$segment" ]] || continue
    segments=$((segments + 1))
    "$VTOPCTL" segment verify "$segment" --require self >> "$vlog" 2>&1 \
      || failed+=("$(basename "$segment")")
  done
  python3 - "$RUN_DIR/finals.json" "data-$n" "$segments" "$actives" ${failed[@]+"${failed[@]}"} <<'EOF'
import json, sys
path, node, segments, actives, *failed = sys.argv[1:]
finals = json.load(open(path))
finals["replicas"][node] = {"segments": int(segments), "active_tails": int(actives), "failed": failed}
json.dump(finals, open(path, "w"), indent=2, sort_keys=True)
EOF
done
event run_end
log "run complete"
