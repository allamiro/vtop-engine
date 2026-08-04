#!/usr/bin/env bash
# Scenario 05 — one follower runs out of disk under quorum load.
#
# Follower 2's data dir lives on an 8 MiB tmpfs inside a user namespace (no
# root needed). The produce stream overflows it. Invariants: the quorum
# (leader + follower 1) keeps acknowledging, the starved follower fails
# CLOSED — process alive, cleanly rejecting appends, still answering status —
# and never acknowledges past what its disk actually holds.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir
require_mount_namespace

TMPFS_MIB="${CHAOS_DISKFULL_TMPFS_MIB:-8}"
RECORDS="${CHAOS_DISKFULL_RECORDS:-40000}"
BATCH="${CHAOS_DISKFULL_BATCH:-200}"
VALUE_BYTES="${CHAOS_DISKFULL_VALUE_BYTES:-512}"
require_integer_in_range CHAOS_DISKFULL_TMPFS_MIB "$TMPFS_MIB" 1 1024
require_integer_in_range CHAOS_DISKFULL_RECORDS "$RECORDS" 1 100000000
require_integer_in_range CHAOS_DISKFULL_BATCH "$BATCH" 1 "$RECORDS"
require_integer_in_range CHAOS_DISKFULL_VALUE_BYTES "$VALUE_BYTES" 1 1048576
(( RECORDS * VALUE_BYTES > TMPFS_MIB * 1024 * 1024 )) \
  || fail "disk-full workload is too small for the configured tmpfs; increase CHAOS_DISKFULL_RECORDS/CHAOS_DISKFULL_VALUE_BYTES or reduce CHAOS_DISKFULL_TMPFS_MIB"

F1_PID="$(start_follower 1)"

# Follower 2 inside unshare -rm with a tiny tmpfs as its data dir.
F2_DIR="$WORKDIR/data-follower-2"
mkdir -p "$F2_DIR"
F2_CFG="$(emit_follower_config 2 "$F2_DIR")"
F2_LOG="$WORKDIR/logs/data-follower-2.log"
unshare -rm bash -c "mount -t tmpfs -o size=${TMPFS_MIB}m tmpfs '$F2_DIR' && exec '$VTOP_NODE' data --config '$F2_CFG'" \
  > "$F2_LOG" 2>&1 &
F2_PID=$!
echo "$F2_PID" >> "$WORKDIR/pids"
deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
until grep -q data_node_ready "$F2_LOG" 2>/dev/null; do
  kill -0 "$F2_PID" 2>/dev/null || { cat "$F2_LOG" >&2; fail "follower 2 died on start"; }
  [[ $SECONDS -lt $deadline ]] || fail "follower 2 not ready"
  sleep 0.1
done
log "follower 2 up on an 8 MiB tmpfs"

LEADER_PID="$(start_leader)"
CLIENT_CFG="$(emit_client_config)"

# ~20 MiB of records — at least double the tmpfs — with quorum acks. The
# quorum is leader + follower 1, so the stream must complete even while
# follower 2's disk overflows.
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$RECORDS" --batch "$BATCH" --value-bytes "$VALUE_BYTES" --durability quorum \
  --acked-file "$WORKDIR/acked" > "$WORKDIR/logs/produce.log" 2>&1 \
  || fail "quorum produce failed while one follower was disk-full: $(tail -3 "$WORKDIR/logs/produce.log")"
ACKED="$(cat "$WORKDIR/acked")"
[[ "$ACKED" -eq "$RECORDS" ]] || fail "acked $ACKED != $RECORDS"
log "produce completed on the surviving quorum: $ACKED records"

# Byte-exact read-back of everything acknowledged.
"$VTOP_NODE" verify --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --expect-at-least "$ACKED" --value-bytes "$VALUE_BYTES" > "$WORKDIR/logs/verify.log" 2>&1 \
  || fail "verify failed: $(tail -3 "$WORKDIR/logs/verify.log")"

# Fail-closed follower: still alive, still answering status, and its local
# offset is honest — strictly behind the acknowledged stream, never ahead of
# its 8 MiB disk.
kill -0 "$F2_PID" 2>/dev/null || fail "disk-full follower crashed instead of failing closed"
PROBE_CFG="$(emit_replica_probe_config)"
STATUS="$("$VTOP_NODE" replica-status --client-config "$PROBE_CFG" \
  --addr "$(replica_addr 2)")" || fail "disk-full follower stopped answering status"
LOCAL="${STATUS#*local_committed_offset=}"
LOCAL="${LOCAL%% *}"
NEXT="${STATUS##*next_offset=}"
[[ "$LOCAL" -le "$NEXT" ]] || fail "full follower committed offset $LOCAL > next offset $NEXT"
[[ "$NEXT" -lt "$ACKED" ]] || fail "full follower claims $NEXT records on a $TMPFS_MIB MiB disk"
log "follower 2 failed closed at committed=$LOCAL next_offset=$NEXT (< $ACKED), process alive"

STATUS1="$("$VTOP_NODE" replica-status --client-config "$PROBE_CFG" \
  --addr "$(replica_addr 1)")"
LOCAL1="${STATUS1#*local_committed_offset=}"
LOCAL1="${LOCAL1%% *}"
NEXT1="${STATUS1##*next_offset=}"
[[ "$LOCAL1" -eq "$ACKED" ]] || fail "healthy follower committed offset $LOCAL1 != $ACKED"
[[ "$NEXT1" -eq "$ACKED" ]] || fail "healthy follower next_offset $NEXT1 != $ACKED"
log "healthy follower holds the full stream; quorum math is honest"

# The full follower lives in a private mount namespace. Freeze it at a stable
# point and copy its durable active file + commit boundary out through /proc;
# recovery of that exact copy must seal and verify cleanly despite the failed
# append tail.
kill -STOP "$F2_PID"
F2_VERIFY_DIR="$WORKDIR/verify-follower-2"
mkdir -p "$F2_VERIFY_DIR"
cp "/proc/$F2_PID/root$F2_DIR/range.active" "$F2_VERIFY_DIR/range.active"
cp "/proc/$F2_PID/root$F2_DIR/range.commit" "$F2_VERIFY_DIR/range.commit"

stop_node_now "$LEADER_PID"
stop_node_now "$F1_PID"
stop_node_now "$F2_PID"
seal_and_verify_active leader "$WORKDIR/data-leader/range.active"
seal_and_verify_active follower-1 "$WORKDIR/data-follower-1/range.active"
seal_and_verify_active follower-2-diskfull "$F2_VERIFY_DIR/range.active"
