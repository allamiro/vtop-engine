#!/usr/bin/env bash
# Scenario 05b — one follower's fsync/fdatasync starts returning EIO.
#
# A process-local LD_PRELOAD shim leaves startup untouched, then injects real
# libc sync-call failures after a trigger file appears. Invariants: leader +
# healthy follower continue quorum commits, the failing follower remains
# alive and reports only its durable prefix, and recovery truncates its
# failed append tail into an independently verifiable sealed artifact.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir
require_command cc "Install a C compiler (for example gcc) to build the fsync fault-injection shim."
prepare_shim_dir

RECORDS="${CHAOS_FSYNC_RECORDS:-10000}"
BATCH="${CHAOS_FSYNC_BATCH:-100}"
VALUE_BYTES="${CHAOS_FSYNC_VALUE_BYTES:-256}"
require_integer_in_range CHAOS_FSYNC_RECORDS "$RECORDS" 1 100000000
require_integer_in_range CHAOS_FSYNC_BATCH "$BATCH" 1 "$RECORDS"
require_integer_in_range CHAOS_FSYNC_VALUE_BYTES "$VALUE_BYTES" 1 1048576

SHIM="$SHIM_DIR/fsync-fail.so"
SHIM_SOURCE="$WORKDIR/fsync-fail.c"
apply_fsync_shim_source() {
  cat > "$SHIM_SOURCE" <<'EOF'
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

static int (*real_fsync)(int);
static int (*real_fdatasync)(int);

__attribute__((constructor)) static void load_real_syncs(void) {
    real_fsync = (int (*)(int))dlsym(RTLD_NEXT, "fsync");
    real_fdatasync = (int (*)(int))dlsym(RTLD_NEXT, "fdatasync");
}

static int should_fail(void) {
    const char *trigger = getenv("VTOP_FSYNC_FAIL_TRIGGER");
    return trigger && access(trigger, F_OK) == 0;
}

int fsync(int fd) {
    if (should_fail()) {
        errno = EIO;
        return -1;
    }
    return real_fsync(fd);
}

int fdatasync(int fd) {
    if (should_fail()) {
        errno = EIO;
        return -1;
    }
    return real_fdatasync(fd);
}
EOF
}
apply_fsync_shim_source
cc -shared -fPIC -O2 -o "$SHIM" "$SHIM_SOURCE" -ldl || fail "fsync shim build failed"

F1_PID="$(start_follower 1)"

F2_CFG="$(emit_follower_config 2)"
F2_LOG="$WORKDIR/logs/data-follower-2.log"
TRIGGER="$WORKDIR/fail-fsync"
LD_PRELOAD="$SHIM" VTOP_FSYNC_FAIL_TRIGGER="$TRIGGER" \
  "$VTOP_NODE" data --config "$F2_CFG" > "$F2_LOG" 2>&1 &
F2_PID=$!
echo "$F2_PID" >> "$WORKDIR/pids"
deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
until grep -q data_node_ready "$F2_LOG" 2>/dev/null; do
  kill -0 "$F2_PID" 2>/dev/null || { sed 's/^/    /' "$F2_LOG" >&2; fail "follower 2 died on start"; }
  [[ $SECONDS -lt $deadline ]] || fail "follower 2 not ready"
  sleep 0.1
done
grep -q "cannot be preloaded" "$F2_LOG" && fail "dynamic loader refused the fsync shim"
log "follower 2 ready with sync-failure injection armed"

LEADER_PID="$(start_leader)"
CLIENT_CFG="$(emit_client_config)"
touch "$TRIGGER"

"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$RECORDS" --batch "$BATCH" --value-bytes "$VALUE_BYTES" --durability quorum \
  --acked-file "$WORKDIR/acked" > "$WORKDIR/logs/produce.log" 2>&1 \
  || fail "quorum produce failed during follower fsync errors: $(tail -3 "$WORKDIR/logs/produce.log")"
ACKED="$(cat "$WORKDIR/acked")"
[[ "$ACKED" -eq "$RECORDS" ]] || fail "acked $ACKED != $RECORDS"

"$VTOP_NODE" verify --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --expect-at-least "$ACKED" --value-bytes "$VALUE_BYTES" > "$WORKDIR/logs/verify.log" 2>&1 \
  || fail "verify failed: $(tail -3 "$WORKDIR/logs/verify.log")"

kill -0 "$F2_PID" 2>/dev/null || fail "fsync-failing follower crashed"
PROBE_CFG="$(emit_replica_probe_config)"
STATUS2="$("$VTOP_NODE" replica-status --client-config "$PROBE_CFG" \
  --addr "$(replica_addr 2)")" || fail "fsync-failing follower stopped answering status"
LOCAL2="${STATUS2#*local_committed_offset=}"
LOCAL2="${LOCAL2%% *}"
NEXT2="${STATUS2##*next_offset=}"
log "fsync-failing follower status: committed=$LOCAL2 next_offset=$NEXT2"
[[ "$LOCAL2" -le "$NEXT2" ]] || fail "failing follower committed $LOCAL2 > next $NEXT2"
[[ "$NEXT2" -lt "$ACKED" ]] || fail "fsync-failing follower falsely claims the full stream"
log "sync failures stayed fail-closed at committed=$LOCAL2 next_offset=$NEXT2"

STATUS1="$("$VTOP_NODE" replica-status --client-config "$PROBE_CFG" \
  --addr "$(replica_addr 1)")"
LOCAL1="${STATUS1#*local_committed_offset=}"
LOCAL1="${LOCAL1%% *}"
[[ "$LOCAL1" -eq "$ACKED" ]] || fail "healthy follower committed $LOCAL1 != $ACKED"

stop_node_now "$LEADER_PID"
stop_node_now "$F1_PID"
stop_node_now "$F2_PID"
seal_and_verify_active leader "$WORKDIR/data-leader/range.active"
seal_and_verify_active follower-1 "$WORKDIR/data-follower-1/range.active"
seal_and_verify_active follower-2-fsync-failed "$WORKDIR/data-follower-2/range.active"
