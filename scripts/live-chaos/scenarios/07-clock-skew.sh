#!/usr/bin/env bash
# Scenario 07 — wall-clock skew on one metadata node.
#
# An LD_PRELOAD shim shifts CLOCK_REALTIME by +1h on node 3 (monotonic time
# is left honest, as NTP skew would). Invariants: realtime skew does not
# destabilize leadership, proposals keep committing, and every node remains
# converged. Elections are anchored to monotonic time, so a legitimate win by
# the skewed node is allowed; simultaneous leaders are not.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir
require_command gcc "Install GCC or set up a compatible C compiler for the realtime-skew shim."
prepare_shim_dir

CLOCK_SKEW_SECONDS="${CHAOS_CLOCK_SKEW_SECONDS:-3600}"
CLOCK_SKEW_TOLERANCE_SECONDS="${CHAOS_CLOCK_SKEW_TOLERANCE_SECONDS:-10}"
LEADER_SAMPLES="${CHAOS_CLOCK_LEADER_SAMPLES:-5}"
require_integer_in_range CHAOS_CLOCK_SKEW_SECONDS "$CLOCK_SKEW_SECONDS" 1 86400
require_integer_in_range CHAOS_CLOCK_SKEW_TOLERANCE_SECONDS "$CLOCK_SKEW_TOLERANCE_SECONDS" 0 300
require_integer_in_range CHAOS_CLOCK_LEADER_SAMPLES "$LEADER_SAMPLES" 1 1000

# Build the skew shim.
SHIM="$SHIM_DIR/realtime-skew.so"
cat > "$WORKDIR/skew.c" <<'EOF'
#define _GNU_SOURCE
#include <time.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
/* Shift CLOCK_REALTIME by SKEW seconds; leave monotonic clocks honest. */
#ifndef VTOP_SKEW_SECONDS
#define VTOP_SKEW_SECONDS 3600
#endif
__attribute__((constructor)) static void record_loaded_time(void) {
    const char *marker = getenv("VTOP_SKEW_LOADED_MARKER");
    int (*real)(clockid_t, struct timespec *) =
        (int (*)(clockid_t, struct timespec *))dlsym(RTLD_NEXT, "clock_gettime");
    if (!marker || !real) return;
    struct timespec ts;
    if (real(CLOCK_REALTIME, &ts) != 0) return;
    int fd = open(marker, O_CREAT | O_WRONLY | O_TRUNC, 0600);
    if (fd < 0) return;
    char value[64];
    int length = snprintf(value, sizeof(value), "%lld\n", (long long)ts.tv_sec + VTOP_SKEW_SECONDS);
    if (length > 0) (void)write(fd, value, (size_t)length);
    close(fd);
}
int clock_gettime(clockid_t clk, struct timespec *ts) {
    static int (*real)(clockid_t, struct timespec *) = 0;
    if (!real) real = (int (*)(clockid_t, struct timespec *))dlsym(RTLD_NEXT, "clock_gettime");
    int rc = real(clk, ts);
    if (rc == 0 && (clk == CLOCK_REALTIME || clk == CLOCK_REALTIME_COARSE))
        ts->tv_sec += VTOP_SKEW_SECONDS;
    return rc;
}
time_t time(time_t *out) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    if (out) *out = ts.tv_sec;
    return ts.tv_sec;
}
EOF
gcc -shared -fPIC -O2 -DVTOP_SKEW_SECONDS="$CLOCK_SKEW_SECONDS" \
  -o "$SHIM" "$WORKDIR/skew.c" -ldl || fail "shim build failed"

start_meta_node 1 2 3 > /dev/null
start_meta_node 2 1 3 > /dev/null

# Node 3 runs one hour in the future.
CFG3="$(emit_meta_config 3 1 2)"
LOG3="$WORKDIR/logs/meta-3.log"
SKEW_MARKER="$WORKDIR/skew-loaded"
LD_PRELOAD="$SHIM" VTOP_SKEW_LOADED_MARKER="$SKEW_MARKER" \
  "$VTOP_NODE" meta --config "$CFG3" > "$LOG3" 2>&1 &
P3=$!
echo "$P3" >> "$WORKDIR/pids"
deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
until grep -q meta_node_ready "$LOG3" 2>/dev/null; do
  kill -0 "$P3" 2>/dev/null || { cat "$LOG3" >&2; fail "skewed node died on start"; }
  [[ $SECONDS -lt $deadline ]] || fail "skewed node not ready"
  sleep 0.1
done
grep -q "cannot be preloaded" "$LOG3" && fail "dynamic loader refused the clock-skew shim"
[[ -s "$SKEW_MARKER" ]] || fail "clock-skew shim did not execute its constructor"
SKEWED_EPOCH="$(cat "$SKEW_MARKER")"
HOST_EPOCH="$(date +%s)"
DELTA=$((SKEWED_EPOCH - HOST_EPOCH))
LOWER_SKEW_BOUND=$((CLOCK_SKEW_SECONDS - CLOCK_SKEW_TOLERANCE_SECONDS))
UPPER_SKEW_BOUND=$((CLOCK_SKEW_SECONDS + CLOCK_SKEW_TOLERANCE_SECONDS))
[[ "$DELTA" -ge "$LOWER_SKEW_BOUND" && "$DELTA" -le "$UPPER_SKEW_BOUND" ]] \
  || fail "clock-skew shim reported ${DELTA}s instead of +${CLOCK_SKEW_SECONDS}s"
log "node 3 running with +${CLOCK_SKEW_SECONDS}s CLOCK_REALTIME skew"

meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
log "leader elected: $LEADER_ID"

# Sustained commits with the skewed node participating in quorum.
for n in $(seq 96 111); do
  propose_register "$LEADER_ID" "$(printf '%02x' "$n")" > /dev/null
done

# Exactly one leader across repeated samples — skew must not induce dueling
# leadership.
for _ in $(seq 1 "$LEADER_SAMPLES"); do
  leaders=0
  for id in 1 2 3; do
    state="$(meta_status_field "$id" "['server_state']" 2>/dev/null || echo unknown)"
    [[ "$state" == "Leader" ]] && leaders=$((leaders + 1))
  done
  [[ "$leaders" -eq 1 ]] || fail "$leaders simultaneous leaders under clock skew"
  sleep 0.4
done

# The skewed node converges like any healthy member.
TARGET="$(meta_status_field "$LEADER_ID" "['last_applied']['index']")"
deadline=$((SECONDS + ELECTION_TIMEOUT_SECONDS))
while true; do
  APPLIED="$(meta_status_field 3 "['last_applied']['index']" 2>/dev/null || echo 0)"
  [[ "$APPLIED" == "null" ]] && APPLIED=0
  [[ "$APPLIED" -ge "$TARGET" ]] && break
  [[ $SECONDS -lt $deadline ]] || fail "skewed node stuck at applied=$APPLIED < $TARGET"
  sleep 0.3
done
log "single stable leader throughout; skewed node converged at applied=$APPLIED"
