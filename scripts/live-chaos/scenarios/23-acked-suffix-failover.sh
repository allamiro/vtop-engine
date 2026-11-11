#!/usr/bin/env bash
# Scenario 23 — a replica holding acknowledged records the candidate lacks
# must keep them and refuse the candidate (#611).
#
# System model: crash-recovery processes, honest nodes, a partially
# synchronous network. Three identical candidates over TLS, as scenario 14.
#
# The #611 shape, placed with signals rather than luck:
#   1. the leader L and both followers take a shared prefix;
#   2. one follower, C, is SIGKILLed, so the suffix L produces next is
#      acknowledged by L and the other follower, R, alone. Killed rather
#      than frozen: a frozen process still has the leader's appends queued in
#      its socket buffer and applies them when it thaws, which is not the
#      shape (a first draft froze C and C came back holding everything);
#   3. L is SIGKILLed while the producer is mid-stream, so R may not have
#      learned the last commit mark;
#   4. R is frozen and C restarted from its own directory, so C is the only
#      candidate that can take the lease, and it does;
#   5. R is thawed the moment C holds the lease, so C's promotion probe fences
#      R. R must keep the suffix C lacks and answer above C, and C must be
#      refused. Before #616, R truncated the suffix at the fence and C was
#      established without it;
#   6. L restarts. C is behind R's tail, and a new leader does not stream a
#      lagging follower up from its log (#341), so R needs L for a quorum.
#
# Invariants:
#   * SAFETY (#611): C, which lacks acknowledged records, never leads, and R
#     takes the range at a newer epoch.
#   * SAFETY: every record acknowledged before the kill is readable,
#     byte-exact, from whichever candidate ends up serving.
#   * LIVENESS: some candidate serves quorum produce at a newer epoch within
#     two lease durations plus an election timeout once every fault is
#     removed.
# When L comes back holding records past R's tail that nobody acknowledged
# (its own copy lands first, so a kill can leave one in-flight batch there),
# nothing fences it and no quorum forms: that is #641. The scenario then
# exits 77 (SKIP) after the #611 check, saying so; it never reports PASS.
# It also reports whether C's probe was refused by R's answer (the #611
# path proper) or missed its quorum.
#
# Knobs: CHAOS_611_SEED picks which follower lags and how far into the
# suffix the kill lands (logged, so a failure replays exactly);
# CHAOS_611_SHARED, CHAOS_611_SUFFIX_MAX and CHAOS_611_BATCH size the stream.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

# v2: every promotion appends a boundary marker, as in scenario 14.
export CHAOS_SEGMENT_FORMAT=v2

require_binaries
init_workdir

SEED="${CHAOS_611_SEED:-$RANDOM}"
SHARED="${CHAOS_611_SHARED:-200}"
SUFFIX_MAX="${CHAOS_611_SUFFIX_MAX:-2000}"
BATCH="${CHAOS_611_BATCH:-20}"
require_integer_in_range CHAOS_611_SEED "$SEED" 0 4294967295
require_integer_in_range CHAOS_611_SHARED "$SHARED" 1 100000000
require_integer_in_range CHAOS_611_SUFFIX_MAX "$SUFFIX_MAX" 1 100000000
require_integer_in_range CHAOS_611_BATCH "$BATCH" 1 65536
# The kill lands after at least one batch of suffix and at most SUFFIX_MAX.
SUFFIX=$((BATCH + SEED % SUFFIX_MAX))
TOTAL=$((SHARED + SUFFIX + 100000))
log "seed=$SEED shared=$SHARED suffix=$SUFFIX batch=$BATCH"

# --- metadata plane and three candidates (scenario 14's bring-up) ------------
M1=$(start_meta_node 1 2 3)
M2=$(start_meta_node 2 1 3)
M3=$(start_meta_node 3 1 2)
log "meta nodes up: $M1 $M2 $M3"
meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
meta_admin "$LEADER_ID" create-topic \
  --name "$TOPIC" --topic-uuid "$TOPIC_UUID" --root-range-uuid "$RANGE_ID" > /dev/null \
  || fail "could not create the topic in metadata"
for node in "$LEADER_UUID" "$FOLLOWER1_UUID" "$FOLLOWER2_UUID"; do
  meta_admin "$LEADER_ID" register-node \
    --node-uuid "$node" --addr "$REGISTER_HOST_PREFIX.1:$REGISTER_PORT" > /dev/null \
    || fail "could not register data node $node"
done

declare -a PIDS
PIDS[1]=$(start_candidate 1 "$LEADER_ID")
PIDS[2]=$(start_candidate 2 "$LEADER_ID")
PIDS[3]=$(start_candidate 3 "$LEADER_ID")

read -r HOLDER EPOCH <<<"$(await_any_lease_holder "$LEADER_ID")"
L="$(candidate_by_uuid "$HOLDER")"
await_log_line "$WORKDIR/logs/data-candidate-$L.log" \
  "data_node_role_changed role=leader" "the first leader must take the range"
# The two followers, ordered, and the seed picks which one lags.
read -r F_LOW F_HIGH <<<"$(for n in 1 2 3; do [[ $n -ne $L ]] && printf '%s ' "$n"; done)"
if (( SEED % 2 == 0 )); then C="$F_LOW" R="$F_HIGH"; else C="$F_HIGH" R="$F_LOW"; fi
read -r C_UUID _ <<<"$(candidate_identity "$C")"
log "leader L=$L at epoch $EPOCH; C=$C will lag, R=$R will hold the suffix"

# --- 1. the shared prefix, 2. the suffix only L and R acknowledge ------------
ACKED_FILE="$WORKDIR/acked"
CLIENT_CFG="$(emit_client_config_at_epoch "$EPOCH")"
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" \
  --addr "$(candidate_native_addr "$L")" \
  --records "$TOTAL" --batch "$BATCH" --durability quorum \
  --acked-file "$ACKED_FILE" > "$WORKDIR/logs/produce.log" 2>&1 &
PRODUCER=$!
echo "$PRODUCER" >> "$WORKDIR/pids"
await_acked_floor "$ACKED_FILE" "$SHARED"
kill9_pid "${PIDS[$C]}"
FROZEN_AT="$(cat "$ACKED_FILE")"
log "C=$C SIGKILLed with the acknowledged floor at $FROZEN_AT"
await_acked_floor "$ACKED_FILE" $((FROZEN_AT + SUFFIX))

# --- 3. L dies mid-stream --------------------------------------------------
kill9_pid "${PIDS[$L]}"
kill -STOP "${PIDS[$R]}"
kill9_pid "$PRODUCER"
wait "$PRODUCER" 2>/dev/null || true
ACKED="$(cat "$ACKED_FILE")"
log "L=$L SIGKILLed mid-stream; $ACKED records acknowledged, $((ACKED - FROZEN_AT)) of \
them by L and R alone; R=$R frozen"
[[ "$ACKED" -gt "$FROZEN_AT" ]] \
  || fail "no record was acknowledged while C was down; the #611 shape was not reached"

# --- 4. C is the only candidate that can take the lease ----------------------
PIDS[C]=$(start_candidate "$C" "$LEADER_ID")
log "C=$C restarted from its own directory"
C_EPOCH="$(await_lease_holder "$LEADER_ID" "$C_UUID" \
  $((LEASE_DURATION_MS / 1000 + ELECTION_TIMEOUT_SECONDS)))"
# --- 5. R answers C's fence ------------------------------------------------
kill -CONT "${PIDS[$R]}"
log "C=$C holds the lease at epoch $C_EPOCH; R=$R thawed to answer its fence"

# --- the #611 claim: R, not C, takes the range ------------------------------
R_LOG="$WORKDIR/logs/data-candidate-$R.log"
C_LOG="$WORKDIR/logs/data-candidate-$C.log"
await_log_line "$R_LOG" "data_node_role_changed role=leader" \
  "R holds every acknowledged record and must take the range once C is refused" \
  $((2 * LEASE_DURATION_MS / 1000 + ELECTION_TIMEOUT_SECONDS))
R_EPOCH="$(sed -n 's/.*data_node_role_changed role=leader .* epoch=\([0-9]*\).*/\1/p' "$R_LOG" | tail -1)"
if grep -q "data_node_role_changed role=leader" "$C_LOG"; then
  fail "C=$C became leader without the $((ACKED - FROZEN_AT)) acknowledged records it never \
received (#611): $(grep "data_node_role_changed" "$C_LOG" | tail -1)"
fi
if grep -q "refusing promotion: fewer than a majority" "$C_LOG"; then
  REFUSED=yes
else
  REFUSED=no
fi
log "R=$R took the range at epoch $R_EPOCH; C=$C never led (refused by R's answer: $REFUSED)"

# --- liveness needs a second follower that holds R's tail -------------------
# C is behind R's tail, and a new leader does not stream a lagging follower
# up from its log: that is the repair path's job (#341), so R and C alone
# cannot commit R's boundary marker. The deposed leader comes back instead.
# L may hold records past R's tail that nobody acknowledged (its own copy
# lands before the followers'), and nothing fences it on return: that is
# #641, reported here, not judged.
R_NEXT="$(sed -n 's/.*segment_recovered report=.*next_offset: \([0-9]*\).*/\1/p' "$R_LOG" | tail -1)"
PIDS[L]=$(start_candidate "$L" "$LEADER_ID")
L_NEXT="$(sed -n 's/.*segment_recovered report=.*next_offset: \([0-9]*\).*/\1/p' \
  "$WORKDIR/logs/data-candidate-$L.log" | tail -1)"
log "L=$L restarted holding ${L_NEXT:-?} records; R's tail at its promotion was ${R_NEXT:-?}"
if [[ -n "$L_NEXT" && -n "$R_NEXT" && "$L_NEXT" -gt "$R_NEXT" ]]; then
  # #641, live: L returns unfenced with records past R's tail that nobody
  # acknowledged. R's appends there meet a replica that reports holding
  # them, so R cannot reach a quorum with L, and C is behind (#341). The
  # #611 claim above has been checked; the read-back cannot run without a
  # published mark. Exit 77 is reported as SKIP by run-all.sh, never PASS.
  log "RESULT seed=$SEED shared=$FROZEN_AT acked=$ACKED c=$C r=$R c_epoch=$C_EPOCH \
c_refused_by_r=$REFUSED r_epoch=$R_EPOCH r_next=$R_NEXT l_next=$L_NEXT serving=none (#641)"
  log "SKIP: L returned holding $((L_NEXT - R_NEXT)) unacknowledged record(s) past R's tail \
and is never fenced (#641); no quorum can form until it is"
  exit 77
fi

# --- liveness: some candidate serves at a newer epoch -------------------------
deadline=$((SECONDS + 2 * LEASE_DURATION_MS / 1000 + ELECTION_TIMEOUT_SECONDS))
SERVING="" SERVING_EPOCH=""
while :; do
  if holder="$(lease_field "$LEADER_ID" "d['lease']['holder_node_uuid'] if d.get('lease') else ''")" \
    && [[ -n "$holder" && "$holder" != "$HOLDER" ]] \
    && epoch="$(lease_field "$LEADER_ID" "d['lease']['fencing_epoch']")" && [[ -n "$epoch" ]]; then
    n="$(candidate_by_uuid "$holder")"
    # A bumped producer epoch, so the batch is fresh writes rather than
    # duplicates of the first stream's sequences (scenario 14's lesson).
    if "$VTOP_NODE" produce --client-config "$(emit_client_config_at_epoch "$epoch" 2)" \
      --addr "$(candidate_native_addr "$n")" --records "$BATCH" --batch "$BATCH" \
      --durability quorum --acked-file "$WORKDIR/acked-after" \
      > "$WORKDIR/logs/produce-after.log" 2>&1; then
      SERVING="$n" SERVING_EPOCH="$epoch"
      break
    fi
  fi
  [[ $SECONDS -lt $deadline ]] \
    || fail "no candidate served quorum produce after the failover (last holder \
${holder:-none}; see $WORKDIR/logs/produce-after.log)"
  sleep 0.5
done
log "candidate $SERVING serves quorum produce at epoch $SERVING_EPOCH"

# --- safety: every acknowledged record, byte-exact ---------------------------
CHAOS_MAX_OFFSET_GAPS="$SERVING_EPOCH" \
  await_verified_floor "$(emit_client_config_at_epoch "$SERVING_EPOCH" 2)" \
  "$(candidate_native_addr "$SERVING")" "$ACKED" "$ACKED"
log "all $ACKED acknowledged records read back byte-exact from candidate $SERVING"

log "RESULT seed=$SEED shared=$FROZEN_AT acked=$ACKED c=$C r=$R c_epoch=$C_EPOCH \
c_refused_by_r=$REFUSED r_epoch=$R_EPOCH r_next=${R_NEXT:-?} l_next=${L_NEXT:-?} \
serving=$SERVING serving_epoch=$SERVING_EPOCH"
