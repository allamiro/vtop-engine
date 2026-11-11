#!/usr/bin/env bash
# Scenario 23 — a replica holding acknowledged records the candidate lacks
# must keep them and refuse the candidate (#611).
#
# System model: crash-recovery processes, honest nodes, a partially
# synchronous network. Three identical candidates over TLS, as scenario 14.
#
# The #611 shape, placed with signals rather than luck:
#   1. the leader L and both followers take a shared prefix, and both
#      followers' committed offsets are seen to cover it;
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
#     takes the range at an epoch newer than both L's and the one C was
#     refused at.
#   * SAFETY: every record acknowledged before the kill is readable,
#     byte-exact, from whichever candidate ends up serving.
#   * LIVENESS: some candidate serves quorum produce at a newer epoch within
#     two lease durations plus an election timeout once every fault is
#     removed.
# When L comes back holding records past R's tail that nobody acknowledged
# (its own copy lands first, so a kill can leave one in-flight batch there),
# nothing fences it and no quorum forms: that is #641. The scenario then
# exits 77 (SKIP) after the #611 check, saying so; it never reports PASS.
# Likewise when C comes back holding every acknowledged record (the
# producer's acknowledged floor trails what the leader has already sent):
# the #611 shape was not reached, and the scenario says so and exits 77.
# It also reports whether C's probe was refused by R's answer (the #611
# path proper) or missed its quorum.
#
# Knobs: CHAOS_611_SEED picks which follower lags and how far into the
# suffix the kill lands (logged, so a failure replays exactly);
# CHAOS_611_SHARED is the shared prefix (default 200), CHAOS_611_BATCH the
# produce batch (default 20), and CHAOS_611_SUFFIX_MAX (default 2000, at
# least one batch) bounds the suffix acknowledged before L dies, chosen in
# [BATCH, SUFFIX_MAX].
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

# METADATA LEADERSHIP MAY MOVE, so everything here follows it. This scenario
# keeps a loaded host busy (three data nodes, a producer, two kills and a
# freeze), and two early runs (seeds 604 and 609) saw the metadata leader
# change mid-scenario. With the library's single `admin_endpoint`, every lease
# agent then asked the former leader forever ("not the metadata leader; it
# named node 2") and no candidate could take the range: a harness limit, not
# the protocol under test. Scenario-local overrides, so the other scenarios'
# configs stay exactly as they are:
#   * candidates get `lease.admin_peers` for all three metadata nodes, which
#     is how a lease agent follows a redirect (#292);
#   * lease reads try the named node first, then the other two.
emit_lease_yaml() {
  local id="$1" cert="${2:-data-1}" m
  echo "lease:"
  echo "  admin_endpoint: \"$(meta_host "$id"):$(meta_admin_port "$id")\""
  echo "  server_name: \"localhost\""
  echo "  topic_uuid: $TOPIC_UUID"
  echo "  tls: { ca: $CERTS/ca.pem, cert: $CERTS/$cert.pem, key: $CERTS/$cert-key.pem }"
  echo "  lease_duration_ms: $LEASE_DURATION_MS"
  echo "  renew_interval_ms: $LEASE_RENEW_MS"
  echo "  poll_interval_ms: $LEASE_POLL_MS"
  echo "  admin_peers:"
  for m in 1 2 3; do
    echo "    - { node_id: $m, endpoint: \"$(meta_host "$m"):$(meta_admin_port "$m")\" }"
  done
}
eval "lib_$(declare -f lease_field)"
lease_field() { # <meta-id> <python expression over `d`>
  local id="$1" expr="$2" m
  for m in "$id" 1 2 3; do
    lib_lease_field "$m" "$expr" && return 0
  done
  return 1
}

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
require_integer_in_range CHAOS_611_BATCH "$BATCH" 1 65536
# At least one batch: the kill waits for that much suffix to be acknowledged.
require_integer_in_range CHAOS_611_SUFFIX_MAX "$SUFFIX_MAX" "$BATCH" 100000000
# How much suffix L and R acknowledge before L dies: in [BATCH, SUFFIX_MAX].
SUFFIX=$((BATCH + SEED % (SUFFIX_MAX - BATCH + 1)))
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
# An acknowledgement proves only that SOME majority holds the prefix (#646
# review). Both followers must hold it before C dies, so that what C lacks is
# exactly the suffix acknowledged after. The committed offset counts the
# epoch's boundary marker too, hence the epoch on top of the record count.
PREFIX="$(cat "$ACKED_FILE")"
for n in "$C" "$R"; do
  await_metric_at_least "$(data_metrics_addr $((n + 10)))"     vtop_broker_local_committed_offset $((PREFIX + EPOCH))     "follower $n must hold the $PREFIX-record shared prefix before C is killed"
done
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
C_NEXT="$(sed -n 's/.*segment_recovered report=.*next_offset: \([0-9]*\).*/\1/p' \
  "$WORKDIR/logs/data-candidate-$C.log" | tail -1)"
log "C=$C restarted from its own directory holding ${C_NEXT:-?} records"
# The acknowledged floor the producer writes trails what the leader has
# already sent, so C can die holding records acknowledged after it (seed 701
# did: C came back with all 300). Then C lacks nothing, may legitimately
# lead, and the #611 shape was not reached: reported, never a PASS. One
# boundary marker per promoted epoch sits in the log beside the records.
if [[ -n "$C_NEXT" && "$C_NEXT" -ge $((ACKED + EPOCH)) ]]; then
  log "RESULT seed=$SEED shared=$FROZEN_AT acked=$ACKED c=$C r=$R c_next=$C_NEXT \
serving=none (shape not reached)"
  log "SKIP: C came back holding every one of the $ACKED acknowledged records; the #611 \
shape (C lacking acknowledged records) was not reached"
  exit 77
fi
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
[[ -n "$R_EPOCH" ]] \
  || fail "R=$R logged a role change without an epoch: $(grep "data_node_role_changed" "$R_LOG" | tail -1)"
[[ "$R_EPOCH" -gt "$EPOCH" && "$R_EPOCH" -gt "$C_EPOCH" ]] \
  || fail "R=$R must lead at an epoch newer than the deposed leader's $EPOCH and the refused \
candidate's $C_EPOCH; it leads at $R_EPOCH"
log "R=$R took the range at epoch $R_EPOCH (L led at $EPOCH, C was refused at $C_EPOCH); \
C=$C never led (refused by R's answer: $REFUSED)"

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
  log "RESULT seed=$SEED shared=$FROZEN_AT acked=$ACKED c=$C r=$R c_next=${C_NEXT:-?} c_epoch=$C_EPOCH \
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

log "RESULT seed=$SEED shared=$FROZEN_AT acked=$ACKED c=$C r=$R c_next=${C_NEXT:-?} c_epoch=$C_EPOCH \
c_refused_by_r=$REFUSED r_epoch=$R_EPOCH r_next=${R_NEXT:-?} l_next=${L_NEXT:-?} \
serving=$SERVING serving_epoch=$SERVING_EPOCH"
