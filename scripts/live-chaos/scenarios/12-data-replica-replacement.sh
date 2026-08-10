#!/usr/bin/env bash
# Scenario 12 — live data-replica replacement on real processes (#242).
#
# Scenario 03 replaces a metadata VOTER. The deterministic harness proves the
# #180/#181 repair-and-retire path under simulated faults. Nothing ran it on
# real processes with real disks, and that gap is where six separate dead-ends
# were hiding: until #304/#305/#308 the flow could not be driven from the CLI
# at all — no way to set a failure domain, so every RF > 1 placement was
# refused; no way to open a rebalance, so retirement dead-ended at full
# replication factor; no way to read the generation every command compares
# against. Each was found by trying to use the thing, which is what this
# scenario now does on every CI run.
#
# What it proves, in order:
#   1. a replica set is committed at the placement metadata itself computes,
#      not one the operator invented;
#   2. a replica is lost permanently and a fresh node joins in its place;
#   3. `vtopctl node repair` populates the newcomer from a surviving replica,
#      byte-for-byte, and the transferred artifact verifies on its own;
#   4. retirement is REFUSED until the replacement proof commits — the
#      ordering, not merely the end state;
#   5. after the proof, retirement is accepted and metadata's placement names
#      the newcomer and not the dead replica;
#   6. every record acknowledged before the loss is still readable afterwards.
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

require_binaries
init_workdir

RECORDS="${CHAOS_REPLACEMENT_RECORDS:-1500}"
BATCH="${CHAOS_REPLACEMENT_BATCH:-100}"
# The replication factor drives the whole scenario: how many nodes are
# registered, how many failure domains are needed, what the placement must
# hold before and during the move, and what it must come back to. Derived
# rather than repeated, because a factor written in six places is a factor
# that will disagree with itself the first time anyone changes one of them.
RF="${CHAOS_REPLACEMENT_RF:-3}"
# Sized DOWN from the engine defaults so the range rolls under this scenario's
# own load, because only sealed segments transfer and a range that never rolls
# gives a repair nothing to move. The three are coupled — the engine requires
# record <= group <= segment — so they come down together.
#
# The group must hold a whole produce batch: an append larger than the group is
# rejected outright, so the bound is derived from BATCH rather than picked, and
# stays correct when the batch is overridden.
# The replacement is not a follower when it is repaired, so the leader has to
# be told it may pull. Set before any leader starts, because the allowlist is
# read from the config at construction.
export CHAOS_TRANSFER_PEERS="${CHAOS_TRANSFER_PEERS:-$SPARE_UUID}"
# The record bound is sized from the RECORD, not from how many of them go in a
# batch — those are unrelated, and deriving one from the other made
# CHAOS_REPLACEMENT_BATCH=1 produce a 64-byte limit for a 136-byte record, so
# the first produce was rejected before the scenario reached anything it tests.
#
# The producer writes an 8-byte key and `--value-bytes` of value; the rest is
# framing. The multiple is slack for that framing, not a guess at it.
VALUE_BYTES="${CHAOS_REPLACEMENT_VALUE_BYTES:-128}"
require_integer_in_range CHAOS_REPLACEMENT_VALUE_BYTES "$VALUE_BYTES" 1 1048576
# Fixed slack for key and framing, not a multiple of the value: a multiple
# grows the segment threshold faster than the data does, so a larger value
# width stopped the range rolling at all and the scenario had nothing to
# transfer.
RECORD_CEILING=$((VALUE_BYTES + 512))
export CHAOS_MAX_RECORD_BYTES="${CHAOS_MAX_RECORD_BYTES:-$RECORD_CEILING}"
# A group holds a whole batch, PLUS one record's worth of headroom: the engine
# requires a group to fit a maximal record plus frame overhead, so a group sized
# to exactly one record is refused — which is what a batch of 1 produced.
export CHAOS_MAX_GROUP_BYTES="${CHAOS_MAX_GROUP_BYTES:-$(((BATCH + 1) * RECORD_CEILING))}"
# Twice the group, so the range rolls every couple of batches. The assertion
# below fails loudly if this stops producing a sealed segment rather than
# letting the scenario continue against an unrolled range.
# One batch per segment, so the number of rolls tracks RECORDS/BATCH and stays
# the same whatever the records weigh.
export CHAOS_MAX_SEGMENT_BYTES="${CHAOS_MAX_SEGMENT_BYTES:-$CHAOS_MAX_GROUP_BYTES}"
# CHECKED AFTER DERIVING, not by guessing a bound on the inputs. Each override
# is individually reasonable and the combination need not be: a 1 MiB value at
# the default batch derives a group far above the engine's 256 MiB ceiling, and
# the node then refuses to start with a message about group sizing rather than
# about the two settings that produced it.
ENGINE_MAX_GROUP_BYTES=$((256 * 1024 * 1024))
[[ "$CHAOS_MAX_GROUP_BYTES" -le "$ENGINE_MAX_GROUP_BYTES" ]] \
  || fail "CHAOS_REPLACEMENT_VALUE_BYTES=$VALUE_BYTES with batch $BATCH derives a group limit of \
$CHAOS_MAX_GROUP_BYTES bytes, above the engine maximum of $ENGINE_MAX_GROUP_BYTES. Lower either."
[[ "$CHAOS_MAX_SEGMENT_BYTES" -le $((8 * 1024 * 1024 * 1024)) ]] \
  || fail "the derived segment limit $CHAOS_MAX_SEGMENT_BYTES exceeds the engine maximum"
# A whole batch also has to fit one produce request.
NODE_MAX_FRAME_BYTES=$((32 * 1024 * 1024))
[[ $((BATCH * CHAOS_MAX_RECORD_BYTES)) -le "$NODE_MAX_FRAME_BYTES" ]] \
  || fail "batch $BATCH of up to $CHAOS_MAX_RECORD_BYTES-byte records exceeds the node frame \
limit of $NODE_MAX_FRAME_BYTES bytes; lower CHAOS_REPLACEMENT_BATCH or CHAOS_REPLACEMENT_VALUE_BYTES"
DOMAIN_PREFIX="${CHAOS_REPLACEMENT_DOMAIN_PREFIX:-rack}"
require_integer_in_range CHAOS_REPLACEMENT_RECORDS "$RECORDS" 1 100000000
# Bounded by the PROTOCOL, not just by the record count: the produce path caps
# a request at MAX_RECORDS, so a larger batch cannot be encoded at all and the
# scenario would exit before reaching anything it exists to test.
#
# NOT overridable. Making the ceiling an environment variable would let an
# operator raise it above the limit the binary still enforces, which restores
# the exact failure the cap was added to prevent — a bound you can opt out of
# is not a bound.
PRODUCE_MAX_RECORDS=65536
BATCH_CEILING="$RECORDS"
[[ "$BATCH_CEILING" -gt "$PRODUCE_MAX_RECORDS" ]] && BATCH_CEILING="$PRODUCE_MAX_RECORDS"
require_integer_in_range CHAOS_REPLACEMENT_BATCH "$BATCH" 1 "$BATCH_CEILING"
# Exactly 3. A lower factor would still register all three original nodes, so
# rendezvous would place only some of them and the node this scenario retires
# might not be in the placement at all — `propose-rebalance` rejects a source
# that is not a current replica, and the failure would look like a bug in the
# replacement flow rather than in the scenario's own arithmetic.
require_integer_in_range CHAOS_REPLACEMENT_RF "$RF" 3 3
DURING_MOVE=$((RF + 1))
# A first registration, so the generation starts at the base the state machine
# expects. Overridable rather than fixed in the command line below.
SEGMENT_GENERATION="${CHAOS_REPLACEMENT_SEGMENT_GENERATION:-1}"
RANGE_GENERATION="${CHAOS_RANGE_GENERATION:-1}"

# --- metadata plane ---------------------------------------------------------
M1=$(start_meta_node 1 2 3)
M2=$(start_meta_node 2 1 3)
M3=$(start_meta_node 3 1 2)
log "meta nodes up: $M1 $M2 $M3"
meta_admin 1 init --members 1,2,3 > /dev/null
LEADER_ID="$(wait_meta_leader 1 2 3)"
log "meta leader elected: node $LEADER_ID"

meta_admin "$LEADER_ID" create-topic \
  --name "$TOPIC" --topic-uuid "$TOPIC_UUID" --root-range-uuid "$RANGE_ID" > /dev/null \
  || fail "could not create the topic in metadata"

# THREE nodes, not four. The spare joins after the loss, which is both the real
# sequence and the one that makes the test meaningful: a spare registered up
# front is a candidate for the original placement, and there would be nothing
# to replace it into.
# register_with_domain <node-uuid> <replica-n> <domain-suffix>
#
# The node generation is READ FROM THE ACK, never assumed. It is the
# compare-and-swap token the next command needs, and a hardcoded 1 would work
# right up until anything else touched the record — then fail as a rejected
# write with nothing to say the cause was a stale token.
register_with_domain() {
  local uuid="$1" n="$2" suffix="$3"
  local domain="$DOMAIN_PREFIX-$suffix"
  local ack="$WORKDIR/ack-register-$suffix.json"
  meta_admin "$LEADER_ID" register-node \
    --node-uuid "$uuid" --addr "$(replica_addr "$n")" > "$ack" \
    || fail "could not register data node $uuid"
  local generation
  generation="$(json_field "$ack" 'd["generation"]')" \
    || fail "the register Ack carried no generation for $uuid: $(cat "$ack")"
  [[ -n "$generation" && "$generation" != "None" ]] \
    || fail "the register Ack carried no generation for $uuid: $(cat "$ack")"
  # WITHOUT THIS the placement below is refused, and the refusal names failure
  # domains rather than the command that sets them. `register-node` leaves the
  # domain empty and `CommitSegmentPlacement` requires distinct domains above
  # RF 1, so a cluster built entirely through this CLI could never commit a
  # multi-replica placement until #305 exposed this command.
  meta_admin "$LEADER_ID" set-node-placement-attrs \
    --node-uuid "$uuid" --failure-domain "$domain" \
    --placement-weight "$PLACEMENT_WEIGHT" --expected-generation "$generation" > /dev/null \
    || fail "could not set the failure domain for $uuid"
  log "registered $uuid in $domain at generation $generation"
}
PLACEMENT_WEIGHT="${CHAOS_REPLACEMENT_WEIGHT:-1}"
require_integer_in_range CHAOS_REPLACEMENT_WEIGHT "$PLACEMENT_WEIGHT" 1 1000000
register_with_domain "$LEADER_UUID" 0 a
register_with_domain "$FOLLOWER1_UUID" 1 b
register_with_domain "$FOLLOWER2_UUID" 2 c
log "metadata knows the topic and $RF data nodes, each in its own failure domain"

# --- data plane -------------------------------------------------------------
EXPECTED_FIRST_EPOCH=1
F1=$(start_follower 1 "" "$EXPECTED_FIRST_EPOCH")
# F2 WATCHES METADATA. It has to survive the replacement, and the epoch moves
# several times on the way through — a follower pinned to the epoch it was born
# at refuses every append once metadata mints a newer one, so the restarted
# leader could never form a quorum with it.
F2=$(start_follower 2 "" "" "$LEADER_ID")
LEADER=$(start_leader_with_lease "$LEADER_ID" replacement)
log "data plane up: leader plus two followers"

# THE EPOCH await_lease_holder ALREADY RETURNED, not a second read — the same
# rule the post-replacement restart below states: a separate `lease_field`
# sits outside the helper's retry loop, and a momentary metadata refusal
# turns it into an abort about nothing the scenario tests (#326).
EPOCH="$(await_lease_holder "$LEADER_ID" "$LEADER_UUID")"
[[ "$EPOCH" == "$EXPECTED_FIRST_EPOCH" ]] \
  || fail "expected the first acquisition to mint epoch $EXPECTED_FIRST_EPOCH, got $EPOCH"
log "lease held by $LEADER_UUID at epoch $EPOCH"

# A COMPLETED produce, not an interrupted one. Scenario 09 already covers the
# race between a dying producer and an election; what this scenario needs is a
# known acknowledged floor to hold the replacement against, so every record is
# on record before anything is broken.
ACKED_FILE="$WORKDIR/acked"
CLIENT_CFG="$(emit_client_config_at_epoch "$EPOCH")"
"$VTOP_NODE" produce --client-config "$CLIENT_CFG" --addr "$(native_addr)" \
  --records "$RECORDS" --batch "$BATCH" --value-bytes "$VALUE_BYTES" --durability quorum \
  --acked-file "$ACKED_FILE" > "$WORKDIR/logs/produce.log" 2>&1 \
  || fail "the quorum produce failed: $(tail -5 "$WORKDIR/logs/produce.log")"
await_acked_floor "$ACKED_FILE" "$RECORDS"
ACKED="$(cat "$ACKED_FILE")"
log "$ACKED records acknowledged under quorum produce"

# --- the range rolled under load, so a sealed prefix exists -----------------
# ONLY SEALED SEGMENTS TRANSFER: the active tail is still being appended to, so
# its bytes can be superseded by a truncation mid-copy. The roll threshold is
# set small above rather than sealing offline — sealing a stopped node's tail
# leaves a directory that cannot be opened again ("its tail was sealed without
# a successor"), so the leader would not come back, and no operator would do
# that to a live range anyway.
SEALED_SOURCE=""
for candidate in "$WORKDIR/data-leader"/*.segment; do
  [[ -f "$candidate" ]] && SEALED_SOURCE="$candidate"
done
[[ -n "$SEALED_SOURCE" ]] \
  || fail "the leader rolled no segment under $RECORDS records at a $CHAOS_MAX_SEGMENT_BYTES-byte \
threshold, so there is nothing for a repair to transfer"
log "leader holds a sealed segment: $(basename "$SEALED_SOURCE")"

# --- register the sealed segment in metadata --------------------------------
# METADATA HAS TO KNOW THE SEGMENT EXISTS before anything can be said about it:
# a placement, a proof, a retirement and a retention decision all resolve the
# segment record first and fail with NotFound otherwise. Sealing is a local
# event on the node, so the fact has to be carried across deliberately.
#
# Every field is READ FROM THE ARTIFACT, not asserted. `segment verify`
# re-derives the offsets and the content root from the frames themselves, so
# what lands in metadata is what the bytes say — which matters because every
# later replacement proof is compared against this root.
VERIFY_JSON="$WORKDIR/verify-source.json"
"$VTOPCTL" --json segment verify "$SEALED_SOURCE" --require self > "$VERIFY_JSON" 2>/dev/null \
  || fail "the leader's sealed segment did not verify: $(head -c 400 "$VERIFY_JSON")"
SEG_BASE="$(json_field "$VERIFY_JSON" 'd[0]["base_offset"]')"
SEG_NEXT="$(json_field "$VERIFY_JSON" 'd[0]["next_offset"]')"
SEG_ROOT="$(json_field "$VERIFY_JSON" 'd[0]["content_root"]')"
SEG_UUID="$(json_field "$VERIFY_JSON" 'd[0]["segment_id"]')"
log "sealed segment $SEG_UUID covers [$SEG_BASE, $SEG_NEXT) with root ${SEG_ROOT:0:16}…"

meta_admin "$LEADER_ID" register-sealed-segment \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  --segment-generation "$SEGMENT_GENERATION" \
  --base-offset "$SEG_BASE" --next-offset "$SEG_NEXT" \
  --content-root "$SEG_ROOT" --sealed-by-epoch "$EPOCH" \
  --expected-range-generation "$RANGE_GENERATION" > /dev/null \
  || fail "could not register the sealed segment in metadata"
log "sealed segment registered in metadata"

# VERIFIED IS A SEPARATE FACT from registered. `commit-segment-placement`
# refuses a segment that has not been verified — placing bytes nobody has read
# would put a durability promise behind an unchecked artifact. The root passed
# here is the one `segment verify` re-derived above, and metadata compares it
# against what was registered, so this cannot bless a segment as something it
# is not.
# DEADLINE-POLLED, like every observation of the cluster this suite makes: a
# linearizable read taken 76ms after the register once found the leader's
# ReadIndex round with only its own answer on a loaded runner, and the one-shot
# read turned that instant into a scenario failure (#326).
SEG_AFTER_REGISTER="$WORKDIR/placement-after-register.json"
meta_admin_read "$SEG_AFTER_REGISTER" "$LEADER_ID" get-placement \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  || fail "could not read the segment after registering it"
meta_admin "$LEADER_ID" mark-segment-verified \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  --content-root "$SEG_ROOT" \
  --expected-generation "$(json_field "$SEG_AFTER_REGISTER" 'd["segment"]["segment_generation"]')" \
  > "$WORKDIR/logs/mark-verified.log" 2>&1 \
  || fail "could not mark the segment verified: $(tail -3 "$WORKDIR/logs/mark-verified.log")"
log "segment marked verified against the root the bytes produced"

# --- commit the placement metadata itself computes --------------------------
# ASKED FOR, not invented. `CommitSegmentPlacement` compares the proposal
# POSITIONALLY against a rendezvous over the currently Active nodes, and an
# operator can see neither the candidate set nor the algorithm — so before #308
# a first placement could only be reached by guessing an order and resubmitting
# until one was accepted.
PLACEMENT_JSON="$WORKDIR/placement-initial.json"
meta_admin_read "$PLACEMENT_JSON" "$LEADER_ID" get-placement \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  --for-replication-factor "$RF" \
  || fail "could not read the placement proposal"


PROPOSED="$(json_field "$PLACEMENT_JSON" '" ".join(d["proposal"]["replica_nodes"])')" \
  || fail "the proposal was refused: $(cat "$PLACEMENT_JSON")"
[[ -n "$PROPOSED" ]] || fail "no proposal came back: $(cat "$PLACEMENT_JSON")"
log "metadata proposes this placement, in order: $PROPOSED"

REPLICA_ARGS=()
for node in $PROPOSED; do
  REPLICA_ARGS+=(--replica-node "$node")
done
meta_admin "$LEADER_ID" commit-segment-placement \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  --replication-factor "$RF" "${REPLICA_ARGS[@]}" \
  --expected-segment-generation "$(json_field "$PLACEMENT_JSON" 'd["segment"]["segment_generation"]')" \
  > "$WORKDIR/logs/commit-placement.log" 2>&1 \
  || fail "the placement metadata proposed was refused when committed back: \
$(tail -3 "$WORKDIR/logs/commit-placement.log")"
log "placement committed at the factor and order metadata chose"

# The order must round-trip. A placement that came back permuted would be
# refused by every later command, and the refusal would say nothing about why.
COMMITTED="$WORKDIR/placement-committed.json"
meta_admin_read "$COMMITTED" "$LEADER_ID" get-placement \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  || fail "could not read the committed placement"
READ_BACK="$(json_field "$COMMITTED" '" ".join(d["replica_nodes"])')"
[[ "$READ_BACK" == "$PROPOSED" ]] \
  || fail "the committed placement came back in a different order: proposed [$PROPOSED], read [$READ_BACK]"
log "the committed placement reads back in the same order it was proposed"

# --- lose a replica permanently ---------------------------------------------
stop_node_now "$F1"
log "follower 1 ($FOLLOWER1_UUID) is gone for good"

# --- a fresh node joins ------------------------------------------------------
register_with_domain "$SPARE_UUID" 3 d

PLACEMENT_GEN="$(json_field "$COMMITTED" 'd["generation"]')"
meta_admin "$LEADER_ID" propose-rebalance \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  --from-node-uuid "$FOLLOWER1_UUID" --to-node-uuid "$SPARE_UUID" \
  --expected-placement-generation "$PLACEMENT_GEN" > /dev/null \
  || fail "could not open the rebalance"
log "rebalance opened: $FOLLOWER1_UUID -> $SPARE_UUID"

# DURABILITY MUST NOT DIP. The destination is added before the source retires,
# so the segment runs at RF + 1 for the duration of the move and never at
# RF - 1. That is the whole reason the flow has a rebalance step rather than a
# swap.
MOVING="$WORKDIR/placement-moving.json"
meta_admin_read "$MOVING" "$LEADER_ID" get-placement \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  || fail "could not read the placement mid-move"
MOVING_COUNT="$(json_field "$MOVING" 'len(d["replica_nodes"])')"
[[ "$MOVING_COUNT" == "$DURING_MOVE" ]] \
  || fail "expected RF+1 = $DURING_MOVE replicas while the move is open, got $MOVING_COUNT"
json_field "$MOVING" 'd["replica_nodes"]' | grep -q "$SPARE_UUID" \
  || fail "the destination is not in the placement, so nothing can be proven against it"
json_field "$MOVING" 'd["replica_nodes"]' | grep -q "$FOLLOWER1_UUID" \
  || fail "the retiring source left the placement early; durability dipped to RF-1"
log "placement is at RF+1 during the move: durability never dips"

# --- repair populates the newcomer ------------------------------------------
SPARE_DIR="$WORKDIR/data-follower-3"
REPAIR_CONFIG="$(emit_repair_config 0)"
# `|| REPAIR_EXIT=$?` rather than reading `$?` on the next line: under `set -e`
# a non-zero exit aborts the script before the assignment runs, so the exit
# code this scenario is about would never be examined — and the abort prints
# nothing, which is how it hid the first time.
REPAIR_EXIT=0
"$VTOPCTL" node repair \
  --config "$REPAIR_CONFIG" --from "$LEADER_UUID" --into "$SPARE_DIR" \
  --fencing-epoch "$EPOCH" > "$WORKDIR/logs/repair.log" 2>&1 || REPAIR_EXIT=$?
# 0 current, 1 behind by a measured gap, 2 adopted but unmeasured. A gap is
# EXPECTED here: the records written after the seal are in the leader's active
# segment, which never transfers, and the replica closes that by catching up.
[[ "$REPAIR_EXIT" -le 1 ]] \
  || fail "repair failed (exit $REPAIR_EXIT): $(tail -5 "$WORKDIR/logs/repair.log")"
log "repair finished with exit $REPAIR_EXIT: $(grep -c . "$WORKDIR/logs/repair.log") lines of report"

SEALED_COPY=""
for candidate in "$SPARE_DIR"/*.segment; do
  [[ -f "$candidate" ]] && SEALED_COPY="$candidate"
done
[[ -n "$SEALED_COPY" ]] || fail "repair left no sealed segment in $SPARE_DIR"

# BYTE-EXACT, not merely valid. The transfer's whole claim is that the bytes
# are the same bytes; a copy that verified on its own but differed would be a
# different segment with a consistent internal story.
cmp -s "$SEALED_SOURCE" "$SEALED_COPY" \
  || fail "the transferred segment differs from the source byte-for-byte"
"$VTOPCTL" segment verify "$SEALED_COPY" --require self > "$WORKDIR/logs/verify-spare.log" 2>&1 \
  || fail "the transferred segment failed verify: $(tail -5 "$WORKDIR/logs/verify-spare.log")"
log "the transferred artifact is byte-identical to the source and verifies on its own"

# --- retirement is refused until the proof commits ---------------------------
# THE ORDERING IS THE POINT. #242 asks for the ordering to be asserted, not the
# end state: a flow that retired first and proved afterwards would reach the
# same final placement while having dropped to RF-1 on an unproven copy.
set +e
"$VTOPCTL" meta plan-replica-retirement \
  --config "$(emit_admin_config "$LEADER_ID")" \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  --retiring-node-uuid "$FOLLOWER1_UUID" \
  --expected-segment-generation "$(json_field "$MOVING" 'd["segment"]["segment_generation"]')" \
  --fencing-epoch "$EPOCH" \
  > "$WORKDIR/logs/premature-retirement.log" 2>&1
PREMATURE_EXIT=$?
set -e
[[ "$PREMATURE_EXIT" -ne 0 ]] \
  || fail "retirement was ACCEPTED before any replacement proof committed; the ordering that keeps a replica from being dropped on an unproven copy is not enforced"
log "retirement refused before the proof, as it must be: $(tail -1 "$WORKDIR/logs/premature-retirement.log")"

# --- prove the copy ----------------------------------------------------------
# THE ROOT COMES FROM METADATA, THE PROOF COMES FROM THE COPY. `commit_replacement_proof`
# requires the submitted root to equal the one on the segment record, so
# inventing a root is not possible — but neither is that, by itself, evidence
# the destination holds those bytes. So the root is read from metadata and then
# CHECKED AGAINST THE TRANSFERRED FILE with `--expect-root`, which is the step
# that makes the proof mean something: it fails if the copy is not the segment
# metadata is describing.
CONTENT_ROOT="$(json_field "$MOVING" 'd["segment"]["content_root"]')"
[[ -n "$CONTENT_ROOT" ]] || fail "metadata has no content root for this segment"
"$VTOPCTL" segment verify "$SEALED_COPY" --expect-root "$CONTENT_ROOT" --require self \
  > "$WORKDIR/logs/verify-proof.log" 2>&1 \
  || fail "the transferred copy does not match the content root metadata records for this \
segment, so no proof can honestly be made for it: $(tail -5 "$WORKDIR/logs/verify-proof.log")"
LENGTH_BYTES="$(wc -c < "$SEALED_COPY" | tr -d ' ')"
[[ "$LENGTH_BYTES" -gt 0 ]] || fail "the transferred copy is empty"
log "the copy verifies against metadata's content root ${CONTENT_ROOT:0:16}…, $LENGTH_BYTES bytes"

# THE RAFT TERM, read from metadata — not the range's fencing epoch. They are
# different numbers that happen to coincide while nothing has re-elected, and
# `verified_term` is the consensus term the proof is audited against. Passing
# the epoch would commit a proof that records a term nobody was ever in, and
# the run would still pass.
META_TERM="$(meta_status_field "$LEADER_ID" '["current_term"]')"
[[ -n "$META_TERM" ]] || fail "could not read the metadata leader's current term for the proof"
log "proof will record metadata term $META_TERM (range fencing epoch is $EPOCH)"

# THE PROOF NAMES THE REPLICA BEING REPLACED, not the node the bytes came from.
# `commit_replacement_proof` requires the source to equal the open intent's
# `from`, and the intent is the move F1 -> spare. The bytes were pulled from
# the leader, because F1 is dead — which is the whole point of a replacement —
# and that is an implementation detail of how the copy was made. What the proof
# asserts is that the SPARE now holds the segment F1 was holding, and the
# content root check above is what makes that assertion true.

meta_admin "$LEADER_ID" commit-replacement-proof \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  --expected-segment-generation "$(json_field "$MOVING" 'd["segment"]["segment_generation"]')" \
  --content-root "$CONTENT_ROOT" --expected-length-bytes "$LENGTH_BYTES" \
  --source-node-uuid "$FOLLOWER1_UUID" --destination-node-uuid "$SPARE_UUID" \
  --fencing-epoch "$EPOCH" --verifier-node-uuid "$LEADER_UUID" --verified-term "$META_TERM" \
  > "$WORKDIR/logs/commit-proof.log" 2>&1 \
  || fail "the replacement proof was refused: $(tail -3 "$WORKDIR/logs/commit-proof.log")"
log "replacement proof committed"

# --- now retirement is accepted ---------------------------------------------
PROVEN="$WORKDIR/placement-proven.json"
meta_admin_read "$PROVEN" "$LEADER_ID" get-placement \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  || fail "could not read the placement after the proof"

meta_admin "$LEADER_ID" plan-replica-retirement \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  --retiring-node-uuid "$FOLLOWER1_UUID" \
  --expected-segment-generation "$(json_field "$PROVEN" 'd["segment"]["segment_generation"]')" \
  --fencing-epoch "$EPOCH" \
  > /dev/null || fail "retirement was refused even after the proof committed"
log "retirement planned, on the strength of committed evidence"

PLANNED="$WORKDIR/placement-planned.json"
meta_admin_read "$PLANNED" "$LEADER_ID" get-placement \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  || fail "could not read the placement after planning"
# THE PHYSICAL EFFECT, before the record of it. `confirm-replica-retired`
# asserts the local bytes are gone; confirming while they sit on disk records
# something untrue and the scenario would pass having exercised the bookkeeping
# and not the retirement. The node is already stopped, so this is what an
# operator reclaiming the disk actually does.
rm -rf "$WORKDIR/data-follower-1"
[[ ! -d "$WORKDIR/data-follower-1" ]] \
  || fail "the retired replica's data directory is still present"
log "the retired replica's bytes are deleted, as the confirmation is about to assert"

meta_admin "$LEADER_ID" confirm-replica-retired \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  --retiring-node-uuid "$FOLLOWER1_UUID" \
  --expected-segment-generation "$(json_field "$PLANNED" 'd["segment"]["segment_generation"]')" \
  > /dev/null || fail "the retirement could not be confirmed"
log "retirement confirmed"

# --- the placement names the newcomer, not the dead replica -----------------
FINAL="$WORKDIR/placement-final.json"
meta_admin_read "$FINAL" "$LEADER_ID" get-placement \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  || fail "could not read the final placement"
FINAL_NODES="$(json_field "$FINAL" '" ".join(d["replica_nodes"])')"
grep -q "$SPARE_UUID" <<< "$FINAL_NODES" \
  || fail "the replacement is not in the final placement: $FINAL_NODES"
grep -q "$FOLLOWER1_UUID" <<< "$FINAL_NODES" \
  && fail "the retired replica is still in the placement: $FINAL_NODES"
FINAL_COUNT="$(json_field "$FINAL" 'len(d["replica_nodes"])')"
[[ "$FINAL_COUNT" == "$RF" ]] \
  || fail "expected the placement back at RF = $RF after the move, got $FINAL_COUNT"
[[ "$(json_field "$FINAL" 'd["rebalance_intent"] is None')" == "True" ]] \
  || fail "the rebalance intent is still open after the move completed; the segment stays locked"
log "final placement is back at RF $RF, names the replacement, and holds no open intent"

# MARKED DEAD, or it stays a placement candidate. `active_placement_candidates`
# takes every Active node, so a replica this scenario declared gone for good
# would be selected for the NEXT segment's placement — recording durability on
# a node that does not exist. Retiring it from one segment says nothing about
# the registry.
DEAD_GEN="$(json_field "$WORKDIR/ack-register-b.json" 'd["generation"]')"
meta_admin "$LEADER_ID" set-node-state \
  --node-uuid "$FOLLOWER1_UUID" --state dead \
  --expected-generation "$((DEAD_GEN + 1))" \
  > "$WORKDIR/logs/mark-dead.log" 2>&1 \
  || fail "could not mark the lost replica dead: $(tail -3 "$WORKDIR/logs/mark-dead.log")"

# And it is really out: a placement proposal at the same factor must no longer
# be able to name it.
CANDIDATES="$WORKDIR/placement-after-dead.json"
meta_admin_read "$CANDIDATES" "$LEADER_ID" get-placement \
  --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" --segment-uuid "$SEG_UUID" \
  --for-replication-factor "$RF" \
  || fail "could not re-read the placement proposal after marking the node dead"
if grep -q "$FOLLOWER1_UUID" <<< "$(json_field "$CANDIDATES" 'd["proposal"]')"; then
  fail "the lost replica is still a placement candidate after being marked dead"
fi
log "the lost replica is marked dead and no longer proposed for placement"

# --- nothing acknowledged was lost ------------------------------------------
# The leader is restarted with the NEW replica set, because the follower list
# is static config: retiring a replica in metadata does not stop the leader
# replicating to it, and adding one does not start it. An operator who skips
# this still has a leader talking to a node metadata no longer counts.
# THE NEWCOMER HAS TO ACTUALLY RUN. Verified promotion requires a quorum of
# replicas to confirm the committed boundary, so a leader restarted with two
# followers configured and only one alive refuses to promote — correctly, and
# with a message about the boundary rather than about the missing process.
#
# Started as a WATCHING follower: the epoch has moved several times during the
# replacement, and a follower pinned to the epoch it was born at would refuse
# every append forever.
SPARE=$(start_follower 3 "" "" "$LEADER_ID")
log "the replacement replica is running against the repaired directory"

stop_node_now "$LEADER"
LEADER=$(start_leader_with_replicas "$LEADER_ID" post-replacement 2 3)
await_lease_holder "$LEADER_ID" "$LEADER_UUID" > /dev/null
# RE-READ, not reused. The old lease can expire while the leader restarts, in
# which case the replacement process reacquires at a NEW epoch — and a client
# config pinned to the original one is fenced on every request, so the
# verification below would time out having proven nothing about the data.
# THE EPOCH await_lease_holder ALREADY RETURNED, not a second read. It echoes
# the epoch from inside its own retry loop, so taking its value keeps the read
# under that loop's deadline. A separate `lease_field` afterwards sits outside
# it and returns empty on a transient metadata refusal, which under `set -e`
# aborts the durability check for a reason having nothing to do with the data.
EPOCH_AFTER="$(await_lease_holder "$LEADER_ID" "$LEADER_UUID")"
[[ -n "$EPOCH_AFTER" ]] \
  || fail "the restarted leader holds the lease but reported no epoch"
log "leader restarted with the post-replacement replica set, holding epoch $EPOCH_AFTER"
CLIENT_CFG="$(emit_client_config_at_epoch "$EPOCH_AFTER")"

await_verified_floor "$CLIENT_CFG" "$(native_addr)" "$ACKED" "" "$VALUE_BYTES"
log "every one of the $ACKED acknowledged records is still readable after the replacement"

# WHAT THE REPLACEMENT REPLICA HOLDS AFTER A LEADER TRANSITION — the assertion
# #315 was filed to make possible.
#
# The check above reads through the leader, so it proves the RANGE survived; it
# says nothing about the newcomer. Before #315, the newcomer lost everything
# the repair gave it here: the transfer carried no epoch history, so its first
# reconciliation with a promoted leader found no common point and truncated the
# range to its base. The repair now installs the source's `fencing-epochs`
# alongside the segments, so the reconciliation AGREES and the repaired data
# survives the one operation that asks it to prove its lineage.
#
# A glob that matches nothing makes `ls` exit non-zero, and under `pipefail`
# that aborts the scenario with no message, so counting must not be able to
# fail.
SPARE_SEALED=0
for _candidate in "$SPARE_DIR"/*.segment; do
  [[ -f "$_candidate" ]] && SPARE_SEALED=$((SPARE_SEALED + 1))
done
[[ "$SPARE_SEALED" -gt 0 ]] \
  || fail "the replacement lost its sealed segments to the leader transition; the repaired \
range was truncated, which is #315 reopened"
# The committed offset must HOLD THE SEALED-PREFIX END, not merely leave files
# on disk: a truncation would drag the boundary back toward the base even if a
# directory listing still showed segments. Deadline-polled, because the
# restarted leader's HWM stream has to reach the newcomer before its boundary
# is republished.
#
# The bound is $SEG_NEXT — the end of the sealed prefix the repair installed —
# and NOT the acknowledged floor of $ACKED. The records in ($SEG_NEXT, $ACKED]
# live in the leader's active tail, which only the append path can deliver,
# and a restarted leader's retransmission buffer is empty: that remaining gap
# is #306's boundary, stated below rather than silently waited on.
SPARE_DEADLINE=$((SECONDS + PROGRESS_TIMEOUT_SECONDS))
SPARE_OFFSET="$(follower_committed_offset 3)"
while [[ "$SPARE_OFFSET" -lt "$SEG_NEXT" ]]; do
  [[ $SECONDS -lt $SPARE_DEADLINE ]] \
    || fail "the replacement kept $SPARE_SEALED sealed segment(s) but its committed offset \
reads $SPARE_OFFSET, below the sealed-prefix end of $SEG_NEXT; the repaired range was \
truncated or never re-served, which is #315 reopened"
  sleep 1
  SPARE_OFFSET="$(follower_committed_offset 3)"
done
log "the replacement survived the leader transition with $SPARE_SEALED sealed segment(s), \
committed through $SPARE_OFFSET >= $SEG_NEXT — the repair outlives the failover that used to \
erase it. The $((ACKED - SEG_NEXT)) tail records above the sealed prefix remain out of its \
reach until #306 gives the gap a road back"

# Named directly rather than through `${!name}`: indirect expansion is not a
# USE as far as shellcheck is concerned, so the variables stayed flagged and
# the check read as decoration.
assert_running() { # <label> <pid>
  kill -0 "$2" 2>/dev/null \
    || fail "$1 (pid $2) is not running at the end of the replacement; the placement says RF $RF \
but the range is short a replica"
  # `kill -0` ALSO SUCCEEDS FOR A ZOMBIE — a process that has exited and not
  # been reaped still has a pid entry — which is exactly the RF-1 case this
  # check exists to catch. The state is what distinguishes them.
  local state
  state="$(ps -o stat= -p "$2" 2>/dev/null | tr -d ' ')"
  case "$state" in
    Z*) fail "$1 (pid $2) has exited and not been reaped (state $state); the placement says RF \
$RF but the range is short a replica" ;;
  esac
}
assert_running "the surviving follower" "$F2"
assert_running "the replacement replica" "$SPARE"
log "the surviving replica and the newcomer are both still serving"

# STOPPED FIRST. `seal_and_verify_active` performs offline recovery and seals
# the active file in place; run against a live follower it races that
# follower's own descriptor, which can write through the rename and invalidate
# the manifest it just produced. Every other scenario stops the node first, and
# the liveness assertion above is what makes stopping it here meaningful rather
# than incidental.
stop_node_now "$SPARE"
# The transferred artifacts were checked byte-for-byte against the source
# earlier; the assertion block above proved the same data outlived the leader
# transition and the newcomer rejoined replication through the acknowledged
# floor. That is the full claim of the replacement flow.
log "PASS"
