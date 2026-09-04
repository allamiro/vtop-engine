//! Deterministic metadata state machine.
//!
//! `apply` is a pure function of (current state, apply index, command): it
//! reads no clock, no RNG, and no environment. Every id is proposer-supplied
//! (collisions reject with `AlreadyExists`), optimistic concurrency uses
//! per-record generations, and fencing epochs are strictly monotonic so a
//! stale leaseholder can never publish under a current epoch.
//!
//! Exactly-once semantics: a FIFO dedup table of the last
//! [`DEDUP_CAPACITY`] request ids returns the stored original response for a
//! replayed request, and the table itself is part of the snapshot encoding so
//! dedup survives snapshot/restore identically on every replica.

use crate::command::{
    decode_promotion_outcome, encode_promotion_outcome, MetadataCommand, MetadataError,
    MetadataResponse, NodeState, PromotionOutcome, RangeAssignment, VerificationMethod,
    MAX_ASSIGNED_RANGES, MAX_NODE_ADDR_BYTES, MAX_TIER_BACKEND_ID_BYTES, MAX_TIER_OBJECT_URI_BYTES,
    MAX_TIER_VERSION_ID_BYTES, MAX_TRANSITION_QUORUM,
};
use crate::keys::{validate_group_name, validate_topic_name, MetaKey};
use crate::placement::{
    select_replicas, PlacementCandidate, DEFAULT_PLACEMENT_WEIGHT, MAX_FAILURE_DOMAIN_BYTES,
    MAX_REPLICAS, MAX_TRANSIENT_REPLICAS, MIN_PLACEMENT_WEIGHT,
};
use crate::wire::{
    put_bounded_str, put_bytes32, put_i64, put_u16, put_u32, put_u64, put_u8, put_uuid, CodecError,
    Reader,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use uuid::Uuid;

/// FIFO capacity of the request dedup table, evicted in apply order.
pub const DEDUP_CAPACITY: usize = 65_536;

/// Version byte stream prefix of the state-machine snapshot payload.
const SNAPSHOT_PAYLOAD_VERSION: u16 = 1;

const MAX_SNAPSHOT_KEY_BYTES: usize = 256;
const MAX_SNAPSHOT_VALUE_BYTES: usize = 1024;
const MAX_SNAPSHOT_RESPONSE_BYTES: usize = 1024;
const MAX_SNAPSHOT_RECORDS: u32 = 1 << 24;

const VALUE_TAG_NODE: u8 = 1;
/// Node records that include failure-domain and placement weight.
const VALUE_TAG_NODE_V2: u8 = 12;
const VALUE_TAG_TOPIC: u8 = 2;
const VALUE_TAG_TOPIC_NAME: u8 = 3;
const VALUE_TAG_RANGE: u8 = 4;
const VALUE_TAG_SEGMENT: u8 = 5;
const VALUE_TAG_KEY: u8 = 6;
const VALUE_TAG_GROUP: u8 = 7;
const VALUE_TAG_GROUP_NAME: u8 = 8;
const VALUE_TAG_GROUP_MEMBER: u8 = 9;
/// Member records that include `last_heartbeat_apply_index`.
const VALUE_TAG_GROUP_MEMBER_V2: u8 = 11;
const VALUE_TAG_GROUP_CURSOR: u8 = 10;
const VALUE_TAG_SEGMENT_PLACEMENT: u8 = 13;
const VALUE_TAG_REPLACEMENT_PROOF: u8 = 14;
const VALUE_TAG_REBALANCE_INTENT: u8 = 15;
const VALUE_TAG_RANGE_V2: u8 = 16;
const VALUE_TAG_SEGMENT_TIER_COPY: u8 = 17;
const VALUE_TAG_TOPIC_RETENTION_POLICY: u8 = 18;
const VALUE_TAG_RANGE_TRANSITION: u8 = 19;

/// A registered broker/controller node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRecord {
    pub addr: String,
    pub state: NodeState,
    pub generation: u64,
    /// Failure-domain attribute used by distinct-domain placement filters.
    /// Empty until [`MetadataCommand::SetNodePlacementAttrs`] sets it.
    pub failure_domain: String,
    /// Relative capacity weight for weighted rendezvous scoring.
    pub placement_weight: u32,
}

/// A topic incarnation. `topic_epoch` is allocated by the state machine as
/// `prior epoch for this name + 1`, so recreating a name always fences every
/// earlier incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicRecord {
    pub name: String,
    pub topic_epoch: u64,
    pub generation: u64,
}

/// The name index entry: which incarnation currently owns a topic name and
/// the highest epoch ever allocated for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicNameRecord {
    pub topic_uuid: Uuid,
    pub latest_epoch: u64,
}

/// An outstanding range lease. `granted_apply_index` records *when* in log
/// order the lease was granted — the only place the apply index enters state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseRecord {
    pub holder_node_uuid: Uuid,
    pub fencing_epoch: u64,
    pub granted_apply_index: u64,
    /// Absolute deadline in milliseconds, derived from the acquiring
    /// command's `issued_at_ms` plus its requested duration (#223).
    ///
    /// `None` means "never expires": that is what an administrative
    /// [`MetadataCommand::GrantRangeLease`] mints, and it is also how a lease
    /// written before expiry existed decodes — the conservative reading of a
    /// record from a version that had no concept of one.
    ///
    /// The deadline is derived from data in the replicated log, never from a
    /// local clock, so every replica computes the same expiry. It is a
    /// LIVENESS mechanism, not a safety one: safety comes from the fencing
    /// epoch, which a grant always advances. A clock-skewed candidate can
    /// therefore acquire early and be disruptive, but can never produce two
    /// brokers that both believe they may write.
    pub expires_at_ms: Option<i64>,
}

impl LeaseRecord {
    /// Whether this lease is still live at `now_ms`.
    ///
    /// A lease with no deadline is always live; it can only be replaced by an
    /// explicit release or an administrative grant.
    pub fn is_live_at(&self, now_ms: i64) -> bool {
        match self.expires_at_ms {
            None => true,
            Some(deadline) => now_ms < deadline,
        }
    }
}

/// A key range of a topic. `fencing_epoch` only ever moves forward: every
/// grant increments it, and a release never rewinds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeRecord {
    /// Metadata CAS token: bumps on every mutation of this record (grants,
    /// segment registrations, ...). Never a lineage signal.
    pub generation: u64,
    pub key_prefix: u64,
    pub key_prefix_bits: u8,
    pub fencing_epoch: u64,
    /// Lineage version of the key interval itself. Bumps only on an actual
    /// lineage transition (split/merge); unrelated metadata updates that
    /// advance `generation` leave it untouched, so cursor lineage checks
    /// survive CAS churn. No transition exists yet, so it stays 0 today.
    pub lineage_generation: u64,
    pub lease: Option<LeaseRecord>,
}

/// Verification lifecycle of a sealed segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentState {
    SealedUnverified,
    Verified,
    /// Replacement verification in progress; still not retireable.
    Repairing,
    /// Retirement authorized by a committed ReplacementProof.
    RetirePlanned,
    /// Physical retirement effect confirmed and no durable local placement
    /// remains. A per-replica retirement with surviving replicas returns the
    /// segment to `Verified`.
    Retired,
    /// Bytes failed verification; must not be served.
    Quarantined,
    /// Whole-segment deletion authorized; local bytes still present. Distinct
    /// from `RetirePlanned`, which is the *per-replica* move/repair state — a
    /// single-replica replacement proof must never authorize whole-segment
    /// deletion. TIER_VERIFIED is deliberately not a state: it is the derived
    /// condition `Verified` + committed `SegmentTierCopy` record, so tiered
    /// segments stay eligible for placement and repair.
    RetentionPlanned,
    /// Physical deletion of all local replicas confirmed. The segment and
    /// tier-copy records are retained forever as the rehydration pointer and
    /// corruption-audit anchor.
    RetentionExpired,
}

/// A sealed segment registered against a range under a fencing epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentRecord {
    pub segment_generation: u64,
    pub base_offset: u64,
    pub next_offset: u64,
    pub content_root: [u8; 32],
    pub state: SegmentState,
    pub sealed_by_epoch: u64,
}

/// Lifecycle of a public-key record. Only `Active` exists in this slice;
/// revocation arrives with the security slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyState {
    Active,
}

/// A registered public-key record. Immutable once written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRecord {
    pub scheme: u16,
    pub public_material_digest: [u8; 32],
    pub state: KeyState,
}

/// A consumer group incarnation. `generation` bumps on join/leave/assign.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerGroupRecord {
    pub name: String,
    pub generation: u64,
}

/// Name index entry for consumer groups.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupNameRecord {
    pub group_uuid: Uuid,
}

/// A joined consumer-group member and its durable range assignment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMemberRecord {
    pub generation: u64,
    /// Apply-index of the most recent join or heartbeat. Ephemeral liveness
    /// only — durable cursors outlive member expiry.
    pub last_heartbeat_apply_index: u64,
    pub assigned: Vec<RangeAssignment>,
}

/// Lineage-aware durable cursor checkpoint for one group/topic/range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorCheckpointRecord {
    pub topic_epoch: u64,
    /// The range's [`RangeRecord::lineage_generation`] the cursor was
    /// committed under — not the record's CAS `generation`.
    pub range_generation: u64,
    pub segment_uuid: Uuid,
    pub segment_generation: u64,
    pub segment_root: [u8; 32],
    pub record_offset: u64,
    pub record_index: u64,
    pub lineage_transition_id: Option<Uuid>,
    pub checkpoint_generation: u64,
    pub committed_by_member: Uuid,
}

/// Durable ordered replica set for a verified segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentPlacementRecord {
    pub generation: u64,
    /// Declared replication factor — the durability target. Preserved across
    /// every placement mutation: `replica_nodes.len()` may temporarily exceed
    /// it while a rebalance intent is in flight (RF + 1) and drops below it
    /// only after a plain retirement, pending repair. It is never inferred
    /// from the list length.
    pub declared_replication_factor: u8,
    pub replica_nodes: Vec<Uuid>,
    pub committed_apply_index: u64,
}

/// The single in-flight rebalance move for a segment. Present from
/// `ProposeRebalance` until `ConfirmReplicaRetired` completes the move (or
/// `CancelRebalance` abandons it); its presence blocks a second proposal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebalanceIntentRecord {
    pub from_node_uuid: Uuid,
    pub to_node_uuid: Uuid,
    pub proposed_at_apply_index: u64,
    pub placement_generation_at_proposal: u64,
}

/// Authenticated evidence that a replacement replica matches sealed identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplacementProofRecord {
    pub generation: u64,
    pub segment_generation: u64,
    pub content_root: [u8; 32],
    pub expected_length_bytes: u64,
    pub source_node_uuid: Uuid,
    pub destination_node_uuid: Uuid,
    pub fencing_epoch: u64,
    pub verification_method: VerificationMethod,
    pub verifier_node_uuid: Uuid,
    pub verified_at_apply_index: u64,
    pub verified_term: u64,
}

/// Verified cold-tier copy of a sealed segment. One per segment in this
/// slice; committed only after out-of-band upload plus read-back verification
/// of the authenticated content root. There is no `verified` flag: the state
/// machine records only verified facts (an unverified upload is external,
/// idempotent, and re-runnable).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TierCopyRecord {
    /// CAS token, starts at 0.
    pub generation: u64,
    /// Segment generation pinned at commit time, like `ReplacementProof`.
    pub segment_generation: u64,
    /// Must equal `SegmentRecord.content_root` at commit and at plan time.
    pub content_root: [u8; 32],
    pub byte_length: u64,
    /// 1..=[`MAX_TIER_BACKEND_ID_BYTES`] bytes ("s3-native", "localfs", ...).
    pub backend_id: String,
    /// 1..=[`MAX_TIER_OBJECT_URI_BYTES`] bytes; the manifest object is
    /// `object_uri + ".manifest.json"` by convention.
    pub object_uri: String,
    /// Immutable version of the segment object itself. Without this pin a
    /// later overwrite of `object_uri` could redirect rehydration after local
    /// replicas have been retired. Stays `Option` for stored-record and
    /// legacy-snapshot compatibility; `plan_retention` refuses deletion
    /// authority on unpinned evidence unless the topic's explicit
    /// unarchived-deletion policy opts out.
    pub object_version_id: Option<String>,
    /// 0..=[`MAX_TIER_VERSION_ID_BYTES`] bytes; the #135 immutable-version pin.
    pub manifest_version_id: Option<String>,
    /// Pins the canonical v2 manifest bytes with the commit statement
    /// stripped (the verifier's manifest-digest pin).
    pub manifest_core_digest: [u8; 32],
    /// Only `AuthenticatedContentRoot` may authorize retention.
    pub verification_method: VerificationMethod,
    pub verifier_node_uuid: Uuid,
    pub verified_at_apply_index: u64,
    pub verified_term: u64,
    pub fencing_epoch: u64,
}

/// Per-topic retention policy. Minimal and clock-free: the deterministic
/// state machine cannot judge age or size, so retention is proposed
/// externally and this record only widens (never replaces) the evidence gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicRetentionPolicyRecord {
    pub generation: u64,
    /// Explicit operator opt-out: sealed segments of this topic may be
    /// retention-planned WITHOUT tier evidence. An absent record means false.
    pub unarchived_deletion_allowed: bool,
}

/// A typed record value stored under an encoded [`MetaKey`].
/// How an epoch came to be minted (#240 item 5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrantKind {
    /// [`MetadataCommand::AcquireRangeLease`]: a candidate won the election.
    Election,
    /// [`MetadataCommand::GrantRangeLease`]: an operator named the holder.
    Administrative,
}

/// What the holder reported about its grant, if anything yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransitionOutcome {
    /// Minted, never reported on. An epoch that was granted and never
    /// served under looks exactly like this — deliberately visible rather
    /// than absent, so a gap in the chain can only ever mean a missing
    /// record, never a legitimate silence.
    Pending,
    Reported {
        outcome: PromotionOutcome,
        reported_at_ms: i64,
        reported_apply_index: u64,
    },
}

/// One epoch transition of a range (#240 item 5): minted by the state
/// machine at the moment the epoch is, so the chain of a range's records is
/// gapless by construction — every grant has one, whether or not anyone
/// ever served under it. The evidence the promotion computed (the fenced
/// quorum, the boundary adopted, the §5.4.1 vote) arrives afterwards from
/// the holder and is kept here rather than logged and lost.
///
/// What it asserts is checkable afterwards by anyone holding the record and
/// the replicas: `epoch_to > epoch_from` and the chain has no gaps; the
/// boundary is at or below what a majority in the quorum reported; the vote
/// recomputes from the quorum. What it does NOT claim, stated plainly: it
/// prevents nothing, it does not attest the outgoing holder's final state
/// (`holder_from` is who metadata believed held the range, usually dead by
/// then), and an unfenced replica's offset never appears in it.
///
/// The canonical encoding — [`MetaValue::encode`] — is the signing input
/// for the transition statement; a MAC over these bytes is computed where
/// the record is served, not inside `apply`, because the replicated state
/// machine must stay deterministic without a secret every voter shares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeTransitionRecord {
    pub epoch_from: u64,
    pub epoch_to: u64,
    /// Who metadata believed held the range when the epoch was minted;
    /// `None` when nobody did.
    pub holder_from: Option<Uuid>,
    pub holder_to: Uuid,
    pub grant: GrantKind,
    /// The granting command's `issued_at_ms` — data in the replicated log,
    /// never a local clock.
    pub granted_at_ms: i64,
    pub granted_apply_index: u64,
    pub outcome: TransitionOutcome,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaValue {
    Node(NodeRecord),
    Topic(TopicRecord),
    TopicName(TopicNameRecord),
    Range(RangeRecord),
    Segment(SegmentRecord),
    Key(KeyRecord),
    Group(ConsumerGroupRecord),
    GroupName(GroupNameRecord),
    GroupMember(GroupMemberRecord),
    GroupCursor(CursorCheckpointRecord),
    SegmentPlacement(SegmentPlacementRecord),
    ReplacementProof(ReplacementProofRecord),
    RebalanceIntent(RebalanceIntentRecord),
    TierCopy(TierCopyRecord),
    TopicRetentionPolicy(TopicRetentionPolicyRecord),
    RangeTransition(RangeTransitionRecord),
}

impl MetaValue {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::with_capacity(64);
        match self {
            MetaValue::Node(node) => {
                put_u8(&mut out, VALUE_TAG_NODE_V2);
                put_bounded_str(&mut out, &node.addr, MAX_NODE_ADDR_BYTES, "node address")?;
                put_u8(&mut out, node_state_tag(node.state));
                put_u64(&mut out, node.generation);
                put_bounded_str(
                    &mut out,
                    &node.failure_domain,
                    MAX_FAILURE_DOMAIN_BYTES,
                    "failure domain",
                )?;
                put_u32(&mut out, node.placement_weight);
            }
            MetaValue::Topic(topic) => {
                put_u8(&mut out, VALUE_TAG_TOPIC);
                put_bounded_str(
                    &mut out,
                    &topic.name,
                    crate::keys::MAX_TOPIC_NAME_BYTES,
                    "topic name",
                )?;
                put_u64(&mut out, topic.topic_epoch);
                put_u64(&mut out, topic.generation);
            }
            MetaValue::TopicName(name) => {
                put_u8(&mut out, VALUE_TAG_TOPIC_NAME);
                put_uuid(&mut out, name.topic_uuid);
                put_u64(&mut out, name.latest_epoch);
            }
            MetaValue::Range(range) => {
                // A range that has never seen a lineage transition encodes as
                // the legacy tag byte-for-byte, so pinned snapshot vectors and
                // mixed-version replicas stay stable; the v2 tag appears only
                // once a transition actually bumps `lineage_generation`.
                if range.lineage_generation == 0 {
                    put_u8(&mut out, VALUE_TAG_RANGE);
                } else {
                    put_u8(&mut out, VALUE_TAG_RANGE_V2);
                }
                put_u64(&mut out, range.generation);
                put_u64(&mut out, range.key_prefix);
                put_u8(&mut out, range.key_prefix_bits);
                put_u64(&mut out, range.fencing_epoch);
                if range.lineage_generation != 0 {
                    put_u64(&mut out, range.lineage_generation);
                }
                // Presence byte 1 is the pre-#223 lease and is still emitted
                // whenever there is no deadline, so pinned snapshot vectors
                // and mixed-version replicas stay byte-exact. Tag 2 appears
                // only once a lease actually carries an expiry.
                match &range.lease {
                    None => put_u8(&mut out, 0),
                    Some(lease) => {
                        match lease.expires_at_ms {
                            None => put_u8(&mut out, 1),
                            Some(_) => put_u8(&mut out, 2),
                        }
                        put_uuid(&mut out, lease.holder_node_uuid);
                        put_u64(&mut out, lease.fencing_epoch);
                        put_u64(&mut out, lease.granted_apply_index);
                        if let Some(expires_at_ms) = lease.expires_at_ms {
                            put_i64(&mut out, expires_at_ms);
                        }
                    }
                }
            }
            MetaValue::Segment(segment) => {
                put_u8(&mut out, VALUE_TAG_SEGMENT);
                put_u64(&mut out, segment.segment_generation);
                put_u64(&mut out, segment.base_offset);
                put_u64(&mut out, segment.next_offset);
                put_bytes32(&mut out, &segment.content_root);
                put_u8(
                    &mut out,
                    match segment.state {
                        SegmentState::SealedUnverified => 1,
                        SegmentState::Verified => 2,
                        SegmentState::Repairing => 3,
                        SegmentState::RetirePlanned => 4,
                        SegmentState::Retired => 5,
                        SegmentState::Quarantined => 6,
                        SegmentState::RetentionPlanned => 7,
                        SegmentState::RetentionExpired => 8,
                    },
                );
                put_u64(&mut out, segment.sealed_by_epoch);
            }
            MetaValue::Key(key) => {
                put_u8(&mut out, VALUE_TAG_KEY);
                put_u16(&mut out, key.scheme);
                put_bytes32(&mut out, &key.public_material_digest);
                put_u8(
                    &mut out,
                    match key.state {
                        KeyState::Active => 1,
                    },
                );
            }
            MetaValue::Group(group) => {
                put_u8(&mut out, VALUE_TAG_GROUP);
                put_bounded_str(
                    &mut out,
                    &group.name,
                    crate::keys::MAX_GROUP_NAME_BYTES,
                    "group name",
                )?;
                put_u64(&mut out, group.generation);
            }
            MetaValue::GroupName(name) => {
                put_u8(&mut out, VALUE_TAG_GROUP_NAME);
                put_uuid(&mut out, name.group_uuid);
            }
            MetaValue::GroupMember(member) => {
                put_u8(&mut out, VALUE_TAG_GROUP_MEMBER_V2);
                put_u64(&mut out, member.generation);
                put_u64(&mut out, member.last_heartbeat_apply_index);
                encode_assigned_ranges(&mut out, &member.assigned)?;
            }
            MetaValue::GroupCursor(cursor) => {
                put_u8(&mut out, VALUE_TAG_GROUP_CURSOR);
                put_u64(&mut out, cursor.topic_epoch);
                put_u64(&mut out, cursor.range_generation);
                put_uuid(&mut out, cursor.segment_uuid);
                put_u64(&mut out, cursor.segment_generation);
                put_bytes32(&mut out, &cursor.segment_root);
                put_u64(&mut out, cursor.record_offset);
                put_u64(&mut out, cursor.record_index);
                match cursor.lineage_transition_id {
                    None => put_u8(&mut out, 0),
                    Some(id) => {
                        put_u8(&mut out, 1);
                        put_uuid(&mut out, id);
                    }
                }
                put_u64(&mut out, cursor.checkpoint_generation);
                put_uuid(&mut out, cursor.committed_by_member);
            }
            MetaValue::SegmentPlacement(placement) => {
                put_u8(&mut out, VALUE_TAG_SEGMENT_PLACEMENT);
                put_u64(&mut out, placement.generation);
                put_u8(&mut out, placement.declared_replication_factor);
                put_u64(&mut out, placement.committed_apply_index);
                encode_replica_nodes(&mut out, &placement.replica_nodes)?;
            }
            MetaValue::ReplacementProof(proof) => {
                put_u8(&mut out, VALUE_TAG_REPLACEMENT_PROOF);
                put_u64(&mut out, proof.generation);
                put_u64(&mut out, proof.segment_generation);
                put_bytes32(&mut out, &proof.content_root);
                put_u64(&mut out, proof.expected_length_bytes);
                put_uuid(&mut out, proof.source_node_uuid);
                put_uuid(&mut out, proof.destination_node_uuid);
                put_u64(&mut out, proof.fencing_epoch);
                put_u8(
                    &mut out,
                    match proof.verification_method {
                        VerificationMethod::AuthenticatedContentRoot => 1,
                    },
                );
                put_uuid(&mut out, proof.verifier_node_uuid);
                put_u64(&mut out, proof.verified_at_apply_index);
                put_u64(&mut out, proof.verified_term);
            }
            MetaValue::RebalanceIntent(intent) => {
                put_u8(&mut out, VALUE_TAG_REBALANCE_INTENT);
                put_uuid(&mut out, intent.from_node_uuid);
                put_uuid(&mut out, intent.to_node_uuid);
                put_u64(&mut out, intent.proposed_at_apply_index);
                put_u64(&mut out, intent.placement_generation_at_proposal);
            }
            MetaValue::TierCopy(tier) => {
                put_u8(&mut out, VALUE_TAG_SEGMENT_TIER_COPY);
                put_u64(&mut out, tier.generation);
                put_u64(&mut out, tier.segment_generation);
                put_bytes32(&mut out, &tier.content_root);
                put_u64(&mut out, tier.byte_length);
                put_bounded_str(
                    &mut out,
                    &tier.backend_id,
                    MAX_TIER_BACKEND_ID_BYTES,
                    "tier backend id",
                )?;
                put_bounded_str(
                    &mut out,
                    &tier.object_uri,
                    MAX_TIER_OBJECT_URI_BYTES,
                    "tier object uri",
                )?;
                match &tier.manifest_version_id {
                    None => put_u8(&mut out, 0),
                    Some(version_id) => {
                        put_u8(&mut out, 1);
                        put_bounded_str(
                            &mut out,
                            version_id,
                            MAX_TIER_VERSION_ID_BYTES,
                            "tier manifest version id",
                        )?;
                    }
                }
                put_bytes32(&mut out, &tier.manifest_core_digest);
                put_u8(
                    &mut out,
                    match tier.verification_method {
                        VerificationMethod::AuthenticatedContentRoot => 1,
                    },
                );
                put_uuid(&mut out, tier.verifier_node_uuid);
                put_u64(&mut out, tier.verified_at_apply_index);
                put_u64(&mut out, tier.verified_term);
                put_u64(&mut out, tier.fencing_epoch);
                if let Some(version_id) = &tier.object_version_id {
                    put_u8(&mut out, 1);
                    put_bounded_str(
                        &mut out,
                        version_id,
                        MAX_TIER_VERSION_ID_BYTES,
                        "tier object version id",
                    )?;
                }
            }
            MetaValue::RangeTransition(transition) => {
                put_u8(&mut out, VALUE_TAG_RANGE_TRANSITION);
                put_u64(&mut out, transition.epoch_from);
                put_u64(&mut out, transition.epoch_to);
                match transition.holder_from {
                    None => put_u8(&mut out, 0),
                    Some(holder) => {
                        put_u8(&mut out, 1);
                        put_uuid(&mut out, holder);
                    }
                }
                put_uuid(&mut out, transition.holder_to);
                put_u8(
                    &mut out,
                    match transition.grant {
                        GrantKind::Election => 1,
                        GrantKind::Administrative => 2,
                    },
                );
                put_i64(&mut out, transition.granted_at_ms);
                put_u64(&mut out, transition.granted_apply_index);
                match &transition.outcome {
                    TransitionOutcome::Pending => put_u8(&mut out, 0),
                    TransitionOutcome::Reported {
                        outcome,
                        reported_at_ms,
                        reported_apply_index,
                    } => {
                        put_u8(&mut out, 1);
                        encode_promotion_outcome(&mut out, outcome)?;
                        put_i64(&mut out, *reported_at_ms);
                        put_u64(&mut out, *reported_apply_index);
                    }
                }
            }
            MetaValue::TopicRetentionPolicy(policy) => {
                put_u8(&mut out, VALUE_TAG_TOPIC_RETENTION_POLICY);
                put_u64(&mut out, policy.generation);
                put_u8(&mut out, u8::from(policy.unarchived_deletion_allowed));
            }
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let tag = reader.u8("record value tag")?;
        let value = match tag {
            VALUE_TAG_NODE => MetaValue::Node(NodeRecord {
                addr: reader.bounded_str(MAX_NODE_ADDR_BYTES, "node address")?,
                state: NodeState::from_wire(reader.u8("node state")?)?,
                generation: reader.u64("node generation")?,
                // Pre-placement node records omit attrs; defaults keep them
                // registerable while requiring SetNodePlacementAttrs before
                // multi-replica distinct-domain placement can succeed.
                failure_domain: String::new(),
                placement_weight: DEFAULT_PLACEMENT_WEIGHT,
            }),
            VALUE_TAG_NODE_V2 => MetaValue::Node(NodeRecord {
                addr: reader.bounded_str(MAX_NODE_ADDR_BYTES, "node address")?,
                state: NodeState::from_wire(reader.u8("node state")?)?,
                generation: reader.u64("node generation")?,
                failure_domain: reader.bounded_str(MAX_FAILURE_DOMAIN_BYTES, "failure domain")?,
                placement_weight: reader.u32("placement weight")?,
            }),
            VALUE_TAG_TOPIC => MetaValue::Topic(TopicRecord {
                name: reader.bounded_str(crate::keys::MAX_TOPIC_NAME_BYTES, "topic name")?,
                topic_epoch: reader.u64("topic epoch")?,
                generation: reader.u64("topic generation")?,
            }),
            VALUE_TAG_TOPIC_NAME => MetaValue::TopicName(TopicNameRecord {
                topic_uuid: reader.uuid("topic uuid")?,
                latest_epoch: reader.u64("latest topic epoch")?,
            }),
            VALUE_TAG_RANGE | VALUE_TAG_RANGE_V2 => {
                let generation = reader.u64("range generation")?;
                let key_prefix = reader.u64("key prefix")?;
                let key_prefix_bits = reader.u8("key prefix bits")?;
                let fencing_epoch = reader.u64("fencing epoch")?;
                // Legacy range records predate lineage transitions, so their
                // lineage version is the initial one. The v2 tag exists only
                // for transitioned ranges; a zero there is non-canonical (it
                // would re-encode as the legacy tag) and must be rejected.
                let lineage_generation = if tag == VALUE_TAG_RANGE_V2 {
                    let lineage_generation = reader.u64("lineage generation")?;
                    if lineage_generation == 0 {
                        return Err(CodecError::InvalidValue {
                            what: "range lineage generation",
                            reason: "v2 range records must carry a nonzero lineage generation",
                        });
                    }
                    lineage_generation
                } else {
                    0
                };
                // 0 = no lease, 1 = lease with no deadline (the pre-#223
                // encoding, still emitted for administrative grants), 2 =
                // lease carrying an expiry. Anything else is a record this
                // build cannot interpret, and guessing at it would be worse
                // than refusing.
                let lease = match reader.u8("lease presence")? {
                    0 => None,
                    presence @ (1 | 2) => Some(LeaseRecord {
                        holder_node_uuid: reader.uuid("lease holder uuid")?,
                        fencing_epoch: reader.u64("lease fencing epoch")?,
                        granted_apply_index: reader.u64("lease apply index")?,
                        expires_at_ms: match presence {
                            2 => Some(reader.i64("lease expiry ms")?),
                            _ => None,
                        },
                    }),
                    _ => {
                        return Err(CodecError::InvalidValue {
                            what: "lease presence",
                            reason: "lease presence byte must be 0, 1, or 2",
                        })
                    }
                };
                MetaValue::Range(RangeRecord {
                    generation,
                    key_prefix,
                    key_prefix_bits,
                    fencing_epoch,
                    lineage_generation,
                    lease,
                })
            }
            VALUE_TAG_SEGMENT => MetaValue::Segment(SegmentRecord {
                segment_generation: reader.u64("segment generation")?,
                base_offset: reader.u64("base offset")?,
                next_offset: reader.u64("next offset")?,
                content_root: reader.bytes32("content root")?,
                state: match reader.u8("segment state")? {
                    1 => SegmentState::SealedUnverified,
                    2 => SegmentState::Verified,
                    3 => SegmentState::Repairing,
                    4 => SegmentState::RetirePlanned,
                    5 => SegmentState::Retired,
                    6 => SegmentState::Quarantined,
                    7 => SegmentState::RetentionPlanned,
                    8 => SegmentState::RetentionExpired,
                    other => {
                        return Err(CodecError::UnknownTag {
                            what: "segment state",
                            tag: u32::from(other),
                        });
                    }
                },
                sealed_by_epoch: reader.u64("sealed-by epoch")?,
            }),
            VALUE_TAG_KEY => MetaValue::Key(KeyRecord {
                scheme: reader.u16("key scheme")?,
                public_material_digest: reader.bytes32("public material digest")?,
                state: match reader.u8("key state")? {
                    1 => KeyState::Active,
                    other => {
                        return Err(CodecError::UnknownTag {
                            what: "key state",
                            tag: u32::from(other),
                        });
                    }
                },
            }),
            VALUE_TAG_GROUP => MetaValue::Group(ConsumerGroupRecord {
                name: reader.bounded_str(crate::keys::MAX_GROUP_NAME_BYTES, "group name")?,
                generation: reader.u64("group generation")?,
            }),
            VALUE_TAG_GROUP_NAME => MetaValue::GroupName(GroupNameRecord {
                group_uuid: reader.uuid("group uuid")?,
            }),
            VALUE_TAG_GROUP_MEMBER => MetaValue::GroupMember(GroupMemberRecord {
                generation: reader.u64("member generation")?,
                // Pre-heartbeat member records omit liveness; treat as never
                // heartbeated so ExpireStaleMember can reclaim them.
                last_heartbeat_apply_index: 0,
                assigned: decode_assigned_ranges(&mut reader)?,
            }),
            VALUE_TAG_GROUP_MEMBER_V2 => MetaValue::GroupMember(GroupMemberRecord {
                generation: reader.u64("member generation")?,
                last_heartbeat_apply_index: reader.u64("member last heartbeat apply index")?,
                assigned: decode_assigned_ranges(&mut reader)?,
            }),
            VALUE_TAG_GROUP_CURSOR => {
                let topic_epoch = reader.u64("topic epoch")?;
                let range_generation = reader.u64("range generation")?;
                let segment_uuid = reader.uuid("segment uuid")?;
                let segment_generation = reader.u64("segment generation")?;
                let segment_root = reader.bytes32("segment root")?;
                let record_offset = reader.u64("record offset")?;
                let record_index = reader.u64("record index")?;
                let lineage_transition_id = if reader.flag("lineage transition presence")? {
                    Some(reader.uuid("lineage transition id")?)
                } else {
                    None
                };
                MetaValue::GroupCursor(CursorCheckpointRecord {
                    topic_epoch,
                    range_generation,
                    segment_uuid,
                    segment_generation,
                    segment_root,
                    record_offset,
                    record_index,
                    lineage_transition_id,
                    checkpoint_generation: reader.u64("checkpoint generation")?,
                    committed_by_member: reader.uuid("committed-by member")?,
                })
            }
            VALUE_TAG_SEGMENT_PLACEMENT => MetaValue::SegmentPlacement(SegmentPlacementRecord {
                generation: reader.u64("placement generation")?,
                declared_replication_factor: reader.u8("replication factor")?,
                committed_apply_index: reader.u64("placement apply index")?,
                replica_nodes: decode_replica_nodes(&mut reader)?,
            }),
            VALUE_TAG_REPLACEMENT_PROOF => MetaValue::ReplacementProof(ReplacementProofRecord {
                generation: reader.u64("proof generation")?,
                segment_generation: reader.u64("proof segment generation")?,
                content_root: reader.bytes32("proof content root")?,
                expected_length_bytes: reader.u64("proof expected length")?,
                source_node_uuid: reader.uuid("proof source node")?,
                destination_node_uuid: reader.uuid("proof destination node")?,
                fencing_epoch: reader.u64("proof fencing epoch")?,
                verification_method: VerificationMethod::from_wire(
                    reader.u8("proof verification method")?,
                )?,
                verifier_node_uuid: reader.uuid("proof verifier node")?,
                verified_at_apply_index: reader.u64("proof verified apply index")?,
                verified_term: reader.u64("proof verified term")?,
            }),
            VALUE_TAG_REBALANCE_INTENT => MetaValue::RebalanceIntent(RebalanceIntentRecord {
                from_node_uuid: reader.uuid("rebalance source node")?,
                to_node_uuid: reader.uuid("rebalance destination node")?,
                proposed_at_apply_index: reader.u64("rebalance proposed apply index")?,
                placement_generation_at_proposal: reader
                    .u64("rebalance placement generation at proposal")?,
            }),
            VALUE_TAG_SEGMENT_TIER_COPY => {
                let generation = reader.u64("tier generation")?;
                let segment_generation = reader.u64("tier segment generation")?;
                let content_root = reader.bytes32("tier content root")?;
                let byte_length = reader.u64("tier byte length")?;
                if byte_length == 0 {
                    return Err(CodecError::InvalidValue {
                        what: "tier byte length",
                        reason: "must be > 0",
                    });
                }
                let backend_id =
                    reader.bounded_str(MAX_TIER_BACKEND_ID_BYTES, "tier backend id")?;
                if backend_id.is_empty() {
                    return Err(CodecError::InvalidValue {
                        what: "tier backend id",
                        reason: "must not be empty",
                    });
                }
                let object_uri =
                    reader.bounded_str(MAX_TIER_OBJECT_URI_BYTES, "tier object uri")?;
                if object_uri.is_empty() {
                    return Err(CodecError::InvalidValue {
                        what: "tier object uri",
                        reason: "must not be empty",
                    });
                }
                let manifest_version_id = if reader.flag("tier manifest version presence")? {
                    Some(
                        reader
                            .bounded_str(MAX_TIER_VERSION_ID_BYTES, "tier manifest version id")?,
                    )
                } else {
                    None
                };
                let manifest_core_digest = reader.bytes32("tier manifest core digest")?;
                let verification_method =
                    VerificationMethod::from_wire(reader.u8("tier verification method")?)?;
                let verifier_node_uuid = reader.uuid("tier verifier node")?;
                let verified_at_apply_index = reader.u64("tier verified apply index")?;
                let verified_term = reader.u64("tier verified term")?;
                let fencing_epoch = reader.u64("tier fencing epoch")?;
                let object_version_id = if reader.remaining() == 0 {
                    None
                } else if reader.flag("tier object version presence")? {
                    Some(reader.bounded_str(MAX_TIER_VERSION_ID_BYTES, "tier object version id")?)
                } else {
                    return Err(CodecError::InvalidValue {
                        what: "tier object version presence",
                        reason: "legacy None must omit the extension",
                    });
                };
                MetaValue::TierCopy(TierCopyRecord {
                    generation,
                    segment_generation,
                    content_root,
                    byte_length,
                    backend_id,
                    object_uri,
                    object_version_id,
                    manifest_version_id,
                    manifest_core_digest,
                    verification_method,
                    verifier_node_uuid,
                    verified_at_apply_index,
                    verified_term,
                    fencing_epoch,
                })
            }
            VALUE_TAG_RANGE_TRANSITION => MetaValue::RangeTransition(RangeTransitionRecord {
                epoch_from: reader.u64("transition epoch from")?,
                epoch_to: reader.u64("transition epoch to")?,
                holder_from: if reader.flag("transition holder-from presence")? {
                    Some(reader.uuid("transition holder from")?)
                } else {
                    None
                },
                holder_to: reader.uuid("transition holder to")?,
                grant: match reader.u8("transition grant kind")? {
                    1 => GrantKind::Election,
                    2 => GrantKind::Administrative,
                    other => {
                        return Err(CodecError::UnknownTag {
                            what: "transition grant kind",
                            tag: u32::from(other),
                        })
                    }
                },
                granted_at_ms: reader.i64("transition granted at")?,
                granted_apply_index: reader.u64("transition granted apply index")?,
                outcome: if reader.flag("transition outcome presence")? {
                    TransitionOutcome::Reported {
                        outcome: decode_promotion_outcome(&mut reader)?,
                        reported_at_ms: reader.i64("transition reported at")?,
                        reported_apply_index: reader.u64("transition reported apply index")?,
                    }
                } else {
                    TransitionOutcome::Pending
                },
            }),
            VALUE_TAG_TOPIC_RETENTION_POLICY => {
                MetaValue::TopicRetentionPolicy(TopicRetentionPolicyRecord {
                    generation: reader.u64("retention policy generation")?,
                    unarchived_deletion_allowed: reader.flag("unarchived deletion allowed")?,
                })
            }
            other => {
                return Err(CodecError::UnknownTag {
                    what: "record value tag",
                    tag: u32::from(other),
                });
            }
        };
        reader.finish()?;
        Ok(value)
    }
}

fn node_state_tag(state: NodeState) -> u8 {
    match state {
        NodeState::Active => 1,
        NodeState::Draining => 2,
        NodeState::Dead => 3,
    }
}

/// The deterministic metadata state machine.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetaStateMachine {
    records: BTreeMap<Vec<u8>, MetaValue>,
    dedup_order: VecDeque<Uuid>,
    dedup_responses: HashMap<Uuid, MetadataResponse>,
}

impl MetaStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up a record by typed key.
    pub fn record(&self, key: &MetaKey) -> Option<&MetaValue> {
        self.records.get(&key.encode())
    }

    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    pub fn dedup_len(&self) -> usize {
        self.dedup_order.len()
    }

    /// Apply one command at `apply_index`. Pure and deterministic: identical
    /// (state, index, command) triples always produce identical responses
    /// and identical successor states on every replica.
    pub fn apply(&mut self, apply_index: u64, command: &MetadataCommand) -> MetadataResponse {
        let request_id = command.envelope().request_id;
        if let Some(original) = self.dedup_responses.get(&request_id) {
            return original.clone();
        }
        let response = self.apply_inner(apply_index, command);
        self.remember(request_id, response.clone());
        response
    }

    fn remember(&mut self, request_id: Uuid, response: MetadataResponse) {
        if self.dedup_order.len() == DEDUP_CAPACITY {
            if let Some(evicted) = self.dedup_order.pop_front() {
                self.dedup_responses.remove(&evicted);
            }
        }
        self.dedup_order.push_back(request_id);
        self.dedup_responses.insert(request_id, response);
    }

    fn apply_inner(&mut self, apply_index: u64, command: &MetadataCommand) -> MetadataResponse {
        // Check order is fixed and part of the contract: existence, then
        // epoch fencing, then generation CAS, then semantic guards.
        match command {
            MetadataCommand::RegisterNode {
                node_uuid,
                addr,
                expected_generation,
                ..
            } => self.register_node(*node_uuid, addr, *expected_generation),
            MetadataCommand::SetNodeState {
                node_uuid,
                state,
                expected_generation,
                ..
            } => self.set_node_state(*node_uuid, *state, *expected_generation),
            MetadataCommand::CreateTopic {
                name,
                topic_uuid,
                root_range_uuid,
                ..
            } => self.create_topic(name, *topic_uuid, *root_range_uuid),
            MetadataCommand::GrantRangeLease {
                env,
                topic_uuid,
                range_uuid,
                holder_node_uuid,
                expected_range_generation,
            } => self.grant_range_lease(
                apply_index,
                env.issued_at_ms,
                *topic_uuid,
                *range_uuid,
                *holder_node_uuid,
                *expected_range_generation,
            ),
            MetadataCommand::ReportPromotionOutcome {
                env,
                topic_uuid,
                range_uuid,
                holder_node_uuid,
                fencing_epoch,
                outcome,
            } => self.report_promotion_outcome(
                apply_index,
                env.issued_at_ms,
                *topic_uuid,
                *range_uuid,
                *holder_node_uuid,
                *fencing_epoch,
                outcome,
            ),
            MetadataCommand::AcquireRangeLease {
                env,
                topic_uuid,
                range_uuid,
                holder_node_uuid,
                expected_range_generation,
                lease_duration_ms,
            } => self.acquire_range_lease(
                apply_index,
                env.issued_at_ms,
                *topic_uuid,
                *range_uuid,
                *holder_node_uuid,
                *expected_range_generation,
                *lease_duration_ms,
            ),
            MetadataCommand::RenewRangeLease {
                env,
                topic_uuid,
                range_uuid,
                holder_node_uuid,
                expected_fencing_epoch,
                lease_duration_ms,
            } => self.renew_range_lease(
                env.issued_at_ms,
                *topic_uuid,
                *range_uuid,
                *holder_node_uuid,
                *expected_fencing_epoch,
                *lease_duration_ms,
            ),
            MetadataCommand::ReleaseRangeLease {
                topic_uuid,
                range_uuid,
                expected_fencing_epoch,
                ..
            } => self.release_range_lease(*topic_uuid, *range_uuid, *expected_fencing_epoch),
            MetadataCommand::RegisterSealedSegment {
                topic_uuid,
                range_uuid,
                segment_uuid,
                segment_generation,
                base_offset,
                next_offset,
                content_root,
                sealed_by_epoch,
                expected_range_generation,
                ..
            } => self.register_sealed_segment(
                *topic_uuid,
                *range_uuid,
                *segment_uuid,
                *segment_generation,
                *base_offset,
                *next_offset,
                *content_root,
                *sealed_by_epoch,
                *expected_range_generation,
            ),
            MetadataCommand::MarkSegmentVerified {
                topic_uuid,
                range_uuid,
                segment_uuid,
                content_root,
                expected_generation,
                ..
            } => self.mark_segment_verified(
                *topic_uuid,
                *range_uuid,
                *segment_uuid,
                *content_root,
                *expected_generation,
            ),
            MetadataCommand::PutKeyRecord {
                key_uuid,
                scheme,
                public_material_digest,
                ..
            } => self.put_key_record(*key_uuid, *scheme, *public_material_digest),
            MetadataCommand::CreateConsumerGroup {
                name, group_uuid, ..
            } => self.create_consumer_group(name, *group_uuid),
            MetadataCommand::JoinConsumerGroup {
                group_uuid,
                member_uuid,
                expected_group_generation,
                ..
            } => self.join_consumer_group(
                apply_index,
                *group_uuid,
                *member_uuid,
                *expected_group_generation,
            ),
            MetadataCommand::LeaveConsumerGroup {
                group_uuid,
                member_uuid,
                expected_member_generation,
                ..
            } => self.leave_consumer_group(*group_uuid, *member_uuid, *expected_member_generation),
            MetadataCommand::AssignMemberRanges {
                group_uuid,
                member_uuid,
                ranges,
                expected_member_generation,
                ..
            } => self.assign_member_ranges(
                *group_uuid,
                *member_uuid,
                ranges,
                *expected_member_generation,
            ),
            MetadataCommand::CommitGroupCursor {
                group_uuid,
                member_uuid,
                topic_uuid,
                range_uuid,
                topic_epoch,
                range_generation,
                segment_uuid,
                segment_generation,
                segment_root,
                record_offset,
                record_index,
                lineage_transition_id,
                expected_checkpoint_generation,
                ..
            } => self.commit_group_cursor(CommitCursorArgs {
                group_uuid: *group_uuid,
                member_uuid: *member_uuid,
                topic_uuid: *topic_uuid,
                range_uuid: *range_uuid,
                topic_epoch: *topic_epoch,
                range_generation: *range_generation,
                segment_uuid: *segment_uuid,
                segment_generation: *segment_generation,
                segment_root: *segment_root,
                record_offset: *record_offset,
                record_index: *record_index,
                lineage_transition_id: *lineage_transition_id,
                expected_checkpoint_generation: *expected_checkpoint_generation,
            }),
            MetadataCommand::HeartbeatMember {
                group_uuid,
                member_uuid,
                ..
            } => self.heartbeat_member(apply_index, *group_uuid, *member_uuid),
            MetadataCommand::ExpireStaleMember {
                group_uuid,
                member_uuid,
                stale_before_apply_index,
                ..
            } => self.expire_stale_member(*group_uuid, *member_uuid, *stale_before_apply_index),
            MetadataCommand::SetNodePlacementAttrs {
                node_uuid,
                failure_domain,
                placement_weight,
                expected_generation,
                ..
            } => self.set_node_placement_attrs(
                *node_uuid,
                failure_domain,
                *placement_weight,
                *expected_generation,
            ),
            MetadataCommand::CommitSegmentPlacement {
                topic_uuid,
                range_uuid,
                segment_uuid,
                replication_factor,
                replica_nodes,
                expected_segment_generation,
                expected_placement_generation,
                ..
            } => self.commit_segment_placement(
                apply_index,
                *topic_uuid,
                *range_uuid,
                *segment_uuid,
                *replication_factor,
                replica_nodes,
                *expected_segment_generation,
                *expected_placement_generation,
            ),
            MetadataCommand::CommitReplacementProof {
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation,
                content_root,
                expected_length_bytes,
                source_node_uuid,
                destination_node_uuid,
                fencing_epoch,
                verification_method,
                verifier_node_uuid,
                verified_term,
                ..
            } => self.commit_replacement_proof(
                apply_index,
                CommitReplacementProofArgs {
                    topic_uuid: *topic_uuid,
                    range_uuid: *range_uuid,
                    segment_uuid: *segment_uuid,
                    expected_segment_generation: *expected_segment_generation,
                    content_root: *content_root,
                    expected_length_bytes: *expected_length_bytes,
                    source_node_uuid: *source_node_uuid,
                    destination_node_uuid: *destination_node_uuid,
                    fencing_epoch: *fencing_epoch,
                    verification_method: *verification_method,
                    verifier_node_uuid: *verifier_node_uuid,
                    verified_term: *verified_term,
                },
            ),
            MetadataCommand::PlanReplicaRetirement {
                topic_uuid,
                range_uuid,
                segment_uuid,
                retiring_node_uuid,
                expected_segment_generation,
                fencing_epoch,
                ..
            } => self.plan_replica_retirement(
                *topic_uuid,
                *range_uuid,
                *segment_uuid,
                *retiring_node_uuid,
                *expected_segment_generation,
                *fencing_epoch,
            ),
            MetadataCommand::ConfirmReplicaRetired {
                topic_uuid,
                range_uuid,
                segment_uuid,
                retiring_node_uuid,
                expected_segment_generation,
                ..
            } => self.confirm_replica_retired(
                apply_index,
                *topic_uuid,
                *range_uuid,
                *segment_uuid,
                *retiring_node_uuid,
                *expected_segment_generation,
            ),
            MetadataCommand::ProposeRebalance {
                topic_uuid,
                range_uuid,
                segment_uuid,
                from_node_uuid,
                to_node_uuid,
                expected_placement_generation,
                ..
            } => self.propose_rebalance(
                apply_index,
                *topic_uuid,
                *range_uuid,
                *segment_uuid,
                *from_node_uuid,
                *to_node_uuid,
                *expected_placement_generation,
            ),
            MetadataCommand::CancelRebalance {
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_placement_generation,
                ..
            } => self.cancel_rebalance(
                apply_index,
                *topic_uuid,
                *range_uuid,
                *segment_uuid,
                *expected_placement_generation,
            ),
            MetadataCommand::CommitTierEvidence {
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation,
                content_root,
                byte_length,
                backend_id,
                object_uri,
                object_version_id,
                manifest_version_id,
                manifest_core_digest,
                verification_method,
                verifier_node_uuid,
                fencing_epoch,
                verified_term,
                ..
            } => self.commit_tier_evidence(
                apply_index,
                CommitTierEvidenceArgs {
                    topic_uuid: *topic_uuid,
                    range_uuid: *range_uuid,
                    segment_uuid: *segment_uuid,
                    expected_segment_generation: *expected_segment_generation,
                    content_root: *content_root,
                    byte_length: *byte_length,
                    backend_id: backend_id.clone(),
                    object_uri: object_uri.clone(),
                    object_version_id: object_version_id.clone(),
                    manifest_version_id: manifest_version_id.clone(),
                    manifest_core_digest: *manifest_core_digest,
                    verification_method: *verification_method,
                    verifier_node_uuid: *verifier_node_uuid,
                    fencing_epoch: *fencing_epoch,
                    verified_term: *verified_term,
                },
            ),
            MetadataCommand::SetTopicRetentionPolicy {
                topic_uuid,
                unarchived_deletion_allowed,
                expected_generation,
                ..
            } => self.set_topic_retention_policy(
                *topic_uuid,
                *unarchived_deletion_allowed,
                *expected_generation,
            ),
            MetadataCommand::PlanRetention {
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation,
                fencing_epoch,
                ..
            } => self.plan_retention(
                *topic_uuid,
                *range_uuid,
                *segment_uuid,
                *expected_segment_generation,
                *fencing_epoch,
            ),
            MetadataCommand::ConfirmRetentionExpired {
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation,
                ..
            } => self.confirm_retention_expired(
                apply_index,
                *topic_uuid,
                *range_uuid,
                *segment_uuid,
                *expected_segment_generation,
            ),
            MetadataCommand::CancelRetention {
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation,
                ..
            } => self.cancel_retention(
                *topic_uuid,
                *range_uuid,
                *segment_uuid,
                *expected_segment_generation,
            ),
        }
    }

    fn register_node(
        &mut self,
        node_uuid: Uuid,
        addr: &str,
        expected_generation: Option<u64>,
    ) -> MetadataResponse {
        if addr.is_empty() || addr.len() > MAX_NODE_ADDR_BYTES {
            return reject(MetadataError::limit(format!(
                "node address must be 1..={MAX_NODE_ADDR_BYTES} bytes, got {}",
                addr.len()
            )));
        }
        let key = MetaKey::Node { node_uuid }.encode();
        match (self.records.get_mut(&key), expected_generation) {
            (None, None) => {
                self.records.insert(
                    key,
                    MetaValue::Node(NodeRecord {
                        addr: addr.to_owned(),
                        state: NodeState::Active,
                        generation: 0,
                        failure_domain: String::new(),
                        placement_weight: DEFAULT_PLACEMENT_WEIGHT,
                    }),
                );
                MetadataResponse::Ack { generation: 0 }
            }
            (None, Some(_)) => reject(MetadataError::NotFound),
            (Some(_), None) => reject(MetadataError::AlreadyExists),
            (Some(MetaValue::Node(node)), Some(expected)) => {
                if node.generation != expected {
                    return reject(MetadataError::GenerationMismatch {
                        expected,
                        actual: node.generation,
                    });
                }
                node.addr = addr.to_owned();
                node.state = NodeState::Active;
                node.generation += 1;
                MetadataResponse::Ack {
                    generation: node.generation,
                }
            }
            (Some(_), Some(_)) => unreachable!("node keys only ever hold node records"),
        }
    }

    fn set_node_state(
        &mut self,
        node_uuid: Uuid,
        target: NodeState,
        expected_generation: u64,
    ) -> MetadataResponse {
        let key = MetaKey::Node { node_uuid }.encode();
        let Some(MetaValue::Node(node)) = self.records.get_mut(&key) else {
            return reject(MetadataError::NotFound);
        };
        if node.generation != expected_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_generation,
                actual: node.generation,
            });
        }
        // Guarded transitions, vtop-core style: Dead is terminal (rejoining
        // is RegisterNode's CAS path), and same-state writes are rejected so
        // a lost-then-retried command cannot silently burn a generation.
        let allowed = matches!(
            (node.state, target),
            (NodeState::Active, NodeState::Draining)
                | (NodeState::Active, NodeState::Dead)
                | (NodeState::Draining, NodeState::Active)
                | (NodeState::Draining, NodeState::Dead)
        );
        if !allowed {
            return reject(MetadataError::invalid_transition(format!(
                "node state {} -> {} is not allowed",
                node.state, target
            )));
        }
        node.state = target;
        node.generation += 1;
        MetadataResponse::Ack {
            generation: node.generation,
        }
    }

    fn create_topic(
        &mut self,
        name: &str,
        topic_uuid: Uuid,
        root_range_uuid: Uuid,
    ) -> MetadataResponse {
        if validate_topic_name(name).is_err() {
            return reject(MetadataError::limit(format!(
                "topic name must be 1..={} bytes, got {}",
                crate::keys::MAX_TOPIC_NAME_BYTES,
                name.len()
            )));
        }
        let topic_key = MetaKey::Topic { topic_uuid }.encode();
        if self.records.contains_key(&topic_key) {
            return reject(MetadataError::AlreadyExists);
        }
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid: root_range_uuid,
        }
        .encode();
        if self.records.contains_key(&range_key) {
            return reject(MetadataError::AlreadyExists);
        }
        // Epoch allocation is the one piece of state the SM computes itself:
        // the highest epoch ever used for this name, plus one. Recreating a
        // name therefore fences every earlier incarnation, which is why the
        // name record survives and is rebound rather than treated as a
        // conflict.
        let name_key = MetaKey::TopicByName {
            name: name.to_owned(),
        }
        .encode();
        let prior_epoch = match self.records.get(&name_key) {
            Some(MetaValue::TopicName(record)) => record.latest_epoch,
            Some(_) => unreachable!("topic-name keys only ever hold name records"),
            None => 0,
        };
        let topic_epoch = prior_epoch + 1;
        self.records.insert(
            name_key,
            MetaValue::TopicName(TopicNameRecord {
                topic_uuid,
                latest_epoch: topic_epoch,
            }),
        );
        self.records.insert(
            topic_key,
            MetaValue::Topic(TopicRecord {
                name: name.to_owned(),
                topic_epoch,
                generation: 0,
            }),
        );
        // The root range covers the full key interval: prefix 0 with 0
        // prefix bits, generation 0, no fencing or lineage history yet.
        self.records.insert(
            range_key,
            MetaValue::Range(RangeRecord {
                generation: 0,
                key_prefix: 0,
                key_prefix_bits: 0,
                fencing_epoch: 0,
                lineage_generation: 0,
                lease: None,
            }),
        );
        MetadataResponse::TopicCreated {
            topic_uuid,
            topic_epoch,
            root_range_uuid,
        }
    }

    fn grant_range_lease(
        &mut self,
        apply_index: u64,
        now_ms: i64,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        holder_node_uuid: Uuid,
        expected_range_generation: u64,
    ) -> MetadataResponse {
        match self.record(&MetaKey::Node {
            node_uuid: holder_node_uuid,
        }) {
            None => return reject(MetadataError::NotFound),
            Some(MetaValue::Node(node)) => {
                if node.state != NodeState::Active {
                    return reject(MetadataError::invalid_transition(format!(
                        "lease holder {holder_node_uuid} is {}, not active",
                        node.state
                    )));
                }
            }
            Some(_) => unreachable!("node keys only ever hold node records"),
        }
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get_mut(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        if range.generation != expected_range_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_range_generation,
                actual: range.generation,
            });
        }
        // Strict monotonicity is the fencing invariant: a grant always mints
        // a fresh, higher epoch, even when it steals the lease from a live
        // holder.
        let Some(fencing_epoch) = range.fencing_epoch.checked_add(1) else {
            return reject(MetadataError::limit("fencing epoch space is exhausted"));
        };
        let epoch_from = range.fencing_epoch;
        let holder_from = range.lease.as_ref().map(|lease| lease.holder_node_uuid);
        range.fencing_epoch = fencing_epoch;
        range.lease = Some(LeaseRecord {
            holder_node_uuid,
            fencing_epoch,
            granted_apply_index: apply_index,
            // Administrative grants never expire: an operator asked for this
            // holder explicitly, and silently handing the range to an election
            // later would undo their decision.
            expires_at_ms: None,
        });
        range.generation += 1;
        self.mint_transition(
            topic_uuid,
            range_uuid,
            RangeTransitionRecord {
                epoch_from,
                epoch_to: fencing_epoch,
                holder_from,
                holder_to: holder_node_uuid,
                grant: GrantKind::Administrative,
                granted_at_ms: now_ms,
                granted_apply_index: apply_index,
                outcome: TransitionOutcome::Pending,
            },
        );
        MetadataResponse::LeaseGranted { fencing_epoch }
    }

    /// Record the transition an epoch mint IS (#240 item 5), in the same
    /// apply as the mint, so the chain can never have a legitimate gap: a
    /// missing record means a missing record.
    fn mint_transition(
        &mut self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        transition: RangeTransitionRecord,
    ) {
        let key = MetaKey::RangeTransition {
            topic_uuid,
            range_uuid,
            fencing_epoch: transition.epoch_to,
        }
        .encode();
        self.records
            .insert(key, MetaValue::RangeTransition(transition));
    }

    /// The holder of an epoch reports what its promotion established or why
    /// it stood aside (#240 item 5).
    ///
    /// Argument list mirrors the command's fields, as every other apply
    /// method here does.
    #[allow(clippy::too_many_arguments)]
    fn report_promotion_outcome(
        &mut self,
        apply_index: u64,
        now_ms: i64,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        holder_node_uuid: Uuid,
        fencing_epoch: u64,
        outcome: &PromotionOutcome,
    ) -> MetadataResponse {
        if let PromotionOutcome::Established { quorum, .. } = outcome {
            if quorum.len() > MAX_TRANSITION_QUORUM {
                return reject(MetadataError::limit(format!(
                    "a promotion report may carry at most {MAX_TRANSITION_QUORUM} quorum answers"
                )));
            }
        }
        let key = MetaKey::RangeTransition {
            topic_uuid,
            range_uuid,
            fencing_epoch,
        }
        .encode();
        let Some(MetaValue::RangeTransition(transition)) = self.records.get_mut(&key) else {
            return reject(MetadataError::NotFound);
        };
        // Only the node the epoch was granted to has evidence about it; a
        // report from anyone else is at best confusion and at worst a
        // forgery, and either way not this record's to keep.
        if transition.holder_to != holder_node_uuid {
            return reject(MetadataError::invalid_transition(format!(
                "epoch {fencing_epoch} was granted to {}, not {holder_node_uuid}",
                transition.holder_to
            )));
        }
        // An established transition is final: what a quorum proved cannot
        // be rewritten by a later report. A refusal may be superseded — a
        // quorum miss is retryable, and the retry that succeeds at the same
        // epoch is exactly the report that should stand.
        if let TransitionOutcome::Reported {
            outcome: PromotionOutcome::Established { .. },
            ..
        } = &transition.outcome
        {
            return reject(MetadataError::invalid_transition(format!(
                "epoch {fencing_epoch} is already recorded as established; an established \
                 transition is final"
            )));
        }
        transition.outcome = TransitionOutcome::Reported {
            outcome: outcome.clone(),
            reported_at_ms: now_ms,
            reported_apply_index: apply_index,
        };
        MetadataResponse::TransitionRecorded { fencing_epoch }
    }

    /// A range's transition chain from `from_epoch` upward, at most `limit`
    /// records, in epoch order (#240 item 5).
    pub fn range_transitions(
        &self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        from_epoch: u64,
        limit: usize,
    ) -> Vec<RangeTransitionRecord> {
        let start = MetaKey::RangeTransition {
            topic_uuid,
            range_uuid,
            fencing_epoch: from_epoch,
        }
        .encode();
        let end = MetaKey::RangeTransition {
            topic_uuid,
            range_uuid,
            fencing_epoch: u64::MAX,
        }
        .encode();
        self.records
            .range(start..=end)
            .take(limit)
            .filter_map(|(_, value)| match value {
                MetaValue::RangeTransition(transition) => Some(transition.clone()),
                _ => None,
            })
            .collect()
    }

    /// Election path (#223): take the lease unless someone else still holds a
    /// live one.
    ///
    /// Argument list mirrors the command's fields, as every other apply method
    /// here does; grouping them into a struct would only move the same list one
    /// indirection away from the dispatch that builds it.
    #[allow(clippy::too_many_arguments)]
    fn acquire_range_lease(
        &mut self,
        apply_index: u64,
        now_ms: i64,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        holder_node_uuid: Uuid,
        expected_range_generation: u64,
        lease_duration_ms: u64,
    ) -> MetadataResponse {
        if lease_duration_ms == 0 {
            return reject(MetadataError::invalid_transition(
                "lease duration must be greater than zero; a zero-length lease \
                 would expire before its holder could renew it",
            ));
        }
        match self.record(&MetaKey::Node {
            node_uuid: holder_node_uuid,
        }) {
            None => return reject(MetadataError::NotFound),
            Some(MetaValue::Node(node)) => {
                if node.state != NodeState::Active {
                    return reject(MetadataError::invalid_transition(format!(
                        "lease candidate {holder_node_uuid} is {}, not active",
                        node.state
                    )));
                }
            }
            Some(_) => unreachable!("node keys only ever hold node records"),
        }
        let Some(expires_at_ms) = now_ms.checked_add_unsigned(lease_duration_ms) else {
            return reject(MetadataError::limit(
                "lease deadline overflows the representable time range",
            ));
        };
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get_mut(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        if range.generation != expected_range_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_range_generation,
                actual: range.generation,
            });
        }
        // A live lease held by SOMEONE ELSE is refused. This is politeness,
        // not safety — the epoch mint below would fence the old holder anyway
        // — but without it any candidate could take a healthy leader's range
        // at will and the cluster would flap.
        //
        // The holder renewing through this path is allowed: it is how a leader
        // that lost track of its own epoch recovers, and it still pays a new
        // epoch for the privilege.
        if let Some(existing) = range.lease.as_ref() {
            // An administrative grant is off-limits to elections entirely —
            // even to its own holder. Letting the holder "re-acquire" it would
            // trade the operator's permanent, deadline-less lease for an
            // expiring one, which a rival could then take once it lapses. The
            // same reasoning already forbids renewing such a lease; the only
            // ways out are an explicit release or a fresh administrative grant.
            if existing.expires_at_ms.is_none() {
                return reject(MetadataError::invalid_transition(
                    "range holds an administrative lease with no deadline; it cannot \
                     be acquired by election, only released or re-granted",
                ));
            }
            if existing.holder_node_uuid != holder_node_uuid && existing.is_live_at(now_ms) {
                return reject(MetadataError::invalid_transition(format!(
                    "range lease is held by {} and is still live",
                    existing.holder_node_uuid
                )));
            }
        }
        // Strict monotonicity is the fencing invariant: acquisition always
        // mints a fresh, higher epoch, so the previous holder is fenced by
        // construction rather than by any timing assumption.
        let Some(fencing_epoch) = range.fencing_epoch.checked_add(1) else {
            return reject(MetadataError::limit("fencing epoch space is exhausted"));
        };
        let epoch_from = range.fencing_epoch;
        let holder_from = range.lease.as_ref().map(|lease| lease.holder_node_uuid);
        range.fencing_epoch = fencing_epoch;
        range.lease = Some(LeaseRecord {
            holder_node_uuid,
            fencing_epoch,
            granted_apply_index: apply_index,
            expires_at_ms: Some(expires_at_ms),
        });
        range.generation += 1;
        self.mint_transition(
            topic_uuid,
            range_uuid,
            RangeTransitionRecord {
                epoch_from,
                epoch_to: fencing_epoch,
                holder_from,
                holder_to: holder_node_uuid,
                grant: GrantKind::Election,
                granted_at_ms: now_ms,
                granted_apply_index: apply_index,
                outcome: TransitionOutcome::Pending,
            },
        );
        MetadataResponse::LeaseGranted { fencing_epoch }
    }

    /// Extend the current holder's deadline without minting a new epoch
    /// (#223), so a live leader keeps serving across renewals.
    fn renew_range_lease(
        &mut self,
        now_ms: i64,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        holder_node_uuid: Uuid,
        expected_fencing_epoch: u64,
        lease_duration_ms: u64,
    ) -> MetadataResponse {
        if lease_duration_ms == 0 {
            return reject(MetadataError::invalid_transition(
                "lease duration must be greater than zero",
            ));
        }
        let Some(expires_at_ms) = now_ms.checked_add_unsigned(lease_duration_ms) else {
            return reject(MetadataError::limit(
                "lease deadline overflows the representable time range",
            ));
        };
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get_mut(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        let Some(lease) = range.lease.as_ref() else {
            return reject(MetadataError::invalid_transition(
                "range holds no lease to renew",
            ));
        };
        // Both identity AND epoch must match. Checking only identity would let
        // a partitioned old leader — already fenced by a newer grant to itself
        // after a restart — keep extending a lease it no longer holds.
        if lease.holder_node_uuid != holder_node_uuid {
            return reject(MetadataError::invalid_transition(format!(
                "range lease is held by {}, not {holder_node_uuid}",
                lease.holder_node_uuid
            )));
        }
        if lease.fencing_epoch != expected_fencing_epoch {
            return reject(MetadataError::EpochMismatch {
                expected: expected_fencing_epoch,
                actual: lease.fencing_epoch,
            });
        }
        // An administrative grant has no deadline and must not acquire one:
        // renewing it would convert an operator's explicit, permanent choice
        // into an expiring lease that an election could take later — exactly
        // the outcome `GrantRangeLease` exists to prevent.
        let Some(current_deadline) = lease.expires_at_ms else {
            return reject(MetadataError::invalid_transition(
                "range holds an administrative lease with no deadline; it cannot be \
                 renewed, only released or re-granted",
            ));
        };
        // An ALREADY-EXPIRED lease cannot be renewed back to life. Without
        // this a stale holder could keep pushing the deadline forward forever
        // and postpone takeover indefinitely — never advancing the epoch, so
        // no rival could ever win. That would defeat the whole point of
        // expiry. Once a lease lapses the only way back is acquisition, which
        // mints a new epoch and fences whoever held it before.
        if now_ms >= current_deadline {
            return reject(MetadataError::invalid_transition(format!(
                "range lease expired at {current_deadline}; acquire it instead of \
                 renewing"
            )));
        }
        // The holder must still be a node the cluster considers usable. Grant
        // and acquire both check this; renewal skipping it would let a node
        // marked Dead hold its range forever through heartbeats alone.
        match self.record(&MetaKey::Node {
            node_uuid: holder_node_uuid,
        }) {
            None => return reject(MetadataError::NotFound),
            Some(MetaValue::Node(node)) => {
                if node.state != NodeState::Active {
                    return reject(MetadataError::invalid_transition(format!(
                        "lease holder {holder_node_uuid} is {}, not active",
                        node.state
                    )));
                }
            }
            Some(_) => unreachable!("node keys only ever hold node records"),
        }
        // Never shorten: a renewal that arrived out of order behind a longer
        // one must not pull the deadline back in and trigger an election.
        let extended = current_deadline.max(expires_at_ms);
        let Some(MetaValue::Range(range)) = self.records.get_mut(&range_key) else {
            unreachable!("the range was present a moment ago")
        };
        let Some(lease) = range.lease.as_mut() else {
            unreachable!("the lease was present a moment ago")
        };
        lease.expires_at_ms = Some(extended);
        // Deliberately NOT bumping `range.generation`. A renewal changes
        // neither the holder nor the epoch — nothing another command's CAS
        // token could be stale ABOUT — and it returns no generation for the
        // caller to re-learn. Bumping here would make steady heartbeats
        // silently invalidate the leader's own in-flight range CAS operations
        // (a `RegisterSealedSegment` prepared before the renewal landed would
        // be refused with a GenerationMismatch it could never anticipate).
        MetadataResponse::LeaseGranted {
            fencing_epoch: expected_fencing_epoch,
        }
    }

    fn release_range_lease(
        &mut self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        expected_fencing_epoch: u64,
    ) -> MetadataResponse {
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get_mut(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        if range.fencing_epoch != expected_fencing_epoch {
            return reject(MetadataError::EpochMismatch {
                expected: expected_fencing_epoch,
                actual: range.fencing_epoch,
            });
        }
        if range.lease.is_none() {
            return reject(MetadataError::invalid_transition(
                "range holds no lease to release",
            ));
        }
        // Release clears the lease but never rewinds the fencing epoch.
        range.lease = None;
        range.generation += 1;
        MetadataResponse::Ack {
            generation: range.generation,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn register_sealed_segment(
        &mut self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        segment_generation: u64,
        base_offset: u64,
        next_offset: u64,
        content_root: [u8; 32],
        sealed_by_epoch: u64,
        expected_range_generation: u64,
    ) -> MetadataResponse {
        let segment_key = MetaKey::Segment {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        // Sealing is an act of the current leaseholder. Without a live
        // lease there is no authority to publish at all: a fresh range and
        // a just-released range both sit at a "matching" epoch with no
        // holder, and neither may accept a segment.
        let Some(lease) = range.lease.as_ref() else {
            return reject(MetadataError::invalid_transition(
                "range holds no active lease to seal under",
            ));
        };
        // The epoch gate: a sealer fenced by a newer grant must not be able
        // to publish, however stale or fresh its CAS token is.
        if sealed_by_epoch != lease.fencing_epoch {
            return reject(MetadataError::EpochMismatch {
                expected: sealed_by_epoch,
                actual: lease.fencing_epoch,
            });
        }
        if range.generation != expected_range_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_range_generation,
                actual: range.generation,
            });
        }
        if next_offset < base_offset {
            return reject(MetadataError::invalid_transition(format!(
                "segment offsets regress: next {next_offset} < base {base_offset}"
            )));
        }
        if self.records.contains_key(&segment_key) {
            return reject(MetadataError::AlreadyExists);
        }
        self.records.insert(
            segment_key,
            MetaValue::Segment(SegmentRecord {
                segment_generation,
                base_offset,
                next_offset,
                content_root,
                state: SegmentState::SealedUnverified,
                sealed_by_epoch,
            }),
        );
        let Some(MetaValue::Range(range)) = self.records.get_mut(&range_key) else {
            unreachable!("range record was present above and apply is single-threaded");
        };
        range.generation += 1;
        MetadataResponse::Ack {
            generation: range.generation,
        }
    }

    fn mark_segment_verified(
        &mut self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        content_root: [u8; 32],
        expected_generation: u64,
    ) -> MetadataResponse {
        let segment_key = MetaKey::Segment {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get_mut(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        if segment.segment_generation != expected_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_generation,
                actual: segment.segment_generation,
            });
        }
        if segment.content_root != content_root {
            return reject(MetadataError::invalid_transition(
                "content root does not match the sealed segment",
            ));
        }
        if segment.state != SegmentState::SealedUnverified {
            return reject(MetadataError::invalid_transition(
                "verification requires SEALED_UNVERIFIED",
            ));
        }
        // The registration accepts any proposer-supplied generation, so the
        // ceiling must be rejected deterministically here rather than
        // wrapping (or panicking every replica in checked builds).
        let Some(next_generation) = segment.segment_generation.checked_add(1) else {
            return reject(MetadataError::limit(
                "segment generation space is exhausted",
            ));
        };
        segment.state = SegmentState::Verified;
        segment.segment_generation = next_generation;
        MetadataResponse::Ack {
            generation: next_generation,
        }
    }

    fn put_key_record(
        &mut self,
        key_uuid: Uuid,
        scheme: u16,
        public_material_digest: [u8; 32],
    ) -> MetadataResponse {
        let key = MetaKey::Key { key_uuid }.encode();
        if self.records.contains_key(&key) {
            return reject(MetadataError::AlreadyExists);
        }
        self.records.insert(
            key,
            MetaValue::Key(KeyRecord {
                scheme,
                public_material_digest,
                state: KeyState::Active,
            }),
        );
        MetadataResponse::Ack { generation: 0 }
    }

    fn create_consumer_group(&mut self, name: &str, group_uuid: Uuid) -> MetadataResponse {
        if validate_group_name(name).is_err() {
            return reject(MetadataError::limit(format!(
                "group name must be 1..={} bytes, got {}",
                crate::keys::MAX_GROUP_NAME_BYTES,
                name.len()
            )));
        }
        let group_key = MetaKey::Group { group_uuid }.encode();
        if self.records.contains_key(&group_key) {
            return reject(MetadataError::AlreadyExists);
        }
        let name_key = MetaKey::GroupByName {
            name: name.to_owned(),
        }
        .encode();
        if self.records.contains_key(&name_key) {
            return reject(MetadataError::AlreadyExists);
        }
        self.records.insert(
            name_key,
            MetaValue::GroupName(GroupNameRecord { group_uuid }),
        );
        self.records.insert(
            group_key,
            MetaValue::Group(ConsumerGroupRecord {
                name: name.to_owned(),
                generation: 0,
            }),
        );
        MetadataResponse::GroupCreated {
            group_uuid,
            generation: 0,
        }
    }

    fn join_consumer_group(
        &mut self,
        apply_index: u64,
        group_uuid: Uuid,
        member_uuid: Uuid,
        expected_group_generation: u64,
    ) -> MetadataResponse {
        let group_key = MetaKey::Group { group_uuid }.encode();
        let Some(MetaValue::Group(group)) = self.records.get(&group_key) else {
            return reject(MetadataError::NotFound);
        };
        if group.generation != expected_group_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_group_generation,
                actual: group.generation,
            });
        }
        let member_key = MetaKey::GroupMember {
            group_uuid,
            member_uuid,
        }
        .encode();
        if self.records.contains_key(&member_key) {
            return reject(MetadataError::AlreadyExists);
        }
        let Some(next_group_generation) = group.generation.checked_add(1) else {
            return reject(MetadataError::limit("group generation space is exhausted"));
        };
        self.records.insert(
            member_key,
            MetaValue::GroupMember(GroupMemberRecord {
                generation: 0,
                last_heartbeat_apply_index: apply_index,
                assigned: Vec::new(),
            }),
        );
        let Some(MetaValue::Group(group)) = self.records.get_mut(&group_key) else {
            unreachable!("group record was present above");
        };
        group.generation = next_group_generation;
        MetadataResponse::MemberJoined {
            member_generation: 0,
            group_generation: next_group_generation,
        }
    }

    fn leave_consumer_group(
        &mut self,
        group_uuid: Uuid,
        member_uuid: Uuid,
        expected_member_generation: u64,
    ) -> MetadataResponse {
        let group_key = MetaKey::Group { group_uuid }.encode();
        let Some(MetaValue::Group(group)) = self.records.get(&group_key) else {
            return reject(MetadataError::NotFound);
        };
        let Some(next_group_generation) = group.generation.checked_add(1) else {
            return reject(MetadataError::limit("group generation space is exhausted"));
        };
        let member_key = MetaKey::GroupMember {
            group_uuid,
            member_uuid,
        }
        .encode();
        let Some(MetaValue::GroupMember(member)) = self.records.get(&member_key) else {
            return reject(MetadataError::NotFound);
        };
        if member.generation != expected_member_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_member_generation,
                actual: member.generation,
            });
        }
        self.records.remove(&member_key);
        let Some(MetaValue::Group(group)) = self.records.get_mut(&group_key) else {
            unreachable!("group record was present above");
        };
        group.generation = next_group_generation;
        MetadataResponse::Ack {
            generation: next_group_generation,
        }
    }

    fn assign_member_ranges(
        &mut self,
        group_uuid: Uuid,
        member_uuid: Uuid,
        ranges: &[RangeAssignment],
        expected_member_generation: u64,
    ) -> MetadataResponse {
        if ranges.len() > MAX_ASSIGNED_RANGES {
            return reject(MetadataError::limit(format!(
                "assigned ranges must be <= {MAX_ASSIGNED_RANGES}, got {}",
                ranges.len()
            )));
        }
        let mut seen = BTreeMap::new();
        for assignment in ranges {
            let range_key = MetaKey::Range {
                topic_uuid: assignment.topic_uuid,
                range_uuid: assignment.range_uuid,
            }
            .encode();
            if !matches!(self.records.get(&range_key), Some(MetaValue::Range(_))) {
                return reject(MetadataError::NotFound);
            }
            if seen
                .insert((assignment.topic_uuid, assignment.range_uuid), ())
                .is_some()
            {
                return reject(MetadataError::invalid_transition(
                    "assigned ranges contain a duplicate topic/range pair",
                ));
            }
        }
        // Exclusive ownership: a range may be assigned to at most one live
        // member of the group. Overlapping assignment during rebalance is
        // rejected rather than allowing concurrent cursor commits.
        for (key_bytes, value) in &self.records {
            let Ok(MetaKey::GroupMember {
                group_uuid: other_group,
                member_uuid: other_member,
            }) = MetaKey::decode(key_bytes)
            else {
                continue;
            };
            if other_group != group_uuid || other_member == member_uuid {
                continue;
            }
            let MetaValue::GroupMember(other) = value else {
                continue;
            };
            for assignment in ranges {
                if other.assigned.iter().any(|held| {
                    held.topic_uuid == assignment.topic_uuid
                        && held.range_uuid == assignment.range_uuid
                }) {
                    return reject(MetadataError::invalid_transition(
                        "range is already assigned to another group member",
                    ));
                }
            }
        }
        let group_key = MetaKey::Group { group_uuid }.encode();
        if !matches!(self.records.get(&group_key), Some(MetaValue::Group(_))) {
            return reject(MetadataError::NotFound);
        }
        let member_key = MetaKey::GroupMember {
            group_uuid,
            member_uuid,
        }
        .encode();
        let Some(MetaValue::GroupMember(member)) = self.records.get_mut(&member_key) else {
            return reject(MetadataError::NotFound);
        };
        if member.generation != expected_member_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_member_generation,
                actual: member.generation,
            });
        }
        let Some(next_member_generation) = member.generation.checked_add(1) else {
            return reject(MetadataError::limit("member generation space is exhausted"));
        };
        member.assigned = ranges.to_vec();
        member.generation = next_member_generation;
        let Some(MetaValue::Group(group)) = self.records.get_mut(&group_key) else {
            unreachable!("group record was present above");
        };
        let Some(next_group_generation) = group.generation.checked_add(1) else {
            return reject(MetadataError::limit("group generation space is exhausted"));
        };
        group.generation = next_group_generation;
        MetadataResponse::Ack {
            generation: next_member_generation,
        }
    }

    fn commit_group_cursor(&mut self, args: CommitCursorArgs) -> MetadataResponse {
        let group_key = MetaKey::Group {
            group_uuid: args.group_uuid,
        }
        .encode();
        if !matches!(self.records.get(&group_key), Some(MetaValue::Group(_))) {
            return reject(MetadataError::NotFound);
        }
        let member_key = MetaKey::GroupMember {
            group_uuid: args.group_uuid,
            member_uuid: args.member_uuid,
        }
        .encode();
        let Some(MetaValue::GroupMember(member)) = self.records.get(&member_key) else {
            return reject(MetadataError::NotFound);
        };
        let owns_range = member.assigned.iter().any(|assignment| {
            assignment.topic_uuid == args.topic_uuid && assignment.range_uuid == args.range_uuid
        });
        if !owns_range {
            return reject(MetadataError::invalid_transition(
                "member is not assigned the cursor topic/range",
            ));
        }

        let topic_key = MetaKey::Topic {
            topic_uuid: args.topic_uuid,
        }
        .encode();
        let Some(MetaValue::Topic(topic)) = self.records.get(&topic_key) else {
            return reject(MetadataError::NotFound);
        };
        if topic.topic_epoch != args.topic_epoch {
            return reject(MetadataError::EpochMismatch {
                expected: args.topic_epoch,
                actual: topic.topic_epoch,
            });
        }

        let range_key = MetaKey::Range {
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        let cursor_key = MetaKey::GroupCursor {
            group_uuid: args.group_uuid,
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
        }
        .encode();
        let existing_cursor = self.records.get(&cursor_key).cloned();
        let legacy_cursor_generation = matches!(
            &existing_cursor,
            Some(MetaValue::GroupCursor(existing))
                if range.lineage_generation == 0
                    && args.range_generation != 0
                    && existing.range_generation == args.range_generation
        );
        // The cursor pins the range's lineage version, not its CAS token:
        // unrelated metadata updates (grants, segment registrations) bump
        // `generation` without moving the key interval, and must not strand
        // an otherwise valid checkpoint. Snapshots from the pre-lineage
        // release stored that CAS token in existing cursor records; accept
        // exactly that previously committed value once, then normalize the
        // rewritten checkpoint to lineage generation zero.
        if range.lineage_generation != args.range_generation && !legacy_cursor_generation {
            return reject(MetadataError::LineageMismatch {
                expected: args.range_generation,
                actual: range.lineage_generation,
            });
        }
        let committed_range_generation = range.lineage_generation;

        let segment_key = MetaKey::Segment {
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
            segment_uuid: args.segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        if segment.segment_generation != args.segment_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: args.segment_generation,
                actual: segment.segment_generation,
            });
        }
        if segment.content_root != args.segment_root {
            return reject(MetadataError::invalid_transition(
                "segment root does not match the registered segment",
            ));
        }
        if args.record_offset < segment.base_offset || args.record_offset > segment.next_offset {
            return reject(MetadataError::invalid_transition(format!(
                "record offset {} is outside sealed segment [{}, {}]",
                args.record_offset, segment.base_offset, segment.next_offset
            )));
        }

        match (existing_cursor, args.expected_checkpoint_generation) {
            (None, None) => {
                self.records.insert(
                    cursor_key,
                    MetaValue::GroupCursor(CursorCheckpointRecord {
                        topic_epoch: args.topic_epoch,
                        range_generation: committed_range_generation,
                        segment_uuid: args.segment_uuid,
                        segment_generation: args.segment_generation,
                        segment_root: args.segment_root,
                        record_offset: args.record_offset,
                        record_index: args.record_index,
                        lineage_transition_id: args.lineage_transition_id,
                        checkpoint_generation: 0,
                        committed_by_member: args.member_uuid,
                    }),
                );
                MetadataResponse::CursorCommitted {
                    checkpoint_generation: 0,
                }
            }
            (None, Some(_)) => reject(MetadataError::NotFound),
            (Some(_), None) => reject(MetadataError::AlreadyExists),
            (Some(MetaValue::GroupCursor(existing)), Some(expected)) => {
                if existing.checkpoint_generation != expected {
                    return reject(MetadataError::GenerationMismatch {
                        expected,
                        actual: existing.checkpoint_generation,
                    });
                }
                if existing.topic_epoch != args.topic_epoch {
                    return reject(MetadataError::EpochMismatch {
                        expected: args.topic_epoch,
                        actual: existing.topic_epoch,
                    });
                }
                if let Err(error) = cursor_is_forward_or_equal(&existing, &args) {
                    return reject(error);
                }
                let Some(next_generation) = existing.checkpoint_generation.checked_add(1) else {
                    return reject(MetadataError::limit(
                        "checkpoint generation space is exhausted",
                    ));
                };
                self.records.insert(
                    cursor_key,
                    MetaValue::GroupCursor(CursorCheckpointRecord {
                        topic_epoch: args.topic_epoch,
                        range_generation: committed_range_generation,
                        segment_uuid: args.segment_uuid,
                        segment_generation: args.segment_generation,
                        segment_root: args.segment_root,
                        record_offset: args.record_offset,
                        record_index: args.record_index,
                        lineage_transition_id: args.lineage_transition_id,
                        checkpoint_generation: next_generation,
                        committed_by_member: args.member_uuid,
                    }),
                );
                MetadataResponse::CursorCommitted {
                    checkpoint_generation: next_generation,
                }
            }
            (Some(_), Some(_)) => unreachable!("cursor keys only hold cursor records"),
        }
    }

    fn heartbeat_member(
        &mut self,
        apply_index: u64,
        group_uuid: Uuid,
        member_uuid: Uuid,
    ) -> MetadataResponse {
        let group_key = MetaKey::Group { group_uuid }.encode();
        if !matches!(self.records.get(&group_key), Some(MetaValue::Group(_))) {
            return reject(MetadataError::NotFound);
        }
        let member_key = MetaKey::GroupMember {
            group_uuid,
            member_uuid,
        }
        .encode();
        let Some(MetaValue::GroupMember(member)) = self.records.get_mut(&member_key) else {
            return reject(MetadataError::NotFound);
        };
        // Heartbeats are monotonic in apply order and never rewind. A replayed
        // lower index cannot appear because apply indices advance, but guard
        // anyway so identical state machines stay locked.
        if apply_index < member.last_heartbeat_apply_index {
            return reject(MetadataError::invalid_transition(
                "heartbeat apply index would rewind member liveness",
            ));
        }
        member.last_heartbeat_apply_index = apply_index;
        MetadataResponse::Ack {
            generation: member.generation,
        }
    }

    fn expire_stale_member(
        &mut self,
        group_uuid: Uuid,
        member_uuid: Uuid,
        stale_before_apply_index: u64,
    ) -> MetadataResponse {
        let group_key = MetaKey::Group { group_uuid }.encode();
        let Some(MetaValue::Group(group)) = self.records.get(&group_key) else {
            return reject(MetadataError::NotFound);
        };
        let Some(next_group_generation) = group.generation.checked_add(1) else {
            return reject(MetadataError::limit("group generation space is exhausted"));
        };
        let member_key = MetaKey::GroupMember {
            group_uuid,
            member_uuid,
        }
        .encode();
        let Some(MetaValue::GroupMember(member)) = self.records.get(&member_key) else {
            return reject(MetadataError::NotFound);
        };
        if member.last_heartbeat_apply_index >= stale_before_apply_index {
            return reject(MetadataError::invalid_transition(
                "member heartbeat is still within the live window",
            ));
        }
        // Drop membership and assignment only. Durable cursors remain so a
        // replacement member can resume lineage-aware progress.
        self.records.remove(&member_key);
        let Some(MetaValue::Group(group)) = self.records.get_mut(&group_key) else {
            unreachable!("group record was present above");
        };
        group.generation = next_group_generation;
        MetadataResponse::Ack {
            generation: next_group_generation,
        }
    }

    fn set_node_placement_attrs(
        &mut self,
        node_uuid: Uuid,
        failure_domain: &str,
        placement_weight: u32,
        expected_generation: u64,
    ) -> MetadataResponse {
        if failure_domain.len() > MAX_FAILURE_DOMAIN_BYTES {
            return reject(MetadataError::limit(format!(
                "failure domain must be 0..={MAX_FAILURE_DOMAIN_BYTES} bytes, got {}",
                failure_domain.len()
            )));
        }
        if placement_weight < MIN_PLACEMENT_WEIGHT {
            return reject(MetadataError::limit(format!(
                "placement weight must be >= {MIN_PLACEMENT_WEIGHT}"
            )));
        }
        let key = MetaKey::Node { node_uuid }.encode();
        let Some(MetaValue::Node(node)) = self.records.get_mut(&key) else {
            return reject(MetadataError::NotFound);
        };
        if node.generation != expected_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_generation,
                actual: node.generation,
            });
        }
        node.failure_domain = failure_domain.to_owned();
        node.placement_weight = placement_weight;
        node.generation += 1;
        MetadataResponse::Ack {
            generation: node.generation,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_segment_placement(
        &mut self,
        apply_index: u64,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        replication_factor: u8,
        replica_nodes: &[Uuid],
        expected_segment_generation: u64,
        expected_placement_generation: Option<u64>,
    ) -> MetadataResponse {
        let rf = usize::from(replication_factor);
        if !(1..=MAX_REPLICAS).contains(&rf) {
            return reject(MetadataError::limit(format!(
                "replication factor must be 1..={MAX_REPLICAS}, got {replication_factor}"
            )));
        }
        // RF is an independent durability claim: never infer it from the list
        // length alone, so an undersized proposal cannot silently drop
        // intended redundancy.
        if replica_nodes.len() != rf {
            return reject(MetadataError::invalid_transition(format!(
                "replica set length {} does not match replication_factor {replication_factor}",
                replica_nodes.len()
            )));
        }
        let mut seen = replica_nodes.to_vec();
        seen.sort_unstable();
        seen.dedup();
        if seen.len() != replica_nodes.len() {
            return reject(MetadataError::invalid_transition(
                "replica set contains duplicate node ids",
            ));
        }

        let segment_key = MetaKey::Segment {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        if segment.segment_generation != expected_segment_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_segment_generation,
                actual: segment.segment_generation,
            });
        }
        if segment.state != SegmentState::Verified {
            return reject(MetadataError::invalid_transition(
                "placement requires a verified segment",
            ));
        }

        // A placement under an active rebalance intent is locked: re-running
        // the deterministic assignment would clobber the temporary RF + 1 set
        // and orphan the intent. Cancel or complete the move first.
        let intent_key = MetaKey::SegmentRebalanceIntent {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        if self.records.contains_key(&intent_key) {
            return reject(MetadataError::invalid_transition(
                "placement is locked by an active rebalance intent",
            ));
        }

        let candidates = self.active_placement_candidates();
        let require_distinct = rf > 1;
        let expected = match select_replicas(segment_uuid, &candidates, rf, require_distinct) {
            Ok(nodes) => nodes,
            Err(error) => {
                return reject(MetadataError::invalid_transition(error.to_string()));
            }
        };
        if replica_nodes != expected.as_slice() {
            return reject(MetadataError::invalid_transition(
                "proposed replica set does not match deterministic placement",
            ));
        }

        let placement_key = MetaKey::SegmentPlacement {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        match (
            self.records.get(&placement_key),
            expected_placement_generation,
        ) {
            (None, None) => {
                self.records.insert(
                    placement_key,
                    MetaValue::SegmentPlacement(SegmentPlacementRecord {
                        generation: 0,
                        declared_replication_factor: replication_factor,
                        replica_nodes: replica_nodes.to_vec(),
                        committed_apply_index: apply_index,
                    }),
                );
                MetadataResponse::Ack { generation: 0 }
            }
            (None, Some(_)) => reject(MetadataError::NotFound),
            (Some(_), None) => reject(MetadataError::AlreadyExists),
            (Some(MetaValue::SegmentPlacement(existing)), Some(expected_generation)) => {
                if existing.generation != expected_generation {
                    return reject(MetadataError::GenerationMismatch {
                        expected: expected_generation,
                        actual: existing.generation,
                    });
                }
                let Some(next_generation) = existing.generation.checked_add(1) else {
                    return reject(MetadataError::limit(
                        "placement generation space is exhausted",
                    ));
                };
                self.records.insert(
                    placement_key,
                    MetaValue::SegmentPlacement(SegmentPlacementRecord {
                        generation: next_generation,
                        declared_replication_factor: replication_factor,
                        replica_nodes: replica_nodes.to_vec(),
                        committed_apply_index: apply_index,
                    }),
                );
                MetadataResponse::Ack {
                    generation: next_generation,
                }
            }
            (Some(_), Some(_)) => {
                unreachable!("placement keys only ever hold placement records")
            }
        }
    }

    fn commit_replacement_proof(
        &mut self,
        apply_index: u64,
        args: CommitReplacementProofArgs,
    ) -> MetadataResponse {
        if args.source_node_uuid == args.destination_node_uuid {
            return reject(MetadataError::invalid_transition(
                "replacement source and destination must differ",
            ));
        }
        if args.expected_length_bytes == 0 {
            return reject(MetadataError::limit(
                "replacement expected length must be > 0",
            ));
        }
        // Only authenticated content-root verification may authorize retirement.
        if args.verification_method != VerificationMethod::AuthenticatedContentRoot {
            return reject(MetadataError::invalid_transition(
                "unsupported verification method for replacement proof",
            ));
        }

        let range_key = MetaKey::Range {
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        let Some(lease) = range.lease.as_ref() else {
            return reject(MetadataError::invalid_transition(
                "range holds no active lease for replacement proof",
            ));
        };
        if args.fencing_epoch != lease.fencing_epoch {
            return reject(MetadataError::EpochMismatch {
                expected: args.fencing_epoch,
                actual: lease.fencing_epoch,
            });
        }

        let segment_key = MetaKey::Segment {
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
            segment_uuid: args.segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        if segment.segment_generation != args.expected_segment_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: args.expected_segment_generation,
                actual: segment.segment_generation,
            });
        }
        if !matches!(
            segment.state,
            SegmentState::Verified | SegmentState::Repairing
        ) {
            return reject(MetadataError::invalid_transition(
                "replacement proof requires a verified or repairing segment",
            ));
        }
        if segment.content_root != args.content_root {
            return reject(MetadataError::invalid_transition(
                "replacement proof content root does not match sealed segment",
            ));
        }

        for node_uuid in [
            args.source_node_uuid,
            args.destination_node_uuid,
            args.verifier_node_uuid,
        ] {
            let node_key = MetaKey::Node { node_uuid }.encode();
            let Some(MetaValue::Node(node)) = self.records.get(&node_key) else {
                return reject(MetadataError::NotFound);
            };
            if node.state == NodeState::Dead {
                return reject(MetadataError::invalid_transition(
                    "replacement proof references a dead node",
                ));
            }
        }

        // During an active rebalance intent only the intended move may be
        // proven; an unrelated proof would let retirement race the rebalance
        // and strand the intent.
        let intent_key = MetaKey::SegmentRebalanceIntent {
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
            segment_uuid: args.segment_uuid,
        }
        .encode();
        if let Some(MetaValue::RebalanceIntent(intent)) = self.records.get(&intent_key) {
            if intent.from_node_uuid != args.source_node_uuid
                || intent.to_node_uuid != args.destination_node_uuid
            {
                return reject(MetadataError::invalid_transition(
                    "replacement proof does not match the active rebalance intent",
                ));
            }
        }

        let proof_key = MetaKey::SegmentReplacementProof {
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
            segment_uuid: args.segment_uuid,
        }
        .encode();
        let proof_generation = match self.records.get(&proof_key) {
            None => 0,
            Some(MetaValue::ReplacementProof(existing)) => {
                if existing.fencing_epoch == args.fencing_epoch {
                    return reject(MetadataError::AlreadyExists);
                }
                if existing.segment_generation != args.expected_segment_generation
                    || existing.content_root != args.content_root
                    || existing.expected_length_bytes != args.expected_length_bytes
                    || existing.source_node_uuid != args.source_node_uuid
                    || existing.destination_node_uuid != args.destination_node_uuid
                    || existing.verification_method != args.verification_method
                {
                    return reject(MetadataError::invalid_transition(
                        "stale replacement proof identity does not match the new proof",
                    ));
                }
                let Some(next_generation) = existing.generation.checked_add(1) else {
                    return reject(MetadataError::limit(
                        "replacement proof generation space is exhausted",
                    ));
                };
                next_generation
            }
            Some(_) => unreachable!("replacement-proof keys only hold proof records"),
        };
        self.records.insert(
            proof_key,
            MetaValue::ReplacementProof(ReplacementProofRecord {
                generation: proof_generation,
                segment_generation: args.expected_segment_generation,
                content_root: args.content_root,
                expected_length_bytes: args.expected_length_bytes,
                source_node_uuid: args.source_node_uuid,
                destination_node_uuid: args.destination_node_uuid,
                fencing_epoch: args.fencing_epoch,
                verification_method: args.verification_method,
                verifier_node_uuid: args.verifier_node_uuid,
                verified_at_apply_index: apply_index,
                verified_term: args.verified_term,
            }),
        );
        MetadataResponse::Ack {
            generation: proof_generation,
        }
    }

    fn plan_replica_retirement(
        &mut self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        retiring_node_uuid: Uuid,
        expected_segment_generation: u64,
        fencing_epoch: u64,
    ) -> MetadataResponse {
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        let Some(lease) = range.lease.as_ref() else {
            return reject(MetadataError::invalid_transition(
                "range holds no active lease for retirement planning",
            ));
        };
        if fencing_epoch != lease.fencing_epoch {
            return reject(MetadataError::EpochMismatch {
                expected: fencing_epoch,
                actual: lease.fencing_epoch,
            });
        }

        // While a rebalance intent is in flight, the only replica that may be
        // retired is the intent's source: retiring any other node would drop
        // the segment below its declared replication factor mid-move.
        let intent_key = MetaKey::SegmentRebalanceIntent {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        if let Some(MetaValue::RebalanceIntent(intent)) = self.records.get(&intent_key) {
            if intent.from_node_uuid != retiring_node_uuid {
                return reject(MetadataError::invalid_transition(
                    "segment has an active rebalance intent for a different node",
                ));
            }
        }

        let proof_key = MetaKey::SegmentReplacementProof {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::ReplacementProof(proof)) = self.records.get(&proof_key) else {
            return reject(MetadataError::invalid_transition(
                "replica retirement requires a committed replacement proof",
            ));
        };
        if proof.segment_generation != expected_segment_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_segment_generation,
                actual: proof.segment_generation,
            });
        }
        if proof.fencing_epoch != fencing_epoch {
            return reject(MetadataError::EpochMismatch {
                expected: fencing_epoch,
                actual: proof.fencing_epoch,
            });
        }
        if proof.destination_node_uuid == retiring_node_uuid {
            return reject(MetadataError::invalid_transition(
                "cannot retire the verified replacement destination",
            ));
        }
        if proof.source_node_uuid != retiring_node_uuid {
            return reject(MetadataError::invalid_transition(
                "retiring node is not the replacement proof source",
            ));
        }
        let proof_content_root = proof.content_root;
        let placement_key = MetaKey::SegmentPlacement {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::SegmentPlacement(placement)) = self.records.get(&placement_key) else {
            return reject(MetadataError::invalid_transition(
                "replica retirement requires a committed segment placement",
            ));
        };
        if !placement
            .replica_nodes
            .contains(&proof.destination_node_uuid)
        {
            return reject(MetadataError::invalid_transition(
                "placement does not contain the verified replacement destination",
            ));
        }
        if !placement.replica_nodes.contains(&retiring_node_uuid) {
            return reject(MetadataError::invalid_transition(
                "placement does not contain the retiring source replica",
            ));
        }

        let segment_key = MetaKey::Segment {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get_mut(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        if segment.segment_generation != expected_segment_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_segment_generation,
                actual: segment.segment_generation,
            });
        }
        if segment.content_root != proof_content_root {
            return reject(MetadataError::invalid_transition(
                "segment content root no longer matches replacement proof",
            ));
        }
        if segment.state != SegmentState::Verified {
            return reject(MetadataError::invalid_transition(
                "retirement planning requires a verified segment",
            ));
        }
        let Some(next_generation) = segment.segment_generation.checked_add(1) else {
            return reject(MetadataError::limit(
                "segment generation space is exhausted",
            ));
        };
        segment.state = SegmentState::RetirePlanned;
        segment.segment_generation = next_generation;
        MetadataResponse::Ack {
            generation: next_generation,
        }
    }

    fn confirm_replica_retired(
        &mut self,
        apply_index: u64,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        retiring_node_uuid: Uuid,
        expected_segment_generation: u64,
    ) -> MetadataResponse {
        let proof_key = MetaKey::SegmentReplacementProof {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::ReplacementProof(proof)) = self.records.get(&proof_key) else {
            return reject(MetadataError::invalid_transition(
                "replica retirement confirmation requires a committed replacement proof",
            ));
        };
        if proof.source_node_uuid != retiring_node_uuid {
            return reject(MetadataError::invalid_transition(
                "retiring node is not the replacement proof source",
            ));
        }
        let destination = proof.destination_node_uuid;

        // A rebalance intent matching the proof completes with this
        // confirmation: the intent record is removed in the same apply so the
        // whole move commits or rejects as one unit.
        let intent_key = MetaKey::SegmentRebalanceIntent {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let completes_rebalance = matches!(
            self.records.get(&intent_key),
            Some(MetaValue::RebalanceIntent(intent))
                if intent.from_node_uuid == retiring_node_uuid
                    && intent.to_node_uuid == destination
        );

        // Apply is all-or-nothing: validate every rejection path — segment
        // AND placement — before mutating either record, so a rejected
        // command never leaves the segment half-retired and unretryable.
        let segment_key = MetaKey::Segment {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        if segment.segment_generation != expected_segment_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_segment_generation,
                actual: segment.segment_generation,
            });
        }
        if segment.state != SegmentState::RetirePlanned {
            return reject(MetadataError::invalid_transition(
                "retirement confirmation requires RETIRE_PLANNED",
            ));
        }
        let Some(next_generation) = segment.segment_generation.checked_add(1) else {
            return reject(MetadataError::limit(
                "segment generation space is exhausted",
            ));
        };

        let placement_key = MetaKey::SegmentPlacement {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let (next_placement_generation, has_surviving_placement) =
            match self.records.get(&placement_key) {
                Some(MetaValue::SegmentPlacement(placement)) => {
                    if !placement.replica_nodes.contains(&destination) {
                        return reject(MetadataError::invalid_transition(
                            "placement no longer contains the verified replacement destination",
                        ));
                    }
                    if !placement.replica_nodes.contains(&retiring_node_uuid) {
                        return reject(MetadataError::invalid_transition(
                            "placement no longer contains the retiring source replica",
                        ));
                    }
                    let Some(next) = placement.generation.checked_add(1) else {
                        return reject(MetadataError::limit(
                            "placement generation space is exhausted",
                        ));
                    };
                    (Some(next), true)
                }
                _ => (None, false),
            };

        // Every rejection has been ruled out; mutate both records. Replica
        // retirement consumes its proof so a later repair/rebalance can
        // commit fresh evidence under the segment's new CAS generation.
        if let Some(MetaValue::Segment(segment)) = self.records.get_mut(&segment_key) {
            segment.state = if has_surviving_placement {
                SegmentState::Verified
            } else {
                SegmentState::Retired
            };
            segment.segment_generation = next_generation;
        }
        if let Some(next_placement_generation) = next_placement_generation {
            if let Some(MetaValue::SegmentPlacement(placement)) =
                self.records.get_mut(&placement_key)
            {
                // Drop the retiring replica; the verified destination remains.
                // The declared replication factor is a durability target and
                // is deliberately preserved — never rewritten from the list
                // length — so a shrunken set stays visibly below target.
                placement
                    .replica_nodes
                    .retain(|node| *node != retiring_node_uuid);
                placement.generation = next_placement_generation;
                placement.committed_apply_index = apply_index;
            }
        }
        if completes_rebalance {
            self.records.remove(&intent_key);
        }
        self.records.remove(&proof_key);

        MetadataResponse::Ack {
            generation: next_generation,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn propose_rebalance(
        &mut self,
        apply_index: u64,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        from_node_uuid: Uuid,
        to_node_uuid: Uuid,
        expected_placement_generation: u64,
    ) -> MetadataResponse {
        // Existence first: segment, placement, then the destination node.
        let segment_key = MetaKey::Segment {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        let placement_key = MetaKey::SegmentPlacement {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::SegmentPlacement(placement)) = self.records.get(&placement_key) else {
            return reject(MetadataError::NotFound);
        };
        let Some(MetaValue::Node(to_node)) = self.records.get(
            &MetaKey::Node {
                node_uuid: to_node_uuid,
            }
            .encode(),
        ) else {
            return reject(MetadataError::NotFound);
        };
        // Generation CAS on the placement record being mutated.
        if placement.generation != expected_placement_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_placement_generation,
                actual: placement.generation,
            });
        }
        // One in-flight rebalance per segment: a live intent blocks a second
        // proposal outright.
        let intent_key = MetaKey::SegmentRebalanceIntent {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        if self.records.contains_key(&intent_key) {
            return reject(MetadataError::AlreadyExists);
        }
        // Semantic guards, all validated before any mutation.
        if segment.state != SegmentState::Verified {
            return reject(MetadataError::invalid_transition(
                "rebalance requires a verified segment",
            ));
        }
        // A pre-existing proof belongs to a repair/retirement already under
        // way; the rebalance lifecycle must start before its proof.
        let proof_key = MetaKey::SegmentReplacementProof {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        if self.records.contains_key(&proof_key) {
            return reject(MetadataError::invalid_transition(
                "segment already has a committed replacement proof",
            ));
        }
        if from_node_uuid == to_node_uuid {
            return reject(MetadataError::invalid_transition(
                "rebalance source and destination must differ",
            ));
        }
        if !placement.replica_nodes.contains(&from_node_uuid) {
            return reject(MetadataError::invalid_transition(
                "rebalance source is not in the current placement",
            ));
        }
        if placement.replica_nodes.contains(&to_node_uuid) {
            return reject(MetadataError::invalid_transition(
                "rebalance destination is already in the placement",
            ));
        }
        if to_node.state != NodeState::Active {
            return reject(MetadataError::invalid_transition(format!(
                "rebalance destination {to_node_uuid} is {}, not active",
                to_node.state
            )));
        }
        if to_node.placement_weight < MIN_PLACEMENT_WEIGHT {
            return reject(MetadataError::invalid_transition(
                "rebalance destination has no placement weight",
            ));
        }
        // The temporary set may hold exactly one replica above the declared
        // bound (add-before-retire); a second concurrent expansion is
        // impossible because the intent record is exclusive.
        if placement.replica_nodes.len() >= MAX_TRANSIENT_REPLICAS {
            return reject(MetadataError::limit(format!(
                "placement already holds {MAX_TRANSIENT_REPLICAS} replicas"
            )));
        }
        // Preserve the distinct-failure-domain durability constraint that
        // deterministic placement enforced: when RF > 1, the destination
        // must not share a domain with any replica that SURVIVES the move
        // (the source is leaving, so its domain is free to reuse).
        if usize::from(placement.declared_replication_factor) > 1 {
            for survivor in placement
                .replica_nodes
                .iter()
                .filter(|node| **node != from_node_uuid)
            {
                let survivor_key = MetaKey::Node {
                    node_uuid: *survivor,
                }
                .encode();
                let Some(MetaValue::Node(survivor_node)) = self.records.get(&survivor_key) else {
                    return reject(MetadataError::invalid_transition(format!(
                        "surviving replica {survivor} has no node record"
                    )));
                };
                if survivor_node.failure_domain == to_node.failure_domain {
                    return reject(MetadataError::invalid_transition(format!(
                        "rebalance destination shares failure domain {:?} with surviving replica {survivor}",
                        to_node.failure_domain
                    )));
                }
            }
        }
        let Some(next_placement_generation) = placement.generation.checked_add(1) else {
            return reject(MetadataError::limit(
                "placement generation space is exhausted",
            ));
        };

        // Every rejection has been ruled out; record the intent and add the
        // destination so the segment never runs below its declared RF.
        self.records.insert(
            intent_key,
            MetaValue::RebalanceIntent(RebalanceIntentRecord {
                from_node_uuid,
                to_node_uuid,
                proposed_at_apply_index: apply_index,
                placement_generation_at_proposal: expected_placement_generation,
            }),
        );
        let Some(MetaValue::SegmentPlacement(placement)) = self.records.get_mut(&placement_key)
        else {
            unreachable!("placement record was present above and apply is single-threaded");
        };
        placement.replica_nodes.push(to_node_uuid);
        placement.generation = next_placement_generation;
        placement.committed_apply_index = apply_index;
        MetadataResponse::Ack {
            generation: next_placement_generation,
        }
    }

    fn cancel_rebalance(
        &mut self,
        apply_index: u64,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        expected_placement_generation: u64,
    ) -> MetadataResponse {
        let intent_key = MetaKey::SegmentRebalanceIntent {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::RebalanceIntent(intent)) = self.records.get(&intent_key) else {
            return reject(MetadataError::NotFound);
        };
        let intent = intent.clone();
        let placement_key = MetaKey::SegmentPlacement {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::SegmentPlacement(placement)) = self.records.get(&placement_key) else {
            return reject(MetadataError::NotFound);
        };
        if placement.generation != expected_placement_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_placement_generation,
                actual: placement.generation,
            });
        }
        // Once the move's replacement proof is committed the copy is verified
        // evidence; the move must complete via retirement, not be cancelled.
        let proof_key = MetaKey::SegmentReplacementProof {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        if let Some(MetaValue::ReplacementProof(proof)) = self.records.get(&proof_key) {
            if proof.source_node_uuid == intent.from_node_uuid
                && proof.destination_node_uuid == intent.to_node_uuid
            {
                return reject(MetadataError::invalid_transition(
                    "cannot cancel a rebalance whose replacement proof is committed",
                ));
            }
        }
        let Some(next_placement_generation) = placement.generation.checked_add(1) else {
            return reject(MetadataError::limit(
                "placement generation space is exhausted",
            ));
        };

        // Every rejection has been ruled out; drop the intent and the
        // destination replica it added (a no-op retain if an unrelated
        // retirement already removed it).
        self.records.remove(&intent_key);
        let Some(MetaValue::SegmentPlacement(placement)) = self.records.get_mut(&placement_key)
        else {
            unreachable!("placement record was present above and apply is single-threaded");
        };
        placement
            .replica_nodes
            .retain(|node| *node != intent.to_node_uuid);
        placement.generation = next_placement_generation;
        placement.committed_apply_index = apply_index;
        MetadataResponse::Ack {
            generation: next_placement_generation,
        }
    }

    fn commit_tier_evidence(
        &mut self,
        apply_index: u64,
        args: CommitTierEvidenceArgs,
    ) -> MetadataResponse {
        // Bounds first: the codec already rejects these, but apply re-checks
        // them so a hand-constructed command cannot bypass the invariants.
        if args.byte_length == 0 {
            return reject(MetadataError::limit("tier byte length must be > 0"));
        }
        if args.backend_id.is_empty() || args.backend_id.len() > MAX_TIER_BACKEND_ID_BYTES {
            return reject(MetadataError::limit(format!(
                "tier backend id must be 1..={MAX_TIER_BACKEND_ID_BYTES} bytes, got {}",
                args.backend_id.len()
            )));
        }
        if args.object_uri.is_empty() || args.object_uri.len() > MAX_TIER_OBJECT_URI_BYTES {
            return reject(MetadataError::limit(format!(
                "tier object uri must be 1..={MAX_TIER_OBJECT_URI_BYTES} bytes, got {}",
                args.object_uri.len()
            )));
        }
        if let Some(version_id) = &args.manifest_version_id {
            if version_id.len() > MAX_TIER_VERSION_ID_BYTES {
                return reject(MetadataError::limit(format!(
                    "tier manifest version id must be <= {MAX_TIER_VERSION_ID_BYTES} bytes, got {}",
                    version_id.len()
                )));
            }
        }
        if let Some(version_id) = &args.object_version_id {
            if version_id.is_empty() || version_id.len() > MAX_TIER_VERSION_ID_BYTES {
                return reject(MetadataError::limit(format!(
                    "tier object version id must be 1..={MAX_TIER_VERSION_ID_BYTES} bytes, got {}",
                    version_id.len()
                )));
            }
        }
        // Only authenticated content-root verification may become deletion
        // authority (the ADR rule: object-store replication by itself is not
        // VTOP verification).
        if args.verification_method != VerificationMethod::AuthenticatedContentRoot {
            return reject(MetadataError::invalid_transition(
                "unsupported verification method for tier evidence",
            ));
        }

        let range_key = MetaKey::Range {
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        let Some(lease) = range.lease.as_ref() else {
            return reject(MetadataError::invalid_transition(
                "range holds no active lease for tier evidence",
            ));
        };
        if args.fencing_epoch != lease.fencing_epoch {
            return reject(MetadataError::EpochMismatch {
                expected: args.fencing_epoch,
                actual: lease.fencing_epoch,
            });
        }

        let segment_key = MetaKey::Segment {
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
            segment_uuid: args.segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        if segment.segment_generation != args.expected_segment_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: args.expected_segment_generation,
                actual: segment.segment_generation,
            });
        }
        if segment.state != SegmentState::Verified {
            return reject(MetadataError::invalid_transition(
                "tier evidence requires a verified segment",
            ));
        }
        if segment.content_root != args.content_root {
            return reject(MetadataError::invalid_transition(
                "tier evidence content root does not match the sealed segment",
            ));
        }

        let verifier_key = MetaKey::Node {
            node_uuid: args.verifier_node_uuid,
        }
        .encode();
        let Some(MetaValue::Node(verifier)) = self.records.get(&verifier_key) else {
            return reject(MetadataError::NotFound);
        };
        if verifier.state == NodeState::Dead {
            return reject(MetadataError::invalid_transition(
                "tier evidence verifier node is dead",
            ));
        }

        let tier_key = MetaKey::SegmentTierCopy {
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
            segment_uuid: args.segment_uuid,
        }
        .encode();
        if self.records.contains_key(&tier_key) {
            return reject(MetadataError::AlreadyExists);
        }
        // The segment record itself is NOT mutated (same as
        // CommitReplacementProof), so the pinned generation still matches at
        // PlanRetention time unless something else moved the segment.
        self.records.insert(
            tier_key,
            MetaValue::TierCopy(TierCopyRecord {
                generation: 0,
                segment_generation: args.expected_segment_generation,
                content_root: args.content_root,
                byte_length: args.byte_length,
                backend_id: args.backend_id,
                object_uri: args.object_uri,
                object_version_id: args.object_version_id,
                manifest_version_id: args.manifest_version_id,
                manifest_core_digest: args.manifest_core_digest,
                verification_method: args.verification_method,
                verifier_node_uuid: args.verifier_node_uuid,
                verified_at_apply_index: apply_index,
                verified_term: args.verified_term,
                fencing_epoch: args.fencing_epoch,
            }),
        );
        MetadataResponse::Ack { generation: 0 }
    }

    fn set_topic_retention_policy(
        &mut self,
        topic_uuid: Uuid,
        unarchived_deletion_allowed: bool,
        expected_generation: Option<u64>,
    ) -> MetadataResponse {
        let topic_key = MetaKey::Topic { topic_uuid }.encode();
        if !matches!(self.records.get(&topic_key), Some(MetaValue::Topic(_))) {
            return reject(MetadataError::NotFound);
        }
        let policy_key = MetaKey::TopicRetentionPolicy { topic_uuid }.encode();
        match (self.records.get_mut(&policy_key), expected_generation) {
            (None, None) => {
                self.records.insert(
                    policy_key,
                    MetaValue::TopicRetentionPolicy(TopicRetentionPolicyRecord {
                        generation: 0,
                        unarchived_deletion_allowed,
                    }),
                );
                MetadataResponse::Ack { generation: 0 }
            }
            (None, Some(_)) => reject(MetadataError::NotFound),
            (Some(_), None) => reject(MetadataError::AlreadyExists),
            (Some(MetaValue::TopicRetentionPolicy(policy)), Some(expected)) => {
                if policy.generation != expected {
                    return reject(MetadataError::GenerationMismatch {
                        expected,
                        actual: policy.generation,
                    });
                }
                let Some(next_generation) = policy.generation.checked_add(1) else {
                    return reject(MetadataError::limit(
                        "retention policy generation space is exhausted",
                    ));
                };
                policy.unarchived_deletion_allowed = unarchived_deletion_allowed;
                policy.generation = next_generation;
                MetadataResponse::Ack {
                    generation: next_generation,
                }
            }
            (Some(_), Some(_)) => {
                unreachable!("retention policy keys only ever hold policy records")
            }
        }
    }

    fn plan_retention(
        &mut self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        expected_segment_generation: u64,
        fencing_epoch: u64,
    ) -> MetadataResponse {
        // Retention is an act of current leaseholder authority, mirroring
        // plan_replica_retirement: no lease, no deletion authorization.
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        let Some(lease) = range.lease.as_ref() else {
            return reject(MetadataError::invalid_transition(
                "range holds no active lease for retention planning",
            ));
        };
        if fencing_epoch != lease.fencing_epoch {
            return reject(MetadataError::EpochMismatch {
                expected: fencing_epoch,
                actual: lease.fencing_epoch,
            });
        }

        let segment_key = MetaKey::Segment {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        if segment.segment_generation != expected_segment_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_segment_generation,
                actual: segment.segment_generation,
            });
        }
        // This alone excludes retention during repair, after quarantine, and
        // during a replica retirement.
        if segment.state != SegmentState::Verified {
            return reject(MetadataError::invalid_transition(
                "retention planning requires a verified segment",
            ));
        }
        let segment_content_root = segment.content_root;
        let segment_next_offset = segment.next_offset;

        // Planning retention mid-move would strand the intent, violating the
        // add-before-retire discipline.
        let intent_key = MetaKey::SegmentRebalanceIntent {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        if self.records.contains_key(&intent_key) {
            return reject(MetadataError::invalid_transition(
                "segment has an active rebalance intent",
            ));
        }
        // Mirrors propose_rebalance's guard: retention must not race an
        // in-flight verified move.
        let proof_key = MetaKey::SegmentReplacementProof {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        if self.records.contains_key(&proof_key) {
            return reject(MetadataError::invalid_transition(
                "segment has a committed replacement proof; complete or resolve the replica retirement first",
            ));
        }

        // The evidence gate — the point of this slice. A present-but-
        // mismatched tier copy REJECTS; it never falls through to the policy
        // branch (fail-closed).
        let tier_key = MetaKey::SegmentTierCopy {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let archived = match self.records.get(&tier_key) {
            Some(MetaValue::TierCopy(evidence)) => {
                // Tier evidence is bound to immutable segment identity (the
                // key's segment UUID plus the sealed content root), not the
                // lifecycle CAS token. Planning/cancelling retention or a
                // replica move may bump `segment_generation` without changing
                // a byte, and must not strand a still-valid cold copy.
                if evidence.content_root != segment_content_root {
                    return reject(MetadataError::invalid_transition(
                        "tier evidence content root does not match the sealed segment",
                    ));
                }
                // Deletion authority requires an immutable cold copy: without
                // the object version pin a later overwrite of `object_uri`
                // could redirect rehydration after the local replicas are
                // retired. The CLI refuses to commit unpinned evidence, but
                // the state machine is the trust boundary — a hand-built
                // admin proposal, or a legacy snapshot record, can carry
                // `object_version_id: None`. Unpinned evidence counts as NO
                // archive evidence (the record itself stays as the audit
                // anchor); the policy branch below is the operator escape
                // hatch, exactly as if no evidence existed.
                evidence.object_version_id.is_some()
            }
            Some(_) => unreachable!("tier-copy keys only ever hold tier-copy records"),
            None => false,
        };
        if !archived {
            let policy_key = MetaKey::TopicRetentionPolicy { topic_uuid }.encode();
            let allowed = matches!(
                self.records.get(&policy_key),
                Some(MetaValue::TopicRetentionPolicy(policy))
                    if policy.unarchived_deletion_allowed
            );
            // An absent policy record is the fail-closed default.
            if !allowed {
                return reject(MetadataError::invalid_transition(
                    "retention requires pinned tier evidence or an explicit unarchived-deletion policy",
                ));
            }
        }

        // Cursor protection over stage-7 state: only *durable committed
        // cursors* protect data. A member assigned the range that has never
        // committed holds no durable claim (stage 7: durable cursors are the
        // ownership; membership is ephemeral). Offsets are logical and
        // monotonic within a range's lineage stream, so one comparison covers
        // both a cursor inside this segment and a cursor in an older segment
        // that will still need this one; a cursor at exactly `next_offset`
        // has fully consumed the segment and does not block.
        for (key_bytes, value) in &self.records {
            let Ok(MetaKey::GroupCursor {
                group_uuid,
                topic_uuid: cursor_topic,
                range_uuid: cursor_range,
            }) = MetaKey::decode(key_bytes)
            else {
                continue;
            };
            if cursor_topic != topic_uuid || cursor_range != range_uuid {
                continue;
            }
            let MetaValue::GroupCursor(cursor) = value else {
                continue;
            };
            if cursor.record_offset < segment_next_offset {
                return reject(MetadataError::invalid_transition(format!(
                    "group cursor {group_uuid} at offset {} is below segment end {}",
                    cursor.record_offset, segment_next_offset
                )));
            }
        }

        let Some(MetaValue::Segment(segment)) = self.records.get_mut(&segment_key) else {
            unreachable!("segment record was present above and apply is single-threaded");
        };
        let Some(next_generation) = segment.segment_generation.checked_add(1) else {
            return reject(MetadataError::limit(
                "segment generation space is exhausted",
            ));
        };
        segment.state = SegmentState::RetentionPlanned;
        segment.segment_generation = next_generation;
        MetadataResponse::Ack {
            generation: next_generation,
        }
    }

    fn confirm_retention_expired(
        &mut self,
        apply_index: u64,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        expected_segment_generation: u64,
    ) -> MetadataResponse {
        // Apply is all-or-nothing: validate every rejection path — segment
        // AND placement — before mutating either record.
        let segment_key = MetaKey::Segment {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        if segment.segment_generation != expected_segment_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_segment_generation,
                actual: segment.segment_generation,
            });
        }
        if segment.state != SegmentState::RetentionPlanned {
            return reject(MetadataError::invalid_transition(
                "retention confirmation requires RETENTION_PLANNED",
            ));
        }
        let Some(next_generation) = segment.segment_generation.checked_add(1) else {
            return reject(MetadataError::limit(
                "segment generation space is exhausted",
            ));
        };

        let placement_key = MetaKey::SegmentPlacement {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let next_placement_generation = match self.records.get(&placement_key) {
            Some(MetaValue::SegmentPlacement(placement)) => {
                let Some(next) = placement.generation.checked_add(1) else {
                    return reject(MetadataError::limit(
                        "placement generation space is exhausted",
                    ));
                };
                Some(next)
            }
            _ => None,
        };

        // Every rejection has been ruled out; mutate both records in one
        // apply. The segment and tier-copy records are retained forever: the
        // durable audit reads "target RF n, zero local replicas, tier
        // evidence at key X" — the full-circle record.
        if let Some(MetaValue::Segment(segment)) = self.records.get_mut(&segment_key) {
            segment.state = SegmentState::RetentionExpired;
            segment.segment_generation = next_generation;
        }
        if let Some(next_placement_generation) = next_placement_generation {
            if let Some(MetaValue::SegmentPlacement(placement)) =
                self.records.get_mut(&placement_key)
            {
                // Empty the replica set but preserve the declared replication
                // factor verbatim: the durability target is never rewritten
                // from the list length.
                placement.replica_nodes.clear();
                placement.generation = next_placement_generation;
                placement.committed_apply_index = apply_index;
            }
        }
        MetadataResponse::Ack {
            generation: next_generation,
        }
    }

    fn cancel_retention(
        &mut self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        expected_segment_generation: u64,
    ) -> MetadataResponse {
        let segment_key = MetaKey::Segment {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        if segment.segment_generation != expected_segment_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_segment_generation,
                actual: segment.segment_generation,
            });
        }
        if segment.state != SegmentState::RetentionPlanned {
            return reject(MetadataError::invalid_transition(
                "retention cancellation requires RETENTION_PLANNED",
            ));
        }
        // PlanRetention is the durable deletion authorization. The state
        // machine has no proof that workers have not already removed one or
        // more replicas, so restoring Verified here would make possibly
        // deleted replicas readable again. Recovery must complete retention
        // or establish fresh verified placement evidence through repair.
        // The command is deprecated — retained on the wire for
        // compatibility only — and every path below fails closed.
        reject(MetadataError::invalid_transition(
            "retention cannot be cancelled after deletion is authorized",
        ))
    }

    /// The nodes a placement may be built from, as the state machine sees them.
    ///
    /// `pub(crate)` so a linearizable read can compute the placement the
    /// algorithm WOULD choose (#308). An operator can see neither this set nor
    /// the rendezvous, and `commit_segment_placement` compares their proposal
    /// against it positionally — so without a way to ask, a first placement
    /// could only be reached by guessing an order and resubmitting until one
    /// was accepted.
    pub(crate) fn active_placement_candidates(&self) -> Vec<PlacementCandidate> {
        let mut candidates = Vec::new();
        for (key_bytes, value) in &self.records {
            let Ok(MetaKey::Node { node_uuid }) = MetaKey::decode(key_bytes) else {
                continue;
            };
            let MetaValue::Node(node) = value else {
                continue;
            };
            if node.state != NodeState::Active {
                continue;
            }
            if node.placement_weight < MIN_PLACEMENT_WEIGHT {
                continue;
            }
            candidates.push(PlacementCandidate {
                node_uuid,
                failure_domain: node.failure_domain.clone(),
                weight: node.placement_weight,
            });
        }
        candidates
    }

    /// Encode the full state — sorted records plus the dedup FIFO — as one
    /// canonical byte string. Identical states always produce identical
    /// bytes, which the snapshot determinism tests pin.
    pub fn encode_snapshot(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::with_capacity(64 + self.records.len() * 64);
        put_u16(&mut out, SNAPSHOT_PAYLOAD_VERSION);
        let record_count = u32::try_from(self.records.len())
            .ok()
            .filter(|count| *count <= MAX_SNAPSHOT_RECORDS)
            .ok_or(CodecError::BoundExceeded {
                what: "snapshot record count",
                actual: self.records.len(),
                maximum: MAX_SNAPSHOT_RECORDS as usize,
            })?;
        put_u32(&mut out, record_count);
        for (key, value) in &self.records {
            if key.len() > MAX_SNAPSHOT_KEY_BYTES {
                return Err(CodecError::BoundExceeded {
                    what: "snapshot key",
                    actual: key.len(),
                    maximum: MAX_SNAPSHOT_KEY_BYTES,
                });
            }
            put_u16(&mut out, key.len() as u16);
            out.extend_from_slice(key);
            let encoded = value.encode()?;
            put_u32(&mut out, encoded.len() as u32);
            out.extend_from_slice(&encoded);
        }
        put_u32(&mut out, self.dedup_order.len() as u32);
        for request_id in &self.dedup_order {
            let response = self
                .dedup_responses
                .get(request_id)
                .expect("dedup order and dedup responses always agree");
            put_uuid(&mut out, *request_id);
            let encoded = response.encode()?;
            put_u32(&mut out, encoded.len() as u32);
            out.extend_from_slice(&encoded);
        }
        Ok(out)
    }

    /// Decode a snapshot payload, enforcing canonical form: strictly
    /// ascending unique keys, value tags that match their key category,
    /// bounded lengths, and no trailing bytes.
    pub fn decode_snapshot(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let version = reader.u16("snapshot payload version")?;
        if version != SNAPSHOT_PAYLOAD_VERSION {
            return Err(CodecError::UnknownTag {
                what: "snapshot payload version",
                tag: u32::from(version),
            });
        }
        let record_count = reader.u32("snapshot record count")?;
        if record_count > MAX_SNAPSHOT_RECORDS {
            return Err(CodecError::BoundExceeded {
                what: "snapshot record count",
                actual: record_count as usize,
                maximum: MAX_SNAPSHOT_RECORDS as usize,
            });
        }
        let mut records = BTreeMap::new();
        let mut previous_key: Option<Vec<u8>> = None;
        for _ in 0..record_count {
            let key_len = reader.u16("snapshot key length")? as usize;
            if key_len > MAX_SNAPSHOT_KEY_BYTES {
                return Err(CodecError::BoundExceeded {
                    what: "snapshot key",
                    actual: key_len,
                    maximum: MAX_SNAPSHOT_KEY_BYTES,
                });
            }
            let key = reader.take(key_len, "snapshot key")?.to_vec();
            if previous_key.as_ref().is_some_and(|prior| *prior >= key) {
                return Err(CodecError::InvalidValue {
                    what: "snapshot key order",
                    reason: "keys must be strictly ascending",
                });
            }
            let typed_key = MetaKey::decode(&key)?;
            let value_len = reader.u32("snapshot value length")? as usize;
            if value_len > MAX_SNAPSHOT_VALUE_BYTES {
                return Err(CodecError::BoundExceeded {
                    what: "snapshot value",
                    actual: value_len,
                    maximum: MAX_SNAPSHOT_VALUE_BYTES,
                });
            }
            let value = MetaValue::decode(reader.take(value_len, "snapshot value")?)?;
            if !key_matches_value(&typed_key, &value) {
                return Err(CodecError::InvalidValue {
                    what: "snapshot record",
                    reason: "value type does not match its key category",
                });
            }
            // A transition's invariants are part of its canonical form
            // (review): the key names the epoch it minted, and a mint only
            // ever moves the epoch forward. A snapshot that disagrees with
            // either was not written by this state machine.
            if let (
                MetaKey::RangeTransition { fencing_epoch, .. },
                MetaValue::RangeTransition(transition),
            ) = (&typed_key, &value)
            {
                if transition.epoch_to != *fencing_epoch
                    || transition.epoch_to <= transition.epoch_from
                {
                    return Err(CodecError::InvalidValue {
                        what: "snapshot record",
                        reason: "transition epochs disagree with the key or do not advance",
                    });
                }
            }
            previous_key = Some(key.clone());
            records.insert(key, value);
        }
        let dedup_count = reader.u32("dedup entry count")?;
        if dedup_count as usize > DEDUP_CAPACITY {
            return Err(CodecError::BoundExceeded {
                what: "dedup entry count",
                actual: dedup_count as usize,
                maximum: DEDUP_CAPACITY,
            });
        }
        let mut dedup_order = VecDeque::with_capacity(dedup_count as usize);
        let mut dedup_responses = HashMap::with_capacity(dedup_count as usize);
        for _ in 0..dedup_count {
            let request_id = reader.uuid("dedup request id")?;
            let response_len = reader.u32("dedup response length")? as usize;
            if response_len > MAX_SNAPSHOT_RESPONSE_BYTES {
                return Err(CodecError::BoundExceeded {
                    what: "dedup response",
                    actual: response_len,
                    maximum: MAX_SNAPSHOT_RESPONSE_BYTES,
                });
            }
            let response = MetadataResponse::decode(reader.take(response_len, "dedup response")?)?;
            if dedup_responses.insert(request_id, response).is_some() {
                return Err(CodecError::InvalidValue {
                    what: "dedup entry",
                    reason: "request id appears twice in the FIFO",
                });
            }
            dedup_order.push_back(request_id);
        }
        reader.finish()?;
        Ok(Self {
            records,
            dedup_order,
            dedup_responses,
        })
    }
}

fn key_matches_value(key: &MetaKey, value: &MetaValue) -> bool {
    matches!(
        (key, value),
        (MetaKey::Node { .. }, MetaValue::Node(_))
            | (MetaKey::Topic { .. }, MetaValue::Topic(_))
            | (MetaKey::TopicByName { .. }, MetaValue::TopicName(_))
            | (MetaKey::Range { .. }, MetaValue::Range(_))
            | (MetaKey::Segment { .. }, MetaValue::Segment(_))
            | (MetaKey::Key { .. }, MetaValue::Key(_))
            | (MetaKey::Group { .. }, MetaValue::Group(_))
            | (MetaKey::GroupByName { .. }, MetaValue::GroupName(_))
            | (MetaKey::GroupMember { .. }, MetaValue::GroupMember(_))
            | (MetaKey::GroupCursor { .. }, MetaValue::GroupCursor(_))
            | (
                MetaKey::SegmentPlacement { .. },
                MetaValue::SegmentPlacement(_)
            )
            | (
                MetaKey::SegmentReplacementProof { .. },
                MetaValue::ReplacementProof(_)
            )
            | (
                MetaKey::SegmentRebalanceIntent { .. },
                MetaValue::RebalanceIntent(_)
            )
            | (MetaKey::SegmentTierCopy { .. }, MetaValue::TierCopy(_))
            | (
                MetaKey::TopicRetentionPolicy { .. },
                MetaValue::TopicRetentionPolicy(_)
            )
            | (
                MetaKey::RangeTransition { .. },
                MetaValue::RangeTransition(_)
            )
    )
}

struct CommitCursorArgs {
    group_uuid: Uuid,
    member_uuid: Uuid,
    topic_uuid: Uuid,
    range_uuid: Uuid,
    topic_epoch: u64,
    range_generation: u64,
    segment_uuid: Uuid,
    segment_generation: u64,
    segment_root: [u8; 32],
    record_offset: u64,
    record_index: u64,
    lineage_transition_id: Option<Uuid>,
    expected_checkpoint_generation: Option<u64>,
}

struct CommitReplacementProofArgs {
    topic_uuid: Uuid,
    range_uuid: Uuid,
    segment_uuid: Uuid,
    expected_segment_generation: u64,
    content_root: [u8; 32],
    expected_length_bytes: u64,
    source_node_uuid: Uuid,
    destination_node_uuid: Uuid,
    fencing_epoch: u64,
    verification_method: VerificationMethod,
    verifier_node_uuid: Uuid,
    verified_term: u64,
}

struct CommitTierEvidenceArgs {
    topic_uuid: Uuid,
    range_uuid: Uuid,
    segment_uuid: Uuid,
    expected_segment_generation: u64,
    content_root: [u8; 32],
    byte_length: u64,
    backend_id: String,
    object_uri: String,
    object_version_id: Option<String>,
    manifest_version_id: Option<String>,
    manifest_core_digest: [u8; 32],
    verification_method: VerificationMethod,
    verifier_node_uuid: Uuid,
    fencing_epoch: u64,
    verified_term: u64,
}

fn cursor_is_forward_or_equal(
    existing: &CursorCheckpointRecord,
    args: &CommitCursorArgs,
) -> Result<(), MetadataError> {
    if args.segment_uuid == existing.segment_uuid {
        if args.segment_generation != existing.segment_generation
            || args.segment_root != existing.segment_root
        {
            return Err(MetadataError::invalid_transition(
                "same segment identity changed generation or root",
            ));
        }
        if args.record_offset < existing.record_offset
            || (args.record_offset == existing.record_offset
                && args.record_index < existing.record_index)
        {
            return Err(MetadataError::invalid_transition(
                "cursor moved backward within the same segment",
            ));
        }
        return Ok(());
    }
    // Different segment: require a strictly increasing record offset as a
    // coarse forward signal until split/merge transition evidence lands in
    // a later slice. Equal offsets across segment identity changes are
    // rejected — the same numeric offset in a different segment is not
    // proven to represent forward progress.
    if args.record_offset <= existing.record_offset {
        return Err(MetadataError::invalid_transition(
            "cursor did not advance across segment identity change",
        ));
    }
    Ok(())
}

fn encode_assigned_ranges(out: &mut Vec<u8>, ranges: &[RangeAssignment]) -> Result<(), CodecError> {
    if ranges.len() > MAX_ASSIGNED_RANGES {
        return Err(CodecError::BoundExceeded {
            what: "assigned ranges",
            actual: ranges.len(),
            maximum: MAX_ASSIGNED_RANGES,
        });
    }
    put_u16(out, ranges.len() as u16);
    for range in ranges {
        put_uuid(out, range.topic_uuid);
        put_uuid(out, range.range_uuid);
    }
    Ok(())
}

fn decode_assigned_ranges(reader: &mut Reader<'_>) -> Result<Vec<RangeAssignment>, CodecError> {
    let count = reader.u16("assigned range count")? as usize;
    if count > MAX_ASSIGNED_RANGES {
        return Err(CodecError::BoundExceeded {
            what: "assigned ranges",
            actual: count,
            maximum: MAX_ASSIGNED_RANGES,
        });
    }
    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        ranges.push(RangeAssignment {
            topic_uuid: reader.uuid("assigned topic uuid")?,
            range_uuid: reader.uuid("assigned range uuid")?,
        });
    }
    Ok(ranges)
}

fn encode_replica_nodes(out: &mut Vec<u8>, nodes: &[Uuid]) -> Result<(), CodecError> {
    // Must match decode_replica_nodes: a snapshot taken while a rebalance
    // intent holds the transient RF + 1 set has to encode, or Raft snapshot
    // creation and follower recovery stall for the whole move.
    if nodes.len() > MAX_TRANSIENT_REPLICAS {
        return Err(CodecError::BoundExceeded {
            what: "replica nodes",
            actual: nodes.len(),
            maximum: MAX_TRANSIENT_REPLICAS,
        });
    }
    put_u16(out, nodes.len() as u16);
    for node in nodes {
        put_uuid(out, *node);
    }
    Ok(())
}

fn decode_replica_nodes(reader: &mut Reader<'_>) -> Result<Vec<Uuid>, CodecError> {
    let count = reader.u16("replica node count")? as usize;
    // The stored set may transiently exceed MAX_REPLICAS by one while a
    // rebalance intent is in flight; snapshots taken then must round-trip.
    if count > MAX_TRANSIENT_REPLICAS {
        return Err(CodecError::BoundExceeded {
            what: "replica nodes",
            actual: count,
            maximum: MAX_TRANSIENT_REPLICAS,
        });
    }
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(reader.uuid("replica node uuid")?);
    }
    Ok(nodes)
}

fn reject(error: MetadataError) -> MetadataResponse {
    MetadataResponse::Rejected(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandEnvelope;

    fn envelope(request: u128) -> CommandEnvelope {
        CommandEnvelope {
            request_id: Uuid::from_u128(request),
            issued_at_ms: 0,
        }
    }

    fn envelope_at(request: u128, issued_at_ms: i64) -> CommandEnvelope {
        CommandEnvelope {
            request_id: Uuid::from_u128(request),
            issued_at_ms,
        }
    }

    /// One active node plus one root range, the minimum a lease needs.
    fn leaseable_range(node: u128) -> (MetaStateMachine, Uuid, Uuid, Uuid) {
        let mut machine = MetaStateMachine::new();
        let node_uuid = Uuid::from_u128(node);
        let topic_uuid = Uuid::from_u128(20);
        let range_uuid = Uuid::from_u128(21);
        machine.apply(
            1,
            &MetadataCommand::RegisterNode {
                env: envelope(1),
                node_uuid,
                addr: "n1:9200".to_owned(),
                expected_generation: None,
            },
        );
        machine.apply(
            2,
            &MetadataCommand::CreateTopic {
                env: envelope(2),
                name: "events.v1".to_owned(),
                topic_uuid,
                root_range_uuid: range_uuid,
            },
        );
        (machine, node_uuid, topic_uuid, range_uuid)
    }

    fn range_of(machine: &MetaStateMachine, topic_uuid: Uuid, range_uuid: Uuid) -> RangeRecord {
        let key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        };
        match machine.record(&key) {
            Some(MetaValue::Range(range)) => range.clone(),
            other => panic!("expected a range record, got {other:?}"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire(
        machine: &mut MetaStateMachine,
        index: u64,
        request: u128,
        now_ms: i64,
        holder: Uuid,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        generation: u64,
        duration_ms: u64,
    ) -> MetadataResponse {
        machine.apply(
            index,
            &MetadataCommand::AcquireRangeLease {
                env: envelope_at(request, now_ms),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                expected_range_generation: generation,
                lease_duration_ms: duration_ms,
            },
        )
    }

    fn transitions_of(
        machine: &MetaStateMachine,
        topic_uuid: Uuid,
        range_uuid: Uuid,
    ) -> Vec<RangeTransitionRecord> {
        machine.range_transitions(topic_uuid, range_uuid, 0, 64)
    }

    fn established(
        boundary: u64,
        quorum: &[(u128, u64)],
        votes: u32,
        required: u32,
    ) -> PromotionOutcome {
        PromotionOutcome::Established {
            boundary_offset: Some(boundary),
            sealed_prefix_end: None,
            quorum: quorum
                .iter()
                .map(|(node, offset)| crate::command::QuorumAnswer {
                    node_uuid: Uuid::from_u128(*node),
                    offset: *offset,
                })
                .collect(),
            votes,
            required,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn report(
        machine: &mut MetaStateMachine,
        index: u64,
        request: u128,
        now_ms: i64,
        holder: Uuid,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        fencing_epoch: u64,
        outcome: PromotionOutcome,
    ) -> MetadataResponse {
        machine.apply(
            index,
            &MetadataCommand::ReportPromotionOutcome {
                env: envelope_at(request, now_ms),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                fencing_epoch,
                outcome,
            },
        )
    }

    /// Every mint records its transition, in the same apply (#240 item 5):
    /// an election from nobody, an election that displaces a lapsed holder,
    /// and an administrative grant each leave a record, the chain links
    /// epoch to epoch with no gap, and a fresh record is visibly PENDING —
    /// the honest shape of an epoch nobody has served under yet.
    #[test]
    fn every_grant_mints_a_transition_and_the_chain_is_gapless() {
        let (mut machine, first, topic_uuid, range_uuid) = leaseable_range(10);
        let second = Uuid::from_u128(11);
        machine.apply(
            3,
            &MetadataCommand::RegisterNode {
                env: envelope(3),
                node_uuid: second,
                addr: "n2:9200".to_owned(),
                expected_generation: None,
            },
        );
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        assert!(matches!(
            acquire(
                &mut machine,
                4,
                4,
                1_000,
                first,
                topic_uuid,
                range_uuid,
                generation,
                5_000
            ),
            MetadataResponse::LeaseGranted { fencing_epoch: 1 }
        ));
        // Lapsed, then taken by the other node.
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        assert!(matches!(
            acquire(
                &mut machine,
                5,
                5,
                7_000,
                second,
                topic_uuid,
                range_uuid,
                generation,
                5_000
            ),
            MetadataResponse::LeaseGranted { fencing_epoch: 2 }
        ));
        // An operator hands it back to the first node, permanently.
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        assert!(matches!(
            machine.apply(
                6,
                &MetadataCommand::GrantRangeLease {
                    env: envelope_at(6, 9_000),
                    topic_uuid,
                    range_uuid,
                    holder_node_uuid: first,
                    expected_range_generation: generation,
                },
            ),
            MetadataResponse::LeaseGranted { fencing_epoch: 3 }
        ));

        let chain = transitions_of(&machine, topic_uuid, range_uuid);
        assert_eq!(chain.len(), 3, "one record per mint, no more and no fewer");
        assert_eq!(
            chain[0],
            RangeTransitionRecord {
                epoch_from: 0,
                epoch_to: 1,
                holder_from: None,
                holder_to: first,
                grant: GrantKind::Election,
                granted_at_ms: 1_000,
                granted_apply_index: 4,
                outcome: TransitionOutcome::Pending,
            }
        );
        assert_eq!(chain[1].epoch_from, 1);
        assert_eq!(chain[1].epoch_to, 2);
        assert_eq!(chain[1].holder_from, Some(first));
        assert_eq!(chain[1].holder_to, second);
        assert_eq!(chain[1].grant, GrantKind::Election);
        assert_eq!(chain[2].epoch_from, 2);
        assert_eq!(chain[2].epoch_to, 3);
        assert_eq!(chain[2].holder_from, Some(second));
        assert_eq!(chain[2].holder_to, first);
        assert_eq!(chain[2].grant, GrantKind::Administrative);
        assert_eq!(chain[2].granted_at_ms, 9_000);
        for pair in chain.windows(2) {
            assert_eq!(
                pair[0].epoch_to, pair[1].epoch_from,
                "each record begins where the previous one ended: no gap, ever"
            );
        }
        // The read is a key range in epoch order, and `from_epoch` cuts it.
        assert_eq!(
            machine
                .range_transitions(topic_uuid, range_uuid, 2, 64)
                .len(),
            2
        );
        assert_eq!(
            machine
                .range_transitions(topic_uuid, range_uuid, 0, 1)
                .len(),
            1
        );
        assert!(machine
            .range_transitions(Uuid::from_u128(99), range_uuid, 0, 64)
            .is_empty());
    }

    /// The holder's report fills in the outcome (#240 item 5): the fenced
    /// quorum, the adopted boundary and the vote are kept as data a checker
    /// can recompute from rather than trust. Only the holder may report; a
    /// refusal may be superseded by the retry that succeeds; an established
    /// transition is final; and an epoch never minted has no record to
    /// fill.
    #[test]
    fn a_promotion_report_fills_the_holders_record_and_an_established_one_is_final() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        acquire(
            &mut machine,
            3,
            3,
            1_000,
            holder,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        );
        let rival = Uuid::from_u128(11);

        // Somebody else's report on this epoch is refused by name.
        assert!(matches!(
            report(
                &mut machine,
                4,
                4,
                1_100,
                rival,
                topic_uuid,
                range_uuid,
                1,
                established(0, &[], 0, 0)
            ),
            MetadataResponse::Rejected(MetadataError::InvalidTransition(_))
        ));
        // An epoch never minted has nothing to fill.
        assert!(matches!(
            report(
                &mut machine,
                5,
                5,
                1_100,
                holder,
                topic_uuid,
                range_uuid,
                7,
                established(0, &[], 0, 0)
            ),
            MetadataResponse::Rejected(MetadataError::NotFound)
        ));
        // A quorum miss, reported honestly...
        assert!(matches!(
            report(
                &mut machine,
                6,
                6,
                1_200,
                holder,
                topic_uuid,
                range_uuid,
                1,
                PromotionOutcome::Refused {
                    reason: crate::command::PromotionRefusal::QuorumUnavailable
                },
            ),
            MetadataResponse::TransitionRecorded { fencing_epoch: 1 }
        ));
        // ...may be superseded by the retry that succeeds at the same epoch.
        let evidence = established(401, &[(10, 401), (11, 400), (12, 350)], 3, 2);
        assert!(matches!(
            report(
                &mut machine,
                7,
                7,
                1_500,
                holder,
                topic_uuid,
                range_uuid,
                1,
                evidence.clone()
            ),
            MetadataResponse::TransitionRecorded { fencing_epoch: 1 }
        ));
        let chain = transitions_of(&machine, topic_uuid, range_uuid);
        assert_eq!(
            chain[0].outcome,
            TransitionOutcome::Reported {
                outcome: evidence,
                reported_at_ms: 1_500,
                reported_apply_index: 7,
            }
        );
        // Established is final: a later report cannot rewrite what was proven.
        assert!(matches!(
            report(
                &mut machine,
                8,
                8,
                1_600,
                holder,
                topic_uuid,
                range_uuid,
                1,
                established(0, &[], 0, 0)
            ),
            MetadataResponse::Rejected(MetadataError::InvalidTransition(_))
        ));
        // The quorum list is bounded in apply as well as on the wire.
        let oversized = established(
            0,
            &(0..MAX_TRANSITION_QUORUM as u128 + 1)
                .map(|node| (node, 0))
                .collect::<Vec<_>>(),
            0,
            0,
        );
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        acquire(
            &mut machine,
            9,
            9,
            9_000,
            holder,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        );
        assert!(matches!(
            report(
                &mut machine,
                10,
                10,
                9_100,
                holder,
                topic_uuid,
                range_uuid,
                2,
                oversized
            ),
            MetadataResponse::Rejected(MetadataError::Limit(_))
        ));
    }

    /// A snapshot whose transition record disagrees with its key, or whose
    /// epochs do not advance, is refused (review): the invariants are part
    /// of the canonical form, not a courtesy of the writer.
    #[test]
    fn a_snapshot_transition_that_breaks_its_invariants_is_refused() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        acquire(
            &mut machine,
            3,
            3,
            1_000,
            holder,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        );
        let encoded = machine.encode_snapshot().unwrap();
        let key = MetaKey::RangeTransition {
            topic_uuid,
            range_uuid,
            fencing_epoch: 1,
        }
        .encode();
        let key_at = encoded
            .windows(key.len())
            .position(|window| window == key.as_slice())
            .expect("the transition key is in the snapshot");
        // key, then u32 value length, then the value: tag, epoch_from,
        // epoch_to.
        let value_at = key_at + key.len() + 4;
        assert_eq!(encoded[value_at], VALUE_TAG_RANGE_TRANSITION);
        let mut not_advancing = encoded.clone();
        not_advancing[value_at + 1..value_at + 9].copy_from_slice(&1_u64.to_be_bytes());
        assert!(
            MetaStateMachine::decode_snapshot(&not_advancing).is_err(),
            "epoch_from == epoch_to is not a transition"
        );
        let mut disagreeing = encoded.clone();
        disagreeing[value_at + 9..value_at + 17].copy_from_slice(&2_u64.to_be_bytes());
        assert!(
            MetaStateMachine::decode_snapshot(&disagreeing).is_err(),
            "a value whose epoch_to is not the key's epoch was not written by this machine"
        );
        assert!(MetaStateMachine::decode_snapshot(&encoded).is_ok());
    }

    /// The record survives a snapshot byte-exactly in both outcomes, and its
    /// value tag agrees with its key category (#240 item 5).
    #[test]
    fn transition_records_round_trip_through_snapshots() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        acquire(
            &mut machine,
            3,
            3,
            1_000,
            holder,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        );
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        acquire(
            &mut machine,
            4,
            4,
            7_000,
            holder,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        );
        report(
            &mut machine,
            5,
            5,
            7_100,
            holder,
            topic_uuid,
            range_uuid,
            2,
            established(401, &[(10, 401), (11, 400)], 2, 2),
        );
        let encoded = machine.encode_snapshot().unwrap();
        let decoded = MetaStateMachine::decode_snapshot(&encoded).unwrap();
        assert_eq!(decoded.encode_snapshot().unwrap(), encoded);
        assert_eq!(
            transitions_of(&decoded, topic_uuid, range_uuid),
            transitions_of(&machine, topic_uuid, range_uuid)
        );
        let value =
            MetaValue::RangeTransition(transitions_of(&machine, topic_uuid, range_uuid)[1].clone());
        assert_eq!(MetaValue::decode(&value.encode().unwrap()).unwrap(), value);
    }

    /// The core liveness property: a leader that stops renewing loses the
    /// range. Without expiry a dead leader holds it forever and no follower
    /// can take over — which is the whole gap #223 exists to close.
    #[test]
    fn a_lease_can_be_taken_only_after_it_expires() {
        let (mut machine, first, topic_uuid, range_uuid) = leaseable_range(10);
        let second = Uuid::from_u128(11);
        machine.apply(
            3,
            &MetadataCommand::RegisterNode {
                env: envelope(3),
                node_uuid: second,
                addr: "n2:9200".to_owned(),
                expected_generation: None,
            },
        );
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        let granted = acquire(
            &mut machine,
            4,
            4,
            1_000,
            first,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        );
        let MetadataResponse::LeaseGranted { fencing_epoch } = granted else {
            panic!("expected a grant, got {granted:?}")
        };

        // Still live: a rival is refused, so a healthy leader cannot be
        // displaced at will and the cluster does not flap.
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        let refused = acquire(
            &mut machine,
            5,
            5,
            3_000,
            second,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        );
        assert!(
            matches!(refused, MetadataResponse::Rejected { .. }),
            "a live lease must not be stealable: {refused:?}"
        );

        // Past the deadline the rival wins, and the epoch advances — which is
        // what fences the old holder, independently of any clock agreement.
        let taken = acquire(
            &mut machine,
            6,
            6,
            6_001,
            second,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        );
        let MetadataResponse::LeaseGranted {
            fencing_epoch: next,
        } = taken
        else {
            panic!("expected a grant after expiry, got {taken:?}")
        };
        assert!(
            next > fencing_epoch,
            "acquisition must mint a higher epoch ({next} <= {fencing_epoch})"
        );
        let lease = range_of(&machine, topic_uuid, range_uuid).lease.unwrap();
        assert_eq!(lease.holder_node_uuid, second);
    }

    /// Renewal keeps a leader serving without disturbing its epoch — a new
    /// epoch on every heartbeat would fence the leader against its own
    /// in-flight produce requests.
    #[test]
    fn renewal_extends_the_deadline_without_minting_an_epoch() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        let granted = acquire(
            &mut machine,
            3,
            3,
            1_000,
            holder,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        );
        let MetadataResponse::LeaseGranted { fencing_epoch } = granted else {
            panic!("expected a grant")
        };

        let renewed = machine.apply(
            4,
            &MetadataCommand::RenewRangeLease {
                env: envelope_at(4, 4_000),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                expected_fencing_epoch: fencing_epoch,
                lease_duration_ms: 5_000,
            },
        );
        assert!(matches!(
            renewed,
            MetadataResponse::LeaseGranted { fencing_epoch: e } if e == fencing_epoch
        ));
        let lease = range_of(&machine, topic_uuid, range_uuid).lease.unwrap();
        assert_eq!(lease.fencing_epoch, fencing_epoch, "epoch must not move");
        assert_eq!(lease.expires_at_ms, Some(9_000));
    }

    /// A renewal must not consume the range's CAS token either. It changes
    /// neither holder nor epoch and returns no generation, so bumping would
    /// make steady heartbeats silently invalidate the leader's own in-flight
    /// range operations — a `RegisterSealedSegment` prepared moments before a
    /// renewal landed would be refused with a GenerationMismatch it could
    /// never anticipate.
    #[test]
    fn renewal_does_not_consume_the_range_cas() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        let granted = acquire(
            &mut machine,
            3,
            3,
            1_000,
            holder,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        );
        let MetadataResponse::LeaseGranted { fencing_epoch } = granted else {
            panic!("expected a grant")
        };
        let generation_after_grant = range_of(&machine, topic_uuid, range_uuid).generation;

        machine.apply(
            4,
            &MetadataCommand::RenewRangeLease {
                env: envelope_at(4, 4_000),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                expected_fencing_epoch: fencing_epoch,
                lease_duration_ms: 5_000,
            },
        );
        assert_eq!(
            range_of(&machine, topic_uuid, range_uuid).generation,
            generation_after_grant,
            "a heartbeat must not invalidate CAS tokens handed out under this lease"
        );
    }

    /// A fenced leader must not be able to keep its lease alive. Checking only
    /// the holder id would let a partitioned old leader renew forever.
    #[test]
    fn renewal_from_a_fenced_epoch_is_refused() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        let MetadataResponse::LeaseGranted { fencing_epoch } = acquire(
            &mut machine,
            3,
            3,
            1_000,
            holder,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        ) else {
            panic!("expected a grant")
        };
        let refused = machine.apply(
            4,
            &MetadataCommand::RenewRangeLease {
                env: envelope_at(4, 2_000),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                expected_fencing_epoch: fencing_epoch - 1,
                lease_duration_ms: 5_000,
            },
        );
        assert!(
            matches!(refused, MetadataResponse::Rejected { .. }),
            "a renewal naming a stale epoch must be refused: {refused:?}"
        );
    }

    /// An out-of-order renewal must never pull the deadline back in and
    /// trigger an election against a leader that is renewing correctly.
    #[test]
    fn a_late_short_renewal_never_shortens_the_lease() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        let MetadataResponse::LeaseGranted { fencing_epoch } = acquire(
            &mut machine,
            3,
            3,
            1_000,
            holder,
            topic_uuid,
            range_uuid,
            generation,
            30_000,
        ) else {
            panic!("expected a grant")
        };
        machine.apply(
            4,
            &MetadataCommand::RenewRangeLease {
                env: envelope_at(4, 1_500),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                expected_fencing_epoch: fencing_epoch,
                lease_duration_ms: 1_000,
            },
        );
        let lease = range_of(&machine, topic_uuid, range_uuid).lease.unwrap();
        assert_eq!(
            lease.expires_at_ms,
            Some(31_000),
            "the longer deadline must win"
        );
    }

    /// The renewal that would defeat the whole mechanism: a stale holder
    /// pushing its deadline forward forever, never advancing the epoch, so no
    /// rival can ever win. Once a lease lapses the only way back is
    /// acquisition.
    #[test]
    fn an_expired_lease_cannot_be_renewed_back_to_life() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        let MetadataResponse::LeaseGranted { fencing_epoch } = acquire(
            &mut machine,
            3,
            3,
            1_000,
            holder,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        ) else {
            panic!("expected a grant")
        };
        let refused = machine.apply(
            4,
            &MetadataCommand::RenewRangeLease {
                env: envelope_at(4, 6_001),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                expected_fencing_epoch: fencing_epoch,
                lease_duration_ms: 5_000,
            },
        );
        assert!(
            matches!(refused, MetadataResponse::Rejected { .. }),
            "an expired lease must be acquired, not renewed: {refused:?}"
        );
        assert_eq!(
            range_of(&machine, topic_uuid, range_uuid)
                .lease
                .unwrap()
                .expires_at_ms,
            Some(6_000),
            "the refused renewal must not have moved the deadline"
        );
    }

    /// Grant and acquire both refuse a non-active holder; renewal skipping the
    /// check would let a node marked Dead keep its range through heartbeats.
    #[test]
    fn a_dead_holder_cannot_renew() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        let MetadataResponse::LeaseGranted { fencing_epoch } = acquire(
            &mut machine,
            3,
            3,
            1_000,
            holder,
            topic_uuid,
            range_uuid,
            generation,
            5_000,
        ) else {
            panic!("expected a grant")
        };
        machine.apply(
            4,
            &MetadataCommand::SetNodeState {
                env: envelope(4),
                node_uuid: holder,
                state: NodeState::Dead,
                // A freshly registered node sits at generation 0.
                expected_generation: 0,
            },
        );
        assert!(
            matches!(
                machine.record(&MetaKey::Node { node_uuid: holder }),
                Some(MetaValue::Node(node)) if node.state == NodeState::Dead
            ),
            "the holder must actually be Dead for this test to mean anything"
        );
        let refused = machine.apply(
            5,
            &MetadataCommand::RenewRangeLease {
                env: envelope_at(5, 2_000),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                expected_fencing_epoch: fencing_epoch,
                lease_duration_ms: 5_000,
            },
        );
        assert!(
            matches!(refused, MetadataResponse::Rejected { .. }),
            "a dead holder must not be able to renew: {refused:?}"
        );
    }

    /// Renewing an administrative lease would give it a deadline, letting an
    /// election take later what an operator chose permanently.
    #[test]
    fn an_administrative_lease_cannot_be_renewed_into_an_expiring_one() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        machine.apply(
            3,
            &MetadataCommand::GrantRangeLease {
                env: envelope(3),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                expected_range_generation: generation,
            },
        );
        let epoch = range_of(&machine, topic_uuid, range_uuid)
            .lease
            .unwrap()
            .fencing_epoch;
        let refused = machine.apply(
            4,
            &MetadataCommand::RenewRangeLease {
                env: envelope_at(4, 1_000),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                expected_fencing_epoch: epoch,
                lease_duration_ms: 5_000,
            },
        );
        assert!(
            matches!(refused, MetadataResponse::Rejected { .. }),
            "an administrative lease must not be renewable: {refused:?}"
        );
        assert_eq!(
            range_of(&machine, topic_uuid, range_uuid)
                .lease
                .unwrap()
                .expires_at_ms,
            None,
            "it must remain never-expiring"
        );
    }

    /// The acquire path must refuse an administrative lease for the same
    /// reason the renew path does: even the holder itself "re-acquiring"
    /// would swap the operator's permanent lease for an expiring one that a
    /// rival could take once it lapses.
    #[test]
    fn an_administrative_lease_cannot_be_acquired_into_an_expiring_one() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        machine.apply(
            3,
            &MetadataCommand::GrantRangeLease {
                env: envelope(3),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                expected_range_generation: generation,
            },
        );
        let before = range_of(&machine, topic_uuid, range_uuid).lease.unwrap();
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        let refused = machine.apply(
            4,
            &MetadataCommand::AcquireRangeLease {
                env: envelope_at(4, 1_000),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                expected_range_generation: generation,
                lease_duration_ms: 5_000,
            },
        );
        assert!(
            matches!(refused, MetadataResponse::Rejected { .. }),
            "the holder must not be able to acquire over its own administrative \
             lease: {refused:?}"
        );
        let after = range_of(&machine, topic_uuid, range_uuid).lease.unwrap();
        assert_eq!(after, before, "the administrative lease must be untouched");
        assert_eq!(after.expires_at_ms, None, "it must remain never-expiring");
    }

    /// An administrative grant is an operator decision and must not be undone
    /// by an election later.
    #[test]
    fn an_administrative_grant_never_expires() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        machine.apply(
            3,
            &MetadataCommand::GrantRangeLease {
                env: envelope(3),
                topic_uuid,
                range_uuid,
                holder_node_uuid: holder,
                expected_range_generation: generation,
            },
        );
        let lease = range_of(&machine, topic_uuid, range_uuid).lease.unwrap();
        assert_eq!(lease.expires_at_ms, None);
        assert!(
            lease.is_live_at(i64::MAX),
            "a lease with no deadline is live at any time"
        );
    }

    /// A zero-length lease would expire before its holder could renew it, so
    /// it is refused rather than granted and instantly lost.
    #[test]
    fn a_zero_length_lease_is_refused() {
        let (mut machine, holder, topic_uuid, range_uuid) = leaseable_range(10);
        let generation = range_of(&machine, topic_uuid, range_uuid).generation;
        let refused = acquire(
            &mut machine,
            3,
            3,
            1_000,
            holder,
            topic_uuid,
            range_uuid,
            generation,
            0,
        );
        assert!(matches!(refused, MetadataResponse::Rejected { .. }));
    }

    /// The durable format is a compatibility contract: a lease with no
    /// deadline must still encode with the pre-#223 presence byte, so pinned
    /// snapshot vectors and mixed-version replicas stay byte-exact.
    #[test]
    fn a_deadlineless_lease_still_encodes_with_the_legacy_presence_byte() {
        let without = MetaValue::Range(RangeRecord {
            generation: 3,
            key_prefix: 0,
            key_prefix_bits: 0,
            fencing_epoch: 2,
            lineage_generation: 0,
            lease: Some(LeaseRecord {
                holder_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 2,
                granted_apply_index: 7,
                expires_at_ms: None,
            }),
        });
        let encoded = without.encode().unwrap();
        assert_eq!(
            encoded[encoded.len() - 33],
            1,
            "a lease without a deadline must keep presence byte 1"
        );
        assert_eq!(MetaValue::decode(&encoded).unwrap(), without);

        let with = MetaValue::Range(RangeRecord {
            lease: Some(LeaseRecord {
                holder_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 2,
                granted_apply_index: 7,
                expires_at_ms: Some(1_234),
            }),
            ..match without {
                MetaValue::Range(range) => range,
                _ => unreachable!(),
            }
        });
        let encoded = with.encode().unwrap();
        assert_eq!(MetaValue::decode(&encoded).unwrap(), with);
    }

    #[test]
    fn replica_node_codec_round_trips_the_transient_nine_node_set() {
        // A snapshot taken mid-rebalance carries declared RF + 1 replicas;
        // encoder and decoder must agree on the transient bound or Raft
        // snapshot creation stalls for the duration of the move.
        let nine: Vec<Uuid> = (0..MAX_TRANSIENT_REPLICAS as u128)
            .map(Uuid::from_u128)
            .collect();
        let mut out = Vec::new();
        encode_replica_nodes(&mut out, &nine).expect("transient set must encode");
        let mut reader = Reader::new(&out);
        assert_eq!(
            decode_replica_nodes(&mut reader).expect("transient set must decode"),
            nine
        );

        let ten: Vec<Uuid> = (0..(MAX_TRANSIENT_REPLICAS + 1) as u128)
            .map(Uuid::from_u128)
            .collect();
        assert!(encode_replica_nodes(&mut Vec::new(), &ten).is_err());
    }

    #[test]
    fn snapshot_round_trip_preserves_records_and_dedup_fifo_byte_exactly() {
        let mut machine = MetaStateMachine::new();
        machine.apply(
            1,
            &MetadataCommand::RegisterNode {
                env: envelope(1),
                node_uuid: Uuid::from_u128(10),
                addr: "n1:9200".to_owned(),
                expected_generation: None,
            },
        );
        machine.apply(
            2,
            &MetadataCommand::CreateTopic {
                env: envelope(2),
                name: "events.v1".to_owned(),
                topic_uuid: Uuid::from_u128(20),
                root_range_uuid: Uuid::from_u128(21),
            },
        );
        let encoded = machine.encode_snapshot().unwrap();
        let decoded = MetaStateMachine::decode_snapshot(&encoded).unwrap();
        assert_eq!(decoded.encode_snapshot().unwrap(), encoded);
        assert_eq!(decoded.record_count(), machine.record_count());
        assert_eq!(decoded.dedup_len(), machine.dedup_len());
    }

    #[test]
    fn snapshot_decode_rejects_unsorted_keys_trailing_bytes_and_unknown_versions() {
        let mut machine = MetaStateMachine::new();
        machine.apply(
            1,
            &MetadataCommand::PutKeyRecord {
                env: envelope(1),
                key_uuid: Uuid::from_u128(40),
                scheme: 1,
                public_material_digest: [1; 32],
            },
        );
        let encoded = machine.encode_snapshot().unwrap();

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert_eq!(
            MetaStateMachine::decode_snapshot(&trailing),
            Err(CodecError::Trailing(1))
        );

        let mut future = encoded.clone();
        future[..2].copy_from_slice(&2_u16.to_be_bytes());
        assert_eq!(
            MetaStateMachine::decode_snapshot(&future),
            Err(CodecError::UnknownTag {
                what: "snapshot payload version",
                tag: 2,
            })
        );

        // Duplicate the single record: the second key is not strictly above
        // the first, so canonical form is violated.
        let mut machine_two = MetaStateMachine::new();
        machine_two.apply(
            1,
            &MetadataCommand::PutKeyRecord {
                env: envelope(1),
                key_uuid: Uuid::from_u128(40),
                scheme: 1,
                public_material_digest: [1; 32],
            },
        );
        let single = machine_two.encode_snapshot().unwrap();
        let record_bytes = &single[6..single.len() - 4 - 16 - 4 - single_dedup_len(&machine_two)];
        let mut duplicated = single[..6].to_vec();
        duplicated[2..6].copy_from_slice(&2_u32.to_be_bytes());
        duplicated.extend_from_slice(record_bytes);
        duplicated.extend_from_slice(record_bytes);
        duplicated.extend_from_slice(&single[6 + record_bytes.len()..]);
        assert!(matches!(
            MetaStateMachine::decode_snapshot(&duplicated),
            Err(CodecError::InvalidValue {
                what: "snapshot key order",
                ..
            })
        ));
    }

    fn single_dedup_len(machine: &MetaStateMachine) -> usize {
        machine
            .dedup_responses
            .values()
            .next()
            .unwrap()
            .encode()
            .unwrap()
            .len()
    }

    #[test]
    fn legacy_group_member_value_tag_decodes_with_zero_heartbeat() {
        let mut bytes = Vec::new();
        put_u8(&mut bytes, VALUE_TAG_GROUP_MEMBER);
        put_u64(&mut bytes, 3);
        put_u16(&mut bytes, 0); // no assigned ranges
        let value = MetaValue::decode(&bytes).unwrap();
        assert_eq!(
            value,
            MetaValue::GroupMember(GroupMemberRecord {
                generation: 3,
                last_heartbeat_apply_index: 0,
                assigned: Vec::new(),
            })
        );
    }

    #[test]
    fn range_value_codec_keeps_legacy_bytes_until_a_lineage_transition() {
        // Untransitioned ranges encode as the legacy tag byte-for-byte, so
        // pinned snapshot vectors stay stable; decode defaults lineage to 0.
        let untransitioned = MetaValue::Range(RangeRecord {
            generation: 5,
            key_prefix: 0,
            key_prefix_bits: 0,
            fencing_epoch: 3,
            lineage_generation: 0,
            lease: None,
        });
        let encoded = untransitioned.encode().unwrap();
        assert_eq!(encoded[0], VALUE_TAG_RANGE);
        assert_eq!(MetaValue::decode(&encoded).unwrap(), untransitioned);

        // A transitioned range round-trips through the v2 tag.
        let transitioned = MetaValue::Range(RangeRecord {
            generation: 5,
            key_prefix: 0,
            key_prefix_bits: 0,
            fencing_epoch: 3,
            lineage_generation: 4,
            lease: None,
        });
        let encoded = transitioned.encode().unwrap();
        assert_eq!(encoded[0], VALUE_TAG_RANGE_V2);
        assert_eq!(MetaValue::decode(&encoded).unwrap(), transitioned);

        // A v2 tag carrying lineage 0 would re-encode as the legacy tag, so
        // it is rejected as non-canonical instead of silently accepted.
        let mut noncanonical = encoded.clone();
        noncanonical[26..34].fill(0);
        assert!(matches!(
            MetaValue::decode(&noncanonical),
            Err(CodecError::InvalidValue {
                what: "range lineage generation",
                ..
            })
        ));
    }

    fn max_tier_copy_record() -> TierCopyRecord {
        TierCopyRecord {
            generation: u64::MAX,
            segment_generation: u64::MAX,
            content_root: [0xff; 32],
            byte_length: u64::MAX,
            backend_id: "b".repeat(MAX_TIER_BACKEND_ID_BYTES),
            object_uri: "u".repeat(MAX_TIER_OBJECT_URI_BYTES),
            object_version_id: Some("o".repeat(MAX_TIER_VERSION_ID_BYTES)),
            manifest_version_id: Some("v".repeat(MAX_TIER_VERSION_ID_BYTES)),
            manifest_core_digest: [0xff; 32],
            verification_method: VerificationMethod::AuthenticatedContentRoot,
            verifier_node_uuid: Uuid::from_u128(u128::MAX),
            verified_at_apply_index: u64::MAX,
            verified_term: u64::MAX,
            fencing_epoch: u64::MAX,
        }
    }

    #[test]
    fn max_size_tier_copy_record_stays_inside_the_snapshot_value_bound() {
        // Pin the arithmetic: tag 1 + generation 8 + segment generation 8 +
        // root 32 + length 8 + backend (2 + 64) + uri (2 + 512) + version
        // (1 + 2 + 128) + digest 32 + method 1 + verifier 16 + apply index 8
        // + term 8 + epoch 8 + object version extension (1 + 2 + 128) =
        // 972 bytes. This is why the URI bound is 512 (not S3's 1024) and the
        // manifest has no separate URI field.
        let encoded = MetaValue::TierCopy(max_tier_copy_record())
            .encode()
            .unwrap();
        assert_eq!(encoded.len(), 972);
        assert!(encoded.len() < MAX_SNAPSHOT_VALUE_BYTES);
        assert_eq!(
            MetaValue::decode(&encoded).unwrap(),
            MetaValue::TierCopy(max_tier_copy_record())
        );
    }

    #[test]
    fn tier_copy_value_codec_rejects_bounds_and_non_verified_facts() {
        // One byte over the URI bound rejects at encode time...
        let mut over = max_tier_copy_record();
        over.object_uri = "u".repeat(MAX_TIER_OBJECT_URI_BYTES + 1);
        assert!(matches!(
            MetaValue::TierCopy(over).encode(),
            Err(CodecError::BoundExceeded { .. })
        ));
        // ...and a crafted byte string carrying 513 URI bytes rejects at
        // decode time, so an over-bound record can never enter the map.
        let encoded = MetaValue::TierCopy(max_tier_copy_record())
            .encode()
            .unwrap();
        let uri_len_at = 1 + 8 + 8 + 32 + 8 + 2 + MAX_TIER_BACKEND_ID_BYTES;
        let mut crafted = encoded[..uri_len_at].to_vec();
        crafted.extend_from_slice(&((MAX_TIER_OBJECT_URI_BYTES + 1) as u16).to_be_bytes());
        crafted.extend_from_slice("u".repeat(MAX_TIER_OBJECT_URI_BYTES + 1).as_bytes());
        crafted.extend_from_slice(&encoded[uri_len_at + 2 + MAX_TIER_OBJECT_URI_BYTES..]);
        assert!(matches!(
            MetaValue::decode(&crafted),
            Err(CodecError::BoundExceeded { .. })
        ));

        // Zero byte length is rejected: the record only ever stores verified
        // facts, and nothing verifiable has zero length.
        let mut zero_length = encoded.clone();
        zero_length[1 + 8 + 8 + 32..1 + 8 + 8 + 32 + 8].fill(0);
        assert!(matches!(
            MetaValue::decode(&zero_length),
            Err(CodecError::InvalidValue { .. })
        ));

        // An unknown verification-method tag is rejected, never defaulted.
        let method_at = 1
            + 8
            + 8
            + 32
            + 8
            + 2
            + MAX_TIER_BACKEND_ID_BYTES
            + 2
            + MAX_TIER_OBJECT_URI_BYTES
            + 1
            + 2
            + MAX_TIER_VERSION_ID_BYTES
            + 32;
        assert_eq!(encoded[method_at], 1);
        let mut unknown_method = encoded.clone();
        unknown_method[method_at] = 9;
        assert!(matches!(
            MetaValue::decode(&unknown_method),
            Err(CodecError::UnknownTag {
                what: "verification method",
                ..
            })
        ));
    }

    #[test]
    fn retention_policy_value_codec_round_trips_and_rejects_noncanonical_flags() {
        for allowed in [false, true] {
            let value = MetaValue::TopicRetentionPolicy(TopicRetentionPolicyRecord {
                generation: 3,
                unarchived_deletion_allowed: allowed,
            });
            let encoded = value.encode().unwrap();
            assert_eq!(encoded[0], VALUE_TAG_TOPIC_RETENTION_POLICY);
            assert_eq!(MetaValue::decode(&encoded).unwrap(), value);
        }
        let mut noncanonical = MetaValue::TopicRetentionPolicy(TopicRetentionPolicyRecord {
            generation: 3,
            unarchived_deletion_allowed: false,
        })
        .encode()
        .unwrap();
        *noncanonical.last_mut().unwrap() = 2;
        assert!(matches!(
            MetaValue::decode(&noncanonical),
            Err(CodecError::InvalidValue { .. })
        ));
    }

    #[test]
    fn retention_segment_states_round_trip_and_unknown_tag_nine_rejects() {
        for (state, tag) in [
            (SegmentState::RetentionPlanned, 7_u8),
            (SegmentState::RetentionExpired, 8),
        ] {
            let value = MetaValue::Segment(SegmentRecord {
                segment_generation: 4,
                base_offset: 0,
                next_offset: 64,
                content_root: [7; 32],
                state,
                sealed_by_epoch: 1,
            });
            let encoded = value.encode().unwrap();
            assert_eq!(encoded[1 + 8 + 8 + 8 + 32], tag);
            assert_eq!(MetaValue::decode(&encoded).unwrap(), value);
        }
        // The next unassigned state tag is rejected, never defaulted: a
        // snapshot from a future version must refuse to decode here.
        let mut future = MetaValue::Segment(SegmentRecord {
            segment_generation: 4,
            base_offset: 0,
            next_offset: 64,
            content_root: [7; 32],
            state: SegmentState::Verified,
            sealed_by_epoch: 1,
        })
        .encode()
        .unwrap();
        future[1 + 8 + 8 + 8 + 32] = 9;
        assert_eq!(
            MetaValue::decode(&future),
            Err(CodecError::UnknownTag {
                what: "segment state",
                tag: 9,
            })
        );
    }

    #[test]
    fn legacy_node_value_tag_decodes_with_default_placement_attrs() {
        let mut bytes = Vec::new();
        put_u8(&mut bytes, VALUE_TAG_NODE);
        put_bounded_str(&mut bytes, "10.0.0.1:9200", MAX_NODE_ADDR_BYTES, "addr").unwrap();
        put_u8(&mut bytes, 1); // Active
        put_u64(&mut bytes, 2);
        let value = MetaValue::decode(&bytes).unwrap();
        assert_eq!(
            value,
            MetaValue::Node(NodeRecord {
                addr: "10.0.0.1:9200".to_owned(),
                state: NodeState::Active,
                generation: 2,
                failure_domain: String::new(),
                placement_weight: DEFAULT_PLACEMENT_WEIGHT,
            })
        );
    }
}
