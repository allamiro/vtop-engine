//! Metadata commands, responses, and their hand-coded wire codec.
//!
//! A command is what the (future) raft log replicates; `apply` in
//! [`crate::state`] consumes exactly these types. The codec is the durable
//! byte format for `Normal` log entries, so it follows the crate's codec
//! discipline: `kind:u16` tag, big-endian integers, length-delimited bounded
//! strings, canonical option encoding, and trailing-byte rejection.
//!
//! Determinism contract: every id in a command (topic, range, segment, key,
//! request) is proposer-supplied, never allocated by the state machine.
//!
//! `issued_at_ms` is advisory for every command EXCEPT the lease-election pair
//! ([`MetadataCommand::AcquireRangeLease`] and
//! [`MetadataCommand::RenewRangeLease`], #223), where it is the time the
//! deadline is computed from AND the time an incumbent's lease is judged live
//! against. Apply stays deterministic either way — the value is data in the
//! replicated log, so every replica derives the same expiry.
//!
//! That value comes from the PROPOSER's wall clock, which used to be trusted
//! without limit: one data node with a bad clock could steal live leases
//! (clock ahead: every rival looked expired) or poison its own (clock ahead
//! while holding: the deadline sat in the future, and after it died nobody
//! could take over until the skew elapsed) (#555). The metadata leader now
//! REFUSES a lease request whose clock is further from its own than a bound
//! ([`MetadataCommand::lease_clock_skew`]), before the command is replicated.
//! A node with a bad clock cannot acquire or renew at all — loudly, with the
//! skew in the error — instead of silently moving a deadline.
//!
//! It is refused, not overwritten, on purpose: the lease agent compares the
//! recorded deadline with its own clock to decide whether to renew, so a
//! deadline silently moved into the leader's clock domain would make a
//! fast-clocked holder see its fresh lease as lapsed and re-acquire it, minting
//! epochs every round. Within the bound the proposer's value is kept, so the
//! error either side can make is at most the bound.
//!
//! Skew is bounded in consequence, not prevented: acquisition always mints a
//! higher fencing epoch, so a skewed clock can make a range change hands
//! earlier or later than intended, but can never produce two holders that both
//! believe they may write. Expiry is liveness; the epoch is safety.

use crate::keys::{MAX_GROUP_NAME_BYTES, MAX_TOPIC_NAME_BYTES};
use crate::placement::{MAX_FAILURE_DOMAIN_BYTES, MAX_REPLICAS, MIN_PLACEMENT_WEIGHT};
use crate::wire::{
    put_bounded_str, put_bytes32, put_i64, put_u16, put_u32, put_u64, put_u8, put_uuid, CodecError,
    Reader,
};
use thiserror::Error;
use uuid::Uuid;

/// Node addresses are host:port style strings, bounded like every other
/// variable-length field so a command can never smuggle unbounded bytes.
pub const MAX_NODE_ADDR_BYTES: usize = 256;

/// Bound for the human-readable detail carried by deterministic rejections.
pub const MAX_ERROR_DETAIL_BYTES: usize = 256;

/// Upper bound on ranges assigned to one consumer-group member in a single
/// durable assignment command. Keeps member records inside the snapshot value
/// bound without inviting rebalance storms in this slice.
pub const MAX_ASSIGNED_RANGES: usize = 16;

/// Bound for a tier backend identifier (`"s3-native"`, `"localfs"`, ...).
pub const MAX_TIER_BACKEND_ID_BYTES: usize = 64;

/// Bound for a tier object URI. Deliberately below S3's 1024-byte key limit
/// so a max-size [`crate::state::TierCopyRecord`] stays inside the snapshot
/// value bound with margin (the arithmetic is pinned in a state test).
pub const MAX_TIER_OBJECT_URI_BYTES: usize = 512;

/// Bound for an immutable object version id (S3 `x-amz-version-id`).
pub const MAX_TIER_VERSION_ID_BYTES: usize = 128;

const COMMAND_KIND_REGISTER_NODE: u16 = 1;
const COMMAND_KIND_SET_NODE_STATE: u16 = 2;
const COMMAND_KIND_CREATE_TOPIC: u16 = 3;
const COMMAND_KIND_GRANT_RANGE_LEASE: u16 = 4;
const COMMAND_KIND_RELEASE_RANGE_LEASE: u16 = 5;
const COMMAND_KIND_REGISTER_SEALED_SEGMENT: u16 = 6;
const COMMAND_KIND_MARK_SEGMENT_VERIFIED: u16 = 7;
const COMMAND_KIND_PUT_KEY_RECORD: u16 = 8;
const COMMAND_KIND_CREATE_CONSUMER_GROUP: u16 = 9;
const COMMAND_KIND_JOIN_CONSUMER_GROUP: u16 = 10;
const COMMAND_KIND_LEAVE_CONSUMER_GROUP: u16 = 11;
const COMMAND_KIND_ASSIGN_MEMBER_RANGES: u16 = 12;
const COMMAND_KIND_COMMIT_GROUP_CURSOR: u16 = 13;
const COMMAND_KIND_HEARTBEAT_MEMBER: u16 = 14;
const COMMAND_KIND_EXPIRE_STALE_MEMBER: u16 = 15;
const COMMAND_KIND_SET_NODE_PLACEMENT_ATTRS: u16 = 16;
const COMMAND_KIND_COMMIT_SEGMENT_PLACEMENT: u16 = 17;
const COMMAND_KIND_COMMIT_REPLACEMENT_PROOF: u16 = 18;
const COMMAND_KIND_PLAN_REPLICA_RETIREMENT: u16 = 19;
const COMMAND_KIND_CONFIRM_REPLICA_RETIRED: u16 = 20;
const COMMAND_KIND_PROPOSE_REBALANCE: u16 = 21;
const COMMAND_KIND_CANCEL_REBALANCE: u16 = 22;
const COMMAND_KIND_COMMIT_TIER_EVIDENCE: u16 = 23;
const COMMAND_KIND_SET_TOPIC_RETENTION_POLICY: u16 = 24;
const COMMAND_KIND_PLAN_RETENTION: u16 = 25;
const COMMAND_KIND_CONFIRM_RETENTION_EXPIRED: u16 = 26;
const COMMAND_KIND_CANCEL_RETENTION: u16 = 27;
// Lease election (#223). `GrantRangeLease` stays exactly as it was — an
// administrative, never-expiring grant — and these are the two commands the
// automatic election loop uses. New kinds rather than new fields on kind 4, so
// every pinned golden vector for the existing command stays byte-exact.
const COMMAND_KIND_ACQUIRE_RANGE_LEASE: u16 = 28;
const COMMAND_KIND_RENEW_RANGE_LEASE: u16 = 29;
const COMMAND_KIND_REPORT_PROMOTION_OUTCOME: u16 = 30;
const COMMAND_KIND_COMMIT_GROUP_CURSOR_FENCED: u16 = 31;
const COMMAND_KIND_ENSURE_GROUP_MEMBER_FOR_RANGE: u16 = 32;
const COMMAND_KIND_COMMIT_GROUP_CURSOR_COORDINATED: u16 = 33;
const COMMAND_KIND_ENSURE_GROUP_MEMBER_COORDINATED: u16 = 34;
const COMMAND_KIND_SPLIT_RANGE: u16 = 35;
const COMMAND_KIND_SET_FEATURE_LEVEL: u16 = 36;
const COMMAND_KIND_SET_RANGE_REPLICA_SET: u16 = 37;
const COMMAND_KIND_REPORT_NODE_VERSION: u16 = 38;
const COMMAND_KIND_ATTEST_UNPROVEN_PROMOTION: u16 = 39;

/// The metadata feature level of a cluster that has never recorded one: the
/// command set of v0.6.0, the last release before levels existed (#554).
///
/// A command kind an old voter does not know fails to decode, and with it the
/// whole AppendEntries batch carrying it: a leader that proposes one during a
/// rolling upgrade leaves every old voter unable to append. So a command
/// newer than the cluster's level is refused BEFORE it reaches the log, and
/// the level is raised - by [`MetadataCommand::SetFeatureLevel`], once every
/// voter runs a release that knows it - never implied by the leader's build.
pub const BASELINE_FEATURE_LEVEL: u32 = 3;

/// The highest feature level this build understands.
///
/// Raise it, and give the new kinds that level in
/// [`MetadataCommand::feature_level`], whenever a release adds anything an
/// older voter cannot read: a command kind, a snapshot record tag, or a
/// change to what an existing command does when applied (gate the new
/// behaviour on the recorded level, so every replica applies the same
/// thing). `every_kind_newer_than_the_baseline_release_is_gated` catches a
/// kind that forgets; the other two are on the author.
pub const SUPPORTED_FEATURE_LEVEL: u32 = 8;

/// The level at which a promotion report may carry a lineage verdict on each
/// quorum answer (#577). Gated by PAYLOAD rather than by kind: the command is
/// still `ReportPromotionOutcome` (kind 30), but its lineage-carrying outcome
/// is a tag an older voter does not know, so it is held back exactly as a new
/// kind is (see [`MetadataCommand::feature_level`]).
pub const LINEAGE_VERDICTS_FEATURE_LEVEL: u32 = 6;

/// The level at which a data node may report its release and what it can run
/// (#634): [`MetadataCommand::ReportNodeVersion`].
///
/// Also the last level whose raise is judged on the metadata tier alone. Data
/// nodes cannot report before this level is in force - the report is a
/// command an older voter cannot decode, so it is held back like any other -
/// so nothing could vouch for them when it is raised. Every level ABOVE this
/// one is refused while a counted node has not reported it or higher (see
/// [`crate::versions::ClusterVersions::blockers`]), and this level changes
/// nothing on the replica plane, so raising it cannot cut a peer off.
pub const NODE_VERSION_REPORTS_FEATURE_LEVEL: u32 = 7;

/// Bound for the release string a data node reports (#634): a version such
/// as `0.8.0`, or one with a build suffix. Bounded like every other string a
/// command carries.
pub const MAX_RELEASE_BYTES: usize = 64;

/// The level of the operator's last-resort override for a range whose
/// replicas cannot prove lineage (#636): the attestation command
/// ([`MetadataCommand::AttestUnprovenPromotion`]), a promotion report that
/// carries the attestation it used, and the
/// [`PromotionRefusal::LineageUnproven`] reason. All three are payloads an
/// older voter cannot decode, so each is held back below this level exactly
/// as a new kind is.
pub const PROMOTE_UNPROVEN_FEATURE_LEVEL: u32 = 8;

/// Bound on the operator name an attestation records (#636).
pub const MAX_ATTESTATION_OPERATOR_BYTES: usize = 128;

/// Why a fenced cursor commit is refused (#457 slice 2b): the commit did not
/// come from the range's current leaseholder at its current fencing epoch. A
/// constant, so a gateway can tell this refusal from any other
/// `InvalidTransition` without parsing prose.
pub const NOT_LEASEHOLDER: &str =
    "the commit is not the range's leaseholder's at the current fencing epoch";

/// Why a cursor commit is refused when the member no longer holds the range
/// (#457 slice 2b): a constant, so a gateway can tell "stand again" from a
/// position the plane will not take.
pub const NOT_ASSIGNED: &str = "member is not assigned the cursor topic/range";

const RESPONSE_KIND_ACK: u16 = 1;
const RESPONSE_KIND_TOPIC_CREATED: u16 = 2;
const RESPONSE_KIND_LEASE_GRANTED: u16 = 3;
const RESPONSE_KIND_REJECTED: u16 = 4;
const RESPONSE_KIND_GROUP_CREATED: u16 = 5;
const RESPONSE_KIND_MEMBER_JOINED: u16 = 6;
const RESPONSE_KIND_CURSOR_COMMITTED: u16 = 7;
const RESPONSE_KIND_TRANSITION_RECORDED: u16 = 8;

const ERROR_KIND_GENERATION_MISMATCH: u16 = 1;
const ERROR_KIND_EPOCH_MISMATCH: u16 = 2;
const ERROR_KIND_ALREADY_EXISTS: u16 = 3;
const ERROR_KIND_NOT_FOUND: u16 = 4;
const ERROR_KIND_INVALID_TRANSITION: u16 = 5;
const ERROR_KIND_LIMIT: u16 = 6;
const ERROR_KIND_LINEAGE_MISMATCH: u16 = 7;

/// Common prefix of every command. `request_id` keys the exactly-once dedup
/// table.
///
/// `issued_at_ms` is advisory for every command except the lease-election pair
/// (#223), which derives a lease deadline from it. For that pair the metadata
/// leader refuses a value too far from its own clock before the command is
/// replicated (#555) — see the module docs.
///
/// Every other command keeps the proposer's value as an audit timestamp: no
/// decision in the state machine reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandEnvelope {
    pub request_id: Uuid,
    pub issued_at_ms: i64,
}

/// The smallest clock error between a lease proposer and the metadata leader
/// that is always tolerated, whatever the lease length. A request spends
/// time on the wire and in a queue, and NTP-synchronised hosts agree to
/// milliseconds; one second is far above both and far below any lease.
pub const MIN_LEASE_SKEW_BOUND_MS: u64 = 1_000;

/// The clock error tolerated between a lease proposer and the metadata leader
/// for a lease of `lease_duration_ms`: a twentieth of the lease, never less
/// than [`MIN_LEASE_SKEW_BOUND_MS`] - except that it is never more than a
/// tenth of the lease, which for a lease under ten seconds is below the floor
/// and wins.
///
/// A deadline is the proposer's `issued_at_ms` plus the lease, and a rival's
/// acquire is judged against it by the rival's own `issued_at_ms`. A holder
/// slow by the bound and a rival fast by it therefore see a lease that is
/// shorter, in real time, by TWICE the bound - which the holder's renewal
/// pacing must absorb on top of a lost round
/// (`LeaseAgentConfig::validate`). At a quarter of the lease, as it was, the
/// default 15 s lease kept only 7.5 s, less than one lost 5 s round plus its
/// retry (review of #566). NTP-synchronised hosts agree to milliseconds, so
/// the floor is still far above honest error. The cap keeps the two-sided
/// loss to a fifth of any lease: the live-chaos pacing (6 s lease, 2 s renew,
/// 0.5 s poll) keeps 4.8 s against the 4.5 s one lost round needs, where the
/// floor alone would leave 4 s.
///
/// The skew is measured when the request reaches the leader, so time on the
/// wire counts against it, always in the direction of the proposer looking
/// behind. A request whose timestamp is older than the tolerated skew when it
/// arrives is refused and retried next round with a fresh one; its deadline
/// is not yet stale, but it can no longer be told apart from a slow clock.
pub fn lease_skew_bound_ms(lease_duration_ms: u64) -> u64 {
    (lease_duration_ms / 20)
        .max(MIN_LEASE_SKEW_BOUND_MS)
        .min(lease_duration_ms / 10)
}

/// How far a lease proposer's clock was from the metadata leader's, against
/// what the leader tolerates for that request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseClockSkew {
    /// The request this was measured for, so a caller can tell a request that
    /// already committed (answered from the dedup table) from a new one.
    pub request_id: Uuid,
    /// Proposer's `issued_at_ms` minus the leader's clock. Positive: the
    /// proposer is ahead.
    pub skew_ms: i64,
    /// The largest error tolerated for this request.
    pub bound_ms: u64,
}

impl LeaseClockSkew {
    pub fn is_within_bound(&self) -> bool {
        self.skew_ms.unsigned_abs() <= self.bound_ms
    }
}

impl MetadataCommand {
    /// For the lease-election pair, how far the proposer's clock is from
    /// `leader_now_ms` and how much the leader tolerates; `None` for every
    /// other command, whose outcome never reads `issued_at_ms`.
    ///
    /// The tolerance is [`lease_skew_bound_ms`] of the requested lease. A
    /// proposer's error then moves a deadline by at most that much, and two
    /// proposers' errors move a takeover by at most twice it, which
    /// `LeaseAgentConfig::validate` counts against the renewal pacing.
    pub fn lease_clock_skew(&self, leader_now_ms: i64) -> Option<LeaseClockSkew> {
        let (env, lease_duration_ms) = match self {
            MetadataCommand::AcquireRangeLease {
                env,
                lease_duration_ms,
                ..
            }
            | MetadataCommand::RenewRangeLease {
                env,
                lease_duration_ms,
                ..
            } => (env, *lease_duration_ms),
            _ => return None,
        };
        Some(LeaseClockSkew {
            request_id: env.request_id,
            skew_ms: env.issued_at_ms.saturating_sub(leader_now_ms),
            bound_ms: lease_skew_bound_ms(lease_duration_ms),
        })
    }
}

/// Lifecycle state of a registered node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeState {
    Active,
    Draining,
    Dead,
}

impl NodeState {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeState::Active => "active",
            NodeState::Draining => "draining",
            NodeState::Dead => "dead",
        }
    }

    pub(crate) fn wire_tag(self) -> u8 {
        match self {
            NodeState::Active => 1,
            NodeState::Draining => 2,
            NodeState::Dead => 3,
        }
    }

    pub(crate) fn from_wire(tag: u8) -> Result<Self, CodecError> {
        match tag {
            1 => Ok(NodeState::Active),
            2 => Ok(NodeState::Draining),
            3 => Ok(NodeState::Dead),
            other => Err(CodecError::UnknownTag {
                what: "node state",
                tag: u32::from(other),
            }),
        }
    }
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// How replacement bytes were verified before retirement may be planned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerificationMethod {
    /// Destination bytes match the authenticated segment content root.
    AuthenticatedContentRoot,
}

impl VerificationMethod {
    fn wire_tag(self) -> u8 {
        match self {
            VerificationMethod::AuthenticatedContentRoot => 1,
        }
    }

    pub(crate) fn from_wire(tag: u8) -> Result<Self, CodecError> {
        match tag {
            1 => Ok(VerificationMethod::AuthenticatedContentRoot),
            other => Err(CodecError::UnknownTag {
                what: "verification method",
                tag: u32::from(other),
            }),
        }
    }
}

/// A topic/range pair assigned to a consumer-group member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RangeAssignment {
    pub topic_uuid: Uuid,
    pub range_uuid: Uuid,
}

/// The UUID a Kafka-named consumer group has on this plane (#457 slice 2b):
/// a gateway names a group by the name its clients use, the plane by UUID,
/// and one derives the other under the cluster id — so every node of the
/// cluster, and an operator's tool, agree on it without a lookup.
pub fn derived_group_uuid(cluster_id: Uuid, name: &str) -> Uuid {
    Uuid::new_v5(&cluster_id, name.as_bytes())
}

/// Bound on the fenced-quorum answers a promotion report may carry: the
/// replica set plus the transient extra a rebalance holds, with room.
///
/// It also sizes the largest transition record, which a snapshot must carry
/// (#657): 881 bytes without verdicts, 1169 with a reconciled verdict on
/// every answer. Raising it, or adding to what an answer carries, has to keep
/// `the_largest_record_of_every_kind_fits_the_snapshot_value_bound` in
/// `state.rs` passing.
pub const MAX_TRANSITION_QUORUM: usize = 32;

/// One fenced replica's answer at promotion, as the transition record keeps
/// it (#240 item 5): who was asked, and the committed offset it reported
/// once fenced at the new epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuorumAnswer {
    pub node_uuid: Uuid,
    pub offset: u64,
    /// Whether the offset describes the CANDIDATE's history (#577). `None`
    /// is a record written before verdicts existed (or at a feature level
    /// below [`LINEAGE_VERDICTS_FEATURE_LEVEL`]): judged exactly as it always
    /// was, by offset.
    pub lineage: Option<AnswerLineage>,
}

/// What the candidate could prove about one fenced replica's history, next
/// to the offset it answered with (#577).
///
/// An offset is a count of records, not an identity: two replicas at 90 need
/// not hold the same 90 records. The fence reconciles each replica against
/// the candidate's epoch vector before it answers, so where lineage is
/// provable the offset describes the candidate's history - but where it is
/// not (a replica with no epoch history, a broken journal, a candidate that
/// cannot vouch for its own) nothing is reconciled and the offset describes
/// whatever wrote it. Readers count only the first two.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnswerLineage {
    /// The replica's history agrees with the candidate's for every record it
    /// holds. Nothing was discarded.
    Agreed,
    /// The replica diverged, discarded the records written under a
    /// leadership the candidate does not share, and then agreed.
    Reconciled { truncated_records: u64 },
    /// Nothing could be proven either way. The offset is not counted.
    Unknown,
}

impl AnswerLineage {
    /// Whether an answer with this verdict counts toward a quorum.
    pub fn is_provable(self) -> bool {
        !matches!(self, AnswerLineage::Unknown)
    }
}

impl QuorumAnswer {
    /// Whether this answer counts toward a quorum: a verdict that proves the
    /// candidate's lineage, or no verdict at all - a record that predates
    /// verdicts is judged as it always was (#577).
    pub fn counts(&self) -> bool {
        self.lineage.is_none_or(AnswerLineage::is_provable)
    }
}

/// Why a promotion was refused, as the transition record keeps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PromotionRefusal {
    /// Too few replicas could be fenced and read to establish a boundary.
    QuorumUnavailable,
    /// The quorum proved a boundary the candidate's own log does not reach.
    LeaderBehind,
    /// Fewer than a majority of the fenced replicas were at or below the
    /// candidate's own offset (Raft §5.4.1).
    CandidateBehindVoters,
    /// The quorum's boundary fell below a watermark an earlier promotion of
    /// the range published (#449).
    BelowPublished,
    /// Enough replicas were fenced and answered to form a quorum, but too
    /// few of the answers had a lineage the candidate could prove (#577,
    /// #636): the refusal is about history, not reachability, and waiting
    /// for replicas does not clear it. Recorded only at
    /// [`PROMOTE_UNPROVEN_FEATURE_LEVEL`]; below it the same refusal is
    /// recorded as [`PromotionRefusal::QuorumUnavailable`], as it always was.
    LineageUnproven,
}

impl PromotionRefusal {
    fn wire_tag(self) -> u8 {
        match self {
            PromotionRefusal::QuorumUnavailable => 1,
            PromotionRefusal::LeaderBehind => 2,
            PromotionRefusal::CandidateBehindVoters => 3,
            PromotionRefusal::BelowPublished => 4,
            PromotionRefusal::LineageUnproven => 5,
        }
    }

    /// The name operators see in logs, metrics and `vtopctl` output.
    pub fn as_str(self) -> &'static str {
        match self {
            PromotionRefusal::QuorumUnavailable => "quorum_unavailable",
            PromotionRefusal::LeaderBehind => "leader_behind",
            PromotionRefusal::CandidateBehindVoters => "candidate_behind_voters",
            PromotionRefusal::BelowPublished => "below_published",
            PromotionRefusal::LineageUnproven => "lineage_unproven",
        }
    }

    pub(crate) fn from_wire(tag: u8) -> Result<Self, CodecError> {
        match tag {
            1 => Ok(PromotionRefusal::QuorumUnavailable),
            2 => Ok(PromotionRefusal::LeaderBehind),
            3 => Ok(PromotionRefusal::CandidateBehindVoters),
            4 => Ok(PromotionRefusal::BelowPublished),
            5 => Ok(PromotionRefusal::LineageUnproven),
            other => Err(CodecError::UnknownTag {
                what: "promotion refusal",
                tag: u32::from(other),
            }),
        }
    }
}

/// An operator's statement that one replica's log is to become a range's
/// canonical history although its lineage cannot be proven (#636): the
/// last-resort override for a range whose replicas have no epoch history,
/// in the spirit of an unclean leader election, and as explicit.
///
/// Recorded by [`MetadataCommand::AttestUnprovenPromotion`], never by
/// configuration or by any automatic path. It lets exactly ONE promotion of
/// the range, by the replica it names, count answers whose lineage is
/// Unknown. That promotion carries it into the transition record, and
/// metadata removes it in the same apply, so a second promotion needs a new
/// one. Records the named replica does not hold are lost; the operator
/// accepts that by recording it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PromotionAttestation {
    /// The attesting command's request id: the attestation's identity, so
    /// a report names exactly the one it used.
    pub attestation_id: Uuid,
    /// The replica whose log becomes canonical.
    pub replica_node_uuid: Uuid,
    /// Who attested, as they named themselves to `vtopctl`.
    pub operator: String,
    /// The attesting command's `issued_at_ms`: the operator's clock, kept
    /// for the audit and read by no decision.
    pub attested_at_ms: i64,
}

impl PromotionAttestation {
    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_uuid(out, self.attestation_id);
        put_uuid(out, self.replica_node_uuid);
        put_bounded_str(
            out,
            &self.operator,
            MAX_ATTESTATION_OPERATOR_BYTES,
            "attestation operator",
        )?;
        put_i64(out, self.attested_at_ms);
        Ok(())
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            attestation_id: reader.uuid("attestation id")?,
            replica_node_uuid: reader.uuid("attested replica")?,
            operator: reader.bounded_str(MAX_ATTESTATION_OPERATOR_BYTES, "attestation operator")?,
            attested_at_ms: reader.i64("attested at")?,
        })
    }
}

/// What a grant became, reported by its holder (#240 item 5): the evidence
/// the promotion computed and used to be discarded. `Established` asserts
/// exactly what the verification PROVED — a quorum was fenced and read, a
/// boundary was adopted, the §5.4.1 vote passed — and nothing more: not
/// that the holder went on to serve, which no record could assert (a leader
/// can die the millisecond after it activates, and a candidate whose lease
/// moved during its build stands back down with the proof intact). A chain
/// reader who needs "did it serve" reads the next link: a holder displaced
/// at once shows as `holder_from` of an epoch minted moments later.
/// Everything in it is recomputable by a reader holding the record and the
/// replicas — `votes` and `required` are carried so a checker can recompute
/// them from `quorum` rather than trust them, and `boundary_offset` is
/// `None` only for a standalone promotion, where there is no quorum and the
/// node's own log is the boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PromotionOutcome {
    Established {
        boundary_offset: Option<u64>,
        /// Sealed-segment identity at the transition (#306), when the
        /// holder had one to report.
        sealed_prefix_end: Option<u64>,
        quorum: Vec<QuorumAnswer>,
        votes: u32,
        required: u32,
        /// The operator's attestation this promotion counted Unknown
        /// answers under (#636); `None` for every promotion that proved its
        /// quorum's lineage, which is all of them but the override. Present
        /// only with lineage verdicts, so the record shows exactly which
        /// answers were counted on the operator's word.
        attestation: Option<PromotionAttestation>,
    },
    Refused {
        reason: PromotionRefusal,
    },
}

impl PromotionOutcome {
    /// The feature level a cluster must be at to record this outcome: the
    /// level of the oldest release that can decode it.
    pub fn feature_level(&self) -> u32 {
        match self {
            PromotionOutcome::Established {
                attestation: Some(_),
                ..
            }
            | PromotionOutcome::Refused {
                reason: PromotionRefusal::LineageUnproven,
            } => PROMOTE_UNPROVEN_FEATURE_LEVEL,
            outcome if outcome.records_lineage() => LINEAGE_VERDICTS_FEATURE_LEVEL,
            // v0.5.0 (#240 item 5).
            _ => 2,
        }
    }

    /// The attestation this promotion used, if it was the override (#636).
    pub fn attestation(&self) -> Option<&PromotionAttestation> {
        match self {
            PromotionOutcome::Established { attestation, .. } => attestation.as_ref(),
            PromotionOutcome::Refused { .. } => None,
        }
    }

    /// This outcome as a cluster below [`PROMOTE_UNPROVEN_FEATURE_LEVEL`]
    /// records it (#636): [`PromotionRefusal::LineageUnproven`] becomes the
    /// [`PromotionRefusal::QuorumUnavailable`] it was recorded as before
    /// the reason existed. Every other outcome is returned unchanged.
    pub fn without_unproven_refusal(&self) -> PromotionOutcome {
        match self {
            PromotionOutcome::Refused {
                reason: PromotionRefusal::LineageUnproven,
            } => PromotionOutcome::Refused {
                reason: PromotionRefusal::QuorumUnavailable,
            },
            other => other.clone(),
        }
    }

    /// Whether this outcome carries lineage verdicts on its quorum answers -
    /// the payload only a cluster at [`LINEAGE_VERDICTS_FEATURE_LEVEL`] may
    /// record (#577).
    pub fn records_lineage(&self) -> bool {
        matches!(
            self,
            PromotionOutcome::Established { quorum, .. }
                if quorum.iter().any(|answer| answer.lineage.is_some())
        )
    }

    /// This outcome as a cluster below [`LINEAGE_VERDICTS_FEATURE_LEVEL`] can
    /// record it: no verdicts, and NO Unknown answers (#577).
    ///
    /// Dropping the Unknown answers is what keeps the legacy form safe, not a
    /// loss of evidence that matters to a reader: every reader of a record
    /// without verdicts counts every answer by its offset, which is the
    /// exact mistake verdicts exist to prevent. Left in, an Unknown answer
    /// would vote in the recomputed §5.4.1 count and in the replica-set gate
    /// (`established_under`) as though it held the candidate's log. The
    /// promoter never counted them, so `votes`, `required` and the boundary
    /// recompute from what remains exactly as they were computed.
    ///
    /// An outcome that used an attestation (#636) DID count its Unknown
    /// answers and has no legacy form: it is returned unchanged, and stays
    /// refused below its level.
    pub fn without_lineage(&self) -> PromotionOutcome {
        match self {
            PromotionOutcome::Established {
                attestation: Some(_),
                ..
            } => self.clone(),
            PromotionOutcome::Established {
                boundary_offset,
                sealed_prefix_end,
                quorum,
                votes,
                required,
                attestation: None,
            } => PromotionOutcome::Established {
                boundary_offset: *boundary_offset,
                sealed_prefix_end: *sealed_prefix_end,
                quorum: quorum
                    .iter()
                    .filter(|answer| answer.counts())
                    .map(|answer| QuorumAnswer {
                        lineage: None,
                        ..*answer
                    })
                    .collect(),
                votes: *votes,
                required: *required,
                attestation: None,
            },
            refused @ PromotionOutcome::Refused { .. } => refused.clone(),
        }
    }
}

/// `PromotionOutcome::Established` without lineage verdicts: the form every
/// release since v0.5.0 reads.
const PROMOTION_OUTCOME_ESTABLISHED: u8 = 1;
const PROMOTION_OUTCOME_REFUSED: u8 = 2;
/// `PromotionOutcome::Established` with a lineage verdict after each quorum
/// answer (#577). A new tag rather than a new field, so every existing record
/// keeps its bytes; recorded only at [`LINEAGE_VERDICTS_FEATURE_LEVEL`].
const PROMOTION_OUTCOME_ESTABLISHED_WITH_LINEAGE: u8 = 3;
/// The lineage-carrying form followed by the attestation the promotion used
/// (#636). Recorded only at [`PROMOTE_UNPROVEN_FEATURE_LEVEL`].
const PROMOTION_OUTCOME_ESTABLISHED_UNDER_ATTESTATION: u8 = 4;

const ANSWER_LINEAGE_AGREED: u8 = 1;
const ANSWER_LINEAGE_RECONCILED: u8 = 2;
const ANSWER_LINEAGE_UNKNOWN: u8 = 3;

pub(crate) fn encode_promotion_outcome(
    out: &mut Vec<u8>,
    outcome: &PromotionOutcome,
) -> Result<(), CodecError> {
    match outcome {
        PromotionOutcome::Established {
            boundary_offset,
            sealed_prefix_end,
            quorum,
            votes,
            required,
            attestation,
        } => {
            // Verdicts on every answer or on none: a record half of whose
            // answers carry one would be read by no reader correctly.
            let with_lineage = outcome.records_lineage();
            if with_lineage && quorum.iter().any(|answer| answer.lineage.is_none()) {
                return Err(CodecError::InvalidValue {
                    what: "promotion quorum answers",
                    reason: "a lineage verdict must be on every answer or on none",
                });
            }
            // An attestation says which answers were counted on the
            // operator's word, which only verdicts can show (#636).
            if attestation.is_some() && !with_lineage {
                return Err(CodecError::InvalidValue {
                    what: "promotion attestation",
                    reason: "an attested promotion carries a lineage verdict on every answer",
                });
            }
            put_u8(
                out,
                if attestation.is_some() {
                    PROMOTION_OUTCOME_ESTABLISHED_UNDER_ATTESTATION
                } else if with_lineage {
                    PROMOTION_OUTCOME_ESTABLISHED_WITH_LINEAGE
                } else {
                    PROMOTION_OUTCOME_ESTABLISHED
                },
            );
            encode_optional_u64(out, *boundary_offset);
            encode_optional_u64(out, *sealed_prefix_end);
            if quorum.len() > MAX_TRANSITION_QUORUM {
                return Err(CodecError::BoundExceeded {
                    what: "promotion quorum answers",
                    actual: quorum.len(),
                    maximum: MAX_TRANSITION_QUORUM,
                });
            }
            put_u16(out, quorum.len() as u16);
            for answer in quorum {
                put_uuid(out, answer.node_uuid);
                put_u64(out, answer.offset);
                match answer.lineage {
                    None => {}
                    Some(AnswerLineage::Agreed) => put_u8(out, ANSWER_LINEAGE_AGREED),
                    Some(AnswerLineage::Reconciled { truncated_records }) => {
                        put_u8(out, ANSWER_LINEAGE_RECONCILED);
                        put_u64(out, truncated_records);
                    }
                    Some(AnswerLineage::Unknown) => put_u8(out, ANSWER_LINEAGE_UNKNOWN),
                }
            }
            put_u32(out, *votes);
            put_u32(out, *required);
            if let Some(attestation) = attestation {
                attestation.encode_into(out)?;
            }
        }
        PromotionOutcome::Refused { reason } => {
            put_u8(out, PROMOTION_OUTCOME_REFUSED);
            put_u8(out, reason.wire_tag());
        }
    }
    Ok(())
}

pub(crate) fn decode_promotion_outcome(
    reader: &mut Reader<'_>,
) -> Result<PromotionOutcome, CodecError> {
    match reader.u8("promotion outcome")? {
        tag @ (PROMOTION_OUTCOME_ESTABLISHED
        | PROMOTION_OUTCOME_ESTABLISHED_WITH_LINEAGE
        | PROMOTION_OUTCOME_ESTABLISHED_UNDER_ATTESTATION) => {
            let attested = tag == PROMOTION_OUTCOME_ESTABLISHED_UNDER_ATTESTATION;
            let with_lineage = attested || tag == PROMOTION_OUTCOME_ESTABLISHED_WITH_LINEAGE;
            let boundary_offset = decode_optional_u64(reader, "promotion boundary offset")?;
            let sealed_prefix_end = decode_optional_u64(reader, "promotion sealed prefix end")?;
            let count = reader.u16("promotion quorum answer count")? as usize;
            if count > MAX_TRANSITION_QUORUM {
                return Err(CodecError::BoundExceeded {
                    what: "promotion quorum answers",
                    actual: count,
                    maximum: MAX_TRANSITION_QUORUM,
                });
            }
            let mut quorum = Vec::with_capacity(count);
            for _ in 0..count {
                let node_uuid = reader.uuid("promotion quorum node")?;
                let offset = reader.u64("promotion quorum offset")?;
                let lineage = if with_lineage {
                    Some(match reader.u8("quorum answer lineage")? {
                        ANSWER_LINEAGE_AGREED => AnswerLineage::Agreed,
                        ANSWER_LINEAGE_RECONCILED => AnswerLineage::Reconciled {
                            truncated_records: reader.u64("quorum answer truncated records")?,
                        },
                        ANSWER_LINEAGE_UNKNOWN => AnswerLineage::Unknown,
                        other => {
                            return Err(CodecError::UnknownTag {
                                what: "quorum answer lineage",
                                tag: u32::from(other),
                            })
                        }
                    })
                } else {
                    None
                };
                quorum.push(QuorumAnswer {
                    node_uuid,
                    offset,
                    lineage,
                });
            }
            // A lineage-tagged outcome with no answers would decode to the
            // same value as the legacy tag and re-encode differently, so a
            // MAC over the re-encoding (#240 item 5) could not verify it.
            if with_lineage && quorum.is_empty() {
                return Err(CodecError::InvalidValue {
                    what: "promotion quorum answers",
                    reason: "a lineage-carrying outcome must carry at least one answer",
                });
            }
            let votes = reader.u32("promotion votes")?;
            let required = reader.u32("promotion required votes")?;
            let attestation = if attested {
                Some(PromotionAttestation::decode_from(reader)?)
            } else {
                None
            };
            Ok(PromotionOutcome::Established {
                boundary_offset,
                sealed_prefix_end,
                quorum,
                votes,
                required,
                attestation,
            })
        }
        PROMOTION_OUTCOME_REFUSED => Ok(PromotionOutcome::Refused {
            reason: PromotionRefusal::from_wire(reader.u8("promotion refusal")?)?,
        }),
        other => Err(CodecError::UnknownTag {
            what: "promotion outcome",
            tag: u32::from(other),
        }),
    }
}

/// The full deterministic command set of stage-5 PR 1 plus stage-7 group and
/// lineage-aware cursor commands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataCommand {
    RegisterNode {
        env: CommandEnvelope,
        node_uuid: Uuid,
        addr: String,
        /// `None` expects the node to be absent (first registration);
        /// `Some(generation)` is a CAS re-registration of an existing node.
        expected_generation: Option<u64>,
    },
    SetNodeState {
        env: CommandEnvelope,
        node_uuid: Uuid,
        state: NodeState,
        expected_generation: u64,
    },
    CreateTopic {
        env: CommandEnvelope,
        name: String,
        topic_uuid: Uuid,
        root_range_uuid: Uuid,
    },
    GrantRangeLease {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        holder_node_uuid: Uuid,
        expected_range_generation: u64,
    },
    ReleaseRangeLease {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        expected_fencing_epoch: u64,
    },
    /// Take range leadership, respecting an unexpired lease held by someone
    /// else (#223).
    ///
    /// The election path, as distinct from [`MetadataCommand::GrantRangeLease`]
    /// — which is the administrative grant and carries no expiry. Raft makes
    /// this linearizable, so exactly one candidate can win a term; the mint of
    /// `fencing_epoch + 1` fences the previous holder by construction, with no
    /// unclean-election knob to misconfigure.
    ///
    /// `lease_duration_ms` is added to the envelope's `issued_at_ms` to form
    /// the deadline. Both are data in the replicated log, so every replica
    /// computes the same expiry — the state machine never reads a local clock.
    AcquireRangeLease {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        holder_node_uuid: Uuid,
        expected_range_generation: u64,
        lease_duration_ms: u64,
    },
    /// Extend the current holder's deadline without minting a new epoch, so a
    /// live leader keeps serving across renewals (#223).
    ///
    /// Rejected unless the caller is the recorded holder AND names the current
    /// epoch: a renewal from a leader that has already been fenced must fail,
    /// or a partitioned old leader could keep its lease alive forever.
    RenewRangeLease {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        holder_node_uuid: Uuid,
        expected_fencing_epoch: u64,
        lease_duration_ms: u64,
    },
    /// Report what a grant became (#240 item 5): the holder of
    /// `fencing_epoch` records the evidence its promotion computed — the
    /// fenced quorum, the boundary it adopted, the §5.4.1 vote — or the
    /// refusal that made it stand aside. Every grant already has a
    /// transition record from the moment it is minted, so this only ever
    /// fills in an outcome; a record whose holder never reports stays
    /// visibly `Pending`, which is the honest shape of an epoch nobody
    /// served under.
    ///
    /// Node-scoped: only the holder the epoch was granted to may report on
    /// it, and an established transition is final — a later report cannot
    /// rewrite what was proven.
    ReportPromotionOutcome {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        holder_node_uuid: Uuid,
        fencing_epoch: u64,
        outcome: PromotionOutcome,
    },
    /// Split a range at a barrier: seal the parent's offset space at the
    /// committed tail its leaseholder names, and create two children that
    /// halve its key interval (#473 slice 1). Fenced exactly as
    /// [`MetadataCommand::RegisterSealedSegment`] is — the plane takes it
    /// only from the parent's current leaseholder at its current epoch,
    /// CAS'd on the parent's record generation — because the barrier offset
    /// is a claim about the log, and the lease fence is what makes that
    /// claim exactly-one-writer.
    ///
    /// `transition_id` names the lineage transition durably: a replay with
    /// the SAME id is an idempotent Ack, a second split with a different id
    /// is refused by name. The transition record it mints is the object the
    /// cursor carry (#468's rule, slice 2) checks.
    SplitRange {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        parent_range_uuid: Uuid,
        left_range_uuid: Uuid,
        right_range_uuid: Uuid,
        transition_id: Uuid,
        /// The parent leader's committed tail at seal time. Children start
        /// their own logs; this is where the parent's readable history ends
        /// and the mapping the cursor carry consumes begins.
        barrier_offset: u64,
        expected_range_generation: u64,
        /// The node splitting, which must hold the parent's lease...
        holder_node_uuid: Uuid,
        /// ...at this fencing epoch, the one the plane granted it.
        fencing_epoch: u64,
    },
    RegisterSealedSegment {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        segment_generation: u64,
        base_offset: u64,
        next_offset: u64,
        content_root: [u8; 32],
        sealed_by_epoch: u64,
        expected_range_generation: u64,
    },
    MarkSegmentVerified {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        content_root: [u8; 32],
        expected_generation: u64,
    },
    PutKeyRecord {
        env: CommandEnvelope,
        key_uuid: Uuid,
        scheme: u16,
        public_material_digest: [u8; 32],
    },
    CreateConsumerGroup {
        env: CommandEnvelope,
        name: String,
        group_uuid: Uuid,
    },
    JoinConsumerGroup {
        env: CommandEnvelope,
        group_uuid: Uuid,
        member_uuid: Uuid,
        expected_group_generation: u64,
    },
    LeaveConsumerGroup {
        env: CommandEnvelope,
        group_uuid: Uuid,
        member_uuid: Uuid,
        expected_member_generation: u64,
    },
    AssignMemberRanges {
        env: CommandEnvelope,
        group_uuid: Uuid,
        member_uuid: Uuid,
        ranges: Vec<RangeAssignment>,
        expected_member_generation: u64,
    },
    /// Durably commit a lineage-aware cursor. `expected_checkpoint_generation`
    /// is `None` for the first commit (cursor must be absent) and `Some(g)` for
    /// a CAS advance against an existing checkpoint. A nil `segment_uuid` (with
    /// zero generation, root and index) is an UNPINNED cursor (#457 slice 2b):
    /// bound to the topic epoch and lineage generation, the record offset its
    /// position, no segment named — a Kafka gateway's commit at the head.
    CommitGroupCursor {
        env: CommandEnvelope,
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
    },
    /// Refresh ephemeral member liveness. Does not bump member generation and
    /// never mutates durable cursors.
    HeartbeatMember {
        env: CommandEnvelope,
        group_uuid: Uuid,
        member_uuid: Uuid,
    },
    /// The membership a range's gateway needs, in one fenced step (#457 slice
    /// 2b): the group exists, this member is in it, and it holds this range.
    /// Idempotent and deterministic — the state machine sees what stands and
    /// makes the rest so, with no compare-and-set for the caller to lose a
    /// race on. Node-scoped and fenced by the lease, like the commit it
    /// precedes: a node that does not hold the range may not take a group's
    /// membership on it, and a data node's own certificate is enough to
    /// submit it (the create/join/assign trio it replaces is cluster-scoped,
    /// an operator's).
    EnsureGroupMemberForRange {
        env: CommandEnvelope,
        /// The group's Kafka name, as the gateway derives its UUID from.
        name: String,
        group_uuid: Uuid,
        member_uuid: Uuid,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        holder_node_uuid: Uuid,
        fencing_epoch: u64,
    },
    /// `CommitGroupCursor` from a range's leaseholder (#457 slice 2b): the
    /// same commit, fenced by the lease. The plane takes it only from the
    /// range's current holder at its current fencing epoch — the gate
    /// `RegisterSealedSegment` has — so a leader whose lease was stolen while
    /// its Kafka listener stayed reachable cannot move a group's position
    /// after its successor has. A new kind rather than new fields on the old
    /// one: entries already in a log keep their shape, and a plane that
    /// predates it refuses the kind it does not know.
    CommitGroupCursorFenced {
        env: CommandEnvelope,
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
        /// The node committing, which must hold the range's lease...
        holder_node_uuid: Uuid,
        /// ...at this fencing epoch, the one the plane granted it.
        fencing_epoch: u64,
    },
    /// `CommitGroupCursorFenced` from the group's COORDINATOR (#457 slice 4):
    /// the same commit, on a range the committer does not lead, fenced by the
    /// one it does.
    ///
    /// A Kafka consumer group spans every partition of a topic and ONE broker
    /// coordinates it. A Kafka partition is a vtop range of its own — today a
    /// whole topic of its own — so the coordinator must store the offset of a
    /// range another node leads, whose lease it will never hold. Fencing that
    /// commit on the target range's lease would make it impossible; fencing
    /// it on nothing would let any reachable gateway move any group's
    /// position. So the fence moves to the range the committer DOES lead, the
    /// one that makes it the coordinator at all.
    ///
    /// That is the whole property, and it is the one that matters: a
    /// coordinator is deposed exactly by losing its own lease, and from that
    /// moment every commit it sends is refused, whichever partition it names.
    /// What it does not prove is which range ought to coordinate which group
    /// — the plane cannot see a Kafka topology, so it cannot judge that — and
    /// the compare-and-set on `expected_checkpoint_generation` is what keeps
    /// two writers from silently losing each other's update.
    CommitGroupCursorCoordinated {
        env: CommandEnvelope,
        group_uuid: Uuid,
        member_uuid: Uuid,
        topic_uuid: Uuid,
        /// The range the cursor is ON: the partition being committed.
        range_uuid: Uuid,
        /// The range the committer LEADS: what its lease fences.
        coordinator_topic_uuid: Uuid,
        coordinator_range_uuid: Uuid,
        topic_epoch: u64,
        range_generation: u64,
        segment_uuid: Uuid,
        segment_generation: u64,
        segment_root: [u8; 32],
        record_offset: u64,
        record_index: u64,
        lineage_transition_id: Option<Uuid>,
        expected_checkpoint_generation: Option<u64>,
        /// The node committing, which must hold `coordinator_range_uuid`...
        holder_node_uuid: Uuid,
        /// ...at this fencing epoch, the one the plane granted it.
        fencing_epoch: u64,
    },
    /// `EnsureGroupMemberForRange` from the group's coordinator (#457 slice
    /// 4), fenced the same way: the membership a coordinator needs before it
    /// can commit a cursor on a range it does not lead.
    EnsureGroupMemberCoordinated {
        env: CommandEnvelope,
        name: String,
        group_uuid: Uuid,
        member_uuid: Uuid,
        topic_uuid: Uuid,
        /// The range the member is assigned: the partition it consumes.
        range_uuid: Uuid,
        /// The range the coordinator leads: what its lease fences.
        coordinator_topic_uuid: Uuid,
        coordinator_range_uuid: Uuid,
        holder_node_uuid: Uuid,
        fencing_epoch: u64,
    },
    /// Remove a member whose last heartbeat apply-index is strictly older than
    /// `stale_before_apply_index`. Durable cursors are retained.
    ExpireStaleMember {
        env: CommandEnvelope,
        group_uuid: Uuid,
        member_uuid: Uuid,
        stale_before_apply_index: u64,
    },
    /// Set failure-domain and capacity weight used by deterministic placement.
    SetNodePlacementAttrs {
        env: CommandEnvelope,
        node_uuid: Uuid,
        failure_domain: String,
        placement_weight: u32,
        expected_generation: u64,
    },
    /// Commit an ordered replica set for a verified segment. The proposer
    /// supplies an explicit `replication_factor` and candidate list; `apply`
    /// rejects any set whose length differs from that factor or that differs
    /// from the deterministic rendezvous selection over currently Active nodes.
    CommitSegmentPlacement {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        /// Independent durability target; must equal `replica_nodes.len()`.
        replication_factor: u8,
        replica_nodes: Vec<Uuid>,
        expected_segment_generation: u64,
        /// `None` for the first placement; `Some(g)` CAS-updates an existing one.
        expected_placement_generation: Option<u64>,
    },
    /// Commit authenticated evidence that a replacement replica matches the
    /// sealed segment identity, length, epoch, and content root.
    CommitReplacementProof {
        env: CommandEnvelope,
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
        /// Proposer-supplied consensus term recorded for audit; not a clock.
        verified_term: u64,
    },
    /// Gate replica retirement on a committed matching ReplacementProof.
    PlanReplicaRetirement {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        retiring_node_uuid: Uuid,
        expected_segment_generation: u64,
        fencing_epoch: u64,
    },
    /// Confirm physical retirement effect after RETIRE_PLANNED. Consumes the
    /// replacement proof; a segment with surviving placed replicas returns to
    /// `Verified`, while an unplaced segment becomes `Retired`.
    ConfirmReplicaRetired {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        retiring_node_uuid: Uuid,
        expected_segment_generation: u64,
    },
    /// Open the single in-flight rebalance move for a verified segment:
    /// records a `RebalanceIntent` and adds `to_node_uuid` to the placement so
    /// the segment temporarily runs at declared RF + 1 replicas, never fewer.
    /// The move completes through the existing replacement-proof/retirement
    /// flow ([`MetadataCommand::CommitReplacementProof`] onward).
    ProposeRebalance {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        from_node_uuid: Uuid,
        to_node_uuid: Uuid,
        expected_placement_generation: u64,
    },
    /// Abandon an in-flight rebalance before its replacement proof commits:
    /// removes the intent and drops the destination replica it added.
    CancelRebalance {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        expected_placement_generation: u64,
    },
    /// Commit verified cold-tier copy evidence for a sealed segment. The
    /// upload and read-back verification happen out-of-band (vtopctl); only
    /// *verified* facts enter the state machine — there is no unverified
    /// tier-copy record, and the segment record itself is not mutated.
    CommitTierEvidence {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        expected_segment_generation: u64,
        content_root: [u8; 32],
        byte_length: u64,
        backend_id: String,
        /// Object URI of the tiered segment bytes; the manifest object is
        /// `object_uri + ".manifest.json"` by convention (no separate field,
        /// keeping the record inside the snapshot value bound).
        object_uri: String,
        /// Immutable version of the segment object itself.
        object_version_id: Option<String>,
        /// Immutable manifest version pin (#135); `None` only when the
        /// backend exposes no versions and the operator opted out.
        manifest_version_id: Option<String>,
        manifest_core_digest: [u8; 32],
        verification_method: VerificationMethod,
        verifier_node_uuid: Uuid,
        fencing_epoch: u64,
        /// Proposer-supplied consensus term recorded for audit; not a clock.
        verified_term: u64,
    },
    /// Create or CAS-update a topic's retention policy. The absent record is
    /// the fail-closed default: retention planning without tier evidence is
    /// rejected unless an explicit committed policy allows it.
    SetTopicRetentionPolicy {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        unarchived_deletion_allowed: bool,
        /// `None` expects the policy to be absent (creation); `Some(g)` is a
        /// CAS update of an existing record — the `RegisterNode` pattern.
        expected_generation: Option<u64>,
    },
    /// Authorize whole-segment deletion: `Verified -> RetentionPlanned`,
    /// gated on matching tier evidence (or an explicit policy override),
    /// mutual exclusion with rebalance/repair, and durable group cursors.
    PlanRetention {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        expected_segment_generation: u64,
        fencing_epoch: u64,
    },
    /// Confirm the physical deletion of every local replica after
    /// `RetentionPlanned`: `RetentionPlanned -> RetentionExpired`, emptying
    /// the placement replica set while preserving its declared factor. The
    /// segment and tier-copy records are retained forever as the rehydration
    /// pointer and corruption-audit anchor.
    ConfirmRetentionExpired {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        expected_segment_generation: u64,
    },
    /// Deprecated: always rejected (fails closed) since #184. `PlanRetention`
    /// is durable deletion authority and the state machine cannot prove no
    /// worker already removed replicas, so `RetentionPlanned` is terminal —
    /// recovery must complete retention or repair. Retained only for wire
    /// compatibility; do not send.
    CancelRetention {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        expected_segment_generation: u64,
    },
    /// Raise the cluster's metadata feature level (#554), enabling the
    /// command kinds up to it. Monotonic: a level at or below the current
    /// one is an idempotent Ack, never a downgrade. Refused above
    /// [`SUPPORTED_FEATURE_LEVEL`].
    ///
    /// The one command the feature gate does not hold back, because it is
    /// how the gate opens - and the one an operator must only send once
    /// EVERY metadata voter runs a release that knows it (this kind is
    /// itself new at level 4): an old voter cannot decode it, exactly as it
    /// could not decode the commands it unlocks.
    SetFeatureLevel {
        env: CommandEnvelope,
        feature_level: u32,
    },
    /// Record the replica set a range's quorum is counted over (#553).
    ///
    /// Each data node used to size its quorum from its own configuration, so
    /// growing a set by rolling nodes could leave a node counting a majority
    /// of a set the others never shared - two majorities with no node in
    /// common, and acknowledged records overwritten. With a recorded set, a
    /// candidate whose configured set differs refuses to promote. A change
    /// adds or removes exactly ONE member (consecutive sets' majorities then
    /// always intersect), clears the range's lease so it is re-won under the
    /// new set, and is refused until a promotion under the set it replaces
    /// shows the leader's whole log on a majority of that set (see
    /// `MetaStateMachine::established_under`). CAS'd on the record's
    /// generation (0: none yet).
    SetRangeReplicaSet {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        members: Vec<Uuid>,
        expected_generation: u64,
    },
    /// Record an operator's attestation that `replica_node_uuid`'s log is to
    /// become the range's canonical history although its lineage cannot be
    /// proven (#636): the last-resort override for a range whose replicas
    /// have no epoch history, after restore from backup. See
    /// [`PromotionAttestation`] for what it permits and what it costs.
    ///
    /// Cluster-scoped (an operator's, never a data node's). It replaces any
    /// live attestation of the range: the newest statement is the one that
    /// counts. The replica must be a registered node and, where the range
    /// records a replica set, a member of it.
    AttestUnprovenPromotion {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        replica_node_uuid: Uuid,
        operator: String,
    },
    /// A data node's own account of what it runs (#634): its release, the
    /// highest replica-plane protocol it speaks, and the highest metadata
    /// feature level it implements. Sent when the node starts, and repeated
    /// until metadata accepts it; replaces the node's previous report.
    ///
    /// What a raise of the feature level is judged against: a level above
    /// [`NODE_VERSION_REPORTS_FEATURE_LEVEL`] is refused while any counted
    /// node has not reported that level or higher - a live range member, or
    /// any node that has reported, unless marked dead (see
    /// [`crate::versions::NodeVersionEntry::counted`]). A report is a
    /// fact about one node, so it is node-scoped: only that node's identity
    /// may send it, and only a registered node's is accepted (a node that
    /// never reports can then still be marked dead, which is how an operator
    /// takes a node that will never return out of the count).
    ReportNodeVersion {
        env: CommandEnvelope,
        node_uuid: Uuid,
        /// 1..=[`MAX_RELEASE_BYTES`] bytes.
        release: String,
        max_replica_protocol: u16,
        supported_feature_level: u32,
    },
}

impl MetadataCommand {
    /// The feature level a cluster must be at before this command may enter
    /// the log (#554): the level of the release that introduced its kind.
    pub fn feature_level(&self) -> u32 {
        match self {
            // A lineage verdict on each quorum answer (#577), an attestation
            // and the lineage-unproven refusal (#636) are the same kind with
            // a payload an older voter cannot decode, so a report is gated
            // by what it carries; a report with none of them is still
            // v0.5.0's.
            MetadataCommand::ReportPromotionOutcome { outcome, .. } => outcome.feature_level(),
            // v0.6.0 (#457 slices 2b and 4a).
            MetadataCommand::CommitGroupCursorFenced { .. }
            | MetadataCommand::EnsureGroupMemberForRange { .. }
            | MetadataCommand::CommitGroupCursorCoordinated { .. }
            | MetadataCommand::EnsureGroupMemberCoordinated { .. } => 3,
            // After v0.6.0: the split (#473) and the gate itself.
            MetadataCommand::SplitRange { .. } | MetadataCommand::SetFeatureLevel { .. } => 4,
            // The recorded replica set (#553).
            MetadataCommand::SetRangeReplicaSet { .. } => 5,
            // A data node's version report (#634).
            MetadataCommand::ReportNodeVersion { .. } => NODE_VERSION_REPORTS_FEATURE_LEVEL,
            // The operator's override for a range with no provable lineage
            // (#636).
            MetadataCommand::AttestUnprovenPromotion { .. } => PROMOTE_UNPROVEN_FEATURE_LEVEL,
            // Everything v0.4.0 already knew.
            _ => 1,
        }
    }

    pub fn envelope(&self) -> &CommandEnvelope {
        match self {
            MetadataCommand::RegisterNode { env, .. }
            | MetadataCommand::SetNodeState { env, .. }
            | MetadataCommand::CreateTopic { env, .. }
            | MetadataCommand::GrantRangeLease { env, .. }
            | MetadataCommand::AcquireRangeLease { env, .. }
            | MetadataCommand::RenewRangeLease { env, .. }
            | MetadataCommand::ReportPromotionOutcome { env, .. }
            | MetadataCommand::ReleaseRangeLease { env, .. }
            | MetadataCommand::SplitRange { env, .. }
            | MetadataCommand::RegisterSealedSegment { env, .. }
            | MetadataCommand::MarkSegmentVerified { env, .. }
            | MetadataCommand::PutKeyRecord { env, .. }
            | MetadataCommand::CreateConsumerGroup { env, .. }
            | MetadataCommand::JoinConsumerGroup { env, .. }
            | MetadataCommand::LeaveConsumerGroup { env, .. }
            | MetadataCommand::AssignMemberRanges { env, .. }
            | MetadataCommand::CommitGroupCursor { env, .. }
            | MetadataCommand::CommitGroupCursorFenced { env, .. }
            | MetadataCommand::EnsureGroupMemberForRange { env, .. }
            | MetadataCommand::CommitGroupCursorCoordinated { env, .. }
            | MetadataCommand::EnsureGroupMemberCoordinated { env, .. }
            | MetadataCommand::HeartbeatMember { env, .. }
            | MetadataCommand::ExpireStaleMember { env, .. }
            | MetadataCommand::SetNodePlacementAttrs { env, .. }
            | MetadataCommand::CommitSegmentPlacement { env, .. }
            | MetadataCommand::CommitReplacementProof { env, .. }
            | MetadataCommand::PlanReplicaRetirement { env, .. }
            | MetadataCommand::ConfirmReplicaRetired { env, .. }
            | MetadataCommand::ProposeRebalance { env, .. }
            | MetadataCommand::CancelRebalance { env, .. }
            | MetadataCommand::CommitTierEvidence { env, .. }
            | MetadataCommand::SetTopicRetentionPolicy { env, .. }
            | MetadataCommand::PlanRetention { env, .. }
            | MetadataCommand::ConfirmRetentionExpired { env, .. }
            | MetadataCommand::CancelRetention { env, .. }
            | MetadataCommand::SetFeatureLevel { env, .. }
            | MetadataCommand::SetRangeReplicaSet { env, .. }
            | MetadataCommand::ReportNodeVersion { env, .. }
            | MetadataCommand::AttestUnprovenPromotion { env, .. } => env,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::with_capacity(96);
        match self {
            MetadataCommand::RegisterNode {
                env,
                node_uuid,
                addr,
                expected_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_REGISTER_NODE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *node_uuid);
                put_bounded_str(&mut out, addr, MAX_NODE_ADDR_BYTES, "node address")?;
                encode_optional_u64(&mut out, *expected_generation);
            }
            MetadataCommand::SetNodeState {
                env,
                node_uuid,
                state,
                expected_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_SET_NODE_STATE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *node_uuid);
                put_u8(&mut out, state.wire_tag());
                put_u64(&mut out, *expected_generation);
            }
            MetadataCommand::CreateTopic {
                env,
                name,
                topic_uuid,
                root_range_uuid,
            } => {
                put_u16(&mut out, COMMAND_KIND_CREATE_TOPIC);
                encode_envelope(&mut out, env);
                put_bounded_str(&mut out, name, MAX_TOPIC_NAME_BYTES, "topic name")?;
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *root_range_uuid);
            }
            MetadataCommand::GrantRangeLease {
                env,
                topic_uuid,
                range_uuid,
                holder_node_uuid,
                expected_range_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_GRANT_RANGE_LEASE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *holder_node_uuid);
                put_u64(&mut out, *expected_range_generation);
            }
            MetadataCommand::ReleaseRangeLease {
                env,
                topic_uuid,
                range_uuid,
                expected_fencing_epoch,
            } => {
                put_u16(&mut out, COMMAND_KIND_RELEASE_RANGE_LEASE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_u64(&mut out, *expected_fencing_epoch);
            }
            MetadataCommand::AcquireRangeLease {
                env,
                topic_uuid,
                range_uuid,
                holder_node_uuid,
                expected_range_generation,
                lease_duration_ms,
            } => {
                put_u16(&mut out, COMMAND_KIND_ACQUIRE_RANGE_LEASE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *holder_node_uuid);
                put_u64(&mut out, *expected_range_generation);
                put_u64(&mut out, *lease_duration_ms);
            }
            MetadataCommand::RenewRangeLease {
                env,
                topic_uuid,
                range_uuid,
                holder_node_uuid,
                expected_fencing_epoch,
                lease_duration_ms,
            } => {
                put_u16(&mut out, COMMAND_KIND_RENEW_RANGE_LEASE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *holder_node_uuid);
                put_u64(&mut out, *expected_fencing_epoch);
                put_u64(&mut out, *lease_duration_ms);
            }
            MetadataCommand::ReportPromotionOutcome {
                env,
                topic_uuid,
                range_uuid,
                holder_node_uuid,
                fencing_epoch,
                outcome,
            } => {
                put_u16(&mut out, COMMAND_KIND_REPORT_PROMOTION_OUTCOME);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *holder_node_uuid);
                put_u64(&mut out, *fencing_epoch);
                encode_promotion_outcome(&mut out, outcome)?;
            }
            MetadataCommand::SplitRange {
                env,
                topic_uuid,
                parent_range_uuid,
                left_range_uuid,
                right_range_uuid,
                transition_id,
                barrier_offset,
                expected_range_generation,
                holder_node_uuid,
                fencing_epoch,
            } => {
                put_u16(&mut out, COMMAND_KIND_SPLIT_RANGE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *parent_range_uuid);
                put_uuid(&mut out, *left_range_uuid);
                put_uuid(&mut out, *right_range_uuid);
                put_uuid(&mut out, *transition_id);
                put_u64(&mut out, *barrier_offset);
                put_u64(&mut out, *expected_range_generation);
                put_uuid(&mut out, *holder_node_uuid);
                put_u64(&mut out, *fencing_epoch);
            }
            MetadataCommand::SetFeatureLevel { env, feature_level } => {
                put_u16(&mut out, COMMAND_KIND_SET_FEATURE_LEVEL);
                encode_envelope(&mut out, env);
                put_u32(&mut out, *feature_level);
            }
            MetadataCommand::SetRangeReplicaSet {
                env,
                topic_uuid,
                range_uuid,
                members,
                expected_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_SET_RANGE_REPLICA_SET);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                encode_uuid_list(&mut out, members, MAX_REPLICAS, "replica set members")?;
                put_u64(&mut out, *expected_generation);
            }
            MetadataCommand::AttestUnprovenPromotion {
                env,
                topic_uuid,
                range_uuid,
                replica_node_uuid,
                operator,
            } => {
                put_u16(&mut out, COMMAND_KIND_ATTEST_UNPROVEN_PROMOTION);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *replica_node_uuid);
                put_bounded_str(
                    &mut out,
                    operator,
                    MAX_ATTESTATION_OPERATOR_BYTES,
                    "attestation operator",
                )?;
            }
            MetadataCommand::ReportNodeVersion {
                env,
                node_uuid,
                release,
                max_replica_protocol,
                supported_feature_level,
            } => {
                put_u16(&mut out, COMMAND_KIND_REPORT_NODE_VERSION);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *node_uuid);
                put_bounded_str(&mut out, release, MAX_RELEASE_BYTES, "release")?;
                put_u16(&mut out, *max_replica_protocol);
                put_u32(&mut out, *supported_feature_level);
            }
            MetadataCommand::RegisterSealedSegment {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                segment_generation,
                base_offset,
                next_offset,
                content_root,
                sealed_by_epoch,
                expected_range_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_REGISTER_SEALED_SEGMENT);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_u64(&mut out, *segment_generation);
                put_u64(&mut out, *base_offset);
                put_u64(&mut out, *next_offset);
                put_bytes32(&mut out, content_root);
                put_u64(&mut out, *sealed_by_epoch);
                put_u64(&mut out, *expected_range_generation);
            }
            MetadataCommand::MarkSegmentVerified {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                content_root,
                expected_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_MARK_SEGMENT_VERIFIED);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_bytes32(&mut out, content_root);
                put_u64(&mut out, *expected_generation);
            }
            MetadataCommand::PutKeyRecord {
                env,
                key_uuid,
                scheme,
                public_material_digest,
            } => {
                put_u16(&mut out, COMMAND_KIND_PUT_KEY_RECORD);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *key_uuid);
                put_u16(&mut out, *scheme);
                put_bytes32(&mut out, public_material_digest);
            }
            MetadataCommand::CreateConsumerGroup {
                env,
                name,
                group_uuid,
            } => {
                put_u16(&mut out, COMMAND_KIND_CREATE_CONSUMER_GROUP);
                encode_envelope(&mut out, env);
                put_bounded_str(&mut out, name, MAX_GROUP_NAME_BYTES, "group name")?;
                put_uuid(&mut out, *group_uuid);
            }
            MetadataCommand::JoinConsumerGroup {
                env,
                group_uuid,
                member_uuid,
                expected_group_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_JOIN_CONSUMER_GROUP);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *group_uuid);
                put_uuid(&mut out, *member_uuid);
                put_u64(&mut out, *expected_group_generation);
            }
            MetadataCommand::LeaveConsumerGroup {
                env,
                group_uuid,
                member_uuid,
                expected_member_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_LEAVE_CONSUMER_GROUP);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *group_uuid);
                put_uuid(&mut out, *member_uuid);
                put_u64(&mut out, *expected_member_generation);
            }
            MetadataCommand::AssignMemberRanges {
                env,
                group_uuid,
                member_uuid,
                ranges,
                expected_member_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_ASSIGN_MEMBER_RANGES);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *group_uuid);
                put_uuid(&mut out, *member_uuid);
                encode_range_assignments(&mut out, ranges)?;
                put_u64(&mut out, *expected_member_generation);
            }
            MetadataCommand::CommitGroupCursor {
                env,
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
            } => {
                put_u16(&mut out, COMMAND_KIND_COMMIT_GROUP_CURSOR);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *group_uuid);
                put_uuid(&mut out, *member_uuid);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_u64(&mut out, *topic_epoch);
                put_u64(&mut out, *range_generation);
                put_uuid(&mut out, *segment_uuid);
                put_u64(&mut out, *segment_generation);
                put_bytes32(&mut out, segment_root);
                put_u64(&mut out, *record_offset);
                put_u64(&mut out, *record_index);
                encode_optional_uuid(&mut out, *lineage_transition_id);
                encode_optional_u64(&mut out, *expected_checkpoint_generation);
            }
            MetadataCommand::EnsureGroupMemberForRange {
                env,
                name,
                group_uuid,
                member_uuid,
                topic_uuid,
                range_uuid,
                holder_node_uuid,
                fencing_epoch,
            } => {
                put_u16(&mut out, COMMAND_KIND_ENSURE_GROUP_MEMBER_FOR_RANGE);
                encode_envelope(&mut out, env);
                put_bounded_str(&mut out, name, MAX_GROUP_NAME_BYTES, "group name")?;
                put_uuid(&mut out, *group_uuid);
                put_uuid(&mut out, *member_uuid);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *holder_node_uuid);
                put_u64(&mut out, *fencing_epoch);
            }
            MetadataCommand::CommitGroupCursorFenced {
                env,
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
                holder_node_uuid,
                fencing_epoch,
            } => {
                put_u16(&mut out, COMMAND_KIND_COMMIT_GROUP_CURSOR_FENCED);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *group_uuid);
                put_uuid(&mut out, *member_uuid);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_u64(&mut out, *topic_epoch);
                put_u64(&mut out, *range_generation);
                put_uuid(&mut out, *segment_uuid);
                put_u64(&mut out, *segment_generation);
                put_bytes32(&mut out, segment_root);
                put_u64(&mut out, *record_offset);
                put_u64(&mut out, *record_index);
                encode_optional_uuid(&mut out, *lineage_transition_id);
                encode_optional_u64(&mut out, *expected_checkpoint_generation);
                put_uuid(&mut out, *holder_node_uuid);
                put_u64(&mut out, *fencing_epoch);
            }
            MetadataCommand::CommitGroupCursorCoordinated {
                env,
                group_uuid,
                member_uuid,
                topic_uuid,
                range_uuid,
                coordinator_topic_uuid,
                coordinator_range_uuid,
                topic_epoch,
                range_generation,
                segment_uuid,
                segment_generation,
                segment_root,
                record_offset,
                record_index,
                lineage_transition_id,
                expected_checkpoint_generation,
                holder_node_uuid,
                fencing_epoch,
            } => {
                put_u16(&mut out, COMMAND_KIND_COMMIT_GROUP_CURSOR_COORDINATED);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *group_uuid);
                put_uuid(&mut out, *member_uuid);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *coordinator_topic_uuid);
                put_uuid(&mut out, *coordinator_range_uuid);
                put_u64(&mut out, *topic_epoch);
                put_u64(&mut out, *range_generation);
                put_uuid(&mut out, *segment_uuid);
                put_u64(&mut out, *segment_generation);
                put_bytes32(&mut out, segment_root);
                put_u64(&mut out, *record_offset);
                put_u64(&mut out, *record_index);
                encode_optional_uuid(&mut out, *lineage_transition_id);
                encode_optional_u64(&mut out, *expected_checkpoint_generation);
                put_uuid(&mut out, *holder_node_uuid);
                put_u64(&mut out, *fencing_epoch);
            }
            MetadataCommand::EnsureGroupMemberCoordinated {
                env,
                name,
                group_uuid,
                member_uuid,
                topic_uuid,
                range_uuid,
                coordinator_topic_uuid,
                coordinator_range_uuid,
                holder_node_uuid,
                fencing_epoch,
            } => {
                put_u16(&mut out, COMMAND_KIND_ENSURE_GROUP_MEMBER_COORDINATED);
                encode_envelope(&mut out, env);
                put_bounded_str(&mut out, name, MAX_GROUP_NAME_BYTES, "group name")?;
                put_uuid(&mut out, *group_uuid);
                put_uuid(&mut out, *member_uuid);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *coordinator_topic_uuid);
                put_uuid(&mut out, *coordinator_range_uuid);
                put_uuid(&mut out, *holder_node_uuid);
                put_u64(&mut out, *fencing_epoch);
            }
            MetadataCommand::HeartbeatMember {
                env,
                group_uuid,
                member_uuid,
            } => {
                put_u16(&mut out, COMMAND_KIND_HEARTBEAT_MEMBER);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *group_uuid);
                put_uuid(&mut out, *member_uuid);
            }
            MetadataCommand::ExpireStaleMember {
                env,
                group_uuid,
                member_uuid,
                stale_before_apply_index,
            } => {
                put_u16(&mut out, COMMAND_KIND_EXPIRE_STALE_MEMBER);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *group_uuid);
                put_uuid(&mut out, *member_uuid);
                put_u64(&mut out, *stale_before_apply_index);
            }
            MetadataCommand::SetNodePlacementAttrs {
                env,
                node_uuid,
                failure_domain,
                placement_weight,
                expected_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_SET_NODE_PLACEMENT_ATTRS);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *node_uuid);
                put_bounded_str(
                    &mut out,
                    failure_domain,
                    MAX_FAILURE_DOMAIN_BYTES,
                    "failure domain",
                )?;
                put_u32(&mut out, *placement_weight);
                put_u64(&mut out, *expected_generation);
            }
            MetadataCommand::CommitSegmentPlacement {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                replication_factor,
                replica_nodes,
                expected_segment_generation,
                expected_placement_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_COMMIT_SEGMENT_PLACEMENT);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_u8(&mut out, *replication_factor);
                encode_uuid_list(&mut out, replica_nodes, MAX_REPLICAS, "replica nodes")?;
                put_u64(&mut out, *expected_segment_generation);
                encode_optional_u64(&mut out, *expected_placement_generation);
            }
            MetadataCommand::CommitReplacementProof {
                env,
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
            } => {
                put_u16(&mut out, COMMAND_KIND_COMMIT_REPLACEMENT_PROOF);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_u64(&mut out, *expected_segment_generation);
                put_bytes32(&mut out, content_root);
                put_u64(&mut out, *expected_length_bytes);
                put_uuid(&mut out, *source_node_uuid);
                put_uuid(&mut out, *destination_node_uuid);
                put_u64(&mut out, *fencing_epoch);
                put_u8(&mut out, verification_method.wire_tag());
                put_uuid(&mut out, *verifier_node_uuid);
                put_u64(&mut out, *verified_term);
            }
            MetadataCommand::PlanReplicaRetirement {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                retiring_node_uuid,
                expected_segment_generation,
                fencing_epoch,
            } => {
                put_u16(&mut out, COMMAND_KIND_PLAN_REPLICA_RETIREMENT);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_uuid(&mut out, *retiring_node_uuid);
                put_u64(&mut out, *expected_segment_generation);
                put_u64(&mut out, *fencing_epoch);
            }
            MetadataCommand::ConfirmReplicaRetired {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                retiring_node_uuid,
                expected_segment_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_CONFIRM_REPLICA_RETIRED);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_uuid(&mut out, *retiring_node_uuid);
                put_u64(&mut out, *expected_segment_generation);
            }
            MetadataCommand::ProposeRebalance {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                from_node_uuid,
                to_node_uuid,
                expected_placement_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_PROPOSE_REBALANCE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_uuid(&mut out, *from_node_uuid);
                put_uuid(&mut out, *to_node_uuid);
                put_u64(&mut out, *expected_placement_generation);
            }
            MetadataCommand::CancelRebalance {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_placement_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_CANCEL_REBALANCE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_u64(&mut out, *expected_placement_generation);
            }
            MetadataCommand::CommitTierEvidence {
                env,
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
            } => {
                put_u16(&mut out, COMMAND_KIND_COMMIT_TIER_EVIDENCE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_u64(&mut out, *expected_segment_generation);
                put_bytes32(&mut out, content_root);
                put_u64(&mut out, *byte_length);
                put_bounded_str(
                    &mut out,
                    backend_id,
                    MAX_TIER_BACKEND_ID_BYTES,
                    "backend id",
                )?;
                put_bounded_str(
                    &mut out,
                    object_uri,
                    MAX_TIER_OBJECT_URI_BYTES,
                    "object uri",
                )?;
                match manifest_version_id {
                    None => put_u8(&mut out, 0),
                    Some(version_id) => {
                        put_u8(&mut out, 1);
                        put_bounded_str(
                            &mut out,
                            version_id,
                            MAX_TIER_VERSION_ID_BYTES,
                            "manifest version id",
                        )?;
                    }
                }
                put_bytes32(&mut out, manifest_core_digest);
                put_u8(&mut out, verification_method.wire_tag());
                put_uuid(&mut out, *verifier_node_uuid);
                put_u64(&mut out, *fencing_epoch);
                put_u64(&mut out, *verified_term);
                if let Some(version_id) = object_version_id {
                    put_u8(&mut out, 1);
                    put_bounded_str(
                        &mut out,
                        version_id,
                        MAX_TIER_VERSION_ID_BYTES,
                        "object version id",
                    )?;
                }
            }
            MetadataCommand::SetTopicRetentionPolicy {
                env,
                topic_uuid,
                unarchived_deletion_allowed,
                expected_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_SET_TOPIC_RETENTION_POLICY);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_u8(&mut out, u8::from(*unarchived_deletion_allowed));
                encode_optional_u64(&mut out, *expected_generation);
            }
            MetadataCommand::PlanRetention {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation,
                fencing_epoch,
            } => {
                put_u16(&mut out, COMMAND_KIND_PLAN_RETENTION);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_u64(&mut out, *expected_segment_generation);
                put_u64(&mut out, *fencing_epoch);
            }
            MetadataCommand::ConfirmRetentionExpired {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_CONFIRM_RETENTION_EXPIRED);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_u64(&mut out, *expected_segment_generation);
            }
            MetadataCommand::CancelRetention {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_CANCEL_RETENTION);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_u64(&mut out, *expected_segment_generation);
            }
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let command = Self::decode_from(&mut reader)?;
        reader.finish()?;
        Ok(command)
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let kind = reader.u16("command kind")?;
        match kind {
            COMMAND_KIND_REGISTER_NODE => {
                let env = decode_envelope(reader)?;
                Ok(MetadataCommand::RegisterNode {
                    env,
                    node_uuid: reader.uuid("node uuid")?,
                    addr: {
                        let addr = reader.bounded_str(MAX_NODE_ADDR_BYTES, "node address")?;
                        if addr.is_empty() {
                            return Err(CodecError::InvalidValue {
                                what: "node address",
                                reason: "must not be empty",
                            });
                        }
                        addr
                    },
                    expected_generation: decode_optional_u64(reader, "expected generation")?,
                })
            }
            COMMAND_KIND_SET_NODE_STATE => Ok(MetadataCommand::SetNodeState {
                env: decode_envelope(reader)?,
                node_uuid: reader.uuid("node uuid")?,
                state: NodeState::from_wire(reader.u8("node state")?)?,
                expected_generation: reader.u64("expected generation")?,
            }),
            COMMAND_KIND_CREATE_TOPIC => {
                let env = decode_envelope(reader)?;
                let name = reader.bounded_str(MAX_TOPIC_NAME_BYTES, "topic name")?;
                if name.is_empty() {
                    return Err(CodecError::InvalidValue {
                        what: "topic name",
                        reason: "must not be empty",
                    });
                }
                Ok(MetadataCommand::CreateTopic {
                    env,
                    name,
                    topic_uuid: reader.uuid("topic uuid")?,
                    root_range_uuid: reader.uuid("root range uuid")?,
                })
            }
            COMMAND_KIND_GRANT_RANGE_LEASE => Ok(MetadataCommand::GrantRangeLease {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                holder_node_uuid: reader.uuid("holder node uuid")?,
                expected_range_generation: reader.u64("expected range generation")?,
            }),
            COMMAND_KIND_ACQUIRE_RANGE_LEASE => Ok(MetadataCommand::AcquireRangeLease {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                holder_node_uuid: reader.uuid("holder node uuid")?,
                expected_range_generation: reader.u64("expected range generation")?,
                lease_duration_ms: reader.u64("lease duration ms")?,
            }),
            COMMAND_KIND_RENEW_RANGE_LEASE => Ok(MetadataCommand::RenewRangeLease {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                holder_node_uuid: reader.uuid("holder node uuid")?,
                expected_fencing_epoch: reader.u64("expected fencing epoch")?,
                lease_duration_ms: reader.u64("lease duration ms")?,
            }),
            COMMAND_KIND_REPORT_PROMOTION_OUTCOME => Ok(MetadataCommand::ReportPromotionOutcome {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                holder_node_uuid: reader.uuid("holder node uuid")?,
                fencing_epoch: reader.u64("fencing epoch")?,
                outcome: decode_promotion_outcome(reader)?,
            }),
            COMMAND_KIND_RELEASE_RANGE_LEASE => Ok(MetadataCommand::ReleaseRangeLease {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                expected_fencing_epoch: reader.u64("expected fencing epoch")?,
            }),
            COMMAND_KIND_SPLIT_RANGE => Ok(MetadataCommand::SplitRange {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                parent_range_uuid: reader.uuid("parent range uuid")?,
                left_range_uuid: reader.uuid("left range uuid")?,
                right_range_uuid: reader.uuid("right range uuid")?,
                transition_id: reader.uuid("transition id")?,
                barrier_offset: reader.u64("barrier offset")?,
                expected_range_generation: reader.u64("expected range generation")?,
                holder_node_uuid: reader.uuid("holder node uuid")?,
                fencing_epoch: reader.u64("fencing epoch")?,
            }),
            COMMAND_KIND_SET_FEATURE_LEVEL => Ok(MetadataCommand::SetFeatureLevel {
                env: decode_envelope(reader)?,
                feature_level: reader.u32("feature level")?,
            }),
            COMMAND_KIND_SET_RANGE_REPLICA_SET => Ok(MetadataCommand::SetRangeReplicaSet {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                members: decode_uuid_list(reader, MAX_REPLICAS, "replica set members")?,
                expected_generation: reader.u64("expected replica set generation")?,
            }),
            COMMAND_KIND_ATTEST_UNPROVEN_PROMOTION => {
                Ok(MetadataCommand::AttestUnprovenPromotion {
                    env: decode_envelope(reader)?,
                    topic_uuid: reader.uuid("topic uuid")?,
                    range_uuid: reader.uuid("range uuid")?,
                    replica_node_uuid: reader.uuid("attested replica")?,
                    operator: reader
                        .bounded_str(MAX_ATTESTATION_OPERATOR_BYTES, "attestation operator")?,
                })
            }
            COMMAND_KIND_REPORT_NODE_VERSION => Ok(MetadataCommand::ReportNodeVersion {
                env: decode_envelope(reader)?,
                node_uuid: reader.uuid("node uuid")?,
                release: {
                    let release = reader.bounded_str(MAX_RELEASE_BYTES, "release")?;
                    if release.is_empty() {
                        return Err(CodecError::InvalidValue {
                            what: "release",
                            reason: "must not be empty",
                        });
                    }
                    release
                },
                max_replica_protocol: reader.u16("max replica protocol")?,
                supported_feature_level: reader.u32("supported feature level")?,
            }),
            COMMAND_KIND_REGISTER_SEALED_SEGMENT => Ok(MetadataCommand::RegisterSealedSegment {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
                segment_generation: reader.u64("segment generation")?,
                base_offset: reader.u64("base offset")?,
                next_offset: reader.u64("next offset")?,
                content_root: reader.bytes32("content root")?,
                sealed_by_epoch: reader.u64("sealed-by epoch")?,
                expected_range_generation: reader.u64("expected range generation")?,
            }),
            COMMAND_KIND_MARK_SEGMENT_VERIFIED => Ok(MetadataCommand::MarkSegmentVerified {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
                content_root: reader.bytes32("content root")?,
                expected_generation: reader.u64("expected generation")?,
            }),
            COMMAND_KIND_PUT_KEY_RECORD => Ok(MetadataCommand::PutKeyRecord {
                env: decode_envelope(reader)?,
                key_uuid: reader.uuid("key uuid")?,
                scheme: reader.u16("key scheme")?,
                public_material_digest: reader.bytes32("public material digest")?,
            }),
            COMMAND_KIND_CREATE_CONSUMER_GROUP => {
                let env = decode_envelope(reader)?;
                let name = reader.bounded_str(MAX_GROUP_NAME_BYTES, "group name")?;
                if name.is_empty() {
                    return Err(CodecError::InvalidValue {
                        what: "group name",
                        reason: "must not be empty",
                    });
                }
                Ok(MetadataCommand::CreateConsumerGroup {
                    env,
                    name,
                    group_uuid: reader.uuid("group uuid")?,
                })
            }
            COMMAND_KIND_JOIN_CONSUMER_GROUP => Ok(MetadataCommand::JoinConsumerGroup {
                env: decode_envelope(reader)?,
                group_uuid: reader.uuid("group uuid")?,
                member_uuid: reader.uuid("member uuid")?,
                expected_group_generation: reader.u64("expected group generation")?,
            }),
            COMMAND_KIND_LEAVE_CONSUMER_GROUP => Ok(MetadataCommand::LeaveConsumerGroup {
                env: decode_envelope(reader)?,
                group_uuid: reader.uuid("group uuid")?,
                member_uuid: reader.uuid("member uuid")?,
                expected_member_generation: reader.u64("expected member generation")?,
            }),
            COMMAND_KIND_ASSIGN_MEMBER_RANGES => Ok(MetadataCommand::AssignMemberRanges {
                env: decode_envelope(reader)?,
                group_uuid: reader.uuid("group uuid")?,
                member_uuid: reader.uuid("member uuid")?,
                ranges: decode_range_assignments(reader)?,
                expected_member_generation: reader.u64("expected member generation")?,
            }),
            COMMAND_KIND_COMMIT_GROUP_CURSOR => Ok(MetadataCommand::CommitGroupCursor {
                env: decode_envelope(reader)?,
                group_uuid: reader.uuid("group uuid")?,
                member_uuid: reader.uuid("member uuid")?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                topic_epoch: reader.u64("topic epoch")?,
                range_generation: reader.u64("range generation")?,
                segment_uuid: reader.uuid("segment uuid")?,
                segment_generation: reader.u64("segment generation")?,
                segment_root: reader.bytes32("segment root")?,
                record_offset: reader.u64("record offset")?,
                record_index: reader.u64("record index")?,
                lineage_transition_id: decode_optional_uuid(reader, "lineage transition id")?,
                expected_checkpoint_generation: decode_optional_u64(
                    reader,
                    "expected checkpoint generation",
                )?,
            }),
            COMMAND_KIND_ENSURE_GROUP_MEMBER_FOR_RANGE => {
                let env = decode_envelope(reader)?;
                let name = reader.bounded_str(MAX_GROUP_NAME_BYTES, "group name")?;
                if name.is_empty() {
                    return Err(CodecError::InvalidValue {
                        what: "group name",
                        reason: "must not be empty",
                    });
                }
                Ok(MetadataCommand::EnsureGroupMemberForRange {
                    env,
                    name: name.to_owned(),
                    group_uuid: reader.uuid("group uuid")?,
                    member_uuid: reader.uuid("member uuid")?,
                    topic_uuid: reader.uuid("topic uuid")?,
                    range_uuid: reader.uuid("range uuid")?,
                    holder_node_uuid: reader.uuid("holder node uuid")?,
                    fencing_epoch: reader.u64("fencing epoch")?,
                })
            }
            COMMAND_KIND_COMMIT_GROUP_CURSOR_FENCED => {
                Ok(MetadataCommand::CommitGroupCursorFenced {
                    env: decode_envelope(reader)?,
                    group_uuid: reader.uuid("group uuid")?,
                    member_uuid: reader.uuid("member uuid")?,
                    topic_uuid: reader.uuid("topic uuid")?,
                    range_uuid: reader.uuid("range uuid")?,
                    topic_epoch: reader.u64("topic epoch")?,
                    range_generation: reader.u64("range generation")?,
                    segment_uuid: reader.uuid("segment uuid")?,
                    segment_generation: reader.u64("segment generation")?,
                    segment_root: reader.bytes32("segment root")?,
                    record_offset: reader.u64("record offset")?,
                    record_index: reader.u64("record index")?,
                    lineage_transition_id: decode_optional_uuid(reader, "lineage transition id")?,
                    expected_checkpoint_generation: decode_optional_u64(
                        reader,
                        "expected checkpoint generation",
                    )?,
                    holder_node_uuid: reader.uuid("holder node uuid")?,
                    fencing_epoch: reader.u64("fencing epoch")?,
                })
            }
            COMMAND_KIND_COMMIT_GROUP_CURSOR_COORDINATED => {
                Ok(MetadataCommand::CommitGroupCursorCoordinated {
                    env: decode_envelope(reader)?,
                    group_uuid: reader.uuid("group uuid")?,
                    member_uuid: reader.uuid("member uuid")?,
                    topic_uuid: reader.uuid("topic uuid")?,
                    range_uuid: reader.uuid("range uuid")?,
                    coordinator_topic_uuid: reader.uuid("coordinator topic uuid")?,
                    coordinator_range_uuid: reader.uuid("coordinator range uuid")?,
                    topic_epoch: reader.u64("topic epoch")?,
                    range_generation: reader.u64("range generation")?,
                    segment_uuid: reader.uuid("segment uuid")?,
                    segment_generation: reader.u64("segment generation")?,
                    segment_root: reader.bytes32("segment root")?,
                    record_offset: reader.u64("record offset")?,
                    record_index: reader.u64("record index")?,
                    lineage_transition_id: decode_optional_uuid(reader, "lineage transition id")?,
                    expected_checkpoint_generation: decode_optional_u64(
                        reader,
                        "expected checkpoint generation",
                    )?,
                    holder_node_uuid: reader.uuid("holder node uuid")?,
                    fencing_epoch: reader.u64("fencing epoch")?,
                })
            }
            COMMAND_KIND_ENSURE_GROUP_MEMBER_COORDINATED => {
                let env = decode_envelope(reader)?;
                let name = reader.bounded_str(MAX_GROUP_NAME_BYTES, "group name")?;
                if name.is_empty() {
                    return Err(CodecError::InvalidValue {
                        what: "group name",
                        reason: "must not be empty",
                    });
                }
                Ok(MetadataCommand::EnsureGroupMemberCoordinated {
                    env,
                    name: name.to_owned(),
                    group_uuid: reader.uuid("group uuid")?,
                    member_uuid: reader.uuid("member uuid")?,
                    topic_uuid: reader.uuid("topic uuid")?,
                    range_uuid: reader.uuid("range uuid")?,
                    coordinator_topic_uuid: reader.uuid("coordinator topic uuid")?,
                    coordinator_range_uuid: reader.uuid("coordinator range uuid")?,
                    holder_node_uuid: reader.uuid("holder node uuid")?,
                    fencing_epoch: reader.u64("fencing epoch")?,
                })
            }
            COMMAND_KIND_HEARTBEAT_MEMBER => Ok(MetadataCommand::HeartbeatMember {
                env: decode_envelope(reader)?,
                group_uuid: reader.uuid("group uuid")?,
                member_uuid: reader.uuid("member uuid")?,
            }),
            COMMAND_KIND_EXPIRE_STALE_MEMBER => Ok(MetadataCommand::ExpireStaleMember {
                env: decode_envelope(reader)?,
                group_uuid: reader.uuid("group uuid")?,
                member_uuid: reader.uuid("member uuid")?,
                stale_before_apply_index: reader.u64("stale-before apply index")?,
            }),
            COMMAND_KIND_SET_NODE_PLACEMENT_ATTRS => {
                let env = decode_envelope(reader)?;
                let node_uuid = reader.uuid("node uuid")?;
                let failure_domain =
                    reader.bounded_str(MAX_FAILURE_DOMAIN_BYTES, "failure domain")?;
                let placement_weight = reader.u32("placement weight")?;
                if placement_weight < MIN_PLACEMENT_WEIGHT {
                    return Err(CodecError::InvalidValue {
                        what: "placement weight",
                        reason: "must be >= 1",
                    });
                }
                Ok(MetadataCommand::SetNodePlacementAttrs {
                    env,
                    node_uuid,
                    failure_domain,
                    placement_weight,
                    expected_generation: reader.u64("expected generation")?,
                })
            }
            COMMAND_KIND_COMMIT_SEGMENT_PLACEMENT => {
                let env = decode_envelope(reader)?;
                let topic_uuid = reader.uuid("topic uuid")?;
                let range_uuid = reader.uuid("range uuid")?;
                let segment_uuid = reader.uuid("segment uuid")?;
                let replication_factor = reader.u8("replication factor")?;
                if replication_factor == 0 || usize::from(replication_factor) > MAX_REPLICAS {
                    return Err(CodecError::InvalidValue {
                        what: "replication factor",
                        reason: "must be 1..=MAX_REPLICAS",
                    });
                }
                let replica_nodes = decode_uuid_list(reader, MAX_REPLICAS, "replica nodes")?;
                if replica_nodes.len() != usize::from(replication_factor) {
                    return Err(CodecError::InvalidValue {
                        what: "replica nodes",
                        reason: "length must equal replication_factor",
                    });
                }
                Ok(MetadataCommand::CommitSegmentPlacement {
                    env,
                    topic_uuid,
                    range_uuid,
                    segment_uuid,
                    replication_factor,
                    replica_nodes,
                    expected_segment_generation: reader.u64("expected segment generation")?,
                    expected_placement_generation: decode_optional_u64(
                        reader,
                        "expected placement generation",
                    )?,
                })
            }
            COMMAND_KIND_COMMIT_REPLACEMENT_PROOF => {
                let env = decode_envelope(reader)?;
                let topic_uuid = reader.uuid("topic uuid")?;
                let range_uuid = reader.uuid("range uuid")?;
                let segment_uuid = reader.uuid("segment uuid")?;
                let expected_segment_generation = reader.u64("expected segment generation")?;
                let content_root = reader.bytes32("content root")?;
                let expected_length_bytes = reader.u64("expected length bytes")?;
                let source_node_uuid = reader.uuid("source node uuid")?;
                let destination_node_uuid = reader.uuid("destination node uuid")?;
                let fencing_epoch = reader.u64("fencing epoch")?;
                let verification_method =
                    VerificationMethod::from_wire(reader.u8("verification method")?)?;
                let verifier_node_uuid = reader.uuid("verifier node uuid")?;
                let verified_term = reader.u64("verified term")?;
                Ok(MetadataCommand::CommitReplacementProof {
                    env,
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
                })
            }
            COMMAND_KIND_PLAN_REPLICA_RETIREMENT => Ok(MetadataCommand::PlanReplicaRetirement {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
                retiring_node_uuid: reader.uuid("retiring node uuid")?,
                expected_segment_generation: reader.u64("expected segment generation")?,
                fencing_epoch: reader.u64("fencing epoch")?,
            }),
            COMMAND_KIND_CONFIRM_REPLICA_RETIRED => Ok(MetadataCommand::ConfirmReplicaRetired {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
                retiring_node_uuid: reader.uuid("retiring node uuid")?,
                expected_segment_generation: reader.u64("expected segment generation")?,
            }),
            COMMAND_KIND_PROPOSE_REBALANCE => Ok(MetadataCommand::ProposeRebalance {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
                from_node_uuid: reader.uuid("rebalance source node uuid")?,
                to_node_uuid: reader.uuid("rebalance destination node uuid")?,
                expected_placement_generation: reader.u64("expected placement generation")?,
            }),
            COMMAND_KIND_CANCEL_REBALANCE => Ok(MetadataCommand::CancelRebalance {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
                expected_placement_generation: reader.u64("expected placement generation")?,
            }),
            COMMAND_KIND_COMMIT_TIER_EVIDENCE => {
                let env = decode_envelope(reader)?;
                let topic_uuid = reader.uuid("topic uuid")?;
                let range_uuid = reader.uuid("range uuid")?;
                let segment_uuid = reader.uuid("segment uuid")?;
                let expected_segment_generation = reader.u64("expected segment generation")?;
                let content_root = reader.bytes32("content root")?;
                let byte_length = reader.u64("byte length")?;
                if byte_length == 0 {
                    return Err(CodecError::InvalidValue {
                        what: "byte length",
                        reason: "must be > 0",
                    });
                }
                let backend_id = reader.bounded_str(MAX_TIER_BACKEND_ID_BYTES, "backend id")?;
                if backend_id.is_empty() {
                    return Err(CodecError::InvalidValue {
                        what: "backend id",
                        reason: "must not be empty",
                    });
                }
                let object_uri = reader.bounded_str(MAX_TIER_OBJECT_URI_BYTES, "object uri")?;
                if object_uri.is_empty() {
                    return Err(CodecError::InvalidValue {
                        what: "object uri",
                        reason: "must not be empty",
                    });
                }
                let manifest_version_id = if reader.flag("manifest version presence")? {
                    Some(reader.bounded_str(MAX_TIER_VERSION_ID_BYTES, "manifest version id")?)
                } else {
                    None
                };
                let manifest_core_digest = reader.bytes32("manifest core digest")?;
                let verification_method =
                    VerificationMethod::from_wire(reader.u8("verification method")?)?;
                let verifier_node_uuid = reader.uuid("verifier node uuid")?;
                let fencing_epoch = reader.u64("fencing epoch")?;
                let verified_term = reader.u64("verified term")?;
                let object_version_id = if reader.remaining() == 0 {
                    None
                } else if reader.flag("object version presence")? {
                    Some(reader.bounded_str(MAX_TIER_VERSION_ID_BYTES, "object version id")?)
                } else {
                    return Err(CodecError::InvalidValue {
                        what: "object version presence",
                        reason: "legacy None must omit the extension",
                    });
                };
                Ok(MetadataCommand::CommitTierEvidence {
                    env,
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
                })
            }
            COMMAND_KIND_SET_TOPIC_RETENTION_POLICY => {
                Ok(MetadataCommand::SetTopicRetentionPolicy {
                    env: decode_envelope(reader)?,
                    topic_uuid: reader.uuid("topic uuid")?,
                    unarchived_deletion_allowed: reader.flag("unarchived deletion allowed")?,
                    expected_generation: decode_optional_u64(reader, "expected generation")?,
                })
            }
            COMMAND_KIND_PLAN_RETENTION => Ok(MetadataCommand::PlanRetention {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
                expected_segment_generation: reader.u64("expected segment generation")?,
                fencing_epoch: reader.u64("fencing epoch")?,
            }),
            COMMAND_KIND_CONFIRM_RETENTION_EXPIRED => {
                Ok(MetadataCommand::ConfirmRetentionExpired {
                    env: decode_envelope(reader)?,
                    topic_uuid: reader.uuid("topic uuid")?,
                    range_uuid: reader.uuid("range uuid")?,
                    segment_uuid: reader.uuid("segment uuid")?,
                    expected_segment_generation: reader.u64("expected segment generation")?,
                })
            }
            COMMAND_KIND_CANCEL_RETENTION => Ok(MetadataCommand::CancelRetention {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
                expected_segment_generation: reader.u64("expected segment generation")?,
            }),
            other => Err(CodecError::UnknownTag {
                what: "command kind",
                tag: u32::from(other),
            }),
        }
    }
}

/// Deterministic rejection values. These are semantic outcomes of `apply`,
/// never I/O errors, so replaying the same log always reproduces them.
/// Convention: `expected` is what the proposer claimed, `actual` is the
/// authoritative value in the state machine.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum MetadataError {
    #[error("generation mismatch: proposer expected {expected}, state holds {actual}")]
    GenerationMismatch { expected: u64, actual: u64 },
    #[error("epoch mismatch: proposer expected {expected}, state holds {actual}")]
    EpochMismatch { expected: u64, actual: u64 },
    #[error("lineage generation mismatch: proposer expected {expected}, state holds {actual}")]
    LineageMismatch { expected: u64, actual: u64 },
    #[error("record already exists")]
    AlreadyExists,
    #[error("record not found")]
    NotFound,
    #[error("invalid transition: {0}")]
    InvalidTransition(String),
    #[error("limit violated: {0}")]
    Limit(String),
}

impl MetadataError {
    /// Build an `InvalidTransition` whose detail is truncated to the wire
    /// bound at a character boundary, so the error always encodes.
    pub fn invalid_transition(detail: impl Into<String>) -> Self {
        MetadataError::InvalidTransition(bound_detail(detail.into()))
    }

    /// Build a `Limit` with the same bounded-detail guarantee.
    pub fn limit(detail: impl Into<String>) -> Self {
        MetadataError::Limit(bound_detail(detail.into()))
    }

    fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        match self {
            MetadataError::GenerationMismatch { expected, actual } => {
                put_u16(out, ERROR_KIND_GENERATION_MISMATCH);
                put_u64(out, *expected);
                put_u64(out, *actual);
            }
            MetadataError::EpochMismatch { expected, actual } => {
                put_u16(out, ERROR_KIND_EPOCH_MISMATCH);
                put_u64(out, *expected);
                put_u64(out, *actual);
            }
            MetadataError::LineageMismatch { expected, actual } => {
                put_u16(out, ERROR_KIND_LINEAGE_MISMATCH);
                put_u64(out, *expected);
                put_u64(out, *actual);
            }
            MetadataError::AlreadyExists => put_u16(out, ERROR_KIND_ALREADY_EXISTS),
            MetadataError::NotFound => put_u16(out, ERROR_KIND_NOT_FOUND),
            MetadataError::InvalidTransition(detail) => {
                put_u16(out, ERROR_KIND_INVALID_TRANSITION);
                put_bounded_str(out, detail, MAX_ERROR_DETAIL_BYTES, "error detail")?;
            }
            MetadataError::Limit(detail) => {
                put_u16(out, ERROR_KIND_LIMIT);
                put_bounded_str(out, detail, MAX_ERROR_DETAIL_BYTES, "error detail")?;
            }
        }
        Ok(())
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let kind = reader.u16("error kind")?;
        match kind {
            ERROR_KIND_GENERATION_MISMATCH => Ok(MetadataError::GenerationMismatch {
                expected: reader.u64("expected generation")?,
                actual: reader.u64("actual generation")?,
            }),
            ERROR_KIND_EPOCH_MISMATCH => Ok(MetadataError::EpochMismatch {
                expected: reader.u64("expected epoch")?,
                actual: reader.u64("actual epoch")?,
            }),
            ERROR_KIND_LINEAGE_MISMATCH => Ok(MetadataError::LineageMismatch {
                expected: reader.u64("expected lineage generation")?,
                actual: reader.u64("actual lineage generation")?,
            }),
            ERROR_KIND_ALREADY_EXISTS => Ok(MetadataError::AlreadyExists),
            ERROR_KIND_NOT_FOUND => Ok(MetadataError::NotFound),
            ERROR_KIND_INVALID_TRANSITION => Ok(MetadataError::InvalidTransition(
                reader.bounded_str(MAX_ERROR_DETAIL_BYTES, "error detail")?,
            )),
            ERROR_KIND_LIMIT => Ok(MetadataError::Limit(
                reader.bounded_str(MAX_ERROR_DETAIL_BYTES, "error detail")?,
            )),
            other => Err(CodecError::UnknownTag {
                what: "error kind",
                tag: u32::from(other),
            }),
        }
    }
}

fn bound_detail(mut detail: String) -> String {
    if detail.len() > MAX_ERROR_DETAIL_BYTES {
        let mut cut = MAX_ERROR_DETAIL_BYTES;
        while !detail.is_char_boundary(cut) {
            cut -= 1;
        }
        detail.truncate(cut);
    }
    detail
}

/// What `apply` returns and what the dedup table stores.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataResponse {
    Ack {
        generation: u64,
    },
    TopicCreated {
        topic_uuid: Uuid,
        topic_epoch: u64,
        root_range_uuid: Uuid,
    },
    LeaseGranted {
        fencing_epoch: u64,
    },
    GroupCreated {
        group_uuid: Uuid,
        generation: u64,
    },
    MemberJoined {
        member_generation: u64,
        group_generation: u64,
    },
    CursorCommitted {
        checkpoint_generation: u64,
    },
    /// A promotion outcome was recorded against the transition that minted
    /// this epoch (#240 item 5).
    TransitionRecorded {
        fencing_epoch: u64,
    },
    Rejected(MetadataError),
}

impl MetadataResponse {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::with_capacity(48);
        self.encode_into(&mut out)?;
        Ok(out)
    }

    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        match self {
            MetadataResponse::Ack { generation } => {
                put_u16(out, RESPONSE_KIND_ACK);
                put_u64(out, *generation);
            }
            MetadataResponse::TopicCreated {
                topic_uuid,
                topic_epoch,
                root_range_uuid,
            } => {
                put_u16(out, RESPONSE_KIND_TOPIC_CREATED);
                put_uuid(out, *topic_uuid);
                put_u64(out, *topic_epoch);
                put_uuid(out, *root_range_uuid);
            }
            MetadataResponse::LeaseGranted { fencing_epoch } => {
                put_u16(out, RESPONSE_KIND_LEASE_GRANTED);
                put_u64(out, *fencing_epoch);
            }
            MetadataResponse::TransitionRecorded { fencing_epoch } => {
                put_u16(out, RESPONSE_KIND_TRANSITION_RECORDED);
                put_u64(out, *fencing_epoch);
            }
            MetadataResponse::GroupCreated {
                group_uuid,
                generation,
            } => {
                put_u16(out, RESPONSE_KIND_GROUP_CREATED);
                put_uuid(out, *group_uuid);
                put_u64(out, *generation);
            }
            MetadataResponse::MemberJoined {
                member_generation,
                group_generation,
            } => {
                put_u16(out, RESPONSE_KIND_MEMBER_JOINED);
                put_u64(out, *member_generation);
                put_u64(out, *group_generation);
            }
            MetadataResponse::CursorCommitted {
                checkpoint_generation,
            } => {
                put_u16(out, RESPONSE_KIND_CURSOR_COMMITTED);
                put_u64(out, *checkpoint_generation);
            }
            MetadataResponse::Rejected(error) => {
                put_u16(out, RESPONSE_KIND_REJECTED);
                error.encode_into(out)?;
            }
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let response = Self::decode_from(&mut reader)?;
        reader.finish()?;
        Ok(response)
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let kind = reader.u16("response kind")?;
        match kind {
            RESPONSE_KIND_ACK => Ok(MetadataResponse::Ack {
                generation: reader.u64("generation")?,
            }),
            RESPONSE_KIND_TOPIC_CREATED => Ok(MetadataResponse::TopicCreated {
                topic_uuid: reader.uuid("topic uuid")?,
                topic_epoch: reader.u64("topic epoch")?,
                root_range_uuid: reader.uuid("root range uuid")?,
            }),
            RESPONSE_KIND_LEASE_GRANTED => Ok(MetadataResponse::LeaseGranted {
                fencing_epoch: reader.u64("fencing epoch")?,
            }),
            RESPONSE_KIND_TRANSITION_RECORDED => Ok(MetadataResponse::TransitionRecorded {
                fencing_epoch: reader.u64("fencing epoch")?,
            }),
            RESPONSE_KIND_GROUP_CREATED => Ok(MetadataResponse::GroupCreated {
                group_uuid: reader.uuid("group uuid")?,
                generation: reader.u64("group generation")?,
            }),
            RESPONSE_KIND_MEMBER_JOINED => Ok(MetadataResponse::MemberJoined {
                member_generation: reader.u64("member generation")?,
                group_generation: reader.u64("group generation")?,
            }),
            RESPONSE_KIND_CURSOR_COMMITTED => Ok(MetadataResponse::CursorCommitted {
                checkpoint_generation: reader.u64("checkpoint generation")?,
            }),
            RESPONSE_KIND_REJECTED => Ok(MetadataResponse::Rejected(MetadataError::decode_from(
                reader,
            )?)),
            other => Err(CodecError::UnknownTag {
                what: "response kind",
                tag: u32::from(other),
            }),
        }
    }
}

fn encode_envelope(out: &mut Vec<u8>, env: &CommandEnvelope) {
    put_uuid(out, env.request_id);
    put_i64(out, env.issued_at_ms);
}

fn decode_envelope(reader: &mut Reader<'_>) -> Result<CommandEnvelope, CodecError> {
    Ok(CommandEnvelope {
        request_id: reader.uuid("request id")?,
        issued_at_ms: reader.i64("issued-at millis")?,
    })
}

/// Canonical option encoding: presence byte, then the value only if present.
fn encode_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_u64(out, value);
        }
    }
}

fn decode_optional_u64(
    reader: &mut Reader<'_>,
    what: &'static str,
) -> Result<Option<u64>, CodecError> {
    if reader.flag(what)? {
        Ok(Some(reader.u64(what)?))
    } else {
        Ok(None)
    }
}

fn encode_optional_uuid(out: &mut Vec<u8>, value: Option<Uuid>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_uuid(out, value);
        }
    }
}

fn decode_optional_uuid(
    reader: &mut Reader<'_>,
    what: &'static str,
) -> Result<Option<Uuid>, CodecError> {
    if reader.flag(what)? {
        Ok(Some(reader.uuid(what)?))
    } else {
        Ok(None)
    }
}

fn encode_range_assignments(
    out: &mut Vec<u8>,
    ranges: &[RangeAssignment],
) -> Result<(), CodecError> {
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

fn decode_range_assignments(reader: &mut Reader<'_>) -> Result<Vec<RangeAssignment>, CodecError> {
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

fn encode_uuid_list(
    out: &mut Vec<u8>,
    values: &[Uuid],
    maximum: usize,
    what: &'static str,
) -> Result<(), CodecError> {
    if values.len() > maximum {
        return Err(CodecError::BoundExceeded {
            what,
            actual: values.len(),
            maximum,
        });
    }
    put_u16(out, values.len() as u16);
    for value in values {
        put_uuid(out, *value);
    }
    Ok(())
}

fn decode_uuid_list(
    reader: &mut Reader<'_>,
    maximum: usize,
    what: &'static str,
) -> Result<Vec<Uuid>, CodecError> {
    let count = reader.u16(what)? as usize;
    if count > maximum {
        return Err(CodecError::BoundExceeded {
            what,
            actual: count,
            maximum,
        });
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(reader.uuid(what)?);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(request: u128) -> CommandEnvelope {
        CommandEnvelope {
            request_id: Uuid::from_u128(request),
            issued_at_ms: 1_750_000_000_000,
        }
    }

    /// #555: only the lease-election pair has a clock to check, and the bound
    /// is a quarter of the requested lease, never less than the floor.
    #[test]
    fn only_the_lease_election_pair_has_a_clock_skew_to_check() {
        const LEADER_NOW: i64 = 1_750_000_000_000;
        let mut checked = 0;
        for command in every_command() {
            let skew = command.lease_clock_skew(LEADER_NOW);
            match &command {
                MetadataCommand::AcquireRangeLease {
                    env,
                    lease_duration_ms,
                    ..
                }
                | MetadataCommand::RenewRangeLease {
                    env,
                    lease_duration_ms,
                    ..
                } => {
                    checked += 1;
                    let skew = skew.expect("the lease pair is checked");
                    assert_eq!(skew.skew_ms, env.issued_at_ms - LEADER_NOW);
                    assert_eq!(skew.bound_ms, lease_skew_bound_ms(*lease_duration_ms));
                }
                _ => assert_eq!(skew, None, "{command:?} never reads issued_at_ms"),
            }
        }
        assert_eq!(
            checked, 2,
            "both lease-election commands are in the fixture"
        );
    }

    #[test]
    fn the_skew_bound_is_a_twentieth_of_the_lease_with_a_floor_and_a_cap_and_is_symmetric() {
        // The two-sided loss stays inside a fifth of every lease, so pacing
        // can be validated against it (review of #566).
        for lease in [500, 1_000, 6_000, 10_000, 15_000, 60_000, 600_000] {
            assert!(2 * lease_skew_bound_ms(lease) <= lease / 5, "{lease}");
        }
        let acquire =
            |issued_at_ms: i64, lease_duration_ms: u64| MetadataCommand::AcquireRangeLease {
                env: CommandEnvelope {
                    request_id: Uuid::from_u128(1),
                    issued_at_ms,
                },
                topic_uuid: Uuid::from_u128(2),
                range_uuid: Uuid::from_u128(3),
                holder_node_uuid: Uuid::from_u128(4),
                expected_range_generation: 0,
                lease_duration_ms,
            };
        let within = |issued_at_ms: i64, lease_ms: u64| {
            acquire(issued_at_ms, lease_ms)
                .lease_clock_skew(1_000_000)
                .unwrap()
                .is_within_bound()
        };
        // 15 s lease: the 1 s floor either way (a twentieth is 750 ms).
        assert!(within(1_000_000, 15_000));
        assert!(within(1_001_000, 15_000));
        assert!(within(999_000, 15_000));
        assert!(!within(1_001_001, 15_000), "just ahead of the bound");
        assert!(!within(998_999, 15_000), "just behind it");
        // 60 s lease: a twentieth, 3 s.
        assert!(within(1_003_000, 60_000));
        assert!(!within(1_003_001, 60_000));
        // Under ten seconds the cap, a tenth of the lease, is below the floor
        // and wins: two proposers' errors can eat at most a fifth of it. A
        // rival 900 ms fast against a 500 ms lease is refused (review of
        // #566).
        assert!(within(1_000_600, 6_000));
        assert!(!within(1_000_601, 6_000));
        assert!(
            within(1_001_000, 10_000),
            "the floor and the cap meet at 10 s"
        );
        assert!(!within(1_001_001, 10_000));
        assert!(within(1_000_050, 500));
        assert!(!within(1_000_051, 500));
        assert!(!within(1_000_900, 500));
        // Extremes cannot overflow.
        assert!(!within(i64::MAX, 15_000));
        assert!(!within(i64::MIN, 15_000));
    }

    fn every_command() -> Vec<MetadataCommand> {
        vec![
            MetadataCommand::RegisterNode {
                env: envelope(1),
                node_uuid: Uuid::from_u128(10),
                addr: "10.0.0.1:9200".to_owned(),
                expected_generation: None,
            },
            MetadataCommand::RegisterNode {
                env: envelope(2),
                node_uuid: Uuid::from_u128(10),
                addr: "10.0.0.2:9200".to_owned(),
                expected_generation: Some(4),
            },
            MetadataCommand::SetNodeState {
                env: envelope(3),
                node_uuid: Uuid::from_u128(10),
                state: NodeState::Draining,
                expected_generation: 5,
            },
            MetadataCommand::CreateTopic {
                env: envelope(4),
                name: "events.v1".to_owned(),
                topic_uuid: Uuid::from_u128(20),
                root_range_uuid: Uuid::from_u128(21),
            },
            MetadataCommand::SplitRange {
                env: envelope(45),
                topic_uuid: Uuid::from_u128(20),
                parent_range_uuid: Uuid::from_u128(21),
                left_range_uuid: Uuid::from_u128(0x41),
                right_range_uuid: Uuid::from_u128(0x42),
                transition_id: Uuid::from_u128(0x77),
                barrier_offset: 640,
                expected_range_generation: 3,
                holder_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 9,
            },
            MetadataCommand::GrantRangeLease {
                env: envelope(5),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                holder_node_uuid: Uuid::from_u128(10),
                expected_range_generation: 0,
            },
            MetadataCommand::ReleaseRangeLease {
                env: envelope(6),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                expected_fencing_epoch: 1,
            },
            MetadataCommand::RegisterSealedSegment {
                env: envelope(7),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                segment_generation: 0,
                base_offset: 0,
                next_offset: 128,
                content_root: [7; 32],
                sealed_by_epoch: 1,
                expected_range_generation: 2,
            },
            MetadataCommand::MarkSegmentVerified {
                env: envelope(8),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                content_root: [7; 32],
                expected_generation: 0,
            },
            MetadataCommand::PutKeyRecord {
                env: envelope(9),
                key_uuid: Uuid::from_u128(40),
                scheme: 1,
                public_material_digest: [9; 32],
            },
            MetadataCommand::CreateConsumerGroup {
                env: envelope(10),
                name: "audit.consumers".to_owned(),
                group_uuid: Uuid::from_u128(50),
            },
            MetadataCommand::JoinConsumerGroup {
                env: envelope(11),
                group_uuid: Uuid::from_u128(50),
                member_uuid: Uuid::from_u128(51),
                expected_group_generation: 0,
            },
            MetadataCommand::LeaveConsumerGroup {
                env: envelope(12),
                group_uuid: Uuid::from_u128(50),
                member_uuid: Uuid::from_u128(51),
                expected_member_generation: 0,
            },
            MetadataCommand::AssignMemberRanges {
                env: envelope(13),
                group_uuid: Uuid::from_u128(50),
                member_uuid: Uuid::from_u128(51),
                ranges: vec![RangeAssignment {
                    topic_uuid: Uuid::from_u128(20),
                    range_uuid: Uuid::from_u128(21),
                }],
                expected_member_generation: 0,
            },
            MetadataCommand::CommitGroupCursor {
                env: envelope(14),
                group_uuid: Uuid::from_u128(50),
                member_uuid: Uuid::from_u128(51),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                topic_epoch: 1,
                range_generation: 0,
                segment_uuid: Uuid::from_u128(30),
                segment_generation: 0,
                segment_root: [3; 32],
                record_offset: 42,
                record_index: 7,
                lineage_transition_id: Some(Uuid::from_u128(60)),
                expected_checkpoint_generation: None,
            },
            MetadataCommand::HeartbeatMember {
                env: envelope(15),
                group_uuid: Uuid::from_u128(50),
                member_uuid: Uuid::from_u128(51),
            },
            MetadataCommand::ExpireStaleMember {
                env: envelope(16),
                group_uuid: Uuid::from_u128(50),
                member_uuid: Uuid::from_u128(51),
                stale_before_apply_index: 42,
            },
            MetadataCommand::SetNodePlacementAttrs {
                env: envelope(17),
                node_uuid: Uuid::from_u128(10),
                failure_domain: "rack-a".to_owned(),
                placement_weight: 100,
                expected_generation: 5,
            },
            MetadataCommand::CommitSegmentPlacement {
                env: envelope(18),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                replication_factor: 2,
                replica_nodes: vec![Uuid::from_u128(10), Uuid::from_u128(11)],
                expected_segment_generation: 1,
                expected_placement_generation: None,
            },
            MetadataCommand::CommitReplacementProof {
                env: envelope(19),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                expected_segment_generation: 1,
                content_root: [7; 32],
                expected_length_bytes: 4096,
                source_node_uuid: Uuid::from_u128(10),
                destination_node_uuid: Uuid::from_u128(11),
                fencing_epoch: 3,
                verification_method: VerificationMethod::AuthenticatedContentRoot,
                verifier_node_uuid: Uuid::from_u128(11),
                verified_term: 5,
            },
            MetadataCommand::PlanReplicaRetirement {
                env: envelope(20),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                retiring_node_uuid: Uuid::from_u128(10),
                expected_segment_generation: 1,
                fencing_epoch: 3,
            },
            MetadataCommand::ConfirmReplicaRetired {
                env: envelope(21),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                retiring_node_uuid: Uuid::from_u128(10),
                expected_segment_generation: 2,
            },
            MetadataCommand::ProposeRebalance {
                env: envelope(22),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                from_node_uuid: Uuid::from_u128(10),
                to_node_uuid: Uuid::from_u128(12),
                expected_placement_generation: 0,
            },
            MetadataCommand::CancelRebalance {
                env: envelope(23),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                expected_placement_generation: 1,
            },
            MetadataCommand::CommitTierEvidence {
                env: envelope(24),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                expected_segment_generation: 1,
                content_root: [7; 32],
                byte_length: 4096,
                backend_id: "s3-native".to_owned(),
                object_uri: "s3://tier/native/audit.v1/segment-30.segment".to_owned(),
                object_version_id: Some("4sL4kqCJo05qOWBhBqpfOFAdT4dRJVvW".to_owned()),
                manifest_version_id: Some("3sL4kqCJo05qOWBhBqpfOFAdT4dRJVvV".to_owned()),
                manifest_core_digest: [11; 32],
                verification_method: VerificationMethod::AuthenticatedContentRoot,
                verifier_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 3,
                verified_term: 5,
            },
            MetadataCommand::CommitTierEvidence {
                env: envelope(25),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                expected_segment_generation: 1,
                content_root: [7; 32],
                byte_length: 4096,
                backend_id: "localfs".to_owned(),
                object_uri: "s3://tier/native/audit.v1/segment-30.segment".to_owned(),
                object_version_id: None,
                manifest_version_id: None,
                manifest_core_digest: [11; 32],
                verification_method: VerificationMethod::AuthenticatedContentRoot,
                verifier_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 3,
                verified_term: 5,
            },
            MetadataCommand::SetTopicRetentionPolicy {
                env: envelope(26),
                topic_uuid: Uuid::from_u128(20),
                unarchived_deletion_allowed: true,
                expected_generation: None,
            },
            MetadataCommand::SetTopicRetentionPolicy {
                env: envelope(27),
                topic_uuid: Uuid::from_u128(20),
                unarchived_deletion_allowed: false,
                expected_generation: Some(2),
            },
            MetadataCommand::PlanRetention {
                env: envelope(28),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                expected_segment_generation: 1,
                fencing_epoch: 3,
            },
            MetadataCommand::ConfirmRetentionExpired {
                env: envelope(29),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                expected_segment_generation: 2,
            },
            MetadataCommand::CancelRetention {
                env: envelope(30),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                expected_segment_generation: 2,
            },
            MetadataCommand::SetFeatureLevel {
                env: envelope(36),
                feature_level: SUPPORTED_FEATURE_LEVEL,
            },
            MetadataCommand::SetRangeReplicaSet {
                env: envelope(37),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                members: vec![
                    Uuid::from_u128(10),
                    Uuid::from_u128(11),
                    Uuid::from_u128(12),
                ],
                expected_generation: 0,
            },
            MetadataCommand::ReportNodeVersion {
                env: envelope(38),
                node_uuid: Uuid::from_u128(10),
                release: "0.8.0".to_owned(),
                max_replica_protocol: 2,
                supported_feature_level: SUPPORTED_FEATURE_LEVEL,
            },
            MetadataCommand::AcquireRangeLease {
                env: envelope(31),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                holder_node_uuid: Uuid::from_u128(10),
                expected_range_generation: 3,
                lease_duration_ms: 10_000,
            },
            MetadataCommand::RenewRangeLease {
                env: envelope(32),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                holder_node_uuid: Uuid::from_u128(10),
                expected_fencing_epoch: 4,
                lease_duration_ms: 10_000,
            },
            MetadataCommand::ReportPromotionOutcome {
                env: envelope(33),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                holder_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 4,
                outcome: PromotionOutcome::Established {
                    boundary_offset: Some(401),
                    sealed_prefix_end: Some(300),
                    quorum: vec![
                        QuorumAnswer {
                            node_uuid: Uuid::from_u128(10),
                            offset: 401,
                            lineage: None,
                        },
                        QuorumAnswer {
                            node_uuid: Uuid::from_u128(11),
                            offset: 400,
                            lineage: None,
                        },
                    ],
                    votes: 2,
                    required: 2,
                    attestation: None,
                },
            },
            MetadataCommand::ReportPromotionOutcome {
                env: envelope(34),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                holder_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 5,
                outcome: PromotionOutcome::Refused {
                    reason: PromotionRefusal::LeaderBehind,
                },
            },
            // The same kind with a lineage verdict on every answer (#577):
            // every verdict, so the round-trip, trailing-byte and truncation
            // cases cover each arm of the new tag.
            MetadataCommand::ReportPromotionOutcome {
                env: envelope(39),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                holder_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 6,
                outcome: lineage_outcome(),
            },
            // The operator's override (#636): the attestation, a report
            // that used one, and the refusal that names the missing lineage.
            MetadataCommand::AttestUnprovenPromotion {
                env: envelope(41),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                replica_node_uuid: Uuid::from_u128(12),
                operator: "oncall@example".to_owned(),
            },
            MetadataCommand::ReportPromotionOutcome {
                env: envelope(42),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                holder_node_uuid: Uuid::from_u128(12),
                fencing_epoch: 7,
                outcome: attested_outcome(),
            },
            MetadataCommand::ReportPromotionOutcome {
                env: envelope(43),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                holder_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 8,
                outcome: PromotionOutcome::Refused {
                    reason: PromotionRefusal::LineageUnproven,
                },
            },
            // The four group commands a gateway sends (#457). They were not
            // in this list when they landed, so the round-trip, the
            // trailing-byte and the truncation cases never saw them; they are
            // here now, and the coordinated pair carries a coordinator range
            // deliberately unlike the range the cursor lands on, so a field
            // dropped or transposed on the wire cannot round-trip by
            // accident.
            MetadataCommand::EnsureGroupMemberForRange {
                env: envelope(35),
                name: "audit.consumers".to_owned(),
                group_uuid: Uuid::from_u128(50),
                member_uuid: Uuid::from_u128(51),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                holder_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 5,
            },
            MetadataCommand::CommitGroupCursorFenced {
                env: envelope(36),
                group_uuid: Uuid::from_u128(50),
                member_uuid: Uuid::from_u128(51),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                topic_epoch: 2,
                range_generation: 0,
                segment_uuid: Uuid::nil(),
                segment_generation: 0,
                segment_root: [0; 32],
                record_offset: 4_096,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: Some(7),
                holder_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 5,
            },
            MetadataCommand::EnsureGroupMemberCoordinated {
                env: envelope(37),
                name: "audit.consumers".to_owned(),
                group_uuid: Uuid::from_u128(50),
                member_uuid: Uuid::from_u128(51),
                topic_uuid: Uuid::from_u128(22),
                range_uuid: Uuid::from_u128(23),
                coordinator_topic_uuid: Uuid::from_u128(20),
                coordinator_range_uuid: Uuid::from_u128(21),
                holder_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 5,
            },
            MetadataCommand::CommitGroupCursorCoordinated {
                env: envelope(38),
                group_uuid: Uuid::from_u128(50),
                member_uuid: Uuid::from_u128(51),
                topic_uuid: Uuid::from_u128(22),
                range_uuid: Uuid::from_u128(23),
                coordinator_topic_uuid: Uuid::from_u128(20),
                coordinator_range_uuid: Uuid::from_u128(21),
                topic_epoch: 3,
                range_generation: 1,
                segment_uuid: Uuid::from_u128(30),
                segment_generation: 2,
                segment_root: [7; 32],
                record_offset: 8_192,
                record_index: 12,
                lineage_transition_id: Some(Uuid::from_u128(60)),
                expected_checkpoint_generation: None,
                holder_node_uuid: Uuid::from_u128(10),
                fencing_epoch: 5,
            },
        ]
    }

    fn every_response() -> Vec<MetadataResponse> {
        vec![
            MetadataResponse::Ack { generation: 3 },
            MetadataResponse::TopicCreated {
                topic_uuid: Uuid::from_u128(20),
                topic_epoch: 2,
                root_range_uuid: Uuid::from_u128(21),
            },
            MetadataResponse::LeaseGranted { fencing_epoch: 9 },
            MetadataResponse::TransitionRecorded { fencing_epoch: 9 },
            MetadataResponse::GroupCreated {
                group_uuid: Uuid::from_u128(50),
                generation: 0,
            },
            MetadataResponse::MemberJoined {
                member_generation: 0,
                group_generation: 1,
            },
            MetadataResponse::CursorCommitted {
                checkpoint_generation: 0,
            },
            MetadataResponse::Rejected(MetadataError::GenerationMismatch {
                expected: 1,
                actual: 2,
            }),
            MetadataResponse::Rejected(MetadataError::EpochMismatch {
                expected: 3,
                actual: 4,
            }),
            MetadataResponse::Rejected(MetadataError::LineageMismatch {
                expected: 0,
                actual: 1,
            }),
            MetadataResponse::Rejected(MetadataError::AlreadyExists),
            MetadataResponse::Rejected(MetadataError::NotFound),
            MetadataResponse::Rejected(MetadataError::invalid_transition("dead -> active")),
            MetadataResponse::Rejected(MetadataError::limit("too many ranges")),
        ]
    }

    /// A lineage-carrying outcome (#577) with every verdict: the candidate
    /// itself (agreed), a replica that discarded two records to agree, and
    /// one whose history could not be compared - which is not counted, so
    /// the boundary and the votes are those of the first two.
    fn lineage_outcome() -> PromotionOutcome {
        PromotionOutcome::Established {
            boundary_offset: Some(400),
            sealed_prefix_end: None,
            quorum: vec![
                QuorumAnswer {
                    node_uuid: Uuid::from_u128(10),
                    offset: 401,
                    lineage: Some(AnswerLineage::Agreed),
                },
                QuorumAnswer {
                    node_uuid: Uuid::from_u128(11),
                    offset: 400,
                    lineage: Some(AnswerLineage::Reconciled {
                        truncated_records: 2,
                    }),
                },
                QuorumAnswer {
                    node_uuid: Uuid::from_u128(12),
                    offset: 500,
                    lineage: Some(AnswerLineage::Unknown),
                },
            ],
            votes: 2,
            required: 2,
            attestation: None,
        }
    }

    /// The override's shape (#636): every answer Unknown - a range whose
    /// replicas have no epoch history - counted on the attestation naming
    /// the candidate, 12.
    fn attested_outcome() -> PromotionOutcome {
        PromotionOutcome::Established {
            boundary_offset: Some(500),
            sealed_prefix_end: None,
            quorum: vec![
                QuorumAnswer {
                    node_uuid: Uuid::from_u128(11),
                    offset: 500,
                    lineage: Some(AnswerLineage::Unknown),
                },
                QuorumAnswer {
                    node_uuid: Uuid::from_u128(12),
                    offset: 500,
                    lineage: Some(AnswerLineage::Unknown),
                },
            ],
            votes: 2,
            required: 2,
            attestation: Some(PromotionAttestation {
                attestation_id: Uuid::from_u128(41),
                replica_node_uuid: Uuid::from_u128(12),
                operator: "oncall@example".to_owned(),
                attested_at_ms: 1_700_000_000_000,
            }),
        }
    }

    /// #636: the attestation command, a report that used one, and the
    /// lineage-unproven refusal are each a payload no release below level 8
    /// can decode, so each is held back below it; the forms a lower level
    /// records instead are the ones it always recorded. An attested
    /// promotion has no lower form at all: it counted Unknown answers, and
    /// dropping them would leave a record whose votes do not add up.
    #[test]
    fn the_override_and_its_refusal_reason_need_their_own_level() {
        const { assert!(PROMOTE_UNPROVEN_FEATURE_LEVEL > LINEAGE_VERDICTS_FEATURE_LEVEL) };
        let refused = PromotionOutcome::Refused {
            reason: PromotionRefusal::LineageUnproven,
        };
        assert_eq!(refused.feature_level(), PROMOTE_UNPROVEN_FEATURE_LEVEL);
        assert_eq!(
            refused.without_unproven_refusal(),
            PromotionOutcome::Refused {
                reason: PromotionRefusal::QuorumUnavailable,
            },
            "below level 8 the refusal is recorded as it always was"
        );
        assert_eq!(refused.without_unproven_refusal().feature_level(), 2);

        let attested = attested_outcome();
        assert_eq!(attested.feature_level(), PROMOTE_UNPROVEN_FEATURE_LEVEL);
        assert_eq!(
            attested.attestation().map(|a| a.replica_node_uuid),
            Some(Uuid::from_u128(12))
        );
        assert_eq!(attested.without_lineage(), attested);
        assert_eq!(attested.without_unproven_refusal(), attested);
        // An attestation without verdicts would hide which answers were
        // counted on the operator's word: refused by the codec.
        let PromotionOutcome::Established { quorum, .. } = &attested else {
            unreachable!()
        };
        let mut unverdicted = attested.clone();
        if let PromotionOutcome::Established { quorum: q, .. } = &mut unverdicted {
            *q = quorum
                .iter()
                .map(|answer| QuorumAnswer {
                    lineage: None,
                    ..*answer
                })
                .collect();
        }
        assert!(matches!(
            encode_promotion_outcome(&mut Vec::new(), &unverdicted),
            Err(CodecError::InvalidValue {
                what: "promotion attestation",
                ..
            })
        ));
        // The attested tag reads as NEWER on a reader that predates it.
        let mut bytes = Vec::new();
        encode_promotion_outcome(&mut bytes, &attested).unwrap();
        assert_eq!(bytes[0], PROMOTION_OUTCOME_ESTABLISHED_UNDER_ATTESTATION);
        let mut refusal_bytes = Vec::new();
        encode_promotion_outcome(&mut refusal_bytes, &refused).unwrap();
        assert_eq!(refusal_bytes, vec![PROMOTION_OUTCOME_REFUSED, 5]);
        refusal_bytes[1] = 0x7f;
        assert!(crate::storage::is_newer_format(
            &decode_promotion_outcome(&mut Reader::new(&refusal_bytes)).unwrap_err()
        ));
        // Every refusal has a distinct operator-facing name.
        let names: std::collections::BTreeSet<_> = [
            PromotionRefusal::QuorumUnavailable,
            PromotionRefusal::LeaderBehind,
            PromotionRefusal::CandidateBehindVoters,
            PromotionRefusal::BelowPublished,
            PromotionRefusal::LineageUnproven,
        ]
        .into_iter()
        .map(PromotionRefusal::as_str)
        .collect();
        assert_eq!(names.len(), 5);
        // ...and a distinct wire tag that reads back as itself.
        let reasons = [
            PromotionRefusal::QuorumUnavailable,
            PromotionRefusal::LeaderBehind,
            PromotionRefusal::CandidateBehindVoters,
            PromotionRefusal::BelowPublished,
            PromotionRefusal::LineageUnproven,
        ];
        let tags: std::collections::BTreeSet<u8> =
            reasons.iter().map(|reason| reason.wire_tag()).collect();
        assert_eq!(tags.len(), reasons.len(), "two refusal reasons share a tag");
        for reason in reasons {
            assert_eq!(PromotionRefusal::from_wire(reason.wire_tag()), Ok(reason));
        }
    }

    /// #577: a promotion report carrying lineage verdicts is a payload an
    /// older voter cannot decode, so it needs its own feature level, and the
    /// form a lower level records instead carries no verdict and NO answer
    /// whose lineage was unknown - every reader of the legacy form counts
    /// every answer by its offset.
    #[test]
    fn a_lineage_carrying_report_is_gated_and_its_legacy_form_drops_unknown_answers() {
        let report = |outcome: PromotionOutcome| MetadataCommand::ReportPromotionOutcome {
            env: envelope(40),
            topic_uuid: Uuid::from_u128(20),
            range_uuid: Uuid::from_u128(21),
            holder_node_uuid: Uuid::from_u128(10),
            fencing_epoch: 6,
            outcome,
        };
        let with = lineage_outcome();
        assert!(with.records_lineage());
        assert_eq!(
            report(with.clone()).feature_level(),
            LINEAGE_VERDICTS_FEATURE_LEVEL,
            "verdicts are a payload v0.6.0 cannot decode"
        );
        const { assert!(LINEAGE_VERDICTS_FEATURE_LEVEL > BASELINE_FEATURE_LEVEL) };

        let legacy = with.without_lineage();
        assert!(!legacy.records_lineage());
        assert_eq!(report(legacy.clone()).feature_level(), 2);
        let PromotionOutcome::Established {
            quorum,
            votes,
            required,
            boundary_offset,
            ..
        } = &legacy
        else {
            panic!("{legacy:?}")
        };
        assert_eq!(
            quorum
                .iter()
                .map(|answer| (answer.node_uuid.as_u128(), answer.offset, answer.lineage))
                .collect::<Vec<_>>(),
            vec![(10, 401, None), (11, 400, None)],
            "the unknown answer at 500 would count by its offset in every legacy reader"
        );
        assert_eq!((*votes, *required, *boundary_offset), (2, 2, Some(400)));
        // The legacy form is byte-for-byte what a v0.5.0 report is.
        let mut bytes = Vec::new();
        encode_promotion_outcome(&mut bytes, &legacy).unwrap();
        assert_eq!(bytes[0], PROMOTION_OUTCOME_ESTABLISHED);

        // A refusal has no answers to judge and is unchanged.
        let refused = PromotionOutcome::Refused {
            reason: PromotionRefusal::QuorumUnavailable,
        };
        assert_eq!(refused.without_lineage(), refused);
        assert_eq!(report(refused).feature_level(), 2);
    }

    /// #577: verdicts are on every answer or on none, and an unknown verdict
    /// tag is a newer writer, not corruption.
    #[test]
    fn lineage_verdicts_are_all_or_nothing_and_an_unknown_one_reads_as_newer() {
        let PromotionOutcome::Established {
            boundary_offset,
            sealed_prefix_end,
            mut quorum,
            votes,
            required,
            ..
        } = lineage_outcome()
        else {
            unreachable!()
        };
        quorum[1].lineage = None;
        let mixed = PromotionOutcome::Established {
            boundary_offset,
            sealed_prefix_end,
            quorum,
            votes,
            required,
            attestation: None,
        };
        let mut out = Vec::new();
        assert!(matches!(
            encode_promotion_outcome(&mut out, &mixed),
            Err(CodecError::InvalidValue { .. })
        ));

        let mut bytes = Vec::new();
        encode_promotion_outcome(&mut bytes, &lineage_outcome()).unwrap();
        // outcome tag, boundary (1 + 8), sealed prefix (1), count (2), then
        // the first answer's uuid and offset: its verdict byte follows.
        let verdict_at = 1 + 9 + 1 + 2 + 16 + 8;
        assert_eq!(bytes[verdict_at], ANSWER_LINEAGE_AGREED);
        bytes[verdict_at] = 0x7f;
        let error = decode_promotion_outcome(&mut Reader::new(&bytes)).unwrap_err();
        assert_eq!(
            error,
            CodecError::UnknownTag {
                what: "quorum answer lineage",
                tag: 0x7f
            }
        );
        assert!(crate::storage::is_newer_format(&error));
        let mut unknown_outcome = bytes.clone();
        unknown_outcome[0] = 0x7f;
        assert!(crate::storage::is_newer_format(
            &decode_promotion_outcome(&mut Reader::new(&unknown_outcome)).unwrap_err()
        ));
    }

    /// #554: a command kind an older voter does not know must not enter the
    /// log until the cluster's level says every voter does. So every kind
    /// added after v0.6.0 (the last release before levels: kinds 1..=34)
    /// carries a level above the baseline, and a future kind that forgets
    /// one fails here rather than stalling a rolling upgrade. The one older
    /// kind with a newer PAYLOAD - a promotion report carrying lineage
    /// verdicts (#577), an outcome tag v0.6.0 cannot decode - is gated too,
    /// and nothing else of an older kind may be.
    #[test]
    fn every_kind_newer_than_the_baseline_release_is_gated() {
        const LAST_BASELINE_KIND: u16 = 34;
        let mut seen = std::collections::BTreeSet::new();
        for command in every_command() {
            let bytes = command.encode().unwrap();
            let kind = u16::from_be_bytes([bytes[0], bytes[1]]);
            seen.insert(kind);
            let level = command.feature_level();
            assert!(
                (1..=SUPPORTED_FEATURE_LEVEL).contains(&level),
                "kind {kind}: level {level}"
            );
            // The report's newer payloads: lineage verdicts (#577), an
            // attestation and the lineage-unproven refusal (#636).
            if let MetadataCommand::ReportPromotionOutcome { outcome, .. } = &command {
                if outcome.feature_level() > 2 {
                    assert!(
                        [
                            LINEAGE_VERDICTS_FEATURE_LEVEL,
                            PROMOTE_UNPROVEN_FEATURE_LEVEL
                        ]
                        .contains(&level),
                        "kind {kind}: level {level}"
                    );
                    continue;
                }
            }
            assert_eq!(
                kind > LAST_BASELINE_KIND,
                level > BASELINE_FEATURE_LEVEL,
                "kind {kind} has level {level}; kinds after {LAST_BASELINE_KIND} must be gated \
                 above the baseline {BASELINE_FEATURE_LEVEL}, and older ones must not be"
            );
        }
        let highest = *seen.last().unwrap();
        assert_eq!(
            seen.len(),
            usize::from(highest),
            "every kind 1..={highest} must be in every_command, or this check is blind to it"
        );
    }

    /// #634 review: a version report's release is 1..=MAX_RELEASE_BYTES, and
    /// the codec enforces both ends, as it does a node address - so an empty
    /// one is refused at the admin endpoint and never reaches a log. Safe to
    /// enforce at decode only because the kind is new in this release: no
    /// log written before it can hold one.
    #[test]
    fn a_version_report_with_an_empty_release_does_not_decode() {
        let empty = MetadataCommand::ReportNodeVersion {
            env: envelope(38),
            node_uuid: Uuid::from_u128(10),
            release: String::new(),
            max_replica_protocol: 2,
            supported_feature_level: SUPPORTED_FEATURE_LEVEL,
        };
        let encoded = empty.encode().unwrap();
        assert!(
            matches!(
                MetadataCommand::decode(&encoded),
                Err(CodecError::InvalidValue {
                    what: "release",
                    ..
                })
            ),
            "{:?}",
            MetadataCommand::decode(&encoded)
        );
    }

    /// #694: every command is on one side of the audit list (the match in
    /// `audit::command_audit` has no wildcard, so a new variant does not
    /// compile until it is placed), and the sides are the ones the plan
    /// chose: operator commands audited; node-initiated and client-rate
    /// commands not.
    #[test]
    fn every_command_is_on_one_side_of_the_audit_list() {
        use crate::audit::{command_audit, command_name, AuditAction};
        let commands = every_command();
        let mut names: Vec<_> = commands.iter().map(command_name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), 39, "every_command() must hold every variant");
        let audited: std::collections::BTreeSet<_> = commands
            .iter()
            .filter(|c| command_audit(c).is_some())
            .map(command_name)
            .collect();
        let expected: std::collections::BTreeSet<_> = [
            "RegisterNode",
            "SetNodeState",
            "CreateTopic",
            "GrantRangeLease",
            "ReleaseRangeLease",
            "RegisterSealedSegment",
            "MarkSegmentVerified",
            "PutKeyRecord",
            "SetNodePlacementAttrs",
            "CommitSegmentPlacement",
            "CommitReplacementProof",
            "PlanReplicaRetirement",
            "ConfirmReplicaRetired",
            "ProposeRebalance",
            "CancelRebalance",
            "CommitTierEvidence",
            "SetTopicRetentionPolicy",
            "PlanRetention",
            "ConfirmRetentionExpired",
            "CancelRetention",
            "SetFeatureLevel",
            "SetRangeReplicaSet",
            "AttestUnprovenPromotion",
        ]
        .into_iter()
        .collect();
        assert_eq!(audited, expected);
        for command in &commands {
            let Some(audit) = command_audit(command) else {
                continue;
            };
            let expected_action = match command {
                MetadataCommand::SetFeatureLevel { .. } => AuditAction::ClusterEnableFeatures,
                MetadataCommand::CreateTopic { .. } => AuditAction::ClusterCreateTopic,
                MetadataCommand::AttestUnprovenPromotion { .. } => AuditAction::PromotionAttest,
                _ => AuditAction::ClusterPropose,
            };
            assert_eq!(audit.action, expected_action, "{}", audit.command);
            // Identifiers and small values only: never a URI, a digest or
            // key material.
            for (name, value) in &audit.params {
                assert!(
                    !value.contains("://") && value.len() < 512,
                    "{} param {name} = {value:?}",
                    audit.command
                );
                assert!(
                    !value.chars().all(|c| c.is_ascii_hexdigit()) || value.len() < 32,
                    "{} param {name} looks like a digest: {value}",
                    audit.command
                );
            }
        }
    }

    #[test]
    fn every_command_and_response_round_trips_byte_exactly() {
        for command in every_command() {
            let encoded = command.encode().unwrap();
            assert_eq!(MetadataCommand::decode(&encoded).unwrap(), command);
        }
        for response in every_response() {
            let encoded = response.encode().unwrap();
            assert_eq!(MetadataResponse::decode(&encoded).unwrap(), response);
        }
    }

    #[test]
    fn decode_rejects_trailing_bytes_truncation_and_unknown_kinds() {
        for command in every_command() {
            let mut trailing = command.encode().unwrap();
            trailing.push(0);
            let decoded = MetadataCommand::decode(&trailing);
            if matches!(
                command,
                MetadataCommand::CommitTierEvidence {
                    object_version_id: None,
                    ..
                }
            ) {
                assert!(matches!(
                    decoded,
                    Err(CodecError::InvalidValue {
                        what: "object version presence",
                        ..
                    })
                ));
            } else {
                assert_eq!(decoded, Err(CodecError::Trailing(1)));
            }

            let encoded = command.encode().unwrap();
            let mut truncated = encoded.clone();
            truncated.pop();
            assert!(
                matches!(
                    MetadataCommand::decode(&truncated),
                    Err(CodecError::Truncated(_) | CodecError::InvalidUtf8(_))
                ),
                "{command:?}"
            );
        }
        assert_eq!(
            MetadataCommand::decode(&[0, 99]),
            Err(CodecError::UnknownTag {
                what: "command kind",
                tag: 99,
            })
        );
        assert_eq!(
            MetadataResponse::decode(&[0, 99]),
            Err(CodecError::UnknownTag {
                what: "response kind",
                tag: 99,
            })
        );

        let mut rejected = MetadataResponse::Rejected(MetadataError::NotFound)
            .encode()
            .unwrap();
        rejected.push(1);
        assert_eq!(
            MetadataResponse::decode(&rejected),
            Err(CodecError::Trailing(1))
        );
    }

    #[test]
    fn oversized_and_empty_bounded_strings_are_rejected_by_the_codec() {
        let command = MetadataCommand::RegisterNode {
            env: envelope(1),
            node_uuid: Uuid::from_u128(10),
            addr: "x".repeat(MAX_NODE_ADDR_BYTES + 1),
            expected_generation: None,
        };
        assert!(matches!(
            command.encode(),
            Err(CodecError::BoundExceeded { .. })
        ));

        // An empty address survives encode but the decoder rejects it, so it
        // can never round-trip into apply.
        let empty_addr = MetadataCommand::RegisterNode {
            env: envelope(1),
            node_uuid: Uuid::from_u128(10),
            addr: String::new(),
            expected_generation: None,
        };
        assert!(matches!(
            MetadataCommand::decode(&empty_addr.encode().unwrap()),
            Err(CodecError::InvalidValue { .. })
        ));

        let long_topic = MetadataCommand::CreateTopic {
            env: envelope(2),
            name: "y".repeat(MAX_TOPIC_NAME_BYTES + 1),
            topic_uuid: Uuid::from_u128(20),
            root_range_uuid: Uuid::from_u128(21),
        };
        assert!(matches!(
            long_topic.encode(),
            Err(CodecError::BoundExceeded { .. })
        ));
    }

    /// A CommitTierEvidence with the variable-length fields parameterized so
    /// bound tests can probe each one without functional-update syntax
    /// (unavailable on enum variants).
    fn tier_evidence(
        request: u128,
        byte_length: u64,
        backend_id: &str,
        object_uri: &str,
        object_version_id: Option<String>,
        manifest_version_id: Option<String>,
    ) -> MetadataCommand {
        MetadataCommand::CommitTierEvidence {
            env: envelope(request),
            topic_uuid: Uuid::from_u128(20),
            range_uuid: Uuid::from_u128(21),
            segment_uuid: Uuid::from_u128(30),
            expected_segment_generation: 1,
            content_root: [7; 32],
            byte_length,
            backend_id: backend_id.to_owned(),
            object_uri: object_uri.to_owned(),
            object_version_id,
            manifest_version_id,
            manifest_core_digest: [11; 32],
            verification_method: VerificationMethod::AuthenticatedContentRoot,
            verifier_node_uuid: Uuid::from_u128(10),
            fencing_epoch: 3,
            verified_term: 5,
        }
    }

    #[test]
    fn tier_evidence_codec_enforces_every_bound_and_validity_rule() {
        // Exactly at the URI bound round-trips; one byte over rejects at
        // encode time.
        let at_bound = tier_evidence(
            1,
            4096,
            "s3-native",
            &"u".repeat(MAX_TIER_OBJECT_URI_BYTES),
            None,
            None,
        );
        let encoded = at_bound.encode().unwrap();
        assert_eq!(MetadataCommand::decode(&encoded).unwrap(), at_bound);
        assert!(matches!(
            tier_evidence(
                2,
                4096,
                "s3-native",
                &"u".repeat(MAX_TIER_OBJECT_URI_BYTES + 1),
                None,
                None,
            )
            .encode(),
            Err(CodecError::BoundExceeded { .. })
        ));

        // Backend id and version id bounds reject at encode time.
        assert!(matches!(
            tier_evidence(
                3,
                4096,
                &"b".repeat(MAX_TIER_BACKEND_ID_BYTES + 1),
                "s3://tier/object",
                None,
                None,
            )
            .encode(),
            Err(CodecError::BoundExceeded { .. })
        ));
        assert!(matches!(
            tier_evidence(
                4,
                4096,
                "s3-native",
                "s3://tier/object",
                None,
                Some("v".repeat(MAX_TIER_VERSION_ID_BYTES + 1)),
            )
            .encode(),
            Err(CodecError::BoundExceeded { .. })
        ));
        assert!(matches!(
            tier_evidence(
                5,
                4096,
                "s3-native",
                "s3://tier/object",
                Some("v".repeat(MAX_TIER_VERSION_ID_BYTES + 1)),
                None,
            )
            .encode(),
            Err(CodecError::BoundExceeded { .. })
        ));

        // Zero byte length and empty strings survive encode but the decoder
        // rejects them, so they can never round-trip into apply.
        for invalid in [
            tier_evidence(6, 0, "s3-native", "s3://tier/object", None, None),
            tier_evidence(7, 4096, "", "s3://tier/object", None, None),
            tier_evidence(8, 4096, "s3-native", "", None, None),
        ] {
            assert!(matches!(
                MetadataCommand::decode(&invalid.encode().unwrap()),
                Err(CodecError::InvalidValue { .. })
            ));
        }

        // An unknown verification-method tag is rejected, never defaulted.
        let mut unknown_method =
            tier_evidence(9, 4096, "s3-native", "s3://tier/object", None, None)
                .encode()
                .unwrap();
        // The method byte sits immediately before verifier uuid + two u64s.
        let method_at = unknown_method.len() - 8 - 8 - 16 - 1;
        assert_eq!(unknown_method[method_at], 1);
        unknown_method[method_at] = 9;
        assert_eq!(
            MetadataCommand::decode(&unknown_method),
            Err(CodecError::UnknownTag {
                what: "verification method",
                tag: 9,
            })
        );

        // The retention-policy flag byte is canonical (0 or 1 only).
        let policy = MetadataCommand::SetTopicRetentionPolicy {
            env: envelope(9),
            topic_uuid: Uuid::from_u128(20),
            unarchived_deletion_allowed: false,
            expected_generation: None,
        };
        let mut bytes = policy.encode().unwrap();
        let flag_at = bytes.len() - 2;
        assert_eq!(bytes[flag_at], 0);
        bytes[flag_at] = 2;
        assert!(matches!(
            MetadataCommand::decode(&bytes),
            Err(CodecError::InvalidValue { .. })
        ));
    }

    #[test]
    fn error_detail_constructors_truncate_at_a_character_boundary() {
        // 255 ASCII bytes then a 2-byte character straddling the 256 bound.
        let detail = format!("{}é", "a".repeat(255));
        let MetadataError::InvalidTransition(bounded) = MetadataError::invalid_transition(detail)
        else {
            panic!("constructor changed variant");
        };
        assert_eq!(bounded.len(), 255);
        assert!(
            MetadataResponse::Rejected(MetadataError::InvalidTransition(bounded))
                .encode()
                .is_ok()
        );

        let MetadataError::Limit(long) = MetadataError::limit("z".repeat(400)) else {
            panic!("constructor changed variant");
        };
        assert_eq!(long.len(), MAX_ERROR_DETAIL_BYTES);
    }

    /// The promotion report round-trips byte-exactly in both shapes, its
    /// quorum list is bounded on both sides, and its response has a kind of
    /// its own (#240 item 5).
    #[test]
    fn report_promotion_outcome_round_trips_and_bounds_its_quorum() {
        let env = CommandEnvelope {
            request_id: Uuid::from_u128(0x77),
            issued_at_ms: 1_700_000_000_000,
        };
        let served = MetadataCommand::ReportPromotionOutcome {
            env,
            topic_uuid: Uuid::from_u128(20),
            range_uuid: Uuid::from_u128(21),
            holder_node_uuid: Uuid::from_u128(10),
            fencing_epoch: 4,
            outcome: PromotionOutcome::Established {
                boundary_offset: Some(401),
                sealed_prefix_end: None,
                quorum: vec![
                    QuorumAnswer {
                        node_uuid: Uuid::from_u128(10),
                        offset: 401,
                        lineage: None,
                    },
                    QuorumAnswer {
                        node_uuid: Uuid::from_u128(11),
                        offset: 400,
                        lineage: None,
                    },
                ],
                votes: 2,
                required: 2,
                attestation: None,
            },
        };
        assert_eq!(
            MetadataCommand::decode(&served.encode().unwrap()).unwrap(),
            served
        );
        let refused = MetadataCommand::ReportPromotionOutcome {
            env,
            topic_uuid: Uuid::from_u128(20),
            range_uuid: Uuid::from_u128(21),
            holder_node_uuid: Uuid::from_u128(10),
            fencing_epoch: 5,
            outcome: PromotionOutcome::Refused {
                reason: PromotionRefusal::CandidateBehindVoters,
            },
        };
        assert_eq!(
            MetadataCommand::decode(&refused.encode().unwrap()).unwrap(),
            refused
        );

        let MetadataCommand::ReportPromotionOutcome { outcome, .. } = &served else {
            unreachable!()
        };
        let mut oversized = outcome.clone();
        if let PromotionOutcome::Established { quorum, .. } = &mut oversized {
            *quorum = (0..MAX_TRANSITION_QUORUM as u128 + 1)
                .map(|node| QuorumAnswer {
                    node_uuid: Uuid::from_u128(node),
                    offset: 0,
                    lineage: None,
                })
                .collect();
        }
        let mut out = Vec::new();
        assert!(matches!(
            encode_promotion_outcome(&mut out, &oversized),
            Err(CodecError::BoundExceeded { .. })
        ));

        let response = MetadataResponse::TransitionRecorded { fencing_epoch: 4 };
        assert_eq!(
            MetadataResponse::decode(&response.encode().unwrap()).unwrap(),
            response
        );
    }

    #[test]
    fn v1_create_topic_command_matches_golden_vector() {
        let command = MetadataCommand::CreateTopic {
            env: CommandEnvelope {
                request_id: Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
                issued_at_ms: 0x0102_0304_0506_0708,
            },
            name: "audit.v1".to_owned(),
            topic_uuid: Uuid::parse_str("ffeeddcc-bbaa-9988-7766-554433221100").unwrap(),
            root_range_uuid: Uuid::parse_str("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0").unwrap(),
        };
        let encoded = command.encode().unwrap();
        let hex: String = encoded.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            hex,
            concat!(
                "000300112233445566778899aabbccddeeff0102030405060708",
                "000861756469742e7631",
                "ffeeddccbbaa99887766554433221100",
                "0f1e2d3c4b5a69788796a5b4c3d2e1f0"
            )
        );
    }
}
