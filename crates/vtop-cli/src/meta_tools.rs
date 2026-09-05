//! `vtopctl meta` — admin client for the metadata Raft group.
//!
//! Unlike `segment` tools these take `--config`: a small YAML describing the
//! admin mTLS endpoint. Secrets stay on disk as PEM paths; the YAML itself
//! never embeds key material.

use clap::{Args, Subcommand};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_meta::command::{CommandEnvelope, NodeState, MAX_NODE_ADDR_BYTES};
use vtop_meta::{
    resolve_endpoint, AdminCandidate, AdminClient, AdminStatusResponse, MetaNodeId,
    MetadataCommand, MetadataResponse, TlsMaterial, WireLogId,
};
use vtop_meta::{AdminTransitionView, GrantKind, PromotionOutcome, TransitionOutcome};
use vtop_protocol::ReplicaEpochStart;

#[derive(Subcommand, Debug)]
pub enum MetaCommand {
    /// Show Raft status and membership from the admin endpoint.
    Status {
        #[command(flatten)]
        common: MetaCommonArgs,
    },
    /// Show the current voter/learner membership.
    Membership {
        #[command(flatten)]
        common: MetaCommonArgs,
    },
    /// Bootstrap a fresh Raft group with these voter ids (#215). One-shot:
    /// every listed node must be running with an empty log.
    Init {
        #[command(flatten)]
        common: MetaCommonArgs,
        /// Comma-separated voter node ids, e.g. `1,2,3`.
        #[arg(long, value_delimiter = ',', required = true)]
        members: Vec<u64>,
    },
    /// Add a learner that replicates the log without joining quorum (#215).
    AddLearner {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        node_id: u64,
    },
    /// Replace the voter set via joint consensus (#215). Removed voters are
    /// fenced: they cannot commit after losing membership.
    ChangeMembership {
        #[command(flatten)]
        common: MetaCommonArgs,
        /// Comma-separated voter node ids, e.g. `1,2,3,4,5`.
        #[arg(long, value_delimiter = ',', required = true)]
        voters: Vec<u64>,
        /// Keep removed voters as learners instead of dropping them.
        #[arg(long, default_value_t = false)]
        retain_removed_as_learners: bool,
    },
    /// Create a topic and its root range.
    CreateTopic {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        name: String,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        root_range_uuid: Uuid,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Read a range's current lease through a linearizable fence (#223).
    ///
    /// The read an election makes before it acts: it reports the holder, the
    /// epoch, and the deadline, so a candidate can tell "the leader is healthy"
    /// apart from "my view was stale".
    RangeLease {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
    },
    /// The range's leadership-transition chain (#240 item 5), audited: each
    /// link's epoch continuity, each established promotion's vote recomputed
    /// from the quorum it recorded, and — given --mac-key-env — each
    /// statement's MAC verified against the range identity asked for here,
    /// never one carried in the reply. Exits non-zero when any check fails.
    Transitions {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        /// The first epoch to read; the chain is read upward from here.
        #[arg(long, default_value_t = 1)]
        from_epoch: u64,
        /// Records per page; the whole chain is read, page after page,
        /// whatever the page size (the server clamps it to its own maximum).
        /// A page of zero would read nothing and pass, so it is refused.
        #[arg(long, default_value_t = 256, value_parser = clap::value_parser!(u16).range(1..))]
        limit: u16,
        /// Environment variable holding the 32-byte hex MAC key. Without it a
        /// signed statement is reported unverified, never verified.
        #[arg(long)]
        mac_key_env: Option<String>,
        /// A `vtopctl node status` config naming the range's replicas: each
        /// replica's epoch vector — which fencing epoch wrote each stretch of
        /// its log — is read over the replica plane and held to the chain
        /// (#240). An epoch no grant explains, an epoch starting below the
        /// boundary its promotion proved, or a replica that cannot be asked,
        /// fails the audit by name.
        #[arg(long)]
        replicas: Option<std::path::PathBuf>,
        /// Per-replica timeout for the epoch-vector read. Zero would time
        /// every read out and call healthy replicas unreachable, so it is
        /// refused.
        #[arg(long, default_value_t = 5_000, value_parser = clap::value_parser!(u64).range(1..))]
        replica_timeout_ms: u64,
    },
    /// Propose `RegisterNode` through the Consensus façade.
    RegisterNode {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        node_uuid: Uuid,
        #[arg(long)]
        addr: String,
        #[arg(long)]
        expected_generation: Option<u64>,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Propose `SetNodeState` through the Consensus façade.
    SetNodeState {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        node_uuid: Uuid,
        #[arg(long, value_parser = parse_node_state)]
        state: NodeState,
        #[arg(long)]
        expected_generation: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Propose raw `CommitTierEvidence` (escape hatch for evidence produced
    /// by tooling other than `vtopctl tier copy`).
    CommitTierEvidence {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        expected_generation: u64,
        /// Sealed content root as 64 hex characters.
        #[arg(long, value_parser = parse_hash32)]
        content_root: [u8; 32],
        #[arg(long)]
        byte_length: u64,
        #[arg(long)]
        backend_id: String,
        #[arg(long)]
        object_uri: String,
        /// Immutable segment-object version id (omit only for unversioned backends).
        #[arg(long)]
        object_version_id: Option<String>,
        /// Immutable manifest version id (omit only for unversioned backends).
        #[arg(long)]
        manifest_version_id: Option<String>,
        /// BLAKE3 digest (64 hex characters) of the canonical manifest core.
        #[arg(long, value_parser = parse_hash32)]
        manifest_core_digest: [u8; 32],
        #[arg(long)]
        verifier_node_uuid: Uuid,
        #[arg(long)]
        fencing_epoch: u64,
        #[arg(long)]
        verified_term: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Register a sealed segment in metadata (#180).
    ///
    /// THE FIRST STEP OF THE WHOLE SEGMENT LIFECYCLE, and it had no CLI. A
    /// placement, a replacement proof, a retirement and a retention decision
    /// all name a segment metadata must already know about — `NotFound`
    /// otherwise — so without this the rest of the flow was unreachable for
    /// any segment a real node had actually sealed.
    ///
    /// The offsets and the content root come from `vtopctl segment verify`,
    /// which re-derives them from the frames. Registering figures a node has
    /// not verified would put a root in metadata that the bytes do not
    /// support, and every later proof compares against it.
    RegisterSealedSegment {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        segment_generation: u64,
        #[arg(long)]
        base_offset: u64,
        #[arg(long)]
        next_offset: u64,
        /// Sealed content root as 64 hex characters, from `segment verify`.
        #[arg(long, value_parser = parse_hash32)]
        content_root: [u8; 32],
        /// The fencing epoch the sealing leader held. A segment sealed under a
        /// deposed epoch belongs to a history the cluster moved past.
        #[arg(long)]
        sealed_by_epoch: u64,
        #[arg(long)]
        expected_range_generation: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Record that a sealed segment's bytes were verified (#180).
    ///
    /// THE SECOND LINK, and it had no CLI either. `commit-segment-placement`
    /// refuses a segment that is not yet Verified — "placement requires a
    /// verified segment" — so registering one was not enough to place it, and
    /// the chain register → verify → place was broken at both of its first two
    /// links.
    ///
    /// The root is checked against the registered one, so this cannot bless a
    /// segment as something it is not: it asserts that a node READ these bytes
    /// and got this root, which is why it is a separate step from registering
    /// the claim.
    MarkSegmentVerified {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        /// The root `vtopctl segment verify` re-derived from the frames. Must
        /// equal what was registered.
        #[arg(long, value_parser = parse_hash32)]
        content_root: [u8; 32],
        #[arg(long)]
        expected_generation: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Read a segment's placement, generation, and any open rebalance (#308).
    ///
    /// THE READ EVERY OTHER COMMAND IN THIS FLOW DEPENDS ON. They all take
    /// `--expected-placement-generation`, and `commit-segment-placement`
    /// additionally needs the replica set in placement order. Without this,
    /// those values could only be obtained by having watched the `Ack` that
    /// produced them — so after a restart, a handover, or an incident the only
    /// route was to guess and submit rejected writes until one landed. That is
    /// a poor interface for a routine operation and an actively bad one during
    /// an incident, which is when replacements happen.
    ///
    /// Reports the open rebalance intent too. It matters as much as the
    /// generation: while one stands the segment refuses placement updates,
    /// further proposals, and retention planning, and a stale generation and a
    /// blocked segment produce similar-looking rejections that want opposite
    /// responses — one is "read again and retry", the other is
    /// "cancel-rebalance first".
    ///
    /// Linearizable, so the answer cannot come from a deposed node's lagging
    /// copy. A placement read that can lag is worse than none, because a
    /// compare-and-swap built on it fails exactly like somebody else's
    /// concurrent write.
    GetPlacement {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        /// Also report the placement the algorithm WOULD choose at this
        /// factor.
        ///
        /// The only way to build a FIRST placement. `commit-segment-placement`
        /// compares a proposal positionally against a rendezvous over the
        /// currently Active nodes, and an operator can see neither the
        /// candidate set nor the algorithm — so without asking, the only route
        /// to the right list was to guess an order and resubmit until one was
        /// accepted.
        #[arg(long)]
        for_replication_factor: Option<u8>,
    },
    /// Set a node's placement attributes (#180).
    ///
    /// A PREREQUISITE for any multi-replica placement, and easy to miss.
    /// `register-node` leaves `failure_domain` empty, and
    /// `CommitSegmentPlacement` requires DISTINCT failure domains whenever the
    /// replication factor exceeds one — so a cluster built entirely through
    /// this CLI cannot commit a placement for RF > 1 until every node has been
    /// given one. The rejection arrives from the state machine and names the
    /// domains rather than the command that should have set them, which is a
    /// hard trail to follow backwards.
    ///
    /// The domain is whatever failure the deployment wants replicas spread
    /// across: a rack, an availability zone, a host. Metadata does not
    /// interpret it beyond requiring distinctness.
    SetNodePlacementAttrs {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        node_uuid: Uuid,
        /// Rack, zone, host — whatever the deployment wants replicas spread
        /// across. Two replicas of a segment never share one.
        #[arg(long)]
        failure_domain: String,
        /// Relative share of placements this node attracts. Equal weights
        /// spread evenly.
        ///
        /// REQUIRED, with no default, because the command replaces the domain
        /// and the weight together. A default of 1 would mean that correcting a
        /// typo in a failure domain silently resets a node weighted above 1
        /// back to 1 — changing deterministic placement and capacity
        /// distribution as a side effect of an edit that named neither. Pass
        /// the node's current weight to keep it.
        #[arg(long)]
        placement_weight: u32,
        #[arg(long)]
        expected_generation: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Open a rebalance move, adding a destination replica before the source
    /// retires (#181).
    ///
    /// THE STEP THAT MAKES REPLACEMENT POSSIBLE at full replication factor.
    /// `plan-replica-retirement` requires the placement to contain BOTH the
    /// retiring source and the verified destination, and
    /// `commit-segment-placement` cannot produce that: it refuses a list whose
    /// length differs from the declared factor. This is what adds the
    /// destination, so the segment runs at RF + 1 for the duration of the move
    /// and never at RF - 1 — the whole point being that durability does not dip
    /// while a replica is being replaced.
    ///
    /// The move then completes through the ordinary flow:
    /// `commit-replacement-proof`, `plan-replica-retirement`,
    /// `confirm-replica-retired`.
    ProposeRebalance {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        /// The replica being replaced. Stays in the placement until it is
        /// retired.
        #[arg(long)]
        from_node_uuid: Uuid,
        /// The replica taking over. Added now, proven later.
        #[arg(long)]
        to_node_uuid: Uuid,
        #[arg(long)]
        expected_placement_generation: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Abandon an open rebalance, releasing the segment (#181).
    ///
    /// THE WAY OUT when a move cannot finish. `propose-rebalance` writes an
    /// intent that persists until the move completes or is cancelled, and while
    /// it stands the segment refuses placement updates, further rebalance
    /// proposals, and retention planning. A destination that dies mid-copy, or
    /// bytes that fail verification, therefore leaves the segment locked — and
    /// the failure that caused it is exactly the situation in which an operator
    /// least wants a second, silent problem.
    ///
    /// Cancelling releases the segment and leaves the ORIGINAL replica set
    /// untouched, because the source never stopped serving: the destination is
    /// added by the proposal and only retired into by
    /// `plan-replica-retirement`. Nothing durable is given up by cancelling a
    /// move that did not finish.
    CancelRebalance {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        expected_placement_generation: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Record which nodes hold a sealed segment (#180).
    ///
    /// `--replication-factor` is an INDEPENDENT durability target, not a count
    /// of the list: the state machine refuses a placement whose declared factor
    /// and actual replicas disagree, because a placement that silently records
    /// fewer replicas than it promises is a durability claim nobody made.
    CommitSegmentPlacement {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        replication_factor: u8,
        /// Repeat once per replica, IN THE ORDER METADATA COMPUTES.
        ///
        /// This is a confirmation, not a choice. `apply` derives the replica
        /// set deterministically by rendezvous over the currently Active nodes
        /// and compares the proposal POSITIONALLY against it — the same UUIDs
        /// in a different order are refused, as is any set the algorithm would
        /// not have chosen. Supplying a list here states which placement the
        /// proposer believes is current, so a proposal built against a stale
        /// view of the cluster fails instead of silently committing a
        /// different one.
        #[arg(long = "replica-node", required = true)]
        replica_nodes: Vec<Uuid>,
        #[arg(long)]
        expected_segment_generation: u64,
        /// Omit for the FIRST placement of this segment; supply the current
        /// generation to CAS-update an existing one. Omitting it against an
        /// existing placement is refused rather than treated as zero — the two
        /// are different intentions and only one of them is safe.
        #[arg(long)]
        expected_placement_generation: Option<u64>,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Commit evidence that a replacement replica really holds the segment
    /// (#181).
    ///
    /// THE EVIDENCE MUST COME FROM A VERIFIER, not from an operator retyping
    /// numbers. `--content-root` and `--expected-length-bytes` are what
    /// `vtopctl segment verify` reports for the destination copy; a proof
    /// asserting a root nobody computed is worse than no proof, because the
    /// state machine believes it and will then permit a retirement on its
    /// strength.
    CommitReplacementProof {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        expected_segment_generation: u64,
        /// Sealed content root as 64 hex characters, as verified on the
        /// DESTINATION replica.
        #[arg(long, value_parser = parse_hash32)]
        content_root: [u8; 32],
        #[arg(long)]
        expected_length_bytes: u64,
        /// The replica the bytes came from.
        #[arg(long)]
        source_node_uuid: Uuid,
        /// The replica that now holds them.
        #[arg(long)]
        destination_node_uuid: Uuid,
        #[arg(long)]
        fencing_epoch: u64,
        /// Who performed the verification. Recorded for audit: a proof is only
        /// as good as the identity that stands behind it.
        #[arg(long)]
        verifier_node_uuid: Uuid,
        #[arg(long)]
        verified_term: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Plan a replica's retirement (#181). REFUSED unless a matching
    /// replacement proof has already committed — which is the entire point:
    /// the copy must be proven before the original is allowed to go.
    PlanReplicaRetirement {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        retiring_node_uuid: Uuid,
        #[arg(long)]
        expected_segment_generation: u64,
        #[arg(long)]
        fencing_epoch: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Confirm a planned retirement actually happened (#181).
    ///
    /// Run AFTER the bytes are physically gone. This consumes the replacement
    /// proof, so proposing it before the deletion would leave metadata
    /// believing a replica is retired while its data is still on disk.
    ConfirmReplicaRetired {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        retiring_node_uuid: Uuid,
        #[arg(long)]
        expected_segment_generation: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Propose `SetTopicRetentionPolicy` (create with no
    /// --expected-generation; CAS-update with one).
    SetRetentionPolicy {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        /// Allow retention planning WITHOUT tier evidence for this topic.
        #[arg(long)]
        allow_unarchived_deletion: bool,
        #[arg(long)]
        expected_generation: Option<u64>,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Propose `PlanRetention` (Verified -> RetentionPlanned).
    PlanRetention {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        expected_generation: u64,
        #[arg(long)]
        fencing_epoch: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Propose `ConfirmRetentionExpired` after every local replica has been
    /// physically deleted (RetentionPlanned -> RetentionExpired).
    ConfirmRetentionExpired {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        expected_generation: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Deprecated: propose `CancelRetention`. The state machine always
    /// rejects it (fails closed) since #184; retained only for wire
    /// compatibility.
    CancelRetention {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        expected_generation: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
}

#[derive(Args, Debug)]
pub struct MetaCommonArgs {
    /// Path to a meta admin client YAML (endpoint + PEM paths).
    #[arg(long)]
    pub config: PathBuf,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum AdminTransport {
    #[default]
    Tls,
    Plaintext,
}

#[derive(Debug, Deserialize)]
struct MetaAdminConfig {
    /// `host:port` of the admin mTLS listener to ask FIRST.
    endpoint: String,
    /// rustls server name (usually matches a SAN on the server cert).
    #[serde(default = "default_server_name")]
    server_name: String,
    /// How the endpoint is dialed (#294): `tls` (the default) or `plaintext`,
    /// matching the metadata node's `admin_transport`. Under plaintext the
    /// PEM paths are not needed and no identity is presented.
    #[serde(default)]
    transport: AdminTransport,
    #[serde(default)]
    ca_cert: Option<PathBuf>,
    #[serde(default)]
    client_cert: Option<PathBuf>,
    #[serde(default)]
    client_key: Option<PathBuf>,
    /// Every other metadata node this command may be redirected to (#292).
    ///
    /// Reads and writes on this plane must reach the RAFT LEADER, and a
    /// non-leader answers with a redirect naming who to ask. With a single
    /// endpoint there is nowhere to go, so `vtopctl meta create-topic` against
    /// whichever node an operator happened to name fails outright roughly two
    /// times in three — and which node leads depends on an election, so it is
    /// not even stable between invocations.
    ///
    /// Optional and empty by default: a single-node group's only node is
    /// always its leader, so every existing config keeps working untouched.
    #[serde(default)]
    peers: Vec<MetaAdminPeer>,
}

/// One more metadata node `vtopctl` may follow a redirect to.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetaAdminPeer {
    /// The metadata node id, as Raft knows it. Required, because a redirect
    /// names an id: without it the client can only rotate through peers
    /// hopefully rather than going straight to the leader it was told about.
    node_id: u64,
    endpoint: String,
    /// Empty uses the top-level `server_name`, which is right under a shared
    /// SAN and wrong with per-node certificates — so it is per peer here.
    #[serde(default)]
    server_name: String,
    /// This peer's admin transport (#294), when it differs from the config's:
    /// a metadata tier migrated one node at a time is mixed for the duration,
    /// and a redirect may lead to either side. Unset inherits `transport`.
    #[serde(default)]
    transport: Option<AdminTransport>,
}

/// Whether a candidate is dialed plaintext: its own transport, else the
/// config's.
fn admin_dial_plaintext(peer: Option<&AdminTransport>, default: &AdminTransport) -> bool {
    peer.unwrap_or(default) == &AdminTransport::Plaintext
}

/// Whether PEM material is needed at all: when the config's transport is TLS,
/// or any peer's is (review) — the leader may be on either side of a rolling
/// migration.
fn admin_needs_tls<'a>(
    default: &AdminTransport,
    peers: impl Iterator<Item = Option<&'a AdminTransport>>,
) -> bool {
    let mut peers = peers;
    default == &AdminTransport::Tls || peers.any(|peer| peer == Some(&AdminTransport::Tls))
}

fn default_server_name() -> String {
    "localhost".to_owned()
}

/// Parse a 64-hex-character BLAKE3 digest / content root into raw bytes.
pub(crate) fn parse_hash32(value: &str) -> Result<[u8; 32], String> {
    blake3::Hash::from_hex(value)
        .map(|hash| *hash.as_bytes())
        .map_err(|_| format!("{value:?} is not a 64-hex-character digest"))
}

fn parse_node_state(value: &str) -> Result<NodeState, String> {
    match value {
        "active" => Ok(NodeState::Active),
        "draining" => Ok(NodeState::Draining),
        "dead" => Ok(NodeState::Dead),
        other => Err(format!(
            "unknown node state {other:?}; expected active|draining|dead"
        )),
    }
}

fn load_admin_config(path: &Path) -> Result<MetaAdminConfig, String> {
    let text =
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    serde_yaml::from_str(&text).map_err(|error| format!("parse {}: {error}", path.display()))
}

/// Report that the configured endpoint was not the leader.
///
/// Called from EVERY arm that builds a client, not just the ones that obviously
/// write. `init`, `add-learner`, `change-membership` and the lease read all go
/// through the same dispatch loop and can all be redirected, so leaving them out
/// made the diagnostic quietly untrue for exactly the commands an operator runs
/// while something is already going wrong.
///
/// To stderr, so it never contaminates `--json` output that a script parses.
/// Worth saying at all because it is actionable: an operator pointing at a node
/// that is never the leader is paying an extra round trip on every command, and
/// nothing else would tell them.
fn note_redirects(client: &AdminClient) {
    let followed = client.redirects_followed();
    if followed > 0 {
        eprintln!(
            "note: followed {followed} leader redirect(s); the configured endpoint is not the \
             metadata leader"
        );
    }
}

fn connect(config: &MetaAdminConfig) -> Result<AdminClient, String> {
    // Dialed the way the metadata node listens (#294): under TLS with the
    // operator's identity, or plaintext with none — the node's own choice,
    // matched here rather than guessed.
    let needs_tls = admin_needs_tls(
        &config.transport,
        config.peers.iter().map(|peer| peer.transport.as_ref()),
    );
    let material = if !needs_tls {
        None
    } else {
        let (Some(cert), Some(key), Some(ca)) =
            (&config.client_cert, &config.client_key, &config.ca_cert)
        else {
            return Err(
                "TLS material is needed — the admin config is `transport: tls` (the default), \
                 or a peer is — but ca_cert, client_cert and client_key are not all set: give \
                 the PEM paths, or set `transport: plaintext` on the config and change any peer \
                 with `transport: tls` to `transport: plaintext`"
                    .to_owned(),
            );
        };
        Some(TlsMaterial::from_pem_files(cert, key, ca).map_err(|error| error.to_string())?)
    };
    let plaintext = admin_dial_plaintext(None, &config.transport);
    // The configured endpoint first — it is what the operator named, and under
    // co-location it is usually the closest node — then everywhere a redirect
    // could point.
    let mut candidates = vec![AdminCandidate {
        // No id for the primary: it is tried first regardless, and a redirect
        // naming it matches whichever peer entry covers the same node.
        node_id: None,
        endpoint: resolve_endpoint(&config.endpoint).map_err(|error| error.to_string())?,
        host: config
            .endpoint
            .parse::<std::net::SocketAddr>()
            .err()
            .map(|_| config.endpoint.clone()),
        server_name: config.server_name.clone(),
        plaintext,
    }];
    for peer in &config.peers {
        candidates.push(AdminCandidate {
            node_id: Some(MetaNodeId(peer.node_id)),
            endpoint: resolve_endpoint(&peer.endpoint).map_err(|error| error.to_string())?,
            host: peer
                .endpoint
                .parse::<std::net::SocketAddr>()
                .err()
                .map(|_| peer.endpoint.clone()),
            server_name: if peer.server_name.is_empty() {
                config.server_name.clone()
            } else {
                peer.server_name.clone()
            },
            plaintext: admin_dial_plaintext(peer.transport.as_ref(), &config.transport),
        });
    }
    match material {
        Some(material) => AdminClient::with_candidates(material, candidates),
        None => AdminClient::plaintext(candidates),
    }
    .map_err(|error| error.to_string())
}

/// Dispatch `vtopctl meta` and return a process exit code.
pub async fn run(command: MetaCommand, json: bool) -> i32 {
    match run_inner(command, json).await {
        Ok(()) => 0,
        Err(message) => {
            eprintln!("error: {message}");
            1
        }
    }
}

/// Client-side refusals for the replacement flow.
///
/// Free functions rather than inline blocks so a test can reach them: the
/// dispatch arms they came from need a configured cluster to enter, which would
/// make the only coverage of these messages an end-to-end one. The checks are
/// the part most likely to rot — each duplicates a rule the state machine also
/// enforces, and a duplicated rule that drifts is worse than no rule at all,
/// because the two disagree about the same command.
fn check_failure_domain(failure_domain: &str) -> Result<(), String> {
    if failure_domain.trim().is_empty() {
        return Err(
            "--failure-domain is empty; an empty domain is what every node has BEFORE this \
             command runs, so setting it to empty would leave multi-replica placements refused \
             for the same reason they are refused now"
                .to_owned(),
        );
    }
    Ok(())
}

fn check_rebalance_endpoints(from: Uuid, to: Uuid) -> Result<(), String> {
    if from == to {
        return Err(format!(
            "--from-node-uuid and --to-node-uuid are both {from}; a rebalance moves a replica to \
             a DIFFERENT node, and moving it to itself would add nothing while locking the \
             placement behind an intent"
        ));
    }
    Ok(())
}

/// Checked HERE as well as in the state machine, because a refusal that arrives
/// after a round trip through consensus is a worse experience for the same
/// mistake — and the state machine's message cannot say which flag to change.
fn check_replica_set(replication_factor: u8, replica_nodes: &[Uuid]) -> Result<(), String> {
    if replica_nodes.len() != usize::from(replication_factor) {
        return Err(format!(
            "--replication-factor is {replication_factor} but {} --replica-node value(s) were \
             given; the declared durability target and the replicas that back it must agree, or \
             the placement promises something nobody committed to",
            replica_nodes.len()
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for node in replica_nodes {
        if !seen.insert(*node) {
            return Err(format!(
                "--replica-node {node} appears more than once; one node cannot be two replicas, \
                 and counting it twice would overstate the durability this placement records"
            ));
        }
    }
    Ok(())
}

fn check_replacement_proof(
    expected_length_bytes: u64,
    source: Uuid,
    destination: Uuid,
) -> Result<(), String> {
    if expected_length_bytes == 0 {
        return Err(
            "--expected-length-bytes is 0; a sealed segment always has bytes, so a proof \
             asserting an empty one is evidence of nothing and would be refused after a round \
             trip through consensus"
                .to_owned(),
        );
    }
    if source == destination {
        return Err(format!(
            "--source-node-uuid and --destination-node-uuid are both {source}; a replacement \
             proof asserts that the destination now holds what the source held, and a node \
             cannot be its own replacement"
        ));
    }
    Ok(())
}

async fn run_inner(command: MetaCommand, json: bool) -> Result<(), String> {
    match command {
        MetaCommand::Status { common } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let status = client.status().await.map_err(|error| error.to_string())?;
            note_redirects(&client);
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&status_json(&status))
                        .map_err(|error| error.to_string())?
                );
            } else {
                print_status(&status);
            }
            Ok(())
        }
        MetaCommand::Init { common, members } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let response = client
                .init(members)
                .await
                .map_err(|error| error.to_string())?;
            note_redirects(&client);
            print_membership_change(&response.membership, "initialized", json)
        }
        MetaCommand::AddLearner { common, node_id } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let response = client
                .add_learner(node_id)
                .await
                .map_err(|error| error.to_string())?;
            note_redirects(&client);
            print_membership_change(&response.membership, "learner added", json)
        }
        MetaCommand::ChangeMembership {
            common,
            voters,
            retain_removed_as_learners,
        } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let response = client
                .change_membership(voters, retain_removed_as_learners)
                .await
                .map_err(|error| error.to_string())?;
            note_redirects(&client);
            print_membership_change(&response.membership, "membership changed", json)
        }
        MetaCommand::Membership { common } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let status = client.status().await.map_err(|error| error.to_string())?;
            note_redirects(&client);
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&membership_json(&status.membership))
                        .map_err(|error| error.to_string())?
                );
            } else {
                println!("voters:");
                for MetaNodeId(id) in &status.membership.voters {
                    println!("  {id}");
                }
                if !status.membership.learners.is_empty() {
                    println!("learners:");
                    for (MetaNodeId(id), addr) in &status.membership.learners {
                        println!("  {id}  {addr}");
                    }
                }
                if let Some(outgoing) = &status.membership.joint_outgoing {
                    println!("joint outgoing voters:");
                    for MetaNodeId(id) in outgoing {
                        println!("  {id}");
                    }
                }
            }
            Ok(())
        }
        MetaCommand::CreateTopic {
            common,
            name,
            topic_uuid,
            root_range_uuid,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::CreateTopic {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                name,
                topic_uuid,
                root_range_uuid,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::RangeLease {
            common,
            topic_uuid,
            range_uuid,
        } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let view = client
                .read_range_lease(topic_uuid, range_uuid)
                .await
                .map_err(|error| error.to_string())?;
            note_redirects(&client);
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "found": view.found,
                        "range_generation": view.range_generation,
                        "fencing_epoch": view.fencing_epoch,
                        "read_at_applied_index": view.read_at_applied_index,
                        "lease": view.lease.map(|lease| serde_json::json!({
                            "holder_node_uuid": lease.holder_node_uuid,
                            "fencing_epoch": lease.fencing_epoch,
                            "expires_at_ms": lease.expires_at_ms,
                        })),
                    }))
                    .map_err(|error| error.to_string())?
                );
            } else if !view.found {
                println!("range not found");
            } else {
                match view.lease {
                    None => println!(
                        "range generation={} fencing_epoch={} lease=none",
                        view.range_generation, view.fencing_epoch
                    ),
                    Some(lease) => println!(
                        "range generation={} fencing_epoch={} holder={} lease_epoch={} expires_at_ms={}",
                        view.range_generation,
                        view.fencing_epoch,
                        lease.holder_node_uuid,
                        lease.fencing_epoch,
                        lease
                            .expires_at_ms
                            .map(|ms| ms.to_string())
                            .unwrap_or_else(|| "never".to_owned()),
                    ),
                }
            }
            Ok(())
        }
        MetaCommand::Transitions {
            common,
            topic_uuid,
            range_uuid,
            from_epoch,
            limit,
            mac_key_env,
            replicas,
            replica_timeout_ms,
        } => {
            let key = match mac_key_env.as_deref() {
                Some(name) => Some(mac_key_from_env(name)?),
                None => None,
            };
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            // The replicas FIRST (review): a grant that lands between the two
            // reads then lands after the vectors, so any epoch a replica holds
            // is one the chain, read afterwards, has seen granted — a stray is
            // a stray, never an artefact of timing.
            let vectors = match replicas.as_deref() {
                Some(path) => Some(
                    crate::node_tools::epoch_vectors(
                        path,
                        std::time::Duration::from_millis(replica_timeout_ms),
                        range_uuid,
                    )
                    .await?,
                ),
                None => None,
            };
            // The WHOLE chain, paged (review): the server clamps a read to
            // its own maximum, so one read of a long history is a window,
            // and an audit over a window is not an audit of the range.
            // `--limit` is the page size; the chain is read page after page
            // from the last epoch seen until a page comes back empty.
            // The range's CURRENT epoch, snapshotted BEFORE the pages are read
            // (review): the chain must reach at least this epoch; a grant that
            // lands while the pages are being read only makes it longer.
            let lease = client
                .read_range_lease(topic_uuid, range_uuid)
                .await
                .map_err(|error| error.to_string())?;
            let client_ref = &client;
            let read = read_chain_consistently(
                move |next_from, limit| async move {
                    client_ref
                        .read_range_transitions(topic_uuid, range_uuid, next_from, limit)
                        .await
                        .map_err(|error| error.to_string())
                },
                from_epoch,
                limit,
            )
            .await?;
            note_redirects(&client);
            let found = read.is_some() && lease.found;
            let (transitions, read_at_applied_index) = read.unwrap_or_default();
            if !found {
                if json {
                    println!("{}", serde_json::json!({ "found": false }));
                } else {
                    println!("range not found");
                }
                return Ok(());
            }
            let audit = audit_transitions(
                &transitions,
                key.as_ref(),
                topic_uuid,
                range_uuid,
                from_epoch,
                Some(lease.fencing_epoch),
            )?;
            // The replicas' own account (#240): each epoch vector held to the
            // chain the metadata holds.
            let cross = vectors
                .as_deref()
                .map(|vectors| cross_check_epoch_vectors(&transitions, from_epoch.max(1), vectors));
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "found": true,
                        "read_at_applied_index": read_at_applied_index,
                        "transitions": transitions.iter().zip(&audit.records).map(|(view, record)| transition_json(view, record)).collect::<Vec<_>>(),
                        "summary": {
                            "transitions": audit.records.len(),
                            "broken_links": audit.broken_links,
                            "vote_disagreements": audit.vote_disagreements,
                            "mac": { "verified": audit.verified, "unsigned": audit.unsigned, "unverified": audit.unverified, "mismatch": audit.mismatches },
                        },
                        "epoch_vectors": cross.as_ref().map(EpochCrossCheck::json),
                    }))
                    .map_err(|error| error.to_string())?
                );
            } else {
                for (view, record) in transitions.iter().zip(&audit.records) {
                    println!("{}", transition_line(view, record));
                }
                println!(
                    "{} transition(s) from epoch {from_epoch} (read at applied index {}); links: {} broken; votes: {} disagree; mac: {} verified, {} unsigned, {} unverified, {} MISMATCH",
                    audit.records.len(),
                    read_at_applied_index,
                    audit.broken_links,
                    audit.vote_disagreements,
                    audit.verified,
                    audit.unsigned,
                    audit.unverified,
                    audit.mismatches
                );
                if let Some(cross) = &cross {
                    for line in cross.lines() {
                        println!("{line}");
                    }
                }
            }
            let mut verdict = audit.verdict();
            if let Some(reason) = cross.as_ref().and_then(|cross| cross.verdict().err()) {
                verdict = Err(match verdict {
                    Ok(()) => reason,
                    Err(first) => format!("{first}; {reason}"),
                });
            }
            verdict
        }
        MetaCommand::RegisterNode {
            common,
            node_uuid,
            addr,
            expected_generation,
            issued_at_ms,
            request_id,
        } => {
            if addr.is_empty() || addr.len() > MAX_NODE_ADDR_BYTES {
                return Err(format!("addr must be 1..={MAX_NODE_ADDR_BYTES} bytes"));
            }
            let command = MetadataCommand::RegisterNode {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                node_uuid,
                addr,
                expected_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::SetNodeState {
            common,
            node_uuid,
            state,
            expected_generation,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::SetNodeState {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                node_uuid,
                state,
                expected_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::RegisterSealedSegment {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            segment_generation,
            base_offset,
            next_offset,
            content_root,
            sealed_by_epoch,
            expected_range_generation,
            issued_at_ms,
            request_id,
        } => {
            // `<`, matching the state machine — NOT `<=`. An empty sealed
            // segment is legal: `ActiveSegment::seal` produces one, so sealing
            // an untouched tail is a real artifact an operator can hold, and
            // refusing to register it here would be this CLI inventing a rule
            // the engine does not have. Only a REVERSED range is impossible.
            if next_offset < base_offset {
                return Err(format!(
                    "--next-offset {next_offset} is below --base-offset {base_offset}; a segment \
                     cannot end before it begins, so these offsets did not come from the segment \
                     being registered"
                ));
            }
            let command = MetadataCommand::RegisterSealedSegment {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                segment_generation,
                base_offset,
                next_offset,
                content_root,
                sealed_by_epoch,
                expected_range_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::MarkSegmentVerified {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            content_root,
            expected_generation,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::MarkSegmentVerified {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                content_root,
                expected_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::GetPlacement {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            for_replication_factor,
        } => {
            if for_replication_factor == Some(0) {
                return Err(
                    "--for-replication-factor is 0; zero replicas is not a placement, and the \
                     flag is how you ask for one — omit it entirely to skip the proposal"
                        .to_owned(),
                );
            }
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let placement = client
                .read_segment_placement(
                    topic_uuid,
                    range_uuid,
                    segment_uuid,
                    for_replication_factor.unwrap_or(0),
                )
                .await
                .map_err(|error| error.to_string())?;
            note_redirects(&client);
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&placement_json(&placement))
                        .map_err(|error| error.to_string())?
                );
            } else {
                print_placement(&placement);
            }
            Ok(())
        }
        MetaCommand::SetNodePlacementAttrs {
            common,
            node_uuid,
            failure_domain,
            placement_weight,
            expected_generation,
            issued_at_ms,
            request_id,
        } => {
            check_failure_domain(&failure_domain)?;
            let command = MetadataCommand::SetNodePlacementAttrs {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                node_uuid,
                failure_domain,
                placement_weight,
                expected_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::ProposeRebalance {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            from_node_uuid,
            to_node_uuid,
            expected_placement_generation,
            issued_at_ms,
            request_id,
        } => {
            check_rebalance_endpoints(from_node_uuid, to_node_uuid)?;
            let command = MetadataCommand::ProposeRebalance {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                from_node_uuid,
                to_node_uuid,
                expected_placement_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::CancelRebalance {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            expected_placement_generation,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::CancelRebalance {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_placement_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::CommitSegmentPlacement {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            replication_factor,
            replica_nodes,
            expected_segment_generation,
            expected_placement_generation,
            issued_at_ms,
            request_id,
        } => {
            check_replica_set(replication_factor, &replica_nodes)?;
            let command = MetadataCommand::CommitSegmentPlacement {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                replication_factor,
                replica_nodes,
                expected_segment_generation,
                expected_placement_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::CommitReplacementProof {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            expected_segment_generation,
            content_root,
            expected_length_bytes,
            source_node_uuid,
            destination_node_uuid,
            fencing_epoch,
            verifier_node_uuid,
            verified_term,
            issued_at_ms,
            request_id,
        } => {
            check_replacement_proof(
                expected_length_bytes,
                source_node_uuid,
                destination_node_uuid,
            )?;
            let command = MetadataCommand::CommitReplacementProof {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation,
                content_root,
                expected_length_bytes,
                source_node_uuid,
                destination_node_uuid,
                fencing_epoch,
                // One variant today. Named rather than defaulted so adding a
                // second forces every call site to say which it means.
                verification_method: vtop_meta::VerificationMethod::AuthenticatedContentRoot,
                verifier_node_uuid,
                verified_term,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::PlanReplicaRetirement {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            retiring_node_uuid,
            expected_segment_generation,
            fencing_epoch,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::PlanReplicaRetirement {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                retiring_node_uuid,
                expected_segment_generation,
                fencing_epoch,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::ConfirmReplicaRetired {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            retiring_node_uuid,
            expected_segment_generation,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::ConfirmReplicaRetired {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                retiring_node_uuid,
                expected_segment_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::CommitTierEvidence {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            expected_generation,
            content_root,
            byte_length,
            backend_id,
            object_uri,
            object_version_id,
            manifest_version_id,
            manifest_core_digest,
            verifier_node_uuid,
            fencing_epoch,
            verified_term,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::CommitTierEvidence {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation: expected_generation,
                content_root,
                byte_length,
                backend_id,
                object_uri,
                object_version_id,
                manifest_version_id,
                manifest_core_digest,
                verification_method: vtop_meta::VerificationMethod::AuthenticatedContentRoot,
                verifier_node_uuid,
                fencing_epoch,
                verified_term,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::SetRetentionPolicy {
            common,
            topic_uuid,
            allow_unarchived_deletion,
            expected_generation,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::SetTopicRetentionPolicy {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                unarchived_deletion_allowed: allow_unarchived_deletion,
                expected_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::PlanRetention {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            expected_generation,
            fencing_epoch,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::PlanRetention {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation: expected_generation,
                fencing_epoch,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::ConfirmRetentionExpired {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            expected_generation,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::ConfirmRetentionExpired {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation: expected_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::CancelRetention {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            expected_generation,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::CancelRetention {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation: expected_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
    }
}

pub(crate) async fn propose_and_print(
    config_path: &Path,
    command: MetadataCommand,
    json: bool,
) -> Result<(), String> {
    let config = load_admin_config(config_path)?;
    let client = connect(&config)?;
    let response = client
        .propose(command)
        .await
        .map_err(|error| error.to_string())?;
    note_redirects(&client);
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&propose_json(&response.log_id, &response.response))
                .map_err(|error| error.to_string())?
        );
    } else {
        println!(
            "committed term={} index={}",
            response.log_id.term, response.log_id.index
        );
        print_response(&response.response);
    }
    if matches!(response.response, MetadataResponse::Rejected(_)) {
        return Err("metadata command was rejected".to_owned());
    }
    Ok(())
}

fn status_json(status: &AdminStatusResponse) -> serde_json::Value {
    serde_json::json!({
        "node_id": status.node_id.0,
        "current_term": status.current_term,
        "vote": {
            "term": status.vote.term,
            "voted_for": status.vote.voted_for.map(|MetaNodeId(id)| id),
            "vote_committed": status.vote.vote_committed,
        },
        "current_leader": status.current_leader.map(|MetaNodeId(id)| id),
        "server_state": status.server_state,
        "last_applied": status.last_applied.map(|WireLogId { term, index }| {
            serde_json::json!({ "term": term, "index": index })
        }),
        "membership": membership_json(&status.membership),
    })
}

fn membership_json(membership: &vtop_meta::MetaMembership) -> serde_json::Value {
    let voters: Vec<_> = membership.voters.iter().map(|MetaNodeId(id)| *id).collect();
    let learners: Vec<_> = membership
        .learners
        .iter()
        .map(|(MetaNodeId(id), addr)| serde_json::json!({ "id": id, "addr": addr }))
        .collect();
    let joint_outgoing_voters = membership.joint_outgoing.as_ref().map(|outgoing| {
        outgoing
            .iter()
            .map(|MetaNodeId(id)| *id)
            .collect::<Vec<_>>()
    });
    serde_json::json!({
        "voters": voters,
        "learners": learners,
        "joint_outgoing_voters": joint_outgoing_voters,
    })
}

fn print_membership_change(
    membership: &vtop_meta::MetaMembership,
    action: &str,
    json: bool,
) -> Result<(), String> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "ok",
                "action": action,
                "membership": membership_json(membership),
            }))
            .map_err(|error| error.to_string())?
        );
    } else {
        let voters: Vec<_> = membership.voters.iter().map(ToString::to_string).collect();
        println!("{action}; voters: {}", voters.join(", "));
    }
    Ok(())
}

fn propose_json(log_id: &WireLogId, response: &MetadataResponse) -> serde_json::Value {
    serde_json::json!({
        "log_id": { "term": log_id.term, "index": log_id.index },
        "response": format!("{response:?}"),
        // THE GENERATION AS A FIELD, not only inside a Debug string. Almost
        // every command that follows one of these takes the resulting
        // generation as its compare-and-swap token, so a caller scripting the
        // flow had to either scrape `{response:?}` or hardcode a value it
        // assumed — and a hardcoded CAS token fails silently the first time
        // the record was touched by anything else.
        //
        // Null for responses that carry no generation, so a consumer can tell
        // "this command does not produce one" from "the field is missing".
        "generation": match response {
            MetadataResponse::Ack { generation }
            | MetadataResponse::GroupCreated { generation, .. } => {
                serde_json::json!(generation)
            }
            _ => serde_json::Value::Null,
        },
    })
}

fn placement_json(
    placement: &vtop_meta::transport::AdminReadSegmentPlacementResponse,
) -> serde_json::Value {
    serde_json::json!({
        "found": placement.found,
        "generation": placement.generation,
        "declared_replication_factor": placement.declared_replication_factor,
        "replica_nodes": placement
            .replica_nodes
            .iter()
            .map(|node| node.to_string())
            .collect::<Vec<_>>(),
        "rebalance_intent": placement.rebalance_intent.map(|intent| serde_json::json!({
            "from_node_uuid": intent.from_node_uuid.to_string(),
            "to_node_uuid": intent.to_node_uuid.to_string(),
            "placement_generation_at_proposal": intent.placement_generation_at_proposal,
        })),
        "segment": placement.segment.map(|segment| serde_json::json!({
            "segment_generation": segment.segment_generation,
            "base_offset": segment.base_offset,
            "next_offset": segment.next_offset,
            "content_root": hex_lower(&segment.content_root),
            "state": segment.state_name(),
            "state_tag": segment.state_tag,
            "sealed_by_epoch": segment.sealed_by_epoch,
        })),
        "proposal": match &placement.proposal {
            None => serde_json::Value::Null,
            Some(Ok(nodes)) => serde_json::json!({
                "replica_nodes": nodes.iter().map(|n| n.to_string()).collect::<Vec<_>>(),
            }),
            Some(Err(reason)) => serde_json::json!({ "error": reason }),
        },
        "read_at_applied_index": placement.read_at_applied_index,
    })
}

fn hex_lower(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn print_segment(segment: &Option<vtop_meta::transport::AdminSegmentView>) {
    let Some(segment) = segment else {
        println!("segment              : no segment record");
        return;
    };
    println!("segment generation   : {}", segment.segment_generation);
    println!(
        "segment state        : {}",
        // An unknown tag is reported as unknown rather than mapped to the
        // nearest name: a newer leader can commit a state this binary has
        // never heard of, and guessing would be a confident lie about where
        // the segment is in its lifecycle.
        match segment.state_name() {
            Some(name) => name.to_owned(),
            None => format!("unknown (tag {})", segment.state_tag),
        }
    );
    println!(
        "segment offsets      : [{}, {}) sealed by epoch {}",
        segment.base_offset, segment.next_offset, segment.sealed_by_epoch
    );
    println!(
        "segment content root : {}",
        hex_lower(&segment.content_root)
    );
}

fn print_proposal(proposal: &Option<Result<Vec<Uuid>, String>>) {
    print!("{}", proposal_text(proposal));
}

/// Rendered rather than printed straight out, so a test can assert the
/// first-placement branch actually emits it.
///
/// That branch returns early, and an edit adding this call to it silently
/// failed to apply once — leaving the ordered list reachable only under
/// `--json`, in the one workflow that has no prior placement to read and so
/// nothing else to go on.
fn proposal_text(proposal: &Option<Result<Vec<Uuid>, String>>) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    match proposal {
        None => {}
        Some(Ok(nodes)) => {
            let _ = writeln!(
                out,
                "proposed placement   : {} node(s), in this order",
                nodes.len()
            );
            for (position, node) in nodes.iter().enumerate() {
                let _ = writeln!(out, "  [{position}] {node}");
            }
            let _ = writeln!(
                out,
                "  Pass these to commit-segment-placement as --replica-node, IN THIS ORDER. \
                 The comparison is positional."
            );
        }
        Some(Err(reason)) => {
            // PRINTED VERBATIM. The remedy comes from metadata, which is the
            // only side that knows why the selection failed — whether the
            // factor was out of range, whether there were too few nodes, or
            // whether there were enough nodes in too few domains. This side
            // used to append one remedy for all three, so it confidently told
            // operators to set failure domains when the real problem was an
            // unsupported factor. Advice that cannot work, printed right after
            // the real reason, is worse than none: it gets tried first.
            let _ = writeln!(out, "proposed placement   : REFUSED — {reason}");
        }
    }
    out
}

fn print_placement(placement: &vtop_meta::transport::AdminReadSegmentPlacementResponse) {
    if !placement.found {
        println!("no placement committed for this segment");
        // The distinction an operator needs immediately: a first placement
        // takes NO expected generation, and passing 0 for one is a different
        // proposal that will be refused.
        println!(
            "  commit-segment-placement takes no --expected-placement-generation for a first \
             placement"
        );
        if let Some(intent) = placement.rebalance_intent {
            println!(
                "  WARNING: a rebalance intent exists ({} -> {}) with no placement; \
                 cancel-rebalance before proposing one",
                intent.from_node_uuid, intent.to_node_uuid
            );
        }
        // BOTH printed before returning. This branch is the whole reason the
        // proposal exists — a first placement has nothing to read back — so
        // returning without it left the one workflow that needs it able to see
        // the answer only in `--json`.
        print_segment(&placement.segment);
        print_proposal(&placement.proposal);
        println!(
            "  read at applied index: {}",
            placement.read_at_applied_index
        );
        return;
    }
    println!("placement generation : {}", placement.generation);
    println!(
        "replication factor   : {} (declared)",
        placement.declared_replication_factor
    );
    // NUMBERED, because the order is the value. `commit-segment-placement`
    // compares positionally, so an operator retyping this list out of order
    // gets a refusal that says nothing about ordering.
    println!(
        "replicas             : {} node(s), in placement order",
        placement.replica_nodes.len()
    );
    for (position, node) in placement.replica_nodes.iter().enumerate() {
        println!("  [{position}] {node}");
    }
    match placement.rebalance_intent {
        None => println!("rebalance            : none open"),
        Some(intent) => {
            println!(
                "rebalance            : OPEN, {} -> {} (proposed at placement generation {})",
                intent.from_node_uuid, intent.to_node_uuid, intent.placement_generation_at_proposal
            );
            println!(
                "  While this stands the segment refuses placement updates, further rebalance \
                 proposals, and retention planning. Finish the move, or cancel-rebalance to \
                 release it."
            );
        }
    }
    print_segment(&placement.segment);
    print_proposal(&placement.proposal);
    println!("read at applied index: {}", placement.read_at_applied_index);
}

fn print_status(status: &AdminStatusResponse) {
    println!("node_id:        {}", status.node_id);
    println!("term:           {}", status.current_term);
    println!("server_state:   {}", status.server_state);
    println!(
        "leader:         {}",
        status
            .current_leader
            .map(|id| id.to_string())
            .unwrap_or_else(|| "-".to_owned())
    );
    if let Some(applied) = status.last_applied {
        println!(
            "last_applied:   term={} index={}",
            applied.term, applied.index
        );
    } else {
        println!("last_applied:   -");
    }
    print!("voters:         ");
    let voters: Vec<_> = status
        .membership
        .voters
        .iter()
        .map(|id| id.to_string())
        .collect();
    println!("{}", voters.join(", "));
    if let Some(outgoing) = &status.membership.joint_outgoing {
        let outgoing: Vec<_> = outgoing.iter().map(ToString::to_string).collect();
        println!("joint outgoing: {}", outgoing.join(", "));
    }
}

fn print_response(response: &MetadataResponse) {
    match response {
        MetadataResponse::Ack { generation } => println!("ack generation={generation}"),
        MetadataResponse::TopicCreated {
            topic_uuid,
            topic_epoch,
            root_range_uuid,
        } => println!(
            "topic_created uuid={topic_uuid} epoch={topic_epoch} root_range={root_range_uuid}"
        ),
        MetadataResponse::TransitionRecorded { fencing_epoch } => {
            println!("transition recorded fencing_epoch={fencing_epoch}")
        }
        MetadataResponse::LeaseGranted { fencing_epoch } => {
            println!("lease_granted fencing_epoch={fencing_epoch}")
        }
        MetadataResponse::GroupCreated {
            group_uuid,
            generation,
        } => println!("group_created uuid={group_uuid} generation={generation}"),
        MetadataResponse::MemberJoined {
            member_generation,
            group_generation,
        } => println!(
            "member_joined member_generation={member_generation} group_generation={group_generation}"
        ),
        MetadataResponse::CursorCommitted {
            checkpoint_generation,
        } => println!("cursor_committed checkpoint_generation={checkpoint_generation}"),
        MetadataResponse::Rejected(error) => println!("rejected: {error}"),
    }
}

/// What the reader could establish about one transition statement (#240
/// item 5), each verdict its own word so a summary can count them apart.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransitionAudit {
    /// The epoch this record's `epoch_from` had to equal the previous
    /// record's `epoch_to`; the first record read has nothing before it.
    pub link_ok: bool,
    /// For an established promotion: the vote recomputed from the recorded
    /// quorum the way the holder counted it, beside what it recorded.
    pub vote: Option<VoteAudit>,
    pub mac: MacVerdict,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoteAudit {
    pub recorded: u32,
    pub recomputed: u32,
    pub required: u32,
    /// The holder answered its own fencing round. The promoter cannot
    /// establish without the candidate's answer (`LeaderBehind` otherwise),
    /// so a replicated quorum that omits the holder is evidence nothing
    /// real could have produced (review). A standalone promotion has no
    /// quorum and nothing to answer.
    pub holder_answered: bool,
    /// The recorded boundary is the one the quorum proves (review): the
    /// `required`-th highest answer, as the promoter takes it; `None` for
    /// a standalone promotion, which proves none.
    pub boundary_ok: bool,
    /// The recomputation agrees, the holder answered, no replica is
    /// counted twice, the majority required is at least a majority of the
    /// replicas that answered, the boundary is the quorum's, AND the
    /// recorded vote reached it.
    pub ok: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MacVerdict {
    /// The served MAC is the key's MAC over this record as a transition of
    /// the range asked for.
    Verified,
    /// A MAC was served and the key does not vouch for it — or vouches for
    /// it as another range's.
    Mismatch,
    /// No MAC was served: the serving node has no key.
    Unsigned,
    /// A MAC was served and no key was given to check it.
    Unverified,
}

impl MacVerdict {
    fn word(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Mismatch => "MISMATCH",
            Self::Unsigned => "unsigned",
            Self::Unverified => "unverified",
        }
    }
}

#[derive(Debug, Default)]
pub struct ChainAudit {
    pub records: Vec<TransitionAudit>,
    /// The epoch the range was at when the reader began, and whether the
    /// chain read reaches at least that far: `Some((current, false))` is a
    /// chain with its tail missing. A grant that lands mid-read only makes
    /// the chain longer, never shorter, so "at least" is the right bound.
    pub reaches_current: Option<(u64, bool)>,
    pub broken_links: usize,
    pub vote_disagreements: usize,
    pub verified: usize,
    pub unsigned: usize,
    pub unverified: usize,
    pub mismatches: usize,
}

/// One replica's epoch vector held to the chain (#240).
#[derive(Debug, PartialEq, Eq)]
pub enum EpochFinding {
    /// The replica wrote under an epoch no grant in the chain (from the
    /// epoch read) explains.
    StrayEpoch { epoch: u64, start_offset: u64 },
    /// The replica's first record under an epoch sits below the boundary
    /// that epoch's promotion proved committed: records the quorum proved
    /// were written under an earlier epoch have been written over.
    BelowBoundary {
        epoch: u64,
        start_offset: u64,
        boundary: u64,
    },
}

/// One replica's answer to the epoch-vector read: who it is, where, and the
/// vector or why it could not be asked.
pub type ReplicaEpochVector = (Uuid, String, Result<Vec<ReplicaEpochStart>, String>);

/// What a replica said about its history.
#[derive(Debug, PartialEq, Eq)]
pub enum EpochVector {
    Known(Vec<ReplicaEpochStart>),
    /// An empty answer is UNKNOWN by the handler's own contract (review): an
    /// older peer, or a journal it cannot read. Not "nothing to flag".
    Unknown,
    Unreachable(String),
}

#[derive(Debug)]
pub struct ReplicaEpochCheck {
    pub node_uuid: Uuid,
    pub addr: String,
    pub vector: EpochVector,
    pub findings: Vec<EpochFinding>,
}

#[derive(Debug)]
pub struct EpochCrossCheck {
    pub replicas: Vec<ReplicaEpochCheck>,
}

impl EpochCrossCheck {
    /// Replicas that could not be checked: unreachable, or with no history
    /// to check.
    pub fn unchecked(&self) -> usize {
        self.replicas
            .iter()
            .filter(|replica| !matches!(replica.vector, EpochVector::Known(_)))
            .count()
    }

    pub fn findings(&self) -> usize {
        self.replicas
            .iter()
            .map(|replica| replica.findings.len())
            .sum()
    }

    /// A replica that could not be checked fails the cross-check too: the
    /// operator asked for it, and "not checked" is not "checked".
    pub fn verdict(&self) -> Result<(), String> {
        let mut reasons = Vec::new();
        if self.unchecked() > 0 {
            reasons.push(format!(
                "{} replica(s) could not be checked (unreachable, or no epoch history)",
                self.unchecked()
            ));
        }
        if self.findings() > 0 {
            reasons.push(format!(
                "{} epoch-vector finding(s) against the chain",
                self.findings()
            ));
        }
        if reasons.is_empty() {
            Ok(())
        } else {
            Err(reasons.join("; "))
        }
    }

    pub fn lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        for replica in &self.replicas {
            match &replica.vector {
                EpochVector::Unreachable(error) => lines.push(format!(
                    "replica {} {}: UNREACHABLE: {error}",
                    replica.node_uuid, replica.addr
                )),
                EpochVector::Unknown => lines.push(format!(
                    "replica {} {}: UNKNOWN: no epoch history (an older peer, or a journal it \
                     cannot read)",
                    replica.node_uuid, replica.addr
                )),
                EpochVector::Known(vector) => {
                    let epochs: Vec<String> = vector
                        .iter()
                        .map(|start| format!("{}@{}", start.epoch, start.start_offset))
                        .collect();
                    lines.push(format!(
                        "replica {} {}: epochs [{}]{}",
                        replica.node_uuid,
                        replica.addr,
                        epochs.join(" "),
                        if replica.findings.is_empty() {
                            " OK"
                        } else {
                            ""
                        }
                    ));
                    for finding in &replica.findings {
                        lines.push(match finding {
                            EpochFinding::StrayEpoch {
                                epoch,
                                start_offset,
                            } => format!(
                                "  STRAY EPOCH {epoch} starting at {start_offset}: no grant in the \
                                 chain explains it"
                            ),
                            EpochFinding::BelowBoundary {
                                epoch,
                                start_offset,
                                boundary,
                            } => format!(
                                "  BELOW BOUNDARY: epoch {epoch} starts at {start_offset}, its \
                                 promotion proved {boundary} committed"
                            ),
                        });
                    }
                }
            }
        }
        lines.push(format!(
            "epoch vectors: {} replica(s), {} not checked, {} finding(s)",
            self.replicas.len(),
            self.unchecked(),
            self.findings()
        ));
        lines
    }

    pub fn json(&self) -> serde_json::Value {
        let replicas: Vec<serde_json::Value> = self
            .replicas
            .iter()
            .map(|replica| {
                let (epochs, error) = match &replica.vector {
                    EpochVector::Unknown => (
                        serde_json::Value::Null,
                        serde_json::Value::String("unknown: no epoch history".to_owned()),
                    ),
                    EpochVector::Known(vector) => (
                        serde_json::Value::Array(
                            vector
                                .iter()
                                .map(|start| {
                                    serde_json::json!({
                                        "epoch": start.epoch,
                                        "start_offset": start.start_offset,
                                    })
                                })
                                .collect(),
                        ),
                        serde_json::Value::Null,
                    ),
                    EpochVector::Unreachable(error) => (
                        serde_json::Value::Null,
                        serde_json::Value::String(error.clone()),
                    ),
                };
                let findings: Vec<serde_json::Value> = replica
                    .findings
                    .iter()
                    .map(|finding| match finding {
                        EpochFinding::StrayEpoch {
                            epoch,
                            start_offset,
                        } => serde_json::json!({
                            "kind": "stray_epoch", "epoch": epoch, "start_offset": start_offset,
                        }),
                        EpochFinding::BelowBoundary {
                            epoch,
                            start_offset,
                            boundary,
                        } => serde_json::json!({
                            "kind": "below_boundary", "epoch": epoch,
                            "start_offset": start_offset, "boundary": boundary,
                        }),
                    })
                    .collect();
                serde_json::json!({
                    "node_uuid": replica.node_uuid.to_string(),
                    "addr": replica.addr,
                    "epochs": epochs,
                    "error": error,
                    "findings": findings,
                })
            })
            .collect();
        serde_json::json!({
            "replicas": replicas,
            "unchecked": self.unchecked(),
            "findings": self.findings(),
        })
    }
}

/// Hold each replica's epoch vector to the chain (#240): every epoch a
/// replica wrote under must have been granted, and the HOLDER's first write
/// under its epoch may not sit below the boundary its promotion proved
/// committed — records the quorum proved were written under an earlier epoch
/// cannot have been written over under a later one. The holder only
/// (review): a follower behind at promotion adopts the new epoch at its own
/// tail, legitimately below the boundary, as the leader replicates what it
/// missed. A new leader's own epoch may start ABOVE its boundary (it keeps
/// the longer tail it was elected for); never below. Epochs below
/// `from_epoch` were not read, and are not judged.
pub fn cross_check_epoch_vectors(
    chain: &[AdminTransitionView],
    from_epoch: u64,
    vectors: &[ReplicaEpochVector],
) -> EpochCrossCheck {
    let granted: std::collections::BTreeMap<u64, (Uuid, Option<u64>)> = chain
        .iter()
        .map(|view| {
            let boundary = match &view.outcome {
                TransitionOutcome::Reported {
                    outcome:
                        PromotionOutcome::Established {
                            boundary_offset, ..
                        },
                    ..
                } => *boundary_offset,
                _ => None,
            };
            (view.epoch_to, (view.holder_to, boundary))
        })
        .collect();
    let replicas = vectors
        .iter()
        .map(|(node_uuid, addr, vector)| {
            let vector = match vector {
                Err(error) => EpochVector::Unreachable(error.clone()),
                Ok(vector) if vector.is_empty() => EpochVector::Unknown,
                Ok(vector) => EpochVector::Known(vector.clone()),
            };
            let findings = match &vector {
                EpochVector::Known(vector) => vector
                    .iter()
                    .filter(|start| start.epoch >= from_epoch)
                    .filter_map(|start| match granted.get(&start.epoch) {
                        None => Some(EpochFinding::StrayEpoch {
                            epoch: start.epoch,
                            start_offset: start.start_offset,
                        }),
                        Some((holder, Some(boundary)))
                            if holder == node_uuid && start.start_offset < *boundary =>
                        {
                            Some(EpochFinding::BelowBoundary {
                                epoch: start.epoch,
                                start_offset: start.start_offset,
                                boundary: *boundary,
                            })
                        }
                        Some(_) => None,
                    })
                    .collect(),
                EpochVector::Unknown | EpochVector::Unreachable(_) => Vec::new(),
            };
            ReplicaEpochCheck {
                node_uuid: *node_uuid,
                addr: addr.clone(),
                vector,
                findings,
            }
        })
        .collect();
    EpochCrossCheck { replicas }
}

impl ChainAudit {
    /// The command's exit: a broken link, a vote that does not recompute,
    /// or a MAC the key refuses each fails the audit by name.
    pub fn verdict(&self) -> Result<(), String> {
        let mut reasons = Vec::new();
        if let Some((current, false)) = self.reaches_current {
            reasons.push(format!(
                "the chain stops before the range's current epoch {current} (tail missing)"
            ));
        }
        if self.broken_links > 0 {
            reasons.push(format!("{} broken link(s)", self.broken_links));
        }
        if self.vote_disagreements > 0 {
            reasons.push(format!(
                "{} promotion(s) whose recorded vote does not recompute from its quorum",
                self.vote_disagreements
            ));
        }
        if self.mismatches > 0 {
            reasons.push(format!("{} MAC mismatch(es)", self.mismatches));
        }
        if reasons.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "transition chain audit failed: {}",
                reasons.join("; ")
            ))
        }
    }
}

/// Audit a chain as read, in order, against the identity the reader asked
/// for (#240 item 5). Nothing in the reply is trusted for the MAC check but
/// the record bytes the view rebuilds; the identity comes from the caller.
pub fn audit_transitions(
    views: &[AdminTransitionView],
    key: Option<&[u8; 32]>,
    topic_uuid: Uuid,
    range_uuid: Uuid,
    from_epoch: u64,
    current_epoch: Option<u64>,
) -> Result<ChainAudit, String> {
    // Read up from `from_epoch`, the chain must end at the epoch the range
    // holds now; a range still at its genesis has no transition to show.
    let reaches_current = current_epoch.map(|current| {
        let last = views
            .last()
            .map_or(from_epoch.max(1) - 1, |view| view.epoch_to);
        (current, last >= current)
    });
    let mut audit = ChainAudit {
        reaches_current,
        ..ChainAudit::default()
    };
    // Every epoch is minted as exactly the previous one plus one, by both
    // grant paths — so the chain from `from_epoch` upward must present
    // EVERY epoch in turn (review): a record that jumps, or a first record
    // above the epoch asked for, is a missing record, not a legitimate
    // silence. Continuity is on the epoch, not the holder: a released lease
    // is followed by a grant from nobody, legitimately.
    // Epoch 0 is the range's genesis, established by no transition: asked
    // from 0, the first record to expect is the one that made epoch 1.
    let mut expected_to = from_epoch.max(1);
    for view in views {
        let link_ok = view.epoch_to == expected_to && view.epoch_from + 1 == view.epoch_to;
        if !link_ok {
            audit.broken_links += 1;
        }
        expected_to = view.epoch_to + 1;
        let vote = match &view.outcome {
            TransitionOutcome::Reported {
                outcome:
                    PromotionOutcome::Established {
                        boundary_offset,
                        quorum,
                        votes,
                        required,
                        ..
                    },
                ..
            } => {
                // ONE answer per replica (review): the promoter built its
                // evidence from a map keyed by node, and neither the report
                // validation nor the wire enforce uniqueness, so a quorum
                // naming the holder twice must not count it twice.
                let mut answers = std::collections::BTreeMap::new();
                let mut duplicated = false;
                for answer in quorum {
                    if answers.insert(answer.node_uuid, answer.offset).is_some() {
                        duplicated = true;
                    }
                }
                // The holder's own offset, as the promoter used it: its
                // answer in the quorum. A standalone promotion (no quorum,
                // nothing required) has no answer to give; a replicated one
                // without the holder's answer is not evidence at all.
                let standalone = quorum.is_empty() && *required == 0;
                // A replicated promotion always records the majority it
                // needed (review): the replication factor is at least the
                // number of replicas that answered and the majority grows
                // with it, so `required` below a majority of the answers is
                // evidence no promotion could produce — and so is zero.
                let majority_recorded = standalone || (*required as usize) > answers.len() / 2;
                let holder_answer = answers.get(&view.holder_to).copied();
                let holder_answered = standalone || holder_answer.is_some();
                let candidate_offset = holder_answer.or(*boundary_offset).unwrap_or(0);
                let recomputed = answers
                    .values()
                    .filter(|offset| **offset <= candidate_offset)
                    .count() as u32;
                // The boundary the quorum proves, as the promoter takes it
                // (review): the `required`-th highest answer. Recorded
                // arbitrarily, it would pass a boundary no quorum reached.
                let proven_boundary = if standalone {
                    None
                } else {
                    let mut offsets: Vec<u64> = answers.values().copied().collect();
                    offsets.sort_unstable_by(|a, b| b.cmp(a));
                    offsets.get((*required as usize).saturating_sub(1)).copied()
                };
                let boundary_ok = *boundary_offset == proven_boundary;
                let ok = holder_answered
                    && majority_recorded
                    && boundary_ok
                    && !duplicated
                    && recomputed == *votes
                    && *votes >= *required;
                if !ok {
                    audit.vote_disagreements += 1;
                }
                Some(VoteAudit {
                    recorded: *votes,
                    recomputed,
                    required: *required,
                    holder_answered,
                    boundary_ok,
                    ok,
                })
            }
            _ => None,
        };
        let mac = match (key, view.mac.is_some()) {
            (_, false) => MacVerdict::Unsigned,
            (None, true) => MacVerdict::Unverified,
            (Some(key), true) => {
                if view
                    .verify_mac(key, topic_uuid, range_uuid)
                    .map_err(|error| format!("re-encode transition {}: {error}", view.epoch_to))?
                {
                    MacVerdict::Verified
                } else {
                    MacVerdict::Mismatch
                }
            }
        };
        match mac {
            MacVerdict::Verified => audit.verified += 1,
            MacVerdict::Mismatch => audit.mismatches += 1,
            MacVerdict::Unsigned => audit.unsigned += 1,
            MacVerdict::Unverified => audit.unverified += 1,
        }
        audit.records.push(TransitionAudit { link_ok, vote, mac });
    }
    Ok(audit)
}

fn grant_word(grant: GrantKind) -> &'static str {
    match grant {
        GrantKind::Election => "election",
        GrantKind::Administrative => "administrative",
    }
}

fn outcome_text(view: &AdminTransitionView, record: &TransitionAudit) -> String {
    match &view.outcome {
        TransitionOutcome::Pending => "pending".to_owned(),
        TransitionOutcome::Reported {
            outcome,
            reported_at_ms,
            reported_apply_index,
        } => match outcome {
            PromotionOutcome::Established {
                boundary_offset,
                sealed_prefix_end,
                quorum,
                ..
            } => {
                let vote = record
                    .vote
                    .as_ref()
                    .map(|vote| {
                        format!(
                            "votes={}/{} recomputed={} {}",
                            vote.recorded,
                            vote.required,
                            vote.recomputed,
                            if vote.ok {
                                "ok"
                            } else if !vote.holder_answered {
                                "HOLDER ABSENT FROM QUORUM"
                            } else if !vote.boundary_ok {
                                "BOUNDARY NOT THE QUORUM'S"
                            } else {
                                "VOTE MISMATCH"
                            }
                        )
                    })
                    .unwrap_or_default();
                let quorum = quorum
                    .iter()
                    .map(|answer| format!("{}:{}", answer.node_uuid, answer.offset))
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    "established boundary={} sealed_prefix_end={} {vote} quorum=[{quorum}] reported_at_ms={reported_at_ms} reported_apply_index={reported_apply_index}",
                    boundary_offset.map(|n| n.to_string()).unwrap_or_else(|| "none".to_owned()),
                    sealed_prefix_end.map(|n| n.to_string()).unwrap_or_else(|| "none".to_owned()),
                )
            }
            PromotionOutcome::Refused { reason } => format!(
                "refused reason={reason:?} reported_at_ms={reported_at_ms} reported_apply_index={reported_apply_index}"
            ),
        },
    }
}

/// Read a range's whole chain, page after page, and accept only pages that
/// describe ONE applied state (review): a chain read across several applied
/// indices may hold a record from before a transition next to one from after
/// it, and an audit of that is an audit of nothing. Pages carrying the same
/// index are one read; otherwise the chain is read again and accepted only
/// when two passes agree — after three disagreeing passes the chain is
/// moving faster than it can be read, and the audit says so instead of
/// presenting the last pass as a snapshot.
///
/// `Ok(None)` is a range that does not exist.
async fn read_chain_consistently<F, Fut>(
    mut fetch: F,
    from_epoch: u64,
    limit: u16,
) -> Result<Option<(Vec<AdminTransitionView>, u64)>, String>
where
    F: FnMut(u64, u16) -> Fut,
    Fut: std::future::Future<
        Output = Result<vtop_meta::transport::wire::AdminReadRangeTransitionsResponse, String>,
    >,
{
    const PASSES: usize = 3;
    let mut previous: Option<Vec<AdminTransitionView>> = None;
    let mut span = (0, 0);
    for _ in 0..PASSES {
        let mut chain = Vec::new();
        let mut next_from = from_epoch;
        let mut first_index = None;
        let mut last_index;
        loop {
            let page = fetch(next_from, limit).await?;
            if !page.found {
                return Ok(None);
            }
            first_index.get_or_insert(page.read_at_applied_index);
            last_index = page.read_at_applied_index;
            let Some(last) = page.transitions.last().map(|view| view.epoch_to) else {
                break;
            };
            chain.extend(page.transitions);
            if last == u64::MAX {
                break;
            }
            next_from = last + 1;
        }
        let first_index = first_index.unwrap_or(last_index);
        span = (first_index, last_index);
        if first_index == last_index || previous.as_ref() == Some(&chain) {
            return Ok(Some((chain, last_index)));
        }
        previous = Some(chain);
    }
    Err(format!(
        "the transition chain changed while it was being read: {PASSES} passes disagreed, the \
         last spanning applied index {}..{}; re-run the audit",
        span.0, span.1
    ))
}

fn transition_line(view: &AdminTransitionView, record: &TransitionAudit) -> String {
    format!(
        "epoch {}->{} holder {}->{} grant={} granted_at_ms={} apply_index={} link={} outcome={} mac={}",
        view.epoch_from,
        view.epoch_to,
        view.holder_from
            .map(|id| id.to_string())
            .unwrap_or_else(|| "none".to_owned()),
        view.holder_to,
        grant_word(view.grant),
        view.granted_at_ms,
        view.granted_apply_index,
        if record.link_ok { "ok" } else { "BROKEN" },
        outcome_text(view, record),
        record.mac.word(),
    )
}

fn transition_json(view: &AdminTransitionView, record: &TransitionAudit) -> serde_json::Value {
    let outcome = match &view.outcome {
        TransitionOutcome::Pending => serde_json::json!({ "kind": "pending" }),
        TransitionOutcome::Reported {
            outcome,
            reported_at_ms,
            reported_apply_index,
        } => match outcome {
            PromotionOutcome::Established {
                boundary_offset,
                sealed_prefix_end,
                quorum,
                votes,
                required,
            } => serde_json::json!({
                "kind": "established",
                "boundary_offset": boundary_offset,
                "sealed_prefix_end": sealed_prefix_end,
                "quorum": quorum.iter().map(|answer| serde_json::json!({ "node_uuid": answer.node_uuid, "offset": answer.offset })).collect::<Vec<_>>(),
                "votes": votes,
                "required": required,
                "reported_at_ms": reported_at_ms,
                "reported_apply_index": reported_apply_index,
            }),
            PromotionOutcome::Refused { reason } => serde_json::json!({
                "kind": "refused",
                "reason": format!("{reason:?}"),
                "reported_at_ms": reported_at_ms,
                "reported_apply_index": reported_apply_index,
            }),
        },
    };
    serde_json::json!({
        "epoch_from": view.epoch_from,
        "epoch_to": view.epoch_to,
        "holder_from": view.holder_from,
        "holder_to": view.holder_to,
        "grant": grant_word(view.grant),
        "granted_at_ms": view.granted_at_ms,
        "granted_apply_index": view.granted_apply_index,
        "link_ok": record.link_ok,
        "vote": record.vote.as_ref().map(|vote| serde_json::json!({
            "recorded": vote.recorded, "recomputed": vote.recomputed, "required": vote.required, "holder_answered": vote.holder_answered, "boundary_ok": vote.boundary_ok, "ok": vote.ok,
        })),
        "mac": record.mac.word(),
        "outcome": outcome,
    })
}

/// The 32-byte MAC key from an environment variable, as 64 hex characters
/// — the same shape the metadata node reads at startup.
fn mac_key_from_env(name: &str) -> Result<[u8; 32], String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("--mac-key-env must name a non-empty environment variable".to_owned());
    }
    let value = std::env::var(name).map_err(|_| {
        format!("MAC key environment variable {name} is missing or not valid Unicode")
    })?;
    let hex = value.trim();
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!(
            "{name} must hold exactly 64 hex characters (a 32-byte key), not {} character(s)",
            hex.len()
        ));
    }
    let mut key = [0_u8; 32];
    for (index, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let pair = std::str::from_utf8(chunk).map_err(|error| error.to_string())?;
        key[index] = u8::from_str_radix(pair, 16).map_err(|error| error.to_string())?;
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #240: a replica's epoch vector is held to the chain — an epoch no
    /// grant explains, or the holder's epoch starting below the boundary its
    /// promotion proved, is a finding; a follower behind at promotion is not;
    /// a replica that cannot be asked, or has no history, fails the check
    /// rather than passing it.
    #[test]
    fn epoch_vectors_are_held_to_the_chain() {
        let (a, b, c, d, e) = (
            Uuid::from_u128(1),
            Uuid::from_u128(2),
            Uuid::from_u128(3),
            Uuid::from_u128(4),
            Uuid::from_u128(5),
        );
        let established = |epoch_to: u64, holder: Uuid, boundary: u64| AdminTransitionView {
            epoch_from: epoch_to - 1,
            epoch_to,
            holder_from: None,
            holder_to: holder,
            grant: GrantKind::Election,
            granted_at_ms: 1_000 * epoch_to as i64,
            granted_apply_index: 10 * epoch_to,
            outcome: TransitionOutcome::Reported {
                outcome: PromotionOutcome::Established {
                    boundary_offset: Some(boundary),
                    sealed_prefix_end: None,
                    quorum: Vec::new(),
                    votes: 2,
                    required: 2,
                },
                reported_at_ms: 1_500 * epoch_to as i64,
                reported_apply_index: 10 * epoch_to + 2,
            },
            mac: None,
        };
        let chain = vec![
            established(1, a, 0),
            established(2, a, 120),
            established(3, b, 340),
        ];
        let start = |epoch, start_offset| ReplicaEpochStart {
            epoch,
            start_offset,
        };
        let vectors = vec![
            // The old holder, caught up under epoch 3 from its own tail: not
            // the holder of 3, so its boundary does not bind it.
            (
                a,
                "a:1".to_owned(),
                Ok(vec![start(1, 0), start(2, 120), start(3, 400)]),
            ),
            // The holder of epoch 3, written over below the boundary its
            // quorum proved, then under an epoch nobody granted.
            (
                b,
                "b:1".to_owned(),
                Ok(vec![start(1, 0), start(3, 300), start(4, 500)]),
            ),
            (c, "c:1".to_owned(), Err("connection refused".to_owned())),
            // A follower behind at promotion adopts epoch 3 at its own tail
            // (review): legitimate, and clean.
            (d, "d:1".to_owned(), Ok(vec![start(1, 0), start(3, 50)])),
            // No history at all is UNKNOWN, not clean (review).
            (e, "e:1".to_owned(), Ok(Vec::new())),
        ];
        let check = cross_check_epoch_vectors(&chain, 1, &vectors);
        assert!(
            check.replicas[0].findings.is_empty(),
            "{:?}",
            check.replicas[0].findings
        );
        assert_eq!(
            check.replicas[1].findings,
            vec![
                EpochFinding::BelowBoundary {
                    epoch: 3,
                    start_offset: 300,
                    boundary: 340,
                },
                EpochFinding::StrayEpoch {
                    epoch: 4,
                    start_offset: 500,
                },
            ]
        );
        assert!(
            check.replicas[3].findings.is_empty(),
            "a lagging follower is clean"
        );
        assert_eq!(check.replicas[4].vector, EpochVector::Unknown);
        assert_eq!(check.unchecked(), 2, "unreachable and unknown alike");
        assert_eq!(check.findings(), 2);
        let refusal = check.verdict().unwrap_err();
        assert!(
            refusal.contains("2 replica(s) could not be checked")
                && refusal.contains("2 epoch-vector finding(s)"),
            "{refusal}"
        );
        let text = check.lines().join("\n");
        assert!(
            text.contains("BELOW BOUNDARY: epoch 3 starts at 300, its promotion proved 340")
                && text.contains("STRAY EPOCH 4 starting at 500")
                && text.contains("UNREACHABLE: connection refused")
                && text.contains("UNKNOWN: no epoch history"),
            "{text}"
        );
        let json = check.json();
        assert_eq!(json["findings"], 2);
        assert_eq!(json["unchecked"], 2);
        assert_eq!(json["replicas"][1]["findings"][0]["kind"], "below_boundary");
        assert!(json["replicas"][2]["epochs"].is_null());
        assert!(json["replicas"][4]["epochs"].is_null());

        // Epochs below the epoch read were not read, and are not judged.
        let later = cross_check_epoch_vectors(&chain[1..], 2, &vectors[..1]);
        assert!(later.replicas[0].findings.is_empty() && later.verdict().is_ok());
        // Every replica answering with nothing to flag: clean.
        assert!(cross_check_epoch_vectors(&chain, 1, &vectors[..1])
            .verdict()
            .is_ok());
    }

    /// The chain is accepted only as ONE applied state (review): pages that
    /// straddle a transition are read again, two agreeing passes are the
    /// read, and three disagreeing passes are a refusal, not a snapshot.
    #[tokio::test]
    async fn a_chain_read_across_applied_states_is_accepted_only_when_two_passes_agree() {
        use std::cell::RefCell;
        use std::collections::VecDeque;
        use vtop_meta::transport::wire::AdminReadRangeTransitionsResponse as Page;
        use vtop_meta::PromotionRefusal;
        let holder = Uuid::from_u128(0xa);
        let view = |epoch_to: u64, outcome: TransitionOutcome| AdminTransitionView {
            epoch_from: epoch_to - 1,
            epoch_to,
            holder_from: None,
            holder_to: holder,
            grant: GrantKind::Election,
            granted_at_ms: 1_000 * epoch_to as i64,
            granted_apply_index: 10 * epoch_to,
            outcome,
            mac: None,
        };
        let page = |transitions: Vec<AdminTransitionView>, index: u64| Page {
            found: true,
            transitions,
            read_at_applied_index: index,
        };
        let reported = |at: i64| TransitionOutcome::Reported {
            outcome: PromotionOutcome::Refused {
                reason: PromotionRefusal::QuorumUnavailable,
            },
            reported_at_ms: at,
            reported_apply_index: 22,
        };
        // Pass 1 straddles a transition: epoch 2 is pending on its first
        // page and the index moves under its second. Pass 2 is one state.
        let script = RefCell::new(VecDeque::from(vec![
            page(
                vec![view(1, reported(1)), view(2, TransitionOutcome::Pending)],
                10,
            ),
            page(vec![view(3, TransitionOutcome::Pending)], 12),
            page(vec![], 12),
            page(vec![view(1, reported(1)), view(2, reported(2))], 13),
            page(vec![view(3, TransitionOutcome::Pending)], 13),
            page(vec![], 13),
        ]));
        let asked = RefCell::new(Vec::new());
        let fetch = |from: u64, limit: u16| {
            asked.borrow_mut().push((from, limit));
            std::future::ready(Ok(script.borrow_mut().pop_front().expect("scripted page")))
        };
        let (chain, index) = read_chain_consistently(fetch, 1, 2).await.unwrap().unwrap();
        assert_eq!(index, 13, "the agreeing pass's index");
        assert_eq!(chain.len(), 3);
        assert!(
            matches!(chain[1].outcome, TransitionOutcome::Reported { .. }),
            "the second pass's record, not the first's"
        );
        assert_eq!(
            *asked.borrow(),
            vec![(1, 2), (3, 2), (4, 2), (1, 2), (3, 2), (4, 2)],
            "paged from the last epoch seen, twice"
        );

        // Three passes that never agree: a refusal naming the span, not the
        // last pass dressed as a snapshot.
        let mut moving = VecDeque::new();
        for pass in 0..3_i64 {
            moving.push_back(page(vec![view(1, reported(pass))], 20 + pass as u64 * 2));
            moving.push_back(page(vec![], 21 + pass as u64 * 2));
        }
        let script = RefCell::new(moving);
        let fetch = |_: u64, _: u16| {
            std::future::ready(Ok(script.borrow_mut().pop_front().expect("scripted page")))
        };
        let refused = read_chain_consistently(fetch, 1, 2).await.unwrap_err();
        assert!(
            refused.contains("re-run the audit") && refused.contains("24..25"),
            "{refused}"
        );

        // A range that does not exist is `None`, on the first page.
        let fetch = |_: u64, _: u16| {
            std::future::ready(Ok(Page {
                found: false,
                transitions: Vec::new(),
                read_at_applied_index: 0,
            }))
        };
        assert!(read_chain_consistently(fetch, 1, 2)
            .await
            .unwrap()
            .is_none());
    }

    /// #294 (review): a redirect peer may sit on the other side of a rolling
    /// transport migration; each is dialed its own way, and material is kept
    /// while any needs it.
    #[test]
    fn admin_dial_follows_each_peer_and_material_is_kept_while_any_peer_needs_it() {
        let tls = AdminTransport::Tls;
        let plain = AdminTransport::Plaintext;
        assert!(admin_dial_plaintext(None, &plain));
        assert!(!admin_dial_plaintext(None, &tls));
        assert!(!admin_dial_plaintext(Some(&tls), &plain));
        assert!(admin_dial_plaintext(Some(&plain), &tls));
        assert!(!admin_needs_tls(&plain, [None, Some(&plain)].into_iter()));
        assert!(admin_needs_tls(&plain, [None, Some(&tls)].into_iter()));
        assert!(admin_needs_tls(&tls, [Some(&plain)].into_iter()));
    }

    #[test]
    fn parse_node_state_accepts_canonical_names() {
        assert_eq!(parse_node_state("active").unwrap(), NodeState::Active);
        assert_eq!(parse_node_state("draining").unwrap(), NodeState::Draining);
        assert!(parse_node_state("online").is_err());
    }

    #[test]
    fn meta_admin_config_deserializes() {
        let yaml = r#"
endpoint: "127.0.0.1:9701"
ca_cert: /tmp/ca.pem
client_cert: /tmp/client.pem
client_key: /tmp/client.key
"#;
        let config: MetaAdminConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.endpoint, "127.0.0.1:9701");
        assert_eq!(config.server_name, "localhost");
        assert_eq!(config.transport, AdminTransport::Tls);

        // A plaintext admin plane (#294) needs no PEM paths; a tls one
        // without them is refused at connect time, naming the knob.
        let plain: MetaAdminConfig = serde_yaml::from_str(
            r#"
endpoint: "127.0.0.1:9701"
transport: plaintext
"#,
        )
        .unwrap();
        assert_eq!(plain.transport, AdminTransport::Plaintext);
        assert!(plain.ca_cert.is_none());
        let bare: MetaAdminConfig = serde_yaml::from_str(r#"endpoint: "127.0.0.1:9701""#).unwrap();
        let refusal = match connect(&bare) {
            Err(refusal) => refusal,
            Ok(_) => panic!("tls without paths must be refused"),
        };
        assert!(refusal.contains("transport: plaintext"), "{refusal}");
    }

    /// A node UUID that differs per index, so a test that means "two nodes"
    /// cannot accidentally pass by using one.
    fn node(n: u8) -> Uuid {
        Uuid::from_bytes([n; 16])
    }

    #[test]
    fn a_placement_whose_replica_count_contradicts_its_factor_is_refused() {
        // The premise: the same list at the matching factor IS accepted, so the
        // rejection below is about the disagreement and not about the list.
        check_replica_set(2, &[node(1), node(2)]).expect("a matching set is the accepted case");

        let error = check_replica_set(3, &[node(1), node(2)])
            .expect_err("declaring RF 3 while naming two replicas must not reach consensus");
        assert!(
            error.contains("--replication-factor is 3") && error.contains("2 --replica-node"),
            "the message must name BOTH numbers so the operator knows which to change: {error}"
        );
    }

    #[test]
    fn a_node_listed_twice_cannot_stand_in_for_two_replicas() {
        let error = check_replica_set(2, &[node(1), node(1)])
            .expect_err("one node counted twice overstates durability");
        assert!(error.contains("appears more than once"), "{error}");
    }

    #[test]
    fn a_proof_is_refused_when_it_is_evidence_of_nothing() {
        // Zero bytes: nothing is being asserted.
        let error = check_replacement_proof(0, node(1), node(2))
            .expect_err("an empty segment is not evidence a replica holds anything");
        assert!(error.contains("--expected-length-bytes is 0"), "{error}");

        // Same node twice: the proof would say a replica replaced itself.
        //
        // Worth being precise about what the source MEANS here, because the
        // help used to say the wrong thing: it names the replica being
        // REPLACED — the state machine requires it to equal the open
        // rebalance's `from` — not the node the bytes were physically copied
        // from. Those differ whenever the replica being replaced is the dead
        // one, which is the ordinary case.
        let error = check_replacement_proof(4096, node(1), node(1))
            .expect_err("a node cannot be its own replacement");
        assert!(error.contains("cannot be its own replacement"), "{error}");

        check_replacement_proof(4096, node(1), node(2))
            .expect("a non-empty segment on a different node is the whole point of the command");
    }

    #[test]
    fn a_rebalance_that_moves_a_replica_to_itself_is_refused() {
        check_rebalance_endpoints(node(1), node(1)).expect_err("a move to itself moves nothing");
        check_rebalance_endpoints(node(1), node(2)).expect("distinct endpoints are a real move");
    }

    #[test]
    fn a_blank_failure_domain_is_refused_rather_than_stored() {
        // Whitespace matters here: "  " is not a domain, but it is not empty
        // either, and storing it would satisfy a naive distinctness check while
        // meaning nothing to an operator reading it back.
        for blank in ["", "   ", "\t"] {
            check_failure_domain(blank)
                .expect_err("a blank domain leaves RF > 1 refused for the same reason as before");
        }
        check_failure_domain("rack-a").expect("a real domain is the accepted case");
    }

    /// The four replacement-flow commands parse, and the arguments they need in
    /// order to be honest are REQUIRED rather than defaulted.
    ///
    /// Worth pinning: every one of these takes an expected-generation, and a
    /// default would turn a concurrency check into a formality that always
    /// passes — the operator would stop supplying the value they are supposed
    /// to have read.
    #[test]
    fn the_replacement_commands_require_the_generations_they_check_against() {
        use clap::Parser;

        #[derive(Parser)]
        struct Harness {
            #[command(subcommand)]
            command: MetaCommand,
        }

        let ok = Harness::try_parse_from([
            "vtopctl",
            "propose-rebalance",
            "--config",
            "/nonexistent/meta.yaml",
            "--topic-uuid",
            &node(1).to_string(),
            "--range-uuid",
            &node(2).to_string(),
            "--segment-uuid",
            &node(3).to_string(),
            "--from-node-uuid",
            &node(4).to_string(),
            "--to-node-uuid",
            &node(5).to_string(),
            "--expected-placement-generation",
            "7",
        ]);
        assert!(ok.is_ok(), "{:?}", ok.err().map(|error| error.to_string()));

        // Same command with the generation dropped: refused at parse time.
        let missing = Harness::try_parse_from([
            "vtopctl",
            "propose-rebalance",
            "--config",
            "/nonexistent/meta.yaml",
            "--topic-uuid",
            &node(1).to_string(),
            "--range-uuid",
            &node(2).to_string(),
            "--segment-uuid",
            &node(3).to_string(),
            "--from-node-uuid",
            &node(4).to_string(),
            "--to-node-uuid",
            &node(5).to_string(),
        ]);
        assert!(
            missing.is_err(),
            "expected-placement-generation must be supplied, not assumed"
        );

        // The cancellation path is what keeps a failed move from locking the
        // segment for good, so it must be reachable — and it takes the same
        // generation guard, since cancelling the WRONG open move is its own way
        // to lose a placement.
        let cancel = Harness::try_parse_from([
            "vtopctl",
            "cancel-rebalance",
            "--config",
            "/nonexistent/meta.yaml",
            "--topic-uuid",
            &node(1).to_string(),
            "--range-uuid",
            &node(2).to_string(),
            "--segment-uuid",
            &node(3).to_string(),
            "--expected-placement-generation",
            "9",
        ]);
        assert!(
            cancel.is_ok(),
            "{:?}",
            cancel.err().map(|error| error.to_string())
        );

        // The read that unblocks the rest. It takes no generation — it is what
        // an operator runs to LEARN one — so a required CAS flag here would
        // recreate the chicken-and-egg it exists to break.
        let get = Harness::try_parse_from([
            "vtopctl",
            "get-placement",
            "--config",
            "/nonexistent/meta.yaml",
            "--topic-uuid",
            &node(1).to_string(),
            "--range-uuid",
            &node(2).to_string(),
            "--segment-uuid",
            &node(3).to_string(),
        ]);
        assert!(
            get.is_ok(),
            "{:?}",
            get.err().map(|error| error.to_string())
        );

        let attrs = Harness::try_parse_from([
            "vtopctl",
            "set-node-placement-attrs",
            "--config",
            "/nonexistent/meta.yaml",
            "--node-uuid",
            &node(1).to_string(),
            "--failure-domain",
            "rack-a",
            "--placement-weight",
            "4",
            "--expected-generation",
            "3",
        ]);
        assert!(
            attrs.is_ok(),
            "{:?}",
            attrs.err().map(|error| error.to_string())
        );

        // Weight omitted: refused, because the command replaces domain AND
        // weight atomically. Defaulting it would make a domain correction
        // silently reset a node weighted above 1, changing placement as a side
        // effect of an edit that named only the domain.
        let no_weight = Harness::try_parse_from([
            "vtopctl",
            "set-node-placement-attrs",
            "--config",
            "/nonexistent/meta.yaml",
            "--node-uuid",
            &node(1).to_string(),
            "--failure-domain",
            "rack-a",
            "--expected-generation",
            "3",
        ]);
        assert!(
            no_weight.is_err(),
            "the weight must be stated, because this command overwrites it either way"
        );
    }

    /// The first-placement branch renders the proposal.
    ///
    /// Pinned because the human-readable path returns early and an edit to it
    /// silently did not apply, leaving the ordered list reachable only through
    /// `--json` — in the one workflow that has no prior placement to read and
    /// therefore nothing else to go on.
    #[test]
    fn the_proposal_is_rendered_in_order_with_its_positional_warning() {
        let text = proposal_text(&Some(Ok(vec![node(7), node(3), node(5)])));
        let positions: Vec<usize> = ["[0]", "[1]", "[2]"]
            .iter()
            .map(|marker| text.find(marker).expect("every replica must be numbered"))
            .collect();
        assert!(
            positions.windows(2).all(|pair| pair[0] < pair[1]),
            "the list must be rendered in placement order:\n{text}"
        );
        assert!(
            text.contains(&node(7).to_string())
                && text.find(&node(7).to_string()) < text.find(&node(3).to_string()),
            "the first proposed replica must print first:\n{text}"
        );
        assert!(
            text.contains("IN THIS ORDER"),
            "the order is compared positionally, so saying so is the point:\n{text}"
        );
    }

    /// A refusal is reported exactly as metadata gave it, remedy included.
    ///
    /// This side must not append guidance of its own. It cannot tell an
    /// out-of-range factor from too few nodes from too few domains — the error
    /// arrives as text — and it used to print the failure-domain remedy for
    /// all three, which is confidently wrong advice in two of them.
    #[test]
    fn a_refusal_is_printed_verbatim_without_invented_guidance() {
        let text = proposal_text(&Some(Err(
            "replication factor 9 must be 1..=7. Choose a replication factor within the \
             supported range."
                .to_owned(),
        )));
        assert!(text.contains("replication factor 9"), "{text}");
        assert!(
            !text.contains("set-node-placement-attrs"),
            "an out-of-range factor is not fixed by setting failure domains, and saying so \
             sends the operator to a command that cannot help:\n{text}"
        );

        // And when the remedy IS about domains, it still arrives — from the
        // side that knows.
        let domains = proposal_text(&Some(Err(
            "2 of 3 replicas placeable. Set one on each with `set-node-placement-attrs`."
                .to_owned(),
        )));
        assert!(domains.contains("set-node-placement-attrs"), "{domains}");
    }

    /// No request, no output — the proposal must not invent a placement for a
    /// caller who did not ask what one would be.
    #[test]
    fn no_proposal_renders_nothing() {
        assert!(proposal_text(&None).is_empty());
    }

    /// The audit trusts nothing in the reply but the record bytes (#240
    /// item 5): a chain signed for THIS range verifies; the same records
    /// presented as another range fail; a gap in the epochs is a broken
    /// link; a vote that does not recompute from its own quorum is named;
    /// and without a key a signed statement is unverified, never verified.
    #[test]
    fn a_transition_chain_is_audited_against_the_identity_asked_for() {
        use vtop_meta::{PromotionRefusal, QuorumAnswer};
        let topic = Uuid::from_u128(0x70);
        let range = Uuid::from_u128(0x71);
        let key = [0x11_u8; 32];
        let holder_a = Uuid::from_u128(0xa1);
        let holder_b = Uuid::from_u128(0xa2);
        let mut views = vec![
            AdminTransitionView {
                epoch_from: 0,
                epoch_to: 1,
                holder_from: None,
                holder_to: holder_a,
                grant: GrantKind::Election,
                granted_at_ms: 1_000,
                granted_apply_index: 10,
                outcome: TransitionOutcome::Reported {
                    outcome: PromotionOutcome::Established {
                        boundary_offset: Some(90),
                        sealed_prefix_end: None,
                        quorum: vec![
                            QuorumAnswer {
                                node_uuid: holder_a,
                                offset: 90,
                            },
                            QuorumAnswer {
                                node_uuid: holder_b,
                                offset: 80,
                            },
                            QuorumAnswer {
                                node_uuid: Uuid::from_u128(0xa3),
                                offset: 95,
                            },
                        ],
                        votes: 2,
                        required: 2,
                    },
                    reported_at_ms: 1_500,
                    reported_apply_index: 12,
                },
                mac: None,
            },
            AdminTransitionView {
                epoch_from: 1,
                epoch_to: 2,
                holder_from: Some(holder_a),
                holder_to: holder_b,
                grant: GrantKind::Election,
                granted_at_ms: 2_000,
                granted_apply_index: 20,
                outcome: TransitionOutcome::Reported {
                    outcome: PromotionOutcome::Refused {
                        reason: PromotionRefusal::QuorumUnavailable,
                    },
                    reported_at_ms: 2_500,
                    reported_apply_index: 22,
                },
                mac: None,
            },
            AdminTransitionView {
                epoch_from: 2,
                epoch_to: 3,
                holder_from: None,
                holder_to: holder_a,
                grant: GrantKind::Administrative,
                granted_at_ms: 3_000,
                granted_apply_index: 30,
                outcome: TransitionOutcome::Pending,
                mac: None,
            },
        ];
        for view in &mut views {
            view.mac = Some(view.record().mac(&key, topic, range).unwrap());
        }

        let audit = audit_transitions(&views, Some(&key), topic, range, 1, None).unwrap();
        assert!(audit.verdict().is_ok(), "{audit:?}");
        assert_eq!(audit.verified, 3);
        assert!(audit.records.iter().all(|record| record.link_ok));
        let vote = audit.records[0]
            .vote
            .as_ref()
            .expect("an established promotion is recomputed");
        assert_eq!(
            (vote.recorded, vote.recomputed, vote.required, vote.ok),
            (2, 2, 2, true)
        );
        assert!(
            audit_transitions(&views, Some(&key), topic, range, 0, None)
                .unwrap()
                .verdict()
                .is_ok(),
            "asked from the genesis epoch 0, the chain that made epoch 1 is intact"
        );
        assert!(audit.records[1].vote.is_none() && audit.records[2].vote.is_none());

        let relabelled =
            audit_transitions(&views, Some(&key), topic, Uuid::from_u128(0x72), 1, None).unwrap();
        assert_eq!(
            relabelled.mismatches, 3,
            "another range's chain is not this one's"
        );
        assert!(relabelled.verdict().unwrap_err().contains("MAC mismatch"));

        let unchecked = audit_transitions(&views, None, topic, range, 1, None).unwrap();
        assert_eq!(
            (unchecked.unverified, unchecked.verified),
            (3, 0),
            "no key, no verdict"
        );
        assert!(
            unchecked.verdict().is_ok(),
            "unverified is not a failure; it is an absence"
        );

        let mut gapped = views.clone();
        gapped[2].epoch_from = 5;
        gapped[2].mac = Some(gapped[2].record().mac(&key, topic, range).unwrap());
        let gap = audit_transitions(&gapped, Some(&key), topic, range, 1, None).unwrap();
        assert_eq!(gap.broken_links, 1);
        assert!(!gap.records[2].link_ok && gap.records[1].link_ok);
        assert!(gap.verdict().unwrap_err().contains("broken link"));

        // A record that jumps an epoch is a missing record, and so is a
        // first record above the epoch asked for (review).
        let mut jumped = views.clone();
        jumped[2].epoch_from = 3;
        jumped[2].epoch_to = 4;
        jumped[2].mac = Some(jumped[2].record().mac(&key, topic, range).unwrap());
        let jump = audit_transitions(&jumped, Some(&key), topic, range, 1, None).unwrap();
        assert_eq!(jump.broken_links, 1, "epoch 3 is missing between 2 and 4");
        let headless = audit_transitions(&views[1..], Some(&key), topic, range, 1, None).unwrap();
        assert!(
            !headless.records[0].link_ok && headless.records[1].link_ok,
            "asked from epoch 1, the chain must begin at epoch 1"
        );
        let mid = audit_transitions(&views[1..], Some(&key), topic, range, 2, None).unwrap();
        assert!(
            mid.verdict().is_ok(),
            "asked from epoch 2, it may begin there"
        );

        // The range's current epoch bounds the chain from above (review): a
        // chain that reaches it passes, one that stops short is missing its
        // tail, and a range at its genesis has nothing to show.
        assert!(
            audit_transitions(&views, Some(&key), topic, range, 1, Some(3))
                .unwrap()
                .verdict()
                .is_ok()
        );
        assert!(
            audit_transitions(&views[..2], Some(&key), topic, range, 1, Some(1))
                .unwrap()
                .verdict()
                .is_ok(),
            "a grant that landed after the snapshot only makes the chain longer"
        );
        let short = audit_transitions(&views[..2], Some(&key), topic, range, 1, Some(3)).unwrap();
        assert_eq!(short.reaches_current, Some((3, false)));
        assert!(short.verdict().unwrap_err().contains("tail missing"));
        assert!(audit_transitions(&[], Some(&key), topic, range, 1, Some(0))
            .unwrap()
            .verdict()
            .is_ok());
        assert!(audit_transitions(&[], Some(&key), topic, range, 1, Some(2))
            .unwrap()
            .verdict()
            .is_err());

        // A quorum naming one replica twice counts it once (review).
        let mut doubled = views.clone();
        if let TransitionOutcome::Reported {
            outcome: PromotionOutcome::Established { quorum, .. },
            ..
        } = &mut doubled[0].outcome
        {
            quorum.retain(|answer| answer.node_uuid == holder_a);
            let again = quorum[0];
            quorum.push(again);
        }
        doubled[0].mac = Some(doubled[0].record().mac(&key, topic, range).unwrap());
        let dup = audit_transitions(&doubled, Some(&key), topic, range, 1, None).unwrap();
        let vote = dup.records[0].vote.as_ref().unwrap();
        assert_eq!(
            (vote.recomputed, vote.ok),
            (1, false),
            "the holder twice is one replica"
        );

        // A replicated quorum that omits the holder is evidence nothing real
        // could have produced (review); a standalone promotion has none.
        let mut absent = views.clone();
        if let TransitionOutcome::Reported {
            outcome: PromotionOutcome::Established { quorum, .. },
            ..
        } = &mut absent[0].outcome
        {
            quorum.retain(|answer| answer.node_uuid != holder_a);
        }
        absent[0].mac = Some(absent[0].record().mac(&key, topic, range).unwrap());
        let gone = audit_transitions(&absent, Some(&key), topic, range, 1, None).unwrap();
        let vote = gone.records[0].vote.as_ref().unwrap();
        assert!(!vote.holder_answered && !vote.ok, "{vote:?}");
        assert!(transition_line(&absent[0], &gone.records[0]).contains("HOLDER ABSENT"));
        let mut standalone = views.clone();
        standalone[0].outcome = TransitionOutcome::Reported {
            outcome: PromotionOutcome::Established {
                boundary_offset: None,
                sealed_prefix_end: None,
                quorum: Vec::new(),
                votes: 0,
                required: 0,
            },
            reported_at_ms: 1_500,
            reported_apply_index: 12,
        };
        standalone[0].mac = Some(standalone[0].record().mac(&key, topic, range).unwrap());
        let alone = audit_transitions(&standalone, Some(&key), topic, range, 1, None).unwrap();
        assert!(
            alone.records[0].vote.as_ref().unwrap().ok,
            "nothing to answer, nothing missing"
        );

        // A replicated quorum claiming to have needed nobody is malformed
        // (review): the promoter always records the majority it needed.
        let mut needless = views.clone();
        if let TransitionOutcome::Reported {
            outcome:
                PromotionOutcome::Established {
                    required, votes, ..
                },
            ..
        } = &mut needless[0].outcome
        {
            *required = 0;
            *votes = 2;
        }
        needless[0].mac = Some(needless[0].record().mac(&key, topic, range).unwrap());
        let zero = audit_transitions(&needless, Some(&key), topic, range, 1, None).unwrap();
        assert!(
            !zero.records[0].vote.as_ref().unwrap().ok,
            "required = 0 with a quorum is no evidence"
        );

        // A majority smaller than a majority of the answers is impossible
        // (review): three answers need at least two.
        let mut small = views.clone();
        if let TransitionOutcome::Reported {
            outcome:
                PromotionOutcome::Established {
                    required, votes, ..
                },
            ..
        } = &mut small[0].outcome
        {
            *required = 1;
            *votes = 2;
        }
        small[0].mac = Some(small[0].record().mac(&key, topic, range).unwrap());
        let one = audit_transitions(&small, Some(&key), topic, range, 1, None).unwrap();
        assert!(
            !one.records[0].vote.as_ref().unwrap().ok,
            "required = 1 of three is no majority"
        );

        // The boundary must be the required-th highest answer (review): the
        // fixture's 90 is the second of {95, 90, 80}; a recorded 0 is not.
        let mut low = views.clone();
        if let TransitionOutcome::Reported {
            outcome:
                PromotionOutcome::Established {
                    boundary_offset, ..
                },
            ..
        } = &mut low[0].outcome
        {
            *boundary_offset = Some(0);
        }
        low[0].mac = Some(low[0].record().mac(&key, topic, range).unwrap());
        let wrong = audit_transitions(&low, Some(&key), topic, range, 1, None).unwrap();
        let vote = wrong.records[0].vote.as_ref().unwrap();
        assert!(!vote.boundary_ok && !vote.ok, "{vote:?}");
        assert!(transition_line(&low[0], &wrong.records[0]).contains("BOUNDARY NOT THE QUORUM"));

        let mut overstated = views.clone();
        if let TransitionOutcome::Reported {
            outcome: PromotionOutcome::Established { votes, .. },
            ..
        } = &mut overstated[0].outcome
        {
            *votes = 3;
        }
        overstated[0].mac = Some(overstated[0].record().mac(&key, topic, range).unwrap());
        let vote = audit_transitions(&overstated, Some(&key), topic, range, 1, None).unwrap();
        assert_eq!(
            vote.vote_disagreements, 1,
            "three votes are claimed; the quorum shows two at or below the holder"
        );
        assert!(vote.verdict().unwrap_err().contains("does not recompute"));

        let mut unsigned = views.clone();
        unsigned[1].mac = None;
        let some = audit_transitions(&unsigned, Some(&key), topic, range, 1, None).unwrap();
        assert_eq!((some.verified, some.unsigned), (2, 1));
        assert!(
            some.verdict().is_ok(),
            "an unsigned statement is not a forged one"
        );
        assert_eq!(
            transition_line(&unsigned[1], &some.records[1])
                .matches("mac=unsigned")
                .count(),
            1
        );
    }

    /// The key comes from the environment as 64 hex characters, or not at all.
    #[test]
    fn the_mac_key_is_read_from_the_environment_as_hex() {
        let name = "VTOP_TEST_TRANSITION_MAC_KEY";
        std::env::set_var(name, "0123456789abcdef".repeat(4));
        let key = mac_key_from_env(name).unwrap();
        assert_eq!(&key[..4], &[0x01, 0x23, 0x45, 0x67]);
        std::env::set_var(name, "abc");
        assert!(mac_key_from_env(name).unwrap_err().contains("64 hex"));
        std::env::remove_var(name);
        assert!(mac_key_from_env(name).unwrap_err().contains("missing"));
        assert!(mac_key_from_env(" ").unwrap_err().contains("non-empty"));
    }
}
