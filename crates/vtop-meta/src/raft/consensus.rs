//! VTOP-owned [`Consensus`] façade over openraft.
//!
//! Application code (admin transport, `vtopctl meta`, future broker fencing)
//! talks only to this trait. Openraft request/response types stay inside
//! `raft/`; status and propose results are VTOP wire types.

#![allow(clippy::result_large_err)]

use crate::command::{MetadataCommand, MetadataResponse};
use crate::keys::MetaKey;
use crate::keys::MetaNodeId;
use crate::raft::convert::{membership_to_meta, to_meta_index, vote_to_hard_state};
use crate::raft::observation::{RaftObservation, RaftServerState};
use crate::raft::store::MetaRaftStore;
use crate::raft::type_config::{MetaRaftTypeConfig, NodeId};
use crate::state::MetaValue;
use crate::transport::admin::AdminHandler;
use crate::transport::wire::{
    AdminGroupCursorView, AdminLeaseView, AdminReadGroupCursorResponse,
    AdminReadRangeLeaseResponse, AdminReadRangeTransitionsResponse,
    AdminReadSegmentPlacementResponse, AdminRebalanceIntentView, AdminSegmentView,
    AdminTransitionView, MAX_TRANSITIONS_PER_READ,
};
use crate::transport::wire::{
    AdminMembershipResponse, AdminProposeResponse, AdminStatusResponse, TransportError,
    TransportResult, WireLogId,
};
use async_trait::async_trait;
use openraft::Raft;
use openraft::ServerState;
use thiserror::Error;
use uuid::Uuid;

type MemRaft = Raft<MetaRaftTypeConfig>;

/// Receipt returned after a command is committed and applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitReceipt {
    pub log_id: WireLogId,
    pub response: MetadataResponse,
}

/// Linearizable read fence (stage-5 foundation: leader check + metrics cursor).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadFence {
    pub term: u64,
    pub last_applied: Option<WireLogId>,
}

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("{0}")]
    Message(String),
    /// This node is not the leader, so it cannot serve the request; ask the
    /// node named here instead.
    ///
    /// TYPED, rather than folded into [`ConsensusError::Message`] with every
    /// other failure. Reads and writes on this plane must reach the Raft
    /// leader, and a non-leader has always refused them — but it refused with
    /// openraft's Display text, so a caller could only recognise the condition
    /// by matching English. Nothing did, and the consequence was that a
    /// co-located deployment worked on exactly one node: the lease watcher on
    /// every other replica asked its own local metadata node forever, failed
    /// closed, and never became ready (#292).
    ///
    /// `leader` is an id and not an address on purpose — that is all Raft
    /// knows. Openraft's own node metadata here is `EmptyNode`, so resolving
    /// the id to an endpoint is the caller's job, from the peer list it was
    /// configured with.
    #[error("not the metadata leader{}", match .leader {
        Some(id) => format!("; ask node {id}"),
        None => "; no leader is known yet".to_owned(),
    })]
    NotLeader { leader: Option<MetaNodeId> },
}

pub type ConsensusResult<T> = Result<T, ConsensusError>;

/// The redirect a non-leader answers a WRITE with.
///
/// Every Raft call site used to be `error.to_string()`, which is why the
/// distinction was invisible: openraft carries the redirect as a structured
/// `ForwardToLeader` and that threw it away, leaving a caller nothing to act
/// on but prose.
///
/// Matched on the concrete variant rather than through openraft's generic
/// `forward_to_leader` helper, whose `TryAsRef` bound is a private trait — so
/// the generic form does not compile outside the crate. Two small functions
/// that name the variant are also plainer to read than a bound nobody can
/// satisfy.
pub(crate) fn classify_write_error(
    error: openraft::error::RaftError<
        NodeId,
        openraft::error::ClientWriteError<NodeId, openraft::EmptyNode>,
    >,
) -> ConsensusError {
    match &error {
        openraft::error::RaftError::APIError(
            openraft::error::ClientWriteError::ForwardToLeader(forward),
        ) => not_leader(forward),
        _ => ConsensusError::Message(error.to_string()),
    }
}

/// The same redirect, for a linearizable READ.
pub(crate) fn classify_read_error(
    error: openraft::error::RaftError<
        NodeId,
        openraft::error::CheckIsLeaderError<NodeId, openraft::EmptyNode>,
    >,
) -> ConsensusError {
    match &error {
        openraft::error::RaftError::APIError(
            openraft::error::CheckIsLeaderError::ForwardToLeader(forward),
        ) => not_leader(forward),
        _ => ConsensusError::Message(error.to_string()),
    }
}

/// The id, deliberately, and not `leader_node`: this type config uses
/// `EmptyNode`, so Raft holds no address for a peer and reporting one would be
/// an invention. Resolving the id to an endpoint belongs to the caller, which
/// has the configured peer list.
fn not_leader(
    forward: &openraft::error::ForwardToLeader<NodeId, openraft::EmptyNode>,
) -> ConsensusError {
    ConsensusError::NotLeader {
        leader: forward.leader_id.map(MetaNodeId),
    }
}

/// Narrow consensus interface from the native broker architecture.
#[async_trait]
pub trait Consensus: Send + Sync {
    async fn propose(&self, command: MetadataCommand) -> ConsensusResult<CommitReceipt>;
    async fn status(&self) -> ConsensusResult<AdminStatusResponse>;
    async fn read_index(&self) -> ConsensusResult<ReadFence>;
}

/// Openraft-backed [`Consensus`].
pub struct OpenraftConsensus {
    raft: MemRaft,
    /// Applied state, for linearizable reads (#223). Optional so the existing
    /// harnesses that construct a consensus façade from a bare Raft handle
    /// keep working; a read against one of those reports the store as
    /// unavailable rather than inventing an answer.
    store: Option<MetaRaftStore>,
    /// The key transition statements are signed with when served (#240 item
    /// 5). `None` serves them unsigned — stated on the wire as an absent
    /// MAC, never as a MAC of nothing. Installed after construction because
    /// the node resolves it from its environment once the store is open.
    transition_mac_key: std::sync::RwLock<Option<[u8; 32]>>,
}

impl OpenraftConsensus {
    pub fn new(raft: MemRaft) -> Self {
        Self {
            raft,
            store: None,
            transition_mac_key: std::sync::RwLock::new(None),
        }
    }

    /// Sign served transition statements with `key` from now on.
    pub fn set_transition_mac_key(&self, key: [u8; 32]) {
        *self
            .transition_mac_key
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(key);
    }

    /// Attach the applied state so this node can serve linearizable reads.
    pub fn with_store(mut self, store: MetaRaftStore) -> Self {
        self.store = Some(store);
        self
    }

    pub fn raft(&self) -> &MemRaft {
        &self.raft
    }

    /// Non-blocking snapshot of this node's Raft state for the operational
    /// surface (#224).
    ///
    /// Reads the latest published metrics without messaging the Raft core, so
    /// a `/metrics` scrape can never queue behind consensus work — least of all
    /// on the struggling node whose numbers are being asked for. Indices are
    /// translated to the 1-based VTOP index space, matching what
    /// `vtopctl meta status` prints; a metric and a CLI that disagree about
    /// what "index 7" means is a bug waiting for an incident.
    pub fn observe(&self) -> RaftObservation {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        RaftObservation {
            node_id: MetaNodeId(metrics.id),
            running: metrics.running_state.is_ok(),
            current_term: metrics.current_term,
            server_state: match metrics.state {
                ServerState::Learner => RaftServerState::Learner,
                ServerState::Follower => RaftServerState::Follower,
                ServerState::Candidate => RaftServerState::Candidate,
                ServerState::Leader => RaftServerState::Leader,
                ServerState::Shutdown => RaftServerState::Shutdown,
            },
            current_leader: metrics.current_leader.map(MetaNodeId),
            last_log_index: metrics.last_log_index.map(to_meta_index),
            last_applied_index: metrics.last_applied.map(|id| to_meta_index(id.index)),
            snapshot_index: metrics.snapshot.map(|id| to_meta_index(id.index)),
            purged_index: metrics.purged.map(|id| to_meta_index(id.index)),
            voters: membership.voter_ids().count(),
            learners: membership.learner_ids().count(),
            millis_since_quorum_ack: metrics.millis_since_quorum_ack,
            peer_matched_index: metrics
                .replication
                .as_ref()
                .map(|replication| {
                    replication
                        .iter()
                        .map(|(peer, matched)| {
                            (MetaNodeId(*peer), matched.map(|id| to_meta_index(id.index)))
                        })
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

#[async_trait]
impl AdminReadRangeLease for OpenraftConsensus {
    async fn read_range_lease(
        &self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
    ) -> ConsensusResult<AdminReadRangeLeaseResponse> {
        let Some(store) = self.store.as_ref() else {
            return Err(ConsensusError::Message(
                "this node was built without applied state and cannot serve reads".to_owned(),
            ));
        };
        // Fence FIRST. Reading applied state without establishing that this
        // node is still the leader would let a deposed node answer from its
        // own lagging copy — and a candidate acting on that would fence a
        // leader that is perfectly healthy.
        self.raft
            .ensure_linearizable()
            .await
            .map_err(classify_read_error)?;
        let key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        };
        Ok(store.with_storage(|storage| {
            let read_at_applied_index = storage.last_applied();
            match storage.state().record(&key) {
                Some(MetaValue::Range(range)) => AdminReadRangeLeaseResponse {
                    found: true,
                    range_generation: range.generation,
                    fencing_epoch: range.fencing_epoch,
                    lease: range.lease.as_ref().map(|lease| AdminLeaseView {
                        holder_node_uuid: lease.holder_node_uuid,
                        fencing_epoch: lease.fencing_epoch,
                        expires_at_ms: lease.expires_at_ms,
                    }),
                    read_at_applied_index,
                },
                // Absent and lease-less are different answers: one means the
                // range does not exist, the other means nobody leads it.
                _ => AdminReadRangeLeaseResponse {
                    found: false,
                    range_generation: 0,
                    fencing_epoch: 0,
                    lease: None,
                    read_at_applied_index,
                },
            }
        }))
    }
}

/// Linearizable transition-chain read (#240 item 5), its own trait for the
/// same reason the lease read is.
#[async_trait]
pub trait AdminReadRangeTransitions: Send + Sync {
    async fn read_range_transitions(
        &self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        from_epoch: u64,
        limit: u16,
    ) -> ConsensusResult<AdminReadRangeTransitionsResponse>;
}

#[async_trait]
impl AdminReadRangeTransitions for OpenraftConsensus {
    async fn read_range_transitions(
        &self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        from_epoch: u64,
        limit: u16,
    ) -> ConsensusResult<AdminReadRangeTransitionsResponse> {
        let Some(store) = self.store.as_ref() else {
            return Err(ConsensusError::Message(
                "this node was built without applied state and cannot serve reads".to_owned(),
            ));
        };
        // Fence FIRST, as every admin read does: a chain served from a
        // lagging copy could be missing its newest link, and "the record is
        // not there" is exactly the observation this evidence exists to make
        // meaningful.
        self.raft
            .ensure_linearizable()
            .await
            .map_err(classify_read_error)?;
        // The caller's maximum, capped at the page bound; zero is zero
        // (review) — a maximum is not a hint.
        let limit = usize::from(limit).min(MAX_TRANSITIONS_PER_READ);
        let key = *self
            .transition_mac_key
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        store.with_storage(|storage| {
            let read_at_applied_index = storage.last_applied();
            let state = storage.state();
            let found = matches!(
                state.record(&MetaKey::Range {
                    topic_uuid,
                    range_uuid,
                }),
                Some(MetaValue::Range(_))
            );
            let mut transitions = Vec::new();
            for record in state.range_transitions(topic_uuid, range_uuid, from_epoch, limit) {
                // Signed HERE, at the serving path, over the key the read
                // was for plus the canonical bytes the snapshot carries —
                // see RangeTransitionRecord::mac for why not in apply.
                let mac = match key {
                    Some(key) => {
                        Some(record.mac(&key, topic_uuid, range_uuid).map_err(|error| {
                            ConsensusError::Message(format!("sign transition: {error}"))
                        })?)
                    }
                    None => None,
                };
                let mut view = AdminTransitionView::from(record);
                view.mac = mac;
                transitions.push(view);
            }
            Ok(AdminReadRangeTransitionsResponse {
                found,
                transitions,
                read_at_applied_index,
            })
        })
    }
}

/// Linearizable read of a group's committed cursor on a range (#457 slice
/// 2b), its own trait for the same reason the lease and transition reads
/// are.
#[async_trait]
pub trait AdminReadGroupCursor: Send + Sync {
    async fn read_group_cursor(
        &self,
        group_uuid: Uuid,
        topic_uuid: Uuid,
        range_uuid: Uuid,
    ) -> ConsensusResult<AdminReadGroupCursorResponse>;
}

#[async_trait]
impl AdminReadGroupCursor for OpenraftConsensus {
    async fn read_group_cursor(
        &self,
        group_uuid: Uuid,
        topic_uuid: Uuid,
        range_uuid: Uuid,
    ) -> ConsensusResult<AdminReadGroupCursorResponse> {
        let Some(store) = self.store.as_ref() else {
            return Err(ConsensusError::Message(
                "this node was built without applied state and cannot serve reads".to_owned(),
            ));
        };
        // Fence FIRST, as every admin read does: a committed offset served
        // from a lagging copy could be behind what the group was told it
        // committed, and a consumer resuming there would replay.
        self.raft
            .ensure_linearizable()
            .await
            .map_err(classify_read_error)?;
        store.with_storage(|storage| {
            let read_at_applied_index = storage.last_applied();
            let state = storage.state();
            let group_found = matches!(
                state.record(&MetaKey::Group { group_uuid }),
                Some(MetaValue::Group(_))
            );
            let cursor = match state.record(&MetaKey::GroupCursor {
                group_uuid,
                topic_uuid,
                range_uuid,
            }) {
                Some(MetaValue::GroupCursor(record)) => Some(AdminGroupCursorView::from(record)),
                _ => None,
            };
            Ok(AdminReadGroupCursorResponse {
                group_found,
                cursor,
                read_at_applied_index,
            })
        })
    }
}

/// Linearizable range-lease read, kept as its own trait so the consensus
/// façade stays the narrow propose/status interface it was.
#[async_trait]
pub trait AdminReadRangeLease: Send + Sync {
    async fn read_range_lease(
        &self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
    ) -> ConsensusResult<AdminReadRangeLeaseResponse>;
}

#[async_trait]
impl AdminReadSegmentPlacement for OpenraftConsensus {
    async fn read_segment_placement(
        &self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        for_replication_factor: u8,
    ) -> ConsensusResult<AdminReadSegmentPlacementResponse> {
        let Some(store) = self.store.as_ref() else {
            return Err(ConsensusError::Message(
                "this node was built without applied state and cannot serve reads".to_owned(),
            ));
        };
        // Fence FIRST, exactly as the lease read does. A placement read that
        // can lag is worse than none: every command in the replacement flow is
        // a compare-and-swap against the generation this returns, so a stale
        // answer fails in a way indistinguishable from somebody else's
        // concurrent write — and the operator's correct response to those two
        // is opposite.
        self.raft
            .ensure_linearizable()
            .await
            .map_err(classify_read_error)?;
        let placement_key = MetaKey::SegmentPlacement {
            topic_uuid,
            range_uuid,
            segment_uuid,
        };
        let intent_key = MetaKey::SegmentRebalanceIntent {
            topic_uuid,
            range_uuid,
            segment_uuid,
        };
        // THE THIRD RECORD, read under the same fence as the other two. All
        // four replacement commands take `--expected-segment-generation` as
        // well as the placement generation, and returning only the placement
        // left the caller exactly where they started for the other half.
        let segment_key = MetaKey::Segment {
            topic_uuid,
            range_uuid,
            segment_uuid,
        };
        Ok(store.with_storage(|storage| {
            // BOTH under one fence. Read separately they could straddle an
            // apply, and the pairing is the whole point: a generation without
            // the intent that blocks it describes a segment that will reject
            // the very command the generation was fetched for.
            // COMPUTED HERE, under the same fence as the records. A proposal
            // derived from a different snapshot of the node set than the
            // generation it is paired with would be a placement the state
            // machine no longer agrees with — refused, and for a reason
            // nothing in the answer would explain.
            let proposal = (for_replication_factor > 0).then(|| {
                let candidates = storage.state().active_placement_candidates();
                crate::placement::select_replicas(
                    segment_uuid,
                    &candidates,
                    usize::from(for_replication_factor),
                    // The same rule `commit_segment_placement` applies, not a
                    // guess at it: distinctness is required above RF 1.
                    for_replication_factor > 1,
                )
                .map_err(|error| refusal_with_remedy(&error, &candidates, for_replication_factor))
            });
            placement_view(
                storage.state().record(&placement_key),
                storage.state().record(&intent_key),
                storage.state().record(&segment_key),
                proposal,
                storage.last_applied(),
            )
        }))
    }
}

/// The algorithm's refusal, plus the remedy for THIS refusal.
///
/// Written here rather than in the CLI because only this side knows why the
/// selection failed. The error crosses the wire as text, so a client can do no
/// better than print one remedy for every failure — and it did: it advised
/// setting failure domains even when the factor was out of range, or when
/// there were simply too few nodes. Advice that cannot work, printed
/// immediately after the real reason, is worse than no advice, because an
/// operator will try it before doubting it.
fn refusal_with_remedy(
    error: &crate::placement::PlacementError,
    candidates: &[crate::placement::PlacementCandidate],
    for_replication_factor: u8,
) -> String {
    use crate::placement::PlacementError;
    let remedy = match error {
        // Nothing about the cluster can make an out-of-range factor work.
        PlacementError::InvalidReplicationFactor(_) => {
            "Choose a replication factor within the supported range.".to_owned()
        }
        PlacementError::InsufficientEligibleNodes { requested, .. } => {
            let distinct: std::collections::BTreeSet<&str> = candidates
                .iter()
                .map(|candidate| candidate.failure_domain.as_str())
                .collect();
            // THE TWO CASES LOOK THE SAME in the error and want different
            // actions. Enough nodes but too few distinct domains is the one an
            // operator hits after building a cluster through the CLI, where
            // `register-node` leaves the domain empty — and no amount of
            // adding nodes fixes it.
            if candidates.len() >= *requested && distinct.len() < *requested {
                let blank = candidates
                    .iter()
                    .filter(|candidate| candidate.failure_domain.trim().is_empty())
                    .count();
                format!(
                    "There are {} eligible node(s) but only {} distinct failure domain(s), and a \
                     placement above RF 1 needs one per replica. {} node(s) have no domain set at \
                     all — `register-node` leaves it empty. Set one on each with \
                     `set-node-placement-attrs`, then ask again.",
                    candidates.len(),
                    distinct.len(),
                    blank
                )
            } else {
                format!(
                    "Only {} node(s) are eligible for placement — Active, with a placement weight \
                     above zero. Register or reactivate enough nodes to satisfy a factor of {}.",
                    candidates.len(),
                    for_replication_factor
                )
            }
        }
    };
    format!("{error}. {remedy}")
}

/// Shape two metadata records into the answer an operator acts on.
///
/// A free function so the mapping can be tested without standing up a Raft
/// cluster. It is small, but three of its decisions are load-bearing and none
/// is visible in the types: absent is distinct from empty, the intent is
/// reported even with no placement, and the replica order is passed through
/// untouched.
fn placement_view(
    placement: Option<&MetaValue>,
    intent: Option<&MetaValue>,
    segment: Option<&MetaValue>,
    proposal: Option<Result<Vec<Uuid>, String>>,
    read_at_applied_index: u64,
) -> AdminReadSegmentPlacementResponse {
    let segment = match segment {
        Some(MetaValue::Segment(segment)) => Some(AdminSegmentView {
            segment_generation: segment.segment_generation,
            base_offset: segment.base_offset,
            next_offset: segment.next_offset,
            content_root: segment.content_root,
            // The durable tags, not a parallel numbering — a second mapping
            // would be free to drift from the one on disk.
            state_tag: match segment.state {
                crate::state::SegmentState::SealedUnverified => 1,
                crate::state::SegmentState::Verified => 2,
                crate::state::SegmentState::Repairing => 3,
                crate::state::SegmentState::RetirePlanned => 4,
                crate::state::SegmentState::Retired => 5,
                crate::state::SegmentState::Quarantined => 6,
                crate::state::SegmentState::RetentionPlanned => 7,
                crate::state::SegmentState::RetentionExpired => 8,
            },
            sealed_by_epoch: segment.sealed_by_epoch,
        }),
        _ => None,
    };
    let rebalance_intent = match intent {
        Some(MetaValue::RebalanceIntent(intent)) => Some(AdminRebalanceIntentView {
            from_node_uuid: intent.from_node_uuid,
            to_node_uuid: intent.to_node_uuid,
            placement_generation_at_proposal: intent.placement_generation_at_proposal,
        }),
        _ => None,
    };
    match placement {
        Some(MetaValue::SegmentPlacement(placement)) => AdminReadSegmentPlacementResponse {
            found: true,
            generation: placement.generation,
            declared_replication_factor: placement.declared_replication_factor,
            // NOT sorted, NOT deduplicated. `commit_segment_placement` compares
            // proposals positionally against the rendezvous result, so any
            // tidying here would hand back a list that cannot be resubmitted.
            replica_nodes: placement.replica_nodes.clone(),
            rebalance_intent,
            segment,
            proposal,
            read_at_applied_index,
        },
        // Absent is not "committed but empty". A caller proposing a FIRST
        // placement passes no expected generation at all, and reporting 0 for
        // both cases would leave them unable to tell which they are looking at
        // — the two need different commands.
        _ => AdminReadSegmentPlacementResponse {
            found: false,
            generation: 0,
            declared_replication_factor: 0,
            replica_nodes: Vec::new(),
            // Reported even with no placement. An intent without one is a
            // segment that will refuse the first placement proposal, and an
            // operator told only "not found" would retry it forever.
            rebalance_intent,
            segment,
            proposal,
            read_at_applied_index,
        },
    }
}

/// Linearizable segment-placement read, kept as its own trait for the same
/// reason as the lease read: the consensus facade stays a narrow
/// propose/status interface.
#[async_trait]
pub trait AdminReadSegmentPlacement: Send + Sync {
    async fn read_segment_placement(
        &self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        for_replication_factor: u8,
    ) -> ConsensusResult<AdminReadSegmentPlacementResponse>;
}

#[async_trait]
impl Consensus for OpenraftConsensus {
    async fn propose(&self, command: MetadataCommand) -> ConsensusResult<CommitReceipt> {
        let response = self
            .raft
            .client_write(command)
            .await
            .map_err(classify_write_error)?;
        let log_id = response.log_id;
        let meta_index = to_meta_index(log_id.index);
        let bytes = response.data;
        let decoded = MetadataResponse::decode(&bytes)
            .map_err(|error| ConsensusError::Message(error.to_string()))?;
        Ok(CommitReceipt {
            log_id: WireLogId {
                term: log_id.leader_id.term,
                index: meta_index,
            },
            response: decoded,
        })
    }

    async fn status(&self) -> ConsensusResult<AdminStatusResponse> {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = membership_to_meta(metrics.membership_config.membership())
            .map_err(|error| ConsensusError::Message(error.to_string()))?;
        let last_applied = metrics.last_applied.map(|id| WireLogId {
            term: id.leader_id.term,
            index: to_meta_index(id.index),
        });
        Ok(AdminStatusResponse {
            node_id: MetaNodeId(metrics.id),
            current_term: metrics.current_term,
            vote: vote_to_hard_state(&metrics.vote),
            current_leader: metrics.current_leader.map(MetaNodeId),
            server_state: format!("{:?}", metrics.state),
            last_applied,
            membership,
        })
    }

    async fn read_index(&self) -> ConsensusResult<ReadFence> {
        self.raft
            .ensure_linearizable()
            .await
            .map_err(classify_read_error)?;
        let metrics = self.raft.metrics().borrow().clone();
        Ok(ReadFence {
            term: metrics.current_term,
            last_applied: metrics.last_applied.map(|id| WireLogId {
                term: id.leader_id.term,
                index: to_meta_index(id.index),
            }),
        })
    }
}

/// Carry [`ConsensusError::NotLeader`] across into the transport layer instead
/// of flattening it to prose.
///
/// This is the second place the redirect used to be lost. Classifying the
/// openraft error correctly is useless if the very next `map_err` turns it back
/// into a string, which is what `TransportError::Protocol(error.to_string())`
/// did at every one of these call sites.
fn to_transport_error(error: ConsensusError) -> TransportError {
    match error {
        ConsensusError::NotLeader { leader } => TransportError::NotLeader {
            message: ConsensusError::NotLeader { leader }.to_string(),
            leader,
        },
        other => TransportError::Protocol(other.to_string()),
    }
}

#[async_trait]
impl AdminHandler for OpenraftConsensus {
    async fn read_range_lease(
        &self,
        request: crate::transport::wire::AdminReadRangeLeaseRequest,
    ) -> TransportResult<AdminReadRangeLeaseResponse> {
        AdminReadRangeLease::read_range_lease(self, request.topic_uuid, request.range_uuid)
            .await
            .map_err(to_transport_error)
    }

    async fn read_range_transitions(
        &self,
        request: crate::transport::wire::AdminReadRangeTransitionsRequest,
    ) -> TransportResult<AdminReadRangeTransitionsResponse> {
        AdminReadRangeTransitions::read_range_transitions(
            self,
            request.topic_uuid,
            request.range_uuid,
            request.from_epoch,
            request.limit,
        )
        .await
        .map_err(to_transport_error)
    }

    async fn read_group_cursor(
        &self,
        request: crate::transport::wire::AdminReadGroupCursorRequest,
    ) -> TransportResult<AdminReadGroupCursorResponse> {
        AdminReadGroupCursor::read_group_cursor(
            self,
            request.group_uuid,
            request.topic_uuid,
            request.range_uuid,
        )
        .await
        .map_err(to_transport_error)
    }

    async fn read_segment_placement(
        &self,
        request: crate::transport::wire::AdminReadSegmentPlacementRequest,
    ) -> TransportResult<AdminReadSegmentPlacementResponse> {
        AdminReadSegmentPlacement::read_segment_placement(
            self,
            request.topic_uuid,
            request.range_uuid,
            request.segment_uuid,
            request.for_replication_factor,
        )
        .await
        .map_err(to_transport_error)
    }

    async fn status(&self) -> TransportResult<AdminStatusResponse> {
        Consensus::status(self).await.map_err(to_transport_error)
    }

    async fn propose(&self, command: MetadataCommand) -> TransportResult<AdminProposeResponse> {
        let receipt = Consensus::propose(self, command)
            .await
            .map_err(to_transport_error)?;
        Ok(AdminProposeResponse {
            log_id: receipt.log_id,
            response: receipt.response,
        })
    }

    async fn init(&self, members: Vec<u64>) -> TransportResult<AdminMembershipResponse> {
        let members: std::collections::BTreeSet<u64> = members.into_iter().collect();
        // NOT classified as a possible redirect, and that is correct rather
        // than an oversight: `InitializeError` has only `NotAllowed` and
        // `NotInMembers`. Bootstrap is valid solely on an uninitialised node,
        // so there is no leader for it to be forwarded to — a redirect here
        // would be an invented answer.
        self.raft
            .initialize(members)
            .await
            .map_err(|error| TransportError::Protocol(error.to_string()))?;
        // `initialize` returns once the membership is accepted by the engine,
        // but metrics are published to their watch channel asynchronously —
        // so reading them immediately can still observe the pre-init state, in
        // which a node has NO membership at all. Answering a successful init
        // with "voters: []" tells the operator their bootstrap did nothing.
        //
        // The race is wide open on a single-node group, where `initialize`
        // needs no peer round trip and returns almost instantly; it is why the
        // co-located scenario (#215), the only one that bootstraps one member,
        // was the one that failed.
        self.awaited_membership().await
    }

    async fn add_learner(&self, node_id: u64) -> TransportResult<AdminMembershipResponse> {
        // A membership change is a WRITE and redirects like any other. This
        // flattened to a protocol error until now, so `vtopctl meta add-learner`
        // against a follower failed outright while `propose` against the same
        // node was quietly redirected — the inconsistency being invisible
        // because both look like "the command failed" from the outside.
        self.raft
            .add_learner(node_id, openraft::EmptyNode {}, true)
            .await
            .map_err(|error| to_transport_error(classify_write_error(error)))?;
        self.current_membership()
    }

    async fn change_membership(
        &self,
        voters: Vec<u64>,
        retain_removed_as_learners: bool,
    ) -> TransportResult<AdminMembershipResponse> {
        let voters: std::collections::BTreeSet<u64> = voters.into_iter().collect();
        // Same as `add_learner`: a write, and a redirect is the answer a
        // follower owes the caller.
        self.raft
            .change_membership(voters, retain_removed_as_learners)
            .await
            .map_err(|error| to_transport_error(classify_write_error(error)))?;
        self.current_membership()
    }
}

impl OpenraftConsensus {
    fn current_membership(&self) -> TransportResult<AdminMembershipResponse> {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = membership_to_meta(metrics.membership_config.membership())
            .map_err(|error| TransportError::Protocol(error.to_string()))?;
        Ok(AdminMembershipResponse { membership })
    }

    /// [`Self::current_membership`], but waits for the metrics watch to publish
    /// a membership that has voters.
    ///
    /// Bounded, and on expiry it returns whatever it last saw rather than
    /// failing: the caller's operation already succeeded, so turning a slow
    /// metrics publish into an error would report a failure that did not
    /// happen. An empty answer after the wait is still truthful — it says the
    /// node has not published a membership yet.
    async fn awaited_membership(&self) -> TransportResult<AdminMembershipResponse> {
        let mut metrics = self.raft.metrics();
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_millis(MEMBERSHIP_PUBLISH_MS);
        loop {
            let current = self.current_membership()?;
            if !current.membership.voters.is_empty() {
                return Ok(current);
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(current);
            }
            match tokio::time::timeout(remaining, metrics.changed()).await {
                // A new value was published — re-read it.
                Ok(Ok(())) => continue,
                // Channel closed (the raft core is gone) or the wait expired.
                // Either way, report what we last saw rather than inventing an
                // error for an operation that already succeeded.
                Ok(Err(_)) | Err(_) => return Ok(current),
            }
        }
    }
}

/// How long an admin `init` waits for the metrics watch to publish the
/// membership it just established. Generous relative to a local watch send
/// (microseconds) and short relative to any admin RPC timeout.
const MEMBERSHIP_PUBLISH_MS: u64 = 2_000;

#[cfg(test)]
mod tests {
    use super::*;

    fn forward_to(node: u64) -> openraft::error::ForwardToLeader<NodeId, openraft::EmptyNode> {
        openraft::error::ForwardToLeader {
            leader_id: Some(node),
            leader_node: Some(openraft::EmptyNode {}),
        }
    }

    /// A write refused for want of leadership classifies as a REDIRECT, not as
    /// a generic protocol failure.
    ///
    /// This is the server half of #292, and the half a transport test cannot
    /// see: those drive a stub handler, so flattening the classification here
    /// leaves them passing while every real membership change against a
    /// follower fails. `add_learner` and `change_membership` both route through
    /// this function, and both flattened until now — so `meta add-learner`
    /// against a follower failed outright while `propose` against the same node
    /// was quietly redirected.
    #[test]
    fn a_write_refused_for_leadership_classifies_as_a_redirect() {
        let error = openraft::error::RaftError::APIError(
            openraft::error::ClientWriteError::ForwardToLeader(forward_to(7)),
        );
        match classify_write_error(error) {
            ConsensusError::NotLeader { leader } => assert_eq!(leader, Some(MetaNodeId(7))),
            other => panic!("expected a redirect, got {other:?}"),
        }
    }

    /// A linearizable READ refused for the same reason classifies the same way.
    #[test]
    fn a_read_refused_for_leadership_classifies_as_a_redirect() {
        let error = openraft::error::RaftError::APIError(
            openraft::error::CheckIsLeaderError::ForwardToLeader(forward_to(3)),
        );
        match classify_read_error(error) {
            ConsensusError::NotLeader { leader } => assert_eq!(leader, Some(MetaNodeId(3))),
            other => panic!("expected a redirect, got {other:?}"),
        }
    }

    /// Everything else stays a plain message.
    ///
    /// The classifier must not turn unrelated failures into redirects: a client
    /// told to ask someone else about a problem that is not about leadership
    /// would rotate through every candidate collecting the same error, and
    /// report the last one instead of the real first one.
    #[test]
    fn an_unrelated_failure_is_not_reported_as_a_redirect() {
        let error: openraft::error::RaftError<
            NodeId,
            openraft::error::ClientWriteError<NodeId, openraft::EmptyNode>,
        > = openraft::error::RaftError::APIError(
            openraft::error::ClientWriteError::ChangeMembershipError(
                openraft::error::ChangeMembershipError::EmptyMembership(
                    openraft::error::EmptyMembership {},
                ),
            ),
        );
        assert!(matches!(
            classify_write_error(error),
            ConsensusError::Message(_)
        ));
    }

    /// The redirect survives the hop into the transport layer.
    ///
    /// Classifying correctly and then rebuilding the error as a generic
    /// protocol failure one line later is exactly how this was lost the first
    /// time, so the conversion is pinned separately from the classification.
    #[test]
    fn the_redirect_survives_conversion_into_a_transport_error() {
        let converted = to_transport_error(ConsensusError::NotLeader {
            leader: Some(MetaNodeId(5)),
        });
        match converted {
            TransportError::NotLeader { leader, .. } => assert_eq!(leader, Some(MetaNodeId(5))),
            other => panic!("expected a transport-level redirect, got {other}"),
        }
    }

    /// A committed placement is reported with its order and its declared
    /// factor intact, and the factor is NOT inferred from the list length.
    ///
    /// Those two differ on purpose during a move — the list runs at RF + 1
    /// while a rebalance is open — so a view that derived one from the other
    /// would report the target as 3 mid-move and send an operator to
    /// commit-segment-placement with the wrong number.
    #[test]
    fn a_committed_placement_reports_its_order_and_its_declared_factor() {
        let placement = MetaValue::SegmentPlacement(crate::state::SegmentPlacementRecord {
            generation: 7,
            declared_replication_factor: 2,
            // Three nodes at a declared factor of two: a move is in flight.
            replica_nodes: vec![node(3), node(1), node(2)],
            committed_apply_index: 40,
        });
        let view = placement_view(Some(&placement), None, None, None, 99);

        assert!(view.found);
        assert_eq!(view.generation, 7);
        assert_eq!(
            view.declared_replication_factor, 2,
            "the declared target must survive a list that is temporarily longer than it"
        );
        assert_eq!(
            view.replica_nodes,
            vec![node(3), node(1), node(2)],
            "order is compared positionally by the state machine, so it must be returned as-is"
        );
        assert_eq!(view.read_at_applied_index, 99);
    }

    /// No placement is not the same answer as a placement at generation 0.
    #[test]
    fn an_absent_placement_is_distinguishable_from_one_at_generation_zero() {
        let view = placement_view(None, None, None, None, 12);
        assert!(
            !view.found,
            "a first placement takes no expected generation at all, so the caller has to be able \
             to tell this case from a committed one"
        );
        assert_eq!(view.generation, 0);
        assert!(view.replica_nodes.is_empty());
    }

    /// An open rebalance is reported even when no placement exists.
    ///
    /// That pairing is the reason the two records are read together. An intent
    /// standing over an uncommitted placement blocks the very proposal an
    /// operator would make next, and "not found" alone would send them round
    /// that loop indefinitely.
    #[test]
    fn an_open_rebalance_is_reported_even_with_no_placement() {
        let intent = MetaValue::RebalanceIntent(crate::state::RebalanceIntentRecord {
            from_node_uuid: node(1),
            to_node_uuid: node(4),
            proposed_at_apply_index: 30,
            placement_generation_at_proposal: 6,
        });
        let view = placement_view(None, Some(&intent), None, None, 31);

        assert!(!view.found);
        let reported = view
            .rebalance_intent
            .expect("the intent blocks the next proposal, so it must be reported regardless");
        assert_eq!(reported.from_node_uuid, node(1));
        assert_eq!(reported.to_node_uuid, node(4));
        assert_eq!(reported.placement_generation_at_proposal, 6);
    }

    /// The proposal is what makes a FIRST placement possible, so it is
    /// reported alongside "not found" rather than instead of it.
    ///
    /// Both halves matter and they say different things: `found: false` tells
    /// the operator to omit `--expected-placement-generation` entirely, and
    /// the proposal tells them which nodes to name and in what order. Either
    /// alone leaves the command unbuildable.
    #[test]
    fn a_proposal_accompanies_a_not_found_placement() {
        let view = placement_view(None, None, None, Some(Ok(vec![node(2), node(1)])), 5);
        assert!(!view.found);
        assert_eq!(
            view.proposal,
            Some(Ok(vec![node(2), node(1)])),
            "the order is the answer; a set would not be usable"
        );
    }

    /// A refusal from the algorithm is carried through rather than flattened
    /// into an absent proposal.
    ///
    /// "No proposal" and "a proposal is impossible because no node has a
    /// failure domain" want completely different actions, and the second is
    /// the one an operator hits after building a cluster through the CLI,
    /// where `register-node` leaves the domain empty.
    #[test]
    fn a_refused_proposal_carries_its_reason() {
        let view = placement_view(
            None,
            None,
            None,
            Some(Err(
                "only 1 distinct failure domain for 3 replicas".to_owned()
            )),
            5,
        );
        match view.proposal {
            Some(Err(reason)) => assert!(reason.contains("failure domain"), "{reason}"),
            other => panic!("the reason must survive, got {other:?}"),
        }
    }

    fn node(n: u8) -> Uuid {
        Uuid::from_bytes([n; 16])
    }
}
