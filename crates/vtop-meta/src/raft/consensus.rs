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
use crate::raft::type_config::MetaRaftTypeConfig;
use crate::state::MetaValue;
use crate::transport::admin::AdminHandler;
use crate::transport::wire::{AdminLeaseView, AdminReadRangeLeaseResponse};
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
}

pub type ConsensusResult<T> = Result<T, ConsensusError>;

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
}

impl OpenraftConsensus {
    pub fn new(raft: MemRaft) -> Self {
        Self { raft, store: None }
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
            .map_err(|error| ConsensusError::Message(error.to_string()))?;
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
impl Consensus for OpenraftConsensus {
    async fn propose(&self, command: MetadataCommand) -> ConsensusResult<CommitReceipt> {
        let response = self
            .raft
            .client_write(command)
            .await
            .map_err(|error| ConsensusError::Message(error.to_string()))?;
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
            .map_err(|error| ConsensusError::Message(error.to_string()))?;
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

#[async_trait]
impl AdminHandler for OpenraftConsensus {
    async fn read_range_lease(
        &self,
        request: crate::transport::wire::AdminReadRangeLeaseRequest,
    ) -> TransportResult<AdminReadRangeLeaseResponse> {
        AdminReadRangeLease::read_range_lease(self, request.topic_uuid, request.range_uuid)
            .await
            .map_err(|error| TransportError::Protocol(error.to_string()))
    }

    async fn status(&self) -> TransportResult<AdminStatusResponse> {
        Consensus::status(self)
            .await
            .map_err(|error| TransportError::Protocol(error.to_string()))
    }

    async fn propose(&self, command: MetadataCommand) -> TransportResult<AdminProposeResponse> {
        let receipt = Consensus::propose(self, command)
            .await
            .map_err(|error| TransportError::Protocol(error.to_string()))?;
        Ok(AdminProposeResponse {
            log_id: receipt.log_id,
            response: receipt.response,
        })
    }

    async fn init(&self, members: Vec<u64>) -> TransportResult<AdminMembershipResponse> {
        let members: std::collections::BTreeSet<u64> = members.into_iter().collect();
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
        self.raft
            .add_learner(node_id, openraft::EmptyNode {}, true)
            .await
            .map_err(|error| TransportError::Protocol(error.to_string()))?;
        self.current_membership()
    }

    async fn change_membership(
        &self,
        voters: Vec<u64>,
        retain_removed_as_learners: bool,
    ) -> TransportResult<AdminMembershipResponse> {
        let voters: std::collections::BTreeSet<u64> = voters.into_iter().collect();
        self.raft
            .change_membership(voters, retain_removed_as_learners)
            .await
            .map_err(|error| TransportError::Protocol(error.to_string()))?;
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
