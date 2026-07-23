//! VTOP-owned [`Consensus`] façade over openraft.
//!
//! Application code (admin transport, `vtopctl meta`, future broker fencing)
//! talks only to this trait. Openraft request/response types stay inside
//! `raft/`; status and propose results are VTOP wire types.

#![allow(clippy::result_large_err)]

use crate::command::{MetadataCommand, MetadataResponse};
use crate::keys::MetaNodeId;
use crate::raft::convert::{membership_to_meta, to_meta_index, vote_to_hard_state};
use crate::raft::type_config::MetaRaftTypeConfig;
use crate::transport::admin::AdminHandler;
use crate::transport::wire::{
    AdminProposeResponse, AdminStatusResponse, TransportError, TransportResult, WireLogId,
};
use async_trait::async_trait;
use openraft::Raft;
use thiserror::Error;

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
}

impl OpenraftConsensus {
    pub fn new(raft: MemRaft) -> Self {
        Self { raft }
    }

    pub fn raft(&self) -> &MemRaft {
        &self.raft
    }
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
}
