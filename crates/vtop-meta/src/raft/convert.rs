//! Field-by-field translation between openraft types and VTOP durable types.
//!
//! No serde of openraft values: every mapping is explicit so the on-disk
//! codecs stay independent of openraft's in-memory layout.

use crate::keys::MetaNodeId;
use crate::storage::hardstate::HardState;
use crate::storage::log::{MetaLogEntry, MetaLogPayload, MetaMembership};
use crate::MetaStoreError;
use openraft::{
    AnyError, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, StorageError,
    StorageIOError, Vote,
};
use std::collections::{BTreeMap, BTreeSet};

use super::type_config::{MetaRaftTypeConfig, NodeId};

/// Openraft index `i` is stored on disk as `i + 1`. See module docs on
/// [`crate::raft`].
pub(crate) fn to_meta_index(raft_index: u64) -> u64 {
    raft_index.saturating_add(1)
}

pub(crate) fn to_raft_index(meta_index: u64) -> Option<u64> {
    meta_index.checked_sub(1)
}

pub(crate) fn raft_log_id(term: u64, raft_index: u64) -> LogId<NodeId> {
    LogId::new(CommittedLeaderId::new(term, 0), raft_index)
}

pub(crate) fn sto_err_logs(error: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageIOError::write_logs(AnyError::error(format!("{error}"))).into()
}

pub(crate) fn sto_err_vote(error: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageIOError::write_vote(AnyError::error(format!("{error}"))).into()
}

pub(crate) fn sto_err_read_logs(error: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageIOError::read_logs(AnyError::error(format!("{error}"))).into()
}

pub(crate) fn sto_err_apply(
    log_id: LogId<NodeId>,
    error: impl std::fmt::Display,
) -> StorageError<NodeId> {
    StorageIOError::apply(log_id, AnyError::error(format!("{error}"))).into()
}

pub(crate) fn sto_err_snapshot(error: impl std::fmt::Display) -> StorageError<NodeId> {
    StorageIOError::write_snapshot(None, AnyError::error(format!("{error}"))).into()
}

pub(crate) fn map_store_err(error: MetaStoreError) -> StorageError<NodeId> {
    sto_err_logs(error)
}

/// `Vote` ↔ `HardState` under `single-term-leader`: term, voted_for, committed.
pub(crate) fn vote_to_hard_state(vote: &Vote<NodeId>) -> HardState {
    HardState {
        term: vote.leader_id().get_term(),
        voted_for: vote.leader_id().voted_for().map(MetaNodeId),
        vote_committed: vote.is_committed(),
    }
}

pub(crate) fn hard_state_to_vote(state: &HardState) -> Option<Vote<NodeId>> {
    // Fresh hard state (term 0, no vote) is openraft's "no vote persisted".
    if state.term == 0 && state.voted_for.is_none() && !state.vote_committed {
        return None;
    }
    let Some(MetaNodeId(node_id)) = state.voted_for else {
        // Term advanced but vote not yet cast: openraft still wants a Vote
        // with voted_for=None, which Vote::new cannot build — construct
        // directly.
        return Some(Vote {
            leader_id: openraft::LeaderId {
                term: state.term,
                voted_for: None,
            },
            committed: state.vote_committed,
        });
    };
    Some(if state.vote_committed {
        Vote::new_committed(state.term, node_id)
    } else {
        Vote::new(state.term, node_id)
    })
}

pub(crate) fn entry_to_meta(
    entry: &Entry<MetaRaftTypeConfig>,
) -> Result<MetaLogEntry, StorageError<NodeId>> {
    let payload = match &entry.payload {
        EntryPayload::Blank => MetaLogPayload::Blank,
        EntryPayload::Normal(command) => MetaLogPayload::Normal(command.clone()),
        EntryPayload::Membership(membership) => {
            MetaLogPayload::Membership(membership_to_meta(membership)?)
        }
    };
    Ok(MetaLogEntry {
        term: entry.log_id.leader_id.term,
        index: to_meta_index(entry.log_id.index),
        payload,
    })
}

pub(crate) fn meta_to_entry(
    entry: MetaLogEntry,
) -> Result<Entry<MetaRaftTypeConfig>, StorageError<NodeId>> {
    let raft_index = to_raft_index(entry.index).ok_or_else(|| {
        sto_err_read_logs(format!(
            "durable log index {} is below the openraft offset",
            entry.index
        ))
    })?;
    let payload = match entry.payload {
        MetaLogPayload::Blank => EntryPayload::Blank,
        MetaLogPayload::Normal(command) => EntryPayload::Normal(command),
        MetaLogPayload::Membership(membership) => {
            EntryPayload::Membership(meta_to_membership(&membership)?)
        }
    };
    Ok(Entry {
        log_id: raft_log_id(entry.term, raft_index),
        payload,
    })
}

/// Map openraft membership → MetaMembership.
///
/// Joint configs (len != 1) are rejected: the durable format is a single
/// voter set, and this PR never issues `change_membership` mid-flight.
pub(crate) fn membership_to_meta(
    membership: &Membership<NodeId, openraft::EmptyNode>,
) -> Result<MetaMembership, StorageError<NodeId>> {
    let configs = membership.get_joint_config();
    if configs.len() != 1 {
        return Err(sto_err_logs(format!(
            "joint membership with {} configs cannot be stored in MetaMembership \
             (single voter-set only)",
            configs.len()
        )));
    }
    let voters = configs[0]
        .iter()
        .copied()
        .map(MetaNodeId)
        .collect::<Vec<_>>();
    let learners = membership
        .learner_ids()
        .map(|id| (MetaNodeId(id), String::new()))
        .collect::<Vec<_>>();
    Ok(MetaMembership { voters, learners })
}

pub(crate) fn meta_to_membership(
    membership: &MetaMembership,
) -> Result<Membership<NodeId, openraft::EmptyNode>, StorageError<NodeId>> {
    let mut voters = BTreeSet::new();
    let mut nodes = BTreeMap::new();
    for MetaNodeId(id) in &membership.voters {
        voters.insert(*id);
        nodes.insert(*id, openraft::EmptyNode::default());
    }
    for (MetaNodeId(id), _) in &membership.learners {
        nodes.insert(*id, openraft::EmptyNode::default());
    }
    Ok(Membership::new(vec![voters], nodes))
}
