//! [`openraft::storage::RaftLogStorage`] over [`crate::MetaStorage`].

use crate::raft::convert::{
    entry_to_meta, hard_state_to_vote, map_store_err, meta_to_entry, sto_err_vote, to_meta_index,
    to_raft_index, vote_to_hard_state,
};
use crate::raft::store::MetaRaftStore;
use crate::raft::type_config::{MetaRaftTypeConfig, NodeId};
use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, StorageError, Vote};
use std::fmt::Debug;
use std::ops::RangeBounds;

/// Log + vote half of the adapter. Cloneable; shares the [`MetaRaftStore`].
#[derive(Clone)]
pub struct MetaRaftLogStore {
    store: MetaRaftStore,
}

impl MetaRaftLogStore {
    pub fn new(store: MetaRaftStore) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &MetaRaftStore {
        &self.store
    }
}

impl RaftLogReader<MetaRaftTypeConfig> for MetaRaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<MetaRaftTypeConfig>>, StorageError<NodeId>> {
        let guard = self.store.lock();
        let mut start_raft = match range.start_bound() {
            std::ops::Bound::Included(&s) => s,
            std::ops::Bound::Excluded(&s) => s.saturating_add(1),
            std::ops::Bound::Unbounded => 0,
        };
        let end_raft = match range.end_bound() {
            std::ops::Bound::Included(&e) => e.saturating_add(1),
            std::ops::Bound::Excluded(&e) => e,
            std::ops::Bound::Unbounded => u64::MAX,
        };
        if let Some(purged) = guard.last_purged {
            start_raft = start_raft.max(purged.index.saturating_add(1));
        }
        if start_raft >= end_raft {
            return Ok(Vec::new());
        }

        let start_meta = to_meta_index(start_raft);
        let end_meta = if end_raft == u64::MAX {
            guard
                .storage
                .log()
                .last_index()
                .map(|last| last.saturating_add(1))
                .unwrap_or(start_meta)
        } else {
            // end_raft is exclusive in openraft coordinates.
            to_meta_index(end_raft.saturating_sub(1)).saturating_add(1)
        };
        if start_meta >= end_meta {
            return Ok(Vec::new());
        }
        let entries = guard
            .storage
            .log()
            .read_range(start_meta, end_meta)
            .map_err(map_store_err)?;
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Some(purged) = guard.last_purged {
                if let Some(raft_index) = to_raft_index(entry.index) {
                    if raft_index <= purged.index {
                        continue;
                    }
                }
            }
            out.push(meta_to_entry(entry)?);
        }
        Ok(out)
    }
}

impl RaftLogStorage<MetaRaftTypeConfig> for MetaRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<MetaRaftTypeConfig>, StorageError<NodeId>> {
        let guard = self.store.lock();
        let last_purged_log_id = guard.last_purged;
        let last_log_id = match guard.storage.log().last_index() {
            Some(meta_last) => {
                let entries = guard
                    .storage
                    .log()
                    .read_range(meta_last, meta_last + 1)
                    .map_err(map_store_err)?;
                let entry = entries.into_iter().next().ok_or_else(|| {
                    map_store_err(crate::MetaStoreError::InvalidConfig(
                        "log last_index has no entry".into(),
                    ))
                })?;
                if let Some(purged) = last_purged_log_id {
                    if let Some(raft_index) = to_raft_index(entry.index) {
                        if raft_index <= purged.index {
                            return Ok(LogState {
                                last_purged_log_id,
                                last_log_id: last_purged_log_id,
                            });
                        }
                    }
                }
                Some(meta_to_entry(entry)?.log_id)
            }
            None => last_purged_log_id,
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut guard = self.store.lock();
        let hard = vote_to_hard_state(vote);
        guard.storage.save_hard_state(hard).map_err(sto_err_vote)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let guard = self.store.lock();
        Ok(hard_state_to_vote(guard.storage.hard_state()))
    }

    async fn save_committed(
        &mut self,
        _committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        // Applied frontier is flushed from RaftStateMachine::apply via
        // MetaStorage::apply_through → meta.applied; committed==applied for
        // this store because apply is synchronous with commit handling.
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(None)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<MetaRaftTypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<MetaRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let meta_entries = entries
            .into_iter()
            .map(|entry| entry_to_meta(&entry))
            .collect::<Result<Vec<_>, _>>()?;
        if meta_entries.is_empty() {
            callback.log_io_completed(Ok(()));
            return Ok(());
        }
        let result = {
            let mut guard = self.store.lock();
            guard.storage.append(&meta_entries)
        };
        match result {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                let io = std::io::Error::other(error.to_string());
                callback.log_io_completed(Err(io));
                Err(map_store_err(error))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut guard = self.store.lock();
        let meta_index = to_meta_index(log_id.index);
        guard
            .storage
            .truncate_since(meta_index)
            .map_err(map_store_err)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut guard = self.store.lock();
        let meta_index = to_meta_index(log_id.index);
        let covered = guard
            .storage
            .snapshots()
            .newest()
            .map(|meta| meta.last_index)
            .unwrap_or(0);
        if meta_index <= covered {
            guard
                .storage
                .purge_upto(meta_index)
                .map_err(map_store_err)?;
            // After snapshot install, the physical tail may still end below
            // the purge/snapshot frontier; drop it so appends can continue.
            guard
                .storage
                .discard_stale_log_tail()
                .map_err(map_store_err)?;
        }
        // else: openraft emits PurgeLog in the same command batch as
        // InstallFullSnapshot without waiting for the SM command to finish
        // (`Command::PurgeLog` has no Condition). On a blank follower the log
        // is empty so skipping the physical purge is safe; `last_purged`
        // still advances so readers honor the logical frontier after install.
        match guard.last_purged {
            Some(prev) if prev.index > log_id.index => {}
            _ => {
                guard.last_purged = Some(log_id);
                guard
                    .storage
                    .save_purged(log_id.leader_id.term, meta_index)
                    .map_err(map_store_err)?;
            }
        }
        Ok(())
    }
}
