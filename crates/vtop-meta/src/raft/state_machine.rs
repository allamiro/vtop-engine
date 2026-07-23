//! [`openraft::storage::RaftStateMachine`] over [`crate::MetaStorage`].

use crate::raft::convert::{
    membership_to_meta, meta_to_membership, raft_log_id, sto_err_apply, sto_err_snapshot,
    to_meta_index, to_raft_index,
};
use crate::raft::store::{
    applied_raft_log_id, install_snapshot_bytes, read_snapshot_file_bytes, MetaRaftStore,
};
use crate::raft::type_config::{MetaRaftTypeConfig, NodeId, Response};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogId, SnapshotMeta, StorageError,
    StoredMembership,
};
use std::io::Cursor;

/// State-machine + snapshot half of the adapter.
#[derive(Clone)]
pub struct MetaRaftStateMachine {
    store: MetaRaftStore,
}

impl MetaRaftStateMachine {
    pub fn new(store: MetaRaftStore) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &MetaRaftStore {
        &self.store
    }
}

impl RaftSnapshotBuilder<MetaRaftTypeConfig> for MetaRaftStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<MetaRaftTypeConfig>, StorageError<NodeId>> {
        let mut guard = self.store.lock();
        let meta = guard.storage.write_snapshot().map_err(sto_err_snapshot)?;
        let bytes = read_snapshot_file_bytes(&guard, &meta.path)?;
        let last_log_id =
            to_raft_index(meta.last_index).map(|index| raft_log_id(meta.last_term, index));
        // Membership in the snapshot header is the applied membership; keep
        // the openraft StoredMembership log_id from the adapter's tracking.
        let last_membership = guard.last_membership.clone();
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id,
                last_membership,
                snapshot_id: meta.snapshot_id,
            },
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

impl RaftStateMachine<MetaRaftTypeConfig> for MetaRaftStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::EmptyNode>,
        ),
        StorageError<NodeId>,
    > {
        let guard = self.store.lock();
        let last_applied = applied_raft_log_id(&guard.storage);
        Ok((last_applied, guard.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<MetaRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry<MetaRaftTypeConfig>> = entries.into_iter().collect();
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mut guard = self.store.lock();
        let last = entries.last().expect("non-empty");
        let through_meta = to_meta_index(last.get_log_id().index);
        let responses = guard
            .storage
            .apply_through(through_meta)
            .map_err(|error| sto_err_apply(*last.get_log_id(), error))?;

        let mut resp_iter = responses.into_iter();
        let mut out = Vec::with_capacity(entries.len());
        for entry in &entries {
            match &entry.payload {
                EntryPayload::Normal(_) => {
                    let response = resp_iter.next().ok_or_else(|| {
                        sto_err_apply(
                            entry.log_id,
                            "apply_through returned fewer Normal responses than entries",
                        )
                    })?;
                    let bytes = response.encode().map_err(|error| {
                        sto_err_apply(entry.log_id, format!("encode response: {error}"))
                    })?;
                    out.push(bytes);
                }
                EntryPayload::Membership(membership) => {
                    // apply_through already updated MetaStorage membership and
                    // flushed meta.membership_log_id; keep the openraft view.
                    membership_to_meta(membership)?;
                    guard.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    out.push(Vec::new());
                }
                EntryPayload::Blank => out.push(Vec::new()),
            }
        }
        if resp_iter.next().is_some() {
            return Err(sto_err_apply(
                *last.get_log_id(),
                "apply_through returned more Normal responses than Normal entries",
            ));
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::EmptyNode>,
        mut snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = std::mem::take(snapshot.get_mut());
        let membership_log_id = *meta.last_membership.log_id();
        let membership_meta =
            membership_log_id.map(|log_id| (log_id.leader_id.term, to_meta_index(log_id.index)));
        let mut guard = self.store.lock();
        // Membership LogId is embedded in the published snapshot (and mirrored
        // to the sidecar) inside install_snapshot_bytes — no post-publish
        // sidecar-only window.
        install_snapshot_bytes(&mut guard, &bytes, membership_meta)?;
        // Align adapter bookkeeping with the installed snapshot. The VTOP
        // file carries its own last_index/term/membership; openraft's meta is
        // the authority for the StoredMembership log_id.
        if let Some(last_log_id) = meta.last_log_id {
            match guard.last_purged {
                Some(prev) if prev.index >= last_log_id.index => {}
                _ => {
                    // Until the log store purges, readers still see old
                    // physical entries; logical purged catches up on purge().
                    // After a blank-disk install the log is empty and
                    // recover_last_purged already set this on reopen — refresh.
                    if guard.storage.log().last_index().is_none() {
                        guard.last_purged = Some(last_log_id);
                    }
                }
            }
        }
        let recovered = meta_to_membership(guard.storage.membership())?;
        guard.last_membership = StoredMembership::new(membership_log_id, recovered);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MetaRaftTypeConfig>>, StorageError<NodeId>> {
        let guard = self.store.lock();
        let Some(meta) = guard.storage.snapshots().newest() else {
            return Ok(None);
        };
        let bytes = read_snapshot_file_bytes(&guard, &meta.path)?;
        let last_log_id =
            to_raft_index(meta.last_index).map(|index| raft_log_id(meta.last_term, index));
        Ok(Some(Snapshot {
            meta: SnapshotMeta {
                last_log_id,
                last_membership: guard.last_membership.clone(),
                snapshot_id: meta.snapshot_id,
            },
            snapshot: Box::new(Cursor::new(bytes)),
        }))
    }
}
