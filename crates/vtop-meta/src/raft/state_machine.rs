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
    /// This metadata node, named in its applied audit records (#694).
    node_id: Option<NodeId>,
}

impl MetaRaftStateMachine {
    pub fn new(store: MetaRaftStore) -> Self {
        Self {
            store,
            node_id: None,
        }
    }

    /// Name this replica in its applied audit records.
    pub fn with_node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub fn store(&self) -> &MetaRaftStore {
        &self.store
    }
}

impl MetaRaftStateMachine {
    /// The applied record of an audited command (#694): every replica writes
    /// one, with the request id and the log index, so the record comes from
    /// the applied command and not only from the client. Written after
    /// `apply_through` has saved the applied frontier: a crash in between
    /// loses this replica's record of the entry (the others keep theirs),
    /// and an entry a snapshot brought gets none. Readers dedupe on the
    /// log index.
    fn applied_record(
        &self,
        entry: &Entry<MetaRaftTypeConfig>,
        command: &crate::command::MetadataCommand,
        response: &crate::command::MetadataResponse,
    ) -> Option<crate::audit::AuditRecord> {
        let audit = crate::audit::command_audit(command)?;
        let mut record = crate::audit::AuditRecord::for_command(
            crate::audit::AuditStage::Applied,
            crate::audit::AuditActor::Replica(self.node_id),
            command,
            audit,
            crate::audit::AuditOutcome::Success,
        );
        let index = to_meta_index(entry.log_id.index);
        record.log_index = Some(index);
        if let crate::command::MetadataResponse::Rejected(error) = response {
            record.outcome = crate::audit::AuditOutcome::Failure;
            record.reason = Some(format!(
                "rejected by the state machine at index {index}: {error}"
            ));
        }
        Some(record)
    }

    fn membership_record(
        &self,
        entry: &Entry<MetaRaftTypeConfig>,
        membership: &crate::MetaMembership,
    ) -> crate::audit::AuditRecord {
        let voters = membership
            .voters
            .iter()
            .map(|id| id.0.to_string())
            .collect::<Vec<_>>()
            .join(",");
        crate::audit::AuditRecord {
            stage: crate::audit::AuditStage::Applied,
            action: crate::audit::AuditAction::ClusterMembership,
            actor: crate::audit::AuditActor::Replica(self.node_id),
            target_kind: "cluster",
            target_id: None,
            params: vec![("voters", voters)],
            outcome: crate::audit::AuditOutcome::Success,
            reason: None,
            request_id: None,
            log_index: Some(to_meta_index(entry.log_id.index)),
        }
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
        let mut applied = Vec::new();
        for entry in &entries {
            match &entry.payload {
                EntryPayload::Normal(command) => {
                    let response = resp_iter.next().ok_or_else(|| {
                        sto_err_apply(
                            entry.log_id,
                            "apply_through returned fewer Normal responses than entries",
                        )
                    })?;
                    let bytes = response.encode().map_err(|error| {
                        sto_err_apply(entry.log_id, format!("encode response: {error}"))
                    })?;
                    if let Some(record) = self.applied_record(entry, command, &response) {
                        applied.push(record);
                    }
                    out.push(bytes);
                }
                EntryPayload::Membership(membership) => {
                    // apply_through already updated MetaStorage membership and
                    // flushed meta.membership_log_id; keep the openraft view.
                    let meta = membership_to_meta(membership)?;
                    guard.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    applied.push(self.membership_record(entry, &meta));
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
        // After the store lock is released, and only for entries this call
        // applied: a log replayed when the store opens is applied inside
        // `MetaStorage`, never through here, so it writes no records.
        drop(guard);
        for record in &applied {
            crate::audit::emit(record);
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
