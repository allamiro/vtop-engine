//! Shared owner of [`crate::MetaStorage`] for the openraft log + state-machine
//! halves.
//!
//! Both [`super::MetaRaftLogStore`] and [`super::MetaRaftStateMachine`] clone
//! this handle; all durable work goes through [`crate::MetaStorage`] so store
//! guards (vote safety, truncate-below-applied, purge-above-snapshot) stay in
//! force.

use crate::raft::convert::{meta_to_membership, raft_log_id, sto_err_logs, to_raft_index};
use crate::storage::log::MetaLogConfig;
use crate::storage::snapshot::MetaSnapshots;
use crate::{MetaStorage, MetaStorageConfig, MetaStoreResult};
use openraft::{LogId, StoredMembership};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use vtop_log::env::Env;

use super::type_config::NodeId;

/// Interior state behind the adapter.
pub(crate) struct MetaRaftInner {
    pub(crate) env: Env,
    pub(crate) data_dir: PathBuf,
    pub(crate) cluster_id: Uuid,
    pub(crate) config: MetaStorageConfig,
    pub(crate) storage: MetaStorage,
    /// Last membership observed at or below the applied frontier, in openraft
    /// coordinates (raft index, not meta index).
    pub(crate) last_membership: StoredMembership<NodeId, openraft::EmptyNode>,
    /// Openraft's last purged log id. Physical chunks may still hold bytes
    /// below this point (MetaLog only drops whole chunks); readers must not
    /// return them.
    pub(crate) last_purged: Option<LogId<NodeId>>,
}

/// Cloneable owner of a [`MetaStorage`], shared by the log-store and
/// state-machine adapters.
#[derive(Clone)]
pub struct MetaRaftStore {
    pub(crate) inner: Arc<Mutex<MetaRaftInner>>,
}

impl MetaRaftStore {
    /// Open (or recover) the durable store and wrap it for openraft.
    pub fn open(
        env: &Env,
        data_dir: impl AsRef<Path>,
        cluster_id: Uuid,
        config: MetaStorageConfig,
    ) -> MetaStoreResult<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        let storage = MetaStorage::open_with(env, &data_dir, cluster_id, config)?;
        let last_membership = recover_membership(&storage)?;
        let last_purged = recover_last_purged(&storage);
        Ok(Self {
            inner: Arc::new(Mutex::new(MetaRaftInner {
                env: env.clone(),
                data_dir,
                cluster_id,
                config,
                storage,
                last_membership,
                last_purged,
            })),
        })
    }

    /// Convenience: default 8 MiB log chunks.
    pub fn open_in(
        env: &Env,
        data_dir: impl AsRef<Path>,
        cluster_id: Uuid,
    ) -> MetaStoreResult<Self> {
        Self::open(env, data_dir, cluster_id, MetaStorageConfig::default())
    }

    /// Tiny chunks for harness crash / multi-chunk scenarios.
    pub fn open_tiny(
        env: &Env,
        data_dir: impl AsRef<Path>,
        cluster_id: Uuid,
    ) -> MetaStoreResult<Self> {
        Self::open(
            env,
            data_dir,
            cluster_id,
            MetaStorageConfig {
                log: MetaLogConfig {
                    max_chunk_bytes: 256,
                },
            },
        )
    }

    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, MetaRaftInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Inspect the underlying state machine (test / admin helper).
    pub fn with_storage<R>(&self, f: impl FnOnce(&MetaStorage) -> R) -> R {
        let guard = self.lock();
        f(&guard.storage)
    }
}

fn recover_membership(
    storage: &MetaStorage,
) -> Result<StoredMembership<NodeId, openraft::EmptyNode>, crate::MetaStoreError> {
    if storage.last_applied() == 0 && storage.membership().voters.is_empty() {
        return Ok(StoredMembership::default());
    }
    let membership = meta_to_membership(storage.membership()).map_err(|error| {
        crate::MetaStoreError::InvalidConfig(format!("cannot map recovered membership: {error}"))
    })?;
    // Prefer the log id of the last membership entry at or below applied,
    // scanning the durable log; fall back to the applied frontier.
    let mut membership_log_id = applied_raft_log_id(storage);
    let applied = storage.last_applied();
    if applied > 0 {
        if let Ok(entries) = storage.log().read_range(1, applied + 1) {
            for entry in entries {
                if matches!(entry.payload, crate::MetaLogPayload::Membership(_)) {
                    if let Some(raft_index) = to_raft_index(entry.index) {
                        membership_log_id = Some(raft_log_id(entry.term, raft_index));
                    }
                }
            }
        }
    }
    Ok(StoredMembership::new(membership_log_id, membership))
}

fn recover_last_purged(storage: &MetaStorage) -> Option<LogId<NodeId>> {
    match storage.log().first_index() {
        Some(first) if first > 1 => {
            let purged_meta = first - 1;
            let term = storage
                .snapshots()
                .newest()
                .map(|meta| {
                    if meta.last_index >= purged_meta {
                        meta.last_term
                    } else {
                        // Should not happen under MetaStorage guards; use a
                        // conservative term from the first live entry.
                        storage
                            .log()
                            .read_range(first, first + 1)
                            .ok()
                            .and_then(|entries| entries.into_iter().next())
                            .map(|entry| entry.term)
                            .unwrap_or(0)
                    }
                })
                .unwrap_or(0);
            to_raft_index(purged_meta).map(|index| raft_log_id(term, index))
        }
        None => {
            // Empty log: if a snapshot covers anything, treat it as purged
            // through the snapshot frontier (blank-follower / post-purge).
            let meta = storage.snapshots().newest()?;
            if meta.last_index == 0 {
                return None;
            }
            to_raft_index(meta.last_index).map(|index| raft_log_id(meta.last_term, index))
        }
        _ => None,
    }
}

pub(crate) fn applied_raft_log_id(storage: &MetaStorage) -> Option<LogId<NodeId>> {
    let meta_index = storage.last_applied();
    if meta_index == 0 {
        return None;
    }
    let raft_index = to_raft_index(meta_index)?;
    let term = if let Ok(entries) = storage.log().read_range(meta_index, meta_index + 1) {
        if let Some(entry) = entries.into_iter().next() {
            entry.term
        } else if let Some(snap) = storage.snapshots().newest() {
            if snap.last_index == meta_index {
                snap.last_term
            } else {
                return None;
            }
        } else {
            return None;
        }
    } else if let Some(snap) = storage.snapshots().newest() {
        if snap.last_index == meta_index {
            snap.last_term
        } else {
            return None;
        }
    } else {
        return None;
    };
    Some(raft_log_id(term, raft_index))
}

pub(crate) fn read_snapshot_file_bytes(
    inner: &MetaRaftInner,
    path: &Path,
) -> Result<Vec<u8>, openraft::StorageError<NodeId>> {
    inner
        .env
        .storage
        .read(path)
        .map_err(|source| sto_err_logs(format!("read snapshot {}: {source}", path.display())))
}

pub(crate) fn install_snapshot_bytes(
    inner: &mut MetaRaftInner,
    bytes: &[u8],
) -> Result<(), openraft::StorageError<NodeId>> {
    let mut snapshots = MetaSnapshots::open_in(&inner.env, &inner.data_dir, inner.cluster_id)
        .map_err(sto_err_logs)?;
    snapshots.install(bytes).map_err(sto_err_logs)?;
    // Full recovery path: newest snapshot + replay of any log above it.
    inner.storage =
        MetaStorage::open_with(&inner.env, &inner.data_dir, inner.cluster_id, inner.config)
            .map_err(sto_err_logs)?;
    Ok(())
}
