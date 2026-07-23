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
        let mut storage = MetaStorage::open_with(env, &data_dir, cluster_id, config)?;
        // Crash window after snapshot install: applied/snapshot may be durable
        // while discard_stale_log_tail never ran. Heal on every reopen so the
        // recovered log is appendable from last_applied + 1.
        storage.discard_stale_log_tail()?;
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
    // Prefer the exact LogId the adapter persisted on apply / snapshot install.
    // Falling back to the applied frontier invents a membership version when the
    // membership entry was purged or never present on a blank follower.
    if let Some(id) = storage.last_membership_log_id() {
        let membership_log_id = to_raft_index(id.index).map(|index| raft_log_id(id.term, index));
        return Ok(StoredMembership::new(membership_log_id, membership));
    }
    // Legacy disks: scan the durable log for the last membership entry at or
    // below applied; only then fall back to the applied frontier.
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
    // Prefer the exact LogId the adapter persisted on purge(). Inferring from
    // chunk layout is lossy when only whole chunks were removed.
    if let Some(purged) = storage.last_purged() {
        return to_raft_index(purged.index).map(|index| raft_log_id(purged.term, index));
    }
    match storage.log().first_index() {
        Some(first) if first > 1 => {
            let purged_meta = first - 1;
            // Without a durable purged file, fall back to the term of the
            // first live entry (never invent a snapshot-final term for an
            // index the snapshot may not have covered at that term).
            let term = storage
                .log()
                .read_range(first, first + 1)
                .ok()
                .and_then(|entries| entries.into_iter().next())
                .map(|entry| entry.term)
                .or_else(|| {
                    storage
                        .snapshots()
                        .newest()
                        .and_then(|meta| (meta.last_index == purged_meta).then_some(meta.last_term))
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
    let term = match storage.log().read_range(meta_index, meta_index + 1) {
        Ok(entries) => match entries.into_iter().next() {
            Some(entry) => entry.term,
            None => {
                let snap = storage.snapshots().newest()?;
                if snap.last_index == meta_index {
                    snap.last_term
                } else {
                    return None;
                }
            }
        },
        Err(_) => {
            let snap = storage.snapshots().newest()?;
            if snap.last_index == meta_index {
                snap.last_term
            } else {
                return None;
            }
        }
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
    // Full recovery path: newest snapshot + replay limited by durable applied.
    inner.storage =
        MetaStorage::open_with(&inner.env, &inner.data_dir, inner.cluster_id, inner.config)
            .map_err(sto_err_logs)?;
    // Snapshot install advances the applied cursor without apply_through; keep
    // the durable frontier aligned so a later reopen does not replay a stale
    // pre-snapshot applied file past the new baseline incorrectly.
    inner
        .storage
        .sync_applied_frontier()
        .map_err(sto_err_logs)?;
    // A non-blank follower may still hold a physical log ending below the
    // snapshot frontier. purge_upto cannot drop the newest chunk, so discard
    // that stale tail before the next append must extend from last_applied+1.
    inner
        .storage
        .discard_stale_log_tail()
        .map_err(sto_err_logs)?;
    Ok(())
}
