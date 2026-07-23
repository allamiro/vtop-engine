//! Durable metadata store: hard state, chunked raft log, and snapshots,
//! orchestrated into one deterministic recovery path.
//!
//! Every byte flows through the [`vtop_log::env::Env`] seam, so the whole
//! store runs unchanged against the real filesystem or the deterministic
//! crash simulator. Consensus does not live here: PR 2 wires a Raft engine over
//! these primitives; `MetaStorage` treats every durable log
//! entry as committed during single-node recovery, while the adapter decides
//! the commit frontier under consensus.

pub mod applied;
pub mod hardstate;
pub mod log;
pub mod membership_log_id;
pub mod purged;
pub mod snapshot;

use crate::command::MetadataResponse;
use crate::state::MetaStateMachine;
use crate::wire::CodecError;
use applied::{AppliedFrontier, AppliedFrontierFile, APPLIED_FILE_NAME};
use hardstate::{HardState, HardStateFile};
use log::{MetaLog, MetaLogConfig, MetaLogEntry, MetaLogPayload, MetaMembership};
use membership_log_id::{MembershipLogId, MembershipLogIdFile, MEMBERSHIP_LOG_ID_FILE_NAME};
use purged::{PurgedFrontierFile, PurgedLogId, PURGED_FILE_NAME};
use snapshot::{MetaSnapshots, SnapshotMeta};
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;
use vtop_log::env::{Env, OpenMode, Storage};

/// Errors from the durable store. Semantic rejections never appear here —
/// those are [`crate::command::MetadataError`] values inside responses.
#[derive(Debug, Error)]
pub enum MetaStoreError {
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("corrupt metadata artifact {path}: {reason}")]
    Corrupt { path: PathBuf, reason: String },
    #[error("unsupported format version {version} in {path}")]
    UnsupportedVersion { path: PathBuf, version: u16 },
    #[error("invalid metadata store usage: {0}")]
    InvalidConfig(String),
    #[error("{0} is poisoned after an uncertain write; reopen the store to recover")]
    Poisoned(&'static str),
}

pub type MetaStoreResult<T> = Result<T, MetaStoreError>;

pub(crate) fn io_error(path: &Path, source: io::Error) -> MetaStoreError {
    MetaStoreError::Io {
        path: path.to_path_buf(),
        source,
    }
}

pub(crate) fn corrupt(path: &Path, reason: impl Into<String>) -> MetaStoreError {
    MetaStoreError::Corrupt {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

pub(crate) fn codec_corrupt(path: &Path, error: &CodecError) -> MetaStoreError {
    corrupt(path, error.to_string())
}

pub(crate) fn sync_parent(storage: &dyn Storage, path: &Path) -> MetaStoreResult<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    storage
        .sync_dir(parent)
        .map_err(|source| io_error(parent, source))
}

/// Atomic publication: write a `.{name}.{uuid}.tmp` sibling, sync its data,
/// rename over the target, then sync the directory. The temp-name shape is
/// the one the whole engine pattern-matches when classifying leftovers.
pub(crate) fn write_atomic(env: &Env, path: &Path, bytes: &[u8]) -> MetaStoreResult<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            MetaStoreError::InvalidConfig("target path has no UTF-8 file name".to_owned())
        })?;
    let temporary = path.with_file_name(format!(
        ".{file_name}.{}.tmp",
        Uuid::from_u128(env.rng.next_u128())
    ));
    let mut file = env
        .storage
        .open(&temporary, OpenMode::CreateNew)
        .map_err(|source| io_error(&temporary, source))?;
    let written = std::io::Write::write_all(&mut file, bytes).and_then(|()| file.sync_data());
    drop(file);
    if let Err(source) = written {
        let _ = env.storage.remove_file(&temporary);
        return Err(io_error(&temporary, source));
    }
    if let Err(source) = env.storage.rename(&temporary, path) {
        let _ = env.storage.remove_file(&temporary);
        return Err(io_error(path, source));
    }
    sync_parent(env.storage.as_ref(), path)
}

/// Whether a directory entry is an in-flight atomic-write temporary
/// (`.{name}.{uuid}.tmp`). Such files are ignored by every recovery scan:
/// they were never published, so they carry no acknowledged bytes.
pub(crate) fn is_atomic_temp_name(name: &str) -> bool {
    name.starts_with('.') && name.ends_with(".tmp")
}

/// Tuning knobs for the store. Only the log chunk size is configurable, and
/// only so the crash sweeps can exercise rotation with tiny chunks.
#[derive(Clone, Copy, Debug, Default)]
pub struct MetaStorageConfig {
    pub log: MetaLogConfig,
}

/// The recovered, durable metadata store for one shard of one cluster.
pub struct MetaStorage {
    env: Env,
    hardstate: HardStateFile,
    applied: AppliedFrontierFile,
    purged: PurgedFrontierFile,
    membership_log_id: MembershipLogIdFile,
    log: MetaLog,
    snapshots: MetaSnapshots,
    state: MetaStateMachine,
    membership: MetaMembership,
    last_applied_index: u64,
    last_applied_term: u64,
}

impl MetaStorage {
    /// Open with default configuration (8 MiB log chunks).
    pub fn open_in(
        env: &Env,
        data_dir: impl AsRef<Path>,
        cluster_id: Uuid,
    ) -> MetaStoreResult<Self> {
        Self::open_with(env, data_dir, cluster_id, MetaStorageConfig::default())
    }

    /// Deterministic recovery: newest valid snapshot, then replay of durable
    /// log entries above it through a fresh state machine.
    ///
    /// When a durable `meta.applied` frontier is present, replay
    /// stops there so uncommitted tail entries stay durable but invisible to
    /// the state machine (Raft append ≠ commit). When the file is absent,
    /// recovery keeps the historical single-node behaviour of replaying every
    /// durable log entry through the log tail.
    pub fn open_with(
        env: &Env,
        data_dir: impl AsRef<Path>,
        cluster_id: Uuid,
        config: MetaStorageConfig,
    ) -> MetaStoreResult<Self> {
        let data_dir = data_dir.as_ref();
        let snapshots = MetaSnapshots::open_in(env, data_dir, cluster_id)?;
        let hardstate =
            HardStateFile::open_in(env, data_dir.join(hardstate::HARD_STATE_FILE_NAME))?;
        let applied = AppliedFrontierFile::open_in(env, data_dir.join(APPLIED_FILE_NAME))?;
        let purged = PurgedFrontierFile::open_in(env, data_dir.join(PURGED_FILE_NAME))?;
        let membership_log_id =
            MembershipLogIdFile::open_in(env, data_dir.join(MEMBERSHIP_LOG_ID_FILE_NAME))?;
        let log = MetaLog::open_in(env, data_dir, cluster_id, config.log)?;

        let (mut state, mut membership, mut last_applied_index, mut last_applied_term) =
            match snapshots.newest() {
                Some(meta) => {
                    let payload = snapshots.read(&meta)?;
                    let state = MetaStateMachine::decode_snapshot(&payload)
                        .map_err(|error| codec_corrupt(&meta.path, &error))?;
                    (state, meta.membership, meta.last_index, meta.last_term)
                }
                None => (MetaStateMachine::new(), MetaMembership::default(), 0, 0),
            };

        // Durable applied frontier wins over the snapshot baseline when it is
        // ahead; a stale file behind the snapshot is ignored (snapshot install
        // is the newer authority).
        let replay_through = match applied.get() {
            Some(frontier) if frontier.index > last_applied_index => frontier.index,
            Some(_) => last_applied_index,
            // Legacy single-node disks: no applied file means "everything
            // durable was committed".
            None => log.last_index().unwrap_or(last_applied_index),
        };

        if let Some(first_index) = log.first_index() {
            if first_index > last_applied_index + 1 {
                return Err(corrupt(
                    data_dir,
                    format!(
                        "log begins at {first_index} but the newest snapshot covers only                          up to {last_applied_index}: entries are missing"
                    ),
                ));
            }
        }
        if replay_through > last_applied_index {
            let last = log.last_index().unwrap_or(last_applied_index);
            let through = replay_through.min(last);
            if through > last_applied_index {
                for entry in log.read_range(last_applied_index + 1, through + 1)? {
                    match &entry.payload {
                        MetaLogPayload::Normal(command) => {
                            state.apply(entry.index, command);
                        }
                        MetaLogPayload::Membership(new_membership) => {
                            membership = new_membership.clone();
                        }
                        MetaLogPayload::Blank => {}
                    }
                    last_applied_index = entry.index;
                    last_applied_term = entry.term;
                }
            }
        }

        Ok(Self {
            env: env.clone(),
            hardstate,
            applied,
            purged,
            membership_log_id,
            log,
            snapshots,
            state,
            membership,
            last_applied_index,
            last_applied_term,
        })
    }

    pub fn state(&self) -> &MetaStateMachine {
        &self.state
    }

    pub fn hard_state(&self) -> &HardState {
        self.hardstate.state()
    }

    pub fn membership(&self) -> &MetaMembership {
        &self.membership
    }

    pub fn last_applied(&self) -> u64 {
        self.last_applied_index
    }

    pub fn last_applied_term(&self) -> u64 {
        self.last_applied_term
    }

    /// Exact last-purged meta log id, when the adapter has persisted one.
    pub fn last_purged(&self) -> Option<PurgedLogId> {
        self.purged.get()
    }

    /// Exact log id of the applied membership, when the adapter has persisted one.
    pub fn last_membership_log_id(&self) -> Option<MembershipLogId> {
        self.membership_log_id.get()
    }

    pub fn log(&self) -> &MetaLog {
        &self.log
    }

    pub fn snapshots(&self) -> &MetaSnapshots {
        &self.snapshots
    }

    /// Durably persist a new hard state (term/vote).
    pub fn save_hard_state(&mut self, next: HardState) -> MetaStoreResult<()> {
        self.hardstate.save(next)
    }

    /// Durably append entries without applying them. The first entry must
    /// extend the log (or, on an empty log, sit exactly above the applied
    /// frontier), so a hole can never open between snapshot and log.
    pub fn append(&mut self, entries: &[MetaLogEntry]) -> MetaStoreResult<()> {
        if let Some(first) = entries.first() {
            if self.log.last_index().is_none() && first.index != self.last_applied_index + 1 {
                return Err(MetaStoreError::InvalidConfig(format!(
                    "append starts at {} but the applied frontier is {}",
                    first.index, self.last_applied_index
                )));
            }
        }
        self.log.append(entries)
    }

    /// Apply durable entries through `index`, returning each response in log
    /// order. Membership entries update the tracked membership; blanks are
    /// applied as no-ops.
    pub fn apply_through(&mut self, index: u64) -> MetaStoreResult<Vec<MetadataResponse>> {
        if index <= self.last_applied_index {
            return Ok(Vec::new());
        }
        let last = self.log.last_index().unwrap_or(self.last_applied_index);
        if index > last {
            return Err(MetaStoreError::InvalidConfig(format!(
                "cannot apply through {index}: the log ends at {last}"
            )));
        }
        let mut responses = Vec::new();
        let mut membership_log_id = None;
        for entry in self
            .log
            .read_range(self.last_applied_index + 1, index + 1)?
        {
            match &entry.payload {
                MetaLogPayload::Normal(command) => {
                    responses.push(self.state.apply(entry.index, command));
                }
                MetaLogPayload::Membership(membership) => {
                    self.membership = membership.clone();
                    membership_log_id = Some(MembershipLogId {
                        term: entry.term,
                        index: entry.index,
                    });
                }
                MetaLogPayload::Blank => {}
            }
            self.last_applied_index = entry.index;
            self.last_applied_term = entry.term;
        }
        self.applied.save(AppliedFrontier {
            index: self.last_applied_index,
            term: self.last_applied_term,
        })?;
        // After the applied frontier is durable, persist the membership LogId so
        // reopen does not invent one from a later normal entry at the frontier.
        if let Some(log_id) = membership_log_id {
            self.membership_log_id.save(log_id)?;
        }
        Ok(responses)
    }

    /// Remove entries at and above `index`. Applied entries are immutable
    /// history and can never be truncated away.
    pub fn truncate_since(&mut self, index: u64) -> MetaStoreResult<()> {
        if index <= self.last_applied_index {
            return Err(MetaStoreError::InvalidConfig(format!(
                "cannot truncate at {index}: entries through {} are applied",
                self.last_applied_index
            )));
        }
        self.log.truncate_since(index)
    }

    /// Write a snapshot of the current applied state and retire all but the
    /// newest two snapshot files.
    ///
    /// When a durable membership LogId is known it is embedded in the snapshot
    /// (v2) so install/publish cannot lose it if the sidecar write is interrupted.
    pub fn write_snapshot(&mut self) -> MetaStoreResult<SnapshotMeta> {
        let payload = self.state.encode_snapshot().map_err(|error| {
            MetaStoreError::InvalidConfig(format!("cannot encode state snapshot: {error}"))
        })?;
        let snapshot_id = Uuid::from_u128(self.env.rng.next_u128()).to_string();
        self.snapshots.write(
            self.last_applied_index,
            self.last_applied_term,
            self.membership.clone(),
            self.membership_log_id.get(),
            &snapshot_id,
            &payload,
        )
    }

    /// Delete whole log chunks at or below `index`. Purging is only legal
    /// below snapshot coverage, so recovery always has a replay source.
    pub fn purge_upto(&mut self, index: u64) -> MetaStoreResult<()> {
        let covered = self
            .snapshots
            .newest()
            .map(|meta| meta.last_index)
            .unwrap_or(0);
        if index > covered {
            return Err(MetaStoreError::InvalidConfig(format!(
                "cannot purge through {index}: the newest snapshot covers only {covered}"
            )));
        }
        self.log.purge_upto(index)
    }

    /// Persist the exact acknowledged purged log id (meta coordinates).
    pub fn save_purged(&mut self, term: u64, index: u64) -> MetaStoreResult<()> {
        self.purged.save(PurgedLogId { term, index })
    }

    /// Persist the exact log id of the applied membership (meta coordinates).
    pub fn save_membership_log_id(&mut self, term: u64, index: u64) -> MetaStoreResult<()> {
        self.membership_log_id.save(MembershipLogId { term, index })
    }

    /// Align the durable applied frontier with the in-memory cursor (e.g.
    /// after installing a snapshot that advanced `last_applied` without going
    /// through [`Self::apply_through`]).
    pub fn sync_applied_frontier(&mut self) -> MetaStoreResult<()> {
        if self.last_applied_index == 0 {
            return Ok(());
        }
        self.applied.save(AppliedFrontier {
            index: self.last_applied_index,
            term: self.last_applied_term,
        })
    }

    /// For Raft-managed directories: if `meta.applied` is absent and the disk is
    /// otherwise empty, persist a zero frontier so a crash after the first
    /// append does not full-replay uncommitted entries. Legacy disks that
    /// already have a log or snapshot without `meta.applied` keep the
    /// absent-file full-replay semantics.
    pub fn ensure_raft_applied_frontier(&mut self) -> MetaStoreResult<()> {
        if self.applied.get().is_some() {
            return Ok(());
        }
        if self.log.last_index().is_some()
            || self.last_applied_index > 0
            || self.snapshots.newest().is_some()
        {
            return Ok(());
        }
        self.applied.save(AppliedFrontier { index: 0, term: 0 })
    }

    /// Discard the physical log when it ends strictly below the applied
    /// frontier. Needed after snapshot install onto a non-blank disk: the
    /// chunked log always retains its newest chunk, which would otherwise
    /// reject appends that must continue from `last_applied + 1`.
    pub fn discard_stale_log_tail(&mut self) -> MetaStoreResult<()> {
        let Some(last) = self.log.last_index() else {
            return Ok(());
        };
        if last < self.last_applied_index {
            self.log.discard_all()?;
        }
        Ok(())
    }
}
