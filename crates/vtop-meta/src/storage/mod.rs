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

use crate::command::{MetadataResponse, SUPPORTED_FEATURE_LEVEL};
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
    /// Checksum-valid, but naming a kind or tag this build does not know:
    /// written by a NEWER release, not damaged (#554). Reported apart from
    /// `Corrupt` because the two call for opposite responses - upgrade this
    /// node, versus restore or replace it - and an old node that restarts
    /// against a newer cluster's log used to call the log corrupt.
    #[error(
        "metadata artifact {path} was written by a newer release ({reason}); this build \
         cannot read it. Upgrade this node: downgrade is not supported"
    )]
    NewerFormat { path: PathBuf, reason: String },
    /// The directory's recorded feature level - in a snapshot, in a raise
    /// entry in the log, or in the applied state - is above what this build
    /// supports. Not corruption and not a kind this build cannot decode (a
    /// raise adds no command kind at some levels, so the decoders see
    /// nothing wrong): the level itself says the directory was raised by
    /// a release that implements more than this one does. Opening it, or
    /// applying the raise as a rejection, would leave this voter at a level
    /// the others are not at, and the downgrade policy says an older binary
    /// refuses a directory it cannot fully understand.
    #[error(
        "metadata directory {path} is at feature level {directory_level} ({found}), above the \
         highest level this binary supports ({supported}). This node did not open or apply it \
         and changed nothing. Run a newer binary that supports level {directory_level} or \
         higher on this node: downgrade is not supported"
    )]
    NewerFeatureLevel {
        path: PathBuf,
        directory_level: u32,
        supported: u32,
        /// Where the level was found: "recorded in the newest snapshot",
        /// "raised by the log entry at index N", and so on.
        found: String,
    },
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
    if is_newer_format(error) {
        return MetaStoreError::NewerFormat {
            path: path.to_path_buf(),
            reason: error.to_string(),
        };
    }
    corrupt(path, error.to_string())
}

/// Refuse a feature level above what this build supports (#743).
///
/// `found` says where the level was seen, for the message.
pub(crate) fn refuse_newer_level(
    path: &Path,
    directory_level: u32,
    supported: u32,
    found: impl FnOnce() -> String,
) -> MetaStoreResult<()> {
    if directory_level > supported {
        return Err(MetaStoreError::NewerFeatureLevel {
            path: path.to_path_buf(),
            directory_level,
            supported,
            found: found(),
        });
    }
    Ok(())
}

/// The first log entry in `entries` that raises the feature level above
/// `supported`, as `(index, level)`. A raise is the one command that names a
/// level, and every other command this build decodes is at or below its own
/// level, so it is the only one the apply and replay paths look for.
pub(crate) fn first_raise_above(entries: &[MetaLogEntry], supported: u32) -> Option<(u64, u32)> {
    entries.iter().find_map(|entry| match &entry.payload {
        MetaLogPayload::Normal(crate::command::MetadataCommand::SetFeatureLevel {
            feature_level,
            ..
        }) if *feature_level > supported => Some((entry.index, *feature_level)),
        _ => None,
    })
}

/// The `UnknownTag` `what`s of enums a later release may extend. On a reader
/// that predates the variant, a checksum-valid record carrying it was
/// written by a newer release, so it is reported as a newer format, never
/// as corruption (#554, #743). Every `UnknownTag` site in this crate is in
/// this list or in `CLOSED_FOREVER_WHATS`; a test scans the sources.
/// An enum or presence byte that can grow must fail with `UnknownTag`, not
/// `InvalidValue`, or this review cannot see it.
pub(crate) const NEWER_FORMAT_WHATS: &[&str] = &[
    "command kind",
    "log entry kind",
    "record value tag",
    "meta key category",
    "snapshot payload version",
    // Inside a known kind (#577): a promotion outcome or answer verdict this
    // build does not know was written by a newer one, exactly as an unknown
    // kind is.
    "promotion outcome",
    "quorum answer lineage",
    // A refusal reason this build does not know (#636 added one) is the
    // same: a newer writer, not corruption.
    "promotion refusal",
    // The closed-enum review (#743): enums inside snapshot records and the
    // request dedup table (which travels in snapshots), and the transition
    // grant on the wire. Each can gain a variant behind a feature level; a
    // voter that predates it must say "newer format", not "corrupt".
    "node state",
    "verification method",
    "segment state",
    "key state",
    "transition grant kind",
    "response kind",
    "error kind",
    // Shapes that grew in place before: a range record's lease presence
    // (#223 added 2, a lease with a deadline), and a membership's joint
    // marker (#215 appended it). Both reach a voter in a snapshot or a log
    // entry from a newer leader.
    "lease presence",
    "joint marker",
    // Inside kind 40 and the record of tag 26 (#706): the actor, the peer
    // and the subject an attribution carries, and the outcome the tail
    // records. A later level adds an actor kind (a declared user, say) or a
    // subject (a membership change) by a new tag; a reader that predates it
    // says "newer", not "corrupt".
    "attribution actor",
    "attribution peer",
    "attribution subject",
    "audit outcome",
];

/// The `UnknownTag` `what`s this review does not classify as newer, each
/// with its reason. All five are decoded only on a client side (the leader
/// reading AppendEntries replies, `vtopctl` reading admin replies), where
/// nothing consults `is_newer_format` today: a new caller of the classifier
/// on those paths must revisit this list.
#[cfg(test)]
pub(crate) const CLOSED_FOREVER_WHATS: &[&str] = &[
    // AppendEntries reply kinds, read by the leader: the review of #554
    // kept them out of the newer set on purpose
    // (`only_a_newer_peers_frames_are_called_newer`).
    "append response kind",
    // Presence bytes of `AdminReadSegmentPlacementResponse`, read by
    // `vtopctl`: an Option's absent or present (and, for a placement
    // proposal, refused).
    "rebalance intent presence",
    "segment presence",
    "proposal presence",
    // The redirect hint on an admin error, read by `vtopctl`. It grew in
    // place once (#292); a client that cannot read a newer hint reports the
    // error text it has.
    "admin error redirect",
];

/// Whether a decode failure is a kind, tag or version this build does not
/// know - the signature of a newer writer - rather than malformed bytes. Only
/// meaningful once the bytes' checksum has verified, as it has everywhere
/// these are decoded from disk.
pub(crate) fn is_newer_format(error: &CodecError) -> bool {
    matches!(
        error,
        CodecError::UnknownTag { what, .. } if NEWER_FORMAT_WHATS.contains(what)
    )
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
    data_dir: PathBuf,
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
                    // A snapshot a newer release wrote at a level this build
                    // does not implement is installed as it is by nothing
                    // that checks, and the directory would open at a level
                    // the voters running it do not understand (#743).
                    refuse_newer_level(
                        data_dir,
                        state.feature_level(),
                        SUPPORTED_FEATURE_LEVEL,
                        || {
                            format!(
                                "recorded in the snapshot {} covering log index {}",
                                meta.path.display(),
                                meta.last_index
                            )
                        },
                    )?;
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
                let entries = log.read_range(last_applied_index + 1, through + 1)?;
                // A raise above this build's level, replayed as a rejection,
                // would open the directory at a level the voters that
                // accepted it are not at (#743).
                if let Some((index, level)) = first_raise_above(&entries, SUPPORTED_FEATURE_LEVEL) {
                    refuse_newer_level(data_dir, level, SUPPORTED_FEATURE_LEVEL, || {
                        format!("raised by the log entry at index {index}")
                    })?;
                }
                for entry in entries {
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
            data_dir: data_dir.to_path_buf(),
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
        let entries = self
            .log
            .read_range(self.last_applied_index + 1, index + 1)?;
        // Refused BEFORE anything in the batch is applied: a raise above
        // this build's level applied as a rejection would leave this voter
        // at a level the voters that accepted it are not at, and the
        // directory would keep opening (#743). Nothing is applied and the
        // applied frontier does not move, so the next open, on a build
        // that supports the level, applies the same entries.
        if let Some((raise_index, level)) = first_raise_above(&entries, SUPPORTED_FEATURE_LEVEL) {
            refuse_newer_level(&self.data_dir, level, SUPPORTED_FEATURE_LEVEL, || {
                format!("raised by the log entry at index {raise_index}")
            })?;
        }
        for entry in entries {
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
    pub fn write_snapshot(&mut self) -> MetaStoreResult<SnapshotMeta> {
        let payload = self.state.encode_snapshot().map_err(|error| {
            MetaStoreError::InvalidConfig(format!("cannot encode state snapshot: {error}"))
        })?;
        let snapshot_id = Uuid::from_u128(self.env.rng.next_u128()).to_string();
        self.snapshots.write(
            self.last_applied_index,
            self.last_applied_term,
            self.membership.clone(),
            &snapshot_id,
            &payload,
            self.membership_log_id.get(),
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

    /// Whether a durable `meta.applied` file is present.
    pub fn has_applied_frontier(&self) -> bool {
        self.applied.get().is_some()
    }

    /// Pin a Raft applied frontier when the durable file is still absent.
    ///
    /// New blank stores get a zero frontier so a crash after the first
    /// uncommitted appends does not fall into legacy full-log replay.
    /// Legacy disks without the file keep full-log replay on
    /// [`Self::open_with`]; once the Raft adapter opens them it pins the
    /// recovered cursor here so later uncommitted tails stay truncatable.
    pub fn ensure_raft_applied_frontier(&mut self) -> MetaStoreResult<()> {
        if self.applied.get().is_some() {
            return Ok(());
        }
        self.applied.save(AppliedFrontier {
            index: self.last_applied_index,
            term: self.last_applied_term,
        })
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

/// #743: the closed-enum review, made mechanical. Every `UnknownTag` a
/// decoder in this crate can raise names its `what`; each must be either a
/// newer format (the enum can gain a variant, so an older reader must say
/// "newer", not "corrupt") or closed forever. A new site that is in neither
/// list fails here, so the decision is made when the enum is written, not
/// the day an older node meets its first new variant.
#[cfg(test)]
mod closed_enum_review_tests {
    use super::*;
    use std::collections::BTreeSet;

    /// What the production sources (each file up to its first `#[cfg(test)]`
    /// module) say about `UnknownTag`: every literal `what` they name, and
    /// the ones a decoder actually returns (`Err(...UnknownTag { .. })`).
    /// A site whose `what` is not a literal fails here: the scan could not
    /// classify it, so it must not pass silently.
    fn unknown_tag_whats() -> (BTreeSet<String>, BTreeSet<String>) {
        let mut named = BTreeSet::new();
        let mut raised = BTreeSet::new();
        let mut pending = vec![std::path::PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src"
        ))];
        while let Some(path) = pending.pop() {
            for entry in std::fs::read_dir(&path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                let full = std::fs::read_to_string(&path).unwrap();
                // The first test module, private or not: command.rs's is
                // `pub(crate)` so the fuzz seeds can reuse its values.
                let source = ["#[cfg(test)]\nmod ", "#[cfg(test)]\npub(crate) mod "]
                    .iter()
                    .filter_map(|marker| full.find(marker))
                    .min()
                    .map_or(&full[..], |at| &full[..at]);
                for (at, opening) in source.match_indices("UnknownTag {") {
                    // The struct literal up to its closing brace.
                    let body = &source[at + opening.len()..];
                    let window = &body[..body.find('}').unwrap_or(body.len())];
                    let Some(start) = window.find("what: \"") else {
                        // A pattern (`{ what, .. }`) or the variant's own
                        // definition names no value; anything else must.
                        assert!(
                            window.contains("..") || window.contains("what: &'static str"),
                            "{}: an UnknownTag whose what is not a literal: {window:?}. \
                             Use a literal, so this review can classify it",
                            path.display()
                        );
                        continue;
                    };
                    let rest = &window[start + "what: \"".len()..];
                    let end = rest.find('"').expect("an unterminated what literal");
                    let what = rest[..end].to_owned();
                    let before = source[..at].trim_end();
                    if before.ends_with("Err(CodecError::")
                        || before.ends_with("Err(crate::wire::CodecError::")
                    {
                        raised.insert(what.clone());
                    }
                    named.insert(what);
                }
            }
        }
        (named, raised)
    }

    #[test]
    fn unknown_tag_sites_are_classified() {
        let (whats, raised) = unknown_tag_whats();
        assert!(
            raised.contains("command kind") && raised.contains("node state"),
            "the source scan found nothing it should: {raised:?}"
        );
        let unclassified: Vec<_> = whats
            .iter()
            .filter(|what| {
                !NEWER_FORMAT_WHATS.contains(&what.as_str())
                    && !CLOSED_FOREVER_WHATS.contains(&what.as_str())
            })
            .collect();
        assert!(
            unclassified.is_empty(),
            "UnknownTag sites with no classification: {unclassified:?}. Add each to \
             NEWER_FORMAT_WHATS if a later release may add a variant (an older reader then \
             reports it as newer, not corrupt), or to CLOSED_FOREVER_WHATS with the reason it \
             can never grow"
        );
        for what in NEWER_FORMAT_WHATS {
            assert!(
                !CLOSED_FOREVER_WHATS.contains(what),
                "{what} is in both lists"
            );
            assert!(
                is_newer_format(&CodecError::UnknownTag { what, tag: 0x7f }),
                "{what} is listed as newer but not classified so"
            );
        }
        for what in CLOSED_FOREVER_WHATS {
            assert!(
                !is_newer_format(&CodecError::UnknownTag { what, tag: 0x7f }),
                "{what} is closed forever but classified as newer"
            );
        }
        let stale: Vec<_> = NEWER_FORMAT_WHATS
            .iter()
            .chain(CLOSED_FOREVER_WHATS)
            .filter(|what| !raised.contains(**what))
            .collect();
        assert!(
            stale.is_empty(),
            "listed but no decoder returns it: {stale:?}"
        );
    }
}

/// #743: a directory whose recorded feature level is above this build's is
/// refused, at open, at snapshot install and at the apply of a raise, with
/// nothing changed. Equal and lower levels open as before.
#[cfg(test)]
mod feature_level_refusal_tests {
    use super::*;
    use crate::command::{CommandEnvelope, MetadataCommand, BASELINE_FEATURE_LEVEL};
    use std::path::Path;
    use vtop_log::sim::SimStorage;

    const ABOVE: u32 = SUPPORTED_FEATURE_LEVEL + 1;

    fn cluster() -> Uuid {
        Uuid::from_u128(0x743)
    }

    fn sim() -> (SimStorage, Env) {
        let sim = SimStorage::new();
        sim.create_dir_all(Path::new("/n1"));
        let env = sim.env(0x743);
        (sim, env)
    }

    fn raise(index: u64, level: u32) -> MetaLogEntry {
        MetaLogEntry {
            term: 1,
            index,
            payload: MetaLogPayload::Normal(MetadataCommand::SetFeatureLevel {
                env: CommandEnvelope {
                    request_id: Uuid::from_u128(u128::from(index)),
                    issued_at_ms: 1_750_000_000_000,
                },
                feature_level: level,
            }),
        }
    }

    /// Publish a snapshot of a state recorded at `level` into `/n1`.
    fn publish_snapshot_at(env: &Env, level: u32) {
        let payload = MetaStateMachine::at_feature_level_for_test(level)
            .encode_snapshot()
            .unwrap();
        MetaSnapshots::open_in(env, "/n1", cluster())
            .unwrap()
            .write(5, 1, MetaMembership::default(), "snap", &payload, None)
            .unwrap();
    }

    fn refused(result: MetaStoreResult<MetaStorage>) -> (u32, u32, String) {
        match result {
            Err(error @ MetaStoreError::NewerFeatureLevel { .. }) => {
                let text = error.to_string();
                let MetaStoreError::NewerFeatureLevel {
                    directory_level,
                    supported,
                    ..
                } = error
                else {
                    unreachable!()
                };
                (directory_level, supported, text)
            }
            Err(other) => panic!("expected a feature level refusal, got: {other}"),
            Ok(_) => panic!("a directory above this build's level was opened"),
        }
    }

    #[test]
    fn a_snapshot_above_this_builds_level_is_refused_at_open() {
        let (_sim, env) = sim();
        publish_snapshot_at(&env, ABOVE);
        let (level, supported, text) = refused(MetaStorage::open_in(&env, "/n1", cluster()));
        assert_eq!((level, supported), (ABOVE, SUPPORTED_FEATURE_LEVEL));
        // Names both levels and what to do.
        assert!(text.contains(&format!("feature level {ABOVE}")), "{text}");
        assert!(
            text.contains(&format!("supports ({SUPPORTED_FEATURE_LEVEL})")),
            "{text}"
        );
        assert!(text.contains("Run a newer binary"), "{text}");
        assert!(text.contains("snapshot"), "{text}");
    }

    #[test]
    fn a_snapshot_at_this_builds_level_opens() {
        let (_sim, env) = sim();
        publish_snapshot_at(&env, SUPPORTED_FEATURE_LEVEL);
        let opened = MetaStorage::open_in(&env, "/n1", cluster()).unwrap();
        assert_eq!(opened.state().feature_level(), SUPPORTED_FEATURE_LEVEL);
        assert_eq!(opened.last_applied(), 5);
    }

    #[test]
    fn a_snapshot_below_this_builds_level_opens() {
        let (_sim, env) = sim();
        publish_snapshot_at(&env, BASELINE_FEATURE_LEVEL);
        let opened = MetaStorage::open_in(&env, "/n1", cluster()).unwrap();
        assert_eq!(opened.state().feature_level(), BASELINE_FEATURE_LEVEL);
    }

    #[test]
    fn a_log_raise_above_this_builds_level_is_refused_when_replayed_at_open() {
        let (_sim, env) = sim();
        // No applied file: the legacy open replays every durable entry.
        let mut log = MetaLog::open_in(&env, "/n1", cluster(), MetaLogConfig::default()).unwrap();
        log.append(&[raise(1, ABOVE)]).unwrap();
        drop(log);
        let (level, supported, text) = refused(MetaStorage::open_in(&env, "/n1", cluster()));
        assert_eq!((level, supported), (ABOVE, SUPPORTED_FEATURE_LEVEL));
        assert!(text.contains("log entry at index 1"), "{text}");
    }

    #[test]
    fn a_raise_above_this_builds_level_is_refused_when_applied_not_turned_into_a_rejection() {
        let (_sim, env) = sim();
        let mut storage = MetaStorage::open_in(&env, "/n1", cluster()).unwrap();
        storage.ensure_raft_applied_frontier().unwrap();
        // The entries before the raise, and the raise, in one batch.
        storage
            .append(&[raise(1, SUPPORTED_FEATURE_LEVEL), raise(2, ABOVE)])
            .unwrap();
        let error = storage.apply_through(2).unwrap_err();
        assert!(
            matches!(
                error,
                MetaStoreError::NewerFeatureLevel {
                    directory_level: ABOVE,
                    supported: SUPPORTED_FEATURE_LEVEL,
                    ..
                }
            ),
            "{error}"
        );
        // Nothing was applied, not even the entry before the raise, and the
        // frontier did not move: a build that supports the level applies
        // the same entries.
        assert_eq!(storage.last_applied(), 0);
        assert_eq!(storage.state().feature_level(), BASELINE_FEATURE_LEVEL);
        assert_eq!(storage.log().last_index(), Some(2));
        drop(storage);
        // The directory still refuses at the next open only if the raise was
        // applied before: it was not, so a reopen is at the old frontier.
        let reopened = MetaStorage::open_in(&env, "/n1", cluster()).unwrap();
        assert_eq!(reopened.last_applied(), 0);
    }

    #[test]
    fn a_raise_to_this_builds_level_is_applied() {
        let (_sim, env) = sim();
        let mut storage = MetaStorage::open_in(&env, "/n1", cluster()).unwrap();
        storage.ensure_raft_applied_frontier().unwrap();
        storage
            .append(&[raise(1, SUPPORTED_FEATURE_LEVEL)])
            .unwrap();
        storage.apply_through(1).unwrap();
        assert_eq!(storage.state().feature_level(), SUPPORTED_FEATURE_LEVEL);
        drop(storage);
        let reopened = MetaStorage::open_in(&env, "/n1", cluster()).unwrap();
        assert_eq!(reopened.state().feature_level(), SUPPORTED_FEATURE_LEVEL);
    }

    #[test]
    fn installing_a_snapshot_above_this_builds_level_publishes_nothing() {
        let (_sim, env) = sim();
        let mut snapshots = MetaSnapshots::open_in(&env, "/n1", cluster()).unwrap();
        let payload = MetaStateMachine::at_feature_level_for_test(ABOVE)
            .encode_snapshot()
            .unwrap();
        let bytes = snapshot::encode_snapshot_file(
            cluster(),
            9,
            1,
            &MetaMembership::default(),
            "from-a-newer-leader",
            &payload,
            None,
        )
        .unwrap();
        let error = snapshots.install(&bytes).unwrap_err();
        assert!(
            matches!(
                error,
                MetaStoreError::NewerFeatureLevel {
                    directory_level: ABOVE,
                    supported: SUPPORTED_FEATURE_LEVEL,
                    ..
                }
            ),
            "{error}"
        );
        assert_eq!(snapshots.published_count(), 0);
        let reopened = MetaSnapshots::open_in(&env, "/n1", cluster()).unwrap();
        assert!(reopened.newest().is_none(), "the refused bytes were kept");
    }
}
