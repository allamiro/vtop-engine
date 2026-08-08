//! Receiver side of sealed-segment transfer (#270).
//!
//! # Verbatim, or not at all
//!
//! A transferred segment ships as the exact bytes on the leader's disk:
//! `.segment`, `.manifest.json`, and the `.producers` frontier it inherited.
//! Nothing is decoded and re-appended, because v1 folds `(producer_id,
//! producer_epoch)` into a derived storage id — a decode-and-reappend repair
//! would re-derive those ids and diverge from the leader silently, byte by
//! byte, while every offset still lined up. Byte-identity IS the correctness
//! mechanism, so this module never transforms received artifact bytes.
//!
//! # The receiver validates rather than trusts
//!
//! `.index` and `.chunks` deliberately do not cross the wire: they are
//! rebuildable caches, and shipping them would hand the receiver bytes it has
//! no way to distrust. The receiver rebuilds both from the received frames
//! and then runs the same verification `vtopctl segment verify --require
//! self` performs — over the STAGED files, before anything gets a real name.
//!
//! # A half-received segment must never look like a real one
//!
//! Received bytes land under `.{stem}.transfer-{artifact}.{uuid}.tmp` names.
//! [`crate::StartupCatalog`] classifies those as nothing at all — not as
//! artifacts, and deliberately not as `IncompleteAtomicWrite` either, which
//! quarantines and would leave a range refusing to open because a transfer
//! died. (This is why the names avoid the `write_atomic` markers the
//! classifier pattern-matches: a transfer is expected to die sometimes, and
//! its debris must be ignorable, not alarming.) Only after fsync and
//! verification do the artifacts rename into place, primary last, with a
//! directory fsync sealing the publication. Every byte moves through the
//! [`Env`] seam so the whole protocol is crash-sweepable.

use crate::env::{Env, OpenMode, StorageFile};
use crate::segment::{
    io_error, segment_stem, validate_sealed_and_rebuild_sidecars_at, SegmentPaths, SegmentReader,
};
use crate::verify::{verify_sealed_segment_at, VerifyExpectations, VerifyLevel};
use crate::{LogError, VtopLogResult};
use std::io::Write;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// The artifacts a sealed segment ships as; mirrors the wire enum in
/// `vtop-protocol` without depending on it, because this crate owns the disk
/// contract and the protocol crate owns the frame contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferArtifact {
    Segment,
    Manifest,
    Producers,
}

impl TransferArtifact {
    fn label(self) -> &'static str {
        match self {
            Self::Segment => "segment",
            Self::Manifest => "manifest",
            Self::Producers => "producers",
        }
    }

    fn slot(self) -> usize {
        match self {
            Self::Segment => 0,
            Self::Manifest => 1,
            Self::Producers => 2,
        }
    }
}

/// Absolute path of one transferable artifact of a sealed segment.
///
/// For the SENDER: the leader resolves what to serve from its sealed
/// readers' primary paths without duplicating the sidecar naming contract,
/// which lives here and nowhere else.
pub fn sealed_artifact_path(
    segment_path: &Path,
    artifact: TransferArtifact,
) -> VtopLogResult<PathBuf> {
    let paths = SegmentPaths::from_segment(segment_path)?;
    Ok(match artifact {
        TransferArtifact::Segment => paths.segment,
        TransferArtifact::Manifest => paths.manifest,
        TransferArtifact::Producers => paths.producers,
    })
}

/// What occupies the name a sealed segment would take in a receiver's
/// directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentPresence {
    /// Nothing there; stage and install.
    Absent,
    /// This exact segment, whole and verified. Skip it — this is what makes a
    /// resumed transfer cheap.
    Matching,
    /// Something else, or something unreadable. Discard it, then fetch.
    Foreign,
}

/// Lands transferred sealed segments in a directory it owns.
///
/// "Owns" is load-bearing: the sweep in [`Self::open`] deletes torn
/// publications on the grounds that everything here is re-fetchable from the
/// leader. That reasoning does not hold for a live range directory — a
/// mid-roll orphan `.producers` there is recovery state, not debris — so the
/// receiver refuses a directory containing an active segment outright.
/// Follower adoption (opening the received set and resuming replication) is
/// deliberately NOT built here; it is the next slice.
pub struct SegmentReceiver {
    env: Env,
    directory: PathBuf,
}

impl SegmentReceiver {
    /// Open a transfer destination, sweeping debris of earlier attempts.
    ///
    /// Two kinds of debris, both provably this mechanism's own:
    ///
    /// * `.{...}.transfer-{...}.{uuid}.tmp` staging files — a receive died
    ///   mid-artifact. Discovery ignores them, so they are harmless, but a
    ///   retried transfer should not accumulate them forever.
    /// * Real-named sidecars whose stem has NO primary — a publication died
    ///   between renames. The primary renames LAST precisely so this is the
    ///   only torn-publication shape, and it is safe to delete only because
    ///   this directory's contents are re-fetchable.
    pub fn open(env: &Env, directory: impl AsRef<Path>) -> VtopLogResult<Self> {
        let receiver = Self {
            env: env.clone(),
            directory: directory.as_ref().to_path_buf(),
        };
        receiver.sweep()?;
        Ok(receiver)
    }

    /// Where received segments land.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    fn sweep(&self) -> VtopLogResult<()> {
        let entries = self
            .env
            .storage
            .read_dir(&self.directory)
            .map_err(|source| io_error(&self.directory, source))?;
        // The ownership guard first: refuse to judge a live range at all.
        for entry in &entries {
            let Some(name) = entry.path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.ends_with(".active") {
                return Err(LogError::InvalidDescriptor(format!(
                    "transfer destination {} contains an active segment; a receiver only \
                     owns directories it fills, and sweeping a live range would delete \
                     recovery state",
                    self.directory.display()
                )));
            }
        }
        let mut sidecars: Vec<(String, PathBuf)> = Vec::new();
        let mut primaries: Vec<String> = Vec::new();
        for entry in &entries {
            let Some(name) = entry.path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.starts_with('.') && name.ends_with(".tmp") && name.contains(".transfer-") {
                self.env
                    .storage
                    .remove_file(&entry.path)
                    .map_err(|source| io_error(&entry.path, source))?;
                continue;
            }
            if let Some(stem) = name.strip_suffix(".segment") {
                primaries.push(stem.to_owned());
            } else if let Some(stem) = name
                .strip_suffix(".index")
                .or_else(|| name.strip_suffix(".chunks"))
                .or_else(|| name.strip_suffix(".manifest.json"))
                .or_else(|| name.strip_suffix(".producers"))
                .or_else(|| name.strip_suffix(".commit"))
            {
                sidecars.push((stem.to_owned(), entry.path.clone()));
            }
        }
        for (stem, path) in sidecars {
            if !primaries.contains(&stem) {
                self.env
                    .storage
                    .remove_file(&path)
                    .map_err(|source| io_error(&path, source))?;
            }
        }
        // Deletions durable before any new transfer builds on the cleaned
        // directory, mirroring `finish_truncation`.
        self.env
            .storage
            .sync_dir(&self.directory)
            .map_err(|source| io_error(&self.directory, source))
    }

    /// What, if anything, occupies the name the sender's segment would take.
    ///
    /// The primary is the completion witness because it renames last: its
    /// presence means the whole bundle published and verified.
    ///
    /// Three answers rather than a boolean, because "not the segment we want"
    /// and "nothing here" call for different actions and conflating them
    /// strands the repair. A caller that only learns "not complete" will stage
    /// the bytes and then be refused by [`StagedSegment::install`], which will
    /// not overwrite a sealed segment — so the very case this mechanism exists
    /// for, a follower whose directory holds a stale or torn segment at that
    /// offset, could never be repaired.
    pub fn presence(&self, base_offset: u64, segment_id: Uuid) -> VtopLogResult<SegmentPresence> {
        let path = self
            .directory
            .join(format!("{}.segment", segment_stem(base_offset)));
        if !self
            .env
            .storage
            .exists(&path)
            .map_err(|source| io_error(&path, source))?
        {
            return Ok(SegmentPresence::Absent);
        }
        match SegmentReader::open_in(&self.env, &path) {
            Ok(reader) if reader.segment_id() == segment_id => Ok(SegmentPresence::Matching),
            // Identity, not just the name. A segment at this base offset can
            // belong to a previous incarnation of the range — a different
            // `segment_id` after a rebuild, a lineage truncated and rewritten
            // — and treating "a file exists here" as "the requested segment is
            // here" would leave the receiver holding somebody else's history
            // while reporting a completed transfer.
            Ok(_) => Ok(SegmentPresence::Foreign),
            // Present but unopenable: a torn or half-installed artifact from
            // an interrupted run. Replace rather than reason about it.
            Err(_) => Ok(SegmentPresence::Foreign),
        }
    }

    /// Remove the whole bundle at `base_offset`, so a transfer can replace it.
    ///
    /// Only ever called for a [`SegmentPresence::Foreign`] bundle, and only in
    /// a directory the receiver owns — everything here is re-fetchable from
    /// the leader, which is the entire reason discarding is safe. It would not
    /// be in a live range.
    ///
    /// The PRIMARY goes first, the inverse of publication order. A crash
    /// part-way through then leaves sidecars without a primary, which is the
    /// one torn shape [`SegmentReceiver::open`]'s sweep already removes.
    /// Removing sidecars first would leave a primary whose frontier and index
    /// are gone — a segment that looks real to discovery and cannot be read.
    pub fn discard(&self, base_offset: u64) -> VtopLogResult<()> {
        let stem = segment_stem(base_offset);
        let mut names = vec![format!("{stem}.segment")];
        names.extend(
            ["index", "chunks", "manifest.json", "producers", "commit"]
                .into_iter()
                .map(|suffix| format!("{stem}.{suffix}")),
        );
        for name in names {
            let path = self.directory.join(name);
            if self
                .env
                .storage
                .exists(&path)
                .map_err(|source| io_error(&path, source))?
            {
                self.env
                    .storage
                    .remove_file(&path)
                    .map_err(|source| io_error(&path, source))?;
            }
        }
        self.env
            .storage
            .sync_dir(&self.directory)
            .map_err(|source| io_error(&self.directory, source))
    }

    /// Start staging one sealed segment.
    pub fn begin(&self, base_offset: u64) -> VtopLogResult<StagedSegment> {
        Ok(StagedSegment {
            env: self.env.clone(),
            directory: self.directory.clone(),
            stem: segment_stem(base_offset),
            staged: Default::default(),
            rebuilt: Vec::new(),
        })
    }
}

struct StagedArtifact {
    path: PathBuf,
    /// `None` once [`StagedSegment::finish_artifact`] has fsynced and closed.
    file: Option<Box<dyn StorageFile>>,
}

/// One sealed segment mid-receive. Dropping it without [`Self::install`]
/// abandons the staged bytes; they are ignorable by construction and the next
/// [`SegmentReceiver::open`] sweeps them.
pub struct StagedSegment {
    env: Env,
    directory: PathBuf,
    stem: String,
    staged: [Option<StagedArtifact>; 3],
    /// Staging paths this receiver creates ITSELF during `install`: the
    /// rebuilt `.index` / `.chunks` caches, and the synthesized frontier and
    /// commit names.
    ///
    /// Tracked separately from `staged` because nothing ever writes to them
    /// through this struct — they are named here and filled in by the rebuild
    /// — and an untracked file is one nothing cleans up. An install failing
    /// after the rebuild used to leave real, durable `.transfer-index` and
    /// `.transfer-chunks` bytes behind: invisible to discovery, so harmless to
    /// correctness, but accumulating for the life of the directory across
    /// every failed repair attempt.
    rebuilt: Vec<PathBuf>,
}

impl StagedSegment {
    fn staged_name(&self, artifact: TransferArtifact) -> String {
        // The `.transfer-` infix is what keeps these names OUTSIDE every
        // pattern the catalog classifier knows: not an artifact extension,
        // and not one of the `write_atomic` markers it quarantines as an
        // incomplete write. A transfer that dies must leave debris the range
        // can shrug at.
        format!(
            ".{}.transfer-{}.{}.tmp",
            self.stem,
            artifact.label(),
            Uuid::from_u128(self.env.rng.next_u128())
        )
    }

    /// Append received bytes to one artifact, verbatim.
    pub fn append_artifact(
        &mut self,
        artifact: TransferArtifact,
        bytes: &[u8],
    ) -> VtopLogResult<()> {
        let slot = artifact.slot();
        if self.staged[slot].is_none() {
            let path = self.directory.join(self.staged_name(artifact));
            let file = self
                .env
                .storage
                .open(&path, OpenMode::CreateNew)
                .map_err(|source| io_error(&path, source))?;
            self.staged[slot] = Some(StagedArtifact {
                path,
                file: Some(file),
            });
        }
        let staged = self.staged[slot].as_mut().expect("created above");
        let Some(file) = staged.file.as_mut() else {
            return Err(LogError::InvalidDescriptor(format!(
                "{} artifact was already finished; a transfer must not append past its \
                 durability point",
                artifact.label()
            )));
        };
        file.write_all(bytes)
            .map_err(|source| io_error(&staged.path, source))
    }

    /// Make one artifact's staged bytes durable and close it.
    pub fn finish_artifact(&mut self, artifact: TransferArtifact) -> VtopLogResult<()> {
        let staged = self.staged[artifact.slot()].as_mut().ok_or_else(|| {
            LogError::InvalidDescriptor(format!("{} artifact was never received", artifact.label()))
        })?;
        let Some(file) = staged.file.as_mut() else {
            return Err(LogError::InvalidDescriptor(format!(
                "{} artifact was already finished",
                artifact.label()
            )));
        };
        // SYNC FIRST, then drop the handle. `file.is_none()` is what `install`
        // reads as "these bytes are durable", so taking the handle before the
        // fsync succeeded made a FAILED sync indistinguishable from a
        // successful one: the error was returned, but a caller that retried was
        // told the artifact was "already finished", and publication would then
        // rename bytes no barrier had ever covered.
        //
        // Keeping the handle on failure also leaves the retry meaningful — the
        // same artifact can be synced again rather than being permanently
        // stuck in a state it cannot leave.
        file.sync_data()
            .map_err(|source| io_error(&staged.path, source))?;
        staged.file.take();
        Ok(())
    }

    /// Validate the staged bytes, rebuild the derived sidecars, and publish.
    ///
    /// Order inside, and why:
    ///
    /// 1. Every staged artifact must be finished — bytes that were never
    ///    fsynced have no business being renamed into a durability contract.
    /// 2. Rebuild `.index` / `.chunks` from the staged frames and run the
    ///    open-grade validation, then the full `verify --require self` check
    ///    set, ALL against the staged names. A failure here deletes the
    ///    staged files and the directory never learns the segment existed.
    /// 3. Rename into place: sidecars first, the `.segment` primary LAST,
    ///    then one directory fsync. The primary is the completion witness —
    ///    a crash anywhere in this window leaves sidecars without a primary,
    ///    which is exactly the shape [`SegmentReceiver::open`]'s sweep
    ///    removes, and never a bundle discovery would open.
    pub fn install(mut self) -> VtopLogResult<PathBuf> {
        let result = self.install_inner();
        if result.is_err() {
            self.remove_staged();
        }
        result
    }

    fn install_inner(&mut self) -> VtopLogResult<PathBuf> {
        for required in [TransferArtifact::Segment, TransferArtifact::Manifest] {
            match &self.staged[required.slot()] {
                Some(staged) if staged.file.is_none() => {}
                Some(_) => {
                    return Err(LogError::InvalidDescriptor(format!(
                        "{} artifact was staged but never finished",
                        required.label()
                    )))
                }
                None => {
                    return Err(LogError::InvalidDescriptor(format!(
                        "cannot install without the {} artifact",
                        required.label()
                    )))
                }
            }
        }
        if let Some(staged) = &self.staged[TransferArtifact::Producers.slot()] {
            if staged.file.is_some() {
                return Err(LogError::InvalidDescriptor(
                    "producers artifact was staged but never finished".to_owned(),
                ));
            }
        }
        let final_segment = self.directory.join(format!("{}.segment", self.stem));
        if self
            .env
            .storage
            .exists(&final_segment)
            .map_err(|source| io_error(&final_segment, source))?
        {
            // Same refusal as `seal`: a sealed segment is immutable, so a
            // second copy claiming its name is either redundant or a
            // conflict, and neither is this receiver's to resolve silently.
            return Err(LogError::InvalidDescriptor(format!(
                "refusing to replace existing sealed segment {}",
                final_segment.display()
            )));
        }

        let staged_path = |artifact: TransferArtifact| {
            self.staged[artifact.slot()]
                .as_ref()
                .map(|staged| staged.path.clone())
        };
        let received_segment = staged_path(TransferArtifact::Segment).expect("checked above");
        let received_manifest = staged_path(TransferArtifact::Manifest).expect("checked above");
        let received_producers = staged_path(TransferArtifact::Producers);
        // Rebuilt sidecars stage under the same ignorable naming; the commit
        // path is a name that never exists (sealed validation reads no
        // commit boundary), present only so the struct is total.
        //
        // Every path this block INVENTS is recorded in `rebuilt` before it can
        // be written to, so a failure anywhere below has something to delete.
        // Recording them at construction rather than after the rebuild is the
        // point: the rebuild is exactly what may fail part-way, and a path
        // registered only on success is registered only when it is not needed.
        let mut invented = |suffix: &str| {
            let path = self.directory.join(format!(
                ".{}.transfer-{suffix}.{}.tmp",
                self.stem,
                Uuid::from_u128(self.env.rng.next_u128())
            ));
            self.rebuilt.push(path.clone());
            path
        };
        let index = invented("index");
        let chunks = invented("chunks");
        let commit = invented("commit");
        let producers = match received_producers {
            Some(received) => received,
            None => invented("producers"),
        };
        let staged_paths = SegmentPaths {
            segment: received_segment,
            manifest: received_manifest,
            producers,
            index,
            chunks,
            commit,
        };

        // Open-grade validation plus sidecar rebuild, then the operator-grade
        // check set. Both run against staged names so nothing unvalidated is
        // ever visible under a real one.
        validate_sealed_and_rebuild_sidecars_at(&self.env, &staged_paths)?;
        let report = verify_sealed_segment_at(
            &self.env,
            &staged_paths,
            &VerifyExpectations {
                require: VerifyLevel::SelfConsistent,
                ..VerifyExpectations::default()
            },
        )?;
        if !report.passed() {
            let failed = report
                .checks
                .iter()
                .filter(|check| !check.passed)
                .map(|check| format!("{}: {}", check.name, check.detail))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(LogError::InvalidDescriptor(format!(
                "transferred segment failed verification and was not installed: {failed}"
            )));
        }

        // Publication. The inherited frontier goes first for the same reason
        // `roll_in` writes it before the successor: a primary must never be
        // visible without the frontier that makes it readable. The primary
        // goes last as the completion witness.
        let mut renames: Vec<(PathBuf, PathBuf)> = Vec::new();
        let has_producers = self
            .env
            .storage
            .exists(&staged_paths.producers)
            .map_err(|source| io_error(&staged_paths.producers, source))?;
        if has_producers {
            renames.push((
                staged_paths.producers.clone(),
                self.directory.join(format!("{}.producers", self.stem)),
            ));
        }
        renames.push((
            staged_paths.index.clone(),
            self.directory.join(format!("{}.index", self.stem)),
        ));
        let has_chunks = self
            .env
            .storage
            .exists(&staged_paths.chunks)
            .map_err(|source| io_error(&staged_paths.chunks, source))?;
        if has_chunks {
            renames.push((
                staged_paths.chunks.clone(),
                self.directory.join(format!("{}.chunks", self.stem)),
            ));
        }
        renames.push((
            staged_paths.manifest.clone(),
            self.directory.join(format!("{}.manifest.json", self.stem)),
        ));
        renames.push((staged_paths.segment.clone(), final_segment.clone()));
        // A rename that fails part-way leaves the sidecars ahead of it under
        // REAL names. A crash there is fine — the primary renames last, so
        // discovery sees orphan sidecars and the next `open` sweeps them — but
        // an in-process failure has no such sweep coming, and `install`
        // reports failure while the directory keeps the debris. So the
        // destinations already claimed are released here, in reverse, before
        // the error is returned.
        //
        // Reverse order is the same reasoning as everywhere else in this file:
        // it walks back toward the state the directory was in, never through
        // one where a primary exists without what makes it readable.
        let mut published: Vec<PathBuf> = Vec::new();
        for (from, to) in renames {
            if let Err(source) = self.env.storage.rename(&from, &to) {
                let failure = io_error(&to, source);
                for claimed in published.into_iter().rev() {
                    let _ = self.env.storage.remove_file(&claimed);
                }
                let _ = self.env.storage.sync_dir(&self.directory);
                return Err(failure);
            }
            published.push(to);
        }
        self.env
            .storage
            .sync_dir(&self.directory)
            .map_err(|source| io_error(&self.directory, source))?;
        // Renames consumed the staged files; nothing is left to clean. The
        // rebuilt list is cleared too — its entries were either renamed above
        // or are names the rebuild never created (`commit` always, `chunks`
        // for v1, `producers` for a first segment), and deleting a path just
        // published under a real name would undo the publication.
        self.staged = Default::default();
        self.rebuilt.clear();
        Ok(final_segment)
    }

    /// Delete the staged bytes without publishing anything.
    pub fn abort(mut self) {
        self.remove_staged();
    }

    fn remove_staged(&mut self) {
        for staged in self.staged.iter_mut().flatten() {
            // Close before removing; some storage backends refuse to unlink
            // an open file.
            staged.file.take();
            let _ = self.env.storage.remove_file(&staged.path);
        }
        // The sidecars this receiver rebuilt for itself. Most of these names
        // never became files — the errors ignored here are overwhelmingly "no
        // such file", which is the outcome being aimed at anyway.
        for path in self.rebuilt.drain(..) {
            let _ = self.env.storage.remove_file(&path);
        }
        self.staged = Default::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Durability, KeyRange, LogRecord, RangeLineage, SegmentConfig, SegmentDescriptor,
        SegmentReader, SegmentSet, StartupCatalog,
    };
    use std::fs;
    use tempfile::tempdir;

    fn descriptor(segment_id: u128) -> SegmentDescriptor {
        SegmentDescriptor {
            segment_id: Uuid::from_u128(segment_id),
            topic: "events.v1".to_owned(),
            topic_epoch: 7,
            lineage: RangeLineage {
                range_id: Uuid::from_u128(2),
                generation: 0,
                key_range: KeyRange::full(),
                parents: Vec::new(),
            },
            base_offset: 0,
        }
    }

    fn config() -> SegmentConfig {
        SegmentConfig {
            max_record_bytes: 1024,
            max_group_bytes: 4096,
            max_segment_bytes: 16 * 1024,
            max_segment_records: 100,
            index_stride: 2,
        }
    }

    fn record(producer: u128, sequence: u64, value: &[u8]) -> LogRecord {
        LogRecord {
            producer_id: Uuid::from_u128(producer),
            producer_epoch: 0,
            sequence,
            timestamp_millis: 1_700_000_000_000 + sequence as i64,
            attributes: 0,
            key: b"key".to_vec(),
            value: value.to_vec(),
        }
    }

    /// A leader-shaped range: rolled a few times, with one producer's
    /// sequences crossing every boundary so the sealed prefix genuinely
    /// depends on its `.producers` frontiers.
    fn rolled_range(directory: &Path) -> SegmentSet {
        let env = Env::real();
        let mut set = SegmentSet::create_in(&env, directory, descriptor(1), config()).unwrap();
        for sequence in 0..48 {
            set.append_group_minting(&[record(10, sequence, &vec![b'x'; 900])], Durability::Fsync)
                .unwrap();
        }
        assert!(
            set.sealed().len() >= 2,
            "the fixture must roll at least twice, got {}",
            set.sealed().len()
        );
        set
    }

    fn transfer_all(source: &SegmentSet, receiver: &SegmentReceiver) -> Vec<PathBuf> {
        let mut installed = Vec::new();
        for reader in source.sealed() {
            let mut staged = receiver.begin(reader.base_offset()).unwrap();
            for artifact in [
                TransferArtifact::Producers,
                TransferArtifact::Segment,
                TransferArtifact::Manifest,
            ] {
                let path = sealed_artifact_path(reader.path(), artifact).unwrap();
                if !path.exists() {
                    assert_eq!(
                        artifact,
                        TransferArtifact::Producers,
                        "only the frontier may be absent"
                    );
                    continue;
                }
                let bytes = fs::read(&path).unwrap();
                // Chunked feed, to exercise the append path the wire uses.
                for chunk in bytes.chunks(64) {
                    staged.append_artifact(artifact, chunk).unwrap();
                }
                staged.finish_artifact(artifact).unwrap();
            }
            installed.push(staged.install().unwrap());
        }
        installed
    }

    #[test]
    fn transferred_prefix_is_byte_identical_verified_and_discoverable() {
        let source_dir = tempdir().unwrap();
        let destination = tempdir().unwrap();
        let set = rolled_range(source_dir.path());
        let receiver = SegmentReceiver::open(&Env::real(), destination.path()).unwrap();

        let installed = transfer_all(&set, &receiver);
        assert_eq!(installed.len(), set.sealed().len());

        for reader in set.sealed() {
            let stem = segment_stem(reader.base_offset());
            for name in [
                format!("{stem}.segment"),
                format!("{stem}.manifest.json"),
                format!("{stem}.producers"),
            ] {
                let source = source_dir.path().join(&name);
                let received = destination.path().join(&name);
                if !source.exists() {
                    assert!(!received.exists(), "{name} must not be invented");
                    continue;
                }
                // Byte identity is the correctness mechanism, so it is
                // asserted as a hash of both sides, not inferred from sizes.
                assert_eq!(
                    blake3::hash(&fs::read(&source).unwrap()),
                    blake3::hash(&fs::read(&received).unwrap()),
                    "{name} must ship verbatim"
                );
            }
            // Rebuilt receiver-side, never shipped.
            assert!(destination.path().join(format!("{stem}.index")).exists());
            // And the rebuilt sidecars verify: the reader opens, which
            // re-validates everything against the manifest.
            let received =
                SegmentReader::open(destination.path().join(format!("{stem}.segment"))).unwrap();
            assert_eq!(received.segment_id(), reader.segment_id());
            assert_eq!(received.next_offset(), reader.next_offset());
        }

        // The received directory is a valid catalog with nothing quarantined.
        let catalog = StartupCatalog::discover(destination.path()).unwrap();
        assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);
        assert_eq!(catalog.entries.len(), set.sealed().len());
    }

    /// Killing a receive mid-artifact must leave the directory clean: the
    /// staged names are invisible to discovery, and the next receiver sweeps
    /// them.
    #[test]
    fn a_torn_receive_leaves_no_quarantine_and_is_swept() {
        let source_dir = tempdir().unwrap();
        let destination = tempdir().unwrap();
        let set = rolled_range(source_dir.path());
        let receiver = SegmentReceiver::open(&Env::real(), destination.path()).unwrap();

        let reader = &set.sealed()[0];
        let mut staged = receiver.begin(reader.base_offset()).unwrap();
        let bytes = fs::read(reader.path()).unwrap();
        staged
            .append_artifact(TransferArtifact::Segment, &bytes[..bytes.len() / 2])
            .unwrap();
        // Kill: drop without finish, abort, or install.
        drop(staged);

        let leftovers = fs::read_dir(destination.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(leftovers.len(), 1, "{leftovers:?}");
        assert!(leftovers[0].contains(".transfer-segment."), "{leftovers:?}");

        let catalog = StartupCatalog::discover(destination.path()).unwrap();
        assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);
        assert!(catalog.entries.is_empty());

        // A fresh receiver sweeps the debris and the transfer succeeds.
        let receiver = SegmentReceiver::open(&Env::real(), destination.path()).unwrap();
        assert!(fs::read_dir(destination.path()).unwrap().next().is_none());
        let installed = transfer_all(&set, &receiver);
        assert_eq!(installed.len(), set.sealed().len());
    }

    /// A torn PUBLICATION — sidecars renamed, primary not — is the one shape
    /// a crash inside `install` can leave, and the sweep removes it.
    #[test]
    fn a_torn_publication_is_swept_because_the_primary_renames_last() {
        let destination = tempdir().unwrap();
        let stem = segment_stem(0);
        for name in [
            format!("{stem}.manifest.json"),
            format!("{stem}.producers"),
            format!("{stem}.index"),
        ] {
            fs::write(destination.path().join(name), b"torn").unwrap();
        }
        // Without the sweep this is OrphanSidecars — visible, but stuck.
        let catalog = StartupCatalog::discover(destination.path()).unwrap();
        assert_eq!(catalog.quarantined.len(), 1);

        let _receiver = SegmentReceiver::open(&Env::real(), destination.path()).unwrap();
        assert!(fs::read_dir(destination.path()).unwrap().next().is_none());
    }

    /// The sweep must never judge a live range: an `.active` file means this
    /// is not a transfer destination, whatever else it looks like.
    #[test]
    fn the_receiver_refuses_a_directory_with_an_active_segment() {
        let directory = tempdir().unwrap();
        let env = Env::real();
        drop(SegmentSet::create_in(&env, directory.path(), descriptor(1), config()).unwrap());
        let error = match SegmentReceiver::open(&env, directory.path()) {
            Err(error) => error,
            Ok(_) => panic!("a live range must be refused"),
        };
        assert!(error.to_string().contains("active segment"), "{error}");
    }

    /// Tampered bytes must fail verification and leave the directory clean:
    /// the receiver validates rather than trusts.
    #[test]
    fn tampered_segment_bytes_are_refused_and_nothing_is_published() {
        let source_dir = tempdir().unwrap();
        let destination = tempdir().unwrap();
        let set = rolled_range(source_dir.path());
        let receiver = SegmentReceiver::open(&Env::real(), destination.path()).unwrap();

        let reader = &set.sealed()[0];
        let mut staged = receiver.begin(reader.base_offset()).unwrap();
        let mut bytes = fs::read(reader.path()).unwrap();
        let flip = bytes.len() - 10;
        bytes[flip] ^= 1;
        staged
            .append_artifact(TransferArtifact::Segment, &bytes)
            .unwrap();
        staged.finish_artifact(TransferArtifact::Segment).unwrap();
        for artifact in [TransferArtifact::Manifest, TransferArtifact::Producers] {
            let path = sealed_artifact_path(reader.path(), artifact).unwrap();
            if path.exists() {
                staged
                    .append_artifact(artifact, &fs::read(path).unwrap())
                    .unwrap();
                staged.finish_artifact(artifact).unwrap();
            }
        }
        staged.install().unwrap_err();

        assert!(
            fs::read_dir(destination.path()).unwrap().next().is_none(),
            "a refused install must leave nothing behind"
        );
    }

    /// An install that fails AFTER the sidecar rebuild must leave nothing —
    /// including the `.index` and `.chunks` the receiver rebuilt for ITSELF.
    ///
    /// The tampered-bytes test above fails during open-grade validation, which
    /// is BEFORE anything is rebuilt; it passed just as happily while those
    /// files were untracked, because at that point there were none to leak.
    /// The leak only exists past that point, so the failure has to be injected
    /// past it: at the first PUBLICATION rename, with every rebuilt sidecar on
    /// disk under a staging name and nothing yet published.
    ///
    /// Two identical simulated runs, same seed and so the same operation
    /// sequence. The first learns which global op index that rename is; the
    /// second fails exactly that op.
    #[test]
    fn an_install_failing_after_the_rebuild_removes_the_rebuilt_sidecars_too() {
        use crate::sim::{FaultPlan, SimStorage, TraceKind};

        let source_dir = tempdir().unwrap();
        let set = rolled_range(source_dir.path());
        let reader = &set.sealed()[0];
        let base_offset = reader.base_offset();
        let artifacts: Vec<(TransferArtifact, Vec<u8>)> = [
            TransferArtifact::Segment,
            TransferArtifact::Manifest,
            TransferArtifact::Producers,
        ]
        .into_iter()
        .filter_map(|artifact| {
            let path = sealed_artifact_path(reader.path(), artifact).ok()?;
            path.exists().then(|| (artifact, fs::read(&path).unwrap()))
        })
        .collect();

        let destination = Path::new("/transfer");
        let stage = |sim: &SimStorage| {
            sim.create_dir_all(destination);
            let env = sim.env(11);
            let receiver = SegmentReceiver::open(&env, destination).unwrap();
            let mut staged = receiver.begin(base_offset).unwrap();
            for (artifact, bytes) in &artifacts {
                staged.append_artifact(*artifact, bytes).unwrap();
                staged.finish_artifact(*artifact).unwrap();
            }
            (env, staged)
        };

        // Run one: unobstructed, to learn where publication begins.
        //
        // Not every rename is a publication. `write_atomic` renames its own
        // temp into place while REBUILDING a sidecar, and those come first;
        // failing one of those would abort inside the rebuild and prove
        // nothing. They are distinguishable by name: a `write_atomic` temp is
        // ".{target}.{uuid}.tmp" over a target that is already a dotted
        // staging name, so it is the only rename source beginning "..".
        let rehearsal = SimStorage::new();
        let (_, staged) = stage(&rehearsal);
        staged.install().unwrap();
        let first_publication = rehearsal
            .trace()
            .into_iter()
            .filter(|entry| entry.kind == TraceKind::Rename)
            .find(|entry| {
                !entry
                    .path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(".."))
            })
            .expect("a successful install publishes by renaming")
            .index;

        // Run two: the same script, with that rename failing.
        let injected = SimStorage::new();
        let (env, staged) = stage(&injected);
        injected.set_fault(FaultPlan::FailOp {
            op: first_publication,
            kind: std::io::ErrorKind::PermissionDenied,
        });
        staged
            .install()
            .expect_err("the injected rename failure must fail the install");

        let leftovers = env
            .storage
            .read_dir(destination)
            .unwrap()
            .into_iter()
            .map(|entry| entry.path.display().to_string())
            .collect::<Vec<_>>();
        assert!(
            leftovers.is_empty(),
            "a failed install must not leave rebuilt sidecars behind: {leftovers:?}"
        );
    }

    /// A rename failing PART-WAY through publication releases the names it
    /// already claimed.
    ///
    /// The crash case is already safe and stays that way: the primary renames
    /// last, so a crash leaves orphan sidecars, and the next
    /// [`SegmentReceiver::open`] sweeps them. An in-process failure has no
    /// such sweep coming — `install` returns an error and the caller carries
    /// on — so the directory would keep real, discoverable `{stem}.index` and
    /// `{stem}.manifest.json` files for a segment that was never published,
    /// while `install` reported that nothing happened.
    ///
    /// Injected at the LAST rename, which is the primary: the strongest form,
    /// with every other destination already claimed.
    #[test]
    fn a_publication_failing_part_way_releases_the_names_it_already_claimed() {
        use crate::sim::{FaultPlan, SimStorage, TraceKind};

        let source_dir = tempdir().unwrap();
        let set = rolled_range(source_dir.path());
        let reader = &set.sealed()[0];
        let base_offset = reader.base_offset();
        let artifacts: Vec<(TransferArtifact, Vec<u8>)> = [
            TransferArtifact::Segment,
            TransferArtifact::Manifest,
            TransferArtifact::Producers,
        ]
        .into_iter()
        .filter_map(|artifact| {
            let path = sealed_artifact_path(reader.path(), artifact).ok()?;
            path.exists().then(|| (artifact, fs::read(&path).unwrap()))
        })
        .collect();

        let destination = Path::new("/transfer");
        let stage = |sim: &SimStorage| {
            sim.create_dir_all(destination);
            let env = sim.env(11);
            let receiver = SegmentReceiver::open(&env, destination).unwrap();
            let mut staged = receiver.begin(base_offset).unwrap();
            for (artifact, bytes) in &artifacts {
                staged.append_artifact(*artifact, bytes).unwrap();
                staged.finish_artifact(*artifact).unwrap();
            }
            (env, staged)
        };

        let rehearsal = SimStorage::new();
        let (_, staged) = stage(&rehearsal);
        staged.install().unwrap();
        // Publication renames only — a `write_atomic` temp is the one rename
        // source beginning "..", see the test above.
        let publications: Vec<u64> = rehearsal
            .trace()
            .into_iter()
            .filter(|entry| entry.kind == TraceKind::Rename)
            .filter(|entry| {
                !entry
                    .path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(".."))
            })
            .map(|entry| entry.index)
            .collect();
        assert!(
            publications.len() >= 2,
            "the test needs an earlier rename to have already succeeded, got {publications:?}"
        );

        let injected = SimStorage::new();
        let (env, staged) = stage(&injected);
        injected.set_fault(FaultPlan::FailOp {
            op: *publications.last().expect("checked above"),
            kind: std::io::ErrorKind::PermissionDenied,
        });
        staged
            .install()
            .expect_err("the injected rename failure must fail the install");

        let leftovers = env
            .storage
            .read_dir(destination)
            .unwrap()
            .into_iter()
            .map(|entry| entry.path.display().to_string())
            .collect::<Vec<_>>();
        assert!(
            leftovers.is_empty(),
            "a failed publication must not leave real sidecars behind: {leftovers:?}"
        );
    }

    /// The v2 path end to end: a rolled v2 range verifies (the manifest's
    /// producer summary is CUMULATIVE across rolls, which the seeded verifier
    /// must reconstruct) and transfers, with the receiver rebuilding the
    /// `.chunks` sidecar it never shipped.
    #[test]
    fn a_rolled_v2_range_verifies_and_transfers_with_rebuilt_chunks() {
        let source_dir = tempdir().unwrap();
        let destination = tempdir().unwrap();
        let active = crate::ActiveSegment::create_v2(
            source_dir
                .path()
                .join(format!("{}.active", segment_stem(0))),
            crate::SegmentDescriptorV2 {
                segment_id: Uuid::from_u128(21),
                topic: "events.v1".to_owned(),
                topic_epoch: 7,
                lineage: RangeLineage {
                    range_id: Uuid::from_u128(2),
                    generation: 0,
                    key_range: KeyRange::full(),
                    parents: Vec::new(),
                },
                base_offset: 0,
                segment_generation: 3,
                creation_node_id: Uuid::from_u128(500),
                creation_fencing_epoch: 1,
            },
            crate::SegmentConfigV2 {
                max_record_bytes: 1024,
                max_group_bytes: 4096,
                max_segment_bytes: 16 * 1024,
                max_segment_records: 100,
                index_stride: 2,
                chunk_size: 64 * 1024,
            },
        )
        .unwrap();
        let mut set = SegmentSet::from(active);
        for sequence in 0..48 {
            set.append_group_minting(
                &[LogRecord {
                    producer_epoch: 2,
                    ..record(10, sequence, &vec![b'x'; 900])
                }],
                Durability::Fsync,
            )
            .unwrap();
        }
        assert!(set.sealed().len() >= 2, "got {}", set.sealed().len());

        for reader in set.sealed() {
            let report = crate::verify::verify_sealed_segment(
                reader.path(),
                &VerifyExpectations {
                    require: VerifyLevel::SelfConsistent,
                    ..VerifyExpectations::default()
                },
            )
            .unwrap();
            assert!(
                report.passed(),
                "v2 segment at {} failed: {:?}",
                reader.base_offset(),
                report
                    .checks
                    .iter()
                    .filter(|check| !check.passed)
                    .collect::<Vec<_>>()
            );
        }

        let receiver = SegmentReceiver::open(&Env::real(), destination.path()).unwrap();
        let installed = transfer_all(&set, &receiver);
        assert_eq!(installed.len(), set.sealed().len());
        for reader in set.sealed() {
            let stem = segment_stem(reader.base_offset());
            // Never shipped, rebuilt receiver-side — and byte-identical to
            // the leader's copy, since both derive from the same frames.
            let chunks = destination.path().join(format!("{stem}.chunks"));
            assert!(chunks.exists(), "v2 receiver must rebuild {stem}.chunks");
            assert_eq!(
                blake3::hash(&fs::read(source_dir.path().join(format!("{stem}.chunks"))).unwrap()),
                blake3::hash(&fs::read(&chunks).unwrap()),
            );
        }
        let catalog = StartupCatalog::discover(destination.path()).unwrap();
        assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);
        assert_eq!(catalog.entries.len(), set.sealed().len());
        assert!(catalog
            .entries
            .iter()
            .all(|entry| entry.format_version == 2));
    }

    /// A damaged `.producers` is REPORTED, not raised.
    ///
    /// Verification's product is a verdict on artifacts that may be damaged —
    /// that is the entire job — so returning `Err` when one of them is
    /// damaged left the caller with no verdict at all: `vtopctl segment
    /// verify` printed a decode error instead of a report, and the transfer
    /// receiver lost the per-check detail naming which artifact the peer sent
    /// broken.
    #[test]
    fn a_damaged_producer_frontier_is_a_finding_not_an_error() {
        let source_dir = tempdir().unwrap();
        let set = rolled_range(source_dir.path());
        // The SECOND sealed segment: the first inherits nothing and so has no
        // frontier to damage.
        let reader = &set.sealed()[1];
        let frontier = sealed_artifact_path(reader.path(), TransferArtifact::Producers).unwrap();
        assert!(
            frontier.exists(),
            "a rolled segment must have inherited one"
        );
        // Truncated mid-header: decodable magic, nothing after it.
        fs::write(&frontier, &fs::read(&frontier).unwrap()[..9]).unwrap();

        let report = crate::verify::verify_sealed_segment(
            reader.path(),
            &VerifyExpectations {
                require: VerifyLevel::SelfConsistent,
                ..VerifyExpectations::default()
            },
        )
        .expect("damage in an input is a finding, not a reason to have no report");
        assert!(!report.passed(), "a damaged frontier must not pass");
        let frontier_check = report
            .checks
            .iter()
            .find(|check| check.name == crate::verify::CHECK_PRODUCER_FRONTIER)
            .expect("the report must name the frontier explicitly");
        assert!(!frontier_check.passed, "{}", frontier_check.detail);
        // And the consequence is reported too, rather than only the cause: a
        // segment read without its frontier has sequences beginning nowhere.
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == crate::verify::CHECK_FRAME_SCAN && !check.passed),
            "the scan must also report what reading without the frontier costs"
        );
    }

    /// A rolled segment — producers mid-sequence, frontier in `.producers` —
    /// passes the offline verifier. Pinned here because the verifier predates
    /// rolling and used to demand every producer start at zero, which would
    /// have made the receiver reject every rolled segment a transfer exists
    /// to ship.
    #[test]
    fn the_offline_verifier_accepts_a_rolled_segment() {
        let source_dir = tempdir().unwrap();
        let set = rolled_range(source_dir.path());
        for reader in set.sealed() {
            let report = crate::verify::verify_sealed_segment(
                reader.path(),
                &VerifyExpectations {
                    require: VerifyLevel::SelfConsistent,
                    ..VerifyExpectations::default()
                },
            )
            .unwrap();
            assert!(
                report.passed(),
                "segment at {} failed: {:?}",
                reader.base_offset(),
                report
                    .checks
                    .iter()
                    .filter(|check| !check.passed)
                    .collect::<Vec<_>>()
            );
        }
    }
}
