//! A range as a SEQUENCE of segments: sealed prefixes plus one active tail.
//!
//! # Why a range stopped being one file
//!
//! It was one `range.active` growing for the life of the process, which meant
//! there was never an immutable prefix. Everything that needs one was blocked
//! behind that: transferring a segment to repair a replica that fell too far
//! behind to catch up, retention (nothing can be deleted from a file still
//! being appended to), and naming the segments either side of a leadership
//! transition.
//!
//! # What this type is responsible for
//!
//! Routing. A read names an offset, and exactly one segment holds it; a write
//! always lands on the tail, which rolls when it reaches its bound. Callers see
//! a range, not a filesystem layout.
//!
//! # Reads cross boundaries; writes never do
//!
//! A fetch that begins in a sealed segment continues into the next one until it
//! reaches the caller's limits or the high-water mark. Stopping at a boundary
//! would be correct but would make every reader responsible for knowing the
//! layout, and a consumer asking for 500 records from offset 0 would silently
//! get however many happened to fit in the first segment.
//!
//! A write never spans a roll: the batch either fits in the tail or the tail
//! rolls first and the whole batch lands in the successor. A group split across
//! two segments would have part of a producer's commit group on either side of
//! a boundary, and the two halves could then be transferred, truncated, or
//! retained independently.

use crate::env::Env;
use crate::producer_snapshot::ProducerSnapshot;
use crate::retention_intent::{RetainedSegment, RetentionIntent, RETENTION_INTENT_FILE};
use crate::segment::{
    io_error, rewrite_empty_header_in_place, roll_in, roll_in_with, segment_stem, write_atomic,
    ActiveSegment, SegmentReader, SuccessorConfig,
};
use crate::truncate_intent::{DoomedSegment, TruncateIntent, TRUNCATE_INTENT_FILE};
use crate::{
    CatalogEntry, CatalogSegmentState, Durability, FetchBatch, FetchedRecord, LogError, LogRecord,
    RollThresholds, SegmentDescriptor, StartupCatalog, VtopLogResult,
};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Finish a truncation that died between its marker and its final rename.
///
/// Shared by [`SegmentSet::open_in`] and [`SegmentSet::adopt_in`] because both
/// read a layout, and reading one with an unfinished truncation in it sees the
/// half-made range rather than the completed one. Adoption skipping this was a
/// way to LOSE WRITES, not merely to read something stale: it would mint a tail
/// over a prefix the marker still condemns, serve appends into it, and the next
/// `open_in` would finish the intent and delete exactly that tail.
///
/// A marker that does not decode is deliberately left alone. It names an intent
/// that cannot be honoured, so it falls through to discovery, which quarantines
/// it by name and makes the caller refuse — which is the right outcome for a
/// range nobody can prove the shape of.
fn finish_pending_truncation(env: &Env, directory: &Path) -> VtopLogResult<()> {
    let marker = directory.join(TRUNCATE_INTENT_FILE);
    if !env
        .storage
        .exists(&marker)
        .map_err(|source| io_error(&marker, source))?
    {
        return Ok(());
    }
    let decoded = env
        .storage
        .read(&marker)
        .map_err(|source| io_error(&marker, source))
        .and_then(|bytes| TruncateIntent::decode(&bytes));
    if let Ok(intent) = decoded {
        finish_truncation(env, directory, &intent)?;
    }
    Ok(())
}

/// Finish a sealed-prefix retention that a crash interrupted (#290).
///
/// Same contract as [`finish_pending_truncation`]: a decodable marker is
/// completed, an undecodable one is deliberately left for discovery to
/// quarantine by name — deleting segments on the strength of bytes that were
/// not written whole by this code is exactly what the checksum exists to
/// prevent.
fn finish_pending_retention(env: &Env, directory: &Path) -> VtopLogResult<()> {
    let marker = directory.join(RETENTION_INTENT_FILE);
    if !env
        .storage
        .exists(&marker)
        .map_err(|source| io_error(&marker, source))?
    {
        return Ok(());
    }
    let decoded = env
        .storage
        .read(&marker)
        .map_err(|source| io_error(&marker, source))
        .and_then(|bytes| RetentionIntent::decode(&bytes));
    if let Ok(intent) = decoded {
        finish_retention(env, directory, &intent)?;
    }
    Ok(())
}

/// The deletion half of a retention: idempotent, so it serves both the live
/// path and crash recovery.
///
/// Deletes every doomed stem's files (present or not), makes the unlinks
/// durable, then removes the marker — in that order, so a crash at any point
/// leaves either a marker whose work can be re-run or a directory with no
/// trace of the retention. The doomed segments' `.producers` sidecars go
/// with them: a sealed segment's frontier is inherited by its successor's
/// sidecar at roll time, so the oldest SURVIVING segment already carries
/// everything the deleted prefix contributed.
fn finish_retention(env: &Env, directory: &Path, intent: &RetentionIntent) -> VtopLogResult<()> {
    for doomed in &intent.doomed {
        remove_segment_files(env, directory, doomed.base_offset)?;
    }
    env.storage
        .sync_dir(directory)
        .map_err(|source| io_error(directory, source))?;
    let marker = directory.join(RETENTION_INTENT_FILE);
    if env
        .storage
        .exists(&marker)
        .map_err(|source| io_error(&marker, source))?
    {
        env.storage
            .remove_file(&marker)
            .map_err(|source| io_error(&marker, source))?;
        env.storage
            .sync_dir(directory)
            .map_err(|source| io_error(directory, source))?;
    }
    Ok(())
}

/// What a range keeps, expressed as a disk bound (#290).
///
/// Size only, deliberately: an age bound needs a durable per-segment seal
/// time, which no manifest version records yet — adding one is a format
/// change that belongs to its own slice, not a side effect of this one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Total bytes of encoded record frames the range may hold (sealed
    /// content plus the active tail's current frames) before the oldest
    /// sealed segments become eligible for reclamation.
    pub max_total_bytes: u64,
}

/// What one retention pass reclaimed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionOutcome {
    pub segments_removed: usize,
    pub bytes_removed: u64,
}

/// Every segment in a range directory must belong to the SAME range.
///
/// Discovery does not cover this. `mark_conflicting_lineage` groups candidates
/// BY range and flags disagreement within a group, so segments from two
/// different ranges are two clean groups and nothing is quarantined. Offsets
/// alone then look fine — two ranges can trivially hold contiguous offsets —
/// and a set stitched from both would serve one range's records under the
/// other's identity, with the tail inheriting whichever descriptor happened to
/// be last.
///
/// The v1-shaped catalog descriptor is the right thing to compare: a v2
/// descriptor projects onto its common prefix, so this is format-independent.
fn validate_single_lineage(entries: &[CatalogEntry], directory: &Path) -> VtopLogResult<()> {
    let mut first: Option<&SegmentDescriptor> = None;
    for entry in entries {
        let descriptor = &entry.descriptor;
        match first {
            None => first = Some(descriptor),
            Some(first)
                if first.topic == descriptor.topic
                    && first.topic_epoch == descriptor.topic_epoch
                    && first.lineage.range_id == descriptor.lineage.range_id
                    && first.lineage.generation == descriptor.lineage.generation
                    && first.lineage.key_range == descriptor.lineage.key_range => {}
            Some(first) => {
                return Err(LogError::InvalidDescriptor(format!(
                    "range at {} mixes lineages: {} belongs to topic {:?} epoch {} range {}/{} \
                     while {} belongs to topic {:?} epoch {} range {}/{}. These are different \
                     ranges and must not be served as one.",
                    directory.display(),
                    entry.path.display(),
                    descriptor.topic,
                    descriptor.topic_epoch,
                    descriptor.lineage.range_id,
                    descriptor.lineage.generation,
                    "the first segment",
                    first.topic,
                    first.topic_epoch,
                    first.lineage.range_id,
                    first.lineage.generation,
                )))
            }
        }
    }
    Ok(())
}

/// What [`SegmentSet::reconfigure`] did (#314). Three distinct outcomes,
/// because the operator's next question differs for each: an unchanged
/// range needs no verification, a rolled one has a new sealed segment to
/// account for, and a rewritten tail has the same file count it started
/// with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconfigureOutcome {
    /// Every requested threshold already matched the tail's header.
    Unchanged,
    /// The tail was sealed and a successor opened under the new limits,
    /// beginning at `successor_base`.
    Rolled { successor_base: u64 },
    /// The tail held no records, so its header was rewritten in place —
    /// rolling would have sealed an empty segment.
    RewrittenInPlace,
}

/// One range's segments, ordered and contiguous.
pub struct SegmentSet {
    env: Env,
    directory: PathBuf,
    /// Sealed segments in ascending offset order, contiguous with each other
    /// and with `active`.
    sealed: Vec<SegmentReader>,
    /// `Option` only so the tail can be moved out by value: during a roll,
    /// which `roll_in` requires, and during a cross-segment truncation, which
    /// deletes the file the tail has open. It is `None` for the duration of
    /// those calls — and permanently if either fails part-way, which every
    /// accessor treats as a programming error rather than a state to serve
    /// from. An earlier version parked a throwaway segment in the range
    /// directory instead, and its orphaned sidecars made discovery quarantine
    /// the whole range on the next open.
    active: Option<ActiveSegment>,
}

/// The file a repair writes when it finds a destination diverged from its
/// source, and the reason any opener must refuse the range.
///
/// WRITTEN BY `vtopctl node repair`, ENFORCED HERE. Divergence is detected only
/// after the transferred prefix has been adopted, so a condemned directory is
/// structurally indistinguishable from a healthy one — contiguous sealed
/// segments and a live tail. Nothing in the layout records that its history was
/// disowned by the source, and discovery ignores names it does not recognise,
/// so the range opens and serves.
///
/// Leaving the check in the CLI would mean the verdict only binds an operator
/// who happens to run repair again. A supervisor restarting the node, a
/// Kubernetes pod rescheduling, or an operator following the ordinary "repair,
/// then start it" workflow would all serve records the range no longer has.
/// The condemnation belongs to the DIRECTORY, so it is enforced wherever a
/// directory is opened.
pub const CONDEMNED_MARKER: &str = ".vtop-repair-diverged";

/// Refuse a range an earlier repair condemned.
fn refuse_if_condemned(env: &Env, directory: &Path) -> VtopLogResult<()> {
    let marker = directory.join(CONDEMNED_MARKER);
    if !env
        .storage
        .exists(&marker)
        .map_err(|source| io_error(&marker, source))?
    {
        return Ok(());
    }
    let detail = env
        .storage
        .read(&marker)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .unwrap_or_default();
    Err(LogError::InvalidDescriptor(format!(
        "range at {} was CONDEMNED by an earlier repair: it holds records its source no longer \
         has, so serving it would hand out a history the range has disowned. This is not a \
         layout problem and the range will open cleanly once the verdict is removed — do not \
         remove it without understanding why it is there. Rebuild the directory from a replica \
         that is current.\n\nThe finding was:\n{}",
        directory.display(),
        detail.trim()
    )))
}

/// Delete the losing halves of interrupted atomic writes.
///
/// Discovery only reports them (see `StartupCatalog::temporary`); this is the
/// path that owns the directory, so this is where they go. A `.tmp` whose
/// rename never happened carries nothing committed — the real file is either
/// the previous version or absent — so removing it cannot lose anything, and
/// leaving it made a range refuse to open after a crash landed inside a write
/// (#310).
///
/// A failure to remove one is NOT fatal. The range is openable either way, and
/// refusing to start over an undeletable stray file would reintroduce the
/// outage this removes.
fn sweep_interrupted_writes(env: &Env, directory: &Path, catalog: &StartupCatalog) {
    let mut removed_any = false;
    for path in &catalog.temporary {
        // Printed rather than traced: this crate carries no logging
        // dependency, and a line on stderr is enough for a condition an
        // operator will otherwise never see.
        match env.storage.remove_file(path) {
            Ok(()) => removed_any = true,
            Err(error) => eprintln!(
                "could not remove interrupted atomic write {}: {error}",
                path.display()
            ),
        }
    }
    // Make the unlinks durable, as every other directory-entry removal in
    // this crate does. Without it a crash right after the sweep can resurrect
    // the debris — self-healing on the next open, but a cleanup that reported
    // success should stay done. Failure is as non-fatal as a failed removal.
    if removed_any {
        if let Err(error) = env.storage.sync_dir(directory) {
            eprintln!(
                "could not sync {} after sweeping interrupted writes: {error}",
                directory.display()
            );
        }
    }
}

/// Consume the ONE unambiguous quarantine: the successor sidecar `roll_in`
/// writes between sealing a tail and creating the successor's own file.
///
/// That ordering is deliberate — a successor must never exist without the
/// frontier that makes it readable — but it opens a window where a crash
/// leaves a sealed prefix, no tail, and a lone `.producers` file for a
/// segment that never came to be. Discovery rightly reports the file as an
/// orphan sidecar; refusing the whole range over it is what turns a routine
/// interruption into a stranded directory (#314 review), because both
/// `open_in` and `adopt_in` refuse quarantined layouts and the recovery
/// (adoption) can then never run.
///
/// The conditions are DELIBERATELY NARROW, and every one of them is what
/// distinguishes this window from real ambiguity:
///
/// * the directory's ONLY quarantine is a single orphan-sidecar bundle of
///   exactly one `.producers` file — anything more is not this window;
/// * the catalog holds no active tail — with a tail present, an orphan
///   sidecar belongs to some other history and is not ours to explain;
/// * the file sits at exactly the last sealed segment's end offset — the
///   one place `roll_in` writes it.
///
/// Deleting it loses nothing: the snapshot is derived from the sealed
/// predecessor, and `open_successor_in` re-derives and rewrites it when
/// adoption mints the successor this one never became.
fn sweep_roll_window_sidecar(
    env: &Env,
    directory: &Path,
    catalog: &StartupCatalog,
) -> VtopLogResult<bool> {
    let [bundle] = catalog.quarantined.as_slice() else {
        return Ok(false);
    };
    if bundle.reasons != [crate::QuarantineReason::OrphanSidecars] {
        return Ok(false);
    }
    let [path] = bundle.paths.as_slice() else {
        return Ok(false);
    };
    let has_active = catalog
        .entries
        .iter()
        .any(|entry| entry.state == CatalogSegmentState::Active);
    if has_active {
        return Ok(false);
    }
    let Some(sealed_end) = catalog
        .entries
        .iter()
        .filter(|entry| entry.state == CatalogSegmentState::Sealed)
        .map(|entry| entry.next_offset)
        .max()
    else {
        return Ok(false);
    };
    if *path != directory.join(format!("{}.producers", segment_stem(sealed_end))) {
        return Ok(false);
    }
    // write_atomic never leaves a partial file, so an undecodable snapshot
    // is not the roll's debris — it is separate evidence, kept quarantined.
    let bytes = env
        .storage
        .read(path)
        .map_err(|source| io_error(path, source))?;
    if ProducerSnapshot::decode(&bytes).is_err() {
        return Ok(false);
    }
    env.storage
        .remove_file(path)
        .map_err(|source| io_error(path, source))?;
    env.storage
        .sync_dir(directory)
        .map_err(|source| io_error(directory, source))?;
    Ok(true)
}

/// Repair the roll's LAST window: a successor whose primary file was fully
/// written but whose commit boundary never was (#314 review).
///
/// Discovery quarantines the bundle because recovery refuses an active with
/// no boundary — rightly, in general, since a tail with records cannot prove
/// what was durable. This window's tail provably has NO records: the repair
/// only fires when the quarantined primary is exactly a valid header sitting
/// at the last sealed segment's end, accompanied by nothing but the roll's
/// own producers sidecar, with no commit file present and no other tail in
/// the directory. Its boundary is then its base by definition, and after
/// rebuilding it the bundle is a healthy empty tail — the same state an
/// uninterrupted roll would have reached.
fn repair_roll_window_successor(env: &Env, catalog: &StartupCatalog) -> VtopLogResult<bool> {
    let [bundle] = catalog.quarantined.as_slice() else {
        return Ok(false);
    };
    let extension_of = |path: &PathBuf| {
        path.extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_owned)
            .unwrap_or_default()
    };
    // Only the files the interrupted roll itself writes; a commit file in
    // the bundle means the boundary EXISTS and failed for another reason —
    // rebuilding over it would mask corruption, not repair an interruption.
    if !bundle
        .paths
        .iter()
        .all(|path| matches!(extension_of(path).as_str(), "active" | "producers"))
    {
        return Ok(false);
    }
    let Some(active_path) = bundle
        .paths
        .iter()
        .find(|path| extension_of(path) == "active")
    else {
        return Ok(false);
    };
    // The roll's own sidecar is always a COMPLETE write (write_atomic), so
    // one that does not decode is a SEPARATE corruption — not this window —
    // and it must gate BOTH outcomes: a repair must not promote past it,
    // and the torn-discard must not delete the evidence next to it (review
    // round eight).
    let sidecar_path = bundle
        .paths
        .iter()
        .find(|path| extension_of(path) == "producers");
    if let Some(path) = sidecar_path {
        let bytes = env
            .storage
            .read(path)
            .map_err(|source| io_error(path, source))?;
        if ProducerSnapshot::decode(&bytes).is_err() {
            return Ok(false);
        }
    }
    let sidecar_present = sidecar_path.is_some();
    if catalog
        .entries
        .iter()
        .any(|entry| entry.state == CatalogSegmentState::Active)
    {
        return Ok(false);
    }
    let Some(last_sealed) = catalog
        .entries
        .iter()
        .filter(|entry| entry.state == CatalogSegmentState::Sealed)
        .max_by_key(|entry| entry.next_offset)
    else {
        return Ok(false);
    };
    // Everything else — the name anchor, the identity, the format, the
    // frontier the sidecar must carry — is judged inside against the sealed
    // predecessor itself, the same source of truth adoption derives from.
    crate::segment::rebuild_empty_successor_commit(
        env,
        active_path,
        &last_sealed.path,
        sidecar_present,
    )
}

impl SegmentSet {
    /// Open every segment of the range in `directory`.
    ///
    /// Refuses to open a directory discovery quarantined anything in. A
    /// quarantined artifact means the layout is ambiguous — two active
    /// segments, overlapping intervals, a half-written atomic file — and
    /// opening the subset that happens to look fine would serve a range with a
    /// hole in it while reporting success.
    pub fn open_in(env: &Env, directory: impl AsRef<Path>) -> VtopLogResult<Option<Self>> {
        let directory = directory.as_ref().to_path_buf();
        // BEFORE anything else, including the truncation replay: a condemned
        // range must not be repaired into shape and then opened.
        refuse_if_condemned(env, &directory)?;
        // A truncation that died between its marker and its final rename is
        // not an ambiguous layout: the marker says exactly which segments
        // were doomed and what replaces them. Finish it BEFORE reading the
        // layout, so the discovery below sees the completed range instead of
        // quarantining the half-made one. A marker that does not decode is
        // different — it names an intent that cannot be honoured — so it
        // falls through to discovery, which quarantines it by name, and the
        // open refuses like any other ambiguity.
        finish_pending_truncation(env, &directory)?;
        // AFTER the truncation replay, same reasoning: a retention that died
        // between its marker and its final unlink is not an ambiguous layout —
        // the marker says exactly which prefix is doomed. Finish it before
        // discovery so the catalog sees the completed range (#290).
        finish_pending_retention(env, &directory)?;
        let mut catalog = StartupCatalog::discover_in(env, &directory)?;
        sweep_interrupted_writes(env, &directory, &catalog);
        // The roll-window repairs, to a fixed point: discarding a torn
        // successor can expose the orphan sidecar its roll wrote first, so
        // one pass is not always enough. Two suffice — the windows are
        // sequential — and the bound is a backstop, not a loop condition.
        for _ in 0..3 {
            if sweep_roll_window_sidecar(env, &directory, &catalog)?
                || repair_roll_window_successor(env, &catalog)?
            {
                // The layout changed — a consumed sidecar, a rebuilt
                // boundary, or a discarded torn create — so re-read it: the
                // refusal below must describe what is on disk NOW, and a
                // healed tail must open normally.
                catalog = StartupCatalog::discover_in(env, &directory)?;
            } else {
                break;
            }
        }
        if !catalog.quarantined.is_empty() {
            // Name every quarantined bundle and its reasons in the refusal.
            // This error is what an operator sees at startup, and "N
            // bundles are quarantined" gives them nothing to act on — the
            // reason is the difference between deleting a stray .tmp file
            // and restoring from a replica.
            let details = catalog
                .quarantined
                .iter()
                .map(|bundle| {
                    let paths = bundle
                        .paths
                        .iter()
                        .map(|path| path.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("[{paths}: {:?}]", bundle.reasons)
                })
                .collect::<Vec<_>>()
                .join("; ");
            return Err(LogError::InvalidDescriptor(format!(
                "range at {} has {} quarantined artifact bundle(s); refusing to open a subset \
                 of an ambiguous range: {details}",
                directory.display(),
                catalog.quarantined.len()
            )));
        }
        if catalog.entries.is_empty() {
            return Ok(None);
        }
        // The same check adoption needs, for the same reason: `validate_contiguous`
        // below compares OFFSETS, and two different ranges can abut perfectly.
        validate_single_lineage(&catalog.entries, &directory)?;

        let mut sealed_paths: Vec<(u64, PathBuf)> = Vec::new();
        let mut active_path: Option<PathBuf> = None;
        for entry in &catalog.entries {
            match entry.state {
                CatalogSegmentState::Sealed => {
                    sealed_paths.push((entry.descriptor.base_offset, entry.path.clone()));
                }
                CatalogSegmentState::Active => {
                    if active_path.is_some() {
                        // Discovery already quarantines this, so reaching here
                        // means the two disagree — refuse rather than pick.
                        return Err(LogError::InvalidDescriptor(
                            "range has more than one active segment".to_owned(),
                        ));
                    }
                    active_path = Some(entry.path.clone());
                }
            }
        }
        sealed_paths.sort_by_key(|(base, _)| *base);

        let Some(active_path) = active_path else {
            // A crash between sealing a segment and creating its successor
            // leaves a range whose tail is closed. That is recoverable — open a
            // new active at its end — but it is a decision for a writer, not
            // something a reader should do silently. Typed, so the one caller
            // with the standing to be that writer can recognise it (#314).
            return Err(LogError::TailSealedWithoutSuccessor { directory });
        };

        let mut sealed = Vec::with_capacity(sealed_paths.len());
        for (_, path) in sealed_paths {
            sealed.push(SegmentReader::open_in(env, &path)?);
        }
        let active = ActiveSegment::recover_in(env, &active_path)?;

        let set = Self {
            env: env.clone(),
            directory,
            sealed,
            active: Some(active),
        };
        set.validate_contiguous()?;
        Ok(Some(set))
    }

    /// Open a received sealed prefix and mint the tail it needs to serve.
    ///
    /// The last unmet piece of #270. Segment transfer lands sealed segments and
    /// nothing else, and [`Self::open_in`] refuses a range whose tail is
    /// sealed:
    ///
    /// > range at {} has no active segment; its tail was sealed without a
    /// > successor
    ///
    /// That refusal is right and stays: a reader must not decide to extend a
    /// range it was only asked to read. Adoption is the writer that gets to
    /// decide, and this is the one place that decision is made.
    ///
    /// REFUSES A DIRECTORY THAT ALREADY HAS A TAIL, rather than adopting around
    /// it. A tail means the range is already live, and minting a second one
    /// would produce two writers for the same offsets — a split range that
    /// discovery would then quarantine, if it were lucky enough to notice.
    /// `open_in` is the call for that directory.
    pub fn adopt_in(
        env: &Env,
        directory: impl AsRef<Path>,
        successor_id: Uuid,
    ) -> VtopLogResult<Self> {
        let directory = directory.as_ref().to_path_buf();
        refuse_if_condemned(env, &directory)?;
        // BEFORE discovery, exactly as `open_in` does. An unfinished truncation
        // makes the layout below the half-made one, and minting a tail over a
        // prefix the marker still condemns would lose writes: the next
        // `open_in` finishes the intent and deletes that very tail, with
        // whatever was appended to it.
        finish_pending_truncation(env, &directory)?;
        // AFTER the truncation replay, same reasoning: a retention that died
        // between its marker and its final unlink is not an ambiguous layout —
        // the marker says exactly which prefix is doomed. Finish it before
        // discovery so the catalog sees the completed range (#290).
        finish_pending_retention(env, &directory)?;
        let catalog = StartupCatalog::discover_in(env, &directory)?;
        sweep_interrupted_writes(env, &directory, &catalog);
        // Adoption is exactly the writer the roll-window sidecar is waiting
        // for, and `open_successor_in` rewrites that sidecar from the sealed
        // predecessor — so consuming the orphan here loses nothing.
        let catalog = if sweep_roll_window_sidecar(env, &directory, &catalog)? {
            StartupCatalog::discover_in(env, &directory)?
        } else {
            catalog
        };
        if !catalog.quarantined.is_empty() {
            return Err(LogError::InvalidDescriptor(format!(
                "range at {} has {} quarantined artifact bundle(s); refusing to adopt an \
                 ambiguous range, because a tail minted onto a prefix with a hole in it would \
                 make the hole permanent",
                directory.display(),
                catalog.quarantined.len()
            )));
        }
        // One range, not several that happen to abut. Offsets alone cannot tell
        // the difference and discovery does not look across ranges at all.
        validate_single_lineage(&catalog.entries, &directory)?;

        let mut sealed_paths: Vec<(u64, PathBuf)> = Vec::new();
        for entry in &catalog.entries {
            match entry.state {
                CatalogSegmentState::Sealed => {
                    sealed_paths.push((entry.descriptor.base_offset, entry.path.clone()))
                }
                CatalogSegmentState::Active => {
                    return Err(LogError::InvalidDescriptor(format!(
                        "range at {} already has an active segment at {}; adoption mints a tail \
                         and this range has one, so a second would give the same offsets two \
                         writers. Use `open_in` for a range that is already live.",
                        directory.display(),
                        entry.path.display()
                    )))
                }
            }
        }
        if sealed_paths.is_empty() {
            return Err(LogError::InvalidDescriptor(format!(
                "range at {} holds no sealed segments; there is nothing to adopt and no offset \
                 to begin a tail at. Use `create_in` for a new range.",
                directory.display()
            )));
        }
        sealed_paths.sort_by_key(|(base, _)| *base);

        let mut sealed = Vec::with_capacity(sealed_paths.len());
        for (_, path) in &sealed_paths {
            sealed.push(SegmentReader::open_in(env, path)?);
        }
        // Contiguity BEFORE the tail is minted. The wire decoder refuses a
        // gapped listing (#292) and discovery refuses an ambiguous directory,
        // but neither covers bytes that arrived by other means — a restored
        // backup, a hand-copied directory. Minting a tail onto a prefix with a
        // hole would bless the hole: the range opens from then on, and the
        // missing offsets are simply never served.
        for pair in sealed.windows(2) {
            if pair[0].next_offset() != pair[1].base_offset() {
                return Err(LogError::InvalidDescriptor(format!(
                    "cannot adopt {}: sealed segments are not contiguous — {} ends at offset {} \
                     but {} begins at {}. Adopting would make the gap permanent.",
                    directory.display(),
                    pair[0].path().display(),
                    pair[0].next_offset(),
                    pair[1].path().display(),
                    pair[1].base_offset()
                )));
            }
        }

        let last = sealed.last().expect("checked non-empty above");
        let active = crate::segment::open_successor_in(env, last.path(), successor_id)?;

        let set = Self {
            env: env.clone(),
            directory,
            sealed,
            active: Some(active),
        };
        // The same invariant `open_in` ends on, so an adopted range and a
        // reopened one are the same thing by the time either is returned.
        set.validate_contiguous()?;
        Ok(set)
    }

    /// Create a range's first segment.
    pub fn create_in(
        env: &Env,
        directory: impl AsRef<Path>,
        descriptor: SegmentDescriptor,
        config: crate::SegmentConfig,
    ) -> VtopLogResult<Self> {
        let directory = directory.as_ref().to_path_buf();
        let path = directory.join(format!("{}.active", segment_stem(descriptor.base_offset)));
        let active = ActiveSegment::create_in(env, &path, descriptor, config)?;
        Ok(Self {
            env: env.clone(),
            directory,
            sealed: Vec::new(),
            active: Some(active),
        })
    }

    /// Append a group, minting the successor's identity from this set's own
    /// environment if the append forces a roll.
    ///
    /// [`Self::append_group`] takes the successor id as a parameter because
    /// crash sweeps need it deterministic per step. A broker's produce path
    /// has no meaningful id to choose, and threading a UUID through every
    /// produce call just to discard it when no roll happens would put the
    /// minting far from the one place that already owns an rng — the same
    /// place cross-segment truncation mints its replacement id.
    pub fn append_group_minting(
        &mut self,
        records: &[LogRecord],
        durability: Durability,
    ) -> VtopLogResult<Vec<crate::AppendOutcome>> {
        let successor_id = Uuid::from_u128(self.env.rng.next_u128());
        self.append_group(records, durability, successor_id)
    }

    /// Advance the tail's durable commit boundary.
    ///
    /// Sealed segments are durable by construction — sealing commits first —
    /// so committing a range is exactly committing its tail. This exists so a
    /// follower batching appends under one durability barrier does not have
    /// to know which segment the barrier lands on.
    pub fn commit(&mut self) -> VtopLogResult<u64> {
        self.tail_mut().commit()
    }

    /// Every segment must begin where the previous one ended.
    ///
    /// A gap is unreadable and an overlap means two segments claim the same
    /// offset. Discovery rejects overlaps on its own; this also catches a gap,
    /// which it does not, because a gap looks like two individually valid
    /// segments.
    fn validate_contiguous(&self) -> VtopLogResult<()> {
        let mut expected: Option<u64> = None;
        for reader in &self.sealed {
            if let Some(expected) = expected {
                if reader.base_offset() != expected {
                    return Err(LogError::InvalidDescriptor(format!(
                        "segment begins at offset {} but the previous one ended at {expected}",
                        reader.base_offset()
                    )));
                }
            }
            expected = Some(reader.next_offset());
        }
        if let Some(expected) = expected {
            if self.tail().base_offset() != expected {
                return Err(LogError::InvalidDescriptor(format!(
                    "active segment begins at offset {} but the last sealed one ended at \
                     {expected}",
                    self.tail().base_offset()
                )));
            }
        }
        Ok(())
    }

    /// Where this range's segments live.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// First offset the range holds.
    pub fn base_offset(&self) -> u64 {
        self.sealed
            .first()
            .map(SegmentReader::base_offset)
            .unwrap_or_else(|| self.tail().base_offset())
    }

    /// First offset it does not hold.
    pub fn next_offset(&self) -> u64 {
        self.tail().next_offset()
    }

    /// Durably committed frontier of the range.
    pub fn committed_offset(&self) -> u64 {
        self.tail().committed_offset()
    }

    /// Sealed segments, oldest first. This is what a transfer ships and what
    /// retention would delete from.
    pub fn sealed(&self) -> &[SegmentReader] {
        &self.sealed
    }

    pub fn active(&self) -> &ActiveSegment {
        self.tail()
    }

    pub fn active_mut(&mut self) -> &mut ActiveSegment {
        self.tail_mut()
    }

    /// The tail. Present except transiently inside [`Self::roll`] and
    /// [`Self::truncate_to`]'s cross-segment path — or permanently after
    /// either fails, which poisons the set.
    fn tail(&self) -> &ActiveSegment {
        self.active
            .as_ref()
            .expect("the tail is absent only mid-roll, mid-truncation, or after one failed")
    }

    fn tail_mut(&mut self) -> &mut ActiveSegment {
        self.active
            .as_mut()
            .expect("the tail is absent only mid-roll, mid-truncation, or after one failed")
    }

    /// Append a group, rolling if it does not fit in the tail.
    ///
    /// The bound is not re-derived here. `append_group` already refuses a batch
    /// that would exceed `max_segment_bytes`, and it refuses BEFORE writing any
    /// bytes — so the refusal is the signal to roll, and a second size estimate
    /// living here could only disagree with the one that matters.
    ///
    /// That refusal is what a range hitting its limit does today: it errors.
    /// Rolling turns it into a boundary.
    ///
    /// The retry lands the WHOLE group in the successor. A group split across a
    /// roll would leave half of a producer's commit group either side of a
    /// boundary, and the two halves could then be transferred, truncated, or
    /// retained independently of each other.
    /// Append to the tail WITHOUT rolling at the bound; the limit errors
    /// surface exactly as a single-segment range reported them.
    ///
    /// For callers whose durability is being deliberately withheld — the
    /// fault harness's fsync hold. A roll seals the tail, and sealing makes
    /// bytes durable, which would silently commit records the injection
    /// promised were still at risk of dying with a crash. Refusing at the
    /// bound keeps that promise exact; nothing in production appends through
    /// this.
    pub fn append_group_tail_only(
        &mut self,
        records: &[LogRecord],
        durability: Durability,
    ) -> VtopLogResult<Vec<crate::AppendOutcome>> {
        self.tail_mut().append_group(records, durability)
    }

    pub fn append_group(
        &mut self,
        records: &[LogRecord],
        durability: Durability,
        successor_id: Uuid,
    ) -> VtopLogResult<Vec<crate::AppendOutcome>> {
        match self.tail_mut().append_group(records, durability) {
            Err(LogError::SegmentByteLimit { .. }) | Err(LogError::SegmentRecordLimit { .. }) => {}
            other => return other,
        }
        // An empty tail that still cannot hold the group means the group is
        // larger than a whole segment. Rolling would produce another empty
        // segment and fail identically, so report the original limit rather
        // than looping.
        if self.tail().next_offset() == self.tail().base_offset() {
            return self.tail_mut().append_group(records, durability);
        }
        self.roll(successor_id)?;
        self.tail_mut().append_group(records, durability)
    }

    /// Seal the tail and open a successor at its end.
    ///
    /// The successor inherits its predecessor's configuration, including its
    /// roll thresholds: a segment's limits live in its header, and a range
    /// carries them forward from the moment it was created.
    pub fn roll(&mut self, successor_id: Uuid) -> VtopLogResult<()> {
        // A segment holding nothing would seal into an empty sealed segment and
        // an identically-based successor, so rolling it is a no-op that costs a
        // file.
        if self.tail().next_offset() == self.tail().base_offset() {
            return Ok(());
        }
        let active = self
            .active
            .take()
            .expect("the tail is absent only mid-roll, mid-truncation, or after one failed");
        // If this fails the tail stays `None`, which every accessor treats as a
        // programming error rather than a state to serve from. That is
        // deliberate: a set whose roll failed part-way has no coherent tail, and
        // continuing to answer reads from a half-rolled range would be worse
        // than stopping.
        let (sealed, successor) = roll_in(&self.env, active, successor_id)?;
        self.active = Some(successor);
        self.sealed.push(sealed);
        Ok(())
    }

    /// Change the range's roll thresholds, from the tail forward (#314).
    ///
    /// A segment's limits live in its header and nowhere else, and that
    /// stays true here: the new thresholds take effect by putting a NEW
    /// header in front of the log — sealing the tail and opening a successor
    /// under the new limits — never by mutating a header that existing bytes
    /// were already validated against. Every sealed segment, and the sealed
    /// former tail, keeps describing exactly the records it holds, which is
    /// why narrowing and raising are the same operation: no live segment's
    /// contract changes, a new segment simply starts under a new one. It is
    /// also why nothing here needs remembering — reopen, adoption, and
    /// cross-segment truncation all rebuild the tail from a header that now
    /// carries the reconfigured limits.
    ///
    /// Validation happens FIRST, under the rules of the tail's ACTUAL
    /// format: the v1 and v2 frame overheads differ, so a group bound that
    /// fits one framed v1 record can be one byte too small for the same
    /// record framed as v2 — and it must be refused before the seal, not
    /// discovered after it has left the range tail-less.
    ///
    /// An override that changes nothing is a no-op: rolling for it would
    /// seal a segment purely to re-state its own limits. A tail holding no
    /// records has its header rewritten in place instead of rolling, because
    /// sealing it would leave an empty sealed segment.
    pub fn reconfigure(
        &mut self,
        thresholds: RollThresholds,
        successor_id: Uuid,
    ) -> VtopLogResult<ReconfigureOutcome> {
        let candidate = match self.tail().config_v2() {
            Some(current) => {
                let applied = thresholds.applied_to_v2(current);
                if applied == current {
                    return Ok(ReconfigureOutcome::Unchanged);
                }
                SuccessorConfig::V2(applied.validate()?)
            }
            None => {
                let current = self.tail().config();
                let applied = thresholds.applied_to(current);
                if applied == current {
                    return Ok(ReconfigureOutcome::Unchanged);
                }
                SuccessorConfig::V1(applied.validate()?)
            }
        };
        if self.tail().next_offset() == self.tail().base_offset() {
            let active = self
                .active
                .take()
                .expect("the tail is absent only mid-roll, mid-truncation, or after one failed");
            let replacement = rewrite_empty_header_in_place(&self.env, active, candidate)?;
            self.active = Some(replacement);
            return Ok(ReconfigureOutcome::RewrittenInPlace);
        }
        let active = self
            .active
            .take()
            .expect("the tail is absent only mid-roll, mid-truncation, or after one failed");
        let (sealed, successor) = roll_in_with(&self.env, active, successor_id, Some(candidate))?;
        let successor_base = successor.base_offset();
        self.active = Some(successor);
        self.sealed.push(sealed);
        Ok(ReconfigureOutcome::Rolled { successor_base })
    }

    /// [`Self::reconfigure`], minting the successor's identity from this
    /// set's own environment — the same minting `append_group_minting` and
    /// cross-segment truncation use.
    pub fn reconfigure_minting(
        &mut self,
        thresholds: RollThresholds,
    ) -> VtopLogResult<ReconfigureOutcome> {
        let successor_id = Uuid::from_u128(self.env.rng.next_u128());
        self.reconfigure(thresholds, successor_id)
    }

    /// Adopt a stranded range — tail sealed, successor never created — so a
    /// reconfigure can proceed, VALIDATING FIRST (#314).
    ///
    /// Adoption mutates the directory: it mints a tail. A resume that
    /// adopted and then discovered the thresholds invalid would leave the
    /// range changed by a command that reported failure — startable, under
    /// limits the operator never confirmed. So the thresholds are checked
    /// against the LAST SEALED segment's header before anything is written:
    /// that header is exactly what the adopted tail will inherit, so the
    /// validation answers for the config `reconfigure` will really see. On
    /// any refusal the directory is byte-for-byte what this call found.
    ///
    /// Returns the adopted set with its tail still at the sealed header's
    /// limits; the caller runs [`Self::reconfigure_minting`] on it, which
    /// takes the empty-tail rewrite path.
    pub fn adopt_for_reconfigure(
        env: &Env,
        directory: impl AsRef<Path>,
        thresholds: RollThresholds,
        successor_id: Uuid,
    ) -> VtopLogResult<Self> {
        let directory = directory.as_ref().to_path_buf();
        // A read-only pass: discovery reports, it never writes.
        let catalog = StartupCatalog::discover_in(env, &directory)?;
        let last_sealed = catalog
            .entries
            .iter()
            .filter(|entry| entry.state == CatalogSegmentState::Sealed)
            .max_by_key(|entry| entry.descriptor.base_offset)
            .ok_or_else(|| {
                LogError::InvalidDescriptor(format!(
                    "range at {} has no sealed segment to adopt a tail onto",
                    directory.display()
                ))
            })?;
        let reader = SegmentReader::open_in(env, &last_sealed.path)?;
        match reader.config_v2() {
            Some(current) => {
                let applied = thresholds.applied_to_v2(current);
                if applied != current {
                    applied.validate()?;
                }
            }
            None => {
                let current = reader.config();
                let applied = thresholds.applied_to(current);
                if applied != current {
                    applied.validate()?;
                }
            }
        }
        drop(reader);
        Self::adopt_in(env, &directory, successor_id)
    }

    /// Reclaim sealed segments from the FRONT of the range until the policy
    /// is satisfied (#290).
    ///
    /// Deletion is only ever a strict prefix drop of whole sealed segments —
    /// the tail is never touched, and the surviving front stays contiguous
    /// with everything above it. Eligibility is doubly bounded:
    ///
    /// * by the POLICY: segments are dropped oldest-first only while the
    ///   range's total bytes (sealed content plus the tail's current frames)
    ///   exceed `max_total_bytes`;
    /// * by the FLOOR: a segment whose `next_offset` exceeds `floor` is never
    ///   dropped, whatever the policy says. The caller passes the offset
    ///   below which records are acknowledged (cluster-committed); data that
    ///   only this replica may hold is not reclaimable disk, it is the
    ///   durability the quorum was promised.
    ///
    /// Crash-safe under a durable intent marker, exactly as cross-segment
    /// truncation is: the marker is written before the first unlink, and an
    /// interrupted retention is finished by the next open rather than leaving
    /// a partially-deleted front bundle that discovery would quarantine.
    ///
    /// A dropped segment's producer frontier survives by construction — it
    /// was inherited into its successor's `.producers` sidecar at roll time —
    /// and a consumer whose cursor points into the reclaimed prefix gets
    /// [`LogError::OffsetBelowRange`] from [`Self::fetch_through`], not a
    /// silent skip.
    pub fn retain(
        &mut self,
        policy: &RetentionPolicy,
        floor: u64,
    ) -> VtopLogResult<RetentionOutcome> {
        // FINISH BEFORE STARTING. A previous pass may have failed between its
        // marker and its last unlink; the caller keeps serving and calls
        // retain again on a later append. Writing a fresh marker over the
        // unfinished one would forget its doomed list — files half-deleted
        // under the old intent would be orphans no recovery ever revisits,
        // and discovery would quarantine the range for them. Completing the
        // old intent first makes the live path self-healing, exactly as the
        // next open would be; if the finish fails again, the error propagates
        // and the OLD marker stays authoritative for the next attempt.
        finish_pending_retention(&self.env, &self.directory)?;
        let sealed_bytes: u64 = self
            .sealed
            .iter()
            .map(|reader| reader.manifest().content_bytes)
            .sum();
        let total = sealed_bytes + self.tail().content_bytes();
        if total <= policy.max_total_bytes {
            return Ok(RetentionOutcome::default());
        }
        let mut cut = 0_usize;
        let mut bytes_removed = 0_u64;
        for reader in &self.sealed {
            if total - bytes_removed <= policy.max_total_bytes {
                break;
            }
            if reader.next_offset() > floor {
                break;
            }
            bytes_removed += reader.manifest().content_bytes;
            cut += 1;
        }
        if cut == 0 {
            return Ok(RetentionOutcome::default());
        }
        let doomed: Vec<RetainedSegment> = self.sealed[..cut]
            .iter()
            .map(|reader| RetainedSegment {
                segment_id: reader.segment_id(),
                base_offset: reader.base_offset(),
            })
            .collect();
        let new_base = self
            .sealed
            .get(cut)
            .map(|reader| reader.base_offset())
            .unwrap_or_else(|| self.tail().base_offset());
        let intent = RetentionIntent { new_base, doomed };
        // POINT OF NO RETURN. Once the marker is durable the retention is
        // promised: a crash after this line is finished by the next open.
        write_atomic(
            &self.env,
            &self.directory.join(RETENTION_INTENT_FILE),
            &intent.encode()?,
        )?;
        // In-memory state first, so a failure while unlinking leaves this set
        // agreeing with what recovery will produce rather than serving
        // readers over deleted files.
        self.sealed.drain(..cut);
        finish_retention(&self.env, &self.directory, &intent)?;
        Ok(RetentionOutcome {
            segments_removed: cut,
            bytes_removed,
        })
    }

    /// Discard every record at or above `offset`.
    ///
    /// The repair for a replica that diverged from its range's current
    /// leadership (#240).
    ///
    /// A cut inside the tail is the single-segment case, unchanged. A cut
    /// below the tail deletes whole segments and replaces the tail, which is
    /// only crash-safe under a durable intent marker; see
    /// [`Self::truncate_across_segments`] for the protocol.
    pub fn truncate_to(&mut self, offset: u64) -> VtopLogResult<crate::TruncateOutcome> {
        if offset > self.next_offset() {
            return Err(LogError::TruncateBeyondTail {
                requested: offset,
                next_offset: self.next_offset(),
            });
        }
        if offset < self.tail().base_offset() {
            return self.truncate_across_segments(offset);
        }
        self.tail_mut().truncate_to(offset)
    }

    /// Honour a cut below the tail: delete every segment at or above the cut
    /// and open a replacement tail there.
    ///
    /// # The marker is what makes this survivable
    ///
    /// Deleting the tail and creating its replacement cannot be ordered
    /// crash-safely on their own. Replacement-first leaves two active
    /// segments, which discovery quarantines; delete-first leaves none, which
    /// nothing can distinguish from a range whose tail was lost. So the
    /// intent is made durable FIRST — `range.truncate-intent`, at the range
    /// directory level, because a marker inside a segment being deleted would
    /// not survive the thing it protects. From that point an interruption at
    /// any step is finishable: [`finish_truncation`] is idempotent and
    /// [`Self::open_in`] runs it before reading the layout.
    ///
    /// # Cuts land on sealed-segment boundaries, deliberately
    ///
    /// A cut strictly inside a sealed segment would need the retained part of
    /// an immutable file rewritten into the replacement tail — a different
    /// repair, with its own recovery story, that deleting whole segments does
    /// not need. Refusing it loudly beats a half-implemented version of it.
    ///
    /// # The replacement inherits the retained prefix's producer frontier
    ///
    /// The first doomed segment begins at the cut, so the frontier IT
    /// inherited — its `.producers` sidecar — is exactly the producer state
    /// of everything retained. Those bytes are embedded in the marker (the
    /// sidecar itself shares a filename stem with the doomed segment and dies
    /// with it) and become the replacement's own sidecar. Without this, the
    /// next append from a producer already in the retained prefix would be
    /// rejected as `FirstSequence` — the exact failure `.producers` exists to
    /// prevent.
    ///
    /// # On failure the set poisons
    ///
    /// Once the marker is durable, the directory changes underneath this
    /// value, and an error part-way would leave it describing segments that
    /// no longer exist on disk. Ownership of the tail is therefore given up
    /// before any file is touched and only restored on success — the same
    /// posture as a failed [`Self::roll`], where every accessor treats the
    /// missing tail as a programming error rather than a state to serve
    /// from. Refusing to serve is safe precisely BECAUSE the marker is
    /// durable: the next open finishes the truncation instead of reopening
    /// the wreckage.
    fn truncate_across_segments(&mut self, offset: u64) -> VtopLogResult<crate::TruncateOutcome> {
        // The marker records a v1 segment identity. A v2 range would need the
        // v2 descriptor fields carried too; until the format does, refuse
        // loudly rather than have recovery rebuild a tail with the wrong
        // identity.
        if self.tail().descriptor_v2().is_some()
            || self.sealed.iter().any(|r| r.descriptor_v2().is_some())
        {
            return Err(LogError::InvalidConfig(
                "cannot truncate a v2 range across segments: the truncation intent marker \
                 records a v1 segment identity"
                    .to_owned(),
            ));
        }
        let Some(cut_index) = self
            .sealed
            .iter()
            .position(|reader| reader.base_offset() == offset)
        else {
            return Err(LogError::InvalidConfig(format!(
                "cannot truncate to offset {offset}: it is below the active segment's start \
                 {} and does not land on a sealed segment boundary; a cut inside a sealed \
                 segment would need the retained part of an immutable file rewritten, which \
                 is a different repair than deleting whole segments",
                self.tail().base_offset()
            )));
        };

        // Read the inherited frontier back from the sidecar rather than
        // trusting memory: these are the bytes that survive, and recovery
        // will have nothing else. Absent reads as empty, exactly as it does
        // for a range's first segment.
        let producers_path = self
            .directory
            .join(format!("{}.producers", segment_stem(offset)));
        let inherited = if self
            .env
            .storage
            .exists(&producers_path)
            .map_err(|source| io_error(&producers_path, source))?
        {
            let bytes = self
                .env
                .storage
                .read(&producers_path)
                .map_err(|source| io_error(&producers_path, source))?;
            ProducerSnapshot::decode(&bytes)?
        } else {
            ProducerSnapshot::default()
        };

        let mut replacement = self.tail().v1_descriptor_view();
        replacement.segment_id = Uuid::from_u128(self.env.rng.next_u128());
        replacement.base_offset = offset;
        let mut doomed: Vec<DoomedSegment> = self.sealed[cut_index..]
            .iter()
            .map(|reader| DoomedSegment {
                segment_id: reader.manifest().descriptor.segment_id,
                base_offset: reader.base_offset(),
            })
            .collect();
        doomed.push(DoomedSegment {
            segment_id: self.tail().v1_descriptor_view().segment_id,
            base_offset: self.tail().base_offset(),
        });
        let intent = TruncateIntent {
            target_offset: offset,
            replacement,
            config: self.tail().config(),
            doomed,
            inherited,
        };
        let records_removed = self.next_offset() - offset;
        let bytes_removed = self.sealed[cut_index..]
            .iter()
            .map(|reader| reader.manifest().content_bytes)
            .sum::<u64>()
            + self.tail().content_bytes();

        // Nothing above changed any state, so every refusal so far left the
        // range untouched. This write is the point of no return: once the
        // marker is durable the truncation WILL complete — in this call, or
        // in the next open if this process dies first.
        write_atomic(
            &self.env,
            &self.directory.join(TRUNCATE_INTENT_FILE),
            &intent.encode()?,
        )?;

        // Close every handle onto doomed files before deleting them, and
        // give up the tail so a failure below leaves the set refusing to
        // serve rather than serving a range that is no longer on disk.
        drop(
            self.active
                .take()
                .expect("the tail is absent only mid-roll, mid-truncation, or after one failed"),
        );
        self.sealed.truncate(cut_index);
        finish_truncation(&self.env, &self.directory, &intent)?;
        // Reopen through recovery rather than keeping a create-time handle:
        // recovery is the path that seeds producer state from the sidecar,
        // so the in-memory tail and the on-disk one cannot disagree about
        // what was inherited.
        let replacement_path = self
            .directory
            .join(format!("{}.active", segment_stem(offset)));
        self.active = Some(ActiveSegment::recover_in(&self.env, &replacement_path)?);
        Ok(crate::TruncateOutcome {
            records_removed,
            bytes_removed,
            next_offset: offset,
        })
    }

    /// Read from `start_offset`, crossing segment boundaries as needed.
    ///
    /// Visibility is clamped at `high_watermark` exactly as a single segment
    /// clamps it, so a set never exposes more than the same records would have
    /// been in one file.
    ///
    /// A `start_offset` below the range's first offset is an ERROR, not a
    /// clamp. This used to silently skip forward, which was survivable when a
    /// range could only begin where it was created — but retention (#290)
    /// moves the front, and a consumer whose cursor points into a reclaimed
    /// prefix must be told records are gone, not handed the new front as if
    /// nothing were missing.
    pub fn fetch_through(
        &mut self,
        start_offset: u64,
        max_bytes: usize,
        max_records: usize,
        high_watermark: u64,
    ) -> VtopLogResult<FetchBatch> {
        let earliest = self.base_offset();
        if start_offset < earliest {
            return Err(LogError::OffsetBelowRange {
                requested: start_offset,
                earliest,
            });
        }
        let high_watermark = high_watermark.min(self.committed_offset());
        let mut records: Vec<FetchedRecord> = Vec::new();
        let mut encoded_bytes = 0_usize;
        let mut cursor = start_offset;

        while cursor < high_watermark && records.len() < max_records && encoded_bytes < max_bytes {
            let remaining_bytes = max_bytes - encoded_bytes;
            let remaining_records = max_records - records.len();
            let batch =
                self.fetch_one(cursor, remaining_bytes, remaining_records, high_watermark)?;
            if batch.records.is_empty() {
                // No progress: either the caller's byte budget cannot fit the
                // next record, or the cursor has reached the readable end.
                // Returning what we have beats looping.
                break;
            }
            encoded_bytes += batch.encoded_bytes;
            cursor = batch.next_offset;
            records.extend(batch.records);
        }

        Ok(FetchBatch {
            records,
            encoded_bytes,
            next_offset: cursor,
            high_watermark,
        })
    }

    /// Read from whichever single segment holds `offset`.
    fn fetch_one(
        &mut self,
        offset: u64,
        max_bytes: usize,
        max_records: usize,
        high_watermark: u64,
    ) -> VtopLogResult<FetchBatch> {
        if offset >= self.tail().base_offset() {
            return self
                .tail_mut()
                .fetch_through(offset, max_bytes, max_records, high_watermark);
        }
        let index = self
            .sealed
            .iter()
            .position(|reader| offset < reader.next_offset())
            .ok_or_else(|| {
                LogError::InvalidCursor(format!("offset {offset} is not in this range"))
            })?;
        // The caller's watermark is passed through, not just the segment's own
        // frontier. A watermark falling INSIDE a sealed segment would otherwise
        // be ignored and the read would return records above it — the one thing
        // a fetch path must never do. The segment still caps at its own
        // manifest, so the read stops at the boundary and the loop above moves
        // on to the next segment.
        self.sealed[index].fetch_through(offset, max_bytes, max_records, high_watermark)
    }
}

/// An already-open tail is a set of one.
///
/// The broker grew up over a single `ActiveSegment`, and every constructor
/// and harness that hands one over is describing exactly the range this
/// conversion produces: no sealed prefix, one tail. Making it a `From` lets
/// those callers keep their shape while the node hands over a catalog-opened
/// set instead — the conversion is lossless, whereas the reverse (a set down
/// to its tail) would silently discard sealed segments and is deliberately
/// not provided.
///
/// The environment and directory come from the segment itself, not from the
/// caller: a tail opened against the simulator must roll its successor onto
/// the simulator, and its parent directory is by definition where its range
/// lives.
impl From<ActiveSegment> for SegmentSet {
    fn from(active: ActiveSegment) -> Self {
        let env = active.env().clone();
        let directory = active
            .path()
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        Self {
            env,
            directory,
            sealed: Vec::new(),
            active: Some(active),
        }
    }
}

/// Carry a durably recorded truncation to completion.
///
/// Idempotent by construction: every step either deletes something that may
/// already be gone or recreates the replacement from what the marker says, so
/// it can run after a crash at ANY point — including inside a previous run of
/// itself. That is why the replacement is deleted and rebuilt even when a
/// prior attempt already created it: the marker is the one source of truth,
/// and rebuilding from it is what makes "how far did the last attempt get"
/// a question nobody has to answer. Nothing is lost by the rebuild — the
/// replacement cannot have accepted an append until the marker is gone.
///
/// Removing the marker is the commit point, and its removal is made durable
/// before returning: a resurrected marker would re-run this function and
/// delete a replacement that HAS accepted appends by then.
fn finish_truncation(env: &Env, directory: &Path, intent: &TruncateIntent) -> VtopLogResult<()> {
    // Sweep debris of an interrupted attempt first. A crash inside one of
    // this function's own atomic writes can leave a `.{name}.{uuid}.tmp`
    // behind, and the discovery that follows would quarantine it as an
    // incomplete write — leaving the range unopenable even though the
    // truncation itself completed. Only debris this truncation could have
    // produced is touched.
    remove_truncation_debris(env, directory, intent)?;
    for doomed in &intent.doomed {
        remove_segment_files(env, directory, doomed.base_offset)?;
    }
    // Deletions must be durable before anything is built on top of them: a
    // crash after the marker clears but before a deletion lands would leave
    // a doomed segment beside the replacement with no marker left to explain
    // it.
    env.storage
        .sync_dir(directory)
        .map_err(|source| io_error(directory, source))?;

    let stem = segment_stem(intent.target_offset);
    if !intent.inherited.is_empty() {
        // Sidecar before the segment file, matching `roll_in`: a tail must
        // never exist without the frontier that makes it readable.
        write_atomic(
            env,
            &directory.join(format!("{stem}.producers")),
            &intent.inherited.encode()?,
        )?;
    }
    let replacement_path = directory.join(format!("{stem}.active"));
    drop(ActiveSegment::create_in(
        env,
        &replacement_path,
        intent.replacement.clone(),
        intent.config,
    )?);

    let marker = directory.join(TRUNCATE_INTENT_FILE);
    env.storage
        .remove_file(&marker)
        .map_err(|source| io_error(&marker, source))?;
    env.storage
        .sync_dir(directory)
        .map_err(|source| io_error(directory, source))
}

/// Delete every file a segment beginning at `base_offset` could own, present
/// or not. Whether it was active or sealed at the crash is unknowable and
/// does not matter; a leftover sidecar is an orphan, which discovery
/// quarantines, so a partial delete would turn the repair into a range that
/// refuses to open.
fn remove_segment_files(env: &Env, directory: &Path, base_offset: u64) -> VtopLogResult<()> {
    let stem = segment_stem(base_offset);
    for name in [
        format!("{stem}.active"),
        format!("{stem}.segment"),
        format!("{stem}.commit"),
        format!("{stem}.index"),
        format!("{stem}.manifest.json"),
        format!("{stem}.chunks"),
        format!("{stem}.producers"),
    ] {
        let path = directory.join(name);
        if env
            .storage
            .exists(&path)
            .map_err(|source| io_error(&path, source))?
        {
            env.storage
                .remove_file(&path)
                .map_err(|source| io_error(&path, source))?;
        }
    }
    Ok(())
}

/// Delete leftover `.{name}.{uuid}.tmp` files from an interrupted attempt at
/// THIS truncation: the marker's own, or a sidecar of a doomed stem (the
/// replacement shares the first doomed stem, so its debris is covered too).
/// Anything else's temporary files are not this repair's to judge.
fn remove_truncation_debris(
    env: &Env,
    directory: &Path,
    intent: &TruncateIntent,
) -> VtopLogResult<()> {
    let stems: Vec<String> = intent
        .doomed
        .iter()
        .map(|doomed| format!(".{}.", segment_stem(doomed.base_offset)))
        .collect();
    let marker_prefix = format!(".{TRUNCATE_INTENT_FILE}.");
    let entries = env
        .storage
        .read_dir(directory)
        .map_err(|source| io_error(directory, source))?;
    for entry in entries {
        let Some(name) = entry.path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !(name.starts_with('.') && name.ends_with(".tmp")) {
            continue;
        }
        if name.starts_with(&marker_prefix) || stems.iter().any(|stem| name.starts_with(stem)) {
            env.storage
                .remove_file(&entry.path)
                .map_err(|source| io_error(&entry.path, source))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KeyRange, RangeLineage, SegmentConfig};
    use tempfile::tempdir;

    fn descriptor() -> SegmentDescriptor {
        SegmentDescriptor {
            segment_id: Uuid::from_u128(1),
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
            max_record_bytes: 256,
            max_group_bytes: 512,
            // Small enough that a handful of records fills a segment, so the
            // roll path is exercised rather than described.
            max_segment_bytes: 512,
            max_segment_records: 100,
            index_stride: 2,
        }
    }

    fn record(producer: Uuid, sequence: u64) -> LogRecord {
        LogRecord {
            producer_id: producer,
            producer_epoch: 0,
            sequence,
            timestamp_millis: 1_700_000_000_000 + sequence as i64,
            attributes: 0,
            key: b"key".to_vec(),
            value: format!("value-{sequence:04}").into_bytes(),
        }
    }

    /// Write enough to force several rolls, then read the whole range back in
    /// one call. The read must cross every boundary without the caller knowing
    /// there were any.
    #[test]
    fn a_read_crosses_every_segment_boundary() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(9);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();

        for sequence in 0..40 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(1000 + sequence as u128),
            )
            .unwrap();
        }

        assert!(
            !set.sealed().is_empty(),
            "40 records into 512-byte segments must have rolled at least once"
        );
        assert_eq!(set.next_offset(), 40);

        let batch = set.fetch_through(0, 1 << 20, 100, 40).unwrap();
        assert_eq!(
            batch.records.len(),
            40,
            "one read must return the whole range, not the first segment's worth"
        );
        for (index, fetched) in batch.records.iter().enumerate() {
            assert_eq!(fetched.offset, index as u64);
            assert_eq!(
                fetched.record.value,
                format!("value-{index:04}").into_bytes(),
                "records must come back in order across boundaries"
            );
        }
    }

    /// A read starting inside a sealed segment still crosses into the active
    /// one — the boundary is invisible from either side.
    #[test]
    fn a_read_starting_mid_range_crosses_into_the_active_segment() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(11);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..30 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(2000 + sequence as u128),
            )
            .unwrap();
        }

        let batch = set.fetch_through(5, 1 << 20, 100, 30).unwrap();
        assert_eq!(batch.records.first().unwrap().offset, 5);
        assert_eq!(batch.records.last().unwrap().offset, 29);
        assert_eq!(batch.records.len(), 25);
    }

    /// The caller's record limit is honoured across boundaries rather than
    /// being reset per segment.
    #[test]
    fn caller_limits_are_honoured_across_boundaries() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(13);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..30 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(3000 + sequence as u128),
            )
            .unwrap();
        }

        let batch = set.fetch_through(0, 1 << 20, 7, 30).unwrap();
        assert_eq!(batch.records.len(), 7);
        assert_eq!(batch.next_offset, 7);
    }

    /// Visibility is clamped at the high-water mark exactly as one segment
    /// clamps it: a set must never expose more than the same records would have
    /// in a single file.
    #[test]
    fn a_set_never_reads_past_the_high_water_mark() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(17);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..30 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(4000 + sequence as u128),
            )
            .unwrap();
        }

        let batch = set.fetch_through(0, 1 << 20, 100, 12).unwrap();
        assert_eq!(batch.records.len(), 12);
        assert_eq!(batch.high_watermark, 12);
    }

    /// A rolled range reopens from disk as one range, with its segments in
    /// order and its producer sequences intact.
    #[test]
    fn a_rolled_range_reopens_as_one_range() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(19);
        {
            let mut set =
                SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config())
                    .unwrap();
            for sequence in 0..30 {
                set.append_group(
                    &[record(producer, sequence)],
                    Durability::Fsync,
                    Uuid::from_u128(5000 + sequence as u128),
                )
                .unwrap();
            }
        }

        let mut reopened = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the range exists");
        assert_eq!(reopened.next_offset(), 30);
        assert_eq!(reopened.base_offset(), 0);

        let batch = reopened.fetch_through(0, 1 << 20, 100, 30).unwrap();
        assert_eq!(batch.records.len(), 30);

        // And it still accepts the next sequence: the producer frontier
        // survived both the rolls and the reopen.
        reopened
            .append_group(
                &[record(producer, 30)],
                Durability::Fsync,
                Uuid::from_u128(6000),
            )
            .unwrap();
        assert_eq!(reopened.next_offset(), 31);
    }

    /// An empty directory is not an error — it is a range that does not exist
    /// yet, which the caller creates.
    #[test]
    fn an_empty_directory_reports_no_range() {
        let directory = tempdir().unwrap();
        assert!(SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .is_none());
    }

    /// REGRESSION. A high-water mark falling INSIDE a sealed segment must still
    /// bound the read.
    ///
    /// `SegmentReader::fetch` caps at the segment's own manifest frontier, and
    /// routing to it without passing the caller's watermark returned records
    /// above that watermark — exposing data no quorum had acknowledged.
    ///
    /// The earlier watermark test passed only because the boundary happened to
    /// fall favourably for the value it chose. This one finds the first sealed
    /// segment's real end and puts the watermark strictly inside it, so it
    /// cannot pass by luck.
    #[test]
    fn a_watermark_inside_a_sealed_segment_still_bounds_the_read() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(23);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..40 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(7000 + sequence as u128),
            )
            .unwrap();
        }

        let first_sealed_end = set
            .sealed()
            .first()
            .expect("40 records must have rolled")
            .next_offset();
        assert!(
            first_sealed_end > 1,
            "need a sealed segment holding at least two records"
        );
        // Strictly inside the first sealed segment.
        let watermark = first_sealed_end - 1;

        let batch = set.fetch_through(0, 1 << 20, 1000, watermark).unwrap();
        assert_eq!(
            batch.records.len() as u64,
            watermark,
            "a read must stop at the caller's watermark even when it falls inside \
             a sealed segment"
        );
        assert!(
            batch.records.iter().all(|r| r.offset < watermark),
            "no record at or above the watermark may be returned"
        );
        assert_eq!(batch.next_offset, watermark);
    }

    /// A cut inside the tail is the single-segment case, unchanged.
    #[test]
    fn a_cut_inside_the_tail_truncates_only_the_tail() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(29);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..40 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(8000 + sequence as u128),
            )
            .unwrap();
        }
        let sealed_before = set.sealed().len();
        let cut = set.active().base_offset() + 1;

        let outcome = set.truncate_to(cut).unwrap();
        assert_eq!(outcome.next_offset, cut);
        assert_eq!(set.next_offset(), cut);
        assert_eq!(
            set.sealed().len(),
            sealed_before,
            "a cut inside the tail must not disturb sealed segments"
        );

        // And the range reopens, which is what proves nothing was left
        // inconsistent on disk.
        drop(set);
        let reopened = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the truncated range must still open");
        assert_eq!(reopened.next_offset(), cut);
    }

    /// A cut below the tail that does not land on a sealed segment boundary
    /// is refused rather than half-done.
    ///
    /// Honouring it would mean rewriting the retained part of an immutable
    /// sealed segment into the replacement tail — a different repair from
    /// deleting whole segments, with its own recovery story the intent
    /// marker does not carry.
    #[test]
    fn a_cut_inside_a_sealed_segment_is_refused() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(37);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..40 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(10_000 + sequence as u128),
            )
            .unwrap();
        }
        assert!(!set.sealed().is_empty(), "need at least one sealed segment");
        let inside = set.sealed()[0].base_offset() + 1;
        assert!(
            inside < set.sealed()[0].next_offset(),
            "the cut must fall strictly inside the first sealed segment"
        );

        assert!(matches!(
            set.truncate_to(inside),
            Err(LogError::InvalidConfig(_))
        ));
        assert_eq!(
            set.next_offset(),
            40,
            "a refused truncation must leave the range untouched"
        );
        assert!(
            !directory.path().join(TRUNCATE_INTENT_FILE).exists(),
            "a refusal must not leave an intent marker behind"
        );
        // Still fully usable: the refusal happened before anything changed.
        set.append_group(
            &[record(producer, 40)],
            Durability::Fsync,
            Uuid::from_u128(10_999),
        )
        .unwrap();
        assert_eq!(set.next_offset(), 41);
    }

    /// Truncation removes records; it can never invent them.
    #[test]
    fn truncating_a_range_beyond_its_tail_is_refused() {
        let directory = tempdir().unwrap();
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        set.append_group(
            &[record(Uuid::from_u128(41), 0)],
            Durability::Fsync,
            Uuid::from_u128(11_000),
        )
        .unwrap();

        assert!(matches!(
            set.truncate_to(99),
            Err(LogError::TruncateBeyondTail { requested: 99, .. })
        ));
    }

    /// Write a rolled range to `directory` and return the second sealed
    /// segment's base offset — a cut there leaves a non-empty retained prefix,
    /// so the frontier the replacement must inherit is non-trivial.
    fn build_rolled_range(directory: &Path, producer: Uuid) -> u64 {
        let mut set = SegmentSet::create_in(&Env::real(), directory, descriptor(), config())
            .expect("the range must be creatable");
        for sequence in 0..40 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(30_000 + sequence as u128),
            )
            .unwrap();
        }
        assert!(
            set.sealed().len() >= 2,
            "the workload must roll at least twice"
        );
        set.sealed()[1].base_offset()
    }

    /// The intent the live path would write for a cut at `target`: doomed
    /// segments from the catalog, frontier from the sidecar of the segment at
    /// the cut. Staging it by hand lets a test park the on-disk state at any
    /// point of the protocol.
    fn staged_intent(directory: &Path, target: u64) -> TruncateIntent {
        let catalog = StartupCatalog::discover_in(&Env::real(), directory).unwrap();
        assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);
        let doomed = catalog
            .entries
            .iter()
            .filter(|entry| entry.descriptor.base_offset >= target)
            .map(|entry| DoomedSegment {
                segment_id: entry.descriptor.segment_id,
                base_offset: entry.descriptor.base_offset,
            })
            .collect::<Vec<_>>();
        let producers = directory.join(format!("{}.producers", segment_stem(target)));
        let inherited = if producers.exists() {
            ProducerSnapshot::decode(&std::fs::read(&producers).unwrap()).unwrap()
        } else {
            ProducerSnapshot::default()
        };
        let mut replacement = descriptor();
        replacement.segment_id = Uuid::from_u128(777);
        replacement.base_offset = target;
        TruncateIntent {
            target_offset: target,
            replacement,
            config: config(),
            doomed,
            inherited,
        }
    }

    /// A cut at a sealed segment boundary removes whole segments and replaces
    /// the tail — and, the regression this feature exists to prevent: the
    /// replacement inherits the retained prefix's producer frontier, so a
    /// producer already in the range continues without `FirstSequence`.
    #[test]
    fn a_cut_at_a_sealed_boundary_removes_whole_segments() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(43);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..40 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(12_000 + sequence as u128),
            )
            .unwrap();
        }
        assert!(set.sealed().len() >= 2, "need at least two sealed segments");
        let cut = set.sealed()[1].base_offset();

        let outcome = set.truncate_to(cut).unwrap();
        assert_eq!(outcome.next_offset, cut);
        assert_eq!(outcome.records_removed, 40 - cut);
        assert!(outcome.bytes_removed > 0);
        assert_eq!(set.next_offset(), cut);
        assert_eq!(set.base_offset(), 0);
        assert_eq!(
            set.sealed().len(),
            1,
            "only segments at or above the cut may be removed"
        );
        assert!(
            !directory.path().join(TRUNCATE_INTENT_FILE).exists(),
            "the marker must be cleared once the truncation completes"
        );

        // The producer's frontier survived the cut: its next sequence is
        // accepted, not rejected as a first-sequence violation.
        set.append_group(
            &[record(producer, cut)],
            Durability::Fsync,
            Uuid::from_u128(13_000),
        )
        .unwrap();
        assert_eq!(set.next_offset(), cut + 1);

        // Contiguity across the surviving boundary.
        let batch = set.fetch_through(0, 1 << 20, 1000, cut + 1).unwrap();
        assert_eq!(batch.records.len() as u64, cut + 1);
        for (index, fetched) in batch.records.iter().enumerate() {
            assert_eq!(fetched.offset, index as u64);
        }

        // And it reopens, which is what proves nothing was left inconsistent
        // on disk.
        drop(set);
        let mut reopened = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the truncated range must still open");
        assert_eq!(reopened.next_offset(), cut + 1);
        reopened
            .append_group(
                &[record(producer, cut + 1)],
                Durability::Fsync,
                Uuid::from_u128(13_001),
            )
            .unwrap();
    }

    /// Recovery completes a truncation that died right after its marker
    /// became durable, before any file was touched.
    #[test]
    fn recovery_completes_a_truncation_that_wrote_only_the_marker() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(47);
        let cut = build_rolled_range(directory.path(), producer);
        let intent = staged_intent(directory.path(), cut);
        std::fs::write(
            directory.path().join(TRUNCATE_INTENT_FILE),
            intent.encode().unwrap(),
        )
        .unwrap();

        let mut set = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the range must open with the truncation completed");
        assert_eq!(set.next_offset(), cut);
        assert!(!directory.path().join(TRUNCATE_INTENT_FILE).exists());
        // The frontier travelled through the marker: the producer continues.
        set.append_group(
            &[record(producer, cut)],
            Durability::Fsync,
            Uuid::from_u128(14_000),
        )
        .unwrap();
        assert_eq!(set.next_offset(), cut + 1);
    }

    /// Recovery completes a truncation that died after creating the
    /// replacement but before deleting anything — the replacement-first crash
    /// whose two active segments discovery quarantines when no marker
    /// explains them.
    #[test]
    fn recovery_completes_a_truncation_interrupted_after_creating_the_replacement() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(53);
        let cut = build_rolled_range(directory.path(), producer);
        let intent = staged_intent(directory.path(), cut);
        let stem = segment_stem(cut);
        if !intent.inherited.is_empty() {
            std::fs::write(
                directory.path().join(format!("{stem}.producers")),
                intent.inherited.encode().unwrap(),
            )
            .unwrap();
        }
        drop(
            ActiveSegment::create_in(
                &Env::real(),
                directory.path().join(format!("{stem}.active")),
                intent.replacement.clone(),
                intent.config,
            )
            .unwrap(),
        );
        // Without the marker this layout is exactly the ambiguity the whole
        // protocol exists to remove: two active segments, and a stem holding
        // both a sealed segment and an active file.
        assert!(
            SegmentSet::open_in(&Env::real(), directory.path()).is_err(),
            "the staged state must be unopenable without the marker"
        );

        std::fs::write(
            directory.path().join(TRUNCATE_INTENT_FILE),
            intent.encode().unwrap(),
        )
        .unwrap();
        let mut set = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("with the marker the same state must open, completed");
        assert_eq!(set.next_offset(), cut);
        assert!(!directory.path().join(TRUNCATE_INTENT_FILE).exists());
        set.append_group(
            &[record(producer, cut)],
            Durability::Fsync,
            Uuid::from_u128(15_000),
        )
        .unwrap();
        assert_eq!(set.next_offset(), cut + 1);
    }

    /// Recovery clears a marker whose work was already done: deletions and
    /// replacement both landed, only the final unlink was lost.
    #[test]
    fn recovery_clears_a_marker_whose_work_is_already_done() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(59);
        let cut = build_rolled_range(directory.path(), producer);
        let intent = staged_intent(directory.path(), cut);
        for doomed in &intent.doomed {
            let stem = segment_stem(doomed.base_offset);
            for suffix in [
                "active",
                "segment",
                "commit",
                "index",
                "manifest.json",
                "chunks",
                "producers",
            ] {
                let _ = std::fs::remove_file(directory.path().join(format!("{stem}.{suffix}")));
            }
        }
        let stem = segment_stem(cut);
        if !intent.inherited.is_empty() {
            std::fs::write(
                directory.path().join(format!("{stem}.producers")),
                intent.inherited.encode().unwrap(),
            )
            .unwrap();
        }
        drop(
            ActiveSegment::create_in(
                &Env::real(),
                directory.path().join(format!("{stem}.active")),
                intent.replacement.clone(),
                intent.config,
            )
            .unwrap(),
        );
        std::fs::write(
            directory.path().join(TRUNCATE_INTENT_FILE),
            intent.encode().unwrap(),
        )
        .unwrap();

        let mut set = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the range must open with the marker cleared");
        assert_eq!(set.next_offset(), cut);
        assert!(!directory.path().join(TRUNCATE_INTENT_FILE).exists());
        // The replacement was rebuilt from the marker, so it carries the
        // marker's identity — proof that completion is deterministic rather
        // than minting a new segment per attempt.
        assert_eq!(
            set.active().descriptor().segment_id,
            intent.replacement.segment_id
        );
        set.append_group(
            &[record(producer, cut)],
            Durability::Fsync,
            Uuid::from_u128(16_000),
        )
        .unwrap();
        assert_eq!(set.next_offset(), cut + 1);
    }

    /// Completion sweeps debris of its own interrupted atomic writes.
    /// Otherwise discovery would quarantine the finished range for an
    /// incomplete write that no longer matters.
    #[test]
    fn recovery_sweeps_debris_of_its_own_interrupted_writes() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(61);
        let cut = build_rolled_range(directory.path(), producer);
        let intent = staged_intent(directory.path(), cut);
        let marker_debris = directory
            .path()
            .join(".range.truncate-intent.00000000-0000-0000-0000-000000000001.tmp");
        let sidecar_debris = directory.path().join(format!(
            ".{}.producers.00000000-0000-0000-0000-000000000002.tmp",
            segment_stem(cut)
        ));
        std::fs::write(&marker_debris, b"debris").unwrap();
        std::fs::write(&sidecar_debris, b"debris").unwrap();
        std::fs::write(
            directory.path().join(TRUNCATE_INTENT_FILE),
            intent.encode().unwrap(),
        )
        .unwrap();

        let set = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the range must open with the truncation completed");
        assert_eq!(set.next_offset(), cut);
        assert!(!marker_debris.exists(), "marker debris must be swept");
        assert!(!sidecar_debris.exists(), "sidecar debris must be swept");
    }

    /// The fault harness's fsync hold appends through the non-rolling path:
    /// a roll seals the tail and sealing makes bytes durable, which would
    /// silently commit records the injection promised were still at risk.
    /// At the bound that path must refuse — never roll.
    #[test]
    fn the_tail_only_append_refuses_at_the_bound_instead_of_rolling() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(72);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        let mut refused = false;
        for sequence in 0..200 {
            match set.append_group_tail_only(&[record(producer, sequence)], Durability::Fsync) {
                Ok(_) => {}
                Err(LogError::SegmentByteLimit { .. })
                | Err(LogError::SegmentRecordLimit { .. }) => {
                    refused = true;
                    break;
                }
                Err(other) => panic!("unexpected refusal: {other:?}"),
            }
        }
        assert!(refused, "the bound must eventually refuse");
        assert!(
            set.sealed().is_empty(),
            "a tail-only append must never seal: sealing would commit bytes a \
             fault injection is deliberately holding out of durability"
        );
    }

    /// A data directory holding only the legacy single `range.active` — every
    /// deployment before the broker opened through the catalog — opens as a
    /// set of one, keeps its records, and ROLLS: the sealed prefix keeps the
    /// legacy stem, the successor takes an offset-based one, and the two read
    /// as one range. No migration step, because none is needed: discovery is
    /// stem-agnostic and rolling derives the successor's name from its base
    /// offset, never from its predecessor's.
    #[test]
    fn a_legacy_single_file_range_opens_as_a_set_of_one_and_rolls() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(71);
        let legacy = directory.path().join("range.active");
        {
            let mut segment =
                ActiveSegment::create_in(&Env::real(), &legacy, descriptor(), config()).unwrap();
            for sequence in 0..3 {
                segment
                    .append_group(&[record(producer, sequence)], Durability::Fsync)
                    .unwrap();
            }
        }

        let mut set = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the legacy layout is a valid range of one segment");
        assert_eq!(set.next_offset(), 3);
        assert!(set.sealed().is_empty());

        // Write past the bound: the legacy file seals under its own name and
        // the successor opens under the offset-based stem.
        for sequence in 3..40 {
            set.append_group_minting(&[record(producer, sequence)], Durability::Fsync)
                .unwrap();
        }
        assert!(!set.sealed().is_empty(), "the range must have rolled");
        assert!(
            directory.path().join("range.segment").exists(),
            "the legacy tail seals under its legacy stem"
        );

        // And the mixed-stem range reopens and reads as one.
        drop(set);
        let mut reopened = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("a mixed legacy/offset-stem range must reopen");
        assert_eq!(reopened.next_offset(), 40);
        let batch = reopened.fetch_through(0, 1 << 20, 100, 40).unwrap();
        assert_eq!(batch.records.len(), 40);
        reopened
            .append_group_minting(&[record(producer, 40)], Durability::Fsync)
            .unwrap();
    }

    /// A quarantined bundle refuses the open WITH its reason in the error.
    /// "N bundles are quarantined" gives an operator nothing to act on; the
    /// reason is the difference between deleting a stray temp file and
    /// restoring from a replica.
    #[test]
    fn a_quarantine_refusal_names_the_reason_and_the_path() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(73);
        build_rolled_range(directory.path(), producer);
        let stray = directory.path().join("stray.active");
        std::fs::write(&stray, b"not a segment").unwrap();

        let Err(problem) = SegmentSet::open_in(&Env::real(), directory.path()) else {
            panic!("a quarantined bundle must refuse the open");
        };
        let message = problem.to_string();
        assert!(
            message.contains("InvalidArtifact"),
            "the refusal must carry the quarantine reason: {message}"
        );
        assert!(
            message.contains("stray.active"),
            "the refusal must name the offending path: {message}"
        );
    }

    /// A marker that does not decode names an intent that cannot be honoured.
    /// It is quarantined by name and the open refuses — recovery never
    /// guesses at which segments to delete.
    #[test]
    fn a_corrupt_marker_quarantines_instead_of_guessing() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(67);
        build_rolled_range(directory.path(), producer);
        std::fs::write(
            directory.path().join(TRUNCATE_INTENT_FILE),
            b"not an intent",
        )
        .unwrap();

        let catalog = StartupCatalog::discover_in(&Env::real(), directory.path()).unwrap();
        assert!(catalog.truncate_intent.is_none());
        assert!(
            catalog.quarantined.iter().any(|item| item
                .reasons
                .iter()
                .any(|reason| matches!(reason, crate::QuarantineReason::InvalidTruncateIntent(_)))),
            "{:?}",
            catalog.quarantined
        );
        assert!(
            !catalog.entries.is_empty(),
            "the segments themselves are intact and stay cataloged"
        );
        assert!(
            SegmentSet::open_in(&Env::real(), directory.path()).is_err(),
            "an undecodable intent must refuse the open, not be acted on"
        );
    }

    /// A range that is only a sealed prefix — what segment transfer produces —
    /// is adopted, served, and reopens as an ordinary range.
    ///
    /// The last unmet piece of #270. `open_in` refuses this directory by
    /// design, so before adoption existed a transferred prefix was bytes on
    /// disk that nothing could turn back into a replica.
    ///
    /// The producer sequence is the assertion that matters. A sealed segment's
    /// `.producers` holds what it INHERITED, not what it ends with, so a tail
    /// seeded from that sidecar would rewind every producer to where the last
    /// segment began and the next append would be rejected as a sequence gap.
    /// This continues the same producer across the adoption boundary, which is
    /// only possible if the frontier came from the scan.
    #[test]
    fn a_sealed_prefix_is_adopted_into_a_range_that_serves_and_reopens() {
        let source = tempdir().unwrap();
        let env = Env::real();
        let producer = Uuid::from_u128(0x9001);

        // A leader-shaped range: rolled several times, one producer throughout.
        let mut leader =
            SegmentSet::create_in(&env, source.path(), descriptor(), config()).unwrap();
        for sequence in 0..24 {
            leader
                .append_group_minting(&[record(producer, sequence)], Durability::Fsync)
                .unwrap();
        }
        assert!(leader.sealed().len() >= 2, "the fixture must roll");
        let sealed_count = leader.sealed().len();
        let prefix_end = leader.sealed()[sealed_count - 1].next_offset();

        // The receiver's directory: the SEALED PREFIX ONLY, which is exactly
        // what a transfer lands — the tail is never shipped.
        let received = tempdir().unwrap();
        for reader in leader.sealed() {
            for artifact in reader.paths().unwrap() {
                if artifact.exists() {
                    let name = artifact.file_name().unwrap();
                    std::fs::copy(&artifact, received.path().join(name)).unwrap();
                }
            }
        }

        // Precondition: this is the refusal adoption exists to answer.
        let refused = SegmentSet::open_in(&env, received.path())
            .map(|_| ())
            .expect_err("a prefix without a tail must not open as a range");
        assert!(
            refused.to_string().contains("no active segment"),
            "{refused}"
        );

        let mut adopted =
            SegmentSet::adopt_in(&env, received.path(), Uuid::from_u128(0xADD1)).unwrap();
        assert_eq!(adopted.sealed().len(), sealed_count);
        assert_eq!(
            adopted.next_offset(),
            prefix_end,
            "the tail must begin where the prefix ended, not at zero"
        );

        // THE POINT: the producer continues from where the PREFIX left it —
        // which is not where the leader left it, because the leader's unsealed
        // tail never ships. One record per group here, so the next sequence is
        // the offset the prefix ended at; deriving it rather than hardcoding is
        // what makes this an assertion about the frontier instead of about the
        // fixture.
        //
        // A tail seeded from the last sealed segment's inherited sidecar would
        // rewind to where that segment BEGAN and reject the very first append
        // as a gap. Asked for 24 while the frontier sat at 19, this reported
        // `SequenceGap { expected: 20, actual: 24 }` — the mechanism working,
        // and the reason the expected value is computed here.
        let appended = 6;
        for sequence in prefix_end..prefix_end + appended {
            adopted
                .append_group_minting(&[record(producer, sequence)], Durability::Fsync)
                .expect("the adopted tail must continue the producer's sequence");
        }

        // And it is an ordinary range afterwards: reopens without adoption, and
        // every record from both sides reads back in one call.
        drop(adopted);
        let reopened = SegmentSet::open_in(&env, received.path())
            .unwrap()
            .expect("an adopted range reopens like any other");
        assert_eq!(reopened.next_offset(), prefix_end + appended);
        let mut reopened = reopened;
        let through = reopened.next_offset();
        let batch = reopened.fetch_through(0, 1 << 20, 100, through).unwrap();
        assert_eq!(
            batch.records.len() as u64,
            prefix_end + appended,
            "every record, across the adoption boundary"
        );
        for (index, fetched) in batch.records.iter().enumerate() {
            assert_eq!(fetched.offset, index as u64);
        }
    }

    /// Adoption refuses a range that already has a tail.
    ///
    /// Minting a second tail would give the same offsets two writers, which is
    /// a split range that discovery quarantines only if it happens to notice.
    /// Refusing names the alternative rather than leaving the caller to guess.
    #[test]
    fn adoption_refuses_a_range_that_is_already_live() {
        let directory = tempdir().unwrap();
        let env = Env::real();
        drop(SegmentSet::create_in(&env, directory.path(), descriptor(), config()).unwrap());

        let refused = SegmentSet::adopt_in(&env, directory.path(), Uuid::from_u128(0xADD2))
            .map(|_| ())
            .expect_err("a live range must not be adopted");
        assert!(
            refused
                .to_string()
                .contains("already has an active segment"),
            "{refused}"
        );
        assert!(
            refused.to_string().contains("open_in"),
            "and name the alternative: {refused}"
        );
    }

    /// A gapped prefix is refused rather than blessed.
    ///
    /// The wire decoder refuses a gapped listing (#292) and discovery refuses
    /// an ambiguous directory, but neither covers bytes that arrived some other
    /// way — a restored backup, a hand-copied directory. Minting a tail onto a
    /// hole makes the hole permanent: the range opens from then on and the
    /// missing offsets are simply never served.
    #[test]
    fn adoption_refuses_a_prefix_with_a_hole_in_it() {
        let source = tempdir().unwrap();
        let env = Env::real();
        let producer = Uuid::from_u128(0x9002);
        let mut leader =
            SegmentSet::create_in(&env, source.path(), descriptor(), config()).unwrap();
        for sequence in 0..36 {
            leader
                .append_group_minting(&[record(producer, sequence)], Durability::Fsync)
                .unwrap();
        }
        assert!(
            leader.sealed().len() >= 3,
            "need three to drop a middle one"
        );

        // Copy the prefix, SKIPPING one segment in the middle.
        let received = tempdir().unwrap();
        let skip = leader.sealed()[1].base_offset();
        for reader in leader.sealed() {
            if reader.base_offset() == skip {
                continue;
            }
            for artifact in reader.paths().unwrap() {
                if artifact.exists() {
                    let name = artifact.file_name().unwrap();
                    std::fs::copy(&artifact, received.path().join(name)).unwrap();
                }
            }
        }

        let refused = SegmentSet::adopt_in(&env, received.path(), Uuid::from_u128(0xADD3))
            .map(|_| ())
            .expect_err("a gapped prefix must not be adopted");
        let text = refused.to_string();
        assert!(
            text.contains("not contiguous") || text.contains("quarantined"),
            "the refusal must name the gap: {text}"
        );
    }

    /// Two different ranges that happen to abut must not be adopted as one.
    ///
    /// Offsets cannot tell them apart, and discovery does not look: it groups
    /// candidates BY range and flags disagreement within a group, so segments
    /// from two ranges are two clean groups and nothing is quarantined. Stitched
    /// together, the set would serve one range's records under the other's
    /// identity and the tail would inherit whichever descriptor came last.
    #[test]
    fn adoption_refuses_segments_from_two_different_ranges() {
        let env = Env::real();
        let producer = Uuid::from_u128(0x9003);

        // Range A: sealed prefix starting at 0.
        let a = tempdir().unwrap();
        let mut set_a = SegmentSet::create_in(&env, a.path(), descriptor(), config()).unwrap();
        for sequence in 0..24 {
            set_a
                .append_group_minting(&[record(producer, sequence)], Durability::Fsync)
                .unwrap();
        }
        assert!(!set_a.sealed().is_empty());

        // Range B: a DIFFERENT range id, whose first segment begins exactly
        // where A's prefix ends — the case offset checks cannot see.
        let b = tempdir().unwrap();
        let mut foreign = descriptor();
        foreign.lineage.range_id = Uuid::from_u128(0xBEEF);
        // A distinct segment id as well as a distinct range: sharing one would
        // be caught by discovery as `DuplicateSegmentId`, and the test would
        // pass on a refusal that has nothing to do with lineage. My first
        // version did exactly that.
        foreign.segment_id = Uuid::from_u128(0xB001);
        foreign.base_offset = set_a.sealed().last().unwrap().next_offset();
        let mut set_b = SegmentSet::create_in(&env, b.path(), foreign, config()).unwrap();
        for sequence in 0..24 {
            set_b
                .append_group_minting(&[record(producer, sequence)], Durability::Fsync)
                .unwrap();
        }
        assert!(!set_b.sealed().is_empty());

        let mixed = tempdir().unwrap();
        for reader in set_a.sealed().iter().chain(set_b.sealed().iter()) {
            for artifact in reader.paths().unwrap() {
                if artifact.exists() {
                    let name = artifact.file_name().unwrap();
                    std::fs::copy(&artifact, mixed.path().join(name)).unwrap();
                }
            }
        }

        // The premise: discovery is HAPPY with this directory. If it
        // quarantined, the refusal below would prove nothing about lineage.
        let catalog = StartupCatalog::discover(mixed.path()).unwrap();
        assert!(
            catalog.quarantined.is_empty(),
            "discovery must not object, or this tests the wrong thing: {:?}",
            catalog.quarantined
        );

        let refused = SegmentSet::adopt_in(&env, mixed.path(), Uuid::from_u128(0xADD4))
            .map(|_| ())
            .expect_err("segments from two ranges must not be adopted as one");
        assert!(
            refused.to_string().contains("mixes lineages"),
            "the refusal must name the cause: {refused}"
        );
    }

    /// Adoption must finish an in-flight truncation before reading the layout.
    ///
    /// This is a way to LOSE WRITES, not merely to read something stale.
    /// Adopting over a live marker mints a tail on a prefix the marker still
    /// condemns; the tail then accepts appends, and the next `open_in` finishes
    /// the intent and deletes exactly that tail, with everything written to it.
    #[test]
    fn adoption_finishes_a_pending_truncation_instead_of_building_on_it() {
        let env = Env::real();
        let producer = Uuid::from_u128(0x9004);
        let source = tempdir().unwrap();
        let mut set = SegmentSet::create_in(&env, source.path(), descriptor(), config()).unwrap();
        for sequence in 0..24 {
            set.append_group_minting(&[record(producer, sequence)], Durability::Fsync)
                .unwrap();
        }
        assert!(set.sealed().len() >= 2);

        // A received prefix, plus a marker condemning its last sealed segment —
        // the shape a transfer interrupted by a truncation leaves behind.
        let received = tempdir().unwrap();
        for reader in set.sealed() {
            for artifact in reader.paths().unwrap() {
                if artifact.exists() {
                    let name = artifact.file_name().unwrap();
                    std::fs::copy(&artifact, received.path().join(name)).unwrap();
                }
            }
        }
        let doomed = set.sealed().last().unwrap();
        let cut = doomed.base_offset();
        let mut replacement = descriptor();
        replacement.segment_id = Uuid::from_u128(0xDEAD);
        replacement.base_offset = cut;
        let intent = TruncateIntent {
            target_offset: cut,
            replacement,
            config: config(),
            doomed: vec![DoomedSegment {
                segment_id: doomed.segment_id(),
                base_offset: doomed.base_offset(),
            }],
            inherited: Default::default(),
        };
        std::fs::write(
            received.path().join(TRUNCATE_INTENT_FILE),
            intent.encode().unwrap(),
        )
        .unwrap();

        // Whatever adoption decides, it must not leave the marker live with a
        // tail built on top of it. Either it finishes the truncation and adopts
        // the surviving prefix, or it refuses — both are safe; carrying on is
        // not.
        match SegmentSet::adopt_in(&env, received.path(), Uuid::from_u128(0xADD5)) {
            Ok(adopted) => {
                assert!(
                    !received.path().join(TRUNCATE_INTENT_FILE).exists(),
                    "a tail was minted while the marker is still live; the next open would \
                     finish the truncation and delete it"
                );
                assert!(
                    adopted.next_offset() <= doomed.next_offset(),
                    "the adopted tail must not begin past what the truncation condemned"
                );
            }
            Err(error) => {
                assert!(
                    !error.to_string().is_empty(),
                    "a refusal must say why: {error}"
                );
            }
        }
    }

    /// A range whose commit write was interrupted still opens, and the debris
    /// is gone afterwards.
    ///
    /// This is #310, and the reason it is written by hand rather than by
    /// killing a process: the real failure needs a `kill -9` to land inside
    /// `write_atomic`, between the temp file and the rename. A test that waits
    /// for that window reproduces the bug perhaps one run in ten, which is how
    /// it reached a release — it looked like a flaky scenario.
    ///
    /// Creating the temp directly is exactly the state the crash leaves, and
    /// asserts on it every run.
    #[test]
    fn an_interrupted_commit_write_does_not_stop_a_range_opening() {
        let dir = tempfile::tempdir().unwrap();
        let env = Env::real();
        let mut set = SegmentSet::create_in(
            &env,
            dir.path(),
            descriptor(),
            crate::SegmentConfig::default(),
        )
        .unwrap();
        let producer = Uuid::from_u128(1);
        set.append_group(&[record(producer, 0)], Durability::Fsync, Uuid::new_v4())
            .unwrap();
        drop(set);

        // The losing half of an atomic commit write: fsynced, never renamed.
        let debris = dir
            .path()
            .join(".range-00000000000000000000.commit.052480a2-9d38-4915-b5c1-773eb42625a7.tmp");
        std::fs::write(&debris, b"half a commit record").unwrap();

        let reopened = SegmentSet::open_in(&env, dir.path())
            .expect("an interrupted commit write must not make a range unopenable")
            .expect("the range exists");
        assert_eq!(
            reopened.next_offset(),
            1,
            "the record written before the interruption is still there"
        );
        assert!(
            !debris.exists(),
            "the opener owns the directory, so it should have swept the debris rather than \
             leaving an operator to delete a file whose name cannot tell them that is safe"
        );
    }

    /// The counterpart boundary: the sweep must take ONLY the exact
    /// `write_atomic` shape `.{target}.{uuid}.tmp`. A dotted `.tmp` that
    /// merely mentions `.commit.` is not this crate's file, and deleting it
    /// would break discovery's promise to ignore what it does not recognize.
    #[test]
    fn the_sweep_does_not_delete_an_unrelated_dotted_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let env = Env::real();
        let mut set = SegmentSet::create_in(
            &env,
            dir.path(),
            descriptor(),
            crate::SegmentConfig::default(),
        )
        .unwrap();
        let producer = Uuid::from_u128(1);
        set.append_group(&[record(producer, 0)], Durability::Fsync, Uuid::new_v4())
            .unwrap();
        drop(set);

        // Mentions `.commit.` but its trailing component is not a UUID, so it
        // cannot be a write_atomic scratch file.
        let unrelated = dir.path().join(".notes.commit.backup.tmp");
        std::fs::write(&unrelated, b"operator notes, not ours").unwrap();

        let reopened = SegmentSet::open_in(&env, dir.path())
            .expect("an unrelated dotted .tmp must not affect opening")
            .expect("the range exists");
        assert_eq!(reopened.next_offset(), 1);
        assert!(
            unrelated.exists(),
            "a file that is not an interrupted atomic write must survive the sweep untouched"
        );
    }

    /// The claim of #290: a bounded range reclaims its oldest sealed
    /// segments, stays contiguous, still serves everything it kept, and
    /// leaves no marker or orphan behind.
    #[test]
    fn retention_reclaims_the_oldest_sealed_segments_until_the_bound_holds() {
        let dir = tempdir().unwrap();
        let producer = Uuid::from_u128(0xAA);
        build_rolled_range(dir.path(), producer);
        let mut set = SegmentSet::open_in(&Env::real(), dir.path())
            .unwrap()
            .expect("the range exists");
        let before = set.sealed().len();
        assert!(before >= 2);
        let front_stem = segment_stem(set.sealed()[0].base_offset());
        let next_offset = set.next_offset();

        // A bound below the current total, with everything acknowledged.
        let outcome = set
            .retain(
                &RetentionPolicy {
                    max_total_bytes: 600,
                },
                next_offset,
            )
            .expect("retention must succeed");

        assert!(outcome.segments_removed >= 1, "{outcome:?}");
        assert_eq!(set.sealed().len(), before - outcome.segments_removed);
        let new_base = set.base_offset();
        assert!(new_base > 0, "the front moved");
        // Everything kept is still readable across the surviving boundaries.
        let batch = set
            .fetch_through(new_base, 1 << 20, 10_000, next_offset)
            .unwrap();
        assert_eq!(batch.records.len() as u64, next_offset - new_base);
        // The reclaimed files are gone, marker included; discovery accepts
        // the directory with zero quarantines.
        assert!(!dir.path().join(format!("{front_stem}.segment")).exists());
        assert!(!dir.path().join(RETENTION_INTENT_FILE).exists());
        drop(set);
        let catalog = StartupCatalog::discover_in(&Env::real(), dir.path()).unwrap();
        assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);
        // And the range reopens.
        SegmentSet::open_in(&Env::real(), dir.path())
            .unwrap()
            .expect("a retained range must still open");
    }

    /// Data the quorum has not acknowledged is not reclaimable disk. A floor
    /// inside the first sealed segment must keep everything.
    #[test]
    fn retention_never_reclaims_past_the_acknowledged_floor() {
        let dir = tempdir().unwrap();
        let producer = Uuid::from_u128(0xAB);
        build_rolled_range(dir.path(), producer);
        let mut set = SegmentSet::open_in(&Env::real(), dir.path())
            .unwrap()
            .expect("the range exists");
        let before = set.sealed().len();
        let floor_inside_first = set.sealed()[0].next_offset() - 1;

        let outcome = set
            .retain(&RetentionPolicy { max_total_bytes: 0 }, floor_inside_first)
            .expect("a fully-bounded retention is still a success");

        assert_eq!(
            outcome.segments_removed, 0,
            "nothing below the policy bound is eligible above the floor"
        );
        assert_eq!(set.sealed().len(), before);
    }

    /// A crash between the marker and the unlinks must finish on the next
    /// open, not quarantine the range.
    #[test]
    fn recovery_completes_a_retention_that_wrote_only_the_marker() {
        let dir = tempdir().unwrap();
        let producer = Uuid::from_u128(0xAC);
        build_rolled_range(dir.path(), producer);
        let set = SegmentSet::open_in(&Env::real(), dir.path())
            .unwrap()
            .expect("the range exists");
        let doomed_front = crate::retention_intent::RetainedSegment {
            segment_id: set.sealed()[0].segment_id(),
            base_offset: set.sealed()[0].base_offset(),
        };
        let new_base = set.sealed()[1].base_offset();
        let expected_sealed = set.sealed().len() - 1;
        let next_offset = set.next_offset();
        drop(set);
        // The exact on-disk state of a crash straight after the point of no
        // return: marker durable, nothing deleted yet.
        let intent = RetentionIntent {
            new_base,
            doomed: vec![doomed_front],
        };
        write_atomic(
            &Env::real(),
            &dir.path().join(RETENTION_INTENT_FILE),
            &intent.encode().unwrap(),
        )
        .unwrap();

        let mut reopened = SegmentSet::open_in(&Env::real(), dir.path())
            .unwrap()
            .expect("recovery must finish the retention and open the range");
        assert_eq!(reopened.sealed().len(), expected_sealed);
        assert_eq!(reopened.base_offset(), new_base);
        assert!(!dir.path().join(RETENTION_INTENT_FILE).exists());
        assert!(!dir
            .path()
            .join(format!(
                "{}.segment",
                segment_stem(doomed_front.base_offset)
            ))
            .exists());
        let batch = reopened
            .fetch_through(new_base, 1 << 20, 10_000, next_offset)
            .unwrap();
        assert_eq!(batch.records.len() as u64, next_offset - new_base);
    }

    /// A marker that does not decode names an intent that cannot be honoured;
    /// the range must refuse to open on a guess.
    #[test]
    fn a_corrupt_retention_marker_quarantines_instead_of_guessing() {
        let dir = tempdir().unwrap();
        let producer = Uuid::from_u128(0xAD);
        build_rolled_range(dir.path(), producer);
        std::fs::write(dir.path().join(RETENTION_INTENT_FILE), b"not a marker").unwrap();

        let refused = SegmentSet::open_in(&Env::real(), dir.path());
        assert!(
            refused.is_err(),
            "an unhonourable retention intent must refuse the open"
        );
        let catalog = StartupCatalog::discover_in(&Env::real(), dir.path()).unwrap();
        assert!(
            catalog
                .quarantined
                .iter()
                .any(|item| item.reasons.iter().any(|reason| matches!(
                    reason,
                    crate::QuarantineReason::InvalidRetentionIntent(_)
                ))),
            "{:?}",
            catalog.quarantined
        );
    }

    /// The nameable error #290 requires: a cursor pointing into the reclaimed
    /// prefix is told the records are gone, not silently skipped to the new
    /// front.
    #[test]
    fn a_fetch_below_the_retained_base_names_the_missing_prefix() {
        let dir = tempdir().unwrap();
        let producer = Uuid::from_u128(0xAE);
        build_rolled_range(dir.path(), producer);
        let mut set = SegmentSet::open_in(&Env::real(), dir.path())
            .unwrap()
            .expect("the range exists");
        let next_offset = set.next_offset();
        set.retain(
            &RetentionPolicy {
                max_total_bytes: 600,
            },
            next_offset,
        )
        .unwrap();
        let earliest = set.base_offset();
        assert!(earliest > 0);

        let refused = set.fetch_through(0, 1 << 20, 10_000, next_offset);
        match refused {
            Err(LogError::OffsetBelowRange {
                requested,
                earliest: named,
            }) => {
                assert_eq!(requested, 0);
                assert_eq!(named, earliest);
            }
            other => panic!("a retained offset must be a nameable error, got {other:?}"),
        }
    }

    /// The exact state a FAILED pass leaves behind: marker durable, the
    /// in-memory front already drained, deletions incomplete, and the error
    /// swallowed by a caller that keeps serving. A later pass must finish
    /// that intent before writing its own — replacing it would forget the
    /// old doomed list and leave its half-deleted files as orphans no
    /// recovery ever revisits.
    #[test]
    fn a_new_retention_pass_finishes_an_unfinished_marker_first() {
        let dir = tempdir().unwrap();
        let producer = Uuid::from_u128(0xB0);
        build_rolled_range(dir.path(), producer);
        let mut set = SegmentSet::open_in(&Env::real(), dir.path())
            .unwrap()
            .expect("the range exists");
        let front = crate::retention_intent::RetainedSegment {
            segment_id: set.sealed()[0].segment_id(),
            base_offset: set.sealed()[0].base_offset(),
        };
        let front_stem = segment_stem(front.base_offset);
        let new_base = set.sealed()[1].base_offset();
        let intent = RetentionIntent {
            new_base,
            doomed: vec![front],
        };
        write_atomic(
            &Env::real(),
            &dir.path().join(RETENTION_INTENT_FILE),
            &intent.encode().unwrap(),
        )
        .unwrap();
        // What drain(..cut) had already done before the failure.
        set.sealed.remove(0);

        // A later pass whose own policy is satisfied must STILL finish the
        // unfinished intent rather than ignoring or replacing it.
        let outcome = set
            .retain(
                &RetentionPolicy {
                    max_total_bytes: u64::MAX,
                },
                u64::MAX,
            )
            .expect("the pass must complete the pending intent");
        assert_eq!(outcome.segments_removed, 0, "its own policy dooms nothing");
        assert!(
            !dir.path().join(format!("{front_stem}.segment")).exists(),
            "the OLD intent's deletions must have been finished"
        );
        assert!(!dir.path().join(RETENTION_INTENT_FILE).exists());
        drop(set);
        let reopened = SegmentSet::open_in(&Env::real(), dir.path())
            .unwrap()
            .expect("no orphans, no quarantine");
        assert_eq!(reopened.base_offset(), new_base);
    }

    /// The frontier of a reclaimed segment survives in its successor's
    /// sidecar, so the same producer continues its sequence across a
    /// retention — worth a test, not an assumption (#290).
    #[test]
    fn the_producer_frontier_survives_a_retention() {
        let dir = tempdir().unwrap();
        let producer = Uuid::from_u128(0xAF);
        build_rolled_range(dir.path(), producer);
        let mut set = SegmentSet::open_in(&Env::real(), dir.path())
            .unwrap()
            .expect("the range exists");
        let next_offset = set.next_offset();
        set.retain(
            &RetentionPolicy {
                max_total_bytes: 600,
            },
            next_offset,
        )
        .unwrap();
        drop(set);

        // Reopen (as a restart would) and continue the same producer at its
        // next sequence: the inherited frontier must admit it, and a replay
        // of an already-accepted sequence must still be recognized as such.
        let mut reopened = SegmentSet::open_in(&Env::real(), dir.path())
            .unwrap()
            .expect("a retained range must reopen");
        reopened
            .append_group(
                &[record(producer, 40)],
                Durability::Fsync,
                Uuid::from_u128(40_000),
            )
            .expect("the producer's sequence must continue across the retention");
        assert_eq!(reopened.next_offset(), next_offset + 1);
    }

    // ----- reconfigure (#314) -----------------------------------------------
    //
    // Each test below pins one of the six cases review found on #313 before
    // the feature was pulled out, and every range is built through the real
    // create/open path — a hand-assembled config hides invalid combinations,
    // which is how an earlier test came to assert nothing.

    /// Case 1: the change happens NOW, durably — a roll at the reconfigure,
    /// not a stored setting waiting for an unpredictable moment.
    #[test]
    fn reconfigure_rolls_once_and_the_new_tail_carries_the_new_limits() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(31);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..5 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(3100 + sequence as u128),
            )
            .unwrap();
        }
        let sealed_before = set.sealed().len();

        let outcome = set
            .reconfigure(
                RollThresholds {
                    max_segment_bytes: Some(4096),
                    max_group_bytes: Some(1024),
                    ..RollThresholds::default()
                },
                Uuid::from_u128(3999),
            )
            .unwrap();

        assert!(
            matches!(outcome, ReconfigureOutcome::Rolled { .. }),
            "a tail holding records must roll, so its sealed bytes keep the header they were \
             written under"
        );
        assert_eq!(
            set.sealed().len(),
            sealed_before + 1,
            "reconfigure rolls exactly once"
        );
        let tail = set.active().config();
        assert_eq!(tail.max_segment_bytes, 4096);
        assert_eq!(tail.max_group_bytes, 1024);
        assert_eq!(
            tail.max_record_bytes,
            config().max_record_bytes,
            "an absent threshold keeps the tail's current value; the operator restates nothing"
        );
        // The sealed former tail still describes its own records.
        assert_eq!(
            set.sealed().last().unwrap().config().max_segment_bytes,
            config().max_segment_bytes,
            "no existing header changes; the read path decodes sealed bytes against the limits \
             they were written under"
        );
    }

    /// Case 4: a RAISED limit has a roll path — the record the old tail
    /// refused is admitted by the new one.
    #[test]
    fn a_raised_record_limit_admits_the_record_the_old_limits_refused() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(37);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        set.append_group(
            &[record(producer, 0)],
            Durability::Fsync,
            Uuid::from_u128(1),
        )
        .unwrap();
        let big = LogRecord {
            value: vec![0xAB; 600],
            sequence: 1,
            ..record(producer, 1)
        };
        assert!(
            set.append_group(
                std::slice::from_ref(&big),
                Durability::Fsync,
                Uuid::from_u128(2)
            )
            .is_err(),
            "a 600-byte value must be refused under a 256-byte record limit, or this test \
             proves nothing about raising it"
        );

        set.reconfigure(
            RollThresholds {
                max_record_bytes: Some(2048),
                max_group_bytes: Some(4096),
                max_segment_bytes: Some(8192),
                ..RollThresholds::default()
            },
            Uuid::from_u128(3),
        )
        .unwrap();

        set.append_group(&[big], Durability::Fsync, Uuid::from_u128(4))
            .expect("after raising the limits, a workload of larger records must not stay wedged");
        assert_eq!(set.next_offset(), 2);
    }

    /// Case 3: narrowing never touches an existing header — the sealed prefix
    /// written under the wider limits stays readable, and only the new tail
    /// rolls at the narrower bound.
    #[test]
    fn a_narrowed_limit_governs_the_new_tail_while_the_sealed_prefix_still_reads() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(41);
        let wide = SegmentConfig {
            max_segment_bytes: 8192,
            max_group_bytes: 1024,
            ..config()
        };
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), wide).unwrap();
        for sequence in 0..10 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(4100 + sequence as u128),
            )
            .unwrap();
        }
        assert!(
            set.sealed().is_empty(),
            "10 small records must fit one 8 KiB segment, or the narrow assertion below is \
             measuring the old limits"
        );

        set.reconfigure(
            RollThresholds {
                max_segment_bytes: Some(512),
                max_group_bytes: Some(512),
                ..RollThresholds::default()
            },
            Uuid::from_u128(4998),
        )
        .unwrap();
        for sequence in 10..20 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(4200 + sequence as u128),
            )
            .unwrap();
        }
        assert!(
            set.sealed().len() >= 2,
            "the narrowed tail must roll under load the wide config absorbed"
        );
        let batch = set.fetch_through(0, 1 << 20, 100, 20).unwrap();
        assert_eq!(
            batch.records.len(),
            20,
            "every record on both sides of the narrowing must read back"
        );
    }

    /// Case 5: validation follows the tail's ACTUAL format. The v1 and v2
    /// frame overheads differ by ten bytes, so the identical numbers are a
    /// working v1 configuration and an impossible v2 one.
    #[test]
    fn validation_follows_the_tails_actual_format() {
        let v1_dir = tempdir().unwrap();
        let mut v1_set =
            SegmentSet::create_in(&Env::real(), v1_dir.path(), descriptor(), config()).unwrap();
        let exactly_one_v1_record = RollThresholds {
            max_record_bytes: Some(256),
            max_group_bytes: Some(256 + crate::types::RECORD_FRAME_OVERHEAD_BYTES),
            max_segment_bytes: Some(512),
            ..RollThresholds::default()
        };
        v1_set
            .reconfigure(exactly_one_v1_record, Uuid::from_u128(51))
            .expect("a group bound of one framed v1 record is a working v1 configuration");

        let v2_dir = tempdir().unwrap();
        let env = Env::real();
        let base = 42;
        let path = v2_dir.path().join(format!("{}.active", segment_stem(base)));
        drop(
            crate::segment::ActiveSegment::create_v2_in(
                &env,
                &path,
                crate::SegmentDescriptorV2 {
                    segment_id: Uuid::from_u128(52),
                    topic: "audit.v1".to_owned(),
                    topic_epoch: 3,
                    lineage: RangeLineage::root(Uuid::from_u128(53)),
                    base_offset: base,
                    segment_generation: 1,
                    creation_node_id: Uuid::from_u128(54),
                    creation_fencing_epoch: 1,
                },
                crate::SegmentConfigV2 {
                    max_record_bytes: 256,
                    max_group_bytes: 1024,
                    max_segment_bytes: 4096,
                    max_segment_records: 100,
                    index_stride: 2,
                    chunk_size: 64 * 1024,
                },
            )
            .unwrap(),
        );
        let mut v2_set = SegmentSet::open_in(&env, v2_dir.path()).unwrap().unwrap();
        let refused = v2_set.reconfigure(exactly_one_v1_record, Uuid::from_u128(55));
        assert!(
            refused.is_err(),
            "the same numbers must be refused on a v2 range: its frame overhead is ten bytes \
             larger, so this group bound cannot fit one framed record"
        );
        assert!(
            v2_set.active().config_v2().is_some(),
            "a refused reconfigure must leave the v2 tail untouched"
        );
    }

    /// The empty-tail path: rewritten in place, because sealing a segment
    /// holding nothing would cost a file to say nothing.
    #[test]
    fn an_empty_tail_is_rewritten_in_place_not_sealed_empty() {
        let directory = tempdir().unwrap();
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        let identity_before = set.active().descriptor().segment_id;

        let outcome = set
            .reconfigure(
                RollThresholds {
                    max_segment_bytes: Some(2048),
                    ..RollThresholds::default()
                },
                Uuid::from_u128(61),
            )
            .unwrap();

        assert_eq!(outcome, ReconfigureOutcome::RewrittenInPlace);
        assert!(
            set.sealed().is_empty(),
            "reconfiguring an empty range must not manufacture an empty sealed segment"
        );
        assert_eq!(
            set.active().descriptor().segment_id,
            identity_before,
            "an in-place rewrite changes the limits, not the segment's identity"
        );
        assert_eq!(set.active().config().max_segment_bytes, 2048);

        // And the rewrite is durable: the reopened range runs the new limits.
        drop(set);
        let reopened = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .unwrap();
        assert_eq!(reopened.active().config().max_segment_bytes, 2048);
    }

    /// Case 1's durability half: the reconfigured limits ARE the header, so
    /// a reopen cannot lose them — there is no second channel to forget.
    #[test]
    fn reconfigured_limits_survive_reopen() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(67);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..5 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(6100 + sequence as u128),
            )
            .unwrap();
        }
        set.reconfigure(
            RollThresholds {
                max_segment_bytes: Some(4096),
                max_group_bytes: Some(1024),
                ..RollThresholds::default()
            },
            Uuid::from_u128(6999),
        )
        .unwrap();
        drop(set);

        let reopened = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .unwrap();
        assert_eq!(
            reopened.active().config().max_segment_bytes,
            4096,
            "the reconfigured limit must come back from the header on reopen"
        );
    }

    /// Case 6: cross-segment truncation replaces the tail — and the
    /// replacement must carry the reconfigured limits, because the intent
    /// captures the live tail's header, which is where they now live.
    #[test]
    fn reconfigured_limits_survive_cross_segment_truncation() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(71);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..5 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(7100 + sequence as u128),
            )
            .unwrap();
        }
        set.reconfigure(
            RollThresholds {
                max_segment_bytes: Some(4096),
                max_group_bytes: Some(1024),
                ..RollThresholds::default()
            },
            Uuid::from_u128(7998),
        )
        .unwrap();
        for sequence in 5..10 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(7200 + sequence as u128),
            )
            .unwrap();
        }
        // A cross-segment cut must land on a sealed boundary; the last
        // sealed segment's base is one, and it is strictly below the tail.
        let below_tail = set.sealed().last().unwrap().base_offset();

        set.truncate_to(below_tail).unwrap();

        assert_eq!(
            set.active().config().max_segment_bytes,
            4096,
            "a fenced follower whose truncation crossed segments must keep the reconfigured \
             limits, not silently revert to the ones in an older header"
        );
    }

    /// Stage the layout a crash between roll_in's seal and its
    /// successor-create leaves: a sealed prefix, no tail — and, when the
    /// sealed segment carried producer state, a lone `.producers` sidecar
    /// for the successor that never came to be.
    fn strand_after_seal(directory: &Path, with_orphan_sidecar: bool) {
        let producer = Uuid::from_u128(77);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory, descriptor(), config()).unwrap();
        for sequence in 0..3 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(7700 + sequence as u128),
            )
            .unwrap();
        }
        let end = set.next_offset();
        drop(set);
        let active_path = directory.join(format!("{}.active", segment_stem(0)));
        let tail = ActiveSegment::recover_in(&Env::real(), &active_path).unwrap();
        drop(tail.seal().expect("sealing stages the crash layout"));
        if with_orphan_sidecar {
            // The exact file roll_in writes in its window, at the exact
            // place: the successor's stem, before the successor exists —
            // and VALID, because write_atomic never leaves a partial file;
            // an undecodable snapshot is a different corruption the sweep
            // rightly refuses to touch.
            std::fs::write(
                directory.join(format!("{}.producers", segment_stem(end))),
                ProducerSnapshot::default().encode().unwrap(),
            )
            .unwrap();
        }
    }

    /// The review's P1: the sidecar roll_in writes before the successor's
    /// own file quarantined discovery, so neither open nor adoption could
    /// ever run — the one interruption window the typed refusal missed.
    #[test]
    fn an_orphan_successor_sidecar_from_an_interrupted_roll_is_consumed() {
        let directory = tempdir().unwrap();
        strand_after_seal(directory.path(), true);

        let refused = SegmentSet::open_in(&Env::real(), directory.path())
            .map(|_| ())
            .expect_err("a prefix without a tail must still refuse to open as a range");
        assert!(
            matches!(refused, LogError::TailSealedWithoutSuccessor { .. }),
            "the roll-window sidecar must be consumed so the refusal is the typed,              recoverable one — a quarantine here strands the range: {refused}"
        );

        let adopted = SegmentSet::adopt_in(&Env::real(), directory.path(), Uuid::from_u128(0xABCD))
            .expect("adoption must proceed once the re-derivable sidecar is consumed");
        assert_eq!(adopted.next_offset(), 3);
        assert_eq!(adopted.sealed().len(), 1);
    }

    /// The sweep's conditions are narrow: a sidecar anywhere but the sealed
    /// end is NOT the roll window, and stays quarantined.
    #[test]
    fn a_sidecar_away_from_the_sealed_end_stays_quarantined() {
        let directory = tempdir().unwrap();
        strand_after_seal(directory.path(), false);
        std::fs::write(
            directory
                .path()
                .join(format!("{}.producers", segment_stem(999))),
            b"not the roll window",
        )
        .unwrap();

        let refused = SegmentSet::open_in(&Env::real(), directory.path())
            .map(|_| ())
            .expect_err("an unexplained sidecar must keep the range quarantined");
        assert!(
            !matches!(refused, LogError::TailSealedWithoutSuccessor { .. }),
            "a sidecar at an offset roll_in never wrote must not be consumed as if it were              the roll window"
        );
    }

    /// The roll's LAST window: the successor's file exists — synced, valid,
    /// empty — but its commit boundary was never written. Recovery refuses
    /// it, rightly in general; for THIS provable shape the boundary is
    /// rebuilt at the base and the open proceeds as if the roll finished.
    #[test]
    fn an_empty_successor_missing_its_commit_boundary_is_repaired_on_open() {
        let directory = tempdir().unwrap();
        // Stage through the REAL roll — sidecar, descriptor, format all
        // exactly what roll_in writes — then delete only the boundary, the
        // one file the crash window never reached.
        let producer = Uuid::from_u128(77);
        let mut staged =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..3 {
            staged
                .append_group(
                    &[record(producer, sequence)],
                    Durability::Fsync,
                    Uuid::from_u128(7900 + sequence as u128),
                )
                .unwrap();
        }
        staged.roll(Uuid::from_u128(0xC0FFEE)).unwrap();
        drop(staged);
        std::fs::remove_file(directory.path().join(format!("{}.commit", segment_stem(3)))).unwrap();

        let set = SegmentSet::open_in(&Env::real(), directory.path())
            .expect("the provably-empty successor must be repaired, not quarantined")
            .expect("the range must open");
        assert_eq!(set.sealed().len(), 1);
        assert_eq!(set.active().base_offset(), 3);
        assert_eq!(
            set.next_offset(),
            3,
            "the repaired successor is empty; its boundary is its base by definition"
        );
    }

    /// The roll's FIRST window: a crash mid-write of the successor's own
    /// header. The commit sidecar's absence proves creation never finished,
    /// so the torn file has never held a record and is discarded — the
    /// layout becomes sealed-only, whose recovery is adoption.
    #[test]
    fn a_torn_successor_header_is_discarded_and_the_range_recovers() {
        let directory = tempdir().unwrap();
        strand_after_seal(directory.path(), true);
        std::fs::write(
            directory.path().join(format!("{}.active", segment_stem(3))),
            b"VTOPSEG1 but torn mid-wr",
        )
        .unwrap();

        let refused = SegmentSet::open_in(&Env::real(), directory.path())
            .map(|_| ())
            .expect_err("a prefix without a usable tail must still refuse to open");
        assert!(
            matches!(refused, LogError::TailSealedWithoutSuccessor { .. }),
            "the torn create and its sidecar must both be consumed, leaving the typed              recoverable refusal: {refused}"
        );
        let adopted = SegmentSet::adopt_in(&Env::real(), directory.path(), Uuid::from_u128(0xDEAD))
            .expect("adoption recovers the discarded roll");
        assert_eq!(adopted.next_offset(), 3);
    }

    /// A header-only active from ANOTHER RANGE at a coincidentally matching
    /// offset must not be promoted — and must not receive a boundary marker
    /// moments before the directory is refused anyway. The foreign header
    /// differs ONLY in its key range: every other identity field matches,
    /// deliberately, because the key range is part of the lineage
    /// `validate_single_lineage` compares and so must be part of what the
    /// repair compares. (Generation 1 with a split parent, because a
    /// generation-zero lineage must be the full keyspace and could not
    /// differ in key range at all.)
    #[test]
    fn a_foreign_empty_active_is_not_given_a_commit_boundary() {
        let directory = tempdir().unwrap();
        let split = |prefix: u64| RangeLineage {
            range_id: Uuid::from_u128(2),
            generation: 1,
            key_range: KeyRange {
                prefix,
                prefix_bits: 1,
            },
            parents: vec![crate::ParentRange {
                range_id: Uuid::from_u128(0xAA),
                generation: 0,
                key_range: KeyRange::full(),
            }],
        };
        let mut base = descriptor();
        base.lineage = split(0);
        let producer = Uuid::from_u128(0xF7);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), base.clone(), config()).unwrap();
        for sequence in 0..3 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(0xF700 + sequence as u128),
            )
            .unwrap();
        }
        // A REAL roll, so the sidecar, the name, and the format are exactly
        // right and the lineage is the ONLY thing the overwrite changes.
        set.roll(Uuid::from_u128(0xF0F)).unwrap();
        drop(set);
        let commit_path = directory.path().join(format!("{}.commit", segment_stem(3)));
        std::fs::remove_file(&commit_path).unwrap();

        let mut foreign = base;
        foreign.segment_id = Uuid::from_u128(0xF0);
        foreign.base_offset = 3;
        foreign.lineage = split(1 << 63);
        let foreign_path = directory.path().join(format!("{}.active", segment_stem(3)));
        std::fs::write(
            &foreign_path,
            crate::codec::encode_header(&crate::codec::SegmentHeader::new(foreign, config()))
                .unwrap(),
        )
        .unwrap();

        SegmentSet::open_in(&Env::real(), directory.path())
            .map(|_| ())
            .expect_err("a mixed-lineage directory must refuse to open");
        assert!(
            !commit_path.exists(),
            "the refusal must not have written a durable boundary into the foreign artifact;              a command that fails must not mutate what it failed on"
        );
    }

    /// The filename is an anchor: a header-only active under any other name
    /// is not a successor a roll could have created, whatever its offsets
    /// say.
    #[test]
    fn an_empty_active_under_a_foreign_name_is_not_promoted() {
        let directory = tempdir().unwrap();
        strand_after_seal(directory.path(), false);
        let mut stray = descriptor();
        stray.segment_id = Uuid::from_u128(0xF2);
        stray.base_offset = 3;
        let stray_path = directory.path().join("zzz.active");
        drop(ActiveSegment::create_in(&Env::real(), &stray_path, stray, config()).unwrap());
        std::fs::remove_file(directory.path().join("zzz.commit")).unwrap();

        let refused = SegmentSet::open_in(&Env::real(), directory.path())
            .map(|_| ())
            .expect_err("a stray active must keep the directory refused");
        assert!(
            !matches!(refused, LogError::TailSealedWithoutSuccessor { .. }),
            "a stray active is not the roll window and must not be consumed as if it were"
        );
        assert!(
            !directory.path().join("zzz.commit").exists(),
            "no boundary may be invented for a file the roll never wrote"
        );
    }

    /// A complete header at a version this binary does not speak is a file
    /// from a NEWER build, not a torn write — deleting it would destroy the
    /// successor's work during a downgrade. The compatibility refusal must
    /// survive the repair path.
    #[test]
    fn a_newer_format_successor_is_refused_not_deleted() {
        let directory = tempdir().unwrap();
        strand_after_seal(directory.path(), false);
        let successor_path = directory.path().join(format!("{}.active", segment_stem(3)));
        let mut future = crate::codec::SegmentHeader::new(descriptor(), config());
        future.version += 1;
        future.descriptor.base_offset = 3;
        std::fs::write(
            &successor_path,
            crate::codec::encode_header(&future).unwrap(),
        )
        .unwrap();

        let refused = SegmentSet::open_in(&Env::real(), directory.path())
            .map(|_| ())
            .expect_err("a newer-format successor must refuse this binary");
        assert!(
            matches!(refused, LogError::UnsupportedVersion(_)),
            "the refusal must be the compatibility error, not a quarantine and never a              deletion: {refused}"
        );
        assert!(
            successor_path.exists(),
            "an older binary must not delete a newer build's successor"
        );
    }

    /// A real roll over producer state writes the frontier BEFORE the
    /// successor's file, so a boundary-less successor with no sidecar over
    /// a predecessor whose records carry producer state cannot be the roll
    /// window — and repairing it would open the tail with an empty
    /// inherited frontier, rejecting a continuing producer or letting a
    /// stale epoch past fencing.
    #[test]
    fn a_successor_without_the_frontier_its_predecessor_requires_stays_quarantined() {
        let directory = tempdir().unwrap();
        strand_after_seal(directory.path(), false);
        let successor_path = directory.path().join(format!("{}.active", segment_stem(3)));
        let mut successor_descriptor = descriptor();
        successor_descriptor.segment_id = Uuid::from_u128(0xC0FFED);
        successor_descriptor.base_offset = 3;
        drop(
            ActiveSegment::create_in(
                &Env::real(),
                &successor_path,
                successor_descriptor,
                config(),
            )
            .unwrap(),
        );
        let commit_path = directory.path().join(format!("{}.commit", segment_stem(3)));
        std::fs::remove_file(&commit_path).unwrap();

        SegmentSet::open_in(&Env::real(), directory.path())
            .map(|_| ())
            .expect_err("a successor missing the frontier its predecessor requires must refuse");
        assert!(
            !commit_path.exists(),
            "no boundary may be written for a tail whose inherited frontier would be wrong"
        );
    }

    /// A COMPLETE file under an unknown magic is a future format, not a
    /// torn write — v2 itself arrived as a new magic older binaries read as
    /// Corrupt. A downgrade must quarantine it, never delete it.
    #[test]
    fn a_future_magic_successor_is_preserved_not_deleted() {
        let directory = tempdir().unwrap();
        strand_after_seal(directory.path(), false);
        let future_path = directory.path().join(format!("{}.active", segment_stem(3)));
        std::fs::write(&future_path, b"VTOPSEG9 a complete file from a newer build").unwrap();

        let refused = SegmentSet::open_in(&Env::real(), directory.path())
            .map(|_| ())
            .expect_err("an unknown-magic successor must refuse this binary");
        assert!(
            !matches!(refused, LogError::TailSealedWithoutSuccessor { .. }),
            "the future file must not have been classified as torn and deleted"
        );
        assert!(
            future_path.exists(),
            "a downgrade must quarantine a newer build's format, never delete it"
        );
    }

    /// A torn successor next to an UNDECODABLE sidecar is two corruptions,
    /// not one window: the discard must not destroy the evidence beside it.
    #[test]
    fn a_torn_successor_beside_a_corrupt_sidecar_stays_quarantined() {
        let directory = tempdir().unwrap();
        strand_after_seal(directory.path(), false);
        let torn_path = directory.path().join(format!("{}.active", segment_stem(3)));
        std::fs::write(&torn_path, b"VTOPSEG1 but torn mid-wr").unwrap();
        let sidecar_path = directory
            .path()
            .join(format!("{}.producers", segment_stem(3)));
        std::fs::write(&sidecar_path, b"not a snapshot").unwrap();

        let refused = SegmentSet::open_in(&Env::real(), directory.path())
            .map(|_| ())
            .expect_err("two corruptions must keep the range quarantined");
        assert!(
            !matches!(refused, LogError::TailSealedWithoutSuccessor { .. }),
            "the pair must not have been consumed as if it were the roll window"
        );
        assert!(
            torn_path.exists() && sidecar_path.exists(),
            "neither file may be deleted while the layout is unexplained"
        );
    }

    /// The repair's guard: a successor holding RECORDS with no boundary
    /// cannot prove what was durable, and must stay refused.
    #[test]
    fn a_successor_with_records_and_no_commit_boundary_stays_quarantined() {
        let directory = tempdir().unwrap();
        strand_after_seal(directory.path(), false);
        let successor_path = directory.path().join(format!("{}.active", segment_stem(3)));
        let mut successor_descriptor = descriptor();
        successor_descriptor.segment_id = Uuid::from_u128(0xC0FFEF);
        successor_descriptor.base_offset = 3;
        let mut successor = ActiveSegment::create_in(
            &Env::real(),
            &successor_path,
            successor_descriptor,
            config(),
        )
        .unwrap();
        successor
            .append_group(
                &[LogRecord {
                    sequence: 0,
                    ..record(Uuid::from_u128(78), 0)
                }],
                Durability::Fsync,
            )
            .unwrap();
        drop(successor);
        std::fs::remove_file(directory.path().join(format!("{}.commit", segment_stem(3)))).unwrap();

        SegmentSet::open_in(&Env::real(), directory.path())
            .map(|_| ())
            .expect_err(
                "a tail with records and no commit boundary cannot prove what was durable;                  repairing it would invent a boundary",
            );
    }

    /// The review's P2: a resume must validate BEFORE it adopts, so a failed
    /// command leaves the directory byte-for-byte as it found it.
    #[test]
    fn an_invalid_stranded_reconfigure_mutates_nothing() {
        let directory = tempdir().unwrap();
        strand_after_seal(directory.path(), false);

        let invalid = RollThresholds {
            // A group bound one framed record cannot fit — refused by
            // validation under every format.
            max_record_bytes: Some(256),
            max_group_bytes: Some(10),
            ..RollThresholds::default()
        };
        SegmentSet::adopt_for_reconfigure(
            &Env::real(),
            directory.path(),
            invalid,
            Uuid::from_u128(0xBEEF),
        )
        .map(|_| ())
        .expect_err("invalid thresholds must be refused before anything is written");
        assert!(
            !directory
                .path()
                .join(format!("{}.active", segment_stem(3)))
                .exists(),
            "a refused resume must not have minted a tail; the failed command changed the              directory it reported failing on"
        );

        let set = SegmentSet::adopt_for_reconfigure(
            &Env::real(),
            directory.path(),
            RollThresholds {
                max_segment_bytes: Some(4096),
                max_group_bytes: Some(1024),
                ..RollThresholds::default()
            },
            Uuid::from_u128(0xBEF0),
        )
        .expect("valid thresholds adopt the stranded range");
        assert_eq!(set.next_offset(), 3);
    }

    /// An override that changes nothing is a no-op — rolling for it would
    /// seal a segment purely to restate its own limits — and an EMPTY
    /// override is the same case, not an error.
    #[test]
    fn an_unchanged_reconfigure_is_a_no_op() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(73);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        set.append_group(
            &[record(producer, 0)],
            Durability::Fsync,
            Uuid::from_u128(1),
        )
        .unwrap();
        let sealed_before = set.sealed().len();

        let restated = set
            .reconfigure(
                RollThresholds {
                    max_segment_bytes: Some(config().max_segment_bytes),
                    ..RollThresholds::default()
                },
                Uuid::from_u128(2),
            )
            .unwrap();
        let empty = set
            .reconfigure(RollThresholds::default(), Uuid::from_u128(3))
            .unwrap();

        assert_eq!(restated, ReconfigureOutcome::Unchanged);
        assert_eq!(empty, ReconfigureOutcome::Unchanged);
        assert_eq!(
            set.sealed().len(),
            sealed_before,
            "restating the current limits must not cost a sealed segment"
        );
    }
}
