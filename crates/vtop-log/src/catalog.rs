use crate::env::Env;
use crate::retention_intent::{RetentionIntent, RETENTION_INTENT_FILE};
use crate::segment::{inspect_active_segment, inspect_sealed_segment, SegmentInspection};
use crate::truncate_intent::{TruncateIntent, TRUNCATE_INTENT_FILE};
use crate::{LogError, SegmentDescriptor, SegmentId, VtopLogResult};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// A validated local segment that is safe to place in the broker's startup
/// catalog. Discovery is read-only: active tails are not truncated and sparse
/// indexes are not rebuilt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogEntry {
    pub state: CatalogSegmentState,
    pub path: PathBuf,
    /// The v1-shaped identity of the segment; a v2 descriptor projects onto
    /// its common prefix.
    pub descriptor: SegmentDescriptor,
    /// On-disk envelope version of the primary file: 1 or 2.
    pub format_version: u16,
    pub record_count: u64,
    pub next_offset: u64,
    pub content_bytes: u64,
    /// Present only for a sealed segment whose stored bytes matched its
    /// canonical manifest. A v1 root is a linear integrity digest, not an
    /// authenticated proof root; a v2 root is the BLAKE3 chunk-tree root.
    pub sealed_content_root: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CatalogSegmentState {
    Active,
    Sealed,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum QuarantineReason {
    InvalidArtifact(String),
    NonRegularArtifact,
    ConflictingPrimaryFiles,
    OrphanSidecars,
    IncompleteAtomicWrite,
    DuplicateSegmentId(SegmentId),
    ConflictingLogSlot {
        topic: String,
        topic_epoch: u64,
        range_id: Uuid,
        range_generation: u64,
        base_offset: u64,
    },
    ConflictingRangeLineage {
        topic: String,
        topic_epoch: u64,
        range_id: Uuid,
        range_generation: u64,
    },
    OverlappingOffsetIntervals {
        topic: String,
        topic_epoch: u64,
        range_id: Uuid,
        range_generation: u64,
    },
    MultipleActiveSegments {
        topic: String,
        topic_epoch: u64,
        range_id: Uuid,
        range_generation: u64,
    },
    /// A `range.truncate-intent` marker that cannot be decoded. A valid
    /// marker is an in-flight truncation the next open finishes; one that
    /// does not decode names an intent that cannot be honoured, and acting on
    /// a guess would delete segments.
    InvalidTruncateIntent(String),
    /// A retention marker exists but cannot be decoded (#290). Same rule as
    /// the truncation marker: it names an intent that cannot be honoured, so
    /// the range must not open on a guess about which prefix is doomed.
    InvalidRetentionIntent(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantinedArtifacts {
    pub paths: Vec<PathBuf>,
    pub reasons: Vec<QuarantineReason>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StartupCatalog {
    pub entries: Vec<CatalogEntry>,
    pub quarantined: Vec<QuarantinedArtifacts>,
    /// A valid `range.truncate-intent` marker, if one is present: a
    /// cross-segment truncation that was interrupted mid-flight. Discovery
    /// only REPORTS it — it never moves or deletes anything — and the layout
    /// entries above describe the directory as it stands, doomed segments
    /// included. `SegmentSet::open_in` is what finishes the truncation.
    pub truncate_intent: Option<PathBuf>,
    /// A decodable retention marker (#290): a sealed-prefix reclamation was
    /// interrupted and the opener must finish it. Same read-only contract as
    /// `truncate_intent`.
    pub retention_intent: Option<PathBuf>,
    /// Interrupted atomic writes: `.{stem}.<kind>.<uuid>.tmp` files whose
    /// rename never happened.
    ///
    /// REPORTED, NOT QUARANTINED. These used to be quarantined, and a range
    /// with any quarantined bundle refuses to open — so a `kill -9` landing
    /// inside a commit write left a node that would not start, and the remedy
    /// was an operator deleting a file whose name gave them no way to know
    /// that deleting it was safe (#310).
    ///
    /// It is safe by construction. `write_atomic` writes the temp, fsyncs, and
    /// renames over the real name; if the process died before the rename then
    /// the committed state is either the previous file or no file, and in
    /// neither case does the temp carry anything. Quarantine is the right
    /// answer for an artifact that MIGHT hold records — a half-written
    /// segment, an undecodable manifest — and the wrong one for the losing
    /// side of a rename, which the `<uuid>.tmp` naming already identifies.
    ///
    /// Discovery still only reports, keeping its promise above.
    /// `SegmentSet::open_in` removes them, because it owns the directory.
    pub temporary: Vec<PathBuf>,
}

impl StartupCatalog {
    /// Discover and validate native-log artifacts in one directory.
    ///
    /// Invalid or ambiguous bundles are described in `quarantined`; this
    /// method never moves, deletes, repairs, truncates, or otherwise modifies
    /// them. Files unrelated to the native segment naming contract are ignored.
    pub fn discover(directory: impl AsRef<Path>) -> VtopLogResult<Self> {
        Self::discover_in(&Env::real(), directory)
    }

    pub fn discover_in(env: &Env, directory: impl AsRef<Path>) -> VtopLogResult<Self> {
        let directory = directory.as_ref();
        let mut discovered = env
            .storage
            .read_dir(directory)
            .map_err(|source| LogError::Io {
                path: directory.to_path_buf(),
                source,
            })?;
        discovered.sort_by(|left, right| left.path.cmp(&right.path));

        let mut bundles = BTreeMap::<PathBuf, ArtifactBundle>::new();
        let mut quarantined = Vec::new();
        let mut temporary = Vec::new();
        let mut truncate_intent = None;
        let mut retention_intent = None;
        for entry in discovered {
            let Some(classification) = classify_artifact(&entry.path) else {
                continue;
            };
            if classification.kind == ArtifactKind::TruncateIntent {
                // Validated here so a caller reading only the catalog knows
                // whether the marker can be acted on, but never acted on
                // here: discovery is read-only, and finishing a truncation
                // deletes segments.
                let decoded = env
                    .storage
                    .read(&entry.path)
                    .map_err(|source| LogError::Io {
                        path: entry.path.clone(),
                        source,
                    })
                    .and_then(|bytes| TruncateIntent::decode(&bytes));
                match decoded {
                    Ok(_) => truncate_intent = Some(entry.path),
                    Err(error) => quarantined.push(QuarantinedArtifacts {
                        paths: vec![entry.path],
                        reasons: vec![QuarantineReason::InvalidTruncateIntent(error.to_string())],
                    }),
                }
                continue;
            }
            if classification.kind == ArtifactKind::RetentionIntent {
                // Same read-only validation as the truncation marker above.
                let decoded = env
                    .storage
                    .read(&entry.path)
                    .map_err(|source| LogError::Io {
                        path: entry.path.clone(),
                        source,
                    })
                    .and_then(|bytes| RetentionIntent::decode(&bytes));
                match decoded {
                    Ok(_) => retention_intent = Some(entry.path),
                    Err(error) => quarantined.push(QuarantinedArtifacts {
                        paths: vec![entry.path],
                        reasons: vec![QuarantineReason::InvalidRetentionIntent(error.to_string())],
                    }),
                }
                continue;
            }
            if classification.kind == ArtifactKind::Temporary {
                temporary.push(entry.path);
                continue;
            }
            bundles.entry(classification.base).or_default().insert(
                classification.kind,
                entry.path,
                entry.is_regular_file,
            );
        }

        let mut candidates = Vec::new();
        for bundle in bundles.into_values() {
            let paths = bundle.paths();
            let bundle_has_chunks = bundle.chunks.is_some();
            if bundle.has_non_regular {
                quarantined.push(QuarantinedArtifacts {
                    paths,
                    reasons: vec![QuarantineReason::NonRegularArtifact],
                });
                continue;
            }
            let primary = match (&bundle.active, &bundle.sealed) {
                (Some(_), Some(_)) => {
                    quarantined.push(QuarantinedArtifacts {
                        paths,
                        reasons: vec![QuarantineReason::ConflictingPrimaryFiles],
                    });
                    continue;
                }
                (Some(path), None) => (CatalogSegmentState::Active, path),
                (None, Some(path)) => (CatalogSegmentState::Sealed, path),
                (None, None) => {
                    quarantined.push(QuarantinedArtifacts {
                        paths,
                        reasons: vec![QuarantineReason::OrphanSidecars],
                    });
                    continue;
                }
            };
            let inspected = match primary.0 {
                CatalogSegmentState::Active => inspect_active_segment(env, primary.1),
                CatalogSegmentState::Sealed => inspect_sealed_segment(env, primary.1),
            };
            match inspected {
                // A v1 segment never publishes a `.chunks` sidecar, so one
                // sitting beside a valid v1 bundle is an orphan of some other
                // history and quarantines the whole bundle. A v2 segment
                // without its rebuildable sidecar stays catalogable.
                Ok(inspection)
                    if inspection.format_version == crate::types::FORMAT_VERSION
                        && bundle_has_chunks =>
                {
                    quarantined.push(QuarantinedArtifacts {
                        paths,
                        reasons: vec![QuarantineReason::OrphanSidecars],
                    });
                }
                Ok(inspection) => candidates.push(Candidate {
                    entry: catalog_entry(primary.0, primary.1.clone(), inspection),
                    paths,
                }),
                Err(error) => quarantined.push(QuarantinedArtifacts {
                    paths,
                    reasons: vec![QuarantineReason::InvalidArtifact(error.to_string())],
                }),
            }
        }

        let mut reasons = vec![BTreeSet::new(); candidates.len()];
        mark_duplicate_ids(&candidates, &mut reasons);
        mark_conflicting_slots(&candidates, &mut reasons);
        mark_conflicting_lineage(&candidates, &mut reasons);
        mark_overlapping_offsets(&candidates, &mut reasons);
        mark_multiple_active_segments(&candidates, &mut reasons);

        let mut entries = Vec::new();
        for (candidate, reasons) in candidates.into_iter().zip(reasons) {
            if reasons.is_empty() {
                entries.push(candidate.entry);
            } else {
                quarantined.push(QuarantinedArtifacts {
                    paths: candidate.paths,
                    reasons: reasons.into_iter().collect(),
                });
            }
        }
        entries.sort_by_key(entry_sort_key);
        quarantined.sort_by(|left, right| {
            left.paths
                .cmp(&right.paths)
                .then_with(|| left.reasons.cmp(&right.reasons))
        });
        Ok(Self {
            entries,
            temporary,
            quarantined,
            truncate_intent,
            retention_intent,
        })
    }
}

fn catalog_entry(
    state: CatalogSegmentState,
    path: PathBuf,
    inspection: SegmentInspection,
) -> CatalogEntry {
    CatalogEntry {
        state,
        path,
        descriptor: inspection.descriptor,
        format_version: inspection.format_version,
        record_count: inspection.record_count,
        next_offset: inspection.next_offset,
        content_bytes: inspection.content_bytes,
        sealed_content_root: inspection.sealed_content_root,
    }
}

#[derive(Default)]
struct ArtifactBundle {
    active: Option<PathBuf>,
    sealed: Option<PathBuf>,
    commit: Option<PathBuf>,
    index: Option<PathBuf>,
    manifest: Option<PathBuf>,
    chunks: Option<PathBuf>,
    producers: Option<PathBuf>,
    has_non_regular: bool,
}

impl ArtifactBundle {
    fn insert(&mut self, kind: ArtifactKind, path: PathBuf, is_regular: bool) {
        let destination = match kind {
            ArtifactKind::Active => &mut self.active,
            ArtifactKind::Sealed => &mut self.sealed,
            ArtifactKind::Commit => &mut self.commit,
            ArtifactKind::Index => &mut self.index,
            ArtifactKind::Manifest => &mut self.manifest,
            ArtifactKind::Chunks => &mut self.chunks,
            ArtifactKind::Producers => &mut self.producers,
            ArtifactKind::Temporary => unreachable!("temporary files are not bundled"),
            ArtifactKind::TruncateIntent => {
                unreachable!("the truncation marker is range-level, not part of a bundle")
            }
            ArtifactKind::RetentionIntent => {
                unreachable!("the retention marker is range-level, not part of a bundle")
            }
        };
        *destination = Some(path);
        self.has_non_regular |= !is_regular;
    }

    fn paths(&self) -> Vec<PathBuf> {
        let mut paths = [
            self.active.as_ref(),
            self.sealed.as_ref(),
            self.commit.as_ref(),
            self.index.as_ref(),
            self.manifest.as_ref(),
            self.chunks.as_ref(),
            self.producers.as_ref(),
        ]
        .into_iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
        paths.sort();
        paths
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArtifactKind {
    Active,
    Sealed,
    Commit,
    Index,
    Manifest,
    Chunks,
    /// The producer frontier a rolled segment inherited (#270).
    Producers,
    /// The range-level marker of a cross-segment truncation in flight
    /// (#270). Registered so an interrupted truncation is VISIBLE: an
    /// unclassified file is ignored, and ignoring the marker would report a
    /// half-truncated range as merely broken instead of finishable.
    TruncateIntent,
    /// The range-level marker of a sealed-prefix retention in flight (#290).
    /// Registered for the same reason as the truncation marker: ignoring it
    /// would report a half-reclaimed range as broken instead of finishable.
    RetentionIntent,
    Temporary,
}

struct ArtifactClassification {
    base: PathBuf,
    kind: ArtifactKind,
}

/// Does `name` have the exact shape `write_atomic` gives its scratch file,
/// `.{target}.{uuid}.tmp`, for a sidecar target this crate owns?
///
/// The match is deliberately exact and not `contains`: what is classified
/// temporary is DELETED by `SegmentSet::open_in`, so a loose match would
/// silently remove an unrelated dotted `.tmp` that merely mentions a marker
/// (`.notes.commit.backup.tmp`), breaking discovery's promise to ignore
/// files it does not recognize. Requiring the trailing UUID and a recognized
/// target suffix means only names this crate can actually produce qualify.
fn interrupted_atomic_write(name: &str) -> bool {
    let Some(stripped) = name
        .strip_prefix('.')
        .and_then(|rest| rest.strip_suffix(".tmp"))
    else {
        return false;
    };
    let Some((target, uuid)) = stripped.rsplit_once('.') else {
        return false;
    };
    // Parseable is not enough: `Uuid::parse_str` accepts braced, urn,
    // simple, and uppercase spellings, while `write_atomic` only ever emits
    // the lowercase hyphenated form. Whatever qualifies here is DELETED at
    // open, so the gate enforces the writer's exact syntax — a round-trip
    // through the canonical form — or the doc comment above ("only names
    // this crate can actually produce") would be a promise the code breaks.
    let canonical = Uuid::parse_str(uuid)
        .map(|parsed| parsed.as_hyphenated().to_string() == uuid)
        .unwrap_or(false);
    if !canonical {
        return false;
    }
    // The two range-level markers are single fixed names, so their scratch
    // files are matched exactly — a suffix match would sweep an unrelated
    // `.notes.retention-intent.<uuid>.tmp`, which is nobody's scratch file.
    if target == TRUNCATE_INTENT_FILE || target == RETENTION_INTENT_FILE {
        return true;
    }
    [
        ".commit",
        ".index",
        ".manifest.json",
        ".chunks",
        // `write_atomic` names an in-progress sidecar
        // `.{stem}.producers.<uuid>.tmp`. Without this a half-written
        // frontier is classified as a real artifact, and an interrupted
        // roll is reported as a corrupt range rather than an incomplete
        // write.
        ".producers",
    ]
    .iter()
    .any(|suffix| target.ends_with(suffix))
}

fn classify_artifact(path: &Path) -> Option<ArtifactClassification> {
    let name = path.file_name()?.to_str()?;
    if interrupted_atomic_write(name) {
        return Some(ArtifactClassification {
            base: path.to_path_buf(),
            kind: ArtifactKind::Temporary,
        });
    }
    // Exact name, not extension: the marker is one fixed range-level file,
    // and anything else ending in `.truncate-intent` is no more this crate's
    // business than a stray README.
    if name == TRUNCATE_INTENT_FILE {
        return Some(ArtifactClassification {
            base: path.to_path_buf(),
            kind: ArtifactKind::TruncateIntent,
        });
    }
    if name == RETENTION_INTENT_FILE {
        return Some(ArtifactClassification {
            base: path.to_path_buf(),
            kind: ArtifactKind::RetentionIntent,
        });
    }
    if let Some(stem) = name.strip_suffix(".manifest.json") {
        if stem.is_empty() {
            return None;
        }
        return Some(ArtifactClassification {
            base: path.with_file_name(stem),
            kind: ArtifactKind::Manifest,
        });
    }
    let kind = match path.extension() {
        Some(extension) if extension == OsStr::new("active") => ArtifactKind::Active,
        Some(extension) if extension == OsStr::new("segment") => ArtifactKind::Sealed,
        Some(extension) if extension == OsStr::new("commit") => ArtifactKind::Commit,
        Some(extension) if extension == OsStr::new("index") => ArtifactKind::Index,
        Some(extension) if extension == OsStr::new("chunks") => ArtifactKind::Chunks,
        // Registered so an incomplete roll is VISIBLE. A `.producers` written
        // for a successor that was never created is an orphan sidecar, and
        // discovery only reports what it recognises — an unclassified file is
        // ignored, so the half-finished roll would look like a healthy range.
        Some(extension) if extension == OsStr::new("producers") => ArtifactKind::Producers,
        _ => return None,
    };
    Some(ArtifactClassification {
        base: path.with_extension(""),
        kind,
    })
}

struct Candidate {
    entry: CatalogEntry,
    paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct LogSlot {
    topic: String,
    topic_epoch: u64,
    range_id: Uuid,
    range_generation: u64,
    base_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RangeGeneration {
    topic: String,
    topic_epoch: u64,
    range_id: Uuid,
    range_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct LineageShape {
    key_prefix: u64,
    key_prefix_bits: u8,
    parents: Vec<(Uuid, u64, u64, u8)>,
}

fn log_slot(descriptor: &SegmentDescriptor) -> LogSlot {
    LogSlot {
        topic: descriptor.topic.clone(),
        topic_epoch: descriptor.topic_epoch,
        range_id: descriptor.lineage.range_id,
        range_generation: descriptor.lineage.generation,
        base_offset: descriptor.base_offset,
    }
}

fn range_generation(descriptor: &SegmentDescriptor) -> RangeGeneration {
    RangeGeneration {
        topic: descriptor.topic.clone(),
        topic_epoch: descriptor.topic_epoch,
        range_id: descriptor.lineage.range_id,
        range_generation: descriptor.lineage.generation,
    }
}

fn lineage_shape(descriptor: &SegmentDescriptor) -> LineageShape {
    let mut parents = descriptor
        .lineage
        .parents
        .iter()
        .map(|parent| {
            (
                parent.range_id,
                parent.generation,
                parent.key_range.prefix,
                parent.key_range.prefix_bits,
            )
        })
        .collect::<Vec<_>>();
    parents.sort_unstable();
    LineageShape {
        key_prefix: descriptor.lineage.key_range.prefix,
        key_prefix_bits: descriptor.lineage.key_range.prefix_bits,
        parents,
    }
}

fn mark_duplicate_ids(candidates: &[Candidate], reasons: &mut [BTreeSet<QuarantineReason>]) {
    let mut by_id = BTreeMap::<SegmentId, Vec<usize>>::new();
    for (index, candidate) in candidates.iter().enumerate() {
        by_id
            .entry(candidate.entry.descriptor.segment_id)
            .or_default()
            .push(index);
    }
    for (segment_id, indices) in by_id {
        if indices.len() > 1 {
            for index in indices {
                reasons[index].insert(QuarantineReason::DuplicateSegmentId(segment_id));
            }
        }
    }
}

fn mark_conflicting_slots(candidates: &[Candidate], reasons: &mut [BTreeSet<QuarantineReason>]) {
    let mut by_slot = BTreeMap::<LogSlot, Vec<usize>>::new();
    for (index, candidate) in candidates.iter().enumerate() {
        by_slot
            .entry(log_slot(&candidate.entry.descriptor))
            .or_default()
            .push(index);
    }
    for (slot, indices) in by_slot {
        if indices.len() > 1 {
            let reason = QuarantineReason::ConflictingLogSlot {
                topic: slot.topic,
                topic_epoch: slot.topic_epoch,
                range_id: slot.range_id,
                range_generation: slot.range_generation,
                base_offset: slot.base_offset,
            };
            for index in indices {
                reasons[index].insert(reason.clone());
            }
        }
    }
}

fn mark_conflicting_lineage(candidates: &[Candidate], reasons: &mut [BTreeSet<QuarantineReason>]) {
    let mut by_range = BTreeMap::<RangeGeneration, Vec<usize>>::new();
    for (index, candidate) in candidates.iter().enumerate() {
        by_range
            .entry(range_generation(&candidate.entry.descriptor))
            .or_default()
            .push(index);
    }
    for (range, indices) in by_range {
        let shapes = indices
            .iter()
            .map(|index| lineage_shape(&candidates[*index].entry.descriptor))
            .collect::<BTreeSet<_>>();
        if shapes.len() > 1 {
            let reason = QuarantineReason::ConflictingRangeLineage {
                topic: range.topic,
                topic_epoch: range.topic_epoch,
                range_id: range.range_id,
                range_generation: range.range_generation,
            };
            for index in indices {
                reasons[index].insert(reason.clone());
            }
        }
    }
}

fn mark_overlapping_offsets(candidates: &[Candidate], reasons: &mut [BTreeSet<QuarantineReason>]) {
    let mut by_range = BTreeMap::<RangeGeneration, Vec<usize>>::new();
    for (index, candidate) in candidates.iter().enumerate() {
        by_range
            .entry(range_generation(&candidate.entry.descriptor))
            .or_default()
            .push(index);
    }
    for (range, mut indices) in by_range {
        indices.sort_by_key(|index| {
            (
                candidates[*index].entry.descriptor.base_offset,
                candidates[*index].entry.next_offset,
                candidates[*index].entry.descriptor.segment_id,
            )
        });
        let reason = QuarantineReason::OverlappingOffsetIntervals {
            topic: range.topic,
            topic_epoch: range.topic_epoch,
            range_id: range.range_id,
            range_generation: range.range_generation,
        };
        let mut frontier: Option<(u64, usize)> = None;
        for index in indices {
            let start = candidates[index].entry.descriptor.base_offset;
            let end = candidates[index].entry.next_offset;
            if let Some((frontier_end, frontier_index)) = frontier {
                if start < frontier_end {
                    reasons[index].insert(reason.clone());
                    reasons[frontier_index].insert(reason.clone());
                }
                if end > frontier_end {
                    frontier = Some((end, index));
                }
            } else {
                frontier = Some((end, index));
            }
        }
    }
}

fn mark_multiple_active_segments(
    candidates: &[Candidate],
    reasons: &mut [BTreeSet<QuarantineReason>],
) {
    let mut by_range = BTreeMap::<RangeGeneration, Vec<usize>>::new();
    for (index, candidate) in candidates.iter().enumerate() {
        if candidate.entry.state == CatalogSegmentState::Active {
            by_range
                .entry(range_generation(&candidate.entry.descriptor))
                .or_default()
                .push(index);
        }
    }
    for (range, indices) in by_range {
        if indices.len() > 1 {
            let reason = QuarantineReason::MultipleActiveSegments {
                topic: range.topic,
                topic_epoch: range.topic_epoch,
                range_id: range.range_id,
                range_generation: range.range_generation,
            };
            for index in indices {
                reasons[index].insert(reason.clone());
            }
        }
    }
}

fn entry_sort_key(entry: &CatalogEntry) -> (LogSlot, CatalogSegmentState, SegmentId, PathBuf) {
    (
        log_slot(&entry.descriptor),
        entry.state,
        entry.descriptor.segment_id,
        entry.path.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ActiveSegment, Durability, KeyRange, LogRecord, ParentRange, RangeLineage, SegmentConfig,
        SegmentReader,
    };
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use tempfile::tempdir;

    fn descriptor(segment_id: u128, base_offset: u64) -> SegmentDescriptor {
        SegmentDescriptor {
            segment_id: Uuid::from_u128(segment_id),
            topic: "events.v1".to_owned(),
            topic_epoch: 7,
            lineage: RangeLineage::root(Uuid::from_u128(100)),
            base_offset,
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

    fn child_descriptor(segment_id: u128, base_offset: u64, right: bool) -> SegmentDescriptor {
        let parent = KeyRange::full();
        let children = parent.children().unwrap();
        SegmentDescriptor {
            segment_id: Uuid::from_u128(segment_id),
            topic: "events.v1".to_owned(),
            topic_epoch: 7,
            lineage: RangeLineage {
                range_id: Uuid::from_u128(101),
                generation: 1,
                key_range: if right { children.1 } else { children.0 },
                parents: vec![ParentRange {
                    range_id: Uuid::from_u128(100),
                    generation: 0,
                    key_range: parent,
                }],
            },
            base_offset,
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

    fn has_reason(
        quarantined: &QuarantinedArtifacts,
        predicate: impl Fn(&QuarantineReason) -> bool,
    ) -> bool {
        quarantined.reasons.iter().any(predicate)
    }

    #[test]
    fn discovers_committed_active_without_truncating_buffered_tail() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("active.active");
        let mut segment = ActiveSegment::create(&path, descriptor(1, 40), config()).unwrap();
        segment
            .append(record(10, 0, b"committed"), Durability::Fsync)
            .unwrap();
        segment
            .append(record(10, 1, b"buffered"), Durability::Buffered)
            .unwrap();
        let length_before = fs::metadata(&path).unwrap().len();
        drop(segment);

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.quarantined.is_empty());
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].state, CatalogSegmentState::Active);
        assert_eq!(catalog.entries[0].record_count, 1);
        assert_eq!(catalog.entries[0].next_offset, 41);
        assert_eq!(fs::metadata(&path).unwrap().len(), length_before);

        let recovered = ActiveSegment::recover(&path).unwrap();
        assert!(recovered.recovery_report().truncated_bytes > 0);
    }

    #[test]
    fn validates_sealed_bytes_without_rebuilding_missing_index() {
        let directory = tempdir().unwrap();
        let active = directory.path().join("sealed.active");
        let index = directory.path().join("sealed.index");
        let mut segment = ActiveSegment::create(&active, descriptor(2, 0), config()).unwrap();
        segment
            .append(record(20, 0, b"stored"), Durability::Fsync)
            .unwrap();
        drop(segment.seal().unwrap());
        fs::remove_file(&index).unwrap();

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.quarantined.is_empty());
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].state, CatalogSegmentState::Sealed);
        assert!(catalog.entries[0].sealed_content_root.is_some());
        assert!(!index.exists());

        drop(SegmentReader::open(directory.path().join("sealed.segment")).unwrap());
        assert!(index.exists());
    }

    #[test]
    fn accepts_prepublication_active_with_complete_seal_sidecars() {
        let directory = tempdir().unwrap();
        let active = directory.path().join("publishing.active");
        let sealed = directory.path().join("publishing.segment");
        let segment = ActiveSegment::create(&active, descriptor(3, 0), config()).unwrap();
        drop(segment.seal().unwrap());
        fs::rename(&sealed, &active).unwrap();

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.quarantined.is_empty());
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].state, CatalogSegmentState::Active);
    }

    #[test]
    fn quarantines_invalid_primary_without_modifying_it() {
        let directory = tempdir().unwrap();
        let active = directory.path().join("damaged.active");
        let commit = directory.path().join("damaged.commit");
        let mut segment = ActiveSegment::create(&active, descriptor(4, 0), config()).unwrap();
        segment
            .append(record(40, 0, b"durable"), Durability::Fsync)
            .unwrap();
        drop(segment);
        fs::write(&commit, b"bad marker").unwrap();
        let bytes_before = fs::read(&active).unwrap();

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.entries.is_empty());
        assert_eq!(catalog.quarantined.len(), 1);
        assert!(has_reason(&catalog.quarantined[0], |reason| matches!(
            reason,
            QuarantineReason::InvalidArtifact(message)
                if message.contains("commit boundary")
        )));
        assert_eq!(fs::read(&active).unwrap(), bytes_before);
    }

    #[test]
    fn quarantines_sealed_content_that_no_longer_matches_manifest() {
        let directory = tempdir().unwrap();
        let active = directory.path().join("tampered.active");
        let sealed = directory.path().join("tampered.segment");
        let segment = ActiveSegment::create(&active, descriptor(5, 0), config()).unwrap();
        drop(segment.seal().unwrap());
        OpenOptions::new()
            .append(true)
            .open(&sealed)
            .unwrap()
            .write_all(b"tamper")
            .unwrap();
        let length_before = fs::metadata(&sealed).unwrap().len();

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.entries.is_empty());
        assert_eq!(catalog.quarantined.len(), 1);
        assert!(has_reason(&catalog.quarantined[0], |reason| matches!(
            reason,
            QuarantineReason::InvalidArtifact(_)
        )));
        assert_eq!(fs::metadata(&sealed).unwrap().len(), length_before);
    }

    #[test]
    fn quarantines_duplicate_ids_conflicting_slots_and_multiple_actives() {
        let directory = tempdir().unwrap();
        ActiveSegment::create(
            directory.path().join("a.active"),
            descriptor(6, 0),
            config(),
        )
        .unwrap();
        ActiveSegment::create(
            directory.path().join("b.active"),
            descriptor(6, 100),
            config(),
        )
        .unwrap();
        let first = ActiveSegment::create(
            directory.path().join("c.active"),
            descriptor(7, 200),
            config(),
        )
        .unwrap();
        drop(first.seal().unwrap());
        let second = ActiveSegment::create(
            directory.path().join("d.active"),
            descriptor(8, 200),
            config(),
        )
        .unwrap();
        drop(second.seal().unwrap());

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.entries.is_empty());
        assert_eq!(catalog.quarantined.len(), 4);
        assert_eq!(
            catalog
                .quarantined
                .iter()
                .filter(|item| has_reason(item, |reason| matches!(
                    reason,
                    QuarantineReason::DuplicateSegmentId(id) if *id == Uuid::from_u128(6)
                )))
                .count(),
            2
        );
        assert_eq!(
            catalog
                .quarantined
                .iter()
                .filter(|item| has_reason(item, |reason| matches!(
                    reason,
                    QuarantineReason::ConflictingLogSlot {
                        base_offset: 200,
                        ..
                    }
                )))
                .count(),
            2
        );
        assert_eq!(
            catalog
                .quarantined
                .iter()
                .filter(|item| has_reason(item, |reason| matches!(
                    reason,
                    QuarantineReason::MultipleActiveSegments { .. }
                )))
                .count(),
            2
        );
    }

    #[test]
    fn quarantines_conflicting_lineage_for_one_range_generation() {
        let directory = tempdir().unwrap();
        let left = ActiveSegment::create(
            directory.path().join("left.active"),
            child_descriptor(12, 0, false),
            config(),
        )
        .unwrap();
        drop(left.seal().unwrap());
        let right = ActiveSegment::create(
            directory.path().join("right.active"),
            child_descriptor(13, 100, true),
            config(),
        )
        .unwrap();
        drop(right.seal().unwrap());

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.entries.is_empty());
        assert_eq!(catalog.quarantined.len(), 2);
        assert!(catalog
            .quarantined
            .iter()
            .all(|item| has_reason(item, |reason| matches!(
                reason,
                QuarantineReason::ConflictingRangeLineage { .. }
            ))));
    }

    #[test]
    fn quarantines_overlapping_offset_intervals_without_guessing_a_winner() {
        let directory = tempdir().unwrap();
        let mut first = ActiveSegment::create(
            directory.path().join("first.active"),
            descriptor(14, 0),
            config(),
        )
        .unwrap();
        first
            .append_group(
                &[record(140, 0, b"zero"), record(140, 1, b"one")],
                Durability::Fsync,
            )
            .unwrap();
        drop(first.seal().unwrap());
        let mut second = ActiveSegment::create(
            directory.path().join("second.active"),
            descriptor(15, 1),
            config(),
        )
        .unwrap();
        second
            .append(record(150, 0, b"overlap"), Durability::Fsync)
            .unwrap();
        drop(second.seal().unwrap());

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.entries.is_empty());
        assert_eq!(catalog.quarantined.len(), 2);
        assert!(catalog
            .quarantined
            .iter()
            .all(|item| has_reason(item, |reason| matches!(
                reason,
                QuarantineReason::OverlappingOffsetIntervals { .. }
            ))));
    }

    #[test]
    fn quarantines_conflicting_primaries_and_orphans_but_reports_interrupted_writes() {
        let directory = tempdir().unwrap();
        let active = directory.path().join("both.active");
        let sealed = directory.path().join("both.segment");
        let segment = ActiveSegment::create(&active, descriptor(9, 0), config()).unwrap();
        drop(segment.seal().unwrap());
        fs::copy(&sealed, &active).unwrap();
        fs::write(directory.path().join("orphan.commit"), b"orphan").unwrap();
        fs::write(
            directory
                .path()
                .join(".pending.manifest.json.00000000-0000-0000-0000-000000000001.tmp"),
            b"temporary",
        )
        .unwrap();
        fs::write(directory.path().join("README.txt"), b"unrelated").unwrap();

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.entries.is_empty());
        // TWO, not three. An interrupted atomic write is reported separately
        // and no longer quarantines the range — quarantine is for artifacts
        // that might hold records, and the losing side of a rename does not
        // (#310).
        assert_eq!(catalog.quarantined.len(), 2);
        assert_eq!(
            catalog.temporary.len(),
            1,
            "the interrupted write must still be REPORTED, so the opener can remove it"
        );
        assert!(catalog.temporary[0]
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".tmp")));
        assert!(catalog
            .quarantined
            .iter()
            .any(|item| has_reason(item, |reason| {
                matches!(reason, QuarantineReason::ConflictingPrimaryFiles)
            })));
        assert!(catalog
            .quarantined
            .iter()
            .any(|item| has_reason(item, |reason| {
                matches!(reason, QuarantineReason::OrphanSidecars)
            })));
        assert!(
            !catalog
                .quarantined
                .iter()
                .any(|item| has_reason(item, |reason| matches!(
                    reason,
                    QuarantineReason::IncompleteAtomicWrite
                ))),
            "an interrupted atomic write must not quarantine the range: it made a kill -9 during \
             a commit write leave a node that would not start"
        );
    }

    /// A dotted `.tmp` that merely mentions a sidecar marker is NOT this
    /// crate's file. What is classified temporary gets DELETED by the opener,
    /// so a `contains` match would silently remove operator or application
    /// data; only the exact `write_atomic` shape `.{target}.{uuid}.tmp` may
    /// qualify.
    #[test]
    fn an_unrelated_dotted_tmp_is_ignored_not_classified_temporary() {
        let directory = tempdir().unwrap();
        for name in [
            // Mentions `.commit.` but the trailing component is not a UUID.
            ".notes.commit.backup.tmp",
            // Valid UUID but no recognized sidecar target before it.
            ".scratch.00000000-0000-0000-0000-000000000001.tmp",
            // Recognized marker but not in target position.
            ".commit.notes.backup.tmp",
            // Valid UUID and an intent-marker SUFFIX, but not the one fixed
            // range-level name — the markers are matched exactly, so these
            // are nobody's scratch files (#290).
            ".notes.retention-intent.00000000-0000-0000-0000-000000000002.tmp",
            ".notes.truncate-intent.00000000-0000-0000-0000-000000000003.tmp",
            // UUID spellings `parse_str` accepts but `write_atomic` never
            // emits — braced, uppercase, simple, urn. The gate demands the
            // writer's exact lowercase hyphenated form, because whatever
            // qualifies is deleted at open (#407).
            ".notes.commit.{052480a2-9d38-4915-b5c1-773eb42625a7}.tmp",
            ".notes.commit.052480A2-9D38-4915-B5C1-773EB42625A7.tmp",
            ".notes.commit.052480a29d384915b5c1773eb42625a7.tmp",
            ".notes.commit.urn:uuid:052480a2-9d38-4915-b5c1-773eb42625a7.tmp",
        ] {
            fs::write(directory.path().join(name), b"not ours").unwrap();
        }

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(
            catalog.temporary.is_empty(),
            "unrelated files must never be swept: {:?}",
            catalog.temporary
        );
        assert!(
            catalog.quarantined.is_empty(),
            "and ignoring them must not quarantine anything either: {:?}",
            catalog.quarantined
        );
    }

    fn descriptor_v2(segment_id: u128, base_offset: u64) -> crate::SegmentDescriptorV2 {
        crate::SegmentDescriptorV2 {
            segment_id: Uuid::from_u128(segment_id),
            topic: "events.v1".to_owned(),
            topic_epoch: 7,
            lineage: RangeLineage::root(Uuid::from_u128(100)),
            base_offset,
            segment_generation: 3,
            creation_node_id: Uuid::from_u128(500),
            creation_fencing_epoch: 1,
        }
    }

    fn config_v2() -> crate::SegmentConfigV2 {
        crate::SegmentConfigV2 {
            max_record_bytes: 1024,
            max_group_bytes: 4096,
            max_segment_bytes: 16 * 1024,
            max_segment_records: 100,
            index_stride: 2,
            chunk_size: 64 * 1024,
        }
    }

    fn record_v2(producer: u128, epoch: u64, sequence: u64, value: &[u8]) -> LogRecord {
        LogRecord {
            producer_epoch: epoch,
            ..record(producer, sequence, value)
        }
    }

    #[test]
    fn mixed_directory_catalogs_v1_and_v2_bundles_with_their_format_versions() {
        let directory = tempdir().unwrap();
        let mut v1 = ActiveSegment::create(
            directory.path().join("v1.active"),
            descriptor(30, 0),
            config(),
        )
        .unwrap();
        v1.append(record(300, 0, b"v1-sealed"), Durability::Fsync)
            .unwrap();
        drop(v1.seal().unwrap());
        let mut v2 = ActiveSegment::create_v2(
            directory.path().join("v2.active"),
            descriptor_v2(31, 100),
            config_v2(),
        )
        .unwrap();
        v2.append(record_v2(301, 2, 0, b"v2-sealed"), Durability::Fsync)
            .unwrap();
        let v2_root = v2
            .seal_v2(None)
            .unwrap()
            .manifest_v2()
            .unwrap()
            .chunk_tree_root
            .clone();
        let mut v2_active = ActiveSegment::create_v2(
            directory.path().join("v2-active.active"),
            descriptor_v2(32, 200),
            config_v2(),
        )
        .unwrap();
        v2_active
            .append(record_v2(302, 1, 0, b"v2-active"), Durability::Fsync)
            .unwrap();
        drop(v2_active);

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);
        assert_eq!(catalog.entries.len(), 3);
        assert_eq!(
            catalog
                .entries
                .iter()
                .map(|entry| (
                    entry.descriptor.base_offset,
                    entry.format_version,
                    entry.state
                ))
                .collect::<Vec<_>>(),
            vec![
                (0, 1, CatalogSegmentState::Sealed),
                (100, 2, CatalogSegmentState::Sealed),
                (200, 2, CatalogSegmentState::Active),
            ]
        );
        assert_eq!(
            catalog.entries[1].sealed_content_root.as_deref(),
            Some(v2_root.as_str())
        );
        assert!(catalog.entries[2].sealed_content_root.is_none());
    }

    #[test]
    fn v1_bundle_with_stray_chunk_sidecar_is_quarantined_as_orphan() {
        let directory = tempdir().unwrap();
        let sealed = ActiveSegment::create(
            directory.path().join("stray.active"),
            descriptor(33, 0),
            config(),
        )
        .unwrap();
        drop(sealed.seal().unwrap());
        fs::write(directory.path().join("stray.chunks"), b"not a v1 artifact").unwrap();

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.entries.is_empty());
        assert_eq!(catalog.quarantined.len(), 1);
        assert_eq!(
            catalog.quarantined[0].reasons,
            vec![QuarantineReason::OrphanSidecars]
        );
        assert!(catalog.quarantined[0]
            .paths
            .contains(&directory.path().join("stray.chunks")));
    }

    #[test]
    fn v2_segment_without_chunk_sidecar_is_catalogable_and_rebuilt_on_open() {
        let directory = tempdir().unwrap();
        let mut segment = ActiveSegment::create_v2(
            directory.path().join("rebuildable.active"),
            descriptor_v2(34, 0),
            config_v2(),
        )
        .unwrap();
        segment
            .append(record_v2(340, 1, 0, b"stored"), Durability::Fsync)
            .unwrap();
        drop(segment.seal_v2(None).unwrap());
        let chunks = directory.path().join("rebuildable.chunks");
        let pristine = fs::read(&chunks).unwrap();
        fs::remove_file(&chunks).unwrap();

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].format_version, 2);
        assert!(!chunks.exists());

        drop(SegmentReader::open(directory.path().join("rebuildable.segment")).unwrap());
        assert_eq!(fs::read(&chunks).unwrap(), pristine);
    }

    /// A valid truncation marker is reported, never quarantined: it is an
    /// in-flight repair the next open finishes, and the segments beside it —
    /// doomed or not — are still described as they stand, because discovery
    /// only reads.
    #[test]
    fn a_valid_truncation_intent_is_reported_not_quarantined() {
        let directory = tempdir().unwrap();
        let mut segment = ActiveSegment::create(
            directory.path().join("range-00000000000000000000.active"),
            descriptor(20, 0),
            config(),
        )
        .unwrap();
        segment
            .append(record(200, 0, b"kept"), Durability::Fsync)
            .unwrap();
        drop(segment);
        let marker = directory.path().join(super::TRUNCATE_INTENT_FILE);
        let intent = TruncateIntent {
            target_offset: 1,
            replacement: crate::truncate_intent::ReplacementTail::V1 {
                descriptor: descriptor(21, 1),
                config: config(),
            },
            doomed: vec![crate::truncate_intent::DoomedSegment {
                segment_id: Uuid::from_u128(22),
                base_offset: 1,
            }],
            inherited: Default::default(),
        };
        fs::write(&marker, intent.encode().unwrap()).unwrap();

        let catalog = StartupCatalog::discover(directory.path()).unwrap();

        assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.truncate_intent, Some(marker.clone()));
        assert!(marker.exists(), "discovery must not consume the marker");
    }

    #[test]
    fn catalog_order_is_independent_of_creation_and_directory_iteration_order() {
        let directory = tempdir().unwrap();
        let later = ActiveSegment::create(
            directory.path().join("z-later.active"),
            descriptor(11, 100),
            config(),
        )
        .unwrap();
        drop(later.seal().unwrap());
        let earlier = ActiveSegment::create(
            directory.path().join("a-earlier.active"),
            descriptor(10, 0),
            config(),
        )
        .unwrap();
        drop(earlier.seal().unwrap());

        let first = StartupCatalog::discover(directory.path()).unwrap();
        let second = StartupCatalog::discover(directory.path()).unwrap();

        assert_eq!(first, second);
        assert_eq!(
            first
                .entries
                .iter()
                .map(|entry| entry.descriptor.base_offset)
                .collect::<Vec<_>>(),
            vec![0, 100]
        );
    }
}
