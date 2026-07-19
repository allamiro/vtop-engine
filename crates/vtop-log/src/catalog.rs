use crate::env::Env;
use crate::segment::{inspect_active_segment, inspect_sealed_segment, SegmentInspection};
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
    pub descriptor: SegmentDescriptor,
    pub record_count: u64,
    pub next_offset: u64,
    pub content_bytes: u64,
    /// Present only for a sealed segment whose stored bytes matched its
    /// canonical v1 manifest. V1 roots are linear integrity digests, not
    /// authenticated proof roots.
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
        for entry in discovered {
            let Some(classification) = classify_artifact(&entry.path) else {
                continue;
            };
            if classification.kind == ArtifactKind::Temporary {
                quarantined.push(QuarantinedArtifacts {
                    paths: vec![entry.path],
                    reasons: vec![QuarantineReason::IncompleteAtomicWrite],
                });
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
            quarantined,
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
            ArtifactKind::Temporary => unreachable!("temporary files are not bundled"),
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
    Temporary,
}

struct ArtifactClassification {
    base: PathBuf,
    kind: ArtifactKind,
}

fn classify_artifact(path: &Path) -> Option<ArtifactClassification> {
    let name = path.file_name()?.to_str()?;
    if name.starts_with('.')
        && name.ends_with(".tmp")
        && [".commit.", ".index.", ".manifest.json."]
            .iter()
            .any(|marker| name.contains(marker))
    {
        return Some(ArtifactClassification {
            base: path.to_path_buf(),
            kind: ArtifactKind::Temporary,
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
    fn quarantines_conflicting_primaries_orphans_and_atomic_temporary_files() {
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
        assert_eq!(catalog.quarantined.len(), 3);
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
        assert!(catalog
            .quarantined
            .iter()
            .any(|item| has_reason(item, |reason| {
                matches!(reason, QuarantineReason::IncompleteAtomicWrite)
            })));
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
