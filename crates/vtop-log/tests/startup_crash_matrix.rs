use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::{tempdir, TempDir};
use uuid::Uuid;
use vtop_log::{
    ActiveSegment, CatalogSegmentState, Durability, LogRecord, QuarantineReason, RangeLineage,
    SegmentConfig, SegmentDescriptor, StartupCatalog,
};

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

fn record(sequence: u64) -> LogRecord {
    LogRecord {
        producer_id: Uuid::from_u128(200),
        producer_epoch: 0,
        sequence,
        timestamp_millis: 1_700_000_000_000 + sequence as i64,
        attributes: 0,
        key: b"key".to_vec(),
        value: format!("record-{sequence}").into_bytes(),
    }
}

fn snapshot(directory: &Path) -> BTreeMap<OsString, Vec<u8>> {
    fs::read_dir(directory)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let path = entry.path();
            (entry.file_name(), fs::read(path).unwrap())
        })
        .collect()
}

fn discover_read_only(directory: &Path) -> StartupCatalog {
    let before = snapshot(directory);
    let first = StartupCatalog::discover(directory).unwrap();
    assert_eq!(
        snapshot(directory),
        before,
        "discovery changed stored bytes"
    );
    let second = StartupCatalog::discover(directory).unwrap();
    assert_eq!(second, first, "startup decision changed between scans");
    assert_eq!(
        snapshot(directory),
        before,
        "repeat discovery changed bytes"
    );
    first
}

#[derive(Clone, Copy)]
enum Expected {
    Active,
    Sealed,
    Invalid,
    ConflictingPrimaries,
}

struct Scenario {
    name: &'static str,
    directory: TempDir,
    expected: Expected,
}

fn create_active(name: &str, segment_id: u128) -> (TempDir, PathBuf) {
    let directory = tempdir().unwrap();
    let path = directory.path().join(format!("{name}.active"));
    drop(ActiveSegment::create(&path, descriptor(segment_id, 0), config()).unwrap());
    (directory, path)
}

fn create_sealed(name: &str, segment_id: u128) -> (TempDir, PathBuf) {
    let (directory, active) = create_active(name, segment_id);
    let sealed = active.with_extension("segment");
    let segment = ActiveSegment::recover(&active).unwrap();
    drop(segment.seal().unwrap());
    (directory, sealed)
}

#[test]
fn startup_matrix_covers_create_commit_and_seal_publication_states() {
    let (created, _) = create_active("created", 1);

    let (missing_commit, missing_commit_active) = create_active("missing-commit", 2);
    fs::remove_file(missing_commit_active.with_extension("commit")).unwrap();

    let (index_published, index_active) = create_sealed("index-published", 3);
    let index_active_path = index_active.with_extension("active");
    fs::rename(&index_active, &index_active_path).unwrap();
    fs::remove_file(index_active.with_extension("manifest.json")).unwrap();

    let (manifest_published, manifest_segment) = create_sealed("manifest-published", 4);
    fs::rename(&manifest_segment, manifest_segment.with_extension("active")).unwrap();

    let (sealed_complete, _) = create_sealed("sealed-complete", 5);

    let (sealed_without_index, sealed_without_index_path) =
        create_sealed("sealed-without-index", 6);
    fs::remove_file(sealed_without_index_path.with_extension("index")).unwrap();

    let (sealed_without_manifest, sealed_without_manifest_path) =
        create_sealed("sealed-without-manifest", 7);
    fs::remove_file(sealed_without_manifest_path.with_extension("manifest.json")).unwrap();

    let (conflicting_primaries, conflicting_segment) = create_sealed("both", 8);
    fs::copy(
        &conflicting_segment,
        conflicting_segment.with_extension("active"),
    )
    .unwrap();

    let scenarios = [
        Scenario {
            name: "active after initial commit publication",
            directory: created,
            expected: Expected::Active,
        },
        Scenario {
            name: "active before initial commit publication",
            directory: missing_commit,
            expected: Expected::Invalid,
        },
        Scenario {
            name: "active after index publication",
            directory: index_published,
            expected: Expected::Active,
        },
        Scenario {
            name: "active after manifest publication but before rename",
            directory: manifest_published,
            expected: Expected::Active,
        },
        Scenario {
            name: "sealed after primary rename",
            directory: sealed_complete,
            expected: Expected::Sealed,
        },
        Scenario {
            name: "sealed with rebuildable index absent",
            directory: sealed_without_index,
            expected: Expected::Sealed,
        },
        Scenario {
            name: "sealed before required manifest publication",
            directory: sealed_without_manifest,
            expected: Expected::Invalid,
        },
        Scenario {
            name: "ambiguous active and sealed primaries",
            directory: conflicting_primaries,
            expected: Expected::ConflictingPrimaries,
        },
    ];

    for scenario in scenarios {
        let catalog = discover_read_only(scenario.directory.path());
        match scenario.expected {
            Expected::Active | Expected::Sealed => {
                assert!(
                    catalog.quarantined.is_empty(),
                    "{} unexpectedly quarantined: {:?}",
                    scenario.name,
                    catalog.quarantined
                );
                assert_eq!(catalog.entries.len(), 1, "{}", scenario.name);
                let expected_state = match scenario.expected {
                    Expected::Active => CatalogSegmentState::Active,
                    Expected::Sealed => CatalogSegmentState::Sealed,
                    _ => unreachable!(),
                };
                assert_eq!(
                    catalog.entries[0].state, expected_state,
                    "{}",
                    scenario.name
                );
            }
            Expected::Invalid => {
                assert!(catalog.entries.is_empty(), "{}", scenario.name);
                assert_eq!(catalog.quarantined.len(), 1, "{}", scenario.name);
                assert!(
                    catalog.quarantined[0]
                        .reasons
                        .iter()
                        .any(|reason| matches!(reason, QuarantineReason::InvalidArtifact(_))),
                    "{}: {:?}",
                    scenario.name,
                    catalog.quarantined[0].reasons
                );
            }
            Expected::ConflictingPrimaries => {
                assert!(catalog.entries.is_empty(), "{}", scenario.name);
                assert_eq!(catalog.quarantined.len(), 1, "{}", scenario.name);
                assert_eq!(
                    catalog.quarantined[0].reasons,
                    vec![QuarantineReason::ConflictingPrimaryFiles],
                    "{}",
                    scenario.name
                );
            }
        }
    }
}

fn descriptor_v2(segment_id: u128) -> vtop_log::SegmentDescriptorV2 {
    vtop_log::SegmentDescriptorV2 {
        segment_id: Uuid::from_u128(segment_id),
        topic: "events.v1".to_owned(),
        topic_epoch: 7,
        lineage: RangeLineage::root(Uuid::from_u128(100)),
        base_offset: 0,
        segment_generation: 3,
        creation_node_id: Uuid::from_u128(500),
        creation_fencing_epoch: 1,
    }
}

fn config_v2() -> vtop_log::SegmentConfigV2 {
    vtop_log::SegmentConfigV2 {
        max_record_bytes: 1024,
        max_group_bytes: 4096,
        max_segment_bytes: 16 * 1024,
        max_segment_records: 100,
        index_stride: 2,
        chunk_size: 64 * 1024,
    }
}

fn create_active_v2(name: &str, segment_id: u128) -> (TempDir, PathBuf) {
    let directory = tempdir().unwrap();
    let path = directory.path().join(format!("{name}.active"));
    let mut segment =
        ActiveSegment::create_v2(&path, descriptor_v2(segment_id), config_v2()).unwrap();
    let mut record = record(0);
    record.producer_epoch = 2;
    segment.append(record, Durability::Fsync).unwrap();
    drop(segment);
    (directory, path)
}

fn create_sealed_v2(name: &str, segment_id: u128) -> (TempDir, PathBuf) {
    let (directory, active) = create_active_v2(name, segment_id);
    let sealed = active.with_extension("segment");
    let segment = ActiveSegment::recover(&active).unwrap();
    drop(segment.seal_v2(None).unwrap());
    (directory, sealed)
}

/// The v2 seal publishes `.chunks`, then `.index`, then `.manifest.json`,
/// then renames the primary. A crash before or after each step must leave a
/// state that classifies without repair: the manifest is the only sidecar
/// that is required rather than rebuildable.
#[test]
fn startup_matrix_covers_v2_chunk_sidecar_publication_states() {
    // Crash after the initial commit publication, before any seal sidecar.
    let (created, _) = create_active_v2("v2-created", 40);

    // Crash after `.chunks` publication, before `.index`.
    let (chunks_published, chunks_segment) = create_sealed_v2("v2-chunks", 41);
    fs::rename(&chunks_segment, chunks_segment.with_extension("active")).unwrap();
    fs::remove_file(chunks_segment.with_extension("index")).unwrap();
    fs::remove_file(chunks_segment.with_extension("manifest.json")).unwrap();

    // Crash after `.index` publication, before `.manifest.json`.
    let (index_published, index_segment) = create_sealed_v2("v2-index", 42);
    fs::rename(&index_segment, index_segment.with_extension("active")).unwrap();
    fs::remove_file(index_segment.with_extension("manifest.json")).unwrap();

    // Crash after `.manifest.json` publication, before the rename.
    let (manifest_published, manifest_segment) = create_sealed_v2("v2-manifest", 43);
    fs::rename(&manifest_segment, manifest_segment.with_extension("active")).unwrap();

    // Crash after the rename: the sealed bundle is complete.
    let (sealed_complete, _) = create_sealed_v2("v2-sealed", 44);

    // A sealed v2 segment with its rebuildable `.chunks` sidecar lost.
    let (sealed_without_chunks, sealed_without_chunks_path) = create_sealed_v2("v2-no-chunks", 45);
    fs::remove_file(sealed_without_chunks_path.with_extension("chunks")).unwrap();

    // A sealed v2 segment with its rebuildable `.index` lost.
    let (sealed_without_index, sealed_without_index_path) = create_sealed_v2("v2-no-index", 46);
    fs::remove_file(sealed_without_index_path.with_extension("index")).unwrap();

    // A sealed v2 segment without its required manifest.
    let (sealed_without_manifest, sealed_without_manifest_path) =
        create_sealed_v2("v2-no-manifest", 47);
    fs::remove_file(sealed_without_manifest_path.with_extension("manifest.json")).unwrap();

    let expectations = [
        (
            "v2 active after initial commit publication",
            &created,
            Some(CatalogSegmentState::Active),
        ),
        (
            "v2 active after chunks publication",
            &chunks_published,
            Some(CatalogSegmentState::Active),
        ),
        (
            "v2 active after index publication",
            &index_published,
            Some(CatalogSegmentState::Active),
        ),
        (
            "v2 active after manifest publication before rename",
            &manifest_published,
            Some(CatalogSegmentState::Active),
        ),
        (
            "v2 sealed after primary rename",
            &sealed_complete,
            Some(CatalogSegmentState::Sealed),
        ),
        (
            "v2 sealed with rebuildable chunks absent",
            &sealed_without_chunks,
            Some(CatalogSegmentState::Sealed),
        ),
        (
            "v2 sealed with rebuildable index absent",
            &sealed_without_index,
            Some(CatalogSegmentState::Sealed),
        ),
        (
            "v2 sealed before required manifest publication",
            &sealed_without_manifest,
            None,
        ),
    ];
    for (name, directory, expected_state) in expectations {
        let catalog = discover_read_only(directory.path());
        match expected_state {
            Some(state) => {
                assert!(
                    catalog.quarantined.is_empty(),
                    "{name} unexpectedly quarantined: {:?}",
                    catalog.quarantined
                );
                assert_eq!(catalog.entries.len(), 1, "{name}");
                assert_eq!(catalog.entries[0].state, state, "{name}");
                assert_eq!(catalog.entries[0].format_version, 2, "{name}");
            }
            None => {
                assert!(catalog.entries.is_empty(), "{name}");
                assert_eq!(catalog.quarantined.len(), 1, "{name}");
                assert!(
                    catalog.quarantined[0]
                        .reasons
                        .iter()
                        .any(|reason| matches!(reason, QuarantineReason::InvalidArtifact(_))),
                    "{name}: {:?}",
                    catalog.quarantined[0].reasons
                );
            }
        }
    }
}

#[test]
fn prior_commit_marker_exposes_only_the_prior_prefix_and_keeps_newer_bytes() {
    let directory = tempdir().unwrap();
    let active_path = directory.path().join("commit-race.active");
    let commit_path = active_path.with_extension("commit");
    let mut segment = ActiveSegment::create(&active_path, descriptor(9, 40), config()).unwrap();
    segment.append(record(0), Durability::Fsync).unwrap();
    let prior_marker = fs::read(&commit_path).unwrap();
    segment.append(record(1), Durability::Fsync).unwrap();
    drop(segment);
    fs::write(&commit_path, prior_marker).unwrap();
    let file_length = fs::metadata(&active_path).unwrap().len();

    let catalog = discover_read_only(directory.path());

    assert!(catalog.quarantined.is_empty());
    assert_eq!(catalog.entries.len(), 1);
    assert_eq!(catalog.entries[0].record_count, 1);
    assert_eq!(catalog.entries[0].next_offset, 41);
    assert_eq!(fs::metadata(&active_path).unwrap().len(), file_length);
}

#[test]
fn incomplete_atomic_sidecar_is_reported_without_hiding_safe_committed_state() {
    let directory = tempdir().unwrap();
    let active_path = directory.path().join("atomic.active");
    let mut segment = ActiveSegment::create(&active_path, descriptor(10, 0), config()).unwrap();
    segment.append(record(0), Durability::Fsync).unwrap();
    drop(segment);
    fs::write(
        directory
            .path()
            .join(".atomic.commit.00000000-0000-0000-0000-000000000001.tmp"),
        b"incomplete replacement marker",
    )
    .unwrap();

    let catalog = discover_read_only(directory.path());

    assert_eq!(catalog.entries.len(), 1);
    assert_eq!(catalog.entries[0].state, CatalogSegmentState::Active);
    assert_eq!(catalog.entries[0].next_offset, 1);
    assert_eq!(catalog.quarantined.len(), 1);
    assert_eq!(
        catalog.quarantined[0].reasons,
        vec![QuarantineReason::IncompleteAtomicWrite]
    );
}
