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
        sequence,
        timestamp_millis: 1_700_000_000_000 + sequence as i64,
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
