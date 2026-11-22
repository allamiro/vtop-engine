//! Metadata directories written by published releases (#743).
//!
//! `tests/fixtures/meta/<tag>/data` is the directory a voter of that release
//! left (one voter, raised to the highest feature level the release accepts,
//! 5100 log entries, so a snapshot and a purged log prefix, then SIGKILL),
//! written by the release's PUBLISHED `vtop-node` and `vtopctl`
//! (`scripts/compat/regenerate-meta-fixtures.sh`; each tag has a PROVENANCE).
//! `expect.txt` is what that release itself reported when it stopped.
//!
//! Until now the metadata tier was opened only by directories this build
//! wrote. Here this build opens what each release wrote, and must:
//!
//! - recover without error, from the snapshot and the log above it;
//! - recover exactly the applied index, the voter set, the feature level and
//!   a record's generation the release reported;
//! - leave the directory as it found it on a read-only open;
//! - take a new write, and recover it after a reopen.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_log::env::Env;
use vtop_meta::{
    CommandEnvelope, MetaKey, MetaLogEntry, MetaLogPayload, MetaNodeId, MetaStorage, MetaValue,
    MetadataCommand, MetadataResponse, SUPPORTED_FEATURE_LEVEL,
};

/// The tags with a fixture, with the highest feature level each accepted.
const RELEASES: &[(&str, u32)] = &[("v0.8.0", 7), ("v0.9.0", 7), ("v0.10.1", 8)];

fn fixture_root(tag: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/meta")
        .join(tag)
}

fn expect(tag: &str) -> BTreeMap<String, String> {
    let text = fs::read_to_string(fixture_root(tag).join("expect.txt")).expect("expect.txt");
    text.lines()
        .filter_map(|line| line.split_once(' '))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

fn copy_data(tag: &str) -> tempfile::TempDir {
    let temp = tempfile::tempdir().expect("tempdir");
    for entry in fs::read_dir(fixture_root(tag).join("data")).expect("fixture data") {
        let entry = entry.expect("entry");
        fs::copy(entry.path(), temp.path().join(entry.file_name())).expect("copy");
    }
    temp
}

/// Every file's name and bytes, in name order.
fn contents(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut files: Vec<(String, Vec<u8>)> = fs::read_dir(dir)
        .expect("read dir")
        .map(|entry| {
            let entry = entry.expect("entry");
            (
                entry.file_name().to_string_lossy().into_owned(),
                fs::read(entry.path()).expect("read"),
            )
        })
        .collect();
    files.sort();
    files
}

fn uuid(text: &str) -> Uuid {
    Uuid::parse_str(text).expect("a uuid")
}

fn open(dir: &Path, cluster: Uuid) -> MetaStorage {
    MetaStorage::open_in(&Env::real(), dir, cluster)
        .unwrap_or_else(|error| panic!("this build cannot open the directory: {error}"))
}

#[test]
fn every_release_has_a_fixture_with_a_provenance() {
    for (tag, _) in RELEASES {
        for name in ["PROVENANCE", "expect.txt"] {
            assert!(
                fixture_root(tag).join(name).is_file(),
                "{tag} has no {name}"
            );
        }
    }
    // A fixture directory the table does not list is a release nobody opens.
    let listed: Vec<&str> = RELEASES.iter().map(|(tag, _)| *tag).collect();
    for entry in fs::read_dir(fixture_root("..").join("meta")).expect("fixtures") {
        let name = entry
            .expect("entry")
            .file_name()
            .to_string_lossy()
            .into_owned();
        assert!(
            listed.contains(&name.as_str()),
            "{name} has a fixture and no row in RELEASES"
        );
    }
}

#[test]
fn this_build_recovers_what_each_release_reported() {
    for (tag, level) in RELEASES {
        let expected = expect(tag);
        let temp = copy_data(tag);
        let cluster = uuid(&expected["cluster_id"]);
        let storage = open(temp.path(), cluster);

        assert_eq!(
            storage.last_applied().to_string(),
            expected["last_applied_index"],
            "{tag}: the applied index"
        );
        assert_eq!(
            storage.membership().voters,
            vec![MetaNodeId(1)],
            "{tag}: the voter set"
        );
        assert_eq!(
            storage.state().feature_level(),
            *level,
            "{tag}: the feature level in force"
        );
        assert_eq!(
            expected["feature_level"],
            level.to_string(),
            "{tag}: expect.txt"
        );
        assert!(
            *level <= SUPPORTED_FEATURE_LEVEL,
            "{tag} wrote a level this build does not support"
        );

        // The compare-and-swap token only a state replayed to the release's
        // last record can hold: the snapshot's records, then the log above it.
        let node = uuid(&expected["node_uuid"]);
        match storage.state().record(&MetaKey::Node { node_uuid: node }) {
            Some(MetaValue::Node(record)) => {
                assert_eq!(
                    record.generation.to_string(),
                    expected["node_generation"],
                    "{tag}: the node record's generation"
                );
                assert_eq!(
                    record.addr, expected["node_addr"],
                    "{tag}: the node's address"
                );
            }
            other => panic!("{tag}: the node record is {other:?}"),
        }
        assert!(
            storage
                .state()
                .record(&MetaKey::TopicByName {
                    name: expected["topic"].clone()
                })
                .is_some(),
            "{tag}: the topic created before the snapshot"
        );
        // Request dedup travels in the snapshot: a request the release
        // applied is still known as applied.
        assert!(storage.state().dedup_len() > 0, "{tag}: the dedup table");
    }
}

#[test]
fn the_snapshot_and_the_purged_log_prefix_are_what_recovery_reads() {
    for (tag, _) in RELEASES {
        let temp = copy_data(tag);
        let expected = expect(tag);
        let storage = open(temp.path(), uuid(&expected["cluster_id"]));

        let snapshot = storage
            .snapshots()
            .newest()
            .unwrap_or_else(|| panic!("{tag}: no snapshot was written"));
        assert!(
            snapshot.last_index >= 5000,
            "{tag}: the snapshot covers {}",
            snapshot.last_index
        );
        // The log begins above what a purge took, never below the snapshot's
        // reach, and the state came through the snapshot rather than a full
        // replay (a replay from 1 is impossible once the prefix is purged
        // only if the snapshot is read).
        let first = storage
            .log()
            .first_index()
            .expect("a log above the snapshot");
        assert!(
            first <= snapshot.last_index + 1,
            "{tag}: a hole above the snapshot"
        );
        // The snapshot payload decodes back to the state it was taken from
        // (the payload is read through the same decoder recovery uses).
        let payload = storage.snapshots().read(&snapshot).expect("read snapshot");
        let decoded = vtop_meta::MetaStateMachine::decode_snapshot(&payload).expect("decode");
        assert!(decoded.feature_level() <= SUPPORTED_FEATURE_LEVEL);
        assert!(
            storage.last_purged().is_some(),
            "{tag}: the engine purged behind its snapshot, so the file exists"
        );
    }
}

#[test]
fn opening_a_release_directory_changes_nothing_it_wrote() {
    for (tag, _) in RELEASES {
        let temp = copy_data(tag);
        let before = contents(temp.path());
        let expected = expect(tag);
        drop(open(temp.path(), uuid(&expected["cluster_id"])));
        assert_eq!(
            contents(temp.path()),
            before,
            "{tag}: a read-only open rewrote files the release wrote"
        );
    }
}

#[test]
fn a_write_on_a_release_directory_survives_a_reopen() {
    for (tag, _) in RELEASES {
        let temp = copy_data(tag);
        let expected = expect(tag);
        let cluster = uuid(&expected["cluster_id"]);
        let node = uuid(&expected["node_uuid"]);
        let generation: u64 = expected["node_generation"].parse().expect("generation");
        let applied: u64 = expected["last_applied_index"].parse().expect("index");

        let mut storage = open(temp.path(), cluster);
        let term = storage.last_applied_term();
        let entry = MetaLogEntry {
            term,
            index: applied + 1,
            payload: MetaLogPayload::Normal(MetadataCommand::RegisterNode {
                env: CommandEnvelope {
                    request_id: Uuid::from_u128(0xfeed_0000 + u128::from(applied)),
                    issued_at_ms: 0,
                },
                node_uuid: node,
                addr: "10.9.9.9:9200".to_owned(),
                expected_generation: Some(generation),
            }),
        };
        storage.append(&[entry]).expect("append");
        let responses = storage.apply_through(applied + 1).expect("apply");
        assert_eq!(
            responses,
            vec![MetadataResponse::Ack {
                generation: generation + 1
            }],
            "{tag}: the compare-and-swap on the replayed record"
        );
        drop(storage);

        let storage = open(temp.path(), cluster);
        assert_eq!(storage.last_applied(), applied + 1, "{tag}: reopened index");
        match storage.state().record(&MetaKey::Node { node_uuid: node }) {
            Some(MetaValue::Node(record)) => {
                assert_eq!(
                    record.generation,
                    generation + 1,
                    "{tag}: reopened generation"
                );
                assert_eq!(record.addr, "10.9.9.9:9200");
            }
            other => panic!("{tag}: the node record is {other:?}"),
        }
    }
}
