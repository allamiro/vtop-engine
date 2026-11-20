//! The `meta_storage_files` fuzz target's seeds (#729): bundles of a real
//! metadata data directory (`SimDiskState::to_bundle`) written through
//! `MetaStorage` on the simulated disk, so the fuzzer starts from every file
//! the store keeps: the hard state, the log chunks and their entry frames,
//! a snapshot, and the applied, purged and membership frontiers.
//!
//! `the_committed_fuzz_seeds_open` keeps the committed seeds honest: each
//! must restore to a directory this build recovers, so a format change that
//! strands them fails here. `write_fuzz_seeds` rewrites them.

use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_log::sim::{SimDiskState, SimStorage};
use vtop_meta::{
    Attribution, CommandActor, CommandEnvelope, HardState, MetaLogEntry, MetaLogPayload,
    MetaMembership, MetaNodeId, MetaStorage, MetadataCommand,
};

const SEED: u64 = 0x5eed_0729;
/// The data directory and cluster the fuzz target reads under; keep them in
/// step with fuzz/fuzz_targets/meta_storage_files.rs.
const ROOT: &str = "/meta";
const CLUSTER: u128 = 0xc1c1_c1c1_0000_4000_8000_0000_0000_0729;

fn seed_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/meta_storage_files")
}

fn command(seed: u128) -> MetadataCommand {
    MetadataCommand::CreateTopic {
        env: CommandEnvelope {
            request_id: Uuid::from_u128(seed),
            issued_at_ms: 1_750_000_000_000,
        },
        name: format!("events.{seed}"),
        topic_uuid: Uuid::from_u128(0x1000 + seed),
        root_range_uuid: Uuid::from_u128(0x2000 + seed),
    }
}

fn entries(term: u64, from: u64, to: u64) -> Vec<MetaLogEntry> {
    (from..=to)
        .map(|index| MetaLogEntry {
            term,
            index,
            payload: match index {
                1 => MetaLogPayload::Membership(MetaMembership {
                    voters: vec![MetaNodeId(1), MetaNodeId(2), MetaNodeId(3)],
                    learners: vec![],
                    joint_outgoing: None,
                }),
                4 => MetaLogPayload::Blank,
                _ => MetaLogPayload::Normal(command(u128::from(index))),
            },
        })
        .collect()
}

fn seeds() -> Vec<(&'static str, Vec<u8>)> {
    let cluster = Uuid::from_u128(CLUSTER);
    let mut seeds = Vec::new();

    // A log and a hard state, nothing applied durably yet.
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    let mut storage = MetaStorage::open_in(&env, ROOT, cluster).unwrap();
    storage
        .save_hard_state(HardState {
            term: 2,
            voted_for: Some(MetaNodeId(1)),
            vote_committed: true,
        })
        .unwrap();
    storage.append(&entries(2, 1, 6)).unwrap();
    drop(storage);
    seeds.push(("log-and-hardstate", sim.snapshot().to_bundle()));

    // Every file: entries applied and snapshotted, a tail after the
    // snapshot, the applied, purged and membership frontiers.
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    let mut storage = MetaStorage::open_in(&env, ROOT, cluster).unwrap();
    storage
        .save_hard_state(HardState {
            term: 3,
            voted_for: Some(MetaNodeId(2)),
            vote_committed: false,
        })
        .unwrap();
    storage.append(&entries(3, 1, 8)).unwrap();
    storage.apply_through(8).unwrap();
    storage.write_snapshot().unwrap();
    storage.append(&entries(3, 9, 11)).unwrap();
    storage.apply_through(10).unwrap();
    storage.sync_applied_frontier().unwrap();
    storage.save_purged(3, 2).unwrap();
    storage.save_membership_log_id(3, 1).unwrap();
    drop(storage);
    seeds.push(("all-files", sim.snapshot().to_bundle()));

    // Level 9 (#706): attributed commands (kind 40) in the log and the tail
    // of attributed commands (key category 23, value tag 26) in a snapshot.
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    let mut storage = MetaStorage::open_in(&env, ROOT, cluster).unwrap();
    storage
        .save_hard_state(HardState {
            term: 3,
            voted_for: Some(MetaNodeId(1)),
            vote_committed: true,
        })
        .unwrap();
    let attributed = |index: u64| MetaLogEntry {
        term: 3,
        index,
        payload: MetaLogPayload::Normal(MetadataCommand::Attributed {
            attribution: Attribution {
                actor: if index.is_multiple_of(2) {
                    CommandActor::Named("ops-alice".to_owned())
                } else {
                    CommandActor::Node(Uuid::from_u128(0x10))
                },
                peer: Some("192.0.2.7".parse().unwrap()),
                vouched_by: 1,
                recorded_at_ms: 1_750_000_000_500,
            },
            subject: Box::new(command(u128::from(index))),
        }),
    };
    let mut log = entries(3, 1, 4);
    log.extend((5..=8).map(attributed));
    storage.append(&log).unwrap();
    storage.apply_through(8).unwrap();
    storage.write_snapshot().unwrap();
    storage
        .append(&(9..=11).map(attributed).collect::<Vec<_>>())
        .unwrap();
    storage.apply_through(10).unwrap();
    storage.sync_applied_frontier().unwrap();
    drop(storage);
    seeds.push(("attributed-tail", sim.snapshot().to_bundle()));

    seeds
}

/// Rewrites the committed seeds. Run on purpose after a format change:
/// `cargo test -p vtop-meta --test fuzz_seeds -- --ignored write_fuzz_seeds`.
#[test]
#[ignore = "writes fuzz/corpus/meta_storage_files; run on purpose"]
fn write_fuzz_seeds() {
    let dir = seed_dir();
    std::fs::create_dir_all(&dir).unwrap();
    for (name, bytes) in seeds() {
        std::fs::write(dir.join(format!("{name}.seed")), bytes).unwrap();
    }
}

/// The seeds this build writes recover, and the richest one holds every
/// kind of file the store keeps.
#[test]
fn the_written_seeds_open_and_cover_every_file() {
    let seeds = seeds();
    for (name, bundle) in &seeds {
        opens(name, bundle);
    }
    let all = SimDiskState::from_bundle(&seeds[1].1);
    // The level-9 seed's directory holds a state that carries the tail.
    let attributed = SimStorage::new();
    attributed.restore(&SimDiskState::from_bundle(&seeds[2].1));
    attributed.create_dir_all(Path::new(ROOT));
    let storage =
        MetaStorage::open_in(&attributed.env(SEED), ROOT, Uuid::from_u128(CLUSTER)).unwrap();
    assert!(
        storage.state().audit_tail().count() >= 3,
        "the attributed-tail seed holds no tail"
    );
    let names: Vec<String> = all
        .files
        .keys()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    for wanted in [
        "meta.hardstate",
        "meta.applied",
        "meta.purged",
        "meta.membership_log_id",
    ] {
        assert!(
            names.iter().any(|n| n == wanted),
            "no {wanted} in {names:?}"
        );
    }
    assert!(
        names.iter().any(|n| n.ends_with(".vmlog")),
        "no log chunk in {names:?}"
    );
    assert!(
        names.iter().any(|n| n.ends_with(".vmsnap")),
        "no snapshot in {names:?}"
    );
}

/// The committed seeds recover with this build.
#[test]
fn the_committed_fuzz_seeds_open() {
    let mut count = 0;
    for entry in std::fs::read_dir(seed_dir()).unwrap() {
        let path = entry.unwrap().path();
        // Only the committed seeds: a local fuzzing run leaves the inputs it
        // found here too (git-ignored), and those may rightly not decode.
        if path.extension().and_then(|e| e.to_str()) != Some("seed") {
            continue;
        }
        opens(&path.display().to_string(), &std::fs::read(&path).unwrap());
        count += 1;
    }
    assert!(
        count >= 3,
        "only {count} committed seed(s); run write_fuzz_seeds"
    );
}

fn opens(name: &str, bundle: &[u8]) {
    let sim = SimStorage::new();
    sim.restore(&SimDiskState::from_bundle(bundle));
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    let storage = MetaStorage::open_in(&env, ROOT, Uuid::from_u128(CLUSTER))
        .unwrap_or_else(|error| panic!("{name}: {error}"));
    assert!(storage.last_applied() > 0, "{name}: nothing recovered");
}
