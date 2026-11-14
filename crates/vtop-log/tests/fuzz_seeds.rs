//! The `segment_codec` fuzz target's seeds (#729): bundles of real segment
//! directories (`SimDiskState::to_bundle`), written through the crate's own
//! API on the simulated disk, so the fuzzer starts from every file a segment
//! has (header, frames, index, manifest, chunk sidecar, commit marker,
//! producer sidecar) in both formats, sealed and active.
//!
//! `the_committed_fuzz_seeds_open` keeps the committed seeds honest: each
//! must restore to a directory this build reads, so a format change that
//! strands them fails here rather than silently starting the nightly from
//! inputs rejected at the first byte. `write_fuzz_seeds` rewrites them.

use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_log::env::Env;
use vtop_log::sim::{SimDiskState, SimStorage};
use vtop_log::{
    segment_stem, ActiveSegment, Durability, LogRecord, RangeLineage, SegmentCommitKey,
    SegmentConfig, SegmentConfigV2, SegmentDescriptor, SegmentDescriptorV2, SegmentReader,
};

const SEED: u64 = 0x5eed_0729;
const ROOT: &str = "/log";
const BASE: u64 = 40;

fn seed_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/segment_codec")
}

fn active_path() -> PathBuf {
    Path::new(ROOT).join(format!("{}.active", segment_stem(BASE)))
}

/// A record; v1 frames carry no producer epoch, so only v2 seeds set one.
fn record(sequence: u64, producer_epoch: u64) -> LogRecord {
    LogRecord {
        producer_id: Uuid::from_u128(200),
        producer_epoch,
        sequence,
        timestamp_millis: 1_700_000_000_000 + sequence as i64,
        attributes: 0,
        key: b"key".to_vec(),
        value: format!("value-{sequence}").into_bytes(),
    }
}

fn descriptor_v1() -> SegmentDescriptor {
    SegmentDescriptor {
        segment_id: Uuid::from_u128(1),
        topic: "events.v1".to_owned(),
        topic_epoch: 7,
        lineage: RangeLineage::root(Uuid::from_u128(100)),
        base_offset: BASE,
    }
}

fn config_v1() -> SegmentConfig {
    SegmentConfig {
        max_record_bytes: 1024,
        max_group_bytes: 4096,
        max_segment_bytes: 16 * 1024,
        max_segment_records: 100,
        index_stride: 2,
    }
}

fn descriptor_v2() -> SegmentDescriptorV2 {
    SegmentDescriptorV2 {
        segment_id: Uuid::from_u128(2),
        topic: "events.v1".to_owned(),
        topic_epoch: 7,
        lineage: RangeLineage::root(Uuid::from_u128(100)),
        base_offset: BASE,
        segment_generation: 3,
        creation_node_id: Uuid::from_u128(300),
        creation_fencing_epoch: 5,
    }
}

fn config_v2() -> SegmentConfigV2 {
    SegmentConfigV2 {
        max_record_bytes: 1024,
        max_group_bytes: 4096,
        max_segment_bytes: 16 * 1024,
        max_segment_records: 100,
        index_stride: 2,
        // The smallest chunk the format allows; a seed stays one chunk,
        // and small inputs fuzz faster.
        chunk_size: 64 * 1024,
    }
}

fn disk() -> (SimStorage, Env) {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    (sim, env)
}

fn append_all(segment: &mut ActiveSegment, count: u64, producer_epoch: u64) {
    for sequence in 0..count {
        segment
            .append(record(sequence, producer_epoch), Durability::Fsync)
            .unwrap();
    }
}

fn seeds() -> Vec<(&'static str, Vec<u8>)> {
    let mut seeds = Vec::new();

    let (sim, env) = disk();
    let mut segment =
        ActiveSegment::create_in(&env, active_path(), descriptor_v1(), config_v1()).unwrap();
    append_all(&mut segment, 5, 0);
    segment.seal().unwrap();
    seeds.push(("v1-sealed", sim.snapshot().to_bundle()));

    let (sim, env) = disk();
    let mut segment =
        ActiveSegment::create_in(&env, active_path(), descriptor_v1(), config_v1()).unwrap();
    append_all(&mut segment, 3, 0);
    drop(segment);
    seeds.push(("v1-active", sim.snapshot().to_bundle()));

    let (sim, env) = disk();
    let mut segment =
        ActiveSegment::create_v2_in(&env, active_path(), descriptor_v2(), config_v2()).unwrap();
    append_all(&mut segment, 6, 1);
    segment.seal_v2(None).unwrap();
    seeds.push(("v2-sealed-unkeyed", sim.snapshot().to_bundle()));

    let (sim, env) = disk();
    let mut segment =
        ActiveSegment::create_v2_in(&env, active_path(), descriptor_v2(), config_v2()).unwrap();
    append_all(&mut segment, 6, 1);
    let key = SegmentCommitKey::from_hex(&"29".repeat(32)).unwrap();
    segment.seal_v2(Some(&key)).unwrap();
    seeds.push(("v2-sealed-keyed", sim.snapshot().to_bundle()));

    let (sim, env) = disk();
    let mut segment =
        ActiveSegment::create_v2_in(&env, active_path(), descriptor_v2(), config_v2()).unwrap();
    append_all(&mut segment, 4, 1);
    drop(segment);
    seeds.push(("v2-active", sim.snapshot().to_bundle()));

    seeds
}

/// Rewrites the committed seeds. Run on purpose after a format change:
/// `cargo test -p vtop-log --test fuzz_seeds -- --ignored write_fuzz_seeds`.
#[test]
#[ignore = "writes fuzz/corpus/segment_codec; run on purpose"]
fn write_fuzz_seeds() {
    let dir = seed_dir();
    std::fs::create_dir_all(&dir).unwrap();
    for (name, bytes) in seeds() {
        std::fs::write(dir.join(format!("{name}.seed")), bytes).unwrap();
    }
}

/// The seeds this build writes open, so they are worth committing.
#[test]
fn the_written_seeds_open() {
    for (name, bundle) in seeds() {
        opens(name, &bundle);
    }
}

/// The committed seeds open with this build.
#[test]
fn the_committed_fuzz_seeds_open() {
    let mut count = 0;
    for entry in std::fs::read_dir(seed_dir()).unwrap() {
        let path = entry.unwrap().path();
        opens(&path.display().to_string(), &std::fs::read(&path).unwrap());
        count += 1;
    }
    assert!(
        count >= 5,
        "only {count} committed seed(s); run write_fuzz_seeds"
    );
}

/// Every sealed segment in the bundle opens and every active one recovers.
fn opens(name: &str, bundle: &[u8]) {
    let disk = SimDiskState::from_bundle(bundle);
    let sealed: Vec<&PathBuf> = disk
        .files
        .keys()
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("segment"))
        .collect();
    let active: Vec<&PathBuf> = disk
        .files
        .keys()
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some("active"))
        .collect();
    assert!(
        sealed.len() + active.len() == 1,
        "{name}: expected one segment, found {sealed:?} {active:?}"
    );
    let sim = SimStorage::new();
    sim.restore(&disk);
    let env = sim.env(SEED);
    for path in sealed {
        SegmentReader::open_in(&env, path).unwrap_or_else(|error| panic!("{name}: {error}"));
    }
    for path in active {
        ActiveSegment::recover_in(&env, path).unwrap_or_else(|error| panic!("{name}: {error}"));
    }
}
