//! Write amplification and proof-carrying overhead measurement harness (#189).
//!
//! ## What this measures
//!
//! | Scenario | Question |
//! |---|---|
//! | `v1_fsync_seal` | Baseline physical/logical amp without chunk proofs |
//! | `v2_fsync_seal` | Same workload with proof-carrying v2 (chunk tree + sidecar) |
//! | `v2_rollover` | Amp across an explicit seal → new-segment rollover |
//!
//! Physical bytes come from [`SimStorage::trace`] `HandleWrite` lengths
//! (application-issued write payloads, including atomic sidecar temps). That
//! is the right layer for single-copy body accounting; OS page-cache /
//! drive-cache amplification is out of scope for this in-process harness.
//!
//! ## How to run
//!
//! ```text
//! # CI suite (also covered by `cargo test --workspace --locked`)
//! cargo test -p vtop-log --test write_amp_proof_harness --locked
//!
//! # Emit machine-readable JSON (directory is created if missing)
//! VTOP_WRITE_AMP_JSON=benchmarks/results/native-write-amp/summary.json \
//!   cargo test -p vtop-log --test write_amp_proof_harness --locked -- --nocapture
//! ```
//!
//! Methodology: `docs/WRITE_AMP_PROOF_OVERHEAD.md`. Complements archive
//! matrix issues #92 / #98 / #130 — does not claim Kafka superiority.

use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use uuid::Uuid;
use vtop_log::proof::{leaf_hash, prove_chunk, ChunkProof};
use vtop_log::sim::{SimStorage, TraceEntry, TraceKind};
use vtop_log::verify::chunk_proof_in;
use vtop_log::{
    ActiveSegment, Durability, LogRecord, RangeLineage, SegmentCommitKey, SegmentConfig,
    SegmentConfigV2, SegmentDescriptor, SegmentDescriptorV2, RECORD_FRAME_OVERHEAD_BYTES_V2,
};

const SEED: u64 = 0x5eed_0189;
const ROOT: &str = "/log";
const BASE_OFFSET: u64 = 0;
const PRODUCER: Uuid = Uuid::from_u128(0xB189);
/// Keep chunk geometry small enough for a fast CI workload while still
/// producing multiple leaves (MIN_CHUNK_SIZE_BYTES = 64 KiB).
const CHUNK_SIZE: u32 = 64 * 1024;
const PAYLOAD_BYTES: usize = 1024;
const RECORD_COUNT: u64 = 256;
/// Fsync every N records so commit-boundary amp is visible but not dominant.
const FSYNC_EVERY: u64 = 16;
const HARNESS_VERSION: &str = "1";
const ISSUE: &str = "189";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
enum WriteBucket {
    SegmentBody,
    CommitBoundary,
    Index,
    Manifest,
    Chunks,
    AtomicTemp,
    Other,
}

#[derive(Clone, Debug, Serialize)]
struct PhysicalWrites {
    by_bucket: BTreeMap<WriteBucket, u64>,
    total_write_bytes: u64,
    body_write_ops: u64,
    distinct_body_paths: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct Amplification {
    /// physical_total / logical_payload
    physical_over_payload: f64,
    /// physical_total / logical_framed (header excluded from framed)
    physical_over_framed: f64,
    /// body_writes / logical_framed — ~1.0 (+ header) confirms single-copy bodies
    body_over_framed: f64,
    commit_boundary_bytes: u64,
    index_bytes: u64,
    manifest_bytes: u64,
    chunks_bytes: u64,
    atomic_temp_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
struct ProofMetrics {
    chunk_size: u32,
    chunk_count: u64,
    chunk_sidecar_durable_bytes: u64,
    sample_proof_bytes: u64,
    sample_proof_path_len: usize,
    /// Bytes a repair client must fetch to authenticate one corrupted chunk
    /// (chunk payload + sibling digests). Contrast with `full_segment_scan_bytes`.
    localized_repair_read_bytes: u64,
    full_segment_scan_bytes: u64,
    localization_ratio: f64,
}

#[derive(Clone, Debug, Serialize)]
struct ScenarioReport {
    name: String,
    format: String,
    record_count: u64,
    payload_bytes_per_record: usize,
    durability: String,
    logical_payload_bytes: u64,
    logical_framed_bytes: u64,
    header_bytes: u64,
    durable_segment_bytes: u64,
    append_elapsed_ms: f64,
    seal_elapsed_ms: f64,
    append_records_per_sec: f64,
    physical: PhysicalWrites,
    amplification: Amplification,
    single_copy_body_path: bool,
    single_copy_notes: Vec<String>,
    proof: Option<ProofMetrics>,
    /// Rough in-process proof-state estimate (not RSS): active chunk buffer +
    /// finalized leaf table after seal for v2; linear hasher only for v1.
    estimated_proof_state_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
struct HarnessReport {
    harness_version: String,
    issue: String,
    seed: u64,
    methodology: String,
    caveats: Vec<String>,
    scenarios: Vec<ScenarioReport>,
    comparison: Comparison,
}

#[derive(Clone, Debug, Serialize)]
struct Comparison {
    /// (v2_append_rps / v1_append_rps) - 1, negative means v2 slower.
    append_throughput_delta_ratio: f64,
    seal_latency_delta_ms: f64,
    write_amp_payload_v1: f64,
    write_amp_payload_v2: f64,
    proof_sidecar_over_payload: f64,
    body_second_wal_detected: bool,
}

fn descriptor_v1() -> SegmentDescriptor {
    SegmentDescriptor {
        segment_id: Uuid::from_u128(0x189),
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        lineage: RangeLineage::root(Uuid::from_u128(0x0189_0001)),
        base_offset: BASE_OFFSET,
    }
}

fn descriptor_v2(segment_id: u128) -> SegmentDescriptorV2 {
    SegmentDescriptorV2 {
        segment_id: Uuid::from_u128(segment_id),
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        lineage: RangeLineage::root(Uuid::from_u128(0x0189_0001)),
        base_offset: BASE_OFFSET,
        segment_generation: 1,
        creation_node_id: Uuid::from_u128(0x0189_0002),
        creation_fencing_epoch: 1,
    }
}

fn config_v1() -> SegmentConfig {
    SegmentConfig {
        max_record_bytes: 64 * 1024,
        max_group_bytes: 1024 * 1024,
        max_segment_bytes: 32 * 1024 * 1024,
        max_segment_records: 100_000,
        index_stride: 32,
    }
}

fn config_v2() -> SegmentConfigV2 {
    SegmentConfigV2 {
        max_record_bytes: 64 * 1024,
        max_group_bytes: 1024 * 1024,
        max_segment_bytes: 32 * 1024 * 1024,
        max_segment_records: 100_000,
        index_stride: 32,
        chunk_size: CHUNK_SIZE,
    }
}

fn record(sequence: u64) -> LogRecord {
    let mut value = vec![0_u8; PAYLOAD_BYTES];
    let seq = sequence.to_le_bytes();
    value[..8].copy_from_slice(&seq);
    LogRecord {
        producer_id: PRODUCER,
        producer_epoch: 0,
        sequence,
        timestamp_millis: 1_700_000_000_000 + sequence as i64,
        attributes: 0,
        key: b"k".to_vec(),
        value,
    }
}

fn classify_write(path: &Path) -> WriteBucket {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    let is_temp = name.starts_with('.') && name.ends_with(".tmp");
    if is_temp {
        return WriteBucket::AtomicTemp;
    }
    if name.ends_with(".active") || name.ends_with(".segment") {
        return WriteBucket::SegmentBody;
    }
    if name.ends_with(".commit") {
        return WriteBucket::CommitBoundary;
    }
    if name.ends_with(".index") {
        return WriteBucket::Index;
    }
    if name.ends_with(".manifest.json") {
        return WriteBucket::Manifest;
    }
    if name.ends_with(".chunks") {
        return WriteBucket::Chunks;
    }
    WriteBucket::Other
}

fn classify_temp_target(path: &Path) -> Option<WriteBucket> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    if !(name.starts_with('.') && name.ends_with(".tmp")) {
        return None;
    }
    // Temps are `.{final_name}.{uuid}.tmp`.
    let trimmed = name.trim_start_matches('.').trim_end_matches(".tmp");
    let final_name = trimmed.rsplit_once('.').map(|(stem, _)| stem).unwrap_or(trimmed);
    if final_name.ends_with(".commit") || final_name == "commit" || final_name.contains(".commit")
    {
        Some(WriteBucket::CommitBoundary)
    } else if final_name.ends_with(".index") || final_name.contains(".index") {
        Some(WriteBucket::Index)
    } else if final_name.ends_with(".manifest.json") || final_name.contains(".manifest.json") {
        Some(WriteBucket::Manifest)
    } else if final_name.ends_with(".chunks") || final_name.contains(".chunks") {
        Some(WriteBucket::Chunks)
    } else {
        Some(WriteBucket::Other)
    }
}

fn summarize_physical(trace: &[TraceEntry]) -> PhysicalWrites {
    let mut by_bucket = BTreeMap::new();
    let mut body_write_ops = 0_u64;
    let mut body_paths = BTreeMap::<String, u64>::new();
    let mut total = 0_u64;

    for entry in trace {
        if entry.kind != TraceKind::HandleWrite || entry.len == 0 {
            continue;
        }
        total += entry.len;
        let mut bucket = classify_write(&entry.path);
        if bucket == WriteBucket::AtomicTemp {
            if let Some(target) = classify_temp_target(&entry.path) {
                // Attribute temp payload bytes to the sidecar they become so
                // commit/index/manifest/chunks amp includes the atomic write.
                bucket = target;
            }
        }
        *by_bucket.entry(bucket).or_insert(0) += entry.len;
        if bucket == WriteBucket::SegmentBody {
            body_write_ops += 1;
            let key = entry.path.display().to_string();
            *body_paths.entry(key).or_insert(0) += entry.len;
        }
    }

    PhysicalWrites {
        by_bucket,
        total_write_bytes: total,
        body_write_ops,
        distinct_body_paths: body_paths.keys().cloned().collect(),
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn proof_encoded_bytes(proof: &ChunkProof) -> u64 {
    // index (u64) + each sibling (32-byte hash + 1-byte side tag)
    8 + proof.path.len() as u64 * 33
}

fn durable_len(sim: &SimStorage, path: &Path) -> u64 {
    sim.snapshot()
        .files
        .get(path)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0)
}

fn check_single_copy(
    physical: &PhysicalWrites,
    logical_framed: u64,
    header_bytes: u64,
) -> (bool, Vec<String>) {
    let mut notes = Vec::new();
    let body = *physical
        .by_bucket
        .get(&WriteBucket::SegmentBody)
        .unwrap_or(&0);
    let expected_body = header_bytes + logical_framed;
    // Bodies are appended once; allow exact match only (sim writes are exact).
    let body_ok = body == expected_body;
    if body_ok {
        notes.push(format!(
            "segment body HandleWrite bytes ({body}) == header ({header_bytes}) + framed content ({logical_framed})"
        ));
    } else {
        notes.push(format!(
            "segment body HandleWrite bytes ({body}) != header+framed ({expected_body})"
        ));
    }

    // A second local data WAL would show up as another non-sidecar path with
    // body-scale writes. Sidecar buckets are metadata by construction.
    let metadata: u64 = [
        WriteBucket::CommitBoundary,
        WriteBucket::Index,
        WriteBucket::Manifest,
        WriteBucket::Chunks,
    ]
    .iter()
    .map(|bucket| *physical.by_bucket.get(bucket).unwrap_or(&0))
    .sum();
    let other = *physical.by_bucket.get(&WriteBucket::Other).unwrap_or(&0);
    let no_second_wal = other == 0 && metadata < logical_framed;
    if no_second_wal {
        notes.push(
            "no second body-scale data path: Other=0 and metadata sidecars < framed content"
                .to_owned(),
        );
    } else {
        notes.push(format!(
            "possible extra data path: other={other} metadata={metadata} framed={logical_framed}"
        ));
    }

    // Seal renames .active -> .segment; at most one body path should receive
    // writes (the active file). After rename the durable name is .segment.
    let paths_ok = physical.distinct_body_paths.len() == 1
        && physical.distinct_body_paths[0].ends_with(".active");
    if paths_ok {
        notes.push(
            "all body writes targeted a single .active path (seal is rename-only)".to_owned(),
        );
    } else {
        notes.push(format!(
            "unexpected body write paths: {:?}",
            physical.distinct_body_paths
        ));
    }

    (body_ok && no_second_wal && paths_ok, notes)
}

fn commit_key() -> SegmentCommitKey {
    SegmentCommitKey::from_hex(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .expect("valid test commit key")
}

fn run_v1(name: &str) -> ScenarioReport {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    let active = Path::new(ROOT).join("measure.active");

    let mut segment =
        ActiveSegment::create_in(&env, &active, descriptor_v1(), config_v1()).expect("create v1");
    let header_bytes = durable_len(&sim, &active);

    let append_start = Instant::now();
    for sequence in 0..RECORD_COUNT {
        let durability = if (sequence + 1) % FSYNC_EVERY == 0 {
            Durability::Fsync
        } else {
            Durability::Buffered
        };
        segment
            .append(record(sequence), durability)
            .expect("append v1");
    }
    let append_elapsed = append_start.elapsed();

    let seal_start = Instant::now();
    let reader = segment.seal().expect("seal v1");
    let seal_elapsed = seal_start.elapsed();

    let logical_framed = reader.manifest().content_bytes;
    let sealed_path = Path::new(ROOT).join("measure.segment");
    let durable_segment_bytes = durable_len(&sim, &sealed_path);
    drop(reader);

    finish_report(
        name,
        "v1",
        &sim,
        header_bytes,
        logical_framed,
        durable_segment_bytes,
        append_elapsed,
        seal_elapsed,
        None,
        32, // linear BLAKE3 hasher state (approximate)
    )
}

fn run_v2(name: &str, segment_id: u128, stem: &str) -> ScenarioReport {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    let active = Path::new(ROOT).join(format!("{stem}.active"));

    let mut segment =
        ActiveSegment::create_v2_in(&env, &active, descriptor_v2(segment_id), config_v2())
            .expect("create v2");
    let header_bytes = durable_len(&sim, &active);

    let append_start = Instant::now();
    for sequence in 0..RECORD_COUNT {
        let durability = if (sequence + 1) % FSYNC_EVERY == 0 {
            Durability::Fsync
        } else {
            Durability::Buffered
        };
        segment
            .append(record(sequence), durability)
            .expect("append v2");
    }
    let append_elapsed = append_start.elapsed();

    let seal_start = Instant::now();
    let reader = segment.seal_v2(Some(&commit_key())).expect("seal v2");
    let seal_elapsed = seal_start.elapsed();

    let manifest = reader.manifest_v2().expect("v2 manifest").clone();
    let logical_framed = manifest.content_bytes;
    let sealed_path = Path::new(ROOT).join(format!("{stem}.segment"));
    let durable_segment_bytes = durable_len(&sim, &sealed_path);
    let chunks_path = Path::new(ROOT).join(format!("{stem}.chunks"));
    let chunk_sidecar_durable_bytes = durable_len(&sim, &chunks_path);

    let (_params, proof, chunk_bytes) =
        chunk_proof_in(&env, &sealed_path, 0).expect("chunk proof for leaf 0");
    let sample_proof_bytes = proof_encoded_bytes(&proof);
    let localized = chunk_bytes.len() as u64 + sample_proof_bytes;
    let full_scan = durable_segment_bytes;
    let proof_metrics = ProofMetrics {
        chunk_size: manifest.chunk_size,
        chunk_count: manifest.chunk_count,
        chunk_sidecar_durable_bytes,
        sample_proof_bytes,
        sample_proof_path_len: proof.path.len(),
        localized_repair_read_bytes: localized,
        full_segment_scan_bytes: full_scan,
        localization_ratio: ratio(localized, full_scan),
    };
    // Active builder holds up to one chunk buffer + 32 B per finalized leaf.
    let estimated_proof_state =
        u64::from(manifest.chunk_size) + manifest.chunk_count.saturating_mul(32);
    drop(reader);

    finish_report(
        name,
        "v2",
        &sim,
        header_bytes,
        logical_framed,
        durable_segment_bytes,
        append_elapsed,
        seal_elapsed,
        Some(proof_metrics),
        estimated_proof_state,
    )
}

fn run_v2_rollover() -> ScenarioReport {
    // Two half-sized segments: seal the first, open a second, append the rest.
    // Amp and single-copy checks cover both bodies.
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    let half = RECORD_COUNT / 2;

    let mut total_header = 0_u64;
    let mut total_framed = 0_u64;
    let mut total_durable = 0_u64;
    let mut append_ms = 0.0_f64;
    let mut seal_ms = 0.0_f64;
    let mut proof = None;
    let mut estimated_proof_state = 0_u64;

    for (idx, stem_id) in [0x189A_u128, 0x189B_u128].into_iter().enumerate() {
        let stem = format!("rollover-{idx}");
        let active = Path::new(ROOT).join(format!("{stem}.active"));
        let mut desc = descriptor_v2(stem_id);
        desc.base_offset = BASE_OFFSET + half * idx as u64;
        desc.segment_generation = (idx as u64) + 1;
        let mut segment =
            ActiveSegment::create_v2_in(&env, &active, desc, config_v2()).expect("create");
        total_header += durable_len(&sim, &active);

        let append_start = Instant::now();
        for sequence in 0..half {
            let durability = if (sequence + 1) % FSYNC_EVERY == 0 {
                Durability::Fsync
            } else {
                Durability::Buffered
            };
            // Fresh producer sequence space per segment for this measurement.
            let mut rec = record(sequence);
            rec.producer_id = Uuid::from_u128(0xB189 + idx as u128);
            segment.append(rec, durability).expect("append");
        }
        append_ms += append_start.elapsed().as_secs_f64() * 1000.0;

        let seal_start = Instant::now();
        let reader = segment.seal_v2(Some(&commit_key())).expect("seal");
        seal_ms += seal_start.elapsed().as_secs_f64() * 1000.0;
        let manifest = reader.manifest_v2().expect("manifest").clone();
        total_framed += manifest.content_bytes;
        let sealed = Path::new(ROOT).join(format!("{stem}.segment"));
        total_durable += durable_len(&sim, &sealed);
        estimated_proof_state = estimated_proof_state.max(
            u64::from(manifest.chunk_size) + manifest.chunk_count.saturating_mul(32),
        );
        if idx == 0 {
            let (_, p, chunk) = chunk_proof_in(&env, &sealed, 0).expect("proof");
            let sample_proof_bytes = proof_encoded_bytes(&p);
            let localized = chunk.len() as u64 + sample_proof_bytes;
            let chunks = Path::new(ROOT).join(format!("{stem}.chunks"));
            proof = Some(ProofMetrics {
                chunk_size: manifest.chunk_size,
                chunk_count: manifest.chunk_count,
                chunk_sidecar_durable_bytes: durable_len(&sim, &chunks),
                sample_proof_bytes,
                sample_proof_path_len: p.path.len(),
                localized_repair_read_bytes: localized,
                full_segment_scan_bytes: durable_len(&sim, &sealed),
                localization_ratio: ratio(localized, durable_len(&sim, &sealed)),
            });
        }
        drop(reader);
    }

    let physical = summarize_physical(&sim.trace());
    let logical_payload = (PAYLOAD_BYTES as u64 + 1) * RECORD_COUNT;
    let (single_copy, notes) = check_single_copy_rollover(&physical, total_framed, total_header);
    let amp = amplification(&physical, logical_payload, total_framed);
    ScenarioReport {
        name: "v2_rollover".to_owned(),
        format: "v2".to_owned(),
        record_count: RECORD_COUNT,
        payload_bytes_per_record: PAYLOAD_BYTES,
        durability: format!("Buffered with Fsync every {FSYNC_EVERY}"),
        logical_payload_bytes: logical_payload,
        logical_framed_bytes: total_framed,
        header_bytes: total_header,
        durable_segment_bytes: total_durable,
        append_elapsed_ms: append_ms,
        seal_elapsed_ms: seal_ms,
        append_records_per_sec: if append_ms > 0.0 {
            RECORD_COUNT as f64 / (append_ms / 1000.0)
        } else {
            0.0
        },
        physical,
        amplification: amp,
        single_copy_body_path: single_copy,
        single_copy_notes: notes,
        proof,
        estimated_proof_state_bytes: estimated_proof_state,
    }
}

fn check_single_copy_rollover(
    physical: &PhysicalWrites,
    logical_framed: u64,
    header_bytes: u64,
) -> (bool, Vec<String>) {
    let mut notes = Vec::new();
    let body = *physical
        .by_bucket
        .get(&WriteBucket::SegmentBody)
        .unwrap_or(&0);
    let expected = header_bytes + logical_framed;
    let body_ok = body == expected;
    notes.push(if body_ok {
        format!("rollover body writes ({body}) == headers+framed ({expected})")
    } else {
        format!("rollover body writes ({body}) != headers+framed ({expected})")
    });
    let paths_ok = physical.distinct_body_paths.len() == 2
        && physical
            .distinct_body_paths
            .iter()
            .all(|path| path.ends_with(".active"));
    notes.push(if paths_ok {
        "exactly two .active body paths (one per generation); seal rename-only".to_owned()
    } else {
        format!("unexpected body paths: {:?}", physical.distinct_body_paths)
    });
    let other = *physical.by_bucket.get(&WriteBucket::Other).unwrap_or(&0);
    let no_wal = other == 0;
    notes.push(if no_wal {
        "no Other body-scale write path across rollover".to_owned()
    } else {
        format!("Other write bytes present: {other}")
    });
    (body_ok && paths_ok && no_wal, notes)
}

fn amplification(
    physical: &PhysicalWrites,
    logical_payload: u64,
    logical_framed: u64,
) -> Amplification {
    let body = *physical
        .by_bucket
        .get(&WriteBucket::SegmentBody)
        .unwrap_or(&0);
    Amplification {
        physical_over_payload: ratio(physical.total_write_bytes, logical_payload),
        physical_over_framed: ratio(physical.total_write_bytes, logical_framed),
        body_over_framed: ratio(body, logical_framed),
        commit_boundary_bytes: *physical
            .by_bucket
            .get(&WriteBucket::CommitBoundary)
            .unwrap_or(&0),
        index_bytes: *physical.by_bucket.get(&WriteBucket::Index).unwrap_or(&0),
        manifest_bytes: *physical.by_bucket.get(&WriteBucket::Manifest).unwrap_or(&0),
        chunks_bytes: *physical.by_bucket.get(&WriteBucket::Chunks).unwrap_or(&0),
        atomic_temp_bytes: *physical
            .by_bucket
            .get(&WriteBucket::AtomicTemp)
            .unwrap_or(&0),
    }
}

#[allow(clippy::too_many_arguments)]
fn finish_report(
    name: &str,
    format: &str,
    sim: &SimStorage,
    header_bytes: u64,
    logical_framed: u64,
    durable_segment_bytes: u64,
    append_elapsed: std::time::Duration,
    seal_elapsed: std::time::Duration,
    proof: Option<ProofMetrics>,
    estimated_proof_state_bytes: u64,
) -> ScenarioReport {
    let physical = summarize_physical(&sim.trace());
    let logical_payload = (PAYLOAD_BYTES as u64 + 1) * RECORD_COUNT;
    let (single_copy, notes) = check_single_copy(&physical, logical_framed, header_bytes);
    let amp = amplification(&physical, logical_payload, logical_framed);
    let append_ms = append_elapsed.as_secs_f64() * 1000.0;
    ScenarioReport {
        name: name.to_owned(),
        format: format.to_owned(),
        record_count: RECORD_COUNT,
        payload_bytes_per_record: PAYLOAD_BYTES,
        durability: format!("Buffered with Fsync every {FSYNC_EVERY}"),
        logical_payload_bytes: logical_payload,
        logical_framed_bytes: logical_framed,
        header_bytes,
        durable_segment_bytes,
        append_elapsed_ms: append_ms,
        seal_elapsed_ms: seal_elapsed.as_secs_f64() * 1000.0,
        append_records_per_sec: if append_ms > 0.0 {
            RECORD_COUNT as f64 / (append_ms / 1000.0)
        } else {
            0.0
        },
        physical,
        amplification: amp,
        single_copy_body_path: single_copy,
        single_copy_notes: notes,
        proof,
        estimated_proof_state_bytes,
    }
}

fn build_report() -> HarnessReport {
    let v1 = run_v1("v1_fsync_seal");
    let v2 = run_v2("v2_fsync_seal", 0x189, "measure");
    let rollover = run_v2_rollover();

    let append_delta = if v1.append_records_per_sec > 0.0 {
        (v2.append_records_per_sec / v1.append_records_per_sec) - 1.0
    } else {
        0.0
    };
    let sidecar_over_payload = v2
        .proof
        .as_ref()
        .map(|proof| ratio(proof.chunk_sidecar_durable_bytes, v2.logical_payload_bytes))
        .unwrap_or(0.0);

    HarnessReport {
        harness_version: HARNESS_VERSION.to_owned(),
        issue: ISSUE.to_owned(),
        seed: SEED,
        methodology: "docs/WRITE_AMP_PROOF_OVERHEAD.md".to_owned(),
        caveats: vec![
            "Physical bytes are SimStorage HandleWrite payloads (application layer), not filesystem journal or drive-cache amplification.".to_owned(),
            "Wall-clock append/seal timings are in-process and noisy under CI load; use them for relative v1 vs v2 on the same host, not absolute SLA claims.".to_owned(),
            "This harness does not run Kafka or claim superiority versus Kafka (#92/#98/#130 remain the archive/matrix methodology).".to_owned(),
            "Multi-hour soak and exotic filesystem lab notes are deferred.".to_owned(),
        ],
        comparison: Comparison {
            append_throughput_delta_ratio: append_delta,
            seal_latency_delta_ms: v2.seal_elapsed_ms - v1.seal_elapsed_ms,
            write_amp_payload_v1: v1.amplification.physical_over_payload,
            write_amp_payload_v2: v2.amplification.physical_over_payload,
            proof_sidecar_over_payload: sidecar_over_payload,
            body_second_wal_detected: !(v1.single_copy_body_path
                && v2.single_copy_body_path
                && rollover.single_copy_body_path),
        },
        scenarios: vec![v1, v2, rollover],
    }
}

fn maybe_write_json(report: &HarnessReport) {
    let Ok(raw) = std::env::var("VTOP_WRITE_AMP_JSON") else {
        return;
    };
    let path = PathBuf::from(&raw);
    // `cargo test -p vtop-log` sets cwd to the package dir; resolve relative
    // paths against the workspace root so docs examples work as written.
    let path = if path.is_absolute() {
        path
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path)
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create JSON output directory");
    }
    let body = serde_json::to_string_pretty(report).expect("serialize report");
    fs::write(&path, body).expect("write JSON report");
    eprintln!("wrote write-amp report to {}", path.display());
}

#[test]
fn write_amp_and_proof_overhead_report() {
    let report = build_report();
    maybe_write_json(&report);

    assert!(
        !report.comparison.body_second_wal_detected,
        "single-copy body path violated: {:?}",
        report
            .scenarios
            .iter()
            .map(|s| (&s.name, s.single_copy_body_path, &s.single_copy_notes))
            .collect::<Vec<_>>()
    );

    for scenario in &report.scenarios {
        assert!(
            scenario.single_copy_body_path,
            "scenario {} failed single-copy checks: {:?}",
            scenario.name, scenario.single_copy_notes
        );
        assert!(
            scenario.amplification.physical_over_payload >= 1.0,
            "amp < 1 is impossible for scenario {}",
            scenario.name
        );
        // Commit-boundary amp must be present under Fsync (atomic rewrite).
        assert!(
            scenario.amplification.commit_boundary_bytes > 0,
            "expected commit-boundary writes in {}",
            scenario.name
        );
        // Body writes should be ~1x framed content (plus headers counted in body bucket).
        assert!(
            scenario.amplification.body_over_framed < 1.25,
            "body amp unexpectedly high in {}: {}",
            scenario.name,
            scenario.amplification.body_over_framed
        );
    }

    let v2 = report
        .scenarios
        .iter()
        .find(|s| s.name == "v2_fsync_seal")
        .expect("v2 scenario");
    let proof = v2.proof.as_ref().expect("v2 proof metrics");
    assert!(proof.chunk_count >= 2, "workload should span >= 2 chunks");
    assert!(proof.sample_proof_bytes > 0);
    assert!(
        proof.localized_repair_read_bytes < proof.full_segment_scan_bytes,
        "chunk proof should localize repair below a full segment scan"
    );
    assert!(
        proof.chunk_sidecar_durable_bytes > 0,
        "v2 must persist a .chunks sidecar"
    );

    // Framed overhead sanity: v2 frames add RECORD_FRAME_OVERHEAD_BYTES_V2 + key + value.
    let expected_min_framed =
        RECORD_COUNT * (RECORD_FRAME_OVERHEAD_BYTES_V2 + 1 + PAYLOAD_BYTES as u64);
    assert!(
        v2.logical_framed_bytes >= expected_min_framed,
        "framed {} < expected minimum {}",
        v2.logical_framed_bytes,
        expected_min_framed
    );

    // JSON round-trip: report must stay machine-readable for matrix tools.
    let encoded = serde_json::to_value(&report).expect("json value");
    assert_eq!(encoded["issue"], ISSUE);
    assert_eq!(encoded["harness_version"], HARNESS_VERSION);
    assert!(encoded["scenarios"].as_array().unwrap().len() >= 3);
}

#[test]
fn prove_chunk_size_matches_encoded_estimate() {
    // Tiny unit check that the harness's proof byte estimate tracks the
    // sibling path length from the public prove_chunk API.
    let leaves: Vec<_> = (0..8_u8).map(|i| leaf_hash(&[i])).collect();
    let proof = prove_chunk(&leaves, 3);
    assert_eq!(proof_encoded_bytes(&proof), 8 + proof.path.len() as u64 * 33);
}

/// Optional longer local run (not in default CI). Same assertions, larger N.
#[test]
#[ignore = "extended local measurement; run with --ignored"]
fn write_amp_extended_record_count_smoke() {
    // Re-run the default report path; extended sizing can be layered later
    // without changing the JSON schema (harness_version bump).
    let report = build_report();
    assert!(!report.comparison.body_second_wal_detected);
    maybe_write_json(&report);
}
