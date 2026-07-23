//! End-to-end test of the config-free `vtopctl segment` tools.
//!
//! The test drives the library handler functions directly (no subprocess):
//! it seals a real v2 bundle into a tempdir, runs the verify handler, and
//! asserts both the exit-code contract and the machine-readable JSON shape.

use std::fs;
use std::path::{Path, PathBuf};
use tempfile::tempdir;
use uuid::Uuid;
use vtop_cli::segment_tools::{
    build_expectations, build_proof_file, exit_code_for, report_json, run, ProofFile,
    ProveChunkArgs, RequireLevel, SegmentCommand, VerifyArgs, VerifyChunkArgs, EXIT_AUTH_FAILURE,
    EXIT_BELOW_REQUIRED_LEVEL, EXIT_CORRUPT, EXIT_OK, EXIT_USAGE,
};
use vtop_log::verify::{chunk_proof, verify_sealed_segment};
use vtop_log::{
    ActiveSegment, Durability, LogRecord, RangeLineage, SegmentCommitKey, SegmentConfigV2,
    SegmentDescriptorV2,
};

fn key_hex() -> String {
    (0..32).map(|byte| format!("{byte:02x}")).collect()
}

fn commit_key() -> SegmentCommitKey {
    SegmentCommitKey::from_hex(&key_hex()).unwrap()
}

fn descriptor() -> SegmentDescriptorV2 {
    SegmentDescriptorV2 {
        segment_id: Uuid::from_u128(0x71),
        topic: "audit.v1".to_owned(),
        topic_epoch: 3,
        lineage: RangeLineage::root(Uuid::from_u128(0x72)),
        base_offset: 0,
        segment_generation: 1,
        creation_node_id: Uuid::from_u128(0x73),
        creation_fencing_epoch: 2,
    }
}

fn record(sequence: u64, value: &[u8]) -> LogRecord {
    LogRecord {
        producer_id: Uuid::from_u128(0x74),
        producer_epoch: 1,
        sequence,
        timestamp_millis: 1_700_000_000_000 + sequence as i64,
        attributes: 0,
        key: b"key".to_vec(),
        value: value.to_vec(),
    }
}

/// Seal a keyed v2 bundle whose content spans several 64 KiB chunks.
fn seal_bundle(directory: &Path) -> PathBuf {
    let config = SegmentConfigV2 {
        max_record_bytes: 64 * 1024,
        max_group_bytes: 128 * 1024,
        max_segment_bytes: 1024 * 1024,
        max_segment_records: 100,
        index_stride: 2,
        chunk_size: 64 * 1024,
    };
    let mut segment =
        ActiveSegment::create_v2(directory.join("audit.active"), descriptor(), config).unwrap();
    for sequence in 0..5_u64 {
        segment
            .append(
                record(sequence, &vec![sequence as u8; 40 * 1024]),
                Durability::Fsync,
            )
            .unwrap();
    }
    drop(segment.seal_v2(Some(&commit_key())).unwrap());
    directory.join("audit.segment")
}

fn verify_args(
    paths: Vec<PathBuf>,
    key_file: Option<PathBuf>,
    require: RequireLevel,
) -> VerifyArgs {
    // The seal path leaves the statement's key_id empty, so the keyring must
    // register the key under the empty id.
    let (key_ids, key_files) = match key_file {
        Some(path) => (vec![String::new()], vec![path]),
        None => (Vec::new(), Vec::new()),
    };
    VerifyArgs {
        paths,
        expect_root: None,
        expect_manifest_digest: None,
        key_ids,
        key_envs: Vec::new(),
        key_files,
        require,
    }
}

#[test]
fn verify_handler_reaches_every_exit_code_and_reports_machine_readable_json() {
    let directory = tempdir().unwrap();
    let sealed = seal_bundle(directory.path());
    let key_file = directory.path().join("commit.key");
    fs::write(&key_file, format!("{}\n", key_hex())).unwrap();

    // Authenticated verification through the real handler exits 0.
    let args = verify_args(
        vec![sealed.clone()],
        Some(key_file.clone()),
        RequireLevel::Auth,
    );
    assert_eq!(run(&SegmentCommand::Verify(args), true), EXIT_OK);

    // The JSON shape carries the identity, the counters, and every check.
    let args = verify_args(
        vec![sealed.clone()],
        Some(key_file.clone()),
        RequireLevel::Auth,
    );
    let expectations = build_expectations(&args).unwrap();
    let report = verify_sealed_segment(&sealed, &expectations).unwrap();
    let code = exit_code_for(&report);
    assert_eq!(code, EXIT_OK);
    let json = report_json(&sealed, &report, code);
    assert_eq!(json["path"], sealed.display().to_string());
    assert_eq!(json["format_version"], 2);
    assert_eq!(json["segment_id"], Uuid::from_u128(0x71).to_string());
    assert_eq!(json["record_count"], 5);
    assert_eq!(json["achieved"], "authenticated");
    assert_eq!(json["passed"], true);
    assert_eq!(json["exit_code"], 0);
    let checks = json["checks"].as_array().unwrap();
    assert!(!checks.is_empty());
    for check in checks {
        assert!(check["name"].is_string());
        assert_eq!(check["passed"], true);
        assert!(check["detail"].is_string());
    }

    // Requiring authentication without any key exits 6.
    let args = verify_args(vec![sealed.clone()], None, RequireLevel::Auth);
    assert_eq!(
        run(&SegmentCommand::Verify(args), true),
        EXIT_BELOW_REQUIRED_LEVEL
    );

    // The wrong key exits 5.
    let wrong_key_file = directory.path().join("wrong.key");
    fs::write(&wrong_key_file, "11".repeat(32)).unwrap();
    let args = verify_args(
        vec![sealed.clone()],
        Some(wrong_key_file),
        RequireLevel::SelfConsistent,
    );
    assert_eq!(run(&SegmentCommand::Verify(args), true), EXIT_AUTH_FAILURE);

    // A wrong root pin exits 4.
    let mut args = verify_args(vec![sealed.clone()], None, RequireLevel::SelfConsistent);
    args.expect_root = Some("ab".repeat(32));
    assert_eq!(run(&SegmentCommand::Verify(args), true), 4);

    // A flipped content byte exits 3, and with several paths the first
    // nonzero code in path order wins while every path is still reported.
    let tampered_directory = tempdir().unwrap();
    let tampered = seal_bundle(tampered_directory.path());
    let mut bytes = fs::read(&tampered).unwrap();
    let position = bytes.len() - 10;
    bytes[position] ^= 0xff;
    fs::write(&tampered, bytes).unwrap();
    let args = verify_args(
        vec![tampered, sealed.clone()],
        Some(key_file),
        RequireLevel::SelfConsistent,
    );
    assert_eq!(run(&SegmentCommand::Verify(args), true), EXIT_CORRUPT);

    // A missing key file is a usage error.
    let args = verify_args(
        vec![sealed],
        Some(directory.path().join("missing.key")),
        RequireLevel::SelfConsistent,
    );
    assert_eq!(run(&SegmentCommand::Verify(args), true), EXIT_USAGE);
}

#[test]
fn prove_chunk_and_verify_chunk_round_trip_and_reject_a_tampered_chunk() {
    let directory = tempdir().unwrap();
    let sealed = seal_bundle(directory.path());
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(directory.path().join("audit.manifest.json")).unwrap())
            .unwrap();
    let root = manifest["chunk_tree_root"].as_str().unwrap().to_owned();
    let chunk_count = manifest["chunk_count"].as_u64().unwrap();
    assert!(chunk_count >= 3, "{chunk_count}");

    // The prove-chunk handler itself succeeds on every chunk.
    for index in 0..chunk_count {
        let args = ProveChunkArgs {
            segment: sealed.clone(),
            index,
        };
        assert_eq!(run(&SegmentCommand::ProveChunk(args), true), EXIT_OK);
    }

    // Round trip the documented JSON shape through the verify-chunk handler.
    let (params, proof, chunk) = chunk_proof(&sealed, 1).unwrap();
    let proof_file_value = build_proof_file(params, &proof, &chunk);
    let encoded = serde_json::to_vec_pretty(&proof_file_value).unwrap();
    let decoded: ProofFile = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded.scheme, "vtop-b3tree-v1");
    assert_eq!(decoded.index, 1);
    assert_eq!(decoded.chunk_count, params.chunk_count);

    let chunk_path = directory.path().join("chunk-1.bin");
    let proof_path = directory.path().join("chunk-1.proof.json");
    fs::write(&chunk_path, &chunk).unwrap();
    fs::write(&proof_path, &encoded).unwrap();
    let args = VerifyChunkArgs {
        root: root.clone(),
        chunk_size: params.chunk_size,
        chunk_count: params.chunk_count,
        index: 1,
        chunk_file: chunk_path.clone(),
        proof_file: proof_path.clone(),
    };
    assert_eq!(run(&SegmentCommand::VerifyChunk(args), false), EXIT_OK);

    // A tampered chunk fails cleanly with the corruption exit code.
    let mut tampered_chunk = chunk.clone();
    tampered_chunk[0] ^= 0xff;
    let tampered_path = directory.path().join("chunk-1-tampered.bin");
    fs::write(&tampered_path, &tampered_chunk).unwrap();
    let args = VerifyChunkArgs {
        root: root.clone(),
        chunk_size: params.chunk_size,
        chunk_count: params.chunk_count,
        index: 1,
        chunk_file: tampered_path,
        proof_file: proof_path.clone(),
    };
    assert_eq!(run(&SegmentCommand::VerifyChunk(args), false), EXIT_CORRUPT);

    // Asking verify-chunk about a different index than the proof was built
    // for is a usage error, not a verification verdict.
    let args = VerifyChunkArgs {
        root,
        chunk_size: params.chunk_size,
        chunk_count: params.chunk_count,
        index: 2,
        chunk_file: chunk_path,
        proof_file: proof_path,
    };
    assert_eq!(run(&SegmentCommand::VerifyChunk(args), false), EXIT_USAGE);

    // An out-of-range prove-chunk index is a usage error as well.
    let args = ProveChunkArgs {
        segment: sealed,
        index: chunk_count,
    };
    assert_eq!(run(&SegmentCommand::ProveChunk(args), true), EXIT_USAGE);
}
