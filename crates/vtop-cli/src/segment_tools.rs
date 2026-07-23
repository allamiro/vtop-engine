//! Config-free `vtopctl segment` tools for sealed native segments.
//!
//! Every other `vtopctl` subcommand loads a `--config`; these deliberately do
//! not. A verifier that needs the operator's configuration cannot be handed
//! to an auditor, so the only inputs here are the sealed artifacts on disk
//! plus explicitly pinned expectations (roots, digests, keys). Keys are read
//! from environment variables or files and are never echoed or logged.

use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use vtop_log::proof::{ChunkParams, ChunkProof, Side};
use vtop_log::verify::{
    chunk_proof, level_name, verify_sealed_segment, VerifyExpectations, VerifyLevel, VerifyReport,
    CHECK_CHUNK_SIDECAR, CHECK_CONTENT_ROOT, CHECK_FRAME_SCAN, CHECK_MANIFEST_CANONICAL,
    CHECK_MANIFEST_CONSISTENCY, CHECK_MANIFEST_DIGEST_PIN, CHECK_REQUIRED_LEVEL, CHECK_ROOT_PIN,
    CHECK_STATEMENT_DIGEST, CHECK_STATEMENT_ECHO, CHECK_STATEMENT_MAC,
};
use vtop_log::{LogError, SegmentCommitKey, CHUNK_TREE_SCHEME_V1};

/// Verified at the required level.
pub const EXIT_OK: i32 = 0;
/// Invalid arguments, unloadable keys, or a malformed proof file.
pub const EXIT_USAGE: i32 = 2;
/// The artifacts are corrupt or disagree with each other.
pub const EXIT_CORRUPT: i32 = 3;
/// A caller-supplied pin did not match.
pub const EXIT_PIN_MISMATCH: i32 = 4;
/// A commit-statement MAC failed to verify.
pub const EXIT_AUTH_FAILURE: i32 = 5;
/// Everything checked out but the required level was not reached.
pub const EXIT_BELOW_REQUIRED_LEVEL: i32 = 6;

#[derive(Subcommand, Debug)]
pub enum SegmentCommand {
    /// Verify sealed segments offline against optional pins and commit keys.
    Verify(VerifyArgs),
    /// Emit the inclusion proof for one content chunk of a sealed v2 segment.
    ProveChunk(ProveChunkArgs),
    /// Check a chunk file and proof file against a pinned chunk-tree root.
    VerifyChunk(VerifyChunkArgs),
}

#[derive(Args, Debug)]
pub struct VerifyArgs {
    /// Sealed segment paths (each must end in `.segment`).
    #[arg(required = true, value_name = "PATH")]
    pub paths: Vec<PathBuf>,

    /// Expected content root as 64 hex characters: the chunk-tree root of a
    /// v2 segment, or the linear BLAKE3 root of a v1 segment.
    #[arg(long, value_name = "HEX")]
    pub expect_root: Option<String>,

    /// Expected BLAKE3 digest (64 hex characters) of the canonical v2
    /// manifest bytes with the commit statement stripped.
    #[arg(long, value_name = "HEX")]
    pub expect_manifest_digest: Option<String>,

    /// Key identifier for a commit key; repeatable. Each occurrence pairs
    /// positionally with a `--key-env` or `--key-file` occurrence, with all
    /// `--key-env` pairs taken before all `--key-file` pairs.
    #[arg(long = "key-id", value_name = "ID")]
    pub key_ids: Vec<String>,

    /// Environment variable holding a 64-hex-character commit key.
    #[arg(long = "key-env", value_name = "VAR")]
    pub key_envs: Vec<String>,

    /// File holding a 64-hex-character commit key.
    #[arg(long = "key-file", value_name = "PATH")]
    pub key_files: Vec<PathBuf>,

    /// Verification level that must be reached for exit code 0.
    #[arg(long, value_enum, default_value = "self")]
    pub require: RequireLevel,
}

#[derive(Args, Debug)]
pub struct ProveChunkArgs {
    /// Sealed v2 segment path.
    #[arg(long, value_name = "PATH")]
    pub segment: PathBuf,

    /// Zero-based content chunk index.
    #[arg(long, value_name = "N")]
    pub index: u64,
}

#[derive(Args, Debug)]
pub struct VerifyChunkArgs {
    /// Pinned chunk-tree root as 64 hex characters.
    #[arg(long, value_name = "HEX")]
    pub root: String,

    /// Chunk size in bytes the segment was sealed with.
    #[arg(long, value_name = "N")]
    pub chunk_size: u32,

    /// Total chunk count of the sealed segment.
    #[arg(long, value_name = "N")]
    pub chunk_count: u64,

    /// Zero-based index of the chunk being checked.
    #[arg(long, value_name = "N")]
    pub index: u64,

    /// File holding the raw chunk bytes.
    #[arg(long, value_name = "PATH")]
    pub chunk_file: PathBuf,

    /// Proof file: the `--json` output of `segment prove-chunk`.
    #[arg(long, value_name = "PATH")]
    pub proof_file: PathBuf,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum RequireLevel {
    /// The artifacts must agree with themselves.
    #[value(name = "self")]
    SelfConsistent,
    /// A supplied root or manifest-digest pin must match.
    #[value(name = "root")]
    Root,
    /// A keyed commit statement must verify against a supplied key.
    #[value(name = "auth")]
    Auth,
}

impl From<RequireLevel> for VerifyLevel {
    fn from(level: RequireLevel) -> Self {
        match level {
            RequireLevel::SelfConsistent => VerifyLevel::SelfConsistent,
            RequireLevel::Root => VerifyLevel::RootPinned,
            RequireLevel::Auth => VerifyLevel::Authenticated,
        }
    }
}

/// Machine shape of a chunk proof, written by `prove-chunk --json` and read
/// back by `verify-chunk --proof-file`.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProofFile {
    /// Always [`CHUNK_TREE_SCHEME_V1`].
    pub scheme: String,
    pub chunk_size: u32,
    pub chunk_count: u64,
    pub index: u64,
    /// Domain-separated BLAKE3 leaf hash of the chunk bytes.
    pub chunk_blake3: String,
    /// Sibling digests ordered leaf-to-root.
    pub proof_path: Vec<ProofStep>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProofStep {
    pub hash: String,
    /// `"left"` or `"right"`: which side of the join the sibling sits on.
    pub side: String,
}

/// Dispatch a `vtopctl segment` invocation and return the process exit code.
pub fn run(command: &SegmentCommand, json: bool) -> i32 {
    match command {
        SegmentCommand::Verify(args) => run_verify(args, json),
        SegmentCommand::ProveChunk(args) => run_prove_chunk(args, json),
        SegmentCommand::VerifyChunk(args) => run_verify_chunk(args),
    }
}

/// Map the CLI arguments to verifier expectations. Key material flows from
/// the environment or files straight into opaque [`SegmentCommitKey`]s and is
/// never printed.
pub fn build_expectations(args: &VerifyArgs) -> Result<VerifyExpectations, String> {
    let chunk_tree_root = args
        .expect_root
        .as_deref()
        .map(parse_hash_hex)
        .transpose()
        .map_err(|error| format!("--expect-root: {error}"))?;
    let manifest_core_digest = args
        .expect_manifest_digest
        .as_deref()
        .map(parse_hash_hex)
        .transpose()
        .map_err(|error| format!("--expect-manifest-digest: {error}"))?;
    let sources = pair_key_sources(&args.key_ids, &args.key_envs, &args.key_files)?;
    let mut keyring = BTreeMap::new();
    for (key_id, source) in sources {
        let hex = match &source {
            KeySource::Env(variable) => std::env::var(variable)
                .map_err(|_| format!("--key-env {variable}: environment variable is not set"))?,
            KeySource::File(path) => std::fs::read_to_string(path)
                .map_err(|error| format!("--key-file {}: {error}", path.display()))?,
        };
        let key = SegmentCommitKey::from_hex(hex.trim())
            .map_err(|_| format!("key {key_id:?} is not 64 hex characters"))?;
        if keyring.insert(key_id.clone(), key).is_some() {
            return Err(format!("duplicate --key-id {key_id:?}"));
        }
    }
    Ok(VerifyExpectations {
        chunk_tree_root,
        manifest_core_digest,
        keyring,
        require: VerifyLevel::from(args.require),
    })
}

/// Where one commit key comes from; the value itself is loaded later.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeySource {
    Env(String),
    File(PathBuf),
}

/// Pair `--key-id` occurrences with their key sources: all `--key-env`
/// occurrences first, then all `--key-file` occurrences, each in CLI order.
pub fn pair_key_sources(
    key_ids: &[String],
    key_envs: &[String],
    key_files: &[PathBuf],
) -> Result<Vec<(String, KeySource)>, String> {
    if key_ids.len() != key_envs.len() + key_files.len() {
        return Err(format!(
            "{} --key-id occurrences but {} key sources; every --key-id needs exactly one --key-env or --key-file",
            key_ids.len(),
            key_envs.len() + key_files.len()
        ));
    }
    let sources = key_envs
        .iter()
        .cloned()
        .map(KeySource::Env)
        .chain(key_files.iter().cloned().map(KeySource::File));
    Ok(key_ids.iter().cloned().zip(sources).collect())
}

/// Map one verification report to its exit code. Corruption outranks a pin
/// mismatch, which outranks an authentication failure, which outranks merely
/// falling short of the required level.
pub fn exit_code_for(report: &VerifyReport) -> i32 {
    const CORRUPTION_CHECKS: [&str; 7] = [
        CHECK_FRAME_SCAN,
        CHECK_CONTENT_ROOT,
        CHECK_CHUNK_SIDECAR,
        CHECK_MANIFEST_CANONICAL,
        CHECK_MANIFEST_CONSISTENCY,
        CHECK_STATEMENT_ECHO,
        CHECK_STATEMENT_DIGEST,
    ];
    let failed = |name: &str| {
        report
            .checks
            .iter()
            .any(|check| check.name == name && !check.passed)
    };
    if CORRUPTION_CHECKS.iter().any(|name| failed(name)) {
        EXIT_CORRUPT
    } else if failed(CHECK_ROOT_PIN) || failed(CHECK_MANIFEST_DIGEST_PIN) {
        EXIT_PIN_MISMATCH
    } else if failed(CHECK_STATEMENT_MAC) {
        EXIT_AUTH_FAILURE
    } else if failed(CHECK_REQUIRED_LEVEL) {
        EXIT_BELOW_REQUIRED_LEVEL
    } else {
        EXIT_OK
    }
}

/// JSON shape of one verification report, as printed by `--json`.
pub fn report_json(path: &Path, report: &VerifyReport, exit_code: i32) -> serde_json::Value {
    serde_json::json!({
        "path": path.display().to_string(),
        "format_version": report.format_version,
        "segment_id": report.segment_id.to_string(),
        "record_count": report.record_count,
        "content_bytes": report.content_bytes,
        "chunk_count": report.chunk_count,
        "achieved": level_name(report.achieved),
        "checks": report.checks.iter().map(|check| serde_json::json!({
            "name": check.name,
            "passed": check.passed,
            "detail": check.detail,
        })).collect::<Vec<_>>(),
        "passed": exit_code == EXIT_OK,
        "exit_code": exit_code,
    })
}

fn run_verify(args: &VerifyArgs, json: bool) -> i32 {
    let expectations = match build_expectations(args) {
        Ok(expectations) => expectations,
        Err(message) => {
            eprintln!("error: {message}");
            return EXIT_USAGE;
        }
    };
    // With multiple paths every segment is still verified and reported; the
    // exit code is the first nonzero one in path order.
    let mut exit_code = EXIT_OK;
    let mut json_reports = Vec::new();
    for path in &args.paths {
        // Path-shape mistakes are usage; anything the verifier surfaces after
        // that — including InvalidDescriptor/InvalidConfig from a decoded
        // but invalid header — is an artifact we could not trust.
        if path.extension().and_then(|value| value.to_str()) != Some("segment") {
            let message = format!(
                "{}: sealed segment path must end in .segment",
                path.display()
            );
            let code = EXIT_USAGE;
            if json {
                json_reports.push(serde_json::json!({
                    "path": path.display().to_string(),
                    "error": message,
                    "passed": false,
                    "exit_code": code,
                }));
            } else {
                eprintln!("error: {message}");
            }
            if exit_code == EXIT_OK {
                exit_code = code;
            }
            continue;
        }
        let code = match verify_sealed_segment(path, &expectations) {
            Ok(report) => {
                let code = exit_code_for(&report);
                if json {
                    json_reports.push(report_json(path, &report, code));
                } else {
                    print_human_report(path, &report, &expectations, code);
                }
                code
            }
            Err(error) => {
                let code = EXIT_CORRUPT;
                if json {
                    json_reports.push(serde_json::json!({
                        "path": path.display().to_string(),
                        "error": error.to_string(),
                        "passed": false,
                        "exit_code": code,
                    }));
                } else {
                    println!("{}: unreadable: {error}", path.display());
                }
                code
            }
        };
        if exit_code == EXIT_OK {
            exit_code = code;
        }
    }
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::Value::Array(json_reports))
                .expect("report values are valid JSON")
        );
    }
    exit_code
}

fn print_human_report(
    path: &Path,
    report: &VerifyReport,
    expectations: &VerifyExpectations,
    exit_code: i32,
) {
    println!(
        "{}: format v{}, segment {}, {} records, {} content bytes, {} chunks",
        path.display(),
        report.format_version,
        report.segment_id,
        report.record_count,
        report.content_bytes,
        report.chunk_count
    );
    for check in &report.checks {
        println!(
            "  {}  {:<22} {}",
            if check.passed { "PASS" } else { "FAIL" },
            check.name,
            check.detail
        );
    }
    println!(
        "  level: {} (required: {}) -> exit {exit_code}",
        level_name(report.achieved),
        level_name(expectations.require)
    );
}

/// Exit codes for prove-chunk / verify-chunk argument and open failures.
/// Unlike [`run_verify`], these subcommands treat a malformed path or an
/// out-of-range chunk index as a caller mistake (usage), not corruption.
fn error_exit_code(error: &LogError) -> i32 {
    match error {
        LogError::InvalidDescriptor(_) | LogError::InvalidConfig(_) => EXIT_USAGE,
        _ => EXIT_CORRUPT,
    }
}

/// Build the proof-file shape from a proof produced by
/// [`vtop_log::verify::chunk_proof`].
pub fn build_proof_file(params: ChunkParams, proof: &ChunkProof, chunk: &[u8]) -> ProofFile {
    ProofFile {
        scheme: CHUNK_TREE_SCHEME_V1.to_owned(),
        chunk_size: params.chunk_size,
        chunk_count: params.chunk_count,
        index: proof.index,
        chunk_blake3: vtop_log::proof::leaf_hash(chunk).to_hex().to_string(),
        proof_path: proof
            .path
            .iter()
            .map(|(hash, side)| ProofStep {
                hash: hash.to_hex().to_string(),
                side: match side {
                    Side::Left => "left",
                    Side::Right => "right",
                }
                .to_owned(),
            })
            .collect(),
    }
}

/// Check chunk bytes against a pinned root using a decoded proof file.
/// Usage-level problems (bad hex, scheme or index disagreement) are `Err`;
/// a clean `false` means the chunk does not belong under the root.
pub fn check_proof_file(
    root_hex: &str,
    chunk_size: u32,
    chunk_count: u64,
    index: u64,
    chunk: &[u8],
    proof_file: &ProofFile,
) -> Result<bool, String> {
    if proof_file.scheme != CHUNK_TREE_SCHEME_V1 {
        return Err(format!(
            "proof file scheme {:?} is not {CHUNK_TREE_SCHEME_V1:?}",
            proof_file.scheme
        ));
    }
    if proof_file.index != index {
        return Err(format!(
            "proof file was built for chunk {}, not chunk {index}",
            proof_file.index
        ));
    }
    let root = parse_hash_hex(root_hex).map_err(|error| format!("--root: {error}"))?;
    let mut path = Vec::with_capacity(proof_file.proof_path.len());
    for step in &proof_file.proof_path {
        let hash =
            parse_hash_hex(&step.hash).map_err(|error| format!("proof path sibling: {error}"))?;
        let side = match step.side.as_str() {
            "left" => Side::Left,
            "right" => Side::Right,
            other => return Err(format!("proof path side {other:?} is not left or right")),
        };
        path.push((hash, side));
    }
    let proof = ChunkProof { index, path };
    Ok(vtop_log::proof::verify_chunk(
        &root,
        ChunkParams {
            chunk_size,
            chunk_count,
        },
        index,
        chunk,
        &proof,
    ))
}

fn run_prove_chunk(args: &ProveChunkArgs, json: bool) -> i32 {
    match chunk_proof(&args.segment, args.index) {
        Ok((params, proof, chunk)) => {
            let proof_file = build_proof_file(params, &proof, &chunk);
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&proof_file)
                        .expect("proof file shape is valid JSON")
                );
            } else {
                println!(
                    "chunk {} of {} ({} bytes each, final chunk may be short)",
                    proof_file.index, proof_file.chunk_count, proof_file.chunk_size
                );
                println!("chunk blake3 (leaf): {}", proof_file.chunk_blake3);
                for step in &proof_file.proof_path {
                    println!("  sibling ({:<5}): {}", step.side, step.hash);
                }
            }
            EXIT_OK
        }
        Err(error) => {
            eprintln!("error: {error}");
            error_exit_code(&error)
        }
    }
}

fn run_verify_chunk(args: &VerifyChunkArgs) -> i32 {
    let chunk = match std::fs::read(&args.chunk_file) {
        Ok(chunk) => chunk,
        Err(error) => {
            eprintln!("error: --chunk-file {}: {error}", args.chunk_file.display());
            return EXIT_USAGE;
        }
    };
    let proof_bytes = match std::fs::read(&args.proof_file) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("error: --proof-file {}: {error}", args.proof_file.display());
            return EXIT_USAGE;
        }
    };
    let proof_file: ProofFile = match serde_json::from_slice(&proof_bytes) {
        Ok(proof_file) => proof_file,
        Err(error) => {
            eprintln!(
                "error: --proof-file {}: not a prove-chunk --json document: {error}",
                args.proof_file.display()
            );
            return EXIT_USAGE;
        }
    };
    match check_proof_file(
        &args.root,
        args.chunk_size,
        args.chunk_count,
        args.index,
        &chunk,
        &proof_file,
    ) {
        Ok(true) => {
            println!(
                "chunk {} of {} verifies against root {}",
                args.index, args.chunk_count, args.root
            );
            EXIT_OK
        }
        Ok(false) => {
            println!(
                "chunk {} of {} does NOT verify against root {}",
                args.index, args.chunk_count, args.root
            );
            EXIT_CORRUPT
        }
        Err(message) => {
            eprintln!("error: {message}");
            EXIT_USAGE
        }
    }
}

fn parse_hash_hex(value: &str) -> Result<blake3::Hash, String> {
    blake3::Hash::from_hex(value)
        .map_err(|_| format!("{value:?} is not a 64-hex-character BLAKE3 digest"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use vtop_log::verify::CheckOutcome;

    fn report_with(checks: Vec<CheckOutcome>) -> VerifyReport {
        VerifyReport {
            format_version: 2,
            segment_id: uuid::Uuid::nil(),
            record_count: 1,
            content_bytes: 100,
            chunk_count: 1,
            achieved: VerifyLevel::SelfConsistent,
            checks,
        }
    }

    fn outcome(name: &'static str, passed: bool) -> CheckOutcome {
        CheckOutcome {
            name,
            passed,
            detail: String::new(),
        }
    }

    #[test]
    fn require_flag_maps_onto_the_three_verify_levels() {
        assert_eq!(
            VerifyLevel::from(RequireLevel::SelfConsistent),
            VerifyLevel::SelfConsistent
        );
        assert_eq!(
            VerifyLevel::from(RequireLevel::Root),
            VerifyLevel::RootPinned
        );
        assert_eq!(
            VerifyLevel::from(RequireLevel::Auth),
            VerifyLevel::Authenticated
        );
    }

    #[test]
    fn key_pairing_takes_env_sources_before_file_sources_and_checks_counts() {
        let ids = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let envs = vec!["FIRST".to_owned(), "SECOND".to_owned()];
        let files = vec![PathBuf::from("third.key")];
        assert_eq!(
            pair_key_sources(&ids, &envs, &files).unwrap(),
            vec![
                ("a".to_owned(), KeySource::Env("FIRST".to_owned())),
                ("b".to_owned(), KeySource::Env("SECOND".to_owned())),
                ("c".to_owned(), KeySource::File(PathBuf::from("third.key"))),
            ]
        );
        assert!(pair_key_sources(&ids, &envs, &[]).is_err());
        assert!(pair_key_sources(&[], &envs, &[]).is_err());
        assert!(pair_key_sources(&[], &[], &[]).unwrap().is_empty());
    }

    #[test]
    fn build_expectations_rejects_bad_hex_missing_sources_and_duplicate_ids() {
        let base = VerifyArgs {
            paths: vec![PathBuf::from("x.segment")],
            expect_root: None,
            expect_manifest_digest: None,
            key_ids: Vec::new(),
            key_envs: Vec::new(),
            key_files: Vec::new(),
            require: RequireLevel::SelfConsistent,
        };
        let expectations = build_expectations(&base).unwrap();
        assert_eq!(expectations.require, VerifyLevel::SelfConsistent);
        assert!(expectations.chunk_tree_root.is_none());
        assert!(expectations.keyring.is_empty());

        let bad_root = VerifyArgs {
            expect_root: Some("zz".repeat(32)),
            paths: vec![PathBuf::from("x.segment")],
            expect_manifest_digest: None,
            key_ids: Vec::new(),
            key_envs: Vec::new(),
            key_files: Vec::new(),
            require: RequireLevel::SelfConsistent,
        };
        assert!(build_expectations(&bad_root).is_err());

        let unpaired = VerifyArgs {
            key_ids: vec!["k".to_owned()],
            paths: vec![PathBuf::from("x.segment")],
            expect_root: None,
            expect_manifest_digest: None,
            key_envs: Vec::new(),
            key_files: Vec::new(),
            require: RequireLevel::SelfConsistent,
        };
        assert!(build_expectations(&unpaired).is_err());
    }

    #[test]
    fn verify_maps_decoded_artifact_errors_to_corruption_and_path_shape_to_usage() {
        // Wrong extension is a caller mistake, caught before the verifier runs.
        let wrong_shape = VerifyArgs {
            paths: vec![PathBuf::from("bundle.active")],
            expect_root: None,
            expect_manifest_digest: None,
            key_ids: Vec::new(),
            key_envs: Vec::new(),
            key_files: Vec::new(),
            require: RequireLevel::SelfConsistent,
        };
        assert_eq!(run_verify(&wrong_shape, false), EXIT_USAGE);

        // A .segment path whose contents are garbage is an artifact problem.
        let directory = tempfile::tempdir().unwrap();
        let garbage = directory.path().join("garbage.segment");
        std::fs::write(&garbage, b"not a segment").unwrap();
        let corrupt = VerifyArgs {
            paths: vec![garbage],
            expect_root: None,
            expect_manifest_digest: None,
            key_ids: Vec::new(),
            key_envs: Vec::new(),
            key_files: Vec::new(),
            require: RequireLevel::SelfConsistent,
        };
        assert_eq!(run_verify(&corrupt, false), EXIT_CORRUPT);
    }

    #[test]
    fn exit_codes_rank_corruption_over_pins_over_authentication_over_level() {
        assert_eq!(
            exit_code_for(&report_with(vec![outcome(CHECK_FRAME_SCAN, true)])),
            EXIT_OK
        );
        // Corruption dominates everything else in the same report.
        assert_eq!(
            exit_code_for(&report_with(vec![
                outcome(CHECK_FRAME_SCAN, false),
                outcome(CHECK_ROOT_PIN, false),
                outcome(CHECK_STATEMENT_MAC, false),
                outcome(CHECK_REQUIRED_LEVEL, false),
            ])),
            EXIT_CORRUPT
        );
        assert_eq!(
            exit_code_for(&report_with(vec![
                outcome(CHECK_MANIFEST_DIGEST_PIN, false),
                outcome(CHECK_STATEMENT_MAC, false),
                outcome(CHECK_REQUIRED_LEVEL, false),
            ])),
            EXIT_PIN_MISMATCH
        );
        assert_eq!(
            exit_code_for(&report_with(vec![
                outcome(CHECK_STATEMENT_MAC, false),
                outcome(CHECK_REQUIRED_LEVEL, false),
            ])),
            EXIT_AUTH_FAILURE
        );
        assert_eq!(
            exit_code_for(&report_with(vec![outcome(CHECK_REQUIRED_LEVEL, false)])),
            EXIT_BELOW_REQUIRED_LEVEL
        );
        for name in [
            CHECK_CONTENT_ROOT,
            CHECK_CHUNK_SIDECAR,
            CHECK_MANIFEST_CANONICAL,
            CHECK_MANIFEST_CONSISTENCY,
            CHECK_STATEMENT_ECHO,
            CHECK_STATEMENT_DIGEST,
        ] {
            assert_eq!(
                exit_code_for(&report_with(vec![outcome(name, false)])),
                EXIT_CORRUPT,
                "check {name}"
            );
        }
    }
}
