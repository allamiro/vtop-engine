//! `vtopctl tier` — out-of-band cold-tier copy of sealed native segments.
//!
//! Who uploads: this tool, out-of-band, then commits evidence — exactly how
//! repair proofs work. No daemon, no broker involvement, and no delete
//! capability anywhere on this path: `UploadBackend::delete_object` is never
//! called. The metadata state machine alone judges the evidence; every step
//! here refuses fail-closed so nothing weaker than a verified authenticated
//! content root can ever reach `CommitTierEvidence`.
//!
//! The whole command is idempotent: re-running re-uploads the same immutable
//! bytes and the propose replays through dedup or rejects `AlreadyExists`.
//!
//! Large segments use resumable multipart upload (#191) when the backend
//! supports it and the object meets the configured threshold. Physical local
//! deletion is still authorized only by the metadata plan→confirm path
//! (`vtopctl meta confirm-retention-expired`); this module never deletes
//! tier objects. Rehydration downloads a version-pinned tier copy back to a
//! local path for repair/serve.

use clap::{ArgAction, Args, Subcommand};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_log::verify::{verify_sealed_segment, VerifyExpectations, VerifyLevel};
use vtop_log::{SegmentCommitKey, SegmentManifestV2};
use vtop_meta::command::CommandEnvelope;
use vtop_meta::MetadataCommand;
use vtop_upload::base::parse_s3_uri;
use vtop_upload::{
    cleanup_abandoned, upload_resumable, MultipartFence, MultipartUploadConfig, ObjectChecksum,
    UploadBackend,
};

#[derive(Subcommand, Debug)]
pub enum TierCommand {
    /// Upload a sealed segment + manifest to the object tier, verify the
    /// stored bytes by reading them back against the pinned root, and commit
    /// `CommitTierEvidence` through the meta admin endpoint.
    Copy(TierCopyArgs),
    /// Download a version-pinned tier object to a local path and verify its
    /// content digest (first-slice rehydration; does not mutate metadata).
    Rehydrate(TierRehydrateArgs),
    /// Abort abandoned in-progress multipart uploads whose session files are
    /// older than `upload.multipart_abandon_after_secs`.
    CleanupAbandoned(TierCleanupArgs),
}

#[derive(Args, Debug)]
pub struct TierCopyArgs {
    /// YAML deserializing the standard `upload:` backend configuration
    /// (`vtop_upload::build_backend`).
    #[arg(long)]
    pub upload_config: PathBuf,

    /// Meta admin client YAML (endpoint + PEM paths), as `vtopctl meta`.
    #[arg(long)]
    pub meta_config: PathBuf,

    /// Sealed v2 segment path (`.segment`, with its sibling manifest).
    #[arg(long)]
    pub segment: PathBuf,

    #[arg(long)]
    pub topic_uuid: Uuid,
    #[arg(long)]
    pub range_uuid: Uuid,
    #[arg(long)]
    pub segment_uuid: Uuid,

    /// Current metadata segment generation (CAS token).
    #[arg(long)]
    pub expected_generation: u64,
    /// Live lease fencing epoch of the range.
    #[arg(long)]
    pub fencing_epoch: u64,

    /// Operator-pinned metadata `SegmentRecord.content_root` (64 hex chars).
    #[arg(long)]
    pub expected_root: String,

    /// Destination object URI (the manifest is stored at
    /// `<object-uri>.manifest.json`).
    #[arg(long)]
    pub object_uri: String,

    /// Require an immutable-versioning bucket and a stored manifest version
    /// id (the #135 hardened profile). Default true; disabling drops the
    /// version pin, never the content verification.
    #[arg(long, default_value_t = true, action = ArgAction::Set, value_name = "BOOL")]
    pub require_versioning: bool,

    /// Environment variable holding a 64-hex-character segment commit key;
    /// upgrades the required local verify level to `authenticated`.
    #[arg(long)]
    pub commit_key_env: Option<String>,
    /// Key id the commit key is registered under (empty string is valid).
    #[arg(long, default_value = "")]
    pub commit_key_id: String,

    /// Node uuid recorded as the evidence verifier (must be a registered,
    /// non-dead node).
    #[arg(long)]
    pub verifier_node_uuid: Uuid,
    /// Consensus term recorded for audit.
    #[arg(long, default_value_t = 0)]
    pub verified_term: u64,

    #[arg(long, default_value_t = 0)]
    pub issued_at_ms: i64,
    /// Stable metadata idempotency identity. When omitted, VTOP derives it
    /// from the logical tier-copy inputs so a retry after an ambiguous
    /// response replays the original receipt. Supply a new value only after a
    /// definitive rejection whose prerequisite has been corrected.
    #[arg(long)]
    pub request_id: Option<Uuid>,

    /// Directory for persisted multipart session files (upload id, parts,
    /// per-part digests). Required when the segment is large enough to use
    /// resumable multipart; ignored for single-part puts.
    #[arg(long)]
    pub multipart_state_dir: Option<PathBuf>,

    /// Override `upload.multipart_part_size_bytes` for this invocation.
    #[arg(long)]
    pub multipart_part_size_bytes: Option<u64>,

    /// Override `upload.multipart_threshold_bytes` for this invocation.
    #[arg(long)]
    pub multipart_threshold_bytes: Option<u64>,

    /// Override `upload.multipart_max_parallelism` for this invocation.
    #[arg(long)]
    pub multipart_max_parallelism: Option<usize>,
}

#[derive(Args, Debug)]
pub struct TierRehydrateArgs {
    #[arg(long)]
    pub upload_config: PathBuf,
    /// Tier object URI recorded in `SegmentTierCopy.object_uri`.
    #[arg(long)]
    pub object_uri: String,
    /// Immutable object version pin from tier evidence (`object_version_id`).
    #[arg(long)]
    pub object_version_id: String,
    /// Expected whole-object digest (hex).
    #[arg(long)]
    pub expected_digest: String,
    #[arg(long, default_value = "blake3")]
    pub digest_algorithm: String,
    /// Expected byte length.
    #[arg(long)]
    pub expected_size: u64,
    /// Destination path for the rehydrated bytes.
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Args, Debug)]
pub struct TierCleanupArgs {
    #[arg(long)]
    pub upload_config: PathBuf,
    #[arg(long)]
    pub multipart_state_dir: PathBuf,
}

/// Resolved, backend-independent inputs of one tier copy. Split from the CLI
/// arguments so the verification flow is testable against [`MockBackend`]
/// without config files or an admin endpoint.
pub struct TierCopyRequest {
    pub segment: PathBuf,
    pub topic_uuid: Uuid,
    pub range_uuid: Uuid,
    pub segment_uuid: Uuid,
    pub expected_generation: u64,
    pub fencing_epoch: u64,
    pub expected_root: blake3::Hash,
    pub object_uri: String,
    pub require_versioning: bool,
    /// Commit keys by key id; non-empty upgrades the required verify level
    /// to `Authenticated`.
    pub keyring: BTreeMap<String, SegmentCommitKey>,
    pub verifier_node_uuid: Uuid,
    pub verified_term: u64,
    pub request_id: Uuid,
    pub issued_at_ms: i64,
    /// When set, large segments upload via resumable multipart.
    pub multipart: Option<MultipartUploadConfig>,
}

impl TierCopyRequest {
    fn expectations(&self) -> VerifyExpectations {
        VerifyExpectations {
            chunk_tree_root: Some(self.expected_root),
            manifest_core_digest: None,
            keyring: self.keyring.clone(),
            require: if self.keyring.is_empty() {
                VerifyLevel::RootPinned
            } else {
                VerifyLevel::Authenticated
            },
        }
    }
}

fn refuse(step: &str, detail: impl std::fmt::Display) -> String {
    format!("refusing at {step}: {detail}")
}

fn manifest_path_of(segment: &Path) -> Result<PathBuf, String> {
    if segment.extension().and_then(|value| value.to_str()) != Some("segment") {
        return Err("sealed segment path must end in .segment".to_owned());
    }
    let stem = segment
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| "segment filename is not UTF-8".to_owned())?;
    let parent = segment.parent().unwrap_or_else(|| Path::new("."));
    Ok(parent.join(format!("{stem}.manifest.json")))
}

fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// Run the full upload-and-verify flow against `backend` and return the
/// `CommitTierEvidence` command it justifies. Every step refuses fail-closed;
/// nothing is proposed by this function.
pub async fn run_tier_copy(
    backend: &dyn UploadBackend,
    request: &TierCopyRequest,
) -> Result<MetadataCommand, String> {
    // 1. Local pre-check against the operator-pinned root (and commit key
    //    when supplied). Any failing check refuses before any upload.
    let expectations = request.expectations();
    let report = verify_sealed_segment(&request.segment, &expectations)
        .map_err(|error| refuse("local verify", error))?;
    if !report.passed() {
        let failed: Vec<String> = report
            .checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| format!("{}: {}", check.name, check.detail))
            .collect();
        return Err(refuse("local verify", failed.join("; ")));
    }

    // Pin the canonical manifest core digest. A keyed/committed manifest
    // carries it inside its commit statement, and the passing report above
    // recomputed and cross-checked it (statement-digest check). A manifest
    // without a statement IS its own canonical core (the statement-stripped
    // bytes equal the file bytes, whose canonical encoding the report's
    // manifest-canonical check just verified), so hash the file directly.
    let manifest_path = manifest_path_of(&request.segment)?;
    let manifest_bytes =
        std::fs::read(&manifest_path).map_err(|error| refuse("manifest read", error))?;
    let manifest: SegmentManifestV2 = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| refuse("manifest decode", error))?;
    let manifest_core_digest = match &manifest.commit_statement {
        Some(statement) => *blake3::Hash::from_hex(&statement.manifest_core_digest)
            .map_err(|_| {
                refuse(
                    "manifest core digest",
                    "commit statement digest is not 64 hex characters",
                )
            })?
            .as_bytes(),
        None => *blake3::hash(&manifest_bytes).as_bytes(),
    };

    let byte_length = std::fs::metadata(&request.segment)
        .map_err(|error| refuse("segment stat", error))?
        .len();
    if byte_length == 0 {
        return Err(refuse("segment stat", "segment file is empty"));
    }
    // Segments may be as large as 8 GiB. Hash them with the shared bounded
    // streaming helper instead of materializing the file in memory.
    let segment_digest = vtop_core::checksum::blake3_file(&request.segment)
        .await
        .map_err(|error| refuse("segment digest", error))?;
    let manifest_digest = blake3_hex(&manifest_bytes);

    // 2. Versioning preflight: the hardened profile fails closed on any
    //    backend or bucket that cannot keep immutable versions.
    if request.require_versioning {
        let (bucket, _key) = parse_s3_uri(&request.object_uri)
            .map_err(|error| refuse("versioning preflight", error))?;
        backend
            .verify_bucket_versioning(&bucket)
            .await
            .map_err(|error| refuse("versioning preflight", error))?;
    }

    // 3. Upload segment and manifest, capturing both immutable object
    //    versions. A missing version id under the hardened profile is exactly
    //    the rollback surface #135 removed: abort.
    //
    // Large segments use resumable multipart when configured and supported.
    // Evidence commit still requires the strong read-back verify below —
    // multipart ETags never authorize CommitTierEvidence.
    let checksum = ObjectChecksum::new("blake3", &segment_digest);
    let stored_segment = match &request.multipart {
        Some(multipart)
            if multipart.should_multipart(backend, byte_length) =>
        {
            let fence = MultipartFence {
                expected_segment_generation: request.expected_generation,
                fencing_epoch: request.fencing_epoch,
                content_digest_hex: segment_digest.clone(),
                content_digest_algorithm: "blake3".to_owned(),
                byte_length,
            };
            upload_resumable(
                backend,
                multipart,
                &request.segment,
                &request.object_uri,
                Some(checksum),
                fence,
            )
            .await
            .map_err(|error| refuse("segment multipart upload", error))?
        }
        _ => backend
            .put_object(&request.segment, &request.object_uri, Some(checksum))
            .await
            .map_err(|error| refuse("segment upload", error))?,
    };
    if request.require_versioning && stored_segment.version_id.is_none() {
        return Err(refuse(
            "segment upload",
            "backend returned no immutable segment version id under --require-versioning",
        ));
    }
    let manifest_uri = format!("{}.manifest.json", request.object_uri);
    let stored = backend
        .put_manifest(
            &manifest_path,
            &manifest_uri,
            Some(ObjectChecksum::new("blake3", &manifest_digest)),
        )
        .await
        .map_err(|error| refuse("manifest upload", error))?;
    if request.require_versioning && stored.version_id.is_none() {
        return Err(refuse(
            "manifest upload",
            "backend returned no immutable version id under --require-versioning",
        ));
    }

    // 4. Read-back verification of the STORED bytes. A backend-limited
    //    (size/existence-only) result is never deletion authority.
    let verification = backend
        .verify_object(
            &request.object_uri,
            byte_length,
            Some(ObjectChecksum::new("blake3", &segment_digest)),
        )
        .await
        .map_err(|error| refuse("read-back verify", error))?;
    if !verification.passed {
        return Err(refuse("read-back verify", verification.message));
    }
    if verification.backend_limited {
        return Err(refuse(
            "read-back verify",
            format!(
                "backend-limited verification cannot authorize tier evidence: {}",
                verification.message
            ),
        ));
    }

    // `verify_object` is required to hash the actual stored body (or use a
    // storage-service-computed checksum) for a non-limited success. Because
    // the local file already passed the root-pinned segment verifier, exact
    // whole-file BLAKE3 equality proves the stored bytes carry that same
    // authenticated root without downloading up to 8 GiB into a Vec.
    let stored_manifest = match &stored.version_id {
        Some(version_id) => backend
            .get_manifest_pinned(
                &manifest_uri,
                version_id,
                vtop_core::manifest::MAX_MANIFEST_BYTES,
            )
            .await
            .map_err(|error| refuse("pinned manifest read-back", error))?,
        None => backend
            .get_object_bounded(&manifest_uri, vtop_core::manifest::MAX_MANIFEST_BYTES)
            .await
            .map_err(|error| refuse("manifest read-back", error))?,
    };
    // Byte equality with the locally verified manifest implies the core
    // digest matches; anything else is a replaced or torn store.
    if stored_manifest != manifest_bytes {
        return Err(refuse(
            "manifest read-back",
            "stored manifest bytes differ from the sealed manifest",
        ));
    }

    // 5. Everything verified: the command carries only verified facts.
    Ok(MetadataCommand::CommitTierEvidence {
        env: CommandEnvelope {
            request_id: request.request_id,
            issued_at_ms: request.issued_at_ms,
        },
        topic_uuid: request.topic_uuid,
        range_uuid: request.range_uuid,
        segment_uuid: request.segment_uuid,
        expected_segment_generation: request.expected_generation,
        content_root: *request.expected_root.as_bytes(),
        byte_length,
        backend_id: backend.backend_name().to_owned(),
        object_uri: request.object_uri.clone(),
        object_version_id: stored_segment.version_id,
        manifest_version_id: stored.version_id,
        manifest_core_digest,
        verification_method: vtop_meta::VerificationMethod::AuthenticatedContentRoot,
        verifier_node_uuid: request.verifier_node_uuid,
        fencing_epoch: request.fencing_epoch,
        verified_term: request.verified_term,
    })
}

/// Build a [`TierCopyRequest`] from CLI arguments, loading the optional
/// commit key from its environment variable (never echoed).
pub fn build_request(args: &TierCopyArgs) -> Result<TierCopyRequest, String> {
    let expected_root = blake3::Hash::from_hex(&args.expected_root)
        .map_err(|_| "--expected-root is not a 64-hex-character digest".to_owned())?;
    let mut keyring = BTreeMap::new();
    if let Some(variable) = &args.commit_key_env {
        let hex = std::env::var(variable)
            .map_err(|_| format!("--commit-key-env {variable}: environment variable is not set"))?;
        let key = SegmentCommitKey::from_hex(hex.trim())
            .map_err(|_| format!("--commit-key-env {variable}: not 64 hex characters"))?;
        keyring.insert(args.commit_key_id.clone(), key);
    }
    let request_id = args
        .request_id
        .unwrap_or_else(|| derived_tier_copy_request_id(args, expected_root.as_bytes()));
    Ok(TierCopyRequest {
        segment: args.segment.clone(),
        topic_uuid: args.topic_uuid,
        range_uuid: args.range_uuid,
        segment_uuid: args.segment_uuid,
        expected_generation: args.expected_generation,
        fencing_epoch: args.fencing_epoch,
        expected_root,
        object_uri: args.object_uri.clone(),
        require_versioning: args.require_versioning,
        keyring,
        verifier_node_uuid: args.verifier_node_uuid,
        verified_term: args.verified_term,
        request_id,
        issued_at_ms: args.issued_at_ms,
        multipart: None,
    })
}

fn multipart_config_from_args(
    args: &TierCopyArgs,
    upload: &vtop_core::config::UploadConfig,
) -> Result<Option<MultipartUploadConfig>, String> {
    let Some(state_dir) = &args.multipart_state_dir else {
        return Ok(None);
    };
    let mut cfg = MultipartUploadConfig::from_upload(upload, state_dir.clone());
    if let Some(v) = args.multipart_part_size_bytes {
        cfg.part_size_bytes = v;
    }
    if let Some(v) = args.multipart_threshold_bytes {
        cfg.threshold_bytes = v;
    }
    if let Some(v) = args.multipart_max_parallelism {
        cfg.max_parallelism = v;
    }
    if cfg.part_size_bytes == 0 || cfg.max_parallelism == 0 || cfg.threshold_bytes == 0 {
        return Err("multipart part size, threshold, and parallelism must be > 0".to_owned());
    }
    Ok(Some(cfg))
}

/// Rehydrate a version-pinned tier object to a local path after strong verify.
pub async fn run_tier_rehydrate(
    backend: &dyn UploadBackend,
    object_uri: &str,
    object_version_id: &str,
    expected_size: u64,
    expected: ObjectChecksum<'_>,
    output: &Path,
) -> Result<(), String> {
    if expected_size == 0 {
        return Err(refuse("rehydrate", "expected size must be > 0"));
    }
    // Cap the download to the expected size (+0) by using get_object_pinned
    // with expected_size as the bound — an oversized replacement fails closed.
    let max_bytes = usize::try_from(expected_size).map_err(|_| {
        refuse(
            "rehydrate",
            "expected size exceeds addressable memory on this host",
        )
    })?;
    let bytes = backend
        .get_object_pinned(object_uri, object_version_id, max_bytes)
        .await
        .map_err(|error| refuse("rehydrate download", error))?;
    if bytes.len() as u64 != expected_size {
        return Err(refuse(
            "rehydrate",
            format!(
                "size mismatch: expected {expected_size}, got {}",
                bytes.len()
            ),
        ));
    }
    let algo = expected
        .algorithm
        .parse::<vtop_core::types::ChecksumAlgorithm>()
        .map_err(|error| refuse("rehydrate", error))?;
    let actual = vtop_core::checksum::digest_bytes(algo, &bytes)
        .ok_or_else(|| refuse("rehydrate", "checksum algorithm disabled"))?;
    if !actual.eq_ignore_ascii_case(expected.hex) {
        return Err(refuse(
            "rehydrate",
            "pinned object digest does not match expected",
        ));
    }
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent).map_err(|error| refuse("rehydrate write", error))?;
    }
    std::fs::write(output, &bytes).map_err(|error| refuse("rehydrate write", error))?;
    Ok(())
}

fn derived_tier_copy_request_id(args: &TierCopyArgs, expected_root: &[u8; 32]) -> Uuid {
    let mut hasher = blake3::Hasher::new_derive_key("vtop tier-copy operation id v1");
    hasher.update(args.topic_uuid.as_bytes());
    hasher.update(args.range_uuid.as_bytes());
    hasher.update(args.segment_uuid.as_bytes());
    hasher.update(&args.expected_generation.to_be_bytes());
    hasher.update(&args.fencing_epoch.to_be_bytes());
    hasher.update(expected_root);
    hasher.update(&(args.object_uri.len() as u64).to_be_bytes());
    hasher.update(args.object_uri.as_bytes());
    hasher.update(&[u8::from(args.require_versioning)]);
    hasher.update(args.verifier_node_uuid.as_bytes());
    hasher.update(&args.verified_term.to_be_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Dispatch `vtopctl tier` and return a process exit code.
pub async fn run(command: TierCommand, json: bool) -> i32 {
    match run_inner(command, json).await {
        Ok(()) => 0,
        Err(message) => {
            eprintln!("error: {message}");
            1
        }
    }
}

async fn run_inner(command: TierCommand, json: bool) -> Result<(), String> {
    match command {
        TierCommand::Copy(args) => {
            let mut request = build_request(&args)?;
            let upload_text = std::fs::read_to_string(&args.upload_config)
                .map_err(|error| format!("read {}: {error}", args.upload_config.display()))?;
            let upload_config: vtop_core::config::UploadConfig = serde_yaml::from_str(&upload_text)
                .map_err(|error| format!("parse {}: {error}", args.upload_config.display()))?;
            request.multipart = multipart_config_from_args(&args, &upload_config)?;
            let backend = vtop_upload::build_backend(&upload_config)
                .await
                .map_err(|error| error.to_string())?;
            let command = run_tier_copy(backend.as_ref(), &request).await?;
            crate::meta_tools::propose_and_print(&args.meta_config, command, json).await
        }
        TierCommand::Rehydrate(args) => {
            let upload_text = std::fs::read_to_string(&args.upload_config)
                .map_err(|error| format!("read {}: {error}", args.upload_config.display()))?;
            let upload_config: vtop_core::config::UploadConfig = serde_yaml::from_str(&upload_text)
                .map_err(|error| format!("parse {}: {error}", args.upload_config.display()))?;
            let backend = vtop_upload::build_backend(&upload_config)
                .await
                .map_err(|error| error.to_string())?;
            run_tier_rehydrate(
                backend.as_ref(),
                &args.object_uri,
                &args.object_version_id,
                args.expected_size,
                ObjectChecksum::new(&args.digest_algorithm, &args.expected_digest),
                &args.output,
            )
            .await?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "rehydrated": true,
                        "object_uri": args.object_uri,
                        "object_version_id": args.object_version_id,
                        "output": args.output,
                        "bytes": args.expected_size,
                    })
                );
            } else {
                println!(
                    "rehydrated {}@{} -> {} ({} bytes)",
                    args.object_uri,
                    args.object_version_id,
                    args.output.display(),
                    args.expected_size
                );
            }
            Ok(())
        }
        TierCommand::CleanupAbandoned(args) => {
            let upload_text = std::fs::read_to_string(&args.upload_config)
                .map_err(|error| format!("read {}: {error}", args.upload_config.display()))?;
            let upload_config: vtop_core::config::UploadConfig = serde_yaml::from_str(&upload_text)
                .map_err(|error| format!("parse {}: {error}", args.upload_config.display()))?;
            let backend = vtop_upload::build_backend(&upload_config)
                .await
                .map_err(|error| error.to_string())?;
            let cfg =
                MultipartUploadConfig::from_upload(&upload_config, args.multipart_state_dir.clone());
            let cleaned = cleanup_abandoned(backend.as_ref(), &cfg)
                .await
                .map_err(|error| error.to_string())?;
            if json {
                println!("{}", serde_json::json!({ "cleaned": cleaned }));
            } else {
                println!("cleaned {cleaned} abandoned multipart session(s)");
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::path::Path;
    use vtop_core::errors::VtopError;
    use vtop_log::{
        ActiveSegment, Durability, LogRecord, RangeLineage, SegmentConfigV2, SegmentDescriptorV2,
    };
    use vtop_upload::base::{ObjectHead, StoredManifest, VerificationResult};
    use vtop_upload::MockBackend;

    const TOPIC: Uuid = Uuid::from_u128(0x20);
    const RANGE: Uuid = Uuid::from_u128(0x21);
    const SEGMENT_ID: Uuid = Uuid::from_u128(0x30);
    const NODE: Uuid = Uuid::from_u128(0x10);

    fn commit_key() -> SegmentCommitKey {
        SegmentCommitKey::from_hex(&"2a".repeat(32)).unwrap()
    }

    fn seal_bundle(directory: &Path) -> PathBuf {
        seal_bundle_with(directory, None)
    }

    fn seal_bundle_with(directory: &Path, key: Option<&SegmentCommitKey>) -> PathBuf {
        let active = directory.join("bundle.active");
        let descriptor = SegmentDescriptorV2 {
            segment_id: SEGMENT_ID,
            topic: "events.v1".to_owned(),
            topic_epoch: 1,
            lineage: RangeLineage::root(RANGE),
            base_offset: 0,
            segment_generation: 0,
            creation_node_id: NODE,
            creation_fencing_epoch: 1,
        };
        let config = SegmentConfigV2 {
            max_record_bytes: 1024,
            max_group_bytes: 4096,
            max_segment_bytes: 16 * 1024,
            max_segment_records: 100,
            index_stride: 2,
            chunk_size: 64 * 1024,
        };
        let mut segment = ActiveSegment::create_v2(&active, descriptor, config).unwrap();
        let records: Vec<LogRecord> = (0..3)
            .map(|sequence| LogRecord {
                producer_id: Uuid::from_u128(0x54),
                producer_epoch: 2,
                sequence,
                timestamp_millis: 1_700_000_000_000 + sequence as i64,
                attributes: 0,
                key: b"key".to_vec(),
                value: format!("value-{sequence}").into_bytes(),
            })
            .collect();
        segment.append_group(&records, Durability::Fsync).unwrap();
        drop(segment.seal_v2(key).unwrap());
        directory.join("bundle.segment")
    }

    fn sealed_root(sealed: &Path) -> blake3::Hash {
        let manifest_path = sealed.with_file_name("bundle.manifest.json");
        let manifest: SegmentManifestV2 =
            serde_json::from_slice(&std::fs::read(manifest_path).unwrap()).unwrap();
        blake3::Hash::from_hex(&manifest.chunk_tree_root).unwrap()
    }

    fn request(sealed: &Path, root: blake3::Hash) -> TierCopyRequest {
        TierCopyRequest {
            segment: sealed.to_path_buf(),
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT_ID,
            expected_generation: 1,
            fencing_epoch: 1,
            expected_root: root,
            object_uri: "s3://tier/native/events.v1/bundle.segment".to_owned(),
            require_versioning: true,
            keyring: BTreeMap::new(),
            verifier_node_uuid: NODE,
            verified_term: 2,
            request_id: Uuid::from_u128(0xfeed),
            issued_at_ms: 0,
            multipart: None,
        }
    }

    /// A versioned-looking backend that never returns a version id: the #135
    /// rollback surface the hardened profile must refuse.
    struct NoVersionBackend(MockBackend);

    #[async_trait]
    impl UploadBackend for NoVersionBackend {
        async fn put_object(
            &self,
            local_path: &Path,
            object_uri: &str,
            checksum: Option<ObjectChecksum<'_>>,
        ) -> Result<vtop_upload::StoredObject, VtopError> {
            self.0.put_object(local_path, object_uri, checksum).await?;
            Ok(vtop_upload::StoredObject { version_id: None })
        }
        async fn put_manifest(
            &self,
            local_path: &Path,
            manifest_uri: &str,
            checksum: Option<ObjectChecksum<'_>>,
        ) -> Result<StoredManifest, VtopError> {
            self.0
                .put_manifest(local_path, manifest_uri, checksum)
                .await?;
            Ok(StoredManifest { version_id: None })
        }
        async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
            self.0.head_object(object_uri).await
        }
        async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
            self.0.get_object(object_uri).await
        }
        async fn get_object_bounded(
            &self,
            object_uri: &str,
            max_bytes: usize,
        ) -> Result<Vec<u8>, VtopError> {
            self.0.get_object_bounded(object_uri, max_bytes).await
        }
        async fn verify_object(
            &self,
            object_uri: &str,
            expected_size: u64,
            expected: Option<ObjectChecksum<'_>>,
        ) -> Result<VerificationResult, VtopError> {
            self.0
                .verify_object(object_uri, expected_size, expected)
                .await
        }
        async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
            self.0.delete_object(object_uri).await
        }
        async fn verify_bucket_versioning(&self, _bucket: &str) -> Result<(), VtopError> {
            Ok(())
        }
        fn backend_name(&self) -> &'static str {
            "mock-unversioned"
        }
        fn supports_checksum_verification(&self) -> bool {
            true
        }
        fn supports_multipart(&self) -> bool {
            false
        }
        async fn get_object_pinned(
            &self,
            object_uri: &str,
            version_id: &str,
            max_bytes: usize,
        ) -> Result<Vec<u8>, VtopError> {
            self.0
                .get_object_pinned(object_uri, version_id, max_bytes)
                .await
        }
    }

    #[tokio::test]
    async fn tier_copy_success_produces_a_well_formed_commit_tier_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let sealed = seal_bundle(directory.path());
        let root = sealed_root(&sealed);
        let backend = MockBackend::new();
        let command = run_tier_copy(&backend, &request(&sealed, root))
            .await
            .expect("tier copy must succeed");
        let MetadataCommand::CommitTierEvidence {
            env,
            topic_uuid,
            range_uuid,
            segment_uuid,
            expected_segment_generation,
            content_root,
            byte_length,
            backend_id,
            object_uri,
            object_version_id,
            manifest_version_id,
            manifest_core_digest,
            verification_method,
            verifier_node_uuid,
            fencing_epoch,
            verified_term,
        } = &command
        else {
            panic!("expected CommitTierEvidence, got {command:?}");
        };
        assert_eq!(env.request_id, Uuid::from_u128(0xfeed));
        assert_eq!(*topic_uuid, TOPIC);
        assert_eq!(*range_uuid, RANGE);
        assert_eq!(*segment_uuid, SEGMENT_ID);
        assert_eq!(*expected_segment_generation, 1);
        assert_eq!(*content_root, *root.as_bytes());
        assert_eq!(
            *byte_length,
            std::fs::metadata(&sealed).unwrap().len(),
            "byte length pins the sealed file"
        );
        assert_eq!(backend_id, "mock");
        assert_eq!(object_uri, "s3://tier/native/events.v1/bundle.segment");
        assert!(
            object_version_id.is_some(),
            "versioned backend must pin the segment object"
        );
        assert!(manifest_version_id.is_some(), "versioned backend must pin");
        assert_eq!(
            *verification_method,
            vtop_meta::VerificationMethod::AuthenticatedContentRoot
        );
        assert_eq!(*verifier_node_uuid, NODE);
        assert_eq!(*fencing_epoch, 1);
        assert_eq!(*verified_term, 2);
        // A statement-less manifest is its own canonical core, so the
        // committed digest pins the manifest file bytes.
        let manifest_bytes = std::fs::read(sealed.with_file_name("bundle.manifest.json")).unwrap();
        assert_eq!(
            *manifest_core_digest,
            *blake3::hash(&manifest_bytes).as_bytes()
        );
        // The command round-trips the wire codec (well-formed by
        // construction).
        let encoded = command.encode().unwrap();
        assert_eq!(MetadataCommand::decode(&encoded).unwrap(), command);
        // Both objects landed in the store.
        assert!(backend.contains("s3://tier/native/events.v1/bundle.segment"));
        assert!(backend.contains("s3://tier/native/events.v1/bundle.segment.manifest.json"));
    }

    #[tokio::test]
    async fn tier_copy_with_a_commit_key_requires_and_pins_the_authenticated_level() {
        let directory = tempfile::tempdir().unwrap();
        let key = commit_key();
        let sealed = seal_bundle_with(directory.path(), Some(&key));
        let root = sealed_root(&sealed);
        let backend = MockBackend::new();
        let mut authenticated = request(&sealed, root);
        authenticated.keyring = BTreeMap::from([(String::new(), key)]);
        let command = run_tier_copy(&backend, &authenticated)
            .await
            .expect("keyed tier copy must succeed");
        // The committed digest is the statement's recomputed core digest.
        let manifest: SegmentManifestV2 = serde_json::from_slice(
            &std::fs::read(sealed.with_file_name("bundle.manifest.json")).unwrap(),
        )
        .unwrap();
        let statement = manifest.commit_statement.unwrap();
        let MetadataCommand::CommitTierEvidence {
            manifest_core_digest,
            ..
        } = &command
        else {
            panic!("expected CommitTierEvidence");
        };
        assert_eq!(
            *manifest_core_digest,
            *blake3::Hash::from_hex(&statement.manifest_core_digest)
                .unwrap()
                .as_bytes()
        );

        // The wrong key refuses at the local verify (below the required
        // Authenticated level).
        let sealed_other = {
            let other_dir = tempfile::tempdir().unwrap();
            let sealed = seal_bundle_with(other_dir.path(), Some(&commit_key()));
            let root = sealed_root(&sealed);
            let mut wrong = request(&sealed, root);
            wrong.keyring = BTreeMap::from([(
                String::new(),
                SegmentCommitKey::from_hex(&"11".repeat(32)).unwrap(),
            )]);
            let error = run_tier_copy(&backend, &wrong)
                .await
                .expect_err("a wrong commit key must refuse");
            assert!(error.contains("local verify"), "{error}");
            sealed
        };
        drop(sealed_other);
    }

    #[tokio::test]
    async fn tier_copy_refuses_a_failed_local_verify_before_any_upload() {
        let directory = tempfile::tempdir().unwrap();
        let sealed = seal_bundle(directory.path());
        // A wrong pinned root fails the local root-pin check.
        let wrong_root = blake3::hash(b"not the segment root");
        let backend = MockBackend::new();
        let error = run_tier_copy(&backend, &request(&sealed, wrong_root))
            .await
            .expect_err("wrong pinned root must refuse");
        assert!(error.contains("local verify"), "{error}");
        assert!(
            !backend.contains("s3://tier/native/events.v1/bundle.segment"),
            "nothing may be uploaded after a failed local verify"
        );
    }

    #[tokio::test]
    async fn tier_copy_refuses_backend_limited_read_back_verification() {
        let directory = tempfile::tempdir().unwrap();
        let sealed = seal_bundle(directory.path());
        let root = sealed_root(&sealed);
        let backend = MockBackend::limited();
        let error = run_tier_copy(&backend, &request(&sealed, root))
            .await
            .expect_err("size-only verification is never deletion authority");
        assert!(error.contains("backend-limited"), "{error}");
    }

    #[tokio::test]
    async fn tier_copy_refuses_a_missing_version_id_under_require_versioning() {
        let directory = tempfile::tempdir().unwrap();
        let sealed = seal_bundle(directory.path());
        let root = sealed_root(&sealed);
        let backend = NoVersionBackend(MockBackend::new());
        let error = run_tier_copy(&backend, &request(&sealed, root))
            .await
            .expect_err("missing version id must refuse under the hardened profile");
        assert!(error.contains("no immutable segment version id"), "{error}");
    }

    #[tokio::test]
    async fn tier_copy_refuses_a_read_back_mismatch_of_stored_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let sealed = seal_bundle(directory.path());
        let root = sealed_root(&sealed);
        // The storage service silently replaces stored bytes after upload
        // while leaving size and uploader metadata unchanged.
        let backend = MockBackend::corrupting();
        let error = run_tier_copy(&backend, &request(&sealed, root))
            .await
            .expect_err("corrupted stored bytes must refuse");
        assert!(error.contains("read-back verify"), "{error}");
    }

    #[tokio::test]
    async fn tier_copy_versioning_preflight_fails_closed_on_non_s3_uris() {
        let directory = tempfile::tempdir().unwrap();
        let sealed = seal_bundle(directory.path());
        let root = sealed_root(&sealed);
        let backend = MockBackend::new();
        let mut bad_uri = request(&sealed, root);
        bad_uri.object_uri = "file:///tier/bundle.segment".to_owned();
        let error = run_tier_copy(&backend, &bad_uri)
            .await
            .expect_err("non-s3 uri cannot satisfy the versioning preflight");
        assert!(error.contains("versioning preflight"), "{error}");
    }

    #[tokio::test]
    async fn tier_copy_multipart_path_still_pins_versions_and_commits_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let sealed = seal_bundle(directory.path());
        let root = sealed_root(&sealed);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new();
        let mut req = request(&sealed, root);
        let size = std::fs::metadata(&sealed).unwrap().len();
        req.multipart = Some(MultipartUploadConfig {
            part_size_bytes: (size / 2).max(1),
            threshold_bytes: 1,
            max_parallelism: 2,
            abandon_after_secs: 60,
            state_dir: state.path().to_path_buf(),
        });
        let command = run_tier_copy(&backend, &req)
            .await
            .expect("multipart tier copy must succeed");
        let MetadataCommand::CommitTierEvidence {
            object_version_id,
            manifest_version_id,
            verification_method,
            ..
        } = &command
        else {
            panic!("expected CommitTierEvidence");
        };
        assert!(object_version_id.is_some());
        assert!(manifest_version_id.is_some());
        assert_eq!(
            *verification_method,
            vtop_meta::VerificationMethod::AuthenticatedContentRoot
        );
        assert!(backend.contains(&req.object_uri));
    }

    #[tokio::test]
    async fn tier_rehydrate_writes_pinned_bytes_after_digest_check() {
        let backend = MockBackend::new();
        let directory = tempfile::tempdir().unwrap();
        let sealed = seal_bundle(directory.path());
        let data = std::fs::read(&sealed).unwrap();
        let digest = vtop_core::checksum::blake3_bytes(&data);
        let stored = backend
            .put_object(
                &sealed,
                "s3://tier/rehydrate.segment",
                Some(ObjectChecksum::new("blake3", &digest)),
            )
            .await
            .unwrap();
        let version = stored.version_id.unwrap();
        let out = directory.path().join("restored.segment");
        run_tier_rehydrate(
            &backend,
            "s3://tier/rehydrate.segment",
            &version,
            data.len() as u64,
            ObjectChecksum::new("blake3", &digest),
            &out,
        )
        .await
        .unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), data);
    }

    #[test]
    fn build_request_maps_arguments_and_rejects_bad_hex() {
        let mut args = TierCopyArgs {
            upload_config: PathBuf::from("upload.yaml"),
            meta_config: PathBuf::from("meta.yaml"),
            segment: PathBuf::from("bundle.segment"),
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT_ID,
            expected_generation: 1,
            fencing_epoch: 3,
            expected_root: "ab".repeat(32),
            object_uri: "s3://tier/bundle.segment".to_owned(),
            require_versioning: true,
            commit_key_env: None,
            commit_key_id: String::new(),
            verifier_node_uuid: NODE,
            verified_term: 7,
            issued_at_ms: 0,
            request_id: Some(Uuid::from_u128(1)),
            multipart_state_dir: None,
            multipart_part_size_bytes: None,
            multipart_threshold_bytes: None,
            multipart_max_parallelism: None,
        };
        let request = build_request(&args).unwrap();
        assert_eq!(request.expected_root.to_hex().to_string(), "ab".repeat(32));
        assert!(request.keyring.is_empty());
        assert_eq!(request.request_id, Uuid::from_u128(1));

        // The default operation ID is retry-stable for identical logical
        // inputs and changes when the destination operation changes.
        args.request_id = None;
        let derived = build_request(&args).unwrap().request_id;
        assert_eq!(build_request(&args).unwrap().request_id, derived);
        args.object_uri = "s3://tier/other.segment".to_owned();
        assert_ne!(build_request(&args).unwrap().request_id, derived);

        args.expected_root = "zz".repeat(32);
        assert!(build_request(&args).is_err());
    }
}
