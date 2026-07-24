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

use clap::{ArgAction, Args, Subcommand};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_log::verify::{verify_sealed_segment, VerifyExpectations, VerifyLevel};
use vtop_log::{SegmentCommitKey, SegmentManifestV2};
use vtop_meta::command::CommandEnvelope;
use vtop_meta::MetadataCommand;
use vtop_upload::base::parse_s3_uri;
use vtop_upload::{ObjectChecksum, UploadBackend};

#[derive(Subcommand, Debug)]
pub enum TierCommand {
    /// Upload a sealed segment + manifest to the object tier, verify the
    /// stored bytes by reading them back against the pinned root, and commit
    /// `CommitTierEvidence` through the meta admin endpoint.
    Copy(TierCopyArgs),
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
    #[arg(long)]
    pub request_id: Option<Uuid>,
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

    // 3. Upload segment and manifest, capturing the immutable manifest
    //    version. A missing version id under the hardened profile is exactly
    //    the rollback surface #135 removed: abort.
    backend
        .put_object(
            &request.segment,
            &request.object_uri,
            Some(ObjectChecksum::new("blake3", &segment_digest)),
        )
        .await
        .map_err(|error| refuse("segment upload", error))?;
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
        request_id: args.request_id.unwrap_or_else(Uuid::new_v4),
        issued_at_ms: args.issued_at_ms,
    })
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
            let request = build_request(&args)?;
            let upload_text = std::fs::read_to_string(&args.upload_config)
                .map_err(|error| format!("read {}: {error}", args.upload_config.display()))?;
            let upload_config: vtop_core::config::UploadConfig = serde_yaml::from_str(&upload_text)
                .map_err(|error| format!("parse {}: {error}", args.upload_config.display()))?;
            let backend = vtop_upload::build_backend(&upload_config)
                .await
                .map_err(|error| error.to_string())?;
            let command = run_tier_copy(backend.as_ref(), &request).await?;
            crate::meta_tools::propose_and_print(&args.meta_config, command, json).await
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
        ) -> Result<(), VtopError> {
            self.0.put_object(local_path, object_uri, checksum).await
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
        assert!(error.contains("no immutable version id"), "{error}");
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

    #[test]
    fn build_request_maps_arguments_and_rejects_bad_hex() {
        let args = TierCopyArgs {
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
        };
        let request = build_request(&args).unwrap();
        assert_eq!(request.expected_root.to_hex().to_string(), "ab".repeat(32));
        assert!(request.keyring.is_empty());
        assert_eq!(request.request_id, Uuid::from_u128(1));

        let bad = TierCopyArgs {
            expected_root: "zz".repeat(32),
            ..args
        };
        assert!(build_request(&bad).is_err());
    }
}
