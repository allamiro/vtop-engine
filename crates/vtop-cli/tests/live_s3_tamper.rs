//! The anti-tamper features against a real S3 server (the lab's MinIO).
//!
//! Every case is a pair: someone changes the archive, then a defence reports
//! or refuses it. Where a defence is OFF, the same change is shown NOT to be
//! caught, so the limits of each feature are stated by a test and not only
//! by a document. `docs/reference/tamper-evidence.md` lists every case with
//! its test name; `tamper_evidence_doc.rs` fails if the two drift apart.
//!
//! The attacker here holds write access to the bucket (the lab's root
//! credentials, used through a bare S3 client, never through the engine) and
//! not the manifest MAC key. Where a case says so, the attacker also edits
//! the ledger, which is what a second line of defence has to survive.
//!
//! What each group of tests covers:
//!
//! - signed manifests (`manifest.mac`, #685): a replaced object with a
//!   recomputed manifest digest, another batch's valid manifest, a made-up
//!   key id, a retired key, and the same changes with the MAC off;
//! - recovery's storage re-check: a VERIFIED row whose object or manifest
//!   was replaced is replayed, never committed (a copy of the row is put in
//!   a fresh ledger, as a restart after a crash between VERIFIED and the
//!   source commit would find it);
//! - S3 Object Lock (`upload.object_lock`, #684): what the store refuses and
//!   what it does not, and `scrub --check-retention`;
//! - bucket versioning (`upload.require_object_versioning`): an overwrite is
//!   a new version, the scrub compares the current key with the pinned one;
//! - verification receipts (`manifest.verification_receipts`, #671);
//! - `audit-coverage --check-storage` (#573): what a HEAD can and cannot
//!   prove.
//!
//! The audit trail's hash chain is in `tamper_audit_trail.rs`; it needs no
//! store.
//!
//! Run against the compose MinIO lab with:
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin
//!  AWS_SECRET_ACCESS_KEY=minioadmin cargo test -p vtop-cli
//!  --test live_s3_tamper -- --ignored --test-threads=1`
//! Every object a test writes is deleted at its end (governance retention
//! bypassed with the lab's root credentials). CI runs it in the
//! `s3 compat (minio)` leg of `s3-compat-matrix.yml`.
#![cfg(unix)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use serde_json::{json, Value};
use vtop_cli::engine::RecoverySummary;
use vtop_cli::scrub::{
    self, ReceiptResult, RetentionResult, ScrubOptions, ScrubReport, ScrubResult, ScrubRow,
};
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::checksum::sha256_bytes;
use vtop_core::config::{
    ManifestKeyConfig, ManifestMacConfig, ObjectLockConfig, ObjectLockMode, StreamsConfig,
    VtopConfig,
};
use vtop_core::manifest::{ManifestWriteVersion, ParsedManifest};
use vtop_core::partitioning::receipt_uri;
use vtop_core::state_machine::BatchState;
use vtop_core::types::SourceType;
use vtop_state::{BatchPatch, BatchRecord, VerificationClass};

// ---- the lab ----------------------------------------------------------------

fn endpoint() -> String {
    std::env::var("VTOP_TEST_S3_ENDPOINT")
        .expect("VTOP_TEST_S3_ENDPOINT must name the live endpoint")
}

/// An ordinary bucket: no versioning, no Object Lock.
fn plain_bucket() -> String {
    std::env::var("VTOP_TEST_S3_BUCKET").unwrap_or_else(|_| "telemetry-raw".into())
}

/// A bucket created with Object Lock (and so versioned): the lab's.
fn locked_bucket() -> String {
    std::env::var("VTOP_TEST_S3_LOCKED_BUCKET").unwrap_or_else(|_| "telemetry-locked".into())
}

/// Created by the tests that need a versioned bucket without a lock.
const VERSIONED_BUCKET: &str = "telemetry-tamper-versioned";
/// Created, versioned, and then suspended by the test that does so.
const SUSPENDED_BUCKET: &str = "telemetry-tamper-suspended";
/// The same, for the test that suspends it under a running engine.
const SUSPENDED_RUNNING_BUCKET: &str = "telemetry-tamper-suspended-running";

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

async fn s3_client() -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    let env = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"));
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint())
        .force_path_style(true)
        .credentials_provider(Credentials::new(
            env("AWS_ACCESS_KEY_ID"),
            env("AWS_SECRET_ACCESS_KEY"),
            None,
            None,
            "test",
        ))
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

/// A bucket the lab does not create: made here, optionally versioned.
async fn ensure_bucket(client: &aws_sdk_s3::Client, name: &str, versioned: bool) {
    if let Err(e) = client.create_bucket().bucket(name).send().await {
        let said = format!("{e:?}");
        assert!(
            said.contains("BucketAlreadyOwnedByYou") || said.contains("BucketAlreadyExists"),
            "creating {name}: {said}"
        );
    }
    if versioned {
        set_versioning(client, name, BucketVersioningStatus::Enabled).await;
    }
}

async fn set_versioning(client: &aws_sdk_s3::Client, name: &str, status: BucketVersioningStatus) {
    client
        .put_bucket_versioning()
        .bucket(name)
        .versioning_configuration(VersioningConfiguration::builder().status(status).build())
        .send()
        .await
        .unwrap_or_else(|e| panic!("setting versioning on {name}: {e:?}"));
}

/// Every version and delete marker under `prefix`, as (key, version id,
/// is_delete_marker).
async fn versions_under(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> Vec<(String, String, bool)> {
    let listed = client
        .list_object_versions()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .unwrap();
    let mut all: Vec<(String, String, bool)> = listed
        .versions()
        .iter()
        .map(|v| {
            (
                v.key().unwrap().to_owned(),
                v.version_id().unwrap_or("null").to_owned(),
                false,
            )
        })
        .chain(listed.delete_markers().iter().map(|m| {
            (
                m.key().unwrap().to_owned(),
                m.version_id().unwrap_or("null").to_owned(),
                true,
            )
        }))
        .collect();
    all.sort();
    all
}

// ---- the attacker's tools -----------------------------------------------------

/// What an attacker who can write the bucket but does not hold the key can
/// produce from a stored document (`block` is `manifest` or `receipt`): the
/// document with `edit` applied, its hash recomputed the way the engine does
/// it (the document with the hash and MAC blanked, keys sorted, compact), and
/// the MAC left as it was (stale) unless `sign` gives a key the attacker does
/// hold, as hex, and the id to record.
fn forge(
    original: &[u8],
    block: &str,
    edit: impl FnOnce(&mut Value),
    sign: Option<(&str, Option<&str>)>,
) -> Vec<u8> {
    let mut doc: Value = serde_json::from_slice(original).unwrap();
    edit(&mut doc);
    if let Some((_, Some(id))) = sign {
        doc[block]["mac_key_id"] = json!(id);
    }
    if sign.is_some() && doc[block].get("mac").is_none() {
        doc[block]["mac"] = json!("");
    }
    let mut canonical = doc.clone();
    canonical[block]["sha256"] = json!("");
    if canonical[block].get("mac").is_some() {
        canonical[block]["mac"] = json!("");
    }
    canonical.sort_all_objects();
    let auth = serde_json::to_vec(&canonical).unwrap();
    doc[block]["sha256"] = json!(sha256_bytes(&auth));
    if let Some((hex, _)) = sign {
        let mut key = [0u8; 32];
        for (i, byte) in key.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).unwrap();
        }
        doc[block]["mac"] = json!(blake3::keyed_hash(&key, &auth).to_hex().to_string());
    }
    serde_json::to_vec_pretty(&doc).unwrap()
}

/// The bytes an attacker puts where an object was.
const FORGED_OBJECT: &[u8] = b"forged: bytes the attacker chose, not what the source produced\n";

/// A manifest describing [`FORGED_OBJECT`], digest recomputed, MAC stale.
fn forged_manifest_of(original: &[u8], sign: Option<(&str, Option<&str>)>) -> Vec<u8> {
    forge(
        original,
        "manifest",
        |doc| {
            doc["object"]["size_bytes"] = json!(FORGED_OBJECT.len());
            doc["object"]["checksum"] = json!(sha256_bytes(FORGED_OBJECT));
        },
        sign,
    )
}

/// The ledger an attacker with write access to it would leave: the row
/// agrees with the forged manifest.
fn forge_ledger_row(row: &mut BatchRecord, forged_manifest: &[u8]) {
    let parsed = ParsedManifest::from_slice(forged_manifest).unwrap();
    let fields = parsed.fields();
    row.manifest_sha256 = Some(fields.manifest.sha256.clone());
    row.object_sha256 = Some(fields.object.checksum.clone());
    row.object_size_bytes = Some(fields.object.size_bytes as i64);
}

// ---- keys ------------------------------------------------------------------------

/// Key files outside the archive: `q3` (`3c` x 32) and `q4` (`4d` x 32).
const Q3_HEX: &str = "3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c";
const Q4_HEX: &str = "4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d";
/// A key the attacker made up: no engine is configured with it.
const ATTACKER_HEX: &str = "a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7";

/// A `manifest.mac` over key files in `dir`: `keys` as (id, legacy).
fn keyring(dir: &Path, active: &str, keys: &[&str], retired: &[&str]) -> ManifestMacConfig {
    let key_dir = dir.join("keys");
    std::fs::create_dir_all(&key_dir).unwrap();
    for (id, hex) in [("q3", Q3_HEX), ("q4", Q4_HEX)] {
        let path = key_dir.join(id);
        std::fs::write(&path, format!("{hex}\n")).unwrap();
        // Owner-only, as the engine requires of a key file (#733).
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    ManifestMacConfig {
        active: active.into(),
        keys: keys
            .iter()
            .map(|id| ManifestKeyConfig {
                id: (*id).into(),
                env: None,
                file: Some(key_dir.join(id).to_string_lossy().into_owned()),
                legacy: false,
            })
            .collect(),
        retired: retired.iter().map(|r| r.to_string()).collect(),
    }
}

// ---- an archive in the lab ------------------------------------------------------

/// What an archive is written with.
#[derive(Clone, Default)]
struct Setup {
    /// (active key id, key ids that verify).
    mac: Option<(&'static str, Vec<&'static str>)>,
    versioning: bool,
    lock: bool,
    receipts: bool,
}

struct Archive {
    dir: tempfile::TempDir,
    inputs: Vec<PathBuf>,
    bucket: String,
    prefix: String,
    setup: Setup,
    client: aws_sdk_s3::Client,
    engine: Engine,
    /// The committed rows, by batch id.
    rows: Vec<BatchRecord>,
}

impl Archive {
    /// `n` batches (one per input file), archived and committed.
    async fn new(case: &str, bucket: &str, n: usize, setup: Setup) -> Archive {
        let dir = tempfile::tempdir().unwrap();
        let inputs: Vec<PathBuf> = (0..n)
            .map(|i| {
                let path = dir.path().join(format!("app-{i}.log"));
                let mut f = std::fs::File::create(&path).unwrap();
                for line in 0..5 {
                    writeln!(f, "tamper case {case} file {i} record {line}").unwrap();
                }
                path
            })
            .collect();
        let prefix = format!("vtop-it-tamper-{case}-{}", nanos());
        let cfg = config(dir.path(), "state", bucket, &prefix, &inputs, &setup);
        let engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .expect("the engine starts against the lab");
        let mut archive = Archive {
            prefix,
            bucket: bucket.to_owned(),
            client: s3_client().await,
            engine,
            dir,
            inputs,
            setup,
            rows: Vec::new(),
        };
        let outcomes = archive.engine.process_once(SourceType::File).await.unwrap();
        assert_eq!(outcomes.len(), n, "one batch per input file: {outcomes:?}");
        assert!(outcomes.iter().all(|o| o.committed), "{outcomes:?}");
        let mut rows = archive.engine.store.list_batches().await.unwrap();
        rows.sort_by(|a, b| a.batch_id.cmp(&b.batch_id));
        archive.rows = rows;
        archive
    }

    /// The config over ledger `state`, with `mac` replacing the archive's own
    /// keyring when given: (active, verifying ids, retired ids).
    fn config(&self, state: &str, mac: Option<(&str, &[&str], &[&str])>) -> VtopConfig {
        let mut cfg = config(
            self.dir.path(),
            state,
            &self.bucket,
            &self.prefix,
            &self.inputs,
            &self.setup,
        );
        if let Some((active, keys, retired)) = mac {
            cfg.manifest.mac = Some(keyring(self.dir.path(), active, keys, retired));
        }
        cfg
    }

    fn key_of(&self, uri: &str) -> String {
        uri.strip_prefix(&format!("s3://{}/", self.bucket))
            .unwrap_or_else(|| panic!("{uri} is not in {}", self.bucket))
            .to_owned()
    }

    fn object_uri(&self, i: usize) -> String {
        self.rows[i].object_uri.clone().unwrap()
    }

    fn manifest_uri(&self, i: usize) -> String {
        self.rows[i].manifest_uri.clone().unwrap()
    }

    fn receipt_uri(&self, i: usize) -> String {
        receipt_uri(&self.manifest_uri(i)).unwrap()
    }

    /// The bytes at a key's current version.
    async fn get(&self, uri: &str) -> Vec<u8> {
        self.client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key_of(uri))
            .send()
            .await
            .unwrap_or_else(|e| panic!("reading {uri}: {e:?}"))
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes()
            .to_vec()
    }

    /// The bytes of one stored version.
    async fn get_version(&self, uri: &str, version: &str) -> Vec<u8> {
        self.client
            .get_object()
            .bucket(&self.bucket)
            .key(self.key_of(uri))
            .version_id(version)
            .send()
            .await
            .unwrap_or_else(|e| panic!("reading {uri} version {version}: {e:?}"))
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes()
            .to_vec()
    }

    /// The attacker's overwrite: a plain PUT at the key.
    async fn put(&self, uri: &str, bytes: &[u8]) {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(self.key_of(uri))
            .body(bytes.to_vec().into())
            .send()
            .await
            .unwrap_or_else(|e| panic!("the attacker's PUT of {uri}: {e:?}"));
    }

    /// The attacker's delete: a plain DELETE at the key.
    async fn delete(&self, uri: &str) {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(self.key_of(uri))
            .send()
            .await
            .unwrap_or_else(|e| panic!("the attacker's DELETE of {uri}: {e:?}"));
    }

    async fn ledger_rows(&self) -> Vec<ScrubRow> {
        scrub::committed_rows(self.engine.store.list_batches().await.unwrap(), Vec::new())
    }

    async fn scrub(&self, opts: &ScrubOptions) -> ScrubReport {
        self.scrub_rows(&self.engine, self.ledger_rows().await, opts)
            .await
    }

    async fn scrub_rows(
        &self,
        engine: &Engine,
        rows: Vec<ScrubRow>,
        opts: &ScrubOptions,
    ) -> ScrubReport {
        engine.scrub(rows, opts).await.unwrap()
    }

    /// The archive's ledger rows with row `i` rewritten by `edit`: what a
    /// scrub reads when the attacker also changed the ledger.
    async fn ledger_rows_edited(
        &self,
        i: usize,
        edit: impl FnOnce(&mut BatchRecord),
    ) -> Vec<ScrubRow> {
        let mut rows = self.ledger_rows().await;
        let at = rows
            .iter()
            .position(|r| r.record.batch_id == self.rows[i].batch_id)
            .unwrap();
        edit(&mut rows[at].record);
        rows
    }

    /// A restart that finds `row` VERIFIED and not committed (the engine
    /// died between the two), over a fresh ledger and the same store.
    /// Returns what recovery did and the row's state after it.
    async fn recover_verified(
        &self,
        row: &BatchRecord,
        cfg: VtopConfig,
    ) -> (RecoverySummary, BatchState) {
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let mut verified = row.clone();
        verified.state = BatchState::Batching;
        verified.verification_class = None;
        verified.owner = None;
        verified.lease_expires_at = None;
        engine.store.save_batch_state(&verified).await.unwrap();
        for state in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
        ] {
            engine
                .store
                .update_batch_state(&row.batch_id, state, &BatchPatch::default())
                .await
                .unwrap();
        }
        engine
            .store
            .mark_verified(&row.batch_id, VerificationClass::Strong)
            .await
            .unwrap();
        let summary = engine.recover().await.unwrap();
        let after = engine
            .store
            .get_batch(&row.batch_id)
            .await
            .unwrap()
            .unwrap()
            .state;
        (summary, after)
    }

    /// Delete everything the test wrote, locked or not (governance
    /// bypassed: the lab's root credentials may).
    async fn cleanup(self) {
        for (key, version, _) in versions_under(&self.client, &self.bucket, &self.prefix).await {
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(&key)
                .version_id(&version)
                .bypass_governance_retention(true)
                .send()
                .await
                .unwrap_or_else(|e| panic!("cleaning up {key} version {version}: {e:?}"));
        }
        let left = versions_under(&self.client, &self.bucket, &self.prefix).await;
        assert!(left.is_empty(), "left behind: {left:?}");
    }
}

fn config(
    dir: &Path,
    state: &str,
    bucket: &str,
    prefix: &str,
    inputs: &[PathBuf],
    setup: &Setup,
) -> VtopConfig {
    let mut cfg = file_config(
        dir.join(format!("work-{state}")).to_str().unwrap(),
        &format!("sqlite://{}", dir.join(format!("{state}.db")).display()),
        inputs
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect(),
        "s3_native",
    );
    cfg.upload.bucket = bucket.into();
    cfg.upload.prefix = prefix.into();
    cfg.upload.endpoint_url = Some(endpoint());
    cfg.upload.force_path_style = true;
    cfg.upload.verify_tls = false;
    // The 0.4 manifest is the one that can name a key and a plan.
    cfg.manifest.write_version = ManifestWriteVersion::V0_4;
    cfg.manifest.verification_receipts = setup.receipts;
    cfg.upload.require_object_versioning = setup.versioning || setup.lock;
    if setup.lock {
        cfg.upload.object_lock = Some(ObjectLockConfig {
            mode: ObjectLockMode::Governance,
            retain_days: 1,
            legal_hold: false,
        });
    }
    if let Some((active, keys)) = &setup.mac {
        cfg.manifest.mac = Some(keyring(dir, active, keys, &[]));
    }
    cfg
}

fn mac_q4() -> Setup {
    Setup {
        mac: Some(("q4", vec!["q4"])),
        ..Setup::default()
    }
}

fn findings(report: &ScrubReport) -> Vec<(String, ScrubResult)> {
    report
        .findings
        .iter()
        .map(|f| (f.batch_id.clone(), f.result))
        .collect()
}

fn count(report: &ScrubReport, result: ScrubResult) -> u64 {
    report.results.get(result.as_str()).copied().unwrap_or(0)
}

fn detail_of<'a>(report: &'a ScrubReport, batch_id: &str) -> &'a str {
    &report
        .findings
        .iter()
        .find(|f| f.batch_id == batch_id)
        .unwrap_or_else(|| panic!("no finding for {batch_id}: {report:#?}"))
        .detail
}

// ---- 1. signed manifests ------------------------------------------------------------

/// (a) An attacker who can write the bucket and does not hold the MAC key
/// replaces an object with other bytes and writes a manifest describing them,
/// its digest recomputed. The MAC does not verify: the scrub reports the
/// batch unauthenticated, even when the attacker has also changed the ledger
/// to agree, and recovery replays a VERIFIED copy of the row instead of
/// committing it. A manifest with its MAC stripped fares no better.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_mac_on_a_replaced_object_with_a_recomputed_manifest_digest_is_unauthenticated_and_recovery_refuses(
) {
    let a = Archive::new("mac-replace", &plain_bucket(), 1, mac_q4()).await;
    let baseline = a.scrub(&ScrubOptions::default()).await;
    assert!(baseline.passed, "{baseline:#?}");
    assert_eq!(count(&baseline, ScrubResult::Ok), 1, "{baseline:#?}");
    assert_eq!(baseline.manifest_keys.get("q4"), Some(&1), "{baseline:#?}");

    let row = a.rows[0].clone();
    let original = a.get(&a.manifest_uri(0)).await;
    a.put(&a.object_uri(0), FORGED_OBJECT).await;
    let forged = forged_manifest_of(&original, None);
    a.put(&a.manifest_uri(0), &forged).await;

    // The attacker holds the bucket only: the ledger's digest disagrees too,
    // but the MAC is what is checked first.
    let report = a.scrub(&ScrubOptions::default()).await;
    assert!(!report.passed, "{report:#?}");
    assert_eq!(
        findings(&report),
        vec![(row.batch_id.clone(), ScrubResult::Unauthenticated)],
        "{report:#?}"
    );
    assert!(
        detail_of(&report, &row.batch_id).contains("MAC verification failed"),
        "{report:#?}"
    );

    // The attacker also rewrote the ledger so everything else agrees: only
    // the key tells.
    let rows = a
        .ledger_rows_edited(0, |r| forge_ledger_row(r, &forged))
        .await;
    let report = a
        .scrub_rows(&a.engine, rows, &ScrubOptions::default())
        .await;
    assert_eq!(
        findings(&report),
        vec![(row.batch_id.clone(), ScrubResult::Unauthenticated)],
        "{report:#?}"
    );
    assert!(
        detail_of(&report, &row.batch_id).contains("MAC verification failed"),
        "{report:#?}"
    );

    // The same, with the MAC removed instead of left stale.
    let stripped = forge(
        &original,
        "manifest",
        |doc| {
            doc["object"]["size_bytes"] = json!(FORGED_OBJECT.len());
            doc["object"]["checksum"] = json!(sha256_bytes(FORGED_OBJECT));
            doc["manifest"].as_object_mut().unwrap().remove("mac");
        },
        None,
    );
    a.put(&a.manifest_uri(0), &stripped).await;
    let rows = a
        .ledger_rows_edited(0, |r| forge_ledger_row(r, &stripped))
        .await;
    let report = a
        .scrub_rows(&a.engine, rows, &ScrubOptions::default())
        .await;
    assert_eq!(
        findings(&report),
        vec![(row.batch_id.clone(), ScrubResult::Unauthenticated)],
        "{report:#?}"
    );
    assert!(
        detail_of(&report, &row.batch_id).contains("MAC is required"),
        "{report:#?}"
    );

    // Recovery: a restart finds the batch VERIFIED and the ledger agreeing
    // with the forgery. It does not commit it; it hands the range back.
    a.put(&a.manifest_uri(0), &forged).await;
    let mut forged_row = row.clone();
    forge_ledger_row(&mut forged_row, &forged);
    let (summary, after) = a
        .recover_verified(&forged_row, a.config("state-b", None))
        .await;
    assert_eq!(summary.committed, 0, "{summary:?}");
    assert_ne!(after, BatchState::SourceCommitted, "{after:?}");
    assert_eq!(summary.replay_required, 1, "{summary:?}");
    a.cleanup().await;
}

/// (a) with the MAC OFF. The same replacement, by an attacker who holds the
/// bucket only, is still caught, but by the ledger's recorded digest, not by
/// the manifest. An attacker who also rewrites the ledger is not caught at
/// all: the manifest is internally consistent and describes the bytes that
/// are there, so the scrub passes and recovery commits. This is what the MAC
/// is for, and what it costs to leave it off.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_mac_off_a_replaced_object_with_a_recomputed_digest_is_caught_only_by_the_ledger() {
    let a = Archive::new("mac-off", &plain_bucket(), 1, Setup::default()).await;
    let baseline = a.scrub(&ScrubOptions::default()).await;
    assert!(baseline.passed, "{baseline:#?}");
    assert!(baseline.manifest_keys.is_empty(), "{baseline:#?}");

    let row = a.rows[0].clone();
    let original = a.get(&a.manifest_uri(0)).await;
    a.put(&a.object_uri(0), FORGED_OBJECT).await;
    let forged = forged_manifest_of(&original, None);
    a.put(&a.manifest_uri(0), &forged).await;

    // Bucket access only: the ledger still names the original digest.
    let report = a.scrub(&ScrubOptions::default()).await;
    assert_eq!(
        findings(&report),
        vec![(row.batch_id.clone(), ScrubResult::Unauthenticated)],
        "{report:#?}"
    );
    assert!(
        detail_of(&report, &row.batch_id).contains("the ledger recorded"),
        "caught by the ledger binding, not by a MAC: {report:#?}"
    );

    // Bucket and ledger: nothing is caught.
    let rows = a
        .ledger_rows_edited(0, |r| forge_ledger_row(r, &forged))
        .await;
    let report = a
        .scrub_rows(&a.engine, rows, &ScrubOptions::default())
        .await;
    assert!(
        report.passed,
        "NOT CAUGHT: with no MAC key a forgery that the ledger agrees with passes: {report:#?}"
    );
    assert_eq!(count(&report, ScrubResult::Ok), 1, "{report:#?}");

    let mut forged_row = row.clone();
    forge_ledger_row(&mut forged_row, &forged);
    let (summary, after) = a
        .recover_verified(&forged_row, a.config("state-b", None))
        .await;
    assert_eq!(
        summary.committed, 1,
        "NOT CAUGHT: recovery commits the forged batch: {summary:?}"
    );
    assert_eq!(after, BatchState::SourceCommitted, "{after:?}");
    a.cleanup().await;
}

/// The baseline under the signed-manifest cases, with no key at all: an
/// object replaced behind an honest manifest fails the checksum the manifest
/// and the ledger agree on, in the scrub and in recovery's storage re-check.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_a_replaced_object_behind_an_honest_manifest_is_mismatched_and_recovery_refuses() {
    let a = Archive::new("object-only", &plain_bucket(), 2, Setup::default()).await;
    let victim = a.rows[0].clone();
    // Same size, other bytes: a size check alone would not see it.
    let mut bytes = a.get(&a.object_uri(0)).await;
    bytes[0] ^= 0xff;
    a.put(&a.object_uri(0), &bytes).await;

    let report = a.scrub(&ScrubOptions::default()).await;
    assert_eq!(
        findings(&report),
        vec![(victim.batch_id.clone(), ScrubResult::Mismatched)],
        "{report:#?}"
    );
    assert_eq!(count(&report, ScrubResult::Ok), 1, "{report:#?}");

    let (summary, after) = a.recover_verified(&victim, a.config("state-b", None)).await;
    assert_eq!(summary.committed, 0, "{summary:?}");
    assert_ne!(after, BatchState::SourceCommitted, "{after:?}");
    assert_eq!(summary.replay_required, 1, "{summary:?}");
    a.cleanup().await;
}

/// (b) Another batch's valid manifest is copied over this batch's: it
/// authenticates (it was signed by the engine) and is not this batch's. The
/// scrub binds a manifest to its ledger row, so it is reported, with the MAC
/// on and off alike. The MAC is not what catches this one.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_a_swapped_in_valid_manifest_of_another_batch_is_unauthenticated() {
    for (case, setup) in [("swap-mac", mac_q4()), ("swap-nomac", Setup::default())] {
        let a = Archive::new(case, &plain_bucket(), 2, setup).await;
        let (victim, donor) = (a.rows[0].clone(), a.rows[1].clone());
        let donated = a.get(&a.manifest_uri(1)).await;
        a.put(&a.manifest_uri(0), &donated).await;

        let report = a.scrub(&ScrubOptions::default()).await;
        assert_eq!(
            findings(&report),
            vec![(victim.batch_id.clone(), ScrubResult::Unauthenticated)],
            "[{case}] {report:#?}"
        );
        assert!(
            detail_of(&report, &victim.batch_id).contains(&format!(
                "is batch {}'s, not {}",
                donor.batch_id, victim.batch_id
            )),
            "[{case}] {report:#?}"
        );
        assert_eq!(count(&report, ScrubResult::Ok), 1, "[{case}] {report:#?}");

        let (summary, _) = a.recover_verified(&victim, a.config("state-b", None)).await;
        assert_eq!(summary.committed, 0, "[{case}] {summary:?}");
        assert_eq!(summary.replay_required, 1, "[{case}] {summary:?}");
        a.cleanup().await;
    }
}

/// `vtopctl verify-manifest` on `uri`, with `engine`'s ledger and store.
async fn verify_manifest_on(
    engine: &Engine,
    uri: String,
    keyring: &vtop_core::manifest::ManifestKeyring,
) -> vtop_cli::commands::VerifyReport {
    vtop_cli::commands::verify_manifest_deep(
        engine.store.as_ref(),
        engine.backend.as_ref(),
        &uri,
        keyring,
    )
    .await
    .unwrap()
}

/// (b2) The same swap, checked by `vtopctl verify-manifest` on the victim's
/// key, with the archive's ledger and with a ledger that has never seen the
/// archive (an auditor holding only the bucket). The donated manifest is
/// genuine, so its self-hash, its MAC, its object and its ledger row all
/// check out; what fails is the key: the manifest names the key it was
/// written to, and it was not written to this one. Under the MAC that name is
/// authenticated. The donor's manifest at its own key still passes.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_verify_manifest_fails_another_batchs_valid_manifest_at_this_key() {
    for (case, setup) in [
        ("verify-swap-mac", mac_q4()),
        ("verify-swap-nomac", Setup::default()),
    ] {
        let a = Archive::new(case, &plain_bucket(), 2, setup).await;
        let donor = a.rows[1].clone();
        let (victim_uri, donor_uri) = (a.manifest_uri(0), a.manifest_uri(1));
        let auditor = Engine::new(
            a.config("state-auditor", None),
            StreamsConfig { streams: vec![] },
        )
        .await
        .unwrap();
        assert!(auditor.store.list_batches().await.unwrap().is_empty());
        let keyring = a.engine.config.resolve_manifest_keyring().unwrap();
        let verify = |ledger: &str, uri: String| {
            let engine = if ledger == "ledger" {
                &a.engine
            } else {
                &auditor
            };
            verify_manifest_on(engine, uri, &keyring)
        };
        for ledger in ["ledger", "no ledger"] {
            let report = verify(ledger, victim_uri.clone()).await;
            assert!(report.passed, "[{case}, {ledger}] {:#?}", report.lines);
        }

        let donated = a.get(&donor_uri).await;
        a.put(&victim_uri, &donated).await;

        for ledger in ["ledger", "no ledger"] {
            let report = verify(ledger, victim_uri.clone()).await;
            assert!(!report.passed, "[{case}, {ledger}] {:#?}", report.lines);
            assert_eq!(report.json["manifest_key_ok"], false, "[{case}, {ledger}]");
            assert_eq!(report.json["self_hash_ok"], true, "[{case}, {ledger}]");
            let line = report
                .lines
                .iter()
                .find(|l| l.starts_with("manifest key"))
                .unwrap();
            assert!(
                line.contains(&victim_uri)
                    && line.contains(&donor_uri)
                    && line.contains(&donor.batch_id),
                "[{case}, {ledger}] {line}"
            );
            if case == "verify-swap-mac" {
                assert!(
                    report.json["manifest_authentication"]
                        .as_str()
                        .unwrap()
                        .starts_with("OK"),
                    "[{case}, {ledger}] the MAC verifies: the manifest is genuine {:#?}",
                    report.json
                );
            }
            let genuine = verify(ledger, donor_uri.clone()).await;
            assert!(genuine.passed, "[{case}, {ledger}] {:#?}", genuine.lines);
        }
        drop(auditor);
        a.cleanup().await;
    }
}

/// (c) A forgery signed with a key the attacker made up, under a key id of
/// the attacker's choosing, valid in itself (its hash and MAC agree), and the
/// ledger rewritten to match. The reader uses the key a manifest names and
/// never tries another, so the id the engine does not hold is
/// unauthenticated, not corrupt. Recovery replays.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_a_manifest_signed_under_a_made_up_key_id_is_unauthenticated() {
    let a = Archive::new("made-up-key", &plain_bucket(), 1, mac_q4()).await;
    let row = a.rows[0].clone();
    let original = a.get(&a.manifest_uri(0)).await;
    a.put(&a.object_uri(0), FORGED_OBJECT).await;
    let forged = forged_manifest_of(&original, Some((ATTACKER_HEX, Some("attacker-q9"))));
    a.put(&a.manifest_uri(0), &forged).await;

    let rows = a
        .ledger_rows_edited(0, |r| forge_ledger_row(r, &forged))
        .await;
    let report = a
        .scrub_rows(&a.engine, rows, &ScrubOptions::default())
        .await;
    assert_eq!(
        findings(&report),
        vec![(row.batch_id.clone(), ScrubResult::Unauthenticated)],
        "{report:#?}"
    );
    let detail = detail_of(&report, &row.batch_id);
    assert!(
        detail.contains("attacker-q9") && detail.contains("which is not in this keyring"),
        "{detail}"
    );

    let mut forged_row = row.clone();
    forge_ledger_row(&mut forged_row, &forged);
    let (summary, _) = a
        .recover_verified(&forged_row, a.config("state-b", None))
        .await;
    assert_eq!(summary.committed, 0, "{summary:?}");
    assert_eq!(summary.replay_required, 1, "{summary:?}");
    a.cleanup().await;
}

/// (d) A key was withdrawn (listed in `manifest.mac.retired`): every manifest
/// it signed is reported as retired, by the scrub and by recovery, and so is
/// a forgery made with the leaked key. While the key was still trusted the
/// same forgery, with the ledger rewritten to agree, passed: a leaked key is
/// not protected until it is retired.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_a_manifest_signed_with_a_retired_key_is_reported_retired_and_a_leaked_key_is_not_caught_before(
) {
    let setup = Setup {
        mac: Some(("q3", vec!["q3"])),
        ..Setup::default()
    };
    let a = Archive::new("retired-key", &plain_bucket(), 1, setup).await;
    let row = a.rows[0].clone();
    assert!(a.scrub(&ScrubOptions::default()).await.passed);

    // The leaked key signs a forgery, and the ledger is made to agree. While
    // `q3` is trusted, nothing is caught.
    let original = a.get(&a.manifest_uri(0)).await;
    let original_object = a.get(&a.object_uri(0)).await;
    a.put(&a.object_uri(0), FORGED_OBJECT).await;
    let forged = forged_manifest_of(&original, Some((Q3_HEX, Some("q3"))));
    a.put(&a.manifest_uri(0), &forged).await;
    let rows = a
        .ledger_rows_edited(0, |r| forge_ledger_row(r, &forged))
        .await;
    let report = a
        .scrub_rows(&a.engine, rows, &ScrubOptions::default())
        .await;
    assert!(
        report.passed,
        "NOT CAUGHT: a forgery made with a key the engine still trusts passes: {report:#?}"
    );

    // The operator withdraws `q3` and signs with `q4` from now on.
    let rotated = a.config("state", Some(("q4", &["q4"], &["q3"])));
    let engine = Engine::new(rotated, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let rows = a
        .ledger_rows_edited(0, |r| forge_ledger_row(r, &forged))
        .await;
    let report = a.scrub_rows(&engine, rows, &ScrubOptions::default()).await;
    assert_eq!(
        findings(&report),
        vec![(row.batch_id.clone(), ScrubResult::Unauthenticated)],
        "{report:#?}"
    );
    assert!(
        detail_of(&report, &row.batch_id).contains("which is retired"),
        "{report:#?}"
    );

    // The genuine manifest, put back, is reported the same way: a retired key
    // authenticates nothing, by design.
    a.put(&a.manifest_uri(0), &original).await;
    a.put(&a.object_uri(0), &original_object).await;
    let report = a
        .scrub_rows(&engine, a.ledger_rows().await, &ScrubOptions::default())
        .await;
    assert_eq!(
        findings(&report),
        vec![(row.batch_id.clone(), ScrubResult::Unauthenticated)],
        "{report:#?}"
    );
    assert!(
        detail_of(&report, &row.batch_id).contains("which is retired"),
        "{report:#?}"
    );

    let (summary, _) = a
        .recover_verified(&row, a.config("state-b", Some(("q4", &["q4"], &["q3"]))))
        .await;
    assert_eq!(summary.committed, 0, "{summary:?}");
    assert_eq!(summary.replay_required, 1, "{summary:?}");
    a.cleanup().await;
}

// ---- 2. S3 Object Lock ----------------------------------------------------------------

impl Archive {
    /// The version id of the key's current version.
    async fn current_version(&self, uri: &str) -> String {
        self.client
            .head_object()
            .bucket(&self.bucket)
            .key(self.key_of(uri))
            .send()
            .await
            .unwrap_or_else(|e| panic!("HEAD {uri}: {e:?}"))
            .version_id()
            .unwrap_or_else(|| panic!("{uri} has no version id: is the bucket versioned?"))
            .to_owned()
    }

    /// The lock a version is under, as the store reports it: (mode, until).
    async fn lock_of(&self, uri: &str, version: &str) -> (Option<String>, Option<i64>) {
        let head = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(self.key_of(uri))
            .version_id(version)
            .send()
            .await
            .unwrap_or_else(|e| panic!("HEAD {uri} version {version}: {e:?}"));
        (
            head.object_lock_mode().map(|m| m.as_str().to_owned()),
            head.object_lock_retain_until_date().map(|d| d.secs()),
        )
    }
}

/// With the profile on, the store refuses to delete a locked version or to
/// shorten its retention, and VTOP's own check passes. What the store does
/// not refuse is shown too, and is reported by the scrub: an overwrite is a
/// new version and a plain DELETE a delete marker (the locked version is
/// untouched underneath, with its bytes and its lock), and a principal
/// allowed to bypass governance can remove the retention (the scrub reports
/// the object as having none). Compliance mode, which not even the account
/// root can bypass, is not exercised: its objects could not be removed from
/// the lab afterwards.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_object_lock_refuses_a_delete_of_a_locked_version_and_the_scrub_reports_what_it_allows(
) {
    use aws_sdk_s3::types::{ObjectLockRetention, ObjectLockRetentionMode};
    let setup = Setup {
        lock: true,
        ..Setup::default()
    };
    let a = Archive::new("lock", &locked_bucket(), 3, setup).await;
    let lock_opts = ScrubOptions {
        check_retention: Some(ObjectLockMode::Governance),
        ..ScrubOptions::default()
    };
    let locked = RetentionResult::Locked.as_str();

    // VTOP's own check passes: every object and manifest is under the
    // governance retention the engine asked for.
    let baseline = a.scrub(&lock_opts).await;
    assert!(baseline.passed, "{baseline:#?}");
    assert_eq!(baseline.retention.get(locked), Some(&3), "{baseline:#?}");

    // Batch 0: the store refuses a delete of the locked versions, of the
    // object and of the manifest, and a shortening of the retention.
    let (object0, manifest0) = (a.object_uri(0), a.manifest_uri(0));
    let object_version = a.current_version(&object0).await;
    let manifest_version = a.rows[0].manifest_version_id.clone().unwrap();
    for (uri, version) in [(&object0, &object_version), (&manifest0, &manifest_version)] {
        let refused = a
            .client
            .delete_object()
            .bucket(&a.bucket)
            .key(a.key_of(uri))
            .version_id(version)
            .send()
            .await;
        assert!(
            refused.is_err(),
            "the store deleted locked {uri}: {refused:?}"
        );
        let shortened = a
            .client
            .put_object_retention()
            .bucket(&a.bucket)
            .key(a.key_of(uri))
            .version_id(version)
            .retention(
                ObjectLockRetention::builder()
                    .mode(ObjectLockRetentionMode::Governance)
                    .retain_until_date(aws_sdk_s3::primitives::DateTime::from_secs(unix_now() + 60))
                    .build(),
            )
            .send()
            .await;
        assert!(
            shortened.is_err(),
            "the store shortened the retention of {uri}: {shortened:?}"
        );
        assert!(!a.get_version(uri, version).await.is_empty());
    }
    let report = a.scrub(&lock_opts).await;
    assert!(report.passed, "nothing changed: {report:#?}");
    assert_eq!(report.retention.get(locked), Some(&3), "{report:#?}");

    // Batch 1: an overwrite of the object and the manifest is accepted by
    // the store as a new version. The locked versions stay as they were.
    // Batch 2: a plain DELETE of the object is accepted as a delete marker.
    let (object1, manifest1, object2) = (a.object_uri(1), a.manifest_uri(1), a.object_uri(2));
    let kept = [
        (object1.clone(), a.current_version(&object1).await),
        (
            manifest1.clone(),
            a.rows[1].manifest_version_id.clone().unwrap(),
        ),
        (object2.clone(), a.current_version(&object2).await),
    ];
    let mut original = Vec::new();
    for (uri, version) in &kept {
        original.push(a.get_version(uri, version).await);
    }
    a.put(&object1, FORGED_OBJECT).await;
    a.put(
        &manifest1,
        &forged_manifest_of(&a.get(&manifest1).await, None),
    )
    .await;
    a.delete(&object2).await;
    for ((uri, version), bytes) in kept.iter().zip(&original) {
        assert_eq!(
            &a.get_version(uri, version).await,
            bytes,
            "the locked version of {uri} keeps its bytes"
        );
        let (mode, until) = a.lock_of(uri, version).await;
        assert_eq!(mode.as_deref(), Some("GOVERNANCE"), "{uri}");
        assert!(until.unwrap() > unix_now() + 3_600, "{uri}: {until:?}");
    }
    let report = a.scrub(&lock_opts).await;
    assert!(!report.passed, "{report:#?}");
    let mut got = findings(&report);
    got.sort();
    let mut want = vec![
        (a.rows[1].batch_id.clone(), ScrubResult::Mismatched),
        (a.rows[2].batch_id.clone(), ScrubResult::Missing),
    ];
    want.sort();
    assert_eq!(got, want, "{report:#?}");

    // Batch 0 again: a principal allowed to bypass governance (the lab's
    // root) clears the retention. The store allows it; the scrub reports it.
    a.client
        .put_object_retention()
        .bucket(&a.bucket)
        .key(a.key_of(&object0))
        .version_id(&object_version)
        .bypass_governance_retention(true)
        .retention(ObjectLockRetention::builder().build())
        .send()
        .await
        .expect("the lab's root credentials may clear a governance retention");
    let report = a.scrub(&lock_opts).await;
    let batch0 = report
        .findings
        .iter()
        .find(|f| f.batch_id == a.rows[0].batch_id)
        .unwrap_or_else(|| panic!("the cleared retention is a finding: {report:#?}"));
    assert_eq!(
        batch0.retention,
        Some(RetentionResult::Missing),
        "{report:#?}"
    );
    assert_eq!(
        report.retention.get(RetentionResult::Missing.as_str()),
        Some(&1),
        "{report:#?}"
    );
    a.cleanup().await;
}

// ---- 3. bucket versioning ----------------------------------------------------------------

/// With `upload.require_object_versioning`, an overwrite of a manifest or an
/// object is a new version, and the ledger pins the version the engine
/// wrote. The scrub reads the pinned manifest, then the current key, and
/// reports a key that no longer holds it; an object overwritten behind its
/// manifest is a mismatch, and the original version is still in the store.
/// The control is an unversioned bucket: the same overwrite leaves one
/// version, the forgery, and the original bytes are gone.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_a_versioned_overwrite_is_a_new_version_and_the_scrub_reports_the_replacement() {
    ensure_bucket(&s3_client().await, VERSIONED_BUCKET, true).await;
    let setup = Setup {
        versioning: true,
        ..Setup::default()
    };
    let a = Archive::new("versioned", VERSIONED_BUCKET, 2, setup).await;
    for row in &a.rows {
        assert!(
            row.manifest_version_id.is_some(),
            "the ledger pins the manifest version: {row:?}"
        );
    }
    let baseline = a.scrub(&ScrubOptions::default()).await;
    assert!(baseline.passed, "{baseline:#?}");

    // Batch 0: the manifest is overwritten.
    let manifest0 = a.manifest_uri(0);
    let pinned = a.rows[0].manifest_version_id.clone().unwrap();
    let original = a.get(&manifest0).await;
    let forged = forge(
        &original,
        "manifest",
        |doc| doc["record_count"] = json!(999),
        None,
    );
    a.put(&manifest0, &forged).await;
    // Batch 1: the object is overwritten, behind a manifest nobody touched.
    let object1 = a.object_uri(1);
    let object_version = a.current_version(&object1).await;
    let object_bytes = a.get(&object1).await;
    a.put(&object1, FORGED_OBJECT).await;

    let report = a.scrub(&ScrubOptions::default()).await;
    let mut got = findings(&report);
    got.sort();
    let mut want = vec![
        (a.rows[0].batch_id.clone(), ScrubResult::Mismatched),
        (a.rows[1].batch_id.clone(), ScrubResult::Mismatched),
    ];
    want.sort();
    assert_eq!(got, want, "{report:#?}");
    assert!(
        detail_of(&report, &a.rows[0].batch_id)
            .contains(&format!("no longer holds version {pinned}")),
        "{report:#?}"
    );

    // Both overwrites are new versions; the originals are still there.
    let key0 = a.key_of(&manifest0);
    let versions = versions_under(&a.client, &a.bucket, &key0).await;
    assert_eq!(versions.len(), 2, "{versions:?}");
    assert_eq!(a.get_version(&manifest0, &pinned).await, original);
    assert_eq!(
        a.get(&manifest0).await,
        forged,
        "the current key is the forgery"
    );
    assert_eq!(a.get_version(&object1, &object_version).await, object_bytes);
    assert_eq!(a.get(&object1).await, FORGED_OBJECT);
    a.cleanup().await;

    // Control, no versioning: the overwrite replaces the bytes for good.
    let b = Archive::new("unversioned", &plain_bucket(), 1, Setup::default()).await;
    assert!(
        b.rows[0].manifest_version_id.is_none(),
        "no version to pin: {:?}",
        b.rows[0]
    );
    let object = b.object_uri(0);
    b.put(&object, FORGED_OBJECT).await;
    let versions = versions_under(&b.client, &b.bucket, &b.key_of(&object)).await;
    assert_eq!(
        versions.len(),
        1,
        "NOT RECOVERABLE: nothing keeps the original bytes: {versions:?}"
    );
    assert_eq!(b.get(&object).await, FORGED_OBJECT);
    b.cleanup().await;
}

/// The bucket's versioning is itself something to tamper with: suspended
/// after the engine started, the next engine start refuses the bucket, and an
/// engine that is already running fails the batch instead of writing it
/// under no pinned version. With `upload.require_object_versioning` off, the
/// same bucket is accepted and a batch is written.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_suspended_versioning_makes_the_engine_refuse_the_bucket() {
    let client = s3_client().await;
    ensure_bucket(&client, SUSPENDED_BUCKET, true).await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    std::fs::write(&input, "tamper suspended record\n").unwrap();
    let inputs = vec![input];
    let prefix = format!("vtop-it-tamper-suspended-{}", nanos());
    let setup = Setup {
        versioning: true,
        ..Setup::default()
    };
    let cfg = |setup: &Setup, state: &str| {
        config(dir.path(), state, SUSPENDED_BUCKET, &prefix, &inputs, setup)
    };
    let streams = || StreamsConfig { streams: vec![] };
    drop(
        Engine::new_exclusive(cfg(&setup, "before"), streams())
            .await
            .expect("a versioned bucket is accepted"),
    );
    // Running before the tampering: its first upload will see the bucket as
    // it is by then.
    let mut running = Engine::new(cfg(&setup, "running"), streams())
        .await
        .unwrap();

    set_versioning(&client, SUSPENDED_BUCKET, BucketVersioningStatus::Suspended).await;
    let refused = match Engine::new_exclusive(cfg(&setup, "after"), streams()).await {
        Ok(_) => panic!("an engine must not start against a bucket whose versioning was suspended"),
        Err(e) => e.to_string(),
    };
    assert!(
        refused.contains(&format!(
            "bucket {SUSPENDED_BUCKET} does not have versioning enabled"
        )),
        "{refused}"
    );
    let out = running.process_once(SourceType::File).await.unwrap();
    assert!(
        out.iter().all(|o| !o.committed),
        "a batch must not commit into a bucket that keeps no versions: {out:?}"
    );
    assert!(
        versions_under(&client, SUSPENDED_BUCKET, &prefix)
            .await
            .is_empty(),
        "nothing was written"
    );

    // Control, the setting off: the same bucket is accepted and written to.
    let mut off = Engine::new_exclusive(cfg(&Setup::default(), "off"), streams())
        .await
        .expect("NOT CAUGHT: without the setting a bucket with versioning suspended is accepted");
    let out = off.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    set_versioning(&client, SUSPENDED_BUCKET, BucketVersioningStatus::Enabled).await;
    for (key, version, _) in versions_under(&client, SUSPENDED_BUCKET, &prefix).await {
        client
            .delete_object()
            .bucket(SUSPENDED_BUCKET)
            .key(&key)
            .version_id(&version)
            .send()
            .await
            .unwrap();
    }
}

/// The external tamper evaluation of v0.10.2: versioning suspended under an
/// engine that had checked the bucket at startup and was archiving. Every
/// retry used to put one more manifest, each failing "no object version to
/// pin": 10 objects in 25 s, nothing committed. Now the batch in flight
/// writes its object, its manifest is not written, and nothing more is
/// written however often the range is retried, including at a check that
/// still finds versioning suspended. Once versioning is enabled again the
/// engine resumes on its own and the range commits.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_suspended_versioning_under_a_running_engine_writes_one_object_and_resumes() {
    let client = s3_client().await;
    ensure_bucket(&client, SUSPENDED_RUNNING_BUCKET, true).await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    std::fs::write(&input, "tamper suspended-running record 1\n").unwrap();
    let inputs = vec![input.clone()];
    let prefix = format!("vtop-it-tamper-suspended-running-{}", nanos());
    let setup = Setup {
        versioning: true,
        ..Setup::default()
    };
    let cfg = config(
        dir.path(),
        "running",
        SUSPENDED_RUNNING_BUCKET,
        &prefix,
        &inputs,
        &setup,
    );
    // Started with the bucket versioned: the startup check passes, as it
    // did for the evaluated engine.
    let mut engine = Engine::new_exclusive(cfg, StreamsConfig { streams: vec![] })
        .await
        .expect("a versioned bucket is accepted");
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(
        out.iter().all(|o| o.committed) && !out.is_empty(),
        "{out:?}"
    );
    let stored = |client: &aws_sdk_s3::Client| {
        let (client, prefix) = (client.clone(), prefix.clone());
        async move { versions_under(&client, SUSPENDED_RUNNING_BUCKET, &prefix).await }
    };
    let committed = stored(&client).await;
    assert_eq!(
        committed.len(),
        2,
        "one object and one manifest: {committed:?}"
    );

    set_versioning(
        &client,
        SUSPENDED_RUNNING_BUCKET,
        BucketVersioningStatus::Suspended,
    )
    .await;
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&input)
        .unwrap();
    writeln!(f, "tamper suspended-running record 2").unwrap();
    drop(f);
    for retry in 0..10 {
        let out = engine.process_once(SourceType::File).await.unwrap();
        assert!(
            out.iter().all(|o| !o.committed),
            "retry {retry}: nothing commits into a bucket that keeps no versions: {out:?}"
        );
    }
    let after = stored(&client).await;
    let written: Vec<_> = after.iter().filter(|v| !committed.contains(v)).collect();
    assert_eq!(
        written.len(),
        1,
        "the batch in flight wrote its object and no manifest, and ten retries wrote \
         nothing: {written:?}"
    );
    assert!(!written[0].0.ends_with(".manifest.json"), "{written:?}");

    // Past the first check (5 s): versioning is still suspended, so the
    // check writes nothing either.
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| !o.committed), "{out:?}");
    assert_eq!(stored(&client).await, after, "a check writes nothing");

    // Enabled again: past the next check (10 s), the engine resumes and the
    // range commits.
    set_versioning(
        &client,
        SUSPENDED_RUNNING_BUCKET,
        BucketVersioningStatus::Enabled,
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_secs(11)).await;
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(
        out.iter().all(|o| o.committed) && !out.is_empty(),
        "the engine resumed: {out:?}"
    );
    drop(engine);
    for (key, version, _) in stored(&client).await {
        client
            .delete_object()
            .bucket(SUSPENDED_RUNNING_BUCKET)
            .key(&key)
            .version_id(&version)
            .send()
            .await
            .unwrap();
    }
}

// ---- 4. verification receipts -------------------------------------------------------------

fn receipt_opts(require: bool) -> ScrubOptions {
    ScrubOptions {
        check_receipts: true,
        require_receipts: require,
        ..ScrubOptions::default()
    }
}

fn receipts(report: &ScrubReport, result: ReceiptResult) -> u64 {
    report.receipts.get(result.as_str()).copied().unwrap_or(0)
}

/// With the MAC on, a receipt altered without the key is `invalid`, whether
/// its hash was recomputed (the MAC does not verify) or not (the hash does
/// not); another batch's genuine receipt in its place is `mismatched`; a
/// deleted receipt is `missing`, a finding only under `--require-receipts`.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_receipts_with_the_mac_on_an_altered_swapped_or_deleted_receipt_is_reported() {
    let setup = Setup {
        receipts: true,
        ..mac_q4()
    };
    let a = Archive::new("receipt-mac", &plain_bucket(), 4, setup).await;
    let baseline = a.scrub(&receipt_opts(true)).await;
    assert!(baseline.passed, "{baseline:#?}");
    assert_eq!(receipts(&baseline, ReceiptResult::Ok), 4, "{baseline:#?}");

    let r = |i| a.receipt_uri(i);
    // 0: class changed, hash recomputed, MAC stale.
    let original0 = a.get(&r(0)).await;
    let recomputed = forge(
        &original0,
        "receipt",
        |doc| doc["verification"]["class"] = json!("size_only"),
        None,
    );
    a.put(&r(0), &recomputed).await;
    // 1: class changed in the bytes, nothing recomputed.
    let text = String::from_utf8(a.get(&r(1)).await).unwrap();
    assert!(text.contains("\"strong\""), "{text}");
    a.put(
        &r(1),
        text.replace("\"strong\"", "\"size_only\"").as_bytes(),
    )
    .await;
    // 2: batch 3's genuine receipt; 3: deleted.
    let donor = a.get(&r(3)).await;
    a.put(&r(2), &donor).await;
    a.delete(&r(3)).await;

    let report = a.scrub(&receipt_opts(true)).await;
    assert!(!report.passed, "{report:#?}");
    assert_eq!(receipts(&report, ReceiptResult::Invalid), 2, "{report:#?}");
    assert_eq!(
        receipts(&report, ReceiptResult::Mismatched),
        1,
        "{report:#?}"
    );
    assert_eq!(receipts(&report, ReceiptResult::Missing), 1, "{report:#?}");
    assert_eq!(receipts(&report, ReceiptResult::Ok), 0, "{report:#?}");
    let by_batch = |i: usize| {
        report
            .findings
            .iter()
            .find(|f| f.batch_id == a.rows[i].batch_id)
            .unwrap_or_else(|| panic!("no finding for batch {i}: {report:#?}"))
    };
    assert_eq!(by_batch(0).receipt, Some(ReceiptResult::Invalid));
    assert!(
        by_batch(0).detail.contains("MAC verification failed"),
        "{report:#?}"
    );
    assert_eq!(by_batch(1).receipt, Some(ReceiptResult::Invalid));
    assert_eq!(by_batch(2).receipt, Some(ReceiptResult::Mismatched));
    assert_eq!(by_batch(3).receipt, Some(ReceiptResult::Missing));
    // The batches themselves are untouched: the findings are about evidence.
    assert_eq!(count(&report, ScrubResult::Ok), 4, "{report:#?}");

    // Without --require-receipts a deleted receipt is counted, not a finding.
    let lenient = a.scrub(&receipt_opts(false)).await;
    assert_eq!(
        receipts(&lenient, ReceiptResult::Missing),
        1,
        "{lenient:#?}"
    );
    assert_eq!(lenient.findings.len(), 3, "{lenient:#?}");
    a.cleanup().await;
}

/// With the MAC off, a receipt edited with its hash recomputed is caught
/// only where the ledger holds the fact it changed: the verification class
/// is the ledger's, so that edit is `mismatched`. A field the ledger does
/// not hold (who verified, the method) can be edited and the receipt still
/// passes.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_receipts_with_the_mac_off_only_what_the_ledger_holds_is_checked() {
    let setup = Setup {
        receipts: true,
        ..Setup::default()
    };
    let a = Archive::new("receipt-nomac", &plain_bucket(), 2, setup).await;
    let baseline = a.scrub(&receipt_opts(true)).await;
    assert!(baseline.passed, "{baseline:#?}");

    let original0 = a.get(&a.receipt_uri(0)).await;
    a.put(
        &a.receipt_uri(0),
        &forge(
            &original0,
            "receipt",
            |doc| doc["verification"]["class"] = json!("size_only"),
            None,
        ),
    )
    .await;
    let original1 = a.get(&a.receipt_uri(1)).await;
    a.put(
        &a.receipt_uri(1),
        &forge(
            &original1,
            "receipt",
            |doc| {
                doc["verified_by"] = json!("someone-else");
                doc["verification"]["method"] = json!("read_back");
            },
            None,
        ),
    )
    .await;

    let report = a.scrub(&receipt_opts(true)).await;
    assert_eq!(
        receipts(&report, ReceiptResult::Mismatched),
        1,
        "{report:#?}"
    );
    assert_eq!(
        receipts(&report, ReceiptResult::Ok),
        1,
        "NOT CAUGHT: an edit to a field the ledger does not hold passes: {report:#?}"
    );
    assert_eq!(report.findings.len(), 1, "{report:#?}");
    assert_eq!(report.findings[0].batch_id, a.rows[0].batch_id);
    a.cleanup().await;
}

// ---- 5. the audit of coverage -----------------------------------------------------------------

/// `audit-coverage --check-storage` asks the store about each committed
/// object with a HEAD: present, and of the recorded size. It reports an
/// object deleted behind the ledger and one replaced by another size. A
/// replacement of the same size is not reported (a HEAD cannot prove
/// content), which is the scrub's job, and it does.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint"]
async fn live_check_storage_sees_a_deleted_or_resized_object_not_a_same_size_replacement() {
    let a = Archive::new("coverage", &plain_bucket(), 3, Setup::default()).await;
    let records = a.rows.clone();
    let clean = vtop_cli::coverage::check_storage(a.engine.backend.as_ref(), &records).await;
    assert!(clean.is_empty(), "{clean:?}");

    a.delete(&a.object_uri(0)).await;
    let mut same_size = a.get(&a.object_uri(1)).await;
    same_size[0] ^= 0xff;
    a.put(&a.object_uri(1), &same_size).await;
    a.put(&a.object_uri(2), FORGED_OBJECT).await;

    let found = vtop_cli::coverage::check_storage(a.engine.backend.as_ref(), &records).await;
    let mut batches: Vec<&str> = found.iter().map(|f| f.batch_id.as_str()).collect();
    batches.sort();
    let mut want = vec![a.rows[0].batch_id.as_str(), a.rows[2].batch_id.as_str()];
    want.sort();
    assert_eq!(
        batches, want,
        "NOT CAUGHT: the same-size replacement of batch 1 is invisible to a HEAD: {found:#?}"
    );
    let for_batch = |i: usize| {
        found
            .iter()
            .find(|f| f.batch_id == a.rows[i].batch_id)
            .unwrap()
    };
    assert!(for_batch(0).problem.contains("not readable"), "{found:#?}");
    assert!(
        for_batch(2)
            .problem
            .contains("bytes stored, the ledger recorded"),
        "{found:#?}"
    );

    // The scrub reads the bytes: it reports all three.
    let report = a.scrub(&ScrubOptions::default()).await;
    assert_eq!(report.findings.len(), 3, "{report:#?}");
    assert_eq!(count(&report, ScrubResult::Missing), 1, "{report:#?}");
    assert_eq!(count(&report, ScrubResult::Mismatched), 2, "{report:#?}");
    a.cleanup().await;
}
