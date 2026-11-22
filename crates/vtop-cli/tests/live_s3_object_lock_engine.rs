//! #684 end to end against a real S3-compatible store with Object Lock: the
//! engine, started the way `vtopctl run` starts it (`Engine::new_exclusive`),
//! with the s3_native backend and `upload.object_lock` set.
//!
//! - against a bucket created without Object Lock it refuses to start, and
//!   says what to do;
//! - against a locked bucket it starts, archives a batch to
//!   SOURCE_COMMITTED, and the object, the manifest and the verification
//!   receipt (#671) it wrote are under the configured mode until a date about
//!   `retain_days` away, read back from the store behind the engine's back;
//! - a replay that resolves to an object already archived under its derived
//!   key whose lock has ended (#709) writes the batch as a fresh locked
//!   object under its attempt key and commits, and the old version keeps its
//!   bytes and its (ended) retention;
//! - `vtopctl scrub --check-retention`, with receipts checked, reads the
//!   batch's and the receipt's retention from the store and finds both
//!   locked; once the receipt's retention is cleared (by a principal allowed
//!   to bypass governance), it reports the receipt, and only the receipt,
//!   as a `missing` retention finding (#710);
//! - `vtopctl tier copy` with the profile refuses a bucket without Object
//!   Lock before writing, and against a locked bucket writes the segment and
//!   its manifest under the lock and checks both before the evidence.
//!
//! Run against the compose MinIO lab with:
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin
//!  AWS_SECRET_ACCESS_KEY=minioadmin cargo test -p vtop-cli
//!  --test live_s3_object_lock_engine -- --ignored --test-threads=1`
//! The objects it writes are under a one-day governance retention; the test
//! deletes them with `x-amz-bypass-governance-retention`, which the lab's
//! root credentials may use. CI runs it in the `minio-integration` job.
//! `VTOP_TEST_S3_SERVER` names the server and its capability file (#722).
#![cfg(unix)]

use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use vtop_cli::scrub::{RetentionResult, ScrubOptions};
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::{ObjectLockConfig, ObjectLockMode, StreamsConfig, VtopConfig};
use vtop_core::types::SourceType;
use vtop_s3_testkit::{Capability, LiveS3, S3Settings};

/// How to reach the store, from `VTOP_TEST_S3_*` (#722), read once.
fn settings() -> S3Settings {
    static SETTINGS: std::sync::OnceLock<S3Settings> = std::sync::OnceLock::new();
    SETTINGS
        .get_or_init(|| LiveS3::from_env().settings.clone())
        .clone()
}

fn locked_bucket() -> String {
    settings().locked_bucket
}

fn plain_bucket() -> String {
    settings().bucket
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn config(dir: &Path, bucket: &str, prefix: &str) -> VtopConfig {
    config_named(dir, bucket, prefix, "state")
}

/// [`config`] with its own ledger and work dir named `state`, over the same
/// input file: a second engine over it reads the same window, so its batch
/// has the same bytes and resolves to the same derived key (#626).
fn config_named(dir: &Path, bucket: &str, prefix: &str, state: &str) -> VtopConfig {
    let input = dir.join("app.log");
    if !input.exists() {
        let mut f = std::fs::File::create(&input).unwrap();
        for i in 0..5 {
            writeln!(f, "locked record {i}").unwrap();
        }
    }
    let mut cfg = file_config(
        dir.join(format!("work-{state}")).to_str().unwrap(),
        &format!("sqlite://{}", dir.join(format!("{state}.db")).display()),
        vec![input.to_string_lossy().into_owned()],
        "s3_native",
    );
    cfg.upload.bucket = bucket.into();
    cfg.upload.prefix = prefix.into();
    let s = settings();
    cfg.upload.endpoint_url = s.endpoint;
    cfg.upload.region = s.region;
    cfg.upload.force_path_style = s.force_path_style;
    cfg.upload.verify_tls = s.verify_tls;
    cfg.upload.require_object_versioning = true;
    cfg.upload.object_lock = Some(ObjectLockConfig {
        mode: ObjectLockMode::Governance,
        retain_days: 1,
        legal_hold: false,
    });
    // The receipt goes through the manifest write path, so it is locked too.
    cfg.manifest.verification_receipts = true;
    cfg
}

async fn s3_client() -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    let env = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"));
    let s = settings();
    let mut conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(s.region))
        .force_path_style(s.force_path_style);
    if let Some(endpoint) = s.endpoint {
        conf = conf.endpoint_url(endpoint);
    }
    let conf = conf
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

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_the_engine_refuses_a_bucket_without_object_lock_at_startup() {
    let live = LiveS3::from_env();
    live.case(
        "engine.object_lock.refuses_a_plain_bucket_at_startup",
        &[Capability::ObjectLock],
        || async {
            let dir = tempfile::tempdir().unwrap();
            let cfg = config(dir.path(), &plain_bucket(), "vtop-it-684-engine");
            let err = match Engine::new_exclusive(cfg, StreamsConfig { streams: vec![] }).await {
                Ok(_) => panic!("an engine must not start against a bucket without Object Lock"),
                Err(e) => e.to_string(),
            };
            for part in [
                format!(
                    "bucket {} does not have Object Lock enabled",
                    plain_bucket()
                ),
                "the engine did not start".to_owned(),
                "point upload.bucket at it".to_owned(),
            ] {
                assert!(err.contains(&part), "{err:?} should say {part:?}");
            }
        },
    )
    .await;
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_the_engine_archives_a_batch_under_the_configured_lock() {
    let live = LiveS3::from_env();
    live.case(
        "engine.object_lock.archives_a_batch_locked",
        &[Capability::Versioning, Capability::ObjectLock],
        || async {
            let dir = tempfile::tempdir().unwrap();
            let bucket = locked_bucket();
            let prefix = format!("vtop-it-684-engine-{}", unix_now());
            let mut engine = Engine::new_exclusive(
                config(dir.path(), &bucket, &prefix),
                StreamsConfig { streams: vec![] },
            )
            .await
            .expect("a locked bucket passes the startup check");
            let before = unix_now();
            let out = engine.process_once(SourceType::File).await.unwrap();
            assert_eq!(out.len(), 1, "{out:?}");
            assert!(out[0].committed, "{out:?}");
            let row = engine
                .store
                .get_batch(&out[0].batch_id)
                .await
                .unwrap()
                .unwrap();

            // The scrub reads the retention back through the backend (#710): the
            // batch's object and manifest, and the receipt at its key.
            let rows = vtop_cli::scrub::committed_rows(
                engine.store.list_batches().await.unwrap(),
                Vec::new(),
            );
            let report = engine
                .scrub(
                    rows,
                    &ScrubOptions {
                        check_retention: Some(ObjectLockMode::Governance),
                        check_receipts: true,
                        ..ScrubOptions::default()
                    },
                )
                .await
                .unwrap();
            assert!(report.passed, "{report:#?}");
            let locked = RetentionResult::Locked.as_str();
            assert_eq!(report.retention.get(locked), Some(&1), "{report:#?}");
            assert_eq!(
                report.receipt_retention.get(locked),
                Some(&1),
                "{report:#?}"
            );

            let client = s3_client().await;
            let receipt =
                vtop_core::partitioning::receipt_uri(row.manifest_uri.as_deref().unwrap()).unwrap();
            let key_of = |uri: &str| {
                uri.strip_prefix(&format!("s3://{bucket}/"))
                    .unwrap()
                    .to_owned()
            };
            let mut written = Vec::new();
            for uri in [
                row.object_uri.clone().unwrap(),
                row.manifest_uri.clone().unwrap(),
                receipt.clone(),
            ] {
                let key = key_of(&uri);
                let head = client
                    .head_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(
                    head.object_lock_mode().map(|m| m.as_str()),
                    Some("GOVERNANCE"),
                    "{uri}"
                );
                let until = head.object_lock_retain_until_date().unwrap().secs();
                assert!(
                    until >= before + 86_400 && until <= unix_now() + 86_400 + 1,
                    "{uri} is retained one day from its write: {until}"
                );
                written.push((uri, key, head.version_id().unwrap().to_owned()));
            }

            // Clear the receipt's retention, as a principal allowed to bypass
            // governance can (#710): the scrub reports the receipt, not the batch.
            client
                .put_object_retention()
                .bucket(&bucket)
                .key(key_of(&receipt))
                .bypass_governance_retention(true)
                .retention(aws_sdk_s3::types::ObjectLockRetention::builder().build())
                .send()
                .await
                .expect("the lab's root credentials may clear a governance retention");
            let rows = vtop_cli::scrub::committed_rows(
                engine.store.list_batches().await.unwrap(),
                Vec::new(),
            );
            let report = engine
                .scrub(
                    rows,
                    &ScrubOptions {
                        check_retention: Some(ObjectLockMode::Governance),
                        check_receipts: true,
                        ..ScrubOptions::default()
                    },
                )
                .await
                .unwrap();
            assert!(!report.passed, "{report:#?}");
            assert_eq!(report.retention.get(locked), Some(&1), "{report:#?}");
            assert_eq!(
                report
                    .receipt_retention
                    .get(RetentionResult::Missing.as_str()),
                Some(&1),
                "{report:#?}"
            );
            assert_eq!(report.findings.len(), 1, "{report:#?}");
            assert_eq!(
                report.findings[0].receipt_retention,
                Some(RetentionResult::Missing),
                "{report:#?}"
            );

            // Clean up: the lab's root credentials may bypass governance.
            for (uri, key, version) in written {
                client
                    .delete_object()
                    .bucket(&bucket)
                    .key(&key)
                    .version_id(&version)
                    .bypass_governance_retention(true)
                    .send()
                    .await
                    .unwrap_or_else(|e| panic!("cleaning up {uri}: {e:?}"));
            }
        },
    )
    .await;
}

/// #709 against a real store: the retention of the object a first engine
/// archived is shortened (governance, bypassed with the lab's root
/// credentials) to end two seconds later. A second engine over the same
/// input resolves to that object under its derived key, finds its lock
/// ended, and writes a fresh object, locked for a day, under its attempt key
/// instead; it commits, and the old version is left with its bytes and its
/// ended retention.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_a_replay_over_an_ended_lock_commits_a_fresh_locked_object() {
    let live = LiveS3::from_env();
    live.case(
        "engine.object_lock.a_replay_over_an_ended_lock_commits_a_fresh_object",
        &[Capability::Versioning, Capability::ObjectLock],
        || async {
            use aws_sdk_s3::types::{ObjectLockRetention, ObjectLockRetentionMode};
            let dir = tempfile::tempdir().unwrap();
            let bucket = locked_bucket();
            let prefix = format!("vtop-it-709-engine-{}", unix_now());
            let client = s3_client().await;
            let key_of = |uri: &str| {
                uri.strip_prefix(&format!("s3://{bucket}/"))
                    .unwrap()
                    .to_owned()
            };
            // No time in the path, so both engines resolve to one prefix whatever
            // the hour.
            let template = "tenant={tenant}/source={source}/format={format}/";

            let mut cfg = config_named(dir.path(), &bucket, &prefix, "first");
            cfg.partitioning.template = template.into();
            let mut first = Engine::new_exclusive(cfg, StreamsConfig { streams: vec![] })
                .await
                .unwrap();
            let out = first.process_once(SourceType::File).await.unwrap();
            assert!(out[0].committed, "{out:?}");
            let derived = out[0].object_uri.clone().unwrap();
            let derived_key = key_of(&derived);
            let head = client
                .head_object()
                .bucket(&bucket)
                .key(&derived_key)
                .send()
                .await
                .unwrap();
            let version = head.version_id().unwrap().to_owned();
            let ends = unix_now() + 2;
            client
                .put_object_retention()
                .bucket(&bucket)
                .key(&derived_key)
                .version_id(&version)
                .bypass_governance_retention(true)
                .retention(
                    ObjectLockRetention::builder()
                        .mode(ObjectLockRetentionMode::Governance)
                        .retain_until_date(aws_sdk_s3::primitives::DateTime::from_secs(ends))
                        .build(),
                )
                .send()
                .await
                .unwrap_or_else(|e| panic!("shortening the retention of {derived}: {e:?}"));
            drop(first);
            while unix_now() <= ends + 1 {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }

            let mut cfg = config_named(dir.path(), &bucket, &prefix, "second");
            cfg.partitioning.template = template.into();
            let mut second = Engine::new_exclusive(cfg, StreamsConfig { streams: vec![] })
                .await
                .unwrap();
            let before = unix_now();
            let out = second.process_once(SourceType::File).await.unwrap();
            assert_eq!(out.len(), 1, "{out:?}");
            assert!(out[0].committed, "the replay commits: {out:?}");
            let landed = out[0].object_uri.clone().unwrap();
            assert_ne!(landed, derived, "the ended lock was not reused");
            assert!(
                landed.contains(&out[0].batch_id),
                "{landed} is the attempt key"
            );

            let fresh = client
                .head_object()
                .bucket(&bucket)
                .key(key_of(&landed))
                .send()
                .await
                .unwrap();
            assert_eq!(
                fresh.object_lock_mode().map(|m| m.as_str()),
                Some("GOVERNANCE")
            );
            let until = fresh.object_lock_retain_until_date().unwrap().secs();
            assert!(
                until >= before + 86_400 && until <= unix_now() + 86_400 + 1,
                "{landed} is retained one day from its write: {until}"
            );
            let old = client
                .head_object()
                .bucket(&bucket)
                .key(&derived_key)
                .send()
                .await
                .unwrap();
            assert_eq!(
                old.version_id(),
                Some(version.as_str()),
                "{derived} is untouched"
            );
            assert_eq!(
                old.object_lock_retain_until_date().map(|d| d.secs()),
                Some(ends),
                "{derived} kept its ended retention"
            );

            // Clean up every version under the prefix; the lab's root credentials
            // may bypass governance.
            let listed = client
                .list_object_versions()
                .bucket(&bucket)
                .prefix(&prefix)
                .send()
                .await
                .unwrap();
            assert_eq!(
                listed
                    .versions()
                    .iter()
                    .filter(|v| v.key() == Some(derived_key.as_str()))
                    .count(),
                1,
                "nothing was written over {derived}"
            );
            for v in listed.versions() {
                client
                    .delete_object()
                    .bucket(&bucket)
                    .key(v.key().unwrap())
                    .version_id(v.version_id().unwrap())
                    .bypass_governance_retention(true)
                    .send()
                    .await
                    .unwrap_or_else(|e| panic!("cleaning up {:?}: {e:?}", v.key()));
            }
        },
    )
    .await;
}

/// A sealed segment and its manifest in `dir`, as a data node leaves them
/// for `vtopctl tier copy`; and the chunk-tree root the copy is pinned to.
fn seal_segment(dir: &Path) -> (std::path::PathBuf, blake3::Hash) {
    use vtop_log::{
        ActiveSegment, Durability, LogRecord, RangeLineage, SegmentConfigV2, SegmentDescriptorV2,
        SegmentManifestV2,
    };
    let descriptor = SegmentDescriptorV2 {
        segment_id: uuid::Uuid::from_u128(0x30),
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        lineage: RangeLineage::root(uuid::Uuid::from_u128(0x21)),
        base_offset: 0,
        segment_generation: 0,
        creation_node_id: uuid::Uuid::from_u128(0x10),
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
    let mut segment =
        ActiveSegment::create_v2(dir.join("bundle.active"), descriptor, config).unwrap();
    let records: Vec<LogRecord> = (0..3)
        .map(|sequence| LogRecord {
            producer_id: uuid::Uuid::from_u128(0x54),
            producer_epoch: 2,
            sequence,
            timestamp_millis: 1_700_000_000_000 + sequence as i64,
            attributes: 0,
            key: b"key".to_vec(),
            value: format!("value-{sequence}").into_bytes(),
        })
        .collect();
    segment.append_group(&records, Durability::Fsync).unwrap();
    drop(segment.seal_v2(None).unwrap());
    let manifest: SegmentManifestV2 =
        serde_json::from_slice(&std::fs::read(dir.join("bundle.manifest.json")).unwrap()).unwrap();
    (
        dir.join("bundle.segment"),
        blake3::Hash::from_hex(&manifest.chunk_tree_root).unwrap(),
    )
}

/// A `vtopctl tier copy` request for `sealed` to `object_uri`, under a
/// one-day governance lock.
fn tier_request(
    sealed: &Path,
    root: blake3::Hash,
    object_uri: &str,
) -> vtop_cli::tier_tools::TierCopyRequest {
    vtop_cli::tier_tools::TierCopyRequest {
        segment: sealed.to_path_buf(),
        topic_uuid: uuid::Uuid::from_u128(0x20),
        range_uuid: uuid::Uuid::from_u128(0x21),
        segment_uuid: uuid::Uuid::from_u128(0x30),
        expected_generation: 1,
        fencing_epoch: 1,
        expected_root: root,
        object_uri: object_uri.to_owned(),
        require_versioning: true,
        keyring: std::collections::BTreeMap::new(),
        verifier_node_uuid: uuid::Uuid::from_u128(0x10),
        verified_term: 2,
        request_id: uuid::Uuid::from_u128(0xfeed),
        issued_at_ms: 0,
        multipart: None,
        object_lock: Some(ObjectLockConfig {
            mode: ObjectLockMode::Governance,
            retain_days: 1,
            legal_hold: false,
        }),
    }
}

/// The tier's upload config, as `vtopctl tier copy --upload-config` reads
/// it, with the Object Lock block.
async fn tier_backend(bucket: &str) -> std::sync::Arc<dyn vtop_upload::UploadBackend> {
    let upload: vtop_core::config::UploadConfig = serde_yaml::from_str(&format!(
        "backend: s3_native\nbucket: {bucket}\nendpoint_url: {}\nforce_path_style: true\n\
         verify_tls: false\nrequire_object_versioning: true\n\
         object_lock:\n  mode: governance\n  retain_days: 1\n",
        endpoint()
    ))
    .unwrap();
    vtop_upload::build_backend(&upload).await.unwrap()
}

/// #684 for the cluster plane's cold tier: `vtopctl tier copy` with
/// `upload.object_lock` writes the segment and its manifest under the lock
/// and checks both retentions before it proposes the evidence; the store,
/// read behind its back, keeps both versions in governance mode for a day.
/// Against a bucket without Object Lock it refuses before writing anything.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_a_tier_copy_is_written_and_checked_under_the_lock() {
    let dir = tempfile::tempdir().unwrap();
    let (sealed, root) = seal_segment(dir.path());
    let client = s3_client().await;

    // A bucket without Object Lock: refused, nothing written.
    let plain = plain_bucket();
    let prefix = format!("vtop-it-684-tier-{}", unix_now());
    let refused = vtop_cli::tier_tools::run_tier_copy(
        tier_backend(&plain).await.as_ref(),
        &tier_request(
            &sealed,
            root,
            &format!("s3://{plain}/{prefix}/bundle.segment"),
        ),
    )
    .await
    .expect_err("a tier copy to a bucket without Object Lock was written");
    assert!(
        refused.contains(&format!(
            "bucket {plain} does not have Object Lock enabled; nothing was copied"
        )),
        "{refused}"
    );
    let listed = client
        .list_object_versions()
        .bucket(&plain)
        .prefix(&prefix)
        .send()
        .await
        .unwrap();
    assert!(listed.versions().is_empty(), "{listed:?}");

    // A locked bucket: written, checked, and locked in the store.
    let bucket = locked_bucket();
    let object_uri = format!("s3://{bucket}/{prefix}/bundle.segment");
    let before = unix_now();
    let command = vtop_cli::tier_tools::run_tier_copy(
        tier_backend(&bucket).await.as_ref(),
        &tier_request(&sealed, root, &object_uri),
    )
    .await
    .expect("a locked tier copy must succeed");
    let vtop_meta::MetadataCommand::CommitTierEvidence {
        object_version_id,
        manifest_version_id,
        ..
    } = command
    else {
        panic!("not tier evidence: {command:?}");
    };
    let written = [
        (
            format!("{prefix}/bundle.segment"),
            object_version_id.expect("the segment's version"),
        ),
        (
            format!("{prefix}/bundle.segment.manifest.json"),
            manifest_version_id.expect("the manifest's version"),
        ),
    ];
    for (key, version) in &written {
        let head = client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .version_id(version)
            .send()
            .await
            .unwrap();
        assert_eq!(
            head.object_lock_mode().map(|m| m.as_str()),
            Some("GOVERNANCE"),
            "{key}"
        );
        let until = head.object_lock_retain_until_date().unwrap().secs();
        assert!(
            until >= before + 86_400 && until <= unix_now() + 86_400 + 1,
            "{key} is retained one day from its write: {until}"
        );
    }

    // Clean up: the lab's root credentials may bypass governance.
    for (key, version) in written {
        client
            .delete_object()
            .bucket(&bucket)
            .key(&key)
            .version_id(version)
            .bypass_governance_retention(true)
            .send()
            .await
            .unwrap_or_else(|e| panic!("cleaning up {key}: {e:?}"));
    }
}
