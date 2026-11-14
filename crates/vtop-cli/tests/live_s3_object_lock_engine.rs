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
//! - `vtopctl scrub --check-retention`, with receipts checked, reads the
//!   batch's and the receipt's retention from the store and finds both
//!   locked; once the receipt's retention is cleared (by a principal allowed
//!   to bypass governance), it reports the receipt, and only the receipt,
//!   as a `missing` retention finding (#710).
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
    let input = dir.join("app.log");
    let mut f = std::fs::File::create(&input).unwrap();
    for i in 0..5 {
        writeln!(f, "locked record {i}").unwrap();
    }
    let mut cfg = file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join("state.db").display()),
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

/// An S3 AccessDenied: the AWS leg's role holds no
/// s3:BypassGovernanceRetention, on purpose (#722).
fn access_denied<E: aws_sdk_s3::error::ProvideErrorMetadata, R>(
    e: &aws_sdk_s3::error::SdkError<E, R>,
) -> bool {
    e.as_service_error().and_then(|se| se.code()) == Some("AccessDenied")
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
            // governance can (#710): the scrub reports the receipt, not the
            // batch. The lab's root credentials may; the AWS leg's role
            // deliberately may not (#722: no s3:BypassGovernanceRetention), so
            // there this half is not run, and the log says so.
            let cleared = client
                .put_object_retention()
                .bucket(&bucket)
                .key(key_of(&receipt))
                .bypass_governance_retention(true)
                .retention(aws_sdk_s3::types::ObjectLockRetention::builder().build())
                .send()
                .await;
            match cleared {
                Ok(_) => {
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
                }
                Err(e) if access_denied(&e) => eprintln!(
                    "{receipt}: clearing a governance retention was refused (no bypass \
                     permission); the scrub of a receipt without retention is not run here"
                ),
                Err(e) => panic!("clearing the receipt's retention: {e:?}"),
            }

            // Clean up: the lab's root credentials may bypass governance; the
            // AWS leg's role may not, and there the bucket's lifecycle rule
            // removes the versions once their one-day retention ends.
            for (uri, key, version) in written {
                let deleted = client
                    .delete_object()
                    .bucket(&bucket)
                    .key(&key)
                    .version_id(&version)
                    .bypass_governance_retention(true)
                    .send()
                    .await;
                match deleted {
                    Ok(_) => {}
                    Err(e) if access_denied(&e) => {
                        eprintln!("{uri}: governance bypass refused; left for the bucket's lifecycle rule")
                    }
                    Err(e) => panic!("cleaning up {uri}: {e:?}"),
                }
            }
        },
    )
    .await;
}
