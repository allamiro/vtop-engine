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
//!   as a `missing` retention finding (#710).
//!
//! Run against the compose MinIO lab with:
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin
//!  AWS_SECRET_ACCESS_KEY=minioadmin cargo test -p vtop-cli
//!  --test live_s3_object_lock_engine -- --ignored --test-threads=1`
//! The objects it writes are under a one-day governance retention; the test
//! deletes them with `x-amz-bypass-governance-retention`, which the lab's
//! root credentials may use. CI runs it in the `s3-compat` matrix (#722).
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
use vtop_s3_testkit::{Capability, LiveS3, RefusalAt, S3Settings};

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

/// The refusal on a store without Object Lock or versioning (#722; Garage):
/// the engine, started as `vtopctl run` starts it, refuses at startup in three
/// parts (#770). Against the plain bucket: such a store has no locked one.
async fn refused_at_startup(capability: Capability, at: RefusalAt) {
    assert_eq!(
        at,
        RefusalAt::Startup,
        "VTOP refuses {capability} at startup"
    );
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path(), &plain_bucket(), "vtop-it-722-refusal");
    let parts: &[&str] = match capability {
        Capability::ObjectLock | Capability::ObjectLockEnforced => &[
            "the store does not implement S3 Object Lock",
            "the engine did not start",
            "remove upload.object_lock",
        ],
        Capability::Versioning => {
            // The hardened profile alone, so its own check is the one asked.
            cfg.upload.object_lock = None;
            &[
                "versioning",
                "the engine did not start",
                "upload.require_object_versioning: false",
            ]
        }
        other => panic!("no refusal is asserted for {other}"),
    };
    let err = match Engine::new_exclusive(cfg, StreamsConfig { streams: vec![] }).await {
        Ok(_) => panic!("an engine must not start on a store without {capability}"),
        Err(e) => e.to_string(),
    };
    for part in parts {
        assert!(err.contains(part), "{err:?} should say {part:?}");
    }
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_the_engine_refuses_a_bucket_without_object_lock_at_startup() {
    let live = LiveS3::from_env();
    live.case_with_refusal(
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
        refused_at_startup,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_the_engine_archives_a_batch_under_the_configured_lock() {
    let live = LiveS3::from_env();
    live.case_with_refusal(
        "engine.object_lock.archives_a_batch_locked",
        &[Capability::ObjectLock, Capability::Versioning],
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
        refused_at_startup,
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
    live.case_with_refusal(
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
        refused_at_startup,
    )
    .await;
}
