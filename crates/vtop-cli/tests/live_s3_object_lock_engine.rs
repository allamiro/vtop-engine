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
//!   bytes and its (ended) retention.
//!
//! Run against the compose MinIO lab with:
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin
//!  AWS_SECRET_ACCESS_KEY=minioadmin cargo test -p vtop-cli
//!  --test live_s3_object_lock_engine -- --ignored --test-threads=1`
//! The objects it writes are under a one-day governance retention; the test
//! deletes them with `x-amz-bypass-governance-retention`, which the lab's
//! root credentials may use. CI runs it in the `minio-integration` job.
#![cfg(unix)]

use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::{ObjectLockConfig, ObjectLockMode, StreamsConfig, VtopConfig};
use vtop_core::types::SourceType;

fn endpoint() -> String {
    std::env::var("VTOP_TEST_S3_ENDPOINT")
        .expect("VTOP_TEST_S3_ENDPOINT must name the live endpoint")
}

fn locked_bucket() -> String {
    std::env::var("VTOP_TEST_S3_LOCKED_BUCKET").unwrap_or_else(|_| "telemetry-locked".into())
}

fn plain_bucket() -> String {
    std::env::var("VTOP_TEST_S3_BUCKET").unwrap_or_else(|_| "telemetry-raw".into())
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
    cfg.upload.endpoint_url = Some(endpoint());
    cfg.upload.force_path_style = true;
    cfg.upload.verify_tls = false;
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

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_the_engine_refuses_a_bucket_without_object_lock_at_startup() {
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
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_the_engine_archives_a_batch_under_the_configured_lock() {
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

    let client = s3_client().await;
    let receipt =
        vtop_core::partitioning::receipt_uri(row.manifest_uri.as_deref().unwrap()).unwrap();
    for uri in [
        row.object_uri.clone().unwrap(),
        row.manifest_uri.clone().unwrap(),
        receipt,
    ] {
        let key = uri
            .strip_prefix(&format!("s3://{bucket}/"))
            .unwrap()
            .to_owned();
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
        // Clean up: the lab's root credentials may bypass governance.
        let version = head.version_id().unwrap().to_owned();
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
}
