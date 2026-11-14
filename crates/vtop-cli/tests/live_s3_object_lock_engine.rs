//! #684 end to end against a real S3-compatible store with Object Lock: the
//! engine, started the way `vtopctl run` starts it (`Engine::new_exclusive`),
//! with the s3_native backend and `upload.object_lock` set.
//!
//! - against a bucket created without Object Lock it refuses to start, and
//!   says what to do;
//! - against a locked bucket it starts, archives a batch to
//!   SOURCE_COMMITTED, and the object and the manifest it wrote are under the
//!   configured mode until a date about `retain_days` away, read back from
//!   the store behind the engine's back.
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
    cfg.upload.endpoint_url = Some(endpoint());
    cfg.upload.force_path_style = true;
    cfg.upload.verify_tls = false;
    cfg.upload.require_object_versioning = true;
    cfg.upload.object_lock = Some(ObjectLockConfig {
        mode: ObjectLockMode::Governance,
        retain_days: 1,
        legal_hold: false,
    });
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
    for uri in [
        row.object_uri.clone().unwrap(),
        row.manifest_uri.clone().unwrap(),
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
