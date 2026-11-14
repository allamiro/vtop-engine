//! #722: `upload.require_object_versioning` (the hardened manifest profile)
//! checks a literal bucket's versioning at startup, as the Object Lock
//! profile already checks its lock, and every refusal says what happened,
//! what the engine did and what to do.
//!
//! Before, nothing was checked until the first upload, which failed with
//! "bucket versioning preflight failed: ... the hardened manifest profile
//! requires it" and no word on what to do; a store that does not implement
//! versioning at all (HTTP 501, or Garage's `GetBucketVersioning` stub that
//! always answers "not enabled") ran, and failed every batch. These tests pin,
//! over the mock store:
//!
//! - a literal bucket without versioning refuses startup, naming it and the
//!   fix;
//! - a store that does not implement versioning refuses startup, saying no
//!   bucket setting can change it;
//! - a backend that cannot address object versions refuses startup, through
//!   `Engine::new_exclusive` itself;
//! - a check the store could not answer starts the engine, and the bucket is
//!   checked again at its first upload;
//! - a bucket checked at startup is not checked again at the first upload;
//! - a templated bucket is checked at its first upload, and refused there
//!   with the same three parts;
//! - without the profile nothing is checked.
#![cfg(unix)]

use std::sync::Arc;

use vtop_cli::Engine;
use vtop_core::config::StreamsConfig;
use vtop_core::state_machine::BatchState;
use vtop_core::types::SourceType;
use vtop_upload::{BucketFeature, MockBackend};

#[path = "support/object_lock.rs"]
mod support;
use support::*;

/// An engine with `upload.require_object_versioning` set (and no Object
/// Lock), over `backend`.
async fn versioned_engine(
    dir: &std::path::Path,
    tenant: &str,
    bucket: &str,
    backend: &Arc<MockBackend>,
) -> Engine {
    let input = dir.join("app.log");
    write_lines(&input, "app", 2);
    let mut engine = engine(config(dir, tenant, bucket, &input), None, backend).await;
    engine.config.upload.require_object_versioning = true;
    engine
}

#[tokio::test]
async fn a_literal_bucket_without_versioning_refuses_startup_naming_it_and_the_fix() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(MockBackend::new().with_bucket_versioning(BucketFeature::NotEnabled));
    let engine = versioned_engine(dir.path(), "ver-off", "Telemetry_Raw", &backend).await;

    let err = engine
        .preflight_bucket_versioning()
        .await
        .expect_err("a bucket without versioning must refuse startup")
        .to_string();
    assert_says(
        &err,
        &[
            // What happened, on the name batches are written to.
            "bucket telemetry-raw does not have versioning enabled",
            // What the engine did.
            "the engine did not start",
            // What to do.
            "Enable versioning on the bucket",
            "or set upload.require_object_versioning: false",
            // Review of #770: some stores answer "not enabled" and cannot
            // enable it (Garage's GetBucketVersioning is a stub).
            "if the store cannot enable it, use a store that keeps object versions",
        ],
    );
    assert_eq!(backend.versioning_calls(), 1);
}

#[tokio::test]
async fn a_store_that_does_not_implement_versioning_refuses_startup_saying_so() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(MockBackend::new().with_bucket_versioning(BucketFeature::NotSupported));
    let engine = versioned_engine(dir.path(), "ver-unsupported", "telemetry-raw", &backend).await;

    let err = engine
        .preflight_bucket_versioning()
        .await
        .expect_err("a store without versioning must refuse startup")
        .to_string();
    assert_says(
        &err,
        &[
            "the store does not implement S3 bucket versioning",
            "bucket telemetry-raw",
            "the engine did not start",
            "use a store that keeps object versions, or set \
             upload.require_object_versioning: false",
        ],
    );
}

/// Through the real door: `Engine::new_exclusive` (what `run`,
/// `process-once` and `replay` use) refuses a backend that cannot address
/// object versions, before anything is read or written. Before, it started
/// and failed every batch.
#[tokio::test]
async fn a_backend_without_object_versions_refuses_startup() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 2);
    let mut cfg = config(dir.path(), "ver-localfs", "telemetry-raw", &input);
    cfg.upload.backend = "localfs".into();
    cfg.upload.local_path = Some(dir.path().join("store").to_string_lossy().into_owned());
    cfg.upload.require_object_versioning = true;

    let err = match Engine::new_exclusive(cfg, StreamsConfig { streams: vec![] }).await {
        Ok(_) => panic!("a backend without object versions must refuse startup"),
        Err(e) => e.to_string(),
    };
    assert_says(
        &err,
        &[
            "upload.require_object_versioning is set",
            "the localfs upload backend cannot address object versions",
            "the engine did not start",
            "Use the s3_native backend, or set upload.require_object_versioning: false",
        ],
    );
}

#[tokio::test]
async fn a_check_the_store_could_not_answer_starts_and_is_retried_at_the_first_batch() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(MockBackend::new().with_versioning_errors(1));
    let mut engine = versioned_engine(dir.path(), "ver-blip", "telemetry-raw", &backend).await;

    engine
        .preflight_bucket_versioning()
        .await
        .expect("a store blip does not keep the engine down");
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    assert_eq!(
        backend.versioning_calls(),
        2,
        "the bucket the startup check could not read is checked at its first upload"
    );
}

#[tokio::test]
async fn a_bucket_checked_at_startup_is_not_checked_again() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(MockBackend::new());
    let mut engine = versioned_engine(dir.path(), "ver-ok", "telemetry-raw", &backend).await;

    engine.preflight_bucket_versioning().await.unwrap();
    assert_eq!(backend.versioning_calls(), 1);
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    assert_eq!(backend.versioning_calls(), 1);
}

#[tokio::test]
async fn a_templated_bucket_without_versioning_fails_its_first_batch_in_three_parts() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(MockBackend::new().with_bucket_versioning(BucketFeature::NotEnabled));
    let mut engine =
        versioned_engine(dir.path(), "ver-templated", "telemetry-{format}", &backend).await;

    engine
        .preflight_bucket_versioning()
        .await
        .expect("a templated bucket is checked at first use, not at startup");
    assert_eq!(backend.versioning_calls(), 0);

    let out = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(!out[0].committed, "{out:?}");
    let failed = row(&engine, &out[0].batch_id).await;
    assert_eq!(failed.state, BatchState::ReplayRequired);
    assert!(
        failed.object_uri.is_none(),
        "nothing was uploaded: {failed:?}"
    );
    assert_says(
        &failed.error_message.unwrap_or_default(),
        &[
            "bucket telemetry-raw does not have versioning enabled",
            "the batch was not uploaded and will be replayed",
            "Enable versioning on the bucket",
        ],
    );
}

#[tokio::test]
async fn without_the_profile_no_bucket_versioning_is_checked() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(MockBackend::new().with_bucket_versioning(BucketFeature::NotSupported));
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 2);
    let mut engine = engine(
        config(dir.path(), "ver-unset", "telemetry-raw", &input),
        None,
        &backend,
    )
    .await;

    engine.preflight_bucket_versioning().await.unwrap();
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    assert_eq!(backend.versioning_calls(), 0);
}

/// Review of #770: with `upload.create_bucket` the bucket may not exist yet
/// at startup (the pipeline creates it), so the startup check would only
/// report a missing bucket as an unreadable one. It is left to the first
/// upload, after the bucket is created.
#[tokio::test]
async fn with_create_bucket_the_check_waits_for_the_first_upload() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(MockBackend::new());
    let mut engine = versioned_engine(dir.path(), "ver-create", "telemetry-raw", &backend).await;
    engine.config.upload.create_bucket = true;

    engine.preflight_bucket_versioning().await.unwrap();
    assert_eq!(backend.versioning_calls(), 0);
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    assert_eq!(backend.versioning_calls(), 1);
}
