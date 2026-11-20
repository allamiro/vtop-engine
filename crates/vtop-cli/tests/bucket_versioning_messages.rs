//! #722: the hardened profile's versioning log events, with their fields
//! (review of #770: an important message is asserted, so a refactor cannot
//! drop it silently).
//!
//! One test in this binary, on purpose, as in `object_lock_messages.rs`:
//! tracing caches each callsite's interest globally, and a concurrent test
//! that hits the callsite first with no subscriber can hide the event.
//!
//! - the templated-bucket refusal: `bucket_versioning_refused`, ERROR, with
//!   the bucket and the batch id, in three parts;
//! - a versioning check the store could not answer at startup: a WARN saying
//!   the engine started anyway and checks again at the first upload.
#![cfg(unix)]

use std::sync::Arc;

use vtop_core::types::SourceType;
use vtop_upload::{BucketFeature, MockBackend};

#[path = "support/object_lock.rs"]
mod support;
use support::*;

#[tokio::test]
async fn the_versioning_events_carry_what_an_operator_needs() {
    // ---- the templated-bucket refusal
    {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 2);
        let backend =
            Arc::new(MockBackend::new().with_bucket_versioning(BucketFeature::NotEnabled));
        let mut engine = engine(
            config(dir.path(), "vmsg-templated", "telemetry-{format}", &input),
            None,
            &backend,
        )
        .await;
        engine.config.upload.require_object_versioning = true;
        let (logs, _guard) = Logs::capture();
        let out = engine.process_once(SourceType::File).await.unwrap();
        assert!(!out[0].committed);
        let event = logs.events("bucket_versioning_refused");
        assert_eq!(event.len(), 1, "{}", logs.raw());
        assert_eq!(text(&event[0], "bucket"), "telemetry-raw");
        assert_eq!(text(&event[0], "batch_id"), out[0].batch_id);
        assert!(logs.raw().contains("\"level\":\"ERROR\""), "{}", logs.raw());
        assert_says(
            &text(&event[0], "message"),
            &[
                "bucket telemetry-raw does not have versioning enabled",
                "the batch was not uploaded and will be replayed",
                "Enable versioning on the bucket",
            ],
        );
    }

    // ---- a check the store could not answer at startup
    {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 2);
        let backend = Arc::new(MockBackend::new().with_versioning_errors(1));
        let mut engine = engine(
            config(dir.path(), "vmsg-blip", "telemetry-raw", &input),
            None,
            &backend,
        )
        .await;
        engine.config.upload.require_object_versioning = true;
        let (logs, _guard) = Logs::capture();
        engine.preflight_bucket_versioning().await.unwrap();
        let raw = logs.raw();
        assert!(raw.contains("\"level\":\"WARN\""), "{raw}");
        assert_says(
            &raw,
            &[
                "the versioning of bucket telemetry-raw could not be read",
                "ServiceUnavailable (http 503)",
                "the engine started anyway, and the bucket is checked again before the first upload to it",
                "s3:GetBucketVersioning",
            ],
        );
    }
}
