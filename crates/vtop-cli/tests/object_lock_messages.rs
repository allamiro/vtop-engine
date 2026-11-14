//! #684: the Object Lock profile's operator log events, with their fields.
//!
//! One test in this binary, on purpose. Tracing caches each callsite's
//! interest globally; when a test that captures logs runs beside tests that
//! hit the same callsite with no subscriber, the cache can hold "never" and
//! the event never reaches the capture (it failed that way in CI when these
//! assertions lived in `object_lock.rs`). Here nothing runs concurrently, so
//! an event that is logged is captured, and a missing one is a real failure.
//!
//! - the templated-bucket refusal: `object_lock_bucket_refused`, with the
//!   bucket and the batch id;
//! - each failed retention check: `object_lock_check_failed`, with the
//!   result, the batch id and the object URI;
//! - a store error quoting a signed URL: the event is logged, redacted;
//! - a bucket check the store could not answer at startup: a WARN saying the
//!   engine started anyway and checks again at the first upload.
#![cfg(unix)]

use std::sync::Arc;

use vtop_core::config::ObjectLockMode;
use vtop_core::types::SourceType;
use vtop_upload::LockFault;

#[path = "support/object_lock.rs"]
mod support;
use support::*;

#[tokio::test]
async fn the_object_lock_events_carry_what_an_operator_needs() {
    // ---- the templated-bucket refusal
    {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 2);
        let lock = governance();
        let backend = Arc::new(locked_mock(&lock).without_bucket_object_lock());
        let mut engine = engine(
            config(dir.path(), "msg-templated", "telemetry-{format}", &input),
            Some(lock),
            &backend,
        )
        .await;
        let (logs, _guard) = Logs::capture();
        let out = engine.process_once(SourceType::File).await.unwrap();
        assert!(!out[0].committed);
        let event = logs.events("object_lock_bucket_refused");
        assert_eq!(event.len(), 1, "{}", logs.raw());
        assert_eq!(text(&event[0], "bucket"), "telemetry-raw");
        assert_eq!(text(&event[0], "batch_id"), out[0].batch_id);
        assert_says(
            &text(&event[0], "message"),
            &[
                "does not have Object Lock enabled",
                "the batch was not uploaded",
            ],
        );
    }

    // ---- each failed retention check
    for (result, fault, legal_hold) in [
        ("missing", LockFault::Ignored, false),
        (
            "mode_mismatch",
            LockFault::Mode(ObjectLockMode::Compliance),
            false,
        ),
        ("too_short", LockFault::ShiftSeconds(-1), false),
        ("legal_hold_missing", LockFault::NoLegalHold, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 3);
        let lock = lock(ObjectLockMode::Governance, legal_hold);
        let backend = Arc::new(locked_mock(&lock).with_lock_fault(fault));
        let mut engine = engine(
            config(
                dir.path(),
                &format!("msg-{result}"),
                "telemetry-raw",
                &input,
            ),
            Some(lock),
            &backend,
        )
        .await;
        let (logs, _guard) = Logs::capture();
        let out = engine.process_once(SourceType::File).await.unwrap();
        assert!(!out[0].committed, "[{result}]");
        let failed = row(&engine, &out[0].batch_id).await;
        let event = logs.events("object_lock_check_failed");
        assert_eq!(event.len(), 1, "[{result}] {}", logs.raw());
        assert_eq!(text(&event[0], "result"), result);
        assert_eq!(text(&event[0], "batch_id"), out[0].batch_id);
        assert_eq!(text(&event[0], "what"), "object");
        assert_eq!(
            text(&event[0], "object_uri"),
            failed.object_uri.clone().unwrap()
        );
        assert_says(
            &text(&event[0], "message"),
            &[
                "is not under the requested retention",
                "the batch was marked failed and will be replayed",
                "s3:PutObjectRetention",
            ],
        );
    }

    // ---- a store error quoting a signed URL
    {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 2);
        let lock = governance();
        let signed = "https://telemetry-raw.s3.amazonaws.com/x/obj.raw?versionId=v1\
                      &X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAEXAMPLE%2F20261114\
                      &X-Amz-Signature=deadbeefcafe";
        let backend = Arc::new(
            locked_mock(&lock)
                .with_retention_error(format!("head_object {signed}: connection reset")),
        );
        let mut engine = engine(
            config(dir.path(), "msg-redaction", "telemetry-raw", &input),
            Some(lock),
            &backend,
        )
        .await;
        let (logs, _guard) = Logs::capture();
        let out = engine.process_once(SourceType::File).await.unwrap();
        assert!(!out[0].committed);
        let event = logs.events("object_lock_check_failed");
        assert_eq!(event.len(), 1, "the failure is logged: {}", logs.raw());
        assert_eq!(text(&event[0], "result"), "error");
        assert!(text(&event[0], "message").contains("could not be read"));
        for secret in [
            "X-Amz-Signature",
            "deadbeefcafe",
            "X-Amz-Credential",
            "AKIAEXAMPLE",
        ] {
            assert!(!logs.raw().contains(secret), "the log holds {secret}");
        }
    }

    // ---- a bucket check the store could not answer at startup
    {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 2);
        let lock = governance();
        let backend = Arc::new(locked_mock(&lock).with_lock_config_errors(1));
        let engine = engine(
            config(dir.path(), "msg-blip", "telemetry-raw", &input),
            Some(lock),
            &backend,
        )
        .await;
        let (logs, _guard) = Logs::capture();
        engine.preflight_object_lock().await.unwrap();
        let raw = logs.raw();
        assert!(
            raw.contains("\"level\":\"WARN\"")
                && raw.contains("could not be read")
                && raw.contains("the engine started anyway")
                && raw.contains("checked again before the first upload"),
            "{raw}"
        );
    }
}
