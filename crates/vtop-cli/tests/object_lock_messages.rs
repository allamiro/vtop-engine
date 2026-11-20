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
//!   engine started anyway and checks again at the first upload;
//! - a replay that finds its object unlocked or its lock ended (#709): an
//!   `object_key_fallback` WARN naming the batch, the old key and the new
//!   key, in three parts, with no query string;
//! - a replay that cannot read that object's retention: an ERROR, and no
//!   fallback;
//! - a verification receipt the scrub found without its retention (#710):
//!   `scrub_receipt_retention_finding`, a WARN with the batch id, the
//!   receipt's URI and result, and a detail saying what happened, that the
//!   scrub changed nothing, and what to do.
#![cfg(unix)]

use std::sync::Arc;

use vtop_cli::scrub::{self, ScrubOptions};
use vtop_core::config::ObjectLockMode;
use vtop_core::types::SourceType;
use vtop_upload::{LockFault, StoredRetention};

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

    // ---- a replay that falls back from an unlocked or ended lock (#709)
    for (decision, kept, why) in [
        (
            "fallback_unlocked",
            StoredRetention::default(),
            "it carries no Object Lock retention",
        ),
        (
            "fallback_lock_elapsed",
            StoredRetention {
                mode: Some("GOVERNANCE".into()),
                retain_until_unix_secs: Some(1_000_000_000),
                legal_hold: Some(false),
            },
            "its Object Lock retention ended at 2001-09-09T01:46:40Z",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 3);
        let lock = governance();
        let backend = Arc::new(locked_mock(&lock).with_throttled_manifest_puts(1));
        let mut engine = engine(
            config(
                dir.path(),
                &format!("msg-{decision}"),
                "telemetry-raw",
                &input,
            ),
            Some(lock),
            &backend,
        )
        .await;
        let first = engine.process_once(SourceType::File).await.unwrap();
        assert!(!first[0].committed, "[{decision}]");
        let derived = row(&engine, &first[0].batch_id).await.object_uri.unwrap();
        backend.set_stored_retention(&derived, None, kept);

        let (logs, _guard) = Logs::capture();
        let second = engine.process_once(SourceType::File).await.unwrap();
        assert!(second[0].committed, "[{decision}] {second:?}");
        let landed = second[0].object_uri.clone().unwrap();
        let event = logs.events("object_key_fallback");
        assert_eq!(event.len(), 1, "[{decision}] {}", logs.raw());
        let raw = logs.raw();
        let line = raw
            .lines()
            .find(|l| l.contains("object_key_fallback"))
            .unwrap();
        assert!(line.contains("\"level\":\"WARN\""), "[{decision}] {line}");
        assert_eq!(text(&event[0], "decision"), decision);
        assert_eq!(text(&event[0], "batch_id"), second[0].batch_id);
        assert_eq!(text(&event[0], "key"), derived);
        assert_eq!(text(&event[0], "fallback_key"), landed);
        let message = text(&event[0], "message");
        assert_says(
            &message,
            &[
                // what happened
                "is already archived at",
                &derived,
                why,
                // what the engine did
                &format!(
                    "writing batch {} under its attempt key {landed}",
                    second[0].batch_id
                ),
                "as a fresh object locked governance for 30 days",
                "commits it only once that lock is read back",
                "is left as it is (not written over, deleted or re-locked)",
                // what to do
                "nothing is lost",
            ],
        );
        assert!(
            !message.contains('?'),
            "[{decision}] no query string: {message}"
        );
        assert!(
            logs.events("object_lock_check_failed").is_empty(),
            "[{decision}] the fallback is not a failed check: {}",
            logs.raw()
        );
    }

    // ---- a replay that cannot read the reused object's retention (#709)
    {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 3);
        let lock = governance();
        let backend = Arc::new(
            locked_mock(&lock)
                .with_retention_error("mock head_object: ServiceUnavailable (http 503)"),
        );
        let mut engine = engine(
            config(dir.path(), "msg-reuse-error", "telemetry-raw", &input),
            Some(lock),
            &backend,
        )
        .await;
        let first = engine.process_once(SourceType::File).await.unwrap();
        assert!(!first[0].committed);
        let derived = row(&engine, &first[0].batch_id).await.object_uri.unwrap();

        let (logs, _guard) = Logs::capture();
        let second = engine.process_once(SourceType::File).await.unwrap();
        assert!(!second[0].committed);
        assert!(
            logs.events("object_key_fallback").is_empty(),
            "no fallback on a store error: {}",
            logs.raw()
        );
        let event = logs.events("object_lock_check_failed");
        assert_eq!(event.len(), 1, "{}", logs.raw());
        assert_eq!(text(&event[0], "result"), "error");
        assert_eq!(text(&event[0], "batch_id"), second[0].batch_id);
        assert_eq!(text(&event[0], "object_uri"), derived);
        assert_says(
            &text(&event[0], "message"),
            &[
                "could not be read",
                "it was neither reused nor replaced by a fresh object",
                "the batch was marked failed and will be replayed",
            ],
        );
    }

    // ---- the fallback notice is rate-limited per source and kind (#709):
    // a second fallback of the same kind for the same source within the
    // minute is counted, not logged.
    {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 3);
        let lock = governance();
        let backend = Arc::new(locked_mock(&lock));
        let tenant = "msg-rate-limit";
        let mut engine = engine(
            config(dir.path(), tenant, "telemetry-raw", &input),
            Some(lock),
            &backend,
        )
        .await;
        let (logs, _guard) = Logs::capture();
        for round in 0..2 {
            if round == 1 {
                // A new range of the same file, archived the same way.
                write_lines(&input, "more", 3);
            }
            // The first attempt's manifest put fails, leaving its object
            // under the derived key.
            backend.set_failing_put_suffix(Some(".manifest.json"));
            let first = engine.process_once(SourceType::File).await.unwrap();
            assert!(!first[0].committed, "[{round}]");
            backend.set_failing_put_suffix(None);
            let derived = row(&engine, &first[0].batch_id).await.object_uri.unwrap();
            backend.set_stored_retention(&derived, None, StoredRetention::default());
            let replay = engine.process_once(SourceType::File).await.unwrap();
            assert!(replay[0].committed, "[{round}] {replay:?}");
        }
        assert_eq!(decisions(tenant, "fallback_unlocked"), 2);
        assert_eq!(
            logs.events("object_key_fallback").len(),
            1,
            "the second notice within the minute is suppressed: {}",
            logs.raw()
        );
    }

    // ---- lock_withheld: the fresh object is not usable either (#709)
    {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 3);
        let lock = governance();
        let backend = Arc::new(locked_mock(&lock).with_throttled_manifest_puts(1));
        let mut engine = engine(
            config(dir.path(), "msg-withheld", "telemetry-raw", &input),
            Some(lock),
            &backend,
        )
        .await;
        let first = engine.process_once(SourceType::File).await.unwrap();
        assert!(!first[0].committed);
        let derived = row(&engine, &first[0].batch_id).await.object_uri.unwrap();
        backend.set_stored_retention(&derived, None, StoredRetention::default());
        backend.set_lock_fault(Some(LockFault::Ignored));
        let second = engine.process_once(SourceType::File).await.unwrap();
        assert!(!second[0].committed);
        let fresh = row(&engine, &second[0].batch_id).await.object_uri.unwrap();

        let (logs, _guard) = Logs::capture();
        let third = engine.process_once(SourceType::File).await.unwrap();
        assert!(!third[0].committed);
        let event = logs.events("object_key_fallback");
        assert_eq!(event.len(), 1, "{}", logs.raw());
        assert_eq!(text(&event[0], "decision"), "lock_withheld");
        assert_eq!(text(&event[0], "batch_id"), third[0].batch_id);
        assert_eq!(text(&event[0], "key"), derived);
        assert_eq!(text(&event[0], "fallback_key"), fresh);
        assert_says(
            &text(&event[0], "message"),
            &[
                "is not usable either",
                "nothing was written: the batch was marked failed and will be replayed",
                "check the bucket's Object Lock configuration",
            ],
        );
    }

    // ---- a receipt the scrub found without its retention (#710)
    {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 2);
        let lock = governance();
        let backend = Arc::new(locked_mock(&lock));
        let mut cfg = config(dir.path(), "msg-receipt-retention", "telemetry-raw", &input);
        cfg.manifest.verification_receipts = true;
        let mut engine = engine(cfg, Some(lock), &backend).await;
        let out = engine.process_once(SourceType::File).await.unwrap();
        assert!(out[0].committed, "{out:?}");
        let committed = row(&engine, &out[0].batch_id).await;
        let ruri = vtop_core::partitioning::receipt_uri(committed.manifest_uri.as_deref().unwrap())
            .unwrap();
        backend.set_stored_retention(&ruri, None, StoredRetention::default());
        let all = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
        let (logs, _guard) = Logs::capture();
        let report = engine
            .scrub(
                all,
                &ScrubOptions {
                    check_retention: Some(ObjectLockMode::Governance),
                    check_receipts: true,
                    ..ScrubOptions::default()
                },
            )
            .await
            .unwrap();
        assert!(!report.passed, "{report:#?}");
        let event = logs.events("scrub_receipt_retention_finding");
        assert_eq!(event.len(), 1, "{}", logs.raw());
        assert_eq!(text(&event[0], "batch_id"), out[0].batch_id);
        assert_eq!(text(&event[0], "receipt_uri"), ruri);
        assert_eq!(text(&event[0], "receipt_retention"), "missing");
        let line = logs
            .raw()
            .lines()
            .find(|l| l.contains("scrub_receipt_retention_finding"))
            .map(str::to_owned)
            .unwrap();
        let line: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(line["level"], "WARN", "{line}");
        assert_eq!(
            text(&event[0], "message"),
            "scrub receipt retention finding"
        );
        assert_says(
            &text(&event[0], "detail"),
            &[
                "carries no retention",
                "the scrub reported it and changed nothing",
                "re-apply the retention with PutObjectRetention",
            ],
        );
    }
}
