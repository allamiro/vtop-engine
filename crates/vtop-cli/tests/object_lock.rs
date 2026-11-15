//! #684: the S3 Object Lock profile through the engine.
//!
//! With `upload.object_lock` set, every object and manifest a batch writes is
//! asked for a retention lock, the bucket is checked for Object Lock before
//! anything is written to it, and a batch whose stored object or manifest
//! does not carry the retention the engine asked for FAILS: it is never
//! VERIFIED, its source position does not advance, and it is replayed. These
//! tests pin, over the mock store with its Object Lock semantics:
//!
//! - a bucket without Object Lock refuses startup (a literal bucket name) or
//!   the first batch for it (a templated one), with a message naming it;
//! - with the profile unset nothing is checked, as before;
//! - a lock the store kept passes, and a longer bucket default passes;
//! - a lock the store dropped, kept in another mode, kept shorter, or kept
//!   without the legal hold fails the batch, each counted by its own result;
//! - the retention read is of the exact version written, not the key;
//! - a reused object (#626) is checked with the weaker bound (still locked),
//!   and the reusing attempt's own manifest strictly;
//! - an object at the derived key that does not verify is not written over,
//!   and the batch lands, locked, on its attempt key;
//! - a multipart object carries the lock its CreateMultipartUpload asked for;
//! - a store error quoting a signed URL never reaches the ledger or the log;
//! - `vtopctl scrub --check-retention` reports locked, elapsed, missing and
//!   mode_mismatch, the last two as findings;
//! - when the scrub also checks verification receipts, it reads each existing
//!   receipt's own retention (#710), counted apart from the batch's in
//!   `vtop_scrub_receipt_retention_total`: a receipt with no retention or
//!   under another mode is a finding, an ended one is not, a missing receipt
//!   is never a retention finding, a retention that cannot be read makes the
//!   receipt (not the batch) unreadable, and nothing is read unless both
//!   checks run.
#![cfg(unix)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use vtop_cli::scrub::{self, RetentionResult, ScrubOptions};
use vtop_cli::Engine;
use vtop_core::config::{ObjectLockConfig, ObjectLockMode};
use vtop_core::state_machine::BatchState;
use vtop_core::types::SourceType;
use vtop_upload::{LockFault, MockBackend, ObjectLockPolicy, StoredRetention, UploadBackend};

// Log events are asserted in `object_lock_messages.rs`, a binary with one
// test: tracing caches a callsite's interest globally, and a concurrent test
// that hits it first with no subscriber can hide the event from a capture.
#[path = "support/object_lock.rs"]
mod support;
use support::*;

// ---- The bucket ------------------------------------------------------------

#[tokio::test]
async fn a_literal_bucket_without_object_lock_refuses_startup_naming_it() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 2);
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock).without_bucket_object_lock());
    let engine = engine(
        config(dir.path(), "lock-startup", "telemetry-plain", &input),
        Some(lock),
        &backend,
    )
    .await;

    let err = engine
        .preflight_object_lock()
        .await
        .expect_err("a bucket without Object Lock must refuse startup")
        .to_string();
    assert_says(
        &err,
        &[
            "bucket telemetry-plain does not have Object Lock enabled",
            "the engine did not start",
            "Object Lock can only be enabled when a bucket is created",
            "point upload.bucket at it",
        ],
    );
    assert_eq!(backend.object_lock_config_calls(), 1);
}

#[tokio::test]
async fn a_templated_bucket_without_object_lock_fails_its_first_batch_and_uploads_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 2);
    let tenant = "lock-templated";
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock).without_bucket_object_lock());
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-{format}", &input),
        Some(lock),
        &backend,
    )
    .await;
    let refused_before = checks(tenant, "bucket_not_locked");

    engine
        .preflight_object_lock()
        .await
        .expect("a templated bucket is checked at first use, not at startup");
    assert_eq!(backend.object_lock_config_calls(), 0);

    let out = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(!out[0].committed, "{out:?}");
    let failed = row(&engine, &out[0].batch_id).await;
    assert_eq!(failed.state, BatchState::ReplayRequired);
    assert!(
        failed.object_uri.is_none(),
        "nothing was uploaded to an unlocked bucket: {failed:?}"
    );
    let message = failed.error_message.unwrap_or_default();
    assert_says(
        &message,
        &[
            "bucket telemetry-raw does not have Object Lock enabled",
            "the batch was not uploaded",
            "create a new bucket with it and point upload.bucket at it",
        ],
    );
    assert_eq!(checks(tenant, "bucket_not_locked"), refused_before + 1);
}

#[tokio::test]
async fn without_the_profile_no_bucket_or_object_is_checked() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 2);
    let tenant = "lock-unset";
    let backend = Arc::new(MockBackend::new());
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-raw", &input),
        None,
        &backend,
    )
    .await;

    engine.preflight_object_lock().await.unwrap();
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    assert_eq!(
        backend.object_lock_config_calls(),
        0,
        "no GetObjectLockConfiguration without the profile"
    );
    let uri = out[0].object_uri.clone().unwrap();
    assert_eq!(
        backend.stored_retention(&uri, None),
        StoredRetention::default()
    );
    for result in vtop_core::telemetry::OBJECT_LOCK_CHECK_RESULTS {
        assert_eq!(checks(tenant, result), 0, "{result}");
    }
    // The startup check sets the mode gauge: off (review of #708).
    let gauge = |mode: &str| {
        vtop_core::telemetry::init()
            .unwrap()
            .object_lock_mode
            .with_label_values(&[tenant, mode])
            .get()
    };
    assert_eq!(
        (gauge("off"), gauge("governance"), gauge("compliance")),
        (1, 0, 0)
    );
}

// ---- What the store kept ----------------------------------------------------

#[tokio::test]
async fn a_lock_the_store_kept_passes_for_the_object_and_the_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-kept";
    let lock = lock(ObjectLockMode::Compliance, true);
    let backend = Arc::new(locked_mock(&lock));
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-raw", &input),
        Some(lock),
        &backend,
    )
    .await;
    let ok_before = checks(tenant, "ok");

    engine.preflight_object_lock().await.unwrap();
    assert_eq!(
        backend.object_lock_config_calls(),
        1,
        "the literal bucket is checked at startup"
    );
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    assert_eq!(
        checks(tenant, "ok"),
        ok_before + 2,
        "the object and the manifest were each checked"
    );
    let rows = committed(&engine).await;
    let object = backend.stored_retention(rows[0].object_uri.as_deref().unwrap(), None);
    let manifest = backend.stored_retention(rows[0].manifest_uri.as_deref().unwrap(), None);
    for kept in [object, manifest] {
        assert_eq!(kept.mode.as_deref(), Some("COMPLIANCE"));
        assert_eq!(kept.legal_hold, Some(true));
    }
    // The first batch for the bucket was not checked again.
    assert_eq!(backend.object_lock_config_calls(), 1);
}

/// A store that keeps a LATER date than asked for passes (a guard: the
/// comparison is "no earlier", not "equal"). S3 itself keeps exactly the
/// date asked for, since an explicit retention replaces a bucket default
/// rather than combining with it, so this stands for a retention extended
/// between the write and the check.
#[tokio::test]
async fn a_longer_bucket_default_passes() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-longer";
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock).with_lock_fault(LockFault::ShiftSeconds(86_400)));
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-raw", &input),
        Some(lock),
        &backend,
    )
    .await;
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    assert_eq!(checks(tenant, "ok"), 2);
}

/// Each way a store can keep less than it was asked for fails the batch:
/// FAILED then REPLAY_REQUIRED, nothing committed, the verification failure
/// counted, and the check counted under its own result.
#[tokio::test]
async fn a_lock_the_store_did_not_keep_fails_the_batch_by_its_own_result() {
    let cases = [
        ("missing", LockFault::Ignored, false, "found no retention"),
        (
            "mode_mismatch",
            LockFault::Mode(ObjectLockMode::Compliance),
            false,
            "found mode COMPLIANCE",
        ),
        (
            "too_short",
            LockFault::ShiftSeconds(-1),
            false,
            "found retain-until",
        ),
        (
            "legal_hold_missing",
            LockFault::NoLegalHold,
            true,
            "found no legal hold",
        ),
    ];
    for (result, fault, legal_hold, found) in cases {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 3);
        let tenant = format!("lock-fault-{result}");
        let lock = lock(ObjectLockMode::Governance, legal_hold);
        let backend = Arc::new(locked_mock(&lock).with_lock_fault(fault));
        let mut engine = engine(
            config(dir.path(), &tenant, "telemetry-raw", &input),
            Some(lock),
            &backend,
        )
        .await;
        let out = engine.process_once(SourceType::File).await.unwrap();
        assert_eq!(out.len(), 1, "[{result}] {out:?}");
        assert!(
            !out[0].committed,
            "[{result}] the batch must not commit: {out:?}"
        );
        assert!(committed(&engine).await.is_empty(), "[{result}]");
        let failed = row(&engine, &out[0].batch_id).await;
        assert_eq!(failed.state, BatchState::ReplayRequired, "[{result}]");
        assert_says(
            &failed.error_message.unwrap_or_default(),
            &[
                "is not under the requested retention",
                found,
                "the batch was marked failed and will be replayed",
                "check the bucket's Object Lock configuration and the credentials' \
                 s3:PutObjectRetention permission",
            ],
        );
        if result == "missing" {
            // A missing s3:GetObjectRetention reads as a missing lock: S3
            // returns the headers only to a caller holding it (review of
            // #708).
            assert_says(
                &row(&engine, &out[0].batch_id)
                    .await
                    .error_message
                    .unwrap_or_default(),
                &["s3:GetObjectRetention"],
            );
        }
        assert_eq!(
            checks(&tenant, result),
            1,
            "[{result}] counted under its own result"
        );
        assert_eq!(verification_failures(&tenant), 1, "[{result}]");
    }
}

/// The range is handed back, not skipped: once the store keeps the lock, the
/// next pass archives the same range. (A shortened lock is the case a replay
/// can clear by itself: the reused object is still locked.)
#[tokio::test]
async fn a_failed_check_hands_the_range_back_for_the_replay() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-replay";
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock).with_lock_fault(LockFault::ShiftSeconds(-1)));
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-raw", &input),
        Some(lock),
        &backend,
    )
    .await;

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed);
    backend.set_lock_fault(None);
    let second = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(second.len(), 1, "{second:?}");
    assert!(second[0].committed, "{second:?}");
    let rows = committed(&engine).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].progress_start,
        row(&engine, &first[0].batch_id).await.progress_start,
        "the replay archived the range the failed batch held"
    );
}

/// A store that dropped the lock leaves an unlocked object at the derived
/// key. Before #709 a replay of the same bytes reused it and failed again on
/// every pass, so the range stalled until an operator acted. Now the replay
/// sees the missing lock before it commits to reuse, writes the batch as a
/// fresh object under its attempt key, locked as configured, verifies that
/// lock on the version it wrote, and commits; the unlocked object is left as
/// it is.
#[tokio::test]
async fn a_replay_that_finds_an_unlocked_object_writes_a_fresh_locked_one_and_commits() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-reuse-missing";
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock).with_lock_fault(LockFault::Ignored));
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-raw", &input),
        Some(lock),
        &backend,
    )
    .await;
    let unlocked_before = decisions(tenant, "fallback_unlocked");

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed);
    let derived = row(&engine, &first[0].batch_id).await.object_uri.unwrap();
    let old = Snapshot::of(&backend, &derived).await;
    assert_eq!(old.retention, StoredRetention::default(), "stored unlocked");
    backend.set_lock_fault(None);

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(second.len(), 1, "{second:?}");
    assert!(second[0].committed, "the replay commits: {second:?}");
    assert_fell_back(&engine, &backend, &second[0], &derived, &old).await;
    assert_eq!(
        decisions(tenant, "fallback_unlocked"),
        unlocked_before + 1,
        "counted as a fallback from an unlocked object"
    );
    assert_eq!(decisions(tenant, "reused"), 0, "nothing was reused");
    assert_eq!(
        checks(tenant, "missing"),
        1,
        "only the first attempt's own object failed a check; the replay's \
         look at the old object is a key decision, not a failed check"
    );
    assert_eq!(checks(tenant, "ok"), 2, "the fresh object and its manifest");
}

#[tokio::test]
async fn the_retention_is_read_on_the_exact_version_written() {
    // Another writer re-puts every object WITHOUT a lock right after it
    // lands. The engine's version is locked; the key's current one is not.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-exact-version";
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock).with_shadow_writes());
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-raw", &input),
        Some(lock),
        &backend,
    )
    .await;

    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    let uri = out[0].object_uri.clone().unwrap();
    assert_eq!(
        backend.stored_retention(&uri, None),
        StoredRetention::default(),
        "the key's current version is the unlocked one"
    );
    assert_eq!(
        checks(tenant, "ok"),
        2,
        "the version the engine wrote was checked, and it is locked"
    );
}

// ---- Replay keys (#626) -----------------------------------------------------

#[tokio::test]
async fn a_reused_object_still_locked_passes_and_the_new_manifest_is_checked_strictly() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-reuse";
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock).with_throttled_manifest_puts(1));
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-raw", &input),
        Some(lock),
        &backend,
    )
    .await;

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(
        !first[0].committed,
        "the throttled manifest fails the first attempt"
    );
    // The store now drops the lock: the reused object keeps the lock it got
    // the first time, the new manifest gets none.
    backend.set_lock_fault(Some(LockFault::Ignored));
    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(
        !second[0].committed,
        "the reusing attempt's manifest is unlocked"
    );
    let failed = row(&engine, &second[0].batch_id).await;
    assert_says(
        &failed.error_message.unwrap_or_default(),
        &[
            "manifest",
            "is not under the requested retention",
            "found no retention",
        ],
    );
    assert_eq!(checks(tenant, "ok"), 1, "the reused object passed");
    assert_eq!(checks(tenant, "missing"), 1, "the new manifest did not");

    backend.set_lock_fault(None);
    let third = engine.process_once(SourceType::File).await.unwrap();
    assert!(third[0].committed, "{third:?}");
    assert_eq!(
        third[0].object_uri,
        row(&engine, &first[0].batch_id).await.object_uri,
        "the object was reused"
    );
}

/// A reused object whose lock has ended (#709): before, the replay reused it
/// and failed on every pass; now it falls back to a fresh locked object
/// under the attempt key and commits, and the old object is untouched.
#[tokio::test]
async fn a_reused_object_whose_lock_has_ended_falls_back_to_a_fresh_locked_object() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-reuse-elapsed";
    let lock = governance();
    let (mut engine, backend, derived) = first_attempt_leaves_the_derived_object(
        dir.path(),
        tenant,
        &input,
        &lock,
        locked_mock(&lock).with_throttled_manifest_puts(1),
    )
    .await;
    // The lock it got has ended: a governance retention a minute in the past.
    backend.set_stored_retention(
        &derived,
        None,
        StoredRetention {
            mode: Some("GOVERNANCE".into()),
            retain_until_unix_secs: Some(unix_now() - 60),
            legal_hold: Some(false),
        },
    );
    let old = Snapshot::of(&backend, &derived).await;
    let elapsed_before = decisions(tenant, "fallback_lock_elapsed");

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(second.len(), 1, "{second:?}");
    assert!(second[0].committed, "the replay commits: {second:?}");
    assert_fell_back(&engine, &backend, &second[0], &derived, &old).await;
    assert_eq!(
        decisions(tenant, "fallback_lock_elapsed"),
        elapsed_before + 1
    );
    assert_eq!(decisions(tenant, "reused"), 0);
    assert_eq!(checks(tenant, "too_short"), 0, "no check failed");
    assert_eq!(checks(tenant, "ok"), 2, "the fresh object and its manifest");
}

/// Decision 5 of #709: a reused object locked in the OTHER mode falls back
/// too, in both directions. The committed object must be under the mode the
/// profile configures; a compliance lock where governance is configured is
/// stronger, but it is not the profile the operator chose.
#[tokio::test]
async fn a_reused_object_locked_in_the_other_mode_falls_back_to_the_configured_mode() {
    for (configured, found) in [
        (ObjectLockMode::Governance, "COMPLIANCE"),
        (ObjectLockMode::Compliance, "GOVERNANCE"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 3);
        let tenant = format!("lock-reuse-mode-{found}");
        let lock = lock(configured, false);
        let (mut engine, backend, derived) = first_attempt_leaves_the_derived_object(
            dir.path(),
            &tenant,
            &input,
            &lock,
            locked_mock(&lock).with_throttled_manifest_puts(1),
        )
        .await;
        backend.set_stored_retention(
            &derived,
            None,
            StoredRetention {
                mode: Some(found.into()),
                retain_until_unix_secs: Some(unix_now() + 86_400),
                legal_hold: Some(false),
            },
        );
        let old = Snapshot::of(&backend, &derived).await;

        let second = engine.process_once(SourceType::File).await.unwrap();
        assert!(second[0].committed, "[{found}] {second:?}");
        assert_fell_back(&engine, &backend, &second[0], &derived, &old).await;
        let landed = second[0].object_uri.clone().unwrap();
        assert_eq!(
            backend.stored_retention(&landed, None).mode.as_deref(),
            Some(configured.as_s3()),
            "[{found}] the fresh object is under the configured mode"
        );
        assert_eq!(decisions(&tenant, "fallback_lock_mode"), 1, "[{found}]");
        assert_eq!(checks(&tenant, "mode_mismatch"), 0, "[{found}]");
    }
}

/// A reused object whose retention cannot be read (a store error) is NOT a
/// reason to fall back (#709): falling back on errors would write a second
/// object for every replay during an outage. The batch fails at the reuse
/// decision, before writing anything, and says why.
#[tokio::test]
async fn a_retention_read_that_fails_at_the_reuse_decision_writes_nothing_and_does_not_fall_back() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-reuse-store-error";
    let lock = governance();
    // Every retention read fails: attempt 1 writes its object and manifest
    // under the derived key and fails at the check before the commit.
    let backend = Arc::new(
        locked_mock(&lock).with_retention_error("mock head_object: ServiceUnavailable (http 503)"),
    );
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-raw", &input),
        Some(lock),
        &backend,
    )
    .await;
    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed, "{first:?}");
    let derived = row(&engine, &first[0].batch_id).await.object_uri.unwrap();
    let old = Snapshot::of(&backend, &derived).await;
    let error_before = checks(tenant, "error");

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(!second[0].committed, "{second:?}");
    let failed = row(&engine, &second[0].batch_id).await;
    assert_eq!(failed.state, BatchState::ReplayRequired);
    assert!(
        failed.object_uri.is_none() && failed.manifest_uri.is_none(),
        "attempt 2 wrote no object and no manifest: {failed:?}"
    );
    assert_says(
        &failed.error_message.unwrap_or_default(),
        &[
            &derived,
            "could not be read",
            "it was neither reused nor replaced by a fresh object",
            "no object or manifest was written",
            "the batch was marked failed and will be replayed",
            "s3:GetObjectRetention",
        ],
    );
    for decision in FALLBACKS.iter().chain(["reused", "lock_withheld"].iter()) {
        assert_eq!(decisions(tenant, decision), 0, "no {decision}");
    }
    assert_eq!(checks(tenant, "error"), error_before + 1);
    assert_eq!(Snapshot::of(&backend, &derived).await, old);
}

/// Review of #757: a fallback whose upload never landed is not an extra
/// object. The next replay, inside the cooldown, finds the attempt key
/// absent and writes a fresh object at once instead of withholding.
#[tokio::test]
async fn a_fallback_whose_upload_failed_is_retried_at_once_not_withheld() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-fallback-put-failed";
    let lock = governance();
    let (mut engine, backend, derived) = first_attempt_leaves_the_derived_object(
        dir.path(),
        tenant,
        &input,
        &lock,
        locked_mock(&lock).with_throttled_manifest_puts(1),
    )
    .await;
    backend.set_stored_retention(&derived, None, StoredRetention::default());
    let old = Snapshot::of(&backend, &derived).await;
    // Every object put fails; the derived key is never put again.
    let ext = derived
        .rsplit_once('/')
        .unwrap()
        .1
        .split_once('.')
        .unwrap()
        .1;
    backend.set_failing_put_suffix(Some(&format!(".{ext}")));

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(!second[0].committed, "{second:?}");
    assert_eq!(decisions(tenant, "fallback_unlocked"), 1);

    backend.set_failing_put_suffix(None);
    let third = engine.process_once(SourceType::File).await.unwrap();
    assert!(third[0].committed, "{third:?}");
    assert_eq!(decisions(tenant, "lock_withheld"), 0, "not withheld");
    assert_eq!(decisions(tenant, "fallback_unlocked"), 2);
    assert_fell_back(&engine, &backend, &third[0], &derived, &old).await;
}

/// Review of #757: credentials that lost s3:GetObjectRetention get a HEAD
/// with no lock headers, which reads as an unlocked object. That must not
/// authorize a fresh object: the absence is confirmed with a request the
/// store refuses, and a refusal fails the batch with nothing written, on
/// every replay, rather than writing an object per cooldown window.
#[tokio::test]
async fn a_lost_lock_read_permission_never_writes_a_fallback_object() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-reuse-denied";
    let lock = governance();
    let (mut engine, backend, derived) = first_attempt_leaves_the_derived_object(
        dir.path(),
        tenant,
        &input,
        &lock,
        locked_mock(&lock)
            .without_lock_read_permission()
            .with_throttled_manifest_puts(1),
    )
    .await;
    // The object IS locked; the engine just cannot see it.
    assert_eq!(
        backend.stored_retention(&derived, None).mode.as_deref(),
        Some("GOVERNANCE")
    );
    engine.set_lock_fallback_cooldown(Duration::ZERO);
    let error_before = checks(tenant, "error");

    for attempt in 2..=3 {
        let out = engine.process_once(SourceType::File).await.unwrap();
        assert!(!out[0].committed, "[{attempt}] {out:?}");
        let failed = row(&engine, &out[0].batch_id).await;
        assert!(
            failed.object_uri.is_none() && failed.manifest_uri.is_none(),
            "[{attempt}] nothing written: {failed:?}"
        );
        assert_says(
            &failed.error_message.unwrap_or_default(),
            &[
                &derived,
                "could not be read",
                "confirming that was refused or failed",
                "AccessDenied",
                "s3:GetObjectRetention",
                "it was neither reused nor replaced by a fresh object",
            ],
        );
    }
    for decision in FALLBACKS.iter().chain(["reused", "lock_withheld"].iter()) {
        assert_eq!(decisions(tenant, decision), 0, "no {decision}");
    }
    assert_eq!(checks(tenant, "error"), error_before + 2);
    assert_eq!(backend.version_count(&derived), 1);
}

/// The commit rule is unchanged by the fallback (#709): nothing commits
/// before the retention check passes on the object the manifest names. A
/// fresh object the store does not keep under the lock asked for (no lock,
/// or the other mode) fails its batch like any other write. And D8 bounds the
/// cost of such a store: the next replay finds that fresh object unusable
/// and writes nothing (`lock_withheld`) until the cooldown has passed, then
/// one more.
#[tokio::test]
async fn a_fallback_object_whose_retention_read_back_fails_does_not_commit() {
    for (case, fault) in [
        ("dropped", LockFault::Ignored),
        ("mode", LockFault::Mode(ObjectLockMode::Compliance)),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 3);
        let tenant = format!("lock-fallback-readback-{case}");
        let lock = governance();
        let (mut engine, backend, derived) = first_attempt_leaves_the_derived_object(
            dir.path(),
            &tenant,
            &input,
            &lock,
            locked_mock(&lock).with_throttled_manifest_puts(1),
        )
        .await;
        // The old object is unlocked, so the replay falls back; the store
        // then mishandles the fresh object's lock.
        backend.set_stored_retention(&derived, None, StoredRetention::default());
        let old = Snapshot::of(&backend, &derived).await;
        backend.set_lock_fault(Some(fault));

        let second = engine.process_once(SourceType::File).await.unwrap();
        assert!(!second[0].committed, "[{case}] {second:?}");
        assert!(committed(&engine).await.is_empty(), "[{case}]");
        let failed = row(&engine, &second[0].batch_id).await;
        assert_eq!(failed.state, BatchState::ReplayRequired, "[{case}]");
        let fresh = failed.object_uri.clone().unwrap();
        assert!(fresh.contains(&second[0].batch_id), "[{case}] fell back");
        assert_says(
            &failed.error_message.unwrap_or_default(),
            &[&fresh, "is not under the requested retention"],
        );
        assert_eq!(decisions(&tenant, "fallback_unlocked"), 1, "[{case}]");

        // The next replay finds the fresh object unusable too, moments
        // later: it writes nothing.
        let third = engine.process_once(SourceType::File).await.unwrap();
        assert!(!third[0].committed, "[{case}] {third:?}");
        let withheld = row(&engine, &third[0].batch_id).await;
        assert!(
            withheld.object_uri.is_none(),
            "[{case}] nothing written: {withheld:?}"
        );
        assert_says(
            &withheld.error_message.unwrap_or_default(),
            &[
                &derived,
                &fresh,
                "is not usable either",
                "nothing was written",
                "the batch was marked failed and will be replayed",
                "s3:PutObjectRetention",
            ],
        );
        assert_eq!(decisions(&tenant, "lock_withheld"), 1, "[{case}]");
        assert_eq!(decisions(&tenant, "fallback_unlocked"), 1, "[{case}]");

        // Once the cooldown has passed (zero here) and the store keeps the
        // lock again, one more fresh object is written, and it commits.
        engine.set_lock_fallback_cooldown(Duration::ZERO);
        backend.set_lock_fault(None);
        let fourth = engine.process_once(SourceType::File).await.unwrap();
        assert!(fourth[0].committed, "[{case}] {fourth:?}");
        assert_fell_back(&engine, &backend, &fourth[0], &derived, &old).await;
        assert_eq!(decisions(&tenant, "fallback_unlocked"), 2, "[{case}]");
        assert_eq!(Snapshot::of(&backend, &derived).await, old, "[{case}]");
    }
}

/// The restart variant (#709): a new engine over the same ledger and store
/// has no memory of earlier fallbacks, and the store still keeps no lock.
/// The replay falls back again, the fresh object fails its check, and
/// nothing commits.
#[tokio::test]
async fn a_fresh_object_that_is_not_locked_either_is_never_committed() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-fallback-restart";
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock).with_lock_fault(LockFault::Ignored));
    let cfg = config(dir.path(), tenant, "telemetry-raw", &input);
    let mut engine = engine(cfg.clone(), Some(lock.clone()), &backend).await;
    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed);
    let derived = row(&engine, &first[0].batch_id).await.object_uri.unwrap();
    drop(engine);

    let mut restarted = support::engine(cfg, Some(lock), &backend).await;
    restarted.recover().await.unwrap();
    let second = restarted.process_once(SourceType::File).await.unwrap();
    assert!(second.iter().all(|o| !o.committed), "{second:?}");
    assert!(committed(&restarted).await.is_empty());
    let fresh = second[0].object_uri.clone();
    let failed = row(&restarted, &second[0].batch_id).await;
    assert_eq!(failed.state, BatchState::ReplayRequired);
    assert_ne!(
        failed.object_uri.as_deref(),
        Some(derived.as_str()),
        "{fresh:?}"
    );
    assert_eq!(decisions(tenant, "fallback_unlocked"), 1);
    assert_eq!(
        decisions(tenant, "lock_withheld"),
        0,
        "a new process remembers nothing"
    );
    assert_eq!(backend.version_count(&derived), 1);
}

/// D8 of #709: the fallback object landed and its manifest was throttled.
/// The next replay reuses that fresh, locked object instead of writing a
/// third one.
#[tokio::test]
async fn a_replay_after_a_fallback_reuses_the_fresh_object_and_writes_no_third() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-fallback-reuse";
    let lock = governance();
    let (mut engine, backend, derived) = first_attempt_leaves_the_derived_object(
        dir.path(),
        tenant,
        &input,
        &lock,
        locked_mock(&lock).with_throttled_manifest_puts(2),
    )
    .await;
    backend.set_stored_retention(&derived, None, StoredRetention::default());
    let old = Snapshot::of(&backend, &derived).await;

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(!second[0].committed, "the second manifest is throttled too");
    let fresh = row(&engine, &second[0].batch_id).await.object_uri.unwrap();
    assert!(fresh.contains(&second[0].batch_id), "{fresh}");
    assert_eq!(decisions(tenant, "fallback_unlocked"), 1);
    let ok_before = checks(tenant, "ok");

    let third = engine.process_once(SourceType::File).await.unwrap();
    assert!(third[0].committed, "{third:?}");
    assert_eq!(
        third[0].object_uri.as_deref(),
        Some(fresh.as_str()),
        "the replay reused the fresh object"
    );
    assert_eq!(backend.version_count(&fresh), 1, "no second put of it");
    assert_eq!(decisions(tenant, "reused"), 1);
    assert_eq!(decisions(tenant, "fallback_unlocked"), 1, "no third object");
    assert_eq!(
        checks(tenant, "ok"),
        ok_before + 2,
        "the reused fresh object was read again before the commit, and the manifest"
    );
    assert_eq!(Snapshot::of(&backend, &derived).await, old);
}

// ---- Multipart --------------------------------------------------------------

#[tokio::test]
async fn a_multipart_object_carries_the_lock_its_create_asked_for() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 400);
    let tenant = "lock-multipart";
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock));
    let mut cfg = config(dir.path(), tenant, "telemetry-raw", &input);
    cfg.compression.kind = vtop_core::types::CompressionType::None;
    cfg.upload.multipart_part_size_bytes = 1024;
    cfg.upload.multipart_threshold_bytes = 1024;
    let mut engine = engine(cfg, Some(lock), &backend).await;

    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    assert!(
        backend.multipart_parts_uploaded() > 1,
        "the object went through multipart"
    );
    let uri = out[0].object_uri.clone().unwrap();
    assert_eq!(
        backend.stored_retention(&uri, None).mode.as_deref(),
        Some("GOVERNANCE")
    );
    assert_eq!(checks(tenant, "ok"), 2);
}

/// A multipart-sized batch takes the fallback too (#709): the upload goes to
/// the attempt key under the lock its create asked for.
#[tokio::test]
async fn a_multipart_batch_that_falls_back_is_locked_on_its_attempt_key() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 400);
    let tenant = "lock-multipart-fallback";
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock).with_throttled_manifest_puts(1));
    let mut cfg = config(dir.path(), tenant, "telemetry-raw", &input);
    cfg.compression.kind = vtop_core::types::CompressionType::None;
    cfg.upload.multipart_part_size_bytes = 1024;
    cfg.upload.multipart_threshold_bytes = 1024;
    let mut engine = engine(cfg, Some(lock), &backend).await;
    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed);
    let derived = row(&engine, &first[0].batch_id).await.object_uri.unwrap();
    backend.set_stored_retention(&derived, None, StoredRetention::default());
    let old = Snapshot::of(&backend, &derived).await;
    let parts_before = backend.multipart_parts_uploaded();

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(second[0].committed, "{second:?}");
    assert!(
        backend.multipart_parts_uploaded() > parts_before + 1,
        "the fresh object went through multipart"
    );
    assert_fell_back(&engine, &backend, &second[0], &derived, &old).await;
}

// ---- Redaction --------------------------------------------------------------

#[tokio::test]
async fn a_signed_url_in_a_store_error_never_reaches_the_ledger_or_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 2);
    let tenant = "lock-redaction";
    let lock = governance();
    let signed = "https://telemetry-raw.s3.amazonaws.com/x/obj.raw?versionId=v1\
                  &X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAEXAMPLE%2F20261114\
                  &X-Amz-Signature=deadbeefcafe";
    let backend = Arc::new(
        locked_mock(&lock).with_retention_error(format!("head_object {signed}: connection reset")),
    );
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-raw", &input),
        Some(lock),
        &backend,
    )
    .await;
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(!out[0].committed);
    let message = row(&engine, &out[0].batch_id)
        .await
        .error_message
        .unwrap_or_default();
    assert!(message.contains("could not be read"), "{message}");
    for secret in [
        "X-Amz-Signature",
        "deadbeefcafe",
        "X-Amz-Credential",
        "AKIAEXAMPLE",
    ] {
        assert!(
            !message.contains(secret),
            "the ledger holds {secret}: {message}"
        );
    }
    assert_eq!(checks(tenant, "error"), 1);
}

// ---- Scrub --check-retention ------------------------------------------------

async fn three_committed(
    tenant: &str,
    retain: Duration,
) -> (tempfile::TempDir, Engine, Arc<MockBackend>) {
    three_committed_with(tenant, retain, false).await
}

/// Three committed batches, with `manifest.verification_receipts` set when
/// `receipts` is: each receipt then goes through the manifest's write path
/// and carries the same retention.
async fn three_committed_with(
    tenant: &str,
    retain: Duration,
    receipts: bool,
) -> (tempfile::TempDir, Engine, Arc<MockBackend>) {
    let dir = tempfile::tempdir().unwrap();
    let inputs: Vec<PathBuf> = (0..3)
        .map(|i| dir.path().join(format!("f{i}.log")))
        .collect();
    for (i, input) in inputs.iter().enumerate() {
        write_lines(input, &format!("file{i}"), 2);
    }
    let lock = governance();
    let backend = Arc::new(MockBackend::new().with_object_lock(
        ObjectLockPolicy::with_retention_for_tests(ObjectLockMode::Governance, retain, false),
    ));
    let mut cfg = config(dir.path(), tenant, "telemetry-raw", &inputs[0]);
    if let Some(file) = cfg.sources.file.as_mut() {
        file.paths = inputs
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
    }
    cfg.manifest.verification_receipts = receipts;
    let mut engine = engine(cfg, Some(lock), &backend).await;
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 3, "{out:?}");
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    (dir, engine, backend)
}

fn with_retention() -> ScrubOptions {
    ScrubOptions {
        check_retention: Some(ObjectLockMode::Governance),
        ..ScrubOptions::default()
    }
}

#[tokio::test]
async fn scrub_check_retention_reports_locked_missing_and_mode_mismatch() {
    let (_dir, engine, backend) =
        three_committed("scrub-retention", Duration::from_secs(30 * 86_400)).await;
    let rows = committed(&engine).await;
    // One stays locked; one lost its retention (a principal allowed to
    // bypass governance removed it); one is under another mode.
    backend.set_stored_retention(
        rows[1].object_uri.as_deref().unwrap(),
        None,
        StoredRetention::default(),
    );
    let mut compliance = backend.stored_retention(rows[2].object_uri.as_deref().unwrap(), None);
    compliance.mode = Some("COMPLIANCE".into());
    backend.set_stored_retention(rows[2].object_uri.as_deref().unwrap(), None, compliance);

    let all = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let report = engine.scrub(all.clone(), &with_retention()).await.unwrap();
    assert!(!report.passed, "{report:#?}");
    let count = |r: RetentionResult| report.retention.get(r.as_str()).copied();
    assert_eq!(count(RetentionResult::Locked), Some(1), "{report:#?}");
    assert_eq!(count(RetentionResult::Missing), Some(1), "{report:#?}");
    assert_eq!(count(RetentionResult::ModeMismatch), Some(1), "{report:#?}");
    assert_eq!(count(RetentionResult::Elapsed), Some(0), "{report:#?}");
    let mut found: Vec<(String, Option<RetentionResult>)> = report
        .findings
        .iter()
        .map(|f| (f.batch_id.clone(), f.retention))
        .collect();
    found.sort();
    assert_eq!(
        found,
        vec![
            (rows[1].batch_id.clone(), Some(RetentionResult::Missing)),
            (
                rows[2].batch_id.clone(),
                Some(RetentionResult::ModeMismatch)
            ),
        ]
    );
    let text = report.prometheus_text(None, 1_000);
    assert!(
        text.contains("vtop_scrub_retention_total{result=\"missing\"} 1"),
        "{text}"
    );
    assert!(
        text.contains("vtop_scrub_retention_total{result=\"locked\"} 1"),
        "{text}"
    );

    // Without the flag: no retention is read, and the content passes.
    let plain = engine.scrub(all, &ScrubOptions::default()).await.unwrap();
    assert!(plain.passed, "{plain:#?}");
    assert!(plain.retention.is_empty());
}

#[tokio::test]
async fn scrub_check_retention_counts_an_ended_lock_without_failing() {
    let (_dir, engine, _backend) = three_committed("scrub-elapsed", Duration::from_secs(1)).await;
    tokio::time::sleep(Duration::from_millis(3_100)).await;
    let all = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let report = engine.scrub(all, &with_retention()).await.unwrap();
    assert!(
        report.passed,
        "an ended retention is normal for an old object: {report:#?}"
    );
    assert_eq!(
        report
            .retention
            .get(RetentionResult::Elapsed.as_str())
            .copied(),
        Some(3),
        "{report:#?}"
    );
}

// ---- Scrub --check-retention over verification receipts (#710) -------------

fn with_receipt_retention() -> ScrubOptions {
    ScrubOptions {
        check_receipts: true,
        ..with_retention()
    }
}

fn receipt_of(row: &vtop_state::BatchRecord) -> String {
    vtop_core::partitioning::receipt_uri(row.manifest_uri.as_deref().unwrap()).unwrap()
}

/// The `vtop_scrub_receipt_retention_total` samples of a metrics file, in
/// the order written.
fn receipt_retention_samples(text: &str) -> Vec<&str> {
    text.lines()
        .filter(|l| l.starts_with("vtop_scrub_receipt_retention_total{"))
        .collect()
}

#[tokio::test]
async fn scrub_check_retention_reads_each_receipt_and_reports_it_apart_from_the_batch() {
    let (_dir, engine, backend) = three_committed_with(
        "scrub-receipt-retention",
        Duration::from_secs(30 * 86_400),
        true,
    )
    .await;
    let rows = committed(&engine).await;
    // The objects and manifests all stay locked. One receipt stays locked;
    // one lost its retention (a principal allowed to bypass governance
    // removed it); one is under another mode.
    backend.set_stored_retention(&receipt_of(&rows[1]), None, StoredRetention::default());
    let mut compliance = backend.stored_retention(&receipt_of(&rows[2]), None);
    compliance.mode = Some("COMPLIANCE".into());
    backend.set_stored_retention(&receipt_of(&rows[2]), None, compliance);

    let all = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let report = engine
        .scrub(all.clone(), &with_receipt_retention())
        .await
        .unwrap();
    assert!(
        !report.passed,
        "a receipt that can be deleted fails the run: {report:#?}"
    );
    // The batch's own retention keeps #708's meaning: object and manifest.
    let batch = |r: RetentionResult| report.retention.get(r.as_str()).copied();
    assert_eq!(batch(RetentionResult::Locked), Some(3), "{report:#?}");
    assert_eq!(batch(RetentionResult::Missing), Some(0), "{report:#?}");
    let receipt = |r: RetentionResult| report.receipt_retention.get(r.as_str()).copied();
    assert_eq!(receipt(RetentionResult::Locked), Some(1), "{report:#?}");
    assert_eq!(receipt(RetentionResult::Missing), Some(1), "{report:#?}");
    assert_eq!(
        receipt(RetentionResult::ModeMismatch),
        Some(1),
        "{report:#?}"
    );
    assert_eq!(receipt(RetentionResult::Elapsed), Some(0), "{report:#?}");
    assert_eq!(
        report.receipts.get("ok").copied(),
        Some(3),
        "the receipts themselves are sound: {report:#?}"
    );

    let mut found: Vec<_> = report
        .findings
        .iter()
        .map(|f| {
            (
                f.batch_id.clone(),
                f.receipt_retention,
                f.retention,
                f.receipt,
            )
        })
        .collect();
    found.sort();
    assert_eq!(
        found,
        vec![
            (
                rows[1].batch_id.clone(),
                Some(RetentionResult::Missing),
                None,
                None
            ),
            (
                rows[2].batch_id.clone(),
                Some(RetentionResult::ModeMismatch),
                None,
                None
            ),
        ],
        "only the receipts are findings, each as a receipt retention finding"
    );
    for finding in &report.findings {
        let ruri = receipt_of(
            rows.iter()
                .find(|r| r.batch_id == finding.batch_id)
                .unwrap(),
        );
        let (what, remedy): (&str, &[&str]) =
            if finding.receipt_retention == Some(RetentionResult::Missing) {
                (
                    "carries no retention",
                    &[
                        "can be deleted or overwritten",
                        "written before upload.object_lock was set",
                        "verification_receipt_failed",
                        "re-apply the retention with PutObjectRetention",
                        "check who could bypass it",
                        // A HEAD without the permission reads as no lock
                        // (review of #749).
                        "s3:GetObjectRetention",
                    ],
                )
            } else {
                (
                    "is under another retention mode than the configured one",
                    &[
                        "nothing to do if upload.object_lock.mode was changed on purpose",
                        "find who wrote or re-locked it",
                    ],
                )
            };
        assert_says(
            &finding.detail,
            &[
                // What happened.
                &format!("verification receipt {ruri} {what}"),
                "expected governance",
                // What the scrub did.
                "the scrub reported it and changed nothing",
                "the batch's object and manifest are checked separately",
            ],
        );
        // What to do.
        assert_says(&finding.detail, remedy);
    }

    let lines = report.lines();
    assert!(
        lines.contains(&"receipt retention: elapsed=0 locked=1 missing=1 mode_mismatch=1".into()),
        "its own line, so an operator can tell a receipt from an object: {lines:#?}"
    );
    assert!(
        lines.contains(&"retention: elapsed=0 locked=3 missing=0 mode_mismatch=0".into()),
        "{lines:#?}"
    );
    assert!(
        lines.iter().any(|l| l.starts_with(&format!(
            "FAIL receipt retention missing {} ",
            rows[1].batch_id
        ))),
        "{lines:#?}"
    );
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["receipt_retention"]["missing"], 1, "{json:#}");
    let json_findings = json["findings"].as_array().unwrap();
    assert!(
        json_findings
            .iter()
            .any(|f| f["receipt_retention"] == "missing"
                && f["batch_id"] == rows[1].batch_id.as_str()),
        "{json:#}"
    );

    // Its own family, with the bounded result set of
    // vtop_scrub_retention_total; that family is unchanged.
    let first = report.prometheus_text(None, 1_000);
    assert_eq!(
        receipt_retention_samples(&first),
        vec![
            "vtop_scrub_receipt_retention_total{result=\"elapsed\"} 0",
            "vtop_scrub_receipt_retention_total{result=\"locked\"} 1",
            "vtop_scrub_receipt_retention_total{result=\"missing\"} 1",
            "vtop_scrub_receipt_retention_total{result=\"mode_mismatch\"} 1",
        ],
        "{first}"
    );
    assert!(
        first.contains("# TYPE vtop_scrub_receipt_retention_total counter\n"),
        "{first}"
    );
    assert!(
        first.contains("vtop_scrub_retention_total{result=\"locked\"} 3\n")
            && first.contains("vtop_scrub_retention_total{result=\"missing\"} 0\n"),
        "{first}"
    );
    assert!(
        first.contains("vtop_scrub_last_run_findings 2\n"),
        "{first}"
    );

    // A run that reads no retention carries the totals forward unchanged; a
    // run that does adds to them.
    let plain = engine
        .scrub(
            all.clone(),
            &ScrubOptions {
                check_receipts: true,
                ..ScrubOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(plain.passed, "{plain:#?}");
    assert!(plain.receipt_retention.is_empty(), "{plain:#?}");
    let second = plain.prometheus_text(Some(&first), 2_000);
    assert_eq!(
        receipt_retention_samples(&second),
        receipt_retention_samples(&first),
        "{second}"
    );
    let again = engine.scrub(all, &with_receipt_retention()).await.unwrap();
    let third = again.prometheus_text(Some(&second), 3_000);
    assert_eq!(
        receipt_retention_samples(&third),
        vec![
            "vtop_scrub_receipt_retention_total{result=\"elapsed\"} 0",
            "vtop_scrub_receipt_retention_total{result=\"locked\"} 2",
            "vtop_scrub_receipt_retention_total{result=\"missing\"} 2",
            "vtop_scrub_receipt_retention_total{result=\"mode_mismatch\"} 2",
        ],
        "{third}"
    );
}

#[tokio::test]
async fn a_receipt_whose_lock_has_ended_is_counted_not_reported() {
    let (_dir, engine, backend) = three_committed_with(
        "scrub-receipt-elapsed",
        Duration::from_secs(30 * 86_400),
        true,
    )
    .await;
    let rows = committed(&engine).await;
    let mut ended = backend.stored_retention(&receipt_of(&rows[0]), None);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    ended.retain_until_unix_secs = Some(now - 60);
    backend.set_stored_retention(&receipt_of(&rows[0]), None, ended);
    let all = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let report = engine.scrub(all, &with_receipt_retention()).await.unwrap();
    assert!(
        report.passed,
        "an ended retention is normal for an old receipt: {report:#?}"
    );
    let receipt = |r: RetentionResult| report.receipt_retention.get(r.as_str()).copied();
    assert_eq!(receipt(RetentionResult::Elapsed), Some(1), "{report:#?}");
    assert_eq!(receipt(RetentionResult::Locked), Some(2), "{report:#?}");
}

/// The receipt is read at its key, not pinned (#710): a later version
/// written there without a lock is the one judged.
#[tokio::test]
async fn receipt_retention_is_read_at_the_current_key() {
    let (_dir, engine, backend) = three_committed_with(
        "scrub-receipt-current",
        Duration::from_secs(30 * 86_400),
        true,
    )
    .await;
    let rows = committed(&engine).await;
    let ruri = receipt_of(&rows[0]);
    let same = backend.get_object(&ruri).await.unwrap();
    backend.add_unlocked_version(&ruri, same);
    let all = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let report = engine.scrub(all, &with_receipt_retention()).await.unwrap();
    let found: Vec<_> = report
        .findings
        .iter()
        .map(|f| (f.batch_id.clone(), f.receipt, f.receipt_retention))
        .collect();
    assert_eq!(
        found,
        vec![(
            rows[0].batch_id.clone(),
            None,
            Some(RetentionResult::Missing)
        )],
        "the same receipt, re-written unlocked: {report:#?}"
    );
}

/// A receipt that exists is read for its lock whatever its own check found
/// (#710): a tampered receipt that is also unlocked has both findings, so
/// each is fixed on its own.
#[tokio::test]
async fn a_tampered_receipt_without_a_lock_has_both_findings() {
    let (_dir, engine, backend) = three_committed_with(
        "scrub-receipt-tampered",
        Duration::from_secs(30 * 86_400),
        true,
    )
    .await;
    let rows = committed(&engine).await;
    backend.add_unlocked_version(&receipt_of(&rows[2]), b"{ not a receipt".to_vec());
    let all = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let report = engine.scrub(all, &with_receipt_retention()).await.unwrap();
    assert!(!report.passed, "{report:#?}");
    assert_eq!(report.receipts.get("invalid").copied(), Some(1));
    assert_eq!(
        report
            .receipt_retention
            .get(RetentionResult::Missing.as_str())
            .copied(),
        Some(1),
        "{report:#?}"
    );
    let mut found: Vec<_> = report
        .findings
        .iter()
        .map(|f| (f.batch_id.clone(), f.receipt, f.receipt_retention))
        .collect();
    found.sort();
    assert_eq!(
        found,
        vec![
            (
                rows[2].batch_id.clone(),
                None,
                Some(RetentionResult::Missing)
            ),
            (
                rows[2].batch_id.clone(),
                Some(scrub::ReceiptResult::Invalid),
                None
            ),
        ],
        "{report:#?}"
    );
}

/// Review of #749: a receipt that is already a finding and whose retention
/// cannot be read keeps its result, and its detail does not claim it was
/// reported unreadable.
#[tokio::test]
async fn a_tampered_receipt_whose_retention_cannot_be_read_stays_invalid() {
    let (_dir, engine, backend) = three_committed_with(
        "scrub-receipt-tampered-unreadable",
        Duration::from_secs(30 * 86_400),
        true,
    )
    .await;
    let rows = committed(&engine).await;
    let ruri = receipt_of(&rows[2]);
    backend.add_unlocked_version(&ruri, b"{ not a receipt".to_vec());
    backend.set_retention_error(Some("head_object: connection reset".into()));
    backend.set_retention_error_only_for(Some(&ruri));
    let all = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let report = engine.scrub(all, &with_receipt_retention()).await.unwrap();
    assert_eq!(
        report.receipts.get("invalid").copied(),
        Some(1),
        "{report:#?}"
    );
    assert_eq!(
        report.receipts.get("unreadable").copied(),
        Some(0),
        "{report:#?}"
    );
    assert_eq!(
        report.receipt_retention.values().sum::<u64>(),
        2,
        "no verdict for the receipt that could not be read: {report:#?}"
    );
    let found: Vec<_> = report
        .findings
        .iter()
        .filter(|f| f.batch_id == rows[2].batch_id)
        .collect();
    assert_eq!(found.len(), 1, "{report:#?}");
    assert_eq!(found[0].receipt, Some(scrub::ReceiptResult::Invalid));
    assert_says(
        &found[0].detail,
        &[
            "does not parse",
            "its retention could not be read either",
            "connection reset",
            "the receipt stays reported invalid",
        ],
    );
    assert!(
        !found[0].detail.contains("reported unreadable"),
        "{}",
        found[0].detail
    );
}

/// Review of #749: a receipt that is another batch's (mismatched) is read
/// for its lock too.
#[tokio::test]
async fn a_mismatched_receipt_without_a_lock_has_both_findings() {
    let (_dir, engine, backend) = three_committed_with(
        "scrub-receipt-mismatched",
        Duration::from_secs(30 * 86_400),
        true,
    )
    .await;
    let rows = committed(&engine).await;
    let other = backend.get_object(&receipt_of(&rows[1])).await.unwrap();
    backend.add_unlocked_version(&receipt_of(&rows[0]), other);
    let all = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let before = backend.retention_reads().len();
    let report = engine.scrub(all, &with_receipt_retention()).await.unwrap();
    assert!(
        backend.retention_reads()[before..].contains(&receipt_of(&rows[0])),
        "the mismatched receipt's retention was not read"
    );
    let mut found: Vec<_> = report
        .findings
        .iter()
        .map(|f| (f.batch_id.clone(), f.receipt, f.receipt_retention))
        .collect();
    found.sort();
    assert_eq!(
        found,
        vec![
            (
                rows[0].batch_id.clone(),
                None,
                Some(RetentionResult::Missing)
            ),
            (
                rows[0].batch_id.clone(),
                Some(scrub::ReceiptResult::Mismatched),
                None
            ),
        ],
        "{report:#?}"
    );
}

#[tokio::test]
async fn a_missing_receipt_is_not_a_retention_finding() {
    let (_dir, engine, backend) =
        three_committed_with("scrub-receipt-gone", Duration::from_secs(30 * 86_400), true).await;
    let rows = committed(&engine).await;
    let gone = receipt_of(&rows[1]);
    backend.delete_object(&gone).await.unwrap();
    let all = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());

    let before = backend.retention_reads().len();
    let report = engine
        .scrub(all.clone(), &with_receipt_retention())
        .await
        .unwrap();
    assert!(
        report.passed,
        "a missing receipt is counted, not a finding: {report:#?}"
    );
    assert_eq!(report.receipts.get("missing").copied(), Some(1));
    let receipt = |r: RetentionResult| report.receipt_retention.get(r.as_str()).copied();
    assert_eq!(receipt(RetentionResult::Locked), Some(2), "{report:#?}");
    assert_eq!(
        receipt(RetentionResult::Missing),
        Some(0),
        "an absent receipt has no retention to judge: {report:#?}"
    );
    assert!(
        !backend.retention_reads()[before..].contains(&gone),
        "the scrub asked for the retention of a receipt it found absent"
    );

    // Required: the missing receipt is a receipt finding, and only that.
    let required = engine
        .scrub(
            all,
            &ScrubOptions {
                require_receipts: true,
                ..with_retention()
            },
        )
        .await
        .unwrap();
    let found: Vec<_> = required
        .findings
        .iter()
        .map(|f| (f.batch_id.clone(), f.receipt, f.receipt_retention))
        .collect();
    assert_eq!(
        found,
        vec![(
            rows[1].batch_id.clone(),
            Some(scrub::ReceiptResult::Missing),
            None
        )],
        "{required:#?}"
    );
    assert_eq!(
        required
            .receipt_retention
            .get(RetentionResult::Missing.as_str())
            .copied(),
        Some(0)
    );
}

#[tokio::test]
async fn receipts_are_read_for_retention_only_when_both_checks_run() {
    let (_dir, engine, backend) =
        three_committed_with("scrub-receipt-when", Duration::from_secs(30 * 86_400), true).await;
    let rows = committed(&engine).await;
    let receipts: Vec<String> = rows.iter().map(receipt_of).collect();
    let all = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let receipt_reads = |since: usize| -> Vec<String> {
        backend.retention_reads()[since..]
            .iter()
            .filter(|uri| uri.ends_with(".receipt.json"))
            .cloned()
            .collect()
    };

    // --check-retention without receipts being checked.
    let before = backend.retention_reads().len();
    let report = engine.scrub(all.clone(), &with_retention()).await.unwrap();
    assert!(report.passed, "{report:#?}");
    assert_eq!(receipt_reads(before), Vec::<String>::new());
    assert!(report.receipt_retention.is_empty(), "{report:#?}");
    assert!(
        receipt_retention_samples(&report.prometheus_text(None, 1)).is_empty(),
        "no family for a run that read no receipt"
    );

    // Receipts checked without --check-retention.
    let before = backend.retention_reads().len();
    let report = engine
        .scrub(
            all.clone(),
            &ScrubOptions {
                check_receipts: true,
                ..ScrubOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(report.passed, "{report:#?}");
    assert_eq!(report.receipts.get("ok").copied(), Some(3));
    assert_eq!(backend.retention_reads()[before..].len(), 0);
    assert!(report.receipt_retention.is_empty(), "{report:#?}");

    // Both, by the config's receipts and by --require-receipts: each receipt
    // is read once, at its key.
    for opts in [
        with_receipt_retention(),
        ScrubOptions {
            require_receipts: true,
            ..with_retention()
        },
    ] {
        let before = backend.retention_reads().len();
        let report = engine.scrub(all.clone(), &opts).await.unwrap();
        assert!(report.passed, "{report:#?}");
        let mut read = receipt_reads(before);
        read.sort();
        let mut want = receipts.clone();
        want.sort();
        assert_eq!(read, want);
        assert_eq!(
            report
                .receipt_retention
                .get(RetentionResult::Locked.as_str())
                .copied(),
            Some(3),
            "{report:#?}"
        );
    }
}

#[tokio::test]
async fn a_receipt_retention_the_store_cannot_answer_is_an_unreadable_receipt_redacted() {
    let (_dir, engine, backend) = three_committed_with(
        "scrub-receipt-unreadable",
        Duration::from_secs(30 * 86_400),
        true,
    )
    .await;
    let rows = committed(&engine).await;
    let ruri = receipt_of(&rows[0]);
    let signed = format!(
        "{}?versionId=v1&X-Amz-Algorithm=AWS4-HMAC-SHA256\
         &X-Amz-Credential=AKIAEXAMPLE%2F20261114&X-Amz-Signature=deadbeefcafe",
        ruri.replacen(
            "s3://telemetry-raw/",
            "https://telemetry-raw.s3.amazonaws.com/",
            1
        )
    );
    backend.set_retention_error(Some(format!("head_object {signed}: connection reset")));
    backend.set_retention_error_only_for(Some(".receipt.json"));
    let all = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let report = engine.scrub(all, &with_receipt_retention()).await.unwrap();
    assert!(
        !report.passed,
        "a scrub that could not look did not pass: {report:#?}"
    );
    // The receipt is unreadable, not the batch: its data and its own lock
    // were read and are fine.
    assert_eq!(report.results.get("ok").copied(), Some(3), "{report:#?}");
    assert_eq!(
        report
            .retention
            .get(RetentionResult::Locked.as_str())
            .copied(),
        Some(3),
        "{report:#?}"
    );
    assert!(
        report
            .findings
            .iter()
            .all(|f| f.result == scrub::ScrubResult::Ok && f.retention.is_none()),
        "{report:#?}"
    );
    assert_eq!(
        report.receipts.get("unreadable").copied(),
        Some(3),
        "{report:#?}"
    );
    assert!(
        report.receipt_retention.values().all(|n| *n == 0),
        "nothing was judged: {report:#?}"
    );
    let receipt_findings: Vec<_> = report
        .findings
        .iter()
        .filter(|f| f.receipt == Some(scrub::ReceiptResult::Unreadable))
        .collect();
    assert_eq!(receipt_findings.len(), 3, "{report:#?}");
    let first = receipt_findings
        .iter()
        .find(|f| f.batch_id == rows[0].batch_id)
        .unwrap();
    assert_says(
        &first.detail,
        &[
            // What happened.
            &format!("the retention of verification receipt {ruri} could not be read"),
            "connection reset",
            // What the scrub did.
            "the receipt is reported unreadable and the batch's own result is left alone",
            // What to do.
            "check the store's health and the scrub's read permission",
        ],
    );
    let everything = format!(
        "{}\n{}\n{}",
        report
            .findings
            .iter()
            .map(|f| f.detail.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
        report.lines().join("\n"),
        report.prometheus_text(None, 1)
    );
    for secret in [
        "X-Amz-Signature",
        "deadbeefcafe",
        "X-Amz-Credential",
        "AKIAEXAMPLE",
        "versionId=v1",
    ] {
        assert!(
            !everything.contains(secret),
            "the report holds {secret}: {everything}"
        );
    }
}

// ---- Review of #708 ---------------------------------------------------------

/// The startup check names the bucket the engine will write to: a literal
/// `upload.bucket` is sanitized (lower case, `.` and `_` to `-`) before use,
/// and checking the raw spelling would ask about a bucket nobody writes to.
#[tokio::test]
async fn startup_checks_the_bucket_name_the_batches_are_written_to() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 2);
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock));
    let mut engine = engine(
        config(dir.path(), "lock-sanitized", "Telemetry_Locked", &input),
        Some(lock),
        &backend,
    )
    .await;
    engine.preflight_object_lock().await.unwrap();
    assert_eq!(
        backend.object_lock_config_buckets(),
        vec!["telemetry-locked"]
    );
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    assert_eq!(
        backend.object_lock_config_calls(),
        1,
        "the first batch found the bucket already checked"
    );
}

/// A bucket check the store could not answer at startup is not a reason to
/// stay down: the engine starts, says so, and checks again before the first
/// upload to that bucket. Only a definite "not enabled" refuses startup.
#[tokio::test]
async fn a_failed_bucket_check_at_startup_is_retried_at_the_first_batch() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 2);
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock).with_lock_config_errors(1));
    let mut engine = engine(
        config(dir.path(), "lock-blip", "telemetry-raw", &input),
        Some(lock),
        &backend,
    )
    .await;
    engine
        .preflight_object_lock()
        .await
        .expect("a store blip does not keep the engine down");
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    assert_eq!(backend.object_lock_config_calls(), 2);
}

/// A backend that does not apply the configured lock refuses startup.
#[tokio::test]
async fn startup_refuses_a_backend_that_does_not_apply_the_configured_lock() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 2);
    let backend = Arc::new(MockBackend::new());
    let engine = engine(
        config(dir.path(), "lock-mismatch", "telemetry-raw", &input),
        Some(governance()),
        &backend,
    )
    .await;
    let err = engine
        .preflight_object_lock()
        .await
        .unwrap_err()
        .to_string();
    assert_says(
        &err,
        &[
            "does not apply it",
            "unlocked objects",
            "the engine did not start",
            "s3_native",
        ],
    );
}

/// A write that asked for no lock while the profile is set fails its batch,
/// and the message says what the replay will do.
#[tokio::test]
async fn a_write_that_asked_for_no_lock_fails_the_batch() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 2);
    let tenant = "lock-none-asked";
    let backend = Arc::new(MockBackend::new());
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-raw", &input),
        Some(governance()),
        &backend,
    )
    .await;
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(!out[0].committed);
    assert_says(
        &row(&engine, &out[0].batch_id)
            .await
            .error_message
            .unwrap_or_default(),
        &[
            "without asking for the retention upload.object_lock sets",
            "the batch was marked failed and will be replayed",
            "the replay checks the stored object itself",
        ],
    );
    assert_eq!(checks(tenant, "error"), 1);
}

/// A reused object without the legal hold the config asks for (the hold
/// was removed, or the object predates `legal_hold: true`). Before #709 the
/// replay failed on every pass until someone placed a hold; now it falls back
/// to a fresh object that carries the hold, and commits.
#[tokio::test]
async fn a_reused_object_without_the_legal_hold_falls_back_to_one_that_has_it() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 2);
    let tenant = "lock-reuse-hold";
    let lock = lock_with_hold();
    let (mut engine, backend, derived) = first_attempt_leaves_the_derived_object(
        dir.path(),
        tenant,
        &input,
        &lock,
        locked_mock(&lock).with_throttled_manifest_puts(1),
    )
    .await;
    let mut kept = backend.stored_retention(&derived, None);
    kept.legal_hold = Some(false);
    backend.set_stored_retention(&derived, None, kept);
    let old = Snapshot::of(&backend, &derived).await;

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(second[0].committed, "{second:?}");
    assert_fell_back(&engine, &backend, &second[0], &derived, &old).await;
    let landed = second[0].object_uri.clone().unwrap();
    assert_eq!(
        backend.stored_retention(&landed, None).legal_hold,
        Some(true)
    );
    assert_eq!(decisions(tenant, "fallback_hold_missing"), 1);
    assert_eq!(checks(tenant, "legal_hold_missing"), 0);
}

fn lock_with_hold() -> ObjectLockConfig {
    lock(ObjectLockMode::Governance, true)
}

/// The fallback values #709 adds to `vtop_object_key_decisions_total`.
const FALLBACKS: [&str; 4] = [
    "fallback_lock_elapsed",
    "fallback_unlocked",
    "fallback_lock_mode",
    "fallback_hold_missing",
];

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// The first attempt puts its object, locked, under the derived key and
/// fails at its manifest (the caller's mock throttles it), so the next pass replays the same
/// bytes and resolves to that object (#626). Returns the engine, the mock
/// and the derived key.
async fn first_attempt_leaves_the_derived_object(
    dir: &std::path::Path,
    tenant: &str,
    input: &std::path::Path,
    lock: &ObjectLockConfig,
    mock: MockBackend,
) -> (Engine, Arc<MockBackend>, String) {
    let backend = Arc::new(mock);
    let mut engine = engine(
        config(dir, tenant, "telemetry-raw", input),
        Some(lock.clone()),
        &backend,
    )
    .await;
    let first = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(first.len(), 1, "{first:?}");
    assert!(!first[0].committed, "the throttled manifest fails it");
    let derived = row(&engine, &first[0].batch_id)
        .await
        .object_uri
        .expect("the first attempt put its object");
    assert!(
        !derived.contains(&first[0].batch_id),
        "{derived} is the derived key, not the attempt key"
    );
    (engine, backend, derived)
}

/// What the store holds at a key: every version's bytes, and the current
/// version's retention. Equal before and after means untouched: nothing
/// written over it, deleted, or re-locked.
#[derive(Debug, PartialEq)]
struct Snapshot {
    versions: usize,
    bytes: Vec<u8>,
    retention: StoredRetention,
}

impl Snapshot {
    async fn of(backend: &MockBackend, uri: &str) -> Self {
        Self {
            versions: backend.version_count(uri),
            bytes: backend.get_object_bounded(uri, 1 << 20).await.unwrap(),
            retention: backend.stored_retention(uri, None),
        }
    }
}

/// The replay committed on a fresh object under its attempt key (#709):
/// locked in the configured mode until about `retain_days` from now, named
/// by the ledger row and the manifest, and the old object unchanged.
async fn assert_fell_back(
    engine: &Engine,
    backend: &MockBackend,
    outcome: &vtop_cli::BatchOutcome,
    derived: &str,
    old: &Snapshot,
) {
    let landed = outcome.object_uri.clone().unwrap();
    assert_ne!(landed, derived, "the replay did not reuse {derived}");
    assert!(
        landed.contains(&outcome.batch_id),
        "{landed} is the batch's attempt key"
    );
    let lock = engine.config.upload.object_lock.clone().unwrap();
    let kept = backend.stored_retention(&landed, None);
    assert_eq!(kept.mode.as_deref(), Some(lock.mode.as_s3()), "{kept:?}");
    let until = kept
        .retain_until_unix_secs
        .expect("the fresh object is locked");
    let want = unix_now() + i64::from(lock.retain_days) * 86_400;
    assert!(
        (want - 120..=want + 120).contains(&until),
        "locked for retain_days from now: {until} vs {want}"
    );
    let row = row(engine, &outcome.batch_id).await;
    assert_eq!(row.state, BatchState::SourceCommitted);
    assert_eq!(row.object_uri.as_deref(), Some(landed.as_str()));
    let manifest = backend
        .get_object_bounded(row.manifest_uri.as_deref().unwrap(), 1 << 20)
        .await
        .unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
    assert_eq!(
        manifest["object"]["uri"].as_str(),
        Some(landed.as_str()),
        "the manifest names the fresh object"
    );
    assert_eq!(
        &Snapshot::of(backend, derived).await,
        old,
        "{derived} is untouched: not written over, deleted or re-locked"
    );
}
