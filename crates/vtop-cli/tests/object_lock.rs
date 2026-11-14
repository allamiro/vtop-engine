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
//!   mode_mismatch, the last two as findings.
#![cfg(unix)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use vtop_cli::scrub::{self, RetentionResult, ScrubOptions};
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::{ObjectLockConfig, ObjectLockMode, StreamsConfig, VtopConfig};
use vtop_core::state_machine::BatchState;
use vtop_core::types::SourceType;
use vtop_state::BatchRecord;
use vtop_upload::{LockFault, MockBackend, ObjectLockPolicy, StoredRetention, UploadBackend};

const TEMPLATE: &str = "tenant={tenant}/source={source}/format={format}/";

fn write_lines(path: &Path, tag: &str, lines: usize) {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    for i in 0..lines {
        writeln!(f, "{tag} record {i}").unwrap();
    }
}

fn lock(mode: ObjectLockMode, legal_hold: bool) -> ObjectLockConfig {
    ObjectLockConfig {
        mode,
        retain_days: 30,
        legal_hold,
    }
}

fn governance() -> ObjectLockConfig {
    lock(ObjectLockMode::Governance, false)
}

/// A file-source config on the mock backend, with its own tenant so the
/// counters a test reads are its own.
fn config(dir: &Path, tenant: &str, bucket: &str, input: &Path) -> VtopConfig {
    let mut cfg = file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join("state.db").display()),
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    cfg.partitioning.template = TEMPLATE.into();
    cfg.upload.bucket = bucket.into();
    cfg.engine.tenant = tenant.into();
    cfg
}

/// An engine with the profile set, over `backend`. Built WITHOUT the block
/// first, because `build_backend` refuses it for the mock backend (as it does
/// for every backend but s3_native), then given the block and the mock.
async fn engine(
    mut cfg: VtopConfig,
    lock: Option<ObjectLockConfig>,
    backend: &Arc<MockBackend>,
) -> Engine {
    // The registry, before the engine counts anything: without one the
    // engine counts nothing, by design.
    vtop_core::telemetry::init().unwrap();
    cfg.upload.object_lock = None;
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    if let Some(lock) = lock {
        engine.config.upload.require_object_versioning = true;
        engine.config.upload.object_lock = Some(lock);
    }
    engine.backend = backend.clone() as Arc<dyn UploadBackend>;
    engine
}

fn locked_mock(lock: &ObjectLockConfig) -> MockBackend {
    MockBackend::new().with_object_lock(ObjectLockPolicy::from_config(lock))
}

/// The operator log, captured as JSON lines.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);
struct LogWriter(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = LogWriter;
    fn make_writer(&'a self) -> LogWriter {
        LogWriter(self.0.clone())
    }
}
impl Logs {
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let logs = Logs::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::INFO)
            .with_writer(logs.clone())
            .finish();
        (logs.clone(), tracing::subscriber::set_default(subscriber))
    }
    fn raw(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
    fn events(&self, event: &str) -> Vec<Value> {
        self.raw()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter_map(|v| v.get("fields").cloned())
            .filter(|f| f.get("event").and_then(Value::as_str) == Some(event))
            .collect()
    }
}

fn text(fields: &Value, name: &str) -> String {
    fields
        .get(name)
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| v.to_string())
        })
        .unwrap_or_default()
}

fn checks(tenant: &str, result: &str) -> u64 {
    vtop_core::telemetry::init()
        .unwrap()
        .object_lock_checks_total
        .with_label_values(&[tenant, result])
        .get()
}

fn verification_failures(tenant: &str) -> u64 {
    vtop_core::telemetry::init()
        .unwrap()
        .verification_failures_total
        .with_label_values(&[tenant, "file", "raw"])
        .get()
}

async fn row(engine: &Engine, batch_id: &str) -> BatchRecord {
    engine.store.get_batch(batch_id).await.unwrap().unwrap()
}

async fn committed(engine: &Engine) -> Vec<BatchRecord> {
    let mut rows: Vec<BatchRecord> = engine
        .store
        .list_batches()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.state == BatchState::SourceCommitted)
        .collect();
    rows.sort_by(|a, b| a.batch_id.cmp(&b.batch_id));
    rows
}

/// The three parts of an operator message, each asserted.
fn assert_says(message: &str, parts: &[&str]) {
    for part in parts {
        assert!(message.contains(part), "{message:?} should say {part:?}");
    }
}

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
    let (logs, _guard) = Logs::capture();
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
    let event = logs.events("object_lock_bucket_refused");
    assert_eq!(event.len(), 1, "{}", logs.raw());
    assert_eq!(text(&event[0], "bucket"), "telemetry-raw");
    assert_eq!(text(&event[0], "batch_id"), out[0].batch_id);
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
        let (logs, _guard) = Logs::capture();

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
        let event = logs.events("object_lock_check_failed");
        assert_eq!(event.len(), 1, "[{result}] {}", logs.raw());
        assert_eq!(text(&event[0], "result"), result);
        assert_eq!(text(&event[0], "batch_id"), out[0].batch_id);
        assert_eq!(
            text(&event[0], "object_uri"),
            failed.object_uri.clone().unwrap()
        );
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
/// key, and a replay of the same bytes reuses it (#626) and fails again: the
/// message says so and what to do.
#[tokio::test]
async fn a_replay_that_reuses_an_unlocked_object_fails_again_and_says_what_to_do() {
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

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed);
    backend.set_lock_fault(None);
    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(!second[0].committed, "{second:?}");
    let failed = row(&engine, &second[0].batch_id).await;
    assert_eq!(
        failed.object_uri,
        row(&engine, &first[0].batch_id).await.object_uri,
        "the replay reused the object at the derived key"
    );
    assert_says(
        &failed.error_message.unwrap_or_default(),
        &[
            "reused from an earlier attempt",
            "is not under the requested retention",
            "the batch was marked failed and will be replayed",
            "delete that version",
        ],
    );
    assert_eq!(checks(tenant, "missing"), 2);
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

#[tokio::test]
async fn a_reused_object_whose_lock_has_ended_fails() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-reuse-elapsed";
    let lock = governance();
    // A one-second lock, which configuration cannot express: it has ended by
    // the time the replay reuses the object. The dates are whole seconds,
    // rounded up when asked for, so the wait is three seconds.
    let backend = Arc::new(
        MockBackend::new()
            .with_object_lock(ObjectLockPolicy::with_retention_for_tests(
                ObjectLockMode::Governance,
                Duration::from_secs(1),
                false,
            ))
            .with_throttled_manifest_puts(1),
    );
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-raw", &input),
        Some(lock),
        &backend,
    )
    .await;

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed);
    tokio::time::sleep(Duration::from_millis(3_100)).await;
    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(!second[0].committed, "{second:?}");
    let failed = row(&engine, &second[0].batch_id).await;
    assert_eq!(
        failed.object_uri,
        row(&engine, &first[0].batch_id).await.object_uri,
        "the replay reused the object the first attempt locked"
    );
    assert_says(
        &failed.error_message.unwrap_or_default(),
        &["reused from an earlier attempt", "found retain-until"],
    );
    assert_eq!(checks(tenant, "too_short"), 1);
}

#[tokio::test]
async fn a_damaged_object_at_the_derived_key_is_not_written_over_and_the_batch_lands_locked() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let tenant = "lock-fallback";
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock).with_throttled_manifest_puts(1));
    let mut engine = engine(
        config(dir.path(), tenant, "telemetry-raw", &input),
        Some(lock),
        &backend,
    )
    .await;

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed);
    let derived = row(&engine, &first[0].batch_id).await.object_uri.unwrap();
    backend.corrupt(&derived);

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(second[0].committed, "{second:?}");
    let landed = second[0].object_uri.clone().unwrap();
    assert_ne!(landed, derived, "the batch fell back to its attempt key");
    assert!(
        backend
            .get_object_pinned(&derived, "v2", 1 << 20)
            .await
            .is_err(),
        "nothing was written over the damaged object"
    );
    let kept = backend.stored_retention(&landed, None);
    assert_eq!(kept.mode.as_deref(), Some("GOVERNANCE"));
    assert_eq!(checks(tenant, "ok"), 2);
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
    let (logs, _guard) = Logs::capture();

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
        assert!(!logs.raw().contains(secret), "the log holds {secret}");
    }
    assert_eq!(checks(tenant, "error"), 1);
}

// ---- Scrub --check-retention ------------------------------------------------

async fn three_committed(
    tenant: &str,
    retain: Duration,
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
    let (logs, _guard) = Logs::capture();
    engine
        .preflight_object_lock()
        .await
        .expect("a store blip does not keep the engine down");
    assert!(
        logs.raw().contains("could not be read")
            && logs.raw().contains("checked again before the first upload"),
        "{}",
        logs.raw()
    );
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

/// A reused object without the legal hold the config now asks for needs a
/// legal hold, not a longer retention: the message says which.
#[tokio::test]
async fn a_reused_object_without_the_legal_hold_says_to_place_one() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 2);
    let lock = governance();
    let backend = Arc::new(locked_mock(&lock).with_throttled_manifest_puts(1));
    let mut engine = engine(
        config(dir.path(), "lock-reuse-hold", "telemetry-raw", &input),
        Some(lock),
        &backend,
    )
    .await;
    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed);
    engine.config.upload.object_lock = Some(lock_with_hold());
    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(!second[0].committed);
    assert_says(
        &row(&engine, &second[0].batch_id)
            .await
            .error_message
            .unwrap_or_default(),
        &["reused from an earlier attempt", "PutObjectLegalHold"],
    );
}

fn lock_with_hold() -> ObjectLockConfig {
    lock(ObjectLockMode::Governance, true)
}
