//! A failed multipart part must not discard the parts that already
//! completed (#668).
//!
//! On a slow or policed link the parts are most of the cost of an object: at
//! 10 Mbit/s an 8 MiB part takes minutes. These tests pin what the resumable
//! upload does when one part fails:
//!
//! - only the failed part is sent again, within the call, and the object
//!   completes;
//! - parts that finished beside a failing part stay in the session;
//! - a part that never succeeds costs exactly the configured attempts, and the
//!   error names the part, the attempts and the cause;
//! - a later call with the same bytes resumes the session, whatever the local
//!   file is called (every engine attempt names its file after its own batch);
//! - other bytes never resume it, and a session with no content digest is
//!   never resumed at all;
//! - a session the store no longer holds (`NoSuchUpload`) restarts once;
//! - a resumed object still has to pass the whole-object read-back.
//!
//! The configuration is read from JSON the way the engine reads it, so this
//! file compiles unchanged against a tree without `multipart_part_attempts`
//! (where the key is ignored) and shows the failures there.

use std::io::Write;
use std::path::Path;
use std::time::Duration;
use vtop_core::config::UploadConfig;
use vtop_core::errors::VtopError;
use vtop_upload::multipart::{load_session, session_file_name};
use vtop_upload::{
    upload_resumable, upload_resumable_with_report, MockBackend, MultipartFence, MultipartReport,
    MultipartUploadConfig, ObjectChecksum, PartFault, UploadBackend,
};

const PART: usize = 1_000;

/// Three parts of distinct bytes, so a part sent in the wrong place shows.
fn three_parts(seed: u8) -> Vec<u8> {
    (0..3 * PART)
        .map(|i| seed.wrapping_add((i / PART) as u8).wrapping_mul(31) ^ (i as u8))
        .collect()
}

fn write(dir: &Path, name: &str, data: &[u8]) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(data).unwrap();
    f.flush().unwrap();
    path
}

fn config(state: &Path, parallelism: usize, attempts: u32) -> MultipartUploadConfig {
    let upload: UploadConfig = serde_json::from_str(&format!(
        r#"{{"bucket":"b","backend":"mock",
            "multipart_part_size_bytes":{PART},"multipart_threshold_bytes":{PART},
            "multipart_max_parallelism":{parallelism},"multipart_part_attempts":{attempts}}}"#
    ))
    .unwrap();
    MultipartUploadConfig::from_upload(&upload, state.to_path_buf())
}

fn fence_for(data: &[u8]) -> (String, MultipartFence) {
    let digest = vtop_core::checksum::sha256_bytes(data);
    let fence = MultipartFence {
        expected_segment_generation: 0,
        fencing_epoch: 0,
        content_digest_hex: digest.clone(),
        content_digest_algorithm: "sha256".into(),
        byte_length: data.len() as u64,
    };
    (digest, fence)
}

async fn upload(
    backend: &MockBackend,
    cfg: &MultipartUploadConfig,
    path: &Path,
    uri: &str,
    data: &[u8],
) -> Result<(), VtopError> {
    let (digest, fence) = fence_for(data);
    upload_resumable(
        backend,
        cfg,
        path,
        uri,
        Some(ObjectChecksum::new("sha256", &digest)),
        fence,
    )
    .await
    .map(|_| ())
}

/// The part numbers the session for `data` at `uri` holds.
fn kept_parts(state: &Path, uri: &str, data: &[u8]) -> Vec<u32> {
    let (_, fence) = fence_for(data);
    let session = load_session(&state.join(session_file_name(uri, &fence)))
        .expect("the session must survive the failure");
    session.parts.keys().copied().collect()
}

async fn verifies(backend: &MockBackend, uri: &str, data: &[u8]) -> bool {
    let digest = vtop_core::checksum::sha256_bytes(data);
    let v = backend
        .verify_object(
            uri,
            data.len() as u64,
            Some(ObjectChecksum::new("sha256", &digest)),
        )
        .await
        .unwrap();
    v.passed && !v.backend_limited
}

/// Part 2 of 3 fails once: it alone is sent again and the object completes
/// in the same call. Before #668 the first failed part ended the whole call.
#[tokio::test(start_paused = true)]
async fn a_part_that_fails_once_is_sent_again_alone_and_the_object_completes() {
    let dir = tempfile::tempdir().unwrap();
    let data = three_parts(1);
    let path = write(dir.path(), "batch-1.raw", &data);
    let backend = MockBackend::new().with_part_fault(2, PartFault::Reset, 1);
    let uri = "s3://b/once.raw";

    upload(&backend, &config(dir.path(), 3, 3), &path, uri, &data)
        .await
        .expect("one reset part is retried, not the end of the upload");

    assert_eq!(
        [
            backend.part_sends(1),
            backend.part_sends(2),
            backend.part_sends(3)
        ],
        [1, 2, 1],
        "only part 2 is sent a second time"
    );
    assert!(verifies(&backend, uri, &data).await);
}

/// Parts 1 and 3 finish in the same round as part 2, which never succeeds:
/// both are kept in the session. Before #668 the results of a round were
/// walked with `?`, so the failing part 2 returned before part 3 was
/// recorded, and part 3 was uploaded and then forgotten.
#[tokio::test(start_paused = true)]
async fn parts_that_finish_beside_a_failing_part_are_kept_in_the_session() {
    let dir = tempfile::tempdir().unwrap();
    let data = three_parts(2);
    let path = write(dir.path(), "batch-1.raw", &data);
    let backend = MockBackend::new().with_part_fault(2, PartFault::Reset, usize::MAX);
    let uri = "s3://b/beside.raw";

    upload(&backend, &config(dir.path(), 3, 3), &path, uri, &data)
        .await
        .expect_err("part 2 never succeeds");

    assert_eq!(
        kept_parts(dir.path(), uri, &data),
        vec![1, 3],
        "both parts that completed are in the session"
    );
}

/// A part that never succeeds costs exactly the configured attempts, and the
/// error says which part, after how many attempts, why, and what was kept.
#[tokio::test(start_paused = true)]
async fn a_part_that_never_succeeds_costs_the_configured_attempts_and_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let data = three_parts(3);
    let path = write(dir.path(), "batch-1.raw", &data);
    let backend = MockBackend::new().with_part_fault(2, PartFault::Reset, usize::MAX);
    let uri = "s3://b/budget.raw";

    let err = upload(&backend, &config(dir.path(), 1, 4), &path, uri, &data)
        .await
        .expect_err("part 2 never succeeds");

    assert_eq!(
        backend.part_sends(2),
        4,
        "multipart_part_attempts: 4 means four requests for the part, no more"
    );
    let message = err.to_string();
    for says in [
        "part 2 of 3",
        uri,
        "after 4 attempts",
        "connection reset by peer",
        "the upload session keeps 1 of 3 parts",
    ] {
        assert!(message.contains(says), "{message:?} should say {says:?}");
    }
    assert!(
        matches!(err, VtopError::Upload(_)),
        "a reset is not a throttle: {err:?}"
    );
    // With one part in flight, part 3 was never started once part 2 failed
    // for good: nothing new is begun after a failure.
    assert_eq!(backend.part_sends(3), 0);
    assert_eq!(kept_parts(dir.path(), uri, &data), vec![1]);
}

/// The engine names its compressed file after the attempt's batch id, so a
/// replay of the same bytes reads them from another path. The session is
/// identified by its content, and every kept part is re-read and its digest
/// compared before it is skipped, so the path does not matter. Before #668
/// the session was refused ("local path ... does not match").
#[tokio::test(start_paused = true)]
async fn the_same_bytes_under_another_file_name_resume_the_session() {
    let dir = tempfile::tempdir().unwrap();
    let data = three_parts(4);
    let first = write(dir.path(), "attempt-1.raw", &data);
    let backend = MockBackend::new().with_part_fault(2, PartFault::Reset, usize::MAX);
    let uri = "s3://b/replayed.raw";
    let cfg = config(dir.path(), 3, 2);

    upload(&backend, &cfg, &first, uri, &data)
        .await
        .expect_err("attempt 1 fails on part 2");
    std::fs::remove_file(&first).unwrap();

    backend.set_part_fault(2, PartFault::Reset, 0);
    let second = write(dir.path(), "attempt-2.raw", &data);
    upload(&backend, &cfg, &second, uri, &data)
        .await
        .expect("attempt 2 resumes the session attempt 1 left");

    assert_eq!(
        [backend.part_sends(1), backend.part_sends(3)],
        [1, 1],
        "the parts attempt 1 completed are not sent again"
    );
    assert!(verifies(&backend, uri, &data).await);
    assert!(
        std::fs::read_dir(dir.path()).unwrap().all(|e| !e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".multipart.json")),
        "a completed upload leaves no session behind"
    );
}

/// Other bytes at the same key never resume a session: the digest names the
/// session, so they start their own, and the one left by the first bytes is
/// untouched for the abandoned-session sweep.
#[tokio::test(start_paused = true)]
async fn other_bytes_never_resume_a_session() {
    let dir = tempfile::tempdir().unwrap();
    let old = three_parts(5);
    let new = three_parts(6);
    assert_eq!(old.len(), new.len());
    let backend = MockBackend::new().with_part_fault(2, PartFault::Reset, usize::MAX);
    let uri = "s3://b/grown.raw";
    let cfg = config(dir.path(), 3, 2);

    let path = write(dir.path(), "attempt-1.raw", &old);
    upload(&backend, &cfg, &path, uri, &old)
        .await
        .expect_err("the first bytes fail on part 2");
    backend.set_part_fault(2, PartFault::Reset, 0);
    let before = backend.part_sends_total();

    let path = write(dir.path(), "attempt-2.raw", &new);
    upload(&backend, &cfg, &path, uri, &new).await.unwrap();

    assert_eq!(
        backend.part_sends_total() - before,
        3,
        "every part of the new bytes is sent"
    );
    assert!(verifies(&backend, uri, &new).await);
    assert_eq!(
        kept_parts(dir.path(), uri, &old),
        vec![1, 3],
        "the first bytes' session is left for the sweep, not adopted"
    );
}

/// With checksums off there is no content digest, so nothing identifies the
/// bytes a session was for: such a session is never resumed.
#[tokio::test(start_paused = true)]
async fn a_session_without_a_content_digest_is_never_resumed() {
    let dir = tempfile::tempdir().unwrap();
    let data = three_parts(7);
    let path = write(dir.path(), "batch.raw", &data);
    let backend = MockBackend::new().with_part_fault(2, PartFault::Reset, usize::MAX);
    let uri = "s3://b/unchecked.raw";
    let cfg = config(dir.path(), 3, 2);
    let fence = MultipartFence {
        expected_segment_generation: 0,
        fencing_epoch: 0,
        content_digest_hex: String::new(),
        content_digest_algorithm: "none".into(),
        byte_length: data.len() as u64,
    };

    upload_resumable(&backend, &cfg, &path, uri, None, fence.clone())
        .await
        .expect_err("part 2 fails");
    backend.set_part_fault(2, PartFault::Reset, 0);
    upload_resumable(&backend, &cfg, &path, uri, None, fence)
        .await
        .unwrap();

    assert_eq!(
        [backend.part_sends(1), backend.part_sends(3)],
        [2, 2],
        "with no digest the second call starts over"
    );
}

/// A throttled part is retried after a pause rather than failing the object.
#[tokio::test(start_paused = true)]
async fn a_throttled_part_is_retried_after_a_pause() {
    let dir = tempfile::tempdir().unwrap();
    let data = three_parts(8);
    let path = write(dir.path(), "batch.raw", &data);
    let backend = MockBackend::new().with_part_fault(2, PartFault::Throttled, 1);
    let uri = "s3://b/throttled.raw";

    let started = tokio::time::Instant::now();
    upload(&backend, &config(dir.path(), 1, 3), &path, uri, &data)
        .await
        .unwrap();
    assert_eq!(backend.part_sends(2), 2);
    // The paused clock moves only by the wait: more than nothing (a retry
    // with no pause would leave it at zero), at most the 1 s base.
    assert!(
        started.elapsed() > Duration::ZERO && started.elapsed() <= Duration::from_secs(1),
        "the first retry waits within (0, 1 s]: {:?}",
        started.elapsed()
    );
    assert!(verifies(&backend, uri, &data).await);
}

/// The store no longer holds the upload (S3 `NoSuchUpload`, after a
/// lifecycle rule or a sweep aborted it): the call starts one new upload and
/// sends every part to it. Sending the same request again would only fail the
/// same way.
#[tokio::test(start_paused = true)]
async fn a_session_the_store_no_longer_holds_restarts_once() {
    let dir = tempfile::tempdir().unwrap();
    let data = three_parts(9);
    let path = write(dir.path(), "batch.raw", &data);
    let backend = MockBackend::new().with_part_fault(2, PartFault::SessionGone, 1);
    let uri = "s3://b/gone.raw";

    upload(&backend, &config(dir.path(), 1, 3), &path, uri, &data)
        .await
        .expect("one gone session is restarted");
    assert_eq!(
        [
            backend.part_sends(1),
            backend.part_sends(2),
            backend.part_sends(3)
        ],
        [2, 2, 1],
        "part 1 went to the lost upload and again to the new one; part 2 was \
         answered NoSuchUpload once and is not retried against the lost upload"
    );
    assert!(verifies(&backend, uri, &data).await);
}

/// A second gone session in the same call is an error, not a loop.
#[tokio::test(start_paused = true)]
async fn a_session_lost_twice_in_one_call_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let data = three_parts(10);
    let path = write(dir.path(), "batch.raw", &data);
    let backend = MockBackend::new().with_part_fault(2, PartFault::SessionGone, 2);
    let uri = "s3://b/gone-twice.raw";

    let err = upload(&backend, &config(dir.path(), 1, 3), &path, uri, &data)
        .await
        .expect_err("a second gone session ends the call");
    let message = err.to_string();
    assert!(
        message.contains("NoSuchUpload") && message.contains("after a restart"),
        "{message}"
    );
    assert_eq!(backend.part_sends(2), 2, "one request per upload, no retry");
    let (_, fence) = fence_for(&data);
    assert!(
        !dir.path().join(session_file_name(uri, &fence)).exists(),
        "a session the store does not hold is not kept"
    );
}

/// What a resumed upload does NOT check: it trusts the parts the store
/// acknowledged (ETags are not evidence) and returns Ok for an object
/// assembled from a part the store damaged afterwards. The whole-object
/// read-back after it is what catches that; here the backend's own
/// `verify_object` stands in for it. The engine test
/// `a_resumed_object_that_does_not_read_back_is_never_committed` is the
/// gate itself: such a batch is never committed.
#[tokio::test(start_paused = true)]
async fn a_resumed_object_whose_stored_part_was_damaged_fails_the_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let data = three_parts(11);
    let path = write(dir.path(), "batch.raw", &data);
    let backend = MockBackend::new().with_part_fault(2, PartFault::Reset, usize::MAX);
    let uri = "s3://b/damaged.raw";
    let cfg = config(dir.path(), 3, 2);

    upload(&backend, &cfg, &path, uri, &data)
        .await
        .expect_err("part 2 fails");
    backend.corrupt_pending_part(1);
    backend.set_part_fault(2, PartFault::Reset, 0);
    upload(&backend, &cfg, &path, uri, &data)
        .await
        .expect("the resumed upload completes; the store took every part");

    assert!(
        !verifies(&backend, uri, &data).await,
        "the read-back must catch the damaged part"
    );
}

/// The store refuses the complete with `InvalidPart` (a part it holds is not
/// the one the session recorded) and keeps the upload. Completing again would
/// fail the same way on every replay, so the call aborts that upload and
/// starts one new one (#668, review).
#[tokio::test(start_paused = true)]
async fn a_complete_the_store_refuses_starts_one_new_upload_and_aborts_the_old() {
    let dir = tempfile::tempdir().unwrap();
    let data = three_parts(12);
    let path = write(dir.path(), "batch.raw", &data);
    let backend = MockBackend::new().with_complete_faults(1);
    let uri = "s3://b/invalid-part.raw";

    upload(&backend, &config(dir.path(), 3, 3), &path, uri, &data)
        .await
        .expect("one refused complete is restarted");
    assert_eq!(
        backend.part_sends_total(),
        6,
        "every part went to the refused upload and again to the new one"
    );
    assert!(
        !backend.has_pending_multipart(uri, "mpu-1"),
        "the refused upload was aborted, not left billed at the store"
    );
    assert_eq!(backend.pending_multipart_count(), 0);
    assert!(verifies(&backend, uri, &data).await);
}

/// Refused twice in one call: nothing is left that a replay could resume
/// into the same refusal. The session is gone and so are both uploads, and
/// the next call starts afresh.
#[tokio::test(start_paused = true)]
async fn a_complete_refused_twice_leaves_nothing_to_resume() {
    let dir = tempfile::tempdir().unwrap();
    let data = three_parts(13);
    let path = write(dir.path(), "batch.raw", &data);
    let backend = MockBackend::new().with_complete_faults(2);
    let uri = "s3://b/invalid-part-twice.raw";
    let cfg = config(dir.path(), 3, 3);
    let (digest, fence) = fence_for(&data);
    let mut report = MultipartReport::default();

    let err = upload_resumable_with_report(
        &backend,
        &cfg,
        &path,
        uri,
        Some(ObjectChecksum::new("sha256", &digest)),
        fence.clone(),
        &mut report,
    )
    .await
    .expect_err("a second refusal ends the call");
    assert!(err.to_string().contains("InvalidPart"), "{err}");
    assert!(!report.session_left, "{report:?}");
    assert!(!dir.path().join(session_file_name(uri, &fence)).exists());
    assert_eq!(backend.pending_multipart_count(), 0, "both uploads aborted");

    upload(&backend, &cfg, &path, uri, &data).await.unwrap();
    assert!(verifies(&backend, uri, &data).await);
}

/// A session file that cannot be read (a torn write before the session was
/// synced, or a hand edit) does not block the object: a new upload takes
/// its place and completes.
#[tokio::test(start_paused = true)]
async fn an_unreadable_session_file_is_replaced_by_a_new_upload() {
    let dir = tempfile::tempdir().unwrap();
    let data = three_parts(14);
    let path = write(dir.path(), "batch.raw", &data);
    let backend = MockBackend::new();
    let uri = "s3://b/unreadable.raw";
    let (_, fence) = fence_for(&data);
    let session = dir.path().join(session_file_name(uri, &fence));
    std::fs::write(&session, b"").unwrap();

    upload(&backend, &config(dir.path(), 3, 3), &path, uri, &data)
        .await
        .unwrap();
    assert!(verifies(&backend, uri, &data).await);
    assert!(
        !session.exists(),
        "the replacement completed and was removed"
    );
}
