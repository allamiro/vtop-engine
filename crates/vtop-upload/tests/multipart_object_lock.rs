//! #684: a multipart object's Object Lock is fixed when the upload is
//! CREATED, and a resumed upload must keep reporting that date.
//!
//! S3 takes `x-amz-object-lock-retain-until-date` on CreateMultipartUpload,
//! not on complete, so an upload resumed days later completes an object locked
//! until creation plus the retention. The engine checks the stored date
//! against the date the backend asked for, so the session has to remember it.
//! These tests pin:
//!
//! - a locked upload reports the date its CreateMultipartUpload asked for,
//!   and the store keeps exactly that;
//! - after a resume, the date reported is the one fixed at creation, not the
//!   time of the resume (a check against "now plus the retention" would fail
//!   such an object);
//! - a session written before the field existed still loads and resumes;
//! - a session created WITHOUT a lock is not resumed by a locked upload: its
//!   object could never carry the lock, so a new upload is created that does.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use vtop_core::config::UploadConfig;
use vtop_upload::multipart::{
    save_session, session_file_name, MultipartSession, MultipartSessionPhase, PersistedPart,
};
use vtop_upload::{
    upload_resumable_with_report, MockBackend, MultipartFence, MultipartReport,
    MultipartUploadConfig, ObjectChecksum, ObjectLockMode, ObjectLockPolicy, RetentionCheck,
    UploadBackend,
};

const PART: usize = 1_000;
const DAY: u64 = 86_400;

fn data() -> Vec<u8> {
    (0..3 * PART).map(|i| (i % 251) as u8).collect()
}

fn write(dir: &Path, data: &[u8]) -> std::path::PathBuf {
    let path = dir.join("object.raw");
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(data).unwrap();
    f.flush().unwrap();
    path
}

fn config(state: &Path) -> MultipartUploadConfig {
    let upload: UploadConfig = serde_json::from_str(&format!(
        r#"{{"bucket":"b","backend":"mock",
            "multipart_part_size_bytes":{PART},"multipart_threshold_bytes":{PART},
            "multipart_max_parallelism":2}}"#
    ))
    .unwrap();
    MultipartUploadConfig::from_upload(&upload, state.to_path_buf())
}

fn fence(data: &[u8]) -> (String, MultipartFence) {
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

fn policy() -> ObjectLockPolicy {
    ObjectLockPolicy::with_retention_for_tests(
        ObjectLockMode::Governance,
        Duration::from_secs(30 * DAY),
        false,
    )
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// A session an earlier process left: part 1 uploaded, created `age` ago.
fn earlier_session(
    uri: &str,
    local: &Path,
    fence: &MultipartFence,
    data: &[u8],
    age: Duration,
) -> MultipartSession {
    let created = unix_now() as u64 - age.as_secs();
    let mut parts = BTreeMap::new();
    parts.insert(
        1,
        PersistedPart {
            part_number: 1,
            etag: format!("etag-1-{PART}"),
            size_bytes: PART as u64,
            part_digest_hex: vtop_core::checksum::blake3_bytes(&data[..PART]),
        },
    );
    MultipartSession {
        schema_version: 1,
        backend_id: "mock".into(),
        object_uri: uri.into(),
        local_path: local.to_path_buf(),
        upload_id: "mpu-earlier".into(),
        part_size_bytes: PART as u64,
        fence: fence.clone(),
        phase: MultipartSessionPhase::InProgress,
        parts,
        created_at_unix_secs: created,
        updated_at_unix_secs: created,
        completed_version_id: None,
        retention: None,
    }
}

#[tokio::test]
async fn a_locked_upload_reports_the_date_its_create_asked_for() {
    let dir = tempfile::tempdir().unwrap();
    let data = data();
    let path = write(dir.path(), &data);
    let (digest, fence) = fence(&data);
    let backend = MockBackend::new().with_object_lock(policy());
    let uri = "s3://b/locked/object.raw";

    let before = unix_now();
    let mut report = MultipartReport::default();
    let stored = upload_resumable_with_report(
        &backend,
        &config(dir.path()),
        &path,
        uri,
        Some(ObjectChecksum::new("sha256", &digest)),
        fence,
        &mut report,
    )
    .await
    .expect("the upload completes");
    let after = unix_now();

    let asked = stored
        .retention
        .expect("a locked upload reports the retention it asked for");
    let thirty = (30 * DAY) as i64;
    assert!(
        asked.retain_until_unix_secs >= before + thirty
            && asked.retain_until_unix_secs <= after + thirty + 1,
        "{asked:?}"
    );
    let kept = backend.stored_retention(uri, stored.version_id.as_deref());
    assert_eq!(
        kept.retain_until_unix_secs,
        Some(asked.retain_until_unix_secs)
    );
    assert!(backend
        .verify_retention(uri, stored.version_id.as_deref(), &asked.expectation())
        .await
        .unwrap()
        .passed());
}

#[tokio::test]
async fn a_resumed_upload_reports_the_date_fixed_at_its_creation() {
    let dir = tempfile::tempdir().unwrap();
    let data = data();
    let path = write(dir.path(), &data);
    let (digest, fence) = fence(&data);
    let backend = MockBackend::new().with_object_lock(policy());
    let uri = "s3://b/resumed/object.raw";
    let cfg = config(dir.path());

    // Created two days ago, under the lock asked for then.
    let two_days = Duration::from_secs(2 * DAY);
    let fixed = policy().request_at(SystemTime::now() - two_days);
    let mut session = earlier_session(uri, &path, &fence, &data, two_days);
    session.retention = Some(fixed);
    save_session(&dir.path().join(session_file_name(uri, &fence)), &session).unwrap();
    backend.seed_multipart_locked(uri, "mpu-earlier", vec![(1, data[..PART].to_vec())], fixed);

    let mut report = MultipartReport::default();
    let stored = upload_resumable_with_report(
        &backend,
        &cfg,
        &path,
        uri,
        Some(ObjectChecksum::new("sha256", &digest)),
        fence,
        &mut report,
    )
    .await
    .expect("the resumed upload completes");
    assert!(report.resumed && report.parts_reused == 1, "{report:?}");

    assert_eq!(
        stored.retention,
        Some(fixed),
        "the date reported is the one CreateMultipartUpload fixed, two days ago"
    );
    // The store holds that date, which a check against "now plus thirty
    // days" would call too short: the reason the session remembers it.
    let now_plus = policy().request_at(SystemTime::now());
    assert!(matches!(
        backend
            .verify_retention(uri, stored.version_id.as_deref(), &now_plus.expectation())
            .await
            .unwrap(),
        RetentionCheck::TooShort { .. }
    ));
    assert!(backend
        .verify_retention(uri, stored.version_id.as_deref(), &fixed.expectation())
        .await
        .unwrap()
        .passed());
}

/// A session file written before the field existed: schema 1, no
/// `retention` key. It must load and resume as it always did.
#[tokio::test]
async fn a_session_written_before_the_field_still_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let data = data();
    let path = write(dir.path(), &data);
    let (digest, fence) = fence(&data);
    let backend = MockBackend::new();
    let uri = "s3://b/older/object.raw";

    let session = earlier_session(uri, &path, &fence, &data, Duration::from_secs(60));
    let mut json = serde_json::to_value(&session).unwrap();
    json.as_object_mut().unwrap().remove("retention");
    assert!(
        json.get("retention").is_none(),
        "the hand-written file has no field"
    );
    std::fs::write(
        dir.path().join(session_file_name(uri, &fence)),
        serde_json::to_vec_pretty(&json).unwrap(),
    )
    .unwrap();
    backend.seed_multipart(uri, "mpu-earlier", vec![(1, data[..PART].to_vec())]);

    let mut report = MultipartReport::default();
    upload_resumable_with_report(
        &backend,
        &config(dir.path()),
        &path,
        uri,
        Some(ObjectChecksum::new("sha256", &digest)),
        fence,
        &mut report,
    )
    .await
    .expect("an older session resumes");
    assert!(report.resumed && report.parts_reused == 1, "{report:?}");
}

/// An upload created without a lock completes an UNLOCKED object, whatever
/// is asked later: S3 fixes the lock at create. A locked upload must not
/// adopt such a session; it starts a new upload that carries the lock.
#[tokio::test]
async fn a_session_created_without_a_lock_is_not_resumed_by_a_locked_upload() {
    let dir = tempfile::tempdir().unwrap();
    let data = data();
    let path = write(dir.path(), &data);
    let (digest, fence) = fence(&data);
    let backend = MockBackend::new().with_object_lock(policy());
    let uri = "s3://b/unlocked-session/object.raw";

    let session = earlier_session(uri, &path, &fence, &data, Duration::from_secs(60));
    save_session(&dir.path().join(session_file_name(uri, &fence)), &session).unwrap();
    backend.seed_multipart(uri, "mpu-earlier", vec![(1, data[..PART].to_vec())]);

    let mut report = MultipartReport::default();
    let stored = upload_resumable_with_report(
        &backend,
        &config(dir.path()),
        &path,
        uri,
        Some(ObjectChecksum::new("sha256", &digest)),
        fence,
        &mut report,
    )
    .await
    .expect("the upload completes on a new, locked upload");
    assert!(
        !report.resumed,
        "the unlocked session must not be adopted: {report:?}"
    );
    assert!(
        !backend.has_pending_multipart(uri, "mpu-earlier"),
        "the unlocked upload is aborted, not left to the sweep"
    );
    let asked = stored.retention.expect("the new upload asked for the lock");
    assert_eq!(
        backend
            .stored_retention(uri, stored.version_id.as_deref())
            .retain_until_unix_secs,
        Some(asked.retain_until_unix_secs),
        "the completed object carries the lock"
    );
}

/// Review of #708: a session's lock was fixed at its creation, and a session
/// lives as long as replays keep resuming it. One whose retention has
/// already ended would complete an object nobody has to keep, and that
/// object's date is exactly the one asked for, so it would pass. It is not
/// adopted: a new upload carries a lock from now.
#[tokio::test]
async fn a_session_whose_lock_has_already_ended_is_not_resumed() {
    let dir = tempfile::tempdir().unwrap();
    let data = data();
    let path = write(dir.path(), &data);
    let (digest, fence) = fence(&data);
    let backend = MockBackend::new().with_object_lock(policy());
    let uri = "s3://b/ended-session/object.raw";

    let ended = ObjectLockPolicy::with_retention_for_tests(
        ObjectLockMode::Governance,
        Duration::from_secs(DAY),
        false,
    )
    .request_at(SystemTime::now() - Duration::from_secs(2 * DAY));
    assert!(ended.retain_until_unix_secs < unix_now());
    let mut session = earlier_session(uri, &path, &fence, &data, Duration::from_secs(2 * DAY));
    session.retention = Some(ended);
    save_session(&dir.path().join(session_file_name(uri, &fence)), &session).unwrap();
    backend.seed_multipart_locked(uri, "mpu-earlier", vec![(1, data[..PART].to_vec())], ended);

    let mut report = MultipartReport::default();
    let stored = upload_resumable_with_report(
        &backend,
        &config(dir.path()),
        &path,
        uri,
        Some(ObjectChecksum::new("sha256", &digest)),
        fence,
        &mut report,
    )
    .await
    .expect("the upload completes on a new upload");
    assert!(!report.resumed, "{report:?}");
    let asked = stored.retention.unwrap();
    assert!(
        asked.retain_until_unix_secs > unix_now() + (29 * DAY) as i64,
        "the object is locked from now: {asked:?}"
    );
}
