//! #684 against a real S3-compatible store with Object Lock: what the
//! profile relies on the store to do.
//!
//! - a bucket created with Object Lock answers the preflight with "enabled",
//!   a bucket created without it with "not enabled";
//! - an object PUT, a manifest PUT, a BLAKE3 write and a multipart upload
//!   under the profile are accepted (the integrity header a locked write
//!   needs), and the retention read back from the exact version is the one
//!   asked for, in both modes, with and without a legal hold;
//! - a locked version cannot be deleted, an overwrite makes a new version
//!   and leaves it intact, and a plain delete only adds a delete marker;
//! - once the retention ends the store still reports the mode and the past
//!   date, which is what the scrub calls `elapsed` (not `missing`).
//!
//! Retentions here are a few seconds long, which configuration cannot
//! express (`ObjectLockPolicy::with_retention_for_tests`), so every test can
//! delete what it wrote once they end.
//!
//! Run against the compose MinIO lab (`minio-init` creates
//! `telemetry-locked` with Object Lock and `telemetry-raw` without) with:
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin
//!  AWS_SECRET_ACCESS_KEY=minioadmin cargo test -p vtop-upload
//!  --test live_s3_object_lock -- --ignored --test-threads=1`
//! `VTOP_TEST_S3_LOCKED_BUCKET` and `VTOP_TEST_S3_BUCKET` may name others.
//! CI runs it in the `s3-compat` matrix (#722).

use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use vtop_s3_testkit::{Capability, LiveS3, RefusalAt, S3Settings};
use vtop_upload::s3_native::{S3NativeBackend, S3NativeConfig};
use vtop_upload::{
    upload_resumable, MultipartFence, MultipartUploadConfig, ObjectChecksum, ObjectLockMode,
    ObjectLockPolicy, RetentionCheck, RetentionExpectation, UploadBackend,
};

const MIB: usize = 1024 * 1024;

/// How to reach the store, from `VTOP_TEST_S3_*` (#722), read once.
fn settings() -> S3Settings {
    static SETTINGS: std::sync::OnceLock<S3Settings> = std::sync::OnceLock::new();
    SETTINGS
        .get_or_init(|| LiveS3::from_env().settings.clone())
        .clone()
}

fn locked_bucket() -> String {
    settings().locked_bucket
}

fn plain_bucket() -> String {
    settings().bucket
}

async fn backend(lock: Option<ObjectLockPolicy>) -> S3NativeBackend {
    let s = settings();
    S3NativeBackend::new(&S3NativeConfig {
        region: s.region,
        endpoint_url: s.endpoint,
        force_path_style: s.force_path_style,
        verify_tls: s.verify_tls,
        transport: "tcp_tls".into(),
        tuning: Default::default(),
        max_egress_bytes_per_second: None,
        request_timeout_seconds: 300,
        request_min_bytes_per_second: 4 * 1024,
        object_lock: lock,
    })
    .await
    .unwrap()
}

/// A client straight to the store, for what the backend has no call for:
/// deleting one version, a legal hold, listing versions.
async fn s3_client() -> aws_sdk_s3::Client {
    let s = settings();
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(s.region));
    if let Some(endpoint) = s.endpoint {
        loader = loader.endpoint_url(endpoint);
    }
    let shared = loader.load().await;
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(s.force_path_style)
            .build(),
    )
}

fn unique(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("vtop-it-684/{tag}-{nanos}")
}

fn file(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(bytes).unwrap();
    f.flush().unwrap();
    f
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Delete every version under `prefix`, once their retention has ended.
/// Legal holds are lifted first; a version still retained is reported.
async fn clean_up(client: &aws_sdk_s3::Client, bucket: &str, prefix: &str) {
    let listed = client
        .list_object_versions()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .unwrap();
    let mut targets: Vec<(String, String)> = listed
        .versions()
        .iter()
        .filter_map(|v| Some((v.key()?.to_owned(), v.version_id()?.to_owned())))
        .collect();
    targets.extend(
        listed
            .delete_markers()
            .iter()
            .filter_map(|m| Some((m.key()?.to_owned(), m.version_id()?.to_owned()))),
    );
    for (key, version) in targets {
        let _ = client
            .put_object_legal_hold()
            .bucket(bucket)
            .key(&key)
            .version_id(&version)
            .legal_hold(
                aws_sdk_s3::types::ObjectLockLegalHold::builder()
                    .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::Off)
                    .build(),
            )
            .send()
            .await;
        if let Err(e) = client
            .delete_object()
            .bucket(bucket)
            .key(&key)
            .version_id(&version)
            .send()
            .await
        {
            eprintln!("could not clean up {key} version {version}: {e:?}");
        }
    }
}

/// The refusal on a store without Object Lock or versioning (#722; Garage
/// documents both missing): the backend's own answer, which the engine
/// refuses at startup on (#770), checked against the plain bucket, since
/// such a store has no locked one.
async fn store_lacks_it(capability: Capability, at: RefusalAt) {
    assert_eq!(
        at,
        RefusalAt::Startup,
        "VTOP refuses {capability} at startup"
    );
    let b = backend(None).await;
    match capability {
        Capability::ObjectLock | Capability::ObjectLockEnforced => assert_eq!(
            b.bucket_object_lock(&plain_bucket()).await.unwrap(),
            BucketFeature::NotSupported,
            "the store answers that it does not implement Object Lock, which the engine \
             refuses at startup"
        ),
        Capability::Versioning => assert_ne!(
            b.bucket_versioning(&plain_bucket()).await.unwrap(),
            BucketFeature::Enabled,
            "the store does not keep versions, which the hardened profile refuses at startup"
        ),
        other => panic!("no refusal is asserted for {other}"),
    }
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_the_preflight_tells_a_locked_bucket_from_a_plain_one() {
    let live = LiveS3::from_env();
    live.case_with_refusal(
        "object_lock.the_preflight_tells_locked_from_plain",
        &[Capability::ObjectLock],
        || async {
            let backend = backend(None).await;
            assert!(
                backend
                    .bucket_object_lock_enabled(&locked_bucket())
                    .await
                    .unwrap(),
                "{} was created with Object Lock",
                locked_bucket()
            );
            assert!(
                !backend
                    .bucket_object_lock_enabled(&plain_bucket())
                    .await
                    .unwrap(),
                "{} was created without Object Lock: the store's answer is a definite no",
                plain_bucket()
            );
        },
        store_lacks_it,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_every_write_path_lands_under_the_lock_it_asked_for() {
    let live = LiveS3::from_env();
    live.case_with_refusal(
        "object_lock.every_write_path_lands_locked",
        &[
            Capability::ObjectLock,
            Capability::Versioning,
            Capability::ChecksumCrc32Default,
        ],
        || async {
            let bucket = locked_bucket();
            let prefix = unique("writes");
            let client = s3_client().await;
            let retain = Duration::from_secs(4);
            for (mode, legal_hold) in [
                (ObjectLockMode::Governance, false),
                (ObjectLockMode::Compliance, false),
                (ObjectLockMode::Governance, true),
            ] {
                let locked = backend(Some(ObjectLockPolicy::with_retention_for_tests(
                    mode, retain, legal_hold,
                )))
                .await;
                let body = b"locked bytes";
                let f = file(body);
                let sha = vtop_core::checksum::sha256_bytes(body);
                let b3 = vtop_core::checksum::blake3_bytes(body);
                let tag = format!("{mode}-{legal_hold}");
                let writes = [
                    (
                        "sha256 object",
                        locked
                            .put_object(
                                f.path(),
                                &format!("s3://{bucket}/{prefix}/{tag}/sha.raw"),
                                Some(ObjectChecksum::new("sha256", &sha)),
                            )
                            .await
                            .map(|s| (s.version_id, s.retention, "sha.raw")),
                    ),
                    (
                        "blake3 object",
                        locked
                            .put_object(
                                f.path(),
                                &format!("s3://{bucket}/{prefix}/{tag}/b3.raw"),
                                Some(ObjectChecksum::new("blake3", &b3)),
                            )
                            .await
                            .map(|s| (s.version_id, s.retention, "b3.raw")),
                    ),
                    (
                        "manifest",
                        locked
                            .put_manifest(
                                f.path(),
                                &format!("s3://{bucket}/{prefix}/{tag}/m.json"),
                                Some(ObjectChecksum::new("sha256", &sha)),
                            )
                            .await
                            .map(|s| (s.version_id, s.retention, "m.json")),
                    ),
                ];
                for (what, written) in writes {
                    let (version, asked, name) = written.unwrap_or_else(|e| {
                        panic!("[{tag}] the store accepts a locked {what}: {e}")
                    });
                    let asked =
                        asked.unwrap_or_else(|| panic!("[{tag}] {what}: the lock asked for"));
                    let uri = format!("s3://{bucket}/{prefix}/{tag}/{name}");
                    let found = locked
                        .verify_retention(&uri, version.as_deref(), &asked.expectation())
                        .await
                        .unwrap();
                    assert_eq!(
                        found,
                        RetentionCheck::Ok {
                            retain_until_unix_secs: asked.retain_until_unix_secs
                        },
                        "[{tag}] {what}: the store keeps exactly the date asked for"
                    );
                }
            }
            tokio::time::sleep(retain + Duration::from_secs(2)).await;
            clean_up(&client, &bucket, &prefix).await;
        },
        store_lacks_it,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_a_locked_version_survives_delete_and_overwrite_and_reports_elapsed_after() {
    let live = LiveS3::from_env();
    live.case_with_refusal(
        "object_lock.a_locked_version_survives_delete_and_overwrite",
        &[
            Capability::ObjectLock,
            Capability::Versioning,
            Capability::ObjectLockEnforced,
        ],
        || async {
            let bucket = locked_bucket();
            let prefix = unique("worm");
            let key = format!("{prefix}/obj.raw");
            let uri = format!("s3://{bucket}/{key}");
            let client = s3_client().await;
            let retain = Duration::from_secs(4);
            let locked = backend(Some(ObjectLockPolicy::with_retention_for_tests(
                ObjectLockMode::Governance,
                retain,
                false,
            )))
            .await;
            let original = b"the archived bytes";
            let f = file(original);
            let sha = vtop_core::checksum::sha256_bytes(original);
            let stored = locked
                .put_object(f.path(), &uri, Some(ObjectChecksum::new("sha256", &sha)))
                .await
                .unwrap();
            let version = stored
                .version_id
                .clone()
                .expect("a locked bucket versions every write");
            let asked = stored.retention.unwrap();

            // Delete the version, as a caller that does not bypass governance.
            let refused = client
                .delete_object()
                .bucket(&bucket)
                .key(&key)
                .version_id(&version)
                .send()
                .await;
            assert!(
                refused.is_err(),
                "a version under retention cannot be deleted"
            );

            // Overwrite the key: a new version; the locked one is intact.
            let other = file(b"other bytes, same size");
            locked.put_object(other.path(), &uri, None).await.unwrap();
            // A plain delete: only a delete marker.
            locked.delete_object(&uri).await.unwrap();
            let pinned = locked
                .get_object_pinned(&uri, &version, 1024)
                .await
                .unwrap();
            assert_eq!(
                pinned, original,
                "the locked version still holds the archived bytes"
            );
            assert!(locked
                .verify_retention(&uri, Some(&version), &asked.expectation())
                .await
                .unwrap()
                .passed());

            // Once it ends, the store still reports the mode and the past date.
            tokio::time::sleep(retain + Duration::from_secs(2)).await;
            let now = RetentionExpectation {
                mode: ObjectLockMode::Governance,
                retain_until_at_least_unix_secs: unix_now(),
                legal_hold: false,
            };
            assert_eq!(
                locked
                    .verify_retention(&uri, Some(&version), &now)
                    .await
                    .unwrap(),
                RetentionCheck::TooShort {
                    retain_until_unix_secs: asked.retain_until_unix_secs
                },
                "an ended retention reads as ended (scrub: elapsed), not as missing"
            );
            clean_up(&client, &bucket, &prefix).await;
        },
        store_lacks_it,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_a_multipart_object_carries_the_lock_its_create_asked_for() {
    let live = LiveS3::from_env();
    live.case_with_refusal(
        "object_lock.a_multipart_object_carries_its_lock",
        &[
            Capability::Multipart,
            Capability::ObjectLock,
            Capability::Versioning,
        ],
        || async {
            let bucket = locked_bucket();
            let prefix = unique("multipart");
            let uri = format!("s3://{bucket}/{prefix}/big.raw");
            let client = s3_client().await;
            let retain = Duration::from_secs(4);
            let locked = backend(Some(ObjectLockPolicy::with_retention_for_tests(
                ObjectLockMode::Governance,
                retain,
                false,
            )))
            .await;
            let data: Vec<u8> = (0..11 * MIB).map(|i| (i % 253) as u8).collect();
            let f = file(&data);
            let digest = vtop_core::checksum::sha256_bytes(&data);
            let state = tempfile::tempdir().unwrap();
            let upload: vtop_core::config::UploadConfig =
                serde_json::from_value(serde_json::json!({
                    "bucket": "b",
                    "multipart_part_size_bytes": 5 * MIB,
                    "multipart_threshold_bytes": 5 * MIB,
                }))
                .unwrap();
            let cfg = MultipartUploadConfig::from_upload(&upload, state.path().to_path_buf());
            let stored = upload_resumable(
                &locked,
                &cfg,
                f.path(),
                &uri,
                Some(ObjectChecksum::new("sha256", &digest)),
                MultipartFence {
                    expected_segment_generation: 0,
                    fencing_epoch: 0,
                    content_digest_hex: digest.clone(),
                    content_digest_algorithm: "sha256".into(),
                    byte_length: data.len() as u64,
                },
            )
            .await
            .expect("the store accepts a locked multipart upload");
            let asked = stored.retention.expect("the create asked for the lock");
            assert!(locked
                .verify_retention(&uri, stored.version_id.as_deref(), &asked.expectation())
                .await
                .unwrap()
                .passed());
            tokio::time::sleep(retain + Duration::from_secs(2)).await;
            clean_up(&client, &bucket, &prefix).await;
        },
        store_lacks_it,
    )
    .await;
}
