//! `upload.server_side_encryption` against a real S3-compatible store with a
//! KMS: what the profile relies on the store to do.
//!
//! - an object, a BLAKE3 object and a multipart object written under `sse-s3`,
//!   `sse-kms` with a named key and `sse-kms` with no key are accepted;
//! - HeadObject, asked straight of the store and through the backend, reports
//!   the encryption that was asked for;
//! - verification passes on them, strong (the store's SHA-256 or a read-back)
//!   and not size-only, and the stored bytes read back intact;
//! - an object written WITHOUT encryption fails verification under every mode,
//!   and an encrypted one fails under a key it was not written with.
//!
//! Run against the compose MinIO lab, whose `minio` service has a single
//! static KMS key (`MINIO_KMS_SECRET_KEY`, key name `vtop-lab-key`):
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin
//!  AWS_SECRET_ACCESS_KEY=minioadmin cargo test -p vtop-upload
//!  --test live_s3_sse -- --ignored --test-threads=1`
//! `VTOP_TEST_S3_BUCKET` and `VTOP_TEST_S3_KMS_KEY` may name others. CI runs
//! it in the MinIO leg of `s3-compat-matrix.yml`, against the lab's MinIO.

use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use vtop_core::config::{ServerSideEncryptionConfig, ServerSideEncryptionMode};
use vtop_upload::s3_native::{S3NativeBackend, S3NativeConfig};
use vtop_upload::{
    upload_resumable, MultipartFence, MultipartUploadConfig, ObjectChecksum, UploadBackend,
    VerificationResult,
};

const MIB: usize = 1024 * 1024;

fn endpoint() -> String {
    std::env::var("VTOP_TEST_S3_ENDPOINT")
        .expect("VTOP_TEST_S3_ENDPOINT must name the live endpoint")
}

fn bucket() -> String {
    std::env::var("VTOP_TEST_S3_BUCKET").unwrap_or_else(|_| "telemetry-raw".into())
}

/// The KMS key the store has: MinIO's static key in the lab.
fn kms_key() -> String {
    std::env::var("VTOP_TEST_S3_KMS_KEY").unwrap_or_else(|_| "vtop-lab-key".into())
}

fn sse(mode: ServerSideEncryptionMode, key: Option<&str>) -> Option<ServerSideEncryptionConfig> {
    Some(ServerSideEncryptionConfig {
        mode,
        kms_key_id: key.map(str::to_owned),
        bucket_key: false,
    })
}

async fn backend(sse: Option<ServerSideEncryptionConfig>) -> S3NativeBackend {
    S3NativeBackend::new(&S3NativeConfig {
        region: "us-east-1".into(),
        endpoint_url: Some(endpoint()),
        force_path_style: true,
        verify_tls: false,
        transport: "tcp_tls".into(),
        tuning: Default::default(),
        max_egress_bytes_per_second: None,
        request_timeout_seconds: 300,
        request_min_bytes_per_second: 4 * 1024,
        object_lock: None,
        server_side_encryption: sse,
    })
    .await
    .unwrap()
}

/// A client straight to the store, so what it reports is not read through the
/// backend under test.
async fn s3_client() -> aws_sdk_s3::Client {
    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint())
        .load()
        .await;
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(true)
            .build(),
    )
}

fn unique(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("vtop-it-sse/{tag}-{nanos}")
}

fn file(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(bytes).unwrap();
    f.flush().unwrap();
    f
}

/// What the store itself says about one object's encryption.
async fn stored_as(uri: &str) -> (Option<String>, Option<String>) {
    let (b, key) = uri.strip_prefix("s3://").unwrap().split_once('/').unwrap();
    let head = s3_client()
        .await
        .head_object()
        .bucket(b)
        .key(key)
        .send()
        .await
        .unwrap();
    (
        head.server_side_encryption().map(|a| a.as_str().to_owned()),
        head.ssekms_key_id().map(str::to_owned),
    )
}

fn strong(result: &VerificationResult, what: &str) {
    assert!(result.passed, "{what}: {}", result.message);
    assert!(!result.backend_limited, "{what}: {}", result.message);
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with a KMS"]
async fn live_sse_s3_is_stored_reported_and_verified() {
    let uri = format!("s3://{}/{}.bin", bucket(), unique("sse-s3"));
    let payload = b"VTOP encrypted at rest, SSE-S3";
    let body = file(payload);
    let sha = vtop_core::checksum::sha256_bytes(payload);
    let b = backend(sse(ServerSideEncryptionMode::SseS3, None)).await;
    b.put_object(body.path(), &uri, Some(ObjectChecksum::new("sha256", &sha)))
        .await
        .expect("the store accepts an SSE-S3 write");

    let (alg, _) = stored_as(&uri).await;
    assert_eq!(alg.as_deref(), Some("AES256"), "the store's own HEAD");
    let head = b.head_object(&uri).await.unwrap();
    assert_eq!(head.encryption.unwrap().algorithm, "AES256");

    let result = b
        .verify_object(
            &uri,
            payload.len() as u64,
            Some(ObjectChecksum::new("sha256", &sha)),
        )
        .await
        .unwrap();
    strong(&result, "sha256 under SSE-S3");
    eprintln!(
        "SSE-S3 sha256 verified by {:?}: {}",
        result.method, result.message
    );
    assert_eq!(
        b.get_object(&uri).await.unwrap(),
        payload,
        "read-back is plaintext"
    );

    // The BLAKE3 path reads the object back, which must decrypt.
    let b3 = vtop_core::checksum::blake3_bytes(payload);
    let b3_uri = format!("s3://{}/{}.bin", bucket(), unique("sse-s3-b3"));
    b.put_object(
        body.path(),
        &b3_uri,
        Some(ObjectChecksum::new("blake3", &b3)),
    )
    .await
    .unwrap();
    let result = b
        .verify_object(
            &b3_uri,
            payload.len() as u64,
            Some(ObjectChecksum::new("blake3", &b3)),
        )
        .await
        .unwrap();
    strong(&result, "blake3 under SSE-S3");
    for u in [&uri, &b3_uri] {
        b.delete_object(u).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with a KMS"]
async fn live_sse_kms_with_and_without_a_key_is_stored_reported_and_verified() {
    let payload = b"VTOP encrypted at rest, SSE-KMS";
    let body = file(payload);
    let sha = vtop_core::checksum::sha256_bytes(payload);
    let ck = ObjectChecksum::new("sha256", &sha);
    let key = kms_key();

    for (label, configured) in [("named key", Some(key.as_str())), ("no key id", None)] {
        let uri = format!("s3://{}/{}.bin", bucket(), unique("sse-kms"));
        let b = backend(sse(ServerSideEncryptionMode::SseKms, configured)).await;
        b.put_object(body.path(), &uri, Some(ck))
            .await
            .unwrap_or_else(|e| panic!("[{label}] the store accepts an SSE-KMS write: {e}"));

        let (alg, reported_key) = stored_as(&uri).await;
        eprintln!("SSE-KMS [{label}]: store reports {alg:?} key {reported_key:?}");
        assert_eq!(
            alg.as_deref(),
            Some("aws:kms"),
            "[{label}] the store's own HEAD"
        );
        let head = b.head_object(&uri).await.unwrap();
        assert_eq!(head.encryption.unwrap().algorithm, "aws:kms");

        let result = b
            .verify_object(&uri, payload.len() as u64, Some(ck))
            .await
            .unwrap();
        strong(&result, &format!("[{label}] sha256 under SSE-KMS"));
        eprintln!(
            "SSE-KMS [{label}] verified by {:?}: {}",
            result.method, result.message
        );
        assert_eq!(b.get_object(&uri).await.unwrap(), payload, "[{label}]");

        if configured.is_some() {
            // The same object, judged against a key it was not written with.
            let other = backend(sse(
                ServerSideEncryptionMode::SseKms,
                Some("some-other-key"),
            ))
            .await;
            let result = other
                .verify_object(&uri, payload.len() as u64, Some(ck))
                .await
                .unwrap();
            assert!(
                !result.passed && result.message.contains("key differs"),
                "another key must fail: {result:?}"
            );
        }
        b.delete_object(&uri).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with a KMS"]
async fn live_an_unencrypted_object_fails_verification_under_every_mode() {
    let uri = format!("s3://{}/{}.bin", bucket(), unique("plain"));
    let payload = b"VTOP written without encryption";
    let body = file(payload);
    let sha = vtop_core::checksum::sha256_bytes(payload);
    let ck = ObjectChecksum::new("sha256", &sha);
    let plain = backend(None).await;
    plain.put_object(body.path(), &uri, Some(ck)).await.unwrap();
    assert_eq!(
        stored_as(&uri).await.0,
        None,
        "the bucket has no default encryption, so the store reports none"
    );
    // Without the setting the object verifies as it always did.
    strong(
        &plain
            .verify_object(&uri, payload.len() as u64, Some(ck))
            .await
            .unwrap(),
        "no setting",
    );
    for configured in [
        sse(ServerSideEncryptionMode::SseS3, None),
        sse(ServerSideEncryptionMode::SseKms, None),
        sse(ServerSideEncryptionMode::SseKms, Some(&kms_key())),
    ] {
        let b = backend(configured.clone()).await;
        let result = b
            .verify_object(&uri, payload.len() as u64, Some(ck))
            .await
            .unwrap();
        assert!(
            !result.passed && result.message.contains("encryption at rest missing"),
            "{configured:?}: {result:?}"
        );
    }
    plain.delete_object(&uri).await.unwrap();
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with a KMS"]
async fn live_a_multipart_object_is_encrypted_by_its_create_and_verified() {
    let data: Vec<u8> = (0..11 * MIB).map(|i| (i % 251) as u8).collect();
    let f = file(&data);
    let digest = vtop_core::checksum::sha256_bytes(&data);
    let upload: vtop_core::config::UploadConfig = serde_json::from_str(
        r#"{"bucket":"b","multipart_part_size_bytes":5242880,"multipart_threshold_bytes":5242880}"#,
    )
    .unwrap();
    let key = kms_key();
    for (label, mode, configured, want) in [
        ("sse-s3", ServerSideEncryptionMode::SseS3, None, "AES256"),
        (
            "sse-kms",
            ServerSideEncryptionMode::SseKms,
            Some(key.as_str()),
            "aws:kms",
        ),
    ] {
        let uri = format!("s3://{}/{}.raw", bucket(), unique(label));
        let b = backend(sse(mode, configured)).await;
        let state = tempfile::tempdir().unwrap();
        let cfg = MultipartUploadConfig::from_upload(&upload, state.path().to_path_buf());
        upload_resumable(
            &b,
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
        .unwrap_or_else(|e| panic!("[{label}] the store accepts an encrypted multipart: {e}"));
        assert_eq!(
            stored_as(&uri).await.0.as_deref(),
            Some(want),
            "[{label}] the multipart object is encrypted"
        );
        let result = b
            .verify_object(
                &uri,
                data.len() as u64,
                Some(ObjectChecksum::new("sha256", &digest)),
            )
            .await
            .unwrap();
        strong(&result, &format!("[{label}] multipart"));
        eprintln!(
            "multipart [{label}] verified by {:?}: {}",
            result.method, result.message
        );
        b.delete_object(&uri).await.unwrap();
    }
}
