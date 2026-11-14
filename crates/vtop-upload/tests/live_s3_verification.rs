//! Optional live S3-compatible verification test.
//!
//! Run against the compose MinIO lab with:
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 cargo test -p vtop-upload --test live_s3_verification -- --ignored`
//! `VTOP_TEST_S3_BUCKET` may select an existing bucket (defaults to
//! `telemetry-raw`). The test deletes both uniquely named objects it creates.
//!
//! The server, and what it is expected to do, come from `VTOP_TEST_S3_SERVER`
//! and its capability file (#722, `tests/s3-compat/README.md`); the region,
//! addressing and TLS flag from `VTOP_TEST_S3_*`.

use std::io::Write;
use vtop_core::checksum::{blake3_bytes, sha256_bytes};
use vtop_s3_testkit::{record_method, Capability, LiveS3, S3Settings};
use vtop_upload::s3_native::{S3NativeBackend, S3NativeConfig};
use vtop_upload::{ObjectChecksum, UploadBackend, VerificationMethod};

fn native_config(s: &S3Settings) -> S3NativeConfig {
    S3NativeConfig {
        region: s.region.clone(),
        endpoint_url: s.endpoint.clone(),
        force_path_style: s.force_path_style,
        verify_tls: s.verify_tls,
        transport: "tcp_tls".into(),
        tuning: Default::default(),
        max_egress_bytes_per_second: None,
        request_timeout_seconds: 300,
        request_min_bytes_per_second: 4 * 1024,
        object_lock: None,
    }
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint and credentials"]
async fn native_s3_verifies_service_sha256_and_streamed_blake3() {
    let live = LiveS3::from_env();
    let s = &live.settings;
    let backend = S3NativeBackend::new(&native_config(s)).await.unwrap();

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let nonce = format!("{}-{nanos}", std::process::id());
    let sha_uri = format!(
        "s3://{}/vtop-live-verification/{nonce}-sha256.bin",
        s.bucket
    );
    let b3_uri = format!(
        "s3://{}/vtop-live-verification/{nonce}-blake3.bin",
        s.bucket
    );
    let payload = b"VTOP stored-content verification";
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(payload).unwrap();
    file.flush().unwrap();
    let sha = sha256_bytes(payload);
    let b3 = blake3_bytes(payload);

    // A SHA-256 object verifies strongly, by whichever method the store
    // allows.
    live.case(
        "verification.sha256_verifies_strongly",
        &[Capability::PutGetHead],
        || async {
            backend
                .put_object(
                    file.path(),
                    &sha_uri,
                    Some(ObjectChecksum::new("sha256", &sha)),
                )
                .await
                .unwrap();
            let sha_result = backend
                .verify_object(
                    &sha_uri,
                    payload.len() as u64,
                    Some(ObjectChecksum::new("sha256", &sha)),
                )
                .await
                .unwrap();
            if let Some(method) = sha_result.method {
                record_method(method.as_str());
            }
            assert!(sha_result.passed, "{}", sha_result.message);
            assert!(!sha_result.backend_limited, "{}", sha_result.message);
        },
    )
    .await;

    // The store returned its own SHA-256 of the body, so the receipt says
    // store_checksum; a store that returns none is verified by read-back,
    // which the case above already holds to strong.
    live.case(
        "verification.sha256_by_store_checksum",
        &[Capability::PutGetHead, Capability::ChecksumSha256Returned],
        || async {
            let sha_result = backend
                .verify_object(
                    &sha_uri,
                    payload.len() as u64,
                    Some(ObjectChecksum::new("sha256", &sha)),
                )
                .await
                .unwrap();
            if let Some(method) = sha_result.method {
                record_method(method.as_str());
            }
            assert_eq!(
                sha_result.method,
                Some(VerificationMethod::StoreChecksum),
                "{}",
                sha_result.message
            );
        },
    )
    .await;

    // BLAKE3 has no service checksum: the SDK's default CRC32 goes on the
    // PUT, and the verification is a bounded read-back, still strong.
    live.case(
        "verification.blake3_by_read_back",
        &[Capability::PutGetHead, Capability::ChecksumCrc32Default],
        || async {
            backend
                .put_object(
                    file.path(),
                    &b3_uri,
                    Some(ObjectChecksum::new("blake3", &b3)),
                )
                .await
                .unwrap();
            let b3_result = backend
                .verify_object(
                    &b3_uri,
                    payload.len() as u64,
                    Some(ObjectChecksum::new("blake3", &b3)),
                )
                .await
                .unwrap();
            if let Some(method) = b3_result.method {
                record_method(method.as_str());
            }
            assert!(b3_result.passed, "{}", b3_result.message);
            assert!(!b3_result.backend_limited, "{}", b3_result.message);
        },
    )
    .await;

    backend.delete_object(&sha_uri).await.ok();
    backend.delete_object(&b3_uri).await.ok();
}
