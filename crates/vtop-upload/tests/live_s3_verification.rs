//! Optional live S3-compatible verification test.
//!
//! Run against the compose MinIO lab with:
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 cargo test -p vtop-upload --test live_s3_verification -- --ignored`
//! `VTOP_TEST_S3_BUCKET` may select an existing bucket (defaults to
//! `telemetry-raw`). The test deletes both uniquely named objects it creates.

use std::io::Write;
use vtop_core::checksum::{blake3_bytes, sha256_bytes};
use vtop_upload::s3_native::{S3NativeBackend, S3NativeConfig};
use vtop_upload::{ObjectChecksum, UploadBackend};

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint and credentials"]
async fn native_s3_verifies_service_sha256_and_streamed_blake3() {
    let endpoint = std::env::var("VTOP_TEST_S3_ENDPOINT")
        .expect("VTOP_TEST_S3_ENDPOINT must name the live endpoint");
    let bucket = std::env::var("VTOP_TEST_S3_BUCKET").unwrap_or_else(|_| "telemetry-raw".into());
    let backend = S3NativeBackend::new(&S3NativeConfig {
        region: "us-east-1".into(),
        endpoint_url: Some(endpoint),
        force_path_style: true,
        verify_tls: false,
    })
    .await
    .unwrap();

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let nonce = format!("{}-{nanos}", std::process::id());
    let sha_uri = format!("s3://{bucket}/vtop-live-verification/{nonce}-sha256.bin");
    let b3_uri = format!("s3://{bucket}/vtop-live-verification/{nonce}-blake3.bin");
    let payload = b"VTOP stored-content verification";
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(payload).unwrap();
    file.flush().unwrap();

    let sha = sha256_bytes(payload);
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
    assert!(sha_result.passed, "{}", sha_result.message);
    assert!(!sha_result.backend_limited, "{}", sha_result.message);

    let b3 = blake3_bytes(payload);
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
    assert!(b3_result.passed, "{}", b3_result.message);
    assert!(!b3_result.backend_limited, "{}", b3_result.message);

    backend.delete_object(&sha_uri).await.unwrap();
    backend.delete_object(&b3_uri).await.unwrap();
}
