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

/// One object, PUT with `algorithm`'s checksum and verified, and deleted.
/// Each case writes its own (review of #764), so no case depends on another
/// having passed.
async fn put_verify_delete(
    s: &S3Settings,
    tag: &str,
    algorithm: &str,
) -> vtop_upload::VerificationResult {
    let backend = S3NativeBackend::new(&native_config(s)).await.unwrap();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let uri = format!(
        "s3://{}/vtop-live-verification/{}-{nanos}-{tag}.bin",
        s.bucket,
        std::process::id()
    );
    let payload = b"VTOP stored-content verification";
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(payload).unwrap();
    file.flush().unwrap();
    let digest = match algorithm {
        "sha256" => sha256_bytes(payload),
        _ => blake3_bytes(payload),
    };
    backend
        .put_object(
            file.path(),
            &uri,
            Some(ObjectChecksum::new(algorithm, &digest)),
        )
        .await
        .unwrap();
    let result = backend
        .verify_object(
            &uri,
            payload.len() as u64,
            Some(ObjectChecksum::new(algorithm, &digest)),
        )
        .await
        .unwrap();
    if let Some(method) = result.method {
        record_method(method.as_str());
    }
    backend.delete_object(&uri).await.unwrap();
    result
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint and credentials"]
async fn native_s3_verifies_service_sha256_and_streamed_blake3() {
    let live = LiveS3::from_env();
    let s = &live.settings;

    // A SHA-256 object verifies strongly, by whichever method the store
    // allows.
    live.case(
        "verification.sha256_verifies_strongly",
        &[Capability::PutGetHead],
        || async {
            let r = put_verify_delete(s, "sha256", "sha256").await;
            assert!(r.passed, "{}", r.message);
            assert!(!r.backend_limited, "{}", r.message);
        },
    )
    .await;

    // The store returned its own SHA-256 of the body, so the receipt says
    // store_checksum; a store that returns none is verified by read-back,
    // which the case above holds to strong.
    live.case(
        "verification.sha256_by_store_checksum",
        &[Capability::PutGetHead, Capability::ChecksumSha256Returned],
        || async {
            let r = put_verify_delete(s, "sha256-method", "sha256").await;
            assert!(r.passed, "{}", r.message);
            assert_eq!(
                r.method,
                Some(VerificationMethod::StoreChecksum),
                "{}",
                r.message
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
            let r = put_verify_delete(s, "blake3", "blake3").await;
            assert!(r.passed, "{}", r.message);
            assert!(!r.backend_limited, "{}", r.message);
        },
    )
    .await;
}
