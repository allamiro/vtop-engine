//! Egress-transport conformance battery (#479).
//!
//! One endpoint-gated body run over EVERY registered transport, asserting the
//! same evidence rules hold no matter which wire carried the bytes — the seam
//! changes how bytes travel, never what counts as proof. A transport that
//! quietly weakened verification would fail here.
//!
//! Run against the compose MinIO lab with:
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 cargo test -p vtop-upload \
//!   --test transport_conformance -- --ignored`
//! `VTOP_TEST_S3_BUCKET` may select an existing bucket (defaults to
//! `telemetry-raw`). Cleanup is best-effort: every object is uniquely named
//! (run pid + timestamp + transport) and deleted on the success path, but a
//! panic mid-body may leave one behind — async cleanup cannot run from `Drop`.
//! The unique names make a leaked object easy to find and `mc rm -r` to clear.

use std::io::Write;
use vtop_core::checksum::{blake3_bytes, sha256_bytes};
use vtop_upload::s3_native::transport::TransportRegistry;
use vtop_upload::s3_native::{S3NativeBackend, S3NativeConfig};
use vtop_upload::{ObjectChecksum, UploadBackend};

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint and credentials"]
async fn every_registered_transport_holds_the_evidence_rules() {
    let endpoint = std::env::var("VTOP_TEST_S3_ENDPOINT")
        .expect("VTOP_TEST_S3_ENDPOINT must name the live endpoint");
    let bucket = std::env::var("VTOP_TEST_S3_BUCKET").unwrap_or_else(|_| "telemetry-raw".into());

    // The battery runs over the SAME set the config validator accepts, so a
    // transport that ships must pass it — the list is not hardcoded here.
    for transport in TransportRegistry::with_builtins().names() {
        let backend = S3NativeBackend::new(&S3NativeConfig {
            region: "us-east-1".into(),
            endpoint_url: Some(endpoint.clone()),
            force_path_style: true,
            verify_tls: false,
            transport: transport.clone(),
            tuning: Default::default(),
        })
        .await
        .unwrap_or_else(|e| panic!("[{transport}] backend construction: {e}"));

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let nonce = format!("{}-{nanos}-{transport}", std::process::id());
        let prefix = format!("s3://{bucket}/vtop-transport-conformance/{nonce}");
        let sha_uri = format!("{prefix}-sha256.bin");
        let b3_uri = format!("{prefix}-blake3.bin");
        let payload = b"VTOP transport conformance: proof is independent of the wire";

        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(payload).unwrap();
        file.flush().unwrap();

        // 1. Service-computed SHA-256 verifies as a NON-limited success.
        let sha = sha256_bytes(payload);
        backend
            .put_object(
                file.path(),
                &sha_uri,
                Some(ObjectChecksum::new("sha256", &sha)),
            )
            .await
            .unwrap_or_else(|e| panic!("[{transport}] sha256 put: {e}"));
        let r = backend
            .verify_object(
                &sha_uri,
                payload.len() as u64,
                Some(ObjectChecksum::new("sha256", &sha)),
            )
            .await
            .unwrap_or_else(|e| panic!("[{transport}] sha256 verify: {e}"));
        assert!(
            r.passed,
            "[{transport}] service sha256 must verify: {}",
            r.message
        );
        assert!(
            !r.backend_limited,
            "[{transport}] service sha256 must be non-limited: {}",
            r.message
        );

        // 2. Streamed BLAKE3 verifies as a NON-limited success.
        let b3 = blake3_bytes(payload);
        backend
            .put_object(
                file.path(),
                &b3_uri,
                Some(ObjectChecksum::new("blake3", &b3)),
            )
            .await
            .unwrap_or_else(|e| panic!("[{transport}] blake3 put: {e}"));
        let r = backend
            .verify_object(
                &b3_uri,
                payload.len() as u64,
                Some(ObjectChecksum::new("blake3", &b3)),
            )
            .await
            .unwrap_or_else(|e| panic!("[{transport}] blake3 verify: {e}"));
        assert!(
            r.passed,
            "[{transport}] streamed blake3 must verify: {}",
            r.message
        );
        assert!(
            !r.backend_limited,
            "[{transport}] streamed blake3 must be non-limited: {}",
            r.message
        );

        // 3. A replacement whose digest no longer matches is DETECTED: verifying
        //    the stored body against a different expected checksum must not pass.
        let wrong = sha256_bytes(b"a different body than the one stored");
        let r = backend
            .verify_object(
                &sha_uri,
                payload.len() as u64,
                Some(ObjectChecksum::new("sha256", &wrong)),
            )
            .await
            .unwrap_or_else(|e| panic!("[{transport}] mismatch verify: {e}"));
        assert!(
            !r.passed,
            "[{transport}] a checksum mismatch must fail verification: {}",
            r.message
        );

        // 4. get_object_bounded errors past its cap rather than buffering an
        //    oversized (possibly replaced) body.
        let err = backend
            .get_object_bounded(&sha_uri, payload.len() - 1)
            .await;
        assert!(
            err.is_err(),
            "[{transport}] get_object_bounded must error past its cap"
        );

        backend.delete_object(&sha_uri).await.ok();
        backend.delete_object(&b3_uri).await.ok();
    }
}
