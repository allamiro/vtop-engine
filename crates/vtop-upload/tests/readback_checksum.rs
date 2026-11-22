//! #735: a store that returns other bytes than it holds, while the engine reads
//! an object back to verify it, has failed the verification.
//!
//! The SDK checks every GET body against the checksum the store sends with it
//! (`x-amz-checksum-*`) while the body streams, and stops the read with an I/O
//! error ("streaming error") when they differ. The read-back verification
//! (BLAKE3, and SHA-256 for a multipart object) used to pass that error up as it
//! is: the engine saw a failed read, not a failed verification, so the batch
//! was not marked FAILED, `vtop_verification_failures_total` did not count it
//! and `VtopVerificationFailures` did not fire. The batch was never committed
//! either way; what was lost was the signal.
//!
//! The real SDK against a socket that answers a HEAD with the object's size
//! and a GET with altered bytes under the checksum of the original. Its own
//! test binary because it sets credentials in the process environment.

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vtop_core::checksum::digest_bytes;
use vtop_core::ChecksumAlgorithm;
use vtop_upload::s3_native::{S3NativeBackend, S3NativeConfig};
use vtop_upload::{ObjectChecksum, UploadBackend};

const BODY: &[u8] = b"payload";

/// CRC-32 (IEEE), the checksum the SDK validates a response with by default.
fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// A store that holds `BODY`, answers a HEAD with its size and a GET with
/// `served` under the checksum of `BODY`.
async fn store_serving(served: &'static [u8]) -> String {
    store_serving_with_length(served, BODY.len()).await
}

/// The same, with the `content-length` the GET claims (a store that drops the
/// connection mid-body claims more than it sends).
async fn store_serving_with_length(served: &'static [u8], get_length: usize) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let checksum = B64.encode(crc32(BODY).to_be_bytes());
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let checksum = checksum.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let head = request.starts_with(b"HEAD ");
                let length = if head { BODY.len() } else { get_length };
                let header = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {length}\r\nx-amz-checksum-crc32: {checksum}\r\nconnection: close\r\n\r\n"
                );
                let _ = socket.write_all(header.as_bytes()).await;
                if !head {
                    let _ = socket.write_all(served).await;
                }
                let _ = socket.shutdown().await;
            });
        }
    });
    endpoint
}

async fn backend(endpoint: String) -> S3NativeBackend {
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    S3NativeBackend::new(&S3NativeConfig {
        region: "us-east-1".into(),
        endpoint_url: Some(endpoint),
        force_path_style: true,
        verify_tls: false,
        transport: "tcp_tls".into(),
        tuning: Default::default(),
        request_timeout_seconds: 5,
        request_min_bytes_per_second: 4 * 1024,
        object_lock: None,
        server_side_encryption: None,
        max_egress_bytes_per_second: None,
    })
    .await
    .expect("the backend builds against a plaintext lab endpoint")
}

#[tokio::test]
async fn an_altered_read_back_is_a_failed_verification_not_an_io_error() {
    let uri = "s3://telemetry-raw/x/vtop-src-b0-10-abc.raw";
    let digest = digest_bytes(ChecksumAlgorithm::Blake3, BODY).unwrap();

    let store = backend(store_serving(b"pXyload").await).await;
    let result = store
        .verify_object(
            uri,
            BODY.len() as u64,
            Some(ObjectChecksum::new("blake3", &digest)),
        )
        .await
        .expect(
            "a body that fails its checksum is a verdict, not an error the engine cannot classify",
        );
    assert!(
        !result.passed,
        "an altered read-back must not verify: {}",
        result.message
    );
    assert!(
        result.message.to_ascii_lowercase().contains("checksum"),
        "the message says why: {}",
        result.message
    );
}

#[tokio::test]
async fn an_intact_read_back_still_verifies() {
    let uri = "s3://telemetry-raw/x/vtop-src-b0-10-abc.raw";
    let digest = digest_bytes(ChecksumAlgorithm::Blake3, BODY).unwrap();

    let store = backend(store_serving(BODY).await).await;
    let result = store
        .verify_object(
            uri,
            BODY.len() as u64,
            Some(ObjectChecksum::new("blake3", &digest)),
        )
        .await
        .unwrap();
    assert!(result.passed, "{}", result.message);
}

#[tokio::test]
async fn a_read_back_that_drops_mid_body_is_still_an_error() {
    // A connection that ends before the body does says nothing about the
    // object: it is not a failed verification, and the engine must keep
    // treating it as a store it could not read.
    let uri = "s3://telemetry-raw/x/vtop-src-b0-10-abc.raw";
    let digest = digest_bytes(ChecksumAlgorithm::Blake3, BODY).unwrap();

    let store = backend(store_serving_with_length(b"pay", BODY.len()).await).await;
    let outcome = store
        .verify_object(
            uri,
            BODY.len() as u64,
            Some(ObjectChecksum::new("blake3", &digest)),
        )
        .await;
    assert!(
        outcome.is_err(),
        "a truncated read is an error, not a verdict: {outcome:?}"
    );
}
