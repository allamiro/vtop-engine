//! #722: what a store does with the requests VTOP's guarantees lean on, asked
//! straight through the SDK, against whichever server `VTOP_TEST_S3_SERVER`
//! names (its capability file says what to expect).
//!
//! - `probe.a_wrong_service_checksum_is_rejected` (`checksum_sha256_enforced`):
//!   a PUT whose `x-amz-checksum-sha256` does not match its body must be
//!   refused, and nothing stored. `store_checksum` verification trusts the
//!   SHA-256 a store reports on HEAD; a store that took the header without
//!   hashing the body, and echoed it back, would pass corrupt data as
//!   verified. No server's documentation says this, so it is asked.
//! - `probe.conditional_put_if_none_match` (`conditional_put`,
//!   informational: VTOP does not use it, decision D14): a second PUT with
//!   `If-None-Match: *` to an existing key must be refused with 412.
//!
//! Run like the other live tests (`tests/s3-compat/README.md`):
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin
//!  AWS_SECRET_ACCESS_KEY=minioadmin cargo test -p vtop-upload
//!  --test live_s3_probes -- --ignored --test-threads=1`

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use std::time::{SystemTime, UNIX_EPOCH};
use vtop_s3_testkit::{Capability, LiveS3, S3Settings};

/// A client straight to the store: the probes need requests the backend
/// never makes.
async fn client(s: &S3Settings) -> aws_sdk_s3::Client {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(s.region.clone()));
    if let Some(endpoint) = &s.endpoint {
        loader = loader.endpoint_url(endpoint);
    }
    let shared = loader.load().await;
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(s.force_path_style)
            .build(),
    )
}

fn key(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("vtop-it-722/{tag}-{}-{nanos}", std::process::id())
}

fn sha256_b64(body: &[u8]) -> String {
    let hex = vtop_core::checksum::sha256_bytes(body);
    let raw: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    B64.encode(raw)
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint and credentials"]
async fn live_a_wrong_service_checksum_is_rejected() {
    let live = LiveS3::from_env();
    let s = &live.settings;
    live.case(
        "probe.a_wrong_service_checksum_is_rejected",
        &[Capability::PutGetHead, Capability::ChecksumSha256Enforced],
        || async {
            let c = client(s).await;
            let key = key("wrong-sha256");
            let body = b"the bytes the store receives";
            // The SHA-256 of other bytes, well formed: only hashing the body
            // can tell it is wrong.
            let wrong = sha256_b64(b"the bytes the checksum describes");
            let put = c
                .put_object()
                .bucket(&s.bucket)
                .key(&key)
                .body(ByteStream::from_static(body))
                .checksum_sha256(&wrong)
                .send()
                .await;
            let stored = c.head_object().bucket(&s.bucket).key(&key).send().await;
            if stored.is_ok() {
                let _ = c.delete_object().bucket(&s.bucket).key(&key).send().await;
            }
            match put {
                Ok(_) => {
                    panic!(
                    "the store accepted a PUT whose x-amz-checksum-sha256 does not match its body \
                     (and {} it): its reported SHA-256 cannot be trusted as evidence",
                    if stored.is_ok() { "stored" } else { "did not store" }
                )
                }
                Err(e) => {
                    let status = e.raw_response().map(|r| r.status().as_u16());
                    assert_eq!(
                        status,
                        Some(400),
                        "a mismatched checksum is the client's error (400), got {status:?}: {:?}",
                        e.code()
                    );
                    assert!(
                        stored.is_err(),
                        "the store refused the PUT, yet {key} exists afterwards"
                    );
                }
            }
        },
    )
    .await;
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint and credentials"]
async fn live_conditional_put_if_none_match() {
    let live = LiveS3::from_env();
    let s = &live.settings;
    live.case(
        "probe.conditional_put_if_none_match",
        &[Capability::PutGetHead, Capability::ConditionalPut],
        || async {
            let c = client(s).await;
            let key = key("if-none-match");
            let put = |body: &'static [u8]| {
                c.put_object()
                    .bucket(&s.bucket)
                    .key(&key)
                    .if_none_match("*")
                    .body(ByteStream::from_static(body))
                    .send()
            };
            put(b"first").await.expect("a PUT to a new key succeeds");
            let second = put(b"second").await;
            let got = c
                .get_object()
                .bucket(&s.bucket)
                .key(&key)
                .send()
                .await
                .unwrap()
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes();
            let _ = c.delete_object().bucket(&s.bucket).key(&key).send().await;
            match second {
                Ok(_) => panic!(
                    "a second PUT with If-None-Match: * to an existing key was accepted; the key \
                     now holds {:?}",
                    String::from_utf8_lossy(&got)
                ),
                Err(e) => {
                    let status = e.raw_response().map(|r| r.status().as_u16());
                    assert_eq!(status, Some(412), "{:?}", e.code());
                    assert_eq!(&got[..], b"first", "the refused PUT changed nothing");
                }
            }
        },
    )
    .await;
}
