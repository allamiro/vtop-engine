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
//! - `probe.the_sdks_default_crc32_trailer_is_accepted`
//!   (`checksum_crc32_default`, informational): a PUT of a file with no
//!   checksum of its own, under the SDK's default
//!   `AWS_REQUEST_CHECKSUM_CALCULATION`, goes out as an `aws-chunked` body
//!   with a CRC32 trailer. Garage and Zenko
//!   CloudServer refuse it. VTOP no longer sends it (#778: BLAKE3 and
//!   no-checksum writes carry a SHA-256 header), so no VTOP write depends on
//!   it; it stays asked so a store's answer is on the page, and because
//!   another tool pointed at the same store would meet it.
//!
//! - `probe.a_locked_put_without_an_integrity_header_is_handled_as_the_store_does`
//!   (`object_lock`, `versioning`; informational, nothing is judged): AWS S3
//!   refuses (400) a PUT that asks for Object Lock retention unless it carries
//!   `Content-MD5` or an `x-amz-checksum-*` header (#684, #708). VTOP always
//!   sends one, so no VTOP write depends on the answer. The probe sends a
//!   locked PUT with neither, records whether the store refused it (4xx) or
//!   took it, and passes either way; only another answer (a 5xx, a refused
//!   connection) fails it. A store that takes it is laxer than S3, which
//!   matters to no VTOP write: the line only says how this server differs.
//!
//! Run like the other live tests (`tests/s3-compat/README.md`):
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin
//!  AWS_SECRET_ACCESS_KEY=minioadmin cargo test -p vtop-upload
//!  --test live_s3_probes -- --ignored --test-threads=1`

use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use vtop_s3_testkit::{record_observation, Capability, LiveS3, RefusalAt, S3Settings};
use vtop_upload::s3_native::{S3NativeBackend, S3NativeConfig};
use vtop_upload::{BucketFeature, UploadBackend};

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

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint and credentials"]
async fn live_the_sdks_default_crc32_trailer_is_accepted() {
    let live = LiveS3::from_env();
    let s = &live.settings;
    live.case(
        "probe.the_sdks_default_crc32_trailer_is_accepted",
        &[Capability::PutGetHead, Capability::ChecksumCrc32Default],
        || async {
            let c = client(s).await;
            let key = key("sdk-default-crc32");
            // A file body, as the backend sends one: the SDK cannot hash a
            // streamed body ahead of the request, so under its default it
            // sends the CRC32 as an aws-chunked trailer. (An in-memory body
            // gets the CRC32 as a plain header, which every store takes.)
            let mut f = tempfile::NamedTempFile::new().unwrap();
            std::io::Write::write_all(&mut f, &vec![b'x'; 256 * 1024]).unwrap();
            let body = ByteStream::from_path(f.path()).await.unwrap();
            let put = c
                .put_object()
                .bucket(&s.bucket)
                .key(&key)
                .body(body)
                .send()
                .await;
            if let Err(e) = put {
                let code = e
                    .as_service_error()
                    .and_then(|se| se.code())
                    .map(str::to_owned);
                panic!("the store refused the SDK's default PUT (code {code:?}): {e:?}");
            }
            let _ = c.delete_object().bucket(&s.bucket).key(&key).send().await;
        },
    )
    .await;
}

/// The refusal on a store without Object Lock or versioning (#722; Garage
/// documents both missing): the backend's own answer for the plain bucket,
/// which the engine refuses at startup on (#770), since such a store has no
/// locked bucket for the probe to write to.
async fn store_lacks_it(capability: Capability, at: RefusalAt, s: &S3Settings) {
    assert_eq!(
        at,
        RefusalAt::Startup,
        "VTOP refuses {capability} at startup"
    );
    let backend = S3NativeBackend::new(&S3NativeConfig {
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
        server_side_encryption: None,
    })
    .await
    .unwrap();
    match capability {
        Capability::ObjectLock => assert_eq!(
            backend.bucket_object_lock(&s.bucket).await.unwrap(),
            BucketFeature::NotSupported,
            "the store answers that it does not implement Object Lock, which the engine \
             refuses at startup"
        ),
        Capability::Versioning => assert_ne!(
            backend.bucket_versioning(&s.bucket).await.unwrap(),
            BucketFeature::Enabled,
            "the store does not keep versions, which the hardened profile refuses at startup"
        ),
        other => panic!("no refusal is asserted for {other}"),
    }
}

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint with Object Lock"]
async fn live_a_locked_put_without_an_integrity_header_is_handled_as_the_store_does() {
    let live = LiveS3::from_env();
    let s = &live.settings;
    live.case_with_refusal(
        "probe.a_locked_put_without_an_integrity_header_is_handled_as_the_store_does",
        &[Capability::ObjectLock, Capability::Versioning],
        || async {
            // The SDK computes a checksum for every PUT by default; asking for
            // one only when an operation requires it leaves a PUT of a small
            // in-memory body with neither a checksum nor a Content-MD5.
            let loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_config::Region::new(s.region.clone()));
            let loader = match &s.endpoint {
                Some(endpoint) => loader.endpoint_url(endpoint),
                None => loader,
            };
            let shared = loader.load().await;
            let c = aws_sdk_s3::Client::from_conf(
                aws_sdk_s3::config::Builder::from(&shared)
                    .force_path_style(s.force_path_style)
                    .request_checksum_calculation(
                        aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
                    )
                    .build(),
            );
            let key = key("locked-no-integrity");
            let retain_until = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                + 5;
            let put = c
                .put_object()
                .bucket(&s.locked_bucket)
                .key(&key)
                .body(ByteStream::from_static(
                    b"a locked write with no integrity header",
                ))
                .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
                .object_lock_retain_until_date(aws_sdk_s3::primitives::DateTime::from_secs(
                    retain_until,
                ))
                .send()
                .await;
            match put {
                Ok(out) => {
                    record_observation(
                        "a locked PUT with no Content-MD5 and no checksum header was accepted",
                    );
                    // Governance retention of a few seconds: delete the
                    // version once it has ended, so the bucket is left clean.
                    tokio::time::sleep(Duration::from_secs(7)).await;
                    let mut delete = c.delete_object().bucket(&s.locked_bucket).key(&key);
                    if let Some(version) = out.version_id() {
                        delete = delete.version_id(version);
                    }
                    if let Err(e) = delete.send().await {
                        eprintln!("could not clean up {key}: {e:?}");
                    }
                }
                Err(e) => {
                    let status = e.raw_response().map(|r| r.status().as_u16());
                    assert!(
                        matches!(status, Some(400..=499)),
                        "a locked PUT with no integrity header is either taken or refused as the \
                         client's error (4xx; S3 answers 400); got {status:?} {:?}",
                        e.code()
                    );
                    record_observation(&format!(
                        "a locked PUT with no Content-MD5 and no checksum header was refused \
                         ({} {})",
                        status.unwrap_or_default(),
                        e.code().unwrap_or("no code")
                    ));
                    let stored = c
                        .head_object()
                        .bucket(&s.locked_bucket)
                        .key(&key)
                        .send()
                        .await;
                    assert!(
                        stored.is_err(),
                        "the store refused the PUT, yet {key} exists afterwards"
                    );
                }
            }
        },
        |capability, at| store_lacks_it(capability, at, s),
    )
    .await;
}
