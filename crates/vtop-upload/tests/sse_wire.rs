//! What the S3 backend puts on the wire for `upload.server_side_encryption`,
//! and how it reads the store's answer.
//!
//! The real SDK against a local socket that records every request and answers
//! as a store would. It pins:
//!
//! - every object PUT, manifest PUT and CreateMultipartUpload carries the
//!   configured `x-amz-server-side-encryption` headers, UploadPart carries
//!   none, and with `none` no request carries any;
//! - `sse-kms` with no `kms_key_id` sends `aws:kms` and no key id header, so
//!   the store applies its default key;
//! - HeadObject's encryption headers are read into the object head;
//! - verification FAILS, as a checksum mismatch does, when the store reports
//!   no encryption, another algorithm, or another key, and passes when the
//!   report matches, with the store's own SHA-256 still the method.
//!
//! Its own test binary because it sets credentials in the process
//! environment.

use std::io::Write;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vtop_core::config::{ServerSideEncryptionConfig, ServerSideEncryptionMode};
use vtop_upload::s3_native::{S3NativeBackend, S3NativeConfig};
use vtop_upload::{ObjectChecksum, UploadBackend, VerificationMethod};

#[derive(Debug, Clone)]
struct Seen {
    method: String,
    headers: Vec<(String, String)>,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    /// Every encryption header on the request, by name.
    fn sse_headers(&self) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(k, _)| k.starts_with("x-amz-server-side-encryption"))
            .map(|(k, _)| k.as_str())
            .collect()
    }
}

#[derive(Clone, Default)]
struct Store {
    seen: Arc<Mutex<Vec<Seen>>>,
    head_headers: Arc<Mutex<Vec<(String, String)>>>,
}

impl Store {
    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
    fn answer_head(&self, headers: &[(&str, &str)]) {
        *self.head_headers.lock().unwrap() = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
    }

    async fn start(&self) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let this = self.clone();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let this = this.clone();
                tokio::spawn(async move { this.serve(socket).await });
            }
        });
        endpoint
    }

    async fn serve(&self, mut socket: tokio::net::TcpStream) {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 16 * 1024];
        let head_end = loop {
            if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let mut lines = head.split("\r\n");
        let mut request_line = lines.next().unwrap_or_default().split(' ');
        let method = request_line.next().unwrap_or_default().to_string();
        let target = request_line.next().unwrap_or_default().to_string();
        let headers: Vec<(String, String)> = lines
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
            .collect();
        let seen = Seen {
            method: method.clone(),
            headers,
        };
        let mut body = buf[head_end..].to_vec();
        if let Some(len) = seen
            .header("content-length")
            .and_then(|v| v.parse::<usize>().ok())
        {
            while body.len() < len {
                match socket.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => body.extend_from_slice(&chunk[..n]),
                }
            }
        } else if seen
            .header("transfer-encoding")
            .is_some_and(|v| v.contains("chunked"))
        {
            while !body.windows(5).any(|w| w == b"0\r\n\r\n") && !body.ends_with(b"\r\n\r\n") {
                match socket.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => body.extend_from_slice(&chunk[..n]),
                }
            }
        }
        self.seen.lock().unwrap().push(seen);

        let (status, extra, body): (&str, Vec<(String, String)>, String) = if method == "PUT" {
            (
                "200 OK",
                vec![
                    ("x-amz-version-id".into(), "ver-put".into()),
                    ("etag".into(), "\"e\"".into()),
                ],
                String::new(),
            )
        } else if method == "POST" && target.contains("uploads") {
            (
                "200 OK",
                vec![("content-type".into(), "application/xml".into())],
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><InitiateMultipartUploadResult>\
                 <Bucket>b</Bucket><Key>k</Key><UploadId>up-1</UploadId>\
                 </InitiateMultipartUploadResult>"
                    .into(),
            )
        } else if method == "HEAD" {
            let mut h = vec![
                ("content-length".into(), "5".into()),
                ("etag".into(), "\"e\"".into()),
            ];
            h.extend(self.head_headers.lock().unwrap().clone());
            ("200 OK", h, String::new())
        } else {
            ("500 Internal Server Error", vec![], String::new())
        };
        let mut response = format!("HTTP/1.1 {status}\r\nconnection: close\r\n");
        for (k, v) in &extra {
            if method == "HEAD" || k != "content-length" {
                response.push_str(&format!("{k}: {v}\r\n"));
            }
        }
        if method != "HEAD" {
            response.push_str(&format!("content-length: {}\r\n", body.len()));
        }
        response.push_str("\r\n");
        response.push_str(&body);
        let _ = socket.write_all(response.as_bytes()).await;
        let _ = socket.shutdown().await;
    }
}

fn sse(
    mode: ServerSideEncryptionMode,
    key: Option<&str>,
    bucket_key: bool,
) -> Option<ServerSideEncryptionConfig> {
    Some(ServerSideEncryptionConfig {
        mode,
        kms_key_id: key.map(str::to_owned),
        bucket_key,
    })
}

async fn backend(endpoint: String, sse: Option<ServerSideEncryptionConfig>) -> S3NativeBackend {
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
        max_egress_bytes_per_second: None,
        object_lock: None,
        server_side_encryption: sse,
    })
    .await
    .expect("the backend builds against a plaintext lab endpoint")
}

fn file(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(bytes).unwrap();
    f.flush().unwrap();
    f
}

/// The headers the object PUT, the manifest PUT, the CreateMultipartUpload and
/// the UploadPart carried, in that order.
async fn drive(sse: Option<ServerSideEncryptionConfig>) -> Vec<Seen> {
    let store = Store::default();
    let endpoint = store.start().await;
    let b = backend(endpoint, sse).await;
    let body = file(b"hello");
    let digest = vtop_core::checksum::sha256_bytes(b"hello");
    let ck = ObjectChecksum::new("sha256", &digest);
    b.put_object(body.path(), "s3://b/a/obj.raw", Some(ck))
        .await
        .unwrap();
    b.put_manifest(body.path(), "s3://b/a/obj.manifest.json", Some(ck))
        .await
        .unwrap();
    let created = b
        .create_multipart_upload("s3://b/a/big.raw", "application/octet-stream", Some(ck))
        .await
        .unwrap();
    b.upload_part(
        "s3://b/a/big.raw",
        &created.upload_id,
        1,
        bytes::Bytes::from_static(b"hello"),
    )
    .await
    .unwrap();
    let seen = store.seen();
    assert_eq!(seen.len(), 4, "{seen:#?}");
    assert_eq!(
        seen.iter().map(|r| r.method.as_str()).collect::<Vec<_>>(),
        ["PUT", "PUT", "POST", "PUT"],
        "{seen:#?}"
    );
    seen
}

#[tokio::test]
async fn sse_s3_puts_aes256_on_every_write_and_none_on_a_part() {
    let seen = drive(sse(ServerSideEncryptionMode::SseS3, None, false)).await;
    for (request, what) in seen
        .iter()
        .zip(["object PUT", "manifest PUT", "CreateMultipartUpload"])
    {
        assert_eq!(
            request.header("x-amz-server-side-encryption"),
            Some("AES256"),
            "{what}: {request:?}"
        );
        assert_eq!(
            request.sse_headers(),
            ["x-amz-server-side-encryption"],
            "{what}: no key and no bucket key"
        );
    }
    assert!(
        seen[3].sse_headers().is_empty(),
        "UploadPart takes its encryption from the create: {:?}",
        seen[3]
    );
}

#[tokio::test]
async fn sse_kms_puts_the_key_and_the_bucket_key_on_every_write() {
    let seen = drive(sse(
        ServerSideEncryptionMode::SseKms,
        Some("arn:aws:kms:eu-west-1:111122223333:key/abc"),
        true,
    ))
    .await;
    for (request, what) in seen
        .iter()
        .zip(["object PUT", "manifest PUT", "CreateMultipartUpload"])
    {
        assert_eq!(
            request.header("x-amz-server-side-encryption"),
            Some("aws:kms"),
            "{what}: {request:?}"
        );
        assert_eq!(
            request.header("x-amz-server-side-encryption-aws-kms-key-id"),
            Some("arn:aws:kms:eu-west-1:111122223333:key/abc"),
            "{what}: {request:?}"
        );
        assert_eq!(
            request.header("x-amz-server-side-encryption-bucket-key-enabled"),
            Some("true"),
            "{what}: {request:?}"
        );
    }
    assert!(seen[3].sse_headers().is_empty(), "{:?}", seen[3]);
}

/// The store applies its default KMS key (on AWS, the account's AWS-managed
/// `aws/s3` key, which has no monthly charge) only when the request names no
/// key: the algorithm header must be there and the key id header must not.
#[tokio::test]
async fn sse_kms_with_no_key_id_sends_aws_kms_and_no_key_header() {
    let seen = drive(sse(ServerSideEncryptionMode::SseKms, None, false)).await;
    for (request, what) in seen
        .iter()
        .zip(["object PUT", "manifest PUT", "CreateMultipartUpload"])
    {
        assert_eq!(
            request.sse_headers(),
            ["x-amz-server-side-encryption"],
            "{what}: {request:?}"
        );
        assert_eq!(
            request.header("x-amz-server-side-encryption"),
            Some("aws:kms"),
            "{what}"
        );
    }
}

#[tokio::test]
async fn without_the_setting_no_request_carries_an_encryption_header() {
    for none in [None, sse(ServerSideEncryptionMode::None, None, false)] {
        for request in drive(none).await {
            assert!(request.sse_headers().is_empty(), "{request:?}");
        }
    }
}

/// The store answers a HEAD with these headers; what does verification say?
async fn verdict(
    configured: Option<ServerSideEncryptionConfig>,
    head: &[(&str, &str)],
) -> vtop_upload::VerificationResult {
    let store = Store::default();
    // The store's own SHA-256 over the five bytes: the strong path.
    let digest = vtop_core::checksum::sha256_bytes(b"hello");
    let raw: Vec<u8> = (0..digest.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&digest[i..i + 2], 16).unwrap())
        .collect();
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
    let mut headers = vec![("x-amz-checksum-sha256", b64.as_str())];
    headers.extend_from_slice(head);
    store.answer_head(&headers);
    let endpoint = store.start().await;
    let b = backend(endpoint, configured).await;
    b.verify_object(
        "s3://b/a/obj.raw",
        5,
        Some(ObjectChecksum::new("sha256", &digest)),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn head_reports_the_encryption_the_store_states() {
    let store = Store::default();
    store.answer_head(&[
        ("x-amz-server-side-encryption", "aws:kms"),
        (
            "x-amz-server-side-encryption-aws-kms-key-id",
            "arn:aws:kms:r:1:key/abc",
        ),
    ]);
    let endpoint = store.start().await;
    let b = backend(endpoint, None).await;
    let head = b.head_object("s3://b/a/obj.raw").await.unwrap();
    let e = head.encryption.expect("the headers are read");
    assert_eq!(e.algorithm, "aws:kms");
    assert_eq!(e.kms_key_id.as_deref(), Some("arn:aws:kms:r:1:key/abc"));

    store.answer_head(&[]);
    let head = b.head_object("s3://b/a/obj.raw").await.unwrap();
    assert!(head.encryption.is_none(), "no header is no encryption");
}

/// A write that should be encrypted and is not must never be committed: the
/// verification fails, the way a checksum mismatch does, although the size
/// and the store's SHA-256 are right.
#[tokio::test]
async fn a_store_that_reports_no_encryption_fails_verification() {
    for configured in [
        sse(ServerSideEncryptionMode::SseS3, None, false),
        sse(ServerSideEncryptionMode::SseKms, None, false),
        sse(ServerSideEncryptionMode::SseKms, Some("key-1"), false),
    ] {
        let result = verdict(configured.clone(), &[]).await;
        assert!(
            !result.passed,
            "{configured:?}: an unencrypted object passed: {result:?}"
        );
        assert!(
            result.message.contains("encryption at rest missing"),
            "{}",
            result.message
        );
    }
}

#[tokio::test]
async fn another_algorithm_or_another_key_fails_verification() {
    let kms = [("x-amz-server-side-encryption", "aws:kms")];
    let wrong_alg = verdict(sse(ServerSideEncryptionMode::SseS3, None, false), &kms).await;
    assert!(
        !wrong_alg.passed && wrong_alg.message.contains("differs"),
        "{wrong_alg:?}"
    );
    let aes = [("x-amz-server-side-encryption", "AES256")];
    let wrong_alg = verdict(sse(ServerSideEncryptionMode::SseKms, None, false), &aes).await;
    assert!(!wrong_alg.passed, "{wrong_alg:?}");

    let other_key = verdict(
        sse(ServerSideEncryptionMode::SseKms, Some("key-1"), false),
        &[
            ("x-amz-server-side-encryption", "aws:kms"),
            (
                "x-amz-server-side-encryption-aws-kms-key-id",
                "arn:aws:kms:eu-west-1:111122223333:key/key-2",
            ),
        ],
    )
    .await;
    assert!(
        !other_key.passed && other_key.message.contains("key differs"),
        "{other_key:?}"
    );
}

/// With the report matching, the store's own SHA-256 still decides: SSE does
/// not move verification off `store_checksum` or past a digest that is wrong.
#[tokio::test]
async fn a_matching_report_passes_and_keeps_the_stores_checksum_as_the_method() {
    let arn = "arn:aws:kms:eu-west-1:111122223333:key/abc-123";
    for (configured, head) in [
        (
            sse(ServerSideEncryptionMode::SseS3, None, false),
            vec![("x-amz-server-side-encryption", "AES256")],
        ),
        // No key id: any aws:kms object, the account's aws/s3 key included.
        (
            sse(ServerSideEncryptionMode::SseKms, None, false),
            vec![
                ("x-amz-server-side-encryption", "aws:kms"),
                ("x-amz-server-side-encryption-aws-kms-key-id", arn),
            ],
        ),
        // A key id matches the ARN the store reports, and the ARN itself.
        (
            sse(ServerSideEncryptionMode::SseKms, Some("abc-123"), true),
            vec![
                ("x-amz-server-side-encryption", "aws:kms"),
                ("x-amz-server-side-encryption-aws-kms-key-id", arn),
            ],
        ),
        (
            sse(ServerSideEncryptionMode::SseKms, Some(arn), false),
            vec![
                ("x-amz-server-side-encryption", "aws:kms"),
                ("x-amz-server-side-encryption-aws-kms-key-id", arn),
            ],
        ),
    ] {
        let result = verdict(configured.clone(), &head).await;
        assert!(result.passed, "{configured:?}: {result:?}");
        assert_eq!(
            result.method,
            Some(VerificationMethod::StoreChecksum),
            "{configured:?}: {result:?}"
        );
    }
}
