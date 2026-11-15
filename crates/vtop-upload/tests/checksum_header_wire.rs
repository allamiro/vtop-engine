//! #778: the BLAKE3 and no-checksum writes carry a whole-object checksum
//! HEADER the backend computed itself, never the SDK's aws-chunked trailer.
//!
//! Garage v2.5.0 answers an `aws-chunked` body with a CRC32 trailer with
//! `400 InvalidRequest: Invalid payload signature`, and Zenko CloudServer
//! 9.5.5 with `400 BadRequest`. The SHA-256 path already sent
//! `x-amz-checksum-sha256` itself and passed on every store in the #722
//! matrix, so the other two paths now send the same header. The real SDK runs
//! against a local socket that answers as such a store would. It pins:
//!
//! - against a store that refuses trailers, a BLAKE3 write and a write with no
//!   checksum succeed, each carrying `x-amz-checksum-sha256` equal to the
//!   body's SHA-256 and no `x-amz-trailer`, plain or under Object Lock;
//! - a locked write still names its algorithm (`x-amz-sdk-checksum-algorithm`),
//!   the integrity header S3 requires under retention (#708), and with
//!   `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` it still carries the
//!   value, because the profile pins the calculation on;
//! - without the profile, `when_required` sends no checksum header at all: the
//!   operator's way out for a store that refuses checksum headers too;
//! - a store that refuses the header fails the write with an error that says
//!   what happened, what the engine did and what to do, naming
//!   `AWS_REQUEST_CHECKSUM_CALCULATION`; a `BadDigest`, which is the store
//!   catching a corrupted body, does not get that advice.
//!
//! Its own test binary because it sets variables in the process environment;
//! the tests that do take [`ENV`] so no backend is built while one is set.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vtop_upload::s3_native::{S3NativeBackend, S3NativeConfig};
use vtop_upload::{ObjectChecksum, ObjectLockMode, ObjectLockPolicy, UploadBackend};

/// Held by every test: the process environment is shared.
static ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// What the stub store refuses.
#[derive(Clone, Copy, PartialEq)]
enum Refuses {
    /// An `aws-chunked` body or a trailer, as Garage and CloudServer do.
    Trailer,
    /// Any `x-amz-checksum-*` header, as a store with no checksum support
    /// might.
    ChecksumHeader,
    /// Every PUT, with `BadDigest`: the store found the body did not match.
    BadDigest,
}

#[derive(Debug, Clone)]
struct Seen {
    headers: Vec<(String, String)>,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    fn checksum_headers(&self) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(k, _)| k.starts_with("x-amz-checksum-"))
            .map(|(k, _)| k.as_str())
            .collect()
    }
}

#[derive(Clone)]
struct Store {
    refuses: Refuses,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Store {
    fn new(refuses: Refuses) -> Self {
        Self {
            refuses,
            seen: Arc::default(),
        }
    }

    fn last(&self) -> Seen {
        self.seen
            .lock()
            .unwrap()
            .last()
            .cloned()
            .expect("the store saw a request")
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
        let headers: Vec<(String, String)> = head
            .split("\r\n")
            .skip(1)
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
            .collect();
        let seen = Seen { headers };
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
        }
        let trailer = seen.header("x-amz-trailer").is_some()
            || seen
                .header("content-encoding")
                .is_some_and(|v| v.contains("aws-chunked"));
        let checksum_header = !seen.checksum_headers().is_empty();
        self.seen.lock().unwrap().push(seen);

        let refusal = match self.refuses {
            Refuses::Trailer if trailer => Some((
                "400 Bad Request",
                "InvalidRequest",
                "Bad request: Invalid payload signature",
            )),
            Refuses::ChecksumHeader if checksum_header => Some((
                "400 Bad Request",
                "InvalidRequest",
                "Bad request: unsupported checksum header",
            )),
            Refuses::BadDigest => Some((
                "400 Bad Request",
                "BadDigest",
                "The Content-SHA256 you specified did not match what we received.",
            )),
            _ => None,
        };
        let response = match refusal {
            Some((status, code, message)) => {
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code>\
                     <Message>{message}</Message></Error>"
                );
                format!(
                    "HTTP/1.1 {status}\r\nconnection: close\r\ncontent-type: application/xml\r\n\
                     content-length: {}\r\n\r\n{body}",
                    body.len()
                )
            }
            None => "HTTP/1.1 200 OK\r\nconnection: close\r\netag: \"e\"\r\n\
                     content-length: 0\r\n\r\n"
                .to_string(),
        };
        let _ = socket.write_all(response.as_bytes()).await;
        let _ = socket.shutdown().await;
    }
}

async fn backend(endpoint: String, lock: Option<ObjectLockPolicy>) -> S3NativeBackend {
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
        object_lock: lock,
    })
    .await
    .expect("the backend builds against a plaintext lab endpoint")
}

/// A backend built with `AWS_REQUEST_CHECKSUM_CALCULATION=when_required`.
async fn backend_when_required(
    endpoint: String,
    lock: Option<ObjectLockPolicy>,
) -> S3NativeBackend {
    std::env::set_var("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required");
    let built = backend(endpoint, lock).await;
    std::env::remove_var("AWS_REQUEST_CHECKSUM_CALCULATION");
    built
}

fn lock() -> ObjectLockPolicy {
    ObjectLockPolicy::with_retention_for_tests(
        ObjectLockMode::Governance,
        Duration::from_secs(86_400),
        false,
    )
}

/// Large enough that a body would be sent in more than one chunk.
fn payload() -> Vec<u8> {
    (0..200_000u32).map(|i| (i % 251) as u8).collect()
}

fn file(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(bytes).unwrap();
    f.flush().unwrap();
    f
}

/// `x-amz-checksum-sha256` as S3 spells it: base64 of the raw digest.
fn sha256_b64(bytes: &[u8]) -> String {
    let hex = vtop_core::checksum::sha256_bytes(bytes);
    aws_smithy_types::base64::encode(hex::decode(hex).unwrap())
}

/// The two paths #778 is about: BLAKE3, and no checksum at all.
fn non_sha256_paths(bytes: &[u8]) -> Vec<(&'static str, Option<(String, String)>)> {
    vec![
        (
            "blake3",
            Some(("blake3".into(), vtop_core::checksum::blake3_bytes(bytes))),
        ),
        ("none", None),
    ]
}

fn assert_whole_body_header(request: &Seen, bytes: &[u8], what: &str) {
    assert_eq!(
        request.header("x-amz-trailer"),
        None,
        "{what}: no trailer: {request:#?}"
    );
    assert!(
        !request
            .header("content-encoding")
            .is_some_and(|v| v.contains("aws-chunked")),
        "{what}: no aws-chunked body: {request:#?}"
    );
    assert_eq!(
        request.header("x-amz-checksum-sha256"),
        Some(sha256_b64(bytes).as_str()),
        "{what}: the body's SHA-256, as a header: {request:#?}"
    );
}

#[tokio::test]
async fn a_blake3_or_unchecked_write_to_a_store_that_refuses_trailers_succeeds_with_a_header() {
    let _env = ENV.lock().await;
    let store = Store::new(Refuses::Trailer);
    let plain = backend(store.start().await, None).await;
    let bytes = payload();
    let body = file(&bytes);
    for (path, checksum) in non_sha256_paths(&bytes) {
        let checksum = checksum
            .as_ref()
            .map(|(algo, hex)| ObjectChecksum::new(algo, hex));
        plain
            .put_object(body.path(), &format!("s3://raw/{path}.bin"), checksum)
            .await
            .unwrap_or_else(|e| panic!("{path}: a store that refuses trailers took it: {e}"));
        assert_whole_body_header(&store.last(), &bytes, path);
        plain
            .put_manifest(body.path(), &format!("s3://raw/{path}.json"), checksum)
            .await
            .unwrap_or_else(|e| panic!("{path} manifest: {e}"));
        assert_whole_body_header(&store.last(), &bytes, &format!("{path} manifest"));
    }
}

#[tokio::test]
async fn a_locked_write_keeps_its_integrity_header_and_sends_no_trailer() {
    let _env = ENV.lock().await;
    let bytes = payload();
    let body = file(&bytes);
    for when_required in [false, true] {
        let store = Store::new(Refuses::Trailer);
        let endpoint = store.start().await;
        let locked = if when_required {
            backend_when_required(endpoint, Some(lock())).await
        } else {
            backend(endpoint, Some(lock())).await
        };
        for (path, checksum) in non_sha256_paths(&bytes) {
            let what = format!("{path}, locked, when_required={when_required}");
            let checksum = checksum
                .as_ref()
                .map(|(algo, hex)| ObjectChecksum::new(algo, hex));
            locked
                .put_object(body.path(), &format!("s3://locked/{path}.bin"), checksum)
                .await
                .unwrap_or_else(|e| panic!("{what}: {e}"));
            let request = store.last();
            assert_eq!(
                request.header("x-amz-object-lock-mode"),
                Some("GOVERNANCE"),
                "{what}"
            );
            assert_eq!(
                request.header("x-amz-sdk-checksum-algorithm"),
                Some("SHA256"),
                "{what}: a write under retention names its algorithm (#708): {request:#?}"
            );
            assert_whole_body_header(&request, &bytes, &what);
        }
    }
}

#[tokio::test]
async fn when_required_without_the_lock_profile_sends_no_checksum_header() {
    let _env = ENV.lock().await;
    let store = Store::new(Refuses::ChecksumHeader);
    let plain = backend_when_required(store.start().await, None).await;
    let bytes = payload();
    let body = file(&bytes);
    for (path, checksum) in non_sha256_paths(&bytes) {
        let checksum = checksum
            .as_ref()
            .map(|(algo, hex)| ObjectChecksum::new(algo, hex));
        plain
            .put_object(body.path(), &format!("s3://raw/{path}.bin"), checksum)
            .await
            .unwrap_or_else(|e| panic!("{path}: {e}"));
        let request = store.last();
        assert!(
            request.checksum_headers().is_empty() && request.header("x-amz-trailer").is_none(),
            "{path}: when_required sends no checksum: {request:#?}"
        );
    }
}

#[tokio::test]
async fn a_store_that_refuses_the_header_fails_the_write_naming_the_variable() {
    let _env = ENV.lock().await;
    let store = Store::new(Refuses::ChecksumHeader);
    let plain = backend(store.start().await, None).await;
    let bytes = payload();
    let body = file(&bytes);
    for (path, checksum) in non_sha256_paths(&bytes) {
        let checksum = checksum
            .as_ref()
            .map(|(algo, hex)| ObjectChecksum::new(algo, hex));
        let err = plain
            .put_object(body.path(), &format!("s3://raw/{path}.bin"), checksum)
            .await
            .expect_err("the store refuses the header")
            .to_string();
        // What happened: the store's own answer, and what the write carried.
        assert!(err.contains("InvalidRequest"), "{path}: {err}");
        assert!(err.contains("x-amz-checksum-sha256"), "{path}: {err}");
        // What the engine did.
        assert!(err.contains("nothing was committed"), "{path}: {err}");
        // What to do.
        assert!(
            err.contains("AWS_REQUEST_CHECKSUM_CALCULATION=when_required"),
            "{path}: {err}"
        );
        assert!(err.contains("read-back"), "{path}: {err}");
    }
}

#[tokio::test]
async fn a_locked_write_the_store_refuses_does_not_suggest_the_variable() {
    let _env = ENV.lock().await;
    let store = Store::new(Refuses::ChecksumHeader);
    let locked = backend(store.start().await, Some(lock())).await;
    let bytes = payload();
    let body = file(&bytes);
    let err = locked
        .put_object(body.path(), "s3://locked/none.bin", None)
        .await
        .expect_err("the store refuses the header")
        .to_string();
    // The profile pins the calculation on, so the variable cannot help here,
    // and the message says so rather than sending the operator to it.
    assert!(err.contains("x-amz-checksum-sha256"), "{err}");
    assert!(err.contains("Object Lock"), "{err}");
    assert!(err.contains("nothing was committed"), "{err}");
    assert!(
        !err.contains("AWS_REQUEST_CHECKSUM_CALCULATION=when_required"),
        "{err}"
    );
}

#[tokio::test]
async fn a_bad_digest_is_not_blamed_on_the_header() {
    let _env = ENV.lock().await;
    let store = Store::new(Refuses::BadDigest);
    let plain = backend(store.start().await, None).await;
    let bytes = payload();
    let body = file(&bytes);
    let b3 = vtop_core::checksum::blake3_bytes(&bytes);
    let err = plain
        .put_object(
            body.path(),
            "s3://raw/b3.bin",
            Some(ObjectChecksum::new("blake3", &b3)),
        )
        .await
        .expect_err("the store refuses the body")
        .to_string();
    assert!(err.contains("BadDigest"), "{err}");
    assert!(!err.contains("AWS_REQUEST_CHECKSUM_CALCULATION"), "{err}");
}
