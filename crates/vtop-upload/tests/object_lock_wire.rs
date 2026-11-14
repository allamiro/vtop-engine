//! #684: what the S3 backend puts on the wire for the Object Lock profile,
//! and how it reads the store's answers.
//!
//! The real SDK against a local socket that records every request and answers
//! as a store would. It pins:
//!
//! - every object PUT, manifest PUT and CreateMultipartUpload carries the
//!   lock mode, the retain-until date the backend reports it asked for, and
//!   the legal hold when one is configured; with no profile, none of them
//!   carries a lock header;
//! - a locked PUT carries the integrity header S3 requires for a write under
//!   retention (`Content-MD5` or `x-amz-sdk-checksum-algorithm`), for the
//!   SHA-256 and the BLAKE3 path alike;
//! - the retention check reads the EXACT version it is given, and a store
//!   that returns no lock headers (one that ignored them) fails it;
//! - the bucket preflight tells "Object Lock is not enabled" (S3's
//!   `ObjectLockConfigurationNotFoundError`) from a check that could not be
//!   made.
//!
//! Its own test binary because it sets credentials in the process
//! environment.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vtop_upload::s3_native::{S3NativeBackend, S3NativeConfig};
use vtop_upload::{
    ObjectChecksum, ObjectLockMode, ObjectLockPolicy, RequestedRetention, RetentionCheck,
    RetentionExpectation, UploadBackend,
};

/// One request as the store saw it.
#[derive(Debug, Clone)]
struct Seen {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    fn lock_headers(&self) -> Vec<&(String, String)> {
        self.headers
            .iter()
            .filter(|(k, _)| k.starts_with("x-amz-object-lock"))
            .collect()
    }
}

/// How the stub answers a HEAD and a GetObjectLockConfiguration.
#[derive(Clone, Default)]
struct Answers {
    head_headers: Vec<(String, String)>,
    lock_config: Option<(&'static str, &'static str)>,
}

#[derive(Clone, Default)]
struct Store {
    seen: Arc<Mutex<Vec<Seen>>>,
    answers: Arc<Mutex<Answers>>,
}

impl Store {
    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
    fn clear(&self) {
        self.seen.lock().unwrap().clear();
    }
    fn answer_head(&self, headers: &[(&str, &str)]) {
        self.answers.lock().unwrap().head_headers = headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
    }
    fn answer_lock_config(&self, status: &'static str, body: &'static str) {
        self.answers.lock().unwrap().lock_config = Some((status, body));
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
            target: target.clone(),
            headers,
        };
        // Drain the body: by length, or to the last chunk when chunked.
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

        let answers = self.answers.lock().unwrap().clone();
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
                     <Bucket>telemetry-locked</Bucket><Key>k</Key><UploadId>up-1</UploadId>\
                     </InitiateMultipartUploadResult>"
                    .into(),
            )
        } else if method == "HEAD" {
            let mut h = vec![
                ("content-length".into(), "5".into()),
                ("etag".into(), "\"e\"".into()),
            ];
            h.extend(answers.head_headers.clone());
            ("200 OK", h, String::new())
        } else if method == "GET" && target.contains("object-lock") {
            let (status, body) = answers
                .lock_config
                .unwrap_or(("500 Internal Server Error", ""));
            (
                status,
                vec![("content-type".into(), "application/xml".into())],
                body.into(),
            )
        } else {
            ("500 Internal Server Error", vec![], String::new())
        };
        let mut response = format!("HTTP/1.1 {status}\r\nconnection: close\r\n");
        for (k, v) in &extra {
            if method == "HEAD" && k == "content-length" {
                response.push_str(&format!("{k}: {v}\r\n"));
            } else if k != "content-length" {
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

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// The retain-until header as Unix seconds.
fn header_secs(value: &str) -> i64 {
    aws_smithy_types::DateTime::from_str(value, aws_smithy_types::date_time::Format::DateTime)
        .unwrap_or_else(|e| panic!("{value:?} is not an ISO 8601 date: {e}"))
        .secs()
}

fn file(bytes: &[u8]) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(bytes).unwrap();
    f.flush().unwrap();
    f
}

const DAY: u64 = 86_400;

/// Every lock header on one request agrees with what the backend reported
/// it asked for.
fn assert_locked(request: &Seen, asked: Option<RequestedRetention>, legal_hold: bool, what: &str) {
    let asked =
        asked.unwrap_or_else(|| panic!("{what}: the backend reports the lock it asked for"));
    assert_eq!(
        request.header("x-amz-object-lock-mode"),
        Some("GOVERNANCE"),
        "{what}: {request:?}"
    );
    let until = request
        .header("x-amz-object-lock-retain-until-date")
        .unwrap_or_else(|| panic!("{what}: no retain-until date: {request:?}"));
    assert_eq!(
        header_secs(until),
        asked.retain_until_unix_secs,
        "{what}: the date sent is the date reported"
    );
    assert_eq!(asked.mode, ObjectLockMode::Governance, "{what}");
    assert_eq!(
        request.header("x-amz-object-lock-legal-hold"),
        legal_hold.then_some("ON"),
        "{what}: {request:?}"
    );
}

#[tokio::test]
async fn every_write_carries_the_lock_it_reports_and_none_without_the_profile() {
    let store = Store::default();
    let endpoint = store.start().await;
    let policy = ObjectLockPolicy::with_retention_for_tests(
        ObjectLockMode::Governance,
        Duration::from_secs(30 * DAY),
        true,
    );
    let locked = backend(endpoint.clone(), Some(policy)).await;
    let body = file(b"hello");
    let sha = vtop_core::checksum::sha256_bytes(b"hello");
    let ck = ObjectChecksum::new("sha256", &sha);

    let before = unix_now();
    let object = locked
        .put_object(body.path(), "s3://telemetry-locked/a/obj.raw", Some(ck))
        .await
        .expect("the stub accepts the put");
    let manifest = locked
        .put_manifest(
            body.path(),
            "s3://telemetry-locked/a/obj.manifest.json",
            Some(ck),
        )
        .await
        .expect("the stub accepts the manifest");
    let created = locked
        .create_multipart_upload(
            "s3://telemetry-locked/a/big.raw",
            "application/octet-stream",
            Some(ck),
        )
        .await
        .expect("the stub starts the upload");
    let after = unix_now();
    assert_eq!(created.upload_id, "up-1");

    let seen = store.seen();
    assert_eq!(seen.len(), 3, "{seen:#?}");
    assert_locked(&seen[0], object.retention, true, "object PUT");
    assert_locked(&seen[1], manifest.retention, true, "manifest PUT");
    assert_locked(&seen[2], created.retention, true, "CreateMultipartUpload");
    for r in [object.retention, manifest.retention, created.retention] {
        let until = r.unwrap().retain_until_unix_secs;
        assert!(
            until >= before + (30 * DAY) as i64 && until <= after + (30 * DAY) as i64 + 1,
            "now plus thirty days, rounded up to a whole second: {until} for [{before}, {after}]"
        );
    }

    // Without the profile: no lock header anywhere, and nothing reported.
    store.clear();
    let plain = backend(endpoint, None).await;
    let object = plain
        .put_object(body.path(), "s3://telemetry-locked/a/plain.raw", Some(ck))
        .await
        .unwrap();
    let created = plain
        .create_multipart_upload(
            "s3://telemetry-locked/a/plain-big.raw",
            "application/octet-stream",
            None,
        )
        .await
        .unwrap();
    assert!(object.retention.is_none() && created.retention.is_none());
    for request in store.seen() {
        assert!(
            request.lock_headers().is_empty(),
            "no lock header without the profile: {request:?}"
        );
    }
}

/// S3: "The Content-MD5 or x-amz-sdk-checksum-algorithm header is required
/// for any request to upload an object with a retention period configured
/// using Amazon S3 Object Lock." The SHA-256 path sends its own
/// `x-amz-checksum-sha256`; the BLAKE3 path sends no checksum of its own.
#[tokio::test]
async fn a_locked_put_carries_the_integrity_header_s3_requires_for_either_checksum() {
    let store = Store::default();
    let endpoint = store.start().await;
    let policy = ObjectLockPolicy::with_retention_for_tests(
        ObjectLockMode::Compliance,
        Duration::from_secs(DAY),
        false,
    );
    let locked = backend(endpoint, Some(policy)).await;
    let body = file(b"hello");
    let sha = vtop_core::checksum::sha256_bytes(b"hello");
    let b3 = vtop_core::checksum::blake3_bytes(b"hello");
    locked
        .put_object(
            body.path(),
            "s3://telemetry-locked/sha.raw",
            Some(ObjectChecksum::new("sha256", &sha)),
        )
        .await
        .unwrap();
    locked
        .put_object(
            body.path(),
            "s3://telemetry-locked/b3.raw",
            Some(ObjectChecksum::new("blake3", &b3)),
        )
        .await
        .unwrap();
    locked
        .put_object(body.path(), "s3://telemetry-locked/none.raw", None)
        .await
        .unwrap();
    for request in store.seen() {
        assert_eq!(request.header("x-amz-object-lock-mode"), Some("COMPLIANCE"));
        assert!(
            request.header("content-md5").is_some()
                || request.header("x-amz-sdk-checksum-algorithm").is_some(),
            "a write under retention needs Content-MD5 or x-amz-sdk-checksum-algorithm: {request:#?}"
        );
    }
}

#[tokio::test]
async fn the_retention_check_reads_the_exact_version_and_fails_a_store_that_kept_no_lock() {
    let store = Store::default();
    let endpoint = store.start().await;
    let backend = backend(endpoint, None).await;
    let uri = "s3://telemetry-locked/a/obj.raw";
    let until = unix_now() + 3_600;
    let until_header = vtop_upload::format_unix_secs(until);
    let expected = RetentionExpectation {
        mode: ObjectLockMode::Governance,
        retain_until_at_least_unix_secs: until,
        legal_hold: false,
    };

    store.answer_head(&[
        ("x-amz-object-lock-mode", "GOVERNANCE"),
        ("x-amz-object-lock-retain-until-date", &until_header),
    ]);
    let check = backend
        .verify_retention(uri, Some("ver-1"), &expected)
        .await
        .expect("the stub answers the HEAD");
    assert_eq!(
        check,
        RetentionCheck::Ok {
            retain_until_unix_secs: until
        }
    );
    let head = store.seen().pop().unwrap();
    assert_eq!(head.method, "HEAD");
    assert!(
        head.target.contains("versionId=ver-1"),
        "the HEAD names the version it was given: {}",
        head.target
    );

    // A store that ignored the lock headers answers with none of them.
    store.answer_head(&[]);
    assert_eq!(
        backend
            .verify_retention(uri, Some("ver-1"), &expected)
            .await
            .unwrap(),
        RetentionCheck::Missing
    );
    // One second short.
    let short = vtop_upload::format_unix_secs(until - 1);
    store.answer_head(&[
        ("x-amz-object-lock-mode", "GOVERNANCE"),
        ("x-amz-object-lock-retain-until-date", &short),
    ]);
    assert_eq!(
        backend
            .verify_retention(uri, Some("ver-1"), &expected)
            .await
            .unwrap(),
        RetentionCheck::TooShort {
            retain_until_unix_secs: until - 1
        }
    );
    // Another mode, and a missing legal hold.
    store.answer_head(&[
        ("x-amz-object-lock-mode", "COMPLIANCE"),
        ("x-amz-object-lock-retain-until-date", &until_header),
    ]);
    assert!(matches!(
        backend
            .verify_retention(uri, Some("ver-1"), &expected)
            .await
            .unwrap(),
        RetentionCheck::ModeMismatch { .. }
    ));
    store.answer_head(&[
        ("x-amz-object-lock-mode", "GOVERNANCE"),
        ("x-amz-object-lock-retain-until-date", &until_header),
        ("x-amz-object-lock-legal-hold", "OFF"),
    ]);
    let with_hold = RetentionExpectation {
        legal_hold: true,
        ..expected
    };
    assert_eq!(
        backend
            .verify_retention(uri, Some("ver-1"), &with_hold)
            .await
            .unwrap(),
        RetentionCheck::LegalHoldMissing
    );
    // No version: the current one, and the HEAD names none.
    store.clear();
    store.answer_head(&[
        ("x-amz-object-lock-mode", "GOVERNANCE"),
        ("x-amz-object-lock-retain-until-date", &until_header),
    ]);
    assert!(backend
        .verify_retention(uri, None, &expected)
        .await
        .unwrap()
        .passed());
    assert!(!store.seen().pop().unwrap().target.contains("versionId"));
}

#[tokio::test]
async fn the_bucket_preflight_tells_not_enabled_from_could_not_check() {
    let store = Store::default();
    let endpoint = store.start().await;
    let backend = backend(endpoint, None).await;

    store.answer_lock_config(
        "200 OK",
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ObjectLockConfiguration>\
         <ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>",
    );
    assert!(backend
        .bucket_object_lock_enabled("telemetry-locked")
        .await
        .unwrap());
    let request = store.seen().pop().unwrap();
    assert!(request.target.contains("object-lock"), "{request:?}");

    store.answer_lock_config(
        "404 Not Found",
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>\
         <Code>ObjectLockConfigurationNotFoundError</Code>\
         <Message>Object Lock configuration does not exist for this bucket</Message></Error>",
    );
    assert!(
        !backend
            .bucket_object_lock_enabled("telemetry-plain")
            .await
            .unwrap(),
        "S3's answer for a bucket without Object Lock is a definite no"
    );

    store.answer_lock_config(
        "403 Forbidden",
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>AccessDenied</Code>\
         <Message>Access Denied</Message></Error>",
    );
    let err = backend
        .bucket_object_lock_enabled("telemetry-locked")
        .await
        .expect_err("a refused check proves nothing either way");
    assert!(err.to_string().contains("403"), "{err}");
}
