//! #668 against a real S3-compatible store: a failed multipart part does not
//! discard the parts that completed.
//!
//! An in-test TCP forwarder sits between the native S3 backend and the store.
//! It reads each request's head, counts the `UploadPart` requests by part
//! number, and closes the connection on the requests for part 2 it is told to
//! refuse, so the store never receives them. The counts are what the store
//! was asked, independent of anything the code under test reports.
//!
//! Run against the compose MinIO lab with:
//! `VTOP_TEST_S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin
//!  AWS_SECRET_ACCESS_KEY=minioadmin cargo test -p vtop-upload
//!  --test live_s3_multipart_resume -- --ignored --test-threads=1`
//! `VTOP_TEST_S3_BUCKET` may select an existing bucket (defaults to
//! `telemetry-raw`, created if missing). Each test aborts or deletes what it
//! creates.
//!
//! CI runs it in the `minio-integration` job, against the lab's MinIO.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use vtop_upload::multipart::{load_session, save_session, session_file_name};
use vtop_upload::s3_native::{S3NativeBackend, S3NativeConfig};
use vtop_upload::{
    cleanup_abandoned, upload_resumable, upload_resumable_with_report, MultipartFence,
    MultipartReport, MultipartUploadConfig, ObjectChecksum, UploadBackend,
};

const MIB: usize = 1024 * 1024;
/// S3's smallest non-final part.
const PART: usize = 5 * MIB;

/// Counts `UploadPart` requests by part number, and refuses the next
/// `refuse` requests for part 2 by closing the connection.
#[derive(Clone, Default)]
struct Forwarder {
    parts: Arc<Mutex<HashMap<u32, usize>>>,
    refuse: Arc<AtomicUsize>,
}

impl Forwarder {
    fn sent(&self, part: u32) -> usize {
        self.parts.lock().unwrap().get(&part).copied().unwrap_or(0)
    }

    /// Spend one refusal if any is left. A compare-exchange loop because
    /// `fetch_update` is deprecated on current stable and its replacement is
    /// newer than the MSRV.
    fn take_refusal(&self) -> bool {
        let mut left = self.refuse.load(Ordering::SeqCst);
        loop {
            if left == 0 {
                return false;
            }
            match self.refuse.compare_exchange_weak(
                left,
                left - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(observed) => left = observed,
            }
        }
    }

    fn reset_counts(&self) {
        self.parts.lock().unwrap().clear();
    }

    /// Listen on a local port and forward every connection to `upstream`.
    async fn start(&self, upstream: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let this = self.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let this = this.clone();
                let upstream = upstream.clone();
                tokio::spawn(async move { this.serve(client, &upstream).await });
            }
        });
        endpoint
    }

    async fn serve(&self, client: TcpStream, upstream: &str) {
        let Ok(server) = TcpStream::connect(upstream).await else {
            return;
        };
        let (mut client_rd, mut client_wr) = client.into_split();
        let (mut server_rd, mut server_wr) = server.into_split();
        // Responses go back untouched.
        let back = tokio::spawn(async move {
            let _ = tokio::io::copy(&mut server_rd, &mut client_wr).await;
        });
        let mut buf: Vec<u8> = Vec::new();
        'requests: loop {
            // One request head.
            let head_end = loop {
                if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break end + 4;
                }
                let mut chunk = [0u8; 16 * 1024];
                match client_rd.read(&mut chunk).await {
                    Ok(0) | Err(_) => break 'requests,
                    Ok(n) => buf.extend_from_slice(&chunk[..n]),
                }
            };
            let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
            let request_line = head.lines().next().unwrap_or_default().to_owned();
            if let Some(part) = part_number(&request_line) {
                *self.parts.lock().unwrap().entry(part).or_default() += 1;
                let refuse = part == 2 && self.take_refusal();
                if refuse {
                    // Close before the store sees a byte of this request:
                    // the client finds its connection reset mid-request.
                    break 'requests;
                }
            }
            let length = head
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.trim()
                        .eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            if server_wr.write_all(&buf[..head_end]).await.is_err() {
                break;
            }
            buf.drain(..head_end);
            let mut left = length;
            let have = buf.len().min(left);
            if server_wr.write_all(&buf[..have]).await.is_err() {
                break;
            }
            buf.drain(..have);
            left -= have;
            while left > 0 {
                let mut chunk = vec![0u8; left.min(256 * 1024)];
                match client_rd.read(&mut chunk).await {
                    Ok(0) | Err(_) => break 'requests,
                    Ok(n) => {
                        if server_wr.write_all(&chunk[..n]).await.is_err() {
                            break 'requests;
                        }
                        left -= n;
                    }
                }
            }
        }
        back.abort();
    }
}

fn part_number(request_line: &str) -> Option<u32> {
    let query = request_line.split_whitespace().nth(1)?.split_once('?')?.1;
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix("partNumber="))
        .and_then(|n| n.parse().ok())
}

fn upstream() -> (String, String) {
    let endpoint = std::env::var("VTOP_TEST_S3_ENDPOINT")
        .expect("VTOP_TEST_S3_ENDPOINT must name the live endpoint");
    let host = endpoint
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_owned();
    (endpoint, host)
}

fn bucket() -> String {
    std::env::var("VTOP_TEST_S3_BUCKET").unwrap_or_else(|_| "telemetry-raw".into())
}

async fn backend(endpoint: &str) -> S3NativeBackend {
    S3NativeBackend::new(&S3NativeConfig {
        region: "us-east-1".into(),
        endpoint_url: Some(endpoint.to_owned()),
        force_path_style: true,
        verify_tls: false,
        transport: "tcp_tls".into(),
        tuning: Default::default(),
        max_egress_bytes_per_second: None,
        request_timeout_seconds: 300,
        request_min_bytes_per_second: 4 * 1024,
        object_lock: None,
    })
    .await
    .unwrap()
}

/// A client straight to the store, for what the test checks behind the
/// backend's back: the uploads the store still holds.
async fn s3_client(endpoint: &str) -> aws_sdk_s3::Client {
    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .load()
        .await;
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(true)
            .build(),
    )
}

async fn pending_uploads(client: &aws_sdk_s3::Client, key: &str) -> usize {
    client
        .list_multipart_uploads()
        .bucket(bucket())
        .prefix(key)
        .send()
        .await
        .unwrap()
        .uploads()
        .len()
}

fn object(dir: &Path, seed: u8) -> (std::path::PathBuf, Vec<u8>, String) {
    let data: Vec<u8> = (0..2 * PART + MIB)
        .map(|i| seed ^ (i % 251) as u8 ^ (i / PART) as u8)
        .collect();
    let path = dir.join(format!("object-{seed}.bin"));
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(&data).unwrap();
    f.flush().unwrap();
    let digest = vtop_core::checksum::sha256_bytes(&data);
    (path, data, digest)
}

fn fence(data: &[u8], digest: &str) -> MultipartFence {
    MultipartFence {
        expected_segment_generation: 0,
        fencing_epoch: 0,
        content_digest_hex: digest.to_owned(),
        content_digest_algorithm: "sha256".into(),
        byte_length: data.len() as u64,
    }
}

fn config(state: &Path, attempts: u32) -> MultipartUploadConfig {
    MultipartUploadConfig {
        part_size_bytes: PART as u64,
        threshold_bytes: PART as u64,
        max_parallelism: 3,
        abandon_after_secs: 86_400,
        state_dir: state.to_path_buf(),
        part_attempts: attempts,
        part_backoff: Default::default(),
    }
}

fn unique_key(name: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("vtop-live-668/{}-{nanos}-{name}.bin", std::process::id())
}

async fn verifies(backend: &S3NativeBackend, uri: &str, data: &[u8], digest: &str) -> bool {
    let v = backend
        .verify_object(
            uri,
            data.len() as u64,
            Some(ObjectChecksum::new("sha256", digest)),
        )
        .await
        .unwrap();
    v.passed && !v.backend_limited
}

/// The connection is closed on the first three requests for part 2: the SDK's
/// own three attempts at that request all fail, so it is the upload's
/// per-part retry (#668) that sends part 2 a fourth time. Before #668 the
/// upload failed there. Only part 2 is sent again, and the object completes
/// and reads back.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint and credentials"]
async fn live_a_part_whose_connection_is_reset_is_sent_again_alone() {
    let (endpoint, host) = upstream();
    let forwarder = Forwarder::default();
    let via = forwarder.start(host).await;
    let backend = backend(&via).await;
    backend.ensure_bucket(&bucket()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (path, data, digest) = object(dir.path(), 1);
    let uri = format!("s3://{}/{}", bucket(), unique_key("reset-once"));

    forwarder.refuse.store(3, Ordering::SeqCst);
    let mut report = MultipartReport::default();
    upload_resumable_with_report(
        &backend,
        &config(dir.path(), 3),
        &path,
        &uri,
        Some(ObjectChecksum::new("sha256", &digest)),
        fence(&data, &digest),
        &mut report,
    )
    .await
    .expect("a part whose request failed every SDK attempt is retried, not the end of the upload");

    assert_eq!(
        [forwarder.sent(1), forwarder.sent(2), forwarder.sent(3)],
        [1, 4, 1],
        "only part 2's request was repeated: {report:?}"
    );
    assert_eq!(
        report.part_retries, 1,
        "the fourth request was the upload's own retry of the part: {report:?}"
    );
    assert!(verifies(&backend, &uri, &data, &digest).await);
    backend.delete_object(&uri).await.unwrap();
    let _ = endpoint;
}

/// Part 2 fails on every request: the upload fails and its session keeps
/// parts 1 and 3. A second backend instance, the way a restarted process
/// builds one, resumes that session, sends part 2 alone, and the completed
/// object reads back.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint and credentials"]
async fn live_a_second_backend_resumes_the_session_the_first_left() {
    let (endpoint, host) = upstream();
    let forwarder = Forwarder::default();
    let via = forwarder.start(host).await;
    let first = backend(&via).await;
    first.ensure_bucket(&bucket()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (path, data, digest) = object(dir.path(), 2);
    let key = unique_key("resume");
    let uri = format!("s3://{}/{key}", bucket());
    let cfg = config(dir.path(), 1);

    forwarder.refuse.store(usize::MAX, Ordering::SeqCst);
    upload_resumable(
        &first,
        &cfg,
        &path,
        &uri,
        Some(ObjectChecksum::new("sha256", &digest)),
        fence(&data, &digest),
    )
    .await
    .expect_err("part 2 is refused on every request");
    let session = load_session(
        &dir.path()
            .join(session_file_name(&uri, &fence(&data, &digest))),
    )
    .expect("the failed upload's session is kept");
    assert_eq!(
        session.parts.keys().copied().collect::<Vec<_>>(),
        vec![1, 3]
    );
    let client = s3_client(&endpoint).await;
    assert_eq!(
        pending_uploads(&client, &key).await,
        1,
        "the store holds the upload"
    );
    drop(first);

    forwarder.refuse.store(0, Ordering::SeqCst);
    forwarder.reset_counts();
    let second = backend(&via).await;
    upload_resumable(
        &second,
        &cfg,
        &path,
        &uri,
        Some(ObjectChecksum::new("sha256", &digest)),
        fence(&data, &digest),
    )
    .await
    .expect("the second backend resumes the session");
    assert_eq!(
        [forwarder.sent(1), forwarder.sent(2), forwarder.sent(3)],
        [0, 1, 0],
        "the resume sent part 2 and nothing else"
    );
    assert!(verifies(&second, &uri, &data, &digest).await);
    assert_eq!(pending_uploads(&client, &key).await, 0);
    second.delete_object(&uri).await.unwrap();
}

/// A session older than the abandon window is swept: its upload is aborted
/// at the store, so its parts are no longer kept there.
#[tokio::test]
#[ignore = "requires an S3-compatible endpoint and credentials"]
async fn live_the_sweep_aborts_an_abandoned_upload_at_the_store() {
    let (endpoint, host) = upstream();
    let forwarder = Forwarder::default();
    let via = forwarder.start(host).await;
    let backend = backend(&via).await;
    backend.ensure_bucket(&bucket()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (path, data, digest) = object(dir.path(), 3);
    let key = unique_key("swept");
    let uri = format!("s3://{}/{key}", bucket());
    let mut cfg = config(dir.path(), 1);

    forwarder.refuse.store(usize::MAX, Ordering::SeqCst);
    upload_resumable(
        &backend,
        &cfg,
        &path,
        &uri,
        Some(ObjectChecksum::new("sha256", &digest)),
        fence(&data, &digest),
    )
    .await
    .expect_err("part 2 is refused");
    let client = s3_client(&endpoint).await;
    assert_eq!(pending_uploads(&client, &key).await, 1);

    let session_path = dir
        .path()
        .join(session_file_name(&uri, &fence(&data, &digest)));
    let mut session = load_session(&session_path).unwrap();
    session.updated_at_unix_secs = 1;
    save_session(&session_path, &session).unwrap();
    cfg.abandon_after_secs = 60;
    assert_eq!(cleanup_abandoned(&backend, &cfg).await.unwrap(), 1);
    assert!(!session_path.exists());
    assert_eq!(
        pending_uploads(&client, &key).await,
        0,
        "ListMultipartUploads no longer shows the abandoned upload"
    );
}
