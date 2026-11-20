//! A body that crosses a slow link must be waited for, not cut at a fixed
//! deadline and sent again from its first byte (#663).
//!
//! On a policed 10 Mbit/s uplink the engine's default width put 24 part
//! uploads on the link at once; each one's share was so small that an 8 MiB
//! part needed far longer than `upload.request_timeout_seconds`, so every
//! attempt of every part was cut at the deadline, re-sent, and cut again, and
//! the default configuration committed nothing. This is the real SDK against a
//! real socket that reads request bodies at a fixed, slow rate, with
//! `request_timeout_seconds` at its minimum so the test is quick: a body that
//! takes a few seconds to cross must be accepted, and one that crosses slower
//! than `upload.request_min_bytes_per_second` must still fail.
//!
//! The backend is built from an `UploadConfig` read from JSON, the way the
//! engine builds it, so this file compiles unchanged against a tree without
//! the setting (where the key is simply ignored) and shows the failure there.
//!
//! Its own integration-test binary on purpose: it sets credentials in the
//! process environment for the SDK's default chain.

use std::io::Write;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use vtop_upload::s3_native::{config_from_upload, S3NativeBackend};
use vtop_upload::UploadBackend;

/// The store's read rate: one 4 KiB read every 50 ms, at most 80 KiB/s.
const READ_CHUNK: usize = 4 * 1024;
const READ_PAUSE: Duration = Duration::from_millis(50);

/// The body each request carries: at the rate above, at least 2.4 s to cross,
/// so a fixed one-second deadline cuts every attempt of it.
const BODY_BYTES: usize = 192 * 1024;

/// Read one request's head; `None` when the client closed the connection.
async fn read_head(socket: &mut TcpStream, buf: &mut Vec<u8>) -> Option<String> {
    loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
            buf.drain(..end + 4);
            return Some(head);
        }
        let mut chunk = [0u8; 1024];
        match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        (key.trim() == name).then(|| value.trim())
    })
}

/// A store behind a slow link: it reads every request body at a fixed slow
/// rate, through a small receive buffer so the client is held to that rate
/// rather than handing the whole body to the kernel at once, and only then
/// answers 200 with an ETag - what both a PUT and an UploadPart expect.
async fn slow_reading_store() -> (String, tokio::task::JoinHandle<()>) {
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(8 * 1024).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(16).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = Vec::new();
                // Keep-alive: the SDK reuses the connection for its next
                // request, so serve requests until the client closes it.
                while let Some(head) = read_head(&mut socket, &mut buf).await {
                    let length: usize = header(&head, "content-length")
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| {
                            panic!("the test store reads a content-length body: {head}")
                        });
                    if header(&head, "expect") == Some("100-continue")
                        && socket
                            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
                            .await
                            .is_err()
                    {
                        return;
                    }
                    let mut read = buf.len().min(length);
                    buf.drain(..read);
                    while read < length {
                        let mut chunk = vec![0u8; READ_CHUNK.min(length - read)];
                        match socket.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => read += n,
                        }
                        tokio::time::sleep(READ_PAUSE).await;
                    }
                    let response = "HTTP/1.1 200 OK\r\nETag: \"663\"\r\nContent-Length: 0\r\n\r\n";
                    if socket.write_all(response.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (endpoint, task)
}

/// The backend the engine would build from this upload section, against the
/// slow store, with `request_timeout_seconds` at its minimum. `extra` adds
/// upload keys as JSON members.
async fn backend_for(endpoint: &str, extra: &str) -> S3NativeBackend {
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    let upload: vtop_core::config::UploadConfig = serde_json::from_str(&format!(
        r#"{{"backend":"s3_native","bucket":"telemetry-raw","endpoint_url":"{endpoint}",
             "region":"us-east-1","force_path_style":true,"verify_tls":false,
             "request_timeout_seconds":1{extra}}}"#
    ))
    .unwrap();
    S3NativeBackend::new(&config_from_upload(&upload))
        .await
        .expect("the backend builds against a plaintext lab endpoint")
}

fn body_file() -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(&vec![b'x'; BODY_BYTES]).unwrap();
    file.flush().unwrap();
    file
}

#[tokio::test]
async fn an_object_and_a_part_that_cross_a_slow_link_are_waited_for() {
    let (endpoint, _store) = slow_reading_store().await;
    // The default floor: a 192 KiB body is allowed 1 s plus 48 s per attempt.
    let backend = backend_for(&endpoint, "").await;
    let body = body_file();

    let started = Instant::now();
    let put = tokio::time::timeout(
        Duration::from_secs(60),
        backend.put_object(body.path(), "s3://telemetry-raw/slow/object.bin", None),
    )
    .await
    .expect("the upload was still going after 60 s");
    let took = started.elapsed();
    put.unwrap_or_else(|e| {
        panic!(
            "a body that crosses in about {:.1} s was cut by a fixed 1 s deadline instead of \
             being waited for (failed after {took:?}): {e}",
            BODY_BYTES as f64 / (READ_CHUNK as f64 / READ_PAUSE.as_secs_f64())
        )
    });
    assert!(
        took >= Duration::from_secs(2),
        "the store reads at most 80 KiB/s, so a 192 KiB body cannot have crossed in {took:?}"
    );

    let started = Instant::now();
    let part = tokio::time::timeout(
        Duration::from_secs(60),
        backend.upload_part(
            "s3://telemetry-raw/slow/parted.bin",
            "upload-663",
            1,
            bytes::Bytes::from(vec![b'y'; BODY_BYTES]),
        ),
    )
    .await
    .expect("the part upload was still going after 60 s");
    let took = started.elapsed();
    let part = part.unwrap_or_else(|e| {
        panic!("a part that crosses slowly was cut at the fixed deadline (after {took:?}): {e}")
    });
    assert_eq!(part.etag, "\"663\"");
}

#[tokio::test]
async fn a_body_slower_than_the_floor_still_fails() {
    // The deadline grows with the body; it does not go away. At a floor of
    // 1 MiB/s a 192 KiB body is allowed 1 + 1 s per attempt, and this store
    // needs at least 2.4 s, so every attempt is still cut.
    let (endpoint, _store) = slow_reading_store().await;
    let backend = backend_for(&endpoint, r#","request_min_bytes_per_second":1048576"#).await;
    let body = body_file();

    let started = Instant::now();
    let put = tokio::time::timeout(
        Duration::from_secs(60),
        backend.put_object(body.path(), "s3://telemetry-raw/slow/floor.bin", None),
    )
    .await
    .expect("the upload was still going after 60 s: nothing bounds a slow body");
    assert!(
        put.is_err(),
        "a body slower than the configured floor must not be waited for indefinitely"
    );
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "three attempts of 2 s each, plus backoff, gave up only after {:?}",
        started.elapsed()
    );
}
