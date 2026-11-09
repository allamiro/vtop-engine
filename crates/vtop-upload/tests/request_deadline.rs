//! A store that accepts a connection and never answers must fail the request,
//! not hang the engine (#556).
//!
//! The AWS SDK bounds connecting but, by default, not waiting for a response,
//! so an endpoint that goes silent after the TCP handshake blocked the awaiting
//! upload forever: the process alive, the health endpoint green, no error in
//! any log. This is the real SDK against a real socket that behaves exactly
//! that way, with `upload.request_timeout_seconds` at its minimum so the test
//! is quick.
//!
//! Its own integration-test binary on purpose: it must set credentials in the
//! process environment for the SDK's default chain, and no other test shares
//! this process.

use std::io::Write;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vtop_upload::s3_native::{request_timeouts, S3NativeBackend, S3NativeConfig};
use vtop_upload::UploadBackend;

/// Accept connections and read whatever arrives, never writing a byte back and
/// never closing: a blackholed or wedged store.
async fn silent_store() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            // Keep the socket open for the life of the task.
            held.push(socket);
        }
    });
    (endpoint, task)
}

#[tokio::test]
async fn a_store_that_accepts_and_never_answers_fails_the_request_instead_of_hanging() {
    // Static credentials, so the SDK's default chain never goes looking for
    // others (it would otherwise fail on credentials, which is not the thing
    // under test), and no instance-metadata probe.
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");

    let (endpoint, _store) = silent_store().await;
    let backend = S3NativeBackend::new(&S3NativeConfig {
        region: "us-east-1".into(),
        endpoint_url: Some(endpoint),
        force_path_style: true,
        verify_tls: false,
        transport: "tcp_tls".into(),
        tuning: Default::default(),
        request_timeout_seconds: 1,
    })
    .await
    .expect("the backend builds against a plaintext lab endpoint");

    let mut body = tempfile::NamedTempFile::new().unwrap();
    body.write_all(b"telemetry").unwrap();
    body.flush().unwrap();

    let started = Instant::now();
    // The outer bound exists only so that a regression fails this test instead
    // of hanging the suite; a working deadline returns long before it.
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        backend.put_object(body.path(), "s3://telemetry-raw/some/key.bin", None),
    )
    .await;
    let result = outcome.expect(
        "the upload was still waiting after 60 s against a store that never answers: the \
         client has no response deadline",
    );
    assert!(
        result.is_err(),
        "a store that never answers cannot have accepted the object: {result:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "with a 1 s deadline and the SDK's retries the request must give up in seconds, took {:?}",
        started.elapsed()
    );
}

/// The knob maps onto every deadline the SDK offers, so that none of the ways
/// a request can stall is left open.
#[test]
fn the_deadline_covers_connect_read_attempt_and_the_whole_operation() {
    let config = request_timeouts(20);
    assert_eq!(config.connect_timeout(), Some(Duration::from_secs(10)));
    assert_eq!(config.read_timeout(), Some(Duration::from_secs(20)));
    assert_eq!(
        config.operation_attempt_timeout(),
        Some(Duration::from_secs(20))
    );
    assert_eq!(
        config.operation_timeout(),
        Some(Duration::from_secs(60)),
        "the whole operation is allowed the SDK's three attempts"
    );
}

/// Answer every request with headers promptly and then a body that drips one
/// byte every half second and never finishes: past the SDK's stalled-stream
/// protection (which catches a stream that STOPS), and past every SDK
/// timeout, which end when the headers arrive.
async fn dripping_store() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut request = [0u8; 4096];
                let _ = socket.read(&mut request).await;
                let headers = "HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\
                               Content-Type: application/octet-stream\r\n\r\n";
                if socket.write_all(headers.as_bytes()).await.is_err() {
                    return;
                }
                loop {
                    if socket.write_all(b"x").await.is_err() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            });
        }
    });
    (endpoint, task)
}

/// Review of #568: a store that answers the headers and then drips the body
/// held every body read - verification, manifest recovery - open long past
/// the configured deadline. The read now fails within the whole-operation
/// allowance (three attempts of 1 s here).
#[tokio::test]
async fn a_body_that_drips_after_the_headers_fails_the_read_instead_of_hanging() {
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");

    let (endpoint, _store) = dripping_store().await;
    let backend = S3NativeBackend::new(&S3NativeConfig {
        region: "us-east-1".into(),
        endpoint_url: Some(endpoint),
        force_path_style: true,
        verify_tls: false,
        transport: "tcp_tls".into(),
        tuning: Default::default(),
        request_timeout_seconds: 1,
    })
    .await
    .expect("the backend builds against a plaintext lab endpoint");

    let started = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        backend.get_object("s3://telemetry-raw/some/key.bin"),
    )
    .await
    .expect("the body read was still going after 60 s: nothing bounds it");
    let error = outcome.expect_err("a body that never finishes cannot have been read");
    assert!(
        error.to_string().contains("did not finish within"),
        "the failure must name the body deadline: {error}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "a 3 s body allowance gave up after {:?}",
        started.elapsed()
    );
}

/// Headers that arrive late, then a body that drips: the store spends most
/// of an attempt before answering at all.
async fn slow_then_dripping_store(header_delay: Duration) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut request = [0u8; 4096];
                let _ = socket.read(&mut request).await;
                tokio::time::sleep(header_delay).await;
                let headers = "HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\
                               Content-Type: application/octet-stream\r\n\r\n";
                if socket.write_all(headers.as_bytes()).await.is_err() {
                    return;
                }
                loop {
                    if socket.write_all(b"x").await.is_err() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            });
        }
    });
    (endpoint, task)
}

/// Review of #568, second round: the body's deadline used to start when the
/// headers arrived, so headers that took most of an attempt bought the body
/// a fresh full allowance - nearly twice the operation's limit in all. The
/// deadline now counts from before the request: with 2 s attempts (a 6 s
/// allowance) and headers at 1.8 s, the read must fail at about 6 s, not 7.8.
#[tokio::test]
async fn slow_headers_come_out_of_the_bodys_allowance() {
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");

    let (endpoint, _store) = slow_then_dripping_store(Duration::from_millis(1_800)).await;
    let backend = S3NativeBackend::new(&S3NativeConfig {
        region: "us-east-1".into(),
        endpoint_url: Some(endpoint),
        force_path_style: true,
        verify_tls: false,
        transport: "tcp_tls".into(),
        tuning: Default::default(),
        request_timeout_seconds: 2,
    })
    .await
    .expect("the backend builds against a plaintext lab endpoint");

    let started = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        backend.get_object("s3://telemetry-raw/some/key.bin"),
    )
    .await
    .expect("the body read was still going after 60 s: nothing bounds it");
    let error = outcome.expect_err("a body that never finishes cannot have been read");
    assert!(
        error.to_string().contains("did not finish within"),
        "the failure must name the deadline: {error}"
    );
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(5_500) && took < Duration::from_millis(7_000),
        "a 6 s operation allowance, headers at 1.8 s: the read ended after {took:?}"
    );
}
