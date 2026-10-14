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
