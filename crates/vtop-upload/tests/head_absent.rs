//! #626: the S3 backend tells an absent key from a refused HEAD.
//!
//! The engine writes a batch under its derived object key only when the store
//! says the key holds nothing, so `head_object` must answer a 404, and only a
//! 404, with `VtopError::NotFound`. A 403 is what S3 answers for a missing key
//! when the caller may not list the bucket; it says nothing about the key, so
//! it must stay an error the engine does not read as "free".
//!
//! The real SDK against a socket that answers every request with one status.
//! Its own test binary because it sets credentials in the process
//! environment.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vtop_core::errors::VtopError;
use vtop_upload::s3_native::{S3NativeBackend, S3NativeConfig};
use vtop_upload::UploadBackend;

/// A store that answers every request with `status` and an empty body.
async fn store_answering(status: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let response =
                    format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
                let _ = socket.write_all(response.as_bytes()).await;
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
        max_egress_bytes_per_second: None,
    })
    .await
    .expect("the backend builds against a plaintext lab endpoint")
}

#[tokio::test]
async fn a_404_head_is_not_found_and_a_403_head_is_not() {
    let uri = "s3://telemetry-raw/x/vtop-src-b0-10-abc.raw";

    let absent = backend(store_answering("404 Not Found").await).await;
    assert!(absent.reports_absent_objects());
    match absent.head_object(uri).await {
        Err(VtopError::NotFound(at)) => assert_eq!(at, uri),
        other => panic!("a 404 HEAD is an absent key: {other:?}"),
    }

    let refused = backend(store_answering("403 Forbidden").await).await;
    match refused.head_object(uri).await {
        Err(VtopError::NotFound(_)) => {
            panic!("a 403 says nothing about the key and must not read as absent")
        }
        Err(e) => assert!(e.to_string().contains("403"), "{e}"),
        Ok(head) => panic!("a 403 is not an object: {head:?}"),
    }
}
