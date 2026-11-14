//! #683: with an OTLP endpoint set, `vtopctl`'s tracing setup installs the
//! exporter, a batch's spans reach the collector over OTLP/HTTP as protobuf,
//! and shutdown flushes them.
//!
//! Its own test binary, with this one test: it installs the process's global
//! subscriber and sets the exporter's environment, both process-wide.
#![cfg(feature = "otel")]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use vtop_cli::otel;
use vtop_core::config::StreamsConfig;
use vtop_core::types::SourceType;

const RECORD: &str = "do-not-export-me";

#[cfg(target_os = "linux")]
fn export_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|task| std::fs::read_to_string(task.unwrap().path().join("comm")).ok())
        .filter(|name| name.starts_with("OpenTelemetry"))
        .count()
}

fn find(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// A collector that answers every request 200 and hands each one (head,
/// body) to the test.
async fn collector(listener: TcpListener, requests: mpsc::UnboundedSender<(String, Vec<u8>)>) {
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let requests = requests.clone();
        tokio::spawn(async move {
            let mut buf = Vec::new();
            loop {
                let head_end = loop {
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                    let mut chunk = [0u8; 4096];
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                };
                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                let length = head
                    .lines()
                    .find_map(|l| {
                        let (name, value) = l.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())?
                    })
                    .unwrap_or(0);
                while buf.len() < head_end + length {
                    let mut chunk = [0u8; 4096];
                    match socket.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                    }
                }
                let body = buf[head_end..head_end + length].to_vec();
                buf.drain(..head_end + length);
                let _ = requests.send((head, body));
                let reply = b"HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\ncontent-length: 0\r\n\r\n";
                if socket.write_all(reply).await.is_err() {
                    return;
                }
            }
        });
    }
}

#[test]
fn a_batch_reaches_the_collector_as_otlp_protobuf_and_shutdown_flushes_it() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    // Set before any other thread exists: the environment is process-wide.
    std::env::set_var(otel::ENDPOINT_ENV, &endpoint);
    std::env::set_var(otel::SERVICE_NAME_ENV, "vtop-it");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        listener.set_nonblocking(true).unwrap();
        let (tx, mut requests) = mpsc::unbounded_channel();
        tokio::spawn(collector(TcpListener::from_std(listener).unwrap(), tx));

        vtop_cli::commands::init_tracing("warn", false);
        assert!(
            otel::installed(),
            "no exporter was installed with {endpoint} set"
        );
        #[cfg(target_os = "linux")]
        assert_eq!(export_threads(), 1);

        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        std::fs::write(&input, format!("{RECORD} 1\n{RECORD} 2\n")).unwrap();
        let cfg = vtop_cli::testkit::file_config(
            dir.path().join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        let mut engine = vtop_cli::Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let outcome = engine
            .process_once(SourceType::File)
            .await
            .unwrap()
            .remove(0);
        assert!(outcome.committed);

        // Nothing is sent before the batch processor's 5 s delay; shutdown
        // sends it now.
        let started = std::time::Instant::now();
        otel::shutdown().await;
        assert!(started.elapsed() < otel::SHUTDOWN_FLUSH_DEADLINE);
        assert!(!otel::installed());
        #[cfg(target_os = "linux")]
        assert_eq!(
            export_threads(),
            0,
            "shutdown left the export thread running"
        );

        let mut body = Vec::new();
        while let Ok(Some((head, part))) =
            tokio::time::timeout(Duration::from_millis(500), requests.recv()).await
        {
            assert!(head.starts_with("POST /v1/traces HTTP/1.1\r\n"), "{head}");
            assert!(
                head.to_ascii_lowercase()
                    .contains("content-type: application/x-protobuf"),
                "{head}"
            );
            body.extend(part);
        }
        assert!(!body.is_empty(), "the collector received no export");
        // Protobuf carries strings as raw UTF-8, so the names and values are
        // visible in the bytes without decoding the message.
        for expected in [
            outcome.batch_id.as_bytes(),
            b"vtop-it",
            b"upload_manifest",
            b"verification_method",
        ] {
            assert!(
                find(&body, expected),
                "{} is not in the export",
                String::from_utf8_lossy(expected)
            );
        }
        assert!(
            !find(&body, RECORD.as_bytes()),
            "record content was exported"
        );
    });
}
