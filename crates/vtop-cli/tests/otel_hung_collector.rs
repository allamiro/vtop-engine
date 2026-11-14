//! #683: a collector that accepts the connection and never answers, the
//! worst a collector can do. Batches still commit at the rate they commit
//! with export off, memory stays flat (the span queue is bounded, and what
//! does not fit is dropped and counted), and the exit is not held past the
//! flush deadline.
//!
//! Its own test binary, with this one test: it installs the process's global
//! subscriber and sets the exporter's environment, both process-wide.

use std::time::{Duration, Instant};

use tokio::io::AsyncReadExt;
use vtop_core::config::StreamsConfig;
use vtop_core::types::SourceType;
use vtop_observe::otel;

const BATCHES: usize = 20;
const SPANS: usize = 200_000;

/// Resident memory, from /proc (Linux only; elsewhere the check is skipped).
fn rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096)
}

/// Archive `BATCHES` one-line files and return how long it took.
async fn archive(dir: &std::path::Path) -> Duration {
    let paths: Vec<String> = (0..BATCHES)
        .map(|i| {
            let path = dir.join(format!("f{i}.log"));
            std::fs::write(&path, format!("line {i}\n")).unwrap();
            path.to_string_lossy().into_owned()
        })
        .collect();
    let cfg = vtop_cli::testkit::file_config(
        dir.join("work").to_str().unwrap(),
        "sqlite::memory:",
        paths,
        "mock",
    );
    let mut engine = vtop_cli::Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let started = Instant::now();
    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    let took = started.elapsed();
    assert_eq!(outcomes.len(), BATCHES);
    assert!(
        outcomes.iter().all(|o| o.committed),
        "a batch did not commit"
    );
    took
}

#[test]
fn a_collector_that_never_answers_costs_spans_not_batches_memory_or_the_exit() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    // Set before any other thread exists: the environment is process-wide.
    std::env::set_var(otel::ENDPOINT_ENV, &endpoint);
    std::env::set_var("OTEL_EXPORTER_OTLP_TIMEOUT", "60000");
    std::env::set_var("OTEL_METRICS_EXPORTER", "none");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        // Accept, read, never answer.
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut sink = [0u8; 8192];
                    while matches!(socket.read(&mut sink).await, Ok(n) if n > 0) {}
                    std::future::pending::<()>().await;
                });
            }
        });

        // The rate with export off: no subscriber is installed yet.
        let off = tempfile::tempdir().unwrap();
        let baseline = archive(off.path()).await;

        vtop_cli::commands::init_tracing("warn", false, Some("vtop-test")).unwrap();
        assert!(otel::running());
        let on = tempfile::tempdir().unwrap();
        let exporting = archive(on.path()).await;
        assert!(
            exporting < baseline * 3 + Duration::from_secs(1),
            "{BATCHES} batches took {exporting:?} exporting to a hung collector, \
             {baseline:?} with export off"
        );

        // Far more spans than the queue holds: memory stays flat, the queue
        // never passes its capacity, and the excess is counted as dropped.
        let stats = otel::stats::stats();
        let dropped = || stats.spans_dropped.with_label_values(&["queue_full"]).get();
        let dropped_before = dropped();
        let rss_before = rss_bytes();
        let started = Instant::now();
        for _ in 0..SPANS {
            tracing::debug_span!(target: "vtop_cli::engine", "batch", batch_id = "x")
                .in_scope(|| {});
            assert!(stats.queue_length.get() <= otel::settings::DEFAULT_MAX_QUEUE as i64);
        }
        let took = started.elapsed();
        assert!(
            took < Duration::from_secs(20),
            "{SPANS} spans took {took:?} against a hung collector"
        );
        assert!(
            dropped() - dropped_before >= (SPANS - otel::settings::DEFAULT_MAX_QUEUE) as u64,
            "only {} of {SPANS} spans were counted as dropped",
            dropped() - dropped_before
        );
        if let (Some(before), Some(after)) = (rss_before, rss_bytes()) {
            let grew = after.saturating_sub(before);
            assert!(
                grew < 64 << 20,
                "resident memory grew {} MiB over {SPANS} spans; the queue holds {}",
                grew >> 20,
                otel::settings::DEFAULT_MAX_QUEUE
            );
        }

        // The exit waits at most the flush deadline.
        let started = Instant::now();
        vtop_cli::otel::shutdown().await;
        let waited = started.elapsed();
        assert!(
            waited < otel::SHUTDOWN_FLUSH_DEADLINE + Duration::from_secs(2),
            "shutdown waited {waited:?} on a collector that never answers"
        );
        assert!(!otel::running());
    });
}
