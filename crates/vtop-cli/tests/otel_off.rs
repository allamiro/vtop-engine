//! #683: with no OTLP endpoint set, trace export is off: no exporter is
//! installed, no export thread is started, and the engine's spans are never
//! created.
//!
//! Its own test binary, with this one test: it installs the process's global
//! subscriber the way `vtopctl` does, which can happen once per process.
#![cfg(feature = "otel")]

use vtop_cli::otel;
use vtop_core::config::StreamsConfig;
use vtop_core::types::SourceType;

/// Threads the SDK's batch processor runs on (`OpenTelemetry.Traces.BatchProcessor`,
/// truncated by the kernel to 15 bytes).
#[cfg(target_os = "linux")]
fn export_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|task| std::fs::read_to_string(task.unwrap().path().join("comm")).ok())
        .filter(|name| name.starts_with("OpenTelemetry"))
        .count()
}

#[test]
fn without_an_endpoint_no_exporter_or_export_thread_exists() {
    // Set before any other thread exists: the environment is process-wide.
    std::env::remove_var(otel::ENDPOINT_ENV);
    std::env::remove_var(otel::TRACES_ENDPOINT_ENV);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        vtop_cli::commands::init_tracing("info", false);
        assert!(
            !otel::installed(),
            "an exporter was installed with no endpoint set"
        );
        assert!(
            !tracing::enabled!(target: "vtop_cli::engine", tracing::Level::DEBUG),
            "the spans' level is enabled, so every batch would build spans for nothing"
        );

        // A batch runs, and still nothing starts.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        std::fs::write(&input, "one\ntwo\n").unwrap();
        let cfg = vtop_cli::testkit::file_config(
            dir.path().join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        let mut engine = vtop_cli::Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        assert!(engine.process_once(SourceType::File).await.unwrap()[0].committed);
        assert!(!otel::installed());
        #[cfg(target_os = "linux")]
        assert_eq!(
            export_threads(),
            0,
            "an export thread is running with export off"
        );

        // Shutdown with nothing installed returns at once.
        let started = std::time::Instant::now();
        otel::shutdown().await;
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    });
}
