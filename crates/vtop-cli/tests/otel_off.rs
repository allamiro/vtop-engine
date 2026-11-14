//! #683: with no OTLP endpoint set, OpenTelemetry stays idle: no exporter,
//! no export thread or task, and the engine's spans are never created.
//!
//! Its own test binary, with this one test: it installs the process's global
//! subscriber the way `vtopctl` does, which can happen once per process.

use vtop_core::config::StreamsConfig;
use vtop_core::types::SourceType;
use vtop_observe::otel;

/// Threads named like the span queue's export thread (`vtop-otel-export`,
/// which the kernel truncates to 15 bytes).
#[cfg(target_os = "linux")]
fn export_threads() -> usize {
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|task| std::fs::read_to_string(task.unwrap().path().join("comm")).ok())
        .filter(|name| name.starts_with("vtop-otel-expor") || name.starts_with("OpenTelemetry"))
        .count()
}

#[test]
fn without_an_endpoint_nothing_is_built_and_nothing_runs() {
    // Set before any other thread exists: the environment is process-wide.
    for var in [
        otel::ENDPOINT_ENV,
        otel::TRACES_ENDPOINT_ENV,
        otel::METRICS_ENDPOINT_ENV,
    ] {
        std::env::remove_var(var);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        vtop_cli::commands::init_tracing("info", false, Some("vtop-test")).unwrap();
        assert!(!otel::running(), "export is running with no endpoint set");
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
        assert!(!otel::running());
        assert_eq!(
            otel::stats::stats().queue_length.get(),
            0,
            "a span was queued with export off"
        );
        #[cfg(target_os = "linux")]
        assert_eq!(
            export_threads(),
            0,
            "an export thread is running with export off"
        );

        // Shutdown with nothing running returns at once.
        let started = std::time::Instant::now();
        vtop_cli::otel::shutdown().await;
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
    });
}
