//! The engine's binding of `vtop_observe::otel` (#683).
//!
//! The export itself (settings, the span queue, the OTLP exporters, the
//! metrics bridge and the messages) lives in `vtop-observe`, shared with
//! `vtop-node`. What is specific to the engine: its service name, its
//! instance id (the engine's name), its metric registry, and the spans of
//! the batch lifecycle (`crate::spans`), which the tests below pin down.

use vtop_core::errors::VtopError;
use vtop_core::telemetry;
use vtop_observe::otel::{self, Process};

/// The engine's default `service.name`.
pub const SERVICE_NAME: &str = "vtop-engine";

/// Install logging and, when the environment asks for it, OpenTelemetry
/// export. A setting that cannot be honoured is a startup error naming the
/// variable.
pub fn init_tracing(level: &str, json: bool, instance: Option<&str>) -> Result<(), VtopError> {
    otel::init(
        level,
        json,
        Process {
            service: SERVICE_NAME,
            instance,
            traces: true,
        },
    )
    .map_err(|e| VtopError::Config(format!("OpenTelemetry: {e}")))?;
    if otel::metrics_running() {
        // The metrics export reads the engine's registry, which otherwise
        // exists only when `/metrics` is served.
        match telemetry::init() {
            Ok(m) => {
                crate::audit::bind_counters(m);
                if let Err(e) = otel::register_registry(&m.registry) {
                    tracing::warn!(error = %e, "OpenTelemetry counters could not be registered");
                }
            }
            Err(e) => tracing::error!(
                error = %e,
                "metrics registry failed to initialize; OpenTelemetry metrics will be empty"
            ),
        }
    }
    Ok(())
}

/// Flush and stop export before the process exits (bounded; see
/// [`otel::SHUTDOWN_FLUSH_DEADLINE`]). A no-op when export is off.
pub async fn shutdown() {
    otel::shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::testkit::file_config;
    use opentelemetry::trace::{SpanId, Status};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Layer;
    use vtop_core::config::StreamsConfig;
    use vtop_core::types::SourceType;
    use vtop_observe::otel::{ExportQueue, QueueSettings, Sampler, Settings};

    /// What a span exported for one batch must never carry: the records.
    const RECORD: &str = "do-not-export-me";

    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| vars.get(name).cloned()
    }

    /// The binary's layer over an in-memory exporter, as this thread's
    /// subscriber. Thread-local, so tests running beside this one neither
    /// see these spans nor add to them.
    fn capture() -> (
        SdkTracerProvider,
        InMemorySpanExporter,
        tracing::subscriber::DefaultGuard,
    ) {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(otel::layer(&provider));
        let guard = tracing::subscriber::set_default(subscriber);
        (provider, exporter, guard)
    }

    fn finished(provider: &SdkTracerProvider, exporter: &InMemorySpanExporter) -> Vec<SpanData> {
        provider.force_flush().unwrap();
        let mut spans = exporter.get_finished_spans().unwrap();
        spans.sort_by_key(|s| s.start_time);
        spans
    }

    fn attr(span: &SpanData, key: &str) -> Option<String> {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.as_str().into_owned())
    }

    fn named<'a>(spans: &'a [SpanData], name: &str) -> Vec<&'a SpanData> {
        spans.iter().filter(|s| s.name == name).collect()
    }

    /// No attribute of any span holds record content or a URL.
    fn assert_nothing_leaked(spans: &[SpanData]) {
        for span in spans {
            for kv in &span.attributes {
                let value = kv.value.as_str();
                assert!(
                    !value.contains(RECORD) && !value.contains("://"),
                    "span {} attribute {}={value} carries record content or a URL",
                    span.name,
                    kv.key
                );
            }
        }
    }

    fn input(dir: &std::path::Path, lines: usize) -> String {
        let path = dir.join("app.log");
        let body: String = (0..lines).map(|i| format!("{RECORD} {i}\n")).collect();
        std::fs::write(&path, body).unwrap();
        path.to_string_lossy().into_owned()
    }

    async fn engine(dir: &std::path::Path, backend: &str) -> Engine {
        let cfg = file_config(
            dir.join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input(dir, 3)],
            backend,
        );
        Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap()
    }

    /// What a collector does with each request.
    #[derive(Clone, Copy)]
    enum Collector {
        /// Answers 200 at once and records the request.
        Answers,
        /// Accepts the connection and never answers.
        Hangs,
    }

    /// A local collector; returns its base URL and the (request line, body)
    /// of every request it answered.
    async fn collector(
        mode: Collector,
    ) -> (
        String,
        tokio::sync::mpsc::UnboundedReceiver<(String, Vec<u8>)>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    loop {
                        let mut chunk = [0u8; 8192];
                        let head_end = loop {
                            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                break i + 4;
                            }
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
                            match socket.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            }
                        }
                        if let Collector::Hangs = mode {
                            std::future::pending::<()>().await;
                        }
                        let line = head.lines().next().unwrap_or_default().to_owned();
                        let _ = tx.send((line, buf[head_end..head_end + length].to_vec()));
                        buf.drain(..head_end + length);
                        let reply = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
                        if socket.write_all(reply).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (base, rx)
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    /// Run `batches` batches (one file each) through an engine whose spans go
    /// to `provider`, and return how long the batch path took.
    async fn run_batches(provider: &SdkTracerProvider, batches: usize) -> Duration {
        let dir = tempfile::tempdir().unwrap();
        let paths: Vec<String> = (0..batches)
            .map(|i| {
                let path = dir.path().join(format!("f{i}.log"));
                std::fs::write(&path, format!("{RECORD} {i}\n")).unwrap();
                path.to_string_lossy().into_owned()
            })
            .collect();
        let cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            "sqlite::memory:",
            paths,
            "mock",
        );
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let subscriber = tracing_subscriber::registry().with(otel::layer(provider));
        let _guard = tracing::subscriber::set_default(subscriber);
        let started = std::time::Instant::now();
        let outcomes = engine.process_once(SourceType::File).await.unwrap();
        let took = started.elapsed();
        assert_eq!(outcomes.len(), batches);
        assert!(
            outcomes.iter().all(|o| o.committed),
            "a batch did not commit while traces were being exported"
        );
        took
    }

    #[test]
    fn only_the_engines_spans_are_exported() {
        let (provider, exporter, guard) = capture();
        tracing::debug_span!(target: "vtop_cli::engine", "ours").in_scope(|| {});
        tracing::info_span!(target: "aws_smithy_runtime", "theirs", uri = "https://x/?sig=1")
            .in_scope(|| {});
        tracing::info!(target: "vtop_cli::engine", "an event is not a span");
        drop(guard);
        let names: Vec<_> = finished(&provider, &exporter)
            .into_iter()
            .map(|s| s.name.into_owned())
            .collect();
        assert_eq!(names, ["ours"]);
    }

    /// #683: one batch is one trace: a root `batch` span carrying the batch's
    /// identity, sizes, checksum and verification, and its outcome, with a
    /// child per lifecycle step in order.
    #[tokio::test]
    async fn one_batch_is_a_batch_span_with_a_child_per_step() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(dir.path(), "mock").await;
        let (provider, exporter, guard) = capture();
        let outcomes = engine.process_once(SourceType::File).await.unwrap();
        drop(guard);
        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].committed);
        let spans = finished(&provider, &exporter);

        let batches = named(&spans, "batch");
        assert_eq!(
            batches.len(),
            1,
            "spans: {:?}",
            spans.iter().map(|s| &s.name).collect::<Vec<_>>()
        );
        let batch = batches[0];
        assert_eq!(
            batch.parent_span_id,
            SpanId::INVALID,
            "a batch is a root span"
        );
        assert_eq!(batch.status, Status::Ok);
        let expect = [
            ("batch_id", outcomes[0].batch_id.as_str()),
            ("source_type", "file"),
            ("tenant", "default"),
            ("records", "3"),
            ("checksum_algorithm", "sha256"),
            ("verification_method", "read_back"),
            ("verification_result", "strong"),
            ("result", "committed"),
        ];
        for (key, value) in expect {
            assert_eq!(
                attr(batch, key).as_deref(),
                Some(value),
                "batch attribute {key}"
            );
        }
        for key in ["source_name", "format", "bytes", "compressed_bytes"] {
            assert!(
                attr(batch, key).is_some_and(|v| !v.is_empty()),
                "batch attribute {key}"
            );
        }
        assert_eq!(attr(batch, "failed_stage"), None);

        let children: Vec<_> = spans
            .iter()
            .filter(|s| s.parent_span_id == batch.span_context.span_id())
            .collect();
        assert_eq!(
            children.iter().map(|s| s.name.as_ref()).collect::<Vec<_>>(),
            [
                "seal",
                "compress",
                "checksum",
                "upload_object",
                "upload_manifest",
                "verify",
                "commit"
            ],
            "a child per step, in the order the batch takes them"
        );
        for child in &children {
            assert_eq!(
                child.span_context.trace_id(),
                batch.span_context.trace_id(),
                "{} is in the batch's trace",
                child.name
            );
            assert_ne!(child.status, Status::error(""), "{} failed", child.name);
        }
        assert_eq!(
            attr(named(&spans, "upload_object")[0], "multipart").as_deref(),
            Some("false")
        );
        assert_eq!(spans.len(), 8, "nothing but the batch and its steps");
        assert_nothing_leaked(&spans);
    }

    /// A batch that fails says where: the step's span and the batch's carry
    /// an error status, and the batch names the step. The error message is
    /// not recorded (a backend error can quote a URL).
    #[tokio::test]
    async fn a_failed_batch_names_the_step_it_failed_in() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = engine(dir.path(), "mock_fail").await;
        let (provider, exporter, guard) = capture();
        let outcomes = engine.process_once(SourceType::File).await;
        drop(guard);
        assert!(outcomes.is_ok_and(|o| !o.iter().any(|o| o.committed)));
        let spans = finished(&provider, &exporter);

        let batch = named(&spans, "batch")[0];
        assert!(matches!(batch.status, Status::Error { .. }));
        assert_eq!(attr(batch, "result").as_deref(), Some("failed"));
        assert_eq!(attr(batch, "failed_stage").as_deref(), Some("verify"));
        let verify = named(&spans, "verify")[0];
        assert!(matches!(verify.status, Status::Error { .. }));
        assert!(
            named(&spans, "commit").is_empty(),
            "a failed batch never commits"
        );
        assert_nothing_leaked(&spans);
    }

    /// Recovery is a trace with a span per row it decides on, and scrub a
    /// span per batch it checks, each carrying the row's identity and the
    /// outcome.
    #[tokio::test]
    async fn recovery_and_scrub_record_a_span_per_row() {
        let dir = tempfile::tempdir().unwrap();
        let mut failing = engine(dir.path(), "mock_fail").await;
        assert!(!failing.process_once(SourceType::File).await.unwrap()[0].committed);
        let (provider, exporter, guard) = capture();
        let summary = failing.recover().await.unwrap();
        drop(guard);
        let spans = finished(&provider, &exporter);
        let recovery = named(&spans, "recovery");
        assert_eq!(recovery.len(), 1);
        assert_eq!(recovery[0].parent_span_id, SpanId::INVALID);
        assert_eq!(attr(recovery[0], "rows").as_deref(), Some("1"));
        assert_eq!(
            attr(recovery[0], "replay_required"),
            Some(summary.replay_required.to_string())
        );
        let rows = named(&spans, "recovery_row");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].parent_span_id, recovery[0].span_context.span_id());
        assert_eq!(attr(rows[0], "state").as_deref(), Some("replay_required"));
        assert_eq!(attr(rows[0], "action").as_deref(), Some("ReplayFromSource"));
        assert_eq!(
            attr(rows[0], "decision").as_deref(),
            Some("replay_required")
        );
        assert_nothing_leaked(&spans);

        let other = tempfile::tempdir().unwrap();
        let mut engine = engine(other.path(), "mock").await;
        let batch_id = engine.process_once(SourceType::File).await.unwrap()[0]
            .batch_id
            .clone();
        let rows =
            crate::scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
        let (provider, exporter, guard) = capture();
        let report = engine
            .scrub(rows, &crate::scrub::ScrubOptions::default())
            .await
            .unwrap();
        drop(guard);
        assert!(report.passed);
        let spans = finished(&provider, &exporter);
        let checks = named(&spans, "scrub_check");
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].parent_span_id, SpanId::INVALID);
        assert_eq!(attr(checks[0], "batch_id"), Some(batch_id));
        assert_eq!(attr(checks[0], "origin").as_deref(), Some("ledger"));
        assert_eq!(attr(checks[0], "result").as_deref(), Some("ok"));
        assert_eq!(checks[0].status, Status::Ok);
        assert_nothing_leaked(&spans);
    }

    /// A batch whose trace is dropped before it reaches an outcome (an error
    /// returned through `?`, or a flush abandoned mid-batch) is exported as
    /// `incomplete` with an error status, the open step included.
    #[test]
    fn a_batch_dropped_mid_step_is_incomplete_and_an_error() {
        let (provider, exporter, guard) = capture();
        {
            let mut trace = crate::spans::BatchTrace::start("file", "app", 1);
            trace.begin(crate::spans::Stage::UploadObject);
        }
        drop(guard);
        let spans = finished(&provider, &exporter);
        let batch = named(&spans, "batch")[0];
        assert_eq!(attr(batch, "result").as_deref(), Some("incomplete"));
        assert_eq!(
            attr(batch, "failed_stage").as_deref(),
            Some("upload_object")
        );
        assert!(matches!(batch.status, Status::Error { .. }));
        assert!(matches!(
            named(&spans, "upload_object")[0].status,
            Status::Error { .. }
        ));
    }

    /// #683: a failed batch's span carries the error, with its secrets taken
    /// out: a presigned URL is cut to its host and a MAC to `<hex>`.
    #[test]
    fn a_failed_batch_carries_its_error_redacted() {
        let (provider, exporter, guard) = capture();
        {
            let mut trace = crate::spans::BatchTrace::start("file", "app", 1);
            trace.begin(crate::spans::Stage::Verify);
            trace.fail_with(
                "failed",
                "object verification failed: GET https://u:pw@bucket.s3.example/o?X-Amz-Signature=abc \
                 mac 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
            );
        }
        drop(guard);
        let spans = finished(&provider, &exporter);
        for span in [named(&spans, "batch")[0], named(&spans, "verify")[0]] {
            let Status::Error { description } = &span.status else {
                panic!("{} is not an error: {:?}", span.name, span.status);
            };
            assert_eq!(
                description, "object verification failed: GET https://bucket.s3.example mac <hex>",
                "{}",
                span.name
            );
        }
    }

    /// Both OTLP endpoint variables, end to end through the real exporter:
    /// the base URL posts to `/v1/traces`, the traces URL to exactly its own
    /// path, each as protobuf carrying the batch and never its records.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn both_endpoint_variables_reach_the_collector() {
        let (base, mut requests) = collector(Collector::Answers).await;
        let traces_url = format!("{base}/custom/traces");
        for (vars, path) in [
            (
                vec![
                    (otel::ENDPOINT_ENV, base.as_str()),
                    ("OTEL_METRICS_EXPORTER", "none"),
                ],
                "/v1/traces",
            ),
            (
                vec![(otel::TRACES_ENDPOINT_ENV, traces_url.as_str())],
                "/custom/traces",
            ),
        ] {
            let provider = provider_for(&vars);
            run_batches(&provider, 1).await;
            otel::flush_unless(provider, std::future::pending()).await;
            let (line, body) = tokio::time::timeout(Duration::from_secs(2), requests.recv())
                .await
                .expect("no export reached the collector")
                .unwrap();
            assert_eq!(line, format!("POST {path} HTTP/1.1"), "{vars:?}");
            assert!(contains(&body, b"upload_manifest"), "{vars:?}");
            assert!(contains(&body, SERVICE_NAME.as_bytes()), "{vars:?}");
            assert!(contains(&body, b"service.instance.id"), "{vars:?}");
            assert!(
                !contains(&body, RECORD.as_bytes()),
                "record content exported"
            );
        }
    }

    fn provider_for(vars: &[(&str, &str)]) -> SdkTracerProvider {
        let Ok(Settings::On(export)) = otel::settings_from(env(vars), SERVICE_NAME) else {
            panic!("export is not on for {vars:?}");
        };
        otel::build_tracer_provider_for(
            &export,
            Process {
                service: SERVICE_NAME,
                instance: Some("vtop-test"),
                traces: true,
            },
        )
        .unwrap()
    }

    /// An endpoint nobody listens on: every export fails, the engine keeps
    /// archiving at its usual pace, and shutdown does not wait on it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreachable_collector_never_stops_the_batch_path() {
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", closed.local_addr().unwrap());
        drop(closed);
        let provider = provider_for(&[
            (otel::ENDPOINT_ENV, url.as_str()),
            ("OTEL_EXPORTER_OTLP_TIMEOUT", "2000"),
        ]);
        let took = run_batches(&provider, 5).await;
        assert!(took < Duration::from_secs(5), "5 batches took {took:?}");
        let started = std::time::Instant::now();
        otel::flush_unless(provider, std::future::pending()).await;
        assert!(
            started.elapsed() < otel::SHUTDOWN_FLUSH_DEADLINE + Duration::from_secs(2),
            "shutdown waited {:?} on a collector that refuses connections",
            started.elapsed()
        );
    }

    /// A collector that accepts and never answers (the slowest there is):
    /// batches are not held up, and the flush gives up at its deadline even
    /// though each request's own timeout is longer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_hung_collector_never_stops_the_batch_path_or_the_exit() {
        let (base, _requests) = collector(Collector::Hangs).await;
        let provider = provider_for(&[
            (otel::ENDPOINT_ENV, base.as_str()),
            ("OTEL_EXPORTER_OTLP_TIMEOUT", "60000"),
        ]);
        let took = run_batches(&provider, 5).await;
        assert!(took < Duration::from_secs(5), "5 batches took {took:?}");
        let started = std::time::Instant::now();
        otel::flush_unless(provider, std::future::pending()).await;
        let waited = started.elapsed();
        assert!(
            waited < otel::SHUTDOWN_FLUSH_DEADLINE + Duration::from_secs(2),
            "shutdown waited {waited:?}; the deadline is {:?}",
            otel::SHUTDOWN_FLUSH_DEADLINE
        );
    }

    /// An exporter held shut until the test opens it, counting what it is
    /// finally given.
    #[derive(Debug, Clone, Default)]
    struct Gate {
        open: Arc<(Mutex<bool>, std::sync::Condvar)>,
        exported: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Gate {
        fn release(&self) {
            *self.open.0.lock().unwrap() = true;
            self.open.1.notify_all();
        }
    }

    impl opentelemetry_sdk::trace::SpanExporter for Gate {
        async fn export(&self, batch: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
            let (lock, cvar) = &*self.open;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = cvar.wait(open).unwrap();
            }
            self.exported
                .fetch_add(batch.len(), std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// A buffer a test's log output goes to.
    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Logs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Logs {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    /// A collector slower than the engine: spans past the queue's capacity are
    /// dropped and counted, never held or waited for, with one warning that
    /// names the cure. This is the queue the binary installs, with
    /// `OTEL_BSP_MAX_QUEUE_SIZE` / `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` small so
    /// the bound is visible.
    #[test]
    fn a_slow_collector_costs_spans_never_memory_or_time() {
        const QUEUE: usize = 16;
        const BATCH: usize = 4;
        const SPANS: usize = 10_000;
        let gate = Gate::default();
        let queue = ExportQueue::new(
            gate.clone(),
            QueueSettings {
                max_queue: QUEUE,
                max_batch: BATCH,
                schedule_delay: Duration::from_secs(5),
            },
        );
        let probe = queue.probe();
        let provider = otel::tracer_provider(queue, Sampler::ParentBasedAlwaysOn, &[]);
        let dropped_before = otel::stats::stats()
            .spans_dropped
            .with_label_values(&["queue_full"])
            .get();
        let logs = Logs::default();
        let started = std::time::Instant::now();
        {
            let writer = logs.clone();
            let subscriber = tracing_subscriber::registry()
                .with(otel::layer(&provider))
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(move || writer.clone())
                        .with_ansi(false)
                        .with_filter(tracing::level_filters::LevelFilter::INFO),
                );
            let _guard = tracing::subscriber::set_default(subscriber);
            for _ in 0..SPANS {
                tracing::debug_span!(target: "vtop_cli::engine", "batch").in_scope(|| {});
                assert!(probe.pending() <= QUEUE, "the queue grew past its capacity");
            }
        }
        let took = started.elapsed();
        gate.release();
        provider.shutdown().unwrap();
        let exported = gate.exported.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            exported > 0,
            "nothing was exported once the collector caught up"
        );
        assert!(
            exported <= QUEUE,
            "{exported} of {SPANS} spans were kept for a collector that was not answering; \
             the queue holds {QUEUE}"
        );
        let dropped = otel::stats::stats()
            .spans_dropped
            .with_label_values(&["queue_full"])
            .get()
            - dropped_before;
        assert!(
            dropped >= (SPANS - QUEUE) as u64,
            "only {dropped} drops were counted"
        );
        assert!(
            took < Duration::from_secs(5),
            "{SPANS} spans took {took:?} to create against a stalled collector"
        );
        let text = logs.text();
        assert_eq!(
            text.matches("OpenTelemetry export queue full").count(),
            1,
            "the warning is rate-limited to one: {text}"
        );
        assert!(text.contains("batches are unaffected"), "{text}");
        assert!(text.contains("OTEL_BSP_MAX_QUEUE_SIZE"), "{text}");
    }

    /// An exporter that never answers: the collector is down and the request
    /// hangs.
    #[derive(Debug)]
    struct Stuck;

    impl opentelemetry_sdk::trace::SpanExporter for Stuck {
        async fn export(&self, _batch: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
            std::future::pending().await
        }
    }

    fn stuck_provider_with_a_span() -> SdkTracerProvider {
        let queue = ExportQueue::new(
            Stuck,
            QueueSettings {
                max_queue: 16,
                max_batch: 1,
                schedule_delay: Duration::from_millis(10),
            },
        );
        let provider = otel::tracer_provider(queue, Sampler::AlwaysOn, &[]);
        let subscriber = tracing_subscriber::registry().with(otel::layer(&provider));
        let _guard = tracing::subscriber::set_default(subscriber);
        tracing::debug_span!(target: "vtop_cli::engine", "queued").in_scope(|| {});
        tracing::debug_span!(target: "vtop_cli::engine", "queued").in_scope(|| {});
        provider
    }

    /// #683: a stop never waits on the collector for longer than the flush
    /// deadline, however the export hangs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_gives_up_on_a_hung_collector_at_its_deadline() {
        let provider = stuck_provider_with_a_span();
        let started = std::time::Instant::now();
        otel::flush_unless(provider, std::future::pending()).await;
        let waited = started.elapsed();
        assert!(
            waited < otel::SHUTDOWN_FLUSH_DEADLINE + Duration::from_secs(2),
            "shutdown waited {waited:?} on a collector that never answers; the deadline is {:?}",
            otel::SHUTDOWN_FLUSH_DEADLINE
        );
    }

    /// Review of #693: a stop signal during the flush ends the wait at once.
    /// After a second signal has abandoned the engine's own flush, the
    /// operator must not be held by a collector that never answers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stop_signal_cuts_the_flush_short() {
        let provider = stuck_provider_with_a_span();
        let started = std::time::Instant::now();
        otel::flush_unless(provider, async {}).await;
        let waited = started.elapsed();
        assert!(
            waited < Duration::from_secs(1),
            "a stop signal was pending and the flush still waited {waited:?}"
        );
    }
}
