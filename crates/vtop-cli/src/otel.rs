//! OTLP trace export for the batch lifecycle (#683). Off by default.
//!
//! Export is on only when `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or
//! `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Unset, nothing in this module runs
//! past reading those two variables: no exporter, no export thread, and the
//! subscriber is exactly the one `vtop_observe::logging::init` installs. The
//! spans themselves (`crate::spans`) sit at DEBUG, so the default log filter
//! turns them away at the callsite and they cost nothing either.
//!
//! The transport is OTLP over HTTP with protobuf bodies, the protocol the
//! OpenTelemetry spec makes the default. It needs only an HTTP client, and
//! this one is the hyper 1 + hyper-rustls stack the aws-sdk already puts in
//! the binary; the gRPC transport would have added tonic and its stack.
//! TLS uses rustls's `ring` provider, like every other client here, with the
//! system's root certificates (`SSL_CERT_FILE` and `SSL_CERT_DIR` name other
//! ones). There is no proxy support.
//!
//! The standard variables the exporter honours are listed in
//! observability/README.md. Two are refused rather than ignored: a protocol
//! other than `http/protobuf` (sending protobuf to a gRPC port would fail on
//! every export), and `OTEL_SDK_DISABLED=true`, which turns export off.
//!
//! A failure to set export up is logged and never fatal: telemetry must not
//! stop the engine archiving. The same goes for shutdown, which flushes
//! within [`SHUTDOWN_FLUSH_DEADLINE`] and then lets the process exit whatever
//! the collector is doing.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing::{Metadata, Subscriber};
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

/// The base endpoint; the exporter appends `/v1/traces`.
pub const ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
/// The traces endpoint, used as given. Wins over [`ENDPOINT_ENV`].
pub const TRACES_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
/// The `service.name` resource attribute.
pub const SERVICE_NAME_ENV: &str = "OTEL_SERVICE_NAME";
/// The service name when [`SERVICE_NAME_ENV`] is unset.
pub const DEFAULT_SERVICE_NAME: &str = "vtop-engine";
/// The only protocol accepted.
pub const PROTOCOL: &str = "http/protobuf";

/// The longest a stop waits for the last spans to reach the collector. Kept
/// well inside the shortest common termination grace (`docker stop`'s 10 s),
/// which the engine's own shutdown flush draws on first: losing the last few
/// spans to an unreachable collector costs a gap in a trace, and a process
/// that outlives its grace is killed mid-way through whatever it was doing.
pub const SHUTDOWN_FLUSH_DEADLINE: Duration = Duration::from_secs(5);

/// The per-request timeout when neither timeout variable is set (the spec's
/// default).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// What the environment asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Settings {
    /// No endpoint, or `OTEL_SDK_DISABLED=true`: export is off.
    Off,
    /// Export is asked for but cannot be done as asked; the reason is logged.
    Refused(String),
    /// Export to `endpoint` (the URL as configured, never logged whole).
    On {
        endpoint: String,
        service_name: String,
        timeout: Duration,
    },
}

/// Read the settings through `env`, which is `std::env::var` in the binary and
/// a map in the tests (environment variables are process-wide, so tests that
/// set them would race each other).
pub fn settings_from(env: impl Fn(&str) -> Option<String>) -> Settings {
    let set = |name: &str| {
        env(name)
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    };
    let Some(endpoint) = set(TRACES_ENDPOINT_ENV).or_else(|| set(ENDPOINT_ENV)) else {
        return Settings::Off;
    };
    if set("OTEL_SDK_DISABLED").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
        return Settings::Off;
    }
    for var in [
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
    ] {
        if let Some(protocol) = set(var) {
            if protocol != PROTOCOL {
                return Settings::Refused(format!(
                    "{var}={protocol} is not supported; this build exports {PROTOCOL} only"
                ));
            }
            break;
        }
    }
    if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
        return Settings::Refused(format!(
            "the OTLP endpoint must be an http:// or https:// URL (host: {})",
            endpoint_host(&endpoint)
        ));
    }
    let timeout = [
        "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
        "OTEL_EXPORTER_OTLP_TIMEOUT",
    ]
    .into_iter()
    .find_map(|var| set(var).and_then(|ms| ms.parse::<u64>().ok()))
    .map_or(DEFAULT_TIMEOUT, Duration::from_millis);
    Settings::On {
        endpoint,
        service_name: set(SERVICE_NAME_ENV).unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_owned()),
        timeout,
    }
}

/// The host and port of an endpoint URL, for a log line. Everything that can
/// hold a secret is dropped: the user-info before `@`, the path, the query
/// string and the fragment.
pub fn endpoint_host(url: &str) -> String {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host)
        .to_owned()
}

/// Which spans are exported: the engine's own, and nothing else. Library
/// spans are not ours to vouch for (an HTTP client's can carry a request URL
/// with its query string), and log events are not exported at all; the spans
/// carry only the attributes `crate::spans` chooses.
pub fn exported(meta: &Metadata<'_>) -> bool {
    meta.is_span() && meta.target().starts_with("vtop_")
}

/// The tracing layer that hands the engine's spans to `provider`. The binary
/// and the tests build it here, so the tests exercise the filter and the
/// attribute choices the binary ships.
pub fn layer<S>(provider: &SdkTracerProvider) -> impl Layer<S> + Send + Sync
where
    S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
{
    tracing_opentelemetry::layer()
        .with_tracer(provider.tracer("vtop-engine"))
        // Source location, thread and target attributes describe the build,
        // not the batch. Busy/idle time is meaningless for spans that are
        // never entered (see `crate::spans`), so it is not recorded either.
        .with_location(false)
        .with_threads(false)
        .with_target(false)
        .with_tracked_inactivity(false)
        .with_filter(filter_fn(exported))
}

/// The provider an installed exporter flushes through at shutdown. A
/// `Mutex<Option<_>>` so [`shutdown`] can take it exactly once.
static PROVIDER: Mutex<Option<SdkTracerProvider>> = Mutex::new(None);

/// What [`init_tracing`] did, for its caller to report once logging is up.
enum Installed {
    Off,
    Refused(String),
    On { host: String, service_name: String },
    Failed(String),
}

/// Install logging, with the trace exporter beside it when the environment
/// asks for one. `crate::commands::init_tracing` calls this.
pub fn init_tracing(level: &str, json: bool) {
    let installed = match settings_from(|name| std::env::var(name).ok()) {
        Settings::Off => Installed::Off,
        Settings::Refused(why) => Installed::Refused(why),
        Settings::On {
            endpoint,
            service_name,
            timeout,
        } => match build_provider(&service_name, timeout) {
            Ok(provider) => {
                if vtop_observe::logging::init_with_layer(level, json, layer(&provider)) {
                    *PROVIDER.lock().unwrap_or_else(|e| e.into_inner()) = Some(provider);
                    Installed::On {
                        host: endpoint_host(&endpoint),
                        service_name,
                    }
                } else {
                    // A subscriber was already in place, so the layer went
                    // unused: release the export thread now.
                    let _ = provider.shutdown_with_timeout(SHUTDOWN_FLUSH_DEADLINE);
                    return;
                }
            }
            Err(why) => Installed::Failed(why),
        },
    };
    match installed {
        Installed::On { host, service_name } => tracing::info!(
            endpoint_host = %host,
            service_name = %service_name,
            protocol = PROTOCOL,
            "OTLP trace export enabled"
        ),
        Installed::Off => vtop_observe::logging::init(level, json),
        Installed::Refused(why) => {
            vtop_observe::logging::init(level, json);
            tracing::warn!(reason = %why, "OTLP trace export not enabled");
        }
        Installed::Failed(why) => {
            vtop_observe::logging::init(level, json);
            tracing::error!(error = %why, "OTLP trace export could not be set up; continuing without it");
        }
    }
}

/// Build the exporter and the provider that batches spans into it. The
/// endpoint, headers and compression are read from the standard variables by
/// the exporter itself.
fn build_provider(service_name: &str, timeout: Duration) -> Result<SdkTracerProvider, String> {
    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|_| "no async runtime to send exports on".to_owned())?;
    let client = OnRuntime::new(timeout, runtime)?;
    let exporter = SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_http_client(client)
        .build()
        .map_err(|e| e.to_string())?;
    let resource = Resource::builder()
        .with_service_name(service_name.to_owned())
        .with_attribute(opentelemetry::KeyValue::new(
            "service.version",
            env!("CARGO_PKG_VERSION"),
        ))
        .build();
    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build())
}

/// Flush what is queued and stop the exporter, waiting at most
/// [`SHUTDOWN_FLUSH_DEADLINE`]. A no-op when export is off.
///
/// The SDK's shutdown blocks its caller until the export thread answers, so
/// it runs on the blocking pool: on an async worker it would stall the very
/// runtime the export's HTTP request is spawned on. The outer timeout is a
/// second bound in case the SDK's own is ever not honoured; if it fires, the
/// blocking task is left behind and the process exits without it.
pub async fn shutdown() {
    let Some(provider) = PROVIDER.lock().unwrap_or_else(|e| e.into_inner()).take() else {
        return;
    };
    let flush = tokio::task::spawn_blocking(move || {
        provider.shutdown_with_timeout(SHUTDOWN_FLUSH_DEADLINE)
    });
    match tokio::time::timeout(SHUTDOWN_FLUSH_DEADLINE + Duration::from_secs(1), flush).await {
        Ok(Ok(Ok(()))) => tracing::debug!("OTLP trace export flushed"),
        Ok(Ok(Err(e))) => tracing::warn!(error = %e, "OTLP trace export did not flush cleanly"),
        Ok(Err(e)) => tracing::warn!(error = %e, "OTLP trace export flush task failed"),
        Err(_) => tracing::warn!(
            deadline_secs = SHUTDOWN_FLUSH_DEADLINE.as_secs(),
            "OTLP trace export did not flush within its deadline; exiting without it"
        ),
    }
}

/// Whether an exporter is installed in this process.
pub fn installed() -> bool {
    PROVIDER.lock().unwrap_or_else(|e| e.into_inner()).is_some()
}

type Connector = hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>;

/// The hyper client, with each request spawned on the engine's runtime.
///
/// The SDK exports from its own thread, which has no Tokio runtime, and
/// hyper's client needs one to drive its connections. Spawning onto the
/// runtime captured at start-up and awaiting the join handle (which any
/// executor can await) keeps the request on Tokio and the export thread
/// blocked only on the result.
#[derive(Debug, Clone)]
struct OnRuntime {
    client: opentelemetry_http::hyper::HyperClient<Connector>,
    runtime: tokio::runtime::Handle,
}

impl OnRuntime {
    fn new(timeout: Duration, runtime: tokio::runtime::Handle) -> Result<Self, String> {
        use hyper_rustls::ConfigBuilderExt;
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?;
        // No system roots is not fatal: a plain http:// collector (the usual
        // in-cluster or sidecar shape) needs none, and an https:// one then
        // fails its export with a certificate error that says why.
        let tls = match builder.clone().with_native_roots() {
            Ok(builder) => builder.with_no_client_auth(),
            Err(_) => builder
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        };
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .build();
        Ok(Self {
            client: opentelemetry_http::hyper::HyperClient::new(connector, timeout, None),
            runtime,
        })
    }
}

#[async_trait::async_trait]
impl HttpClient for OnRuntime {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let client = self.client.clone();
        self.runtime
            .spawn(async move { client.send_bytes(request).await })
            .await
            .map_err(|e| Box::new(e) as HttpError)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::testkit::file_config;
    use opentelemetry::trace::{SpanId, Status};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SpanData};
    use std::collections::HashMap;
    use tracing_subscriber::layer::SubscriberExt;
    use vtop_core::config::StreamsConfig;
    use vtop_core::types::SourceType;

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
        let subscriber = tracing_subscriber::registry().with(layer(&provider));
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

    #[test]
    fn without_an_endpoint_export_is_off() {
        assert_eq!(settings_from(env(&[])), Settings::Off);
        assert_eq!(
            settings_from(env(&[(ENDPOINT_ENV, "  ")])),
            Settings::Off,
            "a blank endpoint is no endpoint"
        );
        assert_eq!(
            settings_from(env(&[
                (ENDPOINT_ENV, "http://collector:4318"),
                ("OTEL_SDK_DISABLED", "TRUE")
            ])),
            Settings::Off
        );
    }

    #[test]
    fn an_endpoint_turns_export_on_with_the_standard_names() {
        assert_eq!(
            settings_from(env(&[(ENDPOINT_ENV, "http://collector:4318")])),
            Settings::On {
                endpoint: "http://collector:4318".into(),
                service_name: DEFAULT_SERVICE_NAME.into(),
                timeout: DEFAULT_TIMEOUT,
            }
        );
        assert_eq!(
            settings_from(env(&[
                (ENDPOINT_ENV, "http://collector:4318"),
                (TRACES_ENDPOINT_ENV, "https://traces:4318/v1/traces"),
                (SERVICE_NAME_ENV, "archive-eu"),
                ("OTEL_EXPORTER_OTLP_TIMEOUT", "2500"),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf"),
            ])),
            Settings::On {
                endpoint: "https://traces:4318/v1/traces".into(),
                service_name: "archive-eu".into(),
                timeout: Duration::from_millis(2500),
            },
            "the traces endpoint wins over the base one"
        );
    }

    #[test]
    fn a_protocol_or_scheme_this_build_cannot_send_is_refused() {
        for (var, value) in [
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
            ("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "http/json"),
        ] {
            assert!(
                matches!(
                    settings_from(env(&[(ENDPOINT_ENV, "http://c:4317"), (var, value)])),
                    Settings::Refused(_)
                ),
                "{var}={value}"
            );
        }
        let refused = settings_from(env(&[(ENDPOINT_ENV, "collector:4318?token=s3cret")]));
        let Settings::Refused(why) = refused else {
            panic!("a URL with no scheme was accepted: {refused:?}");
        };
        assert!(
            !why.contains("s3cret"),
            "the reason quotes the query: {why}"
        );
    }

    #[test]
    fn the_logged_endpoint_drops_user_info_path_and_query() {
        assert_eq!(
            endpoint_host("https://user:pa55@otel.example:4318/v1/traces?sig=abc#x"),
            "otel.example:4318"
        );
        assert_eq!(endpoint_host("http://alloy:4318"), "alloy:4318");
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

    /// An exporter that never answers: the collector is down and the request
    /// hangs.
    #[derive(Debug)]
    struct Stuck;

    impl opentelemetry_sdk::trace::SpanExporter for Stuck {
        async fn export(&self, _batch: Vec<SpanData>) -> opentelemetry_sdk::error::OTelSdkResult {
            std::future::pending().await
        }
    }

    /// #683: a stop never waits on the collector for longer than the flush
    /// deadline, however the export hangs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_gives_up_on_a_hung_collector_at_its_deadline() {
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(Stuck)
            .build();
        {
            let subscriber = tracing_subscriber::registry().with(layer(&provider));
            let _guard = tracing::subscriber::set_default(subscriber);
            tracing::debug_span!(target: "vtop_cli::engine", "queued").in_scope(|| {});
        }
        *PROVIDER.lock().unwrap() = Some(provider);
        assert!(installed());
        let started = std::time::Instant::now();
        shutdown().await;
        let waited = started.elapsed();
        assert!(
            waited < SHUTDOWN_FLUSH_DEADLINE + Duration::from_secs(2),
            "shutdown waited {waited:?} on a collector that never answers; the deadline is \
             {SHUTDOWN_FLUSH_DEADLINE:?}"
        );
        assert!(!installed(), "shutdown releases the exporter");
        // A second stop has nothing left to flush and returns at once.
        let started = std::time::Instant::now();
        shutdown().await;
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
