//! OpenTelemetry export: traces and metrics over OTLP/HTTP (#683).
//!
//! Compiled into every binary and idle until configured. With no OTLP
//! endpoint in the environment, [`init`] installs exactly the subscriber
//! [`crate::logging::init`] installs, logs one INFO line saying export is
//! disabled and which variable turns it on, and builds nothing else: no
//! exporter, no thread, no task. The engine's spans sit at DEBUG, so the
//! default log filter turns them away at the callsite and they cost nothing.
//!
//! With an endpoint set:
//!
//! * **Traces.** The engine's own spans (targets `vtop_*`; never a library's,
//!   never a log event) go through a bounded queue ([`ExportQueue`]) to the
//!   OTLP exporter. `OTEL_TRACES_SAMPLER` / `_ARG` choose the sampler
//!   (default `parentbased_always_on`).
//! * **Metrics.** The process's Prometheus registry is sent as OTLP every
//!   `OTEL_METRIC_EXPORT_INTERVAL` (see [`metrics`]); `/metrics` is
//!   unchanged. `OTEL_METRICS_EXPORTER=none` turns this off alone.
//! * **Log correlation.** JSON log lines written inside a span carry its
//!   `trace_id` and `span_id`.
//! * **Self-observability.** [`stats`] counts what was exported, dropped and
//!   failed, and the queue's length, on `/metrics` too.
//!
//! The transport is OTLP over HTTP with protobuf bodies. The gRPC exporter
//! brings tonic and its stack; this one needs only an HTTP client, and that
//! is the hyper + rustls client already in the binary ([`http`]).
//!
//! A setting that cannot be honoured is an error at startup naming the
//! variable and what it accepts. Everything after startup is never fatal and
//! never blocks the data path: a collector that is down or slow costs spans
//! and data points, counted and warned about at most once a minute.

mod http;
pub mod metrics;
mod queue;
mod redact;
pub mod settings;
pub mod stats;

use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::{Metadata, Subscriber};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

pub use queue::{ExportQueue, FlushReport, QueueProbe};
pub use redact::redact;
pub use settings::{
    endpoint_display, settings_from, ConfigError, Export, QueueSettings, Sampler, Settings, Signal,
    ENDPOINT_ENV, METRICS_ENDPOINT_ENV, PROTOCOL, SERVICE_NAME_ENV, TRACES_ENDPOINT_ENV,
};

/// The longest a stop waits for the last spans and data points to reach the
/// collector. Kept well inside the shortest common termination grace
/// (`docker stop`'s 10 s), which the process's own shutdown draws on first:
/// losing the last spans to an unreachable collector costs a gap in a trace,
/// and a process that outlives its grace is killed mid-way through its work.
pub const SHUTDOWN_FLUSH_DEADLINE: Duration = Duration::from_secs(5);

/// Who is exporting, for the resource attributes.
#[derive(Debug, Clone, Copy)]
pub struct Process<'a> {
    /// `service.name` unless `OTEL_SERVICE_NAME` says otherwise.
    pub service: &'a str,
    /// `service.instance.id` (the engine's name). The host name when `None`.
    pub instance: Option<&'a str>,
}

/// What is running, for [`shutdown`].
struct Running {
    traces: Option<SdkTracerProvider>,
    metrics: Option<metrics::Pusher>,
}

static RUNNING: Mutex<Option<Running>> = Mutex::new(None);

/// Read the settings from the process environment.
pub fn settings_from_env(default_service: &str) -> Result<Settings, ConfigError> {
    settings_from(|name| std::env::var(name).ok(), default_service)
}

/// Install logging, with OpenTelemetry export beside it when the environment
/// asks for it. Must run inside a Tokio runtime when export is on.
///
/// `Err` is a setting that cannot be honoured; logging is installed anyway
/// (without export) so the caller can report it, and the caller should stop.
pub fn init(level: &str, json: bool, process: Process<'_>) -> Result<(), ConfigError> {
    let export = match settings_from_env(process.service) {
        Err(e) => {
            crate::logging::init(level, json);
            return Err(e);
        }
        Ok(Settings::Off(reason)) => {
            crate::logging::init(level, json);
            tracing::info!("OpenTelemetry export disabled ({reason})");
            return Ok(());
        }
        Ok(Settings::On(export)) => export,
    };
    let runtime = match tokio::runtime::Handle::try_current() {
        Ok(runtime) => runtime,
        Err(_) => {
            crate::logging::init(level, json);
            tracing::error!("OpenTelemetry export needs an async runtime; continuing without it");
            return Ok(());
        }
    };
    let resource = resource_attributes(&export, process);
    let traces = match &export.traces {
        None => None,
        Some(signal) => match build_tracer_provider(signal, &export, &resource, &runtime) {
            Ok(provider) => Some(provider),
            Err(why) => {
                crate::logging::init(level, json);
                return Err(ConfigError {
                    variable: settings::ENDPOINT_ENV,
                    message: format!("the trace exporter could not be built: {}", redact(&why)),
                });
            }
        },
    };
    let layer = traces.as_ref().map(layer);
    if !crate::logging::init_with_layer(level, json, layer, traces.is_some()) {
        // A subscriber was already in place: nothing would reach the layer.
        if let Some(provider) = traces {
            let _ = provider.shutdown_with_timeout(SHUTDOWN_FLUSH_DEADLINE);
        }
        return Ok(());
    }
    let metrics = match &export.metrics {
        None => None,
        Some(signal) => match http::OnRuntime::new(signal.timeout, runtime) {
            Ok(client) => Some(metrics::Pusher::start(
                signal.clone(),
                export.metric_interval,
                client,
                resource.clone(),
                SystemTime::now(),
            )),
            Err(why) => {
                tracing::error!(
                    error = %redact(&why),
                    "OpenTelemetry metrics export could not start; continuing without it"
                );
                None
            }
        },
    };
    *RUNNING.lock().unwrap_or_else(|e| e.into_inner()) = Some(Running { traces, metrics });
    tracing::info!("{}", export.describe());
    Ok(())
}

/// The resource attributes both signals carry: `service.name`,
/// `service.version` (this release), `service.instance.id`, `host.name`, then
/// `OTEL_RESOURCE_ATTRIBUTES`, which may override all but the version and
/// the name (`OTEL_SERVICE_NAME` and the process default decide that).
pub fn resource_attributes(export: &Export, process: Process<'_>) -> Vec<(String, String)> {
    let host = host_name();
    let mut attributes = vec![
        ("service.name".to_owned(), export.service_name.clone()),
        (
            "service.version".to_owned(),
            env!("CARGO_PKG_VERSION").to_owned(),
        ),
        (
            "service.instance.id".to_owned(),
            process.instance.map_or_else(|| host.clone(), str::to_owned),
        ),
        ("host.name".to_owned(), host),
    ];
    for (key, value) in &export.resource_attributes {
        if key == "service.name" || key == "service.version" {
            continue;
        }
        match attributes.iter_mut().find(|(k, _)| k == key) {
            Some(slot) => slot.1 = value.clone(),
            None => attributes.push((key.clone(), value.clone())),
        }
    }
    attributes
}

fn host_name() -> String {
    rustix::system::uname()
        .nodename()
        .to_string_lossy()
        .into_owned()
}

/// The trace pipeline `init` installs for `export`, without installing it:
/// for the tests, which drive the real exporter against a local collector.
/// Must run inside a Tokio runtime.
pub fn build_tracer_provider_for(
    export: &Export,
    process: Process<'_>,
) -> Result<SdkTracerProvider, String> {
    let signal = export
        .traces
        .as_ref()
        .ok_or_else(|| "trace export is off".to_owned())?;
    let runtime = tokio::runtime::Handle::try_current().map_err(|e| e.to_string())?;
    build_tracer_provider(
        signal,
        export,
        &resource_attributes(export, process),
        &runtime,
    )
}

fn build_tracer_provider(
    signal: &Signal,
    export: &Export,
    resource: &[(String, String)],
    runtime: &tokio::runtime::Handle,
) -> Result<SdkTracerProvider, String> {
    let client = http::OnRuntime::new(signal.timeout, runtime.clone())?;
    let headers = signal.headers.iter().cloned().collect();
    let exporter = SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(signal.endpoint.clone())
        .with_timeout(signal.timeout)
        .with_headers(headers)
        .with_http_client(client)
        .build()
        .map_err(|e| e.to_string())?;
    Ok(tracer_provider(
        ExportQueue::new(exporter, export.queue),
        export.sampler,
        resource,
    ))
}

/// A provider over `queue`, with the sampler and resource the binary uses.
/// The tests build theirs here too.
pub fn tracer_provider(
    queue: ExportQueue,
    sampler: Sampler,
    resource: &[(String, String)],
) -> SdkTracerProvider {
    let resource = opentelemetry_sdk::Resource::builder_empty()
        .with_attributes(
            resource
                .iter()
                .map(|(k, v)| opentelemetry::KeyValue::new(k.clone(), v.clone())),
        )
        .build();
    SdkTracerProvider::builder()
        .with_span_processor(queue)
        .with_sampler(sampler.to_sdk())
        .with_resource(resource)
        .build()
}

/// Which spans are exported: the processes' own (targets `vtop_*`), and
/// nothing else. A library's spans are not ours to vouch for (an HTTP
/// client's can carry a request URL with its query string), and log events
/// are not exported at all.
pub fn exported(meta: &Metadata<'_>) -> bool {
    meta.is_span() && meta.target().starts_with("vtop_")
}

/// The tracing layer that hands the exported spans to `provider`. The
/// binaries and the tests build it here, so the tests exercise the filter
/// and the attribute choices the binaries ship.
pub fn layer<S>(provider: &SdkTracerProvider) -> impl Layer<S> + Send + Sync
where
    S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
{
    tracing_opentelemetry::layer()
        .with_tracer(provider.tracer("vtop"))
        // Source location, thread and target attributes describe the build,
        // not the work. Spans are entered only across a batch's own polls,
        // so busy/idle time would mislead; it is not recorded either.
        .with_location(false)
        .with_threads(false)
        .with_target(false)
        .with_tracked_inactivity(false)
        .with_filter(ExportedOnly)
}

/// [`exported`] as a per-layer filter that decides once per callsite.
///
/// `filter_fn` answers `Interest::sometimes` for every callsite and gives no
/// level hint, so with export on every `trace!` in hyper, rustls or the AWS
/// SDK would be evaluated on every call instead of being switched off once
/// (review of #693). The answer depends only on static metadata, so it is
/// `always` or `never`; the exported spans are all DEBUG or above.
struct ExportedOnly;

impl<S> tracing_subscriber::layer::Filter<S> for ExportedOnly {
    fn enabled(&self, meta: &Metadata<'_>, _: &tracing_subscriber::layer::Context<'_, S>) -> bool {
        exported(meta)
    }

    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> tracing::subscriber::Interest {
        if exported(meta) {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(tracing::level_filters::LevelFilter::DEBUG)
    }
}

/// The trace and span ids of the span the current thread is in, for a log
/// line. The OTLP layer makes a span's context current while it is entered.
pub fn current_ids() -> Option<(String, String)> {
    use opentelemetry::trace::TraceContextExt;
    let cx = opentelemetry::Context::current();
    let span = cx.span();
    let sc = span.span_context();
    sc.is_valid()
        .then(|| (sc.trace_id().to_string(), sc.span_id().to_string()))
}

/// Register the exporter's counters into `registry`, and make it the registry
/// the OTLP metrics export reads. Call once the process has its registry.
pub fn register_registry(registry: &prometheus::Registry) -> Result<(), prometheus::Error> {
    stats::register(registry)?;
    metrics::set_registry(registry);
    Ok(())
}

/// Whether export is running in this process.
pub fn running() -> bool {
    RUNNING.lock().unwrap_or_else(|e| e.into_inner()).is_some()
}

/// Whether metrics export is running, so the binary knows to build its
/// registry even with `/metrics` off.
pub fn metrics_running() -> bool {
    RUNNING
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .is_some_and(|r| r.metrics.is_some())
}

/// Flush and stop export, waiting at most [`SHUTDOWN_FLUSH_DEADLINE`], and not
/// at all once another SIGINT or SIGTERM arrives. A no-op when export is off.
///
/// The extra signal matters after a second one has already abandoned the
/// engine's own flush: that is the operator asking to exit now, and Tokio has
/// replaced the default disposition, so without a listener here a further
/// Ctrl-C would do nothing while a dead collector held the exit for the whole
/// deadline (review of #693).
pub async fn shutdown() {
    let Some(running) = RUNNING.lock().unwrap_or_else(|e| e.into_inner()).take() else {
        return;
    };
    shutdown_unless(running.traces, running.metrics, stop_signal()).await;
}

/// [`shutdown`]'s work, ended early by `stop`. Public for the tests.
pub async fn flush_unless(
    provider: SdkTracerProvider,
    stop: impl std::future::Future<Output = ()>,
) {
    shutdown_unless(Some(provider), None, stop).await;
}

async fn shutdown_unless(
    traces: Option<SdkTracerProvider>,
    metrics: Option<metrics::Pusher>,
    stop: impl std::future::Future<Output = ()>,
) {
    // The SDK's shutdown blocks its caller until the export thread answers,
    // so it runs on the blocking pool: on an async worker it would stall the
    // very runtime the export's HTTP request is spawned on. The outer timeout
    // is a second bound in case the queue's own is ever not honoured; if it
    // fires (or `stop` does), the blocking task is left behind and the process
    // exits without it. The queue logs its own outcome.
    // Started before waiting, and owned by the blocking task: if `stop` wins,
    // nothing here drops the provider (whose drop would itself block for the
    // SDK's shutdown).
    let traces = traces.map(|provider| {
        tokio::task::spawn_blocking(move || provider.shutdown_with_timeout(SHUTDOWN_FLUSH_DEADLINE))
    });
    let traces = async {
        if let Some(flush) = traces {
            if tokio::time::timeout(SHUTDOWN_FLUSH_DEADLINE + Duration::from_secs(1), flush)
                .await
                .is_err()
            {
                tracing::warn!(
                    "OpenTelemetry trace flush gave up after {}s; exiting without it",
                    SHUTDOWN_FLUSH_DEADLINE.as_secs() + 1
                );
            }
        }
    };
    let metrics = async {
        if let Some(pusher) = metrics {
            pusher.finish(SHUTDOWN_FLUSH_DEADLINE).await;
        }
    };
    tokio::select! {
        biased;
        () = stop => tracing::warn!(
            "stop signal during the OpenTelemetry flush; exiting without what is still queued"
        ),
        _ = async { tokio::join!(traces, metrics) } => {}
    }
}

/// Resolves on the next SIGINT or SIGTERM. A listener that cannot be
/// registered never resolves, so it can only fail to cut the flush short,
/// never skip it.
async fn stop_signal() {
    let interrupt = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let terminate = async {
            match signal(SignalKind::terminate()) {
                Ok(mut stream) => {
                    stream.recv().await;
                }
                Err(_) => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            () = interrupt => {}
            () = terminate => {}
        }
    }
    #[cfg(not(unix))]
    interrupt.await;
}
