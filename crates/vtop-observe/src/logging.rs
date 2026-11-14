//! One definition of "structured logs" for every VTOP process (#224).
//!
//! JSON is selected by an explicit argument OR `VTOP_LOG_FORMAT=json`. The env
//! form lets a container opt into structured logs without changing the
//! entrypoint, so a log pipeline (Alloy -> Loki) gets parseable
//! `{"level":...,"records":...}` lines instead of pretty text. In pretty mode
//! ANSI colour is emitted ONLY to a real terminal: writing escape codes to a
//! pipe (a container's captured stderr) corrupts every downstream parser — a
//! `level=~"WARN"` filter or `| logfmt` then matches nothing because the field
//! names are wrapped in `\e[3m…\e[0m`.
//!
//! Logs go to STDERR so they never collide with command output on STDOUT
//! (notably machine-readable `--json` payloads, and the live-cluster ready
//! markers the chaos harness parses).

/// Environment variable that selects the log encoding.
pub const FORMAT_ENV: &str = "VTOP_LOG_FORMAT";

/// Whether `VTOP_LOG_FORMAT` asks for JSON.
pub fn json_requested() -> bool {
    std::env::var(FORMAT_ENV)
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false)
}

/// Install the global subscriber.
///
/// `level` is the fallback filter, used only when `RUST_LOG` is unset.
/// `RUST_LOG` winning is deliberate and is the conventional Rust escape hatch:
/// an operator debugging a running deployment must be able to raise verbosity
/// without editing a config file or the entrypoint that passed a flag.
///
/// Whatever either says, the audit target ([`crate::audit::TARGET`]) is
/// written (#694): an audit record is evidence, and an operator's filter of
/// `error` must not silence who changed state.
///
/// Installing twice is not an error: the second call is a no-op because a
/// subscriber is already in place, which is the outcome the caller wanted
/// anyway. Panicking there would let a telemetry detail abort a process whose
/// logging is, by definition, already working.
pub fn init(level: &str, json: bool) {
    use std::io::IsTerminal;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let json = json || json_requested();
    let logs = log_layer(
        level,
        std::env::var("RUST_LOG").ok(),
        json,
        false,
        std::io::stderr().is_terminal(),
        std::io::stderr,
    );
    let _ = tracing_subscriber::registry().with(logs).try_init();
}

/// [`init`] with one more layer beside the log output: the OpenTelemetry
/// trace layer (#683). Returns whether this call installed the subscriber;
/// when it did not (one was already in place), `extra` was dropped unused.
///
/// The log filter here is the log layer's own, not the subscriber's, so the
/// extra layer sees what it asks for whatever `RUST_LOG` says, and the log
/// output is filtered exactly as [`init`] filters it.
///
/// With `correlate` and JSON output, a line written inside an exported span
/// starts with its `trace_id` and `span_id`, so Loki and Tempo can link the
/// two. Lines outside one are unchanged.
pub fn init_with_layer<L>(level: &str, json: bool, extra: L, correlate: bool) -> bool
where
    L: tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    use std::io::IsTerminal;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let json = json || json_requested();
    let logs = log_layer(
        level,
        std::env::var("RUST_LOG").ok(),
        json,
        correlate,
        std::io::stderr().is_terminal(),
        std::io::stderr,
    );
    tracing_subscriber::registry()
        .with(extra)
        .with(logs)
        .try_init()
        .is_ok()
}

/// The log layer [`init`] installs, writing to `writer` instead of stderr and
/// taking `RUST_LOG`'s value as an argument: for tests, which must exercise
/// the filter the binaries ship without touching the process environment.
pub fn log_layer_with_writer<S, W>(
    level: &str,
    rust_log: Option<String>,
    json: bool,
    correlate: bool,
    writer: W,
) -> Box<dyn tracing_subscriber::Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
    W: for<'w> tracing_subscriber::fmt::MakeWriter<'w> + Send + Sync + 'static,
{
    log_layer(level, rust_log, json, correlate, false, writer)
}

fn log_layer<S, W>(
    level: &str,
    rust_log: Option<String>,
    json: bool,
    correlate: bool,
    ansi: bool,
    writer: W,
) -> Box<dyn tracing_subscriber::Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
    W: for<'w> tracing_subscriber::fmt::MakeWriter<'w> + Send + Sync + 'static,
{
    use tracing_subscriber::filter::FilterExt;
    use tracing_subscriber::Layer;

    let filter = rust_log
        .and_then(|value| tracing_subscriber::EnvFilter::try_new(value).ok())
        .unwrap_or_else(|| tracing_subscriber::EnvFilter::new(level.to_lowercase()))
        .or(AuditTarget);
    let logs = tracing_subscriber::fmt::layer().with_writer(writer);
    if json && correlate {
        logs.json()
            .event_format(Correlated(
                tracing_subscriber::fmt::format()
                    .json()
                    .with_current_span(false),
            ))
            .with_filter(filter)
            .boxed()
    } else if json {
        logs.json()
            .with_current_span(false)
            .with_filter(filter)
            .boxed()
    } else {
        logs.with_ansi(ansi).with_filter(filter).boxed()
    }
}

/// Admits the audit target and nothing else, decided once per callsite from
/// its static metadata, so OR-ing it into the log filter adds no per-event
/// work to any other callsite (the lesson of #693's `ExportedOnly`).
struct AuditTarget;

fn is_audit(meta: &tracing::Metadata<'_>) -> bool {
    meta.is_event() && meta.target() == crate::audit::TARGET
}

impl<S> tracing_subscriber::layer::Filter<S> for AuditTarget {
    fn enabled(
        &self,
        meta: &tracing::Metadata<'_>,
        _: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        is_audit(meta)
    }

    fn callsite_enabled(
        &self,
        meta: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        if is_audit(meta) {
            tracing::subscriber::Interest::always()
        } else {
            tracing::subscriber::Interest::never()
        }
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        // Audit records are INFO, WARN or ERROR.
        Some(tracing::level_filters::LevelFilter::INFO)
    }
}

/// A JSON line with the current span's `trace_id` and `span_id` in front.
struct Correlated<F>(F);

impl<S, N, F> tracing_subscriber::fmt::FormatEvent<S, N> for Correlated<F>
where
    F: tracing_subscriber::fmt::FormatEvent<S, N>,
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let Some((trace_id, span_id)) = crate::otel::current_ids() else {
            return self.0.format_event(ctx, writer, event);
        };
        let mut line = String::new();
        self.0.format_event(
            ctx,
            tracing_subscriber::fmt::format::Writer::new(&mut line),
            event,
        )?;
        match line.strip_prefix('{') {
            // Both ids are lowercase hex, so they need no escaping.
            Some(rest) => write!(
                writer,
                "{{\"trace_id\":\"{trace_id}\",\"span_id\":\"{span_id}\",{rest}"
            ),
            None => writer.write_str(&line),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `VTOP_LOG_FORMAT` is process-wide; these tests mutate it, so they must
    /// not run concurrently with each other.
    fn env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    #[test]
    fn json_is_opt_in_and_case_insensitive() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        std::env::remove_var(FORMAT_ENV);
        assert!(!json_requested(), "pretty text is the default");
        std::env::set_var(FORMAT_ENV, "JSON");
        assert!(
            json_requested(),
            "a container setting JSON in any case must get structured logs"
        );
        std::env::set_var(FORMAT_ENV, "text");
        assert!(!json_requested());
        std::env::remove_var(FORMAT_ENV);
    }

    #[test]
    fn init_twice_does_not_panic() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        init("info", false);
        // A second init (e.g. a library helper plus a main) must be inert, not
        // fatal.
        init("debug", true);
    }
}
