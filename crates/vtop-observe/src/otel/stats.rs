//! The exporter's own counters, so a broken collector shows on `/metrics`
//! (and in the OTLP metrics, once they flow) rather than only in a log line.
//!
//! One set per process, created on first use. The binaries register it into
//! their registry ([`register`]) whether export is on or not, so the series
//! exist at 0 from the first scrape: an alert on them reads 0, not "no data".

use std::sync::OnceLock;

use prometheus::{IntCounter, IntCounterVec, IntGauge, Opts, Registry};

/// The counters. Every label is from a fixed set.
#[derive(Clone)]
pub struct Stats {
    /// Spans the collector accepted.
    pub spans_exported: IntCounter,
    /// Spans lost, by `reason`: `queue_full` (the collector fell behind and
    /// the queue was at capacity), `export_failed` (the collector refused or
    /// could not be reached), `shutdown` (still queued when the flush gave up).
    pub spans_dropped: IntCounterVec,
    /// Export requests that failed, by `signal` (`traces`, `metrics`).
    pub export_failures: IntCounterVec,
    /// Metric data points the collector accepted.
    pub metric_points_exported: IntCounter,
    /// Metric data points in exports that failed.
    pub metric_points_dropped: IntCounter,
    /// Spans waiting for export: queued plus in flight.
    pub queue_length: IntGauge,
}

static STATS: OnceLock<Stats> = OnceLock::new();

/// The process's counters.
pub fn stats() -> &'static Stats {
    STATS.get_or_init(|| {
        let counter = |name: &str, help: &str| {
            IntCounter::with_opts(Opts::new(name, help)).expect("a static counter definition")
        };
        let counter_vec = |name: &str, help: &str, label: &str| {
            IntCounterVec::new(Opts::new(name, help), &[label])
                .expect("a static counter definition")
        };
        let stats = Stats {
            spans_exported: counter(
                "otel_spans_exported_total",
                "Spans the OTLP collector accepted",
            ),
            spans_dropped: counter_vec(
                "otel_spans_dropped_total",
                "Spans not exported, by reason (queue_full, export_failed, shutdown)",
                "reason",
            ),
            export_failures: counter_vec(
                "otel_export_failures_total",
                "OTLP export requests that failed, by signal (traces, metrics)",
                "signal",
            ),
            metric_points_exported: counter(
                "otel_metric_points_exported_total",
                "Metric data points the OTLP collector accepted",
            ),
            metric_points_dropped: counter(
                "otel_metric_points_dropped_total",
                "Metric data points in OTLP exports that failed",
            ),
            queue_length: IntGauge::new(
                "otel_export_queue_length",
                "Spans waiting for OTLP export (queued plus in flight)",
            )
            .expect("a static gauge definition"),
        };
        // Every label value exists from the start, so each series reads 0
        // until something happens.
        for reason in ["queue_full", "export_failed", "shutdown"] {
            stats.spans_dropped.with_label_values(&[reason]);
        }
        for signal in ["traces", "metrics"] {
            stats.export_failures.with_label_values(&[signal]);
        }
        stats
    })
}

/// Register the counters into `registry`. Registering into the same registry
/// twice is not an error: the second call finds them there.
pub fn register(registry: &Registry) -> Result<(), prometheus::Error> {
    let s = stats();
    let collectors: [Box<dyn prometheus::core::Collector>; 6] = [
        Box::new(s.spans_exported.clone()),
        Box::new(s.spans_dropped.clone()),
        Box::new(s.export_failures.clone()),
        Box::new(s.metric_points_exported.clone()),
        Box::new(s.metric_points_dropped.clone()),
        Box::new(s.queue_length.clone()),
    ];
    for collector in collectors {
        match registry.register(collector) {
            Ok(()) | Err(prometheus::Error::AlreadyReg) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
