//! Prometheus metrics for the engine itself.
//!
//! Why this module exists: every other component in a VTOP deployment can be
//! observed (MinIO exposes Prometheus natively, Kafka via JMX), but the engine —
//! the component whose correctness actually matters — could only be observed
//! through its logs. [`crate::metrics::BatchMetrics`] already measured the right
//! things; they simply were not exported. This exports them.
//!
//! Design rules, chosen deliberately:
//!
//! * **Label cardinality is bounded.** Labels are tenant / source_type / format
//!   / stage / state — all small, closed sets. `batch_id` and object URIs are
//!   NEVER labels: they are unbounded and would blow up the TSDB. They stay in
//!   logs, where high-cardinality detail belongs.
//! * **Only measured facts are exported.** Rates and throughput (records/sec,
//!   MiB/s) are *derived*, so they are computed in PromQL from counters rather
//!   than exported as gauges — a gauge of a rate is a snapshot that lies between
//!   scrapes.
//! * **The invariant is first-class.** `verification_failures_total` and
//!   `commits_total` exist so the rule "SOURCE_COMMITTED is forbidden until
//!   VERIFIED" is measurable, not merely asserted in tests.
//! * **Registration cannot fail silently.** A metric that fails to register
//!   would otherwise become a permanently-zero panel, which is worse than no
//!   panel at all.

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use std::sync::OnceLock;

/// Pipeline stages, used as a bounded `stage` label.
pub const STAGES: [&str; 7] = [
    "compress",
    "checksum",
    "object_upload",
    "manifest_upload",
    "verify",
    "commit",
    "state_write",
];

/// The engine's metric set. Cheap to clone (all fields are `Arc` internally).
pub struct Metrics {
    pub registry: Registry,

    /// Batches entering each lifecycle state. `state` is the bounded set from
    /// the state machine, so `sum by (state)` reconstructs the funnel and shows
    /// exactly where batches stop.
    pub batches_total: IntCounterVec,

    /// Batches that completed verification. Together with `commits_total` this
    /// makes the core invariant observable: a commit without a preceding
    /// verification is impossible, so `commits_total > verified_total` would
    /// mean the guarantee is broken.
    pub verified_total: IntCounterVec,
    pub commits_total: IntCounterVec,

    /// **Any non-zero value here is an incident**: an uploaded object did not
    /// match its manifest. The engine refuses to commit, so data is safe but
    /// stuck.
    pub verification_failures_total: IntCounterVec,

    /// Verified by size/existence only because the backend could not do better.
    /// Committing on this is weaker than the protocol intends; production sets
    /// `upload.require_strong_verification` to refuse it.
    pub verification_backend_limited_total: IntCounterVec,

    /// Batches sent back to be re-read from source. Safe by design, but a
    /// sustained rate means work is being repeated.
    pub replay_required_total: IntCounterVec,
    pub failed_total: IntCounterVec,

    /// Volume. Rates are derived from these in PromQL, never exported directly.
    pub records_total: IntCounterVec,
    pub bytes_in_total: IntCounterVec,
    pub bytes_out_total: IntCounterVec,

    /// Per-stage wall clock. A histogram, so p95/p99 are answerable — an average
    /// hides exactly the tail an operator cares about.
    pub stage_duration_seconds: HistogramVec,
    /// Whole batch: discovered -> source_committed.
    pub batch_duration_seconds: HistogramVec,
    /// Compression effectiveness (uncompressed/compressed).
    pub compression_ratio: HistogramVec,

    /// Batches accumulated but not yet sealed. A gauge that climbs without
    /// bound means sealing has stalled.
    pub inflight_batches: IntGauge,
    /// Cycles where a source read failed and was skipped (the engine retries
    /// next cycle). A steady rate means a source is unhealthy.
    ///
    /// Labelled by source_type ONLY, never by source name. File and syslog
    /// adapters set `source_name` to the full matched path, and the lab globs
    /// `/data/input/*.log`, so a rotated or dated file set would mint a new
    /// series per file and grow without bound. The failing path is in the log
    /// line beside this counter, where unbounded detail belongs.
    pub source_read_errors_total: IntCounterVec,
}

fn labels3() -> Vec<&'static str> {
    vec!["tenant", "source_type", "format"]
}

impl Metrics {
    /// Build and register the metric set.
    ///
    /// Returns `Err` if registration fails rather than swallowing it: a metric
    /// that silently failed to register renders as a permanently-zero panel,
    /// which is more dangerous than an obviously missing one.
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new_custom(Some("vtop".into()), None)?;

        let cv = |name: &str, help: &str, labels: Vec<&str>| -> Result<IntCounterVec, _> {
            let c = IntCounterVec::new(Opts::new(name, help), &labels)?;
            registry.register(Box::new(c.clone()))?;
            Ok::<IntCounterVec, prometheus::Error>(c)
        };

        let batches_total = cv(
            "batches_total",
            "Batches entering each lifecycle state; sum by (state) reconstructs the pipeline funnel",
            vec!["tenant", "source_type", "format", "state"],
        )?;
        let verified_total = cv(
            "verified_total",
            "Batches that passed verification (object and manifest confirmed)",
            labels3(),
        )?;
        let commits_total = cv(
            "commits_total",
            "Source progress commits; MUST never exceed vtop_verified_total (the core invariant)",
            labels3(),
        )?;
        let verification_failures_total = cv(
            "verification_failures_total",
            "Verification failures; any non-zero value is an incident (object did not match its manifest)",
            labels3(),
        )?;
        let verification_backend_limited_total = cv(
            "verification_backend_limited_total",
            "Verifications confirmed by size/existence only, without a checksum",
            labels3(),
        )?;
        let replay_required_total = cv(
            "replay_required_total",
            "Batches marked REPLAY_REQUIRED and re-read from source",
            labels3(),
        )?;
        let failed_total = cv(
            "failed_total",
            "Batches that reached the FAILED state",
            labels3(),
        )?;
        let records_total = cv(
            "records_total",
            "Telemetry records archived; derive records/sec with rate() in PromQL",
            labels3(),
        )?;
        let bytes_in_total = cv(
            "bytes_in_total",
            "Uncompressed source bytes read into batches",
            labels3(),
        )?;
        let bytes_out_total = cv(
            "bytes_out_total",
            "Compressed bytes uploaded to object storage (bytes actually on the wire)",
            labels3(),
        )?;
        let source_read_errors_total = cv(
            "source_read_errors_total",
            "Source reads that failed and were skipped for this cycle (see logs for the path)",
            vec!["tenant", "source_type"],
        )?;

        // Buckets span 1ms..~30s: compression is sub-millisecond on small
        // batches while an upload to a slow backend can take seconds. Default
        // buckets would put nearly everything in one bin and answer nothing.
        let stage_buckets = vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
        ];
        let stage_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "stage_duration_seconds",
                "Wall-clock duration of each pipeline stage",
            )
            .buckets(stage_buckets.clone()),
            &["tenant", "source_type", "format", "stage"],
        )?;
        registry.register(Box::new(stage_duration_seconds.clone()))?;

        // A batch can legitimately take a minute: it seals on max_batch_age
        // (60s by default), so the upper buckets must reach past that or every
        // idle-lab batch lands in +Inf.
        let batch_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "batch_duration_seconds",
                "End-to-end batch duration, from batch start to source-committed",
            )
            .buckets(vec![
                0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
            ]),
            &labels3(),
        )?;
        registry.register(Box::new(batch_duration_seconds.clone()))?;

        let compression_ratio = HistogramVec::new(
            HistogramOpts::new(
                "compression_ratio",
                "Uncompressed/compressed size ratio; 1.0 means compression achieved nothing",
            )
            .buckets(vec![1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 7.5, 10.0, 20.0, 50.0]),
            &labels3(),
        )?;
        registry.register(Box::new(compression_ratio.clone()))?;

        let inflight_batches = IntGauge::with_opts(Opts::new(
            "inflight_batches",
            "Batches accumulated in memory but not yet sealed",
        ))?;
        registry.register(Box::new(inflight_batches.clone()))?;

        Ok(Self {
            registry,
            batches_total,
            verified_total,
            commits_total,
            verification_failures_total,
            verification_backend_limited_total,
            replay_required_total,
            failed_total,
            records_total,
            bytes_in_total,
            bytes_out_total,
            stage_duration_seconds,
            batch_duration_seconds,
            compression_ratio,
            inflight_batches,
            source_read_errors_total,
        })
    }

    /// Render the registry in Prometheus text format.
    pub fn encode(&self) -> Result<String, prometheus::Error> {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf)?;
        String::from_utf8(buf).map_err(|e| prometheus::Error::Msg(e.to_string()))
    }
}

/// Process-wide metrics.
///
/// A single registry per process: the engine constructs one and the HTTP
/// endpoint reads it. `OnceLock` (not lazy_static) so a registration failure
/// surfaces at startup rather than on first use.
static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Initialize the global metric set. Idempotent; safe to call more than once
/// (tests do).
pub fn init() -> Result<&'static Metrics, prometheus::Error> {
    if let Some(m) = METRICS.get() {
        return Ok(m);
    }
    let m = Metrics::new()?;
    Ok(METRICS.get_or_init(|| m))
}

/// The global metric set, if initialized.
///
/// Returns `Option` rather than panicking: metrics are optional (the endpoint is
/// only started when `VTOP_METRICS_ADDR` is set), and telemetry must never be
/// able to take down the data path.
pub fn metrics() -> Option<&'static Metrics> {
    METRICS.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_is_idempotent_and_registration_succeeds() {
        let a = init().expect("metrics must register");
        let b = init().expect("second init must not fail");
        assert!(std::ptr::eq(a, b), "init() must return the same instance");
    }

    #[test]
    fn encodes_prometheus_text_with_the_vtop_prefix() {
        let m = init().unwrap();
        m.records_total
            .with_label_values(&["default", "file", "jsonl"])
            .inc_by(5);
        let text = m.encode().unwrap();
        assert!(
            text.contains("vtop_records_total"),
            "metrics must carry the vtop_ prefix: {text}"
        );
        assert!(text.contains("# TYPE vtop_records_total counter"));
        assert!(
            text.contains(r#"source_type="file""#),
            "labels must be present"
        );
    }

    /// Guards the design rule: labels must be bounded. batch_id is unbounded and
    /// must never appear, or the TSDB series count grows without limit.
    #[test]
    fn no_unbounded_labels_are_exposed() {
        let m = init().unwrap();
        let text = m.encode().unwrap();
        for forbidden in ["batch_id", "object_uri", "manifest_uri", "path="] {
            assert!(
                !text.contains(forbidden),
                "high-cardinality label {forbidden} must not be a metric label"
            );
        }
    }

    #[test]
    fn every_stage_label_is_recorded() {
        let m = init().unwrap();
        for s in STAGES {
            m.stage_duration_seconds
                .with_label_values(&["default", "kafka", "cef", s])
                .observe(0.01);
        }
        let text = m.encode().unwrap();
        for s in STAGES {
            assert!(
                text.contains(&format!(r#"stage="{s}""#)),
                "missing stage {s}"
            );
        }
    }
}
