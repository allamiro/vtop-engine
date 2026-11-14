//! The spans the engine opens for a batch's lifecycle, for recovery's decision
//! on each row, and for scrub's check of each batch (#683).
//!
//! They are plain `tracing` spans and cost nothing unless a subscriber asks
//! for them. They sit at DEBUG, which the default log filter (`info`) turns
//! away at the callsite, so with trace export off the engine creates none.
//! The OTLP layer (`crate::otel`) accepts them whatever the log level, and it
//! is only installed when an endpoint is configured.
//!
//! Each is created with an explicit parent (a batch, recovery and a scrub
//! check are roots) and closed by dropping it. A batch's span is entered only
//! through `Instrument` on that batch's own futures, so it is current exactly
//! while one of them is being polled: never across an `.await` with a guard,
//! which in the verify phase (many batches' futures polled on one task by
//! `buffer_unordered`) would file one batch's log lines under another. The
//! step spans and recovery's spans are never entered.
//!
//! What may be an attribute: identifiers (batch id, source type and name,
//! tenant), counts, sizes, algorithm and method names, and the outcome. What
//! may not: record content, credentials, keys, and URLs, whose query strings
//! can carry signatures. No URI is recorded. A failed batch's error message
//! becomes its span status only after `vtop_observe::otel::redact` has cut
//! every URL to its host and removed MACs, digests and key ids. Log events
//! are not exported at all; the JSON log lines carry the trace and span ids
//! instead, so a trace leads to its lines in Loki.

use tracing::field::Empty;
use tracing::Span;
use vtop_core::replay::RecoveryAction;
use vtop_state::BatchRecord;

/// One step of a batch's lifecycle, each a child span of the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    Seal,
    Compress,
    Checksum,
    UploadObject,
    UploadManifest,
    Verify,
    Commit,
}

impl Stage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Stage::Seal => "seal",
            Stage::Compress => "compress",
            Stage::Checksum => "checksum",
            Stage::UploadObject => "upload_object",
            Stage::UploadManifest => "upload_manifest",
            Stage::Verify => "verify",
            Stage::Commit => "commit",
        }
    }

    fn open(self, batch: &Span) -> Span {
        // One macro call per name: a span's name is part of its static
        // callsite. The batch is named as the parent explicitly, so a stage
        // never depends on what happens to be entered on this thread.
        match self {
            Stage::Seal => {
                tracing::debug_span!(parent: batch, "seal", otel.status_code = Empty, otel.status_description = Empty)
            }
            Stage::Compress => {
                tracing::debug_span!(parent: batch, "compress", otel.status_code = Empty, otel.status_description = Empty)
            }
            Stage::Checksum => {
                tracing::debug_span!(parent: batch, "checksum", otel.status_code = Empty, otel.status_description = Empty)
            }
            Stage::UploadObject => tracing::debug_span!(
                parent: batch,
                "upload_object",
                multipart = Empty,
                otel.status_code = Empty,
                otel.status_description = Empty
            ),
            Stage::UploadManifest => {
                tracing::debug_span!(parent: batch, "upload_manifest", otel.status_code = Empty, otel.status_description = Empty)
            }
            Stage::Verify => {
                tracing::debug_span!(parent: batch, "verify", otel.status_code = Empty, otel.status_description = Empty)
            }
            Stage::Commit => {
                tracing::debug_span!(parent: batch, "commit", otel.status_code = Empty, otel.status_description = Empty)
            }
        }
    }
}

/// The `batch` span and the stage open under it.
///
/// A batch has exactly one recorded outcome. [`BatchTrace::finish`] and
/// [`BatchTrace::fail`] record it; a trace dropped without either (a ledger
/// write or a store request that errored and returned through `?`, or a flush
/// abandoned by a second signal, which drops the future) records `incomplete`
/// as an error, so a batch that never reached an outcome cannot pass for a
/// healthy one.
pub(crate) struct BatchTrace {
    // Declared before `batch` so that dropping the trace closes the stage
    // before its parent.
    stage: Option<(Stage, Span)>,
    batch: Span,
    settled: bool,
}

impl BatchTrace {
    /// Open the root span of one batch.
    pub(crate) fn start(source_type: &str, source_name: &str, records: usize) -> Self {
        let batch = tracing::debug_span!(
            parent: None,
            "batch",
            batch_id = Empty,
            source_type,
            source_name,
            tenant = Empty,
            format = Empty,
            records,
            bytes = Empty,
            compressed_bytes = Empty,
            checksum_algorithm = Empty,
            verification_method = Empty,
            verification_result = Empty,
            result = Empty,
            failed_stage = Empty,
            otel.status_code = Empty,
            otel.status_description = Empty,
        );
        Self {
            stage: None,
            batch,
            settled: false,
        }
    }

    /// The batch span, for `Instrument` on the batch's futures.
    pub(crate) fn span(&self) -> &Span {
        &self.batch
    }

    /// [`BatchTrace::fail`] with the error, redacted, as the status
    /// description of both spans.
    pub(crate) fn fail_with(&mut self, result: &'static str, error: &str) {
        // The description sets the error status by itself; recording
        // `otel.status_code` after it would replace it with an empty one.
        let description = vtop_observe::otel::redact(error);
        if let Some((stage, span)) = self.stage.take() {
            span.record("otel.status_description", description.as_str());
            self.batch.record("failed_stage", stage.as_str());
        }
        self.batch.record("result", result);
        self.batch
            .record("otel.status_description", description.as_str());
        self.settled = true;
    }

    /// Record one of the batch span's declared attributes.
    pub(crate) fn record<V: tracing::Value>(&self, field: &str, value: V) {
        self.batch.record(field, value);
    }

    /// Close the stage in progress, if any, and open `stage`.
    pub(crate) fn begin(&mut self, stage: Stage) {
        self.stage = None;
        self.stage = Some((stage, stage.open(&self.batch)));
    }

    /// Record one of the current stage span's declared attributes.
    pub(crate) fn record_stage<V: tracing::Value>(&self, field: &str, value: V) {
        if let Some((_, span)) = &self.stage {
            span.record(field, value);
        }
    }

    /// Close the stage in progress.
    pub(crate) fn end(&mut self) {
        self.stage = None;
    }

    /// The batch reached an outcome that is not a failure.
    pub(crate) fn finish(&mut self, result: &'static str) {
        self.stage = None;
        self.batch.record("result", result);
        self.batch.record("otel.status_code", "ok");
        self.settled = true;
    }

    /// The batch failed in the stage in progress (or between stages, when
    /// none is open). Both spans carry an error status.
    pub(crate) fn fail(&mut self, result: &'static str) {
        if let Some((stage, span)) = self.stage.take() {
            span.record("otel.status_code", "error");
            self.batch.record("failed_stage", stage.as_str());
        }
        self.batch.record("result", result);
        self.batch.record("otel.status_code", "error");
        self.settled = true;
    }

    #[cfg(test)]
    pub(crate) fn is_disabled(&self) -> bool {
        self.batch.is_disabled()
    }
}

impl Drop for BatchTrace {
    fn drop(&mut self) {
        if !self.settled {
            self.fail("incomplete");
        }
    }
}

/// The root span of one recovery scan.
pub(crate) fn recovery() -> Span {
    tracing::debug_span!(
        parent: None,
        "recovery",
        rows = Empty,
        committed = Empty,
        replay_required = Empty,
        superseded = Empty,
        still_pending = Empty,
    )
}

/// Recovery's decision on one claimed row. Every path through the scan
/// records a `decision`; [`decide`] and [`decide_error`] do so.
pub(crate) fn recovery_row(parent: &Span, rec: &BatchRecord, action: RecoveryAction) -> Span {
    tracing::debug_span!(
        parent: parent,
        "recovery_row",
        batch_id = rec.batch_id.as_str(),
        source_type = rec.source_type.as_str(),
        source_name = rec.source_name.as_str(),
        tenant = rec.tenant.as_str(),
        state = rec.state.as_str(),
        action = ?action,
        recheck = Empty,
        decision = Empty,
        otel.status_code = Empty,
    )
}

/// The commit recovery makes (or withholds) for a row whose storage
/// re-check passed. It is decided after every replay decision is known, so it
/// is its own span rather than the end of the row's.
pub(crate) fn recovery_commit(parent: &Span, rec: &BatchRecord, recheck: &'static str) -> Span {
    tracing::debug_span!(
        parent: parent,
        "recovery_commit",
        batch_id = rec.batch_id.as_str(),
        source_type = rec.source_type.as_str(),
        source_name = rec.source_name.as_str(),
        tenant = rec.tenant.as_str(),
        recheck,
        decision = Empty,
        otel.status_code = Empty,
    )
}

/// Record a decision that leaves nothing wrong behind.
pub(crate) fn decide(span: &Span, decision: &'static str) {
    span.record("decision", decision);
}

/// Record a decision forced by a failure (a ledger write or a source commit
/// that did not land), which leaves the row for a later pass.
pub(crate) fn decide_error(span: &Span, decision: &'static str) {
    span.record("decision", decision);
    span.record("otel.status_code", "error");
}

/// Scrub's check of one committed batch. A root span: a full pass can check
/// millions of batches, and one trace per check is what keeps each findable.
pub(crate) fn scrub_check(rec: &BatchRecord, origin: &'static str) -> Span {
    tracing::debug_span!(
        parent: None,
        "scrub_check",
        batch_id = rec.batch_id.as_str(),
        source_type = rec.source_type.as_str(),
        source_name = rec.source_name.as_str(),
        tenant = rec.tenant.as_str(),
        origin,
        bytes = rec.object_size_bytes,
        result = Empty,
        otel.status_code = Empty,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::EnvFilter;

    fn logs_at(level: &str) -> tracing::subscriber::DefaultGuard {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new(level))
            .with_writer(std::io::sink)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    /// #683: with trace export off, the subscriber is the log output alone,
    /// at the default `info`, and it turns the spans away: none is created,
    /// so the lifecycle costs what it cost before.
    #[test]
    fn under_the_default_log_filter_no_span_is_created() {
        let _guard = logs_at("info");
        let mut trace = BatchTrace::start("file", "app", 3);
        trace.begin(Stage::Compress);
        assert!(trace.is_disabled());
        assert!(trace.stage.as_ref().is_some_and(|(_, s)| s.is_disabled()));
        assert!(recovery().is_disabled());
    }

    /// The control for the test above: a filter that admits them does get
    /// them, so the assertion there is about the filter, not a broken span.
    #[test]
    fn a_filter_that_admits_debug_creates_them() {
        let _guard = logs_at("debug");
        let trace = BatchTrace::start("file", "app", 3);
        assert!(!trace.is_disabled());
    }
}
