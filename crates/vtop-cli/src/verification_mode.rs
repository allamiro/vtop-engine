//! Saying out loud that the engine may commit on a size-only check (#629).
//!
//! `upload.require_strong_verification: false` lets a batch commit when the
//! object store could confirm only size and existence, so a corrupted or
//! replaced object of the recorded size is archived as verified. The opt-out
//! is explicit and documented, but its only signal used to be a WARN per
//! batch: nothing at startup, nothing from a config check, and nothing at all
//! when recovery's storage re-check accepted such a result. These helpers live
//! outside `engine.rs` to keep the engine's own diff to its call sites.

use vtop_core::config::{effective_algorithm, StreamsConfig, VtopConfig};
use vtop_core::telemetry;
use vtop_state::BatchRecord;
use vtop_upload::verification_method;

/// Called once when an engine starts for mutating work (`run`,
/// `process-once`, `replay`). Logs each [`VtopConfig::warnings`] entry as one
/// WARN and sets `vtop_verification_mode{tenant,mode}`.
///
/// The gauge is set only where a metrics registry exists, which is `run`
/// with `VTOP_METRICS_ADDR` set (`metrics_server::maybe_start`). The one-shot
/// commands serve no endpoint and deliberately build no registry, so for
/// them the WARN is the whole signal.
pub(crate) fn announce(config: &VtopConfig, streams: &StreamsConfig) {
    for warning in streams.warnings(config) {
        tracing::warn!("{warning}");
    }
    // One line per stream with the algorithm its new batches use and the
    // method the backend verifies it by (#669), through the same resolver
    // and mapping `validate --print-effective` prints (#670), so the choice
    // is visible without running `validate`.
    let backend = config.upload.backend.as_str();
    let algorithm = effective_algorithm(config, None);
    tracing::info!(
        algorithm = %algorithm,
        method = verification_method(backend, algorithm).as_str(),
        "default verification settings (sources no stream matches)"
    );
    for stream in &streams.streams {
        let algorithm = effective_algorithm(config, Some(stream));
        tracing::info!(
            stream = %stream.source_name,
            algorithm = %algorithm,
            origin = if stream.checksum.is_some() { "stream" } else { "global" },
            method = verification_method(backend, algorithm).as_str(),
            "stream verification settings"
        );
    }
    if let Some(mx) = telemetry::metrics() {
        let size_only = !config.upload.require_strong_verification;
        let tenant = config.engine.tenant.as_str();
        mx.verification_mode
            .with_label_values(&[tenant, "size_only"])
            .set(i64::from(size_only));
        mx.verification_mode
            .with_label_values(&[tenant, "strong"])
            .set(i64::from(!size_only));
    }
}

/// What recovery's storage re-check of a VERIFIED row concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Recheck {
    /// The row must not commit; it is replayed instead.
    Failed,
    /// Stored content matched a digest.
    Strong,
    /// Only size and existence were confirmed, and the opt-out allows it.
    SizeOnly,
}

/// Recovery's storage re-check got a size-only result for a VERIFIED row.
/// Under the strong default the row fails the re-check; under the opt-out it
/// passes as [`Recheck::SizeOnly`]. Either way the decision is logged with
/// the batch id. The COUNT waits for the commit (see [`count_size_only_commit`]):
/// a row that passes here can still be held back under a lower incomplete
/// range, stay VERIFIED, and be re-checked by every later recovery.
pub(crate) fn recheck_size_only(config: &VtopConfig, rec: &BatchRecord, why: &str) -> Recheck {
    if config.upload.require_strong_verification {
        tracing::warn!(
            batch_id = %rec.batch_id,
            reason = %why,
            "recovery re-check: storage confirmed size only and strong verification \
             is required; not committing"
        );
        return Recheck::Failed;
    }
    tracing::warn!(
        batch_id = %rec.batch_id,
        reason = %why,
        "recovery re-check passed on size alone (backend_limited) under \
         upload.require_strong_verification: false"
    );
    Recheck::SizeOnly
}

/// Recovery committed a row whose re-check was size-only: count it in
/// `vtop_verification_backend_limited_total` with the running engine's labels,
/// beside the `verified_total` recovery counts for the same commit.
pub(crate) fn count_size_only_commit(recheck: Recheck, rec: &BatchRecord) {
    if recheck != Recheck::SizeOnly {
        return;
    }
    tracing::warn!(
        batch_id = %rec.batch_id,
        "recovered: source_committed on a size-only verification"
    );
    if let Some(mx) = telemetry::metrics() {
        mx.verification_backend_limited_total
            .with_label_values(&[
                rec.tenant.as_str(),
                rec.source_type.as_str(),
                rec.format.extension(),
            ])
            .inc();
    }
}
