//! Saying out loud that the engine may commit on a size-only check (#629).
//!
//! `upload.require_strong_verification: false` lets a batch commit when the
//! object store could confirm only size and existence, so a corrupted or
//! replaced object of the recorded size is archived as verified. The opt-out
//! is explicit and documented, but its only signal used to be a WARN per
//! batch: nothing at startup, nothing from a config check, and nothing at all
//! when recovery's storage re-check accepted such a result. These helpers live
//! outside `engine.rs` so the engine's own diff stays two call sites.

use vtop_core::config::VtopConfig;
use vtop_core::telemetry;
use vtop_state::BatchRecord;

/// Called once when an engine starts for mutating work (`run`,
/// `process-once`, `replay`). Logs each [`VtopConfig::warnings`] entry as one
/// WARN and sets `vtop_verification_mode{tenant,mode}`, so the mode can be
/// alerted on before any batch moves.
pub(crate) fn announce(config: &VtopConfig) {
    for warning in config.warnings() {
        tracing::warn!("{warning}");
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

/// Recovery's storage re-check got a size-only result for a VERIFIED row.
/// Returns whether the row may still commit: only under the explicit opt-out.
/// Either way the decision is logged with the batch id; an acceptance is
/// counted in `vtop_verification_backend_limited_total` with the labels the
/// running engine uses, because it is the same weak commit the counter exists
/// for, made by a different path.
pub(crate) fn recheck_accepts_size_only(config: &VtopConfig, rec: &BatchRecord, why: &str) -> bool {
    if config.upload.require_strong_verification {
        tracing::warn!(
            batch_id = %rec.batch_id,
            reason = %why,
            "recovery re-check: storage confirmed size only and strong verification \
             is required; not committing"
        );
        return false;
    }
    tracing::warn!(
        batch_id = %rec.batch_id,
        reason = %why,
        "recovery re-check passed (backend_limited: size-only); committing under \
         upload.require_strong_verification: false"
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
    true
}
