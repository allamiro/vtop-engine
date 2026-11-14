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
use vtop_state::{BatchRecord, VerificationClass};
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
    for stream in streams.applied() {
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
    /// The manifest is from a newer release (#671): the row is neither
    /// committed nor replayed, and waits for an upgraded engine.
    UnsupportedVersion,
    /// Stored content matched a digest.
    Strong,
    /// Only size and existence were confirmed, and the opt-out allows it.
    SizeOnly,
}

impl Recheck {
    /// The re-check's result, as the logs name it.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Failed => "failed",
            Self::UnsupportedVersion => "unsupported_version",
            Self::Strong => "strong",
            Self::SizeOnly => "size_only",
        }
    }
}

/// The class a row was verified with, as the logs name it: `unknown` for a
/// row with none (#664).
pub(crate) fn class_name(rec: &BatchRecord) -> &'static str {
    rec.verification_class
        .map_or("unknown", VerificationClass::as_str)
}

/// Count one recovery re-check of a VERIFIED row in
/// `vtop_recovery_rechecks_total`, by the class the row was verified with and
/// the re-check's result (#664). Bounded labels: three classes, four results.
pub(crate) fn count_recheck(rec: &BatchRecord, recheck: Recheck) {
    if let Some(mx) = telemetry::metrics() {
        mx.recovery_rechecks_total
            .with_label_values(&[
                rec.tenant.as_str(),
                rec.source_type.as_str(),
                rec.format.extension(),
                class_name(rec),
                recheck.as_str(),
            ])
            .inc();
    }
}

/// Recovery re-checked a VERIFIED row and may commit it (#664): say how its
/// commit will be counted when the recorded class cannot speak for itself -
/// it has none, or the re-check found something else - and what, if
/// anything, to do. A row whose class and re-check agree is not logged here;
/// its commit is logged as before.
pub(crate) fn note_stored_class(rec: &BatchRecord, recheck: Recheck) {
    let found = recheck.as_str();
    match rec.verification_class {
        None => tracing::info!(
            batch_id = %rec.batch_id,
            verification_class = "unknown",
            recheck = found,
            "verification class unknown for batch {}: it was verified by a release before the \
             ledger recorded classes (or one newer than this). Recovery re-checked the stored \
             object ({found}) and counts its commit by that when it commits, as before. Nothing \
             to do",
            rec.batch_id
        ),
        Some(VerificationClass::Strong) if recheck == Recheck::SizeOnly => tracing::warn!(
            batch_id = %rec.batch_id,
            verification_class = "strong",
            recheck = found,
            "batch {} was verified strong, but recovery's re-check could confirm only its size \
             (upload.require_strong_verification: false lets it commit). Its commit is counted \
             as strong, the class that admitted it. The store can no longer show its content \
             matches: restore checksum support (checksums enabled, a backend that can hash the \
             stored bytes), then run `vtopctl scrub` to re-check committed batches",
            rec.batch_id
        ),
        Some(VerificationClass::SizeOnly) if recheck == Recheck::Strong => tracing::info!(
            batch_id = %rec.batch_id,
            verification_class = "size_only",
            recheck = found,
            "batch {} was verified size-only, and recovery's re-check now matched its content. \
             Its commit is counted as size-only, the class that admitted it. Nothing to do",
            rec.batch_id
        ),
        Some(_) => {}
    }
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

/// Whether a commit recovery makes of `rec` counts as a size-only one: by
/// the class the row was verified with, as the ledger recorded it with the
/// VERIFIED transition (#664).
///
/// The re-check decides only whether the row may commit. Its class can
/// differ from the verification that admitted the row: a strong row whose
/// re-check confirms size alone, which the opt-out lets commit, or a
/// size-only one whose re-check finds a digest. Counting by the re-check
/// called the first a size-only commit and the second a strong one. A row
/// with no class (verified by a release before #664, or a class this build
/// does not know) is counted by the re-check, as before.
pub(crate) fn counts_as_size_only(rec: &BatchRecord, recheck: Recheck) -> bool {
    match rec.verification_class {
        Some(VerificationClass::SizeOnly) => true,
        Some(VerificationClass::Strong) => false,
        None => recheck == Recheck::SizeOnly,
    }
}

/// Recovery committed a row that was verified size-only (see
/// [`counts_as_size_only`]): count it in
/// `vtop_verification_backend_limited_total` with the running engine's labels,
/// beside the `verified_total` recovery counts for the same commit. `rec`
/// must be the row as the ledger holds it, class included.
pub(crate) fn count_size_only_commit(recheck: Recheck, rec: &BatchRecord) {
    if !counts_as_size_only(rec, recheck) {
        return;
    }
    tracing::warn!(
        batch_id = %rec.batch_id,
        verification_class = class_name(rec),
        recheck = recheck.as_str(),
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
