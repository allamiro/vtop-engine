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
use vtop_core::manifest::ManifestWriteVersion;
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
    tracing::info!(
        manifest_version = config.manifest.write_version.as_str(),
        "{}",
        manifest_version_notice(config)
    );
    // The manifest keys by id (#685): which signs, which only verify, which
    // are retired. Ids and source kinds only, never a value. Until the audit
    // stream (#694) exists, this line is the record of an active-key change.
    tracing::info!(
        active_key_id = config
            .manifest
            .mac
            .as_ref()
            .map(|mac| mac.active.as_str())
            .unwrap_or(""),
        "{}",
        config.manifest_key_summary()
    );
    if let Some(mx) = telemetry::metrics() {
        for (key_id, result) in primed_key_series(config) {
            mx.manifest_authentications_total
                .with_label_values(&[key_id.as_str(), result])
                .inc_by(0);
        }
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

/// The startup line naming the manifest version new batches are written in,
/// and why (#671 release B): what it adds, who can read it, and how to go
/// back. `0.4` is never the default because a reader before v0.8.0 reports
/// it as tampered, and the engine cannot see which releases read the
/// archive, so the setting is the operator's acknowledgement.
pub(crate) fn manifest_version_notice(config: &VtopConfig) -> String {
    match config.manifest.write_version {
        ManifestWriteVersion::V0_3 => {
            let origin = if config.configured_fields.contains("manifest.write_version") {
                "set by manifest.write_version"
            } else {
                "the default"
            };
            format!(
                "manifest.write_version = \"0.3\" ({origin}): new manifests are readable by every \
                 release and carry no verification_plan. Set \"0.4\" to record the verification \
                 plan, once every engine, scrub and auditor of this archive runs v0.8.0 or later"
            )
        }
        ManifestWriteVersion::V0_4 => "manifest.write_version = \"0.4\": new manifests carry \
             verification_plan; every engine, scrub and auditor of this archive must run v0.8.0 \
             or later (older readers report these manifests as tampered). Set \"0.3\" to go back; \
             0.4 manifests already written stay readable"
            .to_owned(),
    }
}

/// The `vtop_manifest_authentications_total` series created at zero at
/// startup (review of #703): a series born at 1 shows no `increase()`, so
/// without them an alert misses the first unknown-key check. Each key's
/// `ok` and `failed`, each tombstone's `retired_key`, and `unknown_key` for
/// `other` (and for `none` with a keyring: a manifest with no id and no
/// legacy key). Configured ids only, so the set is bounded by the config.
pub(crate) fn primed_key_series(config: &VtopConfig) -> Vec<(String, &'static str)> {
    use vtop_core::manifest::{KEY_LABEL_NONE, KEY_LABEL_OTHER};
    let mut series = Vec::new();
    if let Some(mac) = &config.manifest.mac {
        for key in &mac.keys {
            series.push((key.id.clone(), "ok"));
            series.push((key.id.clone(), "failed"));
        }
        for id in &mac.retired {
            series.push((id.clone(), "retired_key"));
        }
        series.push((KEY_LABEL_NONE.to_owned(), "unknown_key"));
    } else if config.manifest_mac_key_env.is_some() {
        series.push((KEY_LABEL_NONE.to_owned(), "ok"));
        series.push((KEY_LABEL_NONE.to_owned(), "failed"));
    } else {
        return series;
    }
    series.push((KEY_LABEL_OTHER.to_owned(), "unknown_key"));
    series
}

/// Recovery's message for a VERIFIED row whose manifest names a key it
/// cannot use (#685): what happened, that the range is replayed, and the
/// remedy for that case. `None` for an ordinary MAC failure, which keeps the
/// generic re-check message.
pub(crate) fn key_recheck_notice(
    key: &vtop_core::manifest::KeyOutcome,
) -> Option<(tracing::Level, &'static str)> {
    use vtop_core::manifest::{KeyResult, KEY_LABEL_NONE};
    match key.result {
        KeyResult::UnknownKey if key.key_id == KEY_LABEL_NONE => Some((
            tracing::Level::ERROR,
            "recovery: a VERIFIED batch's manifest names no key id and no key in \
             manifest.mac.keys is marked legacy: true, so it is unauthenticated (not corrupt); \
             the range is replayed instead of committed (duplicates, never a gap). Mark the key \
             that signed manifests before key ids (the old manifest_mac_key_env key) \
             legacy: true on every engine before the next restart",
        )),
        KeyResult::UnknownKey => Some((
            tracing::Level::ERROR,
            "recovery: a VERIFIED batch's manifest is signed with a key this engine does not \
             hold, so it is unauthenticated (not corrupt); the range is replayed instead of \
             committed (duplicates, never a gap). Add the key to manifest.mac.keys as a \
             verify-only entry on every engine before the next restart",
        )),
        KeyResult::RetiredKey => Some((
            tracing::Level::WARN,
            "recovery: a VERIFIED batch's manifest is signed with a retired key \
             (manifest.mac.retired), so it is unauthenticated; the range is replayed instead of \
             committed and archived again under the active key. Expected only if that key was \
             withdrawn",
        )),
        KeyResult::Ok | KeyResult::Failed => None,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config(manifest: &str) -> VtopConfig {
        VtopConfig::from_yaml_str(&format!(
            "engine: {{name: t, state_store: 'sqlite::memory:', work_dir: /unused}}\n\
             batching: {{}}\ncompression: {{}}\nsources: {{}}\nupload: {{bucket: b}}\n{manifest}"
        ))
        .unwrap()
    }

    fn keyring_config() -> VtopConfig {
        config(
            "manifest:\n  write_version: \"0.4\"\n  mac:\n    active: q4\n    keys:\n      \
             - {id: q3, env: K3, legacy: true}\n      - {id: q4, file: /k4}\n    retired: [q1]\n",
        )
    }

    /// #685, review of #703: recovery's message for a key it cannot use says
    /// what to do for THAT case. A manifest naming no id where no key is
    /// legacy needs a key marked legacy, not a new verify-only entry.
    #[test]
    fn the_recovery_notice_for_a_key_names_the_right_remedy() {
        use vtop_core::manifest::{KeyOutcome, KeyResult};
        let notice = |key_id: &str, result| {
            key_recheck_notice(&KeyOutcome {
                key_id: key_id.into(),
                result,
            })
        };
        let (level, text) = notice("other", KeyResult::UnknownKey).unwrap();
        assert_eq!(level, tracing::Level::ERROR);
        for needle in [
            "does not hold",
            "unauthenticated (not corrupt)",
            "replayed instead of committed",
            "as a verify-only entry",
        ] {
            assert!(text.contains(needle), "{needle}: {text}");
        }
        let (level, text) = notice("none", KeyResult::UnknownKey).unwrap();
        assert_eq!(level, tracing::Level::ERROR);
        assert!(text.contains("names no key id"), "{text}");
        assert!(text.contains("legacy: true"), "{text}");
        assert!(!text.contains("verify-only entry"), "{text}");
        let (level, text) = notice("q1", KeyResult::RetiredKey).unwrap();
        assert_eq!(level, tracing::Level::WARN);
        assert!(
            text.contains("retired key") && text.contains("withdrawn"),
            "{text}"
        );
        assert!(notice("q4", KeyResult::Ok).is_none());
        assert!(notice("q4", KeyResult::Failed).is_none());
    }

    /// Review of #703: the per-key series exist at zero from startup, so an
    /// alert on `increase()` sees the FIRST unknown-key check (a series born
    /// at 1 shows no increase). Configured ids, `none` and `other` only.
    #[test]
    fn the_key_series_an_alert_reads_are_primed_at_startup() {
        let mut primed = primed_key_series(&keyring_config());
        primed.sort();
        assert_eq!(
            primed,
            [
                ("none".to_string(), "unknown_key"),
                ("other".to_string(), "unknown_key"),
                ("q1".to_string(), "retired_key"),
                ("q3".to_string(), "failed"),
                ("q3".to_string(), "ok"),
                ("q4".to_string(), "failed"),
                ("q4".to_string(), "ok"),
            ]
        );
        let mut shorthand = primed_key_series(&config("manifest_mac_key_env: K0\n"));
        shorthand.sort();
        assert_eq!(
            shorthand,
            [
                ("none".to_string(), "failed"),
                ("none".to_string(), "ok"),
                ("other".to_string(), "unknown_key"),
            ]
        );
        assert!(
            primed_key_series(&config("")).is_empty(),
            "no key, no series"
        );
    }

    /// #671 release B: the startup line names the version new manifests get
    /// and why: where 0.3 came from and how to opt in, or, for 0.4, the
    /// minimum reader release, what older readers do, and how to go back.
    #[test]
    fn the_startup_notice_names_the_manifest_version_and_why() {
        let default = manifest_version_notice(&config(""));
        assert!(default.contains("\"0.3\" (the default)"), "{default}");
        assert!(default.contains("v0.8.0"), "{default}");
        let explicit = manifest_version_notice(&config("manifest:\n  write_version: \"0.3\"\n"));
        assert!(
            explicit.contains("set by manifest.write_version"),
            "{explicit}"
        );
        let opted_in = manifest_version_notice(&config("manifest:\n  write_version: \"0.4\"\n"));
        for needle in [
            "\"0.4\"",
            "verification_plan",
            "v0.8.0 or later",
            "tampered",
            "Set \"0.3\" to go back",
        ] {
            assert!(opted_in.contains(needle), "{needle}: {opted_in}");
        }
    }
}
