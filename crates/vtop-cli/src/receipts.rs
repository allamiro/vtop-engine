//! Writing verification receipts (#671 release B).
//!
//! With `manifest.verification_receipts: true` the engine writes
//! `<batch_id>.receipt.json` beside a batch's manifest once the batch is
//! VERIFIED (D5): what the ledger recorded about its verification, in the
//! archive, signed like a 0.4 manifest (`vtop_core::manifest::receipt`).
//!
//! A receipt is evidence about verification, never a precondition for the
//! commit (D6): one that cannot be written is a rate-limited WARN and a
//! `failed` count, and the batch commits regardless. Recovery writes the
//! receipt of a VERIFIED row that has none (D8), after looking first, so it
//! never replaces one: under the Object Lock profile (#684) a second write is
//! a second version, and the first one is the evidence. The scrub only
//! reports a missing receipt; it writes nothing.
//!
//! The receipt goes through `put_manifest`, so it gets the manifest's
//! deadlines, egress shaping, checksum header and, under Object Lock, its
//! retention, which is read back here the way the pipeline reads back a
//! manifest's.

use std::path::Path;

use vtop_core::config::VtopConfig;
use vtop_core::manifest::{ManifestKeyring, ReceiptBuilder, RECEIPT_METHOD_UNSPECIFIED};
use vtop_core::partitioning::receipt_uri;
use vtop_core::telemetry;
use vtop_core::types::ChecksumAlgorithm;
use vtop_state::VerificationClass;
use vtop_upload::multipart::redact_urls;
use vtop_upload::{ObjectChecksum, UploadBackend, VerificationMethod};

use crate::engine::KeyNotices;

/// What a receipt says about one batch.
pub(crate) struct Facts<'a> {
    pub batch_id: &'a str,
    pub tenant: &'a str,
    pub manifest_uri: &'a str,
    pub manifest_sha256: &'a str,
    pub manifest_version_id: Option<&'a str>,
    pub object_uri: &'a str,
    pub object_size_bytes: u64,
    pub checksum_algorithm: &'a str,
    pub object_checksum: &'a str,
    pub object_version_id: Option<&'a str>,
    pub class: VerificationClass,
    /// The check the backend ran on the object; `None` is written as
    /// `unspecified`, never guessed.
    pub method: Option<VerificationMethod>,
    pub origin: &'static str,
}

/// Count one receipt by its bounded result.
pub(crate) fn count(tenant: &str, result: &str) {
    debug_assert!(
        telemetry::RECEIPT_WRITE_RESULTS.contains(&result),
        "{result} is not a pinned result"
    );
    if let Some(mx) = telemetry::metrics() {
        mx.verification_receipts_total
            .with_label_values(&[tenant, result])
            .inc();
    }
}

/// An error as it may appear in a receipt warning: URLs without their query
/// strings or user info, which could carry a signed request's credentials.
fn redacted(error: &impl std::fmt::Display) -> String {
    redact_urls(&error.to_string())
}

/// Build the receipt, stage it in the work directory, store it beside the
/// manifest and, under the Object Lock profile, read its retention back.
/// `Err` is why not, already redacted.
pub(crate) async fn write(
    backend: &dyn UploadBackend,
    keyring: &ManifestKeyring,
    config: &VtopConfig,
    facts: &Facts<'_>,
) -> Result<String, String> {
    let uri = receipt_uri(facts.manifest_uri).ok_or_else(|| {
        "the manifest's key does not end in .manifest.json, so there is no key beside it for \
         the receipt"
            .to_owned()
    })?;
    let written = ReceiptBuilder {
        batch_id: facts.batch_id.to_owned(),
        manifest_uri: facts.manifest_uri.to_owned(),
        manifest_sha256: facts.manifest_sha256.to_owned(),
        manifest_version_id: facts.manifest_version_id.map(str::to_owned),
        object_uri: facts.object_uri.to_owned(),
        object_size_bytes: facts.object_size_bytes,
        object_checksum_algorithm: facts.checksum_algorithm.to_owned(),
        object_checksum: facts.object_checksum.to_owned(),
        object_version_id: facts.object_version_id.map(str::to_owned),
        class: facts.class.as_str().to_owned(),
        method: facts
            .method
            .map_or(RECEIPT_METHOD_UNSPECIFIED, VerificationMethod::as_str)
            .to_owned(),
        verified_at: chrono::Utc::now().to_rfc3339(),
        verified_by: config.engine.name.clone(),
        origin: facts.origin.to_owned(),
    }
    .build_signed(keyring)
    .map_err(|e| redacted(&e))?;
    let path = written
        .write_to_file(Path::new(&config.engine.work_dir))
        .map_err(|e| format!("it could not be staged in the work directory ({e})"))?;
    let stored = store(backend, config, facts, &uri, &path).await;
    // Staged only for the upload; removed whatever became of it.
    let _ = std::fs::remove_file(&path);
    stored.map(|()| uri)
}

async fn store(
    backend: &dyn UploadBackend,
    config: &VtopConfig,
    facts: &Facts<'_>,
    uri: &str,
    path: &Path,
) -> Result<(), String> {
    // The storage-integrity header, by the object's algorithm, as the
    // manifest's is by the batch's.
    let algorithm = facts
        .checksum_algorithm
        .parse::<ChecksumAlgorithm>()
        .unwrap_or(ChecksumAlgorithm::Sha256);
    let digest = vtop_core::checksum::digest_file(algorithm, path)
        .await
        .map_err(|e| format!("it could not be hashed ({e})"))?;
    let checksum = digest
        .as_deref()
        .map(|hex| ObjectChecksum::new(algorithm.as_str(), hex));
    let stored = backend
        .put_manifest(path, uri, checksum)
        .await
        .map_err(|e| redacted(&e))?;
    let Some(lock) = &config.upload.object_lock else {
        return Ok(());
    };
    // Checked like the manifest beside it (#684): the version written, the
    // retention asked for.
    let Some(expected) = stored.retention.map(|r| r.expectation()) else {
        crate::object_lock::count(facts.tenant, "error");
        return Err(format!(
            "it was written without the retention upload.object_lock sets (mode {})",
            lock.mode
        ));
    };
    match backend
        .verify_retention(uri, stored.version_id.as_deref(), &expected)
        .await
    {
        Ok(found) => {
            crate::object_lock::count(facts.tenant, found.label());
            if found.passed() {
                Ok(())
            } else {
                Err(format!(
                    "it is stored, but not under the retention asked for ({})",
                    found.describe(&expected)
                ))
            }
        }
        Err(e) => {
            crate::object_lock::count(facts.tenant, "error");
            Err(format!(
                "it is stored, and its retention could not be read ({})",
                redacted(&e)
            ))
        }
    }
}

/// The engine's write, after the batch reached VERIFIED (D5): counted, and a
/// failure logged (rate-limited per source) and otherwise ignored (D6).
pub(crate) async fn write_after_verify(
    backend: &dyn UploadBackend,
    keyring: &ManifestKeyring,
    config: &VtopConfig,
    notices: &KeyNotices,
    source_name: &str,
    facts: &Facts<'_>,
) {
    match write(backend, keyring, config, facts).await {
        Ok(uri) => {
            count(facts.tenant, "written");
            tracing::info!(
                batch_id = facts.batch_id,
                uri = %uri,
                method = facts.method.map_or(RECEIPT_METHOD_UNSPECIFIED, VerificationMethod::as_str),
                verification_class = facts.class.as_str(),
                "verification_receipt_written"
            );
        }
        Err(why) => {
            count(facts.tenant, "failed");
            if let Some(suppressed) = notices.admit("receipt_failed", source_name) {
                tracing::warn!(
                    event = "verification_receipt_failed",
                    batch_id = facts.batch_id,
                    tenant = facts.tenant,
                    suppressed,
                    "{}",
                    engine_failure_message(facts.batch_id, &why)
                );
            }
        }
    }
}

/// What happened, what the engine did, what to do.
pub(crate) fn engine_failure_message(batch_id: &str, why: &str) -> String {
    format!(
        "the verification receipt for batch {batch_id} could not be written: {why}; the batch \
         is verified and is committed regardless; batches are unaffected. recovery writes the \
         receipt if the batch is still VERIFIED at the next start, and once it is committed \
         vtopctl scrub reports it missing. Nothing to do unless it repeats; then check the \
         store's health and the engine's permission to write beside the manifests"
    )
}

/// What recovery's repair did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Repair {
    /// The receipt was missing and is now written.
    Written,
    /// A receipt is already there; it is left as it is.
    AlreadyThere,
}

/// Recovery's write (D8), for a VERIFIED row its re-check passed: only when
/// the store says no receipt is there. A HEAD that cannot tell writes
/// nothing, so an existing receipt is never replaced.
pub(crate) async fn repair(
    backend: &dyn UploadBackend,
    keyring: &ManifestKeyring,
    config: &VtopConfig,
    facts: &Facts<'_>,
) -> Result<Repair, String> {
    let uri = receipt_uri(facts.manifest_uri).ok_or_else(|| {
        "the manifest's key does not end in .manifest.json, so there is no key beside it for \
         the receipt"
            .to_owned()
    })?;
    match backend.head_object(&uri).await {
        Ok(_) => return Ok(Repair::AlreadyThere),
        Err(e) if crate::scrub::looks_missing(&e) => {}
        Err(e) => {
            return Err(format!(
                "the store could not say whether a receipt is already there ({}), so none was \
                 written: an existing one is never replaced",
                redacted(&e)
            ))
        }
    }
    write(backend, keyring, config, facts)
        .await
        .map(|_| Repair::Written)
}

/// Recovery's message for a receipt it could not write.
pub(crate) fn recovery_failure_message(batch_id: &str, why: &str) -> String {
    format!(
        "recovery could not write the missing verification receipt for batch {batch_id}: \
         {why}; the batch is committed regardless; batches are unaffected. vtopctl scrub \
         reports the receipt missing. Nothing to do unless it repeats; then check the store's \
         health and the engine's permission to write beside the manifests"
    )
}
