//! The hardened manifest profile's bucket check (`upload.require_object_versioning`,
//! #135): what the engine says when a bucket cannot keep the manifest
//! versions it pins, at startup for a literal bucket and at the first upload
//! for a templated one (#722).

use std::collections::HashSet;
use std::sync::Mutex;

use vtop_core::config::VtopConfig;
use vtop_core::errors::VtopError;
use vtop_upload::multipart::redact_urls;
use vtop_upload::{BucketFeature, UploadBackend};

use crate::object_lock::bucket_is_templated;

/// What the engine did, for a refusal at startup.
pub(crate) const DID_NOT_START: &str = "the engine did not start";

/// What the engine did, for a refusal at a batch.
pub(crate) const DID_NOT_UPLOAD: &str = "the batch was not uploaded and will be replayed";

/// A backend that cannot address object versions at all.
pub(crate) fn backend_without_versions(backend: &str, did: &str) -> String {
    format!(
        "upload.require_object_versioning is set, but the {backend} upload backend cannot \
         address object versions, so no manifest could be pinned; {did}. Use the s3_native \
         backend, or set upload.require_object_versioning: false"
    )
}

/// The store's answer for a bucket that does not keep versions (suspended,
/// or never turned on).
pub(crate) fn not_enabled(bucket: &str, did: &str) -> String {
    format!(
        "bucket {bucket} does not have versioning enabled, so the store would not keep the \
         manifest versions the hardened profile pins; {did}. Enable versioning on the bucket \
         (PutBucketVersioning with Status=Enabled), or set \
         upload.require_object_versioning: false"
    )
}

/// The store's answer that it does not implement versioning at all.
pub(crate) fn store_unsupported(bucket: &str, did: &str) -> String {
    format!(
        "the store does not implement S3 bucket versioning (it answered GetBucketVersioning \
         for bucket {bucket} with \"not implemented\"); {did}. No bucket setting can change \
         that: use a store that keeps object versions, or set \
         upload.require_object_versioning: false"
    )
}

/// A check that could not be made.
pub(crate) fn unreadable(bucket: &str, did: &str, error: &VtopError) -> String {
    format!(
        "the versioning of bucket {bucket} could not be read ({}); {did}. Grant the engine's \
         identity s3:GetBucketVersioning on the bucket, and check the store's health if it has \
         it",
        redact_urls(&error.to_string())
    )
}

/// The refusal for a definite answer, or `None` when the bucket keeps
/// versions.
pub(crate) fn refusal(answer: BucketFeature, bucket: &str, did: &str) -> Option<String> {
    match answer {
        BucketFeature::Enabled => None,
        BucketFeature::NotEnabled => Some(not_enabled(bucket, did)),
        BucketFeature::NotSupported => Some(store_unsupported(bucket, did)),
    }
}

/// The startup check (#722): with `upload.require_object_versioning` set,
/// the backend must address object versions and a literal bucket must have
/// versioning enabled. A templated bucket is checked at the first upload to
/// each bucket instead, and a check the store could not answer is left to
/// that first upload too: it proves nothing either way, and is no reason to
/// stay down. Skips everything when the profile is unset.
pub(crate) async fn preflight(
    config: &VtopConfig,
    backend: &dyn UploadBackend,
    versioned_buckets: &Mutex<HashSet<String>>,
) -> Result<(), VtopError> {
    if !config.upload.require_object_versioning {
        return Ok(());
    }
    if !backend.supports_object_versions() {
        return Err(VtopError::Config(backend_without_versions(
            backend.backend_name(),
            DID_NOT_START,
        )));
    }
    if bucket_is_templated(&config.upload.bucket) {
        tracing::info!(
            bucket = %config.upload.bucket,
            "upload.require_object_versioning is set; each bucket the template names is checked \
             for versioning at its first upload"
        );
        return Ok(());
    }
    // The name batches are written to, as the Object Lock check uses.
    let bucket = vtop_core::partitioning::sanitize_bucket(&config.upload.bucket);
    match backend.bucket_versioning(&bucket).await {
        Ok(answer) => {
            if let Some(message) = refusal(answer, &bucket, DID_NOT_START) {
                return Err(VtopError::Config(message));
            }
            versioned_buckets.lock().unwrap().insert(bucket.clone());
            tracing::info!(
                bucket = %bucket,
                "upload.require_object_versioning is set and bucket {bucket} has versioning \
                 enabled (checked)"
            );
        }
        Err(e) => {
            tracing::warn!(
                bucket = %bucket,
                "{}",
                unreadable(
                    &bucket,
                    "the engine started anyway, and the bucket is checked again before the \
                     first upload to it",
                    &e,
                )
            );
        }
    }
    Ok(())
}
