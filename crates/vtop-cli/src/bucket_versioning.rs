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

/// Who is refusing, so a message names the switch the reader can change and
/// whose credentials to fix (review of #770): the engine reads
/// `upload.require_object_versioning`, `vtopctl tier copy` its
/// `--require-versioning` flag.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Refuser<'a> {
    /// The setting that asks for versioning.
    pub setting: &'a str,
    /// How to turn it off.
    pub opt_out: &'a str,
    /// Whose permissions a failed check is about.
    pub identity: &'a str,
    /// What was done about it.
    pub did: &'a str,
}

impl Refuser<'static> {
    /// The engine at startup.
    pub(crate) const STARTUP: Refuser<'static> = Refuser::engine("the engine did not start");
    /// The engine at a batch's first upload to a bucket.
    pub(crate) const BATCH: Refuser<'static> =
        Refuser::engine("the batch was not uploaded and will be replayed");
    /// `vtopctl tier copy`.
    pub(crate) const TIER_COPY: Refuser<'static> = Refuser {
        setting: "--require-versioning",
        opt_out: "pass --require-versioning false",
        identity: "the credentials vtopctl uses",
        did: "nothing was copied",
    };

    const fn engine(did: &'static str) -> Refuser<'static> {
        Refuser {
            setting: "upload.require_object_versioning",
            opt_out: "set upload.require_object_versioning: false",
            identity: "the engine's identity",
            did,
        }
    }
}

/// A backend that cannot address object versions at all.
pub(crate) fn backend_without_versions(backend: &str, r: Refuser<'_>) -> String {
    let Refuser {
        setting,
        opt_out,
        did,
        ..
    } = r;
    format!(
        "{setting} is set, but the {backend} upload backend cannot address object versions, \
         so no manifest could be pinned; {did}. Use the s3_native backend, or {opt_out}"
    )
}

/// The store's answer for a bucket that does not keep versions (suspended,
/// or never turned on). Some stores always answer this and cannot enable
/// it (Garage's GetBucketVersioning is a stub), so the advice covers them.
pub(crate) fn not_enabled(bucket: &str, r: Refuser<'_>) -> String {
    let Refuser { opt_out, did, .. } = r;
    format!(
        "bucket {bucket} does not have versioning enabled, so the store would not keep the \
         manifest versions the hardened profile pins; {did}. Enable versioning on the bucket \
         (PutBucketVersioning with Status=Enabled; if the store cannot enable it, use a store \
         that keeps object versions), or {opt_out}"
    )
}

/// The store's answer that it does not implement versioning at all.
pub(crate) fn store_unsupported(bucket: &str, r: Refuser<'_>) -> String {
    let Refuser { opt_out, did, .. } = r;
    format!(
        "the store does not implement S3 bucket versioning (it answered GetBucketVersioning \
         for bucket {bucket} with \"not implemented\"); {did}. No bucket setting can change \
         that: use a store that keeps object versions, or {opt_out}"
    )
}

/// A check that could not be made.
pub(crate) fn unreadable(bucket: &str, r: Refuser<'_>, error: &VtopError) -> String {
    let Refuser { identity, did, .. } = r;
    format!(
        "the versioning of bucket {bucket} could not be read ({}); {did}. Grant {identity} \
         s3:GetBucketVersioning on the bucket, and check the store's health if it has it",
        redact_urls(&error.to_string())
    )
}

/// The refusal for a definite answer, or `None` when the bucket keeps
/// versions.
pub(crate) fn refusal(answer: BucketFeature, bucket: &str, r: Refuser<'_>) -> Option<String> {
    match answer {
        BucketFeature::Enabled => None,
        BucketFeature::NotEnabled => Some(not_enabled(bucket, r)),
        BucketFeature::NotSupported => Some(store_unsupported(bucket, r)),
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
            Refuser::STARTUP,
        )));
    }
    // With create_bucket the bucket may not exist yet: the pipeline creates
    // it before its first upload, and checks it then (review of #770; a
    // startup check would only report the missing bucket as unreadable).
    if config.upload.create_bucket {
        tracing::info!(
            bucket = %config.upload.bucket,
            "upload.require_object_versioning is set with upload.create_bucket; the bucket is \
             checked for versioning at its first upload, once it exists"
        );
        return Ok(());
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
            if let Some(message) = refusal(answer, &bucket, Refuser::STARTUP) {
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
                    Refuser {
                        did: "the engine started anyway, and the bucket is checked again \
                              before the first upload to it",
                        ..Refuser::STARTUP
                    },
                    &e,
                )
            );
        }
    }
    Ok(())
}
