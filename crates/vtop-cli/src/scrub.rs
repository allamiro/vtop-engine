//! Re-verify committed archive objects: `vtopctl scrub` (#628).
//!
//! Once a batch is SOURCE_COMMITTED nothing read its object again. Bit rot, an
//! overwrite with same-size bytes, a lifecycle rule that deleted the object,
//! or a manifest rewritten in place went unnoticed until someone fetched the
//! data, by which time the source had usually aged out and the batch could no
//! longer be archived again. `audit-coverage --check-storage` HEADs each
//! object, which sees presence and size and nothing else; recovery's storage
//! re-check reads content, but only for VERIFIED rows not yet committed, and
//! only at startup.
//!
//! The scrub reads committed rows from the ledger and from the digests
//! `prune-ledger` wrote for the rows it deleted, and for each one runs the
//! check recovery runs ([`check_stored_batch`], shared with the engine):
//! download the manifest at its pinned version, authenticate it (self-hash,
//! and the keyed MAC when configured), bind it to the ledger row, then have
//! the backend verify the object's stored content against the manifest's
//! digest. On top of that it reads the manifest's CURRENT key, because a
//! reader fetching the archive by key reads that, not the pinned version.
//!
//! A finding can only be reported. The scrub repairs nothing: the engine
//! cannot re-archive a batch whose source no longer holds the data.
//!
//! It is bounded so it can run against a large archive beside a live engine:
//! a concurrency limit, an object rate and a byte rate, and either a cursor
//! file with a per-run limit (a rotating schedule that covers everything once
//! per pass) or a random sample.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing::Instrument;
use vtop_core::errors::VtopError;
use vtop_core::manifest::{
    KeyOutcome, KeyResult, ManifestKeyring, ObjectMetadata, ParsedManifest, ParsedReceipt,
    MAX_MANIFEST_BYTES, MAX_RECEIPT_BYTES,
};
use vtop_core::state_machine::BatchState;
use vtop_core::types::ChecksumAlgorithm;
use vtop_state::BatchRecord;
use vtop_upload::{
    ObjectChecksum, ObjectLockMode, RetentionCheck, RetentionExpectation, UploadBackend,
    VerificationMethod,
};

/// What a check found. The `result` label of `vtop_scrub_objects_total`: a
/// small closed set, never a batch id or a URI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScrubResult {
    /// The stored object's content matches the authenticated manifest, and
    /// the manifest matches the ledger.
    Ok,
    /// Size-only: the backend passed the object on size and existence but
    /// compared no digest (`backend_limited`). That happens when the batch
    /// was archived with `checksum.algorithm: none`, or on a backend that
    /// cannot hash. It is the same result recovery's re-check calls
    /// size-only, decides under `upload.require_strong_verification` and
    /// counts in `vtop_verification_backend_limited_total` (#629, #651).
    /// Counted here, not a finding: the archive was committed that way by
    /// configuration. A same-size replacement cannot be told apart from it,
    /// which is the risk that opt-out accepts.
    Limited,
    /// The object or the manifest (its pinned version, or its current key) is
    /// gone.
    Missing,
    /// Something is stored, and it is not what was committed: the object's
    /// content or size, the manifest's current key, or the ledger's record of
    /// the object disagree.
    Mismatched,
    /// The manifest does not parse, fails its self-hash or MAC, or does not
    /// bind to the ledger row (a different batch's manifest, or a forgery
    /// whose digest is not the one the ledger recorded). Also a manifest
    /// signed with a key this process does not hold or that is retired
    /// (#685): unauthenticated, not corrupt; the detail names the key id and
    /// [`BatchCheck::key`] says which case it is.
    Unauthenticated,
    /// The store could not be read for a reason other than absence (a
    /// timeout, a throttle, a permission error). Nothing was proven either
    /// way, so it is a finding: a scrub that could not look did not pass.
    Unreadable,
    /// The manifest is from a newer release than this build reads (#671),
    /// proven to be the writer's: the row's pinned version, carrying the
    /// batch id and self-hash the ledger recorded. Not checked, so it is a
    /// finding: upgrade the scrub's binary and run it again. The same claim
    /// without that proof is [`ScrubResult::Unauthenticated`].
    UnsupportedVersion,
}

impl ScrubResult {
    pub const ALL: [ScrubResult; 7] = [
        ScrubResult::Ok,
        ScrubResult::Limited,
        ScrubResult::Missing,
        ScrubResult::Mismatched,
        ScrubResult::Unauthenticated,
        ScrubResult::Unreadable,
        ScrubResult::UnsupportedVersion,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ScrubResult::Ok => "ok",
            ScrubResult::Limited => "limited",
            ScrubResult::Missing => "missing",
            ScrubResult::Mismatched => "mismatched",
            ScrubResult::Unauthenticated => "unauthenticated",
            ScrubResult::Unreadable => "unreadable",
            ScrubResult::UnsupportedVersion => "unsupported_version",
        }
    }

    /// Whether this result fails the scrub.
    pub fn is_finding(self) -> bool {
        !matches!(self, ScrubResult::Ok | ScrubResult::Limited)
    }
}

/// The outcome of checking one batch against the store.
#[derive(Debug, Clone)]
pub struct BatchCheck {
    pub result: ScrubResult,
    /// What was found, naming the URI it concerns.
    pub detail: String,
    /// The manifest MAC check (#685): the key used, by its bounded label,
    /// and what it found. `None` when no key is configured or the check did
    /// not get that far.
    pub key: Option<KeyOutcome>,
    /// The check the backend ran on the object (#671 release B, D9), when
    /// the object was verified (`ok` or `limited`): what a receipt recovery
    /// writes names.
    pub method: Option<VerificationMethod>,
    /// The object as the authenticated manifest describes it, when the
    /// object was verified against it. Boxed: a `BatchCheck` is also an
    /// error type.
    pub object: Option<Box<ObjectMetadata>>,
}

impl BatchCheck {
    fn new(result: ScrubResult, detail: impl Into<String>) -> Self {
        Self {
            result,
            detail: detail.into(),
            key: None,
            method: None,
            object: None,
        }
    }

    /// A check whose object was verified, recording how and what.
    fn verified(
        result: ScrubResult,
        detail: impl Into<String>,
        method: Option<VerificationMethod>,
        object: &ObjectMetadata,
    ) -> Self {
        Self {
            method,
            object: Some(Box::new(object.clone())),
            ..Self::new(result, detail)
        }
    }
}

/// Whether a read error says the thing is ABSENT, as opposed to unreadable.
///
/// The mock and `localfs` say so with [`VtopError::NotFound`], and so does the
/// S3 backend for a 404 HEAD (#626). Its other service errors are
/// [`VtopError::Upload`] with the status and code in the message
/// (`http 404`, `NoSuchKey`, `NoSuchVersion`), and the
/// command backends relay the tool's own words; those are matched as text.
/// A miss here only turns `missing` into `unreadable`; both are findings.
pub fn looks_missing(error: &VtopError) -> bool {
    match error {
        VtopError::NotFound(_) => true,
        VtopError::Io(io) => io.kind() == std::io::ErrorKind::NotFound,
        VtopError::Upload(message) | VtopError::Other(message) => {
            let message = message.to_ascii_lowercase();
            [
                "http 404",
                "nosuchkey",
                "nosuchversion",
                "(404)",
                "404 not found",
                "not found (404)",
                "object does not exist",
                "key does not exist",
            ]
            .iter()
            .any(|needle| message.contains(needle))
        }
        _ => false,
    }
}

fn read_failure(what: &str, uri: &str, error: &VtopError) -> BatchCheck {
    if looks_missing(error) {
        BatchCheck::new(
            ScrubResult::Missing,
            format!("{what} {uri} is not in the store: {error}"),
        )
    } else {
        BatchCheck::new(
            ScrubResult::Unreadable,
            format!("{what} {uri} could not be read: {error}"),
        )
    }
}

/// Check one batch against the store: the manifest at its pinned version
/// (the current key only when the row pinned none), authenticated and bound
/// to the ledger row, then the object's stored content verified by the
/// backend against the manifest's digest.
///
/// This is the check recovery runs before committing a VERIFIED row (#64,
/// #69, #135) and the one the scrub runs on committed rows, so the two
/// cannot drift. It never trusts HEAD metadata or uploader-written sidecars
/// for a strong result: `verify_object` derives one from the stored body or a
/// checksum the storage service computed over it.
pub async fn check_stored_batch(
    backend: &dyn UploadBackend,
    keyring: &ManifestKeyring,
    rec: &BatchRecord,
) -> BatchCheck {
    check_with_manifest(backend, keyring, rec).await.0
}

/// Whether a manifest claiming a version this build cannot authenticate is
/// proven to be the one the batch's writer stored, so the claim may be acted
/// on (#671, review of #674). Two things together:
///
/// - the bytes are the row's PINNED version (#135): an immutable version the
///   writer recorded at upload, which an overwrite of the key cannot change.
///   The ledger's `manifest_sha256` is the writer's self-hash, computed by a
///   scheme this build cannot reproduce, so it alone binds nothing: a
///   tamperer can copy it into a forgery at the current key;
/// - and the document carries the batch id and the self-hash the ledger
///   recorded, so the pinned version is this batch's.
///
/// Without a pin there is no such proof, and the manifest is treated as
/// unauthenticated, as every release before #671 treated it.
fn newer_manifest_is_the_fleets(rec: &BatchRecord, bytes: &[u8]) -> Result<(), String> {
    let Some(version) = rec.manifest_version_id.as_deref() else {
        return Err("the row pins no manifest version, so these bytes are not proven to be what its writer stored".into());
    };
    let claimed = vtop_core::manifest::claimed_binding(bytes);
    let bound = claimed.as_ref().is_some_and(|(batch_id, sha256)| {
        batch_id == &rec.batch_id
            && rec
                .manifest_sha256
                .as_deref()
                .is_some_and(|want| !want.is_empty() && want.eq_ignore_ascii_case(sha256))
    });
    if bound {
        Ok(())
    } else {
        Err(format!(
            "its pinned version {version} does not carry the batch id and self-hash the ledger recorded"
        ))
    }
}

/// [`check_stored_batch`], also returning the pinned manifest's bytes when
/// they were read, so the scrub can compare the current key against them.
async fn check_with_manifest(
    backend: &dyn UploadBackend,
    keyring: &ManifestKeyring,
    rec: &BatchRecord,
) -> (BatchCheck, Option<Vec<u8>>) {
    let Some(uri) = rec.object_uri.as_deref() else {
        // Corrupt bookkeeping: nothing names what was archived.
        return (
            BatchCheck::new(ScrubResult::Mismatched, "the ledger row names no object"),
            None,
        );
    };
    let Some(muri) = rec.manifest_uri.as_deref() else {
        return (
            BatchCheck::new(ScrubResult::Mismatched, "the ledger row names no manifest"),
            None,
        );
    };
    // A recorded version pins the read to the immutable manifest this batch
    // verified (#135). A deleted pinned version fails closed here rather than
    // falling back to whatever replaced it.
    let read = match rec.manifest_version_id.as_deref() {
        Some(version) => {
            backend
                .get_manifest_pinned(muri, version, MAX_MANIFEST_BYTES)
                .await
        }
        None => backend.get_object_bounded(muri, MAX_MANIFEST_BYTES).await,
    };
    let bytes = match read {
        Ok(bytes) => bytes,
        Err(e) => {
            let what = match rec.manifest_version_id.as_deref() {
                Some(version) => format!("manifest version {version} of"),
                None => "manifest".to_string(),
            };
            return (read_failure(&what, muri, &e), None);
        }
    };
    let check = check_manifest_and_object(backend, keyring, rec, uri, muri, &bytes).await;
    (check, Some(bytes))
}

async fn check_manifest_and_object(
    backend: &dyn UploadBackend,
    keyring: &ManifestKeyring,
    rec: &BatchRecord,
    uri: &str,
    muri: &str,
    bytes: &[u8],
) -> BatchCheck {
    let unauthenticated = |why: String| BatchCheck::new(ScrubResult::Unauthenticated, why);
    let manifest = match ParsedManifest::from_slice(bytes) {
        Ok(manifest) => manifest,
        Err(e @ VtopError::UnsupportedManifestVersion { .. }) => {
            // The version is read before anything is authenticated, so a
            // tamperer could write `"version": "0.9"` to turn an integrity
            // finding into "upgrade the binary" - and recovery would hold the
            // row VERIFIED for good (review of #674). The claim counts only
            // for bytes proven to be what this fleet wrote.
            let e = e.at_manifest_uri(muri);
            return match newer_manifest_is_the_fleets(rec, bytes) {
                Ok(()) => BatchCheck::new(ScrubResult::UnsupportedVersion, e.to_string()),
                Err(why) => unauthenticated(format!("{e}, and {why}")),
            };
        }
        Err(e) => return unauthenticated(format!("manifest {muri} does not parse: {e}")),
    };
    // With the key the manifest names, never a search (#685), counted by
    // that key's configured id.
    let authentication = manifest.authenticate(keyring);
    authentication.count();
    let key = authentication.key.clone();
    let mut check = match authentication.into_result() {
        Err(e) => unauthenticated(format!("manifest {muri} does not authenticate: {e}")),
        Ok(()) => check_authenticated(backend, rec, uri, muri, &manifest).await,
    };
    check.key = key;
    check
}

/// The rest of [`check_manifest_and_object`], for a manifest that
/// authenticated.
async fn check_authenticated(
    backend: &dyn UploadBackend,
    rec: &BatchRecord,
    uri: &str,
    muri: &str,
    manifest: &ParsedManifest,
) -> BatchCheck {
    let unauthenticated = |why: String| BatchCheck::new(ScrubResult::Unauthenticated, why);
    // Bind the manifest to the durable ledger before trusting it: a validly
    // authenticated manifest of ANOTHER batch, or one whose digest is not the
    // one the ledger recorded, is not this batch's manifest.
    if manifest.fields().batch_id != rec.batch_id {
        return unauthenticated(format!(
            "manifest {muri} is batch {}'s, not {}",
            manifest.fields().batch_id,
            rec.batch_id
        ));
    }
    if manifest.fields().manifest.uri != muri || manifest.fields().object.uri != uri {
        return unauthenticated(format!(
            "manifest {muri} names manifest {} and object {}, the ledger {muri} and {uri}",
            manifest.fields().manifest.uri,
            manifest.fields().object.uri
        ));
    }
    let ledger_manifest_hash_matches = rec.manifest_sha256.as_deref().is_some_and(|want| {
        !want.is_empty() && want.eq_ignore_ascii_case(&manifest.fields().manifest.sha256)
    });
    if !ledger_manifest_hash_matches {
        return unauthenticated(format!(
            "manifest {muri} hashes to {}, the ledger recorded {}",
            manifest.fields().manifest.sha256,
            rec.manifest_sha256.as_deref().unwrap_or("nothing")
        ));
    }
    // The column keeps its historical object_sha256 name but may hold a
    // BLAKE3 digest: it is the configured algorithm's.
    if rec
        .object_size_bytes
        .is_some_and(|want| want < 0 || want as u64 != manifest.fields().object.size_bytes)
        || rec.object_sha256.as_deref().is_some_and(|want| {
            !want.is_empty() && !want.eq_ignore_ascii_case(&manifest.fields().object.checksum)
        })
    {
        return BatchCheck::new(
            ScrubResult::Mismatched,
            format!("manifest {muri} and the ledger disagree on object {uri}'s size or digest"),
        );
    }

    let algo = match manifest
        .fields()
        .object
        .checksum_algorithm
        .parse::<ChecksumAlgorithm>()
    {
        Ok(algo) => algo,
        Err(e) => {
            return BatchCheck::new(
                ScrubResult::Mismatched,
                format!("manifest {muri} names a checksum this build cannot compute: {e}"),
            )
        }
    };
    let expected = if algo.is_enabled() {
        if manifest.fields().object.checksum.is_empty() {
            return BatchCheck::new(
                ScrubResult::Mismatched,
                format!("manifest {muri} declares {algo:?} but records no digest"),
            );
        }
        Some(ObjectChecksum::new(
            algo.as_str(),
            &manifest.fields().object.checksum,
        ))
    } else {
        None
    };
    match backend
        .verify_object(uri, manifest.fields().object.size_bytes, expected)
        .await
    {
        Err(e) => read_failure("object", uri, &e),
        Ok(result) if !result.passed => BatchCheck::new(
            ScrubResult::Mismatched,
            format!("object {uri}: {}", result.message),
        ),
        Ok(result) if result.backend_limited => BatchCheck::verified(
            ScrubResult::Limited,
            format!("object {uri}: {}", result.message),
            result.method,
            &manifest.fields().object,
        ),
        Ok(result) => BatchCheck::verified(
            ScrubResult::Ok,
            result.message,
            result.method,
            &manifest.fields().object,
        ),
    }
}

/// The scrub's check of one committed row: [`check_stored_batch`], then the
/// manifest's current key against the pinned version. A reader fetching the
/// archive by key reads the current key, so a manifest overwritten or deleted
/// there is damage even though the pinned version the ledger trusts survives.
async fn scrub_one(
    backend: &dyn UploadBackend,
    keyring: &ManifestKeyring,
    rec: &BatchRecord,
) -> BatchCheck {
    let (check, pinned) = check_with_manifest(backend, keyring, rec).await;
    if check.result.is_finding() || rec.manifest_version_id.is_none() {
        return check;
    }
    let (Some(pinned), Some(muri)) = (pinned, rec.manifest_uri.as_deref()) else {
        return check;
    };
    match backend.get_object_bounded(muri, MAX_MANIFEST_BYTES).await {
        Ok(current) if current == pinned => check,
        Ok(_) => BatchCheck {
            key: check.key,
            ..BatchCheck::new(
                ScrubResult::Mismatched,
                format!(
                    "manifest {muri}: the current key no longer holds version {} the ledger pinned",
                    rec.manifest_version_id.as_deref().unwrap_or_default()
                ),
            )
        },
        Err(e) => BatchCheck {
            key: check.key,
            ..read_failure("the current key of manifest", muri, &e)
        },
    }
}

/// What `--check-retention` found for one committed batch (#684): its
/// object's and its manifest's S3 Object Lock retention, the worse of the
/// two. Counted in `vtop_scrub_retention_total`, its own family, so the
/// `result` set of `vtop_scrub_objects_total` does not change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionResult {
    /// Under the configured mode, until a date still in the future.
    Locked,
    /// Under the configured mode, until a date that has passed: normal for an
    /// object older than the retention, and not a finding.
    Elapsed,
    /// No retention at all. A finding: the object was written without it, or
    /// a principal allowed to bypass governance removed it.
    Missing,
    /// Another mode than the configured one. A finding.
    ModeMismatch,
}

impl RetentionResult {
    pub const ALL: [RetentionResult; 4] = [
        RetentionResult::Locked,
        RetentionResult::Elapsed,
        RetentionResult::Missing,
        RetentionResult::ModeMismatch,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            RetentionResult::Locked => "locked",
            RetentionResult::Elapsed => "elapsed",
            RetentionResult::Missing => "missing",
            RetentionResult::ModeMismatch => "mode_mismatch",
        }
    }

    pub fn is_finding(self) -> bool {
        matches!(
            self,
            RetentionResult::Missing | RetentionResult::ModeMismatch
        )
    }
}

/// What one retention read found: a judgement, or why there is none.
enum RetentionRead {
    Judged(RetentionResult),
    /// The store answered with an error, already redacted. `absent`: the
    /// error, before redaction, said the key or version is not there.
    Failed {
        error: VtopError,
        absent: bool,
    },
    /// The backend cannot read retention at all.
    Unsupported,
}

/// The retention of `uri` (at `version`, or its current version when
/// `None`), expected under `mode` until a date not yet past (#684).
async fn read_retention(
    backend: &dyn UploadBackend,
    uri: &str,
    version: Option<&str>,
    mode: ObjectLockMode,
) -> RetentionRead {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    let expected = RetentionExpectation {
        mode,
        retain_until_at_least_unix_secs: now,
        legal_hold: false,
    };
    match backend.verify_retention(uri, version, &expected).await {
        Ok(RetentionCheck::Ok { .. } | RetentionCheck::LegalHoldMissing) => {
            RetentionRead::Judged(RetentionResult::Locked)
        }
        Ok(RetentionCheck::TooShort { .. }) => RetentionRead::Judged(RetentionResult::Elapsed),
        Ok(RetentionCheck::Missing) => RetentionRead::Judged(RetentionResult::Missing),
        Ok(RetentionCheck::ModeMismatch { .. }) => {
            RetentionRead::Judged(RetentionResult::ModeMismatch)
        }
        Ok(RetentionCheck::Unsupported) => RetentionRead::Unsupported,
        Err(e) => RetentionRead::Failed {
            absent: looks_missing(&e),
            error: VtopError::Upload(vtop_upload::multipart::redact_urls(&e.to_string())),
        },
    }
}

/// `--check-retention` for one batch (#684): the object at its current key
/// (the bytes the content check just read) and the manifest at its pinned
/// version, each expected under `mode` until a date not yet past; the worse
/// of the two. A read that fails is an `Unreadable` content finding: a
/// scrub that could not look did not pass.
async fn retention_of(
    backend: &dyn UploadBackend,
    rec: &BatchRecord,
    mode: ObjectLockMode,
) -> Result<RetentionResult, BatchCheck> {
    let targets = [
        (rec.object_uri.as_deref(), None),
        (
            rec.manifest_uri.as_deref(),
            rec.manifest_version_id.as_deref(),
        ),
    ];
    let mut worst = RetentionResult::Locked;
    for (uri, version) in targets {
        let Some(uri) = uri else { continue };
        let result = match read_retention(backend, uri, version, mode).await {
            RetentionRead::Judged(result) => result,
            RetentionRead::Failed { error, .. } => {
                return Err(read_failure("the retention of", uri, &error))
            }
            RetentionRead::Unsupported => {
                return Err(BatchCheck::new(
                    ScrubResult::Unreadable,
                    format!(
                        "the retention of {uri} cannot be read by the {} backend",
                        backend.backend_name()
                    ),
                ))
            }
        };
        worst = worst.max(result);
    }
    Ok(worst)
}

/// What a batch's receipt check found (#671 release B), and, with
/// `--check-retention` (#710), the receipt's own retention.
struct ReceiptCheck {
    result: ReceiptResult,
    detail: String,
    /// The receipt's key, when a receipt was read there: the only receipts
    /// whose retention is read. A missing one has none to judge.
    read_at: Option<String>,
    retention: Option<RetentionResult>,
}

/// `--check-retention` for a batch's verification receipt (#710): the
/// receipt at its current key, the one the receipt check just read. Receipts
/// are not pinned in the ledger, and a GET returns no version id here, so
/// there is no version to pin it to. `Ok(None)`: the receipt went between
/// the two reads, which the next run counts as missing. `Err`: why the
/// retention could not be read, for an `unreadable` receipt finding.
async fn receipt_retention_of(
    backend: &dyn UploadBackend,
    receipt_uri: &str,
    mode: ObjectLockMode,
) -> Result<Option<RetentionResult>, String> {
    match read_retention(backend, receipt_uri, None, mode).await {
        RetentionRead::Judged(result) => Ok(Some(result)),
        RetentionRead::Failed { absent: true, .. } => {
            tracing::info!(
                receipt_uri,
                "verification receipt {receipt_uri} was deleted between its read and its \
                 retention read; the scrub judged no retention for it, and the next run \
                 counts it as missing; nothing to do unless it stays missing"
            );
            Ok(None)
        }
        RetentionRead::Failed { error: e, .. } => Err(format!(
            "the retention of verification receipt {receipt_uri} could not be read: {e}; the \
             receipt is reported unreadable and the batch's own result is left alone (nothing \
             was proven about the receipt's lock; the scrub needs s3:GetObjectRetention)"
        )),
        RetentionRead::Unsupported => Err(format!(
            "the retention of verification receipt {receipt_uri} cannot be read by the {} \
             backend; the receipt is reported unreadable and the batch's own result is left \
             alone",
            backend.backend_name()
        )),
    }
}

/// The detail of a receipt retention finding (#710): what happened, what the
/// scrub did, and what to do.
fn receipt_retention_detail(receipt_uri: &str, found: RetentionResult, mode: &str) -> String {
    let (what, remedy) = match found {
        RetentionResult::Missing => (
            "carries no retention",
            "a receipt with no lock can be deleted or overwritten: re-apply the retention with \
             PutObjectRetention on the receipt's current version if it should be kept (it is \
             evidence from the engine, not a commit precondition), and check who could bypass \
             it, or whether an engine older than v0.10.0 wrote it",
        ),
        _ => (
            "is under another retention mode than the configured one",
            "nothing to do if upload.object_lock.mode was changed on purpose (a stricter lock \
             cannot be shortened); otherwise find who wrote or re-locked it in the store's \
             access log",
        ),
    };
    format!(
        "verification receipt {receipt_uri} {what} (S3 Object Lock, expected {mode}); the scrub \
         reported it and changed nothing, and the batch's object and manifest are checked \
         separately and are unaffected by this finding; {remedy}"
    )
}

/// What the receipt check found for one committed batch (#671 release B):
/// the verification receipt beside its manifest. Counted in
/// `vtop_scrub_receipts_total`, its own family, so the `result` set of
/// `vtop_scrub_objects_total` does not change. The scrub only reports: it
/// writes no receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptResult {
    /// It authenticates and binds the batch as the ledger recorded it.
    Ok,
    /// No receipt is stored. Normal for batches archived before
    /// `manifest.verification_receipts` was enabled, so a finding only
    /// under `--require-receipts`.
    Missing,
    /// It does not parse, or fails its self-hash or MAC with a key this scrub
    /// holds (a tampered receipt, or one forged without the key).
    Invalid,
    /// It names a key this scrub does not hold, or a retired one (#685):
    /// not proven bad, and not checkable here.
    Unauthenticated,
    /// It authenticates, and is not this batch's as the ledger recorded it:
    /// another batch, another manifest digest or version, another object, or
    /// another verification class than the ledger's (#664).
    Mismatched,
    /// The store could not be read for a reason other than absence.
    Unreadable,
}

impl ReceiptResult {
    pub const ALL: [ReceiptResult; 6] = [
        ReceiptResult::Ok,
        ReceiptResult::Missing,
        ReceiptResult::Invalid,
        ReceiptResult::Unauthenticated,
        ReceiptResult::Mismatched,
        ReceiptResult::Unreadable,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ReceiptResult::Ok => "ok",
            ReceiptResult::Missing => "missing",
            ReceiptResult::Invalid => "invalid",
            ReceiptResult::Unauthenticated => "unauthenticated",
            ReceiptResult::Mismatched => "mismatched",
            ReceiptResult::Unreadable => "unreadable",
        }
    }

    /// Whether this result fails the scrub: everything but `ok`, and
    /// `missing` only when receipts are required.
    pub fn is_finding(self, required: bool) -> bool {
        match self {
            ReceiptResult::Ok => false,
            ReceiptResult::Missing => required,
            _ => true,
        }
    }
}

/// A string read from a stored receipt, as a finding may quote it: URL query
/// strings and user info stripped, quoted, and cut at 128 characters (review
/// of #711). With no MAC key, anyone who can write the bucket chooses it.
fn shown(stored: &str) -> String {
    let redacted = vtop_upload::multipart::redact_urls(stored);
    if redacted.chars().count() > 128 {
        let cut: String = redacted.chars().take(128).collect();
        format!("{cut:?}...")
    } else {
        format!("{redacted:?}")
    }
}

/// What to do about a receipt finding: the third part of the message. The
/// batch's own object and manifest are checked separately, so a receipt
/// finding is about the evidence, never the data.
fn receipt_remedy(result: ReceiptResult) -> &'static str {
    match result {
        ReceiptResult::Missing => {
            "the batch's data is checked separately; a batch committed without a receipt keeps \
             none, so drop --require-receipts for archives written before receipts were \
             enabled, or find why the engine logged verification_receipt_failed"
        }
        ReceiptResult::Invalid | ReceiptResult::Mismatched => {
            "the receipt is not this batch's as the ledger recorded it: the batch's data is \
             checked separately; keep the receipt as evidence and find who wrote it in the \
             store's access log"
        }
        ReceiptResult::Unauthenticated => {
            "add the key it names to manifest.mac.keys as verify-only, or accept that it was \
             retired"
        }
        ReceiptResult::Unreadable => "check the store's health and the scrub's read permission",
        ReceiptResult::Ok => "",
    }
}

/// [`receipt_of`], and with `retention` (#710) the retention of the receipt
/// it read. A retention that cannot be read turns an `ok` receipt
/// `unreadable` (a scrub that could not look did not pass), and is added to
/// the detail of a receipt that is already a finding.
async fn receipt_check(
    backend: &dyn UploadBackend,
    keyring: &ManifestKeyring,
    rec: &BatchRecord,
    retention: Option<ObjectLockMode>,
) -> ReceiptCheck {
    let (mut result, mut detail, read_at) = receipt_of(backend, keyring, rec).await;
    let mut judged = None;
    if let (Some(mode), Some(ruri)) = (retention, read_at.as_deref()) {
        match receipt_retention_of(backend, ruri, mode).await {
            Ok(found) => judged = found,
            Err(why) if result == ReceiptResult::Ok => {
                result = ReceiptResult::Unreadable;
                detail = why;
            }
            Err(why) => detail = format!("{detail}; {why}"),
        }
    }
    ReceiptCheck {
        result,
        detail,
        read_at,
        retention: judged,
    }
}

/// Check the verification receipt beside `rec`'s manifest: read at its key
/// (receipts are not pinned), authenticated with the key it names (#685),
/// then bound to the ledger row: the batch, the manifest's URI, digest and
/// pinned version, the object, and the class the ledger recorded. The third
/// value is the receipt's key when a receipt was read there.
async fn receipt_of(
    backend: &dyn UploadBackend,
    keyring: &ManifestKeyring,
    rec: &BatchRecord,
) -> (ReceiptResult, String, Option<String>) {
    let mismatched = |why: String| (ReceiptResult::Mismatched, why, None);
    let Some(muri) = rec.manifest_uri.as_deref() else {
        return mismatched("the ledger row names no manifest, so no receipt key".into());
    };
    let Some(ruri) = vtop_core::partitioning::receipt_uri(muri) else {
        return mismatched(format!(
            "manifest {muri} does not end in .manifest.json, so it has no receipt key"
        ));
    };
    // From here on a receipt was read at `ruri`, whatever it turns out to be.
    let read = Some(ruri.clone());
    let mismatched = |why: String| (ReceiptResult::Mismatched, why, read.clone());
    let bytes = match backend.get_object_bounded(&ruri, MAX_RECEIPT_BYTES).await {
        Ok(bytes) => bytes,
        Err(e) if looks_missing(&e) => {
            return (
                ReceiptResult::Missing,
                format!("verification receipt {ruri} is not in the store"),
                None,
            )
        }
        Err(e) => {
            return (
                ReceiptResult::Unreadable,
                format!(
                    "verification receipt {ruri} could not be read: {}",
                    vtop_upload::multipart::redact_urls(&e.to_string())
                ),
                None,
            )
        }
    };
    let receipt = match ParsedReceipt::from_slice(&bytes) {
        Ok(receipt) => receipt,
        Err(e) => {
            return (
                ReceiptResult::Invalid,
                format!("verification receipt {ruri} does not parse: {e}"),
                read,
            )
        }
    };
    let authentication = receipt.authenticate(keyring);
    if let Err(e) = authentication.result {
        let result = match authentication.key.map(|k| k.result) {
            Some(KeyResult::UnknownKey | KeyResult::RetiredKey) => ReceiptResult::Unauthenticated,
            _ => ReceiptResult::Invalid,
        };
        return (
            result,
            format!("verification receipt {ruri} does not authenticate: {e}"),
            read,
        );
    }
    let r = receipt.fields();
    if r.batch_id != rec.batch_id {
        return mismatched(format!(
            "verification receipt {ruri} is batch {}'s, not {}",
            shown(&r.batch_id),
            rec.batch_id
        ));
    }
    if r.manifest.uri != muri || Some(r.object.uri.as_str()) != rec.object_uri.as_deref() {
        return mismatched(format!(
            "verification receipt {ruri} names manifest {} and object {}, the ledger {muri} and {}",
            shown(&r.manifest.uri),
            shown(&r.object.uri),
            rec.object_uri.as_deref().unwrap_or("nothing")
        ));
    }
    let digest_bound = rec
        .manifest_sha256
        .as_deref()
        .is_some_and(|want| !want.is_empty() && want.eq_ignore_ascii_case(&r.manifest.sha256));
    if !digest_bound {
        return mismatched(format!(
            "verification receipt {ruri} binds manifest digest {}, the ledger recorded {}",
            shown(&r.manifest.sha256),
            rec.manifest_sha256.as_deref().unwrap_or("nothing")
        ));
    }
    // A pinned row binds the version: a receipt naming none, or another,
    // is not this batch's (review of #711).
    if let Some(want) = rec.manifest_version_id.as_deref() {
        if r.manifest.version_id.as_deref() != Some(want) {
            return mismatched(format!(
                "verification receipt {ruri} names manifest version {}, the ledger pinned {want}",
                r.manifest
                    .version_id
                    .as_deref()
                    .map_or_else(|| "none".to_owned(), shown)
            ));
        }
    }
    if rec
        .object_size_bytes
        .is_some_and(|want| want < 0 || want as u64 != r.object.size_bytes)
        || rec
            .object_sha256
            .as_deref()
            .is_some_and(|want| !want.is_empty() && !want.eq_ignore_ascii_case(&r.object.checksum))
    {
        return mismatched(format!(
            "verification receipt {ruri} and the ledger disagree on object {}'s size or digest",
            shown(&r.object.uri)
        ));
    }
    if let Some(class) = rec.verification_class {
        if r.verification.class != class.as_str() {
            return mismatched(format!(
                "verification receipt {ruri} says the batch was verified {}, the ledger \
                 recorded class {}",
                shown(&r.verification.class),
                class.as_str()
            ));
        }
    }
    (ReceiptResult::Ok, String::new(), read)
}

// ---- What to check ----------------------------------------------------------

/// Where a committed row was read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RowOrigin {
    Ledger,
    /// A prune digest: the row was deleted from the ledger (#560).
    Digest,
}

impl RowOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            RowOrigin::Ledger => "ledger",
            RowOrigin::Digest => "digest",
        }
    }
}

/// One committed row to check.
#[derive(Debug, Clone)]
pub struct ScrubRow {
    pub record: BatchRecord,
    pub origin: RowOrigin,
}

/// The committed rows of the ledger and the prune digests, each batch once
/// (the ledger's copy wins: a prune that crashed between writing its digest
/// and deleting leaves the row in both), in batch-id order. A batch id starts
/// with its UTC timestamp, so the order is roughly the order of archiving and
/// a cursor walking it meets new batches at the end.
pub fn committed_rows(ledger: Vec<BatchRecord>, digested: Vec<BatchRecord>) -> Vec<ScrubRow> {
    let mut seen = HashSet::new();
    let mut rows: Vec<ScrubRow> = ledger
        .into_iter()
        .map(|record| (record, RowOrigin::Ledger))
        .chain(
            digested
                .into_iter()
                .map(|record| (record, RowOrigin::Digest)),
        )
        .filter(|(record, _)| record.state == BatchState::SourceCommitted)
        .filter(|(record, _)| seen.insert(record.batch_id.clone()))
        .map(|(record, origin)| ScrubRow { record, origin })
        .collect();
    rows.sort_by(|a, b| a.record.batch_id.cmp(&b.record.batch_id));
    rows
}

// ---- Bounds -----------------------------------------------------------------

/// How a run is bounded.
#[derive(Debug, Clone)]
pub struct ScrubOptions {
    /// Batches checked at once.
    pub concurrency: usize,
    /// Objects started per second, at most.
    pub max_objects_per_sec: Option<f64>,
    /// Object bytes read per second, at most, by the ledger's recorded size.
    pub max_bytes_per_sec: Option<u64>,
    /// Batches checked this run, at most.
    pub limit: Option<usize>,
    /// Resume after the batch this file names, and record progress in it.
    pub cursor_file: Option<PathBuf>,
    /// Check this many batches chosen at random instead of walking in order.
    pub sample: Option<usize>,
    /// The sample's seed; random when not given, and reported either way.
    pub seed: Option<u64>,
    /// `--check-retention` (#684): also read each committed batch's S3 Object
    /// Lock retention and expect this mode.
    pub check_retention: Option<ObjectLockMode>,
    /// Also check each committed batch's verification receipt (#671
    /// release B): set when the config writes receipts, or by
    /// `--require-receipts`. A missing one is counted, not a finding.
    pub check_receipts: bool,
    /// `--require-receipts`: check receipts, and a missing one is a finding.
    pub require_receipts: bool,
}

pub const DEFAULT_CONCURRENCY: usize = 4;

impl Default for ScrubOptions {
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_CONCURRENCY,
            max_objects_per_sec: None,
            max_bytes_per_sec: None,
            limit: None,
            cursor_file: None,
            sample: None,
            seed: None,
            check_retention: None,
            check_receipts: false,
            require_receipts: false,
        }
    }
}

/// The longest one object's slot can be. `Duration::from_secs_f64` panics on
/// an interval it cannot hold (a rate of 1e-300 objects/s, or a huge recorded
/// size at 1 B/s), and `Instant + Duration` panics past the clock's range
/// (review); a day is past any rate the command accepts.
pub const MAX_SLOT: Duration = Duration::from_secs(86_400);

fn slot(seconds: f64) -> Duration {
    Duration::try_from_secs_f64(seconds).map_or(MAX_SLOT, |d| d.min(MAX_SLOT))
}

/// Spaces object starts so neither rate is exceeded on average: each start
/// reserves a slot as long as the slower of the two rates says one object of
/// its size takes, after the previous slot. No burst allowance: a scrub has no
/// deadline worth spending the store's headroom on.
pub struct Pacer {
    per_object: Option<Duration>,
    bytes_per_sec: Option<u64>,
    next: Mutex<Option<Instant>>,
}

impl Pacer {
    pub fn new(max_objects_per_sec: Option<f64>, max_bytes_per_sec: Option<u64>) -> Self {
        Self {
            per_object: max_objects_per_sec
                .filter(|rate| rate.is_finite() && *rate > 0.0)
                .map(|rate| slot(1.0 / rate)),
            bytes_per_sec: max_bytes_per_sec.filter(|rate| *rate > 0),
            next: Mutex::new(None),
        }
    }

    /// Reserve the next slot for an object of `bytes`, at or after `now`, and
    /// return when it starts.
    pub fn reserve(&self, now: Instant, bytes: u64) -> Instant {
        let by_bytes = self
            .bytes_per_sec
            .map(|rate| slot(bytes as f64 / rate as f64));
        let cost = self.per_object.max(by_bytes).unwrap_or_default();
        let mut next = self.next.lock().unwrap_or_else(|e| e.into_inner());
        let start = next.map_or(now, |at| at.max(now));
        *next = Some(start + cost);
        start
    }

    async fn admit(&self, bytes: u64) {
        if self.per_object.is_none() && self.bytes_per_sec.is_none() {
            return;
        }
        let at = self.reserve(Instant::now(), bytes);
        tokio::time::sleep_until(at).await;
    }
}

// ---- Cursor -----------------------------------------------------------------

const CURSOR_VERSION: u32 = 1;

/// The cursor file: where the current pass has got to.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Cursor {
    vtop_scrub_cursor: u32,
    /// The last batch checked in the current pass; `None` at a pass's start.
    after: Option<String>,
    passes_completed: u64,
    pass_started_at: Option<String>,
    updated_at: String,
}

impl Cursor {
    fn load(path: &Path) -> Result<Self, VtopError> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self {
                    vtop_scrub_cursor: CURSOR_VERSION,
                    ..Self::default()
                })
            }
            Err(e) => return Err(e.into()),
        };
        // An unreadable cursor is an error, not a fresh start: restarting
        // silently would re-read from the beginning on every run, and on a
        // large archive never reach the end (delete the file to restart).
        let cursor: Self = serde_json::from_str(&text).map_err(|e| {
            VtopError::Other(format!(
                "scrub cursor {} is not a cursor ({e}); delete it to start a new pass",
                path.display()
            ))
        })?;
        if cursor.vtop_scrub_cursor != CURSOR_VERSION {
            return Err(VtopError::Other(format!(
                "scrub cursor {} is version {}; this build reads {CURSOR_VERSION}",
                path.display(),
                cursor.vtop_scrub_cursor
            )));
        }
        Ok(cursor)
    }

    /// Written to a temporary file and renamed into place, so a run killed
    /// mid-write leaves the previous cursor, never half of one.
    fn store(&mut self, path: &Path) -> Result<(), VtopError> {
        self.updated_at = chrono::Utc::now().to_rfc3339();
        let dir = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("scrub.cursor");
        let tmp = dir.join(format!(".{name}.tmp"));
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

/// The locks a run holds from before it reads anything it will replace until
/// it has published (review of #652): the cursor's and the metrics file's,
/// each a `<path>.lock` beside it. Two scrubs sharing a cursor (a cron run
/// that overlaps the previous one) would each advance it past rows the other
/// has not finished. Two sharing a metrics file, cursor or not, would each
/// replace the file from the snapshot they read before the other published,
/// losing its counts and its finding time.
pub struct RunLock {
    _held: Vec<std::fs::File>,
}

/// Take the run's locks: the cursor's first, then the metrics file's, so two
/// runs never hold one each and wait on the other.
pub fn lock_run(cursor: Option<&Path>, metrics: Option<&Path>) -> Result<RunLock, VtopError> {
    let mut held = Vec::new();
    // The cursor's directory is created, as the cursor file itself always
    // has been. The metrics file's is not: it is the textfile collector's,
    // and a run that cannot publish into it should fail here, before any
    // reads, not after a slice of them.
    for (path, what, create_dir) in [(cursor, "cursor", true), (metrics, "metrics file", false)] {
        if let Some(path) = path {
            held.push(lock_file(path, what, create_dir)?);
        }
    }
    Ok(RunLock { _held: held })
}

fn lock_file(path: &Path, what: &str, create_dir: bool) -> Result<std::fs::File, VtopError> {
    let lock = PathBuf::from(format!("{}.lock", path.display()));
    if create_dir {
        if let Some(parent) = lock.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock)
        .map_err(|e| {
            VtopError::Other(format!("cannot lock the {what} at {}: {e}", lock.display()))
        })?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(VtopError::Other(format!(
            "another scrub holds {}; it is still running against this {what}",
            lock.display()
        ))),
        Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

/// Where a run with findings leaves the cursor once its results are
/// published. Until then the saved cursor stays before its first finding, so
/// a run killed in between resumes at the damage and finds it again (review
/// of #652). [`scrub`] commits it at once (it publishes nothing). The
/// command commits it after the report and the metrics file are out.
#[derive(Debug, Clone)]
pub struct PendingCursor {
    path: PathBuf,
    cursor: Cursor,
}

impl PendingCursor {
    /// Save the run's final position. Takes the run's lock as proof it is
    /// still held.
    pub fn commit(mut self, _held: &RunLock) -> Result<(), VtopError> {
        self.cursor.store(&self.path)
    }
}

/// The cursor file as it stood before a run, so the run can be undone if its
/// results cannot be published (review of #652). The cursor is saved batch by
/// batch as the run goes, which a killed run needs. A run that finishes but
/// whose metrics file then fails to write would otherwise have moved the
/// cursor past findings that reached no metrics. The next runs would check
/// later slices, and the findings alert would stay silent until the next
/// pass. Restored, the next run checks the same slice again and publishes
/// what it finds.
pub struct CursorSnapshot {
    path: PathBuf,
    /// `None`: there was no cursor file, so restoring removes the run's one.
    bytes: Option<Vec<u8>>,
}

impl CursorSnapshot {
    pub fn take(path: &Path) -> Result<Self, VtopError> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => Some(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path: path.to_path_buf(),
            bytes,
        })
    }

    /// Put the cursor back. It takes the run's lock, held since before the
    /// snapshot, so no other scrub can have moved the cursor in between.
    pub fn restore(&self, _held: &RunLock) -> Result<(), VtopError> {
        match &self.bytes {
            Some(bytes) => {
                let dir = self
                    .path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."));
                let name = self
                    .path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("scrub.cursor");
                let tmp = dir.join(format!(".{name}.restore.tmp"));
                std::fs::write(&tmp, bytes)?;
                std::fs::rename(&tmp, &self.path)?;
            }
            None => match std::fs::remove_file(&self.path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            },
        }
        Ok(())
    }
}

// ---- The run ------------------------------------------------------------------

/// A finding, as printed and as exported in JSON.
#[derive(Debug, Clone, Serialize)]
pub struct ScrubFinding {
    pub batch_id: String,
    pub result: ScrubResult,
    pub origin: RowOrigin,
    pub object_uri: Option<String>,
    pub manifest_uri: Option<String>,
    pub manifest_version_id: Option<String>,
    /// Set for a retention finding (#684); `result` is then the content
    /// check's, which passed.
    pub retention: Option<RetentionResult>,
    /// Set for a receipt finding (#671 release B); `result` is then the
    /// content check's.
    pub receipt: Option<ReceiptResult>,
    /// Set for a receipt retention finding (#710): the verification
    /// receipt's own S3 Object Lock retention, apart from the batch's object
    /// and manifest (`retention`); `result` is then the content check's.
    pub receipt_retention: Option<RetentionResult>,
    pub detail: String,
}

/// Where a cursor run started and stopped.
#[derive(Debug, Clone, Serialize)]
pub struct CursorProgress {
    pub file: PathBuf,
    pub resumed_after: Option<String>,
    /// Where the next run resumes; `None` when this run completed the pass.
    pub next_after: Option<String>,
    pub pass_complete: bool,
    pub passes_completed: u64,
}

/// Where the rows came from; filled in by the caller that read them.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RowSources {
    pub ledger_rows: usize,
    pub digest_rows: usize,
    pub digest_files: usize,
}

/// A scrub run's result.
#[derive(Debug, Clone, Serialize)]
pub struct ScrubReport {
    pub sources: RowSources,
    /// Committed batches known (ledger and digests, each once).
    pub committed_rows: usize,
    /// `all`, `cursor` or `sample`.
    pub mode: &'static str,
    /// Batches this run set out to check.
    pub selected: usize,
    pub checked: usize,
    /// Every result, at zero when nothing had it.
    pub results: BTreeMap<&'static str, u64>,
    /// Manifests whose MAC authenticated, by the key that verified them
    /// (#685): each configured key id (or `none` for a key with no id), at
    /// zero when it verified nothing. After a rotation, every key the
    /// archive was signed with should appear here and nothing should be a
    /// finding. Empty when no key is configured.
    pub manifest_keys: BTreeMap<String, u64>,
    /// `--check-retention` results by [`RetentionResult`] (#684), every one
    /// at zero when nothing had it; empty without the flag.
    pub retention: BTreeMap<&'static str, u64>,
    /// Receipt results by [`ReceiptResult`] (#671 release B), every one at
    /// zero when nothing had it; empty when receipts were not checked.
    pub receipts: BTreeMap<&'static str, u64>,
    /// The verification receipts' own retention by [`RetentionResult`]
    /// (#710), every one at zero when nothing had it; empty unless the run
    /// both checked receipts and read retention (`--check-retention`). A
    /// missing receipt is not counted here: it is `receipts["missing"]`.
    pub receipt_retention: BTreeMap<&'static str, u64>,
    pub findings: Vec<ScrubFinding>,
    pub cursor: Option<CursorProgress>,
    pub sample_seed: Option<u64>,
    /// Whether every committed batch has now been checked: a run over all of
    /// them, or the cursor run that finished a pass.
    pub pass_complete: bool,
    /// Conditions that do not fail the run but limit what it proves, such as
    /// ledger retention configured with no prune digests found.
    pub warnings: Vec<String>,
    pub passed: bool,
    /// The batches checked, in order. Not exported: a full scrub of a large
    /// archive would print every id it read.
    #[serde(skip)]
    pub checked_ids: Vec<String>,
    /// The cursor's final position, not yet saved because the run found
    /// something: committed once the findings are published.
    #[serde(skip)]
    pub pending_cursor: Option<PendingCursor>,
}

/// Pick `n` rows at random: the `n` whose keyed hash of the batch id is
/// smallest, so a seed reproduces its sample and every row is equally likely.
fn sample_rows(rows: Vec<ScrubRow>, n: usize, seed: u64) -> Vec<ScrubRow> {
    let mut key = [0u8; 32];
    key[..8].copy_from_slice(&seed.to_le_bytes());
    let mut keyed: Vec<([u8; 32], ScrubRow)> = rows
        .into_iter()
        .map(|row| {
            (
                *blake3::keyed_hash(&key, row.record.batch_id.as_bytes()).as_bytes(),
                row,
            )
        })
        .collect();
    keyed.sort_by_key(|(hash, _)| *hash);
    keyed.truncate(n);
    let mut picked: Vec<ScrubRow> = keyed.into_iter().map(|(_, row)| row).collect();
    picked.sort_by(|a, b| a.record.batch_id.cmp(&b.record.batch_id));
    picked
}

/// Check `rows` (from [`committed_rows`]) as `opts` bounds the run.
///
/// With a cursor file, the run resumes after the batch it names, checks up to
/// `limit` batches in order, and records each one as it completes, in order,
/// so a run killed part-way resumes after the last batch it finished. The run
/// that reaches the end completes the pass: the cursor resets and the next run
/// starts again from the first batch. The saved cursor stops at the first
/// finding, so a run killed before it publishes resumes at the damage. This
/// entry point publishes nothing, so it saves the final position before it
/// returns. The `vtopctl scrub` command, which publishes, saves it only once
/// the report and metrics are out ([`PendingCursor`]).
pub async fn scrub(
    backend: &dyn UploadBackend,
    keyring: &ManifestKeyring,
    rows: Vec<ScrubRow>,
    opts: &ScrubOptions,
) -> Result<ScrubReport, VtopError> {
    let lock = lock_run(opts.cursor_file.as_deref(), None)?;
    let mut report = scrub_held(backend, keyring, rows, opts, &lock).await?;
    if let Some(pending) = report.pending_cursor.take() {
        pending.commit(&lock)?;
    }
    Ok(report)
}

/// [`scrub`] under locks the caller took ([`lock_run`]) and holds until it has
/// published. A final cursor position past a finding is returned in
/// `pending_cursor`, for the caller to commit after publishing.
pub async fn scrub_held(
    backend: &dyn UploadBackend,
    keyring: &ManifestKeyring,
    rows: Vec<ScrubRow>,
    opts: &ScrubOptions,
    _held: &RunLock,
) -> Result<ScrubReport, VtopError> {
    let committed = rows.len();
    let concurrency = opts.concurrency.max(1);

    let mut cursor = match &opts.cursor_file {
        Some(path) => Some(Cursor::load(path)?),
        None => None,
    };
    let resumed_after = cursor.as_ref().and_then(|c| c.after.clone());
    // `reaches_end`: whether this run, once it finishes, has checked every
    // committed row after the cursor, which completes the pass.
    let (mode, sample_seed, selected, reaches_end) = if let Some(n) = opts.sample {
        let seed = opts
            .seed
            .unwrap_or_else(|| uuid::Uuid::new_v4().as_u64_pair().0);
        ("sample", Some(seed), sample_rows(rows, n, seed), false)
    } else {
        let mut after: Vec<ScrubRow> = rows
            .into_iter()
            .filter(|row| {
                resumed_after
                    .as_deref()
                    .is_none_or(|after| row.record.batch_id.as_str() > after)
            })
            .collect();
        let remaining = after.len();
        if let Some(limit) = opts.limit {
            after.truncate(limit);
        }
        let reaches_end = after.len() == remaining;
        if let Some(c) = cursor.as_mut().filter(|c| c.after.is_none()) {
            c.pass_started_at = Some(chrono::Utc::now().to_rfc3339());
        }
        let mode = if cursor.is_some() { "cursor" } else { "all" };
        (mode, None, after, reaches_end)
    };

    let pacer = Pacer::new(opts.max_objects_per_sec, opts.max_bytes_per_sec);
    let mut results: BTreeMap<&'static str, u64> =
        ScrubResult::ALL.iter().map(|r| (r.as_str(), 0)).collect();
    let mut manifest_keys: BTreeMap<String, u64> =
        keyring.key_labels().into_iter().map(|k| (k, 0)).collect();
    let mut findings = Vec::new();
    let mut retention_counts: BTreeMap<&'static str, u64> = if opts.check_retention.is_some() {
        RetentionResult::ALL
            .iter()
            .map(|r| (r.as_str(), 0))
            .collect()
    } else {
        BTreeMap::new()
    };
    let check_receipts = opts.check_receipts || opts.require_receipts;
    let mut receipt_counts: BTreeMap<&'static str, u64> = if check_receipts {
        ReceiptResult::ALL.iter().map(|r| (r.as_str(), 0)).collect()
    } else {
        BTreeMap::new()
    };
    // The receipts' own retention (#710): only when the run both checks
    // receipts and reads retention, in its own map so a receipt without a
    // lock is never counted as an object without one.
    let receipt_retention_mode = opts.check_retention.filter(|_| check_receipts);
    let mut receipt_retention_counts: BTreeMap<&'static str, u64> =
        if receipt_retention_mode.is_some() {
            RetentionResult::ALL
                .iter()
                .map(|r| (r.as_str(), 0))
                .collect()
        } else {
            BTreeMap::new()
        };
    let mut checked_ids = Vec::with_capacity(selected.len());
    let selected_count = selected.len();

    {
        let pacer = &pacer;
        let mut checks = futures::stream::iter(selected.iter())
            .map(|row| async move {
                let bytes = row
                    .record
                    .object_size_bytes
                    .and_then(|size| u64::try_from(size).ok())
                    .unwrap_or(0);
                pacer.admit(bytes).await;
                // Opened after the pacer, so the span times the check and not
                // the wait for its turn (#683).
                let span = crate::spans::scrub_check(&row.record, row.origin.as_str());
                let mut check = scrub_one(backend, keyring, &row.record)
                    .instrument(span.clone())
                    .await;
                // Retention only where the content passed (#684): a missing
                // or damaged object is already its own finding.
                let mut retention = None;
                if let (Some(mode), false) = (opts.check_retention, check.result.is_finding()) {
                    match retention_of(backend, &row.record, mode).await {
                        Ok(found) => retention = Some(found),
                        Err(unreadable) => {
                            check = BatchCheck {
                                key: check.key,
                                ..unreadable
                            }
                        }
                    }
                }
                // The receipt beside the manifest (#671 release B),
                // whatever the content check found: it is its own evidence.
                // On the heap: the debug build's command future already
                // outgrows a 2 MiB test stack with the receipt check
                // (integration_scrub.rs), and the retention read adds an
                // await to it (#710).
                let receipt = if check_receipts {
                    Some(
                        Box::pin(receipt_check(
                            backend,
                            keyring,
                            &row.record,
                            receipt_retention_mode,
                        ))
                        .await,
                    )
                } else {
                    None
                };
                span.record("result", check.result.as_str());
                span.record(
                    "otel.status_code",
                    if check.result.is_finding() {
                        "error"
                    } else {
                        "ok"
                    },
                );
                (row, check, retention, receipt)
            })
            // In order, so the cursor only ever passes a contiguous run of
            // checked batches.
            .buffered(concurrency);
        while let Some((row, check, retention, receipt)) = checks.next().await {
            if let Some(ReceiptCheck {
                result: found,
                detail,
                read_at,
                retention: receipt_retention,
            }) = receipt
            {
                *receipt_counts.entry(found.as_str()).or_default() += 1;
                if let (Some(judged), Some(ruri)) = (receipt_retention, read_at.as_deref()) {
                    *receipt_retention_counts.entry(judged.as_str()).or_default() += 1;
                    if judged.is_finding() {
                        let detail = receipt_retention_detail(
                            ruri,
                            judged,
                            opts.check_retention.map(|m| m.as_str()).unwrap_or_default(),
                        );
                        tracing::warn!(
                            event = "scrub_receipt_retention_finding",
                            batch_id = %row.record.batch_id,
                            receipt_uri = ruri,
                            receipt_retention = judged.as_str(),
                            detail = %detail,
                            "scrub receipt retention finding"
                        );
                        findings.push(ScrubFinding {
                            batch_id: row.record.batch_id.clone(),
                            result: check.result,
                            origin: row.origin,
                            object_uri: row.record.object_uri.clone(),
                            manifest_uri: row.record.manifest_uri.clone(),
                            manifest_version_id: row.record.manifest_version_id.clone(),
                            retention: None,
                            receipt: None,
                            receipt_retention: Some(judged),
                            detail,
                        });
                    }
                }
                if found.is_finding(opts.require_receipts) {
                    let detail = format!("{detail}; {}", receipt_remedy(found));
                    tracing::warn!(
                        batch_id = %row.record.batch_id,
                        receipt = found.as_str(),
                        detail = %detail,
                        "scrub receipt finding"
                    );
                    findings.push(ScrubFinding {
                        batch_id: row.record.batch_id.clone(),
                        result: check.result,
                        origin: row.origin,
                        object_uri: row.record.object_uri.clone(),
                        manifest_uri: row.record.manifest_uri.clone(),
                        manifest_version_id: row.record.manifest_version_id.clone(),
                        retention: None,
                        receipt: Some(found),
                        receipt_retention: None,
                        detail,
                    });
                }
            }
            if let Some(found) = retention {
                *retention_counts.entry(found.as_str()).or_default() += 1;
                if found.is_finding() {
                    let what = match found {
                        RetentionResult::Missing => "carries no retention",
                        _ => "is under another retention mode than the configured one",
                    };
                    tracing::warn!(
                        batch_id = %row.record.batch_id,
                        retention = found.as_str(),
                        "scrub retention finding"
                    );
                    findings.push(ScrubFinding {
                        batch_id: row.record.batch_id.clone(),
                        result: check.result,
                        origin: row.origin,
                        object_uri: row.record.object_uri.clone(),
                        manifest_uri: row.record.manifest_uri.clone(),
                        manifest_version_id: row.record.manifest_version_id.clone(),
                        retention: Some(found),
                        receipt: None,
                        receipt_retention: None,
                        detail: format!(
                            "the object or its manifest {what} (S3 Object Lock, expected {}); \
                             it can be deleted or overwritten. Re-apply the retention with \
                             PutObjectRetention, and check who could bypass it",
                            opts.check_retention.map(|m| m.as_str()).unwrap_or_default()
                        ),
                    });
                }
            }
            *results.entry(check.result.as_str()).or_default() += 1;
            if let Some(key) = check.key.as_ref().filter(|k| k.result == KeyResult::Ok) {
                *manifest_keys.entry(key.key_id.clone()).or_default() += 1;
            }
            checked_ids.push(row.record.batch_id.clone());
            if check.result.is_finding() {
                tracing::warn!(
                    batch_id = %row.record.batch_id,
                    result = check.result.as_str(),
                    detail = %check.detail,
                    "scrub finding"
                );
                findings.push(ScrubFinding {
                    batch_id: row.record.batch_id.clone(),
                    result: check.result,
                    origin: row.origin,
                    object_uri: row.record.object_uri.clone(),
                    manifest_uri: row.record.manifest_uri.clone(),
                    manifest_version_id: row.record.manifest_version_id.clone(),
                    retention: None,
                    receipt: None,
                    receipt_retention: None,
                    detail: check.detail,
                });
            }
            if let (Some(c), Some(path)) = (cursor.as_mut(), opts.cursor_file.as_deref()) {
                c.after = Some(row.record.batch_id.clone());
                // Saved only up to the first finding (review of #652): past
                // it, a run killed before publishing would resume after
                // damage nobody was told about. The position is still
                // tracked; it is saved once the findings are published.
                if findings.is_empty() {
                    c.store(path)?;
                }
            }
        }
    }

    let mut pending_cursor = None;
    let progress = match (cursor.as_mut(), opts.cursor_file.as_deref()) {
        (Some(c), Some(path)) => {
            if reaches_end {
                c.after = None;
                c.pass_started_at = None;
                c.passes_completed += 1;
            }
            if findings.is_empty() {
                c.store(path)?;
            } else {
                pending_cursor = Some(PendingCursor {
                    path: path.to_path_buf(),
                    cursor: c.clone(),
                });
            }
            Some(CursorProgress {
                file: path.to_path_buf(),
                resumed_after,
                next_after: c.after.clone(),
                pass_complete: reaches_end,
                passes_completed: c.passes_completed,
            })
        }
        _ => None,
    };

    let passed = findings.is_empty();
    let pass_complete = match &progress {
        Some(progress) => progress.pass_complete,
        None => mode == "all" && selected_count == committed,
    };
    Ok(ScrubReport {
        sources: RowSources::default(),
        committed_rows: committed,
        mode,
        selected: selected_count,
        checked: checked_ids.len(),
        results,
        manifest_keys,
        retention: retention_counts,
        receipts: receipt_counts,
        receipt_retention: receipt_retention_counts,
        findings,
        cursor: progress,
        sample_seed,
        pass_complete,
        warnings: Vec::new(),
        passed,
        checked_ids,
        pending_cursor,
    })
}

/// The warning for a run that cannot see pruned history: ledger retention is
/// on (`engine.ledger_retention_days`), so rows have been or will be pruned,
/// and the digest directory holds no digests. Those batches are known only
/// from the digests, so without them the scrub passes over them without
/// saying so (review of #652). A warning, not a failure: before the first
/// prune there is nothing to find. A deployment pruning only with
/// `vtopctl prune-ledger` sets no retention, and gets no warning.
pub fn missing_digests_warning(
    ledger_retention_days: Option<u32>,
    digest_dir: &Path,
    digest_files: usize,
) -> Option<String> {
    (ledger_retention_days.is_some() && digest_files == 0).then(|| {
        format!(
            "engine.ledger_retention_days is set but {} holds no prune digests: batches \
             pruned from the ledger are not checked; pass --digest-dir where prune-ledger \
             writes them",
            digest_dir.display()
        )
    })
}

impl ScrubReport {
    pub fn lines(&self) -> Vec<String> {
        let mut out = vec![format!(
            "scrubbed {} of {} committed batch(es) ({} ledger row(s), {} digested row(s) from {} \
             digest file(s)); mode {}",
            self.checked,
            self.committed_rows,
            self.sources.ledger_rows,
            self.sources.digest_rows,
            self.sources.digest_files,
            self.mode
        )];
        for warning in &self.warnings {
            out.push(format!("WARN {warning}"));
        }
        if let Some(seed) = self.sample_seed {
            out.push(format!(
                "sample seed {seed} (pass --seed {seed} to repeat it)"
            ));
        }
        out.push(format!(
            "results: {}",
            self.results
                .iter()
                .map(|(result, n)| format!("{result}={n}"))
                .collect::<Vec<_>>()
                .join(" ")
        ));
        if !self.manifest_keys.is_empty() {
            out.push(format!(
                "manifests authenticated by key: {}",
                self.manifest_keys
                    .iter()
                    .map(|(key, n)| format!("{key}={n}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            ));
        }
        if !self.retention.is_empty() {
            out.push(format!(
                "retention: {}",
                self.retention
                    .iter()
                    .map(|(result, n)| format!("{result}={n}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            ));
        }
        if !self.receipts.is_empty() {
            out.push(format!(
                "receipts: {}",
                self.receipts
                    .iter()
                    .map(|(result, n)| format!("{result}={n}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            ));
        }
        if !self.receipt_retention.is_empty() {
            out.push(format!(
                "receipt retention: {}",
                self.receipt_retention
                    .iter()
                    .map(|(result, n)| format!("{result}={n}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            ));
        }
        for finding in &self.findings {
            let origin = match finding.origin {
                RowOrigin::Ledger => "ledger",
                RowOrigin::Digest => "prune digest",
            };
            let result = match (
                finding.retention,
                finding.receipt,
                finding.receipt_retention,
            ) {
                (Some(retention), _, _) => format!("retention {}", retention.as_str()),
                (None, Some(receipt), _) => format!("receipt {}", receipt.as_str()),
                (None, None, Some(retention)) => {
                    format!("receipt retention {}", retention.as_str())
                }
                (None, None, None) => finding.result.as_str().to_owned(),
            };
            out.push(format!(
                "FAIL {result} {} (row from the {origin}): {}",
                finding.batch_id, finding.detail
            ));
        }
        if let Some(cursor) = &self.cursor {
            out.push(match (&cursor.next_after, cursor.pass_complete) {
                (_, true) => format!(
                    "cursor {}: pass complete ({} so far); the next run starts a new pass",
                    cursor.file.display(),
                    cursor.passes_completed
                ),
                (Some(after), false) => format!(
                    "cursor {}: the next run resumes after {after}",
                    cursor.file.display()
                ),
                (None, false) => format!("cursor {}: nothing checked", cursor.file.display()),
            });
        }
        out.push(if self.passed {
            "scrub PASSED".into()
        } else {
            "scrub FAILED".into()
        });
        out
    }

    /// The run as a Prometheus text file for a node exporter's textfile
    /// collector, counted by result only (never by batch or URI).
    ///
    /// `previous` is the file this one replaces. The per-result totals are
    /// carried forward from it, so `vtop_scrub_objects_total` is a real
    /// counter across runs (a deleted file is a counter reset, which
    /// `increase()` handles), and the last-success and last-complete-pass
    /// times survive a run that did not succeed or did not finish a pass.
    pub fn prometheus_text(&self, previous: Option<&str>, now_unix: i64) -> String {
        let carried = Carried::parse(previous.unwrap_or_default());
        let mut out = String::new();
        out.push_str(
            "# HELP vtop_scrub_objects_total Committed batches the archive scrub checked, by \
             result (ok; limited: size-only, size and existence confirmed but no digest \
             compared; any other result is a finding)\n# TYPE vtop_scrub_objects_total counter\n",
        );
        for (result, n) in &self.results {
            let total = carried.counts.get(*result).copied().unwrap_or(0) + n;
            out.push_str(&format!(
                "vtop_scrub_objects_total{{result=\"{result}\"}} {total}\n"
            ));
        }
        // Only with --check-retention (#684), in its own family so the
        // result set of vtop_scrub_objects_total does not change.
        // A run without the flag writes the carried totals unchanged, so
        // they survive it (review of #708).
        let retention: BTreeMap<String, u64> = if self.retention.is_empty() {
            carried.retention.clone()
        } else {
            self.retention
                .iter()
                .map(|(result, n)| {
                    let total = carried.retention.get(*result).copied().unwrap_or(0) + n;
                    ((*result).to_owned(), total)
                })
                .collect()
        };
        if !retention.is_empty() {
            out.push_str(
                "# HELP vtop_scrub_retention_total Committed batches whose S3 Object Lock \
                 retention the scrub read (--check-retention), by result: locked, elapsed (the \
                 retention has ended; not a finding), missing and mode_mismatch (findings)\n\
                 # TYPE vtop_scrub_retention_total counter\n",
            );
            for (result, total) in &retention {
                out.push_str(&format!(
                    "vtop_scrub_retention_total{{result=\"{result}\"}} {total}\n"
                ));
            }
        }
        // Only when receipts were checked (#671 release B), in their own
        // family; a run that did not check them carries the totals forward.
        let receipts: BTreeMap<String, u64> = if self.receipts.is_empty() {
            carried.receipts.clone()
        } else {
            self.receipts
                .iter()
                .map(|(result, n)| {
                    let total = carried.receipts.get(*result).copied().unwrap_or(0) + n;
                    ((*result).to_owned(), total)
                })
                .collect()
        };
        if !receipts.is_empty() {
            out.push_str(
                "# HELP vtop_scrub_receipts_total Committed batches whose verification receipt \
                 the scrub checked, by result: ok; missing (a finding only with \
                 --require-receipts); invalid, unauthenticated, mismatched and unreadable \
                 (findings)\n# TYPE vtop_scrub_receipts_total counter\n",
            );
            for (result, total) in &receipts {
                out.push_str(&format!(
                    "vtop_scrub_receipts_total{{result=\"{result}\"}} {total}\n"
                ));
            }
        }
        // The receipts' own retention (#710), in its own family so an
        // operator can tell a receipt without a lock from an object without
        // one; the same bounded result set as vtop_scrub_retention_total. A
        // run that did not read it carries the totals forward.
        let receipt_retention: BTreeMap<String, u64> = if self.receipt_retention.is_empty() {
            carried.receipt_retention.clone()
        } else {
            self.receipt_retention
                .iter()
                .map(|(result, n)| {
                    let total = carried.receipt_retention.get(*result).copied().unwrap_or(0) + n;
                    ((*result).to_owned(), total)
                })
                .collect()
        };
        if !receipt_retention.is_empty() {
            out.push_str(
                "# HELP vtop_scrub_receipt_retention_total Committed batches whose verification \
                 receipt's S3 Object Lock retention the scrub read (--check-retention with \
                 receipts checked), by result: locked, elapsed (the retention has ended; not a \
                 finding), missing and mode_mismatch (findings). A missing receipt is not \
                 counted here\n\
                 # TYPE vtop_scrub_receipt_retention_total counter\n",
            );
            for (result, total) in &receipt_retention {
                out.push_str(&format!(
                    "vtop_scrub_receipt_retention_total{{result=\"{result}\"}} {total}\n"
                ));
            }
        }
        // By configured key only (#685): the labels are this run's keyring,
        // never an id read from storage, and a key no longer configured
        // drops out (its counter resets if it comes back).
        if !self.manifest_keys.is_empty() {
            out.push_str(
                "# HELP vtop_scrub_manifest_keys_total Manifests the archive scrub authenticated, \
                 by the configured key id that verified them (none: a key with no id)\n# TYPE \
                 vtop_scrub_manifest_keys_total counter\n",
            );
            for (key, n) in &self.manifest_keys {
                let total = carried.key_counts.get(key).copied().unwrap_or(0) + n;
                out.push_str(&format!(
                    "vtop_scrub_manifest_keys_total{{key_id=\"{key}\"}} {total}\n"
                ));
            }
        }
        let gauge = |out: &mut String, name: &str, help: &str, value: String| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}\n"
            ));
        };
        gauge(
            &mut out,
            "vtop_scrub_last_run_findings",
            "Findings (missing, mismatched, unauthenticated, unreadable, unsupported_version; a retention or receipt finding) in the latest scrub run",
            self.findings.len().to_string(),
        );
        gauge(
            &mut out,
            "vtop_scrub_last_run_timestamp_seconds",
            "When the latest scrub run finished",
            now_unix.to_string(),
        );
        let success = if self.passed {
            now_unix
        } else {
            carried.last_success
        };
        gauge(
            &mut out,
            "vtop_scrub_last_success_timestamp_seconds",
            "When a scrub run last finished with no findings (0: never)",
            success.to_string(),
        );
        let pass = if self.pass_complete {
            now_unix
        } else {
            carried.last_pass_complete
        };
        gauge(
            &mut out,
            "vtop_scrub_last_pass_complete_timestamp_seconds",
            "When a scrub last finished checking every committed batch (0: never)",
            pass.to_string(),
        );
        // What the findings alert reads (review of #652). The counter cannot
        // carry it: a counter whose first sample already holds the finding
        // (the first run found damage) shows no `increase()`, and the clean
        // slices of the rest of the pass leave it flat. A timestamp is
        // alertable from its first sample, and stays set across those slices.
        let finding = if self.findings.is_empty() {
            carried.last_finding
        } else {
            now_unix
        };
        gauge(
            &mut out,
            "vtop_scrub_last_finding_timestamp_seconds",
            "When a scrub run last found a missing, mismatched, unauthenticated, unreadable or unsupported-version batch, or a retention or receipt finding (0: never)",
            finding.to_string(),
        );
        out
    }
}

/// What a previous metrics file held that the next one carries forward.
#[derive(Default)]
struct Carried {
    counts: BTreeMap<String, u64>,
    key_counts: BTreeMap<String, u64>,
    retention: BTreeMap<String, u64>,
    receipts: BTreeMap<String, u64>,
    receipt_retention: BTreeMap<String, u64>,
    last_success: i64,
    last_pass_complete: i64,
    last_finding: i64,
}

impl Carried {
    /// Lenient: a value that does not parse is taken as absent (zero), which
    /// costs at most a counter reset.
    fn parse(text: &str) -> Self {
        let mut carried = Self::default();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("vtop_scrub_objects_total{result=\"") {
                if let Some((result, value)) = rest.split_once("\"} ") {
                    if let Ok(value) = value.trim().parse::<u64>() {
                        carried.counts.insert(result.to_owned(), value);
                    }
                }
            } else if let Some(rest) = line.strip_prefix("vtop_scrub_retention_total{result=\"") {
                if let Some((result, value)) = rest.split_once("\"} ") {
                    if let Ok(value) = value.trim().parse::<u64>() {
                        carried.retention.insert(result.to_owned(), value);
                    }
                }
            } else if let Some(rest) =
                line.strip_prefix("vtop_scrub_receipt_retention_total{result=\"")
            {
                if let Some((result, value)) = rest.split_once("\"} ") {
                    if let Ok(value) = value.trim().parse::<u64>() {
                        carried.receipt_retention.insert(result.to_owned(), value);
                    }
                }
            } else if let Some(rest) = line.strip_prefix("vtop_scrub_receipts_total{result=\"") {
                if let Some((result, value)) = rest.split_once("\"} ") {
                    if let Ok(value) = value.trim().parse::<u64>() {
                        carried.receipts.insert(result.to_owned(), value);
                    }
                }
            } else if let Some(rest) = line.strip_prefix("vtop_scrub_manifest_keys_total{key_id=\"")
            {
                if let Some((key, value)) = rest.split_once("\"} ") {
                    if let Ok(value) = value.trim().parse::<u64>() {
                        carried.key_counts.insert(key.to_owned(), value);
                    }
                }
            } else if let Some(value) =
                line.strip_prefix("vtop_scrub_last_success_timestamp_seconds ")
            {
                carried.last_success = value.trim().parse().unwrap_or(0);
            } else if let Some(value) =
                line.strip_prefix("vtop_scrub_last_pass_complete_timestamp_seconds ")
            {
                carried.last_pass_complete = value.trim().parse().unwrap_or(0);
            } else if let Some(value) =
                line.strip_prefix("vtop_scrub_last_finding_timestamp_seconds ")
            {
                carried.last_finding = value.trim().parse().unwrap_or(0);
            }
        }
        carried
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_pacer_spaces_starts_by_the_slower_of_its_two_rates() {
        let t0 = Instant::now();
        let by_count = Pacer::new(Some(2.0), None);
        let starts: Vec<Duration> = (0..3).map(|_| by_count.reserve(t0, 0) - t0).collect();
        assert_eq!(
            starts,
            [0, 500, 1000].map(Duration::from_millis).to_vec(),
            "2 objects/s"
        );

        // 1000 B/s: a 250-byte object holds the store for 250 ms, a
        // 1500-byte one for 1.5 s; the object rate (10/s, 100 ms) is the
        // looser bound for both.
        let by_bytes = Pacer::new(Some(10.0), Some(1000));
        assert_eq!(by_bytes.reserve(t0, 250), t0);
        assert_eq!(by_bytes.reserve(t0, 1500), t0 + Duration::from_millis(250));
        assert_eq!(by_bytes.reserve(t0, 0), t0 + Duration::from_millis(1750));

        // A start reserved after a quiet spell does not inherit a backlog.
        let later = t0 + Duration::from_secs(60);
        assert_eq!(by_bytes.reserve(later, 0), later);
    }

    /// An interval too long for a `Duration` used to panic before the first
    /// check (review); it is held to `MAX_SLOT` instead.
    #[test]
    fn an_absurd_rate_is_clamped_not_a_panic() {
        let t0 = Instant::now();
        let tiny = Pacer::new(Some(1e-300), None);
        assert_eq!(tiny.reserve(t0, 0), t0);
        assert_eq!(tiny.reserve(t0, 0), t0 + MAX_SLOT);
        let huge = Pacer::new(None, Some(1));
        assert_eq!(huge.reserve(t0, u64::MAX), t0);
        assert_eq!(huge.reserve(t0, 0), t0 + MAX_SLOT);
    }

    #[test]
    fn an_unbounded_pacer_never_waits() {
        let pacer = Pacer::new(None, None);
        let t0 = Instant::now();
        assert_eq!(pacer.reserve(t0, u64::MAX), t0);
        assert_eq!(pacer.reserve(t0, u64::MAX), t0);
    }

    #[test]
    fn absence_is_told_from_an_unreadable_store() {
        for missing in [
            VtopError::NotFound("s3://b/k".into()),
            VtopError::Upload("head_object s3://b/k: unparseable response (http 404): ".into()),
            VtopError::Upload("get_object s3://b/k: NoSuchKey (http 404, NoSuchKey)".into()),
            VtopError::Upload("get_object s3://b/k: (http 404, NoSuchVersion)".into()),
            VtopError::Upload(
                "An error occurred (404) when calling the HeadObject operation: Not Found".into(),
            ),
            VtopError::Io(std::io::Error::from(std::io::ErrorKind::NotFound)),
        ] {
            assert!(looks_missing(&missing), "{missing}");
        }
        for unreadable in [
            VtopError::UploadThrottled("SlowDown (http 503)".into()),
            VtopError::Upload("head_object s3://b/k: AccessDenied (http 403)".into()),
            VtopError::Upload("aws verification exceeded the 300s timeout".into()),
            VtopError::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied)),
        ] {
            assert!(!looks_missing(&unreadable), "{unreadable}");
        }
    }

    fn report(results: &[(ScrubResult, u64)], findings: usize, pass_complete: bool) -> ScrubReport {
        let mut counts: BTreeMap<&'static str, u64> =
            ScrubResult::ALL.iter().map(|r| (r.as_str(), 0)).collect();
        for (result, n) in results {
            counts.insert(result.as_str(), *n);
        }
        ScrubReport {
            sources: RowSources::default(),
            committed_rows: 0,
            mode: "all",
            selected: 0,
            checked: 0,
            results: counts,
            manifest_keys: BTreeMap::new(),
            retention: BTreeMap::new(),
            receipts: BTreeMap::new(),
            receipt_retention: BTreeMap::new(),
            findings: (0..findings)
                .map(|i| ScrubFinding {
                    batch_id: format!("b{i}"),
                    result: ScrubResult::Missing,
                    origin: RowOrigin::Ledger,
                    object_uri: None,
                    manifest_uri: None,
                    manifest_version_id: None,
                    retention: None,
                    receipt: None,
                    receipt_retention: None,
                    detail: String::new(),
                })
                .collect(),
            cursor: None,
            sample_seed: None,
            pass_complete,
            warnings: Vec::new(),
            passed: findings == 0,
            checked_ids: Vec::new(),
            pending_cursor: None,
        }
    }

    #[test]
    fn the_metrics_file_counts_across_runs_and_keeps_the_last_success() {
        let first = report(&[(ScrubResult::Ok, 5)], 0, true).prometheus_text(None, 1000);
        for result in ScrubResult::ALL {
            assert!(
                first.contains(&format!(
                    "vtop_scrub_objects_total{{result=\"{}\"}} ",
                    result.as_str()
                )),
                "every result is primed: {first}"
            );
        }
        assert!(first.contains("vtop_scrub_last_success_timestamp_seconds 1000\n"));
        assert!(first.contains("vtop_scrub_last_pass_complete_timestamp_seconds 1000\n"));

        let second = report(&[(ScrubResult::Ok, 2), (ScrubResult::Missing, 1)], 1, false)
            .prometheus_text(Some(&first), 2000);
        assert!(
            second.contains("vtop_scrub_objects_total{result=\"ok\"} 7\n"),
            "{second}"
        );
        assert!(second.contains("vtop_scrub_objects_total{result=\"missing\"} 1\n"));
        assert!(second.contains("vtop_scrub_last_run_findings 1\n"));
        assert!(second.contains("vtop_scrub_last_run_timestamp_seconds 2000\n"));
        assert!(
            second.contains("vtop_scrub_last_success_timestamp_seconds 1000\n"),
            "a failed run keeps the previous success: {second}"
        );
        assert!(second.contains("vtop_scrub_last_pass_complete_timestamp_seconds 1000\n"));

        // No previous success: zero, which a staleness alert reads as never.
        let never = report(&[], 1, false).prometheus_text(Some("garbage\n"), 3000);
        assert!(
            never.contains("vtop_scrub_last_success_timestamp_seconds 0\n"),
            "{never}"
        );
    }

    /// With ledger retention on, pruned rows are known only from the digests;
    /// a scrub that found none would pass over them without saying so
    /// (review of #652). It warns, and the warning is in the report.
    #[test]
    fn retention_without_digests_is_warned_about() {
        let dir = Path::new("/var/lib/vtop/ledger-digests");
        let warning = missing_digests_warning(Some(30), dir, 0).expect("warned");
        assert!(
            warning.contains("/var/lib/vtop/ledger-digests"),
            "{warning}"
        );
        assert!(warning.contains("--digest-dir"), "{warning}");
        assert!(
            missing_digests_warning(Some(30), dir, 2).is_none(),
            "digests found"
        );
        assert!(
            missing_digests_warning(None, dir, 0).is_none(),
            "no retention configured: nothing is known to be pruned"
        );

        let mut with = report(&[], 0, true);
        with.warnings.push(warning.clone());
        assert!(with.lines().iter().any(|line| line.contains(&warning)));
        let json = serde_json::to_value(&with).unwrap();
        assert_eq!(json["warnings"][0], warning.as_str());
    }

    /// Damage found by the very FIRST run must be alertable (review of #652).
    /// The counter is born in that run's file already holding the finding,
    /// so `increase()` has no earlier sample to compare it to and sees
    /// nothing; and the clean slices that follow leave it flat for the rest
    /// of the pass. A timestamp of the last run with findings, carried
    /// forward, is alertable from its first sample on.
    #[test]
    fn the_last_finding_time_is_set_by_the_first_run_and_carried_by_clean_ones() {
        let gauge = "vtop_scrub_last_finding_timestamp_seconds";
        let first = report(&[(ScrubResult::Mismatched, 1)], 1, false).prometheus_text(None, 1000);
        assert!(first.contains(&format!("{gauge} 1000\n")), "{first}");
        assert!(
            first.contains(&format!("# TYPE {gauge} gauge\n")),
            "{first}"
        );

        let clean = report(&[(ScrubResult::Ok, 9)], 0, false).prometheus_text(Some(&first), 2000);
        assert!(
            clean.contains(&format!("{gauge} 1000\n")),
            "a clean slice keeps the last finding's time: {clean}"
        );

        let later =
            report(&[(ScrubResult::Missing, 1)], 1, false).prometheus_text(Some(&clean), 3000);
        assert!(later.contains(&format!("{gauge} 3000\n")), "{later}");

        let never = report(&[(ScrubResult::Ok, 3)], 0, true).prometheus_text(None, 4000);
        assert!(
            never.contains(&format!("{gauge} 0\n")),
            "0: no run has found anything: {never}"
        );
    }

    #[test]
    fn a_cursor_that_is_not_one_is_refused_not_restarted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scrub.cursor");
        assert!(
            Cursor::load(&path).unwrap().after.is_none(),
            "absent: a new pass"
        );
        std::fs::write(&path, "{ half a cur").unwrap();
        let error = Cursor::load(&path).unwrap_err().to_string();
        assert!(error.contains("delete it"), "{error}");
    }

    #[test]
    fn a_cursor_snapshot_puts_back_exactly_what_was_there() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scrub.cursor");
        std::fs::write(&path, b"{\"before\":1}").unwrap();
        // The run's lock is taken before the snapshot and held through the
        // restore, so nothing else moves the cursor in between.
        let held = lock_run(Some(&path), None).unwrap();
        let snapshot = CursorSnapshot::take(&path).unwrap();
        std::fs::write(&path, b"{\"after\":2}").unwrap();
        snapshot.restore(&held).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"{\"before\":1}");
    }

    #[test]
    fn two_scrubs_cannot_share_a_cursor_or_a_metrics_file() {
        let dir = tempfile::tempdir().unwrap();
        let cursor = dir.path().join("scrub.cursor");
        let metrics = dir.path().join("scrub.prom");
        let held = lock_run(Some(&cursor), Some(&metrics)).unwrap();
        for (c, m) in [
            (Some(cursor.as_path()), None),
            (None, Some(metrics.as_path())),
        ] {
            let error = lock_run(c, m).err().expect("the second is refused");
            assert!(error.to_string().contains("another scrub"), "{error}");
        }
        drop(held);
        lock_run(Some(&cursor), Some(&metrics)).expect("free again once the first run ends");
    }

    /// Review of #708: a run without `--check-retention` keeps the retention
    /// totals an earlier run wrote, or an hourly run without the flag would
    /// reset the daily run's counter every hour.
    #[test]
    fn a_run_without_check_retention_carries_the_retention_totals_forward() {
        let mut with = report(&[(ScrubResult::Ok, 2)], 0, true);
        with.retention = RetentionResult::ALL
            .iter()
            .map(|r| (r.as_str(), u64::from(*r == RetentionResult::Locked) * 2))
            .collect();
        let first = with.prometheus_text(None, 100);
        assert!(
            first.contains("vtop_scrub_retention_total{result=\"locked\"} 2\n"),
            "{first}"
        );
        let without = report(&[(ScrubResult::Ok, 1)], 0, true);
        assert!(without.retention.is_empty());
        let second = without.prometheus_text(Some(&first), 200);
        assert!(
            second.contains("vtop_scrub_retention_total{result=\"locked\"} 2\n"),
            "the totals survive a run that did not read retention: {second}"
        );
    }

    /// #710: the receipts' own retention family is carried over a run that
    /// did not read it, whichever check that run left out, and the four
    /// scrub families in one file keep their own numbers when it is parsed
    /// and rewritten (`vtop_scrub_receipts_total{` and
    /// `vtop_scrub_receipt_retention_total{` share a prefix up to `receipt`).
    #[test]
    fn a_run_that_read_no_receipt_retention_carries_its_totals_forward() {
        let at = |pairs: &[(RetentionResult, u64)]| -> BTreeMap<&'static str, u64> {
            RetentionResult::ALL
                .iter()
                .map(|r| {
                    let n = pairs.iter().find(|(p, _)| p == r).map_or(0, |(_, n)| *n);
                    (r.as_str(), n)
                })
                .collect()
        };
        let mut all_four = report(&[(ScrubResult::Ok, 5)], 0, true);
        all_four.retention = at(&[(RetentionResult::Locked, 5)]);
        all_four.receipts = ReceiptResult::ALL
            .iter()
            .map(|r| (r.as_str(), u64::from(*r == ReceiptResult::Ok) * 4))
            .collect();
        all_four.receipt_retention = at(&[
            (RetentionResult::Locked, 2),
            (RetentionResult::Missing, 1),
            (RetentionResult::ModeMismatch, 1),
        ]);
        let first = all_four.prometheus_text(None, 100);
        let family = |text: &str, name: &str| -> Vec<String> {
            text.lines()
                .filter(|l| l.starts_with(&format!("{name}{{")))
                .map(str::to_owned)
                .collect()
        };
        assert_eq!(
            family(&first, "vtop_scrub_receipt_retention_total"),
            [
                "vtop_scrub_receipt_retention_total{result=\"elapsed\"} 0",
                "vtop_scrub_receipt_retention_total{result=\"locked\"} 2",
                "vtop_scrub_receipt_retention_total{result=\"missing\"} 1",
                "vtop_scrub_receipt_retention_total{result=\"mode_mismatch\"} 1",
            ],
            "{first}"
        );

        // (a) neither check; (b) receipts without --check-retention; (c)
        // --check-retention without receipts.
        let neither = report(&[(ScrubResult::Ok, 1)], 0, true);
        let mut receipts_only = report(&[(ScrubResult::Ok, 1)], 0, true);
        receipts_only.receipts = ReceiptResult::ALL
            .iter()
            .map(|r| (r.as_str(), u64::from(*r == ReceiptResult::Ok)))
            .collect();
        let mut retention_only = report(&[(ScrubResult::Ok, 1)], 0, true);
        retention_only.retention = at(&[(RetentionResult::Locked, 1)]);
        for (case, run) in [
            ("neither", neither),
            ("receipts only", receipts_only),
            ("retention only", retention_only),
        ] {
            assert!(run.receipt_retention.is_empty(), "{case}");
            let next = run.prometheus_text(Some(&first), 200);
            assert_eq!(
                family(&next, "vtop_scrub_receipt_retention_total"),
                family(&first, "vtop_scrub_receipt_retention_total"),
                "{case}: {next}"
            );
        }

        // (d) A run of all four adds each family to its own totals.
        let second = all_four.prometheus_text(Some(&first), 300);
        for (name, line) in [
            ("vtop_scrub_objects_total", "{result=\"ok\"} 10"),
            ("vtop_scrub_retention_total", "{result=\"locked\"} 10"),
            ("vtop_scrub_receipts_total", "{result=\"ok\"} 8"),
            (
                "vtop_scrub_receipt_retention_total",
                "{result=\"locked\"} 4",
            ),
            (
                "vtop_scrub_receipt_retention_total",
                "{result=\"missing\"} 2",
            ),
        ] {
            assert!(
                second.contains(&format!("{name}{line}\n")),
                "{name}{line}: {second}"
            );
        }
    }

    /// #710: a receipt that is gone by the time its retention is read gets
    /// no verdict, and is not a read failure; the next run counts it
    /// missing.
    #[tokio::test]
    async fn a_receipt_gone_before_its_retention_read_gets_no_verdict() {
        let backend = vtop_upload::MockBackend::new();
        let found = receipt_retention_of(
            &backend,
            "s3://telemetry-raw/x/b.receipt.json",
            ObjectLockMode::Governance,
        )
        .await;
        assert_eq!(found, Ok(None));
    }
}
