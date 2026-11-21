//! The hardened manifest profile's bucket check (`upload.require_object_versioning`,
//! #135): what the engine says when a bucket cannot keep the manifest
//! versions it pins, at startup for a literal bucket and at the first upload
//! for a templated one (#722).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use vtop_core::config::VtopConfig;
use vtop_core::errors::VtopError;
use vtop_core::telemetry;
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

/// How long the engine waits before its first check of a bucket it stopped
/// writing to, and the longest it waits between checks: the wait doubles at
/// every check that still finds versioning off.
const RECHECK_FIRST: Duration = Duration::from_secs(5);
const RECHECK_CAP: Duration = Duration::from_secs(60);

/// What paused writes to a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PauseCause {
    /// A manifest PUT returned no object version to pin.
    ManifestPut,
    /// An object PUT returned no object version, and GetBucketVersioning
    /// then said versioning is not enabled.
    ObjectPut,
    /// GetBucketVersioning said versioning is not enabled, at the first
    /// upload to the bucket.
    Check,
}

/// Buckets the engine stopped writing to under
/// `upload.require_object_versioning`, because the bucket no longer keeps
/// object versions: a PUT returned no version to pin, or the store said
/// versioning is not enabled.
///
/// That is the bucket's state, not a transient upload failure: retrying the
/// batch writes another object or manifest the bucket cannot pin, and
/// commits nothing (the external tamper evaluation of v0.10.2 saw one new
/// manifest per retry). So nothing more is written to the bucket. Each batch
/// for it fails before its first PUT and is replayed, and the bucket's
/// versioning is checked again on a bounded backoff (5 s, doubling to 60 s);
/// writes resume once the store says it is enabled. Owned by [`crate::Engine`]
/// and shared into every pipeline, like the versioning preflight cache.
#[derive(Debug)]
pub struct VersioningPauses {
    first: Duration,
    cap: Duration,
    paused: Mutex<HashMap<String, Pause>>,
}

#[derive(Debug, Clone, Copy)]
struct Pause {
    since: Instant,
    next_check: Instant,
    /// The wait after the next check that finds versioning still off.
    wait: Duration,
    checks: u64,
    /// A batch holds the check ([`CheckClaim`]) and has not finished it.
    checking: bool,
}

/// What a batch may do with a bucket.
#[derive(Debug)]
pub(crate) enum Gate<'a> {
    /// Not paused: upload as usual.
    Open,
    /// Paused, and not yet due for a check or another batch is checking:
    /// write nothing.
    Wait { next_check_in: Duration },
    /// Paused and due: this batch checks the bucket's versioning, and every
    /// other batch waits until the claim is dropped, however long the
    /// store takes to answer (review of #883). Held for its `Drop` only.
    Check(#[allow(dead_code)] CheckClaim<'a>),
}

/// The one check of a paused bucket in flight. Dropping it, when the check
/// finished or the batch was cancelled, lets the next due batch check.
#[derive(Debug)]
pub(crate) struct CheckClaim<'a> {
    pauses: &'a VersioningPauses,
    bucket: String,
}

impl Drop for CheckClaim<'_> {
    fn drop(&mut self) {
        if let Some(pause) = self.pauses.paused.lock().unwrap().get_mut(&self.bucket) {
            pause.checking = false;
        }
    }
}

/// After a check that found versioning still off.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StillOff {
    pub next_check_in: Duration,
    pub checks: u64,
    pub paused_for: Duration,
}

impl Default for VersioningPauses {
    fn default() -> Self {
        Self {
            first: RECHECK_FIRST,
            cap: RECHECK_CAP,
            paused: Mutex::new(HashMap::new()),
        }
    }
}

impl VersioningPauses {
    pub(crate) fn gate(&self, bucket: &str) -> Gate<'_> {
        let mut paused = self.paused.lock().unwrap();
        let Some(pause) = paused.get_mut(bucket) else {
            return Gate::Open;
        };
        let now = Instant::now();
        if pause.checking || now < pause.next_check {
            return Gate::Wait {
                next_check_in: pause.next_check.saturating_duration_since(now),
            };
        }
        // Claimed by this batch until its check finishes: the others wait
        // for its answer, even past the backoff.
        pause.checking = true;
        pause.next_check = now + pause.wait;
        Gate::Check(CheckClaim {
            pauses: self,
            bucket: bucket.to_owned(),
        })
    }

    pub(crate) fn is_paused(&self, bucket: &str) -> bool {
        self.paused.lock().unwrap().contains_key(bucket)
    }

    /// Stop writing to `bucket`. `Some(first wait)` when it was not paused
    /// already.
    pub(crate) fn pause(&self, bucket: &str) -> Option<Duration> {
        let mut paused = self.paused.lock().unwrap();
        if paused.contains_key(bucket) {
            return None;
        }
        let now = Instant::now();
        paused.insert(
            bucket.to_owned(),
            Pause {
                since: now,
                next_check: now + self.first,
                wait: (self.first * 2).min(self.cap),
                checks: 0,
                checking: false,
            },
        );
        if let Some(mx) = telemetry::metrics() {
            mx.bucket_versioning_paused.inc();
        }
        Some(self.first)
    }

    /// A check found versioning still off: the next one waits longer, up to
    /// the cap.
    pub(crate) fn still_off(&self, bucket: &str) -> StillOff {
        let mut paused = self.paused.lock().unwrap();
        let now = Instant::now();
        let pause = paused.entry(bucket.to_owned()).or_insert_with(|| {
            if let Some(mx) = telemetry::metrics() {
                mx.bucket_versioning_paused.inc();
            }
            Pause {
                since: now,
                next_check: now,
                wait: self.first,
                checks: 0,
                checking: false,
            }
        });
        let wait = pause.wait;
        pause.next_check = now + wait;
        pause.wait = (wait * 2).min(self.cap);
        pause.checks += 1;
        StillOff {
            next_check_in: wait,
            checks: pause.checks,
            paused_for: now - pause.since,
        }
    }

    /// Versioning is enabled again: writes resume. How long they were
    /// paused, when they were.
    pub(crate) fn resume(&self, bucket: &str) -> Option<Duration> {
        let pause = self.paused.lock().unwrap().remove(bucket)?;
        if let Some(mx) = telemetry::metrics() {
            mx.bucket_versioning_paused.dec();
        }
        Some(pause.since.elapsed())
    }

    /// How many buckets are paused.
    #[cfg(test)]
    pub(crate) fn paused(&self) -> usize {
        self.paused.lock().unwrap().len()
    }

    /// Make every paused bucket due for its check now.
    #[cfg(test)]
    pub(crate) fn make_due(&self) {
        let now = Instant::now();
        for pause in self.paused.lock().unwrap().values_mut() {
            pause.next_check = now;
        }
    }
}

/// The ERROR when writes to a bucket stop: what happened, what the engine
/// does, and what to do.
pub(crate) fn paused_message(bucket: &str, cause: PauseCause, first: Duration) -> String {
    let what = match cause {
        PauseCause::ManifestPut => format!(
            "a manifest PUT to bucket {bucket} returned no object version to pin, so the \
             bucket no longer keeps versions (its versioning was suspended or turned off)"
        ),
        PauseCause::ObjectPut => format!(
            "an object PUT to bucket {bucket} returned no object version, and the store says \
             the bucket's versioning is not enabled (it was suspended or turned off)"
        ),
        PauseCause::Check => format!(
            "the store says bucket {bucket} does not have versioning enabled, so it would not \
             keep the manifest versions the hardened profile pins"
        ),
    };
    format!(
        "{what}; upload.require_object_versioning is set, so the engine stopped writing to the \
         bucket: nothing in it commits, each batch for it fails before writing anything and is \
         replayed, and the bucket's versioning is checked again in {}s (then at most every {}s), \
         and writing resumes once it is Enabled. To resume, re-enable versioning on bucket \
         {bucket} (PutBucketVersioning with Status=Enabled), or set \
         upload.require_object_versioning: false",
        first.as_secs(),
        RECHECK_CAP.as_secs()
    )
}

/// The ERROR when a check finds versioning still off.
pub(crate) fn still_off_message(bucket: &str, off: StillOff, suppressed: u64) -> String {
    format!(
        "bucket {bucket} still does not have versioning enabled ({} checks over {}s); the \
         engine is still not writing to it and nothing in it commits; next check in {}s. \
         Re-enable versioning on bucket {bucket} (PutBucketVersioning with Status=Enabled), or \
         set upload.require_object_versioning: false ({suppressed} more like this suppressed \
         since the last notice)",
        off.checks,
        off.paused_for.as_secs(),
        off.next_check_in.as_secs()
    )
}

/// Why a batch for a paused bucket failed without writing.
pub(crate) fn paused_batch(bucket: &str, next_check_in: Duration) -> String {
    format!(
        "writes to bucket {bucket} are paused: it does not keep object versions and \
         upload.require_object_versioning is set; nothing was written and the batch will be \
         replayed; the bucket's versioning is checked again in {}s",
        next_check_in.as_secs().max(1)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Review of #883: a check that takes longer than the backoff keeps the
    /// claim, so a second batch due after the backoff still waits instead of
    /// sending a second request; dropping the claim, when the check is done
    /// or its batch is cancelled, lets the next due batch check.
    #[test]
    fn a_check_in_flight_keeps_every_other_batch_waiting_past_the_backoff() {
        let pauses = VersioningPauses::default();
        assert!(matches!(pauses.gate("b"), Gate::Open));
        assert_eq!(pauses.pause("b"), Some(RECHECK_FIRST));
        assert!(matches!(pauses.gate("b"), Gate::Wait { .. }));

        pauses.make_due();
        let first = pauses.gate("b");
        assert!(matches!(first, Gate::Check(_)), "{first:?}");
        // The store has not answered and the backoff has run out.
        pauses.make_due();
        assert!(
            matches!(pauses.gate("b"), Gate::Wait { .. }),
            "a second check while the first is in flight"
        );
        // Another bucket is not held up by it.
        assert!(matches!(pauses.gate("other"), Gate::Open));

        // The first batch is cancelled before its answer: the claim goes.
        drop(first);
        pauses.make_due();
        let second = pauses.gate("b");
        assert!(matches!(second, Gate::Check(_)), "{second:?}");
        let off = pauses.still_off("b");
        assert_eq!(off.checks, 1);
        drop(second);
        assert!(matches!(pauses.gate("b"), Gate::Wait { .. }), "not due yet");
        pauses.make_due();
        let third = pauses.gate("b");
        assert!(matches!(third, Gate::Check(_)));
        assert!(pauses.resume("b").is_some());
        drop(third);
        assert!(matches!(pauses.gate("b"), Gate::Open));
    }
}
