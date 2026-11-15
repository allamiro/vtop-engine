//! The S3 Object Lock profile's engine side (#684): what the engine says at
//! startup, its bucket preflight, and the check of a written object's
//! retention.

use std::collections::HashSet;
use std::sync::Mutex;

use vtop_core::config::{ObjectLockConfig, ObjectLockMode, VtopConfig};
use vtop_core::errors::VtopError;
use vtop_core::telemetry;
use vtop_upload::multipart::redact_urls;
use vtop_upload::{ObjectLockPolicy, RetentionCheck, RetentionExpectation, UploadBackend};

/// One startup line: its level and its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub warn: bool,
    pub text: String,
}

/// What the engine says about the profile when it starts: the settings and
/// the bucket at INFO, and for `compliance` a WARN that it is irreversible
/// and that failed attempts' objects are locked too. Nothing without it.
pub fn startup_notices(config: &VtopConfig) -> Vec<Notice> {
    let Some(lock) = &config.upload.object_lock else {
        return Vec::new();
    };
    let mut notices = vec![Notice {
        warn: false,
        text: format!(
            "object lock enabled: mode={} retain_days={} legal_hold={} on bucket {}",
            lock.mode, lock.retain_days, lock.legal_hold, config.upload.bucket
        ),
    }];
    if lock.mode == ObjectLockMode::Compliance {
        notices.push(Notice {
            warn: true,
            text: format!(
                "upload.object_lock.mode is compliance: compliance retention cannot be \
                 shortened or removed by anyone, including the account root, until the date \
                 passes ({} days after each write); objects from failed attempts are locked \
                 too, and their storage is billed until then. Nothing to do if that is \
                 intended",
                lock.retain_days
            ),
        });
    }
    notices
}

/// The mode `vtopctl scrub --check-retention` expects (#684): the one
/// `upload.object_lock` configures. The flag without the block is refused:
/// there is no mode to compare against.
pub fn scrub_retention_mode(
    config: &VtopConfig,
    check_retention: bool,
) -> Result<Option<ObjectLockMode>, VtopError> {
    if !check_retention {
        return Ok(None);
    }
    match &config.upload.object_lock {
        Some(lock) => Ok(Some(lock.mode)),
        None => Err(VtopError::Config(
            "--check-retention was given, but the config sets no upload.object_lock, so there \
             is no retention mode to expect; nothing was checked. Pass the config the engine \
             runs with, or drop --check-retention"
                .into(),
        )),
    }
}

/// Whether the bucket name is a template (`telemetry-{format}`), resolved
/// per batch: such a bucket is checked at its first upload, not at startup.
pub(crate) fn bucket_is_templated(bucket: &str) -> bool {
    bucket.contains('{')
}

/// The refusal for a bucket without Object Lock: what happened, what the
/// engine did (`did`), what to do.
pub(crate) fn bucket_refusal(bucket: &str, did: &str) -> String {
    format!(
        "bucket {bucket} does not have Object Lock enabled; {did}. Object Lock can only be \
         enabled when a bucket is created: create a new bucket with it and point upload.bucket \
         at it"
    )
}

/// The message for a bucket whose Object Lock configuration could not be
/// read at all.
pub(crate) fn bucket_unreadable(bucket: &str, did: &str, error: &VtopError) -> String {
    format!(
        "the Object Lock configuration of bucket {bucket} could not be read ({}); {did}. Grant \
         the engine's identity s3:GetBucketObjectLockConfiguration on the bucket, and check \
         the store's health if it has it",
        redact_urls(&error.to_string())
    )
}

/// Count one check by its bounded result.
pub(crate) fn count(tenant: &str, result: &str) {
    debug_assert!(
        telemetry::OBJECT_LOCK_CHECK_RESULTS.contains(&result),
        "{result} is not a pinned result"
    );
    if let Some(mx) = telemetry::metrics() {
        mx.object_lock_checks_total
            .with_label_values(&[tenant, result])
            .inc();
    }
}

/// Set `vtop_object_lock_mode` once at startup.
fn set_mode_gauge(config: &VtopConfig) {
    let Some(mx) = telemetry::metrics() else {
        return;
    };
    let on = config
        .upload
        .object_lock
        .as_ref()
        .map_or("off", |lock| lock.mode.as_str());
    for mode in telemetry::OBJECT_LOCK_MODES {
        mx.object_lock_mode
            .with_label_values(&[config.engine.tenant.as_str(), mode])
            .set(i64::from(mode == on));
    }
    // Every result at zero, so an alert on increase() sees the first one.
    for result in telemetry::OBJECT_LOCK_CHECK_RESULTS {
        mx.object_lock_checks_total
            .with_label_values(&[config.engine.tenant.as_str(), result])
            .inc_by(0);
    }
}

/// The startup check (#684): the backend applies the configured lock, and a
/// literal bucket has Object Lock enabled. A templated bucket is checked at
/// the first upload to each bucket instead. Says what it found, and skips
/// everything when the profile is unset.
pub(crate) async fn preflight(
    config: &VtopConfig,
    backend: &dyn UploadBackend,
    locked_buckets: &Mutex<HashSet<String>>,
) -> Result<(), VtopError> {
    set_mode_gauge(config);
    let Some(lock) = &config.upload.object_lock else {
        return Ok(());
    };
    let applied = backend.object_lock();
    if !applied.is_some_and(|policy| policy == ObjectLockPolicy::from_config(lock)) {
        return Err(VtopError::Config(format!(
            "upload.object_lock is set, but the {} upload backend does not apply it (it would \
             write {}); the engine did not start. Use the s3_native backend, which writes every \
             object under the configured lock",
            backend.backend_name(),
            match applied {
                Some(policy) => format!("under mode {} instead", policy.mode),
                None => "unlocked objects".to_owned(),
            }
        )));
    }
    // The name batches are written to: a literal bucket is sanitized before
    // use (review of #708), so that, not the raw spelling, is what is checked
    // and cached.
    let resolved;
    let bucket = if bucket_is_templated(&config.upload.bucket) {
        config.upload.bucket.as_str()
    } else {
        resolved = vtop_core::partitioning::sanitize_bucket(&config.upload.bucket);
        resolved.as_str()
    };
    let checked = if bucket_is_templated(bucket) {
        "(each bucket the template names is checked at its first upload)"
    } else {
        match backend.bucket_object_lock_enabled(bucket).await {
            Ok(true) => {
                locked_buckets.lock().unwrap().insert(bucket.to_owned());
                "(checked)"
            }
            Ok(false) => {
                return Err(VtopError::Config(bucket_refusal(
                    bucket,
                    "the engine did not start",
                )))
            }
            // A check the store could not answer proves nothing either way,
            // and is no reason to stay down (review of #708): the bucket is
            // left unchecked, and the pipeline checks it before the first
            // upload to it, failing that batch if it is not locked.
            Err(e) => {
                tracing::warn!(
                    bucket,
                    "{}",
                    bucket_unreadable(
                        bucket,
                        "the engine started anyway, and the bucket is checked again before the \
                         first upload to it",
                        &e,
                    )
                );
                "(not yet checked: the store did not answer)"
            }
        }
    };
    for notice in startup_notices(config) {
        if notice.warn {
            tracing::warn!("{}", notice.text);
        } else {
            tracing::info!(
                mode = lock.mode.as_str(),
                retain_days = lock.retain_days,
                legal_hold = lock.legal_hold,
                bucket,
                "{} {checked}",
                notice.text
            );
        }
    }
    Ok(())
}

/// What a reused object's retention must meet (#684, #709): the configured
/// mode, a date no earlier than `now_secs`, and the legal hold when one is
/// configured. One place, so the reuse decision and the check before the
/// commit cannot drift apart.
pub(crate) fn reuse_expectation(lock: &ObjectLockConfig, now_secs: i64) -> RetentionExpectation {
    RetentionExpectation {
        mode: lock.mode,
        retain_until_at_least_unix_secs: now_secs,
        legal_hold: lock.legal_hold,
    }
}

/// What a replay does with an object already archived at a key it would
/// reuse, when the profile is set (#709).
#[derive(Debug)]
pub(crate) enum Reuse {
    /// Locked as configured: reuse it. The check before the commit reads it
    /// again.
    Locked,
    /// Not under the configured lock: do not reuse it, and leave it alone.
    /// `decision` is the bounded value of `vtop_object_key_decisions_total`,
    /// `why` says what was found.
    Unlocked { decision: &'static str, why: String },
    /// The retention could not be read. Not a reason to fall back: during a
    /// store outage every replay would write another object. The batch
    /// fails with this message and is replayed.
    Unanswered(String),
    /// The store throttled the read. Handled like a throttled probe of the
    /// key (review of #757): the batch fails and is retried, counted as
    /// `probe_throttled`, never as a failed check.
    Throttled(String),
}

/// The fallback decision and its reason for a retention found short of
/// `lock` (#709), `None` for one that passed. The mapping, in one place:
/// every shortfall `judge_retention` can return maps to exactly one value.
/// `Unsupported` is no verdict about the lock at all, so it is `None` here
/// and [`judge_reuse`] treats it as a read that could not be made.
pub(crate) fn fallback_decision(
    found: &RetentionCheck,
    lock: &ObjectLockConfig,
) -> Option<(&'static str, String)> {
    match found {
        RetentionCheck::Ok { .. } | RetentionCheck::Unsupported => None,
        RetentionCheck::Missing => Some((
            "fallback_unlocked",
            "it carries no Object Lock retention (it was stored before upload.object_lock was \
             set, or by a store that did not keep the lock headers)"
                .to_owned(),
        )),
        RetentionCheck::TooShort {
            retain_until_unix_secs,
        } => Some((
            "fallback_lock_elapsed",
            format!(
                "its Object Lock retention ended at {}",
                vtop_upload::format_unix_secs(*retain_until_unix_secs)
            ),
        )),
        RetentionCheck::ModeMismatch { found } => Some((
            "fallback_lock_mode",
            format!(
                "it is locked in mode {found}, not the configured {}",
                lock.mode
            ),
        )),
        RetentionCheck::LegalHoldMissing => Some((
            "fallback_hold_missing",
            "it has no legal hold, which upload.object_lock.legal_hold asks for".to_owned(),
        )),
    }
}

/// Judge whether an object already archived at `uri` may be reused under
/// the profile (#709), from the read of its retention against
/// [`reuse_expectation`]. A shortfall is a key decision, which the caller
/// counts in `vtop_object_key_decisions_total`; it is never a failed check,
/// since nothing about the batch failed, and a pass is not counted here
/// either (the check before the commit reads a reused object again and
/// counts it). A read that failed is counted and logged as `check` counts
/// and logs one.
pub(crate) fn judge_reuse(
    read: Result<RetentionCheck, VtopError>,
    batch_id: &str,
    tenant: &str,
    uri: &str,
    lock: &ObjectLockConfig,
) -> Reuse {
    let why = match read {
        Err(e) if e.is_upload_throttle() => {
            return Reuse::Throttled(format!(
                "the retention read of {uri} was throttled: {}",
                redact_urls(&e.to_string())
            ))
        }
        Ok(RetentionCheck::Ok { .. }) => return Reuse::Locked,
        Ok(found) => match fallback_decision(&found, lock) {
            Some((decision, why)) => return Reuse::Unlocked { decision, why },
            None => "the backend cannot read an object's retention".to_owned(),
        },
        Err(e) => redact_urls(&e.to_string()),
    };
    count(tenant, "error");
    let message = format!(
        "the retention of object {uri}, already archived for this batch's range, could not be \
         read ({why}); it was neither reused nor replaced by a fresh object, since a check the \
         store did not answer says nothing about the lock, and no object or manifest was \
         written; the batch was marked failed and will be replayed; check the store's health \
         and the credentials' s3:GetObjectRetention permission (and s3:GetObjectLegalHold \
         with upload.object_lock.legal_hold)"
    );
    tracing::error!(
        event = "object_lock_check_failed",
        batch_id,
        object_uri = uri,
        what = "object",
        result = "error",
        "{message}"
    );
    Reuse::Unanswered(message)
}

/// What the operator should do about a fallback of `decision` (#709). The
/// batch itself needs nothing: the fresh object is locked as configured.
pub(crate) fn fallback_advice(decision: &str) -> &'static str {
    match decision {
        "fallback_lock_elapsed" => {
            "nothing is lost; the old object is no longer locked and can be removed by a \
             lifecycle rule or a delete; if this repeats for ranges that should still be inside \
             their retention, find what stalled the pipeline for upload.object_lock.retain_days"
        }
        "fallback_lock_mode" => {
            "nothing to do if upload.object_lock.mode was changed on purpose; a compliance lock \
             cannot be shortened, so the old object stays until its date"
        }
        "fallback_hold_missing" => {
            "nothing to do; to hold the old object as well, place a legal hold on it \
             (s3:PutObjectLegalHold)"
        }
        _ => {
            "nothing is lost; run `vtopctl scrub --check-retention` to find committed objects \
             in the same state, and check who can bypass or remove retention, or whether an \
             engine older than v0.10.0 wrote it"
        }
    }
}

/// What one retention check of a written or reused object is about.
pub(crate) struct Subject<'a> {
    pub batch_id: &'a str,
    pub tenant: &'a str,
    /// `object` or `manifest`.
    pub what: &'static str,
    pub uri: &'a str,
    pub version_id: Option<&'a str>,
    /// A reused object (#626): checked with the weaker bound, and a replay
    /// meets it again.
    pub reused: bool,
    pub legal_hold_configured: bool,
}

/// Why a check failed, for the batch's error and its counter.
pub(crate) struct Failed {
    pub message: String,
    /// The check found less than was asked for (counted as a verification
    /// failure); false for a check that could not be made.
    pub verification_failure: bool,
}

/// Check one stored object's or manifest's retention (#684), count it, and
/// log a failure with its three parts. `Err` fails the batch.
pub(crate) async fn check(
    backend: &dyn UploadBackend,
    subject: Subject<'_>,
    expected: Option<RetentionExpectation>,
) -> Result<(), Failed> {
    let Subject {
        batch_id,
        tenant,
        what,
        uri,
        version_id,
        reused,
        legal_hold_configured,
    } = subject;
    let version = version_id
        .map(|v| format!(" (version {v})"))
        .unwrap_or_default();
    let Some(expected) = expected else {
        // The backend wrote without asking for a lock, though the profile is
        // set: nothing to compare the store against, and nothing locked.
        count(tenant, "error");
        let message = format!(
            "{what} {uri}{version} was written by the {} backend without asking for the \
             retention upload.object_lock sets (a multipart upload completed by a run without \
             the profile, or a backend that does not apply it); the batch was marked failed and \
             will be replayed, and the replay checks the stored object itself; nothing to do \
             unless it repeats, then check that the engine runs s3_native with \
             upload.object_lock",
            backend.backend_name()
        );
        tracing::error!(
            event = "object_lock_check_failed",
            batch_id,
            object_uri = uri,
            what,
            result = "error",
            "{message}"
        );
        return Err(Failed {
            message,
            verification_failure: false,
        });
    };
    let found = match backend.verify_retention(uri, version_id, &expected).await {
        Ok(found) => found,
        Err(e) => {
            count(tenant, "error");
            let message = format!(
                "the retention of {what} {uri}{version} could not be read ({}); the batch was \
                 marked failed and will be replayed; check the store's health and the \
                 credentials' s3:GetObjectRetention permission",
                redact_urls(&e.to_string())
            );
            tracing::error!(
                event = "object_lock_check_failed",
                batch_id,
                object_uri = uri,
                what,
                result = "error",
                "{message}"
            );
            return Err(Failed {
                message,
                verification_failure: false,
            });
        }
    };
    count(tenant, found.label());
    if found.passed() {
        return Ok(());
    }
    let describe = found.describe(&expected);
    let message = if reused {
        // The replay checked this object before reusing it (#709), so its
        // lock or hold ended, or was removed, while this batch was in
        // flight. The replay sees that and writes a fresh locked object.
        format!(
            "{what} {uri}, reused from an earlier attempt, is not under the requested \
             retention ({describe}), though it was when this batch chose to reuse it; the batch \
             was marked failed and will be replayed, and the replay writes the batch as a fresh \
             locked object under its attempt key, leaving this one as it is (or, if the lock \
             cannot be read, fails without writing); nothing to do unless it repeats, then check \
             who changes the retention of archived objects and the credentials' \
             s3:GetObjectRetention permission"
        )
    } else {
        let hold = if legal_hold_configured {
            " (and s3:PutObjectLegalHold for the legal hold)"
        } else {
            ""
        };
        format!(
            "{what} {uri}{version} is not under the requested retention ({describe}); the \
             batch was marked failed and will be replayed; check the bucket's Object Lock \
             configuration and the credentials' s3:PutObjectRetention permission{hold}, and \
             s3:GetObjectRetention, without which the store answers a HEAD with no lock at all"
        )
    };
    let unsupported = matches!(found, RetentionCheck::Unsupported);
    tracing::error!(
        event = "object_lock_check_failed",
        batch_id,
        object_uri = uri,
        what,
        result = found.label(),
        "{message}"
    );
    Err(Failed {
        message,
        verification_failure: !unsupported,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use vtop_core::config::ObjectLockConfig;

    fn config(lock: Option<ObjectLockConfig>) -> VtopConfig {
        let mut cfg = crate::testkit::file_config("/tmp/w", "sqlite::memory:", vec![], "s3_native");
        cfg.upload.bucket = "telemetry-locked".into();
        cfg.upload.require_object_versioning = lock.is_some();
        cfg.upload.object_lock = lock;
        cfg
    }

    #[test]
    fn startup_names_the_mode_the_days_the_hold_and_the_bucket() {
        let notices = startup_notices(&config(Some(ObjectLockConfig {
            mode: ObjectLockMode::Governance,
            retain_days: 90,
            legal_hold: false,
        })));
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert!(!notices[0].warn);
        for part in [
            "object lock enabled",
            "mode=governance",
            "retain_days=90",
            "legal_hold=false",
            "bucket telemetry-locked",
        ] {
            assert!(
                notices[0].text.contains(part),
                "{:?} should say {part:?}",
                notices[0].text
            );
        }
    }

    #[test]
    fn compliance_warns_that_nobody_can_shorten_it_and_failed_attempts_are_locked_too() {
        let notices = startup_notices(&config(Some(ObjectLockConfig {
            mode: ObjectLockMode::Compliance,
            retain_days: 365,
            legal_hold: true,
        })));
        let warn = notices
            .iter()
            .find(|n| n.warn)
            .unwrap_or_else(|| panic!("compliance must warn: {notices:?}"));
        for part in [
            "compliance retention cannot be shortened or removed by anyone",
            "including the account root",
            "objects from failed attempts are locked too",
        ] {
            assert!(
                warn.text.contains(part),
                "{:?} should say {part:?}",
                warn.text
            );
        }
    }

    #[test]
    fn without_the_profile_nothing_is_said() {
        assert!(startup_notices(&config(None)).is_empty());
    }

    #[test]
    fn scrub_check_retention_needs_the_profile_to_know_the_mode() {
        let err = scrub_retention_mode(&config(None), true)
            .expect_err("--check-retention without upload.object_lock has no mode to expect");
        let msg = err.to_string();
        for part in [
            "--check-retention",
            "upload.object_lock",
            "nothing was checked",
        ] {
            assert!(msg.contains(part), "{msg:?} should say {part:?}");
        }
        let locked = config(Some(ObjectLockConfig {
            mode: ObjectLockMode::Compliance,
            retain_days: 30,
            legal_hold: false,
        }));
        assert_eq!(
            scrub_retention_mode(&locked, true).unwrap(),
            Some(ObjectLockMode::Compliance)
        );
        assert_eq!(scrub_retention_mode(&locked, false).unwrap(), None);
    }

    fn governance(legal_hold: bool) -> ObjectLockConfig {
        ObjectLockConfig {
            mode: ObjectLockMode::Governance,
            retain_days: 30,
            legal_hold,
        }
    }

    /// #709: every shortfall maps to exactly one bounded fallback value, a
    /// pass and `Unsupported` to none.
    #[test]
    fn every_retention_shortfall_maps_to_one_pinned_fallback_decision() {
        let lock = governance(true);
        let cases = [
            (
                RetentionCheck::Ok {
                    retain_until_unix_secs: 1,
                },
                None,
            ),
            (RetentionCheck::Unsupported, None),
            (RetentionCheck::Missing, Some("fallback_unlocked")),
            (
                RetentionCheck::TooShort {
                    retain_until_unix_secs: 1_000_000_000,
                },
                Some("fallback_lock_elapsed"),
            ),
            (
                RetentionCheck::ModeMismatch {
                    found: "COMPLIANCE".into(),
                },
                Some("fallback_lock_mode"),
            ),
            (
                RetentionCheck::LegalHoldMissing,
                Some("fallback_hold_missing"),
            ),
        ];
        let mut seen = HashSet::new();
        for (found, want) in cases {
            let got = fallback_decision(&found, &lock);
            assert_eq!(got.as_ref().map(|(d, _)| *d), want, "{found:?}");
            if let Some((decision, why)) = got {
                assert!(telemetry::OBJECT_KEY_DECISIONS.contains(&decision));
                assert!(!why.is_empty());
                assert!(seen.insert(decision), "{decision} maps from two verdicts");
            }
        }
        assert!(fallback_decision(
            &RetentionCheck::TooShort {
                retain_until_unix_secs: 1_000_000_000
            },
            &lock
        )
        .unwrap()
        .1
        .contains("2001-09-09T01:46:40Z"));
    }

    /// #709: a reused object's lock decides reuse or fallback; a read the
    /// store cannot answer, or a backend that cannot read retention, is
    /// neither, and its message is redacted and says nothing was written.
    #[test]
    fn a_read_that_fails_is_neither_reuse_nor_fallback() {
        let lock = governance(false);
        let uri = "s3://telemetry-locked/x/obj.raw";
        assert!(matches!(
            judge_reuse(
                Ok(RetentionCheck::Ok {
                    retain_until_unix_secs: 9
                }),
                "b",
                "t-j",
                uri,
                &lock
            ),
            Reuse::Locked
        ));
        assert!(matches!(
            judge_reuse(Ok(RetentionCheck::Missing), "b", "t-j", uri, &lock),
            Reuse::Unlocked {
                decision: "fallback_unlocked",
                ..
            }
        ));
        for read in [
            Err(VtopError::Upload(
                "mock head_object https://s/x?X-Amz-Signature=deadbeef: 503".into(),
            )),
            Ok(RetentionCheck::Unsupported),
        ] {
            match judge_reuse(read, "b", "t-j", uri, &lock) {
                Reuse::Unanswered(message) => {
                    for part in [
                        uri,
                        "could not be read",
                        "it was neither reused nor replaced by a fresh object",
                        "no object or manifest was written",
                        "the batch was marked failed and will be replayed",
                        "s3:GetObjectRetention",
                    ] {
                        assert!(message.contains(part), "{message:?} should say {part:?}");
                    }
                    assert!(!message.contains("deadbeef"), "{message}");
                }
                other => panic!("a failed read must not decide reuse or fallback: {other:?}"),
            }
        }
    }

    /// Review of #757: a throttled read at the reuse decision is a throttle,
    /// like the probe one request earlier, not a failed check that pages.
    #[test]
    fn a_throttled_read_is_neither_a_verdict_nor_a_failed_check() {
        let mx = telemetry::init().unwrap();
        let errors = || {
            mx.object_lock_checks_total
                .with_label_values(&["t-throttled", "error"])
                .get()
        };
        let before = errors();
        let read = Err(VtopError::UploadThrottled(
            "head_object https://s/x?X-Amz-Signature=deadbeef: SlowDown (http 503)".into(),
        ));
        match judge_reuse(read, "b", "t-throttled", "s3://b/k", &governance(false)) {
            Reuse::Throttled(message) => {
                assert!(message.contains("throttled"), "{message}");
                assert!(!message.contains("deadbeef"), "{message}");
            }
            other => panic!("a throttle is not a verdict: {other:?}"),
        }
        assert_eq!(errors(), before, "not counted as a failed check");
    }

    /// A reused object that passed the check before reuse and fails the one
    /// before the commit lost its lock while the batch was in flight (#709):
    /// the message says the replay writes a fresh locked object, not that an
    /// operator must fix the old one.
    #[tokio::test]
    async fn a_reused_object_that_loses_its_lock_in_flight_says_the_replay_falls_back() {
        use vtop_upload::StoredRetention;
        let uri = "s3://telemetry-locked/x/obj.raw";
        let ahead = 4_000_000_000;
        for (kept, hold) in [
            (StoredRetention::default(), false),
            (
                StoredRetention {
                    mode: Some("GOVERNANCE".into()),
                    retain_until_unix_secs: Some(1_000_000_000),
                    legal_hold: Some(false),
                },
                false,
            ),
            (
                StoredRetention {
                    mode: Some("GOVERNANCE".into()),
                    retain_until_unix_secs: Some(ahead),
                    legal_hold: Some(false),
                },
                true,
            ),
        ] {
            let backend = vtop_upload::MockBackend::new();
            backend.add_unlocked_version(uri, b"bytes".to_vec());
            backend.set_stored_retention(uri, None, kept.clone());
            let failed = check(
                &backend,
                Subject {
                    batch_id: "b1",
                    tenant: "t-inflight",
                    what: "object",
                    uri,
                    version_id: None,
                    reused: true,
                    legal_hold_configured: hold,
                },
                Some(reuse_expectation(&governance(hold), 2_000_000_000)),
            )
            .await
            .expect_err("a reused object short of the lock fails the check before the commit");
            assert!(failed.verification_failure);
            for part in [
                "reused from an earlier attempt",
                "though it was when this batch chose to reuse it",
                "the batch was marked failed and will be replayed",
                "the replay writes the batch as a fresh locked object under its attempt key",
                "nothing to do unless it repeats",
            ] {
                assert!(
                    failed.message.contains(part),
                    "{kept:?}: {:?} should say {part:?}",
                    failed.message
                );
            }
            for stale in [
                "reuses this object again",
                "delete that version",
                "PutObjectLegalHold",
            ] {
                assert!(
                    !failed.message.contains(stale),
                    "{kept:?}: {:?} still says {stale:?}",
                    failed.message
                );
            }
        }
    }
}
