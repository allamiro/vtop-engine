//! The S3 Object Lock profile's engine side (#684): what the engine says at
//! startup, its bucket preflight, and the check of a written object's
//! retention.

use std::collections::HashSet;
use std::sync::Mutex;

use vtop_core::config::{ObjectLockMode, VtopConfig};
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
    let bucket = config.upload.bucket.as_str();
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
            Err(e) => {
                return Err(VtopError::Config(bucket_unreadable(
                    bucket,
                    "the engine did not start",
                    &e,
                )))
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
             retention upload.object_lock sets; the batch was marked failed and will be \
             replayed; use the s3_native backend",
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
        format!(
            "{what} {uri}, reused from an earlier attempt, is not under the requested \
             retention ({describe}); the batch was marked failed and will be replayed; a \
             replay of the same bytes reuses this object again, so extend its retention with \
             PutObjectRetention, or delete that version if nothing protects it and the replay \
             will write a locked one"
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
             configuration and the credentials' s3:PutObjectRetention permission{hold}"
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
}
