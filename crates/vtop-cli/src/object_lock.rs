//! The S3 Object Lock profile's engine side (#684): what the engine says at
//! startup, its bucket preflight, and the check of a written object's
//! retention.

use vtop_core::config::{ObjectLockMode, VtopConfig};
use vtop_core::errors::VtopError;

/// One startup line: its level and its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub warn: bool,
    pub text: String,
}

/// What the engine says about the profile when it starts.
pub fn startup_notices(_config: &VtopConfig) -> Vec<Notice> {
    // SKELETON (#684)
    Vec::new()
}

/// The mode `vtopctl scrub --check-retention` expects.
pub fn scrub_retention_mode(
    _config: &VtopConfig,
    _check_retention: bool,
) -> Result<Option<ObjectLockMode>, VtopError> {
    // SKELETON (#684)
    Ok(None)
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
