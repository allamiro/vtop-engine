//! # vtop-upload
//!
//! Pluggable S3-compatible upload backends. The native [`s3_native`] backend is
//! the primary production backend; [`s3cmd_backend`], [`awscli_backend`], and
//! [`minio_backend`] are compatibility-mode backends that shell out to external
//! tools. Every backend verifies object integrity, and the engine must not
//! commit source progress until verification passes.

pub mod awscli_backend;
pub mod base;
mod command;
pub mod localfs_backend;
pub mod minio_backend;
pub mod mock;
pub mod multipart;
pub mod s3_native;
pub mod s3cmd_backend;

pub use base::{is_throttle_code, is_throttle_status, looks_throttled, THROTTLE_ERROR_CODES};
pub use base::{
    ObjectChecksum, ObjectHead, StoredManifest, StoredObject, UploadBackend, UploadedPart,
    VerificationResult,
};
pub use mock::MockBackend;
pub use multipart::{
    abort_session, cleanup_abandoned, upload_resumable, MultipartFence, MultipartUploadConfig,
};

use std::sync::Arc;
use vtop_core::config::UploadConfig;
use vtop_core::errors::VtopError;

/// Construct the upload backend named in config. Returns a trait object so the
/// engine is backend-agnostic.
pub async fn build_backend(cfg: &UploadConfig) -> Result<Arc<dyn UploadBackend>, VtopError> {
    // The tuning map is validated HERE, not only in `VtopConfig::validate`
    // (review): this is the one door every consumer of an UploadConfig goes
    // through. `vtopctl tier` reads a bare UploadConfig from its
    // --upload-config file and never builds a VtopConfig at all, so while the
    // checks lived only in the outer validate a typo such as
    // `transports.tcp_tsl.part_size_bytes` was accepted and the copy ran on the
    // legacy multipart defaults — tuning recorded and applied by nothing. The
    // method is backend-gated, so this costs nothing for the other backends.
    cfg.validate_transports()?;
    refuse_an_egress_cap_the_backend_cannot_honour(cfg)?;
    let backend: Arc<dyn UploadBackend> = match cfg.backend.as_str() {
        "s3_native" => {
            let s3cfg = s3_native::config_from_upload(cfg);
            Arc::new(s3_native::S3NativeBackend::new(&s3cfg).await?)
        }
        "s3cmd" => {
            let command = command::CommandPolicy::from_config(cfg, "s3cmd")?;
            command.verify_version("s3cmd version").await?;
            Arc::new(s3cmd_backend::S3cmdBackend::new(
                command,
                cfg.profile.clone(),
            ))
        }
        "awscli" => {
            let command = command::CommandPolicy::from_config(cfg, "aws cli")?;
            command.verify_version("aws-cli/").await?;
            Arc::new(awscli_backend::AwsCliBackend::new(
                command,
                cfg.endpoint_url.clone(),
                cfg.profile.clone(),
            ))
        }
        "minio" => {
            let command = command::CommandPolicy::from_config(cfg, "mc")?;
            command.verify_version("mc version").await?;
            Arc::new(minio_backend::MinioBackend::new(
                command,
                cfg.profile.clone().unwrap_or_else(|| "local".to_string()),
            ))
        }
        "localfs" => {
            let root = cfg.local_path.clone().ok_or_else(|| {
                VtopError::Config("localfs backend requires upload.local_path".into())
            })?;
            Arc::new(localfs_backend::LocalFsBackend::new(root))
        }
        "mock" => Arc::new(MockBackend::new()),
        // Benchmark/fault-injection backends.
        "mock_fail" => Arc::new(MockBackend::failing()),
        "mock_limited" => Arc::new(MockBackend::limited()),
        other => {
            return Err(VtopError::Config(format!(
                "unknown upload backend: {other} (expected s3_native|s3cmd|awscli|minio|localfs|mock|mock_fail|mock_limited)"
            )))
        }
    };
    Ok(backend)
}
/// Refuse `upload.max_egress_bytes_per_second` on a backend that cannot be held
/// under it, naming the cap and the backend (#481) — BEFORE anything is built,
/// so a compatibility backend's binary is never even probed for a
/// configuration that cannot run as written.
///
/// Only `s3_native` owns the request bodies it sends, so only it can pace
/// them; which of ITS transports can be held under the cap is decided in
/// `S3NativeBackend::new`, per transport. A cap accepted by a backend that
/// never applies it is the one outcome this must not allow: the operator
/// would believe the uplink protected while a backfill took all of it.
fn refuse_an_egress_cap_the_backend_cannot_honour(cfg: &UploadConfig) -> Result<(), VtopError> {
    let Some(cap) = cfg.max_egress_bytes_per_second else {
        return Ok(());
    };
    let why = match cfg.backend.as_str() {
        "s3_native" => return Ok(()),
        "s3cmd" | "awscli" | "minio" => {
            "it shells out to an external CLI, which sends the bytes itself; VTOP never \
             touches them, so it cannot pace them"
        }
        "localfs" => "it writes to a local directory; there is no egress to shape",
        "mock" | "mock_fail" | "mock_limited" => {
            "it keeps objects in memory; there is no egress to shape"
        }
        // Unknown names are refused below with the registered list.
        _ => return Ok(()),
    };
    Err(VtopError::Config(format!(
        "upload.max_egress_bytes_per_second = {cap} is set, but the {} backend cannot honour \
         it: {why}. Only s3_native shapes its request bodies; omit the cap or use s3_native",
        cfg.backend
    )))
}

#[cfg(test)]
mod egress_cap_construction {
    use super::*;

    /// The backend names `build_backend` accepts, read FROM its own refusal of
    /// an unknown name — so a backend added there joins this table without
    /// anyone remembering to add it here.
    async fn every_backend_name() -> Vec<String> {
        let cfg: UploadConfig = serde_json::from_str(r#"{"bucket":"b","backend":"no_such"}"#)
            .expect("a bare upload config parses");
        let msg = match build_backend(&cfg).await {
            Ok(_) => panic!("an unknown backend must be refused"),
            Err(err) => err.to_string(),
        };
        let list = msg
            .split("(expected ")
            .nth(1)
            .and_then(|tail| tail.split(')').next())
            .unwrap_or_else(|| panic!("the refusal lists the backends: {msg}"));
        list.split('|').map(str::to_owned).collect()
    }

    #[tokio::test]
    async fn a_backend_that_cannot_shape_its_bytes_refuses_a_cap_naming_the_cap_and_the_backend() {
        // Table over EVERY backend build_backend knows (#481). Each either
        // honours the cap or refuses it at construction with the cap and its
        // own name in the message — none may accept it and ignore it. The
        // s3_native row is judged per TRANSPORT in s3_native's own table; here
        // it must simply not be refused for being s3_native.
        let names = every_backend_name().await;
        assert!(
            names.len() >= 8 && names.iter().any(|n| n == "s3_native"),
            "the table must cover the real backend list, or it proves nothing: {names:?}"
        );
        for name in names {
            let mut cfg: UploadConfig = serde_json::from_str(r#"{"bucket":"b"}"#).expect("parses");
            cfg.backend = name.clone();
            cfg.max_egress_bytes_per_second = Some(4 * 1024 * 1024);
            let verdict = refuse_an_egress_cap_the_backend_cannot_honour(&cfg);
            if name == "s3_native" {
                assert!(
                    verdict.is_ok(),
                    "s3_native owns its request bodies and must not be refused here: {verdict:?}"
                );
                continue;
            }
            let msg = verdict
                .expect_err(&format!(
                    "[{name}] cannot shape its bytes and must refuse the cap"
                ))
                .to_string();
            assert!(
                msg.contains("max_egress_bytes_per_second = 4194304") && msg.contains(&name),
                "[{name}] the refusal names the cap AND the backend: {msg}"
            );
            // And through the real door, before any binary is probed: a
            // compatibility backend with no command configured would otherwise
            // fail for THAT reason and hide this one.
            let msg = match build_backend(&cfg).await {
                Ok(_) => panic!("[{name}] build_backend accepted a cap it cannot honour"),
                Err(err) => err.to_string(),
            };
            assert!(
                msg.contains("max_egress_bytes_per_second"),
                "[{name}] build_backend refuses for the cap, first: {msg}"
            );
        }
    }

    #[tokio::test]
    async fn an_unset_cap_changes_nothing_for_any_backend() {
        for name in every_backend_name().await {
            let mut cfg: UploadConfig = serde_json::from_str(r#"{"bucket":"b"}"#).expect("parses");
            cfg.backend = name.clone();
            assert!(
                refuse_an_egress_cap_the_backend_cannot_honour(&cfg).is_ok(),
                "[{name}] no cap, no refusal — the default path is untouched"
            );
        }
    }
}
