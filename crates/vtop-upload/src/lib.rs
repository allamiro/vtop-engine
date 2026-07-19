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
pub mod s3_native;
pub mod s3cmd_backend;

pub use base::{ObjectChecksum, ObjectHead, UploadBackend, VerificationResult};
pub use mock::MockBackend;

use std::sync::Arc;
use vtop_core::config::UploadConfig;
use vtop_core::errors::VtopError;

/// Construct the upload backend named in config. Returns a trait object so the
/// engine is backend-agnostic.
pub async fn build_backend(cfg: &UploadConfig) -> Result<Arc<dyn UploadBackend>, VtopError> {
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
