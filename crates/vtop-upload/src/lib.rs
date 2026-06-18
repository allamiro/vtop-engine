//! # vtop-upload
//!
//! Pluggable S3-compatible upload backends. The native [`s3_native`] backend is
//! the primary production backend; [`s3cmd_backend`], [`awscli_backend`], and
//! [`minio_backend`] are compatibility-mode backends that shell out to external
//! tools. Every backend verifies object integrity, and the engine must not
//! commit source progress until verification passes.

pub mod awscli_backend;
pub mod base;
pub mod minio_backend;
pub mod mock;
pub mod s3_native;
pub mod s3cmd_backend;

pub use base::{ObjectHead, UploadBackend, VerificationResult};
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
        "s3cmd" => Arc::new(s3cmd_backend::S3cmdBackend::new(cfg.profile.clone())),
        "awscli" => Arc::new(awscli_backend::AwsCliBackend::new(
            cfg.endpoint_url.clone(),
            cfg.profile.clone(),
        )),
        "minio" => Arc::new(minio_backend::MinioBackend::new(
            cfg.profile.clone().unwrap_or_else(|| "local".to_string()),
        )),
        "mock" => Arc::new(MockBackend::new()),
        other => {
            return Err(VtopError::Config(format!(
                "unknown upload backend: {other} (expected s3_native|s3cmd|awscli|minio|mock)"
            )))
        }
    };
    Ok(backend)
}
