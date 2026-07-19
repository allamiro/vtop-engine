//! Compatibility backend that shells out to `s3cmd`.
//!
//! Compatibility mode only — the native S3 backend is the primary production
//! backend. Credentials live in the `s3cmd` config (`S3CMD_CONFIG`) and are
//! never printed by this module.

use crate::base::{ObjectChecksum, ObjectHead, UploadBackend, VerificationResult};
use async_trait::async_trait;
use std::path::Path;
use tokio::process::Command;
use vtop_core::errors::VtopError;

pub struct S3cmdBackend {
    /// Optional path to an s3cmd config file (maps to `--config`).
    pub config_file: Option<String>,
}

impl S3cmdBackend {
    pub fn new(config_file: Option<String>) -> Self {
        Self {
            config_file: config_file.or_else(|| std::env::var("S3CMD_CONFIG").ok()),
        }
    }

    fn base_cmd(&self) -> Command {
        let mut c = Command::new("s3cmd");
        if let Some(cfg) = &self.config_file {
            c.arg("--config").arg(cfg);
        }
        c
    }
}

#[async_trait]
impl UploadBackend for S3cmdBackend {
    async fn put_object(
        &self,
        local_path: &Path,
        object_uri: &str,
        _checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<(), VtopError> {
        run(self.base_cmd().arg("put").arg(local_path).arg(object_uri)).await
    }

    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        _checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<(), VtopError> {
        run(self.base_cmd().arg("put").arg(local_path).arg(manifest_uri)).await
    }

    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
        let tmp = tempfile::NamedTempFile::new()
            .map_err(|e| VtopError::Upload(format!("temp file for download: {e}")))?;
        // --force: the temp file already exists (NamedTempFile creates it).
        run(self
            .base_cmd()
            .arg("get")
            .arg("--force")
            .arg(object_uri)
            .arg(tmp.path()))
        .await?;
        tokio::fs::read(tmp.path())
            .await
            .map_err(|e| VtopError::Upload(format!("reading downloaded {object_uri}: {e}")))
    }

    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
        let out = output(self.base_cmd().arg("info").arg(object_uri)).await?;
        let size = parse_size(&out);
        Ok(ObjectHead {
            uri: object_uri.to_string(),
            size_bytes: size,
            etag: None,
            checksum_sha256: None,
        })
    }

    async fn verify_object(
        &self,
        object_uri: &str,
        expected_size: u64,
        _expected_checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError> {
        let head = self.head_object(object_uri).await?;
        match head.size_bytes {
            Some(sz) if sz == expected_size => Ok(VerificationResult::limited(
                "s3cmd: object present and size matches (no sha256 from backend)",
            )),
            Some(sz) => Ok(VerificationResult::failed(format!(
                "size mismatch: expected {expected_size}, got {sz}"
            ))),
            None => Ok(VerificationResult::failed("could not read object size")),
        }
    }

    async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
        run(self.base_cmd().arg("del").arg(object_uri)).await
    }

    fn backend_name(&self) -> &'static str {
        "s3cmd"
    }
    fn supports_checksum_verification(&self) -> bool {
        false
    }
    fn supports_multipart(&self) -> bool {
        true
    }
}

async fn run(cmd: &mut Command) -> Result<(), VtopError> {
    let status = cmd
        .status()
        .await
        .map_err(|e| VtopError::Upload(format!("spawning command failed: {e}")))?;
    if status.success() {
        Ok(())
    } else {
        Err(VtopError::Upload(format!("command exited with {status}")))
    }
}

async fn output(cmd: &mut Command) -> Result<String, VtopError> {
    let out = cmd
        .output()
        .await
        .map_err(|e| VtopError::Upload(format!("spawning command failed: {e}")))?;
    if !out.status.success() {
        return Err(VtopError::Upload(format!(
            "command exited with {}",
            out.status
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Parse a `File size: <n>` line from `s3cmd info` output.
fn parse_size(info: &str) -> Option<u64> {
    for line in info.lines() {
        let l = line.trim();
        if let Some(rest) = l.strip_prefix("File size:") {
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_file_size() {
        let info = "   File size: 924822\n   Last mod: ...";
        assert_eq!(parse_size(info), Some(924822));
    }
}
