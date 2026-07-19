//! Compatibility backend that shells out to `s3cmd`.
//!
//! Compatibility mode only — the native S3 backend is the primary production
//! backend. Credentials live in the `s3cmd` config (`S3CMD_CONFIG`) and are
//! never printed by this module.

use crate::base::{
    read_command_bounded, verify_command_content, ObjectChecksum, ObjectHead, StoredManifest,
    UploadBackend, VerificationResult,
};
use crate::command::CommandPolicy;
use async_trait::async_trait;
use std::path::Path;
use tokio::process::Command;
use vtop_core::errors::VtopError;

pub struct S3cmdBackend {
    command: CommandPolicy,
    /// Optional path to an s3cmd config file (maps to `--config`).
    pub config_file: Option<String>,
}

impl S3cmdBackend {
    pub(crate) fn new(command: CommandPolicy, config_file: Option<String>) -> Self {
        Self {
            command,
            config_file,
        }
    }

    fn base_cmd(&self) -> Command {
        let mut c = self.command.command();
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
        let mut command = self.base_cmd();
        command.arg("put").arg(local_path).arg(object_uri);
        self.command.run(&mut command, "object upload").await
    }

    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        _checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredManifest, VtopError> {
        let mut command = self.base_cmd();
        command.arg("put").arg(local_path).arg(manifest_uri);
        self.command.run(&mut command, "manifest upload").await?;
        Ok(StoredManifest::default())
    }

    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
        let tmp = tempfile::NamedTempFile::new()
            .map_err(|e| VtopError::Upload(format!("temp file for download: {e}")))?;
        // --force: the temp file already exists (NamedTempFile creates it).
        let mut command = self.base_cmd();
        command
            .arg("get")
            .arg("--force")
            .arg(object_uri)
            .arg(tmp.path());
        self.command.run(&mut command, "object download").await?;
        tokio::fs::read(tmp.path())
            .await
            .map_err(|e| VtopError::Upload(format!("reading downloaded {object_uri}: {e}")))
    }

    async fn get_object_bounded(
        &self,
        object_uri: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        let mut cmd = self.base_cmd();
        cmd.arg("get").arg(object_uri).arg("-");
        read_command_bounded(
            &mut cmd,
            max_bytes,
            object_uri,
            "s3cmd",
            self.command.timeout(),
        )
        .await
    }

    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
        let mut command = self.base_cmd();
        command.arg("info").arg(object_uri);
        let out = self.command.output(&mut command, "object metadata").await?;
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
        expected_checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError> {
        let Some(expected) = expected_checksum else {
            let head = self.head_object(object_uri).await?;
            return match head.size_bytes {
                Some(size) if size == expected_size => Ok(VerificationResult::limited(
                    "s3cmd: object present and size matches (checksums disabled)",
                )),
                Some(size) => Ok(VerificationResult::failed(format!(
                    "size mismatch: expected {expected_size}, got {size}"
                ))),
                None => Ok(VerificationResult::failed("could not read object size")),
            };
        };
        let mut cmd = self.base_cmd();
        cmd.arg("get").arg(object_uri).arg("-");
        verify_command_content(
            &mut cmd,
            expected_size,
            expected,
            "s3cmd",
            self.command.timeout(),
        )
        .await
    }

    async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
        let mut command = self.base_cmd();
        command.arg("del").arg(object_uri);
        self.command.run(&mut command, "object delete").await
    }

    fn backend_name(&self) -> &'static str {
        "s3cmd"
    }
    fn supports_checksum_verification(&self) -> bool {
        true
    }
    fn supports_multipart(&self) -> bool {
        true
    }
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
